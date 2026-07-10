"""
iceberg_utility.py  --  Glue 5.0 (Spark 3.5) config-driven Iceberg utility
==========================================================================

A reusable "Iceberg lab" generator. Instead of hardcoding tables, it reads a
declarative JSON SPEC describing a list of OPERATIONS and executes them against
an Iceberg GlueCatalog. Use it to spin up tables with controlled
characteristics (partitioned or not, large vs micro files, skew, trickle
ingest, MoR deletes) and to run CRUD/MERGE on existing tables.

Operations
----------
  create_table : create + populate a table. Levers: partition (cols+transform
                 or null), file_profile (large|micro|medium), target_file_size_mb,
                 rows / commits / rows_per_commit (commits>1 = trickle), skew,
                 schema (with per-column generators), table_properties.
  ingest       : append N rows to an existing table (bulk or trickle).
  crud         : run parameterized DELETE / UPDATE statements.
  merge        : upsert N source rows on a key (MoR upsert).

Job arguments
-------------
  --warehouse_bucket  S3 bucket for the Iceberg warehouse   (required)
  --glue_database     target Glue database                  (default: iceberg_meta_analytics)
  --config            inline JSON spec OR s3://.../spec.json (required)
  --preset            built-in preset name (alternative to --config; e.g. 'demo')
  --spec_prefix       optional s3:// base for named specs shipped as files
                      (seed bucket). A non-built-in --preset <name> loads
                      <spec_prefix>/<name>.json.

Exactly one of --config / --preset must resolve to a spec.
"""
import json
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- #
# 0. Arguments
# --------------------------------------------------------------------------- #
_defaults = {"glue_database": "iceberg_meta_analytics", "config": "", "preset": "",
             # s3:// base (seed bucket) where custom preset specs live. Empty =>
             # legacy behavior (specs under s3://<warehouse>/utility/specs/).
             "spec_prefix": ""}
for k, v in _defaults.items():
    if f"--{k}" not in sys.argv:
        sys.argv += [f"--{k}", v]

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "warehouse_bucket", "glue_database", "config", "preset", "spec_prefix"],
)

WAREHOUSE_BUCKET = args["warehouse_bucket"]
DB = args["glue_database"]
CATALOG = "glue_catalog"
WAREHOUSE = f"s3://{WAREHOUSE_BUCKET}/warehouse/"

# --------------------------------------------------------------------------- #
# 1. Spark / Iceberg session
# --------------------------------------------------------------------------- #
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# spark.sql.extensions is set by Glue's --datalake-formats=iceberg (it is a
# static config and must NOT be set here).
spark.conf.set(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
spark.conf.set("spark.sql.shuffle.partitions", "16")


def log(msg):
    print(f"\n========== {msg} ==========", flush=True)


def _q(ident):
    """Back-quote a SQL identifier so names with hyphens/reserved chars are
    valid (e.g. a database derived from a hyphenated resource prefix)."""
    return "`" + ident.replace("`", "``") + "`"


def fqtn(name):
    # Quote each part: catalog.`db`.`table` -- catalog is a Spark-config name
    # (no quoting), db/table may contain hyphens.
    return f"{CATALOG}.{_q(DB)}.{_q(name)}"


# --------------------------------------------------------------------------- #
# 2. Spec resolution (preset | inline JSON | s3://)
# --------------------------------------------------------------------------- #
def load_spec():
    preset = args.get("preset", "").strip()
    config = args.get("config", "").strip()
    if preset:
        return PRESETS[preset]() if preset in PRESETS else _load_preset_from_s3(preset)
    if config.startswith("s3://"):
        bucket, _, key = config[5:].partition("/")
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)
    if config:
        return json.loads(config)
    raise ValueError("Provide --config (inline JSON or s3:// path) or --preset.")


def _load_preset_from_s3(name):
    # Convenience: presets can also be shipped as JSON specs in S3. They live in
    # the SEED bucket now (code, not data), passed via --spec_prefix as an s3://
    # base. Fall back to the legacy warehouse location when unset.
    spec_prefix = args.get("spec_prefix", "").strip().rstrip("/")
    if spec_prefix:
        bucket, _, base_key = spec_prefix[5:].partition("/")
        key = f"{base_key}/{name}.json"
    else:
        bucket, key = WAREHOUSE_BUCKET, f"utility/specs/{name}.json"
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


# --------------------------------------------------------------------------- #
# 3. Column-value generators
# --------------------------------------------------------------------------- #
# Each generator maps a column spec -> a Spark Column expression. `seq` is the
# monotonic row index column (named "_n"); `salt` varies randomness per commit.
def _zipf_weights(n):
    w = [1.0 / (i + 1) for i in range(n)]
    s = sum(w)
    return [x / s for x in w]


def gen_column(col, salt):
    name = col["name"]
    g = col.get("gen", {"kind": "null"})
    kind = g["kind"]
    n = F.col("_n")

    if kind == "sequence":
        return n.cast(col["type"]).alias(name)
    if kind == "int_mod":
        return (n % F.lit(g["mod"])).cast(col["type"]).alias(name)
    if kind == "rand_int":
        return (F.rand(salt + hash(name) % 1000) * g["max"]).cast("int").cast(col["type"]).alias(name)
    if kind == "rand_double":
        return F.round(F.rand(salt + hash(name) % 1000) * g["max"], 2).cast(col["type"]).alias(name)
    if kind == "choice":
        arr = F.array(*[F.lit(v) for v in g["values"]])
        idx = (F.rand(salt + hash(name) % 1000) * len(g["values"])).cast("int")
        return arr[idx].alias(name)
    if kind == "template":
        # template like "user{id}@x" or "s-{int_mod:2000000}"
        return _template_expr(g["template"], name)
    if kind == "uuid":
        return F.expr("uuid()").alias(name)
    if kind == "now":
        return F.current_timestamp().alias(name)
    if kind == "ts_recent_days":
        secs = F.lit(g["days"] * 86400)
        return (F.current_timestamp() - F.expr("make_interval(0,0,0,0,0,0,0)")
                - (F.rand(salt + hash(name) % 1000) * secs).cast("int").cast("string").cast("interval second")).alias(name) \
            if False else _ts_recent(g["days"], salt, name)
    if kind == "date_sequence":
        # spread rows across `distinct` consecutive dates starting at `start`
        return F.expr(f"date_add(date('{g['start']}'), cast(_n % {g['distinct']} as int))").alias(name)
    if kind == "date_random":
        return F.expr(f"date_add(date('{g['start']}'), cast(rand({salt}) * {g['days_range']} as int))").alias(name)
    if kind == "null":
        return F.lit(None).cast(col["type"]).alias(name)
    raise ValueError(f"Unknown generator kind: {kind}")


def _ts_recent(days, salt, name):
    secs = days * 86400
    return F.expr(
        f"current_timestamp() - make_interval(0,0,0,0,0,0, cast(rand({salt + 7}) * {secs} as int))"
    ).alias(name)


def _template_expr(template, name):
    # supports {id} and {int_mod:N}
    import re
    parts = re.split(r"(\{[^}]+\})", template)
    pieces = []
    for p in parts:
        if p.startswith("{") and p.endswith("}"):
            token = p[1:-1]
            if token == "id":
                pieces.append(F.col("_n").cast("string"))
            elif token.startswith("int_mod:"):
                mod = int(token.split(":")[1])
                pieces.append((F.col("_n") % F.lit(mod)).cast("string"))
            else:
                pieces.append(F.lit(p))
        elif p:
            pieces.append(F.lit(p))
    return F.concat(*pieces).alias(name)


def build_dataframe(schema, n_rows, salt, skew=None, keep_index=False):
    """Build a DataFrame of n_rows using the per-column generators.
    If keep_index, the monotonic row-index column `_n` is retained in the output."""
    df = spark.range(0, n_rows).withColumnRenamed("id", "_n")
    # Skewed partition column (zipf) overrides its generator if requested.
    skew_col = skew["column"] if skew else None
    cols = []
    for c in schema:
        if c["name"] == skew_col or c.get("gen", {}).get("kind") == "skew_partition":
            values = c["gen"]["values"]
            weights = _zipf_weights(len(values))
            cum, expr = 0.0, F.lit(len(values) - 1)
            bounds = []
            for i, w in enumerate(weights):
                cum += w
                bounds.append((cum, i))
            pick = F.rand(salt + 13)
            for upper, idx in reversed(bounds):
                expr = F.when(pick < F.lit(upper), F.lit(idx)).otherwise(expr)
            arr = F.array(*[F.lit(v) for v in values])
            cols.append(arr[expr].alias(c["name"]))
        else:
            cols.append(gen_column(c, salt))
    if keep_index:
        cols = [F.col("_n")] + cols
    return df.select(*cols)


# --------------------------------------------------------------------------- #
# 4. DDL helpers
# --------------------------------------------------------------------------- #
def _transform_sql(p):
    col, t = p["column"], p.get("transform", "identity")
    if t == "identity":
        return col
    if t == "days":
        return f"days({col})"
    if t == "months":
        return f"months({col})"
    if t == "years":
        return f"years({col})"
    if t == "hours":
        return f"hours({col})"
    if t == "bucket":
        return f"bucket({p['n']}, {col})"
    if t == "truncate":
        return f"truncate({p['n']}, {col})"
    raise ValueError(f"Unknown partition transform: {t}")


def create_table(spec_defaults, op):
    table = op["table"]
    log(f"create_table {table}  ({op.get('comment','')})")
    spark.sql(f"DROP TABLE IF EXISTS {fqtn(table)} PURGE")

    cols_ddl = ",\n  ".join(f"{c['name']} {c['type']}" for c in op["schema"])
    part = op.get("partition")
    part_ddl = ""
    if part:
        part_ddl = "PARTITIONED BY (" + ", ".join(_transform_sql(p) for p in part) + ")"

    props = {
        "format-version": str(op.get("format_version", spec_defaults.get("format_version", "2"))),
        "write.parquet.compression-codec": op.get("compression", spec_defaults.get("compression", "zstd")),
    }
    tfs = op.get("target_file_size_mb")
    if tfs:
        props["write.target-file-size-bytes"] = str(int(tfs) * 1024 * 1024)
    props.update(op.get("table_properties", {}))
    props_ddl = ",\n  ".join(f"'{k}' = '{v}'" for k, v in props.items())

    spark.sql(f"""
        CREATE TABLE {fqtn(table)} (
          {cols_ddl}
        )
        USING iceberg
        {part_ddl}
        TBLPROPERTIES (
          {props_ddl}
        )
    """)
    _populate(op, table)


def _files_per_partition(op):
    """Resolve files_per_partition from the spec.

    Accepts an int, or the string "executor_cores" to mean "one file per executor
    core" — i.e. the total number of task slots across the cluster. We read that
    from spark.sparkContext.defaultParallelism, which Spark sets to the total
    executor cores (e.g. G.2X x5 workers => 4 executors x 8 cores = 32). This way
    the file count auto-scales with the worker config instead of being hardcoded.
    """
    fpp = op.get("files_per_partition", 1)
    if isinstance(fpp, str) and fpp.strip().lower() in ("executor_cores", "cores", "auto"):
        try:
            cores = int(spark.sparkContext.defaultParallelism)
            log(f"   files_per_partition=executor_cores -> defaultParallelism={cores}")
            return max(1, cores)
        except Exception as e:
            log(f"   WARN: could not read defaultParallelism ({e}); falling back to 1")
            return 1
    return max(1, int(fpp))


def _populate(op, table):
    profile = op.get("file_profile", "medium")
    schema = op["schema"]
    part = op.get("partition")
    part_cols = [p["column"] for p in part] if part else []

    commits = int(op.get("commits", 1))
    if commits > 1:
        # Trickle ingest: many small commits -> many small files (micro profile).
        rows_per = int(op.get("rows_per_commit", 300))
        seed_every = op.get("seed_every_partition", False)
        skew = op.get("skew")
        for c in range(commits):
            df = build_dataframe(schema, rows_per, salt=100 + c, skew=skew)
            if seed_every and part_cols:
                df = _add_partition_seed(df, schema, part_cols, salt=900 + c)
            writer = df.coalesce(1)
            if part_cols:
                writer = writer.sortWithinPartitions(*part_cols)
            w = writer.writeTo(fqtn(table))
            if part_cols:
                w = w.option("fanout-enabled", "true")
            w.append()
            if (c + 1) % 12 == 0:
                print(f"   {table}: trickle commit {c+1}/{commits}", flush=True)
        return

    # Single bulk commit.
    rows = int(op["rows"])
    df = build_dataframe(schema, rows, salt=1, skew=op.get("skew"))
    if profile == "large" and part_cols:
        ndist = int(op.get("distinct_partitions", 7))
        fpp = _files_per_partition(op)
        if fpp > 1:
            # Spread each partition's rows across `fpp` files. We repartition into
            # ndist*fpp Spark tasks keyed by (partition cols + a uniform bucket id),
            # so every partition yields ~fpp roughly-equal files. fanout-enabled lets
            # a single task write multiple partitions without pre-sorting globally.
            bucket = F.pmod(F.monotonically_increasing_id().cast("long"), F.lit(fpp))
            df = (df.withColumn("_bucket", bucket)
                    .repartition(ndist * fpp, *[F.col(c) for c in part_cols], F.col("_bucket"))
                    .drop("_bucket")
                    .sortWithinPartitions(*part_cols))
            (df.writeTo(fqtn(table)).option("fanout-enabled", "true").append())
            return
        df = df.repartition(ndist, *[F.col(c) for c in part_cols]).sortWithinPartitions(*part_cols)
    elif "output_files" in op:
        df = df.repartition(int(op["output_files"]))
    df.writeTo(fqtn(table)).append()


def _add_partition_seed(df, schema, part_cols, salt):
    """Guarantee every partition value gets at least one row this commit, so
    every partition accumulates a file on every trickle commit."""
    # Only supports single-column identity partition seeding (the micro case).
    pc = part_cols[0]
    col_spec = next(c for c in schema if c["name"] == pc)
    values = col_spec.get("gen", {}).get("values")
    if not values:
        return df
    rows = [(v,) for v in values]
    seed = spark.createDataFrame(rows, [pc])
    # fill other columns with simple defaults
    for c in schema:
        if c["name"] == pc:
            continue
        seed = seed.withColumn(c["name"], gen_column_constant(c, salt))
    seed = seed.select(*[c["name"] for c in schema])
    return df.unionByName(seed)


def gen_column_constant(col, salt):
    """A cheap single-value expression for seed rows."""
    g = col.get("gen", {"kind": "null"})
    kind = g["kind"]
    if kind in ("uuid",):
        return F.expr("uuid()")
    if kind == "now":
        return F.current_timestamp()
    if kind == "choice":
        return F.lit(g["values"][0])
    if kind in ("rand_int", "int_mod", "sequence"):
        return F.lit(0).cast(col["type"])
    if kind in ("rand_double",):
        return F.lit(0.0).cast(col["type"])
    if kind == "template":
        return F.lit("seed")
    return F.lit(None).cast(col["type"])


# --------------------------------------------------------------------------- #
# 5. ingest / crud / merge
# --------------------------------------------------------------------------- #
def ingest(op):
    table = op["table"]
    log(f"ingest {table}  (+{op.get('rows', op.get('rows_per_commit'))} rows)")
    # Reuse existing schema from the catalog table.
    schema = [{"name": f.name, "type": f.dataType.simpleString(),
               "gen": _infer_gen(f.name, f.dataType.simpleString())}
              for f in spark.table(fqtn(table)).schema.fields]
    commits = int(op.get("commits", 1))
    if commits > 1:
        for c in range(commits):
            df = build_dataframe(schema, int(op["rows_per_commit"]), salt=2000 + c)
            df.coalesce(1).writeTo(fqtn(table)).option("fanout-enabled", "true").append()
    else:
        df = build_dataframe(schema, int(op["rows"]), salt=2000)
        df.writeTo(fqtn(table)).append()


def _infer_gen(name, simple_type):
    if simple_type in ("bigint", "int"):
        return {"kind": "rand_int", "max": 1000000}
    if simple_type in ("double", "float"):
        return {"kind": "rand_double", "max": 1000}
    if simple_type == "string":
        return {"kind": "template", "template": f"{name}_{{id}}"}
    if simple_type == "timestamp":
        return {"kind": "now"}
    if simple_type == "date":
        return {"kind": "date_random", "start": "2024-01-01", "days_range": 700}
    return {"kind": "null"}


def crud(op):
    table = op["table"]
    log(f"crud {table}  ({len(op['statements'])} statement(s))")
    for stmt in op["statements"]:
        sql = stmt.replace("{table}", fqtn(table))
        print(f"   -> {sql}", flush=True)
        spark.sql(sql)


def merge(op):
    """Upsert source_rows into the table on `key`. A `key_overlap` fraction of
    the source reuses existing keys (-> MATCHED updates); the remainder use new,
    out-of-range keys (-> NOT MATCHED inserts). Single-column integer key."""
    table = op["table"]
    keys = op["key"]
    key = keys[0]
    log(f"merge {table}  (upsert {op['source_rows']} rows on {keys})")
    fields = spark.table(fqtn(table)).schema.fields
    key_type = next(f.dataType.simpleString() for f in fields if f.name == key)
    schema = [{"name": f.name, "type": f.dataType.simpleString(),
               "gen": _infer_gen(f.name, f.dataType.simpleString())}
              for f in fields]

    src_rows = int(op["source_rows"])
    overlap = float(op.get("key_overlap", 0.5))
    n_update = int(src_rows * overlap)

    # Existing keys to reuse for the "update" portion.
    existing = [r[key] for r in
                spark.table(fqtn(table)).select(key).distinct().limit(n_update).collect()]
    max_existing = spark.table(fqtn(table)).agg(F.max(key)).collect()[0][0] or 0

    # Build source rows from the column generators, then overwrite the key:
    #  - rows [0, len(existing))           -> reuse an existing key  (update)
    #  - rows [len(existing), src_rows)    -> brand-new key > max     (insert)
    src = build_dataframe(schema, src_rows, salt=3000, keep_index=True)
    if existing:
        ex_df = spark.createDataFrame(
            [(i, v) for i, v in enumerate(existing)], ["_n", "_existing_key"])
        src = src.join(ex_df, on="_n", how="left")
        new_key = (F.lit(max_existing) + F.col("_n") + F.lit(1)).cast(key_type)
        src = src.withColumn(
            key, F.coalesce(F.col("_existing_key").cast(key_type), new_key)).drop("_existing_key")
    src = src.select(*[c["name"] for c in schema])

    # MERGE INTO requires a DETERMINISTIC source (Spark re-scans it for matched
    # vs not-matched). Our generators use rand()/uuid(), so materialize the
    # source to a scratch Iceberg table first, then merge from storage.
    scratch = f"{table}__merge_src"
    spark.sql(f"DROP TABLE IF EXISTS {fqtn(scratch)} PURGE")
    src.writeTo(fqtn(scratch)).using("iceberg").create()

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    set_clause = ", ".join(f"t.{c['name']} = s.{c['name']}" for c in schema if c["name"] not in keys)
    insert_cols = ", ".join(c["name"] for c in schema)
    insert_vals = ", ".join(f"s.{c['name']}" for c in schema)
    spark.sql(f"""
        MERGE INTO {fqtn(table)} t
        USING {fqtn(scratch)} s ON {on}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    spark.sql(f"DROP TABLE IF EXISTS {fqtn(scratch)} PURGE")


# --------------------------------------------------------------------------- #
# 6. Built-in presets
# --------------------------------------------------------------------------- #
def _preset_demo():
    return _load_preset_from_s3("demo")


PRESETS = {"demo": _preset_demo}

OP_DISPATCH = {
    "create_table": lambda defaults, op: create_table(defaults, op),
    "ingest": lambda defaults, op: ingest(op),
    "crud": lambda defaults, op: crud(op),
    "merge": lambda defaults, op: merge(op),
}


# --------------------------------------------------------------------------- #
# 7. Run
# --------------------------------------------------------------------------- #
def main():
    spec = load_spec()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{_q(DB)}")
    defaults = spec.get("defaults", {})
    ops = spec["operations"]
    log(f"SPEC: {spec.get('description','(no description)')}  -- {len(ops)} operation(s)")
    for i, op in enumerate(ops):
        kind = op["op"]
        if kind not in OP_DISPATCH:
            raise ValueError(f"Unknown op '{kind}' at index {i}")
        OP_DISPATCH[kind](defaults, op)

    # Verification summary
    log("VERIFICATION SUMMARY")
    tables = sorted({op["table"] for op in ops if "table" in op})
    for t in tables:
        try:
            cnt = spark.sql(f"SELECT COUNT(*) c FROM {fqtn(t)}").collect()[0]["c"]
            files = spark.sql(f"""
                SELECT COUNT(*) n, ROUND(AVG(file_size_in_bytes)/1024.0,1) avg_kb
                FROM {fqtn(t)}.files WHERE content = 0
            """).collect()[0]
            snaps = spark.sql(f"SELECT COUNT(*) c FROM {fqtn(t)}.snapshots").collect()[0]["c"]
            print(f" {t:28s} rows={cnt:>12,}  data_files={files['n']:>6}  "
                  f"avg={files['avg_kb']:>9} KB  snapshots={snaps}", flush=True)
        except Exception as e:
            print(f" {t:28s} (summary failed: {e})", flush=True)

    print("\nIceberg utility run complete.", flush=True)


main()

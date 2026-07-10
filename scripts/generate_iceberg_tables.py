"""
generate_iceberg_tables.py  -- Glue 5.0 (Spark 3.5) data generator
===================================================================

Creates three Apache Iceberg tables in the Glue Data Catalog, each engineered
to exhibit a DIFFERENT metadata profile so the companion notebook has rich,
contrasting data to analyze:

  1. web_events_large        PARTITIONED (event_date) -- MANY LARGE (>=15MB) files/partition
                                                          (one file per executor core)
  2. clickstream_micro       PARTITIONED (country)    -- MANY MICRO (KB) files/partition,
                                                          skewed, many snapshots, MoR deletes
  3. customers_unpartitioned UNPARTITIONED            -- a handful of medium files

The script is idempotent: it DROPs (PURGE) each table before recreating it.

Job arguments (all optional, sensible defaults):
  --warehouse_bucket    S3 bucket for the Iceberg warehouse  (required)
  --glue_database       target Glue database  (default: iceberg_meta_analytics)
  --large_rows          rows for web_events_large            (default: 600000000)
  --large_days          distinct date partitions             (default: 7)
  --micro_commits       trickle-ingest commits for micro tbl (default: 96)
  --micro_rows_commit   bulk rows per micro commit            (default: 500)
  --micro_countries     number of country partitions          (default: 12)
  --customer_rows       rows for customers_unpartitioned      (default: 1200000)
"""
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# --------------------------------------------------------------------------- #
# 0. Arguments
# --------------------------------------------------------------------------- #
ARG_NAMES = [
    "JOB_NAME",
    "warehouse_bucket",
    "glue_database",
    "large_rows",
    "large_days",
    "micro_commits",
    "micro_rows_commit",
    "micro_countries",
    "customer_rows",
]
# getResolvedOptions requires every name to be present; supply defaults by
# scanning argv so the job can be launched with only --warehouse_bucket.
_defaults = {
    "glue_database": "iceberg_meta_analytics",
    "large_rows": "600000000",
    "large_days": "7",
    "micro_commits": "96",
    "micro_rows_commit": "500",
    "micro_countries": "12",
    "customer_rows": "1200000",
}
for k, v in _defaults.items():
    if f"--{k}" not in sys.argv:
        sys.argv += [f"--{k}", v]

args = getResolvedOptions(sys.argv, ARG_NAMES)

WAREHOUSE_BUCKET = args["warehouse_bucket"]
DB = args["glue_database"]
LARGE_ROWS = int(args["large_rows"])
LARGE_DAYS = int(args["large_days"])
MICRO_COMMITS = int(args["micro_commits"])
MICRO_ROWS_COMMIT = int(args["micro_rows_commit"])
MICRO_COUNTRIES = int(args["micro_countries"])
CUSTOMER_ROWS = int(args["customer_rows"])

CATALOG = "glue_catalog"
WAREHOUSE = f"s3://{WAREHOUSE_BUCKET}/warehouse/"

# --------------------------------------------------------------------------- #
# 1. Spark / Iceberg session
# --------------------------------------------------------------------------- #
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# NOTE: spark.sql.extensions is a STATIC config and is already set by Glue's
# --datalake-formats=iceberg job argument, so we must NOT set it here. The
# catalog.* properties below are dynamic and safe to set on the live session.
spark.conf.set(f"spark.sql.catalog.{CATALOG}",
               "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{CATALOG}.catalog-impl",
               "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set(f"spark.sql.catalog.{CATALOG}.io-impl",
               "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
# Keep shuffle small & deterministic so file counts are predictable.
spark.conf.set("spark.sql.shuffle.partitions", "16")


def log(msg):
    print(f"\n========== {msg} ==========", flush=True)


def fqtn(name):
    return f"{CATALOG}.{DB}.{name}"


def drop(name):
    spark.sql(f"DROP TABLE IF EXISTS {fqtn(name)} PURGE")


COUNTRIES = ["US", "IN", "GB", "DE", "BR", "JP", "FR", "CA", "AU", "MX", "NL", "SG",
             "ZA", "KR", "ES", "IT"][:MICRO_COUNTRIES]
DEVICES = ["mobile", "desktop", "tablet", "smart-tv", "console"]
EVENT_TYPES = ["page_view", "click", "scroll", "add_to_cart", "purchase", "search"]
ACTIONS = ["view", "click", "hover", "bounce", "convert"]
SEGMENTS = ["new", "returning", "vip", "churn_risk", "dormant"]

# =========================================================================== #
# TABLE 1: web_events_large  --  PARTITIONED, FEW LARGE FILES PER PARTITION
# =========================================================================== #
log(f"TABLE 1/3  web_events_large  ({LARGE_ROWS:,} rows over {LARGE_DAYS} day partitions)")
drop("web_events_large")

spark.sql(f"""
    CREATE TABLE {fqtn('web_events_large')} (
        event_id    STRING,
        user_id     BIGINT,
        session_id  STRING,
        event_type  STRING,
        url         STRING,
        referrer    STRING,
        device      STRING,
        country     STRING,
        duration_ms INT,
        ts          TIMESTAMP,
        event_date  DATE
    )
    USING iceberg
    PARTITIONED BY (event_date)
    TBLPROPERTIES (
        'format-version'              = '2',
        'write.target-file-size-bytes'= '268435456',   -- 256 MB: force few large files
        'write.parquet.compression-codec' = 'zstd',
        'write.distribution-mode'     = 'hash'
    )
""")

base = spark.range(0, LARGE_ROWS).withColumnRenamed("id", "n")
ev_types_arr = F.array(*[F.lit(x) for x in EVENT_TYPES])
dev_arr = F.array(*[F.lit(x) for x in DEVICES])
ctry_arr = F.array(*[F.lit(x) for x in COUNTRIES])

large = (
    base
    .withColumn("event_id", F.expr("uuid()"))
    .withColumn("user_id", (F.col("n") % F.lit(500000)).cast("bigint"))
    .withColumn("session_id", F.concat(F.lit("s-"), (F.col("n") % F.lit(2000000)).cast("string")))
    .withColumn("day_idx", (F.col("n") % F.lit(LARGE_DAYS)).cast("int"))
    .withColumn("event_type", ev_types_arr[(F.rand(1) * len(EVENT_TYPES)).cast("int")])
    .withColumn("url", F.concat(F.lit("/p/"), (F.col("n") % F.lit(50000)).cast("string")))
    .withColumn("referrer", F.concat(F.lit("https://ref.example/"), (F.col("n") % F.lit(1000)).cast("string")))
    .withColumn("device", dev_arr[(F.rand(2) * len(DEVICES)).cast("int")])
    .withColumn("country", ctry_arr[(F.rand(3) * len(COUNTRIES)).cast("int")])
    .withColumn("duration_ms", (F.rand(4) * 600000).cast("int"))
    .withColumn("event_date", F.expr("date_add(date('2026-06-01'), day_idx)"))
    .withColumn("ts", F.expr("timestamp(event_date) + make_interval(0,0,0,0,0,0, cast(rand(5)*86400 as int))"))
    .drop("n", "day_idx")
)

# Spread each date partition across `files_per_partition` files. We use one file
# per executor core (defaultParallelism = total executor cores, e.g. G.2X x5 =>
# 4 executors x 8 cores = 32), so the file count auto-scales with the worker
# config. Repartition into LARGE_DAYS*fpp tasks keyed by (event_date + a uniform
# bucket id) so every partition yields ~fpp roughly-equal files; fanout-enabled
# lets a single task write multiple partitions without a global pre-sort.
try:
    files_per_partition = max(1, int(spark.sparkContext.defaultParallelism))
except Exception:
    files_per_partition = 1
log(f"web_events_large: files_per_partition={files_per_partition} (executor cores)")
(
    large
    .withColumn("_bucket", F.pmod(F.monotonically_increasing_id().cast("long"),
                                  F.lit(files_per_partition)))
    .repartition(LARGE_DAYS * files_per_partition, F.col("event_date"), F.col("_bucket"))
    .drop("_bucket")
    .sortWithinPartitions("event_date")
    .writeTo(fqtn("web_events_large"))
    .option("fanout-enabled", "true")
    .append()
)

# =========================================================================== #
# TABLE 2: clickstream_micro  --  PARTITIONED, MANY MICRO FILES, SKEW, MoR
# =========================================================================== #
log(f"TABLE 2/3  clickstream_micro  ({MICRO_COMMITS} trickle commits x {MICRO_COUNTRIES} partitions)")
drop("clickstream_micro")

spark.sql(f"""
    CREATE TABLE {fqtn('clickstream_micro')} (
        event_id  STRING,
        user_id   BIGINT,
        page      STRING,
        action    STRING,
        value     DOUBLE,
        ts        TIMESTAMP,
        country   STRING
    )
    USING iceberg
    PARTITIONED BY (country)
    TBLPROPERTIES (
        'format-version'              = '2',
        'write.target-file-size-bytes'= '536870912',   -- large target, but tiny commits => KB files
        'write.parquet.compression-codec' = 'zstd',
        'write.distribution-mode'     = 'none',         -- do NOT let Iceberg coalesce tiny commits
        'write.delete.mode'           = 'merge-on-read',
        'write.update.mode'           = 'merge-on-read',
        'write.merge.mode'            = 'merge-on-read',
        'commit.manifest.min-count-to-merge' = '1000'   -- keep manifests un-merged => visible metadata growth
    )
""")

# Skew weights (zipf-ish): a few "hot" countries dominate row volume.
weights = [1.0 / (i + 1) for i in range(MICRO_COUNTRIES)]   # 1, 1/2, 1/3, ...
wsum = sum(weights)
norm = [w / wsum for w in weights]

# Build a CASE expression mapping a uniform [0,1) draw -> country index (skewed).
cum, bounds = 0.0, []
for i, w in enumerate(norm):
    cum += w
    bounds.append((cum, i))

for c in range(MICRO_COMMITS):
    # (a) seed: exactly one row per country -> guarantees EVERY partition gets a
    #     file on EVERY commit ("a lot of files per partition").
    seed = (
        spark.createDataFrame([(i,) for i in range(MICRO_COUNTRIES)], ["cidx"])
    )
    # (b) skewed bulk rows
    bulk = spark.range(0, MICRO_ROWS_COMMIT).withColumnRenamed("id", "n")
    pick = F.rand(100 + c)
    cidx_expr = F.lit(MICRO_COUNTRIES - 1)
    for upper, idx in reversed(bounds):
        cidx_expr = F.when(pick < F.lit(upper), F.lit(idx)).otherwise(cidx_expr)
    bulk = bulk.withColumn("cidx", cidx_expr).drop("n")

    commit_df = seed.unionByName(bulk)
    ctry_arr2 = F.array(*[F.lit(x) for x in COUNTRIES])
    page_arr = F.array(*[F.lit(f"/c{c}/page-{i}") for i in range(5)])
    act_arr = F.array(*[F.lit(x) for x in ACTIONS])

    commit_df = (
        commit_df
        .withColumn("event_id", F.expr("uuid()"))
        .withColumn("user_id", (F.rand(200 + c) * 100000).cast("bigint"))
        .withColumn("page", page_arr[(F.rand(300 + c) * 5).cast("int")])
        .withColumn("action", act_arr[(F.rand(400 + c) * len(ACTIONS)).cast("int")])
        .withColumn("value", F.round(F.rand(500 + c) * 100, 2))
        .withColumn("ts", F.expr(f"current_timestamp() - make_interval(0,0,0,0,0,{c},0)"))
        .withColumn("country", ctry_arr2[F.col("cidx")])
        .drop("cidx")
        .select("event_id", "user_id", "page", "action", "value", "ts", "country")
    )

    # coalesce(1) -> a single task -> with fanout writer, exactly ONE small file
    # per partition present in this commit. Each commit = one snapshot.
    (
        commit_df.coalesce(1)
        .sortWithinPartitions("country")
        .writeTo(fqtn("clickstream_micro"))
        .option("fanout-enabled", "true")
        .append()
    )
    if (c + 1) % 12 == 0:
        print(f"   micro commit {c + 1}/{MICRO_COMMITS}", flush=True)

# Merge-on-read DML -> position delete files + extra snapshots (rich metadata).
log("clickstream_micro: MoR DELETE + UPDATE (creates delete files & snapshots)")
spark.sql(f"DELETE FROM {fqtn('clickstream_micro')} WHERE action = 'bounce'")
spark.sql(f"UPDATE {fqtn('clickstream_micro')} SET value = value * 1.10 WHERE country = 'US'")

# =========================================================================== #
# TABLE 3: customers_unpartitioned  --  NO PARTITION
# =========================================================================== #
log(f"TABLE 3/3  customers_unpartitioned  ({CUSTOMER_ROWS:,} rows, no partitions)")
drop("customers_unpartitioned")

spark.sql(f"""
    CREATE TABLE {fqtn('customers_unpartitioned')} (
        customer_id    BIGINT,
        name           STRING,
        email          STRING,
        country        STRING,
        segment        STRING,
        signup_date    DATE,
        lifetime_value DOUBLE
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'              = '2',
        'write.target-file-size-bytes'= '134217728',
        'write.parquet.compression-codec' = 'zstd'
    )
""")

seg_arr = F.array(*[F.lit(x) for x in SEGMENTS])
ctry_arr3 = F.array(*[F.lit(x) for x in COUNTRIES])
customers = (
    spark.range(0, CUSTOMER_ROWS).withColumnRenamed("id", "customer_id")
    .withColumn("name", F.concat(F.lit("customer_"), F.col("customer_id").cast("string")))
    .withColumn("email", F.concat(F.lit("user"), F.col("customer_id").cast("string"), F.lit("@example.com")))
    .withColumn("country", ctry_arr3[(F.rand(7) * len(COUNTRIES)).cast("int")])
    .withColumn("segment", seg_arr[(F.rand(8) * len(SEGMENTS)).cast("int")])
    .withColumn("signup_date", F.expr("date_add(date('2024-01-01'), cast(rand(9)*900 as int))"))
    .withColumn("lifetime_value", F.round(F.rand(10) * 5000, 2))
)
(
    customers.repartition(18)
    .writeTo(fqtn("customers_unpartitioned"))
    .append()
)
# One UPDATE -> a second snapshot (copy-on-write rewrite by default).
spark.sql(f"UPDATE {fqtn('customers_unpartitioned')} SET segment = 'vip' "
          f"WHERE lifetime_value > 4500")

# =========================================================================== #
# 4. Verification summary (printed to job logs)
# =========================================================================== #
log("VERIFICATION SUMMARY")
for t in ["web_events_large", "clickstream_micro", "customers_unpartitioned"]:
    cnt = spark.sql(f"SELECT COUNT(*) c FROM {fqtn(t)}").collect()[0]["c"]
    files = spark.sql(f"""
        SELECT COUNT(*) AS n_files,
               ROUND(AVG(file_size_in_bytes)/1024.0, 1) AS avg_kb,
               ROUND(MIN(file_size_in_bytes)/1024.0, 1) AS min_kb,
               ROUND(MAX(file_size_in_bytes)/1024.0/1024.0, 2) AS max_mb
        FROM {fqtn(t)}.files
    """).collect()[0]
    snaps = spark.sql(f"SELECT COUNT(*) c FROM {fqtn(t)}.snapshots").collect()[0]["c"]
    print(f" {t:28s} rows={cnt:>12,}  data_files={files['n_files']:>6}  "
          f"avg={files['avg_kb']:>9} KB  min={files['min_kb']:>7} KB  "
          f"max={files['max_mb']:>6} MB  snapshots={snaps}", flush=True)

print("\nAll three Iceberg tables created successfully.", flush=True)

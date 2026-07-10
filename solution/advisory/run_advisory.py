"""
run_advisory.py  --  headless Iceberg advisory runner (Glue 5.0 Spark job).

A user-driven, post-deploy companion to the interactive notebook (section H).
The stack creates a Glue job that runs this script, but never runs it for you;
you trigger it on demand:

    aws glue start-job-run --job-name <prefix>-advisory \
      --arguments '{"--mode":"advise"}'

It wires the SAME agentic advisor used in the notebook (iceberg_advisor.py +
best_practices.json, pulled from the seed bucket via --advisor_prefix) to LIVE
Spark metadata,
runs the requested mode, prints the agent's output to the job log, AND writes a
Markdown report authored by the agent to S3.

Modes (via --mode):
  analysis    metrics only, no agent (cheapest; no Bedrock call)
  advise      agent highlights issues + cited recommendations (read-only)   [default]
  remediate   agent proposes fixes; executes ONLY actions in --approved_actions

Safety: remediation executes a table-mutating CALL only when its "table:action"
is present in --approved_actions (comma-separated). Empty (default) => the agent
proposes commands but executes nothing, exactly like the notebook's closed gate.
"""
import datetime
import importlib.util
import json
import os
import sys
import tempfile

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# --------------------------------------------------------------------------- #
# Arguments (Glue passes these via DefaultArguments + start-job-run overrides)
# --------------------------------------------------------------------------- #
_REQUIRED = ["warehouse_bucket", "glue_database", "region", "bedrock_model"]
_OPTIONAL = {"mode": "advise", "tables": "", "approved_actions": "",
             "report_prefix": "advisory-reports",
             # s3:// base (seed bucket) holding the advisor module + pack. The
             # stack passes this; empty => fall back to the warehouse bucket's
             # agentic/ prefix (legacy layout).
             "advisor_prefix": ""}

# getResolvedOptions only returns keys that were actually passed, so resolve
# optionals defensively (Glue forbids declaring optional args with no value).
_present = [a[2:].split("=", 1)[0] for a in sys.argv if a.startswith("--")]
_wanted = _REQUIRED + [k for k in _OPTIONAL if k in _present]
args = getResolvedOptions(sys.argv, _wanted)

WAREHOUSE_BUCKET = args["warehouse_bucket"]
DB = args["glue_database"]
REGION = args["region"]
BEDROCK_MODEL = args["bedrock_model"]
MODE = args.get("mode", _OPTIONAL["mode"]).lower().strip()
CATALOG = "glue_catalog"
SMALL_FILE_THRESHOLD_MB = 32

TABLE_ALLOWLIST = [t.strip() for t in args.get("tables", "").split(",") if t.strip()] or None
APPROVED_ACTIONS = {a.strip() for a in args.get("approved_actions", "").split(",") if a.strip()}
REPORT_PREFIX = args.get("report_prefix", _OPTIONAL["report_prefix"]).strip("/")
# Where to load the advisor module + best-practice pack from. Prefer the seed
# bucket base passed by the stack; else legacy warehouse bucket agentic/ prefix.
ADVISOR_PREFIX = args.get("advisor_prefix", "").strip().rstrip("/") \
    or f"s3://{WAREHOUSE_BUCKET}/agentic"

# --------------------------------------------------------------------------- #
# Spark session wired to the Glue catalog as an Iceberg catalog
# --------------------------------------------------------------------------- #
sc = SparkContext()
spark = (SparkSession.builder
         .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
         .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
         .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{WAREHOUSE_BUCKET}/warehouse/")
         .getOrCreate())

s3 = boto3.client("s3", region_name=REGION)


def _q(ident):
    return "`" + ident.replace("`", "``") + "`"


def fqtn(t):
    return f"{CATALOG}.{_q(DB)}.{_q(t)}"


def _discover_tables():
    if TABLE_ALLOWLIST:
        return list(TABLE_ALLOWLIST)
    rows = spark.sql(f"SHOW TABLES IN {CATALOG}.{_q(DB)}").collect()
    found = []
    for r in rows:
        name = r["tableName"]
        try:
            spark.sql(f"SELECT 1 FROM {CATALOG}.{_q(DB)}.{_q(name)}.snapshots LIMIT 1")
            found.append(name)
        except Exception:
            pass
    return found


def is_partitioned(t):
    cols = [f.name for f in spark.table(f"{fqtn(t)}.partitions").schema.fields]
    return "partition" in cols


TABLES = _discover_tables()
print(f">>> advisory runner: mode={MODE} db={DB} tables={TABLES}", flush=True)

# --------------------------------------------------------------------------- #
# Metrics / proposer / executor  -- identical logic to notebook section H
# --------------------------------------------------------------------------- #
import statistics  # noqa: E402


def _table_metrics(t):
    files = spark.sql(f"""
        SELECT COUNT(*) df, COALESCE(SUM(file_size_in_bytes),0) tb,
               COALESCE(AVG(file_size_in_bytes),0) ab, COALESCE(MIN(file_size_in_bytes),0) mn
        FROM {fqtn(t)}.files WHERE content = 0""").first()
    small = spark.sql(f"""SELECT COUNT(*) c FROM {fqtn(t)}.files
        WHERE content = 0 AND file_size_in_bytes < {SMALL_FILE_THRESHOLD_MB}*1024*1024""").first()["c"]
    dels = spark.sql(f"SELECT COUNT(*) c FROM {fqtn(t)}.files WHERE content != 0").first()["c"]
    snaps = spark.sql(f"SELECT COUNT(*) c FROM {fqtn(t)}.snapshots").first()["c"]
    man = spark.sql(f"""SELECT COUNT(*) n,
        ROUND(SUM(added_data_files_count+existing_data_files_count+deleted_data_files_count)
              / GREATEST(COUNT(*),1),1) afpm FROM {fqtn(t)}.manifests""").first()
    m = {"table": t, "partitioned": is_partitioned(t),
         "data_files": int(files["df"]), "small_files": int(small), "delete_files": int(dels),
         "total_mb": round(files["tb"]/1024/1024, 2), "avg_file_mb": round(files["ab"]/1024/1024, 3),
         "min_file_kb": round(files["mn"]/1024, 1), "snapshots": int(snaps),
         "delete_ratio": round(dels/files["df"], 3) if files["df"] else 0.0,
         "n_manifests": int(man["n"]), "avg_files_per_manifest": man["afpm"]}
    if is_partitioned(t):
        parts = spark.sql(f"""SELECT partition, record_count rc, file_count fc,
            ROUND(total_data_file_size_in_bytes/1024.0/file_count,1) avg_kb
            FROM {fqtn(t)}.partitions ORDER BY fc DESC""").collect()
        m["partitions"] = len(parts)
        m["top_partitions"] = [{"partition": str(r["partition"]), "files": r["fc"],
                                "rows": r["rc"], "avg_file_kb": r["avg_kb"]} for r in parts[:8]]
        rows = [r["rc"] for r in parts]
        if len(rows) > 1 and statistics.mean(rows):
            m["skew_cv"] = round(statistics.pstdev(rows)/statistics.mean(rows), 3)
            m["hot_partition_share"] = round(max(rows)/sum(rows), 3)
    else:
        m["partitions"] = None
    return m


def metrics_provider(table=None):
    if table:
        return _table_metrics(table)
    return {"tables": [_table_metrics(t) for t in TABLES]}


def remediation_proposer(table):
    m = _table_metrics(table)
    props = []
    if m["small_files"] > max(4, 0.3*m["data_files"]):
        props.append({"priority": "HIGH", "action": "compact",
            "command": f"CALL {CATALOG}.system.rewrite_data_files(table => '{DB}.{table}', options => map('target-file-size-bytes','536870912','min-input-files','5'))"})
    if m["snapshots"] > 20:
        props.append({"priority": "MEDIUM", "action": "expire_snapshots",
            "command": f"CALL {CATALOG}.system.expire_snapshots(table => '{DB}.{table}', retain_last => 5)"})
    if m["delete_files"] > 0:
        props.append({"priority": "MEDIUM", "action": "rewrite_position_deletes",
            "command": f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{DB}.{table}')"})
    if m["n_manifests"] > 20:
        props.append({"priority": "LOW", "action": "rewrite_manifests",
            "command": f"CALL {CATALOG}.system.rewrite_manifests(table => '{DB}.{table}')"})
    return props


_ACTION_SQL = {
    "compact": lambda t: f"CALL {CATALOG}.system.rewrite_data_files(table => '{DB}.{t}', options => map('target-file-size-bytes','536870912','min-input-files','5'))",
    "expire_snapshots": lambda t: f"CALL {CATALOG}.system.expire_snapshots(table => '{DB}.{t}', retain_last => 5)",
    "rewrite_manifests": lambda t: f"CALL {CATALOG}.system.rewrite_manifests(table => '{DB}.{t}')",
    "rewrite_position_deletes": lambda t: f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{DB}.{t}')",
}


def remediation_executor(table, action):
    sql = _ACTION_SQL[action](table)
    print(f"   [EXECUTING] {sql}", flush=True)
    return spark.sql(sql).toJSON().collect()


# --------------------------------------------------------------------------- #
# Load the advisor module + knowledge pack from the project bucket
# --------------------------------------------------------------------------- #
def _load_advisor():
    tmp = tempfile.mkdtemp()
    adv = os.path.join(tmp, "iceberg_advisor.py")
    bp = os.path.join(tmp, "best_practices.json")
    # ADVISOR_PREFIX is an s3:// base; split into (bucket, key-prefix) once.
    adv_bucket, adv_key_prefix = ADVISOR_PREFIX[len("s3://"):].split("/", 1)
    s3.download_file(adv_bucket, f"{adv_key_prefix}/iceberg_advisor.py", adv)
    s3.download_file(adv_bucket, f"{adv_key_prefix}/best_practices.json", bp)
    spec = importlib.util.spec_from_file_location("iceberg_advisor", adv)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, json.load(open(bp))


# --------------------------------------------------------------------------- #
# Run the chosen mode and build a Markdown report
# --------------------------------------------------------------------------- #
def _now_iso():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp():
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def run():
    ts = _now_iso()
    md = [f"# Iceberg Advisory Report",
          f"",
          f"- **Generated:** {ts}",
          f"- **Database:** `{DB}`",
          f"- **Mode:** `{MODE}`",
          f"- **Tables:** {', '.join(f'`{t}`' for t in TABLES) or '(none found)'}",
          f""]

    if not TABLES:
        md.append("> No Iceberg tables found in the database — nothing to analyze.")
        return "\n".join(md)

    if MODE == "analysis":
        # No agent / no Bedrock call: just emit the structured metrics as Markdown.
        md.append("## Metrics\n")
        for t in TABLES:
            m = _table_metrics(t)
            md.append(f"### `{t}`\n")
            md.append("```json")
            md.append(json.dumps(m, indent=2, default=str))
            md.append("```\n")
        return "\n".join(md)

    # advise / remediate -> drive the agent
    advisor_mod, best_practices = _load_advisor()
    allow_execute = MODE == "remediate" and bool(APPROVED_ACTIONS)
    cfg = advisor_mod.AdvisorConfig(
        model_id=BEDROCK_MODEL, region=REGION,
        allow_execute=allow_execute, approved_actions=set(APPROVED_ACTIONS),
        verbose=True)
    advisor = advisor_mod.IcebergAdvisor(
        metrics_provider, best_practices, remediation_proposer, remediation_executor, cfg)

    if MODE == "remediate":
        md.append(f"- **Approved actions:** "
                  f"{', '.join(sorted(APPROVED_ACTIONS)) or '(none — proposals only, nothing executed)'}\n")

    for t in TABLES:
        print(f"\n=== {MODE} :: {t} ===", flush=True)
        if MODE == "advise":
            text = advisor.advise(t)
        elif MODE == "remediate":
            text = advisor.remediate(t)
        else:
            raise SystemExit(f"unknown --mode '{MODE}' (use analysis | advise | remediate)")
        md.append(f"## `{t}`\n")
        md.append(text.strip() + "\n")

    executed = advisor.executed_log()
    md.append("## Executed actions\n")
    if executed:
        for e in executed:
            md.append(f"- `{e['action']}`")
    else:
        md.append("_None — no approved actions were executed._")
    md.append("")
    return "\n".join(md)


report_md = run()
print("\n" + "=" * 70 + "\n" + report_md + "\n" + "=" * 70, flush=True)

key = f"{REPORT_PREFIX}/advisory-{MODE}-{_stamp()}.md"
s3.put_object(Bucket=WAREHOUSE_BUCKET, Key=key,
              Body=report_md.encode("utf-8"), ContentType="text/markdown")
print(f">>> report written: s3://{WAREHOUSE_BUCKET}/{key}", flush=True)

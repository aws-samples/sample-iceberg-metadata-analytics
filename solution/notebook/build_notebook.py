#!/usr/bin/env python3
"""
build_notebook.py  --  generates iceberg_metadata_analytics.ipynb

We author the notebook programmatically so the cell sources stay readable and
diff-friendly in source control.

Two modes:
  python3 build_notebook.py            -> account-specific build (real bucket/DB
                                          baked in; used for the proven in-account
                                          notebook + cell validators)
  python3 build_notebook.py --template -> emits sentinel tokens ({{WAREHOUSE_BUCKET}},
                                          {{GLUE_DB}}, {{REGION}}, {{BEDROCK_MODEL}},
                                          {{TABLE_ALLOWLIST_JSON}}, {{AGENTIC_ENABLED_PY}})
                                          instead of real values, for the
                                          CloudFormation custom resource to
                                          substitute per-customer.

The SHIPPABLE artifacts are the --template files (.template.ipynb/.py) written
here in solution/notebook/. The account-specific build is a LOCAL VALIDATION
artifact only (real account values baked in) and is written OUTSIDE the shippable
solution -- pass `--outdir <dir>` to place it there (see scripts/05_register_notebook.sh).

  --outdir <dir>   where to write the generated files (default: this directory)
"""
import json
import os
import sys

TEMPLATE_MODE = "--template" in sys.argv

# --outdir <dir>: where to write the generated files. Defaults to this script's
# directory. Template mode (the shippable .template.* files) stays here in
# solution/notebook/; the ACCOUNT-specific build is a local validation artifact
# that lives OUTSIDE the shippable solution, so its callers pass --outdir.
def _arg(flag, default):
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default

# Values used only by the LOCAL account-specific validation build (never by the
# shippable --template files, which carry {{SENTINEL}} tokens). No real account
# identifiers are baked into this shipped source: the local validator overrides
# these via env vars (NB_WAREHOUSE_BUCKET / NB_GLUE_DB / NB_REGION / NB_BEDROCK_MODEL),
# falling back to obvious placeholders so an un-overridden account build is clearly
# non-functional rather than silently pointed at someone else's account.
_ACCOUNT_BUCKET = os.environ.get("NB_WAREHOUSE_BUCKET", "REPLACE_ME_warehouse_bucket")
_ACCOUNT_DB = os.environ.get("NB_GLUE_DB", "REPLACE_ME_glue_db")
_ACCOUNT_REGION = os.environ.get("NB_REGION", "us-west-2")
_ACCOUNT_MODEL = os.environ.get("NB_BEDROCK_MODEL", "us.amazon.nova-pro-v1:0")
# Seed-bucket base (s3://.../rendered/<prefix>) where the notebook loads its
# helper module + advisor pack. Distinct from the warehouse bucket, which is
# data-only. Local account builds override via NB_CODE_S3_BASE.
_ACCOUNT_CODE_BASE = os.environ.get("NB_CODE_S3_BASE", "s3://REPLACE_ME_seed_bucket/REPLACE_ME_code_base")

if TEMPLATE_MODE:
    WAREHOUSE_BUCKET = "{{WAREHOUSE_BUCKET}}"
    CODE_S3_BASE = "{{CODE_S3_BASE}}"
    GLUE_DB = "{{GLUE_DB}}"
    REGION = "{{REGION}}"
    BEDROCK_MODEL = "{{BEDROCK_MODEL}}"
    # Python literal: a list (e.g. ["a","b"]) or None. The CFN Lambda substitutes
    # a Python-valid value ("None" or a list literal), NOT JSON "null".
    TABLE_ALLOWLIST_JSON = "{{TABLE_ALLOWLIST_PY}}"
    # Python bool literal ("True"/"False"). The CFN Lambda sets this from the
    # EnableAgentic parameter so section H no-ops cleanly when agentic is off
    # (the advisor assets aren't uploaded in that case).
    AGENTIC_ENABLED = "{{AGENTIC_ENABLED_PY}}"
else:
    WAREHOUSE_BUCKET = _ACCOUNT_BUCKET
    CODE_S3_BASE = _ACCOUNT_CODE_BASE
    GLUE_DB = _ACCOUNT_DB
    REGION = _ACCOUNT_REGION
    BEDROCK_MODEL = _ACCOUNT_MODEL
    TABLE_ALLOWLIST_JSON = "None"
    AGENTIC_ENABLED = "True"

CELLS = []


def md(text):
    # Glue Studio tags every cell with {"trusted": true, "tags": []}; match it
    # so the notebook opens cleanly in the Glue Studio notebook editor.
    CELLS.append({"cell_type": "markdown",
                  "metadata": {"trusted": True, "tags": []},
                  "source": text.strip("\n").splitlines(keepends=True)})


def code(text):
    CELLS.append({
        "cell_type": "code",
        "metadata": {"trusted": True, "tags": []},
        "execution_count": None, "outputs": [],
        "source": text.strip("\n").splitlines(keepends=True),
    })


# =========================================================================== #
md(r"""
# Apache Iceberg Metadata-Layer Analytics — Glue Interactive Notebook

This notebook explores the **Iceberg metadata layer** to surface operational
insights that the table data alone cannot tell you:

| # | Analysis | Question it answers |
|---|----------|---------------------|
| A | **Inventory & layout** | How big is each table, how many files/snapshots/partitions? |
| B | **File-size distribution** | Where are the *small-file* and *large-file* problems? |
| C | **Partition-level analytics** | Which partitions are skewed / fragmented / hot? |
| D | **Snapshot & commit history** | How fast is the table churning? Trickle ingest? |
| E | **Delete-file (MoR) pressure** | How much read amplification from merge-on-read? |
| F | **Manifest health** | Are manifests bloated, hurting query planning? |
| G | **Cost & remediation** | What `OPTIMIZE` / `expire_snapshots` actions to take? |

It reads Iceberg **metadata tables** (`.files`, `.partitions`, `.snapshots`,
`.manifests`, `.history`, `.all_data_files`, `.metadata_log_entries`) — *not*
the raw data — so it is cheap to run even on huge tables.

> Configured for the three sample tables created by `generate_iceberg_tables.py`:
> `web_events_large` (large files), `clickstream_micro` (micro files / skew),
> `customers_unpartitioned` (no partitions).
""")

# --------------------------------------------------------------------------- #
md(r"""
## 0. Session configuration

These `%%`-magics configure the **AWS Glue interactive session**. Everything is
set in `%%configure` so it takes effect *before* the Spark session starts.

> **Where's all the code?** The analytics logic lives in a single helper module,
> `iceberg_nb_helpers.py`, attached to the session with
> `%extra_py_files` above. That keeps these cells thin — each one just calls a
> method on the `analytics` engine and shows the result. To read or change the
> SQL, open that module (staged in the seed bucket at
> `<code-base>/notebook/iceberg_nb_helpers.py`).
>
> **Why the `--conf` line matters.** `spark.sql.extensions` is a **static**
> Spark config — it must be set at session-creation time and cannot be changed
> with `spark.conf.set()` afterwards. In an interactive session,
> `--datalake-formats=iceberg` alone does **not** register the Iceberg SQL
> extension, so procedure calls like `CALL ...rewrite_data_files(...)`
> (section G) fail to parse. Setting `spark.sql.extensions` (and the catalog
> properties) here fixes that and also wires the `glue_catalog`, so the code
> cells don't need to re-set them.
>
> ⚠️ If you change `%%configure` or `%extra_py_files`, you must **restart the
> kernel** for it to apply (they only run when the session is created).
""")

code('''
%idle_timeout 60
%glue_version 5.0
%worker_type G.2X
%number_of_workers 5
%extra_py_files __CODE_S3_BASE__/notebook/iceberg_nb_helpers.py
'''.replace("__CODE_S3_BASE__", CODE_S3_BASE))

code('''%%configure
{
  "--datalake-formats": "iceberg",
  "--enable-glue-datacatalog": "true",
  "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=s3://__WAREHOUSE_BUCKET__/warehouse/"
}
'''.replace("__WAREHOUSE_BUCKET__", WAREHOUSE_BUCKET))

code('''
# ---- config + analytics engine ---------------------------------------------
# All the metadata SQL lives in iceberg_nb_helpers.py (attached above via
# %extra_py_files). We import it once the Spark session exists and wrap it in a
# single `analytics` object that every cell below drives.
import iceberg_nb_helpers as nb

WAREHOUSE_BUCKET = "__WAREHOUSE_BUCKET__"
DB               = "__GLUE_DB__"
CATALOG          = "glue_catalog"

# Optional allow-list of tables to analyze (Python list, or None = all tables in DB).
TABLE_ALLOWLIST  = __TABLE_ALLOWLIST_JSON__

# Verify the Iceberg SQL extension loaded (required for the CALL procedures in
# section G). If this prints <UNSET>, re-check %%configure and RESTART the kernel.
try:
    print("spark.sql.extensions =", spark.conf.get("spark.sql.extensions"))
except Exception:
    print("spark.sql.extensions = <UNSET> -- CALL procedures in section G will fail")

analytics = nb.IcebergAnalytics(spark, db=DB, catalog=CATALOG,
                                table_allowlist=TABLE_ALLOWLIST)
TABLES = analytics.tables

print("Catalog wired. Tables:", TABLES)
if not TABLES:
    print("No Iceberg tables found in", f"{CATALOG}.{DB}",
          "-- check the database name / that tables exist.")
else:
    print("Partitioned?:", {t: analytics.is_partitioned(t) for t in TABLES})
'''.replace("__WAREHOUSE_BUCKET__", WAREHOUSE_BUCKET)
   .replace("__GLUE_DB__", GLUE_DB)
   .replace("__TABLE_ALLOWLIST_JSON__", TABLE_ALLOWLIST_JSON))

# --------------------------------------------------------------------------- #
md(r"""
## A. Inventory & layout overview

A single sweep over every table's metadata tables. This is the "dashboard"
row you would publish to CloudWatch / QuickSight on a schedule.

* `data_files`, `total_bytes`, `avg_file_mb` → small-file detector
* `snapshots` → churn / trickle-ingest detector
* `partitions` → cardinality (NULL for unpartitioned)
* `delete_files` → merge-on-read read amplification
""")

code(r'''
inventory = analytics.inventory()
inventory.show(truncate=False)
''')

# --------------------------------------------------------------------------- #
md(r"""
## B. File-size distribution (the small-file problem)

Bucketing every data file by size reveals which tables suffer the classic
small-file problem. `clickstream_micro` should be dominated by KB-scale files;
`web_events_large` by a few large files.
""")

code(r'''
for t in TABLES:
    print(f"\n=== {t} — file-size distribution ===")
    analytics.file_size_histogram(t).show(truncate=False)
''')

# --------------------------------------------------------------------------- #
md(r"""
## C. Partition-level analytics  ⭐ (the gap this artifact fills)

The reference blog stops at table-level metrics. Here we go **per partition**:

1. **Fragmentation** — files-per-partition and avg file size per partition.
2. **Skew** — row/byte distribution across partitions (a few hot partitions?).
3. **Small-file partitions** — exactly which partitions to `OPTIMIZE`.

For unpartitioned tables this section is skipped automatically.
""")

code(r'''
for t in TABLES:
    if analytics.is_partitioned(t):
        print(f"\n=== {t} — top partitions by file count ===")
        analytics.partition_profile(t).show(25, truncate=False)
    else:
        print(f"\n=== {t} is UNPARTITIONED — skipping partition profile ===")
''')

code(r'''
# ---- Partition SKEW summary (coefficient of variation of rows-per-partition)
skew = analytics.partition_skew_summary()
if skew is not None:
    skew.show(truncate=False)
print("skew_cv: higher = more skew. hot_partition_share: fraction of rows in the single biggest partition.")
''')

code(r'''
# ---- Which partitions to OPTIMIZE? (small-file partitions) ------------------
for t in TABLES:
    if analytics.is_partitioned(t):
        df = analytics.small_file_partitions(t)
        n = df.count()
        print(f"\n=== {t}: {n} partition(s) flagged for compaction (avg file < 1 MB & >= 8 files) ===")
        if n:
            df.show(25, truncate=False)
''')

# --------------------------------------------------------------------------- #
md(r"""
## D. Snapshot & commit history (churn / trickle ingest)

Each Iceberg commit creates a snapshot. A long, dense snapshot history with
small `added-data-files` per commit is the signature of **trickle ingestion** —
the root cause of small files. `clickstream_micro` should show many snapshots,
each adding a handful of tiny files.
""")

code(r'''
for t in TABLES:
    print(f"\n=== {t} — commit history ===")
    analytics.snapshot_activity(t).show(60, truncate=False)
''')

code(r'''
# ---- Trickle-ingest fingerprint: avg files & bytes added per append commit ---
analytics.ingest_fingerprint_summary().show(truncate=False)
print("Low avg_kb_per_file + high append_commits == trickle ingestion creating small files.")
''')

# --------------------------------------------------------------------------- #
md(r"""
## E. Delete-file (merge-on-read) pressure

Format-v2 merge-on-read tables accumulate **position/equality delete files**.
Too many deletes relative to data files means heavy read amplification — a
signal to run `rewrite_data_files` / `rewrite_position_delete_files`.
`clickstream_micro` was created with MoR `DELETE`/`UPDATE`.
""")

code(r'''
analytics.delete_pressure_summary().show(truncate=False)
print("delete_ratio > ~0.1 is a good trigger to rewrite_data_files / rewrite_position_deletes.")
''')

# --------------------------------------------------------------------------- #
md(r"""
## F. Manifest health

Iceberg plans queries by reading **manifest files**. Many small manifests slow
down planning. The `.manifests` metadata table exposes per-manifest file/row
counts so you can decide whether to `rewrite_manifests`.
""")

code(r'''
for t in TABLES:
    print(f"\n=== {t} — manifest health ===")
    analytics.manifest_health(t).show(truncate=False)
print("Low avg_files_per_manifest across many manifests => candidate for rewrite_manifests.")
''')

# --------------------------------------------------------------------------- #
md(r"""
## G. Remediation playbook — auto-generated recommendations

Turn the metrics above into a prioritized, copy-pasteable action list. These
are **Spark `CALL` procedures** against the Glue catalog's Iceberg system
namespace — review before running in production.
""")

code(r'''
playbook = analytics.remediation_playbook()
if playbook is not None:
    playbook.show(50, truncate=False)
else:
    print("No remediation actions triggered.")
''')

md(r"""
### (Optional) Execute a remediation — example

Uncomment to compact the micro-file table and watch the file count drop. Re-run
section **A** afterwards to confirm.
""")

code(r'''
# spark.sql(f"""
#   CALL {CATALOG}.system.rewrite_data_files(
#     table => '{DB}.clickstream_micro',
#     options => map('target-file-size-bytes','134217728','min-input-files','5'))
# """).show(truncate=False)
''')

# --------------------------------------------------------------------------- #
md(r"""
## H. Agentic advisory (Amazon Bedrock + Claude)  🤖

An optional **agentic layer** that turns the raw metrics above into
plain-language insight. It runs a Bedrock **Converse tool-use loop** with Claude
and gives it four tools over your tables:

| tool | does |
|------|------|
| `get_metrics(table?)` | read the structured metrics (sections A–G) — *read-only* |
| `lookup_best_practice(topic)` | retrieve **cited** Iceberg/AWS guidance from a local pack |
| `propose_remediation(table)` | candidate `CALL` procedures — *does not execute* |
| `execute_remediation(table, action)` | run a `CALL` — **gated behind your explicit approval** |

Three things you can ask for:
* **`summarize`** — plain summary of table state
* **`advise`** — issues + advisory, each tied to a metric and a cited best practice
* **`remediate`** — propose fixes, then **act yourself** (copy the command) **or**
  let the agent run an action *you have approved*

> **Safety:** the agent can never mutate a table on its own. `execute_remediation`
> refuses unless you set `ADVISOR_ALLOW_EXECUTE = True` **and** add the specific
> `table:action` to `APPROVED_ACTIONS`. Only table *metadata* is sent to Bedrock,
> never row data. Requires `bedrock:InvokeModel` on the notebook role (already
> granted for the sample model tiers) and `--additional-python-modules` is not
> needed — `boto3` is built in.
""")

code('''
# ---- load the advisor module + cited knowledge pack from the project bucket -
import boto3, json, importlib.util, tempfile, os

# Set by the deployer from the EnableAgentic parameter. When False, the advisor
# assets are NOT present in the bucket, so this whole section no-ops cleanly
# instead of erroring on a missing object / undefined module.
AGENTIC_ENABLED   = __AGENTIC_ENABLED_PY__

# The advisor module + best-practice pack live in the SEED bucket (code), not
# the warehouse bucket (data-only). This s3:// base is baked in at deploy time.
ADVISOR_S3_PREFIX = "__CODE_S3_BASE__/agentic"
BEDROCK_MODEL     = "__BEDROCK_MODEL__"   # Nova Lite/Micro or Claude profiles also work
BEDROCK_REGION    = "__REGION__"

iceberg_advisor = None
BEST_PRACTICES  = None
_s3 = boto3.client("s3")'''.replace("__CODE_S3_BASE__", CODE_S3_BASE)
                           .replace("__BEDROCK_MODEL__", BEDROCK_MODEL)
                           .replace("__REGION__", REGION)
                           .replace("__AGENTIC_ENABLED_PY__", AGENTIC_ENABLED) + r'''
if not AGENTIC_ENABLED:
    print("Agentic advisory disabled at deploy time (EnableAgentic=false) -- "
          "skipping section H. Re-deploy with agentic enabled to use it.")
else:
    _tmp = tempfile.mkdtemp()
    # Split "s3://bucket/prefix" -> (bucket, prefix) once, then pull by tail key.
    _adv_bucket, _adv_key_prefix = ADVISOR_S3_PREFIX[len("s3://"):].split("/", 1)
    def _pull(name, dest):
        _s3.download_file(_adv_bucket, f"{_adv_key_prefix}/{name}", dest)
        return dest

    _adv_path = _pull("iceberg_advisor.py", os.path.join(_tmp, "iceberg_advisor.py"))
    _bp_path  = _pull("best_practices.json", os.path.join(_tmp, "best_practices.json"))

    _spec = importlib.util.spec_from_file_location("iceberg_advisor", _adv_path)
    iceberg_advisor = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(iceberg_advisor)
    BEST_PRACTICES = json.load(open(_bp_path))
    print("Advisor loaded. Best-practice topics:", [k for k in BEST_PRACTICES if not k.startswith("_")])
''')

code(r'''
# ---- wire the agent's tools to LIVE Spark metadata --------------------------
# The three callables the advisor needs (metrics_provider, remediation_proposer,
# remediation_executor) are bound methods on the same `analytics` engine used by
# sections A–G, so the agent sees exactly the metrics you just explored.
metrics_provider     = analytics.metrics_provider
remediation_proposer = analytics.remediation_proposer
remediation_executor = analytics.remediation_executor

print("Tools wired to live Spark metrics + (gated) executor.")
''')

code(r'''
# ---- approval gate (you control this) ---------------------------------------
ADVISOR_ALLOW_EXECUTE = False          # master switch: must be True to run anything
APPROVED_ACTIONS      = set()          # e.g. {"clickstream_micro:compact"}

def make_advisor():
    if not AGENTIC_ENABLED or iceberg_advisor is None:
        print("Agentic advisory disabled (EnableAgentic=false) -- nothing to run.")
        return None
    cfg = iceberg_advisor.AdvisorConfig(
        model_id=BEDROCK_MODEL, region=BEDROCK_REGION,
        allow_execute=ADVISOR_ALLOW_EXECUTE, approved_actions=set(APPROVED_ACTIONS),
        verbose=True)
    return iceberg_advisor.IcebergAdvisor(
        metrics_provider, BEST_PRACTICES, remediation_proposer, remediation_executor, cfg)

print("Set ADVISOR_ALLOW_EXECUTE / APPROVED_ACTIONS above, then re-run make_advisor().")
''')

md(r"""
### H.1 — Summarize
""")
code(r'''
advisor = make_advisor()
if advisor:
    _ = advisor.summarize()
''')

md(r"""
### H.2 — Advise (issues + cited recommendations)
""")
code(r'''
advisor = make_advisor()
if advisor:
    _ = advisor.advise("clickstream_micro")
''')

md(r"""
### H.3 — Remediate (propose; execute only what you approve)

With the gate **closed** (default), the agent proposes commands for you to run
yourself. To let it execute a specific action, set above e.g.
`ADVISOR_ALLOW_EXECUTE = True` and
`APPROVED_ACTIONS = {"clickstream_micro:compact"}`, re-run the gate cell, then run this.
""")
code(r'''
advisor = make_advisor()
if advisor:
    _ = advisor.remediate("clickstream_micro")
    print("\nExecuted actions this session:", advisor.executed_log())
''')

md(r"""
---
**Next steps for the blog:** wire section A's `inventory` DataFrame to a
scheduled Glue job that publishes the metrics to CloudWatch (one
`put-metric-data` per table/metric), then alarm on `small_files` and
`delete_ratio`. The partition-level outputs (section C) become the
drill-down QuickSight dataset. The agentic advisory (section H) can run the
same way headlessly — in a scheduled Glue job or Lambda — proposing fixes and
(optionally, with approval) executing them.
""")

# =========================================================================== #
NOTEBOOK = {
    "cells": CELLS,
    "metadata": {
        "kernelspec": {"display_name": "Glue PySpark", "name": "glue_pyspark", "language": "python"},
        "language_info": {"name": "Python_Glue_Session", "mimetype": "text/x-python",
                          "codemirror_mode": {"name": "python", "version": 3},
                          "pygments_lexer": "python3", "file_extension": ".py"},
    },
    # nbformat_minor 4 matches what AWS Glue Studio writes for its notebooks.
    "nbformat": 4, "nbformat_minor": 4,
}

HERE = os.path.dirname(os.path.abspath(__file__))
OUTDIR = os.path.abspath(_arg("--outdir", HERE))
os.makedirs(OUTDIR, exist_ok=True)
# Template mode writes the *.template.* artifacts that package_and_deploy.sh
# uploads to S3 (sentinel tokens intact); account-specific mode writes the proven
# in-account notebook + paired script used by the local cell validators.
_BASE = "iceberg_metadata_analytics.template" if TEMPLATE_MODE else "iceberg_metadata_analytics"
out = os.path.join(OUTDIR, f"{_BASE}.ipynb")
with open(out, "w") as f:
    json.dump(NOTEBOOK, f, indent=1)
print(f"Wrote {out}  ({len(CELLS)} cells)")


def is_magic_line(line):
    return line.lstrip().startswith("%")


def export_py():
    """Emit the paired .py the way Glue Studio does: code cells concatenated,
    magic lines (% / %%) commented out, markdown cells turned into # comments.
    This is the script a NOTEBOOK-mode Glue job stores at scripts/<name>.py."""
    parts = ["# Auto-generated from iceberg_metadata_analytics.ipynb — do not edit by hand.",
             "# Regenerate with: python3 build_notebook.py", ""]
    for c in CELLS:
        src = "".join(c["source"])
        if c["cell_type"] == "markdown":
            parts.append("")
            for ln in src.splitlines():
                parts.append(f"# {ln}" if ln.strip() else "#")
            parts.append("")
            continue
        # code cell: comment out magic lines, keep python
        lines = src.splitlines()
        if lines and lines[0].lstrip().startswith("%%"):
            # whole-cell magic (e.g. %%configure): comment the entire cell out
            parts.append("")
            for ln in lines:
                parts.append(f"# {ln}")
            parts.append("")
            continue
        parts.append("")
        for ln in lines:
            parts.append(f"# {ln}" if is_magic_line(ln) else ln)
        parts.append("")
    py = "\n".join(parts).rstrip() + "\n"
    py_out = os.path.join(OUTDIR, f"{_BASE}.py")
    with open(py_out, "w") as f:
        f.write(py)
    print(f"Wrote {py_out}  (paired script for NOTEBOOK-mode Glue job)")


export_py()

# Auto-generated from iceberg_metadata_analytics.ipynb — do not edit by hand.
# Regenerate with: python3 build_notebook.py


# # Apache Iceberg Metadata-Layer Analytics — Glue Interactive Notebook
#
# This notebook explores the **Iceberg metadata layer** to surface operational
# insights that the table data alone cannot tell you:
#
# | # | Analysis | Question it answers |
# |---|----------|---------------------|
# | A | **Inventory & layout** | How big is each table, how many files/snapshots/partitions? |
# | B | **File-size distribution** | Where are the *small-file* and *large-file* problems? |
# | C | **Partition-level analytics** | Which partitions are skewed / fragmented / hot? |
# | D | **Snapshot & commit history** | How fast is the table churning? Trickle ingest? |
# | E | **Delete-file (MoR) pressure** | How much read amplification from merge-on-read? |
# | F | **Manifest health** | Are manifests bloated, hurting query planning? |
# | G | **Cost & remediation** | What `OPTIMIZE` / `expire_snapshots` actions to take? |
#
# It reads Iceberg **metadata tables** (`.files`, `.partitions`, `.snapshots`,
# `.manifests`, `.history`, `.all_data_files`, `.metadata_log_entries`) — *not*
# the raw data — so it is cheap to run even on huge tables.
#
# > Configured for the three sample tables created by `generate_iceberg_tables.py`:
# > `web_events_large` (large files), `clickstream_micro` (micro files / skew),
# > `customers_unpartitioned` (no partitions).


# ## 0. Session configuration
#
# These `%%`-magics configure the **AWS Glue interactive session**. Everything is
# set in `%%configure` so it takes effect *before* the Spark session starts.
#
# > **Where's all the code?** The analytics logic lives in a single helper module,
# > `iceberg_nb_helpers.py`, attached to the session with
# > `%extra_py_files` above. That keeps these cells thin — each one just calls a
# > method on the `analytics` engine and shows the result. To read or change the
# > SQL, open that module (staged in the seed bucket at
# > `<code-base>/notebook/iceberg_nb_helpers.py`).
# >
# > **Why the `--conf` line matters.** `spark.sql.extensions` is a **static**
# > Spark config — it must be set at session-creation time and cannot be changed
# > with `spark.conf.set()` afterwards. In an interactive session,
# > `--datalake-formats=iceberg` alone does **not** register the Iceberg SQL
# > extension, so procedure calls like `CALL ...rewrite_data_files(...)`
# > (section G) fail to parse. Setting `spark.sql.extensions` (and the catalog
# > properties) here fixes that and also wires the `glue_catalog`, so the code
# > cells don't need to re-set them.
# >
# > ⚠️ If you change `%%configure` or `%extra_py_files`, you must **restart the
# > kernel** for it to apply (they only run when the session is created).


# %idle_timeout 60
# %glue_version 5.0
# %worker_type G.2X
# %number_of_workers 5
# %extra_py_files {{CODE_S3_BASE}}/notebook/iceberg_nb_helpers.py


# %%configure
# {
#   "--datalake-formats": "iceberg",
#   "--enable-glue-datacatalog": "true",
#   "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=s3://{{WAREHOUSE_BUCKET}}/warehouse/"
# }


# ---- config + analytics engine ---------------------------------------------
# All the metadata SQL lives in iceberg_nb_helpers.py (attached above via
# %extra_py_files). We import it once the Spark session exists and wrap it in a
# single `analytics` object that every cell below drives.
import iceberg_nb_helpers as nb

WAREHOUSE_BUCKET = "{{WAREHOUSE_BUCKET}}"
DB               = "{{GLUE_DB}}"
CATALOG          = "glue_catalog"

# Optional allow-list of tables to analyze (Python list, or None = all tables in DB).
TABLE_ALLOWLIST  = {{TABLE_ALLOWLIST_PY}}

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


# ## A. Inventory & layout overview
#
# A single sweep over every table's metadata tables. This is the "dashboard"
# row you would publish to CloudWatch / QuickSight on a schedule.
#
# * `data_files`, `total_bytes`, `avg_file_mb` → small-file detector
# * `snapshots` → churn / trickle-ingest detector
# * `partitions` → cardinality (NULL for unpartitioned)
# * `delete_files` → merge-on-read read amplification


inventory = analytics.inventory()
inventory.show(truncate=False)


# ## B. File-size distribution (the small-file problem)
#
# Bucketing every data file by size reveals which tables suffer the classic
# small-file problem. `clickstream_micro` should be dominated by KB-scale files;
# `web_events_large` by a few large files.


for t in TABLES:
    print(f"\n=== {t} — file-size distribution ===")
    analytics.file_size_histogram(t).show(truncate=False)


# ## C. Partition-level analytics  ⭐ (the gap this artifact fills)
#
# The reference blog stops at table-level metrics. Here we go **per partition**:
#
# 1. **Fragmentation** — files-per-partition and avg file size per partition.
# 2. **Skew** — row/byte distribution across partitions (a few hot partitions?).
# 3. **Small-file partitions** — exactly which partitions to `OPTIMIZE`.
#
# For unpartitioned tables this section is skipped automatically.


for t in TABLES:
    if analytics.is_partitioned(t):
        print(f"\n=== {t} — top partitions by file count ===")
        analytics.partition_profile(t).show(25, truncate=False)
    else:
        print(f"\n=== {t} is UNPARTITIONED — skipping partition profile ===")


# ---- Partition SKEW summary (coefficient of variation of rows-per-partition)
skew = analytics.partition_skew_summary()
if skew is not None:
    skew.show(truncate=False)
print("skew_cv: higher = more skew. hot_partition_share: fraction of rows in the single biggest partition.")


# ---- Which partitions to OPTIMIZE? (small-file partitions) ------------------
for t in TABLES:
    if analytics.is_partitioned(t):
        df = analytics.small_file_partitions(t)
        n = df.count()
        print(f"\n=== {t}: {n} partition(s) flagged for compaction (avg file < 1 MB & >= 8 files) ===")
        if n:
            df.show(25, truncate=False)


# ## D. Snapshot & commit history (churn / trickle ingest)
#
# Each Iceberg commit creates a snapshot. A long, dense snapshot history with
# small `added-data-files` per commit is the signature of **trickle ingestion** —
# the root cause of small files. `clickstream_micro` should show many snapshots,
# each adding a handful of tiny files.


for t in TABLES:
    print(f"\n=== {t} — commit history ===")
    analytics.snapshot_activity(t).show(60, truncate=False)


# ---- Trickle-ingest fingerprint: avg files & bytes added per append commit ---
analytics.ingest_fingerprint_summary().show(truncate=False)
print("Low avg_kb_per_file + high append_commits == trickle ingestion creating small files.")


# ## E. Delete-file (merge-on-read) pressure
#
# Format-v2 merge-on-read tables accumulate **position/equality delete files**.
# Too many deletes relative to data files means heavy read amplification — a
# signal to run `rewrite_data_files` / `rewrite_position_delete_files`.
# `clickstream_micro` was created with MoR `DELETE`/`UPDATE`.


analytics.delete_pressure_summary().show(truncate=False)
print("delete_ratio > ~0.1 is a good trigger to rewrite_data_files / rewrite_position_deletes.")


# ## F. Manifest health
#
# Iceberg plans queries by reading **manifest files**. Many small manifests slow
# down planning. The `.manifests` metadata table exposes per-manifest file/row
# counts so you can decide whether to `rewrite_manifests`.


for t in TABLES:
    print(f"\n=== {t} — manifest health ===")
    analytics.manifest_health(t).show(truncate=False)
print("Low avg_files_per_manifest across many manifests => candidate for rewrite_manifests.")


# ## G. Remediation playbook — auto-generated recommendations
#
# Turn the metrics above into a prioritized, copy-pasteable action list. These
# are **Spark `CALL` procedures** against the Glue catalog's Iceberg system
# namespace — review before running in production.


playbook = analytics.remediation_playbook()
if playbook is not None:
    playbook.show(50, truncate=False)
else:
    print("No remediation actions triggered.")


# ### (Optional) Execute a remediation — example
#
# Uncomment to compact the micro-file table and watch the file count drop. Re-run
# section **A** afterwards to confirm.


# spark.sql(f"""
#   CALL {CATALOG}.system.rewrite_data_files(
#     table => '{DB}.clickstream_micro',
#     options => map('target-file-size-bytes','134217728','min-input-files','5'))
# """).show(truncate=False)


# ## H. Agentic advisory (Amazon Bedrock + Claude)  🤖
#
# An optional **agentic layer** that turns the raw metrics above into
# plain-language insight. It runs a Bedrock **Converse tool-use loop** with Claude
# and gives it four tools over your tables:
#
# | tool | does |
# |------|------|
# | `get_metrics(table?)` | read the structured metrics (sections A–G) — *read-only* |
# | `lookup_best_practice(topic)` | retrieve **cited** Iceberg/AWS guidance from a local pack |
# | `propose_remediation(table)` | candidate `CALL` procedures — *does not execute* |
# | `execute_remediation(table, action)` | run a `CALL` — **gated behind your explicit approval** |
#
# Three things you can ask for:
# * **`summarize`** — plain summary of table state
# * **`advise`** — issues + advisory, each tied to a metric and a cited best practice
# * **`remediate`** — propose fixes, then **act yourself** (copy the command) **or**
#   let the agent run an action *you have approved*
#
# > **Safety:** the agent can never mutate a table on its own. `execute_remediation`
# > refuses unless you set `ADVISOR_ALLOW_EXECUTE = True` **and** add the specific
# > `table:action` to `APPROVED_ACTIONS`. Only table *metadata* is sent to Bedrock,
# > never row data. Requires `bedrock:InvokeModel` on the notebook role (already
# > granted for the sample model tiers) and `--additional-python-modules` is not
# > needed — `boto3` is built in.


# ---- load the advisor module + cited knowledge pack from the project bucket -
import boto3, json, importlib.util, tempfile, os

# Set by the deployer from the EnableAgentic parameter. When False, the advisor
# assets are NOT present in the bucket, so this whole section no-ops cleanly
# instead of erroring on a missing object / undefined module.
AGENTIC_ENABLED   = {{AGENTIC_ENABLED_PY}}

# The advisor module + best-practice pack live in the SEED bucket (code), not
# the warehouse bucket (data-only). This s3:// base is baked in at deploy time.
ADVISOR_S3_PREFIX = "{{CODE_S3_BASE}}/agentic"
BEDROCK_MODEL     = "{{BEDROCK_MODEL}}"   # Nova Lite/Micro or Claude profiles also work
BEDROCK_REGION    = "{{REGION}}"

iceberg_advisor = None
BEST_PRACTICES  = None
_s3 = boto3.client("s3")
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


# ---- wire the agent's tools to LIVE Spark metadata --------------------------
# The three callables the advisor needs (metrics_provider, remediation_proposer,
# remediation_executor) are bound methods on the same `analytics` engine used by
# sections A–G, so the agent sees exactly the metrics you just explored.
metrics_provider     = analytics.metrics_provider
remediation_proposer = analytics.remediation_proposer
remediation_executor = analytics.remediation_executor

print("Tools wired to live Spark metrics + (gated) executor.")


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


# ### H.1 — Summarize


advisor = make_advisor()
if advisor:
    _ = advisor.summarize()


# ### H.2 — Advise (issues + cited recommendations)


advisor = make_advisor()
if advisor:
    _ = advisor.advise("clickstream_micro")


# ### H.3 — Remediate (propose; execute only what you approve)
#
# With the gate **closed** (default), the agent proposes commands for you to run
# yourself. To let it execute a specific action, set above e.g.
# `ADVISOR_ALLOW_EXECUTE = True` and
# `APPROVED_ACTIONS = {"clickstream_micro:compact"}`, re-run the gate cell, then run this.


advisor = make_advisor()
if advisor:
    _ = advisor.remediate("clickstream_micro")
    print("\nExecuted actions this session:", advisor.executed_log())


# ---
# **Next steps for the blog:** wire section A's `inventory` DataFrame to a
# scheduled Glue job that publishes the metrics to CloudWatch (one
# `put-metric-data` per table/metric), then alarm on `small_files` and
# `delete_ratio`. The partition-level outputs (section C) become the
# drill-down QuickSight dataset. The agentic advisory (section H) can run the
# same way headlessly — in a scheduled Glue job or Lambda — proposing fixes and
# (optionally, with approval) executing them.

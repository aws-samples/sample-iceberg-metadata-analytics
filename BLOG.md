# Beyond dashboards: interactive and agentic analytics for the Apache Iceberg metadata layer

*How to explore Iceberg table health in a Glue notebook, generate realistic test
tables on demand, and let a Bedrock-powered agent advise (and, with your
approval, remediate) — deployable to your own catalog or from scratch with one
CloudFormation stack.*

---

## Why the metadata layer

Apache Iceberg keeps a rich metadata layer — snapshots, manifests, manifest
lists, and per-file statistics — alongside your data in Amazon S3. That metadata
is what makes Iceberg fast and transactional, but it also accumulates the
operational debt that quietly degrades a lakehouse: millions of tiny files from
streaming ingestion, snapshots that never expire, skewed partitions, and
merge-on-read delete files that pile up until reads slow to a crawl.

The AWS Big Data blog
[*Monitoring Apache Iceberg metadata layer using AWS Lambda, AWS Glue, and Amazon
CloudWatch*](https://aws.amazon.com/blogs/big-data/monitoring-apache-iceberg-metadata-layer-using-aws-lambda-aws-glue-and-aws-cloudwatch/)
showed how to publish table-level metrics to CloudWatch dashboards. That's the
right foundation for *alerting*. This post is about the next two steps:

1. **Interactive, exploratory analytics** — a Glue notebook that goes *below* the
   table level into **per-partition** analytics, file-size distributions, skew,
   trickle-ingest fingerprinting, and merge-on-read pressure, with an
   auto-generated remediation playbook.
2. **An agentic advisory layer** — an Amazon Bedrock agent that reads those same
   metrics, explains them in plain language with **cited** Iceberg best
   practices, and proposes (or, behind an approval gate, runs) the right
   maintenance.

And because not everyone is starting from the same place, the whole thing
deploys two ways: **point it at your existing Iceberg catalog**, or **stand up a
complete sandbox from scratch** — generator included.

---

## What you get

| Component | What it does |
|-----------|--------------|
| **Analytics notebook** | A Glue 5.0 interactive notebook (sections A–H) that reads Iceberg metadata tables (`.files`, `.partitions`, `.snapshots`, `.manifests`, `.history`) — never the row data — so it's cheap to run on huge tables. |
| **Iceberg utility** | A config-driven Glue job that builds and mutates Iceberg tables from a declarative JSON spec: partitioned or not, large vs micro files, skew, trickle ingest, CRUD, MERGE. A reusable "Iceberg lab." |
| **Agentic advisory** | A Bedrock Converse tool-use loop (Claude) with four tools over the metrics; summarizes, advises with citations, and remediates behind a human-approval gate. |
| **CloudFormation stack** | One template, two modes — `NotebookOnly` (bring your own Iceberg) and `Full` (from scratch). |

---

## Part 1 — Analytics below the table level

Open the notebook in **AWS Glue Studio → ETL jobs** and run it top to bottom. It
auto-discovers every Iceberg table in the target database (or a table allow-list
you provide), then sweeps the metadata layer.

### A. Inventory

One dashboard row per table — the numbers you'd publish to CloudWatch on a
schedule:

```
+-----------------------+----------+-----------+------------+--------+-----------+---------+----------+
|table                  |data_files|small_files|delete_files|total_mb|avg_file_mb|snapshots|partitions|
+-----------------------+----------+-----------+------------+--------+-----------+---------+----------+
|web_events_large       |7         |0          |0           |470.4   |67.198     |1        |7         |
|clickstream_micro      |577       |577        |13          |1.7     |0.003      |50       |12        |
|customers_unpartitioned|3         |3          |0           |2.8     |0.931      |2        |NULL      |
+-----------------------+----------+-----------+------------+--------+-----------+---------+----------+
```

> **Tip that bites everyone:** in Glue 5.0 the `.files` metadata table includes
> *delete* files, not just data files. Filter `content = 0` for true data-file
> metrics, `content != 0` for delete metrics — otherwise delete files inflate
> your file counts.

### B–C. The differentiator: per-partition analytics and skew

Table-level metrics hide the real problem. `clickstream_micro` has 577 files
averaging **3 KB** — but *which* partitions, and *how skewed*?

```
clickstream_micro — skew
+------------+--------+--------+---------+-------+-------------------+
|n_partitions|min_rows|max_rows|mean_rows|skew_cv|hot_partition_share|
+------------+--------+--------+---------+-------+-------------------+
|12          |77      |7849    |1532.2   |1.527  |0.427              |
+------------+--------+--------+---------+-------+-------------------+
```

A `skew_cv` of 1.53 and a single partition holding **43%** of the data is the
kind of signal a table-level dashboard never surfaces. The notebook also emits a
flagged-for-compaction list: exactly which partitions to `OPTIMIZE`.

### D. Trickle-ingest fingerprint

```
clickstream_micro: 48 append commits, avg 12 files/commit, avg 2.9 KB/file
```

Low bytes-per-file + many commits is the unmistakable signature of streaming /
trickle ingestion — the root cause behind the small files.

### E–G. Delete pressure, manifest health, remediation playbook

Sections E–F quantify merge-on-read delete ratios and manifest fragmentation.
Section G turns it all into a prioritized, copy-pasteable set of `CALL`
procedures (`rewrite_data_files`, `expire_snapshots`, `rewrite_manifests`,
`rewrite_position_delete_files`).

> **Another gotcha:** the `CALL` procedures need the Iceberg SQL extension, which
> is a *static* Spark config. In an interactive session `--datalake-formats=iceberg`
> alone doesn't register it — set `spark.sql.extensions` in `%%configure` (and
> restart the kernel) or `CALL` fails to parse.

---

## Part 2 — Generating realistic test data (the Iceberg utility)

If you don't already have Iceberg tables exhibiting these pathologies, you can't
test your monitoring. The utility generates them from a declarative spec:

```json
{
  "op": "create_table",
  "table": "clickstream_micro",
  "partition": [{ "column": "country", "transform": "identity" }],
  "file_profile": "micro",
  "skew": { "column": "country", "distribution": "zipf" },
  "commits": 48,
  "rows_per_commit": 300,
  "table_properties": { "write.distribution-mode": "none" }
}
```

`file_profile: "micro"` plus many small `commits` reproduces the small-file
problem; `file_profile: "large"` with a big `target_file_size_mb` produces the
opposite. Other ops — `ingest`, `crud`, `merge` — let you age a table with
appends, row-level deletes/updates, and upserts. The built-in `demo` preset
recreates the three reference tables used throughout this post.

---

## Part 3 — The agentic advisory layer

Dashboards tell you *what*. An agent can tell you *what it means and what to do*.
The advisory layer runs an Amazon Bedrock **Converse** tool-use loop with Claude
and gives it four tools over the metrics:

- `get_metrics(table?)` — read the structured metrics (read-only)
- `lookup_best_practice(topic)` — retrieve **cited** Iceberg/AWS guidance
- `propose_remediation(table)` — candidate `CALL` procedures (no execution)
- `execute_remediation(table, action)` — run a `CALL` — **gated**

Three things you can ask for — `summarize`, `advise`, `remediate`. Asking the
agent to advise on `clickstream_micro` produces something like:

> **HIGH — Severe small-file problem.** 577 data files averaging 3 KB (170,000×
> below the 512 MB target). Run `rewrite_data_files`… *(cites
> iceberg.apache.org/docs/latest/maintenance and AWS Prescriptive Guidance)*
>
> **MEDIUM — Partition skew.** `hot_partition_share` 0.43 — reassess the
> partition spec with a `bucket` transform; use partition evolution rather than
> rebuilding…

### Safety: the agent cannot touch your tables without permission

`execute_remediation` is **fail-closed**. It refuses unless you explicitly set
`ADVISOR_ALLOW_EXECUTE = True` *and* add the specific `table:action` to
`APPROVED_ACTIONS`. With the gate closed (the default), the agent only proposes
commands for you to run yourself. Only table **metadata** — counts, sizes,
partition values, snapshot summaries — is sent to Bedrock; never row data, and
partition values can be redacted if they're sensitive.

The best-practice knowledge pack is **curated and cited** — sourced from the
Apache Iceberg docs and AWS Prescriptive Guidance, baked into the artifact so the
agent retrieves it locally with no runtime web egress (clean for security
review, and reproducible).

---

## Part 4 — Deploy it your way

One CloudFormation template, two modes:

### Bring your own Iceberg (NotebookOnly)

You already have an S3 warehouse and a Glue database. The stack creates only an
IAM role + the notebook job + the agentic assets, pointed at *your* catalog. No
data is created; your bucket and tables are never modified.

```bash
./package_and_deploy.sh notebook-only \
  --region us-east-1 --prefix icebergnb --artifact-bucket <seed-bucket> \
  --warehouse-bucket <your-iceberg-bucket> --glue-db <your_glue_db>
```

### From scratch (Full)

No Iceberg yet? The stack provisions a bucket, Glue database, IAM role, the
generator and utility jobs, the notebook, and the agentic layer — and (optionally)
auto-runs the generator so you have queryable tables the moment the stack
finishes.

```bash
./package_and_deploy.sh full \
  --region us-east-1 --prefix icebergmeta --artifact-bucket <seed-bucket>
```

CloudFormation can't substitute values *inside* an S3 object, so a small Lambda
custom resource renders the notebook and scripts with your bucket/database/region
at deploy time, and (in Full mode) drains the created bucket on stack delete.

---

## Security and least privilege

Every IAM grant is scoped to the *effective* bucket and database — yours in
NotebookOnly, the created ones in Full. The Glue role gets interactive-session
actions on `session/*`, a self-only `iam:PassRole` to Glue, `/aws-glue/*` logs,
optional KMS decrypt for an SSE-KMS bucket, and (if the agentic layer is enabled)
`bedrock:InvokeModel` scoped to the chosen model's inference profile. No
AWS-managed broad policies. The created S3 bucket blocks public access, enforces
TLS, and is encrypted and versioned.

---

## Wrapping up

The metadata layer is where lakehouse health lives — and you can do a lot more
with it than alert on thresholds. An interactive notebook turns it into
exploratory analytics down to the partition level; a generator lets you
reproduce the pathologies you want to study; and an agentic layer turns the
numbers into cited advice and, with your approval, action.

Everything in this post is in the companion repository, deployable to your own
catalog or from scratch. *(Repo link / architecture diagram / screenshots to be
added.)*

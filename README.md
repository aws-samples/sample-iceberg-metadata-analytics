# Apache Iceberg Metadata-Layer Analytics — Glue Notebook Artifact

A reusable, blog-ready artifact for **analyzing the Apache Iceberg metadata
layer** with an AWS Glue interactive notebook. It extends the table-level
monitoring shown in the AWS Big Data blog
[*Monitoring Apache Iceberg metadata layer*](https://aws.amazon.com/blogs/big-data/monitoring-apache-iceberg-metadata-layer-using-aws-lambda-aws-glue-and-aws-cloudwatch/)
with the analytics that reference is missing:

- **Partition-level analytics** — files-per-partition, per-partition file size,
  fragmentation, and **partition skew** (which partitions are hot).
- **Small-file vs large-file analytics** — file-size histograms and an
  auto-generated list of exactly which partitions to `OPTIMIZE`.
- **Trickle-ingest fingerprinting** — snapshot/commit cadence and bytes-per-file
  per commit, the root-cause signal for small files.
- **Merge-on-read delete pressure** and **manifest health**.
- An auto-generated **remediation playbook** (`rewrite_data_files`,
  `expire_snapshots`, `rewrite_manifests`).
- An optional **agentic advisory layer** (Amazon Bedrock + Claude) that
  summarizes state, gives **cited** recommendations, and proposes/executes
  remediation behind a human-approval gate (section H of the notebook).

It also ships a **config-driven Iceberg utility** (`utility/`) — a reusable
"lab generator" that builds and mutates Iceberg tables from a declarative JSON
spec (partitioned or not, large/micro files, skew, trickle ingest, CRUD,
merge) so readers can reproduce the demo or create their own test tables.

**Region:** the shippable CloudFormation deploy (`solution/`) is **region-agnostic**
— it uses `AWS::Region`/`AWS::AccountId` intrinsics throughout, so you pass
`--region <any-region>` and it deploys there (verified end-to-end in `eu-west-1`).
The one exception is the optional **agentic advisory layer (section H)**, which is
**US-region-only as shipped** (it defaults to a `us.` Bedrock inference profile and
the IAM grant is scoped to US foundation-model ARNs); outside the US, deploy with
`--no-agentic` and section H no-ops cleanly. The root-level `scripts/` helpers are
hardcoded to `us-west-2` and are **not** part of the shippable solution. All IAM is
least-privilege.

---

## Repository layout

```
iceberg-meta-analytics/
├── README.md                                  ← you are here
│
├── solution/                                  ← THE SHIPPABLE SOLUTION (this is what deploys)
│   ├── deploy/                                ← ASK 5: CloudFormation repo deployment
│   │   ├── iceberg-meta-analytics.yaml        ← one template, Full | NotebookOnly modes
│   │   ├── package_and_deploy.sh              ← stage artifacts to a seed bucket + deploy
│   │   ├── lambda/render_artifacts.py         ← custom resource: render {{sentinels}} per-customer
│   │   └── README.md                          ← deploy guide (two personas)
│   ├── notebook/
│   │   ├── build_notebook.py                  ← generates the notebook (readable, diff-friendly source)
│   │   └── iceberg_metadata_analytics.template.{ipynb,py}  ← {{SENTINEL}} variants the CFN Lambda renders
│   ├── utility/                               ← ASK 2: config-driven Iceberg "lab" generator
│   │   ├── iceberg_utility.py                 ← spec-driven Glue job (create/ingest/crud/merge)
│   │   ├── run_utility_job.sh                 ← create/update + run the iceberg-utility job
│   │   ├── specs/{demo,example_custom}.json   ← sample specs (demo preset = the 3 reference tables)
│   │   └── README.md
│   └── agentic/                               ← ASK 3: Bedrock agentic advisory layer
│       ├── iceberg_advisor.py                 ← Bedrock Converse tool-use loop (4 tools, gated execute)
│       ├── best_practices.json                ← cited Iceberg/AWS knowledge pack (deep-research verified)
│       └── README.md
│
│   ── manual helper path (account-agnostic; alternative to the CFN deploy) ──
├── iam/
│   ├── glue-trust-policy.template.json        ← Glue service trust (rendered per account/region)
│   └── glue-permissions-policy.template.json  ← least-privilege permissions (one bucket, one DB)
└── scripts/
    ├── _common.sh / _common.py                ← shared account/region resolution (env + STS)
    ├── 00_provision_base.sh                   ← S3 bucket + IAM role + Glue DB (idempotent)
    ├── generate_iceberg_tables.py             ← Glue 5.0 Spark job: builds the 3 sample tables
    ├── 01_run_datagen_job.sh                  ← creates/updates & runs the data-gen Glue job
    ├── 02_validate_athena.sh                  ← proves the tables are queryable from Athena
    ├── 03_validate_notebook_in_session.py     ← probes notebook metadata queries in a live session
    ├── 04_run_notebook_cells.py               ← runs EVERY notebook code cell in a live session
    ├── 05_register_notebook.sh                ← registers the notebook as a Glue Studio NOTEBOOK job
    ├── 06_probe_call_extension.py             ← proves the Iceberg CALL extension parses
    └── 99_teardown.sh                         ← removes all created resources
```

> **`solution/` is the primary deliverable** — a self-contained CloudFormation
> deploy (`solution/deploy/`) for any account/region. The root-level `scripts/` +
> `iam/` are an **alternative manual path** (provision → generate → validate via
> AWS CLI) that is now also account-agnostic: account id comes from your
> credentials, everything else from env vars with defaults (`scripts/_common.sh`).
> They remain **US-region-focused** for the agentic pieces and are best for
> step-by-step exploration; for a one-shot deploy use the CloudFormation path.

## Two ways to consume this (ASK 4)

- **Bring your own Iceberg** (you already have an S3 warehouse + Glue DB):
  deploy in **NotebookOnly** mode — the notebook + agentic layer point at your
  catalog and auto-discover your tables. You don't need the generator.
- **From scratch** (no Iceberg yet): deploy in **Full** mode — the stack creates
  the bucket + Glue DB + IAM, generates demo tables, and wires up the notebook +
  agentic layer so you can test the whole platform immediately.

Both are one CloudFormation deploy — see `solution/deploy/README.md`.

The notebook is built from `solution/notebook/build_notebook.py`, which emits **two
variants**: an account-specific `.ipynb` (real values, used for in-account
validation) and `--template` `.template.{ipynb,py}` files with `{{SENTINEL}}`
tokens that the CFN Lambda substitutes per customer.

---

## Base infrastructure (created by `00_provision_base.sh`)

| Resource | Name | Notes |
|----------|------|-------|
| S3 bucket | `iceberg-meta-analytics-<account-id>-<region>` | versioning, SSE-S3, public-access blocked, TLS-only bucket policy |
| Glue database | `iceberg_meta_analytics` | warehouse at `s3://<bucket>/warehouse/` |
| IAM role | `IcebergMetaAnalytics-GlueRole` | inline least-privilege policy, **no** AWS-managed broad policies |

> These helpers are **account-agnostic**: account id is discovered from your
> credentials (`aws sts get-caller-identity`) and everything else resolves from
> env vars with defaults — override `ACCOUNT_ID`, `REGION`, `WAREHOUSE_BUCKET`,
> `GLUE_DB`, or `ROLE_NAME` (see `scripts/_common.sh`). The `iam/*.template.json`
> policies carry `__ACCOUNT_ID__`/`__REGION__` sentinels rendered at apply time.

**Least-privilege IAM** — the role can touch only:
- the one data bucket (objects + list), nothing else in S3;
- Glue catalog actions scoped to `database/iceberg_meta_analytics` and its
  tables (no `glue:*`, no other databases);
- Glue **interactive-session** actions (`CreateSession`, `RunStatement`,
  `TagResource`, …) scoped to `session/*` in this account/region — required
  because the Glue Studio notebook kernel creates its session *as this role*;
- `iam:PassRole` on **its own ARN only**, conditioned to
  `glue.amazonaws.com` (the notebook passes the role to its session);
- `/aws-glue/*` CloudWatch Log groups for job/session logs.

The trust policy allows `sts:AssumeRole` + `sts:TagSession` (the notebook
tags its session) by the Glue service **in this account and region** via
`aws:SourceAccount` / `aws:SourceArn` conditions (confused-deputy hardening).

> The interactive-session + pass-role permissions were added one at a time,
> each in response to a specific `AccessDenied` from the Glue Studio notebook
> kernel, and verified with the IAM policy simulator — so the policy grants
> exactly what the notebook exercises and nothing more.

---

## The three sample tables

Built by the `generate_iceberg_tables.py` Glue job. Verified profiles:

| Table | Partitioned? | Files | Avg file size | Snapshots | Purpose |
|-------|--------------|-------|---------------|-----------|---------|
| `web_events_large` | **Yes** (`event_date`) | ~7 | **~67 MB** | 1 | few large files / partition |
| `clickstream_micro` | **Yes** (`country`) | ~577 (+13 delete) | **~3 KB** | ~50 | many micro files / partition, partition **skew**, trickle ingest, MoR deletes |
| `customers_unpartitioned` | **No** | ~3 | ~950 KB | 2 | unpartitioned baseline |

`clickstream_micro` is deliberately skewed (e.g. `US` ≈ 7,800 rows vs `NL` ≈ 80)
and built from ~48 tiny "trickle" commits so every partition accumulates many
KB-scale files — the canonical small-file scenario the notebook diagnoses.

---

## How to run

> Prereq: an identity that can create the IAM role / bucket / Glue DB
> (the provisioning step). The Glue **job/session** itself uses only the
> least-privilege `IcebergMetaAnalytics-GlueRole`.

```bash
cd iceberg-meta-analytics/scripts

# Account is auto-detected from your AWS credentials; region defaults to us-west-2.
# To target a different region (or override anything): export REGION=eu-west-1

# 1. base infra (idempotent)
./00_provision_base.sh

# 2. generate the three Iceberg tables (~9 min on 10x G.1X)
./01_run_datagen_job.sh

# 3. (optional) prove Athena can query them
./02_validate_athena.sh

# 4. (optional) prove every notebook cell runs in a live Glue session
#    (builds the account-specific notebook into scripts/.local/, gitignored)
python3 04_run_notebook_cells.py
```

The validator builds the account-specific notebook (real bucket/DB/region baked
in) into `scripts/.local/notebook/` — a local, gitignored artifact. To open it in
**AWS Glue Studio → Notebooks** (or Jupyter with the AWS Glue interactive-sessions
kernel), use that rendered copy, set the notebook's IAM role to
`IcebergMetaAnalytics-GlueRole`, and run top to bottom.

### Querying from Athena instead

The tables are standard Glue-catalog Iceberg tables, so Athena (engine v3) can
query both data and metadata:

```sql
-- data
SELECT country, COUNT(*) FROM clickstream_micro GROUP BY country;
-- metadata layer (Athena $-suffixed metadata tables)
SELECT COUNT(*) AS n_files, AVG(file_size_in_bytes)/1024 AS avg_kb
FROM "clickstream_micro$files";
SELECT "partition".country, record_count, file_count
FROM "clickstream_micro$partitions" ORDER BY file_count DESC;
```

> Note: Athena exposes `$files`, `$partitions`, `$snapshots`, `$manifests`,
> `$history`. The richer `.all_files` / `.all_data_files` tables are
> Spark-only — which is one reason the notebook does the deep analysis in Glue.

---

## Notebook sections

| Section | What it produces |
|---------|------------------|
| A. Inventory & layout | one dashboard row per table (files, bytes, snapshots, partitions, deletes) |
| B. File-size distribution | size-bucket histogram per table (the small-file detector) |
| C. **Partition-level analytics** | per-partition files/rows/bytes, skew (CV + hot-partition share), and a flagged-for-compaction list |
| D. Snapshot & commit history | commit timeline + trickle-ingest fingerprint |
| E. Delete-file (MoR) pressure | data vs position/equality delete files, delete ratio |
| F. Manifest health | manifest count, files-per-manifest |
| G. Remediation playbook | auto-generated, prioritized `CALL` procedures |

---

## Teardown

```bash
./scripts/99_teardown.sh        # add --yes to skip the confirmation prompt
```

Removes the Glue job, tables, database, IAM role/policy, and empties + deletes
the S3 bucket.

---

## Security-review notes

- No credentials or account-specific secrets in code; the account id appears
  only in resource names/ARNs (required for scoping).
- IAM is least-privilege and resource-scoped; trust policy is confused-deputy
  hardened.
- S3: encrypted (SSE-S3 + bucket keys), versioned, public access fully blocked,
  TLS enforced via bucket policy.
- The data-gen job intentionally omits `--enable-metrics` /
  `--enable-observability-metrics` so the role does **not** need
  `cloudwatch:PutMetricData`. If you wire section A to CloudWatch (the suggested
  "next step"), add a narrowly-scoped `PutMetricData` statement at that time.
```

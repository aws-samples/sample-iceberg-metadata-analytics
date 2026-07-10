# Deploy — CloudFormation

One CloudFormation template (`iceberg-meta-analytics.yaml`) deploys the whole
solution in either of two modes, matching the two customer personas.

## Two personas, one template

| | **Full** (Persona B — from scratch) | **NotebookOnly** (Persona A — bring your own Iceberg) |
|---|---|---|
| You have | nothing | an S3 warehouse + Glue database with Iceberg tables |
| Stack creates | bucket + Glue DB + IAM role + **generator** + notebook + agentic | IAM role + notebook + agentic, pointed at **your** catalog |
| You're asked for | region (the stack's), a `ResourcePrefix`, demo on/off | your warehouse bucket, Glue DB, optional table list, optional KMS key |
| Demo data | optional auto-run of the generator (`CreateDemoData`) | none — analyzes your real tables |

The notebook **auto-discovers** every Iceberg table in the target database
(or just the `TableAllowList` if you provide one), so it works whether you have
3 tables or 300.

## How parameters drive the modes

A single `DeploymentMode` parameter (`Full` | `NotebookOnly`) plus CloudFormation
`Conditions` decide which resources are created. The console groups parameters so
each persona knows which to fill (see `Metadata.AWS::CloudFormation::Interface`):

- **NotebookOnly** uses `ExistingWarehouseBucket`, `ExistingGlueDatabase`,
  `TableAllowList`, `ExistingKmsKeyArn`.
- **Full** uses `ResourcePrefix` and `CreateDemoData`. `ResourcePrefix` must be
  lowercase alphanumeric, 2–40 chars, **no hyphens** (it becomes the `<prefix>_db`
  Glue database name, which must be a valid SQL identifier) — e.g. `icebergmeta`.
- Both use `EnableAgentic`, `BedrockModelId`.

## Why a Lambda custom resource

The notebook and scripts are shipped as **templates** with `{{SENTINEL}}` tokens
(`{{WAREHOUSE_BUCKET}}`, `{{CODE_S3_BASE}}`, `{{GLUE_DB}}`, `{{REGION}}`,
`{{BEDROCK_MODEL}}`, `{{TABLE_ALLOWLIST_PY}}`). CloudFormation can't substitute
*inside* an S3 object, so a small Python Lambda (`lambda/render_artifacts.py`)
renders them with your values and writes the rendered code **back into the seed
bucket** under a per-deployment prefix (`<ArtifactPrefix>/rendered/<prefix>/…`)
at the exact paths the Glue jobs expect. It also (optionally) starts the
generator in Full mode, and cleans up the rendered objects + Glue scratch on
stack delete.

**Bucket split — the warehouse bucket is data-only.** All *code* (notebook,
helper module, utility, advisor pack, advisory runner) and Glue scratch
(`glue-temp/`) live in the **seed bucket**. The **warehouse bucket** holds only
Iceberg data under `warehouse/` plus agent-authored `advisory-reports/`. In
NotebookOnly mode this means the deploy never writes code into your existing
Iceberg bucket.

## Deploy

The helper stages artifacts to an S3 "seed" bucket you own, then deploys:

```bash
# Persona B — Full, from scratch (minimal inputs):
./package_and_deploy.sh full \
  --region us-west-2 --prefix icebergmeta --artifact-bucket <your-seed-bucket>

# Persona A — NotebookOnly, against an existing Iceberg catalog:
./package_and_deploy.sh notebook-only \
  --region us-west-2 --prefix icebergmeta --artifact-bucket <your-seed-bucket> \
  --warehouse-bucket <your-iceberg-bucket> --glue-db <your_glue_db> \
  [--tables "orders,events"] [--kms-arn arn:aws:kms:...]

# Flags: --no-agentic (skip Bedrock), --no-demo (Full: don't auto-run generator),
#        --bedrock-model us.amazon.nova-lite-v1:0, --stack-name <name>
```

When the stack finishes, open **AWS Glue Studio → ETL jobs → `<prefix>-notebook`**
and run top to bottom. In Full mode with `CreateDemoData=true`, the demo tables
already exist; in NotebookOnly mode it analyzes your tables immediately.

## Headless advisory runner (agentic; user-driven, post-deploy)

When `EnableAgentic=true`, the stack also creates a Glue job `<prefix>-advisory`
that runs the **same** Bedrock advisor as notebook section H, headlessly. It is
**created but never auto-run** — you trigger it on demand, and it writes a
Markdown report (authored by the agent) to `s3://<warehouse-bucket>/advisory-reports/`.

```bash
# read-only metrics, no agent / no Bedrock call:
aws glue start-job-run --job-name <prefix>-advisory --arguments '{"--mode":"analysis"}'

# agent advice — issues + cited recommendations (read-only):
aws glue start-job-run --job-name <prefix>-advisory --arguments '{"--mode":"advise"}'

# agent remediation — executes ONLY the approved table:action pairs (empty = propose only):
aws glue start-job-run --job-name <prefix>-advisory \
  --arguments '{"--mode":"remediate","--approved_actions":"clickstream_micro:compact"}'
```

Optional args: `--tables "a,b"` (scope; default all), `--report_prefix` (default
`advisory-reports`). **Safety:** `remediate` mutates tables only for the explicit
`table:action` allow-list you pass; with none, the agent proposes commands but
executes nothing — identical to the notebook's closed approval gate.

## What gets created

| Resource | Full | NotebookOnly |
|----------|:----:|:------------:|
| S3 warehouse bucket (versioned, SSE, TLS-only) | ✓ | — (uses yours) |
| Glue database | ✓ | — (uses yours) |
| IAM role (least-privilege, scoped to the effective bucket+DB) | ✓ | ✓ |
| `<prefix>-notebook` (Glue NOTEBOOK job) | ✓ | ✓ |
| `<prefix>-datagen` (demo-data generator) | ✓ | — |
| Bedrock invoke permission + advisor assets (if `EnableAgentic`) | ✓ | ✓ |
| `<prefix>-advisory` (headless runner, if `EnableAgentic`) | ✓ | ✓ |

## Teardown

`aws cloudformation delete-stack --stack-name <name>`. The render Lambda removes
its rendered code + Glue scratch (under `rendered/<prefix>/` and
`glue-temp/<prefix>/` in the seed bucket) on delete. In **Full** mode the data
bucket it created is emptied/removed with the stack; in **NotebookOnly** mode
**your** bucket and tables are never touched.

## IAM (security review)

Least-privilege throughout: the Glue role is scoped to the **effective** bucket
and database only (yours in NotebookOnly, the created ones in Full), plus
interactive-session actions on `session/*`, a self-only `iam:PassRole` to Glue,
`/aws-glue/*` logs, optional KMS-decrypt for an SSE-KMS bucket, and optional
`bedrock:InvokeModel` scoped to the chosen model's inference-profile +
foundation-model ARNs. No AWS-managed broad policies. The render Lambda's role
can only read the artifact bucket, write rendered code + scratch under this
deployment's own `rendered/<prefix>/` and `glue-temp/<prefix>/` seed-bucket
prefixes, drain the created data bucket on delete (Full), and start
`<prefix>-datagen` Glue job runs.

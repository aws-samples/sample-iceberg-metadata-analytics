# Agentic Advisory Layer — Amazon Bedrock + Claude

An optional agentic layer that turns the Iceberg metadata metrics (notebook
sections A–G) into plain-language insight, **cited** recommendations, and
human-approved remediation. It runs a Bedrock **Converse tool-use loop** with
Claude.

## What it does

Three actions:
- **summarize** — plain summary of table state.
- **advise** — highlight issues, each tied to a metric and a **cited** best practice.
- **remediate** — propose `CALL` procedures; you **run them yourself** or let the
  agent execute an action **you have approved**.

Four tools the model can call:
| tool | effect |
|------|--------|
| `get_metrics(table?)` | read structured metrics — read-only |
| `lookup_best_practice(topic)` | retrieve cited guidance from `best_practices.json` |
| `propose_remediation(table)` | candidate maintenance commands — no execution |
| `execute_remediation(table, action)` | run a `CALL` — **gated** |

## Safety model (important for review)

- **No autonomous mutation.** `execute_remediation` refuses unless
  `AdvisorConfig.allow_execute=True` **and** the specific `"table:action"` is in
  `approved_actions` (the executed log stays empty when nothing is approved).
- **Metadata only.** Only counts/sizes/partition values/snapshot summaries are
  sent to Bedrock — never row data. `AdvisorConfig.redact_partitions=True` masks
  partition *values* if they could be sensitive.
- **Least-privilege.** Needs `bedrock:InvokeModel` scoped to the specific model
  inference-profile + foundation-model ARNs (added to the project role).
- **No runtime web egress.** Best practices come from a local, version-controlled
  pack, not live fetches.

## Not Glue-only

`iceberg_advisor.py` is plain `boto3` — no Spark/Glue dependency. It's decoupled
via injected callables (`metrics_provider`, `remediation_proposer`,
`remediation_executor`), so the same advisor runs:
- **inside the Glue notebook** (section H) — metrics from live Spark, executor runs `spark.sql("CALL …")`;
- **standalone / Lambda / EC2** — metrics from Athena `$files`/`$partitions` or a saved JSON, executor proposes-only or triggers an Athena `OPTIMIZE`/`VACUUM`;
- **tests** — stub metrics.

## Files

```
agentic/
├── iceberg_advisor.py        ← the Bedrock Converse tool-use loop + 4 tools
├── best_practices.json       ← cited Iceberg/AWS knowledge pack (deep-research verified)
└── README.md
```

## Knowledge pack

`best_practices.json` has one entry per topic (small_files, compaction_strategy,
partitioning, snapshots, orphan_files, manifests, merge_on_read, format_v2).
Each entry carries: `recommendation`, the Iceberg `property`/`procedure`, a
`threshold`/trigger, and `sources` (URLs to cite). All defaults/thresholds were
reconciled against a 5-angle deep-research pass — 17 primary sources, 25 claims,
all 3-0 adversarially verified vs Apache Iceberg docs / AWS Athena docs / AWS
Prescriptive Guidance.

## Model

Default `us.amazon.nova-pro-v1:0` (Bedrock inference profile, us-west-2). The
Converse tool-use loop is model-agnostic, so `us.amazon.nova-lite-v1:0` /
`us.amazon.nova-micro-v1:0` (cheaper Nova tiers) and the Anthropic Claude
profiles (`us.anthropic.claude-opus-4-8` / `-sonnet-4-6` / `-haiku-4-5-...`) all
work — the role's `bedrock:InvokeModel` scope grants both vendors' FMs in the
three US regions.
```

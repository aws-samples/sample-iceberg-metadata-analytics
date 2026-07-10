# Iceberg Utility — a config-driven Iceberg "lab" generator

A reusable AWS Glue job that builds and mutates Apache Iceberg tables from a
declarative **JSON spec**, so you can spin up tables with *controlled*
characteristics — partitioned or not, large vs micro files, partition skew,
trickle ingest, merge-on-read deletes — to test metadata monitoring and
maintenance against. It generalizes the fixed three-table demo generator into a
parameterized tool.

> Pairs with the **Iceberg Metadata Analytics notebook** in the parent folder:
> use this utility to create tables, then point the notebook (and the agentic
> advisory layer) at them.

## Why a spec instead of flat arguments?

Flat `--key value` job args can't express "create table A partitioned by
`days(ts)` with these 6 columns, trickle 48 commits in, then `DELETE WHERE
status='cancelled'` and upsert 50k rows". A declarative spec can — and it's
reproducible and reviewable, which is what a published artifact needs.

## Operations (the levers)

| op | what it does | key levers |
|----|--------------|-----------|
| `create_table` | create + populate a table | `partition` (cols + transform, or `null` for unpartitioned), `file_profile` (`large`\|`micro`\|`medium`), `target_file_size_mb`, `rows`, `commits`+`rows_per_commit` (commits>1 ⇒ trickle), `skew`, `schema` (per-column generators), `table_properties` |
| `ingest` | append N rows to an existing table | `rows` (bulk) or `commits`+`rows_per_commit` (trickle) |
| `crud` | run parameterized DML | `statements` (`DELETE`/`UPDATE`, `{table}` placeholder) |
| `merge` | MoR upsert on a key | `key`, `source_rows`, `key_overlap` (fraction that updates vs inserts) |

### Partition transforms
`identity`, `days`, `months`, `years`, `hours`, `bucket` (`{"transform":"bucket","n":16}`),
`truncate` (`{"transform":"truncate","n":10}`). `partition: null` ⇒ unpartitioned.

### File profiles
- **`large`** — repartition by partition cols + big `target_file_size_mb` ⇒ few large files per partition.
- **`micro`** — `coalesce(1)` + fanout writer + many small trickle `commits` ⇒ many KB-scale files per partition. Set `write.distribution-mode=none` in `table_properties` so Iceberg doesn't coalesce the tiny commits.
- **`medium`** — `output_files` controls the file count for a single bulk commit.

### Column generators (`schema[].gen.kind`)
`sequence`, `int_mod`, `rand_int`, `rand_double`, `choice`, `template` (`{id}`, `{int_mod:N}`),
`uuid`, `now`, `ts_recent_days`, `date_sequence`, `date_random`, `skew_partition` (zipf), `null`.
For `ingest`/`merge` on existing tables, generators are auto-inferred from the catalog schema.

## Usage

```bash
cd iceberg-meta-analytics/utility

# Reproduce the 3 reference demo tables (same as the notebook expects):
./run_utility_job.sh --preset demo

# Run a custom spec from a file (uploaded to S3 automatically):
./run_utility_job.sh --spec specs/example_custom.json

# Or pass an inline JSON spec:
./run_utility_job.sh --config-inline '{"version":1,"operations":[ ... ]}'
```

The runner uploads `iceberg_utility.py` + the bundled `specs/` to the project
bucket, creates/updates the `iceberg-utility` Glue 5.0 job (least-privilege
`IcebergMetaAnalytics-GlueRole`), starts a run, and polls to completion. The job
prints a verification summary (rows / data_files / avg size / snapshots) per
table at the end.

## Files

```
utility/
├── iceberg_utility.py        ← the spec-driven Glue job (engine)
├── run_utility_job.sh        ← create/update + run the job
├── specs/
│   ├── demo.json             ← built-in 'demo' preset (the 3 reference tables)
│   └── example_custom.json   ← example: bucket + days partitions, ingest, merge, CRUD
└── README.md
```

## Verified

`specs/example_custom.json` ran end-to-end on Glue 5.0 (all four op types):
- `orders_bucketed` — `bucket(16, customer_id)`, 1.88M rows, 5 snapshots (create + ingest + merge + 2× CRUD), `DELETE` removed `cancelled` rows.
- `sensor_daily` — `days(reading_ts)`, micro profile, 10 trickle commits ⇒ ~1.8 KB avg files.

## Notes for security review

- The job only reads/writes the project data bucket and the
  `iceberg_meta_analytics` Glue database (same least-privilege role as the rest
  of the artifact — no new permissions).
- `crud`/`merge` statements come from the spec; treat the spec as trusted input
  (it is authored by the operator, same as a SQL script). The job does not build
  SQL from external/user data at runtime.
- `MERGE INTO` requires a deterministic source, so `merge` first materializes
  generated rows to a scratch Iceberg table (`<table>__merge_src`), runs the
  merge from storage, then drops the scratch table.
```

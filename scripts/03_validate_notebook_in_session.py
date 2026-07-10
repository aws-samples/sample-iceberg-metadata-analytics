#!/usr/bin/env python3
"""
03_validate_notebook_in_session.py

Spins up a REAL Glue interactive session (same role/runtime the notebook uses),
then runs the exact metadata queries the notebook relies on, so we prove every
cell executes before the user opens the notebook. Prints PASS/FAIL per probe.

Tears the session down at the end (always).
"""
import os
import sys
import time
import textwrap

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import REGION, ACCOUNT, ROLE_ARN, BUCKET, GLUE_DB as DB  # noqa: E402

CATALOG = "glue_catalog"
SESSION_ID = "iceberg-meta-analytics-validate"

glue = boto3.client("glue", region_name=REGION)

BOOTSTRAP = textwrap.dedent(f"""
    spark.conf.set("spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.catalog.{CATALOG}.warehouse", "s3://{BUCKET}/warehouse/")
    print("BOOTSTRAP_OK")
""")

def fq(t):
    return f"{CATALOG}.{DB}.{t}"

# Each probe mirrors a metadata access the notebook performs.
PROBES = [
    ("files cols", f"print(spark.table('{fq('clickstream_micro')}.files').columns)"),
    ("files content distinct",
     f"spark.sql(\"SELECT content, COUNT(*) c FROM {fq('clickstream_micro')}.files GROUP BY content\").show()"),
    ("all_files content distinct",
     f"spark.sql(\"SELECT content, COUNT(*) c FROM {fq('clickstream_micro')}.all_files GROUP BY content\").show()"),
    ("partitions cols", f"print(spark.table('{fq('clickstream_micro')}.partitions').columns)"),
    ("partitions struct query",
     f"spark.sql(\"SELECT partition, record_count, file_count, total_data_file_size_in_bytes FROM {fq('clickstream_micro')}.partitions ORDER BY file_count DESC LIMIT 3\").show(truncate=False)"),
    ("unpartitioned .partitions cols",
     f"print(spark.table('{fq('customers_unpartitioned')}.partitions').columns)"),
    ("snapshots+history join",
     f"spark.sql(\"SELECT h.made_current_at, s.operation, CAST(s.summary['added-data-files'] AS INT) adf FROM {fq('clickstream_micro')}.snapshots s JOIN {fq('clickstream_micro')}.history h ON s.snapshot_id=h.snapshot_id ORDER BY made_current_at DESC LIMIT 3\").show(truncate=False)"),
    ("manifests cols",
     f"print(spark.table('{fq('clickstream_micro')}.manifests').columns)"),
    ("manifests agg",
     f"spark.sql(\"SELECT COUNT(*) n, SUM(added_data_files_count+existing_data_files_count+deleted_data_files_count) tracked FROM {fq('clickstream_micro')}.manifests\").show()"),
    ("delete files via all_files",
     f"spark.sql(\"SELECT SUM(CASE WHEN content=1 THEN 1 ELSE 0 END) pos, SUM(CASE WHEN content=2 THEN 1 ELSE 0 END) eq FROM {fq('clickstream_micro')}.all_files\").show()"),
]


def wait_ready():
    print(">>> waiting for session READY ...")
    for _ in range(60):
        st = glue.get_session(Id=SESSION_ID)["Session"]["Status"]
        print(f"    status={st}")
        if st == "READY":
            return
        if st in ("FAILED", "STOPPED", "TIMEOUT"):
            raise RuntimeError(f"session entered {st}")
        time.sleep(10)
    raise RuntimeError("session never became READY")


def run(code, label):
    rid = glue.run_statement(SessionId=SESSION_ID, Code=code)["Id"]
    while True:
        stmt = glue.get_statement(SessionId=SESSION_ID, Id=rid)["Statement"]
        if stmt["State"] in ("AVAILABLE", "ERROR", "CANCELLED"):
            break
        time.sleep(3)
    out = stmt.get("Output", {})
    status = out.get("Status", stmt["State"])
    if status == "ok":
        text = out.get("Data", {}).get("TextPlain", "")
        print(f"  PASS  {label}")
        if text.strip():
            for line in text.strip().splitlines():
                print(f"        | {line}")
        return True
    else:
        err = out.get("ErrorName", "") + ": " + out.get("ErrorValue", "")
        print(f"  FAIL  {label}\n        {err}")
        return False


def main():
    # clean any prior session
    try:
        glue.delete_session(Id=SESSION_ID)
        time.sleep(3)
    except glue.exceptions.IllegalSessionStateException:
        pass
    except Exception:
        pass

    print(">>> creating Glue interactive session")
    glue.create_session(
        Id=SESSION_ID,
        Role=ROLE_ARN,
        Command={"Name": "glueetl", "PythonVersion": "3"},
        GlueVersion="5.0",
        WorkerType="G.1X",
        NumberOfWorkers=2,
        IdleTimeout=15,
        DefaultArguments={
            "--datalake-formats": "iceberg",
            "--enable-glue-datacatalog": "true",
        },
        Tags={"project": "iceberg-meta-analytics", "managed-by": "blog-artifact"},
    )
    ok = True
    try:
        wait_ready()
        if not run(BOOTSTRAP, "bootstrap catalog"):
            return 1
        print(">>> running notebook metadata probes")
        for label, code in PROBES:
            ok = run(code, label) and ok
    finally:
        print(">>> deleting session")
        try:
            glue.delete_session(Id=SESSION_ID)
        except Exception as e:
            print(f"    (delete warning: {e})")
    print("\n>>> RESULT:", "ALL PASS" if ok else "SOME FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

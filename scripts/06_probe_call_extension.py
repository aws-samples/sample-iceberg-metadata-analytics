#!/usr/bin/env python3
"""
06_probe_call_extension.py

Proves whether the Iceberg `CALL` procedure parses in a Glue interactive
session, and whether explicitly setting spark.sql.extensions via session args
fixes it. Uses a THROWAWAY table so the 3 demo tables are never touched.

Run twice via SESSION variant:
  MODE=baseline  -> only --datalake-formats=iceberg          (reproduce the bug)
  MODE=fixed     -> also --conf spark.sql.extensions=...      (verify the fix)
"""
import os
import sys
import time
import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import REGION, ACCOUNT, ROLE_ARN, BUCKET, GLUE_DB as DB  # noqa: E402

CATALOG = "glue_catalog"
MODE = os.environ.get("MODE", "fixed")
SESSION_ID = f"iceberg-meta-call-probe-{MODE}"

glue = boto3.client("glue", region_name=REGION)

EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

default_args = {
    "--datalake-formats": "iceberg",
    "--enable-glue-datacatalog": "true",
}
if MODE == "fixed":
    # Multiple confs are space-joined inside a single --conf value (Glue convention).
    default_args["--conf"] = (
        f"spark.sql.extensions={EXT}"
        f" --conf spark.sql.catalog.{CATALOG}=org.apache.iceberg.spark.SparkCatalog"
        f" --conf spark.sql.catalog.{CATALOG}.warehouse=s3://{BUCKET}/warehouse/"
        f" --conf spark.sql.catalog.{CATALOG}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
        f" --conf spark.sql.catalog.{CATALOG}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    )

BOOT = f"""
spark.conf.set("spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.{CATALOG}.warehouse", "s3://{BUCKET}/warehouse/")
try:
    print("EXTENSIONS =", spark.conf.get("spark.sql.extensions"))
except Exception as e:
    print("EXTENSIONS = <UNSET>", e)
"""

PROBE = f"""
t = "{CATALOG}.{DB}._call_probe"
spark.sql(f"DROP TABLE IF EXISTS {{t}} PURGE")
spark.sql(f"CREATE TABLE {{t}} (id bigint) USING iceberg")
spark.sql(f"INSERT INTO {{t}} VALUES (1)")
spark.sql(f"INSERT INTO {{t}} VALUES (2)")
try:
    res = spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{DB}._call_probe', options => map('min-input-files','2'))")
    res.show(truncate=False)
    print("CALL_RESULT = OK")
except Exception as e:
    print("CALL_RESULT = FAILED:", type(e).__name__, str(e)[:200])
finally:
    spark.sql(f"DROP TABLE IF EXISTS {{t}} PURGE")
"""


def wait_ready():
    for _ in range(60):
        st = glue.get_session(Id=SESSION_ID)["Session"]["Status"]
        print(f"  status={st}")
        if st == "READY":
            return
        if st in ("FAILED", "STOPPED", "TIMEOUT"):
            raise RuntimeError(f"session {st}")
        time.sleep(10)
    raise RuntimeError("never READY")


def run(code):
    rid = glue.run_statement(SessionId=SESSION_ID, Code=code)["Id"]
    while True:
        stmt = glue.get_statement(SessionId=SESSION_ID, Id=rid)["Statement"]
        if stmt["State"] in ("AVAILABLE", "ERROR", "CANCELLED"):
            break
        time.sleep(3)
    out = stmt.get("Output", {})
    if out.get("Status") == "ok":
        print(out.get("Data", {}).get("TextPlain", "").rstrip())
    else:
        print("STATEMENT ERROR:", out.get("ErrorName"), out.get("ErrorValue"))


def main():
    print(f">>> MODE={MODE}")
    try:
        glue.delete_session(Id=SESSION_ID); time.sleep(3)
    except Exception:
        pass
    glue.create_session(
        Id=SESSION_ID, Role=ROLE_ARN,
        Command={"Name": "glueetl", "PythonVersion": "3"},
        GlueVersion="5.0", WorkerType="G.1X", NumberOfWorkers=2, IdleTimeout=15,
        DefaultArguments=default_args,
        Tags={"project": "iceberg-meta-analytics", "managed-by": "blog-artifact"},
    )
    try:
        wait_ready()
        run(BOOT)
        run(PROBE)
    finally:
        try:
            glue.delete_session(Id=SESSION_ID)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

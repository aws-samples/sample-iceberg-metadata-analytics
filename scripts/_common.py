"""
_common.py  --  shared, account-AGNOSTIC config for the Python helper scripts.

Nothing is hardcoded to a particular AWS account: values resolve from the
environment and fall back to sensible defaults; the account id is discovered
from your current credentials via STS.

Override by exporting before running: ACCOUNT_ID, REGION, WAREHOUSE_BUCKET,
GLUE_DB, ROLE_NAME.
"""
import os

import boto3

REGION = os.environ.get("REGION", "us-west-2")
ACCOUNT = os.environ.get("ACCOUNT_ID") or \
    boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
GLUE_DB = os.environ.get("GLUE_DB", "iceberg_meta_analytics")
ROLE_NAME = os.environ.get("ROLE_NAME", "IcebergMetaAnalytics-GlueRole")
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/{ROLE_NAME}"
BUCKET = os.environ.get("WAREHOUSE_BUCKET",
                        f"iceberg-meta-analytics-{ACCOUNT}-{REGION}")

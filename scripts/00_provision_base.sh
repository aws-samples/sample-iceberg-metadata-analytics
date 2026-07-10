#!/usr/bin/env bash
#
# 00_provision_base.sh
#
# Idempotently provisions the base infrastructure for the Iceberg Metadata
# Analytics artifact in your account/region (defaults: us-west-2):
#   * S3 bucket  : iceberg-meta-analytics-<acct>-<region>  (versioning + SSE + public-access-block)
#   * Glue DB    : iceberg_meta_analytics
#   * IAM role   : IcebergMetaAnalytics-GlueRole  (least-privilege inline policy)
#
# Account/region resolve from your credentials + env (see _common.sh); override
# with ACCOUNT_ID / REGION / WAREHOUSE_BUCKET / GLUE_DB / ROLE_NAME.
# Safe to re-run: every step checks for existence before creating.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"
IAM_DIR="$(cd "${SCRIPT_DIR}/../iam" && pwd)"

log() { printf '\n>>> %s\n' "$1"; }

# ---------------------------------------------------------------------------
# 1. S3 bucket
# ---------------------------------------------------------------------------
log "S3 bucket: ${BUCKET}"
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "    already exists, skipping create"
else
  aws s3api create-bucket \
    --bucket "$BUCKET" \
    --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
  echo "    created"
fi

echo "    -> block public access"
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true >/dev/null

echo "    -> enable versioning"
aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Enabled >/dev/null

echo "    -> default SSE-S3 encryption"
aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}' >/dev/null

echo "    -> enforce TLS-only access (bucket policy)"
aws s3api put-bucket-policy --bucket "$BUCKET" --policy "$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyInsecureTransport",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::${BUCKET}",
        "arn:aws:s3:::${BUCKET}/*"
      ],
      "Condition": { "Bool": { "aws:SecureTransport": "false" } }
    }
  ]
}
JSON
)" >/dev/null

# ---------------------------------------------------------------------------
# 2. IAM role + least-privilege inline policy
# ---------------------------------------------------------------------------
log "IAM role: ${ROLE_NAME}"
# Render the account-agnostic IAM templates (sentinels -> this account/region).
TRUST_DOC="$(render_iam "${IAM_DIR}/glue-trust-policy.template.json")"
PERM_DOC="$(render_iam "${IAM_DIR}/glue-permissions-policy.template.json")"
if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "    already exists, updating trust policy"
  aws iam update-assume-role-policy --role-name "$ROLE_NAME" \
    --policy-document "$TRUST_DOC" >/dev/null
else
  aws iam create-role --role-name "$ROLE_NAME" \
    --assume-role-policy-document "$TRUST_DOC" \
    --description "Least-privilege role for Iceberg Metadata Analytics Glue jobs/sessions" \
    --tags Key=project,Value=iceberg-meta-analytics Key=managed-by,Value=blog-artifact >/dev/null
  echo "    created"
fi

echo "    -> put least-privilege inline policy: ${POLICY_NAME}"
aws iam put-role-policy --role-name "$ROLE_NAME" \
  --policy-name "$POLICY_NAME" \
  --policy-document "$PERM_DOC" >/dev/null

ROLE_ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)"

# ---------------------------------------------------------------------------
# 3. Glue database
# ---------------------------------------------------------------------------
log "Glue database: ${GLUE_DB}"
if aws glue get-database --name "$GLUE_DB" >/dev/null 2>&1; then
  echo "    already exists, skipping"
else
  aws glue create-database --database-input \
    "{\"Name\":\"${GLUE_DB}\",\"Description\":\"Iceberg Metadata Analytics blog artifact\",\"LocationUri\":\"s3://${BUCKET}/warehouse/\"}" >/dev/null
  echo "    created"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cat <<SUMMARY

=========================================================================
 Base infrastructure ready (region: ${REGION})
=========================================================================
  S3 bucket    : s3://${BUCKET}
  Warehouse    : s3://${BUCKET}/warehouse/
  Glue database: ${GLUE_DB}
  IAM role ARN : ${ROLE_ARN}
=========================================================================
SUMMARY

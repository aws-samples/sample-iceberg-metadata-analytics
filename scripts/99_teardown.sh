#!/usr/bin/env bash
#
# 99_teardown.sh   --  remove everything 00/01 created.
#
# Usage:  ./99_teardown.sh [--yes]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"
JOB_NAME="iceberg-meta-analytics-datagen"

if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Delete bucket s3://${BUCKET}, DB ${GLUE_DB}, role ${ROLE_NAME}, job ${JOB_NAME}? [y/N] " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "aborted"; exit 0; }
fi

echo ">>> delete Glue jobs (data-gen, notebook, utility)"
for j in "$JOB_NAME" iceberg-meta-analytics-notebook iceberg-utility; do
  aws glue delete-job --job-name "$j" >/dev/null 2>&1 || true
done

echo ">>> stop any lingering interactive sessions"
for sid in iceberg-meta-analytics-validate iceberg-meta-analytics-cells \
           iceberg-meta-call-probe-fixed iceberg-meta-call-probe-baseline; do
  aws glue delete-session --id "$sid" >/dev/null 2>&1 || true
done

echo ">>> delete Glue tables + database"
for t in web_events_large clickstream_micro customers_unpartitioned; do
  aws glue delete-table --database-name "$GLUE_DB" --name "$t" >/dev/null 2>&1 || true
done
aws glue delete-database --name "$GLUE_DB" >/dev/null 2>&1 || true

echo ">>> empty + delete S3 bucket (incl. versions)"
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  # delete all object versions + delete markers, then the bucket
  aws s3 rm "s3://${BUCKET}" --recursive >/dev/null 2>&1 || true
  versions=$(aws s3api list-object-versions --bucket "$BUCKET" \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{}')
  if [[ "$versions" != '{}' && "$versions" != '{"Objects":null}' ]]; then
    aws s3api delete-objects --bucket "$BUCKET" --delete "$versions" >/dev/null 2>&1 || true
  fi
  markers=$(aws s3api list-object-versions --bucket "$BUCKET" \
    --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{}')
  if [[ "$markers" != '{}' && "$markers" != '{"Objects":null}' ]]; then
    aws s3api delete-objects --bucket "$BUCKET" --delete "$markers" >/dev/null 2>&1 || true
  fi
  aws s3api delete-bucket --bucket "$BUCKET" >/dev/null 2>&1 || true
  echo "    bucket deleted"
else
  echo "    bucket already gone"
fi

echo ">>> delete IAM role + inline policy"
aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME" >/dev/null 2>&1 || true
aws iam delete-role --role-name "$ROLE_NAME" >/dev/null 2>&1 || true

echo ">>> teardown complete."

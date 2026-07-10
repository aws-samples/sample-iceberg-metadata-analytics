#!/usr/bin/env bash
#
# _common.sh  --  shared, account-AGNOSTIC config for the helper scripts in this
# directory. Source it from each script: `source "${SCRIPT_DIR}/_common.sh"`.
#
# Nothing here is hardcoded to a particular AWS account. Values resolve from the
# environment (so you can point the helpers at any account/region) and fall back
# to sensible defaults; ACCOUNT_ID is discovered from your current credentials.
#
# Override any of these by exporting them before running a script:
#   ACCOUNT_ID        (default: `aws sts get-caller-identity` of your creds)
#   REGION            (default: us-west-2)
#   WAREHOUSE_BUCKET  (default: iceberg-meta-analytics-<acct>-<region>)
#   GLUE_DB           (default: iceberg_meta_analytics)
#   ROLE_NAME         (default: IcebergMetaAnalytics-GlueRole)

REGION="${REGION:-us-west-2}"
if [[ -z "${ACCOUNT_ID:-}" ]]; then
  ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
fi
[[ -n "${ACCOUNT_ID:-}" ]] || {
  echo "ERROR: could not determine AWS account id. Set ACCOUNT_ID or configure AWS credentials." >&2
  exit 1
}

GLUE_DB="${GLUE_DB:-iceberg_meta_analytics}"
ROLE_NAME="${ROLE_NAME:-IcebergMetaAnalytics-GlueRole}"
POLICY_NAME="${POLICY_NAME:-IcebergMetaAnalytics-GlueInline}"
BUCKET="${WAREHOUSE_BUCKET:-iceberg-meta-analytics-${ACCOUNT_ID}-${REGION}}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"

# Render an iam/*.template.json (sentinels -> real values) to stdout.
render_iam() {
  sed -e "s/__ACCOUNT_ID__/${ACCOUNT_ID}/g" \
      -e "s/__REGION__/${REGION}/g" \
      -e "s/__GLUE_DB__/${GLUE_DB}/g" \
      -e "s/__ROLE_NAME__/${ROLE_NAME}/g" \
      -e "s#__BUCKET__#${BUCKET}#g" \
      "$1"
}

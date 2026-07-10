#!/usr/bin/env bash
#
# teardown.sh  --  fully remove an iceberg-meta-analytics CloudFormation deploy,
# INCLUDING the leftovers CloudFormation doesn't own.
#
# A stack delete/rollback removes everything the stack CREATED, but NOT:
#   * the SEED / artifact bucket (you created it with `aws s3 mb` and passed it
#     as --artifact-bucket, so CFN never owned it), and
#   * the render Lambda's CloudWatch log group (auto-created on first invoke).
# This script deletes the stack, then sweeps those.
#
# Usage:
#   ./teardown.sh --region us-east-1 --prefix icebergmeta \
#       --artifact-bucket iceberg-meta-seed-123456789012-us-east-1
#
#   Flags:
#     --profile <aws-profile> route every aws call through this profile
#     --stack-name <name>     (default: <prefix>-stack)
#     --artifact-prefix <p>   (default: iceberg-meta-analytics/artifacts)
#     --keep-seed-bucket      empty ONLY this deploy's artifacts under the prefix,
#                             leave the bucket (use when the seed bucket is shared
#                             across multiple deployments)
#     --yes                   skip the confirmation prompt
set -euo pipefail

REGION="us-west-2"; PREFIX="icebergmeta"; ARTIFACT_BUCKET=""
ARTIFACT_PREFIX="iceberg-meta-analytics/artifacts"; STACK_NAME=""
KEEP_SEED="false"; ASSUME_YES="false"; AWS_PROFILE_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION="$2"; shift 2;;
    --prefix) PREFIX="$2"; shift 2;;
    --artifact-bucket) ARTIFACT_BUCKET="$2"; shift 2;;
    --artifact-prefix) ARTIFACT_PREFIX="$2"; shift 2;;
    --stack-name) STACK_NAME="$2"; shift 2;;
    --profile) AWS_PROFILE_ARG="$2"; shift 2;;
    --keep-seed-bucket) KEEP_SEED="true"; shift;;
    --yes) ASSUME_YES="true"; shift;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done

# Route every aws call through the chosen profile, if any.
[[ -n "$AWS_PROFILE_ARG" ]] && export AWS_PROFILE="$AWS_PROFILE_ARG"

STACK_NAME="${STACK_NAME:-${PREFIX}-stack}"
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
echo ">>> operating as account ${ACCOUNT_ID} in ${REGION} (profile: ${AWS_PROFILE_ARG:-default})"

# Full-mode warehouse (data) bucket name, per the template's !Sub. Only exists in
# Full mode; in NotebookOnly the warehouse bucket is the CUSTOMER'S and is NEVER
# named this way, so the head-bucket check below simply skips it — we never touch
# a bring-your-own bucket.
WAREHOUSE_BUCKET="${PREFIX}-warehouse-${ACCOUNT_ID}-${REGION}"
LAMBDA_LOG_GROUP="/aws/lambda/${PREFIX}-render-artifacts"

echo "About to tear down:"
echo "  * CloudFormation stack : ${STACK_NAME}"
echo "  * Warehouse bucket      : ${WAREHOUSE_BUCKET}  (only if it exists / Full mode)"
if [[ "$KEEP_SEED" == "true" ]]; then
  echo "  * Seed bucket artifacts : s3://${ARTIFACT_BUCKET}/${ARTIFACT_PREFIX}/  (bucket kept)"
else
  echo "  * Seed bucket           : ${ARTIFACT_BUCKET:-<none given>}  (EMPTIED + DELETED)"
fi
echo "  * Lambda log group      : ${LAMBDA_LOG_GROUP}"
if [[ "$ASSUME_YES" != "true" ]]; then
  read -r -p "Proceed? [y/N] " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "aborted"; exit 0; }
fi

# Does this bucket exist IN THIS ACCOUNT? Use list-buckets (account-level, can't
# 403/301 the way head-bucket does on a bucket you own but with restricted ACLs
# or a cross-region endpoint) -- head-bucket false-negatives were making teardown
# skip a bucket that was really there and report it "absent".
bucket_exists() {
  aws s3api list-buckets --query "Buckets[?Name=='$1'].Name" --output text 2>/dev/null \
    | grep -qx "$1"
}

# ---------------------------------------------------------------------------- #
# Drain a versioned bucket: clear object versions AND delete-markers, looping
# until a fresh listing is empty (a single pass can leave delete-marker residue
# on a versioned bucket -- the same trap the stack's EmptyBucketOnDelete guards).
drain_bucket() {
  local b="$1"
  bucket_exists "$b" || return 1   # genuinely not in this account
  local i page delete_json count
  for i in $(seq 1 200); do
    # One page of up to 1000 versions+markers, already shaped as a delete payload.
    # `text` output of the count tells us cheaply whether the page is empty --
    # no fragile pretty-printed-JSON string comparison.
    # Flatten Versions + DeleteMarkers into one list and count it. NOTE: jmespath
    # has no arithmetic '+', so `length(a) + length(b)` errors on many AWS CLI
    # builds -- and with a `|| echo 0` fallback that error would masquerade as
    # "empty", skipping the drain and leaving delete-bucket to fail BucketNotEmpty.
    # Use a single flattened list + a non-zero sentinel on real errors instead.
    count=$(aws s3api list-object-versions --bucket "$b" --max-keys 1000 \
      --query 'length([Versions[], DeleteMarkers[]][])' \
      --output text 2>/dev/null || echo ERR)
    [[ "$count" == "0" ]] && return 0   # truly empty -> done
    if [[ "$count" == "ERR" || -z "$count" ]]; then
      echo "    WARNING: could not list versions for ${b}; retrying..."; sleep 2; continue
    fi

    delete_json=$(aws s3api list-object-versions --bucket "$b" --max-keys 1000 \
      --query '{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][]}' \
      --output json 2>/dev/null)
    aws s3api delete-objects --bucket "$b" --delete "$delete_json" >/dev/null 2>&1 || true
  done
  echo "    WARNING: ${b} still not empty after 200 passes"; return 1
}

# ---------------------------------------------------------------------------- #
echo ">>> [1/4] delete CloudFormation stack ${STACK_NAME}"
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" >/dev/null 2>&1; then
  aws cloudformation delete-stack --stack-name "$STACK_NAME"
  echo "    waiting for delete to complete (this drains the warehouse bucket)..."
  if ! aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" 2>/dev/null; then
    echo "    stack delete did not complete cleanly -- continuing with manual sweep."
    echo "    (check: aws cloudformation describe-stack-events --stack-name ${STACK_NAME})"
  else
    echo "    stack deleted"
  fi
else
  echo "    stack already gone"
fi

# delete_bucket_verbose <bucket>: delete it, surface the real error, then confirm.
delete_bucket_verbose() {
  local b="$1" err
  if ! err=$(aws s3api delete-bucket --bucket "$b" 2>&1); then
    echo "    ERROR deleting ${b}: ${err}"; return 1
  fi
  if bucket_exists "$b"; then
    echo "    WARNING: ${b} still present after delete-bucket -- check console"; return 1
  fi
  echo "    ${b} emptied + deleted"
}

echo ">>> [2/4] warehouse (data) bucket"
if drain_bucket "$WAREHOUSE_BUCKET"; then
  delete_bucket_verbose "$WAREHOUSE_BUCKET" || true
else
  echo "    ${WAREHOUSE_BUCKET} absent (already removed by stack delete, or NotebookOnly)"
fi

echo ">>> [3/4] seed / artifact bucket"
if [[ -z "$ARTIFACT_BUCKET" ]]; then
  echo "    no --artifact-bucket given; skipping"
elif [[ "$KEEP_SEED" == "true" ]]; then
  # Shared seed bucket: remove only THIS deploy's prefix, keep the bucket.
  aws s3 rm "s3://${ARTIFACT_BUCKET}/${ARTIFACT_PREFIX}/" --recursive || true
  echo "    emptied s3://${ARTIFACT_BUCKET}/${ARTIFACT_PREFIX}/ (bucket kept)"
elif drain_bucket "$ARTIFACT_BUCKET"; then
  delete_bucket_verbose "$ARTIFACT_BUCKET" || true
else
  echo "    ${ARTIFACT_BUCKET} absent (not found in account ${ACCOUNT_ID})"
fi

echo ">>> [4/4] render Lambda log group"
aws logs delete-log-group --log-group-name "$LAMBDA_LOG_GROUP" >/dev/null 2>&1 \
  && echo "    ${LAMBDA_LOG_GROUP} deleted" \
  || echo "    ${LAMBDA_LOG_GROUP} absent"

echo ">>> teardown complete."
echo
echo "Note: Glue writes shared account-wide log groups under /aws-glue/* that are"
echo "NOT deleted here (they hold logs for any other Glue jobs too). Remove them"
echo "manually only if this account has no other Glue usage. Any interactive"
echo "notebook sessions you started are auto-terminated on idle timeout; list with"
echo "  aws glue list-sessions --region ${REGION}"

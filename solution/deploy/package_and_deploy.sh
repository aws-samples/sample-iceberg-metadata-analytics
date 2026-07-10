#!/usr/bin/env bash
#
# package_and_deploy.sh  --  stage artifacts to an S3 "seed" bucket and deploy
# the CloudFormation stack in either Full or NotebookOnly mode.
#
# It can run two ways:
#   * INTERACTIVE (no args, or `--interactive`): prompts for every input, then
#     confirms before deploying. Best for first-time / customer use.
#   * FLAGS (any arg present): fully scriptable, same flags as before.
#
# What it does for you:
#   1. Creates the seed/artifact bucket if it doesn't exist.
#   2. Probes Bedrock model access (Nova Pro, then Sonnet) and picks the first
#      one that actually answers -- unless you pin one with --bedrock-model.
#   3. Builds + stages artifacts, then deploys.
#   4. On ANY failure it rolls the stack back cleanly (delete-stack + wait),
#      unless you pass --no-rollback.
#
# Usage:
#   ./package_and_deploy.sh                      # interactive
#   ./package_and_deploy.sh full --region us-east-1 --prefix icebergmeta \
#       --artifact-bucket my-seed-bucket
#   ./package_and_deploy.sh notebook-only --region us-east-1 --prefix icebergmeta \
#       --artifact-bucket my-seed-bucket \
#       --warehouse-bucket my-iceberg-bucket --glue-db my_catalog_db \
#       [--tables "orders,events"] [--kms-arn arn:aws:kms:...]
#
#   Flags: --no-agentic  --no-demo  --bedrock-model <id>  --profile <aws-profile>
#          --stack-name <name>  --no-rollback  --interactive
set -euo pipefail

# --- Bedrock probe candidates, best-first. Editable. Each is a us. inference
#     profile id; the probe skips any that returns AccessDenied OR an invalid
#     model id, so a wrong string here just means "try the next one". ------- #
MODEL_CANDIDATES=(
  "us.amazon.nova-pro-v1:0"
  "us.amazon.nova-lite-v1:0"
  "us.anthropic.claude-sonnet-4-6"
)

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
MODE=""; REGION="us-west-2"; PREFIX="icebergmeta"
ARTIFACT_BUCKET=""; ARTIFACT_PREFIX="iceberg-meta-analytics/artifacts"
WAREHOUSE_BUCKET=""; GLUE_DB=""; TABLES=""; KMS_ARN=""
ENABLE_AGENTIC="true"; CREATE_DEMO="true"
BEDROCK_MODEL=""            # empty => probe and auto-pick
STACK_NAME=""; AWS_PROFILE_ARG=""; DO_ROLLBACK="true"; INTERACTIVE=""

# First positional arg, if it's a mode, is consumed here.
if [[ "${1:-}" == "full" || "${1:-}" == "notebook-only" ]]; then
  MODE="$1"; shift
elif [[ "${1:-}" == "--interactive" || $# -eq 0 ]]; then
  INTERACTIVE="true"; [[ "${1:-}" == "--interactive" ]] && shift
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION="$2"; shift 2;;
    --prefix) PREFIX="$2"; shift 2;;
    --artifact-bucket) ARTIFACT_BUCKET="$2"; shift 2;;
    --artifact-prefix) ARTIFACT_PREFIX="$2"; shift 2;;
    --warehouse-bucket) WAREHOUSE_BUCKET="$2"; shift 2;;
    --glue-db) GLUE_DB="$2"; shift 2;;
    --tables) TABLES="$2"; shift 2;;
    --kms-arn) KMS_ARN="$2"; shift 2;;
    --bedrock-model) BEDROCK_MODEL="$2"; shift 2;;
    --stack-name) STACK_NAME="$2"; shift 2;;
    --profile) AWS_PROFILE_ARG="$2"; shift 2;;
    --no-agentic) ENABLE_AGENTIC="false"; shift;;
    --no-demo) CREATE_DEMO="false"; shift;;
    --no-rollback) DO_ROLLBACK="false"; shift;;
    --interactive) INTERACTIVE="true"; shift;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done

# Route every aws call through the chosen profile, if any.
[[ -n "$AWS_PROFILE_ARG" ]] && export AWS_PROFILE="$AWS_PROFILE_ARG"

# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
ask() {  # ask <var> <prompt> <default>
  local __var="$1" __prompt="$2" __default="$3" __reply
  if [[ -n "$__default" ]]; then
    read -r -p "$__prompt [$__default]: " __reply
    __reply="${__reply:-$__default}"
  else
    read -r -p "$__prompt: " __reply
  fi
  printf -v "$__var" '%s' "$__reply"
}
ask_yn() {  # ask_yn <var: true|false> <prompt> <default: true|false>
  local __var="$1" __prompt="$2" __default="$3" __reply __hint
  [[ "$__default" == "true" ]] && __hint="Y/n" || __hint="y/N"
  read -r -p "$__prompt [$__hint]: " __reply
  __reply="${__reply:-$__default}"
  case "$__reply" in y|Y|yes|true|true) printf -v "$__var" 'true';; *) printf -v "$__var" 'false';; esac
}

# --------------------------------------------------------------------------- #
# Interactive prompts
# --------------------------------------------------------------------------- #
if [[ "$INTERACTIVE" == "true" ]]; then
  echo "=== Iceberg Metadata Analytics — guided deploy ==="
  echo
  if [[ -z "$MODE" ]]; then
    ask MODE "Deploy mode: 'full' (create everything) or 'notebook-only' (point at your existing Iceberg)" "full"
  fi
  ask REGION "AWS region (must be a US region for the agentic layer)" "$REGION"
  ask PREFIX "Resource name prefix (lowercase alphanumeric, no hyphens)" "$PREFIX"
  # discover account for a good default seed-bucket name
  export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"
  ACCT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo ACCOUNT)"
  ask ARTIFACT_BUCKET "Seed/artifact S3 bucket (created if missing)" "${PREFIX}-seed-${ACCT}-${REGION}"
  if [[ "$MODE" == "notebook-only" ]]; then
    ask WAREHOUSE_BUCKET "Your EXISTING Iceberg warehouse bucket" ""
    ask GLUE_DB "Your EXISTING Glue database" ""
    ask TABLES "Tables to analyze (comma-sep; blank = auto-discover all)" ""
    ask KMS_ARN "KMS key ARN if your bucket is SSE-KMS (blank = none)" ""
  else
    ask_yn CREATE_DEMO "Full mode: auto-run the generator to create 3 demo tables?" "true"
  fi
  ask_yn ENABLE_AGENTIC "Enable the Bedrock agentic advisory layer?" "true"
  echo
fi

# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
[[ "$MODE" == "full" || "$MODE" == "notebook-only" ]] || {
  echo "ERROR: mode must be 'full' or 'notebook-only' (got '${MODE:-<none>}')"; exit 2; }
[[ -n "$ARTIFACT_BUCKET" ]] || { echo "ERROR: --artifact-bucket / seed bucket is required"; exit 2; }
# Must match the template's ResourcePrefix AllowedPattern ([a-z][a-z0-9]{1,39}):
[[ "$PREFIX" =~ ^[a-z][a-z0-9]{1,39}$ ]] || {
  echo "ERROR: --prefix '$PREFIX' invalid: lowercase alphanumeric, 2-40 chars, no hyphens"; exit 2; }
if [[ "$MODE" == "notebook-only" ]]; then
  [[ -n "$WAREHOUSE_BUCKET" && -n "$GLUE_DB" ]] || {
    echo "ERROR: notebook-only requires --warehouse-bucket and --glue-db"; exit 2; }
fi
STACK_NAME="${STACK_NAME:-${PREFIX}-stack}"
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"

# --------------------------------------------------------------------------- #
# [0/6] preflight: credentials + region
# --------------------------------------------------------------------------- #
echo ">>> [0/6] preflight"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)" || {
  echo "ERROR: no working AWS credentials (profile='${AWS_PROFILE_ARG:-default}')"; exit 1; }
echo "    account=${ACCOUNT_ID} region=${REGION} profile=${AWS_PROFILE_ARG:-default}"
case "$REGION" in
  us-east-1|us-east-2|us-west-2) ;;
  *) [[ "$ENABLE_AGENTIC" == "true" ]] && echo "    WARNING: region ${REGION} is non-US; the agentic layer's us. profile + FM ARNs won't resolve. Consider --no-agentic.";;
esac

# --------------------------------------------------------------------------- #
# [1/6] seed bucket (create if missing)
# --------------------------------------------------------------------------- #
echo ">>> [1/6] seed/artifact bucket: ${ARTIFACT_BUCKET}"
if aws s3api head-bucket --bucket "$ARTIFACT_BUCKET" 2>/dev/null; then
  echo "    exists"
else
  echo "    creating..."
  if [[ "$REGION" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "$ARTIFACT_BUCKET" >/dev/null
  else
    aws s3api create-bucket --bucket "$ARTIFACT_BUCKET" \
      --create-bucket-configuration "LocationConstraint=${REGION}" >/dev/null
  fi
  echo "    created"
fi

# --------------------------------------------------------------------------- #
# [2/6] Bedrock model access probe (skip if agentic disabled or model pinned)
# --------------------------------------------------------------------------- #
if [[ "$ENABLE_AGENTIC" != "true" ]]; then
  echo ">>> [2/6] Bedrock probe skipped (agentic disabled)"
  BEDROCK_MODEL="${BEDROCK_MODEL:-us.amazon.nova-pro-v1:0}"   # placeholder; unused
elif [[ -n "$BEDROCK_MODEL" ]]; then
  echo ">>> [2/6] Bedrock model pinned to ${BEDROCK_MODEL} (skipping probe)"
else
  echo ">>> [2/6] probing Bedrock model access (Converse API)"
  PICKED=""
  for mid in "${MODEL_CANDIDATES[@]}"; do
    if aws bedrock-runtime converse --region "$REGION" --model-id "$mid" \
         --messages '[{"role":"user","content":[{"text":"hi"}]}]' \
         --inference-config '{"maxTokens":5}' >/dev/null 2>&1; then
      echo "    OK   -> ${mid}  [selected]"
      PICKED="$mid"; break
    else
      echo "    FAIL -> ${mid}  (no access / invalid id)"
    fi
  done
  if [[ -z "$PICKED" ]]; then
    echo "ERROR: no candidate Bedrock model is invokable in ${REGION}."
    echo "       Enable model access in the Bedrock console (Model access), or"
    echo "       re-run with --no-agentic to deploy without the advisory layer."
    exit 1
  fi
  BEDROCK_MODEL="$PICKED"
fi
echo "    using BedrockModelId=${BEDROCK_MODEL}"

# --------------------------------------------------------------------------- #
# [3/6] build templated artifacts
# --------------------------------------------------------------------------- #
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "${HERE}/.." && pwd)"
DEST="s3://${ARTIFACT_BUCKET}/${ARTIFACT_PREFIX}"

echo ">>> [3/6] (re)build the TEMPLATED notebook artifacts"
( cd "${REPO}/notebook" && python3 build_notebook.py --template >/dev/null )

echo ">>> [4/6] package the render Lambda + stage artifacts to ${DEST}"
TMP="$(mktemp -d)"
cp "${HERE}/lambda/render_artifacts.py" "${TMP}/"
( cd "${TMP}" && zip -q render_artifacts.zip render_artifacts.py )
# Raw {{SENTINEL}} templates -> seed bucket. The render Lambda substitutes them
# at deploy time and writes RENDERED code to ${DEST}/rendered/<prefix>/...
aws s3 cp "${TMP}/render_artifacts.zip" "${DEST}/lambda/render_artifacts.zip" >/dev/null
aws s3 cp "${REPO}/notebook/iceberg_metadata_analytics.template.ipynb" "${DEST}/notebook/iceberg_metadata_analytics.template.ipynb" >/dev/null
aws s3 cp "${REPO}/notebook/iceberg_metadata_analytics.template.py"   "${DEST}/notebook/iceberg_metadata_analytics.template.py"   >/dev/null
aws s3 cp "${REPO}/notebook/iceberg_nb_helpers.py" "${DEST}/notebook/iceberg_nb_helpers.py" >/dev/null
aws s3 cp "${REPO}/utility/iceberg_utility.py" "${DEST}/utility/iceberg_utility.py" >/dev/null
aws s3 cp "${REPO}/utility/specs/" "${DEST}/utility/specs/" --recursive --exclude "*" --include "*.json" >/dev/null
aws s3 cp "${REPO}/agentic/iceberg_advisor.py" "${DEST}/agentic/iceberg_advisor.py" >/dev/null
aws s3 cp "${REPO}/agentic/best_practices.json" "${DEST}/agentic/best_practices.json" >/dev/null
aws s3 cp "${REPO}/advisory/run_advisory.py" "${DEST}/agentic/run_advisory.py" >/dev/null
rm -rf "$TMP"

# --------------------------------------------------------------------------- #
# [5/6] deploy (with clean rollback on failure)
# --------------------------------------------------------------------------- #
COMMON_PARAMS=(
  "ResourcePrefix=${PREFIX}"
  "ArtifactBucket=${ARTIFACT_BUCKET}"
  "ArtifactPrefix=${ARTIFACT_PREFIX}"
  "EnableAgentic=${ENABLE_AGENTIC}"
  "BedrockModelId=${BEDROCK_MODEL}"
)
if [[ "$MODE" == "full" ]]; then
  PARAMS=( "DeploymentMode=Full" "CreateDemoData=${CREATE_DEMO}" "${COMMON_PARAMS[@]}" )
else
  PARAMS=( "DeploymentMode=NotebookOnly"
           "ExistingWarehouseBucket=${WAREHOUSE_BUCKET}"
           "ExistingGlueDatabase=${GLUE_DB}"
           "TableAllowList=${TABLES}"
           "ExistingKmsKeyArn=${KMS_ARN}"
           "${COMMON_PARAMS[@]}" )
fi

echo ">>> [5/6] deploy CloudFormation stack: ${STACK_NAME} (mode=${MODE})"
echo "    mode=${MODE} agentic=${ENABLE_AGENTIC} demo=${CREATE_DEMO:-n/a} model=${BEDROCK_MODEL}"

# If a prior attempt left the stack in ROLLBACK_COMPLETE, it can't be updated --
# delete it first so this is a clean create.
STATUS="$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo NONE)"
if [[ "$STATUS" == "ROLLBACK_COMPLETE" || "$STATUS" == "ROLLBACK_FAILED" ]]; then
  echo "    prior stack in ${STATUS}; deleting before re-create..."
  aws cloudformation delete-stack --stack-name "$STACK_NAME"
  aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" 2>/dev/null || true
fi

# Shared cleanup: delete the (possibly in-flight) stack and wait. A delete-stack
# issued while the stack is CREATE_IN_PROGRESS is accepted by CloudFormation --
# it cancels the create and rolls everything back, draining the warehouse bucket.
_delete_and_wait() {
  aws cloudformation delete-stack --stack-name "$STACK_NAME" 2>/dev/null || true
  aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" 2>/dev/null \
    && echo "    stack ${STACK_NAME} removed" \
    || echo "    WARNING: stack delete did not finish cleanly; check the console."
}

rollback() {
  echo
  echo "!!! deploy FAILED. Last stack events:"
  aws cloudformation describe-stack-events --stack-name "$STACK_NAME" \
    --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`||ResourceStatus==`UPDATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
    --output table 2>/dev/null | head -30 || true
  if [[ "$DO_ROLLBACK" == "true" ]]; then
    echo ">>> rolling back: deleting stack ${STACK_NAME} (drains the created warehouse bucket)"
    _delete_and_wait
  else
    echo ">>> --no-rollback set; leaving the failed stack for inspection."
  fi
  exit 1
}

# Interrupt handler (Ctrl-C / SIGTERM). Only armed for the deploy window, when a
# stack may be mid-creation. Ctrl-C kills the child `aws deploy` too, so by the
# time we're here the stack is likely CREATE_IN_PROGRESS. Offer to tear it down
# so an interrupted run doesn't strand half-created resources. SIGKILL (-9) can't
# be trapped -- use ./teardown.sh after one of those.
STACK_ARMED="false"
on_interrupt() {
  trap - INT TERM            # disarm so a second Ctrl-C exits hard
  echo
  echo "!!! interrupted during deploy."
  if [[ "$STACK_ARMED" != "true" ]]; then
    echo "    (no stack was in flight; nothing to roll back)"; exit 130
  fi
  if [[ "$DO_ROLLBACK" != "true" ]]; then
    echo "    --no-rollback set; leaving stack ${STACK_NAME} as-is. Clean up with ./teardown.sh."; exit 130
  fi
  local ans="y"
  # Prompt only if we have a real terminal; in a non-interactive run, default to
  # rolling back (safer than stranding resources).
  if [[ -t 0 ]]; then
    read -r -p "    roll back (delete) stack ${STACK_NAME} now? [Y/n] " ans || ans="y"
  fi
  case "${ans:-y}" in
    n|N) echo "    left in place. Clean up later with: ./teardown.sh --region ${REGION} --prefix ${PREFIX} --artifact-bucket ${ARTIFACT_BUCKET}";;
    *)   echo ">>> rolling back..."; _delete_and_wait;;
  esac
  exit 130
}
trap on_interrupt INT TERM

STACK_ARMED="true"           # from here on, a stack can exist -> interrupt rolls back
if ! aws cloudformation deploy \
      --stack-name "$STACK_NAME" \
      --template-file "${HERE}/iceberg-meta-analytics.yaml" \
      --capabilities CAPABILITY_NAMED_IAM \
      --parameter-overrides "${PARAMS[@]}"; then
  rollback
fi
trap - INT TERM              # deploy succeeded; disarm the interrupt handler

echo ">>> [6/6] outputs:"
aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query 'Stacks[0].Outputs' --output table
echo ">>> done."

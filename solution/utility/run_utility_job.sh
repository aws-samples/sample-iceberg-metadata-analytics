#!/usr/bin/env bash
#
# run_utility_job.sh  --  create/update & run the config-driven Iceberg utility job.
#
# Usage:
#   ./run_utility_job.sh --preset demo
#   ./run_utility_job.sh --spec specs/example_custom.json
#   ./run_utility_job.sh --config-inline '{"version":1,"operations":[...]}'
set -euo pipefail

# This is an OPTIONAL manual helper for running the utility Glue job outside the
# CloudFormation deploy (the CFN stack creates + runs this job for you). It carries
# no baked-in account identifiers: supply your own via env vars (or edit below).
#   ACCOUNT_ID   your AWS account id            (required)
#   REGION       AWS region                     (default: us-west-2)
#   WAREHOUSE_BUCKET / GLUE_DB / ROLE_ARN       (defaults derived from the above)
REGION="${REGION:-us-west-2}"
ACCOUNT_ID="${ACCOUNT_ID:?set ACCOUNT_ID to your AWS account id}"
BUCKET="${WAREHOUSE_BUCKET:-iceberg-meta-analytics-${ACCOUNT_ID}-${REGION}}"
GLUE_DB="${GLUE_DB:-iceberg_meta_analytics}"
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::${ACCOUNT_ID}:role/IcebergMetaAnalytics-GlueRole}"
JOB_NAME="${JOB_NAME:-iceberg-utility}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"

# ---- parse one of --preset / --spec / --config-inline ----------------------
CONFIG_ARG=""
PRESET_ARG=""
case "${1:-}" in
  --preset)        PRESET_ARG="${2:?preset name}";;
  --spec)          SPEC_FILE="${2:?spec file}";;
  --config-inline) CONFIG_ARG="${2:?inline json}";;
  *) echo "usage: $0 --preset <name> | --spec <file.json> | --config-inline '<json>'"; exit 2;;
esac

echo ">>> upload utility script"
aws s3 cp "${SCRIPT_DIR}/iceberg_utility.py" "s3://${BUCKET}/utility/iceberg_utility.py" >/dev/null

# Always upload the bundled specs so presets resolve from S3.
echo ">>> upload bundled specs"
aws s3 cp "${SCRIPT_DIR}/specs/" "s3://${BUCKET}/utility/specs/" --recursive --exclude "*" --include "*.json" >/dev/null

# If a spec FILE was given, upload it and point --config at its S3 URI.
if [[ -n "${SPEC_FILE:-}" ]]; then
  base="$(basename "$SPEC_FILE")"
  aws s3 cp "$SPEC_FILE" "s3://${BUCKET}/utility/specs/${base}" >/dev/null
  CONFIG_ARG="s3://${BUCKET}/utility/specs/${base}"
fi

COMMAND="{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://${BUCKET}/utility/iceberg_utility.py\",\"PythonVersion\":\"3\"}"
DEFAULT_ARGS=$(cat <<JSON
{
  "--datalake-formats": "iceberg",
  "--warehouse_bucket": "${BUCKET}",
  "--glue_database": "${GLUE_DB}",
  "--enable-glue-datacatalog": "true",
  "--enable-continuous-cloudwatch-log": "true",
  "--TempDir": "s3://${BUCKET}/glue-temp/",
  "--job-language": "python"
}
JSON
)

echo ">>> create/update Glue job: ${JOB_NAME}"
if aws glue get-job --job-name "$JOB_NAME" >/dev/null 2>&1; then
  aws glue update-job --job-name "$JOB_NAME" --job-update "{
      \"Role\": \"${ROLE_ARN}\", \"GlueVersion\": \"5.0\", \"Command\": ${COMMAND},
      \"DefaultArguments\": ${DEFAULT_ARGS}, \"WorkerType\": \"G.1X\",
      \"NumberOfWorkers\": 10, \"Timeout\": 60, \"ExecutionProperty\": {\"MaxConcurrentRuns\": 2}
  }" >/dev/null
else
  aws glue create-job --name "$JOB_NAME" --role "$ROLE_ARN" --glue-version "5.0" \
    --command "$COMMAND" --default-arguments "$DEFAULT_ARGS" --worker-type "G.1X" \
    --number-of-workers 10 --timeout 60 --execution-property '{"MaxConcurrentRuns":2}' \
    --tags project=iceberg-meta-analytics,managed-by=blog-artifact >/dev/null
fi

# ---- build run arguments ---------------------------------------------------
RUN_ARGS="{}"
if [[ -n "$PRESET_ARG" ]]; then
  RUN_ARGS="{\"--preset\":\"${PRESET_ARG}\"}"
elif [[ -n "$CONFIG_ARG" ]]; then
  # pass config as a run-scoped arg (works for s3:// URIs and inline JSON)
  RUN_ARGS=$(python3 -c "import json,sys; print(json.dumps({'--config': sys.argv[1]}))" "$CONFIG_ARG")
fi

echo ">>> start run with args: ${RUN_ARGS}"
RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --arguments "$RUN_ARGS" --query 'JobRunId' --output text)
echo "    JobRunId: ${RUN_ID}"

echo ">>> polling"
while true; do
  sleep 20
  STATE=$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" --query 'JobRun.JobRunState' --output text)
  EXEC=$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" --query 'JobRun.ExecutionTime' --output text)
  printf '    [%s] state=%s exec=%ss\n' "$(date '+%H:%M:%S')" "$STATE" "$EXEC"
  case "$STATE" in
    SUCCEEDED) echo ">>> SUCCEEDED"; break ;;
    FAILED|ERROR|TIMEOUT|STOPPED)
      echo ">>> $STATE"
      aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" --query 'JobRun.ErrorMessage' --output text
      exit 1 ;;
  esac
done
echo ">>> done. RunId=${RUN_ID}"

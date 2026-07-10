#!/usr/bin/env bash
#
# 01_run_datagen_job.sh
#
# Creates (or updates) the Glue 5.0 Spark job that generates the three Iceberg
# tables, uploads the script, starts a run, and polls to completion.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"
JOB_NAME="iceberg-meta-analytics-datagen"

echo ">>> upload data-gen script"
aws s3 cp "${SCRIPT_DIR}/generate_iceberg_tables.py" \
  "s3://${BUCKET}/scripts/generate_iceberg_tables.py" >/dev/null

COMMAND="{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://${BUCKET}/scripts/generate_iceberg_tables.py\",\"PythonVersion\":\"3\"}"

# --datalake-formats=iceberg makes Glue load the bundled Iceberg runtime.
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
      \"Role\": \"${ROLE_ARN}\",
      \"GlueVersion\": \"5.0\",
      \"Command\": ${COMMAND},
      \"DefaultArguments\": ${DEFAULT_ARGS},
      \"WorkerType\": \"G.2X\",
      \"NumberOfWorkers\": 5,
      \"Timeout\": 60,
      \"ExecutionProperty\": {\"MaxConcurrentRuns\": 1}
  }" >/dev/null
  echo "    updated"
else
  aws glue create-job --name "$JOB_NAME" \
    --role "$ROLE_ARN" \
    --glue-version "5.0" \
    --command "$COMMAND" \
    --default-arguments "$DEFAULT_ARGS" \
    --worker-type "G.2X" \
    --number-of-workers 5 \
    --timeout 60 \
    --execution-property '{"MaxConcurrentRuns":1}' \
    --tags project=iceberg-meta-analytics,managed-by=blog-artifact >/dev/null
  echo "    created"
fi

echo ">>> start run"
RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --query 'JobRunId' --output text)
echo "    JobRunId: ${RUN_ID}"

echo ">>> polling (this generates ~15M+ rows; expect several minutes)"
while true; do
  sleep 20
  STATE=$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
            --query 'JobRun.JobRunState' --output text)
  EXEC=$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
            --query 'JobRun.ExecutionTime' --output text)
  printf '    [%s] state=%s exec=%ss\n' "$(date '+%H:%M:%S')" "$STATE" "$EXEC"
  case "$STATE" in
    SUCCEEDED) echo ">>> SUCCEEDED"; break ;;
    FAILED|ERROR|TIMEOUT|STOPPED)
      echo ">>> $STATE"
      aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
        --query 'JobRun.ErrorMessage' --output text
      exit 1 ;;
  esac
done

echo ">>> done. RunId=${RUN_ID}"

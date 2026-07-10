#!/usr/bin/env bash
#
# 05_register_notebook.sh
#
# Registers the analytics notebook as a Glue Studio NOTEBOOK-mode job so it
# shows up under Glue Studio -> Notebooks / ETL jobs. Everything lives in the
# project's OWN dedicated bucket (not the shared aws-glue-assets bucket), so it
# stays within the least-privilege boundary and is trivial to tear down.
#
# Glue Studio convention (verified against existing notebook jobs in this acct):
#   * job name           == notebook base name
#   * notebook .ipynb     -> s3://<bucket>/notebooks/<name>.ipynb
#   * paired script .py   -> s3://<bucket>/scripts/<name>.py   (Command.ScriptLocation)
#   * job has JobMode = NOTEBOOK   <-- this is what makes the console render it as a notebook
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"

# IMPORTANT: AWS Glue Studio resolves a NOTEBOOK job's .ipynb by the JOB NAME,
# not by the script basename. So the S3 object MUST be <notebooks>/<job>.ipynb
# (and paired script <scripts>/<job>.py). We keep the local repo filenames
# readable (iceberg_metadata_analytics.*) but publish under the job name.
LOCAL_BASE="iceberg_metadata_analytics"        # local repo filenames (from build_notebook.py)
JOB_NAME="iceberg-meta-analytics-notebook"     # Glue job (and console) display name
NB_NAME="${JOB_NAME}"                          # S3 object base name MUST equal job name

BUILD_DIR="$(cd "${SCRIPT_DIR}/../solution/notebook" && pwd)"   # build_notebook.py lives here
# The account-specific notebook is a LOCAL build artifact (real values baked in),
# not part of the shippable solution/, so build it into a local, gitignored dir.
NB_DIR="${SCRIPT_DIR}/../.local/notebook"
mkdir -p "$NB_DIR"; NB_DIR="$(cd "$NB_DIR" && pwd)"
IAM_DIR="$(cd "${SCRIPT_DIR}/../iam" && pwd)"

echo ">>> refresh least-privilege IAM (adds sts:TagSession + scoped iam:PassRole needed for notebooks)"
aws iam update-assume-role-policy --role-name "$ROLE_NAME" \
  --policy-document "$(render_iam "${IAM_DIR}/glue-trust-policy.template.json")" >/dev/null
aws iam put-role-policy --role-name "$ROLE_NAME" \
  --policy-name "$POLICY_NAME" \
  --policy-document "$(render_iam "${IAM_DIR}/glue-permissions-policy.template.json")" >/dev/null
echo "    done"

echo ">>> (re)build notebook + paired script"
NB_WAREHOUSE_BUCKET="$BUCKET" NB_GLUE_DB="$GLUE_DB" NB_REGION="$REGION" \
  python3 "${BUILD_DIR}/build_notebook.py" --outdir "$NB_DIR"

NB_S3="s3://${BUCKET}/notebooks/${NB_NAME}.ipynb"
PY_S3="s3://${BUCKET}/scripts/${NB_NAME}.py"
# Helper module attached to the session via the notebook's %extra_py_files magic.
# The magic points at s3://<bucket>/notebook/iceberg_nb_helpers.py, so publish there.
HELPER_S3="s3://${BUCKET}/notebook/iceberg_nb_helpers.py"

echo ">>> upload notebook -> ${NB_S3}"
aws s3 cp "${NB_DIR}/${LOCAL_BASE}.ipynb" "$NB_S3" >/dev/null
echo ">>> upload paired script -> ${PY_S3}"
aws s3 cp "${NB_DIR}/${LOCAL_BASE}.py" "$PY_S3" >/dev/null
echo ">>> upload notebook helper module -> ${HELPER_S3}"
aws s3 cp "${BUILD_DIR}/iceberg_nb_helpers.py" "$HELPER_S3" >/dev/null

echo ">>> remove any stale misnamed objects (basename != job name)"
aws s3 rm "s3://${BUCKET}/notebooks/${LOCAL_BASE}.ipynb" >/dev/null 2>&1 || true
aws s3 rm "s3://${BUCKET}/scripts/${LOCAL_BASE}.py" >/dev/null 2>&1 || true

COMMAND="{\"Name\":\"glueetl\",\"ScriptLocation\":\"${PY_S3}\",\"PythonVersion\":\"3\"}"
DEFAULT_ARGS=$(cat <<JSON
{
  "--datalake-formats": "iceberg",
  "--enable-glue-datacatalog": "true",
  "--job-language": "python",
  "--extra-py-files": "${HELPER_S3}",
  "--TempDir": "s3://${BUCKET}/glue-temp/"
}
JSON
)

echo ">>> create/update NOTEBOOK-mode Glue job: ${JOB_NAME}"
if aws glue get-job --job-name "$JOB_NAME" >/dev/null 2>&1; then
  aws glue update-job --job-name "$JOB_NAME" --job-update "{
      \"Role\": \"${ROLE_ARN}\",
      \"GlueVersion\": \"5.0\",
      \"JobMode\": \"NOTEBOOK\",
      \"Command\": ${COMMAND},
      \"DefaultArguments\": ${DEFAULT_ARGS},
      \"WorkerType\": \"G.1X\",
      \"NumberOfWorkers\": 5,
      \"Timeout\": 480,
      \"ExecutionProperty\": {\"MaxConcurrentRuns\": 1}
  }" >/dev/null
  echo "    updated"
else
  aws glue create-job --name "$JOB_NAME" \
    --role "$ROLE_ARN" \
    --glue-version "5.0" \
    --job-mode "NOTEBOOK" \
    --command "$COMMAND" \
    --default-arguments "$DEFAULT_ARGS" \
    --worker-type "G.1X" \
    --number-of-workers 5 \
    --timeout 480 \
    --execution-property '{"MaxConcurrentRuns":1}' \
    --tags project=iceberg-meta-analytics,managed-by=blog-artifact >/dev/null
  echo "    created"
fi

cat <<SUMMARY

=========================================================================
 Notebook registered with AWS Glue (region: ${REGION})
=========================================================================
  Console : Glue Studio -> ETL jobs / Notebooks -> "${JOB_NAME}"
  JobMode : NOTEBOOK
  Notebook: ${NB_S3}
  Script  : ${PY_S3}
  Role    : ${ROLE_ARN}
=========================================================================
SUMMARY

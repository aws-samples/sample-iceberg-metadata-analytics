#!/usr/bin/env bash
#
# 02_validate_athena.sh
#
# Confirms the three Iceberg tables are queryable from Athena (engine v3),
# including a metadata-table query ($files) to prove the metadata layer is
# reachable from SQL as well as from the Glue notebook.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/_common.sh"
DB="$GLUE_DB"
RESULTS="s3://aws-athena-query-results-${REGION}-${ACCOUNT_ID}/iceberg-meta-analytics/"

run_query() {
  local label="$1" sql="$2"
  echo ""
  echo ">>> ${label}"
  local qid
  qid=$(aws athena start-query-execution \
        --query-string "$sql" \
        --query-execution-context "Database=${DB},Catalog=AwsDataCatalog" \
        --result-configuration "OutputLocation=${RESULTS}" \
        --work-group primary \
        --query 'QueryExecutionId' --output text)
  while true; do
    sleep 3
    local st
    st=$(aws athena get-query-execution --query-execution-id "$qid" \
          --query 'QueryExecution.Status.State' --output text)
    case "$st" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        echo "    QUERY $st:"
        aws athena get-query-execution --query-execution-id "$qid" \
          --query 'QueryExecution.Status.StateChangeReason' --output text
        return 1 ;;
    esac
  done
  aws athena get-query-results --query-execution-id "$qid" \
    --query 'ResultSet.Rows[*].Data[*].VarCharValue' --output text
}

run_query "Row counts (all three tables)" \
  "SELECT 'web_events_large' AS tbl, COUNT(*) AS rows FROM web_events_large
   UNION ALL SELECT 'clickstream_micro', COUNT(*) FROM clickstream_micro
   UNION ALL SELECT 'customers_unpartitioned', COUNT(*) FROM customers_unpartitioned"

run_query "web_events_large — partitions & sample" \
  "SELECT event_date, COUNT(*) AS rows FROM web_events_large GROUP BY event_date ORDER BY event_date"

run_query "clickstream_micro — files metadata table via Athena (\$files)" \
  "SELECT COUNT(*) AS n_files,
          ROUND(AVG(file_size_in_bytes)/1024.0, 1) AS avg_kb,
          ROUND(MIN(file_size_in_bytes)/1024.0, 1) AS min_kb
   FROM \"clickstream_micro\$files\""

run_query "clickstream_micro — partition skew via Athena (\$partitions)" \
  "SELECT \"partition\".country AS country, record_count AS rows, file_count AS files
   FROM \"clickstream_micro\$partitions\" ORDER BY files DESC LIMIT 15"

echo ""
echo ">>> Athena validation complete."

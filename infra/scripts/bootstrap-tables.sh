#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-ap-southeast-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-878670310452}"
PROJECT_NAME="${PROJECT_NAME:-yt-sales-iac}"
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"
LAKEHOUSE_BUCKET="${LAKEHOUSE_BUCKET:-${PROJECT_NAME}-lakehouse-${AWS_ACCOUNT_ID}}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-${PROJECT_NAME}-artifacts-${AWS_ACCOUNT_ID}}"
ATHENA_OUTPUT="${ATHENA_OUTPUT:-s3://${ARTIFACTS_BUCKET}/athena-results/bootstrap/}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_DIR="${INFRA_DIR}/sql/iceberg"

run_query() {
  local file="$1"
  local sql
  sql="$(sed "s/__LAKEHOUSE_BUCKET__/${LAKEHOUSE_BUCKET}/g" "${file}")"
  echo "Executing $(basename "${file}") ..."
  local qid
  qid="$(aws athena start-query-execution \
    --region "${AWS_REGION}" \
    --work-group "${ATHENA_WORKGROUP}" \
    --query-string "${sql}" \
    --result-configuration "OutputLocation=${ATHENA_OUTPUT}" \
    --query 'QueryExecutionId' --output text)"

  while true; do
    local state reason
    state="$(aws athena get-query-execution --region "${AWS_REGION}" --query-execution-id "${qid}" --query 'QueryExecution.Status.State' --output text)"
    case "${state}" in
      SUCCEEDED)
        echo "  SUCCEEDED (${qid})"
        break
        ;;
      FAILED|CANCELLED)
        reason="$(aws athena get-query-execution --region "${AWS_REGION}" --query-execution-id "${qid}" --query 'QueryExecution.Status.StateChangeReason' --output text)"
        echo "ERROR: $(basename "${file}") ${state}: ${reason}" >&2
        exit 1
        ;;
      *) sleep 3 ;;
    esac
  done
}

aws sts get-caller-identity >/dev/null
aws s3api head-bucket --bucket "${LAKEHOUSE_BUCKET}" >/dev/null
aws s3api head-bucket --bucket "${ARTIFACTS_BUCKET}" >/dev/null

for file in "${SQL_DIR}"/*.sql; do
  run_query "${file}"
done

echo
echo "Verifying Glue Catalog tables..."
aws glue get-table --region "${AWS_REGION}" --database-name iata_sales_iac_bronze --name sales_raw >/dev/null
aws glue get-table --region "${AWS_REGION}" --database-name iata_sales_iac_silver --name sales >/dev/null
echo "Bronze and Silver Iceberg tables are ready."

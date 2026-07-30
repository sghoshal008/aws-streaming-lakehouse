#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-ap-southeast-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-878670310452}"
PROJECT_NAME="${PROJECT_NAME:-yt-sales-iac}"

BOOTSTRAP_BUCKET="${BOOTSTRAP_BUCKET:-yt-sales-iac-bootstrap-${AWS_ACCOUNT_ID}}"
TEMPLATE_PREFIX="${TEMPLATE_PREFIX:-${PROJECT_NAME}/cloudformation}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-${PROJECT_NAME}-artifacts-${AWS_ACCOUNT_ID}}"
PLUGINS_BUCKET="${PLUGINS_BUCKET:-${PROJECT_NAME}-plugins-${AWS_ACCOUNT_ID}}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${INFRA_DIR}/.." && pwd)"

TEMPLATE_DIR="${INFRA_DIR}/templates"
BUILD_DIR="${REPO_ROOT}/build"

ACQUISITION_DIR="${REPO_ROOT}/app/acquisition"
LAMBDA_SOURCE="${ACQUISITION_DIR}/yt_sales_acquisition.py"
LAMBDA_ZIP="yt_sales_acquisition.zip"

LANDING_TO_MSK_SCRIPT="${REPO_ROOT}/app/glue/landing-to-msk/yt_sales_landing_to_msk.py"
BRONZE_TO_SILVER_SCRIPT="${REPO_ROOT}/app/glue/bronze-to-silver/yt_sales_bronze_to_silver.py"

JAR_DIR="${REPO_ROOT}/dependencies/glue/kafka"

PLUGIN_ZIP_PATH="${PLUGIN_ZIP_PATH:-${REPO_ROOT}/dependencies/msk-connect/iceberg/iceberg-kafka-connect-runtime-1.11.0-SNAPSHOT.zip}"
PLUGIN_S3_KEY="msk-connect/iceberg/yt-sales-iac-iceberg-sink-plugin.zip"

log() {
  printf '\n============================================================\n%s\n============================================================\n' "$1"
}

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: '$1' is required." >&2
    exit 1
  }
}

require_file() {
  [[ -f "$1" ]] || {
    echo "ERROR: Missing file: $1" >&2
    exit 1
  }
}

require_bucket() {
  local bucket="$1"

  if ! aws s3api head-bucket \
    --bucket "${bucket}" \
    --region "${AWS_REGION}" \
    >/dev/null 2>&1; then
    echo "ERROR: S3 bucket does not exist or is not accessible: ${bucket}" >&2
    exit 1
  fi
}

# ============================================================
# PREREQUISITES
# ============================================================

require aws
require python3
require zip

aws sts get-caller-identity >/dev/null

require_bucket "${BOOTSTRAP_BUCKET}"

# ============================================================
# VALIDATE REPOSITORY INPUTS
# ============================================================

log "Validate repository inputs"

for f in \
  network.yaml \
  storage.yaml \
  iam.yaml \
  messaging.yaml \
  compute.yaml \
  orchestration.yaml \
  monitoring.yaml \
  main.yaml
do
  require_file "${TEMPLATE_DIR}/${f}"

  aws cloudformation validate-template \
    --region "${AWS_REGION}" \
    --template-body "file://${TEMPLATE_DIR}/${f}" \
    >/dev/null

  echo "Validated ${f}"
done

require_file "${LAMBDA_SOURCE}"
require_file "${LANDING_TO_MSK_SCRIPT}"
require_file "${BRONZE_TO_SILVER_SCRIPT}"

for jar in \
  spark-sql-kafka-0-10_2.12-3.5.6.jar \
  spark-token-provider-kafka-0-10_2.12-3.5.6.jar \
  aws-msk-iam-auth-2.3.7-all.jar
do
  require_file "${JAR_DIR}/${jar}"
done

require_file "${PLUGIN_ZIP_PATH}"

echo "Validated Lambda source"
echo "Validated Glue scripts"
echo "Validated Kafka JARs"
echo "Validated Iceberg MSK Connect plugin ZIP"

# ============================================================
# UPLOAD NESTED CLOUDFORMATION TEMPLATES
# ============================================================

log "Upload nested CloudFormation templates"

for f in \
  network.yaml \
  storage.yaml \
  iam.yaml \
  messaging.yaml \
  compute.yaml \
  orchestration.yaml \
  monitoring.yaml
do
  aws s3 cp \
    "${TEMPLATE_DIR}/${f}" \
    "s3://${BOOTSTRAP_BUCKET}/${TEMPLATE_PREFIX}/${f}" \
    --region "${AWS_REGION}"
done

# ============================================================
# PHASE 1 CHECK
# ============================================================

if ! aws s3api head-bucket \
  --bucket "${ARTIFACTS_BUCKET}" \
  --region "${AWS_REGION}" \
  >/dev/null 2>&1; then

  log "Phase-1 resources not created yet"

  echo "Nested CloudFormation templates have been packaged."
  echo
  echo "Artifacts bucket does not exist yet:"
  echo "  ${ARTIFACTS_BUCKET}"
  echo
  echo "Run:"
  echo "  ./infra/scripts/deploy.sh phase1"
  echo
  echo "After Phase 1 completes, run package.sh again."
  exit 0
fi

require_bucket "${ARTIFACTS_BUCKET}"
require_bucket "${PLUGINS_BUCKET}"

# ============================================================
# PACKAGE ACQUISITION LAMBDA
# ============================================================

log "Package Acquisition Lambda"

rm -rf "${BUILD_DIR}/lambda/acquisition"
mkdir -p "${BUILD_DIR}/lambda/acquisition"

cp \
  "${LAMBDA_SOURCE}" \
  "${BUILD_DIR}/lambda/acquisition/yt_sales_acquisition.py"

if [[ -f "${ACQUISITION_DIR}/requirements.txt" ]] \
  && [[ -s "${ACQUISITION_DIR}/requirements.txt" ]] \
  && grep -Eq '^[[:space:]]*[^#[:space:]]' "${ACQUISITION_DIR}/requirements.txt"; then

  log "Install Lambda Python dependencies"

  python3 -m pip install \
    -r "${ACQUISITION_DIR}/requirements.txt" \
    -t "${BUILD_DIR}/lambda/acquisition"
fi

rm -f "${BUILD_DIR}/${LAMBDA_ZIP}"

(
  cd "${BUILD_DIR}/lambda/acquisition"
  zip -qr "../../${LAMBDA_ZIP}" .
)

aws s3 cp \
  "${BUILD_DIR}/${LAMBDA_ZIP}" \
  "s3://${ARTIFACTS_BUCKET}/lambda/${LAMBDA_ZIP}" \
  --region "${AWS_REGION}"

# ============================================================
# UPLOAD GLUE SCRIPTS
# ============================================================

log "Upload Glue scripts"

aws s3 cp \
  "${LANDING_TO_MSK_SCRIPT}" \
  "s3://${ARTIFACTS_BUCKET}/glue/scripts/yt_sales_landing_to_msk.py" \
  --region "${AWS_REGION}"

aws s3 cp \
  "${BRONZE_TO_SILVER_SCRIPT}" \
  "s3://${ARTIFACTS_BUCKET}/glue/scripts/yt_sales_bronze_to_silver.py" \
  --region "${AWS_REGION}"

# ============================================================
# UPLOAD GLUE KAFKA JARS
# ============================================================

log "Upload Glue Kafka JARs"

for jar in \
  spark-sql-kafka-0-10_2.12-3.5.6.jar \
  spark-token-provider-kafka-0-10_2.12-3.5.6.jar \
  aws-msk-iam-auth-2.3.7-all.jar
do
  aws s3 cp \
    "${JAR_DIR}/${jar}" \
    "s3://${ARTIFACTS_BUCKET}/glue/jars/${jar}" \
    --region "${AWS_REGION}"
done

# ============================================================
# UPLOAD SQL / DDL
# ============================================================

if [[ -d "${INFRA_DIR}/sql" ]]; then
  log "Upload SQL / DDL scripts"

  aws s3 sync \
    "${INFRA_DIR}/sql/" \
    "s3://${ARTIFACTS_BUCKET}/sql/" \
    --region "${AWS_REGION}"
fi

# ============================================================
# UPLOAD ICEBERG MSK CONNECT PLUGIN
# ============================================================

log "Upload Iceberg MSK Connect plugin"

echo "Local plugin:"
echo "  ${PLUGIN_ZIP_PATH}"
echo
echo "Destination:"
echo "  s3://${PLUGINS_BUCKET}/${PLUGIN_S3_KEY}"

aws s3 cp \
  "${PLUGIN_ZIP_PATH}" \
  "s3://${PLUGINS_BUCKET}/${PLUGIN_S3_KEY}" \
  --region "${AWS_REGION}"

# ============================================================
# VERIFY CRITICAL ARTIFACTS
# ============================================================

log "Verify uploaded artifacts"

aws s3api head-object \
  --bucket "${ARTIFACTS_BUCKET}" \
  --key "lambda/${LAMBDA_ZIP}" \
  --region "${AWS_REGION}" \
  >/dev/null

aws s3api head-object \
  --bucket "${ARTIFACTS_BUCKET}" \
  --key "glue/scripts/yt_sales_landing_to_msk.py" \
  --region "${AWS_REGION}" \
  >/dev/null

aws s3api head-object \
  --bucket "${ARTIFACTS_BUCKET}" \
  --key "glue/scripts/yt_sales_bronze_to_silver.py" \
  --region "${AWS_REGION}" \
  >/dev/null

for jar in \
  spark-sql-kafka-0-10_2.12-3.5.6.jar \
  spark-token-provider-kafka-0-10_2.12-3.5.6.jar \
  aws-msk-iam-auth-2.3.7-all.jar
do
  aws s3api head-object \
    --bucket "${ARTIFACTS_BUCKET}" \
    --key "glue/jars/${jar}" \
    --region "${AWS_REGION}" \
    >/dev/null
done

aws s3api head-object \
  --bucket "${PLUGINS_BUCKET}" \
  --key "${PLUGIN_S3_KEY}" \
  --region "${AWS_REGION}" \
  >/dev/null

# ============================================================
# COMPLETE
# ============================================================

log "Packaging complete"

echo "Nested templates:"
echo "  s3://${BOOTSTRAP_BUCKET}/${TEMPLATE_PREFIX}/"
echo

echo "Runtime artifacts:"
echo "  s3://${ARTIFACTS_BUCKET}/"
echo

echo "Iceberg MSK Connect plugin:"
echo "  s3://${PLUGINS_BUCKET}/${PLUGIN_S3_KEY}"
echo

echo "Package completed successfully."
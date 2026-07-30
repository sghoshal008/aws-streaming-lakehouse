#!/usr/bin/env bash

set -euo pipefail

# ============================================================
# yt Sales IaC - Deployment Script
#
# Usage:
#
#   Phase 1:
#     ./infra/scripts/deploy.sh phase1
#
#   Phase 2:
#     ./infra/scripts/deploy.sh phase2
#
# Optional:
#   Explicit MSK IAM bootstrap server override:
#
#     ./infra/scripts/deploy.sh phase2 \
#       "b-1.xxx.amazonaws.com:9098,b-2.xxx.amazonaws.com:9098"
#
# Phase 2 automatically:
#   - verifies runtime artifacts
#   - discovers MSK IAM bootstrap servers
#   - substitutes IaC placeholders in SQL
#   - bootstraps Iceberg tables through Athena
#   - verifies Glue Catalog tables
#   - deploys application resources
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================

AWS_REGION="${AWS_REGION:-ap-southeast-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-878670310452}"

PROJECT_NAME="${PROJECT_NAME:-yt-sales-iac}"
STACK_NAME="${STACK_NAME:-yt-sales-iac}"

MSK_CLUSTER_NAME="${MSK_CLUSTER_NAME:-${PROJECT_NAME}-msk}"

BOOTSTRAP_BUCKET="${BOOTSTRAP_BUCKET:-yt-sales-iac-bootstrap-${AWS_ACCOUNT_ID}}"
TEMPLATE_PREFIX="${TEMPLATE_PREFIX:-${PROJECT_NAME}/cloudformation}"

ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-${PROJECT_NAME}-artifacts-${AWS_ACCOUNT_ID}}"
PLUGINS_BUCKET="${PLUGINS_BUCKET:-${PROJECT_NAME}-plugins-${AWS_ACCOUNT_ID}}"
LAKEHOUSE_BUCKET="${LAKEHOUSE_BUCKET:-${PROJECT_NAME}-lakehouse-${AWS_ACCOUNT_ID}}"

ATHENA_OUTPUT_LOCATION="${ATHENA_OUTPUT_LOCATION:-s3://${ARTIFACTS_BUCKET}/athena-results/}"

ALERT_EMAIL="${ALERT_EMAIL:-}"

MAIN_TEMPLATE="${MAIN_TEMPLATE:-infra/templates/main.yaml}"

SQL_PREFIX="${SQL_PREFIX:-sql/iceberg}"


# ============================================================
# ARGUMENTS
# ============================================================

PHASE="${1:-}"

# Optional override.
# Normally Phase 2 discovers this automatically.
MSK_BOOTSTRAP_SERVERS="${2:-}"


if [[ -z "${PHASE}" ]]; then

    echo "Usage:"
    echo
    echo "  $0 phase1"
    echo "  $0 phase2"
    echo
    echo "Optional explicit bootstrap override:"
    echo
    echo "  $0 phase2 \"<MSK-IAM-bootstrap-servers>\""
    echo

    exit 1

fi


if [[ "${PHASE}" != "phase1" && "${PHASE}" != "phase2" ]]; then

    echo "ERROR: Deployment phase must be:"
    echo
    echo "  phase1"
    echo "or"
    echo "  phase2"

    exit 1

fi


# ============================================================
# HELPER FUNCTIONS
# ============================================================

section() {

    echo
    echo "============================================================"
    echo "$1"
    echo "============================================================"
    echo

}


require_s3_object() {

    local bucket="$1"
    local key="$2"

    aws s3api head-object \
        --bucket "${bucket}" \
        --key "${key}" \
        --region "${AWS_REGION}" \
        >/dev/null

}


verify_bucket() {

    local bucket="$1"

    if ! aws s3api head-bucket \
        --bucket "${bucket}" \
        --region "${AWS_REGION}" \
        >/dev/null 2>&1; then

        echo "ERROR: Cannot find or access S3 bucket:"
        echo "  ${bucket}"
        exit 1

    fi

}


run_athena_sql() {

    local sql_key="$1"

    echo
    echo "Executing:"
    echo "  s3://${ARTIFACTS_BUCKET}/${sql_key}"

    local sql

    sql=$(
        aws s3 cp \
            "s3://${ARTIFACTS_BUCKET}/${sql_key}" \
            - \
            --region "${AWS_REGION}"
    )


    if [[ -z "${sql}" ]]; then

        echo "ERROR: SQL file is empty:"
        echo "  ${sql_key}"

        exit 1

    fi


    # ========================================================
    # SUBSTITUTE IaC PLACEHOLDERS
    #
    # SQL files intentionally remain environment/account
    # independent. Example:
    #
    #   s3://__LAKEHOUSE_BUCKET__/bronze/sales_raw
    #
    # becomes:
    #
    #   s3://yt-sales-iac-lakehouse-<account>/bronze/sales_raw
    # ========================================================

    sql="${sql//__LAKEHOUSE_BUCKET__/${LAKEHOUSE_BUCKET}}"


    # ========================================================
    # SAFETY CHECK
    #
    # No unresolved IaC placeholders should reach Athena.
    # ========================================================

    if [[ "${sql}" == *"__LAKEHOUSE_BUCKET__"* ]]; then

        echo
        echo "ERROR: Unresolved __LAKEHOUSE_BUCKET__ placeholder."
        echo "SQL:"
        echo "  s3://${ARTIFACTS_BUCKET}/${sql_key}"

        exit 1

    fi


    if [[ "${sql}" == *"__"* ]]; then

        echo
        echo "WARNING:"
        echo "SQL may contain another unresolved placeholder:"
        echo
        echo "${sql}"
        echo

    fi


    echo
    echo "Resolved IaC placeholders:"
    echo "  __LAKEHOUSE_BUCKET__ -> ${LAKEHOUSE_BUCKET}"


    local query_execution_id

    query_execution_id=$(
        aws athena start-query-execution \
            --region "${AWS_REGION}" \
            --query-string "${sql}" \
            --result-configuration \
                "OutputLocation=${ATHENA_OUTPUT_LOCATION}" \
            --query "QueryExecutionId" \
            --output text
    )


    echo
    echo "Athena QueryExecutionId:"
    echo "  ${query_execution_id}"


    # --------------------------------------------------------
    # Wait for Athena query completion
    # --------------------------------------------------------

    while true; do

        local query_state

        query_state=$(
            aws athena get-query-execution \
                --query-execution-id "${query_execution_id}" \
                --region "${AWS_REGION}" \
                --query "QueryExecution.Status.State" \
                --output text
        )


        case "${query_state}" in

            SUCCEEDED)

                echo "Athena query succeeded."
                break
                ;;

            FAILED)

                local failure_reason

                failure_reason=$(
                    aws athena get-query-execution \
                        --query-execution-id "${query_execution_id}" \
                        --region "${AWS_REGION}" \
                        --query "QueryExecution.Status.StateChangeReason" \
                        --output text
                )

                echo
                echo "ERROR: Athena query failed."
                echo
                echo "SQL:"
                echo "  s3://${ARTIFACTS_BUCKET}/${sql_key}"
                echo
                echo "Resolved lakehouse bucket:"
                echo "  ${LAKEHOUSE_BUCKET}"
                echo
                echo "Reason:"
                echo "  ${failure_reason}"

                exit 1
                ;;

            CANCELLED)

                echo
                echo "ERROR: Athena query was cancelled."
                echo
                echo "SQL:"
                echo "  s3://${ARTIFACTS_BUCKET}/${sql_key}"

                exit 1
                ;;

            QUEUED|RUNNING)

                sleep 3
                ;;

            *)

                echo
                echo "ERROR: Unexpected Athena query state:"
                echo "  ${query_state}"

                exit 1
                ;;

        esac

    done

}


# ============================================================
# PRE-FLIGHT
# ============================================================

section "yt Sales IaC Deployment"

echo "Phase:              ${PHASE}"
echo "Stack:              ${STACK_NAME}"
echo "Region:             ${AWS_REGION}"
echo "Account:            ${AWS_ACCOUNT_ID}"
echo "Project:            ${PROJECT_NAME}"
echo "MSK cluster:        ${MSK_CLUSTER_NAME}"
echo "Bootstrap bucket:   ${BOOTSTRAP_BUCKET}"
echo "Template prefix:    ${TEMPLATE_PREFIX}"
echo "Artifacts bucket:   ${ARTIFACTS_BUCKET}"
echo "Plugins bucket:     ${PLUGINS_BUCKET}"
echo "Lakehouse bucket:   ${LAKEHOUSE_BUCKET}"

echo


# ============================================================
# VERIFY AWS ACCESS
# ============================================================

aws sts get-caller-identity >/dev/null


if [[ ! -f "${MAIN_TEMPLATE}" ]]; then

    echo "ERROR: main.yaml not found:"
    echo "  ${MAIN_TEMPLATE}"

    exit 1

fi


# ============================================================
# VALIDATE MAIN TEMPLATE
# ============================================================

echo "Validating main.yaml..."

aws cloudformation validate-template \
    --template-body "file://${MAIN_TEMPLATE}" \
    --region "${AWS_REGION}" \
    >/dev/null

echo "Validation passed."


# ============================================================
# DEPLOYMENT PARAMETERS
# ============================================================

COMMON_PARAMETERS=(

    "ProjectName=${PROJECT_NAME}"

    "AccountId=${AWS_ACCOUNT_ID}"

    "TemplateBucketName=${BOOTSTRAP_BUCKET}"

    "TemplatePrefix=${TEMPLATE_PREFIX}"

    "AlertEmail=${ALERT_EMAIL}"
)


# ============================================================
# PHASE 1
#
# Creates:
#   Network
#   Storage
#   IAM
#   SNS
#   MSK
#   Schema Registry
#
# Does NOT create:
#   Lambda
#   Glue
#   MSK Connect sink
#   Step Functions
#   Monitoring
# ============================================================

if [[ "${PHASE}" == "phase1" ]]; then

    section "Starting Phase 1 deployment"

    aws cloudformation deploy \
        --template-file "${MAIN_TEMPLATE}" \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --capabilities \
            CAPABILITY_NAMED_IAM \
            CAPABILITY_AUTO_EXPAND \
        --parameter-overrides \
            "${COMMON_PARAMETERS[@]}" \
            DeployApplication=false \
            DeployIcebergConnector=false \
            MSKBootstrapServers="" \
        --no-fail-on-empty-changeset


    section "Phase 1 deployment complete"

    echo "Next:"
    echo
    echo "1. Run:"
    echo
    echo "     ./infra/scripts/package.sh"
    echo
    echo "   This uploads:"
    echo "     - Acquisition Lambda ZIP"
    echo "     - Glue scripts"
    echo "     - Glue Kafka JARs"
    echo "     - Iceberg SQL / DDL"
    echo "     - Iceberg MSK Connect plugin ZIP"
    echo
    echo "2. Run:"
    echo
    echo "     ./infra/scripts/deploy.sh phase2"
    echo
    echo "   Phase 2 automatically:"
    echo "     - discovers MSK IAM bootstrap servers"
    echo "     - substitutes IaC SQL placeholders"
    echo "     - executes Iceberg DDL through Athena"
    echo "     - verifies Glue Catalog tables"
    echo "     - deploys application resources"
    echo

    exit 0

fi


# ============================================================
# PHASE 2
# ============================================================

if [[ "${PHASE}" == "phase2" ]]; then


    # ========================================================
    # VERIFY REQUIRED BUCKETS
    # ========================================================

    section "Phase 2 - Verify S3 buckets"

    echo "Checking artifacts bucket:"
    echo "  ${ARTIFACTS_BUCKET}"
    verify_bucket "${ARTIFACTS_BUCKET}"
    echo "Verified."

    echo
    echo "Checking plugins bucket:"
    echo "  ${PLUGINS_BUCKET}"
    verify_bucket "${PLUGINS_BUCKET}"
    echo "Verified."

    echo
    echo "Checking lakehouse bucket:"
    echo "  ${LAKEHOUSE_BUCKET}"
    verify_bucket "${LAKEHOUSE_BUCKET}"
    echo "Verified."

    echo
    echo "Required S3 buckets verified."


    # ========================================================
    # VERIFY RUNTIME ARTIFACTS
    # ========================================================

    section "Phase 2 - Verify runtime artifacts"

    require_s3_object \
        "${ARTIFACTS_BUCKET}" \
        "lambda/yt_sales_acquisition.zip"

    require_s3_object \
        "${ARTIFACTS_BUCKET}" \
        "glue/scripts/yt_sales_landing_to_msk.py"

    require_s3_object \
        "${ARTIFACTS_BUCKET}" \
        "glue/scripts/yt_sales_bronze_to_silver.py"

    require_s3_object \
        "${ARTIFACTS_BUCKET}" \
        "glue/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar"

    require_s3_object \
        "${ARTIFACTS_BUCKET}" \
        "glue/jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar"

    require_s3_object \
        "${ARTIFACTS_BUCKET}" \
        "glue/jars/aws-msk-iam-auth-2.3.7-all.jar"

    require_s3_object \
        "${PLUGINS_BUCKET}" \
        "msk-connect/iceberg/yt-sales-iac-iceberg-sink-plugin.zip"

    echo "Runtime artifacts verified."


    # ========================================================
    # VERIFY ICEBERG SQL FILES IN S3
    # ========================================================

    section "Phase 2 - Verify Iceberg SQL"

    ICEBERG_SQL_FILES=(

        "001-create-bronze.sql"

        "002-set-bronze-data-path.sql"

        "003-create-silver.sql"

        "004-set-silver-data-path.sql"

    )


    for sql_file in "${ICEBERG_SQL_FILES[@]}"; do

        require_s3_object \
            "${ARTIFACTS_BUCKET}" \
            "${SQL_PREFIX}/${sql_file}"

        echo "Found ${SQL_PREFIX}/${sql_file}"

    done


    echo
    echo "All Iceberg SQL files verified in S3."


    # ========================================================
    # DISCOVER MSK CLUSTER
    # ========================================================

    section "Phase 2 - Resolve MSK IAM bootstrap servers"


    if [[ -z "${MSK_BOOTSTRAP_SERVERS}" ]]; then

        echo "Discovering MSK cluster:"
        echo "  ${MSK_CLUSTER_NAME}"
        echo


        MSK_CLUSTER_ARN=$(
            aws kafka list-clusters-v2 \
                --region "${AWS_REGION}" \
                --query "ClusterInfoList[?ClusterName=='${MSK_CLUSTER_NAME}'].ClusterArn | [0]" \
                --output text
        )


        if [[ -z "${MSK_CLUSTER_ARN}" || "${MSK_CLUSTER_ARN}" == "None" ]]; then

            echo "ERROR: Unable to find MSK cluster:"
            echo "  ${MSK_CLUSTER_NAME}"

            exit 1

        fi


        echo "MSK cluster found:"
        echo "  ${MSK_CLUSTER_ARN}"


        # ====================================================
        # VERIFY MSK CLUSTER STATE
        # ====================================================

        MSK_CLUSTER_STATE=$(
            aws kafka describe-cluster-v2 \
                --cluster-arn "${MSK_CLUSTER_ARN}" \
                --region "${AWS_REGION}" \
                --query "ClusterInfo.State" \
                --output text
        )


        echo
        echo "MSK cluster state:"
        echo "  ${MSK_CLUSTER_STATE}"


        if [[ "${MSK_CLUSTER_STATE}" != "ACTIVE" ]]; then

            echo
            echo "ERROR: MSK cluster is not ACTIVE."
            echo "Current state:"
            echo "  ${MSK_CLUSTER_STATE}"

            exit 1

        fi


        # ====================================================
        # GET IAM BOOTSTRAP SERVERS
        # ====================================================

        echo
        echo "Retrieving MSK IAM bootstrap servers..."


        MSK_BOOTSTRAP_SERVERS=$(
            aws kafka get-bootstrap-brokers \
                --cluster-arn "${MSK_CLUSTER_ARN}" \
                --region "${AWS_REGION}" \
                --query "BootstrapBrokerStringSaslIam" \
                --output text
        )


        if [[ -z "${MSK_BOOTSTRAP_SERVERS}" || "${MSK_BOOTSTRAP_SERVERS}" == "None" ]]; then

            echo
            echo "ERROR: Unable to retrieve MSK IAM bootstrap servers."
            echo
            echo "Verify IAM authentication is enabled."

            exit 1

        fi


        echo
        echo "MSK IAM bootstrap servers retrieved successfully."

    else

        echo "Using explicitly supplied MSK IAM bootstrap servers."

    fi


    if [[ "${MSK_BOOTSTRAP_SERVERS}" != *":9098"* ]]; then

        echo
        echo "WARNING:"
        echo "Bootstrap server string does not contain port 9098."
        echo "Expected IAM-authenticated MSK bootstrap brokers."
        echo

    fi


    echo
    echo "MSK IAM bootstrap servers:"
    echo "  ${MSK_BOOTSTRAP_SERVERS}"


    # ========================================================
    # BOOTSTRAP ICEBERG TABLES
    # ========================================================

    section "Phase 2 - Bootstrap Iceberg tables"

    echo "Athena result location:"
    echo "  ${ATHENA_OUTPUT_LOCATION}"

    echo
    echo "Lakehouse bucket:"
    echo "  ${LAKEHOUSE_BUCKET}"

    echo
    echo "SQL placeholder substitution:"
    echo "  __LAKEHOUSE_BUCKET__ -> ${LAKEHOUSE_BUCKET}"

    echo
    echo "Executing Iceberg DDL in sequence..."


    for sql_file in "${ICEBERG_SQL_FILES[@]}"; do

        run_athena_sql "${SQL_PREFIX}/${sql_file}"

    done


    echo
    echo "Iceberg DDL execution complete."


    # ========================================================
    # VERIFY GLUE CATALOG TABLES
    # ========================================================

    section "Phase 2 - Verify Iceberg tables"


    echo "Checking:"
    echo "  yt_sales_iac_bronze.sales_raw"


    aws glue get-table \
        --region "${AWS_REGION}" \
        --database-name yt_sales_iac_bronze \
        --name sales_raw \
        >/dev/null


    echo "Verified:"
    echo "  yt_sales_iac_bronze.sales_raw"


    echo
    echo "Checking:"
    echo "  yt_sales_iac_silver.sales"


    aws glue get-table \
        --region "${AWS_REGION}" \
        --database-name yt_sales_iac_silver \
        --name sales \
        >/dev/null


    echo "Verified:"
    echo "  yt_sales_iac_silver.sales"


    echo
    echo "Iceberg tables verified successfully."


    # ========================================================
    # PHASE 2 PRE-FLIGHT COMPLETE
    # ========================================================

    section "Phase 2 pre-flight checks passed"


    echo "Starting Phase 2 CloudFormation deployment..."
    echo


    # ========================================================
    # DEPLOY PHASE 2
    # ========================================================

    aws cloudformation deploy \
        --template-file "${MAIN_TEMPLATE}" \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --capabilities \
            CAPABILITY_NAMED_IAM \
            CAPABILITY_AUTO_EXPAND \
        --parameter-overrides \
            "${COMMON_PARAMETERS[@]}" \
            DeployApplication=true \
            DeployIcebergConnector=true \
            "MSKBootstrapServers=${MSK_BOOTSTRAP_SERVERS}" \
        --no-fail-on-empty-changeset


    # ========================================================
    # COMPLETE
    # ========================================================

    section "Phase 2 deployment complete"


    echo "The IaC application stack is now deployed."
    echo

    echo "MSK cluster:"
    echo "  ${MSK_CLUSTER_NAME}"
    echo

    echo "Bronze Iceberg table:"
    echo "  yt_sales_iac_bronze.sales_raw"
    echo

    echo "Silver Iceberg table:"
    echo "  yt_sales_iac_silver.sales"
    echo

    echo "Lakehouse bucket:"
    echo "  s3://${LAKEHOUSE_BUCKET}"
    echo

    echo "MSK IAM bootstrap servers:"
    echo "  ${MSK_BOOTSTRAP_SERVERS}"
    echo

fi
#!/usr/bin/env bash

set -euo pipefail

# ============================================================
# yt Sales IaC - Full Deployment Wrapper
#
# Usage:
#
#   ./infra/scripts/deployment.sh create
#   ./infra/scripts/deployment.sh delete
#
# Optional environment variables:
#
#   AWS_REGION=ap-southeast-1
#   AWS_ACCOUNT_ID=878670310452
#   PROJECT_NAME=yt-sales-iac
#   STACK_NAME=yt-sales-iac
#
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================

AWS_REGION="${AWS_REGION:-ap-southeast-1}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-878670310452}"

PROJECT_NAME="${PROJECT_NAME:-yt-sales-iac}"
STACK_NAME="${STACK_NAME:-yt-sales-iac}"

BOOTSTRAP_BUCKET="${BOOTSTRAP_BUCKET:-yt-sales-iac-bootstrap-${AWS_ACCOUNT_ID}}"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:-${PROJECT_NAME}-artifacts-${AWS_ACCOUNT_ID}}"
PLUGINS_BUCKET="${PLUGINS_BUCKET:-${PROJECT_NAME}-plugins-${AWS_ACCOUNT_ID}}"

MSK_CLUSTER_NAME="${MSK_CLUSTER_NAME:-${PROJECT_NAME}-msk}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${INFRA_DIR}/.." && pwd)"

PACKAGE_SCRIPT="${SCRIPT_DIR}/package.sh"
DEPLOY_SCRIPT="${SCRIPT_DIR}/deploy.sh"
VALIDATE_SCRIPT="${SCRIPT_DIR}/validate.sh"

ACTION="${1:-}"


# ============================================================
# HELPERS
# ============================================================

log() {
    printf '\n'
    printf '============================================================\n'
    printf '%s\n' "$1"
    printf '============================================================\n'
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || \
        die "Required command '$1' was not found."
}

require_file() {
    [[ -f "$1" ]] || die "Required file not found: $1"
}

run_repository_validation() {
    local validation_log
    local validation_rc

    validation_log="$(mktemp)"

    # validate.sh may use cfn-lint configured to return a non-zero exit
    # code for warnings (for example W3005). We want:
    #
    #   - CloudFormation/cfn-lint ERRORS  -> fail deployment
    #   - warning-only output             -> print warnings and continue
    #   - any other unexplained failure   -> fail deployment
    #
    # Temporarily disable `set -e` behavior around this command so that
    # we can inspect the validation result ourselves.
    set +e
    "${VALIDATE_SCRIPT}" 2>&1 | tee "${validation_log}"
    validation_rc="${PIPESTATUS[0]}"
    set -e

    if [[ "${validation_rc}" -eq 0 ]]; then
        rm -f "${validation_log}"
        return 0
    fi

    # cfn-lint error codes are rendered like E3002, E1019, etc.
    # Also treat explicit ERROR/FATAL output as a real validation failure.
    if grep -Eq '(^|[[:space:]])E[0-9]{4}([[:space:]]|$)|(^|[[:space:]])(ERROR|FATAL)(:|[[:space:]])' "${validation_log}"; then
        echo
        echo "Repository validation contains errors."
        rm -f "${validation_log}"
        return "${validation_rc}"
    fi

    # If the non-zero result contains cfn-lint warnings only, continue.
    if grep -Eq '(^|[[:space:]])W[0-9]{4}([[:space:]]|$)' "${validation_log}"; then
        echo
        echo "Repository validation completed with warnings only."
        echo "Warnings do not block deployment."
        rm -f "${validation_log}"
        return 0
    fi

    echo
    echo "Repository validation failed with exit code ${validation_rc}."
    echo "The failure was not recognized as warning-only output."
    rm -f "${validation_log}"
    return "${validation_rc}"
}

stack_exists() {
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        >/dev/null 2>&1
}

bucket_exists() {
    aws s3api head-bucket \
        --bucket "$1" \
        >/dev/null 2>&1
}

get_stack_status() {
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query "Stacks[0].StackStatus" \
        --output text
}

show_failed_stack_events() {
    echo
    echo "Recent CloudFormation failure events:"
    echo

    local status

    for status in CREATE_FAILED UPDATE_FAILED DELETE_FAILED ROLLBACK_IN_PROGRESS; do
        aws cloudformation describe-stack-events \
            --stack-name "${STACK_NAME}" \
            --region "${AWS_REGION}" \
            --query "StackEvents[?ResourceStatus=='${status}'].[Timestamp,LogicalResourceId,ResourceType,PhysicalResourceId,ResourceStatusReason]" \
            --output table \
            || true
    done
}

empty_bucket() {
    local bucket="$1"

    if ! bucket_exists "${bucket}"; then
        echo "Bucket does not exist: ${bucket}"
        return 0
    fi

    echo "Emptying bucket: ${bucket}"

    aws s3 rm \
        "s3://${bucket}" \
        --recursive \
        --region "${AWS_REGION}" \
        || true

    # Handle versioned buckets as well.
    while true; do

        local versions_json

        versions_json="$(
            aws s3api list-object-versions \
                --bucket "${bucket}" \
                --region "${AWS_REGION}" \
                --output json
        )"

        local delete_payload

        delete_payload="$(
            python3 -c '
import json
import sys

data = json.load(sys.stdin)

objects = []

for version in data.get("Versions", []):
    objects.append({
        "Key": version["Key"],
        "VersionId": version["VersionId"]
    })

for marker in data.get("DeleteMarkers", []):
    objects.append({
        "Key": marker["Key"],
        "VersionId": marker["VersionId"]
    })

print(json.dumps({
    "Objects": objects,
    "Quiet": True
}))
' <<< "${versions_json}"
        )"

        local count

        count="$(
            python3 -c '
import json
import sys

print(len(json.load(sys.stdin)["Objects"]))
' <<< "${delete_payload}"
        )"

        if [[ "${count}" == "0" ]]; then
            break
        fi

        aws s3api delete-objects \
            --bucket "${bucket}" \
            --delete "${delete_payload}" \
            --region "${AWS_REGION}" \
            >/dev/null

    done
}

delete_bucket_if_exists() {
    local bucket="$1"

    if ! bucket_exists "${bucket}"; then
        echo "Bucket already absent: ${bucket}"
        return 0
    fi

    empty_bucket "${bucket}"

    echo "Deleting bucket: ${bucket}"

    aws s3api delete-bucket \
        --bucket "${bucket}" \
        --region "${AWS_REGION}"
}


table_exists() {
    aws dynamodb describe-table \
        --table-name "$1" \
        --region "${AWS_REGION}" \
        >/dev/null 2>&1
}

delete_table_if_exists() {
    local table="$1"

    if ! table_exists "${table}"; then
        echo "DynamoDB table already absent: ${table}"
        return 0
    fi

    echo "Deleting DynamoDB table: ${table}"

    aws dynamodb delete-table \
        --table-name "${table}" \
        --region "${AWS_REGION}" \
        >/dev/null

    aws dynamodb wait table-not-exists \
        --table-name "${table}" \
        --region "${AWS_REGION}"
}

get_project_vpc_id() {
    if stack_exists; then
        local vpc_id

        vpc_id="$(
            aws cloudformation describe-stacks \
                --stack-name "${STACK_NAME}" \
                --region "${AWS_REGION}" \
                --query "Stacks[0].Outputs[?OutputKey=='VpcId'].OutputValue | [0]" \
                --output text \
                2>/dev/null || true
        )"

        if [[ -n "${vpc_id}" && "${vpc_id}" != "None" ]]; then
            echo "${vpc_id}"
            return 0
        fi
    fi

    aws ec2 describe-vpcs \
        --region "${AWS_REGION}" \
        --filters "Name=tag:Name,Values=${PROJECT_NAME}-vpc" \
        --query "Vpcs[0].VpcId" \
        --output text \
        2>/dev/null || true
}

cleanup_project_vpc_extras() {
    local vpc_id="$1"

    if [[ -z "${vpc_id}" || "${vpc_id}" == "None" ]]; then
        echo "No project VPC found; skipping project-scoped VPC extra cleanup."
        return 0
    fi

    echo "Checking project-scoped EC2 instances in VPC ${vpc_id}..."

    local instance_ids

    instance_ids="$(
        aws ec2 describe-instances \
            --region "${AWS_REGION}" \
            --filters \
                "Name=vpc-id,Values=${vpc_id}" \
                "Name=instance-state-name,Values=pending,running,stopping,stopped" \
                "Name=tag:Name,Values=${PROJECT_NAME}*" \
            --query "Reservations[].Instances[].InstanceId" \
            --output text \
            2>/dev/null || true
    )"

    if [[ -n "${instance_ids}" && "${instance_ids}" != "None" ]]; then
        echo "Terminating project-scoped EC2 instance(s): ${instance_ids}"

        # shellcheck disable=SC2086
        aws ec2 terminate-instances \
            --instance-ids ${instance_ids} \
            --region "${AWS_REGION}" \
            >/dev/null

        # shellcheck disable=SC2086
        aws ec2 wait instance-terminated \
            --instance-ids ${instance_ids} \
            --region "${AWS_REGION}"
    else
        echo "No project-scoped EC2 instances found."
    fi

    echo "Checking non-default project security groups in VPC ${vpc_id}..."

    local sg_ids

    sg_ids="$(
        aws ec2 describe-security-groups \
            --region "${AWS_REGION}" \
            --filters \
                "Name=vpc-id,Values=${vpc_id}" \
                "Name=group-name,Values=${PROJECT_NAME}*" \
            --query "SecurityGroups[?GroupName!='default'].GroupId" \
            --output text \
            2>/dev/null || true
    )"

    if [[ -n "${sg_ids}" && "${sg_ids}" != "None" ]]; then
        local sg_id

        for sg_id in ${sg_ids}; do
            echo "Deleting project-scoped security group: ${sg_id}"

            aws ec2 delete-security-group \
                --group-id "${sg_id}" \
                --region "${AWS_REGION}" \
                >/dev/null 2>&1 \
                || echo "  Deferred ${sg_id} to CloudFormation or remaining dependency cleanup."
        done
    else
        echo "No extra project-scoped security groups found."
    fi
}

verify_project_resources_absent() {
    local failed=0
    local bucket

    echo
    echo "Final teardown verification:"

    for bucket in \
        "${ARTIFACTS_BUCKET}" \
        "${PLUGINS_BUCKET}" \
        "${PROJECT_NAME}-landing-${AWS_ACCOUNT_ID}" \
        "${PROJECT_NAME}-lakehouse-${AWS_ACCOUNT_ID}"
    do
        if bucket_exists "${bucket}"; then
            echo "  STILL EXISTS: s3://${bucket}"
            failed=1
        else
            echo "  GONE: s3://${bucket}"
        fi
    done

    local control_table="${PROJECT_NAME}-ingestion-control"

    if table_exists "${control_table}"; then
        echo "  STILL EXISTS: DynamoDB ${control_table}"
        failed=1
    else
        echo "  GONE: DynamoDB ${control_table}"
    fi

    if stack_exists; then
        echo "  STILL EXISTS: CloudFormation stack ${STACK_NAME}"
        failed=1
    else
        echo "  GONE: CloudFormation stack ${STACK_NAME}"
    fi

    if [[ "${failed}" -ne 0 ]]; then
        die "Teardown verification failed. One or more project resources still exist."
    fi
}


# ============================================================
# ARGUMENT VALIDATION
# ============================================================

if [[ -z "${ACTION}" ]]; then
    echo
    echo "Usage:"
    echo
    echo "  $0 create"
    echo "  $0 delete"
    echo
    exit 1
fi

case "${ACTION}" in
    create|delete)
        ;;
    *)
        die "Invalid action '${ACTION}'. Use 'create' or 'delete'."
        ;;
esac


# ============================================================
# PRE-FLIGHT
# ============================================================

cd "${REPO_ROOT}"

require_command aws
require_command python3
require_command zip

require_file "${PACKAGE_SCRIPT}"
require_file "${DEPLOY_SCRIPT}"
require_file "${VALIDATE_SCRIPT}"

aws sts get-caller-identity >/dev/null

ACTUAL_ACCOUNT_ID="$(
    aws sts get-caller-identity \
        --query Account \
        --output text
)"

if [[ "${ACTUAL_ACCOUNT_ID}" != "${AWS_ACCOUNT_ID}" ]]; then
    die "AWS account mismatch. Expected ${AWS_ACCOUNT_ID}, connected to ${ACTUAL_ACCOUNT_ID}."
fi


# ============================================================
# CREATE
# ============================================================

create_environment() {

    log "yt Sales IaC - Full Deployment"

    echo "Action:             CREATE"
    echo "Region:             ${AWS_REGION}"
    echo "Account:            ${AWS_ACCOUNT_ID}"
    echo "Project:            ${PROJECT_NAME}"
    echo "Stack:              ${STACK_NAME}"
    echo "Bootstrap bucket:   ${BOOTSTRAP_BUCKET}"
    echo "Artifacts bucket:   ${ARTIFACTS_BUCKET}"
    echo "Plugins bucket:     ${PLUGINS_BUCKET}"
    echo

    # --------------------------------------------------------
    # Check existing stack
    # --------------------------------------------------------

    if stack_exists; then

        CURRENT_STATUS="$(get_stack_status)"

        echo "Existing stack detected: ${CURRENT_STATUS}"

        case "${CURRENT_STATUS}" in

            ROLLBACK_COMPLETE|CREATE_FAILED|ROLLBACK_FAILED)

                die \
                    "Stack is in ${CURRENT_STATUS}. Run '$0 delete' first."

                ;;

            DELETE_IN_PROGRESS)

                die \
                    "Stack deletion is currently in progress. Wait for it to complete."

                ;;

            *)

                echo "Existing stack will be updated where necessary."

                ;;

        esac

    fi


    # --------------------------------------------------------
    # Local validation
    # --------------------------------------------------------

    log "Step 1/7 - Validate Repository"

    if ! run_repository_validation; then
        die "Repository validation failed."
    fi

    echo
    echo "Repository validation passed (warnings, if any, were non-blocking)."


    # --------------------------------------------------------
    # Bootstrap bucket
    # --------------------------------------------------------

    log "Step 2/7 - Ensure Bootstrap Bucket"

    if bucket_exists "${BOOTSTRAP_BUCKET}"; then

        echo "Bootstrap bucket already exists:"
        echo "s3://${BOOTSTRAP_BUCKET}"

    else

        echo "Creating bootstrap bucket:"
        echo "s3://${BOOTSTRAP_BUCKET}"

        if [[ "${AWS_REGION}" == "us-east-1" ]]; then

            aws s3api create-bucket \
                --bucket "${BOOTSTRAP_BUCKET}" \
                --region "${AWS_REGION}"

        else

            aws s3api create-bucket \
                --bucket "${BOOTSTRAP_BUCKET}" \
                --region "${AWS_REGION}" \
                --create-bucket-configuration \
                    "LocationConstraint=${AWS_REGION}"

        fi

        aws s3api put-public-access-block \
            --bucket "${BOOTSTRAP_BUCKET}" \
            --public-access-block-configuration \
                BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

        echo "Bootstrap bucket created."

    fi


    # --------------------------------------------------------
    # Package #1
    # --------------------------------------------------------

    log "Step 3/7 - Package CloudFormation Templates"

    "${PACKAGE_SCRIPT}"


    # --------------------------------------------------------
    # Phase 1
    # --------------------------------------------------------

    log "Step 4/7 - Deploy Phase 1"

    if ! "${DEPLOY_SCRIPT}" phase1; then

        echo
        echo "Phase 1 deployment failed."

        if stack_exists; then
            show_failed_stack_events
        fi

        exit 1

    fi

    echo
    echo "Phase 1 completed successfully."


    # --------------------------------------------------------
    # Package #2
    # --------------------------------------------------------

    log "Step 5/7 - Package Runtime Artifacts"

    "${PACKAGE_SCRIPT}"

    echo
    echo "Runtime artifacts packaged."


    # --------------------------------------------------------
    # Phase 2
    # --------------------------------------------------------

    log "Step 6/7 - Deploy Phase 2"

    if ! "${DEPLOY_SCRIPT}" phase2; then

        echo
        echo "Phase 2 deployment failed."

        if stack_exists; then
            show_failed_stack_events
        fi

        exit 1

    fi

    echo
    echo "Phase 2 completed successfully."


    # --------------------------------------------------------
    # Verification
    # --------------------------------------------------------

    log "Step 7/7 - Verify Deployment"

    FINAL_STATUS="$(get_stack_status)"

    echo "CloudFormation stack:"
    echo "  ${STACK_NAME}"
    echo
    echo "Status:"
    echo "  ${FINAL_STATUS}"
    echo

    if [[ "${FINAL_STATUS}" != "CREATE_COMPLETE" &&
          "${FINAL_STATUS}" != "UPDATE_COMPLETE" ]]; then

        die "Unexpected final stack status: ${FINAL_STATUS}"

    fi


    echo "Checking Iceberg Bronze table..."

    aws glue get-table \
        --region "${AWS_REGION}" \
        --database-name yt_sales_iac_bronze \
        --name sales_raw \
        >/dev/null

    echo "  OK: yt_sales_iac_bronze.sales_raw"


    echo
    echo "Checking Iceberg Silver table..."

    aws glue get-table \
        --region "${AWS_REGION}" \
        --database-name yt_sales_iac_silver \
        --name sales \
        >/dev/null

    echo "  OK: yt_sales_iac_silver.sales"


    echo
    echo "Checking MSK cluster..."

    MSK_STATE="$(
        aws kafka list-clusters-v2 \
            --region "${AWS_REGION}" \
            --query \
            "ClusterInfoList[?ClusterName=='${MSK_CLUSTER_NAME}'].State | [0]" \
            --output text
    )"

    if [[ "${MSK_STATE}" != "ACTIVE" ]]; then
        die "MSK cluster is not ACTIVE. Current state: ${MSK_STATE}"
    fi

    echo "  OK: ${MSK_CLUSTER_NAME} = ACTIVE"


    echo
    echo "Checking MSK Connect connector..."

    CONNECTOR_STATE="$(
        aws kafkaconnect list-connectors \
            --region "${AWS_REGION}" \
            --query \
            "connectors[?connectorName=='${PROJECT_NAME}-iceberg-sink'].connectorState | [0]" \
            --output text
    )"

    echo "  ${PROJECT_NAME}-iceberg-sink = ${CONNECTOR_STATE}"


    log "Deployment Complete"

    echo "yt Sales IaC deployment completed successfully."
    echo
    echo "Stack:"
    echo "  ${STACK_NAME}"
    echo
    echo "Region:"
    echo "  ${AWS_REGION}"
    echo
    echo "Bronze:"
    echo "  yt_sales_iac_bronze.sales_raw"
    echo
    echo "Silver:"
    echo "  yt_sales_iac_silver.sales"
    echo
    echo "Next:"
    echo "  Run the ingestion Step Functions workflow."
    echo
}


# ============================================================
# DELETE
# ============================================================

delete_environment() {

    log "yt Sales IaC - Full Teardown"

    echo "Action:             DELETE"
    echo "Region:             ${AWS_REGION}"
    echo "Account:            ${AWS_ACCOUNT_ID}"
    echo "Project:            ${PROJECT_NAME}"
    echo "Stack:              ${STACK_NAME}"
    echo

    echo "WARNING:"
    echo
    echo "This removes the full project environment and project data."
    echo
    echo "It includes:"
    echo "  - MSK and MSK Connect"
    echo "  - Lambda and Glue"
    echo "  - Step Functions"
    echo "  - DynamoDB ingestion control"
    echo "  - project S3 buckets and all object versions"
    echo "  - project networking resources"
    echo "  - project-scoped EC2 debug instances / security groups"
    echo "  - bootstrap bucket"
    echo
    echo "Only EC2 instances / SGs whose Name/group name starts with '${PROJECT_NAME}'"
    echo "inside the project VPC are considered for extra cleanup."
    echo

    read -r -p "Type DELETE to continue: " CONFIRMATION

    if [[ "${CONFIRMATION}" != "DELETE" ]]; then
        echo "Deletion cancelled."
        exit 0
    fi

    local landing_bucket="${PROJECT_NAME}-landing-${AWS_ACCOUNT_ID}"
    local lakehouse_bucket="${PROJECT_NAME}-lakehouse-${AWS_ACCOUNT_ID}"
    local control_table="${PROJECT_NAME}-ingestion-control"
    local project_vpc_id
    local bucket

    project_vpc_id="$(get_project_vpc_id)"

    log "Step 1/6 - Empty Project S3 Buckets"

    for bucket in \
        "${ARTIFACTS_BUCKET}" \
        "${PLUGINS_BUCKET}" \
        "${landing_bucket}" \
        "${lakehouse_bucket}"
    do
        if bucket_exists "${bucket}"; then
            empty_bucket "${bucket}"
        else
            echo "Bucket already absent: ${bucket}"
        fi
    done

    log "Step 2/6 - Clean Project VPC Extras"

    cleanup_project_vpc_extras "${project_vpc_id}"

    log "Step 3/6 - Delete CloudFormation Stack"

    if stack_exists; then

        echo "Deleting stack:"
        echo "  ${STACK_NAME}"

        aws cloudformation delete-stack \
            --stack-name "${STACK_NAME}" \
            --region "${AWS_REGION}"

        echo
        echo "Waiting for stack deletion."
        echo "MSK deletion can take several minutes..."

        if ! aws cloudformation wait stack-delete-complete \
            --stack-name "${STACK_NAME}" \
            --region "${AWS_REGION}"; then

            echo
            echo "CloudFormation deletion failed."
            show_failed_stack_events

            echo
            echo "Attempting one more project-scoped VPC dependency cleanup..."

            project_vpc_id="$(get_project_vpc_id)"
            cleanup_project_vpc_extras "${project_vpc_id}"

            echo
            echo "Retrying CloudFormation deletion..."

            aws cloudformation delete-stack \
                --stack-name "${STACK_NAME}" \
                --region "${AWS_REGION}"

            if ! aws cloudformation wait stack-delete-complete \
                --stack-name "${STACK_NAME}" \
                --region "${AWS_REGION}"; then
                echo
                echo "CloudFormation deletion failed after retry."
                show_failed_stack_events
                exit 1
            fi
        fi

        echo
        echo "CloudFormation stack deleted."

    else
        echo "Stack does not exist: ${STACK_NAME}"
    fi

    log "Step 4/6 - Delete Named Project Leftovers"

    for bucket in \
        "${ARTIFACTS_BUCKET}" \
        "${PLUGINS_BUCKET}" \
        "${landing_bucket}" \
        "${lakehouse_bucket}"
    do
        delete_bucket_if_exists "${bucket}"
    done

    delete_table_if_exists "${control_table}"

    log "Step 5/6 - Delete Bootstrap Bucket"

    delete_bucket_if_exists "${BOOTSTRAP_BUCKET}"

    log "Step 6/6 - Verify Teardown"

    verify_project_resources_absent

    log "Teardown Complete"

    echo "yt Sales IaC environment has been removed."
    echo
    echo "Stack:"
    echo "  ${STACK_NAME}"
    echo
    echo "Bootstrap bucket:"
    echo "  ${BOOTSTRAP_BUCKET}"
    echo
    echo "You can recreate everything with:"
    echo
    echo "  ./infra/scripts/deploy-wrapper.sh create"
    echo
}


# ============================================================
# MAIN
# ============================================================

case "${ACTION}" in

    create)
        create_environment
        ;;

    delete)
        delete_environment
        ;;

esac
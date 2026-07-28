#!/usr/bin/env bash
set -euo pipefail
AWS_REGION="${AWS_REGION:-ap-southeast-1}"
STACK_NAME="${STACK_NAME:-iata-sales-iac}"
CLUSTER_ARN="$(aws cloudformation describe-stacks --region "${AWS_REGION}" --stack-name "${STACK_NAME}" --query "Stacks[0].Outputs[?OutputKey=='MSKClusterArn'].OutputValue | [0]" --output text)"
[[ -n "${CLUSTER_ARN}" && "${CLUSTER_ARN}" != "None" ]] || { echo "MSKClusterArn output not found." >&2; exit 1; }
aws kafka get-bootstrap-brokers --region "${AWS_REGION}" --cluster-arn "${CLUSTER_ARN}" --query 'BootstrapBrokerStringSaslIam' --output text

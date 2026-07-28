# Deployment Runbook

## Prerequisites

- AWS CLI configured and authenticated
- Python 3
- Bash
- `cfn-lint`
- Sufficient AWS permissions; AdministratorAccess is simplest for the isolated case-study/demo account

```bash
export AWS_REGION="${AWS_REGION:-ap-southeast-1}"
export AWS_ACCOUNT_ID="<AWS_ACCOUNT_ID>"
export ALERT_EMAIL="<ALERT_EMAIL>"

aws sts get-caller-identity
```

## Local Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install cfn-lint
```

Do not commit `.venv/`.

## Validate

```bash
./infra/scripts/validate.sh
```

The GitHub validation workflow can also be run manually. The deploy workflow is intentionally demo-only/non-executing; deployment is performed with the local wrapper.

## Create

```bash
./infra/scripts/deploy-wrapper.sh create
```

The wrapper performs repository validation, packaging and the phased CloudFormation deployment.

## Verify

```bash
aws cloudformation describe-stacks   --stack-name iata-sales-iac   --region "$AWS_REGION"   --query "Stacks[0].[StackStatus,StackStatusReason]"   --output table
```

```bash
aws cloudformation describe-stacks   --stack-name iata-sales-iac   --region "$AWS_REGION"   --query "Stacks[0].Outputs"   --output table
```

```bash
aws cloudformation describe-stack-resources   --stack-name iata-sales-iac   --region "$AWS_REGION"   --output table
```

## Diagnose CloudFormation Failures

Creation:

```bash
aws cloudformation describe-stack-events   --stack-name iata-sales-iac   --region "$AWS_REGION"   --query "StackEvents[?ResourceStatus=='CREATE_FAILED'].[Timestamp,LogicalResourceId,ResourceType,PhysicalResourceId,ResourceStatusReason]"   --output table
```

Deletion:

```bash
aws cloudformation describe-stack-events   --stack-name iata-sales-iac   --region "$AWS_REGION"   --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[Timestamp,LogicalResourceId,ResourceType,PhysicalResourceId,ResourceStatusReason]"   --output table
```

For a failed nested stack, rerun the command against the nested-stack ARN reported by the root stack.

## Delete

```bash
./infra/scripts/deploy-wrapper.sh delete
```

The wrapper is preferred because persistent S3 objects and other dependencies may need cleanup before CloudFormation can delete the stack.

## Demo Execution

EventBridge scheduling is intentionally disabled for the demo. Start the required Step Functions manually so individual stages can be demonstrated and inspected.

See `operations.md` for runtime verification and troubleshooting commands.

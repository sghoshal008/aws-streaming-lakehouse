# Operations & Interview Reference

```bash
export AWS_REGION="${AWS_REGION:-ap-southeast-1}"
export AWS_ACCOUNT_ID="<AWS_ACCOUNT_ID>"
export CONTROL_TABLE="iata-sales-iac-ingestion-control"
```

## CloudFormation

```bash
aws cloudformation describe-stacks --stack-name iata-sales-iac --region "$AWS_REGION"   --query "Stacks[0].[StackStatus,StackStatusReason]" --output table

aws cloudformation describe-stacks --stack-name iata-sales-iac --region "$AWS_REGION"   --query "Stacks[0].Outputs" --output table

aws cloudformation describe-stack-resources --stack-name iata-sales-iac   --region "$AWS_REGION" --output table
```

Failed creation events:

```bash
aws cloudformation describe-stack-events --stack-name iata-sales-iac   --region "$AWS_REGION"   --query "StackEvents[?ResourceStatus=='CREATE_FAILED'].[Timestamp,LogicalResourceId,ResourceType,ResourceStatusReason]"   --output table
```

## Step Functions

```bash
aws stepfunctions list-state-machines --region "$AWS_REGION"   --query "stateMachines[*].[name,stateMachineArn]" --output table

aws stepfunctions list-executions --state-machine-arn <STATE_MACHINE_ARN>   --region "$AWS_REGION" --max-results 10

aws stepfunctions describe-execution --execution-arn <EXECUTION_ARN>   --region "$AWS_REGION"

aws stepfunctions get-execution-history --execution-arn <EXECUTION_ARN>   --region "$AWS_REGION"
```

## DynamoDB

Ingestion runs:

```bash
aws dynamodb scan --table-name "$CONTROL_TABLE" --region "$AWS_REGION"   --filter-expression "begins_with(pk, :p)"   --expression-attribute-values '{":p":{"S":"RUN#"}}'
```

One ingestion run:

```bash
aws dynamodb get-item --table-name "$CONTROL_TABLE" --region "$AWS_REGION"   --key '{"pk":{"S":"RUN#<RUN_ID>"},"sk":{"S":"METADATA"}}'
```

Bronze-to-Silver watermark:

```bash
aws dynamodb get-item --table-name "$CONTROL_TABLE" --region "$AWS_REGION"   --key '{"pk":{"S":"PIPELINE#BRONZE_TO_SILVER"},"sk":{"S":"WATERMARK"}}'
```

Important watermark fields: `last_snapshot_id`, `bronze_records_read`, `valid_records`, `quarantine_records`, `duplicates_removed`, `silver_records_processed`, `last_successful_ts`.

One Silver run:

```bash
aws dynamodb get-item --table-name "$CONTROL_TABLE" --region "$AWS_REGION"   --key '{"pk":{"S":"SILVER_RUN#<SILVER_RUN_ID>"},"sk":{"S":"SUMMARY"}}'
```

All Silver summaries:

```bash
aws dynamodb scan --table-name "$CONTROL_TABLE" --region "$AWS_REGION"   --filter-expression "begins_with(pk, :p)"   --expression-attribute-values '{":p":{"S":"SILVER_RUN#"}}'
```

## Athena / Iceberg

```sql
SELECT COUNT(*) FROM iata_sales_iac_bronze.sales_raw;
SELECT COUNT(*) FROM iata_sales_iac_silver.sales;
```

```sql
SELECT * FROM iata_sales_iac_bronze.sales_raw LIMIT 20;
SELECT * FROM iata_sales_iac_silver.sales LIMIT 20;
```

Bronze duplicates:

```sql
SELECT order_id, COUNT(*) AS cnt
FROM iata_sales_iac_bronze.sales_raw
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 50;
```

Silver dedup check:

```sql
SELECT order_id, COUNT(*) AS cnt
FROM iata_sales_iac_silver.sales
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
```

Iceberg snapshots:

```sql
SELECT *
FROM iata_sales_iac_bronze."sales_raw$snapshots"
ORDER BY committed_at DESC;
```

```sql
SELECT *
FROM iata_sales_iac_silver."sales$snapshots"
ORDER BY committed_at DESC;
```

Example business query:

```sql
SELECT region,
       COUNT(*) AS orders,
       SUM(total_revenue) AS revenue,
       SUM(total_profit) AS profit
FROM iata_sales_iac_silver.sales
GROUP BY region
ORDER BY revenue DESC;
```

## Glue

```bash
aws glue get-jobs --region "$AWS_REGION" --query "Jobs[*].Name"

aws glue get-job-runs --job-name iata-sales-iac-landing-to-msk   --region "$AWS_REGION" --max-results 10   --query "JobRuns[*].[Id,JobRunState,StartedOn,ExecutionTime,ErrorMessage]"   --output table

aws glue get-job-runs --job-name iata-sales-iac-bronze-to-silver   --region "$AWS_REGION" --max-results 10   --query "JobRuns[*].[Id,JobRunState,StartedOn,ExecutionTime,ErrorMessage]"   --output table
```

Glue connections / VPC evidence:

```bash
aws glue get-connections --region "$AWS_REGION"   --query "ConnectionList[*].[Name,ConnectionType,PhysicalConnectionRequirements.SubnetId,PhysicalConnectionRequirements.SecurityGroupIdList]"   --output table
```

## Amazon MSK

```bash
aws kafka list-clusters-v2 --region "$AWS_REGION"   --query "ClusterInfoList[*].[ClusterName,State,ClusterArn]" --output table

aws kafka get-bootstrap-brokers --cluster-arn <MSK_CLUSTER_ARN>   --region "$AWS_REGION"
```

The SASL/IAM TLS bootstrap endpoint uses port 9098.

## MSK Connect

```bash
aws kafkaconnect list-connectors --region "$AWS_REGION"   --query "connectors[*].[connectorName,connectorState,connectorArn]" --output table

aws kafkaconnect describe-connector --connector-arn <CONNECTOR_ARN>   --region "$AWS_REGION"

aws kafkaconnect list-custom-plugins --region "$AWS_REGION"   --query "customPlugins[*].[name,customPluginArn,latestRevision.revision,latestRevision.contentType]"   --output table
```

## S3

```bash
aws s3 ls "s3://iata-sales-iac-landing-${AWS_ACCOUNT_ID}/" --recursive
aws s3 ls "s3://iata-sales-iac-lakehouse-${AWS_ACCOUNT_ID}/" --recursive
aws s3 ls "s3://iata-sales-iac-artifacts-${AWS_ACCOUNT_ID}/glue/" --recursive
aws s3 ls "s3://iata-sales-iac-plugins-${AWS_ACCOUNT_ID}/" --recursive
```

## Lambda

```bash
aws lambda get-function-configuration   --function-name iata-sales-iac-acquisition   --region "$AWS_REGION"

aws lambda get-function-configuration   --function-name iata-sales-iac-acquisition   --region "$AWS_REGION"   --query "VpcConfig"
```

The current Acquisition Lambda is intentionally not attached to the application VPC.

## VPC / Networking

```bash
aws ec2 describe-vpcs --region "$AWS_REGION"   --filters "Name=tag:Name,Values=iata-sales-iac-vpc"   --query "Vpcs[*].[VpcId,CidrBlock,State]" --output table

aws ec2 describe-subnets --region "$AWS_REGION"   --filters "Name=vpc-id,Values=<VPC_ID>"   --query "Subnets[*].[SubnetId,AvailabilityZone,CidrBlock,MapPublicIpOnLaunch,Tags[?Key=='Name'].Value|[0]]"   --output table

aws ec2 describe-route-tables --region "$AWS_REGION"   --filters "Name=vpc-id,Values=<VPC_ID>"

aws ec2 describe-security-groups --region "$AWS_REGION"   --filters "Name=vpc-id,Values=<VPC_ID>"   --query "SecurityGroups[*].[GroupId,GroupName,Description]" --output table

aws ec2 describe-network-interfaces --region "$AWS_REGION"   --filters "Name=vpc-id,Values=<VPC_ID>"   --query "NetworkInterfaces[*].[NetworkInterfaceId,Status,InterfaceType,Description,SubnetId,Groups[*].GroupId]"   --output table

aws ec2 describe-vpc-endpoints --region "$AWS_REGION"   --filters "Name=vpc-id,Values=<VPC_ID>"             "Name=service-name,Values=com.amazonaws.${AWS_REGION}.s3"

aws ec2 describe-nat-gateways --region "$AWS_REGION"   --filter "Name=vpc-id,Values=<VPC_ID>"   --query "NatGateways[*].[NatGatewayId,State,SubnetId,NatGatewayAddresses[0].PublicIp]"   --output table
```

## CloudWatch

```bash
aws logs describe-log-groups --region "$AWS_REGION"   --log-group-name-prefix "/aws/"   --query "logGroups[?contains(logGroupName, 'iata-sales-iac')].logGroupName"

aws logs tail <LOG_GROUP_NAME> --region "$AWS_REGION" --since 30m

aws logs filter-log-events --log-group-name "<LOG_GROUP_NAME>"   --region "$AWS_REGION" --filter-pattern '"ERROR"'

aws cloudwatch list-dashboards --region "$AWS_REGION"   --dashboard-name-prefix iata-sales-iac
```

## SNS

```bash
aws sns list-topics --region "$AWS_REGION"

aws sns list-subscriptions-by-topic   --topic-arn <SNS_TOPIC_ARN>   --region "$AWS_REGION"
```

Email subscriptions must be confirmed by the recipient.

## Fast Failure Checklist

1. Check the Step Functions execution graph/history.
2. Check the failed Lambda or Glue job and CloudWatch logs.
3. Check DynamoDB ingestion metadata or Silver summary.
4. If Bronze is not advancing, check MSK Connect state/logs and Kafka IAM/TLS connectivity.
5. Check S3 landing, manifest, quarantine and Iceberg objects.
6. Check Iceberg snapshots in Athena.
7. Check SNS subscription status if an expected alert was not received.
8. For IaC failures, follow the root CloudFormation event into the failed nested stack/resource.

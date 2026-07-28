# Landing → MSK Glue Configuration

## Job Configuration

- Job name: `iata-sales-iac-landing-to-msk`
- Job type: `Spark`
- Glue version: `5.1`
- Language: `Python 3`
- Worker type: `G.1X`
- Number of workers: `3`
- Execution class: `STANDARD`
- Timeout: `30 minutes`
- Retries: `1`
- IAM role: `iata-sales-iac-glue-landing-to-msk-role`
- VPC connection: `iata-sales-iac-msk-connection`

## Script

- Script location: `s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/scripts/`
- Script name: `iata_sales_landing_to_msk.py`

Full script URI:

```text
s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/scripts/iata_sales_landing_to_msk.py
```

## Extra JARs

The Glue job requires the Spark Kafka connector and AWS MSK IAM authentication libraries.

```text
s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar,
s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar,
s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/aws-msk-iam-auth-2.3.7-all.jar
```

These are supplied to the Glue job through:

```text
--extra-jars
```

## Job Parameters

```text
--MANIFEST_URI=<supplied dynamically by the ingestion Step Function>

--MSK_BOOTSTRAP_SERVERS=<bootstrap servers from the IaC-created MSK cluster>

--MSK_ERROR_TOPIC=iata-sales-iac-errors

--MSK_MAIN_TOPIC=iata-sales-iac-records

--extra-jars=s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar,s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar,s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/aws-msk-iam-auth-2.3.7-all.jar

--user-jars-first=true
```

## Dynamic Parameters

The following parameters must not be hardcoded to a specific ingestion run.

### MANIFEST_URI

Supplied by the ingestion Step Function from the Acquisition Lambda output.

Example runtime value:

```text
s3://iata-sales-iac-landing-<ACCOUNT_ID>/manifests/source=sales/ingestion_date=<date>/run_id=<run-id>/manifest.json
```

### MSK_BOOTSTRAP_SERVERS

Must reference the bootstrap brokers belonging to the IaC-created MSK cluster.

The existing manually-created DEV cluster bootstrap servers must not be hardcoded into the IaC deployment.

## Kafka Topics

Main records topic:

```text
iata-sales-iac-records
```

Error / DLQ topic:

```text
iata-sales-iac-errors
```

Iceberg control topic is managed by the MSK Connect configuration:

```text
iata-sales-iac-control-iceberg
```

## Dependencies

| Dependency | Version | Purpose |
|---|---|---|
| `spark-sql-kafka-0-10_2.12` | `3.5.6` | Spark Kafka integration |
| `spark-token-provider-kafka-0-10_2.12` | `3.5.6` | Kafka token provider support |
| `aws-msk-iam-auth` | `2.3.7` | AWS IAM authentication to MSK |

At deployment time the JAR files are uploaded to:

```text
s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/
```

The pinned dependency versions are documented under:

```text
dependencies/glue/kafka/
```

## Target AWS Resources

| Resource | IaC Resource Name |
|---|---|
| Glue Job | `iata-sales-iac-landing-to-msk` |
| IAM Role | `iata-sales-iac-glue-landing-to-msk-role` |
| Glue Connection | `iata-sales-iac-msk-connection` |
| MSK Cluster | `iata-sales-iac-msk` |
| Main Kafka Topic | `iata-sales-iac-records` |
| Error / DLQ Topic | `iata-sales-iac-errors` |
| Landing Bucket | `iata-sales-iac-landing-<ACCOUNT_ID>` |
| Artifacts Bucket | `iata-sales-iac-artifacts-<ACCOUNT_ID>` |
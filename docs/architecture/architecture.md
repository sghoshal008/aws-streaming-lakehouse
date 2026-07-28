# Architecture

## Overview

The solution implements two asynchronous workflows:

```text
External ZIP
 -> Acquisition Lambda
 -> S3 archive/ + landing/ + manifests/
 -> Glue Landing-to-MSK
 -> Amazon MSK
 -> Amazon MSK Connect / Apache Iceberg Sink
 -> Bronze Iceberg
 -> Bronze-to-Silver Step Function / Glue
 -> Silver Iceberg
 -> Athena
```

EventBridge is the intended scheduler for both workflows but is disabled for the demo so executions can be started and observed manually.

## Architecture Diagram

Place the final image here as `iata-sales-streaming-lakehouse.png` and embed it with:

```markdown
![IATA Sales Streaming Lakehouse](iata-sales-streaming-lakehouse.png)
```

## Ingestion

1. The Ingestion Step Function invokes the Acquisition Lambda.
2. Lambda downloads the external ZIP, calculates the source fingerprint, archives the ZIP, extracts CSVs to `landing/`, and writes `manifest.json` only after all files land.
3. DynamoDB stores ingestion execution state and metadata.
4. The Step Function starts the Landing-to-MSK Glue Spark job.
5. Glue reads the manifest and CSVs, performs technical validation, adds lineage metadata, and publishes Kafka records keyed by `order_id`.
6. Glue connects to private MSK using IAM authentication over TLS on port 9098.
7. The Step Function marks the run complete only after the Glue producer succeeds.

## Streaming and Bronze

Kafka topics:

| Topic | Purpose |
|---|---|
| `iata-sales-iac-records` | Main sales stream |
| `iata-sales-iac-errors` | Kafka Connect / sink error records |
| `iata-sales-iac-control-iceberg` | Iceberg connector control topic |

Amazon MSK Connect runs the Apache Iceberg Sink Connector. The custom plugin/JAR bundle uses the Glue Data Catalog and S3-backed Iceberg storage (`S3FileIO`) and tracks Kafka consumer offsets.

Bronze:
- Database: `iata_sales_iac_bronze`
- Table: `sales_raw`

## Bronze to Silver

Silver:
- Database: `iata_sales_iac_silver`
- Table: `sales`

The separate Bronze-to-Silver workflow:

1. Reads the last successful Bronze Iceberg snapshot watermark from DynamoDB.
2. Determines the current Bronze snapshot and processes the required incremental range.
3. Enforces target types and data-quality rules.
4. Writes invalid records to S3 quarantine.
5. Deduplicates valid records by `order_id`.
6. MERGEs valid records into Silver.
7. Writes a per-run reconciliation summary.
8. Advances the watermark only after successful processing.

The Step Function retrieves the run summary after Glue completes. Quarantined records generate a DQ warning; technical/reconciliation failures fail the workflow and publish SNS alerts.

## DynamoDB Control Model

Control table: `iata-sales-iac-ingestion-control`

| PK | SK | Purpose |
|---|---|---|
| `RUN#<run_id>` | `METADATA` | Ingestion execution state and metadata |
| `PIPELINE#BRONZE_TO_SILVER` | `WATERMARK` | Last successfully processed Bronze snapshot |
| `SILVER_RUN#<run_id>` | `SUMMARY` | Bronze-to-Silver reconciliation metrics |

The watermark includes `last_snapshot_id`, `bronze_records_read`, `valid_records`, `quarantine_records`, `duplicates_removed`, `silver_records_processed`, and `last_successful_ts`.

## Networking

The Acquisition Lambda is intentionally not VPC-attached: it needs public outbound access to the external source and does not need direct MSK connectivity.

The Landing-to-MSK Glue workload obtains VPC connectivity through its Glue Kafka connection, which specifies a private subnet and security group. It reaches private MSK brokers using IAM/TLS on port 9098.

The VPC uses private subnets across Availability Zones, public networking for the NAT Gateway, route tables, security groups, and an S3 Gateway VPC Endpoint for private S3 routing where applicable.

## Security, Monitoring and Failure Handling

IAM roles provide service-specific permissions. CloudWatch provides logs, metrics, alarms and the operational dashboard. Technical failures are published to the SNS pipeline-alert topic.

MSK Connect tracks Kafka offsets. Bronze provides the streamed audit/replay layer. Silver handles replay duplicates using the business key and Iceberg MERGE semantics. The Bronze-to-Silver watermark advances only after successful processing.

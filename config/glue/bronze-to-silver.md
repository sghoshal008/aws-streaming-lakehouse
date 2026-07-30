# Bronze → Silver Glue Configuration

## Job Configuration

| Setting | Value |
|---|---|
| Job name | `yt-sales-iac-bronze-to-silver` |
| Job type | Spark |
| Glue version | `5.1` |
| Language | Python 3 |
| Worker type | `G.1X` |
| Workers | `3` |
| Execution class | `FLEX` |
| Timeout | `10 minutes` |
| Retries | `1` |
| Max concurrent runs | `1` |
| IAM role | `yt-sales-iac-glue-bronze-to-silver-role` |
| Glue connection | None |

The job is not attached to the Kafka VPC connection because it reads/writes
S3-backed Iceberg tables through AWS services rather than connecting to MSK.

## Script

```text
s3://yt-sales-iac-artifacts-<ACCOUNT_ID>/
glue/scripts/yt_sales_bronze_to_silver.py
```

Source:

```text
app/glue/bronze-to-silver/yt_sales_bronze_to_silver.py
```

## Job arguments

Static CloudFormation arguments:

```text
--BRONZE_TABLE=glue_catalog.yt_sales_iac_bronze.sales_raw
--CONTROL_TABLE=yt-sales-iac-ingestion-control
--QUARANTINE_PATH=s3://yt-sales-iac-lakehouse-<ACCOUNT_ID>/quarantine/sales/
--SILVER_TABLE=glue_catalog.yt_sales_iac_silver.sales
--datalake-formats=iceberg
```

Dynamic argument supplied by Step Functions:

```text
--SILVER_RUN_ID=<States.UUID() value>
```

The CloudFormation template still carries `--LOAD_MODE=FULL` as a harmless
legacy/default argument, but the current Spark code does **not** use it.
The actual load type is calculated automatically from the saved Iceberg
snapshot watermark:

```text
no watermark                -> INITIAL_FULL
watermark snapshot exists   -> INCREMENTAL
watermark snapshot expired  -> FULL_RECOVERY
same current snapshot       -> NO_DATA
```

## Iceberg Spark configuration

CloudFormation supplies:

```text
spark.sql.extensions=
  org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

spark.sql.catalog.glue_catalog=
  org.apache.iceberg.spark.SparkCatalog

spark.sql.catalog.glue_catalog.warehouse=
  s3://yt-sales-iac-lakehouse-<ACCOUNT_ID>/

spark.sql.catalog.glue_catalog.catalog-impl=
  org.apache.iceberg.aws.glue.GlueCatalog

spark.sql.catalog.glue_catalog.io-impl=
  org.apache.iceberg.aws.s3.S3FileIO
```

## Incremental / recovery logic

The DynamoDB watermark item is:

```text
pk = PIPELINE#BRONZE_TO_SILVER
sk = WATERMARK
```

The job reads the current Bronze Iceberg snapshot and compares it with
`last_snapshot_id`.

For an incremental execution the Spark read uses:

```text
start-snapshot-id = previous successful snapshot (exclusive)
end-snapshot-id   = current snapshot (inclusive)
```

The watermark advances **only after** successful Silver processing.

## Data-quality validation

Bronze business values arrive mainly as strings. The job creates temporary
typed columns and validates the cast result before allowing a row into Silver.

Examples:

```text
NULL / blank order_id       -> ORDER_ID_MISSING
non-numeric order_id        -> ORDER_ID_INVALID
invalid order_date          -> ORDER_DATE_INVALID
invalid ship_date           -> SHIP_DATE_INVALID
invalid units_sold          -> UNITS_SOLD_INVALID
invalid unit_price          -> UNIT_PRICE_INVALID
invalid unit_cost           -> UNIT_COST_INVALID
invalid total_revenue       -> TOTAL_REVENUE_INVALID
invalid total_cost          -> TOTAL_COST_INVALID
invalid total_profit        -> TOTAL_PROFIT_INVALID
```

Rows with one or more DQ errors are written to:

```text
s3://yt-sales-iac-lakehouse-<ACCOUNT_ID>/
quarantine/sales/snapshot_id=<BRONZE_SNAPSHOT_ID>/
```

The quarantine output also records:

```text
silver_run_id
start_snapshot_id
end_snapshot_id
load_type
dq_error_reason
quarantine_ts
```

## Deduplication

Valid rows are deduplicated by:

```text
order_id
```

using `row_number()` and preferring the latest record by:

```text
etl_create_ts DESC
order_date DESC
run_id DESC
glue_run_id DESC
file_id DESC
```

## Silver merge

The deduplicated input is registered as:

```text
silver_source
```

and merged into:

```text
glue_catalog.yt_sales_iac_silver.sales
```

Business key:

```text
order_id
```

Matched rows are updated only when the incoming lineage/timestamp is at least
as recent as the existing record. New order IDs are inserted.

This makes Silver replay-safe even when Kafka/Bronze contains duplicates.

## Run reconciliation

Each Step Functions execution generates:

```text
pk = SILVER_RUN#<SILVER_RUN_ID>
sk = SUMMARY
```

with metrics including:

```text
bronze_records_read
valid_records
quarantine_records
duplicates_removed
silver_records_processed
start_snapshot_id
end_snapshot_id
load_type
status
```

Step Functions reads this exact item after Glue finishes and raises a DQ warning
when quarantine records are present.

## Observability

CloudWatch log groups:

```text
/aws/glue/yt-sales-iac-bronze-to-silver/error
/aws/glue/yt-sales-iac-bronze-to-silver/output
```

Glue metrics and observability metrics are enabled.

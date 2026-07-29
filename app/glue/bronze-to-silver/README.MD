# Bronze-to-Silver Glue Job – Overall Flow

## Purpose

This AWS Glue Spark job reads records from the Bronze Apache Iceberg table, validates and cleans them, sends invalid records to S3 quarantine, removes duplicates, and merges the latest valid records into the Silver Iceberg table.

It also uses DynamoDB to:

* Track the last successfully processed Bronze snapshot.
* Store a summary for each Silver job execution.
* Support incremental processing and recovery.

---

## High-Level Architecture

```text
Bronze Iceberg Table
        │
        ▼
Read Previous Watermark from DynamoDB
        │
        ▼
Get Current Bronze Snapshot
        │
        ▼
Determine Load Type
        │
        ├── Initial Full Load
        ├── Incremental Load
        └── Full Recovery Load
        │
        ▼
Read Bronze Records
        │
        ▼
Clean and Convert Data Types
        │
        ▼
Apply Data Quality Rules
        │
        ├──────────────────────────┐
        ▼                          ▼
Valid Records               Invalid Records
        │                          │
        ▼                          ▼
Deduplicate by order_id      Write to S3 Quarantine
        │
        ▼
MERGE into Silver Iceberg
        │
        ▼
Save Run Summary in DynamoDB
        │
        ▼
Update Watermark in DynamoDB
        │
        ▼
Commit Glue Job
```

---

# Detailed Processing Flow

## 1. Read Glue Job Parameters

The job receives the following runtime parameters:

```text
JOB_NAME
BRONZE_TABLE
SILVER_TABLE
QUARANTINE_PATH
CONTROL_TABLE
SILVER_RUN_ID
```

These identify:

* The Bronze Iceberg table.
* The Silver Iceberg table.
* The S3 quarantine location.
* The DynamoDB control table.
* The unique identifier for the current Silver execution.

---

## 2. Initialise Spark and AWS Glue

The job creates:

```text
SparkContext
      │
      ▼
GlueContext
      │
      ▼
SparkSession
      │
      ▼
Glue Job
```

Spark performs the distributed data processing, while the Glue job object manages the job lifecycle.

---

## 3. Connect to DynamoDB

The job connects to the DynamoDB control table.

DynamoDB stores two types of records:

### Shared Watermark Record

```text
pk = PIPELINE#BRONZE_TO_SILVER
sk = WATERMARK
```

This stores the last successfully processed Bronze Iceberg snapshot.

### Per-Run Summary Record

```text
pk = SILVER_RUN#<silver-run-id>
sk = SUMMARY
```

This stores the result of one exact Silver execution.

---

## 4. Read the Last Processed Snapshot

The job reads:

```text
last_snapshot_id
```

from DynamoDB.

This tells the job where the previous successful execution stopped.

Example:

```text
Last processed snapshot = 875421
```

---

## 5. Get the Current Bronze Snapshot

The job queries the Iceberg metadata table:

```sql
SELECT
    snapshot_id,
    committed_at
FROM
    bronze_table.snapshots
ORDER BY
    committed_at DESC
LIMIT 1
```

This returns the latest available Bronze snapshot.

Example:

```text
Current Bronze snapshot = 875950
```

---

## 6. Handle No-Data Conditions

### Bronze Has No Snapshot

If the Bronze table has no snapshot:

```text
Status = NO_DATA
```

The job saves a run summary and exits successfully.

### Snapshot Has Not Changed

If:

```text
last_snapshot_id == current_snapshot_id
```

there is nothing new to process.

The job saves a `NO_DATA` summary and exits successfully.

---

## 7. Determine the Load Type

The job selects one of three load types.

### Initial Full Load

Used when no previous watermark exists:

```text
last_snapshot_id = None
```

The complete Bronze table is read.

```text
Load type = INITIAL_FULL
```

### Incremental Load

Used when the saved snapshot still exists in Iceberg.

Only records added between the previous and current snapshots are read.

```text
Start snapshot = previous snapshot, exclusive
End snapshot   = current snapshot, inclusive
```

```text
Load type = INCREMENTAL
```

### Full Recovery Load

Used when the saved snapshot no longer exists.

This may happen if old Iceberg snapshots were expired.

The complete Bronze table is read again.

```text
Load type = FULL_RECOVERY
```

---

## 8. Read and Cache Bronze Records

The selected Bronze data is cached:

```python
bronze_df.cache()
```

The cache prevents Spark from reading and transforming the same Bronze records repeatedly during multiple actions such as:

```text
count()
validation
quarantine write
deduplication
merge
```

The total number of selected Bronze records is calculated.

---

## 9. Clean String Columns

The following columns are cleaned:

```text
region
country
item_type
sales_channel
order_priority
```

For each column:

```text
Leading and trailing spaces are removed.
Empty strings are converted to NULL.
```

Example:

```text
"  Asia  "  → "Asia"
""          → NULL
"   "       → NULL
```

---

## 10. Convert Data Types

Bronze values are initially stored mostly as strings.

Temporary typed columns are created.

```text
order_id       → long
order_date     → date
ship_date      → date
units_sold     → long
unit_price     → decimal(12,2)
unit_cost      → decimal(12,2)
total_revenue  → decimal(18,2)
total_cost     → decimal(18,2)
total_profit   → decimal(18,2)
```

Temporary column names begin with an underscore.

Example:

```text
order_id  → _order_id
```

If a cast fails, Spark produces `NULL`.

Example:

```text
"ABC123" cast to long → NULL
```

---

## 11. Apply Data Quality Validation

The job creates a `dq_errors` array for every row.

Possible validation errors include:

```text
ORDER_ID_MISSING
ORDER_ID_INVALID
ORDER_DATE_INVALID
SHIP_DATE_INVALID
UNITS_SOLD_INVALID
UNIT_PRICE_INVALID
UNIT_COST_INVALID
TOTAL_REVENUE_INVALID
TOTAL_COST_INVALID
TOTAL_PROFIT_INVALID
```

Example:

```json
{
  "order_id": "ABC",
  "order_date": "invalid-date",
  "dq_errors": [
    "ORDER_ID_INVALID",
    "ORDER_DATE_INVALID"
  ]
}
```

---

## 12. Split Valid and Invalid Records

Records are separated based on the number of data-quality errors.

### Valid Records

```text
size(dq_errors) = 0
```

These continue towards Silver.

### Quarantine Records

```text
size(dq_errors) > 0
```

These are written to the S3 quarantine location.

---

## 13. Write Invalid Records to S3 Quarantine

For each invalid record, the job adds:

```text
dq_error_reason
quarantine_ts
silver_run_id
start_snapshot_id
end_snapshot_id
load_type
```

The `dq_errors` array is converted into a comma-separated string.

Example:

```text
ORDER_ID_INVALID,ORDER_DATE_INVALID
```

The quarantine path is:

```text
<quarantine-path>/
snapshot_id=<current-snapshot-id>/
```

Example:

```text
s3://lakehouse-bucket/quarantine/sales/
snapshot_id=875950/
```

Records are written in Parquet format.

---

## 14. Prepare Typed Silver Records

For valid records:

1. Original string columns are removed.
2. Temporary typed columns are renamed to their final names.

Example:

```text
_order_id → order_id
```

The job also adds:

```text
silver_update_ts
```

This records when the row was prepared for Silver.

---

## 15. Deduplicate the Current Input

Records are deduplicated using:

```text
order_id
```

A Spark window groups rows by `order_id`.

Within each group, rows are ordered by:

```text
etl_create_ts descending
order_date descending
run_id descending
glue_run_id descending
file_id descending
```

The first row is retained:

```text
row_number = 1
```

Example:

```text
order_id = 1001

Record A: etl_create_ts = 10:00
Record B: etl_create_ts = 10:05
```

Record B is retained because it is newer.

The job calculates:

```text
duplicates_removed =
valid_records - deduplicated_records
```

---

## 16. Register the Silver Source View

If valid deduplicated records exist, the DataFrame is registered as:

```text
silver_source
```

This temporary Spark SQL view is used by the Iceberg `MERGE` statement.

---

## 17. Merge Records into the Silver Iceberg Table

The merge matches records using:

```sql
target.order_id = source.order_id
```

### Existing Order

An existing Silver record is updated only when the incoming record is newer.

The comparison primarily uses:

```text
etl_create_ts
```

If timestamps are equal, `order_date` is used as a secondary comparison.

### New Order

If the `order_id` does not exist in Silver, the row is inserted.

Conceptually:

```text
Incoming order_id exists?
        │
        ├── Yes → Update when incoming record is newer
        │
        └── No  → Insert new record
```

This provides upsert behaviour.

---

## 18. Save the Exact Run Summary

After processing succeeds, the job stores a run-specific DynamoDB item.

Example:

```json
{
  "pk": "SILVER_RUN#abc-123",
  "sk": "SUMMARY",
  "status": "COMPLETED",
  "load_type": "INCREMENTAL",
  "start_snapshot_id": 875421,
  "end_snapshot_id": 875950,
  "bronze_records_read": 100000,
  "valid_records": 99000,
  "quarantine_records": 1000,
  "duplicates_removed": 500,
  "silver_records_processed": 98500
}
```

Step Functions can retrieve this exact record using `SILVER_RUN_ID`.

---

## 19. Update the Shared Watermark

After all processing completes successfully, the current Bronze snapshot becomes the new watermark.

```text
last_snapshot_id = current_snapshot_id
```

The watermark is updated only after:

```text
Bronze read succeeded
Validation succeeded
Quarantine write succeeded
Silver MERGE succeeded
Run summary was saved
```

This prevents the pipeline from skipping data after a failed run.

---

## 20. Print Final Processing Summary

The job prints:

```text
Silver Run ID
Load type
Previous snapshot
Current snapshot
Bronze records read
Valid records
Quarantined records
Duplicates removed
Silver records processed
```

These logs are available in CloudWatch.

---

## 21. Commit the Glue Job

Finally:

```python
job.commit()
```

marks the AWS Glue job execution as successfully completed.

---

# Example Execution

Assume:

```text
Previous snapshot = 100
Current snapshot  = 105
```

The job performs an incremental read between snapshots 100 and 105.

Suppose it reads:

```text
Bronze records selected = 10,000
```

After validation:

```text
Valid records      = 9,700
Quarantine records = 300
```

After deduplication:

```text
Duplicates removed = 200
Silver records     = 9,500
```

Final flow:

```text
10,000 Bronze records
        │
        ├── 300 invalid
        │       │
        │       ▼
        │   S3 Quarantine
        │
        ▼
9,700 valid records
        │
        ├── 200 duplicates removed
        │
        ▼
9,500 records
        │
        ▼
MERGE into Silver
        │
        ▼
Save summary and snapshot 105 watermark
```

---

# Key Design Characteristics

## Incremental Processing

Only new Bronze snapshots are processed after the initial load.

## Recovery Support

If the previous Iceberg snapshot expires, the job automatically performs a full recovery load.

## Data Quality Isolation

Invalid records do not block valid records from reaching Silver.

## Deduplication

Only one latest record per `order_id` is processed in each execution.

## Idempotent Silver Processing

The Iceberg `MERGE` updates existing records and inserts new records based on `order_id`.

## Safe Watermark Update

The watermark advances only after successful processing.

## Traceability

Every execution can be traced using:

```text
silver_run_id
snapshot IDs
run_id
file_id
source_file_uri
Glue run ID
```

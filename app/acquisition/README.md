# Acquisition Lambda Flow

## Purpose

The Acquisition Lambda retrieves a source ZIP file from an external HTTP URL, archives the original ZIP in Amazon S3, extracts and lands the CSV files, writes ingestion metadata to DynamoDB, and finally creates a READY manifest for downstream processing.

The manifest is written only after all CSV files have been successfully uploaded, ensuring downstream components never process an incomplete ingestion.

---

## High-Level Flow

```text
Manual Trigger / Step Functions
            |
            v
Create unique ingestion run ID
            |
            v
Create DynamoDB control record
            |
            v
Download source ZIP from HTTP URL
            |
            v
Validate that the downloaded file is a ZIP
            |
            v
Calculate SHA-256 of source ZIP
            |
            v
Archive original ZIP in S3
            |
            v
Extract ZIP into Lambda /tmp
            |
            v
Find all CSV files recursively
            |
            v
Calculate SHA-256 for each CSV
            |
            v
Upload CSV files to Landing S3
            |
            v
Write READY manifest to S3 last
            |
            v
Update DynamoDB run status
            |
            v
Return result to Step Functions
```

---

## 1. Lambda Configuration

The Lambda reads the following environment variables:

| Environment variable | Description                                                 |
| -------------------- | ----------------------------------------------------------- |
| `CONTROL_TABLE`      | DynamoDB table used for ingestion control records           |
| `LANDING_BUCKET`     | S3 bucket used for archived ZIPs, landed CSVs and manifests |
| `SOURCE_URL`         | HTTP URL of the source ZIP file                             |
| `SOURCE_NAME`        | Logical source name; defaults to `sales`                    |
| `DATASET_NAME`       | Logical dataset name; defaults to `sales_records`           |

Example:

```text
SOURCE_URL=https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip
SOURCE_NAME=sales
DATASET_NAME=sales_records
```

---

## 2. Generate a Run ID

Every Lambda execution generates a unique `run_id`.

Example:

```text
sales-20260729T061500Z-a1b2c3d4
```

The run ID contains:

```text
<source-name>-<UTC-timestamp>-<random-id>
```

This identifies one complete acquisition attempt and keeps files from separate executions isolated.

---

## 3. Create the DynamoDB Control Record

At the start of processing, the Lambda creates a DynamoDB item:

```text
PK = RUN#<run_id>
SK = METADATA
```

Example:

```json
{
  "pk": "RUN#sales-20260729T061500Z-a1b2c3d4",
  "sk": "METADATA",
  "run_id": "sales-20260729T061500Z-a1b2c3d4",
  "source_name": "sales",
  "dataset_name": "sales_records",
  "source_file_name": "2m-Sales-Records.zip",
  "status": "IN_PROGRESS",
  "stage": "STARTED"
}
```

The same DynamoDB item is updated throughout the ingestion.

Typical stages are:

```text
STARTED
DOWNLOADING
DOWNLOADED
ARCHIVED
LANDED
MANIFEST_READY
FAILED
```

---

## 4. Download the Source ZIP

The Lambda downloads the configured source file from `SOURCE_URL`.

The file is written to Lambda temporary storage:

```text
/tmp/<run_id>-<source-file-name>
```

Example:

```text
/tmp/sales-20260729T061500Z-a1b2c3d4-2m-Sales-Records.zip
```

The download is performed in 8 MB chunks so that the complete source file is not loaded into memory.

---

## 5. Validate the Downloaded File

After downloading, the Lambda verifies that the file is a valid ZIP:

```python
zipfile.is_zipfile(local_zip_path)
```

If the source URL returns HTML, an error page or any non-ZIP content, the Lambda fails with:

```text
Downloaded source is not a valid ZIP file
```

---

## 6. Calculate the Source File SHA-256

The Lambda calculates a SHA-256 hash of the downloaded ZIP.

Example:

```text
a4f07c9c5d86...
```

The hash is stored as:

```text
source_file_id
```

This identifies the source by its contents rather than by its filename.

Therefore:

```text
Same contents + different filename
```

still produce the same `source_file_id`.

The file is read in 8 MB chunks, avoiding the need to load the complete ZIP into memory.

---

## 7. Optional Source-File Deduplication

The code contains optional DynamoDB-based deduplication.

When enabled, it attempts to create:

```text
PK = FILE#<source_file_id>
SK = STATE
```

using a conditional write:

```python
ConditionExpression="attribute_not_exists(pk)"
```

This means only the first execution processing that exact ZIP content can claim it.

### Current demo behaviour

Deduplication is commented out.

Therefore:

```text
Every manual execution processes the ZIP again.
```

### Production behaviour when enabled

```text
Same ZIP SHA-256
        |
        v
Existing DynamoDB claim found
        |
        v
Skip duplicate processing
```

---

## 8. Archive the Original ZIP

The original downloaded ZIP is uploaded to the S3 archive location.

S3 key structure:

```text
archive/
  source=<source-name>/
  ingestion_date=<YYYY-MM-DD>/
  run_id=<run-id>/
  <source-file-name>
```

Example:

```text
s3://<landing-bucket>/
archive/source=sales/
ingestion_date=2026-07-29/
run_id=sales-20260729T061500Z-a1b2c3d4/
2m-Sales-Records.zip
```

Keeping the original ZIP provides:

* source traceability,
* auditability,
* reprocessing capability,
* comparison with landed files.

---

## 9. Extract the ZIP

The Lambda creates a temporary extraction directory:

```text
/tmp/<run_id>
```

It then extracts all files from the ZIP into that directory.

Example:

```text
/tmp/sales-20260729T061500Z-a1b2c3d4/
└── Sales Records.csv
```

Lambda temporary storage must be large enough to hold both:

```text
Downloaded ZIP
+
Extracted contents
```

---

## 10. Find CSV Files

The Lambda recursively walks through the extraction directory and selects files whose names end with `.csv`.

The search is case-insensitive, so all of these are accepted:

```text
sales.csv
SALES.CSV
Sales.Csv
```

Non-CSV files are ignored.

If no CSV files are found, the Lambda fails with:

```text
No CSV file was found inside the ZIP file
```

---

## 11. Land CSV Files in S3

For every extracted CSV, the Lambda:

1. Determines its path relative to the extraction directory.
2. Gets the filename.
3. Calculates the CSV's SHA-256.
4. Uploads the CSV to the Landing S3 location.
5. Adds its metadata to the `uploaded_files` list.

Landing key structure:

```text
landing/
  source=<source-name>/
  ingestion_date=<YYYY-MM-DD>/
  run_id=<run-id>/
  <relative-file-path>
```

Example:

```text
s3://<landing-bucket>/
landing/source=sales/
ingestion_date=2026-07-29/
run_id=sales-20260729T061500Z-a1b2c3d4/
Sales Records.csv
```

For each uploaded CSV, the Lambda records:

```json
{
  "file_id": "<CSV SHA-256>",
  "file_name": "Sales Records.csv",
  "s3_uri": "s3://<bucket>/landing/.../Sales Records.csv"
}
```

The `file_id` identifies the CSV by its contents.

---

## 12. Build the Manifest

After every CSV has been uploaded successfully, the Lambda builds a manifest.

Example:

```json
{
  "manifest_version": "1.0",
  "status": "READY",
  "run_id": "sales-20260729T061500Z-a1b2c3d4",
  "source_name": "sales",
  "dataset_name": "sales_records",
  "source": {
    "file_name": "2m-Sales-Records.zip",
    "source_file_id": "a4f07c9c5d86...",
    "archive_s3_uri": "s3://<bucket>/archive/.../2m-Sales-Records.zip"
  },
  "files": [
    {
      "file_id": "b8e19f32...",
      "file_name": "Sales Records.csv",
      "s3_uri": "s3://<bucket>/landing/.../Sales Records.csv"
    }
  ],
  "file_count": 1,
  "created_at": "2026-07-29T06:15:45+00:00"
}
```

The manifest describes:

* the ingestion run,
* the original source ZIP,
* the archived ZIP location,
* every landed CSV,
* the SHA-256 of each file,
* the number of landed files.

---

## 13. Write the Manifest Last

The manifest is uploaded only after every CSV upload succeeds.

Manifest key structure:

```text
manifests/
  source=<source-name>/
  ingestion_date=<YYYY-MM-DD>/
  run_id=<run-id>/
  manifest.json
```

Example:

```text
s3://<landing-bucket>/
manifests/source=sales/
ingestion_date=2026-07-29/
run_id=sales-20260729T061500Z-a1b2c3d4/
manifest.json
```

The object is uploaded with:

```text
Content-Type: application/json
```

Writing the manifest last makes it the completion signal:

```text
Manifest does not exist
→ ingestion may still be incomplete

Manifest exists with status READY
→ all referenced CSV files have been landed
```

This prevents downstream processing from starting against partially uploaded data.

---

## 14. Update the Final Run Stage

After writing the manifest, the DynamoDB control record is updated to:

```text
stage = MANIFEST_READY
```

It also stores:

```text
manifest_s3_uri
```

This allows the ingestion status and manifest location to be checked using the `run_id`.

---

## 15. Return Data to Step Functions

The Lambda returns:

```json
{
  "status": "SUCCESS",
  "run_id": "<run-id>",
  "source_file_id": "<source ZIP SHA-256>",
  "file_count": 1,
  "archive_uri": "s3://<bucket>/archive/...",
  "manifest_bucket": "<landing-bucket>",
  "manifest_key": "manifests/source=sales/...",
  "manifest_uri": "s3://<bucket>/manifests/...",
  "files": [
    {
      "file_id": "<CSV SHA-256>",
      "file_name": "Sales Records.csv",
      "s3_uri": "s3://<bucket>/landing/..."
    }
  ]
}
```

These fields form the contract between the Acquisition Lambda and Step Functions.

They should not be removed or renamed unless the corresponding Step Functions definition is also updated.

---

## 16. Failure Handling

All main processing runs inside a `try` block.

If any step fails, the Lambda:

1. Captures the error message.
2. Updates the DynamoDB run record.
3. Sets:

```text
status = FAILED
stage = FAILED
```

4. Stores the error message and end time.
5. Raises the exception again.

Raising the exception ensures Step Functions sees the Lambda invocation as failed and can apply its configured retry or failure-handling logic.

Example failure record:

```json
{
  "pk": "RUN#sales-20260729T061500Z-a1b2c3d4",
  "sk": "METADATA",
  "status": "FAILED",
  "stage": "FAILED",
  "error_message": "Downloaded source is not a valid ZIP file"
}
```

---

## Resulting S3 Layout

A successful ingestion produces the following structure:

```text
s3://<landing-bucket>/

├── archive/
│   └── source=sales/
│       └── ingestion_date=2026-07-29/
│           └── run_id=<run-id>/
│               └── 2m-Sales-Records.zip
│
├── landing/
│   └── source=sales/
│       └── ingestion_date=2026-07-29/
│           └── run_id=<run-id>/
│               └── Sales Records.csv
│
└── manifests/
    └── source=sales/
        └── ingestion_date=2026-07-29/
            └── run_id=<run-id>/
                └── manifest.json
```

---

## Complete Processing Summary

```text
1. Generate a unique run ID.
2. Create an IN_PROGRESS DynamoDB control record.
3. Download the source ZIP in chunks.
4. Verify that the downloaded file is a valid ZIP.
5. Calculate the source ZIP SHA-256.
6. Optionally reject an already-processed source file.
7. Archive the original ZIP in S3.
8. Extract the ZIP into Lambda temporary storage.
9. Recursively find CSV files.
10. Calculate the SHA-256 of each CSV.
11. Upload every CSV to Landing S3.
12. Build a manifest containing source and file metadata.
13. Write the READY manifest last.
14. Update the DynamoDB stage to MANIFEST_READY.
15. Return the ingestion result to Step Functions.
16. On failure, mark the DynamoDB run as FAILED and re-raise the error.
```

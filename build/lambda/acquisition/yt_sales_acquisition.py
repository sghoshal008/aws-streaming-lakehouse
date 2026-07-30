# Recommended AWS resource name:
# yt-sales-iac-acquisition

import boto3
import hashlib
import json
import os
import urllib.request
import uuid
import zipfile

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


# ============================================================
# AWS / CONFIG
# ============================================================

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

table = dynamodb.Table(os.environ["CONTROL_TABLE"])

LANDING_BUCKET = os.environ["LANDING_BUCKET"]
SOURCE_URL = os.environ["SOURCE_URL"]
SOURCE_NAME = os.environ.get("SOURCE_NAME", "sales")
DATASET_NAME = os.environ.get("DATASET_NAME", "sales_records")


# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def compact_timestamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ingestion_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def sha256_file(file_path):
    """Calculate SHA-256 without loading the full file into memory."""

    digest = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(
            lambda: file.read(8 * 1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def download_file(url, local_path):
    """Download source file to Lambda /tmp in 8 MB chunks."""

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/149.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "close",
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=300,
    ) as response:

        with open(local_path, "wb") as output:
            while True:
                chunk = response.read(8 * 1024 * 1024)

                if not chunk:
                    break

                output.write(chunk)


def update_run(
    run_id,
    stage,
    status="IN_PROGRESS",
    **fields,
):
    """
    Update the RUN#<run_id> / METADATA control record.

    Step Functions later updates this exact same item.
    """

    updates = [
        "#status = :status",
        "#stage = :stage",
        "updated_at = :updated_at",
    ]

    names = {
        "#status": "status",
        "#stage": "stage",
    }

    values = {
        ":status": status,
        ":stage": stage,
        ":updated_at": utc_now(),
    }

    for index, (name, value) in enumerate(fields.items()):
        name_key = f"#f{index}"
        value_key = f":v{index}"

        updates.append(f"{name_key} = {value_key}")
        names[name_key] = name
        values[value_key] = value

    table.update_item(
        Key={
            "pk": f"RUN#{run_id}",
            "sk": "METADATA",
        },
        UpdateExpression="SET " + ", ".join(updates),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


# ============================================================
# OPTIONAL SOURCE FILE DEDUP
# ============================================================

def claim_source_file(
    source_file_id,
    run_id,
):
    """
    Atomically claim an exact source file using its SHA-256.

    DynamoDB item:

        PK = FILE#<source_file_id>
        SK = STATE

    Conditional write means only the first execution wins.
    """

    try:
        table.put_item(
            Item={
                "pk": f"FILE#{source_file_id}",
                "sk": "STATE",
                "source_file_id": source_file_id,
                "run_id": run_id,
                "status": "IN_PROGRESS",
                "created_at": utc_now(),
                "updated_at": utc_now(),
            },
            ConditionExpression="attribute_not_exists(pk)",
        )

        return True

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return False


def complete_source_file_claim(
    source_file_id,
):
    """Mark the optional source-file claim as completed."""

    table.update_item(
        Key={
            "pk": f"FILE#{source_file_id}",
            "sk": "STATE",
        },
        UpdateExpression=(
            "SET #status = :status, "
            "updated_at = :updated_at"
        ),
        ExpressionAttributeNames={
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": "COMPLETED",
            ":updated_at": utc_now(),
        },
    )


# ============================================================
# LAMBDA
# ============================================================

def lambda_handler(event, context):

    run_id = (
        f"{SOURCE_NAME}-"
        f"{compact_timestamp()}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    start_time = utc_now()
    date_value = ingestion_date()

    source_file_name = (
        Path(urlparse(SOURCE_URL).path).name
        or "source.zip"
    )

    local_zip_path = (
        f"/tmp/{run_id}-{source_file_name}"
    )

    extraction_directory = (
        f"/tmp/{run_id}"
    )

    # --------------------------------------------------------
    # CREATE CONTROL RECORD
    # --------------------------------------------------------

    table.put_item(
        Item={
            "pk": f"RUN#{run_id}",
            "sk": "METADATA",
            "run_id": run_id,
            "source_name": SOURCE_NAME,
            "dataset_name": DATASET_NAME,
            "source_file_name": source_file_name,
            "status": "IN_PROGRESS",
            "stage": "STARTED",
            "start_time": start_time,
            "updated_at": start_time,
        }
    )

    try:

        # ----------------------------------------------------
        # DOWNLOAD
        # ----------------------------------------------------

        print(f"Downloading: {SOURCE_URL}")

        update_run(
            run_id,
            "DOWNLOADING",
        )

        download_file(
            SOURCE_URL,
            local_zip_path,
        )

        if not zipfile.is_zipfile(local_zip_path):
            raise ValueError(
                "Downloaded source is not a valid ZIP file"
            )

        source_file_id = sha256_file(
            local_zip_path
        )

        update_run(
            run_id,
            "DOWNLOADED",
            source_file_id=source_file_id,
            source_file_size_bytes=os.path.getsize(
                local_zip_path
            ),
        )


        # ====================================================
        # OPTIONAL DEDUP - DISABLED FOR DEMO
        # ====================================================
        #
        # Uncomment this block when you want exact source-file
        # idempotency.
        #
        # Current demo behaviour:
        #
        #     every manual execution processes the file again.
        #
        # Production behaviour:
        #
        #     same SHA-256 -> skip duplicate processing.
        #
        #
        # if not claim_source_file(
        #     source_file_id,
        #     run_id,
        # ):
        #
        #     update_run(
        #         run_id,
        #         "SKIPPED_DUPLICATE",
        #         status="COMPLETED",
        #         end_time=utc_now(),
        #     )
        #
        #     return {
        #         "status": "SKIPPED_DUPLICATE",
        #         "run_id": run_id,
        #
        #         # These values are retained because the
        #         # current Step Functions ResultSelector
        #         # expects them.
        #         "source_file_id": source_file_id,
        #         "file_count": 0,
        #         "archive_uri": "",
        #         "manifest_bucket": "",
        #         "manifest_key": "",
        #         "manifest_uri": "",
        #         "files": [],
        #     }
        #
        # ====================================================


        # ----------------------------------------------------
        # ARCHIVE ORIGINAL ZIP
        # ----------------------------------------------------

        archive_key = (
            f"archive/source={SOURCE_NAME}/"
            f"ingestion_date={date_value}/"
            f"run_id={run_id}/"
            f"{source_file_name}"
        )

        s3.upload_file(
            local_zip_path,
            LANDING_BUCKET,
            archive_key,
        )

        archive_uri = (
            f"s3://{LANDING_BUCKET}/{archive_key}"
        )

        update_run(
            run_id,
            "ARCHIVED",
            archive_s3_uri=archive_uri,
        )


        # ----------------------------------------------------
        # EXTRACT ZIP
        # ----------------------------------------------------

        os.makedirs(
            extraction_directory,
            exist_ok=True,
        )

        with zipfile.ZipFile(
            local_zip_path,
            "r",
        ) as zip_file:

            zip_file.extractall(
                extraction_directory
            )


        # ----------------------------------------------------
        # FIND CSV FILES
        # ----------------------------------------------------

        csv_files = [
            os.path.join(root, file_name)

            for root, _, files
            in os.walk(extraction_directory)

            for file_name in files

            if file_name.lower().endswith(".csv")
        ]

        if not csv_files:
            raise ValueError(
                "No CSV file was found inside the ZIP file"
            )


        # ----------------------------------------------------
        # LAND CSV FILES
        # ----------------------------------------------------

        uploaded_files = []

        for csv_path in sorted(csv_files):

            relative_path = (
                Path(csv_path)
                .relative_to(extraction_directory)
                .as_posix()
            )

            file_name = os.path.basename(
                csv_path
            )

            file_id = sha256_file(
                csv_path
            )

            landing_key = (
                f"landing/source={SOURCE_NAME}/"
                f"ingestion_date={date_value}/"
                f"run_id={run_id}/"
                f"{relative_path}"
            )

            s3.upload_file(
                csv_path,
                LANDING_BUCKET,
                landing_key,
            )

            uploaded_files.append({
                "file_id": file_id,
                "file_name": file_name,
                "s3_uri": (
                    f"s3://{LANDING_BUCKET}/"
                    f"{landing_key}"
                ),
            })


        update_run(
            run_id,
            "LANDED",
            file_count=len(uploaded_files),
        )


        # ----------------------------------------------------
        # WRITE MANIFEST LAST
        # ----------------------------------------------------

        manifest = {
            "manifest_version": "1.0",
            "status": "READY",
            "run_id": run_id,
            "source_name": SOURCE_NAME,
            "dataset_name": DATASET_NAME,
            "source": {
                "file_name": source_file_name,
                "source_file_id": source_file_id,
                "archive_s3_uri": archive_uri,
            },
            "files": uploaded_files,
            "file_count": len(uploaded_files),
            "created_at": utc_now(),
        }

        manifest_key = (
            f"manifests/source={SOURCE_NAME}/"
            f"ingestion_date={date_value}/"
            f"run_id={run_id}/"
            f"manifest.json"
        )

        s3.put_object(
            Bucket=LANDING_BUCKET,
            Key=manifest_key,
            Body=json.dumps(
                manifest,
                indent=2,
            ).encode("utf-8"),
            ContentType="application/json",
        )

        manifest_uri = (
            f"s3://{LANDING_BUCKET}/"
            f"{manifest_key}"
        )

        update_run(
            run_id,
            "MANIFEST_READY",
            manifest_s3_uri=manifest_uri,
        )


        # ====================================================
        # OPTIONAL DEDUP COMPLETION - DISABLED FOR DEMO
        # ====================================================
        #
        # Uncomment together with claim_source_file().
        #
        # complete_source_file_claim(
        #     source_file_id
        # )
        #
        # ====================================================


        # ----------------------------------------------------
        # STEP FUNCTIONS CONTRACT
        # ----------------------------------------------------
        #
        # IMPORTANT:
        # Keep these fields unless orchestration.yaml is also
        # changed. The current ResultSelector expects all of them.
        # ----------------------------------------------------

        return {
            "status": "SUCCESS",
            "run_id": run_id,
            "source_file_id": source_file_id,
            "file_count": len(uploaded_files),
            "archive_uri": archive_uri,
            "manifest_bucket": LANDING_BUCKET,
            "manifest_key": manifest_key,
            "manifest_uri": manifest_uri,
            "files": uploaded_files,
        }


    except Exception as error:

        error_message = str(error)[:1000]

        print(
            f"Acquisition failed: "
            f"{error_message}"
        )

        update_run(
            run_id,
            "FAILED",
            status="FAILED",
            error_message=error_message,
            end_time=utc_now(),
        )

        raise
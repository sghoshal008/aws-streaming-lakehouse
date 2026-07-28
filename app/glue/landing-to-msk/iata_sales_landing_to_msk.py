import boto3
import csv
import io
import json
import re
import sys
from urllib.parse import urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ============================================================
# INITIALISE
# ============================================================

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "MANIFEST_URI",
    "MSK_BOOTSTRAP_SERVERS",
    "MSK_MAIN_TOPIC",
    "MSK_ERROR_TOPIC",
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")
sns = boto3.client("sns")

manifest_uri = args["MANIFEST_URI"]
bootstrap_servers = args["MSK_BOOTSTRAP_SERVERS"]
main_topic = args["MSK_MAIN_TOPIC"]
error_topic = args["MSK_ERROR_TOPIC"]


# ============================================================
# SCHEMA
# ============================================================

BUSINESS_COLUMNS = [
    "region",
    "country",
    "item_type",
    "sales_channel",
    "order_priority",
    "order_date",
    "order_id",
    "ship_date",
    "units_sold",
    "unit_price",
    "unit_cost",
    "total_revenue",
    "total_cost",
    "total_profit",
]

BRONZE_COLUMNS = BUSINESS_COLUMNS + [
    "run_id",
    "file_id",
    "file_name",
    "source_file_uri",
    "source_name",
    "dataset_name",
    "manifest_key",
    "glue_run_id",
    "etl_create_ts",
]


# ============================================================
# HELPERS
# ============================================================

def optional_arg(name, default=""):
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


glue_run_id = optional_arg("JOB_RUN_ID", "UNKNOWN")
sns_topic_arn = optional_arg("SNS_TOPIC_ARN", "")


def parse_s3_uri(uri):
    parsed = urlparse(uri)

    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {uri}")

    return parsed.netloc, parsed.path.lstrip("/")


def normalize(name):
    return re.sub(
        r"[^a-z0-9]+",
        "_",
        name.strip().lower(),
    ).strip("_")


def get_header(s3_uri):
    bucket, key = parse_s3_uri(s3_uri)

    response = s3.get_object(
        Bucket=bucket,
        Key=key,
        Range="bytes=0-65535",
    )

    text = (
        response["Body"]
        .read()
        .decode("utf-8-sig", errors="replace")
    )

    lines = text.splitlines()

    if not lines:
        raise ValueError("Empty file")

    header = next(
        csv.reader(
            io.StringIO(lines[0]),
            delimiter=",",
            quotechar='"',
        )
    )

    return [normalize(column) for column in header]


def write_to_kafka(df, topic):
    (
        df.write
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers,
        )
        .option(
            "topic",
            topic,
        )
        .option(
            "kafka.security.protocol",
            "SASL_SSL",
        )
        .option(
            "kafka.sasl.mechanism",
            "AWS_MSK_IAM",
        )
        .option(
            "kafka.sasl.jaas.config",
            "software.amazon.msk.auth.iam."
            "IAMLoginModule required;",
        )
        .option(
            "kafka.sasl.client.callback.handler.class",
            "software.amazon.msk.auth.iam."
            "IAMClientCallbackHandler",
        )
        .option(
            "kafka.acks",
            "all",
        )
        .save()
    )


def send_summary(
    run_id,
    published_count,
    valid_files,
    invalid_files,
):
    if not sns_topic_arn:
        return

    lines = [
        "IATA Sales Ingestion Summary",
        f"Run ID: {run_id}",
        f"Glue Run ID: {glue_run_id}",
        f"Files received: {len(valid_files) + len(invalid_files)}",
        f"Files accepted: {len(valid_files)}",
        f"Files rejected: {len(invalid_files)}",
        f"Records published: {published_count}",
    ]

    for file in invalid_files:
        lines.append(
            f"{file['file_name']}: "
            f"{file['error_message']}"
        )

    try:
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f"IATA ingestion summary - {run_id}"[:100],
            Message="\n".join(lines),
        )
    except Exception as error:
        print(f"WARNING: SNS failed: {error}")


# ============================================================
# READ MANIFEST
# ============================================================

manifest_bucket, manifest_key = parse_s3_uri(
    manifest_uri
)

response = s3.get_object(
    Bucket=manifest_bucket,
    Key=manifest_key,
)

manifest = json.loads(
    response["Body"]
    .read()
    .decode("utf-8")
)

run_id = manifest["run_id"]
source_name = manifest.get(
    "source_name",
    "unknown",
)
dataset_name = manifest.get(
    "dataset_name",
    "unknown",
)
manifest_files = manifest.get(
    "files",
    [],
)

if not manifest_files:
    raise ValueError(
        "Manifest contains no files"
    )


# ============================================================
# VALIDATE FILE HEADERS
# ============================================================

valid_files = []
invalid_files = []

for file in manifest_files:
    try:
        header = get_header(
            file["s3_uri"]
        )

        # Exact match only.
        #
        # Any of these make the file invalid:
        # - missing column
        # - extra column
        # - reordered column
        # - renamed column
        if header != BUSINESS_COLUMNS:
            invalid_files.append({
                **file,
                "error_message": (
                    "Schema validation failed. "
                    f"Expected={BUSINESS_COLUMNS}; "
                    f"Actual={header}"
                ),
            })
        else:
            valid_files.append(
                file
            )

    except Exception as error:
        invalid_files.append({
            **file,
            "error_message": str(error)[:1000],
        })


print(
    f"Files received: {len(manifest_files)}"
)
print(
    f"Files accepted: {len(valid_files)}"
)
print(
    f"Files rejected: {len(invalid_files)}"
)


# ============================================================
# SEND BAD FILES TO ERROR TOPIC
# ============================================================

if invalid_files:
    error_rows = [
        (
            file["file_id"],
            json.dumps({
                "error_scope": "FILE",
                "error_type": "SCHEMA_VALIDATION_FAILURE",
                "error_message": file["error_message"],
                "run_id": run_id,
                "file_id": file["file_id"],
                "file_name": file["file_name"],
                "source_file_uri": file["s3_uri"],
                "manifest_key": manifest_key,
                "glue_run_id": glue_run_id,
            }),
        )
        for file in invalid_files
    ]

    error_df = spark.createDataFrame(
        error_rows,
        ["key", "value"],
    )

    print(
        f"Publishing {len(invalid_files)} "
        f"bad file events to {error_topic}"
    )

    write_to_kafka(
        error_df,
        error_topic,
    )


# ============================================================
# READ ALL VALID FILES TOGETHER
# ============================================================

published_count = 0

if valid_files:
    valid_paths = [
        file["s3_uri"]
        for file in valid_files
    ]

    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(valid_paths)
    )

    # Normalise Spark column names.
    for old_name in df.columns:
        new_name = normalize(
            old_name
        )

        if old_name != new_name:
            df = df.withColumnRenamed(
                old_name,
                new_name,
            )

    # All valid files already have exact schema.
    df = df.select(
        *BUSINESS_COLUMNS
    )


    # ========================================================
    # SOURCE FILE LINEAGE
    # ========================================================

    df = df.withColumn(
        "_source_path",
        F.regexp_replace(
            F.input_file_name(),
            r"^s3a?://",
            "",
        ),
    )

    metadata_rows = []

    for file in valid_files:
        bucket, key = parse_s3_uri(
            file["s3_uri"]
        )

        metadata_rows.append((
            f"{bucket}/{key}",
            file["file_id"],
            file["file_name"],
            file["s3_uri"],
        ))

    metadata_df = spark.createDataFrame(
        metadata_rows,
        [
            "_source_path",
            "file_id",
            "file_name",
            "source_file_uri",
        ],
    )


    # ========================================================
    # ADD INGESTION METADATA
    # ========================================================

    bronze_df = (
        df
        .join(
            F.broadcast(metadata_df),
            "_source_path",
            "left",
        )
        .drop(
            "_source_path"
        )
        .withColumn(
            "run_id",
            F.lit(run_id),
        )
        .withColumn(
            "source_name",
            F.lit(source_name),
        )
        .withColumn(
            "dataset_name",
            F.lit(dataset_name),
        )
        .withColumn(
            "manifest_key",
            F.lit(manifest_key),
        )
        .withColumn(
            "glue_run_id",
            F.lit(glue_run_id),
        )
        .withColumn(
            "etl_create_ts",
            F.current_timestamp(),
        )
        .select(
            *BRONZE_COLUMNS
        )
        .cache()
    )


    # ========================================================
    # COUNT
    # ========================================================

    published_count = (
        bronze_df.count()
    )


    # ========================================================
    # KAFKA PAYLOAD
    # ========================================================

    # No order_id validation.
    #
    # NULL / blank order_id goes to Bronze.
    #
    # No Kafka key is supplied, so records have a NULL key.
    # Spark still writes in parallel.

    kafka_df = bronze_df.select(
        F.to_json(
            F.struct(
                *[
                    F.col(column)
                    for column
                    in BRONZE_COLUMNS
                ]
            )
        ).alias(
            "value"
        )
    )


    # ========================================================
    # PUBLISH
    # ========================================================

    if published_count > 0:
        print(
            f"Publishing "
            f"{published_count} records "
            f"to {main_topic}"
        )

        write_to_kafka(
            kafka_df,
            main_topic,
        )

    bronze_df.unpersist()


# ============================================================
# SUMMARY
# ============================================================

print(
    "======================================"
)
print(
    "Glue producer completed"
)
print(
    "======================================"
)

print(
    f"Run ID: {run_id}"
)
print(
    f"Glue Run ID: {glue_run_id}"
)
print(
    f"Files received: {len(manifest_files)}"
)
print(
    f"Files accepted: {len(valid_files)}"
)
print(
    f"Files rejected: {len(invalid_files)}"
)
print(
    f"Records published: {published_count}"
)


# ============================================================
# SNS
# ============================================================

send_summary(
    run_id,
    published_count,
    valid_files,
    invalid_files,
)


# ============================================================
# COMMIT
# ============================================================

job.commit()
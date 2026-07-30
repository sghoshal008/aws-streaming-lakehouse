import boto3
import sys

from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_TABLE",
        "SILVER_TABLE",
        "QUARANTINE_PATH",
        "CONTROL_TABLE",
        "SILVER_RUN_ID"
    ]
)

BRONZE_TABLE = args["BRONZE_TABLE"]
SILVER_TABLE = args["SILVER_TABLE"]
QUARANTINE_PATH = args["QUARANTINE_PATH"]
CONTROL_TABLE = args["CONTROL_TABLE"]
SILVER_RUN_ID = args["SILVER_RUN_ID"]


# ============================================================
# 2. INITIALIZE GLUE / SPARK
# ============================================================

sc = SparkContext.getOrCreate()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args
)


# ============================================================
# 3. AWS CLIENTS
# ============================================================

dynamodb = boto3.resource("dynamodb")

control_table = dynamodb.Table(
    CONTROL_TABLE
)


# ============================================================
# 4. WATERMARK CONFIGURATION
# ============================================================

WATERMARK_PK = "PIPELINE#BRONZE_TO_SILVER"
WATERMARK_SK = "WATERMARK"


def utc_now():

    return datetime.now(
        timezone.utc
    ).isoformat()


# ============================================================
# 5. READ LAST SUCCESSFUL WATERMARK
# ============================================================

def get_last_snapshot_id():

    response = control_table.get_item(
        Key={
            "pk": WATERMARK_PK,
            "sk": WATERMARK_SK
        }
    )

    item = response.get("Item")

    if not item:
        return None

    snapshot_id = item.get(
        "last_snapshot_id"
    )

    if snapshot_id is None:
        return None

    return int(snapshot_id)


# ============================================================
# 6. SAVE WATERMARK
#
# One shared item.
#
# Purpose:
# Tell the NEXT execution where processing stopped.
# ============================================================

def save_watermark(
    snapshot_id,
    bronze_records_read,
    valid_records,
    quarantine_records,
    duplicates_removed,
    silver_records_processed
):

    control_table.update_item(

        Key={
            "pk": WATERMARK_PK,
            "sk": WATERMARK_SK
        },

        UpdateExpression="""
            SET
                last_snapshot_id = :snapshot_id,
                bronze_records_read = :bronze_records_read,
                valid_records = :valid_records,
                quarantine_records = :quarantine_records,
                duplicates_removed = :duplicates_removed,
                silver_records_processed = :silver_records_processed,
                last_successful_ts = :last_successful_ts
        """,

        ExpressionAttributeValues={

            ":snapshot_id":
                int(snapshot_id),

            ":bronze_records_read":
                int(bronze_records_read),

            ":valid_records":
                int(valid_records),

            ":quarantine_records":
                int(quarantine_records),

            ":duplicates_removed":
                int(duplicates_removed),

            ":silver_records_processed":
                int(silver_records_processed),

            ":last_successful_ts":
                utc_now()
        }
    )


# ============================================================
# 7. SAVE THIS RUN'S SUMMARY
#
# One item per Silver execution.
#
# Purpose:
# Step Functions can retrieve the EXACT run that it started.
# ============================================================

def save_run_summary(
    status,
    load_type,
    start_snapshot_id,
    end_snapshot_id,
    bronze_records_read=0,
    valid_records=0,
    quarantine_records=0,
    duplicates_removed=0,
    silver_records_processed=0
):

    control_table.put_item(

        Item={

            "pk":
                f"SILVER_RUN#{SILVER_RUN_ID}",

            "sk":
                "SUMMARY",

            "silver_run_id":
                SILVER_RUN_ID,

            "status":
                status,

            "load_type":
                load_type,

            "start_snapshot_id":
                int(start_snapshot_id)
                if start_snapshot_id is not None
                else 0,

            "end_snapshot_id":
                int(end_snapshot_id)
                if end_snapshot_id is not None
                else 0,

            "bronze_records_read":
                int(bronze_records_read),

            "valid_records":
                int(valid_records),

            "quarantine_records":
                int(quarantine_records),

            "duplicates_removed":
                int(duplicates_removed),

            "silver_records_processed":
                int(silver_records_processed),

            "completed_at":
                utc_now()
        }
    )


# ============================================================
# 8. GET CURRENT BRONZE SNAPSHOT
# ============================================================

def get_current_snapshot_id():

    snapshots_df = spark.sql(
        f"""
        SELECT
            snapshot_id,
            committed_at
        FROM
            {BRONZE_TABLE}.snapshots
        ORDER BY
            committed_at DESC
        LIMIT 1
        """
    )

    snapshots = snapshots_df.collect()

    if not snapshots:
        return None

    return int(
        snapshots[0]["snapshot_id"]
    )


# ============================================================
# 9. CHECK WHETHER SAVED SNAPSHOT STILL EXISTS
# ============================================================

def snapshot_exists(
    snapshot_id
):

    snapshots_df = spark.sql(
        f"""
        SELECT
            snapshot_id
        FROM
            {BRONZE_TABLE}.snapshots
        WHERE
            snapshot_id = {snapshot_id}
        LIMIT 1
        """
    )

    return snapshots_df.count() > 0


# ============================================================
# MAIN PROCESSING FLOW
# ============================================================

def main():

    # ========================================================
    # 10. FIND SNAPSHOT RANGE
    # ========================================================

    last_snapshot_id = (
        get_last_snapshot_id()
    )

    current_snapshot_id = (
        get_current_snapshot_id()
    )

    print(
        "========================================"
    )

    print(
        f"Silver Run ID          : "
        f"{SILVER_RUN_ID}"
    )

    print(
        f"Last processed snapshot: "
        f"{last_snapshot_id}"
    )

    print(
        f"Current Bronze snapshot: "
        f"{current_snapshot_id}"
    )

    print(
        "========================================"
    )


    # ========================================================
    # 11. BRONZE HAS NO SNAPSHOT
    # ========================================================

    if current_snapshot_id is None:

        print(
            "Bronze table has no snapshots."
        )

        save_run_summary(
            status="NO_DATA",
            load_type="NO_DATA",
            start_snapshot_id=last_snapshot_id,
            end_snapshot_id=None
        )

        job.commit()

        return


    # ========================================================
    # 12. NOTHING NEW
    # ========================================================

    if (
        last_snapshot_id
        ==
        current_snapshot_id
    ):

        print(
            "Bronze snapshot has not changed."
        )

        print(
            "Nothing to process."
        )

        save_run_summary(
            status="NO_DATA",
            load_type="NO_DATA",
            start_snapshot_id=last_snapshot_id,
            end_snapshot_id=current_snapshot_id
        )

        job.commit()

        return


    # ========================================================
    # 13. DETERMINE LOAD TYPE
    # ========================================================

    if last_snapshot_id is None:

        load_type = "INITIAL_FULL"

        print(
            "Running INITIAL FULL LOAD."
        )

        bronze_df = (
            spark.read
            .format("iceberg")
            .load(BRONZE_TABLE)
        )


    elif snapshot_exists(
        last_snapshot_id
    ):

        load_type = "INCREMENTAL"

        print(
            "Running INCREMENTAL LOAD."
        )

        print(
            f"Start snapshot (exclusive): "
            f"{last_snapshot_id}"
        )

        print(
            f"End snapshot (inclusive): "
            f"{current_snapshot_id}"
        )

        bronze_df = (
            spark.read
            .format("iceberg")

            .option(
                "start-snapshot-id",
                str(last_snapshot_id)
            )

            .option(
                "end-snapshot-id",
                str(current_snapshot_id)
            )

            .load(
                BRONZE_TABLE
            )
        )


    else:

        load_type = "FULL_RECOVERY"

        print(
            f"Saved snapshot "
            f"{last_snapshot_id} "
            f"no longer exists."
        )

        print(
            "Running FULL RECOVERY LOAD."
        )

        bronze_df = (
            spark.read
            .format("iceberg")
            .load(BRONZE_TABLE)
        )


    # ========================================================
    # 14. CACHE INPUT
    # ========================================================

    bronze_df = (
        bronze_df.cache()
    )

    bronze_count = (
        bronze_df.count()
    )

    print(
        f"Bronze records selected: "
        f"{bronze_count}"
    )


    # ========================================================
    # 15. SNAPSHOT EXISTS BUT NO ROWS
    # ========================================================

    if bronze_count == 0:

        print(
            "No rows found in snapshot range."
        )

        # Advance checkpoint because this snapshot
        # has successfully been inspected.
        save_watermark(
            snapshot_id=current_snapshot_id,
            bronze_records_read=0,
            valid_records=0,
            quarantine_records=0,
            duplicates_removed=0,
            silver_records_processed=0
        )

        save_run_summary(
            status="NO_DATA",
            load_type=load_type,
            start_snapshot_id=last_snapshot_id,
            end_snapshot_id=current_snapshot_id
        )

        job.commit()

        return


    # ========================================================
    # 16. BASIC STRING CLEANING
    # ========================================================

    string_columns = [
        "region",
        "country",
        "item_type",
        "sales_channel",
        "order_priority"
    ]

    clean_df = bronze_df

    for column_name in string_columns:

        clean_df = (
            clean_df

            .withColumn(

                column_name,

                F.when(

                    F.trim(
                        F.col(
                            column_name
                        )
                    ) == "",

                    F.lit(None)
                )

                .otherwise(

                    F.trim(
                        F.col(
                            column_name
                        )
                    )
                )
            )
        )


    # ========================================================
    # 17. TYPE CONVERSION
    # ========================================================

    typed_df = (

        clean_df

        .withColumn(
            "_order_id",
            F.col(
                "order_id"
            ).cast(
                "long"
            )
        )

        .withColumn(
            "_order_date",
            F.to_date(
                F.col(
                    "order_date"
                ),
                "M/d/yyyy"
            )
        )

        .withColumn(
            "_ship_date",
            F.to_date(
                F.col(
                    "ship_date"
                ),
                "M/d/yyyy"
            )
        )

        .withColumn(
            "_units_sold",
            F.col(
                "units_sold"
            ).cast(
                "long"
            )
        )

        .withColumn(
            "_unit_price",
            F.col(
                "unit_price"
            ).cast(
                "decimal(12,2)"
            )
        )

        .withColumn(
            "_unit_cost",
            F.col(
                "unit_cost"
            ).cast(
                "decimal(12,2)"
            )
        )

        .withColumn(
            "_total_revenue",
            F.col(
                "total_revenue"
            ).cast(
                "decimal(18,2)"
            )
        )

        .withColumn(
            "_total_cost",
            F.col(
                "total_cost"
            ).cast(
                "decimal(18,2)"
            )
        )

        .withColumn(
            "_total_profit",
            F.col(
                "total_profit"
            ).cast(
                "decimal(18,2)"
            )
        )
    )


    # ========================================================
    # 18. DATA QUALITY VALIDATION
    # ========================================================

    validated_df = (

        typed_df

        .withColumn(

            "dq_errors",

            F.array_compact(

                F.array(

                    F.when(
                        F.col(
                            "order_id"
                        ).isNull()
                        |
                        (
                            F.trim(
                                F.col(
                                    "order_id"
                                )
                            ) == ""
                        ),

                        F.lit(
                            "ORDER_ID_MISSING"
                        )
                    ),

                    F.when(
                        F.col(
                            "order_id"
                        ).isNotNull()
                        &
                        (
                            F.trim(
                                F.col(
                                    "order_id"
                                )
                            ) != ""
                        )
                        &
                        F.col(
                            "_order_id"
                        ).isNull(),

                        F.lit(
                            "ORDER_ID_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_order_date"
                        ).isNull(),

                        F.lit(
                            "ORDER_DATE_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_ship_date"
                        ).isNull(),

                        F.lit(
                            "SHIP_DATE_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_units_sold"
                        ).isNull(),

                        F.lit(
                            "UNITS_SOLD_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_unit_price"
                        ).isNull(),

                        F.lit(
                            "UNIT_PRICE_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_unit_cost"
                        ).isNull(),

                        F.lit(
                            "UNIT_COST_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_total_revenue"
                        ).isNull(),

                        F.lit(
                            "TOTAL_REVENUE_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_total_cost"
                        ).isNull(),

                        F.lit(
                            "TOTAL_COST_INVALID"
                        )
                    ),

                    F.when(
                        F.col(
                            "_total_profit"
                        ).isNull(),

                        F.lit(
                            "TOTAL_PROFIT_INVALID"
                        )
                    )
                )
            )
        )
    )


    # ========================================================
    # 19. SPLIT VALID / QUARANTINE
    # ========================================================

    valid_df = (
        validated_df

        .filter(
            F.size(
                F.col(
                    "dq_errors"
                )
            ) == 0
        )
    )


    quarantine_df = (
        validated_df

        .filter(
            F.size(
                F.col(
                    "dq_errors"
                )
            ) > 0
        )
    )


    valid_count = (
        valid_df.count()
    )

    quarantine_count = (
        quarantine_df.count()
    )


    print(
        f"Valid records     : "
        f"{valid_count}"
    )

    print(
        f"Quarantine records: "
        f"{quarantine_count}"
    )


    # ========================================================
    # 20. WRITE QUARANTINE
    # ========================================================

    if quarantine_count > 0:

        quarantine_output_df = (

            quarantine_df

            .withColumn(
                "dq_error_reason",

                F.concat_ws(
                    ",",
                    F.col(
                        "dq_errors"
                    )
                )
            )

            .withColumn(
                "quarantine_ts",
                F.current_timestamp()
            )

            # Very useful for tracing the SNS notification
            # back to these records.
            .withColumn(
                "silver_run_id",
                F.lit(
                    SILVER_RUN_ID
                )
            )

            .withColumn(
                "start_snapshot_id",
                F.lit(
                    last_snapshot_id
                ).cast(
                    "string"
                )
            )

            .withColumn(
                "end_snapshot_id",
                F.lit(
                    current_snapshot_id
                ).cast(
                    "string"
                )
            )

            .withColumn(
                "load_type",
                F.lit(
                    load_type
                )
            )

            .drop(
                "_order_id",
                "_order_date",
                "_ship_date",
                "_units_sold",
                "_unit_price",
                "_unit_cost",
                "_total_revenue",
                "_total_cost",
                "_total_profit",
                "dq_errors"
            )
        )


        quarantine_batch_path = (

            f"{QUARANTINE_PATH.rstrip('/')}/"
            f"snapshot_id="
            f"{current_snapshot_id}/"
        )


        (
            quarantine_output_df

            .write

            .mode(
                "overwrite"
            )

            .parquet(
                quarantine_batch_path
            )
        )


        print(
            f"Quarantine records written to: "
            f"{quarantine_batch_path}"
        )

    else:

        print(
            "No quarantine records."
        )


    # ========================================================
    # 21. PREPARE TYPED SILVER RECORDS
    # ========================================================

    silver_source_df = (

        valid_df

        .drop(
            "order_id",
            "order_date",
            "ship_date",
            "units_sold",
            "unit_price",
            "unit_cost",
            "total_revenue",
            "total_cost",
            "total_profit",
            "dq_errors"
        )

        .withColumnRenamed(
            "_order_id",
            "order_id"
        )

        .withColumnRenamed(
            "_order_date",
            "order_date"
        )

        .withColumnRenamed(
            "_ship_date",
            "ship_date"
        )

        .withColumnRenamed(
            "_units_sold",
            "units_sold"
        )

        .withColumnRenamed(
            "_unit_price",
            "unit_price"
        )

        .withColumnRenamed(
            "_unit_cost",
            "unit_cost"
        )

        .withColumnRenamed(
            "_total_revenue",
            "total_revenue"
        )

        .withColumnRenamed(
            "_total_cost",
            "total_cost"
        )

        .withColumnRenamed(
            "_total_profit",
            "total_profit"
        )

        .withColumn(
            "silver_update_ts",
            F.current_timestamp()
        )
    )


    # ========================================================
    # 22. DEDUPLICATE CURRENT INPUT
    # ========================================================

    dedup_window = (

        Window

        .partitionBy(
            "order_id"
        )

        .orderBy(

            F.col(
                "etl_create_ts"
            ).desc_nulls_last(),

            F.col(
                "order_date"
            ).desc_nulls_last(),

            F.col(
                "run_id"
            ).desc_nulls_last(),

            F.col(
                "glue_run_id"
            ).desc_nulls_last(),

            F.col(
                "file_id"
            ).desc_nulls_last()
        )
    )


    dedup_df = (

        silver_source_df

        .withColumn(

            "row_num",

            F.row_number().over(
                dedup_window
            )
        )

        .filter(
            F.col(
                "row_num"
            ) == 1
        )

        .drop(
            "row_num"
        )
    )


    dedup_count = (
        dedup_df.count()
    )

    duplicate_count = (
        valid_count
        -
        dedup_count
    )


    print(
        f"Duplicates removed : "
        f"{duplicate_count}"
    )

    print(
        f"Silver records      : "
        f"{dedup_count}"
    )


    # ========================================================
    # 23. REGISTER SILVER SOURCE VIEW
    #
    # The Silver Iceberg table is created explicitly during
    # bootstrap from infra/sql/iceberg/003-create-silver.sql.
    # ========================================================

    if dedup_count > 0:
        dedup_df.createOrReplaceTempView("silver_source")


    # ========================================================
    # 24. MERGE INTO SILVER
    # ========================================================

    if dedup_count > 0:


        merge_sql = f"""

        MERGE INTO {SILVER_TABLE} AS target

        USING silver_source AS source

        ON target.order_id = source.order_id


        WHEN MATCHED
        AND
        (
            target.etl_create_ts IS NULL

            OR source.etl_create_ts >
               target.etl_create_ts

            OR
            (
                source.etl_create_ts =
                target.etl_create_ts

                AND
                (
                    target.order_date IS NULL

                    OR source.order_date >=
                       target.order_date
                )
            )
        )

        THEN UPDATE SET

            target.region =
                source.region,

            target.country =
                source.country,

            target.item_type =
                source.item_type,

            target.sales_channel =
                source.sales_channel,

            target.order_priority =
                source.order_priority,

            target.order_date =
                source.order_date,

            target.ship_date =
                source.ship_date,

            target.units_sold =
                source.units_sold,

            target.unit_price =
                source.unit_price,

            target.unit_cost =
                source.unit_cost,

            target.total_revenue =
                source.total_revenue,

            target.total_cost =
                source.total_cost,

            target.total_profit =
                source.total_profit,

            target.run_id =
                source.run_id,

            target.file_id =
                source.file_id,

            target.file_name =
                source.file_name,

            target.source_file_uri =
                source.source_file_uri,

            target.source_name =
                source.source_name,

            target.dataset_name =
                source.dataset_name,

            target.manifest_key =
                source.manifest_key,

            target.glue_run_id =
                source.glue_run_id,

            target.etl_create_ts =
                source.etl_create_ts,

            target.silver_update_ts =
                source.silver_update_ts


        WHEN NOT MATCHED THEN

        INSERT (

            region,
            country,
            item_type,
            sales_channel,
            order_priority,

            order_date,
            order_id,
            ship_date,

            units_sold,
            unit_price,
            unit_cost,
            total_revenue,
            total_cost,
            total_profit,

            run_id,
            file_id,
            file_name,
            source_file_uri,
            source_name,
            dataset_name,
            manifest_key,
            glue_run_id,

            etl_create_ts,
            silver_update_ts
        )

        VALUES (

            source.region,
            source.country,
            source.item_type,
            source.sales_channel,
            source.order_priority,

            source.order_date,
            source.order_id,
            source.ship_date,

            source.units_sold,
            source.unit_price,
            source.unit_cost,
            source.total_revenue,
            source.total_cost,
            source.total_profit,

            source.run_id,
            source.file_id,
            source.file_name,
            source.source_file_uri,
            source.source_name,
            source.dataset_name,
            source.manifest_key,
            source.glue_run_id,

            source.etl_create_ts,
            source.silver_update_ts
        )

        """


        print(
            "Starting Silver MERGE..."
        )

        spark.sql(
            merge_sql
        )

        print(
            "Silver MERGE completed successfully."
        )

    else:

        print(
            "No valid Silver records to MERGE."
        )

    # ========================================================
    # 25. SAVE THIS EXACT RUN SUMMARY
    # ========================================================

    save_run_summary(

        status=
            "COMPLETED",

        load_type=
            load_type,

        start_snapshot_id=
            last_snapshot_id,

        end_snapshot_id=
            current_snapshot_id,

        bronze_records_read=
            bronze_count,

        valid_records=
            valid_count,

        quarantine_records=
            quarantine_count,

        duplicates_removed=
            duplicate_count,

        silver_records_processed=
            dedup_count
    )

    # ========================================================
    # 24. SAVE WATERMARK
    #
    # Only after all processing succeeded.
    # ========================================================

    save_watermark(

        snapshot_id=
            current_snapshot_id,

        bronze_records_read=
            bronze_count,

        valid_records=
            valid_count,

        quarantine_records=
            quarantine_count,

        duplicates_removed=
            duplicate_count,

        silver_records_processed=
            dedup_count
    )

    # ========================================================
    # 26. FINAL SUMMARY
    # ========================================================

    print(
        "========================================"
    )

    print(
        "BRONZE -> SILVER COMPLETE"
    )

    print(
        "========================================"
    )

    print(
        f"Silver Run ID      : "
        f"{SILVER_RUN_ID}"
    )

    print(
        f"Load type          : "
        f"{load_type}"
    )

    print(
        f"Previous snapshot  : "
        f"{last_snapshot_id}"
    )

    print(
        f"Current snapshot   : "
        f"{current_snapshot_id}"
    )

    print(
        f"Bronze read        : "
        f"{bronze_count}"
    )

    print(
        f"Valid              : "
        f"{valid_count}"
    )

    print(
        f"Quarantined        : "
        f"{quarantine_count}"
    )

    print(
        f"Duplicates removed : "
        f"{duplicate_count}"
    )

    print(
        f"Silver processed   : "
        f"{dedup_count}"
    )

    print(
        "========================================"
    )


    # ========================================================
    # 27. COMMIT
    # ========================================================

    job.commit()


if __name__ == "__main__":

    main()
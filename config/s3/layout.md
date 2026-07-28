                         External Sales ZIP
                                │
                                ▼
                     ┌─────────────────────┐
                     │ Acquisition Lambda  │
                     └──────────┬──────────┘
                                │
                    archive + landing + manifest
                                │
                                ▼
                      ┌───────────────────┐
                      │    Amazon S3      │
                      │  Landing Bucket   │
                      └─────────┬─────────┘
                                │
                                │
          ┌─────────────────────▼─────────────────────┐
          │          INGESTION STEP FUNCTION          │
          │                                           │
          │ Acquisition → Glue Landing→MSK → Status  │
          └─────────────────────┬─────────────────────┘
                                │
                                ▼
                ┌───────────────────────────┐
                │ Glue Spark: Landing→MSK   │
                │ validate + publish rows   │
                └─────────────┬─────────────┘
                              │
                              │ IAM/TLS :9098
                              ▼
                   ┌─────────────────────┐
                   │     Amazon MSK      │
                   │ iata-sales-records  │
                   └──────────┬──────────┘
                              │
                              ▼
                 ┌────────────────────────┐
                 │      MSK Connect       │
                 │ Apache Iceberg Sink    │
                 │    Custom Plugin       │
                 └────────────┬───────────┘
                              │
                              ▼
              ┌──────────────────────────────┐
              │       BRONZE ICEBERG         │
              │ Glue Catalog + S3            │
              │ bronze.sales_raw             │
              └──────────────┬───────────────┘
                             │
                             ▼
             ┌───────────────────────────────┐
             │ BRONZE→SILVER STEP FUNCTION   │
             └──────────────┬────────────────┘
                            │
                            ▼
                ┌──────────────────────┐
                │ Glue Bronze→Silver   │
                │                      │
                │ Snapshot incremental │
                │ DQ validation        │
                │ Quarantine           │
                │ Dedup order_id       │
                │ Iceberg MERGE        │
                └──────────┬───────────┘
                           │
                           ▼
              ┌───────────────────────────┐
              │      SILVER ICEBERG       │
              │ Glue Catalog + S3         │
              │ silver.sales              │
              └─────────────┬─────────────┘
                            │
                            ▼
                         Athena
CREATE TABLE IF NOT EXISTS yt_sales_iac_bronze.sales (
  region string,
  country string,
  item_type string,
  sales_channel string,
  order_priority string,
  order_date date,
  order_id bigint,
  ship_date date,
  units_sold bigint,
  unit_price decimal(12, 2),
  unit_cost decimal(12, 2),
  total_revenue decimal(18, 2),
  total_cost decimal(18, 2),
  total_profit decimal(18, 2),
  run_id string,
  file_id string,
  file_name string,
  source_file_uri string,
  source_name string,
  dataset_name string,
  manifest_key string,
  glue_run_id string,
  etl_create_ts timestamp,
  silver_update_ts timestamp
)
PARTITIONED BY (month(order_date))
LOCATION 's3://__LAKEHOUSE_BUCKET__/silver/sales'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='zstd'
);

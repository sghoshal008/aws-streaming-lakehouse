CREATE TABLE IF NOT EXISTS iata_sales_iac_bronze.sales_raw (
  region string,
  country string,
  item_type string,
  sales_channel string,
  order_priority string,
  order_date string,
  order_id string,
  ship_date string,
  units_sold string,
  unit_price string,
  unit_cost string,
  total_revenue string,
  total_cost string,
  total_profit string,
  run_id string,
  file_id string,
  file_name string,
  source_file_uri string,
  source_name string,
  dataset_name string,
  manifest_key string,
  glue_run_id string,
  etl_create_ts timestamp
)
PARTITIONED BY (day(etl_create_ts))
LOCATION 's3://__LAKEHOUSE_BUCKET__/bronze/sales_raw'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='zstd'
);

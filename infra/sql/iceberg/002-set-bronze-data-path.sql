ALTER TABLE iata_sales_iac_bronze.sales_raw
SET TBLPROPERTIES (
  'write.data.path'='s3://__LAKEHOUSE_BUCKET__/bronze/sales_raw/data'
);

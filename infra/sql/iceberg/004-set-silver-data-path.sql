ALTER TABLE yt_sales_iac_silver.sales
SET TBLPROPERTIES (
  'write.data.path'='s3://__LAKEHOUSE_BUCKET__/silver/sales/data'
);

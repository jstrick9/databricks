-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE production_transaction_live_log_stream
AS SELECT
  to_timestamp(lambda_create_ts, 'y-MM-d H:m:s') as lambda_create_ts,
  from_utc_timestamp(current_timestamp(), 'US/Eastern') as dbx_create_ts
  FROM cloud_files("s3://databricks-lab-tmdc/eds/stream_logs/production_transaction_live/", "json")

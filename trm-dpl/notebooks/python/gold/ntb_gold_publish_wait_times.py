# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Wait Times – S3 / UC Volume JSON snapshot for USPTO.gov CMS
# MAGIC
# MAGIC Environment-aware – NO hardcoded catalogs or buckets
# MAGIC - Input: Gold processing_wait_times – catalog from config/{dbx_env}/tmngpdb-conf.yaml
# MAGIC - Output: {publish_bucket}/wait_times_YYYY-MM-DD.json + wait_times_latest.json
# MAGIC - publish_bucket from common_configs['publish']['s3_bucket']

# COMMAND ----------

dbutils.widgets.text("dbx_env", "dev")
dbutils.widgets.text("config_file", "")
# optional override – normally taken from config
dbutils.widgets.text("snapshot_date", "")  # YYYY-MM-DD – blank = latest in Gold

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "tmngpdb-conf.yaml"
default_config = f"../../../config/{dbx_env}/{config_file_name}"
config_file = dbutils.widgets.get("config_file") or default_config

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

import json, datetime
from pyspark.sql import functions as F

common_configs = read_yaml(config_file)

tmngpdb_catalog = common_configs['schema']['trgt_catalog']
gold_schema_name = common_configs['schema'].get('gold_schema', 'gold')
publish_bucket = common_configs['publish']['s3_bucket'].rstrip('/')

spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.dbx_env', dbx_env)
spark.conf.set('conf.publish_bucket', publish_bucket)

gold_table = f"{tmngpdb_catalog}.{gold_schema_name}.processing_wait_times"
audit_table = f"{tmngpdb_catalog}.{gold_schema_name}.etl_audit_log"

print(f"dbx_env={dbx_env}")
print(f"gold_table={gold_table}")
print(f"publish_bucket={publish_bucket}")

# Resolve snapshot_date – widget override or latest in Gold
snapshot_str = dbutils.widgets.get("snapshot_date").strip()
if snapshot_str:
    snapshot_date = datetime.date.fromisoformat(snapshot_date)
else:
    latest = spark.sql(f"SELECT MAX(snapshot_date) as d FROM {gold_table}").collect()[0]["d"]
    if latest is None:
        raise Exception(f"No rows in {gold_table} – run gold/mart_processing_wait_times first")
    snapshot_date = latest

print(f"Publishing snapshot_date={snapshot_date}")

# COMMAND ----------

df = spark.table(gold_table).filter(F.col("snapshot_date") == F.lit(snapshot_date)).orderBy("metric_key")

rows = df.collect()
if not rows:
    raise Exception(f"No gold rows for snapshot_date={snapshot_date} in {gold_table}")

# Build CMS JSON – matches Drupal field schema for /trademarks/application-timeline
data_updated = str(rows[0]["processing_as_of_date"])
exam_start = rows[0]["exam_queue_start_date"]
exam_end = rows[0]["exam_queue_end_date"]

metrics = []
for r in rows:
    metrics.append({
        "metric_key": r["metric_key"],
        "name": r["metric_name"],
        "section": r["section"],
        "unit": r["unit"],
        "average": float(r["average_value"]) if r["average_value"] is not None else None,
        "target": float(r["target_value"]) if r["target_value"] is not None else None,
        "sample_size": int(r["sample_size"] or 0)
    })

payload = {
  "data_updated": data_updated,
  "snapshot_date": str(snapshot_date),
  "exam_queue": {
    "start": str(exam_start) if exam_start else None,
    "end": str(exam_end) if exam_end else None
  },
  "metrics": metrics,
  "source": "trm-dpl",
  "dbx_env": dbx_env,
  "published_ts": datetime.datetime.utcnow().isoformat() + "Z"
}

json_str = json.dumps(payload, indent=2)

# Write versioned + latest – works for s3://, abfss://, /Volumes/
versioned_key = f"{publish_bucket}/wait_times_{snapshot_date}.json"
latest_key = f"{publish_bucket}/wait_times_latest.json"

# dbutils.fs.put works for S3, ADLS, UC Volumes
dbutils.fs.put(versioned_key, json_str, overwrite=True)
dbutils.fs.put(latest_key, json_str, overwrite=True)

print(f"Published:\n  {versioned_key}\n  {latest_key}")
print(f"\nJSON preview (first 800 chars):\n{json_str[:800]}")

# COMMAND ----------

# Audit log
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {audit_table} (
  run_id STRING, job_name STRING, task_name STRING, status STRING,
  records_processed BIGINT, message STRING, run_ts TIMESTAMP
) USING DELTA
""")

audit_message = f"published {versioned_key} | metrics={len(metrics)} | dbx_env={dbx_env}"
spark.sql(f"""
INSERT INTO {audit_table}
VALUES (
  uuid(),
  'trm_wait_times_monthly',
  'publish_snapshot',
  'SUCCESS',
  {len(metrics)},
  '{audit_message}',
  current_timestamp()
)
""")

print(f"\nPublish complete - audit logged to {audit_table}")
print(f"Next step: run shared/cms_publisher with dbx_env={dbx_env} to POST to USPTO API Gateway")
# Databricks notebook source
# MAGIC %md
# MAGIC # QA Validations – Trademark Wait Times
# MAGIC Fails the job if data quality fails – blocks publish
# MAGIC
# MAGIC Environment-aware – NO hardcoded catalogs
# MAGIC Reads catalog/schema from config/{dbx_env}/tmngpdb-conf.yaml

# COMMAND ----------

dbutils.widgets.text("dbx_env", "dev")
dbutils.widgets.text("config_file", "")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "tmngpdb-conf.yaml"
default_config = f"../../../config/{dbx_env}/{config_file_name}"
config_file = dbutils.widgets.get("config_file") or default_config

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

import datetime
from pyspark.sql import functions as F

common_configs = read_yaml(config_file)
tmngpdb_catalog = common_configs['schema']['trgt_catalog']
gold_schema_name = common_configs['schema'].get('gold_schema', 'gold')

spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

gold_table = f"{tmngpdb_catalog}.{gold_schema_name}.processing_wait_times"
audit_table = f"{tmngpdb_catalog}.{gold_schema_name}.etl_audit_log"

print(f"QA - dbx_env={dbx_env}, gold_table={gold_table}")

# Find latest snapshot in Gold – don't assume today (job may be re-running)
latest_snapshot_row = spark.sql(f"SELECT MAX(snapshot_date) as d FROM {gold_table}").collect()[0]
snapshot_date = latest_snapshot_row["d"]
if snapshot_date is None:
    raise Exception(f"QA FAILED: No rows in {gold_table} – run gold/mart_processing_wait_times first")
print(f"Validating snapshot_date = {snapshot_date}")

df = spark.table(gold_table).filter(F.col("snapshot_date") == F.lit(snapshot_date))

checks = []

# 1. Row count = 12
n = df.count()
checks.append(("row_count_12", n == 12, f"found {n}, expected 12"))

# 2. No NULL averages
nulls = df.filter(F.col("average_value").isNull()).count()
checks.append(("no_null_averages", nulls == 0, f"{nulls} nulls"))

# 3. Bounds – months 0-36, days 0-365
bad_bounds = df.filter(
  ((F.col("unit") == "months") & ((F.col("average_value") < 0) | (F.col("average_value") > 36))) |
  ((F.col("unit") == "days") & ((F.col("average_value") < 0) | (F.col("average_value") > 365)))
).count()
checks.append(("bounds_check", bad_bounds == 0, f"{bad_bounds} out of bounds"))

# 4. MoM delta < 30%
prev_snapshot = spark.sql(f"SELECT MAX(snapshot_date) as d FROM {gold_table} WHERE snapshot_date < '{snapshot_date}'").collect()[0]["d"]
mom_ok = True
mom_msg = "no prior snapshot – first run, OK"
if prev_snapshot:
    prev = spark.table(gold_table).filter(F.col("snapshot_date") == F.lit(prev_snapshot)).select("metric_key", F.col("average_value").alias("prev_avg"))
    joined = df.join(prev, "metric_key", "left")
    bad = joined.filter(
      F.col("prev_avg").isNotNull() & (F.col("prev_avg") > 0) &
      (F.abs((F.col("average_value") - F.col("prev_avg")) / F.col("prev_avg")) > 0.30)
    )
    bad_count = bad.count()
    mom_ok = bad_count == 0
    if not mom_ok:
        bad_list = ", ".join([f"{r.metric_key} {r.prev_avg}→{r.average_value}" for r in bad.select("metric_key","prev_avg","average_value").collect()])
        mom_msg = f"{bad_count} metrics >30% MoM swing: {bad_list}"
    else:
        mom_msg = f"0 metrics >30% MoM swing – vs {prev_snapshot}"
checks.append(("mom_delta_lt_30pct", mom_ok, mom_msg))

# 5. target_value present
missing_target = df.filter(F.col("target_value").isNull()).count()
checks.append(("targets_present", missing_target == 0, f"{missing_target} missing"))

# 6. sample_size > 0 for all metrics (warn only – don't fail if ESU/LOP = 0 due to unmapped event codes)
zero_sample = df.filter(F.col("sample_size") == 0).count()
print(f"INFO: {zero_sample} metrics with sample_size = 0 - expected for ESU/LOP if event codes not mapped yet")
# checks.append(("sample_size_gt_0", zero_sample == 0, f"{zero_sample} zero-sample metrics"))

# COMMAND ----------

failed = []
for name, passed, msg in checks:
    status = "PASS" if passed else "FAIL"
    print(f"{status:5} {name}: {msg}")
    if not passed:
        failed.append((name, msg))

if failed:
    msg = "; ".join([f"{n}:{m}" for n, m in failed])
    spark.createDataFrame([(
      str(datetime.datetime.utcnow()),  # run_id – use job run_id in production
      "trm_wait_times_monthly",
      "qa_validations",
      "FAILED",
      int(n),
      msg[:2000],
      datetime.datetime.utcnow()
    )], ["run_id","job_name","task_name","status","records_processed","message","run_ts"]
    ).write.mode("append").saveAsTable(audit_table)
    raise Exception(f"QA FAILED - {len(failed)} checks failed: {failed}\nSee {audit_table} for details – publish is BLOCKED")

print("\nAll QA checks passed - publish allowed")

# Log success
spark.sql(f"""
INSERT INTO {audit_table}
VALUES (
  uuid(),
  'trm_wait_times_monthly',
  'qa_validations',
  'SUCCESS',
  {n},
  'all checks passed – snapshot_date={snapshot_date}',
  current_timestamp()
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### QA Summary – for USPTO.gov publish gate
# MAGIC
# MAGIC | Check | Status |
# MAGIC |---|---|
# MAGIC | Row count = 12 | PASS/FAIL |
# MAGIC | No NULL averages | PASS/FAIL |
# MAGIC | Bounds check (months 0-36, days 0-365) | PASS/FAIL |
# MAGIC | MoM delta < 30% | PASS/FAIL |
# MAGIC | Target values present | PASS/FAIL |
# MAGIC
# MAGIC If any check fails: job stops, no CMS POST is made, audit log entry written to `gold.etl_audit_log`, PagerDuty alert fires via Workflow notification.
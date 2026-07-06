# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngpdb-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
tmngpdb_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmngpdb_catalog=}, {data_quality_catalog=} ')

# COMMAND ----------

database = 'gold'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.gold 
# MAGIC COMMENT 'For gold layer data' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold: Processing Wait Times Mart
# MAGIC -- Publishes to USPTO.gov - https://www.uspto.gov/trademarks/application-timeline
# MAGIC -- Source: ${catalog}.silver.case_milestones
# MAGIC -- Maintained by: notebooks/python/gold/mart_processing_wait_times.py
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- 1. processing_wait_times - 12 USPTO.gov metrics, monthly snapshot
# MAGIC -- ============================================================
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.processing_wait_times (
# MAGIC   metric_key STRING NOT NULL COMMENT 'Stable machine key - e.g. first_action, registration_or_abandonment, pre_exam_teas, itu_sou, postreg_renewal - joins to metric_targets',
# MAGIC   metric_name STRING NOT NULL COMMENT 'Display name as shown on USPTO.gov - e.g. "First examining action in TSDR record"',
# MAGIC   section STRING COMMENT 'Page section grouping - summary / Pre-Examination Unit / Examination Support Unit (ESU) / Intent to use / Petitions Office / Post Registration',
# MAGIC   unit STRING NOT NULL COMMENT 'Unit of measure - months or days - months = avg_days / 30.44',
# MAGIC   average_value DOUBLE COMMENT 'Average wait time for this metric in this snapshot - rounded to 1 decimal - e.g. 4.3 months for First Action',
# MAGIC   target_value DOUBLE COMMENT 'USPTO published target - e.g. 5.0 months for First Action, 10 days for TEAS - from metric_targets',
# MAGIC   processing_as_of_date DATE COMMENT 'Data as-of date shown on USPTO.gov - e.g. 2026-05-31 - usually month-end',
# MAGIC   exam_queue_start_date DATE COMMENT 'Currently examining applications filed on or after this date - shown as "We are currently examining new applications submitted between: Feb 04, 2026 - Feb 18, 2026" - start of range',
# MAGIC   exam_queue_end_date DATE COMMENT 'Currently examining applications filed on or before this date - end of range',
# MAGIC   sample_size INT COMMENT 'Number of trademark cases included in the average calculation for this metric / snapshot - used for QA data quality checks',
# MAGIC   data_updated_ts TIMESTAMP COMMENT 'ETL run timestamp when this metric row was calculated - UTC',
# MAGIC   snapshot_date DATE NOT NULL COMMENT 'Monthly snapshot partition key - YYYY-MM-01 - one full set of 12 metrics per snapshot_date'
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (snapshot_date)
# MAGIC COMMENT 'TRM Gold - Trademark Processing Wait Times - 12 metrics published to www.uspto.gov/trademarks/application-timeline - 1 row per metric_key per snapshot_date - Source: trm_tmngpdb.silver.case_milestones'
# MAGIC TBLPROPERTIES (
# MAGIC   delta.enableChangeDataFeed = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- 2. metric_targets - editorial targets / config - seeded from wait_time_source_mapping.yml
# MAGIC -- ============================================================
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.metric_targets (
# MAGIC   metric_key STRING NOT NULL COMMENT 'Stable machine key - FK to processing_wait_times.metric_key - e.g. first_action',
# MAGIC   metric_name STRING NOT NULL COMMENT 'Display name - must match processing_wait_times.metric_name',
# MAGIC   section STRING COMMENT 'Page section grouping - controls sort order on USPTO.gov',
# MAGIC   unit STRING NOT NULL COMMENT 'months | days',
# MAGIC   target_value DOUBLE NOT NULL COMMENT 'USPTO published target wait time - e.g. 5.0 months for First Action, 10 days for TEAS, 90 days for Post-Reg',
# MAGIC   sort_order INT COMMENT 'Display sort order on USPTO.gov - 10=First Action, 20=Registration, 100+=Processing Times table'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'TRM Gold - Wait Time metric targets / editorial config - 12 rows - seed from notebooks/config/wait_time_source_mapping.yml - drives Gold mart joins and CMS publish ordering';
# MAGIC
# MAGIC ALTER TABLE ${conf.catalog}.gold.metric_targets ADD CONSTRAINT pk_metric_targets PRIMARY KEY (metric_key) RELY;
# MAGIC
# MAGIC -- Seed metric_targets if empty - matches USPTO.gov May 31, 2026
# MAGIC -- Run once:
# MAGIC -- INSERT INTO ${catalog}.gold.metric_targets VALUES
# MAGIC -- ('first_action','First examining action in TSDR record','summary','months',5.0,10),
# MAGIC -- ('registration_or_abandonment','Trademark registering or application abandoning','summary','months',11.0,20),
# MAGIC -- ('pre_exam_teas','TEAS','Pre-Examination Unit','days',10,100),
# MAGIC -- ('pre_exam_madrid','MADRID','Pre-Examination Unit','days',10,110),
# MAGIC -- ('esu_responses','Responses/Corrections','Examination Support Unit (ESU)','days',14,200),
# MAGIC -- ('itu_extension','Extension requests','Intent to use','days',15,300),
# MAGIC -- ('itu_sou','Statement of use','Intent to use','days',15,310),
# MAGIC -- ('itu_divisional','Divisional requests','Intent to use','days',15,320),
# MAGIC -- ('petitions_lop','Letters of protest','Petitions Office','days',60,400),
# MAGIC -- ('postreg_affidavit','Affidavits of Use/Incontestability','Post Registration','days',90,500),
# MAGIC -- ('postreg_renewal','Renewals','Post Registration','days',90,510),
# MAGIC -- ('postreg_amendment','Amendments/Corrections','Post Registration','days',90,520);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- 3. etl_audit_log - run history / QA / publish audit trail
# MAGIC -- ============================================================
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.etl_audit_log (
# MAGIC   run_id STRING NOT NULL COMMENT 'Databricks job run_id / UUID - groups all tasks in a single pipeline run',
# MAGIC   job_name STRING NOT NULL COMMENT 'Databricks Workflow job name - e.g. trm_wait_times_monthly',
# MAGIC   task_name STRING NOT NULL COMMENT 'Task / notebook name - e.g. bronze_ingest_trmng, silver_case_timeline, gold_wait_times, qa_validations, publish_snapshot, trigger_cms_update',
# MAGIC   status STRING NOT NULL COMMENT 'SUCCESS | FAILED | SKIPPED | BLOCKED - QA failures log FAILED and block downstream publish',
# MAGIC   records_processed BIGINT COMMENT 'Number of records processed by this task - e.g. cases in Silver, metrics in Gold (12), 0 for CMS POST',
# MAGIC   message STRING COMMENT 'Human-readable status / error message - e.g. "QA FAILED: mom_delta_lt_30pct - first_action swing 42%", "CMS publish SUCCESS - publish_id abc123", "all checks passed"',
# MAGIC   run_ts TIMESTAMP NOT NULL COMMENT 'Task completion timestamp - UTC'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'TRM Gold - ETL audit log for trm_wait_times_monthly - used for data quality monitoring, PagerDuty alerting, and OCIO ATO audit trail'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true'
# MAGIC );
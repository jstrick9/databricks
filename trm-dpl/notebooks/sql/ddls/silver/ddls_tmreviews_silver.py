# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmreviews-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmreviews_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmreviews_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmreviews_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmreviews_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.silver 
# MAGIC COMMENT 'For staging layer data' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table ${conf.catalog}.silver.job_log (
# MAGIC   job_log_id BIGINT COMMENT 'Unique identifier for each job log entry',--not null generated always as identity,
# MAGIC   job_nm STRING COMMENT 'Name of the job',
# MAGIC   start_ts TIMESTAMP COMMENT 'Timestamp indicating the start time of the job',
# MAGIC   end_ts TIMESTAMP COMMENT 'Timestamp indicating the end time of the job',
# MAGIC   status_ct STRING COMMENT 'Status code indicating the status of the job',
# MAGIC   src_cnt INT COMMENT 'Number of records in the source dataset',
# MAGIC   trgt_cnt INT COMMENT 'Number of records in the target dataset',
# MAGIC   comment_tx STRING COMMENT 'Additional comments or notes about the job'
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/silver/job_log'

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table ${conf.catalog}.silver.job_control (
# MAGIC   job_control_id BIGINT COMMENT 'The unique identifier for each job in the job_control table.',-- not null generated always as identity,
# MAGIC   job_nm STRING COMMENT 'The name of the job being executed.',
# MAGIC   load_ts TIMESTAMP COMMENT 'The timestamp when the job was loaded into the system.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp when the job was created.',
# MAGIC   create_user_id STRING COMMENT 'The user ID of the individual who created the job.',
# MAGIC   last_mod_ts TIMESTAMP COMMENT 'The timestamp when the job was last modified.',
# MAGIC   last_mod_user_id STRING COMMENT 'The user ID of the individual who last modified the job.'
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/silver/job_control'

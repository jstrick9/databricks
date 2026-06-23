# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngpdb-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmngpdb_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmngpdb_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmngpdb_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmngpdb_catalog)
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
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/job_log'

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
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/job_control'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.tmapplser
# MAGIC ( 
# MAGIC   actcd STRING comment 'The action code associated with the load', 
# MAGIC   sernum STRING comment 'The serial number of load',
# MAGIC   pulldt DATE comment 'The date row was pulled from source tables',
# MAGIC   tabname STRING comment 'The table_name were row was pulled from',
# MAGIC   create_ts TIMESTAMP  comment 'The date and time that the record is inserted in the database',
# MAGIC   create_user_id string   comment 'The User Identifier of the logged-on AIS User that initiated the insert of the record into the database',
# MAGIC   last_mod_ts TIMESTAMP  comment 'The date and time that the record was last modified in the database.Upon creation, this will be the same as the Create Timestamp' ,
# MAGIC   last_mod_user_id string  comment 'The User Identifier of the logged on User that initiated the last modification to the record in the database' ,
# MAGIC   lock_control_no INT  comment 'A Number used  to verify that the record being updated has not been altered since it was retrieved for update when optimistic locking is used.'
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/tmapplser'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true, 'delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.bdss_class
# MAGIC ( 
# MAGIC   cl_ser_num STRING comment 'The serial number of load',
# MAGIC   cl_cls_intl_ct INT comment 'count of international classes',
# MAGIC   cl_cls_us_ct INT comment 'count of us classes',
# MAGIC   cls_intl STRING comment 'list of international classes',
# MAGIC   cls_us STRING comment 'list of us classes',
# MAGIC   cls_stat STRING comment 'status of classes',
# MAGIC   dt_stat INT comment 'status date',
# MAGIC   dt_1_use INT comment 'date of first use',
# MAGIC   dt_1_use_comm INT comment 'date of first use commercial',
# MAGIC   prime_cls STRING comment 'prime class',
# MAGIC   create_ts TIMESTAMP  comment 'The date and time that the record is inserted in the database',
# MAGIC   create_user_id string  comment 'The User Identifier of the logged-on AIS User that initiated the insert of the record into the database'
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/bdss_class'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

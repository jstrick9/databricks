# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create TMRWORKER bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file="../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tmworker-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs=read_yaml(config_file)
tmworker_catalog = common_configs['schema']['trgt_catalog']
src_folder=common_configs['cdc']['src_csv_files']
src_database=common_configs['cdc']['src_database']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.tmworker_catalog', tmworker_catalog.lower())
print(f'{tmworker_catalog=},{src_folder=}, ,{src_database=}')

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmworker_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.src_folder', src_folder)
spark.conf.set('conf.src_database', src_database)


# COMMAND ----------

# MAGIC %sql
# MAGIC create CATALOG if not exists  ${conf.catalog};
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table ${conf.catalog}.${conf.database}.WORKER;
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.WORKER(
# MAGIC  worker_gid             string,
# MAGIC  worker_no              string,
# MAGIC  worker_nm              string,
# MAGIC  grade_ct               string,
# MAGIC  signatory_authority_ct string,
# MAGIC  brs_user_id            string,
# MAGIC  active_in              string,
# MAGIC  worker_ct              string,
# MAGIC  cfk_patron_id          string,
# MAGIC  email_address_tx       string,
# MAGIC  begin_effective_dt     timestamp,
# MAGIC  end_dt                 timestamp,
# MAGIC  lock_control_no        DECIMAL(10,0),
# MAGIC  create_ts              timestamp,
# MAGIC  create_user_id         string,
# MAGIC  last_mod_ts            timestamp,
# MAGIC  last_mod_user_id       string,
# MAGIC  grade_step_ct          string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/WORKER'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table ${conf.catalog}.${conf.database}.TM_ORGANIZATION;
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.TM_ORGANIZATION(
# MAGIC  tm_organization_gid string,
# MAGIC  cfk_organization_id int,
# MAGIC  organization_cd     string,
# MAGIC  organization_nm     string,
# MAGIC  description_tx      string,
# MAGIC  email_address_tx    string,
# MAGIC  begin_effective_dt  timestamp,
# MAGIC  end_effective_dt    timestamp,
# MAGIC  lock_control_no     DECIMAL(10,0),
# MAGIC  create_ts           timestamp,
# MAGIC  create_user_id      string,
# MAGIC  last_mod_ts         timestamp,
# MAGIC  last_mod_user_id    string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/TM_ORGANIZATION'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC --s3://bdr-databricks-app-dev/fqt-trm-trmworker-csv/TMWORKER/TM_ORGANIZATION/

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.control_table};
# MAGIC
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.control_table} (
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string,
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   full_load string,
# MAGIC   initial_load_finished boolean
# MAGIC )location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/${conf.control_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.SYNC_TRANSLATE_LOCATION (
# MAGIC   LAW_OFFICE_CD STRING,
# MAGIC   PALM_SHORT_CD STRING,
# MAGIC   TT_TEXT STRING,
# MAGIC   GROUP_CD STRING,
# MAGIC   ACTIVE_IND STRING,
# MAGIC   EMAIL_TX STRING,
# MAGIC   TM_ORGANIZATION_GID STRING
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/SYNC_TRANSLATE_LOCATION'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.WORKER_ROLE (
# MAGIC   FK_USER_ROLE_ID INTEGER,
# MAGIC   FK_TM_ORGANIZATION_GID STRING,
# MAGIC   FK_WORKER_GID STRING,
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP,
# MAGIC   END_EFFECTIVE_DT TIMESTAMP,
# MAGIC   LOCK_CONTROL_NO INTEGER,
# MAGIC   CREATE_TS TIMESTAMP,
# MAGIC   CREATE_USER_ID STRING,
# MAGIC   LAST_MOD_TS TIMESTAMP,
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/WORKER_ROLE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_organization_rltnshp (
# MAGIC   FK_PARENT_TM_ORGANIZATION_GID string,
# MAGIC   FK_CHILD_TM_ORGANIZATION_GID string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   BEGIN_EFFECTIVE_DT timestamp,
# MAGIC   END_EFFECTIVE_DT date
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/tm_organization_rltnshp'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.transaction_instance (
# MAGIC   FK_LEGACY_TRANSACTION_CD string,
# MAGIC   CFK_EMPLOYEE_NO string,
# MAGIC   TRANSACTION_INSTANCE_GID string,
# MAGIC   TRANSACTION_INSTANCE_ID string,
# MAGIC   EFFECTIVE_TS timestamp,
# MAGIC   DETAILS_TX string,
# MAGIC   TERMINATED_IN string,
# MAGIC   ORIGIN_LOCATION_TX string,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/transaction_instance'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.user_role (
# MAGIC   USER_ROLE_ID decimal(20,0),
# MAGIC   USER_ROLE_CD string,
# MAGIC   TITLE_TX string,
# MAGIC   DESCRIPTION_TX string,
# MAGIC   BEGIN_EFFECTIVE_DT date,
# MAGIC   END_EFFECTIVE_DT date,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/user_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.user_role_group (
# MAGIC   USER_ROLE_GROUP_CD string,
# MAGIC   TITLE_TX string,
# MAGIC   DESCRIPTION_TX string,
# MAGIC   BEGIN_EFFECTIVE_DT date,
# MAGIC   END_EFFECTIVE_DT date,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/user_role_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.worker_h (
# MAGIC   WORKER_GID string,
# MAGIC   WORKER_NO string,
# MAGIC   WORKER_NM string,
# MAGIC   GRADE_CT string,
# MAGIC   SIGNATORY_AUTHORITY_CT string,
# MAGIC   BRS_USER_ID string,
# MAGIC   ACTIVE_IN string,
# MAGIC   WORKER_CT string,
# MAGIC   CFK_PATRON_ID string,
# MAGIC   EMAIL_ADDRESS_TX string,
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP,
# MAGIC   END_DT date,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   GRADE_STEP_CT string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/worker_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.worker_role_h (
# MAGIC   FK_USER_ROLE_ID integer,
# MAGIC   FK_TM_ORGANIZATION_GID string,
# MAGIC   FK_WORKER_GID string,
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP,
# MAGIC   END_EFFECTIVE_DT TIMESTAMP,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/worker_role_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

table_schema = spark.table(f'{tmworker_catalog}.{database}.{control_table}').schema

table_data = [
    (src_folder+"/"+"WORKER", 
     tmworker_catalog, 
     database,
     "worker",
     src_database,
     "WORKER",
     "worker_gid",
     "N",     
     False
    ),
     (src_folder+"/"+"TM_ORGANIZATION", 
     tmworker_catalog, 
     database,
     "tm_organization",
     src_database,
     "TM_ORGANIZATION",
     "tm_organization_gid",
     "N",
     False 
    ),
    (src_folder+"/"+"SYNC_TRANSLATE_LOCATION", 
    tmworker_catalog, 
    database,
    "sync_translate_location",
    src_database,
    "SYNC_TRANSLATE_LOCATION",
    "",
    "N",    
    False  
    ),
    (src_folder+"/"+"WORKER_ROLE", 
    tmworker_catalog, 
    database,
    "worker_role",
    src_database,
    "WORKER_ROLE",
    "FK_USER_ROLE_ID,FK_TM_ORGANIZATION_CD,FK_WORKER_GID",
    "N",
    False
    )
]
 
df = spark.createDataFrame(data=table_data,schema=table_schema)
display(df)
df.write.mode('overwrite').saveAsTable(f'{tmworker_catalog}.{database}.{control_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-history table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.job_history_table};
# MAGIC
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.job_history_table} (
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmworker/bronze/${conf.job_history_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

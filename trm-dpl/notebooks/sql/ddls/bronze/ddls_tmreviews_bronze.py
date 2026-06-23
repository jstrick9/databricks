# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create tmreviews bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tmreviews-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 


# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmreviews_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmreviews_catalog=}, {data_quality_catalog=} ')
src_folder = common_configs['cdc']['src_csv_files']
src_database = common_configs['cdc']['src_database']
spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
spark.conf.set('config.tmreviews_catalog', tmreviews_catalog.lower()) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
catalog = tmreviews_catalog
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmreviews_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.tmreviews_catalog} MANAGED LOCATION 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.PRE_EXAM_QUALITY_REVIEW(
# MAGIC cfk_trademark_gid             string,
# MAGIC dn_serial_num_tx              string,
# MAGIC appeal_in                     string,
# MAGIC cop_in                        string,
# MAGIC created_dt                    timestamp,
# MAGIC cfk_department_cd             string,
# MAGIC lead_assigned_dt              timestamp,
# MAGIC cfk_lead_worker_no            string,
# MAGIC manager_assigned_dt           timestamp,
# MAGIC cfk_manager_worker_no         string,
# MAGIC cfk_bcr_pay_period_range_name string,
# MAGIC random_no                     decimal,
# MAGIC cfk_review_status_cd          decimal,
# MAGIC cfk_reviewee_worker_no        string,
# MAGIC upload_count_qt               decimal,
# MAGIC uploaded_dt                   timestamp,
# MAGIC dn_amq_rsn                    decimal,
# MAGIC lock_control_no               decimal,
# MAGIC create_ts                     timestamp,
# MAGIC create_user_id                string,
# MAGIC last_mod_ts                   timestamp,
# MAGIC last_mod_user_id              string,
# MAGIC delete_in                     string)
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/PRE_EXAM_QUALITY_REVIEW'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.PRE_EXAM_QUALITY_RVW_ERR(
# MAGIC cfk_trademark_gid             string,
# MAGIC error_field_no                decimal,
# MAGIC dn_serial_num_tx              string,
# MAGIC completed_dt                  timestamp,
# MAGIC created_dt                    timestamp,
# MAGIC cfk_department_cd             string,
# MAGIC error_explanation_tx          string,
# MAGIC cfk_error_field_cd            string,
# MAGIC cfk_reviewee_worker_no        string,
# MAGIC cfk_reviewer_worker_no        string,
# MAGIC cfk_bcr_pay_period_range_name string,
# MAGIC cfk_review_status_cd          decimal,
# MAGIC cfk_review_level_cd           decimal,
# MAGIC dn_amqe_rsn                   decimal,
# MAGIC lock_control_no               decimal,
# MAGIC create_ts                     timestamp,
# MAGIC create_user_id                string,
# MAGIC last_mod_ts                   timestamp,
# MAGIC last_mod_user_id              string,
# MAGIC delete_in                     string)
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/PRE_EXAM_QUALITY_RVW_ERR'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${config.tmreviews_catalog}.${conf.database}.${conf.control_table};
# MAGIC
# MAGIC create table if not exists ${config.tmreviews_catalog}.${conf.database}.${conf.control_table} (
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string,
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   full_load string,
# MAGIC   initial_load_finished boolean
# MAGIC )USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/${conf.control_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.POST_REG_QUALITY_REVIEW (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   CREATED_DT timestamp,
# MAGIC   RANDOM_NO integer,
# MAGIC   CFK_BE_ORDER_NO integer,
# MAGIC   CFK_OBJECT_TYPE_CD string,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   DN_BUSINESS_EVENT_REASON_CD string,
# MAGIC   APPEAL_IN string,
# MAGIC   APPEAL_COMPLETED_DT date,
# MAGIC   APPEAL_RECEIPT_DT date,
# MAGIC   CFK_APPEAL_CO_REVIEWR_WRKR_NO string,
# MAGIC   CFK_CO_REVIEWER_WORKER_NO string,
# MAGIC   COP_IN string,
# MAGIC   FOLLOWUP_DT date,
# MAGIC   FOLLOWUP_IN string,
# MAGIC   LEAD_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEAD_ASSIGNED_WORKER_NO string,
# MAGIC   LEVEL_1_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEVEL_3_MGR_ASGND_WRKR_NO string,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC   PREG_MANAGER_ASSIGNED_DT date,
# MAGIC   CFK_PREG_MGR_ASSIGNED_WRKR_NO string,
# MAGIC   DN_PRODUCTION_TRANSACTION_CD integer,
# MAGIC   CFK_QUERY_STATUS_CD integer,
# MAGIC   REVIEW_COMPLETED_DT timestamp,
# MAGIC   CFK_REVIEW_STATUS_CD integer,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   CFK_REVIEWER_WORKER_NO string,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   TRANSACTION_SYSTEM_DT timestamp,
# MAGIC   DN_PQR_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/POST_REG_QUALITY_REVIEW'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.POST_REG_QUALITY_REVIEW_ERRLOG (
# MAGIC   ORA_ERR_NUMBER decimal(20,0),
# MAGIC   ORA_ERR_MESG string,
# MAGIC   ORA_ERR_ROWID string,
# MAGIC   ORA_ERR_OPTYP string,
# MAGIC   ORA_ERR_TAG string,
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   CREATED_DT timestamp,
# MAGIC   RANDOM_NO integer,
# MAGIC   CFK_BE_ORDER_NO integer,
# MAGIC   CFK_OBJECT_TYPE_CD string,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   DN_BUSINESS_EVENT_REASON_CD string,
# MAGIC   APPEAL_IN string,
# MAGIC   APPEAL_COMPLETED_DT date,
# MAGIC   APPEAL_RECEIPT_DT date,
# MAGIC   CFK_APPEAL_CO_REVIEWR_WRKR_NO string,
# MAGIC   CFK_CO_REVIEWER_WORKER_NO string,
# MAGIC   COP_IN string,
# MAGIC   FOLLOWUP_DT date,
# MAGIC   FOLLOWUP_IN string,
# MAGIC   LEAD_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEAD_ASSIGNED_WORKER_NO string,
# MAGIC   LEVEL_1_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEVEL_3_MGR_ASGND_WRKR_NO string,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC   PREG_MANAGER_ASSIGNED_DT date,
# MAGIC   CFK_PREG_MGR_ASSIGNED_WRKR_NO string,
# MAGIC   DN_PRODUCTION_TRANSACTION_CD integer,
# MAGIC   CFK_QUERY_STATUS_CD integer,
# MAGIC   REVIEW_COMPLETED_DT timestamp,
# MAGIC   CFK_REVIEW_STATUS_CD integer,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   CFK_REVIEWER_WORKER_NO string,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   TRANSACTION_SYSTEM_DT timestamp,
# MAGIC   DN_PQR_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/POST_REG_QUALITY_REVIEW_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.POST_REG_QUALITY_REVIEW_H (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   CREATED_DT timestamp,
# MAGIC   RANDOM_NO integer,
# MAGIC   CFK_BE_ORDER_NO integer,
# MAGIC   CFK_OBJECT_TYPE_CD string,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   DN_BUSINESS_EVENT_REASON_CD string,
# MAGIC   APPEAL_IN string,
# MAGIC   APPEAL_COMPLETED_DT date,
# MAGIC   APPEAL_RECEIPT_DT date,
# MAGIC   CFK_APPEAL_CO_REVIEWR_WRKR_NO string,
# MAGIC   CFK_CO_REVIEWER_WORKER_NO string,
# MAGIC   COP_IN string,
# MAGIC   FOLLOWUP_DT date,
# MAGIC   FOLLOWUP_IN string,
# MAGIC   LEAD_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEAD_ASSIGNED_WORKER_NO string,
# MAGIC   LEVEL_1_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEVEL_3_MGR_ASGND_WRKR_NO string,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC   PREG_MANAGER_ASSIGNED_DT date,
# MAGIC   CFK_PREG_MGR_ASSIGNED_WRKR_NO string,
# MAGIC   DN_PRODUCTION_TRANSACTION_CD integer,
# MAGIC   CFK_QUERY_STATUS_CD integer,
# MAGIC   REVIEW_COMPLETED_DT timestamp,
# MAGIC   CFK_REVIEW_STATUS_CD integer,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   CFK_REVIEWER_WORKER_NO string,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   TRANSACTION_SYSTEM_DT timestamp,
# MAGIC   DN_PQR_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/POST_REG_QUALITY_REVIEW_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.POST_REG_REVIEW_NOTICE (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   CREATED_DT timestamp,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC   RANDOM_NO integer,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   DN_PRODUCTION_TRANSACTION_CD integer,
# MAGIC   DN_BUSINESS_EVENT_REASON_CD string,
# MAGIC   CFK_BE_ORDER_NO integer,
# MAGIC   APPEAL_IN string,
# MAGIC   CFK_APPEAL_STATUS_CD integer,
# MAGIC   APPEAL_SUBMITTED_DT date,
# MAGIC   APPEAL_END_DT date,
# MAGIC   LEAD_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEAD_ASSIGNED_WORKER_NO string,
# MAGIC   LEVEL_1_ASSIGNED_DT date,
# MAGIC   CFK_PREG_MGR_ASSIGNED_WRKR_NO string,
# MAGIC   PREG_MANAGER_ASSIGNED_DT date,
# MAGIC   EXTENSION_IN string,
# MAGIC   FOLLOWUP_DT date,
# MAGIC   FOLLOWUP_IN string,
# MAGIC   CFK_QUERY_STATUS_CD integer,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   TRANSACTION_SYSTEM_DT timestamp,
# MAGIC   DN_PRN_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/POST_REG_REVIEW_NOTICE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.POST_REG_REVIEW_NOTICE_ERRLOG (
# MAGIC   ORA_ERR_NUMBER decimal(20,0),
# MAGIC   ORA_ERR_MESG string,
# MAGIC   ORA_ERR_OPTYP string,
# MAGIC   ORA_ERR_TAG string,
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   CREATED_DT timestamp,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC   RANDOM_NO integer,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   DN_PRODUCTION_TRANSACTION_CD integer,
# MAGIC   DN_BUSINESS_EVENT_REASON_CD string,
# MAGIC   CFK_BE_ORDER_NO integer,
# MAGIC   APPEAL_IN string,
# MAGIC   CFK_APPEAL_STATUS_CD integer,
# MAGIC   APPEAL_SUBMITTED_DT date,
# MAGIC   APPEAL_END_DT date,
# MAGIC   LEAD_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEAD_ASSIGNED_WORKER_NO string,
# MAGIC   LEVEL_1_ASSIGNED_DT date,
# MAGIC   CFK_PREG_MGR_ASSIGNED_WRKR_NO string,
# MAGIC   PREG_MANAGER_ASSIGNED_DT date,
# MAGIC   EXTENSION_IN string,
# MAGIC   FOLLOWUP_DT date,
# MAGIC   FOLLOWUP_IN string,
# MAGIC   CFK_QUERY_STATUS_CD integer,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   TRANSACTION_SYSTEM_DT timestamp,
# MAGIC   DN_PRN_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/POST_REG_REVIEW_NOTICE_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.POST_REG_REVIEW_NOTICE_H (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   CREATED_DT timestamp,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC   RANDOM_NO integer,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   DN_PRODUCTION_TRANSACTION_CD integer,
# MAGIC   DN_BUSINESS_EVENT_REASON_CD string,
# MAGIC   CFK_BE_ORDER_NO integer,
# MAGIC   APPEAL_IN string,
# MAGIC   CFK_APPEAL_STATUS_CD integer,
# MAGIC   APPEAL_SUBMITTED_DT date,
# MAGIC   APPEAL_END_DT date,
# MAGIC   LEAD_ASSIGNED_DT timestamp,
# MAGIC   CFK_LEAD_ASSIGNED_WORKER_NO string,
# MAGIC   LEVEL_1_ASSIGNED_DT timestamp,
# MAGIC   CFK_PREG_MGR_ASSIGNED_WRKR_NO string,
# MAGIC   PREG_MANAGER_ASSIGNED_DT date,
# MAGIC   EXTENSION_IN string,
# MAGIC   FOLLOWUP_DT date,
# MAGIC   FOLLOWUP_IN string,
# MAGIC   CFK_QUERY_STATUS_CD integer,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   TRANSACTION_SYSTEM_DT timestamp,
# MAGIC   DN_PRN_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/POST_REG_REVIEW_NOTICE_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.PREG_QUALITY_REVIEW_ELEMENT (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   FK_PRQR_CREATED_DT TIMESTAMP,
# MAGIC   FK_PRQR_RANDOM_NO integer,
# MAGIC   CFK_EXAMINATION_ELEMENT_CD string,
# MAGIC   ENTRY_NO integer,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   APPEAL_COMMENTS_TX string,
# MAGIC   APPEAL_NOTES_TX string,
# MAGIC   CFK_APPEAL_STATUS_CD integer,
# MAGIC   CREATED_DT TIMESTAMP,
# MAGIC   FREE_POINT_IN string,
# MAGIC   CFK_ORIGINAL_SEVERITY_CD string,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME integer,
# MAGIC   QUERY_COMMENTS_TX string,
# MAGIC   QUERY_TX string,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   CFK_REVIEWER_WORKER_NO string,
# MAGIC   CFK_SEVERITY_CD string,
# MAGIC   DN_PQRE_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/PREG_QUALITY_REVIEW_ELEMENT'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.PREG_QUALITY_REVIEW_ELEMENT_ERRLOG (
# MAGIC   ORA_ERR_NUMBER decimal(20,0),
# MAGIC   ORA_ERR_MESG string,
# MAGIC   ORA_ERR_ROWID string,
# MAGIC   ORA_ERR_OPTYP string,
# MAGIC   ORA_ERR_TAG string,
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   FK_PRQR_CREATED_DT TIMESTAMP,
# MAGIC   FK_PRQR_RANDOM_NO integer,
# MAGIC   CFK_EXAMINATION_ELEMENT_CD string,
# MAGIC   ENTRY_NO integer,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   APPEAL_COMMENTS_TX string,
# MAGIC   APPEAL_NOTES_TX string,
# MAGIC   CFK_APPEAL_STATUS_CD integer,
# MAGIC   CREATED_DT TIMESTAMP,
# MAGIC   FREE_POINT_IN string,
# MAGIC   CFK_ORIGINAL_SEVERITY_CD string,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME integer,
# MAGIC   QUERY_COMMENTS_TX string,
# MAGIC   QUERY_TX string,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   CFK_REVIEWER_WORKER_NO string,
# MAGIC   CFK_SEVERITY_CD string,
# MAGIC   DN_PQRE_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/PREG_QUALITY_REVIEW_ELEMENT_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmreviews_catalog}.${conf.database}.PREG_QUALITY_REVIEW_ELEMENT_H (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   FK_PRQR_CREATED_DT TIMESTAMP,
# MAGIC   FK_PRQR_RANDOM_NO integer,
# MAGIC   CFK_EXAMINATION_ELEMENT_CD string,
# MAGIC   ENTRY_NO integer,
# MAGIC   DN_SERIAL_NUM_TX string,
# MAGIC   DN_REGISTRATION_NUM integer,
# MAGIC   APPEAL_COMMENTS_TX string,
# MAGIC   APPEAL_NOTES_TX string,
# MAGIC   CFK_APPEAL_STATUS_CD integer,
# MAGIC   CREATED_DT TIMESTAMP,
# MAGIC   FREE_POINT_IN string,
# MAGIC   CFK_ORIGINAL_SEVERITY_CD string,
# MAGIC   CFK_BCR_PAY_PERIOD_RANGE_NAME integer,
# MAGIC   QUERY_COMMENTS_TX string,
# MAGIC   QUERY_TX string,
# MAGIC   CFK_REVIEW_TYPE_CD string,
# MAGIC   CFK_REVIEWEE_WORKER_NO string,
# MAGIC   CFK_REVIEWER_WORKER_NO string,
# MAGIC   CFK_SEVERITY_CD string,
# MAGIC   DN_PQRE_RSN decimal(20,0),
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp,
# MAGIC   DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/PREG_QUALITY_REVIEW_ELEMENT_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

table_schema = spark.table(f'{catalog}.{database}.{control_table}').schema

table_data = [
    (src_folder+"/"+"PRE_EXAM_QUALITY_REVIEW", 
     catalog, 
     database,
     "pre_exam_quality_review",
     src_database,
     "PRE_EXAM_QUALITY_REVIEW",
     "cfk_trademark_gid",
     False
    ),
    (src_folder+"/"+"PRE_EXAM_QUALITY_RVW_ERR", 
     catalog, 
     database,
     "pre_exam_quality_rvw_err",
     src_database,
     "PRE_EXAM_QUALITY_RVW_ERR",
     "cfk_trademark_gid,error_field_no",
     False
    )
]


 
df = spark.createDataFrame(data=table_data,schema=table_schema)

display(df)

df.write.mode('append').saveAsTable(f'{catalog}.{database}.{control_table}')

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
# MAGIC location 's3:// ${conf.cdc_bucket}/eds/delta_tables/trm_tmreviews/bronze/${conf.job_history_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

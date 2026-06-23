# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmintltm-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file
# MAGIC

# COMMAND ----------

common_configs = read_yaml(config_file)
tmintltm_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmintltm_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmbuscalendar_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 


# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog',  tmintltm_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)


# COMMAND ----------

# MAGIC %sql
# MAGIC create OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_appl_event (
# MAGIC   international_appl_event_id decimal(38,10),
# MAGIC   fk_international_appl_gid string,
# MAGIC   order_no int,
# MAGIC   international_appl_evnt_rsn_id int,
# MAGIC   fk_intl_appl_tran_instnc_gid string,
# MAGIC   dn_intl_reg_instance_num string,
# MAGIC   effective_ts timestamp,
# MAGIC   paper_in string,
# MAGIC   cfk_document_id string,
# MAGIC   cfk_worker_gid string,
# MAGIC   dn_worker_no string,
# MAGIC   recordal_dt timestamp,
# MAGIC   lock_control_no int,
# MAGIC   create_ts timestamp,
# MAGIC   create_user_id string,
# MAGIC   last_mod_ts timestamp,
# MAGIC   last_mod_user_id string
# MAGIC ) USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_appl_event' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace TABLE ${conf.catalog}.${conf.database}.international_appl_evnt_rsn (
# MAGIC international_appl_evnt_rsn_id  decimal(38,10),
# MAGIC international_appl_evnt_rsn_cd  string,
# MAGIC title_tx                        string,
# MAGIC description_tx                  string,
# MAGIC cfk_fsm_type_event_id           decimal,
# MAGIC prosecution_history_in          string,
# MAGIC alert_trigger_ct                string,
# MAGIC begin_effective_dt              timestamp,
# MAGIC end_effective_dt                timestamp,
# MAGIC create_ts                       timestamp,
# MAGIC create_user_id                  string,
# MAGIC last_mod_ts                     timestamp,
# MAGIC last_mod_user_id                string
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_appl_evnt_rsn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.base_application (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   DN_SERIAL_NUM string,
# MAGIC   FK_INTERNATIONAL_APPL_GID string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/base_application'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.base_application_h (
# MAGIC   FK_INTERNATIONAL_APPL_GID string,
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   DN_SERIAL_NUM string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   ACTION_CT string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/base_application_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.international_application (
# MAGIC   INTERNATIONAL_APPLICATION_GID string,
# MAGIC   INTERNATIONAL_US_REF_NO string,
# MAGIC   EMAIL_ADDRESS_TX string,
# MAGIC   AUTOMATIC_CERTIFICATION_IN string,
# MAGIC   IB_PUBLICATION_DT timestamp,
# MAGIC   ORIGINAL_FILING_DT timestamp,
# MAGIC   REPLY_BY_DT date,
# MAGIC   PAYMENT_REFERENCE_NO decimal,
# MAGIC   CFK_PAYMENT_TYPE_CD string,
# MAGIC   LOCK_CONTROL_NO decimal,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_application'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace TABLE ${conf.catalog}.${conf.database}.international_application_h (
# MAGIC   INTERNATIONAL_APPLICATION_GID string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   ACTION_CT string,
# MAGIC   INTERNATIONAL_US_REF_NO string,
# MAGIC   EMAIL_ADDRESS_TX string,
# MAGIC   AUTOMATIC_CERTIFICATION_IN string,
# MAGIC   IB_PUBLICATION_DT timestamp,
# MAGIC   ORIGINAL_FILING_DT timestamp,
# MAGIC   REPLY_BY_DT date,
# MAGIC   PAYMENT_REFERENCE_NO decimal,
# MAGIC   CFK_PAYMENT_TYPE_CD string,
# MAGIC   LOCK_CONTROL_NO decimal,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_application_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.international_registration (
# MAGIC   INTERNATIONAL_REG_GID string,
# MAGIC   FK_INTERNATIONAL_REG_NO string,
# MAGIC   INTERNATIONAL_REG_SEQ_NO string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_registration'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.international_registration_h (
# MAGIC   INTERNATIONAL_REG_GID string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   ACTION_CT string,
# MAGIC   FK_INTERNATIONAL_REG_NO string,
# MAGIC   INTERNATIONAL_REG_SEQ_NO string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_registration_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.international_reg_tm (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   FK_INTERNATIONAL_REG_GID string,
# MAGIC   DN_SERIAL_NUM string,
# MAGIC   CFK_STATUS_CD string,
# MAGIC   STATUS_DT timestamp,
# MAGIC   PRIORITY_CLAIMED_DT timestamp,
# MAGIC   AUTO_PROTECT_DT timestamp,
# MAGIC   NOTIFICATION_DT timestamp,
# MAGIC   CANCELLATION_DT timestamp,
# MAGIC   FIRST_REFUSAL_IN string,
# MAGIC   IB_RENEWAL_DT timestamp,
# MAGIC   IB_PUBLICATION_DT timestamp,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_reg_tm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.international_reg_tm_h (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   DN_SERIAL_NUM string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   ACTION_CT string,
# MAGIC   FK_INTERNATIONAL_REG_GID string,
# MAGIC   CFK_STATUS_CD string,
# MAGIC   STATUS_DT timestamp,
# MAGIC   PRIORITY_CLAIMED_DT timestamp,
# MAGIC   AUTO_PROTECT_DT timestamp,
# MAGIC   NOTIFICATION_DT timestamp,
# MAGIC   CANCELLATION_DT timestamp,
# MAGIC   FIRST_REFUSAL_IN string,
# MAGIC   IB_RENEWAL_DT timestamp,
# MAGIC   IB_PUBLICATION_DT timestamp,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_reg_tm_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.international_tm (
# MAGIC   INTERNATIONAL_REG_NO string,
# MAGIC   INTERNATIONAL_REG_DT timestamp,
# MAGIC   SOURCE_CT string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_tm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.international_tm_h (
# MAGIC   INTERNATIONAL_REG_NO string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   ACTION_CT string,
# MAGIC   INTERNATIONAL_REG_DT timestamp,
# MAGIC   SOURCE_CT string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_tm_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.base_appl_intl_reg (
# MAGIC CFK_TRADEMARK_GID string,
# MAGIC FK_INTERNATIONAL_APPL_GID string,
# MAGIC SEQUENCE_NO decimal,
# MAGIC FK_INTERNATIONAL_REG_GID  string,
# MAGIC CFK_STATUS_CD string,
# MAGIC STATUS_DT DATE,
# MAGIC IB_RENEWAL_DT DATE,
# MAGIC LOCK_CONTROL_NO decimal,
# MAGIC CREATE_TS TIMESTAMP,
# MAGIC CREATE_USER_ID  string,
# MAGIC LAST_MOD_TS TIMESTAMP,
# MAGIC LAST_MOD_USER_ID string
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/base_appl_intl_reg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_reg_tm_notice (
# MAGIC intl_reg_tm_notice_id decimal(38,10),
# MAGIC cfk_trademark_gid string, 
# MAGIC cfk_notice_type_cd string,
# MAGIC cfk_notice_source_cd string,
# MAGIC scheduled_notice_dt timestamp, 
# MAGIC processed_notice_dt timestamp,
# MAGIC lock_control_no int,
# MAGIC create_ts timestamp,
# MAGIC create_user_id string,
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_reg_tm_notice'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_base_application_notice (
# MAGIC tm_base_appl_notice_id decimal(38,10),
# MAGIC cfk_trademark_gid string, 
# MAGIC fk_international_appl_gid string, 
# MAGIC cfk_notice_type_cd string,
# MAGIC cfk_notice_source_cd string,
# MAGIC scheduled_notice_dt timestamp, 
# MAGIC processed_notice_dt timestamp,
# MAGIC lock_control_no int,
# MAGIC create_ts timestamp,
# MAGIC create_user_id string,
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string  
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_base_application_notice'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------



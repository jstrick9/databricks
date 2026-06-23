# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

if dbx_env == 'test':
  config_file = "../../../config/qa/tmngidmp-conf.yaml"
else:
  config_file = f"../../../config/{dbx_env}/tmngidmp-conf.yaml"

print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

configs = read_yaml(config_file)
tmngidmp_catalog = configs['schema']['trgt_catalog']
cdc_bucket = configs['cdc']['cdc_bucket']
spark.conf.set('config.cdc_bucket', cdc_bucket)
spark.conf.set('config.tmngidmp_catalog', tmngidmp_catalog)
spark.conf.set('config.dbx_env', dbutils.widgets.get('dbx_env'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.tmngidmp_catalog}

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${config.tmngidmp_catalog}.bronze 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${config.tmngidmp_catalog}.bronze.cdc_batch_job_control (
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
# MAGIC location 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/cdc_batch_job_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${config.tmngidmp_catalog}.bronze.cdc_batch_job_history (
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )USING delta
# MAGIC location 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/cdc_batch_job_history'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.AUDIT_REVISION (
# MAGIC   AUDIT_REVISION_ID INT, 
# MAGIC   OPERATION_CT STRING ,
# MAGIC   OBJECT_NM STRING,
# MAGIC   OBJECT_ID STRING,
# MAGIC   OBJECT_PROPERTY_NM STRING, 
# MAGIC   REVISION_TS TIMESTAMP,
# MAGIC   REVISION_USER_ID STRING, 
# MAGIC   DN_PARENT_OBJECT_NM STRING, 
# MAGIC   PARENT_OBJECT_ID STRING,
# MAGIC   FROM_VALUE_TX STRING,
# MAGIC   TO_VALUE_TX STRING,
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   CREATE_USER_ID STRING,
# MAGIC   CREATE_TS TIMESTAMP,
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/audit_revision'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_COMP (
# MAGIC   SN STRING,
# MAGIC   CLS STRING,
# MAGIC   STRIPPED_TEXT STRING, 
# MAGIC   ORIGINAL_TEXT STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_comp'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_COMP_PARSED (
# MAGIC   SN STRING, 
# MAGIC   CLS STRING,
# MAGIC   TXT STRING,
# MAGIC   ORG_TXT STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_comp_parsed'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_COMP_RESULT (
# MAGIC   SERIAL_NUMBER STRING, 
# MAGIC   CLASS STRING,
# MAGIC   ORIGINAL_TEXT STRING, 
# MAGIC   FILING_DATE STRING,
# MAGIC   CASE_STATUS STRING,
# MAGIC   GOODS_DESC STRING,
# MAGIC   MISCLASSIFIED STRING, 
# MAGIC   TEAS_PLUS_STATUS STRING,
# MAGIC   LITERAL STRING,
# MAGIC   STATUS_DATE STRING, 
# MAGIC   EXAMINING_ATTORNEY STRING,
# MAGIC   LAW_OFFICE STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_comp_result'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_COMP_SAM (
# MAGIC   SN STRING, 
# MAGIC   CLS STRING,
# MAGIC   TXT STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_comp_sam'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_COMP_SAM_RESULT (
# MAGIC   SERIAL_NUMBER STRING, 
# MAGIC   CLASS STRING,
# MAGIC   ORIGINAL_TEXT STRING, 
# MAGIC   FILING_DATE STRING, 
# MAGIC   CASE_STATUS STRING, 
# MAGIC   GOODS_DESC STRING, 
# MAGIC   MISCLASSIFIED STRING, 
# MAGIC   TEAS_PLUS_STATUS STRING, 
# MAGIC   LITERAL STRING, 
# MAGIC   STATUS_DATE STRING, 
# MAGIC   EXAMINING_ATTORNEY STRING, 
# MAGIC   LAW_OFFICE STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_comp_sam_result'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_COMP_TEST (
# MAGIC   SN STRING, 
# MAGIC   CLS STRING, 
# MAGIC   TXT STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_comp_test'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_ID (
# MAGIC   CLS STRING, 
# MAGIC   TXT STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_id'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_ID_CASE_LEVEL_RESULT (
# MAGIC   SERIAL_NUMBER STRING, 
# MAGIC   CASE_STATUS STRING, 
# MAGIC   FILING_DATE STRING, 
# MAGIC   CASE_GOODS_SERVICE STRING,
# MAGIC   CLASS STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_id_case_level_result'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_ID_PARSED (
# MAGIC   CLS STRING, 
# MAGIC   TXT STRING, 
# MAGIC   ORIG_TXT STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_id_parsed'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_ID_PARSED_STANDARD (
# MAGIC   CLS STRING, 
# MAGIC   TXT STRING, 
# MAGIC   ORIG_TXT STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_id_parsed_standard'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_TEAS_PLUS_CLOB (
# MAGIC   SERIALNUMBER STRING, 
# MAGIC   CLASS STRING, 
# MAGIC   SUBMISSIONID STRING, 
# MAGIC   FINAL_DESC STRING, 
# MAGIC   TXT STRING, 
# MAGIC   PARSED_TEXT STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_teas_plus_clob'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.DATA_TEAS_STANDARD_CLOB (
# MAGIC   SERIALNUMBER STRING, 
# MAGIC   CLASS STRING, 
# MAGIC   SUBMISSIONID STRING, 
# MAGIC   FINAL_DESC STRING, 
# MAGIC   TXT STRING, 
# MAGIC   PARSED_TEXT STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/data_teas_standard_clob'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM (
# MAGIC   GOODS_SERVICES_TERM_ID INT, 
# MAGIC   GOODS_SERVICES_TERM_ID_TX STRING, 
# MAGIC   MODIFICATION_NO INT, 
# MAGIC   MODIFICATION_DRAFT_NO INT, 
# MAGIC   TERM_CT STRING, 
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   FK_CLASS_ID INT, 
# MAGIC   FK_TERM_STATUS_CD STRING, 
# MAGIC   CFK_AUTHOR_EMPLOYEE_NO STRING, 
# MAGIC   ACCEPT_PARTNERSHIP_DT DATE, 
# MAGIC   FK_EDITION_NO INT, 
# MAGIC   FK_VERSION_NO INT, 
# MAGIC   FK_RELEASE_NO INT, 
# MAGIC   FK_PREVIOUS_GDS_SRVCS_TERM_ID INT, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP,
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   FK_TAXONOMY_GROUP_ID INT, 
# MAGIC   TM5_ACCEPT_IN STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/goods_services_term'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_DRAFT (
# MAGIC   GOODS_SERVICES_TERM_ID INT, 
# MAGIC   GOODS_SERVICES_TERM_ID_TX STRING, 
# MAGIC   MODIFICATION_NO INT, 
# MAGIC   MODIFICATION_DRAFT_NO INT, 
# MAGIC   TERM_CT STRING, 
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   FK_CLASS_ID INT, 
# MAGIC   FK_TERM_STATUS_CD STRING, 
# MAGIC   CFK_AUTHOR_EMPLOYEE_NO STRING, 
# MAGIC   ACCEPT_PARTNERSHIP_DT DATE, 
# MAGIC   FK_EDITION_NO INT, 
# MAGIC   FK_VERSION_NO INT, 
# MAGIC   FK_RELEASE_NO INT,
# MAGIC   FK_PREVIOUS_GDS_SRVCS_TERM_ID INT,
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   ACTION_CT STRING, 
# MAGIC   FK_TAXONOMY_GROUP_ID INT, 
# MAGIC   TM5_ACCEPT_IN STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/goods_services_term_draft'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE (
# MAGIC   FK_GOODS_SERVICES_NOTE_CD STRING, 
# MAGIC   CFK_EMPLOYEE_NO STRING, 
# MAGIC   NOTE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   NOTE_TX STRING, 
# MAGIC   FK_GOODS_SERVICES_TERM_ID INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/goods_services_term_note'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE_DRAFT (
# MAGIC   FK_GOODS_SERVICES_NOTE_CD STRING, 
# MAGIC   CFK_EMPLOYEE_NO STRING, 
# MAGIC   NOTE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   NOTE_TX STRING, 
# MAGIC   FK_GOODS_SERVICES_TERM_ID INT 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/goods_services_term_note_draft'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.INTERNATIONAL_CLASS_VERSION (
# MAGIC   FK_CLASS_ID INT, 
# MAGIC   FK_EDITION_NO INT, 
# MAGIC   FK_VERSION_NO INT, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/international_class_version'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.INTERNATIONAL_CLSFCN_EDN (
# MAGIC   EDITION_NO INT, 
# MAGIC   GENERAL_DESCRIPTION_TX STRING, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/international_clsfcn_edn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.INTL_CLSFCN_EDN_VER (
# MAGIC   FK_EDITION_NO INT, 
# MAGIC   VERSION_NO INT, 
# MAGIC   VERSION_YEAR_NO INT, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/intl_clsfcn_edn_ver'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.INTL_CLSFCN_EDN_VER_REL (
# MAGIC   FK_EDITION_NO INT, 
# MAGIC   FK_VERSION_NO INT, 
# MAGIC   RELEASE_NO INT, 
# MAGIC   SCHEDULED_PUBLISH_DT TIMESTAMP, 
# MAGIC   CFK_SCHEDULER_EMPLOYEE_NO STRING, 
# MAGIC   PUBLISHED_DT TIMESTAMP, 
# MAGIC   CFK_PUBLISHER_EMPLOYEE_NO STRING, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/intl_clsfcn_edn_ver_rel'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.MENU_ITEM (
# MAGIC   MENU_ITEM_ID INT, 
# MAGIC   LABEL_TX STRING, 
# MAGIC   MENU_ITEM_CD STRING, 
# MAGIC   URL_TX STRING, 
# MAGIC   FK_PARENT_MENU_ITEM_ID INT, 
# MAGIC   ROLE_TX STRING, 
# MAGIC   ICON_TX STRING, 
# MAGIC   DISPLAY_ORDER_NO INT, 
# MAGIC   MENU_LEVEL_NO INT, 
# MAGIC   SHORT_LABEL_TX STRING, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   DISPLAY_IN STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/menu_item'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# not does not exist in hive

# %sql
# CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.MI_ERRLOG (
#   ORA_ERR_NUMBER$ DECIMAL(20,10),
#   ORA_ERR_MESG$ STRING,
#   ORA_ERR_ROWID$ STRING,
#   ORA_ERR_OPTYP$ STRING,
#   ORA_ERR_TAG$ STRING,
#   MENU_ITEM_ID STRING,
#   LABEL_TX STRING,
#   MENU_ITEM_CD STRING,
#   URL_TX STRING,
#   FK_PARENT_MENU_ITEM_ID STRING,
#   ROLE_TX STRING,
#   ICON_TX STRING,
#   DISPLAY_ORDER_NO STRING,
#   MENU_LEVEL_NO STRING,
#   SHORT_LABEL_TX STRING,
#   CREATE_USER_ID STRING,
#   CREATE_TS STRING,
#   LAST_MOD_USER_ID STRING,
#   LAST_MOD_TS STRING,
#   DISPLAY_IN STRING 
# )
# USING DELTA
# LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/mi_errlog'
# TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_APPLICATION_MESSAGE (
# MAGIC   APPLICATION_MESSAGE_ID DECIMAL(20,0), 
# MAGIC   MESSAGE_TX STRING, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   MESSAGE_TYPE_CT STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_application_message'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_APPLICATION_PROPERTY (
# MAGIC   APPLICATION_PROPERTY_CD STRING, 
# MAGIC   VALUE_TX STRING, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_application_property'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_CLASS (
# MAGIC   CLASS_ID INT, 
# MAGIC   FK_CLASS_SCHEDULE_CD STRING, 
# MAGIC   CLASS_NO STRING, 
# MAGIC   MODIFICATION_NO INT, 
# MAGIC   TITLE_TX STRING, 
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   INTL_CLASS_SHORT_TITLE_TX STRING, 
# MAGIC   INTL_CLASS_EXPLANATORY_NOTE_TX STRING, 
# MAGIC   INTL_CLASS_INCLUSIONS_TX STRING, 
# MAGIC   INTL_CLASS_EXCLUSIONS_TX STRING, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   GOODS_SERVICES_CT STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_class'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# does not exist in hive

# %sql
# CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_CLASS_ERRLOG (
#   ORA_ERR_NUMBER$ DECIMAL(20,10),
#   ORA_ERR_MESG$ STRING,
#   ORA_ERR_ROWID$ STRING,
#   ORA_ERR_OPTYP$ STRING,
#   ORA_ERR_TAG$ STRING,
#   CLASS_ID STRING,
#   FK_CLASS_SCHEDULE_CD STRING,
#   CLASS_NO STRING,
#   MODIFICATION_NO STRING,
#   TITLE_TX STRING,
#   DESCRIPTION_TX STRING,
#   INTL_CLASS_SHORT_TITLE_TX STRING,
#   INTL_CLASS_EXPLANATORY_NOTE_TX STRING,
#   INTL_CLASS_INCLUSIONS_TX STRING,
#   INTL_CLASS_EXCLUSIONS_TX STRING,
#   BEGIN_EFFECTIVE_DT STRING,
#   END_EFFECTIVE_DT STRING,
#   CREATE_TS STRING,
#   CREATE_USER_ID STRING,
#   LAST_MOD_TS STRING,
#   LAST_MOD_USER_ID STRING,
#   GOODS_SERVICES_CT STRING
# )
# USING DELTA
# LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_class_errlog'
# TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_CLASS_SCHEDULE (
# MAGIC   CLASS_SCHEDULE_CD STRING, 
# MAGIC   TITLE_TX STRING, 
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   US_IN STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_class_schedule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_COORDINATED_CLASS (
# MAGIC   FK_CLASS_ID INT, 
# MAGIC   FK_COORDINATED_CLASS_ID INT, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_coordinated_class'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_GOODS_SERVICES_NOTE (
# MAGIC   GOODS_SERVICES_NOTE_CD STRING, 
# MAGIC   TITLE_TX STRING, 
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_goods_services_note'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_SYNONYM_GROUP (
# MAGIC   SYNONYM_GROUP_ID DECIMAL(20,0), 
# MAGIC   SYNONYM_GROUP_TX STRING, 
# MAGIC   STATUS_CT STRING, 
# MAGIC   ACTION_CT STRING, 
# MAGIC   NOTE_TX STRING, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING, 
# MAGIC   LOCK_CONTROL_NO INT 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_synonym_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_TERM_STATUS (
# MAGIC   TERM_STATUS_CD STRING, 
# MAGIC   TITLE_TX STRING, 
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_term_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.STND_US_INTL_CLS_MAPPING (
# MAGIC   FK_US_CLASS_ID INT, 
# MAGIC   FK_INTL_CLASS_ID INT, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/stnd_us_intl_cls_mapping'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.SYNC_IDM_UPDATE_LOG (
# MAGIC   INSERT_TS TIMESTAMP, 
# MAGIC   BATCH_NAME STRING, 
# MAGIC   PROCEDURE_NAME STRING, 
# MAGIC   ACTION_CD STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/sync_idm_update_log'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.TAXONOMY_GROUP (
# MAGIC   TAXONOMY_GROUP_ID INT, 
# MAGIC   FK_CLASS_ID INT, 
# MAGIC   FK_EDITION_NO INT, 
# MAGIC   FK_VERSION_NO INT, 
# MAGIC   EXTERNAL_REFERENCE_NUMBER_TX STRING, 
# MAGIC   DN_PARENT_EXTERNAL_REF_NUM_TX STRING, 
# MAGIC   FK_PARENT_TAXONOMY_GROUP_ID INT, 
# MAGIC   TITLE_TYPE_CT STRING, 
# MAGIC   TITLE_TX STRING, 
# MAGIC   LEVEL_NO INT, 
# MAGIC   SCOPE_CT STRING, 
# MAGIC   BEGIN_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   END_EFFECTIVE_DT TIMESTAMP, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/taxonomy_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.TM5_FILE (
# MAGIC   TM5_FILE_ID INT, 
# MAGIC   TM5_FILE_NM STRING, 
# MAGIC   LOAD_DT DATE, 
# MAGIC   RECORD_QT INT, 
# MAGIC   PROCESS_DT DATE, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/tm5_file'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngidmp_catalog}.bronze.TM5_GOODS_SERVICES (
# MAGIC   FK_TM5_FILE_ID INT, 
# MAGIC   TM5_GOODS_SERVICES_ID INT, 
# MAGIC   STATUS_CT INT, 
# MAGIC   DESCRIPTION_TX STRING, 
# MAGIC   CLASS_NO STRING, 
# MAGIC   APPROVAL_DT DATE, 
# MAGIC   REJECTION_DT DATE, 
# MAGIC   PROCESSING_STATUS_CT STRING, 
# MAGIC   CREATE_TS TIMESTAMP, 
# MAGIC   CREATE_USER_ID STRING, 
# MAGIC   LAST_MOD_TS TIMESTAMP, 
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngidmp_catalog}/bronze/tm5_goods_services'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

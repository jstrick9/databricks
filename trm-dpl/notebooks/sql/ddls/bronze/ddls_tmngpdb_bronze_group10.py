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
tmtmngpdb_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmtmngpdb_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmbuscalendar_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmtmngpdb_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_filings (
# MAGIC   FK_TRADEMARK_GID string,
# MAGIC   CFK_LAST_INCNG_CORR_EVENT_CD string,
# MAGIC   INCOMING_CORRESPONDENCE_IN string,
# MAGIC   PAPER_CORRESPONDENCE_RCVD_IN string,
# MAGIC   LAST_APPLICANT_RESPONSE_DT timestamp,
# MAGIC   LATEST_SUBMN_RECEIVED_DT date,
# MAGIC   LATEST_TQR_SUBMN_RECEIVED_DT date,
# MAGIC   LATEST_LIE_SUBMN_RECEIVED_DT date,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_filings'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_filing_bases (
# MAGIC   FK_TRADEMARK_GID string,
# MAGIC   FILED_WITH_FOREIGN_PRTY_DT_IN string,
# MAGIC   FILED_WITH_FRGN_REG_CERT_IN string,
# MAGIC   FILED_WITH_USE_dateS_IN string,
# MAGIC   FOREIGN_DATA_ENTERED_IN string,
# MAGIC   FOREIGN_PRIORITY_CLAIMED_IN string,
# MAGIC   FILED_WITH_SPECIMENS_IN string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_filing_bases'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_itu_extension (
# MAGIC   FK_TRADEMARK_GID string,
# MAGIC   ITU_EXTENSION_NO integer,
# MAGIC   EXPIRATION_DT timestamp,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_itu_extension'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_itu_extension_h (
# MAGIC   FK_TRADEMARK_GID string,
# MAGIC   ITU_EXTENSION_NO integer,
# MAGIC   EXPIRATION_DT timestamp,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_itu_extension_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_itu_h (
# MAGIC   FK_TRADEMARK_GID string,
# MAGIC   AMENDMENT_TO_USE_FILED_IN string,
# MAGIC   APPLICATION_MARK_IN_1 string,
# MAGIC   APPLICATION_MARK_IN_2 string,
# MAGIC   FINAL_ACTION_REFUSAL_ATU_IN string,
# MAGIC   FIRST_ACTION_REFUSAL_ATU_IN string,
# MAGIC   AVAILABLE_FOR_SOU_IN string,
# MAGIC   EXTENSIONS_NOT_ALLOWED_IN string,
# MAGIC   HOLD_FIRST_ACTION_RFSL_ATU_IN string,
# MAGIC   LAST_UA_TRAN_INFRML_RSP_RCV_IN string,
# MAGIC   LAST_UA_TRAN_INFRML_LTR_ML_IN string,
# MAGIC   ITU_CASE_PUBD_FOR_OPSTN_IN string,
# MAGIC   ITU_FREEZE_PERIOD_IN string,
# MAGIC   LATEST_ITU_FILNG_RECEIVED_DT TIMESTAMP,
# MAGIC   SOU_EXT_DENIAL_LTR_MAILED_IN string,
# MAGIC   LAST_EXT_TRAN_DNIL_LTR_PREP_IN string,
# MAGIC   LAST_POSSIBLE_EXTENSION_DT date,
# MAGIC   LAST_EXT_TRAN_SOU_EXT_FILED_IN string,
# MAGIC   USE_AFFIDAVIT_PRCSG_COMPLT_IN string,
# MAGIC   NOA_ISSUED_IN string,
# MAGIC   SOU_EXTENSION_REQ_FILED_IN string,
# MAGIC   NOA_MAILED_IN string,
# MAGIC   POTENTIEL_ABANDONMENT_DT timestamp,
# MAGIC   SOU_RECEIVED_DT TIMESTAMP,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_itu_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_og_publications (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   OG_PUBD_FOR_OPSTN_SEC_12C_DT timestamp,
# MAGIC   OG_PUBD_FOR_OPSTN_DT timestamp,
# MAGIC   OG_IN_PUBLICATION_IN string,
# MAGIC   OG_AMENDED_REGISTRATION_IN string,
# MAGIC   OG_CANCELLED_REGISTRATION_IN string,
# MAGIC   OG_CERTIFICATE_CORRECTION_IN string,
# MAGIC   OG_CERTIFICATE_OF_REG_IN string,
# MAGIC   OG_ORDER_RESTRICTING_SCOPE_IN string,
# MAGIC   OG_EXTRACT_PUBLICATION_IN string,
# MAGIC   OG_REGISTRATION_IN string,
# MAGIC   OG_RENEWAL_IN string,
# MAGIC   OG_SEC_12C_REPUBLICATION_IN string,
# MAGIC   PRINT_MARK_DESCRIPTION_IN string,
# MAGIC   REPUBLISH_SECTION_12_IN string,
# MAGIC   OG_REGISTRATION_NUM_FOUND_IN string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_og_publications'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_og_publications_h (
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   OG_PUBD_FOR_OPSTN_SEC_12C_DT TIMESTAMP,
# MAGIC   OG_PUBD_FOR_OPSTN_DT timestamp,
# MAGIC   OG_IN_PUBLICATION_IN string,
# MAGIC   OG_AMENDED_REGISTRATION_IN string,
# MAGIC   OG_CANCELLED_REGISTRATION_IN string,
# MAGIC   OG_CERTIFICATE_CORRECTION_IN string,
# MAGIC   OG_CERTIFICATE_OF_REG_IN string,
# MAGIC   OG_ORDER_RESTRICTING_SCOPE_IN string,
# MAGIC   OG_EXTRACT_PUBLICATION_IN string,
# MAGIC   OG_REGISTRATION_IN string,
# MAGIC   OG_RENEWAL_IN string,
# MAGIC   OG_SEC_12C_REPUBLICATION_IN string,
# MAGIC   PRINT_MARK_DESCRIPTION_IN string,
# MAGIC   REPUBLISH_SECTION_12_IN string,
# MAGIC   OG_REGISTRATION_NUM_FOUND_IN string,
# MAGIC   LOCK_CONTROL_NO integer,
# MAGIC   CREATE_TS timestamp,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS timestamp,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   ACTION_CT string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC   BEGIN_EFFECTIVE_TS timestamp,
# MAGIC   END_EFFECTIVE_TS timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_og_publications_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

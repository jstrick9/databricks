# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_tmngpdb.bronze.evidence_bin_folder (
# MAGIC evidence_bin_folder_id int, 
# MAGIC fk_evidence_bin_cd string, 
# MAGIC display_order_no int, 
# MAGIC folder_nm string, 
# MAGIC --fk_trademark_gid string, 
# MAGIC cfk_object_gid string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC fk_parent_evidence_bin_fldr_id int, 
# MAGIC fk_work_item_gid string,
# MAGIC dn_object_type_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks-prod-tmdc/eds/delta_tables/trm_tmngpdb/bronze/evidence_bin_folder'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_tmngpdb.bronze.evidence_bin_folder_h (
# MAGIC evidence_bin_folder_id int, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string, 
# MAGIC fk_evidence_bin_cd string, 
# MAGIC display_order_no int, 
# MAGIC folder_nm string, 
# MAGIC cfk_object_gid string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC fk_parent_evidence_bin_fldr_id int, 
# MAGIC fk_work_item_gid string,
# MAGIC dn_object_type_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks-prod-tmdc/eds/delta_tables/trm_tmngpdb/bronze/evidence_bin_folder_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_tmngpdb.bronze.tram_pd (
# MAGIC PD_ENT_NUM int, 
# MAGIC PD_EMPE_LO string, 
# MAGIC PD_EMPE_NUM int, 
# MAGIC PD_SER_NUM int, 
# MAGIC PD_TRAN_CD int, 
# MAGIC PD_OFFICE_TYPE string, 
# MAGIC PD_SUB_TYPE string, 
# MAGIC PD_START_EVENT string, 
# MAGIC PD_START_DT int, 
# MAGIC PD_START_TIME int, 
# MAGIC PD_START_FY_PP int, 
# MAGIC PD_FA_EVENT string, 
# MAGIC PD_FA_DT int, 
# MAGIC PD_FA_TIME int, 
# MAGIC PD_FA_FY_PP int, 
# MAGIC PD_END_EVENT string, 
# MAGIC PD_END_DT int, 
# MAGIC PD_END_TIME int, 
# MAGIC PD_END_FY_PP int, 
# MAGIC PD_PEND_DAYS int, 
# MAGIC PD_UPDATE_DT int, 
# MAGIC PD_UPDATE_TIME int, 
# MAGIC PD_RSN decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks-prod-tmdc/eds/delta_tables/trm_tmngpdb/bronze/tram_pd'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_tmprodvty.bronze.PRODUCTION_TRANSACTION_ERRLOG(
# MAGIC ORA_ERR_NUMBER decimal(20,0),
# MAGIC ORA_ERR_MESG string,
# MAGIC ORA_ERR_ROWID string,
# MAGIC ORA_ERR_OPTYP string,
# MAGIC ORA_ERR_TAG string,
# MAGIC PRODUCTION_CREDIT_TRAN_ID decimal(20,0),
# MAGIC CFK_OBJECT_GID string,
# MAGIC CFK_OBJECT_TYPE_CD string,
# MAGIC FK_GENERATING_PRODVTY_ACTN_ID integer,
# MAGIC FK_CORRECTED_PRODVTY_ACTN_ID integer,
# MAGIC UNIT_COUNT_NO integer,
# MAGIC TRANSACTION_EFFECTIVE_DT TIMESTAMP,
# MAGIC DN_WORKER_NO string,
# MAGIC DN_WORKER_TM_ORGANIZATION_CD string,
# MAGIC DN_WORKER_ROLE_CD string,
# MAGIC CFK_WORKER_GID string,
# MAGIC CFK_WORKER_TM_ORGANIZATION_GID string,
# MAGIC CFK_WORKER_USER_ROLE_ID integer,
# MAGIC DN_CONTRIBUTOR_WORKER_NO string,
# MAGIC DN_CONTRIBUTOR_WORKER_ROLE_CD string,
# MAGIC DN_CONTRIBUTOR_TM_ORG_CD string,
# MAGIC CFK_CONTRIBUTOR_WORKER_GID string,
# MAGIC CFK_CONTRIBUTOR_USER_ROLE_ID integer,
# MAGIC CFK_CONTRIBUTOR_TM_ORG_GID string,
# MAGIC CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC DN_ACTION_NO string,
# MAGIC PRIORITY_IN string,
# MAGIC TRANSACTION_CT string,
# MAGIC WORK_UNIT_CD string,
# MAGIC LOCK_CONTROL_NO integer,
# MAGIC CREATE_TS timestamp,
# MAGIC CREATE_USER_ID string,
# MAGIC LAST_MOD_TS timestamp,
# MAGIC LAST_MOD_USER_ID string,
# MAGIC SUBSEQUENT_ACTION_IN string,
# MAGIC DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://databricks-prod-tmdc/eds/delta_tables/trm_tmprodvty/bronze/PRODUCTION_TRANSACTION_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_tmreviews.bronze.POST_REG_REVIEW_NOTICE_ERRLOG (
# MAGIC   ORA_ERR_NUMBER decimal(20,0),
# MAGIC   ORA_ERR_MESG string,
# MAGIC   ORA_ERR_ROWID string,
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
# MAGIC location 's3://databricks-prod-tmdc/eds/delta_tables/trm_tmreviews/bronze/POST_REG_REVIEW_NOTICE_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_tmreviews.bronze.PREG_QUALITY_REVIEW_ELEMENT_ERRLOG (
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
# MAGIC location 's3://databricks-prod-tmdc/eds/delta_tables/trm_tmreviews/bronze/PREG_QUALITY_REVIEW_ELEMENT_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trm_jbteasps.bronze.stnd_source_system(
# MAGIC source_system_id    int,
# MAGIC short_nm            string,
# MAGIC full_nm             string,
# MAGIC description_tx      string,
# MAGIC begin_effective_dt  timestamp,
# MAGIC end_effective_dt    timestamp,
# MAGIC create_ts           timestamp,
# MAGIC create_user_id      string,
# MAGIC last_mod_ts         timestamp,
# MAGIC last_mod_user_id    string,
# MAGIC display_nm string)
# MAGIC USING delta
# MAGIC location 's3://databricks-prod-tmdc/eds/delta_tables/trm_jbteasps/bronze/STND_SOURCE_SYSTEM'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------



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
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}';
# MAGIC --CREATE CATALOG IF NOT EXISTS ${config.data_quality_catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}}/delta_tables/${config.data_quality_catalog}';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tmcom_batch_ingest_control (
# MAGIC SERIAL_NUM	int,
# MAGIC BATCH_NM	string,
# MAGIC TARGET_ENDPOINT	string,
# MAGIC ENDPOINT_TYPE	string,
# MAGIC TARGET_ERROR_CODE	int,
# MAGIC TARGET_ERROR_MSG	string,
# MAGIC COMPLETED_TS	TIMESTAMP,
# MAGIC STATUS_CT	string,
# MAGIC CREATE_USER_ID	string,
# MAGIC CREATE_TS	TIMESTAMP,
# MAGIC LAST_MOD_USER_ID	string,
# MAGIC LAST_MOD_TS	TIMESTAMP,
# MAGIC BATCH_DT_NO	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tmcom_batch_ingest_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.TEMP_TRM_PQ (
# MAGIC PQ_SER_NUM		int,
# MAGIC PQ_ENT_NUM		int,
# MAGIC PQ_EMPE_NUM		int,
# MAGIC PQ_QUEUE_DT		int,
# MAGIC PQ_QUEUE_TI		int,
# MAGIC PQ_ASGN_EMPE		int,
# MAGIC PQ_QUEUE		string,
# MAGIC PQ_FLG_CUR		int,
# MAGIC PQ_DOC_TYPE		string,
# MAGIC PQ_CMP_TYPE		string,
# MAGIC PQ_DOC_RCVD_DT		int,
# MAGIC PQ_LOP_REASON		int,
# MAGIC PQ_LOP_LO_ASGN		string,
# MAGIC PQ_RSN		decimal(22,0),
# MAGIC DELETE_IN		string,
# MAGIC LOCK_CONTROL_NO		int,
# MAGIC CREATE_TS		timestamp, 
# MAGIC CREATE_USER_ID		string,
# MAGIC LAST_MOD_TS		timestamp, 
# MAGIC LAST_MOD_USER_ID		string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/temp_trm_pq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.TEMP_TRM_PQ_H (
# MAGIC PQ_SER_NUM		int,
# MAGIC PQ_ENT_NUM		int,
# MAGIC PQ_EMPE_NUM		int,
# MAGIC PQ_QUEUE_DT		int,
# MAGIC PQ_QUEUE_TI		int,
# MAGIC PQ_ASGN_EMPE		int,
# MAGIC PQ_QUEUE		string,
# MAGIC PQ_FLG_CUR		int,
# MAGIC PQ_DOC_TYPE		string,
# MAGIC PQ_CMP_TYPE		string,
# MAGIC PQ_DOC_RCVD_DT		int,
# MAGIC PQ_LOP_REASON		int,
# MAGIC PQ_LOP_LO_ASGN		string,
# MAGIC PQ_RSN		decimal(22,0),
# MAGIC DELETE_IN		string,
# MAGIC LOCK_CONTROL_NO		int,
# MAGIC CREATE_TS		timestamp,
# MAGIC CREATE_USER_ID		string,
# MAGIC LAST_MOD_TS		timestamp,
# MAGIC LAST_MOD_USER_ID		string,
# MAGIC ACTION_CT		string,
# MAGIC CFK_TRANSACTION_INSTANCE_GID		string,
# MAGIC BEGIN_EFFECTIVE_TS		timestamp,
# MAGIC END_EFFECTIVE_TS		timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/temp_trm_pq_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.TEMP_TRM_PQC (
# MAGIC PQC_RSN		decimal(22,0),
# MAGIC PQC_CTL_NUM		string,
# MAGIC PQC_ENT_NUM		int,
# MAGIC PQC_EMPE_NUM		int,
# MAGIC PQC_QUEUE_DT		int,
# MAGIC PQC_QUEUE_TI		int,
# MAGIC PQC_ASGN_EMPE		int,
# MAGIC PQC_QUEUE		string,
# MAGIC PQC_FLG_CUR		int,
# MAGIC PQC_DOC_TYPE		string,
# MAGIC PQC_CMP_TYPE		string,
# MAGIC PQC_DOC_RCVD_DT		int,
# MAGIC PQC_LOP_REASON		int,
# MAGIC PQC_LOP_LO_ASGN		string,
# MAGIC DELETE_IN		string,
# MAGIC LOCK_CONTROL_NO		int,
# MAGIC CREATE_TS		timestamp,
# MAGIC CREATE_USER_ID		string,
# MAGIC LAST_MOD_TS		timestamp,
# MAGIC LAST_MOD_USER_ID		string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/temp_trm_pqc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.TEMP_TRM_PQC_H (
# MAGIC PQC_RSN		decimal(22,0),
# MAGIC PQC_CTL_NUM		string,
# MAGIC PQC_ENT_NUM		int,
# MAGIC PQC_EMPE_NUM		int,
# MAGIC PQC_QUEUE_DT		int,
# MAGIC PQC_QUEUE_TI		int,
# MAGIC PQC_ASGN_EMPE		int,
# MAGIC PQC_QUEUE		string,
# MAGIC PQC_FLG_CUR		int,
# MAGIC PQC_DOC_TYPE		string,
# MAGIC PQC_CMP_TYPE		string,
# MAGIC PQC_DOC_RCVD_DT		int,
# MAGIC PQC_LOP_REASON		int,
# MAGIC PQC_LOP_LO_ASGN		string,
# MAGIC DELETE_IN		string,
# MAGIC LOCK_CONTROL_NO		int,
# MAGIC CREATE_TS		timestamp,
# MAGIC CREATE_USER_ID		string,
# MAGIC LAST_MOD_TS		timestamp,
# MAGIC LAST_MOD_USER_ID		string,
# MAGIC ACTION_CT		string,
# MAGIC CFK_TRANSACTION_INSTANCE_GID		string,
# MAGIC BEGIN_EFFECTIVE_TS		timestamp,
# MAGIC END_EFFECTIVE_TS		timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/temp_trm_pqc_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.tm_office_actions (
# MAGIC fk_trademark_gid                 string,
# MAGIC capture_scnd_ea_actn_cntd_in     string,
# MAGIC examining_atty_dspl_cnt_in   string,
# MAGIC final_refusal_in                 string,
# MAGIC first_action_mailed_in           string,
# MAGIC first_action_publication_in      string,
# MAGIC first_ea_action_counted_dt       timestamp,
# MAGIC first_ea_action_counted_in       string,
# MAGIC first_para_action_counted_in     string,
# MAGIC FRST_PR_PRLGL_ACTN_CNTED_DT  timestamp,
# MAGIC hld_exmg_atty_dspl_cnt_in        string,
# MAGIC hld_frst_exmg_at_actn_cnt_in   string,
# MAGIC last_examiner_action_dt          timestamp,
# MAGIC second_ea_action_counted_in      string,
# MAGIC total_paralegal_actions_no       int,
# MAGIC total_examiner_actions_no        int,
# MAGIC lock_control_no                  int,
# MAGIC create_ts                        timestamp,
# MAGIC create_user_id                   string,
# MAGIC last_mod_ts                      timestamp,
# MAGIC last_mod_user_id                  string,
# MAGIC frst_pr_paralegal_actn_cnted_dt timestamp,
# MAGIC examining_attorney_dspl_cnt_in string,
# MAGIC hld_frst_exmg_atty_actn_cnt_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm_office_actions'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_states (
# MAGIC FK_TRADEMARK_GID	string,
# MAGIC AMENDED_TM_APPLICATION_IN	string,
# MAGIC CHILD_APPLICATION_IN	string,
# MAGIC PARENT_APPLICATION_IN	string,
# MAGIC ASSIGNMENT_RECORDED_IN	string,
# MAGIC COMPLETE_CASE_IN_TICRS_IN	string,
# MAGIC CONCURRENT_USE_IN	string,
# MAGIC CNCR_USE_PEND_TTAB_PRCDNG_IN	string,
# MAGIC CONCURRENT_USE_PUBLISHED_IN	string,
# MAGIC INACTIVE_IN	string,
# MAGIC INTF_PENDING_TTAB_PRCDNG_IN	string,
# MAGIC INTERFERENCE_PUBLISHED_IN	string,
# MAGIC INTERNAL_NOTE_IN	string,
# MAGIC MISCELLANEOUS_1_IN	string,
# MAGIC NEW_TM_CASE_ADDED_IN	string,
# MAGIC OPPOSITION_PERIOD_ENDED_DT	date,
# MAGIC REGISTER_AMENDED_PRINCIPAL_IN	string,
# MAGIC REGISTER_AMENDED_SUPL_IN	string,
# MAGIC REGISTRATION_AMENDED_IN	string,
# MAGIC SERIAL_NUMBER_VERIFIED_IN	string,
# MAGIC IN_PUBLICATION_IN	string,
# MAGIC TTAB_ORAL_HEARING_REQUESTED_IN	string,
# MAGIC NO_ACED_IN	string,
# MAGIC OPPOSITION_PEND_TTAB_PRCDNG_IN	string,
# MAGIC EXPARTE_APPEAL_DECISION_IN	string,
# MAGIC LATEST_SUSPENSION_CHECK_DT	date,
# MAGIC REFUSAL_APPEALED_TO_TTAB_IN	string,
# MAGIC LOP_RECEIVED_IN	string,
# MAGIC ACTIVE_PETITION_IN	string,
# MAGIC UNANSWERED_PETITION_IN	string,
# MAGIC LOCK_CONTROL_NO	int,
# MAGIC CREATE_TS	timestamp,
# MAGIC CREATE_USER_ID	string,
# MAGIC LAST_MOD_TS	timestamp,
# MAGIC LAST_MOD_USER_ID	string,
# MAGIC CONCURRENT_USE_STATUS_CT	string,
# MAGIC NOT_ELECTRONIC_IN	string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm_states'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_rt (
# MAGIC rt_ctr_1 int,
# MAGIC rt_ctr_2 decimal(22,0),
# MAGIC rt_dtl_id string,
# MAGIC rt_rpt_id string,
# MAGIC rt_rpt_dt int,
# MAGIC rt_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tram_rt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tqr (
# MAGIC tqr_ser_num  int,
# MAGIC tqr_reg_num  int,
# MAGIC tqr_dt_period  int,
# MAGIC tqr_queue_type  string,
# MAGIC tqr_queue  string,
# MAGIC tqr_tran_cd  int,
# MAGIC tqr_dt_tran  int,
# MAGIC tqr_cm_cd_type  string,
# MAGIC tqr_empe_num  int,
# MAGIC tqr_asgn_empe_num  int,
# MAGIC tqr_fy_pp  int,
# MAGIC tqr_dt_asgn  int,
# MAGIC tqr_dt_cmpltd  int,
# MAGIC tqr_qpa_done_cnt  int,
# MAGIC tqr_qpa_done_1  int,
# MAGIC tqr_qpa_done_2  int,
# MAGIC tqr_qpa_done_3  int,
# MAGIC tqr_qpa_done_4  int,
# MAGIC tqr_qpa_done_5  int,
# MAGIC tqr_qpa_done_6  int,
# MAGIC tqr_qpa_done_7  int,
# MAGIC tqr_qpa_done_8  int,
# MAGIC tqr_qpa_done_9  int,
# MAGIC tqr_qpa_done_10  int,
# MAGIC tqr_qpa_done_11  int,
# MAGIC tqr_qpa_done_12  int,
# MAGIC tqr_qpa_done_13  int,
# MAGIC tqr_qpa_done_14  int,
# MAGIC tqr_qpa_done_15  int,
# MAGIC tqr_qpa_done_16  int,
# MAGIC tqr_qpa_done_17  int,
# MAGIC tqr_qpa_done_18  int,
# MAGIC tqr_qpa_done_19  int,
# MAGIC tqr_qpa_done_20  int,
# MAGIC tqr_qpa_done_21  int,
# MAGIC tqr_qpa_done_22  int,
# MAGIC tqr_qpa_done_23  int,
# MAGIC tqr_qpa_done_24  int,
# MAGIC tqr_qpa_done_25  int,
# MAGIC tqr_qpa_done_26  int,
# MAGIC tqr_qpa_done_27  int,
# MAGIC tqr_qpa_done_28  int,
# MAGIC tqr_qpa_done_29  int,
# MAGIC tqr_qpa_done_30  int,
# MAGIC tqr_qpa_done_31  int,
# MAGIC tqr_qpa_done_32  int,
# MAGIC tqr_qpa_done_33  int,
# MAGIC tqr_qpa_done_34  int,
# MAGIC tqr_qpa_done_35  int,
# MAGIC tqr_qpa_done_36  int,
# MAGIC tqr_qpa_done_37  int,
# MAGIC tqr_qpa_done_38  int,
# MAGIC tqr_qpa_done_39  int,
# MAGIC tqr_qpa_done_40  int,
# MAGIC tqr_qpa_done_41  int,
# MAGIC tqr_qpa_done_42  int,
# MAGIC tqr_qpa_done_43  int,
# MAGIC tqr_qpa_done_44  int,
# MAGIC tqr_qpa_done_45  int,
# MAGIC tqr_qpa_done_46  int,
# MAGIC tqr_qpa_done_47  int,
# MAGIC tqr_qpa_done_48  int,
# MAGIC tqr_qpa_done_49  int,
# MAGIC tqr_qpa_done_50  int,
# MAGIC tqr_qpa_dt_cnt  int,
# MAGIC tqr_qpa_dt_1  int,
# MAGIC tqr_qpa_dt_2  int,
# MAGIC tqr_qpa_dt_3  int,
# MAGIC tqr_qpa_dt_4  int,
# MAGIC tqr_qpa_dt_5  int,
# MAGIC tqr_qpa_dt_6  int,
# MAGIC tqr_qpa_dt_7  int,
# MAGIC tqr_qpa_dt_8  int,
# MAGIC tqr_qpa_dt_9  int,
# MAGIC tqr_qpa_dt_10  int,
# MAGIC tqr_qpa_dt_11  int,
# MAGIC tqr_qpa_dt_12  int,
# MAGIC tqr_qpa_dt_13  int,
# MAGIC tqr_qpa_dt_14  int,
# MAGIC tqr_qpa_dt_15  int,
# MAGIC tqr_qpa_dt_16  int,
# MAGIC tqr_qpa_dt_17  int,
# MAGIC tqr_qpa_dt_18  int,
# MAGIC tqr_qpa_dt_19  int,
# MAGIC tqr_qpa_dt_20  int,
# MAGIC tqr_qpa_dt_21  int,
# MAGIC tqr_qpa_dt_22  int,
# MAGIC tqr_qpa_dt_23  int,
# MAGIC tqr_qpa_dt_24  int,
# MAGIC tqr_qpa_dt_25  int,
# MAGIC tqr_qpa_dt_26  int,
# MAGIC tqr_qpa_dt_27  int,
# MAGIC tqr_qpa_dt_28  int,
# MAGIC tqr_qpa_dt_29  int,
# MAGIC tqr_qpa_dt_30  int,
# MAGIC tqr_qpa_dt_31  int,
# MAGIC tqr_qpa_dt_32  int,
# MAGIC tqr_qpa_dt_33  int,
# MAGIC tqr_qpa_dt_34  int,
# MAGIC tqr_qpa_dt_35  int,
# MAGIC tqr_qpa_dt_36  int,
# MAGIC tqr_qpa_dt_37  int,
# MAGIC tqr_qpa_dt_38  int,
# MAGIC tqr_qpa_dt_39  int,
# MAGIC tqr_qpa_dt_40  int,
# MAGIC tqr_qpa_dt_41  int,
# MAGIC tqr_qpa_dt_42  int,
# MAGIC tqr_qpa_dt_43  int,
# MAGIC tqr_qpa_dt_44  int,
# MAGIC tqr_qpa_dt_45  int,
# MAGIC tqr_qpa_dt_46  int,
# MAGIC tqr_qpa_dt_47  int,
# MAGIC tqr_qpa_dt_48  int,
# MAGIC tqr_qpa_dt_49  int,
# MAGIC tqr_qpa_dt_50  int,
# MAGIC tqr_cm_ent_num  int,
# MAGIC tqr_cm_stat  string,
# MAGIC tqr_tran_actn_num  int,
# MAGIC tqr_tran_stat  string,
# MAGIC tqr_dt_select  int,
# MAGIC tqr_dt_create  int,
# MAGIC tqr_dt_export  int,
# MAGIC tqr_tran_ind  int,
# MAGIC tqr_sub_tran_cd  int,
# MAGIC tqr_random_num  int,
# MAGIC tqr_rview_type  string,
# MAGIC tqr_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tram_tqr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_upd (
# MAGIC upd_ser_num  int,
# MAGIC upd_updt_dt  int,
# MAGIC upd_updt_ti  int,
# MAGIC upd_prog_id  string,
# MAGIC upd_tran_cd  string,
# MAGIC upd_ent_num  int,
# MAGIC upd_set_array string,
# MAGIC upd_rsn  decimal(22,0),
# MAGIC upd_client_id decimal(22,0),
# MAGIC upd_terminal_id  string,
# MAGIC upd_msg_data	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_upd'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %md
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_appeals (
# MAGIC CFK_TRADEMARK_GID	string,
# MAGIC EXPARTE_APPEAL_DECISION_IN	string,
# MAGIC CONCURRENT_USE_IN	string,
# MAGIC CNCL_PENDING_TTAB_PRCDNG_IN	string,
# MAGIC CNCR_USE_PEND_TTAB_PRCDNG_IN	string,
# MAGIC INTF_PENDING_TTAB_PRCDNG_IN	string,
# MAGIC INTERFERENCE_PUBLISHED_IN	string,
# MAGIC OPPOSITION_PEND_TTAB_PRCDNG_IN	string,
# MAGIC REFUSAL_APPEALED_TO_TTAB_IN	string,
# MAGIC TTAB_MISPLACED_APPL_REQ_IN	string,
# MAGIC TTAB_ORAL_HEARING_REQUESTED_IN	string,
# MAGIC LOCK_CONTROL_NO	INT,
# MAGIC CREATE_TS	TIMESTAMP,
# MAGIC CREATE_USER_ID	string,
# MAGIC LAST_MOD_TS	TIMESTAMP,
# MAGIC LAST_MOD_USER_ID	string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm_appeals'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_cmn

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group1

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group2

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group3

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group4

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group5

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group6

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group7

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group8

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group9

# COMMAND ----------

#%run ./ddls_tmngpdb_bronze_group10

# COMMAND ----------

dbutils.notebook.exit(f"Completed executing ddls. ")

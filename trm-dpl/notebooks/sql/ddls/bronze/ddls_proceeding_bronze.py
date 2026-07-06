# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create proceeding bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/proceeding-conf.yaml"
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
proceeding_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{proceeding_catalog=}, {data_quality_catalog=} ')
src_folder = common_configs['cdc']['src_csv_files']
src_database = common_configs['cdc']['src_database']
spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
spark.conf.set('config.proceeding_catalog', proceeding_catalog.lower()) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
catalog = proceeding_catalog
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', proceeding_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.proceeding_catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PETITION(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_petition_type_cd string, 
# MAGIC   per_proceeding_year_no int, 
# MAGIC   per_proceeding_no int, 
# MAGIC   cfk_expunge_reexam_type_cd string, 
# MAGIC   proceeding_instituted_dt timestamp, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_ptd_reason_cd string, 
# MAGIC   ptd_other_reason_explntn_tx string, 
# MAGIC   cfk_ptd_prir_ppr_submn_type_cd string, 
# MAGIC   cfk_ptr_reason_cd string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PETITION'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PETITION_H(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_petition_type_cd string, 
# MAGIC   per_proceeding_year_no int, 
# MAGIC   per_proceeding_no int, 
# MAGIC   cfk_expunge_reexam_type_cd string, 
# MAGIC   proceeding_instituted_dt timestamp, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   cfk_ptd_reason_cd string, 
# MAGIC   ptd_other_reason_explntn_tx string, 
# MAGIC   cfk_ptd_prir_ppr_submn_type_cd string, 
# MAGIC   cfk_ptr_reason_cd string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PETITION_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PETITION_RESPONSE(
# MAGIC   cfk_response_type_cd string, 
# MAGIC   fk_proceeding_gid string, 
# MAGIC   response_received_dt timestamp, 
# MAGIC   response_statement_tx string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PETITION_RESPONSE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PETITION_RESPONSE_DOCUMENT(
# MAGIC   response_received_dt timestamp, 
# MAGIC   sequence_no int, 
# MAGIC   cfk_document_id string, 
# MAGIC   fk_proceeding_gid string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PETITION_RESPONSE_DOCUMENT'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PETITION_RESPONSE_DOCUMENT_H(
# MAGIC   response_received_dt timestamp, 
# MAGIC   sequence_no int, 
# MAGIC   cfk_document_id string, 
# MAGIC   fk_proceeding_gid string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PETITION_RESPONSE_DOCUMENT_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PETITION_RESPONSE_H(
# MAGIC   cfk_response_type_cd string, 
# MAGIC   fk_proceeding_gid string, 
# MAGIC   response_received_dt timestamp, 
# MAGIC   response_statement_tx string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PETITION_RESPONSE_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PRCDNG_TRIGGER_EXCEPTIONS(
# MAGIC   insert_ts timestamp, 
# MAGIC   error_num int, 
# MAGIC   error_msg string, 
# MAGIC   backtrace string, 
# MAGIC   callstack string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PRCDNG_TRIGGER_EXCEPTIONS'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING(
# MAGIC   proceeding_gid string, 
# MAGIC   proceeding_no string, 
# MAGIC   cfk_proceeding_type_cd string, 
# MAGIC   filing_dt timestamp, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   director_initiated_in string, 
# MAGIC   received_dt timestamp, 
# MAGIC   cfk_disposition_granted_rsn_cd string, 
# MAGIC   current_in string, 
# MAGIC   dn_current_state_cd string, 
# MAGIC   dn_current_state_dt timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_CLASS(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_class_id int, 
# MAGIC   dn_class_no string, 
# MAGIC   included_gds_srvcs_tx string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_CLASS'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_CLASS_H(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_class_id int, 
# MAGIC   dn_class_no string, 
# MAGIC   included_gds_srvcs_tx string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_CLASS_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_DOCUMENT(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_prcdng_document_type_cd string, 
# MAGIC   cfk_document_id string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_DOCUMENT'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_DOCUMENT_H(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_prcdng_document_type_cd string, 
# MAGIC   cfk_document_id string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_DOCUMENT_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_EVENT(
# MAGIC   proceeding_event_id decimal(38,10), 
# MAGIC   fk_proceeding_gid string, 
# MAGIC   fk_proceeding_tran_instnc_gid string, 
# MAGIC   effective_ts timestamp, 
# MAGIC   cfk_fsm_instance_h_id decimal, 
# MAGIC   fk_prcdng_event_reason_id decimal(38,10), 
# MAGIC   order_no decimal, 
# MAGIC   document_id string, 
# MAGIC   paper_in string, 
# MAGIC   lock_control_no decimal, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_EVENT'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_EVENT_REASON(
# MAGIC   proceeding_event_reason_id DECIMAL, 
# MAGIC   proceeding_event_reason_cd string, 
# MAGIC   title_tx string, 
# MAGIC   description_tx string, 
# MAGIC   cfk_fsm_type_event_id int, 
# MAGIC   prosecution_history_in string, 
# MAGIC   alert_trigger_ct string, 
# MAGIC   begin_effective_dt timestamp, 
# MAGIC   end_effective_dt timestamp, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_EVENT_REASON'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_FEE(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   fee_type_cd string, 
# MAGIC   fee_item_count_no int, 
# MAGIC   fee_am decimal, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_FEE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_FEE_H(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   fee_type_cd string, 
# MAGIC   fee_item_count_no int, 
# MAGIC   fee_am decimal, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_FEE_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_H(
# MAGIC   proceeding_gid string, 
# MAGIC   proceeding_no string, 
# MAGIC   cfk_proceeding_type_cd string, 
# MAGIC   filing_dt timestamp, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   director_initiated_in string, 
# MAGIC   received_dt timestamp, 
# MAGIC   cfk_disposition_granted_rsn_cd string, 
# MAGIC   current_in string, 
# MAGIC   dn_current_state_cd string, 
# MAGIC   dn_current_state_dt timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_MARK(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_trademark_gid string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   sequence_no int)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_MARK'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_MARK_H(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_trademark_gid string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   sequence_no int)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_MARK_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_PARTICIPANT(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_interested_party_gid string, 
# MAGIC   cfk_proceeding_prtcpnt_role_cd string, 
# MAGIC   sequence_no int, 
# MAGIC   docket_reference_no string, 
# MAGIC   cfk_at_bar_membership_state_cd string, 
# MAGIC   at_other_appointed_attys_tx string, 
# MAGIC   dn_at_bar_membership_state_nm string, 
# MAGIC   at_bar_membership_month_no int, 
# MAGIC   at_bar_membership_day_no string, 
# MAGIC   at_bar_membership_year_no int, 
# MAGIC   at_attorney_bar_no string, 
# MAGIC   at_bar_membership_assc_dt timestamp, 
# MAGIC   at_bar_jurisdiction_tx string, 
# MAGIC   at_canadian_registered_oed_nm string, 
# MAGIC   at_certify_in string, 
# MAGIC   at_member_in_good_standing_in string, 
# MAGIC   at_attorney_affiliation_ct string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_PARTICIPANT'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_PARTICIPANT_H(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_interested_party_gid string, 
# MAGIC   cfk_proceeding_prtcpnt_role_cd string, 
# MAGIC   sequence_no int, 
# MAGIC   docket_reference_no string, 
# MAGIC   cfk_at_bar_membership_state_cd string, 
# MAGIC   at_other_appointed_attys_tx string, 
# MAGIC   dn_at_bar_membership_state_nm string, 
# MAGIC   at_bar_membership_month_no int, 
# MAGIC   at_bar_membership_day_no string, 
# MAGIC   at_bar_membership_year_no int, 
# MAGIC   at_attorney_bar_no string, 
# MAGIC   at_bar_membership_assc_dt timestamp, 
# MAGIC   at_bar_jurisdiction_tx string, 
# MAGIC   at_canadian_registered_oed_nm string, 
# MAGIC   at_certify_in string, 
# MAGIC   at_member_in_good_standing_in string, 
# MAGIC   at_attorney_affiliation_ct string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_PARTICIPANT_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_STATEMENT(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_statement_type_cd string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   statement_tx string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_STATEMENT'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_STATEMENT_H(
# MAGIC   fk_proceeding_gid string, 
# MAGIC   cfk_statement_type_cd string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   statement_tx string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_STATEMENT_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_TRAN_INSTANCE(
# MAGIC   proceeding_tran_instnc_gid string, 
# MAGIC   transaction_instance_id string, 
# MAGIC   cfk_employee_no string, 
# MAGIC   effective_ts timestamp, 
# MAGIC   details_tx string, 
# MAGIC   terminated_in string, 
# MAGIC   origin_location_tx string, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_TRAN_INSTANCE'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.SYNC_TM_COM_EXCEPTION(
# MAGIC   tm_com_exception_id decimal(38,10), 
# MAGIC   insert_ts timestamp, 
# MAGIC   source_ip string, 
# MAGIC   tm_com_service_nm string, 
# MAGIC   endpoint_url string, 
# MAGIC   endpoint_type_cd string, 
# MAGIC   endpoint_body string, 
# MAGIC   http_error_cd string, 
# MAGIC   http_error_msg string, 
# MAGIC   retry_ind string, 
# MAGIC   resolved_ts timestamp,
# MAGIC   ref_no string )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/SYNC_TM_COM_EXCEPTION'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.LETTER_OF_PROTEST (
# MAGIC   FK_PROCEEDING_GID string  ,
# MAGIC   LOCK_CONTROL_NO int     ,
# MAGIC   CREATE_TS TIMESTAMP  ,
# MAGIC   CREATE_USER_ID string  ,
# MAGIC   LAST_MOD_TS TIMESTAMP   ,
# MAGIC   LAST_MOD_USER_ID string  
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/LETTER_OF_PROTEST'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.LETTER_OF_PROTEST_H (
# MAGIC   FK_PROCEEDING_GID string  ,
# MAGIC   LOCK_CONTROL_NO int     ,
# MAGIC   CREATE_TS TIMESTAMP  ,
# MAGIC   CREATE_USER_ID string  ,
# MAGIC   LAST_MOD_TS TIMESTAMP   ,
# MAGIC   LAST_MOD_USER_ID string  ,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID  string  ,
# MAGIC   ACTION_CT  string  ,
# MAGIC   BEGIN_EFFECTIVE_TS TIMESTAMP  ,
# MAGIC   END_EFFECTIVE_TS TIMESTAMP  
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/LETTER_OF_PROTEST_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.LOP_LEGAL_BASIS (
# MAGIC   FK_PROCEEDING_GID string,
# MAGIC   CFK_LEGAL_BASIS_TYPE_CD string,
# MAGIC   OTHER_LEGAL_BASIS_TX string,
# MAGIC   LOCK_CONTROL_NO int,
# MAGIC   CREATE_TS TIMESTAMP ,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS TIMESTAMP ,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/LOP_LEGAL_BASIS'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.LOP_LEGAL_BASIS_H (
# MAGIC   FK_PROCEEDING_GID string,
# MAGIC   CFK_LEGAL_BASIS_TYPE_CD string,
# MAGIC   OTHER_LEGAL_BASIS_TX string,
# MAGIC   LOCK_CONTROL_NO int,
# MAGIC   CREATE_TS TIMESTAMP ,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS TIMESTAMP ,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID  string  ,
# MAGIC   ACTION_CT  string  ,
# MAGIC   BEGIN_EFFECTIVE_TS TIMESTAMP  ,
# MAGIC   END_EFFECTIVE_TS TIMESTAMP    
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/LOP_LEGAL_BASIS_H'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.LOP_LEGAL_BASIS_TRADEMARK (
# MAGIC   FK_PROCEEDING_GID string,
# MAGIC   LOP_LEGAL_BASIS_TRADEMARK_ID decimal(38,10),
# MAGIC   CFK_LEGAL_BASIS_TYPE_CD string,
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   DN_SERIAL_NUM string,
# MAGIC   DN_REGISTRATION_NUM decimal,
# MAGIC   LOCK_CONTROL_NO decimal,
# MAGIC   CREATE_TS TIMESTAMP,
# MAGIC   CREATE_USER_ID string,
# MAGIC   LAST_MOD_TS TIMESTAMP,
# MAGIC   LAST_MOD_USER_ID string
# MAGIC ) USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/LOP_LEGAL_BASIS_TRADEMARK' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.LOP_LEGAL_BASIS_TRADEMARK_H (
# MAGIC   LOP_LEGAL_BASIS_TRADEMARK_ID int ,
# MAGIC   CFK_TRANSACTION_INSTANCE_GID string ,
# MAGIC   ACTION_CT  string ,
# MAGIC   FK_PROCEEDING_GID string ,
# MAGIC   CFK_LEGAL_BASIS_TYPE_CD string ,
# MAGIC   CFK_TRADEMARK_GID string,
# MAGIC   DN_SERIAL_NUM string,
# MAGIC   DN_REGISTRATION_NUM int,
# MAGIC   LOCK_CONTROL_NO int  ,
# MAGIC   CREATE_TS TIMESTAMP  ,
# MAGIC   CREATE_USER_ID string ,
# MAGIC   LAST_MOD_TS TIMESTAMP  ,
# MAGIC   LAST_MOD_USER_ID string,
# MAGIC   BEGIN_EFFECTIVE_TS TIMESTAMP  ,
# MAGIC   END_EFFECTIVE_TS TIMESTAMP 
# MAGIC ) USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/LOP_LEGAL_BASIS_TRADEMARK_H' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_INTL_APPL (FK_PROCEEDING_GID string , CFK_INTERNATIONAL_APPL_GID string ,
# MAGIC  LOCK_CONTROL_NO int  , CREATE_TS TIMESTAMP  , CREATE_USER_ID string ,
# MAGIC   LAST_MOD_TS TIMESTAMP  , LAST_MOD_USER_ID string  ) 
# MAGIC   USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_INTL_APPL' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.PROCEEDING_INTL_APPL_H (FK_PROCEEDING_GID string , CFK_INTERNATIONAL_APPL_GID string ,
# MAGIC LOCK_CONTROL_NO int   , CREATE_TS TIMESTAMP  , CREATE_USER_ID string ,
# MAGIC LAST_MOD_TS TIMESTAMP  , LAST_MOD_USER_ID string , CFK_TRANSACTION_INSTANCE_GID string ,
# MAGIC  ACTION_CT string , BEGIN_EFFECTIVE_TS TIMESTAMP  , END_EFFECTIVE_TS TIMESTAMP  )
# MAGIC  USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/PROCEEDING_INTL_APPL_H' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.STND_LOP_LEGAL_BASIS (LOP_LEGAL_BASIS_CD string , TITLE_TX string , DESCRIPTION_TX string , 
# MAGIC BEGIN_EFFECTIVE_DT TIMESTAMP , END_EFFECTIVE_DT TIMESTAMP, LOCK_CONTROL_NO int   , 
# MAGIC CREATE_TS TIMESTAMP  , CREATE_USER_ID string , LAST_MOD_TS TIMESTAMP  , LAST_MOD_USER_ID string  )USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/STND_LOP_LEGAL_BASIS' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.proceeding_catalog}.${conf.database}.STND_PETITION_TO_DIRECTOR
# MAGIC  (PETITION_TO_DIRECTOR_CD string , TITLE_TX string , 
# MAGIC  DESCRIPTION_TX string , BEGIN_EFFECTIVE_DT TIMESTAMP , END_EFFECTIVE_DT TIMESTAMP, LOCK_CONTROL_NO int    ,
# MAGIC   CREATE_TS TIMESTAMP  , CREATE_USER_ID string , LAST_MOD_TS TIMESTAMP  , LAST_MOD_USER_ID string  
# MAGIC  )USING delta location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/STND_PETITION_TO_DIRECTOR' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${config.proceeding_catalog}.${conf.database}.${conf.control_table};
# MAGIC
# MAGIC create table if not exists ${config.proceeding_catalog}.${conf.database}.${conf.control_table} (
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
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/${conf.control_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

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
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmproceeding/bronze/${conf.job_history_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

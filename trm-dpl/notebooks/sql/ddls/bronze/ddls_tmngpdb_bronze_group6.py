# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_divisional_status (
# MAGIC tm_divisional_status_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_divisional_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_employee_asgmt_role (
# MAGIC tm_employee_role_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_employee_asgmt_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_group_type (
# MAGIC tm_group_type_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_group_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_intrstd_party_role (
# MAGIC tm_intrstd_party_role_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_intrstd_party_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_party_role (
# MAGIC tm_party_role_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC tm_cardinality_ct string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_party_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_review_status (
# MAGIC tm_review_status_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_review_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_us_intl_cls_mapping (
# MAGIC fk_us_class_id int, 
# MAGIC fk_intl_class_id int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_us_intl_cls_mapping'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_work_item_reltnsp_type (
# MAGIC work_item_relationship_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_work_item_reltnsp_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_work_item_request (
# MAGIC work_item_request_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC cfk_business_unit_cd string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_work_item_request'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_work_item_type (
# MAGIC work_item_type_cd string, 
# MAGIC fk_parent_work_item_type_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC work_item_group_in string, 
# MAGIC work_item_ct string, 
# MAGIC office_action_sort_order_no int, 
# MAGIC office_action_frst_actn_in string, 
# MAGIC office_action_pst_frst_actn_in string, 
# MAGIC office_action_pst_fnl_actn_in string, 
# MAGIC office_action_during_appeal_in string, 
# MAGIC office_activity_credit_cand_in string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_work_item_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_work_item_type_doc_tmplt (
# MAGIC fk_document_template_cd string, 
# MAGIC fk_work_item_type_cd string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_work_item_type_doc_tmplt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_work_item_type_rule (
# MAGIC work_item_type_rule_id int, 
# MAGIC fk_work_item_type_cd string, 
# MAGIC rule_nm string, 
# MAGIC rule_type_ct string, 
# MAGIC rule_condition_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_work_item_type_rule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_worker_reltnsp_type (
# MAGIC worker_relationship_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_worker_reltnsp_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_writing_rvw_addl_actn (
# MAGIC writing_rvw_addl_actn_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_writing_rvw_addl_actn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission (
# MAGIC submission_gid string, 
# MAGIC fk_submission_method_cd string, 
# MAGIC fk_submission_form_type_id int, 
# MAGIC received_dt timestamp, 
# MAGIC response_in string, 
# MAGIC filing_dt timestamp, 
# MAGIC status_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_averment (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_AVERMENT_ID int, 
# MAGIC NON_STANDARD_AVERMENT_TX string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_averment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_averment_h (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_AVERMENT_ID int, 
# MAGIC NON_STANDARD_AVERMENT_TX string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp, 
# MAGIC ACTION_CT string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_averment_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_elctrn_addr (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC FK_ELECTRONIC_ADDRESS_GID string, 
# MAGIC PRIMARY_IN string, LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_elctrn_addr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_elctrn_addr_h (
# MAGIC FK_SUBMISSION_GID string,
# MAGIC FK_ELECTRONIC_ADDRESS_GID string, 
# MAGIC PRIMARY_IN string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp, 
# MAGIC ACTION_CT string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_elctrn_addr_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_h (
# MAGIC submission_gid string, 
# MAGIC fk_submission_method_cd string, 
# MAGIC fk_submission_form_type_id int, 
# MAGIC received_dt timestamp, 
# MAGIC response_in string, 
# MAGIC filing_dt timestamp, 
# MAGIC status_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_item (
# MAGIC submission_item_gid string, 
# MAGIC fk_work_item_gid string, 
# MAGIC fk_submission_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_item'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_item_h (
# MAGIC submission_item_gid string, 
# MAGIC fk_submission_gid string, 
# MAGIC fk_work_item_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_item_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.submission_signature (
# MAGIC FK_SUBMISSION_GID string, 
# MAGIC SEQUENCE_NO int, 
# MAGIC SIGNATURE_METHOD_CT string, 
# MAGIC SIGNATURE_TX string, 
# MAGIC SIGNATURE_DT timestamp, 
# MAGIC SIGNATURE_IMAGE_OBJ string, 
# MAGIC SIGNATORY_NAME_TX string, 
# MAGIC SIGNATORY_POSITION_TX string, 
# MAGIC SIGNATORY_TELECOM_NO string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/submission_signature'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_authuser (
# MAGIC id int,
# MAGIC userid string,
# MAGIC password string,
# MAGIC role string,
# MAGIC createdate timestamp,
# MAGIC lastupdated timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_authuser'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_caselock (
# MAGIC SERIAL_NUM int,
# MAGIC LOCK_STATUS string,
# MAGIC LOCK_REASON string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_caselock'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_casestatus (
# MAGIC cs_serial_num int,
# MAGIC cs_uj_date int,
# MAGIC cs_uj_timer int,
# MAGIC cs_status string,
# MAGIC cs_lock string,
# MAGIC cs_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_casestatus'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_checkpoint (
# MAGIC script_nm string,
# MAGIC start_ts timestamp,
# MAGIC commit_count decimal(22,0),
# MAGIC records_commited decimal(22,0),
# MAGIC last_commit_ts timestamp,
# MAGIC commit_frequency string,
# MAGIC end_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_checkpoint'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_exception_type (
# MAGIC error_tx string,
# MAGIC error_type string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_exception_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_exceptions (
# MAGIC insert_dt timestamp,
# MAGIC script_num decimal(22,0),
# MAGIC source_table string,
# MAGIC source_field string,
# MAGIC source_value string,
# MAGIC serial_num string,
# MAGIC object_gid string,
# MAGIC target_table string,
# MAGIC target_field string,
# MAGIC error_num int,
# MAGIC rule	int,
# MAGIC error_msg	string,
# MAGIC cleared_ind	string,
# MAGIC type_ct	string,
# MAGIC resolved_ts	timestamp,
# MAGIC severity_cd	string,
# MAGIC sync_exceptions_id	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_exceptions'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_log (
# MAGIC id int,
# MAGIC createdate timestamp,
# MAGIC action string,
# MAGIC userid string,
# MAGIC note string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_log'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_migration_rules (
# MAGIC tram_full_name string,
# MAGIC dataset string,
# MAGIC cobol_field_name string,
# MAGIC tmng_mapping string,
# MAGIC tmng_transformation_rule string,
# MAGIC tmng_data_type_cleansing string,
# MAGIC target_table_name string,
# MAGIC target_column_name string,
# MAGIC updated_date string,
# MAGIC rule_num string,
# MAGIC approve_reject	string,
# MAGIC approve_reject_date	string,
# MAGIC approval_rejection_comments	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_migration_rules'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_migration_script (
# MAGIC script_num decimal(22,0),
# MAGIC script_seq decimal(22,0),
# MAGIC script_name string,
# MAGIC source_table string,
# MAGIC target_table string,
# MAGIC default_create_userid string,
# MAGIC default_last_userid string,
# MAGIC print_only string,
# MAGIC script_description string,
# MAGIC commit_count int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_migration_script'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_runtime (
# MAGIC DNAME string, 
# MAGIC DBROLE string, 
# MAGIC SNAME string, 
# MAGIC SHOST string, 
# MAGIC CSCHEMA string, 
# MAGIC ESCHEMA string, 
# MAGIC USCHEMA string, 
# MAGIC UOS string, 
# MAGIC UHOST string, 
# MAGIC USESSION string, 
# MAGIC TBSDATA string, 
# MAGIC TBSIDX string, 
# MAGIC TBSIDXLRG string, 
# MAGIC SOWNER string, 
# MAGIC DMLROLE string, 
# MAGIC PSTOP string, 
# MAGIC PERR string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_runtime'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_stnd_am_stat (
# MAGIC am_stat int,
# MAGIC description string,
# MAGIC control_num string,
# MAGIC tram_state string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_stnd_am_stat'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_tm_com_exception (
# MAGIC tm_com_exception_id  decimal(22,0),
# MAGIC insert_ts            timestamp,
# MAGIC source_ip            string,
# MAGIC tm_com_service_nm    string,
# MAGIC endpoint_url         string,
# MAGIC endpoint_type_cd     string,
# MAGIC endpoint_body        string,
# MAGIC http_error_cd        string,
# MAGIC http_error_msg       string,
# MAGIC retry_ind            string,
# MAGIC resolved_ts          timestamp,
# MAGIC ref_no               string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_tm_com_exception'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_tram_trm_obj_id_mapping (
# MAGIC rsn		int,
# MAGIC legacy_dataset		string,
# MAGIC class_name		string,
# MAGIC serial_num		int,
# MAGIC trademark_id		int,
# MAGIC gid		string,
# MAGIC row_id_key		int,
# MAGIC row_gid_key		string,
# MAGIC obj_creator		string,
# MAGIC row_cd		string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_tram_trm_obj_id_mapping'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_tranlog  (
# MAGIC TL_DATE int, 
# MAGIC TL_TIMER int, 
# MAGIC TL_SER_NUM int, 
# MAGIC TL_STATE string, 
# MAGIC TL_TIMESTAMP timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_tranlog'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_assumed_name (
# MAGIC data_tx string,
# MAGIC conv_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_assumed_name'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_emp_lo (
# MAGIC empe_num int,
# MAGIC empe_lo string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_emp_lo'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_ep (
# MAGIC prodvty_cd  int,
# MAGIC prodvty_ind  int,
# MAGIC exam_no  int,
# MAGIC reason_tx  string,
# MAGIC fk_work_item_code  string,
# MAGIC fk_credit_tran_rsn_type_cd  string,
# MAGIC reason_ct  string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_ep'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_geo (
# MAGIC legacy_cd string,
# MAGIC geo_unit_cd string,
# MAGIC geo_unit_nm string,
# MAGIC country_cd string,
# MAGIC country_nm string,
# MAGIC geo_type_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_geo'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_location (
# MAGIC law_office_cd string,
# MAGIC palm_short_cd string,
# MAGIC tt_text string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_location'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_og_catg (
# MAGIC og_cat int,
# MAGIC pub_cat_cd string,
# MAGIC pub_cat_des string,
# MAGIC pub_sub_cd string,
# MAGIC pub_sub_des string,
# MAGIC lvl1 string,
# MAGIC lvl2 string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_og_catg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_party_type (
# MAGIC legacy_party_type string,
# MAGIC milestone_cd string,
# MAGIC owner_type_cd string,
# MAGIC fk_owner_type_id int,
# MAGIC owner_type_sequence_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_party_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_petition_dockt (
# MAGIC doc_type_cd string,
# MAGIC description_tx string,
# MAGIC role_cd string,
# MAGIC docket_id int,
# MAGIC docket_tx string,
# MAGIC event_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_petition_dockt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_work_item_cms (
# MAGIC work_item_type_cd string,
# MAGIC cms_doc_type string,
# MAGIC doc_description string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_work_item_cms'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_trm_to_tram_control (
# MAGIC trm_tram_sync_control_id  decimal(22,0),
# MAGIC insert_ts                 timestamp,
# MAGIC source_hostname           string,
# MAGIC target_endpoint           string,
# MAGIC endpoint_type             string,
# MAGIC target_header             string,
# MAGIC target_error_code         int,
# MAGIC target_error_msg          string,
# MAGIC completed_ts              timestamp,
# MAGIC action_ct                 string,
# MAGIC create_user_id            string,
# MAGIC create_ts                 timestamp,
# MAGIC last_mod_user_id          string,
# MAGIC last_mod_ts               timestamp,
# MAGIC tran_code                 string,
# MAGIC ref_no                    string,
# MAGIC target_body               string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_trm_to_tram_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.telecom_address (
# MAGIC telecom_address_gid string, 
# MAGIC telecom_no string, 
# MAGIC extension_no string, 
# MAGIC fk_telecom_type_cd string, 
# MAGIC fk_telecom_format_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC submitted_telecom_no string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/telecom_address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.telecom_address_h (
# MAGIC telecom_address_gid string, 
# MAGIC telecom_no string, 
# MAGIC extension_no string, 
# MAGIC fk_telecom_type_cd string, 
# MAGIC fk_telecom_format_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC submitted_telecom_no string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/telecom_address_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_additional_statement (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_statement_type_cd string, 
# MAGIC order_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC statement_tx string,
# MAGIC actv_pr_other_prior_reg_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_additional_statement'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

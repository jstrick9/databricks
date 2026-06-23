# Databricks notebook source
# MAGIC %sql
# MAGIC
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/temp_trm_pq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/temp_trm_pq_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/temp_trm_pqc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
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

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.og_publication (
# MAGIC og_publication_gid string, 
# MAGIC publication_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/og_publication'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.og_publication_h (
# MAGIC og_publication_gid string, 
# MAGIC publication_dt timestamp, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/og_publication_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.og_publication_tm (
# MAGIC fk_og_publication_gid string, 
# MAGIC fk_tm_publication_gid string, 
# MAGIC record_no int, 
# MAGIC og_registration_no int, 
# MAGIC publication_notice_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/og_publication_tm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.og_publication_tm_h (
# MAGIC fk_og_publication_gid string, 
# MAGIC fk_tm_publication_gid string, 
# MAGIC record_no int, 
# MAGIC og_registration_no int, 
# MAGIC publication_notice_dt timestamp, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/og_publication_tm_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists  ${conf.catalog}.${conf.database}.og_tm_review (
# MAGIC og_tm_review_gid string, 
# MAGIC fk_og_publication_gid string, 
# MAGIC fk_tm_publication_gid string, 
# MAGIC fk_work_item_gid string, 
# MAGIC fk_tm_review_status_cd string, 
# MAGIC cfk_reviewer_employee_no string, 
# MAGIC cfk_organization_cd string, 
# MAGIC cfk_employee_role_cd string, 
# MAGIC publication_dt timestamp, 
# MAGIC dn_tm_serial_num_tx string, 
# MAGIC previous_og_bounce_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/og_tm_review'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.prcdng_employee_assignment (
# MAGIC cfk_proceeding_gid         string,
# MAGIC fk_prcdng_employee_role_cd string,
# MAGIC cfk_employee_no            string,
# MAGIC effective_dt               timestamp,
# MAGIC lock_control_no            int,
# MAGIC create_ts                  timestamp,
# MAGIC create_user_id             string,
# MAGIC last_mod_ts                timestamp,
# MAGIC last_mod_user_id           string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/prcdng_employee_assignment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists  ${conf.catalog}.${conf.database}.prcdng_employee_assignment_h (
# MAGIC cfk_proceeding_gid            string,
# MAGIC fk_prcdng_employee_role_cd    string,
# MAGIC cfk_employee_no               string,
# MAGIC effective_dt                  timestamp,
# MAGIC lock_control_no               int,
# MAGIC create_ts                     timestamp,
# MAGIC create_user_id                string,
# MAGIC last_mod_ts                   timestamp,
# MAGIC last_mod_user_id              string,
# MAGIC cfk_transaction_instance_gid  string,
# MAGIC action_ct                     string,
# MAGIC begin_effective_ts            timestamp,
# MAGIC end_effective_ts              timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/prcdng_employee_assignment_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.predefined_paragraph (
# MAGIC predefined_paragraph_id int, 
# MAGIC predefined_paragraph_ct string, 
# MAGIC cfk_fp_id string, 
# MAGIC cfk_employee_no string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/predefined_paragraph'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.predefined_paragraph_ver (
# MAGIC fk_document_component_id int, 
# MAGIC fk_predefined_paragraph_id int, 
# MAGIC fk_instruction_doc_compnt_id int, 
# MAGIC dn_fp_last_modified_dt timestamp, 
# MAGIC paragraph_nm string, 
# MAGIC paragraph_title_tx string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC status_ct string, 
# MAGIC version_no int, 
# MAGIC fk_original_doc_compnt_id int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/predefined_paragraph_ver'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.query_appeal (
# MAGIC query_appeal_gid string, 
# MAGIC fk_appeal_result_cd string, 
# MAGIC cfk_approval_role_cd string, 
# MAGIC cfk_approved_by_employee_no string, 
# MAGIC appeal_result_dt timestamp, 
# MAGIC appeal_proceeding_no string, 
# MAGIC appeal_decision_tx string, 
# MAGIC appeal_reason_tx string, 
# MAGIC director_email_sent_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/query_appeal'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.query_appeal_note (
# MAGIC note_sequence_no int, 
# MAGIC fk_employee_query_appeal_id int, 
# MAGIC appeal_note_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/query_appeal_note'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.query_appeal_status (
# MAGIC appeal_status_ts timestamp, 
# MAGIC fk_appeal_status_cd string, 
# MAGIC fk_employee_query_appeal_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/query_appeal_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.query_ground (
# MAGIC query_ground_id int, 
# MAGIC fk_review_query_gid string, 
# MAGIC fk_ground_cd string, 
# MAGIC fk_ground_type_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/query_ground'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.related_worker (
# MAGIC related_worker_id int, 
# MAGIC fk_base_worker_gid string, 
# MAGIC fk_related_worker_gid string, 
# MAGIC fk_worker_relationship_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/related_worker'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.review_annotation (
# MAGIC REVIEW_ANNOTATION_ID decimal(22,0), 
# MAGIC FK_OFFICE_ACTIVITY_REVIEW_ID int, 
# MAGIC FK_DOCUMENT_COMPONENT_ID int, 
# MAGIC TEXT_SEGMENT_LOCATOR_TX string, 
# MAGIC TEXT_SEGMENT_TX string, 
# MAGIC ANNOTATION_CT string, 
# MAGIC FK_ANNOTATION_STATUS_CD string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/review_annotation'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.review_issue (
# MAGIC fk_office_activity_review_id int, 
# MAGIC fk_review_issue_cd string, 
# MAGIC comment_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/review_issue'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.review_query (
# MAGIC review_query_gid string, 
# MAGIC fk_og_tm_review_gid string, 
# MAGIC cfk_approval_role_cd string, 
# MAGIC query_tx string, 
# MAGIC print_error_in string, 
# MAGIC og_page_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/review_query'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.review_query_appeal (
# MAGIC review_query_appeal_id int, 
# MAGIC fk_query_appeal_gid string, 
# MAGIC fk_query_ground_id int, 
# MAGIC approval_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/review_query_appeal'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.review_query_class (
# MAGIC fk_class_id int, 
# MAGIC fk_query_ground_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/review_query_class'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.review_query_note (
# MAGIC note_sequence_no int, 
# MAGIC fk_review_query_gid string, 
# MAGIC fk_note_type_cd string, 
# MAGIC cfk_employee_no string, 
# MAGIC cfk_organization_cd string, 
# MAGIC cfk_employee_role_cd string, 
# MAGIC note_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/review_query_note'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.search_strategy (
# MAGIC search_strategy_id int, 
# MAGIC search_strategy_nm string, 
# MAGIC public_in string, 
# MAGIC cfk_employee_no string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC description_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/search_strategy'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.section_2f_prior_reg (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/section_2f_prior_reg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.section_2f_prior_reg_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS TIMESTAMP, 
# MAGIC END_EFFECTIVE_TS TIMESTAMP, 
# MAGIC ACTION_CT string  
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/section_2f_prior_reg_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.section_2f_statement (
# MAGIC fk_trademark_gid string, 
# MAGIC section_2f_ct string, 
# MAGIC section_2f_basis_ct string, 
# MAGIC limitation_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC restrict_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/section_2f_statement'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.section_2f_statement_h (
# MAGIC fk_trademark_gid string, 
# MAGIC section_2f_ct string, 
# MAGIC section_2f_basis_ct string, 
# MAGIC limitation_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC restrict_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/section_2f_statement_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_annotation_status (
# MAGIC annotation_status_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC review_type_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_annotation_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_appeal_result (
# MAGIC appeal_result_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_appeal_result'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_appeal_status (
# MAGIC appeal_status_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_appeal_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_assumed_name_type (
# MAGIC assumed_name_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_assumed_name_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_averment (
# MAGIC averment_id int, 
# MAGIC averment_ct string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_averment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_business_event_rsn_ct (
# MAGIC business_event_rsn_ct_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_business_event_rsn_ct'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_category_doc_type (
# MAGIC fk_doc_type_ct_id int, 
# MAGIC fk_document_type_id int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_category_doc_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_class_schedule (
# MAGIC class_schedule_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC us_in string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_class_schedule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_class_statement_type (
# MAGIC class_statement_type_cd string, 
# MAGIC pre_formatted_statement_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_class_statement_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_coordinated_class (
# MAGIC fk_class_id int, 
# MAGIC fk_coordinated_class_id int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_coordinated_class'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_credit_tran_rsn_type (
# MAGIC credit_tran_rsn_type_cd string, 
# MAGIC credit_tran_rsn_type_ct string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_credit_tran_rsn_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_design_search_code_item (
# MAGIC fk_design_search_group_cd string, 
# MAGIC item_no int, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_design_search_code_item'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_design_search_group (
# MAGIC design_search_group_cd string, 
# MAGIC fk_design_search_group_type_cd string, 
# MAGIC fk_parent_design_search_grp_cd string, 
# MAGIC design_search_code_in string, 
# MAGIC design_search_group_no int, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_design_search_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_design_search_group_type (
# MAGIC design_search_group_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_design_search_group_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_doc_type_ct (
# MAGIC fk_parent_doc_type_ct_id int, 
# MAGIC cfk_business_unit_cd string, 
# MAGIC doc_type_ct_id int, 
# MAGIC doc_type_ct_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC business_unit_display_order_no int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_doc_type_ct'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_docket (
# MAGIC cfk_user_role_cd string, 
# MAGIC docket_id int, 
# MAGIC docket_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_docket'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_docket_fsm_type_state (
# MAGIC fk_docket_id int, 
# MAGIC cfk_fsm_type_state_id int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_docket_fsm_type_state'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_docket_item_event_type (
# MAGIC docket_item_event_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_docket_item_event_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_document_component_type (
# MAGIC document_component_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_document_component_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_document_template (
# MAGIC document_template_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_document_template'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_document_type (
# MAGIC fk_work_item_type_cd string, 
# MAGIC cfk_cms_document_type_cd string, 
# MAGIC document_type_id int, 
# MAGIC definition_source_ct string, 
# MAGIC legacy_document_type_cd string, 
# MAGIC legacy_description_tx string, 
# MAGIC legacy_title_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_document_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_electronic_addr_type (
# MAGIC electronic_addr_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_electronic_addr_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.stnd_evidence_bin (
# MAGIC evidence_bin_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_evidence_bin'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.tram_pqr (
# MAGIC PQR_RSN	decimal(22,0),
# MAGIC PQR_SER_NUM	integer,
# MAGIC PQR_REG_NUM	integer,
# MAGIC PQR_PR_EMPE_NUM	integer,
# MAGIC PQR_EMPE_NUM	integer,
# MAGIC PQR_SHARE_EMPE_NUM	integer,
# MAGIC PQR_LEAD_NUM	integer,
# MAGIC PQR_MGR_NUM	integer,
# MAGIC PQR_PR_MGR_NUM	integer,
# MAGIC PQR_TRAN_FY_PP	integer,
# MAGIC PQR_PR_TRAN_CD	integer,
# MAGIC PQR_CM_ENT_CD	string,
# MAGIC PQR_ENT_NUM	integer,
# MAGIC PQR_TRAN_SYS_DT	integer,
# MAGIC PQR_TRAN_SYS_TI	integer,
# MAGIC PQR_REVIEW_TYPE	string,
# MAGIC PQR_LEVEL1_ASGN_DT	integer,
# MAGIC PQR_LEVEL1_ASGN_TI	integer,
# MAGIC PQR_LEAD_ASGN_DT	integer,
# MAGIC PQR_LEAD_ASGN_TI	integer,
# MAGIC PQR_MGR_ASGN_DT	integer,
# MAGIC PQR_MGR_ASGN_TI	integer,
# MAGIC PQR_CREATE_DT	integer,
# MAGIC PQR_CREATE_TI	integer,
# MAGIC PQR_RANDOM_NUM	integer,
# MAGIC PQR_REVIEW_STAT	integer,
# MAGIC PQR_APPEAL_FLAG	integer,
# MAGIC PQR_COP_FLAG	integer,
# MAGIC PQR_REVIEW_CMPLTD_DT	integer,
# MAGIC PQR_REVIEW_CMPLTD_TI	integer,
# MAGIC PQR_APPEAL_RCPT_DT	integer,
# MAGIC PQR_APPEAL_RCPT_TI	integer,
# MAGIC PQR_APPEAL_CMPLTD_DT	integer,
# MAGIC PQR_APPEAL_CMPLTD_TI	integer,
# MAGIC PQR_FOLLOWUP_FLAG	integer,
# MAGIC PQR_FOLLOWUP_DT	integer,
# MAGIC PQR_APPEAL_EMPE_NUM	integer,
# MAGIC PQR_QUERY_STAT	integer
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pqr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.tram_pqre (
# MAGIC PQRE_RSN	decimal(22,0),
# MAGIC PQRE_SER_NUM	integer,
# MAGIC PQRE_REG_NUM	integer,
# MAGIC PQRE_TRAN_FY_PP	integer,
# MAGIC PQRE_EMPE_NUM	integer,
# MAGIC PQRE_EMPE_REV	integer,
# MAGIC PQRE_ELEM_CD	string,
# MAGIC PQRE_ENT_NUM	integer,
# MAGIC PQRE_QUERY_TEXTS	string,
# MAGIC PQRE_QUERY_COMMENTS	string,
# MAGIC PQRE_CREATE_DT	integer,
# MAGIC PQRE_CREATE_TI	integer,
# MAGIC PQRE_PQR_CREATE_DT	integer,
# MAGIC PQRE_PQR_CREATE_TI	integer,
# MAGIC PQRE_PQR_RANDOM_NUM	integer,
# MAGIC PQRE_SEVERITY	string,
# MAGIC PQRE_REVIEW_TYPE	string,
# MAGIC PQRE_ORIG_SEVERITY	string,
# MAGIC PQRE_POINT_FLAG	integer,
# MAGIC PQRE_APPEAL_COMMENTS	string,
# MAGIC PQRE_APPEAL_STAT	integer,
# MAGIC PQRE_APPEAL_NOTES	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pqre'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.tram_prn (
# MAGIC PRN_RSN	decimal(22,0),
# MAGIC PRN_SER_NUM	integer,
# MAGIC PRN_REG_NUM	integer,
# MAGIC PRN_EMPE_NUM	integer,
# MAGIC PRN_MGR_EMPE_NUM	integer,
# MAGIC PRN_TRAN_FY_PP	integer,
# MAGIC PRN_PR_TRAN_CD	integer,
# MAGIC PRN_CM_ENT_CD	string,
# MAGIC PRN_ENT_NUM	integer,
# MAGIC PRN_TRAN_SYS_DT	integer,
# MAGIC PRN_TRAN_SYS_TI	integer,
# MAGIC PRN_RANDOM_NUM	integer,
# MAGIC PRN_LEVEL1_ASGN_DT	integer,
# MAGIC PRN_LEVEL1_ASGN_TI	integer,
# MAGIC PRN_MGR_ASGN_DT	integer,
# MAGIC PRN_MGR_ASGN_TI	integer,
# MAGIC PRN_CREATE_DT	integer,
# MAGIC PRN_CREATE_TI	integer,
# MAGIC PRN_EXTENSION_FLAG	integer,
# MAGIC PRN_APPEAL_FLAG	integer,
# MAGIC PRN_APPEAL_STAT	integer,
# MAGIC PRN_SUBMITTED_DT	integer,
# MAGIC PRN_SUBMITTED_TI	integer,
# MAGIC PRN_APPEAL_END_DT	integer,
# MAGIC PRN_LEAD_EMPE_NUM	integer,
# MAGIC PRN_FOLLOWUP_FLAG	integer,
# MAGIC PRN_FOLLOWUP_DT	integer,
# MAGIC PRN_LEAD_ASGN_DT	integer,
# MAGIC PRN_LEAD_ASGN_TI	integer,
# MAGIC PRN_REVIEW_TYPE	string,
# MAGIC PRN_QUERY_STAT	integer
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_prn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.${conf.database}.tram_prna (
# MAGIC PRNA_RSN	decimal(22,0),
# MAGIC PRNA_SER_NUM	integer,
# MAGIC PRNA_REG_NUM	integer,
# MAGIC PRNA_TRAN_FY_PP	integer,
# MAGIC PRNA_ELEM_CD	string,
# MAGIC PRNA_ENT_NUM	integer,
# MAGIC PRNA_APPEAL_CMNTS	string,
# MAGIC PRNA_CREATE_DT	integer,
# MAGIC PRNA_CREATE_TI	integer,
# MAGIC PRNA_APPEAL_FLAG	integer,
# MAGIC PRNA_APPEAL_STAT	integer,
# MAGIC PRNA_SEVERITY	string,
# MAGIC PRNA_LDMGR_CMNTS	string,
# MAGIC PRNA_NOTES	string,
# MAGIC PRNA_REVIEW_TYPE	string,
# MAGIC PRNA_TQR_APL_CMNT	string,
# MAGIC PRNA_ORIG_SEVERITY	string,
# MAGIC PRNA_POINT_FLAG	integer,
# MAGIC PRNA_APPEAL_NOTES	string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_prna'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------



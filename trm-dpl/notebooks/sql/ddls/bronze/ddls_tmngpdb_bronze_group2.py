# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.abandonment (
# MAGIC fk_work_item_gid string, 
# MAGIC abandonment_dt timestamp, 
# MAGIC abandonment_date_override_in string, 
# MAGIC response_received_in string, 
# MAGIC response_received_override_in string, 
# MAGIC fk_response_issue_cd string, 
# MAGIC response_issue_tx string, 
# MAGIC response_on_time_in string, 
# MAGIC response_on_time_override_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/abandonment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.abandonment_h (
# MAGIC fk_work_item_gid string, 
# MAGIC abandonment_dt timestamp, 
# MAGIC abandonment_date_override_in string, 
# MAGIC response_received_in string, 
# MAGIC response_received_override_in string, 
# MAGIC fk_response_issue_cd string, 
# MAGIC response_issue_tx string, 
# MAGIC response_on_time_in string, 
# MAGIC response_on_time_override_in string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/abandonment_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.annotation_comment (
# MAGIC FK_REVIEW_ANNOTATION_ID int, 
# MAGIC CFK_EMPLOYEE_NO string, 
# MAGIC COMMENT_DT timestamp, 
# MAGIC COMMENT_SOURCE_CT string, 
# MAGIC COMMENT_TX string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC ANNOTATION_COMMENT_ID decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/annotation_comment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.base_application (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_international_appl_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/base_application'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.base_application_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_international_appl_gid string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/base_application_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.concurrent_use (
# MAGIC fk_trademark_gid string, 
# MAGIC statement_no int, 
# MAGIC concurrent_use_year_no int, 
# MAGIC concurrent_use_month_no int, 
# MAGIC concurrent_use_day_no int, 
# MAGIC concurrent_use_basis_ct string, 
# MAGIC concurrent_use_status_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC statement_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/concurrent_use'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.concurrent_use_h (
# MAGIC fk_trademark_gid string, 
# MAGIC statement_no int, 
# MAGIC concurrent_use_year_no int, 
# MAGIC concurrent_use_month_no int, 
# MAGIC concurrent_use_day_no int, 
# MAGIC concurrent_use_basis_ct string, 
# MAGIC concurrent_use_status_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC statement_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/concurrent_use_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.custom_alert (
# MAGIC CUSTOM_ALERT_ID int, 
# MAGIC TITLE_TX string, 
# MAGIC TRIGGER_TYPE_CT string, 
# MAGIC USER_CONTROL_LEVEL_CT string, 
# MAGIC CFK_DOMAIN_MESSAGE_ID int, 
# MAGIC TRIGGER_SCHEDULE_DT TIMESTAMP, 
# MAGIC CFK_RECIPIENT_EMPLOYEE_NO string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS TIMESTAMP, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS TIMESTAMP, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/custom_alert'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.doc_tmplt_ver_form_para (
# MAGIC fk_document_template_cd string, 
# MAGIC fk_version_no int, 
# MAGIC fk_template_para_type_cd string, 
# MAGIC rank_order_no int, 
# MAGIC editable_in string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_fp_call_number_tx string, 
# MAGIC paragraph_type_ct string, 
# MAGIC doc_tmplt_ver_form_para_id decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/doc_tmplt_ver_form_para'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.docket_item (
# MAGIC fk_work_item_gid string, 
# MAGIC cfk_assignee_employee_no string, 
# MAGIC cfk_assigning_employee_no string, 
# MAGIC cfk_object_gid string, 
# MAGIC cfk_organization_cd string, 
# MAGIC effective_dt timestamp, 
# MAGIC fk_docket_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC docket_item_id decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/docket_item'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.docket_item_event (
# MAGIC fk_docket_item_id int, 
# MAGIC fk_docket_item_event_type_cd string, 
# MAGIC cfk_assignee_employee_no string, 
# MAGIC event_dt timestamp, 
# MAGIC event_goal_dt timestamp, 
# MAGIC event_deadline_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/docket_item_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.docket_item_event_h (
# MAGIC fk_docket_item_id int, 
# MAGIC fk_docket_item_event_type_cd string, 
# MAGIC cfk_assignee_employee_no string, 
# MAGIC event_dt timestamp, 
# MAGIC event_goal_dt timestamp, 
# MAGIC event_deadline_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/docket_item_event_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.docket_item_h (
# MAGIC cfk_object_gid string, 
# MAGIC cfk_assignee_employee_no string, 
# MAGIC cfk_assigning_employee_no string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC fk_docket_id int, 
# MAGIC fk_work_item_gid string, 
# MAGIC cfk_organization_cd string, 
# MAGIC effective_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC docket_item_id decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/docket_item_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.document_component (
# MAGIC document_component_id int, 
# MAGIC fk_document_component_type_cd string, 
# MAGIC document_component_ct string, 
# MAGIC document_component_tx string, 
# MAGIC document_component_metadata_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/document_component'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.document_component_reltnsp (
# MAGIC fk_parent_document_compnt_id int, 
# MAGIC fk_child_document_component_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/document_component_reltnsp'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.document_template_version (
# MAGIC fk_document_template_cd string, 
# MAGIC version_no int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/document_template_version'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.draft_doc_ver_compnt_fpv (
# MAGIC FK_DRAFT_DOCUMENT_ID int,  
# MAGIC FK_DRAFT_DOCUMENT_MOD_NO int,  
# MAGIC FK_DOCUMENT_COMPONENT_ID int,  
# MAGIC CFK_FORM_PARAGRAPH_VERSION_GID string,  
# MAGIC LOCK_CONTROL_NO int,   
# MAGIC CREATE_TS timestamp,  
# MAGIC CREATE_USER_ID string,  
# MAGIC LAST_MOD_TS timestamp,  
# MAGIC LAST_MOD_USER_ID string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/draft_doc_ver_compnt_fpv'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.draft_document (
# MAGIC draft_document_id int, 
# MAGIC draft_document_nm string, 
# MAGIC draft_document_status_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/draft_document'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.draft_document_version (
# MAGIC fk_draft_document_id int, 
# MAGIC draft_document_mod_no int, 
# MAGIC fk_document_template_cd string, 
# MAGIC fk_version_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/draft_document_version'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.draft_document_version_compnt (
# MAGIC fk_draft_document_id int, 
# MAGIC fk_draft_document_mod_no int, 
# MAGIC fk_document_component_id int, 
# MAGIC rank_order_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/draft_document_version_compnt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.electronic_address_h (
# MAGIC electronic_address_gid string, 
# MAGIC electronic_addr_locator_tx string, 
# MAGIC fk_electronic_addr_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/electronic_address_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.employee_award_withdraw (
# MAGIC fk_award_empe_cr_tran_id int, 
# MAGIC fk_withdraw_empe_cr_tran_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/employee_award_withdraw'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.employee_credit_transaction (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_trademark_gid string, 
# MAGIC fk_credit_tran_rsn_type_cd string, 
# MAGIC cfk_earner_empe_no string, 
# MAGIC cfk_approver_empe_no string, 
# MAGIC transaction_effective_dt timestamp, 
# MAGIC transaction_value_no int, 
# MAGIC transaction_reason_ct string, 
# MAGIC transaction_reason_tx string, 
# MAGIC transaction_type_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC employee_credit_tran_id int, 
# MAGIC active_tm_class_count_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/employee_credit_transaction'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.employee_query_appeal (
# MAGIC employee_query_appeal_id int, 
# MAGIC fk_query_appeal_gid string, 
# MAGIC cfk_employee_role_cd string, 
# MAGIC cfk_employee_no string, 
# MAGIC cfk_organization_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/employee_query_appeal'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.employee_review_query (
# MAGIC employee_review_query_id int, 
# MAGIC fk_query_ground_id int, 
# MAGIC cfk_employee_no string, 
# MAGIC cfk_organization_cd string, 
# MAGIC cfk_employee_role_cd string, 
# MAGIC review_assignment_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/employee_review_query'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.employee_review_query_stat (
# MAGIC status_ts timestamp, 
# MAGIC fk_employee_review_query_id int, 
# MAGIC fk_query_review_status_cd string, 
# MAGIC status_reason_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/employee_review_query_stat'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.employee_tm_class_credit (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_class_id int, 
# MAGIC fk_employee_credit_tran_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/employee_tm_class_credit'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.evidence_bin_folder (
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/evidence_bin_folder'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.evidence_bin_folder_h (
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/evidence_bin_folder_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.evidence_document (
# MAGIC evidence_document_id int, 
# MAGIC fk_evidence_bin_folder_id int, 
# MAGIC display_order_no int, 
# MAGIC fk_tm_document_id int, 
# MAGIC fk_sequence_no int, 
# MAGIC fk_evidence_source_category_cd string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC evidence_document_alias_nm string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/evidence_document'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.evidence_document_h (
# MAGIC evidence_document_id int, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string, 
# MAGIC fk_evidence_bin_folder_id int, 
# MAGIC display_order_no int, 
# MAGIC fk_tm_document_id int, 
# MAGIC fk_sequence_no int, 
# MAGIC fk_evidence_source_category_cd string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC evidence_document_alias_nm string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/evidence_document_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.form_paragraph_rule (
# MAGIC form_paragraph_rule_id int, 
# MAGIC rule_nm string, 
# MAGIC rule_type_ct string, 
# MAGIC rule_condition_tx string, 
# MAGIC fk_work_item_type_cd string, 
# MAGIC fk_document_template_cd string, 
# MAGIC cfk_domain_message_id int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_fp_call_number_tx string, 
# MAGIC paragraph_source_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/form_paragraph_rule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.fsm_instance (
# MAGIC fsm_instance_gid string, 
# MAGIC fk_parent_fsm_instance_gid string, 
# MAGIC fk_root_fsm_instance_gid string, 
# MAGIC fk_fsm_type_id int, 
# MAGIC fk_current_fsm_type_state_id int, 
# MAGIC suspended_no int, 
# MAGIC depth_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC terminated_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/fsm_instance'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.fsm_instance_h(
# MAGIC   fsm_instance_gid string, 
# MAGIC   fk_parent_fsm_instance_gid string, 
# MAGIC   fk_root_fsm_instance_gid string, 
# MAGIC   fk_fsm_type_id int, 
# MAGIC   fk_current_fsm_type_state_id int, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   suspended_no int, 
# MAGIC   depth_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   terminated_in string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/fsm_instance_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.gds_srvc_stmt_annotation (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_class_id int, 
# MAGIC parse_option_ct string, 
# MAGIC annotation_ct string, 
# MAGIC display_order_no int, 
# MAGIC text_segment_locator_tx string, 
# MAGIC text_segment_tx string, 
# MAGIC fk_gds_srvc_match_stat_cd string, 
# MAGIC fk_gds_srvc_annotn_status_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/gds_srvc_stmt_annotation'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.ib_transaction (
# MAGIC FK_WORK_ITEM_GID string, 
# MAGIC ORIGIN_CT string, 
# MAGIC MPU_SENT_TS TIMESTAMP, 
# MAGIC MPU_SENT_STATUS_CT string, 
# MAGIC IB_RECEIPT_TS TIMESTAMP, 
# MAGIC IB_RECEIPT_STATUS_CT string, 
# MAGIC DATA_TYPE_CT string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/ib_transaction'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.interested_party_h (
# MAGIC interested_party_gid string, 
# MAGIC interested_party_ct string, 
# MAGIC fk_legal_entity_type_cd string, 
# MAGIC legal_entity_statement_tx string, 
# MAGIC interested_party_nm string, 
# MAGIC individual_given_nm string, 
# MAGIC individual_middle_nm string, 
# MAGIC individual_family_nm string, 
# MAGIC individual_suffix_nm string, 
# MAGIC individual_prefix_nm string, 
# MAGIC preferred_contact_method_ct string, 
# MAGIC individual_minor_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC party_composition_tx string, 
# MAGIC country_role_ct string, 
# MAGIC country_cd string, 
# MAGIC country_nm string, 
# MAGIC geographic_region_cd string, 
# MAGIC geographic_region_nm string,
# MAGIC fk_primary_electronic_addr_gid	string,
# MAGIC fk_primary_telecom_addr_gid	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/interested_party_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.interested_party_assumed_nm (
# MAGIC intrstd_party_assumed_name_id int, 
# MAGIC fk_interested_party_gid string, 
# MAGIC assumed_nm string, 
# MAGIC fk_assumed_name_type_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/interested_party_assumed_nm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.interested_party_assumed_nm_h (
# MAGIC intrstd_party_assumed_name_id int, 
# MAGIC fk_interested_party_gid string, 
# MAGIC assumed_nm string, 
# MAGIC fk_assumed_name_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/interested_party_assumed_nm_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.internal_note (
# MAGIC internal_note_id int, 
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC fk_work_item_gid string, 
# MAGIC fk_document_component_id int, 
# MAGIC note_type_ct string, 
# MAGIC note_location_ct string, 
# MAGIC subject_tx string, 
# MAGIC prevent_publication_aprvl_in string, 
# MAGIC prevent_registration_allwnc_in string, 
# MAGIC fk_business_event_id int, 
# MAGIC cfk_cms_evidence_id string, 
# MAGIC cfk_completed_employee_no string, 
# MAGIC completed_ts timestamp, 
# MAGIC allow_delete_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC legacy_jn_ent_num int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/internal_note'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

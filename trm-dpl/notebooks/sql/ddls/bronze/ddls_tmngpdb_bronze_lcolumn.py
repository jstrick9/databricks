# Databricks notebook source
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
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

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
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.query_appeal (
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.review_query_note (
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_literal (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC lock_control_no int, 
# MAGIC literal_element_tx string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_literal'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_literal_h (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC literal_element_tx string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_literal_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_registration_statement_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_reg_stmnt_type_cd string, 
# MAGIC sequence_no int, 
# MAGIC statement_year_no int, 
# MAGIC statement_month_no int, 
# MAGIC statement_day_no int, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_registration_statement_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.trademark_h (
# MAGIC trademark_gid string, 
# MAGIC fk_mark_drawing_type_cd string, 
# MAGIC fk_fee_process_type_cd string, 
# MAGIC serial_num_tx string, 
# MAGIC registration_num int, 
# MAGIC filing_dt timestamp, 
# MAGIC registry_ct string, 
# MAGIC standard_character_tx string, 
# MAGIC mark_description_tx string, 
# MAGIC preferred_contact_method_ct string, 
# MAGIC effective_filing_dt timestamp, 
# MAGIC collective_in string, 
# MAGIC legacy_status_cd int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC status_dt timestamp, 
# MAGIC last_action_dt timestamp, 
# MAGIC action_ct string, 
# MAGIC available_for_sou_in string, 
# MAGIC external_reference_tx string,
# MAGIC last_event_type_cd	string,
# MAGIC uspto_generated_image_in	string,
# MAGIC fk_filed_fee_process_type_cd	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/trademark_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.trademark (
# MAGIC trademark_gid string, 
# MAGIC fk_mark_drawing_type_cd string, 
# MAGIC fk_fee_process_type_cd string, 
# MAGIC serial_num_tx string, 
# MAGIC registration_num int, 
# MAGIC filing_dt timestamp, 
# MAGIC registry_ct string, 
# MAGIC standard_character_tx string, 
# MAGIC mark_description_tx string, 
# MAGIC preferred_contact_method_ct string, 
# MAGIC effective_filing_dt timestamp, 
# MAGIC collective_in string, 
# MAGIC legacy_status_cd int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC status_dt timestamp, 
# MAGIC last_action_dt timestamp, 
# MAGIC available_for_sou_in string, 
# MAGIC external_reference_tx string,
# MAGIC last_event_type_cd string,
# MAGIC uspto_generated_image_in string,
# MAGIC fk_filed_fee_process_type_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/trademark'
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_additional_statement_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_statement_type_cd string, 
# MAGIC order_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC statement_tx string, 
# MAGIC action_ct string,
# MAGIC actv_pr_other_prior_reg_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_additional_statement_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_registration_statement (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_reg_stmnt_type_cd string, 
# MAGIC sequence_no int, 
# MAGIC statement_year_no int, 
# MAGIC statement_month_no int, 
# MAGIC statement_day_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC statement_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_registration_statement'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.review_query (
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.interested_party (
# MAGIC interested_party_gid string, 
# MAGIC interested_party_ct string, 
# MAGIC fk_legal_entity_type_cd string, 
# MAGIC legal_entity_statement_tx string, 
# MAGIC interested_party_nm string, 
# MAGIC individual_given_nm string, 
# MAGIC individual_suffix_nm string, 
# MAGIC individual_middle_nm string, 
# MAGIC individual_family_nm string, 
# MAGIC individual_prefix_nm string, 
# MAGIC individual_minor_in string, 
# MAGIC preferred_contact_method_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/interested_party'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.query_appeal_note (
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

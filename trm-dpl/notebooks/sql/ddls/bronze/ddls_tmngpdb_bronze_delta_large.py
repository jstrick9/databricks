# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_telecom_addr_h (
# MAGIC fk_tm_party_role_id int, 
# MAGIC fk_telecom_address_gid string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string, 
# MAGIC primary_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_telecom_addr_h'
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mailing_address_line_h (
# MAGIC fk_mailing_address_gid string, 
# MAGIC sequence_no int, 
# MAGIC address_line_ct string, 
# MAGIC address_line_tx string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mailing_address_line_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.work_item_object (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_object_type_cd string, 
# MAGIC cfk_object_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_object'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

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
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_electronic_addr_h (
# MAGIC fk_tm_party_role_id int, 
# MAGIC fk_electronic_address_gid string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string, 
# MAGIC authorized_email_in string, 
# MAGIC primary_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_electronic_addr_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_party_role_h (
# MAGIC tm_party_role_id int, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string, 
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_party_role_cd string, 
# MAGIC party_role_sequence_no int, 
# MAGIC fk_interested_party_gid string, 
# MAGIC attorney_reference_no string, 
# MAGIC bar_information_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC bar_membership_year_no int, 
# MAGIC bar_membership_month_no int, 
# MAGIC bar_membership_state_cd string,
# MAGIC cfk_patron_id string,
# MAGIC bar_membership_state_nm string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_party_role_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class_reference (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_class_id int, 
# MAGIC fk_referenced_class_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_reference'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_mailing_addr (
# MAGIC fk_tm_party_role_id int, 
# MAGIC fk_mailing_address_gid string, 
# MAGIC primary_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_mailing_addr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mailing_address (
# MAGIC mailing_address_gid string, 
# MAGIC name_line_1_tx string, 
# MAGIC name_line_2_tx string, 
# MAGIC street_line_1_tx string, 
# MAGIC street_line_2_tx string, 
# MAGIC city_nm string, 
# MAGIC geographic_region_cd string, 
# MAGIC geographic_region_nm string, 
# MAGIC postal_cd string, 
# MAGIC country_cd string, 
# MAGIC country_nm string, 
# MAGIC department_nm string, 
# MAGIC address_type_ct string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mailing_address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_party_role (
# MAGIC tm_party_role_id int, 
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_party_role_cd string, 
# MAGIC party_role_sequence_no int, 
# MAGIC fk_interested_party_gid string, 
# MAGIC attorney_reference_no string, 
# MAGIC bar_information_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC bar_membership_year_no int, 
# MAGIC bar_membership_month_no int, 
# MAGIC bar_membership_state_cd string,
# MAGIC cfk_patron_id string,
# MAGIC bar_membership_state_nm string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_party_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_milestone_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_milestone_cd string, 
# MAGIC milestone_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_milestone_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_milestone (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_milestone_cd string, 
# MAGIC milestone_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_milestone'
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mailing_address_line (
# MAGIC fk_mailing_address_gid string, 
# MAGIC sequence_no int, 
# MAGIC address_line_ct string, 
# MAGIC address_line_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mailing_address_line'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_telecom_addr (
# MAGIC fk_tm_party_role_id int, 
# MAGIC fk_telecom_address_gid string, 
# MAGIC primary_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_telecom_addr'
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

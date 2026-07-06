# Databricks notebook source
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_addl_stmnt_prior_reg (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_STATEMENT_TYPE_CD string, 
# MAGIC FK_ORDER_NO int, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_addl_stmnt_prior_reg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_addl_stmnt_prior_reg_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_STATEMENT_TYPE_CD string, 
# MAGIC FK_ORDER_NO int, 
# MAGIC FK_PRIOR_REG_TRADEMARK_GID string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp,
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_addl_stmnt_prior_reg_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_amendment (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_amendment_reason_cd string, 
# MAGIC sequence_no int, 
# MAGIC target_element_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string,
# MAGIC cfk_status_cd string,
# MAGIC target_element_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_amendment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class_filing_basis (
# MAGIC fk_trademark_gid    string,
# MAGIC fk_class_id         int,
# MAGIC fk_filing_basis_cd  string,
# MAGIC lock_control_no     int,
# MAGIC create_ts           timestamp,
# MAGIC create_user_id      string,
# MAGIC last_mod_ts         timestamp,
# MAGIC last_mod_user_id    string       
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_filing_basis'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class_gds_srvc_term (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_GDS_SRVC_STATUS_CD string, 
# MAGIC FK_GDS_SRVC_STATUS_RSN_CD string, 
# MAGIC FK_STMNT_ACTVTY_TYPE_CD string, 
# MAGIC FIRST_USE_IN_COMMERCE_MONTH_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_DAY_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_YEAR_NO int, 
# MAGIC FIRST_USE_ANYWHERE_MONTH_NO int, 
# MAGIC FIRST_USE_ANYWHERE_DAY_NO int, 
# MAGIC FIRST_USE_ANYWHERE_YEAR_NO int, 
# MAGIC INTENT_TO_USE_DT timestamp, LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC GDS_SRVC_TERM_TX string, 
# MAGIC SUGGESTED_GDS_SRVC_TERM_TX string, 
# MAGIC FK_GOODS_SERVICES_TERM_ID decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_gds_srvc_term'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class_gds_srvc_term_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC SEQUENCE_NO int, 
# MAGIC FK_STMNT_ACTVTY_TYPE_CD string, 
# MAGIC FK_GDS_SRVC_STATUS_CD string, 
# MAGIC FK_GDS_SRVC_STATUS_RSN_CD string, 
# MAGIC FIRST_USE_IN_COMMERCE_MONTH_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_DAY_NO int, 
# MAGIC FIRST_USE_IN_COMMERCE_YEAR_NO int, 
# MAGIC FIRST_USE_ANYWHERE_MONTH_NO int, 
# MAGIC FIRST_USE_ANYWHERE_DAY_NO int, 
# MAGIC FIRST_USE_ANYWHERE_YEAR_NO int, 
# MAGIC INTENT_TO_USE_DT timestamp, 
# MAGIC LOCK_CONTROL_NO int, CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp, 
# MAGIC GDS_SRVC_TERM_TX string, 
# MAGIC SUGGESTED_GDS_SRVC_TERM_TX string, 
# MAGIC ACTION_CT string, 
# MAGIC FK_GOODS_SERVICES_TERM_ID decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_gds_srvc_term_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class_h (
# MAGIC fk_class_id int, 
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_class_status_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC gds_srvcs_stmnt_tx string, 
# MAGIC gds_srvcs_stmnt_annotated_tx string, 
# MAGIC first_use_in_commerce_month_no int, 
# MAGIC first_use_in_commerce_day_no int, 
# MAGIC first_use_in_commerce_year_no int, 
# MAGIC first_use_anywhere_month_no int, 
# MAGIC first_use_anywhere_day_no int, 
# MAGIC first_use_anywhere_year_no int, 
# MAGIC intent_to_use_dt timestamp, 
# MAGIC action_ct string, 
# MAGIC status_dt timestamp,
# MAGIC excessive_character_fee_cnt_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_class_reference_h(
# MAGIC   fk_trademark_gid string, 
# MAGIC   fk_class_id int, 
# MAGIC   fk_referenced_class_id int, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   action_ct string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class_reference_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_design_element (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_design_search_group_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_design_element'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_design_element_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_design_search_group_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_design_element_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_divisional (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_divisional'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_divisional_child_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_sequence_no int, 
# MAGIC fk_child_trademark_gid string, 
# MAGIC fk_tm_divisional_status_cd string, 
# MAGIC tm_divisional_status_dt timestamp, 
# MAGIC unit_received_dt timestamp, 
# MAGIC mailroom_received_dt timestamp, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_divisional_child_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_divisional_h (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_divisional_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_document (
# MAGIC tm_document_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_document'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_document_reference (
# MAGIC fk_tm_document_id int, 
# MAGIC sequence_no int, 
# MAGIC cfk_document_id string, 
# MAGIC dn_cms_document_type_tx string, 
# MAGIC dn_cms_page_count_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_document_reference'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_drawing (
# MAGIC fk_trademark_gid string, 
# MAGIC color_in string, 
# MAGIC three_dimension_in string, 
# MAGIC color_claim_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string,
# MAGIC spcl_form_filed_3d_drawing_in string,
# MAGIC spcl_form_fild_color_dwg_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_drawing'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_drawing_h (
# MAGIC fk_trademark_gid string, 
# MAGIC color_in string, 
# MAGIC three_dimension_in string, 
# MAGIC color_claim_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string,
# MAGIC spcl_form_filed_3d_drawing_in string,
# MAGIC spcl_form_fild_color_dwg_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_drawing_h'
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_employee_assignment_h (
# MAGIC fk_trademark_gid string, 
# MAGIC action_ct string, 
# MAGIC fk_tm_employee_role_cd string, 
# MAGIC cfk_employee_no string, 
# MAGIC lock_control_no int, 
# MAGIC effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_employee_assignment_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_filing_basis(
# MAGIC   fk_trademark_gid string, 
# MAGIC   fk_filing_basis_cd string, 
# MAGIC   current_in string, 
# MAGIC   amended_in string, 
# MAGIC   filed_in string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_filing_basis'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_foreign_basis (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC foreign_tm_reg_num string, 
# MAGIC foreign_tm_appl_num string, 
# MAGIC foreign_filing_dt timestamp, 
# MAGIC country_cd string, 
# MAGIC country_nm string, 
# MAGIC foreign_registration_dt timestamp, 
# MAGIC foreign_expiration_dt timestamp, 
# MAGIC foreign_renewal_effective_dt timestamp, 
# MAGIC foreign_renewal_num string, 
# MAGIC foreign_renewal_expiration_dt timestamp, 
# MAGIC priority_claimed_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string,
# MAGIC fk_class_id int,
# MAGIC cfk_geographic_region_cd string,
# MAGIC dn_geographic_region_nm string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_foreign_basis'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_foreign_basis_h (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC foreign_tm_reg_num string, 
# MAGIC foreign_tm_appl_num string, 
# MAGIC foreign_filing_dt timestamp, 
# MAGIC country_cd string, 
# MAGIC country_nm string, 
# MAGIC foreign_registration_dt timestamp, 
# MAGIC foreign_expiration_dt timestamp, 
# MAGIC foreign_renewal_effective_dt timestamp, 
# MAGIC foreign_renewal_num string, 
# MAGIC foreign_renewal_expiration_dt timestamp, 
# MAGIC priority_claimed_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC action_ct string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp,
# MAGIC fk_class_id int,
# MAGIC cfk_geographic_region_cd string,
# MAGIC dn_geographic_region_nm string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_foreign_basis_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_gds_srvc_term_filg_basis (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC FK_GDS_SRVC_TERM_SEQUENCE_NO int, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC FK_FILING_BASIS_CD string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_gds_srvc_term_filg_basis'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_gds_srvc_term_filg_basis_h (
# MAGIC FK_TRADEMARK_GID string, 
# MAGIC ACTION_CT string, 
# MAGIC FK_CLASS_ID int, 
# MAGIC FK_GDS_SRVC_TERM_SEQUENCE_NO int, 
# MAGIC FK_FILING_BASIS_CD string, 
# MAGIC LOCK_CONTROL_NO int, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string, 
# MAGIC CFK_TRANSACTION_INSTANCE_GID string, 
# MAGIC BEGIN_EFFECTIVE_TS timestamp, 
# MAGIC END_EFFECTIVE_TS timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_gds_srvc_term_filg_basis_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_group (
# MAGIC tm_group_id int, 
# MAGIC fk_tm_group_type_cd string, 
# MAGIC cfk_owner_employee_no string, 
# MAGIC group_nm string, 
# MAGIC description_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_group_item (
# MAGIC fk_tm_group_id int, 
# MAGIC fk_trademark_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_group_item'
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_locations_h (
# MAGIC fk_trademark_gid                string,         
# MAGIC cfk_asgnd_exam_law_ofc_org_cd   string,         
# MAGIC case_reported_lost_in           string,         
# MAGIC case_reported_lost_dt           timestamp,      
# MAGIC fk_charge_to_location_cd        string,      
# MAGIC cfk_charge_to_worker_no         string,      
# MAGIC current_location_dt             timestamp,      
# MAGIC fk_current_location_cd          string,      
# MAGIC physical_location_dt            timestamp,      
# MAGIC fk_physical_location_cd         string,      
# MAGIC lock_control_no                 int,  
# MAGIC create_ts                       timestamp,  
# MAGIC create_user_id                  string,  
# MAGIC last_mod_ts                     timestamp,  
# MAGIC last_mod_user_id                string,  
# MAGIC action_ct                       string,  
# MAGIC cfk_transaction_instance_gid    string,  
# MAGIC begin_effective_ts              timestamp,  
# MAGIC end_effective_ts                timestamp,
# MAGIC official_search_in_progress_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_locations_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_mailing_addr_h(
# MAGIC   fk_tm_party_role_id int, 
# MAGIC   fk_mailing_address_gid string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   action_ct string, 
# MAGIC   primary_in string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_mailing_addr_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_mark_type (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_mark_type_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_mark_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_mark_type_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_mark_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_mark_type_h'
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
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_notification_message (
# MAGIC fk_trademark_gid string, 
# MAGIC cfk_notification_message_id int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_notification_message'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_organization_location (
# MAGIC location_id             decimal(22,0),
# MAGIC cfk_tm_organization_gid string,
# MAGIC location_cd             string,
# MAGIC location_desc_tx        string,
# MAGIC physical_location_in    string,
# MAGIC aloc_in                 string,
# MAGIC locc_in                 string,
# MAGIC lock_control_no         int,
# MAGIC create_ts               timestamp,
# MAGIC create_user_id          string,
# MAGIC last_mod_ts             timestamp,
# MAGIC last_mod_user_id        string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_organization_location'
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_physical_location (
# MAGIC fk_trademark_gid          string,       
# MAGIC physical_location_dt      timestamp,  
# MAGIC fk_physical_location_cd   string,     
# MAGIC lock_control_no           int,
# MAGIC create_ts                 timestamp,     
# MAGIC create_user_id            string,     
# MAGIC last_mod_ts               timestamp,     
# MAGIC last_mod_user_id          string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_physical_location'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_post_registration (
# MAGIC fk_trademark_gid                 string,      
# MAGIC latest_correspondence_rcvd_dt    timestamp,      
# MAGIC post_reg_principal_rgstr_in      string,         
# MAGIC post_reg_supplemental_rgstr_in   string,         
# MAGIC section_8_filed_in               string,         
# MAGIC section_8_accepted_in            string,         
# MAGIC section_8_partial_accepted_in    string,         
# MAGIC section_15_filed_in              string,         
# MAGIC section_15_ackd_in               string,         
# MAGIC section_71_filed_in              string,         
# MAGIC section_71_accepted_in           string,         
# MAGIC section_71_partial_accepted_in   string,         
# MAGIC lock_control_no                  int,  
# MAGIC create_ts                        timestamp,     
# MAGIC create_user_id                   string,      
# MAGIC last_mod_ts                      timestamp,      
# MAGIC last_mod_user_id                 string,
# MAGIC cfk_cancellation_reason_cd       string,
# MAGIC renewal_filed_in                 string,
# MAGIC post_registration_audit_in       string,
# MAGIC post_reg_audit_begin_dt          timestamp,
# MAGIC republish_section_12_in          string,
# MAGIC registration_amended_in          string     
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_post_registration'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_prior_registration (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_prior_trademark_gid string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_prior_registration'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_prior_registration_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_prior_trademark_gid string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_prior_registration_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_proceeding (
# MAGIC tm_proceeding_id decimal(22,0), 
# MAGIC fk_trademark_gid string, 
# MAGIC cfk_proceeding_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_proceeding'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_proceeding_h (
# MAGIC TM_PROCEEDING_ID decimal(22,0),
# MAGIC FK_TRADEMARK_GID string,
# MAGIC CFK_PROCEEDING_NO int,
# MAGIC LOCK_CONTROL_NO int,
# MAGIC CREATE_TS timestamp,
# MAGIC CREATE_USER_ID string,
# MAGIC LAST_MOD_TS timestamp,
# MAGIC LAST_MOD_USER_ID string,
# MAGIC CFK_TRANSACTION_INSTANCE_GID string,
# MAGIC BEGIN_EFFECTIVE_TS timestamp,
# MAGIC END_EFFECTIVE_TS timestamp,
# MAGIC ACTION_CT string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_proceeding_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_pseudo_class (
# MAGIC fk_pseudo_class_id int, 
# MAGIC fk_trademark_gid string, 
# MAGIC fk_class_id int, 
# MAGIC gds_srvc_phrase_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_pseudo_class'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_pseudo_class_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_class_id int, 
# MAGIC fk_pseudo_class_id int, 
# MAGIC gds_srvc_phrase_tx string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_pseudo_class_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_pseudo_mark (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC pseudo_mark_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_pseudo_mark'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_pseudo_mark_h (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC pseudo_mark_tx string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_pseudo_mark_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_publication (
# MAGIC fk_trademark_gid string, 
# MAGIC tm_publication_gid string,  
# MAGIC og_action_dt timestamp, 
# MAGIC legacy_og_status_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC print_mark_description_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_publication'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_publication_h (
# MAGIC fk_trademark_gid string, 
# MAGIC tm_publication_gid string, 
# MAGIC og_action_dt timestamp, 
# MAGIC legacy_og_status_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC print_mark_description_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_publication_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_publication_subct (
# MAGIC fk_tm_publication_gid string, 
# MAGIC fk_publication_category_cd string, 
# MAGIC fk_publication_subcategory_cd string, 
# MAGIC legacy_des_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_publication_subct'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_publication_subct_h (
# MAGIC fk_tm_publication_gid string, 
# MAGIC fk_publication_category_cd string, 
# MAGIC fk_publication_subcategory_cd string, 
# MAGIC legacy_des_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_publication_subct_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# Databricks notebook source
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_locations (
# MAGIC fk_trademark_gid               string,
# MAGIC cfk_asgnd_exam_law_ofc_org_cd  string,
# MAGIC case_reported_lost_in          string,
# MAGIC case_reported_lost_dt          timestamp,
# MAGIC fk_charge_to_location_cd       string,
# MAGIC cfk_charge_to_worker_no        string,
# MAGIC current_location_dt            timestamp,
# MAGIC fk_current_location_cd         string,
# MAGIC physical_location_dt           timestamp,
# MAGIC fk_physical_location_cd        string,
# MAGIC lock_control_no                int,
# MAGIC create_ts                      timestamp,
# MAGIC create_user_id                 string,
# MAGIC last_mod_ts                    timestamp,
# MAGIC last_mod_user_id               string,
# MAGIC official_search_in_progress_in string)
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_locations'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.tm_filing_basis_h(
# MAGIC fk_trademark_gid string, 
# MAGIC fk_filing_basis_cd string, 
# MAGIC current_in string, 
# MAGIC amended_in string, 
# MAGIC filed_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC action_ct string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_filing_basis_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_employee_assignment (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_employee_role_cd string, 
# MAGIC cfk_employee_no string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC effective_dt timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_employee_assignment'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_relationship (
# MAGIC fk_parent_trademark_gid string, 
# MAGIC fk_related_trademark_gid string, 
# MAGIC fk_relationship_type_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_relationship'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_itu (
# MAGIC   FK_TRADEMARK_GID STRING,
# MAGIC   AMENDMENT_TO_USE_FILED_IN	STRING,
# MAGIC   APPLICATION_MARK_IN_1	STRING,
# MAGIC   APPLICATION_MARK_IN_2	STRING,
# MAGIC   FINAL_ACTION_REFUSAL_ATU_IN	STRING,
# MAGIC   FIRST_ACTION_REFUSAL_ATU_IN	STRING,
# MAGIC   AVAILABLE_FOR_SOU_IN	STRING,
# MAGIC   EXTENSIONS_NOT_ALLOWED_IN	STRING,
# MAGIC   HOLD_FIRST_ACTION_RFSL_ATU_IN STRING,
# MAGIC   LAST_UA_TRAN_INFRML_RSP_RCV_IN	STRING,
# MAGIC   LAST_UA_TRAN_INFRML_LTR_ML_IN	STRING,
# MAGIC   ITU_CASE_PUBD_FOR_OPSTN_IN	STRING,
# MAGIC   ITU_FREEZE_PERIOD_IN	STRING,
# MAGIC   LATEST_ITU_FILNG_RECEIVED_DT	DATE,
# MAGIC   SOU_EXT_DENIAL_LTR_MAILED_IN	STRING,
# MAGIC   LAST_EXT_TRAN_DNIL_LTR_PREP_IN	STRING,
# MAGIC   LAST_POSSIBLE_EXTENSION_DT	DATE,
# MAGIC   LAST_EXT_TRAN_SOU_EXT_FILED_IN STRING,
# MAGIC   USE_AFFIDAVIT_PRCSG_COMPLT_IN	STRING,
# MAGIC   NOA_ISSUED_IN	STRING,
# MAGIC   SOU_EXTENSION_REQ_FILED_IN	STRING,
# MAGIC   NOA_MAILED_IN	STRING,
# MAGIC   POTENTIEL_ABANDONMENT_DT	TIMESTAMP,
# MAGIC   SOU_RECEIVED_DT	TIMESTAMP,
# MAGIC   LOCK_CONTROL_NO	STRING,
# MAGIC   CREATE_TS	TIMESTAMP,
# MAGIC   CREATE_USER_ID	STRING,
# MAGIC   LAST_MOD_TS	TIMESTAMP,
# MAGIC   LAST_MOD_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_itu'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC + CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_office_actions (
# MAGIC + fk_trademark_gid                 string,
# MAGIC + capture_scnd_ea_actn_cntd_in     string,
# MAGIC + examining_atty_dspl_cnt_in   string,
# MAGIC + final_refusal_in                 string,
# MAGIC + first_action_mailed_in           string,
# MAGIC + first_action_publication_in      string,
# MAGIC + first_ea_action_counted_dt       timestamp,
# MAGIC + first_ea_action_counted_in       string,
# MAGIC + first_para_action_counted_in     string,
# MAGIC + FRST_PR_PRLGL_ACTN_CNTED_DT  timestamp,
# MAGIC + hld_exmg_atty_dspl_cnt_in        string,
# MAGIC + hld_frst_exmg_at_actn_cnt_in   string,
# MAGIC + last_examiner_action_dt          timestamp,
# MAGIC + second_ea_action_counted_in      string,
# MAGIC + total_paralegal_actions_no       int,
# MAGIC + total_examiner_actions_no        int,
# MAGIC + lock_control_no                  int,
# MAGIC + create_ts                        timestamp,
# MAGIC + create_user_id                   string,
# MAGIC + last_mod_ts                      timestamp,
# MAGIC + last_mod_user_id                  string,
# MAGIC + frst_pr_paralegal_actn_cnted_dt timestamp,
# MAGIC + examining_attorney_dspl_cnt_in string,
# MAGIC + hld_frst_exmg_atty_actn_cnt_in string
# MAGIC + )
# MAGIC + USING DELTA
# MAGIC + LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_office_actions'
# MAGIC + TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_divisional_child (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_sequence_no int, 
# MAGIC fk_child_trademark_gid string, 
# MAGIC fk_tm_divisional_status_cd string, 
# MAGIC tm_divisional_status_dt timestamp, 
# MAGIC mailroom_received_dt timestamp, 
# MAGIC unit_received_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_divisional_child'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_milestone (
# MAGIC tm_milestone_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_milestone'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_party_role (
# MAGIC tm_party_role_id int, 
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_party_role_owner (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_party_role_cd string, 
# MAGIC fk_party_role_sequence_no int, 
# MAGIC fk_owner_type_id int, 
# MAGIC owner_type_sequence_no int, 
# MAGIC joint_owner_sequence_no int, 
# MAGIC reel_num_tx int, 
# MAGIC frame_num_tx string, 
# MAGIC assignment_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string,
# MAGIC legacy_assignment_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_party_role_owner'
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_electronic_addr (
# MAGIC fk_tm_party_role_id int, 
# MAGIC fk_electronic_address_gid string, 
# MAGIC authorized_email_in string, 
# MAGIC primary_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_electronic_addr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_class (
# MAGIC fk_class_id int, 
# MAGIC fk_trademark_gid string, 
# MAGIC fk_tm_class_status_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC gds_srvcs_stmnt_tx string, 
# MAGIC gds_srvcs_stmnt_annotated_tx string, 
# MAGIC first_use_in_commerce_month_no int, 
# MAGIC first_use_in_commerce_day_no int, 
# MAGIC first_use_in_commerce_year_no int, 
# MAGIC first_use_anywhere_month_no int, 
# MAGIC first_use_anywhere_day_no int, 
# MAGIC first_use_anywhere_year_no int, 
# MAGIC intent_to_use_dt timestamp, 
# MAGIC status_dt timestamp,
# MAGIC excessive_character_fee_cnt_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_class'
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
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.use_in_another_form (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_class_id int, 
# MAGIC fk_class_statement_type_cd string, 
# MAGIC preformatted_text_in string, 
# MAGIC first_use_month_no int, 
# MAGIC first_use_day_no int, 
# MAGIC first_use_year_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC statement_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/use_in_another_form'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.attorney_hold (
# MAGIC   FK_WORK_ITEM_GID	STRING,
# MAGIC   PLACED_ON_HOLD_DT	TIMESTAMP,
# MAGIC   LAST_ACTION_DT	TIMESTAMP,
# MAGIC   CFK_HOLD_WORKER_GID	STRING,
# MAGIC   CFK_HOLD_USER_ROLE_ID	INTEGER,
# MAGIC   CFK_HOLD_STATUS_CD	STRING,
# MAGIC   CFK_HOLD_TM_ORGANIZATION_GID	STRING,
# MAGIC   CFK_HOLD_CATEGORY_CD	STRING,
# MAGIC   HOLD_DOCKET_NO	INTEGER,
# MAGIC   CFK_LAST_ACTION_WORKER_GID	STRING,
# MAGIC   CFK_LAST_ACTION_USER_ROLE_ID	INTEGER,
# MAGIC   CFK_LAST_ACTION_TM_ORG_GID	STRING,
# MAGIC   LOCK_CONTROL_NO	INTEGER,
# MAGIC   CREATE_TS	TIMESTAMP,
# MAGIC   CREATE_USER_ID STRING,
# MAGIC   LAST_MOD_TS	TIMESTAMP,
# MAGIC   LAST_MOD_USER_ID STRING,
# MAGIC   DN_HOLD_WORKER_NO STRING,
# MAGIC   DN_LAST_ACTION_WORKER_NO STRING,
# MAGIC   DN_SERIAL_NUM_TX STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/attorney_hold'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

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
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.international_reg_tm (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_international_reg_gid string, 
# MAGIC status_cd string, 
# MAGIC status_dt timestamp, 
# MAGIC priority_claimed_dt timestamp, 
# MAGIC auto_protect_dt timestamp, 
# MAGIC notification_dt timestamp, 
# MAGIC cancellation_dt timestamp, 
# MAGIC first_refusal_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC ib_renewal_dt timestamp, 
# MAGIC ib_publication_dt timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/international_reg_tm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.electronic_address (
# MAGIC electronic_address_gid string, 
# MAGIC electronic_addr_locator_tx string, 
# MAGIC fk_electronic_addr_type_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/electronic_address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.business_event(
# MAGIC   business_event_id int, 
# MAGIC   cfk_domain_cd string, 
# MAGIC   fk_object_type_cd string, 
# MAGIC   cfk_object_gid string, 
# MAGIC   order_no int, 
# MAGIC   effective_ts timestamp, 
# MAGIC   fk_business_event_reason_id int, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   cfk_fsm_instance_h_id int, 
# MAGIC   cfk_proceeding_no int, 
# MAGIC   document_id string, 
# MAGIC   paper_in string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/business_event'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_business_event_reason (
# MAGIC fk_business_event_rsn_ct_cd string, 
# MAGIC business_event_reason_id int, 
# MAGIC business_event_reason_cd string, 
# MAGIC tm_milestone_in string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC legacy_cm_ent_cd string, 
# MAGIC legacy_cm_ent_type_cd string, 
# MAGIC cfk_fsm_type_event_id int, 
# MAGIC prosecution_history_in string, 
# MAGIC alert_trigger_ct string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_business_event_reason'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_class (
# MAGIC class_id int, 
# MAGIC fk_class_schedule_cd string, 
# MAGIC class_no string, 
# MAGIC modification_no int, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC intl_class_short_title_tx string, 
# MAGIC intl_class_explanatory_note_tx string, 
# MAGIC intl_class_inclusions_tx string, 
# MAGIC intl_class_exclusions_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC goods_services_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_class'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

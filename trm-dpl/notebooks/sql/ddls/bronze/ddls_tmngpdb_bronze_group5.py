# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_evidence_source_category (
# MAGIC evidence_source_category_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_evidence_source_category'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_fee_process_type (
# MAGIC fee_process_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_fee_process_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_filing_basis (
# MAGIC filing_basis_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_filing_basis'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_fsm_category (
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC fsm_category_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_fsm_category'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_fsm_state_legacy_state (
# MAGIC cfk_fsm_type_state_id int, 
# MAGIC examination_no int, 
# MAGIC legacy_state_type_ct string, 
# MAGIC legacy_state_no int, 
# MAGIC fk_office_activity_reason_cd string, 
# MAGIC stnd_fsm_state_legacy_state_id int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_fsm_state_legacy_state'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_fsm_type (
# MAGIC fsm_type_id int, 
# MAGIC fk_precedent_fsm_type_id int, 
# MAGIC fk_initial_fsm_type_state_id int, 
# MAGIC fk_root_fsm_type_id int, 
# MAGIC fk_domain_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC fk_fsm_category_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_fsm_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_fsm_type_event (
# MAGIC fsm_type_event_id int, 
# MAGIC fk_fsm_type_id int, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_fsm_type_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_fsm_type_state (
# MAGIC fsm_type_state_id int, 
# MAGIC fk_fsm_type_id int, 
# MAGIC fk_root_fsm_type_id int, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC human_activity_tx string, 
# MAGIC automated_activity_tx string, 
# MAGIC start_condition_tx string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC state_end_in string, 
# MAGIC state_start_in string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_fsm_type_state'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_fsm_type_state_rule (
# MAGIC fsm_type_state_rule_id int, 
# MAGIC fk_fsm_type_id int, 
# MAGIC fk_root_fsm_type_id int, 
# MAGIC fk_current_fsm_type_state_id int, 
# MAGIC fk_next_fsm_type_state_id int, 
# MAGIC fk_fsm_type_event_id int, 
# MAGIC description_tx string, 
# MAGIC precondition_tx string, 
# MAGIC rule_action_tx string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_fsm_type_state_rule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_gds_srvc_annotn_stat (
# MAGIC gds_srvc_annotn_status_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_gds_srvc_annotn_stat'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_gds_srvc_match_stat (
# MAGIC gds_srvc_match_stat_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_gds_srvc_match_stat'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_gds_srvc_status (
# MAGIC gds_srvc_status_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_gds_srvc_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_ground (
# MAGIC ground_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC sort_order_no int, 
# MAGIC grouping_no int, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC fk_ground_type_cd string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_ground'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_ground_type (
# MAGIC ground_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_ground_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_intrstd_party_rltnsp_type (
# MAGIC intrstd_party_reltnsp_type_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC individual_to_individual_in string, 
# MAGIC organization_to_individual_in string, 
# MAGIC organization_to_org_in string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_intrstd_party_rltnsp_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_legacy_status (
# MAGIC STATUS_NO int,  
# MAGIC DESCRIPTION_TX string,  
# MAGIC TRAM_STATE string, 
# MAGIC TM5_STAT_DESC string, 
# MAGIC TM5_COMMON_STATUS_CD int, 
# MAGIC TM5_COMMON_STAT_DESCRIPTOR_TX string, 
# MAGIC TM5_COMMON_STAT_DEFINITION_TX string, 
# MAGIC TM5_LIVE_DEAD_CT string, 
# MAGIC BEGIN_EFFECTIVE_DT timestamp,  
# MAGIC END_EFFECTIVE_DT timestamp, 
# MAGIC CREATE_TS timestamp,  
# MAGIC CREATE_USER_ID string,  
# MAGIC LAST_MOD_TS timestamp,  
# MAGIC LAST_MOD_USER_ID string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_legacy_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_legacy_transaction (
# MAGIC legacy_transaction_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_legacy_transaction'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_legal_entity_type (
# MAGIC legal_entity_type_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC legal_entity_ct string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_legal_entity_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_mad_birth_rec_ct_type  (
# MAGIC MAD_BIRTH_REC_CT_TYPE_CD string, 
# MAGIC TITLE_TX string, 
# MAGIC DESCRIPTION_TX string, 
# MAGIC BEGIN_EFFECTIVE_DT timestamp, 
# MAGIC END_EFFECTIVE_DT timestamp, 
# MAGIC CREATE_TS timestamp, 
# MAGIC CREATE_USER_ID string, 
# MAGIC LAST_MOD_TS timestamp, 
# MAGIC LAST_MOD_USER_ID string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_mad_birth_rec_ct_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_mad_transaction_type (
# MAGIC mad_transaction_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_mad_transaction_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_mark_drawing_type (
# MAGIC mark_drawing_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_mark_drawing_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_mark_type (
# MAGIC mark_type_cd string, 
# MAGIC display_order_no int, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_mark_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_myuspto_event (
# MAGIC event_cd string,
# MAGIC event_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_myuspto_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_note_type (
# MAGIC note_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_note_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_object_dispatch_type (
# MAGIC object_dispatch_type_cd string, 
# MAGIC object_dispatch_type_ct string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_object_dispatch_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_object_type (
# MAGIC object_type_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC global_identifier_prefix_tx string, 
# MAGIC object_type_id_ct string, 
# MAGIC table_name_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_object_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_office_action_category (
# MAGIC office_action_category_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_office_action_category'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_office_action_ct_state (
# MAGIC fk_office_action_category_cd string, 
# MAGIC cfk_fsm_type_state_id int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_office_action_ct_state'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_office_action_rule (
# MAGIC fk_work_item_type_cd string, 
# MAGIC fk_office_action_category_cd string, 
# MAGIC typical_ct string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_office_action_rule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_office_activity_reason (
# MAGIC office_activity_reason_cd string, 
# MAGIC fk_office_actvty_rsn_ct_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_office_activity_reason'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_office_actn_rule_itm (
# MAGIC office_actn_rule_itm_id int, 
# MAGIC fk_work_item_type_cd string, 
# MAGIC fk_office_action_category_cd string, 
# MAGIC item_no int, 
# MAGIC rule_nm string, 
# MAGIC rule_condition_tx string, 
# MAGIC ready_to_send_in string, 
# MAGIC editable_in string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_office_actn_rule_itm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_office_actvty_rsn_ct (
# MAGIC office_actvty_rsn_ct_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_office_actvty_rsn_ct'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_owner_type (
# MAGIC owner_type_id int, 
# MAGIC owner_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_owner_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_pay_period (
# MAGIC period_no int, 
# MAGIC calendar_year_no int, 
# MAGIC fiscal_year_no int, 
# MAGIC fiscal_quarter_no int, 
# MAGIC period_start_dt timestamp, 
# MAGIC period_end_dt timestamp, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_pay_period'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_prcdng_empe_asgmt_role (
# MAGIC prcdng_employee_role_cd  string,
# MAGIC title_tx                 string,
# MAGIC description_tx           string,
# MAGIC begin_effective_dt       timestamp,
# MAGIC end_effective_dt         timestamp,
# MAGIC create_ts                timestamp,
# MAGIC create_user_id           string,
# MAGIC last_mod_ts              timestamp,
# MAGIC last_mod_user_id         string 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_prcdng_empe_asgmt_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_publication_category (
# MAGIC publication_category_cd string, 
# MAGIC description_tx string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_publication_category'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_publication_subcategory (
# MAGIC fk_publication_category_cd string, 
# MAGIC publication_subcategory_cd string, 
# MAGIC description_tx string, 
# MAGIC reason_for_pub_lvl1 string, 
# MAGIC reason_for_pub_lvl2 string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_publication_subcategory'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_query_review_status (
# MAGIC query_review_status_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_query_review_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_reg_stmnt_type (
# MAGIC reg_stmnt_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_reg_stmnt_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_relationship_type (
# MAGIC relationship_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_relationship_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_response_issue (
# MAGIC response_issue_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_response_issue'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_review_issue (
# MAGIC fk_parent_review_issue_cd string, 
# MAGIC review_issue_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC type_ct string, 
# MAGIC hierarchy_level_ct string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC review_type_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_review_issue'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_review_rating (
# MAGIC review_rating_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_review_rating'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_statement_type (
# MAGIC statement_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_statement_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_submission_method (
# MAGIC submission_method_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_submission_method'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_telecom_format (
# MAGIC telecom_format_cd string, 
# MAGIC title_tx string, 
# MAGIC description_tx string, 
# MAGIC country_cd string, 
# MAGIC country_nm string, 
# MAGIC begin_effective_dt timestamp, 
# MAGIC end_effective_dt timestamp, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_telecom_format'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_telecom_type (
# MAGIC telecom_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_telecom_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_template_para_type (
# MAGIC template_para_type_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_template_para_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_amendment_reason (
# MAGIC tm_amendment_reason_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_amendment_reason'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_tm_class_status (
# MAGIC tm_class_status_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_tm_class_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

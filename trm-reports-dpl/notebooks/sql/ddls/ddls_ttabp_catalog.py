# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = f"../../config/{dbx_env}/{config_file_name}"
if dbx_env == "qa":
    dbx_env = "test"
print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
database = 'bronze'
trgt_catalog = common_configs["schema"]["trgt_catalog"]
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set("conf.catalog", trgt_catalog)
spark.conf.set("conf.database", database)
spark.conf.set("conf.dbx_env", dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.cdc_batch_job_control (
# MAGIC catalog_name STRING,
# MAGIC database_name STRING,
# MAGIC group_name STRING,
# MAGIC table_name STRING,
# MAGIC source_db_name STRING,
# MAGIC source_table_name STRING,
# MAGIC primary_keys STRING,
# MAGIC full_load STRING,
# MAGIC initial_load_finished BOOLEAN
# MAGIC )USING delta
# MAGIC PARTITIONED BY (group_name)
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/cdc_batch_job_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.address (
# MAGIC identifier INT,
# MAGIC role_type STRING,
# MAGIC name STRING,
# MAGIC company STRING,
# MAGIC street_line_1_tx STRING,
# MAGIC city STRING,
# MAGIC state_abbreviation STRING,
# MAGIC country_code STRING,
# MAGIC postal_code STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_partyidentifier INT,
# MAGIC primary_email_tx STRING,
# MAGIC fax_number STRING,
# MAGIC telephone_no STRING,
# MAGIC secondary_email_tx STRING,
# MAGIC street_line_2_tx STRING,
# MAGIC bar_membership_id STRING,
# MAGIC bar_member_admission_year INT,
# MAGIC fk_bar_jurisdiction_region_cd STRING,
# MAGIC bar_active_member_in STRING,
# MAGIC attorney_docket_id STRING,
# MAGIC email_address STRING,
# MAGIC street STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/address'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.address_hist (
# MAGIC address_hist_id INT,
# MAGIC fk_identifier INT,
# MAGIC role_type STRING,
# MAGIC name STRING,
# MAGIC company STRING,
# MAGIC street_line_1_tx STRING,
# MAGIC city STRING,
# MAGIC state_abbreviation STRING,
# MAGIC country_code STRING,
# MAGIC postal_code STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_partyidentifier INT,
# MAGIC primary_email_tx STRING,
# MAGIC fax_number STRING,
# MAGIC telephone_no STRING,
# MAGIC secondary_email_tx STRING,
# MAGIC street_line_2_tx STRING,
# MAGIC bar_membership_id STRING,
# MAGIC bar_member_admission_year INT,
# MAGIC fk_bar_jurisdiction_region_cd STRING,
# MAGIC bar_active_member_in STRING,
# MAGIC attorney_docket_id STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC system_authentication_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/address_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.application_status_reference (
# MAGIC application_status_code STRING,
# MAGIC application_status_text STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/application_status_reference'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.bi_week (
# MAGIC bi_week_id INT,
# MAGIC start_dt TIMESTAMP,
# MAGIC end_dt TIMESTAMP,
# MAGIC quarter_no INT,
# MAGIC fiscal_year_no INT,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/bi_week'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.board_decision (
# MAGIC class_cd STRING,
# MAGIC ttab_action_cd STRING,
# MAGIC ttab_action_dt TIMESTAMP,
# MAGIC dn_prop_ref_ser_num INT,
# MAGIC dn_prop_ref_reg_num INT,
# MAGIC fk_prop_id INT,
# MAGIC sequence_no INT,
# MAGIC dn_prcdng_no INT,
# MAGIC dn_prcdng_type_cd STRING,
# MAGIC informational_tx STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/board_decision'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.contested_motion (
# MAGIC cm_id INT,
# MAGIC fk_prcdng_proceeding_number0 INT,
# MAGIC fk_prcdng_type STRING,
# MAGIC fk_empe_number0_attorney INT,
# MAGIC fk_empe_number0_writer INT,
# MAGIC decision_ready_dt TIMESTAMP,
# MAGIC decision_completed_dt TIMESTAMP,
# MAGIC motion_addressed_qt INT,
# MAGIC assigned_dt TIMESTAMP,
# MAGIC due_dt TIMESTAMP,
# MAGIC last_modified_user_id STRING,
# MAGIC last_modified_ts TIMESTAMP,
# MAGIC telephone_comference_in STRING,
# MAGIC motion_comment_tx STRING,
# MAGIC created_dt TIMESTAMP,
# MAGIC deleted_in STRING,
# MAGIC fk_sm_id INT,
# MAGIC archived_in STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/contested_motion'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.document_detail (
# MAGIC page_no INT,
# MAGIC file_nm STRING,
# MAGIC file_size_qt INT,
# MAGIC last_modified_user_id STRING,
# MAGIC last_modified_ts TIMESTAMP,
# MAGIC fk_ogc_document_id INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/document_detail'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.efiling_session (
# MAGIC efiling_session_id INT,
# MAGIC session_id STRING,
# MAGIC cfk_proceeding_no INT,
# MAGIC cfk_proceeding_type STRING,
# MAGIC session_xml_doc STRING,
# MAGIC lock_session_in STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC lock_control_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/efiling_session'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.efiling_session_archv (
# MAGIC efiling_session_archv_id INT,
# MAGIC session_id STRING,
# MAGIC cfk_proceeding_no INT,
# MAGIC cfk_proceeding_type STRING,
# MAGIC session_xml_doc STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC archv_create_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/efiling_session_archv'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.efiling_session_email (
# MAGIC fk_efiling_session_id INT,
# MAGIC sequence_no INT,
# MAGIC email_address_tx STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC lock_control_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/efiling_session_email'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.efiling_session_email_archv (
# MAGIC fk_efiling_session_archv_id INT,
# MAGIC sequence_no INT,
# MAGIC email_address_tx STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC archv_create_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/efiling_session_email_archv'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.employee (
# MAGIC number0 INT,
# MAGIC given_name STRING,
# MAGIC family_name STRING,
# MAGIC role STRING,
# MAGIC team_code STRING,
# MAGIC signature_initials STRING,
# MAGIC interparte_tdr_high INT,
# MAGIC interparte_tdr_low INT,
# MAGIC potential_tdr_high INT,
# MAGIC potential_tdr_low INT,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC email_address STRING,
# MAGIC part_time STRING,
# MAGIC send_to_in STRING,
# MAGIC employee_start_dt TIMESTAMP,
# MAGIC employee_end_dt TIMESTAMP,
# MAGIC fk_employee_access_type_cd STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/employee'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.employee_hist (
# MAGIC employee_hist_id INT,
# MAGIC fk_employee_no INT,
# MAGIC given_name STRING,
# MAGIC family_name STRING,
# MAGIC role STRING,
# MAGIC team_code STRING,
# MAGIC signature_initials STRING,
# MAGIC interparte_tdr_high INT,
# MAGIC interparte_tdr_low INT,
# MAGIC potential_tdr_high INT,
# MAGIC potential_tdr_low INT,
# MAGIC prev_update_user_id STRING,
# MAGIC prev_update_dt TIMESTAMP,
# MAGIC email_address STRING,
# MAGIC part_time STRING,
# MAGIC send_to_in STRING,
# MAGIC employee_start_dt TIMESTAMP,
# MAGIC employee_end_dt TIMESTAMP,
# MAGIC fk_employee_access_type_cd STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC system_authentication_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/employee_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.employee_proceeding (
# MAGIC identifier INT,
# MAGIC date_assigned TIMESTAMP,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_proceedingnumber0 INT,
# MAGIC fk_employeenumber0 INT,
# MAGIC fk_proceedingtype STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/employee_proceeding'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.employee_unavailability (
# MAGIC employee_unavailability_id INT,
# MAGIC begin_dt DATE,
# MAGIC end_dt DATE,
# MAGIC comment_tx STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP,
# MAGIC fk_employee_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/employee_unavailability'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.entry_information (
# MAGIC entry_code STRING,
# MAGIC type STRING,
# MAGIC status STRING,
# MAGIC days_due INT,
# MAGIC exparte_appl_valid_indicator STRING,
# MAGIC concurrent_valid_indicator STRING,
# MAGIC cancellation_valid_indicator STRING,
# MAGIC opposition_valid_indicator STRING,
# MAGIC extension_valid_indicator STRING,
# MAGIC text STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC board_decision_cd STRING,
# MAGIC incoming_correspondence_in STRING,
# MAGIC outgoing_correspondence_in STRING,
# MAGIC miscellaneous_valid_in STRING,
# MAGIC fk_document_type_cd STRING,
# MAGIC fk_proceeding_status_cd STRING,
# MAGIC proc_stat_update_allowed_in STRING,
# MAGIC tm_cm_entry_cd STRING,
# MAGIC tm_cm_entry_type_cd STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/entry_information'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.event_log (
# MAGIC event_log_id INT,
# MAGIC event_type_nm STRING,
# MAGIC event_source_nm STRING,
# MAGIC message_tx STRING,
# MAGIC event_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/event_log'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.extend_time_grnd_proc_hist (
# MAGIC extend_time_grnd_proc_hist_id INT,
# MAGIC fk_prosecution_hist_evnt_id INT,
# MAGIC fk_proceeding_no INT,
# MAGIC fk_proceeding_type_cd STRING,
# MAGIC fk_extend_date_ground_id INT,
# MAGIC other_ground_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/extend_time_grnd_proc_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ground_rule_assoc (
# MAGIC ground_rule_id INT,
# MAGIC fk_ground_id INT,
# MAGIC fk_rule_id INT,
# MAGIC princ_flg_lte_3_year_in STRING,
# MAGIC princ_flg_gt_3_lte_5_year_in STRING,
# MAGIC princ_flg_gt_5_year_in STRING,
# MAGIC sup_flg_lte_3_year_in STRING,
# MAGIC sup_flg_gt_3_year_in STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ground_rule_assoc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.location_reference (
# MAGIC location_cd STRING,
# MAGIC location_text STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/location_reference'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.paralegal_assignment_rule (
# MAGIC paralegal_assignment_rule_id INT,
# MAGIC fk_paralegal_employee_no INT,
# MAGIC begin_range_no INT,
# MAGIC end_range_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/paralegal_assignment_rule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.party (
# MAGIC identifier INT,
# MAGIC role STRING,
# MAGIC name STRING,
# MAGIC company STRING,
# MAGIC organization STRING,
# MAGIC granted_to_date DATE,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_proceedingnumber0 INT,
# MAGIC fk_proceedingtype STRING,
# MAGIC objection_filed_in STRING,
# MAGIC represented_by_attorney_in STRING,
# MAGIC bypass_bar_info_validation_in STRING,
# MAGIC party_type_cd STRING,
# MAGIC fk_stnd_entity_type_id INT,
# MAGIC entity_tx STRING,
# MAGIC company_loc_state_cd STRING,
# MAGIC company_loc_country_cd STRING,
# MAGIC individual_nationality_cd STRING
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/party'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.party_granted_to_date_hist (
# MAGIC PARTY_GRANTED_TO_DT_HIST_ID INT,
# MAGIC FK_PARTY_ID INT,
# MAGIC PREVIOUS_GRANTED_TO_DT TIMESTAMP,
# MAGIC CURRENT_GRANTED_TO_DT TIMESTAMP,
# MAGIC PREV_UPDATE_USER_ID STRING,
# MAGIC PREV_UPDATE_DT TIMESTAMP,
# MAGIC CREATE_USER_ID STRING,
# MAGIC CREATE_TS TIMESTAMP,
# MAGIC SYSTEM_AUTHENTICATION_USER_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/party_granted_to_date_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.party_hist (
# MAGIC party_hist_id INT,
# MAGIC fk_identifier INT,
# MAGIC role STRING,
# MAGIC name STRING,
# MAGIC company STRING,
# MAGIC organization STRING,
# MAGIC granted_to_date TIMESTAMP,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_proceedingnumber0 INT,
# MAGIC fk_proceedingtype STRING,
# MAGIC objection_filed_in STRING,
# MAGIC represented_by_attorney_in STRING,
# MAGIC bypass_bar_info_validation_in STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC system_authentication_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/party_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.party_type (
# MAGIC pt_id INT,
# MAGIC party_type_nm STRING,
# MAGIC display_pro_se_in STRING,
# MAGIC display_rqstr_in STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/party_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.point_log (
# MAGIC point_log_id INT,
# MAGIC fk_pnt_log_type_pnt_type_id INT,
# MAGIC point_qt DOUBLE,
# MAGIC hour_qt DOUBLE,
# MAGIC comment_tx STRING,
# MAGIC assigned_dt TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC delete_in STRING,
# MAGIC fk_employee_no INT,
# MAGIC fk_party_type_id INT,
# MAGIC fk_requester_employee_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/point_log'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.point_log_prcdng_party (
# MAGIC point_log_prcdng_party_id INT,
# MAGIC fk_point_log_proceeding_id INT,
# MAGIC fk_party_id INT,
# MAGIC entry_no STRING,
# MAGIC dn_party_nm STRING,
# MAGIC dn_address_street_tx STRING,
# MAGIC dn_address_city_nm STRING,
# MAGIC dn_address_state_cd STRING,
# MAGIC dn_address_country_cd STRING,
# MAGIC dn_address_postal_cd STRING,
# MAGIC dn_address_telephone_no STRING,
# MAGIC exhibit_return_in STRING,
# MAGIC exhibit_discard_in STRING,
# MAGIC confidential_in STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC delete_in STRING,
# MAGIC dn_address_street_2_tx STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/point_log_prcdng_party'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.point_log_prcdng_prop (
# MAGIC point_log_prcdng_prop_id INT,
# MAGIC fk_point_log_proceeding_id INT,
# MAGIC fk_property_id INT,
# MAGIC dn_registration_no INT,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC delete_in STRING,
# MAGIC dn_serial_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/point_log_prcdng_prop'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.point_log_proceeding (
# MAGIC point_log_proceeding_id INT,
# MAGIC fk_proceeding_no INT,
# MAGIC fk_proceeding_type_cd STRING,
# MAGIC fk_point_log_id INT,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC delete_in STRING,
# MAGIC proceeding_entry_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/point_log_proceeding'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.prcdng_dscvry_conf (
# MAGIC pdc_id INT,
# MAGIC fk_prcdng_number0 INT,
# MAGIC fk_prcdng_type STRING,
# MAGIC fk_empe_number0 INT,
# MAGIC fk_pt_rqstng_pt_id INT,
# MAGIC fk_pt_pro_se_pt_id INT,
# MAGIC conference_dt TIMESTAMP,
# MAGIC duration_qt INT,
# MAGIC mailing_dt TIMESTAMP,
# MAGIC comment_tx STRING,
# MAGIC fk_spdcs_id INT,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP,
# MAGIC deleted_in STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/prcdng_dscvry_conf'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.prcdng_dscvry_conf_agrmt (
# MAGIC fk_pdc_id INT,
# MAGIC fk_prcdng_number0 INT,
# MAGIC fk_prcdng_type STRING,
# MAGIC fk_sdca_id INT,
# MAGIC agreement_tx STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP,
# MAGIC deleted_in STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/prcdng_dscvry_conf_agrmt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.prcdng_dscvry_conf_claim (
# MAGIC fk_pdc_id INT,
# MAGIC fk_prcdng_number0 INT,
# MAGIC fk_prcdng_type STRING,
# MAGIC fk_sdcc_id INT,
# MAGIC claims_tx STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP,
# MAGIC deleted_in STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/prcdng_dscvry_conf_claim'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.proc_type_ground_rule (
# MAGIC proc_type_ground_rule_id INT,
# MAGIC estta_form_cd STRING,
# MAGIC proceeding_type_cd STRING,
# MAGIC ground_rule_display_seq_no INT,
# MAGIC fk_ground_rule_id INT,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/proc_type_ground_rule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.proceeding (
# MAGIC number0 INT,
# MAGIC type STRING,
# MAGIC callup_date DATE,
# MAGIC ttab_location STRING,
# MAGIC ttab_status STRING,
# MAGIC ttab_status_date DATE,
# MAGIC ttab_date_in_location DATE,
# MAGIC ttab_charged_to_location STRING,
# MAGIC ttab_charged_employee_no INT,
# MAGIC ttab_filing_date DATE,
# MAGIC number_of_parties INT,
# MAGIC purge_indicator STRING,
# MAGIC purge_reason STRING,
# MAGIC informational_text STRING,
# MAGIC lock_flag STRING,
# MAGIC rpt_date_mailed DATE,
# MAGIC rpt_date_out_queue DATE,
# MAGIC rpt_date_in_queue DATE,
# MAGIC rpt_decision_writer STRING,
# MAGIC rpt_ttab_panel STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC twtf_lst_upd_timestamp TIMESTAMP,
# MAGIC int_attorney_num INT,
# MAGIC fk_paralegal_employee_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/proceeding'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.proceeding_schedule (
# MAGIC proceeding_schedule_id INT,
# MAGIC fk_proceeding_no INT,
# MAGIC fk_proceeding_type STRING,
# MAGIC schedule_event_dt TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC delete_in STRING,
# MAGIC fk_proceeding_schedule_id INT,
# MAGIC fk_prcdng_schedule_tmplt_id INT,
# MAGIC schedule_sequence_no INT,
# MAGIC proceeding_event_nm STRING,
# MAGIC proposed_schedule_event_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/proceeding_schedule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.proceeding_status (
# MAGIC code STRING,
# MAGIC status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/proceeding_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.proceeding_status_hist (
# MAGIC proceeding_status_hist_id INT,
# MAGIC fk_proceedingnumber0 INT,
# MAGIC fk_proceedingtype STRING,
# MAGIC fk_identifier INT,
# MAGIC entry_cd STRING,
# MAGIC entry_tx STRING,
# MAGIC fk_ttab_status_code STRING,
# MAGIC status_effective_dt TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC system_authentication_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/proceeding_status_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.proceeding_trial_event (
# MAGIC proceeding_trial_event_id INT,
# MAGIC fk_number0 INT,
# MAGIC fk_type STRING,
# MAGIC fk_trial_event_id INT,
# MAGIC proceeding_trial_event_dt TIMESTAMP,
# MAGIC last_modified_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/proceeding_trial_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.proceeding_trial_event_hist (
# MAGIC proceeding_trial_event_id INT,
# MAGIC fk_number0 INT,
# MAGIC fk_type STRING,
# MAGIC fk_trial_event_id INT,
# MAGIC last_modified_user_id STRING,
# MAGIC last_update_ts TIMESTAMP,
# MAGIC proceeding_trial_event_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/proceeding_trial_event_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.property (
# MAGIC identifier INT,
# MAGIC mark_explanation STRING,
# MAGIC ref_serial_number INT,
# MAGIC ref_reg_number INT,
# MAGIC ref_opposition_number INT,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_partyidentifier INT,
# MAGIC common_law_mark_in STRING,
# MAGIC common_law_mark_type_cd STRING,
# MAGIC ref_tma_proceeding_number STRING,
# MAGIC tma_proceeding_type_cd STRING,
# MAGIC trademark_global_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/property'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.property_filing_type (
# MAGIC property_filing_type_id INT,
# MAGIC fk_property_identifer INT,
# MAGIC property_filing_type_cd STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/property_filing_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.property_good_service (
# MAGIC property_good_service_id INT,
# MAGIC fk_property_filing_type_id INT,
# MAGIC dn_class_cd STRING,
# MAGIC first_use_month_no STRING,
# MAGIC first_use_day_no STRING,
# MAGIC first_use_yr_no STRING,
# MAGIC first_use_in_com_month_no STRING,
# MAGIC first_use_in_com_day_no STRING,
# MAGIC first_use_in_com_yr_no STRING,
# MAGIC select_good_service_cd STRING,
# MAGIC select_good_service_tx STRING,
# MAGIC initial_good_service_tx STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC first_use_dt STRING,
# MAGIC first_use_in_commerce_dt STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/property_good_service'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.property_ground (
# MAGIC property_ground_id INT,
# MAGIC fk_property_filing_type_id INT,
# MAGIC select_ground_tx STRING,
# MAGIC select_rule_tx STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/property_ground'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.prosecution_history_event (
# MAGIC identifier INT,
# MAGIC entry_code STRING,
# MAGIC entry_date TIMESTAMP,
# MAGIC date_due TIMESTAMP,
# MAGIC exhibits_included_indicator STRING,
# MAGIC confidential_indicator STRING,
# MAGIC text STRING,
# MAGIC object_id STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_proceedingnumber0 INT,
# MAGIC fk_entry_informentry_code STRING,
# MAGIC entry_num INT,
# MAGIC fk_proceedingtype STRING,
# MAGIC estta_id STRING,
# MAGIC internal_comment_tx STRING,
# MAGIC external_court_nm STRING,
# MAGIC external_case_no STRING,
# MAGIC trial_extension_days_qt INT,
# MAGIC trial_suspension_days_qt INT,
# MAGIC motion_pending_in STRING,
# MAGIC fk_document_type_cd STRING,
# MAGIC last_mod_doc_type_cd_ts TIMESTAMP,
# MAGIC proceeding_resume_dt TIMESTAMP,
# MAGIC defendant_has_email_in STRING,
# MAGIC fk_ext_of_time_type_id INT,
# MAGIC relinquishment_attachment_in STRING,
# MAGIC fk_party_id INT,
# MAGIC plaintiff_has_email_in STRING,
# MAGIC fk_property_identifer INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/prosecution_history_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.quality_review (
# MAGIC quality_review_id INT,
# MAGIC fk_quality_review_category_id INT,
# MAGIC fk_reviewer_employee_no INT,
# MAGIC fk_proceeding_no INT,
# MAGIC fk_min_qlty_rvw_finding_id INT,
# MAGIC minimal_impact_citation_tx STRING,
# MAGIC fk_mdrt_qlty_rvw_finding_id INT,
# MAGIC moderate_impact_citation_tx STRING,
# MAGIC fk_major_qlty_rvw_finding_id INT,
# MAGIC major_impact_citation_tx STRING,
# MAGIC case_score_qt INT,
# MAGIC fk_bi_week_id DOUBLE,
# MAGIC reviewed_dt TIMESTAMP,
# MAGIC processed_dt TIMESTAMP,
# MAGIC notification_dt TIMESTAMP,
# MAGIC logged_dt TIMESTAMP,
# MAGIC fk_quality_review_attn_src_id INT,
# MAGIC other_finding_in STRING,
# MAGIC other_finding_citation_tx STRING,
# MAGIC deleted_in STRING,
# MAGIC fk_reviewee_employee_no INT,
# MAGIC reviewed_in STRING,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC service_request_no DECIMAL,
# MAGIC fk_employee_current_role_cd STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC comments_tx STRING,
# MAGIC completed_in STRING,
# MAGIC completed_findings_dt TIMESTAMP, 
# MAGIC completed_dt TIMESTAMP,
# MAGIC findings_comments_tx STRING,
# MAGIC completed_findings_in STRING,
# MAGIC fk_proceeding_type_cd STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/quality_review'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.quality_review_finding_role (
# MAGIC quality_review_finding_role_id INT,
# MAGIC fk_role_cd STRING,
# MAGIC fk_quality_review_finding_id INT,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/quality_review_finding_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.quality_rvw_attn_src_role (
# MAGIC quality_rvw_attn_src_role_id INT,
# MAGIC fk_role_cd STRING,
# MAGIC fk_quality_review_attn_src_id INT,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/quality_rvw_attn_src_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.report_log (
# MAGIC rl_id INT,
# MAGIC fk_rp_id INT,
# MAGIC report_status_cd STRING,
# MAGIC run_dt TIMESTAMP,
# MAGIC report_file_path_tx STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/report_log'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.report_parameter (
# MAGIC RP_ID INT,
# MAGIC REPORT_NM STRING,
# MAGIC REPORT_FILE_NM STRING,
# MAGIC FK_SRF_REPORT_FREQUENCY_CD INT,
# MAGIC REPORT_RUN_TYPE_CD INT,
# MAGIC LAST_RUN_DT TIMESTAMP,
# MAGIC LAST_UPDATE_USER_ID STRING,
# MAGIC LAST_UPDATE_TS TIMESTAMP,
# MAGIC DELETED_IN STRING,
# MAGIC REPORT_FOLDER_NM STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/report_parameter'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.role_report_group (
# MAGIC fk_srg_id INT,
# MAGIC fk_sr_role_cd STRING,
# MAGIC report_available_in STRING,
# MAGIC sort_order_no INT,
# MAGIC last_modified_user_id STRING,
# MAGIC last_modified_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/role_report_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.rpt_activity (
# MAGIC identifier INT,
# MAGIC activity_date TIMESTAMP,
# MAGIC processed_cnt INT,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_rpt_eventsidentifier INT,
# MAGIC fk_employee_proidentifier INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/rpt_activity'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.rpt_events (
# MAGIC identifier INT,
# MAGIC event_type STRING,
# MAGIC event_description STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/rpt_events'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.rpt_qa_receipts (
# MAGIC identifier INT,
# MAGIC rpt_event_date TIMESTAMP,
# MAGIC rpt_received_count INT,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_rpt_eventsidentifier INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/rpt_qa_receipts'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_bar_jurisdiction (
# MAGIC bar_jurisdiction_region_cd STRING,
# MAGIC bar_jurisdiction_region_nm STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_bar_jurisdiction'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_calendar (
# MAGIC calendar_dt DATE,
# MAGIC workday_in STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC lock_control_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_calendar'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_consent_motion_type (
# MAGIC stnd_consent_motion_type_id INT,
# MAGIC consent_motion_type_nm STRING,
# MAGIC description_tx STRING,
# MAGIC fk_incoming_plntff_entry_cd STRING,
# MAGIC fk_incoming_defdnt_entry_cd STRING,
# MAGIC fk_outgoing_board_entry_cd STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_consent_motion_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_decision (
# MAGIC decision_cd STRING,
# MAGIC decision_nm STRING,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_decision'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_document_type (
# MAGIC document_type_cd STRING,
# MAGIC document_type_nm STRING,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP,
# MAGIC allowed_doc_type_select_in STRING,
# MAGIC display_seq_no INT,
# MAGIC update_doc_type_required_in STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_document_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_dscvry_conf_agrmt (
# MAGIC sdca_id INT,
# MAGIC agreement_nm STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_dscvry_conf_agrmt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_dscvry_conf_claim (
# MAGIC sdcc_id INT,
# MAGIC claim_nm STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_dscvry_conf_claim'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_employee_access_type (
# MAGIC employee_access_type_cd STRING,
# MAGIC employee_access_type_nm STRING,
# MAGIC description_tx STRING,
# MAGIC on_send_list_in STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_employee_access_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_entity_type (
# MAGIC stnd_entity_type_id INT,
# MAGIC stnd_entity_type_tx STRING,
# MAGIC party_type_cd STRING,
# MAGIC requires_addl_info_in STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_entity_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_exam_att_action_mapping (
# MAGIC stnd_exam_att_action_mapping_id INT,
# MAGIC action_cd STRING,
# MAGIC fk_entry_cd STRING,
# MAGIC is_confidential_cd STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_exam_att_action_mapping'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_ext_of_time_type (
# MAGIC ext_of_time_type_id INT,
# MAGIC ext_of_time_type_nm STRING,
# MAGIC description_tx STRING,
# MAGIC fk_incoming_filing_entry_cd STRING,
# MAGIC fk_outgoing_response_entry_cd STRING,
# MAGIC filing_party_type_ct STRING,
# MAGIC extension_days_qt INT,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP,
# MAGIC display_seq_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_ext_of_time_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_ground (
# MAGIC ground_id INT,
# MAGIC description_tx STRING,
# MAGIC pleaded_mark_in INT,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_ground'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_ground_hist (
# MAGIC ground_hist_id INT,
# MAGIC transaction_id STRING,
# MAGIC operation_cd STRING,
# MAGIC fk_ground_id INT,
# MAGIC description_tx STRING,
# MAGIC pleaded_mark_in INT,
# MAGIC prev_last_mod_ts TIMESTAMP,
# MAGIC prev_last_mod_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC system_authentication_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_ground_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_motion (
# MAGIC sm_id INT,
# MAGIC motion_nm STRING,
# MAGIC last_modified_user_id STRING,
# MAGIC last_modified_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_motion'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_pnt_log_type_pnt_type (
# MAGIC pnt_log_type_pnt_type_id INT,
# MAGIC fk_point_log_type_id INT,
# MAGIC fk_point_type_id INT,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP,
# MAGIC point_qt DOUBLE,
# MAGIC display_sequence_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_pnt_log_type_pnt_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_point_log_type (
# MAGIC point_log_type_id INT,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP,
# MAGIC point_log_type_cd STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_point_log_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_point_type (
# MAGIC point_type_id INT,
# MAGIC point_type_cd STRING,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_point_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_prcdng_dscvry_conf_stat (
# MAGIC spdcs_id INT,
# MAGIC status_nm STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_prcdng_dscvry_conf_stat'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_prcdng_extension_grnd (
# MAGIC prcdng_extension_grnd_id INT,
# MAGIC prcdng_extension_grnd_nm STRING,
# MAGIC description_tx STRING,
# MAGIC display_seq_no INT,
# MAGIC additional_text_required_in STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_prcdng_extension_grnd'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_proceeding_event (
# MAGIC proceeding_event_id INT,
# MAGIC proceeding_event_nm STRING,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_proceeding_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_quality_review_attn_src (
# MAGIC quality_review_attn_src_id INT,
# MAGIC quality_review_attn_src_nm STRING,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_quality_review_attn_src'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_quality_review_category (
# MAGIC quality_review_category_id INT,
# MAGIC quality_review_category_nm STRING,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_quality_review_category'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_quality_review_finding (
# MAGIC quality_review_finding_id INT,
# MAGIC quality_review_finding_nm STRING,
# MAGIC fk_quality_review_category_id INT,
# MAGIC impact_level_ct INT,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_quality_review_finding'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_report_frequency (
# MAGIC report_frequency_cd INT,
# MAGIC description_tx STRING,
# MAGIC last_update_user_id STRING,
# MAGIC last_update_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_report_frequency'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_report_group (
# MAGIC srg_id INT,
# MAGIC group_nm STRING,
# MAGIC last_modified_user_id STRING,
# MAGIC last_modified_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_report_group'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_role (
# MAGIC role_cd STRING,
# MAGIC description_tx STRING,
# MAGIC qr_log_emp_type_in STRING,
# MAGIC qr_log_emp_type_tx STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_role'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_rule (
# MAGIC rule_id INT,
# MAGIC rule_tx STRING,
# MAGIC short_rule_tx STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_rule'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_rule_hist (
# MAGIC stnd_rule_hist_id INT,
# MAGIC transaction_id STRING,
# MAGIC operation_cd STRING,
# MAGIC fk_rule_id INT,
# MAGIC rule_tx STRING,
# MAGIC short_rule_tx STRING,
# MAGIC prev_last_mod_ts TIMESTAMP,
# MAGIC prev_last_mod_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC system_authentication_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_rule_hist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_schedule_type (
# MAGIC schedule_type_id INT,
# MAGIC schedule_type_nm STRING,
# MAGIC description_tx STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_schedule_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.stnd_valid_application_status (
# MAGIC valid_application_status_id INT,
# MAGIC proceeding_type_cd STRING,
# MAGIC tram_status_cd STRING,
# MAGIC create_user_id STRING,
# MAGIC create_ts TIMESTAMP,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC begin_effective_dt TIMESTAMP,
# MAGIC end_effective_dt TIMESTAMP,
# MAGIC tram_status_tx STRING,
# MAGIC timeliness_qt INT,
# MAGIC cm_entry_cd STRING,
# MAGIC processing_type_cd STRING,
# MAGIC proceeding_filing_type_cd STRING,
# MAGIC valid_entry_type_cd STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/stnd_valid_application_status'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.trial_event (
# MAGIC trial_event_id INT,
# MAGIC node_nm STRING,
# MAGIC display_nm STRING,
# MAGIC trial_event_ct STRING,
# MAGIC description_tx STRING,
# MAGIC last_modified_user_id STRING,
# MAGIC last_modified_ts TIMESTAMP,
# MAGIC fk_stnd_proceeding_event_id INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/trial_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ttab_application_case_file (
# MAGIC serial_number INT,
# MAGIC registration INT,
# MAGIC location_code STRING,
# MAGIC application_status STRING,
# MAGIC filing_date TIMESTAMP,
# MAGIC type_of_tram_update STRING,
# MAGIC successful_tram_update_in STRING,
# MAGIC date_of_attempted_tram_upd TIMESTAMP,
# MAGIC date_of_publication TIMESTAMP,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC date_of_last_action TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ttab_application_case_file'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ttab_efoia_upload_control (
# MAGIC ttab_efoia_upload_control_id INT,
# MAGIC insert_ts TIMESTAMP,
# MAGIC etl_completed_ts TIMESTAMP,
# MAGIC cfk_panel_id INT,
# MAGIC document_create_dt TIMESTAMP,
# MAGIC proceeding_no INT,
# MAGIC document_image_id STRING,
# MAGIC proceeding_type_cd STRING,
# MAGIC issue_code_list_tx STRING,
# MAGIC fk_decision_cd STRING,
# MAGIC decision_writer_nm STRING,
# MAGIC proceeding_decision_file_nm STRING,
# MAGIC party_nm STRING,
# MAGIC examining_attorney_nm STRING,
# MAGIC decision_tx STRING,
# MAGIC precedent_citable_in STRING,
# MAGIC panel_member_tx STRING,
# MAGIC opposer_mark_good_service_tx STRING,
# MAGIC applcnt_mark_good_service_tx STRING,
# MAGIC exmg_atty_mark_good_cited_tx STRING,
# MAGIC etl_error_msg STRING,
# MAGIC issue_tx STRING,
# MAGIC delete_in STRING,
# MAGIC action_ct STRING,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP, 
# MAGIC dn_phe_document_type_cd STRING,
# MAGIC dn_phe_entry_no INT,
# MAGIC dn_phe_entry_code STRING,
# MAGIC sequence_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ttab_efoia_upload_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ttab_efoia_upload_control_err (
# MAGIC ttab_efoia_upload_control_id INT,
# MAGIC insert_ts TIMESTAMP,
# MAGIC additional_key INT,
# MAGIC source_dt TIMESTAMP,
# MAGIC proceeding_no INT,
# MAGIC source_trigger_nm STRING,
# MAGIC error_no INT,
# MAGIC error_tx STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ttab_efoia_upload_control_err'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ttab_panel_info (
# MAGIC fk_proceeding_number0 INT,
# MAGIC hearing_onbrief_dt TIMESTAMP,
# MAGIC fk_dw_employee_no INT,
# MAGIC assign_dt TIMESTAMP,
# MAGIC fk_judge1_employee_no INT,
# MAGIC fk_judge2_employee_no INT,
# MAGIC fk_judge3_employee_no INT,
# MAGIC mailed_dt TIMESTAMP,
# MAGIC filing_dt TIMESTAMP,
# MAGIC ground_tx STRING,
# MAGIC decision_tx STRING,
# MAGIC comments_tx STRING,
# MAGIC disabled_in STRING,
# MAGIC last_mod_user_id STRING,
# MAGIC last_mod_ts TIMESTAMP,
# MAGIC hearing_onbrief_tx STRING,
# MAGIC pk_panel_id INT,
# MAGIC examining_attorney_nm STRING,
# MAGIC precident_in STRING,
# MAGIC opposer_goods_services_tx STRING,
# MAGIC applicant_goods_services_tx STRING,
# MAGIC examining_attorney_gds_srvc_tx STRING,
# MAGIC fk_decision_cd STRING,
# MAGIC judge1_dissenting_in STRING,
# MAGIC judge2_dissenting_in STRING,
# MAGIC judge3_dissenting_in STRING,
# MAGIC exclude_ready_for_decision_in STRING,
# MAGIC exclude_end_to_end_in STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ttab_panel_info'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ttab_status_code_reference (
# MAGIC ttab_status_code STRING,
# MAGIC ttab_status_text STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC status_allowed_for_exa_in STRING,
# MAGIC status_allowed_for_opp_in STRING,
# MAGIC status_allowed_for_can_in STRING,
# MAGIC status_allowed_for_cnu_in STRING,
# MAGIC status_allowed_for_ext_in STRING,
# MAGIC status_allowed_for_mis_in STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ttab_status_code_reference'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ttab_tm_mapping (
# MAGIC fk_entry_code STRING,
# MAGIC proceeding_type_cd STRING,
# MAGIC tm_cm_entry_cd STRING,
# MAGIC tm_cm_entry_type_cd STRING,
# MAGIC tm_status_cd STRING,
# MAGIC validate_ttab_stat_entry_in STRING,
# MAGIC validate_data_for_event_in STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ttab_tm_mapping'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.${conf.database}.ttab_user (
# MAGIC user_id STRING,
# MAGIC ttab_password STRING,
# MAGIC security_role STRING,
# MAGIC due_date TIMESTAMP,
# MAGIC wkflw_password STRING,
# MAGIC wkflw_domain STRING,
# MAGIC last_update_userid STRING,
# MAGIC last_update_timestamp TIMESTAMP,
# MAGIC fk_employeenumber0 INT,
# MAGIC uspto_enterprise_user_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/ttab_user'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %md
# MAGIC TTABWF Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_attributes (
    object_id STRING,
    object_type LONG,
    object_name STRING,
    object_server STRING,
    object_class STRING,
    appnum STRING,
    serialnum STRING,
    registrationnum STRING,
    proceedingnum STRING,
    documenttype STRING,
    assigneemp STRING,
    description STRING,
    status STRING,
    confidential STRING,
    isnew STRING,
    duedate STRING,
    teamid STRING,
    proceedingtype STRING
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_attributes'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_codes (
    code LONG,
    type STRING,
    description STRING
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_codes'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_form_status (
    id STRING,
    name STRING,
    queue STRING,
    o_resources STRING,
    time_stamp STRING,
    form STRING,
    field STRING,
    o_row LONG,
    status LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_form_status'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_grpmembership (
    group_name STRING,
    member_name STRING,
    member_type LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_grpmembership'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_link (
    child_id STRING,
    parent_id STRING,
    child_name STRING,
    child_rev LONG,
    child_server STRING,
    workflow_status LONG,
    complete_status LONG,
    unfolded_status LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_link'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_log (
    batch STRING,
    end_time TIMESTAMP,
    error LONG,
    flags LONG,
    form STRING,
    id STRING,
    instance LONG,
    intvar1 LONG,
    intvar2 LONG,
    intvar3 LONG,
    intvar4 LONG,
    intvar5 LONG,
    intvar6 LONG,
    intvar7 LONG,
    intvar8 LONG,
    intvar9 LONG,
    intvar10 LONG,
    intvar11 LONG,
    intvar12 LONG,
    intvar13 LONG,
    intvar14 LONG,
    intvar15 LONG,
    intvar16 LONG,
    intvar17 LONG,
    intvar18 LONG,
    intvar19 LONG,
    intvar20 LONG,
    intvar21 LONG,
    intvar22 LONG,
    intvar23 LONG,
    intvar24 LONG,
    intvar25 LONG,
    intvar26 LONG,
    intvar27 LONG,
    intvar28 LONG,
    intvar29 LONG,
    intvar30 LONG,
    intvar31 LONG,
    intvar32 LONG,
    intvar33 LONG,
    intvar34 LONG,
    intvar35 LONG,
    intvar36 LONG,
    intvar37 LONG,
    intvar38 LONG,
    intvar39 LONG,
    intvar40 LONG,
    intvar41 LONG,
    intvar42 LONG,
    intvar43 LONG,
    intvar44 LONG,
    intvar45 LONG,
    intvar46 LONG,
    intvar47 LONG,
    intvar48 LONG,
    intvar49 LONG,
    intvar50 LONG,
    log_seq LONG,
    name STRING,
    new_queue STRING,
    o_resource STRING,
    priority LONG,
    queue STRING,
    rule_num LONG,
    rule_type LONG,
    server STRING,
    start_time TIMESTAMP,
    strvar1 STRING,
    strvar2 STRING,
    strvar3 STRING,
    strvar4 STRING,
    strvar5 STRING,
    strvar6 STRING,
    strvar7 STRING,
    strvar8 STRING,
    strvar9 STRING,
    strvar10 STRING,
    strvar11 STRING,
    strvar12 STRING,
    strvar13 STRING,
    strvar14 STRING,
    strvar15 STRING,
    strvar16 STRING,
    strvar17 STRING,
    strvar18 STRING,
    strvar19 STRING,
    strvar20 STRING,
    strvar21 STRING,
    strvar22 STRING,
    strvar23 STRING,
    strvar24 STRING,
    strvar25 STRING,
    strvar26 STRING,
    strvar27 STRING,
    strvar28 STRING,
    strvar29 STRING,
    strvar30 STRING,
    strvar31 STRING,
    strvar32 STRING,
    strvar33 STRING,
    strvar34 STRING,
    strvar35 STRING,
    strvar36 STRING,
    strvar37 STRING,
    strvar38 STRING,
    strvar39 STRING,
    strvar40 STRING,
    strvar41 STRING,
    strvar42 STRING,
    strvar43 STRING,
    strvar44 STRING,
    strvar45 STRING,
    strvar46 STRING,
    strvar47 STRING,
    strvar48 STRING,
    strvar49 STRING,
    strvar50 STRING,
    time_stamp STRING,
    type LONG,
    var_id LONG,
    workflow STRING,
    workflow_id LONG,
    workset STRING
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_log'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_object (
    id STRING,
    owner STRING,
    locktime LONG,
    archive LONG,
    status LONG,
    flag LONG,
    refcount LONG,
    tscreated STRING,
    created LONG,
    tsreferenced STRING,
    referenced LONG,
    tsmodified STRING,
    modified LONG,
    contents STRING,
    variable STRING,
    varsize LONG,
    static STRING,
    staticsize LONG,
    type LONG,
    class STRING,
    revision LONG,
    name STRING,
    wip_count LONG,
    queue_timestamp STRING,
    stat_rev LONG,
    var_rev LONG,
    entlock LONG,
    lock_seq LONG,
    mod_seq LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_object'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_privilege (
    group_name STRING,
    class STRING,
    privilege LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_privilege'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_profile (
    user_name STRING,
    time_created LONG,
    class1 STRING,
    privilege1 LONG,
    class2 STRING,
    privilege2 LONG,
    class3 STRING,
    privilege3 LONG,
    class4 STRING,
    privilege4 LONG,
    class5 STRING,
    privilege5 LONG,
    class6 STRING,
    privilege6 LONG,
    class7 STRING,
    privilege7 LONG,
    class8 STRING,
    privilege8 LONG,
    class9 STRING,
    privilege9 LONG,
    class10 STRING,
    privilege10 LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_profile'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_queues (
    queue STRING,
    work_status LONG,
    id STRING,
    instance LONG,
    type LONG,
    name STRING,
    server STRING,
    time_stamp STRING,
    o_resource STRING,
    batch STRING,
    lock_time STRING,
    form STRING,
    priority LONG,
    parallel LONG,
    class STRING,
    data_length LONG,
    data_type LONG,
    queue_data STRING,
    sequence LONG,
    timeout LONG,
    child_count LONG,
    error_code LONG,
    associated_user STRING,
    swv STRING,
    intvar1 LONG,
    intvar2 LONG,
    intvar3 LONG,
    intvar4 LONG,
    intvar5 LONG,
    intvar6 LONG,
    intvar7 LONG,
    intvar8 LONG,
    intvar9 LONG,
    intvar10 LONG,
    intvar11 LONG,
    intvar12 LONG,
    intvar13 LONG,
    intvar14 LONG,
    intvar15 LONG,
    intvar16 LONG,
    intvar17 LONG,
    intvar18 LONG,
    intvar19 LONG,
    intvar20 LONG,
    intvar21 LONG,
    intvar22 LONG,
    intvar23 LONG,
    intvar24 LONG,
    intvar25 LONG,
    intvar26 LONG,
    intvar27 LONG,
    intvar28 LONG,
    intvar29 LONG,
    intvar30 LONG,
    intvar31 LONG,
    intvar32 LONG,
    intvar33 LONG,
    intvar34 LONG,
    intvar35 LONG,
    intvar36 LONG,
    intvar37 LONG,
    intvar38 LONG,
    intvar39 LONG,
    intvar40 LONG,
    intvar41 LONG,
    intvar42 LONG,
    intvar43 LONG,
    intvar44 LONG,
    intvar45 LONG,
    intvar46 LONG,
    intvar47 LONG,
    intvar48 LONG,
    intvar49 LONG,
    intvar50 LONG,
    strvar1 STRING,
    strvar2 STRING,
    strvar3 STRING,
    strvar4 STRING,
    strvar5 STRING,
    strvar6 STRING,
    strvar7 STRING,
    strvar8 STRING,
    strvar9 STRING,
    strvar10 STRING,
    strvar11 STRING,
    strvar12 STRING,
    strvar13 STRING,
    strvar14 STRING,
    strvar15 STRING,
    strvar16 STRING,
    strvar17 STRING,
    strvar18 STRING,
    strvar19 STRING,
    strvar20 STRING,
    strvar21 STRING,
    strvar22 STRING,
    strvar23 STRING,
    strvar24 STRING,
    strvar25 STRING,
    strvar26 STRING,
    strvar27 STRING,
    strvar28 STRING,
    strvar29 STRING,
    strvar30 STRING,
    strvar31 STRING,
    strvar32 STRING,
    strvar33 STRING,
    strvar34 STRING,
    strvar35 STRING,
    strvar36 STRING,
    strvar37 STRING,
    strvar38 STRING,
    strvar39 STRING,
    strvar40 STRING,
    strvar41 STRING,
    strvar42 STRING,
    strvar43 STRING,
    strvar44 STRING,
    strvar45 STRING,
    strvar46 STRING,
    strvar47 STRING,
    strvar48 STRING,
    strvar49 STRING,
    strvar50 STRING,
    workflow_id LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_queues'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_qvars (
    o_resource STRING,
    var_id LONG,
    var_value STRING,
    workflow_id LONG,
    category STRING
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_qvars'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_resources (
    O_RESOURCE STRING,
    DETAILS STRING,
    WORKSET STRING,
    O_AUDIT LONG,
    DISABLE LONG,
    PASSWORD STRING,
    LOGON LONG,
    NETWORK_NAME STRING,
    ADMIN_TOKEN LONG,
    LOGON_MODE LONG,
    LAST_LOGON LONG,
    WORKFLOW_ID LONG,
    RES_CREATED LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_resources'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${{conf.catalog}}.${{conf.database}}.ttab_user_activity_log (
    USER_NAME STRING,
    LOGON_TIMESTAMP TIMESTAMP,
    LOGOFF_TIMESTAMP TIMESTAMP,
    TIME_DIFF LONG
)
USING DELTA
LOCATION 's3://${{conf.cdc_bucket}}/delta_tables/${{conf.catalog}}/bronze/ttab_user_activity_log'
TBLPROPERTIES (
    'databricks.delta.autocompact.enabled' = true,
    'delta.enableChangeDataFeed' = true
)
""")

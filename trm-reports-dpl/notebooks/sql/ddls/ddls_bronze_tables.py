# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
print(f"{trgt_catalog=}")
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.bronze 
# MAGIC COMMENT 'For trm reports raw data' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.bronze.tmns_json_raw_file_data (
# MAGIC date_time_range STRING,
# MAGIC in_email string,
# MAGIC in_letter string,
# MAGIC month STRING,
# MAGIC quarter STRING,
# MAGIC report_type STRING,
# MAGIC total_notices_sent_in_email STRING,
# MAGIC total_notices_sent_in_letter STRING,
# MAGIC total_records	INT,
# MAGIC year INT,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING
# MAGIC ) using json 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tmns_json_raw_file_data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.teas_s08n15_xml_file_data (
# MAGIC   description STRING,
# MAGIC   document_type STRING,
# MAGIC   filing_identifier STRING,
# MAGIC   xml_create_date TIMESTAMP,
# MAGIC   submit_date DATE,
# MAGIC   filing_date DATE,
# MAGIC   registration_number INT,
# MAGIC   serial_number INT,
# MAGIC   registration_date DATE,
# MAGIC   pay_additional_fee STRING,
# MAGIC   attorney_filing STRING,
# MAGIC   case_file_owner_name STRING,
# MAGIC   case_file_owner_citizenship_country_name STRING,
# MAGIC   case_file_owner_country_name STRING,
# MAGIC   attorney_docket_number STRING, 
# MAGIC   attorney_credential_bar_membership_number STRING,
# MAGIC   fee_code STRING,
# MAGIC   grace_period INT,
# MAGIC   number_of_classes INT,
# MAGIC   number_of_classes_paid INT,
# MAGIC   subtotal_amount INT,
# MAGIC   class_code STRING,
# MAGIC   deleted_description_text STRING,
# MAGIC   description_text STRING,
# MAGIC   final_description_text STRING,
# MAGIC   keep_description_text_flag STRING,
# MAGIC   create_ts STRING,
# MAGIC   create_user_id STRING,
# MAGIC   year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/teas_s08n15_xml_file_data'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.pea_opensearch_ai_center (
# MAGIC     filing_date STRING,
# MAGIC     md_levenshtein_distance LONG,
# MAGIC     md_levenshtein_ratio DOUBLE,
# MAGIC     md_matt_partial_match BOOLEAN,
# MAGIC     qc_stage STRING,
# MAGIC     registration_number LONG,
# MAGIC     serial_number STRING,
# MAGIC     status_date STRING,
# MAGIC     timestamp STRING,
# MAGIC     audit_metadata_agent_types STRING,
# MAGIC     audit_metadata_record_count LONG,
# MAGIC     tm_data_color_claimed_statement STRING,
# MAGIC     tm_data_design_search_codes STRING,
# MAGIC     tm_data_is_color_mark BOOLEAN,
# MAGIC     tm_data_mark_description STRING,
# MAGIC     tm_data_mark_drawing_code STRING,
# MAGIC     tm_data_mark_text STRING,
# MAGIC     tm_data_mark_type STRING,
# MAGIC     tm_data_pseudomarks STRING,
# MAGIC     agent_responses_mark_description_classes STRING,
# MAGIC     agent_responses_mark_description_color_claimed_statement STRING,
# MAGIC     agent_responses_mark_description_design_search_codes STRING,
# MAGIC     agent_responses_mark_description_literal_mark STRING,
# MAGIC     agent_responses_mark_description_mark_description STRING,
# MAGIC     agent_responses_mark_description_pseudomarks STRING,
# MAGIC     agent_responses_mark_description_serial_number STRING,
# MAGIC     agent_responses_orchestration_color_claimed_statement STRING,
# MAGIC     agent_responses_orchestration_literal_mark STRING,
# MAGIC     agent_responses_orchestration_mark_description STRING,
# MAGIC     agent_responses_orchestration_serial_number STRING,
# MAGIC     tm_data_classes_class_number STRING,
# MAGIC     tm_data_classes_goods_and_services_text STRING,
# MAGIC     agent_responses_orchestration_classes_label STRING,
# MAGIC     agent_responses_orchestration_classes_original_text STRING,
# MAGIC     agent_responses_orchestration_classes_reason STRING,
# MAGIC     agent_responses_orchestration_classes_relevance STRING,
# MAGIC     agent_responses_orchestration_classes_sequence_no STRING,
# MAGIC     agent_responses_orchestration_pseudomarks_analysis STRING,
# MAGIC     agent_responses_orchestration_design_search_codes_codes_code STRING,
# MAGIC     agent_responses_orchestration_design_search_codes_codes_reason STRING,
# MAGIC     agent_responses_orchestration_design_search_codes_codes_relevance STRING,
# MAGIC     agent_responses_orchestration_pseudomarks_values_confidence STRING,
# MAGIC     agent_responses_orchestration_pseudomarks_values_pseudomark STRING
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/pea_opensearch_ai_center'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.teas_s07_xml_file_data (
# MAGIC   description STRING,
# MAGIC   document_type STRING,
# MAGIC   filing_identifier STRING,
# MAGIC   xml_create_date TIMESTAMP,
# MAGIC   submit_date DATE,
# MAGIC   filing_date DATE,
# MAGIC   registration_number INT,
# MAGIC   serial_number INT,
# MAGIC   registration_date DATE,
# MAGIC   pay_additional_fee STRING,
# MAGIC   attorney_filing STRING,
# MAGIC   case_file_owner_name STRING,
# MAGIC   case_file_owner_citizenship_country_name STRING,
# MAGIC   case_file_owner_country_name STRING,
# MAGIC   attorney_docket_number STRING, 
# MAGIC   attorney_credential_bar_membership_number STRING,
# MAGIC   fee_code STRING,
# MAGIC   grace_period INT,
# MAGIC   number_of_classes INT,
# MAGIC   number_of_classes_paid INT,
# MAGIC   subtotal_amount INT,
# MAGIC   class_code STRING,
# MAGIC   deleted_description_text STRING,
# MAGIC   description_text STRING,
# MAGIC   final_description_text STRING,
# MAGIC   keep_description_text_flag STRING,
# MAGIC   create_ts STRING,
# MAGIC   create_user_id STRING,
# MAGIC   year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/teas_s07_xml_file_data'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.teas_s08n09_xml_file_data (
# MAGIC   description STRING,
# MAGIC   document_type STRING,
# MAGIC   filing_identifier STRING,
# MAGIC   xml_create_date TIMESTAMP,
# MAGIC   submit_date DATE,
# MAGIC   filing_date DATE,
# MAGIC   registration_number INT,
# MAGIC   serial_number INT,
# MAGIC   registration_date DATE,
# MAGIC   pay_additional_fee STRING,
# MAGIC   attorney_filing STRING,
# MAGIC   case_file_owner_name STRING,
# MAGIC   case_file_owner_citizenship_country_name STRING,
# MAGIC   case_file_owner_country_name STRING,
# MAGIC   attorney_docket_number STRING, 
# MAGIC   attorney_credential_bar_membership_number STRING,
# MAGIC   fee_code STRING,
# MAGIC   grace_period INT,
# MAGIC   number_of_classes INT,
# MAGIC   number_of_classes_paid INT,
# MAGIC   subtotal_amount INT,
# MAGIC   class_code STRING,
# MAGIC   deleted_description_text STRING,
# MAGIC   description_text STRING,
# MAGIC   final_description_text STRING,
# MAGIC   keep_description_text_flag STRING,
# MAGIC   create_ts STRING,
# MAGIC   create_user_id STRING,
# MAGIC   year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/teas_s08n09_xml_file_data'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.teas_s71n15_xml_file_data (
# MAGIC   description STRING,
# MAGIC   document_type STRING,
# MAGIC   filing_identifier STRING,
# MAGIC   xml_create_date TIMESTAMP,
# MAGIC   submit_date DATE,
# MAGIC   filing_date DATE,
# MAGIC   registration_number INT,
# MAGIC   serial_number INT,
# MAGIC   registration_date DATE,
# MAGIC   pay_additional_fee STRING,
# MAGIC   attorney_filing STRING,
# MAGIC   case_file_owner_name STRING,
# MAGIC   case_file_owner_citizenship_country_name STRING,
# MAGIC   case_file_owner_country_name STRING,
# MAGIC   attorney_docket_number STRING, 
# MAGIC   attorney_credential_bar_membership_number STRING,
# MAGIC   fee_code STRING,
# MAGIC   grace_period INT,
# MAGIC   number_of_classes INT,
# MAGIC   number_of_classes_paid INT,
# MAGIC   subtotal_amount INT,
# MAGIC   class_code STRING,
# MAGIC   deleted_description_text STRING,
# MAGIC   description_text STRING,
# MAGIC   final_description_text STRING,
# MAGIC   keep_description_text_flag STRING,
# MAGIC   create_ts STRING,
# MAGIC   create_user_id STRING,
# MAGIC   year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/teas_s71n15_xml_file_data'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.teas_s08_xml_file_data (
# MAGIC   description STRING,
# MAGIC   document_type STRING,
# MAGIC   filing_identifier STRING,
# MAGIC   xml_create_date TIMESTAMP,
# MAGIC   submit_date DATE,
# MAGIC   filing_date DATE,
# MAGIC   registration_number INT,
# MAGIC   serial_number INT,
# MAGIC   registration_date DATE,
# MAGIC   pay_additional_fee STRING,
# MAGIC   attorney_filing STRING,
# MAGIC   case_file_owner_name STRING,
# MAGIC   case_file_owner_citizenship_country_name STRING,
# MAGIC   case_file_owner_country_name STRING,
# MAGIC   attorney_docket_number STRING, 
# MAGIC   attorney_credential_bar_membership_number STRING,
# MAGIC   fee_code STRING,
# MAGIC   grace_period INT,
# MAGIC   number_of_classes INT,
# MAGIC   number_of_classes_paid INT,
# MAGIC   subtotal_amount INT,
# MAGIC   class_code STRING,
# MAGIC   deleted_description_text STRING,
# MAGIC   description_text STRING,
# MAGIC   final_description_text STRING,
# MAGIC   keep_description_text_flag STRING,
# MAGIC   create_ts STRING,
# MAGIC   create_user_id STRING,
# MAGIC   year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/teas_s08_xml_file_data'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.teas_s71_xml_file_data (
# MAGIC   description STRING,
# MAGIC   document_type STRING,
# MAGIC   filing_identifier STRING,
# MAGIC   xml_create_date TIMESTAMP,
# MAGIC   submit_date DATE,
# MAGIC   filing_date DATE,
# MAGIC   registration_number INT,
# MAGIC   serial_number INT,
# MAGIC   registration_date DATE,
# MAGIC   pay_additional_fee STRING,
# MAGIC   attorney_filing STRING,
# MAGIC   case_file_owner_name STRING,
# MAGIC   case_file_owner_citizenship_country_name STRING,
# MAGIC   case_file_owner_country_name STRING,
# MAGIC   attorney_docket_number STRING, 
# MAGIC   attorney_credential_bar_membership_number STRING,
# MAGIC   fee_code STRING,
# MAGIC   grace_period INT,
# MAGIC   number_of_classes INT,
# MAGIC   number_of_classes_paid INT,
# MAGIC   subtotal_amount INT,
# MAGIC   class_code STRING,
# MAGIC   deleted_description_text STRING,
# MAGIC   description_text STRING,
# MAGIC   final_description_text STRING,
# MAGIC   keep_description_text_flag STRING,
# MAGIC   create_ts STRING,
# MAGIC   create_user_id STRING,
# MAGIC   year_month STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (year_month)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/teas_s71_xml_file_data'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS ${conf.catalog}.bronze.tm5_tri_lookup_status (
# MAGIC   i_Code_Value STRING,
# MAGIC   u_Code_Meaning STRING,
# MAGIC   create_ts timestamp,
# MAGIC   create_user_id string)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm5_tri_lookup_status'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.tm5_tri_partners(
# MAGIC dt_joined date,
# MAGIC f_can_vote boolean,
# MAGIC i_partner_id bigint,
# MAGIC u_active boolean,
# MAGIC u_language string,
# MAGIC u_language_full string,
# MAGIC u_last_expired_email_sent timestamp,
# MAGIC u_partner_name string,
# MAGIC create_ts timestamp,
# MAGIC create_user_id string)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm5_tri_partners'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.tm5_tri_vote_types(
# MAGIC i_primary_vote_type bigint,
# MAGIC i_vote_type_id bigint,
# MAGIC u_visitor_vote_name string,
# MAGIC u_vote_name string,
# MAGIC create_ts timestamp,
# MAGIC create_user_id string)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm5_tri_vote_types'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.bronze.tm5_tri_items(
# MAGIC dt_accepted DATE,
# MAGIC dt_created DATE,
# MAGIC dt_Rejected date,
# MAGIC dt_released DATE,
# MAGIC dt_removed DATE,
# MAGIC dt_Withdrawn DATE,
# MAGIC f_released boolean,
# MAGIC i_Class_ID string,
# MAGIC i_item_id bigint,
# MAGIC i_resubmittal bigint,
# MAGIC i_status bigint,
# MAGIC i_user_id_created_by bigint,
# MAGIC i_User_ID_Released_By BIGINT,
# MAGIC i_user_id_resubmitted_by bigint,
# MAGIC u_item_name string,
# MAGIC create_ts timestamp,
# MAGIC create_user_id string)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm5_tri_items'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.tm5_tri_votes(
# MAGIC dt_created string,
# MAGIC i_item_id bigint,
# MAGIC i_partner_id bigint,
# MAGIC i_user_id bigint,
# MAGIC i_vote_id bigint,
# MAGIC i_vote_type_id bigint,
# MAGIC create_ts timestamp,
# MAGIC create_user_id string)
# MAGIC
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tm5_tri_votes'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.fee_cd (
# MAGIC   fee_cd_grp_id	bigint,
# MAGIC fee_cd	string,
# MAGIC fee_cd_split_in	string,
# MAGIC fee_cd_full	string,
# MAGIC prop_disc_in	string,
# MAGIC dissem_fee_in	string,
# MAGIC fee_title_reporting	string,
# MAGIC patent_tm	string,
# MAGIC patent_type_short	string,
# MAGIC patent_type_long	string,
# MAGIC entity_short	string,
# MAGIC enitity_long	string,
# MAGIC stage	string,
# MAGIC tm_paper_electronic	string,
# MAGIC cat_1_desc	string,
# MAGIC cat_1_order	int,
# MAGIC cat_2_desc	string,
# MAGIC cat_2_order	int,
# MAGIC cat_3_desc	string,
# MAGIC cat_3_order	int,
# MAGIC cat_4_desc	string,
# MAGIC cat_4_order	int,
# MAGIC cat_5_desc	string,
# MAGIC cat_5_order	int,
# MAGIC cat_6_desc	string,
# MAGIC cat_6_order	int,
# MAGIC cat_7_desc	string,
# MAGIC cat_7_order	int,
# MAGIC active_in	string,
# MAGIC create_ts	string,
# MAGIC update_ts	string,
# MAGIC user_nm	string)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/fee_cd'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.new_application_fees(
# MAGIC acc_dt TIMESTAMP,
# MAGIC ser_num STRING,
# MAGIC is_insufficient INT,
# MAGIC is_free_form INT,
# MAGIC `is_>1000` INT,
# MAGIC new_fees_total INT,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id string)
# MAGIC
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/new_application_fees'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  ${conf.catalog}.bronze.rpo_sanctions_and_show_cause_order_name (
# MAGIC   `Serial_Number` BIGINT COMMENT 'Column for Serial Number',
# MAGIC   `Order_name` STRING COMMENT 'Column for Order name',
# MAGIC   `_created_timestamp` TIMESTAMP COMMENT 'Metadata column: Created at',
# MAGIC   `_created_by` STRING COMMENT 'Metadata column: Created by',
# MAGIC   `_updated_timestamp` TIMESTAMP COMMENT 'Metadata column: Updated at',
# MAGIC   `_updated_by` STRING COMMENT 'Metadata column: Updated by'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/rpo_sanctions_and_show_cause_order_name'
# MAGIC TBLPROPERTIES (
# MAGIC   'source' = 'RPO Serial Number and Order Name Excel'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.bronze.opm_holidays_weekends (
# MAGIC   Date DATE COMMENT 'Represents the specific date of a holiday or weekend.',
# MAGIC   Day STRING COMMENT 'Denotes the day of the week, such as Monday, Tuesday, etc.',
# MAGIC   Type STRING COMMENT 'Indicates whether the date is a holiday or a weekend.',
# MAGIC   `Holiday Name` STRING COMMENT 'Specifies the name of the holiday, if applicable.'
# MAGIC ) USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/opm_holidays_weekends'
# MAGIC COMMENT 'The table contains information about holidays and weekends. It can be used to identify non-working days.'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.checkConstraints' = 'supported',
# MAGIC   'delta.feature.columnMapping' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.generatedColumns' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7',
# MAGIC   'delta.parquet.compression.codec' = 'zstd'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  ${conf.catalog}.bronze.hrsa_current_employee (
# MAGIC ACCESSION_NATURE_OF_ACTION_DES string comment 'Accession action description',
# MAGIC APPOINTMENT_TYPE_DESCRIPTION string COMMENT 'Description of appointment type',
# MAGIC BUSINESS_UNIT_CODE string comment 'USPTO business unit',
# MAGIC EMPLOYEE_ACCESSION_NATURE_OF_A decimal(3,0) comment 'Accession action code',
# MAGIC EMPLOYEE_ACCESSION_PAY_PERIOD_ decimal(2,0) comment 'Accession pay period',
# MAGIC EMPLOYEE_GIVEN_NAME string comment 'Employee given name',
# MAGIC EMPLOYEE_CURRENT_FISCAL_YEAR timestamp comment 'Current fiscal year',
# MAGIC EMPLOYEE_EMPLOYMENT_STATUS_COD decimal(2,0) comment 'Current employment status',
# MAGIC EMPLOYEE_GRADE string comment 'Employee grade',
# MAGIC EMPLOYEE_HIRED_DATE timestamp comment 'Date employee was hired',
# MAGIC DETAILEE_SUPERVISOR_EMPLOYEE_N string comment 'Employee supervisor number',
# MAGIC EMPLOYEE_NUMBER string comment 'Employee number',
# MAGIC DETAILEE_SUPERVISOR_DIM_ORGANI string comment 'Detailee supervisor organization 6-digit',
# MAGIC EMPLOYEE_ORGANIZATION_CODE2 string comment 'Employee organization 6-digit',
# MAGIC EMPLOYEE_POSITION_IDENTIFIER decimal(10,0) comment 'Identifier for employees position',
# MAGIC EMPLOYEE_POSITION_NUMBER string comment 'Employee position number',
# MAGIC DETAILEE_SUPERVISOR_ORGANIZATI string comment '	Detailee supervisor organization',
# MAGIC EMPLOYEE_POSITION_STATUS_CODE string comment 'Employee position status code',
# MAGIC EMPLOYEE_TENURE_GROUP_CODE decimal(1,0) comment 'Employee tenure group code',
# MAGIC EMPLOYEE_REDUCTION_IN_FORCE_CO timestamp comment 'Employee reduction in force code',
# MAGIC EMPLOYEE_RETIREMENT_SERVICE_CO timestamp comment 'Employee retirement service code',
# MAGIC EMPLOYEE_SEPARATION_ACCESSION_ decimal(1,0) comment 'Separated employee accession code',
# MAGIC EMPLOYEE_SEPARATION_DATE timestamp comment 'Date employee left',
# MAGIC EMPLOYEE_SEPARATION_FINAL_T_CO string comment '	Separated employee code',
# MAGIC EMPLOYEE_SEPARATION_NATURE_OF_ decimal(3,0) comment 'Circumstances for employee leaving',
# MAGIC EMPLOYEE_SEPARATION_PARENT_LEAVE_CODE string comment 'Separated employee leave code',
# MAGIC EMPLOYEE_SEPARATION_PARENT_RET string comment 'Separated employee parent code',
# MAGIC EMPLOYEE_SEPARATION_PAY_PERIOD decimal(2,0) comment 'Pay period of separated employee',
# MAGIC EMPLOYEE_SEPARATION_TYPE_CODE string comment 'Code for separation type',
# MAGIC EMPLOYMENT_STATUS_DESCRIPTION string comment 'Employee current status',
# MAGIC ORGANIZATION_CODE2 string comment 'Employee organization 6-digit',
# MAGIC ORGANIZATION_FIFTH_LEVEL_CODE string comment 'Employee organization 5-digit',
# MAGIC ORGANIZATION_FIFTH_LEVEL_NAME string comment 'Employee organization 5-digit description',
# MAGIC ORGANIZATION_FIRST_LEVEL_CODE string comment 'Employee organization 1-digit',
# MAGIC ORGANIZATION_FIRST_LEVEL_NAME string comment 'Employee organization 1-digit description',
# MAGIC ORGANIZATION_FOURTH_LEVEL_CODE string comment 'Employee organization 4-digit',
# MAGIC ORGANIZATION_FOURTH_LEVEL_NAME string comment 'Employee organization 4-digit description',
# MAGIC ORGANIZATION_NAME string comment 'Organizations name',
# MAGIC ORGANIZATION_SECOND_LEVEL_CODE string comment 'Employee organization 2-digit',
# MAGIC ORGANIZATION_SECOND_LEVEL_NAME string comment 'Employee organization 2-digit description',
# MAGIC ORGANIZATION_SIXTH_LEVEL_CODE string comment 'Employee organization 6-digit',
# MAGIC ORGANIZATION_SIXTH_LEVEL_NAME string comment 'Employee organization 6-digit description',
# MAGIC ORGANIZATION_THIRD_LEVEL_CODE string comment 'Employee organization 3-digit',
# MAGIC ORGANIZATION_THIRD_LEVEL_NAME string comment 'Employee organization 3-digit description',
# MAGIC PAY_PLAN_SOURCE_CODE string comment 'Pay plan source code',
# MAGIC EMPLOYEE_VETERANS_STATUS_CODE string comment 'If employee is a veteran',
# MAGIC SALARY_RATE_DESCRIPTION string comment 'rate of employees salary',
# MAGIC SUPERVISOR_POSITION_NUMBER string comment 'Supervisor position number',
# MAGIC SUPERVISOR_POSITION_SUPERVISOR string comment 'Supervisor position code',
# MAGIC TENURE_GROUP_DESCRIPTION string comment 'Tenure group description',
# MAGIC POSITION_NUMBER string comment 'Position number',
# MAGIC SEPARATION_ACCESSION_TYPE_DESC string comment 'Separation accession type description',
# MAGIC SEPARATION_FINAL_DESCRIPTION string comment 'Separation description',
# MAGIC SEPARATION_PRINT_DESCRIPTION string comment 'Separation description',
# MAGIC SEPARATION_TYPE_DESCRIPTION string comment 'Separation type description',
# MAGIC TMORG string comment 'Description of TM organization'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/hrsa_current_employee'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  ${conf.catalog}.bronze.hrsa_employee_history (
# MAGIC EMPLOYEE_NUMBER_VW_SL_HRSA_EM STRING comment 'Employee ID number',
# MAGIC EMP_HIST_EMPLOYEE_NUMBER STRING comment 'Employee number',
# MAGIC EMP_HIST_FROM_AGENCY_CODE STRING comment 'Transfer from employee agency code',
# MAGIC EMP_HIST_FROM_DEPARTMENT_CODE STRING comment 'Transfer from employee department code',
# MAGIC EMP_HIST_FROM_ORGANIZATION_COD STRING comment 'Transfer from employee organization code',
# MAGIC EMP_HIST_NATURE_OF_ACTION_DATE TIMESTAMP comment 'Date of employee action',
# MAGIC EMP_HIST_ORGANIZATION_CODE STRING comment 'Employee organization code',
# MAGIC EMP_HIST_TO_AGENCY_CODE STRING comment 'Transfer to employee agency code',
# MAGIC EMP_HIST_TO_DEPARTMENT_CODE STRING comment 'Transfer to employee department code',
# MAGIC EMP_HIST_TO_NATURE_OF_ACTION_C STRING comment 'Transfer action code',
# MAGIC EMP_HIST_TO_NATURE_OF_ACTION_1 TIMESTAMP comment 'Transfer action timestamp',
# MAGIC EMP_HIST_TO_NATURE_OF_ACTION_D STRING comment 'Transfer action description',
# MAGIC EMP_HIST_TO_ORGANIZATION_CODE STRING comment 'Transfer to employee organization code',
# MAGIC NATURE_OF_ACTION_DESCRIPTION STRING comment 'Description of action',
# MAGIC NATURE_OF_ACTION_CODE DECIMAL(3,0) comment 'Description of action code',
# MAGIC LATEST_EMPLOYEE_FAMILY_NAME STRING comment 'Latest employee family name',
# MAGIC LATEST_EMPLOYEE_GIVEN_NAME STRING comment 'Latest employee given name',
# MAGIC LATEST_EMPLOYEE_NUMBER STRING comment 'Latest employee number',
# MAGIC ORGANIZATION_ACTIVE_CODEA STRING comment 'Employee active org code'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/hrsa_employee_history'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  ${conf.catalog}.bronze.hrsa_employee (
# MAGIC ACTV_FLG string comment 'Flag for if employee is actively working',
# MAGIC EMP_FULL_NM string Comment 'Employees full name',
# MAGIC EMP_ID_NO decimal(38,10) comment 'Employee ID Number',
# MAGIC EMP_NO string comment 'Employee Number (different than ID number)',
# MAGIC END_DA timestamp comment 'employee end date of working',
# MAGIC START_DA timestamp comment 'Employee start date of employement',
# MAGIC SUPERVISOR_FLG string comment 'flag for is employee is a supervisor',
# MAGIC TITLE_CD string comment 'Employee title code',
# MAGIC TITLE_DESC_TX string comment 'Employee title description'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/hrsa_employee'
# MAGIC ;

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
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.gold 
# MAGIC COMMENT 'For trm reports gold layer data' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.pou_audit_dashboards (
# MAGIC   attorney_name STRING,
# MAGIC   audit_interim_office_action_dt DATE,
# MAGIC   audit_no_response_office_action_dt DATE,
# MAGIC   cancellation_in BOOLEAN,
# MAGIC   country_or_area_name STRING,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   deletions_after_audit_count DECIMAL(25,0),
# MAGIC   deletions_after_audit_in BOOLEAN,
# MAGIC   em_empe_name STRING,
# MAGIC   filing_basis_cur STRING,
# MAGIC   firm_name STRING,
# MAGIC   first_audit_office_action_dt DATE,
# MAGIC   owner_name STRING,
# MAGIC   reg_classes STRING,
# MAGIC   registration_number STRING,
# MAGIC   response_oa_rec_in BOOLEAN,
# MAGIC   review_fy INT,
# MAGIC   review_fy_quarter STRING,
# MAGIC   review_month STRING,
# MAGIC   review_month_int INT,
# MAGIC   second_audit_office_action_dt DATE,
# MAGIC   serial_number STRING,
# MAGIC   third_audit_office_action_dt DATE,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   termination_dt TIMESTAMP,
# MAGIC   acceptflag_noPUM1 BOOLEAN,
# MAGIC   update_user_id STRING,
# MAGIC   first_deletion_dt TIMESTAMP,
# MAGIC   latest_deletion_dt TIMESTAMP,
# MAGIC   deletion_event_count INT,
# MAGIC   first_action_dt_ph STRING,
# MAGIC   Deletion_Date TIMESTAMP,
# MAGIC   FY_Deleted INT,
# MAGIC   PrePUM1Flag INT NOT NULL,
# MAGIC   Qtr_Deleted STRING,
# MAGIC   percentage_notice_of_acceptance DECIMAL(38,14),
# MAGIC   pct_serials_per_country DECIMAL(38,14),
# MAGIC   business_event_id INT,
# MAGIC   effective_ts TIMESTAMP,
# MAGIC   business_event_reason_cd STRING
# MAGIC )using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/pou_audit_dashboards'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.process_production_staffing_report_rolling (
# MAGIC   First_Action_Compliance_Rate DECIMAL(26,1),
# MAGIC   First_Action_Compliance_Rate_fy STRING,
# MAGIC   First_Action_Compliance_Rate_target DECIMAL(3,1),
# MAGIC   final_compliance_rate STRING,
# MAGIC   Total_compliance_rate_fy STRING,
# MAGIC   final_compliance_rate_fy DECIMAL(26,1),
# MAGIC   Total_Compliance_Rate_target DECIMAL(3,1),
# MAGIC   Exceptional_First_Action_Rate DOUBLE,
# MAGIC   Exceptional_First_Action_Rate_fy DOUBLE,
# MAGIC   Exceptional_First_Action_Rate_target DECIMAL(3,1),
# MAGIC   First_Actions_initial_exam_classes DECIMAL(22,0),
# MAGIC   Abandonment_classes DECIMAL(22,0),
# MAGIC   Approved_for_Publication_classes DECIMAL(22,0),
# MAGIC   Total_Balanced_Disposals DECIMAL(24,0),
# MAGIC   First_Actions_initial_exam_classes_fy DECIMAL(32,0),
# MAGIC   First_Actions_initial_exam_classes_fy_target INT,
# MAGIC   Abandonment_classes_fy DECIMAL(32,0),
# MAGIC   Abandonment_classes_fy_target INT,
# MAGIC   Approved_for_Publication_classes_fy DECIMAL(32,0),
# MAGIC   Approved_for_Publication_classes_fy_target INT,
# MAGIC   Total_Balanced_Disposals_fy DECIMAL(34,0),
# MAGIC   Total_Balanced_Disposals_fy_target INT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_FILED_CLASSES BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_FILED BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy_target INT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_FILED_fy BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_FILED_fy_target INT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy_target INT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy BIGINT,
# MAGIC   TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy_target INT,
# MAGIC   year INT,
# MAGIC   fy_month STRING,
# MAGIC   fy_month_int INT,
# MAGIC   fy_quarter STRING,
# MAGIC   Total_Requests_for_Extension_of_Protection BIGINT,
# MAGIC   Application_Files_filed BIGINT,
# MAGIC   Application_Files_filed_fy BIGINT,
# MAGIC   Application_Files_filed_target INT,
# MAGIC   Total_Applications_Filed_classes BIGINT,
# MAGIC   Total_Applications_Filed_classes_fy BIGINT,
# MAGIC   Total_Applications_Filed_classes_fy_actual BIGINT,
# MAGIC   Total_Applications_Filed_classes_fy_target INT,
# MAGIC   Total_Application_Files_filings_cases BIGINT,
# MAGIC   Total_Application_Files_filings_cases_fy BIGINT,
# MAGIC   Total_Application_Files_filings_cases_fy_actual BIGINT,
# MAGIC   Total_Application_Files_filings_cases_fy_target INT,
# MAGIC   filed_classes_FYTD_growth_rate DOUBLE,
# MAGIC   filed_classes_FYTD_growth_rate_target DECIMAL(2,1),
# MAGIC   filed_cases_FYTD_growth_rate DOUBLE,
# MAGIC   filed_classes_month_growth_rate DOUBLE,
# MAGIC   filed_cases_month_growth_rate DOUBLE
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/process_production_staffing_report_rolling'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.process_production_staffing_report_non_rolling (
# MAGIC   notice_of_allowance_issued_classes LONG,
# MAGIC   notice_of_allowance_issued_classes_fy LONG,
# MAGIC   notice_of_allowance_issued_classes_fy_target INTEGER,
# MAGIC   Published_for_Opposition_classes LONG,
# MAGIC   Registrations_including_Classes LONG,
# MAGIC   Certificates_of_Registration_Issued_Cases LONG,
# MAGIC   Published_for_Opposition_classes_actual LONG,
# MAGIC   Published_for_Opposition_classes_target INTEGER,
# MAGIC   Registrations_including_Classes_fy LONG,
# MAGIC   Registrations_including_Classes_fy_target INTEGER,
# MAGIC   Certificates_of_Registration_Issued_Cases_fy LONG,
# MAGIC   Certificates_of_Registration_Issued_Cases_fy_target INTEGER,
# MAGIC   Total_Pending_Applications_cases_38 LONG,
# MAGIC   Total_Pending_Applications_classes_39 LONG,
# MAGIC   Total_Pending_Applications_cases_38_fy LONG,
# MAGIC   Total_Pending_Applications_classes_39_fy LONG,
# MAGIC   Abandoned_classes_fy LONG,
# MAGIC   Abandoned_classes LONG,
# MAGIC   Abandoned_files_cases_fy LONG,
# MAGIC   Abandoned_files_cases LONG,
# MAGIC   Abandoned_classes_fy_target INTEGER,
# MAGIC   Abandoned_files_cases_fy_target INTEGER,
# MAGIC   Unexamined_New_Applicationn_cases_prior_to_first_action LONG,
# MAGIC   Unexamined_New_Applicationn_classes_prior_to_first_action LONG,
# MAGIC   Unexamined_New_Applicationn_cases_prior_to_first_action_fy LONG,
# MAGIC   Unexamined_New_Applicationn_cases_prior_to_first_action_fy_target INTEGER,
# MAGIC   Unexamined_New_Applicationn_classes_prior_to_first_action_fy LONG,
# MAGIC   Unexamined_New_Applicationn_classes_prior_to_first_action_fy_target INTEGER,
# MAGIC   median_age_of_inventory DECIMAL(15,2),
# MAGIC   Median_age_of_inventory_fy DECIMAL(15,2),
# MAGIC   Median_age_of_inventory_fy_target DECIMAL(2,1),
# MAGIC   year STRING,
# MAGIC   fy_quarter STRING,
# MAGIC   fy_month STRING,
# MAGIC   fy_month_int INTEGER,
# MAGIC   Pendency_to_First_Action_month DOUBLE,
# MAGIC   Pendency_to_First_Action_fy DOUBLE,
# MAGIC   First_Action_target_fy DECIMAL(2,1),
# MAGIC   Pendency_to_Registration_Abandonment_NOA_Exc DOUBLE,
# MAGIC   Pendency_to_Reg_fy_exc DOUBLE,
# MAGIC   Pendency_to_Reg_Target_fy_EXC DECIMAL(11,1),
# MAGIC   Pendency_to_Registration_Abandonment_NOA_INC DOUBLE,
# MAGIC   Pendency_to_Reg_fy_inc DOUBLE,
# MAGIC   Pendency_to_Reg_Target_fy_inc DECIMAL(3,1),
# MAGIC   total_pendency_reg_135 DOUBLE,
# MAGIC   total_pendency_reg_fy_135a DOUBLE,
# MAGIC   total_pendency_noa_136 DOUBLE,
# MAGIC   total_pendency_noa_fy_136a DOUBLE,
# MAGIC   Section_9_Applications_Filed LONG,
# MAGIC   Registrations_Renewed LONG,
# MAGIC   Affidavits_under_Section_8_15_71_Combinations_Filed LONG,
# MAGIC   Affidavits_under_Section_8_15_71_Combinations_Disposed LONG,
# MAGIC   Section_8_Applications_Filed_10yr LONG,
# MAGIC   Section_9_Applications_Filed_fy LONG,
# MAGIC   Section_9_Applications_Filed_fy_target INTEGER,
# MAGIC   Registrations_Renewed_fy LONG,
# MAGIC   Registrations_Renewed_target INTEGER,
# MAGIC   Affidavits_under_Section_8_15_71_Combinations_Filed_fy LONG,
# MAGIC   Affidavits_under_Section_8_15_71_Combinations_Filed_fy_target INTEGER,
# MAGIC   Affidavits_under_Section_8_15_71_Combinations_Disposed_fy LONG,
# MAGIC   Affidavits_under_Section_8_15_71_Combinations_Disposed_fy_target INTEGER,
# MAGIC   Section_8_Applications_Filed_10yr_fy LONG,
# MAGIC   Section_8_Applications_Filed_10yr_fy_target INTEGER
# MAGIC ) using DELTA  
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/process_production_staffing_report_non_rolling'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.tmns_notice_counts (
# MAGIC date_time_range STRING,
# MAGIC abandonment_notice_appeal_terminated INT,
# MAGIC  corrected_notice_of_allowance_email INT,
# MAGIC  corrected_notice_of_allowance_paper INT,
# MAGIC  courtesy_e_reminder_of_sec71_10_yr INT,
# MAGIC  courtesy_e_reminder_of_sec71_6_yr INT,
# MAGIC  courtesy_e_reminder_of_sec8_6_yr INT,
# MAGIC  courtesy_e_reminder_of_sec8_sec9 INT,
# MAGIC  dsc_corr_project INT,
# MAGIC  design_search_code_corr_project INT,
# MAGIC  duplicate_notice_of_allowance_email INT,
# MAGIC  emails_for_notice_of_acceptance_and_acknowledgement_for_section_71_and_15 INT,
# MAGIC  emails_for_notice_of_acceptance_for_section_71 INT,
# MAGIC  filing_receipt INT,
# MAGIC  filing_receipt_email_trademark_application INT,
# MAGIC  mails_trademark_registration_cancelled_in_part_sec_71 INT,
# MAGIC  notice_of_abandonment_ttab_ex_partes INT,
# MAGIC  notice_of_publication INT,
# MAGIC  notice_termination INT,
# MAGIC  notice_of_aau_acceptance INT,
# MAGIC  notice_of_abandonment INT,
# MAGIC  notice_of_abandonment_after_inter_partes INT,
# MAGIC  notice_of_abandonment_after_pub INT,
# MAGIC  notice_of_abandonment_failure_to_file INT,
# MAGIC  notice_of_abandonment_sou INT,
# MAGIC  notice_of_abandonment_of_request_for_extension_of_protection INT,
# MAGIC  notice_of_acceptance_sec_71_15 INT,
# MAGIC  notice_of_acceptance_section_8_email INT,
# MAGIC  notice_of_acceptance_section_8_paper INT,
# MAGIC  notice_of_acceptance_of_sou INT,
# MAGIC  notice_of_acceptance_acknowledgement_sect_8_15_email INT,
# MAGIC  notice_of_acceptance_acknowledgement_sect_8_15_paper INT,
# MAGIC  notice_of_acceptance_renewal_sect_8_9_email INT,
# MAGIC  notice_of_acceptance_renewal_sect_8_9_paper INT,
# MAGIC  notice_of_acknowledgement_sect_15_email INT,
# MAGIC  notice_of_allowance INT,
# MAGIC  notice_of_cancellation_full_email INT,
# MAGIC  notice_of_cancellation_full_paper INT,
# MAGIC  notice_of_cancellation_partial_email INT,
# MAGIC  notice_of_cancellation_partial_paper INT,
# MAGIC  notice_of_cancellation_sec71_email INT,
# MAGIC  notice_of_cancellation_sec8_email INT,
# MAGIC  notice_of_cancellation_sec8_paper INT,
# MAGIC  notice_of_cancellation_of_registered_extension_of_protection INT,
# MAGIC  notice_of_design_search_code INT,
# MAGIC  notice_of_express_abandonment INT,
# MAGIC  notice_of_itu_extension_approval INT,
# MAGIC  notice_of_publication_12a_paper INT,
# MAGIC  notice_of_publication_12c_email INT,
# MAGIC  notice_of_registration_email INT,
# MAGIC  notice_of_renewal INT,
# MAGIC  notice_of_suspension_r_i INT,
# MAGIC  notice_of_updated_registration_email INT,
# MAGIC  notification_of_notice_of_publication_email INT,
# MAGIC  notice_sec71_15_acpt INT,
# MAGIC  notice_of_acknowledgement_sect_15_paper INT,
# MAGIC  notice_of_publication_12a_email INT,
# MAGIC month STRING,
# MAGIC report_type STRING,
# MAGIC total_notices_sent_in_email STRING,
# MAGIC total_notices_sent_in_letter STRING,
# MAGIC total_records	INT,
# MAGIC year INT,
# MAGIC input_format string,
# MAGIC create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC update_ts TIMESTAMP,
# MAGIC update_user_id STRING
# MAGIC ) using DELTA 
# MAGIC partitioned by (Report_Type) 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmns_notice_counts'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Third Level Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ttab_detail (
# MAGIC   serial_number STRING COMMENT 'Unique identifier for the trademark application',
# MAGIC   ttab_issue_type STRING COMMENT 'Type of issue being addressed in the TTAB proceeding',
# MAGIC   proceeding_num STRING COMMENT 'Unique identifier for the TTAB proceeding',
# MAGIC   filing_date DATE COMMENT 'Date the trademark application was filed',
# MAGIC   instituted_date DATE COMMENT 'Date the TTAB proceeding was instituted',
# MAGIC   instituted_code STRING COMMENT 'Code indicating the reason the proceeding was instituted',
# MAGIC   decision_date DATE COMMENT 'Date the decision was made on the proceeding',
# MAGIC   decision_code STRING COMMENT 'Code representing the type of decision made',
# MAGIC   decision_description STRING COMMENT 'Description of the decision made',
# MAGIC   termination_code STRING COMMENT 'Code indicating the reason for termination of the proceeding',
# MAGIC   termination_date DATE COMMENT 'Date the proceeding was terminated',
# MAGIC   termination_date_2 DATE COMMENT 'Date the proceeding was terminated 2',
# MAGIC   termination_date_3 DATE COMMENT 'Date the proceeding was terminated 3',
# MAGIC   termination_date_4 DATE COMMENT 'Date the proceeding was terminated 4',
# MAGIC   termination_date_5 DATE COMMENT 'Date the proceeding was terminated 5',
# MAGIC   final_refusal_date DATE COMMENT 'Date of final refusal, if applicable',
# MAGIC   fp_reason_1 STRING COMMENT 'First reason for final refusal',
# MAGIC   fp_reason_2 STRING COMMENT 'Second reason for final refusal',
# MAGIC   fp_reason_3 STRING COMMENT 'Third reason for final refusal',
# MAGIC   fp_reason_4 STRING COMMENT 'Fourth reason for final refusal',
# MAGIC   fp_reason_5 STRING COMMENT 'Fifth reason for final refusal',
# MAGIC   pendency_d BIGINT COMMENT 'Pendency in days between decision date and instituted date',
# MAGIC   pendency_t BIGINT COMMENT 'Pendency in days between termination date and instituted date',
# MAGIC   pendency_r BIGINT COMMENT 'N/A',
# MAGIC   inventory BOOLEAN COMMENT 'Indicates if the case is part of the inventory',
# MAGIC   non_pro_se STRING COMMENT 'Indicates if the case is non-pro se',
# MAGIC   pctram_link STRING COMMENT 'Link to PCTRAM record',
# MAGIC   law_office STRING COMMENT 'Law office handling the case',
# MAGIC   filing_basis_grp STRING COMMENT 'Group of filing basis',
# MAGIC   filing_method_cur STRING COMMENT 'Current filing method',
# MAGIC   am_stat INT COMMENT 'Amendment status',
# MAGIC   owner_name STRING COMMENT 'Name of the trademark owner',
# MAGIC   city STRING COMMENT 'City of the trademark owner',
# MAGIC   state STRING COMMENT 'State of the trademark owner',
# MAGIC   country_or_area_name STRING COMMENT 'Country or area of the trademark owner',
# MAGIC   reg_class_count BIGINT COMMENT 'Count of registered classes',
# MAGIC   active_class_count BIGINT COMMENT 'Count of active classes',
# MAGIC   group_type STRING COMMENT 'Type of group',
# MAGIC   concat_class STRING COMMENT 'Concatenated class information',
# MAGIC   mark_nm_short STRING COMMENT 'Short name of the mark',
# MAGIC   refusal BOOLEAN COMMENT 'Indicates if there was a refusal',
# MAGIC   appeal BOOLEAN COMMENT 'Indicates if there was an appeal',
# MAGIC   publication_date DATE COMMENT 'Date of publication',
# MAGIC   pubs BOOLEAN COMMENT 'Indicates if published',
# MAGIC   opposition BOOLEAN COMMENT 'Indicates if there was an opposition',
# MAGIC   default_opposition BOOLEAN COMMENT 'Indicates if there was a default opposition',
# MAGIC   default_cancellation BOOLEAN COMMENT 'Indicates if there was a default cancellation',
# MAGIC   cancellation BOOLEAN COMMENT 'Indicates if there was a cancellation',
# MAGIC   constructed_prcd_num STRING COMMENT 'Constructed proceeding number',
# MAGIC   default_date DATE COMMENT 'Date of default',
# MAGIC   cancellation_count BIGINT COMMENT 'Count of cancellations',
# MAGIC   reg_yr STRING COMMENT 'Registration year',
# MAGIC   live_reg_count BIGINT COMMENT 'Count of live registrations',
# MAGIC   can_rate DOUBLE COMMENT 'Cancellation rate',
# MAGIC   concurrent BOOLEAN COMMENT 'Indicates if concurrent',
# MAGIC   rfd_date DATE COMMENT 'Date of refusal',
# MAGIC   rfd_valid BOOLEAN COMMENT 'Indicates if the refusal date is valid',
# MAGIC   proceeding_count INT COMMENT 'Count of proceedings',
# MAGIC   case_age_rfd BIGINT COMMENT 'Age of the case at refusal date',
# MAGIC   case_age_category STRING COMMENT 'Category of case age',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ttab_detail'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.filings_dashboard (
# MAGIC   ser_num integer,
# MAGIC   pendency_cal_start_dt date,
# MAGIC   filing_fy integer,
# MAGIC   non_pro_se string,
# MAGIC   filing_method_filed string,
# MAGIC   filing_basis_grp string,
# MAGIC   class string,
# MAGIC   name string,
# MAGIC   city string,
# MAGIC   ste_ctry_cd string,
# MAGIC   postal_cd string,
# MAGIC   ctry_nm string,
# MAGIC   country_or_area_name string,
# MAGIC   count integer,
# MAGIC   max_pendency_cal_start_dt date,
# MAGIC   coordinated_class string,
# MAGIC   filing_fy2 integer,
# MAGIC   filing_fy_month_int integer,
# MAGIC   filing_fy_quarter string,
# MAGIC   filing_fy_month string,
# MAGIC   top_2_years boolean,
# MAGIC   fee_paid_class integer,
# MAGIC   max_filing_fy integer,
# MAGIC   pctram_link string,
# MAGIC   fixed_count integer,
# MAGIC   realtime_count integer,
# MAGIC   tram_count integer,
# MAGIC   goods_or_services string,
# MAGIC   concat_goods_or_services string,
# MAGIC   entity_type string,
# MAGIC   applicant_bin string,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING,
# MAGIC   output_record_count integer
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/filings_dashboard' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.goods_services_dashboard (
# MAGIC   ser_num integer,
# MAGIC   class string,
# MAGIC   coordinated_class string,
# MAGIC   pendency_cal_start_dt date,
# MAGIC   filing_fy string,
# MAGIC   non_pro_se string,
# MAGIC   filing_method_filed string,
# MAGIC   filing_basis_grp string,
# MAGIC   ste_ctry_cd string,
# MAGIC   country_or_area_name string,
# MAGIC   max_pendency_cal_start_dt date,
# MAGIC   filing_fy_quarter string,
# MAGIC   filing_fy_month string,
# MAGIC   entity_type string,
# MAGIC   applicant_bin string,
# MAGIC   goods_or_services string,
# MAGIC   goods_services_desc string,
# MAGIC   class_count integer,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/goods_services_dashboard' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.pendency_dashboard (
# MAGIC   first_action_pendency_ph DOUBLE COMMENT '',
# MAGIC   first_action_dt_ph DATE COMMENT '',
# MAGIC   first_action_type_num STRING COMMENT '',
# MAGIC   abandonment_dt DATE COMMENT '',
# MAGIC   active_classes_disposal BIGINT COMMENT '',
# MAGIC   active_classes_firstaction BIGINT COMMENT '',
# MAGIC   am_stat INT COMMENT '',
# MAGIC   country_or_area_name STRING COMMENT '',
# MAGIC   ctry_nm STRING COMMENT '',
# MAGIC   days_in_dock INT COMMENT '',
# MAGIC   disposal_dt DATE COMMENT '',
# MAGIC   disposal_pendency DOUBLE COMMENT '',
# MAGIC   disposal_type STRING COMMENT '',
# MAGIC   fa_pendency_filter BOOLEAN COMMENT '',
# MAGIC   fa_pendency_fy STRING COMMENT '',
# MAGIC   fa_pendency_fy_month STRING COMMENT '',
# MAGIC   fa_pendency_fy_quarter STRING COMMENT '',
# MAGIC   filing_basis_grp STRING COMMENT '',
# MAGIC   filing_method_filed STRING COMMENT '',
# MAGIC   first_action_type STRING COMMENT '',
# MAGIC   last_modified_date DATE COMMENT '',
# MAGIC   law_office STRING COMMENT '',
# MAGIC   max_action_dt DATE COMMENT '',
# MAGIC   noa_dt DATE COMMENT '',
# MAGIC   non_pro_se STRING COMMENT '',
# MAGIC   on_hold BOOLEAN COMMENT '',
# MAGIC   pctram_link STRING COMMENT '',
# MAGIC   pendency_cal_end_dt DATE COMMENT '',
# MAGIC   pendency_cal_start_dt DATE COMMENT '',
# MAGIC   pendency_category STRING COMMENT '',
# MAGIC   postal_cd STRING COMMENT '',
# MAGIC   registration_dt DATE COMMENT '',
# MAGIC   ser_num STRING COMMENT '',
# MAGIC   ste_ctry_cd STRING COMMENT '',
# MAGIC   total_pendency_fy STRING COMMENT '',
# MAGIC   total_pendency_fy_filter BOOLEAN COMMENT '',
# MAGIC   total_pendency_fy_month STRING COMMENT '',
# MAGIC   total_pendency_fy_quarter STRING COMMENT '',
# MAGIC   total_pendency_fy_date DATE COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING,
# MAGIC   output_record_count integer
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/pendency_dashboard' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.post_reg_dashboard (
# MAGIC   serial_number STRING COMMENT 'Unique identifier assigned to the trademark registration.',
# MAGIC   registration_dt DATE COMMENT 'Date when the registration was recorded.',
# MAGIC   six_yr_dt DATE COMMENT 'Represents the date when six years have passed.',
# MAGIC   last_10yr_dt DATE COMMENT 'Date of the last 10-year milestone achieved.',
# MAGIC   next_10yr_renewal STRING COMMENT 'Date for the next 10-year renewal period.',
# MAGIC   number_renewals INT COMMENT 'Total count of times a trademark has been renewed.',
# MAGIC   next_6yr_dt DATE COMMENT 'Date of the next six-year milestone from the current date.',
# MAGIC   expiration_dt DATE COMMENT 'Represents the date when the item or record is set to expire.',
# MAGIC   expiration_type STRING COMMENT 'Indicates the type of expiration, such as date-based or event-based, that applies to the associated entity.',
# MAGIC   registration_number STRING COMMENT 'Unique identifier assigned to each registration.',
# MAGIC   am_dt_cncl STRING COMMENT 'Date of cancellation for the agreement or contract.',
# MAGIC   live_registration INT COMMENT 'Indicates whether the registration is currently live.',
# MAGIC   expiration_dt_realtime DATE COMMENT 'Date and time when the trademark is set to expire.  ',
# MAGIC   expiration_type_realtime STRING COMMENT 'Indicates the type of real-time expiration, such as immediate or scheduled, for time-sensitive data or events.',
# MAGIC   live_reg BOOLEAN COMMENT 'Indicates whether the registration is currently live.',
# MAGIC   exp_fy STRING COMMENT 'Fiscal year in which the item or agreement is set to expire.',
# MAGIC   exp_fy_rt STRING COMMENT 'Fiscal year of real-time expiration.',
# MAGIC   reg_fy STRING COMMENT 'Fiscal year in which the registration occurred.',
# MAGIC   today DATE COMMENT 'The current date (when the ETL was run for this table).',
# MAGIC   today_fy STRING COMMENT 'The fiscal year corresponding current date (when the ETL was run for this table).',
# MAGIC   fy_exp_diff INT COMMENT 'Difference between the fiscal year of expiration and the current fiscal year.',
# MAGIC   fy_reg_diff INT COMMENT 'Represents the difference between the fiscal year of registration and the current fiscal year.',
# MAGIC   six_yr_fy STRING COMMENT 'Represents the fiscal year in which a six-year milestone occurs.',
# MAGIC   ten_yr_fy STRING COMMENT 'Represents the fiscal year in which a ten-year milestone occurs.',
# MAGIC   include_6yr_avg BOOLEAN COMMENT 'Indicates whether to include the six-year average in the calculation.',
# MAGIC   include_10yr_avg BOOLEAN COMMENT 'Indicates whether to include the ten-year average in the calculation.',
# MAGIC   max_today_fy STRING COMMENT 'Maximum fiscal year based on the current date.',
# MAGIC   reg_age INT COMMENT 'The reg_age column represents the age of the registration in years.',
# MAGIC   average_life_include BOOLEAN COMMENT 'Indicates whether to include the average life in the calculation.',
# MAGIC   sixyr_num INT COMMENT 'Numerator used in the six-year calculation. Used for reporting purposes.',
# MAGIC   sixyr_denom INT COMMENT 'Denominator used for calculating six-year rates and percentages. Used for reporting purposes.',
# MAGIC   tenyr_num INT COMMENT 'Numerator used in calculating the ten-year value. Used for reporting purposes.',
# MAGIC   tenyr_denom INT COMMENT 'Denominator used in calculating ten-year values. Used for reporting purposes.',
# MAGIC   twentyyr_num INT COMMENT 'Numerator used in the twenty-year calculation. Used for reporting purposes.',
# MAGIC   twentyyr_denom INT COMMENT 'Denominator used for calculating twenty-year values. Used for reporting purposes.',
# MAGIC   thirtyyr_num INT COMMENT 'Numerator used in the calculation of thirty-year values. Used for reporting purposes.',
# MAGIC   thirtyyr_denom INT COMMENT 'Denominator used for calculating thirty-year values. Used for reporting purposes.',
# MAGIC   fortyyr_num INT COMMENT 'Numerator used in the forty-year calculation. Used for reporting purposes.',
# MAGIC   fortyyr_denom INT COMMENT 'Denominator used in calculating forty-year metrics. Used for reporting purposes.',
# MAGIC   fiftyyr_num INT COMMENT 'Numerator used in the fifty-year calculation. Used for reporting purposes.',
# MAGIC   fiftyyr_denom INT COMMENT 'Denominator used in calculating fifty-year values.',
# MAGIC   milestone INT COMMENT 'Indicates a significant event or achievement in a project or process.',
# MAGIC   pendency_cal_start_dt DATE COMMENT 'Date when pendency calculation begins.',
# MAGIC   non_pro_se STRING COMMENT 'Indicates whether the case involves a self-represented party or not.',
# MAGIC   pctram_link STRING COMMENT 'This column contains the link to the PCTRAM resource for further information.',
# MAGIC   law_office STRING COMMENT 'This column represents the law office responsible for handling the case.',
# MAGIC   filing_basis_grp STRING COMMENT 'This column represents the group of filing basis used for reporting purposes.',
# MAGIC   filing_method_cur STRING COMMENT 'Current filing method associated with the trademark.',
# MAGIC   am_stat INT COMMENT 'The status code derived from TRAM indicating the state of the trademark case.',
# MAGIC   owner_name STRING COMMENT 'The owner_name column stores the name of the entity that owns the record.',
# MAGIC   city STRING COMMENT 'The city of the trademark owner for this registration.',
# MAGIC   state STRING COMMENT 'The state of the trademark owner.',
# MAGIC   country_or_area_name STRING COMMENT 'The country or region name of the trademark\'s owner.',
# MAGIC   reg_class_count BIGINT COMMENT 'Number of distinct registration classes.',
# MAGIC   active_class_count BIGINT COMMENT 'Represents the total number of active classes.',
# MAGIC   group_type STRING COMMENT 'Indicates the category or classification of the group, such as public, private, or restricted.',
# MAGIC   concat_class STRING COMMENT 'Stores concatenated class information for easy reference.',
# MAGIC   mark_nm_short STRING COMMENT 'Abbreviated name associated with the mark.',
# MAGIC   max_dt_filter BOOLEAN COMMENT 'Indicates whether to apply a maximum date filter to the data. Used for reporting purposes.',
# MAGIC   create_ts TIMESTAMP COMMENT 'Stores the timestamp when the record was created.',
# MAGIC   create_user_id STRING COMMENT 'Unique identifier of the user or system created the record.',
# MAGIC   update_ts TIMESTAMP COMMENT 'Stores the timestamp of the record\'s last update.',
# MAGIC   update_user_id STRING COMMENT 'Stores the ID of the user or system last updated the record.'
# MAGIC ) USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/post_reg_dashboard'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.post_reg_detail_dashboard (
# MAGIC   recordid INT COMMENT 'Unique identifier for the record',
# MAGIC   serial_number STRING COMMENT 'Unique identifier assigned to the registration.',
# MAGIC   registration_dt DATE COMMENT 'Date when the registration was recorded.',
# MAGIC   registration_number STRING COMMENT 'Unique identifier assigned to a registered trademark.',
# MAGIC   postreg_category STRING COMMENT 'Category indicating the type of action taken after initial registration.',
# MAGIC   start_action_number INT COMMENT 'Indicates the starting point for action numbering.',
# MAGIC   end_action_number INT COMMENT 'Identifies the final action number in a sequence of actions.',
# MAGIC   start_action_date DATE COMMENT 'Represents the date when the action is initiated.',
# MAGIC   end_action_date DATE COMMENT 'Represents the date when the action is completed or concluded.',
# MAGIC   start_5_characters STRING COMMENT 'The 5 character prosecution history event code for the first action.',
# MAGIC   end_5_characters STRING COMMENT 'The 5 character prosecution history event code for the last action.',
# MAGIC   start_cm_desc STRING COMMENT 'The full prosecution history event description for the first action.',
# MAGIC   end_cm_desc STRING COMMENT 'The full prosecution history prosecution history event code for the last action.',
# MAGIC   fifteen_flag BOOLEAN COMMENT 'Indicates a specific condition when set to a non-zero value.',
# MAGIC   inventory BOOLEAN COMMENT 'Tracks the current status of inventory levels.',
# MAGIC   first_action_date DATE COMMENT 'Records the date of the initial action taken.',
# MAGIC   first_action_code STRING COMMENT 'Unique identifier for the initial action taken.',
# MAGIC   renewal_dt DATE COMMENT 'Date when the renewal is scheduled to occur or occured.',
# MAGIC   renewal_number INT COMMENT 'Unique number of renewals for the given trademark.',
# MAGIC   first_action_pendency BIGINT COMMENT 'The first_action_pendency column represents the time elapsed until the initial action is taken.',
# MAGIC   total_pendency BIGINT COMMENT 'Total pendency represents the cumulative amount of time that cases have been pending.',
# MAGIC   max_max_dt DATE COMMENT 'Maximum date stored in the dataset.',
# MAGIC   expiration_type_realtime2 STRING COMMENT 'Specifies the real-time expiration type for automatic data removal.',
# MAGIC   expiration_dt_realtime2 DATE COMMENT 'The expiration_dt_realtime2 column stores the real-time expiration date of a record.',
# MAGIC   max_fy_ph STRING COMMENT 'The latest fiscal year of the prosecution history date.',
# MAGIC   sixyr_disposed_count INT COMMENT 'Number of cases disposed within a six-year timeframe. Used for reporting purposes.',
# MAGIC   sixyr_base INT COMMENT 'Base number used for six-year calculations. Used for reporting purposes.',
# MAGIC   tenyr_disposed_count INT COMMENT 'Number of cases disposed within the last ten years. Used for reporting purposes.',
# MAGIC   tenyr_base INT COMMENT 'Base number used for ten-year calculations. Used for reporting purposes.',
# MAGIC   end_action_fy STRING COMMENT 'Represents the fiscal year in which the end action occurred.',
# MAGIC   ser_num STRING COMMENT 'Unique identifier assigned to a trademark.',
# MAGIC   pendency_cal_start_dt DATE COMMENT 'Date when pendency calculation begins.',
# MAGIC   non_pro_se STRING COMMENT 'Indicates whether the case involves a self-represented party or not.',
# MAGIC   pctram_link STRING COMMENT 'This column contains a link to the PCTRAM resource for further information.',
# MAGIC   law_office STRING COMMENT 'This column represents the law office responsible for handling the case.',
# MAGIC   filing_basis_grp STRING COMMENT 'This column represents the group of filing basis.',
# MAGIC   filing_method_cur STRING COMMENT 'Current filing method used for submissions.',
# MAGIC   am_stat INT COMMENT 'Status code representing the current state of the trademark application. Derived from legacy TRAM codes.',
# MAGIC   owner_name STRING COMMENT 'The name of the trademark owner.',
# MAGIC   city STRING COMMENT 'The city of the trademark owner for this registration.',
# MAGIC   state STRING COMMENT 'The state of the trademark owner.',
# MAGIC   country_or_area_name STRING COMMENT 'This column contains the name of the country or geographic area of the trademark owner.',
# MAGIC   reg_class_count BIGINT COMMENT 'Number of distinct classes at the time of registration.',
# MAGIC   active_class_count BIGINT COMMENT 'Represents the total number of active classes.',
# MAGIC   group_type STRING COMMENT 'Indicates the group type of the registration.',
# MAGIC   fa_percentile FLOAT COMMENT 'Represents the percentile ranking of the first action taken.',
# MAGIC   right_recordid INT COMMENT 'Unique identifier for the record, used during the ETL for joining.',
# MAGIC   fa_percentile_include BOOLEAN COMMENT 'Indicates whether to include the first action percentile.  Used for reporting purposes.',
# MAGIC   tp_percentile FLOAT COMMENT 'Represents the percentile of total pendency.',
# MAGIC   tp_percentile_include BOOLEAN COMMENT 'Indicates whether to include the total pendency percentile in the calculation. Used for reporting purposes.',
# MAGIC   top10_fy_exclude_cfy BOOLEAN COMMENT 'Indicates the top 10 fiscal year registrations, excluding the current fiscal year.  Used for reporting purposes.',
# MAGIC   top5_fy_exclude_cfy BOOLEAN COMMENT 'Indicates the top 5 fiscal year registrations, excluding the current fiscal year.  Used for reporting purposes.',
# MAGIC   renewal_number_grp STRING COMMENT 'Represents a group of renewal numbers associated with a specific entity or policy.',
# MAGIC   category STRING COMMENT 'Displays the category or classification of the item or topic being discussed.',
# MAGIC   concat_class STRING COMMENT 'Stores concatenated class information for easy reference.',
# MAGIC   first_action_inventory BOOLEAN COMMENT 'Indicates the inventory when the first action was taken.',
# MAGIC   reg_fy STRING COMMENT 'Fiscal year in which the registration occurred.',
# MAGIC   drop_off_year BOOLEAN COMMENT 'Indicates the year in which the registration dropped.',
# MAGIC   create_ts TIMESTAMP COMMENT 'Stores the timestamp when the record was created.',
# MAGIC   create_user_id STRING COMMENT 'Unique identifier of the user or system that created the record.',
# MAGIC   update_ts TIMESTAMP COMMENT 'Stores the timestamp of the record\'s last update.',
# MAGIC   update_user_id STRING COMMENT 'Stores the ID of the user or system that last updated the record.'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/post_reg_detail_dashboard' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.quality_dashboard (
# MAGIC   law_office STRING COMMENT 'The name of the law office',
# MAGIC   lastreviewdatetime DATE COMMENT 'The date of the last review',
# MAGIC   searchsufficientindicator BOOLEAN COMMENT 'Indicator of whether the search is sufficient',
# MAGIC   qualitymetricdeficientindicator BOOLEAN COMMENT 'Indicator of deficient quality metric',
# MAGIC   mississueindicator BOOLEAN COMMENT 'Indicator of missing issue',
# MAGIC   newissueindicator BOOLEAN COMMENT 'Indicator of new issue',
# MAGIC   refusalunsoundindicator BOOLEAN COMMENT 'Indicator of unsound refusal',
# MAGIC   substantivedeficientindicator BOOLEAN COMMENT 'Indicator of deficient substantive',
# MAGIC   proceduraldeficientindicator BOOLEAN COMMENT 'Indicator of deficient procedural',
# MAGIC   overalldeficientindicator BOOLEAN COMMENT 'Indicator of overall deficiency',
# MAGIC   overallexcellentindicator BOOLEAN COMMENT 'Indicator of overall excellence',
# MAGIC   evidencedeficientindicator BOOLEAN COMMENT 'Indicator of deficient evidence',
# MAGIC   evidencesatisfactoryindicator BOOLEAN COMMENT 'Indicator of satisfactory evidence',
# MAGIC   evidenceexcellentindicator BOOLEAN COMMENT 'Indicator of excellent evidence',
# MAGIC   writingdeficientindicator BOOLEAN COMMENT 'Indicator of deficient writing',
# MAGIC   writingsatisfactoryindicator BOOLEAN COMMENT 'Indicator of satisfactory writing',
# MAGIC   writingexcellentindicator BOOLEAN COMMENT 'Indicator of excellent writing',
# MAGIC   substantiveerrorindicator BOOLEAN COMMENT 'Indicator of substantive error',
# MAGIC   satisfactoryindicator BOOLEAN COMMENT 'Indicator of satisfactory quality',
# MAGIC   findingindicator BOOLEAN COMMENT 'Indicator of finding',
# MAGIC   go_final STRING COMMENT 'The final status of the case',
# MAGIC   quality_review_id STRING COMMENT 'The ID of the quality review',
# MAGIC   review_type STRING COMMENT 'The type of review',
# MAGIC   final_compliance BOOLEAN COMMENT 'Indicator of final compliance',
# MAGIC   qualitymetricdeficientflag STRING COMMENT 'Flag indicating deficient quality metric',
# MAGIC   excellentflag STRING COMMENT 'Flag indicating excellence',
# MAGIC   max_date DATE COMMENT 'The maximum date',
# MAGIC   fy_date_current DATE COMMENT 'The current fiscal year date',
# MAGIC   current_fy STRING COMMENT 'The current fiscal year',
# MAGIC   current_fy_int INT COMMENT 'The current fiscal year as an integer',
# MAGIC   fy_date DATE COMMENT 'The fiscal year date',
# MAGIC   fy_date_string STRING COMMENT 'The fiscal year date as a string',
# MAGIC   fy_month STRING COMMENT 'The fiscal year month',
# MAGIC   fy_month_int INT COMMENT 'The fiscal year month as an integer',
# MAGIC   fy_quarter STRING COMMENT 'The fiscal year quarter',
# MAGIC   first_action_type STRING COMMENT 'The type of first action',
# MAGIC   disposal_type STRING COMMENT 'The type of disposal',
# MAGIC   pendency_cal_start_dt DATE COMMENT 'The start date for pendency calculation',
# MAGIC   pendency_cal_end_dt DATE COMMENT 'The end date for pendency calculation',
# MAGIC   non_pro_se STRING COMMENT 'Indicator of non-pro se',
# MAGIC   country_or_area_name STRING COMMENT 'The name of the country or area',
# MAGIC   filing_basis_grp STRING COMMENT 'The filing basis group',
# MAGIC   filing_method_filed STRING COMMENT 'The filing method filed',
# MAGIC   ste_ctry_cd STRING COMMENT 'The country code',
# MAGIC   concat_class STRING COMMENT 'The concatenated class',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/quality_dashboard' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ttab_decision_rates (
# MAGIC   fiscal_year INTEGER COMMENT 'The fiscal year of the decision',
# MAGIC   case_end_dt DATE COMMENT 'The date when the case ended',
# MAGIC   ttab_case_type STRING COMMENT 'The type of the TTAB case',
# MAGIC   total_decisions BIGINT COMMENT 'The total number of decisions',
# MAGIC   total_judge_decisions BIGINT COMMENT 'The total number of decisions made by judges',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ttab_decision_rates'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table trm_reporting_dev.gold.ep_query_tept_dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ep_query_tept_dashboard (
# MAGIC   ep_exmr_num	string COMMENT 'Examiner Number',
# MAGIC   ep_pp_period	string COMMENT 'Pay Period from when bd is accounted' ,
# MAGIC   ep_ser_num_list	string COMMENT 'All the serial numbers which are accounted for BD',
# MAGIC   ep_query_balanced_disposals	DECIMAL COMMENT 'Balanced Disposal count from ep_query',
# MAGIC   ename	string COMMENT 'Name of the examiner',
# MAGIC   bi_week_id	string COMMENT 'BI week number as per TEPT',
# MAGIC   tept_balanced_disposals	DECIMAL COMMENT 'Balanced Disposal count from TEPT',
# MAGIC   law_office	string COMMENT 'Law office number' 
# MAGIC
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ep_query_tept_dashboard'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ttab_workloads (
# MAGIC   fiscal_year INTEGER COMMENT 'The fiscal year for the data',
# MAGIC   date DATE COMMENT 'The date of the record',
# MAGIC   ttab_case_type STRING COMMENT 'The type of TTAB case',
# MAGIC   day_total DOUBLE COMMENT 'Total cases or actions for the day',
# MAGIC   actual_estimated STRING COMMENT 'Indicates whether the data is actual or estimated',
# MAGIC   fy_base_total BIGINT COMMENT 'The base total for the fiscal year',
# MAGIC   fy_judge_decisions BIGINT COMMENT 'Number of judge decisions in the fiscal year',
# MAGIC   fy_jdr DOUBLE COMMENT 'Judge decision rate for the fiscal year',
# MAGIC   latest_5yr_avg_jdr DOUBLE COMMENT 'Average judge decision rate over the last 5 years',
# MAGIC   raw_credits BIGINT COMMENT 'Raw credits earned',
# MAGIC   credits_jdr_applied DOUBLE COMMENT 'Credits applied towards the judge decision rate',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ttab_workloads'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.inventory_unexamined_hstry(
# MAGIC   unexamined_date DATE COMMENT 'Date of unexamined cases',
# MAGIC   unexamined_cases BIGINT COMMENT 'Number of unexamined cases',
# MAGIC   unexamined_classes BIGINT COMMENT 'Number of unexamined classes',
# MAGIC   fy STRING COMMENT 'FiscL YEAR',
# MAGIC   ea_examining INT COMMENT 'Count of entities currently being examined',
# MAGIC   ea_unexamined_ratio INT COMMENT 'Ratio of unexamined classes',
# MAGIC   current_fy BOOLEAN COMMENT 'Current Fiscal year',
# MAGIC   create_ts TIMESTAMP COMMENT'Timestamp when the record was created' ,
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_unexamined_hstry' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.form_paragraph_dashboard(
# MAGIC   generated_date DATE COMMENT '',
# MAGIC   category STRING COMMENT '',
# MAGIC   grade INT COMMENT '',
# MAGIC   data_through_date DATE COMMENT '',
# MAGIC   serial_number STRING COMMENT '',
# MAGIC   group_name STRING COMMENT '',
# MAGIC   completed_date DATE COMMENT '',
# MAGIC   transaction_literal STRING COMMENT '',
# MAGIC   action_count INT COMMENT '',
# MAGIC   form_paragraph_id STRING COMMENT '',
# MAGIC   title_text STRING COMMENT '',
# MAGIC   foreign_key_form_paragraph_group_id STRING COMMENT '',
# MAGIC   foreign_key_form_paragraph_category_id STRING COMMENT '',
# MAGIC   form_paragraph_year STRING COMMENT '',
# MAGIC   toc_link STRING COMMENT '',
# MAGIC   concat_form_paragraph_id STRING COMMENT '',
# MAGIC   concat_category STRING COMMENT '',
# MAGIC   first_action_count_numerator INT COMMENT '',
# MAGIC   first_action_count_denominator INT COMMENT '',
# MAGIC   filing_basis_group STRING COMMENT '',
# MAGIC   exam STRING COMMENT '',
# MAGIC   action_type STRING COMMENT '',
# MAGIC   completed_date_year STRING COMMENT '',
# MAGIC   completed_date_fiscal_year STRING COMMENT '',
# MAGIC   tm_analytics_ts TIMESTAMP COMMENT '',
# MAGIC   transaction_number BIGINT COMMENT '',
# MAGIC   action_type_2_possible_fix STRING COMMENT '',
# MAGIC   law_office STRING COMMENT '',
# MAGIC   country_or_area_name STRING COMMENT '',
# MAGIC   last_modified_date TIMESTAMP COMMENT '',
# MAGIC   state_cd STRING COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/form_paragraph_dashboard' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.form_paragraph_enhancement(
# MAGIC   class string,
# MAGIC   ser_num_class integer,
# MAGIC   class_no string,
# MAGIC   modification_no integer,
# MAGIC   title_tx string,
# MAGIC   INTL_CLASS_SHORT_TITLE_TX string,
# MAGIC   goods_and_services_desc string,
# MAGIC   serial_number string,
# MAGIC   law_office string,
# MAGIC   country_or_area_name string,
# MAGIC   FPEPCategory string,
# MAGIC   FPEPCategoryID string,
# MAGIC   FPEPYEAR integer,
# MAGIC   FPEPWorkerID string,
# MAGIC   FPEPActionCt integer,
# MAGIC   FPEPCompletedDt date,
# MAGIC   FPEPGroup string,
# MAGIC   FPEPFPID string,
# MAGIC   FPEPSerNum integer,
# MAGIC   FPGroupID string,
# MAGIC   FPEPTitle string,
# MAGIC   FPEPTransLit string,
# MAGIC   CREATE_USER_ID string,
# MAGIC   FK_USER_ROLE_ID integer,
# MAGIC   User_Role string,
# MAGIC   FK_TM_ORGANIZATION_GID string,
# MAGIC   tm_organization_gid string,
# MAGIC   organization_cd string,
# MAGIC   organization_nm string,
# MAGIC   tmworkerNo string,
# MAGIC   active_in string,
# MAGIC   worker_nm string,
# MAGIC   tmngpdbWorkerNo string,
# MAGIC   grade_cd string,
# MAGIC   brs_user_id string,
# MAGIC   FPDSerialNum string,
# MAGIC   FPDActionType string
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/form_paragraph_enhancement' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.post_reg_workforce(
# MAGIC   Fiscal_Year STRING COMMENT 'Represents the fiscal year associated with the data.',
# MAGIC   Date DATE COMMENT 'Represents the date the data was collected.',
# MAGIC   PostRegCat STRING COMMENT 'Category of activities that occur after registration, such as maintenance and renewal.',
# MAGIC   Base_Total DOUBLE COMMENT 'Represents the total base number used for calculations.',
# MAGIC   Avg_6YR_Rate DOUBLE COMMENT 'Average annual rate over a 6-year period.',
# MAGIC   Avg_10YR_Rate DOUBLE COMMENT 'Average annual rate over the past 10 years.',
# MAGIC   Actual_Estimated STRING COMMENT 'Indicates whether the data is actual or estimated.',
# MAGIC   Continue_Process INT COMMENT 'Indicates whether the process should continue.',
# MAGIC   create_ts TIMESTAMP COMMENT 'Stores the timestamp when the record was created.',
# MAGIC   create_user_id STRING COMMENT 'Unique identifier of the user or system that created the record.',
# MAGIC   update_ts TIMESTAMP COMMENT 'Stores the timestamp of the record\'s last update.',
# MAGIC   update_user_id STRING COMMENT 'Stores the ID of the user or system that last updated the record.'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/post_reg_workforce' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.post_reg_dashboard_running(
# MAGIC   SERIAL_NUMBER STRING COMMENT 'Unique identifier assigned to the trademark.',
# MAGIC   MARK_NM_SHORT STRING COMMENT 'This column stores the abbreviated name of the mark.',
# MAGIC   Concat_Class STRING COMMENT 'Concatenated class information combining multiple class details into a single output.',
# MAGIC   Group_Type STRING COMMENT 'Indicates the group of the trademark.',
# MAGIC   Active_Class_Count INT COMMENT 'Number of classes that are currently active for the trademark.',
# MAGIC   Reg_Class_Count INT COMMENT 'Number of distinct classes at the time of registration.',
# MAGIC   Country_or_Area_Name STRING COMMENT 'The country or region name of the trademark\'s owner.',
# MAGIC   State STRING COMMENT 'The state of the trademark owner for this registration.',
# MAGIC   CITY STRING COMMENT 'The city of the trademark owner for this registration.',
# MAGIC   Owner_Name STRING COMMENT 'The name of the trademark owner.',
# MAGIC   Continue_Process INT COMMENT 'Indicates whether the process should continue. This is used for ETL purposes.',
# MAGIC   AM_STAT INT COMMENT 'Status code derived from TRAM indicating the current state of the record.',
# MAGIC   FILING_BASIS_GRP STRING COMMENT 'Identifies the group of filing basis.',
# MAGIC   LAW_OFFICE STRING COMMENT 'This column identifies the law office responsible for handling the case.',
# MAGIC   PCTRAM_LINK STRING COMMENT 'Contains the URL linking to the PCTRAM resource for further information.',
# MAGIC   NON_PRO_SE STRING COMMENT 'Indicator of whether the case involves a non-pro se party, meaning a party is represented by an attorney.',
# MAGIC   Pendency_Cal_Start_DT DATE COMMENT 'Date when pendency calculation begins.',
# MAGIC   SER_NUM STRING COMMENT 'Unique identifier assigned to each trademark.',
# MAGIC   Max_Dt_Filter BOOLEAN COMMENT 'Indicates whether to apply a maximum date filter.  Used for reporting purposes.',
# MAGIC   LiveRegH_Count INT COMMENT 'Represents the total number of active registrations.',
# MAGIC   LiveRegH_DT DATE COMMENT 'Date of live registration for the donor.',
# MAGIC   LiveRegH_Value DATE COMMENT 'Represents the numerical value associated with a live registration.',
# MAGIC   LiveRegH_Name STRING COMMENT 'The name associated with a live registration.',
# MAGIC   FILING_METHOD_CUR STRING COMMENT 'Current filing method in use.',
# MAGIC   create_ts TIMESTAMP COMMENT 'Stores the timestamp when the record was created.',
# MAGIC   create_user_id STRING COMMENT 'Unique identifier of the user who created the record.',
# MAGIC   update_ts TIMESTAMP COMMENT 'Stores the timestamp of the record\'s last update.',
# MAGIC   update_user_id STRING COMMENT 'Stores the ID of the user or system last updated the record.'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/post_reg_dashboard_running' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.quality_dashboard_pivot(
# MAGIC   law_office string,
# MAGIC   lastreviewdatetime date,
# MAGIC   go_final string,
# MAGIC   review_type string,
# MAGIC   final_compliance boolean,
# MAGIC   qualitymetricdeficientflag string,
# MAGIC   excellentflag string,
# MAGIC   max_date date,
# MAGIC   fy_date_current date,
# MAGIC   current_fy string,
# MAGIC   current_fy_int integer,
# MAGIC   fy_date date,
# MAGIC   fy_date_string string,
# MAGIC   fy_month string,
# MAGIC   fy_month_int integer,
# MAGIC   fy_quarter string,
# MAGIC   first_action_type string,
# MAGIC   disposal_type string,
# MAGIC   pendency_cal_start_dt date,
# MAGIC   pendency_cal_end_dt date,
# MAGIC   non_pro_se string,
# MAGIC   country_or_area_name string,
# MAGIC   filing_basis_grp string,
# MAGIC   filing_method_filed string,
# MAGIC   ste_ctry_cd string,
# MAGIC   concat_class string,
# MAGIC   metric string,
# MAGIC   value boolean,
# MAGIC   case_count integer,
# MAGIC   category string,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/quality_dashboard_pivot' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.inventory_madrid(
# MAGIC   MADRID_PCT Float COMMENT 'Percentage of Madrid filings',
# MAGIC   MADRID_FA_Pendency Float COMMENT 'Pendency of first Action for Madrid filings',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_madrid' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.inventory_dashboard_bd_occurrence(
# MAGIC   --FA_Month Int COMMENT '', -- code change wrong datatype 02/14/2024
# MAGIC   FA_Month STRING COMMENT 'The month for which the financial analysis is conducted',
# MAGIC   Percent_of_FAs Float COMMENT 'Percentage of financial analysis completed',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_dashboard_bd_occurrence' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.inventory_dashboard_ea_counts(
# MAGIC   EA_Not_Exam Int COMMENT 'Aggregate count of emp number',
# MAGIC   EA_Examining Int COMMENT 'Distinct count of EMP number',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_dashboard_ea_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists  ${conf.catalog}.gold.inventory_dashboard_ratio(
# MAGIC   FY String COMMENT 'Fiscal Year',
# MAGIC   EA_Examining Int COMMENT 'Count of Entities Currently Being Examined',
# MAGIC   Unexamined_Classes Int COMMENT 'Count of Classes Not Yet Examined',
# MAGIC   EA_Unexamined_Ratio Int COMMENT 'Ratio of Unexamined Classes to Examined',
# MAGIC   Current_FY Boolean COMMENT 'Current Fiscal Year',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_dashboard_ratio' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.inventory_dashboard_filings(
# MAGIC   Pendency_Cal_Start_DT Date COMMENT ' Start date of pendency',
# MAGIC   Class_Count Int COMMENT 'Count of classes',
# MAGIC   Count_Type String COMMENT 'Type of count',
# MAGIC   Current_FY String COMMENT 'Current fiscal year',
# MAGIC   FY String COMMENT 'fiscal year',
# MAGIC   FY_Plus Int COMMENT 'count of fiscal years in total',
# MAGIC   CurrentFY_CountType String COMMENT 'current fiscal year count type',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_dashboard_filings' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists  ${conf.catalog}.gold.inventory_dashboard_pendency(
# MAGIC   Sum_FAPendencyWeight Double COMMENT 'active classes first action multiplied with first action pendency ph',
# MAGIC   Sum_Active_Classes_FirstAction Int COMMENT 'aggregate sum of active classes first action',
# MAGIC   Current_FY_Weighted_First_Action_Pendency Float COMMENT ' sum of FA pendency weight by sum of active classes first action',
# MAGIC   Data_Through Date COMMENT 'Date through which the data is captured',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT  'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_dashboard_pendency' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.inventory_dashboard_running(
# MAGIC   Pendency_Cal_Start_DT Date COMMENT 'Start date for pendency calculation ',
# MAGIC   Class_Count Int COMMENT 'count of classes',
# MAGIC   Count_Type String COMMENT 'type of count',
# MAGIC   Current_FY String COMMENT 'Indicates if the data is for the current fiscal year',
# MAGIC   FY String COMMENT 'Fiscal year',
# MAGIC   FY_Plus Int COMMENT 'count of fiscal years in total',
# MAGIC   Start_Non_Outlier Date COMMENT 'Start date for non-outlier data',
# MAGIC   RunTot_Class_Count Int COMMENT 'Running total of class count',
# MAGIC   EA_Not_Exam Int COMMENT 'Aggregate count of EMP number',
# MAGIC   EA_Examining Int COMMENT 'Distinct count of EMP number',
# MAGIC   Today_Unexamined Int COMMENT 'Count of todays unexamined entities',
# MAGIC   CurrentFY_CountType String COMMENT 'Count type for the current fiscal year',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created',
# MAGIC   create_user_id STRING COMMENT 'User ID of the creator',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated',
# MAGIC   update_user_id STRING COMMENT 'User ID of the last updater'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/inventory_dashboard_running' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## TM Reports Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.cm24(
# MAGIC   serial_num STRING,
# MAGIC   status integer,
# MAGIC   status_date STRING,
# MAGIC   attorney STRING,
# MAGIC   law_office STRING,
# MAGIC   mark STRING,
# MAGIC   cm_code STRING,
# MAGIC   cm_literal STRING,
# MAGIC   order_no INT,
# MAGIC   photocomp_error STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/cm24' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.overdue_630_638(
# MAGIC   serial_num STRING,
# MAGIC   Examiner STRING,
# MAGIC   ContentManager STRING,
# MAGIC   Mark STRING,
# MAGIC   Filing_IB_Date DATE,
# MAGIC   Age_Months DECIMAL,
# MAGIC   Original_Assigned_Date DATE,
# MAGIC   Original_Assigned_Days INT,
# MAGIC   Status INT,
# MAGIC   Basis STRING,
# MAGIC   Law_Office STRING,
# MAGIC   first_action_dt_ph STRING,
# MAGIC   AM_STATUS_DT DATE,
# MAGIC   MARK_NM_SHORT STRING,
# MAGIC   Last_organization_nm STRING,
# MAGIC   ContentLink STRING,
# MAGIC   ath_hold_status STRING
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/overdue_630_638' 
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.overdue_630_638_hist(
# MAGIC   serial_num STRING,
# MAGIC   Examiner STRING,
# MAGIC   ContentManager STRING,
# MAGIC   Mark STRING,
# MAGIC   Filing_IB_Date DATE,
# MAGIC   Age_Months DECIMAL,
# MAGIC   Original_Assigned_Date DATE,
# MAGIC   Original_Assigned_Days INT,
# MAGIC   Status INT,
# MAGIC   Basis STRING,
# MAGIC   Law_Office STRING,
# MAGIC   first_action_dt_ph STRING,
# MAGIC   AM_STATUS_DT DATE,
# MAGIC   MARK_NM_SHORT STRING,
# MAGIC   Last_organization_nm STRING,
# MAGIC   ContentLink STRING,
# MAGIC   ath_hold_status STRING,
# MAGIC   create_ts TIMESTAMP
# MAGIC ) 
# MAGIC USING DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/overdue_630_638_hist' 
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.tqr_email_report(
# MAGIC   eventinventoryidentifier long,
# MAGIC   qualityreviewidentifier long,
# MAGIC   reviewtypecode string,
# MAGIC   trademarkserialnumber string,
# MAGIC   eventdatetime timestamp,
# MAGIC   examineremployeenumber string,
# MAGIC   organizationcode string,
# MAGIC   searchcompleteindicator boolean,
# MAGIC   revieweremployeenumber string,
# MAGIC   lastreviewdatetime timestamp,
# MAGIC   assigndatetime timestamp,
# MAGIC   completedatetime timestamp,
# MAGIC   financialyear long,
# MAGIC   financialquarternumber long,
# MAGIC   missedtagelementnamebag string,
# MAGIC   newtagelementnamebag string,
# MAGIC   unsoundtagelementnamebag string,
# MAGIC   soundtagelementnamebag string,
# MAGIC   evidencedeficienttagelementnamebag string,
# MAGIC   evidencesatisfactorytagelementnamebag string,
# MAGIC   evidenceexcellenttagelementnamebag string,
# MAGIC   writingdeficienttagelementnamebag string,
# MAGIC   writingsatisfactorytagelementnamebag string,
# MAGIC   writingexcellenttagelementnamebag string,
# MAGIC   searchsufficientindicator boolean,
# MAGIC   qualitymetricdeficientindicator boolean,
# MAGIC   mississueindicator boolean,
# MAGIC   newissueindicator boolean,
# MAGIC   refusalunsoundindicator boolean,
# MAGIC   substantivedeficientindicator boolean,
# MAGIC   proceduraldeficientindicator boolean,
# MAGIC   overalldeficientindicator boolean,
# MAGIC   overallexcellentindicator boolean,
# MAGIC   evidencedeficientindicator boolean,
# MAGIC   evidencesatisfactoryindicator boolean,
# MAGIC   evidenceexcellentindicator boolean,
# MAGIC   writingdeficientindicator boolean,
# MAGIC   writingsatisfactoryindicator boolean,
# MAGIC   writingexcellentindicator boolean,
# MAGIC   substantiveerrorindicator boolean,
# MAGIC   satisfactoryindicator boolean,
# MAGIC   findingindicator boolean,
# MAGIC   createdatetime timestamp,
# MAGIC   createuseridentifier string,
# MAGIC   lastmodifieddatetime timestamp,
# MAGIC   lastmodifieduseridentifier string,
# MAGIC   go_final string,
# MAGIC   quality_review_id string,
# MAGIC   filing_dt date,
# MAGIC   filing_method_filed string,
# MAGIC   create_ts timestamp,
# MAGIC   create_user_id string,
# MAGIC   update_ts timestamp,
# MAGIC   update_user_id string
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tqr_email_report' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.kingpin(
# MAGIC   ser_num integer,
# MAGIC   status_dt date,
# MAGIC   create_ts date,
# MAGIC   law_office string,
# MAGIC   kingpin_status string
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/kingpin' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.new_fee_codes(
# MAGIC   REV_SRC_CD string COMMENT 'The EDW revenue source code',
# MAGIC   FEE_NM string COMMENT 'The EDW fee name',
# MAGIC   PRJCT_CD string COMMENT 'The EDW project code'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/new_fee_codes' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.fee_discrepancy(
# MAGIC   ser_num string COMMENT 'Trademark serial number.',
# MAGIC   fees_paid string COMMENT 'Sum of fee paid classes for this serial number.',
# MAGIC   tram_classes string COMMENT 'Sum of classes for this serial number, paid or unpaid.',
# MAGIC   tram_status string COMMENT 'Status code of the serial number.',
# MAGIC   delta string COMMENT 'Difference between the number of fees paid and the number of classes registered.',
# MAGIC   discrepancy_type string COMMENT 'Either underpayment, if delta is negative, or overpayment, if delta is positive.',
# MAGIC   create_ts string COMMENT 'Auto generated ETL create timestamp.',
# MAGIC   create_user_id string COMMENT 'Auto generated ETL create user id.',
# MAGIC   update_ts string COMMENT 'Auto generated ETL update timestamp.',
# MAGIC   update_user_id string COMMENT 'Auto generated ETL update user id.'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/fee_discrepancy' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.og_issue_registrations(
# MAGIC   filing_class string COMMENT 'Trademark filing basis.',
# MAGIC   date_difference decimal(10,2) COMMENT 'Difference in days between filing date and registration date.'
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/og_issue_registrations' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### POU EMAIL OUTPUT **TABLES**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.pou_audit_characteristics(
# MAGIC   category STRING,
# MAGIC   value STRING,
# MAGIC   audits INT,
# MAGIC   total_audits INT,
# MAGIC   percent DECIMAL(10, 2),
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC   ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/pou_audit_characteristics' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.pou_audit_totals(
# MAGIC   category STRING,
# MAGIC   filing_basis STRING,
# MAGIC   count INT,
# MAGIC   total INT,
# MAGIC   percent DECIMAL(10, 2),
# MAGIC   deletion_rate DECIMAL(10, 2),
# MAGIC   overall_deletion_rate DECIMAL(10, 2),
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC   ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/pou_audit_totals' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.pou_audit_actions(
# MAGIC   fiscal_year STRING,
# MAGIC   first_actions INT,
# MAGIC   second_actions INT,
# MAGIC   third_actions INT,
# MAGIC   interim_office_actions INT,
# MAGIC   no_response_office_actions INT,
# MAGIC   response_received INT,
# MAGIC   cancellations INT,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC   ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/pou_audit_actions' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## OBP ETL TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.tm_opb_metrics (
# MAGIC   DATE DATE COMMENT 'The date of the record.',
# MAGIC   FY_WK_NUM STRING COMMENT 'The fiscal year and week number of the record.',
# MAGIC   FY_YEAR STRING COMMENT 'The fiscal year of the record.',
# MAGIC   CL_YEAR STRING COMMENT 'The calendar year of the record.',
# MAGIC   FYPP STRING COMMENT 'The fiscal year and period of the record.',
# MAGIC   YYYYPP STRING COMMENT 'The year and period of the record.',
# MAGIC   WK_DT_START DATE COMMENT 'The start date of the week.',
# MAGIC   WK_DT_END DATE COMMENT 'The end date of the week.',
# MAGIC   JOIN_DT DATE COMMENT 'The date when the record was joined.',
# MAGIC   MONTH STRING COMMENT 'The month of the record',
# MAGIC   FY_MONTH_NUM INTEGER COMMENT 'The fiscal month number of the record.',
# MAGIC   FILING_FY INTEGER COMMENT 'The fiscal year of filing',
# MAGIC   SIX_TER LONG COMMENT 'The number of filings in 6-ter category.',
# MAGIC   ELECTRONIC LONG COMMENT 'The number of electronic filings.',
# MAGIC   MADRID LONG COMMENT 'The number of Madrid filings.',
# MAGIC   PAPER LONG COMMENT 'The number of paper filings',
# MAGIC   TOTAL_CLASSES LONG COMMENT 'The total number of classes filed',
# MAGIC   FA_CLASSES LONG COMMENT 'The number of classes filed by foreign applicants',
# MAGIC   FA_CASES LONG COMMENT 'The number of cases filed by foreign applicants.',
# MAGIC   ITU_ABANDONMENTS LONG COMMENT 'The number of ITU abandonments',
# MAGIC   SOU_ABANDONMENTS LONG COMMENT 'The number of Statement of Use abandonments.',
# MAGIC   SOU_ACCEPTED LONG COMMENT 'The number of Statement of Use accepted.',
# MAGIC   SIX_YR_MAINTENANCE_FILED LONG COMMENT 'The number of 6-year maintenance filings.',
# MAGIC   SEPARATE15_FILED LONG COMMENT 'The number of separate 15 filings.',
# MAGIC   SIX_YR_15_MAINTENANCE_FILED LONG COMMENT 'The number of 6-year and 15-year maintenance filings',
# MAGIC   TEN_YR_RENEWAL_FILED LONG COMMENT 'The number of 10-year renewal filings.',
# MAGIC   SECTION7_FILED LONG COMMENT 'The number of Section 7 filings.',
# MAGIC   ABANDONMENT_CASES LONG COMMENT 'The number of abandonment cases.',
# MAGIC   NOA_CASES LONG COMMENT 'The number of Notice of Allowance cases.',
# MAGIC   REGISTRATION_CASES LONG COMMENT 'The number of registration cases.',
# MAGIC   ABANDONMENT_CLASSES LONG COMMENT 'The number of abandonment classes.',
# MAGIC   NOA_CLASSES LONG COMMENT 'The number of Notice of Allowance classes.',
# MAGIC   REGISTRATION_CLASSES LONG COMMENT 'The number of registration classes.'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'The tm_opb_metrics table contains data related to trademark operations and performance metrics. It provides information on various aspects such as the date, fiscal year, calendar year, week number, and month. Additionally, it includes data on filing statistics, including the number of electronic and paper filings, as well as Madrid filings. The table also includes information on the total number of classes, classes filed by foreign applicants, and the number of cases. Furthermore, it provides data on ITU abandonments.' LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tm_opb_metrics'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## TTAB_LEADERSHIP TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.TTAB_LEADERSHIP (
# MAGIC   Proceeding_Number BIGINT
# MAGIC   COMMENT 'Unique identifier for each proceeding',
# MAGIC   APPLICATION_NUMBER BIGINT
# MAGIC   COMMENT 'Unique identifier for each application',
# MAGIC   Intl_Reg_Number BIGINT
# MAGIC   COMMENT 'Unique identifier for each international registration',
# MAGIC   FILING_DATE DATE
# MAGIC   COMMENT 'Date when the proceeding or application was filed',
# MAGIC   INSTITUTION_DATE DATE
# MAGIC   COMMENT 'Date when the proceeding was instituted',
# MAGIC   IB_NOTICE_DATE DATE
# MAGIC   COMMENT 'Date of notice from the International Bureau',
# MAGIC   TIME_TO_NOTICE BIGINT
# MAGIC   COMMENT 'Time taken to receive notice after filing (in days)',
# MAGIC   TIME_FROM_FILING BIGINT
# MAGIC   COMMENT 'Time taken from filing to the current status (in days)',
# MAGIC   APPLICATION_STATUS STRING
# MAGIC   COMMENT 'Current status of the application',
# MAGIC   TTAB_STATUS_CODE DOUBLE
# MAGIC   COMMENT 'Code representing the current TTAB status',
# MAGIC   TTAB_STATUS STRING
# MAGIC   COMMENT 'Description of the current TTAB status',
# MAGIC   TTAB_STATUS_DATE DATE
# MAGIC   COMMENT 'Date of the current TTAB status',
# MAGIC   Irregularity_From_IB STRING
# MAGIC   COMMENT 'Irregularities reported by the International Bureau',
# MAGIC   Irregularity_NOtice_Date DATE
# MAGIC   COMMENT 'Date of notice for irregularities',
# MAGIC   LAST_PH_ENTRY STRING
# MAGIC   COMMENT 'Last entry in the Public Hearing field'
# MAGIC ) USING delta
# MAGIC COMMENT 'The ttab_leadership table contains data related to proceedings and applications in the trademark trial and appeal board. It includes information such as proceeding numbers, application numbers, international registration numbers, filing dates, institution dates, notice dates, time to notice, time from filing, application status, TTAB status codes, TTAB status descriptions, TTAB status dates, irregularities from the International Bureau, irregularity notice dates, and the last entry in the Public Hearing field. This table is created by the file upload UI and is part of the trm_reporting catalog in the bronze schema.' 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/TTAB_LEADERSHIP'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### ttab_opposition_response

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.ttab_opposition_response (
# MAGIC Serial_Number BIGINT COMMENT 'Unique identifier for each Serial Number',
# MAGIC International_Registration STRING COMMENT 'International registration number for each record',
# MAGIC Opposition_Notice_Date DATE COMMENT 'Opposition Notice date for each record id',
# MAGIC Response_Due_Date DATE COMMENT 'Resposne due date for each record id',
# MAGIC create_ts TIMESTAMP COMMENT 'created time stamp for each record',
# MAGIC create_user_id STRING COMMENT 'creation user_id  for each record',
# MAGIC update_ts TIMESTAMP COMMENT 'updated time stamp for each record',
# MAGIC update_user_id STRING COMMENT 'Updated user id for each record'
# MAGIC  ) USING delta 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ttab_opposition_response'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### tm_expired_registrations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE 
# MAGIC -- OR REPLACE
# MAGIC TABLE 
# MAGIC IF NOT EXISTS
# MAGIC  ${conf.catalog}.gold.tm_expired_registrations (
# MAGIC     registration_number STRING COMMENT 'Registration Number',
# MAGIC     serial_number STRING COMMENT 'Serial Number',
# MAGIC     registration_date DATE COMMENT 'Registration Date'
# MAGIC )
# MAGIC USING delta
# MAGIC COMMENT 'The tm expired registrations table contains all the application wose registrations have expired for the past week' 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tm_expired_registrations'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### madrid_transformations_and_replacements

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE 
# MAGIC -- OR REPLACE
# MAGIC TABLE 
# MAGIC IF NOT EXISTS
# MAGIC  ${conf.catalog}.gold.madrid_transformations_and_replacements (
# MAGIC     serial_number STRING COMMENT 'Serial Number',
# MAGIC     transformation_or_replacement_date DATE COMMENT 'Transformation / Replacement Date',
# MAGIC     ent_code STRING COMMENT 'Code Description',
# MAGIC     last_cm_date DATE COMMENT 'Last business event date',
# MAGIC     last_cm STRING COMMENT 'Last business event',
# MAGIC     am_stat_date DATE COMMENT 'Trademark status event date',
# MAGIC     am_stat  INTEGER COMMENT 'Trademark status number',
# MAGIC     am  STRING COMMENT 'Trademark status description'
# MAGIC )
# MAGIC USING delta
# MAGIC COMMENT 'The madrid_transformation_and_replacements table contains all the application who have been replaced or transformed' 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/madrid_transformations_and_replacements'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### First Action summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.first_actions_summary (
# MAGIC     Law_office STRING,
# MAGIC     Examiner STRING,
# MAGIC     EA_NAME STRING,
# MAGIC     Pay_Period STRING,
# MAGIC     First_Action_Type STRING,
# MAGIC     `Date` DATE,
# MAGIC     PP_Begin_DT DATE,
# MAGIC     PP_End_DT DATE,
# MAGIC     First_1st_act_FY INT,
# MAGIC     Cases BIGINT,
# MAGIC     Classes INT
# MAGIC ) USING delta 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/first_actions_summery'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.first_actions_details (
# MAGIC   PP_BEGIN_DT DATE,
# MAGIC   PP_END_DT DATE,
# MAGIC   PayPd BIGINT,
# MAGIC   CY BIGINT,
# MAGIC   PP BIGINT,
# MAGIC   `DATE` DATE,
# MAGIC   ser_num INT,
# MAGIC   first_action_dt_ph DATE,
# MAGIC   first_action_type STRING,
# MAGIC   am_1_actn_ct_dt DATE,
# MAGIC   am_cls_ct_actv BIGINT,
# MAGIC   filing_fy INT,
# MAGIC   noa_dt_ph DATE,
# MAGIC   LAW_OFFICE STRING,
# MAGIC   EXMR_EID INT,
# MAGIC   AM_FLG_ITU_FIL INT,
# MAGIC   EA_Name STRING,
# MAGIC   Grade STRING,
# MAGIC   first_act_FY INT
# MAGIC ) USING delta 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/first_actions_details'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## cx survey email address for efile and teas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS   ${conf.catalog}.gold.email_address_for_cx_survey_trm_efile (
# MAGIC     EMAIL_TX string COMMENT 'Email address of the patron',
# MAGIC     FILING_DT timestamp COMMENT 'Filing date of the transaction',
# MAGIC     SERIAL_NO int COMMENT 'Serial number of the transaction',
# MAGIC     REGISTRATION_NO int COMMENT 'Registration number',
# MAGIC     FIRST_NM string COMMENT 'First name of the patron',
# MAGIC     LAST_NM string COMMENT 'Last name of the patron',  
# MAGIC     FILE_NAME string COMMENT 'File name for Excel output',
# MAGIC     FK_FORM_CD string COMMENT 'Foreign key for form code',
# MAGIC     RECORD_ID bigint COMMENT 'Unique record identifier',
# MAGIC     RUN_DATE string COMMENT 'Run date in string format'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/email_address_for_cx_survey_trm_efile'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC COMMENT 'Table storing email addresses for CX survey TRM efile';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  TABLE IF NOT EXISTS   ${conf.catalog}.gold.cx_survey_file_errors (
# MAGIC     SERIAL_NO int COMMENT 'Serial number of the transaction',
# MAGIC     CFK_PATRON_ID string COMMENT 'Foreign key for patron ID',
# MAGIC     IP_ADDRESS_TX string COMMENT 'IP address of the patron',
# MAGIC     FK_TRANSACTION_TYPE_CD string COMMENT 'Foreign key for transaction type code',
# MAGIC     REGISTRATION_NO int COMMENT 'Registration number',
# MAGIC     FK_FORM_CD string COMMENT 'Foreign key for form code',
# MAGIC     SIGNATORY_POSITION_NM string COMMENT 'Signatory position name',
# MAGIC     FILING_DT timestamp COMMENT 'Filing date of the transaction',
# MAGIC     DN_PATRON_FIRST_NM string COMMENT 'First name of the patron',
# MAGIC     DN_PATRON_LAST_NM string COMMENT 'Last name of the patron',
# MAGIC     DN_PATRON_EMAIL_ADDRESS_TX string COMMENT 'Email address of the patron',
# MAGIC     RUNDATE_STR string COMMENT 'Run date in string format'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/cx_survey_file_errors'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC COMMENT 'Table storing file errors for CX survey TRM efile';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.attorney_history (
# MAGIC   ATTY_NM STRING COMMENT 'Attorney Name',
# MAGIC   OWNR_CNTRY STRING COMMENT 'Owner Country',
# MAGIC   PFYTD_Total BIGINT COMMENT 'Previous Fiscal Year To Date Total',
# MAGIC   PFY_Total BIGINT COMMENT 'Previous Fiscal Year Total',
# MAGIC   FYTD_Total BIGINT COMMENT 'Current Fiscal Year To Date Total',
# MAGIC   FYTD_Delta BIGINT COMMENT 'Current Fiscal Year To Date Delta'
# MAGIC   )
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/attorney_history'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.sn_status (
# MAGIC sn	int,
# MAGIC status	int,
# MAGIC status_dt	string,
# MAGIC filing_dt	string,
# MAGIC classes	string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/sn_status'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.unpaid_fees_alert_history (
# MAGIC ser_num string,
# MAGIC fees_paid double,
# MAGIC tram_classes long,
# MAGIC unpaid_classes long,
# MAGIC tram_status integer,
# MAGIC run_date string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unpaid_fees_alert_history'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ppa_report (
# MAGIC FY string,
# MAGIC PPA_Category string,
# MAGIC Hours double,
# MAGIC Percent double
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ppa_report'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ppa_report_fytd_hours (
# MAGIC FY string,
# MAGIC PPA_Category string,
# MAGIC Hours double,
# MAGIC Percent double
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ppa_report_fytd_hours'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ppa_report_ot (
# MAGIC FY string,
# MAGIC Hours decimal,
# MAGIC Percent decimal,
# MAGIC Hours_Type string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ppa_report_ot'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists ${conf.catalog}.gold.ppa_report_ot_fytd_hours (
# MAGIC FY string,
# MAGIC Hours decimal,
# MAGIC Percent decimal,
# MAGIC Hours_Type string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ppa_report_ot_fytd_hours'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ppa_report_leave (
# MAGIC FY string,
# MAGIC Hours decimal,
# MAGIC ACCTG_ACT_NM string,
# MAGIC Percent string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ppa_report_leave'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ppa_report_leave_fytd_hours (
# MAGIC FY string,
# MAGIC Leave_Type string,
# MAGIC Hours decimal,
# MAGIC Percent string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ppa_report_leave_fytd_hours'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ppa_report_biweekly (
# MAGIC FY string,
# MAGIC PY_PRD_LAST_DA string,
# MAGIC PPA_Category string,
# MAGIC SUM_PAY_HR_NO double
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ppa_report_biweekly'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.max_pay_period_ppa (
# MAGIC MAX_PY_PRD_LAST_DA string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/max_pay_period_ppa'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.tm_fee_code_daily_income (
# MAGIC fee_cd_act_sum_acctg_da date,
# MAGIC fee_cd string,
# MAGIC fee_nm string,
# MAGIC daily_income_edw integer,
# MAGIC tm_paper_electronic string,
# MAGIC cat_1_desc string,
# MAGIC cat_2_desc string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tm_fee_code_daily_income'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.tmintl_auto_protect (
# MAGIC am_ser_num	string,
# MAGIC am_dt_fil	date,
# MAGIC am_dt_dock	date,
# MAGIC am_stat	int,
# MAGIC am_stat_dt	date,
# MAGIC days_to_auto_protect	int,
# MAGIC auto_protect_dt	string,
# MAGIC ib_notification_crcv_calculation_months	integer,
# MAGIC cm_desc	string
# MAGIC ) using DELTA 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmintl_auto_protect'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.teas_goods_services_deleted (
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
# MAGIC   update_ts STRING,
# MAGIC   update_user_id STRING,
# MAGIC   year_month STRING)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/teas_goods_services_deleted'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.naics_fasttext (
# MAGIC   ser_num integer,
# MAGIC   class string,
# MAGIC   input_text string,
# MAGIC   NAICS_Code_1 double,
# MAGIC   NAICS_Code_2 double,
# MAGIC   NAICS_Code_3 double,
# MAGIC   NAICS_Label_1 string,
# MAGIC   NAICS_Label_2 string,
# MAGIC   NAICS_Label_3 string,
# MAGIC   Similarity_1 double,
# MAGIC   Similarity_2 double,
# MAGIC   Similarity_3 double
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/naics_fasttext' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.preexam_fee_checker (
# MAGIC   ser_num integer,
# MAGIC   fees_paid integer,
# MAGIC   tram_classes integer,
# MAGIC   delta integer,
# MAGIC   tram_status integer,
# MAGIC   discrepancy_type string
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/preexam_fee_checker' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.tm_category_case_counts (
# MAGIC   time_period string,
# MAGIC   category_description string,
# MAGIC   count integer,
# MAGIC   fee_paid_classes integer,
# MAGIC   other_classes integer
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tm_category_case_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS   ${conf.catalog}.gold.sec_8_and_15_partial_acceptance (
# MAGIC     REGISTRATION_NUMBER INTEGER COMMENT 'Registration number of the mark',
# MAGIC     MARK_LITERAL STRING COMMENT 'Literal element of the mark',
# MAGIC     ACTIVE_CLASSES STRING COMMENT 'Active classes associated with the mark',
# MAGIC     SECTION_B_CANCELLED_CLASSES STRING COMMENT 'Cancelled classes under section B',
# MAGIC     PH_DATE DATE COMMENT 'Date of partial acceptance under section 8 and acknowledgment under section 15',
# MAGIC     OG_ACTION_DATE STRING COMMENT 'Original Gazette action date'
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/sec_8_and_15_partial_acceptance'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )
# MAGIC COMMENT 'Table with recent load of sec 8 and 15 partial acceptance for past 7 days';

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.sec_8_partial_acceptance (
# MAGIC REGISTRATION_NUMBER int,
# MAGIC MARK_LITERAL  string,
# MAGIC ACTIVE_CLASSES  string,
# MAGIC SECTION_B_CANCELLED_CLASSES string,
# MAGIC PH_DATE string,
# MAGIC OG_ACTION_DATE  string
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/sec_8_partial_acceptance'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.tranen_tranex_with_limgr (
# MAGIC ibout_date date,
# MAGIC ibout_status  string,
# MAGIC fk_stsa_cd  string,
# MAGIC control_no  string,
# MAGIC offref  string,
# MAGIC basappn string,
# MAGIC name_type string,
# MAGIC email string,
# MAGIC fk_irn_ib_ref_num string,
# MAGIC pmt_details string
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tranen_tranex_with_limgr'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC --create table if not exists ${conf.catalog}.gold.process_production_staffing_report(
# MAGIC create or replace table  ${conf.catalog}.gold.process_production_staffing_report(
# MAGIC year  string,
# MAGIC fy_quarter  string,
# MAGIC fy_month  string,
# MAGIC fy_month_int  int,
# MAGIC notice_of_allowance_issued_classes int,
# MAGIC notice_of_allowance_issued_classes_fy int,
# MAGIC notice_of_allowance_issued_classes_fy_target int,
# MAGIC published_for_opposition_classes int,
# MAGIC registrations_including_classes int,
# MAGIC certificates_of_registration_issued_cases int,
# MAGIC Published_for_Opposition_classes_actual int,
# MAGIC Published_for_Opposition_classes_target int,
# MAGIC registrations_including_classes_fy  bigint,
# MAGIC registrations_including_classes_fy_target  int,
# MAGIC certificates_of_registration_issued_cases_fy  bigint,
# MAGIC certificates_of_registration_issued_cases_fy_target  int,
# MAGIC total_requests_for_extension_of_protection int,
# MAGIC application_files_filed int,
# MAGIC application_files_filed_fy int,
# MAGIC Application_Files_filed_target int,
# MAGIC first_actions_initial_exam_classes  decimal(22,0),
# MAGIC abandonment_classes  decimal(22,0),
# MAGIC approved_for_publication_classes  decimal(22,0),
# MAGIC total_balanced_disposals  decimal(24,0),
# MAGIC first_actions_initial_exam_classes_fy  decimal(32,0),
# MAGIC first_actions_initial_exam_classes_fy_target  int,
# MAGIC abandonment_classes_fy  decimal(32,0),
# MAGIC abandonment_classes_fy_target  int,
# MAGIC approved_for_publication_classes_fy  decimal(32,0),
# MAGIC approved_for_publication_classes_fy_target  int,
# MAGIC total_balanced_disposals_fy  decimal(34,0),
# MAGIC total_balanced_disposals_fy_target  int,
# MAGIC total_pending_applications_cases_38 int,
# MAGIC total_pending_applications_classes_39 int,
# MAGIC Total_Pending_Applications_cases_38_fy int,
# MAGIC Total_Pending_Applications_classes_39_fy int,
# MAGIC abandoned_classes  bigint,
# MAGIC abandoned_files_cases  bigint,
# MAGIC abandoned_classes_fy  bigint,
# MAGIC abandoned_classes_fy_target  int,
# MAGIC abandoned_files_cases_fy  bigint,
# MAGIC abandoned_files_cases_fy_target  int,
# MAGIC unexamined_new_applicationn_cases_prior_to_first_action  bigint,
# MAGIC unexamined_new_applicationn_classes_prior_to_first_action  bigint,
# MAGIC unexamined_new_applicationn_cases_prior_to_first_action_fy  bigint,
# MAGIC unexamined_new_applicationn_cases_prior_to_first_action_fy_target  int,
# MAGIC unexamined_new_applicationn_classes_prior_to_first_action_fy  bigint,
# MAGIC unexamined_new_applicationn_classes_prior_to_first_action_fy_target  int,
# MAGIC median_age_of_inventory decimal(15,2),
# MAGIC median_age_of_inventory_fy  decimal(15,2),
# MAGIC median_age_of_inventory_fy_target  decimal(2,1),
# MAGIC total_statements_of_use_filed_classes  bigint,
# MAGIC total_statements_of_use_filed  bigint,
# MAGIC total_statements_of_use_processing_complete_classes  bigint,
# MAGIC total_statements_of_use_processing_complete  bigint,
# MAGIC total_statements_of_use_filed_classes_fy  bigint,
# MAGIC total_statements_of_use_filed_classes_fy_target  int,
# MAGIC total_statements_of_use_filed_fy  bigint,
# MAGIC total_statements_of_use_filed_fy_target  int,
# MAGIC total_statements_of_use_processing_complete_classes_fy  bigint,
# MAGIC total_statements_of_use_processing_complete_classes_fy_target  int,
# MAGIC total_statements_of_use_processing_complete_fy  bigint,
# MAGIC total_statements_of_use_processing_complete_fy_target  int,
# MAGIC section_9_applications_filed  bigint,
# MAGIC registrations_renewed  bigint,
# MAGIC affidavits_under_section_8_15_71_combinations_filed  bigint,
# MAGIC affidavits_under_section_8_15_71_combinations_disposed  bigint,
# MAGIC section_8_applications_filed_10yr  bigint,
# MAGIC section_9_applications_filed_fy  bigint,
# MAGIC section_9_applications_filed_fy_target  int,
# MAGIC registrations_renewed_fy  bigint,
# MAGIC registrations_renewed_target  int,
# MAGIC affidavits_under_section_8_15_71_combinations_filed_fy  bigint,
# MAGIC affidavits_under_section_8_15_71_combinations_filed_fy_target  int,
# MAGIC affidavits_under_section_8_15_71_combinations_disposed_fy  bigint,
# MAGIC affidavits_under_section_8_15_71_combinations_disposed_fy_target  int,
# MAGIC section_8_applications_filed_10yr_fy  bigint,
# MAGIC section_8_applications_filed_10yr_fy_target  int,
# MAGIC total_applications_filed_classes  bigint,
# MAGIC total_applications_filed_classes_fy  bigint,
# MAGIC total_applications_filed_classes_fy_actual  bigint,
# MAGIC total_applications_filed_classes_fy_target  int,
# MAGIC total_application_files_filings_cases  bigint,
# MAGIC total_application_files_filings_cases_fy  bigint,
# MAGIC total_application_files_filings_cases_fy_actual  bigint,
# MAGIC total_application_files_filings_cases_fy_target  int,
# MAGIC filed_classes_fytd_growth_rate  double,
# MAGIC filed_classes_fytd_growth_rate_target  decimal(2,1),
# MAGIC filed_cases_fytd_growth_rate  double,
# MAGIC filed_classes_month_growth_rate  double,
# MAGIC filed_cases_month_growth_rate  double,
# MAGIC pendency_to_first_action_month  double,
# MAGIC pendency_to_first_action_fy  double,
# MAGIC first_action_target_fy  decimal(2,1),
# MAGIC pendency_to_registration_abandonment_noa_exc  double,
# MAGIC pendency_to_reg_fy_exc  double,
# MAGIC pendency_to_reg_target_fy_exc  decimal(11,1),
# MAGIC pendency_to_registration_abandonment_noa_inc  double,
# MAGIC pendency_to_reg_fy_inc  double,
# MAGIC pendency_to_reg_target_fy_inc  decimal(3,1),
# MAGIC total_pendency_reg  double,
# MAGIC total_pendency_reg_fy  double,
# MAGIC total_pendency_noa  double,
# MAGIC total_pendency_noa_fy  double,
# MAGIC first_action_compliance_rate  double,
# MAGIC first_action_compliance_rate_fy  double,
# MAGIC first_action_compliance_rate_target  decimal(3,1),
# MAGIC final_compliance_rate  double,
# MAGIC final_compliance_rate_fy  double,
# MAGIC final_compliance_rate_target  decimal(3,1),
# MAGIC exceptional_first_action_rate  double,
# MAGIC exceptional_first_action_rate_fy  double,
# MAGIC exceptional_first_action_rate_target  decimal(3,1),
# MAGIC insert_ts timestamp,
# MAGIC last_update_ts timestamp
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/process_production_staffing_report'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.process_production_staffing_workdays(
# MAGIC year  string,
# MAGIC fy_quarter  string,
# MAGIC fy_month  string,
# MAGIC fy_month_int  int,
# MAGIC start_date date,
# MAGIC end_date date,
# MAGIC workdays_in_month int
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/process_production_staffing_workdays'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ml_sarima_total_balanced_disposals (
# MAGIC actual double,
# MAGIC forecast double,
# MAGIC error double
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ml_sarima_total_balanced_disposals'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ml_sarima_bds_case_count (
# MAGIC actual double,
# MAGIC forecast double,
# MAGIC error double
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ml_sarima_bds_case_count'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.ml_sarima_bds_examiners (
# MAGIC actual double,
# MAGIC forecast double,
# MAGIC error double
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/ml_sarima_bds_examiners'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.docket_monitoring (
# MAGIC case_status string, 
# MAGIC ser_num string,
# MAGIC examiner_employee_no string,
# MAGIC review_type_cd string,
# MAGIC docket_type string, 
# MAGIC docket_type_cd string,
# MAGIC dock_date date,
# MAGIC assigned_date date,
# MAGIC action_date date,
# MAGIC goal_date date,
# MAGIC due_date date,
# MAGIC lom_source_event_dt date,
# MAGIC is_late_ind int,
# MAGIC days_in_docket int,
# MAGIC due_date_estimate int,
# MAGIC due_in int,
# MAGIC law_office string,
# MAGIC year_dock int, 
# MAGIC dock_fy int,
# MAGIC fy_month_int int, 
# MAGIC month_dock string, 
# MAGIC fy_quarter string,
# MAGIC create_ts STRING,
# MAGIC create_user_id STRING,
# MAGIC examiner_nm string,
# MAGIC target_days int
# MAGIC ) using DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/docket_monitoring'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists ${conf.catalog}.gold.sec_7_withdrawals (
# MAGIC     TM_paralegal STRING COMMENT 'post reg action tm paralegal',
# MAGIC     last_assigned_paralegal STRING COMMENT 'last assigned post reg action tm paralegal',
# MAGIC     last_assigned_paralegal_dt date COMMENT 'last assigned post reg action tm paralegal date',
# MAGIC     serial_number INTEGER COMMENT 'Serial number of the record' , 
# MAGIC     registration_number INTEGER COMMENT 'Registration number of the mark', 
# MAGIC     sec_7_event_date STRING COMMENT 'Section 7 request received date', 
# MAGIC     wdlrs_event_date STRING COMMENT 'OTQR Withdrawal From Publication Date', 
# MAGIC     pramo_event_date STRING COMMENT 'Post reg action mailed date'
# MAGIC )
# MAGIC USING delta 
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/sec_7_withdrawals' 
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true', 
# MAGIC     'delta.autoOptimize.autoCompact' = 'true' 
# MAGIC )
# MAGIC COMMENT 'Table with recent load of sec 7 withdrawals'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  ${conf.catalog}.gold.pex_inventory_dash (
# MAGIC   ser_num STRING COMMENT 'source_serial_number',
# MAGIC   pendency_cal_start_dt DATE COMMENT 'Pendency calculation start date',
# MAGIC   dock_dt DATE COMMENT 'Dock date',
# MAGIC   NWOS_DT DATE COMMENT 'Date when the action took place.',
# MAGIC   cm_prcd_num STRING COMMENT 'Procedure number associated with the action.',
# MAGIC   tm_worker_eid STRING COMMENT 'Employee ID of the trademark worker involved.',
# MAGIC   pre_exam_status STRING COMMENT 'hits_hits__source_pre_exam_status',
# MAGIC   status_desc STRING,
# MAGIC   assignee STRING COMMENT 'source_assignee',
# MAGIC   pre_exam_received_ts TIMESTAMP COMMENT 'hits_hits__source_date_pre_exam_received',
# MAGIC   ath_hold_status INT COMMENT 'Status of the hold (e.g., active, resolved).',
# MAGIC   ath_hold_docket INT COMMENT 'Docket number associated with the hold.',
# MAGIC   ath_active_status INT COMMENT 'Active status indicator.',
# MAGIC   ath_last_upd_dt DATE COMMENT 'Date when the hold record was last updated.',
# MAGIC   min_pendency_cal_start_dt DATE,
# MAGIC   min_pre_exam_received_ts TIMESTAMP,
# MAGIC   min_pullable_case_date DATE,
# MAGIC   filing_dt DATE,
# MAGIC   create_ts TIMESTAMP,
# MAGIC create_user_id STRING,
# MAGIC   dead_mark_in string, 
# MAGIC   disposal_type string,
# MAGIC   calendar_day date,
# MAGIC   daily_teas_processed int ,
# MAGIC   ph_action_date date, 
# MAGIC   ph_action_code string)
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/pex_inventory_dash' 
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true', 
# MAGIC     'delta.autoOptimize.autoCompact' = 'true' 
# MAGIC )
# MAGIC COMMENT 'Table with pre examination executive dashboard data'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.tm_category_case_counts_hstry (
# MAGIC   rundate date,
# MAGIC   time_period string,
# MAGIC   category_description string,
# MAGIC   count integer,
# MAGIC   fee_paid_classes integer,
# MAGIC   other_classes integer
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tm_category_case_counts_hstry' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.noa_email_report (
# MAGIC   noa_date date,
# MAGIC   cases integer,
# MAGIC   classes integer
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/noa_email_report' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS  ${conf.catalog}.gold.zombie_report (
# MAGIC   serial_number INTEGER,
# MAGIC   first_suspend DATE,
# MAGIC   last_suspend DATE,
# MAGIC   ph_action_code STRING,
# MAGIC   cm_desc STRING,
# MAGIC   days_since_first_suspend INTEGER,
# MAGIC   prior_pending_serial_number STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/zombie_report' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ${conf.catalog}.gold.docket_monitoring
# MAGIC ADD COLUMNS (lo_manager STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.gold.executive_ops_percent_projections (
# MAGIC     fiscal_year_pay_period STRING,
# MAGIC     projected_fa_percent DOUBLE,
# MAGIC     projected_disposal_percent DOUBLE,
# MAGIC     projected_bd_percent DOUBLE,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/executive_ops_percent_projections' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE  ${conf.catalog}.gold.executive_ops_actuals (
# MAGIC     range_nm STRING,
# MAGIC     fk_start_calendar_dt DATE,
# MAGIC     fk_end_calendar_dt DATE,
# MAGIC     FA_Actual INT,
# MAGIC     FA_Cumulative INT,
# MAGIC     BD_Actual INT,
# MAGIC     BD_Cumulative INT,
# MAGIC     Disposals_Actual INT,
# MAGIC     Disposals_Cumulative INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/executive_ops_actuals'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,SharePoint KPI Metrics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.gold.sharepoint_kpi_metrics(
# MAGIC   `1ap` DOUBLE,
# MAGIC   `dp` DOUBLE,
# MAGIC   `1ac` DOUBLE,
# MAGIC   `eoa` DOUBLE,
# MAGIC   `fc` DOUBLE,
# MAGIC   `filings` INT,
# MAGIC   `filings_gr` INT,
# MAGIC   `unex` INT,
# MAGIC   `as_of` DATE,
# MAGIC   `id` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   `modified_by` STRING,
# MAGIC   `modified` DATE,
# MAGIC   `latest` BOOLEAN
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/sharepoint_kpi_metrics'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# DBTITLE 1,OS34 Report: Statuses
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.os34_report_statuses(
# MAGIC   status_code BIGINT,
# MAGIC   status_description STRING,
# MAGIC   abandoned BOOLEAN,
# MAGIC   case_count BIGINT,
# MAGIC   class_count BIGINT,
# MAGIC   load_date DATE,
# MAGIC   is_static BOOLEAN,
# MAGIC   latest BOOLEAN,
# MAGIC   create_user STRING,
# MAGIC   create_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_statuses'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# DBTITLE 1,OS34 Report: Pending Totals
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.os34_report_totals (
# MAGIC   tot_app_cases BIGINT,
# MAGIC   tot_app_class BIGINT,
# MAGIC   totnoacases BIGINT,
# MAGIC   totnoaclass BIGINT,
# MAGIC   totusecase BIGINT,
# MAGIC   totuseclass BIGINT,
# MAGIC   itu_cases BIGINT,
# MAGIC   itu_class BIGINT,
# MAGIC   load_date DATE,
# MAGIC   is_static BOOLEAN,
# MAGIC   latest BOOLEAN,
# MAGIC   create_user STRING,
# MAGIC   create_timestamp TIMESTAMP
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_totals'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,OS34 Report: Abandonments FYTD
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.os34_report_abandonments_fytd (
# MAGIC   status_code BIGINT,
# MAGIC   status_description STRING,
# MAGIC   abandoned BOOLEAN,
# MAGIC   case_count BIGINT,
# MAGIC   class_count BIGINT,
# MAGIC   load_date DATE,
# MAGIC   is_static BOOLEAN,
# MAGIC   latest BOOLEAN,
# MAGIC   create_user STRING,
# MAGIC   create_timestamp TIMESTAMP
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_abandonments_fytd'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,OS34 Report: Deferred Revenue Cases FYTD
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.os34_report_deferred_revenue_cases (
# MAGIC   serial_num STRING,
# MAGIC   status_code STRING,
# MAGIC   active_classes BIGINT,
# MAGIC   status_date DATE,
# MAGIC   load_date DATE,
# MAGIC   create_user STRING,
# MAGIC   create_timestamp TIMESTAMP
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_deferred_revenue_cases'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,OS34 Report: Abandonments FYTD
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.os34_report_abandonments_fytd (
# MAGIC   status_code BIGINT,
# MAGIC   status_description STRING,
# MAGIC   abandoned BOOLEAN,
# MAGIC   case_count BIGINT,
# MAGIC   class_count BIGINT,
# MAGIC   load_date DATE,
# MAGIC   is_static BOOLEAN,
# MAGIC   latest BOOLEAN,
# MAGIC   create_user STRING,
# MAGIC   create_timestamp TIMESTAMP
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_abandonments_fytd'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,OS34 Report: Deferred Revenue Cases FYTD
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.os34_report_deferred_revenue_cases (
# MAGIC   serial_num STRING,
# MAGIC   status_code STRING,
# MAGIC   active_classes BIGINT,
# MAGIC   status_date DATE,
# MAGIC   load_date DATE,
# MAGIC   create_user STRING,
# MAGIC   create_timestamp TIMESTAMP
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_deferred_revenue_cases'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Unsupervised Anomalies Features: Non-Exclusive Result Set
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.gold.unsupervised_anomalies_features_non_exclusive (
# MAGIC   load_date DATE NOT NULL COMMENT 'The load date of the ingest ETL.',
# MAGIC   latest BOOLEAN COMMENT 'The flag indicating whether the associated record was in the latest load.',
# MAGIC   cfk_patron_id STRING NOT NULL
# MAGIC     COMMENT 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
# MAGIC   applicant_bin STRING COMMENT 'A hardcoded bin based on submission history. It can be one of: Small, Medium, Large or Very Large',
# MAGIC   submission_burst_rate DOUBLE COMMENT 'The rate of application submissions',
# MAGIC   d_sig_burst_rate DOUBLE COMMENT 'The direct signature burst rate associated with the account.',
# MAGIC   submissions_per_day DOUBLE COMMENT 'The average submissions per day associated with the account.',
# MAGIC   z_day_0 DOUBLE COMMENT 'The z-score associated with the cumulative submissions on the first day since the creation of the account.',
# MAGIC   z_day_30 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_90 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_180 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_360 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_0 DOUBLE COMMENT 'The log adjusted cumulative submissions on the first day since the creation of the account.',
# MAGIC   log_cumulative_day_30 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_90 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 90th day since the creation of the account.',
# MAGIC   log_cumulative_day_180 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 180th day since the creation of the account.',
# MAGIC   log_cumulative_day_360 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 360th day since the creation of the account.',
# MAGIC   name_similarity_score DOUBLE COMMENT 'The similarity of signatory names associated with the account name historically.',
# MAGIC   avg_class_count DOUBLE COMMENT 'The average class counts the account has submitted applications for.',
# MAGIC   hourly_entropy DOUBLE COMMENT 'The predictability of the hour of a submission associated with an account.',
# MAGIC   weekday_entropy DOUBLE COMMENT 'The predictability of the day of the week of a submission associated with an account.',
# MAGIC   normalized_ip_entropy DOUBLE COMMENT 'The predictability of an IP address associated with an account.',
# MAGIC   ip_switch_burst_rate DOUBLE COMMENT 'The rate at which the account has switched IP addresses across application submissions historically.',
# MAGIC   avg_sig_type_entropy DOUBLE COMMENT 'The predictability of the signature type for the associated account.',
# MAGIC   avg_sig_type_change_rate DOUBLE COMMENT 'The average rate that the signature type changed for the associated account.',
# MAGIC   max_sig_type_entropy DOUBLE COMMENT 'The max unpredictabliity of the signature type for the associated account.',
# MAGIC   max_sig_type_change_rate DOUBLE COMMENT 'The max rate that the signature type changed for the associated account',
# MAGIC   avg_name_similarity DOUBLE COMMENT 'The average name similarity of the signatory name to the name associated with the account.',
# MAGIC   max_name_distance DOUBLE COMMENT 'The max difference (by levenshtein) the signatory name was to the name associated with the account.',
# MAGIC   avg_cluster_size DOUBLE COMMENT 'The average size of the cluster based on name matching that the account belonged to.',
# MAGIC   baseline_intl_ratio DOUBLE COMMENT 'The ratio of domestic to international applications the associated account.',
# MAGIC   recent_50_intl_ratio DOUBLE COMMENT 'The ratio of domestic to international applications the associated account, considering the last 50 applications.',
# MAGIC   recent_10_intl_ratio DOUBLE COMMENT 'The ratio of domestic to international applications the associated account, considering the last 10 applications.',
# MAGIC   intl_spike_zscore_50 DOUBLE COMMENT 'The z-score of international applications submissions associated account, considering the last 50 applications.',
# MAGIC   intl_spike_zscore_10 DOUBLE COMMENT 'The z-score of international applications submissions associated account, considering the last 10 applications.',
# MAGIC   intl_streak_len INT COMMENT 'The length of the number of applications submitted for international applicants from the associated account.',
# MAGIC   completeness DOUBLE COMMENT 'A data quality indicator for the completeness of the row. Used to choose whether or not the data should be used or imputed with something else.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp that the record was loaded.',
# MAGIC   create_user STRING COMMENT 'The system or individual responsible for creating the record.',
# MAGIC   CONSTRAINT `unsupervised_anomalies_features_non_exclusive_pk` PRIMARY KEY (`cfk_patron_id`)
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (load_date)
# MAGIC COMMENT 'A feature store which includes patrons that have already been identified as anomalous by the model.  This is contrary to the sibling table `unsupervised_anomalies_features_exclusive`.'
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unsupervised_anomalies_features_non_exclusive'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Unsupervised Anomalies Features: Exclusive Result Set
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.gold.unsupervised_anomalies_features_exclusive (
# MAGIC   load_date DATE NOT NULL COMMENT 'The load date of the ingest ETL.',
# MAGIC   latest BOOLEAN COMMENT 'The flag indicating whether the associated record was in the latest load.',
# MAGIC   cfk_patron_id STRING NOT NULL
# MAGIC     COMMENT 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
# MAGIC   applicant_bin STRING COMMENT 'A hardcoded bin based on submission history. It can be one of: Small, Medium, Large or Very Large',
# MAGIC   submission_burst_rate DOUBLE COMMENT 'The rate of application submissions',
# MAGIC   d_sig_burst_rate DOUBLE COMMENT 'The direct signature burst rate associated with the account.',
# MAGIC   submissions_per_day DOUBLE COMMENT 'The average submissions per day associated with the account.',
# MAGIC   z_day_0 DOUBLE COMMENT 'The z-score associated with the cumulative submissions on the first day since the creation of the account.',
# MAGIC   z_day_30 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_90 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_180 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_360 DOUBLE COMMENT 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_0 DOUBLE COMMENT 'The log adjusted cumulative submissions on the first day since the creation of the account.',
# MAGIC   log_cumulative_day_30 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_90 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 90th day since the creation of the account.',
# MAGIC   log_cumulative_day_180 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 180th day since the creation of the account.',
# MAGIC   log_cumulative_day_360 DOUBLE COMMENT 'The log adjusted cumulative submissions up to and including the 360th day since the creation of the account.',
# MAGIC   name_similarity_score DOUBLE COMMENT 'The similarity of signatory names associated with the account name historically.',
# MAGIC   avg_class_count DOUBLE COMMENT 'The average class counts the account has submitted applications for.',
# MAGIC   hourly_entropy DOUBLE COMMENT 'The predictability of the hour of a submission associated with an account.',
# MAGIC   weekday_entropy DOUBLE COMMENT 'The predictability of the day of the week of a submission associated with an account.',
# MAGIC   normalized_ip_entropy DOUBLE COMMENT 'The predictability of an IP address associated with an account.',
# MAGIC   ip_switch_burst_rate DOUBLE COMMENT 'The rate at which the account has switched IP addresses across application submissions historically.',
# MAGIC   avg_sig_type_entropy DOUBLE COMMENT 'The predictability of the signature type for the associated account.',
# MAGIC   avg_sig_type_change_rate DOUBLE COMMENT 'The average rate that the signature type changed for the associated account.',
# MAGIC   max_sig_type_entropy DOUBLE COMMENT 'The max unpredictabliity of the signature type for the associated account.',
# MAGIC   max_sig_type_change_rate DOUBLE COMMENT 'The max rate that the signature type changed for the associated account',
# MAGIC   avg_name_similarity DOUBLE COMMENT 'The average name similarity of the signatory name to the name associated with the account.',
# MAGIC   max_name_distance DOUBLE COMMENT 'The max difference (by levenshtein) the signatory name was to the name associated with the account.',
# MAGIC   avg_cluster_size DOUBLE COMMENT 'The average size of the cluster based on name matching that the account belonged to.',
# MAGIC   baseline_intl_ratio DOUBLE COMMENT 'The ratio of domestic to international applications the associated account.',
# MAGIC   recent_50_intl_ratio DOUBLE COMMENT 'The ratio of domestic to international applications the associated account, considering the last 50 applications.',
# MAGIC   recent_10_intl_ratio DOUBLE COMMENT 'The ratio of domestic to international applications the associated account, considering the last 10 applications.',
# MAGIC   intl_spike_zscore_50 DOUBLE COMMENT 'The z-score of international applications submissions associated account, considering the last 50 applications.',
# MAGIC   intl_spike_zscore_10 DOUBLE COMMENT 'The z-score of international applications submissions associated account, considering the last 10 applications.',
# MAGIC   intl_streak_len INT COMMENT 'The length of the number of applications submitted for international applicants from the associated account.',
# MAGIC   completeness DOUBLE COMMENT 'A data quality indicator for the completeness of the row. Used to choose whether or not the data should be used or imputed with something else.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp that the record was loaded.',
# MAGIC   create_user STRING COMMENT 'The system or individual responsible for creating the record.',
# MAGIC   CONSTRAINT `unsupervised_anomalies_features_exclusive_pk` PRIMARY KEY (`cfk_patron_id`)
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (load_date)
# MAGIC COMMENT 'A feature store which includes patrons that have never been identified as anomalous by the model.  This is contrary to the sibling table `unsupervised_anomalies_features_non_exclusive`.'
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unsupervised_anomalies_features_exclusive'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Unsupervised Anomalies Features: Exclusive
# MAGIC %sql
# MAGIC create table ${conf.catalog}.gold.unsupervised_anomalies_features_exclusive (
# MAGIC   load_date date not null comment 'The load date of the ingest ETL.',
# MAGIC   latest boolean
# MAGIC     comment 'The flag indicating whether the associated record was in the latest load.',
# MAGIC   cfk_patron_id string
# MAGIC     not null
# MAGIC     comment 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
# MAGIC   applicant_bin string
# MAGIC     comment 'A hardcoded bin based on submission history. It can be one of: Small, Medium, Large or Very Large',
# MAGIC   submission_burst_rate double comment 'The rate of application submissions',
# MAGIC   d_sig_burst_rate double comment 'The direct signature burst rate associated with the account.',
# MAGIC   submissions_per_day double comment 'The average submissions per day associated with the account.',
# MAGIC   z_day_0 double
# MAGIC     comment 'The z-score associated with the cumulative submissions on the first day since the creation of the account.',
# MAGIC   z_day_30 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_90 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_180 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_360 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_0 double
# MAGIC     comment 'The log adjusted cumulative submissions on the first day since the creation of the account.',
# MAGIC   log_cumulative_day_30 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_90 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 90th day since the creation of the account.',
# MAGIC   log_cumulative_day_180 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 180th day since the creation of the account.',
# MAGIC   log_cumulative_day_360 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 360th day since the creation of the account.',
# MAGIC   name_similarity_score double
# MAGIC     comment 'The similarity of signatory names associated with the account name historically.',
# MAGIC   avg_class_count double
# MAGIC     comment 'The average class counts the account has submitted applications for.',
# MAGIC   hourly_entropy double
# MAGIC     comment 'The predictability of the hour of a submission associated with an account.',
# MAGIC   weekday_entropy double
# MAGIC     comment 'The predictability of the day of the week of a submission associated with an account.',
# MAGIC   normalized_ip_entropy double
# MAGIC     comment 'The predictability of an IP address associated with an account.',
# MAGIC   ip_switch_burst_rate double
# MAGIC     comment 'The rate at which the account has switched IP addresses across application submissions historically.',
# MAGIC   avg_sig_type_entropy double
# MAGIC     comment 'The predictability of the signature type for the associated account.',
# MAGIC   avg_sig_type_change_rate double
# MAGIC     comment 'The average rate that the signature type changed for the associated account.',
# MAGIC   max_sig_type_entropy double
# MAGIC     comment 'The max unpredictabliity of the signature type for the associated account.',
# MAGIC   max_sig_type_change_rate double
# MAGIC     comment 'The max rate that the signature type changed for the associated account',
# MAGIC   avg_name_similarity double
# MAGIC     comment 'The average name similarity of the signatory name to the name associated with the account.',
# MAGIC   max_name_distance double
# MAGIC     comment 'The max difference (by levenshtein) the signatory name was to the name associated with the account.',
# MAGIC   avg_cluster_size double
# MAGIC     comment 'The average size of the cluster based on name matching that the account belonged to.',
# MAGIC   baseline_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account.',
# MAGIC   recent_50_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 50 applications.',
# MAGIC   recent_10_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 10 applications.',
# MAGIC   intl_spike_zscore_50 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 50 applications.',
# MAGIC   intl_spike_zscore_10 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 10 applications.',
# MAGIC   intl_streak_len int
# MAGIC     comment 'The length of the number of applications submitted for international applicants from the associated account.',
# MAGIC   completeness double
# MAGIC     comment 'A data quality indicator for the completeness of the row. Used to choose whether or not the data should be used or imputed with something else.',
# MAGIC   create_ts timestamp comment 'The timestamp that the record was loaded.',
# MAGIC   create_user string comment 'The system or individual responsible for creating the record.',
# MAGIC   selected_role string comment 'Selected role of the patron',
# MAGIC   num_has_sponsored int comment 'Number of times the patron has sponsored others',
# MAGIC   num_has_been_sponsored_by int comment 'Number of times the patron has been sponsored by others',
# MAGIC   is_ten_minute_rapid_filer int comment 'Flag if patron is a rapid filer within ten minutes',
# MAGIC   is_one_minute_rapid_filer int comment 'Flag if patron is a rapid filer within one minute',
# MAGIC   num_times_owner_signed_as_attorney int comment 'Number of times owner signed as attorney',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_owner_same_ip int
# MAGIC     comment 'Max distinct hand/e-sign names as owner from same IP',
# MAGIC   max_num_distinct_different_names int comment 'Max number of distinct names used by patron',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip int
# MAGIC     comment 'Max distinct hand/e-sign names as attorney from same IP',
# MAGIC   max_num_distinct_signatory_names_with_direct_signature_from_same_ip int
# MAGIC     comment 'Max distinct signatory names with direct signature from same IP',
# MAGIC   num_distinct_signatory_names_with_direct_signature int
# MAGIC     comment 'Number of distinct signatory names with direct signature',
# MAGIC   has_submissions_every_fifteen_for_six_hours_or_more int
# MAGIC     comment 'Flag for submissions every 15 minutes for at least 6 hours',
# MAGIC   has_submissions_without_six_hour_break_for_one_day int
# MAGIC     comment 'Flag for submissions every 6 hours for at least 24 hours',
# MAGIC   constraint `unsupervised_anomalies_features_exclusive_pk` primary key (`cfk_patron_id`)
# MAGIC ) using delta
# MAGIC comment 'A feature store which includes patrons that have never been identified as anomalous by the model.  This is contrary to the sibling table `unsupervised_anomalies_features_non_exclusive`.'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unsupervised_anomalies_features_exclusive'
# MAGIC tblproperties (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Unsupervised Anomalies Features: Non-Exclusive
# MAGIC %sql
# MAGIC create table ${conf.catalog}.gold.unsupervised_anomalies_features_non_exclusive (
# MAGIC   load_date date not null comment 'The load date of the ingest ETL.',
# MAGIC   latest boolean
# MAGIC     comment 'The flag indicating whether the associated record was in the latest load.',
# MAGIC   cfk_patron_id string
# MAGIC     not null
# MAGIC     comment 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
# MAGIC   applicant_bin string
# MAGIC     comment 'A hardcoded bin based on submission history. It can be one of: Small, Medium, Large or Very Large',
# MAGIC   submission_burst_rate double comment 'The rate of application submissions',
# MAGIC   d_sig_burst_rate double comment 'The direct signature burst rate associated with the account.',
# MAGIC   submissions_per_day double comment 'The average submissions per day associated with the account.',
# MAGIC   z_day_0 double
# MAGIC     comment 'The z-score associated with the cumulative submissions on the first day since the creation of the account.',
# MAGIC   z_day_30 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_90 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_180 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_360 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_0 double
# MAGIC     comment 'The log adjusted cumulative submissions on the first day since the creation of the account.',
# MAGIC   log_cumulative_day_30 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_90 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 90th day since the creation of the account.',
# MAGIC   log_cumulative_day_180 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 180th day since the creation of the account.',
# MAGIC   log_cumulative_day_360 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 360th day since the creation of the account.',
# MAGIC   name_similarity_score double
# MAGIC     comment 'The similarity of signatory names associated with the account name historically.',
# MAGIC   avg_class_count double
# MAGIC     comment 'The average class counts the account has submitted applications for.',
# MAGIC   hourly_entropy double
# MAGIC     comment 'The predictability of the hour of a submission associated with an account.',
# MAGIC   weekday_entropy double
# MAGIC     comment 'The predictability of the day of the week of a submission associated with an account.',
# MAGIC   normalized_ip_entropy double
# MAGIC     comment 'The predictability of an IP address associated with an account.',
# MAGIC   ip_switch_burst_rate double
# MAGIC     comment 'The rate at which the account has switched IP addresses across application submissions historically.',
# MAGIC   avg_sig_type_entropy double
# MAGIC     comment 'The predictability of the signature type for the associated account.',
# MAGIC   avg_sig_type_change_rate double
# MAGIC     comment 'The average rate that the signature type changed for the associated account.',
# MAGIC   max_sig_type_entropy double
# MAGIC     comment 'The max unpredictabliity of the signature type for the associated account.',
# MAGIC   max_sig_type_change_rate double
# MAGIC     comment 'The max rate that the signature type changed for the associated account',
# MAGIC   avg_name_similarity double
# MAGIC     comment 'The average name similarity of the signatory name to the name associated with the account.',
# MAGIC   max_name_distance double
# MAGIC     comment 'The max difference (by levenshtein) the signatory name was to the name associated with the account.',
# MAGIC   avg_cluster_size double
# MAGIC     comment 'The average size of the cluster based on name matching that the account belonged to.',
# MAGIC   baseline_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account.',
# MAGIC   recent_50_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 50 applications.',
# MAGIC   recent_10_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 10 applications.',
# MAGIC   intl_spike_zscore_50 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 50 applications.',
# MAGIC   intl_spike_zscore_10 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 10 applications.',
# MAGIC   intl_streak_len int
# MAGIC     comment 'The length of the number of applications submitted for international applicants from the associated account.',
# MAGIC   completeness double
# MAGIC     comment 'A data quality indicator for the completeness of the row. Used to choose whether or not the data should be used or imputed with something else.',
# MAGIC   create_ts timestamp comment 'The timestamp that the record was loaded.',
# MAGIC   create_user string comment 'The system or individual responsible for creating the record.',
# MAGIC   selected_role string comment 'Selected role of the patron',
# MAGIC   num_has_sponsored int comment 'Number of times the patron has sponsored others',
# MAGIC   num_has_been_sponsored_by int comment 'Number of times the patron has been sponsored by others',
# MAGIC   is_ten_minute_rapid_filer int comment 'Flag if patron is a rapid filer within ten minutes',
# MAGIC   is_one_minute_rapid_filer int comment 'Flag if patron is a rapid filer within one minute',
# MAGIC   num_times_owner_signed_as_attorney int comment 'Number of times owner signed as attorney',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_owner_same_ip int
# MAGIC     comment 'Max distinct hand/e-sign names as owner from same IP',
# MAGIC   max_num_distinct_different_names int comment 'Max number of distinct names used by patron',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip int
# MAGIC     comment 'Max distinct hand/e-sign names as attorney from same IP',
# MAGIC   max_num_distinct_signatory_names_with_direct_signature_from_same_ip int
# MAGIC     comment 'Max distinct signatory names with direct signature from same IP',
# MAGIC   num_distinct_signatory_names_with_direct_signature int
# MAGIC     comment 'Number of distinct signatory names with direct signature',
# MAGIC   has_submissions_every_fifteen_for_six_hours_or_more int
# MAGIC     comment 'Flag for submissions every 15 minutes for at least 6 hours',
# MAGIC   has_submissions_without_six_hour_break_for_one_day int
# MAGIC     comment 'Flag for submissions every 6 hours for at least 24 hours',
# MAGIC   constraint `unsupervised_anomalies_features_non_exclusive_pk` primary key (`cfk_patron_id`)
# MAGIC ) using delta
# MAGIC comment 'A feature store which includes patrons that have already been identified as anomalous by the model.  This is contrary to the sibling table `unsupervised_anomalies_features_exclusive`.'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unsupervised_anomalies_features_non_exclusive'
# MAGIC tblproperties (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Unsupervised Anomalies Report
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.gold.unsupervised_anomalies (
# MAGIC   cfk_patron_id STRING COMMENT 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
# MAGIC   latest_anomaly_score DOUBLE COMMENT 'The most recent score indicating whether or not the account was identified as anomalous. Negative values indicate more anomalous accounts.',
# MAGIC   first_appeared TIMESTAMP COMMENT 'The first time the account appeared as an anomaly by the model.',
# MAGIC   last_appeared TIMESTAMP COMMENT 'The most recent time the account appeared as an anomaly by the model.',
# MAGIC   times_appeared BIGINT COMMENT 'The number of times the account appeared as an anomaly by the model.',
# MAGIC   pct_times_appeared_of_total_runs DOUBLE COMMENT 'The number of times the account appeared as an anomaly by the model divided by the number of total model executions.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp that the record was loaded.',
# MAGIC   create_user STRING COMMENT 'The system or individual responsible for creating the record.'
# MAGIC ) USING delta
# MAGIC COMMENT 'A table containing basic statistics around accounts that have been identified as anomalous via the Isolation Forest model.'
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unsupervised_anomalies'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ${conf.catalog}.gold.unsupervised_anomalies_cumulative (
# MAGIC   load_date date comment 'The load date associated with the ML model execution.',
# MAGIC   latest boolean
# MAGIC     comment 'A flag associated with the attempted load of the ETL that indicates whether the records in the batch are the latest batch of the complete table history.',
# MAGIC   is_anomaly boolean
# MAGIC     comment 'A flag indicating whether or not the particular account (cfk_patron_id) was identified as anomalous.',
# MAGIC   anomaly_score double
# MAGIC     comment 'A score indicating whether or not the account was identified as anomalous. Negative values indicate more anomalous accounts.',
# MAGIC   cfk_patron_id string
# MAGIC     comment 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
# MAGIC   applicant_bin string
# MAGIC     comment 'A hardcoded bin based on submission history. It can be one of: Small, Medium, Large or Very Large',
# MAGIC   submission_burst_rate double comment 'The rate of application submissions',
# MAGIC   d_sig_burst_rate double comment 'The direct signature burst rate associated with the account.',
# MAGIC   submissions_per_day double comment 'The average submissions per day associated with the account.',
# MAGIC   z_day_0 double
# MAGIC     comment 'The z-score associated with the cumulative submissions on the first day since the creation of the account.',
# MAGIC   z_day_30 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_90 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_180 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_360 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_0 double
# MAGIC     comment 'The log adjusted cumulative submissions on the first day since the creation of the account.',
# MAGIC   log_cumulative_day_30 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_90 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 90th day since the creation of the account.',
# MAGIC   log_cumulative_day_180 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 180th day since the creation of the account.',
# MAGIC   log_cumulative_day_360 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 360th day since the creation of the account.',
# MAGIC   name_similarity_score double
# MAGIC     comment 'The similarity of signatory names associated with the account name historically.',
# MAGIC   avg_class_count double
# MAGIC     comment 'The average class counts the account has submitted applications for.',
# MAGIC   hourly_entropy double
# MAGIC     comment 'The predictability of the hour of a submission associated with an account.',
# MAGIC   weekday_entropy double
# MAGIC     comment 'The predictability of the day of the week of a submission associated with an account.',
# MAGIC   normalized_ip_entropy double
# MAGIC     comment 'The predictability of an IP address associated with an account.',
# MAGIC   ip_switch_burst_rate double
# MAGIC     comment 'The rate at which the account has switched IP addresses across application submissions historically.',
# MAGIC   avg_sig_type_entropy double
# MAGIC     comment 'The predictability of the signature type for the associated account.',
# MAGIC   avg_sig_type_change_rate double
# MAGIC     comment 'The average rate that the signature type changed for the associated account.',
# MAGIC   max_sig_type_entropy double
# MAGIC     comment 'The max unpredictabliity of the signature type for the associated account.',
# MAGIC   max_sig_type_change_rate double
# MAGIC     comment 'The max rate that the signature type changed for the associated account',
# MAGIC   avg_name_similarity double
# MAGIC     comment 'The average name similarity of the signatory name to the name associated with the account.',
# MAGIC   max_name_distance double
# MAGIC     comment 'The max difference (by levenshtein) the signatory name was to the name associated with the account.',
# MAGIC   avg_cluster_size double
# MAGIC     comment 'The average size of the cluster based on name matching that the account belonged to.',
# MAGIC   baseline_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account.',
# MAGIC   recent_50_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 50 applications.',
# MAGIC   recent_10_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 10 applications.',
# MAGIC   intl_spike_zscore_50 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 50 applications.',
# MAGIC   intl_spike_zscore_10 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 10 applications.',
# MAGIC   intl_streak_len int
# MAGIC     comment 'The length of the number of applications submitted for international applicants from the associated account.',
# MAGIC   completeness double
# MAGIC     comment 'A data quality indicator for the completeness of the row. Used to choose whether or not the data should be used or imputed with something else.',
# MAGIC   create_ts timestamp comment 'The timestamp that the record was loaded.',
# MAGIC   create_user string comment 'The system or individual responsible for creating the record.',
# MAGIC   selected_role string comment 'Role selected by the applicant',
# MAGIC   num_has_sponsored int comment 'Number of times the account has sponsored others',
# MAGIC   num_has_been_sponsored_by int comment 'Number of times the account has been sponsored by others',
# MAGIC   is_ten_minute_rapid_filer int comment 'Flag indicating rapid filing within ten minutes',
# MAGIC   is_one_minute_rapid_filer int comment 'Flag indicating rapid filing within one minute',
# MAGIC   num_times_owner_signed_as_attorney int comment 'Number of times owner signed as attorney',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_owner_same_ip int
# MAGIC     comment 'Max distinct hand/e-signs as owner from same IP',
# MAGIC   max_num_distinct_different_names int comment 'Max distinct names used',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip int
# MAGIC     comment 'Max distinct hand/e-signs as attorney from same IP',
# MAGIC   max_num_distinct_signatory_names_with_direct_signature_from_same_ip int
# MAGIC     comment 'Max distinct signatory names with direct signature from same IP',
# MAGIC   num_distinct_signatory_names_with_direct_signature int
# MAGIC     comment 'Number of distinct signatory names with direct signature',
# MAGIC   has_submissions_every_fifteen_for_six_hours_or_more int
# MAGIC     comment 'Flag for submissions every 15 minutes for 6+ hours',
# MAGIC   has_submissions_without_six_hour_break_for_one_day int
# MAGIC     comment 'Flag for submissions without 6-hour break for one day'
# MAGIC ) using delta
# MAGIC comment 'A table containing the historical store of accounts that have been identified as anomalous for the current run. Accounts can appear multiple times as anomalies.'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unsupervised_anomalies_cumulative'
# MAGIC tblproperties (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ${conf.catalog}.gold.unsupervised_anomalies_cumulative_exclusive (
# MAGIC   load_date date comment 'The load date associated with the ML model execution.',
# MAGIC   latest boolean
# MAGIC     comment 'A flag associated with the attempted load of the ETL that indicates whether the records in the batch are the latest batch of the complete table history.',
# MAGIC   is_anomaly boolean
# MAGIC     comment 'A flag indicating whether or not the particular account (cfk_patron_id) was identified as anomalous.',
# MAGIC   anomaly_score double
# MAGIC     comment 'A score indicating whether or not the account was identified as anomalous. Negative values indicate more anomalous accounts.',
# MAGIC   cfk_patron_id string
# MAGIC     comment 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
# MAGIC   applicant_bin string
# MAGIC     comment 'A hardcoded bin based on submission history. It can be one of: Small, Medium, Large or Very Large',
# MAGIC   submission_burst_rate double comment 'The rate of application submissions',
# MAGIC   d_sig_burst_rate double comment 'The direct signature burst rate associated with the account.',
# MAGIC   submissions_per_day double comment 'The average submissions per day associated with the account.',
# MAGIC   z_day_0 double
# MAGIC     comment 'The z-score associated with the cumulative submissions on the first day since the creation of the account.',
# MAGIC   z_day_30 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_90 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_180 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   z_day_360 double
# MAGIC     comment 'The z-score associated with the cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_0 double
# MAGIC     comment 'The log adjusted cumulative submissions on the first day since the creation of the account.',
# MAGIC   log_cumulative_day_30 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 30th day since the creation of the account.',
# MAGIC   log_cumulative_day_90 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 90th day since the creation of the account.',
# MAGIC   log_cumulative_day_180 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 180th day since the creation of the account.',
# MAGIC   log_cumulative_day_360 double
# MAGIC     comment 'The log adjusted cumulative submissions up to and including the 360th day since the creation of the account.',
# MAGIC   name_similarity_score double
# MAGIC     comment 'The similarity of signatory names associated with the account name historically.',
# MAGIC   avg_class_count double
# MAGIC     comment 'The average class counts the account has submitted applications for.',
# MAGIC   hourly_entropy double
# MAGIC     comment 'The predictability of the hour of a submission associated with an account.',
# MAGIC   weekday_entropy double
# MAGIC     comment 'The predictability of the day of the week of a submission associated with an account.',
# MAGIC   normalized_ip_entropy double
# MAGIC     comment 'The predictability of an IP address associated with an account.',
# MAGIC   ip_switch_burst_rate double
# MAGIC     comment 'The rate at which the account has switched IP addresses across application submissions historically.',
# MAGIC   avg_sig_type_entropy double
# MAGIC     comment 'The predictability of the signature type for the associated account.',
# MAGIC   avg_sig_type_change_rate double
# MAGIC     comment 'The average rate that the signature type changed for the associated account.',
# MAGIC   max_sig_type_entropy double
# MAGIC     comment 'The max unpredictabliity of the signature type for the associated account.',
# MAGIC   max_sig_type_change_rate double
# MAGIC     comment 'The max rate that the signature type changed for the associated account',
# MAGIC   avg_name_similarity double
# MAGIC     comment 'The average name similarity of the signatory name to the name associated with the account.',
# MAGIC   max_name_distance double
# MAGIC     comment 'The max difference (by levenshtein) the signatory name was to the name associated with the account.',
# MAGIC   avg_cluster_size double
# MAGIC     comment 'The average size of the cluster based on name matching that the account belonged to.',
# MAGIC   baseline_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account.',
# MAGIC   recent_50_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 50 applications.',
# MAGIC   recent_10_intl_ratio double
# MAGIC     comment 'The ratio of domestic to international applications the associated account, considering the last 10 applications.',
# MAGIC   intl_spike_zscore_50 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 50 applications.',
# MAGIC   intl_spike_zscore_10 double
# MAGIC     comment 'The z-score of international applications submissions associated account, considering the last 10 applications.',
# MAGIC   intl_streak_len int
# MAGIC     comment 'The length of the number of applications submitted for international applicants from the associated account.',
# MAGIC   completeness double
# MAGIC     comment 'A data quality indicator for the completeness of the row. Used to choose whether or not the data should be used or imputed with something else.',
# MAGIC   create_ts timestamp comment 'The timestamp that the record was loaded.',
# MAGIC   create_user string comment 'The system or individual responsible for creating the record.',
# MAGIC   selected_role string comment 'Role selected by the applicant',
# MAGIC   num_has_sponsored int comment 'Number of times the account has sponsored others',
# MAGIC   num_has_been_sponsored_by int comment 'Number of times the account has been sponsored by others',
# MAGIC   is_ten_minute_rapid_filer int comment 'Flag indicating rapid filing within ten minutes',
# MAGIC   is_one_minute_rapid_filer int comment 'Flag indicating rapid filing within one minute',
# MAGIC   num_times_owner_signed_as_attorney int comment 'Number of times owner signed as attorney',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_owner_same_ip int
# MAGIC     comment 'Max distinct hand/e-signs as owner from same IP',
# MAGIC   max_num_distinct_different_names int comment 'Max distinct names used',
# MAGIC   max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip int
# MAGIC     comment 'Max distinct hand/e-signs as attorney from same IP',
# MAGIC   max_num_distinct_signatory_names_with_direct_signature_from_same_ip int
# MAGIC     comment 'Max distinct signatory names with direct signature from same IP',
# MAGIC   num_distinct_signatory_names_with_direct_signature int
# MAGIC     comment 'Number of distinct signatory names with direct signature',
# MAGIC   has_submissions_every_fifteen_for_six_hours_or_more int
# MAGIC     comment 'Flag for submissions every 15 minutes for 6+ hours',
# MAGIC   has_submissions_without_six_hour_break_for_one_day int
# MAGIC     comment 'Flag for submissions without 6-hour break for one day'
# MAGIC ) using delta
# MAGIC comment 'A table containing the historical store of accounts that have been identified as anomalous for the current run. Accounts can appear only once as anomalies.'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/unsupervised_anomalies_cumulative_exclusive'
# MAGIC tblproperties (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Hold Monitor Detail
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.gold.hold_monitoring_detail_h(
# MAGIC   hold_docket INT COMMENT 'Hold docket associated with the case.',
# MAGIC   category_cd STRING COMMENT 'Category code associated with the case',
# MAGIC   hold_status_cd STRING COMMENT 'Status of the hold (e.g., active, resolved).',
# MAGIC   serial_number STRING COMMENT 'Serial number of the hold record.',
# MAGIC   classes INT COMMENT 'Number of classes associated with the case.',
# MAGIC   new_case INT COMMENT 'Indication of whether the case was put on hold during this run month.',
# MAGIC   mark_name STRING COMMENT 'Full name of the mark.',
# MAGIC   placed_on_hold_date DATE COMMENT 'Date when the record was placed on hold.',
# MAGIC   days_on_hold INT COMMENT 'Number of days the record has been on hold.',
# MAGIC   status_cd STRING COMMENT 'Status code of the trademark.',
# MAGIC   abandonment_dt DATE COMMENT 'Date when the case was abandoned if it was abandoned.',
# MAGIC   run_date DATE COMMENT 'Date when this record snapshot was recorded.',
# MAGIC   run_month INT COMMENT 'Numeric month when this record snapshot was recorded.',
# MAGIC   run_month_abbr STRING COMMENT 'Text month when this record snapshot was recorded.',
# MAGIC   run_fy INT COMMENT 'Fiscal year when this record snapshot was recorded.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp that the record was loaded.',
# MAGIC   create_user STRING COMMENT 'The system or individual responsible for creating the record.',
# MAGIC   update_ts TIMESTAMP COMMENT 'The timestamp that the record was updated.',
# MAGIC   update_user_id STRING COMMENT 'The system or individual responsible for updating the record.'
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/hold_monitoring_detail_h'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.gold.cmra_address_validation (
# MAGIC   serial_number string not null,
# MAGIC   is_fully_valid_address boolean,
# MAGIC   is_partially_valid_address boolean,
# MAGIC   is_invalid_address boolean,
# MAGIC   is_unconfirmed_address boolean,
# MAGIC   is_fully_confirmed_address boolean,
# MAGIC   is_partially_confirmed_address boolean,
# MAGIC   is_not_exists_in_usps boolean,
# MAGIC   create_ts TIMESTAMP DEFAULT current_timestamp,
# MAGIC   create_user_id STRING DEFAULT 'CMRA_ETL',
# MAGIC   update_ts TIMESTAMP DEFAULT current_timestamp,
# MAGIC   update_user_id STRING DEFAULT 'CMRA_ETL',
# MAGIC   CONSTRAINT `cmra_address_validation_pk` PRIMARY KEY (`serial_number`)
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/cmra_address_validation'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = 'true',
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC   'delta.feature.changeDataFeed' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.gold.grand_model_pre_exam (
# MAGIC   calendar_day DATE COMMENT 'The date on which the data is aggregated',
# MAGIC   daily_filings INT COMMENT 'Cases with a filing date on the given date',
# MAGIC   pre_pex_cases INT COMMENT 'Total cases filed that have not yet moved to pre-exam on the given date',
# MAGIC   daily_pex_intake INT COMMENT 'Cases that moved to pre-exam on the given date',
# MAGIC   pex_cases INT COMMENT 'Total cases in pre-exam on the given date',
# MAGIC   post_pex_cases INT COMMENT 'Total cases that have left pre-exam but have not received a NWOS on the given date',
# MAGIC   daily_pcp_intake INT COMMENT 'Cases that moved to the pullable case pool on the given date',
# MAGIC   pullable_cases INT COMMENT 'Total cases in the pullable case pool on the given date',
# MAGIC   daily_docked INT COMMENT 'Cases docked on the given date',
# MAGIC   pex_daily_cases_processed INT COMMENT 'Cases processed by pre-exam on the given date',
# MAGIC   pex_autoprocessor_cases INT COMMENT 'Cases processed by the autoprocessor on the given date',
# MAGIC   pex_core_team_cases INT COMMENT 'Cases processed by the pre-exam core team on the given date',
# MAGIC   pex_core_team INT COMMENT 'Number of individuals working cases on the given date whose output exceeds pre-exam averages for the month',
# MAGIC   pex_avg_cases_core_team INT COMMENT 'Average cases processed by the pre-exam core team on the given date',
# MAGIC   pex_support_team_cases INT COMMENT 'Cases processed by the pre-exam support team on the given date',
# MAGIC   pex_support_team INT COMMENT 'Number of individuals working cases on the given date whose output is lower than pre-exam averages for the month',
# MAGIC   pex_avg_cases_support_team INT COMMENT 'Average cases processed by the pre-exam support team on the given date',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp that the record was loaded',
# MAGIC   create_user_id STRING COMMENT 'The system or individual responsible for creating the record',
# MAGIC   update_ts TIMESTAMP COMMENT 'The timestamp that the record was updated',
# MAGIC   update_user_id STRING COMMENT 'The system or individual responsible for updating the record')
# MAGIC
# MAGIC USING delta
# MAGIC PARTITIONED BY (calendar_day)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/grand_model_pre_exam'

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.ttab_paralegal_daily_summary (
# MAGIC      -- Primary Key
# MAGIC      summary_date            DATE NOT NULL COMMENT 'Report Date (Partition Key)',
# MAGIC      paralegal_employee_id   STRING NOT NULL COMMENT 'User ID (use "TOTAL" for aggregated row)',
# MAGIC      
# MAGIC      -- Foreign Keys
# MAGIC      paralegal_employee_no   INT COMMENT 'Employee Number (NULL for TOTAL row)',
# MAGIC      paralegal_name          STRING COMMENT 'Full Name from dim_paralegal (NULL for TOTAL row)',
# MAGIC      
# MAGIC      -- Volume Metrics
# MAGIC      document_count          INT NOT NULL DEFAULT 0 COMMENT 'Total documents in queue',
# MAGIC      folder_count            INT NOT NULL DEFAULT 0 COMMENT 'Total folders in queue',
# MAGIC      total_records           INT NOT NULL DEFAULT 0 COMMENT 'Total items (Docs + Folders)',
# MAGIC      
# MAGIC      -- Assignment Method Breakdown
# MAGIC      count_method_direct     INT NOT NULL DEFAULT 0 COMMENT 'Items assigned via DIRECT method',
# MAGIC      count_method_proceeding INT NOT NULL DEFAULT 0 COMMENT 'Items assigned via PROCEEDING method',
# MAGIC      count_method_range      INT NOT NULL DEFAULT 0 COMMENT 'Items assigned via RANGE method',
# MAGIC      
# MAGIC      -- Object Type Breakdown
# MAGIC      count_opp               INT NOT NULL DEFAULT 0 COMMENT 'Opposition proceedings count',
# MAGIC      count_can               INT NOT NULL DEFAULT 0 COMMENT 'Cancellation proceedings count',
# MAGIC      count_other             INT NOT NULL DEFAULT 0 COMMENT 'Other object types count',
# MAGIC      
# MAGIC      -- SLA Metrics
# MAGIC      records_lte_7_days      INT NOT NULL DEFAULT 0 COMMENT 'Count of items <= 7 business days old',
# MAGIC      records_gt_7_days       INT NOT NULL DEFAULT 0 COMMENT 'Count of items > 7 business days old',
# MAGIC      pct_gt_7_days           DECIMAL(5,2) COMMENT 'Percentage of items breaching SLA (0-100)',
# MAGIC      
# MAGIC      -- Age Metrics
# MAGIC      total_days_lte_7        INT NOT NULL DEFAULT 0 COMMENT 'Sum of business days for items <= 7',
# MAGIC      total_days_gt_7         INT NOT NULL DEFAULT 0 COMMENT 'Sum of business days for items > 7',
# MAGIC      avg_age                 DECIMAL(10,2) COMMENT 'Average age (business days) of all items',
# MAGIC      avg_age_lte_7           DECIMAL(10,2) COMMENT 'Average age of items within SLA',
# MAGIC      avg_age_gt_7            DECIMAL(10,2) COMMENT 'Average age of items breaching SLA',
# MAGIC      max_age                 INT COMMENT 'Maximum age in business days found in queue',
# MAGIC      min_age                 INT COMMENT 'Minimum age in business days found in queue',
# MAGIC      
# MAGIC      -- Audit Fields
# MAGIC      source_snapshot_date    DATE NOT NULL COMMENT 'Snapshot date from silver layer that sourced this summary',
# MAGIC      source_record_count     INT NOT NULL COMMENT 'Number of silver records aggregated',
# MAGIC      dq_validated            BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'TRUE if all source records passed DQ validation',
# MAGIC      created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'Aggregation execution timestamp',
# MAGIC      
# MAGIC      CONSTRAINT pk_paralegal_summary PRIMARY KEY (summary_date, paralegal_employee_id)
# MAGIC  )
# MAGIC  USING DELTA
# MAGIC  PARTITIONED BY (summary_date)
# MAGIC  COMMENT 'Daily aggregated metrics by Paralegal with SLA tracking and workload breakdown'
# MAGIC  TBLPROPERTIES (
# MAGIC      'delta.enableChangeDataFeed' = 'true'
# MAGIC  );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.grand_model_milestone (
# MAGIC   ser_num INT COMMENT 'The unique serial number of the case',
# MAGIC   filing_dt DATE COMMENT 'Milestone Date for Filing',
# MAGIC   latest_ph_action_dt DATE COMMENT 'Date of most recent prosecution history action',
# MAGIC   disposal_dt DATE COMMENT 'Milestone Date for Disposal',
# MAGIC   abandonment_dt DATE COMMENT 'Miletone Date for Abandonment',
# MAGIC   potential_abandonment_dt DATE COMMENT 'POTENTIEL_ABANDONMENT_DT as defined by trm_tmngpdb_dev.bronze.tm_itu',
# MAGIC   abn_min_dt DATE COMMENT 'Min Date for ABN% Prosecution History Action',
# MAGIC   abn_max_dt DATE COMMENT 'Max Date for ABN% Prosecution History Action',
# MAGIC   revival_dt DATE COMMENT 'Milestone Date for Revival',
# MAGIC   div_parent BOOLEAN COMMENT 'Labeled as a divisional parent on trm_tmngpdb_dev.bronze.tm_divisional_child',
# MAGIC   div_child BOOLEAN COMMENT 'Labeled as a divisional child on trm_tmngpdb_dev.bronze.tm_divisional_child',
# MAGIC   repr_dt DATE COMMENT 'Min Date for REPR: SN ASSIGNED FOR SECT 66A APPL FROM IB',
# MAGIC   nwap_dt DATE COMMENT 'Min Date for NWAP: NEW APPLICATION ENTERED',
# MAGIC   nwos_dt DATE COMMENT 'Min Date for NWOS: NEW APPLICATION OFFICE SUPPLIED DATA ENTERED',
# MAGIC   dock_dt DATE COMMENT 'Milestone Date for Docking',
# MAGIC   cnsa_dt DATE COMMENT 'Min Date for CNSA: APPROVED FOR PUB - PRINCIPAL REGISTER',
# MAGIC   noam_dt DATE COMMENT 'Min Date for NOAM: NOA MAILED - SOU REQUIRED FROM APPLICANT',
# MAGIC   noa_dt DATE COMMENT 'Milestone Date for NOA',
# MAGIC   pubo_dt DATE COMMENT 'Min Date for PUBO: PUBLISHED FOR OPPOSITION',
# MAGIC   cnta_dt DATE COMMENT 'Min Date for CNTA: APPROVED FOR REGISTRATION SUPPLEMENTAL REGISTER',
# MAGIC   published_dt DATE COMMENT 'Milestone Date for Publication',
# MAGIC   aitu_dt DATE COMMENT 'Min Date for AITU: CASE ASSIGNED TO INTENT TO USE PARALEGAL',
# MAGIC   ext1_dt DATE COMMENT 'Milestone Date for EXT1: SOU EXTENSION 1 GRANTED',
# MAGIC   ext2_dt DATE COMMENT 'Milestone Date for EXT2: SOU EXTENSION 2 GRANTED',
# MAGIC   ext3_dt DATE COMMENT 'Milestone Date for EXT3: SOU EXTENSION 3 GRANTED',
# MAGIC   ext4_dt DATE COMMENT 'Milestone Date for EXT4: SOU EXTENSION 4 GRANTED',
# MAGIC   ext5_dt DATE COMMENT 'Milestone Date for EXT5: SOU EXTENSION 5 GRANTED',
# MAGIC   ex1g_dt DATE COMMENT 'Min Date for EX1G: SOU EXTENSION 1 GRANTED',
# MAGIC   ex2g_dt DATE COMMENT 'Min Date for EX1G: SOU EXTENSION 1 GRANTED',
# MAGIC   ex3g_dt DATE COMMENT 'Min Date for EX1G: SOU EXTENSION 1 GRANTED',
# MAGIC   ex4g_dt DATE COMMENT 'Min Date for EX1G: SOU EXTENSION 1 GRANTED',
# MAGIC   ex5g_dt DATE COMMENT 'Min Date for EX1G: SOU EXTENSION 1 GRANTED',
# MAGIC   eisu_dt DATE COMMENT 'Min Date for EISU: TEAS STATEMENT OF USE RECEIVED',
# MAGIC   supc_dt DATE COMMENT 'Min Date for SUPC: STATEMENT OF USE PROCESSING COMPLETE',
# MAGIC   suna_dt DATE COMMENT 'Min Date for SUNA: NOTICE OF ACCEPTANCE OF STATEMENT OF USE MAILED',
# MAGIC   iucn_dt DATE COMMENT 'Min Date for IUCN: NOTICE OF ALLOWANCE CANCELLED',
# MAGIC   pcbg_dt DATE COMMENT 'Min Date for PCBG: PETITION TO DIRECTOR - CHANGE BASIS - GRANTED',
# MAGIC   dp1b_dt DATE COMMENT 'Min Date for DP1B: 1(B) BASIS DELETED; PROCEED TO REGISTRATION',
# MAGIC   aupc_dt DATE COMMENT 'Min Date for AUPC: AMENDMENT TO USE PROCESSING COMPLETE',
# MAGIC   drrr_dt DATE COMMENT 'Min Date for DRRR: DIVISIONAL REQUEST RECEIVED',
# MAGIC   ertd_dt DATE COMMENT 'Min Date for ERTD: TEAS REQUEST TO DIVIDE RECEIVED',
# MAGIC   rtdr_dt DATE COMMENT 'Min Date for RTDR: REQUEST TO DIVIDE RECEIVED',
# MAGIC   dpcc_dt DATE COMMENT 'Min Date for DPCC: DIVISIONAL PROCESSING COMPLETE',
# MAGIC   untd_dt DATE COMMENT 'Min Date for UNTD: REQUEST TO DIVIDE UNTIMELY, REFUSED, OR WITHDRAWN',
# MAGIC   r_pr_dt DATE COMMENT 'Min Date for R.PR: REGISTERED-PRINCIPAL REGISTER',
# MAGIC   r_sr_dt DATE COMMENT 'Min Date for R.SR: REGISTERED-SUPPLEMENTAL REGISTER',
# MAGIC   registration_dt DATE COMMENT 'Milestone Date for Registration',
# MAGIC   prg_apre_dt DATE COMMENT 'Min Date for APRE: CASE ASSIGNED TO POST REGISTRATION PARALEGAL',
# MAGIC   prg_dash_start_dt DATE COMMENT 'Start Date of most recent post-reg action',
# MAGIC   prg_dash_end_dt DATE COMMENT 'End Date of most recent post-reg action',
# MAGIC   prg_dash_start_cd STRING COMMENT 'Start Code of most recent post-reg action',
# MAGIC   prg_dash_end_cd STRING COMMENT 'End Code of most recent post-reg action',
# MAGIC   postreg_category STRING COMMENT 'Category of most recent post-reg action',
# MAGIC   prg_es8r_dt DATE COMMENT 'Max Date for ES8R: TEAS SECTION 8 RECEIVED',
# MAGIC   prg_e89r_dt DATE COMMENT 'Max Date for E89R: TEAS SECTION 8 & 9 RECEIVED',
# MAGIC   prg_8ook_dt DATE COMMENT 'Max Date for 8.OK: REGISTERED - SEC. 8 (6-YR) ACCEPTED',
# MAGIC   prg_8prt_dt DATE COMMENT 'Max Date for 8PRT: REGISTERED - PARTIAL SEC. 8 (10-YR) ACCEPTED',
# MAGIC   prg_8opr_dt DATE COMMENT 'Max Date for 8.PR: REGISTERED - PARTIAL SEC. 8 (6-YR) ACCEPTED',
# MAGIC   prg_c8oo_dt DATE COMMENT 'Max Date for C8..: CANCELLED SEC. 8 (6-YR)',
# MAGIC   prg_c8ot_dt DATE COMMENT 'Max Date for C8.T: CANCELLED SEC. 8 (10-YR)',
# MAGIC   prg_caex_dt DATE COMMENT 'Max Date for CAEX: CANCELLED SEC. 8 (10-YR)/EXPIRED SECTION 9',
# MAGIC   prg_89ag_dt DATE COMMENT 'Max Date for 89AG: REGISTERED - SEC. 8 (10-YR) ACCEPTED/SEC. 9 GRANTED',
# MAGIC   prg_s89g_dt DATE COMMENT 'Max Date for S89G: REGISTERED-SUBSEQUENT SEC. 8 (10 YR) ACCEPTED/SEC. 9 GRANTED',
# MAGIC   create_ts TIMESTAMP COMMENT 'Date of creation',
# MAGIC   create_user_id STRING COMMENT 'User ID for creation',
# MAGIC   update_ts TIMESTAMP COMMENT 'Date of update',
# MAGIC   update_user_id STRING COMMENT 'User ID for update'
# MAGIC   )
# MAGIC
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/grand_model_milestone'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.tms_workflow_monitoring_dashboard (
# MAGIC   calendar_day DATE COMMENT 'The date on which the data is aggregated',
# MAGIC   cases INT COMMENT 'The number of cases in a process on the given date',
# MAGIC   intake INT COMMENT 'The number of cases that entered a given process on a given date', 
# MAGIC   output INT COMMENT 'The number of cases that exited a given process on a given date', 
# MAGIC   output_auto INT COMMENT 'Total autoprocessor cases that exited a process on the given date',
# MAGIC   output_manual INT COMMENT 'Total non-autoprocessor cases that exited a process on the given date',
# MAGIC   monthly_intake INT COMMENT 'Total cases that entered a process on the given month',
# MAGIC   monthly_output INT COMMENT 'Total cases that exited a process on the given month',
# MAGIC   monthly_output_auto INT COMMENT 'Total autoprocessor cases that exited a process on the given month',
# MAGIC   monthly_output_manual INT COMMENT 'Total non-autoprocessor cases that exited a process on the given month',
# MAGIC   month_to_date_intake INT COMMENT 'The number of cases that entered the process to date on the given month',
# MAGIC   month_to_date_output INT COMMENT 'The number of cases that exited the process to date on the given month',
# MAGIC   monthly_throughput INT COMMENT 'The difference between case intake and output on the given month',
# MAGIC   avg_resolution_days FLOAT COMMENT 'Mean of the difference between process entry and process exit for the cases that exited on the given date',
# MAGIC   median_resolution_days FLOAT COMMENT 'Median of the difference between process entry and process exit for the cases that exited on the given date',
# MAGIC   monthly_workers INT COMMENT 'Number of workers active in each business phase for the given month',
# MAGIC   monthly_work_rate INT COMMENT 'Average number of cases processed per month in each business phase without autoprocessing',
# MAGIC   target_pendency INT COMMENT 'Pendency targets hard coded per process',
# MAGIC   time_period STRING COMMENT 'Future or Historical time period based on whether the record is in the past or is a projection',
# MAGIC   process STRING COMMENT 'Individual process related to the record (e.g. Extension Request 1)',
# MAGIC   grouping STRING comment 'Process group related to the record (e.g. Extension Requests)',
# MAGIC   phase STRING comment 'Major business phase related to the record (e.g. Intent To Use)',
# MAGIC   fiscal_year INT COMMENT 'Fiscal Year on the given date',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp that the record was loaded',
# MAGIC   create_user_id STRING COMMENT 'The system or individual responsible for creating the record',
# MAGIC   update_ts TIMESTAMP COMMENT 'The timestamp that the record was updated',
# MAGIC   update_user_id STRING COMMENT 'The system or individual responsible for updating the record')
# MAGIC
# MAGIC USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tms_workflow_monitoring_dashboard'

# COMMAND ----------

# DBTITLE 1,annual_workload_table_16_18
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.annual_workload_table_16_18 (
# MAGIC     fiscal_year INT COMMENT 'Fiscal year',
# MAGIC     registration_filed_count INT COMMENT 'Number of trademark registrations filed (Workload Table 16)',
# MAGIC     renewal_filed_count INT COMMENT 'Number of trademark renewals filed (Workload Table 16)',
# MAGIC     s8_affidavit_filed_count INT COMMENT 'Number of Section 8 affidavits filed (Workload Table 16)',
# MAGIC     certs_reg_issued_count INT COMMENT 'Number of certificates of registration issued (Workload Table 18)',
# MAGIC     renewed_count INT COMMENT 'Number of trademarks renewed (Workload Table 18)',
# MAGIC     pub_12c_count INT COMMENT 'Number published under 12(c) (Workload Table 18)',
# MAGIC     reg_inc_class_count INT COMMENT 'Number of registrations including classes (Workload Table 18)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/annual_workload_table_16_18'
# MAGIC COMMENT 'Annual trademark workload statistics for Table 16 ("Trademark Applications Filed for Registrational and Renewal and Trademark Affidavits Filed") and Table 18 ("Trademarks Registered, Renewed, and Published Under Section 12(c)")'

# COMMAND ----------

# DBTITLE 1,annual_workload_table_21
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.annual_workload_table_21 (
# MAGIC     residence STRING COMMENT 'Country or region of residence',
# MAGIC     fiscal_year INT COMMENT 'Fiscal year',
# MAGIC     application_count INT COMMENT 'Number of trademark applications'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/annual_workload_table_21'
# MAGIC COMMENT 'Annual trademark workload statistics for Table 21 (Trademark Applications Filed by Residents of Foreign Countries and Territories)'

# COMMAND ----------

# DBTITLE 1,annual_workload_table_22
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.annual_workload_table_22 (
# MAGIC     residence STRING COMMENT 'Country or region of residence',
# MAGIC     fiscal_year INT COMMENT 'Fiscal year',
# MAGIC     registration_count INT COMMENT 'Number of trademark registrations'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/annual_workload_table_22'
# MAGIC COMMENT 'Annual trademark workload statistics for Table 22 (Trademarks Registered to Residents of Foreign Countries)'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ${conf.catalog}.gold.afr_pending_applications_quarterly (
# MAGIC     load_date date
# MAGIC       not null
# MAGIC       comment 'The date on which this batch of OS-34 report data was loaded into the gold layer. Combined with `stage` to form the composite primary key, allowing historical snapshots to be retained per load date.',
# MAGIC     stage int
# MAGIC       not null
# MAGIC       comment 'Numeric identifier for the stage of processing. Drives sort order and hierarchical rollup logic (e.g., stage 6 = sum of stages 8, 10, 25; stage 10 = sum of stages 12, 16, 18, 23).',
# MAGIC     stage_of_processing string
# MAGIC       not null
# MAGIC       comment 'Description the stage of trademark application processing (e.g., "Pending Applications, Total", "Under Examination, Total", "Awaiting First Action by Examiner").',
# MAGIC     application_files bigint
# MAGIC       comment 'Count of distinct serial numbers (application files) in this stage of processing, prior to adjustment for double-counted second-examination applications.',
# MAGIC     classes bigint
# MAGIC       comment 'Sum of active trademark classes across all application files in this stage of processing, prior to adjustment for double-counted second-examination applications.',
# MAGIC     adjusted_application_files bigint
# MAGIC       comment 'Adjusted count of distinct application files. For rollup stages ("Pending Applications, Total", "Under Examination, Total", "Intent-to-Use Applications Pending Use"), the count of "Applications Under Second Examination" is subtracted to avoid double-counting. All other stages are unadjusted.',
# MAGIC     adjusted_classes bigint
# MAGIC       comment 'Adjusted sum of active trademark classes. For rollup stages ("Pending Applications, Total", "Under Examination, Total", "Intent-to-Use Applications Pending Use"), the class count of "Applications Under Second Examination" is subtracted to avoid double-counting. All other stages are unadjusted.',
# MAGIC     is_active boolean
# MAGIC       not null
# MAGIC       default true
# MAGIC       comment 'Soft-delete flag. Set to false by the ETL merge when a stage row exists in the gold table for today\'s load_date but is no longer produced by the source query (e.g., all status codes for that stage have been retired). Historical rows from prior load dates are never affected. Consumers should filter on is_active = true for current-snapshot queries.',
# MAGIC     create_ts timestamp
# MAGIC       default current_timestamp
# MAGIC       comment 'Timestamp when the record was first created in the gold layer.',
# MAGIC     create_user_id string default 'AFR17_QUARTERLY_REPORT_ETL' comment 'User or process that created the record.',
# MAGIC     update_ts timestamp
# MAGIC       default current_timestamp
# MAGIC       comment 'Timestamp when the record was last updated in the gold layer.',
# MAGIC     update_user_id string
# MAGIC       default 'AFR17_QUARTERLY_REPORT_ETL'
# MAGIC       comment 'User or process that last updated the record.',
# MAGIC     constraint `afr_pending_applications_quarterly_pk` primary key (`stage`, `load_date`)
# MAGIC   ) using delta
# MAGIC   comment '
# MAGIC A gold-layer reporting table derived from the OS-34 Report Status Detail silver table. Each row represents one stage of trademark application processing as defined by the USPTO AFR17 report for a given load date. Rollup rows (stages 6 and 10) aggregate their child stages, and adjusted columns remove double-counting of applications that appear under both initial and second examination. Historical snapshots are preserved per load_date.
# MAGIC '
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/afr_pending_applications_quarterly'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.tms_workflow_monitoring_detail (
# MAGIC     ser_num INT COMMENT 'The unique serial number of the case',
# MAGIC     int_pxu_entry_dt DATE COMMENT 'Date the case entered the pre-exam process',
# MAGIC     int_pxu_exit_dt DATE COMMENT 'Date the case exited the pre-exam process',
# MAGIC     int_pxu_auto INT COMMENT 'Pre-exam autoprocessor designation',
# MAGIC     itu_sou_entry_dt DATE COMMENT 'Date the case entered the Statement of Use process',
# MAGIC     itu_sou_exit_dt DATE COMMENT 'Date the case exited the Statement of Use process',
# MAGIC     itu_sou_auto INT COMMENT 'Statement of Use autoprocessor designation',
# MAGIC     itu_ext_ex1_entry_dt DATE COMMENT 'Date the case entered the first Statement of Use extension process',
# MAGIC     itu_ext_ex1_exit_dt DATE COMMENT 'Date the case exited the first Statement of Use extension process',
# MAGIC     itu_ext_ex1_auto INT COMMENT 'Statement of Use Extension 1 autoprocessor designation',
# MAGIC     itu_ext_ex2_entry_dt DATE COMMENT 'Date the case entered the second statement of use extension process',
# MAGIC     itu_ext_ex2_exit_dt DATE COMMENT 'Date the case exited the second statement of use extension process',
# MAGIC     itu_ext_ex2_auto INT COMMENT 'Statement of Use Extension 2 autoprocessor designation',
# MAGIC     itu_ext_ex3_entry_dt DATE COMMENT 'Date the case entered the third statement of use extension process',
# MAGIC     itu_ext_ex3_exit_dt DATE COMMENT 'Date the case exited the third statement of use extension process',
# MAGIC     itu_ext_ex3_auto INT COMMENT 'Statement of Use Extension 3 autoprocessor designation',
# MAGIC     itu_ext_ex4_entry_dt DATE COMMENT 'Date the case entered the fourth statement of use extension process',
# MAGIC     itu_ext_ex4_exit_dt DATE COMMENT 'Date the case exited the fourth statement of use extension process',
# MAGIC     itu_ext_ex4_auto INT COMMENT 'Statement of Use Extension 4 autoprocessor designation',
# MAGIC     itu_ext_ex5_entry_dt DATE COMMENT 'Date the case entered the fifth statement of use extension process',
# MAGIC     itu_ext_ex5_exit_dt DATE COMMENT 'Date the case exited the fifth statement of use extension process',
# MAGIC     itu_ext_ex5_auto INT COMMENT 'Statement of Use Extension 5 autoprocessor designation',
# MAGIC     itu_div_entry_dt DATE COMMENT 'Date the case entered the Divisional process',
# MAGIC     itu_div_exit_dt DATE COMMENT 'Date the case exited the Divisional process',
# MAGIC     prg_06y_s08_entry_dt DATE COMMENT 'Date the case entered the Section 8 (6 Year) process',
# MAGIC     prg_06y_s08_exit_dt DATE COMMENT 'Date the case exited the Section 8 (6 Year) process',
# MAGIC     prg_06y_815_entry_dt DATE COMMENT 'Date the case entered the Section 8/15 (6 Year) process',
# MAGIC     prg_06y_815_exit_dt DATE COMMENT 'Date the case exited the Section 8/15 (6 Year) process',
# MAGIC     prg_06y_s71_entry_dt DATE COMMENT 'Date the case entered the Section 71 (6 Year) process',
# MAGIC     prg_06y_s71_exit_dt DATE COMMENT 'Date the case exited the Section 71 (6 Year) process',
# MAGIC     prg_06y_715_entry_dt DATE COMMENT 'Date the case entered the Section 7/15 (6 Year) process',
# MAGIC     prg_06y_715_exit_dt DATE COMMENT 'Date the case exited the Section 7/15 (6 Year) process',
# MAGIC     prg_10y_s89_entry_dt DATE COMMENT 'Date the case entered the Section 8/9 (10 Year) process',
# MAGIC     prg_10y_s89_exit_dt DATE COMMENT 'Date the case exited the Section 8/9 (10 Year) process',
# MAGIC     prg_10y_s71_entry_dt DATE COMMENT 'Date the case entered the Section 71 (10 Year) process',
# MAGIC     prg_10y_s71_exit_dt DATE COMMENT 'Date the case exited the Section 71 (10 Year) process',
# MAGIC     prg_10y_715_entry_dt DATE COMMENT 'Date the case entered the Section 7/15 (10 Year) process',
# MAGIC     prg_10y_715_exit_dt DATE COMMENT 'Date the case exited the Section 7/15 (10 Year) process',
# MAGIC     prg_s07_s07_entry_dt DATE COMMENT 'Date the case entered the Section 7 (10 Year) process',
# MAGIC     prg_s07_s07_exit_dt DATE COMMENT 'Date the case exited the Section 7 (10 Year) process',
# MAGIC     prg_s07_7rf_entry_dt DATE COMMENT 'Date the case entered the Section 7 Total Surrender (10 Year) (C7RF) process',
# MAGIC     prg_s07_7rf_exit_dt DATE COMMENT 'Date the case exited the Section 7 Total Surrender (10 Year) (C7RF) process',
# MAGIC     prg_s07_sur_entry_dt DATE COMMENT 'Date the case entered the Section 7 Surrender (10 Year) (ES7S) process',
# MAGIC     prg_s07_sur_exit_dt DATE COMMENT 'Date the case exited the Section 7 Surrender (10 Year) (ES7S) process',
# MAGIC     prg_s15_s15_entry_dt DATE COMMENT 'Date the case entered the Section 15 (10 Year) process',
# MAGIC     prg_s15_s15_exit_dt DATE COMMENT 'Date the case exited the Section 15 (10 Year) process',
# MAGIC     itu_assigned_date DATE COMMENT 'Date assigned to ITU paralegal',
# MAGIC     itu_assigned_eid STRING COMMENT 'Employee ID of ITU paralegal assigned to case',
# MAGIC     itu_assigned_name STRING COMMENT 'Name of ITU paralegal assigned to case',
# MAGIC     prg_assigned_date DATE COMMENT 'Date assigned to postreg paralegal',
# MAGIC     prg_assigned_eid STRING COMMENT 'Employee ID of postreg paralegal assigned to case',
# MAGIC     prg_assigned_name STRING COMMENT 'Name of postreg paralegal assigned to case',
# MAGIC     pxu_assigned_date DATE COMMENT 'Date assigned to pre-exam worker',
# MAGIC     pxu_assigned_eid STRING COMMENT 'Employee ID of preexam worker assigned to case',
# MAGIC     pxu_assigned_name STRING COMMENT 'Name of preexam worker assigned to case',
# MAGIC     create_ts TIMESTAMP COMMENT 'Date of creation',
# MAGIC     create_user_id STRING COMMENT 'User ID for creation',
# MAGIC     update_ts TIMESTAMP COMMENT 'Date of update',
# MAGIC     update_user_id STRING COMMENT 'User ID for update'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tms_workflow_monitoring_detail'
# MAGIC COMMENT 'This table provides the latest start and exit dates for cases that have passed through trademark services processes. Specifically, the table focuses on the Pre-Exam (PXU) process, processes related to the Intent to Use (ITU) division, and processes related to the Post-Registration (PRG) division.'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.gold.prod_simulator (
# MAGIC     employee_nm STRING COMMENT 'Employee name',
# MAGIC     current_organization_cd STRING COMMENT 'Current organization code',
# MAGIC     quarter_bi_week_start_dt DATE COMMENT 'Quarter bi-week start date',
# MAGIC     quarter_bi_week_end_dt DATE COMMENT 'Quarter bi-week end date',
# MAGIC     quarter_no STRING COMMENT 'Quarter number',
# MAGIC     q1_wks INT COMMENT 'Weeks in Q1',
# MAGIC     q2_wks INT COMMENT 'Weeks in Q2',
# MAGIC     q3_wks INT COMMENT 'Weeks in Q3',
# MAGIC     q4_wks INT COMMENT 'Weeks in Q4',
# MAGIC     brs_user_id STRING COMMENT 'Business Reporting System user ID',
# MAGIC     exam_hrs DECIMAL(12,2) COMMENT 'Examining hours',
# MAGIC     adj_hrs DECIMAL(12,2) COMMENT 'Adjusted hours',
# MAGIC     non_exam_hrs DECIMAL(12,2) COMMENT 'Non-examining hours',
# MAGIC     ot_hrs DECIMAL(10,2) COMMENT 'Overtime hours',
# MAGIC     bds DECIMAL(12,1) COMMENT 'Balanced disposal score',
# MAGIC     action_per_examining_hour_qt STRING COMMENT 'Actions per examining hour',
# MAGIC     goal_status_ct STRING COMMENT 'Goal status count',
# MAGIC     docket_management_qt STRING COMMENT 'Docket management quantity',
# MAGIC     document_management_tx STRING COMMENT 'Document management text',
# MAGIC     bi_week_below_goal_qt STRING COMMENT 'Bi-week below goal quantity',
# MAGIC     action_qt STRING COMMENT 'Action quantity',
# MAGIC     Table STRING COMMENT 'Source table',
# MAGIC     serial_num_tx STRING COMMENT 'Serial number',
# MAGIC     statutory_error_qt DECIMAL(5,2) COMMENT 'Statutory error quantity',
# MAGIC     prac_pro_error_qt INT COMMENT 'Practice/procedure error quantity',
# MAGIC     search_ct STRING COMMENT 'Search count',
# MAGIC     write_grade_txt STRING COMMENT 'Write grade text',
# MAGIC     fk_gs_level_cd STRING COMMENT 'GS level code',
# MAGIC     base_c_bds INT COMMENT 'Base C balanced disposal score',
# MAGIC     base_fs_bds INT COMMENT 'Base FS balanced disposal score',
# MAGIC     base_m_bds INT COMMENT 'Base M balanced disposal score',
# MAGIC     base_o_bds INT COMMENT 'Base O balanced disposal score',
# MAGIC     transfer_balanced_disposal_qt STRING COMMENT 'Transfer balanced disposal quantity',
# MAGIC     bds_from_last_qtr STRING COMMENT 'Balanced disposal score from last quarter',
# MAGIC     workflow_qtr_goal DECIMAL(5,2) COMMENT 'Workflow quarter goal',
# MAGIC     schedule_hour_qt STRING COMMENT 'Scheduled hour quantity',
# MAGIC     performance_rating_cd STRING COMMENT 'Performance rating code',
# MAGIC     next_qtr_perf_rate_cd STRING COMMENT 'Next quarter performance rating code',
# MAGIC     suff_rt DOUBLE COMMENT 'Sufficiency rate',
# MAGIC     suff_score STRING COMMENT 'Sufficiency score',
# MAGIC     avg_write_rt STRING COMMENT 'Average write rate',
# MAGIC     avg_write_score STRING COMMENT 'Average write score',
# MAGIC     `write_def%` STRING COMMENT 'Write deficiency percentage',
# MAGIC     fk_start_gs_grade_level_cd STRING COMMENT 'Start GS grade level code',
# MAGIC     promotion_dt STRING COMMENT 'Promotion date',
# MAGIC     org_effectiveness_rt STRING COMMENT 'Organization effectiveness rate',
# MAGIC     org_mentor_qual_rt STRING COMMENT 'Organization mentor qualification rate',
# MAGIC     org_mentor_rt STRING COMMENT 'Organization mentor rate',
# MAGIC     org_mentor_timely STRING COMMENT 'Organization mentor timely',
# MAGIC     org_train_rt STRING COMMENT 'Organization training rate',
# MAGIC     weighted_average_in BYTE COMMENT 'Weighted average indicator',
# MAGIC     weight_0_fully_successful_in BYTE COMMENT 'Weight 0 fully successful indicator',
# MAGIC     org_mentor_score STRING COMMENT 'Organization mentor score',
# MAGIC     org_trn_score STRING COMMENT 'Organization training score',
# MAGIC     org_eff_score STRING COMMENT 'Organization effectiveness score',
# MAGIC     prod_alloc_wgt DECIMAL(5,1) COMMENT 'Product allocation weight',
# MAGIC     qual_alloc_wgt DECIMAL(5,1) COMMENT 'Quality allocation weight',
# MAGIC     wf_alloc_wgt DECIMAL(5,1) COMMENT 'Workflow allocation weight',
# MAGIC     org_alloc_wgt DECIMAL(5,1) COMMENT 'Organization allocation weight',
# MAGIC     org_effectiveness_pt DECIMAL(12,2) COMMENT 'Organization effectiveness points',
# MAGIC     org_train_pt DECIMAL(12,2) COMMENT 'Organization training points',
# MAGIC     org_mentor_pt DECIMAL(12,2) COMMENT 'Organization mentor points',
# MAGIC     avg_score_rt DECIMAL(12,2) COMMENT 'Average score rate',
# MAGIC     examiner_amendment_usage_pt DECIMAL(12,2) COMMENT 'Examiner amendment usage points',
# MAGIC     workflow_performance_rating_cd STRING COMMENT 'Workflow performance rating code',
# MAGIC     no_sig_trainee_biweeks INT COMMENT 'Number of significant trainee bi-weeks',
# MAGIC     partial_sig_trainee_biweeks INT COMMENT 'Number of partial significant trainee bi-weeks',
# MAGIC     pfs_trainee_biweeks INT COMMENT 'Number of PFS trainee bi-weeks',
# MAGIC     exam_hrs_fy DECIMAL(22,2) COMMENT 'Examining hours fiscal year',
# MAGIC     refreshed TIMESTAMP COMMENT 'Last refreshed timestamp',
# MAGIC     max_pro_bds INT COMMENT 'Maximum production balanced disposal score',
# MAGIC     min_pro_bds INT COMMENT 'Minimum production balanced disposal score',
# MAGIC     cur_qtr INT COMMENT 'Current quarter'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/prod_simulator'
# MAGIC COMMENT 'This table contains production simulator hyper file data with exaiming hours and transfer bds'

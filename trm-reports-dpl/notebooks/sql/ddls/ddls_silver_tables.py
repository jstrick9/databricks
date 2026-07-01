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
tmngpb_catalog = common_configs['schema']['tmngpdb_src_catalog']
print(f"{trgt_catalog=}")
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('config.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.tmngpb_catalog', tmngpb_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.silver 
# MAGIC COMMENT 'For trm reports staging layer data' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.silver.job_log (
# MAGIC   job_log_id BIGINT, --not null generated always as identity,
# MAGIC   job_nm STRING,
# MAGIC   start_ts TIMESTAMP,
# MAGIC   end_ts TIMESTAMP,
# MAGIC   status_ct STRING,
# MAGIC   record_qt INT,
# MAGIC   comment_tx STRING
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/job_log'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.silver.job_control (
# MAGIC   job_control_id BIGINT ,--not null generated always as identity,
# MAGIC   job_nm STRING,
# MAGIC   load_ts TIMESTAMP,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   last_mod_ts TIMESTAMP,
# MAGIC   last_mod_user_id STRING
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/job_control'

# COMMAND ----------

# MAGIC %md
# MAGIC ##First Level tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.prosecution_history (
# MAGIC   serial_number INT COMMENT 'SER_NUM',
# MAGIC   ph_action_number INT,
# MAGIC   ph_action_code STRING,
# MAGIC   cm_sys_dt DATE,
# MAGIC   ph_action_date DATE,
# MAGIC   last_modified_date TIMESTAMP,
# MAGIC   oracle_apply_time TIMESTAMP,
# MAGIC   cm_prcd_num STRING,
# MAGIC   ri_notif_dt TIMESTAMP,
# MAGIC   cm_desc STRING,
# MAGIC   fifth_char_cm_type STRING COMMENT '5TH_CHAR_CM_TYPE',
# MAGIC   cm_flg_paper INT,
# MAGIC   ttab_tracking_num STRING,
# MAGIC   tm_worker_eid STRING,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/prosecution_history' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.owner (
# MAGIC   ser_num INT COMMENT 'The unique identifier for a trademark case, the serial number.',
# MAGIC   current_owner STRING COMMENT "Indicates 'Y' if current owner of the record.",
# MAGIC   party_type INT COMMENT 'Type of party: 10 = Original Applicant, 11 - 19 = Subsequent Owner Before Publication, 20 = Owner at Publication, 21-29 Subsequent Owner After Publication, 30 = Original Registrant, 31-69 = Subsequent Owner After Registration',
# MAGIC   name STRING COMMENT 'Name of the owner.',
# MAGIC   address_1 STRING COMMENT 'Primary address line.',
# MAGIC   address_2 STRING COMMENT 'Secondary address line (if any).',
# MAGIC   city STRING COMMENT 'City of the owners address.',
# MAGIC   postal_cd STRING COMMENT 'Postal code',
# MAGIC   citizenship STRING COMMENT 'Citizenship of the owner.',
# MAGIC   entity_type INT COMMENT 'Entity type (corporation, partnership, etc.)',
# MAGIC   ctry_nm STRING COMMENT 'Country name',
# MAGIC   ctry_cd STRING COMMENT 'Country code.',
# MAGIC   country_or_area_name STRING COMMENT 'Specifies the name of the country or area with respect to the owner',
# MAGIC   last_modified_date TIMESTAMP COMMENT 'Timestamp of the last modification.',
# MAGIC   state_cd STRING COMMENT 'State code',
# MAGIC   max_party_type INT COMMENT 'Maximum party type value.',
# MAGIC   owner_num INT COMMENT 'Owner Number is assigned based on partitioning by serial number and party type, ordering by entity number. 1 is the designation for primary owner.',
# MAGIC   owner_email STRING COMMENT 'Email of the owner.',
# MAGIC   create_ts TIMESTAMP COMMENT 'Timestamp when the record was created.',
# MAGIC   create_user_id STRING COMMENT 'User ID of the individual who created the record.',
# MAGIC   update_ts TIMESTAMP COMMENT 'Timestamp when the record was last updated.',
# MAGIC   update_user_id STRING COMMENT 'User ID of the individual who last updated the record.'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'The owner table is like a digital address book for keeping track of the owners of various items or assets. It records essential details about each owner, such as their name, contact information (like address and email), and the type of owner they are (individual, company, etc) The table also includes information about the country and state of the owner, along with a unique serial number for each entry. This table helps in identifying who owns what and how to contact them, making it easier to manage ownership records and communicate with owners when needed.'
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/owner'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.owner_invalid_countries (
# MAGIC   ser_num INT COMMENT 'A unique identifier for a trademark case, known as a serial number, assigned to each trademark owner entry, which helps in identifying and referencing specific records within the dataset.',
# MAGIC   city STRING COMMENT "The name of the city associated with the trademark owner, providing geographical context for the entity's location.",
# MAGIC   ctry_cd STRING COMMENT 'A code representing the country of the trademark owner, useful for categorizing and filtering data by country.',
# MAGIC   postal_cd STRING COMMENT 'The postal code linked to the trademark owner\'s address, which can assist in pinpointing specific locations within a city.',
# MAGIC   ctry_nm STRING COMMENT 'The country name associated with the trademark owner, providing geographical context for the entity\'s location.',
# MAGIC   country_or_area_name STRING COMMENT 'Country or area name associated with the trademark owner, providing geographical context for the entity\'s location.',
# MAGIC   state_cd STRING COMMENT 'A code representing the state or region within the country, aiding in the analysis of data at a more localized level.',
# MAGIC   citizenship STRING COMMENT 'Information regarding the citizenship status of the trademark owner, which can be relevant for understanding legal and demographic aspects.',
# MAGIC   py_ent_num INT COMMENT 'An identifier for the parent entity associated with the trademark owner, useful for tracking relationships between entities.',
# MAGIC   py_entity_type INT COMMENT 'A code indicating the type of the parent entity, which helps in categorizing the nature of the entity involved.',
# MAGIC   py_party_type INT COMMENT 'A code representing the type of party (ex: original applicant, original registrant) associated with the trademark owner, providing insights into the nature of the ownership.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp indicating when the record was created, useful for tracking the history and changes of the data over time.',
# MAGIC   create_user_id STRING COMMENT 'The identifier of the user who created the record, which can be important for accountability and auditing purposes.',
# MAGIC   update_ts TIMESTAMP COMMENT 'The timestamp for the last update made to the record, allowing users to understand the recency of the information.',
# MAGIC   update_user_id STRING COMMENT 'The identifier of the user who last updated the record, providing insight into who is responsible for changes made.'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'The table contains information related to trademark owners with invalid country names in `NOT PROVIDED, NOT IN LIST, OTHER, or STATELESS` and their geographical details. It includes data such as city, country, state, and postal codes, along with entity identifiers and timestamps for creation and updates. This data can be used for analyzing entity distribution across different regions, tracking changes over time, and understanding the demographics of the entities involved.'
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/owner_invalid_countries'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.milestone (
# MAGIC   ser_num INT,
# MAGIC   first_action_dt_ph STRING COMMENT '1st_Action_DT_PH',
# MAGIC   am_1_actn_ct_dt DATE,
# MAGIC   first_action_type STRING COMMENT '1st_Action_Type',
# MAGIC   filing_dt DATE,
# MAGIC   ib_notification_dt DATE,
# MAGIC   published_dt DATE,
# MAGIC   noa_dt DATE,
# MAGIC   abandonment_dt DATE,
# MAGIC   aban_dt_ph DATE,
# MAGIC   registration_dt DATE,
# MAGIC   disposal_type STRING,
# MAGIC   ext1_dt DATE,
# MAGIC   ext2_dt DATE,
# MAGIC   ext3_dt DATE,
# MAGIC   ext4_dt DATE,
# MAGIC   ext5_dt DATE,
# MAGIC   cancellation_dt DATE,
# MAGIC   renewal_dt DATE,
# MAGIC   revival_dt DATE,
# MAGIC   susp_check_dt DATE,
# MAGIC   am_cls_ct_actv BIGINT,
# MAGIC   pendency_cal_start_dt DATE,
# MAGIC   pendency_cal_end_dt DATE,
# MAGIC   noa_registration_check INT COMMENT 'NOA_REGISTRATION Check',
# MAGIC   wgtd_1st_actn_pendency DOUBLE,
# MAGIC   first_action_cd STRING COMMENT '1st_Action_CD',
# MAGIC   disposal_pendency DOUBLE,
# MAGIC   suspension STRING,
# MAGIC   ttab STRING,
# MAGIC   disposal_dt DATE,
# MAGIC   dock_dt DATE,
# MAGIC   am_flg_66a_cur INT,
# MAGIC   am_flg_66a_fil INT,
# MAGIC   noa_dt_ph DATE,
# MAGIC   filing_fy INT,
# MAGIC   non_pro_se STRING COMMENT 'NON/PRO SE',
# MAGIC   first_action_pendency_ph DOUBLE COMMENT '1st Action Pendency_PH',
# MAGIC   last_modified_date TIMESTAMP,
# MAGIC   processing_pend DECIMAL(10,2),
# MAGIC   processing_pend_days INTEGER,
# MAGIC   days_in_dock INT,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/milestone' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.correspondence (
# MAGIC   ser_num INT,
# MAGIC   cor_nm STRING,
# MAGIC   firm_nm STRING,
# MAGIC   add_line1 STRING,
# MAGIC   add_line2 STRING,
# MAGIC   city_nm STRING,
# MAGIC   zipcode STRING,
# MAGIC   state_cd STRING,
# MAGIC   state_nm STRING,
# MAGIC   ctry_cd STRING,
# MAGIC   ctry_nm STRING,
# MAGIC   ctry_name_caps STRING,
# MAGIC   country_or_area_name STRING,
# MAGIC   iso_alpha3_code STRING,
# MAGIC   ip_att_docket_ref STRING,
# MAGIC   atty_nm STRING,
# MAGIC   domestic_rep STRING,
# MAGIC   at_email_auth STRING,
# MAGIC   at_email STRING,
# MAGIC   cr_email1 STRING,
# MAGIC   cr_email2 STRING,
# MAGIC   cr_email3 STRING,
# MAGIC   cr_email4 STRING,
# MAGIC   cr_email_auth STRING,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/correspondence' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.class (
# MAGIC   class_status STRING,
# MAGIC   class STRING,
# MAGIC   ser_num INT,
# MAGIC   cl_cls_us_ct BIGINT,
# MAGIC   cl_cls_us STRING,
# MAGIC   cl_dt_stat DATE,
# MAGIC   cl_flg_anoth_form INT,
# MAGIC   vt_ser_num INT,
# MAGIC   vt_class STRING,
# MAGIC   goods_and_services_desc STRING,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/class' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.bibliography (
# MAGIC   SER_NUM INT,
# MAGIC   TEST_PCTRAM_LINK STRING,
# MAGIC   LAW_OFFICE STRING,
# MAGIC   FILING_BASIS_CUR STRING,
# MAGIC   FILING_METHOD_FILED STRING,
# MAGIC   FILING_METHOD_CUR STRING,
# MAGIC   FILING_BASIS_FIL STRING,
# MAGIC   FILING_BASIS_AMED STRING,
# MAGIC   REGISTRATION_NUMBER STRING,
# MAGIC   AM_FLG_66A_FIL INT,
# MAGIC   AM_FLG_44D_FIL INT,
# MAGIC   AM_FLG_44E_FIL INT,
# MAGIC   FLG_PAPER_FIL INT,
# MAGIC   AM_STAT INT,
# MAGIC   AM_FLG_NO_BAS_FIL INT,
# MAGIC   AM_FLG_TEASRF_FIL INT,
# MAGIC   AM_FLG_USE_FIL INT,
# MAGIC   AM_FLG_ITU_FIL INT,
# MAGIC   AM_FLG_TEASPL_FIL INT,
# MAGIC   LAST_MODIFIED_DATE TIMESTAMP,
# MAGIC   FILING_BASIS_GRP STRING,
# MAGIC   MARK_DWG_CD STRING,
# MAGIC   MARK_DWG_DESC STRING,
# MAGIC   MARK_NM_SHORT STRING,
# MAGIC   MARK_NM STRING,
# MAGIC   TMNG_IMAGE_LINK STRING,
# MAGIC   TM_ANALYTICS_TS TIMESTAMP,
# MAGIC   EXMR_EID INT,
# MAGIC   STATUS_DT TIMESTAMP, -- Added as part of user story US565540
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/bibliography' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.fpep_fact (
# MAGIC CATEGORY STRING,
# MAGIC FK_FP_CATEGORY_ID STRING,
# MAGIC FK_FP_GROUP_ID STRING,
# MAGIC TITLE_TX STRING,
# MAGIC SER_NUM INT,
# MAGIC FP_YEAR INT,
# MAGIC FK_WRKR_ID STRING,
# MAGIC ACTION_COUNT INT,
# MAGIC TRANSACTION_NO INT,
# MAGIC TRANSACTIONAL_LITERAL STRING,
# MAGIC COMPLETED_DT DATE,
# MAGIC GROUP_NAME STRING,
# MAGIC FP_ID STRING,
# MAGIC COMPLETED_TS TIMESTAMP,
# MAGIC TM_ANALYTICS_TS TIMESTAMP
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/fpep_fact' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Second level Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.post_reg_milestone(
# MAGIC   serial_number string,
# MAGIC   registration_dt date,
# MAGIC   six_yr_dt date,
# MAGIC   last_10yr_dt date,
# MAGIC   next_10yr_renewal date,
# MAGIC   number_renewals int,
# MAGIC   next_6yr_dt date,
# MAGIC   expiration_dt date,
# MAGIC   expiration_type string,
# MAGIC   registration_number string,
# MAGIC   am_dt_cncl TIMESTAMP,
# MAGIC   active_classes bigint,
# MAGIC   live_registration int,
# MAGIC   expiration_dt_realtime date,
# MAGIC   expiration_type_realtime string,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/post_reg_milestone' COMMENT 'This table used for post_reg_milestone etl data.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.post_reg_detail(
# MAGIC   serial_number string,
# MAGIC   registration_dt date,
# MAGIC   registration_number string,
# MAGIC   postreg_category string,
# MAGIC   start_action_number int,
# MAGIC   end_action_number int,
# MAGIC   start_action_date date,
# MAGIC   end_action_date date,
# MAGIC   start_5_characters string,
# MAGIC   end_5_characters string,
# MAGIC   start_cm_desc string,
# MAGIC   end_cm_desc string,
# MAGIC   renewal_dt date,
# MAGIC   renewal_number int,
# MAGIC   fifteen_flag boolean,
# MAGIC   inventory boolean,
# MAGIC   first_action_date date,
# MAGIC   first_action_code string,
# MAGIC   first_action_pendency bigint,
# MAGIC   first_action_inventory boolean,
# MAGIC   total_pendency bigint,
# MAGIC   tm_worker_eid string,
# MAGIC   unique_transaction_id string,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/post_reg_detail' COMMENT 'This table used to for post_reg_detail etl data.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.divisionals (
# MAGIC   ser_num STRING COMMENT '',
# MAGIC   filing_dt DATE COMMENT '',
# MAGIC   ib_notification_dt DATE COMMENT '',
# MAGIC   dv_type STRING COMMENT '',
# MAGIC   ref_ser_num STRING COMMENT '',
# MAGIC   dv_dt_rqst STRING COMMENT '',
# MAGIC   dv_dt_complete STRING COMMENT '',
# MAGIC   last_modified_date DATE COMMENT '',
# MAGIC   trans_dt DATE COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/divisionals' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.goods_services_normalization (
# MAGIC   goods_services_desc STRING,
# MAGIC   goods_services_desc_processed STRING,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/goods_services_normalization' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.goods_services_sn_list (
# MAGIC   ser_num integer,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/goods_services_sn_list' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.on_hold (
# MAGIC   ath_ser_num STRING COMMENT '',
# MAGIC   ath_create_dt DATE COMMENT '',
# MAGIC   ath_create_ti INT COMMENT '',
# MAGIC   ath_emp_num INT COMMENT '',
# MAGIC   ath_last_upd_dt DATE COMMENT '',
# MAGIC   ath_last_upd_ti INT COMMENT '',
# MAGIC   ath_last_emp_num INT COMMENT '',
# MAGIC   ath_hold_status INT COMMENT '',
# MAGIC   ath_active_status INT COMMENT '',
# MAGIC   ath_hold_docket INT COMMENT '',
# MAGIC   last_modified_dt DATE COMMENT '',
# MAGIC   oracle_apply_time STRING COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/on_hold' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_detail_summary (
# MAGIC   serial_number STRING COMMENT '',
# MAGIC   ttab_issue_type STRING COMMENT '',
# MAGIC   proceeding_num STRING COMMENT '',
# MAGIC   filing_date DATE COMMENT '',
# MAGIC   instituted_date DATE COMMENT '',
# MAGIC   instituted_code STRING COMMENT '',
# MAGIC   decision_date DATE COMMENT '',
# MAGIC   decision_code STRING COMMENT '',
# MAGIC   decision_description STRING COMMENT '',
# MAGIC   termination_code STRING COMMENT '',
# MAGIC   termination_date DATE COMMENT '',
# MAGIC   termination_date_2 DATE COMMENT '',
# MAGIC   termination_date_3 DATE COMMENT '',
# MAGIC   termination_date_4 DATE COMMENT '',
# MAGIC   termination_date_5 DATE COMMENT '',
# MAGIC   final_refusal_date DATE COMMENT '',
# MAGIC   fp_reason_1 STRING COMMENT '',
# MAGIC   fp_reason_2 STRING COMMENT '',
# MAGIC   fp_reason_3 STRING COMMENT '',
# MAGIC   fp_reason_4 STRING COMMENT '',
# MAGIC   fp_reason_5 STRING COMMENT '',
# MAGIC   appeal BOOLEAN COMMENT '',
# MAGIC   inventory BOOLEAN COMMENT '',
# MAGIC   pendency_d BIGINT COMMENT '',
# MAGIC   pendency_t BIGINT COMMENT '',
# MAGIC   --pendency_r BIGINT COMMENT '',
# MAGIC   publication_date DATE COMMENT '',
# MAGIC   constructed_prcd_num STRING COMMENT '',
# MAGIC   opposition BOOLEAN COMMENT '',
# MAGIC   default_date DATE COMMENT '',
# MAGIC   default_opposition BOOLEAN COMMENT '',
# MAGIC   cancellation BOOLEAN COMMENT '',
# MAGIC   default_cancellation BOOLEAN COMMENT '',
# MAGIC   concurrent BOOLEAN COMMENT '',
# MAGIC   rfd_date DATE COMMENT '',
# MAGIC   rfd_valid BOOLEAN COMMENT '',
# MAGIC   proceeding_count INT COMMENT '',
# MAGIC   non_pro_se STRING COMMENT '',
# MAGIC   pctram_link STRING COMMENT '',
# MAGIC   law_office STRING COMMENT '',
# MAGIC   filing_basis_grp STRING COMMENT '',
# MAGIC   filing_method_cur STRING COMMENT '',
# MAGIC   am_stat INT COMMENT '',
# MAGIC   owner_name STRING COMMENT '',
# MAGIC   city STRING COMMENT '',
# MAGIC   state STRING COMMENT '',
# MAGIC   country_or_area_name STRING COMMENT '',
# MAGIC   reg_class_count BIGINT COMMENT '',
# MAGIC   active_class_count BIGINT COMMENT '',
# MAGIC   group_type STRING COMMENT '',
# MAGIC   concat_class STRING COMMENT '',
# MAGIC   mark_nm_short STRING COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/ttab_detail_summary' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.tqr_detail_metrics (
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
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC )USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/tqr_detail_metrics' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS ${conf.catalog}.silver.vw_law_offices 
# MAGIC COMMENT 'This view provides standardized law office location names.'
# MAGIC AS (
# MAGIC   select law_office_cd, 
# MAGIC   palm_short_cd, 
# MAGIC   regexp_substr(tt_text, r'LAW OFFICE \d{3}') as law_office_nm, 
# MAGIC   regexp_substr(palm_short_cd, r'\d+') as law_office_num,
# MAGIC   substr(law_office_cd, 1, 2) as law_office_cd_short
# MAGIC   from ${conf.tmngpb_catalog}.bronze.sync_translate_location
# MAGIC   where (palm_short_cd like 'LO1%' or palm_short_cd like 'LO3%') and len(palm_short_cd) < 6
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Count Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.filings_counts (
# MAGIC   record_output_date date,
# MAGIC   output_record_count integer,
# MAGIC   record_output_percent_change double,
# MAGIC   continue_process integer,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/filings_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.fixed_class_counts (
# MAGIC   ser_num STRING COMMENT '',
# MAGIC   class_count BIGINT COMMENT '',
# MAGIC   date_stamp TIMESTAMP COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC )USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/fixed_class_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.form_paragraph_counts (
# MAGIC   record_output_date DATE COMMENT '',
# MAGIC   output_record_count BIGINT COMMENT '',
# MAGIC   record_output_percent_change DOUBLE COMMENT '',
# MAGIC   continue_process INT COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/form_paragraph_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.pendency_counts (
# MAGIC   record_output_date DATE COMMENT '',
# MAGIC   record_output_count BIGINT COMMENT '',
# MAGIC   continue_process INT COMMENT '',
# MAGIC   record_output_percent_change DOUBLE COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/pendency_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.pr_detail_counts (
# MAGIC   record_output_date DATE COMMENT '',
# MAGIC   output_record_count BIGINT COMMENT '',
# MAGIC   record_output_percent_change DOUBLE COMMENT '',
# MAGIC   continue_process INT COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/pr_detail_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.pr_milestone_counts (
# MAGIC   record_output_date DATE COMMENT '',
# MAGIC   output_record_count BIGINT COMMENT '',
# MAGIC   record_output_percent_change DOUBLE COMMENT '',
# MAGIC   continue_process INT COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/pr_milestone_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.quality_counts (
# MAGIC   record_output_date DATE COMMENT '',
# MAGIC   output_record_count BIGINT COMMENT '',
# MAGIC   record_output_percent_change DOUBLE COMMENT '',
# MAGIC   continue_process INT COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/quality_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_detail_counts (
# MAGIC   record_output_date DATE COMMENT '',
# MAGIC   output_record_count BIGINT COMMENT '',
# MAGIC   record_output_percent_change DOUBLE COMMENT '',
# MAGIC   continue_process INT COMMENT '',
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/ttab_detail_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.tqr_detail_metrics_counts (
# MAGIC   min_lastreviewdatetime TIMESTAMP,
# MAGIC   max_lastreviewdatetime TIMESTAMP,
# MAGIC   record_ct INT,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   update_ts TIMESTAMP,
# MAGIC   update_user_id STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/tqr_detail_metrics_counts' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### TTAB Staging Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.stg_ttab_input_ph (
# MAGIC   serial_number integer,
# MAGIC   ph_action_number integer,
# MAGIC   ph_action_code string,
# MAGIC   cm_sys_dt date,
# MAGIC   ph_action_date date,
# MAGIC   last_modified_date timestamp,
# MAGIC   oracle_apply_time timestamp,
# MAGIC   cm_prcd_num string,
# MAGIC   ri_notif_dt date,
# MAGIC   cm_desc string,
# MAGIC   fifth_char_cm_type string,
# MAGIC   cm_flg_paper integer,
# MAGIC   ttab_tracking_num string,
# MAGIC   tm_worker_eid string,
# MAGIC   five_characters string,
# MAGIC   year integer
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/stg_ttab_input_ph' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.stg_ttab_input_cde (
# MAGIC   SER_NUM integer,
# MAGIC   Pendency_Cal_Start_DT date,
# MAGIC   NON_PRO_SE string,
# MAGIC   TEST_PCTRAM_LINK string,
# MAGIC   LAW_OFFICE string,
# MAGIC   FILING_BASIS_GRP string,
# MAGIC   FILING_METHOD_CUR string,
# MAGIC   AM_STAT integer,
# MAGIC   Owner_Name string,
# MAGIC   CITY string,
# MAGIC   STATE string,
# MAGIC   Country_or_Area_Name string,
# MAGIC   Reg_Class_Count long,
# MAGIC   Active_Class_Count long,
# MAGIC   Group_Type string,
# MAGIC   Concat_Class string,
# MAGIC   MARK_NM_SHORT string
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/stg_ttab_input_cde' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_detail_appeals (
# MAGIC   serial_number INTEGER,
# MAGIC   TTAB_ISSUE_TYPE STRING,
# MAGIC   PROCEEDING_NUM STRING,
# MAGIC   FINAL_REFUSAL_DATE DATE,
# MAGIC   FILING_DATE DATE,
# MAGIC   INSTITUTED_CODE STRING,
# MAGIC   INSTITUTED_DATE DATE,
# MAGIC   DECISION_DATE DATE,
# MAGIC   DECISION_CODE STRING,
# MAGIC   DECISION_DESCRIPTION STRING,
# MAGIC   TERMINATION_CODE STRING,
# MAGIC   FP_REASON_1 STRING,
# MAGIC   FP_REASON_2 STRING,
# MAGIC   FP_REASON_3 STRING,
# MAGIC   FP_REASON_4 STRING,
# MAGIC   FP_REASON_5 STRING,
# MAGIC   TERMINATION_DATE STRING,
# MAGIC   TERMINATION_DATE_2 STRING,
# MAGIC   TERMINATION_DATE_3 STRING,
# MAGIC   TERMINATION_DATE_4 STRING,
# MAGIC   TERMINATION_DATE_5 STRING,
# MAGIC   APPEAL INTEGER,
# MAGIC   INVENTORY BOOLEAN,
# MAGIC   PENDENCY_D INTEGER,
# MAGIC   PENDENCY_T INTEGER,
# MAGIC   PENDENCY_R INTEGER,
# MAGIC   PUBLICATION_DATE DATE
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/ttab_detail_appeals' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_detail_appeals_1 (
# MAGIC   serial_number INTEGER,
# MAGIC   TTAB_ISSUE_TYPE STRING,
# MAGIC   PROCEEDING_NUM STRING,
# MAGIC   FINAL_REFUSAL_DATE DATE,
# MAGIC   FILING_DATE DATE,
# MAGIC   INSTITUTED_CODE STRING,
# MAGIC   INSTITUTED_DATE DATE,
# MAGIC   DECISION_DATE DATE,
# MAGIC   DECISION_CODE STRING,
# MAGIC   DECISION_DESCRIPTION STRING,
# MAGIC   TERMINATION_CODE STRING,
# MAGIC   FP_REASON_1 STRING,
# MAGIC   FP_REASON_2 STRING,
# MAGIC   FP_REASON_3 STRING,
# MAGIC   FP_REASON_4 STRING,
# MAGIC   FP_REASON_5 STRING,
# MAGIC   TERMINATION_DATE STRING,
# MAGIC   TERMINATION_DATE_2 STRING,
# MAGIC   TERMINATION_DATE_3 STRING,
# MAGIC   TERMINATION_DATE_4 STRING,
# MAGIC   TERMINATION_DATE_5 STRING,
# MAGIC   APPEAL INTEGER,
# MAGIC   INVENTORY BOOLEAN,
# MAGIC   PENDENCY_D INTEGER,
# MAGIC   PENDENCY_T INTEGER,
# MAGIC   PUBLICATION_DATE DATE,
# MAGIC   REFUSAL BOOLEAN,
# MAGIC   NON_PRO_SE STRING,
# MAGIC   TEST_PCTRAM_LINK STRING,
# MAGIC   LAW_OFFICE STRING,
# MAGIC   FILING_BASIS_GRP STRING,
# MAGIC   FILING_METHOD_CUR STRING,
# MAGIC   AM_STAT INTEGER,
# MAGIC   Owner_Name STRING,
# MAGIC   CITY STRING,
# MAGIC   STATE STRING,
# MAGIC   Country_or_Area_Name STRING,
# MAGIC   Reg_Class_Count LONG,
# MAGIC   Active_Class_Count LONG,
# MAGIC   Group_Type STRING,
# MAGIC   Concat_Class STRING,
# MAGIC   MARK_NM_SHORT STRING
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/ttab_detail_appeals_1' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_detail_cancellations (
# MAGIC   SERIAL_NUMBER INTEGER,
# MAGIC   TTAB_ISSUE_TYPE STRING,
# MAGIC   PROCEEDING_NUM STRING,
# MAGIC   FILING_DATE DATE,
# MAGIC   INSTITUTED_DATE DATE,
# MAGIC   INSTITUTED_CODE STRING,
# MAGIC   DECISION_DATE DATE,
# MAGIC   DECISION_CODE STRING,
# MAGIC   DECISION_DESCRIPTION STRING,
# MAGIC   TERMINATION_CODE STRING,
# MAGIC   TERMINATION_DATE DATE,
# MAGIC   TERMINATION_DATE_2 STRING,
# MAGIC   TERMINATION_DATE_3 STRING,
# MAGIC   TERMINATION_DATE_4 STRING,
# MAGIC   TERMINATION_DATE_5 STRING,
# MAGIC   CONSTRUCTED_PRCD_NUM STRING,
# MAGIC   CANCELLATION BOOLEAN,
# MAGIC   INVENTORY BOOLEAN,
# MAGIC   DEFAULT_DATE DATE,
# MAGIC   DEFAULT_CANCELLATION BOOLEAN
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/ttab_detail_cancellations' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_detail_oppositions (
# MAGIC   SERIAL_NUMBER INTEGER,
# MAGIC   TTAB_ISSUE_TYPE STRING,
# MAGIC   PROCEEDING_NUM STRING,
# MAGIC   FILING_DATE DATE,
# MAGIC   INSTITUTED_DATE DATE,
# MAGIC   INSTITUTED_CODE STRING,
# MAGIC   DECISION_DATE DATE,
# MAGIC   DECISION_CODE STRING,
# MAGIC   DECISION_DESCRIPTION STRING,
# MAGIC   TERMINATION_CODE STRING,
# MAGIC   TERMINATION_DATE DATE,
# MAGIC   TERMINATION_DATE_2 DATE,
# MAGIC   TERMINATION_DATE_3 DATE,
# MAGIC   TERMINATION_DATE_4 DATE,
# MAGIC   TERMINATION_DATE_5 DATE,
# MAGIC   CONSTRUCTED_PRCD_NUM STRING,
# MAGIC   OPPOSITION BOOLEAN,
# MAGIC   INVENTORY BOOLEAN,
# MAGIC   DEFAULT_DATE DATE,
# MAGIC   DEFAULT_OPPOSITION BOOLEAN
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/ttab_detail_oppositions' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_detail_concurrent_filings (
# MAGIC   SERIAL_NUMBER INTEGER,
# MAGIC   TTAB_ISSUE_TYPE STRING,
# MAGIC   INSTITUTED_DATE DATE,
# MAGIC   INSTITUTED_CODE STRING,
# MAGIC   PROCEEDING_NUM STRING,
# MAGIC   DECISION_DATE DATE,
# MAGIC   DECISION_CODE STRING,
# MAGIC   DECISION_DESCRIPTION STRING,
# MAGIC   TERMINATION_CODE STRING,
# MAGIC   TERMINATION_DATE STRING,
# MAGIC   TERMINATION_DATE_2 STRING,
# MAGIC   TERMINATION_DATE_3 STRING,
# MAGIC   TERMINATION_DATE_4 STRING,
# MAGIC   TERMINATION_DATE_5 STRING,
# MAGIC   FILING_DATE DATE,
# MAGIC   FILED_YR INTEGER,
# MAGIC   INST_YR INTEGER,
# MAGIC   TERM_YR INTEGER,
# MAGIC   DECISION_YR INTEGER,
# MAGIC   PENDENCY_D INTEGER,
# MAGIC   PENDENCY_T INTEGER,
# MAGIC   CONCURRENT BOOLEAN,
# MAGIC   INVENTORY BOOLEAN
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/ttab_detail_concurrent_filings' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## TM Reports Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.proof_of_use_audit (
    serial_number STRING COMMENT 'Unique identifier assigned to the trademark registration.',
    registration_number STRING COMMENT 'Unique identifier assigned to each registration.',
    em_empe_name STRING COMMENT 'The name of the employee or representative associated with the trademark case.',
    first_audit_office_action_dt DATE COMMENT 'The date of the first office action during the audit.',
    second_audit_office_action_dt DATE COMMENT 'The date of the second office action during the audit.',
    third_audit_office_action_dt DATE COMMENT 'The date of the third office action during the audit.',
    audit_interim_office_action_dt DATE COMMENT 'The date when no response was received for the office action during the audit process.',
    audit_no_response_office_action_dt DATE COMMENT 'The date when no response was received for the office action during the audit process.',
    response_oa_rec_in BOOLEAN COMMENT 'Indicates whether a response to the office action was received.',
    deletions_after_audit_in BOOLEAN COMMENT 'Indicates if there were any deletions made after the audit.',
    deletions_after_audit_count DECIMAL(25,0) COMMENT 'The count of deletions that occurred following the audit.',
    deletion_event_count INT COMMENT 'The count of deletion events for this registration.',
    deletion_event_dates ARRAY<TIMESTAMP> COMMENT 'The deletion date.' ,
    all_deletion_dates ARRAY<TIMESTAMP> COMMENT 'Array of all deletion dates for registrations with multiple deletion events.',
    first_deletion_dt TIMESTAMP  COMMENT 'The earliest deletion date.',
    latest_deletion_dt TIMESTAMP COMMENT'The most recent deletion date.',
    cancellation_in BOOLEAN COMMENT 'Indicates whether the trademark was canceled.',
    acceptflag_noPUM1 BOOLEAN COMMENT 'count of acceptances before we starting using PUM1 PH action' ,
    owner_name STRING COMMENT 'The owner_name column stores the name of the entity that owns the record.',
    attorney_name STRING COMMENT 'Attorney name',
    firm_name STRING COMMENT 'Firm name',
    filing_basis_cur STRING COMMENT 'The current basis for filing the trademark.',
    country_or_area_name STRING COMMENT 'The country or region name of the trademarks owner.',
    reg_classes STRING COMMENT 'Stores concatenated class information for easy reference.',
    review_fy STRING COMMENT 'The fiscal year during which the review took place.',
    review_fy_quarter STRING COMMENT 'The quarter of the fiscal year when the review occurred.',
    review_month STRING COMMENT 'The month in which the review was conducted.',
    review_month_int INT COMMENT 'The integer representation of the review month.',
    termination_dt DATE COMMENT 'The date when the trademark was terminated.',
    create_ts TIMESTAMP COMMENT 'The timestamp indicating when the record was created.',
    create_user_id STRING COMMENT 'The identifier of the user who created the record.',
    update_ts TIMESTAMP COMMENT 'The timestamp for the last update made to the record.',
    update_user_id STRING COMMENT 'The identifier of the user who last updated the record.'
)USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/proof_of_use_audit' TBLPROPERTIES (
  'databricks.delta.autocompact.enabled' = true,
  'delta.enableChangeDataFeed' = true
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.proof_of_use_audit (
# MAGIC   serial_number STRING COMMENT 'The unique identifier for a Trademark case.',
# MAGIC   registration_number STRING COMMENT 'The official number assigned to a Trademark registration.',
# MAGIC   em_empe_name STRING COMMENT 'The name of the employee or representative associated with the trademark case.',
# MAGIC   first_audit_office_action_dt DATE COMMENT 'The date of the first office action during the audit.',
# MAGIC   second_audit_office_action_dt DATE COMMENT 'The date of the second office action during the audit.',
# MAGIC   third_audit_office_action_dt DATE COMMENT 'The date of the third office action during the audit.',
# MAGIC   audit_interim_office_action_dt DATE COMMENT 'The date of any interim office actions taken during the audit process.',
# MAGIC   audit_no_response_office_action_dt DATE COMMENT 'The date when no response was received for the office action during the audit process.',
# MAGIC   response_oa_rec_in BOOLEAN COMMENT 'Indicates whether a response to the office action was received.',
# MAGIC   deletions_after_audit_in BOOLEAN COMMENT 'Indicates if there were any deletions made after the audit.',
# MAGIC   deletions_after_audit_count INT COMMENT 'The count of deletions that occurred following the audit.',
# MAGIC   deletion_event_count INT COMMENT 'The count of deletion events for this registration.',
# MAGIC   all_deletion_dates ARRAY<DATE> COMMENT 'Array of all deletion dates for registrations with multiple deletion events.',
# MAGIC   first_deletion_dt DATE COMMENT 'The earliest deletion date.',
# MAGIC   latest_deletion_dt DATE COMMENT 'The most recent deletion date.',
# MAGIC   cancellation_in BOOLEAN COMMENT 'Indicates whether the trademark was canceled.',
# MAGIC   owner_name STRING COMMENT 'The name of the trademark owner.',
# MAGIC   attorney_name STRING COMMENT 'The name of the attorney representing the trademark owner.',
# MAGIC   firm_name STRING COMMENT 'The name of the law firm associated with the trademark case.',
# MAGIC   filing_basis_cur STRING COMMENT 'The current basis for filing the trademark.',
# MAGIC   country_or_area_name STRING COMMENT 'The name of the country or area where the trademark is registered.',
# MAGIC   reg_classes STRING COMMENT 'The registration classes associated with the trademark.',
# MAGIC   review_fy STRING COMMENT 'The fiscal year during which the review took place.',
# MAGIC   review_fy_quarter STRING COMMENT 'The quarter of the fiscal year when the review occurred.',
# MAGIC   review_month STRING COMMENT 'The month in which the review was conducted.',
# MAGIC   review_month_int INT COMMENT 'The integer representation of the review month.',
# MAGIC   termination_dt DATE COMMENT 'The date when the trademark was terminated.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp indicating when the record was created.',
# MAGIC   create_user_id STRING COMMENT 'The identifier of the user who created the record.',
# MAGIC   update_ts TIMESTAMP COMMENT 'The timestamp for the last update made to the record.',
# MAGIC   update_user_id STRING COMMENT 'The identifier of the user who last updated the record.'
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/proof_of_use_audit'
# MAGIC COMMENT 'The table contains data related to the Proof of Use Audit Program, which promotes the accuracy and integrity of the trademark register. It includes information such as registration numbers, audit dates, and actions taken during the audit process. This data can be used to track the status of registrations, analyze audit outcomes, and assess the performance of different stakeholders involved in the registration process.'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# DBTITLE 1,Currently Processing First Actions With Controls
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.currently_processing_first_actions_with_controls (
# MAGIC   min_record_id BIGINT,
# MAGIC   record_id BIGINT,
# MAGIC   pendency_cal_start_dt DATE,
# MAGIC   cases INT,
# MAGIC   sum_cases INT,
# MAGIC   percent_total DECIMAL(10, 2),
# MAGIC   test BOOLEAN,
# MAGIC   date_plus_two DATE,
# MAGIC   today DATE,
# MAGIC   current_process_pendency DECIMAL(10, 2),
# MAGIC   datetime_out1 STRING,
# MAGIC   datetime_out2 STRING,
# MAGIC   text STRING,
# MAGIC   todays_date STRING,
# MAGIC   email_sub STRING
# MAGIC ) USING delta
# MAGIC COMMENT 'Table for storing the previous day\'s results for the First Actions With Controls pipeline' LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/currently_processing_first_actions_with_controls'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.employee_grade(
# MAGIC   emp_no string COMMENT 'Employee ID number.',
# MAGIC   employee_name_full string COMMENT 'Employee full name in format Last, First MI.',
# MAGIC   grade string COMMENT 'Employee grade level.',
# MAGIC   org_cd string COMMENT "Employee's organization ID code.",
# MAGIC   org_nm string COMMENT "Employee's organization full name.",
# MAGIC   pay_period_clndr_yr string COMMENT "Pay period calendar year.",
# MAGIC   grade_start_dt string COMMENT "Starting date of when employee reached current grade level.",
# MAGIC   grade_end_dt string COMMENT "Ending date of when employee reached current grade level.",
# MAGIC   create_ts TIMESTAMP COMMENT "Auto generated create timestamp for this record.",
# MAGIC   create_user_id STRING COMMENT "Auto generated ID of user to create this record.",
# MAGIC   update_ts TIMESTAMP COMMENT "Auto generated update timestamp for this record.",
# MAGIC   update_user_id STRING COMMENT "Auto generated ID of user to last update this record."
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/employee_grade' 
# MAGIC COMMENT 'This table is a mirror of the EDW EMP_GRADE table.'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## NICE - MGS Report tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.df_acceptance_table (
# MAGIC   CPT STRING,
# MAGIC   CLS STRING,
# MAGIC   ID STRING,
# MAGIC   NCL STRING,
# MAGIC   TM5 STRING,
# MAGIC   USPTO STRING,
# MAGIC   TMCLASS STRING,
# MAGIC   NCL_BASIC STRING,
# MAGIC   PREF STRING,
# MAGIC   TERM STRING,
# MAGIC   ACC_US STRING)
# MAGIC USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/df_acceptance_table' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.df_active_table (
# MAGIC   record_id STRING,
# MAGIC   Term_ID STRING,
# MAGIC   Class STRING,
# MAGIC   Description STRING,
# MAGIC   Status STRING,
# MAGIC   Type STRING,
# MAGIC   Notes STRING,
# MAGIC   Employee_Notes STRING,
# MAGIC   Editor_Notes STRING,
# MAGIC   Stage STRING,
# MAGIC   TM5 STRING,
# MAGIC   NCL_Version STRING,
# MAGIC   Start_Effective_Date DATE,
# MAGIC   End_Effective_Date DATE,
# MAGIC   Fiscal_year INT)
# MAGIC USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/df_active_table' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,G&S Incorrect Classification
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.goods_and_services_incorrect_classification (
# MAGIC   ser_num STRING COMMENT 'The serial number uniquely identifies each trademark case entry in the table, allowing for easy reference and tracking of specific goods and services.',
# MAGIC   goods_and_services_desc STRING COMMENT 'This field contains detailed descriptions of the goods and services, providing context and information necessary for classification and analysis.',
# MAGIC   filed_class STRING COMMENT 'Represents the categories, referred to as classes, under which the goods and services have been filed, aiding in the organization and retrieval of related data.',
# MAGIC   idm_acceptable_classes STRING COMMENT 'Lists the acceptable classes for ID management, ensuring that the goods and services comply with established standards and regulations.',
# MAGIC   run_date DATE COMMENT 'Indicates the date when the data was recorded, which is essential for tracking changes and maintaining an accurate historical record.'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'The table contains information related to goods and services, including their descriptions and classifications. It records the serial number, the description of goods and services, the filed class, acceptable classes for ID management, and the date the data was recorded. This data can be used for tracking and analyzing the classification of goods and services, ensuring compliance with ID management standards, and monitoring changes over time.'
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/goods_and_services_incorrect_classification'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# DBTITLE 1,Fee Checker Historical
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  ${conf.catalog}.silver.preexam_fee_checker_historical (
# MAGIC   ser_num integer,
# MAGIC   fees_paid integer,
# MAGIC   tram_classes integer,
# MAGIC   `delta` integer,
# MAGIC   tram_status string,
# MAGIC   discrepancy_type string,
# MAGIC   days_on_report integer,
# MAGIC   first_report_date date,
# MAGIC   first_time_on_report boolean,
# MAGIC   effective_ts timestamp,
# MAGIC   begin_effective_ts timestamp,
# MAGIC   end_effective_ts timestamp
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/preexam_fee_checker_historical'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# DBTITLE 1,MyUSPTO User Alert List
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.myuspto_monitor_watchlist (
# MAGIC   patron_id string,
# MAGIC   send_alert string,
# MAGIC   is_valid string,
# MAGIC   create_user string,
# MAGIC   create_timestamp timestamp
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/myuspto_monitor_watchlist'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.cmra_request_status (
# MAGIC   serial_number STRING NOT NULL
# MAGIC     COMMENT 'Unique serial number / identifier for the CMRA address record.',
# MAGIC   input_id STRING COMMENT 'The input ID provided to the API. This is identical to the serial number and used as part of the ETL process to capture delta records between partial failures',
# MAGIC   input_street STRING COMMENT 'The input street provided to the API call as part of the payload',
# MAGIC   input_city STRING COMMENT 'The input city provided to the API call as part of the payload',
# MAGIC   input_zipcode STRING COMMENT 'The input zipcode provided to the API call as part of the payload',
# MAGIC   input_state STRING COMMENT 'The input state provided to the API call as part of the payload',
# MAGIC   payload STRING COMMENT 'The payload associated with the initial API call',
# MAGIC   attempt_count INT NOT NULL DEFAULT 0 COMMENT 'The number of attempts made to the API',
# MAGIC   max_attempts INT NOT NULL
# MAGIC     DEFAULT 3
# MAGIC     COMMENT 'The max number of allowed to be made to the API for this address',
# MAGIC   status STRING COMMENT 'The state of the most recent API call (failed | completed | initialized | processing)',
# MAGIC   http_status_code INT COMMENT 'The status code returned upon the API request',
# MAGIC   error_message STRING COMMENT 'The error message associated with the API call',
# MAGIC   response STRING COMMENT 'The response returned from the API call',
# MAGIC   create_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'Timestamp when the record was first created in the silver layer',
# MAGIC   create_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that created the record',
# MAGIC   update_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'User or process that last updated the record',
# MAGIC   update_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that last updated the record',
# MAGIC   CONSTRAINT `cmra_request_status_pk` PRIMARY KEY (`serial_number`)
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/cmra_request_status'
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
# MAGIC CREATE TABLE ${conf.catalog}.silver.cmra_case_status (
# MAGIC   serial_number STRING NOT NULL
# MAGIC     COMMENT 'Unique serial number / identifier for the CMRA address record.',
# MAGIC   cmra_status STRING COMMENT "The status of whether an address (for the initial owner at the time of application) has been identified as a Commercial Mail Receiving Agency. 'Y' = True, 'N' = False, and null indicates that USPS has not yet confirmed the status",
# MAGIC   create_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'Timestamp when the record was first created in the silver layer',
# MAGIC   create_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that created the record',
# MAGIC   update_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'User or process that last updated the record',
# MAGIC   update_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that last updated the record',
# MAGIC   CONSTRAINT `cmra_case_status_pk` PRIMARY KEY (`serial_number`)
# MAGIC ) USING delta
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/cmra_case_status'
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

# DBTITLE 1,Unsupervised Learning Load History
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.unsupervised_anomalies_feature_load_history (
# MAGIC   load_date DATE COMMENT 'A date associated with the attempted load of the ETL.',
# MAGIC   latest BOOLEAN COMMENT 'A flag associated with the attempted load of the ETL that indicates whether the records in the batch are the latest batch of the complete table history.',
# MAGIC   cfk_patron_id STRING COMMENT 'The account ID associated with the engineered features. This is identical to the GUID associated with a MyUSPTO account.',
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
# MAGIC   create_user STRING COMMENT 'The system or individual responsible for creating the record.')
# MAGIC USING delta
# MAGIC PARTITIONED BY (load_date)
# MAGIC COMMENT 'The base feature store table that contains the complete load history of the feature loading ETL for unsupervised learning.'
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/unsupervised_anomalies_feature_load_history'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ${conf.catalog}.silver.unsupervised_anomalies_feature_load_history (
# MAGIC   load_date date comment 'A date associated with the attempted load of the ETL.',
# MAGIC   latest boolean
# MAGIC     comment 'A flag associated with the attempted load of the ETL that indicates whether the records in the batch are the latest batch of the complete table history.',
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
# MAGIC     comment 'Flag for submissions every 6 hours for at least 24 hours'
# MAGIC ) using delta
# MAGIC partitioned by (load_date)
# MAGIC comment 'The base feature store table that contains the complete load history of the feature loading ETL for unsupervised learning.'
# MAGIC location
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/unsupervised_anomalies_feature_load_history'
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

# DBTITLE 1,CMRA DPV Footnotes
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.stnd_smarty_streets_dpv_footnote_code (
# MAGIC   dpv_code STRING COMMENT "The code returned by Smarty Streets associated with a reponse in the dpv_footnotes section.",
# MAGIC   dpv_code_description STRING COMMENT "The detailed description of the code explaining the status of the ",
# MAGIC   create_user STRING COMMENT "The user or system responsible for inserting the record into the table.",
# MAGIC   create_timestamp TIMESTAMP COMMENT "The timestamp the record was inserted into the table."
# MAGIC ) USING delta
# MAGIC COMMENT "
# MAGIC Information related to the delivery point validation of this address. All these footnotes have a length of 2 characters, and there may be up to 14 footnotes.
# MAGIC
# MAGIC Here are some common combinations:
# MAGIC AABB - ZIP, state, city, street name, and primary number match.
# MAGIC AABBCC - ZIP, state, city, street name, and primary number match, but secondary does not. A secondary is not required for delivery.
# MAGIC AAC1 - ZIP, state, city, street name, and primary number match, but secondary does not. A secondary is required for delivery.
# MAGIC AAM1 - ZIP, state, city, and street name match, but the primary number is missing.
# MAGIC AAM3 - ZIP, state, city, and street name match, but the primary number is invalid.
# MAGIC AAN1 - ZIP, state, city, street name, and primary number match, but there is secondary information such as apartment or suite that would be helpful.
# MAGIC AABBR1 - ZIP, state, city, street name, and primary number match. Address confirmed without private mailbox (PMB) info.
# MAGIC "
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/stnd_smarty_streets_dpv_footnote_code'
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

# DBTITLE 1,CMRA Footnotes
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.stnd_smarty_streets_footnote_code (
# MAGIC   footnote_code STRING COMMENT 'The code returned by Smarty Streets associated with a reponse in the footnote section.',
# MAGIC   footnote_code_description STRING COMMENT 'The description of the code explaining the changes or explaination returned from the match.',
# MAGIC   footnote_code_description_verbose STRING COMMENT 'The description of the code explaining the changes returned from the match.',
# MAGIC   create_user STRING COMMENT 'The user or system responsible for inserting the record into the table.',
# MAGIC   create_timestamp TIMESTAMP COMMENT 'The timestamp the record was inserted into the table.')
# MAGIC USING delta
# MAGIC COMMENT 'Indicates which changes were made to the input address upon calling the SmartyStreets API. Footnotes are delimited by a # character.'
# MAGIC LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/stnd_smarty_streets_footnote_code'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpoint.writeStatsAsJson' = 'false',
# MAGIC   'delta.checkpoint.writeStatsAsStruct' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.identityColumns' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.minReaderVersion' = '3',
# MAGIC   'delta.minWriterVersion' = '7')

# COMMAND ----------

# DBTITLE 1,CMRA DPV Match
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.stnd_smarty_streets_dpv_match_code (
# MAGIC   dpv_match_code STRING COMMENT 'The code returned by Smarty Streets associated with a reponse in the dpv_match section.',
# MAGIC   dpv_match_code_description STRING COMMENT 'The description of the code explaining the status of the match.',
# MAGIC   dpv_match_code_description_verbose STRING COMMENT 'The detailed description of the code explaining the status of the match.',
# MAGIC   create_user STRING COMMENT 'The user or system responsible for inserting the record into the table.',
# MAGIC   create_timestamp TIMESTAMP COMMENT 'The timestamp the record was inserted into the table.'
# MAGIC ) USING delta
# MAGIC COMMENT '
# MAGIC Status of the Delivery Point Validation (DPV). This indicates whether or not the address is present in the USPS data.
# MAGIC
# MAGIC - Y — Confirmed; entire address is present in the USPS data. (To be certain the address is actually deliverable, verify that the dpv_vacant field has a value of N. You may also want to verify that the dpv_no_stat field has a value of N. However, the USPS is often several months behind in updating this data point, so only rely on the dpv_no_stat data if you are fully aware of its weaknesses and limitations.)
# MAGIC (e.g., 1600 Amphitheatre Pkwy Mountain View, CA)
# MAGIC - N — Not confirmed; address is not present in the USPS data.
# MAGIC - S — Confirmed by ignoring secondary info; the main address is present in the USPS data, but the submitted secondary information (apartment, suite, etc.) was not recognized.
# MAGIC (e.g., 62 Ea Darden Dr Apt 298 Anniston, AL)
# MAGIC - D — Confirmed but missing secondary info; the main address is present in the USPS data, but it is missing secondary information (apartment, suite, etc.).
# MAGIC (e.g., 122 Mast Rd Lee, NH)
# MAGIC - [blank or null] — The address is not present in the USPS database.
# MAGIC '
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/stnd_smarty_streets_dpv_match_code'
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

# DBTITLE 1,CMRA DPV CMRA
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.stnd_smarty_streets_dpv_cmra_code (
# MAGIC   dpv_cmra_code STRING COMMENT 'The code returned by Smarty Streets that determines whether the associated address is valid CMRA',
# MAGIC   dpv_cmra_code_description STRING COMMENT 'The description of the code explaining the status of the match.',
# MAGIC   create_user STRING COMMENT 'The user or system responsible for inserting the record into the table.',
# MAGIC   create_timestamp TIMESTAMP COMMENT 'The timestamp the record was inserted into the table.'
# MAGIC ) USING delta
# MAGIC COMMENT '
# MAGIC Indicates whether the address is associated with a Commercial Mail Receiving Agency (CMRA), also known as a private mailbox (PMB) operator. A CMRA is a business through which USPS mail may be sent or received, for example the UPS Store and Mailboxes Etc.
# MAGIC Blank entries signify that the address was not submitted for CMRA verification.
# MAGIC '
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/stnd_smarty_streets_dpv_cmra_code'
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
# MAGIC );

# COMMAND ----------

# DBTITLE 1,CMRA SmartyStreets Validation
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.smarty_streets_address_validation (
# MAGIC   serial_number STRING NOT NULL
# MAGIC     COMMENT 'Unique serial number / identifier for the CMRA address record.',
# MAGIC   dpv_match_code STRING COMMENT 'The code returned by Smarty Streets associated with a reponse in the dpv_match section.',
# MAGIC   dpv_match_code_description STRING COMMENT 'The description of the `dpv_match_code` explaining the status of the match.',
# MAGIC   full_dpv_footnote STRING COMMENT 'The code returned by Smarty Streets associated with a reponse in the footnote section.',
# MAGIC   combined_dpv_footnote_code_description STRING COMMENT 'The concatenated description returned by Smarty Streets associated with a reponse in the DPV footnote section.',
# MAGIC   full_footnote STRING COMMENT 'The code returned by Smarty Streets associated with a reponse in the footnote section.',
# MAGIC   combined_footnote_code_description STRING COMMENT 'The concatenated description returned by Smarty Streets associated with a reponse in the full footnote section.',
# MAGIC   create_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'Timestamp when the record was first created in the silver layer',
# MAGIC   create_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that created the record',
# MAGIC   update_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'User or process that last updated the record',
# MAGIC   update_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that last updated the record',
# MAGIC   CONSTRAINT `smarty_streets_address_validation_pk` PRIMARY KEY (`serial_number`)
# MAGIC ) USING delta
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/smarty_streets_address_validation'
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

# DBTITLE 1,CMRA SmartyStreets Validation Detail
# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.smarty_streets_address_validation_detail (
# MAGIC   serial_number string not null
# MAGIC     COMMENT 'Unique serial number / identifier for the CMRA address record.',
# MAGIC   primary_number_status string COMMENT 'The confirmation status in USPS of the `primary_number` associated with the address lookup',
# MAGIC   primary_number_changes array<
# MAGIC     string
# MAGIC   > COMMENT 'The array of any types of changes associated with the `primary_number`',
# MAGIC   street_name_status string COMMENT 'The confirmation status in USPS of the `street_name` associated with the address lookup',
# MAGIC   street_name_changes array<
# MAGIC     string
# MAGIC   > COMMENT 'The array of any types of changes associated with the `street_name`',
# MAGIC   street_suffix_status string COMMENT 'The confirmation status in USPS of the `street_suffix` associated with the address lookup',
# MAGIC   street_suffix_changes array<
# MAGIC     string
# MAGIC   > COMMENT 'The array of any types of changes associated with the `street_suffix`',
# MAGIC   state_abbreviation_status string COMMENT 'The confirmation status in USPS of the `state_abbreviation` associated with the address lookup',
# MAGIC   state_abbreviation_changes array<
# MAGIC     string
# MAGIC   > COMMENT 'The array of any types of changes associated with the `state_abbreviation`',
# MAGIC   zipcode_status string COMMENT 'The confirmation status in USPS of the `zipcode` associated with the address lookup',
# MAGIC   zipcode_changes array<
# MAGIC     string
# MAGIC   > COMMENT 'The array of any types of changes associated with the `zipcode`',
# MAGIC   plus4_code_status string COMMENT 'The confirmation status in USPS of the `plus4_code` associated with the address lookup',
# MAGIC   plus4_code_changes array<
# MAGIC     string
# MAGIC   > COMMENT 'The array of any types of changes associated with the `plus4_code`',
# MAGIC   create_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'Timestamp when the record was first created in the silver layer',
# MAGIC   create_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that created the record',
# MAGIC   update_ts TIMESTAMP DEFAULT current_timestamp
# MAGIC     COMMENT 'User or process that last updated the record',
# MAGIC   update_user_id STRING DEFAULT 'CMRA_ETL' COMMENT 'User or process that last updated the record',
# MAGIC   CONSTRAINT `smarty_streets_address_validation_detail_pk` PRIMARY KEY (`serial_number`)
# MAGIC ) USING delta
# MAGIC COMMENT '
# MAGIC A detail table associated with the SmartyStreets address validation. Records here show which parts of an address are confirmed or unconfirmed, and what parts of an address were modified or added. These details are taken directly from the component-analysis of the API.
# MAGIC '
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/smarty_streets_address_validation_detail'
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
# MAGIC  -- ============================================================================
# MAGIC  -- STAGING TABLE: TTAB Paralegal Daily Snapshot (Pre-DQ)
# MAGIC  -- ============================================================================
# MAGIC  CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_paralegal_daily_snapshot_staging (
# MAGIC      -- Primary Key Components
# MAGIC      snapshot_date           DATE NOT NULL COMMENT 'Date the snapshot was captured (Partition Key)',
# MAGIC      object_id               STRING NOT NULL COMMENT 'Unique Object ID from queues.id (PK component)',
# MAGIC      
# MAGIC      -- Snapshot Metadata
# MAGIC      snapshot_timestamp      TIMESTAMP COMMENT 'Exact UTC timestamp of capture execution',
# MAGIC      
# MAGIC      -- Assignment Information
# MAGIC      paralegal_employee_id   STRING COMMENT 'User ID of assigned paralegal (e.g., CAUSTIN) or UNASSIGNED',
# MAGIC      paralegal_employee_no   INT COMMENT 'Employee Number FK from employee table',
# MAGIC      assignment_method       STRING COMMENT 'Logic used: DIRECT, PROCEEDING, or RANGE',
# MAGIC      
# MAGIC      -- Business Identifiers
# MAGIC      reference_number        STRING COMMENT 'Reference number from attributes.object_name',
# MAGIC      serial_prod_num         STRING COMMENT 'Primary Serial (8 digits) or Proceeding Number (starts with 9)',
# MAGIC      registration_number     STRING COMMENT 'Registration number if applicable (7-8 digits)',
# MAGIC      
# MAGIC      -- Classification
# MAGIC      item_class              STRING COMMENT 'Consolidated type: Document or Folder',
# MAGIC      object_type             STRING COMMENT 'Specific object type code (e.g., OPP, CAN, EXT)',
# MAGIC      proceedingtype          STRING COMMENT 'Proceeding type description (e.g., Opposition, Cancellation)',
# MAGIC      documenttype            STRING COMMENT 'Specific document type description',
# MAGIC      
# MAGIC      -- Temporal Data
# MAGIC      received_timestamp      TIMESTAMP COMMENT 'Timestamp item entered the queue',
# MAGIC      received_date           DATE COMMENT 'Date item entered the queue (derived from timestamp)',
# MAGIC      date_diff_business_days INT COMMENT 'Age in Business Days (Mon-Fri, excluding Federal Holidays)',
# MAGIC      is_over_7_days          BOOLEAN COMMENT 'SLA Flag: True if age > 7 business days',
# MAGIC      
# MAGIC      -- Source Traceability
# MAGIC      queue_name              STRING COMMENT 'Source queue name (should be Paralegal)',
# MAGIC      source_object_id        STRING COMMENT 'Traceability ID to source system (matches object_id)',
# MAGIC      
# MAGIC      -- DQX Engine Fields (SHA-256)
# MAGIC      _natural_key_hash       STRING COMMENT 'SHA-256 hash of (snapshot_date | object_id) for deduplication',
# MAGIC      _record_data_hash       STRING COMMENT 'SHA-256 hash of business columns for SCD2 change detection',
# MAGIC      _created_timestamp      TIMESTAMP COMMENT 'Timestamp when record was created in staging',
# MAGIC      _dq_run_id              STRING COMMENT 'Unique identifier for the Data Quality run'
# MAGIC  )
# MAGIC  USING DELTA
# MAGIC  PARTITIONED BY (snapshot_date)
# MAGIC  COMMENT 'Staging table for Paralegal Snapshot - Pre-DQ validation'
# MAGIC  TBLPROPERTIES (
# MAGIC      'delta.enableChangeDataFeed' = 'false'
# MAGIC  );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CREATE FUNCTION: business_days_between
# MAGIC -- Required by ntb_paralegal_staging to calculate age
# MAGIC -- ============================================================================
# MAGIC CREATE OR REPLACE FUNCTION ${conf.catalog}.silver.business_days_between(from_date DATE, to_date DATE)
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC DETERMINISTIC
# MAGIC COMMENT 'Calculates business days (Mon-Fri) between two dates. Returns 0 if start > end.'
# MAGIC RETURN
# MAGIC   CASE 
# MAGIC     WHEN from_date IS NULL OR to_date IS NULL THEN NULL
# MAGIC     WHEN from_date > to_date THEN 0
# MAGIC     ELSE 
# MAGIC       -- Logic: Difference in days minus 2 days for every full week
# MAGIC       DATEDIFF(to_date, from_date) 
# MAGIC       - (FLOOR(DATEDIFF(to_date, from_date) / 7) * 2)
# MAGIC       -- Adjust if start/end days fall on weekends to ensure count is accurate
# MAGIC       - (CASE WHEN DAYOFWEEK(from_date) = 1 THEN 1 ELSE 0 END) -- Sunday
# MAGIC       - (CASE WHEN DAYOFWEEK(from_date) = 7 THEN 1 ELSE 0 END) -- Saturday
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- EVENT LOG TABLE: Lifecycle Tracking
# MAGIC -- ============================================================================
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.ttab_item_event_log (
# MAGIC     event_id                STRING NOT NULL COMMENT 'Unique Event UUID',
# MAGIC     event_date              DATE NOT NULL COMMENT 'Date the event was detected (Snapshot Date)',
# MAGIC     event_type              STRING NOT NULL COMMENT 'NEW_ARRIVAL, COMPLETED, REASSIGNED',
# MAGIC     
# MAGIC     -- Item Identifiers
# MAGIC     object_id               STRING NOT NULL,
# MAGIC     reference_number        STRING,
# MAGIC     serial_prod_num         STRING,
# MAGIC     item_class              STRING,
# MAGIC     
# MAGIC     -- State Transition
# MAGIC     prev_paralegal_id       STRING COMMENT 'Previous owner (NULL for NEW_ARRIVAL)',
# MAGIC     curr_paralegal_id       STRING COMMENT 'Current owner (NULL for COMPLETED)',
# MAGIC     
# MAGIC     -- Metrics
# MAGIC     days_since_arrival      INT COMMENT 'For COMPLETED items: Days it was in queue',
# MAGIC     
# MAGIC     created_at              TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (event_date)
# MAGIC COMMENT 'Derived table tracking daily changes in queue items'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${conf.catalog}.silver.edw_staffing_dashboard_info  (
# MAGIC EMPLOYEE_NUMBER_VW_SL_HRSA_EM string comment 'employee ID number',
# MAGIC EMP_HIST_EMPLOYEE_NUMBER string comment 'employee number',
# MAGIC EMP_HIST_FROM_AGENCY_CODE string comment 'Transfer from employee agency code',
# MAGIC EMP_HIST_FROM_DEPARTMENT_CODE string comment 'Transfer from employee department code',
# MAGIC EMP_HIST_FROM_ORGANIZATION_COD string comment 'Transfer from employee organization code',
# MAGIC EMP_HIST_NATURE_OF_ACTION_DATE timestamp comment 'Date of employee action',
# MAGIC EMP_HIST_ORGANIZATION_CODE string comment 'Employee organization code',
# MAGIC EMP_HIST_TO_AGENCY_CODE string comment 'Transfer to employee agency code',
# MAGIC EMP_HIST_TO_DEPARTMENT_CODE string comment 'Transfer to employee department code',
# MAGIC EMP_HIST_TO_NATURE_OF_ACTION_C string comment 'Transfer action code',
# MAGIC EMP_HIST_TO_NATURE_OF_ACTION_1 timestamp comment 'Transfer action timestamp',
# MAGIC EMP_HIST_TO_NATURE_OF_ACTION_D string comment 'Transfer action description',
# MAGIC EMP_HIST_TO_ORGANIZATION_CODE string comment 'Transfer to employee organization code',
# MAGIC NATURE_OF_ACTION_DESCRIPTION string comment 'Description of action',
# MAGIC NATURE_OF_ACTION_CODE decimal(3,0) comment 'Description of action code',
# MAGIC LATEST_EMPLOYEE_FAMILY_NAME string comment 'Latest employee family name',
# MAGIC LATEST_EMPLOYEE_GIVEN_NAME string comment 'latest employee given name',
# MAGIC LATEST_EMPLOYEE_NUMBER string comment 'latest employee number',
# MAGIC ORGANIZATION_ACTIVE_CODEA string comment 'Employee active org code',
# MAGIC ACCESSION_NATURE_OF_ACTION_DES string comment 'Accession action description',
# MAGIC APPOINTMENT_TYPE_DESCRIPTION string COMMENT 'description of appointment type',
# MAGIC BUSINESS_UNIT_CODE string comment 'USPTO business unit',
# MAGIC EMPLOYEE_ACCESSION_NATURE_OF_A decimal(3,0) comment 'Accession action code',
# MAGIC EMPLOYEE_ACCESSION_PAY_PERIOD_ decimal(2,0) comment 'Accession pay period',
# MAGIC EMPLOYEE_GIVEN_NAME string comment 'Employee given name',
# MAGIC EMPLOYEE_CURRENT_FISCAL_YEAR timestamp comment 'Current fiscal year',
# MAGIC EMPLOYEE_EMPLOYMENT_STATUS_COD decimal(2,0) comment 'current employment status',
# MAGIC EMPLOYEE_GRADE string comment 'Employee grade',
# MAGIC EMPLOYEE_HIRED_DATE timestamp comment 'date employee was hired',
# MAGIC DETAILEE_SUPERVISOR_EMPLOYEE_N string comment 'employee supervisor number',
# MAGIC DETAILEE_SUPERVISOR_DIM_ORGANI string comment 'Detailee supervisor organization 6-digit',
# MAGIC EMPLOYEE_ORGANIZATION_CODE2 string comment 'Employee organization 6-digit',
# MAGIC EMPLOYEE_POSITION_IDENTIFIER decimal(10,0) comment 'identifier for employees position',
# MAGIC EMPLOYEE_POSITION_NUMBER string comment 'Employee position number',
# MAGIC DETAILEE_SUPERVISOR_ORGANIZATI string comment '	Detailee supervisor organization',
# MAGIC EMPLOYEE_POSITION_STATUS_CODE string comment 'Employee position status code',
# MAGIC EMPLOYEE_TENURE_GROUP_CODE decimal(1,0) comment '	Employee tenure group code',
# MAGIC EMPLOYEE_REDUCTION_IN_FORCE_CO timestamp comment 'Employee reduction in force code',
# MAGIC EMPLOYEE_RETIREMENT_SERVICE_CO timestamp comment 'Employee retirement service code',
# MAGIC EMPLOYEE_SEPARATION_ACCESSION_ decimal(1,0) comment 'Separated employee accession code',
# MAGIC EMPLOYEE_SEPARATION_DATE timestamp comment 'date employee left',
# MAGIC EMPLOYEE_SEPARATION_FINAL_T_CO string comment '	Separated employee code',
# MAGIC EMPLOYEE_SEPARATION_NATURE_OF_ decimal(3,0) comment 'circumstances for employee leaving',
# MAGIC EMPLOYEE_SEPARATION_PARENT_LEAVE_CODE string comment 'Separated employee leave code',
# MAGIC EMPLOYEE_SEPARATION_PARENT_RET string comment 'Separated employee parent code',
# MAGIC EMPLOYEE_SEPARATION_PAY_PERIOD decimal(2,0) comment 'Pay period of separated employee',
# MAGIC EMPLOYEE_SEPARATION_TYPE_CODE string comment 'code for separation type',
# MAGIC EMPLOYMENT_STATUS_DESCRIPTION string comment 'Employee current status',
# MAGIC ORGANIZATION_CODE2 string comment 'Employee organization 6-digit',
# MAGIC ORGANIZATION_FIFTH_LEVEL_CODE string comment 'Employee organization 5-digit',
# MAGIC ORGANIZATION_FIFTH_LEVEL_NAME string comment 'Employee organization 5-digit description',
# MAGIC ORGANIZATION_FIRST_LEVEL_CODE string comment 'Employee organization 1-digit',
# MAGIC ORGANIZATION_FIRST_LEVEL_NAME string comment 'Employee organization 1-digit description',
# MAGIC ORGANIZATION_FOURTH_LEVEL_CODE string comment 'Employee organization 4-digit',
# MAGIC ORGANIZATION_FOURTH_LEVEL_NAME string comment 'Employee organization 4-digit description',
# MAGIC ORGANIZATION_NAME string comment 'Organizartions name',
# MAGIC ORGANIZATION_SECOND_LEVEL_CODE string comment 'Employee organization 2-digit',
# MAGIC ORGANIZATION_SECOND_LEVEL_NAME string comment 'Employee organization 2-digit description',
# MAGIC ORGANIZATION_SIXTH_LEVEL_CODE string comment '	Employee organization 6-digit',
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
# MAGIC TMORG string comment 'Description of TM organization',
# MAGIC ACTV_FLG string comment 'Flag for if employee is actively working',
# MAGIC EMP_FULL_NM string Comment 'Employees full name',
# MAGIC EMP_ID_NO decimal(38,10) comment 'Employee ID Number',
# MAGIC END_DA timestamp comment 'employee end date of working',
# MAGIC START_DA timestamp comment 'Employee start date of employement',
# MAGIC SUPERVISOR_FLG string comment 'flag for is employee is a supervisor',
# MAGIC TITLE_CD string comment 'Employee title code',
# MAGIC TITLE_DESC_TX string comment 'Employee title description'
# MAGIC ) USING delta
# MAGIC COMMENT 'Employee information for staffing dashboard'
# MAGIC LOCATION
# MAGIC 's3://${config.cdc_bucket}/delta_tables/${conf.catalog}/silver/edw_staffing_dashboard_info'
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
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.employee_quarter_bi_week (
# MAGIC     fiscal_year_no INT COMMENT 'Fiscal year number',
# MAGIC     quarter_no TINYINT COMMENT 'Quarter number (1-4)',
# MAGIC     q1_wks INT COMMENT 'Weeks in Q1',
# MAGIC     q2_wks INT COMMENT 'Weeks in Q2',
# MAGIC     q3_wks INT COMMENT 'Weeks in Q3',
# MAGIC     q4_wks INT COMMENT 'Weeks in Q4',
# MAGIC     brs_user_id STRING COMMENT 'Business Reporting System user ID',
# MAGIC     employee_no STRING COMMENT 'Employee number',
# MAGIC     employee_nm STRING COMMENT 'Employee name',
# MAGIC     current_organization_cd STRING COMMENT 'Current organization code',
# MAGIC     quarter_bi_week_start_dt DATE COMMENT 'Quarter bi-week start date',
# MAGIC     quarter_bi_week_end_dt DATE COMMENT 'Quarter bi-week end date',
# MAGIC     TEPT_LO STRING COMMENT 'TEPT LO code'
# MAGIC ) USING delta
# MAGIC COMMENT 'Production Simulator Employee Bi week details'
# MAGIC LOCATION
# MAGIC 's3://${config.cdc_bucket}/delta_tables/${conf.catalog}/silver/employee_quarter_bi_week'
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.employee_bd (
# MAGIC     employee_no STRING COMMENT 'Employee number',
# MAGIC     employee_nm STRING COMMENT 'Employee name',
# MAGIC     current_organization_cd STRING COMMENT 'Current organization code',
# MAGIC     quarter_bi_week_start_dt DATE COMMENT 'Quarter bi-week start date',
# MAGIC     quarter_bi_week_end_dt DATE COMMENT 'Quarter bi-week end date',
# MAGIC     fiscal_year_no INT COMMENT 'Fiscal year number',
# MAGIC     quarter_no BYTE COMMENT 'Quarter number (1-4)',
# MAGIC     q1_wks INT COMMENT 'Weeks in Q1',
# MAGIC     q2_wks INT COMMENT 'Weeks in Q2',
# MAGIC     q3_wks INT COMMENT 'Weeks in Q3',
# MAGIC     q4_wks INT COMMENT 'Weeks in Q4',
# MAGIC     brs_user_id STRING COMMENT 'Business Reporting System user ID',
# MAGIC     TEPT_LO STRING COMMENT 'TEPT LO code',
# MAGIC     exam_hrs DECIMAL(12,2) COMMENT 'Examining hours',
# MAGIC     adj_hrs DECIMAL(12,2) COMMENT 'Adjusted hours',
# MAGIC     adj_hrs_dup DECIMAL(12,2) COMMENT 'Adjusted hours duplicate',
# MAGIC     non_exam_hrs DECIMAL(12,2) COMMENT 'Non-examining hours',
# MAGIC     ot_hrs DECIMAL(10,2) COMMENT 'Overtime hours',
# MAGIC     bds DECIMAL(12,1) COMMENT 'Balanced disposal score',
# MAGIC     action_per_examining_hour_qt STRING COMMENT 'Actions per examining hour',
# MAGIC     action_qt STRING COMMENT 'Action quantity',
# MAGIC     goal_status_ct STRING COMMENT 'Goal status count',
# MAGIC     docket_management_qt STRING COMMENT 'Docket management quantity',
# MAGIC     document_management_tx STRING COMMENT 'Document management text',
# MAGIC     bi_week_below_goal_qt STRING COMMENT 'Bi-week below goal quantity',
# MAGIC     action_qt_int DOUBLE COMMENT 'Action quantity integer',
# MAGIC     table STRING COMMENT 'Source table'
# MAGIC ) USING delta
# MAGIC COMMENT 'production simulator employee details'
# MAGIC LOCATION
# MAGIC 's3://${config.cdc_bucket}/delta_tables/${conf.catalog}/silver/employee_bd'
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.prod_simulator_qual (
# MAGIC     employee_no STRING COMMENT 'Employee number',
# MAGIC     employee_nm STRING COMMENT 'Employee name',
# MAGIC     current_organization_cd STRING COMMENT 'Current organization code',
# MAGIC     quarter_bi_week_start_dt DATE COMMENT 'Quarter bi-week start date',
# MAGIC     quarter_bi_week_end_dt DATE COMMENT 'Quarter bi-week end date',
# MAGIC     fiscal_year_no INT COMMENT 'Fiscal year number',
# MAGIC     quarter_no BYTE COMMENT 'Quarter number (1-4)',
# MAGIC     q1_wks INT COMMENT 'Weeks in Q1',
# MAGIC     q2_wks INT COMMENT 'Weeks in Q2',
# MAGIC     q3_wks INT COMMENT 'Weeks in Q3',
# MAGIC     q4_wks INT COMMENT 'Weeks in Q4',
# MAGIC     brs_user_id STRING COMMENT 'Business Reporting System user ID',
# MAGIC     TEPT_LO STRING COMMENT 'TEPT LO code',
# MAGIC     serial_num_tx STRING COMMENT 'Serial number',
# MAGIC     quality_review_dt DATE COMMENT 'Quality review date',
# MAGIC     statutory_error_qt DECIMAL(5,2) COMMENT 'Statutory error quantity',
# MAGIC     prac_pro_error_qt INT COMMENT 'Practice/procedure error quantity',
# MAGIC     search_ct STRING COMMENT 'Search count',
# MAGIC     write_grade_qt INT COMMENT 'Write grade quantity',
# MAGIC     explanation_tx STRING COMMENT 'Explanation text',
# MAGIC     write_grade_txt STRING COMMENT 'Write grade text',
# MAGIC     qual_status STRING COMMENT 'Quality status'
# MAGIC ) USING delta
# MAGIC COMMENT 'Production Simulator Quality details'
# MAGIC LOCATION
# MAGIC 's3://${config.cdc_bucket}/delta_tables/${conf.catalog}/silver/prod_simulator_qual'
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
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.prod_simulator_fy (
# MAGIC     countnonnull_write_grade_qt LONG COMMENT 'Count of non-null write grade quantities',
# MAGIC     count_write_grade_qt LONG COMMENT 'Total count of write grade quantities',
# MAGIC     count_serial_num_tx LONG COMMENT 'Count of serial numbers',
# MAGIC     count_write_grade_qt_is_1 LONG COMMENT 'Count of write grade quantities equal to 1',
# MAGIC     suff_rt DOUBLE COMMENT 'Sufficiency rate',
# MAGIC     suff_score STRING COMMENT 'Sufficiency score',
# MAGIC     avg_write_rt STRING COMMENT 'Average write rate',
# MAGIC     avg_write_score STRING COMMENT 'Average write score',
# MAGIC     `write_def%` STRING COMMENT 'Write deficiency percentage',
# MAGIC     employee_no STRING COMMENT 'Employee number',
# MAGIC     fiscal_year_no STRING COMMENT 'Fiscal year number',
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
# MAGIC     prod_alloc_wgt_int DECIMAL(5,1) COMMENT 'Product allocation weight',
# MAGIC     qual_alloc_wgt_int DECIMAL(5,1) COMMENT 'Quality allocation weight',
# MAGIC     wf_alloc_wgt_int DECIMAL(5,1) COMMENT 'Workflow allocation weight',
# MAGIC     org_alloc_wgt_int DECIMAL(5,1) COMMENT 'Organization allocation weight',
# MAGIC     org_effectiveness_pt DECIMAL(12,2) COMMENT 'Organization effectiveness points',
# MAGIC     org_train_pt DECIMAL(12,2) COMMENT 'Organization training points',
# MAGIC     org_mentor_pt DECIMAL(12,2) COMMENT 'Organization mentor points',
# MAGIC     employee_nm STRING COMMENT 'Employee name',
# MAGIC     brs_user_id STRING COMMENT 'Business Reporting System user ID',
# MAGIC     current_organization_cd STRING COMMENT 'Current organization code',
# MAGIC     avg_score_rt DECIMAL(12,2) COMMENT 'Average score rate',
# MAGIC     examiner_amendment_usage_pt DECIMAL(12,2) COMMENT 'Examiner amendment usage points',
# MAGIC     workflow_performance_rating_cd STRING COMMENT 'Workflow performance rating code',
# MAGIC     no_sig_trainee_biweeks INTEGER COMMENT 'Number of significant trainee bi-weeks',
# MAGIC     partial_sig_trainee_biweeks INTEGER COMMENT 'Number of partial significant trainee bi-weeks',
# MAGIC     pfs_trainee_biweeks INTEGER COMMENT 'Number of PFS trainee bi-weeks',
# MAGIC     current_gs_grade_level_cd STRING COMMENT 'Current GS grade level code',
# MAGIC     table STRING COMMENT 'Source table'
# MAGIC ) USING delta
# MAGIC COMMENT 'production simulator FY details'
# MAGIC LOCATION
# MAGIC 's3://${config.cdc_bucket}/delta_tables/${conf.catalog}/silver/prod_simulator_fy'
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.prod_simulator_qtr (
# MAGIC     employee_no STRING COMMENT 'Employee number',
# MAGIC     fiscal_year_no STRING COMMENT 'Fiscal year number',
# MAGIC     quarter_no STRING COMMENT 'Quarter number',
# MAGIC     fk_gs_level_cd STRING COMMENT 'GS level code',
# MAGIC     base_c_bds INT COMMENT 'Base C balanced disposal score',
# MAGIC     base_fs_bds INT COMMENT 'Base FS balanced disposal score',
# MAGIC     base_m_bds INT COMMENT 'Base M balanced disposal score',
# MAGIC     base_o_bds INT COMMENT 'Base O balanced disposal score',
# MAGIC     employee_nm STRING COMMENT 'Employee name',
# MAGIC     brs_user_id STRING COMMENT 'Business Reporting System user ID',
# MAGIC     current_organization_cd STRING COMMENT 'Current organization code',
# MAGIC     bds_from_last_qtr STRING COMMENT 'Balanced disposal score from last quarter',
# MAGIC     workflow_qtr_goal DECIMAL(5,2) COMMENT 'Workflow quarter goal',
# MAGIC     schedule_hour_qt STRING COMMENT 'Scheduled hour quantity',
# MAGIC     performance_rating_cd STRING COMMENT 'Performance rating code',
# MAGIC     next_qtr_perf_rate_cd STRING COMMENT 'Next quarter performance rating code',
# MAGIC     transfer_balanced_disposal_qt STRING COMMENT 'Transfer balanced disposal quantity',
# MAGIC     table STRING COMMENT 'Source table'
# MAGIC ) USING delta
# MAGIC COMMENT 'production simulator QTR details'
# MAGIC LOCATION
# MAGIC 's3://${config.cdc_bucket}/delta_tables/${conf.catalog}/silver/prod_simulator_qtr'
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
# MAGIC

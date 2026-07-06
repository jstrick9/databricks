# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngpdb-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmngpdb_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmngpdb_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmngpdb_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.silver 
# MAGIC COMMENT 'For staging layer data' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table ${conf.catalog}.silver.job_log (
# MAGIC   job_log_id BIGINT COMMENT 'Unique identifier for each job log entry',--not null generated always as identity,
# MAGIC   job_nm STRING COMMENT 'Name of the job',
# MAGIC   start_ts TIMESTAMP COMMENT 'Timestamp indicating the start time of the job',
# MAGIC   end_ts TIMESTAMP COMMENT 'Timestamp indicating the end time of the job',
# MAGIC   status_ct STRING COMMENT 'Status code indicating the status of the job',
# MAGIC   src_cnt INT COMMENT 'Number of records in the source dataset',
# MAGIC   trgt_cnt INT COMMENT 'Number of records in the target dataset',
# MAGIC   comment_tx STRING COMMENT 'Additional comments or notes about the job'
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/job_log'

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table ${conf.catalog}.silver.job_control (
# MAGIC   job_control_id BIGINT COMMENT 'The unique identifier for each job in the job_control table.',-- not null generated always as identity,
# MAGIC   job_nm STRING COMMENT 'The name of the job being executed.',
# MAGIC   load_ts TIMESTAMP COMMENT 'The timestamp when the job was loaded into the system.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp when the job was created.',
# MAGIC   create_user_id STRING COMMENT 'The user ID of the individual who created the job.',
# MAGIC   last_mod_ts TIMESTAMP COMMENT 'The timestamp when the job was last modified.',
# MAGIC   last_mod_user_id STRING COMMENT 'The user ID of the individual who last modified the job.'
# MAGIC ) using delta partitioned by (job_nm) location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/job_control'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.tmapplser
# MAGIC ( 
# MAGIC   actcd STRING comment 'The action code associated with the load', 
# MAGIC   sernum STRING comment 'The serial number of load',
# MAGIC   pulldt DATE comment 'The date row was pulled from source tables',
# MAGIC   tabname STRING comment 'The table_name were row was pulled from',
# MAGIC   create_ts TIMESTAMP  comment 'The date and time that the record is inserted in the database',
# MAGIC   create_user_id string   comment 'The User Identifier of the logged-on AIS User that initiated the insert of the record into the database',
# MAGIC   last_mod_ts TIMESTAMP  comment 'The date and time that the record was last modified in the database.Upon creation, this will be the same as the Create Timestamp' ,
# MAGIC   last_mod_user_id string  comment 'The User Identifier of the logged on User that initiated the last modification to the record in the database' ,
# MAGIC   lock_control_no INT  comment 'A Number used  to verify that the record being updated has not been altered since it was retrieved for update when optimistic locking is used.'
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/tmapplser'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true, 'delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.bdss_class
# MAGIC ( 
# MAGIC   cl_ser_num STRING comment 'The serial number of load',
# MAGIC   cl_cls_intl_ct INT comment 'count of international classes',
# MAGIC   cl_cls_us_ct INT comment 'count of us classes',
# MAGIC   cls_intl STRING comment 'list of international classes',
# MAGIC   cls_us STRING comment 'list of us classes',
# MAGIC   cls_stat STRING comment 'status of classes',
# MAGIC   dt_stat INT comment 'status date',
# MAGIC   dt_1_use INT comment 'date of first use',
# MAGIC   dt_1_use_comm INT comment 'date of first use commercial',
# MAGIC   prime_cls STRING comment 'prime class',
# MAGIC   create_ts TIMESTAMP  comment 'The date and time that the record is inserted in the database',
# MAGIC   create_user_id string  comment 'The User Identifier of the logged-on AIS User that initiated the insert of the record into the database'
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/bdss_class'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: Standardized case milestones -SCD2
# MAGIC -- Source: trm_tmngpdb.bronze.trademark_h / tm_itu_h / tm_renewal_h / tm_divisional_child_h / tm_post_registration / tram_am
# MAGIC -- Target: USPTO.gov Trademark Application Timeline -https://www.uspto.gov/trademarks/application-timeline
# MAGIC -- Maintained by: notebooks/python/silver/build_case_timeline.py
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.case_milestones (
# MAGIC   serial_number STRING NOT NULL COMMENT 'USPTO trademark serial number -primary key, from trademark_h.<pk> -SHA-256 hashed in logs per PII policy',
# MAGIC   filing_date DATE COMMENT 'Application filing date -trademark_h.filing_dt',
# MAGIC   filing_basis STRING COMMENT 'Filing basis -TEAS / MADRID / 66a -derived from tram_am.am_flg_action -1=TEAS, 0=MADRID -VERIFY WITH TRM SME',
# MAGIC   
# MAGIC   -- Examination milestones -USPTO.gov Summary table
# MAGIC   first_oa_date DATE COMMENT 'First Office Action date -trademark_h.last_action_dt WHERE last_event_type_cd IN (PROAI,TROAI,IROAI…) -see wait_time_source_mapping.yml -VERIFY EVENT CODES',
# MAGIC   registration_date DATE COMMENT 'Registration date -trademark_h.status_dt WHERE legacy_status_cd IN (700,701,702,705… ) -VERIFY',
# MAGIC   abandonment_date DATE COMMENT 'Abandonment / disposal date -trademark_h.status_dt WHERE legacy_status_cd IN (800,801,802…) -VERIFY',
# MAGIC   disposal_date DATE COMMENT 'Coalesce(registration_date, abandonment_date) -end of prosecution -used for Registration/Abandonment wait time (Target 11.0 months, Avg 9.9 months)',
# MAGIC
# MAGIC   -- Intent to Use -15 day targets
# MAGIC   sou_filing_date DATE COMMENT 'Statement of Use filing received date -tm_itu_h.LATEST_ITU_FILNG_RECEIVED_DT',
# MAGIC   sou_processed_date DATE COMMENT 'Statement of Use processed date -tm_itu_h.SOU_RECEIVED_DT -ITU SOU wait time, Target 15 days',
# MAGIC   extension_request_date DATE COMMENT 'ITU Extension request received date -tm_itu_extension_h.CREATE_TS',
# MAGIC   extension_processed_date DATE COMMENT 'ITU Extension processed / expiration date -tm_itu_extension_h.EXPIRATION_DT -Extension wait time, Target 15 days',
# MAGIC   divisional_request_date DATE COMMENT 'Divisional request mailroom received date -tm_divisional_child_h.mailroom_received_dt',
# MAGIC   divisional_processed_date DATE COMMENT 'Divisional request processed date -tm_divisional_child_h.tm_divisional_status_dt WHERE fk_tm_divisional_status_cd IN (6,5,99) -Target 15 days',
# MAGIC
# MAGIC   -- Petitions Office -60 day target
# MAGIC   lop_filing_date DATE COMMENT 'Letter of Protest filing date -tm_states / tram_am -NOT YET MAPPED -NULL until event_codes confirmed',
# MAGIC   lop_processed_date DATE COMMENT 'Letter of Protest decision date -tm_states / tram_am -NOT YET MAPPED -Petitions LOP wait time, Target 60 days',
# MAGIC
# MAGIC   -- Post Registration -90 day targets
# MAGIC   affidavit_filing_date DATE COMMENT 'Affidavit of Use / Incontestability (Sec 8/9) filing date -tm_post_registration.latest_correspondence_rcvd_dt',
# MAGIC   affidavit_processed_date DATE COMMENT 'Affidavit processed date -tm_post_registration.post_reg_audit_begin_dt -Affidavits wait time, Target 90 days',
# MAGIC   amendment_filing_date DATE COMMENT 'Amendment / Correction filing date -tm_post_registration.latest_correspondence_rcvd_dt -currently same as Affidavit -needs event type split',
# MAGIC   amendment_processed_date DATE COMMENT 'Amendment / Correction processed date -tm_post_registration.post_reg_audit_begin_dt -Amendments wait time, Target 90 days',
# MAGIC   renewal_filing_date DATE COMMENT 'Renewal filing date -tm_renewal_h.renewal_filed_dt',
# MAGIC   renewal_processed_date DATE COMMENT 'Renewal processed date -tm_renewal_h.renewal_begin_effective_dt -Renewals wait time, Target 90 days',
# MAGIC
# MAGIC   -- Examination Support Unit -14 day target
# MAGIC   esu_response_date DATE COMMENT 'ESU Response / Correction received date -employee_credit_transaction.transaction_effective_dt -NOT YET MAPPED -fk_credit_tran_rsn_type_cd needs SME',
# MAGIC   esu_processed_date DATE COMMENT 'ESU Response processed date -employee_credit_transaction.transaction_effective_dt -ESU wait time, Target 14 days',
# MAGIC
# MAGIC   -- SCD2 audit columns
# MAGIC   _valid_from TIMESTAMP COMMENT 'SCD2 valid-from timestamp -row become current',
# MAGIC   _valid_to TIMESTAMP COMMENT 'SCD2 valid-to timestamp -NULL = current row',
# MAGIC   _is_current BOOLEAN COMMENT 'True if this is the current active version of the case milestones',
# MAGIC   _updated_ts TIMESTAMP COMMENT 'ETL last updated timestamp -UTC'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'TRM Silver - Case-level milestone dates for USPTO Trademark Processing Wait Times -1 row per serial_number, SCD2 -Source: trm_tmngpdb.bronze -Drives: www.uspto.gov/trademarks/application-timeline'
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/case_milestones'
# MAGIC TBLPROPERTIES (
# MAGIC   delta.enableChangeDataFeed = true,
# MAGIC   delta.columnMapping.mode = 'name'
# MAGIC );
# MAGIC
# MAGIC -- Primary key -SCD2 natural key
# MAGIC ALTER TABLE ${conf.catalog}.silver.case_milestones ADD CONSTRAINT pk_case_milestones PRIMARY KEY (serial_number, _valid_from) RELY;
# MAGIC
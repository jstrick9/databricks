# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
tqr_catalog = common_configs['schema']['tqr_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)

# COMMAND ----------

spark.conf.set('conf.catalog', tqr_catalog)
spark.conf.set('conf.database', 'silver')

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists ${conf.catalog};
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table ${conf.catalog}.${conf.database}.job_log
# MAGIC ( 
# MAGIC   job_log_id BIGINT not null generated always as identity, 
# MAGIC   job_nm STRING,
# MAGIC   start_ts TIMESTAMP,
# MAGIC   end_ts TIMESTAMP,   
# MAGIC   status_ct STRING,
# MAGIC   record_qt INT,
# MAGIC   comment_tx STRING
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by
# MAGIC (
# MAGIC   job_nm
# MAGIC )
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/job_log'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC create or replace table ${conf.catalog}.${conf.database}.job_control
# MAGIC ( 
# MAGIC   job_control_id BIGINT not null generated always as identity, 
# MAGIC   job_nm STRING,
# MAGIC   load_ts  TIMESTAMP,
# MAGIC   create_ts  TIMESTAMP,   
# MAGIC   create_user_id STRING,
# MAGIC   last_mod_ts TIMESTAMP,
# MAGIC   last_mod_user_id STRING
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by
# MAGIC (
# MAGIC   job_nm
# MAGIC ) 
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/job_control'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table ${conf.catalog}.${conf.database}.employee_organization(
# MAGIC   employee_no varchar(7) , 
# MAGIC   organization_cd varchar(10) , 
# MAGIC   status_ct string , 
# MAGIC   create_ts timestamp , 
# MAGIC   create_user_id varchar(36) , 
# MAGIC   last_mod_ts timestamp , 
# MAGIC   last_mod_user_id varchar(36))
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/employee_organization'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  or replace table ${conf.catalog}.${conf.database}.src_trm_application_event (
# MAGIC   cfk_trademark_gid STRING,
# MAGIC   BUSINESS_EVENT_ID DECIMAL(10,0),
# MAGIC   SERIAL_NUM_TX STRING,
# MAGIC   ckf_business_event_reason_id DECIMAL(10,0),
# MAGIC   business_event_reason_cd STRING,
# MAGIC   EFFECTIVE_TS TIMESTAMP,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   last_mod_ts TIMESTAMP,
# MAGIC   last_mod_user_id STRING)
# MAGIC USING delta 
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/src_trm_application_event'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  or replace table ${conf.catalog}.${conf.database}.src_trm_application (
# MAGIC   cfk_trademark_gid STRING,
# MAGIC   serial_num_tx STRING,
# MAGIC   filing_dt TIMESTAMP,
# MAGIC   literal_element_tx STRING,
# MAGIC   standard_character_tx STRING,
# MAGIC   mark_description_tx STRING,
# MAGIC   mark_drawing_type_cd STRING,
# MAGIC   mark_drawing_type_title_tx STRING,
# MAGIC   examiner_employee_no STRING,
# MAGIC   source_system_nm STRING,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   last_mod_ts TIMESTAMP,
# MAGIC   last_mod_user_id STRING)
# MAGIC USING delta 
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/src_trm_application'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  or replace table ${conf.catalog}.${conf.database}.src_trm_filing_basis (
# MAGIC   cfk_trademark_gid STRING,
# MAGIC   serial_num_tx STRING,
# MAGIC   filing_basis_cd STRING,
# MAGIC   current_in STRING,
# MAGIC   amend_in STRING,
# MAGIC   file_in STRING,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   last_mod_ts TIMESTAMP,
# MAGIC   last_mod_user_id STRING)
# MAGIC USING delta 
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/src_trm_filing_basis'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  or replace table ${conf.catalog}.${conf.database}.stnd_tagged_element (
# MAGIC   tagged_element_id INT,
# MAGIC   tagged_element_nm VARCHAR(100),
# MAGIC   quality_metric_in BOOLEAN,
# MAGIC   refusal_requirements_in BOOLEAN,
# MAGIC   substantive_in BOOLEAN,
# MAGIC   procedural_in BOOLEAN,
# MAGIC   substantive_error_in BOOLEAN,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id VARCHAR(36),
# MAGIC   last_mod_ts TIMESTAMP,
# MAGIC   last_mod_user_id VARCHAR(36))
# MAGIC USING delta 
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/stnd_tagged_element'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table ${conf.catalog}.${conf.database}.event_inventory_stage (
# MAGIC   review_type_cd VARCHAR(15) ,
# MAGIC   serial_num_tx VARCHAR(8) ,
# MAGIC   source_system_nm VARCHAR(100) ,
# MAGIC   search_present_in INT ,
# MAGIC   source_event_dt TIMESTAMP ,
# MAGIC   docket_in INT ,
# MAGIC   mark_literal_element_tx STRING ,
# MAGIC   mark_drawing_type_cd VARCHAR(5) ,
# MAGIC   mark_drawing_type_title_tx VARCHAR(25),
# MAGIC   mark_description_tx STRING ,
# MAGIC   examiner_employee_no VARCHAR(7),
# MAGIC   organization_cd VARCHAR(10) ,
# MAGIC   event_json_doc STRING ,
# MAGIC   inventory_create_ts TIMESTAMP ,
# MAGIC   lock_control_no INT ,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id VARCHAR(36),
# MAGIC   last_mod_ts TIMESTAMP ,
# MAGIC   last_mod_user_id VARCHAR(36) ,
# MAGIC   is_tm_exam BOOLEAN)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/tqr/silver/event_inventory_stage'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

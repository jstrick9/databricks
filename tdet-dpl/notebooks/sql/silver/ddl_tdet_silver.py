# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/tdet-conf.yaml"

print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

configs = read_yaml(config_file)
tdet_catalog = configs['schema']['trgt_catalog']
certificate_bucket = configs['s3']['certificate_bucket']
spark.conf.set('config.certificate_bucket', certificate_bucket)
spark.conf.set('config.tdet_catalog', tdet_catalog)
spark.conf.set('config.dbx_env', dbutils.widgets.get('dbx_env'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.tdet_catalog} MANAGED LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}';

# COMMAND ----------

# MAGIC %sql 
# MAGIC GRANT ALL PRIVILEGES ON CATALOG ${config.tdet_catalog} TO `e5229241-17a2-4935-8626-0e0db3b81fc7`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${config.tdet_catalog}.silver COMMENT 'For TDET silver layer and job audit data';

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${config.tdet_catalog}.silver.job_log (
# MAGIC   job_log_id BIGINT not null generated always as identity,
# MAGIC   job_nm STRING,
# MAGIC   start_ts TIMESTAMP,
# MAGIC   end_ts TIMESTAMP,
# MAGIC   status_ct STRING,
# MAGIC   record_qt INT,
# MAGIC   comment_tx STRING
# MAGIC ) using delta partitioned by (job_nm) location 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/silver/job_log'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${config.tdet_catalog}.silver.job_control (
# MAGIC   job_control_id BIGINT not null generated always as identity,
# MAGIC   job_nm STRING,
# MAGIC   load_ts TIMESTAMP,
# MAGIC   create_ts TIMESTAMP,
# MAGIC   create_user_id STRING,
# MAGIC   last_mod_ts TIMESTAMP,
# MAGIC   last_mod_user_id STRING
# MAGIC ) using delta partitioned by (job_nm) location 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/silver/job_control'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${config.tdet_catalog}.silver.tdet_app_search (
# MAGIC   serial_number VARCHAR(8),
# MAGIC   mark_tx STRING,
# MAGIC   filing_date DATE,
# MAGIC   filed_bases STRING,
# MAGIC   current_bases STRING,
# MAGIC   registration_number INT,
# MAGIC   registration_date DATE,
# MAGIC   owner_name STRING,
# MAGIC   owner_name_historical STRING,
# MAGIC   owner_address STRING,
# MAGIC   owner_country STRING,
# MAGIC   owner_email STRING,
# MAGIC   owner_email_historical STRING,
# MAGIC   owner_phone STRING,
# MAGIC   attorney_membership_number STRING,
# MAGIC   attorney_name STRING,
# MAGIC   attorney_name_historical STRING,
# MAGIC   attorney_address STRING,
# MAGIC   attorney_email STRING,
# MAGIC   attorney_email_historical STRING,
# MAGIC   attorney_phone STRING,
# MAGIC   correspondent_name STRING,
# MAGIC   correspondent_name_historical STRING,
# MAGIC   correspondent_address STRING,
# MAGIC   correspondent_email STRING,
# MAGIC   correspondent_email_historical STRING,
# MAGIC   correspondent_email_secondary STRING,
# MAGIC   correspondent_phone STRING,
# MAGIC   domestic_representative_name STRING,
# MAGIC   domestic_representative_name_historical STRING,
# MAGIC   domestic_representative_email STRING,
# MAGIC   domestic_representative_email_historical STRING,
# MAGIC   domestic_representative_phone STRING,
# MAGIC   examiner_number INT COMMENT 'Examiner ID',
# MAGIC   examiner_name STRING COMMENT 'The workers name.',
# MAGIC   docket_number VARCHAR(120),
# MAGIC   firm_name STRING,
# MAGIC   law_office STRING COMMENT 'Law office handling the case',
# MAGIC   class_list STRING,
# MAGIC   status STRING,
# MAGIC   status_date DATE,
# MAGIC   og_issue_date DATE,
# MAGIC   og_status STRING,
# MAGIC   og_category STRING,
# MAGIC   international_registration_number STRING,
# MAGIC   international_us_reference_number STRING,
# MAGIC   specimen_url STRING,
# MAGIC   _natural_key_hash STRING,
# MAGIC   _record_data_hash STRING,
# MAGIC   _created_date DATE,
# MAGIC   _created_timestamp TIMESTAMP,
# MAGIC   _updated_timestamp TIMESTAMP,
# MAGIC   _is_record_active BOOLEAN
# MAGIC   )
# MAGIC USING delta
# MAGIC PARTITIONED BY (_created_date)
# MAGIC LOCATION 's3://${config.certificate_bucket}/delta_tables/${config.tdet_catalog}/silver/tdet_app_search'

# COMMAND ----------


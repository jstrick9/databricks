# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../../config/{dbx_env}/tmngpdb-conf.yaml"

print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

configs = read_yaml(config_file)
tmngpdb_catalog = configs['schema']['trgt_catalog']
cdc_bucket = configs['cdc']['cdc_bucket']
spark.conf.set('config.cdc_bucket', cdc_bucket)
spark.conf.set('config.tmngpdb_catalog', tmngpdb_catalog)
spark.conf.set('config.dbx_env', dbutils.widgets.get('dbx_env'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngpdb_catalog}.bronze.tm_locations (
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
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngpdb_catalog}/bronze/tm_locations'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.tmngpdb_catalog}.bronze.tm_class (
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
# MAGIC status_dt timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngpdb_catalog}/bronze/tm_class'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${config.tmngpdb_catalog}.bronze.cdc_batch_job_control (
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string,
# MAGIC   group_name string,
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   full_load string,
# MAGIC   initial_load_finished boolean
# MAGIC )USING delta
# MAGIC PARTITIONED BY (group_name)
# MAGIC location 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngpdb_catalog}/bronze/cdc_batch_job_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${config.tmngpdb_catalog}.bronze.cdc_batch_job_history (
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )USING delta
# MAGIC location 's3://${config.cdc_bucket}/eds/delta_tables/${config.tmngpdb_catalog}/bronze/cdc_batch_job_history'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

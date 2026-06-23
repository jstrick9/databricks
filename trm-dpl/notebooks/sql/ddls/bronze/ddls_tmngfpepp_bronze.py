# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create TMNGFPEPP bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file="../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tmngfpepp-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
    print(f'{dbx_env=}')
else:
    dbutils.widgets.text("env", dbx_env)
    print(f'{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs=read_yaml(config_file)
tmngfpepp_catalog = common_configs['schema']['trgt_catalog']
src_folder=common_configs['cdc']['src_csv_files']
src_database=common_configs['cdc']['src_database']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.tmngfpepp_catalog', tmngfpepp_catalog.lower())
print(f'{tmngfpepp_catalog=},{src_folder=}, ,{src_database=}')

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmngfpepp_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.src_folder', src_folder)
spark.conf.set('conf.src_database', src_database)


# COMMAND ----------

# MAGIC %sql
# MAGIC create CATALOG if not exists  ${conf.catalog};
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.databasechangelog 
# MAGIC (
# MAGIC   id string, 
# MAGIC  author string, 
# MAGIC  filename string, 
# MAGIC  dateexecuted timestamp, 
# MAGIC  orderexecuted decimal(38,0), 
# MAGIC  exectype string, 
# MAGIC  md5sum string, 
# MAGIC  description string, 
# MAGIC  comments string, 
# MAGIC  tag string, 
# MAGIC  liquibase string, 
# MAGIC  contexts string, 
# MAGIC  labels string, 
# MAGIC  deployment_id string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/databasechangelog'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.databasechangeloglock 
# MAGIC (
# MAGIC   id  decimal(38,0), 
# MAGIC  locked  decimal(1,0), 
# MAGIC  lockgranted timestamp, 
# MAGIC  lockedby string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/databasechangeloglock'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.form_paragraph 
# MAGIC (
# MAGIC   form_paragraph_gid string, 
# MAGIC  call_number_tx string ,
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string, 
# MAGIC  sort_order_tx string, 
# MAGIC  template_in string
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/form_paragraph'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.form_paragraph_action 
# MAGIC (
# MAGIC   fk_form_paragraph_version_gid string, 
# MAGIC  form_paragraph_action_gid string, 
# MAGIC  fk_form_paragraph_action_cd string, 
# MAGIC  cfk_employee_no string, 
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string, 
# MAGIC  action_ts timestamp 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/form_paragraph_action'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.form_paragraph_reason 
# MAGIC (
# MAGIC   fk_form_paragraph_reason_id decimal(10,0), 
# MAGIC  fk_form_paragraph_gid string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/form_paragraph_reason'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.form_paragraph_version 
# MAGIC (
# MAGIC paragraph_title_tx string, 
# MAGIC  create_ts timestamp, 
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string, 
# MAGIC  begin_effective_ts timestamp, 
# MAGIC  end_effective_ts timestamp, 
# MAGIC  status_ct string, 
# MAGIC  version_no decimal(5,2),
# MAGIC  fk_form_paragraph_group_id decimal(10,0), 
# MAGIC  fk_form_paragraph_category_id decimal(10,0), 
# MAGIC  case_relationship_ct string, 
# MAGIC  allow_end_user_edits_in string, 
# MAGIC  track_end_user_edits_in string,
# MAGIC  scheduled_action_ts timestamp, 
# MAGIC  fk_chapter_section_id int, 
# MAGIC  fk_fp_call_number_tx string,
# MAGIC  source_status_ct string, 
# MAGIC  fk_source_form_para_ver_gid string, 
# MAGIC  form_paragraph_tx string, 
# MAGIC  end_user_notes_tx string, 
# MAGIC  research_notes_tx string, 
# MAGIC  published_ts timestamp, 
# MAGIC  published_by_employee_no string, 
# MAGIC  retired_ts timestamp, 
# MAGIC  retired_by_employee_no string, 
# MAGIC  form_paragraph_version_gid string, 
# MAGIC  scheduled_ts timestamp, 
# MAGIC  scheduled_by_employee_no string, 
# MAGIC  fk_division_id decimal(10,0) 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/form_paragraph_version'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.fpv_scheduled_job 
# MAGIC (
# MAGIC   notified_in string, 
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string, 
# MAGIC  fk_form_paragraph_version_gid string
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/fpv_scheduled_job'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_triggers(
# MAGIC sched_name	    string,
# MAGIC trigger_name	string,
# MAGIC trigger_group	string,
# MAGIC job_name	    string,
# MAGIC job_group	    string,
# MAGIC description	    string,
# MAGIC next_fire_time	decimal(13,0),
# MAGIC prev_fire_time	decimal(13,0),
# MAGIC priority	    decimal(13,0),
# MAGIC trigger_state	string,
# MAGIC trigger_type	string,
# MAGIC start_time	    decimal(13,0),
# MAGIC end_time	    decimal(13,0),
# MAGIC calendar_name	string,
# MAGIC misfire_instr	decimal(2,0),
# MAGIC job_data        string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_triggers'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_calendars 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  calendar_name string, 
# MAGIC  calendar string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_calendars'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_cron_triggers 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  trigger_name string, 
# MAGIC  trigger_group string, 
# MAGIC  cron_expression string, 
# MAGIC  time_zone_id string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_cron_triggers'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_fired_triggers 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  entry_id string, 
# MAGIC  trigger_name string, 
# MAGIC  trigger_group string, 
# MAGIC  instance_name string, 
# MAGIC  fired_time decimal(13,0), 
# MAGIC  sched_time decimal(13,0), 
# MAGIC  priority decimal(13,0), 
# MAGIC  state string, 
# MAGIC  job_name string, 
# MAGIC  job_group string, 
# MAGIC  is_nonconcurrent string, 
# MAGIC  requests_recovery string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_fired_triggers'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_job_details 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  job_name string, 
# MAGIC  job_group string, 
# MAGIC  description string, 
# MAGIC  job_class_name string, 
# MAGIC  is_durable string, 
# MAGIC  is_nonconcurrent string, 
# MAGIC  is_update_data string, 
# MAGIC  requests_recovery string, 
# MAGIC  job_data string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_job_details'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_locks 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  lock_name string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_locks'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_paused_trigger_grps 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  trigger_group string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_paused_trigger_grps'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_scheduler_state 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  instance_name string, 
# MAGIC  last_checkin_time decimal(13,0), 
# MAGIC  checkin_interval decimal(13,0) 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_scheduler_state'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_simple_triggers 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  trigger_name string, 
# MAGIC  trigger_group string, 
# MAGIC  repeat_count decimal(7,0),
# MAGIC  repeat_interval decimal(12,0),
# MAGIC  times_triggered decimal(10,0) 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_simple_triggers'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_simprop_triggers 
# MAGIC (
# MAGIC sched_name    string,
# MAGIC trigger_name  string,
# MAGIC trigger_group string,
# MAGIC str_prop_1    string,
# MAGIC str_prop_2    string,
# MAGIC str_prop_3    string,
# MAGIC int_prop_1    decimal(10,0),
# MAGIC int_prop_2    decimal(10,0),
# MAGIC long_prop_1   decimal(13,0),
# MAGIC long_prop_2   decimal(13,0),
# MAGIC dec_prop_1    decimal(13,4),
# MAGIC dec_prop_2    decimal(13,4),
# MAGIC bool_prop_1   string,
# MAGIC bool_prop_2   string
# MAGIC )  
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_simprop_triggers'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.stnd_chapter_section 
# MAGIC (
# MAGIC   chapter_section_id decimal(10,0), 
# MAGIC  title_tx string, 
# MAGIC  description_tx string, 
# MAGIC  fk_parent_chapter_section_id int, 
# MAGIC  position_order_no decimal(4,0), 
# MAGIC  begin_effective_ts timestamp, 
# MAGIC  end_effective_ts timestamp, 
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string, 
# MAGIC  chapter_section_ct string, 
# MAGIC  hierarchy_level_no decimal(2,0) 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/stnd_chapter_section'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.stnd_form_paragraph_action 
# MAGIC (
# MAGIC   form_paragraph_action_cd string, 
# MAGIC  title_tx string, 
# MAGIC  description_tx string, 
# MAGIC  begin_effective_ts timestamp, 
# MAGIC  end_effective_ts timestamp, 
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/stnd_form_paragraph_action'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.stnd_form_paragraph_category 
# MAGIC (
# MAGIC   form_paragraph_category_id decimal(10,0), 
# MAGIC  title_tx string, 
# MAGIC  description_tx string, 
# MAGIC  fk_chapter_section_id decimal(10,0), 
# MAGIC  begin_effective_ts timestamp, 
# MAGIC  end_effective_ts timestamp, 
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/stnd_form_paragraph_category'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.stnd_form_paragraph_group 
# MAGIC (
# MAGIC   form_paragraph_group_id decimal(10,0), 
# MAGIC  title_tx string, 
# MAGIC  description_tx string, 
# MAGIC  fk_chapter_section_id decimal(10,0), 
# MAGIC  begin_effective_ts timestamp, 
# MAGIC  end_effective_ts timestamp, 
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/stnd_form_paragraph_group'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.stnd_form_paragraph_reason 
# MAGIC (
# MAGIC   form_paragraph_reason_id decimal(10,0), 
# MAGIC  title_tx string, 
# MAGIC  description_tx string, 
# MAGIC  begin_effective_ts timestamp, 
# MAGIC  end_effective_ts timestamp, 
# MAGIC  create_ts timestamp,  
# MAGIC  create_user_id string, 
# MAGIC  last_mod_ts timestamp,  
# MAGIC  last_mod_user_id string 
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/stnd_form_paragraph_reason'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.qrtz_blob_triggers(
# MAGIC sched_name string,
# MAGIC trigger_name string,
# MAGIC trigger_group string,
# MAGIC blob_data string
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/qrtz_blob_triggers'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'=true,'delta.enablechangedatafeed'=true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.cdc_batch_job_control(
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string,
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   full_load string,
# MAGIC   initial_load_finished boolean
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/cdc_batch_job_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table  if not exists ${conf.catalog}.${conf.database}.cdc_batch_job_history(
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmngfpepp/bronze/cdc_batch_job_history'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------



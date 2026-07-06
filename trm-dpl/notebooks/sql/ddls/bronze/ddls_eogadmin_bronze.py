# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create EOGADMIN bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "test"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file="../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/eogadmin-conf.yaml"
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
eogadmin_catalog = common_configs['schema']['trgt_catalog']
src_folder=common_configs['cdc']['src_csv_files']
src_database=common_configs['cdc']['src_database']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.eogadmin_catalog', eogadmin_catalog.lower())
print(f'{eogadmin_catalog=},{src_folder=}, ,{src_database=}')

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', eogadmin_catalog)
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
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.FSM_INSTANCE 
# MAGIC (
# MAGIC fsm_instance_id              decimal(38,0),
# MAGIC fk_parent_fsm_instance_id    decimal(10,0),
# MAGIC fk_root_fsm_instance_id      decimal(10,0),
# MAGIC fk_fsm_type_id               decimal(5,0),
# MAGIC fk_current_fsm_type_state_id decimal(10,0),
# MAGIC terminated_in                string,
# MAGIC suspended_no                 decimal(2,0),
# MAGIC depth_no                     decimal(5,0),
# MAGIC create_ts                    timestamp,
# MAGIC create_user_id               string,
# MAGIC last_mod_ts                  timestamp,
# MAGIC last_mod_user_id             string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/FSM_INSTANCE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.FSM_INSTANCE_H 
# MAGIC (
# MAGIC fsm_instance_h_id             decimal(10,0),
# MAGIC fsm_instance_id               decimal(10,0),
# MAGIC fk_parent_fsm_instance_id     decimal(10,0),
# MAGIC fk_root_fsm_instance_id       decimal(10,0),
# MAGIC fk_fsm_type_id                decimal(5,0),
# MAGIC fk_current_fsm_type_state_id  decimal(10,0),
# MAGIC terminated_in                 string,
# MAGIC suspended_no                  decimal(2,0) ,
# MAGIC depth_no                      decimal(5,0) ,
# MAGIC create_ts                     timestamp,
# MAGIC create_user_id                string,
# MAGIC last_mod_ts                   timestamp,
# MAGIC last_mod_user_id              string,
# MAGIC end_effective_ts              timestamp,
# MAGIC begin_effective_ts            timestamp 
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/FSM_INSTANCE_H'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.FSM_INTERLOCK 
# MAGIC (
# MAGIC fsm_interlock_id         decimal(5,0),
# MAGIC fk_fsm_interlock_type_id decimal(5,0),
# MAGIC fk_fsm_root_type_id      decimal(5,0),
# MAGIC fk_fsm_trigger_type_id   decimal(5,0),
# MAGIC fk_fsm_trigger_state_id  decimal(10,0),
# MAGIC stnd_interlock_type_cd   string,
# MAGIC interlock_description_tx string,
# MAGIC create_ts                timestamp,
# MAGIC create_user_id           string   ,
# MAGIC last_mod_ts              timestamp,
# MAGIC last_mod_user_id         string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/FSM_INTERLOCK'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.OG_APPEAL_FSM_INSTANCE 
# MAGIC (
# MAGIC cfk_root_fsm_instance_id     decimal(10,0),
# MAGIC cfk_current_fsm_instance_id  decimal(10,0),
# MAGIC cfk_review_query_appeal_id   decimal(10,0),
# MAGIC create_ts                    timestamp ,
# MAGIC create_user_id               string,
# MAGIC last_mod_ts                  timestamp ,
# MAGIC last_mod_user_id             string    
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/OG_APPEAL_FSM_INSTANCE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.OG_REVIEW_FSM_INSTANCE 
# MAGIC (
# MAGIC cfk_root_fsm_instance_id     decimal(10,0),
# MAGIC cfk_current_fsm_instance_id  decimal(10,0),
# MAGIC create_ts                    timestamp,
# MAGIC create_user_id               string,
# MAGIC last_mod_ts                  timestamp,
# MAGIC last_mod_user_id             string,
# MAGIC cfk_og_trademark_review_id   decimal(10,0)
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/OG_REVIEW_FSM_INSTANCE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.OG_REVIEW_QUERY_FSM_INSTANCE 
# MAGIC (
# MAGIC cfk_current_fsm_instance_id decimal(10,0),
# MAGIC cfk_review_query_id         decimal(10,0),
# MAGIC create_ts                   timestamp,
# MAGIC create_user_id              string,
# MAGIC last_mod_ts                 timestamp,
# MAGIC last_mod_user_id            string,
# MAGIC cfk_root_fsm_instance_id    decimal(10,0)
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/OG_REVIEW_QUERY_FSM_INSTANCE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_BLOB_TRIGGERS 
# MAGIC (
# MAGIC sched_name     string,
# MAGIC trigger_name   string,
# MAGIC trigger_group  string,
# MAGIC blob_data      binary
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_BLOB_TRIGGERS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_CALENDARS 
# MAGIC (
# MAGIC sched_name     string,
# MAGIC calendar_name  string,
# MAGIC calendar       binary
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_CALENDARS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_CRON_TRIGGERS 
# MAGIC (
# MAGIC sched_name      string,
# MAGIC trigger_name    string,
# MAGIC trigger_group   string,
# MAGIC cron_expression string,
# MAGIC time_zone_id    string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_CRON_TRIGGERS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_FIRED_TRIGGERS 
# MAGIC (
# MAGIC sched_name         string,
# MAGIC entry_id           string,
# MAGIC trigger_name       string,
# MAGIC trigger_group      string,
# MAGIC instance_name      string,
# MAGIC fired_time         decimal(13,0),
# MAGIC sched_time         decimal(13,0),
# MAGIC priority           decimal(13,0),
# MAGIC state              string,
# MAGIC job_name           string,
# MAGIC job_group          string,
# MAGIC is_nonconcurrent   string,
# MAGIC requests_recovery  string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_FIRED_TRIGGERS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_JOB_DETAILS 
# MAGIC (
# MAGIC sched_name         string,
# MAGIC job_name           string,
# MAGIC job_group          string,
# MAGIC description        string,
# MAGIC job_class_name     string,
# MAGIC is_durable         string,
# MAGIC is_nonconcurrent   string,
# MAGIC is_update_data     string,
# MAGIC requests_recovery  string,
# MAGIC job_data           binary
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_JOB_DETAILS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_LOCKS 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  lock_name string 
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_LOCKS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_PAUSED_TRIGGER_GRPS 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  trigger_group string 
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_PAUSED_TRIGGER_GRPS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_SCHEDULER_STATE 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  instance_name string, 
# MAGIC  last_checkin_time decimal(13,0), 
# MAGIC  checkin_interval decimal(13,0) 
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_SCHEDULER_STATE' 
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_SIMPLE_TRIGGERS 
# MAGIC (
# MAGIC   sched_name string, 
# MAGIC  trigger_name string, 
# MAGIC  trigger_group string, 
# MAGIC  repeat_count decimal(7,0), 
# MAGIC  repeat_interval decimal(12,0), 
# MAGIC  times_triggered decimal(10,0) 
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_SIMPLE_TRIGGERS' 
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_SIMPROP_TRIGGERS 
# MAGIC (
# MAGIC sched_name     string,
# MAGIC trigger_name   string,
# MAGIC trigger_group  string,
# MAGIC str_prop_1     string,
# MAGIC str_prop_2     string,
# MAGIC str_prop_3     string,
# MAGIC int_prop_1     decimal(10,0),
# MAGIC int_prop_2     decimal(10,0),
# MAGIC long_prop_1    decimal(13,0),
# MAGIC long_prop_2    decimal(13,0),
# MAGIC dec_prop_1     decimal(13,4),
# MAGIC dec_prop_2     decimal(13,4),
# MAGIC bool_prop_1    string,
# MAGIC bool_prop_2    string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_SIMPROP_TRIGGERS' 
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.QRTZ_TRIGGERS
# MAGIC (
# MAGIC sched_name     string,
# MAGIC trigger_name   string,
# MAGIC trigger_group  string,
# MAGIC job_name       string,
# MAGIC job_group      string,
# MAGIC description    string,
# MAGIC next_fire_time decimal(13,0),
# MAGIC prev_fire_time decimal(13,0),
# MAGIC priority       decimal(13,0),
# MAGIC trigger_state  string,
# MAGIC trigger_type   string,
# MAGIC start_time     decimal(13,0),
# MAGIC end_time       decimal(13,0),
# MAGIC calendar_name  string,
# MAGIC misfire_instr  decimal(2,0),
# MAGIC job_data       string
# MAGIC ) 
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/QRTZ_TRIGGERS'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_DOMAIN 
# MAGIC (
# MAGIC domain_cd          string,
# MAGIC title_tx           string,
# MAGIC description_tx     string,
# MAGIC begin_effective_dt timestamp,
# MAGIC end_effective_dt   timestamp,
# MAGIC create_ts          timestamp,
# MAGIC create_user_id     string,
# MAGIC last_mod_ts        timestamp,
# MAGIC last_mod_user_id   string
# MAGIC )                    
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_DOMAIN'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_FSM_CATEGORY 
# MAGIC (
# MAGIC fsm_category_cd    string,
# MAGIC title_tx           string,
# MAGIC description_tx     string,
# MAGIC begin_effective_dt timestamp,
# MAGIC end_effective_dt   timestamp,
# MAGIC create_ts          timestamp,
# MAGIC create_user_id     string,
# MAGIC last_mod_ts        timestamp,
# MAGIC last_mod_user_id   string
# MAGIC ) 
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_FSM_CATEGORY' 
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_FSM_INTERLOCK 
# MAGIC (
# MAGIC fsm_interlock_id             decimal(5,0) ,
# MAGIC fk_interlock_fsm_type_id     decimal(5,0) ,
# MAGIC fk_root_fsm_type_id          decimal(5,0) ,
# MAGIC fk_trigger_fsm_type_id       decimal(5,0) ,
# MAGIC fk_trigger_fsm_type_state_id decimal(10,0),
# MAGIC fk_fsm_interlock_type_cd     string,
# MAGIC description_tx               string,
# MAGIC create_ts                    timestamp ,
# MAGIC create_user_id               string,
# MAGIC last_mod_ts                  timestamp ,
# MAGIC last_mod_user_id             string
# MAGIC ) 
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_FSM_INTERLOCK'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_FSM_INTERLOCK_TYPE 
# MAGIC (
# MAGIC fsm_interlock_type_cd  string,
# MAGIC title_tx               string,
# MAGIC description_tx         string,
# MAGIC create_ts              timestamp,
# MAGIC create_user_id         string,
# MAGIC last_mod_ts            timestamp,
# MAGIC last_mod_user_id       string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_FSM_INTERLOCK_TYPE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_FSM_TYPE 
# MAGIC (
# MAGIC fsm_type_id                   decimal(5,0),
# MAGIC fk_fsm_category_cd            string,
# MAGIC fk_precedent_fsm_type_id      decimal(5,0),
# MAGIC fk_initial_fsm_type_state_id  decimal(10,0),
# MAGIC fk_root_fsm_type_id           decimal(5,0),
# MAGIC title_tx                      string,
# MAGIC description_tx                string,
# MAGIC begin_effective_dt            timestamp,
# MAGIC end_effective_dt              timestamp,
# MAGIC create_ts                     timestamp,
# MAGIC create_user_id                string,
# MAGIC last_mod_ts                   timestamp,
# MAGIC last_mod_user_id              string  
# MAGIC ) 
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_FSM_TYPE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_FSM_TYPE_EVENT 
# MAGIC (
# MAGIC fsm_type_event_id  decimal(5,0),
# MAGIC fk_fsm_type_id     decimal(5,0),
# MAGIC title_tx           string,
# MAGIC description_tx     string,
# MAGIC create_ts          timestamp,
# MAGIC create_user_id     string,
# MAGIC last_mod_ts        timestamp,
# MAGIC last_mod_user_id   string
# MAGIC ) 
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_FSM_TYPE_EVENT'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_FSM_TYPE_STATE 
# MAGIC (
# MAGIC fsm_type_state_id      decimal(10,0),
# MAGIC fk_fsm_type_id         decimal(5,0) ,
# MAGIC fk_root_fsm_type_id    decimal(5,0) ,
# MAGIC title_tx               string,
# MAGIC state_start_in         string,
# MAGIC state_end_in           string,
# MAGIC description_tx         string,
# MAGIC human_activity_tx      string,
# MAGIC automated_activity_tx  string,
# MAGIC create_ts              timestamp,
# MAGIC create_user_id         string,
# MAGIC last_mod_ts            timestamp,
# MAGIC last_mod_user_id       string,
# MAGIC start_condition_tx     string
# MAGIC ) 
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_FSM_TYPE_STATE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_FSM_TYPE_STATE_RULE 
# MAGIC (
# MAGIC fsm_type_state_rule_id        decimal(5,0),
# MAGIC fk_fsm_type_id                decimal(5,0),
# MAGIC fk_root_fsm_type_id           decimal(5,0),
# MAGIC fk_current_fsm_type_state_id  decimal(10,0),
# MAGIC fk_next_fsm_type_state_id     decimal(10,0),
# MAGIC fk_fsm_type_event_id          decimal(5,0),
# MAGIC description_tx                string,
# MAGIC precondition_tx               string,
# MAGIC rule_action_tx                string,
# MAGIC create_ts                     timestamp,
# MAGIC create_user_id                string,
# MAGIC last_mod_ts                   timestamp,
# MAGIC last_mod_user_id              string
# MAGIC )
# MAGIC USING delta 
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_FSM_TYPE_STATE_RULE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.STND_INTERLOCK_TYPE 
# MAGIC (
# MAGIC stnd_interlock_type_cd  string,
# MAGIC title_tx                string,
# MAGIC description_tx          string,
# MAGIC create_ts               timestamp,
# MAGIC create_user_id          string,
# MAGIC last_mod_ts             timestamp,
# MAGIC last_mod_user_id        string
# MAGIC ) 
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/STND_INTERLOCK_TYPE' 
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.USER_PROFILE 
# MAGIC (
# MAGIC user_profile_id   decimal(10,0),
# MAGIC user_id           string,
# MAGIC profile_nm        string,
# MAGIC description_tx    string,
# MAGIC create_ts         timestamp ,
# MAGIC create_user_id    string,
# MAGIC last_mod_ts       timestamp ,
# MAGIC last_mod_user_id  string
# MAGIC )
# MAGIC USING delta 
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/USER_PROFILE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.USER_PROFILE_PREFERENCE 
# MAGIC (
# MAGIC fk_user_profile_id         decimal(10,0),
# MAGIC fk_domain_cd               string,
# MAGIC resource_nm                string,
# MAGIC preference_nm              string,
# MAGIC preference_value_tx        string,
# MAGIC create_ts                  timestamp,
# MAGIC create_user_id             string,
# MAGIC last_mod_ts                timestamp,
# MAGIC last_mod_user_id           string,
# MAGIC user_profile_preference_id decimal(10,0) 
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/USER_PROFILE_PREFERENCE'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.control_table};
# MAGIC
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.control_table} (
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string,
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   full_load string,
# MAGIC   initial_load_finished boolean
# MAGIC )location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/${conf.control_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

table_schema = spark.table(f'{eogadmin_catalog}.{database}.{control_table}').schema

table_data = [
    (src_folder+"/"+"FSM_INSTANCE", 
     eogadmin_catalog, 
     database,
     "fsm_instance",
     src_database,
     "FSM_INSTANCE",
     "fsm_instance_id",
     'N',
     False
    ),
     (src_folder+"/"+"FSM_INSTANCE_H", 
     eogadmin_catalog, 
     database,
     "fsm_instance_h",
     src_database,
     "FSM_INSTANCE_H",
     "fsm_instance_h_id",
     'N',
     False
    ),
     (src_folder+"/"+"FSM_INTERLOCK", 
     eogadmin_catalog, 
     database,
     "fsm_interlock",
     src_database,
     "FSM_INTERLOCK",
     "fsm_interlock_id",
     'N',
     False
    ),
     (src_folder+"/"+"OG_APPEAL_FSM_INSTANCE", 
     eogadmin_catalog, 
     database,
     "og_appeal_fsm_instance",
     src_database,
     "OG_APPEAL_FSM_INSTANCE",
     "cfk_review_query_appeal_id,cfk_root_fsm_instance_id",
     'N',
     False
    ),
     (src_folder+"/"+"OG_REVIEW_FSM_INSTANCE", 
     eogadmin_catalog, 
     database,
     "og_review_fsm_instance",
     src_database,
     "OG_REVIEW_FSM_INSTANCE",
     "cfk_review_query_id,cfk_root_fsm_instance_id",
     'N',
     False
    ),
    (src_folder+"/"+"OG_REVIEW_QUERY_FSM_INSTANCE", 
     eogadmin_catalog, 
     database,
     "og_review_query_fsm_instance",
     src_database,
     "OG_REVIEW_QUERY_FSM_INSTANCE",
     "cfk_review_query_id,cfk_root_fsm_instance_id",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_BLOB_TRIGGERS", 
     eogadmin_catalog, 
     database,
     "qrtz_blob_triggers",
     src_database,
     "QRTZ_BLOB_TRIGGERS",
     "sched_name,trigger_group,trigger_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_CALENDARS", 
     eogadmin_catalog, 
     database,
     "qrtz_calendars",
     src_database,
     "QRTZ_CALENDARS",
     "calendar_name,sched_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_CRON_TRIGGERS", 
     eogadmin_catalog, 
     database,
     "qrtz_cron_triggers",
     src_database,
     "QRTZ_CRON_TRIGGERS",
     "sched_name,trigger_group,trigger_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_FIRED_TRIGGERS", 
     eogadmin_catalog, 
     database,
     "qrtz_fired_triggers",
     src_database,
     "QRTZ_FIRED_TRIGGERS",
     "entry_id,sched_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_JOB_DETAILS", 
     eogadmin_catalog, 
     database,
     "qrtz_job_details",
     src_database,
     "QRTZ_JOB_DETAILS",
     "job_group,job_name,sched_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_LOCKS", 
     eogadmin_catalog, 
     database,
     "qrtz_locks",
     src_database,
     "QRTZ_LOCKS",
     "lock_name,sched_name",
     'Y',
     False
    ),
     (src_folder+"/"+"QRTZ_PAUSED_TRIGGER_GRPS", 
     eogadmin_catalog, 
     database,
     "qrtz_paused_trigger_grps",
     src_database,
     "QRTZ_PAUSED_TRIGGER_GRPS",
     "sched_name,trigger_group",
     'Y',
     False
    ),
     (src_folder+"/"+"QRTZ_SCHEDULER_STATE", 
     eogadmin_catalog, 
     database,
     "qrtz_scheduler_state",
     src_database,
     "QRTZ_SCHEDULER_STATE",
     "instance_name,sched_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_SIMPLE_TRIGGERS", 
     eogadmin_catalog, 
     database,
     "qrtz_simple_triggers",
     src_database,
     "QRTZ_SIMPLE_TRIGGERS",
     "sched_name,trigger_group,trigger_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_SIMPROP_TRIGGERS", 
     eogadmin_catalog, 
     database,
     "qrtz_simprop_triggers",
     src_database,
     "QRTZ_SIMPROP_TRIGGERS",
     "sched_name,trigger_group,trigger_name",
     'N',
     False
    ),
     (src_folder+"/"+"QRTZ_TRIGGERS", 
     eogadmin_catalog, 
     database,
     "qrtz_triggers",
     src_database,
     "QRTZ_TRIGGERS",
     "sched_name,trigger_group,trigger_name",
     'N',
     False
    ),
     (src_folder+"/"+"STND_DOMAIN", 
     eogadmin_catalog, 
     database,
     "stnd_domain",
     src_database,
     "STND_DOMAIN",
     "domain_cd",
     'N',
     False
    ),
     (src_folder+"/"+"STND_FSM_CATEGORY", 
     eogadmin_catalog, 
     database,
     "stnd_fsm_category",
     src_database,
     "STND_FSM_CATEGORY",
     "fsm_category_cd",
     'N',
     False
    ),
     (src_folder+"/"+"STND_FSM_INTERLOCK_TYPE", 
     eogadmin_catalog, 
     database,
     "stnd_fsm_interlock_type",
     src_database,
     "STND_FSM_INTERLOCK_TYPE",
     "FSM_INTERLOCK_TYPE_CD",
     'Y',
     False
    ),
     (src_folder+"/"+"STND_FSM_TYPE", 
     eogadmin_catalog, 
     database,
     "stnd_fsm_type",
     src_database,
     "STND_FSM_TYPE",
     "fsm_type_id",
     'N',
     False
    ),
     (src_folder+"/"+"STND_FSM_TYPE_EVENT", 
     eogadmin_catalog, 
     database,
     "stnd_fsm_type_event",
     src_database,
     "STND_FSM_TYPE_EVENT",
     "fsm_type_event_id",
     'N',
     False
    ),
     (src_folder+"/"+"STND_FSM_TYPE_STATE", 
     eogadmin_catalog, 
     database,
     "stnd_fsm_type_state",
     src_database,
     "STND_FSM_TYPE_STATE",
     "fsm_type_state_id",
     'N',
     False
    ),
     (src_folder+"/"+"STND_FSM_TYPE_STATE_RULE", 
     eogadmin_catalog, 
     database,
     "stnd_fsm_type_state_rule",
     src_database,
     "STND_FSM_TYPE_STATE_RULE",
     "fsm_type_state_rule_id",
     'N',
     False
    ),
     (src_folder+"/"+"USER_PROFILE", 
     eogadmin_catalog, 
     database,
     "user_profile",
     src_database,
     "USER_PROFILE",
     "user_profile_id",
     'N',
     False
    ),
     (src_folder+"/"+"USER_PROFILE_PREFERENCE", 
     eogadmin_catalog, 
     database,
     "user_profile_preference",
     src_database,
     "USER_PROFILE_PREFERENCE",
     "user_profile_preference_id",
     'N',
     False
    )
]
 
df = spark.createDataFrame(data=table_data,schema=table_schema)
display(df)
df.write.mode('overwrite').saveAsTable(f'{eogadmin_catalog}.{database}.{control_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-history table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.job_history_table};
# MAGIC
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.job_history_table} (
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/trm_eogadmin/bronze/${conf.job_history_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

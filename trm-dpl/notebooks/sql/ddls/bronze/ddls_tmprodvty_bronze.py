# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create prodvty bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tmprodvty-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 


# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
prodvty_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{prodvty_catalog=}, {data_quality_catalog=} ')
src_folder = common_configs['cdc']['src_csv_files']
src_database = common_configs['cdc']['src_database']
spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
spark.conf.set('config.prodvty_catalog', prodvty_catalog.lower())

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
catalog = prodvty_catalog
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', prodvty_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.prodvty_catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/trm_tmprodvty'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.prodvty_catalog}.${conf.database}.PRODUCTION_TRANSACTION(
# MAGIC production_credit_tran_id       DECIMAL(38,10),
# MAGIC cfk_object_gid                  string,
# MAGIC cfk_object_type_cd              string,
# MAGIC fk_generating_prodvty_actn_id   decimal,
# MAGIC fk_corrected_prodvty_actn_id    decimal,
# MAGIC unit_count_no                   decimal,
# MAGIC transaction_effective_dt        timestamp,
# MAGIC dn_worker_no                    string,
# MAGIC dn_worker_tm_organization_cd    string,
# MAGIC dn_worker_role_cd               string,
# MAGIC cfk_worker_gid                  string,
# MAGIC cfk_worker_tm_organization_gid  string,
# MAGIC cfk_worker_user_role_id         decimal,
# MAGIC dn_contributor_worker_no        string,
# MAGIC dn_contributor_worker_role_cd   string,
# MAGIC dn_contributor_tm_org_cd        string,
# MAGIC cfk_contributor_worker_gid      string,
# MAGIC cfk_contributor_user_role_id    decimal,
# MAGIC cfk_contributor_tm_org_gid      string,
# MAGIC cfk_bcr_pay_period_range_name   string,
# MAGIC dn_action_no                    string,
# MAGIC priority_in                     string,
# MAGIC transaction_ct                  string,
# MAGIC work_unit_cd                    string,
# MAGIC lock_control_no                 decimal,
# MAGIC create_ts                       timestamp,
# MAGIC create_user_id                  string,
# MAGIC last_mod_ts                     timestamp,
# MAGIC last_mod_user_id                string,
# MAGIC subsequent_action_in            string,
# MAGIC delete_in                       string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/PRODUCTION_TRANSACTION'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.prodvty_catalog}.${conf.database}.PRODUCTIVITY_ACTION(
# MAGIC productivity_action_id DECIMAL(38,10),
# MAGIC productivity_action_cd string,
# MAGIC sub_action_cd          string,
# MAGIC title_tx               string,
# MAGIC lock_control_no        decimal,
# MAGIC create_ts              timestamp,
# MAGIC create_user_id         string,
# MAGIC last_mod_ts            timestamp,
# MAGIC last_mod_user_id       string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/PRODUCTIVITY_ACTION'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.prodvty_catalog}.${conf.database}.WORKER_TIME_ENTRY(
# MAGIC worker_time_entry_id     DECIMAL(38,10),
# MAGIC cfk_pp_range_nm          string,
# MAGIC entry_date               timestamp,
# MAGIC cfk_worker_gid           string,
# MAGIC cfk_user_role_id         decimal,
# MAGIC cfk_tm_organization_gid  string,
# MAGIC regular_hours_qt         Decimal(5,2),
# MAGIC overtime_hours_qt        Decimal(5,2),
# MAGIC fk_task_cd               string,
# MAGIC lock_control_no          decimal,
# MAGIC create_ts                timestamp,
# MAGIC create_user_id           string,
# MAGIC last_mod_ts              timestamp,
# MAGIC last_mod_user_id         string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/WORKER_TIME_ENTRY'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.prodvty_catalog}.${conf.database}.PRODUCTION_TRANSACTION_ERRLOG(
# MAGIC ORA_ERR_NUMBER decimal(20,0),
# MAGIC ORA_ERR_MESG string,
# MAGIC ORA_ERR_ROWID string,
# MAGIC ORA_ERR_OPTYP string,
# MAGIC ORA_ERR_TAG string,
# MAGIC PRODUCTION_CREDIT_TRAN_ID decimal(20,0),
# MAGIC CFK_OBJECT_GID string,
# MAGIC CFK_OBJECT_TYPE_CD string,
# MAGIC FK_GENERATING_PRODVTY_ACTN_ID integer,
# MAGIC FK_CORRECTED_PRODVTY_ACTN_ID integer,
# MAGIC UNIT_COUNT_NO integer,
# MAGIC TRANSACTION_EFFECTIVE_DT TIMESTAMP,
# MAGIC DN_WORKER_NO string,
# MAGIC DN_WORKER_TM_ORGANIZATION_CD string,
# MAGIC DN_WORKER_ROLE_CD string,
# MAGIC CFK_WORKER_GID string,
# MAGIC CFK_WORKER_TM_ORGANIZATION_GID string,
# MAGIC CFK_WORKER_USER_ROLE_ID integer,
# MAGIC DN_CONTRIBUTOR_WORKER_NO string,
# MAGIC DN_CONTRIBUTOR_WORKER_ROLE_CD string,
# MAGIC DN_CONTRIBUTOR_TM_ORG_CD string,
# MAGIC CFK_CONTRIBUTOR_WORKER_GID string,
# MAGIC CFK_CONTRIBUTOR_USER_ROLE_ID integer,
# MAGIC CFK_CONTRIBUTOR_TM_ORG_GID string,
# MAGIC CFK_BCR_PAY_PERIOD_RANGE_NAME string,
# MAGIC DN_ACTION_NO string,
# MAGIC PRIORITY_IN string,
# MAGIC TRANSACTION_CT string,
# MAGIC WORK_UNIT_CD string,
# MAGIC LOCK_CONTROL_NO integer,
# MAGIC CREATE_TS timestamp,
# MAGIC CREATE_USER_ID string,
# MAGIC LAST_MOD_TS timestamp,
# MAGIC LAST_MOD_USER_ID string,
# MAGIC SUBSEQUENT_ACTION_IN string,
# MAGIC DELETE_IN string
# MAGIC )
# MAGIC USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/PRODUCTION_TRANSACTION_ERRLOG'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${config.prodvty_catalog}.${conf.database}.${conf.control_table};
# MAGIC
# MAGIC create table if not exists ${config.prodvty_catalog}.${conf.database}.${conf.control_table} (
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
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/${conf.control_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

table_schema = spark.table(f'{catalog}.{database}.{control_table}').schema

table_data = [
    (src_folder+"/"+"PRODUCTION_TRANSACTION", 
     catalog, 
     database,
     "production_transaction",
     src_database,
     "PRODUCTION_TRANSACTION",
     "production_credit_tran_id",
     False
    ),
    (src_folder+"/"+"PRODUCTIVITY_ACTION", 
     catalog, 
     database,
     "productivity_action",
     src_database,
     "PRODUCTIVITY_ACTION",
     "productivity_action_id",
     False
    ),
    (src_folder+"/"+"WORKER_TIME_ENTRY", 
     catalog, 
     database,
     "worker_time_entry",
     src_database,
     "WORKER_TIME_ENTRY",
     "worker_time_entry",
     False
    ) 
]


 
df = spark.createDataFrame(data=table_data,schema=table_schema)

display(df)

df.write.mode('append').saveAsTable(f'{catalog}.{database}.{control_table}')

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
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/${conf.job_history_table}'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

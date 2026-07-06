# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmintltm-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "../../"+dbutils.widgets.get("config_file").rstrip()
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmintltm_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmintltm_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('config.tmintltm_catalog', tmintltm_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmintltm_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}';
# MAGIC --CREATE CATALOG IF NOT EXISTS ${config.data_quality_catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${config.data_quality_catalog}';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %run ./ddls_tmintltm_bronze_cmn

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.base_appl_intl_reg (
# MAGIC CFK_TRADEMARK_GID	string,
# MAGIC FK_INTERNATIONAL_APPL_GID	string,
# MAGIC SEQUENCE_NO	int,
# MAGIC FK_INTERNATIONAL_REG_GID	string,
# MAGIC CFK_STATUS_CD	string,
# MAGIC STATUS_DT	DATE,
# MAGIC IB_RENEWAL_DT	DATE,
# MAGIC LOCK_CONTROL_NO	int,
# MAGIC CREATE_TS	TIMESTAMP,
# MAGIC CREATE_USER_ID	string,
# MAGIC LAST_MOD_TS	TIMESTAMP,
# MAGIC LAST_MOD_USER_ID	string
# MAGIC
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/base_appl_intl_reg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

#%run ./ddls_tmintltm_bronze_all

# COMMAND ----------

dbutils.notebook.exit(f"Completed executing ddls. ")

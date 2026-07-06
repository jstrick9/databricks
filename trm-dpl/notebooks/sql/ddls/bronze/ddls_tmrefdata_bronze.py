# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmrefdata-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmrefdata_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmrefdata_catalog=}, {data_quality_catalog=} ')

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', tmrefdata_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.cdc_batch_job_control (
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
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/cdc_batch_job_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.cdc_batch_job_history (
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/cdc_batch_job_history'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_type(
# MAGIC     CODE_TYPE_ID	decimal(20,0),
# MAGIC     CODE_TYPE_NM	string,
# MAGIC     CODE_TYPE_DESCRIPTION_TX	string,
# MAGIC     CODE_VALUE_DATATYPE_CT	string,
# MAGIC     CODE_VALUE_MAXIMUM_LEN	integer,
# MAGIC     CODE_VALUE_DEFAULT_TX	string,
# MAGIC     CODE_NAME_MAXIMUM_LEN	string,
# MAGIC     BEGIN_EFFECTIVE_DT	timestamp,
# MAGIC     END_EFFECTIVE_DT	timestamp,
# MAGIC     LOCK_CONTROL_NO	integer,
# MAGIC     CREATE_TS	timestamp,
# MAGIC     CREATE_USER_ID	string,
# MAGIC     LAST_MOD_TS	timestamp,
# MAGIC     LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_type_62623(
# MAGIC   CODE_TYPE_ID	decimal(20,0),
# MAGIC   CODE_TYPE_NM	string,
# MAGIC   CODE_TYPE_DESCRIPTION_TX	string,
# MAGIC   CODE_VALUE_DATATYPE_CT	string,
# MAGIC   CODE_VALUE_MAXIMUM_LEN	integer,
# MAGIC   CODE_VALUE_DEFAULT_TX	string,
# MAGIC   CODE_NAME_MAXIMUM_LEN	string,
# MAGIC   BEGIN_EFFECTIVE_DT	timestamp,
# MAGIC   END_EFFECTIVE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_type_62623'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_type_dependency(
# MAGIC   FK_ENABLING_CODE_TYPE_ID	decimal(20,0),
# MAGIC   FK_DEPENDENT_CODE_TYPE_ID	decimal(20,0),
# MAGIC   DESCRIPTION_TX	string,
# MAGIC   CARDINALITY_CT	string,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_type_dependency'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_type_dependency_62623(
# MAGIC   FK_ENABLING_CODE_TYPE_ID	decimal(20,0),
# MAGIC   FK_DEPENDENT_CODE_TYPE_ID	decimal(20,0),
# MAGIC   DESCRIPTION_TX	string,
# MAGIC   CARDINALITY_CT	string,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_type_dependency_62623'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_type_domain_service(
# MAGIC   FK_CODE_TYPE_ID	decimal(20,0),
# MAGIC   FK_DOMAIN_SERVICE_ID	decimal(20,0),
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_type_domain_service'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_type_property_type(
# MAGIC   PROPERTY_TYPE_ID	decimal(20,0),
# MAGIC   FK_CODE_TYPE_ID	decimal(20,0),
# MAGIC   PROPERTY_TYPE_NM	string,
# MAGIC   PROPERTY_TYPE_DESC_TX	string,
# MAGIC   PROPERTY_TYPE_DATATYPE_CT	string,
# MAGIC   PROPERTY_TYPE_MAXIMUM_LEN	integer,
# MAGIC   MANDATORY_IN	string,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_type_property_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_type_property_type_62623(
# MAGIC   PROPERTY_TYPE_ID	decimal(20,0),
# MAGIC   FK_CODE_TYPE_ID	decimal(20,0),
# MAGIC   PROPERTY_TYPE_NM	string,
# MAGIC   PROPERTY_TYPE_DESC_TX	string,
# MAGIC   PROPERTY_TYPE_DATATYPE_CT	string,
# MAGIC   PROPERTY_TYPE_MAXIMUM_LEN	integer,
# MAGIC   MANDATORY_IN	string,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_type_property_type_62623'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_value(
# MAGIC   CODE_VALUE_ID	decimal(20,0),
# MAGIC   FK_CODE_TYPE_ID	decimal(20,0),
# MAGIC   SEQUENCE_NO	integer,
# MAGIC   MODIFICATION_NO	integer,
# MAGIC   CODE_VALUE_TX	string,
# MAGIC   CODE_NM	string,
# MAGIC   CODE_DESCRIPTION_TX	string,
# MAGIC   BEGIN_EFFECTIVE_DT	timestamp,
# MAGIC   END_EFFECTIVE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_value'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_value_62623(
# MAGIC   CODE_VALUE_ID		decimal(20,0),
# MAGIC   FK_CODE_TYPE_ID		decimal(20,0),
# MAGIC   SEQUENCE_NO	integer,
# MAGIC   MODIFICATION_NO	integer,
# MAGIC   CODE_VALUE_TX	string,
# MAGIC   CODE_NM	string,
# MAGIC   CODE_DESCRIPTION_TX	string,
# MAGIC   BEGIN_EFFECTIVE_DT	timestamp,
# MAGIC   END_EFFECTIVE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_value_62623'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_value_bak(
# MAGIC   CODE_VALUE_ID		decimal(20,0),
# MAGIC   FK_CODE_TYPE_ID		decimal(20,0),
# MAGIC   SEQUENCE_NO	integer,
# MAGIC   MODIFICATION_NO	integer,
# MAGIC   CODE_VALUE_TX	string,
# MAGIC   CODE_NM	string,
# MAGIC   CODE_DESCRIPTION_TX	string,
# MAGIC   BEGIN_EFFECTIVE_DT	timestamp,
# MAGIC   END_EFFECTIVE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_value_bak'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_value_dependency(
# MAGIC   FK_PARENT_CODE_VALUE_ID	decimal(20,0),
# MAGIC   FK_CHILD_CODE_VALUE_ID	decimal(20,0),
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_value_dependency'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_value_dependency_62623(
# MAGIC   FK_PARENT_CODE_VALUE_ID	decimal(20,0),
# MAGIC   FK_CHILD_CODE_VALUE_ID	decimal(20,0),
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_value_dependency_62623'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_value_property(
# MAGIC   FK_PROPERTY_TYPE_ID	decimal(20,0),
# MAGIC   FK_CODE_VALUE_ID	decimal(20,0),
# MAGIC   TEXT_VALUE_TX	string,
# MAGIC   NUMERIC_VALUE_NO	integer,
# MAGIC   DATE_VALUE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_value_property'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.code_value_property_62623(
# MAGIC   FK_PROPERTY_TYPE_ID	decimal(20,0),
# MAGIC   FK_CODE_VALUE_ID	decimal(20,0),
# MAGIC   TEXT_VALUE_TX	string,
# MAGIC   NUMERIC_VALUE_NO	integer,
# MAGIC   DATE_VALUE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/code_value_property_62623'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.domain_service(
# MAGIC   DOMAIN_SERVICE_ID	decimal(20,0),
# MAGIC   DOMAIN_SERVICE_CD	string,
# MAGIC   DESCRIPTION_TX	string,
# MAGIC   BEGIN_EFFECTIVE_DT	timestamp,
# MAGIC   END_EFFECTIVE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/domain_service'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.domain_service_complete_list(
# MAGIC   DOMAIN_SERVICE_ID	decimal(20,0),
# MAGIC   DOMAIN_SERVICE_CD	string,
# MAGIC   DESCRIPTION_TX	string,
# MAGIC   BEGIN_EFFECTIVE_DT	timestamp,
# MAGIC   END_EFFECTIVE_DT	timestamp,
# MAGIC   LOCK_CONTROL_NO	integer,
# MAGIC   CREATE_TS	timestamp,
# MAGIC   CREATE_USER_ID	string,
# MAGIC   LAST_MOD_TS	timestamp,
# MAGIC   LAST_MOD_USER_ID	string
# MAGIC ) USING delta
# MAGIC location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/domain_service_complete_list'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true);
# MAGIC

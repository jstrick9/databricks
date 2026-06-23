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
data_quality_db = common_configs['schema']['data_quality_catalog']
spark.conf.set('conf.data_quality_db', data_quality_db)
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
print(f'{data_quality_db=} ')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.data_quality_db}

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.data_quality_db};
# MAGIC create schema if not exists  silver;
# MAGIC use silver;

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE TABLE ${conf.data_quality_db}.SILVER.CMN_CATALOG_RFRNC_STG
# MAGIC (SRC_SYS_NAME STRING COMMENT 'Source system name of the process',
# MAGIC SOURCE_DB_NAME STRING COMMENT 'Name of source db',
# MAGIC SRC_TBL_NAME STRING COMMENT 'Name of source tables where data is read from as part of the process',
# MAGIC TARGET_CATALOG_NAME STRING COMMENT 'Name of Target Catalog',
# MAGIC TARGET_DB_NAME STRING COMMENT 'Name of target db',
# MAGIC TRGT_TBL_NAME STRING COMMENT 'Name of Target tables where data is loaded as part of the process' ,
# MAGIC OBJECT_TYPE STRING COMMENT 'Object Code to Identify Database object type and it is set to T for table or V for View')
# MAGIC USING CSV
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/static_files/cmn_catalog_rfrnc_stg'
# MAGIC COMMENT 'Common stage table with information on USPTO databricks catalogs processes';

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE OR REPLACE TABLE ${conf.data_quality_db}.SILVER.CMN_CATALOG_RFRNC
# MAGIC (SOURCE_DB_NAME STRING COMMENT 'Name of source db',
# MAGIC TARGET_CATALOG_NAME STRING COMMENT 'Name of Target Catalog',
# MAGIC TARGET_DB_NAME STRING COMMENT 'Name of target db',
# MAGIC CNCTN_DTL_DESC STRING COMMENT 'Name of source db connection secret/string',
# MAGIC SRC_TBL_NAME STRING COMMENT 'Name of source tables where data is read from as part of the process',
# MAGIC TRGT_TBL_NAME STRING COMMENT 'Name of Target tables where data is loaded as part of the process' ,
# MAGIC IN_DBX_IND STRING COMMENT 'Indicator is set to Y if table is present in DBX and vice-a-versa',
# MAGIC OBJECT_TYPE STRING COMMENT 'Object Type to Identify Database object type',
# MAGIC SRC_SYS_NAME STRING COMMENT 'Source system name of the process',
# MAGIC PROC_CTGRY_CD STRING COMMENT 'Process category code',
# MAGIC AUDT_INSRT_ID STRING COMMENT 'Data lineage column to identify who inserted the record ',
# MAGIC AUDT_INSRT_TS TIMESTAMP COMMENT 'Data lineage column to identify when the user insertred the record'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (SRC_SYS_NAME)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.data_quality_db}/silver/cmn_catalog_rfrnc'
# MAGIC COMMENT 'Common table with information on USPTO databricks catalogs processes';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.data_quality_db}.SILVER.CMN_CATALOG_SIZE_RFRNC
# MAGIC (
# MAGIC SRC_SYS_NAME STRING COMMENT 'Source system name of the process',
# MAGIC TOTAL_SIZE_IN_BYTES STRING COMMENT 'Source SIZE IN BYTES',
# MAGIC AUDT_UPDT_TS TIMESTAMP COMMENT 'Data lineage column to identify when the user insertred the record'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (SRC_SYS_NAME)
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.data_quality_db}/silver/cmn_catalog_size_rfrnc'
# MAGIC COMMENT 'Common table with information on USPTO databricks catalog sizes';

# COMMAND ----------



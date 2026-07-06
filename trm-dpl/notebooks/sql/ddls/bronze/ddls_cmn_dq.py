# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

# MAGIC %sql update  trm_efoiap.bronze.cdc_batch_job_control set initial_load_finished = false;

# COMMAND ----------

dbutils.widgets.text("config_file","../notebooks/config/prod/tmworker-conf.yaml")
config_file = "../../../"+dbutils.widgets.get("config_file").rstrip()
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../../../notebooks/python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs=read_yaml(config_file)
data_quality_catalog = common_configs['schema']['data_quality_catalog']
cdc_bucket = configs['cdc']['cdc_bucket']
spark.conf.set('config.cdc_bucket', cdc_bucket)
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${config.data_quality_db}

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${config.data_quality_db}.SILVER 
# MAGIC COMMENT 'Schema created for data quality tables' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC
# MAGIC (PROC_ID BIGINT not null generated always as identity COMMENT 'System generated unique identifier for a process',
# MAGIC PRNT_PROC_ID INT COMMENT 'Identifier used to identify the parent process for a process',
# MAGIC PROC_NAME STRING COMMENT 'Name of the process',
# MAGIC PROC_DESC STRING COMMENT 'Detailed explanation of the process',
# MAGIC PROC_CTGRY_CD STRING COMMENT 'Category of the process',
# MAGIC PROC_CTGRY_DESC STRING COMMENT 'Detailed explanation of the process category',
# MAGIC PROC_CNFG_FILE_PATH STRING COMMENT 'Process configuration file path',
# MAGIC SRC_TBL_NAME STRING COMMENT 'Name of source tables where data is read from as part of the process',
# MAGIC TRGT_TBL_NAME STRING COMMENT 'Name of Target tables where data is loaded as part of the process' ,
# MAGIC SRC_SYS_NAME STRING COMMENT 'Source system name of the process',
# MAGIC AUDT_INSRT_ID STRING COMMENT 'Data lineage column to identify who inserted the record ',
# MAGIC AUDT_INSRT_TS TIMESTAMP COMMENT 'Data lineage column to identify when the user insertred the record',
# MAGIC AUDT_UPDT_ID STRING COMMENT 'Data lineage column to identify who inserted or updated the record ',
# MAGIC AUDT_UPDT_TS TIMESTAMP COMMENT 'Data lineage column to identify when the user insertred or updated the record')
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (SRC_SYS_NAME)
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.data_quality_db}/silver/cmn_proc_defn_rfrnc'
# MAGIC COMMENT 'Common table with information on USPTO ETL processes';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.data_quality_db}.SILVER.CMN_DQ_VRFCTN_QUERY_RFRNC
# MAGIC (QUERY_NAME STRING COMMENT 'Identifier used to identify the data quality check query',
# MAGIC QUERY_DESC STRING COMMENT 'Detailed explanation of the data quality query',
# MAGIC CNCTN_DTL_DESC STRING COMMENT 'Name of database or connection where the query is executed',
# MAGIC QUERY_TEXT STRING COMMENT 'This Column contains the data quality check sql query',
# MAGIC SRC_SYS_NAME STRING COMMENT 'Source system name of the process')
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (SRC_SYS_NAME)
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.data_quality_db}/silver/cmn_dq_vrfctn_query_rfrnc'
# MAGIC COMMENT 'Data quality check table containing sql queries to verify the quality of data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.data_quality_db}.SILVER.CMN_PROC_VRFCTN_QUERY_ASCTN
# MAGIC (PROC_NAME STRING COMMENT 'Name of the process',
# MAGIC QUERY_SET_ID INT COMMENT 'Identifier used to identify order of queries for each process. If a process has only one source and target query then the value is 1 or else increment the number by 1 for each query set',
# MAGIC QUERY_DQ_CD STRING COMMENT 'Code to identify the type of data quality verification check. CM=CountMatch SM=SampleMatch RM=RuleMatch',
# MAGIC TRGT_QUERY_NAME STRING COMMENT 'Name of data quality query used to verify the Target counts',
# MAGIC SRC_QUERY_NAME STRING COMMENT 'Name of data quality query used to verify the source counts',
# MAGIC QUERY_SET_DESC STRING COMMENT 'Detailed description of the query set',
# MAGIC ERR_THRSHLD_PCT FLOAT COMMENT 'Error threshold percentage used to define the error level at which the data quality issue needs to be reported',
# MAGIC SRC_SYS_NAME STRING COMMENT 'Source system name of the process')
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (SRC_SYS_NAME)
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.data_quality_db}/silver/cmn_proc_vrfctn_query_asctn'
# MAGIC COMMENT 'Data quality check table containing sql query associations to each process';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${config.data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT
# MAGIC (PROC_VRFCTN_RSLT_ID BIGINT not null generated always as identity COMMENT 'System generated unique identifier for the data quality process run',
# MAGIC PROC_ID INT COMMENT 'System generated unique identifier for a process',
# MAGIC PROC_NAME STRING COMMENT 'Name of the process',
# MAGIC PROC_CTGRY_CD STRING COMMENT 'Category of the process',
# MAGIC QUERY_SET_ID INT COMMENT 'Identifier used to identify order of queries for each process. If a process has only one source and target query then the value is 1 or else increment the number by 1 for each query set',
# MAGIC QUERY_DQ_CD STRING COMMENT 'Code to identify the type of data quality verification check. CM=CountMatch SM=SampleMatch RM=RuleMatch',
# MAGIC SRC_QUERY_NAME STRING COMMENT 'Name of data quality query used to verify the source counts',
# MAGIC TRGT_QUERY_NAME STRING COMMENT 'Name of data quality query used to verify the Target counts',
# MAGIC JOB_LOG_ID BIGINT COMMENT 'System generated unique identifier for each process job run',
# MAGIC JOB_START_TS TIMESTAMP COMMENT 'Start Timestamp of job',
# MAGIC RPTD_SRC_RSLT_CNT BIGINT COMMENT 'Reported source result counts captured after executing the source data quality check query',
# MAGIC RPTD_TRGT_RSLT_CNT BIGINT COMMENT 'Reported target result counts captured after executing the target data quality check query',
# MAGIC ERR_THRSHLD_PCT FLOAT COMMENT 'Error threshold percentage used to define the error level at which the data quality issue needs to be reported',
# MAGIC RPTD_VRNC_PCT FLOAT COMMENT 'Reported Variance percentage calculated from the difference between the source and target result counts',
# MAGIC DQ_RSLT_MSG STRING COMMENT 'Description for reported results after data quality execution',
# MAGIC AUDT_INSRT_ID STRING COMMENT 'Data lineage column to identify who has insertred or updated the record',
# MAGIC AUDT_INSRT_TS TIMESTAMP COMMENT 'Data lineage column to identify when the user or process has insertred or updated the record',
# MAGIC SRC_SYS_NAME STRING COMMENT 'Source system name of the process'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (SRC_SYS_NAME)
# MAGIC LOCATION 's3://${config.cdc_bucket}/eds/delta_tables/${config.data_quality_db}/silver/cmn_proc_vrfctn_rslt'
# MAGIC COMMENT 'Data quality check table containing results of data quality query executions for each completed process job run';

# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create proceeding bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/proceeding-conf.yaml"
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
proceeding_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{proceeding_catalog=}, {data_quality_catalog=} ')
src_folder = common_configs['cdc']['src_csv_files']
src_database = common_configs['cdc']['src_database']
spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
spark.conf.set('config.proceeding_catalog', proceeding_catalog.lower()) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
catalog = proceeding_catalog
spark.conf.set('conf.catalog', proceeding_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)


# COMMAND ----------

# Define a dictionary of column comments
TABLE_COLUMNS = {
'PROCEEDING':'CFK_SUBMISSION_METHOD_CD',
'PROCEEDING_H':'CFK_SUBMISSION_METHOD_CD',
'PROCEEDING_INTL_APPL':'DN_INTERNATIONAL_US_REF_NO',
'PROCEEDING_INTL_APPL_H':'DN_INTERNATIONAL_US_REF_NO',
'PROCEEDING_MARK':'DN_SERIAL_NUM',
'PROCEEDING_MARK_H':'DN_SERIAL_NUM',
}

# Loop through the columns and generate column comment queries
for table, column in TABLE_COLUMNS.items():
    add_column_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ADD COLUMN {column} STRING
    """.format(catalog=catalog, database=database, table=table, column=column)
  
    try:
        spark.sql(add_column_query)
    except Exception as e:
        print("Exception message: {}".format(e))

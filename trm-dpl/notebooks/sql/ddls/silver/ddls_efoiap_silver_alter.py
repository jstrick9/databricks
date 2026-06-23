# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "efoiap-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
efoiap_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{efoiap_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', efoiap_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'silver'
control_table = 'job_control'
job_history_table = 'job_log'

spark.conf.set('conf.catalog', efoiap_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

tables_to_comment = {
    'job_control':'The job_control table contains information about each job being executed in the system. It includes the unique identifier for each job, the name of the job, the timestamps for when the job was loaded into the system, created, and last modified. Additionally, it includes the user ID of the individual who created and last modified the job. This table is essential for tracking and managing the execution of jobs within the business.',
    
    'job_log':'The job_log table contains information about each job log entry in the system. It provides a unique identifier for each job log entry, along with the name of the job, start and end timestamps indicating the duration of the job, a status code indicating the status of the job, the number of records in the source and target datasets, and any additional comments or notes about the job. This table is significant to the business as it allows for tracking and monitoring of job executions, providing insights into job performance and any potential issues or errors that may have occurred.'
}

for table_name, comment in tables_to_comment.items():
    alter_table_query = f"""
    ALTER TABLE {spark.conf.get('conf.catalog')}.{spark.conf.get('conf.database')}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'job_control_id': 'Unique identifier for the job control entry',
    'job_nm': 'Name of the job',
    'load_ts': 'Timestamp when the job was loaded',
    'create_ts': 'Timestamp when the job control entry was created',
    'create_user_id': 'User ID of the individual who created the job control entry',
    'last_mod_ts': 'Timestamp when the job control entry was last modified',
    'last_mod_user_id': 'User ID of the individual who last modified the job control entry'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'job_log_id': 'Unique identifier for the job log entry',
    'job_nm': 'Name of the job',
    'start_ts': 'Timestamp when the job started',
    'end_ts': 'Timestamp when the job ended',
    'status_ct': 'Status count of the job',
    'src_cnt': 'Source count of the job',
    'trgt_cnt': 'Target count of the job',
    'comment_tx': 'Comment for the job log entry'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='job_log', column_name=column, comment=comment)

  spark.sql(column_comment_query)

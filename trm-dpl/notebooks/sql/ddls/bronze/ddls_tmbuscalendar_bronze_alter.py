# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmbuscalendar-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmbuscalendar_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmbuscalendar_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmbuscalendar_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'

spark.conf.set('conf.catalog', tmbuscalendar_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://bdr-databricks-app-${conf.dbx_env}/eds/delta_tables/${conf.catalog}';
# MAGIC --CREATE CATALOG IF NOT EXISTS ${config.data_quality_catalog} MANAGED LOCATION 's3://bdr-databricks-app-${conf.dbx_env}/eds/delta_tables/${config.data_quality_catalog}';

# COMMAND ----------

# MAGIC %md
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

tables_to_comment = {
    'business_calendar_range': 'The business_calendar_range table contains data related to the different types of ranges in the business calendar. It provides information about the range type code, start and end dates of the range, range name, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for the management and tracking of various ranges in the business calendar, providing important information for scheduling and planning purposes.',

    'bus_calendar_day_property': 'The bus_calendar_day_property table contains data related to the properties of calendar days in the business. It includes information such as the date of the calendar day, the type of property, the value of the property, and the user who created or modified the property. This table is important for tracking and managing various properties associated with calendar days, allowing the business to make informed decisions based on these properties.',

    'business_calendar_day': 'The business_calendar_day table contains information about each calendar day in the business calendar. It includes the date, fiscal year number, fiscal quarter number, lock control number, create timestamp, create user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it provides a comprehensive record of all business days and their corresponding fiscal year and quarter. It also tracks any modifications made to the calendar. The data in this table is crucial for various business operations such as financial reporting, forecasting, and scheduling.',

    'cdc_batch_job_control': 'The cdc_batch_job_control table in the bronze schema of the trm_tmbuscalendar_dev catalog is used to control and track the Change Data Capture (CDC) batch jobs. It contains information about the source folder, catalog name, database name, and table name for each job. Additionally, it includes the source database name and source table name, which represent the original data source. The primary keys column stores the primary keys for each table, while the full_load column indicates whether a full load is required. The initial_load_finished column is a boolean value that shows whether the initial load has been completed. This table is essential for managing and monitoring the CDC process in the business system.',

    'cdc_batch_job_history' : 'The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs. It stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. This table is significant to the business as it allows tracking and monitoring of CDC batch job activities, providing insights into data changes and updates. The data in this table represents the historical records of CDC batch jobs, enabling analysis and troubleshooting of data synchronization processes.'
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
'cfk_range_type_cd':'Code representing the type of range',
'fk_start_calendar_dt':'Start date of the calendar range',
'fk_end_calendar_dt':'End date of the calendar range',
'range_nm':'Name of the range',
'lock_control_no':'Number used for lock control',
'create_ts':'Timestamp of when the record was created',
'create_user_id':'User ID of the user who created the record',
'last_mod_ts':'Timestamp of when the record was last modified',
'last_mod_user_id':'User ID of the user who last modified the record'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmbuscalendar_catalog, database=database, table='business_calendar_range', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'FK_CALENDAR_DT':'The timestamp of the calendar day',
'CFK_PROPERTY_TYPE_CD':'The code representing the type of property',
'PROPERTY_VALUE_IN':'The string value of the property',
'PROPERTY_VALUE_DT':'The timestamp value of the property',
'PROPERTY_VALUE_TX':'The text value of the property',
'LOCK_CONTROL_NO':'The integer value for lock control',
'CREATE_TS':'The timestamp when the property was created',
'CREATE_USER_ID':'The user ID who created the property',
'LAST_MOD_TS':'The timestamp when the property was last modified',
'LAST_MOD_USER_ID':'The user ID who last modified the property',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmbuscalendar_catalog, database=database, table='bus_calendar_day_property', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'CALENDAR_DT':'The date of the calendar day in the business calendar',
'FISCAL_YEAR_NO':'The fiscal year number corresponding to the calendar day',
'FISCAL_QUARTER_NO':'The fiscal quarter number corresponding to the calendar day',
'LOCK_CONTROL_NO':'The lock control number for the calendar day',
'CREATE_TS':'The timestamp when the calendar day was created',
'CREATE_USER_ID':'The user ID of the user who created the calendar day',
'LAST_MOD_TS':'The timestamp of the last modification made to the calendar day',
'LAST_MOD_USER_ID':'The user ID of the user who made the last modification to the calendar day'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmbuscalendar_catalog, database=database, table='business_calendar_day', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'src_folder':'The folder where the source data is stored.',
'catalog_name':'The name of the catalog where the table is located.',
'database_name':'The name of the database where the table is located.',
'table_name':'The name of the table.',
'source_db_name':'The name of the source database.',
'source_table_name':'The name of the source table.',
'primary_keys':'The primary keys of the table.',
'full_load':'Indicates if the table needs to be fully loaded.',
'initial_load_finished':'Indicates if the initial load of the table has finished.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmbuscalendar_catalog, database=database, table='cdc_batch_job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'cdc_file_path':'The file path of the CDC file.',
'meta_src_time':'The source time of the metadata.',
'cdc_file_date':'The date of the CDC file.',
'processing_time':'The processing time of the job.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmbuscalendar_catalog, database=database, table='cdc_batch_job_history', column_name=column, comment=comment)

  spark.sql(column_comment_query)

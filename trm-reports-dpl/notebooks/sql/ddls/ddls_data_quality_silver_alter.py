# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f"{data_quality_catalog=}")
spark.conf.set('conf.catalog', data_quality_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

database = 'silver'
control_table = 'job_control'
job_history_table = 'job_log'

spark.conf.set('conf.catalog', data_quality_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

tables_to_comment = {

    'cmn_catalog_rfrnc':'Common table with information on USPTO databricks catalogs processes',

    'cmn_catalog_rfrnc_stg':'Common stage table with information on USPTO databricks catalogs processes',

    'cmn_dq_vrfctn_query_rfrnc':'Data quality check table containing sql queries to verify the quality of data',
    
    'cmn_proc_defn_rfrnc':'Common table with information on USPTO ETL processes',

    'cmn_proc_vrfctn_query_asctn':'Data quality check table containing sql query associations to each process',
    
    'cmn_proc_vrfctn_rslt':'The cmn_proc_vrfctn_rslt table contains information about the data quality verification results for various processes. It includes the unique identifier for each process, the name and category of the process, the order of queries for each process, and the type of data quality verification check. Additionally, it captures the names of the data quality queries used to verify the source and target counts, as well as the reported result counts after executing these queries. The table also includes information about the error threshold percentage, reported variance percentage, and a description of the reported results. Data lineage columns are present to track who inserted or updated the records, along with the corresponding timestamps. Finally, the table includes the source system name of the process. This table is essential for monitoring and analyzing data quality issues in various processes.',

    'cmn_proc_vrfctn_rslt_old':'The cmn_proc_vrfctn_rslt_old table contains data quality verification results for various processes. It includes information such as the process ID, process name, category, and the order of queries for each process. The table also includes data quality codes to identify the type of verification check, as well as the names of the queries used to verify the source and target counts. Additionally, it captures the reported result counts, error threshold percentage, variance percentage, and a description of the reported results. The table also includes data lineage columns to track who inserted or updated the records, as well as the timestamp of the insertion or update. Lastly, it includes the source system name of the process. This table is essential for monitoring and analyzing data quality issues in various processes.',
    
    'efoiap_refresh_freq':'The efoiap_refresh_freq table in the data_quality_dev catalog of the silver schema contains information about the refresh frequency of various tables in the system. It includes the source system name, table name, the timestamp when the table was last refreshed from the reported source, and the timestamp when the table was last updated. This table is important for monitoring and tracking the freshness of data in the system, ensuring that the data is up-to-date and reliable for business operations.',

    'eogadmin_refresh_freq':'The eogadmin_refresh_freq table in the data_quality_dev catalog of the silver schema contains information about the refresh frequency of various tables in the EOG administration system. It includes the source system name, table name, the timestamp when the source system was last refreshed, and the timestamp when the table was last updated. This table is important for monitoring and tracking the data freshness in the EOG administration system, ensuring that the data is up-to-date and accurate for business operations.',

    'jbteasps_refresh_freq':'The jbteasps_refresh_freq table in the silver schema of the data_quality_dev catalog contains information about the refresh frequency of various tables in the system. It includes the source system name, table name, the timestamp of the last refresh reported by the source system, and the timestamp of the last update made to the table. This table is important for monitoring and tracking the freshness of data in the system, ensuring that the tables are regularly refreshed and up to date. It provides valuable insights into the data quality and reliability of the system.',

    'proceeding_refresh_freq':'The proceeding_refresh_freq table provides information about the refresh frequency of reported data from the source systems. It includes the name of the source system, the name of the table, and the timestamps for the last refresh of the source data and the last update of the table. This table is significant to the business as it helps track and monitor the timeliness of data updates, ensuring data accuracy and reliability for reporting and analysis purposes.',

    'tmbuscalendar_refresh_freq':'The tmbuscalendar_refresh_freq table contains information about the refresh frequency of the TMBusCalendar data. It includes the source system name, table name, and timestamps for the last refresh from the reported source and the last update. This table is important for tracking the timeliness and accuracy of the TMBusCalendar data, ensuring that it is up to date and reliable for business operations. The timestamps provide insights into when the data was last refreshed and updated, allowing for effective monitoring and troubleshooting of any potential issues.',

    'tmintltm_refresh_freq':'The tmintltm_refresh_freq table in the data_quality_dev database schema represents the refresh frequency of various tables in the system. It contains information about the source system name, table name, and the timestamps of the last refresh and last update. This table is significant to the business as it helps track and monitor the frequency at which data in different tables is refreshed, ensuring data accuracy and timeliness. It provides valuable insights into the data quality and reliability of the system.',

    'tmngfpepp_refresh_freq':'The tmngfpepp_refresh_freq table provides information about the refresh frequency of various tables in the system. It includes the source system name, table name, and the timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and managing data quality, as it allows the business to track when tables were last refreshed and updated. It helps ensure that data is up-to-date and reliable for decision-making processes.',
    
    'tmngidmp_refresh_freq':'The tmngidmp_refresh_freq table in the data_quality_dev catalog of the silver schema contains information about the refresh frequency of data sources in the system. It includes the source system name, table name, and the timestamp of the last refresh from the reported source. The table also includes the timestamp of the last update made to the table itself. This information is crucial for monitoring and managing data quality, ensuring that the data sources are up to date and accurate for business operations.',

    'tmngpdb_group10_refresh_freq':'The tmngpdb_group10_refresh_freq table provides information about the refresh frequency of various tables in the tmngpdb_group10 database. It includes the source system name, table name, and the timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and ensuring data quality by tracking when tables were last refreshed and updated. It helps in identifying any potential data issues or delays in data availability.',

    'tmngpdb_group11_refresh_freq':'The tmngpdb_group11_refresh_freq table provides information about the refresh frequency of various tables in the tmngpdb_group11 database. It includes the source system name, table name, and timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and ensuring the data quality of the tmngpdb_group11 database, as it allows tracking when tables were last refreshed and updated. The timestamps provide valuable insights into the timeliness and reliability of the data in the database.',

    'tmngpdb_group12_refresh_freq':'The tmngpdb_group12_refresh_freq table contains information about the refresh frequency of various tables in the tmngpdb_group12 database. It includes the source system name, table name, and the timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and tracking the freshness of data in the database, ensuring that it is up-to-date and reliable for business operations.',

    'tmngpdb_group1_refresh_freq':'The tmngpdb_group1_refresh_freq table provides information about the refresh frequency of various tables in the tmngpdb_group1 database. It includes the source system name, table name, and the timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and managing the data quality of the tmngpdb_group1 database, as it allows tracking when tables were last refreshed and updated. It helps ensure that the data in the database is up-to-date and reliable for business operations.',

    'tmngpdb_group2_refresh_freq':'The tmngpdb_group2_refresh_freq table provides information about the refresh frequency of data sources in the TMNGPDB system. It includes details such as the source system name, table name, and the timestamps of the last refresh and last update. This table is crucial for monitoring and managing the data quality in the TMNGPDB system, ensuring that the data is up-to-date and accurate. It helps the business track the frequency of data updates and identify any potential issues or delays in data refreshes.',

    'tmngpdb_group4_refresh_freq':'The tmngpdb_group4_refresh_freq table in the data_quality_dev catalog and silver schema contains information about the refresh frequency of data sources. It includes the source system name, table name, and timestamps for the last refresh reported by the source system and the last update made to the table. This table is important for monitoring and managing data quality, as it provides insights into how frequently data sources are refreshed and when the table was last updated. It helps in identifying any delays or issues in data refresh processes and ensuring data accuracy and timeliness.',

    'tmngpdb_group5_refresh_freq':'The tmngpdb_group5_refresh_freq table provides information about the refresh frequency of data sources in the TMNGPDB system. It includes details such as the source system name, table name, and timestamps for the last refresh and last update. This table is significant to the business as it helps track the timeliness and reliability of data in the TMNGPDB system, allowing for better decision-making and ensuring data quality. The timestamps provide insights into when the data was last refreshed and updated, enabling users to identify any potential delays or issues in data availability.',

    'tmngpdb_group6_refresh_freq':'The tmngpdb_group6_refresh_freq table provides information about the refresh frequency of various tables in the tmngpdb_group6 database. It includes the source system name, table name, and the timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and managing data quality, as it allows the business to track when tables were last refreshed and updated. It helps ensure that data is up-to-date and reliable for analysis and decision-making purposes.',

    'tmngpdb_group7_refresh_freq':'The tmngpdb_group7_refresh_freq table provides information about the refresh frequency of various tables in the tmngpdb_group7 database. It includes the source system name, table name, and the timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and managing the data quality of the tmngpdb_group7 database, as it allows tracking the frequency of data updates and identifying any potential issues or delays in data refresh. The timestamps in this table provide valuable insights into the timeliness and reliability of the data in the database.',

    'tmngpdb_group8_refresh_freq':'The tmngpdb_group8_refresh_freq table in the data_quality_dev catalog and silver schema contains information about the refresh frequency of data sources in the system. It includes details such as the source system name, table name, and timestamps for the last refresh and last update. This table is important for tracking the timeliness and reliability of data in the system, allowing the business to ensure that data is up-to-date and accurate for decision-making processes.',
    
    'tmngpdb_group9_refresh_freq':'The tmngpdb_group9_refresh_freq table provides information about the refresh frequency of various tables in the tmngpdb_group9 database. It includes the source system name, table name, and the timestamps for the last refresh from the reported source and the last update. This table is important for monitoring and managing the data quality of the tmngpdb_group9 database, as it allows tracking of when tables were last refreshed and updated. It helps ensure that the data in the database is up-to-date and reliable for business operations.',

    'tmngpdb_group3_refresh_freq':'The tmngpdb_group3_refresh_freq table provides information about the refresh frequency of tables in the tmngpdb database. It includes the source system name, table name, and the timestamp of the last refresh from the reported source. The LAST_UPDT_TS column indicates the timestamp of when the record was last updated in the table. This table is useful for tracking and monitoring the data refresh process, ensuring data quality, and identifying any delays or issues in the refresh frequency of tables.',

    'tmprodvty_refresh_freq':'The tmprodvty_refresh_freq table in the data_quality_dev catalog and silver schema contains information about the refresh frequency of productivity data from the source systems. It includes the name of the source system, the table name, the timestamp of the last refresh from the source system, and the timestamp of the last update in the table. This table is significant to the business as it helps track and monitor the timeliness of productivity data updates, ensuring that the data is up-to-date and reliable for analysis and decision-making purposes.',

    'tmreviews_refresh_freq':'The tmreviews_refresh_freq table in the data_quality_dev catalog and silver schema contains information about the refresh frequency of the reviews data from the source system. It includes details such as the source system name, table name, and timestamps for the last refresh and last update. This table is significant to the business as it helps track the timeliness of the reviews data and ensures that it is up-to-date for analysis and decision-making purposes.',

    'tmworker_refresh_freq':'The tmworker_refresh_freq table in the data_quality_dev database is used to track the refresh frequency of tables in the silver schema. It contains information about the source system name, table name, and the timestamps of the last refresh and last update. This table is important for monitoring and managing the data quality of the tables in the silver schema, ensuring that they are regularly refreshed and updated. It provides valuable insights into the timeliness and reliability of the data in the silver schema, enabling effective decision-making and analysis for the business.'
}

for table_name, comment in tables_to_comment.items():
    # Check if the table exists
    table_exists_query = f"""
    SHOW TABLES IN {spark.conf.get('conf.catalog')}.{spark.conf.get('conf.database')}
    LIKE '{table_name}'
    """
    table_exists = spark.sql(table_exists_query).count() > 0

    if table_exists:
        alter_table_query = f"""
        ALTER TABLE {spark.conf.get('conf.catalog')}.{spark.conf.get('conf.database')}.{table_name}
        SET TBLPROPERTIES ('comment' = '{comment}')
        """
        spark.sql(alter_table_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SOURCE_DB_NAME': 'Name of the source database.',
    'TARGET_CATALOG_NAME': 'Name of the target catalog.',
    'TARGET_DB_NAME': 'Name of the target database.',
    'CNCTN_DTL_DESC': 'Description of the connection details.',
    'SRC_TBL_NAME': 'Name of the source table.',
    'TRGT_TBL_NAME': 'Name of the target table.',
    'IN_DBX_IND': 'Indicator if in Databricks.',
    'OBJECT_TYPE': 'Type of the object.',
    'SRC_SYS_NAME': 'Name of the source system.',
    'PROC_CTGRY_CD': 'Processing category code.',
    'AUDT_INSRT_ID': 'Audit insert ID.',
    'AUDT_INSRT_TS': 'Timestamp when the audit was inserted.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='cmn_catalog_rfrnc', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Name of the source system.',
    'SOURCE_DB_NAME': 'Name of the source database.',
    'SRC_TBL_NAME': 'Name of the source table.',
    'TARGET_CATALOG_NAME': 'Name of the target catalog.',
    'TARGET_DB_NAME': 'Name of the target database.',
    'TRGT_TBL_NAME': 'Name of the target table.',
    'OBJECT_TYPE': 'Type of the object.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='cmn_catalog_rfrnc_stg', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'QUERY_NAME': 'Name of the query.',
    'QUERY_DESC': 'Description of the query.',
    'CNCTN_DTL_DESC': 'Connection detail description.',
    'QUERY_TEXT': 'Text of the query.',
    'SRC_SYS_NAME': 'Name of the source system.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='cmn_dq_vrfctn_query_rfrnc', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'PROC_ID': 'Unique identifier for the process.',
    'PRNT_PROC_ID': 'Identifier for the parent process.',
    'PROC_NAME': 'Name of the process.',
    'PROC_DESC': 'Description of the process.',
    'PROC_CTGRY_CD': 'Category code of the process.',
    'PROC_CTGRY_DESC': 'Category description of the process.',
    'PROC_CNFG_FILE_PATH': 'Configuration file path for the process.',
    'SRC_TBL_NAME': 'Name of the source table.',
    'TRGT_TBL_NAME': 'Name of the target table.',
    'SRC_SYS_NAME': 'Name of the source system.',
    'AUDT_INSRT_ID': 'Audit insert identifier.',
    'AUDT_INSRT_TS': 'Audit insert timestamp.',
    'AUDT_UPDT_ID': 'Audit update identifier.',
    'AUDT_UPDT_TS': 'Audit update timestamp.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='cmn_proc_defn_rfrnc', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'PROC_NAME': 'Name of the process.',
    'QUERY_SET_ID': 'Identifier for the query set.',
    'QUERY_DQ_CD': 'Data quality code for the query.',
    'TRGT_QUERY_NAME': 'Name of the target query.',
    'SRC_QUERY_NAME': 'Name of the source query.',
    'QUERY_SET_DESC': 'Description of the query set.',
    'ERR_THRSHLD_PCT': 'Error threshold percentage.',
    'SRC_SYS_NAME': 'Name of the source system.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='cmn_proc_vrfctn_query_asctn', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")


# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'PROC_ID': 'Process identifier.',
    'PROC_NAME': 'Name of the process.',
    'PROC_CTGRY_CD': 'Process category code.',
    'QUERY_SET_ID': 'Identifier for the query set.',
    'QUERY_DQ_CD': 'Data quality code for the query.',
    'SRC_QUERY_NAME': 'Name of the source query.',
    'TRGT_QUERY_NAME': 'Name of the target query.',
    'JOB_LOG_ID': 'Job log identifier.',
    'JOB_START_TS': 'Job start timestamp.',
    'RPTD_SRC_RSLT_CNT': 'Reported source result count.',
    'RPTD_TRGT_RSLT_CNT': 'Reported target result count.',
    'ERR_THRSHLD_PCT': 'Error threshold percentage.',
    'RPTD_VRNC_PCT': 'Reported variance percentage.',
    'DQ_RSLT_MSG': 'Data quality result message.',
    'AUDT_INSRT_ID': 'Audit insert identifier.',
    'AUDT_INSRT_TS': 'Audit insert timestamp.',
    'SRC_SYS_NAME': 'Name of the source system.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='cmn_proc_vrfctn_rslt', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'PROC_ID': 'Process identifier.',
    'PROC_NAME': 'Name of the process.',
    'PROC_CTGRY_CD': 'Process category code.',
    'QUERY_SET_ID': 'Identifier for the query set.',
    'QUERY_DQ_CD': 'Data quality code for the query.',
    'SRC_QUERY_NAME': 'Name of the source query.',
    'TRGT_QUERY_NAME': 'Name of the target query.',
    'JOB_LOG_ID': 'Job log identifier.',
    'JOB_START_TS': 'Job start timestamp.',
    'RPTD_SRC_RSLT_CNT': 'Reported source result count.',
    'RPTD_TRGT_RSLT_CNT': 'Reported target result count.',
    'ERR_THRSHLD_PCT': 'Error threshold percentage.',
    'RPTD_VRNC_PCT': 'Reported variance percentage.',
    'DQ_RSLT_MSG': 'Data quality result message.',
    'AUDT_INSRT_ID': 'Audit insert identifier.',
    'AUDT_INSRT_TS': 'Audit insert timestamp.',
    'SRC_SYS_NAME': 'Name of the source system.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='cmn_proc_vrfctn_rslt_old', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='efoiap_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='eogadmin_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='jbteasps_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='proceeding_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmbuscalendar_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmintltm_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngfpepp_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")


# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngidmp_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group10_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group11_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group12_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group1_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group2_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")


# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group4_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group5_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group6_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group7_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group8_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group9_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmngpdb_group3_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")


# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmprodvty_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")


# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmreviews_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")


# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'SRC_SYS_NAME': 'Source system name.',
    'TABLE_NAME': 'Name of the table.',
    'RPTD_SRC_LAST_REFRESH_TS': 'Reported source last refresh timestamp.',
    'LAST_UPDT_TS': 'Last update timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=data_quality_catalog, database=database, table='tmworker_refresh_freq', column_name=column, comment=comment)

  try:
    spark.sql(column_comment_query)
  except Exception as e:
    print(f"Skipping column {column} due to error: {e}")



# Databricks notebook source
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/jbteasps-conf.yaml"
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
jbteasps_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{jbteasps_catalog=}, {data_quality_catalog=} ')


# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'

spark.conf.set('conf.catalog', jbteasps_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

tables_to_comment = {
    'audit_log':'The audit_log table contains records of all exchanges and exercises performed inside the business framework. It catches data, for example, reference numbers, chronic numbers, supporter IDs, IP addresses, exchange types, source framework IDs, enrollment numbers, structure codes, accommodation IDs, recording IDs, client IDs, timestamps, signatory names and positions, signature types, documenting dates, benefactor first and last names, and supporter email addresses. This table is pivotal for following and examining the moves initiated by clients and keeping a record of all framework exercises.',

    'cdc_batch_job_control':'The cdc_batch_job_control table in the bronze schema of the trm_tmbuscalendar_dev catalog is used to control and track the Change Data Capture (CDC) batch jobs. It contains information about the source folder, catalog name, database name, and table name for each job. Additionally, it includes the source database name and source table name, which represent the original data source. The primary keys column stores the primary keys for each table, while the full_load column indicates whether a full load is required. The initial_load_finished column is a boolean value that shows whether the initial load has been completed. This table is essential for managing and monitoring the CDC process in the business system.',

    'cdc_batch_job_history':'The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs. It stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. This table is significant to the business as it allows tracking and monitoring of CDC batch job activities, providing insights into data changes and updates. The data in this table represents the historical records of CDC batch jobs, enabling analysis and troubleshooting of data synchronization processes.',

    'stnd_source_system':'The stnd_source_system table contains data about the different source frameworks utilized in the business. It incorporates the extraordinary identifier for each source framework, a short name, a complete name, and a depiction of the framework. The table likewise incorporates timestamps for when the frameworks data was made and last altered, as well as the client IDs related with those activities. Furthermore, there are sections for the powerful dates of the frameworks data and a showcase name for the framework. This table is significant for following and dealing with the different source frameworks utilized in the business.',

    'stnd_transaction_type':'The stnd_transaction_type table contains data about various kinds of exchanges. It incorporates the exchange type code, depiction, and the powerful dates for when the exchange type is substantial. The table additionally tracks the creation and alteration timestamps, as well as the client IDs of the people who made and last changed the exchange type records. This table is significant for classifying and overseeing different kinds of exchanges inside the business framework.'
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
'audit_log_id':'Audit file id.',
'reference_no':'Audit refernce number.',
'serial_no':'Audit serial number',
'cfk_patron_id':'Audit patrons Id',
'ip_address_tx':'Audit IP addresses.',
'fk_transaction_type_cd':'Transaction types.',
'fk_source_system_id':'Source system IDs',
'registration_no':'Registration numbers.',
'fk_form_cd':'Form codes.',
'submission_id':'Audit submission Ids.',
'filing_id':'Audit filling id.',
'create_user_id':'User Id who craeted.',
'create_ts':'Timstamp when audit was created.',
'signatory_nm':'Signature names.',
'signatory_position_nm':'Signature user position.',
'fk_signature_type_cd':'Type of signature.',
'filing_dt':'Filling date of audit.',
'dn_patron_first_nm':'Patrons first name.',
'dn_patron_last_nm':'Patrons last name.',
'dn_patron_email_address_tx':'Patrons mail Id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=jbteasps_catalog, database=database, table='audit_log', column_name=column, comment=comment)

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
  """.format(catalog=jbteasps_catalog, database=database, table='cdc_batch_job_history', column_name=column, comment=comment)

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
  """.format(catalog=jbteasps_catalog, database=database, table='cdc_batch_job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'source_system_id':'Source system id',
'short_nm':'Short name of a framework.',
'full_nm':'Full name of a framework.',
'description_tx':'Description of a framework.',
'begin_effective_dt':'Effective time of a framework.',
'end_effective_dt':'End of an effective tiem of a framework.',
'create_ts':'Timestamp created when framework was craeted.',
'create_user_id':'User who created framework.',
'last_mod_ts':'Timestamp when last modified..',
'last_mod_user_id':'User id who last modified framework.',
'display_nm':'Display name of a framework.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=jbteasps_catalog, database=database, table='stnd_source_system', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'transaction_type_cd':'standard transaction type.',
'description_tx':'Description for transaction type.',
'begin_effective_dt':'Effective time of a transaction type.',
'end_effective_dt':'End of effective time for transaction type.',
'create_ts':'Timestamp created when framework was craeted.',
'create_user_id':'User Id of user who created.',
'last_mod_ts':'Timestamp when last modified.',
'last_mod_user_id':'User id who last modified transaction type..',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=jbteasps_catalog, database=database, table='stnd_transaction_type', column_name=column, comment=comment)

  spark.sql(column_comment_query)

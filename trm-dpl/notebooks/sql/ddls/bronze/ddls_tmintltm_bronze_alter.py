# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmintltm-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file
# MAGIC

# COMMAND ----------

common_configs = read_yaml(config_file)
tmintltm_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmintltm_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmbuscalendar_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 


# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'

spark.conf.set('conf.catalog',  tmintltm_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)


# COMMAND ----------

tables_to_comment = {
    'cdc_batch_job_control':'The cdc_batch_job_control table in the bronze schema of the trm_tmbuscalendar_dev catalog is used to control and track the Change Data Capture (CDC) batch jobs. It contains information about the source folder, catalog name, database name, and table name for each job. Additionally, it includes the source database name and source table name, which represent the original data source. The primary keys column stores the primary keys for each table, while the full_load column indicates whether a full load is required. The initial_load_finished column is a boolean value that shows whether the initial load has been completed. This table is essential for managing and monitoring the CDC process in the business system.',

    'cdc_batch_job_history':'The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs. It stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. This table is significant to the business as it allows tracking and monitoring of CDC batch job activities, providing insights into data changes and updates. The data in this table represents the historical records of CDC batch jobs, enabling analysis and troubleshooting of data synchronization processes.',

    'base_appl_intl_reg':'The base_appl_intl_reg table contains information about international trademark registrations. It includes data such as the trademarks unique identifier, the sequence number, the status code, the status date, the renewal date, the lock control number, and the timestamps for when the record was created and last modified. This table is significant to the business as it allows for tracking and managing international trademark registrations, including their status and renewal dates.',

    'base_application':'The base_application table in the bronze schema of the trm_tmintltm_dev catalog contains data related to trademark applications. It includes information such as the trademarks global identifier, serial number, international application identifier, lock control number, creation timestamp, user ID of the creator, last modification timestamp, and user ID of the last modifier. This table is significant to the business as it stores essential details about trademark applications, allowing for tracking and management of the application process.',

    'base_application_h':'The base_application_h table in the bronze schema of the trm_tmintltm_dev catalog contains data related to international trademark applications. It represents the historical records of trademark applications, including information such as the foreign key for the international application, trademark identifier, serial number, transaction instance, action count, lock control number, creation and modification timestamps, and effective timestamps. This table is significant to the business as it allows tracking and analysis of the history and details of international trademark applications over time.',

    'international_appl_event':'The international_appl_event table contains data related to international application events in the business. It represents various events and reasons associated with international applications. The table includes information such as the order number, document ID, worker ID, and registration instance number. It also includes timestamps for the effective date, recordal date, creation date, and last modification date. This table is essential for tracking and managing international application events within the business.',

    'international_appl_evnt_rsn':'The international_appl_evnt_rsn table contains data related to the reasons for international application events. It provides information on the event codes, titles, and descriptions associated with each reason. The table also includes details on the type of event and whether it is part of the prosecution history. Additionally, it tracks the effective dates, creation and modification timestamps, as well as the user IDs responsible for creating and modifying the data. This table is crucial for understanding the reasons behind international application events and their impact on the business.',

    'international_application':'The international_application table in the bronze schema of the trm_tmintltm_dev catalog contains data related to international patent applications. It includes information such as the unique application ID, the US reference number, the applicants email address, the date of automatic certification, the date of publication, the original filing date, the deadline for reply, the payment reference number, the payment type code, the control number for locking the application, and timestamps for creation and last modification. This table is significant to the business as it stores essential details about international patent applications and allows for tracking and management of the application process.',

    'international_application_h':'The international_application_h table contains data related to international patent applications. It includes information such as the applications unique identifier, transaction instance, action count, US reference number, email address, automatic certification status, publication date, original filing date, reply by date, payment reference number, payment type code, lock control number, creation and modification timestamps, and effective timestamps. This table is significant to the business as it allows for tracking and managing international patent applications throughout their lifecycle.',

    'international_reg_tm':'The international_reg_tm table contains data related to international trademark registrations. It stores information such as the trademarks unique identifier, the international registrations unique identifier, the serial number of the trademark, the status code of the trademark, important dates such as the status date, priority claimed date, auto protect date, and notification date. Additionally, it includes details about cancellation, first refusal, renewal, publication, lock control number, and the timestamps and user IDs for record creation and modification',

    'international_reg_tm_h':'The international_reg_tm_h table contains data related to international trademark registrations. It includes information such as the trademarks unique identifier, serial number, transaction instance identifier, status code, status date, priority claimed date, automatic protection date, notification date, cancellation date, first refusal indicator, renewal date, publication date, lock control number, creation timestamp, creation user ID, last modification timestamp, last modification user ID, and begin effective timestamp. This table is significant to the business as it allows for tracking and managing international trademark registrations and their associated details.',

    'international_reg_tm_notice':'The international_reg_tm_notice table contains data related to international trademark notices. It provides information on the notice type, notice source, scheduled notice date, processed notice date, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it allows for tracking and management of international trademark notices. The data in this table represents the various attributes and details associated with each notice, enabling the business to monitor and analyze international trademark activities.',

    'international_registration':'The international_registration table in the bronze schema of the trm_tmintltm_dev catalog contains data related to international registrations. It represents the records of international registrations made by users. The table includes information such as the international registration ID, sequence number, lock control number, creation timestamp, and user IDs for creation and last modification. This table is significant to the business as it allows tracking and management of international registrations, providing insights into the usage and activity of international registration services.',

    'international_registration_h':'The international_registration_h table contains data related to international registration instances. It includes information such as the unique identifier for each registration, the action count, and the control number for locking purposes. Additionally, it tracks the timestamps for when the registration was created and last modified, as well as the effective timestamps for the beginning and end of the registration. This table is essential for managing and tracking international registrations within the business.',

    'international_tm':'The international_tm table in the bronze schema of the trm_tmintltm_dev catalog contains data related to international trademarks. It includes information such as the international registration number, registration date, source country, lock control number, creation timestamp, creating user ID, last modification timestamp, and last modifying user ID. This table is significant to the business as it allows for the tracking and management of international trademarks, providing key details for legal and administrative purposes.',

    'international_tm_h':'The international_tm_h table in the bronze schema of the trm_tmintltm_dev catalog contains data related to international trademark registrations. It includes information such as the international registration number, transaction instance ID, action type, registration date, source country, lock control number, creation and last modification timestamps, and effective timestamps. This table is significant to the business as it stores historical records of international trademark registrations, allowing for tracking and analysis of registration activities over time.',

    'tm_base_application_notice':'The tm_base_application_notice table contains information about trademark application notices. It includes data such as the notice type, notice source, scheduled notice date, processed notice date, lock control number, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it helps track and manage trademark application notices, providing important details for legal and administrative purposes.'

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
    'CFK_TRADEMARK_GID':'Trademark unique identifier.',
    'FK_INTERNATIONAL_APPL_GID':'International tradenark application.',
    'SEQUENCE_NO':'Sequence number',
    'FK_INTERNATIONAL_REG_GID':'International registration',
    'CFK_STATUS_CD':'Status code.',
    'STATUS_DT':'Status date.',
    'IB_RENEWAL_DT':'Renewal date',
    'LOCK_CONTROL_NO':'Lock control number',
    'CREATE_TS':'Timestamp when the record was created.',
    'CREATE_USER_ID':'User id of user who created record.',
    'LAST_MOD_TS':'Timestamp when record was last modified.',
    'LAST_MOD_USER_ID':'User Id of user who modified record.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='base_appl_intl_reg', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'CFK_TRADEMARK_GID':'Trademark global identifier.',
    'DN_SERIAL_NUM':'Serial number.',
    'FK_INTERNATIONAL_APPL_GID':'International application identifier.',
    'LOCK_CONTROL_NO':'Lock control number.',
    'CREATE_TS':'Timestamp created when record was added.',
    'CREATE_USER_ID':'User id of user who created record.',
    'LAST_MOD_TS':'Timestamp when record was last modified.',
    'LAST_MOD_USER_ID':'User id of user who last modified.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='base_application', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'FK_INTERNATIONAL_APPL_GID':'Foreign key International trademark application.',
    'CFK_TRADEMARK_GID':'Trademark Identifier.',
    'DN_SERIAL_NUM':'Serial number.',
    'CFK_TRANSACTION_INSTANCE_GID':'Transaction instance',
    'ACTION_CT':'Action count',
    'LOCK_CONTROL_NO':'Lock control number',
    'CREATE_TS':'Timestamp created when record was craeted.',
    'CREATE_USER_ID':'User Id of user who craeted it.',
    'LAST_MOD_TS':'Timestamp when record was modified.',
    'LAST_MOD_USER_ID':'User id of user who modified.',
    'BEGIN_EFFECTIVE_TS':'Efffective start date.',
    'END_EFFECTIVE_TS':'Effective end date.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='base_application_h', column_name=column, comment=comment)

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
  """.format(catalog=tmintltm_catalog, database=database, table='cdc_batch_job_history', column_name=column, comment=comment)

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
  """.format(catalog=tmintltm_catalog, database=database, table='cdc_batch_job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'international_appl_event_id':'International application event Id.',
    'fk_international_appl_gid':'foreign key international application.',
    'order_no':'Order number of event.',
    'international_appl_evnt_rsn_id':'Iternational application event id.',
    'fk_intl_appl_tran_instnc_gid':'Application instance gid.',
    'dn_intl_reg_instance_num':'Registration instance number.',
    'effective_ts':'Effective timestamp.',
    'paper_in':'Paper in.',
    'cfk_document_id':'Document Id',
    'cfk_worker_gid':'Worker Id',
    'dn_worker_no':'Worker number.',
    'recordal_dt':'recorded date.',
    'lock_control_no':'Lock control number.',
    'create_ts':'Timestamp when record was created.',
    'create_user_id':'User id when user was created.',
    'last_mod_ts':'Last modified timestamp when record modified.',
    'last_mod_user_id':'User if of user who craeted record.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_appl_event', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'international_appl_evnt_rsn_id':'International application event Id.',
    'international_appl_evnt_rsn_cd':'International application event codes.',
    'title_tx':'Titles.',
    'description_tx':'Descriptions associated with each reason.',
    'cfk_fsm_type_event_id':'Type of event.',
    'prosecution_history_in':'Weather it is prosecution history.',
    'alert_trigger_ct':'Alert Trigger count.',
    'begin_effective_dt':'Effective start date.',
    'end_effective_dt':'Effective end date.',
    'create_ts':'Timestamp when record created.',
    'create_user_id':'User id of user who created record.',
    'last_mod_ts':'Timestamp when record was last modified.',
    'last_mod_user_id':'User id of user who last modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_appl_evnt_rsn', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'INTERNATIONAL_APPLICATION_GID':'International unique application ID.',
    'INTERNATIONAL_US_REF_NO':'The US refernce number.',
    'EMAIL_ADDRESS_TX':'The applicant email address.',
    'AUTOMATIC_CERTIFICATION_IN':'Date of automatic certification.',
    'IB_PUBLICATION_DT':'Date of publication.',
    'ORIGINAL_FILING_DT':'Original filing date',
    'REPLY_BY_DT':'Deadline for reply.',
    'PAYMENT_REFERENCE_NO':'Payment refernce number.',
    'CFK_PAYMENT_TYPE_CD':'Payment type code.',
    'LOCK_CONTROL_NO':'Control number for locking application.',
    'CREATE_TS':'Timestamp when record created.',
    'CREATE_USER_ID':'User Id of user who created.',
    'LAST_MOD_TS':'Timestamp when record last modified.',
    'LAST_MOD_USER_ID':'User Id of user who modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_application', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'INTERNATIONAL_APPLICATION_GID':'Applications unique identifier.',
    'CFK_TRANSACTION_INSTANCE_GID':'Transaction instance.',
    'ACTION_CT':'Action count.',
    'INTERNATIONAL_US_REF_NO':'US reference number.',
    'EMAIL_ADDRESS_TX':'Email address.',
    'AUTOMATIC_CERTIFICATION_IN':'Automatic certification status.',
    'IB_PUBLICATION_DT':'Publication date.',
    'ORIGINAL_FILING_DT':'Original filing date.',
    'REPLY_BY_DT':'Reply by date.',
    'PAYMENT_REFERENCE_NO':'Payment reference number.',
    'CFK_PAYMENT_TYPE_CD':'Payment type code.',
    'LOCK_CONTROL_NO':'Lock control number.',
    'CREATE_TS':'Timestamp created when record was created.',
    'CREATE_USER_ID':'User Id of user who created it.',
    'LAST_MOD_TS':'Timestamp when record was last modified.',
    'LAST_MOD_USER_ID':'User Id of user who modified it.',
    'BEGIN_EFFECTIVE_TS':'Effective start date.',
    'END_EFFECTIVE_TS':'Effective end date.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_application_h', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'CFK_TRADEMARK_GID':'Trademark unique identifier.',
    'FK_INTERNATIONAL_REG_GID':'Registrations unique identifier.',
    'DN_SERIAL_NUM':'Serial number of the trademark.',
    'CFK_STATUS_CD':'Status code of the trademark.',
    'STATUS_DT':'Status date',
    'PRIORITY_CLAIMED_DT':'Priority claim date.',
    'AUTO_PROTECT_DT':'Auto protect date.',
    'NOTIFICATION_DT':'Notification date.',
    'CANCELLATION_DT':'Cancellation date.',
    'FIRST_REFUSAL_IN':'First refusal.',
    'IB_RENEWAL_DT':'Renewal date.',
    'IB_PUBLICATION_DT':'Publication date',
    'LOCK_CONTROL_NO':'Lock control number.',
    'CREATE_TS':'Timestamp created when record created.',
    'CREATE_USER_ID':'User Id of user who created it.',
    'LAST_MOD_TS':'Timestamp when record was modified.',
    'LAST_MOD_USER_ID':'User Id of user who modified record.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_reg_tm', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'intl_reg_tm_notice_id':'International trademark notice Id.',
    'cfk_trademark_gid':'Trademark unique identifier.',
    'cfk_notice_type_cd':'Notice type.',
    'cfk_notice_source_cd':'Notice source.',
    'scheduled_notice_dt':'Scheduled notice date.',
    'processed_notice_dt':'Processed notice date.',
    'lock_control_no':'Lock control number',
    'create_ts':'Timestamp when record was created.',
    'create_user_id':'User Id of user who created it.',
    'last_mod_ts':'Timestamp when record was last modifed.',
    'last_mod_user_id':'User Id of user who modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_reg_tm_notice', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'INTERNATIONAL_REG_GID':'International registration.',
    'FK_INTERNATIONAL_REG_NO':'International registration ID.',
    'INTERNATIONAL_REG_SEQ_NO':'Sequence number.',
    'LOCK_CONTROL_NO':'Lock control number',
    'CREATE_TS':'Timestamp when record was created.',
    'CREATE_USER_ID':'User Id of user who created it.',
    'LAST_MOD_USER_ID':'Timestamp when record was modified.',
    'LAST_MOD_TS':'User Id of user who modified it.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_registration', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'INTERNATIONAL_REG_GID':'International registration instances.',
    'CFK_TRANSACTION_INSTANCE_GID':'Unique identifier for each registration.',
    'ACTION_CT':'Action count.',
    'FK_INTERNATIONAL_REG_NO':'International registration number.',
    'INTERNATIONAL_REG_SEQ_NO':'International registration sequence number.',
    'LOCK_CONTROL_NO':'Control number.',
    'CREATE_TS':'Timestamp when record was created.',
    'CREATE_USER_ID':'User Id of user who created it.',
    'LAST_MOD_USER_ID':'User Id of user who created it.',
    'LAST_MOD_TS':'Timestamp when record modified',
    'BEGIN_EFFECTIVE_TS':'Effective start timestamp.',
    'END_EFFECTIVE_TS':'Effective end timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_registration_h', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'INTERNATIONAL_REG_NO':'International registration number.',
    'INTERNATIONAL_REG_DT':'Registration date.',
    'SOURCE_CT':'Source country.',
    'LOCK_CONTROL_NO':'Lock control number',
    'CREATE_TS':'Timestamp created when record added.',
    'CREATE_USER_ID':'User Id of user who created record.',
    'LAST_MOD_TS':'Timestamp when record was modified.',
    'LAST_MOD_USER_ID':'User if of user who modified it.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_tm', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'INTERNATIONAL_REG_NO':'International registration number.',
    'CFK_TRANSACTION_INSTANCE_GID':'Transaction instance ID.',
    'ACTION_CT':'Action type.',
    'INTERNATIONAL_REG_DT':'Registration date.',
    'SOURCE_CT':'source country.',
    'LOCK_CONTROL_NO':'Lock control number.',
    'CREATE_TS':'Timestamp created when record was added.',
    'CREATE_USER_ID':'User Id of user who created record.',
    'LAST_MOD_TS':'Timestamp when record was modified.',
    'LAST_MOD_USER_ID':'User ID of user who modified.',
    'BEGIN_EFFECTIVE_TS':'Start of effective timestamp.',
    'END_EFFECTIVE_TS':'End of effective timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='international_tm_h', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
   'tm_base_appl_notice_id':'Trademark application notice Id',
   'cfk_trademark_gid':'Tademark unique identifier.',
   'fk_international_appl_gid':'International application Id.',
   'cfk_notice_type_cd':'Notice type.',
   'cfk_notice_source_cd':'Notice source',
   'scheduled_notice_dt':'Scheduled notice date.',
   'processed_notice_dt':'Processed notice date.',
   'lock_control_no':'lock control number.',
   'create_ts':'Timestamp created when record created.',
   'create_user_id':'User Id of user who created it.',
   'last_mod_ts':'Timestamp when record was modified.',
   'last_mod_user_id':'User Id of user who modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmintltm_catalog, database=database, table='tm_base_application_notice', column_name=column, comment=comment)

  spark.sql(column_comment_query)

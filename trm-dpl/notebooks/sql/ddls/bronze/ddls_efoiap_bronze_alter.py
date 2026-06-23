# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = "../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/efoiap-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
else:
    dbutils.widgets.text("env", dbx_env) 

# COMMAND ----------

# MAGIC %run  ../../../../notebooks/python/shared/ntb_common_func_and_params $config_file=config_file
# MAGIC

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
efoiap_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{efoiap_catalog=}, {data_quality_catalog=} ')
src_folder = common_configs['cdc']['src_csv_files']
src_database = common_configs['cdc']['src_database']
spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
spark.conf.set('config.efoiap_catalog', efoiap_catalog.lower()) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
catalog = efoiap_catalog
spark.conf.set('conf.catalog', efoiap_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)

# COMMAND ----------

tables_to_comment = {
    'appeal_decision_issue':'The appeal_decision_issue table in the bronze schema of the trm_efoiap_dev catalog contains data related to the issues raised in trademark appeal proceedings. It provides information on the sequence number and trademark proceeding number associated with each issue. Additionally, it includes codes for the level 1 and level 2 issues. The table also tracks the timestamps and user IDs for when the records were created and last modified. This data is crucial for analyzing and understanding the various issues raised during trademark appeal proceedings.',

    'cdc_batch_job_control':'The cdc_batch_job_control table in the bronze schema of the trm_efoiap_dev catalog contains information about the control and management of Change Data Capture (CDC) batch jobs. It stores details such as the source folder, catalog name, database name, and table name for each job. Additionally, it includes the source database name and source table name for reference. The table also indicates whether a full load is required and if the initial load has been finished, represented by a boolean value. This table is essential for tracking and monitoring the progress of CDC batch jobs in the system.',

    'cdc_batch_job_history':'The cdc_batch_job_history table in the bronze schema of the trm_efoiap_dev catalog contains data related to the history of change data capture (CDC) batch jobs. This table provides information about the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the batch job. The data in this table is essential for tracking and analyzing the execution and performance of CDC batch jobs in the business process.',

    'document_type':'The document_type table in the bronze schema of the trm_efoiap_dev catalog contains information about different types of documents used in the business. It includes the name of the document type, a description of the document type, the timestamp of the last modification made to the document type, the user ID of the person who made the last modification, and the short name of the business associated with the document type. This table is significant to the business as it helps in categorizing and managing various types of documents used within the organization.',

    'efoia_trigger_exceptions':'The efoia_trigger_exceptions table in the bronze schema of the trm_efoiap_dev catalog stores information about exceptions that occur during the processing of FOIA triggers. It captures the proceeding number, timestamp of when the exception was inserted, error number, error message, backtrace, and callstack. This table is essential for tracking and troubleshooting any errors or issues that arise during the FOIA trigger processing, allowing for timely resolution and ensuring smooth operation of the FOIA system.',

    'prosecution_history_event':'The prosecution_history_event table contains data related to the events that occur during the prosecution of a case. It includes information such as the identifier of the event, the entry code, entry date, and date due. The table also indicates whether exhibits are included and if the case is confidential. Additionally, it includes details about the object ID, last update user ID, and last update timestamp. The table is linked to other tables through foreign keys and provides information about the proceeding number, entry inform entry code, entry number, proceeding type, ESTTA ID, internal comment, external court name, external case number, trial extension days, and trial suspension days.',

    'prosecution_history_event2':'The prosecution_history_event2 table contains data related to the events that occur during the prosecution of a case. It includes information such as the identifier of the event, the entry code, the entry date, and the due date. Additionally, it indicates whether exhibits are included and if the case is confidential. The table also includes details about the object ID, the user ID of the last update, and the timestamp of the last update. It further includes information about the proceeding number, the entry inform entry code, the entry number, the proceeding type, and the ESTTA ID. Lastly, it includes internal comments, external court name, external case number, trial extension days, and trial suspension days.',

    'stnd_decision':'The stnd_decision table contains data related to decisions made within the business. It provides information on the decision code, level 1 issue code, decision name, and description. Additionally, it includes details on the user who created and last modified the decision, as well as the timestamps of when these actions occurred. The table also includes the effective dates for when the decision is valid. This data is crucial for tracking and analyzing decisions made within the business.',

    'stnd_level_1_issue':'The stnd_level_1_issue table in the bronze schema of the trm_efoiap_dev catalog contains data related to level 1 issues. Each row represents a specific level 1 issue and includes information such as the issue code, description, effective dates, and user details. This table is significant to the business as it provides a centralized repository for managing and tracking level 1 issues. It allows for easy identification and categorization of issues, enabling efficient resolution and analysis of problem areas.',

    'stnd_level_2_issue':'The stnd_level_2_issue table contains data related to level 2 issues in the business. It provides information on the issue codes, their descriptions, and the corresponding level 1 issue codes. The table also includes timestamps for when the data was created and last modified, as well as information on the effective dates for each record. This table is essential for tracking and managing level 2 issues within the business, allowing for efficient identification and resolution of problems.',

    'tm_appeal_decision':'The tm_appeal_decision table contains data related to trademark appeal decisions. It includes information such as the appeal decision ID, panel ID, document creation date, proceeding number, document image ID, proceeding type code, issue code list, decision code, decision writer name, proceeding decision file name, party name, examining attorney name, decision text, whether the precedent is citable, panel member names, marks of good service for the opposer and applicant, mark of good cited service for the examining attorney, issue text, and creation timestamp. This table is significant to the business as it provides a comprehensive record of trademark appeal decisions and associated details.',

    'tm_appeal_decision_errlog':'The tm_appeal_decision_errlog table contains error log data related to appeal decisions in the trademark proceedings. It includes information such as error numbers, error messages, error row IDs, error operation types, and error tags. The table also includes details about the appeal decision, such as the appeal decision ID, panel ID, document creation date, proceeding number, document image ID, proceeding type code, issue code list, decision code, decision writer name, proceeding decision file name, party name, examining attorney name, decision text, whether the precedent is citable or not, and panel member details. This table is essential for tracking and resolving errors in the appeal decision process and for maintaining a record of all relevant information related to appeal decisions.',

    'tm_appeal_decision_h':'The tm_appeal_decision_h table contains data related to trademark appeal decisions. It includes information such as the appeal decision ID, panel ID, document creation date, proceeding number, document image ID, proceeding type code, issue code list, decision code, decision writer name, proceeding decision file name, party name, examining attorney name, decision text, citable precedent information, panel member names, and marks of good service and citation. This table provides valuable insights into the decisions made during trademark appeal proceedings and helps track the progress and outcomes of these appeals.',

    'tmng_go_live':'The tmng_go_live table in the bronze schema of the trm_efoiap_dev catalog contains data related to the timestamps of when certain processes or features went live. This table is significant to the business as it provides a historical record of when important changes were implemented, allowing for analysis and tracking of the impact of these changes on various aspects of the business operations. The go_live_ts column in this table represents the specific timestamp when a process or feature was launched, providing valuable insights into the timing and sequence of these events.',

    'trademark_appeal_decision':'The trademark_appeal_decision table contains data related to trademark appeal proceedings. It includes information such as the sequence number of the proceeding, the type of proceeding, the names of the parties involved, the decision made in the proceeding, and the status of the proceeding. This table also includes details about the examining attorney, panel members, and any cited marks or precedents. Additionally, it includes timestamps for document creation and references to other related tables for document type and business information.'
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
    'fk_sequence_no':'Sequence number.',
    'fk_trademark_proceeding_no':'Trademark proceeding number',
    'level_1_issue_cd':'Level 1 issue codes associated with each issue.',
    'level_2_issue_cd':'Level 2 issue codes associated with each issue.',
    'create_ts':'Timestamp when issues were recorded.',
    'create_user_id':'User Id of user who added record.',
    'last_modified_ts':'Timestamp when record was modified.',
    'last_modified_user_id':'User Id of user who modified record.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='appeal_decision_issue', column_name=column, comment=comment)

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
  """.format(catalog=efoiap_catalog, database=database, table='cdc_batch_job_history', column_name=column, comment=comment)

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
  """.format(catalog=efoiap_catalog, database=database, table='cdc_batch_job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'dt_document_type_nm':'Name of document type.',
'description_tx':'Description of the document.',
'last_modified_ts':'Timestamp of the last modification.',
'last_modified_user_id':'User Id of user who made modification.',
'dt_business_short_nm':'Short name of the business associated with document type.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='document_type', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'proceeding_no':'Proceeding number.',
'insert_ts':'Timestamp of when the exception occurred.',
'error_num':'Error number.',
'error_msg':'Error message.',
'backtrace':'backtrace',
'callstack':'callstack'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='efoia_trigger_exceptions', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'identifier':'Events Identifier that occur during the prosecution of a case.',
'entry_code':'Entry code.',
'entry_date':'Entry date',
'date_due':'Date due.',
'exhibits_included_indicator':'Weather exhibits are included.',
'confidential_indicator':'If the information is confidential.',
'text':'Text associated.',
'object_id':'Object ID.',
'last_update_userid':'Last update user ID.',
'last_update_timestamp':'Last update timestamp',
'fk_proceedingnumber0':'Proceeding number',
'fk_entry_informentry_code':'Entry information code.',
'entry_num':'Entry number.',
'fk_proceedingtype':'Proceedings type.',
'estta_id':'Id related to events.',
'internal_comment_tx':'Internal comment.',
'external_court_nm':'External court name.',
'external_case_no':'External case number.',
'trial_extension_days_qt':'Trail extension days.',
'trial_suspension_days_qt':'Trail suspension days.',
'motion_pending_in':'Motion pending in.',
'fk_document_type_cd':'Document type code.',
'last_mod_doc_type_cd_ts':'Last modified document type code timestamp.',
'proceeding_resume_dt':'Proceeding resume date',
'defendant_has_email_in':'Defendant has an email addredd on record.',
'fk_ext_of_time_type_id':' type of extension of time granted in the context of the prosecution history of a case.',
'relinquishment_attachment_in':'Relinquishment attachment',
'fk_party_id':'Party Id.',
'plaintiff_has_email_in':'plaintiff has an email address on record'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='prosecution_history_event', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'identifier': 'Events Identifier that occur during the prosecution of a case.',
    'entry_code': 'Entry code.',
    'entry_date': 'Entry date.',
    'date_due': 'Date due.',
    'exhibits_included_indicator': 'Whether exhibits are included.',
    'confidential_indicator': 'If the information is confidential.',
    'text': 'Text associated.',
    'object_id': 'Object ID.',
    'last_update_userid': 'Last update user ID.',
    'last_update_timestamp': 'Last update timestamp.',
    'fk_proceedingnumber0': 'Proceeding number.',
    'fk_entry_informentry_code': 'Entry information code.',
    'entry_num': 'Entry number.',
    'fk_proceedingtype': 'Proceedings type.',
    'estta_id': 'Id related to events.',
    'internal_comment_tx': 'Internal comment.',
    'external_court_nm': 'External court name.',
    'external_case_no': 'External case number.',
    'trial_extension_days_qt': 'Trial extension days.',
    'trial_suspension_days_qt': 'Trial suspension days.',
    'motion_pending_in': 'Motion pending in.',
    'fk_document_type_cd': 'Document type code.',
    'last_mod_doc_type_cd_ts': 'Last modified document type code timestamp.',
    'proceeding_resume_dt': 'Proceeding resume date.',
    'defendant_has_email_in': 'Defendant has an email address on record.',
    'fk_ext_of_time_type_id': 'Type of extension of time granted in the context of the prosecution history of a case.',
    'relinquishment_attachment_in': 'Relinquishment attachment.',
    'fk_party_id': 'Party Id.',
    'plaintiff_has_email_in': 'Plaintiff has an email address on record.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='prosecution_history_event2', column_name=column, comment=comment)

  spark.sql(column_comment_query)


# COMMAND ----------

# Define a dictionary of column comments for stnd_decision table
column_comments = {
    'decision_cd': 'Code representing the decision.',
    'fk_level_1_issue_cd': 'Foreign key referencing the level 1 issue code.',
    'decision_nm': 'Name of the decision.',
    'description_tx': 'Description of the decision.',
    'create_user_id': 'User ID of the person who created the record.',
    'create_ts': 'Timestamp when the record was created.',
    'last_mod_user_id': 'User ID of the person who last modified the record.',
    'last_mod_ts': 'Timestamp when the record was last modified.',
    'begin_effective_dt': 'Date when the decision becomes effective.',
    'end_effective_dt': 'Date when the decision expires.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='stnd_decision', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'level_1_issue_cd': 'Code representing the level 1 issue.',
    'description_tx': 'Description of the level 1 issue.',
    'delete_in': 'Indicator if the issue is deleted.',
    'begin_effective_dt': 'Date when the issue becomes effective.',
    'end_effective_dt': 'Date when the issue expires.',
    'create_ts': 'Timestamp when the record was created.',
    'create_user_id': 'User ID of the person who created the record.',
    'last_modified_ts': 'Timestamp when the record was last modified.',
    'last_modified_user_id': 'User ID of the person who last modified the record.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='stnd_level_1_issue', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments ={
    'level_2_issue_cd': 'Code representing the level 2 issue.',
    'fk_level_1_issue_cd':'Code representing the level 1 issue code.',
    'description_tx': 'Description of the level 2 issue.',
    'delete_in': 'Indicator if the issue is deleted.',
    'begin_effective_dt': 'Date when the issue becomes effective.',
    'end_effective_dt': 'Date when the issue expires.',
    'create_ts': 'Timestamp when the record was created.',
    'create_user_id': 'User ID of the person who created the record.',
    'last_modified_ts': 'Timestamp when the record was last modified.',
    'last_modified_user_id': 'User ID of the person who last modified the record.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='stnd_level_2_issue', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments ={
    'tm_appeal_decision_id': 'ID of the TM appeal decision.',
    'cfk_panel_id': 'ID of the panel.',
    'document_create_dt': 'Timestamp when the document was created.',
    'proceeding_no': 'Number of the proceeding.',
    'document_image_id': 'ID of the document image.',
    'proceeding_type_cd': 'Code representing the type of proceeding.',
    'issue_code_list_tx': 'List of issue codes.',
    'fk_decision_cd': 'Code representing the decision.',
    'decision_writer_nm': 'Name of the decision writer.',
    'proceeding_decision_file_nm': 'File name of the proceeding decision.',
    'party_nm': 'Name of the party.',
    'examining_attorney_nm': 'Name of the examining attorney.',
    'decision_tx': 'Text of the decision.',
    'precedent_citable_in': 'Indicator if the decision is citable as precedent.',
    'panel_member_tx': 'Text of the panel member.',
    'opposer_mark_good_service_tx': 'Text of the opposer mark good service.',
    'applcnt_mark_good_service_tx': 'Text of the applicant mark good service.',
    'exmg_atty_mark_good_cited_tx': 'Text of the examining attorney mark good cited.',
    'issue_tx': 'Text of the issue.',
    'create_ts': 'Timestamp when the record was created.',
    'create_user_id': 'User ID of the person who created the record.',
    'last_mod_user_id': 'User ID of the person who last modified the record.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='tm_appeal_decision', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'ORA_ERR_NUMBER$': 'Error number from Oracle.',
    'ORA_ERR_MESG$': 'Error message from Oracle.',
    'ORA_ERR_ROWID$': 'Row ID from Oracle.',
    'ORA_ERR_OPTYP$': 'Operation type from Oracle.',
    'ORA_ERR_TAG$': 'Error tag from Oracle.',
    'TM_APPEAL_DECISION_ID': 'ID of the TM appeal decision.',
    'CFK_PANEL_ID': 'ID of the panel.',
    'DOCUMENT_CREATE_DT': 'Timestamp when the document was created.',
    'PROCEEDING_NO': 'Number of the proceeding.',
    'DOCUMENT_IMAGE_ID': 'ID of the document image.',
    'PROCEEDING_TYPE_CD': 'Code representing the type of proceeding.',
    'ISSUE_CODE_LIST_TX': 'List of issue codes.',
    'FK_DECISION_CD': 'Code representing the decision.',
    'DECISION_WRITER_NM': 'Name of the decision writer.',
    'PROCEEDING_DECISION_FILE_NM': 'File name of the proceeding decision.',
    'PARTY_NM': 'Name of the party.',
    'EXAMINING_ATTORNEY_NM': 'Name of the examining attorney.',
    'DECISION_TX': 'Text of the decision.',
    'PRECEDENT_CITABLE_IN': 'Indicator if the decision is citable as precedent.',
    'PANEL_MEMBER_TX': 'Text of the panel member.',
    'OPPOSER_MARK_GOOD_SERVICE_TX': 'Text of the opposer mark good service.',
    'APPLCNT_MARK_GOOD_SERVICE_TX': 'Text of the applicant mark good service.',
    'EXMG_ATTY_MARK_GOOD_CITED_TX': 'Text of the examining attorney mark good cited.',
    'ISSUE_TX': 'Text of the issue.',
    'CREATE_TS': 'Timestamp when the record was created.',
    'CREATE_USER_ID': 'User ID of the person who created the record.',
    'LAST_MOD_USER_ID': 'User ID of the person who last modified the record.',
    'LAST_MOD_TS': 'Timestamp when the record was last modified.',
    'DN_PHE_DOCUMENT_TYPE_CD': 'Document type code.',
    'DN_PHE_ENTRY_NO': 'Entry number.',
    'DN_PHE_ENTRY_CODE': 'Entry code.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN `{column_name}`
  COMMENT '{comment}'
  """.format(
      catalog=efoiap_catalog,
      database=database,
      table='tm_appeal_decision_errlog',
      column_name=column,
      comment=comment
  )

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments ={
    'tm_appeal_decision_h_id': 'ID of the TM appeal decision history.',
    'tm_appeal_decision_id': 'ID of the TM appeal decision.',
    'cfk_panel_id': 'ID of the panel.',
    'document_create_dt': 'Timestamp when the document was created.',
    'proceeding_no': 'Number of the proceeding.',
    'document_image_id': 'ID of the document image.',
    'proceeding_type_cd': 'Code representing the type of proceeding.',
    'issue_code_list_tx': 'List of issue codes.',
    'fk_decision_cd': 'Code representing the decision.',
    'decision_writer_nm': 'Name of the decision writer.',
    'proceeding_decision_file_nm': 'File name of the proceeding decision.',
    'party_nm': 'Name of the party.',
    'examining_attorney_nm': 'Name of the examining attorney.',
    'decision_tx': 'Text of the decision.',
    'precedent_citable_in': 'Indicator if the decision is citable as precedent.',
    'panel_member_tx': 'Text of the panel member.',
    'opposer_mark_good_service_tx': 'Text of the opposer mark good service.',
    'applcnt_mark_good_service_tx': 'Text of the applicant mark good service.',
    'exmg_atty_mark_good_cited_tx': 'Text of the examining attorney mark good cited.',
    'issue_tx': 'Text of the issue.',
    'create_ts': 'Timestamp when the record was created.',
    'create_user_id': 'User ID of the person who created the record.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='tm_appeal_decision_h', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments ={
  'go_live_ts':'Timestamp when the go-live event occurred.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='tmng_go_live', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments ={
    'sequence_no': 'Sequence number as a decimal with precision 10 and scale 0.',
    'trademark_proceeding_no': 'Trademark proceeding number as a decimal with precision 10 and scale 0.',
    'proceeding_type_cd': 'Proceeding type code as a string.',
    'proceeding_decision_file_nm': 'Proceeding decision file name as a string.',
    'party_nm': 'Party name as a string.',
    'examining_attorney_nm': 'Examining attorney name as a string.',
    'decision_tx': 'Decision text as a string.',
    'opposer_mark_good_service_tx': 'Opposer mark good service text as a string.',
    'applcnt_mark_good_service_tx': 'Applicant mark good service text as a string.',
    'precedent_citable_in': 'Precedent citable indicator as a string.',
    'status_cd': 'Status code as a string.',
    'decision_type_cd': 'Decision type code as a string.',
    'panel_member_tx': 'Panel member text as a string.',
    'exmg_atty_mark_good_cited_tx': 'Examining attorney mark good cited text as a string.',
    'document_image_id': 'Document image ID as a string.',
    'issue_tx': 'Issue text as a string.',
    'delete_in': 'Delete indicator as a string.',
    'document_create_dt': 'Document creation date as a timestamp.',
    'fk_dt_document_type_nm': 'Foreign key document type name as a string.',
    'fk_dt_business_short_nm': 'Foreign key business short name as a string.',
    'last_modified_user_id': 'Last modified user ID as a string.',
    'last_modified_ts': 'Last modified timestamp as a timestamp.'
}

for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=efoiap_catalog, database=database, table='trademark_appeal_decision', column_name=column, comment=comment)

  spark.sql(column_comment_query)

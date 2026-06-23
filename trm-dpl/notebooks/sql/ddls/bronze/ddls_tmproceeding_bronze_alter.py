# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "proceeding-conf.yaml"
config_file = (
    "../../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name
)
if dbx_env == "qa":
    dbx_env = "test"
print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# schema variables
common_configs = read_yaml(config_file)
tmproceeding_catalog = common_configs["schema"]["trgt_catalog"]
data_quality_catalog = common_configs["schema"]["data_quality_catalog"]
print(f"{tmproceeding_catalog=}, {data_quality_catalog=}")


# spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
# spark.conf.set('conf.catalog', tmproceeding_catalog.lower())
# spark.conf.set('dbx_env', dbx_env)

# COMMAND ----------

database = "bronze"
control_table = "cdc_batch_job_control"
job_history_table = "cdc_batch_job_history"
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set("conf.catalog", tmproceeding_catalog)
spark.conf.set("conf.database", database)
spark.conf.set("conf.control_table", control_table)
spark.conf.set("conf.job_history_table", job_history_table)
spark.conf.set("conf.dbx_env", dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

tables_to_comment = {'proceeding_document': 'The proceeding_document table in the bronze schema of the trm_tmproceeding catalog contains data related to documents associated with legal proceedings. It provides information on the type of document, document ID, lock control number, creation and modification timestamps, and user IDs of the individuals who created and last modified the document. This table is essential for tracking and managing the various documents involved in legal proceedings, ensuring proper documentation and accountability throughout the process.', 'proceeding_event': 'The proceeding_event table contains records of events that occur during a legal proceeding. It includes information such as the proceeding ID, event reason, order number, and document ID. This table is significant to the business as it allows for tracking and monitoring of legal proceedings, as well as providing a historical record of events. The create and last modified timestamps and user IDs provide insight into who made changes to the record and when. The lock control number indicates if a record is currently being edited by another user.', 'petition': 'The petition table contains data related to legal proceedings and petitions. It includes information such as the type of petition, the year and number of the proceeding, the reason for expungement or reexamination, the date the proceeding was instituted, and details about the submission and modification of the petition. This table is significant to the business as it provides a comprehensive record of all petitions and proceedings, allowing for analysis and tracking of legal activities.', 'lop_legal_basis': 'The lop_legal_basis table contains information about the legal basis types associated with proceedings. It includes the foreign key to the proceeding, the legal basis type code, and any additional information about the legal basis. The table also includes control numbers for locking purposes, as well as timestamps and user IDs for tracking creation and modification of records. This table is important for tracking and managing the legal basis of proceedings within the business.', 'proceeding': 'The `proceeding` table contains data related to legal proceedings. It includes information such as the proceeding ID, type of proceeding, filing date, lock control number, creation and modification timestamps, user IDs of the creators and modifiers, initiation details, received date, disposition granted reason code, current status, current state code, and current state date. This table is significant to the business as it provides a comprehensive record of legal proceedings, allowing for tracking and analysis of cases. The data in this table represents the various attributes and details associated with each proceeding, enabling informed decision-making and legal process management.', 'petition_response_document': 'The petition_response_document table in the bronze schema of the trm_tmproceeding catalog contains data related to the documents received as responses to petitions. It includes information such as the date and time the response was received, the sequence number of the document, the unique identifier of the document, the unique identifier of the proceeding it is associated with, the control number for locking purposes, the timestamp and user ID of when the record was created, and the timestamp and user ID of when the record was last modified. This table is essential for tracking and managing petition responses in the business.', 'proceeding_class': 'The proceeding_class table in the bronze schema of the trm_tmproceeding catalog contains data related to the classification of proceedings. It represents the relationship between proceedings and their corresponding classes. The table includes information such as the foreign key to the proceeding, the class ID, the class number, and details about the included goods and services. Additionally, it stores information about the lock control number, creation and modification timestamps, and user IDs. This table is essential for tracking and managing the classification of proceedings within the business.', 'lop_legal_basis_trademark': 'The lop_legal_basis_trademark table contains information about the legal basis for a trademark proceeding. It includes the foreign key to the proceeding, the legal basis type code, and the trademark global identifier. Additionally, it includes the serial number, registration number, and lock control number associated with the trademark. The table also tracks the creation and last modification timestamps and user IDs. This data is important for tracking legal proceedings related to trademarks and ensuring accurate record-keeping.', 'petition_response': 'The petition_response table in the bronze schema of the trm_tmproceeding catalog contains data related to the responses received for petitions. It provides information about the type of response, the proceeding it is associated with, the date the response was received, and the statement provided in the response. Additionally, it includes details such as the lock control number, timestamps for when the record was created and last modified, and the user IDs of the individuals who performed these actions. This table is essential for tracking and analyzing the responses received for petitions, enabling the business to make informed decisions based on the data.', 'prcdng_trigger_exceptions': 'The `prcdng_trigger_exceptions` table in the `bronze` schema of the `trm_tmproceeding` catalog stores information about exceptions that occurred during the processing of triggers. It captures the timestamp of when the exception was inserted, the error number associated with the exception, the error message, the backtrace information, and the callstack information. This table is significant to the business as it helps in identifying and troubleshooting issues that arise during trigger processing, allowing for timely resolution and ensuring smooth functioning of the system.', 'cdc_batch_job_control': 'The cdc_batch_job_control table in the bronze schema of the trm_tmproceeding catalog contains information about the control and status of Change Data Capture (CDC) batch jobs. It provides details such as the source folder, catalog name, database name, and table name for the CDC process. Additionally, it includes information about the source database and table names, primary keys, and whether a full load is required. The table also tracks the status of the initial load, indicating whether it has been finished or not through a boolean value. This table is essential for monitoring and managing CDC batch jobs in the business.', 'cdc_batch_job_history': 'The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs in the TRM Tmproceeding Dev system. This table stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. The data in this table is important for tracking and analyzing the execution of CDC batch jobs, providing insights into the timing and progress of data changes in the system.', 'letter_of_protest': 'The letter_of_protest table in the bronze schema of the trm_tmproceeding catalog contains data related to letters of protest in legal proceedings. It stores information about the foreign key of the proceeding, the lock control number, the timestamp of creation and last modification, and the user IDs of the creators and modifiers. This table is significant to the business as it allows tracking and management of letters of protest in legal proceedings, providing a historical record of creation and modification activities.', 'proceeding_tran_instance': 'The proceeding_tran_instance table contains data related to transaction instances in the business. It provides information about the unique identifier of each transaction instance, the employee associated with the transaction, the timestamp of when the transaction became effective, details of the transaction, whether it was terminated or not, the location where the transaction originated, and timestamps for when the transaction was created and last modified. This table is important for tracking and analyzing transaction activities within the business.', 'proceeding_statement': 'The proceeding_statement table in the bronze schema of the trm_tmproceeding catalog contains data related to statements made during legal proceedings. It includes information such as the type of statement, the control number for locking purposes, the timestamp of creation and last modification, and the user IDs of the creators and modifiers. The statement text itself is also stored in this table. This data is important for tracking and analyzing statements made during legal proceedings, providing insights into the progress and history of the proceedings.', 'proceeding_participant': 'The proceeding_participant table contains information about the participants involved in legal proceedings. It provides details such as the role of the participant, their bar membership state, bar membership date, attorney bar number, and their affiliation. This table is important for tracking the involvement of various parties in legal proceedings and their qualifications. It helps in understanding the composition of participants and their roles in a given proceeding.', 'proceeding_event_reason': 'The proceeding_event_reason table contains information about the reasons for different events that occur during legal proceedings. It includes details such as the reason code, title, and description of each event. Additionally, it provides information about the type of event and whether it is related to prosecution history or triggers an alert. The table also includes timestamps for when the records were created and last modified, along with the corresponding user IDs. This table is essential for tracking and analyzing the reasons behind various events in legal proceedings.', 'stnd_petition_to_director': 'The stnd_petition_to_director table contains data related to petitions submitted to the director. It includes information such as the petition code, title, description, effective dates, lock control number, and details about when the record was created or last modified. This table is significant to the business as it allows for tracking and managing petitions and their associated details. The data in this table represents the various petitions submitted to the director and provides a historical record of these submissions.', 'proceeding_mark': 'The proceeding_mark table in the bronze schema of the trm_tmproceeding catalog contains data related to the association between trademark proceedings and trademarks. It represents the link between a trademark proceeding and a specific trademark. The table includes information such as the foreign key for the proceeding, the foreign key for the trademark, a lock control number, timestamps for creation and modification, user IDs for creation and modification, and a sequence number. This table is crucial for tracking and managing trademark proceedings and their associated trademarks within the business.', 'proceeding_fee': 'The proceeding_fee table contains data related to the fees associated with legal proceedings. It provides information on the type of fee, the number of fee items, and the fee amount. Additionally, it includes details on the control number used for locking purposes. The table also tracks the creation and modification timestamps, as well as the user IDs responsible for those actions. This data is crucial for analyzing and managing the financial aspects of legal proceedings within the business.', 'proceeding_intl_appl': 'The proceeding_intl_appl table in the bronze schema of the trm_tmproceeding catalog contains data related to international applications in the proceedings. It provides information about the foreign key of the proceeding, the foreign key of the international application, the lock control number, the timestamp of creation and last modification, as well as the user IDs of the creator and last modifier. This table is significant to the business as it allows tracking and management of international applications within the proceedings, including their creation and modification history.', 'sync_tm_com_exception': 'The `sync_tm_com_exception` table contains data related to exceptions that occur during the synchronization process of communication services. It captures information such as the unique exception ID, the timestamp when the exception was inserted, the source IP address, the name of the communication service, the endpoint URL, the type of endpoint, the body of the endpoint, the HTTP error code and message, a flag indicating if the exception should be retried, the timestamp when the exception was resolved, and a reference number. This table is essential for tracking and resolving synchronization issues in the communication services.', 'stnd_lop_legal_basis': 'The stnd_lop_legal_basis table contains information about the legal basis for Line of Proceeding (LOP) titles. It includes the LOP legal basis code, title, description, and the effective dates for when the legal basis is valid. Additionally, it includes the lock control number, timestamps for when the record was created and last modified, as well as the user IDs associated with those actions. This table is essential for understanding the legal framework and justification behind each Line of Proceeding in the business.', 'lop_legal_basis_trademark_h': '[HISTORICAL] The lop_legal_basis_trademark table contains information about the legal basis for a trademark proceeding. It includes the foreign key to the proceeding, the legal basis type code, and the trademark global identifier. Additionally, it includes the serial number, registration number, and lock control number associated with the trademark. The table also tracks the creation and last modification timestamps and user IDs. This data is important for tracking legal proceedings related to trademarks and ensuring accurate record-keeping.', 'petition_h': '[HISTORICAL] The petition table contains data related to legal proceedings and petitions. It includes information such as the type of petition, the year and number of the proceeding, the reason for expungement or reexamination, the date the proceeding was instituted, and details about the submission and modification of the petition. This table is significant to the business as it provides a comprehensive record of all petitions and proceedings, allowing for analysis and tracking of legal activities.', 'proceeding_h': '[HISTORICAL] The `proceeding` table contains data related to legal proceedings. It includes information such as the proceeding ID, type of proceeding, filing date, lock control number, creation and modification timestamps, user IDs of the creators and modifiers, initiation details, received date, disposition granted reason code, current status, current state code, and current state date. This table is significant to the business as it provides a comprehensive record of legal proceedings, allowing for tracking and analysis of cases. The data in this table represents the various attributes and details associated with each proceeding, enabling informed decision-making and legal process management.', 'petition_response_h': '[HISTORICAL] The petition_response table in the bronze schema of the trm_tmproceeding catalog contains data related to the responses received for petitions. It provides information about the type of response, the proceeding it is associated with, the date the response was received, and the statement provided in the response. Additionally, it includes details such as the lock control number, timestamps for when the record was created and last modified, and the user IDs of the individuals who performed these actions. This table is essential for tracking and analyzing the responses received for petitions, enabling the business to make informed decisions based on the data.', 'petition_response_document_h': '[HISTORICAL] The petition_response_document table in the bronze schema of the trm_tmproceeding catalog contains data related to the documents received as responses to petitions. It includes information such as the date and time the response was received, the sequence number of the document, the unique identifier of the document, the unique identifier of the proceeding it is associated with, the control number for locking purposes, the timestamp and user ID of when the record was created, and the timestamp and user ID of when the record was last modified. This table is essential for tracking and managing petition responses in the business.', 'proceeding_class_h': '[HISTORICAL] The proceeding_class table in the bronze schema of the trm_tmproceeding catalog contains data related to the classification of proceedings. It represents the relationship between proceedings and their corresponding classes. The table includes information such as the foreign key to the proceeding, the class ID, the class number, and details about the included goods and services. Additionally, it stores information about the lock control number, creation and modification timestamps, and user IDs. This table is essential for tracking and managing the classification of proceedings within the business.', 'proceeding_document_h': '[HISTORICAL] The proceeding_document table in the bronze schema of the trm_tmproceeding catalog contains data related to documents associated with legal proceedings. It provides information on the type of document, document ID, lock control number, creation and modification timestamps, and user IDs of the individuals who created and last modified the document. This table is essential for tracking and managing the various documents involved in legal proceedings, ensuring proper documentation and accountability throughout the process.', 'lop_legal_basis_h': '[HISTORICAL] The lop_legal_basis table contains information about the legal basis types associated with proceedings. It includes the foreign key to the proceeding, the legal basis type code, and any additional information about the legal basis. The table also includes control numbers for locking purposes, as well as timestamps and user IDs for tracking creation and modification of records. This table is important for tracking and managing the legal basis of proceedings within the business.', 'letter_of_protest_h': '[HISTORICAL] The letter_of_protest table in the bronze schema of the trm_tmproceeding catalog contains data related to letters of protest in legal proceedings. It stores information about the foreign key of the proceeding, the lock control number, the timestamp of creation and last modification, and the user IDs of the creators and modifiers. This table is significant to the business as it allows tracking and management of letters of protest in legal proceedings, providing a historical record of creation and modification activities.', 'proceeding_statement_h': '[HISTORICAL] The proceeding_statement table in the bronze schema of the trm_tmproceeding catalog contains data related to statements made during legal proceedings. It includes information such as the type of statement, the control number for locking purposes, the timestamp of creation and last modification, and the user IDs of the creators and modifiers. The statement text itself is also stored in this table. This data is important for tracking and analyzing statements made during legal proceedings, providing insights into the progress and history of the proceedings.', 'proceeding_participant_h': '[HISTORICAL] The proceeding_participant table contains information about the participants involved in legal proceedings. It provides details such as the role of the participant, their bar membership state, bar membership date, attorney bar number, and their affiliation. This table is important for tracking the involvement of various parties in legal proceedings and their qualifications. It helps in understanding the composition of participants and their roles in a given proceeding.', 'proceeding_intl_appl_h': '[HISTORICAL] The proceeding_intl_appl table in the bronze schema of the trm_tmproceeding catalog contains data related to international applications in the proceedings. It provides information about the foreign key of the proceeding, the foreign key of the international application, the lock control number, the timestamp of creation and last modification, as well as the user IDs of the creator and last modifier. This table is significant to the business as it allows tracking and management of international applications within the proceedings, including their creation and modification history.', 'proceeding_fee_h': '[HISTORICAL] The proceeding_fee table contains data related to the fees associated with legal proceedings. It provides information on the type of fee, the number of fee items, and the fee amount. Additionally, it includes details on the control number used for locking purposes. The table also tracks the creation and modification timestamps, as well as the user IDs responsible for those actions. This data is crucial for analyzing and managing the financial aspects of legal proceedings within the business.', 'proceeding_mark_h': '[HISTORICAL] The proceeding_mark table in the bronze schema of the trm_tmproceeding catalog contains data related to the association between trademark proceedings and trademarks. It represents the link between a trademark proceeding and a specific trademark. The table includes information such as the foreign key for the proceeding, the foreign key for the trademark, a lock control number, timestamps for creation and modification, user IDs for creation and modification, and a sequence number. This table is crucial for tracking and managing trademark proceedings and their associated trademarks within the business.'}

for table_name, comment in tables_to_comment.items():
    alter_table_query = f"""
    ALTER TABLE {tmproceeding_catalog}.{database}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_prcdng_document_type_cd': 'Code representing the type of document associated with the proceeding', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'cfk_document_id': 'Foreign key referencing the document table', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'fk_proceeding_gid': 'Foreign key referencing the proceeding table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_document",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_proceeding_tran_instnc_gid': 'Foreign key referencing the transaction_instance table, indicating the associated transaction instance', 'proceeding_event_id': 'Unique identifier for each record in the proceeding_event table', 'fk_proceeding_gid': 'Foreign key referencing the proceeding table, indicating the associated legal proceeding', 'effective_ts': 'The timestamp of when the record is effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_fsm_instance_h_id': 'Foreign key referencing the fsm_instance_history table, indicating the associated FSM instance history', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'document_id': 'Unique identifier for the document associated with the event', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'order_no': 'Numeric value indicating the order of the event within a legal proceeding', 'fk_prcdng_event_reason_id': 'Foreign key referencing the proceeding_event_reason table, indicating the reason for the event', 'paper_in': 'Indicates whether the document associated with the event is in paper format'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_event",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_ptd_prir_ppr_submn_type_cd': 'Code representing the type of prior paper submission for petition', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'per_proceeding_no': 'Numeric value representing the number of the proceeding', 'cfk_ptr_reason_cd': 'Code representing the reason for pointer', 'cfk_petition_type_cd': 'Code representing the type of petition', 'ptd_other_reason_explntn_tx': 'Text explaining other reasons for petition', 'cfk_expunge_reexam_type_cd': 'Code representing the type of expunge reexamination', 'cfk_ptd_reason_cd': 'Code representing the reason for petition', 'last_mod_ts': 'The timestamp of when the record was last modified', 'proceeding_instituted_dt': 'Timestamp indicating the date and time when the proceeding was instituted', 'fk_proceeding_gid': 'Foreign key referencing the unique identifier of the proceeding in another table', 'lock_control_no': 'A number used for locking purposes', 'per_proceeding_year_no': 'Numeric value representing the year of the proceeding', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="petition",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CFK_LEGAL_BASIS_TYPE_CD': 'Code representing the type of legal basis', 'OTHER_LEGAL_BASIS_TX': 'Text representing any other legal basis', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_PROCEEDING_GID': 'Foreign key referencing the unique identifier of a proceeding'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="lop_legal_basis",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'current_in': 'Indicator if the proceeding is currently active', 'cfk_disposition_granted_rsn_cd': 'Code indicating the reason for granting disposition', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'CFK_SUBMISSION_METHOD_CD': 'COMMENT REQUIRED', 'received_dt': 'Date and time when the proceeding was received', 'lock_control_no': 'A number used for locking purposes', 'cfk_proceeding_type_cd': 'Code indicating the type of proceeding', 'proceeding_no': 'Number assigned to the proceeding', 'proceeding_gid': 'Unique identifier for each proceeding', 'dn_current_state_cd': 'Code indicating the current state of the proceeding', 'filing_dt': 'Date and time when the proceeding was filed', 'dn_current_state_dt': 'Date and time when the proceeding entered the current state', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'director_initiated_in': 'Indicator if the proceeding was initiated by the director'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_document_id': 'The ID of the document associated with the response', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'fk_proceeding_gid': 'The global ID of the proceeding associated with the response', 'response_received_dt': 'Date and time when the response was received', 'last_mod_ts': 'The timestamp of when the record was last modified', 'sequence_no': 'The sequence number of the response', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="petition_response_document",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'included_gds_srvcs_tx': 'The goods and services text that was included', 'create_ts': 'The timestamp of when the record was created', 'fk_proceeding_gid': 'The foreign key to unique identifier associated with the proceeding', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'dn_class_no': 'The class number', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_class_id': 'The foreign key associated with the unique identifier for a class'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LOP_LEGAL_BASIS_TRADEMARK_ID': 'Identifier for the legal basis', 'DN_REGISTRATION_NUM': 'Registration number associated with the trademark', 'CFK_TRADEMARK_GID': 'Global identifier for the trademark', 'FK_PROCEEDING_GID': 'Foreign key to the proceeding', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'DN_SERIAL_NUM': 'Serial number associated with the trademark', 'CFK_LEGAL_BASIS_TYPE_CD': 'Code for the legal basis type', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="lop_legal_basis_trademark",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_proceeding_gid': 'Unique identifier for the related proceeding', 'response_received_dt': 'Date and time when the response was received', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_response_type_cd': 'Code representing the type of response', 'response_statement_tx': 'Text containing the statement of the response', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="petition_response",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'error_msg': 'Error message describing the exception', 'error_num': 'Numeric code representing the type of error', 'backtrace': 'Stack trace of the exception', 'callstack': 'Call stack information of the exception', 'insert_ts': 'Timestamp when the exception was inserted'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="prcdng_trigger_exceptions",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'full_load': 'Indicates whether a full load is required for the CDC process.', 'source_db_name': 'The name of the source database for the CDC process.', 'table_name': 'The name of the table where the CDC batch job is running.', 'primary_keys': 'The primary keys of the table being captured by the CDC process.', 'catalog_name': 'The name of the catalog where the CDC batch job is running.', 'src_folder': 'The source folder where the CDC batch job is located.', 'initial_load_finished': 'Indicates whether the initial load for the CDC process has been finished or not.', 'source_table_name': 'The name of the source table for the CDC process.', 'database_name': 'The name of the database where the CDC batch job is running.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="cdc_batch_job_control",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cdc_file_path': 'The file path of the CDC (Change Data Capture) file.', 'processing_time': 'The timestamp of when the processing of the CDC file occurred.', 'meta_src_time': 'The timestamp of when the source data was last modified.', 'cdc_file_date': 'The date when the CDC file was created.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="cdc_batch_job_history",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_PROCEEDING_GID': 'Foreign key referencing the unique identifier of a proceeding', 'CREATE_USER_ID': 'The user ID that created the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_TS': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="letter_of_protest",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'transaction_instance_id': 'Identifier for the transaction instance', 'details_tx': 'Textual details or description of the transaction', 'proceeding_tran_instnc_gid': 'Unique identifier for each proceeding transaction instance', 'terminated_in': 'Location where the transaction was terminated', 'create_ts': 'The timestamp of when the record was created', 'cfk_employee_no': 'Employee number associated with the transaction', 'effective_ts': 'The timestamp of when the record is effective', 'origin_location_tx': 'Textual description of the origin location of the transaction', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_tran_instance",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'cfk_statement_type_cd': 'Code representing the type of statement', 'statement_tx': 'The descriptive statement text', 'last_mod_ts': 'The timestamp of when the record was last modified', 'fk_proceeding_gid': 'Foreign key referencing the proceeding table', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_statement",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'dn_at_bar_membership_state_nm': 'Name of the state for bar membership for the participant', 'create_ts': 'The timestamp of when the record was created', 'docket_reference_no': 'Reference number of the docket associated with the participant', 'at_attorney_affiliation_ct': 'Text field indicating the attorney affiliation count for the participant', 'at_bar_membership_year_no': 'Number indicating the year of bar membership for the participant', 'at_member_in_good_standing_in': 'Text field indicating the organization where the participant is a member in good standing', 'cfk_proceeding_prtcpnt_role_cd': 'Code indicating the role of the participant in the proceeding', 'at_bar_membership_assc_dt': 'Timestamp indicating the date of bar membership association for the participant', 'fk_proceeding_gid': 'Foreign key referencing the unique identifier of the proceeding', 'last_mod_user_id': 'The user ID that last modified the record', 'sequence_no': 'Number indicating the sequence of the participant in the proceeding', 'cfk_interested_party_gid': 'Foreign key referencing the unique identifier of the interested party', 'at_bar_jurisdiction_tx': 'Text field indicating the jurisdiction of bar membership for the participant', 'at_bar_membership_day_no': 'Number indicating the day of bar membership for the participant', 'at_canadian_registered_oed_nm': 'Name of the Canadian registered Office of Enrollment and Discipline for the participant', 'at_bar_membership_month_no': 'Number indicating the month of bar membership for the participant', 'lock_control_no': 'A number used for locking purposes', 'at_other_appointed_attys_tx': 'Text field indicating any other appointed attorneys for the participant', 'at_certify_in': 'Text field indicating the area of certification for the participant', 'at_attorney_bar_no': 'Bar number of the attorney for the participant', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_at_bar_membership_state_cd': 'Code indicating the state of bar membership for the participant'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_participant",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'proceeding_event_reason_cd': 'Code representing the proceeding event reason', 'last_mod_user_id': 'The user ID that last modified the record', 'description_tx': 'Description of the proceeding event reason', 'alert_trigger_ct': 'Count of alert triggers', 'begin_effective_dt': 'The timestamp of when the record began its effectiveness', 'create_ts': 'The timestamp of when the record was created', 'prosecution_history_in': 'Indicator for prosecution history', 'title_tx': 'Title of the proceeding event reason', 'proceeding_event_reason_id': 'Unique identifier for each proceeding event reason', 'end_effective_dt': 'The timestamp of when the record is no longer effective', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_fsm_type_event_id': 'Foreign key referencing the FSM type event', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_event_reason",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'PETITION_TO_DIRECTOR_CD': 'Code representing the petition to the director', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'TITLE_TX': 'Description of the petition', 'DESCRIPTION_TX': 'Description of the petition', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'LAST_MOD_USER_ID': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="stnd_petition_to_director",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'cfk_trademark_gid': 'Foreign key referencing the trademark identifier in another table', 'last_mod_user_id': 'The user ID that last modified the record', 'sequence_no': 'Integer value representing the sequence number of the record', 'fk_proceeding_gid': 'Foreign key referencing the proceeding identifier in another table', 'DN_SERIAL_NUM': 'COMMENT REQUIRED', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_mark",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'fee_item_count_no': 'Number of fee items', 'create_user_id': 'The user ID that created the record', 'fk_proceeding_gid': 'Foreign key referencing the proceeding table', 'fee_type_cd': 'Code representing the type of fee', 'fee_am': 'Amount of the fee', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_fee",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'FK_PROCEEDING_GID': 'Foreign key referencing the unique identifier of the proceeding', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CFK_INTERNATIONAL_APPL_GID': 'Foreign key referencing the unique identifier of the international application', 'DN_INTERNATIONAL_US_REF_NO': 'COMMENT REQUIRED', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_USER_ID': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_intl_appl",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'insert_ts': 'Timestamp when the exception was inserted', 'retry_ind': 'Indicator whether the exception should be retried', 'resolved_ts': 'Timestamp when the exception was resolved', 'tm_com_service_nm': 'Name of the communication service', 'tm_com_exception_id': 'Unique identifier for each exception', 'endpoint_type_cd': 'Code indicating the type of endpoint', 'source_ip': 'IP address of the source that triggered the exception', 'ref_no': 'Reference number associated with the exception', 'http_error_msg': 'Error message associated with the HTTP error', 'endpoint_body': 'Body of the request sent to the endpoint', 'http_error_cd': 'HTTP error code received from the endpoint', 'endpoint_url': 'URL of the endpoint that caused the exception'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="sync_tm_com_exception",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LOP_LEGAL_BASIS_CD': 'Code representing the legal basis for a line of business operation', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'DESCRIPTION_TX': 'Description of the legal basis', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'TITLE_TX': 'Description of the legal basis'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="stnd_lop_legal_basis",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'DN_SERIAL_NUM': 'Serial number associated with the trademark', 'CFK_LEGAL_BASIS_TYPE_CD': 'Code for the legal basis type', 'CFK_TRADEMARK_GID': 'Global identifier for the trademark', 'FK_PROCEEDING_GID': 'Foreign key to the proceeding', 'CREATE_TS': 'The timestamp of when the record was created', 'LOP_LEGAL_BASIS_TRADEMARK_ID': 'Identifier for the legal basis', 'DN_REGISTRATION_NUM': 'Registration number associated with the trademark', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'ACTION_CT': 'The action category executed on the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="lop_legal_basis_trademark_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'cfk_expunge_reexam_type_cd': 'Code representing the type of expunge reexamination', 'fk_proceeding_gid': 'Foreign key referencing the unique identifier of the proceeding in another table', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_ptr_reason_cd': 'Code representing the reason for pointer', 'lock_control_no': 'A number used for locking purposes', 'proceeding_instituted_dt': 'Timestamp indicating the date and time when the proceeding was instituted', 'create_ts': 'The timestamp of when the record was created', 'last_mod_ts': 'The timestamp of when the record was last modified', 'per_proceeding_year_no': 'Numeric value representing the year of the proceeding', 'per_proceeding_no': 'Numeric value representing the number of the proceeding', 'cfk_petition_type_cd': 'Code representing the type of petition', 'ptd_other_reason_explntn_tx': 'Text explaining other reasons for petition', 'create_user_id': 'The user ID that created the record', 'cfk_ptd_prir_ppr_submn_type_cd': 'Code representing the type of prior paper submission for petition', 'cfk_ptd_reason_cd': 'Code representing the reason for petition', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="petition_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'filing_dt': 'Date and time when the proceeding was filed', 'lock_control_no': 'A number used for locking purposes', 'CFK_SUBMISSION_METHOD_CD': 'COMMENT REQUIRED', 'director_initiated_in': 'Indicator if the proceeding was initiated by the director', 'create_user_id': 'The user ID that created the record', 'cfk_proceeding_type_cd': 'Code indicating the type of proceeding', 'received_dt': 'Date and time when the proceeding was received', 'proceeding_no': 'Number assigned to the proceeding', 'create_ts': 'The timestamp of when the record was created', 'dn_current_state_cd': 'Code indicating the current state of the proceeding', 'cfk_disposition_granted_rsn_cd': 'Code indicating the reason for granting disposition', 'last_mod_ts': 'The timestamp of when the record was last modified', 'dn_current_state_dt': 'Date and time when the proceeding entered the current state', 'last_mod_user_id': 'The user ID that last modified the record', 'proceeding_gid': 'Unique identifier for each proceeding', 'current_in': 'Indicator if the proceeding is currently active', 'action_ct': 'The action category executed on the record', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'response_received_dt': 'Date and time when the response was received', 'create_user_id': 'The user ID that created the record', 'fk_proceeding_gid': 'Unique identifier for the related proceeding', 'create_ts': 'The timestamp of when the record was created', 'cfk_response_type_cd': 'Code representing the type of response', 'lock_control_no': 'A number used for locking purposes', 'response_statement_tx': 'Text containing the statement of the response', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="petition_response_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'response_received_dt': 'Date and time when the response was received', 'lock_control_no': 'A number used for locking purposes', 'sequence_no': 'The sequence number of the response', 'cfk_document_id': 'The ID of the document associated with the response', 'fk_proceeding_gid': 'The global ID of the proceeding associated with the response', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="petition_response_document_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'dn_class_no': 'The class number', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'included_gds_srvcs_tx': 'The goods and services text that was included', 'last_mod_ts': 'The timestamp of when the record was last modified', 'cfk_class_id': 'The foreign key associated with the unique identifier for a class', 'fk_proceeding_gid': 'The foreign key to unique identifier associated with the proceeding', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_class_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'last_mod_user_id': 'The user ID that last modified the record', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'cfk_document_id': 'Foreign key referencing the document table', 'fk_proceeding_gid': 'Foreign key referencing the proceeding table', 'cfk_prcdng_document_type_cd': 'Code representing the type of document associated with the proceeding', 'action_ct': 'The action category executed on the record', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_document_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LOCK_CONTROL_NO': 'A number used for locking purposes', 'OTHER_LEGAL_BASIS_TX': 'Text representing any other legal basis', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_PROCEEDING_GID': 'Foreign key referencing the unique identifier of a proceeding', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CFK_LEGAL_BASIS_TYPE_CD': 'Code representing the type of legal basis', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'ACTION_CT': 'The action category executed on the record', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="lop_legal_basis_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_PROCEEDING_GID': 'Foreign key referencing the unique identifier of a proceeding', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'ACTION_CT': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="letter_of_protest_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'statement_tx': 'The descriptive statement text', 'fk_proceeding_gid': 'Foreign key referencing the proceeding table', 'last_mod_ts': 'The timestamp of when the record was last modified', 'lock_control_no': 'A number used for locking purposes', 'create_user_id': 'The user ID that created the record', 'last_mod_user_id': 'The user ID that last modified the record', 'cfk_statement_type_cd': 'Code representing the type of statement', 'create_ts': 'The timestamp of when the record was created', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_statement_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'fk_proceeding_gid': 'Foreign key referencing the unique identifier of the proceeding', 'create_ts': 'The timestamp of when the record was created', 'create_user_id': 'The user ID that created the record', 'lock_control_no': 'A number used for locking purposes', 'sequence_no': 'Number indicating the sequence of the participant in the proceeding', 'cfk_interested_party_gid': 'Foreign key referencing the unique identifier of the interested party', 'cfk_proceeding_prtcpnt_role_cd': 'Code indicating the role of the participant in the proceeding', 'at_attorney_bar_no': 'Bar number of the attorney for the participant', 'at_bar_jurisdiction_tx': 'Text field indicating the jurisdiction of bar membership for the participant', 'cfk_at_bar_membership_state_cd': 'Code indicating the state of bar membership for the participant', 'at_bar_membership_year_no': 'Number indicating the year of bar membership for the participant', 'at_bar_membership_month_no': 'Number indicating the month of bar membership for the participant', 'dn_at_bar_membership_state_nm': 'Name of the state for bar membership for the participant', 'at_bar_membership_assc_dt': 'Timestamp indicating the date of bar membership association for the participant', 'last_mod_user_id': 'The user ID that last modified the record', 'at_canadian_registered_oed_nm': 'Name of the Canadian registered Office of Enrollment and Discipline for the participant', 'at_bar_membership_day_no': 'Number indicating the day of bar membership for the participant', 'at_other_appointed_attys_tx': 'Text field indicating any other appointed attorneys for the participant', 'docket_reference_no': 'Reference number of the docket associated with the participant', 'at_member_in_good_standing_in': 'Text field indicating the organization where the participant is a member in good standing', 'at_certify_in': 'Text field indicating the area of certification for the participant', 'last_mod_ts': 'The timestamp of when the record was last modified', 'at_attorney_affiliation_ct': 'Text field indicating the attorney affiliation count for the participant', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_participant_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'CFK_INTERNATIONAL_APPL_GID': 'Foreign key referencing the unique identifier of the international application', 'CREATE_USER_ID': 'The user ID that created the record', 'DN_INTERNATIONAL_US_REF_NO': 'COMMENT REQUIRED', 'FK_PROCEEDING_GID': 'Foreign key referencing the unique identifier of the proceeding', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'ACTION_CT': 'The action category executed on the record', 'CFK_TRANSACTION_INSTANCE_GID': 'The foreign key referencing the unique identifier of the transaction instance', 'BEGIN_EFFECTIVE_TS': 'The timestamp of when the record began its effectiveness', 'END_EFFECTIVE_TS': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_intl_appl_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'fk_proceeding_gid': 'Foreign key referencing the proceeding table', 'lock_control_no': 'A number used for locking purposes', 'create_ts': 'The timestamp of when the record was created', 'fee_type_cd': 'Code representing the type of fee', 'last_mod_ts': 'The timestamp of when the record was last modified', 'last_mod_user_id': 'The user ID that last modified the record', 'fee_am': 'Amount of the fee', 'fee_item_count_no': 'Number of fee items', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_fee_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_ts': 'The timestamp of when the record was created', 'sequence_no': 'Integer value representing the sequence number of the record', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'DN_SERIAL_NUM': 'COMMENT REQUIRED', 'create_user_id': 'The user ID that created the record', 'cfk_trademark_gid': 'Foreign key referencing the trademark identifier in another table', 'fk_proceeding_gid': 'Foreign key referencing the proceeding identifier in another table', 'end_effective_ts': 'The timestamp of when the record is no longer effective', 'cfk_transaction_instance_gid': 'The foreign key referencing the unique identifier of the transaction instance', 'begin_effective_ts': 'The timestamp of when the record began its effectiveness', 'action_ct': 'The action category executed on the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmproceeding_catalog,
        database=database,
        table="proceeding_mark_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

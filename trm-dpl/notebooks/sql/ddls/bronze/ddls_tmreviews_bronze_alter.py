# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmreviews-conf.yaml"
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
tmreviews_catalog = common_configs["schema"]["trgt_catalog"]
data_quality_catalog = common_configs["schema"]["data_quality_catalog"]
print(f"{tmreviews_catalog=}, {data_quality_catalog=}")

# spark.conf.set("config.data_quality_catalog", data_quality_catalog.lower())
# spark.conf.set("conf.catalog", tmreviews_catalog.lower())
# spark.conf.set("dbx_env", dbx_env)

# COMMAND ----------

database = "bronze"
control_table = "cdc_batch_job_control"
job_history_table = "cdc_batch_job_history"

spark.conf.set("conf.catalog", tmreviews_catalog)
spark.conf.set("conf.database", database)
spark.conf.set("conf.control_table", control_table)
spark.conf.set("conf.job_history_table", job_history_table)
spark.conf.set("conf.dbx_env", dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

tables_to_comment = {
    "post_reg_review_notice": "The post_reg_review_notice table contains data related to post-registration review notices. It includes information such as the trademark ID, creation date, pay period range name, random number, serial number, registration number, reviewee worker number, production transaction code, business event reason code, order number, appeal status code, appeal submission date, appeal end date, lead assigned date, lead assigned worker number, level 1 assigned date,  manager assigned worker number,  manager assigned date, and extension indicator. This table is significant to the business as it helps track and manage post-registration review notices and their associated details.",
    "post_reg_quality_review": "The post_reg_quality_review table contains data related to quality reviews of post-registration events. It includes information such as the trademark global identifier, the date the review was created, a random number, the order number, the object type code, the serial number, the registration number, the business event reason code, whether an appeal was made, the date the appeal was completed, the date the appeal was received, the worker number of the appeal co-reviewer, the worker number of the co-reviewer, whether a COP (Certificate of Publication) was made, the follow-up date, whether a follow-up was made, the date the lead was assigned, the worker number of the lead assigned, the date level 1 was assigned, and the worker number of the level 3 manager assigned.",
    "cdc_batch_job_history": "The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs in the TRM Tmproceeding Dev system. This table stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. The data in this table is important for tracking and analyzing the execution of CDC batch jobs, providing insights into the timing and progress of data changes in the system.",
    "post_reg_quality_review_errlog": "The post_reg_quality_review_errlog table contains data related to error logs generated during the quality review process after trademark registration. It captures information such as error numbers, error messages, error types, and tags associated with the errors. Additionally, it includes details about the trademark, creation date, random number, order number, object type code, serial number, registration number, business event reason code, appeal status, appeal completion date, appeal receipt date, appeal reviewer worker number, co-reviewer worker number, COP status, follow-up date, and follow-up status. This table is essential for tracking and resolving quality issues in trademark registrations.",
    "post_reg_review_notice_errlog": "The post_reg_review_notice_errlog table contains data related to error logs generated during the post-registration review process. It captures information such as error numbers, error messages, error types, and tags associated with the errors. Additionally, it includes details about the trademark, pay period range name, random number, serial number, registration number, reviewee worker number, production transaction code, business event reason code, order number, appeal status, appeal submission and end dates, lead assignment date, and assigned worker number. This table is essential for tracking and resolving errors encountered during the post-registration review process.",
    "cdc_batch_job_control": "The cdc_batch_job_control table in the bronze schema of the trm_tmproceeding catalog contains information about the control and status of Change Data Capture (CDC) batch jobs. It provides details such as the source folder, catalog name, database name, and table name for the CDC process. Additionally, it includes information about the source database and table names, primary keys, and whether a full load is required. The table also tracks the status of the initial load, indicating whether it has been finished or not through a boolean value. This table is essential for monitoring and managing CDC batch jobs in the business.",
    "pre_exam_quality_rvw_err": "The pre_exam_quality_rvw_err table contains data related to errors found during the pre-examination quality review process. It includes information such as the trademark ID, error field number, serial number, completion date, creation date, department code, error explanation, reviewer and reviewee worker numbers, pay period range name, review status code, review level code, AMQE reason, lock control number, creation and last modification timestamps, and user IDs. This table is significant to the business as it helps track and manage errors in the quality review process, allowing for improvements in the trademark examination workflow.",
    "preg_quality_review_element": "The preg_quality_review_element table contains data related to the quality review process for trademarks. It includes information such as the trademark global identifier, creation date, random number, examination element code, entry number, serial number, registration number, appeal comments, appeal notes, appeal status code, creation date, free point in, original severity code, pay period range name, query comments, query text, review type code, reviewee worker number, reviewer worker number, and severity code. This table is significant to the business as it allows for the tracking and analysis of quality review activities for trademarks.",
    "preg_quality_review_element_errlog": "The preg_quality_review_element_errlog table in the trm_tmreviews database schema contains data related to errors encountered during the quality review process for trademark registrations. It includes information such as error numbers, error messages, error types, tags, trade mark global IDs, creation dates, random numbers, examination element codes, entry numbers, serial numbers, registration numbers, appeal comments, appeal notes, appeal status codes, creation dates, free points, original severity codes, pay period range names, query comments, and queries. This table is essential for tracking and resolving quality issues in trademark registrations.",
    "pre_exam_quality_review": "The pre_exam_quality_review table contains data related to quality reviews conducted before the examination of trademarks. It includes information such as the trademark ID, serial number, appeal status, creation date, department code, lead worker assignment date, manager worker assignment date, BCR pay period range name, random number, review status code, reviewee worker number, upload count, uploaded date, AMQ reason, lock control number, creation timestamp, and user ID. This table is significant to the business as it helps track and analyze the quality of trademark examinations and identify areas for improvement in the pre-examination process.",
    "post_reg_quality_review_h": "[HISTORICAL] The post_reg_quality_review table contains data related to quality reviews of post-registration events. It includes information such as the trademark global identifier, the date the review was created, a random number, the order number, the object type code, the serial number, the registration number, the business event reason code, whether an appeal was made, the date the appeal was completed, the date the appeal was received, the worker number of the appeal co-reviewer, the worker number of the co-reviewer, whether a COP (Certificate of Publication) was made, the follow-up date, whether a follow-up was made, the date the lead was assigned, the worker number of the lead assigned, the date level 1 was assigned, and the worker number of the level 3 manager assigned.",
    "post_reg_review_notice_h": "[HISTORICAL] The post_reg_review_notice table contains data related to post-registration review notices. It includes information such as the trademark ID, creation date, pay period range name, random number, serial number, registration number, reviewee worker number, production transaction code, business event reason code, order number, appeal status code, appeal submission date, appeal end date, lead assigned date, lead assigned worker number, level 1 assigned date, manager assigned worker number, manager assigned date, and extension indicator. This table is significant to the business as it helps track and manage post-registration review notices and their associated details.",
    "preg_quality_review_element_h": "[HISTORICAL] The preg_quality_review_element table contains data related to the quality review process for trademarks. It includes information such as the trademark global identifier, creation date, random number, examination element code, entry number, serial number, registration number, appeal comments, appeal notes, appeal status code, creation date, free point in, original severity code, pay period range name, query comments, query text, review type code, reviewee worker number, reviewer worker number, and severity code. This table is significant to the business as it allows for the tracking and analysis of quality review activities for trademarks.",
}

for table_name, comment in tables_to_comment.items():
    alter_table_query = f"""
    ALTER TABLE {tmreviews_catalog}.{database}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"CREATE_USER_ID": "The user ID that created the record", "DN_PRN_RSN": "Reason code for DN", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "CFK_PREG_MGR_ASSIGNED_WRKR_NO": "Worker number of the manager assigned", "CREATED_DT": "Date when the record was created", "LEAD_ASSIGNED_DT": "Date when the lead was assigned", "CFK_APPEAL_STATUS_CD": "Status code for the appeal", "CFK_QUERY_STATUS_CD": "Status code for the query", "APPEAL_IN": "Indicator for appeal submission", "FOLLOWUP_DT": "Date when the follow-up is scheduled", "EXTENSION_IN": "Indicator for extension", "LEVEL_1_ASSIGNED_DT": "Date when level 1 was assigned", "TRANSACTION_SYSTEM_DT": "Date when the transaction was recorded in the system", "DN_PRODUCTION_TRANSACTION_CD": "Production transaction code of the DN", "DN_BUSINESS_EVENT_REASON_CD": "Business event reason code of the DN", "CREATE_TS": "The timestamp of when the record was created", "CFK_REVIEW_TYPE_CD": "Type code for the review", "LAST_MOD_USER_ID": "The user ID that last modified the record", "RANDOM_NO": "Random number assigned to the record", "PREG_MANAGER_ASSIGNED_DT": "Date when the manager was assigned", "LOCK_CONTROL_NO": "A number used for locking purposes", "CFK_LEAD_ASSIGNED_WORKER_NO": "Worker number of the lead assigned", "DN_REGISTRATION_NUM": "Registration number of the DN", "APPEAL_END_DT": "Date when the appeal ended", "FOLLOWUP_IN": "Indicator for follow-up", "LAST_MOD_TS": "The timestamp of when the record was last modified", "CFK_BE_ORDER_NO": "Order number for the business event", "DELETE_IN": "Indicator for deletion", "APPEAL_SUBMITTED_DT": "Date when the appeal was submitted", "DN_SERIAL_NUM_TX": "Serial number of the DN", "CFK_TRADEMARK_GID": "Unique identifier for the trademark", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range for BCR"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="post_reg_review_notice",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"CFK_PREG_MGR_ASSIGNED_WRKR_NO": "Worker number of the manager assigned to the post-registration event", "CFK_QUERY_STATUS_CD": "Code indicating the status of the query", "DELETE_IN": "Indicator if the record has been deleted", "PREG_MANAGER_ASSIGNED_DT": "Date when a manager was assigned to the post-registration event", "APPEAL_RECEIPT_DT": "Date when the appeal was received", "RANDOM_NO": "Random number assigned to the record", "DN_BUSINESS_EVENT_REASON_CD": "Code indicating the reason for the business event", "CREATE_USER_ID": "The user ID that created the record", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range associated with the BCR", "LOCK_CONTROL_NO": "A number used for locking purposes", "TRANSACTION_SYSTEM_DT": "Date when the transaction was recorded in the system", "CFK_OBJECT_TYPE_CD": "Code indicating the type of object", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "DN_PQR_RSN": "Reason code for the post-registration quality review", "DN_REGISTRATION_NUM": "Registration number of the trademark", "CREATED_DT": "Date when the record was created", "CFK_LEVEL_3_MGR_ASGND_WRKR_NO": "Worker number of the level 3 manager assigned", "CFK_LEAD_ASSIGNED_WORKER_NO": "Worker number of the lead assigned", "CFK_APPEAL_CO_REVIEWR_WRKR_NO": "Worker number of the appeal co-reviewer", "FOLLOWUP_IN": "Indicator if a follow-up action is required", "CFK_TRADEMARK_GID": "Unique identifier for the trademark", "FOLLOWUP_DT": "Date when a follow-up action is required", "CFK_REVIEW_STATUS_CD": "Code indicating the status of the review", "DN_SERIAL_NUM_TX": "Serial number of the trademark", "CFK_CO_REVIEWER_WORKER_NO": "Worker number of the co-reviewer", "DN_PRODUCTION_TRANSACTION_CD": "Code indicating the type of production transaction", "LAST_MOD_TS": "The timestamp of when the record was last modified", "COP_IN": "Indicator if a COP (Certificate of Publication) has been received", "APPEAL_COMPLETED_DT": "Date when the appeal was completed", "LEAD_ASSIGNED_DT": "Date when a lead was assigned", "CFK_REVIEW_TYPE_CD": "Code indicating the type of review", "APPEAL_IN": "Indicator if an appeal has been made", "CFK_BE_ORDER_NO": "Order number associated with the trademark", "LEVEL_1_ASSIGNED_DT": "Date when a level 1 assignment was made", "REVIEW_COMPLETED_DT": "Date when the review was completed", "CREATE_TS": "The timestamp of when the record was created", "CFK_REVIEWER_WORKER_NO": "Worker number of the reviewer", "LAST_MOD_USER_ID": "The user ID that last modified the record"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="post_reg_quality_review",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"cdc_file_date": "The data of the change data capture of the file", "processing_time": "The time when the record was processed", "meta_src_time": "The metadata source time", "cdc_file_path": "The path of the change data capture file"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="cdc_batch_job_history",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"APPEAL_IN": "Indicator for appeal", "APPEAL_COMPLETED_DT": "Date when the appeal was completed", "LAST_MOD_USER_ID": "The user ID that last modified the record", "CFK_REVIEW_TYPE_CD": "Code representing the review type", "CFK_REVIEW_STATUS_CD": "Code representing the review status", "DN_BUSINESS_EVENT_REASON_CD": "Code representing the reason for the business event", "DN_REGISTRATION_NUM": "Registration number of the DN", "CFK_TRADEMARK_GID": "Global ID of the trademark", "DN_PQR_RSN": "Reason for DN PQR (Post Quality Review)", "LAST_MOD_TS": "The timestamp of when the record was last modified", "CFK_CO_REVIEWER_WORKER_NO": "Worker number of the co-reviewer", "CFK_QUERY_STATUS_CD": "Code representing the query status", "APPEAL_RECEIPT_DT": "Date when the appeal was received", "CREATE_TS": "The timestamp of when the record was created", "ORA_ERR_MESG": "Error message associated with the Oracle error", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range for BCR", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "ORA_ERR_NUMBER": "Error number associated with the Oracle error", "ORA_ERR_OPTYP": "Operation type that caused the Oracle error", "DN_SERIAL_NUM_TX": "Serial number of the DN", "RANDOM_NO": "Random number", "PREG_MANAGER_ASSIGNED_DT": "Date when the  manager was assigned", "CFK_REVIEWER_WORKER_NO": "Worker number of the reviewer", "CFK_OBJECT_TYPE_CD": "Code representing the object type", "CFK_BE_ORDER_NO": "Order number associated with the business event", "CREATED_DT": "Date when the record was created", "CFK_PREG_MGR_ASSIGNED_WRKR_NO": "Worker number of the  manager assigned", "CREATE_USER_ID": "The user ID that created the record", "CFK_LEAD_ASSIGNED_WORKER_NO": "Worker number of the lead assigned", "REVIEW_COMPLETED_DT": "Date when the review was completed", "TRANSACTION_SYSTEM_DT": "Date of the transaction in the system", "LOCK_CONTROL_NO": "A number used for locking purposes", "LEVEL_1_ASSIGNED_DT": "Date when level 1 was assigned", "CFK_LEVEL_3_MGR_ASGND_WRKR_NO": "Worker number of level 3 manager assigned", "FOLLOWUP_DT": "Date for follow-up", "FOLLOWUP_IN": "Indicator for follow-up", "DN_PRODUCTION_TRANSACTION_CD": "Code representing the production transaction of DN", "LEAD_ASSIGNED_DT": "Date when the lead was assigned", "DELETE_IN": "Indicator for deletion of the record", "COP_IN": "Indicator for COP (Certificate of Publication)", "ORA_ERR_TAG": "Tag associated with the Oracle error", "CFK_APPEAL_CO_REVIEWR_WRKR_NO": "Worker number of the appeal co-reviewer"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="post_reg_quality_review_errlog",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"APPEAL_SUBMITTED_DT": "Date when the appeal was submitted", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "APPEAL_IN": "Indicator for appeal status", "CREATED_DT": "Timestamp indicating when the record was created", "DN_REGISTRATION_NUM": "Registration number of the DN", "DN_PRODUCTION_TRANSACTION_CD": "Production transaction code of the DN", "DN_BUSINESS_EVENT_REASON_CD": "Business event reason code of the DN", "ORA_ERR_NUMBER": "Error number associated with the Oracle error", "ORA_ERR_MESG": "Error message associated with the Oracle error", "RANDOM_NO": "Random number", "APPEAL_END_DT": "Date when the appeal ended", "ORA_ERR_TAG": "Tag associated with the Oracle error", "ORA_ERR_OPTYP": "Operation type that caused the Oracle error", "CFK_APPEAL_STATUS_CD": "Appeal status code", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range for BCR", "DN_SERIAL_NUM_TX": "Serial number of the DN", "CFK_BE_ORDER_NO": "Order number of the business event", "CFK_TRADEMARK_GID": "Global ID of the trademark", "CFK_LEAD_ASSIGNED_WORKER_NO": "Worker number of the lead assigned", "DN_PRN_RSN": "Reason for DN (Decision Notice) printing", "CFK_REVIEW_TYPE_CD": "Review type code", "LOCK_CONTROL_NO": "A number used for locking purposes", "PREG_MANAGER_ASSIGNED_DT": "Timestamp indicating when the  Manager was assigned", "DELETE_IN": "Indicator for deletion status", "EXTENSION_IN": "Indicator for extension status", "CREATE_USER_ID": "The user ID that created the record", "LAST_MOD_TS": "The timestamp of when the record was last modified", "LEVEL_1_ASSIGNED_DT": "Timestamp indicating when the Level 1 was assigned", "CFK_QUERY_STATUS_CD": "Query status code", "FOLLOWUP_IN": "Indicator for follow-up status", "FOLLOWUP_DT": "Date when the follow-up was conducted", "TRANSACTION_SYSTEM_DT": "Timestamp indicating when the transaction was processed by the system", "CREATE_TS": "The timestamp of when the record was created", "CFK_PREG_MGR_ASSIGNED_WRKR_NO": "Worker number of the assigned  Manager", "LAST_MOD_USER_ID": "The user ID that last modified the record", "LEAD_ASSIGNED_DT": "Timestamp indicating when the lead was assigned"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="post_reg_review_notice_errlog",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"database_name": "The name of the database where the table is located.", "source_db_name": "The name of the source database.", "table_name": "The name of the table.", "src_folder": "The folder where the source data is stored.", "initial_load_finished": "Indicates if the initial load of the table has finished.", "primary_keys": "The primary keys of the table.", "catalog_name": "The name of the catalog where the table is located.", "full_load": "Indicates if the table needs to be fully loaded.", "source_table_name": "The name of the source table."}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="cdc_batch_job_control",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"cfk_reviewee_worker_no": "Worker number of the reviewee", "cfk_trademark_gid": "Global ID of the trademark", "dn_serial_num_tx": "Textual representation of the serial number", "cfk_review_level_cd": "Code representing the review level", "cfk_bcr_pay_period_range_name": "Name of the pay period range for BCR", "cfk_error_field_cd": "Code representing the error field", "last_mod_ts": "The timestamp of when the record was last modified", "completed_dt": "Timestamp indicating the completion date", "dn_amqe_rsn": "Reason for AMQE (Automated Manual Quality Examination)", "created_dt": "Timestamp indicating the creation date", "lock_control_no": "A number used for locking purposes", "error_explanation_tx": "Text explaining the error", "create_user_id": "The user ID that created the record", "cfk_review_status_cd": "Code representing the review status", "cfk_department_cd": "Code representing the department", "error_field_no": "Number indicating the error field", "last_mod_user_id": "The user ID that last modified the record", "cfk_reviewer_worker_no": "Worker number of the reviewer", "create_ts": "The timestamp of when the record was created", "delete_in": "Indication of deletion"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="pre_exam_quality_rvw_err",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"CREATE_USER_ID": "The user ID that created the record", "QUERY_COMMENTS_TX": "Comments related to the query", "LAST_MOD_TS": "The timestamp of when the record was last modified", "LOCK_CONTROL_NO": "A number used for locking purposes", "CFK_APPEAL_STATUS_CD": "Code representing the appeal status", "CFK_REVIEWER_WORKER_NO": "Worker number of the reviewer", "CREATE_TS": "The timestamp of when the record was created", "CFK_TRADEMARK_GID": "Unique identifier for a trademark", "DN_SERIAL_NUM_TX": "Serial number of the DN", "FK_PRQR_CREATED_DT": "Date when the record was created", "FREE_POINT_IN": "The free point indicator", "DN_PQRE_RSN": "Reason for the quality review element", "CFK_ORIGINAL_SEVERITY_CD": "Code representing the original severity", "CFK_SEVERITY_CD": "Code representing the severity", "DELETE_IN": "Indicator for deletion status of the record", "CREATED_DT": "Date when the record was created", "CFK_EXAMINATION_ELEMENT_CD": "Code representing the examination element", "CFK_REVIEW_TYPE_CD": "Code representing the review type", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "APPEAL_COMMENTS_TX": "Comments related to the appeal", "DN_REGISTRATION_NUM": "Registration number of the DN", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range for BCR", "QUERY_TX": "Query information", "ENTRY_NO": "Number associated with the entry", "FK_PRQR_RANDOM_NO": "Random number associated with the record", "LAST_MOD_USER_ID": "The user ID that last modified the record", "APPEAL_NOTES_TX": "Notes related to the appeal"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="preg_quality_review_element",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"LOCK_CONTROL_NO": "A number used for locking purposes", "CFK_TRADEMARK_GID": "The foreign key referencing the global ID of the trademark registration.", "FK_PRQR_CREATED_DT": "The creation date of the quality review element.", "FK_PRQR_RANDOM_NO": "The random number associated with the quality review element.", "DN_REGISTRATION_NUM": "The registration number of the trademark registration.", "QUERY_COMMENTS_TX": "The comments associated with a query made for the quality review element.", "QUERY_TX": "The query associated with the quality review element.", "CFK_SEVERITY_CD": "The foreign key referencing the severity code associated with the quality review element.", "APPEAL_COMMENTS_TX": "The comments associated with an appeal made for the quality review element.", "DELETE_IN": "Indicates whether the quality review element is marked for deletion.", "CREATED_DT": "The creation date of the quality review element error log entry.", "LAST_MOD_USER_ID": "The user ID that last modified the record", "ORA_ERR_NUMBER": "The error number associated with the error encountered during the quality review process for trademark registrations.", "DN_PQRE_RSN": "The reason associated with the quality review element.", "ENTRY_NO": "The entry number associated with the quality review element.", "CFK_REVIEWEE_WORKER_NO": "The foreign key referencing the worker number of the reviewee associated with the quality review element.", "CFK_APPEAL_STATUS_CD": "The foreign key referencing the status code of an appeal made for the quality review element.", "CREATE_USER_ID": "The user ID that created the record", "CFK_REVIEW_TYPE_CD": "The foreign key referencing the review type code associated with the quality review element.", "CFK_REVIEWER_WORKER_NO": "The foreign key referencing the worker number of the reviewer associated with the quality review element.", "ORA_ERR_OPTYP": "The operation type associated with the error encountered during the quality review process for trademark registrations.", "LAST_MOD_TS": "The timestamp of when the record was last modified", "ORA_ERR_MESG": "The error message associated with the error encountered during the quality review process for trademark registrations.", "DN_SERIAL_NUM_TX": "The serial number of the trademark registration.", "CFK_EXAMINATION_ELEMENT_CD": "The foreign key referencing the examination element code.", "CFK_ORIGINAL_SEVERITY_CD": "The foreign key referencing the original severity code of the quality review element.", "CREATE_TS": "The timestamp of when the record was created", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "The foreign key referencing the pay period range name associated with the quality review element.", "ORA_ERR_TAG": "The tag associated with the error encountered during the quality review process for trademark registrations.", "APPEAL_NOTES_TX": "The notes associated with an appeal made for the quality review element.", "FREE_POINT_IN": "The free point information associated with the quality review element."}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="preg_quality_review_element_errlog",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"cfk_trademark_gid": "The unique identifier for the trademark in the system", "last_mod_ts": "The timestamp of when the record was last modified", "cfk_review_status_cd": "The code representing the status of the quality review", "random_no": "A randomly generated number for the quality review", "create_ts": "The timestamp of when the record was created", "upload_count_qt": "The count of uploads made for the quality review", "uploaded_dt": "The date when the quality review was uploaded", "cfk_bcr_pay_period_range_name": "The name of the pay period range for the quality review", "last_mod_user_id": "The user ID that last modified the record", "lock_control_no": "A number used for locking purposes", "dn_amq_rsn": "The reason code for any AMQ (Additional Material Required) in the quality review", "appeal_in": "Indicates if there is an appeal for the trademark", "cfk_reviewee_worker_no": "The unique identifier for the worker being reviewed in the quality review", "cfk_lead_worker_no": "The unique identifier for the lead worker assigned to the quality review", "lead_assigned_dt": "The date when a lead worker was assigned to the quality review", "dn_serial_num_tx": "The serial number of the trademark", "create_user_id": "The user ID that created the record", "cop_in": "Indicates if there is a change of ownership for the trademark", "created_dt": "The date when the quality review was created", "cfk_department_cd": "The code representing the department responsible for the quality review", "manager_assigned_dt": "The date when a manager worker was assigned to the quality review", "cfk_manager_worker_no": "The unique identifier for the manager worker assigned to the quality review", "delete_in": "Indicates if the quality review is marked for deletion"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="pre_exam_quality_review",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"APPEAL_IN": "Indicator if an appeal has been made", "CFK_LEVEL_3_MGR_ASGND_WRKR_NO": "Worker number of the level 3 manager assigned", "FOLLOWUP_DT": "Date when a follow-up action is required", "CFK_LEAD_ASSIGNED_WORKER_NO": "Worker number of the lead assigned", "DN_BUSINESS_EVENT_REASON_CD": "Code indicating the reason for the business event", "FOLLOWUP_IN": "Indicator if a follow-up action is required", "CFK_OBJECT_TYPE_CD": "Code indicating the type of object", "DN_REGISTRATION_NUM": "Registration number of the trademark", "CFK_PREG_MGR_ASSIGNED_WRKR_NO": "Worker number of the manager assigned to the post-registration event", "CFK_BE_ORDER_NO": "Order number associated with the trademark", "CFK_APPEAL_CO_REVIEWR_WRKR_NO": "Worker number of the appeal co-reviewer", "DN_PQR_RSN": "Reason code for the post-registration quality review", "DN_PRODUCTION_TRANSACTION_CD": "Code indicating the type of production transaction", "CFK_REVIEW_TYPE_CD": "Code indicating the type of review", "CREATED_DT": "Date when the record was created", "COP_IN": "Indicator if a COP (Certificate of Publication) has been received", "LAST_MOD_TS": "The timestamp of when the record was last modified", "APPEAL_RECEIPT_DT": "Date when the appeal was received", "CREATE_USER_ID": "The user ID that created the record", "PREG_MANAGER_ASSIGNED_DT": "Date when a manager was assigned to the post-registration event", "CREATE_TS": "The timestamp of when the record was created", "DN_SERIAL_NUM_TX": "Serial number of the trademark", "LOCK_CONTROL_NO": "A number used for locking purposes", "REVIEW_COMPLETED_DT": "Date when the review was completed", "TRANSACTION_SYSTEM_DT": "Date when the transaction was recorded in the system", "CFK_QUERY_STATUS_CD": "Code indicating the status of the query", "LEAD_ASSIGNED_DT": "Date when a lead was assigned", "CFK_REVIEWER_WORKER_NO": "Worker number of the reviewer", "CFK_REVIEW_STATUS_CD": "Code indicating the status of the review", "CFK_CO_REVIEWER_WORKER_NO": "Worker number of the co-reviewer", "RANDOM_NO": "Random number assigned to the record", "APPEAL_COMPLETED_DT": "Date when the appeal was completed", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range associated with the BCR", "CFK_TRADEMARK_GID": "Unique identifier for the trademark", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "LAST_MOD_USER_ID": "The user ID that last modified the record", "LEVEL_1_ASSIGNED_DT": "Date when a level 1 assignment was made", "DELETE_IN": "Indicator if the record has been deleted", "BEGIN_EFFECTIVE_TS": "The timestamp of when the record began its effectiveness", "ACTION_CT": "The action category executed on the record", "END_EFFECTIVE_TS": "The timestamp of when the record is no longer effective", "CFK_TRANSACTION_INSTANCE_GID": "The foreign key referencing the unique identifier of the transaction instance"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="post_reg_quality_review_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"CFK_BE_ORDER_NO": "Order number for the business event", "CFK_REVIEW_TYPE_CD": "Type code for the review", "DN_REGISTRATION_NUM": "Registration number of the DN", "FOLLOWUP_DT": "Date when the follow-up is scheduled", "TRANSACTION_SYSTEM_DT": "Date when the transaction was recorded in the system", "DN_BUSINESS_EVENT_REASON_CD": "Business event reason code of the DN", "LAST_MOD_USER_ID": "The user ID that last modified the record", "CREATE_USER_ID": "The user ID that created the record", "LAST_MOD_TS": "The timestamp of when the record was last modified", "DELETE_IN": "Indicator for deletion", "CFK_TRADEMARK_GID": "Unique identifier for the trademark", "EXTENSION_IN": "Indicator for extension", "DN_PRN_RSN": "Reason code for DN", "LEVEL_1_ASSIGNED_DT": "Date when level 1 was assigned", "LEAD_ASSIGNED_DT": "Date when the lead was assigned", "DN_PRODUCTION_TRANSACTION_CD": "Production transaction code of the DN", "CFK_LEAD_ASSIGNED_WORKER_NO": "Worker number of the lead assigned", "DN_SERIAL_NUM_TX": "Serial number of the DN", "CFK_APPEAL_STATUS_CD": "Status code for the appeal", "LOCK_CONTROL_NO": "A number used for locking purposes", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "FOLLOWUP_IN": "Indicator for follow-up", "APPEAL_SUBMITTED_DT": "Date when the appeal was submitted", "PREG_MANAGER_ASSIGNED_DT": "Date when the  manager was assigned", "RANDOM_NO": "Random number assigned to the record", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range for BCR", "APPEAL_IN": "Indicator for appeal submission", "CFK_QUERY_STATUS_CD": "Status code for the query", "CREATED_DT": "Date when the record was created", "CFK_PREG_MGR_ASSIGNED_WRKR_NO": "Worker number of the  manager assigned", "CREATE_TS": "The timestamp of when the record was created", "APPEAL_END_DT": "Date when the appeal ended", "END_EFFECTIVE_TS": "The timestamp of when the record is no longer effective", "ACTION_CT": "The action category executed on the record", "CFK_TRANSACTION_INSTANCE_GID": "The foreign key referencing the unique identifier of the transaction instance", "BEGIN_EFFECTIVE_TS": "The timestamp of when the record began its effectiveness"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="post_reg_review_notice_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {"DN_SERIAL_NUM_TX": "Serial number of the DN", "CREATED_DT": "Date when the record was created", "CFK_EXAMINATION_ELEMENT_CD": "Code representing the examination element", "CREATE_TS": "The timestamp of when the record was created", "QUERY_TX": "Query information", "CFK_ORIGINAL_SEVERITY_CD": "Code representing the original severity", "FK_PRQR_CREATED_DT": "Date when the record was created", "QUERY_COMMENTS_TX": "Comments related to the query", "FREE_POINT_IN": "The free point indicator", "CFK_REVIEWEE_WORKER_NO": "Worker number of the reviewee", "CREATE_USER_ID": "The user ID that created the record", "CFK_REVIEW_TYPE_CD": "Code representing the review type", "CFK_TRADEMARK_GID": "Unique identifier for a trademark", "DN_REGISTRATION_NUM": "Registration number of the DN", "LAST_MOD_USER_ID": "The user ID that last modified the record", "DELETE_IN": "Indicator for deletion status of the record", "CFK_SEVERITY_CD": "Code representing the severity", "LOCK_CONTROL_NO": "A number used for locking purposes", "APPEAL_COMMENTS_TX": "Comments related to the appeal", "CFK_REVIEWER_WORKER_NO": "Worker number of the reviewer", "LAST_MOD_TS": "The timestamp of when the record was last modified", "FK_PRQR_RANDOM_NO": "Random number associated with the record", "CFK_BCR_PAY_PERIOD_RANGE_NAME": "Name of the pay period range for BCR", "CFK_APPEAL_STATUS_CD": "Code representing the appeal status", "DN_PQRE_RSN": "Reason for the quality review element", "APPEAL_NOTES_TX": "Notes related to the appeal", "ENTRY_NO": "Number associated with the entry", "CFK_TRANSACTION_INSTANCE_GID": "The foreign key referencing the unique identifier of the transaction instance", "ACTION_CT": "The action category executed on the record", "END_EFFECTIVE_TS": "The timestamp of when the record is no longer effective", "BEGIN_EFFECTIVE_TS": "The timestamp of when the record began its effectiveness"}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT "{comment}"
    """.format(
        catalog=tmreviews_catalog,
        database=database,
        table="preg_quality_review_element_h",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

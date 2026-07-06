# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngidmp-conf.yaml"
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
tmngidmp_catalog = common_configs["schema"]["trgt_catalog"]
data_quality_catalog = common_configs["schema"]["data_quality_catalog"]
print(f"{tmngidmp_catalog=}, {data_quality_catalog=}")


# spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
# spark.conf.set('conf.catalog', tmngidmp_catalog.lower())
# spark.conf.set('dbx_env', dbx_env)

# COMMAND ----------

database = "bronze"
control_table = "cdc_batch_job_control"
job_history_table = "cdc_batch_job_history"

spark.conf.set("conf.catalog", tmngidmp_catalog)
spark.conf.set("conf.database", database)
spark.conf.set("conf.control_table", control_table)
spark.conf.set("conf.job_history_table", job_history_table)
spark.conf.set("conf.dbx_env", dbx_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};

# COMMAND ----------

tables_to_comment = {'goods_services_term': "The goods_services_term table contains information about the terms and conditions for goods and services offered by the business. It includes details such as the term ID, modification number, description, term status, author employee number, acceptance partnership date, edition number, version number, release number, previous goods/services term ID, effective dates, and timestamps for creation and last modification. This table is essential for managing and tracking the various terms and conditions associated with the businessgoods and services.", 'international_class_version': 'The international_class_version table contains data related to the versions of international classes. It represents the different versions of international classes used in the business. The table includes information such as the class ID, edition number, version number, effective dates, and user details for creation and modification. This table is essential for tracking and managing the changes and updates made to international classes over time.', 'goods_services_term_note': 'The goods_services_term_note table contains information about notes related to goods and services terms. It includes details such as the note code, employee number, note date, creation timestamp, creator user ID, last modification timestamp, last modifier user ID, note text, and the term ID associated with the note. This table is important for tracking and managing notes related to goods and services terms within the business.', 'data_comp_result': 'The data_comp_result table contains information about trademark applications. It includes details such as the serial number, class, original text, filing date, case status, goods description, misclassified status, TEAS plus status, literal, status date, examining attorney, and law office. This table is significant to the business as it provides a comprehensive view of trademark applications and their associated details. It allows for tracking the progress and status of trademark applications, as well as analyzing the goods description and potential misclassification. The data in this table is essential for trademark management and decision-making processes.', 'audit_revision': 'The audit_revision table contains data related to revisions made in the system. It tracks the changes made to various objects, including their properties, values, and descriptions. The table captures information such as the revision ID, operation count, object name, object ID, revision timestamp, user ID, parent object name, parent object ID, from value, to value, and creation/modification timestamps. This data is crucial for auditing purposes and helps in tracking and analyzing the changes made to objects in the system.', 'goods_services_term_note_draft': "The goods_services_term_note_draft table stores draft notes for goods and services terms. It contains information about the notes code, the employee who created the note, the date it was created, and the timestamp of when it was last modified. Additionally, it includes the user IDs of the creator and modifier, the actual text of the note, and the ID of the goods and services term it is associated with. This table is significant to the business as it allows employees to collaborate and make changes to draft notes for goods and services terms before they are finalized and implemented.", 'goods_services_term_draft': 'The goods_services_term_draft table contains data related to draft versions of goods and services terms. It includes information such as the term ID, modification number, term status, author employee number, acceptance partnership date, edition number, version number, release number, previous goods and services term ID, effective dates, and timestamps for creation and last modification. This table is significant to the business as it allows for the management and tracking of draft versions of goods and services terms, ensuring accurate and up-to-date documentation.', 'data_comp_sam': 'The data_comp_sam table contains information related to a specific business process. It includes data that represents various aspects of the process, such as serial numbers (SN), classification (CLS), and text descriptions (TXT). This table is essential for tracking and analyzing the progress and outcomes of the business process, allowing stakeholders to make informed decisions based on the data stored within. The table provides a comprehensive view of the process, enabling the identification of trends, patterns, and potential areas for improvement.', 'data_id_parsed': 'The data_id_parsed table contains parsed data related to an unidentified business process. The table includes three columns: CLS, TXT, and ORIG_TXT. The CLS column represents a classification or category assigned to each record in the table. The TXT column contains the parsed text data, which has been processed and transformed for analysis. The ORIG_TXT column stores the original, unprocessed text data. This table is essential for understanding and analyzing the parsed data within the business process, allowing for further insights and decision-making.', 'cdc_batch_job_history': 'The cdc_batch_job_history table contains data related to the history of Change Data Capture (CDC) batch jobs. It includes information such as the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. This table is significant to the business as it allows tracking and monitoring of CDC batch job activities, providing insights into the timing and progress of data changes captured by CDC.', 'data_teas_standard_clob': 'The data_teas_standard_clob table contains information related to serialized products. It includes details such as the serial number, class, submission ID, final description, and parsed text. This table is significant to the business as it provides a comprehensive record of serialized products and their associated information. The data in this table can be used for various purposes such as tracking product inventory, analyzing product classes, and generating reports on product submissions.', 'data_comp_parsed': 'The data_comp_parsed table contains parsed data related to a specific business process. The table includes information such as serial numbers, class types, text descriptions, and original text. This data is essential for analyzing and understanding the details of the business process, allowing for further analysis and decision-making. The table provides a comprehensive view of the parsed data, enabling users to gain insights and identify patterns or trends within the business process.', 'data_comp_sam_result': 'The data_comp_sam_result table contains information about trademark applications. It includes details such as the serial number, class, original text, filing date, case status, goods description, misclassified status, TEAS plus status, literal, status date, examining attorney, and law office. This table is significant to the business as it provides a comprehensive view of trademark applications and their associated information. It allows for tracking the progress and status of trademark applications, ensuring accurate classification of goods, and monitoring the work of examining attorneys and law offices.', 'data_comp': 'The data_comp table contains information related to text data processing. It includes columns such as SN, CLS, STRIPPED_TEXT, and ORIGINAL_TEXT. This table is significant to the business as it stores processed text data that has been stripped of any unnecessary characters or formatting. The STRIPPED_TEXT column represents the cleaned version of the original text, while the ORIGINAL_TEXT column stores the raw unprocessed text. The SN and CLS columns may be used for identification or classification purposes, although their specific meaning is not clear from the provided schema.', 'cdc_batch_job_control': 'The cdc_batch_job_control table is used to track the control information for Change Data Capture (CDC) batch jobs. It contains data related to the source folder, catalog name, database name, table name, source database name, and source table name. Additionally, it includes information about the primary keys, full load status, and whether the initial load has finished. This table is essential for managing and monitoring CDC batch jobs and their progress in the system.', 'data_teas_plus_clob': 'The data_teas_plus_clob table contains information related to serial numbers, classes, submission IDs, final descriptions, text, and parsed text. This table is significant to the business as it stores data that is used for analysis and decision-making processes. The data in this table represents various attributes and details of products or items, which are important for tracking, categorization, and understanding their characteristics. It provides a comprehensive view of the textual information associated with each item, including parsed text for further analysis. Overall, this table plays a crucial role in managing and analyzing product-related data for the business.', 'international_clsfcn_edn': 'The international_clsfcn_edn table contains information about different editions of international classifications. It includes the edition number, a general description of the edition, the dates when the edition becomes effective and when it ends, as well as timestamps for creation and last modification. This table is significant to the business as it allows for tracking and managing changes in international classifications over time.', 'data_id_case_level_result': 'The data in this table represents case level results for a specific identification number. It includes information such as the serial number, case status, filing date, case goods or service, and class. This table is significant to the business as it provides a comprehensive overview of the status and details of each case associated with an identification number. It allows for easy tracking and analysis of case progress and outcomes.', 'data_comp_test': 'The data_comp_test table contains information related to a specific business process. The table stores records with three columns: SN, CLS, and TXT. These columns represent various attributes or characteristics of the business process. The data in this table is valuable for analyzing and understanding the specific instances of the business process, allowing for insights and improvements to be made. However, without further context or knowledge of the specific business process, the exact meaning and significance of the data in this table may vary.', 'data_id': 'The data_id table contains information related to data identification. It includes two columns, CLS and TXT. The CLS column represents the classification of the data, while the TXT column contains the textual description associated with the data. This table is significant to the business as it helps in organizing and categorizing different types of data, providing a clear understanding of their classification and corresponding descriptions.', 'emp4': 'The emp4 table contains data related to employees in the organization. It includes information such as employee ID, name, and department ID. This table is significant for tracking and managing employee details within the business. The EMP_ID column represents the unique identifier for each employee, while the NAME column stores the name of the employee. The DEPT_ID column indicates the department to which the employee belongs. This table is essential for various HR processes, including payroll, performance evaluation, and organizational structure analysis.', 'data_id_parsed_standard': 'The data_id_parsed_standard table contains information related to parsed data. It includes the CLS, TXT, and ORIG_TXT columns. The CLS column represents a classification of the parsed data, while the TXT column contains the parsed text. The ORIG_TXT column stores the original text before parsing. This table is significant to the business as it provides structured and organized data that can be used for analysis, reporting, and decision-making purposes.', 'stnd_synonym_group': 'The stnd_synonym_group table contains data related to synonym groups. Synonym groups are used to group together words or phrases that have the same or similar meaning. This table stores information about the synonym group ID, the synonym group text, the status and action associated with the group, any notes related to the group, the effective dates of the group, and the user IDs responsible for creating and modifying the group. Additionally, there is a lock control number column used for concurrency control. This table is essential for managing and organizing synonyms within the business.', 'tm5_goods_services': 'The tm5_goods_services table contains information about the goods and services associated with a file. It includes details such as the status, description, class number, approval date, rejection date, processing status, and timestamps for creation and modification. This table is essential for tracking and managing the goods and services related to files in the business. It provides valuable insights into the progress and history of each item, allowing for efficient decision-making and monitoring of the overall process.', 'stnd_coordinated_class': "The stnd_coordinated_class table in the bronze schema of the trm_tmngidmp catalog contains data related to coordinated classes. It represents the relationship between two classes, with each row representing a unique combination of class IDs. The table includes information about the effective dates of the coordination, as well as timestamps for creation and last modification. This data is valuable for tracking and analyzing coordinated classes within the business.", 'intl_clsfcn_edn_ver_rel': "The intl_clsfcn_edn_ver_rel table in the bronze schema of the trm_tmngidmp catalog contains data related to the relationship between international classifications, editions, and versions. It includes information such as the edition number, version number, release number, scheduled publish date, publisher employee number, and creation and modification timestamps. This table is significant to the business as it helps track and manage the publication process of international classifications, ensuring accurate and up-to-date information is available to users.", 'stnd_class': "The stnd_class table contains information about different classes. Each row represents a unique class and includes details such as the class ID, class schedule code, class number, modification number, title, description, international class short title, international class explanatory note, international class inclusions, international class exclusions, begin effective date, end effective date, create timestamp, create user ID, last modification timestamp, last modification user ID, and goods/services count. This table is significant to the business as it provides a comprehensive overview of all classes and their associated details.", 'intl_clsfcn_edn_ver': "The intl_clsfcn_edn_ver table in the bronze schema of the trm_tmngidmp catalog contains data related to international classification edition versions. It stores information such as the edition number, version number, version year number, and the effective dates for each version. Additionally, it tracks the creation and modification timestamps along with the corresponding user IDs. This table is crucial for tracking and managing changes to international classification editions over time.", 'menu_item': "The menu_item table contains information about the menu items in a system. It represents the structure and hierarchy of the menu. Each row in the table represents a menu item, with details such as the menu item ID, label, code, URL, parent menu item ID, role, icon, display order, menu level, short label, creation user ID, creation timestamp, last modified user ID, last modified timestamp, and display location. This table is essential for managing and displaying the menu items in the systems user interface.", "stnd_class_schedule": "The stnd_class_schedule table in the bronze schema of the trm_tmngidmp catalog contains data related to class schedules. It includes information such as the schedule code, title, description, effective dates, creation and modification timestamps, user IDs, and a flag indicating if the schedule is in use. This table is significant to the business as it allows for the management and tracking of class schedules, enabling efficient scheduling and organization of classes. The data in this table represents the various class schedules offered by the business and their corresponding details.", 'taxonomy_group': "The taxonomy_group table is a key table in the database that stores information about different taxonomy groups. It contains data related to the taxonomy groups ID, class ID, edition number, version number, external reference number, parent external reference number, parent taxonomy group ID, title type, title, level number, scope, begin effective date, end effective date, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is crucial for organizing and categorizing various entities within the business. It helps in maintaining a hierarchical structure and allows for easy navigation and retrieval of data based on taxonomy group relationships.", 'stnd_goods_services_note': 'The stnd_goods_services_note table contains information about the notes associated with goods and services. It includes the note code, title, description, effective dates, and details about the creation and modification of the notes. This table is important for tracking and managing the various notes related to goods and services in the business. It provides a centralized repository for storing and retrieving information about these notes, allowing for easy reference and maintenance.', 'stnd_us_intl_cls_mapping': 'The stnd_us_intl_cls_mapping table is used to map the US classification IDs to international classification IDs. It contains information about the effective dates of the mappings, as well as the timestamps of when the mappings were created and last modified. This table is important for maintaining consistency and accuracy in classifying products and services across different regions and jurisdictions.', 'stnd_application_property': 'The stnd_application_property table stores information about various application properties. It is used to manage and track the values of different properties that are associated with applications. The table contains columns for the application property code, the corresponding value, the user who created the property, the timestamp of creation, the user who last modified the property, and the timestamp of the last modification. This table is essential for maintaining and updating application properties in the system.', 'tm5_file': 'The tm5_file table contains information about the files that are processed in the TM5 system. It includes details such as the file ID, file name, load date, record quantity, process date, creation timestamp, creation user ID, last modification timestamp, and last modification user ID. This table is significant to the business as it helps track and manage the files that are being processed in the TM5 system. The data in this table represents the metadata and audit trail of the files, allowing for effective monitoring and analysis of file processing activities.', 'stnd_term_status': 'The stnd_term_status table in the bronze schema of the trm_tmngidmp catalog contains data related to the status of terms. It includes information such as the term status code, title, description, begin effective date, end effective date, creation timestamp, creating user ID, last modification timestamp, and last modifying user ID. This table is significant to the business as it allows for tracking and managing the status of terms, providing insights into when terms become effective, when they expire, and who made the changes. The data in this table is crucial for ensuring accurate and up-to-date term management within the organization.', 'stnd_application_message': 'The stnd_application_message table contains data related to application messages. It stores information about the message ID, the message text, the user who created the message, the timestamp of when the message was created, the user who last modified the message, the timestamp of when the message was last modified, and the type of message. This table is significant to the business as it allows for tracking and management of application messages, providing insights into communication and system updates within the application.', 'sync_idm_update_log': 'The sync_idm_update_log table contains data related to the synchronization of updates made in the identity management system. It records the timestamp of when the updates were inserted, the batch name associated with the updates, the procedure name used for the synchronization, and an action code indicating the type of update performed. This table is essential for tracking and auditing changes made in the identity management system, providing insights into the synchronization process and facilitating troubleshooting and analysis.'}

for table_name, comment in tables_to_comment.items():
    alter_table_query = f"""
    ALTER TABLE {tmngidmp_catalog}.{database}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_USER_ID': 'The user ID that created the record', 'TERM_CT': 'Category or type of the goods/services term', 'MODIFICATION_DRAFT_NO': 'Number indicating the draft modification of the goods/services term', 'FK_PREVIOUS_GDS_SRVCS_TERM_ID': 'Foreign key referencing the previous goods/services term ID', 'FK_CLASS_ID': 'Foreign key referencing the class of the goods/services term', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'ACCEPT_PARTNERSHIP_DT': 'Date indicating when the partnership for the goods/services term was accepted', 'GOODS_SERVICES_TERM_ID_TX': 'Textual representation of the goods/services term identifier', 'DESCRIPTION_TX': 'Textual description of the goods/services term', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_TERM_STATUS_CD': 'Foreign key referencing the status code of the goods/services term', 'TM5_ACCEPT_IN': 'Indicator for accepting the goods/services term in TM5', 'GOODS_SERVICES_TERM_ID': 'Unique identifier for each goods/services term', 'FK_TAXONOMY_GROUP_ID': 'The foreign key for the taxonomy group ID', 'FK_RELEASE_NO': 'Foreign key referencing the release number of the goods/services term', 'MODIFICATION_NO': 'Number indicating the modification of the goods/services term', 'FK_EDITION_NO': 'Foreign key referencing the edition number of the goods/services term', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CFK_AUTHOR_EMPLOYEE_NO': 'Foreign key referencing the employee number of the author of the goods/services term', 'FK_VERSION_NO': 'Foreign key referencing the version number of the goods/services term', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="goods_services_term",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_CLASS_ID': 'Foreign key referencing the class ID in another table', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'FK_EDITION_NO': 'Foreign key referencing the edition number in another table', 'FK_VERSION_NO': 'Foreign key referencing the version number in another table', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LAST_MOD_USER_ID': 'The user ID that last modified the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="international_class_version",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_GOODS_SERVICES_TERM_ID': 'Foreign key referencing the term ID in the goods_services_term table', 'CFK_EMPLOYEE_NO': 'Foreign key referencing the employee number in the employee table', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'NOTE_DT': 'Date when the note was created', 'CREATE_TS': 'The timestamp of when the record was created', 'FK_GOODS_SERVICES_NOTE_CD': 'Foreign key referencing the note code in the goods_services_note table', 'NOTE_TX': 'Text content of the note', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="goods_services_term_note",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAW_OFFICE': 'The law office associated with the record', 'EXAMINING_ATTORNEY': 'The name of the examining attorney for the record', 'TEAS_PLUS_STATUS': 'The TEAS Plus status of the record', 'MISCLASSIFIED': 'Indicator if the record is misclassified', 'ORIGINAL_TEXT': 'The original text associated with the record', 'SERIAL_NUMBER': 'Unique identifier for each record', 'LITERAL': 'Literal value associated with the record', 'CASE_STATUS': 'The status of the case associated with the record', 'FILING_DATE': 'The date when the record was filed', 'CLASS': 'Classification code for the record', 'STATUS_DATE': 'The date when the status of the record was last updated', 'GOODS_DESC': 'Description of the goods associated with the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_comp_result",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'DESCRIPTION_TX': 'Description of the revision', 'REVISION_USER_ID': 'User ID of the user who made the revision', 'AUDIT_REVISION_ID': 'Unique identifier for each audit revision', 'OBJECT_PROPERTY_NM': 'Name of the property of the object', 'CREATE_TS': 'The timestamp of when the record was created', 'FROM_VALUE_TX': 'Previous value of the property', 'OBJECT_ID': 'Unique identifier for the object', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'DN_PARENT_OBJECT_NM': 'Name of the parent object in the directory', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_USER_ID': 'The user ID that created the record', 'PARENT_OBJECT_ID': 'Unique identifier for the parent object', 'REVISION_TS': 'Timestamp of the revision', 'OBJECT_NM': 'Name of the object', 'TO_VALUE_TX': 'New value of the property', 'OPERATION_CT': 'Type of operation performed on the object'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="audit_revision",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_GOODS_SERVICES_NOTE_CD': 'Foreign key referencing the note code in the goods_services_note table', 'NOTE_TX': 'Text content of the note', 'CFK_EMPLOYEE_NO': 'Foreign key referencing the employee number in the employee table', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_GOODS_SERVICES_TERM_ID': 'Foreign key referencing the term ID in the goods_services_term table', 'NOTE_DT': 'Date when the note was created', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="goods_services_term_note_draft",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TERM_CT': 'Category of the goods/services term', 'FK_RELEASE_NO': 'Foreign key referencing the release number of the goods/services term', 'MODIFICATION_NO': 'Number indicating the modification of the goods/services term', 'CREATE_TS': 'The timestamp of when the record was created', 'TM5_ACCEPT_IN': 'Indicator for accepting the goods/services term in TM5', 'FK_PREVIOUS_GDS_SRVCS_TERM_ID': 'Foreign key referencing the previous goods/services term identifier', 'FK_TAXONOMY_GROUP_ID': 'Foreign key referencing the taxonomy group ID', 'FK_EDITION_NO': 'Foreign key referencing the edition number of the goods/services term', 'FK_VERSION_NO': 'Foreign key referencing the version number of the goods/services term', 'ACTION_CT': 'The action category executed on the record', 'MODIFICATION_DRAFT_NO': 'Number indicating the draft modification of the goods/services term', 'FK_CLASS_ID': 'Foreign key referencing the class of the goods/services term', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_TERM_STATUS_CD': 'Foreign key referencing the status code of the goods/services term', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CFK_AUTHOR_EMPLOYEE_NO': 'Foreign key referencing the employee number of the author of the goods/services term', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'DESCRIPTION_TX': 'Textual description of the goods/services term', 'GOODS_SERVICES_TERM_ID': 'Unique identifier for each goods/services term', 'GOODS_SERVICES_TERM_ID_TX': 'Textual representation of the goods/services term identifier', 'ACCEPT_PARTNERSHIP_DT': 'Date indicating the acceptance of partnership for the goods/services term'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="goods_services_term_draft",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TXT': 'Text description of the data', 'CLS': 'Classification of the data', 'SN': 'Serial number of the data entry'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_comp_sam",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'ORIG_TXT': 'The original text data before processing', 'TXT': 'The processed text data', 'CLS': 'The classification of the data'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_id_parsed",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'processing_time': 'The timestamp of processing', 'cdc_file_date': 'The date of the CDC file', 'meta_src_time': 'The timestamp of the CDC source file', 'cdc_file_path': 'The path to the CDC file'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="cdc_batch_job_history",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TXT': 'Text data', 'FINAL_DESC': 'Final description of the data', 'CLASS': 'Classification category of the data', 'PARSED_TEXT': 'Parsed version of the text data', 'SERIALNUMBER': 'Unique identifier for each record', 'SUBMISSIONID': 'Identifier for the submission of the data'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_teas_standard_clob",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TXT': 'The parsed text', 'SN': 'The serial number', 'ORG_TXT': 'The organization text', 'CLS': 'The class name'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_comp_parsed",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'SERIAL_NUMBER': 'Unique identifier for each record', 'LAW_OFFICE': 'The law office associated with the trademark case', 'STATUS_DATE': 'The date when the status of the trademark was last updated', 'MISCLASSIFIED': 'Indicator if the trademark is misclassified', 'TEAS_PLUS_STATUS': 'Status of TEAS Plus application for the trademark', 'GOODS_DESC': 'Description of the goods associated with the trademark', 'ORIGINAL_TEXT': 'The original text of the trademark', 'FILING_DATE': 'The date when the trademark was filed', 'LITERAL': 'Literal representation of the trademark', 'CASE_STATUS': 'The current status of the trademark case', 'CLASS': 'Classification code for the trademark', 'EXAMINING_ATTORNEY': 'Name of the attorney responsible for examining the trademark case'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_comp_sam_result",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'STRIPPED_TEXT': 'Text with any formatting or special characters removed', 'CLS': 'Classification of the data', 'ORIGINAL_TEXT': 'Original text without any modifications', 'SN': 'Serial number of the data'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_comp",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'initial_load_finished': 'Indicates if the initial load of the table has been finished.', 'source_db_name': 'The name of the source database.', 'table_name': 'The name of the table.', 'src_folder': 'The folder where the source data is stored.', 'primary_keys': 'The primary keys of the table.', 'database_name': 'The name of the database where the table belongs.', 'full_load': 'Indicates if the table needs to be fully loaded.', 'catalog_name': 'The name of the catalog where the table belongs.', 'source_table_name': 'The name of the source table.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="cdc_batch_job_control",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FINAL_DESC': 'Final description of the data', 'TXT': 'Text data associated with the record', 'SERIALNUMBER': 'Unique identifier for each record', 'SUBMISSIONID': 'Identifier for the submission of the data', 'CLASS': 'Classification category of the data', 'PARSED_TEXT': 'Parsed version of the text data'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_teas_plus_clob",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'EDITION_NO': 'The edition number of the international classification', 'GENERAL_DESCRIPTION_TX': 'The general description of the international classification', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="international_clsfcn_edn",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CASE_STATUS': 'The status of the case', 'CASE_GOODS_SERVICE': 'The goods and services case', 'CLASS': 'The class text', 'SERIAL_NUMBER': 'The serial number', 'FILING_DATE': 'The filing date'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_id_case_level_result",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CLS': 'Classification of the data', 'TXT': 'Text description of the data', 'SN': 'Serial number of the data entry'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_comp_test",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TXT': 'The text description', 'CLS': 'The class text'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_id",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'NAME': 'The employee name', 'EMP_ID': 'The unique employee ID', 'DEPT_ID': 'The unique department ID'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="emp4",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CLS': 'The classification of the data', 'TXT': 'The processed text data', 'ORIG_TXT': 'The original unprocessed text data'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="data_id_parsed_standard",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'CREATE_TS': 'The timestamp of when the record was created', 'SYNONYM_GROUP_ID': 'Unique identifier for each synonym group', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'NOTE_TX': 'Additional notes or comments about the synonym group', 'ACTION_CT': 'The action category executed on the record', 'LOCK_CONTROL_NO': 'A number used for locking purposes', 'SYNONYM_GROUP_TX': 'Text representation of the synonym group', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'STATUS_CT': 'Current status of the synonym group'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_synonym_group",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_TM5_FILE_ID': 'Foreign key referencing the file ID in the TM5 File table', 'DESCRIPTION_TX': 'Text description of the goods or services', 'PROCESSING_STATUS_CT': 'Code indicating the processing status of the goods or services', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created', 'TM5_GOODS_SERVICES_ID': 'Primary key for the TM5 Goods Services table', 'CLASS_NO': 'Number indicating the class of the goods or services', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'APPROVAL_DT': 'Date when the goods or services were approved', 'STATUS_CT': 'Code indicating the status of the goods or services', 'REJECTION_DT': 'Date when the goods or services were rejected'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="tm5_goods_services",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'FK_CLASS_ID': 'Foreign key referencing the class ID in another table', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_COORDINATED_CLASS_ID': 'Foreign key referencing the coordinated class ID in another table'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_coordinated_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'SCHEDULED_PUBLISH_DT': 'Timestamp indicating the scheduled publish date', 'RELEASE_NO': 'Number indicating the release', 'CFK_SCHEDULER_EMPLOYEE_NO': 'Foreign key referencing the employee number of the scheduler', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CFK_PUBLISHER_EMPLOYEE_NO': 'Foreign key referencing the employee number of the publisher', 'FK_EDITION_NO': 'Foreign key referencing the edition number', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_VERSION_NO': 'Foreign key referencing the version number', 'PUBLISHED_DT': 'Timestamp indicating the date of publication'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="intl_clsfcn_edn_ver_rel",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'INTL_CLASS_INCLUSIONS_TX': 'Inclusions for the international class', 'DESCRIPTION_TX': 'Description of the class', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'CLASS_NO': 'Number assigned to the class', 'TITLE_TX': 'Title of the class', 'CLASS_ID': 'Unique identifier for each class', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'INTL_CLASS_EXCLUSIONS_TX': 'Exclusions for the international class', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'GOODS_SERVICES_CT': 'Category of goods or services', 'FK_CLASS_SCHEDULE_CD': 'Foreign key referencing the class schedule code', 'INTL_CLASS_SHORT_TITLE_TX': 'Short title of the international class', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'MODIFICATION_NO': 'Number indicating the modification of the class', 'INTL_CLASS_EXPLANATORY_NOTE_TX': 'Explanatory note for the international class'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_EDITION_NO': 'Foreign key referencing the edition number', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'CREATE_USER_ID': 'The user ID that created the record', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'VERSION_NO': 'Version number of the edition', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'VERSION_YEAR_NO': 'Year number of the edition version'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="intl_clsfcn_edn_ver",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'SHORT_LABEL_TX': 'Short label for the menu item', 'MENU_LEVEL_NO': 'Level of the menu item in the hierarchy', 'MENU_ITEM_ID': 'Unique identifier for each menu item', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'FK_PARENT_MENU_ITEM_ID': 'Foreign key referencing the parent menu item', 'ROLE_TX': 'Role required to access the menu item', 'DISPLAY_ORDER_NO': 'Order in which the menu item is displayed', 'ICON_TX': 'Icon associated with the menu item', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LABEL_TX': 'Text label for the menu item', 'URL_TX': 'URL associated with the menu item', 'CREATE_USER_ID': 'The user ID that created the record', 'DISPLAY_IN': 'Where the menu item is displayed', 'MENU_ITEM_CD': 'Code for the menu item'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="menu_item",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'DESCRIPTION_TX': 'Description of the class schedule', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'US_IN': 'Indicates if the class schedule is in use', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'TITLE_TX': 'Title of the class schedule', 'CLASS_SCHEDULE_CD': 'Unique identifier for each class schedule'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_class_schedule",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FK_EDITION_NO': 'Foreign key referencing the edition table', 'LEVEL_NO': 'Level of the taxonomy group in the hierarchy', 'FK_CLASS_ID': 'Foreign key referencing the class table', 'EXTERNAL_REFERENCE_NUMBER_TX': 'External reference number for the taxonomy group', 'DN_PARENT_EXTERNAL_REF_NUM_TX': 'External reference number of the parent taxonomy group', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'SCOPE_CT': 'Scope of the taxonomy group', 'FK_VERSION_NO': 'Foreign key referencing the version table', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'TAXONOMY_GROUP_ID': 'Unique identifier for each taxonomy group', 'TITLE_TYPE_CT': 'Type of title for the taxonomy group', 'FK_PARENT_TAXONOMY_GROUP_ID': 'Foreign key referencing the parent taxonomy group', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created', 'CREATE_USER_ID': 'The user ID that created the record', 'TITLE_TX': 'Title of the taxonomy group'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="taxonomy_group",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'CREATE_TS': 'The timestamp of when the record was created', 'TITLE_TX': 'Title of the note', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'GOODS_SERVICES_NOTE_CD': 'Code for the goods/services note', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'DESCRIPTION_TX': 'Description of the note'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_goods_services_note",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_USER_ID': 'The user ID that created the record', 'FK_US_CLASS_ID': 'Foreign key referencing the US class ID', 'FK_INTL_CLASS_ID': 'Foreign key referencing the international class ID'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_us_intl_cls_mapping",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_USER_ID': 'The user ID that created the record', 'VALUE_TX': 'Value of the application property', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'APPLICATION_PROPERTY_CD': 'Code for the application property'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_application_property",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'RECORD_QT': 'Number of records in the TM5 file', 'LOAD_DT': 'Date when the TM5 file was loaded', 'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'TM5_FILE_ID': 'Unique identifier for each TM5 file', 'TM5_FILE_NM': 'Name of the TM5 file', 'PROCESS_DT': 'Date when the TM5 file was processed', 'CREATE_TS': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="tm5_file",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'TERM_STATUS_CD': 'Code representing the status of a term', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'CREATE_TS': 'The timestamp of when the record was created', 'TITLE_TX': 'Title of the term', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'BEGIN_EFFECTIVE_DT': 'The timestamp of when the record began its effectiveness', 'DESCRIPTION_TX': 'Description of the term', 'CREATE_USER_ID': 'The user ID that created the record', 'END_EFFECTIVE_DT': 'The timestamp of when the record is no longer effective'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_term_status",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CREATE_USER_ID': 'The user ID that created the record', 'LAST_MOD_USER_ID': 'The user ID that last modified the record', 'MESSAGE_TX': 'Text of the message', 'CREATE_TS': 'The timestamp of when the record was created', 'LAST_MOD_TS': 'The timestamp of when the record was last modified', 'MESSAGE_TYPE_CT': 'Type of the message', 'APPLICATION_MESSAGE_ID': 'Unique identifier for each application message'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="stnd_application_message",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'PROCEDURE_NAME': 'Name of the procedure or function that initiated the update.', 'BATCH_NAME': 'Name of the batch process that performed the update.', 'ACTION_CD': "Code indicating the type of action performed during the update (e.g., INSERT, UPDATE, DELETE).", 'INSERT_TS': 'Timestamp indicating the date and time when the record was inserted into the table.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngidmp_catalog,
        database=database,
        table="sync_idm_update_log",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

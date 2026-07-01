# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')


# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
trm_reporting_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{trm_reporting_catalog=}, {data_quality_catalog=} ')

spark.catalog.setCurrentCatalog(trm_reporting_catalog)
spark.catalog.setCurrentDatabase("silver")

# COMMAND ----------

def tableExists(tableName,schemaName='silver'):
    return spark.sql(f"show tables in {schemaName} like '{tableName}'").count() == 1

# COMMAND ----------

# Set table properties
table_name = 'bibliography'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'Table for storing {table_name} information of trademarks')
        """

    spark.sql(alter_table_query)

    # Change column comments
    columns_comments = {
        "SER_NUM": "Serial Number, unique identifier for each record",
        "TEST_PCTRAM_LINK": "Link to the PCTRAM test results",
        "LAW_OFFICE": "Law office handling the case",
        "FILING_BASIS_CUR": "Current filing basis",
        "FILING_METHOD_FILED": "Method of filing initially used",
        "FILING_METHOD_CUR": "Current filing method",
        "FILING_BASIS_FIL": "Filing basis at the time of filing",
        "FILING_BASIS_AMED": "Amended filing basis",
        "REGISTRATION_NUMBER": "Trademark registration number",
        "AM_FLG_66A_FIL": "Flag for Article 66(a) filing",
        "AM_FLG_44D_FIL": "Flag for Article 44(d) filing",
        "AM_FLG_44E_FIL": "Flag for Article 44(e) filing",
        "FLG_PAPER_FIL": "Flag indicating paper filing",
        "AM_STAT": "Amendment status",
        "AM_FLG_NO_BAS_FIL": "Flag for no basis filing",
        "AM_FLG_TEASRF_FIL": "Flag for TEAS RF filing",
        "AM_FLG_USE_FIL": "Flag for use in commerce filing",
        "AM_FLG_ITU_FIL": "Flag for intent to use filing",
        "AM_FLG_TEASPL_FIL": "Flag for TEAS Plus filing",
        "LAST_MODIFIED_DATE": "Timestamp of the last modification",
        "FILING_BASIS_GRP": "Group of filing basis",
        "MARK_DWG_CD": "Code for the mark drawing",
        "MARK_DWG_DESC": "Description of the mark drawing",
        "MARK_NM_SHORT": "Short name of the mark",
        "MARK_NM": "Full name of the mark",
        "TMNG_IMAGE_LINK": "Link to the trademark image",
        "TM_ANALYTICS_TS": "Timestamp for trademark analytics",
        "EXMR_EID": "Examiner ID",
        "STATUS_DT": "Date of the current status",
        "create_ts": "Timestamp of record creation",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp of the last update",
        "update_user_id": "User ID of the last updater"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE bibliography ALTER COLUMN {column} COMMENT '{comment}';
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'class'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'Table for class information for trademark')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "class_status": "Status of the class",
        "class": "Class identifier",
        "ser_num": "Serial number",
        "cl_cls_us_ct": "Count of US class",
        "cl_cls_us": "US class",
        "cl_dt_stat": "Date of status",
        "cl_flg_anoth_form": "Flag for another form",
        "vt_ser_num": "VT serial number",
        "vt_class": "VT class",
        "goods_and_services_desc": "Description of goods and services",
        "create_ts": "Timestamp of creation",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp of the last update",
        "update_user_id": "User ID of the last updater"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------


table_name = 'correspondence'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}  
        SET TBLPROPERTIES ('comment' = 'Table for {table_name} information for trademark')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "ser_num": "Serial number",
        "cor_nm": "Correspondent name",
        "firm_nm": "Firm name",
        "add_line1": "Address line 1",
        "add_line2": "Address line 2",
        "city_nm": "City name",
        "zipcode": "Zip code",
        "state_cd": "State code",
        "state_nm": "State name",
        "ctry_cd": "Country code",
        "ctry_nm": "Country name",
        "ctry_name_caps": "Country name in capitals",
        "country_or_area_name": "Country or area name",
        "iso_alpha3_code": "ISO alpha-3 code",
        "ip_att_docket_ref": "IP attorney docket reference",
        "atty_nm": "Attorney name",
        "domestic_rep": "Domestic representative",
        "at_email_auth": "Attorney email authorization",
        "at_email": "Attorney email",
        "cr_email1": "Correspondence email 1",
        "cr_email2": "Correspondence email 2",
        "cr_email3": "Correspondence email 3",
        "cr_email4": "Correspondence email 4",
        "cr_email_auth": "Correspondence email authorization",
        "create_ts": "Timestamp of creation",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp of the last update",
        "update_user_id": "User ID of the last updater"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE correspondence ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'divisionals'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'the {table_name} table serves as a detailed log of divisional applications within the trademark process, tracking their lifecycle from submission to completion, including any international notifications and updates along the way.')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "ser_num": "Serial number of the divisional application",
        "filing_dt": "Filing date of the divisional application",
        "ib_notification_dt": "Date of notification from the International Bureau",
        "dv_type": "Type of divisional application",
        "ref_ser_num": "Reference serial number of the original application",
        "dv_dt_rqst": "Date when divisional request was made",
        "dv_dt_complete": "Date when divisional processing was completed",
        "last_modified_date": "The last date when the record was modified",
        "trans_dt": "Transaction date",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last person who updated the record"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'filings_counts'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is designed to keep track of how many records are being processed or created each day. It records the total number of these records, how much this number has changed compared to the previous day as a percentage, and whether or not the process that generates these records should continue. Additionally, it keeps information about when each record was created or last updated, along with the ID of the user who performed the creation or update. This table is crucial for monitoring the flow of data and ensuring that everything is running as expected.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "record_output_date": "Date of record output.",
        "output_record_count": "Number of records outputted.",
        "record_output_percent_change": "Percentage change in record output from the previous day.",
        "continue_process": "Indicator whether the process should continue or not.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the creator.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the last updater."
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE filings_counts ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'fixed_class_counts'
if tableExists(table_name):
  alter_table_query = f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a detailed logbook that keeps track of specific records, each identified by a unique serial number. For each record, it notes how many categories or "classes" it belongs to, marking the exact date and time this information was recorded or last updated. Additionally, it keeps a record of who created this information and when, as well as who last updated it and when. This table helps in monitoring and managing the classification of records over time, ensuring that the data is accurately maintained and up-to-date.')
      """
  spark.sql(alter_table_query)
  # Define a dictionary with column names as keys and their comments as values
  columns_comments = {
    "ser_num" : 'Unique serial number identifying the record',
    "class_count" :"Number of classes/categories the record belongs to",
    "date_stamp" : 'Timestamp when the class count was recorded',
    "create_ts" : 'Timestamp when the record was created',
    "create_user_id" : 'User ID of the creator',
    "update_ts":  "Timestamp when the record was last updated",
    "update_user_id" : 'User ID of the last person who updated the record'
  }


  for column, comment in columns_comments.items():
      spark.sql(f"""
      ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
      """)
else:
  print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'form_paragraph_counts'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table acts like a daily diary for tracking how many paragraphs (or sections) of forms are processed each day. It records the date, the total number of paragraphs counted, how much this number has changed from the day before (in percentage), and whether the counting process should keep going or stop. Additionally, it keeps a log of when each entry was made or last updated, along with the ID of the person who did it. This table is useful for keeping an eye on the workflow and making sure everything is moving smoothly.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "record_output_date": "The date when the record was output.",
        "output_record_count": "The total number of records outputted.",
        "record_output_percent_change": "The percentage change in record output compared to the previous period.",
        "continue_process": "Indicator (1 for yes, 0 for no) whether the process should continue.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'fpep_fact'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a detailed catalog that keeps track of various activities or tasks related to specific projects or items. Each record in the table represents a unique task, identified by a serial number, and includes information such as the category of the task, the title or description, the fiscal year it pertains to, and the number of actions taken on it. It also records when the task was completed, the group responsible for it, and a unique identifier for easy reference. Additionally, it keeps track of who created and last updated each record, along with the exact times those actions occurred. This table is essential for monitoring the progress and management of tasks within an organization.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "CATEGORY": "Category of the record.",
        "FK_FP_CATEGORY_ID": "Foreign key linking to the category ID.",
        "FK_FP_GROUP_ID": "Foreign key linking to the group ID.",
        "TITLE_TX": "Title or textual description of the record.",
        "SER_NUM": "Serial number of the record.",
        "FP_YEAR": "Fiscal year associated with the record.",
        "FK_WRKR_ID": "Foreign key linking to the worker ID.",
        "ACTION_COUNT": "Count of actions taken on the record.",
        "TRANSACTION_NO": "Transaction number associated with the record.",
        "TRANSACTIONAL_LITERAL": "Textual description of the transaction.",
        "COMPLETED_DT": "Date when the record was completed.",
        "GROUP_NAME": "Name of the group associated with the record.",
        "FP_ID": "Unique identifier for the record.",
        "COMPLETED_TS": "Timestamp when the record was completed.",
        "TM_ANALYTICS_TS": "Timestamp for analytics purposes."
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

spark.sql(f"ALTER TABLE {trm_reporting_catalog}.silver.fpep_fact ADD COLUMNS (CURRENT_TITLE STRING COMMENT 'Current title or description associated with the record')")

# COMMAND ----------

table_name = 'goods_services_normalization'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a digital filing cabinet that stores descriptions of various goods or services. Each record in the table includes the original description as it was provided, alongside a processed or cleaned-up version of that description to ensure consistency and clarity. The table also tracks when each record was created and last updated, as well as who made those updates. This helps in maintaining a clear and organized database of goods or services descriptions, making it easier to search and analyze the information.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "goods_services_desc": "Original description of goods or services.",
        "goods_services_desc_processed": "Processed or normalized description of goods or services.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'goods_services_sn_list'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table serves as a simple yet effective tracker for goods or services, each uniquely identified by a serial number. It records when each item was first added to the list and by whom, as well as any updates made to the item, including the time of the update and the user responsible for it. This table is essential for keeping an organized record of goods or services, making it easier to manage and track changes over time.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "ser_num": "Serial number associated with the goods or services entry.",
        "create_ts": "Timestamp when the entry was created.",
        "create_user_id": "User ID of the individual who created the entry.",
        "update_ts": "Timestamp when the entry was last updated.",
        "update_user_id": "User ID of the individual who last updated the entry."
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = 'job_control'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table acts like a command center for managing various tasks or jobs within a system. It keeps a detailed log of each job, including a unique identifier (job_control_id) for easy tracking, the name of the job, and timestamps indicating when the job was loaded into the system, created, and last modified. It also records the identity of the users who created and last modified each job. This table is essential for overseeing the progress and updates of tasks, ensuring that everything runs smoothly and efficiently.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "job_control_id": "Unique identifier for the job control record.",
        "job_nm": "Name of the job.",
        "load_ts": "Timestamp when the job was loaded.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "last_mod_ts": "Timestamp when the record was last modified.",
        "last_mod_user_id": "User ID of the individual who last modified the record."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")



# COMMAND ----------

table_name ='job_log'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table acts as a detailed journal for tracking the execution of various jobs or tasks within a system.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "job_log_id": "Unique identifier for the job log entry.",
        "job_nm": "Name of the job.",
        "start_ts": "Timestamp marking the start of the job.",
        "end_ts": "Timestamp marking the end of the job.",
        "status_ct": "Status code indicating the outcome of the job.",
        "record_qt": "Quantity of records processed by the job.",
        "comment_tx": "Additional comments regarding the job log entry."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")


# COMMAND ----------

table_name ='milestone'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a detailed timeline for tracking the progress and key events of various projects or tasks. Each entry in the table represents a significant step or milestone reached, such as when a project was started, when it reached certain stages, or when it was completed. It includes dates for these events, types of actions taken, and any relevant comments or statuses to provide a comprehensive view of the projects journey. This table helps in understanding how projects are moving forward and identifying any delays or issues that need attention.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "ser_num": "Serial number",
        "first_action_dt_ph": "1st_Action_DT_PH",
        "am_1_actn_ct_dt": "Date of first action count",
        "first_action_type": "1st_Action_Type",
        "filing_dt": "Filing date",
        "ib_notification_dt": "IB notification date",
        "published_dt": "Published date",
        "noa_dt": "Notice of Allowance date",
        "abandonment_dt": "Abandonment date",
        "aban_dt_ph": "Abandonment date placeholder",
        "registration_dt": "Registration date",
        "disposal_type": "Type of disposal",
        "ext1_dt": "Extension 1 date",
        "ext2_dt": "Extension 2 date",
        "ext3_dt": "Extension 3 date",
        "ext4_dt": "Extension 4 date",
        "ext5_dt": "Extension 5 date",
        "cancellation_dt": "Cancellation date",
        "renewal_dt": "Renewal date",
        "revival_dt": "Revival date",
        "susp_check_dt": "Suspension check date",
        "am_cls_ct_actv": "Active class count",
        "pendency_cal_start_dt": "Pendency calculation start date",
        "pendency_cal_end_dt": "Pendency calculation end date",
        "noa_registration_check": "NOA_REGISTRATION Check",
        "wgtd_1st_actn_pendency": "Weighted first action pendency",
        "first_action_cd": "1st Action code",
        "disposal_pendency": "Disposal pendency",
        "suspension": "Suspension status",
        "ttab": "TTAB status",
        "disposal_dt": "Disposal date",
        "dock_dt": "Dock date",
        "am_flg_66a_cur": "Current flag 66a",
        "am_flg_66a_fil": "Filed flag 66a",
        "noa_dt_ph": "NOA date placeholder",
        "filing_fy": "Filing fiscal year",
        "non_pro_se": "NON/PRO SE",
        "first_action_pendency_ph": "1st Action Pendency_PH",
        "last_modified_date": "Last modified date",
        "processing_pend": "Processing pendency",
        "processing_pend_days": "Processing pendency in days",
        "days_in_dock": "Days in dock",
        "create_ts": "Creation timestamp",
        "create_user_id": "Creator user ID",
        "update_ts": "Update timestamp",
        "update_user_id": "Updater user ID"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")


# COMMAND ----------

table_name ='on_hold'
if tableExists(table_name):
    alter_table_query = f"""ALTER TABLE {table_name}
    SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a storage area for keeping track of items or tasks that have been paused or put on hold. It records details such as a unique identifier for each hold record, when the hold was initiated, who initiated it, and the last updates made to it. It also includes information about the status of the hold, whether its still active, and any related docket numbers. This table helps in managing and reviewing items that are not currently active but may need attention or action in the future.')"""
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "ath_ser_num": "Serial number of the hold record.",
        "ath_create_dt": "Date when the hold record was created.",
        "ath_create_ti": "Time when the hold record was created.",
        "ath_emp_num": "Employee number who created the hold record.",
        "ath_last_upd_dt": "Date when the hold record was last updated.",
        "ath_last_upd_ti": "Time when the hold record was last updated.",
        "ath_last_emp_num": "Employee number who last updated the hold record.",
        "ath_hold_status": "Status of the hold (e.g., active, resolved).",
        "ath_active_status": "Active status indicator.",
        "ath_hold_docket": "Docket number associated with the hold.",
        "last_modified_dt": "Date when the record was last modified.",
        "oracle_apply_time": "Timestamp of when changes were applied in Oracle.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }

    # Iterate over the dictionary and execute ALTER COLUMN COMMENT statements for each column
    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column}  COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------


table_name ='owner'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a digital address book for keeping track of the owners of various items or assets. It records essential details about each owner, such as their name, contact information (like address and email), and the type of owner they are (individual, company, etc) The table also includes information about the country and state of the owner, along with a unique serial number for each entry. This table helps in identifying who owns what and how to contact them, making it easier to manage ownership records and communicate with owners when needed.')
        """
    spark.sql(alter_table_query)
    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "ser_num": "Trademark serial number.",
        "current_owner": "Y/N flag indicating if this is the current owner of the serial number. Set to Y if party type matches the maximum party type associated with this serial number.",
        "party_type": "Two digit code identifying the party type of the owner. Common mappings - 10: Owner at Application; 20: Owner at Publication; 30: Owner at Registration. Full mappings can be found in the table trm_tmngpdb.bronze.sync_translate_party_type.",
        "name": "Name of the owner.",
        "address_1": "Primary address line.",
        "address_2": "Secondary address line (if any).",
        "city": "City of the owner's address.",
        "postal_cd": "Postal code of the owner's address.",
        "citizenship": "Citizenship of the owner.",
        "entity_type": "Code indentifying the entity type of the owner. Common mappings - 1: Corporation; 3: Individual; 16: LLC. Full mappings can be found in the table trm_tmngpdb.bronze.stnd_legal_entity_type.",
        "ctry_nm": "Country name of owner's address.",
        "ctry_cd": "Country code of owner's address.",
        "country_or_area_name": "Country or area name of owner's address.",
        "state_cd": "State or sub-region code of the owner's address, where applicable.",
        "max_party_type": "Maximum party_type value associated with this serial number. See party_type description for more information.",
        "owner_num": "Owner number when there are multiple owners associated with a given serial number and party type. Owner number 1 is the primary owner.",
        "owner_email": "Email address of the owner.",
        "create_ts": "Auto generated timestamp of when the record was created.",
        "create_user_id": "Account ID that created the record.",
        "update_ts": "Auto generated timestamp of when the record was last updated.",
        "update_user_id": "Account ID that last updated the record."
    }

    # Iterate over the dictionary and execute ALTER COLUMN COMMENT statements for each column
    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column}  COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

# Define the table name
table_name = "pendency_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a scoreboard that tracks how many tasks or records are waiting to be processed over time. It records the date of the count, how many items are pending, whether the process for handling these items should continue, and how the number of pending items has changed compared to the previous count. This table helps in understanding the workload and efficiency of processing tasks, indicating whether things are getting better, staying the same, or getting worse. Its a useful tool for managing and improving operational processes.')
        """
    spark.sql(alter_table_query)

    # Define a dictionary with column names as keys and their comments as values
    columns_comments = {
        "record_output_date": "The date of the record output.",
        "record_output_count": "The count of records outputted.",
        "continue_process": "Indicator if the process should continue (1) or not (0).",
        "record_output_percent_change": "The percentage change in record output.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }

    # Iterate over the dictionary and execute ALTER COLUMN COMMENT statements for each column
    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "post_reg_detail"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a detailed journal that keeps track of various actions and changes after the registration of an item, such as a trademark. It includes information like the serial number of the registration, the date it was registered, and the registration number. Additionally, it records categories of post-registration activities, dates and descriptions of actions taken from the start to the end, and details about renewals. This table also tracks specific flags indicating certain conditions, the total time pending for actions to be completed, and identifies the staff involved in processing. Essentially, its a comprehensive log that helps understand the lifecycle and management of registered items after their initial registration.')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "serial_number": "Unique identifier for each record.",
        "registration_dt": "Date of registration.",
        "registration_number": "Number assigned upon registration.",
        "postreg_category": "Category of post-registration action.",
        "start_action_number": "Starting action number for a range of actions.",
        "end_action_number": "Ending action number for a range of actions.",
        "start_action_date": "Date when the start action was initiated.",
        "end_action_date": "Date when the end action was completed.",
        "start_5_characters": "First 5 characters of the start action description.",
        "end_5_characters": "First 5 characters of the end action description.",
        "start_cm_desc": "Description of the start action.",
        "end_cm_desc": "Description of the end action.",
        "renewal_dt": "Date of renewal.",
        "renewal_number": "Number assigned upon renewal.",
        "fifteen_flag": "Flag indicating a specific condition (true/false).",
        "inventory": "Flag indicating if the item is in inventory (true/false).",
        "first_action_date": "Date of the first action.",
        "first_action_code": "Code of the first action.",
        "first_action_pendency": "Pendency period of the first action.",
        "first_action_inventory": "Flag indicating if the first action is in inventory (true/false).",
        "total_pendency": "Total pendency period.",
        "tm_worker_eid": "Employee ID of the trademark worker.",
        "unique_transaction_id": "Unique ID for the transaction.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "post_reg_milestone"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a detailed journal that keeps track of various actions and changes after the registration of an item, such as a trademark. It includes information like the serial number of the registration, the date it was registered, and the registration number. Additionally, it records categories of post-registration activities, dates and descriptions of actions taken from the start to the end, and details about renewals. This table also tracks specific flags indicating certain conditions, the total time pending for actions to be completed, and identifies the staff involved in processing. Essentially, its a comprehensive log that helps understand the lifecycle and management of registered items after their initial registration.')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "serial_number": "The unique identifier for each registration.",
        "registration_dt": "The date when the registration was officially recorded.",
        "six_yr_dt": "The date by which a 6-year maintenance document is due.",
        "last_10yr_dt": "The last date a 10-year renewal document was filed.",
        "next_10yr_renewal": "The next due date for a 10-year renewal.",
        "number_renewals": "The total number of times the registration has been renewed.",
        "next_6yr_dt": "The next due date for a 6-year maintenance filing.",
        "expiration_dt": "The date when the registration is set to expire.",
        "expiration_type": "The type of expiration for the registration.",
        "registration_number": "The official number assigned to the registration.",
        "am_dt_cncl": "The date and time when the registration was amended or cancelled.",
        "active_classes": "The number of active classes associated with the registration.",
        "live_registration": "Indicator if the registration is currently active (1) or not (0).",
        "expiration_dt_realtime": "The real-time expiration date considering any amendments or changes.",
        "expiration_type_realtime": "The real-time expiration type considering any amendments or changes.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "pr_detail_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is designed to keep track of specific records over time. It records the date when data was processed, how many records were handled, the percentage change in the number of records compared to a previous period, and whether the process should continue. Additionally, it logs when each entry was created or last updated, along with the user responsible for these actions. This table is useful for monitoring the flow and changes in data processing activities, helping to understand trends and make decisions about future actions.')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "record_output_date": "The date when the records were output.",
        "output_record_count": "The count of records outputted on the specified date.",
        "record_output_percent_change": "The percentage change in record output compared to the previous period.",
        "continue_process": "Indicator whether to continue the process (1 for yes, 0 for no).",
        "create_ts": "Timestamp when the record was initially created.",
        "create_user_id": "Identifier of the user who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "Identifier of the user who last updated the record."
    }
    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "pr_milestone_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is designed to track and analyze the progress of certain milestones or goals over time. It records the date of each report, the number of milestones reached on that date, and the percentage change in milestones reached compared to a previous period. Additionally, it indicates whether the process of reaching these milestones should continue. The table also keeps track of when each entry was created or updated and by whom, ensuring that the progress towards milestones is meticulously documented and monitored.')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "record_output_date": "The date when the records were output.",
        "output_record_count": "The count of records outputted on the specified date.",
        "record_output_percent_change": "The percentage change in record output compared to the previous period.",
        "continue_process": "Indicator whether to continue the process (1 for yes, 0 for no).",
        "create_ts": "Timestamp when the record was initially created.",
        "create_user_id": "Identifier of the user who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "Identifier of the user who last updated the record."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column}  COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "prosecution_history"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a detailed diary for tracking the journey of legal actions or processes related to trademarks or patents. It records each significant step or action taken, such as when a particular legal action was initiated, what type of action it was, and when it was last modified. This table also keeps tabs on specific dates and descriptions related to these actions, and identifies the staff involved in processing them. Essentially, its a comprehensive log that helps understand the sequence of events and actions taken during the legal prosecution of trademarks or patents, ensuring that every step is documented and traceable.')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "serial_number": "SER_NUM",
        "ph_action_number": "Action number in the prosecution history.",
        "ph_action_code": "Code representing the type of action taken.",
        "cm_sys_dt": "System date when the action was recorded.",
        "ph_action_date": "Date when the action took place.",
        "last_modified_date": "The last date when the record was modified.",
        "oracle_apply_time": "Timestamp when changes were applied in Oracle.",
        "cm_prcd_num": "Procedure number associated with the action.",
        "ri_notif_dt": "Date when a notification was sent regarding the action.",
        "cm_desc": "Description of the action taken.",
        "fifth_char_cm_type": "5TH_CHAR_CM_TYPE",
        "cm_flg_paper": "Flag indicating if the action was documented on paper.",
        "ttab_tracking_num": "Tracking number for Trademark Trial and Appeal Board cases.",
        "tm_worker_eid": "Employee ID of the trademark worker involved.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "quality_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is designed to monitor and evaluate the performance of a specific process or task over time. It records the date of each evaluation, the number of items or records processed on that date, and how this number has changed compared to a previous period. Additionally, it indicates whether the process should continue or stop. The table also tracks when each entry was created or last updated, and by whom, providing a clear history of the process performance and oversight. This helps in understanding the efficiency and quality of the process being monitored.')
        """
    spark.sql(alter_table_query)
    columns_comments = {
        "record_output_date": "The date when the records were output.",
        "output_record_count": "The count of records outputted on the specified date.",
        "record_output_percent_change": "The percentage change in record output compared to the previous period.",
        "continue_process": "Indicator whether to continue the process (1 for yes, 0 for no).",
        "create_ts": "Timestamp when the record was initially created.",
        "create_user_id": "Identifier of the user who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "Identifier of the user who last updated the record."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "stg_ttab_input_cde"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is like a temporary storage area for input data related to trademark cases. It holds information about each case, such as the serial number, the start date for calculating the time it takes to process the case, whether it is a non-prosecution case, and links to test results. The table also includes details about the law office handling the case, the filing basis, the filing method, and the status of any amendments. Additionally, it captures information about the owner of the trademark, including their name, city, state, and country. The table also keeps track of the number of registered and active classes associated with the trademark, as well as the type of group it belongs to. Finally, it includes a short name for the trademark. Overall, this table serves as a temporary repository for various details related to trademark cases, facilitating further processing and analysis.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "SER_NUM": "Serial number of the record.",
        "Pendency_Cal_Start_DT": "Date when pendency calculation starts.",
        "NON_PRO_SE": "Indicator if the case is non-prosecution.",
        "TEST_PCTRAM_LINK": "Link to PCTRAM test results.",
        "LAW_OFFICE": "Law office handling the case.",
        "FILING_BASIS_GRP": "Group of filing basis.",
        "FILING_METHOD_CUR": "Current filing method.",
        "AM_STAT": "Status of amendment.",
        "Owner_Name": "Name of the owner.",
        "CITY": "City of the owner.",
        "STATE": "State of the owner.",
        "Country_or_Area_Name": "Country or area name of the owner.",
        "Reg_Class_Count": "Count of registered classes.",
        "Active_Class_Count": "Count of active classes.",
        "Group_Type": "Type of group.",
        "Concat_Class": "Concatenated class information.",
        "MARK_NM_SHORT": "Short name of the mark."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "stg_ttab_input_ph"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table serves as a collection point for detailed information related to the legal proceedings or actions taken on trademark applications or registrations. It includes unique identifiers for each case (serial number), details about specific actions taken (like the type of action, when it was taken, and a description of the action), and administrative details (such as the date of last modification and the employee involved). This table helps in tracking the progress and handling of trademark cases, providing a comprehensive view of each cases history and current status.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "serial_number": "Unique identifier for each record.",
        "ph_action_number": "Action number in the prosecution history.",
        "ph_action_code": "Code representing the type of action taken.",
        "cm_sys_dt": "System date when the action was recorded.",
        "ph_action_date": "Date when the action took place.",
        "last_modified_date": "The last date when the record was modified.",
        "oracle_apply_time": "Timestamp when changes were applied in Oracle.",
        "cm_prcd_num": "Procedure number associated with the action.",
        "ri_notif_dt": "Date when a notification was sent regarding the action.",
        "cm_desc": "Description of the action taken.",
        "fifth_char_cm_type": "Fifth character representing the type of action.",
        "cm_flg_paper": "Flag indicating if the action was documented on paper.",
        "ttab_tracking_num": "Tracking number for Trademark Trial and Appeal Board cases.",
        "tm_worker_eid": "Employee ID of the trademark worker involved.",
        "five_Characters": "Five-character code associated with the record.",
        "year": "Year associated with the record."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "tqr_detail_metrics"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is essentially a detailed report card for reviewing the quality of work related to trademark applications. It keeps track of various reviews and actions taken on these applications, including when they happened and who was involved. The table records specific identifiers for each event and review, types of reviews conducted, and the serial numbers of trademarks being reviewed. It also notes the timing of important steps in the review process, such as when a review was assigned, completed, or last reviewed, along with the financial year and quarter for reporting purposes.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "eventinventoryidentifier": "Unique identifier for the event inventory.",
        "qualityreviewidentifier": "Unique identifier for the quality review.",
        "reviewtypecode": "Code representing the type of review.",
        "trademarkserialnumber": "Serial number of the trademark.",
        "eventdatetime": "Timestamp of the event.",
        "examineremployeenumber": "Employee number of the examiner.",
        "organizationcode": "Code representing the organization.",
        "searchcompleteindicator": "Indicator if the search is complete (True/False).",
        "revieweremployeenumber": "Employee number of the reviewer.",
        "lastreviewdatetime": "Timestamp of the last review.",
        "assigndatetime": "Timestamp when assigned.",
        "completedatetime": "Timestamp when completed.",
        "financialyear": "Financial year of the record.",
        "financialquarternumber": "Financial quarter of the record.",
        "missedtagelementnamebag": "Bag of names for missed tag elements.",
        "newtagelementnamebag": "Bag of names for new tag elements.",
        "unsoundtagelementnamebag": "Bag of names for unsound tag elements.",
        "soundtagelementnamebag": "Bag of names for sound tag elements.",
        "evidencedeficienttagelementnamebag": "Bag of names for evidence-deficient tag elements.",
        "evidencesatisfactorytagelementnamebag": "Bag of names for evidence-satisfactory tag elements.",
        "evidenceexcellenttagelementnamebag": "Bag of names for evidence-excellent tag elements.",
        "writingdeficienttagelementnamebag": "Bag of names for writing-deficient tag elements.",
        "writingsatisfactorytagelementnamebag": "Bag of names for writing-satisfactory tag elements.",
        "writingexcellenttagelementnamebag": "Bag of names for writing-excellent tag elements.",
        "searchsufficientindicator": "Indicator if the search is sufficient (True/False).",
        "qualitymetricdeficientindicator": "Indicator if the quality metric is deficient (True/False).",
        "mississueindicator": "Indicator if there is a missed issue (True/False).",
        "newissueindicator": "Indicator if there is a new issue (True/False).",
        "refusalunsoundindicator": "Indicator if the refusal is unsound (True/False).",
        "substantivedeficientindicator": "Indicator if there is a substantive deficiency (True/False).",
        "proceduraldeficientindicator": "Indicator if there is a procedural deficiency (True/False).",
        "overalldeficientindicator": "Indicator if overall deficient (True/False).",
        "overallexcellentindicator": "Indicator if overall excellent (True/False).",
        "evidencedeficientindicator": "Indicator if evidence is deficient (True/False).",
        "evidencesatisfactoryindicator": "Indicator if evidence is satisfactory (True/False).",
        "evidenceexcellentindicator": "Indicator if evidence is excellent (True/False).",
        "writingdeficientindicator": "Indicator if writing is deficient (True/False).",
        "writingsatisfactoryindicator": "Indicator if writing is satisfactory (True/False).",
        "writingexcellentindicator": "Indicator if writing is excellent (True/False).",
        "substantiveerrorindicator": "Indicator if there is a substantive error (True/False).",
        "satisfactoryindicator": "Indicator if satisfactory (True/False).",
        "findingindicator": "Indicator if finding is present (True/False).",
        "createdatetime": "Timestamp when the record was created.",
        "createuseridentifier": "Identifier of the user who created the record.",
        "lastmodifieddatetime": "Timestamp when the record was last modified.",
        "lastmodifieduseridentifier": "Identifier of the user who last modified the record.",
        "go_final": "Final go status of the review.",
        "quality_review_id": "Unique identifier for the quality review.",
        "create_ts": "Timestamp when the record was created.",
        "create_user_id": "User ID of the individual who created the record.",
        "update_ts": "Timestamp when the record was last updated.",
        "update_user_id": "User ID of the individual who last updated the record."
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "tqr_detail_metrics_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is designed to store summary information about the review process of certain records or data points. Overall, the {table_name} table serves as a log or summary of the review process, tracking when it happened, how many records were reviewed, and who was involved in creating and updating the summary.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "min_lastreviewdatetime": "The earliest review datetime in the records",
        "max_lastreviewdatetime": "The latest review datetime in the records",
        "record_ct": "The count of records",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last updater"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "ttab_detail_appeals"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is a collection of detailed records about appeals related to trademark cases. This table serves as a comprehensive resource for tracking the progress and outcomes of trademark appeal cases, providing valuable insights into the legal proceedings surrounding trademark disputes.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "serial_number": "Unique identifier for each trademark case",
        "TTAB_ISSUE_TYPE": "Type of issue raised in the TTAB proceedings",
        "PROCEEDING_NUM": "Number assigned to the proceeding",
        "FINAL_REFUSAL_DATE": "Date of final refusal",
        "FILING_DATE": "Date when the case was filed",
        "INSTITUTED_CODE": "Code related to the institution of the case",
        "INSTITUTED_DATE": "Date when the case was officially instituted",
        "DECISION_DATE": "Date when a decision was made on the case",
        "DECISION_CODE": "Code representing the type of decision made",
        "DECISION_DESCRIPTION": "Description of the decision",
        "TERMINATION_CODE": "Code indicating the reason for case termination",
        "FP_REASON_1": "First reason for final refusal",
        "FP_REASON_2": "Second reason for final refusal",
        "FP_REASON_3": "Third reason for final refusal",
        "FP_REASON_4": "Fourth reason for final refusal",
        "FP_REASON_5": "Fifth reason for final refusal",
        "TERMINATION_DATE": "Date when the case was terminated",
        "TERMINATION_DATE_2": "Additional termination date, if applicable",
        "TERMINATION_DATE_3": "Additional termination date, if applicable",
        "TERMINATION_DATE_4": "Additional termination date, if applicable",
        "TERMINATION_DATE_5": "Additional termination date, if applicable",
        "APPEAL": "Indicator of whether the case was appealed",
        "INVENTORY": "Indicator of whether the case is part of the inventory",
        "PENDENCY_D": "Pendency in days",
        "PENDENCY_T": "Pendency in terms",
        "PENDENCY_R": "Pendency in rounds",
        "PUBLICATION_DATE": "Date of publication"
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "ttab_detail_appeals_1"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is a structured collection of data that stores comprehensive information about appeals related to trademark cases. This table serves as a vital resource for tracking the progress and outcomes of trademark appeal cases, providing stakeholders with detailed insights into each cases specifics and status.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "serial_number": "Unique identifier for each trademark case",
        "TTAB_ISSUE_TYPE": "Type of issue raised in the TTAB proceedings",
        "PROCEEDING_NUM": "Number assigned to the proceeding",
        "FINAL_REFUSAL_DATE": "Date of final refusal issued by the TTAB",
        "FILING_DATE": "Date when the case was filed",
        "INSTITUTED_CODE": "Code related to the institution of the case",
        "INSTITUTED_DATE": "Date when the case was officially instituted",
        "DECISION_DATE": "Date when a decision was made on the case",
        "DECISION_CODE": "Code representing the type of decision made",
        "DECISION_DESCRIPTION": "Description of the decision",
        "TERMINATION_CODE": "Code indicating the reason for case termination",
        "FP_REASON_1": "First reason for final refusal",
        "FP_REASON_2": "Second reason for final refusal",
        "FP_REASON_3": "Third reason for final refusal",
        "FP_REASON_4": "Fourth reason for final refusal",
        "FP_REASON_5": "Fifth reason for final refusal",
        "TERMINATION_DATE": "Date when the case was terminated",
        "TERMINATION_DATE_2": "Additional termination date, if applicable",
        "TERMINATION_DATE_3": "Additional termination date, if applicable",
        "TERMINATION_DATE_4": "Additional termination date, if applicable",
        "TERMINATION_DATE_5": "Additional termination date, if applicable",
        "APPEAL": "Indicator of whether the case was appealed",
        "INVENTORY": "Indicator of whether the case is part of the inventory",
        "PENDENCY_D": "Pendency in days",
        "PENDENCY_T": "Pendency in terms",
        "PUBLICATION_DATE": "Date of publication",
        "REFUSAL": "Indicator of whether there was a refusal",
        "NON_PRO_SE": "Indicates if the party is representing themselves without a lawyer",
        "TEST_PCTRAM_LINK": "Link to the PCTRAM test",
        "LAW_OFFICE": "Law office handling the case",
        "FILING_BASIS_GRP": "Group of filing basis",
        "FILING_METHOD_CUR": "Current filing method",
        "AM_STAT": "AM status",
        "Owner_Name": "Name of the trademark owner",
        "CITY": "City of the trademark owner",
        "STATE": "State of the trademark owner",
        "Country_or_Area_Name": "Country or area name of the trademark owner",
        "Reg_Class_Count": "Count of registered classes",
        "Active_Class_Count": "Count of active classes",
        "Group_Type": "Type of group",
        "Concat_Class": "Concatenated class",
        "MARK_NM_SHORT": "Short name of the mark"
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "ttab_detail_cancellations"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is a structured collection of data that keeps track of specific legal actions called cancellations within the realm of trademark cases. This table serves as a vital resource for tracking the outcomes and details of trademark cancellation proceedings, providing insights into the legal processes surrounding trademark disputes.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "SERIAL_NUMBER": "Unique identifier for each trademark case.",
        "TTAB_ISSUE_TYPE": "Type of issue raised in the Trademark Trial and Appeal Board.",
        "PROCEEDING_NUM": "Number assigned to the proceeding.",
        "FILING_DATE": "Date when the case was filed.",
        "INSTITUTED_DATE": "Date when the case was officially instituted.",
        "INSTITUTED_CODE": "Code related to the institution of the case.",
        "DECISION_DATE": "Date when a decision was made on the case.",
        "DECISION_CODE": "Code representing the type of decision made.",
        "DECISION_DESCRIPTION": "Description of the decision.",
        "TERMINATION_CODE": "Code indicating the reason for case termination.",
        "TERMINATION_DATE": "Date when the case was terminated.",
        "TERMINATION_DATE_2": "Additional termination date, if applicable.",
        "TERMINATION_DATE_3": "Additional termination date, if applicable.",
        "TERMINATION_DATE_4": "Additional termination date, if applicable.",
        "TERMINATION_DATE_5": "Additional termination date, if applicable.",
        "CONSTRUCTED_PRCD_NUM": "Constructed proceeding number for internal use.",
        "CANCELLATION": "Indicator of whether the case led to a cancellation.",
        "INVENTORY": "Indicator of whether the case is part of the inventory.",
        "DEFAULT_DATE": "Date of default judgment, if applicable.",
        "DEFAULT_CANCELLATION": "Indicator of whether the case led to a default cancellation."
    }



    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "ttab_detail_concurrent_filings"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is a structured collection of data that tracks specific details about trademark cases handled by the Trademark Trial and Appeal Board (TTAB). This table serves as a comprehensive resource for tracking the progress and outcomes of trademark cases that involve concurrent filings, providing valuable insights into the legal processes surrounding trademark disputes.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "SERIAL_NUMBER": "Unique identifier for each trademark case",
        "TTAB_ISSUE_TYPE": "Type of issue raised in the TTAB proceedings",
        "INSTITUTED_DATE": "Date when the case was officially instituted",
        "INSTITUTED_CODE": "Code related to the institution of the case",
        "PROCEEDING_NUM": "Number assigned to the proceeding",
        "DECISION_DATE": "Date when a decision was made on the case",
        "DECISION_CODE": "Code representing the type of decision made",
        "DECISION_DESCRIPTION": "Description of the decision",
        "TERMINATION_CODE": "Code indicating the reason for case termination",
        "TERMINATION_DATE": "Date when the case was terminated",
        "TERMINATION_DATE_2": "Additional termination date, if applicable",
        "TERMINATION_DATE_3": "Additional termination date, if applicable",
        "TERMINATION_DATE_4": "Additional termination date, if applicable",
        "TERMINATION_DATE_5": "Additional termination date, if applicable",
        "FILING_DATE": "Date when the case was filed",
        "FILED_YR": "Year when the case was filed",
        "INST_YR": "Year when the case was instituted",
        "TERM_YR": "Year when the case was terminated",
        "DECISION_YR": "Year when the decision was made",
        "PENDENCY_D": "Pendency in days",
        "PENDENCY_T": "Pendency in terms",
        "CONCURRENT": "Indicator of concurrent filings",
        "INVENTORY": "Indicator of whether the case is part of the inventory"
    }



    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "ttab_detail_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is designed to store and track statistical data related to certain processes or outputs over time. Overall, the table serves as a monitoring tool, providing a historical view of activity levels, trends, and operational decisions based on the recorded data.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "record_output_date": "The date when the record was output",
        "output_record_count": "The count of records output on the given date",
        "record_output_percent_change": "The percentage change in record output compared to the previous period",
        "continue_process": "Indicator if the process should continue (1) or not (0)",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last updater"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "ttab_detail_oppositions"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table contains detailed information about opposition proceedings related to trademark applications. These proceedings occur when someone objects to the registration of a new trademark. This table is essential for tracking and managing the legal process surrounding trademark oppositions, providing a comprehensive overview of each cases status, decisions, and outcomes.')
        """
    spark.sql(alter_table_query)

    columns_comments = {
        "SERIAL_NUMBER": "Unique identifier for each trademark case.",
        "TTAB_ISSUE_TYPE": "Type of issue raised in the TTAB proceedings.",
        "PROCEEDING_NUM": "Number assigned to the proceeding.",
        "FILING_DATE": "Date when the case was filed.",
        "INSTITUTED_DATE": "Date when the case was officially instituted.",
        "INSTITUTED_CODE": "Code related to the institution of the case.",
        "DECISION_DATE": "Date when a decision was made on the case.",
        "DECISION_CODE": "Code representing the type of decision made.",
        "DECISION_DESCRIPTION": "Description of the decision.",
        "TERMINATION_CODE": "Code indicating the reason for case termination.",
        "TERMINATION_DATE": "Date when the case was terminated.",
        "TERMINATION_DATE_2": "Additional termination date, if applicable.",
        "TERMINATION_DATE_3": "Additional termination date, if applicable.",
        "TERMINATION_DATE_4": "Additional termination date, if applicable.",
        "TERMINATION_DATE_5": "Additional termination date, if applicable.",
        "CONSTRUCTED_PRCD_NUM": "Constructed proceeding number for internal use.",
        "OPPOSITION": "Indicator of whether the case involved an opposition.",
        "INVENTORY": "Indicator of whether the case is part of the inventory.",
        "DEFAULT_DATE": "Date of default judgment, if applicable.",
        "DEFAULT_OPPOSITION": "Indicator of whether the case led to a default opposition."
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "ttab_detail_summary"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table contains information about trademark cases in the TTAB (Trademark Trial and Appeal Board) proceedings. It includes details such as the type of issue raised, the proceeding number, filing dates, decision dates, termination dates, and various codes related to the proceedings. Additionally, it tracks information about appeals, inventory status, pendency, publication dates, oppositions, cancellations, and other relevant details. This table is used to store and analyze data related to trademark cases and their associated proceedings.')
        """
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "serial_number": "Serial number of the entity",
        "ttab_issue_type": "Type of issue identified by TTAB",
        "proceeding_num": "Number of the proceeding",
        "filing_date": "Date the document was filed",
        "instituted_date": "Date the proceeding was instituted",
        "instituted_code": "Code related to the institution of the proceeding",
        "decision_date": "Date of the decision",
        "decision_code": "Code related to the decision",
        "decision_description": "Description of the decision",
        "termination_code": "Code related to the termination of the proceeding",
        "termination_date": "Date of termination",
        "termination_date_2": "Secondary date of termination if applicable",
        "termination_date_3": "Tertiary date of termination if applicable",
        "termination_date_4": "Quaternary date of termination if applicable",
        "termination_date_5": "Quinary date of termination if applicable",
        "final_refusal_date": "Date of final refusal",
        "fp_reason_1": "First reason for final position",
        "fp_reason_2": "Second reason for final position",
        "fp_reason_3": "Third reason for final position",
        "fp_reason_4": "Fourth reason for final position",
        "fp_reason_5": "Fifth reason for final position",
        "appeal": "Indicates if there was an appeal",
        "inventory": "Indicates if this is part of the inventory",
        "pendency_d": "Pendency in days",
        "pendency_t": "Pendency in terms",
        "publication_date": "Date of publication",
        "constructed_prcd_num": "Constructed proceeding number",
        "opposition": "Indicates if there was opposition",
        "default_date": "Default date",
        "default_opposition": "Indicates if there was a default in opposition",
        "cancellation": "Indicates if there was a cancellation",
        "default_cancellation": "Indicates if there was a default in cancellation",
        "concurrent": "Indicates if concurrent",
        "rfd_date": "Date of RFD",
        "rfd_valid": "Indicates if RFD is valid",
        "proceeding_count": "Count of proceedings",
        "non_pro_se": "Indicates if non-pro-se",
        "pctram_link": "Link to PCTRAM",
        "law_office": "Associated law office",
        "filing_basis_grp": "Filing basis group",
        "filing_method_cur": "Current filing method",
        "am_stat": "AM status",
        "owner_name": "Name of the owner",
        "city": "City of the owner",
        "state": "State of the owner",
        "country_or_area_name": "Country or area name of the owner",
        "reg_class_count": "Count of registered classes",
        "active_class_count": "Count of active classes",
        "group_type": "Type of group",
        "concat_class": "Concatenated class",
        "mark_nm_short": "Short name of the mark",
        "create_ts": "Timestamp of creation",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp of last update",
        "update_user_id": "User ID of the last updater"
    }

    # Alter table to add comments for each column
    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
   print(f"table {table_name} does not exist")

# COMMAND ----------

spark.sql(f"""
ALTER TABLE {trm_reporting_catalog}.silver.bibliography
ADD COLUMNS (SRCHRG_IND STRING)
""")

# COMMAND ----------

# Set table properties
table_name = 'currently_processing_first_actions_with_controls'
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('comment' = 'The {table_name} table is designed to store detailed information about the processing of first actions with controls in trademark cases. It tracks various metrics and identifiers related to the pendency and processing of these actions, providing insights into the workflow and efficiency of trademark case management.')
        """

    spark.sql(alter_table_query)

    # Change column comments
    columns_comments = {
        "min_record_id": "Minimum record ID in the dataset",
        "record_id": "Unique identifier for each record",
        "pendency_cal_start_dt": "Start date for pendency calculation",
        "cases": "Number of cases docketed",
        "sum_cases": "Cumulative sum of cases",
        "percent_total": "Percentage of total cases",
        "test": "Test flag for internal use",
        "date_plus_two": "Date plus two days",
        "today": "Current date",
        "current_process_pendency": "Pendency of the current processing start date",
        "datetime_out1": "First output datetime",
        "datetime_out2": "Second output datetime",
        "text": "Text field for additional information",
        "todays_date": "Todays date",
        "email_sub": "Email subject for notifications"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "df_active_table"
if tableExists(table_name):
    # Add comments to the table
    spark.sql(f"""
        COMMENT ON TABLE {table_name} IS 'Table to store active records with term details and status history'
    """)

    # Add comments to the columns
    columns_comments = {
        "record_id": "Unique identifier for each record",
        "Term_ID": "Identifier for the term associated with the record",
        "Class": "Classification of the term",
        "Description": "Description of the term",
        "Status": "Current status of the term",
        "Type": "Type of the term",
        "Notes": "General notes related to the term",
        "Employee_Notes": "Notes added by employees",
        "Editor_Notes": "Notes added by editors",
        "Stage": "Current stage of the term in the process",
        "TM5": "TM5 classification indicator",
        "NCL_Version": "Version of the NCL classification",
        "Start_Effective_Date": "Start date of the term effectiveness",
        "End_Effective_Date": "End date of the term effectiveness",
        "Fiscal_year": "Fiscal year associated with the term"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------



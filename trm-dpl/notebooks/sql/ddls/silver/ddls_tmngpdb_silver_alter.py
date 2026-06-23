# Databricks notebook source
dbutils.widgets.text("dbx_env", "dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngpdb-conf.yaml"
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
tmngpdb_catalog = common_configs["schema"]["trgt_catalog"]
data_quality_catalog = common_configs["schema"]["data_quality_catalog"]
print(f"{tmngpdb_catalog=}, {data_quality_catalog=}")


# spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
# spark.conf.set('conf.catalog', tmngpdb_catalog.lower())
# spark.conf.set('dbx_env', dbx_env)

# COMMAND ----------

database = "silver"
control_table = "cdc_batch_job_control"
job_history_table = "cdc_batch_job_history"

spark.conf.set("conf.catalog", tmngpdb_catalog)
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

tables_to_comment = {'bdss_correspondent_data_daily_stg': "The bdss_correspondent_data_daily_stg table in the silver schema of the trm_tmngpdb catalog contains daily data related to correspondents. It includes information such as the correspondents serial number, address details (address1, address2, address3, address4, and address5). This table is significant to the business as it provides a snapshot of the correspondents addresses on a daily basis, allowing for analysis and tracking of changes over time.", 'bdss_case_file_data_daily_stg': 'The bdss_case_file_data_daily_stg table contains data related to case files in the business. It includes information such as the serial number of the case, international registration number, dates of international registration, prior claim status, death date, status of the case, renewal date, automatic protection date, publication dates, amendment registration date, cancellation code, filing date, and status date. This table serves as a repository for important details about the case files and is used for tracking and managing the progress and status of each case.', 'bdss_class': 'COMMENT REQUIRED', 'bdss_case_events_daily_stg': 'The bdss_case_events_daily_stg table contains data related to daily case events in the business. It includes information such as the serial number of the case, the entity number and code associated with the case, text descriptions, the type of entity, the date of entry, the timestamp of creation, and the user ID of the creator. This table serves as a staging area for daily case events data, providing a centralized location for analysis and reporting purposes.', 'bdss_owners_data_daily_stg': 'The bdss_owners_data_daily_stg table contains information about owners of various entities. It includes data such as the serial number, entity number, citizenship, name, party type, address, city, state/country code, zip code, and entity type. This table also includes flags indicating the presence of certain information like address, company statement, DBA/AKA, entity, and name text. The PTYPEENUMBER column represents the type of party associated with the owner. Overall, this table provides a comprehensive view of the owners associated with different entities and their relevant details.', 'bdss_name_change_daily_stg': 'The bdss_name_change_daily_stg table in the silver schema of the trm_tmngpdb catalog contains data related to daily name change events. It includes information such as the text of the name change, the serial number associated with the name change, the type of text, and additional text related to the name change. This table is significant to the business as it provides a record of all name change events on a daily basis, allowing for tracking and analysis of name change trends and patterns.', 'bdss_wipo_daily_stg': 'The bdss_wipo_daily_stg table in the trm_tmngpdb catalog of the silver schema represents daily data related to World Intellectual Property Organization (WIPO) records. It contains information such as the serial number (sernum) of the records, the WIPO code (wp_wipo_cd) associated with each record, the timestamp (create_ts) when the record was created, and the user ID (create_user_id) of the user who created the record. This table is essential for tracking and analyzing WIPO data on a daily basis, providing valuable insights for business decision-making and intellectual property management.', 'bdss_designs_daily_stg': 'The bdss_designs_daily_stg table in the silver schema of the trm_tmngpdb catalog contains data related to daily designs. It includes information such as the serial number, the WIPO code, the timestamp of creation, and the user ID of the creator. This table is significant to the business as it allows tracking and analysis of daily design activities, providing insights into design trends and user engagement.', 'bdss_foreign_apps_daily_stg': 'The bdss_foreign_apps_daily_stg table contains data related to foreign applications for a specific entity. It includes information such as the serial number, foreign filing date, foreign registration date, foreign expiration date, renewal registration date, renewal expiration date, entity number, foreign application number, foreign registration number, renewal registration number, whether a foreign priority claim is made, foreign country code, country, and other miscellaneous information. This table is significant to the business as it provides a comprehensive record of foreign applications and their associated details for analysis and decision-making purposes.', 'bdss_madrid_and_history_data_daily_stg': 'The bdss_madrid_and_history_data_daily_stg table contains daily data related to Madrid and history. It includes information such as control numbers, actions taken, dates, text descriptions, row numbers, original file dates, international registration numbers, statuses, reply by dates, renewal dates, serial numbers, and more. This table is significant to the business as it provides a comprehensive view of daily activities and events related to Madrid and history, allowing for analysis and decision-making based on the data.', 'job_control': 'The job_control table contains information about various jobs that are being executed in the business. It provides details such as the job ID, job name, timestamps for when the job was loaded, created, and last modified, as well as the user IDs of the individuals responsible for creating and modifying the job. This table is essential for tracking and managing the execution of jobs within the business, allowing for effective monitoring and control of job processes.', 'tmapplser': 'The tmapplser table contains information about the actions associated with loads. It includes the action code, serial number of the load, date the row was pulled from source tables, and the name of the table from which the row was pulled. Additionally, it tracks the timestamps of when the record was created and last modified in the database, along with the user identifiers of the users who initiated these actions. The table also includes a lock control number for optimistic locking purposes. Overall, this table provides valuable insights into the actions performed on loads and their associated details.', 'bdss_prior_regs_daily_stg': 'The bdss_prior_regs_daily_stg table contains data related to prior registrations in the business. It includes information such as the serial number, record type, release ID number, creation timestamp, and the ID of the user who created the record. This table is used to track and manage prior registrations, providing a historical record of activities within the business. The data in this table is crucial for analyzing trends, identifying patterns, and making informed decisions based on past registration data.', 'job_log': 'COMMENT REQUIRED', 'bdss_vt_text_data_daily_stg': 'The bdss_vt_text_data_daily_stg table in the silver schema of the trm_tmngpdb catalog contains daily text data related to various types. This table is significant to the business as it stores valuable information that can be used for analysis and insights. The vt_text_type column represents the type of text data, while the vt_text column contains the actual text content. The sernum column is an identifier associated with each record. This table serves as a staging area for daily text data, allowing further processing and analysis to be performed on the data.'}

for table_name, comment in tables_to_comment.items():
    alter_table_query = f"""
    ALTER TABLE {tmngpdb_catalog}.{database}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'sernum': 'The serial number of the correspondent.', 'address3': "The third line of the correspondents address.", 'address1': "The first line of the correspondents address.", 'address2': "The second line of the correspondents address.", 'address4': "The fourth line of the correspondents address.", 'address5': "The fifth line of the correspondents address."}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_correspondent_data_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'FLG_USE_AMED': 'Flag indicating if the use statement was amended', 'DT_AMND_REG': 'The date of amendment registration for the case', 'DT_PUB': 'The publication date of the case', 'FLG_3D_DRW_FIL': 'Flag indicating if there is a 3D drawing filing for the case', 'STAT_DT': 'The date of the case status', 'FLG_SCT_8_FIL': 'Flag indicating if the case has been reported as section 8 filed', 'FLG_ITU_AMED': 'Flag indicating if the intent to use was amended', 'FLG_44E_FIL': 'Flag indicating if there is a 44E filing for the case', 'FLG_COLL_TM': 'Flag indicating if it is a collective trademark case', 'FLG_SM': 'Flag indicating if it is a service mark case', 'FLG_STD_CHAR': 'Flag indicating if there is a standard character for the case', 'FLG_RPB_SCT_12': 'Flag indicating if the case has been reported as section 12', 'AM_FLG_MARK_OFLW': 'Flag indicating if there is an overflow of marks for the case', 'FLG_PRIOR_CLMD': 'Flag indicating if there is a prior claim for the case', 'CNCL_CD': 'The cancellation code for the case', 'FLG_C_DRW_CUR': 'Flag indicating if there is a current C drawing for the case', 'RI_SER_NUM': 'The serial number of the case file', 'FLG_COLL_SM': 'Flag indicating if it is a collective service mark case', 'DT_RNWL': 'The renewal date of the case', 'FLG_CM': 'Flag indicating if it is a certification mark case', 'CURR_LOC': 'The current location of the case', 'FLG_ITU_CUR': 'Flag indicating if the intent to use is current', 'EMPE_NAM': 'The name of the employee associated with the case', 'FLG_COLL_MM': 'Flag indicating if it is a collective membership mark case', 'FLG_USE_CUR': 'Flag indicating if the use statement is current', 'FLG_3D_DRW_CUR': 'Flag indicating if there is a current 3D drawing for the case', 'RI_INTL_REG_NUM': 'The international registration number of the case', 'LO_ASGN': 'The location assignment for the case', 'MARK_1_LIN': 'The first line of the mark for the case', 'DT_STAT': 'The status date of the case', 'FLG_SCT_8_P_A': 'Flag indicating if section 8 was filed with a positive acknowledgment', 'AM_STAT': 'The status of the amendment', 'ATTY_DKT_NUM': 'The attorney docket number for the case', 'DT_ABAN': 'The date of abandonment for the case', 'FLG_SCT_8_ACPT': 'Flag indicating if the case has been reported as section 8 accepted', 'FLG_44E_CUR': 'Flag indicating if there is a current 44E for the case', 'FLG_1ST_REF': 'Flag indicating if it is the first reference for the case', 'PRIOR_CLMD_DT': 'The date of the prior claim', 'FLG_66A_CUR': 'Flag indicating if there is a current 66A for the case', 'STAT': 'The status of the case', 'DT_CNCL': 'The date of cancellation for the case', 'FLG_OPPS_PEND': 'Flag indicating if the opposition status is pending', 'FLG_NO_BAS_CUR': 'Flag indicating if there is no basis current for the case', 'FLG_TM': 'Flag indicating if it is a trademark case', 'FLG_AMND_SUPL': 'Flag indicating if there is an amendment to the supplementary', 'FLG_44E_AMED': 'Flag indicating if there is a 44E amendment for the case', 'FLG_CHNG_REG': 'Flag indicating if a change of registration was filed', 'AM_MARK_DWG_CD': 'The drawing code for the amendment', 'FLG_USE_FIL': 'Flag indicating if a use statement was filed', 'FLG_NO_BAS_FIL': 'Flag indicating if there is no basis filing for the case', 'FLG_PUB_CNCR': 'Flag indicating if the publication is concurrent', 'DT_IN_LOC': 'The date of input location for the case', 'REG_NUM': 'The registration number of the case', 'INTL_REG_DT': 'The date of international registration', 'FLG_66A_FIL': 'Flag indicating if there is a 66A filing for the case', 'AUTO_PROTEC_DT': 'The automatic protection date of the case', 'FLG_SUPL_REG': 'Flag indicating if supplemental registration was filed', 'DT_REG': 'The registration date of the case', 'FLG_CNCR': 'Flag indicating if the case is concurrent', 'FLG_SCT_2F_PT': 'Flag indicating if the case has been reported as section 2(f) with a disclaimer of part of the mark', 'FLG_44D_CUR': 'Flag indicating if the §44(d) filing is current', 'FLG_SCT_15_FIL': 'Flag indicating if section 15 was filed', 'FLG_44D_FIL': 'Flag indicating if a §44(d) filing was made', 'FLG_ITU_FIL': 'Flag indicating if an intent to use was filed', 'FLG_FRPR_CLMD': 'Flag indicating if a foreign priority claim was made', 'DEATH_DT': 'The date of death related to the case', 'DT_FIL': 'The filing date of the case', 'FLG_CNCR_PEND': 'Flag indicating if the concurrent status is pending', 'FLG_SCT_15_ACK': 'Flag indicating if section 15 was filed with an acknowledgment', 'FLG_AMND_PRIN': 'Flag indicating if there is an amendment to the principal', 'DT_PUB_12_C': 'The publication date of the case in Class 12', 'FLG_C_DRW_FIL': 'Flag indicating if there is a C drawing filing for the case', 'RNWL_DT': 'The renewal date of the case', 'FLG_RNWL_FIL': 'Flag indicating if renewal was filed', 'FLG_INTF_PEND': 'Flag indicating if the interface status is pending', 'FLG_AND_OTH_CD': "Flag indicating if there is an *and other* code for the case", 'FLG_44D_AMED': 'Flag indicating if the §44(d) filing was amended', 'FLG_CNCL_PEND': 'Flag indicating if the case cancellation is pending', 'FLG_SCT_2F': 'Flag indicating if the case has been reported as section 2(f)', 'IB_PUB_DT': 'The publication date of the case in the International Bureau', 'APPLY_TIME': 'The time of application for the case', 'AM_SER_NUM': 'The serial number of the case file in the AM system'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_case_file_data_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'dt_1_use_comm': 'date of first use commercial', 'prime_cls': 'prime class', 'dt_stat': 'status date', 'cl_cls_us_ct': 'count of us classes', 'cls_us': 'list of us classes', 'cl_cls_intl_ct': 'count of international classes', 'create_ts': 'The timestamp of when the record was created', 'dt_1_use': 'date of first use', 'cls_stat': 'status of classes', 'cl_ser_num': 'The serial number of load', 'create_user_id': 'The user ID that created the record', 'cls_intl': 'list of international classes'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_class",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'ent_dt': 'The date of entry for the case event.', 'CM_ENT_TYPE': 'The type of entity associated with the case.', 'tt_text_1': 'A text description related to the case event.', 'create_user_id': 'The user ID that created the record', 'CM_SER_NUM': 'The serial number of the case.', 'CM_ENT_NUM': 'The entity number associated with the case.', 'create_ts': 'The timestamp of when the record was created', 'CM_ENT_CD': 'The code associated with the case entity.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_case_events_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'CITIZENSHIP': 'Citizenship of the owner', 'PY_FLG_CMP_STMT': 'Flag indicating presence of company statement information', 'STE_CTRY_CD': "State or country code of the owners address", 'NAM': 'Name of the owner', 'PTYPEENUMBER': 'Type number associated with the party type', 'PY_FLG_DBA_AKA': 'Flag indicating presence of DBA/AKA information', 'PY_ENT_NUM': 'Entity number associated with the owner', 'ZIP_CD': "Zip code of the owners address", 'CITIZEN_COUNTRY': "Country of the owners citizenship", 'sernum': 'Serial number of the owner', 'PY_FLG_ENTITY': 'Flag indicating presence of entity information', 'PY_FLG_NAM_TEXT': 'Flag indicating presence of name text information', 'PY_FLG_ADDR_2': 'Flag indicating presence of address line 2 information', 'CITIZEN_STATE': "State of the owners citizenship", 'STATE': "State of the owners address", 'PY_FLG_ADDR_1': 'Flag indicating presence of address line 1 information', 'CITY': "City of the owners address", 'COUNTRY': "Country of the owners address", 'CITIZEN_OTHER': "Other information about the owners citizenship", 'ADDR_2': 'Address line 2 of the owner', 'PARTY_TYPE': 'Type of party associated with the owner', 'ADDR_1': 'Address line 1 of the owner', 'ENTITY_TYPE': 'Type of entity associated with the owner'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_owners_data_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

if 1==0:
    # Define a dictionary of column comments
    column_comments = {'actcd': 'The action code associated with the load', 'pulldt': 'The date row was pulled from source tables', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'create_user_id': 'The user ID that created the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_ts': 'The timestamp of when the record was created', 'tabname': 'The table_name were row was pulled from', 'sernum': 'The serial number of load'}

    # Loop through the columns and generate column comment queries
    for column, comment in column_comments.items():
        column_comment_query = """
        ALTER TABLE {catalog}.{database}.{table}
        ALTER COLUMN {column_name}
        COMMENT '{comment}'
        """.format(
            catalog=tmngpdb_catalog,
            database=database,
            table="tmappsler",
            column_name=column,
            comment=comment,
        )

        spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'name_change_text': 'Additional text related to the name change event.', 'vt_ser_num': 'The serial number associated with the name change event.', 'vt_text': 'The text of the name change event.', 'vt_text_type': 'The type of text for the name change event.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_name_change_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'wp_wipo_cd': 'The WIPO code associated with each record.', 'create_user_id': 'The user ID that created the record', 'sernum': 'The serial number of the WIPO record.', 'create_ts': 'The timestamp of when the record was created'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_wipo_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'sernum': 'The serial number of the design.', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'wp_wipo_cd': 'The WIPO code associated with the design.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_designs_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'country': 'The name of the country.', 'create_user_id': 'The user ID that created the record', 'create_ts': 'The timestamp of when the record was created', 'frgn_appl_num': 'The foreign application number.', 'other': 'Other miscellaneous information related to the foreign application.', 'rnwl_reg_num': 'The renewal registration number.', 'frgn_reg_num': 'The foreign registration number.', 'flg_frpr_clmd': 'Indicates whether a foreign priority claim is made.', 'fn_ent_num': 'The entity number associated with the foreign application.', 'dt_frgn_reg': 'The date of foreign registration.', 'dt_frgn_fil': 'The date of foreign filing.', 'fn_frgn_ctry_cd': 'The country code of the foreign country.', 'dt_frgn_exp': 'The date of foreign expiration.', 'dt_rnwl_exp': 'The date of renewal expiration.', 'fn_ser_num': 'The serial number of the foreign application.', 'dt_rnwl_reg': 'The date of renewal registration.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_foreign_apps_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'tt_text_1': 'Text description for the data', 'intl_reg_num': 'International registration number', 'ent_dt': 'Date of entry for the data', 'mas_ctl_num': 'Control number for master data', 'stat': 'Status of the data', 'reply_by_dt': 'Date for reply by', 'mRow': 'Row number for data', 'orig_fil_dt': 'Original file date for the data', 'sernum': 'Serial number for the data', 'rnwl_dt': 'Renewal date for the data', 'stat_dt': 'Date of status update', 'hRow': 'Row number for historical data', 'mhi_action': 'Action taken for Madrid and history data', 'mhi_ctl_num': 'Control number for Madrid and history data', 'intl_reg_dt': 'International registration date'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_madrid_and_history_data_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'job_nm': 'The name of the job being executed.', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record', 'job_control_id': 'The unique identifier for each job in the job_control table.', 'last_mod_user_id': 'The user ID that last modified the record', 'create_ts': 'The timestamp of when the record was created', 'load_ts': 'The timestamp when the job was loaded into the system.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="job_control",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'pulldt': 'The date row was pulled from source tables', 'sernum': 'The serial number of load', 'tabname': 'The table_name were row was pulled from', 'actcd': 'The action code associated with the load', 'create_ts': 'The timestamp of when the record was created', 'lock_control_no': 'A number used for locking purposes', 'last_mod_user_id': 'The user ID that last modified the record', 'last_mod_ts': 'The timestamp of when the record was last modified', 'create_user_id': 'The user ID that created the record'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="tmapplser",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'create_user_id': 'The user ID that created the record', 'sernum': 'The serial number of the prior registration record.', 'create_ts': 'The timestamp of when the record was created', 'pr_rcd_type': 'The type of record for the prior registration.', 'pr_rel_id_num': 'The ID number of the release associated with the prior registration.'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_prior_regs_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'start_ts': 'Timestamp indicating the start time of the job', 'end_ts': 'Timestamp indicating the end time of the job', 'job_nm': 'Name of the job', 'status_ct': 'Status code indicating the status of the job', 'job_log_id': 'Unique identifier for each job log entry', 'comment_tx': 'Additional comments or notes about the job', 'src_cnt': 'Number of records in the source dataset', 'trgt_cnt': 'Number of records in the target dataset'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="job_log",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {'vt_text_type': 'The type of text data', 'sernum': 'An identifier associated with each record', 'vt_text': 'The actual text content'}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
    column_comment_query = """
    ALTER TABLE {catalog}.{database}.{table}
    ALTER COLUMN {column_name}
    COMMENT '{comment}'
    """.format(
        catalog=tmngpdb_catalog,
        database=database,
        table="bdss_vt_text_data_daily_stg",
        column_name=column,
        comment=comment,
    )

    spark.sql(column_comment_query)

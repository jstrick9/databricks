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
spark.catalog.setCurrentDatabase("gold")

# COMMAND ----------

def tableExists(tableName,schemaName='gold'):
    return spark.sql(f"show tables in {schemaName} like '{tableName}'").count() == 1

# COMMAND ----------

table_name = "filings_dashboard"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a comprehensive dataset that tracks various details about filings, such as applications or submissions, typically related to legal, patent, or trademark processes. This table serves as a dashboard for monitoring and analyzing filing activities, providing insights into trends, applicant demographics, and filing characteristics.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "ser_num": "Serial number of the filing",
        "pendency_cal_start_dt": "Start date for pendency calculation",
        "filing_fy": "Fiscal year of filing",
        "non_pro_se": "Indicates if filed by a professional (non-pro-se)",
        "filing_method_filed": "Method used for filing",
        "filing_basis_grp": "Basis group of the filing",
        "class": "Classification of the filing",
        "name": "Name of the filer or entity",
        "city": "City of the filer",
        "ste_ctry_cd": "State or country code",
        "postal_cd": "Postal code of the filer",
        "ctry_nm": "Country name",
        "country_or_area_name": "Country or area name",
        "count": "Count of filings",
        "max_pendency_cal_start_dt": "Maximum start date for pendency calculation",
        "coordinated_class": "Coordinated classification",
        "filing_fy2": "Secondary fiscal year of filing",
        "filing_fy_month_int": "Filing month as an integer",
        "filing_fy_quarter": "Fiscal quarter of filing",
        "filing_fy_month": "Filing month",
        "top_2_years": "Indicator if within the top 2 years",
        "fee_paid_class": "Class for which fee was paid",
        "max_filing_fy": "Maximum fiscal year of filing",
        "fixed_count": "Fixed count of filings",
        "realtime_count": "Real-time count of filings",
        "tram_count": "TRAM count",
        "goods_or_services": "Goods or services associated with the filing",
        "concat_goods_or_services": "Concatenated list of goods or services",
        "entity_type": "Type of entity filing",
        "applicant_bin": "Applicant binary identifier",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last updater",
        "output_record_count": "Count of output records"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """)
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "form_paragraph_dashboard"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a structured collection of data that tracks and organizes information related to paragraphs found in various forms. This table serves as a comprehensive dashboard for managing and analyzing form paragraphs, providing insights into their usage, categorization, and the actions associated with them.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "generated_date": "The date when the data was generated",
        "category": "Category of the form paragraph",
        "grade": "Grade assigned to the form paragraph",
        "data_through_date": "The date up to which data is included",
        "serial_number": "Serial number associated with the form paragraph",
        "group_name": "Name of the group associated with the form paragraph",
        "completed_date": "Date when the form paragraph was completed",
        "transaction_literal": "Literal description of the transaction",
        "action_count": "Count of actions associated with the form paragraph",
        "form_paragraph_id": "Unique identifier for the form paragraph",
        "title_text": "Title text of the form paragraph",
        "foreign_key_form_paragraph_group_id": "Foreign key linking to the form paragraph group",
        "foreign_key_form_paragraph_category_id": "Foreign key linking to the form paragraph category",
        "form_paragraph_year": "Year associated with the form paragraph",
        "toc_link": "Link to the table of contents",
        "concat_form_paragraph_id": "Concatenated form paragraph identifier",
        "concat_category": "Concatenated category information",
        "first_action_count_numerator": "Numerator for the first action count",
        "first_action_count_denominator": "Denominator for the first action count",
        "filing_basis_group": "Group basis on which the filing was made",
        "exam": "Examination status or details",
        "action_type": "Type of action taken",
        "completed_date_year": "Year when the form paragraph was completed",
        "completed_date_fiscal_year": "Fiscal year when the form paragraph was completed",
        "tm_analytics_ts": "Timestamp for trademark analytics",
        "transaction_number": "Number associated with the transaction",
        "action_type_2_possible_fix": "Possible fix for the second type of action",
        "law_office": "Law office associated with the form paragraph",
        "country_or_area_name": "Country or area name associated with the form paragraph",
        "last_modified_date": "Last date when the form paragraph was modified",
        "state_cd": "State code associated with the form paragraph",
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

table_name = "form_paragraph_enhancement"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name}  table is designed to store detailed information about enhancements or modifications made to paragraphs within forms, typically in a legal or regulatory context. This table serves as a comprehensive repository for managing and analyzing enhancements to form paragraphs, providing insights into the processes, personnel, and classifications involved.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "class": "Classification of the form paragraph",
        "ser_num_class": "Serial number classification",
        "class_no": "Class number",
        "modification_no": "Modification number",
        "title_tx": "Title text",
        "INTL_CLASS_SHORT_TITLE_TX": "International classification short title",
        "goods_and_services_desc": "Description of goods and services",
        "serial_number": "Serial number of the filing",
        "law_office": "Law office handling the filing",
        "country_or_area_name": "Country or area name associated with the filing",
        "FPEPCategory": "Form paragraph enhancement category",
        "FPEPCategoryID": "Form paragraph enhancement category ID",
        "FPEPYEAR": "Year of form paragraph enhancement",
        "FPEPWorkerID": "Worker ID associated with form paragraph enhancement",
        "FPEPActionCt": "Action count in form paragraph enhancement",
        "FPEPCompletedDt": "Completion date of form paragraph enhancement",
        "FPEPGroup": "Group associated with form paragraph enhancement",
        "FPEPFPID": "Form paragraph ID",
        "FPEPSerNum": "Serial number associated with form paragraph enhancement",
        "FPGroupID": "Form paragraph group ID",
        "FPEPTitle": "Title of form paragraph enhancement",
        "FPEPTransLit": "Literal transaction of form paragraph enhancement",
        "CREATE_USER_ID": "User ID of the creator",
        "FK_USER_ROLE_ID": "Foreign key to user role ID",
        "User_Role": "Role of the user",
        "FK_TM_ORGANIZATION_GID": "Foreign key to organization GID",
        "tm_organization_gid": "Trademark organization GID",
        "organization_cd": "Organization code",
        "organization_nm": "Organization name",
        "tmworkerNo": "Trademark worker number",
        "active_in": "Indicator if active",
        "worker_nm": "Worker name",
        "tmngpdbWorkerNo": "Trademark NGPDB worker number",
        "grade_cd": "Grade code",
        "brs_user_id": "BRS user ID",
        "FPDSerialNum": "Form paragraph dashboard serial number",
        "FPDActionType": "Action type in form paragraph dashboard"
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """) 
else:
    print(f"table {table_name} does not exist") 

# COMMAND ----------

table_name = "goods_services_dashboard"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a structured collection of data that provides detailed insights into filings related to goods and services. This table serves as a comprehensive dashboard for monitoring and analyzing filings related to goods and services, providing valuable insights into the types of filings being made, their timing, and the entities making them.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "ser_num": "Serial number of the filing",
        "class": "Classification of the filing",
        "coordinated_class": "Coordinated classification",
        "pendency_cal_start_dt": "Start date for pendency calculation",
        "filing_fy": "Fiscal year of filing",
        "non_pro_se": "Indicates if filed without professional legal representation",
        "filing_method_filed": "Method used for filing",
        "filing_basis_grp": "Basis group of the filing",
        "ste_ctry_cd": "State or country code",
        "country_or_area_name": "Country or area name",
        "max_pendency_cal_start_dt": "Maximum start date for pendency calculation",
        "filing_fy_quarter": "Fiscal quarter of filing",
        "filing_fy_month": "Filing month",
        "entity_type": "Type of entity filing",
        "applicant_bin": "Applicant binary identifier",
        "goods_or_services": "Goods or services associated with the filing",
        "goods_services_desc": "Description of goods or services",
        "class_count": "Count of classes filed",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last updater"
    }

    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column}  COMMENT '{comment}'
        """) 
else:
    print(f"table {table_name} does not exist") 

# COMMAND ----------

table_name = "inventory_dashboard_bd_occurrence"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to track and analyze occurrences within an inventory system, specifically focusing on financial analyses (FAs) conducted over time. This table serves as a dashboard for monitoring the progress and completion rates of financial analyses within an inventory system, offering a clear view of operational efficiency over time.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "FA_Month": "The month for which the financial analysis is conducted",
        "Percent_of_FAs": "Percentage of financial analyses completed",
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

table_name = "inventory_dashboard_ea_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to provide a snapshot of the examination process within an inventory or filing system. Additionally, the table tracks when each record was created and last updated, along with the IDs of the users who performed these actions. This information is crucial for managing and monitoring the efficiency and progress of the examination process within the system.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "EA_Not_Exam": "Count of entities not yet examined",
        "EA_Examining": "Count of entities currently being examined",
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

table_name = "inventory_dashboard_filings"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table stores information about filings in an inventory dashboard. It includes details such as the start date for pendency calculation, count of classes, type of count, current fiscal year, fiscal year, fiscal year plus, current fiscal year count type, timestamps for record creation and update, and user IDs of the creator and last updater. This table serves as a repository for managing and analyzing data related to filings, providing insights into various aspects of the inventory dashboard.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "Pendency_Cal_Start_DT": "Start date for pendency calculation",
        "Class_Count": "Count of classes",
        "Count_Type": "Type of count",
        "Current_FY": "Current fiscal year",
        "FY": "Fiscal year",
        "FY_Plus": "Fiscal year plus",
        "CurrentFY_CountType": "Current fiscal year count type",
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

table_name = "inventory_dashboard_pendency"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a part of an inventory dashboard and it stores information related to filings. It includes various details such as the sum of FA (First Action) Pendency Weight, the sum of Active Classes First Action, the weighted first action pendency for the current fiscal year, the date through which the data is captured, timestamps for record creation and update, and user IDs of the creator and last updater. This table helps in managing and analyzing data related to filings, providing insights into various aspects of the inventory dashboard. It allows users to track and monitor the pendency of filings, understand the workload of active classes, and analyze the efficiency of the first action process. The table serves as a repository for important information that can be used to make informed decisions and improve the overall performance of the inventory system.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "Sum_FAPendencyWeight": "Sum of FA Pendency Weight",
        "Sum_Active_Classes_FirstAction": "Sum of Active Classes First Action",
        "Current_FY_Weighted_First_Action_Pendency": "Weighted First Action Pendency for Current FY",
        "Data_Through": "Date through which the data is captured",
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

table_name = "inventory_dashboard_ratio"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to track and analyze the examination process of classes within an inventory system, focusing on the fiscal year. This table serves as a crucial tool for monitoring the efficiency and progress of the examination process, helping to identify bottlenecks and areas for improvement in managing the inventory of classes.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "FY": "Fiscal Year",
        "EA_Examining": "Count of Entities Currently Being Examined",
        "Unexamined_Classes": "Count of Classes Not Yet Examined",
        "EA_Unexamined_Ratio": "Ratio of Unexamined Classes to Examined",
        "Current_FY": "Indicates if the Data is for the Current Fiscal Year",
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

table_name = "inventory_dashboard_running"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to track and analyze the status of inventory over time, focusing on the examination process of various classes. This table serves as a comprehensive dashboard for monitoring the flow and examination status of inventory, helping to identify bottlenecks, track progress, and plan for future inventory management strategies.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "Pendency_Cal_Start_DT": "Start date for pendency calculation",
        "Class_Count": "Count of classes",
        "Count_Type": "Type of count",
        "Current_FY": "Indicates if the data is for the current fiscal year",
        "FY": "Fiscal year",
        "FY_Plus": "Fiscal year plus additional years",
        "Start_Non_Outlier": "Start date for non-outlier data",
        "RunTot_Class_Count": "Running total of class count",
        "EA_Not_Exam": "Count of entities not yet examined",
        "EA_Examining": "Count of entities currently being examined",
        "Today_Unexamined": "Count of todays unexamined entities",
        "CurrentFY_CountType": "Count type for the current fiscal year",
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

table_name = "inventory_madrid"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name}  table is designed to track and analyze specific metrics related to Madrid filings within an inventory system. This table serves as a valuable tool for understanding the efficiency and volume of Madrid filings within the system, providing insights that can help in managing and optimizing the trademark registration process.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "MADRID_PCT": "Percentage of Madrid filings",
        "MADRID_FA_Pendency": "Pendency of First Action for Madrid filings",
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

table_name = "inventory_unexamined_hstry"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name}  table stores historical data related to unexamined cases and classes within an inventory system. It includes information such as the unexamined date, the number of unexamined cases, the number of unexamined classes, the fiscal year, the count of entities currently being examined, the ratio of unexamined classes to examined, and a flag indicating if the data is for the current fiscal year. Additionally, the table tracks the timestamps and user IDs for record creation and updates. This table provides insights into the backlog of unexamined cases and classes, allowing for analysis of workload distribution and monitoring of examination progress. It helps in identifying areas that require attention and resource allocation to ensure efficient processing of cases and classes within the inventory system.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "unexamined_date": "Date of unexamined cases",
        "unexamined_cases": "Number of unexamined cases",
        "unexamined_classes": "Number of unexamined classes",
        "fy": "Fiscal year",
        "ea_examining": "Count of entities currently being examined",
        "ea_unexamined_ratio": "Ratio of unexamined classes to examined",
        "current_fy": "Indicates if the data is for the current fiscal year",
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

table_name = "pendency_dashboard"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is used to track and analyze various metrics related to the pendency of trademark applications. This table provides valuable insights into the pendency of trademark applications, allowing for analysis of processing times, workload distribution, and efficiency. It helps in identifying bottlenecks, monitoring progress, and making informed decisions to improve the trademark application process.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "first_action_pendency_ph": "Pendency of first action in hours",
        "first_action_dt_ph": "Date of first action",
        "first_action_type_num": "Numeric code for first action type",
        "abandonment_dt": "Date of abandonment",
        "active_classes_disposal": "Number of active classes at disposal",
        "active_classes_firstaction": "Number of active classes at first action",
        "am_stat": "AM status",
        "country_or_area_name": "Name of country or area",
        "ctry_nm": "Country name",
        "days_in_dock": "Number of days in dock",
        "disposal_dt": "Date of disposal",
        "disposal_pendency": "Pendency of disposal",
        "disposal_type": "Type of disposal",
        "fa_pendency_filter": "Flag for filtering first action pendency",
        "fa_pendency_fy": "Fiscal year of first action pendency",
        "fa_pendency_fy_month": "Month of fiscal year for first action pendency",
        "fa_pendency_fy_quarter": "Quarter of fiscal year for first action pendency",
        "filing_basis_grp": "Filing basis group",
        "filing_method_filed": "Filing method used",
        "first_action_type": "Type of first action",
        "last_modified_date": "Date of last modification",
        "law_office": "Law office",
        "max_action_dt": "Maximum action date",
        "noa_dt": "Notice of allowance date",
        "non_pro_se": "Indicator for non-pro se",
        "on_hold": "Flag for on hold status",
        "pctram_link": "PCT RAM link",
        "pendency_cal_end_dt": "End date of pendency calculation",
        "pendency_cal_start_dt": "Start date of pendency calculation",
        "pendency_category": "Category of pendency",
        "postal_cd": "Postal code",
        "registration_dt": "Registration date",
        "ser_num": "Serial number",
        "ste_ctry_cd": "State country code",
        "total_pendency_fy": "Fiscal year of total pendency",
        "total_pendency_fy_filter": "Flag for filtering total pendency",
        "total_pendency_fy_month": "Month of fiscal year for total pendency",
        "total_pendency_fy_quarter": "Quarter of fiscal year for total pendency",
        "total_pendency_fy_date": "Date of total pendency",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last updater",
        "output_record_count": "Count of output records"
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
        """) 
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "post_reg_dashboard"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to keep track of important milestones and statuses of trademark registrations after they have been officially registered. This table serves as a comprehensive resource for managing the post-registration lifecycle of trademarks, helping to ensure that they remain protected and in force by monitoring critical dates and compliance requirements.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "serial_number": "Serial number of the registration",
        "registration_dt": "Date of registration",
        "six_yr_dt": "Date of the six-year milestone",
        "last_10yr_dt": "Date of the last 10-year milestone",
        "next_10yr_renewal": "Next 10-year renewal information",
        "number_renewals": "Number of renewals",
        "next_6yr_dt": "Date of the next six-year milestone",
        "expiration_dt": "Date of expiration",
        "expiration_type": "Type of expiration",
        "registration_number": "Registration number",
        "am_dt_cncl": "Date of cancellation",
        "live_registration": "Indicator if the registration is live",
        "expiration_dt_realtime": "Real-time expiration date",
        "expiration_type_realtime": "Real-time expiration type",
        "live_reg": "Indicator if the registration is live",
        "exp_fy": "Fiscal year of expiration",
        "exp_fy_rt": "Fiscal year of real-time expiration",
        "reg_fy": "Fiscal year of registration",
        "today": "Current date",
        "today_fy": "Fiscal year of the current date",
        "fy_exp_diff": "Difference between fiscal year of expiration and current fiscal year",
        "fy_reg_diff": "Difference between fiscal year of registration and current fiscal year",
        "six_yr_fy": "Fiscal year of the six-year milestone",
        "ten_yr_fy": "Fiscal year of the ten-year milestone",
        "include_6yr_avg": "Flag to include six-year average",
        "include_10yr_avg": "Flag to include ten-year average",
        "max_today_fy": "Maximum fiscal year of the current date",
        "reg_age": "Age of the registration",
        "average_life_include": "Flag to include average life",
        "sixyr_num": "Numerator for six-year calculation",
        "sixyr_denom": "Denominator for six-year calculation",
        "tenyr_num": "Numerator for ten-year calculation",
        "tenyr_denom": "Denominator for ten-year calculation",
        "twentyyr_num": "Numerator for twenty-year calculation",
        "twentyyr_denom": "Denominator for twenty-year calculation",
        "thirtyyr_num": "Numerator for thirty-year calculation",
        "thirtyyr_denom": "Denominator for thirty-year calculation",
        "fortyyr_num": "Numerator for forty-year calculation",
        "fortyyr_denom": "Denominator for forty-year calculation",
        "fiftyyr_num": "Numerator for fifty-year calculation",
        "fiftyyr_denom": "Denominator for fifty-year calculation",
        "milestone": "Milestone indicator",
        "pendency_cal_start_dt": "Start date for pendency calculation",
        "non_pro_se": "Indicator if the case is non-pro se",
        "pctram_link": "Link to PCTRAM",
        "law_office": "Law office handling the case",
        "filing_basis_grp": "Group of filing basis",
        "filing_method_cur": "Current filing method",
        "am_stat": "Status code",
        "owner_name": "Name of the owner",
        "city": "City",
        "state": "State",
        "country_or_area_name": "Name of the country or area",
        "reg_class_count": "Count of registration classes",
        "active_class_count": "Count of active classes",
        "group_type": "Type of group",
        "concat_class": "Concatenated class information",
        "mark_nm_short": "Short name of the mark",
        "max_dt_filter": "Flag for maximum date filter",
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

table_name = "post_reg_dashboard_running"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to monitor and manage the ongoing status of trademark registrations. This table serves as a comprehensive dashboard for stakeholders to understand the current state of trademark registrations, ensuring they are up-to-date and identifying any necessary actions to maintain their active status.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "SERIAL_NUMBER": "Serial number of the registration",
        "MARK_NM_SHORT": "Short name of the mark",
        "Concat_Class": "Concatenated class information",
        "Group_Type": "Type of group",
        "Active_Class_Count": "Count of active classes",
        "Reg_Class_Count": "Count of registration classes",
        "Country_or_Area_Name": "Name of the country or area",
        "State": "State",
        "CITY": "City",
        "Owner_Name": "Name of the owner",
        "Continue_Process": "Indicator for continuing the process",
        "AM_STAT": "Status code",
        "FILING_BASIS_GRP": "Group of filing basis",
        "LAW_OFFICE": "Law office handling the case",
        "PCTRAM_LINK": "Link to PCTRAM",
        "NON_PRO_SE": "Indicator if the case is non-pro se",
        "Pendency_Cal_Start_DT": "Start date for pendency calculation",
        "SER_NUM": "Serial number",
        "Max_Dt_Filter": "Flag for maximum date filter",
        "LiveRegH_Count": "Count of live registrations",
        "LiveRegH_DT": "Date of live registration",
        "LiveRegH_Value": "Value of live registration",
        "LiveRegH_Name": "Name of live registration",
        "FILING_METHOD_CUR": "Current filing method",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last updater"
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column}  COMMENT '{comment}'
        """) 
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "post_reg_detail_dashboard"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a comprehensive resource designed to track detailed information about the post-registration activities and statuses of trademarks. This table serves as a vital tool for managing and analyzing the lifecycle of trademark registrations, ensuring compliance, and monitoring the efficiency of post-registration processes.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "recordid": "Unique identifier for the record",
        "serial_number": "Serial number of the registration",
        "registration_dt": "Date of registration",
        "registration_number": "Registration number",
        "postreg_category": "Category of post-registration action",
        "start_action_number": "Starting action number",
        "end_action_number": "Ending action number",
        "start_action_date": "Date of the start action",
        "end_action_date": "Date of the end action",
        "start_5_characters": "First 5 characters of the start action",
        "end_5_characters": "Last 5 characters of the end action",
        "start_cm_desc": "Description of the start action",
        "end_cm_desc": "Description of the end action",
        "fifteen_flag": "Flag indicating a specific condition",
        "inventory": "Indicator of inventory status",
        "first_action_date": "Date of the first action",
        "first_action_code": "Code of the first action",
        "renewal_dt": "Date of renewal",
        "renewal_number": "Renewal number",
        "first_action_pendency": "Pendency of the first action",
        "total_pendency": "Total pendency",
        "max_max_dt": "Maximum date",
        "expiration_type_realtime2": "Real-time expiration type",
        "expiration_dt_realtime2": "Real-time expiration date",
        "max_fy_ph": "Fiscal year placeholder",
        "sixyr_disposed_count": "Count of disposed cases in six years",
        "sixyr_base": "Base number for six-year calculation",
        "tenyr_disposed_count": "Count of disposed cases in ten years",
        "tenyr_base": "Base number for ten-year calculation",
        "end_action_fy": "Fiscal year of the end action",
        "ser_num": "Serial number",
        "pendency_cal_start_dt": "Start date for pendency calculation",
        "non_pro_se": "Indicator if the case is non-pro se",
        "pctram_link": "Link to PCTRAM",
        "law_office": "Law office handling the case",
        "filing_basis_grp": "Group of filing basis",
        "filing_method_cur": "Current filing method",
        "am_stat": "Status code",
        "owner_name": "Name of the owner",
        "city": "City",
        "state": "State",
        "country_or_area_name": "Name of the country or area",
        "reg_class_count": "Count of registration classes",
        "active_class_count": "Count of active classes",
        "group_type": "Type of group",
        "fa_percentile": "Percentile of the first action",
        "right_recordid": "Right-side unique identifier for the record",
        "fa_percentile_include": "Flag to include first action percentile",
        "tp_percentile": "Percentile of total pendency",
        "tp_percentile_include": "Flag to include total pendency percentile",
        "top10_fy_exclude_cfy": "Flag to exclude current fiscal year from top 10",
        "top5_fy_exclude_cfy": "Flag to exclude current fiscal year from top 5",
        "renewal_number_grp": "Group of renewal numbers",
        "category": "Category",
        "concat_class": "Concatenated class information",
        "first_action_inventory": "Indicator of first action inventory",
        "reg_fy": "Fiscal year of registration",
        "drop_off_year": "Indicator of drop-off year",
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

table_name = "post_reg_workforce"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to track and analyze workforce-related metrics and activities after the registration of trademarks. This table serves as a valuable resource for understanding the efficiency and trends in post-registration processes, helping to make informed decisions regarding workforce management and planning.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "Fiscal_Year": "The fiscal year for the data",
        "Date": "The date the data was recorded",
        "PostRegCat": "Category of post-registration activities",
        "Base_Total": "Total base number for calculations",
        "Avg_6YR_Rate": "Average rate over 6 years",
        "Avg_10YR_Rate": "Average rate over 10 years",
        "Actual_Estimated": "Indicator if the data is actual or estimated",
        "Continue_Process": "Flag indicating whether to continue the process",
        "create_ts": "Timestamp when the record was created",
        "create_user_id": "User ID of the creator",
        "update_ts": "Timestamp when the record was last updated",
        "update_user_id": "User ID of the last updater"
    }


    for column, comment in columns_comments.items():
        spark.sql(f"""
        ALTER TABLE {table_name} ALTER COLUMN {column}  COMMENT '{comment}'
        """) 
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "quality_dashboard"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a collection of data that provides insights into the quality of trademark cases. This table provides a comprehensive view of various aspects related to the quality of trademark cases, allowing for analysis and monitoring of the cases progress, compliance, and overall quality.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "law_office": "The name of the law office",
        "lastreviewdatetime": "The date of the last review",
        "searchsufficientindicator": "Indicator of whether the search is sufficient",
        "qualitymetricdeficientindicator": "Indicator of deficient quality metric",
        "mississueindicator": "Indicator of missing issue",
        "newissueindicator": "Indicator of new issue",
        "refusalunsoundindicator": "Indicator of unsound refusal",
        "substantivedeficientindicator": "Indicator of deficient substantive",
        "proceduraldeficientindicator": "Indicator of deficient procedural",
        "overalldeficientindicator": "Indicator of overall deficiency",
        "overallexcellentindicator": "Indicator of overall excellence",
        "evidencedeficientindicator": "Indicator of deficient evidence",
        "evidencesatisfactoryindicator": "Indicator of satisfactory evidence",
        "evidenceexcellentindicator": "Indicator of excellent evidence",
        "writingdeficientindicator": "Indicator of deficient writing",
        "writingsatisfactoryindicator": "Indicator of satisfactory writing",
        "writingexcellentindicator": "Indicator of excellent writing",
        "substantiveerrorindicator": "Indicator of substantive error",
        "satisfactoryindicator": "Indicator of satisfactory quality",
        "findingindicator": "Indicator of finding",
        "go_final": "The final status of the case",
        "quality_review_id": "The ID of the quality review",
        "review_type": "The type of review",
        "final_compliance": "Indicator of final compliance",
        "qualitymetricdeficientflag": "Flag indicating deficient quality metric",
        "excellentflag": "Flag indicating excellence",
        "max_date": "The maximum date",
        "fy_date_current": "The current fiscal year date",
        "current_fy": "The current fiscal year",
        "current_fy_int": "The current fiscal year as an integer",
        "fy_date": "The fiscal year date",
        "fy_date_string": "The fiscal year date as a string",
        "fy_month": "The fiscal year month",
        "fy_month_int": "The fiscal year month as an integer",
        "fy_quarter": "The fiscal year quarter",
        "first_action_type": "The type of first action",
        "disposal_type": "The type of disposal",
        "pendency_cal_start_dt": "The start date for pendency calculation",
        "pendency_cal_end_dt": "The end date for pendency calculation",
        "non_pro_se": "Indicator of non-pro se",
        "country_or_area_name": "The name of the country or area",
        "filing_basis_grp": "The filing basis group",
        "filing_method_filed": "The filing method filed",
        "ste_ctry_cd": "The country code",
        "concat_class": "The concatenated class",
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

table_name = "quality_dashboard_pivot"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is designed to provide a comprehensive overview of the quality of various cases managed by a law office. This table serves as a pivotal resource for analyzing case quality, tracking compliance, and understanding case outcomes over time, providing valuable insights for law offices and related entities.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "law_office": "The law office handling the case",
        "lastreviewdatetime": "The date of the last review",
        "go_final": "The final decision on the case",
        "review_type": "The type of review conducted",
        "final_compliance": "Indicates if the case complies with final requirements",
        "qualitymetricdeficientflag": "Flag indicating if there are deficiencies in quality metrics",
        "excellentflag": "Flag indicating if the case meets excellence criteria",
        "max_date": "The latest date in the dataset",
        "fy_date_current": "The current fiscal year date",
        "current_fy": "The current fiscal year",
        "current_fy_int": "The current fiscal year as an integer",
        "fy_date": "The fiscal year date",
        "fy_date_string": "The fiscal year date as a string",
        "fy_month": "The fiscal year month",
        "fy_month_int": "The fiscal year month as an integer",
        "fy_quarter": "The fiscal year quarter",
        "first_action_type": "The type of the first action taken on the case",
        "disposal_type": "The type of disposal for the case",
        "pendency_cal_start_dt": "The start date for pendency calculation",
        "pendency_cal_end_dt": "The end date for pendency calculation",
        "non_pro_se": "Indicates if the case is non-pro se",
        "country_or_area_name": "The name of the country or area related to the case",
        "filing_basis_grp": "The group of filing basis",
        "filing_method_filed": "The method used for filing",
        "ste_ctry_cd": "The country code",
        "concat_class": "Concatenated class information",
        "metric": "The metric being measured",
        "value": "The value of the metric",
        "case_count": "The count of cases",
        "category": "The category of the metric",
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

table_name = "tmns_notice_counts"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a detailed record that tracks various types of notices sent by a trademark management system. These notices can include alerts about case abandonment, allowances, reminders for legal filings, and updates on trademark application statuses, among others. Each row in the table represents counts of different types of notices sent within a specific time frame, such as a month or year. The table also includes information about the method of delivery (email or paper) for these notices, the total number of notices sent, and administrative details like when the record was created or last updated. This table serves as a comprehensive overview of communication activities related to trademark cases, helping to monitor and manage the workflow of trademark applications and registrations.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "date_time_range": "The range of dates for the notices",
        "abandonment_notice_appeal_terminated": "Count of abandonment notices where appeal was terminated",
        "corrected_notice_of_allowance_email": "Count of corrected notices of allowance sent via email",
        "corrected_notice_of_allowance_paper": "Count of corrected notices of allowance sent via paper",
        "courtesy_e_reminder_of_sec71_10_yr": "Count of courtesy email reminders for Section 71 (10 year)",
        "courtesy_e_reminder_of_sec71_6_yr": "Count of courtesy email reminders for Section 71 (6 year)",
        "courtesy_e_reminder_of_sec8_6_yr": "Count of courtesy email reminders for Section 8 (6 year)",
        "courtesy_e_reminder_of_sec8_sec9": "Count of courtesy email reminders for Section 8 and Section 9",
        "dsc_corr_project": "Count of DSC correction projects",
        "design_search_code_corr_project": "Count of design search code correction projects",
        "duplicate_notice_of_allowance_email": "Count of duplicate notices of allowance sent via email",
        "emails_for_notice_of_acceptance_and_acknowledgement_for_section_71_and_15": "Count of emails for notice of acceptance and acknowledgement for Section 71 and 15",
        "emails_for_notice_of_acceptance_for_section_71": "Count of emails for notice of acceptance for Section 71",
        "filing_receipt": "Count of filing receipts",
        "filing_receipt_email_trademark_application": "Count of filing receipt emails for trademark applications",
        "mails_trademark_registration_cancelled_in_part_sec_71": "Count of mails for trademark registration cancelled in part under Section 71",
        "notice_of_abandonment_ttab_ex_partes": "Count of notices of abandonment from TTAB ex partes",
        "notice_of_publication": "Count of notices of publication",
        "notice_termination": "Count of notices of termination",
        "notice_of_aau_acceptance": "Count of notices of AAU acceptance",
        "notice_of_abandonment": "Count of notices of abandonment",
        "notice_of_abandonment_after_inter_partes": "Count of notices of abandonment after inter partes",
        "notice_of_abandonment_after_pub": "Count of notices of abandonment after publication",
        "notice_of_abandonment_failure_to_file": "Count of notices of abandonment due to failure to file",
        "notice_of_abandonment_sou": "Count of notices of abandonment of SOU",
        "notice_of_abandonment_of_request_for_extension_of_protection": "Count of notices of abandonment of request for extension of protection",
        "notice_of_acceptance_sec_71_15": "Count of notices of acceptance under Section 71 and 15",
        "notice_of_acceptance_section_8_email": "Count of notices of acceptance for Section 8 sent via email",
        "notice_of_acceptance_section_8_paper": "Count of notices of acceptance for Section 8 sent via paper",
        "notice_of_acceptance_of_sou": "Count of notices of acceptance of SOU",
        "notice_of_acceptance_acknowledgement_sect_8_15_email": "Count of notices of acceptance and acknowledgement for Section 8 and 15 sent via email",
        "notice_of_acceptance_acknowledgement_sect_8_15_paper": "Count of notices of acceptance and acknowledgement for Section 8 and 15 sent via paper",
        "notice_of_acceptance_renewal_sect_8_9_email": "Count of notices of acceptance and renewal for Section 8 and 9 sent via email",
        "notice_of_acceptance_renewal_sect_8_9_paper": "Count of notices of acceptance and renewal for Section 8 and 9 sent via paper",
        "notice_of_acknowledgement_sect_15_email": "Count of notices of acknowledgement for Section 15 sent via email",
        "notice_of_allowance": "Count of notices of allowance",
        "notice_of_cancellation_full_email": "Count of full cancellation notices sent via email",
        "notice_of_cancellation_full_paper": "Count of full cancellation notices sent via paper",
        "notice_of_cancellation_partial_email": "Count of partial cancellation notices sent via email",
        "notice_of_cancellation_partial_paper": "Count of partial cancellation notices sent via paper",
        "notice_of_cancellation_sec71_email": "Count of cancellation notices under Section 71 sent via email",
        "notice_of_cancellation_sec8_email": "Count of cancellation notices under Section 8 sent via email",
        "notice_of_cancellation_sec8_paper": "Count of cancellation notices under Section 8 sent via paper",
        "notice_of_cancellation_of_registered_extension_of_protection": "Count of cancellation notices of registered extension of protection",
        "notice_of_design_search_code": "Count of notices of design search code",
        "notice_of_express_abandonment": "Count of notices of express abandonment",
        "notice_of_itu_extension_approval": "Count of notices of ITU extension approval",
        "notice_of_publication_12a_paper": "Count of notices of publication 12a sent via paper",
        "notice_of_publication_12c_email": "Count of notices of publication 12c sent via email",
        "notice_of_registration_email": "Count of notices of registration sent via email",
        "notice_of_renewal": "Count of notices of renewal",
        "notice_of_suspension_r_i": "Count of notices of suspension R&I",
        "notice_of_updated_registration_email": "Count of notices of updated registration sent via email",
        "notification_of_notice_of_publication_email": "Count of notifications of notice of publication sent via email",
        "notice_sec71_15_acpt": "Count of notices under Section 71 and 15 acceptance",
        "notice_of_acknowledgement_sect_15_paper": "Count of notices of acknowledgement for Section 15 sent via paper",
        "notice_of_publication_12a_email": "Count of notices of publication 12a sent via email",
        "month": "The month of the report",
        "report_type": "The type of report",
        "total_notices_sent_in_email": "Total number of notices sent via email",
        "total_notices_sent_in_letter": "Total number of notices sent via letter",
        "total_records": "Total number of records",
        "year": "The year of the report",
        "input_format": "The format of the input data",
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

table_name = "ttab_decision_rates"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a record that tracks the rates of decisions made by the Trademark Trial and Appeal Board (TTAB). This table contains information about the fiscal year in which the decision was made, the date when the case ended, the type of TTAB case, and the total number of decisions. It also includes the number of decisions made by judges specifically. The table helps to analyze and understand the rate at which decisions are being made by the TTAB, providing insights into the efficiency and workload of the board.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "fiscal_year": "The fiscal year of the decision",
        "case_end_dt": "The date when the case ended",
        "ttab_case_type": "The type of the TTAB case",
        "total_decisions": "The total number of decisions",
        "total_judge_decisions": "The total number of decisions made by judges",
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

table_name = "ttab_detail"
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a record that contains detailed information about trademark cases handled by the Trademark Trial and Appeal Board (TTAB). This table includes various columns that provide information about the case, such as the serial number of the trademark application, the type of issue being addressed in the TTAB proceeding, the filing date of the application, and the dates related to the proceeding, such as the date it was instituted and the date of the decision. Overall, this table provides a comprehensive view of the various aspects and stages of trademark cases handled by the TTAB, allowing for analysis and understanding of the workflow, status, and characteristics of these cases.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "serial_number": "Unique identifier for the trademark application",
        "ttab_issue_type": "Type of issue being addressed in the TTAB proceeding",
        "proceeding_num": "Unique identifier for the TTAB proceeding",
        "filing_date": "Date the trademark application was filed",
        "instituted_date": "Date the TTAB proceeding was instituted",
        "instituted_code": "Code indicating the reason the proceeding was instituted",
        "decision_date": "Date the decision was made on the proceeding",
        "decision_code": "Code representing the type of decision made",
        "decision_description": "Description of the decision made",
        "termination_code": "Code indicating the reason for termination of the proceeding",
        "termination_date": "Date the proceeding was terminated",
        "final_refusal_date": "Date of final refusal, if applicable",
        "fp_reason_1": "First reason for final refusal",
        "pendency_d": "Pendency days count",
        "inventory": "Indicates if the case is part of the inventory",
        "non_pro_se": "Indicates if the case is non-pro se",
        "pctram_link": "Link to PCTRAM record",
        "law_office": "Law office handling the case",
        "filing_basis_grp": "Group of filing basis",
        "filing_method_cur": "Current filing method",
        "am_stat": "Amendment status",
        "owner_name": "Name of the trademark owner",
        "city": "City of the trademark owner",
        "state": "State of the trademark owner",
        "country_or_area_name": "Country or area of the trademark owner",
        "reg_class_count": "Count of registered classes",
        "active_class_count": "Count of active classes",
        "group_type": "Type of group",
        "concat_class": "Concatenated class information",
        "mark_nm_short": "Short name of the mark",
        "refusal": "Indicates if there was a refusal",
        "appeal": "Indicates if there was an appeal",
        "publication_date": "Date of publication",
        "pubs": "Indicates if published",
        "opposition": "Indicates if there was an opposition",
        "default_opposition": "Indicates if there was a default opposition",
        "default_cancellation": "Indicates if there was a default cancellation",
        "cancellation": "Indicates if there was a cancellation",
        "constructed_prcd_num": "Constructed proceeding number",
        "default_date": "Date of default",
        "cancellation_count": "Count of cancellations",
        "reg_yr": "Registration year",
        "live_reg_count": "Count of live registrations",
        "can_rate": "Cancellation rate",
        "concurrent": "Indicates if concurrent",
        "rfd_date": "Date of refusal",
        "rfd_valid": "Indicates if the refusal date is valid",
        "proceeding_count": "Count of proceedings",
        "case_age_rfd": "Age of the case at refusal date",
        "case_age_category": "Category of case age",
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

table_name = "ttab_workloads "
if tableExists(table_name):
    alter_table_query = f"""
        ALTER TABLE  {table_name}
        SET TBLPROPERTIES ('comment' = 'This {table_name} table is a structured collection of data that tracks and analyzes the workload associated with various cases handled by the Trademark Trial and Appeal Board (TTAB). This table serves as a crucial tool for monitoring the volume and types of cases processed by the TTAB, helping in resource planning, trend analysis, and making informed decisions regarding case management.' )"""
    spark.sql(alter_table_query)

    # List of columns and their suggested comments
    columns_comments = {
        "fiscal_year": "The fiscal year for the data",
        "date": "The date of the record",
        "ttab_case_type": "The type of TTAB case",
        "day_total": "Total cases or actions for the day",
        "actual_estimated": "Indicates whether the data is actual or estimated",
        "fy_base_total": "The base total for the fiscal year",
        "fy_judge_decisions": "Number of judge decisions in the fiscal year",
        "fy_jdr": "Judge decision rate for the fiscal year",
        "latest_5yr_avg_jdr": "Average judge decision rate over the last 5 years",
        "raw_credits": "Raw credits earned",
        "credits_jdr_applied": "Credits applied towards the judge decision rate",
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

table_name = "post_reg_dashboard"
if tableExists(table_name):
    columns_comments = {
        "serial_number": "Unique identifier assigned to the trademark registration.",
        "registration_dt": "Date when the registration was recorded.",
        "six_yr_dt": "Represents the date when six years have passed.",
        "last_10yr_dt": "Date of the last 10-year milestone achieved.",
        "next_10yr_renewal": "Date for the next 10-year renewal period.",
        "number_renewals": "Total count of times a trademark has been renewed.",
        "next_6yr_dt": "Date of the next six-year milestone from the current date.",
        "expiration_dt": "Represents the date when the item or record is set to expire.",
        "expiration_type": "Indicates the type of expiration, such as date-based or event-based, that applies to the associated entity.",
        "registration_number": "Unique identifier assigned to each registration.",
        "am_dt_cncl": "Date of cancellation for the agreement or contract.",
        "live_registration": "Indicates whether the registration is currently live.",
        "expiration_dt_realtime": "Date and time when the trademark is set to expire.  ",
        "expiration_type_realtime": "Indicates the type of real-time expiration, such as immediate or scheduled, for time-sensitive data or events.",
        "live_reg": "Indicates whether the registration is currently live.",
        "exp_fy": "Fiscal year in which the item or agreement is set to expire.",
        "exp_fy_rt": "Fiscal year of real-time expiration.",
        "reg_fy": "Fiscal year in which the registration occurred.",
        "today": "The current date (when the ETL was run for this table).",
        "today_fy": "The fiscal year corresponding current date (when the ETL was run for this table).",
        "fy_exp_diff": "Difference between the fiscal year of expiration and the current fiscal year.",
        "fy_reg_diff": "Represents the difference between the fiscal year of registration and the current fiscal year.",
        "six_yr_fy": "Represents the fiscal year in which a six-year milestone occurs.",
        "ten_yr_fy": "Represents the fiscal year in which a ten-year milestone occurs.",
        "include_6yr_avg": "Indicates whether to include the six-year average in the calculation.",
        "include_10yr_avg": "Indicates whether to include the ten-year average in the calculation.",
        "max_today_fy": "Maximum fiscal year based on the current date.",
        "reg_age": "The reg_age column represents the age of the registration in years.",
        "average_life_include": "Indicates whether to include the average life in the calculation.",
        "sixyr_num": "Numerator used in the six-year calculation. Used for reporting purposes.",
        "sixyr_denom": "Denominator used for calculating six-year rates and percentages. Used for reporting purposes.",
        "tenyr_num": "Numerator used in calculating the ten-year value. Used for reporting purposes.",
        "tenyr_denom": "Denominator used in calculating ten-year values. Used for reporting purposes.",
        "twentyyr_num": "Numerator used in the twenty-year calculation. Used for reporting purposes.",
        "twentyyr_denom": "Denominator used for calculating twenty-year values. Used for reporting purposes.",
        "thirtyyr_num": "Numerator used in the calculation of thirty-year values. Used for reporting purposes.",
        "thirtyyr_denom": "Denominator used for calculating thirty-year values. Used for reporting purposes.",
        "fortyyr_num": "Numerator used in the forty-year calculation. Used for reporting purposes.",
        "fortyyr_denom": "Denominator used in calculating forty-year metrics. Used for reporting purposes.",
        "fiftyyr_num": "Numerator used in the fifty-year calculation. Used for reporting purposes.",
        "fiftyyr_denom": "Denominator used in calculating fifty-year values.",
        "milestone": "Indicates a significant event or achievement in a project or process.",
        "pendency_cal_start_dt": "Date when pendency calculation begins.",
        "non_pro_se": "Indicates whether the case involves a self-represented party or not.",
        "pctram_link": "This column contains the link to the PCTRAM resource for further information.",
        "law_office": "This column represents the law office responsible for handling the case.",
        "filing_basis_grp": "This column represents the group of filing basis used for reporting purposes.",
        "filing_method_cur": "Current filing method associated with the trademark.",
        "am_stat": "The status code derived from TRAM indicating the state of the trademark case.",
        "owner_name": "The owner_name column stores the name of the entity that owns the record.",
        "city": "The city of the trademark owner for this registration.",
        "state": "The state of the trademark owner.",
        "country_or_area_name": "The country or region name of the trademarks owner.",
        "reg_class_count": "Number of distinct registration classes.",
        "active_class_count": "Represents the total number of active classes.",
        "group_type": "Indicates the category or classification of the group, such as public, private, or restricted.",
        "concat_class": "Stores concatenated class information for easy reference.",
        "mark_nm_short": "Abbreviated name associated with the mark.",
        "max_dt_filter": "Indicates whether to apply a maximum date filter to the data. Used for reporting purposes.",
        "create_ts": "Stores the timestamp when the record was created.",
        "create_user_id": "Unique identifier of the user or system created the record.",
        "update_ts": "Stores the timestamp of the records last update.",
        "update_user_id": "Stores the ID of the user or system last updated the record.",
    }

for column, comment in columns_comments.items():
    try:
        print(f"Executing ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
        spark.sql(
            f"""
            ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
            """
        )
    except:
        print(f"Could not execute: ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "post_reg_dashboard_running"
if tableExists(table_name):
    columns_comments = {
        "SERIAL_NUMBER": "Unique identifier assigned to the trademark.",
        "MARK_NM_SHORT": "This column stores the abbreviated name of the mark.",
        "Concat_Class": "Concatenated class information combining multiple class details into a single output.",
        "Group_Type": "Indicates the group of the trademark.",
        "Active_Class_Count": "Number of classes that are currently active for the trademark.",
        "Reg_Class_Count": "Number of distinct classes at the time of registration.",
        "Country_or_Area_Name": "The country or region name of the trademarks owner.",
        "State": "The state of the trademark owner for this registration.",
        "CITY": "The city of the trademark owner for this registration.",
        "Owner_Name": "The name of the trademark owner.",
        "Continue_Process": "Indicates whether the process should continue. This is used for ETL purposes.",
        "AM_STAT": "Status code derived from TRAM indicating the current state of the record.",
        "FILING_BASIS_GRP": "Identifies the group of filing basis.",
        "LAW_OFFICE": "This column identifies the law office responsible for handling the case.",
        "PCTRAM_LINK": "Contains the URL linking to the PCTRAM resource for further information.",
        "NON_PRO_SE": "Indicator of whether the case involves a non-pro se party, meaning a party is represented by an attorney.",
        "Pendency_Cal_Start_DT": "Date when pendency calculation begins.",
        "SER_NUM": "Unique identifier assigned to each trademark.",
        "Max_Dt_Filter": "Indicates whether to apply a maximum date filter.  Used for reporting purposes.",
        "LiveRegH_Count": "Represents the total number of active registrations.",
        "LiveRegH_DT": "Date of live registration for the donor.",
        "LiveRegH_Value": "Represents the numerical value associated with a live registration.",
        "LiveRegH_Name": "The name associated with a live registration.",
        "FILING_METHOD_CUR": "Current filing method in use.",
        "create_ts": "Stores the timestamp when the record was created.",
        "create_user_id": "Unique identifier of the user who created the record.",
        "update_ts": "Stores the timestamp of the records last update.",
        "update_user_id": "Stores the ID of the user or system last updated the record.",
        "recordid": "Unique identifier for the record. Used for ETL purposes.",
    }

    for column, comment in columns_comments.items():
        try:
            print(f"Executing ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Could not execute: ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
    else:
        print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "post_reg_detail_dashboard"
if tableExists(table_name):
    columns_comments = {
        "serial_number": "Unique identifier assigned to the registration.",
        "registration_dt": "Date when the registration was recorded.",
        "registration_number": "Unique identifier assigned to a registered trademark.",
        "postreg_category": "Category indicating the type of action taken after initial registration.",
        "start_action_number": "Indicates the starting point for action numbering.",
        "end_action_number": "Identifies the final action number in a sequence of actions.",
        "start_action_date": "Represents the date when the action is initiated.",
        "end_action_date": "Represents the date when the action is completed or concluded.",
        "start_5_characters": "The 5 character prosecution history event code for the first action.",
        "end_5_characters": "The 5 character prosecution history event code for the last action.",
        "start_cm_desc": "The full prosecution history event description for the first action.",
        "end_cm_desc": "The full prosecution history prosecution history event code for the last action.",
        "fifteen_flag": "Indicates a specific condition when set to a non-zero value.",
        "inventory": "Tracks the current status of inventory levels.",
        "first_action_date": "Records the date of the initial action taken.",
        "first_action_code": "Unique identifier for the initial action taken.",
        "renewal_dt": "Date when the renewal is scheduled to occur or occured.",
        "renewal_number": "Unique number of renewals for the given trademark.",
        "first_action_pendency": "The first_action_pendency column represents the time elapsed until the initial action is taken.",
        "total_pendency": "Total pendency represents the cumulative amount of time that cases have been pending.",
        "max_max_dt": "Maximum date stored in the dataset.",
        "expiration_type_realtime2": "Specifies the real-time expiration type for automatic data removal.",
        "expiration_dt_realtime2": "The expiration_dt_realtime2 column stores the real-time expiration date of a record.",
        "max_fy_ph": "The latest fiscal year of the prosecution history date.",
        "sixyr_disposed_count": "Number of cases disposed within a six-year timeframe. Used for reporting purposes.",
        "sixyr_base": "Base number used for six-year calculations. Used for reporting purposes.",
        "tenyr_disposed_count": "Number of cases disposed within the last ten years. Used for reporting purposes.",
        "tenyr_base": "Base number used for ten-year calculations. Used for reporting purposes.",
        "end_action_fy": "Represents the fiscal year in which the end action occurred.",
        "ser_num": "Unique identifier assigned to a trademark.",
        "pendency_cal_start_dt": "Date when pendency calculation begins.",
        "non_pro_se": "Indicates whether the case involves a self-represented party or not.",
        "pctram_link": "This column contains a link to the PCTRAM resource for further information.",
        "law_office": "This column represents the law office responsible for handling the case.",
        "filing_basis_grp": "This column represents the group of filing basis.",
        "filing_method_cur": "Current filing method used for submissions.",
        "am_stat": "Status code representing the current state of the trademark application. Derived from legacy TRAM codes.",
        "owner_name": "The name of the trademark owner.",
        "city": "The city of the trademark owner for this registration.",
        "state": "The state of the trademark owner.",
        "country_or_area_name": "This column contains the name of the country or geographic area of the trademark owner.",
        "reg_class_count": "Number of distinct classes at the time of registration.",
        "active_class_count": "Represents the total number of active classes.",
        "group_type": "Indicates the group type of the registration.",
        "fa_percentile": "Represents the percentile ranking of the first action taken.",
        "right_recordid": "Unique identifier for the record, used during the ETL for joining.",
        "fa_percentile_include": "Indicates whether to include the first action percentile.  Used for reporting purposes.",
        "tp_percentile": "Represents the percentile of total pendency.",
        "tp_percentile_include": "Indicates whether to include the total pendency percentile in the calculation. Used for reporting purposes.",
        "top10_fy_exclude_cfy": "Indicates the top 10 fiscal year registrations, excluding the current fiscal year.  Used for reporting purposes.",
        "top5_fy_exclude_cfy": "Indicates the top 5 fiscal year registrations, excluding the current fiscal year.  Used for reporting purposes.",
        "renewal_number_grp": "Represents a group of renewal numbers associated with a specific entity or policy.",
        "category": "Displays the category or classification of the item or topic being discussed.",
        "concat_class": "Stores concatenated class information for easy reference.",
        "first_action_inventory": "Indicates the inventory when the first action was taken.",
        "reg_fy": "Fiscal year in which the registration occurred.",
        "drop_off_year": "Indicates the year in which the registration dropped.",
        "create_ts": "Stores the timestamp when the record was created.",
        "create_user_id": "Unique identifier of the user or system that created the record.",
        "update_ts": "Stores the timestamp of the records last update.",
        "update_user_id": "Stores the ID of the user or system that last updated the record.",
    }

    for column, comment in columns_comments.items():
        try:
            print(f"Executing ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
            """
            )
        except:
            print(f"Could not execute: ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "post_reg_workforce"

if tableExists(table_name):
    columns_comments = {
        "Fiscal_Year": "Represents the fiscal year associated with the data.",
        "Date": "Represents the date the data was collected.",
        "PostRegCat": "Category of activities that occur after registration, such as maintenance and renewal.",
        "Base_Total": "Represents the total base number used for calculations.",
        "Avg_6YR_Rate": "Average annual rate over a 6-year period.",
        "Avg_10YR_Rate": "Average annual rate over the past 10 years.",
        "Actual_Estimated": "Indicates whether the data is actual or estimated.",
        "Continue_Process": "Indicates whether the process should continue.",
        "create_ts": "Stores the timestamp when the record was created.",
        "create_user_id": "Unique identifier of the user or system that created the record.",
        "update_ts": "Stores the timestamp of the records last update.",
        "update_user_id": "Stores the ID of the user or system that last updated the record.",
    }

    for column, comment in columns_comments.items():
        try:
            print(f"Executing ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Could not execute: ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
    else:
        print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "quality_dashboard"
if tableExists(table_name):
    columns_comments = {
        "law_office": "This column stores the name of the law office number.",
        "lastreviewdatetime": "Stores the date and time of the most recent review.",
        "searchsufficientindicator": "Indicates whether the search results are sufficient to proceed with the next steps.",
        "qualitymetricdeficientindicator": "Indicator signaling that a quality metric has not met the required standards.",
        "mississueindicator": "Indicates whether an issue is missing from the expected sequence.",
        "newissueindicator": "Indicates whether a security is a new issue, providing a flag for recently released securities.",
        "refusalunsoundindicator": "Indicator signaling that a refusal is deemed unsound.",
        "substantivedeficientindicator": "This column indicates whether a substantive is deficient, providing a clear signal for further review or action.",
        "proceduraldeficientindicator": "Indicator of whether a procedural deficiency has been identified.",
        "overalldeficientindicator": "Indicates whether there is an overall deficiency in the quality of the case.",
        "overallexcellentindicator": "Measures overall achievement and excellence in a concise and comprehensive manner.",
        "evidencedeficientindicator": "Indicator of insufficient or lacking evidence.",
        "evidencesatisfactoryindicator": "This indicator signifies that the provided evidence is sufficient to support the claim or decision.",
        "evidenceexcellentindicator": "This column denotes a high-quality indicator that signifies the presence of excellent evidence.",
        "writingdeficientindicator": "This column identifies potential writing deficiencies in submissions.",
        "writingsatisfactoryindicator": "This column tracks whether the writing meets the required standards and expectations.",
        "writingexcellentindicator": "A well-structured and engaging piece of writing is an excellent indicator of effective communication and strong writing skills.",
        "substantiveerrorindicator": "Indicates whether a response contains a substantive error that affects the accuracy of the data.",
        "satisfactoryindicator": "Indicates whether the quality of the data is satisfactory.",
        "findingindicator": "This column serves as an indicator to highlight notable findings.",
        "go_final": "Indicates the final disposition or outcome of the cases quality review.",
        "quality_review_id": "Unique identifier for the quality review.",
        "review_type": "This field indicates the category of review, such as editorial, peer, or automated.",
        "final_compliance": "Indicates whether all compliance requirements have been fully met.",
        "qualitymetricdeficientflag": "Indicates whether a quality metric is deficient, requiring attention or improvement.",
        "excellentflag": "Indicates a high level of achievement or quality, signifying something as exceptionally good or outstanding.",
        "max_date": "The latest date of the quality review for that case.  Used for reporting purposes.",
        "fy_date_current": "Represents the date in the current fiscal year.",
        "current_fy": "The current fiscal year being reported.",
        "current_fy_int": "The current fiscal year represented as an integer value.",
        "fy_date": "Represents the date in the organizations fiscal year.",
        "fy_date_string": "Fiscal year date in string format.",
        "fy_month": "The fiscal year month represents the month of the year for financial reporting purposes.",
        "fy_month_int": "Fiscal year month represented as an integer value.",
        "fy_quarter": "The fiscal year quarter represents the quarter of the fiscal year, typically divided into four quarters.",
        "first_action_type": "Indicates the category of the initial action taken.",
        "disposal_type": "Indicates the method or category of waste disposal, such as recycling, landfill, or incineration.",
        "pendency_cal_start_dt": "Date when pendency calculation begins.",
        "pendency_cal_end_dt": "Date marking the end of the pendency calculation period.",
        "non_pro_se": "This column indicates whether a party is represented by an attorney or is proceeding without professional legal representation.",
        "country_or_area_name": "This column contains the full name of the country or geographic area being referenced.",
        "filing_basis_grp": "This column represents the group classification for filing basis.",
        "filing_method_filed": "Indicates the method by which the filing was submitted, such as electronically or by mail.",
        "ste_ctry_cd": "Represents the standard two-letter code for the country of origin.",
        "concat_class": "Stores the combined class labels from multiple classes.",
        "create_ts": "Stores the timestamp when the record was created.",
        "create_user_id": "Unique identifier of the user or system that created the record.",
        "update_ts": "Stores the timestamp of the records last update.",
        "update_user_id": "Stores the ID of the user who last updated the record.",
    }
    for column, comment in columns_comments.items():
        try:
            print(f"Executing ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Could not execute: ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
    else:
        print(f"table {table_name} does not exist")

# COMMAND ----------

table_name = "quality_dashboard_pivot"
if tableExists(table_name):
    columns_comments = {
        "law_office": "This column represents the law office responsible for handling the case.",
        "lastreviewdatetime": "Stores the date and time of the most recent review.",
        "go_final": "The go_final column represents the final decision made in the case.",
        "review_type": "This field indicates the type of review that was conducted during the quality examination.",
        "final_compliance": "Indicates whether the case meets all final requirements.",
        "qualitymetricdeficientflag": "Indicates whether quality metric deficiencies exist, flagged as true if any metrics are deficient and false otherwise.",
        "excellentflag": "Indicates whether the case meets excellence criteria.",
        "max_date": "Represents the latest date of the review for that case.",
        "fy_date_current": "Represents the date in the current fiscal year.",
        "current_fy": "The current fiscal year being reported.",
        "current_fy_int": "The current fiscal year represented as an integer value.",
        "fy_date": "Represents the date in the companys fiscal year.",
        "fy_date_string": "Fiscal year date in string format.",
        "fy_month": "The fiscal year month represents the month of the year for financial reporting purposes.",
        "fy_month_int": "Fiscal year month represented as an integer value.",
        "fy_quarter": "The fiscal year quarter represents the quarter of the fiscal year, typically divided into four quarters.",
        "first_action_type": "Indicates the type of the initial action taken on a case.",
        "disposal_type": "Indicates the method or category of disposal used to resolve the case.",
        "pendency_cal_start_dt": "Date when pendency calculation begins.",
        "pendency_cal_end_dt": "Date marking the end of the pendency calculation period.",
        "non_pro_se": "Indicates whether the case involves a non-pro se party, meaning a party is represented by an attorney.",
        "country_or_area_name": "This column contains the name of the country or geographic area associated with the trademark owner.",
        "filing_basis_grp": "This column represents the category or group of filing basis used for the trademark.",
        "filing_method_filed": "Indicates the method by which a filing was submitted.",
        "ste_ctry_cd": "Represents the standard two-letter code for the country of origin.",
        "concat_class": "Stores concatenated class information for easy reference.",
        "metric": "This column displays the specific metric being measured.",
        "value": "This column represents the quantitative value of the metric being measured.",
        "case_count": "Represents the total number of cases. Used for reporting purposes.",
        "category": "This field specifies the category that the metric belongs to.",
        "create_ts": "Stores the timestamp when the record was created.",
        "create_user_id": "Unique identifier of the user or system that created the record.",
        "update_ts": "The timestamp of the records last update.",
        "update_user_id": "The ID of the user who last updated the record.",
    }
    for column, comment in columns_comments.items():
        try:
            print(f"Executing ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Could not execute: ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")
else:
    print(f"table {table_name} does not exist")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 's3://bdr-databricks-app-dev/eds/delta_tables/trm_reporting_dev/gold/'

# COMMAND ----------

table_name = "form_paragraph_dashboard"
columns_comments = {
    "action_type": "Indicates the specific type of action that was performed in relation to the form paragraph",
    "category": "Specifies the category to which the form paragraph belongs",
    "group_name": "Provides the name of the group that is linked to the form paragraph",
    "create_user_id": "Contains the unique identifier of the user or system that created the record",
    "first_action_count_numerator": "Represents the numerator used in calculating the count of the first type of action taken. Used for downstream reporting.",
    "action_type_2_possible_fix": "Suggests a potential solution or correction for the second type of action taken. Used as an intermediate fix in the respective ETL.",
    "law_office": "Identifies the law office that is connected to the form paragraph action",
    "create_ts": "Records the exact date and time when the record was initially created",
    "transaction_number": "Contains a unique number that is linked to the specific transaction",
    "completed_date_year": "Indicates the year in which the form paragraph was finalized",
    "form_paragraph_id": "Provides a unique identifier that distinguishes each form paragraph",
    "completed_date": "Shows the specific date on which the form paragraph was completed",
    "state_cd": "Contains the code representing the state that is associated with the form paragraph",
    "completed_date_fiscal_year": "Indicates the fiscal year during which the form paragraph was completed",
    "tm_analytics_ts": "Records the date and time relevant to system reporting load. Legacy naming for timestamps used by TM Data Analytics for downstream reporting and troubleshooting.",
    "concat_category": "Contains category information that has been combined into a single string",
    "update_user_id": "Contains the unique identifier of the user or system that last updated the record",
    "serial_number": "Provides the serial number that is linked to the form paragraph. This should serve as the primary key for cases.",
    "filing_basis_group": "Specifies the basis or reason for the group under which the filing was made",
    "data_through_date": "Indicates the most recent date for which data is included in the record",
    "action_count": "Shows the total number of actions that are linked to the form paragraph",
    "generated_date": "Records the date on which the data was generated",
    "update_ts": "Records the exact date and time when the record was most recently updated",
    "transaction_literal": "Provides a textual description of the transaction",
    "foreign_key_form_paragraph_group_id": "Contains a foreign key that links the record to the group associated with the form paragraph",
    "exam": "Provides information about the status or details of the examination process",
    "country_or_area_name": "Specifies the name of the country or area that is linked to owner of the case that the form paragraph association is tied to",
    "foreign_key_form_paragraph_category_id": "Contains a foreign key that links the record to the category of the form paragraph",
    "first_action_count_denominator": "Represents the denominator used in calculating the count of the first type of action taken.  Used for downstream reporting.",
    "last_modified_date": "Shows the most recent date on which the form paragraph was modified",
    "toc_link": "Provides a hyperlink to the table of contents relevant to the form paragraph",
    "grade": "Indicates the grade or rating that has been assigned to the form paragraph",
    "title_text": "Contains the title or heading of the form paragraph",
    "concat_form_paragraph_id": "Provides an identifier for the form paragraph that is created by combining multiple fields",
    "form_paragraph_year": "Indicates the year that is relevant to the form paragraph",
}

if tableExists(table_name):
    for column, comment in columns_comments.items():
        alter_query = f"""ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'"""
        print(alter_query)
        try:
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Unable to update table comment for `{table_name}`")

# COMMAND ----------

table_name = "goods_services_dashboard"

columns_comments = {
    "update_user_id": "Contains the unique identifier of the user that most recently updated the record",
    "filing_basis_grp": "Specifies the filing group basis of the trademark",
    "filing_fy_month": "Indicates the month in which the filing occurred",
    "update_ts": "Records the exact date and time when the record was most recently updated",
    "filing_fy_quarter": "Indicates the fiscal quarter during which the filing was made",
    "max_pendency_cal_start_dt": "Provides the latest possible start date used in calculating pendency",
    "filing_method_filed": "Specifies the method or process that was used to file the form paragraph",
    "country_or_area_name": "Provides the name of the country or area with respect to the owner of the trademark",
    "class_count": "Shows the total number of classes that were filed in relation to the form paragraph",
    "create_ts": "Records the exact date and time when the record was initially created",
    "applicant_bin": "Indicates the bin of the applicant. This is a downstream, generated bin based on filing history",
    "non_pro_se": "Indicates whether the filing was made without the assistance of a professional legal representative (pro se filing)",
    "coordinated_class": "Provides the class associated with the filing",
    "filing_fy": "Indicates the fiscal year in which the filing was made",
    "goods_services_desc": "Contains a description of the goods or services associated with the filing",
    "pendency_cal_start_dt": "Provides the date from which pendency is calculated",
    "ser_num": "Contains the serial number that is assigned to the filing. This should serve as the primary key.",
    "entity_type": "Specifies the type of entity (such as individu corporati etc.) that is making the filing",
    "ste_ctry_cd": "Contains the code representing the state or country relevant to the filing",
    "class": "Provides the class code assigned to the filing",
    "goods_or_services": "Lists the goods or services that are linked to the filing",
    "create_user_id": "Contains the unique identifier of the user who created the record",
}

if tableExists(table_name):
    for column, comment in columns_comments.items():
        alter_query = f"""ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'"""
        print(alter_query)
        try:
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Unable to update table comment for `{table_name}`")

# COMMAND ----------

table_name = "pendency_dashboard"

columns_comments = {
    "pendency_category": "Specifies the category under which pendency is measured",
    "total_pendency_fy_month": "Indicates the month within the fiscal year that is relevant for total pendency calculations",
    "update_ts": "Records the exact date and time when the record was most recently updated",
    "first_action_type_num": "Contains a numeric code that represents the type of the first action taken",
    "update_user_id": "Contains the unique identifier of the user who most recently updated the record",
    "filing_basis_grp": "Specifies the group basis or reason for which the filing was made",
    "non_pro_se": "Indicates whether the filing was made with professional legal representation (non-pro se)",
    "ste_ctry_cd": "Contains the code representing the state or country relevant to the filing",
    "ctry_nm": "Provides the name of the country relevant to the record",
    "total_pendency_fy_quarter": "Indicates the quarter within the fiscal year that is relevant for total pendency calculations",
    "country_or_area_name": "Specifies the name of the country or area with respect to the owner who is filing",
    "am_stat": "Provides the current status of the trademark derived from TRAM legacy status codes",
    "first_action_pendency_ph": "Shows the amount of ti measured in hou that elapsed before the first action was taken",
    "law_office": "Identifies the law office associated with the record",
    "disposal_pendency": "Indicates the amount of time that elapsed before the disposal of the form paragraph",
    "pctram_link": "Provides a hyperlink to the TM Review site that contains additional details about the filing",
    "abandonment_dt": "Shows the date on which the form paragraph or filing was abandoned",
    "active_classes_firstaction": "Indicates the number of active classes present at the time of the first action",
    "first_action_dt_ph": "Records the date on which the first action was taken",
    "max_action_dt": "Provides the date of the latest action that occurred",
    "first_action_type": "Specifies the type of the first action that was taken",
    "filing_method_filed": "Indicates the method or process that was used to file the form paragraph",
    "active_classes_disposal": "Shows the number of active classes present at the time of disposal",
    "fa_pendency_fy_month": "Indicates the month within the fiscal year that is relevant for first action pendency calculations",
    "postal_cd": "Contains the postal code relevant to the record",
    "ser_num": "Provides the serial number associated with the filing. This should serve as the primary key.",
    "fa_pendency_fy_quarter": "Indicates the quarter within the fiscal year that is relevant for first action pendency calculations",
    "total_pendency_fy": "Specifies the fiscal year relevant to the calculation of total pendency",
    "disposal_dt": "Records the date on which the form paragraph or filing was disposed",
    "noa_dt": "Shows the date on which a notice of allowance was issued",
    "on_hold": "Indicates whether the record is currently on hold",
    "pendency_cal_end_dt": "Provides the date on which the pendency calculation ends",
    "disposal_type": "Specifies the type of disposal that occurred for the form paragraph or filing",
    "last_modified_date": "Records the most recent date on which the record was modified",
    "total_pendency_fy_filter": "Indicates whether the record should be included when filtering for total pendency",
    "create_ts": "Records the exact date and time when the record was initially created",
    "days_in_dock": "Shows the total number of days the record spent in the dock (a holding or review area)",
    "registration_dt": "Provides the date on which the form paragraph or filing was registered",
    "create_user_id": "Contains the unique identifier of the user or system that created the record",
    "pendency_cal_start_dt": "Provides the date from which the pendency calculation begins",
    "fa_pendency_filter": "Indicates whether the record should be included when filtering for first action pendency",
    "total_pendency_fy_date": "Records the date relevant to the calculation of total pendency",
    "fa_pendency_fy": "Specifies the fiscal year relevant to the calculation of first action pendency",
    "output_record_count": "Shows the total number of output records generated by the query. This is used for downstream reporting purposes.",
}

if tableExists(table_name):
    for column, comment in columns_comments.items():
        alter_query = f"""ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'"""
        print(alter_query)
        try:
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Unable to update table comment for `{table_name}`")

# COMMAND ----------

table_name = "ttab_decision_rates"

columns_comments = {
    "fiscal_year": "The fiscal year of the decision",
    "case_end_dt": "The date when the case ended",
    "ttab_case_type": "The type of the TTAB case",
    "total_decisions": "The total number of decisions",
    "total_judge_decisions": "The total number of decisions made by judges",
    "create_ts": "Timestamp when the record was created",
    "create_user_id": "User ID of the creator",
    "update_ts": "Timestamp when the record was last updated",
    "update_user_id": "User ID of the last updater",
}

if tableExists(table_name):
    for column, comment in columns_comments.items():
        alter_query = (
            f"""ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'"""
        )
        print(alter_query)
        try:
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Unable to update table comment for `{table_name}`")

# COMMAND ----------

table_name = "ttab_detail"

columns_comments = {
    "serial_number": "Unique identifier for the trademark application",
    "ttab_issue_type": "Type of issue being addressed in the TTAB proceeding",
    "proceeding_num": "Unique identifier for the TTAB proceeding",
    "filing_date": "Date the trademark application was filed",
    "instituted_date": "Date the TTAB proceeding was instituted",
    "instituted_code": "Code indicating the reason the proceeding was instituted",
    "decision_date": "Date the decision was made on the proceeding",
    "decision_code": "Code representing the type of decision made",
    "decision_description": "Description of the decision made",
    "termination_code": "Code indicating the reason for termination of the proceeding",
    "termination_date": "Date the proceeding was terminated",
    "termination_date_2": "Date the proceeding was terminated 2",
    "termination_date_3": "Date the proceeding was terminated 3",
    "termination_date_4": "Date the proceeding was terminated 4",
    "termination_date_5": "Date the proceeding was terminated 5",
    "final_refusal_date": "Date of final refusal, if applicable",
    "fp_reason_1": "First reason for final refusal",
    "fp_reason_2": "Second reason for final refusal",
    "fp_reason_3": "Third reason for final refusal",
    "fp_reason_4": "Fourth reason for final refusal",
    "fp_reason_5": "Fifth reason for final refusal",
    "pendency_d": "Pendency in days between decision date and instituted date",
    "pendency_t": "Pendency in days between termination date and instituted date",
    "pendency_r": "N/A",
    "inventory": "Indicates if the case is part of the inventory",
    "non_pro_se": "Indicates if the case is non-pro se",
    "pctram_link": "Link to PCTRAM record",
    "law_office": "Law office handling the case",
    "filing_basis_grp": "Group of filing basis",
    "filing_method_cur": "Current filing method",
    "am_stat": "Amendment status",
    "owner_name": "Name of the trademark owner",
    "city": "City of the trademark owner",
    "state": "State of the trademark owner",
    "country_or_area_name": "Country or area of the trademark owner",
    "reg_class_count": "Count of registered classes",
    "active_class_count": "Count of active classes",
    "group_type": "Type of group",
    "concat_class": "Concatenated class information",
    "mark_nm_short": "Short name of the mark",
    "refusal": "Indicates if there was a refusal",
    "appeal": "Indicates if there was an appeal",
    "publication_date": "Date of publication",
    "pubs": "Indicates if published",
    "opposition": "Indicates if there was an opposition",
    "default_opposition": "Indicates if there was a default opposition",
    "default_cancellation": "Indicates if there was a default cancellation",
    "cancellation": "Indicates if there was a cancellation",
    "constructed_prcd_num": "Constructed proceeding number",
    "default_date": "Date of default",
    "cancellation_count": "Count of cancellations",
    "reg_yr": "Registration year",
    "live_reg_count": "Count of live registrations",
    "can_rate": "Cancellation rate",
    "concurrent": "Indicates if concurrent",
    "rfd_date": "Date of refusal",
    "rfd_valid": "Indicates if the refusal date is valid",
    "proceeding_count": "Count of proceedings",
    "case_age_rfd": "Age of the case at refusal date",
    "case_age_category": "Category of case age",
    "create_ts": "Timestamp when the record was created",
    "create_user_id": "User ID of the creator",
    "update_ts": "Timestamp when the record was last updated",
    "update_user_id": "User ID of the last updater",
}

if tableExists(table_name):
    for column, comment in columns_comments.items():
        alter_query = (
            f"""ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'"""
        )
        print(alter_query)
        try:
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Unable to update table comment for `{table_name}`")

# COMMAND ----------

table_name = "ttab_workloads"

columns_comments = {
    "fiscal_year": "The fiscal year for the data",
    "date": "The date of the record",
    "ttab_case_type": "The type of TTAB case",
    "day_total": "Total cases or actions for the day",
    "actual_estimated": "Indicates whether the data is actual or estimated",
    "fy_base_total": "The base total for the fiscal year",
    "fy_judge_decisions": "Number of judge decisions in the fiscal year",
    "fy_jdr": "Judge decision rate for the fiscal year",
    "latest_5yr_avg_jdr": "Average judge decision rate over the last 5 years",
    "raw_credits": "Raw credits earned",
    "credits_jdr_applied": "Credits applied towards the judge decision rate",
    "create_ts": "Timestamp when the record was created",
    "create_user_id": "User ID of the creator",
    "update_ts": "Timestamp when the record was last updated",
    "update_user_id": "User ID of the last updater",
}

if tableExists(table_name):
    for column, comment in columns_comments.items():
        alter_query = (
            f"""ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'"""
        )
        print(alter_query)
        try:
            spark.sql(
                f"""
                ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'
                """
            )
        except:
            print(f"Unable to update table comment for `{table_name}`")

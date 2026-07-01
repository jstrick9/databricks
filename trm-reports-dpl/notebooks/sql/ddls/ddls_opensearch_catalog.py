# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose:
# MAGIC <pre> 
# MAGIC In this notebook contains SQL code to create catalog and schema for PEA Opensearch analytics. 
# MAGIC <pre>

# COMMAND ----------

# DBTITLE 1,Create Widget
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
print(f'{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Read Config File.
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common functions notebook.
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Set Parameters.
common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
print(f"{trgt_catalog=}")
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Creating Catalog
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${conf.catalog} MANAGED LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}';

# COMMAND ----------

# DBTITLE 1,Creating Schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.bronze 
# MAGIC COMMENT 'For trm reports raw data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.silver 
# MAGIC COMMENT 'For trm reports staging layer data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ${conf.catalog}.gold 
# MAGIC COMMENT 'For trm reports gold layer data' ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Bronze Trademark Applications Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.pea_trademark_applications (
# MAGIC
# MAGIC assignee STRING,
# MAGIC case_internal_status STRING,
# MAGIC case_status_code STRING,
# MAGIC date_last_uploaded STRING,
# MAGIC date_pre_exam_received STRING,
# MAGIC filing_date STRING,
# MAGIC mark STRING,
# MAGIC pre_exam_status STRING,
# MAGIC serial_number STRING,
# MAGIC trademark_track_type STRING,
# MAGIC pre_exam_history_latest_order_no STRING,
# MAGIC pre_exam_history_history_action STRING,
# MAGIC pre_exam_history_history_by STRING,
# MAGIC pre_exam_history_history_date_time STRING,
# MAGIC pre_exam_history_history_from STRING,
# MAGIC pre_exam_history_history_order STRING,
# MAGIC pre_exam_history_history_to STRING,
# MAGIC last_updated STRING
# MAGIC
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/pea_trademark_applications' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Bronze TQR Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.bronze.pea_tqr (
# MAGIC assignee STRING,
# MAGIC case_internal_status STRING,
# MAGIC date_uploaded STRING,
# MAGIC pre_exam_status STRING,
# MAGIC review_manager STRING,
# MAGIC review_started STRING,
# MAGIC reviewer STRING,
# MAGIC serial_number STRING,
# MAGIC class_class_number STRING,
# MAGIC class_goods_services_text STRING,
# MAGIC class_latest_order_no STRING,
# MAGIC class_status STRING,
# MAGIC design_search_code_latest_order_no STRING,
# MAGIC design_search_code_status STRING,
# MAGIC design_search_code_value STRING,
# MAGIC mark_drawing_code_latest_order_no STRING,
# MAGIC mark_drawing_code_status STRING,
# MAGIC mark_drawing_code_value STRING,
# MAGIC pseudomarks_latest_order_no STRING,
# MAGIC pseudomarks_status STRING,
# MAGIC pseudomarks_value STRING,
# MAGIC word_mark_latest_order_no STRING,
# MAGIC word_mark_status STRING,
# MAGIC word_mark_value STRING,
# MAGIC class_comments_action STRING,
# MAGIC class_comments_by STRING,
# MAGIC class_comments_date_time STRING,
# MAGIC class_comments_message STRING,
# MAGIC class_comments_order STRING,
# MAGIC design_search_code_comments_action STRING,
# MAGIC design_search_code_comments_by STRING,
# MAGIC design_search_code_comments_date_time STRING,
# MAGIC design_search_code_comments_message STRING,
# MAGIC design_search_code_comments_order STRING,
# MAGIC mark_drawing_code_comments_action STRING,
# MAGIC mark_drawing_code_comments_by STRING,
# MAGIC mark_drawing_code_comments_date_time STRING,
# MAGIC mark_drawing_code_comments_message STRING,
# MAGIC mark_drawing_code_comments_order STRING,
# MAGIC pseudomarks_comments_action STRING,
# MAGIC pseudomarks_comments_by STRING,
# MAGIC pseudomarks_comments_date_time STRING,
# MAGIC pseudomarks_comments_message STRING,
# MAGIC pseudomarks_comments_order STRING,
# MAGIC word_mark_comments_action STRING,
# MAGIC word_mark_comments_by STRING,
# MAGIC word_mark_comments_date_time STRING,
# MAGIC word_mark_comments_message STRING,
# MAGIC word_mark_comments_order STRING
# MAGIC
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/pea_tqr' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Silver PEA Trademark Applications
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.pea_trademark_applications (
# MAGIC   tm_app STRING COMMENT 'Contains the unique identifier for each trademark application, allowing for easy tracking and reference throughout the application process.',
# MAGIC   ser_num STRING COMMENT 'Serial number, the unique identifier for a trademark case.',
# MAGIC   assignee STRING COMMENT 'Identifies the individual to whom the trademark application is assigned, providing insight into ownership and responsibility.',
# MAGIC   pre_exam_status STRING COMMENT 'Indicates the current status of the trademark application during the pre-examination phase, which is crucial for understanding its progress.',
# MAGIC   submission_type STRING COMMENT 'Describes the type of submission made for the trademark application, helping to categorize the nature of the application.',
# MAGIC   filing_ts TIMESTAMP COMMENT 'Records the timestamp of when the trademark application was filed, which is important for tracking timelines and deadlines.',
# MAGIC   last_updated_ts TIMESTAMP COMMENT 'Captures the most recent timestamp when any changes were made to the trademark application, allowing for monitoring of updates.',
# MAGIC   last_uploaded_ts TIMESTAMP COMMENT 'Notes the timestamp of the last document uploaded related to the trademark application, which is useful for document management.',
# MAGIC   pre_exam_received_ts TIMESTAMP COMMENT 'Marks the timestamp when the trademark application was received for pre-examination, providing a reference point for processing times.',
# MAGIC   history_action STRING COMMENT 'Details the specific action taken during the history of the trademark application, which is vital for understanding its lifecycle.',
# MAGIC   history_by STRING COMMENT 'Indicates who performed the action recorded in the history of the trademark application, offering accountability and traceability.',
# MAGIC   history_ts TIMESTAMP COMMENT 'Records the timestamp of when the action in the history occurred, which is essential for tracking changes over time.',
# MAGIC   history_from STRING COMMENT 'Shows the previous state or status of the trademark application before the recorded action, providing context for changes made.',
# MAGIC   history_to STRING COMMENT 'Indicates the new state or status of the trademark application after the recorded action, helping to understand the impact of changes.',
# MAGIC   history_order INT COMMENT 'Represents the order of the action in the history log, which is useful for organizing and reviewing the sequence of events.',
# MAGIC   latest_order_no INT COMMENT 'Contains the latest order number assigned to the trademark application, which is important for tracking the most recent updates.',
# MAGIC   create_ts TIMESTAMP COMMENT 'Records the timestamp when the trademark application entry was created, providing a reference for the application inception.',
# MAGIC   last_updt_ts TIMESTAMP COMMENT 'Captures the last timestamp when the trademark application was updated, which is important for maintaining accurate records.'
# MAGIC ) USING DELTA
# MAGIC LOCATION
# MAGIC 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/pea_trademark_applications'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled' = true, 'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# DBTITLE 1,Silver PEA TQR
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.silver.pea_tqr (
# MAGIC   tqr_app STRING COMMENT 'The identifier for the Trademark Quality Review application.',
# MAGIC   assignee STRING COMMENT 'The Pre-exam worker assigned to the case.',
# MAGIC   date_uploaded_ts TIMESTAMP COMMENT 'The timestamp of when the application was uploaded.',
# MAGIC   pre_exam_status STRING COMMENT 'Status code indicating the applications current progress. For example: 115 is Manager Denied LIE appeal.',
# MAGIC   review_manager STRING COMMENT 'The manager overseeing the review process',
# MAGIC   review_started TIMESTAMP COMMENT 'The timestamp when the review process officially began.',
# MAGIC   reviewer STRING COMMENT 'The individual conducting the review.',
# MAGIC   serial_number STRING COMMENT 'The unique identifier for a Trademark case assigned to the application.',
# MAGIC   class_comments_action STRING COMMENT 'Action such as LIE appeal, reviewer comment, or manager accept',
# MAGIC   class_comments_message STRING COMMENT 'Class comment message such as Accepted the appeal.',
# MAGIC   design_search_code_comments_action STRING COMMENT 'Actions associated with comments on design search codes such as reviewer_advisory_comment',
# MAGIC   design_search_code_comments_message STRING COMMENT 'Messages related to design search code comments.',
# MAGIC   mark_drawing_code_comments_actions STRING COMMENT 'Actions taken regarding comments on mark drawing codes.',
# MAGIC   mark_drawing_code_comments_message STRING COMMENT 'Messages related to mark drawing code comments.',
# MAGIC   pseudomarks_comments_action STRING COMMENT 'Actions taken concerning pseudomarks comments.',
# MAGIC   pseudomarks_comments_message STRING COMMENT 'Messages related to pseudomarks comments.',
# MAGIC   word_mark_comments_action STRING COMMENT 'Actions associated with comments on word marks.',
# MAGIC   word_mark_comments_message STRING COMMENT 'Messages related to word mark comments.',
# MAGIC   create_ts TIMESTAMP COMMENT 'The timestamp when the review record was created.',
# MAGIC   last_updt_ts TIMESTAMP COMMENT 'The timestamp of the most recent update to this TQR review record.'
# MAGIC   )
# MAGIC USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/pea_tqr' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Gold Worker Performance
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${conf.catalog}.gold.pea_worker_performance (
# MAGIC
# MAGIC calendar_day DATE COMMENT 'a day in the calendar year in which the work was performed and counted',
# MAGIC pay_period STRING COMMENT 'the pay period of the business day',
# MAGIC pp_start_date DATE COMMENT 'the starting date of the pay period',
# MAGIC pp_end_date DATE COMMENT 'the ending date of the pay period',
# MAGIC assignee STRING COMMENT 'the worker no of the person the pre exam application is assigned',
# MAGIC worker_nm STRING COMMENT 'the name of the person the pre exam application is assigned',
# MAGIC daily_teas_processed INT COMMENT 'the number of teas applications that were uploaded as status 103 for a given assignee on that workday',
# MAGIC daily_teas_assigned INT COMMENT 'the number of teas applications that were assigned in status 101 for a given assignee on that workday',
# MAGIC daily_madrd_processed INT COMMENT 'the number of madrd applications that were uploaded as status 103 for a given assignee on that workday',
# MAGIC daily_madrd_assigned INT COMMENT  'the number of teas applications that were assigned in status 101 for a given assignee on that workday',
# MAGIC daily_paper_processed INT COMMENT 'the number of paper applications that were uploaded as status 103 for a given assignee on that workday',
# MAGIC daily_paper_assigned INT COMMENT 'the number of paper applications that were assigned in status 101 to an assignee',
# MAGIC teas_inventory_processed INT COMMENT 'the number of teas applications to date that have been processed by an employee',
# MAGIC teas_inventory_assigned INT COMMENT 'the number of teas applications to date that have been assigned to an employee',
# MAGIC teas_todate_inventory INT COMMENT 'the number of teas applications to date that are currently in an employee docket',
# MAGIC madrd_inventory_processed INT COMMENT 'the number of madrd applications to date that have been processed by an employee',
# MAGIC madrd_inventory_assigned INT COMMENT 'the number of madrd applications to date that have been assigned to an employee',
# MAGIC madrd_todate_inventory INT COMMENT 'the number of madrd applications to date that are currently in an employee docket',
# MAGIC paper_inventory_processed INT COMMENT 'the number of madrd applications to date that have been processed by an employee',
# MAGIC paper_inventory_assigned INT COMMENT 'the number of madrd applications to date that have been assigned to an employee',
# MAGIC paper_todate_inventory INT COMMENT 'the number of madrd applications to date that are currently in an employee docket',
# MAGIC daily_inventory INT COMMENT 'the total inventory of all apps added for an employee on that day',
# MAGIC teas_overall_inventory INT COMMENT 'the overall teas inventory for a specific day, applications in status 100 unassigned, includes rollover',
# MAGIC madrd_overall_inventory INT COMMENT 'the overall madrd inventory for a specific day, applications in status 100 unassigned, includes rollover',
# MAGIC paper_overall_inventory INT COMMENT 'the overall paper inventory for a specific day, applications in status 100 unassigned, includes rollover',
# MAGIC total_inventory INT COMMENT 'the total inventory that includes rollover, applications in status 100 on a specific day',
# MAGIC oldest_serial_teas STRING COMMENT 'the oldest teas serial in the dataset on that day in an unassigned status, not completed',
# MAGIC oldest_filing_date_teas DATE COMMENT 'the oldest teas filing date in the dataset for that day in an assigned or unassigned status, not completed',
# MAGIC oldest_serial_madrd STRING COMMENT 'the oldest madrid serial in the dataset on that day in an unassigned status, not completed',
# MAGIC oldest_filing_date_madrd DATE COMMENT 'the oldest madrid filing date in the dataset for that day in an unassigned status, not completed',
# MAGIC oldest_serial_paper STRING COMMENT 'the oldest paper serial in the dataset on that day in an unassigned status, not completed',
# MAGIC oldest_filing_date_paper DATE COMMENT 'the oldest paper filing date in the dataset for that day in an unassigned status, not completed',
# MAGIC teas_pendency INT COMMENT 'on the current business day, the difference between the oldest unassigned date and the current business day',
# MAGIC madrd_pendency INT COMMENT 'on the current business day, the difference between the oldest unassigned date and the current business day',
# MAGIC paper_pendency INT COMMENT 'on the current business day, the difference between the oldest unassigned date and the current business day',
# MAGIC oldest_serial STRING COMMENT 'the oldest serial in the dataset on that day in an unassigned status, not completed',
# MAGIC oldest_filing_date DATE COMMENT 'the oldest filing date in the dataset for that day in an unassigned status, not completed, tied to oldest serial',
# MAGIC tqr_assigned_review INT COMMENT 'the number of apps assigned for tqr review',
# MAGIC tqr_errors INT COMMENT 'the number of apps marked as having errors from tqr review',
# MAGIC tqr_review_completed INT COMMENT 'the number of apps marked as having completed tqr review',
# MAGIC tqr_review_completed_after_correction INT COMMENT 'the number of apps marked as completed tqr review after corrections made',
# MAGIC tqr_advisories_no_action INT COMMENT 'the number of apps marked as tqr advisories with no action needed',
# MAGIC tqr_advisories_action_needed INT COMMENT 'the number of apps mark as tqr advisories where action is needed',
# MAGIC create_ts TIMESTAMP COMMENT 'The date and time that entity is inserted in the database',
# MAGIC create_user_id STRING COMMENT 'The job identifier that initiated the insert into the database',
# MAGIC last_mod_ts TIMESTAMP COMMENT 'The timestamp that record was last modified in the database. Upon creation, same as the create timestamp',
# MAGIC last_mod_user_id STRING COMMENT 'The logged-on User that initiated the update of the entity into the database. Upon creation, same as the Create User Identifier.'
# MAGIC
# MAGIC ) USING DELTA LOCATION 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/pea_worker_performance' TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled' = true,
# MAGIC   'delta.enableChangeDataFeed' = true
# MAGIC );

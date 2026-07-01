# Databricks notebook source
# DBTITLE 1,Set Environment
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name
print(f"{config_file=}")

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
trgt_catalog = common_configs["schema"]["trgt_catalog"]
print(f"{trgt_catalog=}")
cdc_bucket = common_configs["cdc"]["cdc_bucket"]
spark.conf.set("conf.cdc_bucket", cdc_bucket)
spark.conf.set("conf.catalog", trgt_catalog)
spark.conf.set("conf.dbx_env", dbx_env)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

# DBTITLE 1,os34_report_status_detail
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.os34_report_status_detail (
# MAGIC   serial_num string comment 'The unique ID for a trademark case.',
# MAGIC   status_code string
# MAGIC     comment 'The legacy TRAM code associated with a trademark case at the time of the load_date.',
# MAGIC   registration_num string comment 'The unique ID associated with registered trademark cases.',
# MAGIC   status_date date comment 'The date associated with the legacy TRAM status code.',
# MAGIC   is_currently_filed_itu boolean
# MAGIC     comment 'A flag indicating whether the case is currently in ITU status at the time of the load_date.',
# MAGIC   is_filed_itu boolean comment 'A flag indicating whether the case was filed in an ITU state.',
# MAGIC   filing_date date comment 'The date the trademark was filed.',
# MAGIC   num_active_classes bigint
# MAGIC     comment 'The number of active classes associated with the trademark case. For OS34, these are are classes with 6, W, or P status.',
# MAGIC   has_eligible_non_registered_and_active_status boolean
# MAGIC     comment 'A flag specifc for counting OS34 cases. Records with this flag have no registration number and are status 771 at the time of loading.',
# MAGIC   has_no_classes boolean
# MAGIC     comment 'A flag indicating whether the trademark case has no classes associated with it. This is specific to the OS34 workflow to help qualify cases as part of the aggregate.',
# MAGIC   is_counted_application boolean comment 'A flag indicating whether OS34 will count the record as a pending application.',
# MAGIC   is_counted_noa boolean comment 'A flag indicating whether OS34 will count the record as an application with NOA status.',
# MAGIC   is_counted_use boolean comment 'A flag indicating whether OS34 will count the record as an application with USE status.',
# MAGIC   is_counted_itu boolean comment 'A flag indicating whether OS34 will count the record as an application with ITU status.',
# MAGIC   load_date date
# MAGIC     comment 'The date that the record was loaded as part of the batch process. This is used to sync table aggregates across several metrics.',
# MAGIC   is_static boolean
# MAGIC     comment 'A flag indicating whether the record should be used to display to users.',
# MAGIC   latest boolean
# MAGIC     comment 'A flag indicating whether the record is the latest for its associated attributes.',
# MAGIC   create_user string comment 'The user or system that inserted the record',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was inserted.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/os34_report_status_detail'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.identityColumns' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,os34_report_status_total_detail
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.os34_report_status_total_detail (
# MAGIC   status_date date
# MAGIC     comment 'The date associated with the legacy TRAM status code. This is used in tandem with the load date for OS34 in order to aggregate applcation, NOA, USE, and ITU cases and classes.',
# MAGIC   total_application_cases bigint
# MAGIC     comment 'The total number of application cases associated with the status date.',
# MAGIC   total_application_classes bigint
# MAGIC     comment 'The total number of application classes associated with the status date.',
# MAGIC   total_noa_cases bigint comment 'The total number of NOA cases associated with the status date.',
# MAGIC   total_noa_classes bigint
# MAGIC     comment 'The total number of NOA classes associated with the status date.',
# MAGIC   total_use_cases bigint comment 'The total number of USE cases associated with the status date.',
# MAGIC   total_use_classes bigint
# MAGIC     comment 'The total number of USE classes associated with the status date.',
# MAGIC   total_itu_cases bigint comment 'The total number of ITU cases associated with the status date.',
# MAGIC   total_itu_classes bigint
# MAGIC     comment 'The total number of ITU classes associated with the status date.',
# MAGIC   load_date date
# MAGIC     comment 'The date that the record was loaded as part of the batch process. This is used to sync table aggregates across several metrics.',
# MAGIC   is_static boolean
# MAGIC     comment 'A flag indicating whether the record should be used to display to users.',
# MAGIC   latest boolean
# MAGIC     comment 'A flag indicating whether the record is the latest for its associated attributes.',
# MAGIC   create_user string comment 'The user or system that inserted the record',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was inserted.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/os34_report_status_total_detail'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.identityColumns' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,os34_report_abandonments_detail_fytd
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.os34_report_abandonments_detail_fytd (
# MAGIC   status_code string comment 'The TRAM legacy status code associated with a serial number.',
# MAGIC   status_description string comment 'The description of the TRAM legacy status code.',
# MAGIC   serial_num string comment 'The unique ID associated with trademark cases.',
# MAGIC   active_class_cnt bigint
# MAGIC     comment 'The number of active classes associated with the case. For OS34, this is any class with a status of 6, P, or W.',
# MAGIC   action_date date
# MAGIC     comment 'The date of the status. For non-abandonments, the status date is used. For abandonments, the milestone date for abandonment is used. This is specific to OS34.',
# MAGIC   abandoned boolean
# MAGIC     comment 'A flag indicating whether the associated case has an abandoned status at the time the record was loaded.',
# MAGIC   load_date date
# MAGIC     comment 'The date that the record was loaded as part of the batch process. This is used to sync table aggregates across several metrics.',
# MAGIC   is_static boolean
# MAGIC     comment 'A flag indicating whether the record should be used to display to users.',
# MAGIC   latest boolean
# MAGIC     comment 'A flag indicating whether the record is the latest for its associated attributes.',
# MAGIC   create_user string comment 'The user or system that inserted the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was inserted.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/os34_report_abandonments_detail_fytd'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.identityColumns' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

# DBTITLE 1,os34_report_deferred_revenue_cases
# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.os34_report_deferred_revenue_cases (
# MAGIC     serial_num string comment 'The unique ID for a trademark case.',
# MAGIC     status_code string
# MAGIC       comment 'The legacy TRAM status code associated with the case at the time of the ETL execution.',
# MAGIC     active_classes bigint
# MAGIC       comment 'The number of classes with an active status according to the OS34 report. This means that classes with a status of 6, P, or W are considered active (as opposed to classes with only a status of 6).',
# MAGIC     status_date date comment 'The date associated with the legacy TRAM status code.',
# MAGIC     load_date date
# MAGIC       comment 'The date that the record was loaded as part of the batch process. This is used to sync table aggregates across several metrics.',
# MAGIC     create_user string comment 'The user or system that inserted the record.',
# MAGIC     create_timestamp timestamp comment 'The timestamp that the record was inserted.'
# MAGIC   ) using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_deferred_revenue_cases'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.identityColumns' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,os34_report_abandonments_fytd
# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.os34_report_abandonments_fytd (
# MAGIC     status_code bigint comment 'The TRAM legacy status code.',
# MAGIC     status_description string
# MAGIC       comment 'The status description associated with the legacy TRAM status code.',
# MAGIC     abandoned boolean comment 'A flag indicating whether the given status code is a code giving .',
# MAGIC     case_count bigint
# MAGIC       comment 'The number of cases associated with the status code for that load_date.',
# MAGIC     class_count bigint
# MAGIC       comment 'The number of classes associated with the status code for that load_date.',
# MAGIC     load_date date
# MAGIC       comment 'The date that the record was loaded as part of the batch process. This is used to sync table aggregates across several metrics.',
# MAGIC     is_static boolean
# MAGIC       comment 'A flag indicating whether the record should be used to display to users.',
# MAGIC     latest boolean
# MAGIC       comment 'A flag indicating whether the record is the latest for its associated attributes.',
# MAGIC     create_user string comment 'The user or system that inserted the record',
# MAGIC     create_timestamp timestamp comment 'The timestamp that the record was inserted.'
# MAGIC   ) using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_abandonments_fytd'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.identityColumns' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,os34_report_statuses
# MAGIC %sql
# MAGIC create table if not exists ${conf.catalog}.gold.os34_report_statuses (
# MAGIC     status_code bigint comment 'The TRAM legacy status code.',
# MAGIC     status_description string
# MAGIC       comment 'The status description associated with the legacy TRAM status code.',
# MAGIC     abandoned boolean comment 'A flag indicating whether or not the associated status indicates case abandonment.',
# MAGIC     case_count bigint
# MAGIC       comment 'The number of cases associated with the status code for the given load_date.',
# MAGIC     class_count bigint comment 'The number of classes associated with the status code for the given load_date.',
# MAGIC     load_date date
# MAGIC       comment 'The date that the record was loaded as part of the batch process. This is used to sync table aggregates across several metrics.',
# MAGIC     is_static boolean
# MAGIC       comment 'A flag indicating whether the record should be used to display to users.',
# MAGIC     latest boolean
# MAGIC       comment 'A flag indicating whether the record is the latest for its associated attributes.',
# MAGIC     create_user string comment 'The user or system that inserted the record',
# MAGIC     create_timestamp timestamp comment 'The timestamp that the record was inserted.'
# MAGIC   ) using DELTA
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_statuses'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = true,
# MAGIC     'delta.enableChangeDataFeed' = true
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,os34_report_totals
# MAGIC %sql
# MAGIC -- column names are associated with the legacy report and therefore, will not change.
# MAGIC create table if not exists ${conf.catalog}.gold.os34_report_totals (
# MAGIC     tot_app_cases bigint
# MAGIC       comment 'The current number of application cases at the time of the load_date.',
# MAGIC     tot_app_class bigint
# MAGIC       comment 'The current number of application classes at the time of the load_date.',
# MAGIC     totnoacases bigint comment 'The current number of NOA cases at the time of the load_date.',
# MAGIC     totnoaclass bigint comment 'The current number of NOA classes at the time of the load_date.',
# MAGIC     totusecase bigint comment 'The current number of USE cases at the time of the load_date.',
# MAGIC     totuseclass bigint comment 'The current number of USE classes at the time of the load_date.',
# MAGIC     itu_cases bigint comment 'The current number of ITU cases at the time of the load_date.',
# MAGIC     itu_class bigint comment 'The current number of ITU classes at the time of the load_date.',
# MAGIC     load_date date
# MAGIC       comment 'The date that the record was loaded as part of the batch process. This is used to sync table aggregates across several metrics.',
# MAGIC     is_static boolean
# MAGIC       comment 'A flag indicating whether the record should be used to display to users.',
# MAGIC     latest boolean
# MAGIC       comment 'A flag indicating whether the record is the latest for its associated attributes.',
# MAGIC     create_user string comment 'The user or system that inserted the record',
# MAGIC     create_timestamp timestamp comment 'The timestamp that the record was inserted.'
# MAGIC   ) using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/os34_report_totals'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.identityColumns' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
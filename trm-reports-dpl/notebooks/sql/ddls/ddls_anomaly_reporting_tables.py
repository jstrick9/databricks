# Databricks notebook source
# DBTITLE 1,Environment
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=}, {dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Functions
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Configurations
configs = read_yaml(config_file)
reporting_catalog = configs["schema"]["trm_reporting_catalog"]
cdc_bucket = configs["cdc"]["cdc_bucket"]
spark.conf.set("config.cdc_bucket", cdc_bucket)
spark.conf.set("config.catalog", reporting_catalog)
spark.conf.set("config.dbx_env", dbutils.widgets.get("dbx_env"))
print(f"{reporting_catalog=}, {cdc_bucket=}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog Setup

# COMMAND ----------

# DBTITLE 1,Create: Reporting Catalog
# MAGIC %sql
# MAGIC create catalog if not exists ${config.catalog}
# MAGIC managed location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Setup

# COMMAND ----------

# DBTITLE 1,Create: Silver Schema
# MAGIC %sql
# MAGIC create schema if not exists ${config.catalog}.silver
# MAGIC comment 'Silver layer — anomaly reporting derived data.';

# COMMAND ----------

# DBTITLE 1,Create: Gold Schema
# MAGIC %sql
# MAGIC create schema if not exists ${config.catalog}.gold
# MAGIC comment 'Gold layer — anomaly reporting aggregated data for dashboards.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# DBTITLE 1,Table: silver.fact_submission
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.silver.fact_submission (
# MAGIC     myuspto_patron_id string
# MAGIC       not null
# MAGIC       comment 'MyUSPTO Patron UUID. This is a 36-character randomly generated identifier.',
# MAGIC     selected_role string comment 'Assumed role at the time of submission.',
# MAGIC     transaction_id string comment 'Transaction ID associated with the trademark case form submission (taken from audit_log_id).',
# MAGIC     serial_number string comment 'Trademark serial number associated with the submission.',
# MAGIC     registration_number string comment 'Trademark registration number associated with the submission.',
# MAGIC     submission_time timestamp comment 'Full timestamp of the submission event.',
# MAGIC     submission_date date comment 'Calendar date of the submission (derived from submission_time).',
# MAGIC     submission_hour int
# MAGIC       comment 'Hour of day (0-23) of the submission (derived from submission_time).',
# MAGIC     filing_time timestamp comment 'Full timestamp of the filing event.',
# MAGIC     filing_date date comment 'Calendar date of the filing (derived from filing_dt).',
# MAGIC     filing_hour int
# MAGIC       comment 'Hour of day (0-23) of the filing (derived from filing_dt).',
# MAGIC     form_code string comment 'Form code identifying the type of trademark form submitted.',
# MAGIC     proof_code string comment 'Proof-of-use code associated with the submission.',
# MAGIC     signature_type string comment 'Type of electronic signature used on the submission.',
# MAGIC     signatory_name string comment 'Name of the individual who signed the submission.',
# MAGIC     signatory_position string comment 'Position or title of the signatory.',
# MAGIC     source_system string comment 'Human-readable name of the originating source system.',
# MAGIC     transaction_type string comment 'Transaction type code from the source system.',
# MAGIC     create_ts timestamp comment 'Timestamp when this record was created in the silver layer.',
# MAGIC     create_user_id string comment 'User or process that created this record.',
# MAGIC     update_ts timestamp comment 'Timestamp when this record was last updated.',
# MAGIC     update_user_id string comment 'User or process that last updated this record.'
# MAGIC   ) using delta
# MAGIC   cluster by (myuspto_patron_id, submission_date)
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/silver/fact_submission'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'Append fact table capturing every trademark submission event for anomaly analysis. Full-refresh overwrite on each ETL run.';

# COMMAND ----------

# DBTITLE 1,Table: silver.dim_patron_account
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.silver.dim_patron_account (
# MAGIC     myuspto_patron_id string
# MAGIC       not null
# MAGIC       comment 'MyUSPTO Patron UUID. This is a 36-character randomly generated identifier.',
# MAGIC     account_patron_nickname string comment 'Display nickname associated with the patron account.',
# MAGIC     account_patron_name string comment 'Full name of the patron as recorded in the account.',
# MAGIC     account_email string comment 'Primary email address on the patron account.',
# MAGIC     selected_role string
# MAGIC       comment 'Most-recent role selected by the patron (e.g. TrademarkAttorney, TrademarkOwner).',
# MAGIC     account_status string comment 'Current status of the patron account (e.g. active, deactivated).',
# MAGIC     -- SCD2 tracking columns
# MAGIC     effective_start_ts timestamp
# MAGIC       not null
# MAGIC       comment 'Timestamp when this version of the record became effective.',
# MAGIC     effective_end_ts timestamp
# MAGIC       comment 'Timestamp when this version was superseded; null for the current version.',
# MAGIC     is_current boolean
# MAGIC       not null
# MAGIC       comment 'True if this is the currently active version of the record.',
# MAGIC     -- audit columns
# MAGIC     create_ts timestamp comment 'Timestamp when this record was created in the silver layer.',
# MAGIC     create_user_id string comment 'User or process that created this record.',
# MAGIC     update_ts timestamp comment 'Timestamp when this record was last updated.',
# MAGIC     update_user_id string comment 'User or process that last updated this record.'
# MAGIC   ) using delta
# MAGIC   cluster by (myuspto_patron_id)
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/silver/dim_patron_account'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'SCD2 dimension capturing patron account attributes. Each change in business attributes creates a new version row; historical versions are retained with effective_end_ts populated.';

# COMMAND ----------

# DBTITLE 1,Table: silver.fact_sponsorship
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.silver.fact_sponsorship (
# MAGIC     myuspto_patron_id string
# MAGIC       not null
# MAGIC       comment 'MyUSPTO Patron UUID. This is a 36-character randomly generated identifier. This is typically derived from EDW, but in cases where the ID is missing from the dimension, it will be pulled from the submission fact.',
# MAGIC     has_sponsored array<string>
# MAGIC       comment 'Array of account IDs this patron has sponsored.',
# MAGIC     has_been_sponsored_by array<string>
# MAGIC       comment 'Array of account IDs that have sponsored this patron.',
# MAGIC     -- SCD2 tracking columns
# MAGIC     effective_start_ts timestamp
# MAGIC       not null
# MAGIC       comment 'Timestamp when this version of the record became effective.',
# MAGIC     effective_end_ts timestamp
# MAGIC       comment 'Timestamp when this version was superseded; null for the current version.',
# MAGIC     is_current boolean
# MAGIC       not null
# MAGIC       comment 'True if this is the currently active version of the record.',
# MAGIC     -- audit columns
# MAGIC     create_ts timestamp comment 'Timestamp when this record was created in the silver layer.',
# MAGIC     create_user_id string comment 'User or process that created this record.',
# MAGIC     update_ts timestamp comment 'Timestamp when this record was last updated.',
# MAGIC     update_user_id string comment 'User or process that last updated this record.'
# MAGIC   ) using delta
# MAGIC   cluster by (myuspto_patron_id)
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/silver/fact_sponsorship'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'SCD2 fact table tracking sponsorship relationships between patron accounts. Captures both outbound (has_sponsored) and inbound (has_been_sponsored_by) sponsorship chains.';

# COMMAND ----------

# DBTITLE 1,Table: silver.dim_patron_signature
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.silver.dim_patron_signature (
# MAGIC   myuspto_patron_id string
# MAGIC     not null
# MAGIC     comment 'MyUSPTO Account ID; this is typically derived from EDW, but in cases where the ID is missing from the dimension, it will be pulled from the submission fact..',
# MAGIC   -- signature name
# MAGIC   signed_name_counts map<string, bigint> comment 'Submission count per signatory name.',
# MAGIC   usually_signed_name_as string
# MAGIC     comment 'Most frequent signatory name; ties broken by most recent use.',
# MAGIC   -- signature position
# MAGIC   signed_position_counts map<string, bigint> comment 'Submission count per signatory position.',
# MAGIC   usually_signed_position_as string
# MAGIC     comment 'Most frequent signatory position; ties broken by most recent use.',
# MAGIC   -- signature type
# MAGIC   signed_type_counts map<string, bigint> comment 'Submission count per signature type.',
# MAGIC   usually_signed_type_as string
# MAGIC     comment 'Most frequent signature type; ties broken by most recent use.',
# MAGIC   -- temporal bounds
# MAGIC   first_submission_time timestamp comment 'Earliest submission timestamp for this patron.',
# MAGIC   last_submission_time timestamp comment 'Most recent submission timestamp for this patron.',
# MAGIC   -- audit columns
# MAGIC   create_ts timestamp comment 'Timestamp when this record was created in the silver layer.',
# MAGIC   create_user_id string comment 'User or process that created this record.',
# MAGIC   update_ts timestamp comment 'Timestamp when this record was last updated.',
# MAGIC   update_user_id string comment 'User or process that last updated this record.'
# MAGIC )
# MAGIC   using delta
# MAGIC   cluster by (myuspto_patron_id)
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/silver/dim_patron_signature'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'Tracks all signature names, positions, and types used across submissions and modal (most-common) values. Full-refresh overwrite on every ETL run.';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

# DBTITLE 1,Table: gold.patron_overview
# MAGIC %sql
# MAGIC create or replace table ${config.catalog}.gold.patron_overview (
# MAGIC   myuspto_patron_id string not null comment 'MyUSPTO Account ID; this is typically derived from EDW, but in cases where the ID is missing from the dimension, it will be pulled from the submission fact.',
# MAGIC   account_patron_nickname string comment 'Nickname of the patron account.',
# MAGIC   account_patron_name string comment 'Display name of the patron (account_patron_name).',
# MAGIC   account_email string comment 'Primary email address on the patron account.',
# MAGIC   selected_role string comment 'Most-recent role selected by the patron.',
# MAGIC   applicant_bin string comment 'Applicant bin classification from the anomaly model.',
# MAGIC   account_status string comment 'Current status of the patron account.',
# MAGIC   is_on_anomaly_list boolean comment 'True when the patron appears in gold.unsupervised_anomalies.',
# MAGIC   latest_anomaly_score double comment 'Most-recent anomaly score from the unsupervised model.',
# MAGIC   times_appeared long comment 'Number of times this patron has appeared on the anomaly list.',
# MAGIC   first_appeared timestamp comment 'Earliest timestamp the patron appeared on the anomaly list.',
# MAGIC   last_appeared timestamp comment 'Most-recent timestamp the patron appeared on the anomaly list.',
# MAGIC   usually_signed_name_as string comment 'Most-frequent signatory name used by this patron.',
# MAGIC   usually_signed_position_as string comment 'Most-frequent signatory position used by this patron.',
# MAGIC   usually_signed_type_as string comment 'Most-frequent signature type used by this patron.',
# MAGIC   first_submission_time timestamp comment 'Timestamp of the earliest submission by this patron.',
# MAGIC   last_submission_time timestamp comment 'Timestamp of the most-recent submission by this patron.',
# MAGIC   has_sponsored array<string> comment 'Array of account IDs this patron has sponsored.',
# MAGIC   has_been_sponsored_by array<string>
# MAGIC     comment 'Array of account IDs that have sponsored this patron.',
# MAGIC   total_distinct_cases long
# MAGIC     comment 'Total number of distinct trademark cases associated with this patron across all submissions.',
# MAGIC   total_distinct_cases_considered_by_model long
# MAGIC     comment 'Total distinct cases included in the anomaly model scoring window.',
# MAGIC   total_submissions long comment 'Total number of submissions made by this patron.',
# MAGIC   total_submissions_considered_by_model long
# MAGIC     comment 'Total submissions included in the anomaly model scoring window.',
# MAGIC   min_submissions_one_day long comment 'Minimum submissions recorded on any single day (all-time).',
# MAGIC   avg_submissions_per_day double
# MAGIC     comment 'Average daily submission count across active days (all-time).',
# MAGIC   max_submissions_one_day long comment 'Maximum submissions recorded on any single day (all-time).',
# MAGIC   min_submissions_one_day_considered_by_model long
# MAGIC     comment 'Minimum submissions on any single day within the model scoring window.',
# MAGIC   avg_submissions_per_day_considered_by_model double
# MAGIC     comment 'Average daily submission count within the model scoring window.',
# MAGIC   max_submissions_one_day_considered_by_model long
# MAGIC     comment 'Maximum submissions on any single day within the model scoring window.',
# MAGIC   transaction_id string comment 'Transaction identifier for the submission.',
# MAGIC   serial_number string comment 'Trademark serial number for the submission.',
# MAGIC   registration_number string comment 'Trademark registration number for the submission.',
# MAGIC   tm_exam_link string comment 'URL link to the trademark examiner view for the serial number.',
# MAGIC   submission_time timestamp comment 'Full timestamp of the submission event.',
# MAGIC   submission_date date comment 'Date of the submission.',
# MAGIC   submission_hour integer comment 'Hour of day (0-23) of the submission.',
# MAGIC   filing_time timestamp comment 'Full timestamp of the filing event.',
# MAGIC   filing_date date comment 'Date of the filing.',
# MAGIC   filing_hour integer comment 'Hour of day (0-23) of the filing.',
# MAGIC   form_code string comment 'Form code of the submission.',
# MAGIC   proof_code string comment 'Proof code of the submission.',
# MAGIC   signature_type string comment 'Signature type used on the submission.',
# MAGIC   signatory_name string comment 'Name of the signatory on the submission.',
# MAGIC   signatory_position string comment 'Position of the signatory on the submission.',
# MAGIC   source_system string comment 'Source system name for the submission.',
# MAGIC   create_ts timestamp comment 'Timestamp when this record was created in the gold layer.',
# MAGIC   create_user_id string comment 'User or process that created this record.',
# MAGIC   update_ts timestamp comment 'Timestamp when this record was last updated.',
# MAGIC   update_user_id string comment 'User or process that last updated this record.'
# MAGIC )
# MAGIC   using delta
# MAGIC   cluster by (myuspto_patron_id, applicant_bin)
# MAGIC   location 's3://${config.cdc_bucket}/delta_tables/${config.catalog}/gold/patron_overview'
# MAGIC   tblproperties (
# MAGIC     'databricks.delta.autocompact.enabled' = 'true',
# MAGIC     'delta.enableChangeDataFeed' = 'true',
# MAGIC     'delta.enableDeletionVectors' = 'true',
# MAGIC     'delta.feature.allowColumnDefaults' = 'supported',
# MAGIC     'delta.feature.changeDataFeed' = 'supported',
# MAGIC     'delta.feature.deletionVectors' = 'supported',
# MAGIC     'delta.feature.invariants' = 'supported',
# MAGIC     'delta.minReaderVersion' = '3',
# MAGIC     'delta.minWriterVersion' = '7'
# MAGIC   )
# MAGIC   comment 'Gold overview table for each patron. Targeted by ntb_anomaly_gold_query1.sql (runtime-ranked by appearances or score).';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# DBTITLE 1,Verify Silver Tables
# MAGIC %sql
# MAGIC use ${config.catalog}.silver;
# MAGIC
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Verify Gold Tables
# MAGIC %sql
# MAGIC use ${config.catalog}.gold;
# MAGIC
# MAGIC show tables;
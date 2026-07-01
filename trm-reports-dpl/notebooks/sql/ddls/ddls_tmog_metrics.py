# Databricks notebook source
# MAGIC %md
# MAGIC # TMOG Metrics DDLs

# COMMAND ----------

# DBTITLE 1,Set Widget
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,Retrieve Environment Details
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Shared Functions
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
trgt_catalog = common_configs["schema"]["trgt_catalog"]
cdc_bucket = common_configs["cdc"]["cdc_bucket"]
spark.conf.set("conf.cdc_bucket", cdc_bucket)
spark.conf.set("conf.catalog", trgt_catalog)
spark.conf.set("conf.dbx_env", dbx_env)
print(f"{trgt_catalog=}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TMOG Metrics: Bronze Level Tables

# COMMAND ----------

# DBTITLE 1,tmog_metrics_worker_transactions
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.bronze.tmog_metrics_worker_transactions (
# MAGIC   transaction_id string
# MAGIC     comment 'The transaction ID associated with the worker\'s transaction. This can only be an ID derived from `trm_tmngpdb.bronze.og_tm_review_gid`, `trm_tmngpdb.bronze.review_query_gid`, `trm_tmngpdb.bronze.query_ground_id`, or `trm_tmngpdb.bronze.review_query_appeal_id`.',
# MAGIC   transaction_type string
# MAGIC     comment 'The type of transaction the employee initiated or modified with respect to TMOG reviews.',
# MAGIC   transaction_timestamp timestamp comment 'The time that the transaction was initiated.',
# MAGIC   employee_id string comment 'The employee ID of the transaction initiator.',
# MAGIC   employee_name string
# MAGIC     comment 'The assumed employee name at the time of the transaction. Note: this is not enforced by a foreign key unless explicitely flagged.',
# MAGIC   employee_organization_code string
# MAGIC     comment 'The assumed employee organization at the time of the transaction. Note: this is not enforced by a foreign key unless explicitely flagged.',
# MAGIC   is_employee_name_hardcoded boolean
# MAGIC     comment 'A flag indicating whether or not the employee name was hardcoded based on the employee ID.  This is the case for a small number of auto or batch employee IDs.',
# MAGIC   is_employee_organization_hardcoded boolean
# MAGIC     comment 'A flag indicating whether or not the employee organization was hardcoded based on the employee ID. This is the case for a small number of auto or batch employee IDs.',
# MAGIC   is_employee_information_imputed_from_history boolean
# MAGIC     comment 'A flag indicating whether or not attributes associated with the employee ID were inserted based on previous non-null attributes. This is used to backfill attributes which are null.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was inserted',
# MAGIC   create_user string comment 'The user ID that inserted the record.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tmog_metrics_worker_transactions'
# MAGIC   comment 'The table contains transactions related workers who conducted TMOG queries.  This is only the subset of workers who initiated queries, query notes, or query appeals based on reviews. It serves as the basis for downstream TMOG worker metrics.'
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

# DBTITLE 1,tmog_metrics_transactions
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.bronze.tmog_metrics_transactions (
# MAGIC   review_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing an Official Gazette Trademark Review created by a USPTO employee across all TMNG applications.',
# MAGIC   initial_review_employee_id string comment 'Employee number of the initial reviewer.',
# MAGIC   initial_review_employee_organization_code string
# MAGIC     comment 'Assumed organization code based on the initial reviewer at the time of the initial review.',
# MAGIC   initial_review_timestamp timestamp comment 'Timestamp when the initial review was recorded.',
# MAGIC   latest_review_employee_id string comment 'Employee number of the latest reviewer.',
# MAGIC   latest_review_employee_organization_code string
# MAGIC     comment 'Assumed organization code based on the employee number of the latest reviewer.',
# MAGIC   latest_review_timestamp timestamp comment 'Timestamp when the review was last modified.',
# MAGIC   publication_date timestamp comment 'Date when the Official Gazette was issued.',
# MAGIC   serial_number string
# MAGIC     comment 'Trademark application serial number assigned at the time of receipt at the Patent Trademark Office.',
# MAGIC   previous_bounce_number int
# MAGIC     comment 'Number of times the mark application has been refused publication.',
# MAGIC   review_status_code string comment 'Status code for the review process of the mark application.',
# MAGIC   review_status_title string comment 'Status title for the review process of the mark application.',
# MAGIC   review_status_description string
# MAGIC     comment 'Detailed description of the review process status for the mark application.',
# MAGIC   review_query_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing a Review Query Issue across all TMNG applications. A Review Query (Issue) points out a publication error that could preclude a mark case from proceeding to publication.',
# MAGIC   og_page_number int comment 'Page number on which the mark appears in the proposed eOG.',
# MAGIC   print_error_indicator string
# MAGIC     comment 'Indicator denoting whether there was a printing error for the mark in the proposed eOG.',
# MAGIC   review_query_content string
# MAGIC     comment 'Descriptive text of the issue that must be addressed by the applicant for the mark to appear in the Official Gazette.',
# MAGIC   review_query_note_type_code string
# MAGIC     comment 'Code for the type of note attached to a Query Review.',
# MAGIC   review_query_note_type string comment 'Type of note attached to a Query Review.',
# MAGIC   review_query_note_description string
# MAGIC     comment 'Detailed description of the type of note attached to a Query Review.',
# MAGIC   review_query_ground_id string
# MAGIC     comment 'Surrogate key identifier for a Review Issue further categorized with a Ground Code and Ground Type Code.',
# MAGIC   review_query_ground_code string
# MAGIC     comment 'Code for the basis of citing an issue against a case being reviewed for publication in the Official Gazette.',
# MAGIC   review_query_ground string
# MAGIC     comment 'Basis for citing an issue against a case being reviewed for publication in the Official Gazette.',
# MAGIC   review_query_ground_description string
# MAGIC     comment 'Detailed description of the basis for citing an issue against a case being reviewed for publication in the Official Gazette.',
# MAGIC   review_query_ground_order_number int
# MAGIC     comment 'Numeric value specifying the order of significance of a Ground Code within a Ground Type.',
# MAGIC   review_query_ground_grouping_number int
# MAGIC     comment 'Numeric value used to logically group related Grounds.',
# MAGIC   review_query_ground_type_code string
# MAGIC     comment 'Code for the type of grounds for citing an issue against a mark case being reviewed for publication.',
# MAGIC   review_query_ground_type string
# MAGIC     comment 'Type of grounds for citing an issue against a mark case being reviewed for publication.',
# MAGIC   review_query_ground_type_description string
# MAGIC     comment 'Detailed description of the type of grounds for citing an issue against a mark case being reviewed for publication.',
# MAGIC   review_query_ground_class_id string comment 'Class ID associated with the query ground.',
# MAGIC   employee_review_query_id string
# MAGIC     comment 'Surrogate key identifier for a row created by an employee who created a Review Query at the ground level.',
# MAGIC   initial_review_query_employee_id string
# MAGIC     comment 'Employee number of the initial reviewer who queried a class.',
# MAGIC   initial_review_query_employee_organization_code string
# MAGIC     comment 'Assumed organization code based on the initial review query employee at the time of the initial review.',
# MAGIC   initial_review_query_timestamp timestamp
# MAGIC     comment 'Timestamp when the initial review query was recorded.',
# MAGIC   latest_review_query_employee_id string
# MAGIC     comment 'Employee number of the latest reviewer who queried a class.',
# MAGIC   latest_review_query_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the latest review query employee that modified the record.',
# MAGIC   latest_review_query_timestamp timestamp
# MAGIC     comment 'Timestamp when the latest review query was recorded.',
# MAGIC   review_query_assignment_date date
# MAGIC     comment 'Date when the review query was assigned to an employee.',
# MAGIC   initial_employee_review_query_status_employee_id string
# MAGIC     comment 'Employee number of the review query status record.',
# MAGIC   initial_employee_review_query_status_timestamp timestamp
# MAGIC     comment 'Initial timestamp when the status was assigned to the employee review query.',
# MAGIC   latest_employee_review_query_status_employee_id string
# MAGIC     comment 'Employee number of the employee who modified the review query status record last.',
# MAGIC   latest_employee_review_query_status_timestamp timestamp
# MAGIC     comment 'Timestamp when the status of the employee review query was last modified.',
# MAGIC   employee_review_query_status_code string comment 'Code for the status of the status assignment.',
# MAGIC   employee_review_query_status_code_description string
# MAGIC     comment 'Description of the status code of the review query assignment.',
# MAGIC   employee_review_query_status_reason_description string
# MAGIC     comment 'Supplementary note added to the status assignment.',
# MAGIC   review_query_note_sequence_number int
# MAGIC     comment 'Order sequence number of a note added to a Query Review. This is not a surrogate key; it is an auto-incremented value.',
# MAGIC   initial_review_query_note_employee_id string
# MAGIC     comment 'Employee number of the initial review query note author.',
# MAGIC   initial_review_query_note_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the employee at the time of the initial review query note.',
# MAGIC   initial_review_query_note_timestamp timestamp
# MAGIC     comment 'Timestamp when the initial review query note was recorded.',
# MAGIC   latest_review_query_note_employee_id string
# MAGIC     comment 'Employee number of the latest review query note modifier.',
# MAGIC   latest_review_query_note_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the employee who last modified the query query note.',
# MAGIC   latest_review_query_note_timestamp timestamp
# MAGIC     comment 'Timestamp when the latest review query note was recorded.',
# MAGIC   review_query_note_text string
# MAGIC     comment 'Text content of a remark or note regarding a Query Review.',
# MAGIC   review_query_appeal_id string comment 'Unique identifier for a Review Query Appeal.',
# MAGIC   review_query_appeal_approval_indicator string
# MAGIC     comment 'Flag indicating whether an appeal has been approved or denied for a specific case scheduled for a specific publication. Appeals are recorded by Query, Ground, and Ground Type.',
# MAGIC   review_query_appeal_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing a Query Appeal proceeding across all TMNG applications.',
# MAGIC   review_query_appeal_result_date date comment 'Date when the decision was made for the appeal.',
# MAGIC   review_query_appeal_proceeding_number string
# MAGIC     comment 'Number assigned to identify the appeal proceeding.',
# MAGIC   review_query_appeal_decision_description string
# MAGIC     comment 'Descriptive information about the appeal result.',
# MAGIC   review_query_appeal_reason_description string
# MAGIC     comment 'Free-form text describing the reason for the appeal decision.',
# MAGIC   review_query_appeal_director_email_sent_indicator string
# MAGIC     comment 'Y/N indicator denoting whether an email was sent to the Director regarding the Query Appeal.',
# MAGIC   review_query_appeal_result_code string
# MAGIC     comment 'Code specifying the outcome of an appeal proceeding.',
# MAGIC   review_query_appeal_result string comment 'Outcome of an appeal proceeding.',
# MAGIC   review_query_appeal_result_description string
# MAGIC     comment 'Detailed description of the outcome of an appeal proceeding.',
# MAGIC   initial_review_query_appeal_employee_id string
# MAGIC     comment 'Employee number associated with the initial review query appeal.',
# MAGIC   initial_review_query_appeal_employee_organization_code string
# MAGIC     comment 'Assumed organization code associated with the initial review query appeal.',
# MAGIC   initial_review_query_appeal_timestamp timestamp
# MAGIC     comment 'Timestamp of the initial review query appeal.',
# MAGIC   latest_review_query_appeal_employee_id string
# MAGIC     comment 'Employee number associated with the latest review query appeal modification.',
# MAGIC   latest_review_query_appeal_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the employee with the latest review query appeal modification.',
# MAGIC   latest_review_query_appeal_timestamp timestamp
# MAGIC     comment 'Timestamp of the latest modification to the review query appeal.',
# MAGIC   review_query_appeal_status_timestamp timestamp
# MAGIC     comment 'Timestamp when the appeal status was assigned.',
# MAGIC   review_query_appeal_sequence_number int
# MAGIC     comment 'Order number specifying when a note was added for the Query Appeal.',
# MAGIC   review_query_appeal_note string comment 'Comment text regarding the Query Appeal.',
# MAGIC   review_query_appeal_status_code string comment 'Code representing the status of a Query Appeal.',
# MAGIC   review_query_appeal_status string comment 'Status of a Query Appeal.',
# MAGIC   review_query_appeal_status_description string
# MAGIC     comment 'Detailed description of the status of a Query Appeal.',
# MAGIC   is_employee_attributes_derived_by_foreign_key boolean
# MAGIC     comment 'A flag indicating whether the record used the available source data based on a foreign key constraint.  This will likely always be false unless the source table is updated with this requirement.',
# MAGIC   create_user string comment 'The user ID of the user that inserted the record into the table.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was inserted into the table.'
# MAGIC )
# MAGIC   using delta
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/bronze/tmog_metrics_transactions'
# MAGIC   comment 'The table contains transactions related to the TMOG review data. It serves as the basis for downstream TMOG metrics.'
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
# MAGIC ## TMOG Metrics: Silver Level Tables

# COMMAND ----------

# DBTITLE 1,tmog_metrics_review_query_transactions
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.tmog_metrics_review_query_transactions (
# MAGIC   review_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing an Official Gazette Trademark Review created by a USPTO employee across all TMNG applications.',
# MAGIC   fk_review_query_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing a Review Query Issue across all TMNG applications. A Review Query (Issue) points out a publication error that could preclude a mark case from proceeding to publication.',
# MAGIC   initial_review_employee_id string comment 'Employee number of the initial reviewer.',
# MAGIC   initial_review_employee_organization_code string
# MAGIC     comment 'Assumed organization code based on the initial reviewer at the time of the initial review.',
# MAGIC   initial_review_timestamp timestamp comment 'Timestamp when the initial review was recorded.',
# MAGIC   latest_review_employee_id string comment 'Employee number of the latest reviewer.',
# MAGIC   latest_review_timestamp timestamp comment 'Timestamp when the review was last modified.',
# MAGIC   publication_date date comment 'Date when the Official Gazette was issued.',
# MAGIC   serial_number string
# MAGIC     comment 'Trademark application serial number assigned at the time of receipt at the Patent Trademark Office.',
# MAGIC   previous_bounce_number int
# MAGIC     comment 'Number of times the mark application has been refused publication.',
# MAGIC   review_status_code string comment 'Status code for the review process of the mark application.',
# MAGIC   review_status_title string comment 'Status title for the review process of the mark application.',
# MAGIC   review_status_description string
# MAGIC     comment 'Detailed description of the review process status for the mark application.',
# MAGIC   og_page_number int comment 'Page number on which the mark appears in the proposed eOG.',
# MAGIC   print_error_indicator string
# MAGIC     comment 'Indicator denoting whether there was a printing error for the mark in the proposed eOG.',
# MAGIC   review_query_content string
# MAGIC     comment 'Descriptive text of the issue that must be addressed by the applicant for the mark to appear in the Official Gazette.',
# MAGIC   review_query_note_type_code string
# MAGIC     comment 'Code for the type of note attached to a Query Review.',
# MAGIC   review_query_note_type string comment 'Type of note attached to a Query Review.',
# MAGIC   review_query_note_description string
# MAGIC     comment 'Detailed description of the type of note attached to a Query Review.',
# MAGIC   review_query_note_sequence_number int
# MAGIC     comment 'Order sequence number of a note added to a Query Review. This is not a surrogate key; it is an auto-incremented value.',
# MAGIC   initial_review_query_note_employee_id string
# MAGIC     comment 'Employee number of the initial review query note author.',
# MAGIC   initial_review_query_note_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the employee at the time of the initial review query note.',
# MAGIC   initial_review_query_note_timestamp timestamp
# MAGIC     comment 'Timestamp when the initial review query note was recorded.',
# MAGIC   latest_review_query_note_employee_id string
# MAGIC     comment 'Employee number of the latest review query note modifier.',
# MAGIC   latest_review_query_note_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the employee who last modified the query query note.',
# MAGIC   latest_review_query_note_timestamp timestamp
# MAGIC     comment 'Timestamp when the latest review query note was recorded.',
# MAGIC   review_query_note_text string
# MAGIC     comment 'Text content of a remark or note regarding a Query Review.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains transactions related to TMOG query review data. It serves as the basis for downstream TMOG metrics.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/tmog_metrics_review_query_transactions'
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

# DBTITLE 1,tmog_metrics_review_query_appeal_transactions
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.tmog_metrics_review_query_appeal_transactions (
# MAGIC   review_query_appeal_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing a Query Appeal proceeding across all TMNG applications.',
# MAGIC   fk_review_query_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing a Review Query Issue across all TMNG applications. A Review Query (Issue) points out a publication error that could preclude a mark case from proceeding to publication.',
# MAGIC   review_query_appeal_approval_indicator string
# MAGIC     comment 'Flag indicating whether an appeal has been approved or denied for a specific case scheduled for a specific publication. Appeals are recorded by Query, Ground, and Ground Type.',
# MAGIC   review_query_appeal_result_date date comment 'Date when the decision was made for the appeal.',
# MAGIC   review_query_appeal_proceeding_number string
# MAGIC     comment 'Number assigned to identify the appeal proceeding.',
# MAGIC   review_query_appeal_decision_description string
# MAGIC     comment 'Descriptive information about the appeal result.',
# MAGIC   review_query_appeal_reason_description string
# MAGIC     comment 'Free-form text describing the reason for the appeal decision.',
# MAGIC   review_query_appeal_director_email_sent_indicator string
# MAGIC     comment 'Y/N indicator denoting whether an email was sent to the Director regarding the Query Appeal.',
# MAGIC   review_query_appeal_result_code string comment 'Code specifying the outcome of an appeal proceeding.',
# MAGIC   review_query_appeal_result string comment 'Outcome of an appeal proceeding.',
# MAGIC   review_query_appeal_result_description string
# MAGIC     comment 'Detailed description of the outcome of an appeal proceeding.',
# MAGIC   initial_review_query_appeal_employee_id string
# MAGIC     comment 'Employee number associated with the initial review query appeal.',
# MAGIC   initial_review_query_appeal_employee_organization_code string
# MAGIC     comment 'Assumed organization code associated with the initial review query appeal.',
# MAGIC   initial_review_query_appeal_timestamp timestamp
# MAGIC     comment 'Timestamp of the initial review query appeal.',
# MAGIC   latest_review_query_appeal_employee_id string
# MAGIC     comment 'Employee number associated with the latest review query appeal modification.',
# MAGIC   latest_review_query_appeal_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the employee with the latest review query appeal modification.',
# MAGIC   latest_review_query_appeal_timestamp timestamp
# MAGIC     comment 'Timestamp of the latest modification to the review query appeal.',
# MAGIC   review_query_appeal_status_timestamp timestamp comment 'Timestamp when the appeal status was assigned.',
# MAGIC   review_query_appeal_sequence_number int
# MAGIC     comment 'Order number specifying when a note was added for the Query Appeal.',
# MAGIC   review_query_appeal_note string comment 'Comment text regarding the Query Appeal.',
# MAGIC   review_query_appeal_status_code string comment 'Code representing the status of a Query Appeal.',
# MAGIC   review_query_appeal_status string comment 'Status of a Query Appeal.',
# MAGIC   review_query_appeal_status_description string
# MAGIC     comment 'Detailed description of the status of a Query Appeal.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains transactions related to TMOG query appeals data. It serves as the basis for downstream TMOG metrics.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/tmog_metrics_review_query_appeal_transactions'
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

# DBTITLE 1,tmog_metrics_review_query_ground_transactions
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.silver.tmog_metrics_review_query_ground_transactions (
# MAGIC   review_query_ground_id string
# MAGIC     comment 'Surrogate key identifier for a Review Issue further categorized with a Ground Code and Ground Type Code.',
# MAGIC   fk_review_query_gid string
# MAGIC     comment 'TMNG Global Identifier for uniquely identifying and referencing a Review Query Issue across all TMNG applications. A Review Query (Issue) points out a publication error that could preclude a mark case from proceeding to publication.',
# MAGIC   review_query_ground_code string
# MAGIC     comment 'Code for the basis of citing an issue against a case being reviewed for publication in the Official Gazette.',
# MAGIC   review_query_ground string
# MAGIC     comment 'Basis for citing an issue against a case being reviewed for publication in the Official Gazette.',
# MAGIC   review_query_ground_description string
# MAGIC     comment 'Detailed description of the basis for citing an issue against a case being reviewed for publication in the Official Gazette.',
# MAGIC   review_query_ground_order_number int
# MAGIC     comment 'Numeric value specifying the order of significance of a Ground Code within a Ground Type.',
# MAGIC   review_query_ground_grouping_number int comment 'Numeric value used to logically group related Grounds.',
# MAGIC   review_query_ground_type_code string
# MAGIC     comment 'Code for the type of grounds for citing an issue against a mark case being reviewed for publication.',
# MAGIC   review_query_ground_type string
# MAGIC     comment 'Type of grounds for citing an issue against a mark case being reviewed for publication.',
# MAGIC   review_query_ground_type_description string
# MAGIC     comment 'Detailed description of the type of grounds for citing an issue against a mark case being reviewed for publication.',
# MAGIC   review_query_ground_class_id string comment 'Class ID associated with the query ground.',
# MAGIC   initial_review_query_employee_id string
# MAGIC     comment 'Employee number of the initial reviewer who queried a class.',
# MAGIC   initial_review_query_employee_organization_code string
# MAGIC     comment 'Assumed organization code based on the initial review query employee at the time of the initial review.',
# MAGIC   initial_review_query_timestamp timestamp
# MAGIC     comment 'Timestamp when the initial review query was recorded.',
# MAGIC   latest_review_query_employee_id string
# MAGIC     comment 'Employee number of the latest reviewer who queried a class.',
# MAGIC   latest_review_query_employee_organization_code string
# MAGIC     comment 'Assumed organization code of the latest review query employee that modified the record.',
# MAGIC   latest_review_query_timestamp timestamp
# MAGIC     comment 'Timestamp when the latest review query was recorded.',
# MAGIC   review_query_assignment_date date
# MAGIC     comment 'Date when the review query was assigned to an employee.',
# MAGIC   employee_review_query_status_code string comment 'Status code of the review query. Note: This is different from the status of the underlying review.',
# MAGIC   employee_review_query_status_code_description string comment 'Status code description of the review query.',
# MAGIC   employee_review_query_status_reason_description string
# MAGIC     comment 'Supplementary note added to the status assignment.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains transactions related to TMOG query grounds data. It serves as the basis for downstream TMOG metrics.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/silver/tmog_metrics_review_query_ground_transactions'
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
# MAGIC ## TMOG Metrics: Gold Level Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Level Queries

# COMMAND ----------

# DBTITLE 1,tmog_metrics_employee_review_query_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_employee_review_query_metrics (
# MAGIC   initial_review_query_date date comment 'The date that the initial review occured.',
# MAGIC   initial_review_query_employee_id string comment 'The employee ID of the initial review query.',
# MAGIC   employee_review_queries_day_total bigint
# MAGIC     comment 'The total number of review queries by the employee for the initial review query date.',
# MAGIC   employee_review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of review queries by the employee for the previous review query date.',
# MAGIC   employee_review_queries_rolling_total bigint
# MAGIC     comment 'The total number of review queries by the employee up to (and including) the initial review query date.',
# MAGIC   review_queries_day_total bigint
# MAGIC     comment 'The total number of review queries for the initial review date.',
# MAGIC   review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of review queries for the previous review date',
# MAGIC   review_queries_rolling_total bigint
# MAGIC     comment 'The total number of review queries up to (and including) the initial review query date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_employee_review_query_metrics'
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

# DBTITLE 1,tmog_metrics_publication_review_query_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_publication_review_query_metrics (
# MAGIC   initial_review_date date comment 'The date of the initial review.',
# MAGIC   publication_date date comment 'The date of the OG publication.',
# MAGIC   publication_date_review_queries_day_total bigint
# MAGIC     comment 'The total number of review queries associated with the OG publication date for the given review date.',
# MAGIC   publication_date_review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of review queries associated with the OG publication date for the previous review date.',
# MAGIC   publication_date_review_queries_rolling_total bigint
# MAGIC     comment 'up to (and including) the initial review query date.',
# MAGIC   review_queries_day_total bigint
# MAGIC     comment 'The total number of queries for the initial review date.',
# MAGIC   review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of queries based on the previous review date.',
# MAGIC   review_queries_rolling_total bigint
# MAGIC     comment 'The total number of queries up to (and including) the initial review query date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_publication_review_query_metrics'
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

# DBTITLE 1,tmog_metrics_case_review_query_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_case_review_query_metrics (
# MAGIC   initial_review_date date comment 'The date of the initial review.',
# MAGIC   case_review_queries_day_total bigint
# MAGIC     comment 'The total case count for the day of the initial review. Cases may appear more than once.',
# MAGIC   distinct_case_review_queries_day_total bigint
# MAGIC     comment 'The total distinct case count for the day of the initial review.',
# MAGIC   case_review_queries_day_previous_total bigint
# MAGIC     comment 'The total case count for the day of the previous initial review. Cases may appear more than once.',
# MAGIC   distinct_case_review_queries_day_previous_total bigint
# MAGIC     comment 'The total distinct case count for the previous initial review day.',
# MAGIC   case_review_queries_rolling_total bigint
# MAGIC     comment 'The total case count up to (and including) the initial review query date.',
# MAGIC   distinct_case_review_queries_rolling_total bigint
# MAGIC     comment 'The total distinct case count up to (and including) the initial review query date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_case_review_query_metrics'
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

# MAGIC %md
# MAGIC ### Gold Level Appeals

# COMMAND ----------

# DBTITLE 1,tmog_metrics_employee_review_query_appeal_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_employee_review_query_appeal_metrics (
# MAGIC   initial_review_query_appeal_date date comment 'The initial review query date.',
# MAGIC   initial_review_query_appeal_employee_id string
# MAGIC     comment 'The employee ID of the initial query appeal.',
# MAGIC   employee_review_query_appeals_day_total bigint
# MAGIC     comment 'The total number of query appeals for the employee for the query appeal date.',
# MAGIC   employee_review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of query appeals for the employee for the previous query appeal date.',
# MAGIC   employee_review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total number of query appeals by the employee up to (and including) the initial review query appeal date.',
# MAGIC   review_query_appeals_day_total bigint
# MAGIC     comment 'The total number of query appeals for the given query appeal date.',
# MAGIC   review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of query appeals for the previous query appeal date.',
# MAGIC   review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total number of query appeals up to (and including) the initial review query appeal date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_employee_review_query_appeal_metrics'
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

# DBTITLE 1,tmog_metrics_employee_review_query_appeal_status_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_employee_review_query_appeal_status_metrics (
# MAGIC   review_query_appeal_status_date date comment 'The date of the query appeal status.',
# MAGIC   initial_review_query_appeal_employee_id string
# MAGIC     comment 'The employee ID of the query appeal initiator.',
# MAGIC   employee_review_query_appeals_day_total bigint
# MAGIC     comment 'The total number of query appeals for the query appeal status date.',
# MAGIC   employee_review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of query appeals for the previous query appeal status date.',
# MAGIC   employee_review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total up to (and including) the review query appeal status date. Note that this can be different from the system transaction time.',
# MAGIC   review_query_appeals_day_total bigint
# MAGIC     comment 'The total number of query appeals for a query appeal status date (across all employees).',
# MAGIC   review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of query appeals for the previous query appeal status date (across all employees).',
# MAGIC   review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total number of query appeals up to (and including) the query appeal status date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_employee_review_query_appeal_status_metrics'
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

# DBTITLE 1,tmog_metrics_result_review_query_appeal_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_result_review_query_appeal_metrics (
# MAGIC   initial_review_query_appeal_date date comment 'The initial date of the review query appeal.',
# MAGIC   review_query_appeal_result_code string comment 'The code of the result of the query appeal.',
# MAGIC   review_query_appeal_result string comment 'The result of the query appeal.',
# MAGIC   review_query_appeal_result_description string comment 'The result description of the query appeal.',
# MAGIC   result_review_query_appeals_day_total bigint comment 'The total query appeal.',
# MAGIC   result_review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of review query appeals for the previous query appeal date.',
# MAGIC   result_review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total number of review query appeals up to (and including) the query appeal date.',
# MAGIC   review_query_appeals_day_total bigint
# MAGIC     comment 'The total number of query appeals for a given day.',
# MAGIC   review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of query appeals for the previous day.',
# MAGIC   review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total number of query appeals up to (and including) the query appeal date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_result_review_query_appeal_metrics'
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

# DBTITLE 1,tmog_metrics_result_review_query_appeal_status_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_result_review_query_appeal_status_metrics (
# MAGIC   review_query_appeal_status_date date comment 'The status date of the query appeal.',
# MAGIC   review_query_appeal_result_code string comment 'The query appeal result code.',
# MAGIC   review_query_appeal_result string comment 'The query appeal result description.',
# MAGIC   review_query_appeal_result_description string
# MAGIC     comment 'The query appeal result long description (where available).',
# MAGIC   result_review_query_appeals_day_total bigint
# MAGIC     comment 'The total number of query appeal for the given result for the query appeal status date.',
# MAGIC   result_review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of query appeal for the given result for the previous query appeal status date.',
# MAGIC   result_review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total number of query appeals for the given result up to (and including) the query appeal status date.',
# MAGIC   review_query_appeals_day_total bigint
# MAGIC     comment 'The total number of query appeals for the previous appeal status date.',
# MAGIC   review_query_appeals_day_previous_total bigint
# MAGIC     comment 'The total number of query appeals for the previous appeal status date.',
# MAGIC   review_query_appeals_rolling_total bigint
# MAGIC     comment 'The total number of query appeals up to (and including) the query appeal status date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_result_review_query_appeal_status_metrics'
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

# DBTITLE 1,tmog_metrics_status_review_query_appeal_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_status_review_query_appeal_metrics (
# MAGIC   initial_review_query_appeal_date date comment 'The date of the initial review query appeal.',
# MAGIC   review_query_appeal_status string comment 'The status of the review query appeal.',
# MAGIC   review_query_appeal_status_description string comment 'The status description of the review query appeal.',
# MAGIC   status_review_query_appeals_day_total bigint comment 'The total number of query appeals for the status for the initial query appeal date.',
# MAGIC   status_review_query_appeals_day_previous_total bigint comment 'The total number of query appeals for the status for the previous query appeal date.',
# MAGIC   status_review_query_appeals_rolling_total bigint comment 'The total number of query appeals for the status (up to and including) the initial query appeal date.',
# MAGIC   review_query_appeals_day_total bigint comment 'The total number of query appeals for the review query date.',
# MAGIC   review_query_appeals_day_previous_total bigint comment 'The total number of query appeals for the previous review query date.',
# MAGIC   review_query_appeals_rolling_total bigint comment 'The total number of query appeals (up to and including) the initial query appeal date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_status_review_query_appeal_metrics'
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

# DBTITLE 1,tmog_metrics_status_review_query_appeal_status_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_status_review_query_appeal_status_metrics (
# MAGIC   review_query_appeal_status_date date comment 'The initial query review status date.',
# MAGIC   review_query_appeal_status string comment 'The status of the review query appeal.',
# MAGIC   review_query_appeal_status_description string comment 'The status description of the review query appeal.',
# MAGIC   status_review_query_appeals_day_total bigint comment 'The total number of query appeals for the status for the initial query appeal date.',
# MAGIC   status_review_query_appeals_day_previous_total bigint comment 'The total number of query appeals for the status for the previous query appeal date.',
# MAGIC   status_review_query_appeals_rolling_total bigint comment 'The total number of query appeals for the status (up to and including) the initial query appeal date.',
# MAGIC   review_query_appeals_day_total bigint comment 'The total number of query appeals for the review query date.',
# MAGIC   review_query_appeals_day_previous_total bigint comment 'The total number of query appeals for the previous review query date.',
# MAGIC   review_query_appeals_rolling_total bigint comment 'The total number of query appeals (up to and including) the initial query appeal date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_status_review_query_appeal_status_metrics'
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

# MAGIC %md
# MAGIC ### Gold Level Grounds

# COMMAND ----------

# DBTITLE 1,tmog_metrics_employee_review_query_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_employee_review_query_ground_metrics (
# MAGIC   initial_review_date date comment 'The date of the initial review.',
# MAGIC   initial_review_employee_id string comment 'The employee ID associated with the review query.',
# MAGIC   employee_review_queries_day_total bigint
# MAGIC     comment 'The total number of review queries for the employee for the initial review date.',
# MAGIC   employee_review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of review queries for the employee for the previous review date.',
# MAGIC   employee_review_queries_rolling_total bigint
# MAGIC     comment 'The total number of review queries for the employee up to (and including) the initial review query date.',
# MAGIC   review_queries_day_total bigint
# MAGIC     comment 'The total number of reivew queries for the initial review date.',
# MAGIC   review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of queries on the previous review query date.',
# MAGIC   review_queries_rolling_total bigint
# MAGIC     comment 'The total number of queries up to (and including) the initial review query date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_employee_review_query_ground_metrics'
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

# DBTITLE 1,tmog_metrics_review_query_ground_class_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_review_query_ground_class_metrics (
# MAGIC   initial_review_query_date date comment 'The date that the initial review occured.',
# MAGIC   ground_class_id string comment 'The class ID associated with the query ground.',
# MAGIC   class_number string comment 'The class number associated with the class ID.',
# MAGIC   class_schedule_code string comment 'The schedule code associated with the class.',
# MAGIC   goods_and_services_category string
# MAGIC     comment 'The categorization of whether the object is a good or service.',
# MAGIC   class_review_queries_day_total bigint
# MAGIC     comment 'The total number of class queries for the initial review date.',
# MAGIC   class_review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of class queries for the previous review date.',
# MAGIC   class_review_queries_rolling_total bigint
# MAGIC     comment 'The total number of queries for a specific class up to (and including) the initial review query date.',
# MAGIC   review_queries_day_total bigint comment 'The total queries that occured for that review date.',
# MAGIC   review_queries_day_previous_total bigint
# MAGIC     comment 'The total queries that occured for the previous review date. This is helpful for measuring changes and seasonality over time.',
# MAGIC   review_queries_rolling_total bigint
# MAGIC     comment 'The total number of review queries up to (and including) the initial review query date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_review_query_ground_class_metrics'
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

# DBTITLE 1,tmog_metrics_review_query_ground_type_metrics
# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.gold.tmog_metrics_review_query_ground_type_metrics (
# MAGIC   initial_review_query_date date comment 'The date that the initial review occured.',
# MAGIC   ground_type string comment 'The ground type associated with the given query.',
# MAGIC   ground_type_review_queries_day_total bigint
# MAGIC     comment 'The total number of queries for the given ground type.',
# MAGIC   ground_type_review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of queries for the given ground type for the previous review query date.',
# MAGIC   ground_type_review_queries_rolling_total bigint
# MAGIC     comment 'The total number of queries for the given ground type up to (and including) the initial review query date.',
# MAGIC   review_queries_day_total bigint
# MAGIC     comment 'The total number of queries for the given review query date.',
# MAGIC   review_queries_day_previous_total bigint
# MAGIC     comment 'The total number of queries for the given previous review query date.',
# MAGIC   review_queries_rolling_total bigint
# MAGIC     comment 'The total number of review queries up to (and including) the initial review query date.',
# MAGIC   create_user string comment 'The user that created the record.',
# MAGIC   create_timestamp timestamp comment 'The timestamp that the record was created.'
# MAGIC )
# MAGIC   using delta
# MAGIC   comment 'The table contains metrics related to TMOG data.'
# MAGIC   location 's3://${conf.cdc_bucket}/delta_tables/${conf.catalog}/gold/tmog_metrics_review_query_ground_type_metrics'
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

# DBTITLE 1,TMOG Metrics Table Verification
# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ${conf.catalog}.information_schema.tables
# MAGIC where
# MAGIC   startswith(table_name, 'tmog_metrics')
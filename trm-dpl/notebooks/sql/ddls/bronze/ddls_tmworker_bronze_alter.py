# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmworker-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmworker_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmworker_catalog=}, {data_quality_catalog=} ')

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'

spark.conf.set('conf.catalog', tmworker_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

tables_to_comment = {
    
    
    'worker_role_h': 'The worker_role_h table stores information about the roles assigned to workers in the organization. It contains data related to the effective dates of the roles, the user IDs responsible for creating and modifying the roles, and the timestamps of these actions. The table also includes a control number for locking purposes and an action count field. Additionally, it references other tables to maintain data integrity and consistency.',
    
    'worker_role': 'The worker_role table contains information about the roles assigned to workers in the organization. It represents the relationship between workers and their roles, including the effective dates of the role assignments. This table is important for tracking the roles of workers and their historical changes over time. It also includes information about the creation and modification of role assignments, such as timestamps and user IDs. The table does not provide specific details about the columns, but rather focuses on the overall function and significance of the data it contains.',
    
    'worker_h': 'The worker_h table contains data related to workers. It includes information such as worker IDs, names, grades, signatory authorities, user IDs, active status, worker counts, patron IDs, email addresses, effective dates, lock control numbers, creation and modification timestamps, and action counts. This table is significant to the business as it provides a comprehensive record of workers and their associated details, allowing for efficient management and analysis of workforce-related information.',
    
    'worker': 'The worker table contains data related to the workers of the TM business.  It provides information about worker_gid,	worker_no,	worker_nm,	grade_ct,	signatory_authority_ct,	brs_user_id, active_in	worker_ct, cfk_patron_id,	email_address_tx,	begin_effective_dt,	end_dt,	lock_control_no,	create_ts	create_user_id,	last_mod_ts,	last_mod_user_id,	grade_step_ct.  This is an important table for connecting TM employees to actions taken on trademarks.',  

    'user_role_group': 'The user_role_group table contains information about the different groups of user roles within the business. It includes the code, title, and description of each role group, as well as the effective dates for when the role group is valid. The table also includes information about any locks on the role group, as well as timestamps for when the role group was created and last modified, along with the corresponding user IDs. This table is essential for managing and organizing user roles within the business.',
    
    'user_role': 'The user_role table stores information about the different roles that users can have within the business. Each role is identified by a unique ID and a corresponding code. The table also includes a title and description for each role, providing further details about its purpose. The begin and end effective dates indicate the period during which the role is active. The table also tracks the creation and modification timestamps, as well as the user IDs responsible for those actions.',
    
    'transaction_instance': 'The transaction_instance table contains data related to individual instances of transactions in tmworker. It captures information such as the transaction code, employee number, unique identifiers for each transaction instance, effective timestamp, details of the transaction, termination status, origin location, creation timestamp, and user IDs for creation and last modification. This table is significant for tracking and analyzing transaction activities, identifying transaction patterns, and monitoring transaction performance. It provides valuable insights into the history and characteristics of each transaction instance in the system.',
    
    'tm_organization_rltnshp': 'The tm_organization_rltnshp table represents the relationships between parent and child organizations in the business. It contains information about the parent organization, child organization, lock control number, creation timestamp, creation user ID, last modification timestamp, last modification user ID, begin effective date, and end effective date. This table is significant to the business as it helps track and manage the hierarchical relationships between different organizations within the company. ',

    'tm_organization': 'The tm_organization table contains data related to the offices in the TM business organization. It includes information such as a unique organization gid, a foriegn key id, the organization code, organization name, a description of the organization, email address, a lock control number, begin and end effective dates for the org code, create and last modified timestamps and user ids. This table is used to identify the various organizations within Trademarks.',

    'sync_translate_location': 'The sync_translate_location table contains information about each law office. It includes the law office code, palm short code, translation text, group code, a boolean active indicator, email and TM Organization code. This table is used to translate the law office information contatined in the various codes.',

    'cdc_batch_job_control': 'The cdc_batch_job_control table in the bronze schema of the trm_tmworker catalog is used to control and track the Change Data Capture (CDC) batch jobs. It contains information about the source folder, catalog name, database name, and table name for each job. Additionally, it includes the source database name and source table name, which represent the original data source. The primary keys column stores the primary keys for each table, while the full_load column indicates whether a full load is required. The initial_load_finished column is a boolean value that shows whether the initial load has been completed. This table is essential for managing and monitoring the CDC process in the business system.',

    'cdc_batch_job_history' : 'The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs. It stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. This table is significant to the business as it allows tracking and monitoring of CDC batch job activities, providing insights into data changes and updates. The data in this table represents the historical records of CDC batch jobs, enabling analysis and troubleshooting of data synchronization processes.'
}

for table_name, comment in tables_to_comment.items():
    alter_table_query = f"""
    ALTER TABLE {spark.conf.get('conf.catalog')}.{spark.conf.get('conf.database')}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)



# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'FK_USER_ROLE_ID':'Foreign key referencing the unique identifier of the user role id.',
'FK_TM_ORGANIZATION_GID':'Foreign key referencing the unique identifier of the tm organization.',
'FK_WORKER_GID':'Foreign key referencing the unique identifier of the worker.',
'BEGIN_EFFECTIVE_DT':'Date and time when the records effectiveness begins.',
'END_EFFECTIVE_DT':'Date and time when the records effectiveness ends.',
'LOCK_CONTROL_NO':'	Numeric control number for locking purposes.',
'CREATE_TS':'The timestamp of the record when it was created.',
'CREATE_USER_ID':'User ID of the user who created the record.',
'LAST_MOD_TS':'The timestamp of when the record was last modified.',
'LAST_MOD_USER_ID':'User ID of the user who last modified the record.',
'ACTION_CT':'Text description of the action.',
'CFK_TRANSACTION_INSTANCE_GID':'Foreign key referencing the unique identifier of the transaction instance.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='worker_role_h', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'FK_USER_ROLE_ID':'Foreign key referencing the unique identifier of the user role id.',
'FK_TM_ORGANIZATION_GID':'Foreign key referencing the unique identifier of the tm organization.',
'FK_WORKER_GID':'Foreign key referencing the unique identifier of the worker id.',
'BEGIN_EFFECTIVE_DT':'Date and time when the records effectiveness begins.',
'END_EFFECTIVE_DT':'Date and time when the records effectiveness ends.',
'LOCK_CONTROL_NO':'Numeric control number for locking purposes.',
'CREATE_TS':'The timestamp of the record when it was created.',
'CREATE_USER_ID':'User ID of the user who created the record.',
'LAST_MOD_TS':'The timestamp of when the record was last modified.',
'LAST_MOD_USER_ID':'User ID of the user who last modified the record.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='worker_role', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'WORKER_GID':'The unique identifier of a worker',
'WORKER_NO':'The workers employee id.',
'WORKER_NM':'The workers name.',
'GRADE_CT':'The workers grade level.',
'SIGNATORY_AUTHORITY_CT':'A code representing the workers signatory authority level.',
'BRS_USER_ID':'The workers network id.',
'ACTIVE_IN':'Boolean flag indicating if the worker is active.  ',
'WORKER_CT':'A flag identifying the type of account the record represents.  ',
'CFK_PATRON_ID':'Foreign key referencing the unique identifier of a patron id',
'EMAIL_ADDRESS_TX':'The workers email_address.',
'BEGIN_EFFECTIVE_DT':'Date and time when the records effectiveness begins.',
'END_DT':'Date and time when the records effectiveness ends.',
'LOCK_CONTROL_NO':'Numeric control number for locking purposes.',
'CREATE_TS':'The timestamp of the record when it was created.',
'CREATE_USER_ID':'User ID of the user who created the record.',
'LAST_MOD_TS':'The timestamp of when the record was last modified.',
'LAST_MOD_USER_ID':'User ID of the user who last modified the record.',
'BEGIN_EFFECTIVE_TS':'The workers begin_effective level.',
'END_EFFECTIVE_TS':'The workers end_effective level.',
'ACTION_CT':'Text description of the action.',
'CFK_TRANSACTION_INSTANCE_GID':'Foreign key referencing the unique identifier of the transaction instance.',
'GRADE_STEP_CT':'The workers grade step level.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='worker_h', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'worker_gid':'The unique identifier of a worker',
'worker_no':'The workers employee id.',
'worker_nm':'The workers name.',
'grade_ct':'The workers grade level.',
'signatory_authority_ct':'A code representing the workers signatory authority level.',
'brs_user_id':'The workers network id.',
'active_in':'Boolean flag indicating if the worker is active.  ',
'worker_ct':'A flag identifying the type of account the record represents.  ',
'cfk_patron_id':'Foreign key referencing the unique identifier of a patron id',
'email_address_tx':'The workers email_address.',
'begin_effective_dt':'Date and time when the records effectiveness begins.',
'end_dt':'Date and time when the records effectiveness ends.',
'lock_control_no':'Numeric control number for locking purposes.',
'create_ts':'The timestamp of the record when it was created.',
'create_user_id':'User ID of the user who created the record.',
'last_mod_ts':'The timestamp of when the record was last modified.',
'last_mod_user_id':'User ID of the user who last modified the record.',
'grade_step_ct':'The workers grade step level.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='worker', column_name=column, comment=comment)

  spark.sql(column_comment_query)


# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'USER_ROLE_GROUP_CD':'The usser role group code.',
'TITLE_TX':'The title of the group.',
'DESCRIPTION_TX':'A description of the group.',
'BEGIN_EFFECTIVE_DT':'Date and time when the records effectiveness begins.',
'END_EFFECTIVE_DT':'Date and time when the records effectiveness ends.',
'LOCK_CONTROL_NO':'Numeric control number for locking purposes.',
'CREATE_TS':'The timestamp of when the record was created.',
'CREATE_USER_ID':'User ID of the user who created the record.',
'LAST_MOD_TS':'The timestamp of when the record was last modified.',
'LAST_MOD_USER_ID':'User ID of the user who last modified the record.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='user_role_group', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'USER_ROLE_ID':'The unique identifier of a role.',
'USER_ROLE_CD':'The user role code.',
'TITLE_TX':'The role title.',
'DESCRIPTION_TX':'Description of the role.',
'BEGIN_EFFECTIVE_DT':'Date and time when the records effectiveness begins.',
'END_EFFECTIVE_DT':'Date and time when the records effectiveness ends.',
'LOCK_CONTROL_NO':'Numeric control number for locking purposes.',
'CREATE_TS':'The timestamp of when the record was created.',
'CREATE_USER_ID':'User ID of the user who created the record.',
'LAST_MOD_TS':'The timestamp of when the record was last modified.',
'LAST_MOD_USER_ID':'User ID of the user who last modified the record.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='user_role', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'FK_LEGACY_TRANSACTION_CD':'Foreign key referencing the unique identifier of the legacy transaction code.',
'CFK_EMPLOYEE_NO':'Foreign key referencing the unique identifier of the employee.',
'TRANSACTION_INSTANCE_GID':'The global identifier of a transaction instance.',
'TRANSACTION_INSTANCE_ID':'The unique identifier of a transaction instance.',
'EFFECTIVE_TS':'Date and time when the records effectiveness starts.',
'DETAILS_TX':'The transaction details.',
'TERMINATED_IN':'Boolean transaction termination indicator.',
'ORIGIN_LOCATION_TX':'Source of the transaction.',
'CREATE_TS':'The timestamp of when the record was created.',
'CREATE_USER_ID':'User ID of the user who created the record.',
'LAST_MOD_TS':'The timestamp of when the record was last modified.',
'LAST_MOD_USER_ID':'User ID of the user who last modified the record.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='transaction_instance', column_name=column, comment=comment)

  spark.sql(column_comment_query)


# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'FK_PARENT_TM_ORGANIZATION_GID':'Foreign key referencing the global identifier of the parent TM organization.',
'FK_CHILD_TM_ORGANIZATION_GID':'Foreign key referencing the global identifier of the child TM organization.',
'LOCK_CONTROL_NO':'Numeric control number for locking purposes.',
'CREATE_TS':'The timestamp of when the record was created.',
'CREATE_USER_ID':'User ID of the user who created the record.',
'LAST_MOD_TS':'The timestamp of when the record was last modified.',
'LAST_MOD_USER_ID':'User ID of the user who last modified the record.',
'BEGIN_EFFECTIVE_DT':'Date and time when the records effectiveness begins.',
'END_EFFECTIVE_DT':'Date and time when the records effectiveness ends.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='tm_organization_rltnshp', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'tm_organization_gid':'The global identifier of the TM organization.',
'cfk_organization_id':'Foreign key referencing the unique identifier of the tm organization.',
'organization_cd':'The code representing the organization in Trademarks.',
'organization_nm':'The name of the TM organization.',
'description_tx':'The description of the TM orgranization.',
'email_address_tx':'The email address of the TM organization.',
'begin_effective_dt':'Date and time when the records effectiveness begins.',
'end_effective_dt':'Date and time when the records effectiveness ends.',
'lock_control_no':'Numeric control number for locking purposes.',
'create_ts':'The timestamp of when the record was created.',
'create_user_id':'User ID of the user who created the record.',
'last_mod_ts':'The timestamp of when the record was last modified.',
'last_mod_user_id':'User ID of the user who last modified the record.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='tm_organization', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'LAW_OFFICE_CD':'The internal law office code.  ',
'PALM_SHORT_CD':'The external Law Office code.',
'TT_TEXT':'Description of the Law Office.',
'GROUP_CD':'Which group the Law Office is part of if applicable.',
'ACTIVE_IND':'Boolean flag indicating if the Law Office is active.  ',
'EMAIL_TX':'The email address of the Law Office.',
'TM_ORGANIZATION_GID':'The global identifier of the TM organization.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='sync_translate_location', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'src_folder':'The folder where the source data is stored.',
'catalog_name':'The name of the catalog where the table is located.',
'database_name':'The name of the database where the table is located.',
'table_name':'The name of the table.',
'source_db_name':'The name of the source database.',
'source_table_name':'The name of the source table.',
'primary_keys':'The primary keys of the table.',
'full_load':'Indicates if the table needs to be fully loaded.',
'initial_load_finished':'Indicates if the initial load of the table has finished.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='cdc_batch_job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------



# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'cdc_file_path':'The file path of the CDC file.',
'meta_src_time':'The source time of the metadata.',
'cdc_file_date':'The date of the CDC file.',
'processing_time':'The processing time of the job.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmworker_catalog, database=database, table='cdc_batch_job_history', column_name=column, comment=comment)

  spark.sql(column_comment_query)


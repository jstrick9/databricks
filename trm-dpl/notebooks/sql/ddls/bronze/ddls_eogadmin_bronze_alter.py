# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file="../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/eogadmin-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
    print(f'{dbx_env=}')
else:
    dbutils.widgets.text("env", dbx_env)
    print(f'{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs=read_yaml(config_file)
eogadmin_catalog = common_configs['schema']['trgt_catalog']
src_folder=common_configs['cdc']['src_csv_files']
src_database=common_configs['cdc']['src_database']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{eogadmin_catalog=}, {data_quality_catalog=} ')

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
spark.conf.set('conf.catalog', eogadmin_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.src_folder', src_folder)
spark.conf.set('conf.src_database', src_database)

# COMMAND ----------

tables_to_comment = {
    'cdc_batch_job_control':'The cdc_batch_job_control table in the bronze schema of the trm_tmbuscalendar_dev catalog is used to control and track the Change Data Capture (CDC) batch jobs. It contains information about the source folder, catalog name, database name, and table name for each job. Additionally, it includes the source database name and source table name, which represent the original data source. The primary keys column stores the primary keys for each table, while the full_load column indicates whether a full load is required. The initial_load_finished column is a boolean value that shows whether the initial load has been completed. This table is essential for managing and monitoring the CDC process in the business system.',

    'cdc_batch_job_history':'The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs. It stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. This table is significant to the business as it allows tracking and monitoring of CDC batch job activities, providing insights into data changes and updates. The data in this table represents the historical records of CDC batch jobs, enabling analysis and troubleshooting of data synchronization processes.',

    'fsm_instance':'The business instances of finite state machines (FSMs) are documented in the "fsm_instance" table. It keeps track of the current FSM type and state, as well as information about the parent and root FSM instances. The table additionally incorporates insights regarding end, suspension, and profundity of the FSM occurrences. Instance creation and last modification timestamps, as well as the associated user IDs, are recorded. The management and tracking of the various FSM instances within the business processes requires this table.',

    'fsm_instance_h':'The fsm_instance_h table in the bronze construction of the trm_eogadmin_dev list stores verifiable information connected with FSM (Limited State Machine) cases. It includes information about the FSM instance hierarchy, including the IDs of the parent and root FSM instances. The table likewise incorporates insights concerning the FSM type and its present status, as well as data on end, suspension, and profundity. Timestamps are recorded for the effective period of each record, as well as for the creation and modification of records.',

    'fsm_interlock':'The fsm_interlock table contains information connected with interlocks in a field administration the board framework. It represents the various kinds of interlocks, as well as the various types of triggers, their associated root types, and trigger states. Additionally, there are descriptions for each interlock in the table. When the interlocks were created and last modified, the timestamps and user IDs indicate. The management and tracking of interlocks in the company field service operations requires this table.',

    'og_appeal_fsm_instance':'The og_appeal_fsm_instance table in the trm_eogadmin_dev database schema represents the instances of a finite state machine (FSM) used for managing appeals in the business. It stores the unique identifiers for the root FSM instance, the current FSM instance, and the review query appeal. Additionally, it tracks the timestamps and user IDs for when the instances were created and last modified. This table is crucial for tracking and managing the appeal process within the business.',

    'og_review_fsm_instance':'The og_review_fsm_instance table in the trm_eogadmin_dev database schema represents the instances of trademark review processes in the business. It contains information about the root and current instances of the review process, along with timestamps for creation and last modification. The table also includes user IDs for the creation and last modification actions. Additionally, it includes a foreign key to the og_trademark_review table, which represents the specific trademark review associated with each instance. Overall, this table provides a historical record of trademark review instances and their associated details.',

    'og_review_query_fsm_instance':'The og_review_query_fsm_instance table stores information about the instances of review queries in the system. It tracks the current and root FSM instance IDs, as well as the timestamps and user IDs for when the instances were created and last modified. This table is significant to the business as it allows for the tracking and management of review queries, providing insights into their lifecycle and history within the system.',

    'qrtz_blob_triggers':'The qrtz_blob_triggers table in the bronze schema of the trm_eogadmin_dev catalog stores information about blob triggers. Blob triggers are used in the scheduling system to execute tasks based on certain events or conditions. This table contains data related to the name and group of the trigger, as well as any associated blob data. The blob data can be used to store additional information or instructions for the trigger. The table is essential for managing and tracking blob triggers within the business scheduling system',

    'qrtz_calendars':'The qrtz_calendars table stores information about calendars used in scheduling. Calendars are used to define non-working days or time slots for specific events or resources. This table contains the name of the scheduler, the name of the calendar, and the binary representation of the calendar. The calendar data is used to determine when certain events or resources should be excluded from scheduling. The table provides a way to manage and reference calendars within the scheduling system.',

    'qrtz_cron_triggers':'The qrtz_cron_triggers table stores information about cron triggers in the system. Cron triggers are used to schedule jobs to run at specific times or intervals. This table contains details such as the name of the trigger, the group it belongs to, the cron expression that defines the schedule, and the time zone in which the trigger operates. The data in this table is crucial for managing and tracking scheduled jobs in the business.',

    'qrtz_fired_triggers':'The qrtz_fired_triggers table stores information about the fired triggers in the scheduling system. It contains data related to the name and group of the trigger, the name and group of the associated job, the instance name, the fired time, the scheduled time, the priority, the state of the trigger, and flags indicating if the job is non-concurrent and if it requires recovery. This table is important for tracking the execution status of triggers and jobs in the system.',

    'qrtz_job_details':'The qrtz_job_details table contains information about the scheduled jobs in the system. It provides details such as the name, group, and description of each job, as well as the class name of the job implementation. The table also includes flags indicating whether the job is durable, non-concurrent, or requires recovery. Additionally, there is a field for storing binary job data. This table is essential for managing and monitoring scheduled jobs within the business application.',

    'qrtz_locks':'The qrtz_locks table in the bronze schema of the trm_eogadmin_dev catalog stores information about locks used by the scheduling system. This table is significant to the business as it helps in managing and controlling concurrent access to resources. The sched_name column represents the name of the scheduler that acquired the lock, while the lock_name column represents the name of the lock itself. The data in this table provides insights into the usage and availability of locks, which is crucial for ensuring efficient and reliable scheduling operations.',

    'qrtz_paused_trigger_grps':'The qrtz_paused_trigger_grps table stores information about paused trigger groups in the scheduling system. It is used to keep track of which trigger groups have been manually paused, allowing the system to prevent triggers within those groups from firing. The table contains data related to the name of the scheduler and the name of the paused trigger group. This information is crucial for managing and controlling the execution of scheduled tasks in the business application.',

    'qrtz_scheduler_state':'The qrtz_scheduler_state table stores information about the state of the schedulers in the system. It includes the name of the scheduler, the name of the instance, the last check-in time, and the check-in interval. This table is important for monitoring and managing the schedulers, as it provides insights into their activity and health. The last check-in time and check-in interval help to track the responsiveness and availability of the schedulers. Overall, this table plays a crucial role in ensuring the smooth operation of the scheduling system.',

    'qrtz_simple_triggers':'The qrtz_simple_triggers table stores information about simple triggers in the scheduling system. Simple triggers are used to schedule jobs to run at specific intervals or a specific number of times. This table contains data such as the name and group of the trigger, the number of times the trigger has been repeated, and the interval at which the trigger repeats. The data in this table is essential for tracking and managing scheduled jobs in the business.',

    'qrtz_simprop_triggers':'The qrtz_simprop_triggers table in the bronze schema of the trm_eogadmin_dev catalog contains data related to simulated property triggers. This table is used to store information about triggers that are associated with simulated properties. The table includes columns for trigger names, trigger groups, and various properties such as string properties, integer properties, long properties, decimal properties, and boolean properties. The data in this table is important for tracking and managing simulated property triggers within the business.',

    'qrtz_triggers':'The qrtz_triggers table stores information about triggers in the system. Triggers represent a scheduled time for a job to be executed. This table contains data such as the name and group of the trigger, the associated job, the trigger description, the next and previous fire times, the trigger priority, state, and type, as well as the start and end times. Additionally, it includes information about the calendar associated with the trigger, the misfire instruction, and any job-specific data. This table is essential for managing and tracking scheduled jobs within the business.',

    'stnd_domain':'The stnd_domain table in the bronze schema of the trm_eogadmin_dev catalog contains information about different domains. Each row represents a specific domain and includes details such as the domain code, title, description, effective dates, and user information for creation and modification. This table is significant to the business as it provides a centralized repository for managing and organizing domains, allowing for easy reference and retrieval of domain-related information.',

    'stnd_fsm_category':'The stnd_fsm_category table in the bronze schema of the trm_eogadmin_dev catalog represents the standard categories for Field Service Management (FSM) activities. It contains information about the category code, title, and description of each category. Additionally, it includes the effective dates for when each category is valid, as well as timestamps for when the records were created and last modified. This table is essential for organizing and categorizing FSM activities within the business.',

    'stnd_fsm_interlock':'The stnd_fsm_interlock table stores information related to interlocks in the field service management system. It contains data that represents the various types of interlocks, their descriptions, and the triggers associated with them. The table also includes timestamps for when the interlocks were created and last modified, as well as the user IDs of the individuals who made those changes. This table is essential for tracking and managing interlocks within the business operations.',

    'stnd_fsm_interlock_type':'The stnd_fsm_interlock_type table contains information about the different types of interlocks used in the field service management system. It provides a standardized list of interlock types along with their corresponding title and description. The table also includes timestamps for when the interlock types were created and last modified, as well as the user IDs of the individuals who made those changes. This data is essential for tracking and managing interlocks within the business, ensuring consistency and accuracy in the field service management system.',

    'stnd_fsm_type':'The stnd_fsm_type table contains information about different types of finite state machines (FSMs) used in the business. Each FSM type is associated with a category and can have a precedent FSM type, initial FSM type state, and root FSM type. The table also includes a title and description for each FSM type, as well as timestamps for when the FSM type was created and last modified. This table is essential for managing and tracking the various FSMs used in the business processes.',

    'stnd_fsm_type_event':'The stnd_fsm_type_event table contains information about different types of events in the FSM system. Each row represents a specific event type and includes details such as the event title, description, creation timestamp, and the user who created or last modified the event type. This table is essential for managing and categorizing events within the FSM system, allowing users to easily identify and track different types of events.',

    'stnd_fsm_type_state':'The stnd_fsm_type_state table in the bronze schema of the trm_eogadmin_dev catalog contains data related to the different states of a finite state machine (FSM) type. It provides information about the FSM type, its root FSM type, and the title, description, and activities associated with each state. The table also includes timestamps for when the states were created and last modified, as well as the user IDs responsible for those actions. Additionally, it includes a start condition for each state. This table is essential for managing and tracking the various states and their properties within the FSM system.',

    'stnd_fsm_type_state_rule':'The stnd_fsm_type_state_rule table contains information about the rules that govern the state transitions for different types of finite state machines (FSMs) in the business. It stores the IDs of the FSM type, root FSM type, current FSM type state, next FSM type state, and FSM type event associated with each rule. The table also includes descriptions, preconditions, and rule actions for each rule. Additionally, it tracks the timestamps and user IDs for when the rules were created or last modified. This table is crucial for managing and enforcing the logic and behavior of FSMs within the business processes.',

    'stnd_interlock_type':'Thestnd_interlock_type table contains information about the different types of interlocks used in the business. It provides a standardized code for each interlock type, along with a title and description. The table also includes timestamps for when each record was created and last modified, as well as the corresponding user IDs. This table is essential for categorizing and managing interlocks within the business operations.',

    'user_profile':'The user_profile table stores information about user profiles in the system. Each profile is associated with a unique user ID and has a profile name and description. The table also tracks the creation and modification timestamps, as well as the respective user IDs who performed these actions. This table is essential for managing and organizing user profiles, allowing the business to provide personalized experiences and targeted services to its users.',

    'user_profile_preference':'The user_profile_preference table stores preferences for user profiles in the business system. It contains information about the user profile, the domain, the resource, and the preference value. The table also includes timestamps for when the preferences were created and last modified, as well as the corresponding user IDs. The user_profile_preference_id column serves as the primary key for this table. This table is essential for managing and customizing user preferences within the business system.'
}
for table_name, comment in tables_to_comment.items():
    alter_table_query = f"""
    ALTER TABLE {spark.conf.get('conf.catalog')}.{spark.conf.get('conf.database')}.{table_name}
    SET TBLPROPERTIES ('comment' = '{comment}')
    """
    spark.sql(alter_table_query)

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
  """.format(catalog=eogadmin_catalog, database=database, table='cdc_batch_job_history', column_name=column, comment=comment)

  spark.sql(column_comment_query)

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
  """.format(catalog=eogadmin_catalog, database=database, table='cdc_batch_job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_instance_id':'FSM Instance Id',
    'fk_parent_fsm_instance_id':'Parent FSM Instance ID.',
    'fk_root_fsm_instance_id':'Root FSM Instance ID',
    'fk_fsm_type_id':'Type of FSM Instance.',
    'fk_current_fsm_type_state_id':'Current FSM state type.',
    'terminated_in':'Termaination status of FSM Instance.',
    'suspended_no':'Suspension count number of FSM Instance.',
    'depth_no':'Depth number of FSM Instance.',
    'create_ts':'Timestamp when FSM Instance was created.',
    'create_user_id':'User Id of the user who created Instance.',
    'last_mod_ts':'Timestamp when Instance was last modified.',
    'last_mod_user_id':'User ID of user who last modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='fsm_instance', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_instance_h_id':'FSM Instance historical data Id.',
    'fsm_instance_id':'FSM Insatnce Id.',
    'fk_parent_fsm_instance_id':'Parent FSM Instance Id.',
    'fk_root_fsm_instance_id':'Root Id of FSM Instance.',
    'fk_fsm_type_id':'Type of FSM Instance ID',
    'fk_current_fsm_type_state_id':'Current FSM Instance state type.',
    'terminated_in':'Fsm Instance terminated in.',
    'suspended_no':'FSM suspended number.',
    'depth_no':'Depth of FSM instance.',
    'create_ts':'Timestamp craeted when Fsm Instance historical data is created.',
    'create_user_id':'User id of a user who created historical data.',
    'last_mod_ts':'Last modified timestamp.',
    'last_mod_user_id':'User id of user who last modified FSM instance historical data.',
    'end_effective_ts':'Effective time range for each instance when it ended.',
    'begin_effective_ts':'Effective time range for each instance when it started.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='fsm_instance_h', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_interlock_id':'Fsm Interlock id.',
    'fk_fsm_interlock_type_id':'Types of Interlocks in Fsm instance Id.',
    'fk_fsm_root_type_id':'Root type of interlock Id.',
    'fk_fsm_trigger_type_id':'Trigger type of interlock Id.',
    'fk_fsm_trigger_state_id':'Trigger state of Fsm interlock Id',
    'stnd_interlock_type_cd':'Standard interlock type.',
    'interlock_description_tx':'Description of each interlock.',
    'create_ts':'Timestamp when Fsm Interlock was created.',
    'create_user_id':'User id of user who created Fsm Interlock.',
    'last_mod_ts':'Last modified timestamp when Fsm interlock was chnaged recently.',
    'last_mod_user_id':'User Id of user who last modified Interlock Id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='fsm_interlock', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'cfk_root_fsm_instance_id':'Root FSM instance ID.',
    'cfk_current_fsm_instance_id':'Current FSM instance ID.',
    'cfk_review_query_appeal_id':'Review query appeal ID associated with each appeal.',
    'create_ts':'Timestamp created when Root fsm instance id was created.',
    'create_user_id':'User Id of user who created ID.',
    'last_mod_ts':'Last modified Timestamp.',
    'last_mod_user_id':'User Id of user who last modified instance Id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='og_appeal_fsm_instance', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'cfk_root_fsm_instance_id':'Root instances of the review process.',
    'cfk_current_fsm_instance_id':'Current instance of the review process.',
    'create_ts':'Timestamp of when the instances of trademark review process were created.',
    'create_user_id':'User If of user who created.',
    'last_mod_ts':'Last modified timestamp when it was changed.',
    'last_mod_user_id':'User id of user who last modified or changed.',
    'cfk_og_trademark_review_id':'Represents the unique identifier of the trademark review associated with each instance.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='og_review_fsm_instance', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'cfk_current_fsm_instance_id':'Current FSM instance Id',
    'cfk_review_query_id':'Instances of review queries in the system.',
    'create_ts':'Timestamp created when instances of review queries in the system.',
    'create_user_id':'User id of user who created instances of review queries in the system.',
    'last_mod_ts':'Last modified timestamp when modified.',
    'last_mod_user_id':'User id of user who last modified.',
    'cfk_root_fsm_instance_id':'Root FSM instance Id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='og_review_query_fsm_instance', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled name of the blob trigger.',
    'trigger_name':'Blob trigger name.',
    'trigger_group':'Group of the trigger.',
    'blob_data':'Binary data that is associated with the trigger.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_blob_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Name of the schedular.',
    'calendar_name':'Name of the calendar.',
    'calendar':'Calendar data itself.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_calendars', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled name of the trigger.',
    'trigger_name':'Trigger name.',
    'trigger_group':'Group of the trigger.',
    'cron_expression':'Cron expression that define the schedule.',
    'time_zone_id':'Time zone in which trigger operates.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_cron_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled name of the trigger.',
    'entry_id':'Entry id of the trigger.',
    'trigger_name':'Name of the trigger.',
    'trigger_group':'Group of the trigger.',
    'instance_name':'Instance name',
    'fired_time':'Fired time of fired trigger.',
    'sched_time':'Scheduled time.',
    'priority':'Priority of the fired trigger.',
    'state':'State of the trigger.',
    'job_name':'Name of the associated job.',
    'job_group':'Name of the group associated with job',
    'is_nonconcurrent':'If the job is nonconcurrent or concurrent.',
    'requests_recovery':'If the job requests recovery.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_fired_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Name of each job.',
    'job_name':'Name of each job.',
    'job_group':'Group of each job.',
    'description':'Description of each job.',
    'job_class_name':'Class name of the job implementation.',
    'is_durable':'Indicates weather job is durable.',
    'is_nonconcurrent':'Indicates weather job is nonconcurrent.',
    'is_update_data':'Update data of each job.',
    'requests_recovery':'If job requests and recovery.',
    'job_data':'Any data associated with Job.',
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_job_details', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled name.',
    'lock_name':'Lock name that is used by scheduling system.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_locks', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled name.',
    'trigger_group':'Trigger group of trigger in scheduling sytems.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_paused_trigger_grps', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Name of the schedular.',
    'instance_name':'Name of the instance.',
    'last_checkin_time':'Last check-in time about state of schedular.',
    'checkin_interval':'Check-in Interval about state of schedular.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_scheduler_state', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled name of trigger.',
    'trigger_name':'Name of the trigger.',
    'trigger_group':'Name of the trigger.',
    'repeat_count':'Repeat count for the trigger.',
    'repeat_interval':'Repeat interval for the trigger.',
    'times_triggered':'Number of times triggered.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_simple_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled Name.',
    'trigger_name':'Name of the trigger.',
    'trigger_group':'Group of the trigger.',
    'str_prop_1':'String properties associated with trigger.',
    'str_prop_2':'String properties associated with trigger.',
    'str_prop_3':'String properties associated with trigger.',
    'int_prop_1':'Integer properties associated with trigger.',
    'int_prop_2':'Integer properties associated with trigger.',
    'long_prop_1':'Long properties associated with trigger.',
    'long_prop_2':'Long properties associated with trigger.',
    'dec_prop_1':'Decimal properties associated with trigger.',
    'dec_prop_2':'Decimal properties associated with trigger.',
    'bool_prop_1':'Boolean properties associated with trigger.',
    'bool_prop_2':'Boolean properties associated with trigger.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_simprop_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'sched_name':'Scheduled Name.',
    'trigger_name':'Name of the trigger.',
    'trigger_group':'Group of the trigger.',
    'job_name':'Job name associated with trigger.',
    'job_group':'Job group Associated with trigger.',
    'description':'Description of the trigger.',
    'next_fire_time':'Next fire time of the trigger.',
    'prev_fire_time':'Previous fire time of the trigger.',
    'priority':'Priority of the trigger.',
    'trigger_state':'State of the trigger.',
    'trigger_type':'Type of the trigger.',
    'start_time':'Start time of the trigger.',
    'end_time':'End time of the trigger.',
    'calendar_name':'Calendar associated with trigger.',
    'misfire_instr':'Mis fire instruction of the trigger.',
    'job_data':'Any additional Job data related to trigger.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='qrtz_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'domain_cd':'Domain with unique code.',
    'title_tx':'Title of domain.',
    'description_tx':'Description for domain.',
    'begin_effective_dt':'Effective date for each domain.',
    'end_effective_dt':'End of effective date for each domain.',
    'create_ts':'Timestamp when domain added.',
    'create_user_id':'User Id who added domain.',
    'last_mod_ts':'Timestamp when last modified.',
    'last_mod_user_id':'User id of user who last modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_domain', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_category_cd':'Fsm category code.',
    'title_tx':'Title for Fsm category code.',
    'description_tx':'Description for Fsm category code.',
    'begin_effective_dt':'Effective date is time period when category was active.',
    'end_effective_dt':'End Effective date is time period when caterogry was not active.',
    'create_ts':'Timestamp when Fsm category was added.',
    'create_user_id':'User id of user who created.',
    'last_mod_ts':'Timestamp when last modified.',
    'last_mod_user_id':'User id of user who last modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_fsm_category', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_interlock_id':'Fsm system Interlock ID.',
    'fk_interlock_fsm_type_id':'Interlock FSM type ID.',
    'fk_root_fsm_type_id':'Root FSM type ID.',
    'fk_trigger_fsm_type_id':'Trigger FSM type ID.',
    'fk_trigger_fsm_type_state_id':'Trigger FSM type state ID.',
    'fk_fsm_interlock_type_cd':'FM interlock type code.',
    'description_tx':'Description associated with interlock.',
    'create_ts':'Timestamp for FSM Id when created.',
    'create_user_id':'User ID of user who created.',
    'last_mod_ts':'Last modified Timestamp.',
    'last_mod_user_id':'User id of user who last modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_fsm_interlock', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_interlock_type_cd':'Field service management Interlock type code.',
    'title_tx':'Title associated with Fsm interlock type.',
    'description_tx':'Description associated with Fsm type.',
    'create_ts':'Timestamp created when fsm interlock type is created.',
    'create_user_id':'User id of user who created.',
    'last_mod_ts':'Last modified timestamp.',
    'last_mod_user_id':'Last modified user Id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_fsm_interlock_type', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_type_id':'Finite state machines (FSMs) type ID.',
    'fk_fsm_category_cd':'Fsm category code.',
    'fk_precedent_fsm_type_id':'Precedent FSM type ID.',
    'fk_initial_fsm_type_state_id':'Initial FSM type state ID.',
    'fk_root_fsm_type_id':'Root FSM type ID.',
    'title_tx':'Title of FSM Id.',
    'description_tx':'Description associated with Id.',
    'begin_effective_dt':'Begin effective dates.',
    'end_effective_dt':'End effective dates.',
    'create_ts':'Timesatmp created when Id was craeted.',
    'create_user_id':'User id of user who created Id.',
    'last_mod_ts':'Timesatmp when Id was last modified.',
    'last_mod_user_id':'User id of user who modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_fsm_type', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_type_event_id':'FSM event ID.',
    'fk_fsm_type_id':'Type of FSM it belongs to.',
    'title_tx':'Title of the event.',
    'description_tx':'Description of the event.',
    'create_ts':'Timestamp when event was created.',
    'create_user_id':'User id of user who created.',
    'last_mod_ts':'Timestamp craeted when last modified.',
    'last_mod_user_id':'User id of user who last modified.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_fsm_type_event', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_type_state_id':'Finite state machine type state ID.',
    'fk_fsm_type_id':'FSM type ID.',
    'fk_root_fsm_type_id':'Root FSM type ID.',
    'title_tx':'Title related to ID.',
    'state_start_in':'Start states.',
    'state_end_in':'End states.',
    'description_tx':'Description related to Id.',
    'human_activity_tx':'Human activities.',
    'automated_activity_tx':'Automated activities.',
    'create_ts':'Creation timestamps.',
    'create_user_id':'User id of user who created.',
    'last_mod_ts':'Modification timestamps.',
    'last_mod_user_id':'User id of user who created.',
    'start_condition_tx':'Start condition.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_fsm_type_state', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fsm_type_state_rule_id':'Finite state machine state rule Id.',
    'fk_fsm_type_id':'FSM type Id.',
    'fk_root_fsm_type_id':'Root FSM type Id',
    'fk_current_fsm_type_state_id':'Current state of FSM.',
    'fk_next_fsm_type_state_id':'Next state of the FSM.',
    'fk_fsm_type_event_id':'Event that triggering the transition.',
    'description_tx':'Descriptions for each rule.',
    'precondition_tx':'Preconditions for each rule.',
    'rule_action_tx':'Actions to be taken during the transition.',
    'create_ts':'Timestamp when it was created.',
    'create_user_id':'User id who created user.',
    'last_mod_ts':'Timestamp when it was last modified.',
    'last_mod_user_id':'User Id of user who last modified.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_fsm_type_state_rule', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'stnd_interlock_type_cd':'Standard code of the interlock type.',
    'title_tx':'Title for the interlock type.',
    'description_tx':'Description of the interlock.',
    'create_ts':'Timestamp when the interlock was created.',
    'create_user_id':'User Id of user who created it.',
    'last_mod_ts':'Timestamp when it was last modified.',
    'last_mod_user_id':'User Id of user who last modified.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='stnd_interlock_type', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'user_profile_id':'User Id profile.',
    'profile_nm':'Profile name of the user.',
    'description_tx':'Description related to user.',
    'create_ts':'Timestamp when user was created.',
    'create_user_id':'User id of user who created it.',
    'last_mod_ts':'Timestamp when it was last modified.',
    'last_mod_user_id':'User id of user who last modified.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='user_profile', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
    'fk_user_profile_id':'User profile Id.',
    'fk_domain_cd':'Domain code.',
    'resource_nm':'Resource name.',
    'preference_nm':'Preference name.',
    'preference_value_tx':'Preference value.',
    'create_ts':'Timestamp when record was created.',
    'create_user_id':'User Id of user who created it.',
    'last_mod_ts':'timestamp when record was modified.',
    'last_mod_user_id':'User Id of user who last modified record.',
    'user_profile_preference_id':'Specific prefernce for a user profile.'
}


# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=eogadmin_catalog, database=database, table='user_profile_preference', column_name=column, comment=comment)

  spark.sql(column_comment_query)

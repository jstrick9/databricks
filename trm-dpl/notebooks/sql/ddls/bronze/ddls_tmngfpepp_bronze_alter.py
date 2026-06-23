# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngfpepp-conf.yaml"
config_file = f"../../../config/{dbx_env}/{config_file_name}"
if dbx_env == 'qa':
    dbx_env = 'test'
print(f'{config_file=}, {dbx_env=}')

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs = read_yaml(config_file)
tmngfpepp_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(f'{tmngfpepp_catalog=}, {data_quality_catalog=} ')

#spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('conf.catalog', tmngfpepp_catalog.lower()) 
#spark.conf.set('dbx_env', dbx_env) 

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'

spark.conf.set('conf.catalog', tmngfpepp_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

tables_to_comment = {
    'databasechangelog': 'The databasechangelog table in the bronze composition of the trm_tmngfpepp_dev list stores data about executed data set changes. It contains subtleties, for example, the ID of the change, the creator who rolled out the improvement, the filename of the change script, the date and time when the change was executed, the sort of execution, the MD5 checksum of the change, a depiction of the change, any remarks related with the change, a tag for order, data about Liquibase, the settings where the change is material, marks for additional characterization, and the organization ID. This table is significant for following and overseeing data set changes and guaranteeing information respectability.',

    'databasechangeloglock': 'The databasechangeloglock table in the bronze construction of the trm_tmngfpepp_dev index stores data about locks procured during data set changelog tasks. It contains information connected with the lock status, the timestamp when the lock was allowed, and the client who obtained the lock. This table is essential for overseeing simultaneous data set changelog activities and guaranteeing information trustworthiness during construction changes or updates. It forestalls clashes and guarantees that only one client can change the data set changelog at a time.',

    'form_paragraph': 'The form_paragraph table in the bronze blueprint of the trm_tmngfpepp_dev list contains information connected with passages utilized in structures. It stores data, for example, the remarkable identifier of the passage, the call number related with the section, the timestamp of when the passage was made and last adjusted, and the client IDs of the clients who made and last altered the section. Moreover, it incorporates a sort request field and a layout pointer field. This table is influential for the business as it takes into account the administration and association of sections inside structures.',

    'form_paragraph_action': 'The form_paragraph_action table in the bronze outline of the trm_tmngfpepp_dev list contains information connected with activities performed on structure sections. It gives data about the adaptation of the structure passage, the move made, the representative number related with the activity, and the timestamps for creation, alteration, and activity. This table is influential for the business as it permits following and investigation of moves initiated on structure passages, empowering better comprehension of client conduct and interaction effectiveness.',

    'form_paragraph_reason':'The form_paragraph_reason table contains information connected with the explanations behind unambiguous passages in structures. It gives data on the novel identifier of the explanation and the interesting identifier of the structure section. This table is vital for the business as it assists in understanding the explanations for specific sections in structures, which with canning be utilized for examination and dynamic purposes. The information in this table addresses the connection between structure passages and the reasons related with them.',

    'form_paragraph_version': 'The form_paragraph_version table stores renditions of structure sections utilized in the framework. Each column addresses a particular rendition of a structure passage, including its title, content, and other pertinent data. The table tracks the creation and adjustment timestamps, as well as the client IDs of the people who rolled out the improvements. It likewise incorporates compelling timestamps to show the period during which a specific rendition is substantial. The table contains extra fields to oversee associations with different substances, for example, structure passage gatherings and classifications. Generally speaking, this table assumes a pivotal part in overseeing and keeping up with the different variants of structure passages utilized in the business processes.',

    'fpv_scheduled_job': 'The fpv_scheduled_job table contains data about planned positions connected with structure section adaptations. It tracks when a task was informed, made, and last changed, alongside the comparing client IDs. The table likewise incorporates an unfamiliar key reference to the structure passage variant it is related with. This table is significant for following the booking and the executives of occupations connected with structure section variants, considering proficient checking and coordination of errands inside the business.',

    'qrtz_blob_triggers': 'The qrtz_blob_triggers table in the bronze outline of the trm_tmngfpepp_dev list stores data about mass triggers. Mass triggers are utilized to plan and execute undertakings or occasions in view of specific circumstances. This table contains information connected with the name and gathering of the trigger, as well as any extra mass information related with the trigger. The table is fundamental for overseeing and following mass triggers inside the business application.',

    'qrtz_calendars': 'The qrtz_calendars table stores information about calendars used in scheduling. Calendars are used to define non-working days or time intervals that should be excluded from scheduling. This table provides a record of the different calendars available in the system, including their names and associated schedule names. The data in this table is essential for accurately scheduling tasks and ensuring they are not assigned to non-working days or times.',

    'qrtz_cron_triggers': 'The qrtz_cron_triggers table stores data about cron triggers in the framework. Cron triggers are utilized to plan responsibilities to run at explicit times or spans. This table contains subtleties like the name and gathering of the trigger, the cron articulation that characterizes the timetable, and the time region wherein the trigger works. The information in this table is urgent for overseeing and executing planned positions successfully.',
    
    'qrtz_fired_triggers': 'The qrtz_fired_triggers table in the bronze outline of the trm_tmngfpepp_dev list stores data about terminated triggers in the booking framework. It contains information connected with the name, gathering, and occurrence of the trigger, as well as the time it was terminated and booked. The table likewise incorporates insights regarding the need, state, and recuperation status of the trigger. This table is fundamental for following and dealing with the execution of booked positions and guaranteeing their appropriate working.',

    'qrtz_job_details': 'The qrtz_job_details table contains information about the scheduled jobs in the system. It provides details such as the name, group, and description of each job, as well as the class name of the job implementation. The table also indicates whether the job is durable, non-concurrent, and if it requires recovery. The job_data column stores additional data associated with each job. This table is essential for managing and monitoring scheduled jobs in the business application.',

    'qrtz_locks': 'The qrtz_locks table in the bronze mapping of the trm_tmngfpepp_dev index stores data about locks utilized by the planning framework. This table is influential for the business as it oversees simultaneous admittance to assets and forestalls clashes between different cycles. The sched_name segment addresses the name of the scheduler that obtained the lock, while the lock_name section addresses the name of the lock being procured. The information in this table is essential for guaranteeing smooth execution of planned undertakings and keeping up with information respectability.',

    'qrtz_paused_trigger_grps': 'The qrtz_paused_trigger_grps table in the bronze mapping of the trm_tmngfpepp_dev list stores data about stopped trigger gatherings in the booking framework. This table is vital for the business as it takes into account the administration and control of trigger gatherings that have been briefly stopped. The information in this table addresses the names of the scheduler and trigger gatherings that have been stopped, giving a record of which gatherings are right now latent. This data is valuable for checking and investigating purposes, permitting chairmen to handily distinguish and continue stopped trigger gatherings when vital.',

    'qrtz_scheduler_state': 'The qrtz_scheduler_state table stores data about the condition of schedulers in the framework. It incorporates the name of the scheduler, the name of the occurrence, the last registration time, and the registration stretch. This table is significant for observing and dealing with the schedulers in the framework, as it gives bits of knowledge into their movement and wellbeing. The last registration time and registration stretch can be utilized to follow the accessibility and execution of the schedulers. Generally speaking, this table assumes a pivotal part in guaranteeing the smooth activity of the booking framework.',

    'qrtz_simple_triggers': 'The qrtz_simple_triggers table stores data about straightforward triggers in the planning framework. Basic triggers are utilized to plan tasks to run at explicit spans or a proper number of times. This table contains information, for example, the name and gathering of the trigger, the times the trigger has been rehashed, and the span at which the trigger rehashes. The times_triggered segment monitors the complete number of times the trigger has been terminated. This table is fundamental for following and overseeing booked positions in the business.',

    'qrtz_simprop_triggers': 'The qrtz_simprop_triggers table in the bronze pattern of the trm_tmngfpepp_dev list contains information connected with reenacted property triggers. This table stores data about the name and gathering of the trigger, as well as different string, whole number, long, decimal, and boolean properties related with the trigger. The table gives significant experiences into the properties and attributes of recreated property triggers, which are significant for examining and dealing with these triggers in the business setting.',

    'qrtz_triggers': 'The qrtz_triggers table contains data about triggers in the framework. Triggers are utilized to plan the execution of occupations. This table stores subtleties like the name, gathering, and depiction of the trigger, as well as its straightaway and past fire times. It likewise incorporates data about the triggers need, state, type, begin and end times, schedule name, fizzle guidance, and occupation information. This table is fundamental for overseeing and checking booked positions and their related triggers in the business.',

    'stnd_chapter_section': 'The stnd_chapter_section table addresses the various segments inside a section. Each segment has an interesting identifier, a title, and a depiction. The table additionally incorporates data about the parent area, the position request of the part inside the section, and the viable timestamps for when the segment starts and finishes. Also, there are sections to follow the creation and alteration timestamps, as well as the client IDs related with those activities. The table likewise incorporates a segment to show the pecking order level of each part inside the section.',

    'stnd_form_paragraph_action': 'The stnd_form_paragraph_action table contains data about the activities related with structure passages. It incorporates the activity code, title, depiction, compelling timestamps, creation and adjustment timestamps, and client IDs. This table is significant for following and overseeing structure section activities inside the business framework.',

    'stnd_form_paragraph_category': 'The stnd_form_paragraph_category table contains data about various classes of structure passages. Every class has an extraordinary identifier and a title. The depiction field gives extra insights regarding the class. The table likewise incorporates the compelling timestamps for when the class becomes dynamic and when it lapses. The make and last adjustment timestamps track when the classification was at first made and last changed. The make and last adjustment client IDs recognize the clients answerable for these activities. This table is fundamental for arranging and overseeing structure passages inside the framework.',

    'stnd_form_paragraph_group': 'The stnd_form_paragraph_group table contains data about gatherings of passages utilized in structures. Each gathering has a title and portrayal, and is related with a particular section segment. The table additionally incorporates timestamps for when the gathering was made and last adjusted, as well as the client IDs of the makers and modifiers. This table is significant for overseeing and coordinating the substance of structures, taking into account simple recovery and alteration of section bunches inside the framework.',

    'stnd_form_paragraph_reason': 'The stnd_form_paragraph_reason table contains data about the explanations behind utilizing structure sections in the business. It incorporates the title and portrayal of each explanation, as well as the compelling dates for when the explanation is substantial. The table likewise tracks the creation and adjustment timestamps, as well as the client IDs related with those activities. This table is significant for overseeing and sorting out structure sections in the framework, permitting clients to choose and apply the proper justification behind their requirements without any problem.',

    'cdc_batch_job_control': 'The cdc_batch_job_control table in the bronze schema of the trm_tmbuscalendar_dev catalog is used to control and track the Change Data Capture (CDC) batch jobs. It contains information about the source folder, catalog name, database name, and table name for each job. Additionally, it includes the source database name and source table name, which represent the original data source. The primary keys column stores the primary keys for each table, while the full_load column indicates whether a full load is required. The initial_load_finished column is a boolean value that shows whether the initial load has been completed. This table is essential for managing and monitoring the CDC process in the business system.',

    'cdc_batch_job_history' : 'The cdc_batch_job_history table contains information about the history of Change Data Capture (CDC) batch jobs. It stores the file path of the CDC file, the source time of the metadata, the date of the CDC file, and the processing time of the job. This table is significant to the business as it allows tracking and monitoring of CDC batch job activities, providing insights into data changes and updates. The data in this table represents the historical records of CDC batch jobs, enabling analysis and troubleshooting of data synchronization processes.'
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
'id':'id of changed',
'author':'Author who made changes.',
'filename':'File name of changed script',
'dateexecuted':'Date of that file executed',
'orderexecuted':'Order executed number',
'exectype':'Weather it is executed or not',
'md5sum':' MD5 checksum of the change',
'description':'Description of changes done.',
'comments':'Comments associated with change.',
'tag':'tag number',
'liquibase':'Liquibase version number',
'contexts':'Contexts  for specifying the execution environment',
'labels':'Labels  for specifying the execution environment',
'deployment_id':'deployment number'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='databasechangelog', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'id':'id number',
'locked':'Indicated weather it is locked or not.',
'lockgranted':'Records when the lock was acquired.',
'lockedby':'User who acquired lock.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='databasechangeloglock', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'form_paragraph_gid':'Unique identifier of the paragraph.',
'call_number_tx':'Call number that is associated by paragraph.',
'create_ts':'Timestamp when the paragraph was created.',
'create_user_id':'User_id of the user who created paragraph.',
'last_mod_ts':'Timestamp when last time paragraph was modified.',
'last_mod_user_id':'User_id of the user who modified last.',
'sort_order_tx':'Sort order field',
'template_in':'Template indicator'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='form_paragraph', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'fk_form_paragraph_version_gid':'Version of the form paragraph.',
'form_paragraph_action_gid':'Action performed.',
'fk_form_paragraph_action_cd':'Action as it is Migrated or Published_On_Demand.',
'cfk_employee_no':'Employee number.',
'create_ts':'Timestamp when it was created.',
'create_user_id':'User_id of user who created.',
'last_mod_ts':'Last modified date.',
'last_mod_user_id':'User_id who modified.',
'action_ts':'Action Timestamp.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='form_paragraph_action', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'fk_form_paragraph_reason_id':'Represents the unique identifier for each reason.',
'fk_form_paragraph_gid':'Represents the unique identifier for form paragraph.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='form_paragraph_reason', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'paragraph_title_tx':'Title of form paragraph.',
'create_ts':'Timstamp when the form paragraph was created.',
'create_user_id':'user_id of user who created it.',
'last_mod_ts':'Timstamp when form paragraph was last modified.',
'last_mod_user_id':'User_id of user who modified.',
'begin_effective_ts':'Effective Timstamp.',
'end_effective_ts':'End of an effective Timstamp.',
'status_ct':'If the pragraph is Archived or not.',
'version_no':'Version no of form paragraph.',
'fk_form_paragraph_group_id':'Form paragraphs group id.',
'fk_form_paragraph_category_id':'Form paragraph category.',
'case_relationship_ct':'Realtionship to other cases.',
'allow_end_user_edits_in':'Indicates weather users can edit it or not.',
'track_end_user_edits_in':'Indicates weather we need to track who edited',
'scheduled_action_ts':'Scheduled action are recorded.',
'fk_chapter_section_id':'Chapter section id number.',
'fk_fp_call_number_tx':'Call number.',
'source_status_ct':'Source status count.',
'fk_source_form_para_ver_gid':'Source of form paragraph version.',
'form_paragraph_tx':'Form paragraph notes.',
'end_user_notes_tx':'End user notes of form paragraph.',
'research_notes_tx':'Research notes for form paragraph.',
'published_ts':'Timestamp when form paragraph was published.',
'published_by_employee_no':'Employee who published form paragraph.',
'retired_ts':'Timestamp if form paragraph was retired.',
'retired_by_employee_no':'Employee number who retired form paragraph.',
'form_paragraph_version_gid':'Form paragraph version number.',
'scheduled_ts':'Scheduled Timstamp',
'scheduled_by_employee_no':'Employee who scheduled the form paragraph.',
'fk_division_id':'Division number of form paragraph.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='form_paragraph_version', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'notified_in':'Tracks Scheduled jobs are notified in.',
'create_ts':'Timestamp was created when job was notified.',
'create_user_id':'User_id of user who notified in.',
'last_mod_ts':'Last modified timestamp.',
'last_mod_user_id':'Last modified user_id.',
'fk_form_paragraph_version_gid':'From paragraph version number.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='fpv_scheduled_job', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Scheduled name.',
'trigger_name':'Trigger name.',
'trigger_group':'Trigger group.',
'blob_data':'blod data associated with trigger.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_blob_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Scheduled name.',
'calendar_name':'Calendar name.',
'calendar':'Calendar that is scheduled.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_calendars', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler.',
'trigger_name':'Name of the trigger.',
'trigger_group':'Group of the trigger.',
'cron_expression':'Cron expression that defines the schedule.',
'time_zone_id':'Tim zone in which it is figured to run.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_cron_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler.',
'entry_id':'Entry id of fired trigger.',
'trigger_name':'Name of the trigger.',
'trigger_group':'Trigger group.',
'instance_name':'Name of the instance that triggers.',
'fired_time':'Time instance needs to start.',
'sched_time':'Time that is scheduled to trigger.',
'priority':'Priority of trigger.',
'state':'State of the trigger.',
'job_name':'Job name.',
'job_group':'Group of the job.',
'is_nonconcurrent':'Flags indicating if the job is nonconcurrent.',
'requests_recovery':'If job requests recovery.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_fired_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler.',
'job_name':'Job name.',
'job_group':'Job Group.',
'description':'Description of each Job.',
'job_class_name':'Class name of the job implementation.',
'is_durable':'Flags indicating weather job is durable.',
'is_nonconcurrent':'Indicating weather job is nonconcurrent.',
'is_update_data':'Indicating weather job is updated.',
'requests_recovery':'Indicating weather Job needed recovery.',
'job_data':'Additional Job data is stored.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_job_details', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler',
'lock_name':'Name of the lock.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_locks', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler',
'trigger_group':'Trigger group name'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_paused_trigger_grps', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler.',
'instance_name':'Name of instance.',
'last_checkin_time':'Check in time.',
'checkin_interval':'Check in time interval.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_scheduler_state', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler.',
'trigger_name':'Name of trigger.',
'trigger_group':'Group of the trigger.',
'repeat_count':'Number of times trigger has been repeated.',
'repeat_interval':'Interval at which trigger has repeated.',
'times_triggered':'Number of times it triggered.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_simple_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler.',
'trigger_name':'Name of trigger.',
'trigger_group':'Group of the trigger.',
'str_prop_1':'Properties associated with trigger that has string as datatype.',
'str_prop_2':'Properties associated with trigger that has string as datatype.',
'str_prop_3':'Properties associated with trigger that has string as datatype.',
'int_prop_1':'Properties associated with trigger that has Integer as datatype.',
'int_prop_2':'Properties associated with trigger that has Integer as datatype.',
'long_prop_1':'Properties associated with trigger that has Long as datatype.',
'long_prop_2':'Properties associated with trigger that has long as datatype',
'dec_prop_1':'Properties associated with trigger that has decimal as datatype',
'dec_prop_2':'Properties associated with trigger that has decimal as datatype',
'bool_prop_1':'Properties associated with trigger that has boolean as datatype',
'bool_prop_2':'Properties associated with trigger that has boolean as datatype'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_simprop_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'sched_name':'Name of the scheduler.',
'trigger_name':'Name of the trigger.',
'trigger_group':'Group of the trigger.',
'job_name':'Name of the Job.',
'job_group':'Job Group.',
'description':'Description of trigger.',
'next_fire_time':'Next fire time.',
'prev_fire_time':'Previous fire time.',
'priority':'Priority of the trigger.',
'trigger_state':'State of the trigger.',
'trigger_type':'Type of the trigger.',
'start_time':'Start time of the trigger.',
'end_time':'End time of the trigger.',
'calendar_name':'Calendar name of the trigger.',
'misfire_instr':'Miss fire instruction of the trigger.',
'job_data':'Any additional Job data.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='qrtz_triggers', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'chapter_section_id':'Section Id.',
'title_tx':'Title of the section.',
'description_tx':'Description of the section.',
'fk_parent_chapter_section_id':'Parent section id.',
'position_order_no':'Position order of the section',
'begin_effective_ts':'Time period when section is effective.',
'end_effective_ts':'End of time period when section was effective.',
'create_ts':'Timestamp when the user craeted section.',
'create_user_id':'User id of the user who created section.',
'last_mod_ts':'Last modified timestamp.',
'last_mod_user_id':'last user who modified.',
'chapter_section_ct':'Indicates if the section is Chapter or Division.',
'hierarchy_level_no':'Hierarchy level of the section within the chapter.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='stnd_chapter_section', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'form_paragraph_action_cd':'Form paragraph action.',
'title_tx':'Title of the action.',
'description_tx':'Description of the action.',
'begin_effective_ts':'Time period when action is effective.',
'end_effective_ts':'End of time period when action was effective',
'create_ts':'Timestamp when action was created.',
'create_user_id':'User id who created action.',
'last_mod_ts':'Last modified timestamp of action.',
'last_mod_user_id':'Last modified user id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='stnd_form_paragraph_action', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'form_paragraph_category_id':'From paragraph category Id.',
'title_tx':'Title of the category.',
'description_tx':'Description of the category.',
'fk_chapter_section_id':'Specific chapter section Id.',
'begin_effective_ts':'Time period when category is effective.',
'end_effective_ts':'End of time period when category was effective.',
'create_ts':'Timestamp when category was created.',
'create_user_id':'User id of the user who created Timestamp.',
'last_mod_ts':'Last modified timestamp.',
'last_mod_user_id':'User id who modified.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='stnd_form_paragraph_category', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'form_paragraph_group_id':'Form paragraph group id.',
'title_tx':'Title of the Form paragraph group.',
'description_tx':'Description of the group.',
'fk_chapter_section_id':'Associated chapter section.',
'begin_effective_ts':'Time period when group is effective.',
'end_effective_ts':'End of time period when group was effective.',
'create_ts':'Timestamp when group was created.',
'create_user_id':'User id who created group.',
'last_mod_ts':'Last modified timestamp of group.',
'last_mod_user_id':'Last modified user id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='stnd_form_paragraph_group', column_name=column, comment=comment)

  spark.sql(column_comment_query)

# COMMAND ----------

# Define a dictionary of column comments
column_comments = {
'form_paragraph_reason_id':'Form paragraph reason id.',
'title_tx':'Title of the Form paragraph reason.',
'description_tx':'Description of the reason.',
'begin_effective_ts':'Time period when reason is effective.',
'end_effective_ts':'End of time period when reason was effective.',
'create_ts':'Timestamp when reason was created.',
'create_user_id':'User id who created reason.',
'last_mod_ts':'Last modified timestamp of reason.',
'last_mod_user_id':'Last modified user id.'
}

# Loop through the columns and generate column comment queries
for column, comment in column_comments.items():
  column_comment_query = """
  ALTER TABLE {catalog}.{database}.{table}
  ALTER COLUMN {column_name}
  COMMENT '{comment}'
  """.format(catalog=tmngfpepp_catalog, database=database, table='stnd_form_paragraph_reason', column_name=column, comment=comment)

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
  """.format(catalog=tmngfpepp_catalog, database=database, table='cdc_batch_job_control', column_name=column, comment=comment)

  spark.sql(column_comment_query)

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
  """.format(catalog=tmngfpepp_catalog, database=database, table='cdc_batch_job_history', column_name=column, comment=comment)

  spark.sql(column_comment_query)

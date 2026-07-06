# Databricks notebook source
import boto3
import json

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("dbx_env", "dev", "dbx_env")
dbutils.widgets.text("start_tasks","full_load")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
src_sys_name = dbutils.widgets.get("SRC_SYS_NAME")

config_file = f"../../config/{dbx_env}/{src_sys_name.lower()}-conf.yaml"

print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Get Table List
# MAGIC %run ../shared/ntb_tm_brnz_table_list

# COMMAND ----------

# DBTITLE 1,Set Parameter Values
common_configs = read_yaml(config_file)
replication_instance = common_configs['DMS']['replication_instance']
source_endpoint = common_configs['DMS']['source_endpoint']
target_endpoint = common_configs['DMS']['target_endpoint']
full_load_task_name = common_configs['DMS']['full_load_task_name']
source_schema = common_configs['schema']['src_db_name']

print(f"{full_load_task_name=}")
print(f"{source_schema=}")

# COMMAND ----------

# get number of groups for tmngpdb
if src_sys_name.lower() == 'tmngpdb':
    group_num = common_configs['DMS']['groups']
    group_list = [meta.split('_')[2] for meta in dir() if meta.startswith('tmngpdb_metadata_')]
    group_list = [group for group in group_list if int(group[-1]) <= group_num]

# COMMAND ----------

# DBTITLE 1,Connect to DMS
dms_client = boto3.client('dms', 'us-east-1')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Resource ARNs from Names

# COMMAND ----------

# get task ARNs
desc_tasks = dms_client.describe_replication_tasks()
if src_sys_name.lower() == 'tmngpdb':
    start_tasks = []
    for group in group_list:
        task_id = full_load_task_name+'-'+group
        try:
            task = [task for task in desc_tasks['ReplicationTasks'] if task['ReplicationTaskIdentifier'] == task_id][0]
            start_tasks.append(task)
        except Exception as e:
            print(task_id + " : " + str(e))
else:
    full_load_task = [task for task in desc_tasks['ReplicationTasks'] if task['ReplicationTaskIdentifier'] == full_load_task_name][0]

# COMMAND ----------

# wait for all tasks to be ready
if src_sys_name.lower() == 'tmngpdb':
    for task in start_tasks:
        if task['Status'] == 'creating':
            ready_waiter = dms_client.get_waiter('replication_task_ready')
            print("Waiting for task to be ready...")
            ready_waiter.wait(Filters=[{'Name':'replication-task-arn','Values': [task['ReplicationTaskArn']]}])
else:
    ready_waiter = dms_client.get_waiter('replication_task_ready')
    print("Waiting for task to be ready...")
    ready_waiter.wait(Filters=[{'Name':'replication-task-arn','Values': [full_load_task['ReplicationTaskArn']]}])


# COMMAND ----------

# refresh statuses
if src_sys_name.lower() == 'tmngpdb':
    start_tasks = []
    for group in group_list:
        task_id = full_load_task_name+'-'+group
        try:
            task = [task for task in desc_tasks['ReplicationTasks'] if task['ReplicationTaskIdentifier'] == task_id][0]
            start_tasks.append(task)
        except Exception as e:
            print(task_id + " : " + str(e))
else:
    desc_tasks = dms_client.describe_replication_tasks()
    full_load_task = [task for task in desc_tasks['ReplicationTasks'] if task['ReplicationTaskIdentifier'] == full_load_task_name][0]

# COMMAND ----------

# start each task
if src_sys_name.lower() == 'tmngpdb':
    for task in start_tasks:
        if task['Status'] == 'ready':
            response_start = dms_client.start_replication_task(
            ReplicationTaskArn=task['ReplicationTaskArn'],
            StartReplicationTaskType='start-replication')
        else:
            response_start = dms_client.start_replication_task(
            ReplicationTaskArn=task['ReplicationTaskArn'],
            StartReplicationTaskType='reload-target')
else:
    if full_load_task['Status'] == 'ready':
        response_start = dms_client.start_replication_task(
        ReplicationTaskArn=full_load_task['ReplicationTaskArn'],
        StartReplicationTaskType='start-replication')
    else:
        response_start = dms_client.start_replication_task(
        ReplicationTaskArn=full_load_task['ReplicationTaskArn'],
        StartReplicationTaskType='reload-target')

# COMMAND ----------

dbutils.notebook.exit(f"DMS tasks {start_tasks} started for schema {src_sys_name}")

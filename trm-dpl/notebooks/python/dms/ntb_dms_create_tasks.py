# Databricks notebook source
import boto3
import json

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("dbx_env", "dev", "dbx_env")
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
initial_load_task_name = common_configs['DMS']['initial_load_task_name']
full_load_task_name = common_configs['DMS']['full_load_task_name']
source_schema = common_configs['schema']['src_db_name']

map_type = "Include"
print(f"{initial_load_task_name=}")
print(f"{full_load_task_name=}")
print(f"{source_schema=}")

# COMMAND ----------

# get number of groups for tmngpdb
if src_sys_name.lower() == 'tmngpdb':
    group_num = common_configs['DMS']['groups']
    group_list = [meta.split('_')[2] for meta in dir() if meta.startswith('tmngpdb_metadata_')]
    group_list = [group for group in group_list if int(group[-1]) <= group_num]
    groups = {}
    for group in group_list:
        groups[group] = {}
else:
    table_metadata = eval(f"{src_sys_name.lower()}_metadata")

# COMMAND ----------

# DBTITLE 1,Connect to DMS
dms_client = boto3.client('dms', 'us-east-1')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Resource ARNs from Names

# COMMAND ----------

# get endpoint ARNs
desc_endpoints = dms_client.describe_endpoints()
source_endpoint_arn = [endp['EndpointArn'] for endp in desc_endpoints['Endpoints'] if endp['EndpointIdentifier'] == source_endpoint][0]

# get replication instance ARN
replication_instance_arn = dms_client.describe_replication_instances(Filters=[{'Name':'replication-instance-id', 'Values': [replication_instance]}])['ReplicationInstances'][0]['ReplicationInstanceArn']

# COMMAND ----------

if src_sys_name.lower() == 'tmngpdb':
    for group, info in groups.items():
        target_endpoint_arn = [endp['EndpointArn'] for endp in desc_endpoints['Endpoints'] if endp['EndpointIdentifier'] == target_endpoint + f'-{group}'][0]
        groups[group]['target_endpoint_arn'] = target_endpoint_arn
else:
    target_endpoint_arn = [endp['EndpointArn'] for endp in desc_endpoints['Endpoints'] if endp['EndpointIdentifier'] == target_endpoint][0]

# COMMAND ----------

# DBTITLE 1,Common Settings
# common settings used by all tasks
default_settings = {
    "Logging": {
        "EnableLogging": True,
        "EnableLogContext": False
    },
    "TargetMetadata": {
        "LobMaxSize": 1024
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Delta Load Task

# COMMAND ----------

if src_sys_name.lower() == 'tmngpdb':
    for group, info in groups.items():
        # get cdc tables
        table_list_cdc = [table.upper() for table_type, table, full_load_in, delta_col, table_size in eval(f"{src_sys_name.lower()}_metadata_{group}") if full_load_in == 'N' and table_size == '']

        table_list_cdc_lrg = [table.upper() for table_type, table, full_load_in, delta_col, table_size in eval(f"{src_sys_name.lower()}_metadata_{group}") if full_load_in == 'N' and table_size == 'L']

        # create table mapping json
        table_map_list_cdc = [{"Type": map_type, "SourceSchema": source_schema.upper(), "SourceTable": table} for table in table_list_cdc]

        table_mappings_cdc = json.dumps({"TableMappings": table_map_list_cdc})

        groups[group]['table_mappings_cdc'] = table_mappings_cdc
        groups[group]['table_list_cdc_lrg'] = table_list_cdc_lrg
else:
    # get cdc tables
    table_list_cdc = [table.upper() for table_type, table, full_load_in, delta_col in table_metadata if full_load_in == 'N']

    # create table mapping json
    table_map_list_cdc = [{"Type": map_type, "SourceSchema": source_schema.upper(), "SourceTable": table} for table in table_list_cdc]
    table_mappings_cdc = json.dumps({"TableMappings": table_map_list_cdc})

# COMMAND ----------

# create dms task(s)
if src_sys_name.lower() == 'tmngpdb':
    for group, info in groups.items():
        try:
            response_create = dms_client.create_replication_task(ReplicationTaskIdentifier=f"{initial_load_task_name}-{group}",
                SourceEndpointArn=source_endpoint_arn,
                TargetEndpointArn=groups[group]['target_endpoint_arn'],
                ReplicationInstanceArn=replication_instance_arn,
                MigrationType='full-load',
                TableMappings=groups[group]['table_mappings_cdc'],
                ReplicationTaskSettings=json.dumps(default_settings))
        except Exception as e:
            print(e)
        for tbl in groups[group]['table_list_cdc_lrg']:
            table_map = [{"Type": map_type, "SourceSchema": source_schema.upper(), "SourceTable": tbl}]
            mapping = json.dumps({"TableMappings": table_map})
            try:
                response_create = dms_client.create_replication_task(ReplicationTaskIdentifier=f"{initial_load_task_name}-{tbl.lower().replace('_','-')}",
                    SourceEndpointArn=source_endpoint_arn,
                    TargetEndpointArn=groups[group]['target_endpoint_arn'],
                    ReplicationInstanceArn=replication_instance_arn,
                    MigrationType='full-load',
                    TableMappings=mapping,
                    ReplicationTaskSettings=json.dumps(default_settings))
            except Exception as e:
                print(e)

else:
    try:
        response_create = dms_client.create_replication_task(ReplicationTaskIdentifier=initial_load_task_name,
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=target_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType='full-load',
            TableMappings=table_mappings_cdc,
            ReplicationTaskSettings=json.dumps(default_settings))
    except Exception as e:
        print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Full Load Task

# COMMAND ----------

if src_sys_name.lower() == 'tmngpdb':
    for group, info in groups.items():
        # get full load tables
        table_list_full_load = [table.upper() for table_type, table, full_load_in, delta_col, table_size in eval(f"{src_sys_name.lower()}_metadata_{group}") if full_load_in == 'Y']

        # create table mapping json
        table_map_list_full_load = [{"Type": map_type, "SourceSchema": source_schema.upper(), "SourceTable": table} for table in table_list_full_load]
        table_mappings_full_load = json.dumps({"TableMappings": table_map_list_full_load})

        groups[group]['table_mappings_full_load'] = table_mappings_full_load
else:
    # get full load tables
    table_list_full_load = [table.upper() for table_type, table, full_load_in, delta_col in table_metadata if full_load_in == 'Y']

    # create table mapping json
    table_map_list_full_load = [{"Type": map_type, "SourceSchema": source_schema.upper(), "SourceTable": table} for table in table_list_full_load]
    table_mappings_full_load = json.dumps({"TableMappings": table_map_list_full_load})

# COMMAND ----------

# create dms task(s)
if src_sys_name.lower() == 'tmngpdb':
    for group, info in groups.items():
        try:
            response_create = dms_client.create_replication_task(ReplicationTaskIdentifier=f"{full_load_task_name}-{group}",
                SourceEndpointArn=source_endpoint_arn,
                TargetEndpointArn=groups[group]['target_endpoint_arn'],
                ReplicationInstanceArn=replication_instance_arn,
                MigrationType='full-load',
                TableMappings=groups[group]['table_mappings_full_load'],
                ReplicationTaskSettings=json.dumps(default_settings))
        except Exception as e:
            print(e)
else:
    try:
        response_create = dms_client.create_replication_task(ReplicationTaskIdentifier=full_load_task_name,
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=target_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType='full-load',
            TableMappings=table_mappings_full_load,
            ReplicationTaskSettings=json.dumps(default_settings))
    except Exception as e:
        print(e)

# COMMAND ----------

dbutils.notebook.exit(f"DMS tasks created for schema {src_sys_name}")

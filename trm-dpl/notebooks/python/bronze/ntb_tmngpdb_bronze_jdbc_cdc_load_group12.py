# Databricks notebook source
# MAGIC %md
# MAGIC ##Datacompy
# MAGIC https://capitalone.github.io/datacompy/spark_usage.html

# COMMAND ----------

#pip install datacompy

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("data_load_group", "", "data_load_group")#group1
dbutils.widgets.text("full_data_refresh", "N", "full_data_refresh")#group1
dbutils.widgets.text("table_name", "", "table_name")#group1
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
#scope DBRPRODS & JBTEASPS

# COMMAND ----------

# DBTITLE 1,Config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
full_data_refresh = dbutils.widgets.get("full_data_refresh")
table_name = dbutils.widgets.get("table_name")
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml" 
#config_file_name = "tmbuscalendar-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-ng-trm-dpl-mysql-jar/notebooks/config/dev/tmngpdb-conf.yaml"
print(f'{config_file=}')

import pytz
#import datacompy
from pytz import timezone

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Execute Table list metadata ntbk
# MAGIC %run ../shared/ntb_tm_brnz_table_list

# COMMAND ----------

# DBTITLE 1,Set Parameter Values
common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()

if SRC_SYS_NAME == 'TMNGPDB':
    data_load_group = dbutils.widgets.get("data_load_group")
    src_folder = common_configs['cdc']['src_csv_files']+"/"+data_load_group + "/" +src_db_name
    schema_metadata = src_name+"_metadata_"+data_load_group
else:
    src_folder = common_configs['cdc']['src_csv_files']
    schema_metadata = src_name+"_metadata"
    data_load_group = dbutils.widgets.get("data_load_group")

src_database = common_configs['cdc']['src_database']
trm_scope = common_configs['secrets']['trm_scope']

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.trm_scope', trm_scope.lower()) 

spark.sql(f"set SRC_SYS_NAME = SRC_SYS_NAME")
database = 'bronze'
control_table = 'cdc_batch_job_control'


spark.conf.set('config.schema_metadata', schema_metadata.lower())
print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{trm_scope=},{schema_metadata=},{src_folder=} ')

# COMMAND ----------

# DBTITLE 1,Filter Tables from Control Table where Full Load = 'N' and initial load finished ='Y'
if SRC_SYS_NAME == 'TMNGPDB':
    df_control_table = spark.sql(f"""select catalog_name,database_name,table_name,source_db_name,source_table_name,primary_keys,full_load,decode(initial_load_finished,false,0,1) as initial_load_finished from {trgt_catalog}.bronze.cdc_batch_job_control
    where group_name = '{data_load_group}'
    --and source_table_name = upper('{table_name}')
  """)
else:
    df_control_table = spark.sql(f"""select catalog_name,database_name,table_name,source_db_name,source_table_name,primary_keys, full_load,decode(initial_load_finished,false,0,1) as initial_load_finished from {trgt_catalog}.bronze.cdc_batch_job_control """)


df_control_table.display()


# COMMAND ----------

# DBTITLE 1,Get CDC Timestamp Column name from table list ntbk
schema_def = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR"]
df_schema_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = schema_def)
df_schema_metadata = df_schema_metadata.select(f.upper('TABLE_NAME').alias("TABLE_NAME"),'DQ_FLTR').distinct()
job_control_df = df_control_table.alias("df_cntl").join(df_schema_metadata.alias("df_dq_fltr"),(f.col("df_cntl.source_table_name") == f.col("df_dq_fltr.TABLE_NAME")),"inner")
#job_control_df.display()
job_control_parameters = job_control_df.collect()

# COMMAND ----------

# DBTITLE 1,Define Merge Function
from delta.tables import DeltaTable

def merge_cdc_to_main(target_catalog, target_db, target_table, cdc_df, key_columns, all_columns, composite_key_ind):
    target_path = f"{target_catalog}.{target_db}.{target_table}"
    delta_table = DeltaTable.forName(spark, target_path)

    # Build merge condition
    merge_condition = " AND ".join([f"source.{k} = target.{k}" for k in key_columns])

    # Build update set
    update_set = {col: f"source.{col}" for col in all_columns if col not in key_columns}

    # Build insert set
    insert_set = {col: f"source.{col}" for col in all_columns}

    merge_builder = (
        delta_table.alias("target")
        .merge(
            cdc_df.alias("source"),
            merge_condition
        )
    )

    if composite_key_ind == 'Y':
        # Only update if non-key columns differ
        diff_condition = " OR ".join([f"target.{col} != source.{col}" for col in update_set.keys()])
        merge_builder = merge_builder.whenMatchedUpdate(
            condition=diff_condition,
            set=update_set
        )
    else:
        merge_builder = merge_builder.whenMatchedUpdate(set=update_set)

    merge_builder = merge_builder.whenNotMatchedInsert(values=insert_set)

    merge_builder.execute()

# COMMAND ----------

# DBTITLE 1,Delete and Merge Inserts/Updates into Bronze delta tables
def process_table(job_control):
    import datetime
    import pytz
    total_src_count = 0
    failed_tables = []
    try:
        current_cat = job_control['catalog_name']
        current_db  = job_control['database_name']
        current_table = job_control['table_name']
        primary_keys = job_control['primary_keys']
        src_db = job_control['source_db_name']
        src_table = job_control['source_table_name']
        cdc_date_col = job_control['DQ_FLTR']
        full_load_ind = job_control['full_load']
        initial_load_ind = job_control['initial_load_finished']
        job_name = f'ntb_{src_name}_{current_table}_brnz_load'
        print(f"******************************************** \n Now processing: {src_db}.{src_table}")

        key_columns = [item.strip().lower() for item in primary_keys.split(",")]
        all_columns = spark.table(f"{current_cat}.{current_db}.{current_table}").columns
        all_columns = [x.lower() for x in all_columns]
        print(f"all_columns= {all_columns}")
        print(f"key_columns= {primary_keys}")
        print(f"{cdc_date_col=}")

        start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        print(f'{start_ts=}')
        control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

        if not (full_data_refresh == 'Y' or full_load_ind == 'Y' or initial_load_ind == 0):
            dbutils.notebook.exit(f"No criteria satisfied for data load of {current_cat}.{current_db}.{current_table}")

        print(f"Performing full load for {current_cat}.{current_db}.{current_table}")
        # Read all data at once, filter in Spark
        cdc_table_query = f"select * from {src_db}.{src_table}"
        df_src_cdc = read_data_from_oracle_conn_dsu_cmn(cdc_table_query, trm_scope)
        total_src_count = df_src_cdc.count()

        # Delete all data before full load
        spark.sql(f"DELETE FROM {current_cat}.{current_db}.{current_table}")

        # Write in bulk
        df_src_cdc.write.mode("append").format("delta").insertInto(f'{current_cat}.{current_db}.{current_table}')

        # Update batch control table
        update_sql = f"""
            UPDATE {current_cat}.{current_db}.cdc_batch_job_control
            SET initial_load_finished = true 
            WHERE source_table_name = '{src_table}'
            and group_name = '{data_load_group}'
        """
        spark.sql(update_sql)
        print("Updated control table")

        end_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        print(f'{end_ts=}')
        time_elapsed = (end_ts - start_ts)
        print(f'{time_elapsed=}')

        trgt_query = f"select count(*) from {current_cat}.{current_db}.{current_table}"
        df_read_trgt_tbl = spark.sql(trgt_query)
        trgt_count = df_read_trgt_tbl.collect()[0][0]

        print(f'{total_src_count=},{trgt_count=}')
        end_job_cntl(f"{data_quality_catalog}", f"{trgt_catalog}.silver", job_name, start_ts, 'completed', total_src_count, trgt_count, "job completed successfully")
    except Exception as e:
        print("Exception message: {}".format(e))
        print(f"Unable to complete data load for {job_control['table_name']}")
        end_job_cntl(f"{data_quality_catalog}", f"{trgt_catalog}.silver", job_name, start_ts, 'failed', 0, 0, e)
        failed_tables.append(f'{job_control["table_name"]} "Exception message: {e}"')
    return failed_tables

# Sequentially process tables (let Spark handle parallelism)
data_load_failed_tables = []
total_src_count = 0
for job_control in job_control_parameters:
    failed = process_table(job_control)
    if failed:
        data_load_failed_tables.extend(failed)

if len(data_load_failed_tables) > 0:
    raise Exception(f'Data load failed for {data_load_failed_tables}')

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {trgt_catalog}.{database} ")

# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
alteryx_etldb_catalog = common_configs['schema']['alteryx_etldb_catalog']
print(f"{trgt_catalog=},{src_catalog=},{alteryx_etldb_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.alteryx_etldb_catalog', alteryx_etldb_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

job_name = 'ntb_second_level_count_tables_initial_load'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Count table initial load from hive metastore

# COMMAND ----------

# DBTITLE 1, added
list_count_tables = [
'filings_counts',
'fixed_class_counts',
'form_paragraph_counts',
'pendency_counts',
'pr_detail_counts',
'pr_milestone_counts',
'quality_counts',
'ttab_detail_counts',
'goods_services_normalization',
'goods_services_sn_list'
]

# COMMAND ----------

try:
    for table_name in list_count_tables: 
        try:
            print("\n")
            print(f"Performing initial load for {table_name}:")
            insert_query =  f"""insert overwrite table {trgt_catalog}.silver.{table_name} 
            select *, current_timestamp() as create_ts, '-1' as create_user_id,current_timestamp() as update_ts, '-1' as update_user_id from {alteryx_etldb_catalog}.{table_name}"""
            spark.sql(insert_query)
        except Exception as e:
            print("Exception message: {}".format(e))    
except:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise

# COMMAND ----------

end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'completed',0,"job completed successfully")
dbutils.notebook.exit(f"Completed loading second level count Tables ")

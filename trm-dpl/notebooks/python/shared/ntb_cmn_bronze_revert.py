# Databricks notebook source
# MAGIC %md
# MAGIC ###Purpose:
# MAGIC <pre>
# MAGIC This ntbk is used to reset the tables for full load 
# MAGIC 1. Read config file based on SRC SYS NAME
# MAGIC 2. Create a list of tables from control table
# MAGIC 3. Delete data from all tables
# MAGIC 4. Update control table
# MAGIC </pre>

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("data_load_group", "", "data_load_group")#group1
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
#scope DBRPRODS & JBTEASPS

# COMMAND ----------

# DBTITLE 1,Config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml" 
#config_file_name = "tmbuscalendar-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Set Parameter Values
common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']

spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 

spark.sql(f"set SRC_SYS_NAME = SRC_SYS_NAME")
database = 'bronze'
control_table = 'cdc_batch_job_control'
if SRC_SYS_NAME == 'TMNGPDB':
    #data_load_group = dbutils.widgets.get("data_load_group")
    #control_table_filter = f"where group_name='{data_load_group}'"
    control_table_filter = ""
else:
    control_table_filter = ""

print(f'{trgt_catalog=},{control_table_filter=} ')

# COMMAND ----------

control_df = spark.sql(f"select * from {trgt_catalog}.{database}.{control_table} " +control_table_filter)
dms_full_load_jobs_parameter_list = []

jobs_control_parameters = control_df.collect()

for autoloader_parameters_row in jobs_control_parameters:
    dms_full_load_jobs_parameter_list.append(
        (
            autoloader_parameters_row['src_folder'], 
            autoloader_parameters_row['catalog_name'],
            autoloader_parameters_row['database_name'], 
            autoloader_parameters_row['table_name']
        )
    )
    
print(dms_full_load_jobs_parameter_list)

# COMMAND ----------

for table_name in dms_full_load_jobs_parameter_list: 
        try:
            print("\n")
            print(f"Performing restore for {table_name[3]}:")
            prev_ver = spark.sql(f"""SELECT max(version) -1 as previousVersion  FROM (DESCRIBE HISTORY {trgt_catalog}.{database}.{table_name[3]})""").collect()[0][0]
            trgt_query =  f"""RESTORE TABLE {trgt_catalog}.{database}.{table_name[3]} TO VERSION AS OF {prev_ver}"""
            spark.sql(trgt_query)
        except Exception as e:
            
            print("Exception message: {}".format(e))

# COMMAND ----------

dbutils.notebook.exit(f"Completed cleaning up {trgt_catalog}.{database}.{control_table}. " +control_table_filter)

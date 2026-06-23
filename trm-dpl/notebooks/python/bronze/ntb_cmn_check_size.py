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
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,tmrefdata,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
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

data_quality_db = common_configs['schema']['data_quality_catalog']
spark.conf.set('conf.data_quality_db', data_quality_db)
print(f'{data_quality_db=} ')
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 

spark.sql(f"set SRC_SYS_NAME = SRC_SYS_NAME")
database = 'bronze'
control_table = 'cdc_batch_job_control'
if SRC_SYS_NAME == 'TMNGPDB_test':
    data_load_group = dbutils.widgets.get("data_load_group")
    control_table_filter = f"where group_name='{data_load_group}'"
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

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

total_size=0
for table_name in dms_full_load_jobs_parameter_list: 
    
        try:
            print("\n")
            print(f"checking size for {table_name[3]}:")
            trgt_query =  f"""DESCRIBE DETAIL {trgt_catalog}.{database}.{table_name[3]}"""
            df_size= spark.sql(trgt_query)
            table_size = df_size.select("sizeInBytes").collect()[0][0]
            total_size = total_size+table_size
        except Exception as e:
            
            print("Exception message: {}".format(e))
print(total_size)

# COMMAND ----------

df_check_size = spark.createDataFrame(
    [
        (SRC_SYS_NAME, total_size),  # create your data here, be consistent in the types.
    ],
    ["SRC_SYS_NAME", "total_size"]  # add your column names here
)
df_check_size =df_check_size.withColumn("last_updt_ts",current_timestamp())
df_check_size.display()
df_check_size.createOrReplaceTempView("temp_check_size")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite table ${conf.data_quality_db}.silver.CMN_CATALOG_SIZE_RFRNC partition(SRC_SYS_NAME)  
# MAGIC SELECT SRC_SYS_NAME,total_size, last_updt_ts  FROM temp_check_size

# COMMAND ----------

#df_check_size.write.mode("append").format("delta").saveAsTable(f'data_quality_dev.silver.check_data_size')

# COMMAND ----------

dbutils.notebook.exit(f"Completed loading data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  data_quality_dev.SILVER.CMN_CATALOG_SIZE_RFRNC

# COMMAND ----------



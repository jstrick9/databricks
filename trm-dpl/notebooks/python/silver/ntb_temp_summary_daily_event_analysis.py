# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml" 
config_file =  "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()
trm_scope = common_configs['secrets']['trm_scope']

env = dbx_env.upper()
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.trm_scope', trm_scope.lower()) 
spark.conf.set('config.dbx_env', dbx_env.lower())

if trgt_catalog.count("_") == 1:
    env = ""
else:
    env = "_"+trgt_catalog.split("_",2)[-1]

print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{trm_scope=},{dbx_env=},{env=}')
from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %sql
# MAGIC create 
# MAGIC -- or replace
# MAGIC table 
# MAGIC if not exists 
# MAGIC ${config.trgt_catalog}.silver.temp_summary_daily_event_pull
# MAGIC (
# MAGIC   trademark_gid STRING comment 'The trademark identifier code contains the serial number', 
# MAGIC   serial_num_tx STRING comment 'The serial number of load',
# MAGIC   event_dt DATE comment 'The date row was pulled from source tables',
# MAGIC   create_ts TIMESTAMP  comment 'The date and time that the record is inserted in the database',
# MAGIC   create_user_id string   comment 'The User Identifier of the logged-on AIS User that initiated the insert of the record into the database',
# MAGIC   last_mod_ts TIMESTAMP  comment 'The date and time that the record was last modified in the database.Upon creation, this will be the same as the Create Timestamp' ,
# MAGIC   last_mod_user_id string  comment 'The User Identifier of the logged on User that initiated the last modification to the record in the database' ,
# MAGIC   lock_control_no INT  comment 'A Number used  to verify that the record being updated has not been altered since it was retrieved for update when optimistic locking is used.'
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://bdr-databricks-app-${config.dbx_env}/eds/delta_tables/${config.catalog}/silver/temp_summary_daily_event_pull'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true, 'delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

from pyspark.sql.functions import col, lit, from_utc_timestamp, current_timestamp
# Define the query to read data from Oracle
table_query = f"SELECT * FROM { src_db_name}.temp_summary_daily_event"

# Read data from Oracle using the provided function
df_src_cdc = read_data_from_oracle_conn_dsu_cmn(table_query, trm_scope)

df_src_cdc= df_src_cdc.withColumn("create_ts",from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 

# Write the data to the Delta table
df_src_cdc.write.mode("append").format("delta").insertInto(f'{trgt_catalog}.silver.temp_summary_daily_event_pull')

# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()
trm_scope = common_configs['secrets']['trm_scope']
ptas_scope = common_configs['secrets']['ptas_scope']

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.trm_scope', trm_scope.lower()) 
spark.conf.set('config.ptas_scope', ptas_scope.lower())

print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{trm_scope=},{ptas_scope=} ')
from pyspark.sql.functions import col, lit

# COMMAND ----------

src_query="""select * from ptasuser.tmapplser"""

df_tmapplser_initial_load = read_data_from_oracle_conn_dsu_cmn(src_query,ptas_scope)

#df_tmapplser_initial_load.display()

# COMMAND ----------

src_query="""select * from ptasuser.tmapplser"""

df_tmapplser_initial_load = read_data_from_oracle_conn_dsu_cmn(src_query,ptas_scope)

df_tmapplser_initial_load = df_tmapplser_initial_load.withColumn("create_ts", current_timestamp())\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", current_timestamp())\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 

#df_tmapplser_initial_load.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC pushdown_query="(select * from ptasuser.tmapplser)"
# MAGIC
# MAGIC df_tmapplser_initial_load = (spark.read.format("jdbc")\
# MAGIC                           .option("url", "jdbc:oracle:thin:@rk8-corp-db-1.dev.uspto.gov:1620/PTASD")\
# MAGIC                           .option("dbtable",pushdown_query )\
# MAGIC                           .option("user", "BDXEXTRACT")\
# MAGIC                           .option("password", "")\
# MAGIC                           .option("driver", "oracle.jdbc.OracleDriver")\
# MAGIC                           .option("fetchsize","10000")\
# MAGIC                           .load())
# MAGIC
# MAGIC df_tmapplser_initial_load = df_tmapplser_initial_load.withColumn("create_ts", current_timestamp())\
# MAGIC     .withColumn("create_user_id", lit("tmapplser"))\
# MAGIC     .withColumn("last_mod_ts", current_timestamp())\
# MAGIC     .withColumn("last_mod_user_id", lit("tmapplser"))\
# MAGIC     .withColumn("lock_control_no", lit("0")) 
# MAGIC
# MAGIC df_tmapplser_initial_load.count()                     

# COMMAND ----------

df_tmapplser_initial_load.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.silver.tmapplser')

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {trgt_catalog}.silver.tmapplser ")

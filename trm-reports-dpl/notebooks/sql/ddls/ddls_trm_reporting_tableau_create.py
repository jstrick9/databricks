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
trgt_database = 'tableau'

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {trgt_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {trgt_catalog}.{trgt_database}")
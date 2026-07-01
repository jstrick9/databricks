# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ./ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmngfpepp_catalog = common_configs['schema']['tmngfpepp_catalog']
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
print(reporting_catalog)
print(tmngpdb_catalog)
print(altrx_schema)
schema_bronze = "bronze"
schema_silver = "silver"

# COMMAND ----------

df_fpep_fact_altrx = spark.sql(f"""select CATEGORY,
FK_FP_CATEGORY_ID,
FK_FP_GROUP_ID,
TITLE_TX,
SER_NUM,
FP_YEAR,
FK_WRKR_ID,
ACTION_COUNT,
TRANSACTION_NO,
TRANSACTIONAL_LITERAL,
COMPLETED_DT,
GROUP_NAME,
FP_ID,
COMPLETED_TS,
TM_ANALYTICS_TS 
from {altrx_catalog}.{altrx_schema}.fpep_fact """)

# COMMAND ----------

df_fpep_fact_altrx.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.silver.fpep_fact')

# COMMAND ----------

dbutils.notebook.exit(f"Completed initial data load of fpep_fact Table ")

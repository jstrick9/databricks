# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngpdb-conf.yaml"
config_file = "../../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run  ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
data_quality_db = common_configs['schema']['data_quality_catalog']
spark.conf.set('conf.data_quality_db', data_quality_db)
print(f'{data_quality_db=} ')

# COMMAND ----------

df = spark.sql(f"Select * from {data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT")
#df.display()
df1 = df.drop("PROC_VRFCTN_RSLT_ID")

spark.sql(f"DROP TABLE if exists {data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT_OLD")

spark.sql(f"ALTER TABLE {data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT RENAME TO {data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT_OLD ")

df1.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable(f"{data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT")

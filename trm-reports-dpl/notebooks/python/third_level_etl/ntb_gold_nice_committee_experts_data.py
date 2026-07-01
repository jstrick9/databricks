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
#src_catalog = common_configs['schema']['tmngpdb_src_catalog']
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
#spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

df_CE34_33_files = spark.sql(f"""select * from {trgt_catalog}.bronze.nice_committee_experts_dataload""")
display(df_CE34_33_files)

# COMMAND ----------

from pyspark.sql.functions import col, when

df_CE34_33_files = df_CE34_33_files.withColumn(
    "input_file",
    when(col("prop_no").contains("33"), "33")
    .when(col("prop_no").contains("34"), "34")
    .otherwise(None)
)

df_CE34_33_files = df_CE34_33_files.withColumn(
    "decision",
    when(col("decision") == "A", "Approved [A]")
    .when(col("decision") == "A+", "Approved with modification [A+]")
    .when(col("decision") == "R", "Rejected [R]")
    .when(col("decision") == "W", "Withdrawn [W]")
    .otherwise(col("decision"))
)

df_CE34_33_files = df_CE34_33_files.filter(col("input_file").isNotNull()).orderBy("input_file", "prop_no")
display(df_CE34_33_files)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# Add create_ts column to the dataframe
df_CE34_33_files = df_CE34_33_files.withColumn("create_ts", current_timestamp())

# Define the target table path and name
target_table_path = f"s3://bdr-databricks-app-{dbx_env}/eds/delta_tables/{trgt_catalog}/gold/nice_committee_experts_yearly"
target_table_name = f"{trgt_catalog}.gold.nice_committee_experts_yearly"

# Write only new records into the table 
df_CE34_33_files.write.format("delta") \
    .option("path", target_table_path) \
    .option("header", "true") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table_name)

# COMMAND ----------

dbutils.notebook.exit(f"Completed loading ntb_gold_nice_committee_experts_data ")

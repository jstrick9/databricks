# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,Define env variables
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#print(f'{config_file=}')
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Run common function and parameters ntbk
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
#src_catalog = common_configs['schema']['tmngpdb_src_catalog']
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
#spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)
spark.conf.set('conf.cdc_bucket', cdc_bucket)

# COMMAND ----------

# DBTITLE 1,Read xml file into dataframe
try:
    df_all_files = spark.read.format("csv") \
    .options(header='true', inferschema='true', delimiter=',') \
    .option("quote", "\"")\
    .option("escape", "\"")\
    .load(f"s3://{cdc_bucket}/eds/trademark/nice/idml_version_tracking/idmanual.csv")
except Exception as e:
    exception = None
    status = "There are no new files"
    dbutils.notebook.exit(f"status: {status}, exception: {exception}")

# COMMAND ----------

from pyspark.sql.functions import input_file_name
df_all_files_with_filenames = df_all_files.withColumn("filename", input_file_name())
#df_all_files_with_filenames.select("filename").display()

# COMMAND ----------

# DBTITLE 1,Clean column names
from pyspark.sql.functions import col, from_json

# Creating alias for dataframe column names
df_all_files_renamed = df_all_files.select(
    *[col(c).alias(c.replace(" ", "_").lower()) for c in df_all_files.columns]
).withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl"))

#df_all_files_renamed.display(10)

# COMMAND ----------

# DBTITLE 1,Load data into bronze table
df_all_files_renamed.write.format("delta") \
    .option("path", f"s3://{cdc_bucket}/delta_tables/{trgt_catalog}/bronze/nice_idmanual_csv_file_data") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{trgt_catalog}.bronze.nice_idmanual_csv_file_data")

# COMMAND ----------

archive_path = f"s3://{cdc_bucket}/eds/trademark/nice/idml_version_tracking/archive/"

# List of files to move
file_list = [row.filename for row in df_all_files_with_filenames.select("filename").distinct().collect()]

# Move files to archive
for file_path in file_list:
    file_name = file_path.split("idml_version_tracking/")[1]
    dbutils.fs.mv(file_path, archive_path + file_name)

# Display confirmation message
display(dbutils.fs.ls(archive_path))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit(f"Completed loading nice_idmanual_csv_file_data Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select COUNT(*) from trm_reporting_dev.bronze.nice_idmanual_csv_file_data

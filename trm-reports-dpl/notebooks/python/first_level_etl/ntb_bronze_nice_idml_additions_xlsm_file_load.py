# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,Define env variables
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')
#print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common function and parameters ntbk
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
#src_catalog = common_configs['schema']['tmngpdb_src_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
#spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

basePath = f"s3://{cdc_bucket}/eds/trademark/nice/idml_additions_weekly/"

# Function to read a single Excel file
def read_excel(file_path: str) -> DataFrame:
    return spark.read.format("com.crealytics.spark.excel")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(file_path)

# Get the list of file paths directly from the S3 bucket
file_infos = dbutils.fs.ls(basePath)
file_paths = [basePath + file_info.name for file_info in file_infos]
#file_paths

# COMMAND ----------

# Filter the list of file paths to only include .xlsm files
file_paths_xlsm = [file_path for file_path in file_paths if file_path.endswith('.xlsm')]

#Drop null rows
def read_excel(file_path: str) -> DataFrame:
    return spark.read.format("com.crealytics.spark.excel")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("treatEmptyValuesAsNulls", "true")\
        .option("dropInvalid", "true")\
        .load(file_path).dropna(how="all")

try:
    # Use the filtered list of file paths to read each Excel file individually
    excel_files_xlsm = [read_excel(file_path) for file_path in file_paths_xlsm]

    #concatenate all .xlsm Excel files into a single DataFrame
    if excel_files_xlsm:
        df_all_files_xlsm = excel_files_xlsm[0]
        for df in excel_files_xlsm[1:]:
            df_all_files_xlsm = df_all_files_xlsm.unionByName(df)

    # Display the concatenated DataFrame of .xlsm files
    display(df_all_files_xlsm)
except Exception as e:
    exception = None
    status = "There are no new files"
    dbutils.notebook.exit(f"status: {status}, exception: {exception}")

# COMMAND ----------

# DBTITLE 1,Clean column names
from pyspark.sql.functions import col, from_json

# Creating alias for dataframe column names
df_all_files_renamed = df_all_files_xlsm.select(
    *[col(c).alias("codes") if c == "Codes: S, L, G,  M,T, N, O, I" else col(c).alias(c.replace(" ", "_").lower()) for c in df_all_files_xlsm.columns]
).withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl")).drop("_c8")

df_all_files_renamed = df_all_files_renamed.withColumn("class", col("class").cast("integer")).withColumn("date", col("date").cast("date"))
display(df_all_files_renamed)

#df_all_files_renamed.display(10)

# COMMAND ----------

# DBTITLE 1,Load data into bronze table
df_all_files_renamed.write.format("delta") \
    .option("path", f"s3://{cdc_bucket}/delta_tables/{trgt_catalog}/bronze/nice_additions_xlsm_file_data") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable(f"{trgt_catalog}.bronze.nice_additions_xlsm_file_data")

# COMMAND ----------


archive_path = f"s3://{cdc_bucket}/eds/trademark/nice/idml_additions_weekly/archive/"

# List of files to move
#file_list = [row.filename for row in df_all_files_with_filenames.select("filename").distinct().collect()]
file_list = file_paths_xlsm
# Move files to archive
for file_path in file_list:
    file_name = file_path.split("idml_additions_weekly/")[1]
    dbutils.fs.mv(file_path, archive_path + file_name)

# Display confirmation message
display(dbutils.fs.ls(archive_path))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit(f"Completed loading ntb_bronze_nice_idml_additions_xlsm_file_load Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select COUNT(*) from trm_reporting_dev.bronze.nice_additions_xlsm_file_data

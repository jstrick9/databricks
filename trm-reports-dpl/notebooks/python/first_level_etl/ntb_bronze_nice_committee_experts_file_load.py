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
cdc_bucket = common_configs['cdc']['cdc_bucket']
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
#spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)
spark.conf.set('conf.cdc_bucket', cdc_bucket)

# COMMAND ----------

from pyspark.sql.functions import input_file_name
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

basePath = f"s3://{cdc_bucket}/eds/trademark/nice/nice_committee_experts/"

# Function to read a single Excel file
def read_excel(file_path: str) -> DataFrame:
    return spark.read.format("com.crealytics.spark.excel")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(file_path)

# Get the list of file paths directly from the S3 bucket
file_infos = dbutils.fs.ls(basePath)
file_paths = [basePath + file_info.name for file_info in file_infos if file_info.name.endswith('.xlsx')]

if not file_paths:
    status = "There are no new files"
    dbutils.notebook.exit(f"status: {status}, exception: None")

try:
    # Use the filtered list of file paths to read each Excel file individually
    excel_files = [read_excel(file_path) for file_path in file_paths]

    # Concatenate all Excel files into a single DataFrame
    if excel_files:
        df_all_files = excel_files[0]
        for df in excel_files[1:]:
            df_all_files = df_all_files.unionByName(df)

        # Display the concatenated DataFrame
        display(df_all_files)

except Exception as e:
    exception = str(e)
    status = "An error occurred while reading files"
    dbutils.notebook.exit(f"status: {status}, exception: {exception}")

# COMMAND ----------

columns_to_rename = {
    "Cl.": "c1",
    "prop. no./№": "prop_no",
    "basic no. or place / № de base ou endroit": "basic_no_or_place",
    "EN/FR": "en_fr",
    "Existing entry/Entree existante": "existing_entry",
    "New or modified entry/Nouvelle entree ou entree modifiee": "new_or_modified_entry",
    "New Cl./Nlle Cl.": "new_cl",
    "Remarks/Remarques": "remarks",
    "Marked as Amendment": "marked_as_amendment"
}

df_all_files_renamed = df_all_files
for old_name, new_name in columns_to_rename.items():
    df_all_files_renamed = df_all_files_renamed.withColumnRenamed(old_name, new_name)

# Convert all column names to lowercase
df_all_files_renamed = df_all_files_renamed.toDF(*[col.lower() for col in df_all_files_renamed.columns])

# Add additional columns
df_all_files_renamed = df_all_files_renamed.withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl"))

display(df_all_files_renamed)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

# Define the target table path and name
target_table_path = f"s3://{cdc_bucket}/delta_tables/{trgt_catalog}/bronze/nice_committee_experts_dataload"
target_table_name = f"{trgt_catalog}.bronze.nice_committee_experts_dataload"

# Load data
df_all_files_renamed.write.format("delta") \
    .option("path", target_table_path) \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table_name)

# COMMAND ----------

archivePath = f"s3://{cdc_bucket}/eds/trademark/nice/nice_committee_experts/archive/"
basePath = f"s3://{cdc_bucket}/eds/trademark/nice/nice_committee_experts/"

# Function to read a single Excel file
def read_excel(file_path: str) -> DataFrame:
    return spark.read.format("com.crealytics.spark.excel")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(file_path)

# Get the list of file paths directly from the S3 bucket
file_infos = dbutils.fs.ls(basePath)
file_paths = [basePath + file_info.name for file_info in file_infos if file_info.name.endswith('.xlsx')]
# Function to move a file to the archive path
def move_to_archive(file_path: str, archive_path: str):
    file_name = file_path.split("/")[-1]
    dbutils.fs.mv(file_path, archive_path + file_name)

# Move each file to the archive path
for file_path in file_paths:
    move_to_archive(file_path, archivePath)

# COMMAND ----------

dbutils.notebook.exit(f"Completed loading ntb_bronze_nice_committee_experts_file_load Table ")

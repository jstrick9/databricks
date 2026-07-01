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
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Read all files from S3 into Dataframe
from pyspark.sql.functions import input_file_name
from functools import reduce

# List to collect successfully loaded dataframes
dataframes = []

# Define paths and their names for logging (even though /quaterly path is misspelled, this is the actual path folder name with records)
paths = {
    "daily": f"s3://bdr-databricks-app-{dbx_env}/eds/trademark/tmns/reports/daily/*/*/*/*.json",
    "monthly": f"s3://bdr-databricks-app-{dbx_env}/eds/trademark/tmns/reports/monthly/*/*/*.json",
    "quarterly": f"s3://bdr-databricks-app-{dbx_env}/eds/trademark/tmns/reports/quaterly/*/*/*.json",
    "yearly-a": f"s3://bdr-databricks-app-{dbx_env}/eds/trademark/tmns/reports/yearly-a/*/*.json",
    "yearly-f": f"s3://bdr-databricks-app-{dbx_env}/eds/trademark/tmns/reports/yearly-f/*/*.json"
}

# Try to load each path individually
for name, path in paths.items():
    try:
        df = spark.read.option('multiline', 'true').format('json').load(path)
        
        # Check if dataframe actually has data
        if df.head(1):  # Returns empty list if no data
            print(f"Successfully loaded {name}: {df.count()} records")
            dataframes.append(df)
        else:
            print(f"{name}: Path exists but no data found")
            
    except Exception as e:
        print(f"{name}: No files found or error - {str(e)[:100]}")

# Check if we have any data at all
if not dataframes:
    print("No new files available")
    dbutils.notebook.exit("No new data available for loading tmns_json_reports Table")

# Union all successful dataframes
print(f"\nCombining {len(dataframes)} dataframe(s)...")

df_all_files = reduce(
    lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
    dataframes
)

# Add filename column
df_all_files_with_filenames = df_all_files.withColumn("filename", input_file_name())

# COMMAND ----------

# DBTITLE 1,Clean column names to remove space and hyphen
from pyspark.sql.functions import col, from_json

# Creating alias for dataframe column names
df_all_files = df_all_files.select(
    *[col(c).alias(c.replace(" ", "_").replace("-", "").lower()) for c in df_all_files.columns]
).withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl"))

#display(df_all_files)

# COMMAND ----------

# DBTITLE 1,Overwrite data into json table
# MAGIC %md 
# MAGIC df_all_files.write.format("json") \
# MAGIC     .option("path", f"s3://{cdc_bucket}/eds/delta_tables/{trgt_catalog}/bronze/tmns_json_raw_file_data") \
# MAGIC     .mode("append") \
# MAGIC     .saveAsTable(f"{trgt_catalog}.bronze.tmns_json_raw_file_data")

# COMMAND ----------

from pyspark.sql.functions import to_json, col

df_all_files = df_all_files.withColumn("in_email", to_json(col("in_email"))) \
                           .withColumn("in_letter", to_json(col("in_letter")))

df_all_files.write.mode("append").option("mergeSchema", "true").format("json").insertInto(f"{trgt_catalog}.bronze.tmns_json_raw_file_data")

# COMMAND ----------

# DBTITLE 1,Move processed files to archive folder
archive_path = f"s3://{cdc_bucket}/eds/trademark/tmns/reports/archive/"

# List of files to move
file_list = [row.filename for row in df_all_files_with_filenames.select("filename").distinct().collect()]

# Move files to archive
for file_path in file_list:
    file_name = file_path.split("reports/")[1]
    dbutils.fs.mv(file_path, archive_path + file_name)

# Display confirmation message
display(dbutils.fs.ls(archive_path))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit(f"Completed loading tmns_json_file_data Table ")

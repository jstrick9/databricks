# Databricks notebook source
# MAGIC %md
# MAGIC ### MGS Spreadsheet Project (The Madrid Goods & Services Manager (“MGS”) includes a database of pre-approved terms for use in identifications and classifications among 28 IP Office globally. This project reviews the MGS entries for possible inclusion in the USPTO ID Manual.)  Need to track:
# MAGIC - Number of entries reviewed
# MAGIC
# MAGIC - Number  of entries added to IDML (includes tentacle entries)
# MAGIC
# MAGIC - Number  of existing entries CNV/modified

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
edw_scope = common_configs['secrets']['edw_scope']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
reporting_catalog = 'trm_reporting_dev'
print(reporting_catalog)#,run_env)
data_layer = "bronze"

# COMMAND ----------

job_name = 'ntb_load_mgs_data'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# List of files to load from S3
s3_storage1 = f"s3://{cdc_bucket}/eds/trademark/nice/mgs_yearly/ID_Manual_All_Active.csv"
s3_storage2 = f"s3://{cdc_bucket}/eds/trademark/nice/mgs_yearly/MGS_Acceptance_US.csv"

try:
    active_df = spark.read.csv(s3_storage1, header=True)
except Exception as e:
    raise Exception(f"Failed to load file from {s3_storage1}") from e

try:
    df_acceptance = spark.read.csv(s3_storage2, header=True)
except Exception as e:
    raise Exception(f"Failed to load file from {s3_storage2}") from e

# COMMAND ----------

from pyspark.sql.functions import col

# Function to replace spaces and dashes in column names with underscores
def standardize_column_names(df):
    for old_name in df.columns:
        new_name = old_name.replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(old_name, new_name)
    return df


# COMMAND ----------

# Applying the function to each DataFrame
df_acceptance = standardize_column_names(df_acceptance)
df_active = standardize_column_names(active_df)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from pyspark.sql.functions import to_date

# Example: Standardizing date format
df_active = df_active.withColumn("Start_Effective_Date", to_date("Start_Effective_Date", "MM/dd/yyyy")) \
                        .withColumn("End_Effective_Date", to_date("End_Effective_Date", "MM/dd/yyyy")) \
                            .drop("Start Effective Date","End Effective Date")

# df_active_standardized.display()

# COMMAND ----------

from pyspark.sql.functions import year, month, col, when

# Adding Fiscal_year column based on Start_Effective_Date
df_active = df_active.withColumn(
    "Fiscal_year",
    when(month(col("Start_Effective_Date")) >= 10, year(col("Start_Effective_Date")) + 1)
    .otherwise(year(col("Start_Effective_Date")))
)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# List of string columns in df_acceptance you want to apply the replacement
string_columns = [col_name for col_name, dtype in df_acceptance.dtypes if dtype == 'string']

# Replace "►" with "-" in all string columns
for col_name in string_columns:
    df_acceptance = df_acceptance.withColumn(col_name, regexp_replace(col_name, "►", "-"))


# List of string columns in df_active you want to apply the replacement
string_columns = [col_name for col_name, dtype in df_active.dtypes if dtype == 'string']

# Replace """""""" with "-" in all string columns
for col_name in string_columns:
    df_active = df_active.withColumn(col_name, regexp_replace(col_name, '"""', " "))

# COMMAND ----------



# COMMAND ----------

# for p1,p2 in zip(spark.sql(f'select * from {reporting_catalog}.silver.df_active_table').columns, df_active1.columns):
#     print(p1,p2)

# COMMAND ----------

try:
    df_acceptance.select(spark.sql(f'select * from {reporting_catalog}.silver.df_acceptance_table').columns).write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.silver.df_acceptance_table')
    df_active.select(spark.sql(f'select * from {reporting_catalog}.silver.df_active_table').columns).write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.silver.df_active_table')
    print("MGS data load successfully completed")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading MGS Dashboard Table ")

# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
#dbutils.widgets.text("report_internal","")#M/Q/Y

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl-tmns/notebooks/config/dev/trmreports-conf.yaml"
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

# DBTITLE 1,Get newly added data from bronze table
spark.sql(f"refresh table {trgt_catalog}.bronze.tmns_json_raw_file_data")

df_all_files = spark.sql(f"select * from {trgt_catalog}.bronze.tmns_json_raw_file_data \
                         where create_ts > (select nvl(max(update_ts),to_timestamp('1999-01-01','yyyy-MM-dd'))from {trgt_catalog}.gold.tmns_notice_counts )")
df_all_files.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse in_email column

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
filtered_df = df_all_files.filter(F.col('in_email').isNull())

filtered_df_notnull = df_all_files.filter(F.col('in_email').isNotNull())

array_item_schema = \
  spark.read.json(filtered_df_notnull.rdd.map(lambda row: row['in_email'])).schema

json_array_schema = ArrayType(array_item_schema, True)

arrays_df = filtered_df_notnull.select(*filtered_df_notnull.columns,F.from_json('in_email', json_array_schema).alias('json_arrays'))

objects_df = arrays_df.select( *arrays_df.columns,F.explode('json_arrays').alias('objects'))

# COMMAND ----------

df_all_files_email = objects_df.select("date_time_range","objects.*","month","quarter","report_type","total_notices_sent_in_email","total_notices_sent_in_letter","total_records","year","create_ts","create_user_id").withColumn("input_format", lit("in_email")).drop("_corrupt_record")
#df_all_files_email.count()

filtered_df_email = filtered_df.select("date_time_range","month","quarter","report_type","total_notices_sent_in_email","total_notices_sent_in_letter","total_records","year","create_ts","create_user_id").withColumn("input_format", lit("in_email"))
#filtered_df_email.count()

df_all_files_email = df_all_files_email.unionByName(filtered_df_email, allowMissingColumns = True)
df_all_files_email.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parse in_letter column

# COMMAND ----------

filtered_df = df_all_files.filter(F.col('in_letter').isNull())

filtered_df_notnull = df_all_files.filter(F.col('in_letter').isNotNull())

array_item_schema = \
  spark.read.json(filtered_df_notnull.rdd.map(lambda row: row['in_letter'])).schema

json_array_schema = ArrayType(array_item_schema, True)

arrays_df = filtered_df_notnull.select(*filtered_df_notnull.columns,F.from_json('in_letter', json_array_schema).alias('json_arrays'))

objects_df = arrays_df.select( *arrays_df.columns,F.explode('json_arrays').alias('objects'))

# COMMAND ----------

df_all_files_letter = objects_df.select("date_time_range","objects.*","month","quarter","report_type","total_notices_sent_in_email","total_notices_sent_in_letter","total_records","year","create_ts","create_user_id").withColumn("input_format", lit("in_letter")).drop("_corrupt_record")
#df_all_files_letter.count()

filtered_df_letter = filtered_df.select("date_time_range","month","quarter","report_type","total_notices_sent_in_email","total_notices_sent_in_letter","total_records","year","create_ts","create_user_id").withColumn("input_format", lit("in_letter"))
#filtered_df_letter.count()

df_all_files_letter = df_all_files_letter.unionByName(filtered_df_letter, allowMissingColumns = True)
df_all_files_letter.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Union in_Email and in_Letter

# COMMAND ----------

# DBTITLE 1,Explode in_email and in_letter json columns
df_all_files_explode = df_all_files_email.unionByName(df_all_files_letter, allowMissingColumns = True).withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("etl")).withColumn("update_ts", current_timestamp()).withColumn("update_user_id", lit("etl"))
df_all_files_explode.count()

# COMMAND ----------

# DBTITLE 1,Sanitize column names to remove special characters and dedup the rows
from pyspark.sql.functions import col, expr

# Function to sanitize column names by removing special characters and replacing spaces with underscores
def sanitize_column_name(col_name, idx):
    sanitized_name = (col_name.replace(" ", "_")
                      .replace("-", "_")
                      .replace(".", "_")
                      .replace('–', '_')
                      .replace('(', '')
                      .replace(')', '')
                      .replace('/', '_')
                      .replace('&', '_').lower())
    return f"{sanitized_name}"

# Applying the function to each column name in the dataframe with a unique index
df_all_files_explode_sanitized = df_all_files_explode.select(
    *[expr(f"`{c}`").alias(sanitize_column_name(c, idx)) for idx, c in enumerate(df_all_files_explode.columns)]
)

#display(df_all_files_explode_sanitized)


# Function to replace double underscores with single underscore and remove last character if it is an underscore
def clean_column_name(col_name):
    cleaned_name = col_name.replace("__", "")
    if cleaned_name.endswith("_"):
        cleaned_name = cleaned_name[:-1]
    return cleaned_name

# Applying the function to each column name in the dataframe
df_all_files_explode_sanitized_cleaned = df_all_files_explode_sanitized.select(
    *[expr(f"`{c}`").alias(clean_column_name(c)) for c in df_all_files_explode_sanitized.columns]
)

#display(df_all_files_explode_sanitized_cleaned)
#df_all_files_explode_sanitized_cleaned.columns

from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Dedup the rows 
windowSpec = Window.partitionBy("Date_Time_Range", "input_format")\
                   .orderBy(col("update_ts").desc())

df_preprocessed = df_all_files_explode_sanitized_cleaned.withColumn("rn", row_number().over(windowSpec))\
                                                        .filter(col("rn") == 1)\
                                                        .drop("rn")
#df_preprocessed.display()

# Function to sanitize column names by removing special characters and replacing spaces with underscores
def sanitize_column_name(col_name, idx):
    sanitized_name = (col_name.replace(" ", "_")
                      .replace("-", "_")
                      .replace(".", "_")
                      .replace('–', '_')
                      .replace('(', '')
                      .replace(')', '')
                      .replace('/', '_')
                      .replace('&', '_').lower())
    return f"{sanitized_name}"

# Applying the function to each column name in the dataframe with a unique index
df_all_files_explode_sanitized = df_all_files_explode.select(
    *[expr(f"`{c}`").alias(sanitize_column_name(c, idx)) for idx, c in enumerate(df_all_files_explode.columns)]
)

#display(df_all_files_explode_sanitized)


# Function to replace double underscores with single underscore and remove last character if it is an underscore
def clean_column_name(col_name):
    cleaned_name = col_name.replace("__", "")
    if cleaned_name.endswith("_"):
        cleaned_name = cleaned_name[:-1]
    return cleaned_name

# Applying the function to each column name in the dataframe
df_all_files_explode_sanitized_cleaned = df_all_files_explode_sanitized.select(
    *[expr(f"`{c}`").alias(clean_column_name(c)) for c in df_all_files_explode_sanitized.columns]
)

#display(df_all_files_explode_sanitized_cleaned)
#df_all_files_explode_sanitized_cleaned.columns

from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Dedup the rows 
windowSpec = Window.partitionBy("Date_Time_Range", "input_format")\
                   .orderBy(col("update_ts").desc())

df_preprocessed = df_all_files_explode_sanitized_cleaned.withColumn("rn", row_number().over(windowSpec))\
                                                        .filter(col("rn") == 1)\
                                                        .drop("rn")
df_preprocessed.count()

# COMMAND ----------

from pyspark.sql.functions import quarter, concat, lit, col, when, split, coalesce

# Rename values in the 'report_type' column
df_preprocessed = df_preprocessed.withColumn(
    "report_type",
    when(col("report_type") == "YEARLY-F", "FISCAL YEAR")
    .when(col("report_type") == "YEARLY-A", "CALENDAR YEARLY")
    .otherwise(col("report_type"))
)

# Create a new column 'date_month_year' with only date part from 'date_time_range' until the space
df_preprocessed = df_preprocessed.withColumn(
    "daily_date",
    when(col("report_type") == "DAILY", split(col("date_time_range"), " ")[0]).otherwise(None)
)

# Update 'total_notices_sent_in_email' and 'total_notices_sent_in_letter' to 0 when they are NULL
df_preprocessed = df_preprocessed.withColumn(
    "total_notices_sent_in_email",
    coalesce(col("total_notices_sent_in_email"), lit("0 (0%)"))
).withColumn(
    "total_notices_sent_in_letter",
    coalesce(col("total_notices_sent_in_letter"), lit("0 (0%)"))
)

df_preprocessed.count()

# COMMAND ----------

# DBTITLE 1,Merge dataframe into gold table
from delta.tables import DeltaTable

# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Identify the Delta table
delta_table = DeltaTable.forName(spark, f"{trgt_catalog}.gold.tmns_notice_counts")

# Exclude 'create_ts' from whenMatchedUpdateAll and ensure columns exist in target table
target_columns = [field.name for field in delta_table.toDF().schema.fields]
update_columns = {col: "source." + col for col in df_preprocessed.columns 
                  if col != "create_ts" and col in target_columns}

merge_condition = "target.Date_Time_Range = source.Date_Time_Range AND target.input_format = source.input_format"

# Merge the data with schema evolution
delta_table.alias("target").merge(
    df_preprocessed.alias("source"),
    condition=merge_condition
).whenMatchedUpdate(set=update_columns).whenNotMatchedInsertAll().execute()

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit(f"Completed loading tmns_notice_counts Table ")

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
trgt_catalog = common_configs["schema"]["trgt_catalog"]
src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]

spark.conf.set('conf.dbx_env', dbx_env)

dq_catalog = common_configs['schema']['data_quality_catalog']
print(trgt_catalog, src_catalog)

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_zombie_report'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

import re
from pyspark.sql.functions import udf, col, regexp_extract, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

def extract_serial_numbers(text):
    text = text.replace('&nbsp;', ' ').replace('&amp;', ' ')
    text_no_tags = re.sub(r'<[^>]+>', '', text)
    # Replace 'and' and '&' with commas for consistent splitting
    text_no_tags = re.sub(r'\s*(and|&)\s*', ',', text_no_tags)
    matches = re.findall(r'- U\.S\. Application Serial No\(s\)\.?\s*([0-9,\s]+)', text_no_tags)
    matches += re.findall(r'- U\.S\. Application Serial Nos\.?\s*([0-9,\s]+)', text_no_tags)
    numbers = []
    for match in matches:
        numbers += re.findall(r'\d+', match)
    filtered = [n for n in numbers if len(n) in (8, 9) and n[0] >= '7']
    return ', '.join(filtered)

extract_serial_numbers_udf = udf(extract_serial_numbers, StringType())

a = spark.table(f"{src_catalog}.bronze.draft_document")
b = spark.table(f"{src_catalog}.bronze.draft_document_version_compnt")
c = spark.table(f"{src_catalog}.bronze.document_component")

joined = (
    a.join(b, a.draft_document_id == b.fk_draft_document_id, "left")
     .join(c, b.fk_document_component_id == c.document_component_id, "left")
     .filter(
         (a.draft_document_nm.like("Suspension Letter [%")) &
         (c.fk_document_component_type_cd == "FREE") &
         (c.document_component_ct == "DRAFT")
     )
)

joined = joined.withColumn(
    "ser_num",
    regexp_extract(col("draft_document_nm"), r"\[(\d{8})\]", 1)
)

window = Window.partitionBy("ser_num").orderBy(col("draft_document.last_mod_ts").desc())
joined = joined.withColumn("rn", row_number().over(window))

df = joined.filter(col("rn") == 1).select(
    "draft_document_nm", "document_component_tx", "ser_num"
)

df = df.withColumn(
    "serial_number",
    regexp_extract(col("draft_document_nm"), r"\[(\d{8})\]", 1)
)
df = df.withColumn("extracted_serial_numbers", extract_serial_numbers_udf(col("document_component_tx")))

display(df.select("draft_document_nm", "serial_number", "document_component_tx", "extracted_serial_numbers"))

# COMMAND ----------

from pyspark.sql.functions import col, min, max, substring, date_format, datediff, current_date, lit

be_df = spark.table(f"{src_catalog}.bronze.business_event")
reason_df = spark.table(f"{src_catalog}.bronze.stnd_business_event_reason")

prosecution_history = be_df.join(
    reason_df,
    be_df.fk_business_event_reason_id == reason_df.business_event_reason_id
).select(
    substring(col("cfk_object_gid"), -8, 8).alias("serial_number"),
    col("title_tx").alias("cm_desc"),
    col("legacy_cm_ent_cd").alias("ph_action_code"),
    date_format(col("effective_ts"), "yyyy-MM-dd").alias("ph_action_date")
)

suspend_dates = prosecution_history.filter(
    col("ph_action_code") == "RCSC"
).groupBy(
    "serial_number", "cm_desc"
).agg(
    min("ph_action_date").alias("first_suspend"),
    max("ph_action_date").alias("last_suspend")
)

latest_action = prosecution_history.groupBy(
    col("serial_number").alias("latest_serial_number")
).agg(
    max("ph_action_date").alias("latest_date")
)

joined_df = suspend_dates.join(
    latest_action,
    (suspend_dates.serial_number == latest_action.latest_serial_number) &
    (suspend_dates.last_suspend == latest_action.latest_date)
).select(
    suspend_dates.serial_number,
    suspend_dates.first_suspend,
    suspend_dates.last_suspend,
    suspend_dates.cm_desc
)

df = df.withColumnRenamed("extracted_serial_numbers", "prior_pending_serial_number")


final_df = joined_df.join(
    df,
    joined_df.serial_number == df.serial_number,
    how="left"
).select(
    joined_df.serial_number,
    joined_df.first_suspend,
    joined_df.last_suspend,
    lit("RCSC").alias("ph_action_code"),
    joined_df.cm_desc,
    datediff(current_date(), joined_df.first_suspend).alias("days_since_first_suspend"),
    df.prior_pending_serial_number
)
display(final_df)

# COMMAND ----------

target_table_name = f"{trgt_catalog}.gold.zombie_report"
final_df.write.mode("overwrite").format("delta").insertInto(target_table_name)

# COMMAND ----------

recs_count = final_df.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

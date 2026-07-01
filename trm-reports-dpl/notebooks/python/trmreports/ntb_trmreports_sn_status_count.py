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
edw_scope = common_configs["secrets"]["edw_scope"]
primary_email, cc_email = common_configs["alerting"]["sn_status_count"]["email"], common_configs["alerting"]["sn_status_count"]["cc"]
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']
# print(isinstance(primary_email, str))
#print(isinstance(cc_email, str))
print(trgt_catalog, src_catalog, edw_scope, primary_email, cc_email, altrx_schema)

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_sn_status_count'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

input_1df = spark.sql(f"""
SELECT legacy_status_cd AS AM_STAT, serial_num_tx AS AM_SER_NUM, status_dt AS AM_STAT_DT, cl.class, ml.pendency_cal_start_dt, ml.ser_num, cl.ser_num AS cl_ser_num
FROM {src_catalog}.bronze.trademark AS tm 
INNER JOIN {trgt_catalog}.silver.milestone AS ml ON CAST(ml.ser_num AS varchar(8)) = tm.serial_num_tx
INNER JOIN {trgt_catalog}.silver.class AS cl ON tm.serial_num_tx = CAST(cl.ser_num AS varchar(8))
WHERE (legacy_status_cd = 630 OR legacy_status_cd = 631 OR legacy_status_cd = 638)
AND first_action_dt_ph IS NULL
AND class_status NOT ILIKE 'inactive%'
""")
#display(input_1df)

# COMMAND ----------

from pyspark.sql.functions import countDistinct,first

input_2df = input_1df.groupBy("cl_ser_num").agg(
    countDistinct("class").alias("class_count"),
    first("AM_SER_NUM").alias("AM_SER_NUM"),
    first("class").alias("class"),
)

#display(input_2df)

# COMMAND ----------

from pyspark.sql import functions as F
renamed_df1 = input_1df.select(*[F.col(col).alias(col) for col in input_1df.columns])
renamed_df2 = input_2df.select(*[F.col(col).alias(col) for col in input_2df.columns])

joined_df = renamed_df1.join(renamed_df2, renamed_df1["ser_num"] == renamed_df2["cl_ser_num"], "inner") \
                       .drop(renamed_df2["cl_ser_num"])

# COMMAND ----------

from pyspark.sql.functions import asc, date_format

result_grouped = joined_df.withColumnRenamed("ser_num", "sn") \
                       .withColumnRenamed("AM_STAT", "status") \
                       .withColumnRenamed("class_count", "classes")\
                       .withColumnRenamed("AM_STAT_DT", "status_dt") \
                       .withColumnRenamed("pendency_cal_start_dt", "filing_dt") \
                       .select("sn", "status", "status_dt", "filing_dt", "classes") \
                       .withColumn("status_dt", date_format("status_dt", "yyyy-MM-dd")) \
#display(result_grouped)

# COMMAND ----------

result_grouped = result_grouped.dropDuplicates(["sn", "status", "status_dt", "filing_dt", "classes"]).orderBy(asc("sn"))
#display(result_grouped)

# COMMAND ----------

target_table_name = f"{trgt_catalog}.gold.sn_status"
result_grouped.write.mode("overwrite").format("delta").insertInto(target_table_name)

# COMMAND ----------

email_output = result_grouped

# COMMAND ----------

print(f"Sending email to: {primary_email} [primary], {cc_email} [cc]")

from_addr = "trademark_analytics@uspto.gov"
email_subj = f'SN_mail_output'
email_body = """See Attached.<br>Logic Status 630, 631, or 638.<br>No first action"""
attachments = [(email_output, "SN_status_count.xlsx", "excel")]

# Send the email with the attachment
send_email_report(
    job_nm = job_name,
    subject = email_subj,
    send_from = from_addr,
    send_to = primary_email,
    send_to_cc=cc_email,
    html_body= email_body,
    attachments = attachments
)

# COMMAND ----------

# data quality entry
#tbl1 = f"{trgt_catalog}.gold.sn_status"
#tbl2 = f"hive_metastore.{altrx_schema}.sn_status"
#key_cols = ['sn']
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

recs_count = result_grouped.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

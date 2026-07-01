# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Set parameter values
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['reporting_catalog']
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
tqr_catalog = common_configs['schema']['tqr_catalog']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_second_level_tqr_detail_metrics'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Input

# COMMAND ----------

# 118
df_tqr_detail_metrics = spark.sql(f"select * from {tqr_catalog}.gold.tqr_detail_metrics")

# COMMAND ----------

# 104, 103
metrics_ct = df_tqr_detail_metrics.count()

if metrics_ct == 0:
    # end job control with failure
    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', 0,"job failed - tqr detail metrics table is empty")
    dbutils.notebook.exit("Exit notebook - tqr detail metrics table is empty")

# COMMAND ----------

# 108
query_quality_review = """
select JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(quality_review.review_form_json_doc, "$.goFinalDecision"), "$.goFinal")) as go_final,
quality_review.quality_review_id 
from tqr.quality_review"""

df_quality_review = read_data_from_mysql_conn_dsu(query_quality_review, "tqr")

# COMMAND ----------

# 109
df_109 = df_tqr_detail_metrics.join(df_quality_review, df_tqr_detail_metrics.qualityreviewidentifier == df_quality_review.quality_review_id, "left")

# COMMAND ----------

# 116
df_116 = df_109.select('eventinventoryidentifier',
 'qualityreviewidentifier',
 'reviewtypecode',
 'trademarkserialnumber',
 'eventdatetime',
 'examineremployeenumber',
 'organizationcode',
 'searchcompleteindicator',
 'revieweremployeenumber',
 'lastreviewdatetime',
 'assigndatetime',
 'completedatetime',
 'financialyear',
 'financialquarternumber',
 'missedtagelementnamebag',
 'newtagelementnamebag',
 'unsoundtagelementnamebag',
 'soundtagelementnamebag',
 'evidencedeficienttagelementnamebag',
 'evidencesatisfactorytagelementnamebag',
 'evidenceexcellenttagelementnamebag',
 'writingdeficienttagelementnamebag',
 'writingsatisfactorytagelementnamebag',
 'writingexcellenttagelementnamebag',
 'searchsufficientindicator',
 'qualitymetricdeficientindicator',
 'mississueindicator',
 'newissueindicator',
 'refusalunsoundindicator',
 'substantivedeficientindicator',
 'proceduraldeficientindicator',
 'overalldeficientindicator',
 'overallexcellentindicator',
 'evidencedeficientindicator',
 'evidencesatisfactoryindicator',
 'evidenceexcellentindicator',
 'writingdeficientindicator',
 'writingsatisfactoryindicator',
 'writingexcellentindicator',
 'substantiveerrorindicator',
 'satisfactoryindicator',
 'findingindicator',
 date_trunc('SECOND', col('createdatetime')).alias('createdatetime'),
 'createuseridentifier',
 date_trunc('SECOND' ,col('lastmodifieddatetime')).alias('lastmodifieddatetime'),
 'lastmodifieduseridentifier',
 'go_final',
 col('quality_review_id').astype(StringType()).alias("quality_review_id")
).distinct()

# COMMAND ----------

# add audit columns
df_116 = df_116.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

# 100
df_116.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.tqr_detail_metrics")

# COMMAND ----------

# 102
min_reviewtime = df_tqr_detail_metrics.groupBy().agg(min("lastreviewdatetime")).collect()[0][0]
max_reviewtime = df_tqr_detail_metrics.groupBy().agg(max("lastreviewdatetime")).collect()[0][0]

ct_schema = StructType([StructField("min_lastreviewdatetime", TimestampType()), StructField("max_lastreviewdatetime", TimestampType()), StructField("record_ct", IntegerType())])

df_102 = spark.createDataFrame([[min_reviewtime, max_reviewtime, metrics_ct]], ct_schema)

# COMMAND ----------

# 106
df_106 = df_102.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

# 105
df_106.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.tqr_detail_metrics_counts")

# COMMAND ----------

# DBTITLE 1,End Job Control
# end job control
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', metrics_ct,"job completed successfully")

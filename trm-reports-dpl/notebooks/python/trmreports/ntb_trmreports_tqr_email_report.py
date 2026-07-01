# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmworker_catalog = common_configs['schema']['tmworker_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
to_addr = common_configs['alerting']['tqr_email_alert']['email']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_tqr_email_report'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

tqr = spark.sql(f"select * from {reporting_catalog}.silver.tqr_detail_metrics")

filings = spark.sql(f"select * from {reporting_catalog}.gold.filings_dashboard")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

df_8 = filings.select(
    "ser_num",
    col("pendency_cal_start_dt").alias("filing_dt"),
    "filing_method_filed"
).distinct()

# COMMAND ----------

df_out = tqr.join(df_8, tqr.trademarkserialnumber == df_8.ser_num)

# COMMAND ----------

df_out = df_out.select('eventinventoryidentifier',
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
 'createdatetime',
 'createuseridentifier',
 'lastmodifieddatetime',
 'lastmodifieduseridentifier',
 'go_final',
 'quality_review_id',
 'filing_dt',
 'filing_method_filed',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

# COMMAND ----------

from_addr = 'Trademark_Analytics@uspto.gov'
file_nm = 'TQR Output.xlsx'
email_body = 'See Attached'
email_subj = 'Auto-Generated: TQR Output'
attachments = [(df_out, file_nm, 'excel')]


send_email_report(
    job_nm = job_name,
    subject = email_subj,
    send_from = from_addr,
    send_to = to_addr,
    html_body= email_body,
    attachments = attachments
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output

# COMMAND ----------

df_out.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.tqr_email_report")

# COMMAND ----------

#############################################################################################
# 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
# Allows disabling Alteryx workflow schedule fully, saving resources.                       #
#############################################################################################


# # data quality entry
# tbl1 = f"hive_metastore.{altrx_schema}.tqr_email_report" 
# tbl2 = f"{reporting_catalog}.gold.tqr_email_report"
# key_cols = ['qualityreviewidentifier']
 
# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
# print(dq_result)

# COMMAND ----------

# end job control
recs_count = df_out.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

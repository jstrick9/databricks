# Databricks notebook source
from pyspark.sql.functions import regexp_substr

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
reporting_catalog = common_configs['schema']['trgt_catalog']
trm_rpt_catalog = common_configs['schema']['trm_reporting_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
email_addr = common_configs['alerting']['kingpin_status_alert']["email"]

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_kingpin'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

from datetime import datetime
today=datetime.today()
fy_start=f"{today.year if today.month >= 10 else today.year -1 }-10-01"

# COMMAND ----------


query = f"""
SELECT ser_num,
       CAST(STATUS_DT AS DATE) AS STATUS_DT,
       CAST(create_ts AS DATE) AS create_ts,
       law_office,
       CASE WHEN am_stat IS NOT NULL AND (am_stat = 901 OR am_stat = 642) THEN 'Status Changed' ELSE 'No Change' END AS Kingpin_Status
FROM {trm_rpt_catalog}.silver.bibliography
WHERE LAW_OFFICE is NOT NULL AND  (am_stat = 901 OR am_stat = 642)
AND CAST(STATUS_DT as DATE) >= '{fy_start}'
"""

df=spark.sql(query)
df.show()
df.count()

# COMMAND ----------

if df.count() > 0:
    from_addr = 'trademark_analytics@uspto.gov'
    file_nm = 'Kingpin.xlsx'
    email_body = """I'm happy to let you know that our workflow has successfully completed. This workflow monitors changes in serial number statuses, specifically tracking transitions to statuses 642 and 901 to ensure compliance with KingPin regulations. You can view the results at your convenience by accessing the attached excel file. Please feel free to reach out if you have any questions or need further assistance. This is automated workflow that runs daily.
    Sincerely Yours,
    Ashish """
    email_subj = 'Auto-Generated: Kingpin Status'
    send_mail(from_addr,email_addr,from_addr, email_subj, email_body, df, file_nm)
else:
    print("No Status changed ,email not sent.")

# COMMAND ----------

df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.kingpin")

# COMMAND ----------

# data quality entry
#tbl1 = f"hive_metastore.{altrx_schema}.kingpin"
#tbl2 = f"{reporting_catalog}.gold.kingpin"
#key_cols = ['ser_num']
 
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

# end job control
recs_count = df.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

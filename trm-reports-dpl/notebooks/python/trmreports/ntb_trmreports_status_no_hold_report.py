# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
#spark.catalog.clearCache()

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,function to generate random checkpoint folder name
def generate_64bit_ID()-> int:
    return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
CHK_POINT_DIR = "/tmp/checkpoints/post_reg_etl/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
import tempfile
import os
from email.mime.base import MIMEBase
from email import encoders
import shutil
from pyspark.sql.functions import col

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
receiver_email = common_configs['alerting']['sn_status_nohold']['email']
env = dbx_env.upper()

emailid = receiver_email
print(f"{trgt_catalog=},{src_catalog=},{emailid=},{altrx_schema=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_status_no_hold_report'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get Class Data

# COMMAND ----------

df_src_data = spark.sql(f"""Select one.*, two.Class_Count as Classes from (
select cast(ml.ser_num as string) as SN,
ml.pendency_cal_start_dt as Filing_Dt,
--DATEDIFF(ml.Pendency_Cal_End_Dt,ml.DOCK_DT)/30.42 as processing_pend,
cast(tm.STATUS_DT as date)  as Status_Dt,
tm.LEGACY_STATUS_CD as Status
from {src_catalog}.bronze.trademark tm inner join {trgt_catalog}.silver.milestone ml on tm.SERIAL_NUM_TX = cast(ml.ser_num as string)
where ml.first_action_dt_ph is null and 
((tm.LEGACY_STATUS_CD = 630) 
  or (tm.LEGACY_STATUS_CD = 631) 
  or (tm.LEGACY_STATUS_CD = 638))
) as one 
inner join (select cast (ser_num as string), 
count(distinct class) as Class_Count from {trgt_catalog}.silver.class where class_status not ilike '%inactive%' 
group by ser_num) as two on one.SN = two.ser_num""")

SN_Status = df_src_data.orderBy('SN')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get On Hold Data

# COMMAND ----------

df_onhold = spark.sql(f"""select * from {trgt_catalog}.silver.on_hold""")

# COMMAND ----------

SN_Status_No_Hold = df_src_data.join(df_onhold,df_src_data.SN==df_onhold.ath_ser_num,"left_anti").selectExpr("SN","Status","Status_Dt","Filing_Dt","Classes")
SN_Status_No_Hold = SN_Status_No_Hold.orderBy('Status_Dt','SN')
#SN_Status_No_Hold.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Send data in email attachment

# COMMAND ----------

from_addr= 'trademark_analytics@uspto.gov'

if df_src_data.count() > 0:    
    parms = {}

    # Save the DataFrame to a temporary directory as an Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath1 = f"{tmpdir}/SN_Status.xlsx"
        SN_Status.toPandas().to_excel(filepath1, index=False)

        filepath2 = f"{tmpdir}/SN_Status_No_Hold.xlsx"
        SN_Status_No_Hold.toPandas().to_excel(filepath2, index=False)

        attachments = [filepath1, filepath2]
        
        email_body = """See Attached <br>
        Logic <br>
        Status 630, 631, or 638 <br>
        No first action"""

        email_subj = f'Auto-Generated: SN Status - {env}'

        # Send the email with the attachment
        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to = emailid,
            html_body= email_body,
            attachments = attachments
        )
else:
    print("No email notification sent")


# COMMAND ----------

SN_Status.write.mode("overwrite").format("delta").saveAsTable(f"{trgt_catalog}.gold.sn_status_hold")
SN_Status_No_Hold.write.mode("overwrite").format("delta").saveAsTable(f"{trgt_catalog}.gold.sn_status_no_hold")

# COMMAND ----------

# # data quality entry
# tbl1 = f"{trgt_catalog}.gold.sn_status_hold"
# tbl2 = f"hive_metastore.{altrx_schema}.sn_status_hold"
# key_cols = ['SN']
 
# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
# print(dq_result)

# COMMAND ----------

# data quality entry
#tbl1 = f"{trgt_catalog}.gold.sn_status_no_hold"
#tbl2 = f"hive_metastore.{altrx_schema}.sn_status_no_hold"
#key_cols = ['SN']
 
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

recs_count = SN_Status.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

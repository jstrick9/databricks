# Databricks notebook source
# DBTITLE 1,Install openpyxl Package for testing in notebook
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# DBTITLE 1,Set Environment and Rundate Widgets
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl-tm-expired_prod_fix/notebooks/config/dev/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common functions and parameters
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Set Config details
common_configs = read_yaml(config_file)
trm_reporting_catalog = common_configs['schema']['trm_reporting_catalog']
tmngpdb_src_catalog = common_configs['schema']['tmngpdb_src_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
env = dbx_env.upper()

print(f"{trm_reporting_catalog=},{tmngpdb_src_catalog=}, {dq_catalog=}")
spark.conf.set('conf.catalog', trm_reporting_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Start Job Control with Current Timestamp
# set current time for job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
job_start_ts = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = "ntb_trmreports_email_address_for_cx_survey"

control_dt = begin_job_cntl(f"{trm_reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

from pyspark.sql import Row
data = "100 - Unassigned,101 - Assigned,102a - Forwarded to LL/Marked as Informal,102b - LL determined that this application does not contain informal,103 - submitted,103a - The application has been fast tracked,110a - Application is determined to have informal by LL (not insufficient fee),110b - Application is determined to have informal (insufficient fee),112e - Case is undergoing TQR Quality Review"

statuses = data.split(',')
rows = [Row(status_code=status.split(' - ')[0], status_desc=status.split(' - ')[1]) for status in statuses]
df_status = spark.createDataFrame(rows)
display(df_status)
df_status.createOrReplaceTempView("temp_status_list")

# COMMAND ----------

df_pex = spark.sql(f"""
select pex_outer.ser_num,
pendency_cal_start_dt,
dock_dt,
NWOS_DT,
cm_prcd_num,
tm_worker_eid,
pre_exam_status,
status_desc,
assignee,
pre_exam_received_ts,
ath_hold_status,
ath_hold_docket,
ath_active_status,
ath_last_upd_dt,
 min_pendency_cal_start_dt,
 min (case when concatenated_ph_action_code like '%NWOS%' then null else 
  (case when 
pre_exam_received_ts is not null
and disposal_type is null 
and status_desc not in ('The application has been fast tracked','submitted') 
and dead_mark_in ='N'
and ath_active_status is  null
and ath_hold_docket is  null 
and dock_dt is  null
and pendency_cal_start_dt > to_date ('2015-01-01','yyyy-MM-dd')
then pendency_cal_start_dt end)  end) OVER () as min_pre_exam_received_ts,
 --min_pre_exam_received_ts,
 --min_pullable_case_date,
min (case when concatenated_ph_action_code like '%NWOS%' then null else 
  (case when 
disposal_type is null 
and status_desc in ('The application has been fast tracked','submitted') 
and dead_mark_in ='N'
and ath_active_status is  null
and ath_hold_docket is  null 
and dock_dt is  null
and pendency_cal_start_dt > to_date ('2015-01-01','yyyy-MM-dd')
then pendency_cal_start_dt end)  end) OVER () as min_pullable_case_date,
 filing_dt,
 create_ts,
create_user_id,
 dead_mark_in, 
 disposal_type,
 calendar_day,daily_teas_processed  ,
ph_action_date, ph_action_code  
from
(
 select pex.*,
 mark.dead_mark_in, 
 mark.disposal_type,
 --ph.ph_action_date, ph.ph_action_code ,
 pea.calendar_day,pea.daily_teas_processed               
from
(SELECT distinct
pea.ser_num,
pendency_cal_start_dt,
dock_dt,
NWOS_DT,
cm_prcd_num,
tm_worker_eid,
pre_exam_status,
status_desc,
assignee,
pre_exam_received_ts,
ath_hold_status,
ath_hold_docket,
ath_active_status,
ath_last_upd_dt,
min(case when dock_dt is  null and ath_hold_docket is  null and ath_active_status is  null and dead_mark_in ='N' then pendency_cal_start_dt end)over ()  as min_pendency_cal_start_dt,
min(case when dock_dt is  null and ath_hold_docket is  null and ath_active_status is  null and dead_mark_in ='N' then pre_exam_received_ts end) over() as min_pre_exam_received_ts,
--oldest pullable date
--min(case when 
--disposal_type is null 
--and status_desc in ('fast tracked','submitted') 
--and dead_mark_in ='N'
--and ath_active_status is  null
--and ath_hold_docket is  null 
--and dock_dt is  null
--and pendency_cal_start_dt > to_date ('2015-01-01','yyyy-MM-dd')
--then pendency_cal_start_dt end) OVER () as min_pullable_case_date,
filing_dt,
current_timestamp() as create_ts,
'etl' as create_user_id
FROM {trm_reporting_catalog}.silver.pea_trademark_applications pea
inner join temp_status_list status
on pea.pre_exam_status = status.status_code
left join (SELECT
serial_number,
--ph_action_code,
ph_action_date as NWOS_DT,
cm_prcd_num,
tm_worker_eid
From {trm_reporting_catalog}.silver.prosecution_history 
WHERE ph_action_code = 'NWOS' 
and year(ph_action_date)> year(current_date()) - 4)ph
on pea.ser_num = ph.serial_number
inner join (SELECT
ser_num,
pendency_cal_start_dt,
dock_dt,
disposal_type,
filing_dt
FROM {trm_reporting_catalog}.silver.milestone
WHERE pendency_cal_start_dt IS NOT NULL
--and ser_num = 79359945
)ms
on pea.ser_num = ms.ser_num
left join (SELECT ath_ser_num,
ath_hold_status,
ath_hold_docket,
ath_active_status,
ath_last_upd_dt
FROM {trm_reporting_catalog}.silver.on_hold
WHERE ath_active_status = 1
--and ath_ser_num is not null
)oh
on pea.ser_num = oh.ath_ser_num
left join {tmngpdb_src_catalog}.bronze.mv_myuspto_trm_mark mv
on pea.ser_num = mv.ser_num)pex
inner join
(Select mv.dead_mark_in, m.disposal_type, mv.ser_num 
from {tmngpdb_src_catalog}.bronze.mv_myuspto_trm_mark mv 
join {trm_reporting_catalog}.silver.milestone m on m.ser_num = mv.ser_num)mark
on pex.ser_num = mark.ser_num
inner join
(SELECT calendar_day,
assignee,
daily_teas_processed
FROM {trm_reporting_catalog}.gold.pea_worker_performance)pea
on pex.assignee = pea.assignee)pex_outer
inner join
(Select ph_action_date, ph_action_code, serial_number, m.ser_num,
concat_ws(',',collect_set(ph_action_code) OVER (PARTITION BY serial_number)) AS concatenated_ph_action_code
FROM {trm_reporting_catalog}.silver.prosecution_history
join {trm_reporting_catalog}.silver.milestone m on serial_number = m.ser_num
--where and  m.ser_num = 79359945
)ph
on ph.ph_action_date = pex_outer.calendar_day
and ph.ser_num = pex_outer.ser_num

""")

# COMMAND ----------

# DBTITLE 1,write to cs_survey_trm_efile
#df_pex.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{trm_reporting_catalog}.gold.pex_inventory_dash")
df_pex.write.mode("overwrite").format("delta").insertInto(f"{trm_reporting_catalog}.gold.pex_inventory_dash")

# COMMAND ----------

# DBTITLE 1,End Job Control and Exit Notebook
end_job_cntl(
    f"{trm_reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed with {df_pex.count()} records.")

# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("rundate","")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
rundate = dbutils.widgets.get("rundate").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl-aug182025/notebooks/config/dev/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trm_reporting = common_configs['schema']['trm_reporting_catalog']
trm_tmngpdb = common_configs['schema']['tmngpdb_src_catalog']
trm_tmworker = common_configs['schema']['tmworker_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
env = dbx_env.upper()
print(f"{trm_reporting=},{trm_tmworker=}, {trm_tmngpdb=},  {dq_catalog=}, {altrx_schema=}")

# COMMAND ----------

job_name = 'section_7_withdrawals'

#control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{trm_reporting}.silver',job_name,start_ts)

# COMMAND ----------

# DBTITLE 1,Insert from am_h
df = spark.sql(f"""
SELECT
  DISTINCT TM_paralegal,
  last_assigned_paralegal,
  last_assigned_paralegal_dt,
  serial_number,
  registration_number,
  sec_7_event_date,
  wdlrs_event_date_min wdlrs_event_date,
  pramo_event_date_min pramo_event_date
FROM
  (
    SELECT
      worker.worker_nm TM_paralegal,
      last_assigned_paralegal,
      last_assigned_paralegal_dt,
      substr(wdrls.trademark_gid, 13, 8) serial_number,
      trademark.registration_num registration_number,
      sec_7_event_date,
      wdlrs_event_date,
      pramo_event_date,
      case
        when wdrls.wdlrs_event_date - sec7.sec_7_event_date = MIN(
          wdrls.wdlrs_event_date - sec7.sec_7_event_date
        ) over (
          PARTITION BY substr(wdrls.trademark_gid, 13, 8),
          wdrls.wdlrs_event_date
        ) then wdlrs_event_date
        else null
      end wdlrs_event_date_min,
      case
        when pramo_event_date - sec7.sec_7_event_date = MIN(
          pramo_event_date - sec7.sec_7_event_date
        ) over (
          PARTITION BY substr(wdrls.trademark_gid, 13, 8),
          pramo_event_date
        ) then pramo_event_date
        else null
      end pramo_event_date_min
      FROM (
        select
          distinct cfk_object_gid as trademark_gid,
          effective_ts wdlrs_event_date,
          order_no
        from
          { trm_tmngpdb }.bronze.business_event
        where
          fk_business_event_reason_id = 763
      ) wdrls
      inner join (
        select
          distinct cfk_object_gid as trademark_gid,
          effective_ts sec_7_event_date,
          order_no
        from
          { trm_tmngpdb }.bronze.business_event
        where
          fk_business_event_reason_id = 269
      ) sec7 on wdrls.trademark_gid = sec7.trademark_gid
      inner join (
        select
          trademark_gid,
          registration_num
        FROM
          { trm_tmngpdb }.bronze.trademark
      ) trademark on trademark.trademark_gid = wdrls.trademark_gid
      left join (
        select
          distinct cfk_object_gid as trademark_gid,
          effective_ts pramo_event_date,
          order_no,
          last_mod_user_id
        from
          { trm_tmngpdb }.bronze.business_event
        where
          fk_business_event_reason_id IN (725, 593)
      ) pramo on pramo.trademark_gid = sec7.trademark_gid
      left join (
        select
          worker_nm,
          worker_no
        FROM
          { trm_tmworker }.bronze.worker
      ) worker on worker.worker_no = pramo.last_mod_user_id
left JOIN(select cfk_object_gid,worker_nm as last_assigned_paralegal , max_effective_ts as last_assigned_paralegal_dt from
(select  cfk_object_gid,worker_nm , effective_ts, max(effective_ts) over (partition by cfk_object_gid ) as max_effective_ts 
from  { trm_tmngpdb }.bronze.business_event be
inner join  { trm_tmworker }.bronze.worker
on worker.worker_no = be.last_mod_user_id)
where effective_ts = max_effective_ts)last_paralegal

  on last_paralegal.cfk_object_gid = sec7.trademark_gid
    where
      sec7.order_no < wdrls.order_no
      and (
        case
          when pramo.order_no is null then true
          else wdrls.order_no < pramo.order_no
        end
      ) 
  ) 
      """)

# COMMAND ----------

#df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").insertInto(f"{trm_reporting}.gold.sec_7_withdrawals")

# COMMAND ----------

# end job control
recs_count = df.count()
end_job_cntl(f"{trm_reporting}.silver", job_name, start_ts,'completed', recs_count,"job completed successfully")

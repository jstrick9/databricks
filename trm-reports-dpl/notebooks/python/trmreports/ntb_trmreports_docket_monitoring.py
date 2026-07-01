# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

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
trgt_catalog = common_configs["schema"]["trgt_catalog"]
src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmworker_catalog = common_configs["schema"]["tmworker_catalog"]
tqr_catalog = common_configs["schema"]["tqr_catalog"]
#lom_catalog = common_configs["schema"]["lom_catalog"]

spark.conf.set('conf.dbx_env', dbx_env)

dq_catalog = common_configs['schema']['data_quality_catalog']
# print(isinstance(primary_email, str))
#print(isinstance(cc_email, str))
if dbx_env != 'prod':
    lom_catalog = 'lom'+'_'+ dbx_env 
else:
    lom_catalog = 'lom'
print(trgt_catalog, src_catalog, lom_catalog)

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_docket_monitoring'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

docket_examiner_df = spark.sql(f"""
select
  DISTINCT case_status,
  ser_num,
  examiner_employee_no,
  nvl(review_type_cd, 'DOCKD') as review_type_cd,
  docket_type,
  docket_type_cd,
  dock_date,
  assigned_date,
  action_date,
  goal_date,
  nvl(due_date, due_date_cal) as due_date,
  date(source_event_dt) as lom_source_event_dt,
  case
    when current_date() > nvl(due_date, due_date_cal) then 1
    else 0
  end is_late_ind,
  date_diff( current_date(), assigned_date) days_in_docket,
  date_diff(due_date, assigned_date) due_date_estimate,
  date_diff( nvl(due_date, due_date_cal), current_date()) as due_in,
  law_office,
  year(assigned_date) as year_dock,
  --year assigned
  year(DATEADD(month, 3, assigned_date)) as dock_fy,
  --assigned fy
  (
    case
      when month(assigned_date) = 10 then 1
      when month(assigned_date) = 11 then 2
      when month(assigned_date) = 12 then 3
      when month(assigned_date) = 1 then 4
      when month(assigned_date) = 2 then 5
      when month(assigned_date) = 3 then 6
      when month(assigned_date) = 4 then 7
      when month(assigned_date) = 5 then 8
      when month(assigned_date) = 6 then 9
      when month(assigned_date) = 7 then 10
      when month(assigned_date) = 8 then 11
      when month(assigned_date) = 9 then 12
    end
  ) as fy_month_int,
  -------------------------------assigned month int
  date_format(assigned_date, "MMMM") as month_dock,
  --- assigned month
  CASE
    WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
  END AS fy_quarter,
  --------------------------assigned quarter
  current_timestamp() as create_ts,
  'etl' as create_user_id,
  examiner_name,
  date_diff(
    nvl(due_date, due_date_cal),
    nvl(assigned_date, current_date())
  ) Target_days,
  brs_user_id as lo_manager
from

    (
      select
        RIGHT(di.cfk_object_gid, 8) as ser_num,
        di.cfk_assignee_employee_no as examiner_employee_no,
        w.worker_nm as examiner_name,
        sd.description_tx as docket_type,
        fk_docket_item_event_type_cd docket_type_cd,
        di.last_mod_ts as dock_date,
        date(di.last_mod_ts) as assigned_date,
        null as action_date,
        date(event_goal_dt) as goal_date,
        null as due_date,
        case when  trim(lower(sd.description_tx)) = 'new' then date(di.last_mod_ts) +7
            when  trim(lower(sd.description_tx)) = 'amended' then date(di.last_mod_ts) +28
            when  trim(lower(sd.description_tx)) = 'corrections'then date(di.last_mod_ts)+5
            when  trim(lower(sd.description_tx)) ='sou' then date(di.last_mod_ts)+28
            when  trim(lower(sd.description_tx)) = 'suspension check' then date(di.last_mod_ts) +28
            when  trim(lower(sd.description_tx)) ='potential abandonment' then date(di.last_mod_ts)+7
            when  trim(lower(sd.description_tx)) ='ttab' then date(di.last_mod_ts)+128
            when  trim(lower(sd.description_tx)) ='ttab jurisdiction' then date(di.last_mod_ts)+128
        end as due_date_cal,
        --replace(di.cfk_organization_cd, 'LO', '') as law_office,
        w.organization_cd as law_office,
        'DOCKD' AS case_status,
        null as review_type_cd,
        null as source_event_dt,
        brs_user_id
      from
        { src_catalog }.bronze.docket_item di
        left join (
          select
            distinct worker_no,grade_ct,
            worker_nm, organization_cd
          from
            { tmworker_catalog }.bronze.worker
            inner join {tqr_catalog}.silver.employee_organization eo
            on worker_no = employee_no
        where grade_ct <15 or
         worker_no in (76465,68603,93979,92452,77875,92823,92838,91165,80812,82103,93663,89009,81856,74284,72156,68181,83182,76625,81860)
        ) w on di.cfk_assignee_employee_no = w.worker_no
        INNER JOIN { src_catalog }.bronze.stnd_docket sd ON sd.docket_id = di.fk_docket_id
        INNER join { src_catalog }.bronze.docket_item_event die on die.fk_docket_item_id = di.docket_item_id
        left join (  select distinct eo.organization_cd,  concat_ws(',',collect_set(brs_user_id) OVER (PARTITION BY eo.organization_cd)) as brs_user_id
  from  { tmworker_catalog }.bronze.worker 
  inner join { tmworker_catalog }.bronze.worker_role
  on worker_gid = FK_WORKER_GID
  inner join { tmworker_catalog }.bronze.tm_organization to
 on FK_TM_ORGANIZATION_GID = tm_organization_gid
 inner join {tqr_catalog}.silver.employee_organization eo
 on employee_no = worker_no
and eo.organization_cd = replace(to.organization_cd, 'LO', '')

  where to.organization_cd like 'LO%'
  and   worker_no in (73350,76468,90331,91233,89012,92563,70722,76854,90289,88568,90293,80802,93051,61638,94054,67604,92826,77868,93428,76072,93665,91168,92989,72506,68365,91162,94059,82086,90287,91170,90340,78352,93061,90297,93675,73365,94346,76145,77655,92575,81847,78478,93057,81131,93655,77768,83174,81140,82436,90291,72008,82435,82428,93599,88572,96821,76151,92836,77656,85330, 92454,82104)
  )brs
  on w.organization_cd = brs.organization_cd
      WHERE
        sd.cfk_user_role_cd = 'TM_Examining_Attorney'
        and trim(lower(sd.description_tx)) in (
          'new',
          'amended',
          'corrections',
          'sou',
          'suspension check',
          'potential abandonment',
          'ttab',
          'ttab jurisdiction'
        ) --and di.cfk_assignee_employee_no = 20040
        --and cfk_object_gid like '%79404189'
    )
 
  """)
#display(docket_examiner_df)

# COMMAND ----------

target_table_name = f"{trgt_catalog}.gold.docket_monitoring"
docket_examiner_df.write.mode("overwrite").format("delta").insertInto(target_table_name)

# COMMAND ----------

recs_count = docket_examiner_df.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

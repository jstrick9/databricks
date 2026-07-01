# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("full_refresh","","full_refresh")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
full_refresh = dbutils.widgets.get("full_refresh").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl-tm-expired_prod_fix/notebooks/config/dev/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmintltm_src_catalog = common_configs['schema']['tmintltm_src_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
tmbuscalendar_catalog = common_configs['schema']['trm_tmbuscalendar_catalog']
env = dbx_env.upper()

print(f"{trgt_catalog=},{src_catalog=},{tmbuscalendar_catalog=},{full_refresh=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trm_process_prod_staffing_report'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import col
# MAGIC df_stage = spark.read.csv(f"s3://databricks-prod-tmdc/trademark/process_production_staffing/process_prod_staffing_report_stage_load.csv",header=True)
# MAGIC
# MAGIC if full_refresh == 'Y':
# MAGIC     target_table = f"{trgt_catalog}.gold.process_production_staffing_report"
# MAGIC     df_stage.createOrReplaceTempView("stage_data")
# MAGIC     
# MAGIC     merge_query = f"""
# MAGIC     MERGE INTO {target_table} AS target
# MAGIC     USING stage_data AS source
# MAGIC     ON target.year = source.year AND target.fy_month_int = source.fy_month_int
# MAGIC     WHEN MATCHED THEN
# MAGIC       UPDATE SET *
# MAGIC     WHEN NOT MATCHED THEN
# MAGIC       INSERT *
# MAGIC     """
# MAGIC     
# MAGIC     spark.sql(merge_query)

# COMMAND ----------

from pyspark.sql.functions import month, year, current_date

if full_refresh == 'N':
    current_date_value = spark.sql("SELECT current_date() as current_date").collect()[0]['current_date'] 
    current_month_int = current_date_value.month-1#replace 0 with 12
    current_fy = current_date_value.year + 1 if current_month_int >= 10 else current_date_value.year
    current_month = spark.sql("SELECT date_format(add_months(current_date(), -1), 'MMM') as current_month").collect()[0]['current_month']# replace 0 with 12
    current_date_value = spark.sql("SELECT dayofmonth(current_date()) as current_date").collect()[0]['current_date']
    current_date_value=1
    current_month_int = '1'
else:
    current_month_int = ''
    current_month = ''
    current_fy = ''
    current_date_value = ''

print(f"Current_Date = {current_date_value}, Current Month: {current_month_int} {current_month}, Current FY: {current_fy}")

# COMMAND ----------

# MAGIC %md
# MAGIC #  **1.Pendency Dashboard:1**
# MAGIC Refreshed only on 1st of the month
# MAGIC ###Disposal Measures / Pendency Time **:
# MAGIC 10. Pendency to First Action
# MAGIC Pendency to First Action Mailed:
# MAGIC 11. "Pendency to Registration/Abandonment/NOA
# MAGIC    (Excluding Suspended and Inter Partes Cases)"
# MAGIC
# MAGIC 12. "Pendency to Registration/Abandonment/NOA
# MAGIC    (Including Suspended and Inter Partes Cases)"
# MAGIC
# MAGIC ###Trademark Registration Rate
# MAGIC 135. Total Pendency- Registration 
# MAGIC 136. Total Pendency- NOA 

# COMMAND ----------


ppsr_pendency_query = (f"""select 
  fa_month.fa_pendency_fy as year,
  fa_month.fa_pendency_fy_quarter as fy_quarter,
  fa_month.fa_pendency_fy_month as fy_month,
  (case 
      when fa_month.fa_pendency_fy_month = 'Oct' then 1
      when fa_month.fa_pendency_fy_month = 'Nov' then 2
      when fa_month.fa_pendency_fy_month = 'Dec' then 3
      when fa_month.fa_pendency_fy_month = 'Jan' then 4
      when fa_month.fa_pendency_fy_month = 'Feb' then 5
      when fa_month.fa_pendency_fy_month = 'Mar' then 6
      when fa_month.fa_pendency_fy_month = 'Apr' then 7
      when fa_month.fa_pendency_fy_month = 'May' then 8
      when fa_month.fa_pendency_fy_month = 'Jun' then 9
      when fa_month.fa_pendency_fy_month = 'Jul' then 10
      when fa_month.fa_pendency_fy_month = 'Aug' then 11
      when fa_month.fa_pendency_fy_month = 'Sep' then 12
  end) as fy_month_int,
  cast(format_number(Pendency_to_First_Action_month, 2) as double) as Pendency_to_First_Action_month,
  cast(format_number(Pendency_to_First_Action_fy, 2) as double) as Pendency_to_First_Action_fy,
  First_Action_target_fy,
  cast(format_number(Pendency_to_Registration_Abandonment_NOA_Exc, 2) as double) as Pendency_to_Registration_Abandonment_NOA_Exc,
  cast(format_number(Pendency_to_Reg_fy_exc, 2) as double) as Pendency_to_Reg_fy_exc,
  Pendency_to_Reg_Target_fy_EXC,
  cast(format_number(Pendency_to_Registration_Abandonment_NOA_INC, 2) as double) as Pendency_to_Registration_Abandonment_NOA_INC,
  cast(format_number(Pendency_to_Reg_fy_inc, 2) as double) as Pendency_to_Reg_fy_inc,
  Pendency_to_Reg_Target_fy_inc,
  cast(format_number(total_pendency_reg_135, 2) as double) as total_pendency_reg_135,
  cast(format_number(total_pendency_reg_fy_135a, 2) as double) as total_pendency_reg_fy_135a,
  cast(format_number(total_pendency_noa_136, 2) as double) as total_pendency_noa_136,
  cast(format_number(total_pendency_noa_fy_136a, 2) as double) as total_pendency_noa_fy_136a
from
(
  select * from
  (
    SELECT 
      fa_pendency_fy,
      fa_pendency_fy_month,
      fy_month_int,
      fa_pendency_fy_quarter,
      sum(Active_Classes_FirstAction_ph) over (partition by fa_pendency_fy order by fy_month_int )/sum(Active_Classes_FirstAction) over (partition by fa_pendency_fy order by fy_month_int ) as Pendency_to_First_Action_month
    from (
      select 
        fa_pendency_fy_month,
        fa_pendency_fy,
        fa_pendency_fy_quarter,
        (case 
            when fa_pendency_fy_month = 'Oct' then 1
            when fa_pendency_fy_month = 'Nov' then 2
            when fa_pendency_fy_month = 'Dec' then 3
            when fa_pendency_fy_month = 'Jan' then 4
            when fa_pendency_fy_month = 'Feb' then 5
            when fa_pendency_fy_month = 'Mar' then 6
            when fa_pendency_fy_month = 'Apr' then 7
            when fa_pendency_fy_month = 'May' then 8
            when fa_pendency_fy_month = 'Jun' then 9
            when fa_pendency_fy_month = 'Jul' then 10
            when fa_pendency_fy_month = 'Aug' then 11
            when fa_pendency_fy_month = 'Sep' then 12
        end) as fy_month_int,
        sum(Active_Classes_FirstAction*first_action_pendency_ph) as Active_Classes_FirstAction_ph,
        sum(Active_Classes_FirstAction) as Active_Classes_FirstAction
      FROM {trgt_catalog}.gold.pendency_dashboard
      where on_hold=False
      group by fa_pendency_fy,fa_pendency_fy_month,fa_pendency_fy_quarter
    )
  )
  where (fa_pendency_fy = 2026)  
) fa_month

inner join
(
  SELECT 
    total_pendency_fy_month,
    total_pendency_fy,
    total_pendency_fy_quarter,
    sum(Active_Classes_Disposal_exc) over (partition by total_pendency_fy order by fy_month_int )/sum(Active_Classes_Disposal) over (partition by total_pendency_fy order by fy_month_int ) as Pendency_to_Registration_Abandonment_NOA_Exc
  from (
    select 
      total_pendency_fy_month,
      total_pendency_fy,
      total_pendency_fy_quarter,
      (case 
          when total_pendency_fy_month = 'Oct' then 1
          when total_pendency_fy_month = 'Nov' then 2
          when total_pendency_fy_month = 'Dec' then 3
          when total_pendency_fy_month = 'Jan' then 4
          when total_pendency_fy_month = 'Feb' then 5
          when total_pendency_fy_month = 'Mar' then 6
          when total_pendency_fy_month = 'Apr' then 7
          when total_pendency_fy_month = 'May' then 8
          when total_pendency_fy_month = 'Jun' then 9
          when total_pendency_fy_month = 'Jul' then 10
          when total_pendency_fy_month = 'Aug' then 11
          when total_pendency_fy_month = 'Sep' then 12
      end) as fy_month_int,
      sum(Active_Classes_Disposal*DISPOSAL_PENDENCY) as Active_Classes_Disposal_exc,
      sum(Active_Classes_Disposal) as Active_Classes_Disposal
    FROM {trgt_catalog}.gold.pendency_dashboard
    where on_hold=False
      and pendency_category  = 'No Suspension or Opposition'
    group by total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,fy_month_int
  )
) total_Month_Reg_Exc
on fa_month.fa_pendency_fy = total_Month_Reg_Exc.total_pendency_fy
and fa_month.fa_pendency_fy_month = total_Month_Reg_Exc.total_pendency_fy_month 

inner join
(
  SELECT 
    total_pendency_fy_month,
    total_pendency_fy,
    total_pendency_fy_quarter,
    sum(Active_Classes_Disposal_inc) over (partition by total_pendency_fy order by fy_month_int )/sum(Active_Classes_Disposal) over (partition by total_pendency_fy order by fy_month_int ) as Pendency_to_Registration_Abandonment_NOA_INC
  from (
    select 
      total_pendency_fy_month,
      total_pendency_fy,
      total_pendency_fy_quarter,
      (case 
          when total_pendency_fy_month = 'Oct' then 1
          when total_pendency_fy_month = 'Nov' then 2
          when total_pendency_fy_month = 'Dec' then 3
          when total_pendency_fy_month = 'Jan' then 4
          when total_pendency_fy_month = 'Feb' then 5
          when total_pendency_fy_month = 'Mar' then 6
          when total_pendency_fy_month = 'Apr' then 7
          when total_pendency_fy_month = 'May' then 8
          when total_pendency_fy_month = 'Jun' then 9
          when total_pendency_fy_month = 'Jul' then 10
          when total_pendency_fy_month = 'Aug' then 11
          when total_pendency_fy_month = 'Sep' then 12
      end) as fy_month_int,
      sum(Active_Classes_Disposal*DISPOSAL_PENDENCY) as Active_Classes_Disposal_inc,
      sum(Active_Classes_Disposal) as Active_Classes_Disposal
    FROM {trgt_catalog}.gold.pendency_dashboard
    where on_hold=False
    group by total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,fy_month_int
  )
) total_Month_Reg_Inc
on fa_month.fa_pendency_fy = total_Month_Reg_Inc.total_pendency_fy
and fa_month.fa_pendency_fy_month = total_Month_Reg_Inc.total_pendency_fy_month 

inner join
(
  SELECT
    total_pendency_fy_month,
    total_pendency_fy,
    total_pendency_fy_quarter,
    fy_month_int,
    (
      SUM(MonthlyDisCount) OVER (
        PARTITION BY total_pendency_fy
        ORDER BY fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) /
      SUM(MonthlyTotalClasses) OVER (
        PARTITION BY total_pendency_fy
        ORDER BY fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) * 100
    ) AS total_pendency_reg_135,
    (
      SUM(MonthlyNOACount) OVER (
        PARTITION BY total_pendency_fy
        ORDER BY fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) /
      SUM(MonthlyTotalClasses) OVER (
        PARTITION BY total_pendency_fy
        ORDER BY fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) * 100
    ) AS total_pendency_noa_136
  FROM (
    SELECT
      total_pendency_fy_month,
      CASE 
        WHEN total_pendency_fy_month = 'Oct' THEN 1
        WHEN total_pendency_fy_month = 'Nov' THEN 2
        WHEN total_pendency_fy_month = 'Dec' THEN 3
        WHEN total_pendency_fy_month = 'Jan' THEN 4
        WHEN total_pendency_fy_month = 'Feb' THEN 5
        WHEN total_pendency_fy_month = 'Mar' THEN 6
        WHEN total_pendency_fy_month = 'Apr' THEN 7
        WHEN total_pendency_fy_month = 'May' THEN 8
        WHEN total_pendency_fy_month = 'Jun' THEN 9
        WHEN total_pendency_fy_month = 'Jul' THEN 10
        WHEN total_pendency_fy_month = 'Aug' THEN 11
        WHEN total_pendency_fy_month = 'Sep' THEN 12
      END AS fy_month_int,
      total_pendency_fy,
      total_pendency_fy_quarter,
      SUM(Active_Classes_Disposal) AS MonthlyTotalClasses,
      SUM(
        CASE 
          WHEN disposal_type = 'REGISTRATION' THEN Active_Classes_Disposal 
          ELSE 0 
        END
      ) AS MonthlyDisCount,
      SUM(
        CASE 
          WHEN disposal_type = 'NOA' THEN Active_Classes_Disposal 
          ELSE 0 
        END
      ) AS MonthlyNOACount
    FROM {trgt_catalog}.gold.pendency_dashboard
    WHERE on_hold = False 
      AND total_pendency_fy = 2026
    GROUP BY 
      total_pendency_fy_month,
      total_pendency_fy,
      total_pendency_fy_quarter
  ) t
) total_Month_Pen_Inc
on fa_month.fa_pendency_fy = total_Month_Pen_Inc.total_pendency_fy
and fa_month.fa_pendency_fy_month = total_Month_Pen_Inc.total_pendency_fy_month 
and fa_month.fy_month_int = total_Month_Pen_Inc.fy_month_int

inner join
(
  SELECT 
    fa_pendency_fy,
    First_Action_target_fy,
    cast(format_number(sum(Active_Classes_FirstAction*first_action_pendency_ph)/sum(Active_Classes_FirstAction), 2) as double) as Pendency_to_First_Action_fy
  from (
    select 
      fa_pendency_fy_month,
      fa_pendency_fy,
      Active_Classes_FirstAction,
      first_action_pendency_ph,
      CASE WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2021 THEN 4.5
           WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2022 THEN 7.5
           WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2023 THEN 8.5
           WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2024 THEN 8.4
           WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) > 2024 THEN 5.0
           ELSE 3.5
      END as First_Action_target_fy
    FROM {trgt_catalog}.gold.pendency_dashboard
    where on_hold=False
  )
  group by fa_pendency_fy,First_Action_target_fy
) FA_Year
on fa_month.fa_pendency_fy = FA_Year.fa_pendency_fy

inner join
(
  SELECT 
    total_pendency_fy,
    MAX(Total_Pendency_Target_fy_EXC) as Pendency_to_Reg_Target_fy_EXC,
    MAX(Total_Pendency_Target_fy_INC) as Pendency_to_Reg_Target_fy_inc,
    sum(Active_Classes_Disposal*DISPOSAL_PENDENCY)/sum(Active_Classes_Disposal)  as Pendency_to_Reg_fy_inc,
    sum(Active_Classes_Disposal_exc*DISPOSAL_PENDENCY_exc)/sum(Active_Classes_Disposal_exc)  as Pendency_to_Reg_fy_exc
  from (
    select 
      total_pendency_fy_month,
      total_pendency_fy,
      Active_Classes_Disposal,
      DISPOSAL_PENDENCY,
      case when Pendency_Category ='No Suspension or Opposition' then Active_Classes_Disposal else 0 end as  Active_Classes_Disposal_exc,
      case when Pendency_Category ='No Suspension or Opposition' then DISPOSAL_PENDENCY else 0 end as DISPOSAL_PENDENCY_exc,
      CASE when Pendency_Category = "No Suspension or Opposition"
        AND (YEAR(DATEADD(month, 3, disposal_dt)) < 2022 ) THEN 12
        when (Pendency_Category ="No Suspension or Opposition")
        AND (YEAR(DATEADD(month, 3, disposal_dt)) = 2022) THEN 13.5
        when (Pendency_Category ="No Suspension or Opposition")
        AND (YEAR(DATEADD(month, 3, disposal_dt)) = 2023) THEN 14.5
        when (Pendency_Category ="No Suspension or Opposition")
        AND (YEAR(DATEADD(month, 3,disposal_dt)) = 2024) THEN 14.4
        when (Pendency_Category ="No Suspension or Opposition")
        AND (YEAR(DATEADD(month, 3,disposal_dt)) > 2024) THEN 11.0
      END as Total_Pendency_Target_fy_EXC,
      CASE when (YEAR(DATEADD(month, 3, disposal_dt)) > 2023) THEN 14.0 else 15.5 END as Total_Pendency_Target_fy_INC
    FROM {trgt_catalog}.gold.pendency_dashboard
    where on_hold=False
  )
  group by total_pendency_fy
) total_Year
on fa_month.fa_pendency_fy = total_Year.total_pendency_fy

inner join
(
  select 
    total_pendency_fy,
    ((sum(total_Active_Classes_Reg))/(sum(Active_Classes_Disposal))*100) as total_pendency_reg_fy_135a,
    ((sum(total_Active_Classes_NOA))/(sum(Active_Classes_Disposal))*100) as total_pendency_noa_fy_136a
  from (
    select 
      total_pendency_fy,
      case when disposal_type  = 'REGISTRATION' then Active_Classes_Disposal else 0 end as  total_Active_Classes_Reg,
      case when disposal_type  = 'NOA' then Active_Classes_Disposal else 0 end as  total_Active_Classes_NOA,
      Active_Classes_Disposal
    FROM {trgt_catalog}.gold.pendency_dashboard
    where on_hold=False
  )
  group by total_pendency_fy
) total_Pen_Inc
on fa_month.fa_pendency_fy = total_Pen_Inc.total_pendency_fy

""")

if full_refresh == 'N' and current_date_value == 1:#1
    print("Executing query for Data refresh")
    df_ppsr_pendency = spark.sql(ppsr_pendency_query)
else:
    print("Data refresh not required")
    #df_ppsr_pendency = spark.createDataFrame([], schema=spark.sql(ppsr_pendency_query).schema)
    df_ppsr_pendency = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int, Pendency_to_First_Action_month, Pendency_to_First_Action_fy, First_Action_target_fy, Pendency_to_Registration_Abandonment_NOA_Exc, Pendency_to_Reg_fy_exc, Pendency_to_Reg_Target_fy_EXC, Pendency_to_Registration_Abandonment_NOA_INC, Pendency_to_Reg_fy_inc, Pendency_to_Reg_Target_fy_inc, total_pendency_reg, total_pendency_reg_fy, total_pendency_noa, total_pendency_noa_fy 
                                 from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""")

# COMMAND ----------

#display(df_ppsr_pendency)

# COMMAND ----------

# MAGIC %md
# MAGIC # **2. TMIIFY15 Dashboard:1**
# MAGIC (report mailed by Jim, 1st of month)

# COMMAND ----------

df_tmiify15_dshb_query = (f"""
WITH base_agg AS (
  SELECT
    fy,
    fy_month_fil AS fy_month,
    fy_month_fil,
    fy_quarter,
    fy_month_int,
    SUM(Section_9_Applications_Filed) AS Section_9_Applications_Filed,
    SUM(Registrations_Renewed) AS Registrations_Renewed,
    SUM(Affidavits_under_Section_8_15_71_Combinations_Filed) AS Affidavits_under_Section_8_15_71_Combinations_Filed,
    SUM(Affidavits_under_Section_8_15_71_Combinations_Disposed) AS Affidavits_under_Section_8_15_71_Combinations_Disposed,
    SUM(Section_8_Applications_Filed_10yr) AS Section_8_Applications_Filed_10yr
  FROM (
    SELECT
      CASE
        WHEN month(CAST(be.effective_ts AS DATE)) >= 10 THEN year(CAST(be.effective_ts AS DATE)) + 1
        ELSE year(CAST(be.effective_ts AS DATE))
      END AS fy,
      date_format(CAST(be.effective_ts AS DATE), 'MMM') AS fy_month_fil,
      date_format(CAST(be.effective_ts AS DATE), 'MMM') AS fy_month,
      CASE
        WHEN month(CAST(be.effective_ts AS DATE)) IN (10, 11, 12) THEN 'Q1'
        WHEN month(CAST(be.effective_ts AS DATE)) IN (1, 2, 3) THEN 'Q2'
        WHEN month(CAST(be.effective_ts AS DATE)) IN (4, 5, 6) THEN 'Q3'
        WHEN month(CAST(be.effective_ts AS DATE)) IN (7, 8, 9) THEN 'Q4'
      END AS fy_quarter,
      CASE
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'October' THEN 1
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'November' THEN 2
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'December' THEN 3
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'January' THEN 4
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'February' THEN 5
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'March' THEN 6
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'April' THEN 7
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'May' THEN 8
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'June' THEN 9
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'July' THEN 10
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'August' THEN 11
        WHEN date_format(CAST(be.effective_ts AS DATE), "MMMM") = 'September' THEN 12
      END AS fy_month_int,
      (CASE WHEN sbe.legacy_cm_ent_cd IN ('9.AF', '89F', 'E89R') THEN 1 ELSE 0 END) AS Section_9_Applications_Filed,
      (CASE WHEN substring(sbe.legacy_cm_ent_cd, 1, 3) IN ('REN', 'RNL') THEN 1 ELSE 0 END) AS Registrations_Renewed,
      (CASE WHEN sbe.legacy_cm_ent_cd IN ('8.AF','815F','15AF','ES8R','E815','ES71','71AF','ES75','71AF','E15R') THEN 1 ELSE 0 END) AS Affidavits_under_Section_8_15_71_Combinations_Filed,
      (CASE WHEN sbe.legacy_cm_ent_cd IN ('8.OK','8.PR','C15A','C15P','71AG','71.P','C75A','C75P','15AK') THEN 1 ELSE 0 END) AS Affidavits_under_Section_8_15_71_Combinations_Disposed,
      (CASE WHEN sbe.legacy_cm_ent_cd IN ('8AFT', '89AF', 'E89R', '9.AF') THEN 1 ELSE 0 END) AS Section_8_Applications_Filed_10yr
    FROM {src_catalog}.bronze.business_event be
    INNER JOIN {src_catalog}.bronze.stnd_business_event_reason sbe
      ON be.fk_business_event_reason_id = sbe.business_event_reason_id
    WHERE
      (
        sbe.legacy_cm_ent_cd IN ('9.AF','89F','E89R','8.AF','815F','15AF','ES8R','E815','ES71','71AF','ES75','71AF','E15R','8.OK','8.PR','C15A','C15P','71AG','71.P','C75A','C75P','15AK','89AF','8AFT','89AG','8PRT','8OKT','9G8P','12AF','P12C','IUAF','IUAA','R.PRA','R.SRA')
        OR substring(sbe.legacy_cm_ent_cd, 1, 3) IN ('REN', 'RNL')
        OR sbe.business_event_reason_cd IN ('R.PRA', 'R.SRA')
      )
      AND CAST(be.effective_ts AS DATE) BETWEEN
        CASE
          WHEN month(current_date) < 11 THEN CAST((year(current_date) -1) AS STRING) || '-10-01'
          ELSE CAST(year(current_date) AS STRING) || '-10-01'
        END
        AND last_day(current_date())
  )
  GROUP BY
    fy,
    fy_month,
    fy_month_fil,
    fy_quarter,
    fy_month_int
)
SELECT
  fy,
  fy_month,
  fy_month_fil,
  fy_quarter,
  fy_month_int,
  Section_9_Applications_Filed,
  Registrations_Renewed,
  Affidavits_under_Section_8_15_71_Combinations_Filed,
  Affidavits_under_Section_8_15_71_Combinations_Disposed,
  Section_8_Applications_Filed_10yr,
  SUM(Section_9_Applications_Filed) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Section_9_Applications_Filed_fy,
  CASE WHEN fy <= 2024 THEN 96500 ELSE 114700 END AS Section_9_Applications_Filed_fy_target,
  SUM(Registrations_Renewed) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Registrations_Renewed_fy,
  CASE WHEN fy <= 2024 THEN 104000 ELSE 104600 END AS Registrations_Renewed_target,
  SUM(Affidavits_under_Section_8_15_71_Combinations_Filed) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Affidavits_under_Section_8_15_71_Combinations_Filed_fy,
  CASE WHEN fy <= 2024 THEN 94400 ELSE 122000 END AS Affidavits_under_Section_8_15_71_Combinations_Filed_fy_target,
  SUM(Affidavits_under_Section_8_15_71_Combinations_Disposed) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Affidavits_under_Section_8_15_71_Combinations_Disposed_fy,
  CASE WHEN fy <= 2024 THEN 98500 ELSE 144000 END AS Affidavits_under_Section_8_15_71_Combinations_Disposed_fy_target,
  SUM(Section_8_Applications_Filed_10yr) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Section_8_Applications_Filed_10yr_fy,
  CASE WHEN fy <= 2024 THEN 128400 ELSE 152500 END AS Section_8_Applications_Filed_10yr_fy_target
FROM base_agg
ORDER BY fy, fy_month_int
""")
#df_tmiify15_dshb.display()

if full_refresh == 'N' and current_date_value == 1:#1:
    df_tmiify15_dshb = spark.sql(df_tmiify15_dshb_query)
    df_tmiify15_dshb = df_tmiify15_dshb.drop('fy_month_fil')
else:
    df_tmiify15_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,Section_9_Applications_Filed, Registrations_Renewed, Affidavits_under_Section_8_15_71_Combinations_Filed, Affidavits_under_Section_8_15_71_Combinations_Disposed, Section_8_Applications_Filed_10yr, Section_9_Applications_Filed_fy, Section_9_Applications_Filed_fy_target, Registrations_Renewed_fy, Registrations_Renewed_target, Affidavits_under_Section_8_15_71_Combinations_Filed_fy, Affidavits_under_Section_8_15_71_Combinations_Filed_fy_target, Affidavits_under_Section_8_15_71_Combinations_Disposed_fy, Affidavits_under_Section_8_15_71_Combinations_Disposed_fy_target, Section_8_Applications_Filed_10yr_fy, Section_8_Applications_Filed_10yr_fy_target
                                  from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#display(df_tmiify15_dshb)

# COMMAND ----------

df_joined = df_ppsr_pendency.join(
    df_tmiify15_dshb,
    (df_ppsr_pendency.year == df_tmiify15_dshb.fy) &
    (df_ppsr_pendency.fy_quarter == df_tmiify15_dshb.fy_quarter) &
    (df_ppsr_pendency.fy_month_int == df_tmiify15_dshb.fy_month_int),
    "left"
).drop(df_tmiify15_dshb.fy_quarter,df_tmiify15_dshb.fy_month,df_tmiify15_dshb.fy_month,df_tmiify15_dshb.fy,df_tmiify15_dshb.fy_month_int )
#display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC # **3. Unexamined_dashboard:1**
# MAGIC Capture counts until 1st of current month instead of last day of previous month(enhancement)
# MAGIC Median age of inventory is off by .1
# MAGIC (Direct Dashboard, counts captured as of last day of the month, after the data refresh for the last day of the month)
# MAGIC Unexamined New Applications (cases) - prior to first action
# MAGIC Unexamined New Applications (classes) - prior to first action
# MAGIC Median age of inventory (in Months)

# COMMAND ----------

df_unexamined_dshb_query = f"""
SELECT DISTINCT
  max_dt.fy,
  max_dt.fy_month,
  max_dt.fy_month_int,
  CASE
    WHEN max_dt.fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN max_dt.fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN max_dt.fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN max_dt.fy_month_int IN (10, 11, 12) THEN 'Q4'
  END AS fy_quarter,
  raw_monthly.unexamined_cases AS Unexamined_New_Applicationn_cases_prior_to_first_action,
  raw_monthly.unexamined_classes AS Unexamined_New_Applicationn_classes_prior_to_first_action,
  SUM(raw_monthly.unexamined_cases) OVER (PARTITION BY max_dt.fy ORDER BY max_dt.fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Unexamined_New_Applicationn_cases_prior_to_first_action_fy,
  CASE WHEN max_dt.fy <= 2024 THEN 485000 ELSE 402780 END AS Unexamined_New_Applicationn_cases_prior_to_first_action_fy_target,
  SUM(raw_monthly.unexamined_classes) OVER (PARTITION BY max_dt.fy ORDER BY max_dt.fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Unexamined_New_Applicationn_classes_prior_to_first_action_fy,
  CASE WHEN max_dt.fy <= 2024 THEN 485000 ELSE 302000 END AS Unexamined_New_Applicationn_classes_prior_to_first_action_fy_target,
  median.Median_Unexamined_Inventory_Age_FY AS median_age_of_inventory,
  median.Median_Unexamined_Inventory_Age_FY AS Median_age_of_inventory_fy,
  CASE WHEN max_dt.fy <= 2024 THEN 5.0 ELSE 2.7 END AS Median_age_of_inventory_fy_target,
  mm.Median_age_of_inventory_monthly
FROM (
  SELECT DISTINCT
    date_format(add_months(unexamined_date, -1), "MMMM") AS fy_month,
    date_format(add_months(unexamined_date, -1), "MMM") AS fy_month_fil,
    CASE WHEN month(add_months(unexamined_date, -1)) >= 10 THEN year(add_months(unexamined_date, -1)) + 1 ELSE year(add_months(unexamined_date, -1)) END AS fy,
    CASE
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'October' THEN 1
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'November' THEN 2
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'December' THEN 3
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'January' THEN 4
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'February' THEN 5
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'March' THEN 6
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'April' THEN 7
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'May' THEN 8
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'June' THEN 9
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'July' THEN 10
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'August' THEN 11
      WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'September' THEN 12
    END AS fy_month_int,
    MIN(unexamined_date) OVER (
      PARTITION BY 
        CASE WHEN month(add_months(unexamined_date, -1)) >= 10 THEN year(add_months(unexamined_date, -1)) + 1 ELSE year(add_months(unexamined_date, -1)) END,
        month(add_months(unexamined_date, -1))
    ) AS min_unexamined_date_monthly
  FROM {trgt_catalog}.gold.inventory_unexamined_hstry
  WHERE day(unexamined_date) = 1
) max_dt
INNER JOIN {trgt_catalog}.gold.inventory_unexamined_hstry raw_monthly
  ON max_dt.min_unexamined_date_monthly = raw_monthly.unexamined_date
LEFT JOIN (
  WITH base_data AS (
    SELECT Pendency_Cal_Start_DT, fy
    FROM {trgt_catalog}.gold.inventory_dashboard_running
    WHERE Count_Type = 'Actual'
  ),
  ranked_data AS (
    SELECT
      Pendency_Cal_Start_DT,
      fy,
      ROW_NUMBER() OVER (ORDER BY fy, Pendency_Cal_Start_DT) AS row_num,
      COUNT(*) OVER (ORDER BY fy) AS total_rows
    FROM base_data
  ),
  median_data AS (
    SELECT Pendency_Cal_Start_DT, fy
    FROM ranked_data
    WHERE row_num = (total_rows + 1) / 2
       OR (total_rows % 2 = 0 AND row_num = (total_rows / 2) + 1)
  ),
  median_date AS (
    SELECT
      DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(Pendency_Cal_Start_DT AS STRING), 'yyyy-MM-dd'))) AS Median_Unexamined_Inventory,
      fy
    FROM median_data
  )
  SELECT
    fy,
    ROUND((DATEDIFF(CURRENT_DATE, Median_Unexamined_Inventory) / 30.42), 2) AS Median_Unexamined_Inventory_Age_FY
  FROM median_date
) median
  ON max_dt.fy = median.fy
LEFT JOIN (
  SELECT
    max_dt.fy,
    max_dt.fy_month_int,
    max_dt.min_unexamined_date_monthly,
    ROUND(
      PERCENTILE_APPROX(
        DATEDIFF(max_dt.min_unexamined_date_monthly, idr.Pendency_Cal_Start_DT) / 30.42,
        0.5
      ),
      2
    ) AS Median_age_of_inventory_monthly
  FROM (
    SELECT DISTINCT
      CASE WHEN month(add_months(unexamined_date, -1)) >= 10 THEN year(add_months(unexamined_date, -1)) + 1 ELSE year(add_months(unexamined_date, -1)) END AS fy,
      CASE
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'October' THEN 1
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'November' THEN 2
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'December' THEN 3
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'January' THEN 4
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'February' THEN 5
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'March' THEN 6
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'April' THEN 7
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'May' THEN 8
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'June' THEN 9
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'July' THEN 10
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'August' THEN 11
        WHEN date_format(add_months(unexamined_date, -1), "MMMM") = 'September' THEN 12
      END AS fy_month_int,
      MIN(unexamined_date) OVER (
        PARTITION BY 
          CASE WHEN month(add_months(unexamined_date, -1)) >= 10 THEN year(add_months(unexamined_date, -1)) + 1 ELSE year(add_months(unexamined_date, -1)) END,
          month(add_months(unexamined_date, -1))
      ) AS min_unexamined_date_monthly
    FROM {trgt_catalog}.gold.inventory_unexamined_hstry
    WHERE CASE WHEN month(add_months(unexamined_date, -1)) >= 10 THEN year(add_months(unexamined_date, -1)) + 1 ELSE year(add_months(unexamined_date, -1)) END = 2026
      AND day(unexamined_date) = 1
  ) max_dt
  LEFT JOIN {trgt_catalog}.gold.inventory_dashboard_running idr
    ON idr.fy = max_dt.fy
    AND idr.Count_Type = 'Actual'
    AND idr.Pendency_Cal_Start_DT <= max_dt.min_unexamined_date_monthly
  GROUP BY max_dt.fy, max_dt.fy_month_int, max_dt.min_unexamined_date_monthly
) mm
  ON max_dt.fy = mm.fy
  AND max_dt.fy_month_int = mm.fy_month_int
WHERE max_dt.fy = 2026
"""
#display(df_unexamined_dshb)

if full_refresh == 'N' and current_date_value == 1:#1:
    df_unexamined_dshb = spark.sql(df_unexamined_dshb_query)
else:
    df_unexamined_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int, Unexamined_New_Applicationn_cases_prior_to_first_action, Unexamined_New_Applicationn_classes_prior_to_first_action, Unexamined_New_Applicationn_cases_prior_to_first_action_fy, Unexamined_New_Applicationn_cases_prior_to_first_action_fy_target, Unexamined_New_Applicationn_classes_prior_to_first_action_fy, Unexamined_New_Applicationn_classes_prior_to_first_action_fy_target, Median_age_of_inventory,Median_age_of_inventory_fy, Median_age_of_inventory_fy_target
                                    from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#display(df_unexamined_dshb)

# COMMAND ----------

df_joined = df_unexamined_dshb.join(
    df_joined,
    (df_joined.year == df_unexamined_dshb.fy) &
    (df_joined.fy_month_int == df_unexamined_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_unexamined_dshb.fy_quarter),
    "right"
).drop(df_unexamined_dshb.fy,df_unexamined_dshb.fy_month,df_unexamined_dshb.fy_quarter,df_unexamined_dshb.fy_month_int)
#display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC # **4.OS34:1**
# MAGIC Dashboard refreshed only on 1st of month
# MAGIC  
# MAGIC
# MAGIC Office Disposals:
# MAGIC Abandoned - classes
# MAGIC Abandoned files - cases

# COMMAND ----------

df_os34_dshb_query = (f"""
WITH monthly_first_day AS (
  SELECT
    CASE WHEN month(load_date) >= 10 THEN year(load_date) + 1 ELSE year(load_date) END AS fy,
    date_format(add_months(load_date, -1), "MMMM") AS fy_month,
    CASE
      WHEN month(add_months(load_date, -1)) IN (10, 11, 12) THEN 'Q1'
      WHEN month(add_months(load_date, -1)) IN (1, 2, 3) THEN 'Q2'
      WHEN month(add_months(load_date, -1)) IN (4, 5, 6) THEN 'Q3'
      WHEN month(add_months(load_date, -1)) IN (7, 8, 9) THEN 'Q4'
    END AS fy_quarter,
    CASE
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'October' THEN 1
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'November' THEN 2
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'December' THEN 3
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'January' THEN 4
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'February' THEN 5
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'March' THEN 6
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'April' THEN 7
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'May' THEN 8
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'June' THEN 9
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'July' THEN 10
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'August' THEN 11
      WHEN date_format(add_months(load_date, -1), "MMMM") = 'September' THEN 12
    END AS fy_month_int,
    class_count,
    case_count,
    load_date
  FROM {trgt_catalog}.gold.os34_report_abandonments_fytd
  WHERE day(load_date) = 1
    AND CASE WHEN month(load_date) >= 10 THEN year(load_date) + 1 ELSE year(load_date) END = year(current_date()) + (case when month(current_date()) < 10 then 0 else 1 end)
    AND load_date <= current_date()
)
SELECT
  fy,
  fy_month,
  fy_quarter,
  fy_month_int,
  SUM(class_count) AS Abandoned_classes_fy,
  SUM(class_count) AS Abandoned_classes,
  SUM(case_count) AS Abandoned_files_cases_fy,
  SUM(case_count) AS Abandoned_files_cases,
  358200 AS Abandoned_classes_fy_target,
  263500 AS Abandoned_files_cases_fy_target
FROM monthly_first_day
GROUP BY fy, fy_month, fy_quarter, fy_month_int
ORDER BY fy, fy_month_int
""")
#display(df_os34_dshb)

if full_refresh == 'N' and current_date_value == 1:#1:
    df_os34_dshb = spark.sql(df_os34_dshb_query)
else:
    df_os34_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,Abandoned_classes, Abandoned_files_cases, Abandoned_classes_fy, Abandoned_classes_fy_target, Abandoned_files_cases_fy, Abandoned_files_cases_fy_target
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#display(df_os34_dshb)

# COMMAND ----------

df_joined = df_os34_dshb.join(
    df_joined,
    (df_joined.year == df_os34_dshb.fy) &
    (df_joined.fy_month_int == df_os34_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_os34_dshb.fy_quarter),
    "right"
).drop(df_os34_dshb.fy,df_os34_dshb.fy_month,df_os34_dshb.fy_quarter,df_os34_dshb.fy_month_int)
#display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC **# 8.1 OS34:1**
# MAGIC Dashboard refreshed only on 1st of month
# MAGIC The extract refreshes at 12:05 am on 1st of month

# COMMAND ----------

df_os34_pndng_dshb_query = (f"""
SELECT
  SUM(case_count) AS Total_Pending_Applications_cases_38,
  SUM(class_count) AS Total_Pending_Applications_classes_39,
  SUM(case_count) AS Total_Pending_Applications_cases_38_fy,
  SUM(class_count) AS Total_Pending_Applications_classes_39_fy,
  -- Fiscal year starts in October
  CASE
    WHEN MONTH(current_date()) >= 10 THEN YEAR(current_date()) + 1
    ELSE YEAR(current_date())
  END AS fy,
  -- Fiscal month integer (Oct=1, ..., Sep=12), one month back
  ((MONTH(current_date()) + 1) % 12) + 1 AS fy_month_int,
  -- Fiscal month name, one month back
  CASE ((MONTH(current_date()) + 1) % 12) + 1
    WHEN 1 THEN 'Oct'
    WHEN 2 THEN 'Nov'
    WHEN 3 THEN 'Dec'
    WHEN 4 THEN 'Jan'
    WHEN 5 THEN 'Feb'
    WHEN 6 THEN 'Mar'
    WHEN 7 THEN 'Apr'
    WHEN 8 THEN 'May'
    WHEN 9 THEN 'Jun'
    WHEN 10 THEN 'Jul'
    WHEN 11 THEN 'Aug'
    WHEN 12 THEN 'Sep'
  END AS fy_month,
  -- Fiscal quarter, one month back
  CASE
    WHEN ((MONTH(current_date()) + 1) % 12) + 1 BETWEEN 1 AND 3 THEN 'Q1'
    WHEN ((MONTH(current_date()) + 1) % 12) + 1 BETWEEN 4 AND 6 THEN 'Q2'
    WHEN ((MONTH(current_date()) + 1) % 12) + 1 BETWEEN 7 AND 9 THEN 'Q3'
    ELSE 'Q4'
  END AS fy_quarter
FROM {trgt_catalog}.gold.os34_report_statuses
WHERE create_timestamp = (
    SELECT MAX(create_timestamp)
    FROM {trgt_catalog}.gold.os34_report_statuses
    WHERE is_static = true
  )
  AND abandoned = false
""")

#display(df_os34_pndng_dshb_query)

if full_refresh == 'N' and current_date_value == 1 :#1 Extract refreshes on 1st of month
    df_os34_pndng_dshb = spark.sql(df_os34_pndng_dshb_query)
else:
    df_os34_pndng_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,Total_Pending_Applications_cases_38, Total_Pending_Applications_classes_39,Total_Pending_Applications_cases_38_fy, Total_Pending_Applications_classes_39_fy
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#df_os34_pndng_dshb.display()

# COMMAND ----------

df_joined = df_os34_pndng_dshb.join(
    df_joined,
    (df_joined.year == df_os34_pndng_dshb.fy) &
    (df_joined.fy_month_int == df_os34_pndng_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_os34_pndng_dshb.fy_quarter),
    "right"
).drop(df_os34_pndng_dshb.fy,df_os34_pndng_dshb.fy_month,df_os34_pndng_dshb.fy_quarter,df_os34_pndng_dshb.fy_month_int)
#display(df_joined)


# COMMAND ----------

# MAGIC %md
# MAGIC # **5. TMIIMC38:1**
# MAGIC 83. Registrations including Classes
# MAGIC 86. Certificates of Registration Issued - Cases
# MAGIC 55. Published for Opposition - classes (email report sent every Monday and we take closest to month end)

# COMMAND ----------

df_tmiimc38_dshb_query = f"""
SELECT
  fy,
  fy_month,
  fy_month_fil,
  fy_quarter,
  fy_month_int,
  Published_for_Opposition_classes,
  Registrations_including_Classes,
  Certificates_of_Registration_Issued_Cases,
  SUM(Published_for_Opposition_classes) OVER (
    PARTITION BY fy ORDER BY fy_month_int
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Published_for_Opposition_classes_actual,
  CASE WHEN fy <= 2024 THEN 625262 ELSE 712000 END AS Published_for_Opposition_classes_target,
  SUM(Registrations_including_Classes) OVER (
    PARTITION BY fy ORDER BY fy_month_int
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Registrations_including_Classes_fy,
  CASE WHEN fy <= 2024 THEN 455900 ELSE 518400 END AS Registrations_including_Classes_fy_target,
  SUM(Certificates_of_Registration_Issued_Cases) OVER (
    PARTITION BY fy ORDER BY fy_month_int
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Certificates_of_Registration_Issued_Cases_fy,
  CASE WHEN fy <= 2024 THEN 335200 ELSE 381200 END AS Certificates_of_Registration_Issued_Cases_fy_target
FROM (
  SELECT
    CASE WHEN month(rundate) >= 10 THEN year(rundate) + 1 ELSE year(rundate) END AS fy,
    date_format(rundate, "MMMM") AS fy_month,
    date_format(rundate, "MMM") AS fy_month_fil,
    CASE
      WHEN date_format(rundate, "MMMM") = 'October' THEN 1
      WHEN date_format(rundate, "MMMM") = 'November' THEN 2
      WHEN date_format(rundate, "MMMM") = 'December' THEN 3
      WHEN date_format(rundate, "MMMM") = 'January' THEN 4
      WHEN date_format(rundate, "MMMM") = 'February' THEN 5
      WHEN date_format(rundate, "MMMM") = 'March' THEN 6
      WHEN date_format(rundate, "MMMM") = 'April' THEN 7
      WHEN date_format(rundate, "MMMM") = 'May' THEN 8
      WHEN date_format(rundate, "MMMM") = 'June' THEN 9
      WHEN date_format(rundate, "MMMM") = 'July' THEN 10
      WHEN date_format(rundate, "MMMM") = 'August' THEN 11
      WHEN date_format(rundate, "MMMM") = 'September' THEN 12
    END AS fy_month_int,
    CASE
      WHEN date_format(rundate, "MMMM") IN ('October', 'November', 'December') THEN 'Q1'
      WHEN date_format(rundate, "MMMM") IN ('January', 'February', 'March') THEN 'Q2'
      WHEN date_format(rundate, "MMMM") IN ('April', 'May', 'June') THEN 'Q3'
      WHEN date_format(rundate, "MMMM") IN ('July', 'August', 'September') THEN 'Q4'
    END AS fy_quarter,
    SUM(CASE WHEN category_description = 'Published for Opposition' THEN fee_paid_classes END) AS Published_for_Opposition_classes,
    SUM(CASE WHEN category_description != 'Published for Opposition' THEN fee_paid_classes END) AS Registrations_including_Classes,
    SUM(CASE WHEN category_description != 'Published for Opposition' THEN count END) AS Certificates_of_Registration_Issued_Cases
  FROM {trgt_catalog}.gold.tm_category_case_counts_hstry
  WHERE time_period = 'weekly'
    AND rundate >= to_date(
      CASE WHEN month(current_date()) < 10 THEN concat(year(current_date()) - 1, '-10-01')
           ELSE concat(year(current_date()), '-10-01')
      END
    )
    AND rundate <= current_date()
  GROUP BY
    CASE WHEN month(rundate) >= 10 THEN year(rundate) + 1 ELSE year(rundate) END,
    date_format(rundate, "MMMM"),
    date_format(rundate, "MMM"),
    CASE
      WHEN date_format(rundate, "MMMM") = 'October' THEN 1
      WHEN date_format(rundate, "MMMM") = 'November' THEN 2
      WHEN date_format(rundate, "MMMM") = 'December' THEN 3
      WHEN date_format(rundate, "MMMM") = 'January' THEN 4
      WHEN date_format(rundate, "MMMM") = 'February' THEN 5
      WHEN date_format(rundate, "MMMM") = 'March' THEN 6
      WHEN date_format(rundate, "MMMM") = 'April' THEN 7
      WHEN date_format(rundate, "MMMM") = 'May' THEN 8
      WHEN date_format(rundate, "MMMM") = 'June' THEN 9
      WHEN date_format(rundate, "MMMM") = 'July' THEN 10
      WHEN date_format(rundate, "MMMM") = 'August' THEN 11
      WHEN date_format(rundate, "MMMM") = 'September' THEN 12
    END,
    CASE
      WHEN date_format(rundate, "MMMM") IN ('October', 'November', 'December') THEN 'Q1'
      WHEN date_format(rundate, "MMMM") IN ('January', 'February', 'March') THEN 'Q2'
      WHEN date_format(rundate, "MMMM") IN ('April', 'May', 'June') THEN 'Q3'
      WHEN date_format(rundate, "MMMM") IN ('July', 'August', 'September') THEN 'Q4'
    END
)
ORDER BY fy, fy_month_int
"""

if full_refresh == "N" and current_date_value == 1:  # 1:
    df_tmiimc38_dshb = spark.sql(df_tmiimc38_dshb_query)
    df_tmiimc38_dshb=df_tmiimc38_dshb.drop('fy_month_fil')
else:
    df_tmiimc38_dshb = spark.sql(
        f""" select year, fy_quarter, fy_month, fy_month_int,  Published_for_Opposition_classes,
                                   Registrations_including_Classes,Certificates_of_Registration_Issued_Cases,Published_for_Opposition_classes_actual,Published_for_Opposition_classes_target,Registrations_including_Classes_fy,Registrations_including_Classes_fy_target,Certificates_of_Registration_Issued_Cases_fy,Certificates_of_Registration_Issued_Cases_fy_target
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')"""
    ).withColumnRenamed("year", "fy")

# COMMAND ----------

#display(df_tmiimc38_dshb)

# COMMAND ----------

df_joined = df_tmiimc38_dshb.join(
    df_joined,
    (df_joined.year == df_tmiimc38_dshb.fy) &
    (df_joined.fy_month_int == df_tmiimc38_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_tmiimc38_dshb.fy_quarter),
    "right"
).drop(df_tmiimc38_dshb.fy,df_tmiimc38_dshb.fy_month,df_tmiimc38_dshb.fy_quarter,df_tmiimc38_dshb.fy_month_int)
#display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC # **6. Notice of Allowance Report:1**
# MAGIC 60. Notice of Allowance Issued - classes

# COMMAND ----------

df_noa_report_query = f"""
select * from (
select fy_month,fy_month_fil, fy,fy_month_int,fy_quarter,
sum(notice_of_allowance_issued_classes) over (partition by fy) as notice_of_allowance_issued_classes ,
sum(notice_of_allowance_issued_classes) over (partition by fy) as notice_of_allowance_issued_classes_fy,
case when fy <=2024 then 225800 else 287200 end as notice_of_allowance_issued_classes_fy_target
from(
select fy_month,fy_month_fil, fy,fy_month_int,fy_quarter,
sum(classes)  as notice_of_allowance_issued_classes

from(
select 
date_format(noa_date,"MMMM") as fy_month,
date_format(noa_date,"MMM") as fy_month_fil,
    CASE WHEN month(noa_date) >= 10 THEN year(noa_date) + 1 ELSE year(noa_date) END AS fy,
    (case 
    when  date_format(noa_date,"MMMM") = 'October' then 1
    when  date_format(noa_date,"MMMM") = 'November' then 2
    when  date_format(noa_date,"MMMM") = 'December' then 3
    when  date_format(noa_date,"MMMM") = 'January' then 4
    when  date_format(noa_date,"MMMM") = 'February' then 5
    when  date_format(noa_date,"MMMM") = 'March' then 6
    when  date_format(noa_date,"MMMM") = 'April' then 7
    when  date_format(noa_date,"MMMM") = 'May' then 8
    when  date_format(noa_date,"MMMM") = 'June' then 9
    when  date_format(noa_date,"MMMM") = 'July' then 10
    when date_format(noa_date,"MMMM") = 'August' then 11
    when  date_format(noa_date,"MMMM") = 'September' then 12
end) as fy_month_int,
CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,
classes
 from  {trgt_catalog}.gold.noa_email_report)
 group by fy,fy_month, fy_month_fil,fy_quarter,fy_month_int))
 where (fy = '2026')

 """

if full_refresh == "N" and current_date_value == 1:  # 6:
    df_noa_report = spark.sql(df_noa_report_query)
    df_noa_report = df_noa_report.drop('fy_month_fil')
else:
    df_noa_report = spark.sql(
        f""" select year, fy_quarter, fy_month, fy_month_int,  notice_of_allowance_issued_classes,
        notice_of_allowance_issued_classes_fy,notice_of_allowance_issued_classes_fy_target
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')"""
    ).withColumnRenamed("year", "fy")

# COMMAND ----------

#display(df_noa_report)

# COMMAND ----------

df_joined = df_noa_report.join(
    df_joined,
    (df_joined.year == df_noa_report.fy) &
    (df_joined.fy_month_int == df_noa_report.fy_month_int) &
    (df_joined.fy_quarter == df_noa_report.fy_quarter),
    "right"
).drop(df_noa_report.fy,df_noa_report.fy_month,df_noa_report.fy_quarter,df_noa_report.fy_month_int)
#display(df_joined)

# COMMAND ----------

#display(df_joined)

# COMMAND ----------

from pyspark.sql.functions import when

df_joined = df_joined.withColumn(
    "Total_Pending_Applications_cases_38",
    when(df_joined.fy_month == "Oct", 885355)
    .when(df_joined.fy_month == "Nov", 876197)
    .when(df_joined.fy_month == "Dec", 868774)
    .when(df_joined.fy_month == "Jan", 870552)
    .when(df_joined.fy_month == "Feb", 871209)
    .when(df_joined.fy_month == "Mar", 871968)
    .otherwise(df_joined.Total_Pending_Applications_cases_38)
).withColumn(
    "Total_Pending_Applications_classes_39",
    when(df_joined.fy_month == "Oct", 1300422)
    .when(df_joined.fy_month == "Nov", 1291074)
    .when(df_joined.fy_month == "Dec", 1282326)
    .when(df_joined.fy_month == "Jan", 1282447)
    .when(df_joined.fy_month == "Feb", 1283794)
    .when(df_joined.fy_month == "Mar", 1286670)
    .otherwise(df_joined.Total_Pending_Applications_classes_39)
).withColumn(
    "Total_Pending_Applications_cases_38_fy",
    when(df_joined.fy_month == "Oct", 885355)
    .when(df_joined.fy_month == "Nov", 876197)
    .when(df_joined.fy_month == "Dec", 868774)
    .when(df_joined.fy_month == "Jan", 870552)
    .when(df_joined.fy_month == "Feb", 871209)
    .when(df_joined.fy_month == "Mar", 871968)
    .otherwise(df_joined.Total_Pending_Applications_cases_38_fy)
).withColumn(
    "Total_Pending_Applications_classes_39_fy",
    when(df_joined.fy_month == "Oct", 1300422)
    .when(df_joined.fy_month == "Nov", 1291074)
    .when(df_joined.fy_month == "Dec", 1282326)
    .when(df_joined.fy_month == "Jan", 1282447)
    .when(df_joined.fy_month == "Feb", 1283794)
    .when(df_joined.fy_month == "Mar", 1286670)
    .otherwise(df_joined.Total_Pending_Applications_classes_39_fy)
).withColumn(
    "notice_of_allowance_issued_classes",
    when(df_joined.fy_month == "Oct", 42772)
    .when(df_joined.fy_month == "Nov", 63004)
    .when(df_joined.fy_month == "Dec", 98867)
    .when(df_joined.fy_month == "Jan", 125675)
    .when(df_joined.fy_month == "Feb", 151928)
    .when(df_joined.fy_month == "Mar", 183092)
    .otherwise(df_joined.notice_of_allowance_issued_classes)
).withColumn(
    "notice_of_allowance_issued_classes_fy",
    when(df_joined.fy_month == "Oct", 42772)
    .when(df_joined.fy_month == "Nov", 63004)
    .when(df_joined.fy_month == "Dec", 98867)
    .when(df_joined.fy_month == "Jan", 125675)
    .when(df_joined.fy_month == "Feb", 151928)
    .when(df_joined.fy_month == "Mar", 183092)
    .otherwise(df_joined.notice_of_allowance_issued_classes_fy)
)

#display(df_joined)

# COMMAND ----------

#display(df_joined)

# COMMAND ----------

columns_order = ['year', 'fy_quarter', 'fy_month', 'fy_month_int'] + [col for col in df_joined.columns if col not in ['year', 'fy_quarter', 'fy_month', 'fy_month_int']]
df_reordered = df_joined.select(columns_order).withColumn("insert_ts",current_timestamp()).withColumn("last_update_ts",current_timestamp())
#display(df_reordered)

# COMMAND ----------

current_date_value = spark.sql("SELECT current_date() as current_date").collect()[0]['current_date'] 
current_month_int = current_date_value.month #replace 0 with 12
current_month = spark.sql("SELECT date_format((current_date()), 'MMM') as current_month").collect()[0]['current_month']# replace 0 with 12
current_fy = current_date_value.year + 1 if current_month_int >= 10 else current_date_value.year
df_final_full = df_reordered.filter(
    (df_reordered.year != current_fy) | (df_reordered.fy_month != current_month)
)
#df_final_full.display()

# COMMAND ----------

if full_refresh == 'N' :
    #df_final_full.write.mode("overwrite").option("mergeSchema", "true").format("delta").insertInto(f"{trgt_catalog}.gold.process_production_staffing_report_non_rolling")
#else:
    df_final_full.createOrReplaceTempView("temp_merge")
    spark.sql(f"""MERGE INTO {trgt_catalog}.gold.process_production_staffing_report_non_rolling AS target
        USING temp_merge AS source
        ON target.year = source.year
        and target.fy_month_int = source.fy_month_int
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *""")

# COMMAND ----------


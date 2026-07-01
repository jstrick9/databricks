# Databricks notebook source
# MAGIC %md
# MAGIC ### schedule wf to run on 1st, 2nd and 6th of the month at 10am
# MAGIC 1. Full refresh doesnot check for date and overwrites the table
# MAGIC 2. Incremental runs checks for date and only executes part of logic on 1st, 2nd and 6th of month and appends data into the table for the month

# COMMAND ----------

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
# MAGIC ###Stage Data Load for full refresh

# COMMAND ----------

from pyspark.sql.functions import col
df_stage = spark.read.csv(f"s3://bdr-databricks-app-prod/eds/trademark/process_production_staffing/process_prod_staffing_report_stage_load.csv",header=True)

if full_refresh == 'Y':
    target_table = f"{trgt_catalog}.gold.process_production_staffing_report"
    df_stage.createOrReplaceTempView("stage_data")
    
    merge_query = f"""
    MERGE INTO {target_table} AS target
    USING stage_data AS source
    ON target.year = source.year AND target.fy_month_int = source.fy_month_int
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """
    
    spark.sql(merge_query)

# COMMAND ----------

from pyspark.sql.functions import month, year, current_date

if full_refresh == 'N':
    current_date_value = spark.sql("SELECT current_date() as current_date").collect()[0]['current_date'] 
    current_month_int = current_date_value.month-1#replace 0 with 12
    current_fy = current_date_value.year + 1 if current_month_int >= 10 else current_date_value.year
    current_month = spark.sql("SELECT date_format(add_months(current_date(), -1), 'MMM') as current_month").collect()[0]['current_month']# replace 0 with 12
    current_date_value = spark.sql("SELECT dayofmonth(current_date()) as current_date").collect()[0]['current_date']
    #current_date_value=1
else:
    current_month_int = ''
    current_month = ''
    current_fy = ''
    current_date_value = ''

print(f"Current_Date = {current_date_value}, Current Month: {current_month_int} {current_month}, Current FY: {current_fy}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Pendency Dashboard:1
# MAGIC #### Refreshed only on 1st of the month
# MAGIC <pre>
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
# MAGIC
# MAGIC
# MAGIC </pre>

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
Pendency_to_First_Action_month as Pendency_to_First_Action_month,--_10,
Pendency_to_First_Action_fy as Pendency_to_First_Action_fy,--_10a,
First_Action_target_fy as First_Action_target_fy,--_10b,
Pendency_to_Registration_Abandonment_NOA_Exc as Pendency_to_Registration_Abandonment_NOA_Exc,--_12,
Pendency_to_Reg_fy_exc as Pendency_to_Reg_fy_exc,--_12a,
Pendency_to_Reg_Target_fy_EXC as Pendency_to_Reg_Target_fy_EXC,--_12b,
Pendency_to_Registration_Abandonment_NOA_INC as Pendency_to_Registration_Abandonment_NOA_INC,--_14,
Pendency_to_Reg_fy_inc as Pendency_to_Reg_fy_inc,--_14a,
 Pendency_to_Reg_Target_fy_inc as Pendency_to_Reg_Target_fy_inc,--_14b,
total_pendency_reg_135 as total_pendency_reg,
total_pendency_reg_fy_135a as total_pendency_reg_fy,
total_pendency_noa_136 as total_pendency_noa,
total_pendency_noa_fy_136a as total_pendency_noa_fy

 from
 (select * from
(SELECT 
fa_pendency_fy,fa_pendency_fy_month,fy_month_int,fa_pendency_fy_quarter,
round(sum(Active_Classes_FirstAction_ph) over (partition by fa_pendency_fy order by fy_month_int )/sum(Active_Classes_FirstAction) over (partition by fa_pendency_fy order by fy_month_int ),1) as Pendency_to_First_Action_month
from(
select 
fa_pendency_fy_month,
fa_pendency_fy,fa_pendency_fy_quarter,
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
sum(Active_Classes_FirstAction)Active_Classes_FirstAction
FROM 
{trgt_catalog}.gold.pendency_dashboard
where on_hold=False
--and (lower(fa_pendency_fy_month) = lower('{current_month}') or '{current_month}'='') 
--and (fa_pendency_fy = '{current_fy}' or '{current_fy}' = '') 
group by fa_pendency_fy,fa_pendency_fy_month,fa_pendency_fy_quarter
)
)
where (lower(fa_pendency_fy_month) = lower('{current_month}') or '{current_month}'='')
and (fa_pendency_fy = '{current_fy}' or '{current_fy}' = '')  
)FA_Month

inner join
(SELECT 
 total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,
round(sum(Active_Classes_Disposal_exc) over (partition by total_pendency_fy order by fy_month_int )/sum(Active_Classes_Disposal) over (partition by total_pendency_fy order by fy_month_int ),1) as Pendency_to_Registration_Abandonment_NOA_Exc
from(
select total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,
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
sum(Active_Classes_Disposal)Active_Classes_Disposal
FROM 
{trgt_catalog}.gold.pendency_dashboard
where on_hold=False
--and  (lower(total_pendency_fy_month) = lower('{current_month}') or '{current_month}'='')   
--and  (total_pendency_fy = '{current_fy}' or '{current_fy}' = '') 
and pendency_category  = 'No Suspension or Opposition'
group by total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,fy_month_int)
)total_Month_Reg_Exc
on fa_month.fa_pendency_fy = total_month_reg_exc.total_pendency_fy
and  fa_month.fa_pendency_fy_month = total_Month_Reg_Exc.total_pendency_fy_month 

inner join
(SELECT 
 total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,
round(sum(Active_Classes_Disposal_inc) over (partition by total_pendency_fy order by fy_month_int )/sum(Active_Classes_Disposal) over (partition by total_pendency_fy order by fy_month_int ),1) as Pendency_to_Registration_Abandonment_NOA_INC
from(
select total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,
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
sum(Active_Classes_Disposal)Active_Classes_Disposal
FROM 
{trgt_catalog}.gold.pendency_dashboard
where on_hold=False
--and  (lower(total_pendency_fy_month) = lower('{current_month}') or '{current_month}'='')   
--and  (total_pendency_fy = '{current_fy}' or '{current_fy}' = '') 
group by total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,fy_month_int)

)total_Month_Reg_Inc
on fa_month.fa_pendency_fy = total_Month_Reg_Inc.total_pendency_fy
and  fa_month.fa_pendency_fy_month = total_Month_Reg_Inc.total_pendency_fy_month 

inner join
(select total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,
round((((sum(total_Active_Classes_Reg))/(sum(Active_Classes_Disposal)))*100),0) as total_pendency_reg_135,
round((((sum(total_Active_Classes_NOA))/(sum(Active_Classes_Disposal)))*100),0) as total_pendency_noa_136
from(
select total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter,
case when disposal_type  = 'REGISTRATION' then Active_Classes_Disposal else 0 end as  total_Active_Classes_Reg,
case when disposal_type  = 'NOA' then Active_Classes_Disposal else 0 end as  total_Active_Classes_NOA,
Active_Classes_Disposal
FROM 
{trgt_catalog}.gold.pendency_dashboard
where on_hold=False
--and  (lower(total_pendency_fy_month) = lower('{current_month}') or '{current_month}'='') 
--and (total_pendency_fy = '{current_fy}' or '{current_fy}' = '') 
)
group by total_pendency_fy_month,total_pendency_fy,total_pendency_fy_quarter
)total_Month_Pen_Inc
on fa_month.fa_pendency_fy = total_Month_Pen_Inc.total_pendency_fy
and  fa_month.fa_pendency_fy_month = total_Month_Pen_Inc.total_pendency_fy_month 

inner join
(SELECT 
fa_pendency_fy,First_Action_target_fy,
round(sum(Active_Classes_FirstAction*first_action_pendency_ph)/sum(Active_Classes_FirstAction),1) as Pendency_to_First_Action_fy
from(
select 
fa_pendency_fy_month,
fa_pendency_fy,
Active_Classes_FirstAction,
first_action_pendency_ph,
CASE WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2021
THEN 4.5
WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2022
THEN 7.5
WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2023
THEN 8.5
WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) = 2024
THEN 8.4
WHEN YEAR(DATEADD(MONTH, 3, FIRST_Action_DT_PH)) > 2024
THEN 6.7
ELSE 3.5
END First_Action_target_fy
FROM 
{trgt_catalog}.gold.pendency_dashboard
where on_hold=False
 --and (lower(fa_pendency_fy_month) = lower('{current_month}') or '{current_month}'='') 
--and (fa_pendency_fy = '{current_fy}' or '{current_fy}' = '') 
--and fa_pendency_filter = True --This filter gets data only for 2025 but does not impact counts
)
group by fa_pendency_fy,First_Action_target_fy)FA_Year
on fa_month.fa_pendency_fy = FA_Year.fa_pendency_fy

inner join
(SELECT 
total_pendency_fy,
MAX(Total_Pendency_Target_fy_EXC)as Pendency_to_Reg_Target_fy_EXC
,MAX(Total_Pendency_Target_fy_INC) as Pendency_to_Reg_Target_fy_INC,
ROUND(sum(Active_Classes_Disposal*DISPOSAL_PENDENCY)/sum(Active_Classes_Disposal),1)  as Pendency_to_Reg_fy_Inc,
ROUND(sum(Active_Classes_Disposal_exc*DISPOSAL_PENDENCY_exc)/sum(Active_Classes_Disposal_exc),1)  as Pendency_to_Reg_fy_exc

from(
select 
total_pendency_fy_month,
total_pendency_fy,
Active_Classes_Disposal,DISPOSAL_PENDENCY,
case when Pendency_Category ='No Suspension or Opposition' then Active_Classes_Disposal else 0 end as  Active_Classes_Disposal_exc,
case when Pendency_Category ='No Suspension or Opposition' then DISPOSAL_PENDENCY else 0 end as DISPOSAL_PENDENCY_exc,
CASE when Pendency_Category = "No Suspension or Opposition"
AND (YEAR(DATEADD(month, 3, disposal_dt)) < 2022 )
THEN 12
when (Pendency_Category ="No Suspension or Opposition")
AND (YEAR(DATEADD(month, 3, disposal_dt)) = 2022) 
THEN 13.5
when (Pendency_Category ="No Suspension or Opposition")
AND (YEAR(DATEADD(month, 3, disposal_dt)) = 2023) 
THEN 14.5
when (Pendency_Category ="No Suspension or Opposition")
AND (YEAR(DATEADD(month, 3,disposal_dt)) = 2024) 
THEN 14.4
when (Pendency_Category ="No Suspension or Opposition")
AND (YEAR(DATEADD(month, 3,disposal_dt)) > 2024) 
THEN 13.0
END Total_Pendency_Target_fy_EXC,
CASE when (YEAR(DATEADD(month, 3, disposal_dt)) > 2023) 
THEN 15.4
else 15.5
END as Total_Pendency_Target_fy_INC
FROM 
{trgt_catalog}.gold.pendency_dashboard
where on_hold=False
--and  (lower(total_pendency_fy_month) = lower('{current_month}') or '{current_month}'='') 
--and (total_pendency_fy = '{current_fy}' or '{current_fy}' = '') 
--and fa_pendency_filter = True --This filter gets data only for 2025 but does not impact counts
)
group by total_pendency_fy )total_Year
on fa_month.fa_pendency_fy = total_Year.total_pendency_fy

inner join
(select total_pendency_fy,
round((((sum(total_Active_Classes_Reg))/(sum(Active_Classes_Disposal)))*100),0) as total_pendency_reg_fy_135a,
round((((sum(total_Active_Classes_NOA))/(sum(Active_Classes_Disposal)))*100),0) as total_pendency_noa_fy_136a
from(
select total_pendency_fy,
case when disposal_type  = 'REGISTRATION' then Active_Classes_Disposal else 0 end as  total_Active_Classes_Reg,
case when disposal_type  = 'NOA' then Active_Classes_Disposal else 0 end as  total_Active_Classes_NOA,
Active_Classes_Disposal
FROM 
{trgt_catalog}.gold.pendency_dashboard
where on_hold=False
--and (lower(total_pendency_fy_month) = lower('{current_month}') or '{current_month}'='') 
--and (total_pendency_fy = '{current_fy}' or '{current_fy}' = '') 
)
group by total_pendency_fy
)total_Pen_Inc
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

#df_ppsr_pendency.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Quality Dashboard:1
# MAGIC #### Refreshed only on 1st of the month

# COMMAND ----------

df_quality_dshb_query = (f"""
select 
quality_compliance_month.review_fy ,
quality_compliance_month.fy_quarter,
quality_compliance_month.fy_month,
quality_compliance_month.review_month,
quality_compliance_fy.First_Action_Compliance_Rate_fy_18a as First_Action_Compliance_Rate,
quality_compliance_fy.First_Action_Compliance_Rate_fy_18a as First_Action_Compliance_Rate_fy,
95.5 as First_Action_Compliance_Rate_target,--_18b,
final_compliance_rate_fy_19a as final_compliance_rate,
final_compliance_rate_fy_19a as final_compliance_rate_fy,
97.0 as Final_Compliance_Rate_target,--_19b,
Exceptional_First_Action_Rate_20 as Exceptional_First_Action_Rate,
Exceptional_First_Action_Rate_fy_20a as Exceptional_First_Action_Rate_fy,
50.0 as Exceptional_First_Action_Rate_target--_20b

from
(
select review_fy,fy_quarter, fy_month,review_month,
round((sum(num) over (partition by review_fy order by review_month )/sum(deno) over (partition by review_fy order by review_month))*100,1) as First_Action_Compliance_Rate18
from
(
select 
year(DATEADD(month, 3, lastreviewdatetime)) as review_fy,
fy_quarter, fy_month,
month(DATEADD(month, 3, lastreviewdatetime)) review_month,
(count(*) - sum(case when qualitymetricdeficientindicator = true then 1 else 0 end    ) ) as num,
count(*) as deno
--round(((count(*) - sum(case when qualitymetricdeficientindicator = true then 1 else 0 end    ) )/count(*))*100,1) as First_Action_Compliance_Rate18
    from {trgt_catalog}.gold.quality_dashboard
where review_type = "First Action"
and (month(lastreviewdatetime) = '{current_month_int}' or  '{current_month_int}'='')
and (year(DATEADD(month, 3, lastreviewdatetime)) = '{current_fy}' or '{current_fy}' = '')
group by review_fy,fy_quarter, fy_month,review_month)
)quality_compliance_month

inner join
(select review_fy,fy_quarter,fy_month,review_month,
round(round(
   ((sum(num1) over (partition by review_fy order by review_month )/sum(deno1) over (partition by review_fy order by review_month))*100),3)*.891
   +
   round(
   ((sum(num2) over (partition by review_fy order by review_month )/sum(deno2) over (partition by review_fy order by review_month))*100),3)*.109,1)final_compliance_rate_19
from (
select review_fy,fy_quarter,fy_month,review_month,
(sum(count_pub)- sum(pub_comp)) as num1,
sum(count_pub) as deno1,
(sum(count_final)- sum(final_comp)) as num2,
sum(count_final) as deno2
   from(
select year(DATEADD(month, 3, lastreviewdatetime)) as review_fy,
fy_quarter, fy_month,
month(DATEADD(month, 3, lastreviewdatetime)) review_month,
case when Review_Type = "PUB" then 1 else 0 end as count_pub,
case when Review_Type = "PUB" and qualitymetricdeficientindicator = true then 1 else 0 end as pub_comp,
case when Review_Type = "Final Action" then 1 else 0 end as count_final,
case when Review_Type = "Final Action" and qualitymetricdeficientindicator = true then 1 else 0 end as final_comp
from {trgt_catalog}.gold.quality_dashboard
where  (month(lastreviewdatetime) = '{current_month_int}' or  '{current_month_int}'='')
and  (year(DATEADD(month, 3, lastreviewdatetime)) = '{current_fy}' or '{current_fy}' = '')
)
group by review_fy,fy_quarter,fy_month,review_month))final_compliance_month
on quality_compliance_month.review_fy = final_compliance_month.review_fy
and quality_compliance_month.fy_month = final_compliance_month.fy_month

inner join
(select year(DATEADD(month, 3, lastreviewdatetime)) as review_fy,
fy_quarter, fy_month,
month(DATEADD(month, 3, lastreviewdatetime)) review_month,
round((sum(case when overallexcellentindicator = true then 1 else 0 end)/count(*))*100,1) as Exceptional_First_Action_Rate_20
from {trgt_catalog}.gold.quality_dashboard
where Review_Type = "Final Action"
and  (month(lastreviewdatetime) = '{current_month_int}' or  '{current_month_int}'='')
and  (year(DATEADD(month, 3, lastreviewdatetime)) = '{current_fy}' or '{current_fy}' = '')
group by review_fy,fy_quarter,fy_month,review_month)excellence_month
on quality_compliance_month.review_fy = excellence_month.review_fy
and quality_compliance_month.fy_month = excellence_month.fy_month

inner join
(select 
year(DATEADD(month, 3, lastreviewdatetime)) as review_fy,
round(((count(*) - sum(case when qualitymetricdeficientindicator = true then 1 else 0 end    ) )/count(*))*100,1) as First_Action_Compliance_Rate_fy_18a
    from {trgt_catalog}.gold.quality_dashboard
where review_type = "First Action"
and  (year(DATEADD(month, 3, lastreviewdatetime)) = '{current_fy}' or '{current_fy}' = '')
group by review_fy
)quality_compliance_fy
on quality_compliance_month.review_fy = quality_compliance_fy.review_fy

inner join
(select review_fy,
round(round(
   (((sum(count_pub)- sum(pub_comp))/sum(count_pub))*100),3)*.891
   +
   round(
   (((sum(count_final)- sum(final_comp))/sum(count_final))*100),3)*.109,1) as final_compliance_rate_fy_19a
   ---98.75 rounds t 0 98.8 in tableau and 98.7 in dbx
   from(
select year(DATEADD(month, 3, lastreviewdatetime)) as review_fy,
case when Review_Type = "PUB" then 1 else 0 end as count_pub,
case when Review_Type = "PUB" and qualitymetricdeficientindicator = true then 1 else 0 end as pub_comp,
case when Review_Type = "Final Action" then 1 else 0 end as count_final,
case when Review_Type = "Final Action" and qualitymetricdeficientindicator = true then 1 else 0 end as final_comp
from {trgt_catalog}.gold.quality_dashboard
where  (year(DATEADD(month, 3, lastreviewdatetime)) = '{current_fy}' or '{current_fy}' = '')
)
group by review_fy)final_compliance_fy
on quality_compliance_month.review_fy = final_compliance_fy.review_fy

inner join
(select year(DATEADD(month, 3, lastreviewdatetime)) as review_fy,
round((sum(case when overallexcellentindicator = true then 1 else 0 end)/count(*))*100,1) as Exceptional_First_Action_Rate_fy_20a
from {trgt_catalog}.gold.quality_dashboard
where Review_Type = "Final Action"
and  (year(DATEADD(month, 3, lastreviewdatetime)) = '{current_fy}' or '{current_fy}' = '')
group by review_fy)excellence_fy
on quality_compliance_month.review_fy = excellence_fy.review_fy

--order by review_fy desc, fy_quarter
""")
#display(df_quality_dshb)


if  full_refresh == 'N' and current_date_value == 1:#1: 
    df_quality_dshb = spark.sql(df_quality_dshb_query)
else:
    #df_quality_dshb = spark.createDataFrame([], schema=spark.sql(df_quality_dshb_query).schema)
    df_quality_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,First_Action_Compliance_Rate, First_Action_Compliance_Rate_fy, First_Action_Compliance_Rate_target, final_compliance_rate, final_compliance_rate_fy, Final_Compliance_Rate_target, Exceptional_First_Action_Rate, Exceptional_First_Action_Rate_fy, Exceptional_First_Action_Rate_target
                                 from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","review_fy").withColumnRenamed("fy_month_int","review_month")


# COMMAND ----------

#df_quality_dshb.display()

# COMMAND ----------

df_joined = df_ppsr_pendency.join(
    df_quality_dshb,
    (df_ppsr_pendency.year == df_quality_dshb.review_fy) &
    (df_ppsr_pendency.fy_quarter == df_quality_dshb.fy_quarter) &
    (df_ppsr_pendency.fy_month_int == df_quality_dshb.review_month),
    "left"
).drop(df_quality_dshb.fy_quarter,df_quality_dshb.fy_month,df_quality_dshb.review_month,df_quality_dshb.review_fy )
#display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Filings Dashboard:6
# MAGIC #### Refreshed only on 6th of the month

# COMMAND ----------

df_filing_dshb_query = (f"""
select
total_apps_filed_classes.filing_fy ,
total_apps_filed_classes.filing_fy_month ,
total_apps_filed_classes.filing_fy_quarter,
total_apps_filed_classes.filing_month_int,
filed_classes_27 as Total_Applications_Filed_classes,
--filed_classes_28 as Total_Applications_Filed_classes_fy,
sum(filed_classes_27) over (partition by total_apps_filed_classes.filing_fy order by filing_month_int)Total_Applications_Filed_classes_fy,
filed_classes_fy_28a as Total_Applications_Filed_classes_fy_actual,
case when total_apps_filed_classes.filing_fy <=2024 then 740000 else 820000 end as Total_Applications_Filed_classes_fy_target,---HC 28b
filed_cases_31 as Total_Application_Files_filings_cases,
--filed_cases_32 as Total_Application_Files_filings_cases_fy,
sum(filed_cases_31) over (partition by total_apps_filed_classes.filing_fy order by filing_month_int)Total_Application_Files_filings_cases_fy,
filed_cases_fy_32a as Total_Application_Files_filings_cases_fy_actual,
case when total_apps_filed_classes.filing_fy <=2024 then 544000 else 602900 end as Total_Application_Files_filings_cases_fy_target,---HC 32b
filed_classes_FYTD_growth_rate_29a as filed_classes_FYTD_growth_rate,
case when total_apps_filed_classes.filing_fy <=2024 then 0.4 else 6.9 end as filed_classes_FYTD_growth_rate_target,---HC 29b
filed_cases_FYTD_growth_rate_33a as filed_cases_FYTD_growth_rate,
filed_classes_month_growth_rate_29 as filed_classes_month_growth_rate,
filed_cases_month_growth_rate_32 as filed_cases_month_growth_rate

from

(select *, (case 
    when filing_fy_month = 'October' then 1
    when filing_fy_month = 'November' then 2
    when filing_fy_month = 'December' then 3
    when filing_fy_month = 'January' then 4
    when filing_fy_month = 'February' then 5
    when filing_fy_month = 'March' then 6
    when filing_fy_month = 'April' then 7
    when filing_fy_month = 'May' then 8
    when filing_fy_month = 'June' then 9
    when filing_fy_month = 'July' then 10
    when filing_fy_month = 'August' then 11
    when filing_fy_month = 'September' then 12
end) as filing_month_int,
--sum(filed_classes_27) over (partition by filing_fy order by filing_fy_month_int)filed_classes_28,
 --sum(filed_cases_31) over (partition by filing_fy order by filing_fy_month_int)filed_cases_32,
 round(((nvl(filed_classes_27,0) - nvl(lag(filed_classes_27,-1) over (order by filing_fy_month_int desc,filing_fy desc),0) )/abs(nvl(lag(filed_classes_27,-1) over (order by filing_fy_month_int desc,filing_fy desc),0))) *100,1) as filed_classes_month_growth_rate_29,

round(((filed_cases_31 - nvl(lag(filed_cases_31,-1) over (order by filing_fy_month_int desc,filing_fy desc),0))/nvl(lag(filed_cases_31,-1) over (order by filing_fy_month_int desc,filing_fy desc),0)) *100,1) as filed_cases_month_growth_rate_32

 from 
    (select  filing_fy,filing_fy_month, filing_fy_quarter,filing_fy_month_int,
sum(Fixed_Count) filed_classes_27, SUM(COUNT) as filed_cases_31
from {trgt_catalog}.gold.filings_dashboard
where  --(filing_fy_month_int <= '{current_month_int}' or  '{current_month_int}'='')
--and 
 (filing_fy = '{current_fy}' or '{current_fy}' = '') 
group by filing_fy, filing_fy_month,filing_fy_quarter,filing_fy_month_int
))total_apps_filed_classes

inner join

(select filing_fy,filed_classes_fy_28a,
round(((filed_classes_fy_28a - nvl(lag(filed_classes_fy_28a,-1) over (order by filing_fy desc),0))/nvl(lag(filed_classes_fy_28a,-1) over (order by filing_fy desc),0)) *100,1) as filed_classes_FYTD_growth_rate_29a,
filed_cases_fy_32a,
round(((filed_cases_fy_32a - nvl(lag(filed_cases_fy_32a,-1) over (order by filing_fy desc),0))/nvl(lag(filed_cases_fy_32a,-1) over (order by filing_fy desc),0)) *100,1) as 
filed_cases_FYTD_growth_rate_33a
from(
select  filing_fy,  
sum(Fixed_Count) filed_classes_fy_28a,
SUM(COUNT) filed_cases_fy_32a
from {trgt_catalog}.gold.filings_dashboard
where -- (filing_fy_month_int <= '{current_month_int}' or  '{current_month_int}'='')
--and 
(filing_fy = '{current_fy}' or '{current_fy}' = '') 
group by filing_fy))total_apps_filed_classes_fy
on total_apps_filed_classes.filing_fy = total_apps_filed_classes_fy.filing_fy
where (cast(filing_month_int as integer) <= ({current_month_int} +3) or  '{current_month_int}'='')
""")
#display(df_filing_dshb)

if full_refresh == 'N' and current_date_value == 6:#6: 
    df_filing_dshb = spark.sql(df_filing_dshb_query)
else:
    df_filing_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,Total_Applications_Filed_classes, Total_Applications_Filed_classes_fy, Total_Applications_Filed_classes_fy_actual, Total_Applications_Filed_classes_fy_target, Total_Application_Files_filings_cases, Total_Application_Files_filings_cases_fy, Total_Application_Files_filings_cases_fy_actual, Total_Application_Files_filings_cases_fy_target, filed_classes_FYTD_growth_rate, filed_classes_FYTD_growth_rate_target, filed_cases_FYTD_growth_rate, filed_classes_month_growth_rate, filed_cases_month_growth_rate
                                from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","filing_fy").withColumnRenamed("fy_month_int","filing_month_int").withColumnRenamed("fy_quarter","filing_fy_quarter").withColumnRenamed("fy_month","filing_fy_month")


# COMMAND ----------

#df_filing_dshb.display()

# COMMAND ----------

df_joined = df_filing_dshb.join(
    df_joined,
    (df_joined.year == df_filing_dshb.filing_fy) &
    (df_joined.fy_month_int == df_filing_dshb.filing_month_int) &
    (df_joined.fy_quarter == df_filing_dshb.filing_fy_quarter),
    "right"
).drop(df_filing_dshb.filing_fy,df_filing_dshb.filing_fy_quarter,df_filing_dshb.filing_fy_month,df_filing_dshb.filing_month_int)
#display(df_joined)


# COMMAND ----------

df_joined_filings = df_filing_dshb.withColumnRenamed("filing_month_int","fy_month_int").withColumnRenamed("filing_fy","year")

# COMMAND ----------

# MAGIC %md
# MAGIC  7. POST_REG DASHBOARD
# MAGIC #### emailed reports from Jim, sent in Every  week on Monday, get counts from latest report closest to 1st on month. 
# MAGIC #### Query counts captured on 1st of month
# MAGIC <pre>
# MAGIC Total Office Disposals:
# MAGIC Registrations including Classes
# MAGIC Certificates of Registration Issued - Cases
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md
# MAGIC df_post_reg_dshb_query = (f"""
# MAGIC select 
# MAGIC *,
# MAGIC sum(Registrations_including_Classes) over (partition by fy) as Registrations_including_Classes_fy,--83a
# MAGIC case when fy <=2024 then 455900 else 492600 end as Registrations_including_Classes_fy_target,---HC 83b
# MAGIC sum(Certificates_of_Registration_Issued_Cases)  over (partition by fy) as Certificates_of_Registration_Issued_Cases_fy,--86a,
# MAGIC case when fy <=2024 then 335200 else 362200 end as Certificates_of_Registration_Issued_Cases_fy_target---HC 86b
# MAGIC
# MAGIC from(
# MAGIC select  fy,fy_month,   
# MAGIC CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
# MAGIC     WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
# MAGIC     WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
# MAGIC     WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
# MAGIC     END AS fy_quarter,fy_month_int,
# MAGIC     sum(reg_class_count) as Registrations_including_Classes,--83
# MAGIC     count(*) as Certificates_of_Registration_Issued_Cases--86
# MAGIC     
# MAGIC from(
# MAGIC select 
# MAGIC  date_format(registration_dt,"MMMM") as fy_month,
# MAGIC     CASE WHEN month(registration_dt) >= 10 THEN year(registration_dt) + 1 ELSE year(registration_dt) END AS fy,
# MAGIC     (case 
# MAGIC     when  date_format(registration_dt,"MMMM") = 'October' then 1
# MAGIC     when  date_format(registration_dt,"MMMM") = 'November' then 2
# MAGIC     when  date_format(registration_dt,"MMMM") = 'December' then 3
# MAGIC     when  date_format(registration_dt,"MMMM") = 'January' then 4
# MAGIC     when  date_format(registration_dt,"MMMM") = 'February' then 5
# MAGIC     when  date_format(registration_dt,"MMMM") = 'March' then 6
# MAGIC     when  date_format(registration_dt,"MMMM") = 'April' then 7
# MAGIC     when  date_format(registration_dt,"MMMM") = 'May' then 8
# MAGIC     when  date_format(registration_dt,"MMMM") = 'June' then 9
# MAGIC     when  date_format(registration_dt,"MMMM") = 'July' then 10
# MAGIC     when date_format(registration_dt,"MMMM") = 'August' then 11
# MAGIC     when  date_format(registration_dt,"MMMM") = 'September' then 12
# MAGIC end) as fy_month_int,
# MAGIC * 
# MAGIC FROM {trgt_catalog}.gold.post_reg_dashboard
# MAGIC where max_dt_filter = True
# MAGIC and  (date_format(registration_dt,"MMM") = '{current_month}' or  '{current_month}'='')
# MAGIC and (CASE WHEN month(registration_dt) >= 10 THEN year(registration_dt) + 1 ELSE year(registration_dt) END  = '{current_fy}' or '{current_fy}' = '')
# MAGIC AND (Concat_Class IS NOT NULL OR LOWER(Concat_Class) != 'null'))
# MAGIC group by fy,fy_month,fy_month_int)
# MAGIC """)
# MAGIC
# MAGIC #display(df_post_reg_dshb)
# MAGIC if full_refresh == 'N' and current_date_value == 1:
# MAGIC     df_post_reg_dshb = spark.sql(df_post_reg_dshb_query)
# MAGIC else:
# MAGIC     df_post_reg_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int, Registrations_including_Classes, Certificates_of_Registration_Issued_Cases, Registrations_including_Classes_fy, Registrations_including_Classes_fy_target, Certificates_of_Registration_Issued_Cases_fy, Certificates_of_Registration_Issued_Cases_fy_target
# MAGIC                                   from {trgt_catalog}.gold.process_production_staffing_report 
# MAGIC                                  where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

# MAGIC %md
# MAGIC df_joined = df_post_reg_dshb.join(
# MAGIC     df_joined,
# MAGIC     (df_joined.year == df_post_reg_dshb.fy) &
# MAGIC     (df_joined.fy_month_int == df_post_reg_dshb.fy_month_int) &
# MAGIC     (df_joined.fy_quarter == df_post_reg_dshb.fy_quarter),
# MAGIC     "right"
# MAGIC ).drop(df_post_reg_dshb.fy,df_post_reg_dshb.fy_month,df_post_reg_dshb.fy_quarter,df_post_reg_dshb.fy_month_int)
# MAGIC #display(df_joined)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. TMIIFY15 Dashboard:1
# MAGIC #### (report mailed by Jim, 1st of month)

# COMMAND ----------

df_tmiify15_dshb_query = (f"""
select fy,fy_month,fy_month_fil,fy_quarter,  fy_month_int,
    Section_9_Applications_Filed_fy as Section_9_Applications_Filed,
    Registrations_Renewed_fy as Registrations_Renewed,
    Affidavits_under_Section_8_15_71_Combinations_Filed_fy as Affidavits_under_Section_8_15_71_Combinations_Filed,
    Affidavits_under_Section_8_15_71_Combinations_Disposed_fy as Affidavits_under_Section_8_15_71_Combinations_Disposed,
    Section_8_Applications_Filed_10yr_fy as Section_8_Applications_Filed_10yr,
Section_9_Applications_Filed_fy,Section_9_Applications_Filed_fy_target,
Registrations_Renewed_fy,Registrations_Renewed_target,
Affidavits_under_Section_8_15_71_Combinations_Filed_fy,Affidavits_under_Section_8_15_71_Combinations_Filed_fy_target,
Affidavits_under_Section_8_15_71_Combinations_Disposed_fy,
Affidavits_under_Section_8_15_71_Combinations_Disposed_fy_target,
Section_8_Applications_Filed_10yr_fy, Section_8_Applications_Filed_10yr_fy_target
 from(                          
select 
*,
sum(Section_9_Applications_Filed) over (partition by fy)  as Section_9_Applications_Filed_fy,--104a
case when fy <=2024 then 96500 else 99900 end as Section_9_Applications_Filed_fy_target,--104b
sum(Registrations_Renewed) over (partition by fy) as Registrations_Renewed_fy,--110a
case when fy <=2024 then 104000 else 88100 end as Registrations_Renewed_target,--110b
sum(Affidavits_under_Section_8_15_71_Combinations_Filed) over (partition by fy) as Affidavits_under_Section_8_15_71_Combinations_Filed_fy,--114a
case when fy <=2024 then 94400 else 70100 end as Affidavits_under_Section_8_15_71_Combinations_Filed_fy_target,--114b
sum(Affidavits_under_Section_8_15_71_Combinations_Disposed) over (partition by fy) as Affidavits_under_Section_8_15_71_Combinations_Disposed_fy,--117a
case when fy <=2024 then 98500 else 99300 end as Affidavits_under_Section_8_15_71_Combinations_Disposed_fy_target,--117b
sum(Section_8_Applications_Filed_10yr) over (partition by fy) as Section_8_Applications_Filed_10yr_fy,--107a
case when fy<=2024 then 128400 else 132300 end as Section_8_Applications_Filed_10yr_fy_target
from(
select fy,fy_month,fy_month_fil,  CASE
      WHEN filing_month_int IN (1, 2, 3) THEN 'Q1'
      WHEN filing_month_int IN (4, 5, 6) THEN 'Q2'
      WHEN filing_month_int IN (7, 8, 9) THEN 'Q3'
      WHEN filing_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,filing_month_int as fy_month_int,
sum(Section_9_Applications_Filed) as Section_9_Applications_Filed,--104
sum(Registrations_Renewed) as Registrations_Renewed,--110
sum(Affidavits_under_Section_8_15_71_Combinations_Filed) as Affidavits_under_Section_8_15_71_Combinations_Filed,--114,
sum(Affidavits_under_Section_8_15_71_Combinations_Disposed) as Affidavits_under_Section_8_15_71_Combinations_Disposed,--117
sum(Section_8_Applications_Filed_10yr) as Section_8_Applications_Filed_10yr--107
from(
select
      sbe.legacy_cm_ent_cd,
      be.cfk_object_gid serial_num,
      cast(be.effective_ts as date) as effective_ts,
      date_format(effective_ts,"MMMM") as fy_month,
      date_format(effective_ts,"MMM") as fy_month_fil,
    CASE WHEN month(effective_ts) >= 10 THEN year(effective_ts) + 1 ELSE year(effective_ts) END AS fy,
    (case 
    when  date_format(effective_ts,"MMMM") = 'October' then 1
    when  date_format(effective_ts,"MMMM") = 'November' then 2
    when  date_format(effective_ts,"MMMM") = 'December' then 3
    when  date_format(effective_ts,"MMMM") = 'January' then 4
    when  date_format(effective_ts,"MMMM") = 'February' then 5
    when  date_format(effective_ts,"MMMM") = 'March' then 6
    when  date_format(effective_ts,"MMMM") = 'April' then 7
    when  date_format(effective_ts,"MMMM") = 'May' then 8
    when  date_format(effective_ts,"MMMM") = 'June' then 9
    when  date_format(effective_ts,"MMMM") = 'July' then 10
    when date_format(effective_ts,"MMMM") = 'August' then 11
    when  date_format(effective_ts,"MMMM") = 'September' then 12
end) as filing_month_int,
      (case when sbe.legacy_cm_ent_cd in ('9.AF', '89F', 'E89R') then 1 else 0 end) as Section_9_Applications_Filed,--104
      (case when substring(sbe.legacy_cm_ent_cd, 1, 3) in ('REN', 'RNL') then 1 else 0 end )as Registrations_Renewed,--110
      (case when sbe.legacy_cm_ent_cd in ('8.AF','815F','15AF','ES8R','E815','ES71','71AF','ES75','71AF','E15R') then 1 else 0 end) as Affidavits_under_Section_8_15_71_Combinations_Filed,--114,
      (case when sbe.legacy_cm_ent_cd in ('8.OK','8.PR','C15A','C15P','71AG','71.P','C75A','C75P','15AK') then 1 else 0 end) as Affidavits_under_Section_8_15_71_Combinations_Disposed,--117
      (case when sbe.legacy_cm_ent_cd in ('8AFT', '89AF', 'E89R', '9.AF') then 1 else 0 end) as Section_8_Applications_Filed_10yr--107
    from

     {src_catalog}.bronze.business_event be
      INNER JOIN {src_catalog}.bronze.stnd_business_event_reason sbe ON be.fk_business_event_reason_id = sbe.business_event_reason_id
    where
      (sbe.legacy_cm_ent_cd in ('9.AF','89F','E89R','8.AF','815F','15AF','ES8R','E815','ES71','71AF','ES75','71AF','E15R','8.OK','8.PR','C15A','C15P','71AG','71.P','C75A','C75P','15AK','89AF','8AFT','89AG','8PRT','8OKT','9G8P','12AF','P12C','IUAF','IUAA','R.PRA','R.SRA')
      or substring(sbe.legacy_cm_ent_cd, 1, 3) in ('REN', 'RNL')
      or sbe.business_event_reason_cd in ('R.PRA', 'R.SRA')
      )
      and cast(be.effective_ts as date) between (
        select
          case
            when month(current_date) < 11 then cast((year(current_date) -1) as string) || '-10' || '-01'
            else cast(year(current_date) as string) || '-10' || '-01'
          end
      )
      and last_day(current_date() - interval '1' month)
  )
group by fy,fy_month,fy_quarter,filing_month_int,fy_month_fil)
)
  where   (fy_month_fil = '{current_month}' or  '{current_month}'='') 
and  (fy  = '{current_fy}' or '{current_fy}' = '') 
--order by fy desc, fy_quarter desc, filing_month_int asc
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

#df_tmiify15_dshb.display()

# COMMAND ----------

df_joined = df_tmiify15_dshb.join(
    df_joined,
    (df_joined.year == df_tmiify15_dshb.fy) &
    (df_joined.fy_month_int == df_tmiify15_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_tmiify15_dshb.fy_quarter),
    "right"
).drop(df_tmiify15_dshb.fy,df_tmiify15_dshb.fy_month,df_tmiify15_dshb.fy_quarter,df_tmiify15_dshb.fy_month_int)
#display(df_joined)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. TMIIFY20 Dashboard:1
# MAGIC #### (emailed reports from Jim, scheduled to run on 1st  or manual request if not recieved)

# COMMAND ----------

df_tmiify20_dshb_query = (f"""
with active_classes as (
  SELECT  fk_trademark_gid serial_num, count(*)  classes 
  FROM {src_catalog}.bronze.TM_CLASS WHERE FK_TM_CLASS_STATUS_CD in ('6', '8','P','W') GROUP BY FK_TRADEMARK_GID 
  ),
raw as (
select
sbe.legacy_cm_ent_cd as legacy_cm_ent_cd,
be.cfk_object_gid serial_num,effective_ts,
--cast(be.effective_ts as date) as effective_ts,
date_format(effective_ts,"MMMM") as fy_month,
date_format(effective_ts,"MMM") as fy_month_fil,
CASE WHEN month(effective_ts) >= 10 THEN year(effective_ts) + 1 ELSE year(effective_ts) END AS fy,
(case 
    when  date_format(effective_ts,"MMMM") = 'October' then 1
    when  date_format(effective_ts,"MMMM") = 'November' then 2
    when  date_format(effective_ts,"MMMM") = 'December' then 3
    when  date_format(effective_ts,"MMMM") = 'January' then 4
    when  date_format(effective_ts,"MMMM") = 'February' then 5
    when  date_format(effective_ts,"MMMM") = 'March' then 6
    when  date_format(effective_ts,"MMMM") = 'April' then 7
    when  date_format(effective_ts,"MMMM") = 'May' then 8
    when  date_format(effective_ts,"MMMM") = 'June' then 9
    when  date_format(effective_ts,"MMMM") = 'July' then 10
    when date_format(effective_ts,"MMMM") = 'August' then 11
    when  date_format(effective_ts,"MMMM") = 'September' then 12
end) as fy_month_int,
case  when sbe.legacy_cm_ent_cd in ('SUPC') then 1 else 0 end TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE,
case when sbe.legacy_cm_ent_cd in ('EISU') then 1 else 0 end TOTAL_STATEMENTS_OF_USE_FILED
from
{src_catalog}.bronze.business_event be
INNER JOIN {src_catalog}.bronze.stnd_business_event_reason sbe ON be.fk_business_event_reason_id = sbe.business_event_reason_id
where sbe.legacy_cm_ent_cd in ('EISU','SUPC') 
AND   effective_ts <= last_day(current_date() - interval '1' month)
--and (date_format(effective_ts,"MMM") = '{current_month}' or  '{current_month}'='')
--and  (CASE WHEN month(effective_ts) >= 10 THEN year(effective_ts) + 1 ELSE year(effective_ts) END   = '{current_fy}' or '{current_fy}' = '') 

),
classes as (
select --R.serial_num, legacy_cm_ent_cd, effective_ts,
fy,r.fy_month,r.fy_month_fil, r.fy_month_int,
sum(case when TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE = 1 THEN a.classes ELSE 0 END) AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES, 
sum(CASE WHEN TOTAL_STATEMENTS_OF_USE_FILED =1 THEN a.classes ELSE 0 END) AS TOTAL_STATEMENTS_OF_USE_FILED_CLASSES--, 
--a.classes
FROM raw r inner join active_classes a 
on  r.serial_num = a.serial_num
where  legacy_cm_ent_cd in ('EISU','SUPC') 
and effective_ts <= last_day(current_date() - interval '1' month)
group by fy,r.fy_month,r.fy_month_fil, r.fy_month_int
)
select
fy,fy_month,fy_quarter,fy_month_int,
TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy as TOTAL_STATEMENTS_OF_USE_FILED_CLASSES,
TOTAL_STATEMENTS_OF_USE_FILED_fy as TOTAL_STATEMENTS_OF_USE_FILED,
TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES,
TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE,
TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy,
TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy_target,
TOTAL_STATEMENTS_OF_USE_FILED_fy,TOTAL_STATEMENTS_OF_USE_FILED_fy_target,
TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy,TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy_target,
TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy,TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy_target
from
(
select 
*,
sum(TOTAL_STATEMENTS_OF_USE_FILED_CLASSES) over (partition by fy) as TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy,--63a
case when fy<=2024 then 126826 else 140537 end as TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy_target,--63b
sum(TOTAL_STATEMENTS_OF_USE_FILED) over (partition by fy) as TOTAL_STATEMENTS_OF_USE_FILED_fy,--66a
case when fy<=2024 then 93300 else 103300 end as TOTAL_STATEMENTS_OF_USE_FILED_fy_target,--66b
sum(TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES) over (partition by fy) as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy,--69a
case when fy<=2024 then 120500 else 133500 end as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy_target,--69b
sum(TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE) over (partition by fy) as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy,--72a
case when fy<=2024 then 98200 else 88600 end as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy_target--72b
from
(
select  raw.fy,raw.fy_month, raw.fy_month_fil, CASE
      WHEN raw.fy_month_int IN (1, 2, 3) THEN 'Q1'
      WHEN raw.fy_month_int IN (4, 5, 6) THEN 'Q2'
      WHEN raw.fy_month_int IN (7, 8, 9) THEN 'Q3'
      WHEN raw.fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,raw.fy_month_int,
    sum(TOTAL_STATEMENTS_OF_USE_FILED_CLASSES) as TOTAL_STATEMENTS_OF_USE_FILED_CLASSES,--63
    sum(TOTAL_STATEMENTS_OF_USE_FILED) as TOTAL_STATEMENTS_OF_USE_FILED,--66
    sum(TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES) as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES,--69
    sum(TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE) as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE--72   
from (select fy,fy_month,fy_month_fil,fy_month_int,
sum(TOTAL_STATEMENTS_OF_USE_FILED) as TOTAL_STATEMENTS_OF_USE_FILED, 
sum(TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE) as TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE
from raw
group by fy,fy_month,fy_month_fil,fy_month_int)raw
inner JOIN 
classes 
on raw.fy = classes.fy
and raw.fy_month = classes.fy_month
and raw.fy_month_int = classes.fy_month_int
group by raw.fy,raw.fy_month,raw.fy_month_fil,raw.fy_month_int))
where ( fy_month_fil = '{current_month}' or  '{current_month}'='')
and  fy   = '{current_fy}' or '{current_fy}' = ''
--order by fy desc, fy_month_int asc

""")
#display(df_tmiify20_dshb)

if full_refresh == 'N' and current_date_value == 1:#1:
    df_tmiify20_dshb = spark.sql(df_tmiify20_dshb_query)
else:
    df_tmiify20_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,TOTAL_STATEMENTS_OF_USE_FILED_CLASSES, TOTAL_STATEMENTS_OF_USE_FILED, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE, TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy, TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy_target, TOTAL_STATEMENTS_OF_USE_FILED_fy, TOTAL_STATEMENTS_OF_USE_FILED_fy_target, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy_target, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy_target
                                  from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#df_tmiify20_dshb.display()

# COMMAND ----------

df_joined = df_tmiify20_dshb.join(
    df_joined,
    (df_joined.year == df_tmiify20_dshb.fy) &
    (df_joined.fy_month_int == df_tmiify20_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_tmiify20_dshb.fy_quarter),
    "right"
).drop(df_tmiify20_dshb.fy,df_tmiify20_dshb.fy_month,df_tmiify20_dshb.fy_quarter,df_tmiify20_dshb.fy_month_int)
#display(df_joined)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Unexamined_dashboard:1
# MAGIC ###Capture counts until 1st of current month instead of last day of previous month(enhancement)
# MAGIC ###Median age of inventory is off by .1
# MAGIC #### (Direct Dashboard, counts captured as of last day of the month, after the data refresh for the last day of the month)
# MAGIC <pre>
# MAGIC Unexamined New Applications (cases) - prior to first action
# MAGIC Unexamined New Applications (classes) - prior to first action
# MAGIC Median age of inventory (in Months)
# MAGIC </pre>
# MAGIC

# COMMAND ----------

df_unexamined_dshb_query = (f"""
select distinct
max_dt.fy,fy_month, fy_month_int, 
CASE
      WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
      WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
      WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
      WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,
raw_monthly.unexamined_cases as Unexamined_New_Applicationn_cases_prior_to_first_action,--97
raw_monthly.unexamined_classes as Unexamined_New_Applicationn_classes_prior_to_first_action,--98
raw_monthly.unexamined_cases as Unexamined_New_Applicationn_cases_prior_to_first_action_fy,--97a
case when max_dt.fy<=2024 then 485000 else 402780 end as Unexamined_New_Applicationn_cases_prior_to_first_action_fy_target,--97b
raw_monthly.unexamined_classes as Unexamined_New_Applicationn_classes_prior_to_first_action_fy,--98a
case when max_dt.fy<=2024 then 485000 else 383000 end as Unexamined_New_Applicationn_classes_prior_to_first_action_fy_target,--98b
Median_Unexamined_Inventory_Age_FY as median_age_of_inventory,
Median_Unexamined_Inventory_Age_FY as Median_age_of_inventory_fy,--100a
case when max_dt.fy<=2024 then 5.0 else 3.7 end as Median_age_of_inventory_fy_target--100b
from (
  SELECT 
distinct date_format(unexamined_date,"MMMM") as fy_month,
date_format(unexamined_date,"MMM") as fy_month_fil,
CASE WHEN month(unexamined_date) >= 10 THEN year(unexamined_date) + 1 ELSE year(unexamined_date) END AS fy,
(case 
    when  date_format(unexamined_date,"MMMM") = 'October' then 1
    when  date_format(unexamined_date,"MMMM") = 'November' then 2
    when  date_format(unexamined_date,"MMMM") = 'December' then 3
    when  date_format(unexamined_date,"MMMM") = 'January' then 4
    when  date_format(unexamined_date,"MMMM") = 'February' then 5
    when  date_format(unexamined_date,"MMMM") = 'March' then 6
    when  date_format(unexamined_date,"MMMM") = 'April' then 7
    when  date_format(unexamined_date,"MMMM") = 'May' then 8
    when  date_format(unexamined_date,"MMMM") = 'June' then 9
    when  date_format(unexamined_date,"MMMM") = 'July' then 10
    when date_format(unexamined_date,"MMMM") = 'August' then 11
    when  date_format(unexamined_date,"MMMM") = 'September' then 12
end) as fy_month_int,
max(unexamined_date) over (partition by fy,month(unexamined_date) ) as max_unexamined_date_monthly--,
--max(unexamined_date) over (partition by fy ) as max_unexamined_date_fy
FROM {trgt_catalog}.gold.inventory_unexamined_hstry
--where (date_format(unexamined_date,"MMM") = '{current_month}' or  '{current_month}'='')
--and   (CASE WHEN month(unexamined_date) >= 10 THEN year(unexamined_date) + 1 ELSE year(unexamined_date) END   = '{current_fy}' or '{current_fy}' = '')

) max_dt
inner join {trgt_catalog}.gold.inventory_unexamined_hstry raw_monthly
on max_dt.max_unexamined_date_monthly = raw_monthly.unexamined_date
--inner join {trgt_catalog}.gold.inventory_unexamined_hstry raw_fy
--on max_dt.max_unexamined_date_fy = raw_fy.unexamined_date
left join(WITH base_data AS (
  SELECT 
    Pendency_Cal_Start_DT,
    fy
  FROM {trgt_catalog}.gold.inventory_dashboard_running
  WHERE Count_Type = 'Actual'
),
ranked_data AS (
  SELECT 
    Pendency_Cal_Start_DT,
    fy,
    ROW_NUMBER() OVER (ORDER BY fy,Pendency_Cal_Start_DT) AS row_num,
    COUNT(*) OVER (order by fy) AS total_rows
  FROM base_data
),
median_data AS (
  SELECT 
    Pendency_Cal_Start_DT,
    fy
  FROM ranked_data
  WHERE row_num = (total_rows + 1) / 2
     OR (total_rows % 2 = 0 AND row_num = (total_rows / 2) + 1)
),
median_date AS (
  SELECT 
    date(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(Pendency_Cal_Start_DT AS STRING), 'yyyy-MM-dd'))) AS Median_Unexamined_Inventory,
    fy
  FROM median_data

)
SELECT 
  fy,
  round((DATEDIFF(CURRENT_DATE, Median_Unexamined_Inventory) / 30.42), 2) AS Median_Unexamined_Inventory_Age_FY
FROM median_date)median

on max_dt.fy = median.fy
where  ( fy_month_fil = '{current_month}' or  '{current_month}'='')
and  max_dt.fy   = '{current_fy}' or '{current_fy}' = ''


""")
#display(df_unexamined_dshb)

if full_refresh == 'N' and current_date_value == 1:#1:
    df_unexamined_dshb = spark.sql(df_unexamined_dshb_query)
else:
    df_unexamined_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int, Unexamined_New_Applicationn_cases_prior_to_first_action, Unexamined_New_Applicationn_classes_prior_to_first_action, Unexamined_New_Applicationn_cases_prior_to_first_action_fy, Unexamined_New_Applicationn_cases_prior_to_first_action_fy_target, Unexamined_New_Applicationn_classes_prior_to_first_action_fy, Unexamined_New_Applicationn_classes_prior_to_first_action_fy_target, Median_age_of_inventory,Median_age_of_inventory_fy, Median_age_of_inventory_fy_target
                                    from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#df_unexamined_dshb.display()

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
# MAGIC ### 8.OS34:1
# MAGIC #### Dashboard refreshed only on 1st of month
# MAGIC <pre> 
# MAGIC
# MAGIC Office Disposals:
# MAGIC Abandoned - classes
# MAGIC Abandoned files - cases
# MAGIC
# MAGIC </pre>
# MAGIC

# COMMAND ----------

df_os34_dshb_query = (f"""
WITH monthly as (
  SELECT
    TM.LEGACY_STATUS_CD as Status_Code,
    SLS.DESCRIPTION_TX as Status_Description,
    TM.serial_num_tx as SERIAL_NUM,
    SUM(
      CASE
        WHEN TMC.fk_trademark_gid IS NOT NULL THEN 1
        ELSE 0
      END
    ) ACTIVE_CLASS_CNT,
    DATE(MILESTONE_DT) as Action_Date,
    'Y' as ABANDONED
  FROM
    {src_catalog}.BRONZE.TRADEMARK TM
    LEFT JOIN {src_catalog}.BRONZE.STND_LEGACY_STATUS SLS ON SLS.STATUS_NO = TM.LEGACY_STATUS_CD
    LEFT JOIN {src_catalog}.BRONZE.TM_CLASS TMC ON TM.TRADEMARK_GID = TMC.FK_TRADEMARK_GID
    LEFT JOIN {src_catalog}.BRONZE.TM_MILESTONE TMM ON TM.TRADEMARK_GID = TMM.FK_TRADEMARK_GID
  WHERE
    TM.LEGACY_STATUS_CD IN (600,601,602,603,604,605,606,607,608,609,612,614,618)
    AND (TMM.FK_TM_MILESTONE_CD = 'ABAND')
  GROUP BY
    Status_Code,
    Status_Description,
    SERIAL_NUM_TX,
    Action_Date
)
select 
fy,fy_month,  fy_quarter,fy_month_int,
Abandoned_classes_fy as Abandoned_classes,--77
Abandoned_files_cases_fy as Abandoned_files_cases,--80
Abandoned_classes_fy,
Abandoned_classes_fy_target,
Abandoned_files_cases_fy,
Abandoned_files_cases_fy_target
from(
select 
*,
sum(Abandoned_classes) over (partition by fy) as Abandoned_classes_fy,--77a
case when fy<=2024 then 323203 else 335248 end as Abandoned_classes_fy_target,--77b
sum(Abandoned_files_cases)  over (partition by fy) as Abandoned_files_cases_fy,--80a,
case when fy<=2024 then 237600 else 246506 end as Abandoned_files_cases_fy_target--80b
FROM
(select  fy,fy_month,  fy_month_fil,
CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,fy_month_int,
    sum(ACTIVE_CLASS_CNT) as Abandoned_classes,--77
    count(distinct SERIAL_NUM) as Abandoned_files_cases--80
    
from(select
   date_format(Action_Date,"MMMM") as fy_month,
   date_format(Action_Date,"MMM") as fy_month_fil,
    CASE WHEN month(Action_Date) >= 10 THEN year(Action_Date) + 1 ELSE year(Action_Date) END AS fy,
    (case 
    when  date_format(Action_Date,"MMMM") = 'October' then 1
    when  date_format(Action_Date,"MMMM") = 'November' then 2
    when  date_format(Action_Date,"MMMM") = 'December' then 3
    when  date_format(Action_Date,"MMMM") = 'January' then 4
    when  date_format(Action_Date,"MMMM") = 'February' then 5
    when  date_format(Action_Date,"MMMM") = 'March' then 6
    when  date_format(Action_Date,"MMMM") = 'April' then 7
    when  date_format(Action_Date,"MMMM") = 'May' then 8
    when  date_format(Action_Date,"MMMM") = 'June' then 9
    when  date_format(Action_Date,"MMMM") = 'July' then 10
    when date_format(Action_Date,"MMMM") = 'August' then 11
    when  date_format(Action_Date,"MMMM") = 'September' then 12
end) as fy_month_int, *
from
  monthly
where
 ABANDONED ='Y'
 --and calendar_date >=current_date()
  --and Action_Date between '1975-01-01'  and current_date()
 and  Action_Date between  to_date(case when MONTH(current_date()) < 10 then (year(current_date()) - 1) else year(current_date()) end||"/10/01","yyyy/MM/dd")  and current_date()
--and Action_Date <= current_date()-23
--and   (date_format(Action_Date,"MMM") = '{current_month}' or  '{current_month}'='') 
--and  (CASE WHEN month(Action_Date) >= 10 THEN year(Action_Date) + 1 ELSE year(Action_Date) END  = '{current_fy}' or '{current_fy}' = '')
 )
 group by fy,fy_month,fy_month_int,fy_month_fil))
 where  fy_month_fil = '{current_month}' or  '{current_month}'='' 
and  (fy = '{current_fy}' or '{current_fy}' = '')
""")

#display(df_os34_dshb)

if full_refresh == 'N' and current_date_value == 1:#1:
    df_os34_dshb = spark.sql(df_os34_dshb_query)
else:
    df_os34_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,Abandoned_classes, Abandoned_files_cases, Abandoned_classes_fy, Abandoned_classes_fy_target, Abandoned_files_cases_fy, Abandoned_files_cases_fy_target
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#df_os34_dshb.display()

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
# MAGIC ### 8.1 OS34:1
# MAGIC #### Dashboard refreshed only on 1st of month
# MAGIC ##The extract refreshes at 12:05 am on 1st of month
# MAGIC <pre> 
# MAGIC
# MAGIC
# MAGIC </pre>

# COMMAND ----------

df_os34_pndng_dshb_query = (f"""
with base as (
  select
    A.*
  from
    (
      select
        AM.AM_SER_NUM,
        AM.AM_STAT,
        CL.CL_SER_NUM,
        CL.CL_CLS_STAT,
        AM.AM_REG_NUM,
        AM.AM_DT_FIL,
        date(AM.AM_STAT_DT)
      from
        (
          select
            tm.serial_num_tx as AM_SER_NUM,
            tm.legacy_status_cd as AM_STAT,
            tm.registration_num as AM_REG_NUM,
            date(tm.status_dt) as AM_STAT_DT,
            tm.filing_dt as AM_DT_FIL
          from
            {src_catalog}.bronze.trademark tm

        ) AM
        left outer join (
          select
            split(fk_trademark_gid, ':') [2] as CL_SER_NUM,
            fk_tm_class_status_cd as CL_CLS_STAT
          from
            {src_catalog}.bronze.tm_class
        ) CL on AM.AM_SER_NUM = CL.CL_SER_NUM
 
      where
        (
          AM.AM_STAT in (616,620,622,630,631,632,638,640,641,642,643,644,645,646,647,648,649,650,651,652,653,654,661,663,664,665,666,667,668,672,680,681,686,688,689,690,692,693,694,715,718,719,720,721,722,724,725,730,731,732,733,734,744,745,746,747,748,752,753,756,757,760,762,763,764,765,766,772,773,774,775,777,779,782,783,784,785,794,801,802,803,806,807,808,809,810,811,812,813,814,815,816,817,818,819
          )
          or (
            AM.AM_STAT = 771
            and AM.AM_REG_NUM is null
          )
        )
    ) A
),
aggregates as (
  select
    DATE(B.AM_STAT_DT) AM_STAT_DT,
    COUNT(
      DISTINCT CASE
        WHEN B.CL_CLS_STAT in ('6', 'W', 'P')
        and B.AM_STAT not in (622, 620, 632) THEN B.CL_SER_NUM
        ELSE NULL
      END
    ) TOT_APP_CASES,
    Sum(
      case
        when B.CL_SER_NUM is not NULL
        and B.CL_CLS_STAT in ('6', 'W', 'P')
        and B.AM_STAT not in(622, 620, 632) then 1
        else 0
      end
    ) TOT_APP_CLASS
  from
    base B
  group by
    AM_STAT_DT
)
SELECT fy,fy_month,fy_month_int,fy_quarter,
Total_Pending_Applications_cases_38_fy as Total_Pending_Applications_cases_38,
Total_Pending_Applications_classes_39_fy as Total_Pending_Applications_classes_39,
Total_Pending_Applications_cases_38_fy,
Total_Pending_Applications_classes_39_fy

 FROM
(
select *,
sum(Total_Pending_Applications_cases_38) over () as Total_Pending_Applications_cases_38_fy,
sum(Total_Pending_Applications_classes_39) over () as Total_Pending_Applications_classes_39_fy
from(
select 
distinct fy,fy_month,fy_month_fil,fy_month_int,CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,
sum(TOT_APP_Cases)  as Total_Pending_Applications_cases_38,
sum(TOT_APP_CLASS)  as Total_Pending_Applications_classes_39
 from
 (
select
date_format(AM_STAT_DT,"MMMM") as fy_month,
date_format(AM_STAT_DT,"MMM") as fy_month_fil,
    CASE WHEN month(AM_STAT_DT) >= 10 THEN year(AM_STAT_DT) + 1 ELSE year(AM_STAT_DT) END AS fy,
    (case 
    when  date_format(AM_STAT_DT,"MMMM") = 'October' then 1
    when  date_format(AM_STAT_DT,"MMMM") = 'November' then 2
    when  date_format(AM_STAT_DT,"MMMM") = 'December' then 3
    when  date_format(AM_STAT_DT,"MMMM") = 'January' then 4
    when  date_format(AM_STAT_DT,"MMMM") = 'February' then 5
    when  date_format(AM_STAT_DT,"MMMM") = 'March' then 6
    when  date_format(AM_STAT_DT,"MMMM") = 'April' then 7
    when  date_format(AM_STAT_DT,"MMMM") = 'May' then 8
    when  date_format(AM_STAT_DT,"MMMM") = 'June' then 9
    when  date_format(AM_STAT_DT,"MMMM") = 'July' then 10
    when date_format(AM_STAT_DT,"MMMM") = 'August' then 11
    when  date_format(AM_STAT_DT,"MMMM") = 'September' then 12
end) as fy_month_int,
*
from
  aggregates
where
  AM_STAT_DT between '1975-01-01'
  and current_date
  
)group by fy,fy_month,fy_month_int,fy_month_fil
))
WHERE    (fy_month_fil = '{current_month}' or  '{current_month}'='') 
and  fy = '{current_fy}' or '{current_fy}' = ''
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
# MAGIC ###9. EP11:2
# MAGIC #### (EP11 2nd  of month , refresh only previous month, rest stays static)
# MAGIC <pre>
# MAGIC First Examination Processing:
# MAGIC First Actions - initial exam classes
# MAGIC Abandonment's - classes
# MAGIC Approved for Publication - classes
# MAGIC Total Balanced Disposals
# MAGIC </pre>
# MAGIC
# MAGIC

# COMMAND ----------

df_ep11_dshb_query = (f"""
 select
fy,fy_month, fy_quarter,fy_month_int,
First_Actions_initial_exam_classes_fy as First_Actions_initial_exam_classes,
Abandonment_classes_fy as Abandonment_classes,
Approved_for_Publication_classes_fy as Approved_for_Publication_classes,
Total_Balanced_Disposals_fy as Total_Balanced_Disposals,
First_Actions_initial_exam_classes_fy,First_Actions_initial_exam_classes_fy_target,
Abandonment_classes_fy,Abandonment_classes_fy_target,Approved_for_Publication_classes_fy,Approved_for_Publication_classes_fy_target,
Total_Balanced_Disposals_fy,Total_Balanced_Disposals_fy_target
 from(                     
select 
*,
sum(First_Actions_initial_exam_classes) over (partition by fy) as First_Actions_initial_exam_classes_fy,--43a
case when fy<=2024 then 806000 else 869655 end as First_Actions_initial_exam_classes_fy_target,--43b
sum(Abandonment_classes)  over (partition by fy) as Abandonment_classes_fy,--46a,
case when fy<=2024 then 130000 else 153080 end as Abandonment_classes_fy_target,--46b
sum(Approved_for_Publication_classes)  over (partition by fy) as Approved_for_Publication_classes_fy,--49a,
case when fy<=2024 then 645000 else 696600 end as Approved_for_Publication_classes_fy_target,--49b
sum(Total_Balanced_Disposals)  over (partition by fy) as Total_Balanced_Disposals_fy,--52a,
case when fy<=2024 then 1580000 else 1720000 end as Total_Balanced_Disposals_fy_target--52b
from
(
select fy,fy_month,  fy_month_fil,
CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,fy_month_int,
   -- sum(TOT_FA_INIT_FY_CL) as 
   sum(First_Actions_initial_exam_classes) as First_Actions_initial_exam_classes,--43
    --sum(ABAN_CT_FY) as 
    sum(Abandonment_classes) as Abandonment_classes,--46
    --sum(APP_PUB_CT_FY) as 
    sum(Approved_for_Publication_classes) as Approved_for_Publication_classes,--49
    --sum(Total_Balance_Disposals_Y),
    sum(Total_Balanced_Disposals) as Total_Balanced_Disposals--52
    from(
select
   date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") as fy_month,
   date_format(TRANSACTION_EFFECTIVE_DT,"MMM") as fy_month_fil,
    CASE WHEN month(TRANSACTION_EFFECTIVE_DT) >= 10 THEN year(TRANSACTION_EFFECTIVE_DT) + 1 ELSE year(TRANSACTION_EFFECTIVE_DT) END AS fy,
    (case 
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'October' then 1
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'November' then 2
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'December' then 3
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'January' then 4
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'February' then 5
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'March' then 6
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'April' then 7
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'May' then 8
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'June' then 9
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'July' then 10
    when date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'August' then 11
    when  date_format(TRANSACTION_EFFECTIVE_DT,"MMMM") = 'September' then 12
end) as fy_month_int,
--TOT_FA_INIT_FY_CL,
----sum(case when ABAN_CR_FY!=0 then WK_ACTV_CLS else 0 end) as ABAN_CT_FY,
--APP_PUB_CT_FY,
--((case when APP_PUB_CT_FY!=0 THEN WK_ACTV_CLS ELSE 0 END) + (case when ABAN_CT_FY!=0 THEN WK_ACTV_CLS ELSE 0 END) + 
--(case when TOT_FA_INIT_FY_CL!=0 THEN WK_ACTV_CLS ELSE 0 END)) as Total_Balanced_Disposals,
Total_Balance_Disposals_Y as Total_Balanced_Disposals,
--(case when  FA_INIT_FY_CL!=0 THEN WK_ACTV_CLS else 0 end) as First_Actions_initial_exam_classes,
(case when  TOT_FA_INIT_FY_CL!=0 THEN WK_ACTV_CLS ELSE 0 END)as First_Actions_initial_exam_classes,
--(case when APP_PUB_CR_FY!=0 THEN WK_ACTV_CLS ELSE 0 END) as Approved_for_Publication_classes,
(case when APP_PUB_CT_FY!=0 THEN WK_ACTV_CLS ELSE 0 END ) as Approved_for_Publication_classes,
--(case when ABAN_CR_FY!=0 then WK_ACTV_CLS else 0 end)as Abandonment_classes
(case when ABAN_CT_FY!=0 THEN WK_ACTV_CLS ELSE 0 END) as Abandonment_classes
FROM  {trgt_catalog}.silver.epquery_stg3
where WORKER_NAME is not null

 )
group by fy, fy_month, fy_month_int,fy_month_fil))
where  (fy_month_fil = '{current_month}' or  '{current_month}'='')
and fy  = '{current_fy}' or '{current_fy}' = ''
                     """)

#display(df_ep11_dshb)

if full_refresh == 'N' and current_date_value == 2:#2:
    df_ep11_dshb = spark.sql(df_ep11_dshb_query)
else:
    df_ep11_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,First_Actions_initial_exam_classes, Abandonment_classes, Approved_for_Publication_classes, Total_Balanced_Disposals, First_Actions_initial_exam_classes_fy, First_Actions_initial_exam_classes_fy_target, Abandonment_classes_fy, Abandonment_classes_fy_target, Approved_for_Publication_classes_fy, Approved_for_Publication_classes_fy_target, Total_Balanced_Disposals_fy, Total_Balanced_Disposals_fy_target
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#df_ep11_dshb.display()

# COMMAND ----------

df_joined = df_ep11_dshb.join(
    df_joined,
    (df_joined.year == df_ep11_dshb.fy) &
    (df_joined.fy_month_int == df_ep11_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_ep11_dshb.fy_quarter),
    "right"
).drop(df_ep11_dshb.fy,df_ep11_dshb.fy_month,df_ep11_dshb.fy_quarter,df_ep11_dshb.fy_month_int)
#display(df_joined)


# COMMAND ----------

# MAGIC %md
# MAGIC ###10. Calculation
# MAGIC <pre>
# MAGIC Total Office Disposals:
# MAGIC Classes (Registrations and Abandonment's)
# MAGIC Files (Registrations and Abandonment's) 
# MAGIC </pre>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###11. Request for Extension of Protection - 66(a) Filing Basis:1
# MAGIC <pre>
# MAGIC 35. Total Requests for Extension of Protection 66(a) 
# MAGIC 36. Application Files - filed
# MAGIC </pre>

# COMMAND ----------

df_filingBasis_dshb_query = (f"""
WITH rpt_cm_66a AS (
    SELECT
        be.order_no,
        be.cfk_object_gid,
        legacy_cm_ent_cd,
        legacy_cm_ent_type_cd,
        be.last_mod_user_id,
        be.effective_ts,
        be.last_mod_ts
    FROM
             {src_catalog}.bronze.business_event be
        INNER JOIN {src_catalog}.bronze.stnd_business_event_reason sbe ON be.fk_business_event_reason_id = sbe.business_event_reason_id
    WHERE
        legacy_cm_ent_cd IN ( 'REPR', 'SDRC' )
        AND be.cfk_object_gid LIKE '%0:79%'
), internationals AS (
    SELECT
        irt.CFK_TRADEMARK_GID,
        notification_dt,
        international_reg_dt
    FROM
             {tmintltm_src_catalog}.bronze.international_tm  it
        INNER JOIN {tmintltm_src_catalog}.bronze.international_registration ir ON ir.fk_international_reg_no = it.international_reg_no
        INNER JOIN {tmintltm_src_catalog}.bronze.international_reg_tm        irt ON irt.fk_international_reg_gid = ir.international_reg_gid
), class_count AS (
    SELECT
        fk_trademark_gid,
        COUNT(*) active_classes
    FROM
        {src_catalog}.bronze.tm_class
    WHERE
        fk_tm_class_status_cd IN ( '6', '8', 'P', 'W' )
    GROUP BY
        fk_trademark_gid
), state_country_view AS (
    SELECT
        fk_trademark_gid,
        country_nm
    FROM
             {src_catalog}.bronze.tm_party_role tpr
        INNER JOIN {src_catalog}.bronze.interested_party ip ON tpr.fk_interested_party_gid = ip.interested_party_gid
)
select * from
(
select 
distinct fy,fy_month,fy_month_fil,fy_month_int,CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,
    count(distinct serial_number) as Total_Requests_for_Extension_of_Protection,--35
 sum(count(distinct serial_number)) over (partition by fy order by fy_month_int) as Application_Files_filed,--36
 sum(count(distinct serial_number)) over (partition by fy) as Application_Files_filed_fy,
 case when fy<=2024 then 31500 else 29700 end as Application_Files_filed_target
from(
SELECT 
date_format(notification_dt,"MMMM") as fy_month,
date_format(notification_dt,"MMM") as fy_month_fil,
    CASE WHEN month(notification_dt) >= 10 THEN year(notification_dt) + 1 ELSE year(notification_dt) END AS fy,
    (case 
    when  date_format(notification_dt,"MMMM") = 'October' then 1
    when  date_format(notification_dt,"MMMM") = 'November' then 2
    when  date_format(notification_dt,"MMMM") = 'December' then 3
    when  date_format(notification_dt,"MMMM") = 'January' then 4
    when  date_format(notification_dt,"MMMM") = 'February' then 5
    when  date_format(notification_dt,"MMMM") = 'March' then 6
    when  date_format(notification_dt,"MMMM") = 'April' then 7
    when  date_format(notification_dt,"MMMM") = 'May' then 8
    when  date_format(notification_dt,"MMMM") = 'June' then 9
    when  date_format(notification_dt,"MMMM") = 'July' then 10
    when date_format(notification_dt,"MMMM") = 'August' then 11
    when  date_format(notification_dt,"MMMM") = 'September' then 12
end) as fy_month_int,
    serial_num_tx         serial_number
FROM
         rpt_cm_66a rpt
    INNER JOIN {src_catalog}.bronze.trademark     t ON cfk_object_gid = t.trademark_gid
    INNER JOIN internationals                   i ON i.CFK_TRADEMARK_GID = t.trademark_gid
    INNER JOIN class_count                      c ON c.fk_trademark_gid = t.trademark_gid
    LEFT JOIN state_country_view                scv ON scv.fk_trademark_gid = t.trademark_gid
WHERE
        fk_filed_fee_process_type_cd = 'MADRD'
    AND legacy_status_cd <> 622
and date(notification_dt) > cast('2020' as date)
--and  (date_format(notification_dt,"MMM") = '{current_month}' or  '{current_month}'='')
--and (CASE WHEN month(notification_dt) >= 10 THEN year(notification_dt) + 1 ELSE year(notification_dt) END  = '{current_fy}' or '{current_fy}' = '') 
)

group by fy,fy_month,fy_month_int,fy_month_fil)
where  (fy_month_fil= '{current_month}' or  '{current_month}'='')
and (fy = '{current_fy}' or '{current_fy}' = '')
 """)

#display(df_filingBasis_dshb_query)

if full_refresh == 'N' and current_date_value == 1:#2:
    df_filingBasis_dshb = spark.sql(df_filingBasis_dshb_query)
    df_filingBasis_dshb = df_filingBasis_dshb.drop('fy_month_fil')
else:
    df_filingBasis_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,    Total_Requests_for_Extension_of_Protection,Application_Files_filed,Application_Files_filed_fy, Application_Files_filed_target
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#df_filingBasis_dshb.display()

# COMMAND ----------

df_joined = df_filingBasis_dshb.join(
    df_joined,
    (df_joined.year == df_filingBasis_dshb.fy) &
    (df_joined.fy_month_int == df_filingBasis_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_filingBasis_dshb.fy_quarter),
    "right"
).drop(df_filingBasis_dshb.fy,df_filingBasis_dshb.fy_month,df_filingBasis_dshb.fy_quarter,df_filingBasis_dshb.fy_month_int)
#display(df_joined)


# COMMAND ----------

# MAGIC %md
# MAGIC ##12. TMIIMC38:1
# MAGIC <pre>
# MAGIC 83. Registrations including Classes
# MAGIC 86. Certificates of Registration Issued - Cases
# MAGIC 55. Published for Opposition - classes (email report sent every Monday and we take closest to month end)
# MAGIC </pre>

# COMMAND ----------

df_tmiimc38_dshb_query = f"""
select * from
(
select * ,
sum(Published_for_Opposition_classes) over (partition by fy) as Published_for_Opposition_classes_actual,
case when fy<=2024 then 625262 else 675702 end as Published_for_Opposition_classes_target,
sum(Registrations_including_Classes) over (partition by fy) as Registrations_including_Classes_fy,--83a
case when fy <=2024 then 455900 else 492600 end as Registrations_including_Classes_fy_target,---HC 83b
sum(Certificates_of_Registration_Issued_Cases)  over (partition by fy) as Certificates_of_Registration_Issued_Cases_fy,--86a
case when fy <=2024 then 335200 else 362200 end as Certificates_of_Registration_Issued_Cases_fy_target---HC 86b
from(
select date_format(rundate,"MMMM") as fy_month,date_format(rundate,"MMM") as fy_month_fil,
    CASE WHEN month(rundate) >= 10 THEN year(rundate) + 1 ELSE year(rundate) END AS fy,
    (case 
    when  date_format(rundate,"MMMM") = 'October' then 1
    when  date_format(rundate,"MMMM") = 'November' then 2
    when  date_format(rundate,"MMMM") = 'December' then 3
    when  date_format(rundate,"MMMM") = 'January' then 4
    when  date_format(rundate,"MMMM") = 'February' then 5
    when  date_format(rundate,"MMMM") = 'March' then 6
    when  date_format(rundate,"MMMM") = 'April' then 7
    when  date_format(rundate,"MMMM") = 'May' then 8
    when  date_format(rundate,"MMMM") = 'June' then 9
    when  date_format(rundate,"MMMM") = 'July' then 10
    when date_format(rundate,"MMMM") = 'August' then 11
    when  date_format(rundate,"MMMM") = 'September' then 12
end) as fy_month_int,
CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,
--rundate,
--DATE_FORMAT(current_date(), 'yyyy-MM-01'),
sum(case when category_description = 'Published for Opposition' then fee_paid_classes end) as Published_for_Opposition_classes ,--55
sum(case when category_description != 'Published for Opposition' then fee_paid_classes   end) as Registrations_including_Classes,--83
sum(case when category_description != 'Published for Opposition' then count end ) Certificates_of_Registration_Issued_Cases--86

 from (select add_months(rundate,-1)as rundate, category_description, fee_paid_classes,count
  from {trgt_catalog}.gold.tm_category_case_counts_hstry--{trgt_catalog}
where time_period = 'year_to_date'
and rundate = DATE_FORMAT(current_date(), 'yyyy-MM-01')
)
group by rundate))
where (fy_month_fil = '{current_month}' or  '{current_month}'='') 
and (fy = '{current_fy}' or '{current_fy}' = '')

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

#df_tmiimc38_dshb.display()

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
# MAGIC ## 13. Notice of Allowance Report:1
# MAGIC <pre>
# MAGIC 60. Notice of Allowance Issued - classes
# MAGIC </pre>

# COMMAND ----------

df_noa_report_query = f"""
select * from (
select fy_month,fy_month_fil, fy,fy_month_int,fy_quarter,
sum(notice_of_allowance_issued_classes) over (partition by fy) as notice_of_allowance_issued_classes ,
sum(notice_of_allowance_issued_classes) over (partition by fy) as notice_of_allowance_issued_classes_fy,
case when fy <=2024 then 225800 else 229000 end as notice_of_allowance_issued_classes_fy_target
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
 from  trm_reporting.gold.noa_email_report)
 group by fy,fy_month, fy_month_fil,fy_quarter,fy_month_int))
 where (fy_month_fil = '{current_month}' or  '{current_month}'='') 
and (fy = '{current_fy}' or '{current_fy}' = '')

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

#df_noa_report.display()

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

# MAGIC %md
# MAGIC ##Merge Filing into Target table on 6th

# COMMAND ----------


if full_refresh == 'N' and current_date_value == 6 :
    #df_final_full.write.mode("overwrite").option("mergeSchema", "true").format("delta").insertInto(f"{trgt_catalog}.gold.process_production_staffing_report")
#else:
    df_final_filings = df_joined_filings.withColumn("last_update_ts",current_timestamp())
    df_final_filings.createOrReplaceTempView("temp_merge")
    spark.sql(f"""MERGE INTO {trgt_catalog}.gold.process_production_staffing_report AS target
        USING temp_merge AS source
        ON target.year = source.year
        and target.fy_month_int = source.fy_month_int
        WHEN MATCHED THEN
        UPDATE SET 
        target.total_applications_filed_classes= source.Total_Applications_Filed_classes
        ,target.Total_Applications_Filed_classes_fy=source.Total_Applications_Filed_classes_fy 
        ,target.Total_Applications_Filed_classes_fy_actual = source.Total_Applications_Filed_classes_fy_actual
        ,target.Total_Application_Files_filings_cases = source.Total_Application_Files_filings_cases
        ,target.Total_Application_Files_filings_cases_fy = source.Total_Application_Files_filings_cases_fy
        ,target.Total_Application_Files_filings_cases_fy_actual = source.Total_Application_Files_filings_cases_fy_actual
        ,target.last_update_ts =  source.last_update_ts
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge into target table

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
    #df_final_full.write.mode("overwrite").option("mergeSchema", "true").format("delta").insertInto(f"{trgt_catalog}.gold.process_production_staffing_report")
#else:
    df_final_full.createOrReplaceTempView("temp_merge")
    spark.sql(f"""MERGE INTO {trgt_catalog}.gold.process_production_staffing_report AS target
        USING temp_merge AS source
        ON target.year = source.year
        and target.fy_month_int = source.fy_month_int
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Workdays Calculation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION IF NOT EXISTS udf_workdays(StartDate date, EndDate date)
# MAGIC RETURNS int
# MAGIC RETURN
# MAGIC (
# MAGIC     REDUCE(
# MAGIC       SEQUENCE(StartDate, EndDate),
# MAGIC       0,
# MAGIC       (acc, x) -> acc + IF(DATE_FORMAT(x, 'E') IN ('Sat', 'Sun'), 0, 1))
# MAGIC );

# COMMAND ----------

df_workdays = spark.sql(f"""
select year, fy_month, fy_quarter, fy_month_int, 
to_date(date_trunc('month', to_date(concat(year, '-', (case 
    when fy_month = 'Oct' then '10'
    when  fy_month = 'Nov' then '11'
    when  fy_month = 'Dec' then '12'
    when  fy_month = 'Jan' then '01'
    when  fy_month = 'Feb' then '02'
    when  fy_month = 'Mar' then '03'
    when  fy_month = 'Apr' then '04'
    when  fy_month = 'May' then '05'
    when  fy_month = 'Jun' then '06'
    when  fy_month = 'Jul' then '07'
    when  fy_month = 'Aug' then '08'
    when  fy_month = 'Sep' then '09'
end), '-01')))) as start_date,
last_day(to_date(concat(year, '-', (case 
    when fy_month = 'Oct' then '10'
    when  fy_month = 'Nov' then '11'
    when  fy_month = 'Dec' then '12'
    when  fy_month = 'Jan' then '01'
    when  fy_month = 'Feb' then '02'
    when  fy_month = 'Mar' then '03'
    when  fy_month = 'Apr' then '04'
    when  fy_month = 'May' then '05'
    when  fy_month = 'Jun' then '06'
    when  fy_month = 'Jul' then '07'
    when  fy_month = 'Aug' then '08'
    when  fy_month = 'Sep' then '09'
end), '-01'))) as end_date,
udf_workdays(start_date, end_date) as workdays_in_month
from {trgt_catalog}.gold.process_production_staffing_report
""")
#df_workdays.write.mode("overwrite").option("mergeSchema", "true").format("delta").insertInto(f"{trgt_catalog}.gold.process_production_staffing_workdays")
#df_workdays.write.saveAsTable(f"{trgt_catalog}.gold.process_production_staffing_workdays")
df_workdays.write.mode("overwrite").option("mergeSchema", "true").format("delta").insertInto(f"{trgt_catalog}.gold.process_production_staffing_workdays")

# COMMAND ----------

recs_count = df_joined.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
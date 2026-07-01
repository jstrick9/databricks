# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("full_refresh","","full_refresh")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
full_refresh = dbutils.widgets.get("full_refresh").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
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
# MAGIC # **1. Request for Extension of Protection - 66(a) Filing Basis:1**
# MAGIC 35. Total Requests for Extension of Protection 66(a) 
# MAGIC 36. Application Files - filed

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
distinct year,fy_month,fy_month_fil,fy_month_int,CASE WHEN fy_month_int IN (1, 2, 3) THEN 'Q1'
    WHEN fy_month_int IN (4, 5, 6) THEN 'Q2'
    WHEN fy_month_int IN (7, 8, 9) THEN 'Q3'
    WHEN fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,
    count(distinct serial_number) as Total_Requests_for_Extension_of_Protection,--35
 sum(count(distinct serial_number)) over (partition by year order by fy_month_int) as Application_Files_filed,--36
 sum(count(distinct serial_number)) over (partition by year) as Application_Files_filed_fy,
 case when year<=2024 then 31500 else 31300 end as Application_Files_filed_target
from(
SELECT 
date_format(notification_dt,"MMMM") as fy_month,
date_format(notification_dt,"MMM") as fy_month_fil,
    CASE WHEN month(notification_dt) >= 10 THEN year(notification_dt) + 1 ELSE year(notification_dt) END AS year,
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

group by year,fy_month,fy_month_int,fy_month_fil)
where  (year = 2026)
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

# MAGIC %md
# MAGIC # **2. Filings Dashboard:6**
# MAGIC #### Refreshed only on 6th of the month

# COMMAND ----------

df_filing_dshb_query = f"""
WITH monthly_classes AS (
  SELECT
    filing_fy,
    filing_fy_month,
    filing_fy_quarter,
    CASE
      WHEN filing_fy_month = 'October' THEN 1
      WHEN filing_fy_month = 'November' THEN 2
      WHEN filing_fy_month = 'December' THEN 3
      WHEN filing_fy_month = 'January' THEN 4
      WHEN filing_fy_month = 'February' THEN 5
      WHEN filing_fy_month = 'March' THEN 6
      WHEN filing_fy_month = 'April' THEN 7
      WHEN filing_fy_month = 'May' THEN 8
      WHEN filing_fy_month = 'June' THEN 9
      WHEN filing_fy_month = 'July' THEN 10
      WHEN filing_fy_month = 'August' THEN 11
      WHEN filing_fy_month = 'September' THEN 12
    END AS filing_fy_month_int,
    SUM(Fixed_Count) AS filed_classes_27,
    SUM(count) AS filed_cases_31
  FROM {trgt_catalog}.gold.filings_dashboard
  WHERE filing_fy = 2026
  GROUP BY filing_fy, filing_fy_month, filing_fy_quarter
),
prev_year_classes AS (
  SELECT
    CASE
      WHEN filing_fy_month = 'October' THEN 1
      WHEN filing_fy_month = 'November' THEN 2
      WHEN filing_fy_month = 'December' THEN 3
      WHEN filing_fy_month = 'January' THEN 4
      WHEN filing_fy_month = 'February' THEN 5
      WHEN filing_fy_month = 'March' THEN 6
      WHEN filing_fy_month = 'April' THEN 7
      WHEN filing_fy_month = 'May' THEN 8
      WHEN filing_fy_month = 'June' THEN 9
      WHEN filing_fy_month = 'July' THEN 10
      WHEN filing_fy_month = 'August' THEN 11
      WHEN filing_fy_month = 'September' THEN 12
    END AS filing_fy_month_int,
    filing_fy,
    SUM(Fixed_Count) AS prev_year_classes,
    SUM(count) AS prev_year_cases
  FROM {trgt_catalog}.gold.filings_dashboard
  WHERE filing_fy = 2025
  GROUP BY filing_fy, filing_fy_month
),
total_apps_filed_classes AS (
  SELECT
    mc.filing_fy,
    mc.filing_fy_month,
    mc.filing_fy_quarter,
    mc.filing_fy_month_int,
    mc.filed_classes_27,
    mc.filed_cases_31,
    pyc.prev_year_classes,
    CASE
      WHEN pyc.prev_year_classes IS NULL OR pyc.prev_year_classes = 0 THEN NULL
      ELSE ROUND((mc.filed_classes_27 - pyc.prev_year_classes) / pyc.prev_year_classes * 100, 2)
    END AS pct_growth_from_last_year_same_month
  FROM monthly_classes mc
  LEFT JOIN prev_year_classes pyc
    ON mc.filing_fy_month_int = pyc.filing_fy_month_int
),
total_apps_filed_classes_fy AS (
  SELECT
    filing_fy,
    SUM(Fixed_Count) AS filed_classes_fy_28a,
    SUM(count) AS filed_cases_fy_32a,
    ROUND(
      ((SUM(Fixed_Count) - NVL(LAG(SUM(Fixed_Count), 1) OVER (ORDER BY filing_fy ASC), 0)) /
      NULLIF(ABS(NVL(LAG(SUM(Fixed_Count), 1) OVER (ORDER BY filing_fy ASC), 0)), 0)) * 100, 1
    ) AS filed_classes_FYTD_growth_rate_29a,
    ROUND(
      ((SUM(count) - NVL(LAG(SUM(count), 1) OVER (ORDER BY filing_fy ASC), 0)) /
      NULLIF(ABS(NVL(LAG(SUM(count), 1) OVER (ORDER BY filing_fy ASC), 0)), 0)) * 100, 1
    ) AS filed_cases_FYTD_growth_rate_33a
  FROM {trgt_catalog}.gold.filings_dashboard
  WHERE filing_fy = 2026
  GROUP BY filing_fy
)
SELECT
  t.filing_fy,
  t.filing_fy_month,
  t.filing_fy_quarter,
  t.filing_fy_month_int,
  t.filed_classes_27 AS Total_Applications_Filed_classes,
  SUM(t.filed_classes_27) OVER (
    PARTITION BY t.filing_fy
    ORDER BY t.filing_fy_month_int
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Total_Applications_Filed_Classes_FY,
  SUM(p.prev_year_classes) OVER (
    PARTITION BY p.filing_fy
    ORDER BY p.filing_fy_month_int
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Total_Application_Filed_Classes,
  tafc.filed_classes_fy_28a AS Total_Applications_Filed_classes_fy_actual,
  CASE WHEN t.filing_fy <= 2024 THEN 740000 ELSE 860000 END AS Total_Applications_Filed_classes_fy_target,
  t.filed_cases_31 AS Total_Application_Files_filings_cases,
  SUM(t.filed_cases_31) OVER (
    PARTITION BY t.filing_fy
    ORDER BY t.filing_fy_month_int
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Total_Application_Files_filings_cases_fy,
  SUM(p.prev_year_cases) OVER (
    PARTITION BY p.filing_fy
    ORDER BY p.filing_fy_month_int
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS Total_Application_Files_filings_cases_py,
  tafc.filed_cases_fy_32a AS Total_Application_Files_filings_cases_fy_actual,
  CASE WHEN t.filing_fy <= 2024 THEN 544000 ELSE 642000 END AS Total_Application_Files_filings_cases_fy_target,
  CASE
    WHEN SUM(p.prev_year_classes) OVER (
      PARTITION BY p.filing_fy
      ORDER BY p.filing_fy_month_int
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) = 0 THEN NULL
    ELSE ROUND(
      (SUM(t.filed_classes_27) OVER (
        PARTITION BY t.filing_fy
        ORDER BY t.filing_fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) - SUM(p.prev_year_classes) OVER (
        PARTITION BY p.filing_fy
        ORDER BY p.filing_fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )) / SUM(p.prev_year_classes) OVER (
        PARTITION BY p.filing_fy
        ORDER BY p.filing_fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) * 100, 2
    )
  END AS filed_classes_FYTD_growth_rate,
  CASE
    WHEN SUM(p.prev_year_cases) OVER (
      PARTITION BY p.filing_fy
      ORDER BY p.filing_fy_month_int
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) = 0 THEN NULL
    ELSE ROUND(
      (SUM(t.filed_cases_31) OVER (
        PARTITION BY t.filing_fy
        ORDER BY t.filing_fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) - SUM(p.prev_year_cases) OVER (
        PARTITION BY p.filing_fy
        ORDER BY p.filing_fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )) / SUM(p.prev_year_cases) OVER (
        PARTITION BY p.filing_fy
        ORDER BY p.filing_fy_month_int
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) * 100, 2
    )
  END AS filed_cases_FYTD_growth_rate,
  CASE WHEN t.filing_fy <= 2024 THEN 0.4 ELSE 4.3 END AS filed_classes_FYTD_growth_rate_target,
  t.pct_growth_from_last_year_same_month AS filed_classes_month_growth_rate,
  ROUND(
    (
      t.filed_cases_31 - NVL(LAG(t.filed_cases_31, 1) OVER (ORDER BY t.filing_fy_month_int ASC), 0)
    ) / NULLIF(NVL(LAG(t.filed_cases_31, 1) OVER (ORDER BY t.filing_fy_month_int ASC), 0), 0) * 100
  , 1) AS filed_cases_month_growth_rate
FROM total_apps_filed_classes t
INNER JOIN total_apps_filed_classes_fy tafc
  ON t.filing_fy = tafc.filing_fy
INNER JOIN prev_year_classes p
  ON p.filing_fy_month_int = t.filing_fy_month_int
ORDER BY t.filing_fy ASC, t.filing_fy_month_int ASC
"""
#display(df_filing_dshb)

if full_refresh == 'N' and current_date_value == 1:#6: 
    df_filing_dshb = spark.sql(df_filing_dshb_query)
else:
    df_filing_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,Total_Applications_Filed_classes, Total_Applications_Filed_classes_fy, Total_Applications_Filed_classes_fy_actual, Total_Applications_Filed_classes_fy_target, Total_Application_Files_filings_cases, Total_Application_Files_filings_cases_fy, Total_Application_Files_filings_cases_fy_actual, Total_Application_Files_filings_cases_fy_target, filed_classes_FYTD_growth_rate, filed_classes_FYTD_growth_rate_target, filed_cases_FYTD_growth_rate, filed_classes_month_growth_rate, filed_cases_month_growth_rate
                                from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","filing_fy").withColumnRenamed("fy_month_int","filing_month_int").withColumnRenamed("fy_quarter","filing_fy_quarter").withColumnRenamed("fy_month","filing_fy_month")


# COMMAND ----------

#df_filing_dshb.display()

# COMMAND ----------

df_joined = df_filingBasis_dshb.join(
    df_filing_dshb,
    (df_filingBasis_dshb.year == df_filing_dshb.filing_fy) &
    (df_filingBasis_dshb.fy_quarter == df_filing_dshb.filing_fy_quarter) &
    (df_filingBasis_dshb.fy_month_int == df_filing_dshb.filing_fy_month_int),
    "left"
).drop(df_filing_dshb.filing_fy_quarter,df_filing_dshb.filing_fy_month,df_filing_dshb.filing_fy,df_filing_dshb.filing_fy_month_int)
display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC # **3. TMIIFY20 Dashboard:1**
# MAGIC #### (emailed reports from Jim, scheduled to run on 1st  or manual request if not recieved)

# COMMAND ----------

df_tmiify20_dshb_query = (f"""
WITH active_classes AS (
  SELECT fk_trademark_gid AS serial_num, COUNT(*) AS classes
  FROM {src_catalog}.bronze.TM_CLASS
  WHERE FK_TM_CLASS_STATUS_CD IN ('6', '8', 'P', 'W')
  GROUP BY fk_trademark_gid
),
raw AS (
  SELECT
    sbe.legacy_cm_ent_cd AS legacy_cm_ent_cd,
    be.cfk_object_gid AS serial_num,
    be.effective_ts,
    date_format(be.effective_ts, "MMMM") AS fy_month,
    date_format(be.effective_ts, "MMM") AS fy_month_fil,
    CASE WHEN month(be.effective_ts) >= 10 THEN year(be.effective_ts) + 1 ELSE year(be.effective_ts) END AS fy,
    CASE
      WHEN date_format(be.effective_ts, "MMMM") = 'October' THEN 1
      WHEN date_format(be.effective_ts, "MMMM") = 'November' THEN 2
      WHEN date_format(be.effective_ts, "MMMM") = 'December' THEN 3
      WHEN date_format(be.effective_ts, "MMMM") = 'January' THEN 4
      WHEN date_format(be.effective_ts, "MMMM") = 'February' THEN 5
      WHEN date_format(be.effective_ts, "MMMM") = 'March' THEN 6
      WHEN date_format(be.effective_ts, "MMMM") = 'April' THEN 7
      WHEN date_format(be.effective_ts, "MMMM") = 'May' THEN 8
      WHEN date_format(be.effective_ts, "MMMM") = 'June' THEN 9
      WHEN date_format(be.effective_ts, "MMMM") = 'July' THEN 10
      WHEN date_format(be.effective_ts, "MMMM") = 'August' THEN 11
      WHEN date_format(be.effective_ts, "MMMM") = 'September' THEN 12
    END AS fy_month_int,
    CASE WHEN sbe.legacy_cm_ent_cd = 'SUPC' THEN 1 ELSE 0 END AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE,
    CASE WHEN sbe.legacy_cm_ent_cd = 'EISU' THEN 1 ELSE 0 END AS TOTAL_STATEMENTS_OF_USE_FILED
  FROM {src_catalog}.bronze.business_event be
  INNER JOIN {src_catalog}.bronze.stnd_business_event_reason sbe
    ON be.fk_business_event_reason_id = sbe.business_event_reason_id
  WHERE sbe.legacy_cm_ent_cd IN ('EISU', 'SUPC')
    AND be.effective_ts >= to_date(concat(cast(year(current_date()) - 1 as string), '-10-01'))
    AND be.effective_ts <= last_day(to_date(concat(cast(year(current_date()) as string), '-12-01')))
),
joined AS (
  SELECT
    r.fy,
    r.fy_month,
    r.fy_month_fil,
    CASE
      WHEN r.fy_month_int IN (1, 2, 3) THEN 'Q1'
      WHEN r.fy_month_int IN (4, 5, 6) THEN 'Q2'
      WHEN r.fy_month_int IN (7, 8, 9) THEN 'Q3'
      WHEN r.fy_month_int IN (10, 11, 12) THEN 'Q4'
    END AS fy_quarter,
    r.fy_month_int,
    SUM(CASE WHEN r.TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE = 1 THEN a.classes ELSE 0 END) AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES,
    SUM(CASE WHEN r.TOTAL_STATEMENTS_OF_USE_FILED = 1 THEN a.classes ELSE 0 END) AS TOTAL_STATEMENTS_OF_USE_FILED_CLASSES,
    SUM(r.TOTAL_STATEMENTS_OF_USE_FILED) AS TOTAL_STATEMENTS_OF_USE_FILED,
    SUM(r.TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE) AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE
  FROM raw r
  INNER JOIN active_classes a ON r.serial_num = a.serial_num
  WHERE r.legacy_cm_ent_cd IN ('EISU', 'SUPC')
  GROUP BY r.fy, r.fy_month, r.fy_month_fil, r.fy_month_int
)
SELECT
  fy,
  fy_month,
  fy_month_fil,
  fy_quarter,
  fy_month_int,
  TOTAL_STATEMENTS_OF_USE_FILED_CLASSES,
  TOTAL_STATEMENTS_OF_USE_FILED,
  TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES,
  TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE,
  SUM(TOTAL_STATEMENTS_OF_USE_FILED_CLASSES) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy,
  CASE WHEN fy <= 2024 THEN 126826 ELSE 147500 END AS TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy_target,
  SUM(TOTAL_STATEMENTS_OF_USE_FILED) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS TOTAL_STATEMENTS_OF_USE_FILED_fy,
  CASE WHEN fy <= 2024 THEN 93300 ELSE 108500 END AS TOTAL_STATEMENTS_OF_USE_FILED_fy_target,
  SUM(TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy,
  CASE WHEN fy <= 2024 THEN 120500 ELSE 140000 END AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy_target,
  SUM(TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy,
  CASE WHEN fy <= 2024 THEN 98200 ELSE 105000 END AS TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy_target
FROM joined
WHERE fy = 2026
ORDER BY fy_month_int


""")
#display(df_tmiify20_dshb)

if full_refresh == 'N' and current_date_value == 1:#1:
    df_tmiify20_dshb = spark.sql(df_tmiify20_dshb_query)
else:
    df_tmiify20_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,TOTAL_STATEMENTS_OF_USE_FILED_CLASSES, TOTAL_STATEMENTS_OF_USE_FILED, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE, TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy, TOTAL_STATEMENTS_OF_USE_FILED_CLASSES_fy_target, TOTAL_STATEMENTS_OF_USE_FILED_fy, TOTAL_STATEMENTS_OF_USE_FILED_fy_target, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_CLASSES_fy_target, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy, TOTAL_STATEMENTS_OF_USE_PROCESSING_COMPLETE_fy_target
                                  from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#display(df_tmiify20_dshb)

# COMMAND ----------

df_joined = df_tmiify20_dshb.join(
    df_joined,
    (df_joined.year == df_tmiify20_dshb.fy) &
    (df_joined.fy_month_int == df_tmiify20_dshb.fy_month_int) &
    (df_joined.fy_quarter == df_tmiify20_dshb.fy_quarter),
    "right"
).drop(df_tmiify20_dshb.fy,df_tmiify20_dshb.fy_month,df_tmiify20_dshb.fy_quarter,df_tmiify20_dshb.fy_month_int,df_tmiify20_dshb.fy_month_fil,df_tmiify20_dshb.fy_month_fil)
#display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC # **4. EP11:2**
# MAGIC (EP11 2nd of month , refresh only previous month, rest stays static)
# MAGIC First Examination Processing:
# MAGIC First Actions - initial exam classes
# MAGIC Abandonment's - classes
# MAGIC Approved for Publication - classes
# MAGIC Total Balanced Disposals

# COMMAND ----------

df_ep11_dshb_query = (f"""
WITH base AS (
  SELECT
    fy,
    MIN(TRANSACTION_EFFECTIVE_DT) AS pp_start_date,
    MAX(TRANSACTION_EFFECTIVE_DT) AS pp_end_date,
    SUM(First_Actions_initial_exam_classes) AS First_Actions_initial_exam_classes,
    SUM(Abandonment_classes) AS Abandonment_classes,
    SUM(Approved_for_Publication_classes) AS Approved_for_Publication_classes,
    SUM(Total_Balanced_Disposals) AS Total_Balanced_Disposals
  FROM (
    SELECT 
      CASE WHEN month(TRANSACTION_EFFECTIVE_DT) >= 10 THEN year(TRANSACTION_EFFECTIVE_DT) + 1 ELSE year(TRANSACTION_EFFECTIVE_DT) END AS fy,
      TRANSACTION_EFFECTIVE_DT,
      WK_ACTV_CLS,
      (CASE WHEN TOT_FA_INIT_FY_CL != 0 THEN WK_ACTV_CLS ELSE 0 END) AS First_Actions_initial_exam_classes,
      (CASE WHEN ABAN_CT_FY != 0 THEN WK_ACTV_CLS ELSE 0 END) AS Abandonment_classes,
      (CASE WHEN APP_PUB_CT_FY != 0 THEN WK_ACTV_CLS ELSE 0 END) AS Approved_for_Publication_classes,
      Total_Balance_Disposals_Y AS Total_Balanced_Disposals
    FROM {trgt_catalog}.silver.epquery_stg3
    WHERE WORKER_NAME IS NOT NULL
  )
  WHERE fy = 2026
  GROUP BY fy, month(TRANSACTION_EFFECTIVE_DT)
)
, month_agg AS (
  SELECT
    fy,
    date_format(pp_start_date, "MMMM") AS fy_month,
    CASE
      WHEN date_format(pp_start_date, "MMMM") = 'October' THEN 1
      WHEN date_format(pp_start_date, "MMMM") = 'November' THEN 2
      WHEN date_format(pp_start_date, "MMMM") = 'December' THEN 3
      WHEN date_format(pp_start_date, "MMMM") = 'January' THEN 4
      WHEN date_format(pp_start_date, "MMMM") = 'February' THEN 5
      WHEN date_format(pp_start_date, "MMMM") = 'March' THEN 6
      WHEN date_format(pp_start_date, "MMMM") = 'April' THEN 7
      WHEN date_format(pp_start_date, "MMMM") = 'May' THEN 8
      WHEN date_format(pp_start_date, "MMMM") = 'June' THEN 9
      WHEN date_format(pp_start_date, "MMMM") = 'July' THEN 10
      WHEN date_format(pp_start_date, "MMMM") = 'August' THEN 11
      WHEN date_format(pp_start_date, "MMMM") = 'September' THEN 12
    END AS fy_month_int,
    CASE
      WHEN month(pp_start_date) IN (10, 11, 12) THEN 'Q1'
      WHEN month(pp_start_date) IN (1, 2, 3) THEN 'Q2'
      WHEN month(pp_start_date) IN (4, 5, 6) THEN 'Q3'
      WHEN month(pp_start_date) IN (7, 8, 9) THEN 'Q4'
    END AS fy_quarter,
    First_Actions_initial_exam_classes,
    Abandonment_classes,
    Approved_for_Publication_classes,
    Total_Balanced_Disposals
  FROM base
)
SELECT
  fy,
  fy_month,
  fy_month_int,
  fy_quarter,
  First_Actions_initial_exam_classes,
  Abandonment_classes,
  Approved_for_Publication_classes,
  Total_Balanced_Disposals,
  SUM(First_Actions_initial_exam_classes) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS First_Actions_initial_exam_classes_fy,
  CASE WHEN fy <= 2024 THEN 806000 ELSE 910000 END AS First_Actions_initial_exam_classes_fy_target,
  SUM(Abandonment_classes) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Abandonment_classes_fy,
  CASE WHEN fy <= 2024 THEN 130000 ELSE 165000 END AS Abandonment_classes_fy_target,
  SUM(Approved_for_Publication_classes) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Approved_for_Publication_classes_fy,
  CASE WHEN fy <= 2024 THEN 645000 ELSE 735000 END AS Approved_for_Publication_classes_fy_target,
  SUM(Total_Balanced_Disposals) OVER (PARTITION BY fy ORDER BY fy_month_int ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Total_Balanced_Disposals_fy,
  CASE WHEN fy <= 2024 THEN 1580000 ELSE 1810000 END AS Total_Balanced_Disposals_fy_target
FROM month_agg
ORDER BY fy, fy_month_int
                     """)

#display(df_ep11_dshb)

if full_refresh == 'N' and current_date_value == 1:#2:
    df_ep11_dshb = spark.sql(df_ep11_dshb_query)
else:
    df_ep11_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,First_Actions_initial_exam_classes, Abandonment_classes, Approved_for_Publication_classes, Total_Balanced_Disposals, First_Actions_initial_exam_classes_fy, First_Actions_initial_exam_classes_fy_target, Abandonment_classes_fy, Abandonment_classes_fy_target, Approved_for_Publication_classes_fy, Approved_for_Publication_classes_fy_target, Total_Balanced_Disposals_fy, Total_Balanced_Disposals_fy_target
                              from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","fy")

# COMMAND ----------

#display(df_ep11_dshb)

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
# MAGIC # **5. Quality Dashboard:1**
# MAGIC Refreshed only on 1st of the month

# COMMAND ----------

df_quality_dshb_query = (f"""
WITH base AS (
  SELECT 
    YEAR(DATEADD(month, 3, lastreviewdatetime)) AS review_fy,
    fy_quarter,
    fy_month,
    MONTH(DATEADD(month, 3, lastreviewdatetime)) AS review_month,
    SUM(CASE WHEN qualitymetricdeficientindicator = false OR qualitymetricdeficientindicator IS NULL THEN 1 ELSE 0 END) AS compliant_count,
    COUNT(*) AS total_count
  FROM {trgt_catalog}.gold.quality_dashboard
  WHERE review_type IN ('Final Action', 'PUB','SOU','First Action')
    AND YEAR(DATEADD(month, 3, lastreviewdatetime)) = 2026
  GROUP BY YEAR(DATEADD(month, 3, lastreviewdatetime)), fy_quarter, fy_month, MONTH(DATEADD(month, 3, lastreviewdatetime))
),
tc_month AS (
  SELECT
    FY,
    MonthINT,
    review_fy,
    fy_month,
    ROUND(
      (
        (
          SUM(CASE WHEN review_type = 'PUB' THEN 1 ELSE 0 END)
          -
          SUM(CASE WHEN review_type = 'PUB' AND qualitymetricdeficientindicator = true THEN 1 ELSE 0 END)
        )
        /
        NULLIF(SUM(CASE WHEN review_type = 'PUB' THEN 1 ELSE 0 END), 0)
      ) * 0.891
      +
      (
        (
          SUM(CASE WHEN review_type = 'Final Action' THEN 1 ELSE 0 END)
          -
          SUM(CASE WHEN review_type = 'Final Action' AND qualitymetricdeficientindicator = true THEN 1 ELSE 0 END)
        )
        /
        NULLIF(SUM(CASE WHEN review_type = 'Final Action' THEN 1 ELSE 0 END), 0)
      ) * 0.109
    , 3) AS final_compliance_rate,
    ROUND(
      (
        (
          SUM(CASE WHEN review_type = 'PUB' THEN 1 ELSE 0 END)
          -
          SUM(CASE WHEN review_type = 'PUB' AND qualitymetricdeficientindicator = true THEN 1 ELSE 0 END)
        )
        /
        NULLIF(SUM(CASE WHEN review_type = 'PUB' THEN 1 ELSE 0 END), 0)
      ) * 0.891
      +
      (
        (
          SUM(CASE WHEN review_type = 'Final Action' THEN 1 ELSE 0 END)
          -
          SUM(CASE WHEN review_type = 'Final Action' AND qualitymetricdeficientindicator = true THEN 1 ELSE 0 END)
        )
        /
        NULLIF(SUM(CASE WHEN review_type = 'Final Action' THEN 1 ELSE 0 END), 0)
      ) * 0.109
    , 3) AS weightedrunning
  FROM (
    SELECT
      CASE WHEN MONTH(lastreviewdatetime) >= 10 THEN YEAR(lastreviewdatetime) + 1 ELSE YEAR(lastreviewdatetime) END AS FY,
      CASE WHEN MONTH(lastreviewdatetime) >= 10 THEN MONTH(lastreviewdatetime) - 9 ELSE MONTH(lastreviewdatetime) + 3 END AS MonthINT,
      review_type,
      fy_quarter,
      fy_month,
      YEAR(DATEADD(month, 3, lastreviewdatetime)) AS review_fy,
      MONTH(DATEADD(month, 3, lastreviewdatetime)) AS review_month,
      qualitymetricdeficientindicator
    FROM {trgt_catalog}.gold.quality_dashboard
    WHERE review_type IN ('Final Action', 'PUB')
  ) base
  WHERE FY = 2026
  GROUP BY FY, MonthINT, review_fy, fy_month
  ORDER BY MonthINT
),
tc_month_with_total AS (
  SELECT t.*, r.Total_compliance_rate_fy
  FROM tc_month t
  LEFT JOIN (
    WITH base AS (
      SELECT
        CASE 
          WHEN MONTH(lastreviewdatetime) >= 10 THEN YEAR(lastreviewdatetime) + 1 
          ELSE YEAR(lastreviewdatetime) 
        END AS FY,
        CASE 
          WHEN MONTH(lastreviewdatetime) >= 10 THEN MONTH(lastreviewdatetime) - 9 
          ELSE MONTH(lastreviewdatetime) + 3 
        END AS MonthINT,
        review_type,
        qualitymetricdeficientindicator
      FROM {trgt_catalog}.gold.quality_dashboard
      WHERE review_type IN ('Final Action', 'PUB')
    ),
    agg AS (
      SELECT
        FY,
        MonthINT,
        SUM(CASE WHEN review_type = 'PUB' THEN 1 ELSE 0  END ) AS PubCount,
        SUM(CASE WHEN review_type = 'Final Action' THEN 1 ELSE 0  END ) AS FACount,
        SUM(CASE WHEN review_type = 'PUB' THEN CAST(qualitymetricdeficientindicator AS INT) ELSE 0 END) AS PubDef,
        SUM(CASE WHEN review_type = 'Final Action' THEN CAST(qualitymetricdeficientindicator AS INT) ELSE 0 END) AS FADef
      FROM base
      WHERE FY = 2026
      GROUP BY FY, MonthINT
    ),
    running AS (
      SELECT
        FY,
        MonthINT,
        SUM(PubCount) OVER (PARTITION BY FY ORDER BY MonthINT ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rPubCount,
        SUM(PubDef) OVER (PARTITION BY FY ORDER BY MonthINT ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rPubDef,
        SUM(FACount) OVER (PARTITION BY FY ORDER BY MonthINT ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rFACount,
        SUM(FADef) OVER (PARTITION BY FY ORDER BY MonthINT ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rFADef
      FROM agg
    )
    SELECT 
      FY,
      MonthINT,
      (ROUND((rPubCount - rPubDef) / NULLIF(rPubCount, 0), 3) * 0.891
      +
      ROUND ((rFACount - rFADef) / NULLIF(rFACount, 0), 3) * .109) AS Total_compliance_rate_fy
    FROM running
    WHERE FY = 2026
  ) r
  ON t.FY = r.FY AND t.MonthINT = r.MonthINT
),
tc_fy AS (
  SELECT 
    YEAR(DATEADD(month, 3, lastreviewdatetime)) AS review_fy,
    ROUND(
      SUM(CASE WHEN review_type IN ('Final Action', 'PUB') AND (qualitymetricdeficientindicator = false OR qualitymetricdeficientindicator IS NULL) THEN 1 ELSE 0 END) * 100.0
      / NULLIF(SUM(CASE WHEN review_type IN ('Final Action', 'PUB') THEN 1 ELSE 0 END), 0), 1
    ) AS final_compliance_rate_fy
  FROM {trgt_catalog}.gold.quality_dashboard
  WHERE YEAR(DATEADD(month, 3, lastreviewdatetime)) = 2026
  GROUP BY YEAR(DATEADD(month, 3, lastreviewdatetime))
),
qc_fy AS (
  SELECT 
    YEAR(DATEADD(month, 3, lastreviewdatetime)) AS review_fy,
    ROUND(SUM(CASE WHEN qualitymetricdeficientindicator = false OR qualitymetricdeficientindicator IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS First_Action_Compliance_Rate_fy
  FROM {trgt_catalog}.gold.quality_dashboard
  WHERE review_type = 'First Action'
    AND YEAR(DATEADD(month, 3, lastreviewdatetime)) = 2026
  GROUP BY YEAR(DATEADD(month, 3, lastreviewdatetime))
),
qc_fy_base AS (
  SELECT 
    YEAR(DATEADD(month, 3, lastreviewdatetime)) AS review_fy,
    MONTH(DATEADD(month, 3, lastreviewdatetime)) AS review_month,
    SUM(CASE WHEN qualitymetricdeficientindicator = false OR qualitymetricdeficientindicator IS NULL THEN 1 ELSE 0 END) AS compliant_count,
    COUNT(*) AS total_count
  FROM {trgt_catalog}.gold.quality_dashboard
  WHERE review_type = 'First Action'
    AND YEAR(DATEADD(month, 3, lastreviewdatetime)) = 2026
  GROUP BY YEAR(DATEADD(month, 3, lastreviewdatetime)), MONTH(DATEADD(month, 3, lastreviewdatetime))
),
qc_fy_running AS (
  SELECT
    review_fy,
    review_month,
    compliant_count,
    total_count,
    ROUND(compliant_count * 100.0 / total_count, 1) AS First_Action_Compliance_Rate,
    AVG(ROUND(compliant_count * 100.0 / total_count, 1)) 
      OVER (
        ORDER BY review_month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS First_Action_Compliance_Rate_fy
  FROM qc_fy_base
),
exc_month AS (
  SELECT 
    YEAR(DATEADD(month, 3, lastreviewdatetime)) AS review_fy,
    fy_quarter,
    fy_month,
    MONTH(DATEADD(month, 3, lastreviewdatetime)) AS review_month,
    ROUND(
      SUM(CASE WHEN overallexcellentindicator = true THEN 1 ELSE 0 END) 
      / COUNT(*) * 100, 1
    ) AS Exceptional_First_Action_Rate
  FROM {trgt_catalog}.gold.quality_dashboard
  WHERE review_type = 'Final Action'
    AND YEAR(DATEADD(month, 3, lastreviewdatetime)) = 2026
  GROUP BY YEAR(DATEADD(month, 3, lastreviewdatetime)), fy_quarter, fy_month, MONTH(DATEADD(month, 3, lastreviewdatetime))
),
exc_fy AS (
  SELECT 
    YEAR(DATEADD(month, 3, lastreviewdatetime)) AS review_fy,
    ROUND(
      SUM(CASE WHEN overallexcellentindicator = true THEN 1 ELSE 0 END) 
      / COUNT(*) * 100, 1
    ) AS Exceptional_First_Action_Rate_fy
  FROM {trgt_catalog}.gold.quality_dashboard
  WHERE review_type = 'Final Action'
    AND YEAR(DATEADD(month, 3, lastreviewdatetime)) = 2026
  GROUP BY YEAR(DATEADD(month, 3, lastreviewdatetime))
)
SELECT 
  b.review_fy,
  b.fy_quarter,
  b.fy_month,
  b.review_month,
  qc_fy_running.First_Action_Compliance_Rate,
  qc_fy_running.First_Action_Compliance_Rate_fy,
  95.5 AS First_Action_Compliance_Rate_target,
  tc_month_with_total.final_compliance_rate,
  tc_month_with_total.Total_compliance_rate_fy,
  tc_fy.final_compliance_rate_fy,
  97.0 AS Total_Compliance_Rate_target,
  exc_month.Exceptional_First_Action_Rate,
  exc_fy.Exceptional_First_Action_Rate_fy,
  50.0 AS Exceptional_First_Action_Rate_target
FROM base b
LEFT JOIN qc_fy ON b.review_fy = qc_fy.review_fy
LEFT JOIN qc_fy_running ON b.review_fy = qc_fy_running.review_fy AND b.review_month = qc_fy_running.review_month
LEFT JOIN tc_month_with_total ON b.review_fy = tc_month_with_total.FY AND b.review_month = tc_month_with_total.MonthINT
LEFT JOIN tc_fy ON b.review_fy = tc_fy.review_fy
LEFT JOIN exc_month ON b.review_fy = exc_month.review_fy AND b.fy_month = exc_month.fy_month
LEFT JOIN exc_fy ON b.review_fy = exc_fy.review_fy
ORDER BY b.review_fy, b.review_month""")



if  full_refresh == 'N' and current_date_value == 1:#1: 
    df_quality_dshb = spark.sql(df_quality_dshb_query)
else:
    #df_quality_dshb = spark.createDataFrame([], schema=spark.sql(df_quality_dshb_query).schema)
    df_quality_dshb = spark.sql(f""" select year, fy_quarter, fy_month, fy_month_int,First_Action_Compliance_Rate, First_Action_Compliance_Rate_fy, First_Action_Compliance_Rate_target, final_compliance_rate,Total_compliance_rate_fy, final_compliance_rate_fy, Final_Compliance_Rate_target, Exceptional_First_Action_Rate, Exceptional_First_Action_Rate_fy, Exceptional_First_Action_Rate_target
                                 from {trgt_catalog}.gold.process_production_staffing_report 
                                 where year = '{current_fy}' and lower(fy_month) = lower('{current_month}')""").withColumnRenamed("year","review_fy").withColumnRenamed("fy_month_int","review_month")
from pyspark.sql.functions import col, round, format_number
df_quality_dshb = df_quality_dshb \
    .withColumn("final_compliance_rate", format_number(col("final_compliance_rate") * 100, 1)) \
    .withColumn("First_Action_Compliance_Rate_fy", format_number(round(col("First_Action_Compliance_Rate_fy"), 2), 1)) \
    .withColumn("Total_compliance_rate_fy", format_number(col("Total_compliance_rate_fy") * 100, 1)) \
    .withColumn("First_Action_Compliance_Rate_fy", format_number(round(col("First_Action_Compliance_Rate_fy"), 2), 2))

# COMMAND ----------

#display(df_quality_dshb)

# COMMAND ----------

df_joined = df_quality_dshb.join(
    df_joined,
    (df_joined.year == df_quality_dshb.review_fy) &
    (df_joined.fy_month_int == df_quality_dshb.review_month) &
    (df_joined.fy_quarter == df_quality_dshb.fy_quarter),
    "right"
).drop(df_quality_dshb.fy_quarter,df_quality_dshb.fy_month,df_quality_dshb.review_month,df_quality_dshb.review_fy)
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

#df_final_full.display()

# COMMAND ----------

if full_refresh == 'N' :
    #df_final_full.write.mode("overwrite").option("mergeSchema", "true").format("delta").insertInto(f"{trgt_catalog}.gold.process_production_staffing_report")
#else:
    df_final_full.createOrReplaceTempView("temp_merge")
    spark.sql(f"""MERGE INTO {trgt_catalog}.gold.process_production_staffing_report_rolling AS target
        USING temp_merge AS source
        ON target.year = source.year
        and target.fy_month_int = source.fy_month_int
        AND target.fy_month = source.fy_month
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_reporting.gold.process_production_staffing_report_rolling

# COMMAND ----------

# MAGIC %md
# MAGIC if full_refresh == 'N':
# MAGIC     df_final_full.createOrReplaceTempView("temp_merge")
# MAGIC     target_columns = [col for col in df_final_full.columns]
# MAGIC     update_set = ", ".join([f"{col} = source.{col}" for col in target_columns])
# MAGIC     insert_cols = ", ".join(target_columns)
# MAGIC     insert_vals = ", ".join([f"source.{col}" for col in target_columns])
# MAGIC     spark.sql(f"""
# MAGIC         MERGE INTO {trgt_catalog}.gold.process_production_staffing_report_rolling AS target
# MAGIC         USING temp_merge AS source
# MAGIC         ON target.year = source.year
# MAGIC         AND target.fy_month_int = source.fy_month_int
# MAGIC         AND target.fy_month = source.fy_month
# MAGIC         WHEN MATCHED THEN
# MAGIC           UPDATE SET {update_set}
# MAGIC         WHEN NOT MATCHED THEN
# MAGIC           INSERT ({insert_cols}) VALUES ({insert_vals})
# MAGIC     """)
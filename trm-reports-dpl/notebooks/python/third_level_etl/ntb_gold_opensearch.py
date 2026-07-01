# Databricks notebook source
# DBTITLE 1,Get Parameters
# add text parameter default to dev
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.dropdown("index", "trademark_applications", ["trademark_applications", "tqr"])
idx = dbutils.widgets.get("index")

# COMMAND ----------

# DBTITLE 1,Configuration
import yaml
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
if dbx_env =='qa':
    dbx_env = 'test'
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Run Common Functions and Parameters Notebook
# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
bus_catalog  = common_configs['schema']['trm_tmbuscalendar_catalog']
worker_catalog = common_configs['schema']['trm_tmworker_catalog']

# COMMAND ----------

# DBTITLE 1,Job Time Control
import pytz
from pytz import timezone

job_name = f"ntb_gold_opensearch_load_{idx}"
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

database = 'gold'
spark.conf.set('conf.trgt_catalog',  trgt_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.dbx_env', dbx_env)
spark.conf.set('conf.idx', idx)


# COMMAND ----------

# DBTITLE 1,Retrieves Latest Day in Gold Table
max_calendar_day = spark.sql(f"""Select  nvl(max(calendar_day),to_date('01-01-1900','MM-dd-yyyy')) as max_calendar_day from {trgt_catalog}.gold.pea_worker_performance""").collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Calculate Sundays and Holidays for Pendency
from datetime import timedelta
import holidays

def count_holidays_and_sundays(start_date, end_date, country_code='US'):
    """Counts the number of holidays and Sundays between two dates."""

    holiday_count = 0
    sunday_count = 0

    # Get holidays for the specified country
    country_holidays = holidays.country_holidays(country_code)

    current_date = start_date
    while current_date <= end_date:
        # Check if it's a Sunday
        if current_date.weekday() == 6:
            sunday_count += 1

        # Check if it's a holiday
        if current_date in country_holidays:
            holiday_count += 1

        current_date += timedelta(days=1)

    return holiday_count + sunday_count

# COMMAND ----------

# DBTITLE 1,Get Oldest application in unassigned status
#Calculation for pendency takes the app with the oldest pre_exam received date in an unassigned status and excludes apps without history order
#oldest app and dt teas
teasP = spark.sql(f"""SELECT coalesce((SELECT CAST(min(pre_exam_received_ts) AS DATE) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type IN ('TEASE', 'TEASP', 'APPB') AND pre_exam_status = '100' AND tm_app NOT LIKE '%-1'), current_date)""").collect()[0][0]
oldest_serial_teas = spark.sql(f"""SELECT coalesce((SELECT ser_num FROM {trgt_catalog}.silver.pea_trademark_applications WHERE pre_exam_status ='100' AND submission_type IN ('TEASE', 'TEASP', 'APPB') AND tm_app NOT LIKE '%-1' ORDER BY pre_exam_received_ts LIMIT 1), 'None')""").collect()[0][0]
#oldest app and dt madrd
madrdP = spark.sql(f"""SELECT coalesce((SELECT CAST(min(pre_exam_received_ts) AS DATE) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type = 'MADRD' AND pre_exam_status = '100' AND tm_app NOT LIKE '%-1'), current_date)""").collect()[0][0]
oldest_serial_madrd = spark.sql(f"""SELECT coalesce((SELECT ser_num FROM {trgt_catalog}.silver.pea_trademark_applications WHERE pre_exam_status ='100' AND submission_type = 'MADRD' AND tm_app NOT LIKE '%-1' ORDER BY pre_exam_received_ts LIMIT 1), 'None')""").collect()[0][0]
#oldest app and dt paper
paperP = spark.sql(f"""SELECT coalesce((SELECT CAST(min(pre_exam_received_ts) AS DATE) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type = 'PAPER' AND pre_exam_status = '100' AND tm_app NOT LIKE '%-1'), current_date)""").collect()[0][0]
oldest_serial_paper = spark.sql(f"""SELECT coalesce((SELECT ser_num FROM {trgt_catalog}.silver.pea_trademark_applications WHERE pre_exam_status ='100' AND submission_type = 'PAPER' AND tm_app NOT LIKE '%-1' ORDER BY pre_exam_received_ts LIMIT 1), 'None')""").collect()[0][0]
#oldest app and dt overall
oldest_serial = spark.sql(f"""SELECT coalesce((SELECT ser_num FROM {trgt_catalog}.silver.pea_trademark_applications WHERE pre_exam_status ='100' AND tm_app NOT LIKE '%-1' ORDER BY pre_exam_received_ts LIMIT 1), 'None')""").collect()[0][0]
oldest_filing_date = spark.sql(f"""SELECT coalesce((SELECT CAST(min(pre_exam_received_ts) AS DATE) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE pre_exam_status = '100' AND tm_app NOT LIKE '%-1'), current_date)""").collect()[0][0]


# COMMAND ----------

# DBTITLE 1,Calculate Total Days for Pendency
from datetime import date
today = date.today()
total_days_teas = abs(teasP - today).days
total_days_madrd = abs(madrdP - today).days
total_days_paper = abs(paperP -today).days

# COMMAND ----------

# DBTITLE 1,Calculate Pendency
teas_pendency = total_days_teas - count_holidays_and_sundays(teasP, today)
madrd_pendency = total_days_madrd - count_holidays_and_sundays(madrdP, today)
paper_pendency = total_days_paper - count_holidays_and_sundays(paperP, today)

# COMMAND ----------

df_gold = spark.sql(f"""
SELECT 
    main.work_day AS calendar_day, 
    main.PP AS pay_period,
    main.biweek_start AS pp_start_date,
    main.biweek_ending AS pp_end_date,
    main.assignee,
    CASE WHEN main.assignee = '30078' THEN 'Auto-Processor'
         WHEN main.assignee ilike 'c%'THEN 'Contractor'
         WHEN main.assignee = 'None' THEN 'Unassigned' 
         WHEN main.assignee is null THEN 'Unassigned' ELSE worker_nm END AS worker_nm,
    ---------------------------------------------------------Daily Counts from Counts subquery by assignee and workday----------------------------------------------------------------------      
    --teas 
    coalesce(counts.Teas_P_sum, 0) AS daily_teas_processed,
    coalesce(counts.Teas_A_sum, 0) AS daily_teas_assigned,
    --madrid
    coalesce(counts.Madrd_P_sum, 0) AS daily_madrd_processed,
    coalesce(counts.Madrd_A_sum, 0) AS daily_madrd_assigned,
     --paper
    coalesce(counts.Paper_P_sum, 0) AS daily_paper_processed,
    coalesce(counts.Paper_A_sum, 0) AS daily_paper_assigned,
    ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --TQR Daily Counts

    coalesce(Assigned_Review_sum, 0) AS tqr_assigned_review,
    coalesce(Errors_sum, 0) AS tqr_errors,
    coalesce(Review_Completed_sum, 0) AS tqr_review_completed,
    coalesce(Review_Completed_After_Correction_sum, 0) AS tqr_review_completed_after_correction,
    coalesce(Advisories_No_Action_sum, 0) AS tqr_advisories_no_action,
    coalesce(Advisories_Action_Needed_sum, 0) AS tqr_advisories_action_needed,

    ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --total teas processed as of calendar day that exists in db by assignee
    --This will provide total processed and assigned to date, for an individual inventory calculation
    (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications ta_cor
     WHERE submission_type IN ('TEASE', 'TEASP', 'APPB') AND history_to IN ('103', '103a')
     AND history_ts <= main.work_day AND main.assignee = ta_cor.assignee) AS teas_inventory_processed,
     --total teas processed as of calendar day that exists in db by assignee
     (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications ta_cor
      WHERE submission_type IN ('TEASE', 'TEASP', 'APPB') AND history_action = 'assigned' AND ta_cor.pre_exam_status NOT IN ('102a', '102b', '110a', '110b')
      AND history_ts <= main.work_day AND main.assignee = ta_cor.assignee) AS teas_inventory_assigned,
      teas_inventory_assigned - teas_inventory_processed AS teas_todate_inventory,

    --Madrid
    (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications ta_cor
     WHERE submission_type = 'MADRD' AND history_to IN ('103', '103a')
     AND history_ts <= main.work_day AND main.assignee = ta_cor.assignee) AS madrd_inventory_processed,
     --total madrd processed as of calendar day that exists in db by assignee
     (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications ta_cor
      WHERE submission_type = 'MADRD' AND history_action = 'assigned' AND ta_cor.pre_exam_status NOT IN ('102a', '102b', '110a', '110b')
      AND history_ts <= main.work_day AND main.assignee = ta_cor.assignee) AS madrd_inventory_assigned,
      madrd_inventory_assigned - madrd_inventory_processed AS madrd_todate_inventory,

    --Paper
    (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications ta_cor
     WHERE submission_type = 'PAPER' AND history_to IN ('103', '103a')
     AND history_ts <= main.work_day AND main.assignee = ta_cor.assignee) AS paper_inventory_processed,
     (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications ta_cor
      WHERE submission_type = 'PAPER' AND history_action = 'assigned' AND ta_cor.pre_exam_status NOT IN ('102a', '102b', '110a', '110b')
      AND history_ts <= main.work_day AND main.assignee = ta_cor.assignee) AS paper_inventory_assigned,
      paper_inventory_assigned - paper_inventory_processed AS paper_todate_inventory,
    
    teas_todate_inventory + madrd_todate_inventory + paper_todate_inventory AS daily_inventory,
    ------------------------------------------------------------------------------------------------------------------------------------------------------
    --Overall inventory on a specific day counts, takes into account carryover from day of receipt to day of submission
    --The count of total apps in system minus the number that have gone to a completed status
    --102a, 110a and 110b are marked as containing informal so removed from total inventory
    --be careful of null pre_exam_received_date, this field is null as this column was instituted later in development used or filing_ts in logic to account
    --teas
    (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type IN ('TEASE', 'TEASP', 'APPB') AND pre_exam_status NOT IN ('000','102a', '110a', '110b') AND CASE WHEN pre_exam_received_ts IS NULL THEN CAST(filing_ts AS DATE) ELSE CAST(pre_exam_received_ts AS DATE) END <= main.work_day)
     -
      (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type IN ('TEASE', 'TEASP', 'APPB') AND (history_to IN ('103a', '103') OR history_from = '103') AND history_ts <= main.work_day) AS teas_overall_inventory,
    --madrd
    (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type = 'MADRD' AND pre_exam_status NOT IN ('000', '102a', '110a', '110b') AND CASE WHEN pre_exam_received_ts IS NULL THEN CAST(filing_ts AS DATE) ELSE CAST(pre_exam_received_ts AS DATE) END <= main.work_day) -

      (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type = 'MADRD' AND (history_to IN ('103a', '103') OR history_from = '103') AND history_ts <= main.work_day) AS madrd_overall_inventory,
    --paper
    (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type = 'PAPER' AND pre_exam_status NOT IN ('000', '102a', '110a', '110b') AND CAST(pre_exam_received_ts AS DATE) <= main.work_day) -
      (SELECT COUNT(DISTINCT ser_num) FROM {trgt_catalog}.silver.pea_trademark_applications WHERE submission_type = 'PAPER' AND (history_to IN ('103a', '103') OR history_from = '103') AND history_ts <= main.work_day) AS paper_overall_inventory,

    teas_overall_inventory + madrd_overall_inventory + paper_overall_inventory AS total_inventory,
    ------------------------------------------------------------------------------------------------------------------------------------------------------

    --oldest serial and pre_exam received date that is uncompleted in the dataset for each group from above query
    ------------------------------------------------------------------------------------------------------------------------------------------------------
    '{oldest_serial_teas}' AS oldest_serial_teas,
    '{teasP}' AS oldest_filing_date_teas,
    '{oldest_serial_madrd}' AS oldest_serial_madrd,
    '{madrdP}' AS oldest_filing_date_madrd,
    '{oldest_serial_paper}' AS oldest_serial_paper,
    '{paperP}' AS oldest_filing_date_paper,
    '{oldest_serial}' AS oldest_serial,
    '{oldest_filing_date}' AS oldest_filing_date,
    '{teas_pendency}' AS teas_pendency,
    '{madrd_pendency}' AS madrd_pendency,
    '{paper_pendency}' AS paper_pendency 
    ------------------------------------------------------------------------------------------------------------------------------------------------------

FROM
(SELECT DISTINCT 
    CAST(cal.CALENDAR_DT AS DATE) AS work_day,
    'PP #' || calr.RANGE_NM || ' from ' || to_char((FK_START_CALENDAR_DT),'MMM dd,yyyy') || ' thru ' || to_char((FK_end_CALENDAR_DT),'MMM dd,yyyy') PP,
    CAST(FK_end_CALENDAR_DT AS DATE) AS biweek_ending,
    CAST(FK_START_CALENDAR_DT AS DATE) AS biweek_start,
    ta.assignee
FROM {bus_catalog}.bronze.business_calendar_day cal
LEFT JOIN {bus_catalog}.bronze.business_calendar_range calr ON CAST(cal.CALENDAR_DT AS DATE) BETWEEN calr.fk_start_calendar_dt AND calr.fk_end_calendar_dt
CROSS JOIN (SELECT DISTINCT CASE WHEN assignee IS NULL THEN 'None' ELSE assignee END AS assignee 
    FROM {trgt_catalog}.silver.pea_trademark_applications ta) ta
WHERE cal.CALENDAR_DT >= add_months(current_date(), -36)
AND CAST(cal.CALENDAR_DT AS DATE) <= current_date()  
and CAST(CALENDAR_DT AS DATE) >= '{max_calendar_day}'----process data starting from the max date in the gold table (including that day's data as we start load at 4am)
) main
--worker
LEFT JOIN {worker_catalog}.bronze.worker w ON w.worker_no = main.assignee
----------------------------------------------------------------------------------------------------------------------------------------------------------
--Daily Counts for number of processed and assigned each day by assignee, this shows assignees daily changes in workload
LEFT JOIN (SELECT sub.Day_counted,
    sub.assignee,
    SUM(sub.Teas_P) AS Teas_P_sum,
    SUM(sub.Teas_A) AS Teas_A_sum,
    SUM(sub.Paper_P) AS Paper_P_sum,
    SUM(sub.Paper_A) AS Paper_A_sum,
    SUM(sub.Madrd_P) AS Madrd_P_sum,
    SUM(sub.Madrd_A) AS Madrd_A_sum

    FROM (SELECT CAST(ta.history_ts AS DATE) AS Day_Counted, ta.assignee, 
            --history status to 103 is completed status and 103a is fast tracked completed by auto processor
            --autoprocessor gets two status changes to 103a, one from 101 and one from 630 which resulted in multiple counts unless filtered down to from 101
            CASE WHEN ta.submission_type IN ('TEASE', 'TEASP', 'APPB') AND ta.history_to IN ('103', '103a') AND ta.history_from = '101' THEN 1 ELSE 0 END AS Teas_P,
            --history status to 101 is assigned
            CASE WHEN ta.submission_type IN ('TEASE', 'TEASP', 'APPB') AND ta.history_to = '101' THEN 1 ELSE 0 END AS Teas_A,
            CASE WHEN ta.submission_type = 'PAPER' AND ta.history_to = '103' THEN 1 ELSE 0 END AS Paper_P,
            CASE WHEN ta.submission_type = 'PAPER' AND ta.history_to = '101' THEN 1 ELSE 0 END AS Paper_A,
            CASE WHEN ta.submission_type = 'MADRD' AND ta.history_to = '103' AND ta.history_from = '101' THEN 1 ELSE 0 END AS Madrd_P,
            CASE WHEN ta.submission_type = 'MADRD' AND ta.history_to = '101' THEN 1 ELSE 0 END AS Madrd_A

        FROM {trgt_catalog}.silver.pea_trademark_applications ta
        --since this is for daily production do not need unassigned apps which would have a null assignee
        where ta.history_ts is not null AND ta.assignee IS NOT NULL
        and date_trunc('DD', ta.history_ts) >= '{max_calendar_day}'--process data starting from the max date in the gold table (including that day's data as we start load at 4am)
        ) sub
    GROUP BY sub.Day_Counted, sub.assignee
)counts 

ON counts.Day_Counted = main.work_day AND counts.assignee = main.assignee

--Separate TQR Counts from TA counts due to using different date column for logic and the need for distinct due to duplicates
LEFT JOIN (SELECT sub.Day_counted,
    sub.assignee,
    SUM(sub.Assigned_Review) AS Assigned_Review_sum,
    SUM(sub.Errors) AS Errors_sum,
    SUM(sub.Review_Completed) AS Review_Completed_sum,
    SUM(sub.Review_Completed_After_Correction) AS Review_Completed_After_Correction_sum,
    SUM(sub.Advisories_No_Action) AS Advisories_No_Action_sum,
    SUM(sub.Advisories_Action_Needed) AS Advisories_Action_Needed_sum

    FROM (SELECT DISTINCT CAST(ta.history_ts AS DATE) AS Day_Counted, ta.assignee, ser_num,
            --TQR Numbers
            CASE WHEN ta.history_to = '111a' THEN 1 ELSE 0 END AS Assigned_Review,
            CASE WHEN ta.history_to IN ('111b', '115') THEN 1 ELSE 0 END AS Errors,
            CASE WHEN ta.history_to IN ('112a', '112b') THEN 1 ELSE 0 END AS Review_Completed,
            CASE WHEN ta.history_to = '112b' THEN 1 ELSE 0 END AS Review_Completed_After_Correction,
            CASE WHEN ta.history_to = '112d' THEN 1 ELSE 0 END AS Advisories_No_Action,
            CASE WHEN ta.history_to = '112e' THEN 1 ELSE 0 END AS Advisories_Action_Needed
        FROM {trgt_catalog}.silver.pea_trademark_applications ta
        --since this is for daily production do not need unassigned apps which would have a null assignee
        where ta.last_uploaded_ts is not null AND ta.assignee IS NOT NULL
        and date_trunc('DD', ta.history_ts) >= '{max_calendar_day}'--process data starting from the max date in the gold table (including that day's data as we start load at 4am)
        ) sub
    GROUP BY sub.Day_Counted, sub.assignee
)tqr_counts 

ON tqr_counts.Day_Counted = main.work_day AND tqr_counts.assignee = main.assignee
""")


# COMMAND ----------

recs_count = df_gold.count()

df_gold.createOrReplaceTempView("os_gold_temp")

# COMMAND ----------

# DBTITLE 1,Merge--Insert only to avoid duplicate data load due to multiple runs in same day
if recs_count > 0:
        try:
            spark.sql(f"""MERGE INTO {trgt_catalog}.{database}.pea_worker_performance AS trgt
            USING os_gold_temp src
            ON trgt.assignee = src.assignee 
            and trgt.calendar_day = src.calendar_day
            WHEN MATCHED THEN UPDATE SET --UPDATE ONLY LAST DAYS DATA
            TRGT.assignee=SRC.assignee,
            TRGT.worker_nm = SRC.worker_nm,
            TRGT.calendar_day = SRC.calendar_day,
            TRGT.pay_period = SRC.pay_period,
            TRGT.pp_start_date = SRC.pp_start_date,
            TRGT.pp_end_date = SRC.pp_end_date,
            TRGT.daily_teas_processed = SRC.daily_teas_processed,
            TRGT.daily_teas_assigned = SRC.daily_teas_assigned,
            TRGT.daily_madrd_processed = SRC.daily_madrd_processed,
            TRGT.daily_madrd_assigned = SRC.daily_madrd_assigned,
            TRGT.daily_paper_processed = SRC.daily_paper_processed,
            TRGT.daily_paper_assigned = SRC.daily_paper_assigned,
            TRGT.teas_inventory_processed = SRC.teas_inventory_processed,
            TRGT.teas_inventory_assigned = SRC.teas_inventory_assigned,
            TRGT.teas_todate_inventory = SRC.teas_todate_inventory,
            TRGT.madrd_inventory_processed = SRC.madrd_inventory_processed,
            TRGT.madrd_inventory_assigned = SRC.madrd_inventory_assigned,
            TRGT.madrd_todate_inventory = SRC.madrd_todate_inventory,
            TRGT.paper_inventory_processed = SRC.paper_inventory_processed,
            TRGT.paper_inventory_assigned = SRC.paper_inventory_assigned,
            TRGT.paper_todate_inventory = SRC.paper_todate_inventory,
            TRGT.daily_inventory = SRC.daily_inventory,
            TRGT.teas_overall_inventory = SRC.teas_overall_inventory,
            TRGT.madrd_overall_inventory = SRC.madrd_overall_inventory,
            TRGT.paper_overall_inventory = SRC.paper_overall_inventory,
            TRGT.total_inventory = SRC.total_inventory,
            TRGT.oldest_serial_teas = SRC.oldest_serial_teas,
            TRGT.oldest_filing_date_teas = SRC.oldest_filing_date_teas,
            TRGT.oldest_serial_madrd = SRC.oldest_serial_madrd,
            TRGT.oldest_filing_date_madrd = SRC.oldest_filing_date_madrd,
            TRGT.oldest_serial_paper = SRC.oldest_serial_paper,
            TRGT.oldest_filing_date_paper = SRC.oldest_filing_date_paper,
            TRGT.oldest_serial = SRC.oldest_serial,
            TRGT.oldest_filing_date = SRC.oldest_filing_date,
            TRGT.teas_pendency = SRC.teas_pendency,
            TRGT.madrd_pendency = SRC.madrd_pendency,
            TRGT.paper_pendency = SRC.paper_pendency,
            TRGT.tqr_assigned_review = SRC.tqr_assigned_review,
            TRGT.tqr_errors = SRC.tqr_errors,
            TRGT.tqr_review_completed = SRC.tqr_review_completed,
            TRGT.tqr_review_completed_after_correction = SRC.tqr_review_completed_after_correction,
            TRGT.tqr_advisories_no_action = SRC.tqr_advisories_no_action,
            TRGT.tqr_advisories_action_needed = SRC.tqr_advisories_action_needed,
            TRGT.last_mod_ts = current_timestamp()

            WHEN NOT MATCHED THEN 
                INSERT (assignee,worker_nm,calendar_day,pay_period,pp_start_date,pp_end_date,daily_teas_processed,daily_teas_assigned,daily_madrd_processed,daily_madrd_assigned,daily_paper_processed,daily_paper_assigned,teas_inventory_processed,teas_inventory_assigned,teas_todate_inventory,madrd_inventory_processed,madrd_inventory_assigned,madrd_todate_inventory,paper_inventory_processed,paper_inventory_assigned,paper_todate_inventory,daily_inventory,teas_overall_inventory,madrd_overall_inventory,paper_overall_inventory,total_inventory,oldest_serial_teas,oldest_filing_date_teas,oldest_serial_madrd,oldest_filing_date_madrd,oldest_serial_paper,oldest_filing_date_paper,oldest_serial,oldest_filing_date,teas_pendency,madrd_pendency,paper_pendency,tqr_assigned_review,tqr_errors,tqr_review_completed,tqr_review_completed_after_correction,tqr_advisories_no_action,tqr_advisories_action_needed,create_ts,create_user_id,last_mod_ts,last_mod_user_id)
                
                VALUES (assignee,worker_nm,calendar_day,pay_period,pp_start_date,pp_end_date,daily_teas_processed,daily_teas_assigned,daily_madrd_processed,daily_madrd_assigned,daily_paper_processed,daily_paper_assigned,teas_inventory_processed,teas_inventory_assigned,teas_todate_inventory,madrd_inventory_processed,madrd_inventory_assigned,madrd_todate_inventory,paper_inventory_processed,paper_inventory_assigned,paper_todate_inventory,daily_inventory,teas_overall_inventory,madrd_overall_inventory,paper_overall_inventory,total_inventory,oldest_serial_teas,oldest_filing_date_teas,oldest_serial_madrd,oldest_filing_date_madrd,oldest_serial_paper,oldest_filing_date_paper,oldest_serial,oldest_filing_date,teas_pendency,madrd_pendency,paper_pendency,tqr_assigned_review,tqr_errors,tqr_review_completed,tqr_review_completed_after_correction,tqr_advisories_no_action,tqr_advisories_action_needed,current_timestamp(),'etl', current_timestamp(),'etl')
            """)
            
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'completed',recs_count,"job completed successfully")

        except Exception as e:
            print("Exception message: {}".format(e))
            end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,e)
            raise
else:
    end_job_cntl(f"{trgt_catalog}.silver", job_name, start_ts,'completed',recs_count,"job completed successfully")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Unit test cells below

# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Metadata
# MAGIC
# MAGIC **Created by:** Drew McPherson  
# MAGIC **Created on:** 2026-01-21   
# MAGIC **Last updated by:** Drew McPherson  
# MAGIC **Last updated on:** 2026-01-21  
# MAGIC
# MAGIC ## Changelog
# MAGIC - **2026-01-20 (Drew McPherson):** Initial table creation.

# COMMAND ----------

# DBTITLE 1,Load Libraries
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, DateType, LongType

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set parameter values
common_configs = read_yaml(config_file)
tmngpdb_src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
reporting_catalog = common_configs["schema"]["reporting_catalog"]
trgt_catalog = common_configs["schema"]["trgt_catalog"]

# COMMAND ----------

print(f"{tmngpdb_src_catalog=},{reporting_catalog=},{trgt_catalog=}")

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = "grand_model"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Process Records

# COMMAND ----------

# DBTITLE 1,Create Case Dictionary
# The purpose of this step is to bring all the needed case-level information into the Milestone dataset (the "case_dictionary"). This creates a series of case records with enough date and characteristic information to enable downstream transformations. 

case_dictionary_df = spark.sql(f"""
-- This step pulls NWOS and DOCK dates onto a dataframe that is organized by serial number.
WITH pcp_ph AS (
  SELECT
    ph.serial_number,
    min(
      case
        when ph.ph_action_code = 'NWOS' then ph_action_date
      end) as nwos_dt,
    min(
      case
        when ph.ph_action_code = 'DOCK' then ph_action_date
      end) as dock_dt
  FROM
    `{reporting_catalog}`.silver.prosecution_history ph
  GROUP BY ph.serial_number
),

pcp_apps AS (
-- This step pulls information about pre-exam status. It is arranged to provide the latest update by serial number.
SELECT 
  ser_num, 
  pre_exam_status,
  pre_exam_received_ts, 
  history_order,
  assignee, 
  latest_order_no
FROM (
  SELECT
    ser_num,
    pre_exam_status,
    pre_exam_received_ts,
    latest_order_no,
    assignee,
    history_order,
    ROW_NUMBER() OVER (
      PARTITION BY ser_num
      ORDER BY history_order DESC
    ) AS rn
  FROM `{reporting_catalog}`.silver.pea_trademark_applications
)
WHERE rn = 1
),

pex_dates AS (
-- This provides the date when a case exits pre-exam, assuming it has exited. This approach assumes that a case should be available in the pullable case pool as soon as a status of 103 or 103a was logged. Also kept a condition to log if the case moves from 103, since the history_to 103 logging event does not always occur. Also included "42" as a history_to value since some autoprocessor cases do not log a 103 or 103a, but will log a 42 instead.
SELECT 
  ser_num,
  MIN(
    CASE 
      WHEN
        history_to IN ('103', '103a', '42')
        THEN DATE(history_ts)
      WHEN 
        history_from IN ('103')
        THEN DATE(history_ts)
      END)
      AS pex_exit_dt,
  MIN(
    CASE WHEN
      history_order = '0'
      THEN DATE(history_ts)
      END)
      AS pex_first_action_dt
FROM `{reporting_catalog}`.silver.pea_trademark_applications
GROUP BY ser_num
)

SELECT 
  ms.ser_num AS ser_num,
  ms.pendency_cal_start_dt AS pendency_cal_start_dt,
  ms.filing_dt AS filing_dt,
  DATE(ap.pre_exam_received_ts) AS pre_exam_received,

-- The field pre_exam_received_ts only goes back to April 2024. To address this, we will impute pre-exam receival of a case based on the first entry in the pre-exam log. Unfortunately, this only goes back to July 2023. We will therefore impute the filing date as the pre-exam starting point as a last resort. 
  CASE 
    WHEN ap.pre_exam_received_ts IS NOT NULL
      THEN DATE(ap.pre_exam_received_ts)
    WHEN ap.pre_exam_received_ts IS NULL
      AND pe.pex_first_action_dt IS NOT NULL
      THEN DATE(pe.pex_first_action_dt)
    ELSE ms.filing_dt
    END AS pex_intake_dt,

-- Similar to the above. silver.pea_trademark_application only begins logging cases in July 2023. Older cases will not have a pre-exam entry even if they moved forward and received an NWOS. Therefore, it is necessary to use the impute the nwos_dt for these older cases. This prevents these cases as appearing to be stuck in pre-exam.
  CASE 
    WHEN
      pe.pex_exit_dt IS NOT NULL
      THEN pe.pex_exit_dt
    WHEN pe.pex_exit_dt IS NULL
      AND ph.nwos_dt IS NOT NULL
      THEN ph.nwos_dt
    END AS pex_exit_dt,
  ph.nwos_dt AS nwos_dt,
  ph.dock_dt AS dock_dt,
  
  -- Case location is determined by assessing whether a case is in between certain phase gates by date.
  CASE 
    WHEN 
      ms.filing_dt IS NOT NULL
      AND pe.pex_first_action_dt IS NULL
      AND ap.pre_exam_received_ts IS NULL 
      AND pe.pex_exit_dt IS NULL 
      AND ph.nwos_dt IS NULL
      AND ms.dock_dt IS NULL
    THEN TRUE
    ELSE FALSE
    END AS in_intake,
  CASE 
    WHEN
        (ap.pre_exam_received_ts IS NOT NULL 
        OR pe.pex_first_action_dt IS NOT NULL)
      AND pe.pex_exit_dt IS NULL 
      AND ph.nwos_dt IS NULL
      AND ms.dock_dt IS NULL
    THEN TRUE
    ELSE FALSE
    END AS in_pex,
  CASE 
    WHEN
      pe.pex_exit_dt IS NOT NULL
      AND ms.dock_dt IS NULL
      AND ph.nwos_dt IS NULL 
    THEN TRUE
    ELSE FALSE
    END AS in_post_pex,
  CASE 
    WHEN
      ph.nwos_dt IS NOT NULL
      AND ms.dock_dt IS NULL
    THEN TRUE
    ELSE FALSE
    END AS in_pcp,
  CASE 
    WHEN
      ms.dock_dt IS NOT NULL
    THEN TRUE
    ELSE FALSE
  END AS in_exam,

-- Days in status-phase is calculated by measuring the cases' distance between a status-phase entry date and the status-phase exit date (or the current date)
  CASE 
    WHEN 
      ms.filing_dt IS NOT NULL
      AND ap.pre_exam_received_ts IS NOT NULL 
      THEN date_diff(day, ms.filing_dt, ap.pre_exam_received_ts) 
    WHEN 
      ms.filing_dt IS NOT NULL
      AND ap.pre_exam_received_ts IS NULL 
      AND pe.pex_first_action_dt IS NOT NULL
      THEN date_diff(day, ms.filing_dt, pe.pex_first_action_dt) 
    WHEN
      ms.filing_dt IS NOT NULL
      AND ap.pre_exam_received_ts IS NULL
      AND pe.pex_first_action_dt IS NULL
      THEN date_diff(day, ms.filing_dt, CURRENT_DATE) 
    ELSE NULL
    END AS days_in_intake,
  CASE 
    WHEN 
      ap.pre_exam_received_ts IS NOT NULL
      AND pe.pex_exit_dt IS NOT NULL 
      THEN date_diff(day, ap.pre_exam_received_ts, pe.pex_exit_dt) 
    WHEN 
      pe.pex_first_action_dt IS NOT NULL
      AND pe.pex_exit_dt IS NOT NULL 
      THEN date_diff(day, pe.pex_first_action_dt, pe.pex_exit_dt)     
    WHEN
      ap.pre_exam_received_ts IS NOT NULL
      AND pe.pex_exit_dt IS NULL 
      THEN date_diff(day, ap.pre_exam_received_ts, CURRENT_DATE) 
    WHEN
      pe.pex_first_action_dt IS NOT NULL
      AND pe.pex_exit_dt IS NULL 
      THEN date_diff(day, pe.pex_first_action_dt, CURRENT_DATE) 
    ELSE NULL
    END AS days_in_pex,
  CASE 
    WHEN 
      pe.pex_exit_dt IS NOT NULL
      AND ph.nwos_dt IS NOT NULL 
      AND ph.nwos_dt >= pe.pex_exit_dt
      THEN date_diff(day, pe.pex_exit_dt, ph.nwos_dt) 
    WHEN
      pe.pex_exit_dt IS NOT NULL
      AND ph.nwos_dt IS NULL 
      AND CURRENT_DATE >= pe.pex_exit_dt
      THEN date_diff(day, pe.pex_exit_dt, CURRENT_DATE)    
    ELSE NULL
    END AS days_in_post_pex,
  CASE 
    WHEN 
      ph.nwos_dt IS NOT NULL
      AND ms.dock_dt IS NOT NULL 
      THEN date_diff(day, ph.nwos_dt, ms.dock_dt) 
    WHEN
      ph.nwos_dt IS NOT NULL
      AND ms.dock_dt IS NULL 
      THEN date_diff(day, ph.nwos_dt, CURRENT_DATE) 
    ELSE NULL
    END AS days_in_pcp,

-- The information below is not strickly necessary, but is included as it may help identify issues with status-phase location
  bb.filing_method_filed AS filing_method_filed,
  ap.pre_exam_status AS pre_exam_status,
  bb.am_stat AS am_stat,
  bb.status_dt AS status_dt,
  oh.ath_active_status AS ath_active_status,
  oh.ath_hold_docket AS ath_hold_docket,
  oh.ath_hold_status AS ath_hold_status,
  oh.ath_last_upd_dt AS ath_last_upd_dt,
  ms.disposal_dt AS disposal_dt,
  ms.disposal_type AS disposal_type,
  ma.dead_mark_in AS dead_mark_in
FROM `{reporting_catalog}`.silver.milestone ms
LEFT JOIN `{tmngpdb_src_catalog}`.bronze.mv_myuspto_trm_mark ma
  ON ms.ser_num = ma.ser_num
LEFT JOIN pcp_ph ph
  ON ms.ser_num = ph.serial_number
LEFT JOIN pcp_apps ap
  ON ms.ser_num = ap.ser_num
LEFT JOIN `{reporting_catalog}`.silver.bibliography bb
  ON ms.ser_num = bb.ser_num
LEFT JOIN `{reporting_catalog}`.silver.on_hold oh
  ON ms.ser_num = oh.ath_ser_num
LEFT JOIN pex_dates pe
  ON ms.ser_num = pe.ser_num
WHERE ath_active_status IS NULL
  AND ath_hold_docket IS NULL
  AND filing_dt >= to_date('2023-10-01', 'yyyy-MM-dd')
  """)
case_dictionary_df.createOrReplaceTempView("case_dictionary")

# COMMAND ----------

# DBTITLE 1,Create a Daily Calendar
# This step creates a generic blank calendar that other dataframes will use to align their daily counts. 

calendar_df = spark.sql("""
SELECT explode(
    sequence(
      to_date('2023-10-01'),
      current_date(),
      interval 1 day
    )
  ) AS calendar_day
""")
calendar_df.createOrReplaceTempView("calendar")

# COMMAND ----------

# DBTITLE 1,Worker Performance
# The purpose of this step is two-fold. Firstly, it is capturing the "flow" within pre-exam by determining daily production. Secondly, it is establishing performance expectations for staff by looking at pre-exam team member performance. Pre-exam "core team" are defined as any employee who performs above average in a given month amongst employees who processed at least 500 cases. This methodology is based on the historical performance of identified pre-exam team members. 

workers_df = spark.sql(f"""
WITH pex_preliminary_performance AS (

-- This step provides monthly performance of people who worked more than 500 cases in a month.
SELECT 
  year(calendar_day) AS action_year,
  month(calendar_day) AS action_month,
  assignee,
  SUM(daily_teas_processed + daily_madrd_processed + daily_paper_processed) AS cases_processed
FROM `{reporting_catalog}`.gold.pea_worker_performance
WHERE assignee != '30078'
GROUP BY ALL
HAVING SUM(daily_teas_processed + daily_madrd_processed + daily_paper_processed) >= 500
ORDER by year(calendar_day) DESC, month(calendar_day) DESC
),

monthly_average AS (

-- This step assesses average overall monthly performance of people who worked more than 500 cases in a month.
  SELECT
  action_year, 
  action_month,
  ROUND(SUM(cases_processed) / COUNT(cases_processed),0) AS adj_avg_cases_per_human
FROM pex_preliminary_performance
GROUP BY ALL
ORDER by action_year DESC, action_month DESC
),

pex_individual_performance AS(

-- This step provides a dataframe with overall individual monthly performance.
SELECT 
  year(calendar_day) AS action_year,
  month(calendar_day) AS action_month,
  assignee,
  SUM(daily_teas_processed + daily_madrd_processed + daily_paper_processed) AS cases_processed
FROM `{reporting_catalog}`.gold.pea_worker_performance
GROUP BY ALL
ORDER by year(calendar_day) DESC, month(calendar_day) DESC
),

-- This step assigns team status based on whether an individual processed more than the monthly average of pre-exam cases.
team_status AS (
SELECT 
  i.*, 
  m.adj_avg_cases_per_human,
  CASE
    WHEN assignee = '30078'
      THEN 'AUTOPROCESSOR'
    WHEN cases_processed > m.adj_avg_cases_per_human
      THEN 'CORE TEAM'
    ELSE 'SUPPORT TEAM'
    END AS team_status
FROM pex_individual_performance i
LEFT JOIN monthly_average m
ON i.action_year = m.action_year AND i.action_month = m.action_month
ORDER by action_year DESC, action_month DESC
)

-- This step breaks down daily performance, number of individuals, and average production for "Core Team", "Support Team", and Autoprocessor.
SELECT
  calendar_day,
  SUM(daily_teas_processed + daily_madrd_processed + daily_paper_processed) AS cases_processed,
  SUM(
    CASE WHEN team_status = 'AUTOPROCESSOR'
      THEN daily_teas_processed + daily_madrd_processed + daily_paper_processed
      ELSE 0
    END
  ) AS autoprocessor_cases,
  SUM(
    CASE WHEN team_status = 'CORE TEAM'
      THEN daily_teas_processed + daily_madrd_processed + daily_paper_processed
      ELSE 0
    END
  ) AS core_team_cases,
    SUM(
    CASE WHEN team_status = 'SUPPORT TEAM'
      THEN daily_teas_processed + daily_madrd_processed + daily_paper_processed
      ELSE 0
    END
  ) AS support_team_cases,
  COUNT(DISTINCT CASE WHEN team_status = 'CORE TEAM' 
    AND daily_teas_processed + daily_madrd_processed + daily_paper_processed > 0
    THEN p.assignee END) AS core_team,
  COUNT(DISTINCT CASE WHEN team_status = 'SUPPORT TEAM' 
    AND daily_teas_processed + daily_madrd_processed + daily_paper_processed > 0
    THEN p.assignee END) AS support_team,
  ROUND(CASE 
    WHEN COUNT(DISTINCT CASE 
        WHEN team_status = 'CORE TEAM' 
        AND daily_teas_processed + daily_madrd_processed + daily_paper_processed > 0
      THEN p.assignee END) > 0
    THEN 
      SUM(
        CASE WHEN team_status = 'CORE TEAM'
          THEN daily_teas_processed + daily_madrd_processed + daily_paper_processed
          ELSE 0
        END
      ) / 
      COUNT(DISTINCT CASE WHEN team_status = 'CORE TEAM' 
        AND daily_teas_processed + daily_madrd_processed + daily_paper_processed > 0
        THEN p.assignee END)
    ELSE NULL
  END, 0)
  AS avg_cases_core_team,
    ROUND(CASE 
    WHEN COUNT(DISTINCT CASE 
        WHEN team_status = 'SUPPORT TEAM' 
        AND daily_teas_processed + daily_madrd_processed + daily_paper_processed > 0
      THEN p.assignee END) > 0
    THEN 
      SUM(
        CASE WHEN team_status = 'SUPPORT TEAM'
          THEN daily_teas_processed + daily_madrd_processed + daily_paper_processed
          ELSE 0
        END
      ) / 
      COUNT(DISTINCT CASE WHEN team_status = 'SUPPORT TEAM' 
        AND daily_teas_processed + daily_madrd_processed + daily_paper_processed > 0
        THEN p.assignee END)
    ELSE NULL
  END, 0)
  AS avg_cases_support_team
FROM `{reporting_catalog}`.gold.pea_worker_performance p
LEFT JOIN team_status s
ON 
  YEAR(p.calendar_day) = action_year 
  AND MONTH(p.calendar_day) = action_month 
  AND p.assignee = s.assignee
WHERE calendar_day > '2023-09-30'
GROUP BY calendar_day
ORDER BY calendar_day DESC
""")
workers_df.createOrReplaceTempView("workers_df")

# COMMAND ----------

# DBTITLE 1,Intake Daily Inventory
  # This step creates a daily inventory of cases than are in intake before being by pre-exam. It joins the case_dictionary (modified milestone) with the calendar, only joining records when date fields meet certain criteria. On a given date, it is counting the number of unique serial numbers that have a filing_dt but have not received a pre_exam_recieved_ts. 
  # The massive spike in counts between December 2023 and August 2024 is a data phenomenon rather than a business phenomenon. It is related to the creation of the pre_exam_received_ts being established on 2024-04-01. AA large number of cases received their pre_exam_received_ts in that period, and others had their "pex_intake_dt" shifted forward, creating the perception of a bulge which is really related to a shift in time tracking.

intake_time_series_df = spark.sql("""
SELECT
  c.calendar_day AS calendar_day,
  COUNT(DISTINCT ita.ser_num) AS intake_cases
FROM calendar c
LEFT JOIN case_dictionary ita
  ON c.calendar_day >= ita.filing_dt
  AND (c.calendar_day <= ita.pex_intake_dt OR ita.pex_intake_dt IS NULL)
  AND (c.calendar_day < ita.disposal_dt OR ita.disposal_dt IS NULL)
GROUP BY calendar_day
ORDER BY calendar_day
""")

# COMMAND ----------

# DBTITLE 1,Pre Exam Daily Inventory
  # This step creates a daily inventory of cases than are in Pre-Exam. It joins the case_dictionary (modified milestone) with the calendar, only joining records when date fields meet certain criteria. On a given date, it is counting the number of unique serial numbers that have a pre-exam entry date but have not received a pre-exam exit date.

pex_time_series_df = spark.sql("""
SELECT
  c.calendar_day AS calendar_day,
  COUNT(DISTINCT pex.ser_num) AS pex_cases
FROM calendar c
LEFT JOIN case_dictionary pex
  ON c.calendar_day >= pex.pex_intake_dt
  AND (c.calendar_day < pex.pex_exit_dt OR pex.pex_exit_dt IS NULL)
  AND (c.calendar_day < pex.disposal_dt OR pex.disposal_dt IS NULL)
GROUP BY calendar_day
ORDER BY calendar_day
""")
pex_time_series_df.createOrReplaceTempView("pex_time_series")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Post-Pex Daily Inventory
# This step creates a daily inventory of post-pex cases. These are cases that may be stuck after being processed by pre-exam but without receiving a NWOS that enables exam to process them. It joins the case_dictionary (modified milestone) with the calendar, only joining records when date fields meet certain criteria. On a given date, it is counting the number of unique serial numbers that have a pex_exit_dt but not a NWOS Date. 

post_pex_time_series_df = spark.sql("""
SELECT
  c.calendar_day AS calendar_day,
  COUNT(DISTINCT pos.ser_num) AS post_pex_cases
FROM calendar c
LEFT JOIN case_dictionary pos
  ON c.calendar_day >= pos.pex_exit_dt
  AND (c.calendar_day < pos.nwos_dt OR pos.nwos_dt IS NULL)
  AND (c.calendar_day < pos.disposal_dt OR pos.disposal_dt IS NULL)
GROUP BY calendar_day
ORDER BY calendar_day
""")
post_pex_time_series_df.createOrReplaceTempView("post_pex_time_series")

# COMMAND ----------

# DBTITLE 1,Pullable Case Pool Daily Inventory
# This step creates a daily inventory of the pullable case pool. joins the case_dictionary (modified milestone) with the calendar, only joining records when date fields meet certain criteria. On a given date, it is counting the number of unique serial numbers that have an NWOS date but not a Dock Date. 

pcp_time_series_df = spark.sql("""
SELECT
  c.calendar_day AS calendar_day,
  COUNT(DISTINCT pcp.ser_num) AS pcp_cases
FROM calendar c
LEFT JOIN case_dictionary pcp
  ON c.calendar_day >= pcp.nwos_dt
  AND (c.calendar_day < pcp.dock_dt OR pcp.dock_dt IS NULL)
  AND (c.calendar_day < pcp.disposal_dt OR pcp.disposal_dt IS NULL)
GROUP BY calendar_day
ORDER BY calendar_day
""")
pcp_time_series_df.createOrReplaceTempView("pcp_time_series")

# COMMAND ----------

# DBTITLE 1,United Daily Counts
from pyspark.sql import functions as F

# Create DataFrames for each of the subqueries
new_filings_df = case_dictionary_df.filter(F.col("filing_dt").isNotNull()) \
    .groupBy("filing_dt") \
    .agg(F.countDistinct("ser_num").alias("new_filings")) \
    .withColumnRenamed("filing_dt", "calendar_day")

pex_intake_df = case_dictionary_df.filter(F.col("pex_intake_dt").isNotNull()) \
    .groupBy("pex_intake_dt") \
    .agg(F.countDistinct("ser_num").alias("pex_intake")) \
    .withColumnRenamed("pex_intake_dt", "calendar_day")

post_pex_intake_df = case_dictionary_df.filter(F.col("pex_exit_dt").isNotNull()) \
    .filter(F.col("nwos_dt").isNull()) \
    .groupBy("pex_exit_dt") \
    .agg(F.countDistinct("ser_num").alias("daily_post_pex")) \
    .withColumnRenamed("pex_exit_dt", "calendar_day")

pcp_intake_df = case_dictionary_df.filter(F.col("nwos_dt").isNotNull()) \
    .groupBy("nwos_dt") \
    .agg(F.countDistinct("ser_num").alias("daily_pcp_intake")) \
    .withColumnRenamed("nwos_dt", "calendar_day")

docked_df = case_dictionary_df.filter(F.col("dock_dt").isNotNull()) \
    .groupBy("dock_dt") \
    .agg(F.countDistinct("ser_num").alias("docked_daily")) \
    .withColumnRenamed("dock_dt", "calendar_day")

# Join the DataFrames
historical_df = calendar_df.join(new_filings_df, on="calendar_day", how="left") \
    .join(pex_intake_df, on="calendar_day", how="left") \
    .join(post_pex_intake_df, on="calendar_day", how="left") \
    .join(pcp_intake_df, on="calendar_day", how="left") \
    .join(docked_df, on="calendar_day", how="left") \
    .join(workers_df, on="calendar_day", how="left") \
    .join(intake_time_series_df, on="calendar_day", how="left") \
    .join(pex_time_series_df, on="calendar_day", how="left") \
    .join(post_pex_time_series_df, on="calendar_day", how="left") \
    .join(pcp_time_series_df, on="calendar_day", how="left") \
    .filter(F.col("calendar_day") >= "2024-10-01") \
    .orderBy("calendar_day")

# Select the desired columns
historical_df = historical_df.select(
    F.col("calendar_day").alias("calendar_day"),
    F.col("new_filings").alias("daily_filings"),
    F.col("intake_cases").alias("pre_pex_cases"),
    F.col("pex_intake").alias("daily_pex_intake"),
    F.col("pex_cases").alias("pex_cases"),
    F.col("post_pex_cases").alias("post_pex_cases"),
    F.col("daily_pcp_intake").alias("daily_pcp_intake"),
    F.col("pcp_cases").alias("pullable_cases"),
    F.col("docked_daily").alias("daily_docked"),
    F.col("cases_processed").alias("pex_daily_cases_processed"),
    F.col("autoprocessor_cases").alias("pex_autoprocessor_cases"),
    F.col("core_team_cases").alias("pex_core_team_cases"),
    F.col("core_team").alias("pex_core_team"),
    F.col("avg_cases_core_team").alias("pex_avg_cases_core_team"),
    F.col("support_team_cases").alias("pex_support_team_cases"),
    F.col("support_team").alias("pex_support_team"),
    F.col("avg_cases_support_team").alias("pex_avg_cases_support_team")
)

# COMMAND ----------

# DBTITLE 1,Add Timestamps
historical_df = (
    historical_df.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("ETL"))
)
display(historical_df)

# COMMAND ----------

# DBTITLE 1,Write To Table
target_table_name = f"{trgt_catalog}.gold.grand_model_pre_exam"
historical_df.write.mode("overwrite").format("delta").insertInto(target_table_name)

# COMMAND ----------

# DBTITLE 1,End Job Control
recs_count = historical_df.count()

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    recs_count,
    "job completed successfully",
)
dbutils.notebook.exit(f"Completed Loading on_hold Table")

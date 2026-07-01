# Databricks notebook source
# DBTITLE 1,Imports
from io import BytesIO
import pandas as pd
from pyspark.sql.functions import col, countDistinct, datediff, expr, when, sum, round, min, ntile, max
from pyspark.sql import Window
import smtplib

from email import encoders
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Import Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
edw_scope = common_configs["secrets"]["edw_scope"]
altrx_schema = common_configs['schema']['altrx_schema']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
primary_email, cc_email = common_configs["alerting"]["currently_processing_first_actions_with_controls"]["email"], common_configs["alerting"]["currently_processing_first_actions_with_controls"]["cc"]
print(reporting_catalog, tmngpdb_catalog, edw_scope, primary_email, cc_email)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_currently_processing_first_actions_with_controls"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

input_129 = spark.sql(f"""
    select 
        ser_num,
        filing_dt,
        pendency_cal_start_dt,
        dock_dt,
        filing_fy
    from 
        {reporting_catalog}.silver.milestone
    where 
        pendency_cal_start_dt > '2018-09-30' 
        and IsNotNull(dock_dt) 
""")

# COMMAND ----------

input_131 = spark.sql(f"""
    select 
        ath_ser_num
    from 
        {reporting_catalog}.silver.on_hold
""")
select_151 = input_131

# COMMAND ----------

select_153 = input_132 = spark.sql(f"""
select 
  ser_num
from 
  {reporting_catalog}.silver.divisionals
""")

# COMMAND ----------

select_152 = input_133 = spark.sql(f"""
    select 
    ser_num,
    filing_basis_fil
    from 
    {reporting_catalog}.silver.bibliography
    where 
    FILING_BASIS_FIL='MADRID'
""")
filter_51 = select_152

# COMMAND ----------

join_148 = input_129.alias("left").join(
    other=select_151.alias("right"),
    on=[col("left.ser_num") == col("right.ath_ser_num")],
    how="leftanti",
)

# COMMAND ----------

join_49 = (
    join_148.alias("left")
    .join(
        other=select_153.alias("right"),
        on=[col("left.ser_num") == col("right.ser_num")],
        how="leftanti",
    )
)

# COMMAND ----------

join_52 = (
    join_49.alias("left")
    .join(
        other=filter_51.alias("right"),
        on=[col("left.ser_num") == col("right.ser_num")],
        how="leftanti",
    )
)

# COMMAND ----------

filter_50 = join_52.where("pendency_cal_start_dt is not null")

# COMMAND ----------

filter_37 = filter_50.where("""
    datediff(current_date, dock_dt) <= 7
    and datediff(current_date, dock_dt) >= 0
""")

# COMMAND ----------

summarize_39 = filter_37.groupBy("pendency_cal_start_dt").agg(countDistinct("ser_num").alias("cases"))

# COMMAND ----------

sort_53 = summarize_39.orderBy("pendency_cal_start_dt")

# COMMAND ----------

summarize_57 = sort_53.groupBy().sum("cases").withColumnRenamed("sum(cases)", "sum_cases")

# COMMAND ----------

append_58 = sort_53.join(summarize_57)

# COMMAND ----------

formula_59 = append_58.withColumn(
    "percent_total",
    round((col("cases") / col("sum_cases")) * 100, 2).cast("decimal(10,2)"),
)

# COMMAND ----------

record_id_74 = formula_59.select(
    [expr("row_number() over (order by pendency_cal_start_dt) as record_id"), "*"]
)

# COMMAND ----------

multirow_formula_73 = record_id_74.withColumn(
    "test",
    min("cases").over(
        Window.orderBy("record_id").rowsBetween(Window.currentRow, Window.currentRow + 3)
    )
    > 100,
)

# COMMAND ----------

filter_76 = multirow_formula_73.where("test = 1")

# COMMAND ----------

summarize_77 = (
    filter_76.groupBy()
    .min("record_id")
    .withColumnRenamed("min(record_id)", "min_record_id")
)

# COMMAND ----------

join_78 = (
    filter_76.alias("left")
    .join(
        other=summarize_77.alias("right"),
        on=[col("left.record_id") == col("right.min_record_id")],
        how="inner",
    )
)

# COMMAND ----------

formula_79 = join_78.select(
    [
        expr("* except(pendency_cal_start_dt)"),
        expr("date_add(pendency_cal_start_dt, 7) as date_plus_two"),
        expr("current_date as today"),
        expr(
            "round(date_diff(current_date, pendency_cal_start_dt) / 30.42, 2) as current_process_pendency"
        ),
        expr("date_add(pendency_cal_start_dt, -7) as pendency_cal_start_dt"),
    ]
)

# COMMAND ----------

datetime_80 = formula_79.select(
    [
        "*",
        expr("date_format(pendency_cal_start_dt, 'MMMM dd, yyyy') as datetime_out1"),
    ]
)

# COMMAND ----------

datetime_81 = datetime_80.select(
    [expr("*"), expr("date_format(date_plus_two, 'MMMM dd, yyyy') as datetime_out2")]
)

# COMMAND ----------

display(datetime_81)

# COMMAND ----------

if datetime_81.count() > 0:
    dates = [
        f"{row.datetime_out1} - {row.datetime_out2}"
        for row in datetime_81.select("datetime_out1", "datetime_out2").collect()
    ][-1]
else:
    print("Data is empty. No dates calculated.")
    dates = 'No dates calculated.'

report_text_84 = datetime_81.withColumn(
    "text",
    lit(
f"""
Currently Processing:
{dates}





Logic
-Filter all cases docketed in the last 7 days
-Remove divisionals, transformations, if ever on hold, and Madrid
-Select oldest filing date that has 4 consecutive following days with > 100 docketed cases
-Subtract 7 days and add 7 days from selected date for a range
-Run every Friday

Controls
-Selected filing start date must be after most recent previous run filing start date
-Remove on-hold and madrid, currently processing start date pendency must be within 1 month pendency of the last 15 days middle bin pendency
-If any of the controls fail, the algorithm is shutdown, does not update anything, and sends error message to trademark analytics
"""
    ),
)
formula_87 = report_text_84.select(
    [
        "*",
        expr("concat('TM Currently Processing as of ', current_date) as email_sub"),
        expr("current_date as todays_date"),
    ]
)
unique_88 = formula_87.distinct()

# COMMAND ----------

input_136 = spark.sql(f"""
select 
    *
from 
    {reporting_catalog}.gold.pendency_dashboard
""")

# COMMAND ----------

filter_141 = input_136.where("filing_basis_grp != 'MADRID'")

# COMMAND ----------

filter_138 = filter_141.where("on_hold = 0")

# COMMAND ----------

formula_142 = filter_138.select(
    [
        "*",
        expr(
            """
        case when 
            date_diff(current_date, first_action_dt_ph) <= 15
            then 1 
            else 0
        end as last_thirty_days
        """
        ),
    ]
)

# COMMAND ----------

filter_143 = formula_142.where("last_thirty_days = 1")

# COMMAND ----------

formula_137 = filter_143.withColumn(
    "fa_pendendency_weight",
    col("active_classes_firstaction") * col("first_action_pendency_ph"),
)

# COMMAND ----------

multifield_binning_145 = formula_137.withColumn(
    "first_action_pendency_ph_tile_num",
    ntile(3).over(Window.orderBy("first_action_pendency_ph")),
)

# COMMAND ----------

filter_146 = multifield_binning_145.where("first_action_pendency_ph_tile_num = 2")

# COMMAND ----------

summarize_139 = (
    (
        filter_146.groupBy().agg(
            sum("fa_pendendency_weight"), sum("active_classes_firstaction")
        )
    )
    .withColumnRenamed("sum(fa_pendendency_weight)", "sum_fa_pendendency_weight")
    .withColumnRenamed(
        "sum(active_classes_firstaction)", "sum_active_classes_firstaction"
    )
)

# COMMAND ----------

formula_140 = summarize_139.withColumn(
    "1ap",
    round(col("sum_fa_pendendency_weight") / col("sum_active_classes_firstaction"), 1),
)

# COMMAND ----------

select_144 = formula_140.select("1ap")

# COMMAND ----------

input_100 = spark.sql(f"select * from {reporting_catalog}.silver.currently_processing_first_actions_with_controls")

# COMMAND ----------

summarize_101 = (
    input_100.groupBy()
    .agg(max(col("todays_date")))
    .withColumnRenamed("max(todays_date)", "max_todays_date")
)

# COMMAND ----------

join_102 = (
    summarize_101.alias("left")
    .join(
        other=input_100.alias("right"),
        on=[col("left.max_todays_date") == col("right.todays_date")],
        how="inner",
    )
    .select([expr("pendency_cal_start_dt as previous_output")])
)

# COMMAND ----------

unique_127 = join_102.distinct()

# COMMAND ----------

append_106 = unique_88.join(other=unique_127)

# COMMAND ----------

append_107 = append_106.join(other=select_144)

# COMMAND ----------

formula_109 = append_107.select(
    [
        "*",
        expr(
            """
            case 
                when pendency_cal_start_dt >= previous_output 
                and abs(current_process_pendency - 1AP) <= 1
                    then 1
                    else 0
            end as continue_process
            """
        ),
    ]
)

# COMMAND ----------

block_until_done_103 = formula_109

# COMMAND ----------

unique_124 = block_until_done_103.distinct()

# COMMAND ----------

select_105 = block_until_done_103.select(
    [
        "min_record_id",
        "record_id",
        "pendency_cal_start_dt",
        "cases",
        "sum_cases",
        "percent_total",
        "test",
        "date_plus_two",
        "today",
        "current_process_pendency",
        "datetime_out1",
        "datetime_out2",
        "text",
        "todays_date",
        "email_sub",
    ]
)

# COMMAND ----------

union_104 = select_105.union(input_100)

# COMMAND ----------

summarize_128 = union_104.distinct()

# COMMAND ----------

summarize_128.createOrReplaceTempView("loader_99")

# COMMAND ----------

display(
    spark.sql(
      f"""
      insert
        overwrite {reporting_catalog}.silver.currently_processing_first_actions_with_controls
      select
        distinct *
      from
        loader_99
      """
    )
)

# COMMAND ----------

select_125 = formula_109.select(
    [expr("pendency_cal_start_dt as StartDate"), expr("date_plus_two as EndDate")]
)

# COMMAND ----------

# DBTITLE 1,Get Count for Job - Terminate if No Output
output_count = summarize_128.count()
if output_count == 0:
    print("No new records will be loaded.")
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        output_count,
        "job completed successfully",
    )
    dbutils.notebook.exit("Job completed with no records.")

# COMMAND ----------

# DBTITLE 1,Convert Output to Pandas for Email
email_output = select_125

# COMMAND ----------

# DBTITLE 1,Send Email
if unique_124.count() > 0:
    print("Sending email...")
    subject, text = [(row.email_sub, row.text) for row in unique_124.select(["email_sub", "text"]).collect()][0]

    file_nm = "TMCurrentProcessing.xlsx"
    attachments = [(email_output, file_nm, 'excel')]

    send_email_report(
        job_nm = job_name,
        subject = subject,
        send_from = "Trademark_Analytics@uspto.gov",
        send_to = primary_email,
        send_to_cc= cc_email,
        html_body= text.replace("\n", "<br>"),
        attachments = attachments
    )

else:
    print("No email sent")

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    output_count,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed with {output_count} records.")

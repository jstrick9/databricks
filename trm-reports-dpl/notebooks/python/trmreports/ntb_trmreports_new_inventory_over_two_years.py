# Databricks notebook source
# DBTITLE 1,Import
from typing import Final, Dict
import requests
import time
from datetime import datetime, timedelta
import io
import pandas as pd

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

print(reporting_catalog, tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,Globals
ENGINE_OPTIONS: Final = {
    "options": {
        "strings_to_urls": False,
        "strings_to_formulas": False,
    }
}
send_from: str = "trademark_analytics@uspto.gov"
send_to: str = "benjamin.fielstra@uspto.gov"
send_to_cc: str = "benjamin.fielstra@uspto.gov"
subject: str = "New Inventory Over Time Report"
attachment_name: str = "New Inventory Over Time Report.xlsx"
server: str = "mailer.uspto.gov"

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_daily_inventory_over_time_report"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Declare Year Cutoff and Lookback
# MAGIC %sql
# MAGIC declare or replace variable num_years int default 2;
# MAGIC
# MAGIC declare or replace variable decimal_places int default 3;
# MAGIC
# MAGIC declare or replace variable cutoff = case
# MAGIC   when month(current_date) >= 10 then year(current_date) + 1 - num_years
# MAGIC   else year(current_date) - num_years
# MAGIC end;
# MAGIC
# MAGIC select
# MAGIC   cutoff,
# MAGIC   decimal_places,
# MAGIC   num_years;

# COMMAND ----------

# DBTITLE 1,Daily Inventory View
spark.sql(
    f"""
select
  unexamined_date `date`,
  unexamined_classes daily_class_count,
  round(avg(unexamined_classes) over (order by unexamined_date)) class_count_rolling_average,
  round(
    avg(unexamined_classes) over (order by unexamined_date rows between 6 preceding and current row)
  ) class_count_7_day_average,
  round(
    avg(unexamined_classes) over (order by unexamined_date rows between 27 preceding and current row)
  ) class_count_28_day_average,
  round(
    avg(unexamined_classes) over (order by unexamined_date rows between 89 preceding and current row)
  ) class_count_90_day_average
from
  {reporting_catalog}.gold.inventory_unexamined_hstry
where
  unexamined_date >= make_date(cutoff, 10, 1)
"""
).createOrReplaceTempView("daily_inventory_over_time_report")
# display(spark.sql("select * from daily_inventory_over_time_report").limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   unexamined_date `date`,
# MAGIC   unexamined_classes daily_class_count,
# MAGIC   round(avg(unexamined_classes) over (order by unexamined_date)) class_count_rolling_average,
# MAGIC   round(
# MAGIC     avg(unexamined_classes) over (order by unexamined_date rows between 6 preceding and current row)
# MAGIC   ) class_count_7_day_average,
# MAGIC   round(
# MAGIC     avg(unexamined_classes) over (
# MAGIC         order by unexamined_date
# MAGIC         rows between 27 preceding and current row
# MAGIC       )
# MAGIC   ) class_count_28_day_average,
# MAGIC   round(
# MAGIC     avg(unexamined_classes) over (
# MAGIC         order by unexamined_date
# MAGIC         rows between 89 preceding and current row
# MAGIC       )
# MAGIC   ) class_count_90_day_average,
# MAGIC   round(
# MAGIC     avg(unexamined_classes) over (
# MAGIC         order by unexamined_date
# MAGIC         rows between 365 preceding and current row
# MAGIC       )
# MAGIC   ) class_count_366_day_average,
# MAGIC   lag(unexamined_cases) over (order by unexamined_date)
# MAGIC   - lag(unexamined_cases) over (order by unexamined_date - interval 1 month) end_month_difference,
# MAGIC   1 drop_since_peak
# MAGIC from
# MAGIC   trm_reporting.gold.inventory_unexamined_hstry

# COMMAND ----------

# DBTITLE 1,Create Multisheet Excel
sheets = ["Daily Inventory Over Time"]

dataframes = [spark.sql("select * from daily_inventory_over_time_report order by `date`")]


with BytesIO() as stream:
    with pd.ExcelWriter(
        stream,
        engine="xlsxwriter",
        engine_kwargs=ENGINE_OPTIONS,
    ) as writer:
        for sheet, dataframe in zip(sheets, dataframes):
            dataframe.toPandas().to_excel(
                excel_writer=writer, index=False, sheet_name=sheet
            )
            writer.sheets[sheet].autofit()

    email_data: bytes = stream.getvalue()
print(f"FIRST 10: {email_data[:10]}")

# COMMAND ----------

# DBTITLE 1,Send Email
msg = MIMEMultipart()
msg["From"]: str = send_from
msg["To"]: str = COMMASPACE.join(send_to.split(","))
msg["Cc"]: str = COMMASPACE.join(send_to_cc.split(","))
msg["Subject"]: str = subject

text: str = f"""
Hi,

Please see the attached document regarding new inventory over the past two fiscal years (and current).
"""

msg.attach(MIMEText(text))
part = MIMEApplication(email_data)
encoders.encode_base64(part)
part.add_header(
    "Content-Disposition",
    "attachment",
    filename=attachment_name,
)
msg.attach(part)

smtp = smtplib.SMTP(server)
rcpt = send_to.split(",") + (send_to_cc.split(",") if send_to_cc else [])
smtp.sendmail(send_from, rcpt, msg.as_string())
smtp.close()

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed by sending the report successfully.")

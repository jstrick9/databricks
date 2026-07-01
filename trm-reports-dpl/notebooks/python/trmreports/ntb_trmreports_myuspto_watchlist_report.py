# Databricks notebook source
# DBTITLE 1,Import
from typing import Final, Dict
import requests
import time
from datetime import datetime, timedelta
import io
import pandas as pd
from zoneinfo import ZoneInfo

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
jbteasps_catalog = common_configs["schema"]["trm_jbteasps_src_catalog"]
practitioner_catalog = common_configs["schema"]["tm_practitioner_catalog"]
myuspto_contacts = common_configs["alerting"]["myuspto_account_monitor"]

print(reporting_catalog, jbteasps_catalog, practitioner_catalog, myuspto_contacts)

# COMMAND ----------

# DBTITLE 1,Globals
ENGINE_OPTIONS: Final = {
    "options": {
        "strings_to_urls": False,
        "strings_to_formulas": False,
    }
}
send_from: str = "trademark_analytics@uspto.gov"
send_to: str = myuspto_contacts["email"]
send_to_cc: str = myuspto_contacts["cc"]
subject: str = "MyUSPTO Account Monitor Activity"
attachment_name: str = "MyUSPTO Account Monitor Activity.xlsx"
server: str = "mailer.uspto.gov"

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_myuspto_watchlist_report"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Session Data
spark.sql(
    f"""
    with patron_information as (
    select
        account_id patron_id,
        account_patron_name patron_name,
        account_email patron_email,
        account_email patron_account_name,
        date(account_creation_timestamp) account_creation_date
    from
        {practitioner_catalog}.silver.dim_account pi
    ),
    base as (
        select
            upper(al.cfk_patron_id) patron_id,
            al.serial_no serial_number,
            pi.patron_account_name,
            pi.patron_name,
            pi.patron_email,
            pi.account_creation_date,
            al.create_ts submission_time_ts,
            hour(al.create_ts) hour_submission_time_ts,
            date(al.create_ts) submission_date,
            iff(
                date(al.create_ts) >= (current_date - interval 91 days),
                1, 
                0
            ) 90_day_submissions,
            iff(
                date(al.create_ts) = (current_date - interval 2 days),
                1, 
                0
            ) 24_hour_submissions,
            al.fk_signature_type_cd signature_type,
            al.fk_form_cd form_type
        from
            {jbteasps_catalog}.bronze.audit_log al
            inner join patron_information pi
                on al.cfk_patron_id = pi.patron_id
        where
            al.cfk_patron_id ilike '%-%-%-%-%'
            and al.fk_transaction_type_cd = 'Submission'
            and al.serial_no is not null
    )
    select
       *
    from
        base
"""
).createOrReplaceTempView("session_data")
spark.sql("select * from session_data").limit(2).show(truncate=False, vertical=True)

# COMMAND ----------

# DBTITLE 1,Match Data
spark.sql(
    f"""
  select
    * except (create_user, create_timestamp),
    from_utc_timestamp(create_timestamp, 'EST') load_datetime,
    rank() over (order by create_timestamp desc) latest
  from
    {reporting_catalog}.silver.myuspto_monitor_watchlist
  qualify latest = 1
"""
).createOrReplaceTempView("user_data")
spark.sql("select * from user_data").limit(5).show(truncate=False, vertical=True)

# COMMAND ----------

timezone: ZoneInfo = ZoneInfo("US/Eastern")
current_time: datetime = datetime.datetime.now(timezone)
report_time: str = current_time.strftime("%A, %B %d, %Y %I:%M %p EST")

user_data = spark.sql("select max(load_datetime) load_time from user_data having load_time is not null")
if user_data.count() == 0:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        0,
        "job completed successfully",
    )
    dbutils.notebook.exit(
        f"Job completed but did not send the report because there was no user input to process."
    )
load_time: datetime = user_data.collect()[0][0]
latest_load_time: str = load_time.strftime("%A, %B %d, %Y %I:%M %p EST")
print(f"Latest input load generation time: {latest_load_time}")
print(f"Report generation time: {report_time}")

# COMMAND ----------

# DBTITLE 1,Summary Sheet Data
# MAGIC %sql
# MAGIC create or replace temp view summary as
# MAGIC select
# MAGIC   sd.patron_id,
# MAGIC   sd.patron_name,
# MAGIC   sd.patron_email,
# MAGIC   sum(sd.24_hour_submissions) 24_hour_submissions,
# MAGIC   sum(sd.90_day_submissions) 90_day_submissions
# MAGIC from
# MAGIC   session_data sd
# MAGIC     join user_data ud
# MAGIC       on sd.patron_id = ud.patron_id
# MAGIC group by
# MAGIC   all;
# MAGIC
# MAGIC create or replace temp view 24_hour_details as
# MAGIC select
# MAGIC   sd.patron_id,
# MAGIC   sd.patron_name,
# MAGIC   sd.patron_email,
# MAGIC   sd.serial_number,
# MAGIC   sd.form_type,
# MAGIC   sd.submission_time_ts submission_ts
# MAGIC from
# MAGIC   session_data sd
# MAGIC     join user_data ud
# MAGIC       on sd.patron_id = ud.patron_id
# MAGIC where
# MAGIC   sd.24_hour_submissions = 1;
# MAGIC
# MAGIC create or replace temp view 90_day_details as
# MAGIC select
# MAGIC   sd.patron_id,
# MAGIC   sd.patron_name,
# MAGIC   sd.patron_email,
# MAGIC   sd.serial_number,
# MAGIC   sd.form_type,
# MAGIC   sd.submission_time_ts submission_ts
# MAGIC from
# MAGIC   session_data sd
# MAGIC     join user_data ud
# MAGIC       on sd.patron_id = ud.patron_id
# MAGIC where
# MAGIC   sd.90_day_submissions = 1;
# MAGIC
# MAGIC create or replace temp view valid_input_data as
# MAGIC select
# MAGIC   * except (latest)
# MAGIC from
# MAGIC   user_data
# MAGIC where
# MAGIC   send_alert = 'Y'
# MAGIC   and is_valid = 'Y';
# MAGIC
# MAGIC create or replace temp view invalid_input_data as
# MAGIC select
# MAGIC   * except (latest)
# MAGIC from
# MAGIC   user_data
# MAGIC where
# MAGIC   send_alert = 'Y'
# MAGIC   and is_valid = 'N';
# MAGIC
# MAGIC create or replace temp view ignored_input_data as
# MAGIC select
# MAGIC   * except (latest)
# MAGIC from
# MAGIC   user_data
# MAGIC where
# MAGIC   send_alert != 'Y';

# COMMAND ----------

# DBTITLE 1,Create Multisheet Excel
sheets = (
    "Summary",
    "24 Hour Detail",
    "90 Day Detail",
    "Valid Input Data",
    "Invalid Input Data",
    "Ignored Input Data"
)

dataframes = (
    spark.sql("select * from summary"),
    spark.sql("select * from 24_hour_details"),
    spark.sql("select * from 90_day_details"),
    spark.sql("select * from valid_input_data"),
    spark.sql("select * from invalid_input_data"),
    spark.sql("select * from ignored_input_data")
)

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
msg["Subject"]: str = subject + " | " + report_time

text: str = f"""
Hi,

Please see the attached document regarding MyUSPTO accounts with activity that match the watchlist. 

Breakdown of report (generated on {report_time}) using the latest account watchlist (last loaded {latest_load_time}):
    - ({spark.sql("select distinct patron_id from summary").count()}) records had matches.
    - ({spark.sql("select * from user_data").count()}) input records were compared.
    - ({spark.sql("select * from ignored_input_data").count()}) records without a positive alert flags (Y) were ignored (either for invalid alert flags or negative alert flags (N)).
    - ({spark.sql("select * from invalid_input_data").count()}) records with an positive alert flags (Y) were invalid.
    - ({spark.sql("select * from valid_input_data").count()}) records with an positive alert flags (Y) were valid.

To make changes to the watchlist, please visit the following link:
https://usptogov.sharepoint.com/:x:/r/sites/O3G-TrademarkDataandAnalyticsProductGroup/_layouts/15/Doc.aspx?sourcedoc=%7B6E34D510-A551-40F1-A944-2A111C18057A%7D&file=watch.xlsx&action=default&mobileredirect=true
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

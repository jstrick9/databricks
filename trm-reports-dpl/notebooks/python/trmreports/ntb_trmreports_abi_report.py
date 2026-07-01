# Databricks notebook source
# DBTITLE 1,Imports
from dateutil.parser import parse
from io import BytesIO
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")
dbutils.widgets.text("start_date", "")
start_date = dbutils.widgets.get("start_date")
dbutils.widgets.text("end_date", "")
end_date = dbutils.widgets.get("end_date")
dbutils.widgets.text("is_validation", "Y")
is_validation = dbutils.widgets.get("is_validation") != 'N'

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=},{start_date=},{end_date=},{is_validation=}")

# COMMAND ----------

# DBTITLE 1,Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Mixed Functions
def validate_date_string(date_string: str) -> bool:
    try:
        datetime.date.fromisoformat(date_string)
        return True
    except ValueError:
        print(
            "The parameter start/end_date format, should be in the form of `YYYY-MM-DD`."
        )
        return False


def get_bytestream_for_multiple_sheets(
    dataframe_sheets: list[tuple[pd.core.frame.DataFrame, str]],
    output_type: str = "excel",
):
    if not dataframe_sheets:
        raise TypeError(
            "`dataframe_sheets` is not an instance of a `list[tuple[pd.core.frame.DataFrame, str]]` or is null. Please check the incoming data that you intend to attach."
        )
    if output_type == "excel":
        with BytesIO() as stream:
            with pd.ExcelWriter(
                stream,
                engine="xlsxwriter",
                engine_kwargs={
                    "options": {
                        "strings_to_urls": False,
                        "strings_to_formulas": False,
                    }
                },
            ) as writer:
                for df, sheet_name in dataframe_sheets:
                    print(f"Attaching: {sheet_name}")
                    df.to_excel(excel_writer=writer, index=False, sheet_name=sheet_name)
                stream.seek(0)
            return stream.getvalue()
    else:
        raise TypeError("Valid output types are: [`excel`]")


def send_mail(
    send_from: str,
    send_to: str,
    send_to_cc: str,
    subject: str,
    text: str,
    data_to_attach,
    attachment_name: str,
    server: str = "mailer.uspto.gov",
):
    try:
        msg = MIMEMultipart()
        msg["From"] = send_from
        msg["To"] = COMMASPACE.join(send_to.split(","))
        msg["Cc"] = COMMASPACE.join(send_to_cc.split(","))
        msg["Subject"] = subject

        msg.attach(MIMEText(text))

        part = MIMEApplication(get_bytestream_for_multiple_sheets(data_to_attach))
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
    except Exception as error:
        print("An issue occured during the email sending process.")
        raise error

# COMMAND ----------

# DBTITLE 1,Set Catalogs
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmprodvty_catalog = common_configs["schema"]["tmprodvty_catalog"]
print(reporting_catalog, tmngpdb_catalog, tmprodvty_catalog)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_abi_report"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Helpers
def sample(view: str) -> None:
    """
    Simple wrapper to get a sample of a temp view.
    """
    display(spark.sql(f"select * from {view}").limit(5))

# COMMAND ----------

# DBTITLE 1,Generate FY
if (
    start_date
    and end_date
    and validate_date_string(start_date)
    and validate_date_string(end_date)
):
    print("Using custom parameter for date range.")
else:
  print("Using default date ranges based on `current_date`.")
  fiscal_year_details = spark.sql(
      """
  select
    date_format(current_date, 'yyyy-MM-dd') today,
    iff(month(current_date) >= 10, year(current_date) + 1, year(current_date)) fy,
    iff(
      month(current_date) >= 10,
      cast(year(current_date) - 1 as string),
      cast(year(current_date) - 1 as string)
    )
      || '-10-'
      || '01' start_date,
    date_format(date_trunc('month', current_date) - interval 1 day, 'yyyy-MM-dd') end_date
  """
  ).collect()
  today, fy, start_date, end_date = (
      fiscal_year_details[0].today,
      fiscal_year_details[0].fy,
      fiscal_year_details[0].start_date,
      fiscal_year_details[0].end_date,
  )

print(f"{today=}", f"{fy=}", f"{start_date=}", f"{end_date=}")
is_sent_to_users: str = today.endswith("01")
print(f"Report will be sent to users: {is_sent_to_users}")

# COMMAND ----------

# DBTITLE 1,File, Directory, and Email Details
report_directory: str = "\s-2-isl1-smb.uspto.gov\TRA_123753_TMCMS_TMDNA$\Project Excalibur\Report_Output\ABI Report\\"
report_name: str = f"{start_date}-{end_date} ABI Report.xlsx"
fqn: str = f"{report_directory}\\{report_name}"
print(
    f"""{report_directory=}
{report_name=}
{fqn=}"""
)

send_from: str = "trademark_analytics@uspto.gov"
# TODO: Change once valid
send_to: str = (
    "andrew.wang1@uspto.gov; sharmi.dasgupta@uspto.gov; sahar.ahmed@uspto.gov; michael.shaver@uspto.gov"
    if (is_sent_to_users and not is_validation)
    else "benjamin.fielstra@uspto.gov"
)
current_date = datetime.datetime.today().strftime("%B %d, %Y")
send_to_cc: str = "benjamin.fielstra@uspto.gov"
subject: str = f"Auto-generated: ABI Report for {current_date}"
text: str = f"""
Good morning,

Attached, you will find the results for the monthly ABI Report based upon the following date range: {start_date}-{end_date}.

For any questions or concerns, please reach out to us via the TMDnA Request Form.

Thank you,

Trademark Data and Analytics
"""

# COMMAND ----------

# DBTITLE 1,Petitions Definitions
# MAGIC %sql
# MAGIC create or replace temp view petitions_definitions as
# MAGIC select
# MAGIC   col1 `index`,
# MAGIC   col2 `sort`,
# MAGIC   col3 `code`,
# MAGIC   col4 `description`
# MAGIC from
# MAGIC   (
# MAGIC     values
# MAGIC       (1, 1, 'EPGSI', 'Petition to Revive Received'),
# MAGIC       (2, 1, 'PETRI', 'Petition to Revive Received'),
# MAGIC       (3, 1, 'PROAI', 'Petition to Revive Received'),
# MAGIC       (4, 1, 'TPETI', 'Petition to Revive Received'),
# MAGIC       (5, 1, 'TPOAI', 'Petition to Revive Received'),
# MAGIC       (6, 1, 'TPSEI', 'Petition to Revive Received'),
# MAGIC       (7, 2, 'PETGO', 'Petition to Revive Granted'),
# MAGIC       (8, 3, 'PETDO', 'Petition to Revive Denied'),
# MAGIC       (9, 4, 'PINMO', 'Incomplete Petition Notice Mailed'),
# MAGIC       (10, 4, 'PINMI', 'Incomplete Petition Notice Mailed'),
# MAGIC       (11, 5, 'PR.DT', 'Petition to Revive Dismissed'),
# MAGIC       (12, 6, 'PR.WT', 'Petition to Revive Withdrawn'),
# MAGIC       (13, 7, 'PCRCI', 'Petition to the Director Received'),
# MAGIC       (14, 7, 'TPDRI', 'Petition to the Director Received'),
# MAGIC       (15, 8, 'PCGRO', 'Petition to the Director Granted'),
# MAGIC       (16, 9, 'PCDEO', 'Petition to the Director Denied'),
# MAGIC       (17, 10, 'PRIMO', 'Petition Inquiry Letter Mailed'),
# MAGIC       (18, 10, 'PRIMI', 'Petition Inquiry Letter Mailed'),
# MAGIC       (19, 10, 'PILMI', 'Petition Inquiry Letter Mailed'),
# MAGIC       (20, 11, 'PC.DI', 'Petition to the Director Dismissed'),
# MAGIC       (21, 12, 'PCBMI', 'Petition to the Director Withdrawn'),
# MAGIC       (22, 13, 'MREIE', 'Reinstated'),
# MAGIC       (23, 13, 'MREIO', 'Reinstated'),
# MAGIC       (24, 13, 'REINI', 'Reinstated'),
# MAGIC       (25, 13, 'REINO', 'Reinstated'),
# MAGIC       (26, 13, 'RG1BO', 'Reinstated'),
# MAGIC       (27, 13, 'RGDVO', 'Reinstated'),
# MAGIC       (28, 13, 'RGEXO', 'Reinstated'),
# MAGIC       (29, 13, 'RGIAO', 'Reinstated'),
# MAGIC       (30, 13, 'RGLAO', 'Reinstated'),
# MAGIC       (31, 13, 'RGOAO', 'Reinstated'),
# MAGIC       (32, 13, 'RGRNO', 'Reinstated'),
# MAGIC       (33, 13, 'RGRRO', 'Reinstated'),
# MAGIC       (34, 13, 'RGSUO', 'Reinstated'),
# MAGIC       (35, 13, 'RGTDO', 'Reinstated'),
# MAGIC       (36, 13, 'RGTRO', 'Reinstated'),
# MAGIC       (37, 13, 'RGTTO', 'Reinstated'),
# MAGIC       (38, 13, 'RRGGO', 'Reinstated'),
# MAGIC       (39, 14, 'LOPTI', 'Letter of Protest Accepted')
# MAGIC   );
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   petitions_definitions

# COMMAND ----------

# DBTITLE 1,Credit Definitions
# MAGIC %sql
# MAGIC create or replace temp view credit_definitions as
# MAGIC select
# MAGIC   col1 `row_name`,
# MAGIC   col2 `row_num`,
# MAGIC   col3 `filing_method_filed`
# MAGIC from
# MAGIC   (
# MAGIC     values
# MAGIC       ('Initial Exam (1.0 Action point/class):', 1, 'MADRID'),
# MAGIC       ('Approval for Publication:', 7, 'MADRID'),
# MAGIC       ('SOU Exam (0.5 Action point/class):', 13, 'MADRID'),
# MAGIC       ('Allowance for Registration:', 19, 'MADRID'),
# MAGIC       ('Abandonment:', 25, 'MADRID'),
# MAGIC       ('Suspensions:', 37, 'MADRID'),
# MAGIC       ('Final Refusals:', 41, 'MADRID'),
# MAGIC       ('Report Unresponsive Amendment:', 45, 'MADRID'),
# MAGIC       ('Initial Exam (1.0 Action point/class):', 1, 'Paper'),
# MAGIC       ('Approval for Publication:', 7, 'Paper'),
# MAGIC       ('SOU Exam (0.5 Action point/class):', 13, 'Paper'),
# MAGIC       ('Allowance for Registration:', 19, 'Paper'),
# MAGIC       ('Abandonment:', 25, 'Paper'),
# MAGIC       ('Suspensions:', 37, 'Paper'),
# MAGIC       ('Final Refusals:', 41, 'Paper'),
# MAGIC       ('Report Unresponsive Amendment:', 45, 'Paper'),
# MAGIC       ('Initial Exam (1.0 Action point/class):', 1, 'TEAS'),
# MAGIC       ('Approval for Publication:', 7, 'TEAS'),
# MAGIC       ('SOU Exam (0.5 Action point/class):', 13, 'TEAS'),
# MAGIC       ('Allowance for Registration:', 19, 'TEAS'),
# MAGIC       ('Abandonment:', 25, 'TEAS'),
# MAGIC       ('Suspensions:', 37, 'TEAS'),
# MAGIC       ('Final Refusals:', 41, 'TEAS'),
# MAGIC       ('Report Unresponsive Amendment:', 45, 'TEAS'),
# MAGIC       ('Initial Exam (1.0 Action point/class):', 1, 'TEAS PLUS'),
# MAGIC       ('Approval for Publication:', 7, 'TEAS PLUS'),
# MAGIC       ('SOU Exam (0.5 Action point/class):', 13, 'TEAS PLUS'),
# MAGIC       ('Allowance for Registration:', 19, 'TEAS PLUS'),
# MAGIC       ('Abandonment:', 25, 'TEAS PLUS'),
# MAGIC       ('Suspensions:', 37, 'TEAS PLUS'),
# MAGIC       ('Final Refusals:', 41, 'TEAS PLUS'),
# MAGIC       ('Report Unresponsive Amendment:', 45, 'TEAS PLUS'),
# MAGIC       ('Initial Exam (1.0 Action point/class):', 1, 'TEAS STD'),
# MAGIC       ('Approval for Publication:', 7, 'TEAS STD'),
# MAGIC       ('SOU Exam (0.5 Action point/class):', 13, 'TEAS STD'),
# MAGIC       ('Allowance for Registration:', 19, 'TEAS STD'),
# MAGIC       ('Abandonment:', 25, 'TEAS STD'),
# MAGIC       ('Suspensions:', 37, 'TEAS STD'),
# MAGIC       ('Final Refusals:', 41, 'TEAS STD'),
# MAGIC       ('Report Unresponsive Amendment:', 45, 'TEAS STD'),
# MAGIC       ('Initial Exam (1.0 Action point/class):', 1, 'BASE'),
# MAGIC       ('Approval for Publication:', 7, 'BASE'),
# MAGIC       ('SOU Exam (0.5 Action point/class):', 13, 'BASE'),
# MAGIC       ('Allowance for Registration:', 19, 'BASE'),
# MAGIC       ('Abandonment:', 25, 'BASE'),
# MAGIC       ('Suspensions:', 37, 'BASE'),
# MAGIC       ('Final Refusals:', 41, 'BASE'),
# MAGIC       ('Report Unresponsive Amendment:', 45, 'BASE')
# MAGIC   );
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   credit_definitions;

# COMMAND ----------

# DBTITLE 1,EP70 Credit Type Definitions
# MAGIC %sql
# MAGIC create or replace temp view ep_seventy_credit_type_definitions as
# MAGIC select
# MAGIC   col1 `ep_tran_cd`,
# MAGIC   col2 `row_name`
# MAGIC from
# MAGIC   (
# MAGIC     values
# MAGIC       ('6325', 'Non-Final Actions'),
# MAGIC       ('6326', 'Priority Actions'),
# MAGIC       ('6328', 'Examiner\'s Amendment'),
# MAGIC       ('6338', 'Approval for Publications (PR)'),
# MAGIC       ('6339', 'Approval for Publications (SR)'),
# MAGIC       ('6341', 'Approval for Pub. (PR) and Abandon. of Cls.'),
# MAGIC       ('6342', 'Approval for Pub. (SR) and Abandon. of Cls.'),
# MAGIC       ('6125', 'SOU Non-Final Actions'),
# MAGIC       ('6126', 'SOU Priority Actions'),
# MAGIC       ('6128', 'SOU Examiner\'s Amendment'),
# MAGIC       ('6138', 'Allowance for Registration (PR)'),
# MAGIC       ('6139', 'Allowance for Registration (SR)'),
# MAGIC       ('6141', 'Allowance for Reg. (PR) and Abandon. of Class'),
# MAGIC       ('6142', 'Allowance for Reg. (SR) and Abandon. of Class'),
# MAGIC       ('6140', 'Abandonment - Classes Only (SOU Exam)'),
# MAGIC       ('6322', 'Abandonment - Express'),
# MAGIC       ('6323', 'Abandonment - Failure to Respond'),
# MAGIC       ('6324', 'Abandonment - Imcomplete Response'),
# MAGIC       ('6327', 'Abandonment - After Publication'),
# MAGIC       ('6337', 'Abandonment - Late Response'),
# MAGIC       ('6340', 'Abandonment - Classes Only (Initial Exam)'),
# MAGIC       ('6830', 'Abandonment After Ex Parte Appeal'),
# MAGIC       ('6831', 'Abandonment After Inter Parte Proceedings'),
# MAGIC       ('7788', 'Abandonment Credit for Dropped Classes'),
# MAGIC       ('6332', 'Letter of Suspension'),
# MAGIC       ('6333', 'Inquiry as to Suspension'),
# MAGIC       ('6129', 'Final Refusal - SOU Exam'),
# MAGIC       ('6329', 'Final Refusal - Initial Exam'),
# MAGIC       ('6135', 'Report Unresponsive Amendment - SOU Exam'),
# MAGIC       ('6335', 'Report Unresponsive Amendment - Initial Exam'),
# MAGIC       ('7777', 'Retroactive First Action Credit')
# MAGIC   );
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ep_seventy_credit_type_definitions;

# COMMAND ----------

# DBTITLE 1,Disqualifying Events Definitions
# MAGIC %sql
# MAGIC create or replace temp view disqualifying_events_definitions as
# MAGIC select
# MAGIC   col1 `index`,
# MAGIC   col2 `code`,
# MAGIC   col3 `description`
# MAGIC from
# MAGIC   (
# MAGIC     values
# MAGIC       (1, 'FAXXI', '66(a)'),
# MAGIC       (2, 'MAILI', '66(a)'),
# MAGIC       (3, 'FAXXI', 'Non-66(a)'),
# MAGIC       (4, 'MAILI', 'Non-66(a)')
# MAGIC   );
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   disqualifying_events_definitions;

# COMMAND ----------

# DBTITLE 1,Prosecution History
spark.sql(f"""
    select
        *,
        ph_action_code || fifth_char_cm_type `code`
    from
        {reporting_catalog}.silver.prosecution_history
    where
        date(ph_action_date) >= '{start_date}'
        and date(ph_action_date) <= '{end_date}'
""").createOrReplaceTempView("prosecution_history")
sample("prosecution_history")

# COMMAND ----------

# DBTITLE 1,Milestone
spark.sql(f"""
select
    *,
    coalesce(date(registration_dt), abandonment_dt) filter_dt,
    coalesce(
        case when registration_dt is not null then 'Registration' end, 
        case when abandonment_dt is not null then 'Abandonment' end
    ) reg_aban
from
    {reporting_catalog}.silver.milestone
where
    (
        date(first_action_dt_ph) is not null
        and date(registration_dt) between '{start_date}' and '{end_date}'
    )
    or 
    (
        date(first_action_dt_ph) is not null
        and date(abandonment_dt) between '{start_date}' and '{end_date}'
    )
""").createOrReplaceTempView("milestone")
sample("milestone")

# COMMAND ----------

# DBTITLE 1,Bibliography
spark.sql(
    f"""
    select
        ser_num,
        case
            when filing_basis_grp = 'MADRID' 
                then '66(a)'
                else 'Non-66(a)'
        end filing_basis_grp,
        filing_method_filed
    from
        {reporting_catalog}.silver.bibliography
"""
).createOrReplaceTempView("bibliography")
sample("bibliography")

# COMMAND ----------

# DBTITLE 1,ITU
spark.sql(
    f"""
    select
        ser_num,
        filing_method_filed
    from
        {reporting_catalog}.silver.bibliography
    where 
        filing_basis_fil = 'ITU'
"""
).createOrReplaceTempView("itu")
sample("itu")

# COMMAND ----------

# DBTITLE 1,Post Registration Detail
spark.sql(f"""
    select
        *
    from
        {reporting_catalog}.gold.post_reg_detail_dashboard
    where
        date(start_action_date) >= '{start_date}'
        and date(start_action_date) <= '{end_date}'
""").createOrReplaceTempView("post_registration_detail")
sample("post_registration_detail")

# COMMAND ----------

# DBTITLE 1,Classes
spark.sql(f"""
    select distinct
        ser_num,
        class
    from
        {reporting_catalog}.silver.class
    where
        class_status != 'inactive'
"""
).createOrReplaceTempView("classes")
sample("classes")

# COMMAND ----------

# DBTITLE 1,ITU Lie Review
spark.sql(f"""
select distinct
  get(split(pt.cfk_object_gid, ':'), 2) ep_ser_num, -- some object gids are serials (6)
  pt.cfk_object_type_cd,
  pt.fk_generating_prodvty_actn_id,
  pt.fk_corrected_prodvty_actn_id,
  pt.unit_count_no ep_actn_credit,
  pt.dn_worker_no ep_exmr_num,
  pt.dn_worker_tm_organization_cd ep_exmr_lo,
  pt.cfk_worker_gid,
  pt.cfk_bcr_pay_period_range_name,
  cast(pt.dn_action_no as int) ep_actn_num,
  date(pt.create_ts) ep_actn_ct_dt,
  pa.productivity_action_cd ep_tran_cd,
  pa.title_tx
from
  {tmprodvty_catalog}.bronze.production_transaction pt
    join {tmprodvty_catalog}.bronze.productivity_action pa
      on pt.fk_generating_prodvty_actn_id = pa.productivity_action_id
where
  pt.dn_worker_tm_organization_cd is not null
  and date(pt.create_ts) >= '{start_date}'
  and date(pt.create_ts) <= '{end_date}'
""").createOrReplaceTempView("itu_lie_review")
sample("itu_lie_review")

# COMMAND ----------

# DBTITLE 1,Amendments (AAU)
# MAGIC %sql
# MAGIC create or replace temp view amendments as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   itu_lie_review a
# MAGIC     join bibliography b
# MAGIC       on a.ep_ser_num = b.ser_num
# MAGIC where
# MAGIC   ep_tran_cd in (
# MAGIC     '0102',
# MAGIC     '0107',
# MAGIC     '0108',
# MAGIC     '0110',
# MAGIC     '0120',
# MAGIC     '0123',
# MAGIC     '0200',
# MAGIC     '0210',
# MAGIC     '0410',
# MAGIC     '0610',
# MAGIC     '1321',
# MAGIC     '6349',
# MAGIC     '9126',
# MAGIC     '9128',
# MAGIC     '9132'
# MAGIC   );
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   amendments
# MAGIC limit 5;

# COMMAND ----------

# DBTITLE 1,Compute: Petitions
# MAGIC %sql
# MAGIC create or replace temp view petition_cases_and_classes as
# MAGIC select
# MAGIC   serial_number,
# MAGIC   class,
# MAGIC   description,
# MAGIC   `sort`,
# MAGIC   first(description) first_description
# MAGIC from
# MAGIC   petitions_definitions a
# MAGIC     left join prosecution_history b
# MAGIC       on a.code = b.code
# MAGIC     left join classes b
# MAGIC       on b.serial_number = b.ser_num
# MAGIC group by
# MAGIC   serial_number,
# MAGIC   class,
# MAGIC   description,
# MAGIC   `sort`;
# MAGIC
# MAGIC create or replace temp view petition_counts as
# MAGIC select
# MAGIC   description,
# MAGIC   `sort`,
# MAGIC   count(distinct serial_number) distinct_cases,
# MAGIC   count(distinct serial_number, class) distinct_classes
# MAGIC from
# MAGIC   petition_cases_and_classes
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   `sort`;
# MAGIC
# MAGIC create or replace temp view petitions as
# MAGIC select
# MAGIC   * except (`sort`)
# MAGIC from
# MAGIC   petition_counts
# MAGIC order by
# MAGIC   description;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   petitions;

# COMMAND ----------

# DBTITLE 1,SSR Net
# MAGIC %sql
# MAGIC create or replace temp view ssr_base as
# MAGIC select distinct
# MAGIC   ep_ser_num,
# MAGIC   filing_method_filed,
# MAGIC   ep_actn_ct_dt,
# MAGIC   ep_tran_cd
# MAGIC from
# MAGIC   itu_lie_review a
# MAGIC     join bibliography b
# MAGIC       on a.ep_ser_num = b.ser_num
# MAGIC where
# MAGIC   ep_tran_cd in (9126, 9128);
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ssr_base
# MAGIC limit 5;

# COMMAND ----------

# DBTITLE 1,Compute: Extensions
# MAGIC %sql
# MAGIC create or replace temp view extensions as
# MAGIC select
# MAGIC   count(distinct serial_number, ph_action_code) extension_cases,
# MAGIC   count(distinct serial_number, class, ph_action_code) extension_classes
# MAGIC from
# MAGIC   prosecution_history a
# MAGIC     join classes b
# MAGIC       on a.serial_number = b.ser_num
# MAGIC where
# MAGIC   ph_action_code in ('EXT1', 'EXT2', 'EXT3', 'EXT4', 'EXT5');
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   extensions;

# COMMAND ----------

# DBTITLE 1,Compute: SOU
# MAGIC %sql
# MAGIC create or replace temp view sou as
# MAGIC select
# MAGIC   count(distinct serial_number, ph_action_code) sou_cases,
# MAGIC   count(distinct serial_number, class, ph_action_code) sou_classes
# MAGIC from
# MAGIC   prosecution_history a
# MAGIC     join classes b
# MAGIC       on a.serial_number = b.ser_num
# MAGIC where
# MAGIC   ph_action_code = 'EISU';
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   sou;

# COMMAND ----------

# DBTITLE 1,Compute: Post-Registrations
# MAGIC %sql
# MAGIC create or replace temp view post_registrations as
# MAGIC select
# MAGIC   count(distinct serial_number) post_reg_cases,
# MAGIC   count(distinct serial_number, class) post_reg_classes,
# MAGIC   postreg_category
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       serial_number,
# MAGIC       class,
# MAGIC       case
# MAGIC         when postreg_category = 'SECTION 7' then 'SECTION 7 REQUEST FILED'
# MAGIC         when postreg_category = 'SEPARATE 15' then 'SEPARATE 15 Filed'
# MAGIC         when
# MAGIC           postreg_category = '6 YEAR'
# MAGIC           and not contains(start_cm_desc, '15')
# MAGIC         then
# MAGIC           '6 YEAR: SECTION 8 / 71'
# MAGIC         when
# MAGIC           postreg_category = '6 YEAR'
# MAGIC           and contains(start_cm_desc, '15')
# MAGIC         then
# MAGIC           '6 YEAR: SECTION 8 / 71 & 15'
# MAGIC         when postreg_category = '10 YEAR' then '10 YEAR: 8 & 9 / 71'
# MAGIC       end postreg_category
# MAGIC     from
# MAGIC       post_registration_detail a
# MAGIC         left join classes b
# MAGIC           on a.serial_number = b.ser_num
# MAGIC   )
# MAGIC group by
# MAGIC   postreg_category
# MAGIC order by
# MAGIC   postreg_category;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   post_registrations;

# COMMAND ----------

# DBTITLE 1,Compute: Disqualifying Events
# MAGIC %sql
# MAGIC create or replace temp view disqualified_events as
# MAGIC with eligible_disqualified_events as (
# MAGIC   select
# MAGIC     serial_number,
# MAGIC     class,
# MAGIC     b.`code`,
# MAGIC     filing_basis_grp
# MAGIC   from
# MAGIC     bibliography a
# MAGIC       join prosecution_history b
# MAGIC         on a.ser_num = b.serial_number
# MAGIC       left join classes c
# MAGIC         on a.ser_num = c.ser_num
# MAGIC   where
# MAGIC     `code` in ('FAXXI', 'MAILI')
# MAGIC )
# MAGIC select
# MAGIC   count(distinct serial_number) disqualified_cases,
# MAGIC   count(distinct serial_number, class) disqualified_classes,
# MAGIC   a.`code`,
# MAGIC   description filing_basis_grp
# MAGIC from
# MAGIC   disqualifying_events_definitions a
# MAGIC     left join eligible_disqualified_events b
# MAGIC       on a.description = b.filing_basis_grp
# MAGIC       and a.code = b.code
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   `code`,
# MAGIC   filing_basis_grp;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   disqualified_events;

# COMMAND ----------

# DBTITLE 1,Compute: Disposals
# MAGIC %sql
# MAGIC create or replace temp view disposals as
# MAGIC select
# MAGIC   count(distinct b.ser_num) disposal_cases,
# MAGIC   count(distinct b.ser_num, class) disposal_classes,
# MAGIC   filing_basis_grp,
# MAGIC   reg_aban
# MAGIC from
# MAGIC   milestone a
# MAGIC     join classes b
# MAGIC       on a.ser_num = b.ser_num
# MAGIC     join bibliography c
# MAGIC       on a.ser_num = c.ser_num
# MAGIC where
# MAGIC   reg_aban is not null
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   reg_aban,
# MAGIC   filing_basis_grp;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   disposals;

# COMMAND ----------

# DBTITLE 1,Compute: ITU
# MAGIC %sql
# MAGIC create or replace temp view itu_prosecution_history as
# MAGIC select
# MAGIC   count(distinct a.ser_num) itu_cases,
# MAGIC   filing_method_filed,
# MAGIC   case
# MAGIC     when `code` in ('IUAFP', 'IUAFS', 'IUSFP', 'EISUI') then 'Use Statement Filed'
# MAGIC     when `code` in ('SUPCI') then 'Statement of Use Processing Complete'
# MAGIC     when `code` in ('EXT1S', 'EXT2S', 'EXT3S', 'EXT4S', 'EXT5S', 'TPEXI') then 'Extension Filed'
# MAGIC     when
# MAGIC       `code` in ('EX1DS', 'EX2DS', 'EX3DS', 'EX4DS', 'EX5DS')
# MAGIC     then
# MAGIC       'Extension Denied Letter Prepared'
# MAGIC     when
# MAGIC       `code` in ('EX1MS', 'EX2MS', 'EX3MS', 'EX4MS', 'EX5MS')
# MAGIC     then
# MAGIC       'Extension Denied Letter Mailed'
# MAGIC     when `code` in ('DRRRI') then 'Divisional Request Received'
# MAGIC     when `code` in ('DPCCD') then 'Divisional Processing Complete'
# MAGIC     else `code`
# MAGIC   end itu_category
# MAGIC from
# MAGIC   itu a
# MAGIC     join classes b
# MAGIC       on a.ser_num = b.ser_num
# MAGIC     join prosecution_history c
# MAGIC       on a.ser_num = c.serial_number
# MAGIC where
# MAGIC   c.code in (
# MAGIC     'IUAFP',
# MAGIC     'IUAFS',
# MAGIC     'IUSFP',
# MAGIC     'EISUI',
# MAGIC     'SUPCI',
# MAGIC     'EXT1S',
# MAGIC     'EXT2S',
# MAGIC     'EXT3S',
# MAGIC     'EXT4S',
# MAGIC     'EXT5S',
# MAGIC     'TPEXI',
# MAGIC     'EX1DS',
# MAGIC     'EX2DS',
# MAGIC     'EX3DS',
# MAGIC     'EX4DS',
# MAGIC     'EX5DS',
# MAGIC     'EX1MS',
# MAGIC     'EX2MS',
# MAGIC     'EX3MS',
# MAGIC     'EX4MS',
# MAGIC     'EX5MS',
# MAGIC     'DRRRI',
# MAGIC     'DPCCD'
# MAGIC   )
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   filing_method_filed,
# MAGIC   itu_category;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   itu_prosecution_history;

# COMMAND ----------

# DBTITLE 1,Compute: ITU Extensions
# MAGIC %sql
# MAGIC create or replace temp view itu_extensions as
# MAGIC with latest_prosecution_history as (
# MAGIC   select
# MAGIC     serial_number,
# MAGIC     max(ph_action_date) ph_action_date,
# MAGIC     case
# MAGIC       when month(max(ph_action_date)) >= 10 then year(max(ph_action_date)) + 1
# MAGIC       else year(max(ph_action_date))
# MAGIC     end sou_fy
# MAGIC   from
# MAGIC     prosecution_history a
# MAGIC   where
# MAGIC     ph_action_code = 'SUPC'
# MAGIC   group by
# MAGIC     all
# MAGIC ),
# MAGIC modified_latest_prosecution_history as (
# MAGIC   select
# MAGIC     count(distinct serial_number) itu_extension_cases,
# MAGIC     count(distinct serial_number, class) itu_extension_classes,
# MAGIC     case
# MAGIC       when ext5_dt is not null then 'Five Extensions'
# MAGIC       when ext4_dt is not null then 'Four Extensions'
# MAGIC       when ext3_dt is not null then 'Three Extensions'
# MAGIC       when ext2_dt is not null then 'Two Extensions'
# MAGIC       when ext1_dt is not null then 'One Extension'
# MAGIC       else 'No Extensions'
# MAGIC     end extensions_sou_processed,
# MAGIC     case
# MAGIC       when ext5_dt is not null then 6
# MAGIC       when ext4_dt is not null then 5
# MAGIC       when ext3_dt is not null then 4
# MAGIC       when ext2_dt is not null then 3
# MAGIC       when ext1_dt is not null then 2
# MAGIC       else 1
# MAGIC     end `sort`,
# MAGIC     sou_fy
# MAGIC   from
# MAGIC     latest_prosecution_history a
# MAGIC       join milestone b
# MAGIC         on a.serial_number = b.ser_num
# MAGIC       left join classes c
# MAGIC         on a.serial_number = c.ser_num
# MAGIC   where
# MAGIC     noa_dt is not null
# MAGIC   group by
# MAGIC     all
# MAGIC   having
# MAGIC     sou_fy > 2015
# MAGIC   order by
# MAGIC     sou_fy desc,
# MAGIC     `sort`
# MAGIC )
# MAGIC select
# MAGIC   sou_fy,
# MAGIC   extensions_sou_processed,
# MAGIC   itu_extension_cases,
# MAGIC   itu_extension_classes
# MAGIC from
# MAGIC   modified_latest_prosecution_history;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   itu_extensions;

# COMMAND ----------

# DBTITLE 1,Compute: ITU Abandonments
# MAGIC %sql
# MAGIC create or replace temp view itu_abandonments as
# MAGIC with modified_latest_prosecution_history as (
# MAGIC   select
# MAGIC     count(distinct a.ser_num) itu_abandonment_cases,
# MAGIC     count(distinct a.ser_num, class) itu_abandonment_classes,
# MAGIC     case
# MAGIC       when abandonment_dt >= ext5_dt then 'After Fifth Extension'
# MAGIC       when abandonment_dt >= ext4_dt then 'After Fourth Extension'
# MAGIC       when abandonment_dt >= ext3_dt then 'After Third Extension'
# MAGIC       when abandonment_dt >= ext2_dt then 'After Second Extension'
# MAGIC       when abandonment_dt >= ext1_dt then 'After First Extension'
# MAGIC       else 'Before First Extension and After NOA'
# MAGIC     end after_extension,
# MAGIC     case
# MAGIC       when month(max(abandonment_dt)) >= 10 then year(max(abandonment_dt)) + 1
# MAGIC       else year(max(abandonment_dt))
# MAGIC     end abandonment_fy,
# MAGIC     case
# MAGIC       when abandonment_dt >= ext5_dt then 5
# MAGIC       when abandonment_dt >= ext4_dt then 4
# MAGIC       when abandonment_dt >= ext3_dt then 3
# MAGIC       when abandonment_dt >= ext2_dt then 2
# MAGIC       when abandonment_dt >= ext1_dt then 1
# MAGIC       else 0
# MAGIC     end `sort`
# MAGIC   from
# MAGIC     milestone a
# MAGIC       join classes b
# MAGIC         on a.ser_num = b.ser_num
# MAGIC   where
# MAGIC     noa_dt is not null
# MAGIC     and abandonment_dt is not null
# MAGIC   group by
# MAGIC     all
# MAGIC   having
# MAGIC     abandonment_fy > 2015
# MAGIC   order by
# MAGIC     abandonment_fy desc,
# MAGIC     `sort`
# MAGIC )
# MAGIC select
# MAGIC   abandonment_fy,
# MAGIC   after_extension,
# MAGIC   itu_abandonment_cases,
# MAGIC   itu_abandonment_classes
# MAGIC from
# MAGIC   modified_latest_prosecution_history;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   itu_abandonments;

# COMMAND ----------

# DBTITLE 1,Compute: Appeal Brief
# MAGIC %sql
# MAGIC create or replace temp view appeal_brief as
# MAGIC select
# MAGIC   count(distinct a.ser_num) ab_cases,
# MAGIC   count(distinct a.ser_num, class) ab_classes, -- TODO: Ask Jim if order number is necessary to differentiate CNES
# MAGIC   filing_method_filed
# MAGIC from
# MAGIC   bibliography a
# MAGIC     join classes b
# MAGIC       on a.ser_num = b.ser_num
# MAGIC     join prosecution_history c
# MAGIC       on a.ser_num = c.serial_number
# MAGIC where
# MAGIC   c.ph_action_code = 'CNES'
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   filing_method_filed;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   appeal_brief;

# COMMAND ----------

# DBTITLE 1,Compute: ITU LIE Review Cases (6122)
# MAGIC %sql
# MAGIC create or replace temp view lie_review_cases_and_classes as
# MAGIC select
# MAGIC   filing_method_filed,
# MAGIC   count(distinct ep_ser_num) lie_review_itu_cases,
# MAGIC   count(distinct ep_ser_num, class) lie_review_itu_classes
# MAGIC from
# MAGIC   itu_lie_review a
# MAGIC     join bibliography b
# MAGIC       on a.ep_ser_num = b.ser_num
# MAGIC     join classes c
# MAGIC       on a.ep_ser_num = c.ser_num
# MAGIC where
# MAGIC   ep_tran_cd = '6122'
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   filing_method_filed;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   lie_review_cases_and_classes;

# COMMAND ----------

# DBTITLE 1,Compute: LIE Review First Action Credits
# MAGIC %sql
# MAGIC create or replace temp view lie_review_first_action_credits as
# MAGIC with action_credits_base as (
# MAGIC   select
# MAGIC     ep_ser_num,
# MAGIC     filing_method_filed,
# MAGIC     first(ep_actn_credit) ep_actn_credit
# MAGIC   from
# MAGIC     itu_lie_review a
# MAGIC       join bibliography b
# MAGIC         on a.ep_ser_num = b.ser_num
# MAGIC   where
# MAGIC     ep_tran_cd = '6122'
# MAGIC   group by
# MAGIC     all
# MAGIC )
# MAGIC select
# MAGIC   filing_method_filed,
# MAGIC   sum(ep_actn_credit) lie_review_itu_case_credits
# MAGIC from
# MAGIC   action_credits_base
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   filing_method_filed;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   lie_review_first_action_credits;

# COMMAND ----------

# DBTITLE 1,Compute: Net Exam Untimely AAU Cases and Classes
# MAGIC %sql
# MAGIC create or replace temp view aau_cases_and_classes as
# MAGIC with base_aau as (
# MAGIC   select distinct
# MAGIC     ep_ser_num,
# MAGIC     filing_method_filed,
# MAGIC     ep_actn_ct_dt,
# MAGIC     first(ep_actn_credit) over (
# MAGIC         partition by ep_ser_num, filing_method_filed, ep_actn_ct_dt, cfk_worker_gid
# MAGIC         order by fk_generating_prodvty_actn_id, fk_corrected_prodvty_actn_id nulls first
# MAGIC       ) ep_actn_credit
# MAGIC   from
# MAGIC     amendments
# MAGIC )
# MAGIC select
# MAGIC   filing_method_filed,
# MAGIC   count(ep_ser_num) amend_net_exam_untimely_aau_cases,
# MAGIC   sum(ep_actn_credit) amend_net_exam_untimely_aau_classes
# MAGIC from
# MAGIC   base_aau
# MAGIC group by
# MAGIC   filing_method_filed;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   aau_cases_and_classes;

# COMMAND ----------

# DBTITLE 1,Compute: SSR Net Queries Cases and Classes
# MAGIC %sql
# MAGIC create or replace temp view ssr_net_queries_cases_and_classes as
# MAGIC select
# MAGIC   filing_method_filed,
# MAGIC   count(distinct ser_num, ep_actn_ct_dt) ssr_net_queries_case_count,
# MAGIC   count(distinct ser_num, ep_actn_ct_dt, class) ssr_net_queries_class_count
# MAGIC from
# MAGIC   ssr_base a
# MAGIC     join classes b
# MAGIC       on a.ep_ser_num = b.ser_num
# MAGIC where
# MAGIC   ep_tran_cd = '9128'
# MAGIC group by
# MAGIC   filing_method_filed
# MAGIC order by
# MAGIC   filing_method_filed;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ssr_net_queries_cases_and_classes;

# COMMAND ----------

# DBTITLE 1,Compute: SSR Net Amends Cases and Classes
# MAGIC %sql
# MAGIC create or replace temp view ssr_net_amends_cases_and_classes as
# MAGIC select
# MAGIC   filing_method_filed,
# MAGIC   count(distinct ep_ser_num) ssr_amends_case_count,
# MAGIC   count(distinct ep_ser_num, class) ssr_amends_class_count
# MAGIC from
# MAGIC   ssr_base a
# MAGIC     join classes b
# MAGIC       on a.ep_ser_num = b.ser_num
# MAGIC group by
# MAGIC   filing_method_filed
# MAGIC order by
# MAGIC   filing_method_filed;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ssr_net_amends_cases_and_classes;

# COMMAND ----------

# DBTITLE 1,Compute: LIE
# MAGIC %sql
# MAGIC create or replace temp view lie as
# MAGIC with filing_methods as (
# MAGIC   select distinct
# MAGIC     filing_method_filed
# MAGIC   from
# MAGIC     bibliography
# MAGIC )
# MAGIC select
# MAGIC   a.filing_method_filed,
# MAGIC   nvl(b.amend_net_exam_untimely_aau_cases, 0) amend_net_exam_untimely_aau_cases,
# MAGIC   nvl(b.amend_net_exam_untimely_aau_classes, 0) amend_net_exam_untimely_aau_classes,
# MAGIC   nvl(d.ssr_net_queries_case_count, 0) ssr_net_queries_case_count,
# MAGIC   nvl(d.ssr_net_queries_class_count, 0) ssr_net_queries_class_count,
# MAGIC   nvl(e.ssr_amends_case_count, 0) ssr_amends_case_count,
# MAGIC   nvl(e.ssr_amends_class_count, 0) ssr_amends_class_count,
# MAGIC   nvl(f.lie_review_itu_cases, 0) lie_review_itu_cases,
# MAGIC   nvl(f.lie_review_itu_classes, 0) lie_review_itu_classes
# MAGIC from
# MAGIC   filing_methods a
# MAGIC     left join aau_cases_and_classes b
# MAGIC       on a.filing_method_filed = b.filing_method_filed
# MAGIC     left join ssr_net_queries_cases_and_classes d
# MAGIC       on a.filing_method_filed = d.filing_method_filed
# MAGIC     left join ssr_net_amends_cases_and_classes e
# MAGIC       on a.filing_method_filed = e.filing_method_filed
# MAGIC     left join lie_review_cases_and_classes f
# MAGIC       on a.filing_method_filed = f.filing_method_filed;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   lie;

# COMMAND ----------

# DBTITLE 1,Compute: EP70
# MAGIC %sql
# MAGIC create or replace temp view ep_seventy as
# MAGIC with base_action_type_credits as (
# MAGIC   select
# MAGIC     filing_method_filed,
# MAGIC     nvl(row_name, 'N/A') row_name,
# MAGIC     iff(ep_actn_num < 7, ep_actn_num, 7) action_column,
# MAGIC     sum(ep_actn_credit) ep_actn_credit
# MAGIC   from
# MAGIC     itu_lie_review a
# MAGIC       join bibliography b
# MAGIC         on a.ep_ser_num = b.ser_num
# MAGIC       left join ep_seventy_credit_type_definitions c
# MAGIC         on a.ep_tran_cd = c.ep_tran_cd
# MAGIC   where
# MAGIC     ep_actn_num > 0
# MAGIC   group by
# MAGIC     all
# MAGIC )
# MAGIC select
# MAGIC   a.filing_method_filed,
# MAGIC   a.row_name,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when action_column = 1 then ep_actn_credit
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) first_action,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when action_column = 2 then ep_actn_credit
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) second_action,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when action_column = 3 then ep_actn_credit
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) third_action,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when action_column = 4 then ep_actn_credit
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) fourth_action,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when action_column = 5 then ep_actn_credit
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) fifth_action,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when action_column = 6 then ep_actn_credit
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) sixth_action,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when action_column >= 7 then ep_actn_credit
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) greater_than_sixth_action,
# MAGIC   sum(ep_actn_credit) total_actions
# MAGIC from
# MAGIC   base_action_type_credits a
# MAGIC group by
# MAGIC   all
# MAGIC order by
# MAGIC   filing_method_filed,
# MAGIC   row_name;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ep_seventy;

# COMMAND ----------

# DBTITLE 1,Create: Sheets
petitions: DataFrame = spark.sql("select * from petitions").toPandas()
extensions: DataFrame = spark.sql("select * from extensions").toPandas()
sous: DataFrame = spark.sql("select * from sou").toPandas()
post_registrations: DataFrame = spark.sql("select * from post_registrations").toPandas()
disqualifying_events: DataFrame = spark.sql("select * from disqualified_events").toPandas()
disposals: DataFrame = spark.sql("select * from disposals").toPandas()
itu_prosecution_history: DataFrame = spark.sql("select * from itu_prosecution_history").toPandas()
itu_extensions: DataFrame = spark.sql("select * from itu_extensions").toPandas()
itu_abandonments: DataFrame = spark.sql("select * from itu_abandonments").toPandas()
appeal_brief: DataFrame = spark.sql("select * from appeal_brief").toPandas()
ep_seventy: DataFrame = spark.sql("select * from ep_seventy").toPandas()
lie: DataFrame = spark.sql("select * from lie").toPandas()

dataframe_sheets: list[tuple[pd.core.frame.DataFrame, str]] = [
    (petitions, "petitions"),
    (extensions, "extensions"),
    (sous, "sous"),
    (post_registrations, "post_registrations"),
    (disqualifying_events, "disqualifying_events"),
    (disposals, "disposals"),
    (itu_prosecution_history, "itu_prosecution_history"),
    (itu_extensions, "itu_extensions"),
    (itu_abandonments, "itu_abandonments"),
    (appeal_brief, "appeal_brief"),
    (ep_seventy, "ep_seventy"),
    (lie, "lie"),
]

# COMMAND ----------

# DBTITLE 1,Send Email
print("Sending email...")
send_mail(
    send_from=send_from,
    send_to=send_to,
    send_to_cc=send_to_cc,
    subject=subject,
    text=text,
    data_to_attach=dataframe_sheets,
    attachment_name=report_name,
)
print("Email sent.")

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
# Databricks notebook source
import tempfile
import os
from email.mime.base import MIMEBase
from email import encoders
import shutil
import time
import datetime
import pytz

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs["schema"]["trgt_catalog"]
src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
spark.conf.set('conf.dbx_env', dbx_env)
primary_email, cc_email = common_configs["alerting"]["sec8_partial_acceptance"]["email"], common_configs["alerting"]["sec8_partial_acceptance"]["cc"]
dq_catalog = common_configs['schema']['data_quality_catalog']

print(trgt_catalog, src_catalog, primary_email, cc_email)

# COMMAND ----------


# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_section_8_acceptance'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

df = spark.sql(f"""
with prosecution_history_event as (
  SELECT
    serial_number,
    ph_action_number,
    ph_action_code,
    cm_desc,
    ph_action_date,
    tm_worker_eid
  FROM
    {trgt_catalog}.silver.prosecution_history
  WHERE
    ph_action_code IN ('SP6A', '8PRT', '8.PR')
    and ph_action_date >= current_date - interval 7 days
),
mark as (
  select
    t.serial_num_tx,
    t.registration_num,
    coalesce(t.standard_character_tx, a.literal_element_tx, 'NO MARK LITERAL') mark_literal
  from
    {src_catalog}.bronze.trademark t
      left join {src_catalog}.bronze.tm_literal a
        on t.trademark_gid = a.fk_trademark_gid
),
active_or_cancelled_classes_with_og_action_dates as (
  SELECT
    substr(a.fk_trademark_gid, -8, 8) serial_no,
    concat_ws(
      ',',
      collect_list(DISTINCT
        case
          when b.tm_class_status_cd = '6' then c.class_no
          else NULL
        end
      )
    ) ACTIVE_CLASSES,
    concat_ws(
      ',',
      collect_list(DISTINCT
        case
          when b.tm_class_status_cd = 'B' then c.class_no
          else NULL
        end
      )
    ) SECTION_B_CANCELLED_CLASSES,
    concat_ws(',', collect_list(DISTINCT date_format(date(d.og_action_dt), "MM/dd/yyyy"))) OG_ACTION_DATE
  FROM
    {src_catalog}.bronze.TM_CLASS a
      INNER JOIN {src_catalog}.bronze.stnd_tm_class_status b
        ON a.fk_tm_class_status_cd = b.tm_class_status_cd
      INNER JOIN {src_catalog}.bronze.stnd_class c
        ON a.fk_class_id = c.class_id
      LEFT JOIN {src_catalog}.bronze.tm_publication d
        on a.fk_trademark_gid = d.fk_trademark_gid
  GROUP BY
    a.fk_trademark_gid
)
select
  m.registration_num `REGISTRATION NUMBER`,
  m.mark_literal `MARK LITERAL`,
  ad.ACTIVE_CLASSES `ACTIVE CLASSES`,
  ad.SECTION_B_CANCELLED_CLASSES `SECTION 8 CANCELLED CLASSES`,
   date_format(ph.ph_action_date, "MM/dd/yyyy") `REG - PARTIAL SEC. 8 (6-YR) ACCEPTED - PH DATE`,
  DECODE(AD.OG_ACTION_DATE, '','NO OG DATE', AD.OG_ACTION_DATE) `OG ACTION DATE`
from
  prosecution_history_event ph
    left join active_or_cancelled_classes_with_og_action_dates ad
      on ph.serial_number = ad.serial_no
    left join mark m
      on ph.serial_number = m.serial_num_tx""")
#display(df)

# COMMAND ----------

mailed_pdf = f"REG - PARTIAL SEC. 8 (6-YR) ACCEPTED.pdf"
title_tx_1 = """ Section 8 Partial Acceptance """
tm_analytics_image_loc = '../shared/tm_analytics.jpg'
uspto_image_loc = '../shared/uspto_logo.png'
from_addr= 'trademark_analytics@uspto.gov'

# COMMAND ----------

def pdf_prep(df,
            mailed_pdf,
            uspto_image_loc,
            title_tx_1,
            tm_analytics_image_loc,
             ):
    """This function instantiates class for pdf_prep, creates and saves temp space"""
    class PDF(FPDF):
        def header(self):
            # Logo
            self.image(uspto_image_loc, 10, 8, 33)
            # Arial bold 12
            self.set_font('helvetica', 'B', 12)
            # Move to the right
            self.cell(80)
            # Title
            self.cell(30, 10, title_tx_1, 0, 0, 'C')
            self.ln(10)
            self.cell(80)
            self.cell(30, 10,  "Run Date: "+str(date.today()), 0, 0, 'C')
            # Line break
            self.ln(20)
            self.image(tm_analytics_image_loc, 160, 6, 23)
            self.ln(10)
            self.set_font('helvetica', 'B', 6.6)
            self.set_text_color(0,0,0)

    pdf = PDF()
    # start creating ....
    pdf.add_page()
    navy_blue = (0,75,126) #fillcolor
    white =(255,255,255) #text color
    grey_scale = 200 # add alternating grey bands
    from fpdf.fonts import FontFace
    headings_style =FontFace(color=white,  fill_color=navy_blue)
    df_list= df_list= tuple([tuple(df.columns)] + [ row for row in df.itertuples(index=False, name=None)]) #combine column and data into tuple
    with pdf.table(text_align="CENTER", repeat_headings=1, headings_style=headings_style, cell_fill_color=grey_scale, cell_fill_mode="ROWS") as table:
        for  data_row in df_list:
            row = table.row()
            for dataum in data_row:
                row.cell(str(dataum).strip(" "))
    pdf.output(mailed_pdf)
    print("done with pdf")

# COMMAND ----------

df_pandas = df.toPandas()

# COMMAND ----------

if df.count() > 0:    
    parms = {}

    # Save the DataFrame to a temporary directory as a PDF file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath1 = f"{tmpdir}/{mailed_pdf}"
        from fpdf import FPDF
        from datetime import date
        pdf_prep(df_pandas,
                 filepath1,
                 uspto_image_loc,
                 title_tx_1,
                 tm_analytics_image_loc)
        
        attachments = [filepath1]
        
        email_subj = """See Attached Section 8 Partial Acceptance Report.pdf"""

        # Send the email with the attachment
        send_email_report(
            job_nm=job_name,
            subject=email_subj,
            send_from=from_addr,
            send_to=primary_email,
            send_to_cc= cc_email,
            html_body="",
            attachments=attachments
        )
else:
    print("No email notification sent")

# COMMAND ----------

df = df.withColumnRenamed("REGISTRATION NUMBER", "REGISTRATION_NUMBER")\
    .withColumnRenamed("MARK LITERAL", "MARK_LITERAL")\
    .withColumnRenamed("ACTIVE CLASSES", "ACTIVE_CLASSES")\
    .withColumnRenamed("SECTION 8 CANCELLED CLASSES", "SECTION_8_CANCELLED_CLASSES")\
    .withColumnRenamed("REG - PARTIAL SEC. 8 (6-YR) ACCEPTED - PH DATE", "PH_DATE")\
    .withColumnRenamed("OG ACTION DATE","OG_ACTION_DATE")

# COMMAND ----------

df.write.mode("overwrite").format("delta").insertInto(f"{trgt_catalog}.gold.sec_8_partial_acceptance")

# COMMAND ----------

recs_count = df.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
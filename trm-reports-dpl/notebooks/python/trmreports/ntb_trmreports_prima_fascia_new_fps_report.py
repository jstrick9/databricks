# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
#spark.catalog.clearCache()

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,function to generate random checkpoint folder name
def generate_64bit_ID()-> int:
    return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
CHK_POINT_DIR = "/tmp/checkpoints/post_reg_etl/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
import tempfile
import os
from email.mime.base import MIMEBase
from email import encoders
import shutil
from pyspark.sql.functions import col

common_configs = read_yaml(config_file)
#common_configs = read_yaml('/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl_prima/notebooks/config/dev/trmreports-conf.yaml')
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
receiver_email = common_configs['alerting']['prima_fascia']['email']
env = dbx_env.upper()

emailid = receiver_email
print(f"{trgt_catalog=},{src_catalog=},{emailid=},{altrx_schema=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

from fpdf import FPDF

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_prima_fascia_new_fps_report'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get silver Data

# COMMAND ----------

 #where ser_num in (90644165,90644191,90661136,90746108,90749489,90749169,90749176,90738127,90738957,90742439,90749187,90738962,90741962,90747725,90749195,90749201,90738968)

# COMMAND ----------

df_fpep_fact = spark.sql(f"""
    with fpep as (SELECT *, 
    CASE 
        WHEN month(COMPLETED_DT) > 9 THEN month(COMPLETED_DT) - 9
        ELSE month(COMPLETED_DT) + 3 
    END AS fy_month,
    date_format(COMPLETED_DT, 'MMMM') as month
FROM {trgt_catalog}.silver.fpep_fact 
where fp_id In("CFAM1","CFAM1-1","CFAM2","C56","C56-0","M5-0-3","Q29-24-2")
and action_count is null
),

ms as (select * from {trgt_catalog}.silver.milestone),

ij as (select ms.ser_num, ms.am_1_actn_ct_dt,ms.days_in_dock,
    fpep.CATEGORY,fpep.FK_FP_CATEGORY_ID,fpep.FK_FP_GROUP_ID,fpep.TITLE_TX,fpep.FP_YEAR,fpep.FK_WRKR_ID,1 as ACTION_COUNT,
    fpep.TRANSACTION_NO,fpep.TRANSACTIONAL_LITERAL,fpep.COMPLETED_DT,fpep.GROUP_NAME,fpep.FP_ID,fpep.COMPLETED_TS,fpep.TM_ANALYTICS_TS,fpep.fy_month,fpep.month
    from fpep inner join ms on ms.ser_num = fpep.ser_num and ms.am_1_actn_ct_dt = fpep.completed_dt),

l_aj as (select ms.ser_num, ms.am_1_actn_ct_dt,ms.days_in_dock, ms.first_action_dt_ph
    from ms left anti join fpep on ms.ser_num = fpep.ser_num and ms.am_1_actn_ct_dt = fpep.completed_dt),

r_aj as (select fpep.ser_num, fpep.CATEGORY,fpep.FK_FP_CATEGORY_ID,fpep.FK_FP_GROUP_ID,fpep.TITLE_TX,fpep.FP_YEAR,fpep.FK_WRKR_ID,1 as ACTION_COUNT,
    fpep.TRANSACTION_NO,fpep.TRANSACTIONAL_LITERAL,fpep.COMPLETED_DT,fpep.GROUP_NAME,fpep.FP_ID,fpep.COMPLETED_TS,fpep.TM_ANALYTICS_TS,fpep.fy_month,fpep.month
    from fpep left anti join ms on ms.ser_num = fpep.ser_num and ms.am_1_actn_ct_dt = fpep.completed_dt),

i_aj as (select l_aj.ser_num, l_aj.am_1_actn_ct_dt,l_aj.days_in_dock,
    r_aj.CATEGORY,r_aj.FK_FP_CATEGORY_ID,r_aj.FK_FP_GROUP_ID,r_aj.TITLE_TX,r_aj.FP_YEAR,r_aj.FK_WRKR_ID,1 as ACTION_COUNT,
    r_aj.TRANSACTION_NO,r_aj.TRANSACTIONAL_LITERAL,r_aj.COMPLETED_DT,r_aj.GROUP_NAME,r_aj.FP_ID,r_aj.COMPLETED_TS,r_aj.TM_ANALYTICS_TS,r_aj.fy_month,r_aj.month 
    from l_aj inner join r_aj on l_aj.ser_num = r_aj.ser_num and l_aj.first_action_dt_ph = r_aj.completed_dt),

outp as (select * from ij union select * from i_aj)

select fy_month, month, fp_id, TITLE_TX, action_count, count(ser_num) as count, count(distinct ser_num) as countdistinct_ser_num
    from outp
  group by fy_month, month, fp_id, TITLE_TX, action_count
  order by fy_month
""")
#df_fpep_fact.display()
#90644165,90644191

# COMMAND ----------

df_crosstab = df_fpep_fact.groupBy("fp_id", "TITLE_TX").pivot("month").sum("count").orderBy("fp_id")

from pyspark.sql.functions import when, col

month_order = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

df_crosstab = df_crosstab.select("fp_id", "TITLE_TX", *[col(month) for month in month_order if month in df_crosstab.columns])

df_crosstab_email = df_crosstab.select(
    [when(col(c).isNull(), "").otherwise(col(c)).alias(c) for c in df_crosstab.columns]
).withColumnRenamed("fp_id", "FP ID").withColumnRenamed("TITLE_TX", "TITLE")

display(df_crosstab_email)

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table
from textwrap import wrap
from matplotlib.offsetbox import OffsetImage, AnnotationBbox



# Convert Spark DataFrame to Pandas DataFrame
df_crosstab_pd = df_crosstab_email.toPandas()
df_crosstab_pd.style.hide(axis='index')

# Wrap text in TITLE column
df_crosstab_pd['TITLE'] = df_crosstab_pd['TITLE'].apply(lambda x: '\n'.join(wrap(x, width=20)))

# Plot the table
fig, ax = plt.subplots(figsize=(12, 4))  # set size frame
ax.xaxis.set_visible(False)  # hide the x axis
ax.yaxis.set_visible(False)  # hide the y axis
ax.set_frame_on(False)  # no visible frame, uncomment if size is ok

tab = ax.table(cellText=df_crosstab_pd.values, colLabels=df_crosstab_pd.keys(), loc='center')

#tab = table(ax, df_crosstab_pd, cellText=df_crosstab_pd.values,colLabels=df_crosstab_pd.keys(), loc='center', cellLoc='left') 

# Adjust column widths and heights to fit text
tab.auto_set_column_width(col=list(range(len(df_crosstab_pd.columns))))
tab.auto_set_font_size(False)  # Activate set fontsize manually
tab.set_fontsize(8)  # if ++fontsize is necessary ++colWidths
tab.scale(1.2, 1.2)  # Table size.

# Adjust row heights based on TITLE column value
renderer = fig.canvas.get_renderer()
for key, cell in tab.get_celld().items():
    if key[0] != 0:  # Skip header row
        title_text = df_crosstab_pd.iloc[key[0] - 1]['TITLE']
        wrapped_text = '\n'.join(wrap(title_text, width=20))
        cell_height = len(wrapped_text.split('\n')) * 0.05  # Adjust multiplier as needed
        cell.set_height(cell_height)

# Set table header row color
for key, cell in tab.get_celld().items():
    if key[0] == 0:  # Header row
        cell.set_facecolor((169/255, 169/255, 169/255))
    cell.set_edgecolor('white')  # Remove table borders

# Left align FP ID and TITLE columns
for key, cell in tab.get_celld().items():
    if key[1] in [0, 1]:  # FP ID and TITLE columns
        cell.set_text_props(ha='left')

# Color alternate rows with light grey
for key, cell in tab.get_celld().items():
    if key[0] != 0 and key[0] % 2 == 0:  # Skip header row and color alternate rows
        cell.set_facecolor((211/255, 211/255, 211/255))

# Add titles to the PDF
fig.suptitle(f"Prima Fascia Form Paragraph Usage Report\n\nRun Date: {starttime}", fontsize=14)

# Insert image on the left side of the title
uspto_image_loc = '../shared/uspto_logo.png'
#uspto_image_loc = '/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl_prima/notebooks/python/shared/uspto_logo.png'
logo = plt.imread(uspto_image_loc)
imagebox = OffsetImage(logo, zoom=0.33)
ab = AnnotationBbox(imagebox, (0.1, 1.1), frameon=False, xycoords='axes fraction', boxcoords="axes fraction", pad=0)
ax.add_artist(ab)

# Insert image on the right side of the title
tm_analytics_image_loc = '../shared/tm_analytics.jpg'
#tm_analytics_image_loc = '/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl_prima/notebooks/python/shared/tm_analytics.jpg'
tm_logo = plt.imread(tm_analytics_image_loc)
tm_imagebox = OffsetImage(tm_logo, zoom=0.13)
tm_ab = AnnotationBbox(tm_imagebox, (0.9, 1.1), frameon=False, xycoords='axes fraction', boxcoords="axes fraction", pad=0)
ax.add_artist(tm_ab)

# Save the table as a PDF aligned to the top of the page
pdf_path = f"/dbfs/tmp/{starttime}_Prima Fascia New Form Paragraph Usage.pdf"
plt.savefig(pdf_path, bbox_inches='tight', pad_inches=0.1)

# COMMAND ----------

from_addr= 'trademark_analytics@uspto.gov'
email_subj = f'Prima Fascia Form Paragraph Usage Report {env}' 
body = "<strong>Prima Fascia New Form Paragraph Usage Report</strong>"
pdf_path = f"/dbfs/tmp/{starttime}_Prima Fascia New Form Paragraph Usage.pdf"

attachments = [pdf_path]

# Send the email with the attachment
send_email_report(
    job_nm = job_name,
    subject = email_subj,
    send_from = from_addr,
    send_to = emailid,
    html_body= body,
    attachments = attachments
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Send data in email attachment

# COMMAND ----------

df_crosstab.write.mode("overwrite").format("delta").insertInto(f"{trgt_catalog}.gold.prima_fascia_form_paragraph_usage_report")

# COMMAND ----------

# data quality entry
#tbl2 = f"{trgt_catalog}.gold.prima_fascia_form_paragraph_usage_report"
#tbl1 = f"hive_metastore.{altrx_schema}.prima_fascia_form_paragraph_usage_report"
#key_cols = ['fp_id', 'title_tx']
 
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

recs_count = df_crosstab.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

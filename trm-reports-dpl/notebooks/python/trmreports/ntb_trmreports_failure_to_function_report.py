# Databricks notebook source
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
receiver_email = common_configs['alerting']['pull_metrics_on_failure']['email']
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
job_name = 'ntb_trmreports_failure_to_function_report'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get silver Data

# COMMAND ----------

df_fpep_fact = spark.sql(f"""
select Category, count(*) as count, completed_year as year, month_year as month
from(SELECT *, date(completed_dt) as date, year(completed_dt) as completed_year,
date_format(completed_dt,'yyyy-MM') as month_year
FROM {trgt_catalog}.silver.fpep_fact
WHERE contains(CATEGORY,"Failure to Function")
)
group by  Category, completed_year, month_year
order by case when CATEGORY like '%Other' then 1 else 2 end , Category, completed_year, month_year
""")
#df_fpep_fact.display()

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table
from textwrap import wrap
from matplotlib.offsetbox import OffsetImage, AnnotationBbox


# Convert Spark DataFrame to Pandas DataFrame
df_crosstab_pd = df_fpep_fact.toPandas()
df_crosstab_pd.style.hide(axis='index')


# Plot the table
fig, ax = plt.subplots(figsize=(10, 3))  # set size frame
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


# Set table header row color
for key, cell in tab.get_celld().items():
    if key[0] == 0:  # Header row
        cell.set_facecolor((169/255, 169/255, 169/255))
    cell.set_edgecolor('white')  # Remove table borders

# Left align Category olumns
for key, cell in tab.get_celld().items():
    if key[1] in [0]:  
        cell.set_text_props(ha='left')

# Color alternate rows with light grey
for key, cell in tab.get_celld().items():
    if key[0] != 0 and key[0] % 2 == 0:  # Skip header row and color alternate rows
        cell.set_facecolor((211/255, 211/255, 211/255))

# Add titles to the PDF
fig.suptitle(f"Failure to Function Report", fontsize=14)


# Save the table as a PDF aligned to the top of the page
pdf_path = f"/dbfs/tmp/{starttime}_Failure to Function Report.pdf"
plt.savefig(pdf_path, bbox_inches='tight', pad_inches=0.1)

# COMMAND ----------

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Email details
from_addr= 'trademark_analytics@uspto.gov'
#to_addr = 'pawanpreet.sangari@uspto.gov'
to_addr = emailid
subject = "Failure to Function Report Report"
#html_table = df_crosstab_pd.to_html(index=False)
body = f"<h2>Attached report of failure to function pdf document.</h2> \
    <p>Run Date: {starttime}</p>"
#         {html_table}"

# Create the email
msg = MIMEMultipart()
msg['From'] = from_addr
msg['To'] = to_addr
msg['Subject'] = subject

# Attach the body with the msg instance
msg.attach(MIMEText(body, 'html'))

# Attach the PDF file
pdf_path = f"/dbfs/tmp/{starttime}_Failure to Function Report.pdf"
with open(pdf_path, "rb") as f:
    attach = MIMEApplication(f.read(), _subtype="pdf")
    attach.add_header('Content-Disposition', 'attachment', filename=pdf_path.split('/')[-1])
    msg.attach(attach)

# Send the email
#server = smtplib.SMTP('smtp.uspto.gov', 25)
#server.sendmail(from_addr, to_addr, msg.as_string())

attachments = [pdf_path]
        
notify = Notify()

# Convert the list to a tuple before passing it to the function
msg = notify.compose_email_attach(
            body, 
            f'Failure to Function Report ' + env, 
            to_addr,
            from_addr,
            {},
            attachments
        )
        
notify.send_mail(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Send data in email attachment

# COMMAND ----------

df_fpep_fact.write.mode("overwrite").format("delta").saveAsTable(f"{trgt_catalog}.gold.failure_to_function_report")

# COMMAND ----------

# data quality entry
#tbl2 = f"{trgt_catalog}.gold.failure_to_function_report"
#tbl1 = f"hive_metastore.{altrx_schema}.failure_to_function_report"
#key_cols = ['Category, year, month']
 
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

recs_count = df_fpep_fact.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

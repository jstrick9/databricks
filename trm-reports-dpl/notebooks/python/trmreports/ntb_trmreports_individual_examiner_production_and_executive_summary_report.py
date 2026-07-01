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
from fpdf import FPDF

common_configs = read_yaml(config_file)
#common_configs = read_yaml('/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl_prima/notebooks/config/dev/trmreports-conf.yaml')
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmproceeding_catalog = common_configs['schema']['tmproceeding_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
receiver_email = common_configs['alerting']['individual_examiner']['email']
env = dbx_env.upper()

emailid = receiver_email
print(f"{trgt_catalog=},{src_catalog=},{emailid=},{altrx_schema=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_individual_examiner_production_and_executive_summary_report'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get silver Data

# COMMAND ----------

df_role = spark.sql(f"""
select  count(*) as count, fk_prcdng_employee_role_cd as employee_role
 from {src_catalog}.bronze.prcdng_employee_assignment pea
inner join {tmproceeding_catalog}.bronze.proceeding_mark pm
on pea.cfk_proceeding_gid = pm.fk_proceeding_gid
group by fk_prcdng_employee_role_cd
order by fk_prcdng_employee_role_cd
""")

df_employee = spark.sql(f"""
select  count(*) as count, cfk_employee_no as employee_number
 from {src_catalog}.bronze.prcdng_employee_assignment pea
inner join {tmproceeding_catalog}.bronze.proceeding_mark pm
on pea.cfk_proceeding_gid = pm.fk_proceeding_gid
group by cfk_employee_no
order by cfk_employee_no
""")



# COMMAND ----------

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

# Convert Spark DataFrames to Pandas DataFrames
df_role_pd = df_role.toPandas()
df_employee_pd = df_employee.toPandas()

# Create a PDF file
pdf_path = f"/dbfs/tmp/{starttime}_Individual Examiner Production and Executive Summary Reports.pdf"
with PdfPages(pdf_path) as pdf:
    # First figure
    fig1, ax1 = plt.subplots(figsize=(10, 3))
    ax1.xaxis.set_visible(False)
    ax1.yaxis.set_visible(False)
    ax1.set_frame_on(False)
    tab1 = ax1.table(cellText=df_role_pd.values, colLabels=df_role_pd.keys(), loc='center')
    tab1.auto_set_column_width(col=list(range(len(df_role_pd.columns))))
    tab1.auto_set_font_size(False)
    tab1.set_fontsize(8)
    tab1.scale(1.2, 1.2)
    for key, cell in tab1.get_celld().items():
        if key[0] == 0:
            cell.set_facecolor((169/255, 169/255, 169/255))
        if key[0] != 0 and key[0] % 2 == 0:
            cell.set_facecolor((211/255, 211/255, 211/255))
    fig1.suptitle(f"Individual Examiner Production and Executive Summary Reports E&R\n\nRun Date: {starttime}", fontsize=14)
    pdf.savefig(fig1)
    plt.close(fig1)
    
    # Second figure (multiple pages)
    rows_per_page = 10
    num_pages = (len(df_employee_pd) // rows_per_page) + 1
    for page in range(num_pages):
        fig2, ax2 = plt.subplots(figsize=(10, 3))
        ax2.xaxis.set_visible(False)
        ax2.yaxis.set_visible(False)
        ax2.set_frame_on(False)
        start_row = page * rows_per_page
        end_row = start_row + rows_per_page
        tab2 = ax2.table(cellText=df_employee_pd.iloc[start_row:end_row].values, colLabels=df_employee_pd.keys(), loc='center')
        tab2.auto_set_column_width(col=list(range(len(df_employee_pd.columns))))
        tab2.auto_set_font_size(False)
        tab2.set_fontsize(8)
        tab2.scale(1.2, 1.2)
        for key, cell in tab2.get_celld().items():
            if key[0] == 0:
                cell.set_facecolor((169/255, 169/255, 169/255))
            if key[0] != 0 and key[0] % 2 == 0:
                cell.set_facecolor((211/255, 211/255, 211/255))
        fig2.suptitle(f"Individual Examiner Production and Executive Summary Reports E&R", fontsize=14)
        pdf.savefig(fig2)
        plt.close(fig2)
# Save the table as a PDF aligned to the top of the page

#plt.savefig(pdf_path, bbox_inches='tight', pad_inches=0.1)

# COMMAND ----------

#env='DEV'

# COMMAND ----------

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Email details
from_addr= 'trademark_analytics@uspto.gov'
to_addr = emailid
subject = "Individual Examiner Production and Executive Summary Reports E&R"
#html_table = df_crosstab_pd.to_html(index=False)
body = f"<h2>Individual Examiner Production and Executive Summary Reports E&R</h2> \
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
pdf_path = f"/dbfs/tmp/{starttime}_Individual Examiner Production and Executive Summary Reports.pdf"
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
            f'Individual Examiner Production and Executive Summary Reports E&R ' + env, 
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

df_role.write.mode("overwrite").format("delta").saveAsTable(f"{trgt_catalog}.gold.indvdl_exm_prod_and_exc_sum_report_emp_role")

df_employee.write.mode("overwrite").format("delta").saveAsTable(f"{trgt_catalog}.gold.indvdl_exm_prod_and_exc_sum_report_emp_num")

# COMMAND ----------

# data quality entry
tbl2 = f"{trgt_catalog}.gold.indvdl_exm_prod_and_exc_sum_report_emp_role"
tbl1 = f"hive_metastore.{altrx_schema}.indvdl_exm_prod_and_exc_sum_report_emp_role"
key_cols = ['employee_role']
 
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

# data quality entry
#tbl2 = f"{trgt_catalog}.gold.indvdl_exm_prod_and_exc_sum_report_emp_num"
#tbl1 = f"hive_metastore.{altrx_schema}.indvdl_exm_prod_and_exc_sum_report_emp_num"
#key_cols = ['employee_number']
 
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

recs_count = df_employee.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unit test cells under

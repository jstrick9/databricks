# Databricks notebook source
# MAGIC %pip install xlsx2html==0.6.3
# MAGIC %pip install fpdf==1.7.2
# MAGIC %pip install xlsxwriter==3.2.0
# MAGIC %pip install jinja2==3.1.4

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
import tempfile
import os
from email.mime.base import MIMEBase
from email import encoders
import shutil
from pyspark.sql.functions import col
import io
from xlsx2html import xlsx2html
from fpdf import FPDF
from datetime import date, datetime, timedelta

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("rundate","")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
rundate = dbutils.widgets.get("rundate").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl-tm-expired_prod_fix/notebooks/config/dev/trmreports-conf.yaml"
print(f'{config_file=}')


# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,function to generate random checkpoint folder name


def generate_64bit_ID()-> int:
    return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
CHK_POINT_DIR = "/tmp/checkpoints/noa_report/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR


# COMMAND ----------

common_configs = read_yaml(config_file)
trm_reporting_catalog = common_configs['schema']['trm_reporting_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
receiver_email = common_configs['alerting']['noa_report']['email']
receiver_cc = common_configs['alerting']['noa_report']['cc']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
env = dbx_env.upper()

emailid = receiver_email
print(f"{trm_reporting_catalog=},{src_catalog=},{emailid=}, {dq_catalog=}, {altrx_schema=}")
spark.conf.set('conf.catalog', trm_reporting_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_trmreports_noa_report'

control_dt = begin_job_cntl(f'{trm_reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

today = date.today()
if today.month >= 10:
    fiscal_year_start = date(today.year, 10, 1)
    fiscal_year_end = date(today.year + 1, 9, 30)
else:
    fiscal_year_start = date(today.year - 1, 10, 1)
    fiscal_year_end = date(today.year, 9, 30)

df_spark = spark.sql(
    f"""
    SELECT
      noa_dt as `NOA Date`,
      count(distinct ser_num) as Cases,
      sum(am_cls_ct_actv) as Classes
    FROM {trm_reporting_catalog}.silver.milestone
    WHERE noa_dt >= date('{fiscal_year_start}')
      AND noa_dt <= date('{fiscal_year_end}')
    GROUP BY noa_dt
    ORDER BY noa_dt DESC
    """
)

# COMMAND ----------

#df_spark.display()

# COMMAND ----------


df_spark_table = (
    df_spark
    .withColumnsRenamed({'NOA Date': 'noa_date'})
    .filter((col('noa_date') >= lit(fiscal_year_start)) & (col('noa_date') <= lit(fiscal_year_end)))
)
display(df_spark_table)

# COMMAND ----------

mailed_pdf = f"Notice of Allowance (NOA) Report {date.today()}.pdf"
mailed_excel = f"Notice of Allowance (NOA) Report {date.today()}.xlsx"
title_tx_1 = """ Notice of Allowance (NOA) Report"""
title_tx_2 = """  """
title_tx_3 = f"""Run Date:  {date.today()}"""
footer_tx_1  = """"""
footer_tx_2  = """ """
footer_tx_3  = """ """
footer_excel_tx  = " "
excel_string_1 ="""Notice of Allowance (NOA) Report"""
excel_title_tx = """ """
excel_string_2 = f"""Run Date: {date.today()}"""
data_col_1 = 'NOA Date'
data_col_2 = 'Cases'
data_col_3 = 'Classes'
tm_analytics_image_loc = '../shared/tm_analytics.jpg'
#../shared/tm_analytics.jpg
uspto_image_loc = '../shared/uspto_logo.png'
#../shared/uspto_logo.png'
from_addr= 'trademark_analytics@uspto.gov'

# COMMAND ----------


def pdf_prep(df,
            mailed_pdf,
            uspto_image_loc,
            title_tx_1,
            title_tx_2,
            title_tx_3,
            tm_analytics_image_loc,
            data_col_1,
            data_col_2,
            data_col_3,
            #footer_tx_1,
            #footer_tx_2,
            #footer_tx_3
             ):
    """This function instantiates class for pdf_prep, creates and saves temp space"""
    class PDF(FPDF):
        def header(self):
            # Logo
            self.image(uspto_image_loc, 10, 8, 33)
            # Arial bold 12
            self.set_font('Arial', 'B', 12)
            # Move to the right
            self.cell(80)
            # Title
            self.cell(30, 10, title_tx_1, 0, 0, 'C')
            self.ln(5)
            self.cell(80)
            self.cell(30, 10, title_tx_2.format(date.today()), 0, 0, 'C')
            self.ln(5)
            self.cell(80)
            #self.cell(30, 10,  str(date.today()), 0, 0, 'C')
            # Line break
            self.ln(20)
            self.image(tm_analytics_image_loc, 160, 6, 23)
            self.set_font('Arial', 'I', 12)
            self.cell(0, 10, title_tx_3.format(date.today()), 0, 0, 'C')
            self.ln(10)
            self.set_font('Arial', 'B', 10)
            self.set_fill_color(0,75,126)
            self.set_text_color(255,255,255)
            cell_width = [65,65,65]
            self.cell(cell_width[0],5,data_col_1,1,0,align='C',fill=True)
            self.cell(cell_width[1],5, data_col_2,1,0,align='C',fill=True)
            self.cell(cell_width[2],5, data_col_3,1,0,align='C',fill=True)
            self.ln(h = 5)
            self.set_font('Arial', '', 10)
            self.set_text_color(0,0,0)

        # Page footer
        def footer(self):
            # Position at 2 cm from bottom
            self.set_y(-20)
            # Arial italic 6
            self.set_font('Arial', 'I', 6)
            # gradually add footer text with left margin
            self.cell(0, 10, footer_tx_1 , 0, 0, 'L')
            self.ln(5)
            self.cell(0, 10, footer_tx_2 , 0, 0, 'L')
            self.ln(5)
            self.cell(0, 10, footer_tx_3 , 0, 0, 'L')

    cell_width=[65,65,65]
    # Instantiation of inherited class
    pdf = PDF()
    # start creating ....
    pdf.add_page()
    # get count of table
    row_size = df.shape[0]
    for i in range(0, row_size):
        pdf.cell(cell_width[0],5,str(df.loc[i,'NOA Date']),1,0,align='C')
        pdf.cell(cell_width[0],5,str(df.loc[i,'Cases']),1,0,align='C')
        pdf.cell(cell_width[0],5,str(df.loc[i,'Classes']),1,0,align='C')
        pdf.ln(h = 5) # add 5 mm space before next
    pdf.output(mailed_pdf, 'F')
    print("done with pdf")

# COMMAND ----------

# Turn off the default header and skip one row to allow us to insert a
# user defined header.
# Create a Pandas Excel writer using XlsxWriter as the engine.
def excel_prep(df,
                mailed_excel,
               uspto_image_loc,
               tm_analytics_image_loc,
               excel_string_1,
               excel_string_2, excel_title_tx, data_col_1,
               data_col_2,
               data_col_3,
               #footer_excel_tx
               ) -> None:
    """This function generates prepared excel document with headers, styling and footer per user preference"""
    import datetime
    import pandas as pd

    properties = {"border": "1px solid grey",  "text-align": "center", "font-size" : "20px"}

    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(mailed_excel, engine='xlsxwriter')
    df.style.set_properties(**properties).to_excel(writer, sheet_name='Sheet1', startrow=3, index=False, header=True)
    # Get the xlsxwriter worksheet and workbook objects.
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']

    # Add a header format.
    merge_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'align': 'center',
         'font_size': 15})
    
    # Add a footer format.
    footer_format = workbook.add_format({
       # 'border':   6,
       # 'text_wrap': True,
       # 'align':    'center',
       # 'font_size': 20,
        })

    # Insert an image and set column and row heights
    worksheet.set_column(0, 2, 40)
    worksheet.set_row(0, 40)
    worksheet.merge_range("A1:A2", '', merge_format)
    worksheet.insert_image('A1', uspto_image_loc, {'x_scale': 1.0, 'y_scale': 1.0, 'object_position': 1, "x_offset": 60, "y_offset": 20})
    # write strings between
    worksheet.write_string('B1', excel_string_1, merge_format)
    worksheet.write_string('B2', excel_string_2, merge_format)
    worksheet.merge_range("C1:C2", '', merge_format)
    worksheet.insert_image('C1', tm_analytics_image_loc, {'x_scale': 0.5, 'y_scale': 0.225, 'object_position': 1,"x_offset": 65, "y_offset": 3})
    
    worksheet.merge_range("A3:C3", excel_title_tx, merge_format)
    # Add a header format.
    header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
     'font_size' : 20,
     'font_color' : 'white',
    'valign': 'top',
    'align': 'center',
    'fg_color': '#154468',
    'border': 1})

   # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(3, col_num, value, header_format)
    #worksheet.set_row(df.shape[0] + 4, 120)
    # write merge range text into box
    #worksheet.merge_range(f"A{df.shape[0] + 5}:C{df.shape[0] + 5}", footer_excel_tx, footer_format)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    print("done with excel")

# COMMAND ----------

if df_spark.count() > 0:    
    parms = {}

    # Save the DataFrame to a temporary directory as an Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath1 = f"{tmpdir}/{mailed_pdf}"
        filepath2 = f"{tmpdir}/{mailed_excel}"

        df = df_spark.toPandas()
        pdf_prep(df,
            filepath1,
            uspto_image_loc,
            title_tx_1,
            title_tx_2,
            title_tx_3,
            tm_analytics_image_loc,
            data_col_1,
            data_col_2,
            data_col_3,
            #footer_tx_1,
            #footer_tx_2,
            #footer_tx_3
             )
        
       
        import pandas as pd
        writer = pd.ExcelWriter(filepath2, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', startrow=3, index=False, header=True)
        writer.close()

        xlsx_file = open(filepath2, 'rb')
        out_file = io.StringIO()
        xlsx2html(xlsx_file, out_file, locale='en')
        out_file.seek(0)
        result_html = out_file.read()

        attachments = [filepath1, filepath2]
        
        email_subj = """See Attached Notice of Allowance Report"""

        import datetime
        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to_cc= receiver_cc,
            send_to = emailid,
            html_body= result_html,
            attachments = attachments
        )
else:
    print("No email notification sent")

# COMMAND ----------

df_spark_table.write.mode("overwrite").format("delta").insertInto(f"{trm_reporting_catalog}.gold.noa_email_report")

# COMMAND ----------

#############################################################################################
# 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
# Allows disabling Alteryx workflow schedule fully, saving resources.                       #
#############################################################################################


# # # data quality entry
# tbl1 = f"{trm_reporting_catalog}.gold.tm_expired_registrations"
# tbl2 = f"hive_metastore.{altrx_schema}.tm_expired_registrations"
# key_cols = ['registration_number', 'serial_number', 'registration_date']
# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
# print(dq_result)

# COMMAND ----------

# end job control
recs_count = df_spark_table.count()
end_job_cntl(f"{trm_reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")

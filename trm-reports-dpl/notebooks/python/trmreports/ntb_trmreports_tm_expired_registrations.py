# Databricks notebook source
# MAGIC %pip install xlsx2html==0.6.3
# MAGIC %pip install fpdf2

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

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("rundate","")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
rundate = dbutils.widgets.get("rundate").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')




# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,function to generate random checkpoint folder name


def generate_64bit_ID()-> int:
    return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
CHK_POINT_DIR = "/tmp/checkpoints/tm_expired_reg/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR


# COMMAND ----------

common_configs = read_yaml(config_file)
trm_reporting_catalog = common_configs['schema']['trm_reporting_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
receiver_email = common_configs['alerting']['tm_expired_registration']['email']
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
job_name = 'ntb_trmreports_tm_expired_registrations'

control_dt = begin_job_cntl(f'{trm_reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

from fpdf import FPDF
from datetime import date, datetime, timedelta

# Determine today's date
today = datetime.today() #- timedelta(days=5)

if rundate == '':
  #rdate = datetime.datetime.now().strftime('%Y%m%d')
  # Check if today is Friday
  if today.weekday() == 4:  # 4 corresponds to Friday
    rdate = today.strftime('%Y%m%d')
  else:
    # Calculate last Friday's date
    last_friday = today - timedelta(days=(today.weekday() + 2) % 7 + 1)
    rdate = last_friday.strftime('%Y%m%d')
else:
  rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d').strftime('%Y%m%d')
print(rdate)

# COMMAND ----------

df_spark = spark.sql(f"""select registration_num `Registration Number`, serial_num_tx `Serial Number`, registration_dt `Registration Date`
                            from {src_catalog}.bronze.trademark
                            join {src_catalog}.bronze.tm_milestone on trademark_gid = fk_trademark_gid
                            join {trm_reporting_catalog}.silver.milestone on ser_num = serial_num_tx 
                            join {src_catalog}.bronze.tmcom_batch_ingest_control on serial_num = serial_num_tx
                            where tm_milestone.FK_TM_MILESTONE_CD = 'REG' and batch_nm = 'PR85'
                            and '{rdate}' = BATCH_DT_NO
                            """)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

df_spark_table = df_spark.withColumnsRenamed({'Registration Number' :'registration_number',
                                               'Serial Number' :  'serial_number',
                                               'Registration Date' : 'registration_date'})

# COMMAND ----------

#df_spark.display()

# COMMAND ----------

mailed_pdf = f"Expired TM Registrations Report {date.today()}.pdf"
mailed_excel = f"Expired TM Registrations Report {date.today()}.xlsx"
title_tx_1 = """ Notice of Expiration of Trademark Registration"""
title_tx_2 = """  Due To Failure to Renew"""
title_tx_3 = """The trademark registrations below are expired due to failure to renew in accordance with 15 U.S.C.1059."""
footer_tx_1  = """15 U.S.C. 1059 provides that each trademark registration may be renewed for periods of ten years from the end of the expiring period upon payment of the prescribed fee """
footer_tx_2  = """ and the filing of an acceptable application for renewal. This may be done at any time within one year before expiration of the period for which the registration was issued"""
footer_tx_3  = """ or renewed,or it may be done within six months after such expiration on payment of an additional fee ,or it may be done within six months after such expiration on payment of an additional fee."""
footer_excel_tx  = """15 U.S.C. 1059 provides that each trademark registration may be renewed for periods of ten years from the end of the expiring period upon payment of the prescribed fee and the filing of an acceptable application for renewal. This may be done at any time within one year before expiration of the period for which the registration was issued or renewed,or it may be done within six months after such expiration on payment of an additional fee ,or it may be done within six months after such expiration on payment of an additional fee."""
excel_string_1 ="""Notice of Expiration of Trademark Registration"""
excel_title_tx = """The trademark registrations below are expired due to failure to renew in accordance with 15 U.S.C.1059."""
excel_string_2 = f"""Due To Failure to Renew  \n {date.today()}"""
data_col_1 = 'Registration Number'
data_col_2 = 'Serial Number'
data_col_3 = 'Registration Date'
tm_analytics_image_loc = '../shared/TM_DnA_Logo.jpg'
uspto_image_loc = '../shared/uspto_logo.png'
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
            footer_tx_1,
            footer_tx_2,
            footer_tx_3
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
            self.cell(30, 10,  str(date.today()), 0, 0, 'C')
            # Line break
            self.ln(10)
            self.image(tm_analytics_image_loc, x=163, y=8, w=33, h=12)
            self.ln(10)
            self.set_font('Arial', 'I', 12)
            self.cell(0, 10, title_tx_3.format(date.today()), 0, 0, 'C')
            self.ln(1)  # Reduce line break (was 20)
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
        pdf.cell(cell_width[0],5,str(df.loc[i,'Registration Number']),1,0,align='C')
        pdf.cell(cell_width[0],5,str(df.loc[i,'Serial Number']),1,0,align='C')
        pdf.cell(cell_width[0],5,str(df.loc[i,'Registration Date']),1,0,align='C')
        pdf.ln(h = 5) # add 5 mm space before next
    pdf.output(mailed_pdf)
    print("done with pdf")



# COMMAND ----------

def excel_prep(df,
                mailed_excel,
               uspto_image_loc,
               tm_analytics_image_loc,
               excel_string_1,
               excel_string_2, excel_title_tx, data_col_1,
               data_col_2,
               data_col_3,
               footer_excel_tx
               ) -> None:
    """This function generates prepared excel document with headers, styling and footer per user preference"""
    import datetime
    import pandas as pd

    properties = {"border": "1px solid grey",  "text-align": "center", "font-size" : "20px"}

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(mailed_excel, engine='xlsxwriter')
    df.style.set_properties(**properties).to_excel(writer, sheet_name='Sheet1', startrow=4, index=False, header=True)
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
        'border':   6,
        'text_wrap': True,
        'align':    'center',
        'font_size': 20,
        })

    # Insert an image and set column and row heights
    worksheet.set_column(0, 2, 45)
    worksheet.set_row(0, 40)
    worksheet.set_row(1, 40)
    worksheet.merge_range("A1:A2", '', merge_format)
    worksheet.insert_image('A1', uspto_image_loc, {'x_scale': 0.8, 'y_scale': 0.8, 'object_position': 1, "x_offset": 90, "y_offset": 40})
    # write strings between
    worksheet.write_string('B1', excel_string_1, merge_format)
    worksheet.write_string('B2', excel_string_2, merge_format)
    worksheet.merge_range("C1:C2", '', merge_format)
    # Insert tm_analytics_image_loc with smaller size and more centered
    worksheet.insert_image('C1', tm_analytics_image_loc, {'x_scale': 0.07, 'y_scale': 0.07, 'object_position': 1, "x_offset": 80, "y_offset": 40})  # reduced size
    
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
        worksheet.write(4, col_num, value, header_format)
    worksheet.set_row(df.shape[0] + 5, 120)
    # write merge range text into box
    worksheet.merge_range(f"A{df.shape[0] + 6}:C{df.shape[0] + 6}", footer_excel_tx, footer_format)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    print("done with excel")

# COMMAND ----------

import tempfile
import os
import io
#from xlsx2html import xlsx2html
if df_spark_table.count() > 0:    
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
            footer_tx_1,
            footer_tx_2,
            footer_tx_3
             )
        
        excel_prep(df,
               filepath2,
               uspto_image_loc,
               tm_analytics_image_loc,
               excel_string_1,
               excel_string_2, excel_title_tx, data_col_1,
               data_col_2,
               data_col_3,
               footer_excel_tx
               )


        xlsx_file = open(filepath2, 'rb')
        out_file = io.StringIO()
        xlsx2html(xlsx_file, out_file, locale='en')
        out_file.seek(0)
        result_html = out_file.read()

        attachments = [filepath1, filepath2]
        
        email_subj = """See Attached Notice of Expiration of Trademark Registrations Due To Failure to Renew (2026-01-23)"""

        # Send the email with the attachment
        import datetime
        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to = emailid,
            html_body= result_html,
            attachments = attachments
        )
else:
    print("No email notification sent")

# COMMAND ----------

df_spark_table.write.mode("overwrite").format("delta").insertInto(f"{trm_reporting_catalog}.gold.tm_expired_registrations")

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

# Databricks notebook source
# !pip install xlsx2html
# !pip install xlsxwriter
# !pip install jinja2

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
CHK_POINT_DIR = "/tmp/checkpoints/madrid_transformation_and_replacement/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR


# COMMAND ----------

common_configs = read_yaml(config_file)
trm_reporting_catalog = common_configs['schema']['trm_reporting_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
receiver_email = common_configs['alerting']['madrid_tranformation_and_replacement']['email']
env = dbx_env.upper()

emailid = receiver_email
print(f"{trm_reporting_catalog=},{src_catalog=},{emailid=}")
spark.conf.set('conf.catalog', trm_reporting_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_trmreports_madrid_replacements_and_transformations'
control_dt = begin_job_cntl(f'{trm_reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

from datetime import date
# allows us to backfill in case of pipeline failures for entered date
if rundate == '':
  rdate = datetime.datetime.now().strftime('%Y-%m-%d')
else:
  rdate = rundate
print('rundate = ' + str(rdate))

# COMMAND ----------

df_spark_transformations = spark.sql(f"""
with mx_order as (
select be.cfk_object_gid, max(be.order_no) order_no from  {src_catalog}.bronze.business_event be group by be.cfk_object_gid
),
be_data as (
  select * from {src_catalog}.bronze.business_event be inner join {src_catalog}.bronze.STND_BUSINESS_EVENT_REASON BER ON BE.fk_business_event_reason_id = BER.Business_event_reason_id
),
business_data as (
select mo.cfk_object_gid, be.effective_ts LAST_CM_DATE, BER.title_tx CM_TYPE, th.legacy_status_cd AM_STAT,th.status_dt AM_STAT_DT, sl.description_tx AM_TYPE
from   {src_catalog}.bronze.business_event be inner join 
mx_order mo on be.cfk_object_gid = mo.cfk_object_gid and be.order_no = mo.order_no
INNER JOIN {src_catalog}.bronze.STND_BUSINESS_EVENT_REASON BER ON BE.fk_business_event_reason_id = BER.Business_event_reason_id
INNER JOIN {src_catalog}.bronze.trademark  th on  be.cfk_object_gid = th.trademark_gid
INNER JOIN {src_catalog}.bronze.stnd_legacy_status sl on sl.status_no = th.legacy_status_cd
)
select B.cfk_object_gid `Serial Number`, 
       B.effective_ts `Transformation Date`,  
       B.legacy_cm_ent_cd `ENT Code`, 
       A.LAST_CM_DATE `Last CM Date`, 
       A.CM_TYPE `Last CM`, 
       A.AM_STAT_DT `AM STAT Date`, 
       A.AM_STAT `AM STAT`, 
       A.AM_TYPE AM
        from business_data A inner join  be_data B  on A.cfk_object_gid = b.cfk_object_gid
where B.legacy_cm_ent_cd in ('ERFT', 'TRFL') and date(b.effective_ts)  between   date('{rdate}') - 13 and date('{rdate}')
""")

# COMMAND ----------

df_spark_replacements = spark.sql(f"""
with mx_order as (
select be.cfk_object_gid, max(be.order_no) order_no from  {src_catalog}.bronze.business_event be group by be.cfk_object_gid
),
be_data as (
  select * from {src_catalog}.bronze.business_event be inner join {src_catalog}.bronze.STND_BUSINESS_EVENT_REASON BER ON BE.fk_business_event_reason_id = BER.Business_event_reason_id
),
business_data as (
select mo.cfk_object_gid, be.effective_ts LAST_CM_DATE, BER.title_tx CM_TYPE, th.legacy_status_cd AM_STAT,th.status_dt AM_STAT_DT, sl.description_tx AM_TYPE
from   {src_catalog}.bronze.business_event be inner join 
mx_order mo on be.cfk_object_gid = mo.cfk_object_gid and be.order_no = mo.order_no
INNER JOIN {src_catalog}.bronze.STND_BUSINESS_EVENT_REASON BER ON BE.fk_business_event_reason_id = BER.Business_event_reason_id
INNER JOIN {src_catalog}.bronze.trademark  th on  be.cfk_object_gid = th.trademark_gid
INNER JOIN {src_catalog}.bronze.stnd_legacy_status sl on sl.status_no = th.legacy_status_cd
)
select B.cfk_object_gid `Serial Number`, 
       B.effective_ts `Replacement Date`,  
       B.legacy_cm_ent_cd `ENT Code`, 
       A.LAST_CM_DATE `Last CM Date`, 
       A.CM_TYPE `Last CM`, 
       A.AM_STAT_DT `AM STAT Date`, 
       A.AM_STAT `AM STAT`, 
       A.AM_TYPE AM
        from business_data A inner join  be_data B  on A.cfk_object_gid = b.cfk_object_gid
where B.legacy_cm_ent_cd in ('ENOR', 'RFIL')  and date(b.effective_ts)  between  date('{rdate}') - 13 and date('{rdate}')
""")

# COMMAND ----------

excel = f"Madrid Replacements Report Run Date {rdate}.xlsx"
subj = "MADRID: Transformation/Replacement Report"
title_tx_1 = f"""Transformation/Replacement counts {datetime.datetime.strptime(rdate, '%Y-%m-%d').date() - timedelta(13)} and {rdate}"""
title_tx_2 = """Transformation Query (INCLUDE also 'TRFL' for paper) = 1"""
title_tx_3 = """Replacements Query (INCLUDE also 'RFIL' for paper) = 0"""
from_addr= 'trademark_analytics@uspto.gov'

# COMMAND ----------

# DBTITLE 1,create excel file
# Turn off the default header and skip 5 rows to allow us to insert a
# user defined header.
# Create a Pandas Excel writer using XlsxWriter as the engine.
def excel_prep(df_replacement,
               df_transformation,
                excel,
               title_tx_1,
               title_tx_2,
               title_tx_3
               ) -> None:
    """This function generates prepared excel document with headers, styling and footer per user preference"""
    import datetime
    import pandas as pd

    properties = {"border": "5px solid grey",  "text-align": "center", "font-size" : "20px"}
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(excel, engine='xlsxwriter')
    df_transformation.style.set_properties(**properties).to_excel(writer, sheet_name='Sheet1', startrow=5, index=False, header=True)
    # Get the xlsxwriter worksheet and workbook objects.
    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']

    # Add a merge format
    merge_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'align': 'left',
         'font_size': 15})

    # set column and row heights
    worksheet.set_column(0, 7, 60)
    worksheet.set_row(0, 60)
    worksheet.merge_range("A1:H1", title_tx_1, merge_format)
    
    # write strings between
    worksheet.merge_range('A3:H3', title_tx_2, merge_format)
  
    # Add a header format.
    header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
     'font_size' : 16,
     'font_color' : 'white',
    'valign': 'center',
    'align': 'center',
    'fg_color': '#154468',
    'border': 1})

   # Write the column headers with the defined format.
    for col_num, value in enumerate(df_transformation.columns.values):
        worksheet.write(5, col_num, value, header_format)
    
    rowx = df_transformation.shape[0] + 8
    worksheet.merge_range(f'A{rowx}:H{rowx}', title_tx_3, merge_format) 
    df_replacement.style.set_properties(**properties).to_excel(writer, sheet_name='Sheet1', startrow=df_transformation.shape[0] + 9, index=False, header=True)
    for col_num, value in enumerate(df_replacement.columns.values):
        worksheet.write(df_transformation.shape[0] + 9, col_num, value, header_format)
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    print("done with excel")

# COMMAND ----------

# DBTITLE 1,Convert spark dataframe to pandas dataframe
df_transformation = df_spark_transformations.toPandas()
df_replacement = df_spark_replacements.toPandas()

# COMMAND ----------

# DBTITLE 1,Send the excel as a mail

if not df_transformation.empty or not df_replacement.empty :    

    # Save the DataFrame to a temporary directory as an Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath = f"{tmpdir}/{excel}"
        
        excel_prep(df_replacement,
               df_transformation,
                filepath,
               title_tx_1,
               title_tx_2,
               title_tx_3
               )


        xlsx_file = open(filepath, 'rb')
        out_file = io.StringIO()
        xlsx2html(xlsx_file, out_file, locale='en')
        out_file.seek(0)
        result_html = out_file.read()

        attachments = [filepath]

        # Send the email with the attachment
        send_email_report(
            job_nm = job_name,
            subject = subj,
            send_from = from_addr,
            send_to = emailid,
            html_body= result_html,
            attachments = attachments
        )
else:
    print("No email notification sent")

# COMMAND ----------

df = df_spark_transformations.union(df_spark_replacements)

# COMMAND ----------

df.write.mode("overwrite").format("delta").insertInto(f"{trm_reporting_catalog}.gold.madrid_transformations_and_replacements")

# COMMAND ----------

recs_count = df.count()
end_job_cntl(f"{trm_reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")

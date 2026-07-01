# Databricks notebook source
# %pip install fpdf2

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
CHK_POINT_DIR = "/tmp/checkpoints/tm_category_case_counts/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR

# COMMAND ----------

common_configs = read_yaml(config_file)
trm_reporting_catalog = common_configs['schema']['trm_reporting_catalog']
receiver_email = common_configs['alerting']['tm_case_category_counts']['email']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
env = dbx_env.upper()

emailid = receiver_email
print(f"{trm_reporting_catalog=},{emailid=}")
spark.conf.set('conf.catalog', trm_reporting_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')

job_name = 'ntb_trmreports_tm_category_case_counts'

control_dt = begin_job_cntl(f'{trm_reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

from fpdf import FPDF
from datetime import date, timedelta
# allows us to backfill in case of pipeline failures for entered date
if rundate == '':
  rdate = datetime.datetime.now().strftime('%Y-%m-%d')
  issue_date  =  date.today() - timedelta(1)
else:
  rdate = rundate
  issue_date  =  datetime.datetime.strptime(rdate, '%Y-%m-%d').date() - timedelta(1)
print('rundate = ' + str(rdate), 'issue_date = ' + str(issue_date))

# COMMAND ----------

df_spark_counts_issue_date = spark.sql(f"""
                          -- Query 1 uses prosecution history codes to calculate Published for Opposition and Supplemental Register
                          -- Uses prosecution history date
                          SELECT
                              CASE WHEN ph.ph_action_code = 'PUBO' THEN 'Published for Opposition'
                                  WHEN ph.ph_action_code = 'R.SR' THEN 'Supplemental Register' END AS `CATEGORY DESCRIPTION`,
                              COUNT(DISTINCT ph.serial_number) AS `COUNT`,
                              SUM(CASE WHEN class_status IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `FEE PAID CLS`,
                              SUM(CASE WHEN class_status NOT IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `OTHER CLS`

                          FROM {trm_reporting_catalog}.silver.prosecution_history ph
                          INNER JOIN {trm_reporting_catalog}.silver.class cls ON cls.ser_num = ph.serial_number
                          WHERE ph.ph_action_code IN ('PUBO', 'R.SR')
                          AND ph.ph_action_date BETWEEN date('{rdate}')-7 and date('{rdate}')-1
                          GROUP BY `CATEGORY DESCRIPTION`

                          UNION

                          SELECT
                              CASE WHEN ph.ph_action_code = 'R.PR' THEN 'Principal Register' END AS `CATEGORY DESCRIPTION`,
                              COUNT(DISTINCT ph.serial_number) AS `COUNT`,
                              SUM(CASE WHEN class_status IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `FEE PAID CLS`,
                              SUM(CASE WHEN class_status NOT IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `OTHER CLS`

                          FROM {trm_reporting_catalog}.silver.prosecution_history ph
                          INNER JOIN {trm_reporting_catalog}.silver.class cls ON cls.ser_num = ph.serial_number
                          INNER JOIN {trm_reporting_catalog}.silver.milestone ml ON ml.ser_num = ph.serial_number
                          WHERE ph.ph_action_code IN ('R.PR')
                          AND ml.registration_dt BETWEEN date('{rdate}') -7 and date('{rdate}') - 1
                          AND ml.disposal_type = 'REGISTRATION'
                          GROUP BY `CATEGORY DESCRIPTION`

                          UNION 
                          -- 3rd Query calculates what is OG code 15 on the megaspec report and known as Intent to Use.
                          -- Code 15 is defined as Issued Principal Register in Tram
                          -- Using prosecution history code CNPR and milestone registration_dt results in a near perfect match with megaspec
                          -- I have no idea why
                          SELECT
                              CASE WHEN ph.ph_action_code = 'CNPR' THEN 'Intent to Use' END AS `CATEGORY DESCRIPTION`,
                              COUNT(DISTINCT ph.serial_number) AS `COUNT`,
                              SUM(CASE WHEN class_status IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `FEE PAID CLS`,
                              SUM(CASE WHEN class_status NOT IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `OTHER CLS`

                          FROM {trm_reporting_catalog}.silver.prosecution_history ph
                          INNER JOIN {trm_reporting_catalog}.silver.class cls ON cls.ser_num = ph.serial_number
                          INNER JOIN {trm_reporting_catalog}.silver.milestone ml ON ml.ser_num = ph.serial_number
                          WHERE ph.ph_action_code IN ('CNPR')
                          AND ml.registration_dt BETWEEN date('{rdate}') - 7 and date('{rdate}') - 1
                          GROUP BY `CATEGORY DESCRIPTION`
                            """)

# COMMAND ----------

df_spark_counts_ytd = spark.sql(f"""
                            -- Query 1 uses prosecution history codes to calculate Published for Opposition and Supplemental Register
                            -- Uses prosecution history date
                            SELECT
                                CASE WHEN ph.ph_action_code = 'PUBO' THEN 'Published for Opposition'
                                    WHEN ph.ph_action_code = 'R.SR' THEN 'Supplemental Register' END AS `CATEGORY DESCRIPTION`,
                                COUNT(DISTINCT ph.serial_number) AS `COUNT`,
                                SUM(CASE WHEN class_status IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `FEE PAID CLS`,
                                SUM(CASE WHEN class_status NOT IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `OTHER CLS`

                            FROM {trm_reporting_catalog}.silver.prosecution_history ph
                            INNER JOIN {trm_reporting_catalog}.silver.class cls ON cls.ser_num = ph.serial_number
                            WHERE ph.ph_action_code IN ('PUBO', 'R.SR')
                            AND YEAR(DATEADD(MONTH, 3, ph.ph_action_date)) = YEAR(DATEADD(MONTH, 3, date('{rdate}')))
                            GROUP BY `CATEGORY DESCRIPTION`

                            UNION

                            SELECT
                                CASE WHEN ph.ph_action_code = 'R.PR' THEN 'Principal Register' END AS `CATEGORY DESCRIPTION`,
                                COUNT(DISTINCT ph.serial_number) AS `COUNT`,
                                SUM(CASE WHEN class_status IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `FEE PAID CLS`,
                                SUM(CASE WHEN class_status NOT IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `OTHER CLS`

                            FROM {trm_reporting_catalog}.silver.prosecution_history ph
                            INNER JOIN {trm_reporting_catalog}.silver.class cls ON cls.ser_num = ph.serial_number
                            INNER JOIN {trm_reporting_catalog}.silver.milestone ml ON ml.ser_num = ph.serial_number
                            WHERE ph.ph_action_code IN ('R.PR')
                            AND YEAR(DATEADD(MONTH, 3, ml.registration_dt)) = YEAR(DATEADD(MONTH, 3, date('{rdate}')))
                            AND ml.disposal_type = 'REGISTRATION'
                            GROUP BY `CATEGORY DESCRIPTION`

                            UNION 
                            -- 3rd Query calculates what is OG code 15 on the megaspec report and known as Intent to Use.
                            -- Code 15 is defined as Issued Principal Register in Tram
                            -- Using prosecution history code CNPR and milestone registration_dt results in a near perfect match with megaspec
                            -- I have no idea why
                            SELECT
                                CASE WHEN ph.ph_action_code = 'CNPR' THEN 'Intent to Use' END AS `CATEGORY DESCRIPTION`,
                                COUNT(DISTINCT ph.serial_number) AS `COUNT`,
                                SUM(CASE WHEN class_status IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `FEE PAID CLS`,
                                SUM(CASE WHEN class_status NOT IN ('ACTIVE', 'FEE WAIVED', 'Partially Paid', 'Abandoned') THEN 1 ELSE 0 END) AS `OTHER CLS`

                            FROM {trm_reporting_catalog}.silver.prosecution_history ph
                            INNER JOIN {trm_reporting_catalog}.silver.class cls ON cls.ser_num = ph.serial_number
                            --INNER JOIN trm_reporting.silver.milestone ml ON ml.ser_num = ph.serial_number
                            WHERE ph.ph_action_code IN ('CNPR')
                            AND YEAR(DATEADD(MONTH, 3, ph.ph_action_date)) = YEAR(DATEADD(MONTH, 3, date('{rdate}')))
                            GROUP BY `CATEGORY DESCRIPTION`
                            """)

# COMMAND ----------

data_columns = df_spark_counts_ytd.columns

# COMMAND ----------

mailed_pdf = "TMIIMC38_Report.pdf"
title_tx_1 = """TMIIMC38 CATEGORY COUNTS"""
title_tx_2 = f"""FOR Report Date {rdate}"""
title_tx_3 = f"""Category Counts For Issue Date {issue_date}"""
title_tx_4 = """Category Counts Year-To-Date"""
data_col_1 = data_columns[0]
data_col_2 = data_columns[1]
data_col_3 = data_columns[2]
data_col_4 = data_columns[3]
tm_analytics_image_loc = '../shared/tm_analytics.jpg'
uspto_image_loc = '../shared/uspto_logo.png'
from_addr= 'trademark_analytics@uspto.gov'

# COMMAND ----------

# DBTITLE 1,fpdf2 library
def pdf_prep(filepath, uspto_image_loc, title_tx_1, title_tx_2, title_tx_3, title_tx_4,
             tm_analytics_image_loc, data_col_1, data_col_2, data_col_3, data_col_4,
             df_spark_counts_issue_date, df_spark_counts_ytd):
    
    from fpdf import FPDF
    
    pdf = FPDF()
    pdf.add_page()
    
    # Use 'Helvetica' instead of 'Arial' (built-in font in fpdf2)
    pdf.set_font('Helvetica', 'B', 16)
    
    # Header section
    pdf.image(uspto_image_loc, x=10, y=8, w=33)
    
    pdf.cell(0, 10, title_tx_1, new_x="LMARGIN", new_y="NEXT", align='C')
    pdf.cell(0, 10, title_tx_2, new_x="LMARGIN", new_y="NEXT", align='C')
    
    pdf.image(tm_analytics_image_loc, x=170, y=8, w=33)
    
    pdf.ln(10)
    pdf.set_font('Helvetica', '', 12)
    pdf.cell(0, 10, title_tx_3, new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 10, title_tx_4, new_x="LMARGIN", new_y="NEXT")
    
    # Table headers
    pdf.ln(5)
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(50, 10, data_col_1, border=1)
    pdf.cell(30, 10, data_col_2, border=1)
    pdf.cell(30, 10, data_col_3, border=1)
    pdf.cell(30, 10, data_col_4, border=1, new_x="LMARGIN", new_y="NEXT")
    
    # Data rows - Issue Date counts
    pdf.set_font('Helvetica', '', 10)
    for index, row in df_spark_counts_issue_date.iterrows():
        pdf.cell(50, 10, str(row.get(data_col_1, '')), border=1)
        pdf.cell(30, 10, str(row.get(data_col_2, '')), border=1)
        pdf.cell(30, 10, str(row.get(data_col_3, '')), border=1)
        pdf.cell(30, 10, str(row.get(data_col_4, '')), border=1, new_x="LMARGIN", new_y="NEXT")
    
    # Add spacing and YTD section
    pdf.ln(10)
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(0, 10, 'Year to Date Counts:', new_x="LMARGIN", new_y="NEXT")
    
    # Table headers for YTD
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(50, 10, data_col_1, border=1)
    pdf.cell(30, 10, data_col_2, border=1)
    pdf.cell(30, 10, data_col_3, border=1)
    pdf.cell(30, 10, data_col_4, border=1, new_x="LMARGIN", new_y="NEXT")
    
    # Data rows - YTD counts
    pdf.set_font('Helvetica', '', 10)
    for index, row in df_spark_counts_ytd.iterrows():
        pdf.cell(50, 10, str(row.get(data_col_1, '')), border=1)
        pdf.cell(30, 10, str(row.get(data_col_2, '')), border=1)
        pdf.cell(30, 10, str(row.get(data_col_3, '')), border=1)
        pdf.cell(30, 10, str(row.get(data_col_4, '')), border=1, new_x="LMARGIN", new_y="NEXT")
    
    pdf.output(filepath)
    
    print(f"PDF generated: {filepath}")

# COMMAND ----------

# DBTITLE 1,Old fpdf library

# def pdf_prep(
#             mailed_pdf,
#             uspto_image_loc,
#             title_tx_1,
#             title_tx_2,
#             title_tx_3,
#             title_tx_4,
#             tm_analytics_image_loc,
#             data_col_1,
#             data_col_2,
#             data_col_3,
#             data_col_4,
#             df_spark_counts_issue_date,
#             df_spark_counts_ytd
#              ):
#     """This function instantiates class for pdf_prep, creates and saves temp space"""
#     class PDF(FPDF):
#         def header(self):
#             # Logo
#             self.image(uspto_image_loc, 10, 8, 33)
#             # Arial bold 12
#             self.set_font('Arial', 'B', 15)
#             # Move to the right
#             self.cell(80)
#             # Title
#             self.cell(30, 10, title_tx_1, 0, 0, 'C')
#             self.ln(5) #enter
#             self.cell(80)
#             self.cell(30, 10, title_tx_2, 0, 0, 'C')
#             self.ln(5)
#             self.cell(80)
#             # Line break
#             self.ln(20)
#             self.image(tm_analytics_image_loc, 160, 6, 23)
         


#     # Instantiation of inherited class
#     pdf = PDF()
#     # start creating ....
#     pdf.add_page()
#     # get count of table
#     df= None
#     df_list = [df_spark_counts_issue_date,df_spark_counts_ytd]
#     desc_list  = [title_tx_3, title_tx_4]
#     for i in range(len(df_list)):
#         """Allows you to be DRY, dont repeat yourself"""
#         pdf.set_font('Arial', 'B', 12)
#         pdf.cell(0, 10, desc_list[i], 0, 0, 'L')
#         pdf.ln(10)
#         pdf.set_font('Arial', 'B', 10)
#         pdf.set_fill_color(0,75,126)
#         pdf.set_text_color(255,255,255)
#         """Set up column widths to avoid squeezing"""
#         cell_width = [45,40,40, 40]
#         # we are writing the columns headers first
#         pdf.cell(cell_width[0],5,data_col_1,1,0,align='C',fill=True)
#         pdf.cell(cell_width[1],5, data_col_2,1,0,align='C',fill=True)
#         pdf.cell(cell_width[2],5, data_col_3,1,0,align='C',fill=True)
#         pdf.cell(cell_width[3],5, data_col_4,1,0,align='C',fill=True)
#         pdf.ln(h = 5)
#         pdf.set_font('Arial', '', 10)
#         pdf.set_text_color(0,0,0)
#         df=df_list[i]
#         row_size = df.shape[0]
#         for i in range(0, row_size):
#             """This allows you to set up alternating bands of color for the table and write rows 1 by 1"""
#             if i%2 == 0:
#                 pdf.set_fill_color( 224, 224,  224)
#             else:
#                 pdf.set_fill_color(249, 249, 249)
#             pdf.cell(cell_width[0],5,str(df.loc[i,data_col_1]),1,0,align='C', fill=True)
#             pdf.cell(cell_width[1],5,str(df.loc[i,data_col_2]),1,0,align='C', fill=True)
#             pdf.cell(cell_width[2],5,str(df.loc[i,data_col_3]),1,0,align='C',fill=True)
#             pdf.cell(cell_width[3],5,str(df.loc[i,data_col_4]),1,0,align='C', fill=True)
#             pdf.ln(h = 5) # add 5 mm space before next
#     pdf.output(mailed_pdf, 'F')
#     print("done with pdf")

# COMMAND ----------

# DBTITLE 1,create pdf and send email
if df_spark_counts_issue_date.count() > 0 or df_spark_counts_ytd.count() > 0 :  

    df_out = df_spark_counts_issue_date.withColumn(
        'time_period', lit('weekly')
    ).unionByName(
        df_spark_counts_ytd.withColumn('time_period', lit('year_to_date'))
    ).select('time_period', 'category description', 'count', 'FEE PAID CLS', 'OTHER CLS')

    ## write to delta table
    df_out.write.mode("overwrite").format("delta").insertInto(f"{trm_reporting_catalog}.gold.tm_category_case_counts")

    recscount = df_out.count()
    df_hstry = spark.sql(f"""select date('{rdate}') as rundate,* from {trm_reporting_catalog}.gold.tm_category_case_counts""")
    df_hstry.write.mode("append").format("delta").insertInto(f"{trm_reporting_catalog}.gold.tm_category_case_counts_hstry")

    ## convert to pandas for email output
    df_spark_counts_issue_date = df_spark_counts_issue_date.toPandas()
    df_spark_counts_ytd = df_spark_counts_ytd.toPandas()

    # Save the DataFrame to a temporary directory as an Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath1 = f"{tmpdir}/{mailed_pdf}"
        pdf_prep(
            filepath1,
            uspto_image_loc,
            title_tx_1,
            title_tx_2,
            title_tx_3,
            title_tx_4,
            tm_analytics_image_loc,
            data_col_1,
            data_col_2,
            data_col_3,
            data_col_4,
            df_spark_counts_issue_date,
            df_spark_counts_ytd
            )

        from_addr = "trademark_analytics@uspto.gov"
        email_subj = f'Auto Generated: Category Counts {env}'
        email_body = """See Attached Data for Category Counts TMIIMC38"""
        attachments = [filepath1]

        # Send the email with the attachment
        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to = emailid,
            html_body= email_body,
            attachments = attachments
        )

    #############################################################################################
    # 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
    # Allows disabling Alteryx workflow schedule fully, saving resources.                       #
    #############################################################################################

    # # data quality entry
    # tbl1 = f"hive_metastore.{altrx_schema}.tm_category_case_counts" 
    # tbl2 = f"{trm_reporting_catalog}.gold.tm_category_case_counts"
    # key_cols = ['time_period', 'category_description']

    # dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
    # print(dq_result)

    # end job control
    end_job_cntl(f"{trm_reporting_catalog}.silver", job_name, starttime,'completed', recscount,"job completed successfully")
       
else:
    print("No email notification sent")

    # end job control
    end_job_cntl(f"{trm_reporting_catalog}.silver", job_name, starttime,'completed', 0,"job completed successfully")

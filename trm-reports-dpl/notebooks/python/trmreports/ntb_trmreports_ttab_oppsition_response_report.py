# Databricks notebook source
# MAGIC %md
# MAGIC ### **ntb_tmreports_ttab_oppsition_response_report**

# COMMAND ----------

# %pip install fpdf2

# COMMAND ----------

# DBTITLE 1,setting up env
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_src_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmintl_src_catalog = common_configs['schema']['tmintltm_src_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
trmrp_scope = common_configs['secrets']['trmrp_scope']
primary_email, cc_email = common_configs["alerting"]["TTAB_Opposition_Response_Report"]["email"], common_configs["alerting"]["TTAB_Opposition_Response_Report"]["cc"]
altrx_schema = common_configs['schema']['altrx_schema']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
print(reporting_catalog,tmngpdb_src_catalog,primary_email, cc_email,altrx_schema,data_quality_catalog,tmintl_src_catalog)
data_layer = "bronze"

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_trmreports_ttab_oppsition_response_report'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# DBTITLE 1,Inputs
input_56=spark.sql(f"""SELECT * FROM {reporting_catalog}.silver.milestone WHERE IsNull(DISPOSAL_TYPE)""")
input_52=spark.sql(f"""SELECT * FROM {reporting_catalog}.silver.prosecution_history WHERE ph_action_code = 'OP.I' and Year(ph_action_date) > 2021""")
input_16= spark.sql(f"""SELECT ir.DN_SERIAL_NUM as serial_no, ireg.fk_international_reg_no intl_reg FROM {tmintl_src_catalog}.bronze.international_reg_tm ir INNER JOIN {tmintl_src_catalog}.bronze.international_registration ireg ON ir.fk_international_reg_gid = ireg.international_reg_gid""")
input_47 = spark.sql(f"""Select * FROM {reporting_catalog}.silver.bibliography WHERE filing_basis_cur = 'MADRID'""") 

# COMMAND ----------

sumrz_20 = input_16.groupBy("Serial_no", "intl_reg").count()
sel_60 = sumrz_20.select("Serial_no", "intl_reg")

# COMMAND ----------

#input_56

# Drop the specified columns
sel_58 = input_56.drop("create_ts","create_user_id","update_ts","update_user_id")
from pyspark.sql.functions import upper
# Rename columns to uppercase
rename_58 = sel_58
for col in sel_58.columns:
    rename_58 = rename_58.withColumnRenamed(col, col.upper())

# COMMAND ----------

#input_52
sel_53 = input_52.drop("create_ts","create_user_id","update_ts","update_user_id")
from pyspark.sql.functions import upper
rename_54 = sel_53
for col in sel_53.columns:
    rename_54 = rename_54.withColumnRenamed(col, col.upper())

# COMMAND ----------

#input_47
sel_48 = input_47.drop("create_ts","create_user_id","update_ts","update_user_id")
from pyspark.sql.functions import upper
rename_49 = sel_48
for col in sel_48.columns:
    rename_49 = rename_49.withColumnRenamed(col, col.upper())

# Apply filter
fltr_3 = rename_49.filter(rename_49["FILING_BASIS_CUR"] == 'MADRID')

# COMMAND ----------

join_54 = rename_54.alias("l").join(
    fltr_3.alias("r"), 
    rename_54["SERIAL_NUMBER"] == fltr_3["SER_NUM"]
)

# COMMAND ----------

from pyspark.sql.functions import max, last
# summery and orderby Ascending in one 
sumrz_7 = join_54.groupBy("SERIAL_NUMBER") \
    .agg(
        max("PH_ACTION_NUMBER").alias("Max_PH_ACTION_NUMBER"),
        last("PH_ACTION_DATE").alias("Last_PH_ACTION_DATE")
    ).orderBy("SERIAL_NUMBER")

# COMMAND ----------

# Join sumrz_7 and join_54
join_10 = sumrz_7.alias("l").join(
    join_54.alias("r"),
    (sumrz_7["SERIAL_NUMBER"] == join_54["SERIAL_NUMBER"])
    & (sumrz_7["Max_PH_ACTION_NUMBER"] == join_54["PH_ACTION_NUMBER"])
    & (sumrz_7["Last_PH_ACTION_DATE"] == join_54["PH_ACTION_DATE"]),
).select(
  "l.SERIAL_NUMBER",
  "l.Max_PH_ACTION_NUMBER",
  "l.Last_PH_ACTION_DATE",
  "r.PH_ACTION_NUMBER",
  "r.PH_ACTION_DATE"
)

# COMMAND ----------

from pyspark.sql.functions import col, date_add, date_format, when, to_date, lit

# Add 40 days to PH_ACTION_DATE # # Format the response_due_dt to get the day of the week

frml_11 = join_10.withColumn(
    "response_due_dt",
    when(
        col("PH_ACTION_DATE") >= to_date(lit("2025-09-04")),
        date_add(col("PH_ACTION_DATE"), 60),
    ).otherwise(date_add(col("PH_ACTION_DATE"), 40)),
)
# Below code has not been used in forther transofmation hence commenting
# \
#   .withColumn("responsedue_dt_DAY",date_format(response_due_dt, "EEEE")) \
#     .withColumn("responsedue_dt",when(responsedue_dt_DAY == "Saturday", date_add(col("PH_ACTION_DATE"), 42))
#     .when(responsedue_dt_DAY == "Sunday", date_add(col("PH_ACTION_DATE"), 41))
#     .otherwise(date_add(col("PH_ACTION_DATE"), 40)))


# display(frml_11)

# COMMAND ----------

# Filter the DataFrame
fltr_24 =  frml_11.filter(col("response_due_dt") > date_add(current_date(), -1))
# fltr_24.count()

# COMMAND ----------

join_59 = rename_58.alias("l") \
  .join(sumrz_7.alias("r"), rename_58["SER_NUM"] == sumrz_7["SERIAL_NUMBER"])


# COMMAND ----------

# input 16 
sumrz_20 = input_16.groupBy("Serial_no","intl_reg").count().select("Serial_no","intl_reg")
# sumrz_20.count()

# COMMAND ----------

join_19 = (
    sumrz_20.alias("l")
    .join(join_59.alias("r"), sumrz_20["Serial_no"] == join_59["SER_NUM"])
    .select(
        "l.Serial_no",
        "l.intl_reg",
        "r.PENDENCY_CAL_START_DT",
        "r.PENDENCY_CAL_END_DT",
        "r.FIRST_ACTION_CD",
        "r.DISPOSAL_PENDENCY",
        "r.SUSPENSION",
        "r.TTAB",
        "r.DISPOSAL_DT",
        "r.DOCK_DT",
        "r.AM_FLG_66A_CUR",
        "r.AM_FLG_66A_FIL",
        "r.NOA_DT_PH",
        "r.FILING_FY",
        "r.NON_PRO_SE",
        "r.FIRST_ACTION_PENDENCY_PH",
        "r.LAST_MODIFIED_DATE",
        "r.PROCESSING_PEND",
        "r.PROCESSING_PEND_DAYS",
        "r.DAYS_IN_DOCK",
        #"r.SERIAL_NUMBER",
        "r.Max_PH_ACTION_NUMBER"
))

# COMMAND ----------

join_26 = (
    join_19.alias("l")
    .join(fltr_24.alias("r"), join_19["Serial_no"] == fltr_24["SERIAL_NUMBER"])
    .select("l.Serial_no", "l.intl_reg","l.DISPOSAL_DT", "r.PH_ACTION_DATE","r.response_due_dt")
).orderBy("r.response_due_dt")

# COMMAND ----------

# DBTITLE 1,Final Dataframe
## Final dataframe
final_df = (
    join_26.select(
        col("Serial_no").cast("integer").alias("Serial_Number"),
        col("intl_reg").cast("integer").alias("International_Registration"),
        col("PH_ACTION_DATE").alias("Opposition_Notice_Date"),
        col("response_due_dt").alias("Response_Due_Date"),
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)
# join_26.count()
image_df = (
    join_26.select(
        col("Serial_no").cast("integer").alias("Serial_Number"),
        col("intl_reg").alias("International_Registration"),
        col("PH_ACTION_DATE").alias("Opposition_Notice_Date"),
        col("response_due_dt").alias("Response_Due_Date"),
    ))

# COMMAND ----------

from fpdf import FPDF
from fpdf.enums import XPos, YPos
import pandas as pd
import datetime
from datetime import date
import tempfile
import os

text = """Good morning.<br>
Attached is the Daily TTAB Opposition Response Due Date.<br>
If you have any question, concerns or enhancements, please contact trademark_analytics@uspto.gov.<br>
Thank you. """  

csv_df = image_df.toPandas()
mailed_pdf = f"Daily_TTAB_Opposition_Response_Due_Date_{date.today()}.pdf"
title_tx_1 = "TM International" 
title_tx_2 = "FR TTAB Opposition Response Daily Report"
data_col_1 = 'Serial Number'
data_col_2 = 'International Registration'
data_col_3 = 'Opposition Notice Date'
data_col_4 = 'Response Due Date'
tm_analytics_image_loc = '../shared/tm_analytics.jpg'
uspto_image_loc = '../shared/uspto_logo.png'
send_from = "trademark_analytics@uspto.gov"
attachment_name = "TTAB_Opposition_Response_Due_Date.csv"

def pdf_prep(df,
            mailed_pdf,
            uspto_image_loc,
            title_tx_1,
            title_tx_2,
            tm_analytics_image_loc,
            data_col_1,
            data_col_2,
            data_col_3,
            data_col_4
            ):
    """This function instantiates class for pdf_prep, creates and saves temp space"""
    
    class PDF(FPDF):
        def header(self):
            # Logo
            self.image(uspto_image_loc, 10, 8, 33)
            # Helvetica bold 12
            self.set_font('Helvetica', 'B', 12)
            # Move to the right
            self.cell(80)
            # Title - using new_x and new_y instead of positional ln parameter
            self.cell(30, 10, title_tx_1, border=0, align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.ln(5)
            self.cell(80)
            self.cell(30, 10, title_tx_2.format(date.today()), border=0, align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.ln(5)
            self.cell(80)
            self.cell(30, 10, str(date.today()), border=0, align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
            # Line break
            self.ln(20)
            self.image(tm_analytics_image_loc, 160, 6, 23)
            self.set_font('Helvetica', 'I', 12)
            self.ln(10)
            self.set_font('Helvetica', 'B', 10)
            self.set_fill_color(0, 75, 126)
            self.set_text_color(255, 255, 255)
            cell_width = [35, 50, 50, 50]
            # Using named parameters to avoid deprecation warnings
            self.cell(cell_width[0], 5, data_col_1, border=1, align='C', fill=True, new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(cell_width[1], 5, data_col_2, border=1, align='C', fill=True, new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(cell_width[2], 5, data_col_3, border=1, align='C', fill=True, new_x=XPos.RIGHT, new_y=YPos.TOP)
            self.cell(cell_width[3], 5, data_col_4, border=1, align='C', fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_font('Helvetica', '', 10)
            self.set_text_color(0, 0, 0)

        # Page footer
        def footer(self):
            # Position at 2 cm from bottom
            self.set_y(-20)
            # Helvetica italic 6
            self.set_font('Helvetica', 'I', 6)

    cell_width = [35, 50, 50, 50]
    # Instantiation of inherited class
    pdf = PDF()
    # start creating ....
    pdf.add_page()
    # get count of table
    row_size = df.shape[0]
    for i in range(0, row_size):
        pdf.cell(cell_width[0], 5, str(df.loc[i, 'Serial_Number']), border=1, align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.cell(cell_width[1], 5, str(df.loc[i, 'International_Registration']), border=1, align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.cell(cell_width[2], 5, str(df.loc[i, 'Opposition_Notice_Date']), border=1, align='C', new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.cell(cell_width[3], 5, str(df.loc[i, 'Response_Due_Date']), border=1, align='C', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    
    # Output PDF
    pdf.output(mailed_pdf)
    print(f"PDF generated: {mailed_pdf}")


if csv_df.size >= 0:    
    parms = {}

    # Save the DataFrame to a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath1 = f"{tmpdir}/{mailed_pdf}"
        filepath2 = f"{tmpdir}/{attachment_name}"

        df = image_df.toPandas()
        pdf_prep(
            df,
            filepath1,
            uspto_image_loc,
            title_tx_1,
            title_tx_2,
            tm_analytics_image_loc,
            data_col_1,
            data_col_2,
            data_col_3,
            data_col_4
        )
        
        csv_df.to_csv(filepath2, index=False)
        
        attachments = [filepath1, filepath2]
        
        email_subj = """Daily TTAB Opposition Response Due Date"""

        # Send the email with the attachment
        send_email_report(
            job_nm=job_name,
            subject=email_subj,
            send_from=send_from,
            send_to=primary_email,
            send_to_cc=cc_email,
            html_body=text,
            attachments=attachments
        )
else:
    print("No email notification sent")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write Data into Tables. 

# COMMAND ----------

# DBTITLE 1,Writing the data in tables
try:
    final_df.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.ttab_opposition_response')
    recs_count = final_df.count()
    # below code is commented 
    # # data quality entry altrx_schema
    # tbl1 = f"{reporting_catalog}.gold.ttab_opposition_response"
    # if dbx_env == 'dev':
    #     tbl2 = f"hive_metastore.{altrx_schema}.ttab_opposition_response1"
    # else:
    #     tbl2 = f"hive_metastore.{altrx_schema}.ttab_opposition_response"
    # key_cols = ['Serial_Number']
    # dq_catalog = data_quality_catalog
    # # job_name = job_name
    # dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
    #print(dq_result)
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading ttab_opposition_response with data quality check")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading ttab_opposition_response ")

# Databricks notebook source
# DBTITLE 1,Install openpyxl Package for testing in notebook
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# DBTITLE 1,Set Environment and Rundate Widgets
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("rundate","")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
rundate_entered = dbutils.widgets.get("rundate").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Run common functions and parameters
# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Set Config details
common_configs = read_yaml(config_file)
trm_reporting_catalog = common_configs['schema']['trm_reporting_catalog']
tmngpdb_src_catalog = common_configs['schema']['tmngpdb_src_catalog']
jbteasps_src_catalog = common_configs['schema']['trm_jbteasps_src_catalog']
receiver_email = common_configs['alerting']['email_cx_survey']['email']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
cx_survey_scope  = common_configs['secrets']['cx_survey_scope']
jbteasps_scope  = common_configs['secrets']['jbteasps_scope']
env = dbx_env.upper()

emailid = receiver_email
print(f"{trm_reporting_catalog=},{jbteasps_src_catalog=},{tmngpdb_src_catalog=},{emailid=}, {dq_catalog=}, {altrx_schema=} {cx_survey_scope=}")
spark.conf.set('conf.catalog', trm_reporting_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Get token and secrets, you can only get secrets in prod
from collections import OrderedDict
api_token = dbutils.secrets.get(scope="cx_survey", key="api_token") if dbutils.widgets.get("dbx_env") == "prod" else None
teas_automation_token = dbutils.secrets.get(scope="cx_survey", key="teas_automation_token") if dbutils.widgets.get("dbx_env") == "prod" else None
efile_automation_token = dbutils.secrets.get(scope="cx_survey", key="efile_automation_token") if dbutils.widgets.get("dbx_env") == "prod" else None
automation_token = OrderedDict([('teas', teas_automation_token), ('efile', efile_automation_token)])
api_endpoint = common_configs['endpoints']['cx_survey_api_endpoint']

# COMMAND ----------

# DBTITLE 1,Start Job Control with Current Timestamp
# set current time for job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
job_start_ts = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = "ntb_trmreports_email_address_for_cx_survey"

control_dt = begin_job_cntl(f"{trm_reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

from pyspark.sql.functions import col, current_date, to_date, trim, concat, lit, date_format, initcap, monotonically_increasing_id, lower, substring, instr, length, row_number, coalesce, regexp_replace, expr
from pyspark.sql.window import Window

# COMMAND ----------

# Determine the rundate value
from datetime import timedelta
if rundate_entered == '':
    rundate_value = 'current_date'
else:
    rundate_value = f"'{datetime.datetime.strptime(rundate, '%Y-%m-%d').strftime('%d-%b-%y')}'"

print(rundate_value)

# COMMAND ----------

from datetime import timedelta
rundate = datetime.datetime.strptime(rundate, '%Y-%m-%d').date() if rundate_entered != '' else datetime.datetime.today().date()

# COMMAND ----------

df_query = f"""select AUDIT_LOG.AUDIT_LOG_ID,
    AUDIT_LOG.REFERENCE_NO,
    AUDIT_LOG.SERIAL_NO,
    AUDIT_LOG.CFK_PATRON_ID,
    AUDIT_LOG.IP_ADDRESS_TX,
    AUDIT_LOG.FK_TRANSACTION_TYPE_CD,
    AUDIT_LOG.FK_SOURCE_SYSTEM_ID,
    AUDIT_LOG.REGISTRATION_NO,
    AUDIT_LOG.FK_FORM_CD,
    AUDIT_LOG.SUBMISSION_ID,
    AUDIT_LOG.FILING_ID,
    AUDIT_LOG.CREATE_USER_ID,
    AUDIT_LOG.CREATE_TS,
    AUDIT_LOG.SIGNATORY_NM,
    AUDIT_LOG.SIGNATORY_POSITION_NM,
    AUDIT_LOG.FK_SIGNATURE_TYPE_CD,
    AUDIT_LOG.FILING_DT,
    AUDIT_LOG.DN_PATRON_FIRST_NM,
    AUDIT_LOG.DN_PATRON_LAST_NM,
    AUDIT_LOG.DN_PATRON_EMAIL_ADDRESS_TX,
    AUDIT_LOG.FK_PROOF_CD 
from TURM.AUDIT_LOG 
where AUDIT_LOG.FILING_DT BETWEEN (TIMESTAMP '{rundate}' - INTERVAL '1 day') AND (TIMESTAMP '{rundate}')"""

# COMMAND ----------

# DBTITLE 1,ETL Section Begins
# Load audit log data and filter by filing date
audit_log_df = read_data_from_postgres_conn(df_query,jbteasps_scope) 

# Filter logs with valid names and non-empty email addresses
log_false_name_match = audit_log_df \
    .filter((~col("DN_PATRON_FIRST_NM").rlike(r'.*[^\x01-\x7F].*') & ~col("DN_PATRON_LAST_NM").rlike(r'.*[^\x01-\x7F].*'))).filter(length(col("DN_PATRON_EMAIL_ADDRESS_TX")) != 0)

# Filter logs with invalid names
log_true_name_match = audit_log_df \
    .filter((col("DN_PATRON_FIRST_NM").rlike(r".*[^\x01-\x7F].*") | col("DN_PATRON_LAST_NM").rlike(r".*[^\x01-\x7F].*"))) \
    .select( 
    trim(col("SERIAL_NO")).cast("int").alias("SERIAL_NO"),
    col("IP_ADDRESS_TX"),
    col("FK_TRANSACTION_TYPE_CD"),
    col("REGISTRATION_NO").cast("int"),
    col("FK_FORM_CD"),
    col("FILING_DT"),
    col("DN_PATRON_FIRST_NM"),
    col("DN_PATRON_LAST_NM"),
    col("DN_PATRON_EMAIL_ADDRESS_TX"),
    col("SIGNATORY_POSITION_NM"),
    col("CFK_PATRON_ID")
    )

# Add rundate and filenames to the filtered logs
add_rundate_str = log_false_name_match \
    .withColumn("SERIAL_NO", initcap(trim(regexp_replace(col("SERIAL_NO"),"[^\w\s]", "")))) \
    .withColumn("REGISTRATION_NO", initcap(trim(regexp_replace(col("REGISTRATION_NO"),"[^\w\s]", "")))) \
    .withColumn("RUNDATE", lit(rundate)) \
    .withColumn("rundate_str", date_format(lit(rundate), 'ddMMyyyy')) \
    .withColumn("FileName", concat(lit(r'USPTO_TEAS_'), date_format(lit(rundate), 'ddMMyyyy').cast("string"), lit('.csv'))) \
    .withColumn("FileName2", concat(lit(r'USPTO_TEAS_'), date_format(lit(rundate), 'ddMMyyyy').cast("string"), lit(r'.xlsx'))) 

# Filter logs with form code 'APPB'
check_appb_true = add_rundate_str.filter(col("FK_FORM_CD") == 'APPB').select(add_rundate_str["SERIAL_NO"],
        add_rundate_str["IP_ADDRESS_TX"],
        add_rundate_str["FK_TRANSACTION_TYPE_CD"],
        add_rundate_str["REGISTRATION_NO"],
        add_rundate_str["FK_FORM_CD"],
        add_rundate_str["FILING_DT"],
        add_rundate_str["DN_PATRON_FIRST_NM"],
        add_rundate_str["DN_PATRON_LAST_NM"],
        add_rundate_str["DN_PATRON_EMAIL_ADDRESS_TX"],
        add_rundate_str["rundate_str"]
        )

# Filter logs with form code not 'APPB' and update filenames
check_appb_false = add_rundate_str.filter(col("FK_FORM_CD") != 'APPB').withColumn("FileName", concat(lit(r'USPTO_TEAS_'), date_format(lit(rundate), 'dMMyyyy').cast("string"), lit('.csv'))) \
.withColumn("FileName2", concat(lit(r'USPTO_TEAS_'), date_format(lit(rundate), 'ddMMyyyy').cast("string"), lit(r'.xlsx'))) 

# Summarize rundate and filenames
summarize_rundate_str = add_rundate_str \
    .groupBy("rundate_str", "FileName") \
    .agg({"rundate_str": "min", "FileName": "min"}) \
    .withColumnRenamed("min(rundate_str)", "rundate_str_s") \
    .withColumnRenamed("min(FileName)", "FileName_s") \
    .select (col("FileName").alias("FileName"),col("rundate_str_s").alias("rundate_str"))

# Cross join logs with invalid names and summarized rundate
file_errors = log_true_name_match.crossJoin(summarize_rundate_str)
file_errors = file_errors.select([
   "SERIAL_NO",
   "CFK_PATRON_ID",
    "IP_ADDRESS_TX",
   "FK_TRANSACTION_TYPE_CD",
   	"REGISTRATION_NO",
   "FK_FORM_CD",
   "SIGNATORY_POSITION_NM",
    "FILING_DT",
    "DN_PATRON_FIRST_NM",
    "DN_PATRON_LAST_NM",
    "DN_PATRON_EMAIL_ADDRESS_TX",
    "RUNDATE_STR"
]
) 

# Load trademark data and join with correspondence data
from pyspark.sql.functions import col, initcap, left, ltrim, rtrim, substring, current_date, date_format

corr_base = spark.table(f"{tmngpdb_src_catalog}.bronze.trademark").alias("tm") \
    .join(spark.table(f"{trm_reporting_catalog}.silver.correspondence").alias("cor"), col("serial_num_tx") == col("ser_num")) \
    .filter(
        (col("FILING_DT") > rundate + timedelta(days=-1)) & (col("FILING_DT") < rundate)
             & 
             (trim(lower(col("tm.create_user_id"))) == 'tmefile') & (col("cor.cr_email1").isNotNull()))\
    .select(
        col("ser_num").alias("SERIAL_NO"),
        lit("").alias("IP_ADDRESS_TX"),
        lit("").alias("FK_TRANSACTION_TYPE_CD"),
        col("registration_num").alias("REGISTRATION_NO"),
        expr("CASE WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'TEASP' THEN 'FTK' WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'APPB' THEN 'APPB' ELSE 'APPB' END").alias("FK_FORM_CD"),
        col("filing_dt").alias("FILING_DT"),
        col("cor_nm").alias("COR_NM"),
        initcap(substring(col("cor_nm"), 1, instr(col("cor_nm"), ' ') - 1)).alias("DN_PATRON_FIRST_NM"),
        initcap(trim(substring(col("cor_nm"), instr(col("cor_nm"), ' ') + 1, length(col("cor_nm"))))).alias("DN_PATRON_LAST_NM"),
        col("cr_email1").alias("DN_PATRON_EMAIL_ADDRESS_TX"),
        date_format(lit(rundate), 'ddMMyyyy').alias("RUNDATE_STR"),
        concat(lit(r'USPTO_EFILE_'), date_format(lit(rundate), 'ddMMyyyy'), lit('.csv')).alias("FileName"),
        concat(lit(r'USPTO_EFILE_'), date_format(lit(rundate), 'ddMMyyyy'), lit(r'.xlsx')).alias("FILENAME2")
    )

# Join filtered logs with form code 'APPB' and correspondence data
cor_base_appb_true_join = check_appb_true.join(corr_base, (check_appb_true.SERIAL_NO == corr_base.SERIAL_NO) &
    (coalesce(check_appb_true.REGISTRATION_NO, lit(0)) == coalesce(corr_base.REGISTRATION_NO, lit(0))), "right") \
    .select( 
        coalesce(check_appb_true["SERIAL_NO"],corr_base["SERIAL_NO"]).alias("SERIAL_NO"),
        coalesce(check_appb_true["IP_ADDRESS_TX"],corr_base["IP_ADDRESS_TX"]).alias("IP_ADDRESS_TX"),
        coalesce(check_appb_true["FK_TRANSACTION_TYPE_CD"],corr_base["FK_TRANSACTION_TYPE_CD"]).alias("FK_TRANSACTION_TYPE_CD"),
        coalesce(check_appb_true["REGISTRATION_NO"],corr_base["REGISTRATION_NO"]).alias("REGISTRATION_NO"),
        coalesce(check_appb_true["FK_FORM_CD"],corr_base["FK_FORM_CD"]).alias("FK_FORM_CD"),
        coalesce(check_appb_true["FILING_DT"],corr_base["FILING_DT"]).alias("FILING_DT"),
        coalesce(check_appb_true["DN_PATRON_FIRST_NM"],corr_base["DN_PATRON_FIRST_NM"]).alias("DN_PATRON_FIRST_NM"),
        coalesce(check_appb_true["DN_PATRON_LAST_NM"],corr_base["DN_PATRON_LAST_NM"]).alias("DN_PATRON_LAST_NM"),
        coalesce(check_appb_true["DN_PATRON_EMAIL_ADDRESS_TX"],corr_base["DN_PATRON_EMAIL_ADDRESS_TX"]).alias("DN_PATRON_EMAIL_ADDRESS_TX"),
        coalesce(check_appb_true["rundate_str"],corr_base["rundate_str"]).alias("rundate_str")
    ) \
.withColumn("FileName", concat(lit(r'USPTO_EFILE_'), date_format(lit(rundate), 'ddMMyyyy').cast("string"), lit('.csv'))) \
    .withColumn("FileName2", concat(lit(r'USPTO_EFILE_'), date_format(lit(rundate), 'ddMMyyyy').cast("string"), lit(r'.xlsx')))

# Union logs with form code not 'APPB' and joined correspondence data
union_corr_audit = check_appb_false.select(
    col("SERIAL_NO"),
    col("IP_ADDRESS_TX"),
    col("FK_TRANSACTION_TYPE_CD"),
    col("REGISTRATION_NO"),
    col("FK_FORM_CD"),
    col("FILING_DT"),
    lit("").alias("COR_NM"),
    col("DN_PATRON_FIRST_NM"),
    col("DN_PATRON_LAST_NM"),
    col("DN_PATRON_EMAIL_ADDRESS_TX"),
    col("rundate_str"),
    col("FileName"),
    col("FileName2")
).union(
    cor_base_appb_true_join.select(
        col("SERIAL_NO"),
        col("IP_ADDRESS_TX"),
        col("FK_TRANSACTION_TYPE_CD"),
        col("REGISTRATION_NO"),
        col("FK_FORM_CD"),
        col("FILING_DT"),
        lit("").alias("COR_NM"),
        col("DN_PATRON_FIRST_NM"),
        col("DN_PATRON_LAST_NM"),
        col("DN_PATRON_EMAIL_ADDRESS_TX"),
        col("rundate_str"),
        col("FileName"),
        col("FileName2")
    )
)

# Add tiebreaker column and generate unique record IDs
union_corr_audit = union_corr_audit.withColumn("tiebreaker", concat(coalesce(col("SERIAL_NO"), lit("")), coalesce(col("REGISTRATION_NO"), lit("")), coalesce(col("IP_ADDRESS_TX"), lit("")), coalesce(col("DN_PATRON_EMAIL_ADDRESS_TX"), lit(""))))
windowSpec = Window.orderBy("tiebreaker")
union_corr_audit = union_corr_audit.withColumn("RecordID", row_number().over(windowSpec))

# Select and group final columns
df = union_corr_audit.select(
   col("DN_PATRON_EMAIL_ADDRESS_TX").alias("EMAIL_TX"),
    col("FILING_DT"),
    trim(col("SERIAL_NO")).cast('int').alias("SERIAL_NO"),
    col("REGISTRATION_NO"),
    initcap(col("DN_PATRON_FIRST_NM")).alias("FIRST_NM"),
    initcap(col("DN_PATRON_LAST_NM")).alias("LAST_NM"),
    col("FileName2").alias("FILE_NAME"),
    col("FK_FORM_CD"),
    col("RecordID").alias("RECORD_ID"),
    col("rundate_str").alias("RUN_DATE")
).distinct()

# COMMAND ----------

# DBTITLE 1,- Display DataFrame Contents
df.display()

# COMMAND ----------

# DBTITLE 1,- Display File Errors
file_errors.display()

# COMMAND ----------

# DBTITLE 1,Filter DataFrame Based on EFILE Containment
df_not_contain_efile = df.where(~col('FILE_NAME').contains('EFILE'))
df_contains_efile = df.where(col('FILE_NAME').contains('EFILE'))


# COMMAND ----------

df_not_contain_efile.display()

# COMMAND ----------

df_contains_efile.display()

# COMMAND ----------

# DBTITLE 1,- Convert DataFrames to Pandas Format
df_not_contain_efile_pandas = df_not_contain_efile.toPandas()
df_contains_efile_pandas = df_contains_efile.toPandas()

# COMMAND ----------

# DBTITLE 1,Rename DataFrame Columns for Consistency
df_not_contain_efile_pandas.rename(columns={'FILE_NAME': 'FILENAME2', 'RECORD_ID': 'RecordID'}, inplace=True)
df_contains_efile_pandas.rename(columns={'FILE_NAME': 'FILENAME2', 'RECORD_ID': 'RecordID'}, inplace=True)

# COMMAND ----------

df_not_contain_efile_pandas.drop(columns=['RUN_DATE'], inplace=True)
df_contains_efile_pandas.drop(columns=['RUN_DATE'], inplace=True)

# COMMAND ----------

# DBTITLE 1,- Generate Unique Checkpoint Directory
import time
import secrets
def generate_64bit_ID()-> int:
    return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
CHK_POINT_DIR = "/tmp/checkpoints/file_upload/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR

# COMMAND ----------

# DBTITLE 1,Upload File to API Endpoint
def file_upload(api_endpoint, api_token, file_name):
    with open(file_name, 'rb') as file:
        headers = {
            'X-API-TOKEN': api_token
        }
        files = {
            'file': file
        }
        try:
            response = requests.post(api_endpoint, headers=headers, files=files)
        except Exception as e:
            return str(e)
    if response.status_code != 200:
        return (f"File upload failed for {file_name}")
    else:
        return(f"File was uploaded successfully for {file_name}")

# COMMAND ----------

# DBTITLE 1,- Set Email Parameters and Receiver Details
from_addr = 'Trademark_Analytics@uspto.gov'
emailid = receiver_email
parms = {}

# COMMAND ----------

# DBTITLE 1,- Save and Upload DataFrames as CSV and Excel Files
import pandas as pd
import tempfile, os, time

def save_files_to_temp_email_and_upload(df_not_contain_efile_pandas, df_contains_efile_pandas, environment, api_endpoint, automation_token, api_token):
    
        # Save the DataFrame to a temporary directory as an Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        teas = f"{tmpdir}/TEAS_UPLOAD.csv"
        efile = f"{tmpdir}/EFILE_UPLOAD.csv"
        teas_xlsx = f"{tmpdir}/TEAS_UPLOAD.xlsx"
        efile_xlsx = f"{tmpdir}/EFILE_UPLOAD.xlsx"
        df_not_contain_efile_pandas.to_csv(teas, index=False)
        df_contains_efile_pandas.to_csv(efile, index=False)
        df_not_contain_efile_pandas.to_excel(teas_xlsx, index=False)
        df_contains_efile_pandas.to_excel(efile_xlsx, index=False)
        file_list = [teas, efile]
        if environment == 'prod':
            for file_element, token in zip(file_list, automation_token.values()):
                api_endpoint_formatted = api_endpoint.format(token)
                print(file_upload(api_endpoint_formatted, api_token, file_element))
        
        
        attachments = [teas_xlsx, efile_xlsx]

        email_subj = f"""See Attached Data for Qualtrics Upload {dbx_env} environment"""
        email_body = """
                Attached is the CSV file for transmission to Qualtrics.  This contains the email addresses for yesterday's TEAS filings. 
                <br><br>
                I hope this is helpful. 
            """
        # Send the email with the attachment
        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to = emailid,
            html_body= email_body,
            attachments = attachments
        )

        print("email was successfully sent")
    
    return teas, efile

# COMMAND ----------

# DBTITLE 1,Save and Upload Files Based on EFILE Containment
file_list = save_files_to_temp_email_and_upload(df_not_contain_efile_pandas, df_contains_efile_pandas, dbx_env, api_endpoint, automation_token, api_token)

# COMMAND ----------

# DBTITLE 1,write to cs_survey_trm_efile

df.write.mode("append").format("delta").insertInto(f"{trm_reporting_catalog}.gold.email_address_for_cx_survey_trm_efile")


# COMMAND ----------

# DBTITLE 1,write to cx_survey_file_errors
file_errors.write.mode("append").format("delta").insertInto(f"{trm_reporting_catalog}.gold.cx_survey_file_errors")

# COMMAND ----------

# DBTITLE 1,automated data quality check
#############################################################################################
# 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
# Allows disabling Alteryx workflow schedule fully, saving resources.                       #
#############################################################################################


# df_altrx = spark.sql(f"""select  * except(recordid, filename2) from  hive_metastore.{altrx_schema}.cx_survey_both_outputs""")
# df_dbx = spark.sql(f"""select * except(file_name, record_id, run_date) from {trm_reporting_catalog}.gold.email_address_for_cx_survey_trm_efile where run_date = '{rundate.strftime('%d%m%Y')}'""").withColumn(
#   'filing_dt', col('filing_dt').astype(DateType())
# )

# df_altrx = df_altrx.select(sorted([x.lower() for x in df_altrx.columns]))
# df_dbx = df_dbx.select(sorted([x.lower() for x in df_dbx.columns]))

# key_cols = ['serial_no', 'registration_no', 'filing_dt']

# dq_result = detailed_data_match(df_altrx, df_dbx, key_cols)

# alteryx_table = f"hive_metastore.{altrx_schema}.cx_survey_both_outputs"
# dbx_table =f"""{trm_reporting_catalog}.gold.email_address_for_cx_survey_trm_efile"""
# proc_ctgry_cd = 'REPORTS'
# src_sys_name = 'TRM_REPORTS'

# print(dq_result)
# insert_to_dq(job_name, alteryx_table, dbx_table, proc_ctgry_cd, src_sys_name, str(dq_result), dq_catalog)



# COMMAND ----------

# DBTITLE 1,End Job Control and Exit Notebook
end_job_cntl(
    f"{trm_reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed with {df.count()} records.")

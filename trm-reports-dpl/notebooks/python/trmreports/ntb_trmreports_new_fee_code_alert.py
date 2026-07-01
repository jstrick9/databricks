# Databricks notebook source
from pyspark.sql.functions import regexp_substr
from pretty_html_table import build_table

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmworker_catalog = common_configs['schema']['tmworker_catalog']
edw_scope = common_configs['secrets']['edw_scope']
to_addr = common_configs['alerting']['new_fee_cd']['email']
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']

# FEE table doesn't exist on dev server, instead FEE_2
if dbx_env == 'prod':
    ip2_tbl = 'FEE'
else:
    ip2_tbl = 'FEE_2'

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_new_fee_code_alert'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

#### define local function for non-attachment emails with html content in body

def send_mail_short(
    send_from: str,
    send_to: str,
    subject: str,
    text: str,
    server: str = "mailer.uspto.gov",
):
    try:
        msg = MIMEMultipart()
        msg["From"] = send_from
        msg["To"] = COMMASPACE.join(send_to.split(","))
        msg["Subject"] = subject

        part = MIMEText(text, 'html')
        msg.attach(part)

        smtp = smtplib.SMTP(server)
        smtp.sendmail(send_from, send_to.split(","), msg.as_string())
        smtp.close()
    except Exception as error:
        print("An issue occured during the email sending process.")
        raise error


# COMMAND ----------

# known fee codes
known_fee_cds = "'6001', '6002', '6003','6004', '6005', '6006', '6008', '6201', '6203', '6204', '6205', '6206', '6207', '6208', '6209', '6211', '6212', '6213', '6214', '6215', '6216', '6401', '6402', '6403', '6404', '6901', '6902', '6903', '6905', '6906', '6907', '6908', '6951', '6991', '6992', '6993', '6994', '6999', '7001', '7002', '7003', '7004', '7005', '7006', '7007', '7008', '7009', '7201', '7203', '7204', '7205', '7206', '7207', '7208', '7210', '7211', '7212', '7213', '7214', '7215', '7216', '7401', '7402', '7403', '7404', '7405', '7901', '7902', '7903', '7904', '7905', '7906', '7907', '7908', '7931', '7932', '7933', '7951', '7953', '7954', '8031', '8501', '8503', '8504', '8507', '8508', '8513', '8514', '8521', '8522', '8523', '8524', '8531', '8532', '8533', '8534', '8902', '8903', '9101', '9201', '9202', '9209', '9702', '9705', '7010', '7011', '7012', '7013', '7406', '7407', '7408', '7014', '7015', '7016', '7017', '7018', '7019', '7020'"

# COMMAND ----------

input_query1 = f"select distinct REV_SRC_CD, PRJCT_CD from FORECAST.VW_TM_SALE_TRAN WHERE REV_SRC_CD NOT IN ({known_fee_cds})"
ip_df1 = read_data_from_oracle_conn_dsu_cmn(input_query1,edw_scope)

input_query2 = f"select FEE_CD, FEE_NM FROM FORECAST.{ip2_tbl}"
ip_df2 = read_data_from_oracle_conn_dsu_cmn(input_query2,edw_scope)

# COMMAND ----------

df_20 = ip_df1.join(ip_df2, ip_df1.REV_SRC_CD == ip_df2.FEE_CD, "left")

# COMMAND ----------

df_13 = df_20.select("REV_SRC_CD", "FEE_NM", "PRJCT_CD").distinct()

# COMMAND ----------

# set column ordering
df_out = df_13.select(
    'REV_SRC_CD',
    'FEE_NM',
    'PRJCT_CD'
)

# COMMAND ----------

recs_count = df_out.count()
print(recs_count)

# write to table
df_out.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.new_fee_codes")
print("Records written to table")

if recs_count > 0:

    # convert to html table and send in email
    df_pd = df_out.toPandas()
    email_body = '<html><body>' + build_table(df_pd, 'grey_light') + '</body></html>'

    from_addr = 'Trademark_Analytics@uspto.gov'
    email_subj = 'New Fee Codes to Examine - !May impact workflows that use fees!'

    # Send the email
    send_email_report(
        job_nm = job_name,
        subject = email_subj,
        send_from = from_addr,
        send_to = to_addr,
        html_body= email_body
    )

    print("Notification email sent")

    #############################################################################################
    # 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
    # Allows disabling Alteryx workflow schedule fully, saving resources.                       #
    #############################################################################################

    # # data quality entry
    # tbl1 = f"{reporting_catalog}.gold.new_fee_codes"
    # tbl2 = f"hive_metastore.{altrx_schema}.new_fee_codes"
    # key_cols = ['REV_SRC_CD', 'PRJCT_CD']
    
    # dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
    # print("Data quality entry completed")

    # end job control

    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
else:
    # end job control

    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

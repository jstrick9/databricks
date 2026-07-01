# Databricks notebook source
from pyspark.sql.functions import *

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
edw_scope = common_configs['secrets']['edw_scope']
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']
to_addr = common_configs['alerting']['fee_discrepancy']['email']
cc_addr = common_configs['alerting']['fee_discrepancy']['cc']

## EDW connection details
host = dbutils.secrets.get(scope=edw_scope, key="host")
port = dbutils.secrets.get(scope=edw_scope, key="port")
db_name = dbutils.secrets.get(scope=edw_scope, key="db_name")

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_fee_discrepancy_report'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

ip_query_edw = "select * from FORECAST.VW_TM_SALE_TRAN"
df_ip_edw = read_data_from_oracle_conn_dsu_cmn(ip_query_edw,edw_scope)

# COMMAND ----------

df_milestone = spark.sql(f"select * from {reporting_catalog}.silver.milestone")
df_biblo = spark.sql(f"select * from {reporting_catalog}.silver.bibliography")
df_class = spark.sql(f"select * from {reporting_catalog}.silver.class")
df_ph = spark.sql(f"select * from {reporting_catalog}.silver.prosecution_history")

# COMMAND ----------

df_5 = df_milestone.join(df_biblo, "ser_num", "inner")

# COMMAND ----------

df_8 = df_ip_edw.filter(col("rev_src_cd").isin(6001, 7001, 7007, 7009, 7931, 7933))

# COMMAND ----------

df_3 = df_8.join(df_5, df_8.PRJCT_CD == df_5.ser_num, "inner")

# COMMAND ----------

df_4 = df_3.withColumn(
    "days_btw_posted_and_pend_start_dt", date_diff("acctg_dt", "pendency_cal_start_dt")
).withColumn(
    "fee_flag", when(((col("filing_basis_fil") == "MADRID") & (col("days_btw_posted_and_pend_start_dt") <= 50)) | ((col("filing_basis_fil") != "MADRID") & (col("days_btw_posted_and_pend_start_dt") <= 15)), lit(1)).otherwise(lit(0))
).withColumn(
    "registration_flag", when(col("registration_dt").isNull() | (col("acctg_dt") < col("registration_dt")), lit(1)).otherwise(lit(0))
)

# COMMAND ----------

df_6 = df_4.filter((col("registration_flag") == 1) & (col("filing_fy") >= 2010))

# COMMAND ----------

df_7 = df_6.filter(col("fee_flag") == 1)

df_11 = df_6.filter(col("tran_am") < 0).select("prjct_cd").distinct().withColumn(
    "credit_flag", lit(1)
)

df_12 = df_7.join(df_11, "PRJCT_CD", "left")

# COMMAND ----------

df_13 = df_12.withColumn(
    "tran_status_cd", when(col("credit_flag") == 1, lit("A")).otherwise(col("tran_status_cd"))
).withColumn(
    "unit_qt", when((col("tran_status_cd") == "R") | (col("tran_am") < 0), lit(0)).otherwise(col("unit_qt"))
)

df_17 = df_13.groupBy("ser_num").agg(sum("unit_qt").alias("fixed_count"))

# COMMAND ----------

df_19 = df_6.withColumn(
    "unit_qt", when((col("tran_status_cd") == "R") | (col("tran_am") < 0), lit(0)).otherwise(col("unit_qt"))
).groupBy("ser_num").agg(sum("unit_qt").alias("realtime_count").astype(IntegerType()))

# COMMAND ----------

df_26 = df_17.join(df_19, "ser_num", "right").fillna(0, subset=['fixed_count', 'realtime_count'])

# COMMAND ----------

df_27 = df_class.filter(col("class_status") != "INACTIVE-Insufficient Fee Received").join(df_biblo, "ser_num", "inner")

# COMMAND ----------

df_29 = df_27.filter(col("am_stat").isin(630, 638)).groupBy("ser_num", col("am_stat").alias("tram_status")).agg(countDistinct("class").alias("tram_classes"))

# COMMAND ----------

df_30 = df_26.join(df_29, "ser_num").withColumnRenamed(
    'realtime_count', 'fees_paid'
).drop('fixed_count')

# COMMAND ----------

df_47 = df_30.join(
    df_ph.filter(col("ph_action_code").isin("PARI", "DRRR")).select(col("serial_number").alias("ser_num")).distinct(), "ser_num", "anti"
)

# COMMAND ----------

df_31 = df_47.withColumn(
    "delta", (col("fees_paid") - col("tram_classes")).cast(IntegerType())
).withColumn(
    "discrepancy_type", when(col("delta") < 0, lit("Underpayment")).otherwise(lit("Overpayment"))
)

# COMMAND ----------

df_54 = df_31.filter((col("delta") != 0) & (col("discrepancy_type") == "Underpayment"))

# COMMAND ----------

# set ordering
df_out = df_54.select(
    'ser_num',
    'fees_paid',
    'tram_classes',
    'tram_status',
    'delta',
    'discrepancy_type'
).withColumn(
    "create_ts", from_utc_timestamp(current_timestamp(), 'US/Eastern')
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", from_utc_timestamp(current_timestamp(), 'US/Eastern')
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

recs_count = df_out.count()

if recs_count > 0:
    df_out.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.fee_discrepancy")

    from_addr = 'Trademark_Analytics@uspto.gov'
    file_nm = 'TRAM Fee Discrepancies.xlsx'
    email_body = 'See Attached'
    email_subj = 'TRAM Fee Discrepancies'

    attachments = [(df_out, file_nm, 'excel')]

    # Send the email
    send_email_report(
        job_nm = job_name,
        subject = email_subj,
        send_from = from_addr,
        send_to = to_addr,
        send_to_cc=cc_addr,
        html_body= email_body,
        attachments=attachments
    )

    #############################################################################################
    # 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
    # Allows disabling Alteryx workflow schedule fully, saving resources.                       #
    #############################################################################################

    # # data quality entry
    # tbl1 = f"hive_metastore.{altrx_schema}.tram_fee_discrepancy_report"
    # tbl2 = f"{reporting_catalog}.gold.fee_discrepancy"
    # key_cols = ['ser_num']
    
    # dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
    # print("Data quality entry completed")

    # end job control

    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
else:
    # end job control

    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

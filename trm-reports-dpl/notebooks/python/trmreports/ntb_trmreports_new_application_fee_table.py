# Databricks notebook source
from pyspark.sql.functions import when, col, expr, sum

# COMMAND ----------

# DBTITLE 1,Set Widgets
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,Set Config File
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Load Config File
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
edw_scope = common_configs['secrets']['edw_scope']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_new_application_fee_table'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# DBTITLE 1,Set max Calendar Day from target table
# max calendar day is used to determine what data needs to be retrieved and loaded since this is an append only table creation.
max_calendar_day = spark.sql(f"""Select  nvl(max(acc_dt),to_date('01-01-1900','dd-MM-yyyy')) as max_calendar_day from {reporting_catalog}.bronze.new_application_fees""").collect()[0][0]

# COMMAND ----------

# DBTITLE 1,EDW Data
#this should only pull the new fee code transactions for the past 24 hrs when scheduled daily or from the max calendar day.
# new fee codes are '7018', '7019', '7020'
input_query1 = f"""SELECT DISTINCT ACCTG_DT AS acc_dt, PRJCT_CD AS ser_num, REV_SRC_CD AS fee_code FROM FORECAST.VW_TM_SALE_TRAN WHERE REV_SRC_CD in ('7018', '7019', '7020') AND TRAN_AM > 0 AND TRAN_STATUS_CD <> 'R' AND ACCTG_DT >= to_date('{max_calendar_day}', 'yyyy-MM-dd HH24:MI:SS')"""

new_fees_df = read_data_from_oracle_conn_dsu_cmn(input_query1,edw_scope)


# COMMAND ----------

# DBTITLE 1,End Job If Dataframe is empty.
# don't think the df will ever be empty as it will always at least retrieve the last date record in the table, but this catches it and exits the notebook if for some reason it is.
if new_fees_df.isEmpty():
    recs_count = 0
    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed',recs_count,"job completed successfully")
    dbutils.notebook.exit("DataFrame is empty, exiting notebook")

# COMMAND ----------

# DBTITLE 1,Create fee categories and classify
#assigns each fee code to its category
categories = ["is_insufficient", "is_free_form", "is_>1000"]

df_categories = new_fees_df.withColumn("category", when(col("fee_code")==7018, "is_insufficient")
                                       .when(col("fee_code")==7019, "is_free_form")
                                       .when(col("fee_code")==7020, "is_>1000"))

# COMMAND ----------

# DBTITLE 1,Assign boolean to category
#iterate through categories and create new column for category and assign 1 or 0
for cat in categories:
    df_categories = df_categories.withColumn(cat, when(col("category")== cat, 1).otherwise(0))

# COMMAND ----------

# DBTITLE 1,Select only necessary columns
df_selected = df_categories.select("acc_dt", "ser_num", "is_insufficient", "is_free_form", "is_>1000")


# COMMAND ----------

# DBTITLE 1,Group Data by Date and Serial
#Group by serial and accounting date to get one row per serial and date
grouped_df = df_selected.groupBy("acc_dt", "ser_num").agg(sum("is_insufficient").alias("is_insufficient"), sum("is_free_form").alias("is_free_form"), sum("is_>1000").alias("is_>1000"))

# COMMAND ----------

df_sum = grouped_df.withColumn("new_fees_total", col("is_insufficient") + col("is_free_form") + col("is_>1000"))

# COMMAND ----------

df_sum.createOrReplaceTempView("fees_temp")
recs_count = spark.sql(f"""SELECT count(*) FROM fees_temp""").head()[0]
    
try:
    spark.sql(f"""MERGE INTO {reporting_catalog}.bronze.new_application_fees AS trgt
    USING fees_temp AS src
    ON trgt.acc_dt = src.acc_dt
    AND trgt.ser_num = src.ser_num
    WHEN MATCHED THEN UPDATE SET trgt.is_insufficient = src.is_insufficient, trgt.is_free_form = src.is_free_form, trgt.`is_>1000` = src.`is_>1000`, trgt.new_fees_total = src.new_fees_total, trgt.create_ts = current_timestamp(), trgt.create_user_id = 'etl'

    WHEN NOT MATCHED THEN
    INSERT(acc_dt, ser_num, is_insufficient, is_free_form, `is_>1000`, new_fees_total, create_ts, create_user_id)
     VALUES(acc_dt, ser_num, is_insufficient, is_free_form, `is_>1000`, new_fees_total, current_timestamp(), 'etl')
    """)
    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed',recs_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'failed',0,e)
    raise

# Databricks notebook source
# MAGIC %md
# MAGIC ### **ntb_tmreports_first_action**

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
tmworker_catalog = common_configs['schema']['tmworker_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
print(reporting_catalog,altrx_schema,data_quality_catalog,tmworker_catalog)
data_layer = "bronze"

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_trmreports_first_action'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# %pip install pandas openpyxl

# COMMAND ----------

# %pip install boto3

# COMMAND ----------

# DBTITLE 1,* Data Import and Function Definitions
import boto3
import pandas as pd
from io import BytesIO
from pyspark.sql.functions import date_format, col, expr , first, countDistinct, sum, dayofweek, when, count


# COMMAND ----------

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the S3 bucket and file key
file_key = "eds/trademark/firstActionPayPeriod/Pay_Periods_FY2006-2032.xlsx"

try:
    # Get the file object from S3
    s3_object = s3_client.get_object(Bucket=cdc_bucket, Key=file_key)
    # Read the file content into a BytesIO object
    file_content = BytesIO(s3_object['Body'].read())
   
    # Load the Excel file into a pandas DataFrame
    input_pp = pd.read_excel(file_content)
    
    # Convert the pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(input_pp)
    spark_df.printSchema()
    
except Exception as e:
    exception = str(e)
    status = "There are no new files"


# COMMAND ----------

# DBTITLE 1,Set Date Format and Rename Columns
input_3 = spark_df.withColumn("PP_BEGIN_DT", date_format("PP_BEGIN_DT1", "yyyy-MM-dd").cast("date"))\
.withColumn("PP_END_DT", date_format("PP_END_DT1", "yyyy-MM-dd").cast("date")) \
  .withColumnRenamed("YYYYPP","PayPd")\
  .drop("PP_BEGIN_DT1","PP_END_DT1","Unnamed: 2","FY")

# display(input_3)

# COMMAND ----------

# DBTITLE 1,Generates Date Sequences for Each Pay Period
gen_row = input_3.withColumn("DATE", expr("sequence(PP_BEGIN_DT, PP_END_DT, interval 1 day)"))

gen_row4 = gen_row.withColumn("DATE", expr("explode(DATE)"))

gen_row4 = gen_row4.select("PP_BEGIN_DT","PP_END_DT","PayPd","CY","PP","DATE")
# display(gen_row4.count()) count Match

# COMMAND ----------

# DBTITLE 1,Want start of previous Fiscal Year as Dynamic Input
FY = expr("CASE WHEN month(current_date()) >= 10 THEN year(current_date()) - 1 ELSE year(current_date()) - 2 END")
StartDate = expr("concat(FY, '-10-01')")

start_date_df = spark.range(1).select(FY.alias("FY"), StartDate.alias("StartDate"))
start_date_df.show()
start_date = start_date_df.select("StartDate").collect()[0]["StartDate"]

dyn_input_95 = spark.sql(f"""select fa.*, wk.worker_nm as EA_Name, wk.grade_ct as Grade from (
SELECT ml.ser_num,
ml.first_action_dt_ph,
ml.first_action_type,
ml.am_1_actn_ct_dt,
ml.am_cls_ct_actv,
ml.filing_fy,
ml.noa_dt_ph,
bb.LAW_OFFICE,
bb.EXMR_EID,
bb.AM_FLG_ITU_FIL
FROM {reporting_catalog}.silver.milestone ml INNER JOIN {reporting_catalog}.silver.bibliography bb ON ml.ser_num = bb.SER_NUM 
where ml.first_action_type not like '%ABANDONMENT%' 
and ml.first_action_dt_ph >= '{start_date}') fa left join {tmworker_catalog}.bronze.worker wk ON fa.EXMR_EID = wk.worker_no""")

# COMMAND ----------

clean_21 = dyn_input_95.withColumn("first_action_type", trim(col("first_action_type")))

frml_21 = clean_21.withColumn(
    "1st_act_FY",
    when(month(col("first_action_dt_ph")) <= 9, year(col("first_action_dt_ph")))\
    .otherwise(year(col("first_action_dt_ph")) + 1))\
        .withColumn("first_action_dt_ph", col("first_action_dt_ph").cast("date"))

# COMMAND ----------

# gen_row4  frml_21
join_7 = gen_row4.alias("LJ") \
  .join(frml_21.alias("RJ"),
        col("LJ.DATE") == col("RJ.first_action_dt_ph"),"inner") \
          .orderBy("LAW_OFFICE","EXMR_EID","PayPd","first_action_type")



# COMMAND ----------

unique_62 = join_7.dropDuplicates(["ser_num"])



sumrz_13 = unique_62.groupBy(
    col("LAW_OFFICE").alias("Law_office"),
    col("EXMR_EID").alias("Examiner"),
    col("EA_NAME").alias("EA_NAME"),
    col("PayPd").alias("Pay_Period"),
    col("first_action_type").alias("First_Action_Type"),
    col("DATE").alias("Date")
).agg(
    first(col("PP_BEGIN_DT")).alias("PP_Begin_DT"),
    first(col("PP_END_DT")).alias("PP_End_DT"),
    first(col("1st_act_FY")).alias("First_1st_act_FY"),
    countDistinct(col("ser_num")).alias("Cases"),
    sum(col("AM_CLS_CT_ACTV").cast("int")).alias("Classes")
).orderBy("Law_office","Examiner","Pay_Period","Date","First_Action_Type")

# display(sumrz_13)

# COMMAND ----------

# DBTITLE 1,Calculate Day of the Week Column
# frml_129 = sumrz_13.withColumn("Day_of_week", date_format(current_date(), 'E')) <-- This will fill the value with Letters 
frml_129 = sumrz_13.withColumn("Day_of_week", (dayofweek(current_date()) + 5) % 7 + 1)
# display(frml_129)


# COMMAND ----------

fltr_142 = frml_129.filter("Day_of_week == 7")
sel_100 = fltr_142.select("Examiner", "Date", "First_1st_act_FY", "Classes")

from pyspark.sql.functions import month, when, year, lit

frml_102 = (
    sel_100.withColumn("Month", month(col("Date")))
    .withColumn(
        "FYToday",
        when(month(current_date()) <= 9, year(current_date())).otherwise(
            year(current_date()) + 1
        ),
    )
    .withColumn(
        "CURFYTD",
        when(
            (col("Date") <= current_date())
            & (col("First_1st_act_FY") == col("FYToday")),
            col("Classes"),
        ).otherwise(None),
    )
    .withColumn("TodayLY", date_add(current_date(), -365))
    .withColumn(
        "PrvFYTD", when(col("Date") <= col("TodayLY"), col("Classes")).otherwise(None)
    )
)

sumr_101 = frml_102.groupBy(
    col("First_1st_act_FY").alias("First_act_FY"), col("Month"), col("Examiner")
).agg(
    sum("Classes").alias("Classes"),
    sum(col("CURFYTD")).alias("Sum_CURFYTD"),
    sum(col("PrvFYTD")).alias("Sum_PrvFYTD")
)

# COMMAND ----------

data= [(85,110,135,160,185,210)]
columns = ["Target1","Target2","Target3","Target4","Target5","Target6"]
input_104 = spark.createDataFrame(data,columns)
input_104.show()

# COMMAND ----------

# DBTITLE 1,Join Two Tables and Display Results
# Perform a Cartesian join between sumr_101 and input_104
appnd_105 = sumr_101.crossJoin(input_104)

fltr_107 = appnd_105.filter(col("Target1").isNotNull())

frml_103 = fltr_107.withColumn(
    "Award",
    when(
        (col("Classes") >= col("Target1")) & (col("Classes") < col("Target2")),
        col("Target1"),
    )
    .when(
        (col("Classes") >= col("Target2")) & (col("Classes") < col("Target3")),
        col("Target2"),
    )
    .when(
        (col("Classes") >= col("Target3")) & (col("Classes") < col("Target4")),
        col("Target3"),
    )
    .when(
        (col("Classes") >= col("Target4")) & (col("Classes") < col("Target5")),
        col("Target4"),
    )
    .when(
        (col("Classes") >= col("Target5")) & (col("Classes") < col("Target6")),
        col("Target5"),
    )
    .when(col("Classes") >= col("Target6"), col("Target6"))
    .otherwise(0),
)

sumr_108 = frml_103.groupBy("First_act_FY", "Month", "Award",).agg(
    sum(col("Classes")).alias("Classes"),
    sum(col("Sum_CURFYTD")).alias("CurFYTD_FA"),
    sum(col("Sum_PrvFYTD")).alias("PrvFYTD FA"),
    count(col("Examiner").alias("EA_Count"))
)

# COMMAND ----------

sumr_110 = sel_100.groupBy("First_1st_act_FY").agg(
    sum(col("Classes")).alias("Sum_Classes"),
    countDistinct(col("Date")).alias("CountDistinct_Date"),
    max(col("Date")).alias("Max_Date"),
)
frml_109 = sumr_110.withColumn(
    "Avg_classes", col("Sum_Classes") / col("CountDistinct_Date")
).withColumn(
    "EOYClassesPrediction",
    col("Sum_Classes") + (365 - col("CountDistinct_Date")) * col("Avg_classes")
)
sumr_101 = sel_100.agg(max(col("First_1st_act_FY")).alias("Max_FY"))

# COMMAND ----------

# DBTITLE 1,Merging DataFrames with crossJoin
appnd_112 = frml_109.crossJoin(sumr_101)

fltr_114 = appnd_112.filter(col("First_1st_act_FY") == col("Max_FY")).select("Max_Date","EOYClassesPrediction")

appnd_108 = sumr_108.crossJoin(fltr_114)
unique_62 = unique_62.withColumnRenamed("1st_act_FY","first_act_FY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final tables
# MAGIC - First_Actions.hyper file. (Need to check if we need this file.)
# MAGIC - Table_1st_actions_summery -- (sumrz_13)
# MAGIC - Table_1st_actions_Details -- (unique_62)
# MAGIC - Trademark Web hyper file (Need to check if we need this file.) -- (appnd_108)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write Data into Tables. 

# COMMAND ----------

# DBTITLE 1,Writing the data in tables
try:
    sumrz_13.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.first_actions_summary')
    unique_62.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.first_actions_details')
    #appnd_108.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.trademark_web')
    recs_count = sumrz_13.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    print('Completed Loading first_action workflow tables')
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading first_action workflow tables ")

# COMMAND ----------

# DBTITLE 1,Data Quality check
#############################################################################################
# 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
# Allows disabling Alteryx workflow schedule fully, saving resources.                       #
#############################################################################################


#  # data quality entry altrx_schema
# tbl1 = f"{reporting_catalog}.gold.first_actions_summary"
# if dbx_env == 'dev':
#   tbl2 = f"hive_metastore.{altrx_schema}.first_actions"
# else:
#   tbl2 = f"hive_metastore.{altrx_schema}.first_actions"
# key_cols = ['Cases']
# dq_catalog = data_quality_catalog
#     # job_name = job_name
# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
# print(dq_result)
# dbutils.notebook.exit(f"Completed Loading first_action workflow tables with data quality check  {dq_result}")

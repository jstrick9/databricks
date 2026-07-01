# Databricks notebook source
from pyspark.sql.functions import concat, when, length, lpad, regexp_replace, lead, startswith, date_sub, from_utc_timestamp, current_timestamp, trim, min

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
job_name = 'ntb_trmreports_employee_grade_etl'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

ip_query = "select * from DW.VW_EMP_NTR_ACTN where NOA_CD != 782"
df_ip = read_data_from_oracle_conn_dsu_cmn(ip_query,edw_scope)

# COMMAND ----------

df_26 = df_ip.withColumn(
    "employee_name_full", concat(col("last_nm"), lit(", "), col("first_nm"), lit(" "), col("middle_initial"))
).withColumn(
     "org_cd", when(length(col("org_cd")) == 6, col("org_cd")).otherwise(concat(lit(1), col("org_cd")))
).withColumn(
     'YYYYPP', concat(col("pay_period_clndr_yr"), lpad(col("pay_period_no"), 2, '0'))
).withColumn(
    "emp_no", when(col("emp_no").isNull(), regexp_replace('last_nm', r'^[6]*', '')).otherwise(col("emp_no"))
).withColumn(
    "grade", col("grade_no").astype(IntegerType())
).fillna("1", subset=['org_cd']) ## to replicate alteryx behavior of adding 1 where length < 6 even when null

# COMMAND ----------

win98 = Window().orderBy("emp_no", "YYYYPP", "noa_dt", "noa_cd")

df_98 = df_26.withColumn(
    "corrected_noa", when(
            (col("fy_no") == lead(col("fy_no")).over(win98)) & (col("emp_no") == lead(col("emp_no")).over(win98)) & (col("noa_dt") == lead(col("noa_dt")).over(win98)) & (col("noa_rvsn_cd") == '000') & (lead(col("noa_rvsn_cd")).over(win98) == '002'), lit(1)
        ).when(
            (col("fy_no") == lead(col("fy_no")).over(win98)) & (col("emp_no") == lead(col("emp_no")).over(win98)) & (col("noa_dt") == lead(col("noa_dt")).over(win98)) & (col('YYYYPP') < lead(col("YYYYPP")).over(win98)), lit(1)
        ).otherwise(lit(None))
)

# COMMAND ----------

df_99 = df_98.filter(col("corrected_noa").isNull() | (col("corrected_noa") != 1))

# COMMAND ----------

df_22 = df_99.withColumn(
    "grade_start_dt", when((startswith(col("noa_cd"), lit("1"))) | (col("noa_cd").isin(["170", "570", "702", "721", "790"])), col("noa_dt")).otherwise(lit(None))
).withColumn(
    "grade_end_dt", when(startswith(col("noa_cd"), lit("3")), col("noa_dt")).otherwise(lit(None))
)

## set to date type
df_22 = df_22.withColumn(
    "grade_start_dt", col("grade_start_dt").astype(DateType())
).withColumn(
    "grade_end_dt", col("grade_end_dt").astype(DateType())
)

# COMMAND ----------

df_64 = df_22.filter(~(col('grade_start_dt').isNull() & col('grade_end_dt').isNull()))

# COMMAND ----------

win24 = Window().partitionBy('emp_no').orderBy('emp_no', 'employee_name_full', 'noa_dt')

df_24 = df_64.withColumn(
    'grade_end_dt', when(
        col('grade_end_dt').isNotNull(), col('grade_end_dt')
    ).when(
        (col('grade') == lead(col('grade')).over(win24)) & (lead(col('grade_end_dt')).over(win24).isNotNull()), lead(col('grade_end_dt')).over(win24)
    ).when(
        (col('grade') < lead(col('grade')).over(win24)) & (lead(col('grade_end_dt')).over(win24).isNotNull()), lead(col('grade_end_dt')).over(win24)
    ).when(
        (col('grade') + 1 == lead(col('grade')).over(win24)), date_sub(lead(col('grade_start_dt')).over(win24), 1) ## subtract 1 day
    ).when(
        (col('grade') < lead(col('grade')).over(win24)), date_sub(lead(col('grade_start_dt')).over(win24), 1) ## subtract 1 day
    ).when(
        (col('grade') > lead(col('grade')).over(win24)), date_sub(lead(col('grade_start_dt')).over(win24), 1) ## subtract 1 day
    ).when(
        (col('grade') == lead(col('grade')).over(win24)) & (col("pay_period_clndr_yr") < lead(col('pay_period_clndr_yr')).over(win24)), date_sub(lead(col('grade_start_dt')).over(win24), 1) ## subtract 1 day
    ).when(
        (col('grade') == lead(col('grade')).over(win24)) & (col("pay_period_clndr_yr") == lead(col('pay_period_clndr_yr')).over(win24)) & (col("pay_period_no") <= lead(col('pay_period_no')).over(win24)), date_sub(lead(col('grade_start_dt')).over(win24), 1) ## subtract 1 day
    ).otherwise(
        from_utc_timestamp(current_timestamp(), 'US/Eastern')
    )
).withColumn(
    "grade_end_dt", col("grade_end_dt").astype(DateType())
)

# COMMAND ----------

# trim all columns
df_83 = df_24

for c_name in df_83.drop('YYYYPP', 'corrected_noa').columns:
    df_83 = df_83.withColumn(c_name, trim(col(c_name)))

# COMMAND ----------

df_137 = df_83.filter(col('grade_start_dt').isNotNull() & (col('grade_start_dt') < col('grade_end_dt')))

# COMMAND ----------

df_25 = df_137.groupBy(
    'emp_no', 'employee_name_full', 'grade', 'org_cd', 'org_nm', 'pay_period_clndr_yr'
).agg(
    min('grade_start_dt').alias('grade_start_dt'), min('grade_end_dt').alias('grade_end_dt')
)

# COMMAND ----------

df_out = df_25.withColumn(
    'tm_analytics_ts', from_utc_timestamp(current_timestamp(), 'US/Eastern')
)

# COMMAND ----------

# set ordering
df_out_edw = df_out.select(
    'emp_no',
    'employee_name_full',
    'grade',
    'org_cd',
    'org_nm',
    'pay_period_clndr_yr',
    'grade_start_dt',
    'grade_end_dt',
    'tm_analytics_ts'
)

# COMMAND ----------

### Write to EDW

edw_target_table = "EMP_GRADE"

df_out_edw.write.format("jdbc").mode("overwrite").option("url", "jdbc:oracle:thin:@"+host+":"+port+"/"+db_name)\
                          .option("dbtable",edw_target_table)\
                          .option("user", dbutils.secrets.get(scope=edw_scope, key="username"))\
                          .option("password", dbutils.secrets.get(scope=edw_scope, key="password"))\
                          .option("driver", "oracle.jdbc.OracleDriver")\
                          .save()

# COMMAND ----------

df_out_dbx = df_out_edw.drop('tm_analytics_ts').withColumn(
    "create_ts", from_utc_timestamp(current_timestamp(), 'US/Eastern')
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", from_utc_timestamp(current_timestamp(), 'US/Eastern')
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

df_out_dbx.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.employee_grade")

# COMMAND ----------

# data quality entry
#tbl1 = f"hive_metastore.{altrx_schema}.employee_grade" 
#tbl2 = f"{reporting_catalog}.silver.employee_grade"
#key_cols = ['emp_no', 'grade', 'org_cd', 'grade_start_dt']

#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

# end job control
recs_count = df_out.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

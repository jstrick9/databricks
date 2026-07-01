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
tmworker_catalog = common_configs['schema']['tmworker_catalog']
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
to_addr = common_configs['alerting']['og_issue_registrations']['email']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_og_issue_registration_report'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Input

# COMMAND ----------

trademark = spark.sql(f"select * from {tmngpdb_catalog}.bronze.trademark")

filing_basis = spark.sql(f"select * from {tmngpdb_catalog}.bronze.tm_filing_basis")

milestone = spark.sql(f"select ser_num, registration_dt from {reporting_catalog}.silver.milestone")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL

# COMMAND ----------

df_3 = trademark.join(filing_basis, trademark.trademark_gid == filing_basis.fk_trademark_gid, "inner")

df_21 = df_3.join(milestone, df_3.serial_num_tx == milestone.ser_num, "inner")

# COMMAND ----------

df_25 = df_21.withColumn(
    "filing_date", date_trunc("day", col('filing_dt'))
).withColumn(
    "date_diff", date_diff(col('registration_dt'), col("filing_date"))
)

# COMMAND ----------

df_34 = df_25.filter((col('registration_dt').isNotNull()) & (col('filing_dt').isNotNull()))

# COMMAND ----------

### existing logic to dedupe on serial number makes little to no sense IMO - basically randomly selects 1 filing basis per serial number before grouping by filing basis
# for now will be skipping dedupe on serial number

df_out = df_34.groupBy("fk_filing_basis_cd").agg(round(avg(col("date_diff")), 2).alias("date_difference")).withColumnRenamed(
    "fk_filing_basis_cd", "filing_class"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output

# COMMAND ----------

# MAGIC %pip install xlsxwriter

# COMMAND ----------

from_addr = 'Trademark_Analytics@uspto.gov'
file_nm = 'OG ad hoc Registrations.xlsx'
email_body = 'Please see attached file for registrations by OG issue date.'
email_subj = 'Registrations by OG Issue Type'
send_mail(from_addr, to_addr, from_addr, email_subj, email_body, df_out, file_nm)

# COMMAND ----------

df_out.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.og_issue_registrations")

# COMMAND ----------

# data quality entry
#tbl1 = f"hive_metastore.{altrx_schema}.og_issue_registrations" 
#tbl2 = f"{reporting_catalog}.gold.og_issue_registrations"
#key_cols = ['filing_class']
 
#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

# end job control
recs_count = df_out.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

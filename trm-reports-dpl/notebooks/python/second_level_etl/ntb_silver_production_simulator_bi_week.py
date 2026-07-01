# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
trprodvty_catalog = common_configs['schema']['tmprodvty_catalog']
tmworker_catalog = common_configs['schema']['tmworker_catalog']
tept_catalog = common_configs['schema']['tept_catalog']
alteryx_etldb_catalog = common_configs['schema']['alteryx_etldb_catalog']
print(f"{trgt_catalog=},{src_catalog=},{alteryx_etldb_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.alteryx_etldb_catalog', alteryx_etldb_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

import pytz
from pytz import timezone

job_name = "ntb_silver_production_simulator_bi_week"
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

emp_managers_df = spark.sql(f"""
SELECT wb.brs_user_id,
LO,
DN_WORKER_NO, 
DN_WORKER_TM_ORGANIZATION_CD, 
Eff_dt 
from (select DISTINCT 
PTA.DN_WORKER_NO, 
PTA.DN_WORKER_TM_ORGANIZATION_CD, 
MAX(PTA.TRANSACTION_EFFECTIVE_DT) Eff_dt,
substr(PTA.DN_WORKER_TM_ORGANIZATION_CD,3,5) LO,
RANK() OVER (PARTITION BY PTA.DN_WORKER_NO ORDER BY MAX(PTA.TRANSACTION_EFFECTIVE_DT) DESC) LO_Rank
 
FROM  {trprodvty_catalog}.bronze.production_transaction PTA
WHERE PTA.DN_WORKER_TM_ORGANIZATION_CD LIKE 'LO%' -- and DN_WORKER_NO > 50000
GROUP BY PTA.DN_WORKER_NO, PTA.DN_WORKER_TM_ORGANIZATION_CD, substr(PTA.DN_WORKER_TM_ORGANIZATION_CD,3,5), PTA.dn_worker_role_cd) LOB

LEFT JOIN (select * from {tmworker_catalog}.bronze.worker where worker_ct = 'E') WB ON LOB.DN_WORKER_NO = WB.worker_no
WHERE LOB.LO_Rank=1 and wb.brs_user_id is not null and wb.brs_user_id not ilike 'Historic%'
order by WB.brs_user_id
--AND WB.BRS_USER_ID = <Parameters.UserP>)LO2 ON LO1.DN_WORKER_TM_ORGANIZATION_CD = LO2.DN_WORKER_TM_ORGANIZATION_CD) PT1
""")

# COMMAND ----------

from pyspark.sql.functions import trim, col, when

emp_managers_df = emp_managers_df.select(
    trim(col("brs_user_id")).alias("brs_user_id"),
    trim(col("LO")).alias("LO"),
    "DN_WORKER_NO",
    "DN_WORKER_TM_ORGANIZATION_CD",
    "Eff_dt"
).orderBy(col("brs_user_id"), col("Eff_dt").desc()) \
.withColumn(
    "LO",
    when(col("DN_WORKER_NO") == '73350', 301)
    .when(col("DN_WORKER_NO") == '77656', 130)
    .when(col("DN_WORKER_NO") == '90331', 303)
    .when(col("DN_WORKER_NO") == '92454', 132)
    .when(col("DN_WORKER_NO") == '90291', 125)
    .when(col("DN_WORKER_NO") == '92827', 117)
    .when(col("DN_WORKER_NO") == '92837', 128)
    .when(col("DN_WORKER_NO") == '92989', 131)
    .when(col("DN_WORKER_NO") == '93061', 114)
    .otherwise(col("LO"))
)

# COMMAND ----------

bi_week_data_df = spark.sql(f"""
select * 
from 
	(
	select 'XXXXX' as employee_no,
		'DEFAULT' as employee_nm,
		NULL as current_organization_cd 

	union

	select distinct emp.employee_no,
		Concat(last_nm, ', ', first_nm) as employee_nm,
		current_organization_cd
	from {tept_catalog}.bronze.employee emp
		inner join {tept_catalog}.bronze.employee_fiscal_year emp_fy on emp.employee_no = emp_fy.employee_no
		inner join {tept_catalog}.bronze.stnd_quarter_bi_week bwk on bwk.fiscal_year_no = emp_fy.fiscal_year_no
	where emp.current_organization_cd <> 100 
		and bwk.fiscal_year_no = 
		(
		select distinct fiscal_year_no
		from {tept_catalog}.bronze.stnd_quarter_bi_week
		where quarter_bi_week_end_dt >= current_date()

	)) emp, 
	(
	select quarter_bi_week_start_dt,
		quarter_bi_week_end_dt,
		fiscal_year_no,
		quarter_no 
	from {tept_catalog}.bronze.stnd_quarter_bi_week 
	where fiscal_year_no = 
		(
		select distinct fiscal_year_no 
		from {tept_catalog}.bronze.stnd_quarter_bi_week
		where quarter_bi_week_end_dt >= current_date()

	)) week 
order by emp.employee_no, week.quarter_bi_week_start_dt""")

# COMMAND ----------

from pyspark.sql.functions import countDistinct, first, lit

quarter_sum_df = bi_week_data_df.groupBy("quarter_no") \
    .agg(countDistinct("quarter_bi_week_start_dt").alias("qtr_wks"))

quarter_pivot_df = quarter_sum_df.groupBy().pivot("quarter_no", [1, 2, 3, 4]).agg(first("qtr_wks")) \
    .withColumnRenamed("1", "q1_wks") \
    .withColumnRenamed("2", "q2_wks") \
    .withColumnRenamed("3", "q3_wks") \
    .withColumnRenamed("4", "q4_wks")

# Add columns from quarter_pivot_df to bi_week_data_df
quarter_values = quarter_pivot_df.collect()[0].asDict()

for col_name, value in quarter_values.items():
    bi_week_data_df = bi_week_data_df.withColumn(col_name, lit(value))


# COMMAND ----------

from pyspark.sql.functions import col

joined_df_inner = emp_managers_df.join(
    bi_week_data_df,
    emp_managers_df["DN_WORKER_NO"] == bi_week_data_df["employee_no"],
    "inner"
).select(
    bi_week_data_df["fiscal_year_no"],
    bi_week_data_df["quarter_no"],
    bi_week_data_df["q1_wks"],
    bi_week_data_df["q2_wks"],
    bi_week_data_df["q3_wks"],
    bi_week_data_df["q4_wks"],
    emp_managers_df["brs_user_id"],
    emp_managers_df["LO"],
    bi_week_data_df["employee_no"],
    bi_week_data_df["employee_nm"],
    bi_week_data_df["current_organization_cd"],
    bi_week_data_df["quarter_bi_week_start_dt"],
    bi_week_data_df["quarter_bi_week_end_dt"]
)

#joined_df_inner.count()

# COMMAND ----------

from pyspark.sql.functions import col

joined_df_left = bi_week_data_df.join(
    emp_managers_df,
    emp_managers_df["DN_WORKER_NO"] == bi_week_data_df["employee_no"],
    "left_anti"
).select(
    bi_week_data_df["employee_no"],
    bi_week_data_df["employee_nm"],
    bi_week_data_df["current_organization_cd"],
    bi_week_data_df["quarter_bi_week_start_dt"],
    bi_week_data_df["quarter_bi_week_end_dt"],
    bi_week_data_df["fiscal_year_no"],
    bi_week_data_df["quarter_no"],
    bi_week_data_df["q1_wks"],
    bi_week_data_df["q2_wks"],
    bi_week_data_df["q3_wks"],
    bi_week_data_df["q4_wks"],
    #emp_managers_df["brs_user_id"],
    #emp_managers_df["LO"],
)

#joined_df_left.count()

# COMMAND ----------

from pyspark.sql.functions import lit

# Select columns from joined_df_inner and add TEPT_LO
joined_inner_selected = joined_df_inner.select(
    col("fiscal_year_no").alias("fiscal_year_no"),
    col("quarter_no").alias("quarter_no"),
    col("q1_wks"),
    col("q2_wks"),
    col("q3_wks"),
    col("q4_wks"),
    col("brs_user_id"),
    #col("LO"),
    col("employee_no"),
    col("employee_nm"),
    col("current_organization_cd"),
    col("quarter_bi_week_start_dt"),
    col("quarter_bi_week_end_dt")
).withColumn("TEPT_LO", col("current_organization_cd"))

# Select columns from joined_df_left, set brs_user_id to 'DEFAULT', add LO as null, add TEPT_LO
joined_left_selected = joined_df_left.select(
    col("employee_no"),
    col("employee_nm"),
    col("current_organization_cd"),
    col("quarter_bi_week_start_dt"),
    col("quarter_bi_week_end_dt"),
    col("fiscal_year_no"),
    col("quarter_no"),
    #col("LO"),
    col("q1_wks"),
    col("q2_wks"),
    col("q3_wks"),
    col("q4_wks")
).withColumn("brs_user_id", lit("DEFAULT")) \
 .withColumn("TEPT_LO", col("current_organization_cd"))

# Union both DataFrames
final_df = joined_inner_selected.unionByName(joined_left_selected)

#display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").insertInto(f"{trgt_catalog}.silver.employee_quarter_bi_week")

# COMMAND ----------

end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'completed',0,"job completed successfully")
dbutils.notebook.exit(f"Completed loading Production Simulator Bi-Week Data")
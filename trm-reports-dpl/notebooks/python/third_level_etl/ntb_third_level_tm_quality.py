# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Set parameter values
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['reporting_catalog']
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_third_level_tm_quality'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Main ETL

# COMMAND ----------

# 63
df_milestone = spark.sql(f"select * from {reporting_catalog}.silver.milestone")

# 65
df_biblo = spark.sql(f"select * from {reporting_catalog}.silver.bibliography")

# 67
df_owner = spark.sql(f"select * from {reporting_catalog}.silver.owner")

# 69
df_class = spark.sql(f"select * from {reporting_catalog}.silver.class")

# COMMAND ----------

# 32
df_32 = df_class.filter(~(col("class_status").isin(["INACTIVE-Insufficient Fee Received", ""])))

# COMMAND ----------

# 33
df_33 = df_32.groupBy("ser_num").agg(concat_ws(';', collect_list(col("class"))).alias("concat_class"))
df_33 = df_33.withColumn("concat_class", concat(lit(';'), col("concat_class"), lit(';')))

# COMMAND ----------

# 5
df_owner_min = spark.sql(f"select ser_num, min(party_type) as party_type from {reporting_catalog}.silver.owner group by ser_num")
# 6
df_owner = df_owner.join(df_owner_min, ['ser_num','party_type'])
# 7
df_owner = df_owner.filter(col("owner_num") == 1)

# COMMAND ----------

# 4
mj = df_biblo.join(df_owner, "ser_num", "full_outer").join(df_33, "ser_num", "full_outer")
mj = mj.select("ser_num", "country_or_area_name", "filing_basis_grp", "filing_method_filed", "state_cd", "concat_class")

# COMMAND ----------

# 11
df_milestone = df_milestone.select("ser_num", "first_action_type", "disposal_type", "pendency_cal_start_dt", "pendency_cal_end_dt", "non_pro_se")
df_11 = df_milestone.join(mj, "ser_num")

# COMMAND ----------

# 12
df_12 = df_11.withColumn(
    "ste_ctry_cd", expr("case when   state_cd ='AL' THEN 'AL'   when   state_cd ='AK' THEN 'AK'    when   state_cd ='AZ' THEN 'AZ'   when   state_cd ='AR' THEN 'AR'   when   state_cd ='CA' THEN 'CA'    when   state_cd ='CO' THEN 'CO'   when   state_cd ='CT' THEN 'CT'   when   state_cd ='DC' THEN 'DC'  when   state_cd ='DE' THEN 'DE'    when   state_cd ='FL' THEN 'FL'   when   state_cd ='GA' THEN 'GA'   when   state_cd ='HI' THEN 'HI'   when   state_cd ='ID' THEN 'ID'    when   state_cd ='IL' THEN 'IL'   when   state_cd ='IN' THEN 'IN'   when   state_cd ='IA' THEN 'IA'    when   state_cd ='KS' THEN 'KS'   when   state_cd ='KY' THEN 'KY'   when   state_cd ='LA' THEN 'LA'    when   state_cd ='ME' THEN 'ME'   when   state_cd ='MD' THEN 'MD'   when   state_cd ='MA' THEN 'MA'    when   state_cd ='MI' THEN 'MI'   when   state_cd ='MN' THEN 'MN'   when   state_cd ='MS' THEN 'MS'    when   state_cd ='MO' THEN 'MO'   when   state_cd ='MT' THEN 'MT'   when   state_cd ='NE' THEN 'NE'    when   state_cd ='NV' THEN 'NV'   when   state_cd ='NH' THEN 'NH'   when   state_cd ='NJ' THEN 'NJ'    when   state_cd ='NM' THEN 'NM'   when   state_cd ='NY' THEN 'NY'   when   state_cd ='NC' THEN 'NC'    when   state_cd ='ND' THEN 'ND'   when   state_cd ='OH' THEN 'OH'   when   state_cd ='OK' THEN 'OK'    when   state_cd ='OR' THEN 'OR'   when   state_cd ='PA' THEN 'PA'   when   state_cd ='RI' THEN 'RI'   when   state_cd ='SC' THEN 'SC'   when   state_cd ='SD' THEN 'SD'   when   state_cd ='TN' THEN 'TN'   when   state_cd ='TX' THEN 'TX'   when   state_cd ='UT' THEN 'UT'   when   state_cd ='VT' THEN 'VT'   when   state_cd ='VA' THEN 'VA'   when   state_cd ='WA' THEN 'WA'   when   state_cd ='WV' THEN 'WV'   when   state_cd ='WI' THEN 'WI'   when   state_cd ='WY' THEN 'WY' ELSE 'Other'  end ")
).fillna(
    "Unknown", subset=["country_or_area_name"]
).withColumn(
    "filing_basis_grp", when(col("filing_basis_grp").contains("MULTIPLE"), "MULTI-BASIS").otherwise(col("filing_basis_grp"))
)

# COMMAND ----------

# 1
tqr_detail_metrics = spark.sql(f"select * from {reporting_catalog}.silver.tqr_detail_metrics")

# COMMAND ----------

# 61
df_61 = tqr_detail_metrics.withColumn("lastreviewdatetime", col("completedatetime"))
# 35
df_35 = df_61.filter(col("lastreviewdatetime") >= "2018-10-01")
# 13
df_13 = df_35.drop("examineremployeenumber")

# COMMAND ----------

# 14
df_14 = df_13.withColumn(
    "review_type", expr("case when reviewtypecode = '100' then 'First Action' when reviewtypecode = '101' then 'Final Action' when reviewtypecode = '102' then 'PUB' when reviewtypecode = '103' then 'SOU' else Null end ")
).withColumn(
    "final_compliance", (col("reviewtypecode").isin(['101', '102']))
).withColumn(
    "qualitymetricdeficientflag", when(col("qualitymetricdeficientindicator") == True, lit("Deficiency")).otherwise(lit("Compliant"))
).withColumn(
    "excellentflag", when(col("overallexcellentindicator") == True, lit("Excellent")).otherwise(lit("Not Excellent"))
)

# COMMAND ----------

# 15
max_date = tqr_detail_metrics.select(max("lastreviewdatetime").alias("max_date")).collect()[0][0]

# COMMAND ----------

# 16
df_16 = df_14.select(
    "trademarkserialnumber",
    col("organizationcode").alias("law_office"),
    "lastreviewdatetime",
    "searchsufficientindicator",
    "qualitymetricdeficientindicator",
    "mississueindicator",
    "newissueindicator",
    "refusalunsoundindicator",
    "substantivedeficientindicator",
    "proceduraldeficientindicator",
    "overalldeficientindicator",
    "overallexcellentindicator",
    "evidencedeficientindicator",
    "evidencesatisfactoryindicator",
    "evidenceexcellentindicator",
    "writingdeficientindicator",
    "writingsatisfactoryindicator",
    "writingexcellentindicator",
    "substantiveerrorindicator",
    "satisfactoryindicator",
    "findingindicator",
    "go_final",
    "quality_review_id",
    "review_type",
    "final_compliance",
    "qualitymetricdeficientflag",
    "excellentflag",
).withColumn("max_date", lit(max_date))

# COMMAND ----------

# 62
df_62 = df_16.distinct()

# COMMAND ----------

# 20
df_20 = df_62.withColumn(
    "fy_date_current", add_months(col("max_date"), 3)
).withColumn(
    "current_fy", date_format(col("fy_date_current"), 'y')
).withColumn(
    "current_fy_int", col("current_fy").astype(IntegerType())
).withColumn(
    "fy_date", add_months(col("lastreviewdatetime"), 3)
).withColumn(
    "fy_date_string", date_format(col("fy_date"), 'y')
).withColumn(
    "fy_month", date_format(col("lastreviewdatetime"), 'MMMM')
).withColumn(
    "fy_month_int", date_format(col("lastreviewdatetime"), 'M').astype(IntegerType())
).withColumn(
    "fy_quarter", expr("""case when fy_month_int < 4 then 'Q2' 
                       when fy_month_int >= 4 and fy_month_int < 7 then 'Q3' 
                       when fy_month_int >= 7 and fy_month_int < 10 then 'Q4'
                       else 'Q1' end
                       """)
)

# COMMAND ----------

# 19
df_19 = df_20.withColumnRenamed("trademarkserialnumber", "ser_num").join(df_12, "ser_num")
df_19 = df_19.withColumn("lastreviewdatetime", col("lastreviewdatetime").astype(DateType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Counts

# COMMAND ----------

# 46
df_46 = df_19.withColumn("record_output_date", current_timestamp())
# 45
df_45 = df_46.groupBy("record_output_date").agg(count("ser_num").alias("output_record_count"))

# COMMAND ----------

df_45 = df_45.withColumn(
    "record_output_percent_change", lit(None)
).withColumn(
    "continue_process", lit(None)
).withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

df_qual_counts = spark.sql(f"select * from {reporting_catalog}.silver.quality_counts")

# COMMAND ----------

# 47
df_47 = df_45.unionByName(df_qual_counts, allowMissingColumns=True)

# COMMAND ----------

# 48, 54
ct_win = Window().orderBy(desc("record_output_date"))
df_48 = df_47.withColumn("output_record_count_lead", lead(col("output_record_count")).over(ct_win))
df_48 = df_48.withColumn("record_output_percent_change", (col("output_record_count") - col("output_record_count_lead")) / col("output_record_count_lead"))

# COMMAND ----------

# 49
df_49 = df_48.withColumn("continue_process", when((col("output_record_count") >= col("output_record_count_lead")) & (col("record_output_percent_change") < lit(0.05)), lit(1)).otherwise(lit(0)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pivot

# COMMAND ----------

# 31, 27
df_27 = df_19.withColumn(
    "search_insufficient", ~col("searchsufficientindicator")
).withColumnRenamed(
    "mississueindicator", "missed_issues"
).withColumnRenamed(
    "newissueindicator", "new_issues"
).withColumnRenamed(
    "refusalunsoundindicator", "unsound_refusals"
).withColumnRenamed(
    "substantivedeficientindicator", "deficient_substantive_issues"
).withColumnRenamed(
    "proceduraldeficientindicator", "deficient_procedural_issues"
).withColumnRenamed(
    "overalldeficientindicator", "deficient_issues"
).withColumnRenamed(
    "evidencedeficientindicator", "evidence_deficient"
).withColumnRenamed(
    "evidencesatisfactoryindicator", "evidence_satisfactory"
).withColumnRenamed(
    "evidenceexcellentindicator", "evidence_excellent"
).withColumnRenamed(
    "writingdeficientindicator", "writing_deficient"
).withColumnRenamed(
    "writingsatisfactoryindicator", "writing_satisfactory"
).withColumnRenamed(
    "writingexcellentindicator", "writing_excellent"
).withColumnRenamed(
    "substantiveerrorindicator", "substantive_errors"
)

# COMMAND ----------

# columns to keep / group by for transpose
key_cols = ['law_office',
 'lastreviewdatetime',
 'go_final',
 'review_type',
 'final_compliance',
 'qualitymetricdeficientflag',
 'excellentflag',
 'max_date',
 'fy_date_current',
 'current_fy',
 'current_fy_int',
 'fy_date',
 'fy_date_string',
 'fy_month',
 'fy_month_int',
 'fy_quarter',
 "ser_num",
 'first_action_type',
 'disposal_type',
 'pendency_cal_start_dt',
 'pendency_cal_end_dt',
 'non_pro_se',
 'country_or_area_name',
 'filing_basis_grp',
 'filing_method_filed',
 'ste_ctry_cd',
 'concat_class']

# COMMAND ----------

# columns to pivot in transpose
pivot_cols = ['search_insufficient',
 'missed_issues',
 'new_issues',
 'unsound_refusals',
 'deficient_substantive_issues',
 'deficient_procedural_issues',
 'deficient_issues',
 'evidence_deficient',
 'evidence_satisfactory',
 'evidence_excellent',
 'writing_deficient',
 'writing_satisfactory',
 'writing_excellent',
 'substantive_errors']

# COMMAND ----------

# 29 transpose
df_29 = df_27.melt(key_cols, pivot_cols, "metric", "value")

# COMMAND ----------

# 25
win_pivot = Window().partitionBy("ser_num","lastreviewdatetime").orderBy("ser_num","lastreviewdatetime")
df_25 = df_29.withColumn("case_count", when( (col("ser_num") != lag(col("ser_num"), 1, 0).over(win_pivot)) & (col("lastreviewdatetime") != lag(col("lastreviewdatetime"), 1, '1500-01-01').over(win_pivot)), lit(1)).otherwise(lit(0)))

# COMMAND ----------

# 30
df_30 = df_25.withColumn(
    "category", when(lower(col("metric")).contains("writing"), lit("Writing")).otherwise(when(lower(col("metric")).contains("evidence"), lit("Evidence")).otherwise(lit("Other")))
).withColumn(
    "metric", regexp_replace(col("metric"), 'writing_', '')
).withColumn(
    "metric", regexp_replace(col("metric"), 'evidence_', '')
)

# COMMAND ----------

# update column names in metric to match alteryx
col_nm_dict = {'search_insufficient': 'Search Insufficient',
 'missed_issues': 'Missed Issues',
 'new_issues': 'New Issues',
 'unsound_refusals': 'Unsound Refusals',
 'deficient_substantive_issues': 'Deficient Substantive Issues',
 'deficient_procedural_issues': 'Deficient Procedural Issues',
 'deficient_issues': 'Deficient Issues',
 'deficient': 'Deficient',
 'satisfactory': 'Satisfactory',
 'excellent': 'Excellent',
 'substantive_errors': 'Substantive Errors'}

df_30 = df_30.replace(col_nm_dict, subset=['metric'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writes

# COMMAND ----------

# 22
df_22 = df_19.withColumn("max_date", col("max_date").astype(DateType())).drop("ser_num")

# 23
df_23 = df_30.withColumn(
    "max_date", col("max_date").astype(DateType())
).withColumn(
    "value", col("value").astype(BooleanType())
).drop("ser_num")

# COMMAND ----------

# add audit columns
df_22 = df_22.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

df_23 = df_23.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

# set column ordering
df_22 = df_22.select('law_office',
 'lastreviewdatetime',
 'searchsufficientindicator',
 'qualitymetricdeficientindicator',
 'mississueindicator',
 'newissueindicator',
 'refusalunsoundindicator',
 'substantivedeficientindicator',
 'proceduraldeficientindicator',
 'overalldeficientindicator',
 'overallexcellentindicator',
 'evidencedeficientindicator',
 'evidencesatisfactoryindicator',
 'evidenceexcellentindicator',
 'writingdeficientindicator',
 'writingsatisfactoryindicator',
 'writingexcellentindicator',
 'substantiveerrorindicator',
 'satisfactoryindicator',
 'findingindicator',
 'go_final',
 'quality_review_id',
 'review_type',
 'final_compliance',
 'qualitymetricdeficientflag',
 'excellentflag',
 'max_date',
 'fy_date_current',
 'current_fy',
 'current_fy_int',
 'fy_date',
 'fy_date_string',
 'fy_month',
 'fy_month_int',
 'fy_quarter',
 'first_action_type',
 'disposal_type',
 'pendency_cal_start_dt',
 'pendency_cal_end_dt',
 'non_pro_se',
 'country_or_area_name',
 'filing_basis_grp',
 'filing_method_filed',
 'ste_ctry_cd',
 'concat_class',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

df_23 = df_23.select('law_office',
 'lastreviewdatetime',
 'go_final',
 'review_type',
 'final_compliance',
 'qualitymetricdeficientflag',
 'excellentflag',
 'max_date',
 'fy_date_current',
 'current_fy',
 'current_fy_int',
 'fy_date',
 'fy_date_string',
 'fy_month',
 'fy_month_int',
 'fy_quarter',
 'first_action_type',
 'disposal_type',
 'pendency_cal_start_dt',
 'pendency_cal_end_dt',
 'non_pro_se',
 'country_or_area_name',
 'filing_basis_grp',
 'filing_method_filed',
 'ste_ctry_cd',
 'concat_class',
 'metric',
 'value',
 'case_count',
 'category',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

df_49 = df_49.select('record_output_date',
 'output_record_count',
 'record_output_percent_change',
 'continue_process',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

# COMMAND ----------

# write dfs
df_22.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.quality_dashboard")

df_23.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.quality_dashboard_pivot")

df_49.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.quality_counts")

# end job control
recs_count = df_22.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

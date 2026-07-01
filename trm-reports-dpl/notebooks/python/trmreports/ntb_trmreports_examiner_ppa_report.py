# Databricks notebook source
spark.conf.set("spark.sql.photon.enabled", "true")
spark.conf.set("spark.sql.photon.vectorized.reader.enabled", "true")
spark.conf.set("spark.sql.photon.vectorized.writer.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 200)
# Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

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
run_env = common_configs['schema']['tmngpdb_src_catalog']
edw_scope = common_configs['secrets']['edw_scope']
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']
print(edw_scope)
print(reporting_catalog,run_env)
data_layer = "bronze"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# COMMAND ----------

import datetime
import pytz
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_examiner_ppa_report'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# DBTITLE 1,Input
FACT_COMPENSATION_df = read_data_from_oracle_conn_dsu(
    sql_query="""(select FORECAST.FACT_COMPENSATION.PY_PRD_LAST_DA
    from FORECAST.FACT_COMPENSATION 
    where FORECAST.FACT_COMPENSATION.ORG_FOURTH_LVL_CD = '1331')""", 
    schema_name="",
    secrets_name=edw_scope
)

# COMMAND ----------

from pyspark.sql.functions import max
FACT_COMPENSATION_df = FACT_COMPENSATION_df.select(max("PY_PRD_LAST_DA").alias("PY_PRD_LAST_DA1"))

# COMMAND ----------

# DBTITLE 1,Input
max_pay_period_ppa = spark.sql(f"select * from {reporting_catalog}.gold.max_pay_period_ppa")

# COMMAND ----------

# DBTITLE 1,Input
EMP_df = read_data_from_oracle_conn_dsu(
    sql_query="""select DW.EMP.EMP_NO,
	DW.EMP.ORG_CD,
	DW.EMP.EMP_FMLY_NM,
	DW.EMP.EMP_GRD,
	DW.EMP.EMP_GVN_NM,
	DW.EMP.EMP_OCPTNL_SRS_CD,
	DW.EMP.FK_PSTN_SPRVSRY_LVL,
	DW.EMP.FK_PY_PRD_LST_PD_DA 
 from DW.EMP 
 where DW.EMP.ORG_CD Like '13%'""", 
    schema_name="",
    secrets_name=edw_scope,
)

# COMMAND ----------

# DBTITLE 1,Input
FACT_COMPENSATION2_df = read_data_from_oracle_conn_dsu(
    sql_query="""select FORECAST.FACT_COMPENSATION.*,
	FORECAST.FACT_COMPENSATION.ORG_FOURTH_LVL_CD as ORG_FOURTH_LVL_CD1 
    from FORECAST.FACT_COMPENSATION 
    where FORECAST.FACT_COMPENSATION.ORG_FOURTH_LVL_CD = '1331'""", 
    schema_name="",
    secrets_name=edw_scope,
)

# COMMAND ----------

union_max_py_df = max_pay_period_ppa.crossJoin(FACT_COMPENSATION_df)

# COMMAND ----------

# DBTITLE 1,Union
union_final_df = union_max_py_df.crossJoin(EMP_df)

# COMMAND ----------

from pyspark.sql.functions import col, expr, year, month, when

union_final_df = union_final_df.withColumn("Law_Office", expr("right(ORG_CD, 3)"))
union_final_df = union_final_df.withColumn(
    "FY",
    when(month(col("FK_PY_PRD_LST_PD_DA")) > 9, year(col("FK_PY_PRD_LST_PD_DA")) + 1)
    .otherwise(year(col("FK_PY_PRD_LST_PD_DA")))
)
union_final_df = union_final_df.filter(col("FY").isNotNull())
union_final_df = union_final_df.filter(col("EMP_OCPTNL_SRS_CD") == '0905')
union_final_df = union_final_df.filter(~(col("EMP_GRD").contains('15')) & ~(col("EMP_GRD").contains('00')))
union_final_df = union_final_df.groupBy("EMP_NO", "FY").agg({"EMP_NO": "first", "FY": "first"}).select("EMP_NO", "FY")

# COMMAND ----------

from pyspark.sql.functions import col, when, year, month

FACT_COMPENSATION2_df = FACT_COMPENSATION2_df.withColumn(
    "FY",
    when(month(col("PY_PRD_LAST_DA")) > 9, year(col("PY_PRD_LAST_DA")) + 1)
    .otherwise(year(col("PY_PRD_LAST_DA")))
)

FACT_COMPENSATION2_df = FACT_COMPENSATION2_df.withColumn(
    "PPA_Category",
    when(col("ACCTG_ACT_NM").isin(
        '245 NON-PRODUCTION -  SYSTEM UNAVAILABLE', 
        'NONBANK OFFICIAL TIME WITHOUT OTHER CODE', 
        'TWAH NON-PRODUCTION - SYSTEM UNAVAILABLE'), 'Adjustments')
    .when(col("ACCTG_ACT_NM").isin(
        'ATTEND CONFERENCES', 'GENERAL TRAINING', 'GENERAL TRAINING - NON EXAMINING RELATED', 
        'IT SECURITY TRAINING', 'LEGAL LECTURE SERIES', 'LEGAL TRAINING', 'TM ACADEMY TRAINING', 
        'DEVELOP OR ATTEND E-LEARNING', 'EEO TRAINING', 'ETHICS TRAINING', 
        'EXAMINER 1ST YEAR TRAINING PROGRAM', 'FORMAL TRAINING', 'NOFEAR ACT TRAINING', 
        'GENERAL RECORDS MANAGEMENT TRAINING', 'INFORMATION TECHNOLOGY TRAINING', 
        'TECHNICAL IN-HOUSE CONTRACT TRAINING', 'HUMAN RESOURCES TRAINING', 
        "TRAINING IN EXAMINER'S 25 HOUR YEARLY ALLOTMENT", 'TM ACADEMY INSTRUCTION', 
        'TM ATTORNEY TRAINING'), 'Training')
    .when(col("ACCTG_ACT_NM").isin(
        'CONDUCT HEARINGS AND DECIDE CASES ON MERITS', 'EXAMINE 2.146 PETITIONS', 
        'EXAMINE LETTERS OF PROTEST', 'EXAMINE SECTIONS 8,9,15,8/15', 'INNOVATION LAB', 
        'PREPARE APPEALS BRIEFS', 'PROOF OF USE AUDITS PROGRAM IN TM POST REGISTRATION', 
        'PROVIDE COPYRIGHT ADVICE AND TECHNICAL ASSISTANCE', 'PROVIDE IP ADVICE AND TECHNICAL ASSISTANCE', 
        'PROVIDE IP SUPPORT AND ADVICE - TM', 'RELEASE 1', 'RELEASE 2', 'RELEASE 3', 'RELEASE 4', 
        'PROCESS & DECIDE CONTESTED INTERLOC MOTIONS & C.U. (NOT SJ)', 'PROCESS AND DECIDE SUMMARY JUDGMENT MOTIONS', 
        'PROCESS AND DECIDE UNCONTESTED INTERLOC MOTIONS', 'PROCESS EXT OF TIME TO OPPOSE & MISC. POTENTIAL PAPERS', 
        'PREPARE ALL EXAMINER ACTIONS', 'PERFORM EXAMINATION QUALITY REVIEW', 'REVIEW OG RECORDS PRIOR TO PUBLICATION', 
        'APPLICATION - APPLICATION DEVELOPMENT', 'APPLICATION -APPLICATION SUPPORT & OPERATIONS', 'LEGAL RESEARCH', 
        'LITIGATE & SUPPORT IP LEGAL ACTIONS-RED/GREEN BRIEF PREP B', 'LITIGATE AND SUPPORT - OED', 
        'LITIGATE AND SUPPORT IP LEGAL ACTIONS  - NOT AS A PARTY - TM', 'LITIGATE AND SUPPORT IP LEGAL ACTIONS - APP CT -AS PARTY- TM', 
        'LITIGATE AND SUPPORT IP LEGAL ACTIONS - DISTRICT COURT - TM', 'LITIGATE AND SUPPORT IP LEGAL ACTIONS-DIST CT - SECT 1071(B)', 
        'LEGAL RESEARCH (OPEA)', 'INSTITUTE TRIAL PROCEEDINGS', 'HANDLE AND RESPOND TO CUSTOMER INQUIRIES'), 'Other Production')
    .when(col("ACCTG_ACT_NM").isin(
        'EXAMINE APPLICATIONS - EXAMINING ATTORNEY', 'TM ACADEMY EXAMINATION'), 'Examination')
    .when(col("ACCTG_ACT_NM").isin(
        'EMERGENCY PAID LEAVE (AMERICAN RESCUE PLAN)', 'EVACUATION EXCUSED ABSENCE', 'FMLA - PAID PARENTAL LEAVE', 
        'LEAVE - ADMINISTRATIVE', 'LEAVE - ADMINISTRATIVE LEAVE (NON-WEATHER AND SAFETY)', 
        'LEAVE - ADMINISTRATIVE LEAVE (WEATHER & SAFETY)', 'LEAVE - ADMINISTRATIVE LEAVE - VOTING (NON-WEATHER & SAFETY)', 
        'LEAVE - ADMINISTRATIVE LEAVE-VOTING (NON-WEATHER & SAFETY)', 'LEAVE - ANNUAL LEAVE', 'LEAVE - ANNUAL USED FOR FMLA', 
        'LEAVE - FFLA', 'LEAVE - HOLIDAY', 'LEAVE - LEAVE WITHOUT PAY', 'LEAVE - LWOP FOR FMLA', 'LEAVE - OTHER', 
        'LEAVE - SICK', 'LEAVE - SICK USED FOR FMLA', 'COMP USED - (NON-RELIGIOUS, NON MAT/PAT)', 'COMP USED - MATERNITY/PATERNITY', 
        'COMP USED - RELIGIOUS', 'COMP USED - TRAVEL TAKEN', 'LEAVE - OTHER', 'LEAVE: ADMINISTRATIVE LEAVE - SPECIAL COMPENSATORY TIME'), 'Leave')
    .when(col("ACCTG_ACT_NM").isin(
        'EXECUTIVE, MANAGEMENT, AND SUPERVISORY DIRECTION', 'OTHER TIME - SUPPORT/PARTICIPATE IN CORPORATE INITIATIVES', 
        'WORKFORCE RECRUITMENT OPERATIONS', 'POLICY DIRECTION', 'WORKFORCE PLANNING & RECRUITMENT - TM - NON TM ATTORNEYS', 
        'POLICY FORMULATION, INTERPRETATION, AND DISSEMINATION', 'PREPARE OR ATTEND STAFF MEETINGS', 
        'SUPERVISORY, MANAGEMENT, AND LEADERSHIP DEVELOPMENT', 'MEETINGS WITH MANAGEMENTS', 
        'ADMINISTER AFFIRMATIVE EMPLOYMENT INITIATIVES', 'ADMINISTER AWARDS AND RECOGNITION - NON-MONETARY'), 'Mgmt & Supervision')
    .when(col("ACCTG_ACT_NM").isin(
        'LABOR / MANAGEMENT COOPERATION PREPARATION', 'CONTINUITY OF OPERATIONS PLAN/OCCUPANT EMERGENCY PLAN', 
        'CREDIT HOURS WORKED TELEWORK MANAGEMENT AND SUPERVISION', 'GRIEVANCE MEETING (NEGOTIATED GRIEVANCE PROCEDURE)', 
        'INFORMAL MEETINGS (NTEU 245 ONLY)', 'LABOR / MANAGEMENT COOPERATION MEETING', 'MID-TERM BARGAINING PREPARATION', 
        'TERM BARGAINING PREPARATION', 'TERM BARGAINING SESSION', 'MID-TERM BARGAINING SESSION', 
        'THIRD PARTY PROCEEDINGS (LABOR RELATIONS)', 'UNION REP PRESENTATION TO NEW EMPLOYEE ORIENTATION', 
        'UNION SPONSORED TRAINING', 'UNION REPRESENTATIVE CONSULTATION WITH EMPLOYEE'), 'Labor Mgmt')
    .when(col("ACCTG_ACT_NM").isin(
        'PROVIDE MENTORING AND COACHING', 'CAREER COACHING PROGRAM', 'CONTINUING EDUCATION', 
        'LEADERSHIP DEVELOPMENT PROGRAM (LDP)', 'USPTO MENTORING PROGRAM', 'TM ACADEMY MENTORING'), 'Employee Development')
    .otherwise('Other')
)

FACT_COMPENSATION2_df = FACT_COMPENSATION2_df.withColumn(
    "Hours_Type",
    when(col("BOC_CD") == '1171', 'OT').otherwise('Regular')
)

# COMMAND ----------

from pyspark.sql.functions import max
fact_comp_grouped_df = FACT_COMPENSATION2_df.groupBy("EMP_NO").agg(max("PY_PRD_LAST_DA").alias("MAX_PY_PRD_LAST_DA"))
fact_comp_grouped_df = fact_comp_grouped_df.select("MAX_PY_PRD_LAST_DA")

# COMMAND ----------

joined_df = FACT_COMPENSATION2_df.join(broadcast(union_final_df), on=["EMP_NO", "FY"], how="inner")

# COMMAND ----------

joined_df = joined_df.select(
    'FY',
    'ACCTG_ACT_NM',
    'PY_PRD_LAST_DA',
    'PAY_HR_NO',
    'PPA_Category',
    'Hours_Type'
)

joined_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Production vs Non Production**

# COMMAND ----------

grouped_df = joined_df.groupBy("FY", "PPA_Category").sum("PAY_HR_NO").withColumnRenamed("sum(PAY_HR_NO)", "Sum_PAY_HR_NO")
grouped_df = grouped_df.select("FY", "PPA_Category", "Sum_PAY_HR_NO")
grouped_df = grouped_df.filter(grouped_df["Sum_PAY_HR_NO"] > 0)

# COMMAND ----------

from pyspark.sql.functions import col, round
total_FY_df = grouped_df.groupBy("FY").sum("Sum_PAY_HR_NO").withColumnRenamed("sum(Sum_PAY_HR_NO)", "total_FY").select("FY",  "total_FY")
total_FY_df.cache()

# Join grouped_df with total_FY_df to get total_FY in the same DataFrame
grouped_df_with_total_FY = grouped_df.join(total_FY_df, on="FY", how="left")
appended_df = grouped_df_with_total_FY.withColumn("percent", round((col("Sum_PAY_HR_NO") / col("total_FY")) * 100, 1))
appended_df.cache()

# COMMAND ----------

sorted_total_FY_df = total_FY_df.orderBy("FY", ascending=False)
first_8_records = sorted_total_FY_df.limit(8)
first_record = sorted_total_FY_df.limit(1)

# Broadcast smaller DataFrames to optimize join performance
first_8_records = broadcast(first_8_records)
first_record = broadcast(first_record)

# COMMAND ----------

# DBTITLE 1,Join functions
production_df = first_8_records.join(
    appended_df,
    on="FY",
    how="inner"
).select(
    "FY",
    "PPA_Category",
    col("Sum_PAY_HR_NO").alias("Hours"),
    "percent"
)

non_production_df = first_record.join(
    appended_df,
    on="FY",
    how="inner"
).select(
    "FY",
    "PPA_Category",
    col("Sum_PAY_HR_NO").alias("Hours"),
    "percent"
)

# COMMAND ----------

non_production_df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ppa_report_fytd_hours")
production_df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ppa_report")

# COMMAND ----------

# MAGIC %md
# MAGIC # **OT VS Regular**

# COMMAND ----------

from pyspark.sql.functions import sum, col, desc, round
grouped_joined_df = joined_df.groupBy("FY", "Hours_Type").agg(sum("PAY_HR_NO").alias("Sum_PAY_HR_NO"))
grouped_joined_df = grouped_joined_df.filter(grouped_joined_df["Sum_PAY_HR_NO"] > 0)
ordered_grouped_joined_df = grouped_joined_df.orderBy("FY", desc("Sum_PAY_HR_NO"))
sorted_grouped_joined_df = grouped_joined_df.groupBy("FY").agg(sum("Sum_PAY_HR_NO").alias("total_FY"))

# COMMAND ----------

# Assuming 'find' is the value to be found in the 'FY' field
find_value = 'find'

# Filter records where 'FY' matches the find_value
filtered_df = sorted_grouped_joined_df.filter(sorted_grouped_joined_df["FY"] == find_value)

# Append the filtered records back to the original DataFrame
appended_df = sorted_grouped_joined_df.union(filtered_df.withColumn("FY", lit(find_value)))

# Join grouped_df with total_FY_df to get total_FY in the same DataFrame
appended_df = ordered_grouped_joined_df.join(sorted_grouped_joined_df, on="FY", how="left")

# Calculate the percent column
appended_df = appended_df.withColumn("percent", (col("Sum_PAY_HR_NO") / col("total_FY")) * 100)
appended_df = appended_df.withColumn("percent", round(col("percent"), 1))

# Select the required columns
appended_df = appended_df.select("total_FY", "FY", "percent", "Sum_PAY_HR_NO", "Hours_Type")


# COMMAND ----------

# Sorting the DataFrame by FY in descending order
sorted_grouped_joined_df = sorted_grouped_joined_df.orderBy(col("FY").desc())
first_8_records = sorted_grouped_joined_df.limit(8)
first_record = sorted_grouped_joined_df.limit(1)

# COMMAND ----------

ot_df = first_8_records.join(appended_df, on="FY").select(
    "FY",
    col("Sum_PAY_HR_NO").alias("Hours"),
    "percent",
    "Hours_Type"

)

regular_df = first_record.join(appended_df, on="FY").select(
    "FY",
    col("Sum_PAY_HR_NO").alias("Hours"),
    "percent",
    "Hours_Type"
)

# COMMAND ----------

ot_df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ppa_report_ot")
regular_df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ppa_report_ot_fytd_hours")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Leave Types**

# COMMAND ----------

ppa_leave_df = joined_df.groupBy("FY", "Hours_Type","PPA_Category","ACCTG_ACT_NM").agg(sum("PAY_HR_NO").alias("Sum_PAY_HR_NO"))
ppa_leave_df = ppa_leave_df.filter((col("PPA_Category") == "Leave") & (col("Sum_PAY_HR_NO") > 0))
ppa_leave_order_df = ppa_leave_df.orderBy(col("FY").asc(), col("Sum_PAY_HR_NO").desc())
result_df = ppa_leave_df.groupBy("FY").sum("Sum_PAY_HR_NO").withColumnRenamed("sum(Sum_PAY_HR_NO)", "total_FY")

# COMMAND ----------

find_value = 'find'
filtered_df = result_df.filter(result_df["FY"] == find_value)
appended_df = result_df.union(filtered_df.withColumn("FY", lit(find_value)))
appended_df = ppa_leave_order_df.join(result_df, on="FY", how="left")
appended_df = appended_df.withColumn("percent", (col("Sum_PAY_HR_NO") / col("total_FY")) * 100)
appended_df = appended_df.withColumn("percent", round(col("percent"), 1))
appended_df = appended_df.select("ACCTG_ACT_NM", "FY", "Sum_PAY_HR_NO", "total_FY","percent")

# COMMAND ----------

result_df = result_df.orderBy(col("FY").desc())
first_8_records = sorted_grouped_joined_df.limit(8)
first_record = sorted_grouped_joined_df.limit(1)

# COMMAND ----------

joined2_df = first_8_records.join(appended_df, on="FY").select(
    "FY",
    col("Sum_PAY_HR_NO").alias("Hours"),
    "ACCTG_ACT_NM",
    "percent"

)

joined3_df = first_record.join(appended_df, on="FY").select(
    "FY",
    col("ACCTG_ACT_NM").alias("Leave_Type"),
    col("Sum_PAY_HR_NO").alias("Hours"),
    "percent"
)

# COMMAND ----------

joined2_df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ppa_report_leave")
joined3_df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ppa_report_leave_fytd_hours")

# COMMAND ----------

# MAGIC %md
# MAGIC # **# Output**

# COMMAND ----------

# DBTITLE 1,Input for Output table
filtered_df = production_df.filter(col("PPA_Category").isNotNull()).orderBy(col("PPA_Category").asc(), col("FY").asc())

FACT_COMPENSATION2_df = FACT_COMPENSATION2_df.withColumn("MAX_PY_PRD_LAST_DA", max("PY_PRD_LAST_DA").over(Window.partitionBy()))
FACT_COMPENSATION2_df = FACT_COMPENSATION2_df.select("MAX_PY_PRD_LAST_DA", "FY")

# COMMAND ----------

from pyspark.sql.functions import current_date, lit, concat, col
union_df = FACT_COMPENSATION2_df.unionByName(production_df, allowMissingColumns=True)

# Create a new column with the name as today and update it with the specified string
today_date = current_date()
union_df = union_df.withColumn(
    "today",
    concat(lit("Examining Attorney PPA Data Through "), col("MAX_PY_PRD_LAST_DA").cast("string"))
)
union_df = union_df.withColumn("today", col("today").substr(1, 10))

# COMMAND ----------

max_date_df = union_df.unionByName(FACT_COMPENSATION2_df, allowMissingColumns=True) \
    .agg(F.max("MAX_PY_PRD_LAST_DA").alias("MAX_PY_PRD_LAST_DA"))
result_output = max_date_df.select(F.date_format("MAX_PY_PRD_LAST_DA", "MM-dd-yyyy"))

result_output.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.max_pay_period_ppa")

# COMMAND ----------

# DBTITLE 1,BiWeekly
grouped_df = joined_df.groupBy("FY", "PY_PRD_LAST_DA", "PPA_Category").sum("PAY_HR_NO").withColumnRenamed("sum(PAY_HR_NO)", "Sum_PAY_HR_NO")
grouped_df.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ppa_report_biweekly")

# COMMAND ----------

# DBTITLE 1,Automation Data test
# Commented out because of Rally user story US665881: Disable Alteryx Data Quality Match
# data quality entry
# tbl1 = f"{reporting_catalog}.gold.ppa_report"
# tbl2 = f"hive_metastore.{altrx_schema}.ppa_report"
# key_cols = ['FY']
# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
# print(dq_result)

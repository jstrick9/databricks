# Databricks notebook source
# MAGIC %md
# MAGIC #### Configs, Imports & Job Control

# COMMAND ----------

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


# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_gold_pou_audit_report'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inputs

# COMMAND ----------

df_pou = spark.sql(f"select * from {reporting_catalog}.silver.proof_of_use_audit")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Totals

# COMMAND ----------

df_303 = df_pou.groupBy("filing_basis_cur").agg(countDistinct(col("registration_number")).alias("count"))

total_audits = df_303.groupBy().sum("count").collect()[0][0]

df_308 = df_303.withColumn(
  "total", lit(total_audits)
).withColumn(
  "percent", round(col("count") / col("total") * 100, 2)
).withColumn(
  "category", lit("audits")
)

# COMMAND ----------

df_304 = df_pou.filter(col("cancellation_in") == True).groupBy("filing_basis_cur").agg(countDistinct(col("registration_number")).alias("count"))

total_cancellations = df_304.groupBy().sum("count").collect()[0][0]

df_314 = df_304.withColumn(
  "total", lit(total_cancellations)
).withColumn(
  "percent", round(col("count") / col("total") * 100, 2)
).withColumn(
  "category", lit("cancellations")
)

# COMMAND ----------

df_325 = df_pou.filter(col("deletions_after_audit_in") == True).groupBy("filing_basis_cur").agg(countDistinct(col("registration_number")).alias("count"))

total_deletions = df_325.groupBy().sum("count").collect()[0][0]

df_328 = df_325.withColumn(
  "total", lit(total_deletions)
).withColumn(
  "percent", round(col("count") / col("total") * 100, 2)
)

# COMMAND ----------

df_339 = df_pou.filter(col("response_oa_rec_in") == True).groupBy("filing_basis_cur").agg(countDistinct(col("serial_number")).alias("cases_oa_received"))

total_cases_oa_receieved = df_339.groupBy().sum("cases_oa_received").collect()[0][0]

overall_deletion_rate = total_deletions / total_cases_oa_receieved * 100

df_341 = df_325.join(df_339, "filing_basis_cur")

df_342 = df_341.withColumn(
    "deletion_rate",  round(col("count") / col("cases_oa_received") * 100, 2)
).drop(
    "count", "cases_oa_received"
).withColumn(
  "overall_deletion_rate", round(lit(overall_deletion_rate), 2)
)

df_336 = df_328.join(df_342, "filing_basis_cur").withColumn(
  "category", lit("deletions")
)

# COMMAND ----------

df_344 = df_pou.filter(col("response_oa_rec_in") == True).groupBy("filing_basis_cur").agg(countDistinct(col("serial_number")).alias("count"))

total_audit_w_resp = df_344.groupBy().sum("count").collect()[0][0]

df_348 = df_344.withColumn(
  "total", lit(total_audit_w_resp)
).withColumn(
  "percent", round(col("count") / col("total") * 100, 2)
).withColumn(
  "category", lit("audit_with_response")
)

# COMMAND ----------

# union all together
df_totals = df_336.unionByName(
    df_308.withColumn("deletion_rate", lit(None)).withColumn("overall_deletion_rate", lit(None))
).unionByName(
    df_314.withColumn("deletion_rate", lit(None)).withColumn("overall_deletion_rate", lit(None))
).unionByName(
    df_348.withColumn("deletion_rate", lit(None)).withColumn("overall_deletion_rate", lit(None))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Characteristics

# COMMAND ----------

win_chars = Window().partitionBy().orderBy(col("audits").desc(), "value")

# COMMAND ----------

df_362 = df_pou.groupBy("owner_name").agg(
    countDistinct("serial_number").alias("audits")
).withColumn(
    'total_audits', lit(total_audits)
).withColumn(
    'percent', round(col("audits") / col("total_audits") * 100, 2)
)

df_owner = df_362.withColumnRenamed(
  "owner_name", "value"
).withColumn(
    "rank", row_number().over(win_chars)
).filter(col("rank") <= 20).drop("rank").withColumn(
  "category", lit("owner")
)

# COMMAND ----------

df_368 = df_pou.groupBy("attorney_name").agg(
    countDistinct("serial_number").alias("audits")
).withColumn(
    'total_audits', lit(total_audits)
).withColumn(
    'percent', round(col("audits") / col("total_audits") * 100, 2)
).withColumnRenamed(
    'attorney_name', 'attorney'
).fillna('Pro Se', subset=['attorney'])

df_attorney = df_368.withColumnRenamed(
  "attorney", "value"
).withColumn(
    "rank", row_number().over(win_chars)
).filter(col("rank") <= 20).drop("rank").withColumn(
  "category", lit("attorney")
)

# COMMAND ----------

df_374 = df_pou.groupBy("country_or_area_name").agg(
    countDistinct("serial_number").alias("audits")
).withColumn(
    'total_audits', lit(total_audits)
).withColumn(
    'percent', round(col("audits") / col("total_audits") * 100, 2)
)

df_country = df_374.withColumnRenamed(
  "country_or_area_name", "value"
).withColumn(
    "rank", row_number().over(win_chars)
).filter(col("rank") <= 20).drop("rank").withColumn(
  "category", lit("country")
)

# COMMAND ----------

df_non_pro_se = df_pou.withColumn(
    "non_pro_se", when(col("attorney_name").isNull(), lit("Pro se")).otherwise(lit("Non Pro Se"))
).groupBy("non_pro_se").agg(
    countDistinct("serial_number").alias("audits")
).withColumn(
    'total_audits', lit(total_audits)
).withColumn(
    'percent', round(col("audits") / col("total_audits") * 100, 2)
).withColumn(
  "category", lit("non_pro_se")
).withColumnRenamed(
  "non_pro_se", "value"
)

# COMMAND ----------

df_387 = df_pou.withColumn(
    'classes', explode(split(col("reg_classes"), ';'))
).filter(
    (col("classes").isNotNull()) & (col("classes") != "")
)

df_389 = df_387.groupBy("classes").agg(
    countDistinct("serial_number").alias("audits")
).withColumn(
    'total_audits', lit(total_audits)
).withColumn(
    'percent', round(col("audits") / col("total_audits") * 100, 2)
)

df_classes = df_389.withColumnRenamed(
  "classes", "value"
).withColumn(
    "rank", row_number().over(win_chars)
).filter(col("rank") <= 20).drop("rank").withColumn(
  "category", lit("classes")
)

# COMMAND ----------

### union together all characteristic dfs
df_chars = df_owner.unionByName(df_attorney).unionByName(df_country).unionByName(df_non_pro_se).unionByName(df_classes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Audit Actions

# COMMAND ----------

df_action = df_pou.withColumn(
    "fy", when(month(col("first_audit_office_action_dt")) > 9, year(col("first_audit_office_action_dt")) + 1).otherwise(year(col("first_audit_office_action_dt")))
).filter(col("fy") >= 2018)

# COMMAND ----------

df_action_1 = df_action.filter(col("first_audit_office_action_dt").isNotNull()).groupBy("fy").agg(countDistinct("registration_number").alias("first_actions"))

df_action_2 = df_action.filter(col("second_audit_office_action_dt").isNotNull()).groupBy("fy").agg(countDistinct("registration_number").alias("second_actions"))

df_action_3 = df_action.filter(col("third_audit_office_action_dt").isNotNull()).groupBy("fy").agg(countDistinct("registration_number").alias("third_actions"))

df_action_interim = df_action.filter(col("audit_interim_office_action_dt").isNotNull()).groupBy("fy").agg(countDistinct("registration_number").alias("interim_actions"))

df_action_no_resp = df_action.filter(col("audit_no_response_office_action_dt").isNotNull()).groupBy("fy").agg(countDistinct("registration_number").alias("no_resp_actions"))

df_action_resp = df_action.filter(col("response_oa_rec_in") == True).groupBy("fy").agg(countDistinct("registration_number").alias("resp_actions"))

df_action_cancel = df_action.filter(col("cancellation_in") == True).groupBy("fy").agg(countDistinct("registration_number").alias("cancel_actions"))

# COMMAND ----------

df_action_joined = df_action_1.join(df_action_2, "fy").join(df_action_3, "fy").join(df_action_interim, "fy").join(df_action_no_resp, "fy").join(df_action_resp, "fy").join(df_action_cancel, "fy")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Outputs

# COMMAND ----------

# add audit columns and set column ordering
df_totals = df_totals.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
).select(
    "category",
    col("filing_basis_cur").alias("filing_basis"),
    "count",
    "total",
    "percent",
    "deletion_rate",
    "overall_deletion_rate",
    "create_ts",
    "create_user_id",
    "update_ts",
    "update_user_id"
)

# COMMAND ----------

# add audit columns and set column ordering
df_chars = df_chars.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
).select(
    "category",
    "value",
    "audits",
    "total_audits",
    "percent",
    "create_ts",
    "create_user_id",
    "update_ts",
    "update_user_id"
)

# COMMAND ----------

# add audit columns and set column ordering
df_action_joined = df_action_joined.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
).select(
    'fy',
    'first_actions',
    'second_actions',
    'third_actions',
    'interim_actions',
    'no_resp_actions',
    'resp_actions',
    'cancel_actions',
    "create_ts",
    "create_user_id",
    "update_ts",
    "update_user_id"
)

# COMMAND ----------

# DBTITLE 1,POU_AUDIT DASHBOARD LOGIC
pou_dsh_df = spark.sql(f"""WITH business_event_info AS (
  SELECT
    right(be.cfk_object_gid, 8) AS serial_number,
    MAX(be.fk_business_event_reason_id) AS business_event_id,
    MAX(be.effective_ts) AS effective_ts,
    MAX(sbe.business_event_reason_cd) AS business_event_reason_cd
  FROM {tmngpdb_catalog}.bronze.business_event be
  INNER JOIN {tmngpdb_catalog}.bronze.stnd_business_event_reason sbe
    ON be.fk_business_event_reason_id = sbe.business_event_reason_id
  WHERE be.effective_ts >= '2015-10-01'
  GROUP BY right(be.cfk_object_gid, 8)
),
milestone_by_serial AS (
  SELECT
    ser_num AS serial_number,
    MAX(first_action_dt_ph) AS first_action_dt_ph
  FROM {reporting_catalog}.silver.milestone
  WHERE first_action_dt_ph >= '2015-10-01'
  GROUP BY ser_num
)
SELECT
  poa.attorney_name,
  poa.audit_interim_office_action_dt,
  poa.audit_no_response_office_action_dt,
  poa.cancellation_in,
  poa.country_or_area_name,
  poa.create_ts,
  poa.create_user_id,
  poa.deletions_after_audit_count,
  poa.deletions_after_audit_in,
  poa.em_empe_name,
  poa.filing_basis_cur,
  poa.firm_name,
  poa.first_audit_office_action_dt,
  poa.owner_name,
  poa.reg_classes,
  poa.registration_number,
  poa.response_oa_rec_in,
  poa.review_fy,
  poa.review_fy_quarter,
  poa.review_month,
  poa.review_month_int,
  poa.second_audit_office_action_dt,
  poa.serial_number,
  poa.third_audit_office_action_dt,
  poa.update_ts,
  poa.termination_dt,
  poa.acceptflag_noPUM1,
  poa.update_user_id,
  poa.first_deletion_dt,
  poa.latest_deletion_dt,
  poa.deletion_event_count,
  m.first_action_dt_ph,
  y.Deletion_Date,
  CASE
    WHEN poa.termination_dt >= y.deletion_date OR poa.termination_dt IS NULL
    THEN YEAR(add_months(y.deletion_date, 3))
  END AS FY_Deleted,
  CASE WHEN poa.review_fy = 2021 AND poa.AcceptFlag_NoPUM1 = 'true' THEN 1 ELSE 0 END AS PrePUM1Flag,
  CASE
    WHEN (poa.termination_dt >= y.deletion_date OR poa.termination_dt IS NULL) AND MONTH(y.deletion_date) <= 3 THEN 'Q2'
    WHEN (poa.termination_dt >= y.deletion_date OR poa.termination_dt IS NULL) AND MONTH(y.deletion_date) > 3 AND MONTH(y.deletion_date) <= 6 THEN 'Q3'
    WHEN (poa.termination_dt >= y.deletion_date OR poa.termination_dt IS NULL) AND MONTH(y.deletion_date) > 6 AND MONTH(y.deletion_date) <= 9 THEN 'Q4'
    WHEN (poa.termination_dt >= y.deletion_date OR poa.termination_dt IS NULL) AND MONTH(y.deletion_date) > 9 THEN 'Q1'
  END AS Qtr_Deleted,
  COUNT(CASE WHEN poa.response_oa_rec_in = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS percentage_notice_of_acceptance,
  COUNT(CASE WHEN bei.business_event_reason_cd IN ('SUNAE', 'SUNAO') AND DATE(m.first_action_dt_ph) < bei.effective_ts THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS pct_serials_per_country,
  MAX(bei.business_event_id) AS business_event_id,
  MAX(bei.effective_ts) AS effective_ts,
  MAX(bei.business_event_reason_cd) AS business_event_reason_cd
FROM {reporting_catalog}.silver.proof_of_use_audit poa
INNER JOIN business_event_info bei
  ON poa.serial_number = bei.serial_number
LEFT JOIN milestone_by_serial m
  ON poa.serial_number = m.serial_number
LATERAL VIEW OUTER explode(poa.all_deletion_dates) y AS deletion_date
WHERE poa.review_fy >= 2015
GROUP BY  poa.attorney_name,

  poa.attorney_name,
  poa.audit_interim_office_action_dt,
  poa.audit_no_response_office_action_dt,
  poa.cancellation_in,
  poa.country_or_area_name,
  poa.create_ts,
  poa.create_user_id,
  poa.deletions_after_audit_count,
  poa.deletions_after_audit_in,
  poa.em_empe_name,
  poa.filing_basis_cur,
  poa.firm_name,
  poa.first_audit_office_action_dt,
  poa.owner_name,
  poa.reg_classes,
  poa.registration_number,
  poa.response_oa_rec_in,
  poa.review_fy,
  poa.review_fy_quarter,
  poa.review_month,
  poa.review_month_int,
  poa.second_audit_office_action_dt,
  poa.serial_number,
  poa.third_audit_office_action_dt,
  poa.update_ts,
  poa.termination_dt,
  poa.acceptflag_noPUM1,
  poa.update_user_id,
  poa.first_deletion_dt,
  poa.latest_deletion_dt,
  poa.deletion_event_count,
  m.first_action_dt_ph,
  y.deletion_date""")

# COMMAND ----------

try:
    df_totals.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.pou_audit_totals')
    df_chars.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.pou_audit_characteristics')
    df_action_joined.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.pou_audit_actions')
    pou_dsh_df.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.pou_audit_dashboards')

    recs_count = df_totals.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {reporting_catalog}.gold.pou_audit_totals, pou_audit_characteristics, pou_audit_actions tables, pou_audit_dashboard ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading {reporting_catalog}.gold.proof_of_use_audit, pou_audit_characteristics, pou_audit_actions tables, pou_audit_dashboard")

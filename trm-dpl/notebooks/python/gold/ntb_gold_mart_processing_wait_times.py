# Databricks notebook source
# MAGIC %md
# MAGIC # Gold – mart_processing_wait_times
# MAGIC ### USPTO.gov Trademark Wait Times – from Silver case_milestones
# MAGIC

# COMMAND ----------

dbutils.widgets.text("dbx_env", "dev")
dbutils.widgets.text("lookback_months", "18")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "tmngpdb-conf.yaml"
config_file = f"../../config/{dbx_env}/{config_file_name}"

# COMMAND ----------

# MAGIC %run ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)

tmngpdb_catalog = common_configs['schema']['trgt_catalog']
silver_schema_name = 'silver'
gold_schema_name = 'gold'

spark.conf.set('conf.catalog', tmngpdb_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

silver_table = f"`{tmngpdb_catalog}`.`{silver_schema_name}`.case_milestones"
gold_table = f"`{tmngpdb_catalog}`.`{gold_schema_name}`.processing_wait_times"
targets_table = f"`{tmngpdb_catalog}`.`{gold_schema_name}`.metric_targets"

lookback_months = int(dbutils.widgets.get("lookback_months"))

print(f"dbx_env={dbx_env}")
print(f"silver={silver_table}")
print(f"gold={gold_table}")
print(f"lookback_months={lookback_months}")

# COMMAND ----------

from pyspark.sql import functions as F
import datetime

snapshot_date = datetime.date.today()

all_cases = spark.table(f"{tmngpdb_catalog}.{silver_schema_name}.case_milestones").filter("_is_current = true")
case = all_cases.filter(F.col("filing_date") >= F.add_months(F.lit(snapshot_date), -lookback_months))

case_count = case.count()
print(f"Silver cases in lookback ({lookback_months}mo): {case_count}")

if case_count == 0:
    print("WARNING: 0 cases in Silver - did build_case_timeline run for this dbx_env?")
    print(f"Check: SELECT COUNT(*) FROM {tmngpdb_catalog}.{silver_schema_name}.case_milestones WHERE _is_current = true")

# COMMAND ----------

def wait_days_df(filing_col, processed_col, extra_filter=None):
    df = all_cases.filter(
        F.col(processed_col).isNotNull() & 
        F.col(filing_col).isNotNull() & 
        (F.col(filing_col) >= F.add_months(F.lit(snapshot_date), -lookback_months))
    )
    if extra_filter is not None:
        df = df.filter(extra_filter)
    return df.withColumn("wait_days", F.datediff(F.col(processed_col), F.col(filing_col)))

def metric(key, name, section, unit, df):
    try:
        if df.limit(1).count() == 0:
            avg_val, n = 0.0, 0
        else:
            agg = df.agg(F.avg("wait_days").alias("avg"), F.count("*").alias("n")).collect()[0]
            avg_val = float(agg["avg"] or 0)
            n = int(agg["n"] or 0)
        if unit == "months" and avg_val:
            avg_val = avg_val / 30.44
        return (key, name, section, unit, round(avg_val, 1), n)
    except Exception as e:
        print(f"metric {key} failed: {e}")
        return (key, name, section, unit, 0.0, 0)

rows = []

# 1. First Action – months
fa = wait_days_df("filing_date", "first_oa_date")
rows.append(metric("first_action", "First examining action in TSDR record", "summary", "months", fa))

# 2. Registration / Abandonment – months
reg = wait_days_df("filing_date", "disposal_date")
rows.append(metric("registration_or_abandonment", "Trademark registering or application abandoning", "summary", "months", reg))

# 3-4. Pre-Exam TEAS / MADRID – days
# filing_basis populated from tram_am.am_flg_action via Silver
# Config values: TEAS / MADRID – also accept common variants
teas_filter = F.col("filing_basis").rlike("(?i)teas|1a|1b|^1$")
madrid_filter = F.col("filing_basis").rlike("(?i)madrid|66a|ir|^0$")

fa_teas = wait_days_df("filing_date", "first_oa_date", teas_filter)
rows.append(metric("pre_exam_teas", "TEAS", "Pre-Examination Unit", "days", fa_teas))
fa_madrid = wait_days_df("filing_date", "first_oa_date", madrid_filter)
rows.append(metric("pre_exam_madrid", "MADRID", "Pre-Examination Unit", "days", fa_madrid))

# 5. ESU Responses
esu = wait_days_df("esu_response_date", "esu_processed_date")
rows.append(metric("esu_responses", "Responses/Corrections", "Examination Support Unit (ESU)", "days", esu))

# 6-8. ITU
for f_col, p_col, key, name in [
  ("extension_request_date", "extension_processed_date", "itu_extension", "Extension requests"),
  ("sou_filing_date", "sou_processed_date", "itu_sou", "Statement of use"),
  ("divisional_request_date", "divisional_processed_date", "itu_divisional", "Divisional requests"),
]:
    df = wait_days_df(f_col, p_col) if f_col in case.columns else case.filter(F.lit(False))
    rows.append(metric(key, name, "Intent to use", "days", df))

# 9. Petitions LOP
lop = wait_days_df("lop_filing_date", "lop_processed_date") if "lop_filing_date" in case.columns else case.filter(F.lit(False))
rows.append(metric("petitions_lop", "Letters of protest", "Petitions Office", "days", lop))

# 10-12. Post-Reg
for f_col, p_col, key, name in [
  ("affidavit_filing_date", "affidavit_processed_date", "postreg_affidavit", "Affidavits of Use/Incontestability"),
  ("renewal_filing_date", "renewal_processed_date", "postreg_renewal", "Renewals"),
  ("amendment_filing_date", "amendment_processed_date", "postreg_amendment", "Amendments/Corrections"),
]:
    df = wait_days_df(f_col, p_col) if f_col in case.columns else case.filter(F.lit(False))
    rows.append(metric(key, name, "Post Registration", "days", df))

# COMMAND ----------

if spark.table(f"{tmngpdb_catalog}.{gold_schema_name}.metric_targets").count() == 0:
    target_rows = [
      ("first_action","First examining action in TSDR record","summary","months",5.0,10),
      ("registration_or_abandonment","Trademark registering or application abandoning","summary","months",11.0,20),
      ("pre_exam_teas","TEAS","Pre-Examination Unit","days",10.0,100),
      ("pre_exam_madrid","MADRID","Pre-Examination Unit","days",10.0,110),
      ("esu_responses","Responses/Corrections","Examination Support Unit (ESU)","days",14.0,200),
      ("itu_extension","Extension requests","Intent to use","days",15.0,300),
      ("itu_sou","Statement of use","Intent to use","days",15.0,310),
      ("itu_divisional","Divisional requests","Intent to use","days",15.0,320),
      ("petitions_lop","Letters of protest","Petitions Office","days",60.0,400),
      ("postreg_affidavit","Affidavits of Use/Incontestability","Post Registration","days",90.0,500),
      ("postreg_renewal","Renewals","Post Registration","days",90.0,510),
      ("postreg_amendment","Amendments/Corrections","Post Registration","days",90.0,520),
    ]
    (spark.createDataFrame(target_rows, ["metric_key","metric_name","section","unit","target_value","sort_order"])
      .withColumn("target_value", F.col("target_value").cast("double"))
      .withColumn("sort_order", F.col("sort_order").cast("integer"))
      .write.mode("append").saveAsTable(f"{tmngpdb_catalog}.{gold_schema_name}.metric_targets")
    )

targets = spark.table(f"{tmngpdb_catalog}.{gold_schema_name}.metric_targets")

# ---------- Exam queue window ----------
# Currently examining applications filed between X – Y
pending = case.filter(F.col("first_oa_date").isNull() & F.col("filing_date").isNotNull())
queue = pending.agg(
  F.expr("percentile_approx(filing_date, 0.25)").alias("q_start"),
  F.expr("percentile_approx(filing_date, 0.75)").alias("q_end")
).collect()[0] if pending.count() > 0 else {"q_start": None, "q_end": None}
exam_start = queue["q_start"]
exam_end = queue["q_end"]

# ---------- Build Gold output ----------
metrics_df = spark.createDataFrame(rows, schema="metric_key string, metric_name string, section string, unit string, average_value double, sample_size int")

out = (metrics_df.alias("m")
  .join(targets.select("metric_key","target_value"), "metric_key", "left")
  .select(
    F.col("m.metric_key"), F.col("m.metric_name"), F.col("m.section"), F.col("m.unit"),
    F.col("average_value").cast("double").alias("average_value"),
    F.col("target_value").cast("double").alias("target_value"),
    F.lit(snapshot_date).cast("date").alias("processing_as_of_date"),
    F.lit(exam_start).cast("date").alias("exam_queue_start_date"),
    F.lit(exam_end).cast("date").alias("exam_queue_end_date"),
    F.col("sample_size").cast("integer").alias("sample_size"),
    F.current_timestamp().alias("data_updated_ts"),
    F.lit(snapshot_date).cast("date").alias("snapshot_date")
  )
)

out.write.mode("overwrite").option("replaceWhere", f"snapshot_date = '{snapshot_date}'").saveAsTable(f"{tmngpdb_catalog}.{gold_schema_name}.processing_wait_times")

print(f"Gold wait_times written → {gold_table}  snapshot={snapshot_date}")
display(out.orderBy("metric_key"))

# ---------- Validation vs USPTO.gov ----------
print("\n=== USPTO.gov Validation ===")
print("Target (May 31, 2026): First Action = 4.3 months, Registration = 9.9 months")
result = {r.metric_key: r.average_value for r in out.collect()}
fa_val = result.get("first_action", 0)
reg_val = result.get("registration_or_abandonment", 0)
print(f"  first_action = {fa_val} months  target 4.3  {'✓' if 3.0 < fa_val < 7.0 else '✗ OUT OF RANGE – check event_codes in wait_time_source_mapping.yml'}")
print(f"  registration_or_abandonment = {reg_val} months  target 9.9  {'✓' if 7.0 < reg_val < 14.0 else '✗ OUT OF RANGE'}")
if fa_val == 0:
    print("  → First Action = 0 → oa_event_codes in wait_time_source_mapping.yml are not matching any rows in silver.case_milestones - expand event_codes.trademark_last_event_type_cd.first_office_action")
if reg_val == 0:
    print("  → Registration = 0 → reg_status_codes / abn_status_codes need expanding - see wait_time_source_mapping.yml")
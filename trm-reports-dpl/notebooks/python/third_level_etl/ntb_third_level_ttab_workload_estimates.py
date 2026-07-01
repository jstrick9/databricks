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
#trm_catalog = common_configs['schema']['tmngpdb_src_catalog']
reporting_catalog = common_configs['schema']['reporting_catalog']
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_third_level_ttab_workload_estimates'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Input DFs

# COMMAND ----------

# common input DF
ttab_detail = spark.sql(f"select * from {reporting_catalog}.gold.ttab_detail")

# COMMAND ----------

post_reg_milestone = spark.sql(f"select * from {reporting_catalog}.silver.post_reg_milestone")

# COMMAND ----------

# 2305
df_2305 = post_reg_milestone.select("serial_number", "registration_dt", "expiration_dt").distinct()

# COMMAND ----------

# columns to pivot in transpose
pivot_cols = ["registration_dt", "expiration_dt"]

# COMMAND ----------

# 2306 transpose

# concat column name to each cell value
df_2306 = df_2305
for c in pivot_cols:
    df_2306 = df_2306.withColumn(c, concat(lit(c + ':sep:'), col(c)))

# concat all pivot columns
df_2306 = df_2306.withColumn("collected", concat_ws('@sep@', *[col(c) for c in pivot_cols]).alias('collected')).drop(*pivot_cols).select("serial_number", 'collected')

# split concatenated string into array and explode / pivot
df_2306 = df_2306.withColumn("collected", explode(split(col("collected"), "@sep@"))).withColumn("liveregh_name", split(col("collected"), ":sep:")[0]).withColumn("liveregh_value", split(col("collected"), ":sep:")[1])
df_2306 = df_2306.drop("collected")

# COMMAND ----------

# 2307
df_2307 = df_2306.withColumn(
    "liveregh_dt", expr("case when liveregh_name = 'registration_dt' then liveregh_value when liveregh_name = 'expiration_dt' then date_add(liveregh_value, 1) else Null end")
).withColumn(
    "liveregh_count", expr("case when liveregh_name = 'registration_dt' then 1 when liveregh_name = 'expiration_dt' then -1 else 0 end")
).withColumn(
    "liveregh_fy", when(month(col("liveregh_dt")) > 9, year(col("liveregh_dt")) + 1).otherwise(year(col("liveregh_dt")))
)

# COMMAND ----------

# 2309
df_2309 = df_2307.filter(~col("liveregh_dt").isNull())

# COMMAND ----------

# 2310, 2311
win_post_reg = Window.partitionBy().orderBy('liveregh_dt', desc("liveregh_count")).rowsBetween(Window.unboundedPreceding, 0)
df_2310 = df_2309.withColumn('run_tot', sum('liveregh_count').over(win_post_reg))

# COMMAND ----------

# 2312
df_2312 = df_2310.groupBy("liveregh_fy").agg(max("liveregh_dt").alias("max_liveregh_dt"), max("run_tot").astype(IntegerType()).alias("live_registrations")).withColumnRenamed("liveregh_fy", "fy")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cancellations Workloads

# COMMAND ----------

# 2427
df_2427 = ttab_detail.filter(col("ttab_issue_type") == "CANCELLATION")

# COMMAND ----------

# 2429
df_2429 = df_2427.withColumn(
    "filing_fy", when(month(col("instituted_date")) > 9, year(col("instituted_date")) + 1).otherwise(year(col("instituted_date")))
).withColumn(
    "end_action_date", when((col("decision_date").isNull()) | (col("decision_date") == lit("")), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "end_action_fy", when(month(col("end_action_date")) > 9, year(col("end_action_date")) + 1).otherwise(year(col("end_action_date")))
)

# COMMAND ----------

# 2428
df_2428 = df_2429.groupBy("filing_fy").agg(
    count("serial_number").astype(IntegerType()).alias("cancellation_volume")
).withColumnRenamed(
    "filing_fy", "ttab_filing_fy"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top Track

# COMMAND ----------

# 2430
df_2430 = df_2428.join(df_2312, df_2428.ttab_filing_fy == df_2312.fy).withColumnRenamed("ttab_filing_fy", "fiscal_year")

# COMMAND ----------

# 2431, 2432
win_2431 = Window().orderBy(desc("fiscal_year"))
df_2431 = df_2430.withColumn("rn", row_number().over(win_2431))
df_2431 = df_2431.filter(col("rn") != 1).drop("rn")

# COMMAND ----------

# 2435 
df_2435 = df_2431.withColumn(
    "fy_minus_1", col("fiscal_year") - lit(1)
).withColumn(
    "fy_start_dt", make_date(col("fy_minus_1"), lit("10"), lit("01"))
).withColumn(
    "can_rate_per_liveregs", round(col("cancellation_volume") / col("live_registrations"), 6)
)

# COMMAND ----------

# 2491
win_2491 = Window().orderBy("fiscal_year")
df_2491 = df_2435.withColumn(
    "live_registrations_1", lag(col("live_registrations")).over(win_2491)
).withColumn(
    'live_registrations_1', when(col("live_registrations_1").isNull(), col("live_registrations")).otherwise(col("live_registrations_1"))
).withColumn(
    "reg_growth", round((col("live_registrations") - col('live_registrations_1')) / col('live_registrations_1') * 100, 6)
).fillna(0, subset=["reg_growth"])

# COMMAND ----------

# 2492, 2504
df_2492 = df_2491.withColumn(
    "reg_growth_4", lag(col("reg_growth"), 4).over(win_2491)
).withColumn(
    "reg_growth_3", lag(col("reg_growth"), 3).over(win_2491)
).withColumn(
    "reg_growth_2", lag(col("reg_growth"), 2).over(win_2491)
).withColumn(
    "reg_growth_1", lag(col("reg_growth"), 1).over(win_2491)
).fillna(0, subset=["reg_growth_1", "reg_growth_2", "reg_growth_3", "reg_growth_4"])

df_2492 = df_2492.withColumn('five_yr_avg_growth_rate', round((col('reg_growth_4') + col('reg_growth_3') + col('reg_growth_2') + col('reg_growth_1') + col("reg_growth")) / 5, 6))

df_2504 = df_2492.withColumn('three_yr_avg_growth_rate', round((col('reg_growth_2') + col('reg_growth_1') + col("reg_growth")) / 3, 6))

# COMMAND ----------

# 2488
df_2488 = df_2504.withColumn(
    "can_rate_per_liveregs_4", lag(col("can_rate_per_liveregs"), 4).over(win_2491)
).withColumn(
    "can_rate_per_liveregs_3", lag(col("can_rate_per_liveregs"), 3).over(win_2491)
).withColumn(
    "can_rate_per_liveregs_2", lag(col("can_rate_per_liveregs"), 2).over(win_2491)
).withColumn(
    "can_rate_per_liveregs_1", lag(col("can_rate_per_liveregs"), 1).over(win_2491)
)

# account for nearest valid row setting in multi row tool
df_2488 = df_2488.withColumn(
    "can_rate_per_liveregs_1", when(col("can_rate_per_liveregs_1").isNull(), col("can_rate_per_liveregs")).otherwise(col("can_rate_per_liveregs_1"))
).withColumn(
    "can_rate_per_liveregs_2", when(col("can_rate_per_liveregs_2").isNull(), col("can_rate_per_liveregs_1")).otherwise(col("can_rate_per_liveregs_2"))
).withColumn(
    "can_rate_per_liveregs_3", when(col("can_rate_per_liveregs_3").isNull(), col("can_rate_per_liveregs_2")).otherwise(col("can_rate_per_liveregs_3"))
).withColumn(
    "can_rate_per_liveregs_4", when(col("can_rate_per_liveregs_4").isNull(), col("can_rate_per_liveregs_3")).otherwise(col("can_rate_per_liveregs_4"))
)


df_2488 = df_2488.withColumn('five_yr_rate_avg', round((col('can_rate_per_liveregs_4') + col('can_rate_per_liveregs_3') + col('can_rate_per_liveregs_2') + col('can_rate_per_liveregs_1') + col("can_rate_per_liveregs")) / 5, 6))

df_2493 = df_2488.withColumn('three_yr_rate_avg', round((col('can_rate_per_liveregs_2') + col('can_rate_per_liveregs_1') + col("can_rate_per_liveregs")) / 3, 6))

# COMMAND ----------

# 2489
df_2489 = df_2493.withColumn(
    "predicted_3yr_avg", round(col("three_yr_rate_avg") * col("live_registrations"), 6)
).withColumn(
    "predicted_5yr_avg", round(col("five_yr_rate_avg") * col("live_registrations"), 6)
)

# COMMAND ----------

# 2495
df_2495 = df_2489.withColumn(
    "five_yr_delta_pct", round(abs(col("cancellation_volume") - col("predicted_5yr_avg")) / ((col("cancellation_volume") + col("predicted_5yr_avg")) / lit(2)) * lit(100), 6)
).withColumn(
    "three_yr_delta_pct", abs(col("cancellation_volume") - col("predicted_3yr_avg")) / ((col("cancellation_volume") + col("predicted_3yr_avg")) / lit(2)) * lit(100)
).withColumn(
    "diff_counts_5yr", round(col("predicted_5yr_avg") - col("cancellation_volume"), 0)
).withColumn(
    "diff_counts_3yr", round(col("predicted_3yr_avg") - col("cancellation_volume"), 0)
)

# COMMAND ----------

# 2436 - 38
win_2436 = Window().orderBy(desc("fiscal_year"))
df_2436 = df_2495.withColumn("rn", row_number().over(win_2436)).filter(col("rn") == 1).select("fiscal_year")

# COMMAND ----------

# 2439 - 41
df_2439 = df_2436.withColumn("fiscal_year", explode(sequence(col("fiscal_year") + lit(1), col("fiscal_year") + lit(6))))

# COMMAND ----------

# 2442
df_2442 = df_2439.withColumn(
    "fy_start_dt", make_date(col("fiscal_year") - lit(1), lit("10"), lit("01"))
).withColumn(
    "fy_end_dt", make_date(col("fiscal_year"), lit("09"), lit("30"))
)

# COMMAND ----------

# 2443
df_2443 = df_2495.unionByName(df_2442, allowMissingColumns = True)

# COMMAND ----------

### loop through 6 times
win_2451 = Window().orderBy("fiscal_year")
df_can_loop = df_2443
for i in range(0,6):
    df_can_loop = df_can_loop.withColumn(
        "live_registrations", when(~col("live_registrations").isNull(), col("live_registrations")).otherwise(
            round(lag(col("live_registrations")).over(win_2451) + (lag(col("live_registrations")).over(win_2451) * lag(col("three_yr_avg_growth_rate")).over(win_2451) / lit(100)))
        )
    ).withColumn(
        "reg_growth", when(~col("reg_growth").isNull(), col("reg_growth")).otherwise(
            round((col("live_registrations") - lag(col("live_registrations")).over(win_2451)) / lag(col("live_registrations")).over(win_2451) *  lit(100), 6)
        )
    ).withColumn(
        "three_yr_avg_growth_rate", when(~col("three_yr_avg_growth_rate").isNull(), col("three_yr_avg_growth_rate")).otherwise(
            round((col("reg_growth") + lag(col("reg_growth")).over(win_2451) + lag(col("reg_growth"), 2).over(win_2451)) / lit(3), 6)
        )
    ).withColumn(
        "cancellation_volume", when(~col("cancellation_volume").isNull(), col("cancellation_volume")).otherwise(
            round(lag(col("three_yr_rate_avg")).over(win_2451) * lag(col("live_registrations")).over(win_2451))
        )
    ).withColumn(
        "five_yr_avg_growth_rate",  when(col("live_registrations").isNull(), lit(None)).otherwise(col("five_yr_avg_growth_rate"))
    ).withColumn(
        "can_rate_per_liveregs",  when(col("can_rate_per_liveregs").isNull(), round(col("cancellation_volume") / col("live_registrations"), 6)).otherwise(col("can_rate_per_liveregs"))
    ).withColumn(
        "three_yr_avg_growth_rate",  when(col("live_registrations").isNull(), lit(None)).otherwise(col("three_yr_avg_growth_rate"))
    ).withColumn(
        "three_yr_rate_avg", when(~col("three_yr_rate_avg").isNull(), col("three_yr_rate_avg")).otherwise(
            round((col("can_rate_per_liveregs") + lag(col("can_rate_per_liveregs")).over(win_2451) + lag(col("can_rate_per_liveregs"), 2).over(win_2451)) / lit(3), 6)
        )
    ).withColumn(
        "three_yr_rate_avg",  when(col("live_registrations").isNull(), lit(None)).otherwise(col("three_yr_rate_avg"))
    )

# COMMAND ----------

# 2502
df_2502 = df_can_loop.drop("fy_minus_1").withColumnRenamed("cancellation_volume", "volume")

# COMMAND ----------

# 2505
df_2505 = df_2502.withColumn("ttab_type", lit("CANCELLATION"))

# COMMAND ----------

# 2511
df_2511 = df_2505.select("ttab_type", "fiscal_year", "fy_start_dt", "fy_end_dt", "volume")

# COMMAND ----------

# 2512, 2514
min_st_dt = df_2511.groupBy().agg(min(col("fy_start_dt")).alias("min_dt")).collect()[0][0]
max_end_dt = df_2511.groupBy().agg(max(col("fy_end_dt")).alias("max_dt")).collect()[0][0]

df_2514 = df_2511.withColumn(
    "min_fy_start_dt", lit(min_st_dt)
).withColumn(
    "max_fy_end_dt", lit(max_end_dt)
)

# COMMAND ----------

# 2513
df_2514 = df_2514.withColumn(
    "fiscal_date", explode(sequence(date_add(current_date(), 1), lit(max_end_dt)))
)

# COMMAND ----------

# 2516
df_2516 = df_2514.withColumn(
    "fiscal_year",  when(month(col("fiscal_date")) > 9, year(col("fiscal_date")) + 1).otherwise(year(col("fiscal_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bottom Track

# COMMAND ----------

# 2524, 2526
df_2524 = df_2428.filter(col("ttab_filing_fy") > 1989).withColumn("count", lit(1))

# COMMAND ----------

# 2523
df_2523 = df_2524.join(df_2429, df_2524.ttab_filing_fy == df_2429.filing_fy)

# COMMAND ----------

# 2530
df_2530 = df_2523.groupBy("ttab_filing_fy").agg(sum("count").alias("actual_volume")).withColumnRenamed("ttab_filing_fy", "fiscal_year")

# COMMAND ----------

# 2534, 2539
df_2534 = df_2502.filter(lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))).withColumnRenamed("volume", "estimated_volume")

# COMMAND ----------

# 2531
df_2531 = df_2534.join(df_2530, "fiscal_year")

# COMMAND ----------

# 2532
df_2532 = df_2531.withColumn(
    "volume", col("estimated_volume") - col("actual_volume")
)

# COMMAND ----------

# 2536
# add in fy_end_dt column (not present in alteryx workflow) to account for differences in handling null value in between function
df_2536 = df_2532.unionByName(
    df_2502.withColumn(
        "fy_end_dt", make_date(col("fiscal_year"), lit("09"), lit("30"))
    ).filter(~lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))), allowMissingColumns = True
)

# COMMAND ----------

# 2507
df_2507 = df_2516.drop("volume").join(df_2536, "fiscal_year").select(
    "fiscal_date", 
    "fiscal_year", 
    col("ttab_type").alias("ttab_case_type"),
    "volume"
)

# COMMAND ----------

# 2517
win_2517 = Window().partitionBy("fiscal_date").orderBy("fiscal_year")
df_2517 = df_2507.withColumn("rn", row_number().over(win_2517)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------

# 2510
df_2510 = df_2517.filter(col("fiscal_date") > current_date())

# COMMAND ----------

# 2508
df_2508 = df_2510.withColumn(
    "actual_estimated", lit("Estimated")
).withColumn(
    "avg_app_rate", lit(None)
).withColumn(
    "avg_can_rate", lit(None)
).withColumn(
    "avg_opp_rate", lit(None)
).withColumn(
    "todays_fy", when(month(current_date()) > 9, year(current_date()) + 1).otherwise(year(current_date()))
).withColumn(
    "today_fy_end", make_date(col("todays_fy"), lit("09"), lit("30"))
).withColumn(
    "cur_fy_rmg_days", datediff(col("today_fy_end"), current_date())
).withColumn(
    "base_total", when(col("fiscal_year") == col("todays_fy"), col("volume") / col("cur_fy_rmg_days")).otherwise(col("volume") / lit(365))
).withColumnRenamed(
    "fiscal_date", "date"
)

# COMMAND ----------

# 2525
df_2525 = df_2523.groupBy("instituted_date").agg(sum("count").alias("base_total"))

# COMMAND ----------

# 2527
df_2527 = df_2525.withColumn(
    "actual_estimated", lit("Actual")
).withColumn(
    "ttab_case_type", lit("CANCELLATION")
).withColumn(
    "fiscal_year",  when(month(col("instituted_date")) > 9, year(col("instituted_date")) + 1).otherwise(year(col("instituted_date")))
).withColumnRenamed(
    "instituted_date", "date"
)

# COMMAND ----------

# 2518
df_2518 = df_2527.unionByName(df_2508, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Appeals Workloads

# COMMAND ----------

# 2547
df_2547 = ttab_detail.filter(col("ttab_issue_type") == "EX PARTE APPEAL")

# COMMAND ----------

# 2550
df_2550 = df_2547.withColumn(
    "filing_fy", when(month(col("filing_date")) > 9, year(col("filing_date")) + 1).otherwise(year(col("filing_date")))
).withColumn(
    "end_action_date", when((col("decision_date") == "") | (col("decision_date").isNull()), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "end_action_fy", when(month(col("end_action_date")) > 9, year(col("end_action_date")) + 1).otherwise(year(col("end_action_date")))
)

# COMMAND ----------

# 2549 
df_2549 = df_2550.groupBy("filing_fy").agg(count("serial_number").alias("appeal_volume")).withColumnRenamed("filing_fy", "ttab_filing_fy")

# COMMAND ----------

# 2577
df_2577 = ttab_detail.filter(col("refusal"))

# COMMAND ----------

# 2548
df_2548 = df_2577.withColumn(
    "new_refusal_dt", date_add(col("final_refusal_date"), 180)
).withColumn(
    "refusal_fy", when(month(col("new_refusal_dt")) > 9, year(col("new_refusal_dt")) + 1).otherwise(year(col("new_refusal_dt")))
)

# COMMAND ----------

# 2551
df_2551 = df_2548.groupBy("refusal_fy").agg(count("serial_number").alias("total_refusals"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top Track

# COMMAND ----------

# 2552
df_2552 = df_2549.join(df_2551, df_2549.ttab_filing_fy == df_2551.refusal_fy).withColumnRenamed("ttab_filing_fy", "fiscal_year")

# COMMAND ----------

# 2559, 2560
win_2560 = Window().orderBy(desc("fiscal_year"))
df_2560 = df_2552.withColumn("rn", row_number().over(win_2560))
df_2560 = df_2560.filter(col("rn") != 1).drop("rn")

# COMMAND ----------

# 2563
df_2563 = df_2560.withColumn(
    "fy_minus_1", col("fiscal_year") - lit(1)
).withColumn(
    "fy_start_dt", make_date(col("fy_minus_1"), lit("10"), lit("01"))
).withColumn(
    "appeal_rate_per_refusals", round(col("appeal_volume") / col("total_refusals"), 6)
)

# COMMAND ----------

# 2557
win_2557 = Window().orderBy("fiscal_year")
df_2557 = df_2563.withColumn(
    "total_refusals_1", lag(col("total_refusals")).over(win_2557)
).withColumn(
    'total_refusals_1', when(col("total_refusals_1").isNull(), col("total_refusals")).otherwise(col("total_refusals_1"))
).withColumn(
    "refusal_growth", round((col("total_refusals") -  col("total_refusals_1")) / col("total_refusals_1") * 100, 6)
)

# COMMAND ----------

# 2558, 2585
df_2558 = df_2557.withColumn(
    "refusal_growth_4", lag(col("refusal_growth"), 4).over(win_2557)
).withColumn(
    "refusal_growth_3", lag(col("refusal_growth"), 3).over(win_2557)
).withColumn(
    "refusal_growth_2", lag(col("refusal_growth"), 2).over(win_2557)
).withColumn(
    "refusal_growth_1", lag(col("refusal_growth"), 1).over(win_2557)
).fillna(0, subset=["refusal_growth_1", "refusal_growth_2", "refusal_growth_3", "refusal_growth_4"])

df_2558 = df_2558.withColumn('five_yr_avg_refusal_rate', round((col('refusal_growth_4') + col('refusal_growth_3') + col('refusal_growth_2') + col('refusal_growth_1') + col("refusal_growth")) / 5, 6))

df_2585 = df_2558.withColumn('three_yr_avg_refusal_rate', round((col('refusal_growth_2') + col('refusal_growth_1') + col("refusal_growth")) / 3, 6))

# COMMAND ----------

# 2553, 2583
df_2553 = df_2585.withColumn(
    "appeal_rate_per_refusals_4", lag(col("appeal_rate_per_refusals"), 4).over(win_2557)
).withColumn(
    "appeal_rate_per_refusals_3", lag(col("appeal_rate_per_refusals"), 3).over(win_2557)
).withColumn(
    "appeal_rate_per_refusals_2", lag(col("appeal_rate_per_refusals"), 2).over(win_2557)
).withColumn(
    "appeal_rate_per_refusals_1", lag(col("appeal_rate_per_refusals"), 1).over(win_2557)
)

# account for nearest valid row setting in multi row tool
df_2553 = df_2553.withColumn(
    "appeal_rate_per_refusals_1", when(col("appeal_rate_per_refusals_1").isNull(), col("appeal_rate_per_refusals")).otherwise(col("appeal_rate_per_refusals_1"))
).withColumn(
    "appeal_rate_per_refusals_2", when(col("appeal_rate_per_refusals_2").isNull(), col("appeal_rate_per_refusals_1")).otherwise(col("appeal_rate_per_refusals_2"))
).withColumn(
    "appeal_rate_per_refusals_3", when(col("appeal_rate_per_refusals_3").isNull(), col("appeal_rate_per_refusals_2")).otherwise(col("appeal_rate_per_refusals_3"))
).withColumn(
    "appeal_rate_per_refusals_4", when(col("appeal_rate_per_refusals_4").isNull(), col("appeal_rate_per_refusals_3")).otherwise(col("appeal_rate_per_refusals_4"))
)

df_2553 = df_2553.withColumn('five_yr_rate_avg', round((col('appeal_rate_per_refusals_4') + col('appeal_rate_per_refusals_3') + col('appeal_rate_per_refusals_2') + col('appeal_rate_per_refusals_1') + col("appeal_rate_per_refusals")) / 5, 6))

df_2583 = df_2553.withColumn('three_yr_rate_avg', round((col('appeal_rate_per_refusals_2') + col('appeal_rate_per_refusals_1') + col("appeal_rate_per_refusals")) / 3, 6))

# COMMAND ----------

# 2554
df_2554 = df_2583.withColumn(
    "predicted_3yr_avg", round(col("three_yr_rate_avg") * col("total_refusals"), 6)
).withColumn(
    "predicted_5yr_avg", round(col("five_yr_rate_avg") * col("total_refusals"), 6)
)

# COMMAND ----------

# 2555
df_2555 = df_2554.withColumn(
    "five_yr_delta_pct", round(abs(col("appeal_volume") - col("predicted_5yr_avg")) / ((col("appeal_volume") + col("predicted_5yr_avg")) / lit(2)) * lit(100), 6)
).withColumn(
    "three_yr_delta_pct", round(abs(col("appeal_volume") - col("predicted_3yr_avg")) / ((col("appeal_volume") + col("predicted_3yr_avg")) / lit(2)) * lit(100), 6)
).withColumn(
    "diff_counts_5yr", round(col("predicted_5yr_avg") - col("appeal_volume"), 0)
).withColumn(
    "diff_counts_3yr", round(col("predicted_3yr_avg") - col("appeal_volume"), 0)
)

# COMMAND ----------

# 2572
# reuse fiscal year transpose from cancellations
df_2572 = df_2555.unionByName(df_2442, allowMissingColumns=True)

# COMMAND ----------

# loop through 6 times
win_2592 = Window().orderBy("fiscal_year")
df_apps_loop = df_2572
for i in range(0, 6):
    df_apps_loop = df_apps_loop.withColumn(
        "total_refusals", when(~col("total_refusals").isNull(), col("total_refusals")).otherwise(
            round(lag(col("total_refusals")).over(win_2592) + (lag(col("total_refusals")).over(win_2592) * lag(col("three_yr_avg_refusal_rate")).over(win_2592) / lit(100)))
        )
    ).withColumn(
        "refusal_growth", when(~col("refusal_growth").isNull(), col("refusal_growth")).otherwise(
            round((col("total_refusals") - lag(col("total_refusals")).over(win_2592)) / lag(col("total_refusals")).over(win_2592) *  lit(100), 6)
        )
    ).withColumn(
        "three_yr_avg_refusal_rate", when(~col("three_yr_avg_refusal_rate").isNull(), col("three_yr_avg_refusal_rate")).otherwise(
            round((col("refusal_growth") + lag(col("refusal_growth")).over(win_2592) + lag(col("refusal_growth"), 2).over(win_2592)) / lit(3), 6)
        )
    ).withColumn(
        "appeal_volume", when(~col("appeal_volume").isNull(), col("appeal_volume")).otherwise(
            round(lag(col("three_yr_rate_avg")).over(win_2592) * lag(col("total_refusals")).over(win_2592))
        )
    ).withColumn(
        "five_yr_avg_refusal_rate",  when(col("total_refusals").isNull(), lit(None)).otherwise(col("five_yr_avg_refusal_rate"))
    ).withColumn(
        "three_yr_avg_refusal_rate",  when(col("total_refusals").isNull(), lit(None)).otherwise(col("three_yr_avg_refusal_rate"))
    ).withColumn(
        "appeal_rate_per_refusals",  when(col("appeal_rate_per_refusals").isNull(), round(col("appeal_volume") / col("total_refusals"), 6)).otherwise(col("appeal_rate_per_refusals"))
    ).withColumn(
        "three_yr_rate_avg", when(~col("three_yr_rate_avg").isNull(), col("three_yr_rate_avg")).otherwise(
            round((col("appeal_rate_per_refusals") + lag(col("appeal_rate_per_refusals")).over(win_2592) + lag(col("appeal_rate_per_refusals"), 2).over(win_2592)) / lit(3), 6)
        )
    ).withColumn(
        "three_yr_rate_avg",  when(col("total_refusals").isNull(), lit(None)).otherwise(col("three_yr_rate_avg"))
    )

# COMMAND ----------

# 2633
df_2633 = df_apps_loop.withColumn("ttab_type", lit("EX PARTE APPEAL"))

# COMMAND ----------

# 2634
df_2634 = df_2633.select("fiscal_year", col("appeal_volume").alias("volume"), "ttab_type", "fy_start_dt", "fy_end_dt")

# COMMAND ----------

# 2640, 2642
min_st_dt = df_2634.groupBy().agg(min(col("fy_start_dt")).alias("min_dt")).collect()[0][0]
max_end_dt = df_2634.groupBy().agg(max(col("fy_end_dt")).alias("max_dt")).collect()[0][0]

df_2642 = df_2634.withColumn(
    "min_fy_start_dt", lit(min_st_dt)
).withColumn(
    "max_fy_end_dt", lit(max_end_dt)
)

# COMMAND ----------

# 2641
df_2641 = df_2642.withColumn(
    "fiscal_date", explode(sequence(date_add(current_date(), 1), lit(max_end_dt)))
)

# COMMAND ----------

# 2644
df_2644 = df_2641.withColumn(
    "fiscal_year",  when(month(col("fiscal_date")) > 9, year(col("fiscal_date")) + 1).otherwise(year(col("fiscal_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bottom Track

# COMMAND ----------

# 2662, 2664
df_2662 = df_2549.filter(col("ttab_filing_fy") > 1989).withColumn("count", lit(1))

# COMMAND ----------

# 2661
df_2661 = df_2662.join(df_2550, df_2662.ttab_filing_fy == df_2550.filing_fy)

# COMMAND ----------

# 2663
df_2663 = df_2661.groupBy("instituted_date").agg(sum("count").alias("base_total"))

# COMMAND ----------

# 2665
df_2665 = df_2663.withColumn(
    "actual_estimated", lit("Actual")
).withColumn(
    "ttab_case_type", lit("EX PARTE APPEAL")
).withColumn(
    "fiscal_year",  when(month(col("instituted_date")) > 9, year(col("instituted_date")) + 1).otherwise(year(col("instituted_date")))
).withColumnRenamed(
    "instituted_date", "date"
)

# COMMAND ----------

# 2668
df_2668 = df_2665.filter(col("fiscal_year") > 1989)

# COMMAND ----------

# 2667
df_2667 = df_2661.groupBy("ttab_filing_fy").agg(sum("count").alias("actual_volume")).withColumnRenamed("ttab_filing_fy", "fiscal_year")

# COMMAND ----------

# 2655
df_2655 = df_apps_loop.filter(lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))).withColumnRenamed("appeal_volume", "estimated_volume")

# COMMAND ----------

# 2652
df_2652 = df_2667.join(df_2655, "fiscal_year")

# COMMAND ----------

# 2653
df_2653 = df_2652.withColumn("volume", col("estimated_volume") - col("actual_volume"))

# COMMAND ----------

# 2657
# add in fy_end_dt column (not present in alteryx workflow) to account for differences in handling null value in between function
df_2657 =  df_2653.unionByName(
    df_apps_loop.withColumn(
        "fy_end_dt", make_date(col("fiscal_year"), lit("09"), lit("30"))
    ).filter(~lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))).withColumnRenamed("appeal_volume", "volume"), allowMissingColumns=True
)

# COMMAND ----------

# 2635
df_2635 = df_2644.drop("volume").join(df_2657, "fiscal_year").select(
    "fiscal_date", 
    "fiscal_year", 
    col("ttab_type").alias("ttab_case_type"),
    "volume"
)

# COMMAND ----------

# 2645
win_2645 = Window().partitionBy("fiscal_date").orderBy("fiscal_year")
df_2645 = df_2635.withColumn("rn", row_number().over(win_2645)).filter(col("rn") == 1).drop("rn")

# 2638
df_2638 = df_2645.filter(col("fiscal_date") > current_date())

# COMMAND ----------

# 2636
df_2636 = df_2638.withColumn(
    "actual_estimated", lit("Estimated")
).withColumn(
    "avg_app_rate", lit(None)
).withColumn(
    "avg_can_rate", lit(None)
).withColumn(
    "avg_opp_rate", lit(None)
).withColumn(
    "todays_fy", when(month(current_date()) > 9, year(current_date()) + 1).otherwise(year(current_date()))
).withColumn(
    "today_fy_end", make_date(col("todays_fy"), lit("09"), lit("30"))
).withColumn(
    "cur_fy_rmg_days", datediff(col("today_fy_end"), current_date())
).withColumn(
    "base_total", when(col("fiscal_year") == col("todays_fy"), col("volume") / col("cur_fy_rmg_days")).otherwise(col("volume") / lit(365))
).withColumnRenamed(
    "fiscal_date", "date"
)

# COMMAND ----------

# 2646
df_2646 = df_2636.unionByName(df_2668, allowMissingColumns=True).drop("todays_fy", "today_fy_end", "cur_fy_rmg_days", "volume")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Oppositions Workloads

# COMMAND ----------

# 2670
df_2670 = ttab_detail.filter(col("ttab_issue_type") == "OPPOSITION")

# COMMAND ----------

# 2673
df_2673 = df_2670.withColumn(
    "filing_fy", when(month(col("filing_date")) > 9, year(col("filing_date")) + 1).otherwise(year(col("filing_date")))
).withColumn(
    "end_action_date", when((col("decision_date") == "") | (col("decision_date").isNull()), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "end_action_fy", when(month(col("end_action_date")) > 9, year(col("end_action_date")) + 1).otherwise(year(col("end_action_date")))
)

# COMMAND ----------

# 2672
df_2672 = df_2673.groupBy("filing_fy").agg(count("serial_number").alias("opposition_volume")).withColumnRenamed("filing_fy", "fiscal_year")

# COMMAND ----------

# 2752
df_2752 = df_2673.withColumn(
    "pub_fy", when(month(col("publication_date")) > 9, year(col("publication_date")) + 1).otherwise(year(col("publication_date")))
)

# COMMAND ----------

# 2753 - 2757
df_2753 = df_2752.withColumn("new_pub_dt", col("publication_date"))

df_2756 = df_2753.filter(~col("pub_fy").eqNullSafe(col("filing_fy"))).withColumn(
    "new_pub_dt", date_add(col("publication_date"), 180)
).withColumn(
    "pub_fy", when(month(col("new_pub_dt")) > 9, year(col("new_pub_dt")) + 1).otherwise(year(col("new_pub_dt")))
)

df_2757 = df_2756.unionByName(df_2753.filter(col("pub_fy").eqNullSafe(col("filing_fy"))))

# COMMAND ----------

# 2674
df_2674 = df_2757.groupBy("pub_fy").agg(count("serial_number").alias("total_pubs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top Track

# COMMAND ----------

# 2675
df_2675 = df_2672.join(df_2674, df_2672.fiscal_year == df_2674.pub_fy)

# COMMAND ----------

# 2683, 2682
win_2682 = Window().orderBy(desc("fiscal_year"))
df_2682 = df_2675.withColumn("rn", row_number().over(win_2682))
df_2682 = df_2682.filter(col("rn") != 1).drop("rn")

# COMMAND ----------

# 2686
df_2686 = df_2682.withColumn(
    "fy_minus_1", col("fiscal_year") - lit(1)
).withColumn(
    "fy_start_dt", make_date(col("fy_minus_1"), lit("10"), lit("01"))
).withColumn(
    "opp_rate_per_pubs", round(col("opposition_volume") / col("total_pubs"), 6)
)

# COMMAND ----------

# 2680
win_2680 = Window().orderBy("fiscal_year")
df_2680 = df_2686.withColumn(
    "total_pubs_1", lag(col("total_pubs")).over(win_2680)
).withColumn(
    'total_pubs_1', when(col("total_pubs_1").isNull(), col("total_pubs")).otherwise(col("total_pubs_1"))
).withColumn(
    "pub_growth", round((col("total_pubs") -  col("total_pubs_1")) / col("total_pubs_1") * 100, 6)
)

# COMMAND ----------

# 2681, 2763
df_2681 = df_2680.withColumn(
    "pub_growth_4", lag(col("pub_growth"), 4).over(win_2680)
).withColumn(
    "pub_growth_3", lag(col("pub_growth"), 3).over(win_2680)
).withColumn(
    "pub_growth_2", lag(col("pub_growth"), 2).over(win_2680)
).withColumn(
    "pub_growth_1", lag(col("pub_growth"), 1).over(win_2680)
).fillna(0, subset=["pub_growth_1", "pub_growth_2", "pub_growth_3", "pub_growth_4"])

df_2681 = df_2681.withColumn('five_yr_avg_pub_rate', round((col('pub_growth_4') + col('pub_growth_3') + col('pub_growth_2') + col('pub_growth_1') + col("pub_growth")) / 5, 6))

df_2681 = df_2681.withColumn('three_yr_avg_pub_rate', round((col('pub_growth_2') + col('pub_growth_1') + col("pub_growth")) / 3, 6))

# COMMAND ----------

# 2676, 2741
df_2676 = df_2681.withColumn(
    "opp_rate_per_pubs_4", lag(col("opp_rate_per_pubs"), 4).over(win_2680)
).withColumn(
    "opp_rate_per_pubs_3", lag(col("opp_rate_per_pubs"), 3).over(win_2680)
).withColumn(
    "opp_rate_per_pubs_2", lag(col("opp_rate_per_pubs"), 2).over(win_2680)
).withColumn(
    "opp_rate_per_pubs_1", lag(col("opp_rate_per_pubs"), 1).over(win_2680)
)

# account for nearest valid row setting in multi row tool
df_2676 = df_2676.withColumn(
    "opp_rate_per_pubs_1", when(col("opp_rate_per_pubs_1").isNull(), col("opp_rate_per_pubs")).otherwise(col("opp_rate_per_pubs_1"))
).withColumn(
    "opp_rate_per_pubs_2", when(col("opp_rate_per_pubs_2").isNull(), col("opp_rate_per_pubs_1")).otherwise(col("opp_rate_per_pubs_2"))
).withColumn(
    "opp_rate_per_pubs_3", when(col("opp_rate_per_pubs_3").isNull(), col("opp_rate_per_pubs_2")).otherwise(col("opp_rate_per_pubs_3"))
).withColumn(
    "opp_rate_per_pubs_4", when(col("opp_rate_per_pubs_4").isNull(), col("opp_rate_per_pubs_3")).otherwise(col("opp_rate_per_pubs_4"))
)

df_2676 = df_2676.withColumn('five_yr_rate_avg', round((col('opp_rate_per_pubs_4') + col('opp_rate_per_pubs_3') + col('opp_rate_per_pubs_2') + col('opp_rate_per_pubs_1') + col("opp_rate_per_pubs")) / 5, 6))

df_2741 = df_2676.withColumn('three_yr_rate_avg', round((col('opp_rate_per_pubs_2') + col('opp_rate_per_pubs_1') + col("opp_rate_per_pubs")) / 3, 6))

# COMMAND ----------

# 2677
df_2677 = df_2741.withColumn(
    "predicted_3yr_avg", round(col("three_yr_rate_avg") * col("total_pubs"), 6)
).withColumn(
    "predicted_5yr_avg", round(col("five_yr_rate_avg") * col("total_pubs"), 6)
)

# COMMAND ----------

# 2678
df_2678 = df_2677.withColumn(
    "five_yr_delta_pct", round(abs(col("opposition_volume") - col("predicted_5yr_avg")) / ((col("opposition_volume") + col("predicted_5yr_avg")) / lit(2)) * lit(100), 6)
).withColumn(
    "three_yr_delta_pct", round(abs(col("opposition_volume") - col("predicted_3yr_avg")) / ((col("opposition_volume") + col("predicted_3yr_avg")) / lit(2)) * lit(100), 6)
).withColumn(
    "diff_counts_5yr", round(col("predicted_5yr_avg") - col("opposition_volume"), 0)
).withColumn(
    "diff_counts_3yr", round(col("predicted_3yr_avg") - col("opposition_volume"), 0)
)

# COMMAND ----------

# 2695
# reuse fiscal year transpose from cancellations
df_2695 = df_2678.unionByName(df_2442, allowMissingColumns=True)

# COMMAND ----------

# loop through 6 times
win_2704 = Window().orderBy("fiscal_year")
df_opps_loop = df_2695
for i in range(0,6):
    df_opps_loop = df_opps_loop.withColumn(
        "total_pubs", when(~col("total_pubs").isNull(), col("total_pubs")).otherwise(
            round(lag(col("total_pubs")).over(win_2704) + (lag(col("total_pubs")).over(win_2704) * lag(col("three_yr_avg_pub_rate")).over(win_2704) / lit(100)))
        )
    ).withColumn(
        "pub_growth", when(~col("pub_growth").isNull(), col("pub_growth")).otherwise(
            round((col("total_pubs") - lag(col("total_pubs")).over(win_2704)) / lag(col("total_pubs")).over(win_2704) *  lit(100), 6)
        )
    ).withColumn(
        "three_yr_avg_pub_rate", when(~col("three_yr_avg_pub_rate").isNull(), col("three_yr_avg_pub_rate")).otherwise(
            round((col("pub_growth") + lag(col("pub_growth")).over(win_2704) + lag(col("pub_growth"), 2).over(win_2704)) / lit(3), 6)
        )
    ).withColumn(
        "opposition_volume", when(~col("opposition_volume").isNull(), col("opposition_volume")).otherwise(
            round(lag(col("three_yr_rate_avg")).over(win_2704) * lag(col("total_pubs")).over(win_2704))
        )
    ).withColumn(
        "five_yr_avg_pub_rate",  when(col("total_pubs").isNull(), lit(None)).otherwise(col("five_yr_avg_pub_rate"))
    ).withColumn(
        "three_yr_avg_pub_rate",  when(col("total_pubs").isNull(), lit(None)).otherwise(col("three_yr_avg_pub_rate"))
    ).withColumn(
        "opp_rate_per_pubs",  when(col("opp_rate_per_pubs").isNull(), round(col("opposition_volume") / col("total_pubs"), 6)).otherwise(col("opp_rate_per_pubs"))
    ).withColumn(
        "three_yr_rate_avg", when(~col("three_yr_rate_avg").isNull(), col("three_yr_rate_avg")).otherwise(
            round((col("opp_rate_per_pubs") + lag(col("opp_rate_per_pubs")).over(win_2704) + lag(col("opp_rate_per_pubs"), 2).over(win_2704)) / lit(3), 6)
        )
    ).withColumn(
        "three_yr_rate_avg",  when(col("total_pubs").isNull(), lit(None)).otherwise(col("three_yr_rate_avg"))
    )

# COMMAND ----------

# 2767
df_2767 = df_opps_loop.withColumn("ttab_type", lit("OPPOSITION"))

# COMMAND ----------

# 2773
df_2773 = df_2767.select("fiscal_year", col("opposition_volume").alias("volume"), "ttab_type", "fy_start_dt", "fy_end_dt")

# COMMAND ----------

# 2774, 2776
min_st_dt = df_2773.groupBy().agg(min(col("fy_start_dt")).alias("min_dt")).collect()[0][0]
max_end_dt = df_2773.groupBy().agg(max(col("fy_end_dt")).alias("max_dt")).collect()[0][0]

df_2776 = df_2773.withColumn(
    "min_fy_start_dt", lit(min_st_dt)
).withColumn(
    "max_fy_end_dt", lit(max_end_dt)
)

# COMMAND ----------

# 2775
df_2775 = df_2776.withColumn(
    "fiscal_date", explode(sequence(date_add(current_date(), 1), lit(max_end_dt)))
)

# COMMAND ----------

# 2778
df_2778 = df_2775.withColumn(
    "fiscal_year",  when(month(col("fiscal_date")) > 9, year(col("fiscal_date")) + 1).otherwise(year(col("fiscal_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bottom Track

# COMMAND ----------

# 2796
df_2796 = df_2672.filter(col("fiscal_year") > 1989).withColumn("count", lit(1))

# COMMAND ----------

# 2795
df_2795 = df_2796.join(df_2673, df_2796.fiscal_year == df_2673.filing_fy)

# COMMAND ----------

# 2797
df_2797 = df_2795.groupBy("instituted_date").agg(sum("count").alias("base_total"))

# COMMAND ----------

# 2799
df_2799 = df_2797.withColumn(
    "actual_estimated", lit("Actual")
).withColumn(
    "ttab_case_type", lit("OPPOSITION")
).withColumn(
    "fiscal_year",  when(month(col("instituted_date")) > 9, year(col("instituted_date")) + 1).otherwise(year(col("instituted_date")))
).withColumnRenamed(
    "instituted_date", "date"
)

# COMMAND ----------

# 2789
df_2789 = df_opps_loop.filter(lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))).withColumnRenamed("opposition_volume", "estimated_volume")

# COMMAND ----------

# 2801
df_2801 = df_2795.groupBy("fiscal_year").agg(sum("count").alias("actual_volume")).withColumnRenamed("ttab_filing_fy", "fiscal_year")

# COMMAND ----------

# 2786
df_2786 = df_2789.join(df_2801, "fiscal_year")

# COMMAND ----------

# 2787
df_2787 = df_2786.withColumn("volume", col("estimated_volume") - col("actual_volume"))

# COMMAND ----------

# 2791
df_2791 = df_2787.unionByName(
    df_opps_loop.withColumn(
        "fy_end_dt", make_date(col("fiscal_year"), lit("09"), lit("30"))
    ).filter(~lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))).withColumnRenamed("opposition_volume", "volume"), allowMissingColumns=True
)

# COMMAND ----------

# 2769
df_2769 = df_2778.drop("volume").join(df_2791, "fiscal_year").select(
    "fiscal_date", 
    "fiscal_year", 
    col("ttab_type").alias("ttab_case_type"),
    "volume"
)

# COMMAND ----------

# 2779
win_2779 = Window().partitionBy("fiscal_date").orderBy("fiscal_year")
df_2779 = df_2769.withColumn("rn", row_number().over(win_2779)).filter(col("rn") == 1).drop("rn")

# 2772
df_2772 = df_2779.filter(col("fiscal_date") > current_date())

# COMMAND ----------

# 2770
df_2770 = df_2772.withColumn(
    "actual_estimated", lit("Estimated")
).withColumn(
    "avg_app_rate", lit(None)
).withColumn(
    "avg_can_rate", lit(None)
).withColumn(
    "avg_opp_rate", lit(None)
).withColumn(
    "todays_fy", when(month(current_date()) > 9, year(current_date()) + 1).otherwise(year(current_date()))
).withColumn(
    "today_fy_end", make_date(col("todays_fy"), lit("09"), lit("30"))
).withColumn(
    "cur_fy_rmg_days", datediff(col("today_fy_end"), current_date())
).withColumn(
    "base_total", when(col("fiscal_year") == col("todays_fy"), col("volume") / col("cur_fy_rmg_days")).otherwise(col("volume") / lit(365))
).withColumnRenamed(
    "fiscal_date", "date"
)

# COMMAND ----------

# 2780
df_2780 = df_2770.unionByName(df_2799, allowMissingColumns=True).drop("todays_fy", "today_fy_end", "cur_fy_rmg_days", "volume")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Concurrent Workloads

# COMMAND ----------

# 2806
df_2806 = ttab_detail.filter(col("ttab_issue_type") == "CONCURRENT")

# COMMAND ----------

# 2808
df_2808 = df_2806.withColumn(
    "filing_fy", when(month(col("filing_date")) > 9, year(col("filing_date")) + 1).otherwise(year(col("filing_date")))
).withColumn(
    "end_action_date", when((col("decision_date") == "") | (col("decision_date").isNull()), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "end_action_fy", when(month(col("end_action_date")) > 9, year(col("end_action_date")) + 1).otherwise(year(col("end_action_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top Track

# COMMAND ----------

# 2807
df_2807 = df_2808.groupBy("filing_fy").agg(count("serial_number").alias("concurrent_volume")).withColumnRenamed("filing_fy", "ttab_filing_fy")

# COMMAND ----------

# 2809
df_2809 = df_2807.join(df_2312, df_2807.ttab_filing_fy == df_2312.fy).withColumnRenamed("ttab_filing_fy", "fiscal_year")

# COMMAND ----------

# 2810, 2811
win_2810 = Window().orderBy(desc("fiscal_year"))
df_2811 = df_2809.withColumn("rn", row_number().over(win_2810))
df_2811 = df_2811.filter(col("rn") != 1).drop("rn")

# COMMAND ----------

# 2814
df_2814 = df_2811.withColumn(
    "fy_minus_1", col("fiscal_year") - lit(1)
).withColumn(
    "fy_start_dt", make_date(col("fy_minus_1"), lit("10"), lit("01"))
).withColumn(
    "can_rate_per_liveregs", round(col("concurrent_volume") / col("live_registrations"), 6)
)

# COMMAND ----------

# 2827
win_2827 = Window().orderBy("fiscal_year")
df_2827 = df_2814.withColumn(
    "live_registrations_1", lag(col("live_registrations")).over(win_2827)
).withColumn(
    'live_registrations_1', when(col("live_registrations_1").isNull(), col("live_registrations")).otherwise(col("live_registrations_1"))
).withColumn(
    "reg_growth", round((col("live_registrations") - col('live_registrations_1')) / col('live_registrations_1') * 100, 6)
).fillna(0, subset=["reg_growth"])

# COMMAND ----------

# 2828, 2840
df_2828 = df_2827.withColumn(
    "reg_growth_4", lag(col("reg_growth"), 4).over(win_2827)
).withColumn(
    "reg_growth_3", lag(col("reg_growth"), 3).over(win_2827)
).withColumn(
    "reg_growth_2", lag(col("reg_growth"), 2).over(win_2827)
).withColumn(
    "reg_growth_1", lag(col("reg_growth"), 1).over(win_2827)
).fillna(0, subset=["reg_growth_1", "reg_growth_2", "reg_growth_3", "reg_growth_4"])

df_2828 = df_2828.withColumn('five_yr_avg_growth_rate', round((col('reg_growth_4') + col('reg_growth_3') + col('reg_growth_2') + col('reg_growth_1') + col("reg_growth")) / 5, 6))

df_2840 = df_2828.withColumn('three_yr_avg_growth_rate', round((col('reg_growth_2') + col('reg_growth_1') + col("reg_growth")) / 3, 6))

# COMMAND ----------

# 2824, 2830
df_2824 = df_2840.withColumn(
    "can_rate_per_liveregs_4", lag(col("can_rate_per_liveregs"), 4).over(win_2827)
).withColumn(
    "can_rate_per_liveregs_3", lag(col("can_rate_per_liveregs"), 3).over(win_2827)
).withColumn(
    "can_rate_per_liveregs_2", lag(col("can_rate_per_liveregs"), 2).over(win_2827)
).withColumn(
    "can_rate_per_liveregs_1", lag(col("can_rate_per_liveregs"), 1).over(win_2827)
)

# account for nearest valid row setting in multi row tool
df_2824 = df_2824.withColumn(
    "can_rate_per_liveregs_1", when(col("can_rate_per_liveregs_1").isNull(), col("can_rate_per_liveregs")).otherwise(col("can_rate_per_liveregs_1"))
).withColumn(
    "can_rate_per_liveregs_2", when(col("can_rate_per_liveregs_2").isNull(), col("can_rate_per_liveregs_1")).otherwise(col("can_rate_per_liveregs_2"))
).withColumn(
    "can_rate_per_liveregs_3", when(col("can_rate_per_liveregs_3").isNull(), col("can_rate_per_liveregs_2")).otherwise(col("can_rate_per_liveregs_3"))
).withColumn(
    "can_rate_per_liveregs_4", when(col("can_rate_per_liveregs_4").isNull(), col("can_rate_per_liveregs_3")).otherwise(col("can_rate_per_liveregs_4"))
)


df_2824 = df_2824.withColumn('five_yr_rate_avg', round((col('can_rate_per_liveregs_4') + col('can_rate_per_liveregs_3') + col('can_rate_per_liveregs_2') + col('can_rate_per_liveregs_1') + col("can_rate_per_liveregs")) / 5, 6))

df_2830 = df_2824.withColumn('three_yr_rate_avg', round((col('can_rate_per_liveregs_2') + col('can_rate_per_liveregs_1') + col("can_rate_per_liveregs")) / 3, 6))

# COMMAND ----------

# 2825
df_2825 = df_2830.withColumn(
    "predicted_3yr_avg", round(col("three_yr_rate_avg") * col("live_registrations"), 6)
).withColumn(
    "predicted_5yr_avg", round(col("five_yr_rate_avg") * col("live_registrations"), 6)
)

# COMMAND ----------

# 2832
df_2832 = df_2825.withColumn(
    "five_yr_delta_pct", round(abs(col("concurrent_volume") - col("predicted_5yr_avg")) / ((col("concurrent_volume") + col("predicted_5yr_avg")) / lit(2)) * lit(100), 6)
).withColumn(
    "three_yr_delta_pct", abs(col("concurrent_volume") - col("predicted_3yr_avg")) / ((col("concurrent_volume") + col("predicted_3yr_avg")) / lit(2)) * lit(100)
).withColumn(
    "diff_counts_5yr", round(col("predicted_5yr_avg") - col("concurrent_volume"), 0)
).withColumn(
    "diff_counts_3yr", round(col("predicted_3yr_avg") - col("concurrent_volume"), 0)
)

# COMMAND ----------

# 2822
df_2822 = df_2832.unionByName(df_2442, allowMissingColumns = True)

# COMMAND ----------

# loop through 6 times
win_2847 = Window().orderBy("fiscal_year")
df_concur_loop = df_2822
for i in range(0,6):
    df_concur_loop = df_concur_loop.withColumn(
        "live_registrations", when(~col("live_registrations").isNull(), col("live_registrations")).otherwise(
            round(lag(col("live_registrations")).over(win_2847) + (lag(col("live_registrations")).over(win_2847) * lag(col("three_yr_avg_growth_rate")).over(win_2847) / lit(100)))
        )
    ).withColumn(
        "reg_growth", when(~col("reg_growth").isNull(), col("reg_growth")).otherwise(
            round((col("live_registrations") - lag(col("live_registrations")).over(win_2847)) / lag(col("live_registrations")).over(win_2847) *  lit(100), 6)
        )
    ).withColumn(
        "three_yr_avg_growth_rate", when(~col("three_yr_avg_growth_rate").isNull(), col("three_yr_avg_growth_rate")).otherwise(
            round((col("reg_growth") + lag(col("reg_growth")).over(win_2847) + lag(col("reg_growth"), 2).over(win_2847)) / lit(3), 6)
        )
    ).withColumn(
        "concurrent_volume", when(~col("concurrent_volume").isNull(), col("concurrent_volume")).otherwise(
            round(lag(col("three_yr_rate_avg")).over(win_2451) * lag(col("live_registrations")).over(win_2451))
        )
    ).withColumn(
        "five_yr_avg_growth_rate",  when(col("live_registrations").isNull(), lit(None)).otherwise(col("five_yr_avg_growth_rate"))
    ).withColumn(
        "can_rate_per_liveregs",  when(col("can_rate_per_liveregs").isNull(), round(col("concurrent_volume") / col("live_registrations"), 6)).otherwise(col("can_rate_per_liveregs"))
    ).withColumn(
        "three_yr_avg_growth_rate",  when(col("live_registrations").isNull(), lit(None)).otherwise(col("three_yr_avg_growth_rate"))
    ).withColumn(
        "three_yr_rate_avg", when(~col("three_yr_rate_avg").isNull(), col("three_yr_rate_avg")).otherwise(
            round((col("can_rate_per_liveregs") + lag(col("can_rate_per_liveregs")).over(win_2451) + lag(col("can_rate_per_liveregs"), 2).over(win_2451)) / lit(3), 6)
        )
    ).withColumn(
        "three_yr_rate_avg",  when(col("live_registrations").isNull(), lit(None)).otherwise(col("three_yr_rate_avg"))
    )

# COMMAND ----------

# 2887
df_2887 = df_concur_loop.withColumn("ttab_type", lit("CONCURRENT"))

# COMMAND ----------

# 2893
df_2893 = df_2887.select("ttab_type", "fiscal_year", "fy_start_dt", "fy_end_dt", col("concurrent_volume").alias("volume"))

# COMMAND ----------

# 2894, 2896
min_st_dt = df_2893.groupBy().agg(min(col("fy_start_dt")).alias("min_dt")).collect()[0][0]
max_end_dt = df_2893.groupBy().agg(max(col("fy_end_dt")).alias("max_dt")).collect()[0][0]

df_2896 = df_2893.withColumn(
    "min_fy_start_dt", lit(min_st_dt)
).withColumn(
    "max_fy_end_dt", lit(max_end_dt)
)

# COMMAND ----------

# 2895
df_2895 = df_2896.withColumn(
    "fiscal_date", explode(sequence(date_add(current_date(), 1), lit(max_end_dt)))
)

# COMMAND ----------

# 2898
df_2898 = df_2895.withColumn(
    "fiscal_year",  when(month(col("fiscal_date")) > 9, year(col("fiscal_date")) + 1).otherwise(year(col("fiscal_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bottom Track

# COMMAND ----------

# 2916, 2918
df_2918 = df_2807.filter(col("ttab_filing_fy") > 1989).withColumn("count", lit(1))

# COMMAND ----------

# 2915
df_2915 = df_2918.join(df_2808, df_2918.ttab_filing_fy == df_2808.filing_fy)

# COMMAND ----------

# 2921
df_2921 = df_2915.groupBy("ttab_filing_fy").agg(sum("count").alias("actual_volume")).withColumnRenamed("ttab_filing_fy", "fiscal_year")

# COMMAND ----------

# 2909
df_2909 = df_concur_loop.filter(lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))).withColumnRenamed("concurrent_volume", "estimated_volume")

# COMMAND ----------

# 2906
df_2906 = df_2921.join(df_2909, "fiscal_year")

# COMMAND ----------

# 2907
df_2907 = df_2906.withColumn(
    "volume", col("estimated_volume") - col("actual_volume")
)

# COMMAND ----------

# 2911
# add in fy_end_dt column (not present in alteryx workflow) to account for differences in handling null value in between function
df_2911 = df_2907.unionByName(
    df_concur_loop.withColumn(
        "fy_end_dt", make_date(col("fiscal_year"), lit("09"), lit("30"))
    ).filter(~lit(current_date()).between(col("fy_start_dt"), col("fy_end_dt"))), allowMissingColumns = True
)

# COMMAND ----------

# 2889
df_2889 = df_2898.drop("volume").join(df_2911, "fiscal_year").select(
    "fiscal_date", 
    "fiscal_year", 
    col("ttab_type").alias("ttab_case_type"),
    "volume"
)

# COMMAND ----------

# 2899
win_2899 = Window().partitionBy("fiscal_date").orderBy("fiscal_year")
df_2899 = df_2889.withColumn("rn", row_number().over(win_2899)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------

# 2892
df_2892 = df_2899.filter(col("fiscal_date") > current_date())

# COMMAND ----------

# 2890
df_2890 = df_2892.withColumn(
    "actual_estimated", lit("Estimated")
).withColumn(
    "avg_app_rate", lit(None)
).withColumn(
    "avg_can_rate", lit(None)
).withColumn(
    "avg_opp_rate", lit(None)
).withColumn(
    "todays_fy", when(month(current_date()) > 9, year(current_date()) + 1).otherwise(year(current_date()))
).withColumn(
    "today_fy_end", make_date(col("todays_fy"), lit("09"), lit("30"))
).withColumn(
    "cur_fy_rmg_days", datediff(col("today_fy_end"), current_date())
).withColumn(
    "base_total", when(col("fiscal_year") == col("todays_fy"), col("volume") / col("cur_fy_rmg_days")).otherwise(col("volume") / lit(365))
).withColumnRenamed(
    "fiscal_date", "date"
)

# COMMAND ----------

# 2917
df_2917 = df_2915.groupBy("instituted_date").agg(sum("count").alias("base_total"))

# COMMAND ----------

# 2919
df_2919 = df_2917.withColumn(
    "actual_estimated", lit("Actual")
).withColumn(
    "ttab_case_type", lit("CONCURRENT")
).withColumn(
    "fiscal_year",  when(month(col("instituted_date")) > 9, year(col("instituted_date")) + 1).otherwise(year(col("instituted_date")))
).withColumnRenamed(
    "instituted_date", "date"
)

# COMMAND ----------

# 2900
df_2900 = df_2890.unionByName(df_2919, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Union

# COMMAND ----------

df_all_workloads = df_2518.unionByName(df_2646, allowMissingColumns=True).unionByName(df_2780, allowMissingColumns=True).unionByName(df_2900, allowMissingColumns=True).select("fiscal_year", "date", "ttab_case_type", round(col("base_total"), 2).alias("day_total"), "actual_estimated")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cancellation Proceedings

# COMMAND ----------

df_2353 = ttab_detail.filter(col("ttab_issue_type") == "CANCELLATION")

# COMMAND ----------

df_2343 = df_2353.filter((~col("decision_date").isNull()) | (~col("termination_date").isNull()))

# COMMAND ----------

df_2344 = df_2343.withColumn(
    "case_end_dt", when(col("decision_date").isNull(), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "base_fy", when(month(col("case_end_dt")) > 9, year(col("case_end_dt")) + 1).otherwise(year(col("case_end_dt")))
)

# COMMAND ----------

# 2368
df_2368 = df_2344.groupBy("base_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2930
df_2930 = df_2368.groupBy("base_fy").agg(sum("total_decisions").alias("base_total"))

# COMMAND ----------

# 2346
df_2346 = df_2353.filter(col("decision_date").isNotNull() & col("rfd_date").isNotNull() & (col("rfd_valid") == 1))

# COMMAND ----------

# 2342
df_2342 = df_2346.withColumn(
    "case_end_dt", col("decision_date")
).withColumn(
    "decision_fy", when(month(col("decision_date")) > 9, year(col("decision_date")) + 1).otherwise(year(col("decision_date")))
)

# COMMAND ----------

# 2345
df_2345 = df_2342.groupBy("decision_fy").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2367
df_2367 = df_2342.groupBy("decision_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_judge_decisions"))

# COMMAND ----------

# 2363
df_2363 = df_2367.join(df_2368, "case_end_dt", "full_outer")

# COMMAND ----------

# 2365
df_2365 = df_2363.fillna(
    0, subset=["total_decisions", "total_judge_decisions"]
).withColumn(
    "ttab_case_type", lit("CANCELLATION")
).withColumnRenamed(
    "base_fy", "fiscal_year"
)

# COMMAND ----------

# 2348
df_2348 = df_2345.join(df_2930, df_2345.decision_fy == df_2930.base_fy)

# COMMAND ----------

# 2349
df_2349 = df_2348.withColumn(
    "rate", round(col("total_decisions") / col("base_total"), 4)
)

# COMMAND ----------

# 2362
df_2362 = df_2349.withColumn("ttab_case_type", lit("CANCELLATION"))

# COMMAND ----------

# 2351, 2352, 2357, 2358
win_2351 = Window().orderBy(desc("decision_fy"))
df_2351 = df_2349.withColumn("rn", row_number().over(win_2351))
df_2357 = df_2351.filter((col("rn") > 1) & (col("rn") <= 6)).withColumn("ttab_case_type", lit("CANCELLATION"))

# COMMAND ----------

# 2356
df_2356 = df_2357.groupBy("ttab_case_type").agg(avg("rate").alias("five_yr_avg_jdr"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Appeal Proceedings

# COMMAND ----------

# 2383
df_2383 = ttab_detail.filter(col("ttab_issue_type") == "EX PARTE APPEAL")

# COMMAND ----------

# 2371
df_2371 = df_2383.filter((~col("decision_date").isNull()) | (~col("termination_date").isNull()))

# COMMAND ----------

# 2372
df_2372 = df_2371.withColumn(
    "case_end_dt", when(col("decision_date").isNull(), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "base_fy", when(month(col("case_end_dt")) > 9, year(col("case_end_dt")) + 1).otherwise(year(col("case_end_dt")))
)

# COMMAND ----------

# 2389
df_2389 = df_2372.groupBy("base_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2931
df_2931 = df_2389.groupBy("base_fy").agg(sum("total_decisions").alias("base_total"))

# COMMAND ----------

# 2374
df_2374 = df_2383.filter(col("decision_date").isNotNull() & col("rfd_date").isNotNull() & (col("rfd_valid") == 1))

# COMMAND ----------

# 2370
df_2370 = df_2374.withColumn(
    "case_end_dt", col("decision_date")
).withColumn(
    "decision_fy", when(month(col("decision_date")) > 9, year(col("decision_date")) + 1).otherwise(year(col("decision_date")))
)

# COMMAND ----------

# 2373
df_2373 = df_2370.groupBy("decision_fy").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2392
df_2392 = df_2370.groupBy("decision_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_judge_decisions"))

# COMMAND ----------

# 2930
df_2930 = df_2392.join(df_2389, "case_end_dt", "full_outer")

# COMMAND ----------

# 2934
df_2394 = df_2930.fillna(
    0, subset=["total_decisions", "total_judge_decisions"]
).withColumn(
    "ttab_case_type", lit("EX PARTE APPEAL")
).withColumnRenamed(
    "base_fy", "fiscal_year"
)

# COMMAND ----------

# 2376
df_2376 = df_2373.join(df_2931, df_2373.decision_fy == df_2931.base_fy)

# COMMAND ----------

# 2377
df_2377 = df_2376.withColumn(
    "rate", round(col("total_decisions") / col("base_total"), 4)
)

# COMMAND ----------

# 2387
df_2387 = df_2377.withColumn("ttab_case_type", lit("EX PARTE APPEAL"))

# COMMAND ----------

# 2378 - 80, 2387
win_2378 = Window().orderBy(desc("decision_fy"))
df_2378 = df_2377.withColumn("rn", row_number().over(win_2378))
df_2382 = df_2378.filter((col("rn") > 1) & (col("rn") <= 6)).withColumn("ttab_case_type", lit("EX PARTE APPEAL"))

# COMMAND ----------

# 2381
df_2381 = df_2382.groupBy("ttab_case_type").agg(avg("rate").alias("five_yr_avg_jdr"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Opposition Proceedings

# COMMAND ----------

# 2398
df_2398 = ttab_detail.filter(col("ttab_issue_type") == "OPPOSITION")

# COMMAND ----------

# 2410
df_2410 = df_2398.filter((~col("decision_date").isNull()) | (~col("termination_date").isNull()))

# COMMAND ----------

# 2409
df_2409 = df_2410.withColumn(
    "case_end_dt", when(col("decision_date").isNull(), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "base_fy", when(month(col("case_end_dt")) > 9, year(col("case_end_dt")) + 1).otherwise(year(col("case_end_dt")))
)

# COMMAND ----------

# 2415
df_2415 = df_2409.groupBy("base_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2932
df_2932 = df_2415.groupBy("base_fy").agg(sum("total_decisions").alias("base_total"))

# COMMAND ----------

# 2407
df_2407 = df_2398.filter(col("decision_date").isNotNull() & col("rfd_date").isNotNull() & (col("rfd_valid") == 1))

# COMMAND ----------

# 2411
df_2411 = df_2407.withColumn(
    "case_end_dt", col("decision_date")
).withColumn(
    "decision_fy", when(month(col("decision_date")) > 9, year(col("decision_date")) + 1).otherwise(year(col("decision_date")))
)

# COMMAND ----------

# 2408
df_2408 = df_2411.groupBy("decision_fy").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2419
df_2419 = df_2411.groupBy("decision_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_judge_decisions"))

# COMMAND ----------

# 2417
df_2417 = df_2419.join(df_2415, "case_end_dt", "full_outer")

# COMMAND ----------

# 2416
df_2416 = df_2417.fillna(
    0, subset=["total_decisions", "total_judge_decisions"]
).withColumn(
    "ttab_case_type", lit("OPPOSITION")
).withColumnRenamed(
    "base_fy", "fiscal_year"
)

# COMMAND ----------

# 2405
df_2405 = df_2408.join(df_2932, df_2408.decision_fy == df_2932.base_fy)

# COMMAND ----------

# 2404
df_2404 = df_2405.withColumn(
    "rate", round(col("total_decisions") / col("base_total"), 4)
)

# COMMAND ----------

# 2414
df_2414 = df_2404.withColumn("ttab_case_type", lit("OPPOSITION"))

# COMMAND ----------

# 2401 - 3, 2396, 2399, 2414
win_2403 = Window().orderBy(desc("decision_fy"))
df_2403 = df_2404.withColumn("rn", row_number().over(win_2403))
df_2399 = df_2403.filter((col("rn") > 1) & (col("rn") <= 6)).withColumn("ttab_case_type", lit("OPPOSITION"))

# COMMAND ----------

# 2400
df_2400 = df_2399.groupBy("ttab_case_type").agg(avg("rate").alias("five_yr_avg_jdr"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Concurrent Proceedings

# COMMAND ----------

# 2329
df_2329 = ttab_detail.filter(col("ttab_issue_type") == "CONCURRENT")

# COMMAND ----------

# 2317
df_2317 = df_2329.filter((~col("decision_date").isNull()) | (~col("termination_date").isNull()))

# COMMAND ----------

# 2318
df_2318 = df_2317.withColumn(
    "case_end_dt", when(col("decision_date").isNull(), col("termination_date")).otherwise(col("decision_date"))
).withColumn(
    "base_fy", when(month(col("case_end_dt")) > 9, year(col("case_end_dt")) + 1).otherwise(year(col("case_end_dt")))
)

# COMMAND ----------

# 2335
df_2335 = df_2318.groupBy("base_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2933
df_2933 = df_2335.groupBy("base_fy").agg(sum("total_decisions").alias("base_total"))

# COMMAND ----------

# 2320
df_2320 = df_2329.filter(col("decision_date").isNotNull() & col("rfd_date").isNotNull() & (col("rfd_valid") == 1))

# COMMAND ----------

# 2316
df_2316 = df_2320.withColumn(
    "case_end_dt", col("decision_date")
).withColumn(
    "decision_fy", when(month(col("decision_date")) > 9, year(col("decision_date")) + 1).otherwise(year(col("decision_date")))
)

# COMMAND ----------

# 2319
df_2319 = df_2316.groupBy("decision_fy").agg(countDistinct("proceeding_num").alias("total_decisions"))

# COMMAND ----------

# 2340
df_2340 = df_2316.groupBy("decision_fy", "case_end_dt").agg(countDistinct("proceeding_num").alias("total_judge_decisions"))

# COMMAND ----------

# 2338
df_2338 = df_2340.join(df_2335, "case_end_dt", "full_outer")

# COMMAND ----------

# 2337
df_2337 = df_2338.fillna(
    0, subset=["total_decisions", "total_judge_decisions"]
).withColumn(
    "ttab_case_type", lit("CONCURRENT")
).withColumnRenamed(
    "base_fy", "fiscal_year"
)

# COMMAND ----------

# 2322
df_2322 = df_2319.join(df_2933, df_2319.decision_fy == df_2933.base_fy)

# COMMAND ----------

# 2323
df_2323 = df_2322.withColumn(
    "rate", round(col("total_decisions") / col("base_total"), 4)
)

# COMMAND ----------

# 2334
df_2334 = df_2323.withColumn("ttab_case_type", lit("CONCURRENT"))

# COMMAND ----------

# 2324-26, 2331, 2328
win_2324 = Window().orderBy(desc("decision_fy"))
df_2324 = df_2323.withColumn("rn", row_number().over(win_2324))
df_2328 = df_2324.filter((col("rn") > 1) & (col("rn") <= 6)).withColumn("ttab_case_type", lit("CONCURRENT"))

# COMMAND ----------

# 2327
df_2327 = df_2328.groupBy("ttab_case_type").agg(avg("rate").alias("five_yr_avg_jdr"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final

# COMMAND ----------

# 2422
df_2422 = df_2362.unionByName(df_2387).unionByName(df_2414).unionByName(df_2334).withColumnRenamed("total_decisions", "fy_judge_decisions").withColumnRenamed("base_total", "fy_base_total").withColumnRenamed("ttab_case_type", "right_ttab_case_type")

# COMMAND ----------

# 2423
df_2423 = df_2356.unionByName(df_2381).unionByName(df_2400).unionByName(df_2327)

# COMMAND ----------

# 2299
df_2299 = df_2365.unionByName(df_2394).unionByName(df_2416).unionByName(df_2337)

# COMMAND ----------

# 2300
df_2300 = df_2299.filter((col("fiscal_year") > 1989) & (col("ttab_case_type") != "CONCURRENT"))

# COMMAND ----------

# 2285
df_2285 = df_all_workloads.join(df_2422, [df_all_workloads.fiscal_year == df_2422.decision_fy, df_all_workloads.ttab_case_type == df_2422.right_ttab_case_type], "left")

# COMMAND ----------

# 2286
df_2286 = df_2285.join(df_2423, "ttab_case_type")

# COMMAND ----------

# 2283
df_2283 = df_2286.withColumn(
    "raw_credits", when(col("ttab_case_type") == "EX PARTE APPEAL", round(col("day_total") * 2, 0)).otherwise(round(col("day_total") * 4, 0))
).withColumn(
    "credits_jdr_applied", when(col("ttab_case_type").isin(["EX PARTE APPEAL", "OPPOSITION", "CANCELLATION", "CONCURRENT"]), col("raw_credits") * col("five_yr_avg_jdr")).otherwise(lit(None))
)

# COMMAND ----------

# 2294
df_2294 = df_2283.filter(col("date") > "1989-09-30")

# COMMAND ----------

# 2284
df_2284 = df_2294.withColumnRenamed(
    "rate", "fy_jdr"
).withColumnRenamed(
    "five_yr_avg_jdr", "latest_5yr_avg_jdr"
)

# COMMAND ----------

# add audit columns
df_2300 = df_2300.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

df_2284 = df_2284.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

# set data types
df_2300 = df_2300.withColumn(
    "case_end_dt", col("case_end_dt").astype(DateType())
).drop("decision_fy")

df_2284 = df_2284.withColumn(
    "date", col("date").astype(DateType())
).withColumn(
    "raw_credits", col("raw_credits").astype(IntegerType())
).drop("decision_fy", "base_fy", "right_ttab_case_type")

# COMMAND ----------

# set column ordering
df_2300 = df_2300.select('fiscal_year',
 'case_end_dt',
 'ttab_case_type',
 'total_decisions',
 'total_judge_decisions',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

df_2284 = df_2284.select('fiscal_year',
 'date',
 'ttab_case_type',
 'day_total',
 'actual_estimated',
 'fy_base_total',
 'fy_judge_decisions',
 'fy_jdr',
 'latest_5yr_avg_jdr',
 'raw_credits',
 'credits_jdr_applied',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

# COMMAND ----------

# write to table
df_2300.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ttab_decision_rates")

df_2284.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.ttab_workloads")

# COMMAND ----------



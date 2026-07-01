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

job_name = "ntb_silver_production_simulator_prod_qual"
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

prod_df = spark.sql(f"""
select bw.employee_no,
	bw.fiscal_year_no,
	bw.quarter_no,
	(Coalesce(bw.exam_hour_qt, 0)) exam_hrs,
	(Coalesce(bw.adjustment_hour_qt, 0)) adj_hrs,
	(Coalesce(bw.other_non_examining_hour_qt, 0)) non_exam_hrs,
	(Coalesce(bw.overtime_hour_qt)) ot_hrs,
	(Coalesce(bw.balanced_disposal_qt, 0)) bds,
	sqbw.quarter_bi_week_start_dt,
	sqbw.quarter_bi_week_end_dt
from {tept_catalog}.gold.bi_week_production bw 
	inner join {tept_catalog}.bronze.stnd_quarter_bi_week sqbw on bw.fk_quarter_bi_week_id = sqbw.quarter_bi_week_id
where bw.fiscal_year_no = 
	(
	select distinct sqbw.fiscal_year_no 
	from {tept_catalog}.bronze.stnd_quarter_bi_week sqbw
	where sqbw.quarter_bi_week_end_dt >= Current_Date
	) 
order by bw.fiscal_year_no, bw.quarter_no, bw.fk_quarter_bi_week_id, bw.employee_no""")
#display(prod_df)

# COMMAND ----------

emp_df = spark.sql(f""" select *from {trgt_catalog}.silver.employee_quarter_bi_week""")

# COMMAND ----------

from pyspark.sql.functions import round, col
prod_df = prod_df.withColumn("bds_int", round(col("bds"), 1)) \
    .orderBy(col("employee_no").asc(), col("quarter_bi_week_start_dt").asc())

# COMMAND ----------

joined_prod_left_df = prod_df.join(
    emp_df,
    [
        emp_df["employee_no"] == prod_df["employee_no"],
        emp_df["quarter_bi_week_start_dt"] == prod_df["quarter_bi_week_start_dt"]
    ],
    "right"
).select(
    emp_df["employee_no"],
    emp_df["fiscal_year_no"],
    emp_df["quarter_no"],
    prod_df["exam_hrs"],
    prod_df["adj_hrs"],
    prod_df["non_exam_hrs"],
    prod_df["ot_hrs"],
    emp_df["quarter_bi_week_start_dt"],
    emp_df["quarter_bi_week_end_dt"],
    prod_df["bds_int"],
    emp_df["employee_nm"],
    emp_df["current_organization_cd"],
    emp_df["q1_wks"],
    emp_df["q2_wks"],
    emp_df["q3_wks"],
    emp_df["q4_wks"],
    emp_df["brs_user_id"],
    emp_df["TEPT_LO"]
)

#display(joined_prod_left_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # WF

# COMMAND ----------

wf_df = spark.sql(f"""
select wf.employee_no,
	wf.fiscal_year_no,
	wf.quarter_no,
	bw.bi_week_start_dt,
	bw.bi_week_end_dt,
	wf.action_qt,
	wf.action_per_examining_hour_qt,
	wf.goal_status_ct,
	wf.docket_management_qt,
	wf.document_management_tx,
	wf.bi_week_below_goal_qt 
from {tept_catalog}.bronze.bi_week_workflow wf
left join {tept_catalog}.bronze.stnd_bi_week bw on wf.fk_bi_week_id = bw.bi_week_id 
where wf.fiscal_year_no = 
	(
	select distinct fiscal_year_no 
	from {tept_catalog}.bronze.stnd_quarter_bi_week 
	where quarter_bi_week_end_dt >= current_date()
	)
	and bw.bi_week_start_dt is not null
""")

# COMMAND ----------

from pyspark.sql.functions import trim, coalesce, lit, round

columns_to_clean = [
    "employee_no", "fiscal_year_no", "quarter_no", "action_qt", "action_per_examining_hour_qt", "goal_status_ct", "docket_management_qt",
    "document_management_tx", "bi_week_below_goal_qt"
]

for col_name in columns_to_clean:
    wf_df = wf_df.withColumn(
        col_name,
        trim(coalesce(wf_df[col_name], lit(0)))
    )

wf_df = wf_df.withColumn("action_qt_int", round(col("action_qt"), 1))

# COMMAND ----------

from pyspark.sql.functions import col

joined_wf_right_df = wf_df.join(
    joined_prod_left_df,
    [
        wf_df["employee_no"] == joined_prod_left_df["employee_no"],
        wf_df["bi_week_start_dt"] == joined_prod_left_df["quarter_bi_week_start_dt"]
    ],
    "right"
).select(
    joined_prod_left_df["employee_no"],
    joined_prod_left_df["employee_nm"],
    joined_prod_left_df["current_organization_cd"],
    joined_prod_left_df["quarter_bi_week_start_dt"],
    joined_prod_left_df["quarter_bi_week_end_dt"],
    joined_prod_left_df["fiscal_year_no"],
    joined_prod_left_df["quarter_no"],
    joined_prod_left_df["q1_wks"],
    joined_prod_left_df["q2_wks"],
    joined_prod_left_df["q3_wks"],
    joined_prod_left_df["q4_wks"],
    joined_prod_left_df["brs_user_id"],
    joined_prod_left_df["TEPT_LO"],
    joined_prod_left_df["exam_hrs"],
    wf_df["action_qt"],
    joined_prod_left_df["adj_hrs"],
    joined_prod_left_df["adj_hrs"].alias("adj_hrs_dup"),
    joined_prod_left_df["non_exam_hrs"],
    joined_prod_left_df["ot_hrs"],
    joined_prod_left_df["bds_int"].alias("bds"),
    wf_df["action_per_examining_hour_qt"],
    wf_df["goal_status_ct"],
    wf_df["docket_management_qt"],
    wf_df["document_management_tx"],
    wf_df["bi_week_below_goal_qt"],
    #wf_df["action_qt"]
)
joined_wf_right_df = joined_wf_right_df.withColumn("table", lit("BiWk"))

# COMMAND ----------

# DBTITLE 1,Overwrite table BiWeeK
joined_wf_right_df.write.mode("overwrite").insertInto(f"{trgt_catalog}.silver.employee_bd")

# COMMAND ----------

# MAGIC %md
# MAGIC ## QUAL

# COMMAND ----------

qt_dt = spark.sql(f"""
                  select qt.employee_no,
	qt.fiscal_year_no,
	qt.serial_num_tx,
	qt.quality_review_dt,
	qt.statutory_error_qt,
	qt.prac_pro_error_qt,
	qt.search_ct,
	qt.write_grade_qt,
	qt.explanation_tx,
	bw.quarter_no,
	bw.quarter_bi_week_start_dt,
	bw.quarter_bi_week_end_dt 
from {tept_catalog}.bronze.quality_transaction qt
left join {tept_catalog}.bronze.stnd_quarter_bi_week bw on qt.fiscal_year_no = bw.fiscal_year_no and qt.quality_review_dt >= bw.quarter_bi_week_start_dt and qt.quality_review_dt <= bw.quarter_bi_week_end_dt 
where qt.fiscal_year_no = 
	(
	select distinct fiscal_year_no 
	from {tept_catalog}.bronze.stnd_quarter_bi_week 
	where quarter_bi_week_end_dt >= current_date()
	) 
	and qt.confirmation_in = 1 and qt.employee_no is not null""")

qt_dt = qt_dt.withColumn("employee_no", trim(coalesce(qt_dt["employee_no"], lit(0))))

# COMMAND ----------

qt_dt = qt_dt.withColumn(
    "write_grade_qt_str",
    when(col("write_grade_qt").isNull(), lit("")).otherwise(col("write_grade_qt").cast("int").cast("string"))
)

# COMMAND ----------

from pyspark.sql.functions import lit

joined_qual_left_df = joined_prod_left_df.join(
    qt_dt,
    [
        joined_prod_left_df["employee_no"] == qt_dt["employee_no"],
        joined_prod_left_df["quarter_bi_week_start_dt"] == qt_dt["quarter_bi_week_start_dt"]
    ],
    "left_outer"
).select(
    joined_prod_left_df["employee_no"],
    joined_prod_left_df["employee_nm"],
    joined_prod_left_df["current_organization_cd"],
    joined_prod_left_df["quarter_bi_week_start_dt"],
    joined_prod_left_df["quarter_bi_week_end_dt"],
    joined_prod_left_df["fiscal_year_no"],
    joined_prod_left_df["quarter_no"],
    joined_prod_left_df["q1_wks"],
    joined_prod_left_df["q2_wks"],
    joined_prod_left_df["q3_wks"],
    joined_prod_left_df["q4_wks"],
    joined_prod_left_df["brs_user_id"],
    joined_prod_left_df["TEPT_LO"],
    qt_dt["serial_num_tx"],
    qt_dt["quality_review_dt"],
    qt_dt["statutory_error_qt"],
    qt_dt["prac_pro_error_qt"],
    qt_dt["search_ct"],
    qt_dt["write_grade_qt"],
    qt_dt["explanation_tx"],
    qt_dt["write_grade_qt_str"].alias("write_grade_txt")
)

#display(joined_qual_left_df)

# COMMAND ----------

# DBTITLE 1,Qual Table Overwrite
joined_qual_left_df = joined_qual_left_df.withColumn(
    "qual_status",
    when(col("serial_num_tx").isNull(), lit("null")).otherwise(lit("Qual"))
)
joined_qual_left_df.write.mode("overwrite").insertInto(f"{trgt_catalog}.silver.prod_simulator_qual")

# COMMAND ----------

from pyspark.sql.functions import count, avg

qt = qt_dt.filter(col("search_ct") == "Sufficient").groupBy("employee_no", "search_ct").agg(
    count("search_ct").alias("suff_cnt")
).select("employee_no", "search_ct", "suff_cnt")

qt_2 = qt_dt.groupBy("employee_no").agg(
    count("write_grade_qt").alias("count_write_grade_qt"),
    count("serial_num_tx").alias("count_serial_num_tx"),
    count("search_ct").alias("countnonnull_search_ct"),
    count("write_grade_qt").alias("countnonnull_write_grade_qt"),
    avg("write_grade_qt").alias("avg_write_grade_qt")
)

qt_joined = qt.join(qt_2, "employee_no", "right")
#display(qt_joined)

# COMMAND ----------

from pyspark.sql.functions import count

qt_dt_filtered = qt_dt.filter(col("write_grade_qt") == 1).groupBy("employee_no").agg(
    count("write_grade_qt").alias("Count_write_grade_qt_is_1")
)

qt_final = qt_joined.join(qt_dt_filtered, "employee_no", "left")
qt_final.count()

# COMMAND ----------

from pyspark.sql.functions import col, round, when, lit, expr

qt_final = qt_final.withColumn(
    "suff_rt",
    when(
        (col("countnonnull_search_ct") == 0) | col("suff_cnt").isNull(),
        lit(None)
    ).otherwise(
        round(col("suff_cnt") / col("countnonnull_search_ct"), 4)
    )
)

qt_final = qt_final.withColumn(
    "suff_score",
    when(col("suff_rt").isNull() | (col("countnonnull_search_ct") == 0), lit('---'))
    .when(col("suff_rt") >= 0.95, lit('5'))
    .when(col("suff_rt") >= 0.90, lit('4'))
    .when(col("suff_rt") >= 0.85, lit('3'))
    .when(col("suff_rt") >= 0.80, lit('2'))
    .otherwise(lit('1'))
)

qt_final = qt_final.withColumn(
    "avg_write_rt",
    when(col("countnonnull_write_grade_qt") == 0, lit('---'))
    .otherwise(round(col("avg_write_grade_qt"), 3))
)

qt_final = qt_final.withColumn(
    "write_def",
    when(
        (col("countnonnull_write_grade_qt") == 0) | col("countnonnull_write_grade_qt").isNull(),
        lit('---')
    ).otherwise(
        round(col("Count_write_grade_qt_is_1") / col("countnonnull_write_grade_qt"), 4)
    )
)

qt_final = qt_final.withColumn(
    "avg_write_score",
    when(col("avg_write_rt") == lit('---'), lit('---'))
    .when(
        (col("write_def") != lit('---')) &
        (col("write_def").cast("double") >= 0.25) &
        (col("countnonnull_write_grade_qt") >= 8),
        lit('1')
    )
    .when(
        (col("write_def") != lit('---')) &
        (col("write_def").cast("double") >= 0.15) &
        (col("countnonnull_write_grade_qt") >= 8),
        lit('2')
    )
    .when(col("avg_write_rt").cast("double") >= 4.75, lit('5'))
    .when(col("avg_write_rt").cast("double") >= 4, lit('4'))
    .when(col("avg_write_rt").cast("double") >= 3, lit('3'))
    .when(col("avg_write_rt").cast("double") >= 2, lit('2'))
    .otherwise(lit('1'))
)

qt_final = qt_final.withColumn(
    "write_def%",
    when(col("write_def") == lit('---'), col("write_def"))
    .otherwise(
        expr("concat(format_number(write_def * 100, 3), '%')")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Transfer BD's

# COMMAND ----------

qtr_df = spark.sql(f"""
SELECT DISTINCT
    employee_no,
    fiscal_year_no,
    quarter_no,
    fk_gs_level_cd,
    CAST(fk_gs_level_cd AS INT) AS fk_gs_level_cd_no,
    COALESCE(transfer_balanced_disposal_qt, 0) AS transfer_balanced_disposal_qt,
    CASE
        WHEN CAST(fk_gs_level_cd AS INT) IN (9, 11) THEN 510
        WHEN CAST(fk_gs_level_cd AS INT) = 12 THEN 570
        WHEN CAST(fk_gs_level_cd AS INT) = 13 THEN 620
        ELSE 660
    END AS base_c_bds,
    CASE
        WHEN CAST(fk_gs_level_cd AS INT) IN (9, 11) THEN 460
        WHEN CAST(fk_gs_level_cd AS INT) = 12 THEN 520
        WHEN CAST(fk_gs_level_cd AS INT) = 13 THEN 570
        ELSE 610
    END AS base_fs_bds,
    CASE
        WHEN CAST(fk_gs_level_cd AS INT) IN (9, 11) THEN 410
        WHEN CAST(fk_gs_level_cd AS INT) = 12 THEN 470
        WHEN CAST(fk_gs_level_cd AS INT) = 13 THEN 520
        ELSE 560
    END AS base_m_bds,
    CASE
        WHEN CAST(fk_gs_level_cd AS INT) IN (9, 11) THEN 560
        WHEN CAST(fk_gs_level_cd AS INT) = 12 THEN 620
        WHEN CAST(fk_gs_level_cd AS INT) = 13 THEN 670
        ELSE 710
    END AS base_o_bds,
    ROUND(COALESCE(transfer_balanced_disposal_qt, 0), 1) AS transfer_bds
FROM {tept_catalog}.bronze.quarter_production
WHERE fiscal_year_no = (
    SELECT DISTINCT fiscal_year_no
    FROM {tept_catalog}.bronze.stnd_quarter_bi_week
    WHERE quarter_bi_week_end_dt >= CURRENT_DATE
)
""")
qtr_df = qtr_df.orderBy("employee_no", "fiscal_year_no", "quarter_no", ascending=True)

# COMMAND ----------

from pyspark.sql.functions import count

grouped_df = joined_wf_right_df.groupBy(
    "employee_no",
    "employee_nm",
    "brs_user_id",
    "fiscal_year_no",
    "quarter_no",
    "current_organization_cd"
).agg(
    count("*").alias("row_count")
)

#display(grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, when

joined_wf_qtr_df = grouped_df.join(
    qtr_df,
    (
        (grouped_df["employee_no"] == qtr_df["employee_no"]) &
        (grouped_df["fiscal_year_no"] == qtr_df["fiscal_year_no"]) &
        (grouped_df["quarter_no"] == qtr_df["quarter_no"])
    ),
    "right"
).select(
    qtr_df["employee_no"],
    qtr_df["fiscal_year_no"],
    qtr_df["quarter_no"],
    qtr_df["fk_gs_level_cd"],
    qtr_df["base_c_bds"],
    qtr_df["base_fs_bds"],
    qtr_df["base_m_bds"],
    qtr_df["base_o_bds"],
    qtr_df["transfer_bds"],
    grouped_df["row_count"],
    grouped_df["employee_nm"],
    grouped_df["brs_user_id"],
    grouped_df["current_organization_cd"]
)
window_spec = Window.partitionBy("employee_nm", "brs_user_id").orderBy("quarter_no")

transfer_bd = joined_wf_qtr_df.withColumn(
    "bds_from_last_qtr",
    when(col("quarter_no").isin(1, 4), 0)
    .otherwise(lag(col("transfer_bds")).over(window_spec))
).withColumn(
    "new_qtr", when(col("quarter_no") == 4, 1).otherwise(col("quarter_no") + 1)
).withColumn(
    "quarter_no_nae", col("new_qtr")
)

# COMMAND ----------

# MAGIC %md
# MAGIC QTR

# COMMAND ----------

wf_qtr =  spark.sql(f"""select distinct employee_no,
	qf.fiscal_year_no,
	qf.quarter_no,
	rating_value_no as workflow_qtr_goal 
from {tept_catalog}.bronze.quarter_workflow qf
	inner join {tept_catalog}.bronze.stnd_gs_level_workflow sf on qf.fk_gs_level_workflow_id = sf.gs_level_workflow_id and qf.fiscal_year_no = sf.fiscal_year_no 
where qf.fiscal_year_no = 
	(
	select distinct fiscal_year_no 
	from {tept_catalog}.bronze.stnd_quarter_bi_week
	where quarter_bi_week_end_dt >= current_date()
	)""")

# COMMAND ----------

joined_transfer_wf_qtr = transfer_bd.join(
    wf_qtr,
    [
        transfer_bd["employee_no"] == wf_qtr["employee_no"],
        transfer_bd["fiscal_year_no"] == wf_qtr["fiscal_year_no"],
        transfer_bd["quarter_no"] == wf_qtr["quarter_no"]
    ],
    "left"
).select(
    transfer_bd["employee_no"],
    transfer_bd["employee_nm"],
    transfer_bd["current_organization_cd"],
    transfer_bd["brs_user_id"],
    transfer_bd["bds_from_last_qtr"],
    transfer_bd["base_o_bds"],
    transfer_bd["base_m_bds"],
    transfer_bd["base_fs_bds"],
    transfer_bd["base_c_bds"],
    transfer_bd["fiscal_year_no"],
    transfer_bd["fk_gs_level_cd"],
    transfer_bd["quarter_no"],
    transfer_bd["transfer_bds"],
    wf_qtr["workflow_qtr_goal"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC FY

# COMMAND ----------

emp_fy = spark.sql(f"""
select emp_fy.employee_no,
	emp_fy.fiscal_year_no,
	emp_fy.fk_start_gs_grade_level_cd,
	emp_fy.promotion_dt,
	fy_prod.allocated_weight_pt AS prod_alloc_wgt,
	fyq.allocated_weight_pt AS qual_alloc_wgt,
	fywf.allocated_weight_pt AS wf_alloc_wgt,
	fy_org.allocated_weight_pt AS org_alloc_wgt,
	fy_org.effectiveness_pt AS org_effectiveness_pt,
	fy_org.fk_effectiveness_rating_cd AS org_effectiveness_rt,
	fy_org.fk_mentor_quality_rating_cd AS org_mentor_qual_rt,
	fy_org.fk_mentor_rating_cd AS org_mentor_rt,
	fy_org.fk_mentor_timeliness_rating_cd AS org_mentor_timely,
	fy_org.fk_training_rating_cd AS org_train_rt,
	fy_org.training_pt AS org_train_pt,
	fy_org.mentoring_pt AS org_mentor_pt,
	fy_prod.weighted_average_in,
	fy_prod.weight_0_fully_successful_in 
from {tept_catalog}.bronze.employee_fiscal_year emp_fy 
	inner join {tept_catalog}.bronze.fiscal_year_production fy_prod on emp_fy.employee_no = fy_prod.employee_no and emp_fy.fiscal_year_no = fy_prod.fiscal_year_no 
	inner join {tept_catalog}.bronze.fiscal_year_quality fyq on emp_fy.employee_no = fyq.employee_no and emp_fy.fiscal_year_no = fyq.fiscal_year_no
	inner join {tept_catalog}.bronze.fiscal_year_workflow fywf on emp_fy.employee_no = fywf.employee_no and emp_fy.fiscal_year_no = fywf.fiscal_year_no
	inner join {tept_catalog}.bronze.fiscal_year_organization fy_org on emp_fy.employee_no = fy_org.employee_no and emp_fy.fiscal_year_no = fy_org.fiscal_year_no 
where emp_fy.fiscal_year_no = 
	(
	select distinct fiscal_year_no 
	from {tept_catalog}.bronze.stnd_quarter_bi_week 
	where quarter_bi_week_end_dt >= current_date()
	)""")

# COMMAND ----------

emp_fy = emp_fy.withColumn(
    "org_mentor_score",
    when(col("org_mentor_rt").isNull(), "---")
    .when(col("org_mentor_rt").cast("double") >= 4.5, "Outstanding")
    .when(col("org_mentor_rt").cast("double") >= 3.5, "Commendable")
    .when(col("org_mentor_rt").cast("double") >= 2.5, "Fully Successful")
    .when(col("org_mentor_rt").cast("double") >= 1.5, "Commendable")
    .otherwise("Unacceptable")
).withColumn(
    "org_trn_score",
    when(col("org_train_rt").isNull(), "---")
    .when(col("org_train_rt").cast("double") >= 4.5, "Outstanding")
    .when(col("org_train_rt").cast("double") >= 3.5, "Commendable")
    .when(col("org_train_rt").cast("double") >= 2.5, "Fully Successful")
    .when(col("org_train_rt").cast("double") >= 1.5, "Commendable")
    .otherwise("Unacceptable")
).withColumn(
    "org_eff_score",
    when(col("org_effectiveness_rt").isNull(), "---")
    .when(col("org_effectiveness_rt").cast("double") >= 4.5, "Outstanding")
    .when(col("org_effectiveness_rt").cast("double") >= 3.5, "Commendable")
    .when(col("org_effectiveness_rt").cast("double") >= 2.5, "Fully Successful")
    .when(col("org_effectiveness_rt").cast("double") >= 1.5, "Commendable")
    .otherwise("Unacceptable")
).withColumn(
    "prod_alloc_wgt_int", round(col("prod_alloc_wgt"), 1)
).withColumn(
    "qual_alloc_wgt_int", round(col("qual_alloc_wgt"), 1)
).withColumn(
    "wf_alloc_wgt_int", round(col("wf_alloc_wgt"), 1)
).withColumn(
    "org_alloc_wgt_int", round(col("org_alloc_wgt"), 1)
).withColumn(
    "org_eff_pt_int", round(col("org_effectiveness_pt"), 1)
).withColumn(
    "org_train_pt_int", round(col("org_train_pt"), 1)
).withColumn(
    "org_mentor_pt_int", round(col("org_mentor_pt"), 1)
)

# COMMAND ----------

from pyspark.sql.functions import first

fiscal_year_first = emp_df.groupBy(
    "employee_no",
    "employee_nm",
    "brs_user_id",
    "current_organization_cd"
).agg(
    first("fiscal_year_no").alias("first_fiscal_year_no")
)

#display(fiscal_year_first)

# COMMAND ----------

joined_emp_fy = emp_fy.join(
    fiscal_year_first,
    [
        emp_fy["employee_no"] == fiscal_year_first["employee_no"],
        emp_fy["fiscal_year_no"] == fiscal_year_first["first_fiscal_year_no"]
    ],
    "inner"
).select(
    emp_fy["employee_no"],
    emp_fy["fiscal_year_no"],
    emp_fy["fk_start_gs_grade_level_cd"],
    emp_fy["promotion_dt"],
    emp_fy["org_effectiveness_rt"],
    emp_fy["org_effectiveness_pt"],
    emp_fy["org_mentor_qual_rt"],
    emp_fy["org_mentor_rt"],
    emp_fy["org_mentor_timely"],
    emp_fy["org_train_rt"],
    emp_fy["org_train_pt"],
    emp_fy["prod_alloc_wgt"],
    emp_fy["weighted_average_in"],
    emp_fy["weight_0_fully_successful_in"],
    emp_fy["org_mentor_score"],
    emp_fy["org_trn_score"],
    emp_fy["org_eff_score"],
    emp_fy["prod_alloc_wgt_int"],
    emp_fy["qual_alloc_wgt_int"],
    emp_fy["wf_alloc_wgt_int"],
    emp_fy["org_alloc_wgt_int"],
    emp_fy["org_eff_pt_int"],
    emp_fy["org_mentor_pt"],
    emp_fy["org_train_pt_int"],
    emp_fy["org_mentor_pt_int"],
    fiscal_year_first["employee_nm"].alias("right_employee_nm"),
    fiscal_year_first["brs_user_id"],
    fiscal_year_first["current_organization_cd"]
)

# COMMAND ----------

perf_rpt = spark.sql(f"""
                     select employee_no,
	fiscal_year_no,
	Concat(first_nm, ' ', last_nm) as employee_nm,
	q1_schedule_hour_qt,
	q2_schedule_hour_qt,
	q3_schedule_hour_qt,
	q4_schedule_hour_qt,
	avg_score_rt,
	examiner_amendment_usage_pt,
	workflow_performance_rating_cd,
	no_sig_trainee_biweeks,
	partial_sig_trainee_biweeks,
	pfs_trainee_biweeks,
	current_gs_grade_level_cd,
	q1_performance_rating_cd,
	q2_performance_rating_cd,
  q3_performance_rating_cd,
	q4_performance_rating_cd 
from {tept_catalog}.bronze.rpt_performance_data_summary 
where fiscal_year_no = 
	(
	select distinct fiscal_year_no 
	from {tept_catalog}.bronze.stnd_quarter_bi_week
	where quarter_bi_week_end_dt >= current_date()
	)""")

# COMMAND ----------

joined_perf = joined_emp_fy.join(
    perf_rpt,
    [
        joined_emp_fy["employee_no"] == perf_rpt["employee_no"],
        joined_emp_fy["fiscal_year_no"] == perf_rpt["fiscal_year_no"]
    ],
    "left"
)

perf_rpt_cols_to_exclude = {"employee_no", "fiscal_year_no", "employee_nm"}
perf_rpt_cols = [col for col in perf_rpt.columns if col not in perf_rpt_cols_to_exclude]

selected_cols = [joined_emp_fy[c] for c in joined_emp_fy.columns] + [perf_rpt[c] for c in perf_rpt_cols]

final_joined_df = joined_perf.select(*selected_cols)

#display(final_joined_df)

# COMMAND ----------

qt_cols = [
    "countnonnull_write_grade_qt",
    "suff_rt",
    "count_write_grade_qt",
    "count_serial_num_tx",
    "Count_write_grade_qt_is_1",
    "suff_cnt",
    "suff_score",
    "avg_write_rt",
    "avg_write_score",
    "write_def%"
]

joined_qt_perf = final_joined_df.join(
    qt_final,
    [
        final_joined_df["employee_no"] == qt_final["employee_no"]
    ],
    "left"
)

selected_qt_cols = [qt_final[c] for c in qt_cols]
selected_perf_cols = [final_joined_df[c] for c in final_joined_df.columns]

final_qt_perf_df = joined_qt_perf.select(*selected_perf_cols, *selected_qt_cols)

#display(final_qt_perf_df)

# COMMAND ----------

from pyspark.sql.functions import col, when

string_fields = [
    "employee_no", "fiscal_year_no", "fk_start_gs_grade_level_cd", "promotion_dt",
    "org_effectiveness_rt", "org_mentor_qual_rt", "org_mentor_rt", "org_mentor_timely",
    "org_train_rt", "employee_nm", "workflow_performance_rating_cd",
    "q1_performace_rating_cd", "q2_performace_rating_cd", "q3_performace_rating_cd", "q4_performace_rating_cd"
]

numeric_fields = [
    "prod_alloc_wgt", "qual_alloc_wgt", "wf_alloc_wgt", "org_alloc_wgt",
    "org_effectiveness_pt", "org_train_pt", "org_mentor_pt",
    "q1_schedule_hour_qt", "q2_schedule_hour_qt", "q3_schedule_hour_qt", "q4_schedule_hour_qt",
    "avg_score_rt", "examiner_amendment_usage_pt", "no_sig_trainee_biweeks",
    "partial_sig_trainee_biweeks", "pfs_tarinee_biweeks"
]

fy_df = final_qt_perf_df
for field in string_fields:
    if field in fy_df.columns:
        fy_df = fy_df.withColumn(field, when(col(field).isNull(), "").otherwise(col(field)))
for field in numeric_fields:
    if field in fy_df.columns:
        fy_df = fy_df.withColumn(field, when(col(field).isNull(), 0).otherwise(col(field)))
fy_df = fy_df.withColumnRenamed("right_employee_nm", "employee_nm")
#display(fy_df)

# COMMAND ----------

# DBTITLE 1,Overwrite FY data into Table
from pyspark.sql.functions import lit

selected_columns = [
    "countnonnull_write_grade_qt",
    "count_write_grade_qt",
    "count_serial_num_tx",
    "count_write_grade_qt_is_1",
    "suff_rt",
    "suff_score",
    "avg_write_rt",
    "avg_write_score",
    "write_def%",
    "employee_no",
    "fiscal_year_no",
    "fk_start_gs_grade_level_cd",
    "promotion_dt",
    "org_effectiveness_rt",
    "org_mentor_qual_rt",
    "org_mentor_rt",
    "org_mentor_timely",
    "org_train_rt",
    "weighted_average_in",
    "weight_0_fully_successful_in",
    "org_mentor_score",
    "org_trn_score",
    "org_eff_score",
    "prod_alloc_wgt_int",
    "qual_alloc_wgt_int",
    "wf_alloc_wgt_int",
    "org_alloc_wgt_int",
    "org_effectiveness_pt",
    "org_train_pt",
    "org_mentor_pt",
    "employee_nm",
    "brs_user_id",
    "current_organization_cd",
    "avg_score_rt",
    "examiner_amendment_usage_pt",
    "workflow_performance_rating_cd",
    "no_sig_trainee_biweeks",
    "partial_sig_trainee_biweeks",
    "pfs_trainee_biweeks",
    "current_gs_grade_level_cd"
]

fy_selected_df = fy_df.select(*[col for col in selected_columns]).withColumn("table", lit("FY"))
fy_selected_df.write.mode("overwrite").insertInto(f"{trgt_catalog}.silver.prod_simulator_fy")

# COMMAND ----------

selected_fields = [
    "employee_no",
    "q1_schedule_hour_qt",
    "q2_schedule_hour_qt",
    "q3_schedule_hour_qt",
    "q4_schedule_hour_qt",
    "q1_performance_rating_cd",
    "q2_performance_rating_cd",
    "q3_performance_rating_cd",
    "q4_performance_rating_cd"
]

fy_output4 = fy_df.select(*selected_fields)

# COMMAND ----------

from pyspark.sql.functions import lit

fy_output4_qtr1 = fy_output4.withColumn("quarter_no", lit(1)) \
    .withColumn("schedule_hour_qt", col("q1_schedule_hour_qt")) \
    .withColumn("performance_rating_cd", col("q1_performance_rating_cd")) \
    .withColumn("next_qtr_perf_rate_cd", col("q2_performance_rating_cd"))

fy_output4_qtr2 = fy_output4.withColumn("quarter_no", lit(2)) \
    .withColumn("schedule_hour_qt", col("q2_schedule_hour_qt")) \
    .withColumn("performance_rating_cd", col("q2_performance_rating_cd")) \
    .withColumn("next_qtr_perf_rate_cd", col("q3_performance_rating_cd"))

fy_output4_qtr3 = fy_output4.withColumn("quarter_no", lit(3)) \
    .withColumn("schedule_hour_qt", col("q3_schedule_hour_qt")) \
    .withColumn("performance_rating_cd", col("q3_performance_rating_cd")) \
    .withColumn("next_qtr_perf_rate_cd", col("q4_performance_rating_cd"))

fy_output4_qtr4 = fy_output4.withColumn("quarter_no", lit(4)) \
    .withColumn("schedule_hour_qt", col("q4_schedule_hour_qt")) \
    .withColumn("performance_rating_cd", col("q4_performance_rating_cd")) \
    .withColumn("next_qtr_perf_rate_cd", lit(""))

selected_expanded = fy_output4_qtr1.unionByName(fy_output4_qtr2) \
    .unionByName(fy_output4_qtr3) \
    .unionByName(fy_output4_qtr4) \
    .select(
        "employee_no",
        "quarter_no",
        "schedule_hour_qt",
        "performance_rating_cd",
        "next_qtr_perf_rate_cd"
    )

#display(fy_output4_expanded)

# COMMAND ----------

joined_final = joined_transfer_wf_qtr.join(
    selected_expanded,
    [
        joined_transfer_wf_qtr["employee_no"] == selected_expanded["employee_no"],
        joined_transfer_wf_qtr["quarter_no"] == selected_expanded["quarter_no"]
    ],
    "inner"
).select(
    joined_transfer_wf_qtr["employee_no"],
    joined_transfer_wf_qtr["fiscal_year_no"],
    joined_transfer_wf_qtr["quarter_no"],
    joined_transfer_wf_qtr["fk_gs_level_cd"],
    joined_transfer_wf_qtr["base_c_bds"],
    joined_transfer_wf_qtr["base_fs_bds"],
    joined_transfer_wf_qtr["base_m_bds"],
    joined_transfer_wf_qtr["base_o_bds"],
    joined_transfer_wf_qtr["transfer_bds"],
    joined_transfer_wf_qtr["employee_nm"],
    joined_transfer_wf_qtr["brs_user_id"],
    joined_transfer_wf_qtr["current_organization_cd"],
    joined_transfer_wf_qtr["bds_from_last_qtr"],
    joined_transfer_wf_qtr["workflow_qtr_goal"],
    selected_expanded["schedule_hour_qt"],
    selected_expanded["performance_rating_cd"],
    selected_expanded["next_qtr_perf_rate_cd"]
)

#display(joined_final)

# COMMAND ----------

# DBTITLE 1,Overwrite QTR data

from pyspark.sql.functions import lit

cleaned_joined_final = joined_final \
    .withColumn("employee_no", trim(regexp_replace(when(col("employee_no").isNull(), "").otherwise(col("employee_no")), r"[^A-Za-z0-9]", ""))) \
    .withColumn("fiscal_year_no", trim(regexp_replace(when(col("fiscal_year_no").isNull(), "").otherwise(col("fiscal_year_no")), r"[^A-Za-z0-9]", ""))) \
    .withColumn("quarter_no", trim(regexp_replace(when(col("quarter_no").isNull(), "").otherwise(col("quarter_no")), r"[^A-Za-z0-9]", ""))) \
    .withColumn("transfer_balanced_disposal_qt", trim(regexp_replace(when(col("transfer_bds").isNull(), "0").otherwise(col("transfer_bds")), r"[^0-9.]", ""))) \
    .withColumn("bds_from_last_qtr", trim(regexp_replace(when(col("bds_from_last_qtr").isNull(), "0").otherwise(col("bds_from_last_qtr")), r"[^0-9.]", ""))) \
    .withColumn("schedule_hour_qt", trim(regexp_replace(when(col("schedule_hour_qt").isNull(), "0").otherwise(col("schedule_hour_qt")), r"[^0-9.]", ""))) \
    .withColumn("table", lit("qtr")) \
    .drop("transfer_bds")
#cleaned_joined_final.createOrReplaceTempView("cleaned_joined_final")
cleaned_joined_final.write.mode("overwrite").insertInto(f"{trgt_catalog}.silver.prod_simulator_qtr")

# COMMAND ----------

end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'completed',0,"job completed successfully")
dbutils.notebook.exit(f"Completed loading Production Simulator QUAL, BIWEEK, QTR, FY Data")
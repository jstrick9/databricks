# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Setting environment
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
ttab_scope = common_configs['secrets']['ttab_scope']

print(reporting_catalog,run_env,ttab_scope)

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,For testing purpose
# MAGIC %run ./ntb_ttab_detail_tbl_trm_dbx_input

# COMMAND ----------

# MAGIC %md
# MAGIC # CONCURRENT FILINGS

# COMMAND ----------

ph_filter859_T = input_ph.filter(col("five_Characters") == "CU.IT").dropDuplicates(
    ["serial_number", "ph_action_date", "cm_prcd_num"]
)
ph_filter859_F = (
    input_ph.filter(col("five_Characters").isin("CU.AT", "CU.DT", "CU.GT", "CU.MT", "CU.TT"))
    .dropDuplicates(
        ["serial_number", "ph_action_date", "cm_prcd_num", "five_Characters"]
    )
)

ph_join862_Left = (
    (
        ph_filter859_T.join(
            ph_filter859_F.select(
            col("serial_number").alias("Right_serial_number"),
            col("ph_action_number").alias("Right_ph_action_number"),
            col("ph_action_code").alias("Right_ph_action_code"),
            col("cm_sys_dt").alias("Right_cm_sys_dt"),
            col("ph_action_date").alias("Right_ph_action_date"),
            col("last_modified_date").alias("Right_last_modified_date"),
            col("oracle_apply_time").alias("Right_oracle_apply_time"),
            col("cm_prcd_num").alias("Right_cm_prcd_num"),
            col("ri_notif_dt").alias("Right_ri_notif_dt"),
            col("cm_desc").alias("Right_cm_desc"),
            col("fifth_char_cm_type").alias("Right_fifth_char_cm_type"),
            col("cm_flg_paper").alias("Right_cm_flg_paper"),
            col("ttab_tracking_num").alias("Right_ttab_tracking_num"),
            col("tm_worker_eid").alias("Right_tm_worker_eid"),
            col("five_Characters").alias("Right_five_Characters"),
            col("year").alias("Right_year"),
    ),
            on=[col("serial_number") == col("Right_serial_number")],
            how="left",
        )
    )
    .withColumn("TTAB_ISSUE_TYPE", lit("CONCURRENT"))
    .select(
        col("SERIAL_NUMBER"),
        col("TTAB_ISSUE_TYPE"),
        col("five_Characters").alias("INSTITUTED_CODE"),
        col("PH_ACTION_DATE").alias("INSTITUTED_DATE"),
        col("CM_PRCD_NUM").alias("INSTITUTED_PRCD_NUM"),
        col("Right_PH_ACTION_DATE").alias("DECISION_DATE"),
        col("Right_five_Characters").alias("DECISION_CODE"),
        col("Right_CM_PRCD_NUM").alias("DECISION_PRCD_NUM"),
        col("Right_CM_Desc").alias("DECISION_DESCRIPTION"),
    )
)

ph_join862_R = ph_filter859_F.join(
    ph_filter859_T, "serial_number", how="anti"
)

# COMMAND ----------

ph_join909 = (
    ph_join862_R.join(
        ip_df_select921,
        on=[col("serial_number") == col("REF_SERIAL_NUMBER")],
        how="inner",
    )
    .filter(col("FILING_DATE") <= col("PH_ACTION_DATE"))
    .withColumn("TTAB_ISSUE_TYPE", lit("CONCURRENT"))
    .withColumn("INSTITUTED_DATE", col("FILING_DATE"))
    .withColumn("INSTITUTED_CODE", lit("CU.IT"))
    .withColumn("INSTITUTED_PRCD_NUM", col("CM_PRCD_NUM"))
    .select(
        col("SERIAL_NUMBER"),
        col("TTAB_ISSUE_TYPE"),
        col("INSTITUTED_DATE"),
        col("INSTITUTED_CODE"),
        col("INSTITUTED_PRCD_NUM"),
        col("PH_ACTION_DATE").alias("DECISION_DATE"),
        col("CM_PRCD_NUM").alias("DECISION_PRCD_NUM"),
        col("CM_Desc").alias("DECISION_DESCRIPTION"),
        col("five_Characters").alias("DECISION_CODE"),
    )
)

ph_union915 = ph_join862_Left.unionByName(ph_join909, allowMissingColumns=True)

# COMMAND ----------

ph_filter864 = (
    ph_union915.filter(col("INSTITUTED_CODE") == "CU.IT")
    .select(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "INSTITUTED_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_PRCD_NUM",
    ).distinct()
)

ph_filter867 = (
    ph_union915.filter(col("DECISION_CODE") != "CU.TT")
    .withColumnRenamed("SERIAL_NUMBER", "Right_SERIAL_NUMBER")
    .withColumnRenamed("INSTITUTED_DATE", "Right_INSTITUTED_DATE")
    .drop("INSTITUTED_CODE")
    .drop("INSTITUTED_PRCD_NUM")
    .drop("TTAB_ISSUE_TYPE")
)

ph_filter861 = (
    ph_union915.filter(col("DECISION_CODE") == "CU.TT")
    .withColumnRenamed("DECISION_DATE", "TERMINATION_DATE")
    .withColumnRenamed("DECISION_CODE", "TERMINATION_CODE")
    .withColumnRenamed("DECISION_PRCD_NUM", "TERMINATION_PRCD_NUM")
    .select(
        "SERIAL_NUMBER",
        "INSTITUTED_DATE",
        "TERMINATION_DATE",
        "TERMINATION_CODE",
        "TERMINATION_PRCD_NUM",
    ).distinct()
    .withColumnRenamed("SERIAL_NUMBER", "Right_SERIAL_NUMBER")
    .withColumnRenamed("INSTITUTED_DATE", "Right_INSTITUTED_DATE")
)

# COMMAND ----------

ph_join876_1 = (
    ph_filter864.join(
        ph_filter867,
        on=[
            col("serial_number") == col("Right_SERIAL_NUMBER"),
            col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
            col("INSTITUTED_PRCD_NUM") == col("DECISION_PRCD_NUM"),
        ],
        how="outer",
    )
    .drop("Right_SERIAL_NUMBER")
    .drop("Right_INSTITUTED_DATE")
)

ph_join876 = (
    ph_join876_1.join(
        ph_filter861,
        on=[
            col("serial_number") == col("Right_SERIAL_NUMBER"),
            col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
            col("INSTITUTED_PRCD_NUM") == col("TERMINATION_PRCD_NUM"),
        ],
        how="outer",
    )
    .drop("Right_SERIAL_NUMBER")
    .drop("Right_INSTITUTED_DATE")
    .filter(col("serial_number").isNotNull())
    .withColumn("TERM_DATES", lit(None))
    .withColumn("TERM_DATE", col("TERMINATION_DATE"))
)

# COMMAND ----------

ph_cross878 = (
    ph_join876.groupBy(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "INSTITUTED_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_PRCD_NUM",
        "DECISION_DATE",
        "DECISION_CODE",
        "DECISION_PRCD_NUM",
        "DECISION_DESCRIPTION",
        "TERMINATION_CODE",
        "TERMINATION_PRCD_NUM",
    )
    .pivot("TERM_DATES")
    .agg(concat_ws(",", collect_list(col("TERM_DATE"))))
)

# COMMAND ----------

ph_cross878 = ph_cross878.withColumn(
    "null", array_join(array_sort(split(col("null"),',')), ',')
)

# COMMAND ----------

ph_col881 = (
    ph_cross878.withColumn("TERMINATION_DATE", split(col("null"), ",").getItem(0))
    .withColumn("TERMINATION_DATE_2", split(col("null"), ",").getItem(1))
    .withColumn("TERMINATION_DATE_3", split(col("null"), ",").getItem(2))
    .withColumn("TERMINATION_DATE_4", split(col("null"), ",").getItem(3))
    .withColumn("TERMINATION_DATE_5", split(col("null"), ",").getItem(4))
    .drop("null")
    .withColumn(
        "INSTITUTED_PRCD_NUM",
        when(col("INSTITUTED_PRCD_NUM") == "", col("DECISION_PRCD_NUM")).otherwise(
            col("INSTITUTED_PRCD_NUM")
        ),
    )
)

ph_col881 = ph_col881.withColumn("TERMINATION_DATE", when(col("TERMINATION_DATE") == "", lit(None)).otherwise(col("TERMINATION_DATE")))

# COMMAND ----------

ph_join884_L = ph_col881.join(
    ip_df_select921, on=[col("serial_number") == col("REF_SERIAL_NUMBER")], how="anti"
).drop("REF_SERIAL_NUMBER")

ph_join884_I = ph_col881.join(
    ip_df_select921, on=[col("serial_number") == col("REF_SERIAL_NUMBER")], how="inner"
).drop("REF_SERIAL_NUMBER")

ph_sumrz885 = (
    ph_join884_I.select(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "FILING_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_DATE",
        "DECISION_DATE",
        "DECISION_CODE",
        "DECISION_DESCRIPTION",
        "TERMINATION_DATE",
        "TERMINATION_CODE",
        "DECISION_PRCD_NUM",
        "TERMINATION_PRCD_NUM",
        "INSTITUTED_PRCD_NUM",
        "TERMINATION_DATE_2",
        "TERMINATION_DATE_3",
        "TERMINATION_DATE_4",
        "TERMINATION_DATE_5",
    ).distinct()
)

ph_union886 = ph_join884_L.unionByName(ph_sumrz885, allowMissingColumns=True)

# COMMAND ----------

ph_frm890 = ph_union886.withColumn(
    "FILING_DT_VALID",
    when(
        (col("FILING_DATE") <= col("INSTITUTED_DATE")) | (col("FILING_DATE").isNull()),
        lit(True),
    ).otherwise(lit(False)),
).filter(col("FILING_DT_VALID") == lit(True))

ph_filter893_T = ph_frm890.filter(col("FILING_DATE").isNotNull())

win883 = Window().partitionBy("serial_number", "FILING_DATE", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM").orderBy("DECISION_DATE", "TERMINATION_DATE")

ph_sample883 = ph_filter893_T.withColumn("rn", row_number().over(win883)).filter(col("rn") == 1).drop("rn")

ph_filter893_F = ph_frm890.filter(col("FILING_DATE").isNull())

ph_union894 = (
    ph_sample883.unionByName(ph_filter893_F)
    .withColumn(
        "FILING_DATE",
        when(((col("FILING_DATE") == "") | col("FILING_DATE").isNull()), col("INSTITUTED_DATE")).otherwise(
            col("FILING_DATE")
        ),
    )
)

##########################################################################################
# THIS IS WHERE WE START TO GET MISMATCHES WITH ALTERYX BECAUSE THE DROP DUPES IS RANDOM #
##########################################################################################
# ADDED SORT ON INSTITUTED_DATE, DECISION_CODE, DECISION_DATE - FOLLOW UP QUESTION ASC OR DESC? #
###################################################################

win895 = Window().partitionBy("serial_number", "INSTITUTED_PRCD_NUM").orderBy(col("INSTITUTED_DATE").desc(), "DECISION_CODE", col("DECISION_DATE").desc())

ph_unique895 = ph_union894.withColumn("rn", row_number().over(win895)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------

win939 = Window().partitionBy("serial_number", "TTAB_ISSUE_TYPE", "INSTITUTED_DATE", 
                              "DECISION_DATE", "TERMINATION_DATE", "INSTITUTED_PRCD_NUM").orderBy("serial_number")

ph_sample939 = ph_unique895.withColumn("rn", row_number().over(win939)).filter(col("rn") == 1).drop("rn")

win941 = Window().partitionBy("serial_number", "DECISION_DATE", "INSTITUTED_PRCD_NUM").orderBy(col("INSTITUTED_DATE").desc())

ph_sample941 = ph_sample939.withColumn("rn", row_number().over(win941)).filter(col("rn") == 1).drop("rn")

ph_sample941 = ph_sample941.withColumn(
        "INSTITUTED_PRCD_NUM",
        when(((col("INSTITUTED_PRCD_NUM") == "") | col("INSTITUTED_PRCD_NUM").isNull()), col("DECISION_PRCD_NUM")).otherwise(
            col("INSTITUTED_PRCD_NUM")
        ),
    )

# COMMAND ----------

ph_sort900 = (
    ph_sample941
    .withColumnRenamed("INSTITUTED_PRCD_NUM", "PROCEEDING_NUM")
    .drop("FILING_DT_VALID")
    .drop("DECISION_PRCD_NUM")
    .drop("TERMINATION_PRCD_NUM")
    .withColumn(
        "FILED_YR",
        when(month(col("FILING_DATE")) > 9, (year(col("FILING_DATE")) + 1)).otherwise(
            year(col("FILING_DATE"))
        ),
    )
    .withColumn(
        "INST_YR",
        when(
            month(col("INSTITUTED_DATE")) > 9, (year(col("INSTITUTED_DATE")) + 1)
        ).otherwise(year(col("INSTITUTED_DATE"))),
    )
    .withColumn(
        "TERM_YR",
        when(
            month(col("TERMINATION_DATE")) > 9, (year(col("TERMINATION_DATE")) + 1)
        ).otherwise(year(col("TERMINATION_DATE"))),
    )
    .withColumn(
        "DECISION_YR",
        when(
            month(col("DECISION_DATE")) > 9, (year(col("DECISION_DATE")) + 1)
        ).otherwise(year(col("DECISION_DATE"))),
    )
)

# COMMAND ----------

# bug fix: only apply termination code and date changes to true side, replace filter and union with when

ph_frm931 = ph_sort900.withColumn(
    "TERMINATION_CODE", when((col("DECISION_DATE").isNotNull()
        & col("TERMINATION_DATE").isNull()
        & (datediff(current_date(), col("DECISION_DATE")) > 63)), lit("CU.TT")).otherwise(col("TERMINATION_CODE"))
).withColumn(
    "TERMINATION_DATE", when((col("DECISION_DATE").isNotNull()
        & col("TERMINATION_DATE").isNull()
        & (datediff(current_date(), col("DECISION_DATE")) > 63)), date_add(col("DECISION_DATE"), 63)).otherwise(col("TERMINATION_DATE")) 
)

frm934 = (ph_frm931.withColumn("PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("CONCURRENT", lit(True))
    .withColumn(
        "INVENTORY",
        when(
            (col("INSTITUTED_DATE").isNotNull())
            & ((col("DECISION_DATE") == "") | (col("DECISION_DATE").isNull()))
            & ((col("TERMINATION_DATE") == "") | (col("TERMINATION_DATE").isNull())),
            lit(True),
        ).otherwise(False),
    )
)

# COMMAND ----------

# set column ordering
frm934 = frm934.select(
    'SERIAL_NUMBER',
    'TTAB_ISSUE_TYPE',
    'INSTITUTED_DATE',
    'INSTITUTED_CODE',
    'PROCEEDING_NUM',
    'DECISION_DATE',
    'DECISION_CODE',
    'DECISION_DESCRIPTION',
    'TERMINATION_CODE',
    'TERMINATION_DATE',
    'TERMINATION_DATE_2',
    'TERMINATION_DATE_3',
    'TERMINATION_DATE_4',
    'TERMINATION_DATE_5',
    'FILING_DATE',
    'FILED_YR',
    'INST_YR',
    'TERM_YR',
    'DECISION_YR',
    'PENDENCY_D',
    'PENDENCY_T',
    'CONCURRENT',
    'INVENTORY'
)

# COMMAND ----------

frm934.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.ttab_detail_concurrent_filings")

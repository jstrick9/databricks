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
# MAGIC # GET EX PARTE APPEAL EVENTS: CLEANSE, ORGANIZE AND REFINE

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET FINAL REFUSAL DATE

# COMMAND ----------

sel_list = ["CNFR", "CNCF", "GNCF", "GNFN", "GNFR"]
ph_sumrz199 = ph_frm166.select(col("serial_number"),
                 col("ph_action_number"),
                 col("ph_action_code"),
                 col("ph_action_date"),
                 col("cm_desc"),
                 col("five_Characters"),
                 col("year")).filter(col("ph_action_code").isin(sel_list)) \
                     .groupBy("serial_number").agg(min("ph_action_date").alias("Min_PH_ACTION_DATE"))

ph_frm730 = ph_sumrz199.withColumn("FINAL_REFUSAL_DATE",col("Min_PH_ACTION_DATE").cast(DateType())).drop("Min_PH_ACTION_DATE") \
    .withColumn("REFUSAL",lit(True))

#-------------- END ----GET FINAL REFUSAL DATE----------

# COMMAND ----------

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_frm730")
# ph_frm730 = ph_frm730.checkpoint(True)

# COMMAND ----------

# ph_frm730.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Continued 1 (GET EX PARTE APPEAL EVENTS: CLEANSE, ORGANIZE AND REFINE)

# COMMAND ----------

sel_list = ["EXART" ,"EXDAT" ,"EXDMT" ,"EXDRT" ,"EXFBT" ,"EXNIT" ,"EXPAT" ,"EXPIT" ,"EXPRT" ,"EXPTT" ,"EXRET" ,"EXRRT" ,"TTPDA"]

ph_filter196 = ph_frm166.filter(col("five_Characters").isin(sel_list))

# create two dataframes on 5thCharacter value "EXPIT"

ph_filter170_T = ph_filter196.filter(col("five_Characters") == "EXPIT").dropDuplicates(["serial_number","ph_action_date"])

ph_filter170_F = ph_filter196.filter(col("five_Characters") != "EXPIT").dropDuplicates(["serial_number","ph_action_date","five_Characters"]) \
    .select(col("serial_number").alias("Right_serial_number"),
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
            col("year").alias("Right_year"))

ph_join175 = \
(
    ph_filter170_T
        .join(ph_filter170_F,
             on = [col("serial_number") == col("Right_serial_number")],
             how = "left"
             ) \
                 .withColumn("TTAB_ISSUE_TYPE",lit("EX PARTE APPEAL")) \
                     .select(col("serial_number"),
                             col("TTAB_ISSUE_TYPE"),
                             col("five_Characters").alias("INSTITUTED_CODE"),
                             col("ph_action_date").alias("INSTITUTED_DATE"),
                             col("cm_prcd_num").alias("INSTITUTED_PRCD_NUM"),
                             col("Right_ph_action_date").alias("DECISION_DATE"),
                             col("Right_five_Characters").alias("DECISION_CODE"),
                             col("Right_cm_prcd_num").alias("DECISION_PRCD_NUM"),
                             col("Right_cm_desc").alias("DECISION_DESCRIPTION"))
)   


# COMMAND ----------

# ph_join175.count()

# COMMAND ----------

ph_filter183 = ph_join175.filter(col("INSTITUTED_CODE") == "EXPIT") \
    .select(col("serial_number"),
            col("TTAB_ISSUE_TYPE"),
            col("INSTITUTED_CODE"),
            col("INSTITUTED_DATE"),
            col("INSTITUTED_PRCD_NUM"))
#####################################
# bug fix: code is EXPTT, not EXPIT #
#####################################
ph_filter186 = ph_join175.filter(col("DECISION_CODE") != "EXPTT") \
    .select(col("serial_number").alias("Right_SERIAL_NUMBER"),
            col("TTAB_ISSUE_TYPE").alias("Right_TTAB_ISSUE_TYPE"),
            col("INSTITUTED_CODE").alias("Right_INSTITUTED_CODE "),
            col("INSTITUTED_DATE").alias("Right_INSTITUTED_DATE"),
            col("INSTITUTED_PRCD_NUM").alias("Left_Right_INSTITUTED_PRCD_NUM"),
            col("DECISION_DATE"),
            col("DECISION_CODE"),
            col("DECISION_PRCD_NUM"),
            col("DECISION_DESCRIPTION"))
#####################################
# bug fix: code is EXPTT, not EXPIT #
#####################################
ph_filter179 = ph_join175.filter(col("DECISION_CODE") == "EXPTT") \
    .select(col("serial_number").alias("Right_SERIAL_NUMBER"),
            col("TTAB_ISSUE_TYPE").alias("Right_TTAB_ISSUE_TYPE"),
            col("INSTITUTED_CODE").alias("Right_INSTITUTED_CODE "),
            col("INSTITUTED_DATE").alias("Right_INSTITUTED_DATE"),
            col("INSTITUTED_PRCD_NUM").alias("Right_INSTITUTED_PRCD_NUM"),
            col("DECISION_DATE").alias("TERMINATION_DATE"),
            col("DECISION_CODE").alias("TERMINATION_CODE"),
            col("DECISION_PRCD_NUM").alias("TERMINATION_PRCD_NUM"))

# COMMAND ----------

ph_join181 = ph_filter183.join(
            ph_filter186,
            on=[
                col("serial_number") == col("Right_SERIAL_NUMBER"),
                col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
                col("INSTITUTED_PRCD_NUM") == col("DECISION_PRCD_NUM"),
            ],
            how="left",
        ).select(
            col("serial_number"),
            col("TTAB_ISSUE_TYPE"),
            col("INSTITUTED_CODE"),
            col("INSTITUTED_DATE"),
            col("INSTITUTED_PRCD_NUM"),
            col("Left_Right_INSTITUTED_PRCD_NUM"),
            col("DECISION_DATE"),
            col("DECISION_CODE"),
            col("DECISION_PRCD_NUM"),
            col("DECISION_DESCRIPTION"),
        ).distinct()

###################
# change / bug fix: just use distinct
###################
    # .groupBy(
    #     col("serial_number"),
    #     col("TTAB_ISSUE_TYPE"),
    #     col("INSTITUTED_CODE"),
    #     col("INSTITUTED_DATE"),
    #     col("INSTITUTED_PRCD_NUM"),
    #     col("Left_Right_INSTITUTED_PRCD_NUM"),
    #     col("DECISION_DATE"),
    #     col("DECISION_CODE"),
    #     col("DECISION_PRCD_NUM"),
    #     col("DECISION_DESCRIPTION"),
    # )
    # .count()
    # .drop("count")

ph_join189 = ph_join181.join(
    ph_filter179,
    on=[
        col("serial_number") == col("Right_SERIAL_NUMBER"),
        col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
        col("INSTITUTED_PRCD_NUM") == col("TERMINATION_PRCD_NUM"),
    ],
    how="left",
).select(
    col("serial_number"),
    col("TTAB_ISSUE_TYPE"),
    col("INSTITUTED_CODE"),
    col("INSTITUTED_DATE"),
    col("INSTITUTED_PRCD_NUM"),
    col("Left_Right_INSTITUTED_PRCD_NUM"),
    col("DECISION_DATE"),
    col("DECISION_CODE"),
    col("DECISION_PRCD_NUM"),
    col("DECISION_DESCRIPTION"),
    col("Right_INSTITUTED_PRCD_NUM"),
    col("TERMINATION_DATE"),
    col("TERMINATION_CODE"),
    col("TERMINATION_PRCD_NUM"),
)

# COMMAND ----------

# ph_join189.count()

# COMMAND ----------

ph_sumrz199_1 = ph_sumrz199.withColumnRenamed("serial_number", "right_serial_number")

ph_join200 = (
    ph_join189.join(
        ph_sumrz199_1,
        on=[col("serial_number") == col("right_serial_number")],
        how="left",
    ).select(
        col("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("INSTITUTED_CODE"),
        col("INSTITUTED_DATE"),
        col("INSTITUTED_PRCD_NUM"),
        col("Left_Right_INSTITUTED_PRCD_NUM"),
        col("DECISION_DATE"),
        col("DECISION_CODE"),
        col("DECISION_PRCD_NUM"),
        col("DECISION_DESCRIPTION"),
        col("Right_INSTITUTED_PRCD_NUM"),
        col("TERMINATION_DATE"),
        col("TERMINATION_CODE"),
        col("TERMINATION_PRCD_NUM"),
        col("Min_PH_ACTION_DATE").alias("FINAL_REFUSAL_DATE"),
    )
).withColumn(
    "INSTITUTED_PRCD_NUM",
    when(col("INSTITUTED_PRCD_NUM") == "", col("DECISION_PRCD_NUM")).otherwise(
        col("INSTITUTED_PRCD_NUM")
    ),
)


ph_frm1045 = ph_join200.withColumn(
    "INSTITUTED_PRCD_NUM",
    when(col("INSTITUTED_PRCD_NUM") == "", col("TERMINATION_PRCD_NUM")).otherwise(
        col("INSTITUTED_PRCD_NUM")
    ),
)

# COMMAND ----------

# ph_join200.count()

# COMMAND ----------

# ip_df_sumrz1028.count()

# COMMAND ----------

ph_join217 = ph_frm1045.join(
    ip_df_sumrz1028, on=[col("serial_number") == col("REF_SERIAL_NUMBER")], how="left"
).select(
    col("serial_number"),
    col("TTAB_ISSUE_TYPE"),
    col("INSTITUTED_CODE"),
    col("INSTITUTED_DATE"),
    col("INSTITUTED_PRCD_NUM"),
    col("Left_Right_INSTITUTED_PRCD_NUM"),
    col("DECISION_DATE"),
    col("DECISION_CODE"),
    col("DECISION_PRCD_NUM"),
    col("DECISION_DESCRIPTION"),
    col("Right_INSTITUTED_PRCD_NUM"),
    col("TERMINATION_DATE"),
    col("TERMINATION_CODE"),
    col("TERMINATION_PRCD_NUM"),
    col("FINAL_REFUSAL_DATE"),
    col("FILING_DATE").alias("TTAB_FILED_DATE"),
)

ph_join20 = (
    ph_join217.join(
        fact_fpep_col236,
        on=[
            col("serial_number") == col("ser_num"),
            col("FINAL_REFUSAL_DATE") == col("completed_dt"),
        ],
        how="left",
    )
    .drop("completed_dt")
    .drop("ser_num")
    .drop("Form_Paragraphs_Used")
    .withColumnRenamed(
        "Left_Right_INSTITUTED_PRCD_NUM", "Left_Left_Right_INSTITUTED_PRCD_NUM"
    )
    .withColumnRenamed("TTAB_FILED_DATE", "FILING_DATE")
)

# COMMAND ----------

# ph_join217.count()

# COMMAND ----------

# ph_join20.count()

# COMMAND ----------

ph_frm471 = (
    ph_join20.select(
        col("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("INSTITUTED_PRCD_NUM").alias("PROCEEDING_NUM"),
        col("FINAL_REFUSAL_DATE"),
        col("FILING_DATE"),
        col("INSTITUTED_CODE"),
        col("INSTITUTED_DATE"),
        col("DECISION_DATE"),
        col("DECISION_CODE"),
        col("DECISION_DESCRIPTION"),
        col("TERMINATION_DATE"),
        col("TERMINATION_CODE"),
        col("Form_Paragraphs_1").alias("FP_REASON_1"),
        col("Form_Paragraphs_2").alias("FP_REASON_2"),
        col("Form_Paragraphs_3").alias("FP_REASON_3"),
        col("Form_Paragraphs_4").alias("FP_REASON_4"),
        col("Form_Paragraphs_5").alias("FP_REASON_5")
        # ,
        # col("Form_Paragraphs_6").alias("FP_REASON_6"),
        # col("Form_Paragraphs_7").alias("FP_REASON_7"),
        # col("Form_Paragraphs_8").alias("FP_REASON_8"),
        # col("Form_Paragraphs_9").alias("FP_REASON_9"),
        # col("Form_Paragraphs_10").alias("FP_REASON_10")
    )
    .withColumn(
        "YEAR",
        when(
            month(col("FINAL_REFUSAL_DATE")) > 9, (year(col("FINAL_REFUSAL_DATE")) + 1)
        ).otherwise(year(col("FINAL_REFUSAL_DATE"))),
    )
    .withColumn(
        "FP_REASON_1",
        when(col("FP_REASON_1").isNull(), "Unidentified/Not Captured").otherwise(
            col("FP_REASON_1")
        ),
    )
)

# COMMAND ----------

# bug fix: logic for below was wrong in multiple ways
# note: consider some combo of joins instead of exceptAll

ph_dropDups473_U = ph_frm471.dropDuplicates(["serial_number", "PROCEEDING_NUM"])
ph_dropDups473_D = ph_frm471.exceptAll(ph_dropDups473_U)
ph_sumrz475 = ph_dropDups473_D.select("serial_number").distinct()

# ph_join474 = (
#     ph_frm471.join(
#         ph_sumrz475, on=[col("serial_number") == col("Right_serial_number")], how="left"
#     )
# ).drop("Right_serial_number")

ph_join474 = ph_frm471.join(ph_sumrz475, "serial_number")

# COMMAND ----------

# ph_dropDups473_U.count()

# COMMAND ----------

# ph_dropDups473_D.count()

# COMMAND ----------

# ph_sumrz475.count()

# COMMAND ----------

### bug fix: don't need inner - left; just do left anti join
# ph_join488_left = ph_dropDups473.join(
#     ph_sumrz475, on=[col("serial_number") == col("Right_serial_number")], how="left"
# )

# ph_join488_inner = ph_dropDups473.join(
#     ph_sumrz475, on=[col("serial_number") == col("Right_serial_number")], how="inner"
# )

ph_join488 = ph_dropDups473_U.join(ph_sumrz475, "serial_number", "anti").withColumn(
    "TERM_DATES", lit(None)
).withColumn(
    "TERM_DATE", col("TERMINATION_DATE")
)

# COMMAND ----------

# ph_join488.count()

# COMMAND ----------

# DBTITLE 1,Added New code for checkpoint
# def generate_64bit_ID()-> int:
#     return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
# CHK_POINT_DIR = "/tmp/checkpoints/ttab_details/"+str(generate_64bit_ID())+"/"
# print(f'{CHK_POINT_DIR =}')
# global CHK_POINT_DIR

# COMMAND ----------

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_join488")
# ph_join488 = ph_join488.checkpoint(True)

# COMMAND ----------

ph_cross489 = (
    ph_join488.groupBy(
        "serial_number",
        "TTAB_ISSUE_TYPE",
        "PROCEEDING_NUM",
        "FINAL_REFUSAL_DATE",
        "FILING_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_DATE",
        "DECISION_DATE",
        "DECISION_CODE",
        "DECISION_DESCRIPTION",
        "TERMINATION_CODE",
        "FP_REASON_1",
        "FP_REASON_2",
        "FP_REASON_3",
        "FP_REASON_4",
        "FP_REASON_5",
        "YEAR"
    )
    .pivot("TERM_DATES")
    .agg(concat_ws(",", collect_list(col("TERM_DATE"))))
)

# COMMAND ----------

# DBTITLE 1,Added New code for checkpoint
# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_cross489")
# ph_cross489 = ph_cross489.checkpoint(True)

# COMMAND ----------

    # ph_cross489.display()

# COMMAND ----------

ph_col492 = (
    ph_cross489.withColumn("TERMINATION_DATE", split(col("null"), ",").getItem(0))
    .withColumn("TERMINATION_DATE_2", split(col("null"), ",").getItem(1))
    .withColumn("TERMINATION_DATE_3", split(col("null"), ",").getItem(2))
    .withColumn("TERMINATION_DATE_4", split(col("null"), ",").getItem(3))
    .withColumn("TERMINATION_DATE_5", split(col("null"), ",").getItem(4))
    .drop("Right_serial_number")
    .drop("null")
)

# COMMAND ----------

# ph_col492.count()   

# COMMAND ----------

# ph_join474.count()

# COMMAND ----------

ph_filter183_T = ph_join474.filter(col("DECISION_DATE").isNotNull()).orderBy(
    col("serial_number").asc(),
    col("PROCEEDING_NUM").asc(),
    col("INSTITUTED_DATE").desc(),
    col("DECISION_DATE").desc(),
)
ph_filter183_T_1 = ph_filter183_T.select(
    col("serial_number").alias("serial_number_1"),
    col("TTAB_ISSUE_TYPE").alias("TTAB_ISSUE_TYPE_1"),
    col("PROCEEDING_NUM").alias("PROCEEDING_NUM_1"),)
# ).limit(1) # added for testing

ph_filter183_T2 = (
    ph_filter183_T.join(
        ph_filter183_T_1,
        on=[
            col("serial_number") == col("serial_number_1"),
            col("TTAB_ISSUE_TYPE") == col("TTAB_ISSUE_TYPE_1"),
            col("PROCEEDING_NUM") == col("PROCEEDING_NUM_1"),
        ],
        how="inner",
    )
    .drop("serial_number_1")
    .drop("TTAB_ISSUE_TYPE_1")
    .drop("PROCEEDING_NUM_1")
)

ph_sample484 = (
    ph_filter183_T2.orderBy(
        col("serial_number").asc(),
        col("PROCEEDING_NUM").asc(),
        col("INSTITUTED_DATE").desc(),
        col("DECISION_DATE").desc(),
    )
    .groupBy("serial_number", "TTAB_ISSUE_TYPE", "PROCEEDING_NUM")
    .agg(
        first("FINAL_REFUSAL_DATE").alias("FINAL_REFUSAL_DATE"),
        first("FILING_DATE").alias("FILING_DATE"),
        first("INSTITUTED_CODE").alias("INSTITUTED_CODE"),
        first("INSTITUTED_DATE").alias("INSTITUTED_DATE"),
        first("DECISION_DATE").alias("DECISION_DATE"),
        first("DECISION_CODE").alias("DECISION_CODE"),
        first("DECISION_DESCRIPTION").alias("DECISION_DESCRIPTION"),
        first("TERMINATION_DATE").alias("TERMINATION_DATE"),
        first("TERMINATION_CODE").alias("TERMINATION_CODE"),
        first("FP_REASON_1").alias("FP_REASON_1"),
        first("FP_REASON_2").alias("FP_REASON_2"),
        first("FP_REASON_3").alias("FP_REASON_3"),
        first("FP_REASON_4").alias("FP_REASON_4"),
        first("FP_REASON_5").alias("FP_REASON_5"),
        first("YEAR").alias("YEAR"),
    )
)

ph_filter183_F = ph_join474.filter(col("DECISION_DATE").isNull())

ph_union486 = (
    ph_sample484.union(ph_filter183_F)
    .withColumn("TERM_DATES", lit(None))
    .withColumn("TERM_DATE", col("TERMINATION_DATE"))
)

ph_cross477 = (
    ph_union486.groupBy(
        "serial_number",
        "TTAB_ISSUE_TYPE",
        "PROCEEDING_NUM",
        "FINAL_REFUSAL_DATE",
        "FILING_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_DATE",
        "DECISION_DATE",
        "DECISION_CODE",
        "DECISION_DESCRIPTION",
        "TERMINATION_CODE",
        "FP_REASON_1",
        "FP_REASON_2",
        "FP_REASON_3",
        "FP_REASON_4",
        "FP_REASON_5",
        "YEAR",
    )
    .pivot("TERM_DATES")
    .agg(concat_ws(",", collect_list(col("TERM_DATE"))))
)

# COMMAND ----------

# ph_cross477.count()

# COMMAND ----------

# DBTITLE 1,Added for Checkpoint
# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_cross477")
# ph_cross477 = ph_cross477.checkpoint(True)

# COMMAND ----------

# ph_cross477.limit(20).display()

# COMMAND ----------

ph_col480 = (
    ph_cross477.withColumn("TERMINATION_DATE", split(col("null"), ",").getItem(0))
    .withColumn("TERMINATION_DATE_2", split(col("null"), ",").getItem(1))
    .withColumn("TERMINATION_DATE_3", split(col("null"), ",").getItem(2))
    .withColumn("TERMINATION_DATE_4", split(col("null"), ",").getItem(3))
    .withColumn("TERMINATION_DATE_5", split(col("null"), ",").getItem(4))
    .drop("Right_serial_number")
    .drop("null")
)

ph_union494 = (
    ph_col492.unionByName(ph_col480, allowMissingColumns=True)
    .orderBy(col("serial_number"), col("INSTITUTED_DATE"))
    .withColumn(
        "FILING_DATE",
        when(col("FILING_DATE").isNull(), col("INSTITUTED_DATE")).otherwise(
            col("FILING_DATE")
        ),
    )
    .withColumn("APPEAL", lit(1))
    .withColumn(
        "FINAL_REFUSAL_DATE",
        when(
            (col("APPEAL") == 1) & (col("FINAL_REFUSAL_DATE") == ""),
            date_add(col("FILING_DATE"), -180),
        ).otherwise(col("FINAL_REFUSAL_DATE")),
    )
    .withColumn(
        "INVENTORY",
        when(
            (col("INSTITUTED_DATE").isNotNull())
            & (col("DECISION_DATE") == "")
            & (col("TERMINATION_DATE") == ""),
            lit(True),
        ).otherwise(lit(False)),
    )
    .withColumn("PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_R", lit(None))
    .withColumn(
        "FP_REASON_1", when(col("FP_REASON_1") == "", "Unidentified/Not Captured")
    )
    .drop("YEAR")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Clean-up

# COMMAND ----------

ph_filter1081_T = ph_union494.filter(
    (col("DECISION_DATE").isNull()) & (col("TERMINATION_DATE").isNull())
)
ph_filter1081_F = ph_union494.filter(
    (col("DECISION_DATE").isNotNull()) | (col("TERMINATION_DATE").isNotNull())
)  # 1

ph_select711_1 = ph_select711.withColumnRenamed("serial_number", "Right_serial_number")

ph_join1082 = ph_filter1081_T.join(
    ph_select711_1, on=[col("serial_number") == col("Right_serial_number")], how="left"
).drop("Right_serial_number")

ph_join1082_inner = ph_filter1081_T.join(
    ph_select711_1, on=[col("serial_number") == col("Right_serial_number")], how="inner"
).drop("Right_serial_number")

ph_join1082_left = ph_join1082.subtract(ph_join1082_inner)  # 2

ph_union1085_1 = ph_filter1081_F.unionByName(ph_join1082_left, allowMissingColumns=True)

# COMMAND ----------

# ph_union1085_1.count()

# COMMAND ----------

ph_filter1084_T = (
    ph_join1082_left.filter(col("PUBLICATION_DATE") > col("INSTITUTED_DATE"))
    .withColumn("TERMINATION_CODE", lit("EXPTT"))
    .withColumn("TERMINATION_DATE", col("PUBLICATION_DATE"))
)  # 3
ph_filter1084_F = ph_join1082_left.filter(
    col("PUBLICATION_DATE") <= col("INSTITUTED_DATE")
)  # 4

ph_union1085_2 = ph_union1085_1.union(ph_filter1084_T)

ph_union1085 = (
    ph_union1085_2.union(ph_filter1084_F)
    .withColumn(
        "FP_REASON_1",
        when(col("FP_REASON_1") == "", "Unidentified/Not Captured").otherwise(
            col("FP_REASON_1")
        ),
    )
    .withColumn(
        "INVENTORY",
        when(
            (col("INSTITUTED_DATE").isNotNull())
            & (col("DECISION_DATE") == "")
            & (col("TERMINATION_DATE") == ""),
            lit(True),
        ).otherwise(lit(False)),
    )
)

# COMMAND ----------

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_union1085")
# ph_union1085 = ph_union1085.checkpoint(True)
# #ph_union1085.createOrReplaceGlobalTempView("ph_union1085")

# COMMAND ----------

# ph_union1085.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continued 2 (GET EX PARTE APPEAL EVENTS: CLEANSE, ORGANIZE AND REFINE)

# COMMAND ----------

# ph_frm730
# input_cde - Characteristic Data Elements
ph_join784 = (
    ph_frm730.join(
        input_cde, on=[col("serial_number") == col("SER_NUM")], how="left"
    )
    .drop("SER_NUM")
    .drop("Pendency_Cal_Start_DT")
    .withColumnRenamed("serial_number", "Right_SERIAL_NUMBER")
    .withColumnRenamed("FINAL_REFUSAL_DATE", "Right_FINAL_REFUSAL_DATE")
)


ph_join785 = (
    ph_union1085.join(
        ph_join784,
        on=[
            col("serial_number") == col("Right_SERIAL_NUMBER"),
            col("FINAL_REFUSAL_DATE") == col("Right_FINAL_REFUSAL_DATE"),
        ],
        how="fullouter",
    )
    .withColumn(
        "serial_number",
        when(col("serial_number").isNull(), col("Right_SERIAL_NUMBER")).otherwise(
            col("serial_number")
        ),
    )
    .withColumn(
        "FINAL_REFUSAL_DATE",
        when(
            col("FINAL_REFUSAL_DATE").isNull(), col("Right_FINAL_REFUSAL_DATE")
        ).otherwise(col("FINAL_REFUSAL_DATE")),
    )
    .drop("Right_SERIAL_NUMBER")
    .drop("Right_FINAL_REFUSAL_DATE")
)

ph_frm788 = ph_join785.withColumn("REFUSAL", lit(True))

# COMMAND ----------

# ph_join784.count()

# COMMAND ----------

# ph_join785.count()

# COMMAND ----------

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_join785")
# ph_join785 = ph_join785.checkpoint(True)
# ph_join785.count() #takes 1.09 Hr to complete
# dbutils.fs.ls('dbfs:/tmp/checkpoints/ttab_details/')

# COMMAND ----------

# check_point_dir = '/tmp/checkpoints/ttab_details/2047927524856485477/_ph_cross477/ccd3277c-3ffd-4a9e-9d54-2a3c36107f07/rdd-1467/'

# df=spark.read.parquet('dbfs:/tmp/checkpoints/ttab_details/2047927524856485477/_ph_cross477/ccd3277c-3ffd-4a9e-9d54-2a3c36107f07/rdd-1467/')
# # display('dbfs:/tmp/checkpoints/ttab_details/2047927524856485477/_ph_cross477/ccd3277c-3ffd-4a9e-9d54-2a3c36107f07/rdd-1467/')
# df.display()

# COMMAND ----------

# spark.sql("select * from global_temp.ph_frm166").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # OPPOSITIONS

# COMMAND ----------

listValues = ["OP.IT", "OP.DT", "OP.ST", "TTODP", "OP.TT", "OP.NT", "TTPDA"]

ph_filter250 = ph_frm166.filter(col("five_Characters").isin(listValues))
ph_filter573_T = ph_filter250.filter(col("five_Characters") == "OP.IT").dropDuplicates(
    ["serial_number", "ph_action_date", "cm_prcd_num"]
)
ph_filter573_F = (
    ph_filter250.filter(col("five_Characters") != "OP.IT")
    .dropDuplicates(
        ["serial_number", "ph_action_date", "cm_prcd_num", "five_Characters"]
    )
    .select(
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
    )
)


ph_join569_Left = (
    (
        ph_filter573_T.join(
            ph_filter573_F,
            on=[
                col("serial_number") == col("Right_serial_number"),
                col("cm_prcd_num") == col("Right_cm_prcd_num"),
            ],
            how="left",
        )
    )
    .withColumn("TTAB_ISSUE_TYPE", lit("OPPOSITION"))
    .select(
        col("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("five_Characters").alias("INSTITUTED_CODE"),
        col("PH_ACTION_DATE").alias("INSTITUTED_DATE"),
        col("CM_PRCD_NUM").alias("INSTITUTED_PRCD_NUM"),
        col("Right_five_Characters").alias("DECISION_CODE"),
        col("Right_PH_ACTION_DATE").alias("DECISION_DATE"),
        col("Right_CM_PRCD_NUM").alias("DECISION_PRCD_NUM"),
        col("Right_cm_desc").alias("DECISION_DESCRIPTION"),
    )
)

## bug fix: don't need to do right subract inner, just use anti join

ph_join569_Right = ph_filter573_F.join(
    ph_filter573_T, 
    on=[
        col("serial_number") == col("Right_serial_number"),
        col("cm_prcd_num") == col("Right_cm_prcd_num"),
    ],
    how="anti").withColumn(
        "TTAB_ISSUE_TYPE", lit("OPPOSITION")
    ).select(
        col("Right_serial_number").alias("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("Right_five_Characters").alias("DECISION_CODE"),
        col("Right_PH_ACTION_DATE").alias("DECISION_DATE"),
        col("Right_CM_PRCD_NUM").alias("DECISION_PRCD_NUM"),
        col("Right_cm_desc").alias("DECISION_DESCRIPTION"),
    )

# ph_join569_R = ph_filter573_T.join(
#     ph_filter573_F,
#     on=[
#         col("serial_number") == col("Right_serial_number"),
#         col("cm_prcd_num") == col("Right_cm_prcd_num"),
#     ],
#     how="right",
# )

# ph_join569_I = ph_filter573_T.join(
#     ph_filter573_F,
#     on=[
#         col("serial_number") == col("Right_serial_number"),
#         col("cm_prcd_num") == col("Right_cm_prcd_num"),
#     ],
#     how="inner",
# )

# ph_join569_Right = (
#     ph_join569_R.subtract(ph_join569_I)
#     .withColumn("TTAB_ISSUE_TYPE", lit("OPPOSITION"))
#     .select(
#         col("Right_serial_number").alias("serial_number"),
#         col("TTAB_ISSUE_TYPE"),
#         col("Right_five_Characters").alias("DECISION_CODE"),
#         col("Right_PH_ACTION_DATE").alias("DECISION_DATE"),
#         col("Right_CM_PRCD_NUM").alias("DECISION_PRCD_NUM"),
#         col("Right_cm_desc").alias("DECISION_DESCRIPTION"),
#     )
# )

# COMMAND ----------

ph_union500 = ph_join569_Left.unionByName(ph_join569_Right, allowMissingColumns=True)

ph_filter562 = ph_union500.filter(
    col("INSTITUTED_CODE") == "OP.IT"
).select(
    col("serial_number"),
    col("TTAB_ISSUE_TYPE"),
    col("INSTITUTED_CODE"),
    col("INSTITUTED_DATE"),
    col("INSTITUTED_PRCD_NUM")
).distinct()

# bug fix: code is OP.TT not OP.IT
# bug fix: add OR is null to filter
ph_filter561 = (
    ph_union500.filter((col("DECISION_CODE") != "OP.TT") | (col("DECISION_CODE").isNull()))
    .withColumnRenamed("serial_number", "Right_serial_number")
    .withColumnRenamed("INSTITUTED_DATE", "Right_INSTITUTED_DATE")
    .withColumnRenamed("INSTITUTED_CODE", "Right_INSTITUTED_CODE")
    .withColumnRenamed("INSTITUTED_PRCD_NUM", "Right_INSTITUTED_PRCD_NUM")
)

# might need nvl for below
# ph_join506 = (
#     (
#         ph_filter562.alias("ph_filter562").join(
#             ph_filter561.alias("ph_filter561"),
#             on=[
#                 col("serial_number") == col("Right_serial_number"),
#                 col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
#                 col("INSTITUTED_PRCD_NUM") == col("DECISION_PRCD_NUM"),
#             ],
#             how="outer",
#         ).select(
#             expr("nvl(ph_filter562.serial_number, ph_filter561.Right_serial_number) as serial_number"),
#             expr("nvl(ph_filter562.INSTITUTED_DATE, ph_filter561.Right_INSTITUTED_DATE) as INSTITUTED_DATE"),
#             #expr("nvl(ph_filter562.INSTITUTED_PRCD_NUM, ph_filter561.Right_INSTITUTED_PRCD_NUM) as INSTITUTED_PRCD_NUM"),
#             #expr("nvl(ph_filter562.TTAB_ISSUE_TYPE, ph_filter561.TTAB_ISSUE_TYPE) as TTAB_ISSUE_TYPE"),
#             ph_filter562.TTAB_ISSUE_TYPE,
#             "INSTITUTED_PRCD_NUM",
#             "INSTITUTED_CODE",
#             "Right_INSTITUTED_CODE",
#             "DECISION_DATE",
#             "DECISION_CODE",
#             "DECISION_PRCD_NUM",
#             "DECISION_DESCRIPTION"
#         )
#     )
#     .drop("Right_serial_number", "Right_INSTITUTED_DATE")
# )

ph_join506_L = ph_filter562.alias("ph_filter562").join(
            ph_filter561.alias("ph_filter561"),
            on=[
                col("serial_number") == col("Right_serial_number"),
                col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
                col("INSTITUTED_PRCD_NUM") == col("DECISION_PRCD_NUM"),
            ],
            how="left",
        ).select(ph_filter562.serial_number,
                 ph_filter562.TTAB_ISSUE_TYPE,
                 ph_filter562.INSTITUTED_CODE,
                 ph_filter562.INSTITUTED_DATE,
                 ph_filter562.INSTITUTED_PRCD_NUM,
                 ph_filter561.Right_INSTITUTED_CODE,
                 "DECISION_CODE",
                 "DECISION_DATE",
                 "DECISION_PRCD_NUM",
                 "DECISION_DESCRIPTION")

ph_join506_R = ph_filter561.join(
    ph_filter562,
    on=[
        col("serial_number") == col("Right_serial_number"),
        col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
        col("INSTITUTED_PRCD_NUM") == col("DECISION_PRCD_NUM"),
    ],
    how="anti",
).withColumnRenamed(
    "Right_INSTITUTED_CODE", "INSTITUTED_CODE"
).withColumnRenamed(
    "Right_serial_number", "serial_number"
).withColumnRenamed(
    "Right_INSTITUTED_DATE", "INSTITUTED_DATE"
).withColumnRenamed(
    "Right_INSTITUTED_PRCD_NUM", "INSTITUTED_PRCD_NUM"
)

ph_join506 = ph_join506_L.unionByName(ph_join506_R, allowMissingColumns=True)

# bug fix: code is OP.TT not OP.IT
# bug fix: just distinct, don't count
ph_filter565 = (
    ph_union500.filter(col("DECISION_CODE") == "OP.TT")
    .withColumnRenamed("DECISION_DATE", "TERMINATION_DATE")
    .withColumnRenamed("DECISION_CODE", "TERMINATION_CODE")
    .withColumnRenamed("DECISION_PRCD_NUM", "TERMINATION_PRCD_NUM")
    .select(
        col("serial_number"),
        col("INSTITUTED_DATE"),
        col("TERMINATION_DATE"),
        col("TERMINATION_CODE"),
        col("TERMINATION_PRCD_NUM"),
    )
    .withColumnRenamed("serial_number", "Right_SERIAL_NUMBER")
    .withColumnRenamed("INSTITUTED_DATE", "Right_INSTITUTED_DATE")
)

# COMMAND ----------

# ph_union500.count()

# COMMAND ----------

# ph_filter562.count()

# COMMAND ----------

# ph_filter561.count()

# COMMAND ----------

# ph_join506.count()

# COMMAND ----------

# ph_filter565.count()

# COMMAND ----------

# bug fix: no need for subtract, use anti

ph_join505_right = ph_filter565.join(ph_join506, on=[
        col("serial_number") == col("Right_serial_number"),
        col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
        col("INSTITUTED_PRCD_NUM") == col("TERMINATION_PRCD_NUM"),
    ], how="anti"
).withColumnRenamed(
    "Right_serial_number", "serial_number"
).withColumnRenamed(
    "Right_INSTITUTED_DATE", "INSTITUTED_DATE"
)
                                    

# ph_join505_I = ph_join506.join(
#     ph_filter565,
#     on=[
#         col("serial_number") == col("Right_serial_number"),
#         col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
#         col("INSTITUTED_PRCD_NUM") == col("TERMINATION_PRCD_NUM"),
#     ],
#     how="inner",
# )

# ph_join505_R = ph_join506.join(
#     ph_filter565,
#     on=[
#         col("serial_number") == col("Right_serial_number"),
#         col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
#         col("INSTITUTED_PRCD_NUM") == col("TERMINATION_PRCD_NUM"),
#     ],
#     how="right",
# )

# ph_join505_right = ph_join505_R.subtract(ph_join505_I).select(
#     col("Right_SERIAL_NUMBER").alias("SERIAL_NUMBER"),
#     col("Right_INSTITUTED_DATE"),
#     col("TERMINATION_DATE"),
#     col("TERMINATION_CODE"),
#     col("TERMINATION_PRCD_NUM"),
# )

ph_join505_Left = (
    (
        ph_join506.join(
            ph_filter565,
            on=[
                col("serial_number") == col("Right_serial_number"),
                col("INSTITUTED_DATE") == col("Right_INSTITUTED_DATE"),
                col("INSTITUTED_PRCD_NUM") == col("TERMINATION_PRCD_NUM"),
            ],
            how="left",
        )
    )
    .drop("Right_serial_number")
    .drop("Right_INSTITUTED_DATE")
)

# COMMAND ----------

# ph_join505_right.count()

# COMMAND ----------

# ph_join505_Left.count()

# COMMAND ----------

# bug fix: originally missing 'length' call
ph_sumrz_578 = (
    ph_join505_right.select(
        col("SERIAL_NUMBER"),
        col("TERMINATION_DATE"),
        col("TERMINATION_CODE"),
        col("TERMINATION_PRCD_NUM")
    ).distinct()
    .withColumn(
        "proceedingnumber0",
        when(
            length(col("TERMINATION_PRCD_NUM")) == lit(5),
            concat(lit("910"), col("TERMINATION_PRCD_NUM")),
        ).otherwise(concat(lit("91"), col("TERMINATION_PRCD_NUM"))),
    )
)


ph_join579 = (
    (
        ph_sumrz_578.join(
            ip_df_filter1031,
            on=[
                col("proceedingnumber0") == col("FK_PROCEEDINGNUMBER0"),
                col("SERIAL_NUMBER") == col("REF_SERIAL_NUMBER"),
            ],
            how="inner",
        )
    )
    .drop("FK_PROCEEDINGNUMBER0")
    .drop("REF_SERIAL_NUMBER")
    .drop("TYPE")
    .withColumn("TTAB_ISSUE_TYPE", lit("OPPOSITION"))
    .withColumn("INSTITUTED_DATE", col("FILING_DATE"))
    .withColumn("INSTITUTED_CODE", lit("OP.IT"))
    .withColumnRenamed("TERMINATION_PRCD_NUM", "PROCEEDING_NUM")
    .drop("proceedingnumber0")
)
# select580

# COMMAND ----------

# ph_sumrz_578.count()

# COMMAND ----------

# ip_df_filter1031.count()

# COMMAND ----------

ph_join579.count()

# COMMAND ----------

ph_frm545 = (
    ph_join505_Left.filter(col("SERIAL_NUMBER").isNotNull())
    .withColumn("TERM_DATES", lit(None))
    .withColumn("TERM_DATE", col("TERMINATION_DATE"))
)

# need order by in here?

ph_cross546 = (
    ph_frm545.groupBy(
        "serial_number",
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

# sort based on termination dt
ph_cross546 = ph_cross546.withColumn(
    "null", array_join(array_sort(split(col("null"),',')), ',')
)

# COMMAND ----------

ph_cross546.display()

# COMMAND ----------

ph_cross546_altrx = spark.sql("select * from hive_metastore.alteryx_etldb_dev.ttab_comp_testing_cross546").withColumn(
    "serial_number", col("serial_number").cast(IntegerType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").cast(DateType())
).withColumn(
    "DECISION_DATE", col("DECISION_DATE").cast(DateType())
).withColumnRenamed("_null_", "null").withColumn(
    "null", when(col("null") == ',', '').otherwise(col("null"))
)

# COMMAND ----------

ph_cross546.exceptAll(ph_cross546_altrx).display()

# COMMAND ----------

ph_cross546_altrx.exceptAll(ph_cross546).display()

# COMMAND ----------

for c in ph_cross546.drop("serial_number", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM").columns:
    if ph_cross546.select("serial_number", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM", c).exceptAll(ph_cross546_altrx.select("serial_number", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM", c)).count() == 0:
        print(c + ' matches')
    else:
        print(c + ' does not match')

# COMMAND ----------

c = 'null'
ph_cross546.select("serial_number", c).exceptAll(ph_cross546_altrx.select("serial_number", c)).join(ph_cross546_altrx.select("serial_number", col(c).alias(c+'_altrx')), "serial_number", "left").display()

# COMMAND ----------

ph_frm545.filter(col("serial_number") == 74017895).display()

# COMMAND ----------

ph_cross546.filter(col("serial_number") == 74017895).display()

# COMMAND ----------

ph_cross546_altrx.filter(col("serial_number") == 74017895).display()

# COMMAND ----------

# ph_cross546.count()

# COMMAND ----------

# ph_frm545.count()

# COMMAND ----------

# ph_cross546.count()

# COMMAND ----------

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_cross546")
# ph_cross546 = ph_cross546.checkpoint(True)

# COMMAND ----------

ph_col543 = (
    ph_cross546.withColumn("TERMINATION_DATE", split(col("null"), ",").getItem(0))
    .withColumn("TERMINATION_DATE_2", split(col("null"), ",").getItem(1))
    .withColumn("TERMINATION_DATE_3", split(col("null"), ",").getItem(2))
    .withColumn("TERMINATION_DATE_4", split(col("null"), ",").getItem(3))
    .withColumn("TERMINATION_DATE_5", split(col("null"), ",").getItem(4))
    .drop("Right_serial_number")
    .drop("null")
    .select(
        col("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("INSTITUTED_DATE"),
        col("INSTITUTED_CODE"),
        col("INSTITUTED_PRCD_NUM"),
        col("DECISION_DATE"),
        col("DECISION_CODE"),
        col("DECISION_PRCD_NUM"),
        col("DECISION_DESCRIPTION"),
        col("TERMINATION_CODE"),
        col("TERMINATION_PRCD_NUM"),
        col("TERMINATION_DATE").astype(DateType()),
        col("TERMINATION_DATE_2").astype(DateType()),
        col("TERMINATION_DATE_3").astype(DateType()),
        col("TERMINATION_DATE_4").astype(DateType()),
        col("TERMINATION_DATE_5").astype(DateType())
    ).distinct()
    .withColumn(
        "INSTITUTED_PRCD_NUM",
        when(((col("INSTITUTED_PRCD_NUM") == "") | col("INSTITUTED_PRCD_NUM").isNull()), col("DECISION_PRCD_NUM")).otherwise(
            col("INSTITUTED_PRCD_NUM")
        ),
    )
)

# bug fixes: isEmpty needs a check for null records too, not just empty string
# bug fixes: missing col() around INSTITUTED_PRCD_NUM in otherwise

ph_frm1047 = ph_col543.withColumn(
    "INSTITUTED_PRCD_NUM",
    when(((col("INSTITUTED_PRCD_NUM") == "") | col("INSTITUTED_PRCD_NUM").isNull()), col("TERMINATION_PRCD_NUM")).otherwise(
        col("INSTITUTED_PRCD_NUM")
    ),
)

# bug fix: prior logic for constructed prcd num was entirely incorrect


ph_frm1034 = ph_frm1047.withColumn(
    "CONSTRUCTED_PRCD_NUM", expr("""
        case 
        when TTAB_ISSUE_TYPE = 'OPPOSITION'
        then
            case
            when INSTITUTED_PRCD_NUM = '0'
            then 0
            when length(INSTITUTED_PRCD_NUM) = 5
            then '910' || INSTITUTED_PRCD_NUM
            else '91' || INSTITUTED_PRCD_NUM
            end
        when TTAB_ISSUE_TYPE = 'CANCELLATION'
        then
            case
            when INSTITUTED_PRCD_NUM = '0'
            then 0
            when length(INSTITUTED_PRCD_NUM) = 5
            then '920' || INSTITUTED_PRCD_NUM
            else '92' || INSTITUTED_PRCD_NUM
            end
        else "not picked up"
        end                                       
    """)
)


# COMMAND ----------

# ph_frm1034.count()

# COMMAND ----------

ph_join540_I = ph_frm1034.join(
    ip_df_filter1031,
    on=[
        col("serial_number") == col("REF_SERIAL_NUMBER"),
        col("CONSTRUCTED_PRCD_NUM") == col("FK_PROCEEDINGNUMBER0"),
    ],
    how="inner",
)

# bug fix: replace subtract with anti
ph_join540_left = ph_frm1034.join(
    ip_df_filter1031,
    on=[
        col("serial_number") == col("REF_SERIAL_NUMBER"),
        col("CONSTRUCTED_PRCD_NUM") == col("FK_PROCEEDINGNUMBER0"),
    ],
    how="anti",
)

# ph_join540_left = ph_join540_L.subtract(ph_join540_I)

ph_sumrz539 = (
    ph_join540_I.select(
        col("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("FILING_DATE"),
        col("INSTITUTED_DATE"),
        col("DECISION_DATE"),
        col("DECISION_CODE"),
        col("DECISION_DESCRIPTION"),
        col("TERMINATION_DATE"),
        col("TERMINATION_CODE"),
        col("DECISION_PRCD_NUM"),
        col("TERMINATION_PRCD_NUM"),
        col("INSTITUTED_PRCD_NUM"),
        col("TERMINATION_DATE_2"),
        col("TERMINATION_DATE_3"),
        col("TERMINATION_DATE_4"),
        col("TERMINATION_DATE_5"),
        col("INSTITUTED_CODE"),
        col("CONSTRUCTED_PRCD_NUM"),
    ).distinct()
)

ph_union538 = (
    ph_join540_left.unionByName(ph_sumrz539, allowMissingColumns=True)
    .drop("TYPE")
    .drop("FK_PROCEEDINGNUMBER0")
    .drop("REF_SERIAL_NUMBER")
    .withColumn(
        "FILING_DATE",
        when(col("FILING_DATE").isNull(), col("INSTITUTED_DATE")).otherwise(
            col("FILING_DATE")
        ),
    )
    # .orderBy(
    #     col("serial_number").asc(),
    #     col("FILING_DATE").desc(),
    #     col("INSTITUTED_DATE").desc(),
    #     col("DECISION_DATE").desc(),
    #     col("TERMINATION_DATE").desc(),
    #     col("INSTITUTED_PRCD_NUM").asc(),
    # )
)

ph_union538 = ph_union538.withColumn("TERMINATION_DATE", when(col("TERMINATION_DATE") == "", lit(None)).otherwise(col("TERMINATION_DATE")))

# COMMAND ----------

ph_join540_I_altrx = spark.sql("select * from hive_metastore.alteryx_etldb_dev.ttab_comp_testing_sum539").withColumn(
    "serial_number", col("serial_number").cast(IntegerType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").cast(DateType())
).withColumn(
    "DECISION_DATE", col("DECISION_DATE").cast(DateType())
).select(
        col("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("FILING_DATE"),
        col("INSTITUTED_DATE"),
        col("DECISION_DATE"),
        col("DECISION_CODE"),
        col("DECISION_DESCRIPTION"),
        col("TERMINATION_DATE"),
        col("TERMINATION_CODE"),
        col("DECISION_PRCD_NUM"),
        col("TERMINATION_PRCD_NUM"),
        col("INSTITUTED_PRCD_NUM"),
        col("TERMINATION_DATE_2"),
        col("TERMINATION_DATE_3"),
        col("TERMINATION_DATE_4"),
        col("TERMINATION_DATE_5"),
        col("INSTITUTED_CODE"),
        col("CONSTRUCTED_PRCD_NUM"),
    )

# COMMAND ----------

ph_join540_I.select(
        col("serial_number"),
        col("TTAB_ISSUE_TYPE"),
        col("FILING_DATE"),
        col("INSTITUTED_DATE"),
        col("DECISION_DATE"),
        col("DECISION_CODE"),
        col("DECISION_DESCRIPTION"),
        col("TERMINATION_DATE"),
        col("TERMINATION_CODE"),
        col("DECISION_PRCD_NUM"),
        col("TERMINATION_PRCD_NUM"),
        col("INSTITUTED_PRCD_NUM"),
        col("TERMINATION_DATE_2"),
        col("TERMINATION_DATE_3"),
        col("TERMINATION_DATE_4"),
        col("TERMINATION_DATE_5"),
        col("INSTITUTED_CODE"),
        col("CONSTRUCTED_PRCD_NUM"),
    ).withColumn("TERMINATION_DATE", when(col("TERMINATION_DATE") == "", lit(None)).otherwise(col("TERMINATION_DATE"))).exceptAll(ph_join540_I_altrx).display()

# COMMAND ----------



# COMMAND ----------

ph_join540_left_altrx.exceptAll(ph_join540_left).display()

# COMMAND ----------

for c in ph_join540_left.drop("serial_number", "constructed_prcd_num").columns:
    if ph_join540_left.select("serial_number", "constructed_prcd_num", c).exceptAll(ph_join540_left_altrx.select("serial_number", "constructed_prcd_num", c)).count() == 0:
        print(c + ' matches')
    else:
        print(c + ' does not match')

# COMMAND ----------

c = 'TERMINATION_DATE'
ph_join540_left.select("serial_number", "constructed_prcd_num", c).exceptAll(ph_join540_left_altrx.select("serial_number", "constructed_prcd_num", c)).join(ph_join540_left_altrx.select("serial_number", "constructed_prcd_num", col(c).alias(c+'_altrx')), ["serial_number", "constructed_prcd_num"], "left").display()

# COMMAND ----------

c = 'TERMINATION_DATE'
ph_join540_left.select("serial_number", "constructed_prcd_num", c).exceptAll(ph_join540_left_altrx.select("serial_number", "constructed_prcd_num", c)).select(c).distinct().display()

# COMMAND ----------

# ph_join540_I.count()

# COMMAND ----------

# ph_join540_left.count()

# COMMAND ----------

# ph_union538.count()

# COMMAND ----------

## bug fix: prior logic was completely wrong, need to add window function

win1039 = Window().partitionBy("serial_number", "FILING_DATE", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM").orderBy(col("DECISION_DATE").desc(), col("TERMINATION_DATE").desc())

ph_sample1039 = ph_union538.withColumn("rn", row_number().over(win1039)).filter(col("rn") == 1).drop("rn")

# ph_sample1039 = ph_union538.groupBy(
#     "serial_number", "FILING_DATE", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM"
# ).agg(
#     first("TTAB_ISSUE_TYPE").alias("TTAB_ISSUE_TYPE"),
#     first("INSTITUTED_CODE").alias("INSTITUTED_CODE"),
#     first("DECISION_DATE").alias("DECISION_DATE"),
#     first("DECISION_CODE").alias("DECISION_CODE"),
#     first("DECISION_PRCD_NUM").alias("DECISION_PRCD_NUM"),
#     first("DECISION_DESCRIPTION").alias("DECISION_DESCRIPTION"),
#     first("TERMINATION_DATE").alias("TERMINATION_DATE"),
#     first("TERMINATION_CODE").alias("TERMINATION_CODE"),
#     first("TERMINATION_PRCD_NUM").alias("TERMINATION_PRCD_NUM"),
#     first("TERMINATION_DATE_2").alias("TERMINATION_DATE_2"),
#     first("TERMINATION_DATE_3").alias("TERMINATION_DATE_3"),
#     first("TERMINATION_DATE_4").alias("TERMINATION_DATE_4"),
#     first("TERMINATION_DATE_5").alias("TERMINATION_DATE_5"),
#     first("CONSTRUCTED_PRCD_NUM").alias("CONSTRUCTED_PRCD_NUM"),
# )

# COMMAND ----------

ph_sample1039.count()

# COMMAND ----------

ph_sample1039_altrx = spark.sql("select * from hive_metastore.alteryx_etldb_dev.ttab_comp_testing_sample1039").withColumn(
    "serial_number", col("serial_number").cast(IntegerType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").cast(DateType())
).withColumn(
    "DECISION_DATE", col("DECISION_DATE").cast(DateType())
).withColumn(
    "FILING_DATE", col("FILING_DATE").cast(DateType())
# ).withColumn(
#     "TERMINATION_DATE", col("TERMINATION_DATE").cast(DateType())
# ).withColumn(
#     "TERMINATION_DATE_2", col("TERMINATION_DATE_2").cast(DateType())
# ).withColumn(
#     "TERMINATION_DATE_3", col("TERMINATION_DATE_3").cast(DateType())
# ).withColumn(
#     "TERMINATION_DATE_4", col("TERMINATION_DATE_4").cast(DateType())
# ).withColumn(
#     "TERMINATION_DATE_5", col("TERMINATION_DATE_5").cast(DateType())
).select(ph_sample1039.columns)

# COMMAND ----------

## ~30 records with exact same values for group by columns, ends up with decision code being different between dbx and altrx
ph_union538.filter(col("serial_number") == 74180679).display()

# COMMAND ----------

ph_sample1039.exceptAll(ph_sample1039_altrx).display()

# COMMAND ----------

ph_sample1039_altrx.exceptAll(ph_sample1039).display()

# COMMAND ----------

for c in ph_sample1039.columns:
    if ph_sample1039.select("serial_number", "constructed_prcd_num", c).exceptAll(ph_sample1039_altrx.select("serial_number", "constructed_prcd_num", c)).count() == 0:
        print(c + ' matches')
    else:
        print(c + ' does not match')

# COMMAND ----------

c = 'DECISION_CODE'
ph_sample1039.select("serial_number", "constructed_prcd_num", c).exceptAll(ph_sample1039_altrx.select("serial_number", "constructed_prcd_num", c)).join(ph_sample1039_altrx.select("serial_number", "constructed_prcd_num", col(c).alias(c+'_altrx')), ["serial_number", "constructed_prcd_num"], "left").display()

# COMMAND ----------

c = 'DECISION_DESCRIPTION'
ph_sample1039.select("serial_number", "constructed_prcd_num", c).exceptAll(ph_sample1039_altrx.select("serial_number", "constructed_prcd_num", c)).join(ph_sample1039_altrx.select("serial_number", "constructed_prcd_num", col(c).alias(c+'_altrx')), "serial_number", "constructed_prcd_num", "left").display()

# COMMAND ----------

ph_unique532_U = ph_sample1039.dropDuplicates(["serial_number", "INSTITUTED_PRCD_NUM"])
ph_unique532_D = ph_sample1039.exceptAll(ph_unique532_U).select("serial_number").distinct()

ph_join531_I = ph_unique532_U.join(ph_unique532_D, "serial_number")

ph_join531_L = ph_unique532_U.join(ph_unique532_D, "serial_number", "anti")

# COMMAND ----------

ph_unique532_D.join(ph_unique532_U, "serial_number", "anti").display()

# COMMAND ----------

ph_sample1039.count()

# COMMAND ----------

ph_unique532_U_altrx = spark.sql("select * from hive_metastore.alteryx_etldb_dev.ttab_comp_testing_unique532_U").withColumn(
    "serial_number", col("serial_number").cast(IntegerType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").cast(DateType())
).withColumn(
    "DECISION_DATE", col("DECISION_DATE").cast(DateType())
).withColumn(
    "FILING_DATE", col("FILING_DATE").cast(DateType())
)

# COMMAND ----------

ph_unique532_U.exceptAll(ph_unique532_U_altrx).display()

# COMMAND ----------

for c in ph_unique532_U.columns:
    if ph_unique532_U.select("serial_number", "INSTITUTED_PRCD_NUM", c).exceptAll(ph_unique532_U_altrx.select("serial_number", "INSTITUTED_PRCD_NUM", c)).count() == 0:
        print(c + ' matches')
    else:
        print(c + ' does not match')

# COMMAND ----------

c = 'INSTITUTED_DATE'
ph_unique532_U.select("serial_number", "INSTITUTED_PRCD_NUM", c).exceptAll(ph_unique532_U_altrx.select("serial_number", "INSTITUTED_PRCD_NUM", c)).join(ph_unique532_U_altrx.select("serial_number", "INSTITUTED_PRCD_NUM", col(c).alias(c+'_altrx')), ["serial_number", "INSTITUTED_PRCD_NUM"], "left").display()

# COMMAND ----------

ph_sample1039.filter(col("serial_number") == 73243788).display()

# COMMAND ----------

ph_sample1039_altrx.filter(col("serial_number") == 73243788).display()

# COMMAND ----------

ph_unique532_D.count()

# COMMAND ----------

ph_unique532_U.count()

# COMMAND ----------

ph_join531_L.count()

# COMMAND ----------

ph_join531_I.count()

# COMMAND ----------

ph_sumrz528 = (
    ph_join531_I.select(
        "serial_number",
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
    ).distinct())

win527 = Window().partitionBy("serial_number",
            "TTAB_ISSUE_TYPE",
            "INSTITUTED_DATE",
            "DECISION_DATE",
            "TERMINATION_DATE",
            "INSTITUTED_PRCD_NUM").orderBy(col("filing_date").desc())
ph_sample527 = ph_sumrz528.withColumn("rn", row_number().over(win527)).filter(col("rn") == 1).drop("rn")

win516 = Window().partitionBy("serial_number",
            "filing_date",
            "INSTITUTED_PRCD_NUM").orderBy(col("INSTITUTED_DATE").desc())
ph_sample516 = ph_sample527.withColumn("rn", row_number().over(win516)).filter(col("rn") == 1).drop("rn")

# ph_sample516 = ph_sumrz528.orderBy(
#     col("serial_number").asc(),
#     col("INSTITUTED_DATE").asc(),
#     col("FILING_DATE").asc(),
#     col("INSTITUTED_DATE").desc(),
# ).dropDuplicates(["serial_number", "FILING_DATE", "INSTITUTED_PRCD_NUM"])

ph_union525 = (
    ph_join531_L.unionByName(ph_sample516, allowMissingColumns=True)
    .orderBy(col("serial_number").asc(), col("INSTITUTED_DATE").asc())
    .withColumnRenamed("INSTITUTED_PRCD_NUM", "PROCEEDING_NUM")
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
        "DECISION_YR",
        when(
            month(col("DECISION_DATE")) > 9, (year(col("DECISION_DATE")) + 1)
        ).otherwise(year(col("DECISION_DATE"))),
    )
    .withColumn(
        "TERM_YR",
        when(
            month(col("TERMINATION_DATE")) > 9, (year(col("TERMINATION_DATE")) + 1)
        ).otherwise(year(col("TERMINATION_DATE"))),
    )
)

# COMMAND ----------

ph_union525.count()

# COMMAND ----------

ph_filter519_T = ph_union525.filter(
    col("DECISION_DATE").isNotNull()
    & col("TERMINATION_DATE").isNull()
    & (datediff(current_date(), col("DECISION_DATE")) > 63)
)
ph_filter519_F = ph_union525.subtract(ph_filter519_T)

ph_frm515 = ph_filter519_T.withColumn("TERMINATION_CODE", lit("CANTT")).withColumn(
    "TERMINATION_DATE", date_add(col("DECISION_DATE"), 63)
)

# COMMAND ----------

ph_union513 = ph_filter519_F.unionByName(ph_frm515, allowMissingColumns=True)


## bug fix: logic for dropping dupes incorrect, need window

win582 = Window().partitionBy(
    "serial_number", "PROCEEDING_NUM"
).orderBy(
    col("FILING_DATE").desc(), 
    col("INSTITUTED_DATE").desc(),
    col("DECISION_DATE").desc()
)

union582 = ph_join579.unionByName(ph_union513, allowMissingColumns=True)

sample1019 = union582.withColumn("rn", row_number().over(win582)).filter(col("rn") == 1).drop("rn")

# ph_join582 = (
#     ph_join579.unionByName(ph_union513, allowMissingColumns=True)
#     .orderBy(
#         col("serial_number").asc(),
#         col("PROCEEDING_NUM").asc(),
#         col("FILING_DATE").desc(),
#         col("INSTITUTED_DATE").desc(),
#         col("DECISION_DATE").desc(),
#     )
#     .dropDuplicates(["serial_number", "PROCEEDING_NUM"])
# )



# COMMAND ----------

ph_union513.count()

# COMMAND ----------

200061 + 1698

# COMMAND ----------

union582_altrx = spark.sql("select * from hive_metastore.alteryx_etldb_dev.ttab_comp_testing_union582").select(
    col('SERIAL_NUMBER').astype(IntegerType()),
    col('TERMINATION_DATE').astype(DateType()),
    'TERMINATION_CODE',
    'PROCEEDING_NUM',
    col('FILING_DATE').astype(DateType()),
    'TTAB_ISSUE_TYPE',
    col('INSTITUTED_DATE').astype(DateType()),
    'INSTITUTED_CODE',
    col('DECISION_DATE').astype(DateType()),
    'DECISION_CODE',
    'DECISION_DESCRIPTION',
    col('TERMINATION_DATE_2').astype(DateType()),
    col('TERMINATION_DATE_3').astype(DateType()), 
    col('TERMINATION_DATE_4').astype(DateType()), 
    col('TERMINATION_DATE_5').astype(DateType()), 
    'CONSTRUCTED_PRCD_NUM',
    col('FILED_YR').astype(IntegerType()), 
    col('INST_YR').astype(IntegerType()),
    col('DECISION_YR').astype(IntegerType()),
    col('TERM_YR').astype(IntegerType())
)

# COMMAND ----------

union582_altrx.count()

# COMMAND ----------

union582.exceptAll(union582_altrx).display()

# COMMAND ----------

union582.count()

# COMMAND ----------

ph_sumrz1021 = (
    ph_join582.groupBy(
        "SERIAL_NUMBER",
        "TERMINATION_DATE",
        "TERMINATION_CODE",
        "PROCEEDING_NUM",
        "FILING_DATE",
        "TTAB_ISSUE_TYPE",
        "INSTITUTED_DATE",
        "INSTITUTED_CODE",
        "DECISION_DATE",
        "DECISION_CODE",
        "DECISION_DESCRIPTION",
        "TERMINATION_DATE_2",
        "TERMINATION_DATE_3",
        "TERMINATION_DATE_4",
        "TERMINATION_DATE_5",
        "CONSTRUCTED_PRCD_NUM",
        "FILED_YR",
        "INST_YR",
        "DECISION_YR",
        "TERM_YR",
    )
    .count()
    .drop("count")
    .withColumn("PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("OPPOSITION", lit(True))
    .withColumn(
        "INVENTORY",
        when(
            col("INSTITUTED_DATE").isNotNull()
            & (col("DECISION_DATE") == "")
            & (col("TERMINATION_DATE") == ""),
            lit(True),
        ).otherwise(lit(False)),
    )
    .drop("FILED_YR")
    .drop("INST_YR")
    .drop("DECISION_YR")
    .drop("TERM_YR")
    .drop("PENDENCY_D")
    .drop("PENDENCY_T")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEFAULT FLAG - Opposition

# COMMAND ----------

ph_join1108_I = ph_sumrz1021.join(
    filter_1091_F, on=[col("CONSTRUCTED_PRCD_NUM") == col("PROCEEDING")], how="inner"
)

ph_join1108_L = ph_sumrz1021.join(
    filter_1091_F, on=[col("CONSTRUCTED_PRCD_NUM") == col("PROCEEDING")], how="left"
)

ph_join1108_Left = ph_join1108_L.subtract(ph_join1108_I).withColumn(
    "DEFAULT_OPPOSITION", lit(False)
)

ph_sumrz1109 = ph_join1108_I.groupBy(
    col("SERIAL_NUMBER").alias("Right_SERIAL_NUMBER"),
    col("PROCEEDING_NUM").alias("Right_PROCEEDING_NUM"),
).agg(max("NOD_DATE").alias("Max_NOD_DATE"))

# COMMAND ----------

ph_join1110 = (
    ph_join1108_I.join(
        ph_sumrz1109,
        on=[
            col("SERIAL_NUMBER") == col("Right_SERIAL_NUMBER"),
            col("PROCEEDING_NUM") == col("Right_PROCEEDING_NUM"),
            col("NOD_DATE") == col("Max_NOD_DATE"),
        ],
        how="inner",
    )
    .drop("Right_SERIAL_NUMBER")
    .drop("Right_PROCEEDING_NUM")
    .drop("Max_NOD_DATE")
    .withColumn("DEFAULT_OPPOSITION", lit(True))
)

# COMMAND ----------

ph_union1111 = ph_join1110.unionByName(ph_join1108_I,allowMissingColumns=True)

# COMMAND ----------

ph_select1113 = ph_union1111.select(
    col("SERIAL_NUMBER"),
    col("TTAB_ISSUE_TYPE"),
    col("PROCEEDING_NUM"),
    col("FILING_DATE"),
    col("INSTITUTED_DATE"),
    col("INSTITUTED_CODE"),
    col("DECISION_DATE"),
    col("DECISION_CODE"),
    col("DECISION_DESCRIPTION"),
    col("TERMINATION_CODE"),
    col("TERMINATION_DATE"),
    col("TERMINATION_DATE_2"),
    col("TERMINATION_DATE_3"),
    col("TERMINATION_DATE_4"),
    col("TERMINATION_DATE_5"),
    col("CONSTRUCTED_PRCD_NUM"),
    col("OPPOSITION"),
    col("INVENTORY"),
    col("NOD_DATE").alias("DEFAULT_DATE"),
    col("DEFAULT_OPPOSITION"),
)

# COMMAND ----------

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_select1113")
ph_select1113 = ph_select1113.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## continued (OPPOSITIONS)

# COMMAND ----------

ph_frm739 = ph_select711.withColumn("PUBLICATION", lit(True))
# Final output of Characteristic Data Elements select300


ph_join790 = (
    ph_frm739.join(
        input_cde, on=[col("SERIAL_NUMBER") == col("SER_NUM")], how="left"
    )
    .drop("Pendency_Cal_Start_DT")
    .drop("SER_NUM")
    .withColumnRenamed("SERIAL_NUMBER", "Right_SERIAL_NUMBER")
)


ph_join791 = ph_select1113.join(
    ph_join790, on=[col("SERIAL_NUMBER") == col("Right_SERIAL_NUMBER")], how="fullouter"
).withColumn(
    "serial_number",
    when(col("serial_number").isNull(), col("Right_SERIAL_NUMBER")).otherwise(
        col("serial_number")
    ),
)

ph_frm794 = ph_join791.withColumn("PUBS", lit(True))

# COMMAND ----------

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_frm794")
# ph_frm794 = ph_frm794.checkpoint(True)
#ph_frm794.createOrReplaceGlobalTempView("ph_frm794")

# COMMAND ----------

# MAGIC %md
# MAGIC # CANCELLATIONS Updated 2/06/2020

# COMMAND ----------

listValues = ["PETCT", "CANDT", "CANGT", "TTCDP", "CANTT"]

ph_filter605 = ph_frm166.filter(col("five_Characters").isin(listValues))
ph_filter604_T = ph_filter605.filter(col("five_Characters") == "PETCT").dropDuplicates(
    ["serial_number", "ph_action_date", "cm_prcd_num"]
)
ph_filter604_F = (
    ph_filter605.filter(col("five_Characters") != "PETCT")
    .dropDuplicates(
        ["serial_number", "ph_action_date", "cm_prcd_num", "five_Characters"]
    )
    .select(
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
    )
)

ph_join615_Left = (
    (
        ph_filter604_T.join(
            ph_filter604_F,
            on=[col("serial_number") == col("Right_serial_number")],
            how="left",
        )
    )
    .withColumn("TTAB_ISSUE_TYPE", lit("CANCELLATION"))
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

ph_join615_R = ph_filter604_T.join(
    ph_filter604_F, on=[col("serial_number") == col("Right_serial_number")], how="right"
)

ph_join615_I = ph_filter604_T.join(
    ph_filter604_F, on=[col("serial_number") == col("Right_serial_number")], how="inner"
)

ph_join615_Right = ph_join615_R.subtract(ph_join615_I)

# COMMAND ----------

ph_join660 = (
    ph_join615_Right.join(
        ip_df_filter1031_CAN,
        on=[col("serial_number") == col("REF_SERIAL_NUMBER")],
        how="inner",
    )
    .filter(col("FILING_DATE") <= col("PH_ACTION_DATE"))
    .withColumn("TTAB_ISSUE_TYPE", lit("CANCELLATION"))
    .withColumn("INSTITUTED_DATE", col("FILING_DATE"))
    .withColumn("INSTITUTED_CODE", lit("PETCD"))
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
        col("FK_PROCEEDINGNUMBER0"),
    )
)

ph_union666 = ph_join615_Left.unionByName(ph_join660, allowMissingColumns=True)

# COMMAND ----------

ph_filter609 = (
    ph_union666.filter(col("INSTITUTED_CODE") == "PETCT")
    .groupBy(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "INSTITUTED_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_PRCD_NUM",
    )
    .count()
    .drop("count")
)

ph_filter612 = (
    ph_union666.filter(col("DECISION_CODE") != "CANTT")
    .withColumnRenamed("SERIAL_NUMBER", "Right_SERIAL_NUMBER")
    .withColumnRenamed("INSTITUTED_DATE", "Right_INSTITUTED_DATE")
    .drop("TTAB_ISSUE_TYPE")
    .drop("INSTITUTED_CODE")
    .drop("INSTITUTED_PRCD_NUM")
)

ph_filter606 = (
    ph_union666.filter(col("DECISION_CODE") == "CANTT")
    .withColumnRenamed("DECISION_DATE", "TERMINATION_DATE")
    .withColumnRenamed("DECISION_CODE", "TERMINATION_CODE")
    .withColumnRenamed("DECISION_PRCD_NUM", "TERMINATION_PRCD_NUM")
    .drop("DECISION_DESCRIPTION")
    .groupBy(
        "SERIAL_NUMBER",
        "INSTITUTED_DATE",
        "TERMINATION_DATE",
        "TERMINATION_CODE",
        "TERMINATION_PRCD_NUM",
    )
    .count()
    .drop("count")
    .withColumnRenamed("SERIAL_NUMBER", "Right_SERIAL_NUMBER")
    .withColumnRenamed("INSTITUTED_DATE", "Right_INSTITUTED_DATE")
)

# COMMAND ----------

ph_join622_1 = (
    ph_filter609.join(
        ph_filter612,
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

ph_join622 = (
    ph_join622_1.join(
        ph_filter606,
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

ph_cross624 = (
    ph_join622.groupBy(
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

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_cross624")
ph_cross624 = ph_cross624.checkpoint(True)
#ph_frm794.createOrReplaceGlobalTempView("ph_frm794")

# COMMAND ----------

ph_col627 = (
    ph_cross624.withColumn("TERMINATION_DATE", split(col("null"), ",").getItem(0))
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

# COMMAND ----------

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_col627")
ph_col627 = ph_col627.checkpoint(True)

# COMMAND ----------

ph_frm1049 = ph_col627.withColumn(
    "INSTITUTED_PRCD_NUM",
    when(col("INSTITUTED_PRCD_NUM") == "", col("TERMINATION_PRCD_NUM")).otherwise(
        col("INSTITUTED_PRCD_NUM")
    ),
)

ph_frm1035_1 = ph_frm1049.withColumn(
    "CONSTRUCTED_PRCD_NUM",
    when(
        (col("TTAB_ISSUE_TYPE") == "OPPOSITION") & (col("INSTITUTED_PRCD_NUM") == "0"),
        lit("0"),
    )
    .when(
        (col("TTAB_ISSUE_TYPE") == "OPPOSITION")
        & (length(col("INSTITUTED_PRCD_NUM")) == 5),
        concat(lit("910"), col("INSTITUTED_PRCD_NUM")),
    )
    .otherwise(concat(lit("91"), col("INSTITUTED_PRCD_NUM"))),
)

ph_frm1035 = ph_frm1035_1.withColumn(
    "CONSTRUCTED_PRCD_NUM",
    when(
        (col("TTAB_ISSUE_TYPE") != "CANCELLATION")
        & (col("TTAB_ISSUE_TYPE") != "CANCELLATION"),
        "not picked up",
    )
    .when(
        (col("TTAB_ISSUE_TYPE") == "CANCELLATION")
        & (col("INSTITUTED_PRCD_NUM") == "0"),
        lit("0"),
    )
    .when(
        (col("TTAB_ISSUE_TYPE") == "CANCELLATION")
        & (length(col("INSTITUTED_PRCD_NUM")) == 5),
        concat(lit("920"), col("INSTITUTED_PRCD_NUM")),
    )
    .otherwise(concat(lit("92"), col("INSTITUTED_PRCD_NUM"))),
)

# COMMAND ----------

ph_join630_I = (
    (
        ph_frm1035.join(
            ip_df_filter1031_CAN,
            on=[
                col("serial_number") == col("REF_SERIAL_NUMBER"),
                col("CONSTRUCTED_PRCD_NUM") == col("FK_PROCEEDINGNUMBER0"),
            ],
            how="inner",
        )
        .drop("TYPE")
        .drop("FK_PROCEEDINGNUMBER0")
        .drop("REF_SERIAL_NUMBER")
    )
    .groupBy(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "FILING_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_DATE",
        "INSTITUTED_PRCD_NUM",
        "DECISION_DATE",
        "DECISION_CODE",
        "DECISION_PRCD_NUM",
        "DECISION_DESCRIPTION",
        "TERMINATION_CODE",
        "TERMINATION_PRCD_NUM",
        "TERMINATION_DATE",
        "TERMINATION_DATE_2",
        "TERMINATION_DATE_3",
        "TERMINATION_DATE_4",
        "TERMINATION_DATE_5",
        "CONSTRUCTED_PRCD_NUM",
    )
    .count()
    .drop("count")
)

ph_join630_L = (
    ph_frm1035.join(
        ip_df_filter1031_CAN,
        on=[
            col("serial_number") == col("REF_SERIAL_NUMBER"),
            col("CONSTRUCTED_PRCD_NUM") == col("FK_PROCEEDINGNUMBER0"),
        ],
        how="left",
    )
    .drop("TYPE")
    .drop("FK_PROCEEDINGNUMBER0")
    .drop("REF_SERIAL_NUMBER")
)

ph_join630_Left = ph_join630_L.subtract(ph_join630_I)

ph_union632 = (
    ph_join630_Left.union(ph_join630_I)
    .withColumn(
        "FILING_DATE",
        when(col("FILING_DATE").isNull(), col("INSTITUTED_DATE")).otherwise(
            col("FILING_DATE")
        ),
    )
    .orderBy(
        col("serial_number").asc(),
        col("FILING_DATE").desc(),
        col("INSTITUTED_DATE").desc(),
        col("DECISION_DATE").desc(),
        col("TERMINATION_DATE").desc(),
        col("INSTITUTED_PRCD_NUM").asc(),
    )
)

# COMMAND ----------

ph_sample1036 = ph_union632.groupBy(
    "serial_number", "FILING_DATE", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM"
).agg(
    first("TTAB_ISSUE_TYPE").alias("TTAB_ISSUE_TYPE"),
    first("INSTITUTED_CODE").alias("INSTITUTED_CODE"),
    first("DECISION_DATE").alias("DECISION_DATE"),
    first("DECISION_CODE").alias("DECISION_CODE"),
    first("DECISION_PRCD_NUM").alias("DECISION_PRCD_NUM"),
    first("DECISION_DESCRIPTION").alias("DECISION_DESCRIPTION"),
    first("TERMINATION_DATE").alias("TERMINATION_DATE"),
    first("TERMINATION_CODE").alias("TERMINATION_CODE"),
    first("TERMINATION_PRCD_NUM").alias("TERMINATION_PRCD_NUM"),
    first("TERMINATION_DATE_2").alias("TERMINATION_DATE_2"),
    first("TERMINATION_DATE_3").alias("TERMINATION_DATE_3"),
    first("TERMINATION_DATE_4").alias("TERMINATION_DATE_4"),
    first("TERMINATION_DATE_5").alias("TERMINATION_DATE_5"),
    first("CONSTRUCTED_PRCD_NUM").alias("CONSTRUCTED_PRCD_NUM"),
)
ph_unique641 = ph_sample1036.dropDuplicates(["serial_number", "INSTITUTED_PRCD_NUM"])
ph_unique641_dups = (
    ph_sample1036.subtract(ph_unique641)
    .groupBy(col("serial_number").alias("Right_SERIAL_NUMBER"))
    .count()
    .drop("count")
)

# COMMAND ----------

ph_join642_L = ph_unique641.join(
    ph_unique641_dups,
    on=[col("serial_number") == col("Right_SERIAL_NUMBER")],
    how="left",
).drop("Right_SERIAL_NUMBER")
ph_join642_I = ph_unique641.join(
    ph_unique641_dups,
    on=[col("serial_number") == col("Right_SERIAL_NUMBER")],
    how="inner",
).drop("Right_SERIAL_NUMBER")

ph_join642_Left = ph_join642_L.subtract(ph_join642_I)

# COMMAND ----------

ph_sumrz647 = (
    ph_join642_I.groupBy(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "FILING_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_DATE",
        "INSTITUTED_PRCD_NUM",
        "DECISION_DATE",
        "DECISION_CODE",
        "DECISION_PRCD_NUM",
        "DECISION_DESCRIPTION",
        "TERMINATION_CODE",
        "TERMINATION_PRCD_NUM",
        "TERMINATION_DATE",
        "TERMINATION_DATE_2",
        "TERMINATION_DATE_3",
        "TERMINATION_DATE_4",
        "TERMINATION_DATE_5",
        "CONSTRUCTED_PRCD_NUM",
    )
    .count()
    .drop("count")
    .orderBy(
        col("serial_number").asc(),
        col("INSTITUTED_DATE").asc(),
        col("INSTITUTED_PRCD_NUM").asc(),
        col("FILING_DATE").desc(),
    )
    .dropDuplicates(
        [
            "SERIAL_NUMBER",
            "TTAB_ISSUE_TYPE",
            "INSTITUTED_DATE",
            "DECISION_DATE",
            "TERMINATION_DATE",
            "INSTITUTED_PRCD_NUM",
        ]
    )
)

ph_order678 = ph_sumrz647.orderBy(
    col("serial_number").asc(),
    col("INSTITUTED_PRCD_NUM").asc(),
    col("FILING_DATE").asc(),
    col("INSTITUTED_DATE").desc(),
).dropDuplicates(["SERIAL_NUMBER", "FILING_DATE", "INSTITUTED_PRCD_NUM"])

ph_union650 = (
    ph_join642_Left.union(ph_order678)
    .orderBy(col("serial_number").asc(), col("INSTITUTED_DATE").asc())
    .withColumnRenamed("INSTITUTED_PRCD_NUM", "PROCEEDING_NUM")
    .drop("TERMINATION_PRCD_NUM")
    .drop("DECISION_PRCD_NUM")
)

ph_frm653 = (
    ph_union650.select(
        col("serial_number"),
        col("FILING_DATE").cast(DateType()),
        col("INSTITUTED_DATE").cast(DateType()),
        col("PROCEEDING_NUM"),
        col("TTAB_ISSUE_TYPE"),
        col("INSTITUTED_CODE"),
        col("DECISION_DATE").cast(DateType()),
        col("DECISION_CODE"),
        col("DECISION_DESCRIPTION"),
        col("TERMINATION_DATE").cast(DateType()),
        col("TERMINATION_CODE"),
        col("TERMINATION_DATE_2"),
        col("TERMINATION_DATE_3"),
        col("TERMINATION_DATE_4"),
        col("TERMINATION_DATE_5"),
        col("CONSTRUCTED_PRCD_NUM"),
    )
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
        "DECISION_YR",
        when(
            month(col("DECISION_DATE")) > 9, (year(col("DECISION_DATE")) + 1)
        ).otherwise(year(col("DECISION_DATE"))),
    )
    .withColumn(
        "TERM_YR",
        when(
            month(col("TERMINATION_DATE")) > 9, (year(col("TERMINATION_DATE")) + 1)
        ).otherwise(year(col("TERMINATION_DATE"))),
    )
)

# COMMAND ----------

ph_filter676 = ph_frm653.filter(
    (col("DECISION_DATE").isNotNull())
    & (col("TERMINATION_DATE").isNull())
    & (datediff(current_date(), col("DECISION_DATE")) > 63)
)
ph_frm686 = ph_filter676.withColumn("TERMINATION_CODE", lit("CANTT")).withColumn(
    "TERMINATION_DATE", date_add(col("DECISION_DATE"), 63)
)

ph_union688 = (
    ph_frm686.union(ph_filter676)
    .orderBy(col("serial_number").asc(), col("INSTITUTED_DATE").asc())
    .withColumn("PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("CANCELLATION", lit(True))
    .withColumn(
        "INVENTORY",
        when(
            col("INSTITUTED_DATE").isNotNull()
            & (col("DECISION_DATE") == "")
            & (col("TERMINATION_DATE") == ""),
            lit(True),
        ).otherwise(lit(False)),
    )
    .drop("FILED_YR")
    .drop("INST_YR")
    .drop("DECISION_YR")
    .drop("TERM_YR")
    .drop("PENDENCY_D")
    .drop("PENDENCY_T")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEFAULT FLAG - Cancellations

# COMMAND ----------

ph_join1101_I = ph_union688.join(
    filter_1091_T, on=[col("CONSTRUCTED_PRCD_NUM") == col("PROCEEDING")], how="inner"
)

ph_join1101_L = ph_union688.join(
    filter_1091_T, on=[col("CONSTRUCTED_PRCD_NUM") == col("PROCEEDING")], how="left"
)

ph_join1101_Left = ph_join1101_L.subtract(ph_join1101_I).withColumn(
    "DEFAULT_CANCELLATION", lit(False)
)

ph_sumrz1102 = ph_join1101_I.groupBy(
    col("SERIAL_NUMBER").alias("Right_SERIAL_NUMBER"),
    col("PROCEEDING_NUM").alias("Right_PROCEEDING_NUM"),
).agg(max("NOD_DATE").alias("Max_NOD_DATE"))

# COMMAND ----------

ph_join1103 = (
    ph_join1101_I.join(
        ph_sumrz1102,
        on=[
            col("SERIAL_NUMBER") == col("Right_SERIAL_NUMBER"),
            col("PROCEEDING_NUM") == col("Right_PROCEEDING_NUM"),
            col("NOD_DATE") == col("Max_NOD_DATE"),
        ],
        how="inner",
    )
    .drop("Right_SERIAL_NUMBER")
    .drop("Right_PROCEEDING_NUM")
    .drop("Max_NOD_DATE")
    .withColumn("DEFAULT_CANCELLATION", lit(True))
)

ph_union1104 = ph_join1101_Left.union(ph_join1103)

# COMMAND ----------

ph_select1107 = ph_union1104.select(
    col("SERIAL_NUMBER"),
    col("TTAB_ISSUE_TYPE"),
    col("PROCEEDING_NUM"),
    col("FILING_DATE"),
    col("INSTITUTED_DATE"),
    col("INSTITUTED_CODE"),
    col("DECISION_DATE"),
    col("DECISION_CODE"),
    col("DECISION_DESCRIPTION"),
    col("TERMINATION_CODE"),
    col("TERMINATION_DATE"),
    col("TERMINATION_DATE_2"),
    col("TERMINATION_DATE_3"),
    col("TERMINATION_DATE_4"),
    col("TERMINATION_DATE_5"),
    col("CONSTRUCTED_PRCD_NUM"),
    col("CANCELLATION"),
    col("INVENTORY"),
    col("NOD_DATE").alias("DEFAULT_DATE"),
    col("DEFAULT_CANCELLATION"),
)

# COMMAND ----------

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_select1107")
ph_select1107 = ph_select1107.checkpoint(True)
#ph_select1107.createOrReplaceGlobalTempView("ph_select1107")

# COMMAND ----------

# ph_select1107.count() Testing

# COMMAND ----------

# MAGIC %md
# MAGIC ## continued (CANCELLATIONS Updated 2/06/2020)

# COMMAND ----------

# ph_select1107
# input_cde
ph_join1061 = (
    ph_select1107.join(
        input_cde, on=[col("SERIAL_NUMBER") == col("SER_NUM")], how="left"
    )
    .drop("SER_NUM")
    .drop("Pendency_Cal_Start_DT")
    .withColumn(
        "FILING_FY",
        when(month(col("FILING_DATE")) > 9, (year(col("FILING_DATE")) + 1)).otherwise(
            year(col("FILING_DATE"))
        ),
    )
)

ph_sumrz1063 = (
    ph_join1061.groupBy("FILING_FY")
    .agg(_count("SERIAL_NUMBER").alias("Cancellation_Count"))
    .withColumnRenamed("FILING_FY", "Right_FILING_FY")
)

ph_join1066 = ph_join1061.join(
    ph_sumrz1063, on=[col("FILING_FY") == col("Right_FILING_FY")], how="left"
).drop("Right_FILING_FY")

# COMMAND ----------

# DBTITLE 1,Problematic_dataframes
# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_join1066")
# ph_join1066 = ph_join1066.checkpoint(True)
#ph_frm1068.createOrReplaceGlobalTempView("ph_frm1068")
ph_join1066 = ph_join1066.repartition(30)

# COMMAND ----------

ph_join1065 = ph_join1066.join(
    post_reg_mil_sumrz774, on=[col("FILING_FY") == col("REG_YR")], how="left"
)

ph_frm1068 = ph_join1065.withColumn(
    "CAN_RATE", col("Cancellation_Count") / col("LIVE_REG_COUNT")
)

# COMMAND ----------

ph_frm1068.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # CONCURRENT FILINGS

# COMMAND ----------

listValues2 = ["CU.AT", "CU.DT", "CU.GT", "CU.IT", "CU.MT", "CU.TT"]

ph_filter860 = ph_frm166.filter(col("five_Characters").isin(listValues2))
ph_filter859_T = ph_filter860.filter(col("five_Characters") == "CU.IT").dropDuplicates(
    ["serial_number", "ph_action_date", "cm_prcd_num"]
)
ph_filter859_F = (
    ph_filter860.filter(col("five_Characters") != "CU.IT")
    .dropDuplicates(
        ["serial_number", "ph_action_date", "cm_prcd_num", "five_Characters"]
    )
    .select(
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
    )
)

ph_join862_Left = (
    (
        ph_filter859_T.join(
            ph_filter859_F,
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

ph_join862_R = ph_filter859_T.join(
    ph_filter859_F, on=[col("serial_number") == col("Right_serial_number")], how="right"
)

ph_join862_I = ph_filter859_T.join(
    ph_filter859_F, on=[col("serial_number") == col("Right_serial_number")], how="inner"
)

ph_join862_Right = ph_join862_R.subtract(ph_join862_I)

# COMMAND ----------

ph_join909 = (
    ph_join862_Right.join(
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
    .groupBy(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "INSTITUTED_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_PRCD_NUM",
    )
    .count()
    .drop("count")
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
    .drop("DECISION_DESCRIPTION")
    .groupBy(
        "SERIAL_NUMBER",
        "INSTITUTED_DATE",
        "TERMINATION_DATE",
        "TERMINATION_CODE",
        "TERMINATION_PRCD_NUM",
    )
    .count()
    .drop("count")
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

# COMMAND ----------

ph_join884_L = ph_col881.join(
    ip_df_select921, on=[col("serial_number") == col("REF_SERIAL_NUMBER")], how="left"
).drop("REF_SERIAL_NUMBER")

ph_join884_I = ph_col881.join(
    ip_df_select921, on=[col("serial_number") == col("REF_SERIAL_NUMBER")], how="inner"
).drop("REF_SERIAL_NUMBER")

ph_join884_Left = ph_join884_L.subtract(ph_join884_I)

ph_sumrz885 = (
    ph_join884_I.groupBy(
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
    )
    .count()
    .drop("count")
)

ph_union886 = ph_join884_Left.union(ph_sumrz885)

# COMMAND ----------

ph_frm890 = ph_union886.withColumn(
    "FILING_DT_VALID",
    when(
        (col("FILING_DATE") <= col("INSTITUTED_DATE")) | (col("FILING_DATE").isNull()),
        lit(True),
    ).otherwise(lit(False)),
).filter(col("FILING_DT_VALID") == lit(True))
ph_filter893_T = ph_frm890.filter(col("FILING_DATE").isNotNull()).orderBy(
    col("serial_number").asc(),
    col("FILING_DATE").asc(),
    col("INSTITUTED_DATE").asc(),
    col("DECISION_DATE").asc(),
    col("TERMINATION_DATE").asc(),
)

ph_sample883 = ph_filter893_T.groupBy(
    "serial_number", "FILING_DATE", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM"
).agg(
    first("TTAB_ISSUE_TYPE").alias("TTAB_ISSUE_TYPE"),
    first("INSTITUTED_CODE").alias("INSTITUTED_CODE"),
    first("DECISION_DATE").alias("DECISION_DATE"),
    first("DECISION_CODE").alias("DECISION_CODE"),
    first("DECISION_PRCD_NUM").alias("DECISION_PRCD_NUM"),
    first("DECISION_DESCRIPTION").alias("DECISION_DESCRIPTION"),
    first("TERMINATION_DATE").alias("TERMINATION_DATE"),
    first("TERMINATION_CODE").alias("TERMINATION_CODE"),
    first("TERMINATION_PRCD_NUM").alias("TERMINATION_PRCD_NUM"),
    first("TERMINATION_DATE_2").alias("TERMINATION_DATE_2"),
    first("TERMINATION_DATE_3").alias("TERMINATION_DATE_3"),
    first("TERMINATION_DATE_4").alias("TERMINATION_DATE_4"),
    first("TERMINATION_DATE_5").alias("TERMINATION_DATE_5"),
    first("FILING_DT_VALID").alias("FILING_DT_VALID"),
)

ph_filter893_F = ph_frm890.filter(col("FILING_DATE").isNull())

ph_union894 = (
    ph_sample883.union(ph_filter893_F)
    .withColumn(
        "FILING_DATE",
        when(col("FILING_DATE") == "", col("INSTITUTED_DATE")).otherwise(
            col("FILING_DATE")
        ),
    )
    .dropDuplicates(["serial_number", "INSTITUTED_PRCD_NUM"])
    .orderBy(
        col("serial_number").asc(),
        col("INSTITUTED_DATE").asc(),
        col("INSTITUTED_PRCD_NUM").asc(),
    )
)

# COMMAND ----------

ph_sample939 = (
    ph_union894.groupBy(
        "serial_number",
        "TTAB_ISSUE_TYPE",
        "INSTITUTED_DATE",
        "DECISION_DATE",
        "TERMINATION_DATE",
        "INSTITUTED_PRCD_NUM",
    )
    .agg(
        first("FILING_DATE").alias("FILING_DATE"),
        first("INSTITUTED_CODE").alias("INSTITUTED_CODE"),
        first("DECISION_CODE").alias("DECISION_CODE"),
        first("DECISION_PRCD_NUM").alias("DECISION_PRCD_NUM"),
        first("DECISION_DESCRIPTION").alias("DECISION_DESCRIPTION"),
        first("TERMINATION_CODE").alias("TERMINATION_CODE"),
        first("TERMINATION_PRCD_NUM").alias("TERMINATION_PRCD_NUM"),
        first("TERMINATION_DATE_2").alias("TERMINATION_DATE_2"),
        first("TERMINATION_DATE_3").alias("TERMINATION_DATE_3"),
        first("TERMINATION_DATE_4").alias("TERMINATION_DATE_4"),
        first("TERMINATION_DATE_5").alias("TERMINATION_DATE_5"),
        first("FILING_DT_VALID").alias("FILING_DT_VALID"),
    )
    .orderBy(
        col("serial_number").asc(),
        col("INSTITUTED_PRCD_NUM").asc(),
        col("INSTITUTED_DATE").desc(),
        col("DECISION_DATE").asc(),
    )
)

ph_sample941 = (
    ph_sample939.groupBy("serial_number", "DECISION_DATE", "INSTITUTED_PRCD_NUM")
    .agg(
        first("TTAB_ISSUE_TYPE").alias("TTAB_ISSUE_TYPE"),
        first("FILING_DATE").alias("FILING_DATE"),
        first("INSTITUTED_CODE").alias("INSTITUTED_CODE"),
        first("INSTITUTED_DATE").alias("INSTITUTED_DATE"),
        first("DECISION_CODE").alias("DECISION_CODE"),
        first("DECISION_PRCD_NUM").alias("DECISION_PRCD_NUM"),
        first("DECISION_DESCRIPTION").alias("DECISION_DESCRIPTION"),
        first("TERMINATION_DATE").alias("TERMINATION_DATE"),
        first("TERMINATION_CODE").alias("TERMINATION_CODE"),
        first("TERMINATION_PRCD_NUM").alias("TERMINATION_PRCD_NUM"),
        first("TERMINATION_DATE_2").alias("TERMINATION_DATE_2"),
        first("TERMINATION_DATE_3").alias("TERMINATION_DATE_3"),
        first("TERMINATION_DATE_4").alias("TERMINATION_DATE_4"),
        first("TERMINATION_DATE_5").alias("TERMINATION_DATE_5"),
        first("FILING_DT_VALID").alias("FILING_DT_VALID"),
    )
    .withColumn(
        "INSTITUTED_PRCD_NUM",
        when(col("INSTITUTED_PRCD_NUM") == "", col("TERMINATION_PRCD_NUM")).otherwise(
            col("INSTITUTED_PRCD_NUM")
        ),
    )
)

# COMMAND ----------

ph_sort900 = (
    ph_sample941.orderBy(col("serial_number").asc(), col("INSTITUTED_DATE").asc())
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
        ).otherwise(year(col("INSTITUTED_DATE"))),
    )
    .withColumn(
        "DECISION_YR",
        when(
            month(col("TERMINATION_DATE")) > 9, (year(col("DECISION_DATE")) + 1)
        ).otherwise(year(col("DECISION_DATE"))),
    )
    .filter(
        col("DECISION_DATE").isNotNull()
        & col("TERMINATION_DATE").isNull()
        & (datediff(current_date(), col("DECISION_DATE")) > 63)
    )
)

# COMMAND ----------

ph_frm931 = ph_sort900.withColumn("TERMINATION_CODE", lit("CU.TT")).withColumn(
    "TERMINATION_DATE", date_add(col("DECISION_DATE"), 63)
)

ph_union933 = (
    ph_sort900.union(ph_frm931)
    .orderBy(col("serial_number").asc(), col("INSTITUTED_DATE").asc())
    .withColumn("PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("CONCURRENT", lit(True))
    .withColumn(
        "INVENTORY",
        when(
            (col("INSTITUTED_DATE").isNotNull())
            & (col("DECISION_DATE") == "")
            & (col("TERMINATION_DATE") == ""),
            lit(True),
        ).otherwise(False),
    )
)

# COMMAND ----------


# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_union933")
# ph_union933 = ph_union933.checkpoint(True)
#ph_union933.createOrReplaceGlobalTempView("ph_union933")

# COMMAND ----------

#

ph_join947 = (
    ph_union933.join(
        input_cde, on=[col("serial_number") == col("SER_NUM")], how="left"
    )
    .drop("SER_NUM")
    .drop("Pendency_Cal_Start_DT")
)

# COMMAND ----------


# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_ph_join947")
# ph_join947 = ph_join947.checkpoint(True)
#ph_join947.createOrReplaceGlobalTempView("ph_join947")

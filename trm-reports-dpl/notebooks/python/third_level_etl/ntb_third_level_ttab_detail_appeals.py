# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
ttab_scope = common_configs['secrets']['ttab_scope']

print(reporting_catalog,run_env,ttab_scope)

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,For testing purpose
# MAGIC %run ./ntb_ttab_detail_tbl_trm_dbx_input

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET FORM PARAGRAPH DATA ELEMENTS (Input)

# COMMAND ----------

# ip_df1_tbl_fact_fpep = spark.sql(f"""select * from {altrx_catalog}.{altrx_schema}.fpep_fact_calgary""")

edw_query1= "Select * from FP_WEBPAGE_LINK weblink"
df_ip_weblink = read_data_from_oracle_conn_dsu_cmn(edw_query1,edw_scope)

df_ip_fpep = spark.sql(f"select * from {reporting_catalog}.silver.fpep_fact")

ip_df1_tbl_fact_fpep = df_ip_fpep.join(df_ip_weblink, "FP_ID", "left")

fact_fpep_filter856 = ip_df1_tbl_fact_fpep.filter((col("GROUP_NAME") == "REFUSAL") | (col("TRANSACTIONAL_LITERAL") == "Examiner's Final Refusal")) \
    .groupBy("ser_num","completed_dt","category").agg(first("GROUP_NAME").alias("GROUP_NAME"), \
        first("transactional_literal").alias("transactional_literal"), \
            first("transaction_no").alias("transaction_no"), \
                first("action_count").alias("action_count"), \
                    first("fk_wrkr_id").alias("fk_wrkr_id"), \
                        first("fp_id").alias("fp_id"), \
                            first("title_tx").alias("title_tx"), \
                                first("fk_fp_group_id").alias("fk_fp_group_id"), \
                                    first("fk_fp_category_id").alias("fk_fp_category_id"), \
                                        # first("position_order_no").alias("position_order_no"), \
                                            first("fp_year").alias("fp_year")) \
                                                .withColumn("category",regexp_replace(trim(col("category")),"\s+"," "))
    
listCategories = ["Disclaimer - 2e1", "Disclaimer - 2e2", "Disclaimer - generic", "Disclaimer - other", "Distinctiveness - 5 years", "Distinctiveness - other", "Distinctiveness - prior regs", "Failure to Function - Character", "Failure to Function - Color Mark", "Failure to Function - Column", "Failure to Function - Configuration", "Failure to Function - Federal Statute", "Failure to Function - Functionality", "Failure to Function - Model/Grade", "Failure to Function - Ornamental", "Failure to Function - Other", "Failure to Function - Phantom", "Failure to Function - Process", "Failure to Function - Single Work", "Failure to Function - Trade Name", "Failure to Function - Varietal Name", "Generic - 2(e)(1)", "Generic - Supplemental", "Identification of Goods", "Recitation of Services", "Request for Information", "Section 2(a) - deceptive", "Section 2(a) - false connection", "Section 2(a) - wines and spirits", "Section 2(b) - Flags", "Section 2(d) - Likelihood of confusion", "Section 2(e)(1) - Descriptive", "Section 2(e)(1) - Misdescriptive", "Section 2(e)(2 ) - Geographic", "Section 2(e)(3) - Geographic deceptive", "Section 2(e)(4) - Surname", "Specimen -  no services shown", "Specimen - advertising for goods", "Specimen - copy of mark", "Specimen - other"]

fact_fpep_filter238 = fact_fpep_filter856.filter(fact_fpep_filter856.category.isin(listCategories)) \
    .withColumn("CATEGORIES",lit(None))


fact_fpep_cross233 = fact_fpep_filter238.groupBy("ser_num","completed_dt").pivot("CATEGORIES").agg(concat_ws(",",collect_list(col("category")))) \
    .withColumnRenamed("null","Form_Paragraphs_Used")

# bug fix: sort fp reasons

fact_fpep_cross233 = fact_fpep_cross233.withColumn(
    "Form_Paragraphs_Used", array_join(array_sort(split(col("Form_Paragraphs_Used"),',')), ',')
)

fact_fpep_col236 = fact_fpep_cross233.withColumn("Form_Paragraphs_1",split(col("Form_Paragraphs_Used"),",").getItem(0)) \
    .withColumn("Form_Paragraphs_2",split(col("Form_Paragraphs_Used"),",").getItem(1)) \
        .withColumn("Form_Paragraphs_3",split(col("Form_Paragraphs_Used"),",").getItem(2)) \
            .withColumn("Form_Paragraphs_4",split(col("Form_Paragraphs_Used"),",").getItem(3)) \
                .withColumn("Form_Paragraphs_5",split(col("Form_Paragraphs_Used"),",").getItem(4)) \
                    .withColumn("Form_Paragraphs_6",split(col("Form_Paragraphs_Used"),",").getItem(5)) \
                        .withColumn("Form_Paragraphs_7",split(col("Form_Paragraphs_Used"),",").getItem(6)) \
                            .withColumn("Form_Paragraphs_8",split(col("Form_Paragraphs_Used"),",").getItem(7)) \
                                .withColumn("Form_Paragraphs_9",split(col("Form_Paragraphs_Used"),",").getItem(8)) \
                                    .withColumn("Form_Paragraphs_10",split(col("Form_Paragraphs_Used"),",").getItem(9)) \
                                    #     .withColumn("Form_Paragraphs_11",split(col("Form_Paragraphs_Used"),",").getItem(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # GET EX PARTE APPEAL EVENTS: CLEANSE, ORGANIZE AND REFINE

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET FINAL REFUSAL DATE

# COMMAND ----------

sel_list = ["CNFR", "CNCF", "GNCF", "GNFN", "GNFR"]
ph_sumrz199 = input_ph.select(col("serial_number"),
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

# MAGIC %md
# MAGIC ## Continued 1 (GET EX PARTE APPEAL EVENTS: CLEANSE, ORGANIZE AND REFINE)

# COMMAND ----------

sel_list = ["EXART" ,"EXDAT" ,"EXDMT" ,"EXDRT" ,"EXFBT" ,"EXNIT" ,"EXPAT" ,"EXPIT" ,"EXPRT" ,"EXPTT" ,"EXRET" ,"EXRRT" ,"TTPDA"]

ph_filter196 = input_ph.filter(col("five_Characters").isin(sel_list))

# create two dataframes on 5thCharacter value "EXPIT"

win170T = Window().partitionBy("serial_number","ph_action_date").orderBy("ph_action_number")
win170F = Window().partitionBy("serial_number","ph_action_date","five_Characters").orderBy("ph_action_number")

#ph_filter170_T = ph_filter196.filter(col("five_Characters") == "EXPIT").dropDuplicates(["serial_number","ph_action_date"])
ph_filter170_T = ph_filter196.filter(col("five_Characters") == "EXPIT").withColumn("rn", row_number().over(win170T)).filter(col("rn") == 1).drop("rn")

# ph_filter170_F = ph_filter196.filter(col("five_Characters") != "EXPIT").dropDuplicates(["serial_number","ph_action_date","five_Characters"]) \
#     .select(col("serial_number").alias("Right_serial_number"),
#             col("ph_action_number").alias("Right_ph_action_number"),
#             col("ph_action_code").alias("Right_ph_action_code"),
#             col("cm_sys_dt").alias("Right_cm_sys_dt"),
#             col("ph_action_date").alias("Right_ph_action_date"),
#             col("last_modified_date").alias("Right_last_modified_date"),
#             col("oracle_apply_time").alias("Right_oracle_apply_time"),
#             col("cm_prcd_num").alias("Right_cm_prcd_num"),
#             col("ri_notif_dt").alias("Right_ri_notif_dt"),
#             col("cm_desc").alias("Right_cm_desc"),
#             col("fifth_char_cm_type").alias("Right_fifth_char_cm_type"),
#             col("cm_flg_paper").alias("Right_cm_flg_paper"),
#             col("ttab_tracking_num").alias("Right_ttab_tracking_num"),
#             col("tm_worker_eid").alias("Right_tm_worker_eid"),
#             col("five_Characters").alias("Right_five_Characters"),
#             col("year").alias("Right_year"))

ph_filter170_F = ph_filter196.filter(col("five_Characters") != "EXPIT").withColumn("rn", row_number().over(win170F)).filter(col("rn") == 1).drop("rn") \
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

ph_filter183 = ph_join175.filter(col("INSTITUTED_CODE") == "EXPIT") \
    .select(col("serial_number"),
            col("TTAB_ISSUE_TYPE"),
            col("INSTITUTED_CODE"),
            col("INSTITUTED_DATE"),
            col("INSTITUTED_PRCD_NUM"))

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

#########################################################################################################
# HAD TO ADD ORDER ON INSTITUTED_DATE, FILING_DATE, DECISION_DATE, DECISION_CODE FOR CONSISTENT RESULTS #
# FOLLOW UP QUESTION FOR EACH - ASC OR DESC???                                                          #
#########################################################################################################

win473 = Window().partitionBy("serial_number", "PROCEEDING_NUM").orderBy(col("INSTITUTED_DATE").desc(), col("FILING_DATE").desc(),col("DECISION_DATE").desc(), "DECISION_CODE")

ph_unique473_U = ph_frm471.withColumn("rn", row_number().over(win473)).filter(col("rn") == 1).drop("rn")
ph_unique473_D = ph_frm471.withColumn("rn", row_number().over(win473)).filter(col("rn") > 1).drop("rn").select("serial_number").distinct()

ph_join474 = ph_frm471.join(ph_unique473_D, "serial_number")

# COMMAND ----------

### bug fix: don't need inner - left; just do left anti join

ph_join488 = ph_unique473_U.join(ph_unique473_D, "serial_number", "anti").withColumn(
    "TERM_DATES", lit(None)
).withColumn(
    "TERM_DATE", col("TERMINATION_DATE")
)

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

ph_cross489 = ph_cross489.withColumn(
    "null", array_join(array_sort(split(col("null"),',')), ',')
)

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

ph_col492 = ph_col492.withColumn("TERMINATION_DATE", when(col("TERMINATION_DATE") == "", lit(None)).otherwise(col("TERMINATION_DATE")))

# COMMAND ----------

ph_filter183_T = ph_join474.filter(col("DECISION_DATE").isNotNull())

###########################################################################################
# HAD TO ADD ORDER ON FILING_DATE, DECISION_CODE, TERMINATION_DATE FOR CONSISTENT RESULTS #
# FOLLOW UP QUESTION FOR EACH - ASC OR DESC???                                            #
###########################################################################################

win484 = Window().partitionBy("serial_number", "TTAB_ISSUE_TYPE", "PROCEEDING_NUM").orderBy(col("INSTITUTED_DATE").desc(), col("DECISION_DATE").desc(), col("FILING_DATE").desc(), "DECISION_CODE", col("TERMINATION_DATE").desc())

ph_sample484 = ph_filter183_T.withColumn("rn", row_number().over(win484)).filter(col("rn") == 1).drop("rn")

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

ph_cross477 = ph_cross477.withColumn(
    "null", array_join(array_sort(split(col("null"),',')), ',')
)

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

ph_col480 = ph_col480.withColumn("TERMINATION_DATE", when(col("TERMINATION_DATE") == "", lit(None)).otherwise(col("TERMINATION_DATE")))

ph_union494 = (
    ph_col492.unionByName(ph_col480, allowMissingColumns=True)
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
            (col("APPEAL") == 1) & ((col("FINAL_REFUSAL_DATE") == "") | (col("FINAL_REFUSAL_DATE").isNull())),
            date_add(col("FILING_DATE"), -180),
        ).otherwise(col("FINAL_REFUSAL_DATE")),
    )
    .withColumn(
        "INVENTORY",
        when(
            (col("INSTITUTED_DATE").isNotNull())
            & ((col("DECISION_DATE") == "") | col("DECISION_DATE").isNull())
            & ((col("TERMINATION_DATE") == "") | col("TERMINATION_DATE").isNull()),
            lit(True),
        ).otherwise(lit(False)),
    )
    .withColumn("PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_R", lit(None))
    .withColumn(
        "FP_REASON_1", when(((col("FP_REASON_1") == "") | col("FP_REASON_1").isNull()), "Unidentified/Not Captured").otherwise(col("FP_REASON_1"))
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
) 

ph_join1082_left = ph_filter1081_T.join(
    ph_select711, "serial_number", "anti"
)

ph_join1082_inner = ph_filter1081_T.join(
    ph_select711, "serial_number", "inner"
)

ph_union1085_1 = ph_filter1081_F.unionByName(ph_join1082_left, allowMissingColumns=True)

# COMMAND ----------

# bug fix: replace filter + union with when conditions
# bug fix: also was using wrong df, it's inner not left

ph_filter1084 = ph_join1082_inner.withColumn(
    "TERMINATION_CODE", when(col("PUBLICATION_DATE") > col("INSTITUTED_DATE"), lit("EXPTT")).otherwise(col("TERMINATION_CODE"))
).withColumn(
    "TERMINATION_DATE", when(col("PUBLICATION_DATE") > col("INSTITUTED_DATE"), col("PUBLICATION_DATE")).otherwise(col("TERMINATION_DATE"))
)

ph_union1085 = (
    ph_union1085_1.unionByName(ph_filter1084, allowMissingColumns=True)
    .withColumn(
        "FP_REASON_1",
        when(((col("FP_REASON_1") == "") | col("FP_REASON_1").isNull()), "Unidentified/Not Captured").otherwise(
            col("FP_REASON_1")
        ),
    )
    .withColumn(
        "INVENTORY",
        when(
            (col("INSTITUTED_DATE").isNotNull())
            & ((col("DECISION_DATE") == "") | col("DECISION_DATE").isNull())
            & ((col("TERMINATION_DATE") == "") | col("TERMINATION_DATE").isNull()),
            lit(True),
        ).otherwise(lit(False)),
    )
)

# COMMAND ----------

ph_union1085 = ph_union1085.select('serial_number',
 'TTAB_ISSUE_TYPE',
 'PROCEEDING_NUM',
 'FINAL_REFUSAL_DATE',
 'FILING_DATE',
 'INSTITUTED_CODE',
 'INSTITUTED_DATE',
 'DECISION_DATE',
 'DECISION_CODE',
 'DECISION_DESCRIPTION',
 'TERMINATION_CODE',
 'FP_REASON_1',
 'FP_REASON_2',
 'FP_REASON_3',
 'FP_REASON_4',
 'FP_REASON_5',
 'TERMINATION_DATE',
 'TERMINATION_DATE_2',
 'TERMINATION_DATE_3',
 'TERMINATION_DATE_4',
 'TERMINATION_DATE_5',
 'APPEAL',
 'INVENTORY',
 'PENDENCY_D',
 'PENDENCY_T',
 'PENDENCY_R',
 'PUBLICATION_DATE')

# COMMAND ----------

ph_union1085.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.ttab_detail_appeals")

# COMMAND ----------

appeals = spark.sql(f"select * from {reporting_catalog}.silver.ttab_detail_appeals").drop("PENDENCY_R")

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
    appeals.join(
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

# set column ordering
ph_frm788 = ph_frm788.select('serial_number',
 'TTAB_ISSUE_TYPE',
 'PROCEEDING_NUM',
 'FINAL_REFUSAL_DATE',
 'FILING_DATE',
 'INSTITUTED_CODE',
 'INSTITUTED_DATE',
 'DECISION_DATE',
 'DECISION_CODE',
 'DECISION_DESCRIPTION',
 'TERMINATION_CODE',
 'FP_REASON_1',
 'FP_REASON_2',
 'FP_REASON_3',
 'FP_REASON_4',
 'FP_REASON_5',
 'TERMINATION_DATE',
 'TERMINATION_DATE_2',
 'TERMINATION_DATE_3',
 'TERMINATION_DATE_4',
 'TERMINATION_DATE_5',
 'APPEAL',
 'INVENTORY',
 'PENDENCY_D',
 'PENDENCY_T',
 'PUBLICATION_DATE',
 'REFUSAL',
 'NON_PRO_SE',
 'TEST_PCTRAM_LINK',
 'LAW_OFFICE',
 'FILING_BASIS_GRP',
 'FILING_METHOD_CUR',
 'AM_STAT',
 'Owner_Name',
 'CITY',
 'STATE',
 'Country_or_Area_Name',
 'Reg_Class_Count',
 'Active_Class_Count',
 'Group_Type',
 'Concat_Class',
 'MARK_NM_SHORT')

# COMMAND ----------

ph_frm788.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.ttab_detail_appeals_1") 

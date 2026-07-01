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
# MAGIC # OPPOSITIONS

# COMMAND ----------

listValues = ["OP.IT", "OP.DT", "OP.ST", "TTODP", "OP.TT", "OP.NT", "TTPDA"]

ph_filter250 = input_ph.filter(col("five_Characters").isin(listValues))
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

ph_filter561 = (
    ph_union500.filter((col("DECISION_CODE") != "OP.TT") | (col("DECISION_CODE").isNull()))
    .withColumnRenamed("serial_number", "Right_serial_number")
    .withColumnRenamed("INSTITUTED_DATE", "Right_INSTITUTED_DATE")
    .withColumnRenamed("INSTITUTED_CODE", "Right_INSTITUTED_CODE")
    .withColumnRenamed("INSTITUTED_PRCD_NUM", "Right_INSTITUTED_PRCD_NUM")
)

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

# COMMAND ----------

ph_frm545 = (
    ph_join505_Left.filter(col("SERIAL_NUMBER").isNotNull())
    .withColumn("TERM_DATES", lit(None))
    .withColumn("TERM_DATE", col("TERMINATION_DATE"))
)

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

# sort based on termination dt
ph_cross546 = ph_cross546.withColumn(
    "null", array_join(array_sort(split(col("null"),',')), ',')
)

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
)

ph_union538 = ph_union538.withColumn("TERMINATION_DATE", when(col("TERMINATION_DATE") == "", lit(None)).otherwise(col("TERMINATION_DATE")))

# COMMAND ----------

## bug fix: prior logic was completely wrong, need to add window function

######################################################################################################
# THIS IS WHERE WE START TO GET MISMATCHES WITH ALTERYX BECAUSE THE GROUPBY / SORT IS NON-EXHAUSTIVE #
######################################################################################################
# NEED AN ADDITIONAL ORDER BY ON DECISION_CODE - QUESTION: ASC OR DESC??? #
###########################################################################

win1039 = Window().partitionBy("serial_number", "FILING_DATE", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM").orderBy(col("DECISION_DATE").desc(), col("TERMINATION_DATE").desc(), "DECISION_CODE")

ph_sample1039 = ph_union538.withColumn("rn", row_number().over(win1039)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------

##############################################################
# HAD TO ADD ORDER ON INSTITUTED_DATE FOR CONSISTENT RESULTS #
# FOLLOW UP QUESTION - ASC OR DESC???                        #
##############################################################

win532 = Window().partitionBy("serial_number", "INSTITUTED_PRCD_NUM").orderBy(col("INSTITUTED_DATE").desc())

ph_unique532_U = ph_sample1039.withColumn("rn", row_number().over(win532)).filter(col("rn") == 1).drop("rn")
ph_unique532_D = ph_sample1039.withColumn("rn", row_number().over(win532)).filter(col("rn") > 1).drop("rn").select("serial_number").distinct()

ph_join531_I = ph_unique532_U.join(ph_unique532_D, "serial_number")

ph_join531_L = ph_unique532_U.join(ph_unique532_D, "serial_number", "anti")

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

ph_union525 = (
    ph_join531_L.unionByName(ph_sample516, allowMissingColumns=True)
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

# bug fix: replace filter with when condition

ph_union513 = ph_union525.withColumn(
    "TERMINATION_CODE", when((col("DECISION_DATE").isNotNull())
    & (col("TERMINATION_DATE").isNull())
    & (datediff(current_date(), col("DECISION_DATE")) > 63), lit("CANTT")).otherwise(col("TERMINATION_CODE"))
).withColumn(
    "TERMINATION_DATE", when((col("DECISION_DATE").isNotNull())
    & (col("TERMINATION_DATE").isNull())
    & (datediff(current_date(), col("DECISION_DATE")) > 63), date_add(col("DECISION_DATE"), 63)).otherwise(col("TERMINATION_DATE"))
)

# COMMAND ----------

##############################################################################
# ADDED DESC SORT ON TERMINATION DATE - SORT IS NON-DETERMINISTIC WITHOUT IT #
##############################################################################

win1019 = Window().partitionBy(
    "serial_number", "PROCEEDING_NUM"
).orderBy(
    col("FILING_DATE").desc(), 
    col("INSTITUTED_DATE").desc(),
    col("DECISION_DATE").desc(),
    col("TERMINATION_DATE").desc()
)

union582 = ph_join579.unionByName(ph_union513, allowMissingColumns=True)

sample1019 = union582.withColumn("rn", row_number().over(win1019)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------

ph_sumrz1021 = (
    sample1019.select(
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
    ).distinct()
    .withColumn("PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("OPPOSITION", lit(True))
    .withColumn(
        "INVENTORY",
        when(
            col("INSTITUTED_DATE").isNotNull()
            & ((col("DECISION_DATE") == "") | col("DECISION_DATE").isNull())
            & ((col("TERMINATION_DATE") == "") | col("TERMINATION_DATE").isNull()),
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
    filter_1091_F, on=[col("CONSTRUCTED_PRCD_NUM") == col("PROCEEDING")], how="anti"
).withColumn(
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

ph_union1111 = ph_join1110.unionByName(ph_join1108_L,allowMissingColumns=True)

# COMMAND ----------

ph_select1113 = ph_union1111.select(
    'SERIAL_NUMBER',
    'TTAB_ISSUE_TYPE',
    'PROCEEDING_NUM',
    'FILING_DATE',
    'INSTITUTED_DATE',
    'INSTITUTED_CODE',
    'DECISION_DATE',
    'DECISION_CODE',
    'DECISION_DESCRIPTION',
    'TERMINATION_CODE',
    'TERMINATION_DATE',
    'TERMINATION_DATE_2',
    'TERMINATION_DATE_3',
    'TERMINATION_DATE_4',
    'TERMINATION_DATE_5',
    'CONSTRUCTED_PRCD_NUM',
    'OPPOSITION',
    'INVENTORY',
    col("NOD_DATE").alias("DEFAULT_DATE"),
    "DEFAULT_OPPOSITION"
)

# COMMAND ----------

ph_select1113.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.ttab_detail_oppositions")

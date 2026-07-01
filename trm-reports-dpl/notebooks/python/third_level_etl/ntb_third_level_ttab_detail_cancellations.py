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
# MAGIC # CANCELLATIONS Updated 2/06/2020

# COMMAND ----------

ph_filter604_T = input_ph.filter(col("five_Characters") == "PETCT").dropDuplicates(
    ["serial_number", "ph_action_date", "cm_prcd_num"]
)

ph_filter604_F = (
    input_ph.filter(col("five_Characters").isin("CANDT", "CANGT", "TTCDP", "CANTT"))
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

## bug fix: replace subtract with anti join

ph_join615_Right = ph_filter604_F.join(
    ph_filter604_T, on=[col("serial_number") == col("Right_serial_number")], how="anti"
).select(
    col("Right_serial_number").alias("serial_number"),
    col("Right_ph_action_date").alias("ph_action_date"),
    col("Right_cm_prcd_num").alias("cm_prcd_num"),
    col("Right_cm_desc").alias("cm_desc"),
    col("Right_five_Characters").alias("five_Characters")
)

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
    .select(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "INSTITUTED_DATE",
        "INSTITUTED_CODE",
        "INSTITUTED_PRCD_NUM",
    ).distinct()
)

#bug fix: add isNull to or clause
ph_filter612 = (
    ph_union666.filter((col("DECISION_CODE") != "CANTT") | col("DECISION_CODE").isNull())
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

ph_cross624 = ph_cross624.withColumn(
    "null", array_join(array_sort(split(col("null"),',')), ',')
)

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
        when(((col("INSTITUTED_PRCD_NUM") == "") | col("INSTITUTED_PRCD_NUM").isNull()), col("DECISION_PRCD_NUM")).otherwise(
            col("INSTITUTED_PRCD_NUM")
        ),
    )
)

ph_col627 = ph_col627.withColumn("TERMINATION_DATE", when(col("TERMINATION_DATE") == "", lit(None)).otherwise(col("TERMINATION_DATE")))

# COMMAND ----------

# bug fix: need or is null

ph_frm1049 = ph_col627.withColumn(
    "INSTITUTED_PRCD_NUM",
    when(((col("INSTITUTED_PRCD_NUM") == "") | col("INSTITUTED_PRCD_NUM").isNull()), col("DECISION_PRCD_NUM")).otherwise(
        col("INSTITUTED_PRCD_NUM")
    ),
).withColumn(
    "INSTITUTED_PRCD_NUM",
    when(((col("INSTITUTED_PRCD_NUM") == "") | col("INSTITUTED_PRCD_NUM").isNull()), col("TERMINATION_PRCD_NUM")).otherwise(
        col("INSTITUTED_PRCD_NUM")
    ),
)

# bug fix: logic wrong

ph_frm1035 = ph_frm1049.withColumn(
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
    .select(
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
    ).distinct()
)

# bug fix: replace left + sub with anti

ph_join630_L = (
    ph_frm1035.join(
        ip_df_filter1031_CAN,
        on=[
            col("serial_number") == col("REF_SERIAL_NUMBER"),
            col("CONSTRUCTED_PRCD_NUM") == col("FK_PROCEEDINGNUMBER0"),
        ],
        how="anti",
    )
    .drop("TYPE")
    .drop("FK_PROCEEDINGNUMBER0")
    .drop("REF_SERIAL_NUMBER")
)

# ph_join630_Left = ph_join630_L.subtract(ph_join630_I)

ph_union632 = (
    ph_join630_L.unionByName(ph_join630_I, allowMissingColumns=True)
    .withColumn(
        "FILING_DATE",
        when(col("FILING_DATE").isNull(), col("INSTITUTED_DATE")).otherwise(
            col("FILING_DATE")
        ),
    )
)

# COMMAND ----------

# bug fix: logic completely wrong

######################################################################################################
# THIS IS WHERE WE START TO GET MISMATCHES WITH ALTERYX BECAUSE THE GROUPBY / SORT IS NON-EXHAUSTIVE #
######################################################################################################
# NEED AN ADDITIONAL ORDER BY ON DECISION_CODE - QUESTION: ASC OR DESC??? #
###########################################################################

win1036 = Window().partitionBy("serial_number", "FILING_DATE", "INSTITUTED_DATE", "INSTITUTED_PRCD_NUM").orderBy(col("DECISION_DATE").desc(), col("TERMINATION_DATE").desc(), "DECISION_CODE")

ph_sample1036 = ph_union632.withColumn("rn", row_number().over(win1036)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------

##############################################################
# HAD TO ADD ORDER ON INSTITUTED_DATE FOR CONSISTENT RESULTS #
# FOLLOW UP QUESTION - ASC OR DESC???                        #
##############################################################

win641 = Window().partitionBy("serial_number", "INSTITUTED_PRCD_NUM").orderBy(col("INSTITUTED_DATE").desc())

ph_unique641_U = ph_sample1036.withColumn("rn", row_number().over(win641)).filter(col("rn") == 1).drop("rn")
ph_unique641_D = ph_sample1036.withColumn("rn", row_number().over(win641)).filter(col("rn") > 1).drop("rn").select("serial_number").distinct()

# COMMAND ----------

ph_join642_L = ph_unique641_U.join(
    ph_unique641_D,
    "serial_number",
    "anti"
)

ph_join642_I = ph_unique641_U.join(
    ph_unique641_D,
    "serial_number",
    "inner"
)

# COMMAND ----------

ph_sumrz647 = (
    ph_join642_I.select(
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
    ).distinct()
)

win648 = Window().partitionBy("SERIAL_NUMBER",
            "TTAB_ISSUE_TYPE",
            "INSTITUTED_DATE",
            "DECISION_DATE",
            "TERMINATION_DATE",
            "INSTITUTED_PRCD_NUM").orderBy(col("FILING_DATE").desc())

sample648 = ph_sumrz647.withColumn("rn", row_number().over(win648)).filter(col("rn") == 1).drop("rn")

win679 = Window().partitionBy("SERIAL_NUMBER", "FILING_DATE", "INSTITUTED_PRCD_NUM").orderBy(col("INSTITUTED_DATE").desc())

sample679 = sample648.withColumn("rn", row_number().over(win679)).filter(col("rn") == 1).drop("rn")

ph_union650 = (
    ph_join642_L.unionByName(sample679)
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

# bug fix: replace filter + union with conditional column

ph_union688_1 = ph_frm653.withColumn(
    "TERMINATION_CODE", when((col("DECISION_DATE").isNotNull())
    & (col("TERMINATION_DATE").isNull())
    & (datediff(current_date(), col("DECISION_DATE")) > 63), lit("CANTT")).otherwise(col("TERMINATION_CODE"))
).withColumn(
    "TERMINATION_DATE", when((col("DECISION_DATE").isNotNull())
    & (col("TERMINATION_DATE").isNull())
    & (datediff(current_date(), col("DECISION_DATE")) > 63), date_add(col("DECISION_DATE"), 63)).otherwise(col("TERMINATION_DATE"))
)

ph_union688 = (
    ph_union688_1
    .withColumn("CANCELLATION", lit(True))
    .withColumn(
        "INVENTORY",
        when(
            (col("INSTITUTED_DATE").isNotNull()
            & (col("DECISION_DATE").isNull() | (col("DECISION_DATE") == ""))
            & (col("TERMINATION_DATE").isNull() | (col("TERMINATION_DATE") == ""))),
            lit(True),
        ).otherwise(lit(False)),
    )
    .drop("FILED_YR")
    .drop("INST_YR")
    .drop("DECISION_YR")
    .drop("TERM_YR")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEFAULT FLAG - Cancellations

# COMMAND ----------

ph_join1101_I = ph_union688.join(
    filter_1091_T, on=[col("CONSTRUCTED_PRCD_NUM") == col("PROCEEDING")], how="inner"
)

ph_join1101_L = ph_union688.join(
    filter_1091_T, on=[col("CONSTRUCTED_PRCD_NUM") == col("PROCEEDING")], how="anti"
).withColumn(
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

ph_union1104 = ph_join1101_L.unionByName(ph_join1103, allowMissingColumns=True)

# COMMAND ----------

# set column ordering and rename
ph_select1107 = ph_union1104.select(
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
    'CANCELLATION',
    'INVENTORY',
    col("NOD_DATE").alias("DEFAULT_DATE"),
    'DEFAULT_CANCELLATION'
)

# COMMAND ----------

ph_select1107.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.ttab_detail_cancellations")

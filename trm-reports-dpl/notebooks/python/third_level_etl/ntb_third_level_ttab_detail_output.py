# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

if dbx_env.lower() == 'prod':
    ttab_env = 'P'
else:
    ttab_env = 'D'

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
ttab_scope = common_configs['secrets']['ttab_scope']
edw_scope = common_configs['secrets']['edw_scope']
print(reporting_catalog,run_env,ttab_scope)

# COMMAND ----------

job_name = 'ntb_third_level_form_paragraph_dashboard_etl_code'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %run ./ntb_ttab_detail_tbl_trm_dbx_input

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input: Get RFD Dates

# COMMAND ----------

ttab_query6 = "select PH.ENTRY_DATE, PH.FK_PROCEEDINGNUMBER0,PH.FK_PROCEEDINGTYPE from prosecution_history_event PH where (PH.ENTRY_CODE = 793) or (PH.ENTRY_CODE = 646) or (PH.ENTRY_CODE = 792 and PH.TEXT = 'SUBMITTED ON BRIEF ACR')"

ip_df_1008 = read_data_from_oracle_conn_dsu_cmn(ttab_query6,ttab_scope)

# COMMAND ----------

select_1004 = ip_df_1008.withColumn(
    "FK_PROCEEDINGNUMBER0", col("FK_PROCEEDINGNUMBER0").cast(StringType())
).groupBy(
    col("FK_PROCEEDINGNUMBER0"), col("FK_PROCEEDINGTYPE")
).agg(
    max("ENTRY_DATE").alias("RFD_DATE")
).withColumn(
    "RFD_DATE",col("RFD_DATE").cast(DateType())
).withColumn(
    "RFD_FY", when(month(col("RFD_DATE")) > 9, (year(col("RFD_DATE")) + 1)).otherwise(year(col("RFD_DATE")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input: Read Intermediate Inputs

# COMMAND ----------

# need to standardize these table names

appeals = spark.sql(f"select * from {reporting_catalog}.silver.ttab_detail_appeals")
oppositions = spark.sql(f"select * from {reporting_catalog}.silver.ttab_detail_oppositions")
cancellations = spark.sql(f"select * from {reporting_catalog}.silver.ttab_detail_cancellations")
concurrent_filings = spark.sql(f"select * from {reporting_catalog}.silver.ttab_detail_concurrent_filings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET RFD Dates

# COMMAND ----------

# select_1004

union701 = appeals.unionByName(
    oppositions, allowMissingColumns=True
).unionByName(
    cancellations, allowMissingColumns=True
).unionByName(
    concurrent_filings, allowMissingColumns=True
).drop("PENDENCY_R") # drop void column to avoid issues

# COMMAND ----------

filter1137_T = union701.filter(col("TTAB_ISSUE_TYPE") == "EX PARTE APPEAL").withColumn(
    "PROCEEDING_NUM",
    when(length(col("PROCEEDING_NUM")) == 2, concat(lit("0000"), col("PROCEEDING_NUM")))
    .when(length(col("PROCEEDING_NUM")) == 3, concat(lit("000"), col("PROCEEDING_NUM")))
    .when(length(col("PROCEEDING_NUM")) == 4, concat(lit("00"), col("PROCEEDING_NUM")))
    .when(length(col("PROCEEDING_NUM")) == 5, concat(lit("0"), col("PROCEEDING_NUM")))
    .otherwise(col("PROCEEDING_NUM")),
).withColumn(
    "PROCEEDING_NUM",
    when(col("PROCEEDING_NUM") == lit("0"), col("SERIAL_NUMBER")).otherwise(
        col("PROCEEDING_NUM")
    ),
)



filter1137_F = union701.filter(col("TTAB_ISSUE_TYPE") != "EX PARTE APPEAL").withColumn(
    "proceedingnumber0", expr("""
        case 
        when TTAB_ISSUE_TYPE = 'OPPOSITION'
        then
            case
            when PROCEEDING_NUM = '0'
            then 0
            when length(PROCEEDING_NUM) = 5
            then '910' || PROCEEDING_NUM
            else '91' || PROCEEDING_NUM
            end
        when TTAB_ISSUE_TYPE = 'CANCELLATION'
        then
            case
            when PROCEEDING_NUM = '0'
            then 0
            when length(PROCEEDING_NUM) = 5
            then '920' || PROCEEDING_NUM
            else '92' || PROCEEDING_NUM
            end
        else "not picked up"
        end                                       
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bug fix: replace logic of creating 10 different datasets and then union; instead set array of prefixes needed and then explode

# COMMAND ----------

prefix_arr_l5 = [740, 750, 760, 770, 780, 790, 850, 860, 870, 880]
prefix_arr_l6 = [74, 75, 76, 77, 78, 79, 85, 86, 87, 88]

# COMMAND ----------

# split into two dataframes based upon proceeding num length

rfd_l5 = filter1137_T.filter(col("PROCEEDING_NUM") != 0).filter(length(col("PROCEEDING_NUM")) == 5)
rfd_l6 = filter1137_T.filter(col("PROCEEDING_NUM") != 0).filter(length(col("PROCEEDING_NUM")) != 5)

# COMMAND ----------

# explode dataframes with array of prefixes then concatenate

rfd_l5_exp = rfd_l5.withColumn(
    "prefix", explode(array([lit(x) for x in prefix_arr_l5]))
).withColumn(
    "appealproceednumt1", concat("prefix", "PROCEEDING_NUM")
).drop("prefix")

rfd_l6_exp = rfd_l6.withColumn(
    "prefix", explode(array([lit(x) for x in prefix_arr_l6]))
).withColumn(
    "appealproceednumt1", concat("prefix", "PROCEEDING_NUM")
).drop("prefix")

# COMMAND ----------

join_rfd_l5 = rfd_l5_exp.join(select_1004, on=[col("FK_PROCEEDINGNUMBER0") == col("appealproceednumt1")], how="inner")

join_rfd_l6 = rfd_l6_exp.join(select_1004, on=[col("FK_PROCEEDINGNUMBER0") == col("appealproceednumt1")], how="inner")

union988 = join_rfd_l5.unionByName(join_rfd_l6, allowMissingColumns=True)

# COMMAND ----------

filter1134_T = union988.filter(substring(col("FK_PROCEEDINGNUMBER0"),3,9) == substring(col("SERIAL_NUMBER"),3,9)) 

filter1134_F = union988.filter(substring(col("FK_PROCEEDINGNUMBER0"),3,9) != substring(col("SERIAL_NUMBER"),3,9))

# COMMAND ----------

frm1132_1 = filter1134_T.withColumn("PRCD_2CHARS",substring(col("FK_PROCEEDINGNUMBER0"),0,2)) \
        .withColumn("SER_2CHARS",substring(col("SERIAL_NUMBER"),0,2))

frm1132 = frm1132_1.withColumn(
        "RFD_DATE", when(col("PRCD_2CHARS") != col("SER_2CHARS"),lit(None)).otherwise(col("RFD_DATE"))
).withColumn(
        "RFD_FY", when(col("PRCD_2CHARS") != col("SER_2CHARS"),lit(None)).otherwise(col("RFD_FY"))
).withColumn(
        "CONSTRUCTED_PRCD_NUM",col("SERIAL_NUMBER")
)

frm1131 = filter1134_F.withColumn("CONSTRUCTED_PRCD_NUM",col("FK_PROCEEDINGNUMBER0"))

# COMMAND ----------

union1130 = frm1131.unionByName(frm1132, allowMissingColumns=True).drop(
    "FK_PROCEEDINGNUMBER0", 
    "FK_PROCEEDINGTYPE", 
    "RFD_FY", 
    "appealproceednumt1", 
    "PRCD_2CHARS",
    "SER_2CHARS")

# COMMAND ----------

ph_join955= select_1004.join(filter1137_F,
             on = [col("FK_PROCEEDINGNUMBER0") == col("proceedingnumber0")],
             how = "inner")

# COMMAND ----------

union989 = union1130.unionByName(ph_join955, allowMissingColumns=True)

# COMMAND ----------

select994 = union989.groupBy(
    "serial_number","TTAB_ISSUE_TYPE","PROCEEDING_NUM"
).agg(
    max("RFD_DATE").alias("RFD_DATE"), max("RFD_FY").alias("RFD_FY")
)

# COMMAND ----------

ph_join992 = union701.join(
    select994,
    on=["serial_number", "TTAB_ISSUE_TYPE", "PROCEEDING_NUM"],
    how="left",
)

# COMMAND ----------

filter1139_T = ph_join992.filter(col("RFD_Date").isNotNull() & col("DECISION_DATE").isNotNull()).withColumn(
    "RFD_Valid", when(datediff(col("DECISION_DATE"),col("RFD_Date")) >= 0, lit(True)).otherwise(lit(False))
)

filter1139_F = ph_join992.filter(col("RFD_Date").isNull() | col("DECISION_DATE").isNull())

# COMMAND ----------

union1140 = filter1139_T.unionByName(filter1139_F,allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFD Date Validation and Missing PATCH From Judge Log:

# COMMAND ----------

ttab_query7 = f"select * from TTAB{ttab_env}.TTAB_PANEL_INFO order by TTAB_PANEL_INFO.FK_PROCEEDING_NUMBER0"
ttab_query8 = "select p.NUMBER0, p.TYPE, pr.REF_SERIAL_NUMBER, ph.ENTRY_DATE FILING_DATE from proceeding p, party pa, property pr, prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and p.TYPE in ('EXA', 'CAN', 'OPP') and ph.IDENTIFIER = 1 order by p.NUMBER0, p.TYPE, FILING_DATE"
ttab_query9 = "select PH.ENTRY_DATE, PH.FK_PROCEEDINGNUMBER0, pr.REF_SERIAL_NUMBER, PH.FK_PROCEEDINGTYPE from proceeding p, party pa, property pr, prosecution_history_event PH where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = PH.FK_PROCEEDINGNUMBER0 and p.TYPE in ('EXA', 'CAN', 'OPP') and ((PH.ENTRY_CODE = 793) or (PH.ENTRY_CODE = 646) or (PH.ENTRY_CODE = 792 and PH.TEXT = 'SUBMITTED ON BRIEF ACR'))"

# COMMAND ----------

ip_1182= read_data_from_oracle_conn_dsu_cmn(ttab_query7,ttab_scope)
ip_1183= read_data_from_oracle_conn_dsu_cmn(ttab_query8,ttab_scope)
ip_1184= read_data_from_oracle_conn_dsu_cmn(ttab_query9,ttab_scope)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Judge Log - 4 RFD Gap Fill

# COMMAND ----------

select1163 = ip_1182.withColumn("MAILED_DT",col("MAILED_DT").cast(DateType())) \
    .withColumn("FILING_DT",col("FILING_DT").cast(DateType())) \
        .withColumn("MAILED_FY",when(month(col("MAILED_DT")) > 9, (year(col("MAILED_DT")) + 1))
                                                       .otherwise(year(col("MAILED_DT"))))

# COMMAND ----------

frm1166 = ip_1183.withColumn("CASE_TYPE", when(col("TYPE") == lit("OPP"),lit("OPPOSITION"))
                   .when(col("TYPE") == lit("CAN"),lit("CANCELLATION"))
                   .when(col("TYPE") == lit("EXA"),lit("EX PARTE APPEAL"))
                   .otherwise(col("TYPE"))) \
                       .withColumn("REF_SERIAL_NUMBER",col("REF_SERIAL_NUMBER").cast(StringType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get READY FOR DECISION (RFD) Dates

# COMMAND ----------

sumrz1175 = ip_1184.withColumn("FK_PROCEEDINGNUMBER0",col("FK_PROCEEDINGNUMBER0").cast(StringType())) \
    .groupBy("FK_PROCEEDINGNUMBER0","REF_SERIAL_NUMBER","FK_PROCEEDINGTYPE").agg(max(col("ENTRY_DATE")).alias("RFD_Date")) \
        .withColumn("REF_SERIAL_NUMBER",col("REF_SERIAL_NUMBER").cast(StringType())) \
            .withColumn("CASE_TYPE",when(col("FK_PROCEEDINGTYPE") == lit("OPP"),lit("OPPOSITION"))
                   .when(col("FK_PROCEEDINGTYPE") == lit("CAN"),lit("CANCELLATION"))
                   .when(col("FK_PROCEEDINGTYPE") == lit("EXA"),lit("EX PARTE APPEAL"))
                   .otherwise(col("FK_PROCEEDINGTYPE")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### continued (RFD Date Validation and Missing PATCH From Judge Log:)

# COMMAND ----------

filter1142_T = union1140.filter(col("RFD_DATE").isNotNull() & (col("RFD_Valid") == lit(True))) \
    .select(col("SERIAL_NUMBER"),
                              col("TTAB_ISSUE_TYPE"),
                              col("PROCEEDING_NUM"),
                              col("INSTITUTED_DATE"),
                              col("DECISION_DATE"),
                              col("RFD_DATE"),
                              col("CONSTRUCTED_PRCD_NUM"))
    
# bug fix: need to add or isNull for RFD_Valid to mimic alteryx filter
filter1142_F = union1140.filter(col("RFD_DATE").isNull() | (col("RFD_Valid") != lit(True)) | col("RFD_Valid").isNull()) \
    .select(col("SERIAL_NUMBER"),
                              col("TTAB_ISSUE_TYPE"),
                              col("PROCEEDING_NUM"),
                              col("INSTITUTED_DATE"),
                              col("DECISION_DATE"),
                              col("RFD_DATE"),
                              col("CONSTRUCTED_PRCD_NUM"))

# COMMAND ----------

join1143_I = filter1142_T.join(select1163,
             on = [col("serial_number") == col("FK_PROCEEDING_NUMBER0")],
             how = "inner"
             )

# COMMAND ----------

join1143_left = filter1142_T.join(select1163, filter1142_T.SERIAL_NUMBER == select1163.FK_PROCEEDING_NUMBER0, "anti")

# COMMAND ----------

join1143_right = select1163.join(filter1142_T, select1163.FK_PROCEEDING_NUMBER0 == filter1142_T.SERIAL_NUMBER, "anti")

# COMMAND ----------

join1144 = \
(
    join1143_left
        .join(join1143_right,
             on = [col("CONSTRUCTED_PRCD_NUM") == col("FK_PROCEEDING_NUMBER0")],
             how = "inner"
             )
)

# COMMAND ----------

join1144_right = join1143_right.join(join1143_left, join1143_left.CONSTRUCTED_PRCD_NUM == join1143_right.FK_PROCEEDING_NUMBER0, "leftanti") \
    .select(col("FK_PROCEEDING_NUMBER0"),
            col("MAILED_DT"))

# COMMAND ----------

join1148 = (
    join1144_right.join(
        frm1166, on=[col("FK_PROCEEDING_NUMBER0") == col("NUMBER0")], how="inner"
    )
    .select(
        "FK_PROCEEDING_NUMBER0",
        "MAILED_DT",
        "REF_SERIAL_NUMBER",
        "FILING_DATE",
        "CASE_TYPE",
    ).distinct()
)

# COMMAND ----------

#sumrz1175 = sumrz1175.withColumnRenamed("CASE_TYPE", "Right_CASE_TYPE")

join1148 = join1148.withColumnRenamed(
    "REF_SERIAL_NUMBER", "SERIAL_NUMBER"
).withColumnRenamed(
    "MAILED_DT", "JUDGE_DECISION_DATE"
).withColumnRenamed(
    "CASE_TYPE", "TTAB_CASE_TYPE"
)

join1150 = (
    join1148.join(
        sumrz1175,
        on=[
            join1148.FK_PROCEEDING_NUMBER0 == sumrz1175.FK_PROCEEDINGNUMBER0,
            join1148.TTAB_CASE_TYPE == sumrz1175.CASE_TYPE,
        ],
        how="inner",
    )
    .withColumnRenamed("SERIAL_NUMBER", "Right_SERIAL_NUMBER")
    .drop("FK_PROCEEDINGNUMBER0", "FK_PROCEEDINGTYPE", "CASE_TYPE")
).withColumn(
    "RFD_Date", col("RFD_Date").astype(DateType())
)

# COMMAND ----------

# bug fix: fill null dates for join and then revert
select1153 = filter1142_F.drop("RFD_Valid", "RFD_DATE", "RFD_FY").withColumn(
    "DECISION_DATE", col("DECISION_DATE").astype(StringType())
).fillna(
    "8888-01-01", subset=["DECISION_DATE"]
).withColumn(
    "DECISION_DATE", col("DECISION_DATE").astype(DateType())
)

join1150 = join1150.withColumn(
    "JUDGE_DECISION_DATE", col("JUDGE_DECISION_DATE").astype(StringType())
).fillna(
    "8888-01-01", subset=["JUDGE_DECISION_DATE"]
).withColumn(
    "JUDGE_DECISION_DATE", col("JUDGE_DECISION_DATE").astype(DateType())
)

join1151 = select1153.join(
    join1150,
    on=[
        select1153.SERIAL_NUMBER == join1150.Right_SERIAL_NUMBER,
        select1153.TTAB_ISSUE_TYPE == join1150.TTAB_CASE_TYPE,
        select1153.DECISION_DATE == join1150.JUDGE_DECISION_DATE,
    ],
    how="inner",
)

# revert date fill after join
join1151 = join1151.withColumn(
    "DECISION_DATE", when(col("DECISION_DATE") == "8888-01-01", lit(None)).otherwise(col("DECISION_DATE"))
)

# COMMAND ----------

union1145 = join1143_I.unionByName(join1144)

# COMMAND ----------

union1154 = union1145.unionByName(join1151, allowMissingColumns=True)

# COMMAND ----------

select1155 = union1154.select(
    col("SERIAL_NUMBER"), #.alias("Right_SERIAL_NUMBER"),
    col("TTAB_ISSUE_TYPE"), #.alias("Right_TTAB_ISSUE_TYPE"),
    col("PROCEEDING_NUM"), #.alias("Right_PROCEEDING_NUM"),
    col("INSTITUTED_DATE"), #.alias("Right_INSTITUTED_DATE"),
    col("DECISION_DATE"), #.alias("Right_DECISION_DATE"),
    col("RFD_DATE").alias("Right_RFD_Date"),
).withColumn(
    "Right_RFD_Valid",
    when(datediff(col("DECISION_DATE"), col("Right_RFD_Date")) >= 0, lit(True)).otherwise(
        lit(False)
    ),
)

# COMMAND ----------

# bug fix: fill null dates for join and then revert
union1140 = union1140.withColumn(
    "DECISION_DATE", col("DECISION_DATE").astype(StringType())
).fillna(
    "8888-01-01", subset=["DECISION_DATE"]
).withColumn(
    "DECISION_DATE", col("DECISION_DATE").astype(DateType())
)

select1155 = select1155.withColumn(
    "DECISION_DATE", col("DECISION_DATE").astype(StringType())
).fillna(
    "8888-01-01", subset=["DECISION_DATE"]
).withColumn(
    "DECISION_DATE", col("DECISION_DATE").astype(DateType())
)

join1157 = (
    union1140.join(
        select1155,
        on=[
            "SERIAL_NUMBER",
            "TTAB_ISSUE_TYPE",
            "PROCEEDING_NUM",
            "INSTITUTED_DATE",
            "DECISION_DATE"
        ],
        how="inner",
    )
    .drop(
        "RFD_DATE",
        "RFD_FY",
        "RFD_Valid",
        "Right_SERIAL_NUMBER",
        "Right_TTAB_ISSUE_TYPE",
        "Right_PROCEEDING_NUM",
        "Right_INSTITUTED_DATE",
        "Right_DECISION_DATE",
    )
    .withColumnRenamed("Right_RFD_Date", "RFD_DATE")
    .withColumnRenamed("Right_RFD_Valid", "RFD_Valid")
)

join1157_left = union1140.join(
    select1155,
    [
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "PROCEEDING_NUM",
        "INSTITUTED_DATE",
        "DECISION_DATE"
    ],
    "leftanti",
).drop("RFD_FY")

# revert date fill after join
join1157 = join1157.withColumn(
    "DECISION_DATE", when(col("DECISION_DATE") == "8888-01-01", lit(None)).otherwise(col("DECISION_DATE"))
)

join1157_left = join1157_left.withColumn(
    "DECISION_DATE", when(col("DECISION_DATE") == "8888-01-01", lit(None)).otherwise(col("DECISION_DATE"))
)

# COMMAND ----------

union1158 = join1157.union(join1157_left)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PROCEEDING FLAG

# COMMAND ----------

# used distinct instead of groupBy
sumrz1180 = union1158.distinct()

sumrz1117 = sumrz1180.groupBy(
    col("SERIAL_NUMBER"), #.alias("Right_SERIAL_NUMBER"),
    col("TTAB_ISSUE_TYPE"), #.alias("Right_TTAB_ISSUE_TYPE"),
    col("PROCEEDING_NUM") #.alias("Right_PROCEEDING_NUM"),
).agg(
    max("FILING_DATE").alias("FILING_DATE"),
    max("INSTITUTED_DATE").alias("INSTITUTED_DATE")
)

# COMMAND ----------

# bug fix: fill null dates for join and then revert
sumrz1180 = sumrz1180.withColumn(
    "FILING_DATE", col("FILING_DATE").astype(StringType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").astype(StringType())
).fillna(
    "8888-01-01", subset=["FILING_DATE", "INSTITUTED_DATE"]
).withColumn(
    "FILING_DATE", col("FILING_DATE").astype(DateType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").astype(DateType())
)

sumrz1117 = sumrz1117.withColumn(
    "FILING_DATE", col("FILING_DATE").astype(StringType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").astype(StringType())
).fillna(
    "8888-01-01", subset=["FILING_DATE", "INSTITUTED_DATE"]
).withColumn(
    "FILING_DATE", col("FILING_DATE").astype(DateType())
).withColumn(
    "INSTITUTED_DATE", col("INSTITUTED_DATE").astype(DateType())
)

join1118 = sumrz1180.join(
    sumrz1117,
    on=[
        "TTAB_ISSUE_TYPE",
        "PROCEEDING_NUM",
        "SERIAL_NUMBER",
        "FILING_DATE",
        "INSTITUTED_DATE",
    ],
    how="inner",
)

# revert date fill after join
join1118 = join1118.withColumn(
    "FILING_DATE", when(col("FILING_DATE") == "8888-01-01", lit(None)).otherwise(col("FILING_DATE"))
).withColumn(
    "INSTITUTED_DATE", when(col("INSTITUTED_DATE") == "8888-01-01", lit(None)).otherwise(col("INSTITUTED_DATE"))
)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import lag

##############################################
# bug fix: need to add sort on serial number #
##############################################

# ading new columns based on formula
windowSpec = Window.partitionBy("PROCEEDING_NUM", "FILING_DATE").orderBy(
    "SERIAL_NUMBER"
)

multirow1119 = join1118.withColumn("PRCD_COUNT", row_number().over(windowSpec))

# COMMAND ----------

# windowSpec = Window.orderBy("PROCEEDING_NUM")

multirow1119 = (
    multirow1119.withColumn(
        "PROCEEDING_COUNT", when(col("PRCD_COUNT") > 1, lit(None)).otherwise(lit(1))
    )
    .withColumn(
        "FILING_FY",
        when(month(col("FILING_DATE")) > 9, (year(col("FILING_DATE")) + 1)).otherwise(
            year(col("FILING_DATE"))
        ),
    )
    .drop("FILED_YR", "INST_YR", "TERM_YR", "DECISION_YR", "FILING_FY")
)

# COMMAND ----------

multirow1119.write.mode("overwrite").saveAsTable(f"{reporting_catalog}.silver.ttab_detail_select1124")

# COMMAND ----------

select1124 = spark.sql(f"select * from {reporting_catalog}.silver.ttab_detail_select1124")

# COMMAND ----------

# MAGIC %md
# MAGIC # POST PROCESSING AND FINAL OUTPUT: Calgary Datasets and Tableau Hyper Files

# COMMAND ----------

# appeals
appeals_1 = spark.sql(f"select * from {reporting_catalog}.silver.ttab_detail_appeals_1")

# COMMAND ----------

# oppositions
join790 = ph_select711.join(
    input_cde,
    on = [col("SERIAL_NUMBER") == col("SER_NUM")],
    how = "left"
)

oppositions_1 = oppositions.join(join790, "SERIAL_NUMBER", "outer").withColumn("PUBS", lit(1))

# COMMAND ----------

# cancellations
ph_join1061 = (
    cancellations.join(
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
    .agg(count("SERIAL_NUMBER").alias("Cancellation_Count"))
)

ph_join1066 = ph_join1061.join(
    ph_sumrz1063, "FILING_FY", "inner"
)

ph_join1065 = ph_join1066.join(
    post_reg_mil_sumrz774.withColumn("LIVE_REG_COUNT", col("LIVE_REG_COUNT").astype(IntegerType())), on=[col("FILING_FY") == col("REG_YR")], how="left"
)

cancellations_1 = ph_join1065.withColumn(
    "CAN_RATE", col("Cancellation_Count") / col("LIVE_REG_COUNT")
)

# COMMAND ----------

# concurrent filings
concurrent_filings_1 = (
    concurrent_filings.join(
        input_cde, on=[col("serial_number") == col("SER_NUM")], how="left"
    )
    .drop("SER_NUM")
    .drop("Pendency_Cal_Start_DT")
)

# COMMAND ----------

union850 = appeals_1.unionByName(
    oppositions_1, allowMissingColumns=True
).unionByName(
    cancellations_1, allowMissingColumns=True
).unionByName(
    concurrent_filings_1, allowMissingColumns=True
)

# COMMAND ----------

frm853 = (
    union850.withColumn(
        "PENDENCY_D", datediff(col("DECISION_DATE"), col("INSTITUTED_DATE"))
    )
    .withColumn("PENDENCY_T", datediff(col("TERMINATION_DATE"), col("INSTITUTED_DATE")))
    .withColumn("PENDENCY_R", lit(None))
)

# COMMAND ----------

join1010 = frm853.join(
    select1124.select(
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "PROCEEDING_NUM",
        col("RFD_DATE"),
        col("RFD_VALID"),
        col("PROCEEDING_COUNT"),
        col("PRCD_COUNT"),
    ),
    on=[
        "SERIAL_NUMBER",
        "TTAB_ISSUE_TYPE",
        "PROCEEDING_NUM",
    ],
    how="left"
).drop(
    "FILED_YR",
    "INST_YR",
    "TERM_YR",
    "DECISION_YR",
)

# COMMAND ----------

frm1072 = join1010.withColumn(
    "Case_Age_RFD",
    when(
        (col("RFD_DATE").isNotNull() & ((col("DECISION_DATE") == "") | col("DECISION_DATE").isNull())),
        round(datediff(current_date(), col("RFD_Date")) / 7),
    ).otherwise(lit(None)),
).withColumn(
    "Case_Age_Category", expr("""
        case 
        when Case_Age_RFD is not null
        then
            case
            when Case_Age_RFD < 10
            then 'Less than 10 Weeks'
            when Case_Age_RFD >= 10 and Case_Age_RFD <= 15
            then '10-15 Weeks'
            else 'Over 15 Weeks'
            end
        else Null
        end
    """)
)

# COMMAND ----------

# used distinct instead of groupBy
sumrz1185 = (
    frm1072.select(
        col("Active_Class_Count"),
        col("AM_STAT"),
        col("APPEAL"),
        col("CAN_RATE"),
        col("CANCELLATION"),
        col("Cancellation_Count"),
        col("Case_Age_Category"),
        col("Case_Age_RFD"),
        col("CITY"),
        col("Concat_Class"),
        col("CONCURRENT"),
        col("CONSTRUCTED_PRCD_NUM"),
        col("Country_or_Area_Name"),
        col("DECISION_CODE"),
        col("DECISION_DATE"),
        col("DECISION_DESCRIPTION"),
        col("DEFAULT_CANCELLATION"),
        col("DEFAULT_DATE"),
        col("DEFAULT_OPPOSITION"),
        col("FILING_BASIS_GRP"),
        col("FILING_DATE"),
        col("FILING_METHOD_CUR"),
        col("FINAL_REFUSAL_DATE"),
        col("FP_REASON_1"),
        col("FP_REASON_2"),
        col("FP_REASON_3"),
        col("FP_REASON_4"),
        col("FP_REASON_5"),
        col("Group_Type"),
        col("INSTITUTED_CODE"),
        col("INSTITUTED_DATE"),
        col("INVENTORY"),
        col("LAW_OFFICE"),
        col("LIVE_REG_COUNT"),
        col("MARK_NM_SHORT"),
        col("NON_PRO_SE"),
        col("OPPOSITION"),
        col("Owner_Name"),
        col("TEST_PCTRAM_LINK"), 
        col("PENDENCY_D"),
        col("PENDENCY_R"),
        col("PENDENCY_T"),
        col("PRCD_COUNT"),
        col("PROCEEDING_COUNT"),
        col("PROCEEDING_NUM"),
        col("PUBLICATION_DATE"),
        col("PUBS"),
        col("REFUSAL"),
        col("Reg_Class_Count"),
        col("REG_YR"),
        col("RFD_Date"),
        col("RFD_Valid"),
        col("SERIAL_NUMBER"),
        col("State"),
        col("TERMINATION_CODE"),
        col("TERMINATION_DATE"),
        col("TERMINATION_DATE_2"),
        col("TERMINATION_DATE_3"),
        col("TERMINATION_DATE_4"),
        col("TERMINATION_DATE_5"),
        col("TTAB_ISSUE_TYPE"),
    )
    .distinct()
    .drop("PRCD_COUNT")
)

# COMMAND ----------

join729 = \
(
    select1124
        .join(input_cde,
             on = [col("serial_number") == col("SER_NUM")],
             how = "inner"
             ) \
                 .drop("SER_NUM") \
                     .drop("Pendency_Cal_Start_DT")
)

# COMMAND ----------

###########################################################
# REMOVED PENDENCY_R BECAUSE VOID - REPLICATE IN ALTERYX? #
###########################################################

# used distinct instead of groupBy
sumrz1016 = join729.select(
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
    col("FINAL_REFUSAL_DATE"),
    col("FP_REASON_1"),
    col("FP_REASON_2"),
    col("FP_REASON_3"),
    col("FP_REASON_4"),
    col("FP_REASON_5"),
    col("APPEAL"),
    col("INVENTORY"),
    col("PENDENCY_D"),
    col("PENDENCY_T"),
    #col("PENDENCY_R"),
    col("PUBLICATION_DATE"),
    col("CONSTRUCTED_PRCD_NUM"),
    col("OPPOSITION"),
    col("DEFAULT_DATE"),
    col("DEFAULT_OPPOSITION"),
    col("CANCELLATION"),
    col("DEFAULT_CANCELLATION"),
    col("CONCURRENT"),
    col("RFD_DATE"),
    col("RFD_Valid"),
    col("PROCEEDING_COUNT"),
    col("NON_PRO_SE"),
    col("TEST_PCTRAM_LINK"), 
    col("LAW_OFFICE"),
    col("FILING_BASIS_GRP"),
    col("FILING_METHOD_CUR"),
    col("AM_STAT"),
    col("Owner_Name"),
    col("CITY"),
    col("State"),
    col("Country_or_Area_Name"),
    col("Reg_Class_Count"),
    col("Active_Class_Count"),
    col("Group_Type"),
    col("Concat_Class"),
    col("MARK_NM_SHORT"),
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDATION and FINAL OUTPUT: Calgary Datasets and Tableau Hyper Files

# COMMAND ----------

filter1186 = sumrz1185.filter(col("TTAB_ISSUE_TYPE").isNotNull()).withColumn(
    "Record_Output_Date", current_timestamp()
)

# COMMAND ----------

sumrz1188 = filter1186.groupBy("Record_Output_Date").agg(
    count("SERIAL_NUMBER").alias("Output_Record_Count")
)

# COMMAND ----------

ip_df_ttab_dtl_cnts = spark.sql(
    f"""select * from {reporting_catalog}.silver.ttab_detail_counts"""
)
# ip_df_ttab_dtl_cnts = spark.sql(f"""select * from {reporting_catalog}.silver.ttab_detail_counts""") ## Need to replace with this query

union1190 = sumrz1188.unionByName(ip_df_ttab_dtl_cnts, allowMissingColumns=True).orderBy(
    col("Record_Output_Date").desc()
)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import lag, lead

# ading new columns based on formula
window1 = Window.orderBy(col("record_output_date").desc())

multirow1196 = union1190.withColumn(
    "record_output_percent_change",
    (col("output_record_count") - lead(col("output_record_count")).over(window1))
    / lead(col("output_record_count")).over(window1),
)

# COMMAND ----------

multirow1192 = (
    multirow1196.withColumn(
        "continue_process",
        when(
            (
                col("output_record_count")
                >= lead(col("output_record_count")).over(window1)
            )
            & (col("record_output_percent_change") < lit(0.05)),
            1,
        ).otherwise(0),
    )
).limit(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP or OUTPUT Files

# COMMAND ----------

# DBTITLE 1,TTAB Detail Table Output
ttab_detail_tbl_op = (
    sumrz1185.select(
        col("serial_number").cast(StringType()),
        col("ttab_issue_type"),
        col("proceeding_num"),
        col("filing_date"),
        col("instituted_date"),
        col("instituted_code"),
        col("decision_date"),
        col("decision_code"),
        col("decision_description"),
        col("termination_code"),
        col("termination_date").cast(DateType()),
        col("termination_date_2").cast(DateType()),
        col("termination_date_3").cast(DateType()),
        col("termination_date_4").cast(DateType()),
        col("termination_date_5").cast(DateType()),
        col("final_refusal_date").cast(DateType()),
        col("fp_reason_1"),
        col("fp_reason_2"),
        col("fp_reason_3"),
        col("fp_reason_4"),
        col("fp_reason_5"),
        col("pendency_d"),
        col("pendency_t"),
        col("pendency_r"),
        col("inventory"),
        col("NON_PRO_SE"),
        col("TEST_PCTRAM_LINK").alias("pctram_link"), # fix later
        col("law_office"),
        col("filing_basis_grp"),
        col("filing_method_cur"),
        col("am_stat"),
        col("owner_name"),
        col("city"),
        col("state"),
        col("country_or_area_name"),
        col("reg_class_count"),
        col("active_class_count"),
        col("group_type"),
        col("concat_class"),
        col("MARK_NM_SHORT"),
        col("refusal"),
        col("appeal").cast(BooleanType()),
        col("publication_date"),
        col("pubs").cast(BooleanType()),
        col("opposition"),
        col("default_opposition"),
        col("default_cancellation"),
        col("cancellation"),
        col("constructed_prcd_num"),
        col("default_date"),
        col("cancellation_count"),
        col("reg_yr").cast(StringType()),
        col("live_reg_count"),
        col("can_rate"),
        col("concurrent"),
        col("rfd_date"),
        col("rfd_valid"),
        col("proceeding_count"),
        col("case_age_rfd"),
        col("case_age_category"),
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

# COMMAND ----------

# DBTITLE 1,TTAB DETAIL SUMMARY Output
ttab_detail_summary_op = (
    sumrz1016.select(
        col("serial_number").astype(StringType()),
        col("ttab_issue_type"),
        col("proceeding_num"),
        col("filing_date"),
        col("instituted_date"),
        col("instituted_code"),
        col("decision_date"),
        col("decision_code"),
        col("decision_description"),
        col("termination_code"),
        col("termination_date").astype(DateType()),
        col("termination_date_2").astype(DateType()),
        col("termination_date_3").astype(DateType()),
        col("termination_date_4").astype(DateType()),
        col("termination_date_5").astype(DateType()),
        col("final_refusal_date").astype(DateType()),
        col("fp_reason_1"),
        col("fp_reason_2"),
        col("fp_reason_3"),
        col("fp_reason_4"),
        col("fp_reason_5"),
        col("appeal").astype(BooleanType()),
        col("inventory").astype(BooleanType()),
        col("pendency_d"),
        col("pendency_t"),
        #col("pendency_r").LongType(),
        col("publication_date"),
        col("constructed_prcd_num"),
        col("opposition"),
        col("default_date"),
        col("default_opposition"),
        col("cancellation"),
        col("default_cancellation"),
        col("concurrent"),
        col("rfd_date"),
        col("rfd_valid"),
        col("proceeding_count"),
        col("non_pro_se"),
        col("TEST_PCTRAM_LINK").alias("pctram_link"),
        col("law_office"),
        col("filing_basis_grp"),
        col("filing_method_cur"),
        col("am_stat"),
        col("owner_name"),
        col("city"),
        col("state"),
        col("country_or_area_name"),
        col("reg_class_count").astype(IntegerType()),
        col("active_class_count").astype(IntegerType()),
        col("group_type"),
        col("concat_class"),
        col("mark_nm_short")
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

# COMMAND ----------

union1197 = ip_df_ttab_dtl_cnts.union(multirow1192)

# COMMAND ----------

# DBTITLE 1,TTAB Detail count Output
ttab_detail_count_op = (
    union1197.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing the dataframe into tables.

# COMMAND ----------

try:
    print("Writing table in gold level")
    ttab_detail_tbl_op.write.mode("overwrite").format("delta").insertInto(
        f"{reporting_catalog}.gold.ttab_detail"
    )
    print("Writing table in silver level")
    ttab_detail_summary_op.write.mode("overwrite").format("delta").insertInto(
        f"{reporting_catalog}.silver.ttab_detail_summary"
    )
    ttab_detail_count_op.write.mode("overwrite").format("delta").insertInto(
        f"{reporting_catalog}.silver.ttab_detail_counts"
    )
    recs_count = ttab_detail_tbl_op.count()
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        recs_count,
        "job completed successfully",
    )
    dbutils.notebook.exit(f"Completed Loading ttab detail Table ")

except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise
    dbutils.notebook.exit(f"Failed Loading ttab detail Table ")

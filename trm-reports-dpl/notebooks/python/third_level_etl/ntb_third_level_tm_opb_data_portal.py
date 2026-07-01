# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,setting up env
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
edw_scope = common_configs['secrets']['edw_scope']
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']

## EDW connection details
host = dbutils.secrets.get(scope=edw_scope, key="host")
port = dbutils.secrets.get(scope=edw_scope, key="port")
db_name = dbutils.secrets.get(scope=edw_scope, key="db_name")

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_third_level_tm_opb_data_portal'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# DBTITLE 1,Inputs
inpt_139 = spark.sql(f"""select * from {reporting_catalog}.gold.filings_dashboard""")
inpt_140 =spark.sql(f"""select * from {reporting_catalog}.gold.pendency_dashboard""")
inpt_141 = spark.sql(f"""select * from {reporting_catalog}.silver.bibliography""")
inpt_142 = spark.sql(f""" select * from {reporting_catalog}.gold.post_reg_detail_dashboard""")
input_ph = spark.sql(f"""select * from {reporting_catalog}.silver.prosecution_history""")

# COMMAND ----------

# DBTITLE 1,Filing Fixed Class Ct
fltr_118 = inpt_139.filter((col("filing_fy") >= 2018))
sumrz_4 = fltr_118.groupBy("pendency_cal_start_dt","filing_fy","filing_method_filed").agg(sum("fixed_count").alias("fixed_count"))
sumrz_4 = sumrz_4.withColumnRenamed("pendency_cal_start_dt","DATE")

# COMMAND ----------

from pyspark.sql.functions import instr

frmla_9 = sumrz_4.withColumn("FILING_METHOD_GRP",when(((instr(col("FILING_METHOD_FILED"),"TEAS") > 0) | (instr(col("FILING_METHOD_FILED"),"BASE") > 0)),"ELECTRONIC")\
                            .otherwise(col("FILING_METHOD_FILED")))\
            .withColumn("ELECTRONIC_FIXED_CT",
                            when(instr(col("FILING_METHOD_FILED"),"TEAS") > 0,col("fixed_count"))\
                            .otherwise(lit(None)))

# COMMAND ----------

sumrz_26 =frmla_9.groupBy("filing_fy","DATE","FILING_METHOD_GRP").agg(sum("fixed_count").alias("Sum_Fixed_Count"))
crsstb_27 = sumrz_26.groupBy("filing_fy","DATE").pivot("FILING_METHOD_GRP").agg(sum("Sum_Fixed_Count"))

# COMMAND ----------

from pyspark.sql.functions import coalesce

sel_34 = crsstb_27.withColumnRenamed("filing_fy","FILING_FY") \
    .withColumnRenamed("Paper","PAPER")
frmla_33 = sel_34.withColumn("TOTAL_CLASSES",
                             coalesce(col("ELECTRONIC"), lit(0)) +
                             coalesce(col("MADRID"), lit(0)) +
                             coalesce(col("PAPER"), lit(0))) \
                             .withColumn("TM_ANALYTICS_TS",
                                         date_format(current_timestamp(),"yyyy-mm-dd HH:mm:ss")) \
                                            .withColumnRenamed("6 TER","6_TER")

# COMMAND ----------

# frmla_33.display()

# COMMAND ----------

# DBTITLE 1,TM Pendency Dashboard Output
inpt_140
fltr_40 = inpt_140.filter(col("first_action_dt_ph").isNotNull())
fltr_22 = inpt_140.filter(col("disposal_dt").isNotNull())

sumrz_24 = fltr_40.groupBy("first_action_dt_ph").agg(
        sum("active_classes_firstaction").alias("FA_CLASSES"),
        countDistinct("ser_num").alias("FA_CASES")
    )
sumrz_24 = sumrz_24.withColumnRenamed("first_action_dt_ph","DATE")

# COMMAND ----------

# sumrz_24
# frmla_33
join_36 = (
    frmla_33.alias("f").
    join(sumrz_24.alias("s"),
         (col("f.DATE") == col("s.DATE")),
         how="leftouter"
         )
.selectExpr(
        "f.FILING_FY",
        "f.DATE",
        "f.6_TER",  # Specify table alias for ABANDONMENT
        "f.ELECTRONIC",  # Specify table alias for NOA
        "f.MADRID",  # Specify table alias for REGISTRATION
        "f.PAPER",  # Specify table alias for ABANDONMENT
        "f.TOTAL_CLASSES",  # Specify table alias for NOA
        "f.TM_ANALYTICS_TS",  # Specify table alias for REGISTRATION
        "s.FA_CLASSES",
        "s.FA_CASES"
    )
)
uniton_41 = join_36

# COMMAND ----------

# DBTITLE 1,Disposal
#fltr_22
select_158 = fltr_22.withColumnRenamed("active_classes_disposal","Active_Classes_Disposal") \
    .withColumnRenamed("disposal_dt","Disposal_DT") \
        .withColumnRenamed("disposal_type","Disposal_Type") \
            .withColumnRenamed("filing_basis_grp","FILING_BASIS_GRP") \
                .withColumnRenamed("ser_num","SER_NUM") 

sumrz_155 = select_158.groupBy("Disposal_DT","Disposal_Type","FILING_BASIS_GRP").agg(sum("Active_Classes_Disposal").alias("Sum_Active_Classes_Disposal"),countDistinct("SER_NUM").alias("CountD_SER_NUM"))
sumrz_155 = sumrz_155.withColumnRenamed("Disposal_DT","DATE")
# crsstb_156 = sumrz_155.groupBy("").pivot("Disposal_Type")

# COMMAND ----------

crsstb_156 = sumrz_155.groupBy("DATE").pivot("Disposal_Type").agg(sum("CountD_SER_NUM"))
crsstb_157 = sumrz_155.groupBy("DATE").pivot("Disposal_Type").agg(sum("Sum_Active_Classes_Disposal"))

# COMMAND ----------

# crsstb_156.display() ## tested

# COMMAND ----------

join_130 = (
    crsstb_156.alias("a")
    .join(
        crsstb_157.alias("b"),
        (col("a.DATE") == col("b.DATE"))
    )
    .selectExpr(
        "a.DATE",
        "a.ABANDONMENT as ABANDONMENT_CASES",  # Specify table alias for ABANDONMENT
        "a.NOA as NOA_CASES",  # Specify table alias for NOA
        "a.REGISTRATION as REGISTRATION_CASE",  # Specify table alias for REGISTRATION
        "b.ABANDONMENT as ABANDONMENT_CLASSES",  # Specify table alias for ABANDONMENT
        "b.NOA as NOA_CLASSES",  # Specify table alias for NOA
        "b.REGISTRATION as REGISTRATION_CLASSES"  # Specify table alias for REGISTRATION
    )
)

# COMMAND ----------

# join_130.count()# count match

# COMMAND ----------

#SOU Filed and Accepted
fltr_69 = input_ph.filter(col("ph_action_code").isin("CNPR","CNSR"))
sumrz_70 = fltr_69.groupBy(col("ph_action_date")).agg(countDistinct(col("SERIAL_NUMBER")).alias("SOU_ACCEPTED"))
sumrz_70 = sumrz_70.withColumnRenamed("ph_action_date","DATE")

# SOU ABANDONMENTS

join_74 = (
    fltr_69.alias("f")
    .join(input_ph.alias("s"),
          (col("f.SERIAL_NUMBER") == col("s.SERIAL_NUMBER"))

    )
    .selectExpr(
        "f.SERIAL_NUMBER",
        "s.ph_action_date",
        "s.ph_action_code"
    )
)

fltr_72 = join_74.filter(col("ph_action_code").contains("ABN"))

sumrz_73 = fltr_72.groupBy("ph_action_date").agg(countDistinct("SERIAL_NUMBER").alias("SOU_ABANDONMENTS"))
sumrz_73 = sumrz_73.withColumnRenamed("ph_action_date","DATE")

# COMMAND ----------

# DBTITLE 1,Bibliography
select_116 = inpt_141
fltr_83 = select_116.filter((col("AM_STAT") != 633))

fltr_84 = fltr_83.filter((col('FILING_BASIS_FIL')=='ITU'))
fltr_85 = fltr_83.filter((col('FILING_BASIS_FIL')=='USE'))

# COMMAND ----------

## ITU ABANDONMENTS
join_78 = (
    input_ph.alias("l")
    .join(fltr_84.alias("r"),
          (col("l.SERIAL_NUMBER") == col("r.SER_NUM"))
          )
    .selectExpr(
        "l.SERIAL_NUMBER",
        "ph_action_date",
        "ph_action_code"
    )
    )

fltr_76 = join_78.filter(col("ph_action_code").contains("ABN"))

sumrz_77 = fltr_76.groupBy("ph_action_date").agg(countDistinct("SERIAL_NUMBER").alias("ITU_ABANDONMENTS"))
sumrz_77 = sumrz_77.withColumnRenamed("ph_action_date","DATE")

# COMMAND ----------

# DBTITLE 1,PostReg_Aggregates
# inpt_142
fltr_106 = inpt_142.filter(col("start_action_date")>= '2014-10-01')
fltr_99 = fltr_106.filter(col("postreg_category") == "6 YEAR")
fltr_101_T = fltr_99.filter(col("fifteen_flag")== "True")
fltr_101_F = fltr_99.filter(col("fifteen_flag").isNull())
sumrz_98 = fltr_101_T.groupBy(col("START_ACTION_DATE")).agg(countDistinct("serial_number").alias("6YR/15_Maintenance_Filed"))
sumrz_98 = sumrz_98.withColumnRenamed("START_ACTION_DATE","Filing_DT")

sumrz_105 = fltr_101_F.groupBy(col("START_ACTION_DATE")).agg(countDistinct("serial_number").alias("6YR_Maintenance_Filed"))
sumrz_105 = sumrz_105.withColumnRenamed("START_ACTION_DATE","Filing_DT")

fltr_100 = fltr_106.filter(col("postreg_category") == "10 YEAR")
sumrz_104 = fltr_100.groupBy(col("START_ACTION_DATE")).agg(countDistinct("serial_number").alias("10YR_Renewal_Filed"))
sumrz_104 = sumrz_104.withColumnRenamed("START_ACTION_DATE","Filing_DT")

fltr_102 = fltr_106.filter(col("postreg_category") == "SECTION 7")
sumrz_103 = fltr_102.groupBy(col("START_ACTION_DATE")).agg(countDistinct("serial_number").alias("Section7_Filed"))
sumrz_103 = sumrz_103.withColumnRenamed("START_ACTION_DATE","Filing_DT")

fltr_110 = fltr_106.filter(col("postreg_category") == "SEPARATE 15")
sumrz_111 = fltr_110.groupBy(col("START_ACTION_DATE")).agg(countDistinct("serial_number").alias("Separate15_Filed"))
sumrz_111 = sumrz_111.withColumnRenamed("START_ACTION_DATE","Filing_DT")

# COMMAND ----------

# union_108 = sumrz_98.unionByName(sumrz_105).unionByName(sumrz_104).unionByName(sumrz_103).unionByName(sumrz_111)
## Union all the df and summerized in 109 tool. 
sumrz_109 = sumrz_98 \
    .join(sumrz_105, on = "Filing_DT", how="outer")\
        .join(sumrz_104, on = "Filing_DT", how="outer")\
            .join(sumrz_103, on = "Filing_DT", how="outer")\
                .join(sumrz_111, on = "Filing_DT", how="outer")

sumrz_109 = sumrz_109.withColumnRenamed("Filing_DT","DATE")


# COMMAND ----------

sumrz_109 = sumrz_109.withColumnRenamed("6YR/15_Maintenance_Filed","6YR_15_Maintenance_Filed") # a1 input

# COMMAND ----------

# join_130.count()a2 input 
# join_36 a8
# sumrz_70 a6
# sumrz_73 a5
# sumrz_77 -a4
#sumrz_109 a1 input
multi_join_113 = (sumrz_109.alias("a1") \
    .join(join_130.alias("a2"), on= "DATE", how='fullouter')\
        .join(sumrz_77.alias("a4"), on="DATE", how='fullouter')\
            .join(sumrz_73.alias("a5"), on="DATE", how='fullouter')\
                .join(sumrz_70.alias("a6"), on="DATE", how='fullouter')\
                    .join(join_36.alias("a8"),on="DATE", how='fullouter')
                    .selectExpr(
                        "a1.DATE",
                        "ITU_ABANDONMENTS",
                        "SOU_ABANDONMENTS",
                        "SOU_ACCEPTED",
                        "6_TER",
                        "ELECTRONIC",
                        "FA_CASES",
                        "FA_CLASSES",
                        "FILING_FY",
                        "MADRID",
                        "PAPER",
                        "TM_ANALYTICS_TS",
                        "TOTAL_CLASSES",
                        "ABANDONMENT_CASES",
                        "NOA_CASES",
                        "REGISTRATION_CASE as REGISTRATION_CASES",
                        "ABANDONMENT_CLASSES",
                        "NOA_CLASSES",
                        "REGISTRATION_CLASSES",
                        "6YR_15_Maintenance_Filed ",
                        "6YR_Maintenance_Filed",
                        "10YR_RENEWAL_FILED",
                        "Section7_Filed",
                        "Separate15_Filed"
                        ))


# COMMAND ----------

# multi_join_113.display() --Data Validated
ftlr_119 = multi_join_113.filter(col("FILING_FY")>= 2018)

# COMMAND ----------

inpt_file_122 = f"s3://bdr-databricks-app-{dbx_env}/eds/static_files/opb_data/text_input_122.csv"

print(f'{inpt_file_122=}')
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# Creating the Dataframe for input and output
inpt_122 = spark.read.format(file_type) \
  .option("schema",infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .option("encoding", "windows-1252") \
  .load(inpt_file_122)

# COMMAND ----------

inpt_122_dt_frmt = inpt_122.withColumn("WK_DT_START2",to_date("WK_DT_START","M/d/y")) \
    .withColumn("WK_DT_END2",to_date("WK_DT_END","M/d/y"))

# COMMAND ----------

from pyspark.sql.functions import explode

# Generate a sequence of dates from wk_dt_start2 to wk_dt_end2
gen_row_53 = inpt_122_dt_frmt.withColumn("JOIN_DT", explode(expr("sequence(WK_DT_START2, WK_DT_END2, interval 1 day)")))

# COMMAND ----------

sel_125 = gen_row_53.withColumnRenamed("WK_DT_START","WK_DT_START2_temp")\
    .withColumnRenamed("WK_DT_END","WK_DT_END2_temp")\
        .withColumnRenamed("WK_DT_END2","WK_DT_END")\
            .withColumnRenamed("WK_DT_START2","WK_DT_START")\
                .withColumnRenamed("WK_DT_END2_temp","WK_DT_END2")\
                    .withColumnRenamed("WK_DT_START2_temp","WK_DT_START2")

# COMMAND ----------

# sel_125.count() # count validated
join_54 = (ftlr_119
           .join(sel_125,
                 (col("DATE") ==col("JOIN_DT"))))

# COMMAND ----------

form_121= join_54.withColumn("MONTH",date_format("DATE","MMM"))

# COMMAND ----------

from pyspark.sql.functions import when

form_121 = form_121.withColumn("FY_MONTH_NUM", 
                               when(col("MONTH") == "Oct", 1)
                               .when(col("MONTH") == "Nov", 2)
                               .when(col("MONTH") == "Dec", 3)
                               .when(col("MONTH") == "Jan", 4)
                               .when(col("MONTH") == "Feb", 5)
                               .when(col("MONTH") == "Mar", 6)
                               .when(col("MONTH") == "Apr", 7)
                               .when(col("MONTH") == "May", 8)
                               .when(col("MONTH") == "Jun", 9)
                               .when(col("MONTH") == "Jul", 10)
                               .when(col("MONTH") == "Aug", 11)
                               .when(col("MONTH") == "Sep", 12)
                               .otherwise(None))

# COMMAND ----------

sumrz_55 = form_121.groupBy(
    "DATE",
    "FY_WK_NUM",
    "FY_YEAR",
    "CL_YEAR",
    "FYPP",
    "YYYYPP",
    "WK_DT_START",
    "WK_DT_END",
    "JOIN_DT",
    "MONTH",
    "FY_MONTH_NUM",
    "FILING_FY"
).agg(
    sum("6_TER").alias("6_TER"),
    sum("ELECTRONIC").alias("ELECTRONIC"),
    sum("MADRID").alias("MADRID"),
    sum("PAPER").alias("PAPER"),
    sum("TOTAL_CLASSES").alias("TOTAL_CLASSES"),
    sum("FA_CLASSES").alias("FA_CLASSES"),
    sum("FA_CASES").alias("FA_CASES"),
    sum("ITU_ABANDONMENTS").alias("ITU_ABANDONMENTS"),
    sum("SOU_ABANDONMENTS").alias("SOU_ABANDONMENTS"),
    sum("SOU_ACCEPTED").alias("SOU_ACCEPTED"),
    sum("6YR_MAINTENANCE_FILED").alias("6YR_MAINTENANCE_FILED"),
    sum("SEPARATE15_FILED").alias("SEPARATE15_FILED"),
    sum("6YR_15_MAINTENANCE_FILED").alias("6YR_15_MAINTENANCE_FILED"),
    sum("10YR_RENEWAL_FILED").alias("10YR_RENEWAL_FILED"),
    sum("SECTION7_FILED").alias("SECTION7_FILED"),
    sum("ABANDONMENT_CASES").alias("ABANDONMENT_CASES"),
    sum("NOA_CASES").alias("NOA_CASES"),
    sum("REGISTRATION_CASES").alias("REGISTRATION_CASES"),
    sum("ABANDONMENT_CLASSES").alias("ABANDONMENT_CLASSES"),
    sum("NOA_CLASSES").alias("NOA_CLASSES"),
    sum("REGISTRATION_CLASSES").alias("REGISTRATION_CLASSES")
)

# COMMAND ----------

# DBTITLE 1,Final Dataframe
fltr_56 = sumrz_55.filter(~((col("FY_YEAR") == "2018") & (col("FY_WK_NUM")== '1') & (col("ELECTRONIC") == 394 )))

# COMMAND ----------

df_out = fltr_56.select(
    'date',
    'FY_WK_NUM',
    'FY_YEAR',
    'CL_YEAR',
    'FYPP',
    'YYYYPP',
    'WK_DT_START',
    'WK_DT_END',
    'JOIN_DT',
    'MONTH',
    'FY_MONTH_NUM',
    'FILING_FY',
    '6_TER',
    'ELECTRONIC',
    'MADRID',
    'PAPER',
    'TOTAL_CLASSES',
    'FA_CLASSES',
    'FA_CASES',
    'ITU_ABANDONMENTS',
    'SOU_ABANDONMENTS',
    'SOU_ACCEPTED',
    '6YR_Maintenance_Filed',
    'Separate15_Filed',
    '6YR_15_Maintenance_Filed',
    '10YR_RENEWAL_FILED',
    'Section7_Filed',
    'ABANDONMENT_CASES',
    'NOA_CASES',
    'REGISTRATION_CASES',
    'ABANDONMENT_CLASSES',
    'NOA_CLASSES',
    'REGISTRATION_CLASSES'
)

# COMMAND ----------

df_out_edw = fltr_56.select(
    'date',
    'FY_WK_NUM',
    'FY_YEAR',
    'CL_YEAR',
    'FYPP',
    'YYYYPP',
    'WK_DT_START',
    'WK_DT_END',
    'JOIN_DT',
    'MONTH',
    'FY_MONTH_NUM',
    '6_TER',
    'ELECTRONIC',
    'MADRID',
    'PAPER',
    'TOTAL_CLASSES',
    'FA_CLASSES',
    'FA_CASES',
    'ITU_ABANDONMENTS',
    'SOU_ABANDONMENTS',
    'SOU_ACCEPTED',
    '6YR_Maintenance_Filed',
    'Separate15_Filed',
    '6YR_15_Maintenance_Filed',
    '10YR_RENEWAL_FILED',
    'Section7_Filed',
    'FILING_FY',
    'ABANDONMENT_CASES',
    'NOA_CASES',
    'REGISTRATION_CASES',
    'ABANDONMENT_CLASSES',
    'NOA_CLASSES',
    'REGISTRATION_CLASSES'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write Data into Tables. 

# COMMAND ----------

try:
    df_out.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.TM_OPB_METRICS')

    ### Write to EDW

    edw_target_table = "TM_OPB_METRICS"

    df_out_edw.write.format("jdbc").mode("overwrite").option("url", "jdbc:oracle:thin:@"+host+":"+port+"/"+db_name)\
                            .option("dbtable",edw_target_table)\
                            .option("user", dbutils.secrets.get(scope=edw_scope, key="username"))\
                            .option("password", dbutils.secrets.get(scope=edw_scope, key="password"))\
                            .option("driver", "oracle.jdbc.OracleDriver")\
                            .save()

    #############################################################################################
    # 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
    # Allows disabling Alteryx workflow schedule fully, saving resources.                       #
    #############################################################################################

    # # data quality entry
    # tbl1 = f"hive_metastore.{altrx_schema}.TM_OPB_METRICS" 
    # tbl2 = f"{reporting_catalog}.gold.TM_OPB_METRICS"
    # key_cols = ['date']
    
    # dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
    # print(dq_result)

    recs_count = df_out.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading TM_OPB_METRICS Tables ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading TM_OPB_METRICS Table ")

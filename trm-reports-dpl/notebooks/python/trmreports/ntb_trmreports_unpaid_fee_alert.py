# Databricks notebook source
from pyspark.sql.functions import first

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')   

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
edw_scope = common_configs['secrets']['edw_scope']
emailid = common_configs["alerting"]["unpaid_fee_alert"]["email"]
altrx_schema = common_configs['schema']['altrx_schema']
dq_catalog = common_configs['schema']['data_quality_catalog']
tm_analytics_image_loc = '../shared/tm_analytics.jpg'
print(edw_scope, emailid)
print(trgt_catalog,tmngpdb_catalog, altrx_schema,dq_catalog)
data_layer = "bronze"

# COMMAND ----------

import datetime
import pytz
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_unpaid_fee_alert'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

sale_tran = read_data_from_oracle_conn_dsu(
    sql_query="select * from FORECAST.VW_TM_SALE_TRAN", 
    schema_name="",
    secrets_name=edw_scope,
)
#sale_tran.display()

# COMMAND ----------

from pyspark.sql.functions import when, col, size, expr, regexp_count, lit
sale_tran = sale_tran.withColumn(
  "PRJCT_CD", when(regexp_count(col('PRJCT_CD'), lit(r"[A-Za-z]")) > 0, col("PSTNG_REF_TX")).otherwise(col("PRJCT_CD"))
  )

#display(sale_tran)

# COMMAND ----------

filtered_sale_tran = sale_tran.filter(
    (col("REV_SRC_CD") == "6001") |
    (col("REV_SRC_CD") == "7001") |
    (col("REV_SRC_CD") == "7007") |
    (col("REV_SRC_CD") == "7009") |
    (col("REV_SRC_CD") == "7931") |
    (col("REV_SRC_CD") == "7933") |
    (col("REV_SRC_CD") == "7017")
)

#display(filtered_sale_tran)

# COMMAND ----------

# DBTITLE 1,INPUT
dbx_biblio = spark.sql(f""" select 
    SER_NUM,
    TEST_PCTRAM_LINK,
    LAW_OFFICE,
    FILING_BASIS_CUR,
    FILING_METHOD_FILED,
    FILING_METHOD_CUR,
    FILING_BASIS_FIL,
    FILING_BASIS_AMED,
    REGISTRATION_NUMBER,
    AM_FLG_66A_FIL,
    AM_FLG_44D_FIL,
    AM_FLG_44E_FIL,
    FLG_PAPER_FIL,
    AM_STAT,
    AM_FLG_NO_BAS_FIL,
    AM_FLG_TEASRF_FIL,
    AM_FLG_USE_FIL,
    AM_FLG_ITU_FIL,
    AM_FLG_TEASPL_FIL,
    LAST_MODIFIED_DATE,
    FILING_BASIS_GRP,
    MARK_DWG_CD,
    MARK_DWG_DESC,
    MARK_NM_SHORT,
    MARK_NM,
    TMNG_IMAGE_LINK,
    TM_ANALYTICS_TS,
    EXMR_EID
  from {trgt_catalog}.silver.bibliography""")

#dbx_biblio.display()

# COMMAND ----------

# DBTITLE 1,Input
dbx_milestone = spark.sql(f"""select
ser_num as RIGHT_SER_NUM,
first_action_dt_ph as RIGHT_1st_Action_DT_PH,
am_1_actn_ct_dt as RIGHT_AM_1_ACTN_CT_DT,
first_action_type as RIGHT_1st_Action_Type,
filing_dt as RIGHT_FILING_DT,
ib_notification_dt as RIGHT_IB_NOTIFICATION_DT,
published_dt as RIGHT_PUBLISHED_DT,
noa_dt as RIGHT_NOA_DT,
abandonment_dt as RIGHT_ABANDONMENT_DT,
aban_dt_ph as RIGHT_ABAN_DT_PH,
registration_dt as RIGHT_REGISTRATION_DT,
disposal_type as RIGHT_Disposal_Type,
ext1_dt as RIGHT_EXT1_DT,
ext2_dt as RIGHT_EXT2_DT,
ext3_dt as RIGHT_EXT3_DT,
ext4_dt as RIGHT_EXT4_DT,
ext5_dt as RIGHT_EXT5_DT,
cancellation_dt as RIGHT_CANCELLATION_DT,
renewal_dt as RIGHT_RENEWAL_DT,
revival_dt as RIGHT_REVIVAL_DT,
susp_check_dt as RIGHT_SUSP_CHECK_DT,
am_cls_ct_actv as RIGHT_AM_CLS_CT_ACTV,
pendency_cal_start_dt as RIGHT_Pendency_Cal_Start_DT,
pendency_cal_end_dt as RIGHT_Pendency_Cal_End_DT,
noa_registration_check as RIGHT_noa_registration_check,
wgtd_1st_actn_pendency as RIGHT_WGTD_1ST_ACTN_PENDENCY,
first_action_cd as RIGHT_1st_Action_CD,
disposal_pendency as RIGHT_DISPOSAL_PENDENCY,
suspension as RIGHT_Suspension,
ttab as RIGHT_TTAB,
disposal_dt as RIGHT_Disposal_DT,
dock_dt as RIGHT_DOCK_DT,
am_flg_66a_cur as RIGHT_AM_FLG_66A_CUR,
am_flg_66a_fil as RIGHT_AM_FLG_66A_FIL,
noa_dt_ph as RIGHT_NOA_DT_PH,
filing_fy as RIGHT_Filing_FY,
non_pro_se as RIGHT_non_pro_se,
first_action_pendency_ph as RIGHT_first_action_pendency_ph,
last_modified_date as RIGHT_LAST_MODIFIED_DATE,
processing_pend as RIGHT_PROCESSING_PEND,
processing_pend_days as RIGHT_PROCESSING_PEND_Days
from {trgt_catalog}.silver.milestone""")

#display(dbx_milestone)

# COMMAND ----------

from pyspark.sql import functions as F

renamed_df1 = dbx_biblio.select(*[F.col(col).alias(col) for col in dbx_biblio.columns])
renamed_df2 = dbx_milestone.select(*[F.col(col).alias(col) for col in dbx_milestone.columns])

joined_df = renamed_df1.join(
    renamed_df2, 
    renamed_df1["SER_NUM"] == renamed_df2["RIGHT_SER_NUM"], 
    "inner"
)
#display(joined_df)

# COMMAND ----------

# DBTITLE 1,Input
dbx_divisionals = spark.sql(f"""select 
ser_num as RIGHT_SER_NUM,
filing_dt as FILING_DT,
ib_notification_dt as IB_NOTIFICATION_DT,
dv_type as DV_TYPE,
ref_ser_num as REF_SER_NUM,
dv_dt_rqst as DV_DT_RQST,
dv_dt_complete as DV_DT_COMPLETE,
last_modified_date as RIGHT_LAST_MODIFIED_DATE,
trans_dt as TRANS_DT
from {trgt_catalog}.silver.divisionals""")

# COMMAND ----------

from pyspark.sql import functions as F

renamed_df1 = joined_df.select(*[F.col(col).alias(col) for col in joined_df.columns])
renamed_df2 = dbx_divisionals.select(*[F.col(col).alias(col) for col in dbx_divisionals.columns])

joined_df1 = renamed_df1.join(
    renamed_df2, 
    renamed_df1["SER_NUM"] == renamed_df2["RIGHT_SER_NUM"], 
    "inner"
).drop(renamed_df2["RIGHT_SER_NUM"])\
 .drop(renamed_df2["RIGHT_LAST_MODIFIED_DATE"])\
 .withColumnRenamed("RIGHT_SER_NUM", "Left_Right_SER_NUM")\
 .withColumnRenamed("LAST_MODIFIED_DATE", "Left_Right_LAST_MODIFIED_DATE")
#display(joined_df1)

# COMMAND ----------

from pyspark.sql import functions as F

renamed_df1 = filtered_sale_tran.select(*[F.col(col).alias(col) for col in filtered_sale_tran.columns])
renamed_df2 = joined_df1.select(*[F.col(col).alias(col) for col in joined_df1.columns])

joined_df2 = renamed_df1.join(
    renamed_df2, 
    renamed_df1["PRJCT_CD"] == renamed_df2["SER_NUM"], 
    "inner"
).select(
    renamed_df1["ACCTG_DT"],
    renamed_df1["PSTNG_REF_TX"],
    renamed_df1["PRJCT_CD"],
    renamed_df1["MAILROOM_DT"],
    renamed_df1["REV_SRC_CD"],
    renamed_df1["FEE_AM"],
    renamed_df1["UNIT_QT"],
    renamed_df1["TRAN_AM"],
    renamed_df1["DOC_STATUS_CD"],
    renamed_df1["TRAN_STATUS_CD"],
    renamed_df1["DOC_CLSFCN_CD"],
    renamed_df1["TRAN_PSTNG_REF_TX"],
    renamed_df2["SER_NUM"],
    renamed_df2["TEST_PCTRAM_LINK"],
    renamed_df2["LAW_OFFICE"],
    renamed_df2["FILING_BASIS_CUR"],
    renamed_df2["FILING_METHOD_FILED"],
    renamed_df2["FILING_METHOD_CUR"],
    renamed_df2["FILING_BASIS_FIL"],
    renamed_df2["FILING_BASIS_AMED"],
    renamed_df2["REGISTRATION_NUMBER"],
    renamed_df2["AM_FLG_66A_FIL"],
    renamed_df2["AM_FLG_44D_FIL"],
    renamed_df2["AM_FLG_44E_FIL"],
    renamed_df2["FLG_PAPER_FIL"],
    renamed_df2["AM_STAT"],
    renamed_df2["AM_FLG_NO_BAS_FIL"],
    renamed_df2["AM_FLG_TEASRF_FIL"],
    renamed_df2["AM_FLG_USE_FIL"],
    renamed_df2["AM_FLG_ITU_FIL"],
    renamed_df2["AM_FLG_TEASPL_FIL"],
    renamed_df2["Left_Right_LAST_MODIFIED_DATE"],
    renamed_df2["FILING_BASIS_GRP"],
    renamed_df2["MARK_DWG_CD"],
    renamed_df2["MARK_DWG_DESC"],
    renamed_df2["MARK_NM_SHORT"],
    renamed_df2["MARK_NM"],
    renamed_df2["TMNG_IMAGE_LINK"],
    renamed_df2["TM_ANALYTICS_TS"],
    renamed_df2["EXMR_EID"],
    renamed_df2["Left_Right_SER_NUM"],
    renamed_df2["RIGHT_1st_Action_DT_PH"].alias("1st_Action_DT_PH"),
    renamed_df2["RIGHT_AM_1_ACTN_CT_DT"].alias("AM_1_ACTN_CT_DT"),
    renamed_df2["RIGHT_1st_Action_Type"].alias("1st_Action_Type"),
    renamed_df2["RIGHT_FILING_DT"].alias("FILING_DT"),
    renamed_df2["RIGHT_IB_NOTIFICATION_DT"].alias("IB_NOTIFICATION_DT"),
    renamed_df2["RIGHT_PUBLISHED_DT"].alias("PUBLISHED_DT"),
    renamed_df2["RIGHT_NOA_DT"].alias("NOA_DT"),
    renamed_df2["RIGHT_ABANDONMENT_DT"].alias("ABANDONMENT_DT"),
    renamed_df2["RIGHT_ABAN_DT_PH"].alias("ABAN_DT_PH"),
    renamed_df2["RIGHT_REGISTRATION_DT"].alias("REGISTRATION_DT"),
    renamed_df2["RIGHT_Disposal_Type"].alias("Disposal_Type"),
    renamed_df2["RIGHT_EXT1_DT"].alias("EXT1_DT"),
    renamed_df2["RIGHT_EXT2_DT"].alias("EXT2_DT"),
    renamed_df2["RIGHT_EXT3_DT"].alias("EXT3_DT"),
    renamed_df2["RIGHT_EXT4_DT"].alias("EXT4_DT"),
    renamed_df2["RIGHT_EXT5_DT"].alias("EXT5_DT"),
    renamed_df2["RIGHT_CANCELLATION_DT"].alias("CANCELLATION_DT"),
    renamed_df2["RIGHT_RENEWAL_DT"].alias("RENEWAL_DT"),
    renamed_df2["RIGHT_REVIVAL_DT"].alias("REVIVAL_DT"),
    renamed_df2["RIGHT_SUSP_CHECK_DT"].alias("SUSP_CHECK_DT"),
    renamed_df2["RIGHT_AM_CLS_CT_ACTV"].alias("AM_CLS_CT_ACTV"),
    renamed_df2["RIGHT_Pendency_Cal_Start_DT"].alias("Pendency_Cal_Start_DT"),
    renamed_df2["RIGHT_Pendency_Cal_End_DT"].alias("Pendency_Cal_End_DT"),
    renamed_df2["RIGHT_noa_registration_check"].alias("NOA_REGISTRATION_CHECK"),
    renamed_df2["RIGHT_WGTD_1ST_ACTN_PENDENCY"].alias("WGTD_1ST_ACTN_PENDENCY"),
    renamed_df2["RIGHT_1st_Action_CD"].alias("1st_Action_CD"),
    renamed_df2["RIGHT_DISPOSAL_PENDENCY"].alias("DISPOSAL_PENDENCY"),
    renamed_df2["RIGHT_Suspension"].alias("Suspension"),
    renamed_df2["RIGHT_TTAB"].alias("TTAB"),
    renamed_df2["RIGHT_Disposal_DT"].alias("Disposal_DT"),
    renamed_df2["RIGHT_DOCK_DT"].alias("DOCK_DT"),
    renamed_df2["RIGHT_AM_FLG_66A_CUR"].alias("AM_FLG_66A_CUR"),
    renamed_df2["Right_AM_FLG_66A_FIL"].alias("AM_FLG_66A_FIL"),
    renamed_df2["RIGHT_NOA_DT_PH"].alias("NOA_DT_PH"),
    renamed_df2["RIGHT_Filing_FY"].alias("Filing_FY"),
    renamed_df2["RIGHT_non_pro_se"].alias("non_pro_se"),
    renamed_df2["RIGHT_first_action_pendency_ph"].alias("1st Action Pendency_PH"),
    renamed_df2["Left_Right_LAST_MODIFIED_DATE"].alias("Right_LAST_MODIFIED_DATE"),
    renamed_df2["RIGHT_PROCESSING_PEND"].alias("PROCESSING_PEND"),
    renamed_df2["Left_Right_SER_NUM"].alias("Right_SER_NUM"),
    renamed_df2["Right_FILING_DT"].alias("FILING_DT"),
    renamed_df2["Right_IB_NOTIFICATION_DT"].alias("IB_NOTIFICATION_DT"),
    renamed_df2["DV_TYPE"].alias("DV_TYPE"),
    renamed_df2["REF_SER_NUM"].alias("REF_SER_NUM"),
    renamed_df2["DV_DT_RQST"].alias("DV_DT_RQST"),
    renamed_df2["DV_DT_COMPLETE"].alias("DV_DT_COMPLETE"),
    renamed_df2["Right_LAST_MODIFIED_DATE"].alias("LAST_MODIFIED_DATE"),
    renamed_df2["TRANS_DT"].alias("TRANS_DT"),
)

#display(joined_df2)

# COMMAND ----------

from pyspark.sql import functions as F

joined_df2 = joined_df2.withColumn(
    "DAYS_BTW_POSTED_AND_PEND_START_DT",
    F.datediff(F.col("ACCTG_DT"), F.col("Pendency_Cal_Start_DT"))
)

joined_df2 = joined_df2.withColumn(
    "FEE_FLAG",
    F.when(
        (F.col("FILING_BASIS_FIL") == 'MADRID') & (F.col("DAYS_BTW_POSTED_AND_PEND_START_DT") <= 50) |
        (F.col("FILING_BASIS_FIL") != 'MADRID') & (F.col("DAYS_BTW_POSTED_AND_PEND_START_DT") <= 15),
        1
    ).otherwise(0)
)

joined_df2 = joined_df2.withColumn(
    "Registration Flag",
    F.when(
        F.col("REGISTRATION_DT").isNull() | (F.col("ACCTG_DT") < F.col("REGISTRATION_DT")),
        1
    ).otherwise(0)
)

#display(joined_df2)

# COMMAND ----------

true_condition_df = joined_df2.filter(
    (F.col("Registration Flag") == 1) & 
    (F.col("Filing_FY") >= 2010)
)

#display(true_condition_df)

# COMMAND ----------

# DBTITLE 1,<1
from pyspark.sql import functions as F

Credit_Flag_df = (
    true_condition_df.filter(F.col("TRAN_AM") < 0)
    .groupBy("PRJCT_CD")
    .agg(F.lit(1).alias("Credit_Flag"))
)
#display(Credit_Flag_df)

# COMMAND ----------

# DBTITLE 1,=1
free_flag_df = true_condition_df.filter(F.col("FEE_FLAG") == 1)
#display(free_flag_df)

# COMMAND ----------

find_replace_df = Credit_Flag_df.join(free_flag_df, on="PRJCT_CD", how="right")
#display(find_replace_df)

# COMMAND ----------

from pyspark.sql.functions import col, when

updated_df = find_replace_df.withColumn(
    "TRAN_STATUS_CD",
    when(col("Credit_Flag") == 1, "A").otherwise(col("TRAN_STATUS_CD"))
).withColumn(
    "UNIT_QT",
    when((col("TRAN_STATUS_CD") == "R") | (col("TRAN_AM") < 0), 0).otherwise(col("UNIT_QT"))
)

#display(updated_df)

# COMMAND ----------

# DBTITLE 1,YR SUMMARY FOR COMPARISION
from pyspark.sql.functions import sum, col

yr_summary_df = updated_df.groupBy("FILING_FY")\
    .agg(sum("UNIT_QT").cast("int").alias("sum_unit_qt"))\
    .withColumnRenamed("FILING_FY", "filing_fy")

#display(yr_summary_df)

# COMMAND ----------

# DBTITLE 1,FIXED COUNTS BY SER_NUM
from pyspark.sql.functions import sum, col

fixed_count_df = updated_df.groupBy("SER_NUM")\
    .agg(sum("UNIT_QT").cast("int").alias("fixed_count"))\
    .withColumnRenamed("SER_NUM", "ser_num")

#display(fixed_count_df)

# COMMAND ----------

from pyspark.sql.functions import col, when, sum

unit_qt_df = (
    true_condition_df.withColumn(
        "UNIT_QT",
        when((col("TRAN_STATUS_CD") == "R") | (col("TRAN_AM") < 0), 0).otherwise(col("UNIT_QT"))
    )
    .groupBy("SER_NUM")
    .agg(sum("UNIT_QT").alias("realtime_count"))
    .withColumnRenamed("SER_NUM", "ser_num")
)

#display(unit_qt_df)

# COMMAND ----------

from pyspark.sql import functions as F

fixed_count_df = fixed_count_df.join(
    unit_qt_df, 
    fixed_count_df["ser_num"] == unit_qt_df["ser_num"], 
    "right"
).select(unit_qt_df.ser_num.alias("ser_num"),
          fixed_count_df.fixed_count.alias("fixed_count"),
          unit_qt_df.realtime_count.cast("int").alias("realtime_count"))

#display(fixed_count_df)

# COMMAND ----------

# DBTITLE 1,Replce null with zero.
fixed_count_df = fixed_count_df.fillna({ "fixed_count": 0, "realtime_count": 0})
#display(fixed_count_df)

# COMMAND ----------

# DBTITLE 1,Input
class_dbx= spark.sql(f"""select 
class_status as Class_Status,
class as CLASS,
ser_num as SER_NUM,
cl_cls_us_ct as CL_CLS_US_CT,
cl_cls_us as CL_CLS_US,
cl_dt_stat as CL_DT_STAT,
cl_flg_anoth_form as CL_FLG_ANOTH_FORM,
vt_ser_num as VT_SER_NUM,
vt_class as VT_Class,
goods_and_services_desc
from {trgt_catalog}.silver.class""")

# COMMAND ----------

# DBTITLE 1,Input
trademark_dbx = spark.sql(f"""
select tm.serial_num_tx as AM_SER_NUM,
tm.legacy_status_cd as AM_STAT
from {tmngpdb_catalog}.bronze.trademark tm""")
#display(trademark_dbx)

# COMMAND ----------

from pyspark.sql import functions as F

joined_df3 = class_dbx.join(
    trademark_dbx, 
    class_dbx["SER_NUM"] == trademark_dbx["AM_SER_NUM"], 
    "inner"
)

#display(joined_df3)

# COMMAND ----------

from pyspark.sql import functions as F

joined_df3 = class_dbx.join(
    trademark_dbx.filter((trademark_dbx["AM_STAT"] == 630) | (trademark_dbx["AM_STAT"] == 638)), 
    class_dbx["SER_NUM"] == trademark_dbx["AM_SER_NUM"], 
    "inner"
)

#display(joined_df3)

# COMMAND ----------

fee_waived_df = class_dbx.filter(class_dbx["Class_Status"] != "FEE WAIVED")
#display(fee_waived_df)

# COMMAND ----------

from pyspark.sql import functions as F

joined_df4 = joined_df3.alias("df3").join(
    fee_waived_df.alias("fee_waived"), 
    joined_df3["SER_NUM"] == fee_waived_df["SER_NUM"], 
    "inner"
).select(
  F.col("df3.Class_Status"),
  F.col("df3.Class"),
  F.col("df3.SER_NUM"),
  F.col("df3.CL_CLS_US_CT"),
  F.col("df3.CL_CLS_US"),
  F.col("df3.CL_DT_STAT"),
  F.col("df3.CL_FLG_ANOTH_FORM"),
  F.col("df3.VT_SER_NUM"),
  F.col("df3.VT_Class"),
  F.col("df3.goods_and_services_desc"),
  F.col("df3.AM_SER_NUM"),
  F.col("df3.AM_STAT"),
  F.col("fee_waived.Class_Status").alias("RIGHT_Class_Status"),
  F.col("fee_waived.CLASS").alias("RIGHT_CLASS"),
  F.col("fee_waived.SER_NUM").alias("RIGHT_SER_NUM"),
  F.col("fee_waived.CL_CLS_US_CT").alias("RIGHT_CL_CLS_US_CT"),
  F.col("fee_waived.CL_CLS_US").alias("RIGHT_CL_CLS_US"),
  F.col("fee_waived.CL_DT_STAT").alias("RIGHT_CL_DT_STAT"),
  F.col("fee_waived.CL_FLG_ANOTH_FORM").alias("RIGHT_CL_FLG_ANOTH_FORM"),
  F.col("fee_waived.VT_SER_NUM").alias("RIGHT_VT_SER_NUM"),
  F.col("fee_waived.VT_Class").alias("RIGHT_VT_Class"),
  F.col("fee_waived.goods_and_services_desc").alias("RIGHT_goods_and_services_desc"),
)

#display(joined_df4)

# COMMAND ----------

tram_status_df = joined_df4.groupBy("SER_NUM", "AM_STAT").agg(
    F.countDistinct("Class").alias("countdistinct_class")
).withColumnRenamed("SER_NUM", "ser_num")\
 .withColumnRenamed("AM_STAT", "tram_status")

#display(tram_status_df)

# COMMAND ----------

from pyspark.sql import functions as F

joined_df5 = fixed_count_df.join(
    tram_status_df, 
    fixed_count_df["ser_num"] == tram_status_df["ser_num"], 
    "inner"
).select(
    fixed_count_df["ser_num"].alias("ser_num"),
    fixed_count_df["realtime_count"].alias("fee_paid"),
    tram_status_df["countdistinct_class"].alias("tram_classes"),
    tram_status_df["tram_status"]
)

#display(joined_df5)

# COMMAND ----------

dbx_stnd_business = spark.sql(f"""
select right(be.cfk_object_gid,8) as SERIAL_NUMBER,
ber.BUSINESS_EVENT_REASON_CD as CM_ENT_CD
from {tmngpdb_catalog}.bronze.business_event be left join
{tmngpdb_catalog}.bronze.stnd_business_event_reason ber 
on be.fk_business_event_reason_id = ber.business_event_reason_id
where (ber.BUSINESS_EVENT_REASON_CD like 'DRRR%')
  or (ber.BUSINESS_EVENT_REASON_CD like 'PARI%')
  or (ber.BUSINESS_EVENT_REASON_CD like 'PWFG%')""")

#display(dbx_stnd_business)

# COMMAND ----------

grouped_dbx_stnd_business = dbx_stnd_business.groupBy("SERIAL_NUMBER").count()
grouped_dbx_stnd_business = grouped_dbx_stnd_business.drop("count")
#display(grouped_dbx_stnd_business)

# COMMAND ----------

from pyspark.sql import functions as F

joined_df6 = joined_df5.join(
    grouped_dbx_stnd_business, 
    grouped_dbx_stnd_business["SERIAL_NUMBER"] == joined_df5["ser_num"], 
    "left_anti"
).select(
    joined_df5["ser_num"],
    joined_df5["fee_paid"],
    joined_df5["tram_classes"],
    joined_df5["tram_status"]
)


#display(joined_df6)

# COMMAND ----------

# DBTITLE 1,Replce null with zero
joined_df6 = joined_df6.fillna({"fee_paid": 0, "tram_classes": 0})
#display(joined_df6)

# COMMAND ----------

from pyspark.sql.functions import col, when

joined_df6 = joined_df6.withColumn("delta", col("fee_paid") - col("tram_classes"))
joined_df6 = joined_df6.withColumn("discrepancy_type", when(col("delta") < 0, "Underpayment").otherwise("Overpayment"))
#display(joined_df6)

# COMMAND ----------

# DBTITLE 1,Join
joined_df6 = joined_df6.filter(col("delta") != 0)
joined_df6 = joined_df6.filter(col("discrepancy_type") == "Underpayment")
#display(joined_df6)

# COMMAND ----------

from pyspark.sql.functions import col
filtered_joined_df6 = joined_df6.select(
    "ser_num", 
    "fee_paid", 
    "tram_classes", 
    col("delta").alias("unpaid_classes"), 
    "tram_status"
).orderBy(col("unpaid_classes").asc())

#display(filtered_joined_df6)

# COMMAND ----------

# DBTITLE 1,unpaid_classes
from pyspark.sql.functions import sum, countDistinct

unpaid_classes = filtered_joined_df6.agg(
    sum("unpaid_classes").alias("Total Unpaid Classes"),
    countDistinct("ser_num").alias("Total Cases")
)

#display(unpaid_classes)

# COMMAND ----------

# DBTITLE 1,Input
dbx_filing_db = spark.sql(f"""select 
ser_num as SER_NUM,
pendency_cal_start_dt as Pendency_Cal_Start_DT,
filing_fy as Filing_FY,
non_pro_se as NON_PRO_SE,
filing_method_filed as FILING_METHOD_FILED,
filing_basis_grp as FILING_BASIS_GRP,
class as Class,
name as NAME,
city as CITY,
ste_ctry_cd as STE_CTRY_CD,
postal_cd as POSTAL_CD,
ctry_nm as CTRY_NM,
country_or_area_name as Country_Or_Area_Name,
count as Count,
max_pendency_cal_start_dt as Max_Pendency_Cal_Start_DT,
coordinated_class as Coordinated_Class,
filing_fy2 as Filing_FY2,
filing_fy_month_int as Filing_FY_Month_Int,
filing_fy_quarter as Filing_FY_Quarter,
filing_fy_month as Filing_FY_Month,
top_2_years as Top_2_Years,
fee_paid_class as Fee_Paid_Class,
max_filing_fy as Max_Filing_FY,
pctram_link as PCTRAM_LINK,
fixed_count as Fixed_Count,
realtime_count as Realtime_Count,
tram_count as TRAM_Count,
goods_or_services as Goods_Or_Services,
concat_goods_or_services as Concat_Goods_Or_Services,
entity_type as ENTITY_TYPE,
applicant_bin as Applicant_Bin
from {trgt_catalog}.gold.filings_dashboard""")
#display(dbx_filing_db)

# COMMAND ----------

grouped_dbx_filing_db = dbx_filing_db.groupBy(
    "SER_NUM",
    "Country_Or_Area_Name",
    "NAME",
    "Applicant_Bin",
    "FILING_BASIS_GRP",
    "NON_PRO_SE",
    "ENTITY_TYPE",
    "FILING_METHOD_FILED"
).count()
grouped_dbx_filing_db = grouped_dbx_filing_db.drop("count")
#display(grouped_dbx_filing_db)

# COMMAND ----------

filing_db_joined_df6 = filtered_joined_df6.join(
    grouped_dbx_filing_db,
    filtered_joined_df6["ser_num"] == grouped_dbx_filing_db["SER_NUM"],
    "inner"
)


#display(filing_db_joined_df6)

# COMMAND ----------

from pyspark.sql.functions import sum

filing_db_df6 = filing_db_joined_df6.groupBy(
    filing_db_joined_df6["Country_Or_Area_Name"].alias("country_or_area_name"),
    filing_db_joined_df6["NAME"].alias("name"),
    filing_db_joined_df6["Applicant_Bin"].alias("applicant_bin"),
    filing_db_joined_df6["FILING_BASIS_GRP"].alias("filing_basis_grp"),
    filing_db_joined_df6["NON_PRO_SE"].alias("non_pro_se"),
    filing_db_joined_df6["ENTITY_TYPE"].alias("entity_type"),
    filing_db_joined_df6["FILING_METHOD_FILED"].alias("filing_method_filed")
).agg(sum("unpaid_classes").alias("sum_unpaid_classes"))

#display(filing_db_df6)

# COMMAND ----------

# DBTITLE 1,crossjoin
append_join = filing_db_df6.crossJoin(unpaid_classes)
append_join = append_join.withColumnRenamed("Total Unpaid Classes", "total_unpaid_classes")
append_join = append_join.withColumnRenamed("Total Classes", "total_classes")
#display(append_join)

# COMMAND ----------

# DBTITLE 1,Basic Summary table
# MAGIC %md
# MAGIC Basic Summary Table

# COMMAND ----------

# DBTITLE 1,Summary Table 1&2
summary_tbl1 = unpaid_classes
summary_tbl2 = append_join.groupby("country_or_area_name").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl2 = summary_tbl2.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100
).orderBy(col("Percent Of Unpaid").desc())

summary_tbl2 = summary_tbl2.withColumnRenamed("country_or_area_name", "Top Countries")
summary_tbl2 = summary_tbl2.select("Top Countries", "Percent Of Unpaid").limit(5)

#display(summary_tbl1)

# COMMAND ----------

# DBTITLE 1,SumamryTable 3
summary_tbl3 = append_join.groupby("name").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl3 = summary_tbl3.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100
).orderBy(col("Percent Of Unpaid").desc())

summary_tbl3 = summary_tbl3.withColumnRenamed("name", "Top Applicants")
summary_tbl3 = summary_tbl3.select("Top Applicants","Percent Of Unpaid").limit(5)

#display(summary_tbl3)

# COMMAND ----------

# DBTITLE 1,Summary Table 4
summary_tbl4 = append_join.groupby("applicant_bin").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl4 = summary_tbl4.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100
).orderBy(col("Percent Of Unpaid").desc())

summary_tbl4 = summary_tbl4.withColumnRenamed("applicant_bin", "Applicant Type")
summary_tbl4 = summary_tbl4.select("Applicant Type","Percent Of Unpaid").limit(5)

#display(summary_tbl4)

# COMMAND ----------

# DBTITLE 1,Summary Table 5
summary_tbl5 = append_join.groupby("filing_basis_grp").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl5 = summary_tbl5.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100
).orderBy(col("Percent Of Unpaid").desc())

summary_tbl5 = summary_tbl5.withColumnRenamed("filing_basis_grp", "Basis")
summary_tbl5 = summary_tbl5.select("Basis","Percent Of Unpaid").limit(5)

#display(summary_tbl5)

# COMMAND ----------

# DBTITLE 1,Summary Table 6
summary_tbl6 = append_join.groupby("non_pro_se").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl6 = summary_tbl6.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100
).orderBy(col("Percent Of Unpaid").desc())

summary_tbl6 = summary_tbl6.withColumnRenamed("non_pro_se", "NON/PRO SE")
summary_tbl6 = summary_tbl6.select("NON/PRO SE","Percent Of Unpaid").limit(5)

#display(summary_tbl6)

# COMMAND ----------

# DBTITLE 1,Summary Table 7
summary_tbl7 = append_join.groupby("entity_type").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl7 = summary_tbl7.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100
).orderBy(col("Percent Of Unpaid").desc())

summary_tbl7 = summary_tbl7.withColumnRenamed("entity_type", "Entity")
summary_tbl7 = summary_tbl7.select("Entity","Percent Of Unpaid").limit(5)

#display(summary_tbl7)

# COMMAND ----------

# DBTITLE 1,Summary Table 8
summary_tbl8 = append_join.groupby("filing_method_filed").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl8 = summary_tbl8.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100
).orderBy(col("Percent Of Unpaid").desc())

summary_tbl8 = summary_tbl8.withColumnRenamed("filing_method_filed", "Method")
summary_tbl8 = summary_tbl8.select("Method","Percent Of Unpaid").limit(5)

#display(summary_tbl8)

# COMMAND ----------

# DBTITLE 1,Input
dbx_corr = spark.sql(f""" select
ser_num as SER_NUM,
cor_nm as COR_NM,
firm_nm as FIRM_NM,
add_line1 as ADD_LINE1,
add_line2 as ADD_LINE2,
city_nm as CITY_NM,
zipcode as ZIPCODE,
state_cd as STATE_CD,
state_nm as STATE_NM,
ctry_cd as CTRY_CD,
ctry_nm as CTRY_NM,
ctry_name_caps as CTRY_NAME_CAPS,
country_or_area_name as Country_or_Area_Name,
iso_alpha3_code as ISO_ALPHA3_Code,
ip_att_docket_ref as IP_ATT_DOCKET_REF,
atty_nm as ATTY_NM,
domestic_rep as DOMESTIC_REP,
at_email_auth as AT_EMAIL_AUTH,
at_email as AT_EMAIL,
cr_email1 as CR_EMAIL1,
cr_email2 as CR_EMAIL2,
cr_email3 as CR_EMAIL3,
cr_email4 as CR_EMAIL4,
cr_email_auth as CR_EMAIL_AUTH
from {trgt_catalog}.silver.correspondence""")

# COMMAND ----------

from pyspark.sql.functions import first

dbx_corr = dbx_corr.groupBy("ser_num").agg(first("atty_nm").alias("first_atty_nm"))
#display(dbx_corr)

# COMMAND ----------

# DBTITLE 1,Join
joined_df7 = filtered_joined_df6.join(dbx_corr, filtered_joined_df6.ser_num == dbx_corr.ser_num, "inner")
#display(joined_df)

# COMMAND ----------

joined_df7 = joined_df7.groupBy("first_atty_nm").agg({"unpaid_classes": "sum"}).withColumnRenamed("sum(unpaid_classes)", "total_unpaid_classes")
#display(joined_df7)

# COMMAND ----------

# DBTITLE 1,cross Join
append_join1 = filing_db_df6.crossJoin(joined_df7)

# COMMAND ----------

# DBTITLE 1,summary table 9
summary_tbl9 = append_join1.groupby("first_atty_nm").agg(
    sum("sum_unpaid_classes").alias("sum_sum_unpaid_classes"),
    first("total_unpaid_classes").alias("first_total_unpaid_classes")
)
summary_tbl9 = summary_tbl9.withColumn(
    "Percent Of Unpaid",
    (col("sum_sum_unpaid_classes") / col("first_total_unpaid_classes")) * 100).orderBy(col("Percent Of Unpaid").desc())

summary_tbl9 = summary_tbl9.withColumnRenamed("first_atty_nm", "Top Attorney")
summary_tbl9 = summary_tbl9.select("Top Attorney", "Percent Of Unpaid").limit(5)

#display(summary_tbl9)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Writing to DBX

# COMMAND ----------

from pyspark.sql.functions import current_date

filtered_joined_df6 = filtered_joined_df6.withColumn("run_date", current_date())

target_table = f"{trgt_catalog}.gold.unpaid_fees_alert_history"
target_df = spark.table(target_table)

union_df = filtered_joined_df6.union(target_df)
union_df = union_df.orderBy(col("run_date").desc())
#display(union_df)

# COMMAND ----------

union_df.write.mode("overwrite").format("delta").insertInto(target_table)

# COMMAND ----------

#############################################################################################
# 5/2/25 - Commented out data quality check code since it has been succeeding consistently. #
# Allows disabling Alteryx workflow schedule fully, saving resources.                       #
#############################################################################################

# # data quality entry
# tbl1 = f"{trgt_catalog}.gold.unpaid_fees_alert_history"
# tbl2 = f"hive_metastore.{altrx_schema}.unpaid_fees_alert_history"
# key_cols = ['ser_num']
# dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog) 
# print(dq_result)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Email Output

# COMMAND ----------

# DBTITLE 1,Parameterization for Email Output
from datetime import date
excel = f"Report Run Date {date.today()}.xlsx"

title_tx_1A = """Greetings!"""
title_tx_1B = f"Here are some metrics on unpaid classes as of {date.today()}. These are cases in statuses; 630 - New Application - Record initialized not assigned to examiner"
title_tx_1C = """638 - New Application assigned to examiner, Attached is the full list of serial numbers with details."""
title_tx_2 = f"""Current Unpaid Fees (Status 630 and 638)"""
title_tx_3 = """Breakdown by Characteristics"""

title_tx_4 = """Top (5) List"""
title_tx_5 = f'For any questions/comments, please email trademark_analytics@uspto.gov'

from_addr= 'trademark_analytics@uspto.gov'
#uspto_image_loc  = './../shared/tm_analytics.jpg'
subj = f"Auto-generated: Unpaid_Fees_Alert_Report {date.today()}"

# COMMAND ----------

import pandas as pd
import tempfile
import os
from datetime import date
import io

 
def excel_prep(excel, summary_tbl1,summary_tbl4, summary_tbl2, summary_tbl5, summary_tbl3, summary_tbl6, summary_tbl9, summary_tbl8, summary_tbl7, title_tx_1A,title_tx_1B,title_tx_1C, title_tx_2, title_tx_3, title_tx_4, title_tx_5) -> None:
    """This function generates prepared excel document with headers, styling and footer per user preference"""
    import datetime
 
    properties = {"border": "5px solid grey",  "text-align": "center", "font-size" : "20px"}
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(excel, engine='xlsxwriter')
    # write table 1 from row 5
    summary_tbl1.style.set_properties(**properties).to_excel(writer, sheet_name='Sheet1', startrow=4, index=False, header=True)

    workbook  = writer.book
    worksheet = writer.sheets['Sheet1']

    worksheet.set_column(0, 6, 25)
    worksheet.set_row(0, 25)
    merge_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'align': 'center',
        'font_size': 15})
    merge_format_title = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'align': 'left',
        'font_size': 15})
     # Add a header format.
    header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
     'font_size' : 10,
     'font_color' : 'white',
    'valign': 'top',
    'align': 'center',
    'fg_color': '#154468',
    'border': 1})
    for col_num, value in enumerate(summary_tbl1.columns.values):
        worksheet.write(4, col_num, value, header_format) # format headers at row 5

    worksheet.merge_range('A1:E1', title_tx_1A, merge_format_title) #row 1 with index of 0
    worksheet.merge_range('A2:E2', title_tx_1B, merge_format_title)
    worksheet.merge_range('A3:E3', title_tx_1C, merge_format_title)
    worksheet.merge_range('A4:E4', title_tx_2, merge_format) 
    worksheet.merge_range('A46:E46', title_tx_5, merge_format_title)

    worksheet.merge_range(f"A{summary_tbl1.shape[0] +6}:B{summary_tbl1.shape[0] +6}", title_tx_3, merge_format)
    worksheet.merge_range(f"D{summary_tbl1.shape[0] +6}:E{summary_tbl1.shape[0] +6}", title_tx_4, merge_format)
    summary_tables = [(summary_tbl4, summary_tbl2), (summary_tbl5, summary_tbl3), (summary_tbl6, summary_tbl9), (summary_tbl8, summary_tbl7)]
    start_row = summary_tbl1.shape[0] + 8
    start_col = 0
   
    for i, value in enumerate(summary_tables):
      start_col = 0
      for df in value:
        df.style.set_properties(**properties).to_excel(writer, sheet_name='Sheet1', startrow=start_row + 1,startcol=start_col, index=False, header=True)
        # Write the column headers with the defined format.
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(start_row + 1, start_col + col_num, value, header_format)
      
        start_col = 3
      start_row += df.shape[0] + 2
    worksheet.merge_range(f'B{start_row + 2}:D{start_row +8}', '', merge_format_title)
    #worksheet.insert_image(f'C{start_row + 2}', uspto_image_loc, {'x_scale': 0.6, 'y_scale': 0.5, 'object_position': 1, "x_offset": 10, "y_offset": 5})
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    print("done with excel")

# COMMAND ----------

# DBTITLE 1,Function to create excel with all tables
import pandas as pd
import tempfile
import os
from datetime import date

# Convert union_df to Pandas DataFrame
union_df_pd = union_df.toPandas()

# Save the DataFrame to a temporary directory as an Excel file
with tempfile.TemporaryDirectory() as tmpdir:
    os.makedirs(tmpdir, exist_ok=True)
    
    filepath = f"{tmpdir}/union_df_{date.today()}.xlsx"
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(filepath, engine='xlsxwriter')
    
    # Write the DataFrame to the Excel file
    union_df_pd.to_excel(writer, sheet_name='Sheet1', index=False)
    
    # Close the Pandas Excel writer and output the Excel file.
    writer.close()
    
    dbfs_filepath = f"dbfs:/Unpaid_Fee_Alert_Report{date.today()}.xlsx"
    dbutils.fs.cp(f"file://{filepath}", dbfs_filepath)

    print(f"Excel file saved to: {dbfs_filepath}")

# COMMAND ----------

import tempfile
import os
from datetime import date
import io

if append_join.count() == 0:
    dbutils.notebook.exit("No new 630 or 638 records")
else:
    # Save the DataFrame to a temporary directory as an Excel file
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(tmpdir, exist_ok=True)
        
        filepath1 = f"{tmpdir}/ntb_unpaid_fee_alert_{date.today()}.xlsx"
        filepath = f"/dbfs/Unpaid_Fee_Alert_Report{date.today()}.xlsx"

        # Ensure summary_tbl1 to summary_tbl9 are Spark DataFrames
        summary_tbl1 = summary_tbl1.toPandas() if hasattr(summary_tbl1, 'toPandas') else summary_tbl1
        summary_tbl2 = summary_tbl2.toPandas() if hasattr(summary_tbl2, 'toPandas') else summary_tbl2
        summary_tbl3 = summary_tbl3.toPandas() if hasattr(summary_tbl3, 'toPandas') else summary_tbl3
        summary_tbl4 = summary_tbl4.toPandas() if hasattr(summary_tbl4, 'toPandas') else summary_tbl4
        summary_tbl5 = summary_tbl5.toPandas() if hasattr(summary_tbl5, 'toPandas') else summary_tbl5
        summary_tbl6 = summary_tbl6.toPandas() if hasattr(summary_tbl6, 'toPandas') else summary_tbl6
        summary_tbl7 = summary_tbl7.toPandas() if hasattr(summary_tbl7, 'toPandas') else summary_tbl7
        summary_tbl8 = summary_tbl8.toPandas() if hasattr(summary_tbl8, 'toPandas') else summary_tbl8
        summary_tbl9 = summary_tbl9.toPandas() if hasattr(summary_tbl9, 'toPandas') else summary_tbl9
        
        # Save the Excel file
        excel_prep(
            filepath1,
            summary_tbl1,
            summary_tbl4,
            summary_tbl2,
            summary_tbl5,
            summary_tbl3,
            summary_tbl6,
            summary_tbl9,
            summary_tbl8,
            summary_tbl7,
            title_tx_1A,
            title_tx_1B,
            title_tx_1C,
            title_tx_2,
            title_tx_3,
            title_tx_4,
            title_tx_5
        )
        
        dbfs_filepath1 = f"dbfs:/ntb_unpaid_fee_alert_{date.today()}.xlsx"
        dbutils.fs.cp(f"file://{filepath1}", dbfs_filepath1)
        
        with open(filepath1, 'rb') as xlsx_file:
            out_file = io.StringIO()
            xlsx2html(xlsx_file, out_file, locale='en')
            out_file.seek(0)
            result_html = out_file.read()

        attachments = [filepath]
        
        notify = Notify()
        email_subj = f"Auto-generated: Unpaid_Fees_Alert_Report {date.today()}"

        # Send the email
        send_email_report(
            job_nm = job_name,
            subject = email_subj,
            send_from = from_addr,
            send_to = emailid,
            html_body= result_html,
            attachments=attachments
        )

        print("Email sent")
    print(filepath1)


# COMMAND ----------

recs_count = union_df.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime, 'completed', recs_count, "job completed successfully")

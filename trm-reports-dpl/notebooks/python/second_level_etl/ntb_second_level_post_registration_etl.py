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

# DBTITLE 1,function to generate random checkpoint folder name
def generate_64bit_ID()-> int:
    return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
CHK_POINT_DIR = "/tmp/checkpoints/post_reg_etl/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)
print(f"{trgt_catalog=},{src_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
print(job_start_ts)
job_name = 'ntb_second_level_post_registration_etl'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pull Data from DBs and feed into multiple sub-flows

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Class Data

# COMMAND ----------

# DBTITLE 1,Class
df366 = spark.sql(f"""select * from  {trgt_catalog}.silver.class 
                  where upper(class_status)='ACTIVE' 
                  """).selectExpr("class as Class","ser_num as SER_NUM").groupBy("SER_NUM").agg(f.countDistinct("Class").alias("Active_Classes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Prosecution_History

# COMMAND ----------

# DBTITLE 1,Prosecution_History
df355  = spark.sql(f"""select * from {trgt_catalog}.silver.prosecution_history 
                   """).selectExpr("serial_number as SERIAL_NUMBER","ph_action_number as PH_ACTION_NUMBER","ph_action_code as PH_ACTION_CODE","cm_sys_dt as CM_SYS_DT","ph_action_date as PH_ACTION_DATE","last_modified_date as LAST_MODIFIED_DATE","oracle_apply_time as ORACLE_APPLY_TIME","cm_prcd_num as CM_PRCD_NUM","ri_notif_dt as RI_NOTIF_DT","cm_desc as CM_Desc","fifth_char_cm_type as 5TH_CHAR_CM_TYPE","cm_flg_paper as CM_FLG_PAPER","ttab_tracking_num as TTAB_TRACKING_NUM","tm_worker_eid as TM_WORKER_EID").withColumn("5Characters",f.concat(f.col('PH_ACTION_CODE'),f.col('5TH_CHAR_CM_TYPE'))).cache()

df355.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Registered records from the Milestone

# COMMAND ----------

# DBTITLE 1,Milestone
df354 = spark.sql(f"""select * from {trgt_catalog}.silver.milestone 
                  where registration_dt is not null 
                   """).selectExpr("ser_num as SER_NUM","first_action_dt_ph as 1st_Action_DT_PH","am_1_actn_ct_dt as AM_1_ACTN_CT_DT","first_action_type as 1st_Action_Type","filing_dt as FILING_DT","ib_notification_dt as IB_NOTIFICATION_DT","published_dt as PUBLISHED_DT","noa_dt as NOA_DT","abandonment_dt as ABANDONMENT_DT","aban_dt_ph as ABAN_DT_PH","registration_dt as REGISTRATION_DT","disposal_type as Disposal_Type","ext1_dt as EXT1_DT","ext2_dt as EXT2_DT","ext3_dt as EXT3_DT","ext4_dt as EXT4_DT","ext5_dt as EXT5_DT","cancellation_dt as CANCELLATION_DT","renewal_dt as RENEWAL_DT","revival_dt as REVIVAL_DT","susp_check_dt as SUSP_CHECK_DT","am_cls_ct_actv as AM_CLS_CT_ACTV","pendency_cal_start_dt as Pendency_Cal_Start_DT","pendency_cal_end_dt as Pendency_Cal_End_DT","noa_registration_check as NOA_REGISTRATION_Check","wgtd_1st_actn_pendency as WGTD_1ST_ACTN_PENDENCY","first_action_cd as 1st_Action_CD","disposal_pendency as DISPOSAL_PENDENCY","suspension as Suspension","ttab as TTAB","disposal_dt as Disposal_DT","dock_dt as DOCK_DT","am_flg_66a_cur as AM_FLG_66A_CUR","am_flg_66a_fil as AM_FLG_66A_FIL","noa_dt_ph as NOA_DT_PH","filing_fy as Filing_FY","non_pro_se as NON_PRO_SE","first_action_pendency_ph as 1st_Action_Pendency_PH","last_modified_date as LAST_MODIFIED_DATE")

# COMMAND ----------

df353 = df355.join(df354,df355.SERIAL_NUMBER==df354.SER_NUM,"inner").selectExpr("5Characters","5TH_CHAR_CM_TYPE","CM_Desc","PH_ACTION_CODE","PH_ACTION_DATE","PH_ACTION_NUMBER","REGISTRATION_DT","SERIAL_NUMBER","TM_WORKER_EID")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Biblio Data

# COMMAND ----------

# DBTITLE 1,Bibliography
df790 = spark.sql(f"""select * from {trgt_catalog}.silver.bibliography  
                  """).selectExpr("ser_num as SER_NUM","test_pctram_link as TEST_PCTRAM_LINK","law_office as LAW_OFFICE","filing_basis_cur as FILING_BASIS_CUR","filing_method_filed as FILING_METHOD_FILED","filing_method_cur as FILING_METHOD_CUR","filing_basis_fil as FILING_BASIS_FIL","filing_basis_amed as FILING_BASIS_AMED","registration_number as REGISTRATION_NUMBER","am_flg_66a_fil as AM_FLG_66A_FIL","am_flg_44d_fil as AM_FLG_44D_FIL","am_flg_44e_fil as AM_FLG_44E_FIL","flg_paper_fil as FLG_PAPER_FIL","am_stat as AM_STAT","am_flg_no_bas_fil as AM_FLG_NO_BAS_FIL","am_flg_teasrf_fil as AM_FLG_TEASRF_FIL","am_flg_use_fil as AM_FLG_USE_FIL","am_flg_itu_fil as AM_FLG_ITU_FIL","am_flg_teaspl_fil as AM_FLG_TEASPL_FIL","last_modified_date as LAST_MODIFIED_DATE","filing_basis_grp as FILING_BASIS_GRP","mark_dwg_cd as MARK_DWG_CD","mark_dwg_desc as MARK_DWG_DESC","mark_nm_short as MARK_NM_SHORT","mark_nm as MARK_NM","tmng_image_link as TMNG_IMAGE_LINK","tm_analytics_ts as TM_ANALYTICS_TS","exmr_eid as EXMR_EID")

# COMMAND ----------

df356 = df353.join(df790,df353.SERIAL_NUMBER==df790.SER_NUM,"inner").selectExpr("SERIAL_NUMBER","TM_WORKER_EID","5TH_CHAR_CM_TYPE","'' as Active_Classes","REGISTRATION_NUMBER","FILING_BASIS_CUR","5Characters","PH_ACTION_DATE","PH_ACTION_CODE","CM_Desc","FILING_BASIS_FIL","REGISTRATION_DT","PH_ACTION_NUMBER").cache()

df356.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get Cancelled SER_NUMS

# COMMAND ----------

# DBTITLE 1,Read data from TMNGPDB
df362 = spark.sql(f"""SELECT CAST(split(TM.TRADEMARK_GID,':')[2] AS INTEGER) AS AM_SER_NUM ,TMM.AM_DT_CNCL
FROM {src_catalog}.bronze.TRADEMARK TM LEFT JOIN
(SELECT FK_TRADEMARK_GID,MILESTONE_DT AS AM_DT_CNCL 
FROM {src_catalog}.bronze.TM_MILESTONE WHERE FK_TM_MILESTONE_CD = 'CNCL') TMM
ON TM.TRADEMARK_GID = TMM.FK_TRADEMARK_GID
""")

# COMMAND ----------

df363 = df362.where("IsNull(AM_DT_CNCL)")
# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df363")
# df363 = df363.checkpoint(True)

df363_f = df362.where("NOT IsNull(AM_DT_CNCL)")
# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df363_f")
# df363_f = df363_f.checkpoint(True)
# df363_f = df362.where("NOT IsNull(AM_DT_CNCL)")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Post Registration MilestoneTable

# COMMAND ----------

# MAGIC %md
# MAGIC ###Identify 6 year date

# COMMAND ----------

df709 = df355.filter("5Characters in('8.OKO','8.PRO','C15AO','C15PO','NA85O','NA85E') ")
df710 = df709.groupBy("SERIAL_NUMBER").agg(f.min("PH_ACTION_NUMBER").alias("Min_PH_ACTION_NUMBER"))

df711 = df710.alias("df710").join(df709.alias("df709"),(col("df710.SERIAL_NUMBER") == col("df709.SERIAL_NUMBER")) & (col("df710.Min_PH_ACTION_NUMBER") == col("df709.PH_ACTION_NUMBER")) ,"inner").select(col("df710.SERIAL_NUMBER"),col("df709.PH_ACTION_DATE").alias("6_YR_DT"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Identify Non-Madrid 10 Year

# COMMAND ----------

df700 = df356.where(f.col("PH_ACTION_CODE").contains("RNL")|f.col("PH_ACTION_CODE").contains("REN")).groupBy("SERIAL_NUMBER").agg(f.max("PH_ACTION_NUMBER").alias("Max_PH_ACTION_NUMBER"))

df703 = df356.where(~f.col("PH_ACTION_CODE").contains("RNL") & ~f.col("PH_ACTION_CODE").contains("REN")).select("SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","FILING_BASIS_CUR").distinct()

# COMMAND ----------

df701 = df700.alias("df700").join(df356.alias("df356"),(col("df700.SERIAL_NUMBER") == col("df356.SERIAL_NUMBER")) & (col("df700.Max_PH_ACTION_NUMBER") == col("df356.PH_ACTION_NUMBER")) ,"inner").select(col("df700.SERIAL_NUMBER"),col("df356.PH_ACTION_DATE").alias("LAST_Renewal_DT"),col("df356.REGISTRATION_DT"),col("df356.FILING_BASIS_CUR"),col("df356.REGISTRATION_NUMBER"))

df704_left = df701.alias("df701").join(df703.alias("df703"),(col("df701.SERIAL_NUMBER") == col("df703.SERIAL_NUMBER")) ,"left").select(col("df701.SERIAL_NUMBER"),col("df701.FILING_BASIS_CUR"),col("df701.LAST_Renewal_DT"),col("df701.REGISTRATION_DT"),col("df701.REGISTRATION_NUMBER"))

df704_right = df703.alias("df703").join(df701.alias("df701"),(col("df703.SERIAL_NUMBER") == col("df701.SERIAL_NUMBER")) ,"anti").select(col("df703.SERIAL_NUMBER"),col("df703.FILING_BASIS_CUR"),col("df703.REGISTRATION_DT"),col("df703.REGISTRATION_NUMBER"))
df704 = df704_left.unionByName(df704_right,allowMissingColumns=True)

##Add AM cancelation dates
df707 = df704.join(df362,df704.SERIAL_NUMBER==df362.AM_SER_NUM,"left_outer").select("SERIAL_NUMBER","REGISTRATION_NUMBER","FILING_BASIS_CUR","LAST_Renewal_DT","REGISTRATION_DT","AM_DT_CNCL")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Identify Madrid Records

# COMMAND ----------

df714 = df356.where('FILING_BASIS_CUR = "MADRID"')
df713 = df714.where(f.col("PH_ACTION_CODE").isin("NA71", "NA75", "71AG")) 
df715 = df713.groupBy("SERIAL_NUMBER").agg(f.min("PH_ACTION_NUMBER").alias("Min_PH_ACTION_NUMBER"))
df716 = df713.groupBy("SERIAL_NUMBER").agg(f.max("PH_ACTION_NUMBER").alias("Max_PH_ACTION_NUMBER"))

df718 = df715.alias("df715").join(df714.alias("df714"),(col("df715.SERIAL_NUMBER") == col("df714.SERIAL_NUMBER")) & (col("df715.Min_PH_ACTION_NUMBER") == col("df714.PH_ACTION_NUMBER")) ,"inner").select(col("df715.SERIAL_NUMBER"),col("df714.FILING_BASIS_CUR"),col("df714.PH_ACTION_DATE").alias("6_YR_DT"),col("df714.REGISTRATION_DT"),col("df714.REGISTRATION_NUMBER"))

df721 = df718.withColumn("6_YR",f.when(f.floor((f.months_between("6_YR_DT","REGISTRATION_DT"))/12) < 9,"1").otherwise("0")).where('6_YR = 1')

df717 = df716.alias("df716").join(df714.alias("df714"),(col("df716.SERIAL_NUMBER") == col("df714.SERIAL_NUMBER")) & (col("df716.Max_PH_ACTION_NUMBER") == col("df714.PH_ACTION_NUMBER")) ,"inner").select(col("df716.SERIAL_NUMBER"),col("df714.FILING_BASIS_CUR"),col("df714.PH_ACTION_DATE").alias("10_YR_DT"),col("df714.REGISTRATION_DT"),col("df714.REGISTRATION_NUMBER"))

df722 = df717.withColumn("10_YR",f.when(f.floor((f.months_between("10_YR_DT","REGISTRATION_DT"))/12) >= 9,"1").otherwise("0")).where('10_YR = 1')

df723 = df721.alias("df721").join(df722.alias("df722"),df721.SERIAL_NUMBER == df722.SERIAL_NUMBER,"full_outer").select(f.expr("nvl(df721.SERIAL_NUMBER,df722.SERIAL_NUMBER) as SERIAL_NUMBER"),"6_YR_DT","10_YR_DT" )

df724 = df723.selectExpr("SERIAL_NUMBER","cast(6_YR_DT as timestamp) 6_YR_DT_MADRID","cast(10_YR_DT as timestamp) LAST_Renewal_DT_MADRID").distinct()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Put everything together and output Data

# COMMAND ----------

df695 = df707.alias("df707").join(df724.alias("df724"),df707.SERIAL_NUMBER == df724.SERIAL_NUMBER,"left").select("df707.*", col("df724.6_YR_DT_MADRID"),col("df724.LAST_Renewal_DT_MADRID"))

##calculate next renewal and number of renewals
df692 = df695.alias('df695').withColumn("LAST_Renewal_DT",f.when(f.col('df695.LAST_Renewal_DT_MADRID').isNotNull(),(f.col('LAST_Renewal_DT_MADRID'))).otherwise(f.col('df695.LAST_Renewal_DT')))

df684 = df692.withColumn("Next_10Yr_Renewal",f.when(f.col('LAST_Renewal_DT').isNotNull() & f.col('REGISTRATION_DT').isNotNull() ,
                                                    f.when(((f.year("LAST_Renewal_DT")- f.year("REGISTRATION_DT")) % 10) == 0,
                                                           f.add_months(f.concat((f.year("LAST_Renewal_DT")).cast("string"),f.lit("-"),f.format_string("%02d",(f.month("REGISTRATION_DT"))),f.lit("-"),
                                                                    f.when(((f.date_format("REGISTRATION_DT","d") == 29)  & (f.month("REGISTRATION_DT") == 2)),28)
                                                                    .otherwise(f.lpad((f.date_format("REGISTRATION_DT","d").cast("string")),2,"0"))).cast(DateType()),120)
                                                           )
                                                    .when(((f.year("LAST_Renewal_DT")- f.year("REGISTRATION_DT")) % 10) == 9,
                                                           f.add_months(f.concat((f.year("LAST_Renewal_DT")).cast("string"),f.lit("-"),f.format_string("%02d",(f.month("REGISTRATION_DT"))),f.lit("-"),
                                                                    f.when(((f.date_format("REGISTRATION_DT","d") == 29)  & (f.month("REGISTRATION_DT") == 2)),28)
                                                                    .otherwise(f.lpad((f.date_format("REGISTRATION_DT","d").cast("string")),2,"0"))).cast(DateType()),132)
                                                           )
                                                    .otherwise(f.add_months(f.concat((f.year("LAST_Renewal_DT")).cast("string"),f.lit("-"),f.format_string("%02d",(f.month("REGISTRATION_DT"))),f.lit("-"),
                                                                    f.when(((f.date_format("REGISTRATION_DT","d") == 29)  & (f.month("REGISTRATION_DT") == 2)),28)
                                                                    .otherwise(f.lpad((f.date_format("REGISTRATION_DT","d").cast("string")),2,"0"))).cast(DateType()),108))
                                                    ).when(f.col('LAST_Renewal_DT').isNull() & f.col('REGISTRATION_DT').isNotNull() , f.add_months("REGISTRATION_DT",10*12))
                         )

df685 = df684.withColumn("Number_Renewals",f.when(f.col('LAST_Renewal_DT').isNull(), 0)
                         .when(f.col('REGISTRATION_DT') > f.to_date(f.lit("1989-11-16")),f.floor((f.year("LAST_Renewal_DT")- f.year("REGISTRATION_DT")+1)/10))
                         .when( ((((1989 - f.year("REGISTRATION_DT"))%20) == 0) & (f.month("REGISTRATION_DT") > 11) ) | ((((1989 - f.year("REGISTRATION_DT"))%20) == 0) & (f.month("REGISTRATION_DT") == 11) & (f.date_format("REGISTRATION_DT","d") > 16) ),f.floor(((1989 - f.year("REGISTRATION_DT"))/20) + f.floor((f.year("LAST_Renewal_DT")- 1989 + 1)/10)))
                         .when(((((1989 - f.year("REGISTRATION_DT"))%20) == 0) & (f.year("REGISTRATION_DT") != 1989) & (f.month("REGISTRATION_DT") < 11) ) | ((((1989 - f.year("REGISTRATION_DT"))%20) == 0) & (f.year("REGISTRATION_DT") != 1989) & (f.month("REGISTRATION_DT") == 11) & (f.date_format("REGISTRATION_DT","d") <= 16) ) ,
                               f.when(f.year("LAST_Renewal_DT")<1987,f.floor((f.year("LAST_Renewal_DT")- f.year("REGISTRATION_DT")+1)/20))
                               .otherwise(f.floor((1989 - f.year("REGISTRATION_DT"))/20)+1+f.when((f.year("LAST_Renewal_DT") - 2009 + 1) / 10 < 0, f.ceil((f.year("LAST_Renewal_DT") - 2009 + 1) / 10)).otherwise(f.floor((f.year("LAST_Renewal_DT") - 2009 + 1) / 10))
                                          )
                               )
                         .when(f.year("REGISTRATION_DT") == 1989,  f.floor(1 + ((f.year("LAST_Renewal_DT") - 2009 + 1) / 10)))
                         .when((((f.substring(f.substring((f.year("REGISTRATION_DT")).cast("string"), -2,2), 1,1)).cast("integer")% 2) == 0 ) & ((f.year("LAST_Renewal_DT")).cast("integer") > 1989)   ,f.floor(f.floor((1989 - f.year("REGISTRATION_DT")) / 20) + 1 + (f.when(f.floor(((f.year("LAST_Renewal_DT") - (1989 + (10 - (f.substring((1989 - f.year("REGISTRATION_DT")), -1,1).cast("string")).cast("integer")))) + 1) / 10) < 0,f.ceil(((f.year("LAST_Renewal_DT") - (1989 + (10 - (f.substring((1989 - f.year("REGISTRATION_DT")).cast("string"), -1,1)).cast("integer"))) + 1) / 10))).otherwise(f.floor(((f.year("LAST_Renewal_DT") - (1989 + (10 - (f.substring((1989 - f.year("REGISTRATION_DT")).cast("string"), -1,1)).cast("integer"))) + 1) / 10)) ))-1)
                               )
                         .when((f.year("LAST_Renewal_DT")).cast("integer") > 1989 ,f.floor(f.floor((1989 - f.year("REGISTRATION_DT")) / 20) + 1 + f.floor(((f.year("LAST_Renewal_DT") - (1989 + (10 - (f.substring((1989 - f.year("REGISTRATION_DT")).cast("string"), -1,1)).cast("integer"))) + 1) / 10)))
                               )
                         .otherwise(f.floor((((f.months_between("LAST_Renewal_DT","REGISTRATION_DT"))/12).cast("integer") + 1) / 20))
                         )

# COMMAND ----------

##Add 6 year dates
df688 = df685.alias("df685").join(df711.alias("df711"),(col("df685.SERIAL_NUMBER") == col("df711.SERIAL_NUMBER")) ,"leftouter").drop(col("df711.SERIAL_NUMBER")).drop("df711.SERIAL_NUMBER")

df693 = df688.alias('df688').withColumn("6_YR_DT",f.when(f.col('df688.6_YR_DT_MADRID').isNotNull(),(f.col('6_YR_DT_MADRID'))).otherwise(f.col('6_YR_DT')))

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df693")
df693 = df693.checkpoint(True)

##Expiration date and expiration type
df689 = df693.withColumn("Next_6YR_DT",f.when((f.col('LAST_Renewal_DT').isNull()) &(f.col('6_YR_DT').isNull()), f.add_months(f.col('REGISTRATION_DT'),72)).otherwise(f.lit(None)))\
.withColumn("Expiration_DT",f.when(f.add_months("Next_6YR_DT",6) < f.current_date(),f.add_months(f.col('REGISTRATION_DT'),72))
            .when(f.floor(f.months_between(f.current_date(),f.col("Next_10Yr_Renewal"))) >6,f.col("Next_10Yr_Renewal"))
            .when((f.col('6_YR_DT').isNotNull()) & (f.col('LAST_Renewal_DT').isNull()) & (f.floor(f.months_between(f.current_date(),f.col("REGISTRATION_DT"))) >126),f.add_months(f.col("REGISTRATION_DT"),120) )
            .when(f.col('AM_DT_CNCL').isNotNull(), f.col("AM_DT_CNCL"))
            .otherwise(f.lit(None)))\
.withColumn("Expiration_DT_RealTime",f.when(f.col("Next_6YR_DT") < f.current_date(),f.add_months(f.col('REGISTRATION_DT'),72))
            .when(f.col("Next_10Yr_Renewal") <f.current_date(),f.col("Next_10Yr_Renewal"))
            .when((f.col('6_YR_DT').isNotNull()) & (f.col('LAST_Renewal_DT').isNull()) & (f.floor(f.months_between(f.current_date(),f.col("REGISTRATION_DT"))) >119),f.add_months(f.col("REGISTRATION_DT"),120) )
            .when(f.col('AM_DT_CNCL').isNotNull(), f.col("AM_DT_CNCL"))
            .otherwise(f.lit(None)))\
.withColumn("Expiration_Type_RealTime",f.when(((f.col("Next_6YR_DT") <f.current_date())), "6 YEAR")
            .when((f.col('AM_DT_CNCL').isNotNull()) & (f.floor((f.months_between(f.col("AM_DT_CNCL"),f.col("REGISTRATION_DT")))/12) <=7), "6 YEAR")
            .when(f.current_date()>f.col("Next_10Yr_Renewal"), "10 YEAR")
            .when((f.col('6_YR_DT').isNotNull())&(f.col('LAST_Renewal_DT').isNull()) & (f.floor(f.months_between(f.current_date(),f.col("REGISTRATION_DT"))) >119), "10 YEAR")
            .when((f.col('AM_DT_CNCL').isNotNull()) & (f.floor((f.months_between(f.col("AM_DT_CNCL"),f.col("REGISTRATION_DT")))/12) >=8), "10 YEAR")
            .when(f.col('AM_DT_CNCL').isNotNull(),"Other")
            .otherwise(f.lit(None)))\
.withColumn("Expiration_DT_RealTime",f.when((f.col("Expiration_DT_RealTime")<=f.to_date(f.lit("1989-11-16"))) & (f.col("Expiration_DT_RealTime")=="10 YR"),f.add_months(f.col("Expiration_DT_RealTime"),120)).otherwise(f.col("Expiration_DT_RealTime")))\
.withColumn("Expiration_TYPE",f.when(((f.add_months("Next_6YR_DT",6) < f.current_date())),"6 YEAR")
            .when((f.col('AM_DT_CNCL').isNotNull()) & (f.floor((f.months_between(f.col("AM_DT_CNCL"),f.col("REGISTRATION_DT")))/12) <=7), "6 YEAR")
            .when(f.floor(f.months_between(f.current_date(),f.col("Next_10Yr_Renewal")) )>6,"10 YEAR")
            .when((f.col('6_YR_DT').isNotNull())&(f.col('LAST_Renewal_DT').isNull()) & (f.floor(f.months_between(f.current_date(),f.col("REGISTRATION_DT"))) >126), "10 YEAR")
            .when((f.col('AM_DT_CNCL').isNotNull()) & (f.floor((f.months_between(f.col("AM_DT_CNCL"),f.col("REGISTRATION_DT")))/12) >=8), "10 YEAR")
            .when(f.col('AM_DT_CNCL').isNotNull(),"Other")
            .otherwise(f.lit(None)))\
.withColumn("Expiration_DT",f.when((f.col("Expiration_DT")<=f.to_date(f.lit("1989-11-16"))) & (f.col("Expiration_TYPE")=="10 YR"),f.add_months(f.col("Expiration_DT"),120)).otherwise(f.col("Expiration_DT")))\
.withColumn("Next_6YR_DT",f.when((f.floor(f.months_between(f.current_date(),f.col("Next_6YR_DT"))) >6)|(f.col('Expiration_DT').isNotNull()),f.lit(None) ).otherwise(f.col("Next_6YR_DT")))\
.withColumn("Next_10Yr_Renewal",f.when((f.floor(f.months_between(f.current_date(),f.col("Next_10Yr_Renewal"))) >6)|(f.col('Expiration_DT').isNotNull()),f.lit(None) ).otherwise(f.col("Next_10Yr_Renewal")))\
.withColumn("Live_Registration",f.when(f.col("Expiration_DT").isNull(),1).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Overwrite Table1: post_reg_milestone

# COMMAND ----------

df697 = df689.alias("df689").join(df366.alias("df366"),df689.SERIAL_NUMBER == df366.SER_NUM,"left").select("df689.*", col("df366.Active_Classes"))

df696 = df697.selectExpr("SERIAL_NUMBER","REGISTRATION_DT","6_YR_DT as Six_YR_DT","LAST_Renewal_DT as LAST_10YR_DT","Next_10Yr_Renewal","Number_Renewals","Next_6YR_DT","Expiration_DT","Expiration_TYPE","REGISTRATION_NUMBER","AM_DT_CNCL","Active_Classes","Live_Registration","Expiration_DT_RealTime","Expiration_Type_RealTime").distinct()

df696 = df696.withColumn("create_ts", current_timestamp())\
                .withColumn("create_user_id", f.lit("-1"))\
                .withColumn("update_ts", current_timestamp())\
                .withColumn("update_user_id", f.lit("-1"))
df696.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.silver.post_reg_milestone')

# COMMAND ----------

# MAGIC %md
# MAGIC ##6 YEAR Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###START and END CODE Pairing Process (90% of the Data Flows here for this section)

# COMMAND ----------

df497 = df356.where('5Characters IN ("8.AFI","ES8RI","815FI","E815I","8.OKO","8.PRO","C15AO","C15PO","NA85O","NA85E","PRA8O","PR23O", "PRANO")')

df475 = df497.withColumn("Start_End",f.when(f.col('5Characters').isin("8.AFI","ES8RI","815FI","E815I"),"Start").otherwise("End"))\
            .withColumn("PostReg_Category",f.lit("6 YEAR"))\
            .withColumn("Registration_Year",f.year("REGISTRATION_DT"))\
            .withColumn("Action_Year",f.year("PH_ACTION_DATE"))\
            .withColumn("15_FLAG",f.when(f.col('5Characters').isin("15AKO","C15AO","C15PO","C75AO","C75PO","NA75E","NA75O","NA85E","NA85O","PR15O","PR23O","PR75O","15AFI","715FI","815FI","E15RI","E815I","ES75I"),1).otherwise(f.lit(None)))

df477 = df475.where('5Characters in ("8.AFI", "ES8RI","815FI", "E815I")')

df477_f = df475.where('5Characters not in ("8.AFI", "ES8RI","815FI", "E815I")')

df476 = df477.alias("df477").join(df477_f.alias("df477_f"),df477.SERIAL_NUMBER == df477_f.SERIAL_NUMBER,"inner").select(col("df477.SERIAL_NUMBER"),col("df477.PH_ACTION_NUMBER").alias("START_ACTION_NUMBER"),col("df477.PH_ACTION_CODE").alias("START_ACTION_CODE"),col("df477.PH_ACTION_DATE").alias("START_ACTION_DATE"),col("df477.CM_Desc").alias("START_CM_DESC"),col("df477.5Characters").alias("START_5_CHARACTERS"),col("df477.REGISTRATION_DT"),col("df477.FILING_BASIS_CUR"),col("df477.FILING_BASIS_FIL"),col("df477.REGISTRATION_NUMBER"),col("df477.Start_End"),col("df477.PostReg_Category"),col("df477.Registration_Year"),col("df477.Action_Year"),col("df477.15_FLAG"),col("df477_f.PH_ACTION_NUMBER").alias("END_ACTION_NUMBER"),col("df477_f.PH_ACTION_CODE").alias("END_ACTION_CODE"),col("df477_f.PH_ACTION_DATE").alias("END_ACTION_DATE"),col("df477_f.CM_Desc").alias("END_CM_DESC"),col("df477_f.5Characters").alias("END_5_CHARACTERS"),col("df477_f.FILING_BASIS_CUR").alias("Right_FILING_BASIS_CUR"),col("df477_f.FILING_BASIS_FIL").alias("Right_FILING_BASIS_FIL"),col("df477_f.REGISTRATION_NUMBER").alias("Right_REGISTRATION_NUMBER"),col("df477_f.Start_End").alias("Right_Start_End"),col("df477_f.PostReg_Category").alias("Right_PostReg_Category"),col("df477_f.Registration_Year").alias("Right_Registration_Year"),col("df477_f.Action_Year").alias("Right_Action_Year"),col("df477_f.15_FLAG").alias("Right_15_FLAG"))

df494 = df476.withColumn("FLAG_POST_REG",f.when(f.col('START_5_CHARACTERS').isin("8.AFI") & f.col('END_5_CHARACTERS').isin("8.OKO", "8.PRO", "C15AO", "C15PO", "NA85O", "NA85E"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("ES8RI") & f.col('END_5_CHARACTERS').isin("8.OKO", "8.PRO", "C15AO", "C15PO", "NA85O", "NA85E"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("815FI") & f.col('END_5_CHARACTERS').isin("8.OKO", "8.PRO", "C15AO", "C15PO", "NA85O", "NA85E"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("E815I") & f.col('END_5_CHARACTERS').isin("8.OKO", "8.PRO", "C15AO", "C15PO", "NA85O", "NA85E"),"1")\
                                        .otherwise("0"))\
            .withColumn("START_ACTION_YR",f.year("START_ACTION_DATE"))\
            .withColumn("END_ACTION_YR",f.year("END_ACTION_DATE"))\
            .withColumn("PRA_Mailed",f.when(f.col('START_5_CHARACTERS').isin("8.AFI") & f.col('END_5_CHARACTERS').isin("PRA80", "PR23O", "PRANO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("ES8RI") & f.col('END_5_CHARACTERS').isin("PRA80", "PR23O", "PRANO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("815FI") & f.col('END_5_CHARACTERS').isin("PRA80", "PR23O", "PRANO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("E815I") & f.col('END_5_CHARACTERS').isin("PRA80", "PR23O", "PRANO"),"1")\
                                        .otherwise("0"))  

df486 = df494.selectExpr("SERIAL_NUMBER","START_5_CHARACTERS","END_5_CHARACTERS","START_ACTION_DATE","END_ACTION_DATE","START_ACTION_NUMBER","END_ACTION_NUMBER","START_CM_DESC","END_CM_DESC","FILING_BASIS_FIL","FLAG_POST_REG","START_ACTION_YR","END_ACTION_YR","START_ACTION_CODE","REGISTRATION_DT","Start_End","Registration_Year","Action_Year","END_ACTION_CODE","Right_FILING_BASIS_FIL","Right_Start_End","Right_Registration_Year","Right_Action_Year","FILING_BASIS_CUR","Right_FILING_BASIS_CUR","PRA_Mailed","REGISTRATION_NUMBER","PostReg_Category","Right_REGISTRATION_NUMBER","Right_PostReg_Category","15_FLAG","Right_15_FLAG")

df487 = df486.where('FLAG_POST_REG = 1')

##Keep one clean data line per event; Remove all redundant data noise 
df489 = df487.where((f.col("END_ACTION_NUMBER") > f.col("START_ACTION_NUMBER")) | (f.col("END_ACTION_DATE") >= f.col("START_ACTION_DATE")))

w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_5_CHARACTERS"),col("START_ACTION_NUMBER"),col("END_ACTION_NUMBER"))
df492 = df489.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df735 = df492

df737 = df735.dropDuplicates(["SERIAL_NUMBER","END_5_CHARACTERS","END_ACTION_DATE"])

w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy("SERIAL_NUMBER","START_5_CHARACTERS","START_ACTION_NUMBER","END_ACTION_NUMBER")
df516 = df737.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df493 = df516.selectExpr("SERIAL_NUMBER","START_5_CHARACTERS","END_5_CHARACTERS","START_ACTION_DATE","END_ACTION_DATE","START_ACTION_NUMBER","END_ACTION_NUMBER","START_CM_DESC","END_CM_DESC","FILING_BASIS_FIL","FLAG_POST_REG","START_ACTION_YR","END_ACTION_YR","START_ACTION_CODE","REGISTRATION_DT","Start_End","Registration_Year","Action_Year","END_ACTION_CODE","FILING_BASIS_CUR","PRA_Mailed","REGISTRATION_NUMBER","PostReg_Category","Right_REGISTRATION_NUMBER","Right_PostReg_Category","15_FLAG")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan Start Code Transaction lines (~2% Data Flows here)

# COMMAND ----------

# DBTITLE 1,##Incorrect results in alteryx. Added sort in DEV wf to match counts and data
df508 = df477.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as START_ACTION_NUMBER","PH_ACTION_CODE as START_ACTION_CODE","PH_ACTION_DATE as START_ACTION_DATE","CM_Desc as START_CM_DESC","5Characters as START_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG")

w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_ACTION_DATE"),col("START_ACTION_NUMBER"))
df507 = df508.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w = Window.partitionBy("SERIAL_NUMBER").orderBy("SERIAL_NUMBER","START_ACTION_DATE","START_ACTION_NUMBER")
df505 = df507.withColumn("DUPLICATE",f.when((f.col('SERIAL_NUMBER') == f.lead(f.col('SERIAL_NUMBER'),1).over(w)) & (f.floor(f.months_between(f.lead(f.col('START_ACTION_DATE'),1).over(w),f.col('START_ACTION_DATE'))/12 )< 2) ,1).otherwise("0"))

##Incorrect results in alteryx. Added sort in DEV wf to match counts and data
df506 = df505.where('DUPLICATE = 0')

df502 = df506.alias("df506").join(df493.alias("df493"),((col("df506.SERIAL_NUMBER") == col("df493.SERIAL_NUMBER")) & (col("df506.START_ACTION_NUMBER") == col("df493.START_ACTION_NUMBER"))) ,"anti")

df504_anti = df502.alias("df502").join(df363.alias("df363"),(col("df502.SERIAL_NUMBER") == col("df363.AM_SER_NUM"))  ,"anti")

df504_inner = df502.alias("df502").join(df363.alias("df363"),(col("df502.SERIAL_NUMBER") == col("df363.AM_SER_NUM"))  ,"inner")

df510 = df504_inner.withColumn("INVENTORY",f.lit(1))
df503 = df510.selectExpr("SERIAL_NUMBER","REGISTRATION_NUMBER","REGISTRATION_DT","START_ACTION_NUMBER","START_ACTION_CODE","START_ACTION_DATE","START_CM_DESC","START_5_CHARACTERS","FILING_BASIS_CUR","FILING_BASIS_FIL","Start_End","Registration_Year","Action_Year","15_FLAG","DUPLICATE","INVENTORY","PostReg_Category")

df512 = df504_anti.alias("df504_anti").join(df363_f.alias("df363_f"),(col("df504_anti.SERIAL_NUMBER") == col("df363_f.AM_SER_NUM"))  ,"inner").select(f.col("df504_anti.*"),f.col("df363_f.AM_DT_CNCL").alias("END_ACTION_DATE"))

df728 = df512.withColumn("INACTIVE",f.lit(1)).withColumn("END_5_CHARACTERS",f.lit("CANCELLED")).withColumn("END_CM_DESC",f.lit("CANCELLED"))

df511 = df728.selectExpr("SERIAL_NUMBER","REGISTRATION_NUMBER","REGISTRATION_DT","START_ACTION_NUMBER","START_ACTION_CODE","START_ACTION_DATE","START_CM_DESC","START_5_CHARACTERS","FILING_BASIS_CUR","FILING_BASIS_FIL","Start_End","Registration_Year","Action_Year","15_FLAG","'' as START_DATE_TEMP","DUPLICATE","INACTIVE","END_ACTION_DATE","END_5_CHARACTERS","END_CM_DESC","PostReg_Category")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan End Code Transaction lines (~8% Data Flows here)

# COMMAND ----------

##Rename Variables to match with output data
df484 = df477_f.where('5Characters in ("8.OKO", "8.PRO", "C15AO", "C15PO", "NA85O", "NA85E")')

df478 = df484.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as END_ACTION_NUMBER","PH_ACTION_CODE as END_ACTION_CODE","PH_ACTION_DATE as END_ACTION_DATE","CM_Desc as END_CM_DESC","5Characters as END_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","PostReg_Category","Registration_Year","Action_Year","15_FLAG","'' as Category1")

##Keep one line per event and remove redundant data lines 
df734 = df478.orderBy("SERIAL_NUMBER",f.desc("END_ACTION_DATE"),f.desc("END_ACTION_NUMBER")).dropDuplicates(["SERIAL_NUMBER","END_ACTION_DATE"])

# COMMAND ----------

df515 = df734.alias("df734").join(df493.alias("df493"),((col("df734.SERIAL_NUMBER") == col("df493.SERIAL_NUMBER")) & (col("df734.END_ACTION_DATE") == col("df493.END_ACTION_DATE"))) ,"anti")

df498 = df515.unionByName(df493,allowMissingColumns=True).unionByName(df503,allowMissingColumns=True).unionByName(df511,allowMissingColumns=True)

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df498")
df498 = df498.checkpoint(True)

##Since 6-YR filing occurs only once in each serial number life cycle, keep the first instance and ignore rest of the data noise 
df499 = df498.groupBy("SERIAL_NUMBER").agg(f.min("END_ACTION_NUMBER").alias("Min_END_ACTION_NUMBER"))

df500 = df499.alias("df499").join(df498.alias("df498"),((col("df499.SERIAL_NUMBER") == col("df498.SERIAL_NUMBER")) & (df499["Min_END_ACTION_NUMBER"].eqNullSafe(df498["END_ACTION_NUMBER"]))) ,"inner").select(f.col("df499.SERIAL_NUMBER"),f.col("df499.Min_END_ACTION_NUMBER"),f.col("df498.SERIAL_NUMBER").alias("Right_SERIAL_NUMBER"),f.col("df498.REGISTRATION_NUMBER"),f.col("df498.FILING_BASIS_CUR"),f.col("df498.END_5_CHARACTERS"),f.col("df498.END_ACTION_DATE"),f.col("df498.END_ACTION_CODE"),f.col("df498.END_CM_DESC"),f.col("df498.FILING_BASIS_FIL"),f.col("df498.REGISTRATION_DT"),f.col("df498.END_ACTION_NUMBER"),f.col("df498.Start_End"),f.col("df498.PostReg_Category"),f.col("df498.Registration_Year"),f.col("df498.Action_Year"),f.col("df498.15_FLAG"),f.col("df498.START_5_CHARACTERS"),f.col("df498.START_ACTION_DATE"),f.col("df498.START_ACTION_NUMBER"),f.col("df498.START_CM_DESC"),f.col("df498.FLAG_POST_REG"),f.col("df498.START_ACTION_YR"),f.col("df498.END_ACTION_YR"),f.col("df498.START_ACTION_CODE"),f.col("df498.PRA_Mailed"),f.col("df498.Right_REGISTRATION_NUMBER"),f.col("df498.Right_PostReg_Category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get First Action

# COMMAND ----------

##Keep Valid START_END Pairs
df518 = df486.where('END_ACTION_NUMBER > START_ACTION_NUMBER')

##Keep one line per event and remove redundant data lines 
w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_ACTION_NUMBER"),col("END_ACTION_NUMBER"))
df519 = df518.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

##Stack Orphan End code lines with the Final Output Data of this Section
df523 = df519.selectExpr("SERIAL_NUMBER","END_5_CHARACTERS as FIRST_ACTION_CODE","START_ACTION_DATE as FA_START_DATE","END_ACTION_DATE as FIRST_ACTION_DATE","START_ACTION_NUMBER as FA_START_NUMBER","END_ACTION_NUMBER as FIRST_ACTION_NUMBER","END_CM_DESC as FIRST_ACTION_DESC","PostReg_Category","Right_PostReg_Category")

df526 = df500.alias("df500").join(df523.alias("df523"),((col("df500.SERIAL_NUMBER") == col("df523.SERIAL_NUMBER")) & (col("df500.START_ACTION_DATE") == col("df523.FA_START_DATE"))) ,"leftouter").selectExpr("df500.Action_Year","df500.END_5_CHARACTERS","df500.END_ACTION_CODE","df500.END_ACTION_DATE","df500.END_ACTION_NUMBER","df500.END_ACTION_YR","df500.END_CM_DESC","df500.FLAG_POST_REG","df500.Min_END_ACTION_NUMBER","df500.PostReg_Category","df500.PRA_Mailed","df500.REGISTRATION_DT","df500.REGISTRATION_NUMBER","df500.Registration_Year","df500.Right_PostReg_Category	as Left_Right_PostReg_Category","df500.Right_REGISTRATION_NUMBER","df500.Right_SERIAL_NUMBER","df500.SERIAL_NUMBER","df500.START_5_CHARACTERS","df500.START_ACTION_CODE","df500.START_ACTION_DATE","df500.START_ACTION_NUMBER","df500.START_ACTION_YR","df500.START_CM_DESC","df500.Start_End","df523.FA_START_DATE","df523.FA_START_NUMBER","df523.FIRST_ACTION_CODE","df523.FIRST_ACTION_DATE","df523.FIRST_ACTION_DESC","df523.FIRST_ACTION_NUMBER","df523.PostReg_Category	as Right_PostReg_Category","df523.Right_PostReg_Category as Right_Right_PostReg_Category","df500.FILING_BASIS_CUR","df500.FILING_BASIS_FIL","df500.15_FLAG")

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df526")
# df526 = df526.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##10 YEAR DATA - NON-MADRID 10 YEAR

# COMMAND ----------

df441 = df356.where('FILING_BASIS_CUR != "MADRID"').where(f.col("5Characters").isin("89AFI", "8AFTI", "E89RI","8AFTI","9.AFI","E89RI","PRANO","PR89O","89AGO","PRA8O","8OKTO","8PRTO","PRA9O","9G8PO")|f.col("5Characters").contains("RNL")|f.col("5Characters").contains("REN") ) 

df439 = df441.withColumn("Start_End",f.when(f.col("5Characters").contains("RNL")|f.col("5Characters").contains("REN"),"End").otherwise("Start"))\
            .withColumn("POSTREG_CATEGORY",f.lit("10 YEAR"))\
            .withColumn("Registration_Year",f.year("REGISTRATION_DT"))\
            .withColumn("Action_Year",f.year("PH_ACTION_DATE"))\
            .withColumn("15_FLAG",f.when(f.col('5Characters').isin("15AKO","C15AO","C15PO","C75AO","C75PO","NA75E","NA75O","NA85E","NA85O","PR15O","PR23O","PR75O","15AFI","715FI","815FI","E15RI","E815I","ES75I"),1))

df437 = df439.where('5Characters in ("89AFI","8AFTI","E89RI","8AFTI","9.AFI","E89RI")')

df437_f = df439.where('5Characters not in ("89AFI","8AFTI","E89RI","8AFTI","9.AFI","E89RI")')

df438 = (
    df437.alias("df437")
    .join(
        df437_f.alias("df437_f"),
        (col("df437.SERIAL_NUMBER") == col("df437_f.SERIAL_NUMBER")),
        "inner",
    )
    .select(
        col("df437.5Characters").alias("START_5_CHARACTERS"),
        col("df437.CM_Desc").alias("START_CM_DESC"),
        col("df437.FILING_BASIS_CUR"),
        col("df437.FILING_BASIS_FIL"),
        col("df437.PH_ACTION_CODE").alias("START_ACTION_CODE"),
        col("df437.PH_ACTION_DATE").alias("START_ACTION_DATE"),
        col("df437.PH_ACTION_NUMBER").alias("START_ACTION_NUMBER"),
        col("df437.REGISTRATION_DT"),
        col("df437.REGISTRATION_NUMBER"),
        col("df437.SERIAL_NUMBER"),
        col("df437.Start_End"),
        col("df437.POSTREG_CATEGORY"),
        col("df437.Registration_Year"),
        col("df437.Action_Year"),
        col("df437.15_FLAG"),
        col("df437_f.5Characters").alias("END_5_CHARACTERS"),
        col("df437_f.CM_Desc").alias("END_CM_DESC"),
        col("df437_f.FILING_BASIS_CUR").alias("Right_FILING_BASIS_CUR"),
        col("df437_f.FILING_BASIS_FIL").alias("Right_FILING_BASIS_FIL"),
        col("df437_f.PH_ACTION_CODE").alias("END_ACTION_CODE"),
        col("df437_f.PH_ACTION_DATE").alias("END_ACTION_DATE"),
        col("df437_f.PH_ACTION_NUMBER").alias("END_ACTION_NUMBER"),
        col("df437_f.REGISTRATION_NUMBER").alias("Right_REGISTRATION_NUMBER"),
        col("df437_f.Start_End").alias("Right_Start_End"),
        col("df437_f.POSTREG_CATEGORY").alias("Right_POSTREG_CATEGORY"),
        col("df437_f.Registration_Year").alias("Right_Registration_Year"),
        col("df437_f.Action_Year").alias("Right_Action_Year"),
        col("df437_f.15_FLAG").alias("Right_15_FLAG")
    )
)

df436 = df438.withColumn("FLAG_POST_REG",f.when(f.col('START_5_CHARACTERS').isin("89AFI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN")),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("8AFTI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN")),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("E89RI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN")),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("8AFTI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN")),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("9.AFI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN")),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("E89RI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN")),"1")\
                                        .otherwise("0"))\
            .withColumn("START_ACTION_YR",f.year("START_ACTION_DATE"))\
            .withColumn("END_ACTION_YR",f.year("END_ACTION_DATE"))\
            .withColumn("PRA_Mailed",f.when(f.col('START_5_CHARACTERS').isin("8.AFI") & f.col('END_5_CHARACTERS').isin("PR89O","89AGO","PRA8O","8OKTO","8PRTO","PRA9O","9G8PO","PR23O","PRANO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("ES8RI") & f.col('END_5_CHARACTERS').isin("PR89O","89AGO","PRA8O","8OKTO","8PRTO","PRA9O","9G8PO","PR23O","PRANO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("815FI") & f.col('END_5_CHARACTERS').isin("PR89O","89AGO","PRA8O","8OKTO","8PRTO","PRA9O","9G8PO","PR23O","PRANO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("E815I") & f.col('END_5_CHARACTERS').isin("PR89O","89AGO","PRA8O","8OKTO","8PRTO","PRA9O","9G8PO","PR23O","PRANO"),"1")\
                                        .otherwise("0"))  
            
df435 = df436.selectExpr("SERIAL_NUMBER","START_5_CHARACTERS","END_5_CHARACTERS","START_ACTION_DATE","END_ACTION_DATE","START_ACTION_NUMBER","END_ACTION_NUMBER","START_CM_DESC","END_CM_DESC","FILING_BASIS_FIL","FLAG_POST_REG","START_ACTION_YR","END_ACTION_YR","START_ACTION_CODE","REGISTRATION_DT","Start_End","Registration_Year","Action_Year","END_ACTION_CODE","Right_FILING_BASIS_FIL","Right_Start_End","Right_Registration_Year","Right_Action_Year","FILING_BASIS_CUR","Right_FILING_BASIS_CUR","PRA_Mailed","REGISTRATION_NUMBER","POSTREG_CATEGORY","Right_REGISTRATION_NUMBER","Right_POSTREG_CATEGORY","15_FLAG","Right_15_FLAG")

df433 = df435.where('END_ACTION_NUMBER > START_ACTION_NUMBER').where('FLAG_POST_REG =1')
 
df434 = df433

df432  = df434.withColumn("FLAG_POST_REG",f.when(f.col('START_5_CHARACTERS').isin("89AFI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN") ),"1")\
        .when(f.col('START_5_CHARACTERS').isin("8AFTI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN") ),"1")\
        .when(f.col('START_5_CHARACTERS').isin("E89RI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN") ),"1")\
        .when(f.col('START_5_CHARACTERS').isin("8AFTI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN") ),"1")\
        .when(f.col('START_5_CHARACTERS').isin("9.AFI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN") ),"1")\
        .when(f.col('START_5_CHARACTERS').isin("E89RI") & (f.col("END_5_CHARACTERS").contains("RNL")|f.col("END_5_CHARACTERS").contains("REN") ),"1"))\
        .withColumn("START_ACTION_YR",f.year("START_ACTION_DATE"))\
        .withColumn("END_ACTION_YR",f.year("END_ACTION_DATE"))

# COMMAND ----------

df431 = df432.where('FLAG_POST_REG =1')

w2 = Window.partitionBy("SERIAL_NUMBER","START_5_CHARACTERS","START_ACTION_DATE").orderBy("SERIAL_NUMBER","START_ACTION_DATE","END_ACTION_DATE")
df429 = df431.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row")

w2 = Window.partitionBy("SERIAL_NUMBER","END_5_CHARACTERS","END_ACTION_DATE").orderBy("SERIAL_NUMBER",f.desc("START_ACTION_DATE"),f.desc("START_ACTION_NUMBER"),f.desc("END_ACTION_DATE"))
df428 = df429.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row")

w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy("SERIAL_NUMBER","START_ACTION_DATE","END_ACTION_DATE")
df451 = df428.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row")

df427 = df451.select("SERIAL_NUMBER","START_5_CHARACTERS","END_5_CHARACTERS","START_ACTION_DATE","END_ACTION_DATE","START_ACTION_NUMBER","END_ACTION_NUMBER","START_CM_DESC","END_CM_DESC","FILING_BASIS_FIL","FLAG_POST_REG","START_ACTION_YR","END_ACTION_YR","START_ACTION_CODE","REGISTRATION_DT","Start_End","Registration_Year","Action_Year","END_ACTION_CODE","FILING_BASIS_CUR","PRA_Mailed","REGISTRATION_NUMBER","POSTREG_CATEGORY","Right_REGISTRATION_NUMBER","Right_POSTREG_CATEGORY","15_FLAG","Right_15_FLAG")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan Start Code Transaction lines (~2% Data Flows here)

# COMMAND ----------

df416 = df437.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as START_ACTION_NUMBER","PH_ACTION_CODE as START_ACTION_CODE","PH_ACTION_DATE as START_ACTION_DATE","CM_Desc as START_CM_DESC","5Characters as START_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG")

w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_ACTION_DATE"),col("START_ACTION_NUMBER"))
df415 = df416.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w = Window.partitionBy("SERIAL_NUMBER").orderBy("SERIAL_NUMBER","START_ACTION_DATE","START_ACTION_NUMBER")
df419 = df415.withColumn("DUPLICATE",f.when((f.col('SERIAL_NUMBER') == f.lead(f.col('SERIAL_NUMBER'),1).over(w)) & (f.floor(f.months_between(f.lead(f.col('START_ACTION_DATE'),1).over(w),f.col('START_ACTION_DATE'))/12) < 2 ),1).otherwise("0"))

####Incorrect results in Alteryx. considering date time difference value as string##################
df420 = df419.where('DUPLICATE = 0')
df413 = df420.alias("df420").join(df427.alias("df427"),((col("df420.SERIAL_NUMBER") == col("df427.SERIAL_NUMBER")) & (col("df420.START_ACTION_DATE") == col("df427.START_ACTION_DATE"))) ,"anti").select(f.col('df420.*'))

df418_anti = df413.alias("df413").join(df363.alias("df363"),(col("df413.SERIAL_NUMBER") == col("df363.AM_SER_NUM")),"anti").select("SERIAL_NUMBER","REGISTRATION_NUMBER","FILING_BASIS_CUR","START_5_CHARACTERS","START_ACTION_DATE","START_ACTION_CODE","START_CM_DESC","FILING_BASIS_FIL","REGISTRATION_DT","START_ACTION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG","DUPLICATE")

df418_inner = df413.alias("df413").join(df363.alias("df363"),(col("df413.SERIAL_NUMBER") == col("df363.AM_SER_NUM")),"inner").select("SERIAL_NUMBER","REGISTRATION_NUMBER","FILING_BASIS_CUR","START_5_CHARACTERS","START_ACTION_DATE","START_ACTION_CODE","START_CM_DESC","FILING_BASIS_FIL","REGISTRATION_DT","START_ACTION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG","DUPLICATE")

df423 = df418_anti.alias("df418_anti").join(df363_f.alias("df363_f"),(col("df418_anti.SERIAL_NUMBER") == col("df363_f.AM_SER_NUM")),"inner").select("df418_anti.*",col("df363_f.AM_DT_CNCL").alias("END_ACTION_DATE"))

df422 = df423.withColumn("INACTIVE",f.lit(1)).withColumn("END_5_CHARACTERS",f.lit("CANCELLED")).withColumn("END_CM_DESC",f.lit("CANCELLED"))

df417 = df418_inner.withColumn("INVENTORY",f.lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan End Code Transaction lines (~8% Data Flows here)

# COMMAND ----------

##Rename Variables to match with output data
df443 = df437_f.where(f.col("5Characters").contains("RNL")|f.col("5Characters").contains("REN"))
df449 = df443.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as END_ACTION_NUMBER","PH_ACTION_CODE as END_ACTION_CODE","PH_ACTION_DATE as END_ACTION_DATE","CM_Desc as END_CM_DESC","5Characters as END_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year")

w2 = Window.partitionBy("SERIAL_NUMBER","END_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),f.desc("END_ACTION_DATE"),f.desc("END_ACTION_NUMBER"))
df447 = df449.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df426 = df447.alias("df447").join(df427.alias("df427"),((col("df447.SERIAL_NUMBER") == col("df427.SERIAL_NUMBER")) & (col("df447.END_ACTION_DATE") == col("df427.END_ACTION_DATE"))) ,"anti")

df440 = df426.unionByName(df427,allowMissingColumns=True).unionByName(df417,allowMissingColumns=True).unionByName(df422,allowMissingColumns=True)

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df440")
df440 = df440.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get First Action

# COMMAND ----------

df452 = df435.where('END_ACTION_NUMBER > START_ACTION_NUMBER')

##Keep one line per event and remove redundant data lines 
w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_ACTION_NUMBER"),col("END_ACTION_NUMBER"))
df454 = df452.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df458 = df454.selectExpr("SERIAL_NUMBER","END_5_CHARACTERS as FIRST_ACTION_CODE","START_ACTION_DATE as FA_START_DATE","END_ACTION_DATE as FIRST_ACTION_DATE","START_ACTION_NUMBER as FA_START_NUMBER","END_ACTION_NUMBER as FIRST_ACTION_NUMBER","END_CM_DESC as FIRST_ACTION_DESC","Right_15_FLAG")

##Stack Orphan End code lines with the Final Output Data of this Section
df460_join = df440.alias("df440").join(df458.alias("df458"),(col("df440.SERIAL_NUMBER") == col("df458.SERIAL_NUMBER"))& (col("df440.START_ACTION_DATE") == col("df458.FA_START_DATE"))  ,"inner").selectExpr("END_5_CHARACTERS","END_CM_DESC","END_ACTION_CODE","END_ACTION_DATE","END_ACTION_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","df440.SERIAL_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","START_5_CHARACTERS","START_ACTION_DATE","START_ACTION_NUMBER","START_CM_DESC","FLAG_POST_REG","START_ACTION_YR","END_ACTION_YR","START_ACTION_CODE","PRA_Mailed","Right_REGISTRATION_NUMBER","Right_POSTREG_CATEGORY","df458.Right_15_FLAG as Right_Right_15_FLAG","df440.Right_15_FLAG","DUPLICATE","INACTIVE","FIRST_ACTION_NUMBER","FIRST_ACTION_DESC","FIRST_ACTION_DATE","FIRST_ACTION_CODE","FA_START_NUMBER","FA_START_DATE")

df460_anti = df440.alias("df440").join(df458.alias("df458"),(col("df440.SERIAL_NUMBER") == col("df458.SERIAL_NUMBER"))& (col("df440.START_ACTION_DATE") == col("df458.FA_START_DATE"))  ,"anti")

df460 = df460_join.unionByName(df460_anti,allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##10 YEAR DATA - MADRID - 6 YR and 10 YR

# COMMAND ----------

df377 = df356.where('FILING_BASIS_CUR = "MADRID"')

df376 = df377.where(f.col("5Characters").contains("ES71I")|f.col("5Characters").contains("71AFI")|f.col("5Characters").contains("ES75I")|f.col("5Characters").contains("715FI")|f.col("5Characters").contains("NA71")|f.col("5Characters").contains("NA75")|f.col("5Characters").contains("71AG") )

df369 = df376.withColumn("Start_End",f.when(f.col('5Characters').isin("ES71I", "71AFI","ES75I","715FI"),"Start").otherwise("End"))\
            .withColumn("POSTREG_CATEGORY",f.when(f.floor((f.months_between("PH_ACTION_DATE","REGISTRATION_DT"))/12) < 9,"6 YEAR").otherwise("10 YEAR"))\
            .withColumn("Registration_Year",f.year("REGISTRATION_DT"))\
            .withColumn("Action_Year",f.year("PH_ACTION_DATE"))\
            .withColumn("15_FLAG",f.when(f.col('5Characters').isin("15AKO","C15AO","C15PO","C75AO","C75PO","NA75E","NA75O","NA85E","NA85O","PR15O","PR23O","PR75O","15AFI","715FI","815FI","E15RI","E815I","ES75I"),1))

df371 = df369.where('Start_End = "Start"')
df371_f = df369.where('Start_End != "Start"')

df370 = (
    df371.alias("df371")
    .join(
        df371_f.alias("df371_f"),
        (col("df371.SERIAL_NUMBER") == col("df371_f.SERIAL_NUMBER")),
        "inner",
    )
    .select(
        col("df371.5Characters").alias("START_5_CHARACTERS"),
        col("df371.CM_Desc").alias("START_CM_DESC"),
        col("df371.FILING_BASIS_CUR"),
        col("df371.FILING_BASIS_FIL"),
        col("df371.PH_ACTION_CODE").alias("START_ACTION_CODE"),
        col("df371.PH_ACTION_DATE").alias("START_ACTION_DATE"),
        col("df371.PH_ACTION_NUMBER").alias("START_ACTION_NUMBER"),
        col("df371.REGISTRATION_DT"),
        col("df371.REGISTRATION_NUMBER"),
        col("df371.SERIAL_NUMBER"),
        col("df371.Start_End"),
        col("df371.POSTREG_CATEGORY"),
        col("df371.Registration_Year"),
        col("df371.Action_Year"),
        col("df371.15_FLAG"),
        col("df371_f.5Characters").alias("END_5_CHARACTERS"),
        col("df371_f.CM_Desc").alias("END_CM_DESC"),
        col("df371_f.FILING_BASIS_CUR").alias("Right_FILING_BASIS_CUR"),
        col("df371_f.FILING_BASIS_FIL").alias("Right_FILING_BASIS_FIL"),
        col("df371_f.PH_ACTION_CODE").alias("END_ACTION_CODE"),
        col("df371_f.PH_ACTION_DATE").alias("END_ACTION_DATE"),
        col("df371_f.PH_ACTION_NUMBER").alias("END_ACTION_NUMBER"),
        col("df371_f.REGISTRATION_NUMBER").alias("Right_REGISTRATION_NUMBER"),
        col("df371_f.Start_End").alias("Right_Start_End"),
        col("df371_f.POSTREG_CATEGORY").alias("Right_POSTREG_CATEGORY"),
        col("df371_f.Registration_Year").alias("Right_Registration_Year"),
        col("df371_f.Action_Year").alias("Right_Action_Year"),
        col("df371_f.15_FLAG").alias("Right_15_FLAG")
    )
)

df372 = df370.withColumn("FLAG_POST_REG",f.when(f.col('START_5_CHARACTERS').isin("ES71I") & f.col('END_5_CHARACTERS').isin("71AGO", "NA71E","NA71O","NA75E","NA75O"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("71AFI") & f.col('END_5_CHARACTERS').isin("71AGO", "NA71E","NA71O","NA75E","NA75O"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("ES75I") & f.col('END_5_CHARACTERS').isin("71AGO", "NA71E","NA71O","NA75E","NA75O"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("715FI") & f.col('END_5_CHARACTERS').isin("71AGO", "NA71E","NA71O","NA75E","NA75O"),"1")\
                                        .otherwise("0"))\
            .withColumn("START_ACTION_YR",f.year("START_ACTION_DATE"))\
            .withColumn("END_ACTION_YR",f.year("END_ACTION_DATE"))

df373 = df372.selectExpr("SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_5_CHARACTERS","END_5_CHARACTERS","START_ACTION_DATE","END_ACTION_DATE","START_ACTION_NUMBER","END_ACTION_NUMBER","START_CM_DESC","END_CM_DESC","FILING_BASIS_FIL","FLAG_POST_REG","START_ACTION_YR","END_ACTION_YR","START_ACTION_CODE","FILING_BASIS_CUR","Right_15_FLAG","15_FLAG")

df375 = df373.where('END_ACTION_NUMBER > START_ACTION_NUMBER').where('FLAG_POST_REG =1')

w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE","START_5_CHARACTERS").orderBy(col("SERIAL_NUMBER"),col("START_5_CHARACTERS"),col("START_ACTION_NUMBER"),col("END_ACTION_NUMBER"))
df380 = df375.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w2 = Window.partitionBy("SERIAL_NUMBER","END_ACTION_DATE","END_5_CHARACTERS").orderBy(col("SERIAL_NUMBER"),f.desc(col("START_5_CHARACTERS")),f.desc(col("START_ACTION_NUMBER")),f.desc(col("END_ACTION_NUMBER")))
df379 = df380.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan Start Code Transaction lines (~2% Data Flows here)

# COMMAND ----------

df396 = df371.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as START_ACTION_NUMBER","PH_ACTION_CODE as START_ACTION_CODE","PH_ACTION_DATE as START_ACTION_DATE","CM_Desc as START_CM_DESC","5Characters as START_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG")

w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_ACTION_DATE"),col("START_ACTION_NUMBER"))
df394 = df396.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w = Window.partitionBy("SERIAL_NUMBER").orderBy("SERIAL_NUMBER","START_ACTION_DATE")
df392 = df394.withColumn("DUPLICATE",f.when((f.col('SERIAL_NUMBER') == f.lead(f.col('SERIAL_NUMBER'),1).over(w)) & (f.floor(f.months_between(f.lead(f.col('START_ACTION_DATE'),1).over(w),f.col('START_ACTION_DATE'))/12) < 2) ,1).otherwise("0"))

df393 = df392.where('DUPLICATE = 0')

df389 = df393.alias("df393").join(df379.alias("df379"),((df393.SERIAL_NUMBER == df379.SERIAL_NUMBER) & (df393.START_ACTION_NUMBER == df379.START_ACTION_NUMBER)),"anti")

df391_anti = df389.alias("df389").join(df363.alias("df363"),(col("df389.SERIAL_NUMBER") == col("df363.AM_SER_NUM")) ,"anti")

df391_inner = df389.alias("df389").join(df363.alias("df363"),(col("df389.SERIAL_NUMBER") == col("df363.AM_SER_NUM")) ,"inner")

df399 = df391_anti.alias("df391_anti").join(df363_f.alias("df363_f"),(col("df391_anti.SERIAL_NUMBER") == col("df363_f.AM_SER_NUM")) ,"inner").select(f.col("df391_anti.*"),f.col("df363_f.AM_DT_CNCL").alias("END_ACTION_DATE"))

df730 = df399.withColumn("INACTIVE",f.lit(1)).withColumn("END_5_CHARACTERS",f.lit("CANCELLED")).withColumn("END_CM_DESC",f.lit("CANCELLED"))

df398 = df730.selectExpr("SERIAL_NUMBER","REGISTRATION_NUMBER","REGISTRATION_DT","START_ACTION_NUMBER","START_ACTION_CODE","START_ACTION_DATE","START_CM_DESC","START_5_CHARACTERS","FILING_BASIS_CUR","FILING_BASIS_FIL","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG","DUPLICATE","INACTIVE","END_ACTION_DATE","END_5_CHARACTERS","END_CM_DESC")

df397 = df391_inner.withColumn("INVENTORY",f.lit(1))
df390 = df397.selectExpr("SERIAL_NUMBER","REGISTRATION_NUMBER","REGISTRATION_DT","START_ACTION_NUMBER","START_ACTION_CODE","START_ACTION_DATE","START_CM_DESC","START_5_CHARACTERS","FILING_BASIS_CUR","FILING_BASIS_FIL","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG","DUPLICATE","INVENTORY")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan End Code Transaction lines (~8% Data Flows here)

# COMMAND ----------

##Rename Variables to match with output data
df387 = df371_f.where(f.col("5Characters").contains("RNL")|f.col("5Characters").contains("REN"))
df381 = df387.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as END_ACTION_NUMBER","PH_ACTION_CODE as END_ACTION_CODE","PH_ACTION_DATE as END_ACTION_DATE","CM_Desc as END_CM_DESC","5Characters as END_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year")
##Keep one line per event and remove redundant data lines 
w2 = Window.partitionBy("SERIAL_NUMBER","END_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),f.desc("END_ACTION_DATE"),f.desc("END_ACTION_NUMBER"))
df383 = df381.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df402 = df383.alias("df383").join(df379.alias("df379"),((df383.SERIAL_NUMBER == df379.SERIAL_NUMBER) & (df383.END_ACTION_DATE == df379.END_ACTION_DATE)),"anti").select(col("df383.END_5_CHARACTERS"),col("df383.END_CM_DESC"),col("df383.FILING_BASIS_CUR"),col("df383.FILING_BASIS_FIL"),col("df383.END_ACTION_CODE"),col("df383.END_ACTION_DATE"),col("df383.END_ACTION_NUMBER"),col("df383.REGISTRATION_DT"),col("df383.REGISTRATION_NUMBER"),col("df383.SERIAL_NUMBER"),col("df383.Start_End"),
col("df383.POSTREG_CATEGORY"),col("df383.Registration_Year"),col("df383.Action_Year"))

##Stack Orphan End code lines with the Final Output Data of this Section
df403 = df402.unionByName(df390,allowMissingColumns=True).unionByName(df398,allowMissingColumns=True).unionByName(df379,allowMissingColumns=True)

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df403")
df403 = df403.checkpoint(True)

df463 = df460.unionByName(df403,allowMissingColumns=True)
df464 = df463.withColumn("RENEWAL_DT",f.col("END_ACTION_DATE"))\
    .withColumn("Renewal_Number",f.when(f.col('Renewal_DT').isNotNull() & f.col('REGISTRATION_DT').isNotNull(),
                                                    f.when((f.year("Renewal_DT")<1989) | ((f.year("Renewal_DT")==1989) & (f.month("Renewal_DT")< 11)) |((f.year("Renewal_DT")==1989) & (f.month("Renewal_DT")==11) & (f.date_format("Renewal_DT","d") <16)) ,f.floor((f.year("Renewal_DT") - f.year("REGISTRATION_DT") + 2) / 20))
                                                    .when(((f.year("Renewal_DT")>1989) | ((f.year("Renewal_DT")==1989) & (f.month("Renewal_DT")== 12)) |((f.year("Renewal_DT")==1989) & (f.month("Renewal_DT")==11) & (f.date_format("Renewal_DT","d") >=16)))
                                                          & ((f.year("REGISTRATION_DT")>1989) | ((f.year("REGISTRATION_DT")==1989) & (f.month("REGISTRATION_DT")== 12)) |((f.year("REGISTRATION_DT")==1989) & (f.month("REGISTRATION_DT")==11) & (f.date_format("REGISTRATION_DT","d") >=16))),f.floor((f.year("Renewal_DT") - f.year("REGISTRATION_DT") + 2) / 10))
                                                    .otherwise(f.floor((f.year("Renewal_DT") - (20 - ((1989 - f.year("REGISTRATION_DT"))% 20) + 1989) + 2) / 10) + f.floor((1989 - f.year("REGISTRATION_DT") + 2) / 20) + f.when( ((f.substring((f.year("REGISTRATION_DT")).cast("string"), -1,1)).cast("integer") ==0 )| ((f.substring((f.year("REGISTRATION_DT")).cast("string"), -1,1)).cast("integer") ==1 ),0).otherwise(1))
                                                    )
                .when(f.col('Renewal_DT').isNull() & f.col('REGISTRATION_DT').isNotNull(),0)
                .otherwise(f.lit(None)) 
                           )\
    .withColumn("Renewal_Number",f.when((f.col('Renewal_Number').isNull()) | (f.col("Renewal_Number")<=0),1).otherwise(f.col("Renewal_Number")))

##Added sort in Alteryx wf for results to match

w = Window.partitionBy("SERIAL_NUMBER").orderBy("SERIAL_NUMBER","RENEWAL_DT","START_ACTION_DATE","END_ACTION_DATE")
df465 = df464.withColumn("Renewal Number Updated",f.when(((f.col('Renewal_Number') == f.lag(f.col('Renewal_Number'),1).over(w)) & ((f.year("RENEWAL_DT").isNull())|(f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8))),f.col('Renewal_Number')+1).otherwise(f.col('Renewal_Number')))

df466_ind = df465.withColumn("Renewal Number Updated Ind",f.when(((f.col('Renewal Number Updated') == f.lag(f.col('Renewal Number Updated'),1).over(w)) & (f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8)),f.lit(1)).otherwise(f.lit(0)))

df466 = df466_ind.withColumn("Renewal Number Updated Ind", f.when((f.col('Renewal Number Updated Ind')== f.lit(1) )&((f.col('Renewal Number Updated Ind') != f.lag(f.col('Renewal Number Updated Ind'),1).over(w))),f.lit(1)).otherwise(0)).withColumn("Renewal Number Updated", f.col('Renewal Number Updated')+f.col('Renewal Number Updated Ind')).drop('Renewal Number Updated Ind')

df467_ind = df466.withColumn("Renewal Number Updated Ind",f.when(((f.col('Renewal Number Updated') == f.lag(f.col('Renewal Number Updated'),1).over(w)) & (f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8)),f.lit(1)).otherwise(f.lit(0)))

df467 = df466.withColumn("Renewal Number Updated",f.when(((f.col('Renewal Number Updated') == f.lag(f.col('Renewal Number Updated'),1).over(w)) & (f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8)),f.col('Renewal Number Updated')+1).otherwise(f.col('Renewal Number Updated')))

df468_ind = df467.withColumn("Renewal Number Updated Ind",f.when(((f.col('Renewal Number Updated') == f.lag(f.col('Renewal Number Updated'),1).over(w)) & (f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8)),f.lit(1)).otherwise(f.lit(0)))

df468 = df467.withColumn("Renewal Number Updated",f.when(((f.col('Renewal Number Updated') == f.lag(f.col('Renewal Number Updated'),1).over(w)) & (f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8)),f.col('Renewal Number Updated')+1).otherwise(f.col('Renewal Number Updated')))

df469 = df468.withColumn("Renewal Number Updated",f.when(((f.col('Renewal Number Updated') == f.lag(f.col('Renewal Number Updated'),1).over(w)) & (f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8)),f.col('Renewal Number Updated')+1).otherwise(f.col('Renewal Number Updated')))

df471 = df469.withColumn("Renewal Number Updated",f.when(f.col('INACTIVE').isNotNull(),f.lit(None))
                         .when(((f.col('Renewal Number Updated') == f.lag(f.col('Renewal Number Updated'),1).over(w)) & (f.year("RENEWAL_DT") >= (f.lag(f.year("RENEWAL_DT"),1).over(w))+8)),f.col('Renewal Number Updated')+1).otherwise(f.col('Renewal Number Updated')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get First Action

# COMMAND ----------

##Keep Valid START_END Pairs
df404 = df373.where('END_ACTION_NUMBER > START_ACTION_NUMBER')

##Keep one line per event and remove redundant data lines 
w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_ACTION_NUMBER"),col("END_ACTION_NUMBER"))
df406 = df404.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df410 = df406.selectExpr("SERIAL_NUMBER","END_5_CHARACTERS as FIRST_ACTION_CODE","START_ACTION_DATE as FA_START_DATE","END_ACTION_DATE as FIRST_ACTION_DATE","START_ACTION_NUMBER as FA_START_NUMBER","END_ACTION_NUMBER as FIRST_ACTION_NUMBER","END_CM_DESC as FIRST_ACTION_DESC")

##Stack Orphan End code lines with the Final Output Data of this Section
df473 = df471.alias("df471").join(df410.alias("df410"),(col("df471.SERIAL_NUMBER") == col("df410.SERIAL_NUMBER")) & (col("df471.START_ACTION_DATE") == col("df410.FA_START_DATE")) ,"leftouter").selectExpr("df471.END_ACTION_NUMBER","df471.Right_POSTREG_CATEGORY","df471.Right_15_FLAG","df471.DUPLICATE","df471.SERIAL_NUMBER","df471.START_ACTION_CODE","df471.Right_Right_15_FLAG","df471.FIRST_ACTION_NUMBER","df471.FIRST_ACTION_DESC","df471.FIRST_ACTION_DATE","df471.FA_START_NUMBER","df471.REGISTRATION_DT","df471.INACTIVE","df471.END_ACTION_YR","df471.START_ACTION_YR","df471.FLAG_POST_REG","df471.REGISTRATION_NUMBER","df471.END_5_CHARACTERS","df471.END_ACTION_DATE","df471.END_ACTION_CODE","df471.END_CM_DESC","df471.Right_REGISTRATION_NUMBER","df471.Start_End","df471.Renewal_Number","df471.Action_Year","df471.START_5_CHARACTERS","df471.START_ACTION_DATE","df471.START_ACTION_NUMBER","df471.START_CM_DESC","df410.FIRST_ACTION_CODE Right_FIRST_ACTION_CODE","df471.FIRST_ACTION_CODE","df471.FA_START_DATE","df471.PRA_Mailed","df471.Registration_Year","df410.FIRST_ACTION_DATE Right_FIRST_ACTION_DATE","df410.FA_START_NUMBER Right_FA_START_NUMBER","df410.FIRST_ACTION_NUMBER Right_FIRST_ACTION_NUMBER","df410.FIRST_ACTION_DESC Right_FIRST_ACTION_DESC","df471.POSTREG_CATEGORY","df471.RENEWAL_DT","df410.FA_START_DATE Right_FA_START_DATE","df471.`Renewal Number Updated`","df471.FILING_BASIS_CUR","df471.FILING_BASIS_FIL","df471.15_FLAG","df471.INVENTORY")

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df473")
# df473 = df473.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SECTION 7 DATA - UPDATED: One-to-Many Pairing with Inferred START Support

# COMMAND ----------

# Define Section 7 Code Lists
SECTION_7_START_CODES = [
    "AMD7I",   # Section 7 Amendment Filed
    "ES7RI",   # TEAS Section 7 Request Received
    "C.7FI",   # Correction Section 7 Filed
    "C7PFI",   # Correction Section 7 Partial Filed
    "C7RFI",   # Correction Section 7 Request Filed
    "ES7SI"    # TEAS Section 7 Statement
]

SECTION_7_END_CODES = [
    "7.PRO",   # Section 7 Processed
    "A7OKO",   # Amendment Under Section 7 Processed
    "C.7CO",   # Correction Under Section 7 Complete
    "C7..O",   # Correction Section 7 Processed
    "C7P.O",   # Correction Section 7 Partial Processed
    "COC.O"    # Correction Under Section 7 Processed
]

SECTION_7_PRA_CODES = [
    "PRANO",   # PRA Notice
    "PRA7O",   # PRA Section 7
    "PRAMO"    # PRA Mailed
]

# Codes that can be inferred as START when no explicit Section 7 filing exists
SECTION_7_INFERRED_START_CODES = [
    "EROPI",   # TEAS Response to Office Action - Post Reg Received
    "TROAI",   # TEAS Response to Office Action Received
    "CRFAI",   # Correspondence Received in Law Office
    "ERFRI",   # TEAS Request for Reconsideration Received
    "MAILI"    # Paper Received
]

SECTION_7_ALL_CODES = SECTION_7_START_CODES + SECTION_7_END_CODES + SECTION_7_PRA_CODES

# COMMAND ----------

# Filter Section 7 Records
df604 = df356.where(f.col('5Characters').isin(SECTION_7_ALL_CODES))

# Identify Start and End Codes
df601 = df604.withColumn("Start_End",
    f.when(f.col('5Characters').isin(SECTION_7_START_CODES), "Start").otherwise("End"))\
    .withColumn("POSTREG_CATEGORY", f.lit("SECTION 7"))\
    .withColumn("Registration_Year", f.year("REGISTRATION_DT"))\
    .withColumn("Action_Year", f.year("PH_ACTION_DATE"))\
    .withColumn("15_FLAG", f.when(f.col('5Characters').isin(
        "15AKO", "C15AO", "C15PO", "C75AO", "C75PO", "NA75E", "NA75O",
        "NA85E", "NA85O", "PR15O", "PR23O", "PR75O", "15AFI", "715FI",
        "815FI", "E15RI", "E815I", "ES75I"), 1))

df599 = df601.where(f.col('5Characters').isin(SECTION_7_START_CODES))
df599_f = df601.where(~f.col('5Characters').isin(SECTION_7_START_CODES))

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df599")
# df599 = df599.checkpoint(True)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df599_f")
# df599_f = df599_f.checkpoint(True)

# COMMAND ----------

# Cross Join START and END Records
df600 = (
    df599.alias("df599")
    .join(
        df599_f.alias("df599_f"),
        df599.SERIAL_NUMBER == df599_f.SERIAL_NUMBER,
        "inner"
    )
    .select(
        col("df599.5Characters").alias("START_5_CHARACTERS"),
        col("df599.CM_Desc").alias("START_CM_DESC"),
        col("df599.FILING_BASIS_CUR"),
        col("df599.FILING_BASIS_FIL"),
        col("df599.PH_ACTION_CODE").alias("START_ACTION_CODE"),
        col("df599.PH_ACTION_DATE").alias("START_ACTION_DATE"),
        col("df599.PH_ACTION_NUMBER").alias("START_ACTION_NUMBER"),
        col("df599.REGISTRATION_DT"),
        col("df599.REGISTRATION_NUMBER"),
        col("df599.SERIAL_NUMBER"),
        col("df599.Start_End"),
        col("df599.POSTREG_CATEGORY"),
        col("df599.Registration_Year"),
        col("df599.Action_Year"),
        col("df599.15_FLAG"),
        col("df599_f.5Characters").alias("END_5_CHARACTERS"),
        col("df599_f.CM_Desc").alias("END_CM_DESC"),
        col("df599_f.FILING_BASIS_CUR").alias("Right_FILING_BASIS_CUR"),
        col("df599_f.FILING_BASIS_FIL").alias("Right_FILING_BASIS_FIL"),
        col("df599_f.PH_ACTION_CODE").alias("END_ACTION_CODE"),
        col("df599_f.PH_ACTION_DATE").alias("END_ACTION_DATE"),
        col("df599_f.PH_ACTION_NUMBER").alias("END_ACTION_NUMBER"),
        col("df599_f.REGISTRATION_NUMBER").alias("Right_REGISTRATION_NUMBER"),
        col("df599_f.Start_End").alias("Right_Start_End"),
        col("df599_f.POSTREG_CATEGORY").alias("Right_POSTREG_CATEGORY"),
        col("df599_f.Registration_Year").alias("Right_Registration_Year"),
        col("df599_f.Action_Year").alias("Right_Action_Year"),
        col("df599_f.15_FLAG").alias("Right_15_FLAG")
    )
)

# COMMAND ----------

# Flag Valid START-END Pairings
df598 = df600.withColumn("FLAG_POST_REG",
    f.when(f.col('START_5_CHARACTERS').isin("AMD7I") &
           f.col('END_5_CHARACTERS').isin("7.PRO", "A7OKO", "C.7CO", "C7..O", "C7P.O", "COC.O"), "1")
    .when(f.col('START_5_CHARACTERS').isin("C.7FI") &
           f.col('END_5_CHARACTERS').isin("C.7CO", "COC.O"), "1")
    .when(f.col('START_5_CHARACTERS').isin("C7PFI") &
           f.col('END_5_CHARACTERS').isin("7.PRO", "C7..O", "C7P.O"), "1")
    .when(f.col('START_5_CHARACTERS').isin("C7RFI") &
           f.col('END_5_CHARACTERS').isin("A7OKO", "C7..O", "C7P.O"), "1")
    .when(f.col('START_5_CHARACTERS').isin("ES7RI") &
           f.col('END_5_CHARACTERS').isin("7.PRO", "A7OKO", "C.7CO", "C7..O", "C7P.O", "COC.O"), "1")
    .when(f.col('START_5_CHARACTERS').isin("ES7SI") &
           f.col('END_5_CHARACTERS').isin("A7OKO", "C7..O", "C7P.O"), "1")
    .otherwise("0"))\
    .withColumn("START_ACTION_YR", f.year("START_ACTION_DATE"))\
    .withColumn("END_ACTION_YR", f.year("END_ACTION_DATE"))\
    .withColumn("PRA_Mailed",
        f.when(f.col('START_5_CHARACTERS').isin("AMD7I") &
               f.col('END_5_CHARACTERS').isin("PRANO", "PRA7O", "PRAMO"), "1")
        .when(f.col('START_5_CHARACTERS').isin("C.7FI") &
               f.col('END_5_CHARACTERS').isin("PRANO", "PRA7O", "PRAMO"), "1")
        .when(f.col('START_5_CHARACTERS').isin("C7PFI") &
               f.col('END_5_CHARACTERS').isin("PRANO", "PRA7O", "PRAMO"), "1")
        .when(f.col('START_5_CHARACTERS').isin("C7RFI") &
               f.col('END_5_CHARACTERS').isin("PRANO", "PRA7O", "PRAMO"), "1")
        .when(f.col('START_5_CHARACTERS').isin("ES7RI") &
               f.col('END_5_CHARACTERS').isin("PRANO", "PRA7O", "PRAMO"), "1")
        .when(f.col('START_5_CHARACTERS').isin("ES7SI") &
               f.col('END_5_CHARACTERS').isin("PRANO", "PRA7O", "PRAMO"), "1")
        .otherwise("0"))

df597 = df598.selectExpr(
    "SERIAL_NUMBER", "START_5_CHARACTERS", "END_5_CHARACTERS", "START_ACTION_DATE",
    "END_ACTION_DATE", "START_ACTION_NUMBER", "END_ACTION_NUMBER", "START_CM_DESC",
    "END_CM_DESC", "FILING_BASIS_FIL", "FLAG_POST_REG", "START_ACTION_YR", "END_ACTION_YR",
    "START_ACTION_CODE", "REGISTRATION_DT", "Start_End", "Registration_Year", "Action_Year",
    "END_ACTION_CODE", "Right_FILING_BASIS_FIL", "Right_Start_End", "Right_Registration_Year",
    "Right_Action_Year", "FILING_BASIS_CUR", "Right_FILING_BASIS_CUR", "PRA_Mailed",
    "REGISTRATION_NUMBER", "POSTREG_CATEGORY", "15_FLAG", "Right_REGISTRATION_NUMBER",
    "Right_POSTREG_CATEGORY", "Right_15_FLAG"
)

# COMMAND ----------

# Filter Valid Pairings (END > START)
df595 = df597.where('END_ACTION_NUMBER > START_ACTION_NUMBER').where('FLAG_POST_REG = 1')
df595_f = df597.where('END_ACTION_NUMBER > START_ACTION_NUMBER').where('FLAG_POST_REG != 1')

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df595")
# df595 = df595.checkpoint(True)

# COMMAND ----------

# ONE-TO-MANY PAIRING: Each END pairs with most recent START
w_end_to_start = Window.partitionBy(
    "SERIAL_NUMBER",
    "END_ACTION_NUMBER",
    "END_ACTION_DATE"
).orderBy(f.desc("START_ACTION_NUMBER"))

df594 = df595.withColumn(
    "row",
    row_number().over(w_end_to_start)
).filter(col("row") == 1).drop("row")

# Deduplicate END Records
w_dedup_end = Window.partitionBy(
    "SERIAL_NUMBER",
    "END_5_CHARACTERS",
    "END_ACTION_DATE",
    "END_ACTION_NUMBER"
).orderBy(
    f.desc("START_ACTION_NUMBER"),
    f.desc("START_ACTION_DATE")
)

df620 = df594.withColumn(
    "row",
    row_number().over(w_dedup_end)
).filter(col("row") == 1).drop("row")

# Final Deduplication by START_ACTION_DATE
w2 = Window.partitionBy("SERIAL_NUMBER", "START_ACTION_DATE").orderBy(
    "SERIAL_NUMBER", "START_ACTION_NUMBER", "END_ACTION_NUMBER"
)
df621 = df620.withColumn("row", row_number().over(w2)).filter(col("row") == 1).drop("row")

df579 = df621.selectExpr(
    "SERIAL_NUMBER", "START_5_CHARACTERS", "END_5_CHARACTERS", "START_ACTION_DATE",
    "END_ACTION_DATE", "START_ACTION_NUMBER", "END_ACTION_NUMBER", "START_CM_DESC",
    "END_CM_DESC", "FILING_BASIS_FIL", "FLAG_POST_REG", "START_ACTION_YR", "END_ACTION_YR",
    "START_ACTION_CODE", "REGISTRATION_DT", "Start_End", "Registration_Year", "Action_Year",
    "END_ACTION_CODE", "FILING_BASIS_CUR", "PRA_Mailed", "REGISTRATION_NUMBER",
    "POSTREG_CATEGORY", "15_FLAG", "Right_REGISTRATION_NUMBER", "Right_POSTREG_CATEGORY",
    "Right_15_FLAG"
)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df579")
# df579 = df579.checkpoint(True)

# COMMAND ----------

# Handle PRA Mailed Records
df619 = df595_f.where('END_ACTION_NUMBER > START_ACTION_NUMBER').where('PRA_Mailed = 1')

w2 = Window.partitionBy("SERIAL_NUMBER", "START_ACTION_DATE").orderBy(
    "SERIAL_NUMBER", "START_ACTION_NUMBER", "END_ACTION_NUMBER"
)
df625 = df619.withColumn("row", row_number().over(w2)).filter(col("row") == 1).drop("row")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan Start Code Transaction lines (~2% Data Flows here)

# COMMAND ----------

# Process Orphan START Records
df609 = df599.selectExpr(
    "SERIAL_NUMBER",
    "PH_ACTION_NUMBER as START_ACTION_NUMBER",
    "PH_ACTION_CODE as START_ACTION_CODE",
    "PH_ACTION_DATE as START_ACTION_DATE",
    "CM_Desc as START_CM_DESC",
    "5Characters as START_5_CHARACTERS",
    "REGISTRATION_DT",
    "FILING_BASIS_CUR",
    "FILING_BASIS_FIL",
    "REGISTRATION_NUMBER",
    "Start_End",
    "POSTREG_CATEGORY",
    "Registration_Year",
    "Action_Year",
    "15_FLAG"
)

w2 = Window.partitionBy("SERIAL_NUMBER", "START_ACTION_DATE").orderBy(
    col("SERIAL_NUMBER"), col("START_ACTION_DATE"), col("START_ACTION_NUMBER")
)
df612 = df609.withColumn("row", row_number().over(w2)).filter(col("row") == 1).drop("row")

w = Window.partitionBy("SERIAL_NUMBER").orderBy(
    "SERIAL_NUMBER", "START_ACTION_DATE", "START_ACTION_NUMBER"
)
df610 = df612.withColumn("DUPLICATE",
    f.when(
        (f.col('SERIAL_NUMBER') == f.lead(f.col('SERIAL_NUMBER'), 1).over(w)) &
        (f.datediff(f.lead(f.col('START_ACTION_DATE'), 1).over(w), f.col('START_ACTION_DATE')) < 7),
        1
    ).otherwise("0")
)

df611 = df610.where('DUPLICATE = 0')

# Find Orphan STARTs (not matched with any END)
df605 = df611.alias("df611").join(
    df579.alias("df579"),
    (col("df611.SERIAL_NUMBER") == col("df579.SERIAL_NUMBER")) &
    (col("df611.START_ACTION_DATE") == col("df579.START_ACTION_DATE")),
    "anti"
)

# Split Orphan STARTs by Cancellation Status
df606_anti = df605.alias("df605").join(
    df363.alias("df363"),
    col("df605.SERIAL_NUMBER") == col("df363.AM_SER_NUM"),
    "anti"
)

df606_inner = df605.alias("df605").join(
    df363.alias("df363"),
    col("df605.SERIAL_NUMBER") == col("df363.AM_SER_NUM"),
    "inner"
)

# Handle Cancelled Orphan STARTs
df616 = df606_anti.alias("df606_anti").join(
    df363_f.alias("df363_f"),
    col("df606_anti.SERIAL_NUMBER") == col("df363_f.AM_SER_NUM"),
    "inner"
).select(
    col("df606_anti.*"),
    col("df363_f.AM_DT_CNCL").alias("END_ACTION_DATE")
)

df731 = df616.withColumn("INACTIVE", f.lit(1))\
    .withColumn("END_5_CHARACTERS", f.lit("CANCELLED"))\
    .withColumn("END_CM_DESC", f.lit("CANCELLED"))

df615 = df731.selectExpr(
    "SERIAL_NUMBER", "REGISTRATION_NUMBER", "REGISTRATION_DT", "START_ACTION_NUMBER",
    "START_ACTION_CODE", "START_ACTION_DATE", "START_CM_DESC", "START_5_CHARACTERS",
    "FILING_BASIS_CUR", "FILING_BASIS_FIL", "Start_End", "POSTREG_CATEGORY",
    "Registration_Year", "Action_Year", "15_FLAG", "DUPLICATE", "INACTIVE",
    "END_ACTION_DATE", "END_5_CHARACTERS", "END_CM_DESC"
)

# Handle Active Orphan STARTs (Inventory)
df613 = df606_inner.withColumn("INVENTORY", f.lit(1))

df607 = df613.selectExpr(
    "SERIAL_NUMBER", "REGISTRATION_NUMBER", "REGISTRATION_DT", "START_ACTION_NUMBER",
    "START_ACTION_CODE", "START_ACTION_DATE", "START_CM_DESC", "START_5_CHARACTERS",
    "FILING_BASIS_CUR", "FILING_BASIS_FIL", "Start_End", "POSTREG_CATEGORY",
    "Registration_Year", "Action_Year", "15_FLAG", "DUPLICATE", "INVENTORY"
)

# Join with PRA Mailed Records
df626_anti = df607.alias("df607").join(
    df625.alias("df625"),
    (col("df607.SERIAL_NUMBER") == col("df625.SERIAL_NUMBER")) &
    (col("df607.START_ACTION_DATE") == col("df625.START_ACTION_DATE")),
    "anti"
)

df626_inner = df607.alias("df607").join(
    df625.alias("df625"),
    (col("df607.SERIAL_NUMBER") == col("df625.SERIAL_NUMBER")) &
    (col("df607.START_ACTION_DATE") == col("df625.START_ACTION_DATE")),
    "inner"
).select(
    "df625.SERIAL_NUMBER", "df625.START_5_CHARACTERS", "df625.END_5_CHARACTERS",
    "df625.START_ACTION_DATE", "END_ACTION_DATE", "df625.START_ACTION_NUMBER",
    "END_ACTION_NUMBER", "df625.START_CM_DESC", "END_CM_DESC", "df625.FILING_BASIS_FIL",
    "FLAG_POST_REG", "START_ACTION_YR", "END_ACTION_YR", "df625.START_ACTION_CODE",
    "df625.REGISTRATION_DT", "df625.Start_End", "df625.Registration_Year",
    "df625.Action_Year", "df625.END_ACTION_CODE", "df625.FILING_BASIS_CUR",
    "df625.PRA_Mailed", "df625.REGISTRATION_NUMBER", "df625.POSTREG_CATEGORY",
    "df625.15_FLAG"
)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df626_anti")
# df626_anti = df626_anti.checkpoint(True)

df617 = df607.alias("df607").join(
    df626_anti.alias("df626_anti"),
    (col("df607.SERIAL_NUMBER") == col("df626_anti.SERIAL_NUMBER")) &
    (col("df607.START_ACTION_DATE") == col("df626_anti.START_ACTION_DATE")),
    "inner"
).selectExpr(
    "df607.SERIAL_NUMBER",
    "df607.REGISTRATION_NUMBER",
    "df607.REGISTRATION_DT",
    "df607.START_ACTION_NUMBER",
    "df607.START_ACTION_CODE",
    "df607.START_ACTION_DATE",
    "df607.START_CM_DESC",
    "df607.START_5_CHARACTERS",
    "df607.FILING_BASIS_CUR",
    "df607.FILING_BASIS_FIL",
    "df607.Start_End",
    "df607.POSTREG_CATEGORY",
    "df607.Registration_Year",
    "df607.Action_Year",
    "df607.15_FLAG",
    "df607.DUPLICATE",
    "df607.INVENTORY",
    # Right side columns from df626_anti
    "df626_anti.SERIAL_NUMBER as Right_SERIAL_NUMBER",
    "df626_anti.REGISTRATION_NUMBER as Right_REGISTRATION_NUMBER",
    "df626_anti.REGISTRATION_DT as Right_REGISTRATION_DT",
    "df626_anti.START_ACTION_NUMBER as Right_START_ACTION_NUMBER",
    "df626_anti.START_ACTION_CODE as Right_START_ACTION_CODE",
    "df626_anti.START_ACTION_DATE as Right_START_ACTION_DATE",
    "df626_anti.START_CM_DESC as Right_START_CM_DESC",
    "df626_anti.START_5_CHARACTERS as Right_START_5_CHARACTERS",
    "df626_anti.FILING_BASIS_CUR as Right_FILING_BASIS_CUR",
    "df626_anti.FILING_BASIS_FIL as Right_FILING_BASIS_FIL",
    "df626_anti.Start_End as Right_Start_End",
    "df626_anti.POSTREG_CATEGORY as Right_POSTREG_CATEGORY",
    "df626_anti.Registration_Year as Right_Registration_Year",
    "df626_anti.Action_Year as Right_Action_Year",
    "df626_anti.15_FLAG as Right_15_FLAG",
    "df626_anti.DUPLICATE as Right_DUPLICATE",
    "df626_anti.INVENTORY as Right_INVENTORY"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan End Code Transaction lines (~8% Data Flows here) - ENHANCED

# COMMAND ----------

# Get All END Records for Orphan Processing
df580 = df599_f.where(f.col('5Characters').isin(SECTION_7_END_CODES))

df581 = df580.selectExpr(
    "SERIAL_NUMBER",
    "PH_ACTION_NUMBER as END_ACTION_NUMBER",
    "PH_ACTION_CODE as END_ACTION_CODE",
    "PH_ACTION_DATE as END_ACTION_DATE",
    "CM_Desc as END_CM_DESC",
    "5Characters as END_5_CHARACTERS",
    "REGISTRATION_DT",
    "FILING_BASIS_CUR",
    "FILING_BASIS_FIL",
    "REGISTRATION_NUMBER",
    "Start_End",
    "POSTREG_CATEGORY",
    "Registration_Year",
    "Action_Year",
    "15_FLAG"
)

w2 = Window.partitionBy("SERIAL_NUMBER", "END_ACTION_DATE").orderBy(
    col("SERIAL_NUMBER"), f.desc("END_ACTION_DATE"), f.desc("END_ACTION_NUMBER")
)
df583 = df581.withColumn("row", row_number().over(w2)).filter(col("row") == 1).drop("row")

# Find True Orphan ENDs (not matched with explicit Section 7 START)
df602_initial = df583.alias("df583").join(
    df579.alias("df579"),
    (col("df583.SERIAL_NUMBER") == col("df579.SERIAL_NUMBER")) &
    (col("df583.END_ACTION_DATE") == col("df579.END_ACTION_DATE")),
    "anti"
).selectExpr(
    "SERIAL_NUMBER", "REGISTRATION_NUMBER", "FILING_BASIS_CUR", "END_5_CHARACTERS",
    "END_ACTION_DATE", "END_ACTION_CODE", "END_CM_DESC", "FILING_BASIS_FIL",
    "REGISTRATION_DT", "END_ACTION_NUMBER", "Start_End", "POSTREG_CATEGORY",
    "Registration_Year", "Action_Year", "15_FLAG"
)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df602_initial")
# df602_initial = df602_initial.checkpoint(True)

# COMMAND ----------

# Get all potential incoming actions that could serve as inferred STARTs
potential_starts = df356.where(
    f.col('5Characters').isin(SECTION_7_INFERRED_START_CODES)
).selectExpr(
    "SERIAL_NUMBER as ps_serial",
    "PH_ACTION_NUMBER as ps_action_number",
    "PH_ACTION_DATE as ps_action_date",
    "5Characters as ps_5characters",
    "CM_Desc as ps_cm_desc",
    "REGISTRATION_DT as ps_reg_dt",
    "FILING_BASIS_CUR as ps_filing_basis",
    "FILING_BASIS_FIL as ps_filing_basis_fil",
    "REGISTRATION_NUMBER as ps_reg_number"
)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_potential_starts")
# potential_starts = potential_starts.checkpoint(True)

orphan_with_potential = df602_initial.alias("orphan").join(
    potential_starts.alias("ps"),
    col("orphan.SERIAL_NUMBER") == col("ps.ps_serial"), 
    "left"
).where(
    (col("ps.ps_action_number").isNull()) | 
    (
        (col("orphan.END_ACTION_NUMBER") > col("ps.ps_action_number")) &
        (col("orphan.END_ACTION_DATE") >= col("ps.ps_action_date")) &
        (f.datediff(col("orphan.END_ACTION_DATE"), col("ps.ps_action_date")) <= 180)
    )
)

# For each orphan END, get the most recent potential START
w_infer = Window.partitionBy(
    "SERIAL_NUMBER",
    "END_ACTION_NUMBER"
).orderBy(f.desc("ps_action_number"))

inferred_pairs = orphan_with_potential.withColumn(
    "row",
    row_number().over(w_infer)
).filter(col("row") == 1).drop("row")

# COMMAND ----------

# Create Enhanced Orphan Dataset with Inferred STARTs
df602 = inferred_pairs.selectExpr(
    "SERIAL_NUMBER", "REGISTRATION_NUMBER", "FILING_BASIS_CUR", "END_5_CHARACTERS",
    "END_ACTION_DATE", "END_ACTION_CODE", "END_CM_DESC", "FILING_BASIS_FIL",
    "REGISTRATION_DT", "END_ACTION_NUMBER", "Start_End", "POSTREG_CATEGORY",
    "Registration_Year", "Action_Year", "15_FLAG",
    # Inferred START columns (will be NULL if no match found)
    "ps_action_number as START_ACTION_NUMBER",
    "ps_action_date as START_ACTION_DATE",
    "ps_5characters as START_5_CHARACTERS",
    "ps_cm_desc as START_CM_DESC"
)

# Union All Section 7 Records
df603 = df602.unionByName(df626_inner, allowMissingColumns=True)\
    .unionByName(df579, allowMissingColumns=True)\
    .unionByName(df617, allowMissingColumns=True)\
    .unionByName(df615, allowMissingColumns=True)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df603")
# df603 = df603.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get First Action

# COMMAND ----------

# Calculate First Action for Section 7
df627 = df597.where('END_ACTION_NUMBER > START_ACTION_NUMBER')

w2 = Window.partitionBy("SERIAL_NUMBER", "START_ACTION_DATE").orderBy(
    col("SERIAL_NUMBER"), col("START_ACTION_NUMBER"), col("END_ACTION_NUMBER")
)
df629 = df627.withColumn("row", row_number().over(w2)).filter(col("row") == 1).drop("row")

df633 = df629.selectExpr(
    "SERIAL_NUMBER",
    "END_5_CHARACTERS as FIRST_ACTION_CODE",
    "START_ACTION_DATE as FA_START_DATE",
    "END_ACTION_DATE as FIRST_ACTION_DATE",
    "START_ACTION_NUMBER as FA_START_NUMBER",
    "END_ACTION_NUMBER as FIRST_ACTION_NUMBER",
    "END_CM_DESC as FIRST_ACTION_DESC"
)

# Join First Action with Section 7 Records
# df603 is now checkpointed so this join is efficient
df636 = df603.alias("df603").join(
    df633.alias("df633"),
    (col("df603.SERIAL_NUMBER") == col("df633.SERIAL_NUMBER")) &
    (col("df603.START_ACTION_DATE") == col("df633.FA_START_DATE")),
    "leftouter"
).selectExpr(
    "df603.*",
    "df633.FA_START_DATE",
    "df633.FA_START_NUMBER",
    "df633.FIRST_ACTION_CODE",
    "df633.FIRST_ACTION_DATE",
    "df633.FIRST_ACTION_DESC",
    "df633.FIRST_ACTION_NUMBER"
)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df636")
# df636 = df636.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SECTION 15

# COMMAND ----------

df546 = df356.where('5Characters in ("15AFI", "E15RI","PR75I", "PR15O", "15AKO", "PR75O","PR23O","PRANO")')

df528 = df546.withColumn("Start_End",f.when(f.col('5Characters').isin("15AFI","E15RI"),"Start").otherwise("End"))\
            .withColumn("POSTREG_CATEGORY",f.lit("SEPARATE 15"))\
            .withColumn("Registration_Year",f.year("REGISTRATION_DT"))\
            .withColumn("Action_Year",f.year("PH_ACTION_DATE"))\
            .withColumn("15_FLAG",f.when(f.col('5Characters').isin("15AKO","C15AO","C15PO","C75AO","C75PO","NA75E","NA75O","NA85E","NA85O","PR15O","PR23O","PR75O","15AFI","715FI","815FI","E15RI","E815I","ES75I"),1))

df530 = df528.where('5Characters in ("15AFI", "E15RI")')

df530_f = df528.where('5Characters not in ("15AFI", "E15RI")')

df529 = (
    df530.alias("df530")
    .join(
        df530_f.alias("df530_f"), df530.SERIAL_NUMBER == df530_f.SERIAL_NUMBER, "inner"
    )
    .select(
        col("df530.5Characters").alias("START_5_CHARACTERS"),
        col("df530.CM_Desc").alias("START_CM_DESC"),
        col("df530.FILING_BASIS_CUR"),
        col("df530.FILING_BASIS_FIL"),
        col("df530.PH_ACTION_CODE").alias("START_ACTION_CODE"),
        col("df530.PH_ACTION_DATE").alias("START_ACTION_DATE"),
        col("df530.PH_ACTION_NUMBER").alias("START_ACTION_NUMBER"),
        col("df530.REGISTRATION_DT"),
        col("df530.REGISTRATION_NUMBER"),
        col("df530.SERIAL_NUMBER"),
        col("df530.Start_End"),
        col("df530.POSTREG_CATEGORY"),
        col("df530.Registration_Year"),
        col("df530.Action_Year"),
        col("df530.15_FLAG"),
        col("df530_f.5Characters").alias("END_5_CHARACTERS"),
        col("df530_f.CM_Desc").alias("END_CM_DESC"),
        col("df530_f.FILING_BASIS_CUR").alias("Right_FILING_BASIS_CUR"),
        col("df530_f.FILING_BASIS_FIL").alias("Right_FILING_BASIS_FIL"),
        col("df530_f.PH_ACTION_CODE").alias("END_ACTION_CODE"),
        col("df530_f.PH_ACTION_DATE").alias("END_ACTION_DATE"),
        col("df530_f.PH_ACTION_NUMBER").alias("END_ACTION_NUMBER"),
        col("df530_f.REGISTRATION_NUMBER").alias("Right_REGISTRATION_NUMBER"),
        col("df530_f.Start_End").alias("Right_Start_End"),
        col("df530_f.POSTREG_CATEGORY").alias("Right_POSTREG_CATEGORY"),
        col("df530_f.Registration_Year").alias("Right_Registration_Year"),
        col("df530_f.Action_Year").alias("Right_Action_Year"),
        col("df530_f.15_FLAG").alias("Right_15_FLAG")
    )
)

df542 = df529.withColumn("FLAG_POST_REG",f.when(f.col('START_5_CHARACTERS').isin("15AFI") & f.col('END_5_CHARACTERS').isin("PR15O", "15AKO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("E15RI") & f.col('END_5_CHARACTERS').isin("PR15O", "15AKO"),"1")\
                                        .otherwise("0"))\
            .withColumn("START_ACTION_YR",f.year("START_ACTION_DATE"))\
            .withColumn("END_ACTION_YR",f.year("END_ACTION_DATE"))\
            .withColumn("PRA_Mailed",f.when(f.col('START_5_CHARACTERS').isin("15AFI") & f.col('END_5_CHARACTERS').isin("PR75I","PR75O","PR15O","PR23O","PRANO"),"1")\
                                        .when(f.col('START_5_CHARACTERS').isin("E15RI") & f.col('END_5_CHARACTERS').isin("PR75I","PR75O","PR15O","PR23O","PRANO"),"1")\
                                        .otherwise("0"))  

df531 = df542
#"START_CATEGORY","END_CATEGORY","YEARS_BETWEEN", #Missing

df533 = df531.where('END_ACTION_NUMBER > START_ACTION_NUMBER').where('FLAG_POST_REG =1')

w2 = Window.partitionBy("SERIAL_NUMBER","START_5_CHARACTERS","END_5_CHARACTERS","END_ACTION_DATE","END_ACTION_NUMBER").orderBy("SERIAL_NUMBER",f.desc("START_5_CHARACTERS"),f.desc("START_ACTION_NUMBER"),f.desc("END_ACTION_NUMBER"))
df552 = df533.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w2 = Window.partitionBy("SERIAL_NUMBER","END_5_CHARACTERS","START_ACTION_DATE","END_ACTION_DATE","END_ACTION_NUMBER").orderBy("SERIAL_NUMBER",f.desc("START_5_CHARACTERS"),f.desc("START_ACTION_NUMBER"),f.desc("END_ACTION_NUMBER"))
df551 = df552.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w2 = Window.partitionBy("SERIAL_NUMBER","START_5_CHARACTERS","END_5_CHARACTERS","START_ACTION_DATE","START_ACTION_NUMBER").orderBy("SERIAL_NUMBER","START_ACTION_DATE","START_ACTION_NUMBER","END_ACTION_NUMBER")
df550 = df551.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w2 = Window.partitionBy("SERIAL_NUMBER","START_5_CHARACTERS","START_ACTION_DATE","START_ACTION_NUMBER").orderBy("SERIAL_NUMBER","START_ACTION_DATE","START_ACTION_NUMBER","END_ACTION_NUMBER")
df547 = df550.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w2 = Window.partitionBy("SERIAL_NUMBER","END_ACTION_DATE").orderBy("SERIAL_NUMBER",f.desc("START_ACTION_DATE"),f.desc("START_ACTION_NUMBER"),f.desc("END_ACTION_NUMBER"))
df566 = df547.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df543 = df566.selectExpr("Right_15_FLAG","Action_Year","END_5_CHARACTERS","END_ACTION_CODE","END_ACTION_DATE","END_ACTION_NUMBER","END_ACTION_YR","END_CM_DESC","FILING_BASIS_CUR","FILING_BASIS_FIL","FLAG_POST_REG","POSTREG_CATEGORY","PRA_Mailed","REGISTRATION_DT","REGISTRATION_NUMBER","Registration_Year","Right_POSTREG_CATEGORY","Right_REGISTRATION_NUMBER","SERIAL_NUMBER","START_5_CHARACTERS","START_ACTION_CODE","START_ACTION_DATE","START_ACTION_NUMBER","START_ACTION_YR","START_CM_DESC","15_FLAG","Start_End")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan Start Code Transaction lines (~2% Data Flows here)

# COMMAND ----------

df557 = df530.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as START_ACTION_NUMBER","PH_ACTION_CODE as START_ACTION_CODE","PH_ACTION_DATE as START_ACTION_DATE","CM_Desc as START_CM_DESC","5Characters as START_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG")
#df557.display()
w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy(col("SERIAL_NUMBER"),col("START_ACTION_DATE"),col("START_ACTION_NUMBER"))
df560 = df557.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

w = Window.partitionBy("SERIAL_NUMBER").orderBy("SERIAL_NUMBER","START_ACTION_DATE","START_ACTION_NUMBER")
df558 = df560.withColumn("DUPLICATE",f.when((f.col('SERIAL_NUMBER') == f.lead(f.col('SERIAL_NUMBER'),1).over(w)) & (f.datediff(f.lead(f.col('START_ACTION_DATE'),1).over(w),f.col('START_ACTION_DATE')) < 7) ,1).otherwise("0"))

###Add sort in Alteryx
df559 = df558.where('DUPLICATE = 0')

df553 = df559.alias("df559").join(df533.alias("df533"),(col("df559.SERIAL_NUMBER") == col("df533.SERIAL_NUMBER")) & (col("df559.START_ACTION_DATE") == col("df533.START_ACTION_DATE")) ,"anti")

df554_anti = df553.alias("df553").join(df363.alias("df363"),(col("df553.SERIAL_NUMBER") == col("df363.AM_SER_NUM")) ,"anti")

df554_inner = df553.alias("df553").join(df363.alias("df363"),(col("df553.SERIAL_NUMBER") == col("df363.AM_SER_NUM")) ,"inner")

df563 = df554_anti.alias("df554_anti").join(df363_f.alias("df363_f"),(col("df554_anti.SERIAL_NUMBER") == col("df363_f.AM_SER_NUM")) ,"inner").withColumnRenamed('AM_DT_CNCL','END_ACTION_DATE')

df732 = df563.withColumn("INACTIVE",f.lit(1)).withColumn("END_5_CHARACTERS",f.lit("CANCELLED")).withColumn("END_CM_DESC",f.lit("CANCELLED"))

df564 = df732.selectExpr("END_5_CHARACTERS","INACTIVE","DUPLICATE","SERIAL_NUMBER","15_FLAG","Action_Year","Registration_Year","POSTREG_CATEGORY","Start_End","FILING_BASIS_FIL","FILING_BASIS_CUR","START_5_CHARACTERS","START_CM_DESC","START_ACTION_DATE","START_ACTION_CODE","START_ACTION_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","END_CM_DESC", "END_ACTION_DATE")

df561 = df554_inner.withColumn("INVENTORY",f.lit(1))

df555 = df561.selectExpr("FILING_BASIS_CUR","SERIAL_NUMBER","AM_DT_CNCL","AM_SER_NUM","DUPLICATE","15_FLAG","Action_Year","INVENTORY","POSTREG_CATEGORY","Registration_Year","FILING_BASIS_FIL","Start_End","START_CM_DESC","START_ACTION_DATE","START_ACTION_CODE","START_ACTION_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","START_5_CHARACTERS")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handle Orphan End Code Transaction lines (~8% Data Flows here)

# COMMAND ----------

##Rename Variables to match with output data
df540 = df530_f.where('5Characters in ("PR15O", "15AKO")')
df534 = df540.selectExpr("SERIAL_NUMBER","PH_ACTION_NUMBER as END_ACTION_NUMBER","PH_ACTION_CODE as END_ACTION_CODE","PH_ACTION_DATE as END_ACTION_DATE","CM_Desc as END_CM_DESC","5Characters as END_5_CHARACTERS","REGISTRATION_DT","FILING_BASIS_CUR","FILING_BASIS_FIL","REGISTRATION_NUMBER","Start_End","POSTREG_CATEGORY","Registration_Year","Action_Year","15_FLAG")

##Keep one line per event and remove redundant data lines 
w2 = Window.partitionBy("SERIAL_NUMBER","END_ACTION_DATE").orderBy("SERIAL_NUMBER",f.desc("END_ACTION_DATE"),f.desc("END_ACTION_NUMBER"))
df536 = df534.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df544 = df536.alias("df536").join(df543.alias("df543"),(col("df536.SERIAL_NUMBER") == col("df543.SERIAL_NUMBER")) &(col("df536.END_ACTION_DATE") == col("df543.END_ACTION_DATE"))  ,"anti").selectExpr("df536.*")

df545 = df544.unionByName(df543,allowMissingColumns=True).unionByName(df555,allowMissingColumns=True).unionByName(df564,allowMissingColumns=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Get First Action

# COMMAND ----------

##Keep Valid START_END Pairs
df568 = df531.where('END_ACTION_NUMBER > START_ACTION_NUMBER')

##Keep one line per event and remove redundant data lines 
w2 = Window.partitionBy("SERIAL_NUMBER","START_ACTION_DATE").orderBy("SERIAL_NUMBER","START_ACTION_NUMBER","END_ACTION_NUMBER")
df570 = df568.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row") 

df574 = df570.selectExpr("SERIAL_NUMBER","END_5_CHARACTERS as FIRST_ACTION_CODE","START_ACTION_DATE as FA_START_DATE","END_ACTION_DATE as FIRST_ACTION_DATE","START_ACTION_NUMBER as FA_START_NUMBER","END_ACTION_NUMBER as FIRST_ACTION_NUMBER","END_CM_DESC as FIRST_ACTION_DESC")

##Stack Orphan End code lines with the Final Output Data of this Section
df577_anti = df545.alias("df545").join(df574.alias("df574"),(col("df545.SERIAL_NUMBER") == col("df574.SERIAL_NUMBER"))&(col("df545.START_ACTION_DATE") == col("df574.FA_START_DATE"))  ,"anti").selectExpr("df545.START_ACTION_CODE","df545.START_5_CHARACTERS","df545.START_ACTION_NUMBER","df545.POSTREG_CATEGORY","df545.Right_POSTREG_CATEGORY","df545.Right_15_FLAG","df545.Registration_Year","df545.START_ACTION_YR","df545.START_CM_DESC","df545.REGISTRATION_NUMBER","df545.REGISTRATION_DT","df545.PRA_Mailed","df545.START_ACTION_DATE","df545.Right_REGISTRATION_NUMBER","df545.FLAG_POST_REG","df545.Start_End","df545.Action_Year","df545.AM_DT_CNCL","df545.AM_SER_NUM","df545.DUPLICATE","df545.END_5_CHARACTERS","df545.END_ACTION_CODE","df545.END_ACTION_DATE","df545.INACTIVE","df545.END_ACTION_NUMBER","df545.END_CM_DESC","df545.END_ACTION_YR","df545.SERIAL_NUMBER","df545.INVENTORY","df545.15_FLAG","df545.FILING_BASIS_FIL","df545.FILING_BASIS_CUR")


df577_inner = df545.alias("df545").join(df574.alias("df574"),(col("df545.SERIAL_NUMBER") == col("df574.SERIAL_NUMBER"))&(col("df545.START_ACTION_DATE") == col("df574.FA_START_DATE"))  ,"inner").selectExpr("df545.START_ACTION_CODE","df545.START_5_CHARACTERS","df545.START_ACTION_NUMBER","df545.POSTREG_CATEGORY","df545.Right_POSTREG_CATEGORY","df545.Right_15_FLAG","df545.Registration_Year","df545.START_ACTION_YR","df545.START_CM_DESC","df545.REGISTRATION_NUMBER","df545.REGISTRATION_DT","df545.PRA_Mailed","df545.START_ACTION_DATE","df545.Right_REGISTRATION_NUMBER","df545.FLAG_POST_REG","df545.Start_End","df545.Action_Year","df545.AM_DT_CNCL","df545.AM_SER_NUM","df545.DUPLICATE","df545.END_5_CHARACTERS","df545.END_ACTION_CODE","df545.END_ACTION_DATE","df545.INACTIVE","df545.END_ACTION_NUMBER","df545.END_CM_DESC","df545.END_ACTION_YR","df545.SERIAL_NUMBER","df574.FA_START_DATE","df574.FA_START_NUMBER","df574.FIRST_ACTION_CODE","df574.FIRST_ACTION_DATE","df574.FIRST_ACTION_DESC","df574.FIRST_ACTION_NUMBER")

df577 = df577_anti.unionByName(df577_inner,allowMissingColumns=True)

# spark.sparkContext.setCheckpointDir(CHK_POINT_DIR + "_df577")
# df577 = df577.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##STACK ALL POST REG EVENTS, DERIVE ADDITIONAL DATA FIELDS AND OUTPUT FILES

# COMMAND ----------

df660 = df526.selectExpr("FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","Left_Right_PostReg_Category","SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","PostReg_Category","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","Right_Right_PostReg_Category","END_ACTION_DATE","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","FILING_BASIS_CUR","FILING_BASIS_FIL","15_FLAG","START_5_CHARACTERS")

df666= df473.selectExpr("SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","FILING_BASIS_CUR","FILING_BASIS_FIL","15_FLAG","INVENTORY","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","RENEWAL_DT","Renewal_Number","`Renewal Number Updated`","Right_FIRST_ACTION_CODE","Right_FA_START_DATE","Right_FIRST_ACTION_DATE","Right_FA_START_NUMBER","Right_FIRST_ACTION_NUMBER","Right_FIRST_ACTION_DESC")

df662 = df636.selectExpr("FIRST_ACTION_NUMBER","FA_START_NUMBER","FIRST_ACTION_DATE","FA_START_DATE","FIRST_ACTION_CODE","Right_INVENTORY","Right_DUPLICATE","Right_Action_Year","Right_Start_End","Right_FILING_BASIS_FIL","Right_FILING_BASIS_CUR","Right_START_CM_DESC","Right_START_ACTION_DATE","INACTIVE","Right_START_ACTION_CODE","Right_START_ACTION_NUMBER","Right_REGISTRATION_DT","Right_Registration_Year","Right_START_5_CHARACTERS","SERIAL_NUMBER","INVENTORY","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","FILING_BASIS_CUR","FILING_BASIS_FIL","15_FLAG","FIRST_ACTION_DESC")

df664 = df577.selectExpr("INACTIVE","AM_SER_NUM","AM_DT_CNCL","FIRST_ACTION_CODE","FA_START_DATE","FIRST_ACTION_DATE","FA_START_NUMBER","FIRST_ACTION_NUMBER","SERIAL_NUMBER","REGISTRATION_DT","POSTREG_CATEGORY","START_ACTION_NUMBER","REGISTRATION_NUMBER","START_ACTION_DATE","END_ACTION_NUMBER","START_CM_DESC","END_5_CHARACTERS","START_5_CHARACTERS","END_ACTION_DATE","END_CM_DESC","FIRST_ACTION_DESC","INVENTORY","15_FLAG","FILING_BASIS_FIL","FILING_BASIS_CUR")

df640 = df666.unionByName(df664, allowMissingColumns=True)\
             .unionByName(df662, allowMissingColumns=True)\
             .unionByName(df660, allowMissingColumns=True)

df638 = df640.withColumn("15_FLAG",f.when((f.col('START_5_CHARACTERS').isin("15AFI","715FI","815FI","E15RI","E815I","ES75I")) | (f.col('END_5_CHARACTERS').isin("15AKO","C15AO","C15PO","C75AO","C75PO","NA75E","NA75O","NA85E","NA85O","PR15O","PR23O","PR75O")) ,"1"))

df639 = df638.selectExpr("SERIAL_NUMBER","Right_START_CM_DESC","Right_START_ACTION_DATE","Right_START_ACTION_NUMBER","Right_START_5_CHARACTERS","Right_REGISTRATION_DT","INACTIVE","Right_Right_PostReg_Category","Right_START_ACTION_CODE","Right_FILING_BASIS_CUR","Right_Action_Year","Right_Start_End","Right_Registration_Year","Right_DUPLICATE","Right_INVENTORY","FA_START_DATE","FA_START_NUMBER","FIRST_ACTION_NUMBER","AM_SER_NUM","AM_DT_CNCL","Right_FILING_BASIS_FIL","Left_Right_PostReg_Category","Right_FIRST_ACTION_DESC","Right_FIRST_ACTION_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","15_FLAG","INVENTORY","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","RENEWAL_DT","Renewal_Number","`Renewal Number Updated`","Right_FIRST_ACTION_CODE","Right_FA_START_DATE","Right_FIRST_ACTION_DATE","Right_FA_START_NUMBER")

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df639")
df639 = df639.checkpoint(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###REMOVE 6 YEAR ERRONEOUS 

# COMMAND ----------

df639 = df639.withColumn("END_ACTION_DATE", f.col("END_ACTION_DATE").cast(DateType()))
df650 = df639.where(("IsNotNull(END_ACTION_DATE)")).where("POSTREG_CATEGORY = '6 YEAR'")
df642 = df639.where(("IsNull(END_ACTION_DATE)")).where("POSTREG_CATEGORY = '6 YEAR'")

df641 = df639.where(("POSTREG_CATEGORY = '10 YEAR'"))

df651 = df639.where(("POSTREG_CATEGORY != '6 YEAR'"))

df645 = df642.selectExpr("SERIAL_NUMBER","START_ACTION_DATE as 6_YR_START_DT","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","`Renewal Number Updated`","Right_FIRST_ACTION_CODE as Left_Right_FIRST_ACTION_CODE","Right_FA_START_DATE","Right_FIRST_ACTION_DATE","Right_FA_START_NUMBER","Right_FIRST_ACTION_NUMBER","Right_FIRST_ACTION_DESC","'' as Right_Right_15_FLAG","Left_Right_PostReg_Category","Right_Right_PostReg_Category","INACTIVE","Right_REGISTRATION_DT","Right_START_ACTION_NUMBER","Right_START_ACTION_CODE","Right_START_ACTION_DATE","Right_START_CM_DESC","Right_START_5_CHARACTERS","Right_FILING_BASIS_CUR","Right_FILING_BASIS_FIL","Right_Start_End","Right_Registration_Year","Right_Action_Year","Right_DUPLICATE","Right_INVENTORY","FA_START_DATE","FA_START_NUMBER","FIRST_ACTION_NUMBER","AM_SER_NUM","AM_DT_CNCL")

df644 = df641.selectExpr("START_ACTION_DATE as 10_YR_START_DT","FIRST_ACTION_CODE as Right_FIRST_ACTION_CODE","Right_FIRST_ACTION_CODE as Right_Right_FIRST_ACTION_CODE","Right_FA_START_DATE as Right_Right_FA_START_DATE","Right_FIRST_ACTION_DATE as	Right_Right_FIRST_ACTION_DATE","Right_FA_START_NUMBER as Right_Right_FA_START_NUMBER","Right_FIRST_ACTION_NUMBER	as Right_Right_FIRST_ACTION_NUMBER","Right_FIRST_ACTION_DESC as	Right_Right_FIRST_ACTION_DESC","'' as 	Right_Right_Right_15_FLAG","Left_Right_PostReg_Category as 	Right_Left_Right_PostReg_Category","Right_Right_PostReg_Category as	Right_Right_Right_PostReg_Category","Right_START_ACTION_DATE as	Right_Right_START_ACTION_DATE","Right_START_CM_DESC	as Right_Right_START_CM_DESC","SERIAL_NUMBER as RIGHT_SERIAL_NUMBER")

df646 = df645.join(df644,df645.SERIAL_NUMBER==df644.RIGHT_SERIAL_NUMBER,"inner").drop("RIGHT_SERIAL_NUMBER")

df643 = df646.withColumn("ERRONEOUS",f.when(f.floor(f.abs(f.months_between("6_YR_START_DT","10_YR_START_DT"))/12) <= 2,"1").otherwise("0"))\
    .withColumn("DIFF_6YR_10YR",f.abs((f.months_between("6_YR_START_DT","10_YR_START_DT"))/12))\
        .withColumn("DIFF_10YR_6YR",f.abs((f.months_between("10_YR_START_DT","6_YR_START_DT"))/12))

df648 = df643.where('ERRONEOUS = 1')

df649 = df642.alias("df642").join(df648.alias("df648"),(col("df642.SERIAL_NUMBER")==col("df648.SERIAL_NUMBER")) & (col("df642.START_ACTION_DATE")==col("df648.6_YR_START_DT")), "anti" )


df654 = df650.unionByName(df649,allowMissingColumns=True)
df652 = df654.unionByName(df651,allowMissingColumns=True)

df653 = df652

df658 = df653.select("SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","15_FLAG","INVENTORY","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","RENEWAL_DT","Renewal_Number","`Renewal Number Updated`","INACTIVE","Right_REGISTRATION_DT","Right_START_ACTION_NUMBER","Right_START_ACTION_CODE","Right_START_ACTION_DATE","Right_START_CM_DESC","Right_START_5_CHARACTERS","Right_FILING_BASIS_CUR","Right_FILING_BASIS_FIL","Right_Start_End","Right_Registration_Year","Right_Action_Year","Right_DUPLICATE","Right_INVENTORY","FA_START_DATE","FA_START_NUMBER","FIRST_ACTION_NUMBER","AM_SER_NUM","AM_DT_CNCL").distinct()

df656 = df658.selectExpr("AM_SER_NUM","AM_DT_CNCL","SERIAL_NUMBER","Renewal_Number","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","15_FLAG","INVENTORY","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","RENEWAL_DT","`Renewal Number Updated` as Renewal_Number_Updated")

# COMMAND ----------

# MAGIC %md
# MAGIC ###(1) ADD Employee ID (PRCD NUM Length=5 ) From PH Table;  (2) Create Unique Transaction ID; (3) Redefine Inventory Flag

# COMMAND ----------

df750 = df355.where("not IsNull(TM_WORKER_EID)")
df751 = df750.where(~f.col("CM_Desc").contains("ASSIGNED TO"))
df753 = df751.where('5TH_CHAR_CM_TYPE in ("E", "O", "R", "Q")')
df754 = df753.where(f.col("5Characters").isin("8.OKO","8.PRO","C15AO","C15PO","NA85O","NA85E","PRA8O","PR23O","PRANO","PR89O","89AGO","PRA8O","8OKTO","8PRTO","PRA9O","9G8PO","NA71O","NA71E","NA75O","NA75E","71AGO","7.PRO","A7OKO","C.7CO","C7..O","C7P.O","COC.O","PRA7O","PRAMO","PR15O","15AKO","PR75O","PR23O")|f.col("5Characters").contains("RNL")|f.col("5Characters").contains("REN") ) 
df752 = df754.select("SERIAL_NUMBER","5Characters","PH_ACTION_DATE","TM_WORKER_EID").distinct()

df758 = df753.where('5Characters in ("C75PO", "71.PO", "C75AO")')

df759 = df758.select("SERIAL_NUMBER","5Characters","PH_ACTION_DATE","TM_WORKER_EID").distinct()

df759.createOrReplaceTempView("post_reg_df759")

df755_anti = df656.alias("df656").join(df752.alias("df752"),(col("df656.SERIAL_NUMBER")==col("df752.SERIAL_NUMBER")) & (col("df656.END_ACTION_DATE")==col("df752.PH_ACTION_DATE")) &(col("df656.END_5_CHARACTERS")==col("df752.5Characters")), "anti" )

df755_anti.createOrReplaceTempView("post_reg_df755_anti")

df755_inner = df656.alias("df656").join(df752.alias("df752"),(col("df656.SERIAL_NUMBER")==col("df752.SERIAL_NUMBER")) & (col("df656.END_ACTION_DATE")==col("df752.PH_ACTION_DATE")) &(col("df656.END_5_CHARACTERS")==col("df752.5Characters")), "inner" ).selectExpr("15_FLAG","AM_DT_CNCL","AM_SER_NUM","END_5_CHARACTERS","END_ACTION_DATE","END_ACTION_NUMBER","END_CM_DESC","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","INVENTORY","POSTREG_CATEGORY","REGISTRATION_DT","REGISTRATION_NUMBER","Renewal_Number_Updated","RENEWAL_DT","df656.SERIAL_NUMBER","START_5_CHARACTERS","START_ACTION_DATE","START_ACTION_NUMBER","START_CM_DESC","TM_WORKER_EID")

df756 = df755_inner.dropDuplicates(["SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE"])

# COMMAND ----------

# DBTITLE 1,Debug here for tm_worker_eid
df760_anti = spark.sql(f"""SELECT df755_anti.SERIAL_NUMBER,df755_anti.REGISTRATION_DT,df755_anti.REGISTRATION_NUMBER,df755_anti.POSTREG_CATEGORY,df755_anti.START_ACTION_NUMBER,df755_anti.END_ACTION_NUMBER,df755_anti.START_ACTION_DATE,df755_anti.END_ACTION_DATE,df755_anti.START_5_CHARACTERS,df755_anti.END_5_CHARACTERS,df755_anti.START_CM_DESC,df755_anti.END_CM_DESC,df755_anti.15_FLAG,df755_anti.INVENTORY,df755_anti.FIRST_ACTION_CODE,df755_anti.FIRST_ACTION_DATE,df755_anti.FIRST_ACTION_DESC,df755_anti.RENEWAL_DT,df755_anti.Renewal_Number_Updated,df755_anti.AM_SER_NUM,df755_anti.AM_DT_CNCL,null as TM_WORKER_EID
FROM post_reg_df755_anti df755_anti LEFT ANTI JOIN
post_reg_df759 df759
ON df755_anti.SERIAL_NUMBER = df759.SERIAL_NUMBER
and df755_anti.END_ACTION_DATE = df759.PH_ACTION_DATE""")

df760_inner = spark.sql(f"""SELECT df755_anti.SERIAL_NUMBER,df755_anti.REGISTRATION_DT,df755_anti.REGISTRATION_NUMBER,df755_anti.POSTREG_CATEGORY,df755_anti.START_ACTION_NUMBER,df755_anti.END_ACTION_NUMBER,df755_anti.START_ACTION_DATE,df755_anti.END_ACTION_DATE,df755_anti.START_5_CHARACTERS,df755_anti.END_5_CHARACTERS,df755_anti.START_CM_DESC,df755_anti.END_CM_DESC,df755_anti.15_FLAG,df755_anti.INVENTORY,df755_anti.FIRST_ACTION_CODE,df755_anti.FIRST_ACTION_DATE,df755_anti.FIRST_ACTION_DESC,df755_anti.RENEWAL_DT,df755_anti.Renewal_Number_Updated,df755_anti.AM_SER_NUM,df755_anti.AM_DT_CNCL,df759.TM_WORKER_EID
FROM post_reg_df755_anti df755_anti INNER JOIN
post_reg_df759 df759
ON df755_anti.SERIAL_NUMBER = df759.SERIAL_NUMBER
and df755_anti.END_ACTION_DATE = df759.PH_ACTION_DATE""")

df761 = df760_inner.dropDuplicates(["SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE"])

df743 = df760_anti.unionByName(df761,allowMissingColumns=True).unionByName(df756,allowMissingColumns=True)

df744 = df743.withColumn("START_NUM",f.when(f.col('START_ACTION_NUMBER').isNull(),"X").otherwise(f.col("START_ACTION_NUMBER")))\
        .withColumn("END_NUM",f.when(f.col('END_ACTION_NUMBER').isNull(),"X").otherwise(f.col("END_ACTION_NUMBER")))\
            .withColumn("Unique_Transaction_ID",f.concat(f.col("SERIAL_NUMBER"),f.lit("-"),f.col("START_NUM"),f.lit("-"),f.col("END_NUM")))

df747 = df744.selectExpr("15_FLAG","TM_WORKER_EID","START_CM_DESC","START_ACTION_NUMBER","START_ACTION_DATE","START_5_CHARACTERS","SERIAL_NUMBER","RENEWAL_DT","Renewal_Number_Updated","REGISTRATION_NUMBER","POSTREG_CATEGORY","Unique_Transaction_ID","INVENTORY","FIRST_ACTION_DESC","FIRST_ACTION_DATE","FIRST_ACTION_CODE","END_CM_DESC","END_ACTION_NUMBER","END_ACTION_DATE","END_5_CHARACTERS","REGISTRATION_DT")


# COMMAND ----------

# MAGIC %md
# MAGIC ###INVENTORY; FIRST ACTION - INVENTORY, DATE, CODE, PENDENCY

# COMMAND ----------

df763 = df747.withColumn("INVENTORY",f.when((f.col('END_5_CHARACTERS').isNull()) ,"1").otherwise(0))
df766 = df763.where('INVENTORY != 0')

df766_f = df763.where('INVENTORY = 0').selectExpr("SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","15_FLAG","INVENTORY","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","RENEWAL_DT","Renewal_Number_Updated","TM_WORKER_EID","Unique_Transaction_ID")

df773 = df696.where(f.col('Expiration_DT').isNotNull())

df765_anti = df766.alias("df766").join(df773.alias("df773"),(col("df766.SERIAL_NUMBER") == col("df773.SERIAL_NUMBER")) ,"anti").selectExpr("SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","15_FLAG","INVENTORY","FIRST_ACTION_CODE","FIRST_ACTION_DATE","FIRST_ACTION_DESC","RENEWAL_DT","Renewal_Number_Updated","TM_WORKER_EID","Unique_Transaction_ID")

df765_inner = df766.alias("df766").join(df773.alias("df773"),(f.col("df766.SERIAL_NUMBER") == f.col("df773.SERIAL_NUMBER")) ,"inner").select(f.col("df766.*"), f.col('df773.Expiration_DT').alias("Expiration_DT"))

df767 = df765_inner.withColumn("END_5_CHARACTERS",f.lit("CANCELLED")).withColumn("END_ACTION_DATE",f.col('Expiration_DT'))

df769 = df765_anti.unionByName(df767,allowMissingColumns=True).unionByName(df766_f,allowMissingColumns=True)

# Step 1 — derive INVENTORY
df776_a = df769.withColumn(
    "INVENTORY",
    f.when(f.col("END_5_CHARACTERS").isNull(), 1).otherwise(0)
)

# Step 2 — derive all columns that depend on the new INVENTORY value
df776 = (
    df776_a
    .withColumn(
        "FIRST_ACTION_INVENTORY",
        f.when(
            f.col("FIRST_ACTION_CODE").isNotNull() & (f.col("INVENTORY") == 1),
            0
        ).otherwise(f.col("INVENTORY"))   # correctly reads the new INVENTORY
    )
    .withColumn(
        "FIRST_ACTION_CODE",
        f.when(
            f.col("FIRST_ACTION_CODE").isNull(),
            f.col("END_5_CHARACTERS")
        ).otherwise(f.col("FIRST_ACTION_CODE"))
    )
    .withColumn(
        "FIRST_ACTION_DATE",
        f.when(
            f.col("FIRST_ACTION_DATE").isNull(),
            f.col("END_ACTION_DATE")
        ).otherwise(f.col("FIRST_ACTION_DATE"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###First Action and Total Pendency Calculation

# COMMAND ----------

df677 = df776.withColumn("FA_START_DATE",f.col('START_ACTION_DATE'))\
    .withColumn("FA_END_DATE",f.col('FIRST_ACTION_DATE'))
df677 = df677.withColumn("FA_END_DATE", f.col("FA_END_DATE").cast(DateType())).withColumn("FA_START_DATE", f.date_format(f.col("FA_START_DATE"),"yyyy-MM-dd").cast(DateType()))
df670 = df677.where(f.col('FA_END_DATE')> f.col('FA_START_DATE'))

df670_f = df677.where(f.col("FA_END_DATE").isNull() |  f.col("FA_START_DATE").isNull()|(f.col('FA_END_DATE')<= f.col('FA_START_DATE'))|(f.col('FA_END_DATE')== f.col('FA_START_DATE')) )

df738 = df670_f.where(f.col('FA_END_DATE') == f.col('FA_START_DATE'))

df738_f = df670_f.where(f.col("FA_END_DATE").isNull() |  f.col("FA_START_DATE").isNull() |(f.col('FA_END_DATE') != f.col('FA_START_DATE')))

df739 = df738.withColumn("Count",f.lit('0'))
df779 = df738_f.withColumn("Count",f.lit(None))

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType

df669 = spark.read.format("csv").option("header", True).load(f"s3://{cdc_bucket}/eds/static_files/holidays.csv").cache()
df669.count()
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
df669 = df669.select("Subject",func("Start_Date").alias("Start_Date"))
df668 = df670.withColumn('exploded', f.explode(f.sequence(f.to_date('FA_START_DATE'), f.to_date('FA_END_DATE'))))
df668 = df668.filter(~f.dayofweek('exploded').isin([1, 7]))  # Remove both Sunday and Saturday
df668 = df668.join(f.broadcast(df669), df668.exploded == df669.Start_Date, 'anti')

df668 = df668.groupBy('SERIAL_NUMBER', 'REGISTRATION_DT', 'REGISTRATION_NUMBER', 'POSTREG_CATEGORY', 'START_ACTION_NUMBER', 'END_ACTION_NUMBER', 'START_ACTION_DATE', 'END_ACTION_DATE', 'START_5_CHARACTERS', 'END_5_CHARACTERS', 'START_CM_DESC', 'END_CM_DESC', '15_FLAG', 'INVENTORY', 'FIRST_ACTION_CODE', 'FIRST_ACTION_DATE', 'FIRST_ACTION_DESC', 'RENEWAL_DT', 'Renewal_Number_Updated', 'TM_WORKER_EID', 'Unique_Transaction_ID', 'FIRST_ACTION_INVENTORY', 'FA_START_DATE', 'FA_END_DATE').agg(f.count('exploded').alias('Count'))

df674 = df668.unionByName(df739,allowMissingColumns=True).unionByName(df779,allowMissingColumns=True)

# COMMAND ----------

df673 = df674.where(f.col('END_ACTION_DATE')>= f.col('START_ACTION_DATE'))

df673_f = df674.where(f.col("END_ACTION_DATE").isNull() |  f.col("START_ACTION_DATE").isNull() |(f.col('END_ACTION_DATE')< f.col('START_ACTION_DATE')))

df671 = df673.withColumn("TOTAL_PENDENCY",f.datediff(col('END_ACTION_DATE'),col('START_ACTION_DATE')))

df675 = df671.unionByName(df673_f,allowMissingColumns=True)

df676 = df675.selectExpr("SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","RENEWAL_DT","Renewal_Number_Updated as RENEWAL_NUMBER","15_FLAG as fifteen_flag","INVENTORY","FIRST_ACTION_DATE","FIRST_ACTION_CODE","Count as FIRST_ACTION_PENDENCY","FIRST_ACTION_INVENTORY","TOTAL_PENDENCY","TM_WORKER_EID","Unique_Transaction_ID as UNIQUE_TRANSACTION_ID").distinct()
df676 = df676.withColumn("create_ts", current_timestamp())\
                .withColumn("create_user_id", f.lit("-1"))\
                .withColumn("update_ts", current_timestamp())\
                .withColumn("update_user_id", f.lit("-1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Overwrite Table2: post_reg_detail

# COMMAND ----------

try:
    df676.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.silver.post_reg_detail')
    recs_count = df676.count()
    end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.fs.rm(CHK_POINT_DIR,True)
    dbutils.notebook.exit(f"Completed Loading post_reg_detail Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    dbutils.fs.rm(CHK_POINT_DIR,True)
    raise
dbutils.notebook.exit(f"Completed loading second level post_reg_detail Table ")


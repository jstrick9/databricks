# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##Overview
# MAGIC
# MAGIC This notebook will gives us the overview for Fixed Filing Count ETL. Which contain Input and output dataframes. Subsequent Notebook will provide the psudo code.

# COMMAND ----------

#review comment: refer common ntbks by relative path and move them to repo

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input's 

# COMMAND ----------

# MAGIC %run ./ntb_fixed_filings_count_input $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
print(reporting_catalog)

# COMMAND ----------

# DBTITLE 1,Start job control
job_name = 'ntb_second_level_fixed_filings_count_etl_code'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #First flow #2

# COMMAND ----------

ip1_df = ip1_df.withColumn(
    "unit_qt", col("unit_qt").astype(IntegerType())
)

# COMMAND ----------

# input 1 - step 1 - filter out data on values of REV_SRC_CD
fee_codes_lst = ["6001", "7001", "7007", "7009", "7931", "7933", "7017"]
ip1_df_filter = ip1_df.filter(col("REV_SRC_CD").isin(fee_codes_lst))

# COMMAND ----------

#ip1_df.show()

# COMMAND ----------

# input 1 - step 2 - formula to create new column PRJCT_CD
#-----------------------DISCUSS-------------------------
#column with same name is already present in the dataframe so renamed to PRJCT_CD1
ip1_df_formula = ip1_df_filter.select(col("ACCTG_DT"),
                                      col("PSTNG_REF_TX"),
                                      when(ip1_df_filter.PRJCT_CD.rlike("^[a-zA-Z.]*$"),ip1_df_filter.PSTNG_REF_TX)
                                      .otherwise(ip1_df_filter.PRJCT_CD).alias("PRJCT_CD"),
                         col("MAILROOM_DT"),
col("REV_SRC_CD"),
col("FEE_AM"),
col("UNIT_QT"),
col("TRAN_AM"),
col("DOC_STATUS_CD"),
col("TRAN_STATUS_CD"),
col("DOC_CLSFCN_CD"),
col("TRAN_PSTNG_REF_TX")
                         
                         )

# COMMAND ----------

#display(ip1_df_formula)

# COMMAND ----------

## Input 4 step1 create two different DFs based on values of filing_basis_fil

ip4_madrid = ip4_df.filter(col("filing_basis_fil") == "MADRID")

ip4_madrid = ip4_madrid.withColumnRenamed("SER_NUM","Right_SER_NUM")

ip4_non_madrid = ip4_df.filter(col("filing_basis_fil") != "MADRID")

ip4_non_madrid = ip4_non_madrid.withColumnRenamed("SER_NUM","Right_SER_NUM")\
                                .withColumnRenamed("AM_FLG_66A_FIL","Right_AM_FLG_66A_FIL")\
                                    .withColumnRenamed("LAST_MODIFIED_DATE","Right_LAST_MODIFIED_DATE")


# COMMAND ----------

#ip4_non_madrid.show()

# COMMAND ----------

#ip3_df
#ip4_non_madrid
# output DF of this join is used in join of both IP1 and IP2 dataframes

joined_ip3_ip4_nMadrid_df = \
(
    ip3_df
        .join(ip4_non_madrid,
             on = [(col("SER_NUM") == col("Right_SER_NUM"))],
             how = "inner"
             )
        .select(col("ser_num"),
                col("first_action_dt_ph"),
col("am_1_actn_ct_dt"),
col("first_action_type"),
col("filing_dt"),
col("ib_notification_dt"),
col("published_dt"),
col("noa_dt"),
col("abandonment_dt"),
col("aban_dt_ph"),
col("registration_dt"),
col("disposal_type"),
col("ext1_dt"),
col("ext2_dt"),
col("ext3_dt"),
col("ext4_dt"),
col("ext5_dt"),
col("cancellation_dt"),
col("renewal_dt"),
col("revival_dt"),
col("susp_check_dt"),
col("am_cls_ct_actv"),
col("pendency_cal_start_dt"),
col("pendency_cal_end_dt"),
col("noa_registration_check"),
col("wgtd_1st_actn_pendency"),
col("first_action_cd"),
col("disposal_pendency"),
col("suspension"),
col("ttab"),
col("disposal_dt"),
col("dock_dt"),
col("am_flg_66a_cur"),
col("am_flg_66a_fil"),
col("noa_dt_ph"),
col("filing_fy"),
col("non_pro_se"),
col("first_action_pendency_ph"),
col("last_modified_date"),
col("Right_SER_NUM"),
col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),
col("filing_method_filed"),
col("filing_method_cur"),
col("filing_basis_fil"),
col("filing_basis_amed"),
col("registration_number"),
col("Right_AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),
col("am_stat"),
col("am_flg_no_bas_fil"),
col("am_flg_teasrf_fil"),
col("am_flg_use_fil"),
col("am_flg_itu_fil"),
col("am_flg_teaspl_fil"),
col("Right_LAST_MODIFIED_DATE"),
col("filing_basis_grp"),
col("mark_dwg_cd"),
col("mark_dwg_desc"),
col("mark_nm_short"),
col("mark_nm"),
col("tmng_image_link"),
col("tm_analytics_ts"),
col("exmr_eid")
                )
)   


# COMMAND ----------

#joined_ip3_ip4_nMadrid_df.show()

# COMMAND ----------

#ip1_df_formula
#joined_ip3_ip4_nMadrid_df

joined_ip134_nMadrid_df = \
(
    ip1_df_formula
        .join(joined_ip3_ip4_nMadrid_df,
             on = [(col("PRJCT_CD") == col("ser_num"))],
             how = "inner"
             )
        .select(col("ACCTG_DT"),
col("PSTNG_REF_TX"),
col("PRJCT_CD"),
col("MAILROOM_DT"),
col("REV_SRC_CD"),
col("FEE_AM"),
col("UNIT_QT"),
col("TRAN_AM"),
col("DOC_STATUS_CD"),
col("TRAN_STATUS_CD"),
col("DOC_CLSFCN_CD"),
col("TRAN_PSTNG_REF_TX"),
col("ser_num"),
col("first_action_dt_ph"),
col("am_1_actn_ct_dt"),
col("first_action_type"),
col("filing_dt"),
col("ib_notification_dt"),
col("published_dt"),
col("noa_dt"),
col("abandonment_dt"),
col("aban_dt_ph"),
col("registration_dt"),
#col("disposal_type"),
#col("ext1_dt"),
#col("ext2_dt"),
#col("ext3_dt"),
#col("ext4_dt"),
#col("ext5_dt"),
col("cancellation_dt"),
col("renewal_dt"),
col("revival_dt"),
col("susp_check_dt"),
col("am_cls_ct_actv"),
col("pendency_cal_start_dt"),
col("pendency_cal_end_dt"),
col("noa_registration_check"),
#col("wgtd_1st_actn_pendency"),
col("first_action_cd"),
col("disposal_pendency"),
#col("suspension"),
#col("ttab"),
col("disposal_dt"),
col("dock_dt"),
#col("am_flg_66a_cur"),
#col("am_flg_66a_fil"),
#col("noa_dt_ph"),
col("filing_fy"),
#col("non_pro_se"),
col("first_action_pendency_ph"),
#col("last_modified_date"),
#col("Right_SER_NUM").alias("Right_Right_SER_NUM"),
col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),
#col("filing_method_filed"),
#col("filing_method_cur"),
col("filing_basis_fil"),
#col("filing_basis_amed"),
#col("registration_number"),
col("Right_AM_FLG_66A_FIL").alias("Right_Right_AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),
col("am_stat"),
#col("am_flg_no_bas_fil"),
#col("am_flg_teasrf_fil"),
#col("am_flg_use_fil"),
#col("am_flg_itu_fil"),
#col("am_flg_teaspl_fil"),
col("Right_LAST_MODIFIED_DATE").alias("Right_Right_LAST_MODIFIED_DATE"),
col("filing_basis_grp")
#col("mark_dwg_cd"),
#col("mark_dwg_desc"),
#col("mark_nm_short"),
#col("mark_nm"),
#col("tmng_image_link"),
#col("tm_analytics_ts"),
#col("exmr_eid")
)
)   


# COMMAND ----------

#joined_ip134_nMadrid_df.show()

# COMMAND ----------

#Ip134 NonMadrid step 1 
# formula
ip134_non_mad_formula = joined_ip134_nMadrid_df.withColumn("DAYS_BTW_POSTED_AND_PEND_START_DT",datediff(col("ACCTG_DT").cast(DateType()),col("Pendency_Cal_Start_DT")))


# COMMAND ----------

#display(ip134_non_mad_formula)

# COMMAND ----------


ip134_non_mad_formula1 = ip134_non_mad_formula.select(col("*"),
                                   when((ip134_non_mad_formula.filing_basis_fil != "MADRID") & (ip134_non_mad_formula.DAYS_BTW_POSTED_AND_PEND_START_DT <= 6),1)
                                   .otherwise(0).alias("FEE_FLAG"),
                                   when((ip134_non_mad_formula.registration_dt.isNull()) | (ip134_non_mad_formula.ACCTG_DT < ip134_non_mad_formula.registration_dt),1)
                                   .otherwise(0).alias("Registration_Flag")
)


# COMMAND ----------

#Ip134 NonMadrid step 2
# filter to create two DFs
#1) on Registration Flag = 1 and Filing_FY >= 2010 and [FEE_FLAG] = 1
# 2) on Registration Flag = 1 and Filing_FY >= 2010 and [TRAN_AM] < 0 AND [FEE_FLAG] = 0

ip134_non_mad_filter1 = ip134_non_mad_formula1.filter(
    (col("Registration_Flag") == 1) & 
    (col("filing_fy") >= "2010") &
    (col("FEE_FLAG") == 1))

ip134_non_mad_filter2 = ip134_non_mad_formula1.filter(
    (col("Registration_Flag") == 1) & 
    (col("filing_fy") >= "2010") &
    (col("TRAN_AM") < 0) & 
    (col("FEE_FLAG") == 0))

# COMMAND ----------

#Ip134 NonMadrid step 3
# Summarize grouping the data on PRJCT_CD

ip134_non_mad_F2_grouped = ip134_non_mad_filter2.select("PRJCT_CD").distinct()

# COMMAND ----------

#ip134_non_mad_formula1.show()

# COMMAND ----------

#Ip134 NonMadrid step 4
# add new column as credit_flag
ip134_non_mad_F2_formula = ip134_non_mad_F2_grouped.withColumnRenamed("PRJCT_CD","PRJCT_CD1").withColumn("Credit_flag",lit("1"))

# COMMAND ----------

#ip134_non_mad_F2_formula.show()

# COMMAND ----------

#Ip134 NonMadrid step 5
# find and replace as join
#ip134_non_mad_filter1
#ip134_non_mad_F2_formula

joined_ip134_nMad_df = \
(
    ip134_non_mad_filter1
        .join(ip134_non_mad_F2_formula,
             on = [(col("PRJCT_CD") == col("PRJCT_CD1"))],
             how = "left" #########Review comment: this should be full join
             )
        .select(
            col("ACCTG_DT"),
col("PSTNG_REF_TX"),
col("PRJCT_CD"),
col("MAILROOM_DT"),
col("REV_SRC_CD"),
col("FEE_AM"),
col("UNIT_QT"),
col("TRAN_AM"),
col("DOC_STATUS_CD"),
col("TRAN_STATUS_CD"),
col("DOC_CLSFCN_CD"),
col("TRAN_PSTNG_REF_TX"),
col("ser_num"),
col("first_action_dt_ph"),
col("am_1_actn_ct_dt"),
col("first_action_type"),
col("filing_dt"),
col("ib_notification_dt"),
col("published_dt"),
col("noa_dt"),
col("abandonment_dt"),
col("aban_dt_ph"),
col("registration_dt"),
#col("disposal_type"),
#col("ext1_dt"),
#col("ext2_dt"),
#col("ext3_dt"),
#col("ext4_dt"),
#col("ext5_dt"),
col("cancellation_dt"),
col("renewal_dt"),
col("revival_dt"),
col("susp_check_dt"),
col("am_cls_ct_actv"),
col("pendency_cal_start_dt"),
col("pendency_cal_end_dt"),
col("noa_registration_check"),
##col("wgtd_1st_actn_pendency"),
col("first_action_cd"),
col("disposal_pendency"),
#col("suspension"),
#col("ttab"),
col("disposal_dt"),
col("dock_dt"),
#col("am_flg_66a_cur"),
#col("am_flg_66a_fil"),
#col("noa_dt_ph"),
col("filing_fy"),
#col("non_pro_se"),
col("first_action_pendency_ph"),
#col("last_modified_date"),
#col("Right_Right_SER_NUM"),
col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),
#col("filing_method_filed"),
#col("filing_method_cur"),
col("filing_basis_fil"),
#col("filing_basis_amed"),
#col("registration_number"),
col("Right_Right_AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),
#col("am_stat"),
#col("am_flg_no_bas_fil"),
#col("am_flg_teasrf_fil"),
#col("am_flg_use_fil"),
#col("am_flg_itu_fil"),
#col("am_flg_teaspl_fil"),
col("Right_Right_LAST_MODIFIED_DATE"),
col("filing_basis_grp"),
#col("mark_dwg_cd"),
#col("mark_dwg_desc"),
#col("mark_nm_short"),
#col("mark_nm"),
#col("tmng_image_link"),
#col("tm_analytics_ts"),
#col("exmr_eid"),
col("Credit_flag")
)
)   



# COMMAND ----------

joined_ip134_nMad_df = joined_ip134_nMad_df.withColumn(
    "tran_status_cd", when(col("Credit_flag") == 1, "A").otherwise(col("tran_status_cd"))
).withColumn(
    "unit_qt", when((col("tran_status_cd") == "R") | (col("tran_am") < 0), 0).otherwise(col("unit_qt"))
)

# COMMAND ----------

#Ip134 NonMadrid step 6 Formula

ip134_nMad_formula = joined_ip134_nMad_df.select(col("ACCTG_DT"),
col("PSTNG_REF_TX"),
col("PRJCT_CD"),
col("MAILROOM_DT"),
col("REV_SRC_CD"),
col("FEE_AM"),
col("UNIT_QT"),
col("TRAN_AM"),
col("DOC_STATUS_CD"),
col("TRAN_STATUS_CD"),
col("DOC_CLSFCN_CD"),
col("TRAN_PSTNG_REF_TX"),
col("ser_num"),
col("first_action_dt_ph"),
col("am_1_actn_ct_dt"),
col("first_action_type"),
col("filing_dt"),
col("ib_notification_dt"),
col("published_dt"),
col("noa_dt"),
col("abandonment_dt"),
col("aban_dt_ph"),
col("registration_dt"),

col("cancellation_dt"),
col("renewal_dt"),
col("revival_dt"),
col("susp_check_dt"),
col("am_cls_ct_actv"),
col("pendency_cal_start_dt"),
col("pendency_cal_end_dt"),
col("noa_registration_check"),

col("first_action_cd"),
col("disposal_pendency"),

col("disposal_dt"),
col("dock_dt"),

col("filing_fy"),

col("first_action_pendency_ph"),

col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),

col("filing_basis_fil"),

col("Right_Right_AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),

col("Right_Right_LAST_MODIFIED_DATE"),
col("filing_basis_grp"),

col("Credit_flag")

)

# COMMAND ----------

# ip134_nMad_select = ip134_nMad_formula.select(col("SER_NUM"),
#                                                col("ACCTG_DT"),
#                                                col("UNIT_QT"))

# COMMAND ----------

#Ip134 NonMadrid step 7
# Summarize grouping the data on SER_NUM
from pyspark.sql.functions import sum as _sum
ip134_nmad_grouped = ip134_nMad_formula.groupBy(col("ser_num")).agg(min(col("ACCTG_DT")).alias("Min_ACCTG_DT"), \
       _sum("UNIT_QT").alias("Fixed_Count"))

       
#display(ip134_nmad_grouped)
#min(col("ACCTG_DT")).alias("Min_ACCTG_DT"), \
#ip134_non_mad_F2_grouped = ip134_non_mad_F2_grouped.select(col("PRJCT_CD"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Second flow #3

# COMMAND ----------

# input 2 - step 1 - filter out data on values of FEE_CD
fee_codes_lst_2010 = ["6001", "7001", "7007", "7009", "7931", "7933", "7017"]
ip2_df_filter = ip2_df.filter(col("FEE_CD").isin(fee_codes_lst_2010))

# COMMAND ----------

joined_ip3_ip4_nMadrid_df = joined_ip3_ip4_nMadrid_df.withColumn(
    "ser_num", col("ser_num").astype(StringType())
)

# COMMAND ----------


#ip2_df_filter
#joined_ip3_ip4_nMadrid_df


joined_ip234_nMadrid_df = \
(
    ip2_df_filter
        .join(joined_ip3_ip4_nMadrid_df,
             on = [(col("PSTNG_REF_TX") == col("ser_num"))],
             how = "inner"
             )
        .select(
            col("ACCTG_DT"),
col("PSTNG_REF_TX"),
col("MAILROOM_DT"),
col("FEE_CD"),
col("FEE_AM"),
col("UNIT_QTY"),
col("TRAN_AM"),
col("ITEM_STATUS_CD"),
col("ser_num"),
col("first_action_dt_ph"),
col("am_1_actn_ct_dt"),
col("first_action_type"),
col("filing_dt"),
col("ib_notification_dt"),
col("published_dt"),
col("noa_dt"),
col("abandonment_dt"),
col("aban_dt_ph"),
col("registration_dt"),
col("disposal_type"),
col("ext1_dt"),
col("ext2_dt"),
col("ext3_dt"),
col("ext4_dt"),
col("ext5_dt"),
col("cancellation_dt"),
col("renewal_dt"),
col("revival_dt"),
col("susp_check_dt"),
col("am_cls_ct_actv"),
col("pendency_cal_start_dt"),
col("pendency_cal_end_dt"),
col("noa_registration_check"),
col("wgtd_1st_actn_pendency"),
col("first_action_cd"),
col("disposal_pendency"),
col("suspension"),
col("ttab"),
col("disposal_dt"),
col("dock_dt"),
col("am_flg_66a_cur"),
col("am_flg_66a_fil"),
col("noa_dt_ph"),
col("filing_fy"),
col("non_pro_se"),
col("first_action_pendency_ph"),
col("last_modified_date"),
col("Right_SER_NUM").alias("Right_Right_SER_NUM"),
col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),
col("filing_method_filed"),
col("filing_method_cur"),
col("filing_basis_fil"),
col("filing_basis_amed"),
col("registration_number"),
col("Right_AM_FLG_66A_FIL").alias("Right_Right_AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),
col("am_stat"),
col("am_flg_no_bas_fil"),
col("am_flg_teasrf_fil"),
col("am_flg_use_fil"),
col("am_flg_itu_fil"),
col("am_flg_teaspl_fil"),
col("Right_LAST_MODIFIED_DATE").alias("Right_Right_LAST_MODIFIED_DATE"),
col("filing_basis_grp"),
col("mark_dwg_cd"),
col("mark_dwg_desc"),
col("mark_nm_short"),
col("mark_nm"),
col("tmng_image_link"),
col("tm_analytics_ts"),
col("exmr_eid")
)
)   


# COMMAND ----------

#joined_ip234_nMadrid_df.show()

# COMMAND ----------

#Ip234 NonMadrid step 1 
# formula
ip234_non_mad_formula = joined_ip234_nMadrid_df.withColumn("DAYS_BTW_POSTED_AND_PEND_START_DT",datediff(col("ACCTG_DT").cast(DateType()),col("Pendency_Cal_Start_DT")))


# COMMAND ----------


ip234_non_mad_formula1 = ip234_non_mad_formula.select(col("*"),
                                   when((ip234_non_mad_formula.filing_basis_fil != "MADRID") & (ip234_non_mad_formula.DAYS_BTW_POSTED_AND_PEND_START_DT <= 6),1)
                                   .otherwise(0).alias("FEE_FLAG"),
                                   when((ip234_non_mad_formula.registration_dt.isNull()) | (ip234_non_mad_formula.ACCTG_DT < ip234_non_mad_formula.registration_dt),1)
                                   .otherwise(0).alias("Registration_Flag")
)


# COMMAND ----------

#Ip234 NonMadrid step 2
# filter to create two DFs
#1) on Registration Flag = 1 and Filing_FY >= 2010 and [FEE_FLAG] = 1
# 2) on Registration Flag = 1 and Filing_FY >= 2010 and [TRAN_AM] < 0 AND [FEE_FLAG] = 0

ip234_non_mad_filter1 = ip234_non_mad_formula1.filter(
    (col("Registration_Flag") == 1) & 
    (col("filing_fy") < "2010") &
    (col("filing_fy") >= "2006") &
    (col("FEE_FLAG") == 1))

ip234_non_mad_filter2 = ip234_non_mad_filter1.filter(
    (col("Registration_Flag") == 1) & 
    (col("filing_fy") < "2010") &
    (col("filing_fy") >= "2006") &
    (col("TRAN_AM") < 0) & 
    (col("FEE_FLAG") == 0))

# COMMAND ----------

#Ip234 NonMadrid step 3
# Summarize grouping the data on PSTNG_REF_TX

ip234_non_mad_F2_grouped = (
    ip234_non_mad_filter2
    .groupBy(
                col("PSTNG_REF_TX"))
    .count()
)

ip234_non_mad_F2_grouped = ip234_non_mad_F2_grouped.select(col("PSTNG_REF_TX"))

# COMMAND ----------

#Ip234 NonMadrid step 4
# add new column as credit_flag
ip234_non_mad_F2_formula = ip234_non_mad_F2_grouped.withColumnRenamed("PSTNG_REF_TX","PSTNG_REF_TX1").withColumn("Credit_flag",lit("1"))

# COMMAND ----------

#Ip234 NonMadrid step 5
# find and replace as join
#ip234_non_mad_filter1
#ip234_non_mad_F2_formula

joined_ip234_nMad_df = \
(
    ip234_non_mad_filter1
        .join(ip234_non_mad_F2_formula,
             on = [(col("PSTNG_REF_TX") == col("PSTNG_REF_TX1"))],
             how = "left"##################review coment: this should be full join
             )
        .select(
            col("ACCTG_DT"),
col("PSTNG_REF_TX"),
col("MAILROOM_DT"),
col("FEE_CD"),
col("FEE_AM"),
col("UNIT_QTY"),
col("TRAN_AM"),
col("ITEM_STATUS_CD"),
col("ser_num"),
col("first_action_dt_ph"),
col("am_1_actn_ct_dt"),
col("first_action_type"),
col("filing_dt"),
col("ib_notification_dt"),
col("published_dt"),
col("noa_dt"),
col("abandonment_dt"),
col("aban_dt_ph"),
col("registration_dt"),
col("disposal_type"),
col("ext1_dt"),
col("ext2_dt"),
col("ext3_dt"),
col("ext4_dt"),
col("ext5_dt"),
col("cancellation_dt"),
col("renewal_dt"),
col("revival_dt"),
col("susp_check_dt"),
col("am_cls_ct_actv"),
col("pendency_cal_start_dt"),
col("pendency_cal_end_dt"),
col("noa_registration_check"),
col("wgtd_1st_actn_pendency"),
col("first_action_cd"),
col("disposal_pendency"),
col("suspension"),
col("ttab"),
col("disposal_dt"),
col("dock_dt"),
col("am_flg_66a_cur"),
col("am_flg_66a_fil"),
col("noa_dt_ph"),
col("filing_fy"),
col("non_pro_se"),
col("first_action_pendency_ph"),
col("last_modified_date"),
col("Right_Right_SER_NUM"),
col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),
col("filing_method_filed"),
col("filing_method_cur"),
col("filing_basis_fil"),
col("filing_basis_amed"),
col("registration_number"),
col("Right_Right_AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),
col("am_stat"),
col("am_flg_no_bas_fil"),
col("am_flg_teasrf_fil"),
col("am_flg_use_fil"),
col("am_flg_itu_fil"),
col("am_flg_teaspl_fil"),
col("Right_Right_LAST_MODIFIED_DATE"),
col("filing_basis_grp"),
col("mark_dwg_cd"),
col("mark_dwg_desc"),
col("mark_nm_short"),
col("mark_nm"),
col("tmng_image_link"),
col("tm_analytics_ts"),
col("exmr_eid"),
col("DAYS_BTW_POSTED_AND_PEND_START_DT"),
col("FEE_FLAG"),
col("Registration_Flag"),
col("Credit_flag")
)
)   



# COMMAND ----------

#Ip234 NonMadrid step 6 Formula

ip234_nMad_formula = joined_ip234_nMad_df.select(col("ACCTG_DT"),
col("PSTNG_REF_TX"),
col("MAILROOM_DT"),
col("FEE_CD"),
col("FEE_AM"),
when((joined_ip234_nMad_df.ITEM_STATUS_CD == "R") | (joined_ip234_nMad_df.TRAN_AM < 0), 0)
.otherwise(joined_ip234_nMad_df.UNIT_QTY).alias("UNIT_QTY"),
col("TRAN_AM"),
when(joined_ip234_nMad_df.Credit_flag == "1","A")
.otherwise(joined_ip234_nMad_df.ITEM_STATUS_CD).alias("ITEM_STATUS_CD"),
col("ser_num"),
col("first_action_dt_ph"),
col("am_1_actn_ct_dt"),
col("first_action_type"),
col("filing_dt"),
col("ib_notification_dt"),
col("published_dt"),
col("noa_dt"),
col("abandonment_dt"),
col("aban_dt_ph"),
col("registration_dt"),
col("disposal_type"),
col("ext1_dt"),
col("ext2_dt"),
col("ext3_dt"),
col("ext4_dt"),
col("ext5_dt"),
col("cancellation_dt"),
col("renewal_dt"),
col("revival_dt"),
col("susp_check_dt"),
col("am_cls_ct_actv"),
col("pendency_cal_start_dt"),
col("pendency_cal_end_dt"),
col("noa_registration_check"),
col("wgtd_1st_actn_pendency"),
col("first_action_cd"),
col("disposal_pendency"),
col("suspension"),
col("ttab"),
col("disposal_dt"),
col("dock_dt"),
col("am_flg_66a_cur"),
col("am_flg_66a_fil"),
col("noa_dt_ph"),
col("filing_fy"),
col("non_pro_se"),
col("first_action_pendency_ph"),
col("last_modified_date"),
col("Right_Right_SER_NUM"),
col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),
col("filing_method_filed"),
col("filing_method_cur"),
col("filing_basis_fil"),
col("filing_basis_amed"),
col("registration_number"),
col("Right_Right_AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),
col("am_stat"),
col("am_flg_no_bas_fil"),
col("am_flg_teasrf_fil"),
col("am_flg_use_fil"),
col("am_flg_itu_fil"),
col("am_flg_teaspl_fil"),
col("Right_Right_LAST_MODIFIED_DATE"),
col("filing_basis_grp"),
col("mark_dwg_cd"),
col("mark_dwg_desc"),
col("mark_nm_short"),
col("mark_nm"),
col("tmng_image_link"),
col("tm_analytics_ts"),
col("exmr_eid"),
col("DAYS_BTW_POSTED_AND_PEND_START_DT"),
col("FEE_FLAG"),
col("Registration_Flag"),
col("Credit_flag")

)

# COMMAND ----------

# ip234_nMad_select = ip234_nMad_formula.select(col("SER_NUM"),
#                           col("ACCTG_DT"),
#                           col("UNIT_QTY"))

# COMMAND ----------

#Ip234 NonMadrid step 7
# Summarize grouping the data on SER_NUM
from pyspark.sql.functions import sum as _sum
ip234_nmad_grouped = (
    ip234_nMad_formula
    .groupBy(
                col("SER_NUM"))
    .agg(min(col("ACCTG_DT")).alias("Min_ACCTG_DT"), \
        _sum(col("UNIT_QTY")).alias("Fixed_Count"))
)

#ip134_non_mad_F2_grouped = ip134_non_mad_F2_grouped.select(col("PRJCT_CD"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Third flow #1

# COMMAND ----------

#ip5_df
#ip4_madrid
# output DF of this join is used in join of both IP1 and IP2 dataframes

joined_ip5_ip4_Madrid_df = \
(
    ip5_df
        .join(ip4_madrid,
             on = [(col("SER_NUM") == col("Right_SER_NUM"))],
             how = "inner"
             )
        .select(col("class_status"),
col("class"),
col("ser_num"),
col("cl_cls_us_ct"),
col("cl_cls_us"),
col("cl_dt_stat"),
col("cl_flg_anoth_form"),
col("vt_ser_num"),
col("vt_class"),
col("goods_and_services_desc"),
col("Right_SER_NUM"),
col("test_pctram_link"),
col("law_office"),
col("filing_basis_cur"),
col("filing_method_filed"),
col("filing_method_cur"),
col("filing_basis_fil"),
col("filing_basis_amed"),
col("registration_number"),
col("AM_FLG_66A_FIL"),
col("am_flg_44d_fil"),
col("am_flg_44e_fil"),
col("flg_paper_fil"),
col("am_stat"),
col("am_flg_no_bas_fil"),
col("am_flg_teasrf_fil"),
col("am_flg_use_fil"),
col("am_flg_itu_fil"),
col("am_flg_teaspl_fil"),
col("LAST_MODIFIED_DATE"),
col("filing_basis_grp"),
col("mark_dwg_cd"),
col("mark_dwg_desc"),
col("mark_nm_short"),
col("mark_nm"),
col("tmng_image_link"),
col("tm_analytics_ts"),
col("exmr_eid")
                )
)   



# COMMAND ----------

#ip54 Madrid step1

ip45_mad_filter = joined_ip5_ip4_Madrid_df.filter(
    (col("Class_Status") != "INACTIVE-Insufficient Fee Received") & 
    (col("Class_Status") != ""))

# COMMAND ----------

#Ip45 Madrid step 2
# Summarize grouping the data on SER_NUM
from pyspark.sql.functions import count as _count
ip45_mad_grouped = (
    ip45_mad_filter
    .groupBy(
                col("SER_NUM"),
                col("FILING_BASIS_FIL"))
    .agg(min(col("CL_DT_STAT")).alias("Min_ACCTG_DT"), \
        _count(col("class")).alias("Fixed_Count"))########Review comment: This is count and not sum
)

#ip134_non_mad_F2_grouped = ip134_non_mad_F2_grouped.select(col("PRJCT_CD"))

# COMMAND ----------

# MAGIC %md
# MAGIC #Union three flows

# COMMAND ----------

# ip45_mad_filter.select(col("SER_NUM"),
#                        col("FILING_BASIS_FIL"),
#                        col("ACCTG_DT"),
#                        col("UNIT_QT"))

# COMMAND ----------

#res = data_frame1.unionByName(data_frame2, allowMissingColumns=True)

#ip234_nmad_grouped
#ip134_nmad_grouped
#ip45_mad_grouped

all_ip_union1_df = ip234_nmad_grouped.unionByName(ip134_nmad_grouped)
all_ip_union_df = all_ip_union1_df.unionByName(ip45_mad_grouped, allowMissingColumns=True)


# COMMAND ----------

all_ip_union_df = all_ip_union_df.withColumnRenamed("SER_NUM","Right_SER_NUM")


# COMMAND ----------

#ip3_df
#all_ip_union_df

joined_all_union_ip3_df = \
(
    ip3_df.join(all_ip_union_df, on = [(col("SER_NUM") == col("Right_SER_NUM"))], how = "inner")
        .select(col("SER_NUM"),
                col("Pendency_Cal_Start_DT"),
                col("Filing_FY"),
                col("Min_ACCTG_DT"),
                col("Fixed_Count"),
                col("FILING_BASIS_FIL"))
)   



# COMMAND ----------

#filter on filing_fy
all_union_ip3_filter = joined_all_union_ip3_df.filter(col("Filing_FY") >= "2006")

# COMMAND ----------

# MAGIC %md
# MAGIC #Input 6 flow

# COMMAND ----------

#all_union_ip3_filter
#ip6_df
ip6_df1 = ip6_df.withColumnRenamed("SER_NUM","Right_SER_NUM")

joined_all_union_ip3_df = \
(
    all_union_ip3_filter.join(ip6_df1, on = [(col("SER_NUM") == col("Right_SER_NUM"))], how = "left_anti")
        .select(col("SER_NUM"),
                col("Pendency_Cal_Start_DT"),
                col("Filing_FY"),
                col("Min_ACCTG_DT"),
                col("Fixed_Count"),
                col("FILING_BASIS_FIL"))
)   



# COMMAND ----------

#formula

all_union_ip3_formula = joined_all_union_ip3_df.select(col("SER_NUM"),
                col("Pendency_Cal_Start_DT"),
                col("Filing_FY"),
                col("Min_ACCTG_DT"),
                col("Fixed_Count"),
                col("FILING_BASIS_FIL"),
                               when((datediff(joined_all_union_ip3_df.Min_ACCTG_DT,joined_all_union_ip3_df.Pendency_Cal_Start_DT) <= 12) & (datediff(joined_all_union_ip3_df.Min_ACCTG_DT,joined_all_union_ip3_df.Pendency_Cal_Start_DT) >= 0),1)
                               .otherwise(0).alias("TEN_DAYS"))
                               
all_union_ip3_formula = all_union_ip3_formula.withColumn("DATE_STAMP",current_timestamp())


# COMMAND ----------

all_union_ip3_filtered = all_union_ip3_formula.filter(col("TEN_DAYS") == 1)

# COMMAND ----------


# Summarize grouping the data on SER_NUM
from pyspark.sql.functions import sum as _sum
all_union_ip3_grouped = all_union_ip3_filtered.groupBy(col("ser_num")).agg(_sum(col("Fixed_Count")).alias("CLASS_COUNT"), \
       max("DATE_STAMP").alias("DATE_STAMP"))


# COMMAND ----------

# DBTITLE 1,Dropping the Extra column to make the union command happend.
ip6_df_1=ip6_df.select(col("ser_num"),col("class_count"),col("date_stamp"))

# COMMAND ----------

# DBTITLE 1,Union Command
all_ip_union_df = all_union_ip3_grouped.unionByName(ip6_df_1)

# COMMAND ----------

final_df = (
    all_ip_union_df.select(
        col("ser_num").cast(IntegerType()), col("CLASS_COUNT"), col("DATE_STAMP")
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user", lit("-1"))
)

# COMMAND ----------

final_df = final_df.withColumn(
    "DATE_STAMP", col("DATE_STAMP").astype(DateType())
)

# COMMAND ----------

#final_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing dataframe silver schema

# COMMAND ----------

try:
    final_df.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.silver.fixed_class_counts')
    recs_count = final_df.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading fixed_class_counts Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading fixed_class_counts Table ")

# COMMAND ----------



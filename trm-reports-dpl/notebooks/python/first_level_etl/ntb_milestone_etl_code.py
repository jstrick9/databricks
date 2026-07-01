# Databricks notebook source
from pyspark.sql.functions import date_trunc, round, first

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overview of Milestone ETL
# MAGIC
# MAGIC This notebook will gives us the overview for Milestone ETL. Which contain the workflow diagram and Input and output dataframs.
# MAGIC Subsequent Notebook will provide the psudo code to flow
# MAGIC

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./ntb_milestone_etl_input $config_file=config_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Divisional & Transformation

# COMMAND ----------

# DBTITLE 1,Input 1 dataframe
#Input1 - Step1 - Sorting input1 dataframe
ip1_df_sort= ip1_df.orderBy('AM_SER_NUM')

# COMMAND ----------

#input1 - step2 - Selecting and renaming columns
ip1_df_select = ip1_df_sort.select(col("AM_SER_NUM"),
                                col("AM_DT_FIL").alias("FILING_DT"),
                                col("RI_NOTIF_DT").alias("IB_NOTIFICATION_DT"),
                                #col("AM_DT_FIL"),
                                col("AM_DT_CNCL"),
                                col("AM_DT_PUB").alias("PUBLISHED_DT"),
                                col("AM_DT_REG").alias("REGISTRATION_DT"),
                                col("AM_DT_PUB_12C"),
                               # col("RI_IB_PUB_DT"),
                                #col("AM_DT_REG"),
                                col("AM_DT_RNWL"),
                                col("AM_DT_SUSP_CHECK"),
                                col("AM_DT_ABAN"),
                                col("AM_FLG_66A_CUR"),
                                col("AM_FLG_66A_FIL"),
                                col("IU_DT_NOA"),
                                col("AM_1_ACTN_CT_DT"),
                                col("AM_CLS_CT_ACTV"),
                                col("LAST_MODIFIED_DATE")

    
)
#display(ip1_df_select)

# COMMAND ----------

# DBTITLE 1,Input 2

#input2 - Step1 -  Selecting and renaming columns

ip2_df_select = ip2_df.select(col("CL_SER_NUM").alias("SER_NUM").cast(StringType()),
            col("RI_NOTIF_DT"),
            col("AM_DT_FIL"),
            col("CL_DT_1_USE").cast(StringType()),
            col("CL_DT_1_USE_COMM").cast(StringType()),
            col("DV_PRNT_SER_NUM").cast(StringType()),
            col("DV_DT_PRCS_CMPLT").cast(StringType()),
            col("DV_STAT").cast(StringType()),
            col("DV_CHLD_SER_NUM1").alias("DV_CHLD_SER_NUM").cast(StringType()),
            col("DV_DT_PRCS_CMPLT1").cast(StringType()),
            col("DV_STAT1").cast(StringType()),
            col("LAST_MODIFIED_DATE"),
            col("DV_DT_CHLD_RQST").alias("DV_DT_RQST1").cast(StringType())

    
)

# COMMAND ----------

# input 2 - step 2 - filter out all empty data from DV_PRNT_SER_NUM and DV_CHLD_SER_NUM
ip2_df_filter = ip2_df_select.filter(~(col("DV_PRNT_SER_NUM").isNull() & col("DV_CHLD_SER_NUM").isNull()))

# COMMAND ----------

# DBTITLE 1,Input 3
#input3 - Step1 -  Selecting and renaming columns

ip3_df_select = ip3_df.select(col("VT_SER_NUM"),
                        col("VT_TEXT").alias("TRANSFORMED_SER_NUM"),
                        col("LAST_MODIFIED_DATE").alias("TRANS_DT"),
                        col("VT_ENT_NUM"),
                        col("VT_TEXT_TYPE")

    
)

# COMMAND ----------

#Input1 - Step2 - Sorting input1 dataframe
ip3_df_sort= ip3_df_select.orderBy('VT_SER_NUM')

# COMMAND ----------

# DBTITLE 1,Join two dataframes
#ip2_df_filter
#ip3_df_sort

joined_ip2_ip3_full_df = \
(
    ip2_df_filter.join(ip3_df_sort,(col("SER_NUM") == col("VT_SER_NUM")),"full_outer")
        .select(col("SER_NUM"),
                col("VT_SER_NUM"),
            col("RI_NOTIF_DT"),
            col("AM_DT_FIL"),
            col("CL_DT_1_USE"),
            col("CL_DT_1_USE_COMM"),
            col("DV_PRNT_SER_NUM"),
            col("DV_DT_PRCS_CMPLT"),
            col("DV_STAT"),
            col("DV_CHLD_SER_NUM"),
            col("DV_DT_PRCS_CMPLT1"),
            col("DV_STAT1"),
            col("LAST_MODIFIED_DATE"),
            col("DV_DT_RQST1"),
            col("TRANSFORMED_SER_NUM"),
            col("TRANS_DT"),
            col("VT_ENT_NUM"),
            col("VT_TEXT_TYPE")
                )
)   
## Below code is used for full outer join 
joined_ip2_ip3_full_df_frm = joined_ip2_ip3_full_df.withColumn("SER_NUM",when(col("VT_SER_NUM").isNotNull(),col("VT_SER_NUM"))
                                  .otherwise(col("SER_NUM")))

# COMMAND ----------

# convert to dates
joined_ip2_ip3_full_df_frm = joined_ip2_ip3_full_df_frm.withColumn(
    "DV_DT_PRCS_CMPLT1", col("DV_DT_PRCS_CMPLT1").astype(DateType())
).withColumn(
    "DV_DT_PRCS_CMPLT", col("DV_DT_PRCS_CMPLT").astype(DateType())
).withColumn(
    "DV_DT_RQST1", col("DV_DT_RQST1").astype(DateType())
)

# COMMAND ----------

#ip23 Step1 - extract dates in yyyymmdd format

date_format_ip23_df = joined_ip2_ip3_full_df_frm.withColumn(
    "DV_DT_CMPLT", col("DV_DT_PRCS_CMPLT1")
).withColumn(
    "DV_DT_PRCS_CMPLTC", col("DV_DT_PRCS_CMPLT")
).withColumn(
    "DV_DT_PRCS_CMPLTP", col("DV_DT_PRCS_CMPLT1")
).withColumn(
    "DV_DT_RQST", col("DV_DT_RQST1")
)
    

# COMMAND ----------

# ip23 Step2 - apply formula on few columns

ip23_df_formula1 = date_format_ip23_df.select(
    col("*"),
    when(
        date_format_ip23_df.DV_PRNT_SER_NUM.isNotNull()
        & date_format_ip23_df.DV_CHLD_SER_NUM.isNull(),
        "CHILD",
    )
    .when(
        date_format_ip23_df.DV_PRNT_SER_NUM.isNull()
        & date_format_ip23_df.DV_CHLD_SER_NUM.isNotNull(),
        "PARENT",
    )
    .when(
        when(date_format_ip23_df.DV_STAT == "06", True)
        & date_format_ip23_df.DV_PRNT_SER_NUM.isNotNull()
        & date_format_ip23_df.DV_CHLD_SER_NUM.isNotNull(),
        "CHILD",
    )
    .otherwise(None)
    .alias("DV_TYPE"),
)
ip23_df_formula2 = ip23_df_formula1.withColumn(
    "REF_SER_NUM",
    when(col("DV_PRNT_SER_NUM") == '0', None)
    .when(col("DV_CHLD_SER_NUM") == '0', None)
    .when(
        col("DV_PRNT_SER_NUM").isNull() & col("DV_CHLD_SER_NUM").isNotNull(),
        col("DV_CHLD_SER_NUM"),
    )
    .when(
        col("DV_PRNT_SER_NUM").isNotNull() & col("DV_CHLD_SER_NUM").isNull(),
        col("DV_PRNT_SER_NUM"),
    )
    .when(
        col("DV_PRNT_SER_NUM").isNull()
        & col("DV_CHLD_SER_NUM").isNull()
        & col("TRANSFORMED_SER_NUM").isNotNull(),
        col("TRANSFORMED_SER_NUM"),
    )
    .otherwise(None),
)

ip23_df_formula = ip23_df_formula2.select(
    col("*"),
    when(ip23_df_formula1.DV_TYPE == "CHILD", ip23_df_formula1.DV_DT_PRCS_CMPLTC)
    .when(ip23_df_formula1.DV_TYPE == "PARENT", ip23_df_formula1.DV_DT_PRCS_CMPLTP)
    .otherwise(None)
    .alias("DV_DT_COMPLETE"),
)

# COMMAND ----------

#ip23 Step3 - Select required columns

ip23_df_select = ip23_df_formula.select(col("CL_DT_1_USE"),
                                         col("CL_DT_1_USE_COMM"),
                                         col("DV_CHLD_SER_NUM").alias("CHILD_SER_NUM"),
                                         col("DV_DT_COMPLETE"),
                                         col("DV_DT_RQST"),
                                         col("DV_DT_RQST1"),
                                         col("DV_PRNT_SER_NUM").alias("PRNT_SER_NUM"),
                                         col("DV_TYPE"),
                                         col("LAST_MODIFIED_DATE"),
                                         col("REF_SER_NUM").cast(IntegerType()),
                                         col("SER_NUM"),
                                         col("TRANS_DT"),
                                         col("TRANSFORMED_SER_NUM")
)

# COMMAND ----------

# DBTITLE 1,Change aggregation logic - Discuss
#ip23 Step4 - grouping the data on columns
#-----> last modified date is not getting pulled from this dataframe in further join so have not pulled the date ahead
ip23_df_grouped = (
    ip23_df_select
    .groupBy(
                col("SER_NUM"),
                col("TRANSFORMED_SER_NUM"),
                col("TRANS_DT"),
                col("DV_TYPE"),
                col("REF_SER_NUM"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("PRNT_SER_NUM"),
                col("CHILD_SER_NUM")
                )#.agg(last("LAST_MODIFIED_DATE").alias("LAST_MODIFIED_DATE"))
    .count()
) \
    .drop("count")

# COMMAND ----------

# DBTITLE 1,Join two dataframes
#ip1_df_select
#ip23_df_grouped

joined_ip123_left_df = \
(
    ip1_df_select
        .join(ip23_df_grouped,
             on = [(col("AM_SER_NUM") == col("SER_NUM"))],
             how = "left"
             )
        .select(col("AM_SER_NUM").alias("SER_NUM"),
                                col("FILING_DT"),
                                col("IB_NOTIFICATION_DT"),
                                col("AM_DT_CNCL"),
                                col("PUBLISHED_DT"),
                                col("REGISTRATION_DT"),
                                col("AM_DT_RNWL"),
                                col("AM_DT_SUSP_CHECK"),
                                col("AM_DT_ABAN"),
                                col("AM_FLG_66A_CUR"),
                                col("AM_FLG_66A_FIL"),
                                col("IU_DT_NOA"),
                                col("AM_1_ACTN_CT_DT"),
                                col("AM_CLS_CT_ACTV"),
                                col("TRANS_DT"),
                                col("DV_TYPE"),
                                col("DV_DT_COMPLETE"),
                                col("REF_SER_NUM"),
                                col("LAST_MODIFIED_DATE"),
                                col("DV_DT_RQST"),
                                col("PRNT_SER_NUM"),
                                col("CHILD_SER_NUM")
                )
)   


# COMMAND ----------

# DBTITLE 1,Discuss IMP
#ip123 Step1 - grouping the data on columns
ip123_df_grouped = (
    joined_ip123_left_df
    .groupBy(col("SER_NUM"),
                                col("FILING_DT"),
                                col("IB_NOTIFICATION_DT"),
                                col("AM_DT_CNCL"),
                                col("PUBLISHED_DT"),
                                col("REGISTRATION_DT"),
                                col("AM_DT_RNWL"),
                                col("AM_DT_SUSP_CHECK"),
                                col("AM_DT_ABAN"),
                                col("AM_FLG_66A_CUR"),
                                col("AM_FLG_66A_FIL"),
                                col("IU_DT_NOA"),
                                col("AM_1_ACTN_CT_DT"),
                                col("AM_CLS_CT_ACTV"),
                                col("DV_TYPE")
                                ).agg(max("TRANS_DT").alias("TRANS_DT"),
                                      max("DV_DT_COMPLETE").alias("DV_DT_COMPLETE"),
                                      max("REF_SER_NUM").alias("REF_SER_NUM"),
                                      max("LAST_MODIFIED_DATE").alias("LAST_MODIFIED_DATE"),
                                      max("DV_DT_RQST").alias("DV_DT_RQST"))
)

# COMMAND ----------

# ip123 Step2 - Select required columns

ip123_final_df = ip123_df_grouped.select(
    col("AM_1_ACTN_CT_DT"),
    col("AM_CLS_CT_ACTV"),
    col("AM_DT_ABAN"),
    col("AM_DT_CNCL"),
    col("AM_DT_RNWL"),
    col("AM_DT_SUSP_CHECK"),
    col("AM_FLG_66A_CUR"),
    col("AM_FLG_66A_FIL"),
    col("DV_DT_COMPLETE"),
    col("DV_DT_RQST"),
    col("DV_TYPE"),
    col("FILING_DT"),
    col("IB_NOTIFICATION_DT"),
    col("IU_DT_NOA"),
    col("LAST_MODIFIED_DATE"),
    col("PUBLISHED_DT"),
    col("REF_SER_NUM"),
    col("REGISTRATION_DT"),
    col("SER_NUM"),
    col("TRANS_DT"),
)

Op_div_final_df = ip123_final_df.select(
    col("SER_NUM"),
    col("FILING_DT").cast(DateType()),
    col("IB_NOTIFICATION_DT"),
    col("DV_TYPE"),
    col("REF_SER_NUM"),
    col("DV_DT_RQST"),
    col("DV_DT_COMPLETE"),
    col("LAST_MODIFIED_DATE"),
    col("TRANS_DT").cast(DateType()),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: Divisional Tbale

# COMMAND ----------

op_div_table = Op_div_final_df.filter(col("REF_SER_NUM").isNotNull()) \
                                        .withColumn("create_ts", current_timestamp())\
                                        .withColumn("create_user_id", lit("-1"))\
                                        .withColumn("update_ts", current_timestamp())\
                                        .withColumn("update_user_id", lit("-1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Extension Dates Pull

# COMMAND ----------

#ip4 Step1 - Select required columns

ip4_select_df = ip4_df.select(col("AM_SER_NUM").cast(StringType()),
                              col("AM_DT_FIL").cast(DateType()),
                              col("RI_NOTIF_DT").cast(DateType()),
                              col("CM_ENT_CD").cast(StringType()),
                              col("CM_ENT_DT").cast(StringType()),
                              col("CM_ENT_NUM")
                              )


# COMMAND ----------

# Input4 Step2 - sorting by ascending
ip4_sort_df = ip4_select_df.orderBy("AM_SER_NUM", "CM_ENT_NUM")

# COMMAND ----------

#Input4 Step3 - adding new column
ip4_formula_df = ip4_sort_df.withColumn("Column_Heading",expr("concat(CM_ENT_CD,'_DT')"))

# COMMAND ----------

#Input4 Step4 - pivot

ip4_pivot_df = ip4_formula_df.groupBy(col("AM_SER_NUM")).pivot("Column_Heading").agg(first(col("CM_ENT_DT")))

# COMMAND ----------

ip4_final_df = ip4_pivot_df.select(col("AM_SER_NUM"),
                                   col("EXT1_DT").cast(DateType()),
                                   col("EXT2_DT").cast(DateType()),
                                   col("EXT3_DT").cast(DateType()),
                                   col("EXT4_DT").cast(DateType()),
                                   col("EXT5_DT").cast(DateType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ##1st_Action_Date

# COMMAND ----------

#ip5 Step1 - Select required columns

ip5_select_df = ip5_df.select(col("AM_SER_NUM"),
                              col("AM_DT_FIL"),
                              col("RI_NOTIF_DT"),
                              col("CM_ENT_CD"),
                              col("CM_ENT_DT").cast(StringType()),
                              col("CM_ENT_NUM")
                              )


# COMMAND ----------

#ip5 Step2 - sort on two columns 
ip5_sort_df= ip5_select_df.sort("AM_SER_NUM","CM_ENT_NUM")

# COMMAND ----------

#ip5 Step3 - Select required columns

ip5_select_df = ip5_df.select(col("AM_SER_NUM"),
                              col("CM_ENT_CD"),
                              col("CM_ENT_DT").cast(StringType()),
                              col("CM_ENT_NUM")
                              )


# COMMAND ----------

# DBTITLE 1,Joining two dataframes
#ist_actn_blk_ip1.show()
#ip5 Step4 - join data with internal input
#ip5_select_df
#ist_actn_blk_ip1
#-------> in Altryx they have used find and replace component to serve functionality of join because the join keys are not eaxctaly the same. Keys are CM_ENT_CD from left and Code from right. Values from code are subset of values from CM_ENT_CD, thats why this component is used. and when we find value of code in CM_ENT_CD, the action item is to add the "5th Character" field in record as new column. So we have used join to achieve same result.

joined_ip5_left_df = \
(
    ip5_select_df
        .join(ist_actn_blk_ip1,
             on = [(col("CM_ENT_CD") == col("CODE"))],
             how = "left"
             )
        .select(col("AM_SER_NUM"),
                              col("CM_ENT_CD"),
                              col("CM_ENT_DT"),
                              col("CM_ENT_NUM"),
                              col("5th_Character")
                )
)   


# COMMAND ----------

#ip5 Step5 - Filter data on column 5th_Character having F value
ip5_filter_df = joined_ip5_left_df.filter(col("5th_Character") == "F")

# COMMAND ----------

#ip5 Step6 - Summarize the data
ip5_grouped_df = ip5_filter_df.groupBy("AM_SER_NUM").agg(min("CM_ENT_NUM").alias("Min_CM_ENT_NUM"),
                                                         min("CM_ENT_DT").alias("Min_CM_ENT_DT"))

# COMMAND ----------

#ip5 flow 2 Step1 - Select required columns

ip5_flow2_select_df = ip5_df.select(col("AM_SER_NUM").alias("SER_NUM"),
                              col("CM_ENT_CD").cast(StringType()),
                              col("CM_ENT_DT").cast(DateType()),
                              col("CM_ENT_NUM").cast(IntegerType())
                              )


# COMMAND ----------

# DBTITLE 1,Join two dataframes
#ist_actn_blk_ip1.show()
#ip5 Step7 - join data with internal input
#ip5_grouped_df
#ip5_flow2_select_df
#-----> fixed
joined_ip5_left_df = \
(
    ip5_grouped_df
        .join(ip5_flow2_select_df,
             on = [(col("AM_SER_NUM") == col("SER_NUM")),
                   col("Min_CM_ENT_NUM") == col("CM_ENT_NUM")],
             how = "left"
             )
        .select(col("AM_SER_NUM"),
                              col("Min_CM_ENT_NUM"),
                              col("Min_CM_ENT_DT"),
                              col("CM_ENT_CD")
                )
)   


# COMMAND ----------

# DBTITLE 1,Join two dataframes
#ist_actn_blk_ip1.show()
#ip5 Step8 - join data with internal input
#joined_ip5_left_df
#ist_actn_blk_ip1
#--------> fixed
ip5_final_df = \
(
    joined_ip5_left_df
        .join(ist_actn_blk_ip2,
             on = [(upper(col("CM_ENT_CD")) == upper(col("CODE")))],
             how = "left"
             )
        .select(col("AM_SER_NUM"),
                              col("Min_CM_ENT_NUM"),
                              col("Min_CM_ENT_DT"),
                              col("CM_ENT_CD"),
                              col("Description")
                )
)   


# COMMAND ----------

# MAGIC %md
# MAGIC ##FLAGS (SUSP & TTAB)

# COMMAND ----------

# DBTITLE 1,Input6
#ip6 Step1 - Select required columns

ip6_select_df = ip6_df.select(col("AM_SER_NUM"),
                              col("AM_DT_FIL").cast(DateType()),
                              col("RI_NOTIF_DT").cast(DateType()),
                              col("CM_ENT_CD"),
                              col("CM_ENT_DT"),
                              col("CM_ENT_NUM"),
                              col("CM_ENT_TYPE").alias("5th Character")
                              )
                              

# COMMAND ----------

#ip6 Step2 - apply formula to calculate two columns


ip6_formula_df = ip6_select_df.select(col("*"),
                                      when(ip6_select_df.CM_ENT_CD == "NREV", ip6_select_df.CM_ENT_DT)
                                      .otherwise(None).alias("REVIVAL_DT"),
                                      when(ip6_select_df.CM_ENT_CD == "CNSL", "1")
                                      .when(ip6_select_df.CM_ENT_CD == "GNSL", "1")
                                      .otherwise(None).alias("SUSPENSION_FLG"),
                                      )
                

# COMMAND ----------

# DBTITLE 1,Join two Dataframes
#ist_actn_blk_ip1.show()
#ip6 Step3 - join data with internal input
#ip6_formula_df
#ip5_flow2_select_df

joined_ip6_left_df = \
(
    ip6_formula_df
        .join(flage_blk_ip,
             on = [(col("CM_ENT_CD") == col("CODE"))],
             how = "left"
             )
        .select(col("AM_SER_NUM"),
                col("AM_DT_FIL"),
                col("RI_NOTIF_DT"),
                col("CM_ENT_CD"),
                col("CM_ENT_DT"),
                col("CM_ENT_NUM"),
                col("5th Character"),
                col("REVIVAL_DT"),
                col("SUSPENSION_FLG"),
                col("TTAB_5th_Char")
                )
)   


# COMMAND ----------

#ip6 Step4 - apply formula to calculate two columns


ip6_formula_df = joined_ip6_left_df.select(col("*"),
                                      when(joined_ip6_left_df.TTAB_5th_Char == "T", "1")
                                      .when(joined_ip6_left_df.TTAB_5th_Char == "TR", "1")
                                      .otherwise(None).alias("TTAB_FLG"),
                                      )
                

# COMMAND ----------

#ip6 Step5 - Summarize the data
ip6_final_df = ip6_formula_df.groupBy("AM_SER_NUM").agg(min("REVIVAL_DT").alias("Min_REVIVAL_DT"),
                                                          max("SUSPENSION_FLG").alias("Max_SUSPENSION_FLG"),
                                                          max("TTAB_FLG").alias("Max_TTAB_FLG"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ABAN_DT_PH

# COMMAND ----------

# step 2 - Sorting 
ip7_sort_df = ip7_df.sort("AM_SER_NUM","CM_ENT_NUM")

# COMMAND ----------

# step 3 - Formula
ip7_for_df = ip7_sort_df \
    .withColumn("ABAN_DT_PH", 
                when(col("CM_ENT_CD").contains("ABN"),
                     col("CM_ENT_DT"))
                .otherwise(None)) \
    .withColumn("NOA_DT_PH", 
                    when(col("IU_DT_NOA").isNotNull(),
                         col("IU_DT_NOA"))
                    .when(col("IU_DT_NOA").isNull() & col("CM_ENT_CD").contains("NOAM"),
                          col("CM_ENT_DT")
                          )
                    .otherwise(None)
                    )

# COMMAND ----------

# step4 - Select

ip7_sel_df = ip7_for_df.select(col("AM_SER_NUM"), col("CM_ENT_CD"), col("CM_ENT_DT"), col("IU_DT_NOA"), col("ABAN_DT_PH"), col("NOA_DT_PH"))

# COMMAND ----------

#step5 - summarize(flow1)
ip7_sumrz1_df = ip7_sel_df.groupBy("AM_SER_NUM").agg(max("ABAN_DT_PH").alias("ABAN_DT_PH"))

# COMMAND ----------

#step6 - summarize(flow2)
ip7_sumrz2_df = ip7_sel_df.groupBy("AM_SER_NUM").agg(max("NOA_DT_PH").alias("Max_NOA_DT_PH"))

# COMMAND ----------

#step7 - filter(flow3)
ip7_filter_df = ip7_sel_df.filter(ip7_sel_df.CM_ENT_CD == 'IUCN')

# COMMAND ----------

#step8 - summarize(flow8)
ip7_sumrz3_df = ip7_filter_df.groupBy("AM_SER_NUM").agg(max("CM_ENT_DT").alias("Max_IUCN_DT"))

# COMMAND ----------

# DBTITLE 1,Join two dataframes

#step8 - join two summarized dataframes
#ip7_sumrz2_df
#ip7_sumrz3_df

joined_sum23_left_df = \
(
    ip7_sumrz2_df
        .join(ip7_sumrz3_df,
             on = ["AM_SER_NUM"],
             how = "left"
             )
        .select(col("AM_SER_NUM"),
                col("Max_NOA_DT_PH"),
                col("Max_IUCN_DT")
                )
)   


# COMMAND ----------

# step 9 - Formula

joined_sum23_for_df = joined_sum23_left_df \
    .withColumn("NOA_DT_RVD",
                when(col("Max_IUCN_DT").isNull() & col("Max_NOA_DT_PH").isNotNull(),
                     col("Max_NOA_DT_PH"))
                .when(col("Max_NOA_DT_PH")> col("Max_IUCN_DT"),col("Max_NOA_DT_PH"))
                .when(col("Max_NOA_DT_PH")< col("Max_IUCN_DT"),None)
                .otherwise(None)
                       ) 

# COMMAND ----------

# Step 10 - Final join for ABAN_DT_PH
final_anab_df = \
(
    ip7_sumrz1_df
    .join(joined_sum23_for_df,
          on = ["AM_SER_NUM"],
          how = "left"
          )
    .select(col("AM_SER_NUM"),
            col("ABAN_DT_PH"),
            col("Max_NOA_DT_PH").alias("NOA_DT_PH"),
            col("NOA_DT_RVD")
            )
) 


# COMMAND ----------

# MAGIC %md
# MAGIC ##NON/PRO SE (NEW)

# COMMAND ----------

#ip8 Step1 - Select required columns

ip8_select_df = ip8_df.select(col("VT_SER_NUM").cast(StringType()).alias("SER_NUM"),
                              col("VT_TEXT").alias("ATT_NM")
                              )
                              

# COMMAND ----------

#ip8 Step2 - Sort on SER_NUM

ip8_df_sorted = ip8_select_df.orderBy("SER_NUM")

# COMMAND ----------

#ip8 Step3 - Summarize the data
ip8_grouped_df = ip8_df_sorted.groupBy("SER_NUM").agg(first("ATT_NM").alias("ATT_NM"))


# COMMAND ----------

#ip9 Step1 - Select required columns

ip9_select_df = ip9_df.select(col("CM_ENT_DT"),
                              col("CM_ENT_CD"),
                              col("CM_SER_NUM").cast(StringType())
                              )
                              

# COMMAND ----------

#ip9 Step2 - Summarize the data
ip9_grouped_df = ip9_select_df.groupBy("CM_SER_NUM").agg(first("CM_ENT_DT").alias("ATT_PH_ACT_DT"))


# COMMAND ----------

# DBTITLE 1,Join two Dataframes
#ist_actn_blk_ip1.show()
#ip89 Step1 - join data with internal input
#ip8_grouped_df
#ip9_grouped_df

joined_ip89_full_df = \
(
    ip8_grouped_df
        .join(ip9_grouped_df,
             on = [(col("SER_NUM") == col("CM_SER_NUM"))],
             how = "full"
             )
        .select(col("SER_NUM"),
                col("ATT_NM"),
                col("CM_SER_NUM"),
                col("ATT_PH_ACT_DT")
                )
)   


# COMMAND ----------

#ip89 Step2 - apply formula to calculate two columns


ip89_formula_df = joined_ip89_full_df.select(col("CM_SER_NUM"),
                                             col("ATT_PH_ACT_DT"),
                                             when(joined_ip89_full_df.ATT_NM.isNull(), joined_ip89_full_df.ATT_PH_ACT_DT)
                                             .otherwise(joined_ip89_full_df.ATT_NM).alias("ATT_NM"),
                                             when(joined_ip89_full_df.SER_NUM.isNull(), joined_ip89_full_df.CM_SER_NUM)
                                             .otherwise(joined_ip89_full_df.SER_NUM).alias("RIGHT_SER_NUM")
                                      )

# COMMAND ----------

#ip99 Step3 - Summarize the data
ip89_final_df = ip89_formula_df.groupBy("RIGHT_SER_NUM").agg(first("ATT_NM").alias("ATT_NM"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Suspension Flag

# COMMAND ----------

# input 10 - Formula
####Review_Comment: Incorrect formula: .when(col("CM_ENT_CD") == "GNSL",1) it should be 1 instaed of 2
#--Fixed
ip10_for_df=ip10_df.withColumn("Suspension_FLG",when(col("CM_ENT_CD") == "CNSL",1)
                  .when(col("CM_ENT_CD") == "GNSL",1)
                  .otherwise(None))

# COMMAND ----------

# input 10 - step 2 grouping
ip10_nmchng_df=ip10_for_df.withColumn("Suspension_FLG",col("Suspension_FLG").cast(StringType()))
ip10_new_final_df= ip10_nmchng_df.groupBy("AM_SER_NUM","Suspension_FLG").agg(max("CM_ENT_DT").alias("Suspension_DT"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## DOCK_DT

# COMMAND ----------


ip11_for_df=ip11_df.withColumn("DOCK_DT",when(col("CM_ENT_CD") == "DOCK",col("CM_ENT_DT").cast(DateType()))
                  .otherwise(None))

# COMMAND ----------


ip11_fianl_df = ip11_for_df.groupBy("AM_SER_NUM").agg(min("DOCK_DT").alias("DOCK_DT"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##TM_Milestone

# COMMAND ----------

#ip123_final_df
#ip4_final_df

#ist_actn_blk_ip1.show()
#ip89 Step1 - join data with internal input

joined_1_left_df = \
(
    ip123_final_df
        .join(ip4_final_df,
             on = [(col("SER_NUM") == col("AM_SER_NUM"))],
             how = "left"
             )
        .select(col("AM_1_ACTN_CT_DT"),
                col("AM_CLS_CT_ACTV"),
                col("AM_DT_ABAN"),
                col("AM_DT_CNCL"),
                col("AM_DT_RNWL"),
                col("AM_DT_SUSP_CHECK"),
                col("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("DV_TYPE"),
                col("FILING_DT"),
                col("IB_NOTIFICATION_DT"),
                col("IU_DT_NOA"),
                col("LAST_MODIFIED_DATE"),
                col("PUBLISHED_DT"),
                col("REF_SER_NUM"),
                col("REGISTRATION_DT"),
                col("SER_NUM"),
                col("TRANS_DT"),
                col("EXT1_DT"),
                col("EXT2_DT"),
                col("EXT3_DT"),
                col("EXT4_DT"),
                col("EXT5_DT")
                )
)


# COMMAND ----------

#joined_ip1_left_df
# #ip5_final_df

joined_2_left_df = \
(
    joined_1_left_df
        .join(ip5_final_df,
             on = [(col("SER_NUM") == col("AM_SER_NUM"))],
             how = "left"
             )
        .select(col("AM_1_ACTN_CT_DT"),
                col("AM_CLS_CT_ACTV"),
                col("AM_DT_ABAN"),
                col("AM_DT_CNCL"),
                col("AM_DT_RNWL"),
                col("AM_DT_SUSP_CHECK"),
                col("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("DV_TYPE"),
                col("FILING_DT"),
                col("IB_NOTIFICATION_DT"),
                col("IU_DT_NOA"),
                col("LAST_MODIFIED_DATE"),
                col("PUBLISHED_DT"),
                col("REF_SER_NUM"),
                col("REGISTRATION_DT"),
                col("SER_NUM"),
                col("TRANS_DT"),
                col("EXT1_DT"),
                col("EXT2_DT"),
                col("EXT3_DT"),
                col("EXT4_DT"),
                col("EXT5_DT"),
                col("Min_CM_ENT_NUM"),
                col("Min_CM_ENT_DT").alias("1st_Action_DT_PH"),
                col("CM_ENT_CD").alias("1st_Action_CD"),
                col("Description").alias("1st_Action_Type")
                )
)


# COMMAND ----------

#joined_2_left_df
# #ip6_final_df

joined_3_left_df = \
(
    joined_2_left_df
        .join(ip6_final_df,
             on = [(col("SER_NUM") == col("AM_SER_NUM"))],
             how = "left"
             )
        .select(col("AM_1_ACTN_CT_DT"),
                col("AM_CLS_CT_ACTV"),
                col("AM_DT_ABAN"),
                col("AM_DT_CNCL"),
                col("AM_DT_RNWL"),
                col("AM_DT_SUSP_CHECK"),
                col("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("DV_TYPE"),
                col("FILING_DT"),
                col("IB_NOTIFICATION_DT"),
                col("IU_DT_NOA"),
                col("LAST_MODIFIED_DATE"),
                col("PUBLISHED_DT"),
                col("REF_SER_NUM"),
                col("REGISTRATION_DT"),
                col("SER_NUM"),
                col("TRANS_DT"),
                col("EXT1_DT"),
                col("EXT2_DT"),
                col("EXT3_DT"),
                col("EXT4_DT"),
                col("EXT5_DT"),
                col("Min_CM_ENT_NUM"),
                col("1st_Action_DT_PH"),
                col("1st_Action_CD"),
                col("1st_Action_Type"),
                col("Min_REVIVAL_DT"),
                col("Max_SUSPENSION_FLG"),
                col("Max_TTAB_FLG")
                )
)



# COMMAND ----------

#joined_3_left_df
# #final_anab_df

joined_4_left_df = \
(
    joined_3_left_df
        .join(final_anab_df,
             on = [(col("SER_NUM") == col("AM_SER_NUM"))],
             how = "left"
             )
        .select(col("AM_1_ACTN_CT_DT"),
                col("AM_CLS_CT_ACTV"),
                col("AM_DT_ABAN"),
                col("AM_DT_CNCL"),
                col("AM_DT_RNWL"),
                col("AM_DT_SUSP_CHECK"),
                col("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("DV_TYPE"),
                col("FILING_DT"),
                col("IB_NOTIFICATION_DT"),
                col("IU_DT_NOA"),
                col("LAST_MODIFIED_DATE"),
                col("PUBLISHED_DT"),
                col("REF_SER_NUM"),
                col("REGISTRATION_DT"),
                col("SER_NUM"),
                col("TRANS_DT"),
                col("EXT1_DT"),
                col("EXT2_DT"),
                col("EXT3_DT"),
                col("EXT4_DT"),
                col("EXT5_DT"),
                col("Min_CM_ENT_NUM"),
                col("1st_Action_DT_PH"),
                col("1st_Action_CD"),
                col("1st_Action_Type"),
                col("Min_REVIVAL_DT"),
                col("Max_SUSPENSION_FLG"),
                col("Max_TTAB_FLG"),
                col("ABAN_DT_PH"),
                col("NOA_DT_PH"),
                col("NOA_DT_RVD")
                )
)


# COMMAND ----------


#joined_4_left_df
# #ip89_final_df

joined_5_left_df = \
(
    joined_4_left_df
        .join(ip89_final_df,
             on = [(col("SER_NUM") == col("RIGHT_SER_NUM"))],
             how = "left"
             )
        .select(col("AM_1_ACTN_CT_DT"),
                col("AM_CLS_CT_ACTV"),
                col("AM_DT_ABAN"),
                col("AM_DT_CNCL"),
                col("AM_DT_RNWL"),
                col("AM_DT_SUSP_CHECK"),
                col("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("DV_TYPE"),
                col("FILING_DT"),
                col("IB_NOTIFICATION_DT"),
                col("IU_DT_NOA"),
                col("LAST_MODIFIED_DATE"),
                col("PUBLISHED_DT"),
                col("REF_SER_NUM"),
                col("REGISTRATION_DT"),
                col("SER_NUM"),
                col("TRANS_DT"),
                col("EXT1_DT"),
                col("EXT2_DT"),
                col("EXT3_DT"),
                col("EXT4_DT"),
                col("EXT5_DT"),
                col("Min_CM_ENT_NUM"),
                col("1st_Action_DT_PH"),
                col("1st_Action_CD"),
                col("1st_Action_Type"),
                col("Min_REVIVAL_DT"),
                col("Max_SUSPENSION_FLG"),
                col("Max_TTAB_FLG"),
                col("ABAN_DT_PH"),
                col("NOA_DT_PH"),
                col("NOA_DT_RVD"),
                col("ATT_NM")
                )
)


# COMMAND ----------


#joined_5_left_df
# #ip10_new_final_df

joined_6_left_df = \
(
    joined_5_left_df
        .join(ip10_new_final_df,
             on = [(col("SER_NUM") == col("AM_SER_NUM"))],
             how = "left"
             )
        .select(col("AM_1_ACTN_CT_DT"),
                col("AM_CLS_CT_ACTV"),
                col("AM_DT_ABAN"),
                col("AM_DT_CNCL"),
                col("AM_DT_RNWL"),
                col("AM_DT_SUSP_CHECK"),
                col("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("DV_TYPE"),
                col("FILING_DT"),
                col("IB_NOTIFICATION_DT"),
                col("IU_DT_NOA"),
                col("LAST_MODIFIED_DATE"),
                col("PUBLISHED_DT"),
                col("REF_SER_NUM"),
                col("REGISTRATION_DT"),
                col("SER_NUM"),
                col("TRANS_DT"),
                col("EXT1_DT"),
                col("EXT2_DT"),
                col("EXT3_DT"),
                col("EXT4_DT"),
                col("EXT5_DT"),
                col("Min_CM_ENT_NUM"),
                col("1st_Action_DT_PH"),
                col("1st_Action_CD"),
                col("1st_Action_Type"),
                col("Min_REVIVAL_DT"),
                col("Max_SUSPENSION_FLG"),
                col("Max_TTAB_FLG"),
                col("ABAN_DT_PH"),
                col("NOA_DT_PH"),
                col("NOA_DT_RVD"),
                col("ATT_NM"),
                col("Suspension_FLG"),
                col("Suspension_DT")
                )
)


# COMMAND ----------


#joined_6_left_df
# #ip11_fianl_df

joined_7_left_df = \
(
    joined_6_left_df
        .join(ip11_fianl_df,
             on = [(col("SER_NUM") == col("AM_SER_NUM"))],
             how = "left"
             )
        .select(col("AM_1_ACTN_CT_DT"),
                col("AM_CLS_CT_ACTV"),
                col("AM_DT_ABAN"),
                col("AM_DT_CNCL"),
                col("AM_DT_RNWL"),
                col("AM_DT_SUSP_CHECK"),
                col("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL"),
                col("DV_DT_COMPLETE"),
                col("DV_DT_RQST"),
                col("DV_TYPE"),
                col("FILING_DT"),
                col("IB_NOTIFICATION_DT"),
                col("IU_DT_NOA"),
                col("LAST_MODIFIED_DATE"),
                col("PUBLISHED_DT"),
                col("REF_SER_NUM"),
                col("REGISTRATION_DT"),
                col("SER_NUM"),
                col("TRANS_DT"),
                col("EXT1_DT"),
                col("EXT2_DT"),
                col("EXT3_DT"),
                col("EXT4_DT"),
                col("EXT5_DT"),
                col("Min_CM_ENT_NUM"),
                col("1st_Action_DT_PH").alias("fst_Action_DT_PH"),
                col("1st_Action_CD"),
                col("1st_Action_Type"),
                col("Min_REVIVAL_DT"),
                col("Max_SUSPENSION_FLG"),
                col("Max_TTAB_FLG"),
                col("ABAN_DT_PH"),
                col("NOA_DT_PH"),
                col("NOA_DT_RVD"),
                col("ATT_NM"),
                col("Suspension_FLG"),
                col("Suspension_DT"),
                col("DOCK_DT")
                )
)


# COMMAND ----------

all_ip_df_formula = \
joined_7_left_df.select(col("*"),
                        when(joined_7_left_df.NOA_DT_RVD.isNotNull(),"NOA")
                        .when((joined_7_left_df.NOA_DT_RVD < joined_7_left_df.REGISTRATION_DT) | (joined_7_left_df.NOA_DT_RVD < joined_7_left_df.AM_DT_ABAN),"NOA")
                        .when(joined_7_left_df.REGISTRATION_DT.isNull() & joined_7_left_df.AM_DT_ABAN.isNotNull(),"ABANDONMENT")
                        .when(joined_7_left_df.AM_DT_ABAN.isNull() & joined_7_left_df.REGISTRATION_DT.isNotNull(),"REGISTRATION")
                        .when(joined_7_left_df.AM_DT_ABAN > joined_7_left_df.REGISTRATION_DT,"REGISTRATION")
                        .when(joined_7_left_df.AM_DT_ABAN < joined_7_left_df.REGISTRATION_DT,"ABANDONMENT")
                        .when(joined_7_left_df.AM_DT_ABAN.isNotNull() & (joined_7_left_df.AM_DT_ABAN == joined_7_left_df.REGISTRATION_DT),"ABANDONMENT")
                        .otherwise(None).alias("Disposal_Type"),
                        when(joined_7_left_df.NOA_DT_RVD.isNotNull(),joined_7_left_df.NOA_DT_RVD)
                        .when((joined_7_left_df.NOA_DT_RVD < joined_7_left_df.REGISTRATION_DT) | (joined_7_left_df.NOA_DT_RVD < joined_7_left_df.AM_DT_ABAN),joined_7_left_df.NOA_DT_RVD)
                        .when(joined_7_left_df.REGISTRATION_DT.isNull() & joined_7_left_df.AM_DT_ABAN.isNotNull(),joined_7_left_df.ABAN_DT_PH)
                        .when(joined_7_left_df.AM_DT_ABAN.isNull() & joined_7_left_df.REGISTRATION_DT.isNotNull(),joined_7_left_df.REGISTRATION_DT)
                        .when(joined_7_left_df.AM_DT_ABAN > joined_7_left_df.REGISTRATION_DT,joined_7_left_df.REGISTRATION_DT)
                        .when(joined_7_left_df.AM_DT_ABAN < joined_7_left_df.REGISTRATION_DT,joined_7_left_df.AM_DT_ABAN)
                        .when(joined_7_left_df.AM_DT_ABAN.isNotNull() & (joined_7_left_df.AM_DT_ABAN == joined_7_left_df.REGISTRATION_DT),joined_7_left_df.AM_DT_ABAN)
                        .otherwise(None).alias("Disposal_DT"),
                        when(joined_7_left_df.ATT_NM.isNull(),"PRO SE")
                        .otherwise("NON PRO SE").alias("NON/PRO SE"),
                        when((joined_7_left_df.AM_FLG_66A_FIL == "1") | (joined_7_left_df.AM_FLG_66A_CUR == "1") | (joined_7_left_df.SER_NUM.startswith("79")),joined_7_left_df.IB_NOTIFICATION_DT)
                        .otherwise(joined_7_left_df.FILING_DT).alias("Pendency_Cal_Start_DT"),
                        when(joined_7_left_df.NOA_DT_RVD.isNotNull(),joined_7_left_df.NOA_DT_RVD)
                        .when(joined_7_left_df.NOA_DT_RVD < joined_7_left_df.REGISTRATION_DT,joined_7_left_df.NOA_DT_RVD)
                        .when((joined_7_left_df.NOA_DT_RVD < joined_7_left_df.AM_DT_ABAN),joined_7_left_df.NOA_DT_RVD)
                        .when(joined_7_left_df.REGISTRATION_DT.isNull(),joined_7_left_df.AM_DT_ABAN)
                        .when(joined_7_left_df.AM_DT_ABAN.isNull(),joined_7_left_df.REGISTRATION_DT)
                        .when((joined_7_left_df.AM_DT_ABAN > joined_7_left_df.REGISTRATION_DT),joined_7_left_df.REGISTRATION_DT)
                        .when((joined_7_left_df.AM_DT_ABAN < joined_7_left_df.REGISTRATION_DT),joined_7_left_df.AM_DT_ABAN)
                            .when(joined_7_left_df.AM_DT_ABAN.isNotNull() & (joined_7_left_df.AM_DT_ABAN == joined_7_left_df.REGISTRATION_DT),joined_7_left_df.AM_DT_ABAN)
                                    .otherwise(joined_7_left_df.NOA_DT_RVD).alias("Pendency_Cal_End_DT"))

# COMMAND ----------

all_ip_df_formula1 = all_ip_df_formula.select(col("*"),when(all_ip_df_formula.DV_DT_COMPLETE.isNull(),None)
                  .when(all_ip_df_formula.DV_DT_COMPLETE < all_ip_df_formula.fst_Action_DT_PH,"Prior to 1st Action")
                  .when((all_ip_df_formula.DV_DT_COMPLETE < all_ip_df_formula.Disposal_DT),"Prior to Disposal")
                  .when((all_ip_df_formula.DV_DT_COMPLETE > all_ip_df_formula.REGISTRATION_DT),"After Registration")
                  .when((all_ip_df_formula.DV_DT_COMPLETE > all_ip_df_formula.NOA_DT_PH),"After NOA")
                  .otherwise(None).alias("DV_STAGE"),
                  when(month(all_ip_df_formula.Pendency_Cal_Start_DT) > 9,year(all_ip_df_formula.Pendency_Cal_Start_DT) + 1)
                  .otherwise(year(all_ip_df_formula.Pendency_Cal_Start_DT)).alias("Filing_FY"))

# COMMAND ----------

from pyspark.sql.functions import datediff
all_ip_df_formula2 = all_ip_df_formula1.withColumn("DISPOSAL_PENDENCY",datediff(col("Pendency_Cal_End_DT"),col("Pendency_Cal_Start_DT"))/30.42)\
    .withColumn("WGTD_1ST_ ACTN_PENDENCY",datediff(col("AM_1_ACTN_CT_DT"),col("Pendency_Cal_Start_DT"))*col("AM_CLS_CT_ACTV")/30.42)\
        .withColumn("NOA_REGISTRATION Check",datediff(col("IU_DT_NOA"),col("REGISTRATION_DT")))

# COMMAND ----------

all_ip_df_formula3 = all_ip_df_formula2.withColumn("1st Action Pendency_PH",datediff(col("fst_Action_DT_PH").cast(DateType()),col("Pendency_Cal_Start_DT"))/30.42)

#select(col("*"),when(datediff(all_ip_df_formula.Pendency_Cal_End_DT,all_ip_df_formula2.Pendency_Cal_Start_DT).isNotNull(),None)
 #                 .otherwise(datediff(all_ip_df_formula.Pendency_Cal_End_DT,all_ip_df_formula2.Pendency_Cal_Start_DT)/30.42).alias("1st Action Pendency_PH"))

# COMMAND ----------

# round to 2 dec places
all_ip_df_formula3 = all_ip_df_formula3.withColumn(
    "DISPOSAL_PENDENCY", round(col("DISPOSAL_PENDENCY"), 2)
).withColumn(
    "WGTD_1ST_ ACTN_PENDENCY", round(col("WGTD_1ST_ ACTN_PENDENCY"), 2)
).withColumn(
    "1st Action Pendency_PH", round(col("1st Action Pendency_PH"), 2)
)

# set null filing fys to zero
all_ip_df_formula3 = all_ip_df_formula3.fillna(0, subset=["filing_fy"])

# COMMAND ----------

all_ip_df_select = all_ip_df_formula3.select(col("SER_NUM"),
col("AM_1_ACTN_CT_DT"),
col("1st_Action_Type"),
col("FILING_DT"),
col("IB_NOTIFICATION_DT"),
col("PUBLISHED_DT"),
col("IU_DT_NOA").alias("NOA_DT"),
col("AM_DT_ABAN").alias("ABANDONMENT_DT"),
col("ABAN_DT_PH"),
col("REGISTRATION_DT"),
col("Disposal_Type"),
col("EXT1_DT"),
col("EXT2_DT"),
col("EXT3_DT"),
col("EXT4_DT"),
col("EXT5_DT"),
col("AM_DT_CNCL").alias("CANCELLATION_DT"),
col("AM_DT_RNWL").alias("RENEWAL_DT"),
col("Min_REVIVAL_DT").alias("REVIVAL_DT"),
col("AM_DT_SUSP_CHECK").alias("SUSP_CHECK_DT"),
col("AM_CLS_CT_ACTV"),
col("Pendency_Cal_Start_DT"),
col("Pendency_Cal_End_DT"),
col("NOA_REGISTRATION Check"),
col("WGTD_1ST_ ACTN_PENDENCY"),
col("1st_Action_CD"),
col("DISPOSAL_PENDENCY"),
col("Max_SUSPENSION_FLG").alias("Suspension"),
col("Max_TTAB_FLG").alias("TTAB"),
col("Disposal_DT"),
col("DOCK_DT"),
col("AM_FLG_66A_CUR"),
col("AM_FLG_66A_FIL"),
col("NOA_DT_PH"),
col("Filing_FY"),
col("NON/PRO SE"),
col("1st Action Pendency_PH"),
col("TRANS_DT"),
col("DV_TYPE"),
col("REF_SER_NUM"),
col("DV_DT_COMPLETE"),
col("Suspension_FLG"),
col("Suspension_DT"),
col("LAST_MODIFIED_DATE"),
col("NOA_DT_RVD"),
col("DV_STAGE"),
col("DV_DT_RQST"),
col("fst_Action_DT_PH")
)

# COMMAND ----------

# add in new pend columns
all_ip_df_select = all_ip_df_select.withColumn(
    "processing_pend", round(datediff(col("pendency_cal_end_dt"), col("dock_dt")) / lit(30.42), 2)
).withColumn(
    "processing_pend_days", datediff(col("pendency_cal_end_dt"), col("dock_dt"))
).withColumn(
    "days_in_dock", datediff(col("fst_Action_DT_PH"), col("dock_dt")) # US585708
)

# COMMAND ----------

#ip23 Step4 - grouping the data on columns
all_ip_df_grouped = (
    all_ip_df_select
    .groupBy(
                col("SER_NUM").alias("SER_NUM"),
                col("fst_Action_DT_PH").alias("first_action_dt_ph"),
                col("AM_1_ACTN_CT_DT").alias("AM_1_ACTN_CT_DT"),
                col("1st_Action_Type").alias("first_action_type"),
                col("FILING_DT").alias("FILING_DT"),
                col("IB_NOTIFICATION_DT").alias("IB_NOTIFICATION_DT"),
                col("PUBLISHED_DT").alias("PUBLISHED_DT"),
                col("NOA_DT").alias("NOA_DT"),
                col("ABANDONMENT_DT").alias("ABANDONMENT_DT"),
                col("ABAN_DT_PH").alias("ABAN_DT_PH"),
                col("REGISTRATION_DT").alias("REGISTRATION_DT"),
                col("Disposal_Type").alias("Disposal_Type"),
                col("EXT1_DT").alias("EXT1_DT"),
                col("EXT2_DT").alias("EXT2_DT"),
                col("EXT3_DT").alias("EXT3_DT"),
                col("EXT4_DT").alias("EXT4_DT"),
                col("EXT5_DT").alias("EXT5_DT"),
                col("CANCELLATION_DT").alias("CANCELLATION_DT"),
                col("RENEWAL_DT").alias("RENEWAL_DT"),
                col("REVIVAL_DT").alias("REVIVAL_DT"),
                col("SUSP_CHECK_DT").alias("SUSP_CHECK_DT"),
                col("AM_CLS_CT_ACTV").alias("AM_CLS_CT_ACTV"),
                col("Pendency_Cal_Start_DT").alias("Pendency_Cal_Start_DT"),
                col("Pendency_Cal_End_DT").alias("Pendency_Cal_End_DT"),
                col("NOA_REGISTRATION Check").alias("noa_registration_check"),
                col("WGTD_1ST_ ACTN_PENDENCY").alias("wgtd_1st_actn_pendency"),
                col("1st_Action_CD").alias("1st_Action_CD"),
                col("DISPOSAL_PENDENCY").alias("DISPOSAL_PENDENCY"),
                col("Suspension").alias("Suspension"),
                col("TTAB").alias("TTAB"),
                col("Disposal_DT").alias("Disposal_DT"),
                col("DOCK_DT").alias("DOCK_DT"),
                col("AM_FLG_66A_CUR").alias("AM_FLG_66A_CUR"),
                col("AM_FLG_66A_FIL").alias("AM_FLG_66A_FIL"),
                col("NOA_DT_PH").alias("NOA_DT_PH"),
                col("Filing_FY").alias("Filing_FY"),
                col("NON/PRO SE").alias("non_pro_se"),
                col("1st Action Pendency_PH").alias("first_action_pendency_ph"), 
                date_trunc("second", col("LAST_MODIFIED_DATE")).alias("LAST_MODIFIED_DATE"), # remove microseconds from timestamp
                col("processing_pend"),
                col("processing_pend_days"),
                col("days_in_dock") # US585708
                ).count()
)

# COMMAND ----------

final_milestone_df = all_ip_df_grouped.select(col("SER_NUM"),
                                        col("first_action_dt_ph"),
                                        col("AM_1_ACTN_CT_DT").cast(DateType()),
                                        col("first_action_type"),
                                        col("FILING_DT").cast(DateType()),
                                        col("IB_NOTIFICATION_DT").cast(DateType()),
                                        col("PUBLISHED_DT").cast(DateType()),
                                        col("NOA_DT").cast(DateType()),
                                        col("ABANDONMENT_DT").cast(DateType()),
                                        col("ABAN_DT_PH"),
                                        col("REGISTRATION_DT").cast(DateType()),
                                        col("Disposal_Type"),
                                        col("EXT1_DT"),
                                        col("EXT2_DT"),
                                        col("EXT3_DT"),
                                        col("EXT4_DT"),
                                        col("EXT5_DT"),
                                        col("CANCELLATION_DT").cast(DateType()),
                                        col("RENEWAL_DT").cast(DateType()),
                                        col("REVIVAL_DT"),
                                        col("SUSP_CHECK_DT").cast(DateType()),
                                        col("AM_CLS_CT_ACTV"),
                                        col("Pendency_Cal_Start_DT").cast(DateType()),
                                        col("Pendency_Cal_End_DT").cast(DateType()),
                                        col("noa_registration_check"),
                                        col("wgtd_1st_actn_pendency"),
                                        col("1st_Action_CD").alias("first_action_cd"),
                                        col("DISPOSAL_PENDENCY").alias("disposal_pendency"),
                                        col("Suspension"),
                                        col("TTAB"),
                                        col("Disposal_DT").cast(DateType()),
                                        col("DOCK_DT"),
                                        col("AM_FLG_66A_CUR"),
                                        col("AM_FLG_66A_FIL"),
                                        col("NOA_DT_PH").cast(DateType()),
                                        col("Filing_FY"),
                                        col("non_pro_se"),
                                        col("first_action_pendency_ph"),
                                        col("LAST_MODIFIED_DATE"),
                                        col("processing_pend"),
                                        col("processing_pend_days"),
                                        col("days_in_dock")
                                            )\
                                        .withColumn("create_ts", current_timestamp())\
                                        .withColumn("create_user_id", lit("-1"))\
                                        .withColumn("update_ts", current_timestamp())\
                                        .withColumn("update_user_id", lit("-1"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write the dataframe in silver layer

# COMMAND ----------

## Final Milestone dataload
final_milestone_df.write.mode("overwrite").format("delta").saveAsTable(f'{reporting_catalog}.{schema_silver}.milestone')
## Final Divisional dataload
op_div_table.write.mode("overwrite").format("delta").saveAsTable(f'{reporting_catalog}.{schema_silver}.divisionals')

# COMMAND ----------

# MAGIC %md
# MAGIC ##END

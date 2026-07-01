# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# MAGIC %run ./ntb_form_paragraph_dashboard_input $config_file=config_file

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
print(reporting_catalog)

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_third_level_form_paragraph_dashboard_etl_code'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

#cleansing the data from input1

ip1_select = ip1_df.select(initcap(trim(col("SER_NUM"))).alias("SER_NUM"),
                                initcap(trim(col("GROUP_NAME"))).alias("GROUP_NAME"),
                                initcap(trim(col("COMPLETED_DT"))).alias("COMPLETED_DT"),
                                initcap(trim(col("TRANSACTIONAL_LITERAL"))).alias("TRANSACTIONAL_LITERAL"),
                                initcap(trim(col("TRANSACTION_NO"))).alias("TRANSACTION_NO"),
                                initcap(trim(col("ACTION_COUNT"))).alias("ACTION_COUNT"),
                                initcap(trim(col("FK_WRKR_ID"))).alias("FK_WRKR_ID"),
                                initcap(trim(col("FP_ID"))).alias("FP_ID"),
                                initcap(trim(col("TITLE_TX"))).alias("TITLE_TX"),
                                initcap(trim(col("FK_FP_GROUP_ID"))).alias("FK_FP_GROUP_ID"),
                                initcap(trim(col("FK_FP_CATEGORY_ID"))).alias("FK_FP_CATEGORY_ID"),
                                initcap(trim(col("CATEGORY"))).alias("CATEGORY"),
                                initcap(trim(col("FP_YEAR"))).alias("FP_YEAR"),
                                initcap(trim(col("COMPLETED_TS"))).alias("COMPLETED_TS"),
                                initcap(trim(col("TM_ANALYTICS_TS"))).alias("TM_ANALYTICS_TS"),
                                initcap(trim(col("TOC"))).alias("TOC")
                                )

# COMMAND ----------

# add title case after special characters to match alteryx

ip1_select = ip1_select.withColumn(
    "GROUP_NAME", array_join(transform(split(regexp_replace("GROUP_NAME", '("/"|\("|"/|&/|\\(|\\+|_|\/"|\("|"\.|\.-|&|\s-|\[|"|\./|\.|\\/)(.)', '$1#%#$2'), '#%#'), lambda x: initcap(x)), '')
).withColumn(
    "TRANSACTIONAL_LITERAL", array_join(transform(split(regexp_replace("TRANSACTIONAL_LITERAL", '("/"|\("|"/|&/|\\(|\\+|_|\/"|\("|"\.|\.-|&|\s-|\[|"|\./|\.|\\/)(.)', '$1#%#$2'), '#%#'), lambda x: initcap(x)), '')
).withColumn(
    "TITLE_TX", array_join(transform(split(regexp_replace("TITLE_TX", '("/"|\("|"/|&/|\\(|\\+|_|\/"|\("|"\.|\.-|&|\s-|\[|"|\./|\.|\\/)(.)', '$1#%#$2'), '#%#'), lambda x: initcap(x)), '')
).withColumn(
    "CATEGORY", array_join(transform(split(regexp_replace("CATEGORY", '("/"|\("|"/|&/|\\(|\\+|_|\/"|\("|"\.|\.-|&|\s-|\[|"|\./|\.|\\/)(.)', '$1#%#$2'), '#%#'), lambda x: initcap(x)), '')
).withColumn(
    "TOC", array_join(transform(split(regexp_replace("TOC", '("/"|\("|"/|&/|\\(|\\+|_|\/"|\("|"\.|\.-|&|\s-|\[|"|\./|\.|\\/)(.)', '$1#%#$2'), '#%#'), lambda x: initcap(x)), '')
)

# COMMAND ----------

# DBTITLE 1,Below code is to remove tabs, newlines and duplicate whitespace
columns_for_clean=["SER_NUM","GROUP_NAME","COMPLETED_DT","TRANSACTIONAL_LITERAL","TRANSACTION_NO","ACTION_COUNT","FK_WRKR_ID","FP_ID","TITLE_TX","FK_FP_GROUP_ID","FK_FP_CATEGORY_ID","CATEGORY","FP_YEAR","COMPLETED_TS","TM_ANALYTICS_TS","TOC"]
### below function is define in shared comman notebooks for functions
ip1_select = columns_to_clean(ip1_select,columns_for_clean)

# COMMAND ----------

#filter on SerNUm
ip1_filter = ip1_select.filter(col("SER_NUM") != -1).withColumn(
    "action_count", col("action_count").astype(IntegerType())
).withColumn(
    "transaction_no", col("transaction_no").astype(LongType())
)

# COMMAND ----------

#Ip1 Summarize grouping the data on SER_NUM, ACTION_COUNT
ip1_grouped = (
    ip1_filter
    .groupBy(
                col("SER_NUM"),
                col("ACTION_COUNT"))
    .agg(
       concat_ws(";", collect_list(col("FP_ID"))).alias("Concat_FP_ID"), \
           concat_ws(";", collect_list(col("CATEGORY"))).alias("Concat_CATEGORY"))
)

# add leading and trailing semi colons
ip1_grouped = ip1_grouped.withColumn(
    "Concat_FP_ID", concat(lit(';'), col("Concat_FP_ID"), lit(';'))
).withColumn(
    "Concat_CATEGORY", concat(lit(';'), col("Concat_CATEGORY"), lit(';'))
)

# COMMAND ----------

# fill null values for join
ip1_filter = ip1_filter.fillna(99999, subset=["action_count"])
ip1_grouped = ip1_grouped.fillna(99999, subset=["action_count"])

# COMMAND ----------

#ip1_grouped
#ip1_filter

#### this is where count error is coming from

ip1_joined = ip1_filter.join(ip1_grouped, on = ["ser_num", "action_count"], how = "inner").select(
    col("SER_NUM"),
    col("GROUP_NAME"),
    col("COMPLETED_DT"),
    col("TRANSACTIONAL_LITERAL"),
    col("ACTION_COUNT"),
    col("FK_WRKR_ID"),
    col("FP_ID"),
    col("TITLE_TX"),
    col("FK_FP_GROUP_ID"),
    col("FK_FP_CATEGORY_ID"),
    col("CATEGORY"),
    col("FP_YEAR"),
    col("COMPLETED_TS"),
    col("TM_ANALYTICS_TS"),
    col("TOC"),
    col("Concat_FP_ID"),
    col("Concat_CATEGORY"),
    col("TRANSACTION_NO")
)

# COMMAND ----------

# revert null fill
ip1_joined = ip1_joined.withColumn(
    "action_count", when(col("action_count") == 99999, lit(None)).otherwise(col("action_count"))
)

# COMMAND ----------

# #sort on three columns
# ip1_sorted = ip1_joined.orderBy(col("SER_NUM"), col("ACTION_COUNT"), col("CATEGORY"))

# pointless sort

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import lag

#ading new columns based on formula
win_num = Window.partitionBy("SER_NUM").orderBy("SER_NUM", "ACTION_COUNT", "CATEGORY", "FK_WRKR_ID", "FP_ID", "TRANSACTION_NO", "TRANSACTIONAL_LITERAL", "COMPLETED_DT", "FK_FP_GROUP_ID")
win_denom = Window.partitionBy().orderBy("SER_NUM", "ACTION_COUNT", "CATEGORY", "FK_WRKR_ID", "FP_ID", "TRANSACTION_NO", "TRANSACTIONAL_LITERAL", "COMPLETED_DT", "FK_FP_GROUP_ID")

ip1_category = ip1_joined.withColumn(
    "FA_COUNT_NUM", when((lag("CATEGORY",1,'').over(win_num) != col("CATEGORY")) & (col("ACTION_COUNT") == "1"), 1).otherwise(0)
).withColumn(
    "FA_COUNT_DENOM", when((lag("SER_NUM",1,0).over(win_denom) != col("SER_NUM")) & (col("ACTION_COUNT") == "1"), 1).otherwise(0)
).withColumn(
    "ser_num", col("ser_num").astype(IntegerType())
)

# COMMAND ----------

max_comp_dt = ip1_select.groupBy().agg(max(col("completed_dt"))).collect()[0][0]
min_comp_dt = ip1_select.groupBy().agg(min(col("completed_dt"))).collect()[0][0]

# COMMAND ----------


# #Ip1 Summarize data
# from pyspark.sql.functions import sum as _sum
# ip1_group_null_key = (
#     ip1_select
#     .groupBy()
#     .agg(max(col("COMPLETED_DT")).alias("Max_COMPLETED_DT"), \
#         min(col("COMPLETED_DT")).alias("Min_COMPLETED_DT"))
# )


# COMMAND ----------

ip2_df = ip2_df.withColumn(
    "max_completed_dt", lit(max_comp_dt)
).withColumn(
    "min_completed_dt", lit(min_comp_dt)
)

# COMMAND ----------

# #ip2_df
# #ip1_group_null_key
# ip2_df1 = ip2_df.withColumn("key",lit("1"))
# ip1_group_null_key1 = ip1_group_null_key.withColumn("key1",lit("1"))
# ip2_joined = \
# (
#     ip2_df1
#         .join(ip1_group_null_key1,
#              on = [col("key") == col("key1")],
#              how = "left"
#              )
#         .select(col("ser_num"),
#                 col("first_action_dt_ph"),
#                 col("am_1_actn_ct_dt"),
#                 col("first_action_type"),
#                 col("filing_dt"),
#                 col("ib_notification_dt"),
#                 col("published_dt"),
#                 col("noa_dt"),
#                 col("abandonment_dt"),
#                 col("aban_dt_ph"),
#                 col("registration_dt"),
#                 col("disposal_type"),
#                 col("ext1_dt"),
#                 col("ext2_dt"),
#                 col("ext3_dt"),
#                 col("ext4_dt"),
#                 col("ext5_dt"),
#                 col("cancellation_dt"),
#                 col("renewal_dt"),
#                 col("revival_dt"),
#                 col("susp_check_dt"),
#                 col("am_cls_ct_actv"),
#                 col("pendency_cal_start_dt"),
#                 col("pendency_cal_end_dt"),
#                 col("noa_registration_check"),
#                 col("wgtd_1st_actn_pendency"),
#                 col("first_action_cd"),
#                 col("disposal_pendency"),
#                 col("suspension"),
#                 col("ttab"),
#                 col("disposal_dt"),
#                 col("dock_dt"),
#                 col("am_flg_66a_cur"),
#                 col("am_flg_66a_fil"),
#                 col("noa_dt_ph"),
#                 col("filing_fy"),
#                 col("non_pro_se"),
#                 col("first_action_pendency_ph"),
#                 col("last_modified_date"),
#                 col("Max_COMPLETED_DT"),
#                 col("Min_COMPLETED_DT")
# )
# )   



# COMMAND ----------

#filter ip2 on min and max completion date
ip2_filter = ip2_df.filter((col("first_action_dt_ph").isNotNull()) &
                                (col("first_action_dt_ph") <= col("Max_COMPLETED_DT")) &
                                 (col("first_action_dt_ph") >= col("Min_COMPLETED_DT")))

# COMMAND ----------

#Select required field

ip2_select = ip2_filter.select(col("ser_num").astype(IntegerType()).alias("SER_NUM"), "first_action_dt_ph")

# COMMAND ----------

#ip1_category
#ip2_select

ip12_joined = ip1_category.join(ip2_select, on = "ser_num").select(col("SER_NUM"),
                col("GROUP_NAME"),
                col("COMPLETED_DT"),
                col("TRANSACTIONAL_LITERAL"),
                col("ACTION_COUNT"),
                col("FK_WRKR_ID"),
                col("FP_ID"),
                col("TITLE_TX"),
                col("FK_FP_GROUP_ID"),
                col("FK_FP_CATEGORY_ID"),
                col("CATEGORY"),
                col("FP_YEAR"),
                col("COMPLETED_TS"),
                col("TM_ANALYTICS_TS"),
                col("TOC"),
                col("Concat_FP_ID"),
                col("Concat_CATEGORY"),
                col("TRANSACTION_NO"),
                col("FA_COUNT_NUM"),
                col("FA_COUNT_DENOM")
)

# COMMAND ----------

ip2_antijoin = ip2_select.join(ip1_category, "ser_num", "leftanti") 

# COMMAND ----------

ip2_antijoin_formula = ip2_antijoin.withColumn(
    "FA_COUNT_DENOM", lit(1)
).withColumnRenamed(
    "first_action_dt_ph", "COMPLETED_DT"
)

# COMMAND ----------

#union two dataframes, i.e. Inner join and anti join
ip12_union = ip12_joined.unionByName(ip2_antijoin_formula, allowMissingColumns=True)

# COMMAND ----------

#ip3 select required columns

ip3_select = ip3_df.select(col("SER_NUM"),
                           col("LAW_OFFICE"),
                           col("FILING_BASIS_GRP"))


# COMMAND ----------

#ip3 formula to populate filing basis group
ip3_formula = ip3_select.withColumn("FILING_BASIS_GRP",when((col("FILING_BASIS_GRP").contains("MULTIPLE")) , "MULTI-BASIS")
                                    .otherwise(col("FILING_BASIS_GRP")))

# COMMAND ----------

#ip12_union
#ip3_formula

ip123_joined = \
(
    ip12_union
        .join(ip3_formula,
             on = "ser_num",
             how = "left"
             )
        .select(col("SER_NUM"),
                col("GROUP_NAME"),
                col("COMPLETED_DT"),
                col("TRANSACTIONAL_LITERAL"),
                col("ACTION_COUNT"),
                col("FK_WRKR_ID"),
                col("FP_ID"),
                col("TITLE_TX"),
                col("FK_FP_GROUP_ID"),
                col("FK_FP_CATEGORY_ID"),
                col("CATEGORY"),
                col("FP_YEAR"),
                col("COMPLETED_TS"),
                col("TM_ANALYTICS_TS"),
                col("TOC"),
                col("Concat_FP_ID"),
                col("Concat_CATEGORY"),
                col("TRANSACTION_NO"),
                col("FA_COUNT_NUM"),
                col("FA_COUNT_DENOM"),
                col("LAW_OFFICE"),
                col("FILING_BASIS_GRP")
)
)   



# COMMAND ----------

#formula to create new fields

ip123_formula = ip123_joined.withColumn("Exam",when((col("TRANSACTION_NO") == 6129) | (col("TRANSACTION_NO") == 6130) | (col("TRANSACTION_NO") == 6125) | (col("TRANSACTION_NO") == 6124) , "SU")
                                      .when((col("TRANSACTION_NO") == 6329) | (col("TRANSACTION_NO") == 6325) | (col("TRANSACTION_NO") == 6326) | (col("TRANSACTION_NO") == 6330) , "Initial")
                                      .otherwise("Other Exam")) \
                                          .withColumn("Action Type",when(col("ACTION_COUNT") == 1 , "First Action")
                                                      .when((col("TRANSACTION_NO") == 6129) | (col("TRANSACTION_NO") == 6130) | (col("TRANSACTION_NO") == 6329) | (col("TRANSACTION_NO") == 6124) | (col("TRANSACTION_NO") == 6330) , "Final Action")
                                                      .otherwise("Other Action")) \
                                                          .withColumn("Completed Date Year",year(col("COMPLETED_DT").cast(DateType()))) \
                                                              .withColumn("Completed Date Fiscal Year",year(add_months(col("COMPLETED_DT").cast(DateType()),3))) \
                                                                  .withColumn("Action Type 2 possible fix",when(col("ACTION_COUNT") == 1 , "First Action")
                                                      .when((col("TRANSACTION_NO") == 6129) | (col("TRANSACTION_NO") == 6130) | (col("TRANSACTION_NO") == 6329) | (col("TRANSACTION_NO") == 6124) | (col("TRANSACTION_NO") == 6330) , "Final Action")
                                                      .otherwise("Other Action"))

# COMMAND ----------

#filter the records on group name

ip123_filter = ip123_formula.filter(~col("GROUP_NAME").eqNullSafe("Deleted Form Paragraph"))

# COMMAND ----------

#summarize data to get max completed date
ip123_group_null_key = (
    ip123_filter
    .groupBy()
    .agg(max(col("COMPLETED_DT")).alias("Max_COMPLETED_DT"))
)

# COMMAND ----------

#ip123_filter
#ip123_group_null_key
ip123_filter1 = ip123_filter.withColumn("key",lit("1"))
ip123_group_null_key1 = ip123_group_null_key.withColumn("key1",lit("1"))
ip123_max_dt_joined = \
(
    ip123_filter1
        .join(ip123_group_null_key1,
             on = [col("key") == col("key1")],
             how = "left"
             )
        .select(col("Max_COMPLETED_DT"),
                col("SER_NUM"),
                col("GROUP_NAME"),
                col("COMPLETED_DT"),
                col("TRANSACTIONAL_LITERAL"),
                col("ACTION_COUNT"),
                col("FK_WRKR_ID"),
                col("FP_ID"),
                col("TITLE_TX"),
                col("FK_FP_GROUP_ID"),
                col("FK_FP_CATEGORY_ID"),
                col("CATEGORY"),
                col("FP_YEAR"),
                col("COMPLETED_TS"),
                col("TM_ANALYTICS_TS"),
                col("TOC"),
                col("Concat_FP_ID"),
                col("Concat_CATEGORY"),
                col("TRANSACTION_NO"),
                col("FA_COUNT_NUM"),
                col("FA_COUNT_DENOM"),
                col("LAW_OFFICE"),
                col("FILING_BASIS_GRP"),
                col("Exam"),
                col("Action Type"),
                col("Completed Date Year"),
                col("Completed Date Fiscal Year"),
                col("Action Type 2 possible fix")
)
)   


# COMMAND ----------

#select required columns

ip123_max_dt_select = ip123_max_dt_joined.select(col("Max_COMPLETED_DT").alias("Data Through Date"),
                                                col("SER_NUM").alias("Serial Number"),
                                                col("GROUP_NAME").alias("Group Name"),
                                                col("COMPLETED_DT").alias("Completed Date"),
                                                col("TRANSACTIONAL_LITERAL").alias("Transaction Literal"),
                                                col("ACTION_COUNT").alias("Action Count"),
                                                col("FK_WRKR_ID").alias("Foreign Key Worker ID"),
                                                col("FP_ID").alias("Form Paragraph ID"),
                                                col("TITLE_TX").alias("Title Text"),
                                                col("FK_FP_GROUP_ID").alias("Foreign Key Form Paragraph Group ID"),
                                                col("FK_FP_CATEGORY_ID").alias("Foreign Key Form Paragraph Category ID"),
                                                col("CATEGORY").alias("Category"),
                                                col("FP_YEAR").alias("Form Paragraph Year"),
                                                col("TOC").alias("TOC link"),
                                                col("Concat_FP_ID").alias("Concat Form Paragraph ID"),
                                                col("Concat_CATEGORY").alias("Concat Category"),
                                                col("TRANSACTION_NO").alias("Transaction Number"),
                                                col("FA_COUNT_NUM").alias("First Action Count Numerator"),
                                                col("FA_COUNT_DENOM").alias("First Action Count Denominator"),
                                                col("LAW_OFFICE"),
                                                col("FILING_BASIS_GRP").alias("Filing Basis Group"),
                                                col("Exam"),
                                                col("Action Type"),
                                                col("Completed Date Year"),
                                                col("Completed Date Fiscal Year"),
                                                col("Action Type 2 possible fix"))

# COMMAND ----------

ip4_df1 = ip4_df.withColumn("GRADE_START_DATE",col("GRADE_START_DT").cast(DateType())) \
    .withColumn("GRADE_END_DATE",col("GRADE_END_DT").cast(DateType()))

# COMMAND ----------


from pyspark.sql.functions import col, expr, posexplode, sequence

# Calculate the number of days between start_date and end_date
ip4_wip = ip4_df1.withColumn("Days_diff", expr("datediff(GRADE_END_DATE, GRADE_START_DATE) + 1"))

# Generate a sequence of integers from 0 to Days_diff - 1
ip4_wip = ip4_wip.withColumn("Day", expr("sequence(0, Days_diff - 1)"))

# # Explode the Day column to create separate rows
ip4_wip1 = ip4_wip.select("EMP_NO", "EMPLOYEE_NAME_FULL", "GRADE", "ORG_CD", "ORG_NM", "PAY_PERIOD_CLNDR_YR", "GRADE_START_DT", "GRADE_END_DT", "TM_ANALYTICS_TS", "GRADE_START_DATE", "GRADE_END_DATE", posexplode("Day").alias("Day_num", "Day"))

# # Calculate the new date by adding Day to the Start_date
ip4_gen_dt = ip4_wip1.withColumn("Generated Date", expr("date_add(cast(GRADE_START_DATE as date), Day)"))



# COMMAND ----------

#ip123_max_dt_select
#ip4_gen_dt


ip1234_joined = \
(
    ip123_max_dt_select
        .join(ip4_gen_dt,
             on = [col("Foreign Key Worker ID") == col("EMP_NO"),
                   col("Completed Date") == col("Generated Date")],
             how = "left"
             )
        .select(col("Generated Date"),
col("EMP_NO"),
col("EMPLOYEE_NAME_FULL"),
col("GRADE"),
col("ORG_CD"),
col("ORG_NM"),
col("PAY_PERIOD_CLNDR_YR"),
col("GRADE_START_DT"),
col("GRADE_END_DT"),
col("Data Through Date"),
col("Serial Number"),
col("Group Name"),
col("Completed Date"),
col("Transaction Literal"),
col("Action Count"),
col("Foreign Key Worker ID"),
col("Form Paragraph ID"),
col("Title Text"),
col("Foreign Key Form Paragraph Group ID"),
col("Foreign Key Form Paragraph Category ID"),
col("Form Paragraph Year"),
col("TOC link"),
col("Concat Form Paragraph ID"),
col("Concat Category"),
col("First Action Count Numerator"),
col("First Action Count Denominator"),
col("Filing Basis Group"),
col("Exam"),
col("Action Type"),
col("Completed Date Year"),
col("Completed Date Fiscal Year"),
col("TM_ANALYTICS_TS"),
col("Transaction Number"),
col("Action Type 2 possible fix"),
col("Category"),
col("LAW_OFFICE")
)   
)

# COMMAND ----------

#Ip1 Summarize grouping the data on SER_NUM, ACTION_COUNT
#from pyspark.sql.functions import sum as _sum
ip4_grouped = ip4_df.groupBy("EMP_NO").agg(min(col("PAY_PERIOD_CLNDR_YR")).alias("Min_PAY_PERIOD_CLNDR_YR"))

# COMMAND ----------

#ip1234_joined
#ip4_grouped

ip1234_group_joined = \
(
    ip1234_joined
        .join(ip4_grouped,
             on = "emp_no",
             how = "left"
             )
        .select(col("Generated Date"),
col("EMP_NO"),
col("EMPLOYEE_NAME_FULL"),
col("GRADE"),
col("ORG_CD"),
col("ORG_NM"),
col("PAY_PERIOD_CLNDR_YR"),
col("GRADE_START_DT"),
col("GRADE_END_DT"),
col("Data Through Date"),
col("Serial Number"),
col("Group Name"),
col("Completed Date"),
col("Transaction Literal"),
col("Action Count"),
col("Foreign Key Worker ID"),
col("Form Paragraph ID"),
col("Title Text"),
col("Foreign Key Form Paragraph Group ID"),
col("Foreign Key Form Paragraph Category ID"),
col("Form Paragraph Year"),
col("TOC link"),
col("Concat Form Paragraph ID"),
col("Concat Category"),
col("First Action Count Numerator"),
col("First Action Count Denominator"),
col("Filing Basis Group"),
col("Exam"),
col("Action Type"),
col("Completed Date Year"),
col("Completed Date Fiscal Year"),
col("TM_ANALYTICS_TS"),
col("Transaction Number"),
col("Action Type 2 possible fix"),
col("Category"),
col("LAW_OFFICE"),
col("Min_PAY_PERIOD_CLNDR_YR").alias("Employee Class")
)   
)

# COMMAND ----------

#Ip1 Summarize grouping the data on SER_NUM, ACTION_COUNT
#from pyspark.sql.functions import sum as _sum
ip5_grouped = (
    ip5_df
    .groupBy(
                col("SER_NUM"))
    .agg(
       min(col("PARTY_TYPE")).alias("Min_PARTY_TYPE")
)
)



# COMMAND ----------

ip5_df1 = ip5_df.withColumnRenamed("SER_NUM","Right_SER_NUM")

ip5_join_pty_typ = \
(
    ip5_df1
        .join(ip5_grouped,
             on = [col("Right_SER_NUM") == col("SER_NUM"),
                   col("PARTY_TYPE") == col("Min_PARTY_TYPE")],
             how = "inner"
             )
        .select(col("ser_num"),
                col("Min_PARTY_TYPE"),
                col("address_1"),
                col("address_2"),
                col("citizenship"),
                col("city"),
                col("country_or_area_name"),
                col("ctry_cd"),
                col("current_owner"),
                col("entity_type"),
                col("last_modified_date"),
                col("name"),
                col("owner_num"),
                col("party_type"),
                col("postal_cd"),
                col("ctry_nm"),
                col("Right_SER_NUM"),
                col("state_cd")
)
)   

# COMMAND ----------

#filter on ownernum
ip5_filter =ip5_join_pty_typ.filter(col("owner_num") == 1)

# COMMAND ----------

#ip1234_group_joined
#ip5_filter


ip12345_joined = \
(
    ip1234_group_joined
        .join(ip5_filter,
             on = [col("Serial Number") == col("ser_num")],
             how = "inner"
             )
        .select(col("Generated Date"),
col("EMP_NO"),
col("EMPLOYEE_NAME_FULL"),
col("GRADE"),
col("ORG_CD"),
col("ORG_NM"),
col("PAY_PERIOD_CLNDR_YR"),
col("GRADE_START_DT"),
col("GRADE_END_DT"),
col("Data Through Date"),
col("Serial Number"),
col("Group Name"),
col("Completed Date"),
col("Transaction Literal"),
col("Action Count"),
col("Foreign Key Worker ID"),
col("Form Paragraph ID"),
col("Title Text"),
col("Foreign Key Form Paragraph Group ID"),
col("Foreign Key Form Paragraph Category ID"),
col("Form Paragraph Year"),
col("TOC link"),
col("Concat Form Paragraph ID"),
col("Concat Category"),
col("First Action Count Numerator"),
col("First Action Count Denominator"),
col("Filing Basis Group"),
col("Exam"),
col("Action Type"),
col("Completed Date Year"),
col("Completed Date Fiscal Year"),
col("TM_ANALYTICS_TS"),
col("Transaction Number"),
col("Action Type 2 possible fix"),
col("Category"),
col("LAW_OFFICE"),
col("Employee Class"),
col("country_or_area_name"),
col("last_modified_date"),
col("state_cd")
)   
)

# COMMAND ----------

#rename fields

ip12345_select = ip12345_joined.select(col("Generated Date"),
col("EMP_NO").alias("Employee Number"),
col("EMPLOYEE_NAME_FULL").alias("Employee Full Name"),
col("GRADE").alias("Grade"),
col("ORG_CD").alias("Organization Code"),
col("ORG_NM").alias("Organization Name"),
col("PAY_PERIOD_CLNDR_YR").alias("Pay Period Calendar Year"),
col("GRADE_START_DT").alias("Grade Start Date"),
col("GRADE_END_DT").alias("Grade End Date"),
col("Data Through Date"),
col("Serial Number"),
col("Group Name"),
col("Completed Date"),
col("Transaction Literal"),
col("Action Count"),
col("Foreign Key Worker ID"),
col("Form Paragraph ID"),
col("Title Text"),
col("Foreign Key Form Paragraph Group ID"),
col("Foreign Key Form Paragraph Category ID"),
col("Form Paragraph Year"),
col("TOC link"),
col("Concat Form Paragraph ID"),
col("Concat Category"),
col("First Action Count Numerator"),
col("First Action Count Denominator"),
col("Filing Basis Group"),
col("Exam"),
col("Action Type"),
col("Completed Date Year"),
col("Completed Date Fiscal Year"),
col("TM_ANALYTICS_TS"),
col("Transaction Number"),
col("Action Type 2 possible fix"),
col("Category"),
col("LAW_OFFICE"),
col("Employee Class"),
col("country_or_area_name"),
col("last_modified_date"),
col("state_cd"))

# COMMAND ----------

#add new field

ip12345_formula = ip12345_select.withColumn("Record Output Date",current_timestamp())


# COMMAND ----------

#summarize on Record Output Date
#Ip1 Summarize grouping the data on SER_NUM, ACTION_COUNT
#from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import count as _count
ip12345_grouped = (
    ip12345_formula
    .groupBy(
                col("Record Output Date"))
    .agg(
       _count(col("*")).alias("Output Record Count")
)
)

ip12345_grouped = ip12345_grouped.withColumnRenamed("Record Output Date","record_output_date") \
    .withColumnRenamed("Output Record Count","output_record_count")

# COMMAND ----------

# add audit columns
ip12345_grouped = ip12345_grouped.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit("ETL")
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit("ETL")
)

# COMMAND ----------

#ip6_df.show()

#union two dataframes, i.e. Inner join and anti join
ip_count_union = ip12345_grouped.unionByName(ip6_df, allowMissingColumns=True)

# COMMAND ----------

ip_count_sorted = ip_count_union.orderBy(col("record_output_date").desc())

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import lag, lead

#ading new columns based on formula
window1 = Window.orderBy(col("record_output_date").desc())

ip_count_per = (ip_count_sorted.withColumn("record_output_percent_change",(col("output_record_count") - lead(col("output_record_count")).over(window1)) / lead(col("output_record_count")).over(window1))
)

# COMMAND ----------

window2 = Window.orderBy(col("record_output_date").desc())

ip_count_proc = (ip_count_per.withColumn("continue_process", when((col("output_record_count") >= lead(col("output_record_count")).over(window2)) & (col("record_output_percent_change") < lit(0.05)), 1)
                                           .otherwise(0)
))

# COMMAND ----------

# DBTITLE 1,count of current run
df_count_current = ip_count_proc.limit(1)

# COMMAND ----------

#ip_count_final.show()

# COMMAND ----------

# DBTITLE 1,Hyper Output: TM Form Paragraph Dashboard
tm_form_para_dash = ip12345_select.select(col("Generated Date").alias("Generated_Date"),
col("Category"),
col("Grade"),
col("Data Through Date").alias("Data_Through_Date"),
col("Serial Number").alias("Serial_Number"),
col("Group Name").alias("Group_Name"),
col("Completed Date").alias("Completed_Date"),
col("Transaction Literal").alias("Transaction_Literal"),
col("Action Count").alias("Action_Count"),
col("Form Paragraph ID").alias("Form_Paragraph_ID"),
col("Title Text").alias("Title_Text"),
col("Foreign Key Form Paragraph Group ID").alias("Foreign_Key_Form_Paragraph_Group_ID"),
col("Foreign Key Form Paragraph Category ID").alias("Foreign_Key_Form_Paragraph_Category_ID"),
col("Form Paragraph Year").alias("Form_Paragraph_Year"),
col("TOC link").alias("TOC_link"),
col("Concat Form Paragraph ID").alias("Concat_Form_Paragraph_ID"),
col("Concat Category").alias("Concat_Category"),
col("First Action Count Numerator").alias("First_Action_Count_Numerator"),
col("First Action Count Denominator").alias("First_Action_Count_Denominator"),
col("Filing Basis Group").alias("Filing_Basis_Group"),
col("Exam"),
col("Action Type").alias("Action_Type"),
col("Completed Date Year").alias("Completed_Date_Year"),
col("Completed Date Fiscal Year").alias("Completed_Date_Fiscal_Year"),
col("TM_ANALYTICS_TS"),
col("Transaction Number").alias("Transaction_Number"),
col("Action Type 2 possible fix").alias("Action_Type_2_possible_fix"),
col("LAW_OFFICE").alias("Law_Office"),
col("country_or_area_name"),
col("last_modified_date"),
col("state_cd"))\
                .withColumn("create_ts", current_timestamp())\
                .withColumn("create_user_id", lit("ETL"))\
                .withColumn("update_ts", current_timestamp())\
                .withColumn("update_user_id", lit("ETL"))

# COMMAND ----------

# DBTITLE 1,Count final dataframe
form_dash_count_df = df_count_current.unionByName(ip6_df, allowMissingColumns=True).select("record_output_date",
"output_record_count",
"record_output_percent_change",
"continue_process",
"create_ts",
"create_user_id",
"update_ts",
"update_user_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the dadafram into tables.

# COMMAND ----------

try:
    print("Writing table in gold level")
    tm_form_para_dash.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.form_paragraph_dashboard')
    print("Writing table in silver level")
    form_dash_count_df.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.silver.form_paragraph_counts')
    recs_count = tm_form_para_dash.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading form_paragraph_dashboard Table ")
    
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading form_paragraph_dashboard Table ")

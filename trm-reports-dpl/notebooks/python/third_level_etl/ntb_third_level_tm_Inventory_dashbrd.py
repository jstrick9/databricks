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
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
edw_scope = common_configs['secrets']['edw_scope']

print(reporting_catalog,tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_third_level_tm_Inventory_dashbrd'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

## This setting is for the "Determining location of DBIO file fragments. This operation can take some time." error
# spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Inputs
ip1_query = f'''SELECT CAST(SPLIT(WI.CFK_OBJECT_GID, ':')[2] AS INTEGER) AS ATH_SER_NUM, 
CASE WHEN AH.CFK_HOLD_STATUS_CD = 'ON_HOLD' THEN 1 ELSE 0 END AS ATH_ACTIVE_STATUS
FROM {tmngpdb_catalog}.bronze.ATTORNEY_HOLD AH 
LEFT JOIN {tmngpdb_catalog}.bronze.WORK_ITEM_OBJECT WI
ON WI.FK_WORK_ITEM_GID = AH.FK_WORK_ITEM_GID'''
ip1_atrny_hold = spark.sql(ip1_query)

ip_df2_tbl_biblo = spark.sql(f"""select * from {reporting_catalog}.silver.bibliography""")
ip_df3_tbl_milestone = spark.sql(f""" select * from {reporting_catalog}.silver.milestone""")
ip_df4_tbl_class = spark.sql(f"""select * from {reporting_catalog}.silver.class""")
ip_df5_tbl_unex_inv_hstr =spark.sql(f"""select * from {reporting_catalog}.gold.inventory_unexamined_hstry""")
ip_df6_tbl_tm_pend_dash =spark.sql(f"""select * from {reporting_catalog}.gold.pendency_dashboard""")
# ip_df7_org_cd = spark.sql(f"""select EMP_FMLY_NM, emp_grd,EMP_GVN_NM,EMP_NO,EMP_OCPTNL_SRS_CD,FK_PSTN_SPRVSRY_LVL,ORG_CD,ORG_CD as ORG_CD1  from hive_metastore.alteryx_etldb_dev.current_emp where ORG_CD Like '13%'""")
# ip_df8_fact_comp = spark.sql(f"""select EMP_NM,EMP_NO,ACCTG_ACT_NM,PAY_HR_NO,PAY_BDGT_FSCL_YR from hive_metastore.alteryx_etldb_dev.fact_compensation_temp where ORG_FOURTH_LVL_CD = '1331'""")

edw_query1= "select DW.CURRENT_EMP.*,DW.CURRENT_EMP.ORG_CD as ORG_CD1 from DW.CURRENT_EMP  where DW.CURRENT_EMP.ORG_CD Like '13%'"
ip_df7_org_cd= read_data_from_oracle_conn_dsu_cmn(edw_query1,edw_scope)

edw_query2="select FORECAST.FACT_COMPENSATION.*,FORECAST.FACT_COMPENSATION.ORG_FOURTH_LVL_CD as ORG_FOURTH_LVL_CD1 from FORECAST.FACT_COMPENSATION where FORECAST.FACT_COMPENSATION.ORG_FOURTH_LVL_CD = '1331'"
ip_df8_fact_comp= read_data_from_oracle_conn_dsu_cmn(edw_query2,edw_scope)


# COMMAND ----------

# DBTITLE 1,Input 1
atrny_hold_select11 = ip1_atrny_hold.select(col("ATH_ACTIVE_STATUS"),
                                            col("ATH_SER_NUM").cast(StringType()))

atrny_hold_filter9 = atrny_hold_select11.filter(col("ATH_ACTIVE_STATUS") == 1) \
    .withColumn("On_Hold",lit(1))

# COMMAND ----------

# DBTITLE 1,ip1 atrny hold and ip2 bibliography join
bib_atrny_join10 = (
    ip_df2_tbl_biblo
    .join(
        atrny_hold_filter9,
        (ip_df2_tbl_biblo["SER_NUM"] == atrny_hold_filter9["ATH_SER_NUM"]),
        "left",
    )
    .selectExpr(
        "On_Hold",
        "SER_NUM",
        "TEST_PCTRAM_LINK",
        "LAW_OFFICE",
        "FILING_BASIS_CUR",
        "FILING_METHOD_FILED",
        "FILING_METHOD_CUR",
        "FILING_BASIS_FIL",
        "FILING_BASIS_AMED",
        "REGISTRATION_NUMBER",
        "AM_FLG_66A_FIL",
        "AM_FLG_44D_FIL",
        "AM_FLG_44E_FIL",
        "FLG_PAPER_FIL",
        "AM_STAT",
        "AM_FLG_NO_BAS_FIL",
        "AM_FLG_TEASRF_FIL",
        "AM_FLG_USE_FIL",
        "AM_FLG_ITU_FIL",
        "AM_FLG_TEASPL_FIL",
        "LAST_MODIFIED_DATE as Right_LAST_MODIFIED_DATE",
        "FILING_BASIS_GRP",
        "MARK_DWG_CD",
        "MARK_DWG_DESC",
        "MARK_NM_SHORT",
        "MARK_NM",
        "TMNG_IMAGE_LINK",
        "TM_ANALYTICS_TS",
        "EXMR_EID",
        "LAST_MODIFIED_DATE"
    )
)

bib_atrny_form173 = bib_atrny_join10.withColumn("On_Hold",when(bib_atrny_join10.On_Hold.isNull(),lit(0))
                            .otherwise(bib_atrny_join10.On_Hold))

# COMMAND ----------

# MAGIC %md
# MAGIC ## EA Counts

# COMMAND ----------

# DBTITLE 1,Enhancing Employee Data Processing in Python
org_cd_select150 = ip_df7_org_cd.select(col("EMP_FMLY_NM"),
                     col("EMP_GRD"),
                     col("EMP_GVN_NM"),
                     col("EMP_NO"),
                     col("EMP_OCPTNL_SRS_CD"),
                     col("FK_PSTN_SPRVSRY_LVL")).filter(col("EMP_OCPTNL_SRS_CD") == "0905")

org_cd_select149 = org_cd_select150.filter((col("EMP_GRD") != "15") & (col("EMP_GRD") != "00"))

# fact_comp_sumrz164 = ip_df8_fact_comp.groupBy().agg(max("PAY_BDGT_FSCL_YR").alias("Max_PAY_BDGT_FSCL_YR"))
## New Code added####
fact_comp_sumrz164 = ip_df8_fact_comp.select(max(ip_df8_fact_comp.PAY_BDGT_FSCL_YR).alias("Max_PAY_BDGT_FSCL_YR"))

fact_comp_join165 = (
    ip_df8_fact_comp
    .join(
        fact_comp_sumrz164,
        (ip_df8_fact_comp["PAY_BDGT_FSCL_YR"] == fact_comp_sumrz164["Max_PAY_BDGT_FSCL_YR"]),
        "inner",
    )
)

#############
# 6/28 bug fix: removing emp_nm from groupBys to prevent name changes generating duplicates. EX women who get married during the year and last name changes but emp_no stays the same.
#############


fact_comp_sumrz151 = fact_comp_join165.groupBy("EMP_NO","ACCTG_ACT_NM").agg(sum("PAY_HR_NO").alias("Sum_PAY_HR_NO")).filter(col("Sum_PAY_HR_NO") > 0)

fact_comp_sumrz153 = fact_comp_sumrz151.groupBy("EMP_NO").agg(sum("Sum_PAY_HR_NO").alias("Total_Hours")).drop("EMP_NM").withColumnRenamed("EMP_NO","Right_EMP_NO")

fact_comp_find156 = (
    fact_comp_sumrz151
    .join(
        fact_comp_sumrz153,
        (fact_comp_sumrz151["EMP_NO"] == fact_comp_sumrz153["Right_EMP_NO"]),
        "left",
    ).drop("Right_EMP_NO")
)

# COMMAND ----------

fact_comp_filter155 = fact_comp_find156.filter((col("ACCTG_ACT_NM") == "EXAMINE APPLICATIONS - EXAMINING ATTORNEY") | (col("ACCTG_ACT_NM") == "TM ACADEMY EXAMINATION"))

from pyspark.sql.functions import round
# fact_comp_frm154 = fact_comp_filter155.withColumn("percent_examine",(fact_comp_filter155.Sum_PAY_HR_NO / fact_comp_filter155.Total_Hours) * 100).withColumnRenamed("EMP_NO","Right_EMP_NO")
fact_comp_frm154 = fact_comp_filter155.withColumn("percent_examine",round((fact_comp_filter155.Sum_PAY_HR_NO / fact_comp_filter155.Total_Hours) * 100)).withColumnRenamed("EMP_NO","Right_EMP_NO")

org_fact_inner157 = (
    org_cd_select149
    .join(
        fact_comp_frm154,
        (org_cd_select149["EMP_NO"] == fact_comp_frm154["Right_EMP_NO"]),
        "inner",
    )
)


org_fact_right = (
    fact_comp_frm154
    .join(
        org_cd_select149,
        (org_cd_select149["EMP_NO"] == fact_comp_frm154["Right_EMP_NO"]),
        "anti",
    ).select(#"EMP_NM",
                "Right_EMP_NO",
                "ACCTG_ACT_NM",
                "Sum_PAY_HR_NO",
                "Total_Hours",
                "percent_examine")
)



org_fact_left = (
    org_cd_select149
    .join(
        fact_comp_frm154,
        (org_cd_select149["EMP_NO"] == fact_comp_frm154["Right_EMP_NO"]),
        "anti",
    ).select(
        "EMP_FMLY_NM",
        "EMP_GRD",
        "EMP_GVN_NM",
        "EMP_NO",
        "EMP_OCPTNL_SRS_CD",
        "FK_PSTN_SPRVSRY_LVL"
    )
)

# COMMAND ----------

# DBTITLE 1,Spark Employee Examination Summary Logic
org_fact_left_sumrz158 = org_fact_left.select("EMP_NO").distinct()

org_fact_inlt50_filter159 = org_fact_inner157.filter(col("percent_examine") < 50) #True condition

org_fact_inlt50_sumrz160 = org_fact_inlt50_filter159.select("EMP_NO").distinct()

# org_fact_union161 = org_fact_left_sumrz158.union(org_fact_inlt50_sumrz160).groupBy().agg(countDistinct("EMP_NO").alias("EA_Not_Exam")).withColumn("key",lit("1"))
org_fact_union161 = org_fact_left_sumrz158.union(org_fact_inlt50_sumrz160)
# org_fact_sumrz162 = org_fact_union161.select(countDistinct("EMP_NO").alias("EA_Not_Exam")).withColumn("key",lit("1"))
org_fact_sumrz162 = org_fact_union161.distinct().agg(count("EMP_NO").alias("EA_Not_Exam")).withColumn("key",lit("1"))
org_fact_ingt50_filter159 = org_fact_inner157.filter(col("percent_examine") >= 50) ### This should be the False statment for org_fact_inlt50_filter159

# org_fact_r8gt50_filter168 = org_fact_right157.filter(col("percent_examine") >= 50)
org_fact_r8gt50_filter168 = org_fact_right.filter(col("percent_examine") >= 50)
org_fact_r8gt50_filter168 = org_fact_r8gt50_filter168.withColumn("EMP_NO",col("Right_EMP_NO"))

# org_fact_union169 = org_fact_ingt50_filter159.unionByName(org_fact_r8gt50_filter168, allowMissingColumns=True).groupBy().agg(countDistinct("EMP_NO").alias("EA_Examining")).withColumn("key1",lit("1"))
# New code added -- 
org_fact_union169 = org_fact_ingt50_filter159.unionByName(org_fact_r8gt50_filter168, allowMissingColumns=True)


org_fact_sumrz163 = org_fact_union169.select(countDistinct("EMP_NO").alias("EA_Examining")).withColumn("key1",lit("1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: EA Counts

# COMMAND ----------

org_fact_append166= (
    org_fact_sumrz162
    .join(
        org_fact_sumrz163,
        (org_fact_sumrz162["key"] == org_fact_sumrz163["key1"]),
        "left",
    ).select(col("EA_Not_Exam"),
             col("EA_Examining")) \
    .withColumn("create_ts", current_timestamp())\
                .withColumn("create_user_id", lit("-1"))\
                .withColumn("update_ts", current_timestamp())\
                .withColumn("update_user_id", lit("-1")))

org_fact_append166=org_fact_append166.select(
    col("EA_Not_Exam").cast(IntegerType()),
    col("EA_Examining").cast(IntegerType()),
    col("create_ts"),
    col("create_user_id"),
    col("update_ts"),
    col("update_user_id"),

)

## hyper tableau server: TM Inventory Dashboard EA Counts --Done
org_fact_append166.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_dashboard_ea_counts')

## Read back ea counts to use in other workflows
ea_counts = spark.sql(f"select * from {reporting_catalog}.gold.inventory_dashboard_ea_counts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Running

# COMMAND ----------

# DBTITLE 1,join ip1, biblio, milestone 

bib_atrny_filter87 = bib_atrny_form173.filter((col("AM_STAT") == 630) | (col("AM_STAT") == 638))\
    .withColumnRenamed("SER_NUM","bib_ser_num")\
        .withColumnRenamed("Right_LAST_MODIFIED_DATE","Left_Right_LAST_MODIFIED_DATE")

milestone_filter86 = ip_df3_tbl_milestone.filter(col("first_action_dt_ph").isNull())\
    .withColumnRenamed("SER_NUM","mil_ser_num")\
        .withColumnRenamed("AM_FLG_66A_FIL","Right_AM_FLG_66A_FIL")\
            .withColumnRenamed("LAST_MODIFIED_DATE","Right_LAST_MODIFIED_DATE")

abib_mil_join88 = (
    bib_atrny_filter87
    .join(
        milestone_filter86,
        (bib_atrny_filter87["bib_ser_num"] == milestone_filter86["mil_ser_num"]),
        "inner",
    )
    .selectExpr(
        "On_Hold",
        "bib_ser_num as SER_NUM",
        "TEST_PCTRAM_LINK",
        "LAW_OFFICE",
        "FILING_BASIS_CUR",
        "FILING_METHOD_FILED",
        "FILING_METHOD_CUR",
        "FILING_BASIS_FIL",
        "FILING_BASIS_AMED",
        "REGISTRATION_NUMBER",
        "AM_FLG_66A_FIL",
        "AM_FLG_44D_FIL",
        "AM_FLG_44E_FIL",
        "FLG_PAPER_FIL",
        "AM_STAT",
        "AM_FLG_NO_BAS_FIL",
        "AM_FLG_TEASRF_FIL",
        "AM_FLG_USE_FIL",
        "AM_FLG_ITU_FIL",
        "AM_FLG_TEASPL_FIL",
        "Left_Right_LAST_MODIFIED_DATE",
        "FILING_BASIS_GRP",
        "MARK_DWG_CD",
        "MARK_DWG_DESC",
        "MARK_NM_SHORT",
        "MARK_NM",
        "TMNG_IMAGE_LINK",
        "TM_ANALYTICS_TS",
        "EXMR_EID",
        "LAST_MODIFIED_DATE",
        "mil_ser_num as Right_SER_NUM",
        "first_action_dt_ph",
        "am_1_actn_ct_dt",
        "first_action_type",
        "filing_dt",
        "ib_notification_dt",
        "published_dt",
        "noa_dt",
        "abandonment_dt",
        "aban_dt_ph",
        "registration_dt",
        "disposal_type",
        "ext1_dt",
        "ext2_dt",
        "ext3_dt",
        "ext4_dt",
        "ext5_dt",
        "cancellation_dt",
        "renewal_dt",
        "revival_dt",
        "susp_check_dt",
        "am_cls_ct_actv",
        "pendency_cal_start_dt",
        "pendency_cal_end_dt",
        "noa_registration_check",
        "wgtd_1st_actn_pendency",
        "first_action_cd",
        "disposal_pendency",
        "suspension",
        "ttab",
        "disposal_dt",
        "dock_dt",
        "am_flg_66a_cur",
        "Right_AM_FLG_66A_FIL",
        "noa_dt_ph",
        "filing_fy",
        "non_pro_se",
        "first_action_pendency_ph",
        "Right_LAST_MODIFIED_DATE",

    )
)

# COMMAND ----------

# DBTITLE 1,ip4 Class
#from pyspark.sql.functions import count as _count
class_filter89 = ip_df4_tbl_class.filter((col("Class_Status") == "ACTIVE") | (col("Class_Status") == "FEE WAIVED") | (col("Class_Status") == "Partially Paid"))\
    .withColumnRenamed("SER_NUM","Class_SER_NUM")

class_sumrz90 = class_filter89.groupBy("Class_SER_NUM").agg(count(col("Class")).alias("Class_Count"))

# COMMAND ----------

# DBTITLE 1,Ip123 ip1, biblio, milestone  and ip4 class
bib_mil_cls_join91 = (
    abib_mil_join88
    .join(
        class_sumrz90,
        (abib_mil_join88["SER_NUM"] == class_sumrz90["Class_SER_NUM"]),
        "inner",
    )
    .selectExpr(
        "On_Hold",
        "SER_NUM",
        "pendency_cal_start_dt",
        "Class_Count",

    )
)

# COMMAND ----------

#from pyspark.sql.functions import sum as _sum
bib_mil_cls_sumrz92 = bib_mil_cls_join91.groupBy(col("Pendency_Cal_Start_DT"),
    col("On_Hold")).agg(sum(col("Class_Count")).alias("Class_Count"))

bib_mil_cls_sort93 = bib_mil_cls_sumrz92.orderBy("Pendency_Cal_Start_DT")

# COMMAND ----------



bib_mil_cls_formula114 = bib_mil_cls_sort93.withColumn("FY",when(month(bib_mil_cls_sort93.Pendency_Cal_Start_DT) > 9, (year(bib_mil_cls_sort93.Pendency_Cal_Start_DT) + 1))
                                                       .otherwise(year(bib_mil_cls_sort93.Pendency_Cal_Start_DT)))

bib_mil_cls_sumrz94 = bib_mil_cls_formula114.groupBy().agg(max("FY").alias("Current_FY"), \
                                                            max("Pendency_Cal_Start_DT").alias("Max_Pendency_Cal_Start_DT"))

bib_mil_cls_formula95 = bib_mil_cls_sumrz94.withColumn("End_Dt",add_months(to_date(concat(col('current_fy'),lit("-09-30"))),60))


# COMMAND ----------


#from pyspark.sql.functions import col, expr, posexplode, sequence



# Calculate the number of days between start_date and end_date
wip = bib_mil_cls_formula95.withColumn("Days_diff", expr("datediff(End_Dt, Max_Pendency_Cal_Start_DT) + 1"))

# Generate a sequence of integers from 0 to Days_diff - 1
wip1 = wip.withColumn("Day", expr("sequence(1, Days_diff - 1)"))

# # Explode the Day column to create separate rows
wip2 = wip1.select("Current_FY", "Max_Pendency_Cal_Start_DT", "End_Dt", posexplode("Day").alias("Day_num", "Day"))

# # Calculate the new date by adding Day to the Start_date
bib_mil_cls_gen_dt96 = wip2.withColumn("Date", expr("date_add(cast(Max_Pendency_Cal_Start_DT as date), Day)"))

bib_mil_cls_select97 = bib_mil_cls_gen_dt96.select(col("Date").alias("Pendency_Cal_Start_DT"))



# COMMAND ----------

bib_mil_cls_union98 = bib_mil_cls_sort93.unionByName(bib_mil_cls_select97, allowMissingColumns=True)

bib_mil_cls_null99 = bib_mil_cls_union98.filter(col("Class_Count").isNull())
#filter112 and filter99 false are combined in below row
bib_mil_cls_notnull99 = bib_mil_cls_union98.filter(col("Class_Count").isNotNull() & col("Pendency_Cal_Start_DT").isNotNull())


# COMMAND ----------

# DBTITLE 1,Join class_sumrz90 ip_df3_tbl_milestone

cls_mil_join100 = (
    class_sumrz90
    .join(
        ip_df3_tbl_milestone,
        (class_sumrz90["Class_SER_NUM"] == ip_df3_tbl_milestone["ser_num"]),
        "inner",
    )
    .selectExpr(

        "Class_Count",
        "pendency_cal_start_dt",
        "filing_fy"

    )
)

# cls_mil_sumrz101 = cls_mil_join100.groupBy().agg(max("Filing_FY").alias("Max_Filing_FY")) get rid of groupby 
cls_mil_sumrz101 = cls_mil_join100.select(max("Filing_FY").alias("Max_Filing_FY"))
cls_mil_formula102 = cls_mil_sumrz101.withColumn("Max_Filing_FY",(cls_mil_sumrz101.Max_Filing_FY.cast(IntegerType())) - 1)

# cls_mil_join100
# cls_mil_formula102

cls_mil_join103 = (
    cls_mil_join100
    .join(
        cls_mil_formula102,
        (cls_mil_join100["filing_fy"] == cls_mil_formula102["Max_Filing_FY"]),
        "inner",
    )
    .selectExpr(

        "Class_Count",
        "pendency_cal_start_dt",
        "filing_fy"

    )
)

# add_months(to_date(concat(year(current_timestamp().cast(DateType())),lit("-09-30"))),60)

cls_mil_frm105_strtdt = cls_mil_join103.withColumn("pendency_cal_start_dt",add_months(cls_mil_join103.pendency_cal_start_dt, 12))

cls_mil_sumrz104 = cls_mil_frm105_strtdt.groupBy("Pendency_Cal_Start_DT").agg(sum("Class_Count").alias("Class_Count2"))



# COMMAND ----------

# DBTITLE 1,find and replace 108
bil_mil_cls_formula107 = bib_mil_cls_null99.withColumn("date2",substring(bib_mil_cls_null99.Pendency_Cal_Start_DT.cast(StringType()),6,5)).withColumn("Count_Type",lit("Estimate"))

bil_mil_cls_formula106 = cls_mil_sumrz104.withColumn("date2_new",substring(cls_mil_sumrz104.Pendency_Cal_Start_DT.cast(StringType()),6,5)).withColumnRenamed("Pendency_Cal_Start_DT","Pendency_Cal_Start_DT_Right")

cls_mil_find_replace108 = (
    bil_mil_cls_formula107
    .join(
        bil_mil_cls_formula106,
        (bil_mil_cls_formula107["date2"] == bil_mil_cls_formula106["date2_new"]),
        "left",
    )
    .selectExpr(

        "pendency_cal_start_dt",
        "On_Hold",
        "Class_Count",
        "date2",
        "Count_Type",
        "Class_Count2"
    )
)

# COMMAND ----------

bil_mil_cls_frm_clscnt109 = cls_mil_find_replace108.withColumn("Class_Count",when(cls_mil_find_replace108.Class_Count.isNull(),cls_mil_find_replace108.Class_Count2)
                                   .otherwise(cls_mil_find_replace108.Class_Count)) \
                                       .drop("Class_Count2") \
                                           .drop("date2")

cmb_union111 = bib_mil_cls_notnull99.unionByName(bil_mil_cls_frm_clscnt109, allowMissingColumns=True)

# COMMAND ----------

# cmb_union111
# bib_mil_cls_sumrz94
cmb_union111 = cmb_union111.withColumn("key",lit("1"))
bib_mil_cls_sumrz94 = bib_mil_cls_sumrz94.withColumn("key1",lit("1"))
cmb_append116 = \
(
    cmb_union111
        .join(bib_mil_cls_sumrz94,
             on = [col("key") == col("key1")],
             how = "left"
             )
        .select(col("pendency_cal_start_dt"),
        col("On_Hold"),
        col("Class_Count"),
        col("Count_Type"),
        col("Current_FY")
)
)   



# COMMAND ----------

# new line added for multirow function
cmb_append116= cmb_append116.sort("pendency_cal_start_dt")
# partition = Window.orderBy("Class_Count")
partition = Window.orderBy("pendency_cal_start_dt")
cmb_multi_row118 = (cmb_append116.withColumn("Class_Count",when(cmb_append116.Class_Count.isNull(), lag(col("Class_Count")).over(partition))
                                         .otherwise(cmb_append116.Class_Count)))

cmb_formula113 = cmb_multi_row118.withColumn("Count_Type",when(cmb_multi_row118.Count_Type.isNull(), "Actual")
                                             .otherwise(cmb_multi_row118.Count_Type)) \
                                                 .withColumn("FY",when(month(cmb_multi_row118.pendency_cal_start_dt) > 9, (year(cmb_multi_row118.pendency_cal_start_dt) + 1))
                                                       .otherwise(year(cmb_multi_row118.pendency_cal_start_dt)))

cmb_formula113_1 = cmb_formula113.withColumn("FY_Plus",(cmb_formula113.FY - cmb_formula113.Current_FY))

# COMMAND ----------

cmb_filter119 = cmb_formula113_1.filter((col("On_Hold") == 0) & (col("Count_Type") == "Actual"))

from pyspark.sql.functions import sum as _sum
# cmp_sumrz120 = cmb_filter119.groupBy().agg(sum("Class_Count").alias("sum_Class_Count"))
cmp_sumrz120 = cmb_filter119.agg(sum("Class_Count").alias("sum_Class_Count"))

cmb_filter119 = cmb_filter119.withColumn("key",lit("1"))
cmp_sumrz120 = cmp_sumrz120.withColumn("key1",lit("1"))
cmb_append121 = \
(
    cmb_filter119
        .join(cmp_sumrz120,
             on = [col("key") == col("key1")],
             how = "left"
             )
        .select(col("pendency_cal_start_dt"),
        col("On_Hold"),
        col("Class_Count"),
        col("Count_Type"),
        col("Current_FY"),
        col("FY"),
        col("FY_Plus"),
        col("sum_Class_Count")
)
)   

# cmb_percent_frm122 = cmb_append121.withColumn("Percent",((cmb_append121.Class_Count / cmb_append121.sum_Class_Count) * 100)).orderBy("pendency_cal_start_dt")
cmb_percent_frm122 = cmb_append121.withColumn("Percent",round((cmb_append121.Class_Count / cmb_append121.sum_Class_Count) * 100,6)).orderBy("pendency_cal_start_dt")

partition = Window.orderBy("pendency_cal_start_dt")

# cmb_multi_row123 = cmb_percent_frm122.withColumn("Difference",cmb_percent_frm122.Percent - lag(col("Percent")).over(partition))
cmb_multi_row123 = cmb_percent_frm122.withColumn("Difference",round(cmb_percent_frm122.Percent - lag(cmb_percent_frm122.Percent).over(partition)))

# cmb_multi_frm125 = cmb_multi_row123.filter(col("Percent") >= 0.5) # wring value 
cmb_multi_frm125 = cmb_multi_row123.filter(col("Percent") >= 0.05)
# cmb_sumrz126 = cmb_multi_frm125.groupBy().agg(first("pendency_cal_start_dt").alias("Start_Non_Outlier"))
cmb_sumrz126 = cmb_multi_frm125.agg(first("pendency_cal_start_dt").alias("Start_Non_Outlier"))



# COMMAND ----------

# cmb_formula113_1
# cmb_sumrz126
cmb_formula113_1 = cmb_formula113_1.withColumn("key", lit("1"))
cmb_sumrz126 = cmb_sumrz126.withColumn("key1", lit("1"))
cmb_append127 = cmb_formula113_1.join(
    cmb_sumrz126, on=[col("key") == col("key1")], how="left"
).select(
    col("pendency_cal_start_dt"),
    col("On_Hold"),
    col("Class_Count"),
    col("Count_Type"),
    col("Current_FY"),
    col("FY"),
    col("FY_Plus"),
    col("Start_Non_Outlier"),
)

cmb_date2_frm134 = cmb_append127.withColumn(
    "date2",
    when(
        cmb_append127.Count_Type == "Estimate",
        substring(cmb_append127.pendency_cal_start_dt.cast(StringType()), 6, 5),
    ).otherwise(cmb_append127.pendency_cal_start_dt),
)

# COMMAND ----------

# cls_mil_sumrz185 = cls_mil_join100.groupBy().agg(
#     max("Filing_FY").alias("Max_Filing_FY")
# )
#from pyspark.sql.functions import sum as _sum
cls_mil_sumrz185 = cls_mil_join100.agg(max("Filing_FY").alias("Max_Filing_FY"))

cls_mil_sumrz184 = cls_mil_join100.groupBy(
    col("Filing_FY"), col("Pendency_Cal_Start_DT")
).agg(sum("Class_Count").alias("Sum_Class_Count"))


cls_mil_join186 = cls_mil_sumrz185.join(
    cls_mil_sumrz184, on=[col("Max_Filing_FY") == col("Filing_FY")], how="inner"
).select(
    col("Max_Filing_FY"),
    col("Filing_FY"),
    col("Pendency_Cal_Start_DT"),
    col("Sum_Class_Count"),
)

cls_mil_sumrz182 = cls_mil_join186.groupBy(
    col("Pendency_Cal_Start_DT"), col("Sum_Class_Count")
).count()
cls_mil_sumrz182 = (
    cls_mil_sumrz182.drop("count")
    .withColumn(
        "date2_new",
        substring(cls_mil_sumrz182.Pendency_Cal_Start_DT.cast(StringType()), 6, 5),
    )
    .withColumnRenamed("Pendency_Cal_Start_DT", "right_Pendency_Cal_Start_DT")
)

# cls_mil_sumrz182 = (
#     cls_mil_sumrz182.drop("count")
#     .withColumn(
#         "date2_new",
#         substring(cls_mil_sumrz182.Pendency_Cal_Start_DT.cast(StringType()), 6, 5),
#     )
#     .withColumnRenamed("Pendency_Cal_Start_DT", "right_Pendency_Cal_Start_DT")
# )

cls_mil_sumrz182 = (
    cls_mil_sumrz182.drop("count").withColumn("date2_new",date_format(col("right_Pendency_Cal_Start_DT"),"MM-dd")))

# COMMAND ----------

cls_mil_bib_join135_rgt = cmb_date2_frm134.join(
    # cls_mil_sumrz182, on=[col("date2") == col("date2_new")], how="right" ## Code changed joined condition changed
    cls_mil_sumrz182, on=[col("date2") == col("date2_new")], how="full_outer"

)
## new code has been introduce.
testdf_join135 = cls_mil_bib_join135_rgt.withColumn("Class_Count",when(cls_mil_bib_join135_rgt.Sum_Class_Count.isNull(),cls_mil_bib_join135_rgt.Class_Count)
                                   .otherwise(cls_mil_bib_join135_rgt.Sum_Class_Count))

# join_test = cls_mil_bib_join135_rgt1.exceptAll(cls_mil_bib_join135_inner)

# cls_mil_bib_select137= testdf_join135.select(col("Sum_Class_Count").alias("Class_Count"),
#                                                       col("pendency_cal_start_dt").alias("Pendency_Cal_Start_DT"),
#                                                       col("Count_Type"),
#                                                       col("Current_FY"),
#                                                       col("FY"),
#                                                       col("FY_Plus"),
#                                                       col("Start_Non_Outlier"),
#                                                       col("On_Hold"),
#                                                       col("date2"))
cls_mil_bib_select137= testdf_join135.select(
                                                      col("pendency_cal_start_dt").alias("Pendency_Cal_Start_DT"),
                                                      col("Class_Count"),
                                                      col("Count_Type"),
                                                      col("Current_FY"),
                                                      col("FY"),
                                                      col("FY_Plus"),
                                                      col("Start_Non_Outlier"),
                                                      col("On_Hold")
)

cls_mil_bib_frm128 = cls_mil_bib_select137.withColumn(
    "Outlier",
    when(
        col("Pendency_Cal_Start_DT")
        < col("Start_Non_Outlier"),
        1,
    ).otherwise(0),
).orderBy("Pendency_Cal_Start_DT")

# COMMAND ----------

cls_mil_bib_filter178 = cls_mil_bib_frm128.filter(col("Outlier") == 0)

cls_mil_bib_sumrz177 = cls_mil_bib_filter178.groupBy(
    col("Pendency_Cal_Start_DT"),
    col("Count_Type"),
    col("Current_FY"),
    col("FY"),
    col("FY_Plus"),
    col("Start_Non_Outlier"),
).agg(sum("Class_Count").alias("Class_Count"))
# cls_mil_bib_sumrz177 = cls_mil_bib_sumrz177.orderBy("Pendency_Cal_Start_DT")
partition = Window.orderBy("Pendency_Cal_Start_DT")

# cmb_multi_row123 = cmb_percent_frm122.withColumn("unTot_Class_Count",cmb_percent_frm122.Class_Count + lag(col("unTot_Class_Count")).over(partition))

# COMMAND ----------

cls_mil_bib_filter139 = cls_mil_bib_frm128.filter(col("Count_Type") == "Actual")

cls_mil_bib_sumrz138 = (
    cls_mil_bib_filter139.groupBy()
    .agg(sum("Class_Count").alias("Today_Unexamined"))
    .withColumn("key1", lit(1))
)

# COMMAND ----------

# my_window = Window.orderBy("Pendency_Cal_Start_DT").rowsBetween(
#     Window.unboundedPreceding, 0
# )

from pyspark.sql.window import Window

from pyspark.sql.window import Window
my_window = Window.orderBy("Pendency_Cal_Start_DT").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)
cls_mil_bib_running115 = cls_mil_bib_sumrz177.withColumn(
    "RunTot_Class_Count", sum("Class_Count").over(my_window)
).withColumn("key", lit(1))


ea_counts = (
    ea_counts.drop("create_ts")
    .drop("create_user_id")
    .drop("update_ts")
    .drop("update_user_id")
    .withColumn("key1", lit(1))
)

cls_mil_bib_org_append117 = (
    cls_mil_bib_running115.join(
        ea_counts, on=[col("key") == col("key1")], how="left"
    )
).drop("key1")

cls_mil_bib_org_append140 = (
    (
        cls_mil_bib_org_append117.join(
            cls_mil_bib_sumrz138, on=[col("key") == col("key1")], how="left"
        )
    )
    .drop("key1")
    .drop("key")
    .withColumn(
        "date2", substring(col("Pendency_Cal_Start_DT").cast(StringType()), 6, 5)
    )
)
cls_mil_bib_org_append140 = cls_mil_bib_org_append140.select(
    "Pendency_Cal_Start_DT",
    "Class_Count",
    "Count_Type",
    "Current_FY",
    "FY",
    "FY_Plus",
    "Start_Non_Outlier",
    "RunTot_Class_Count",
    "EA_Not_Exam",
    "EA_Examining",
    "Today_Unexamined",
    "date2"
)


# COMMAND ----------

cmb_filter131 = (
    cmb_append127.filter((col("Count_Type") == "Actual") & (col("FY_Plus") == 0))
    # .withColumn(
    #     "date2_new", substring(col("Pendency_Cal_Start_DT").cast(StringType()), 6, 5)
    # )
    # .withColumn("CurrentFY_CountType", lit("Actuals"))
    # .select(col("date2_new"), col("CurrentFY_CountType"))
)
# cls_mil_bib_sumrz138 = (
#     cls_mil_bib_filter139.groupBy()
#     .agg(sum("Class_Count").alias("Today_Unexamined"))
cmb_sumrz132 = cmb_filter131.select(
    col("pendency_cal_start_dt"), col("Class_Count")
).distinct()
cmb_filter133 = (
    cmb_sumrz132.withColumn(
        "date2_new", substring(col("Pendency_Cal_Start_DT").cast(StringType()), 6, 5)
    )
    .withColumn("CurrentFY_CountType", lit("Actuals"))
    .select(
        col("pendency_cal_start_dt").alias("pendency_cal_start_dt_new"),
        col("Class_Count").alias("Class_Count_new"),
        col("date2_new"),
        col("CurrentFY_CountType"),
    )
)
## This code added for duplicated values coming in Dataframe
cmb_filter133_1 = cmb_filter133.dropDuplicates(["date2_new"])

cmbo_find144 = (
    (
        cls_mil_bib_org_append140.join(
            cmb_filter133_1, on=[col("date2") == col("date2_new")], how="left"
        )
    )
    .drop("date2_new")
    .drop("date2")
    .drop("pendency_cal_start_dt_new")
    .drop("Class_Count_new")
    .withColumn(
        "CurrentFY_CountType",
        when(col("FY_Plus") <= 0, "CurrentOrPastFY")
        .when(col("CurrentFY_CountType") == "Actuals", "Actuals")
        .otherwise("Estimate"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: Inventory Running

# COMMAND ----------

tm_inv_dash_run_op = (
    cmbo_find144.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)
tm_inv_dash_run_op = tm_inv_dash_run_op.select(
col("Pendency_Cal_Start_DT"),
col("Class_Count").cast(IntegerType()),
col("Count_Type"),
col("Current_FY").cast(StringType()),
col("FY").cast(StringType()),
col("FY_Plus"),
col("Start_Non_Outlier"),
col("RunTot_Class_Count").cast(IntegerType()),
col("EA_Not_Exam").cast(IntegerType()),
col("EA_Examining").cast(IntegerType()),
col("Today_Unexamined").cast(IntegerType()),
col("CurrentFY_CountType"),
col("create_ts"),
col("create_user_id"),
col("update_ts"),
col("update_user_id"),
)

# COMMAND ----------

## write to delta table
tm_inv_dash_run_op.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_dashboard_running')

df_running = spark.sql(f"select * from {reporting_catalog}.gold.inventory_dashboard_running")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Filings

# COMMAND ----------

cls_mil_join195 = (
    (
        ip_df3_tbl_milestone.join(
            class_sumrz90, on=[col("SER_NUM") == col("Class_SER_NUM")], how="inner"
        ).select(
            col("SER_NUM"),
            col("Pendency_Cal_Start_DT"),
            col("Filing_FY"),
            col("Class_Count"),
        )
    )
    .groupBy("Pendency_Cal_Start_DT", "Filing_FY")
    .agg(sum("Class_Count").alias("Class_Count"))
    .withColumnRenamed("Filing_FY", "FY")
)

# cls_mil_sumrz205 = cls_mil_join195.groupBy().agg(max("FY").alias("Max_FY"))
cls_mil_sumrz205 = cls_mil_join195.select(max("FY").alias("Max_FY"))
cls_mil_join206 = (
    (
        cls_mil_join195.join(
            cls_mil_sumrz205, on=[col("FY") == col("Max_FY")], how="inner"
        )
    ).withColumnRenamed("Max_FY","Current_FY")
    
)

cls_mil_filter207 = cls_mil_join206 \
    .withColumn("Count_Type", lit("Actual")) \
    .withColumn("FY_Plus", lit(0)) \
    .withColumn("CurrentFY_CountType", lit("CurrentOrPastFY"))


# COMMAND ----------


cmbo_select209 = df_running.select("Pendency_Cal_Start_DT", "Class_Count","Count_Type","Current_FY","FY","FY_Plus","CurrentFY_CountType").filter(col("Count_Type") == "Estimate")
#cmbo_select209 = spark.sql ("select Pendency_Cal_Start_DT, Class_Count,Count_Type,Current_FY,FY,FY_Plus,CurrentFY_CountType from cmbo_find144 where Count_Type = 'Estimate'")

cmbo_union210 = cls_mil_filter207.unionByName(cmbo_select209)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: Inventory Filings

# COMMAND ----------

tm_inv_dash_fil = (
    cmbo_union210.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

tm_inv_dash_fil = tm_inv_dash_fil.select(
    col("Pendency_Cal_Start_DT"),
    col("Class_Count"),
    col("Count_Type"),
    col("Current_FY").cast(StringType()),
    col("FY").cast(StringType()),
    col("FY_Plus"),
    col("CurrentFY_CountType"),
    col("create_ts"),
    col("create_user_id"),
    col("update_ts"),
    col("update_user_id"))

## write to delta table
tm_inv_dash_fil.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_dashboard_filings')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Inventory Madrid

# COMMAND ----------

# ip_df6_tbl_tm_pend_dash.printSchema()

tm_pend_dash_sumrz72 = ip_df6_tbl_tm_pend_dash.groupBy().agg(
    max("fa_pendency_fy").alias("Max_FA_Pendency_FY")
)

tm_pend_dash_frm73 = tm_pend_dash_sumrz72.withColumn(
    "Max_Minus_One", tm_pend_dash_sumrz72.Max_FA_Pendency_FY.cast(IntegerType()) - 1
)

tm_pend_dash_join74 = (
    ip_df6_tbl_tm_pend_dash.join(
        tm_pend_dash_frm73,
        (
            ip_df6_tbl_tm_pend_dash["fa_pendency_fy"]
            == tm_pend_dash_frm73["Max_Minus_One"]
        ),
        "inner",
    )
).withColumnRenamed("last_modified_date", "Right_last_modified_date")

tm_pend_tot_sumrz78 = (
    tm_pend_dash_join74.groupBy()
    .agg(sum("Active_Classes_Disposal").alias("TOTAL_FA"))
    .withColumn("key", lit("1"))
)

tm_pend_dash_filter75 = tm_pend_dash_join74.filter(col("filing_basis_grp") == "MADRID")

tm_pend_madrid_sumrz77 = (
    tm_pend_dash_filter75.groupBy()
    .agg(sum("Active_Classes_Disposal").alias("MADRID_FA"))
    .withColumn("key1", lit("1"))
)

tm_pend_all_append78 = tm_pend_tot_sumrz78.join(
    tm_pend_madrid_sumrz77,
    (tm_pend_tot_sumrz78["key"] == tm_pend_madrid_sumrz77["key1"]),
    "left",
).select(col("TOTAL_FA"), col("MADRID_FA"))


# display(tm_pend_all_pct_frm79)

tm_pend_dash_filter75_1 = tm_pend_dash_filter75.withColumn("key", lit("1"))

tm_pend_dash_append81 = (
    tm_pend_dash_filter75_1.join(
        tm_pend_madrid_sumrz77,
        (tm_pend_dash_filter75_1["key"] == tm_pend_madrid_sumrz77["key1"]),
        "left",
    )
    .drop("key")
    .drop("key1")
)

# COMMAND ----------

tm_pend_all_pct_frm79 = tm_pend_all_append78.withColumn(
    "MADRID_PCT", tm_pend_all_append78.MADRID_FA / tm_pend_all_append78.TOTAL_FA
).withColumn("key", lit("1"))

tm_pend_dash_frm80 = tm_pend_dash_append81.withColumn(
    "Pendency",
    (
        tm_pend_dash_append81.active_classes_firstaction
        * tm_pend_dash_append81.first_action_pendency_ph
    )
    / tm_pend_dash_append81.MADRID_FA,
)

tm_pend_dash_sumrz82 = (
    tm_pend_dash_frm80.groupBy()
    .agg(sum("Pendency").alias("MADRID_FA_Pendency"))
    .withColumn("key1", lit("1"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ouptput: Inventory Madrid

# COMMAND ----------

tm_pend_all_append83 = (
    tm_pend_all_pct_frm79.join(
        tm_pend_dash_sumrz82,
        (tm_pend_all_pct_frm79["key"] == tm_pend_dash_sumrz82["key1"]),
        "left",
    )
    .select(col("MADRID_PCT"), col("MADRID_FA_Pendency"))
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

# write to delta table
tm_pend_all_append83.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_madrid')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory BD Occurrence

# COMMAND ----------

# ip_df6_tbl_tm_pend_dash.count() # checked 
ip_df6_tbl_tm_pend_dash= ip_df6_tbl_tm_pend_dash.withColumn("fa_pendency_fy",ip_df6_tbl_tm_pend_dash["fa_pendency_fy"].cast('int'))

# COMMAND ----------


# pend_dash_sumrz38 = ip_df6_tbl_tm_pend_dash.groupBy().agg(
#     max("fa_pendency_fy").alias("Max_FA_Pendency_FY")
# )
pend_dash_sumrz38 = ip_df6_tbl_tm_pend_dash.select(
    max("fa_pendency_fy").alias("Max_FA_Pendency_FY")
)
#pend_dash_sumrz38_1 = pend_dash_sumrz38.withColumn("Max_FA_Pendency_FY", pend_dash_sumrz38["Max_FA_Pendency_FY"].cast("int"))

pend_dash_sumrz36 = ip_df6_tbl_tm_pend_dash.groupBy(
    "fa_pendency_fy", "fa_pendency_fy_month"
).agg(sum("Active_Classes_FirstAction").alias("Sum_Active_Classes_FirstAction"))

org_fact_r8_right39 = pend_dash_sumrz38.join(
    pend_dash_sumrz36,
    (pend_dash_sumrz38["Max_FA_Pendency_FY"] == pend_dash_sumrz36["fa_pendency_fy"]),
    "right",
)

org_fact_r8_inner39 = pend_dash_sumrz38.join(
    pend_dash_sumrz36,
    (pend_dash_sumrz38["Max_FA_Pendency_FY"] == pend_dash_sumrz36["fa_pendency_fy"]),
    "inner",
)

org_fact_r8_join39 = (
    org_fact_r8_right39.subtract(org_fact_r8_inner39)
    .select("fa_pendency_fy", "fa_pendency_fy_month", "Sum_Active_Classes_FirstAction")
    # .filter(col("Max_FA_Pendency_FY") != col("fa_pendency_fy"))
)


pend_dash_sumrz37 = (
    ip_df6_tbl_tm_pend_dash.groupBy("fa_pendency_fy")
    .agg(sum("Active_Classes_FirstAction").alias("Sum_Active_Classes_FirstAction"))
    .orderBy(col("fa_pendency_fy").desc())
)


windowSpec = Window.orderBy(col("fa_pendency_fy").desc())

pend_dash_recID41 = (
    pend_dash_sumrz37.withColumn("RecordID", row_number().over(windowSpec))
    .filter(col("RecordID") <= 6)
    .withColumnRenamed("fa_pendency_fy", "Right_fa_pendency_fy")
    .withColumnRenamed("Sum_Active_Classes_FirstAction", "FY_Total")
)

org_fact_join43 = (
    (
        org_fact_r8_join39.join(
            pend_dash_recID41,
            (
                org_fact_r8_join39["fa_pendency_fy"]
                == pend_dash_recID41["Right_fa_pendency_fy"]
            ),
            "inner",
        )
    )
)
from pyspark.sql.functions import format_number
    # .drop("Right_fa_pendency_fy")
org_fact_filter44=org_fact_join43.withColumn("Percent_FA", col("Sum_Active_Classes_FirstAction") / col("FY_Total")).drop("Max_FA_Pendency_FY","Max_FA_Pendency_FY","RecordID","Right_fa_pendency_fy")

org_fact_sumrz45=org_fact_filter44.groupBy("fa_pendency_fy_month").agg(avg("Percent_FA").alias("Percent_of_FAs"))
org_fact_sumrz45=org_fact_sumrz45.withColumn("Percent_of_FAs",format_number(col("Percent_of_FAs"),6))
# [Sum_Active_Classes_FirstAction]/[FY_Total]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: Inventory BD Occurrence

# COMMAND ----------

tm_inv_dash_bd_occ = (
    org_fact_sumrz45.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)
tm_inv_dash_bd_occ = tm_inv_dash_bd_occ.select(
    col("fa_pendency_fy_month").alias("FA_Month").cast(StringType()),
    col("Percent_of_FAs").cast(FloatType()),
    col("create_ts"),
    col("create_user_id"),
    col("update_ts"),
    col("update_user_id"),
)

## write to delta table
tm_inv_dash_bd_occ.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_dashboard_bd_occurrence')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Pendency

# COMMAND ----------

tm_pend_dash_filter15 = ip_df6_tbl_tm_pend_dash.filter(col("on_hold") == lit(False))

tm_pend_dash_sumrz12 = tm_pend_dash_filter15.groupBy().agg(
    max("fa_pendency_fy").alias("Max_FA_Pendency_FY")
)

tm_pend_dash_join13 = tm_pend_dash_filter15.join(
    tm_pend_dash_sumrz12,
    (
        tm_pend_dash_filter15["fa_pendency_fy"]
        == tm_pend_dash_sumrz12["Max_FA_Pendency_FY"]
    ),
    "inner",
).select(
    col("first_action_pendency_ph"),
    col("first_action_dt_ph"),
    col("active_classes_firstaction"),
)

tm_pend_dash_sumrz19 = (
    tm_pend_dash_join13.groupBy()
    .agg(max("first_action_dt_ph").alias("Data_Through"))
    .withColumn("key", lit("1"))
)

# display(tm_pend_dash_join13,limit=10)
tm_pend_dash_frm14 = (
    tm_pend_dash_join13.withColumn(
        "FAPendencyWeight",
        col("Active_Classes_FirstAction")
        * (col("first_action_pendency_ph").cast(DoubleType())),
    )
    .groupBy()
    .agg(
        sum("FAPendencyWeight").alias("Sum_FAPendencyWeight"),
        sum("Active_Classes_FirstAction").alias("Sum_Active_Classes_FirstAction"),
    )
    .withColumn(
        "Current FY Weighted First Action Pendency",
        round(col("Sum_FAPendencyWeight") / col("Sum_Active_Classes_FirstAction"),6),
    )
    .withColumn("key1", lit("1"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: Inventory Pendency

# COMMAND ----------

tm_Inv_dash_pend_op = (
    tm_pend_dash_frm14.join(
        tm_pend_dash_sumrz19,
        (tm_pend_dash_frm14["key1"] == tm_pend_dash_sumrz19["key"]),
        "left",
    )
    .drop("key")
    .drop("key1")
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)


tm_Inv_dash_pend_op =tm_Inv_dash_pend_op.select(
    round(col("Sum_FAPendencyWeight"), 2).alias("Sum_FAPendencyWeight"),
    col("Sum_Active_Classes_FirstAction").cast(IntegerType()),
    col("Current FY Weighted First Action Pendency").alias("Current_FY_Weighted_First_Action_Pendency").cast(FloatType()),
    col("Data_Through").cast(DateType()),
    col("create_ts"),
    col("create_user_id"),
    col("update_ts"),
    col("update_user_id"),
)

## hyper tableau server: TM Inventory Dashboard Pendency --Done
tm_Inv_dash_pend_op.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_dashboard_pendency')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory History - Part 1

# COMMAND ----------

# cls_mil_bib_frm128.printSchema()

cls_mil_bib_filter27 = (
    cls_mil_bib_frm128.filter(col("Count_Type") == "Actual")
    .groupBy()
    .agg(sum("Class_Count").alias("Unexamined_Classes"))
    .withColumn("key", lit(1))
)

cls_mil_bib_frm23 = (
    bib_mil_cls_join91.withColumn(
        "Unexamined_Date", current_timestamp().cast(DateType())
    )
    .groupBy("Unexamined_Date")
    .agg(countDistinct("SER_NUM").alias("Unexamined_Cases"))
    .withColumn("key1", lit(1))
)

cls_mil_bib_append29 = cls_mil_bib_filter27.join(
    cls_mil_bib_frm23, on=[col("key") == col("key1")], how="left"
).select(col("Unexamined_Date"), col("Unexamined_Cases"), col("Unexamined_Classes"))

cmb_unex_inv_join24 = ip_df5_tbl_unex_inv_hstr.unionByName(
    cls_mil_bib_append29, allowMissingColumns = True
)

## Cachce for use by ratio and then pickup in part 2
cmb_unex_inv_join24.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Ratio

# COMMAND ----------

# ip_df8_fact_comp

fact_comp_sumrz48 = (
    ip_df8_fact_comp.groupBy("PAY_BDGT_FSCL_YR", "EMP_NM", "EMP_NO", "ACCTG_ACT_NM")
    .agg(sum("PAY_HR_NO").alias("Sum_PAY_HR_NO"))
    .filter(col("Sum_PAY_HR_NO") > 0)
)

fact_comp_sumrz50 = (
    fact_comp_sumrz48.groupBy("PAY_BDGT_FSCL_YR", "EMP_NM", "EMP_NO")
    .agg(sum("Sum_PAY_HR_NO").alias("Total_Hours"))
    .withColumnRenamed("PAY_BDGT_FSCL_YR", "Right_PAY_BDGT_FSCL_YR")
    .withColumnRenamed("EMP_NM", "Right_EMP_NM")
    .withColumnRenamed("EMP_NO", "Right_EMP_NO")
)

fact_comp_join55 = (
    (
        fact_comp_sumrz48.join(
            fact_comp_sumrz50,
            [
                col("PAY_BDGT_FSCL_YR") == col("Right_PAY_BDGT_FSCL_YR"),
                col("EMP_NO") == col("Right_EMP_NO"),
            ],
            "inner",
        )
    ))
fact_comp_filter52 = fact_comp_join55.filter(
        (col("ACCTG_ACT_NM") == "EXAMINE APPLICATIONS - EXAMINING ATTORNEY")
        | (col("ACCTG_ACT_NM") == "TM ACADEMY EXAMINATION")
    )
fact_comp_frm51 = fact_comp_filter52.withColumn("percent_examine", (col("Sum_PAY_HR_NO") / col("Total_Hours")) * 100)
from pyspark.sql.functions import round
fact_comp_frm51_1= fact_comp_frm51.withColumn("percent_examine",round(col("percent_examine"),0))

fact_comp_filter51= fact_comp_frm51_1.filter(col("percent_examine") >= 50)

fact_comp_sumrz54= fact_comp_filter51.groupBy("PAY_BDGT_FSCL_YR").agg(countDistinct("EMP_NO").alias("EA_Examining"))


# COMMAND ----------

cmb_unex_inv_frm56 = cmb_unex_inv_join24.withColumn(
    "FY",
    when(
        month(col("Unexamined_Date")) > 9,
        (year(col("Unexamined_Date")) + 1),
    ).otherwise(year(col("Unexamined_Date"))),
)

cmb_unex_inv_sumrz58 = cmb_unex_inv_frm56.groupBy("FY").agg(
    avg("Unexamined_Classes").alias("Unexamined_Classes")
)

cmb_unex_inv_sumrz61 = cmb_unex_inv_frm56.groupBy().agg(
    max("Unexamined_Date").alias("Max_Unexamined_Date")
)

fact_comp_join66 = (
    (
        cmb_unex_inv_sumrz61.join(
            cmb_unex_inv_frm56,
            [col("Max_Unexamined_Date") == col("Unexamined_Date")],
            "inner",
        )
    )
    .drop("FY")
    .drop("Unexamined_Date")
    .drop("Unexamined_Cases")
    .withColumn("key", lit("1"))
    .withColumnRenamed("EA_Examining", "Source_EA_Examining")
)

fact_comp_append62 = (
    (
        org_fact_sumrz163.join(
            fact_comp_join66,
            [col("key1") == col("key")],
            "left",
        )
    )
    .drop("Max_Unexamined_Date")
    .drop("key1")
)

cmb_unex_inv_sumrz64 = (
    cmb_unex_inv_frm56.groupBy().agg(max("FY").alias("FY")).withColumn("key1", lit("1"))
)

fact_comp_append65 = (
    (
        fact_comp_append62.join(
            cmb_unex_inv_sumrz64,
            [col("key") == col("key1")],
            "left",
        )
    )
    .drop("key")
    .drop("key1")
    .withColumn("current_fy", lit(1))
)

# COMMAND ----------

# fact_comp_sumrz54, cmb_unex_inv_sumrz58

fact_comp_unex_inv_join59 = (
    fact_comp_sumrz54.join(
        cmb_unex_inv_sumrz58,
        [col("PAY_BDGT_FSCL_YR") == col("FY")],
        "inner",
    )
).drop("PAY_BDGT_FSCL_YR")

cmb_unex_inv_sumrz64_1 = cmb_unex_inv_sumrz64.withColumnRenamed("FY", "Left_FY")

fact_comp_unex_inv_right60 = cmb_unex_inv_sumrz64_1.join(
    fact_comp_unex_inv_join59,
    [col("Left_FY") == col("FY")],
    "right",
)

fact_comp_unex_inv_inner60 = cmb_unex_inv_sumrz64_1.join(
    fact_comp_unex_inv_join59,
    [col("Left_FY") == col("FY")],
    "inner",
)

fc_unex_inv_right60 = (
    fact_comp_unex_inv_right60.subtract(fact_comp_unex_inv_inner60)
    .drop("Left_FY")
    .drop("key1")
)

fc_unex_inv_union63 = (
    fc_unex_inv_right60.unionByName(fact_comp_append65, allowMissingColumns=True)
    .withColumn("ea_unexamined_ratio", round(col("Unexamined_Classes") / col("EA_Examining")))
    .withColumn(
        "current_fy", when(col("current_fy").isNull(), lit(False)).otherwise(lit(True))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: Inventory Ratio

# COMMAND ----------

tm_inv_dash_ratio_op = (
    fc_unex_inv_union63.drop("Source_EA_Examining")
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

tm_inv_dash_ratio_op = tm_inv_dash_ratio_op.select(
    col("FY").cast(StringType()),
    col("EA_Examining").cast(IntegerType()),
    round(col("Unexamined_Classes")).cast(IntegerType()),
    col("ea_unexamined_ratio").cast(IntegerType()),
    col("current_fy"),
    col("create_ts"),
    col("create_user_id"),
    col("update_ts"),
    col("update_user_id"),

)

## write to delta table
tm_inv_dash_ratio_op.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_dashboard_ratio')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory History - Part 2

# COMMAND ----------

cmb_unex_inv_frm30 = (
    cmb_unex_inv_join24.withColumn(
        "fy",
        when(
            month(col('unexamined_date')) > 9,
            (year(col('unexamined_date')) + 1),
        ).otherwise(year(col('unexamined_date'))),
    )
    .withColumnRenamed("ea_examining", "Left_ea_examining")
    .withColumnRenamed("ea_unexamined_ratio", "Left_ea_unexamined_ratio")
    .withColumnRenamed("current_fy", "Left_current_fy")
)

fc_unex_inv_union63_1 = fc_unex_inv_union63.withColumnRenamed(
    "FY", "Right_FY"
).withColumnRenamed("Unexamined_Classes", "Right_Unexamined_Classes")


fc_unex_inv_join33 = (
    (
        cmb_unex_inv_frm30.join(
            fc_unex_inv_union63_1, on=[col("FY") == col("Right_FY")], how="left"
        ).select(
            col("Unexamined_Date"),
            col("Unexamined_Cases"),
            col("Unexamined_Classes"),
            col("fy").cast(StringType()),
            col("ea_examining"),
            col("ea_unexamined_ratio"),
            col("current_fy"),
        )
    )
    .withColumn(
        "current_fy",
        when(
            (col("current_fy").isNull()) | (col("current_fy") == lit(False)), lit(False)
        ).otherwise(lit(True)),
    )
    .withColumn("ea_unexamined_ratio", round(col("Unexamined_Classes") / col("ea_examining")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output: Inventory History

# COMMAND ----------

tm_inv_dash_hist_op = (
    fc_unex_inv_join33.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

tm_inv_dash_hist_op =tm_inv_dash_hist_op.select(
    col("unexamined_date"),
    col("Unexamined_Cases"),
    col("Unexamined_Classes"),
    col("fy"),
    col("ea_examining").cast(IntegerType()),
    col("ea_unexamined_ratio").cast(IntegerType()),
    col("current_fy"),
    col("create_ts"),
    col("create_user_id"),
    col("update_ts"),
    col("update_user_id"),

)
tm_inv_dash_hist_op = tm_inv_dash_hist_op.dropDuplicates(["unexamined_date"])

# write to table
tm_inv_dash_hist_op.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.inventory_unexamined_hstry')

# COMMAND ----------

display(tm_inv_dash_hist_op)

# COMMAND ----------

# MAGIC %md
# MAGIC ## End Job Control

# COMMAND ----------

try:
    recs_count = df_running.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading TM Inventory Dashboard Tables ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading TM Inventory Dashboard Table ")
    

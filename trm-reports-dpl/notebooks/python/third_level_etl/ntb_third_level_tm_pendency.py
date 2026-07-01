# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Set parameter values
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['reporting_catalog']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_third_level_tm_pendency'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Input DFs

# COMMAND ----------

df_milestone = spark.sql(f"select * from {reporting_catalog}.silver.milestone")
df_biblo = spark.sql(f"select * from {reporting_catalog}.silver.bibliography")
df_owner = spark.sql(f"select * from {reporting_catalog}.silver.owner").drop("last_modified_date")
df_ph = spark.sql(f"select * from {reporting_catalog}.silver.prosecution_history")
df_class = spark.sql(f"select * from {reporting_catalog}.silver.class")
df_onhold = spark.sql(f"select * from {reporting_catalog}.silver.on_hold")

# COMMAND ----------

# 80
df_80 = df_milestone.withColumn(
    "disposal_dt", when(col("disposal_dt") > current_timestamp(), lit(None)).otherwise(col("disposal_dt"))
)

# COMMAND ----------

# 59
df_59 = df_80.select(
    'ser_num',
    'first_action_dt_ph',
    'am_1_actn_ct_dt',
    'first_action_type',
    'filing_dt',
    'ib_notification_dt',
    'published_dt',
    'noa_dt',
    'abandonment_dt',
    'aban_dt_ph',
    'registration_dt',
    'disposal_type',
    'ext1_dt',
    'ext2_dt',
    'ext3_dt',
    'ext4_dt',
    'ext5_dt',
    'cancellation_dt',
    'renewal_dt',
    'revival_dt',
    'susp_check_dt',
    'am_cls_ct_actv',
    'pendency_cal_start_dt',
    'pendency_cal_end_dt',
    'noa_registration_check',
    'wgtd_1st_actn_pendency',
    'first_action_cd',
    'disposal_pendency',
    'suspension',
    'ttab',
    'disposal_dt',
    'dock_dt',
    'am_flg_66a_cur',
    'am_flg_66a_fil',
    'noa_dt_ph',
    'filing_fy',
    'non_pro_se',
    'first_action_pendency_ph',
    'last_modified_date',
    'days_in_dock'
).distinct()

# COMMAND ----------

# 76, 107
max_first_action_dt_ph = df_59.groupBy().agg(max(col("first_action_dt_ph"))).collect()[0][0]
max_disposal_dt = df_59.groupBy().agg(max(col("disposal_dt"))).collect()[0][0]

# COMMAND ----------

# 109
df_109 = df_59.withColumn(
    "max_first_action_dt_ph", lit(max_first_action_dt_ph).astype(DateType())
).withColumn(
    "max_disposal_dt", lit(max_disposal_dt).astype(DateType())
)

# COMMAND ----------

# 110, 77
df_77 = df_109.withColumn(
    "max_action_dt", when(col("max_first_action_dt_ph") > col("max_disposal_dt"), col("max_first_action_dt_ph")).otherwise(col("max_disposal_dt"))
).withColumn(
    "max_action_yr", when(month(col("max_action_dt")) > 9, year(col("max_action_dt")) + 1).otherwise(year(col("max_action_dt")))
).drop("max_first_action_dt_ph", "max_disposal_dt")

# COMMAND ----------

# 72
df_72 = df_owner.groupBy("ser_num").agg(min("party_type").alias("min_party_type")).withColumnRenamed("ser_num", "ser_num1")

# COMMAND ----------

# 73
df_73 = df_owner.join(df_72, [df_owner.ser_num == df_72.ser_num1, df_owner.party_type == df_72.min_party_type]).drop("ser_num1")

# COMMAND ----------

# 74
df_74 = df_73.filter(col("owner_num") == 1)

# COMMAND ----------

# 70
df_biblo = df_biblo.drop("last_modified_date").withColumnRenamed("ser_num", "ser_num_biblo")
df_70 = df_74.join(df_biblo, df_74.ser_num == df_biblo.ser_num_biblo, "full_outer")

# COMMAND ----------

# 75
df_75 = df_77.join(df_70, "ser_num")

# COMMAND ----------

# 61
df_61 = df_ph.filter(col("ph_action_code").isin([
    "CAND", "CANG", "CANT", "CNSI", "CNSL", "GNSL", "CCCN", "CCON", "CU.I", "CU.T", "INTI", "INTT", "OP.D", "OP.I", "OP.S", "OP.T", "CU.A", "CU.D", "CU.M", "CU.G", "IRRE", "JURT", "OP.N", "OPPF", "PC.D", "PETC", "PR.D", "PR.W"
]))

# COMMAND ----------

# 63 ---- unsorted "first" in alteryx
win = Window().partitionBy("serial_number").orderBy("ph_action_code")
df_63 = df_61.withColumn("rn", row_number().over(win)).filter(col("rn") == 1).select(col("serial_number").alias("ser_num"), col("ph_action_code").alias("pendency_exclusion"))

# COMMAND ----------

# 62
df_62 = df_75.join(df_63, "ser_num", "left")

# COMMAND ----------

# 64
df_64 = df_62.withColumnRenamed(
    "first_action_type", "first_action_type_num"
).withColumn(
    "pendency_category", when(col("pendency_exclusion").isNotNull(), lit("Suspension or Opposition")).otherwise(lit("No Suspension or Opposition"))
).withColumn(
    "first_action_type", when(lower(col("first_action_type_num")).contains("abandonment"), "Express Abandonment")
                .when(lower(col("first_action_type_num")).contains("amendment"), "Examiner's Amendment")
                .when(lower(col("first_action_type_num")).contains("approved for"), "Approved for Publication")
                .when(lower(col("first_action_type_num")).contains("final refusal"), "Final Refusal")
                .when(lower(col("first_action_type_num")).contains("suspension"), "Suspension")
                .when(lower(col("first_action_type_num")).contains("non-final"), "Non-Final Action")
                .when(lower(col("first_action_type_num")).contains("notice of publication"), "Notice of Publication")
                .when(lower(col("first_action_type_num")).contains("priority action"), "Priority Action")
                .otherwise(lower(col("first_action_type_num")))
).withColumn(
    "filing_basis_grp", when(col("filing_basis_grp").contains("MULTIPLE"), "MULTI-BASIS").otherwise(col("filing_basis_grp"))
)

# COMMAND ----------

# 66
df_66 = df_64.withColumn(
    "total_pendency_fy", when(month(col("disposal_dt")) > 9, year(col("disposal_dt")) + 1).otherwise(year(col("disposal_dt"))),
).withColumn(
    "total_pendency_fy_month_int", month(col("disposal_dt"))
).fillna(
    0, subset=["total_pendency_fy_month_int"]
).withColumn(
    "total_pendency_fy_quarter", when(col("total_pendency_fy_month_int") < 4, "Q2")
        .when(col("total_pendency_fy_month_int") < 7, "Q3")
        .when(col("total_pendency_fy_month_int") < 10, "Q4")
        .otherwise("Q1"),
).withColumn(
    "total_pendency_fy_month", date_format(to_date("disposal_dt", "yyyy-MM-dd"), "MMM"),
).withColumn(
    "total_pendency_fy_date", when(col("total_pendency_fy_month_int") > 9, add_months(col("disposal_dt"), 12), ).otherwise(col("disposal_dt"))
).withColumn(
    "total_pendency_sort", when(col("total_pendency_fy_month_int") == 10, 12)
    .when(col("total_pendency_fy_month_int") == 11, 11)
    .when(col("total_pendency_fy_month_int") == 12, 10)
    .when(col("total_pendency_fy_month_int") == 1, 9)
    .when(col("total_pendency_fy_month_int") == 2, 8)
    .when(col("total_pendency_fy_month_int") == 3, 7)
    .when(col("total_pendency_fy_month_int") == 4, 6)
    .when(col("total_pendency_fy_month_int") == 5, 5)
    .when(col("total_pendency_fy_month_int") == 6, 4)
    .when(col("total_pendency_fy_month_int") == 7, 3)
    .when(col("total_pendency_fy_month_int") == 8, 2)
    .when(col("total_pendency_fy_month_int") == 9, 1)
    .otherwise(lit(None))
).withColumn(
    "total_pendency_fy_filter", col("total_pendency_fy") == col("max_action_yr")
).fillna(False, subset=["total_pendency_fy_filter"])

# COMMAND ----------

# 67
df_67 = df_66.withColumn(
    "fa_pendency_fy", when(month(col("first_action_dt_ph")) > 9, year(col("first_action_dt_ph")) + 1).otherwise(year(col("first_action_dt_ph")))
).withColumn(
    "fa_pendency_fy_month_int", month(col("first_action_dt_ph"))
).fillna(
    0, subset=["fa_pendency_fy_month_int"]
).withColumn(
    "fa_pendency_fy_quarter", when(col("fa_pendency_fy_month_int") < 4, "Q2")
    .when(col("fa_pendency_fy_month_int") < 7, "Q3")
    .when(col("fa_pendency_fy_month_int") < 10, "Q4")
    .otherwise("Q1")
).withColumn(
    "fa_pendency_fy_month", date_format(to_date("first_action_dt_ph", "yyyy-MM-dd"), "MMM"),
).withColumn(
    "fa_pendency_filter", col("fa_pendency_fy") == col("max_action_yr")
).withColumn(
    "STE_CTRY_CD",
    when(col("STATE_CD") == "AL", "AL")
    .when(col("STATE_CD") == "AK", "AK")
    .when(col("STATE_CD") == "AZ", "AZ")
    .when(col("STATE_CD") == "AR", "AR")
    .when(col("STATE_CD") == "CA", "CA")
    .when(col("STATE_CD") == "CO", "CO")
    .when(col("STATE_CD") == "CT", "CT")
    .when(col("STATE_CD") == "DC", "DC")
    .when(col("STATE_CD") == "DE", "DE")
    .when(col("STATE_CD") == "FL", "FL")
    .when(col("STATE_CD") == "GA", "GA")
    .when(col("STATE_CD") == "HI", "HI")
    .when(col("STATE_CD") == "ID", "ID")
    .when(col("STATE_CD") == "IL", "IL")
    .when(col("STATE_CD") == "IN", "IN")
    .when(col("STATE_CD") == "IA", "IA")
    .when(col("STATE_CD") == "KS", "KS")
    .when(col("STATE_CD") == "KY", "KY")
    .when(col("STATE_CD") == "LA", "LA")
    .when(col("STATE_CD") == "ME", "ME")
    .when(col("STATE_CD") == "MD", "MD")
    .when(col("STATE_CD") == "MA", "MA")
    .when(col("STATE_CD") == "MI", "MI")
    .when(col("STATE_CD") == "MN", "MN")
    .when(col("STATE_CD") == "MS", "MS")
    .when(col("STATE_CD") == "MO", "MO")
    .when(col("STATE_CD") == "MT", "MT")
    .when(col("STATE_CD") == "NE", "NE")
    .when(col("STATE_CD") == "NV", "NV")
    .when(col("STATE_CD") == "NH", "NH")
    .when(col("STATE_CD") == "NJ", "NJ")
    .when(col("STATE_CD") == "NM", "NM")
    .when(col("STATE_CD") == "NY", "NY")
    .when(col("STATE_CD") == "NC", "NC")
    .when(col("STATE_CD") == "ND", "ND")
    .when(col("STATE_CD") == "OH", "OH")
    .when(col("STATE_CD") == "OK", "OK")
    .when(col("STATE_CD") == "OR", "OR")
    .when(col("STATE_CD") == "PA", "PA")
    .when(col("STATE_CD") == "RI", "RI")
    .when(col("STATE_CD") == "SC", "SC")
    .when(col("STATE_CD") == "SD", "SD")
    .when(col("STATE_CD") == "TN", "TN")
    .when(col("STATE_CD") == "TX", "TX")
    .when(col("STATE_CD") == "UT", "UT")
    .when(col("STATE_CD") == "VT", "VT")
    .when(col("STATE_CD") == "VA", "VA")
    .when(col("STATE_CD") == "WA", "WA")
    .when(col("STATE_CD") == "WV", "WV")
    .when(col("STATE_CD") == "WI", "WI")
    .when(col("STATE_CD") == "WY", "WY")
    .otherwise("Other")
).fillna(
    "Unknown", subset=["country_or_area_name"]
).fillna(
    "No Law Office", subset=["law_office"]
).fillna(False, subset=["fa_pendency_filter"])

# COMMAND ----------

# 83
df_83 = df_class.filter(~(col("class_status").isin(["INACTIVE-Insufficient Fee Received", ""])))

# COMMAND ----------

# 84
df_84 = df_83.groupBy("ser_num").agg(count_distinct("class").alias("active_classes_firstaction")).withColumn("active_classes_disposal", col("active_classes_firstaction"))

# COMMAND ----------

# 92
df_92 = df_67.join(df_84, "ser_num", "left")

# COMMAND ----------

# 134, 135
df_135 = df_onhold.select("ath_ser_num").distinct().withColumn("on_hold", lit(1))

# COMMAND ----------

# 137
df_137 = df_92.join(df_135, df_92.ser_num == df_135.ath_ser_num, "left")

# COMMAND ----------

# 139
df_139 = df_137.fillna(0, subset=["on_hold"])

# COMMAND ----------

# 105
df_105 = df_139.select(
    "ser_num",
    "fa_pendency_fy",
    "fa_pendency_fy_month",
    "disposal_type",
    "ctry_nm",
    "active_classes_disposal",
    "first_action_dt_ph",
    "first_action_type_num",
    "disposal_dt",
    "total_pendency_fy_date",
    "total_pendency_fy_month",
    "ste_ctry_cd",
    "disposal_pendency",
    "total_pendency_fy",
    "total_pendency_fy_quarter",
    "fa_pendency_fy_quarter",
    "first_action_pendency_ph",
    "max_action_dt",
    "filing_basis_grp",
    "filing_method_filed",
    "law_office",
    "pendency_cal_end_dt",
    "active_classes_firstaction",
    "country_or_area_name",
    "non_pro_se",
    "pendency_category",
    "postal_cd",
    "pendency_cal_start_dt",
    "first_action_type",
    "last_modified_date",
    "noa_dt",
    "registration_dt",
    "abandonment_dt",
    "am_stat",
    col("test_pctram_link").alias("pctram_link"),
    "total_pendency_fy_filter",
    "fa_pendency_filter",
    "on_hold",
    'days_in_dock'
).distinct()

# COMMAND ----------

# 140
df_140 = df_105.filter(~col("am_stat").isin([937, 617]))

# COMMAND ----------

# 106
df_106 = df_140.filter((col("first_action_dt_ph") >= "2011-10-01") | (col("disposal_dt") >= "2011-10-01"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Pendency Counts

# COMMAND ----------

# 113, 114
df_114 = df_106.withColumn(
    "record_output_date", current_timestamp()
).groupBy(
    "record_output_date"
).agg(
    count("ser_num").alias("record_output_count")
)

rec_ct = df_114.select("record_output_count").collect()[0][0]

# COMMAND ----------

# 165
df_165 = spark.sql(f"select * from {reporting_catalog}.silver.pendency_counts")

# COMMAND ----------

# add missing columns & audit columns for union
df_115 = df_114.withColumn("record_output_percent_change", lit(None)).withColumn("continue_process", lit(None)).withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# 115
df_115 = df_115.unionByName(df_165)

# COMMAND ----------

# 116, 129
ct_win = Window().orderBy(desc("record_output_date"))
df_116 = df_115.withColumn("output_record_count_lead", lead(col("record_output_count")).over(ct_win))
df_129 = df_116.withColumn("record_output_percent_change", round((col("record_output_count") - col("output_record_count_lead")) / col("output_record_count_lead")))

# COMMAND ----------

# 117
df_117 = df_129.withColumn(
    "continue_process", when((col("record_output_count") >= col("output_record_count_lead")) & (col("record_output_percent_change") < lit(0.05)), lit(1)).otherwise(lit(0))
).withColumn(
    "record_output_date", col("record_output_date").astype(DateType())
).withColumn(
    "record_output_count", col("record_output_count").astype(IntegerType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write Output

# COMMAND ----------

# add audit cols
df_pendency_dash = df_106.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
).withColumn(
    "output_record_count", lit(rec_ct)
)

# COMMAND ----------

# set column ordering
df_pendency_dash = df_pendency_dash.select('first_action_pendency_ph',
 'first_action_dt_ph',
 'first_action_type_num',
 'abandonment_dt',
 'active_classes_disposal',
 'active_classes_firstaction',
 'am_stat',
 'country_or_area_name',
 'ctry_nm',
 'days_in_dock',
 'disposal_dt',
 'disposal_pendency',
 'disposal_type',
 'fa_pendency_filter',
 'fa_pendency_fy',
 'fa_pendency_fy_month',
 'fa_pendency_fy_quarter',
 'filing_basis_grp',
 'filing_method_filed',
 'first_action_type',
 'last_modified_date',
 'law_office',
 'max_action_dt',
 'noa_dt',
 'non_pro_se',
 'on_hold',
 'pctram_link',
 'pendency_cal_end_dt',
 'pendency_cal_start_dt',
 'pendency_category',
 'postal_cd',
 'registration_dt',
 'ser_num',
 'ste_ctry_cd',
 'total_pendency_fy',
 'total_pendency_fy_filter',
 'total_pendency_fy_month',
 'total_pendency_fy_quarter',
 'total_pendency_fy_date',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id',
 'output_record_count')

# COMMAND ----------

# set column ordering
df_pendency_counts = df_117.select('record_output_date',
 'record_output_count',
 'continue_process',
 'record_output_percent_change',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

# COMMAND ----------

# write dfs
df_pendency_dash.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.pendency_dashboard")

df_pendency_counts.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.pendency_counts")

# end job control
recs_count = df_pendency_dash.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

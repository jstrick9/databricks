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

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
print(f"{trgt_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

import datetime
import pytz

curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_applicant'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC # Applicant Profile Data Transformation
# MAGIC This notebook converts the "Applicant Profile Data TRM" Alteryx workflow to PySpark with strict logic adherence to produce identical outputs.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Configuration
config = {
    "catalog": "hive_metastore",
    "schema": "trademark_analytics"
}

def lowercase_columns(df):
    """Force all column names to lowercase (including spaces/special chars)."""
    return df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Helper Functions

# COMMAND ----------

import unicodedata

def clean_name_conservative(col):
    """Match the SQL/other code approach - conservative cleaning"""
    c = F.when(col.isNull(), "").otherwise(col)
    c = F.trim(c)
    c = F.regexp_replace(c, "[\\t\\n\\r]", "")
    c = F.regexp_replace(c, "\\s+", " ")
    c = F.regexp_replace(c, "[\\p{Punct}]", "")
    c = F.upper(c)
    return c

def alteryx_decompose_unicode(col):
    """
    Mimics Alteryx DecomposeUnicodeForMatch:
    1. Removes accents (diacritics).
    2. Converts to Uppercase.
    3. Keeps only alphanumeric characters and spaces.
    """
    c = F.translate(
        col,
        "áàâäéèêëíìîïóòôöúùûüçñÁÀÂÄÉÈÊËÍÌÎÏÓÒÔÖÚÙÛÜÇÑ",
        "aaaaeeeeiiiioooouuuucnAAAAEEEEIIIIOOOOUUUUCN"
    )
    c = F.upper(c)
    c = F.regexp_replace(c, "[^A-Z0-9 ]", "")
    c = F.regexp_replace(c, "\\s+", " ")
    return F.trim(c)

def calculate_fy(date_col):
    """
    Mimics Alteryx FY Calculation:
    IF ToNumber(DateTimeFormat([Date],"%m"))>9 THEN (Year+1) ELSE Year
    """
    return (
        F.when(date_col.isNull(), F.lit(None))
         .when(F.month(date_col) > 9, F.year(date_col) + 1)
         .otherwise(F.year(date_col))
         .cast("int")
    )

# COMMAND ----------

df_milestone = spark.read.table(f"{trgt_catalog}.silver.milestone")
# df_milestone.display()
df_owner = spark.read.table(f"{trgt_catalog}.silver.owner")
df_class = spark.read.table(f"{trgt_catalog}.silver.class")
df_biblo = spark.read.table(f"{trgt_catalog}.silver.bibliography")
df_postreg = spark.read.table(f"{trgt_catalog}.gold.post_reg_dashboard")


# COMMAND ----------

df_milestone = lowercase_columns(df_milestone)
df_biblo = lowercase_columns(df_biblo)
df_owner = lowercase_columns(df_owner)
df_class = lowercase_columns(df_class)
df_postreg = lowercase_columns(df_postreg)

# COMMAND ----------

# ---------------------------------------------------------
# Manual Lookups (Tool 10, 46, 60)
# ---------------------------------------------------------
df_name_clean_map = spark.createDataFrame(
    [
        (" CO", ""), (" LTD", ""), (" LLC", ""), (" CORP", ""), (" INC", ""),
        (" LP", ""), (" LLP", ""), (" CHTD", ""), (" PA", ""), (" FSB", ""),
        (" NA", ""), (" LLLP", ""), (" PLLC", ""), (" PC", ""), (" DBA", ""), (" Company", "")
    ],
    ["find", "replace"]
)
df_name_clean_map = lowercase_columns(df_name_clean_map)

df_entity_map = spark.createDataFrame(
    [(1, "INDIVIDUAL"), (3, "CORPORATION"), (16, "LLC"), (11, "COMPANY"), (99, "OTHER")],
    ["entity_type_id", "entity_name"]
)
df_entity_map = lowercase_columns(df_entity_map)





# Sample data as list of tuples
coord_class_data = [
    (1, "Chemicals"),
    (2, "Paints"),
    (3, "Cosmetics and cleaning products"),
    (4, "Lubricants and fuels"),
    (5, "Pharmaceuticals"),
    (6, "Metal goods"),
    (7, "Machinery"),
    (8, "Hand tools"),
    (9, "Electrical and scientific apparatus"),
    (10, "Medical apparatus"),
    (11, "Environmental control apparatus"),
    (12, "Vehicles"),
    (13, "Firearms"),
    (14, "Jewelry"),
    (15, "Musical instruments"),
    (16, "Paper goods and printed matter"),
    (17, "Rubber goods"),
    (18, "Leather goods"),
    (19, "Non-metallic building materials"),
    (20, "Furniture and articles not otherwise"),
    (21, "Housewares and glass"),
    (22, "Cordage and fibers"),
    (23, "Yarns and threads"),
    (24, "Fabrics"),
    (25, "Clothing"),
    (26, "Fancy goods"),
    (27, "Floor coverings"),
    (28, "Toys and sporting goods"),
    (29, "Meats and processed foods"),
    (30, "Staple foods"),
    (31, "Natural agricultural products"),
    (32, "Light beverages"),
    (33, "Wines and spirits"),
    (34, "Smokers articles"),
    (35, "Advertising and business"),
    (36, "Insurance and financial"),
    (37, "Construction and repair"),
    (38, "Communication"),
    (39, "Transportation and storage"),
    (40, "Material treatment"),
    (41, "Education and entertainment"),
    (42, "Computer, scientific and legal"),
    (43, "Hotels and restaurants"),
    (44, "Medical, beauty and agricultural"),
    (45, "Personal"),
]

# Define schema
coord_class_schema = T.StructType([
    T.StructField("class_id", T.IntegerType(), nullable=False),
    T.StructField("coordinated class", T.StringType(), nullable=False),
])

df_class_map = spark.createDataFrame(coord_class_data, schema=coord_class_schema)
df_class_map = lowercase_columns(df_class_map)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Owner & Applicant Processing
# MAGIC Logic mirrors Tools 6, 7, 8, 9, 10, 11, 12, 13, 14, 148, 149.

# COMMAND ----------

# Tool 6 & 7: Group By ser_num, Min(party_type) and Filter owner_num = 1
df_owner_filtered = df_owner.filter(F.col("owner_num") == 1)
df_owner_agg = df_owner_filtered.groupBy("ser_num").agg(F.min("party_type").alias("min_party_type"))

# Tool 13: Join Aggregated Owner with Filtered Owner
df_joined_owner = (
    df_owner_agg.alias("agg")  # alias the aggregated DataFrame
    .join(
        df_owner_filtered.alias("filt"),  # alias the filtered DataFrame
        (col("agg.ser_num") == col("filt.ser_num")) &
        (col("agg.min_party_type") == col("filt.party_type")),
        "inner"
    )
    .select("filt.*")  # select columns from the filtered DataFrame
)

# Tool 9: Cleanse (Name) -> Conservative cleaning
df_cleansed = df_joined_owner.withColumn("name_clean", clean_name_conservative(F.col("name")))

# Tool 10 & 11: Find Replace (Replace corporate suffixes)
replacements = df_name_clean_map.collect()
name_expr = F.col("name_clean")

for row in replacements:
    find_str = row["find"]
    replace_str = row["replace"] if row["replace"] else ""
    name_expr = F.regexp_replace(name_expr, f"(?i){find_str}", replace_str)

df_cleansed = df_cleansed.withColumn("name_processed", F.upper(F.trim(name_expr)))

# Tool 148 & 149: Clean up names via Self-Aggregation - handle NULLs
df_name_dedupe = (
    df_cleansed
    .filter(F.col("name_processed").isNotNull() & (F.col("name_processed") != ""))
    .groupBy("name_processed")
    .agg(F.max("name").alias("max_right_name"))
)

# Join back (Tool 149) - LEFT join to preserve all records
df_applicant_final = (
    df_cleansed.join(df_name_dedupe, "name_processed", "left")
    .withColumn("applicant", F.coalesce(F.col("max_right_name"), F.col("name")))
)

# Mapping fields for downstream joins
df_applicant_final = df_applicant_final.select(
    "ser_num",
    "applicant",
    "ctry_cd", 
    "state_cd",
    "entity_type",
    F.initcap(F.col("ctry_nm")).alias("country or area name"),  
    "name"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Main Data Integration
# MAGIC Logic mirrors Tools 16, 17, 21, 15, 22, 23, 47, 48, 26, 58.

# COMMAND ----------

# Tool 16: Summarize Milestone
df_milestone_agg = (
    df_milestone.groupBy(
        "ser_num", "filing_fy", "abandonment_dt", "registration_dt", "non_pro_se", "pendency_cal_start_dt"
    )
    .count()
    .drop("count")
)

# Tool 17: Summarize Biblo
df_biblo_agg = (
    df_biblo.groupBy("ser_num", "filing_method_filed", "filing_basis_grp")
    .count()
    .drop("count")
)

# Tool 21: Join Milestone & Biblo
df_main_join = df_milestone_agg.join(df_biblo_agg, "ser_num", "left")

# Tool 58: Filter filing_fy >= 2015
df_main_join = df_main_join.filter(F.col("filing_fy") >= 2015)

# Tool 15: Join with Applicant Data
df_main_w_app = df_main_join.join(df_applicant_final, "ser_num", "inner")

# Tool 22: Count Distinct ser_num per applicant
df_app_counts = df_main_w_app.groupBy("applicant").agg(
    F.countDistinct("ser_num").alias("applicant_total_cases")
)

# Tool 23: Join Counts back to Main
df_main_w_counts = df_main_w_app.join(df_app_counts, "applicant", "inner")

# # Tool 47 & 48: Right outer join to entity map
# df_main_entity = df_main_w_counts.join(
#     df_entity_map,
#     df_main_w_counts.entity_type == df_entity_map.entity_type_id,
#     "right_outer"
# )


# Tool 47 & 48: Left join to entity map - preserve main records
df_main_entity = df_main_w_counts.join(
    df_entity_map,
    df_main_w_counts.entity_type == df_entity_map.entity_type_id,
    "left"
)

# Tool 26: Formulas
df_final = (
    df_main_entity
    .withColumn(
        "applicant_bin",
        F.when(F.col("applicant_total_cases") == 1, "One-Time Filer")
         .when((F.col("applicant_total_cases") >= 2) & (F.col("applicant_total_cases") <= 9), "Small Filer")
         .when((F.col("applicant_total_cases") >= 10) & (F.col("applicant_total_cases") <= 99), "Medium Filer")
         .when(F.col("applicant_total_cases") >= 100, "Large Filer")
         .otherwise("Large Filer")
    )
    .withColumn(
        "registration",
        F.when(F.col("registration_dt").isNotNull(), "Registered")
         .when(F.col("abandonment_dt").isNotNull(), "Abandoned")
         .otherwise("Pending")
    )
    .withColumn(
        "filing_basis_grp",
        F.when(F.col("filing_basis_grp").contains("MULTIPLE"), "MULTI-BASIS")
         .otherwise(F.col("filing_basis_grp"))
    )
    .withColumn("case_count", F.lit(1))
)

# ---------------------------------------------------------
# Output 1: Generic Profile
# ---------------------------------------------------------
df_generic_output = df_final.select(
    "ser_num", "filing_fy", "non_pro_se", "filing_method_filed", "filing_basis_grp",
    "applicant", "entity_name", "ctry_cd", "country or area name",
    "applicant_total_cases", "applicant_bin", "case_count", "registration", "pendency_cal_start_dt", "state_cd"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Goods & Services Analysis
# MAGIC Mirrors Tools 19, 59/60, 61, 20, 29, and the Tokenization flow.

# COMMAND ----------

# Tool 19: Filter !Inactive
df_class_active = df_class.filter(~F.col("class_status").contains("Inactive"))

# Tool 59/60: Find Replace Class -> Coordinated Class
df_class_coord = (
    df_class_active.join(
        df_class_map,
        F.col("class") == F.col("class_id"),
        "left"
    )
    .drop("class_id")
)

# Tool 29: Goods vs Services Formula
df_class_calc = df_class_coord.withColumn(
    "goods_or_services",
    F.when(F.col("class").cast("int") < 35, "Goods")
     .when((F.col("class").cast("int") >= 35) & (F.col("class").cast("int") <= 45), "Services")
     .otherwise("Other")
)

# Join Class data with Profile Data to get bins
df_class_profile = df_class_calc.join(
    df_final.where(F.col("ser_num").isNotNull()).select("ser_num", "applicant_bin", "pendency_cal_start_dt", "state_cd"),
    "ser_num",
    "inner"
)

# --- Tokenization Flow ---
df_tokens = df_class_profile.withColumn(
    "word",
    F.explode(F.split(F.upper(F.col("goods_and_services_desc")), "\\s+"))
)

stop_words = [
    'AND', 'FOR', 'OF', 'THE', 'NAMELY', 'IN', 'TO', 'USE', 'A', 'FIELD', 'OR', 'SERVICES',
    'PROVIDING', 'A', 'SERVICES', 'FIELD', 'SERVICES ', 'FEATURING', 'WITH', 'ON', 'AS', 'BY',
    'OTHER', 'VIA', 'NAMELY', 'AN', 'OTHERS', 'PURPOSES', 'ALL', 'USED', 'CONDUCTING', 'NOT',
    'THAT', 'FROM', '[', ']', 'MADE', 'SERVICE', 'OTHERS', 'AND/OR', 'BEING', '', ' ',
    'DEVELOPMENT', 'PROMOTING', 'PREPARATIONS'
]

df_tokens_filtered = df_tokens.filter(~F.col("word").isin(stop_words))

df_word_counts = (
    df_tokens_filtered.groupBy("applicant_bin", "word")
    .count()
    .withColumnRenamed("count", "count")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Post Registration & Renewal Rates
# MAGIC Mirrors Container 119 and 142. Exact FY Logic.

# COMMAND ----------

# Join Post Reg with Profile (Tool 64)
df_postreg_join = df_postreg.join(
    df_final.where(F.col("ser_num").isNotNull()),
    df_postreg.serial_number == df_final.ser_num,
    "inner"
)

# -------------------------------------------------------------------------
# 10 Year Rate (Tools 67-75)
# -------------------------------------------------------------------------
df_10 = df_postreg_join.filter(F.col("last_10yr_dt").isNotNull())
df_10 = df_10.withColumn("ten_fy", calculate_fy(F.col("last_10yr_dt")))

df_10 = (
    df_10.withColumn("base_date", F.greatest(F.col("last_10yr_dt"), F.col("expiration_dt_realtime")))
         .withColumn("base_fy", calculate_fy(F.col("base_date")))
)

df_10_num = df_10.groupBy("applicant_bin", "ten_fy").agg(F.countDistinct("serial_number").alias("ten_total"))
df_10_den = df_10.groupBy("applicant_bin", "base_fy").agg(F.countDistinct("serial_number").alias("base_total"))

df_10_rate = (
    df_10_num.join(
        df_10_den,
        (df_10_num.applicant_bin == df_10_den.applicant_bin) &
        (df_10_num.ten_fy == df_10_den.base_fy),
        "inner"
    )
    .select(
        df_10_num.applicant_bin,
        F.col("ten_fy").alias("fy"),
        (F.col("ten_total") / F.col("base_total")).alias("tenrate")
    )
)

# -------------------------------------------------------------------------
# 6 Year Rate (Tools 76-84)
# -------------------------------------------------------------------------
df_6 = df_postreg_join.filter(F.col("six_yr_dt").isNotNull() | (F.col("expiration_type_realtime") == "6 YEAR"))
df_6 = df_6.withColumn("six_fy", calculate_fy(F.col("six_yr_dt")))

df_6 = (
    df_6.withColumn("base_date", F.coalesce(F.col("six_yr_dt"), F.col("expiration_dt_realtime")))
         .withColumn("base_fy", calculate_fy(F.col("base_date")))
)

df_6_num = df_6.groupBy("applicant_bin", "six_fy").agg(F.countDistinct("serial_number").alias("six_total"))
df_6_den = df_6.groupBy("applicant_bin", "base_fy").agg(F.countDistinct("serial_number").alias("base_total"))

df_6_rate = (
    df_6_num.join(
        df_6_den,
        (df_6_num.applicant_bin == df_6_den.applicant_bin) &
        (df_6_num.six_fy == df_6_den.base_fy),
        "inner"
    )
    .select(
        df_6_num.applicant_bin,
        F.col("six_fy").alias("fy"),
        (F.col("six_total") / F.col("base_total")).alias("sixrate")
    )
)

# Join Rates (Tool 87)
df_renewal_rates = df_10_rate.join(df_6_rate, ["applicant_bin", "fy"], "outer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Outputs
# MAGIC Writing to Delta tables instead of Hyper files.

# COMMAND ----------

from pyspark.sql import functions as F

# Select and rename columns, adding missing ones as NULLs with appropriate types
df_selected = df_generic_output.select(
    "ser_num",
    "filing_fy",
    "non_pro_se",
    "filing_method_filed",
    "filing_basis_grp",
    "applicant",
    "entity_name",
    "ctry_cd",
    "state_cd",
    F.col("country or area name").alias("country_or_area_name"),
    "applicant_total_cases",
    "applicant_bin",
    "case_count",
    "registration",
    "pendency_cal_start_dt",
    F.lit(None).cast("date").alias("abandonment_dt"),
    F.lit(None).cast("date").alias("registration_dt"),
    F.lit(None).cast("string").alias("entity_type"),
    F.lit(None).cast("string").alias("name"),
    F.lit(None).cast("timestamp").alias("create_ts"),
    F.lit(None).cast("string").alias("create_user_id"),
    F.lit(None).cast("timestamp").alias("update_ts"),
    F.lit(None).cast("string").alias("update_user_id")
)

#display(df_selected)

# COMMAND ----------

target_table_name = f"{trgt_catalog}.gold.applicant_profile_data_refresh"
df_selected.write.mode("overwrite").format("delta").option("mergeSchema", "true").insertInto(target_table_name)

# COMMAND ----------

# end job control
recs_count = df_selected.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
ttab_scope = common_configs['secrets']['ttab_scope']

print(reporting_catalog,run_env,ttab_scope)

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA INPUTS: GET NECESSARY DATA ELEMENTS FROM VARIOUS SOURCES

# COMMAND ----------

# MAGIC %md
# MAGIC ## PULL AND PREPARE NECESSARY DATA FIELDS FROM THE PROSECUTION HISTORY TABLE

# COMMAND ----------

ip_df1_tbl_ph = spark.sql(f"""select * from {reporting_catalog}.silver.prosecution_history""")

ph_select1208 = ip_df1_tbl_ph.select(col("serial_number"),
                    col("ph_action_number"),
                    col("ph_action_code"),
                    col("cm_sys_dt"),
                    col("ph_action_date"),
                    col("last_modified_date"),
                    col("oracle_apply_time"),
                    col("cm_prcd_num"),
                    col("ri_notif_dt").cast(DateType()),
                    col("cm_desc"),
                    col("fifth_char_cm_type"),
                    col("cm_flg_paper"),
                    col("ttab_tracking_num"),
                    col("tm_worker_eid")
).withColumn("five_Characters",concat(ip_df1_tbl_ph.ph_action_code,ip_df1_tbl_ph.fifth_char_cm_type) )

listValues = ["CNFR", "CNCF", "GNCF", "GNFN", "GNFR", "PUBO"]
listValues1 = ["ABN4O","C18.O", "C18PO", "C7..O", "C7P.O", "PETCT", "CANDT", "CANGT", "TTCDP", "CANTT", "CCCNT", "CCONT", "TTCDD", "TTCGP", "TTCGR", "CU.AT", "CU.DT", "CU.GT", "CU.IT", "CU.MT", "CU.TT", "ETOFT", "ETOPT", "WOPPI","EXAFT", "EXART", "EXDAT", "EXDMT", "EXDRT", "EXFBT", "EXNIT", "EXPAT", "EXPIT", "EXPRT", "EXPTT", "EXRET", "EXRRT", "TTPDA","INTIT", "INTTT","OPPFT", "OP.IT","OP.DT", "OP.ST", "TTODP", "OP.TT", "OP.NT", "TTPDA","TTJDP", "TTJGP","ABNDZ", "RGTDO", "RGTRO", "RGTTO", "REINI", "REINO","TTPRP","ISTBI", "ISTBZ", "MAILT", "OHTBZ", "TTBNI", "TTBOI", "TTDOI","TTAPP", "TTDNP"]

ph_filter165 = ph_select1208.filter(ph_select1208.ph_action_code.isin(listValues) | ph_select1208.five_Characters.isin(listValues1))

ph_frm166 = ph_filter165.withColumn("year",when(month(ph_filter165.ph_action_date) > 9, (year(ph_filter165.ph_action_date) + 1))
                                                       .otherwise(year(ph_filter165.ph_action_date)))


# COMMAND ----------

# set column ordering
ph_frm166 = ph_frm166.select('serial_number',
 'ph_action_number',
 'ph_action_code',
 'cm_sys_dt',
 'ph_action_date',
 'last_modified_date',
 'oracle_apply_time',
 'cm_prcd_num',
 'ri_notif_dt',
 'cm_desc',
 'fifth_char_cm_type',
 'cm_flg_paper',
 'ttab_tracking_num',
 'tm_worker_eid',
 'five_characters',
 'year')

# COMMAND ----------

ph_frm166.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.stg_ttab_input_ph")

# COMMAND ----------

input_ph = spark.sql(f"select * from {reporting_catalog}.silver.stg_ttab_input_ph")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Characteristic Data Elements

# COMMAND ----------

find_replace_input = [('CO', ' '),
 ('LTD', ' '),
 ('LLC', ' '),
 ('CORP', ' '),
 ('INC', ' '),
 ('LP', ' '),
 ('LLP', ' '),
 ('CHTD', ' '),
 ('PA', ' '),
 ('FSB', ' '),
 ('NA', ' '),
 ('LLLP', ' '),
 ('LLC', ' '),
 ('PLLC', ' '),
 ('PC', ' '),
 ('DBA', ' '),
 ('Company', None)]

find_replace_schema = StructType([StructField('Find', StringType(), True),
                     StructField('Replace', StringType(), True)])

find_replace_df = spark.createDataFrame(find_replace_input, find_replace_schema)

###### input dataframe##############
ip_df1_tbl_corr = spark.sql(f"""select * from {reporting_catalog}.silver.correspondence """)
ip_df2_tbl_milestone = spark.sql(f""" select * from {reporting_catalog}.silver.milestone""")
ip_df3_tbl_biblio = spark.sql(f""" select * from {reporting_catalog}.silver.bibliography""")
ip_df4_tbl_class = spark.sql(f"""select * from {reporting_catalog}.silver.class""")
ip_df4_tbl_owner = spark.sql(f"""select * from {reporting_catalog}.silver.owner""")


corr_frm144 = ip_df1_tbl_corr.withColumn("NON_PRO_SE",when(col("ATTY_NM").isNull(), "PRO SE")
                           .otherwise("NON PRO SE")).select(col("SER_NUM"),col("NON_PRO_SE")).distinct()

mil_select146 = ip_df2_tbl_milestone.drop("create_ts").drop("create_user_id").drop("update_ts").drop("update_user_id").drop("non_pro_se")

class_filter293 = ip_df4_tbl_class.filter((col("Class_Status") != "INACTIVE-Insufficient Fee Received") & (col("Class_Status") != ""))

class_sumrz281 = class_filter293.groupBy("SER_NUM").agg(count("Class").astype(IntegerType()).alias("Reg_Class_Count"))

class_sumrz295 = class_filter293.groupBy("SER_NUM").agg(concat_ws(";", collect_list(col("Class"))).alias("Concat_Class")).withColumn(
  "Concat_Class", concat(lit(';'), col("Concat_Class"), lit(';'))
)

class_filter294 = ip_df4_tbl_class.filter((col("Class_Status") == "ACTIVE")).groupBy("SER_NUM").agg(count("Class").astype(IntegerType()).alias("Active_Class_Count"))

owner_sumrz278 = ip_df4_tbl_owner.filter(col("current_owner") == "Y").groupBy("SER_NUM").agg(max("PARTY_TYPE").alias("Max_PARTY_TYPE"), 
                                                                            min("Owner_Num").alias("Min_Owner_Num"))

ip_df4_tbl_owner_1 = ip_df4_tbl_owner.withColumnRenamed("SER_NUM","Right_SER_NUM").drop("max_party_type").drop("create_ts").drop("create_user_id").drop("update_ts").drop("update_user_id")

# COMMAND ----------

owner_join279 = \
(
    owner_sumrz278
        .join(ip_df4_tbl_owner_1,
             on = [col("SER_NUM") == col("Right_SER_NUM"),
                   col("Max_PARTY_TYPE") == col("PARTY_TYPE"),
                   col("Min_Owner_Num") == col("Owner_Num")],
             how = "inner"
             )
)

## copy same logic from filings, which has already been tested

owner_cleanse282 = owner_join279.withColumn(
    "name_update", col("name")
).fillna(
    '', subset=['name_update'] # replace nulls with spaces
).withColumn(
    "name_update", trim(col("name_update")) # trim leading and trailing whitespace
).withColumn(
    "name_update", regexp_replace(col("name_update"), "[^0-9A-Za-zÀ-ÖØ-öø-ÿ\s]", "") # remove all punctuation
).withColumn(
    "name_update", upper(col("name_update")) # convert to uppercase
).withColumn(
    "name_update", regexp_replace(upper("name_update"), " COMPANY | CORP | CO | LTD | LLC | INC | LP | LLP | CHTD | PA | FSB | NA | LLLP | PLLC | PC | DBA "," ")
).withColumn(
    "name_update", regexp_replace(upper("name_update"), " COMPANY| CORP| CO| LTD| LLC| INC| LP| LLP| CHTD| PA| FSB| NA| LLLP| PLLC| PC| DBA"," ")
).withColumn(
    "name_update", regexp_replace(col("name_update"), "\s+", " ") # remove tabs newlines and duplicate whitespace
).withColumn(
    "name_update", trim(col("name_update")) # trim again after replaces
)

# COMMAND ----------

# 288
owner_window = Window.partitionBy("name_update").orderBy(desc("ser_num"))
df_owner_grp = owner_cleanse282.withColumn("row_id", row_number().over(owner_window).alias("row_id"))
df_owner_grp = df_owner_grp.filter(col("row_id") == 1).select("name_update", "name")

owner_join288 = owner_cleanse282.drop("name").join(df_owner_grp, "name_update")

# COMMAND ----------

# corr_frm144 #6
# class_sumrz281 #2
# class_sumrz295 #5
# class_filter294 #4
# ip_df3_tbl_biblio #1
# owner_join288 #3

ip1_biblio = ip_df3_tbl_biblio.withColumnRenamed("LAST_MODIFIED_DATE","Input_#1_LAST_MODIFIED_DATE") \
    .drop("create_ts") \
        .drop("create_user_id") \
            .drop("update_ts") \
                .drop("update_user_id")
ip2_class = class_sumrz281.withColumnRenamed("SER_NUM","Input_#2_SER_NUM")
ip3_owner = owner_join288.withColumnRenamed("SER_NUM","Input_#3_SER_NUM")
ip4_class = class_filter294.withColumnRenamed("SER_NUM","Input_#4_SER_NUM")
ip5_class = class_sumrz295.withColumnRenamed("SER_NUM","Input_#5_SER_NUM")
ip6_corr = corr_frm144.withColumnRenamed("SER_NUM","Input_#6_SER_NUM")

multijoin290_1 = ip1_biblio.join(ip2_class,ip1_biblio["SER_NUM"] == ip2_class["Input_#2_SER_NUM"],"outer")

multijoin290_2 = multijoin290_1.join(ip3_owner,ip1_biblio["SER_NUM"] == ip3_owner["Input_#3_SER_NUM"],"outer")
multijoin290_3 = multijoin290_2.join(ip4_class,ip1_biblio["SER_NUM"] == ip4_class["Input_#4_SER_NUM"],"outer")
multijoin290_4 = multijoin290_3.join(ip5_class,ip1_biblio["SER_NUM"] == ip5_class["Input_#5_SER_NUM"],"outer")
multijoin290 = multijoin290_4.join(ip6_corr,ip1_biblio["SER_NUM"] == ip6_corr["Input_#6_SER_NUM"],"outer") \
        .withColumnRenamed("AM_FLG_66A_FIL","Right_AM_FLG_66A_FIL") \
            .withColumnRenamed("LAST_MODIFIED_DATE","Right_LAST_MODIFIED_DATE") \
                .withColumnRenamed("Right_LAST_MODIFIED_DATE","Right_Right_LAST_MODIFIED_DATE")


# COMMAND ----------

join289 = mil_select146.join(multijoin290, "SER_NUM").withColumn(
    "Filing_FY", when(month(col("Pendency_Cal_Start_DT")) > 9, (year(col("Pendency_Cal_Start_DT")) + 1)).otherwise(year(col("Pendency_Cal_Start_DT")))
).withColumn(
    "Filing_FY_Month_INT", month(col("Pendency_Cal_Start_DT"))
) 

list_ste_cd = ["AL", "AK" , "AZ", "AR", "CA" , "CO", "CT", "DC", "DE" , "FL", "GA", "HI", "ID" , "IL", "IN", "IA" , "KS", "KY", "LA" , "ME", "MD", "MA" , "MI", "MN", "MS" , "MO", "MT", "NE" , "NV", "NH", "NJ" , "NM", "NY", "NC" , "ND", "OH", "OK" , "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

frm291 = join289.withColumn(
    "Filing_FY_Quarter", when(
        col("Filing_FY_Month_INT") < 4,"Q2"
    ).when(
        col("Filing_FY_Month_INT") < 7,"Q3"
    ).when(
        col("Filing_FY_Month_INT") < 10,"Q4"
    ).otherwise("Q1")
).withColumn(
    "Filing_FY_Month", date_format(col("Pendency_Cal_Start_DT"), "MMMM")
).withColumn(
    "STATE", when(col("state_cd").isin(list_ste_cd), col("state_cd")).otherwise(lit("OTHER"))
).withColumn(
    "country_or_area_name", when(col("country_or_area_name").isNull(),lit("Unknown")).otherwise(col("country_or_area_name"))
).withColumn(
    "FILING_BASIS_GRP", when(col("FILING_BASIS_GRP").contains('MULTIPLE'),lit("MULTI-BASIS")).otherwise(col("FILING_BASIS_GRP"))
).withColumn(
    "Group_Type", when(
        upper(col("FILING_BASIS_GRP")) == "MADRID","Madrid"
    ).otherwise(when(
        col("country_or_area_name") == "United States of America", "Domestic"
    ).when(
        col("country_or_area_name") != "United States of America", "Foreign"
    ).otherwise(None))
).withColumn(
    "NON_PRO_SE", when(col("NON_PRO_SE").isNull(),lit("PRO SE")).otherwise(col("NON_PRO_SE"))
)

# dont count, just distinct

sumrz280 = frm291.select(
    "SER_NUM",
    "Pendency_Cal_Start_DT",
    "NON_PRO_SE",
    "TEST_PCTRAM_LINK",
    "LAW_OFFICE",
    "FILING_BASIS_GRP",
    "FILING_METHOD_CUR",
    "AM_STAT",
    "NAME",
    "CITY",
    "STATE",
    "Country_or_Area_Name",
    "Reg_Class_Count",
    "Active_Class_Count",
    "Group_Type",
    "Concat_Class",
    "MARK_NM_SHORT").distinct().withColumnRenamed("NAME","Owner_Name")

# COMMAND ----------

filter298 = sumrz280.filter(
    ((col("Owner_Name") != "") & col("Owner_Name").isNotNull()) & (col("NON_PRO_SE") == "NON PRO SE")
).dropDuplicates(["Owner_Name"]).select(
    col("NON_PRO_SE").alias("Right_NON_PRO_SE"),
    col("Owner_Name")
)
    
findReplace297 = sumrz280.join(filter298, "Owner_Name", "left")

findReplace297 = findReplace297.withColumn(
    "NON_PRO_SE", when((col("Right_NON_PRO_SE") != "") & col("Right_NON_PRO_SE").isNotNull() & (col("NON_PRO_SE") != col("Right_NON_PRO_SE")),col("Right_NON_PRO_SE")).otherwise(col("NON_PRO_SE"))
).drop("Right_NON_PRO_SE")

# COMMAND ----------

# select for column ordering
findReplace297 = findReplace297.select(
    "SER_NUM",
    "Pendency_Cal_Start_DT",
    "NON_PRO_SE",
    "TEST_PCTRAM_LINK",
    "LAW_OFFICE",
    "FILING_BASIS_GRP",
    "FILING_METHOD_CUR",
    "AM_STAT",
    "Owner_Name",
    "CITY",
    "STATE",
    "Country_or_Area_Name",
    "Reg_Class_Count",
    "Active_Class_Count",
    "Group_Type",
    "Concat_Class",
    "MARK_NM_SHORT"
)

# COMMAND ----------

findReplace297.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.stg_ttab_input_cde")

# COMMAND ----------

input_cde = spark.sql(f"select * from {reporting_catalog}.silver.stg_ttab_input_cde")

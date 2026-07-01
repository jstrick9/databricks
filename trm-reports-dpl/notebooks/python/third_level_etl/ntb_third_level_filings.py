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

# edw connection params
host = dbutils.secrets.get("trm_edw_secret", "host")
port = dbutils.secrets.get("trm_edw_secret", "port")
user = dbutils.secrets.get("trm_edw_secret", "username")
pwd = dbutils.secrets.get("trm_edw_secret", "password")
db_name = dbutils.secrets.get("trm_edw_secret", "db_name")

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_third_level_filings'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Owner Track

# COMMAND ----------

# tool ids: 145
df_owner_min = spark.sql(f"select ser_num, min(party_type) as party_type from {reporting_catalog}.silver.owner group by ser_num")
# 287, 164
df_owner = spark.sql(f"select * from {reporting_catalog}.silver.owner").join(df_owner_min, ['ser_num','party_type'])
# 146
df_owner = df_owner.filter(col("owner_num") == 1).drop('py_ent_num', 'right_py_entity_type')

# COMMAND ----------

# id 160, 158, 159
df_owner_nm = df_owner.withColumn(
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

df_owner_nm = df_owner_nm.withColumn(
    "right_owner_email", col("owner_email")
).withColumn(
    "right_state_cd", col("state_cd")
)

# COMMAND ----------

# 188
owner_window = Window.partitionBy("name_update").orderBy(desc("ser_num"))
df_owner_grp = df_owner_nm.withColumn("row_id", row_number().over(owner_window).alias("row_id"))
df_owner_grp = df_owner_grp.filter(col("row_id") == 1).select("name_update", "name")

# COMMAND ----------

# 189
df_owner_nm = df_owner_nm.drop("name",'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id').join(df_owner_grp, "name_update")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Divisionals / Class Track

# COMMAND ----------

df_div = spark.sql(f"select * from {reporting_catalog}.silver.divisionals").drop('create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')
df_class = spark.sql(f"select * from {reporting_catalog}.silver.class").drop('create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

# COMMAND ----------

# 174, 178
div_cls_chld = df_div.filter(col("DV_TYPE") == "CHILD").join(df_class, "ser_num").withColumn("child_class", lit(1)).withColumn("parent_class", lit(None))
# 175, 177
df_class_join = df_class.withColumnRenamed("ser_num", "class_ser_num")
div_cls_prnt = df_div.filter(col("DV_TYPE") == "CHILD").join(df_class_join, df_div.ref_ser_num == df_class_join.class_ser_num).withColumn("parent_class", lit(1)).withColumn("child_class", lit(None))
# 176
div_cls = div_cls_chld.unionByName(div_cls_prnt.drop("class_ser_num"))

# COMMAND ----------

# 179
div_cls = div_cls.withColumn("parent_child_concat", concat(col("ser_num"), col("ref_ser_num")))

# COMMAND ----------

# 181, 180
div_cls_agg_chld = div_cls.filter(col("child_class").isNotNull()).select("parent_child_concat", "ser_num", "class").distinct()
# 182, 183
div_cls_agg_prnt = div_cls.filter(col("parent_class").isNotNull()).select("parent_child_concat", "ref_ser_num", "class").distinct()
# 184, 185
cls_dstnct = div_cls_agg_chld.join(div_cls_agg_prnt, ["parent_child_concat", "class"]).select("ser_num", "class").distinct()

# COMMAND ----------

# 186
class_186 = df_class.join(cls_dstnct, ["ser_num", "class"], "left_anti")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top Layer Track

# COMMAND ----------

# 285, 149
df_milestone = spark.sql(f"select distinct * from {reporting_catalog}.silver.milestone").drop('create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')
df_milestone = df_milestone.withColumn("pendency_cal_start_dt", to_date(col("pendency_cal_start_dt"), 'y-MM-d'))

# COMMAND ----------

# 289
df_biblo = spark.sql(f"select * from {reporting_catalog}.silver.bibliography")

# COMMAND ----------

# 161
mj = class_186.join(df_biblo, "ser_num", "full_outer").join(df_owner_nm, "ser_num", "full_outer")
# 162
df_top = mj.withColumnRenamed("am_flg_66a_fil", "right_am_flg_66a_fil").withColumnRenamed("last_modified_date", "right_last_modified_date").join(df_milestone, "ser_num")

# COMMAND ----------

# 171
df_top = df_top.filter(col("am_stat") != 622)

# COMMAND ----------

# 147, 141
w147 = Window.partitionBy("ser_num").orderBy(desc("class"))
df_top = df_top.withColumn("counter", row_number().over(w147))
df_top = df_top.withColumn("counter", when(col("counter") == 1, col("counter")).otherwise(lit(0)))

# COMMAND ----------

# 142
coordinated_classes = {'001': 'Chemicals',
 '002': 'Paints',
 '003': 'Cosmetics and cleaning products',
 '004': 'Lubricants and fuels',
 '005': 'Pharmaceuticals',
 '006': 'Metal goods',
 '007': 'Machinery',
 '008': 'Hand tools',
 '009': 'Electrical and scientific apparatus',
 '010': 'Medical apparatus',
 '011': 'Environmental control apparatus',
 '012': 'Vehicles',
 '013': 'Firearms',
 '014': 'Jewelry',
 '015': 'Musical instruments',
 '016': 'Paper goods and printed matter',
 '017': 'Rubber goods',
 '018': 'Leather goods',
 '019': 'Non-metallic building materials',
 '020': 'Furniture and articles not otherwise',
 '021': 'Housewares and glass',
 '022': 'Cordage and fibers',
 '023': 'Yarns and threads',
 '024': 'Fabrics',
 '025': 'Clothing',
 '026': 'Fancy goods',
 '027': 'Floor coverings',
 '028': 'Toys and sporting goods',
 '029': 'Meats and processed foods',
 '030': 'Staple foods',
 '031': 'Natural agricultural products',
 '032': 'Light beverages',
 '033': 'Wines and spirits',
 '034': 'Smokers articles',
 '035': 'Advertising and business',
 '036': 'Insurance and financial',
 '037': 'Construction and repair',
 '038': 'Communication',
 '039': 'Transportation and storage',
 '040': 'Material treatment',
 '041': 'Education and entertainment',
 '042': 'Computer, scientific and legal',
 '043': 'Hotels and restaurants',
 '044': 'Medical, beauty and agricultural',
 '045': 'Personal'}

# COMMAND ----------

# 143
df_top = df_top.withColumn("coordinated_class", when(col("class").isin(list(coordinated_classes.keys())), col("class")).otherwise(lit(None)))
df_top = df_top.replace(coordinated_classes, subset=["coordinated_class"])

# COMMAND ----------

# 170, 144
df_top = df_top.filter(col("class").isNotNull())

# COMMAND ----------

# 144
df_top = df_top.filter(col("pendency_cal_start_dt") >= '2005-10-01')

# COMMAND ----------

# 148
df_top = df_top.withColumn(
    "filing fy", when(month(col("pendency_cal_start_dt")) > 9, year(col("pendency_cal_start_dt")) + 1).otherwise(year(col("pendency_cal_start_dt")))
).withColumn(
    "filing_fy_month_int", month(col("pendency_cal_start_dt"))
).withColumn(
    "filing_fy_quarter", when(month(col("pendency_cal_start_dt")) < 4, 'Q2').otherwise(when(month(col("pendency_cal_start_dt")) < 7, 'Q3').otherwise(when(month(col("pendency_cal_start_dt")) < 10, 'Q4').otherwise('Q1')))
).withColumn(
    "filing_fy_month", date_format(col("pendency_cal_start_dt"), "MMMM")
).withColumn(
    "STE_CTRY_CD", expr("case when   state_cd ='AL' THEN 'AL'   when   state_cd ='AK' THEN 'AK'    when   state_cd ='AZ' THEN 'AZ'   when   state_cd ='AR' THEN 'AR'   when   state_cd ='CA' THEN 'CA'    when   state_cd ='CO' THEN 'CO'   when   state_cd ='CT' THEN 'CT'   when   state_cd ='DC' THEN 'DC'  when   state_cd ='DE' THEN 'DE'    when   state_cd ='FL' THEN 'FL'   when   state_cd ='GA' THEN 'GA'   when   state_cd ='HI' THEN 'HI'   when   state_cd ='ID' THEN 'ID'    when   state_cd ='IL' THEN 'IL'   when   state_cd ='IN' THEN 'IN'   when   state_cd ='IA' THEN 'IA'    when   state_cd ='KS' THEN 'KS'   when   state_cd ='KY' THEN 'KY'   when   state_cd ='LA' THEN 'LA'    when   state_cd ='ME' THEN 'ME'   when   state_cd ='MD' THEN 'MD'   when   state_cd ='MA' THEN 'MA'    when   state_cd ='MI' THEN 'MI'   when   state_cd ='MN' THEN 'MN'   when   state_cd ='MS' THEN 'MS'    when   state_cd ='MO' THEN 'MO'   when   state_cd ='MT' THEN 'MT'   when   state_cd ='NE' THEN 'NE'    when   state_cd ='NV' THEN 'NV'   when   state_cd ='NH' THEN 'NH'   when   state_cd ='NJ' THEN 'NJ'    when   state_cd ='NM' THEN 'NM'   when   state_cd ='NY' THEN 'NY'   when   state_cd ='NC' THEN 'NC'    when   state_cd ='ND' THEN 'ND'   when   state_cd ='OH' THEN 'OH'   when   state_cd ='OK' THEN 'OK'    when   state_cd ='OR' THEN 'OR'   when   state_cd ='PA' THEN 'PA'   when   state_cd ='RI' THEN 'RI'   when   state_cd ='SC' THEN 'SC'   when   state_cd ='SD' THEN 'SD'   when   state_cd ='TN' THEN 'TN'   when   state_cd ='TX' THEN 'TX'   when   state_cd ='UT' THEN 'UT'   when   state_cd ='VT' THEN 'VT'   when   state_cd ='VA' THEN 'VA'   when   state_cd ='WA' THEN 'WA'   when   state_cd ='WV' THEN 'WV'   when   state_cd ='WI' THEN 'WI'   when   state_cd ='WY' THEN 'WY' ELSE 'OTHER'  end ")
).fillna("Unknown", subset=["country_or_area_name"]).withColumn(
    "filing_basis_grp", when(col("filing_basis_grp").contains("MULTIPLE"), "MULTI-BASIS").otherwise(col("filing_basis_grp"))
).withColumn(
    "class_int", col("class").astype(IntegerType())
).fillna(
    0, subset=["class_int"]
).withColumn(
    "goods_or_services", when(col("class_int") < 35, "Goods").otherwise(when(col("class_int") < 46, "Services").otherwise("Other"))
).drop("class_int")

# COMMAND ----------

# 265, 266
df_gs = df_top.select("ser_num", "goods_or_services").distinct()
df_gs_agg = df_gs.groupBy("ser_num").agg(concat_ws(';', collect_list("goods_or_services")).alias("concat_goods_or_services"))
df_gs_agg = df_gs_agg.withColumn("concat_goods_or_services", concat(lit(';'), col("concat_goods_or_services"), lit(';')))

# COMMAND ----------

# 267
df_top = df_top.join(df_gs_agg, "ser_num")

# COMMAND ----------

# prevent losing null value during join
df_top = df_top.fillna(' ', subset=["name"])

# COMMAND ----------

df_top = df_top.withColumn("name_lower", lower(col("name")))

# COMMAND ----------

# 271, 272
df_app_ct = df_top.select("ser_num", "name_lower").groupBy("name_lower").agg(countDistinct("ser_num").alias("applicant_total_cases"))
df_app_ct = df_app_ct.withColumn("applicant_bin", expr("""
case when applicant_total_cases = 1 then "One-Time Filer"
when applicant_total_cases >= 2 AND applicant_total_cases <= 9 then "Small Filer"
when applicant_total_cases >= 10 AND applicant_total_cases <= 99 then "Medium Filer"
else "Large Filer" 
end"""))

# COMMAND ----------

# 273
df_top = df_top.join(df_app_ct, "name_lower")

# COMMAND ----------

# reverse change above after join
df_top = df_top.replace(' ', None, subset=['name'])

# COMMAND ----------

# 268, 100
entity_names = {
 '0': 'OTHER',
 '1': 'INDIVIDUAL',
 '2': 'OTHER',
 '3': 'CORPORATION',
 '4': 'OTHER',
 '5': 'OTHER',
 '6': 'OTHER',
 '7': 'OTHER',
 '8': 'OTHER',
 '9': 'OTHER',
 '10': 'OTHER',
 '11': 'OTHER',
 '12': 'OTHER',
 '13': 'OTHER',
 '14': 'OTHER',
 '15': 'OTHER',
 '16': 'LLC',
 '17': 'OTHER',
 '18': 'OTHER',
 '19': 'OTHER',
 '20': 'OTHER',
 '21': 'OTHER',
 '22': 'OTHER',
 '23': 'OTHER',
 '24': 'OTHER',
 '25': 'OTHER',
 '26': 'OTHER',
 '27': 'OTHER',
 '28': 'OTHER',
 '29': 'OTHER',
 '30': 'OTHER',
 '31': 'OTHER',
 '32': 'OTHER',
 '33': 'OTHER',
 '34': 'OTHER',
 '35': 'OTHER',
 '36': 'OTHER',
 '37': 'OTHER',
 '38': 'OTHER',
 '39': 'OTHER',
 '40': 'OTHER',
 '41': 'OTHER',
 '42': 'OTHER',
 '43': 'OTHER',
 '44': 'OTHER',
 '45': 'OTHER',
 '46': 'OTHER',
 '47': 'OTHER',
 '48': 'OTHER',
 '49': 'OTHER',
 '50': 'OTHER',
 '51': 'OTHER',
 '52': 'OTHER',
 '53': 'OTHER',
 '54': 'OTHER',
 '55': 'OTHER',
 '56': 'OTHER',
 '57': 'OTHER',
 '58': 'OTHER',
 '59': 'OTHER',
 '60': 'OTHER',
 '61': 'OTHER',
 '62': 'OTHER',
 '63': 'OTHER',
 '64': 'OTHER',
 '65': 'OTHER',
 '66': 'OTHER',
 '67': 'OTHER',
 '68': 'OTHER',
 '69': 'OTHER',
 '70': 'OTHER',
 '71': 'OTHER',
 '72': 'OTHER',
 '73': 'OTHER',
 '74': 'OTHER',
 '75': 'OTHER',
 '76': 'OTHER',
 '77': 'OTHER',
 '78': 'OTHER',
 '79': 'OTHER',
 '80': 'OTHER',
 '81': 'OTHER',
 '82': 'OTHER',
 '83': 'OTHER',
 '84': 'OTHER',
 '85': 'OTHER',
 '86': 'OTHER',
 '87': 'OTHER',
 '88': 'OTHER',
 '89': 'OTHER',
 '90': 'OTHER',
 '91': 'OTHER',
 '92': 'OTHER',
 '93': 'OTHER',
 '94': 'OTHER',
 '95': 'OTHER',
 '96': 'OTHER',
 '97': 'OTHER',
 '98': 'OTHER',
 '99': 'OTHER'}

# COMMAND ----------

# 269
df_top = df_top.withColumn("entity_type", col("entity_type").astype(StringType())).replace(entity_names, subset=["entity_type"])

# COMMAND ----------

# 150-152
fee_paid = class_186.filter(~(col("class_status").isin(["INACTIVE-Insufficient Fee Received", ""]))).select("ser_num", "class").distinct()
fee_paid = fee_paid.withColumn("fee_paid_class",lit(1))

# COMMAND ----------

# 167
df_top = df_top.join(fee_paid, ["ser_num", "class"], "left")

# COMMAND ----------

sale_tran_query = "(Select * From FORECAST.VW_TM_SALE_TRAN) query_alias"

# 241
edw_sale_tran = spark.read.format("jdbc").option(
    "url", "jdbc:oracle:thin:@" + host + ":" + port + "/" + db_name
).option("dbtable", sale_tran_query).option(
    "user", user
).option(
    "password", pwd
).option(
    "driver", "oracle.jdbc.OracleDriver"
).option(
    "fetchsize", "10000"
).load()

# COMMAND ----------

# 238
edw_sale_tran = edw_sale_tran.filter(col("REV_SRC_CD").isin(["6001", "7001", "7007", "7009", "7931", "7933", "7017"]))

# COMMAND ----------

# 264
edw_sale_tran = edw_sale_tran.withColumn("PRJCT_CD", when(regexp_extract(col("PRJCT_CD"), "[A-Za-z]", 0) != "", col("PSTNG_REF_TX")).otherwise(col("PRJCT_CD")))

# COMMAND ----------

sale_tran_2010_query = "(select FORECAST.VW_SALE_TRAN_PRE_FY2010.* from FORECAST.VW_SALE_TRAN_PRE_FY2010) query_alias"

# 302
edw_sale_tran_2010 = spark.read.format("jdbc").option(
    "url", "jdbc:oracle:thin:@" + host + ":" + port + "/" + db_name
).option("dbtable", sale_tran_2010_query).option(
    "user", user
).option(
    "password", pwd
).option(
    "driver", "oracle.jdbc.OracleDriver"
).option(
    "fetchsize", "10000"
).load()

# COMMAND ----------

# 250
edw_sale_tran_2010 = edw_sale_tran_2010.filter(col("FEE_CD").isin(["6001", "7001", "7007", "7009", "7931", "7933", "7017"]))

# COMMAND ----------

# 245
biblo_madrid = df_biblo.filter(col("filing_basis_fil") == "MADRID")
biblo_non_madrid = df_biblo.filter(col("filing_basis_fil") != "MADRID")

# COMMAND ----------

# 244, 262
df_rt_count_3 = df_milestone.join(biblo_madrid.join(class_186, "ser_num"), "ser_num")
# 243
df_rt_count_3 = df_rt_count_3.filter(~(col("class_status").isin(["INACTIVE-Insufficient Fee Received", ""])))
# 263
df_rt_count_3 = df_rt_count_3.filter(col("filing_fy") >= 2006)
# 257
df_rt_count_3 = df_rt_count_3.groupBy("ser_num").agg(count(col("class")).alias("realtime_count"))

# COMMAND ----------

# 235, 236
df_235 = df_milestone.join(biblo_non_madrid, "ser_num")
df_rt_count_1 = edw_sale_tran.join(df_235, edw_sale_tran.PRJCT_CD == df_235.ser_num)

# COMMAND ----------

# 237
df_rt_count_1 = df_rt_count_1.withColumn(
    "days_btw_posted_and_pend_start_dt", datediff(col("ACCTG_DT"), col("pendency_cal_start_dt"))
).withColumn(
    "fee_flag", when((col("filing_basis_fil") != 'MADRID') & (col("days_btw_posted_and_pend_start_dt") <= 6), lit(1)).otherwise(lit(0))
).withColumn(
    "registration_flag", when(col("registration_dt").isNull(), lit(1)).otherwise(when(col("ACCTG_DT") < col("registration_dt"), lit(1)).otherwise(lit(0)))
)

# COMMAND ----------

# 235
df_rt_count_1 = df_rt_count_1.filter(col("filing_fy") >= 2010)

# 240
df_rt_count_1 = df_rt_count_1.withColumn("UNIT_QT", when((col("TRAN_STATUS_CD") == "R") | (col("TRAN_AM") < 0), lit(0)).otherwise(col("UNIT_QT")))

# COMMAND ----------

# 239
df_rt_count_1 = df_rt_count_1.groupBy("ser_num").agg(sum(col("UNIT_QT")).alias("realtime_count"))

# COMMAND ----------

# 247
df_247 = df_milestone.join(biblo_non_madrid, "ser_num").withColumn(
    "ser_num", col("ser_num").astype(StringType()) # convert to string to prevent joins of 0768 == 768 i.e.
)
# 248
df_rt_count_2 = edw_sale_tran_2010.join(df_247, edw_sale_tran_2010.PSTNG_REF_TX == df_247.ser_num)

# COMMAND ----------

# 249
df_rt_count_2 = df_rt_count_2.withColumn(
    "days_btw_posted_and_pend_start_dt", datediff(col("ACCTG_DT"), col("pendency_cal_start_dt"))
).withColumn(
    "fee_flag", when((col("filing_basis_fil") != 'MADRID') & (col("days_btw_posted_and_pend_start_dt") <= 6), lit(1)).otherwise(lit(0))
).withColumn(
    "registration_flag", when(col("registration_dt").isNull(), lit(1)).otherwise(when(col("ACCTG_DT") < col("registration_dt"), lit(1)).otherwise(lit(0)))
)

# COMMAND ----------

# 253
df_rt_count_2 = df_rt_count_2.filter((col("filing_fy") >= 2006) & (col("filing_fy") < 2010))
# 252
df_rt_count_2 = df_rt_count_2.withColumn("UNIT_QTY", when((col("ITEM_STATUS_CD") == "R") | (col("TRAN_AM") < 0), lit(0)).otherwise(col("UNIT_QTY")))

# COMMAND ----------

# 251
df_rt_count_2 = df_rt_count_2.groupBy("ser_num").agg(sum(col("UNIT_QTY")).alias("realtime_count"))

# COMMAND ----------

# 256
df_rt_count = df_rt_count_1.union(df_rt_count_2).union(df_rt_count_3)

# COMMAND ----------

# 205
df_rt_count = df_rt_count.groupBy("ser_num").agg(sum("realtime_count").alias("realtime_count"))

# COMMAND ----------

# 295
df_fixed_class_ct = spark.sql(f"select * from {reporting_catalog}.silver.fixed_class_counts")

# COMMAND ----------

# 203
fixed_count = df_fixed_class_ct.groupBy("ser_num").agg(sum("class_count").alias("fixed_count"))

# COMMAND ----------

# 206, 211
total_count = fixed_count.join(df_rt_count, "ser_num", "right").withColumn("matches", lit(1)).withColumnRenamed("ser_num", "right_ser_num")

# COMMAND ----------

# 210
df_top = df_top.join(total_count, [df_top.ser_num == total_count.right_ser_num, df_top.counter == total_count.matches], "left")

# COMMAND ----------

# 168
df_top = df_top.withColumn(
    "max_filing_fy", lit(df_top.groupBy().agg(max("filing_fy")).collect()[0][0])
).withColumn(
    "max_pendency_cal_start_dt", lit(df_top.groupBy().agg(max("pendency_cal_start_dt")).collect()[0][0])
)

# COMMAND ----------

# 227
w227 = Window.orderBy("ser_num", "class")
df_top = df_top.withColumn("record_id", row_number().over(w227))
# 228
df_tram_count = df_top.groupBy("ser_num").agg(sum("fee_paid_class").alias("tram_count"), min("record_id").alias("min_record_id")).withColumnRenamed("ser_num", "right_ser_num")
# 229
df_top = df_top.join(df_tram_count, [df_top.ser_num == df_tram_count.right_ser_num, df_top.record_id == df_tram_count.min_record_id], "left")

# COMMAND ----------

# 231
df_top = df_top.fillna(0, subset=["tram_count"])
# 190
df_top = df_top.withColumn(
    "top_2_years", ((col("filing_fy") == col("max_filing_fy")) | (col("filing_fy") == (col("max_filing_fy") - lit(1))))
).withColumn(
    "fixed_count", when((datediff(current_date(), col("pendency_cal_start_dt")) < 50) & (col("filing_method_filed") == "MADRID"), col("tram_count")).otherwise(col("fixed_count"))
).withColumn(
    "realtime_count", when((datediff(current_date(), col("pendency_cal_start_dt")) < 50) & (col("filing_method_filed") == "MADRID"), col("tram_count")).otherwise(col("realtime_count"))
)

# COMMAND ----------

# 165
df_final = (
    df_top.select(
        [
            "ser_num",
            "pendency_cal_start_dt",
            "filing_fy",
            "non_pro_se",
            "filing_method_filed",
            "filing_basis_grp",
            "class",
            "name",
            "city",
            "ste_ctry_cd",
            "postal_cd",
            "ctry_nm",
            "country_or_area_name",
            "counter",
            "max_pendency_cal_start_dt",
            "coordinated_class",
            "filing fy",
            "filing_fy_month_int",
            "filing_fy_quarter",
            "filing_fy_month",
            "top_2_years",
            "fee_paid_class",
            "max_filing_fy",
            "test_pctram_link",
            "fixed_count",
            "realtime_count",
            "tram_count",
            "goods_or_services",
            "concat_goods_or_services",
            "entity_type",
            "applicant_bin",
        ]
    )
    .withColumnRenamed("filing fy", "filing_fy2")
    .withColumnRenamed("counter", "count")
    .distinct()
)

# COMMAND ----------

df_final = df_final.withColumn(
    "ser_num", col("ser_num").astype(IntegerType())
).withColumn(
    "fixed_count", col("fixed_count").astype(IntegerType())
).withColumn(
    "realtime_count", col("realtime_count").astype(IntegerType())
).withColumn(
    "tram_count", col("tram_count").astype(IntegerType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Filings Counts

# COMMAND ----------

# 297
df_filings_cts = spark.sql(f"select * from {reporting_catalog}.silver.filings_counts")

# COMMAND ----------

# 216
df_final_ct = df_final.withColumn("record_output_date", current_timestamp())
# 215
df_final_ct = df_final_ct.groupBy("record_output_date").agg(count("ser_num").alias("output_record_count"))

rec_ct = df_final_ct.select("output_record_count").collect()[0][0]

# COMMAND ----------

df_final_ct = df_final_ct.withColumn("record_output_percent_change", lit(None)).withColumn("continue_process", lit(None)).withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)
# 217
df_final_ct_u = df_final_ct.unionByName(df_filings_cts)

# COMMAND ----------

# 218, 224
ct_win = Window().orderBy(desc("record_output_date"))
df_final_ct_u = df_final_ct_u.withColumn("output_record_count_lead", lead(col("output_record_count")).over(ct_win))
df_final_ct_u = df_final_ct_u.withColumn("record_output_percent_change", (col("output_record_count") - col("output_record_count_lead")) / col("output_record_count_lead"))

# COMMAND ----------

# 219
df_final_ct_u = df_final_ct_u.withColumn("continue_process", when((col("output_record_count") >= col("output_record_count_lead")) & (col("record_output_percent_change") < lit(0.05)), lit(1)).otherwise(lit(0)))

# COMMAND ----------

df_final_ct_u = df_final_ct_u.withColumn(
    "record_output_date", col("record_output_date").astype(DateType())
).withColumn(
    "output_record_count", col("output_record_count").astype(IntegerType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to Tables

# COMMAND ----------

df_final = df_final.withColumnRenamed(
    "test_pctram_link", "pctram_link"
).withColumn(
    "output_record_count", lit(rec_ct)
).withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
).filter(col("pendency_cal_start_dt") < current_date()
)

# COMMAND ----------

# set column ordering
df_final = df_final.select(
    "ser_num",
    "pendency_cal_start_dt",
    "filing_fy2",
    "non_pro_se",
    "filing_method_filed",
    "filing_basis_grp",
    "class",
    "name",
    "city",
    "ste_ctry_cd",
    "postal_cd",
    "ctry_nm",
    "country_or_area_name",
    "count",
    "max_pendency_cal_start_dt",
    "coordinated_class",
    "filing_fy",
    "filing_fy_month_int",
    "filing_fy_quarter",
    "filing_fy_month",
    "top_2_years",
    "fee_paid_class",
    "max_filing_fy",
    "pctram_link",
    "fixed_count",
    "realtime_count",
    "tram_count",
    "goods_or_services",
    "concat_goods_or_services",
    expr("nvl(entity_type, 'OTHER') as entity_type"),
    "applicant_bin",
    "create_ts",
    "create_user_id",
    "update_ts",
    "update_user_id",
    "output_record_count",
)

df_final_ct_u = df_final_ct_u.select(
    "record_output_date",
    "output_record_count",
    "record_output_percent_change",
    "continue_process",
    "create_ts",
    "create_user_id",
    "update_ts",
    "update_user_id",
)

# COMMAND ----------

# write dfs
df_final.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.filings_dashboard")

#df_final_ct_u = df_final_ct_u.select("record_output_date", "output_record_count", "record_output_percent_change", "continue_process")
df_final_ct_u.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.filings_counts")

# end job control
recs_count = df_final.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

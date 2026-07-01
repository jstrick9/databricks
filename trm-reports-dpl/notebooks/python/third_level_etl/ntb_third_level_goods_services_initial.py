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
tmngpdb_src_catalog = common_configs['schema']['tmngpdb_src_catalog']
reporting_catalog = common_configs['schema']['reporting_catalog']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_third_level_goods_services_initial'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Process Records

# COMMAND ----------

# 102
df_filings = spark.sql(f"select * from {reporting_catalog}.gold.filings_dashboard")

# COMMAND ----------

# 69 - 73
df_filings = df_filings.filter(col("filing_fy2") > (col("max_filing_fy") - lit(18)))

# COMMAND ----------

# 64
df_filings = df_filings.select('ser_num',
 'pendency_cal_start_dt',
 'filing_fy2',
 'non_pro_se',
 'filing_method_filed',
 'filing_basis_grp',
 'city',
 'ste_ctry_cd',
 'postal_cd',
 'ctry_nm',
 'country_or_area_name',
 'max_pendency_cal_start_dt',
 'filing_fy',
 'filing_fy_month_int',
 'filing_fy_quarter',
 'filing_fy_month',
 'max_filing_fy',
 'entity_type',
 'applicant_bin'
 ).distinct()

# COMMAND ----------

# 98
df_class = spark.sql(f"select * from {reporting_catalog}.silver.class")

# COMMAND ----------

# 6
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

# 5
df_class = df_class.withColumn("coordinated_class", when(col("class").isin(list(coordinated_classes.keys())), col("class")).otherwise(lit(None)))
df_class = df_class.replace(coordinated_classes, subset=["coordinated_class"])

# COMMAND ----------

df_gs_desc = spark.sql(f"""
    select SCL.CLASS_NO AS class,
    GDS_SRVCS_STMNT_TX AS goods_services_desc,
    CAST(split(FK_TRADEMARK_GID,':')[2] AS INTEGER)AS ser_num,
    1 AS VT_ENT_NUM
    from {tmngpdb_src_catalog}.bronze.TM_CLASS CL
    INNER JOIN {tmngpdb_src_catalog}.bronze.STND_CLASS SCL
    ON CL.FK_CLASS_ID = SCL.CLASS_ID"""
)

# COMMAND ----------

# 110
df_gs_desc = df_gs_desc.filter(col("class").rlike("\d+"))

# COMMAND ----------

# 60
df_60 = df_class.drop("goods_services_desc").join(df_gs_desc, ["ser_num", "class"])

# COMMAND ----------

# 16
df_16 = df_60.join(df_filings, "ser_num").drop("filing_fy", "city", "postal_cd", "ctry_nm", "filing_fy_month_int", "max_filing_fy")

# COMMAND ----------

# 2
df_16 = df_16.filter(~lower(df_16.class_status).contains("inactive"))

# COMMAND ----------

# 3
df_3 = df_16.select('ser_num',
 'class',
 'coordinated_class',
 'goods_services_desc',
 'pendency_cal_start_dt',
 'filing_fy2',
 'non_pro_se',
 'filing_method_filed',
 'filing_basis_grp',
 'ste_ctry_cd',
 'country_or_area_name',
 'max_pendency_cal_start_dt',
 'filing_fy_quarter',
 'filing_fy_month',
 'entity_type',
 'applicant_bin'
).distinct()

# COMMAND ----------

# 4
df_4 = df_3.withColumn(
    "class_int", col("class").astype(IntegerType())
).fillna(
    0, subset=["class_int"]
).withColumn(
    "goods_or_services", when(col("class_int") < 35, "Goods").otherwise(when(col("class_int") < 46, "Services").otherwise("Other"))
).drop("class_int")

# COMMAND ----------

# 8
df_8 = df_4.withColumn(
    "goods_services_desc", regexp_replace(col("goods_services_desc"), "[\xa0]", " ") # remove unicode line breaks 
).withColumn(
    "goods_services_desc", regexp_replace(col("goods_services_desc"), "[\xad]", "") # remove unicode invisible hypens
).withColumn(
    "goods_services_desc", trim(col("goods_services_desc")) # trim leading and trailing whitespace
).withColumn(
    "goods_services_desc", regexp_replace(col("goods_services_desc"), "[^0-9A-Za-zÀ-ÖØ-öø-ÿ\s]", "") # remove all punctuation
).withColumn(
    "goods_services_desc", regexp_replace(col("goods_services_desc"), "\s+", " ") # remove tabs newlines and duplicate whitespace
).withColumn(   
    "goods_services_desc", lower(col("goods_services_desc")) # convert to lowercase
).withColumn(
    "goods_services_desc", trim(col("goods_services_desc")) # trim again after cleansing
)

# COMMAND ----------

# 10
df_10 = df_8.withColumn("goods_services_desc_split", explode_outer(split(col("goods_services_desc"), " ")))

# COMMAND ----------

# replace empty strings with null
df_10 = df_10.withColumn(
    "goods_services_desc_split", when(col("goods_services_desc_split").isin('', ' '), lit(None)).otherwise(col("goods_services_desc_split"))
)

# COMMAND ----------

removal_words = ['AND', 'FOR','OF','THE','NAMELY','IN','TO','USE','A','FIELD','OR','SERVICES','PROVIDING','A','SERVICES','FIELD','SERVICES ','FEATURING','WITH','ON','AS','BY','OTHER','VIA','NAMELY','AN','OTHERS','PURPOSES','ALL','USED','CONDUCTING','NOT','THAT','FROM','[',']','MADE','SERVICE','OTHERS','AND/OR','BEING','',' ','development','promoting','preparations', 'out','nature','be','between', 'about','andor','cases','classes', 'including','relating','related','based','through','containing','their','provided','making','adapted','means','which','purpose','primarily','over','inthe','than','regarding','therefore','therfor','therewith','at','such','aforesaid','except','into','are','also','thereof','can','those','up','one','it','among','within','any','is','may','make','without','these','more','during','effects','effect','affect','affects','this','both','where','who','what','where','when','why','how','them','still','after','before','if','etc','services','foregoing','first','second','third','fourth','fifth','sixth','seventh','eighth','ninth','tenth','therefor','therefore','good','goods','service','services','matter','medium','use','de','so','go','much','proceed','want','you','thereon','t','i','aforementioned','comprise','class','include','wherein','themselves','included','includes']

# COMMAND ----------

removal_words = [x.lower() for x in removal_words]

# COMMAND ----------

# 13
df_13 = df_10.filter((~col("goods_services_desc_split").isin(removal_words)) | (col("goods_services_desc_split").isNull()))

# COMMAND ----------

# 100
gs_norm = spark.sql(f"select * from {reporting_catalog}.silver.goods_services_normalization")

# COMMAND ----------

# 65
gs_proc_dict = {'clothe': 'clothes', 'datum': 'data', 'medium': 'media', 'develope': 'develop'}
gs_norm = gs_norm.replace(gs_proc_dict, subset=["goods_services_desc_processed"])

# COMMAND ----------

# 62
df_62 = df_13.drop("goods_services_desc").withColumnRenamed("goods_services_desc_split", "goods_services_desc").join(gs_norm, "goods_services_desc", "left")

# COMMAND ----------

# 63
df_63 = df_62.filter((~df_62.goods_services_desc_processed.contains("-PRON-")) | (col("goods_services_desc_processed").isNull()))

# COMMAND ----------

# 17
df_17 = df_63.withColumn("goods_services_desc", when(col("goods_services_desc_processed").isNotNull(), col("goods_services_desc_processed")).otherwise(col("goods_services_desc"))).drop("goods_services_desc_processed").distinct()

# COMMAND ----------

# 37, 38
w = Window.partitionBy("ser_num", "class").orderBy("ser_num", desc("class"))
df_38 = df_17.withColumn("rn", row_number().over(w).alias("rn"))
df_38 = df_38.withColumn("class_count", when(col("rn") == 1, lit(1)).otherwise(lit(0)))
df_38 = df_38.drop("rn")
df_38 = df_38.withColumnRenamed("filing_fy2", "filing_fy")

# COMMAND ----------

# convert data types
df_38 = df_38.withColumn(
    "ser_num", col("ser_num").astype(IntegerType())
).withColumn(
    "pendency_cal_start_dt", col("pendency_cal_start_dt").astype(DateType())
).withColumn(
    "max_pendency_cal_start_dt", col("max_pendency_cal_start_dt").astype(DateType())
)

# COMMAND ----------

# add audit columns
df_38 = df_38.withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

# set column ordering
df_38 = df_38.select('ser_num',
 'class',
 'coordinated_class',
 'pendency_cal_start_dt',
 'filing_fy',
 'non_pro_se',
 'filing_method_filed',
 'filing_basis_grp',
 'ste_ctry_cd',
 'country_or_area_name',
 'max_pendency_cal_start_dt',
 'filing_fy_quarter',
 'filing_fy_month',
 'entity_type',
 'applicant_bin',
 'goods_or_services',
 'goods_services_desc',
 'class_count',
 'create_ts',
 'create_user_id',
 'update_ts',
 'update_user_id')

# COMMAND ----------

df_38.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.goods_services_dashboard")

# COMMAND ----------

# 77, 105
df_77 = df_38.select("ser_num").distinct()
df_77 = df_77.withColumn("ser_num", col("ser_num").astype(IntegerType())).withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit('ETL')
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit('ETL')
)

# COMMAND ----------

df_77.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.silver.goods_services_sn_list")

# COMMAND ----------

# DBTITLE 1,End Job Control
# end job control
recs_count = df_38.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")

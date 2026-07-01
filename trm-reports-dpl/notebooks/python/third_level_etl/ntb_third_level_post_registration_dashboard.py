# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
#spark.catalog.clearCache()
#ignorenulls=True

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.window import Window

# COMMAND ----------

def generate_64bit_ID()-> int:
    return (time.time_ns() -1505000000000000000)*10+secrets.randbelow(10)
CHK_POINT_DIR = "/tmp/checkpoints/post_reg_dashboard/"+str(generate_64bit_ID())+"/"
print(f'{CHK_POINT_DIR =}')
global CHK_POINT_DIR

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
print(f"{trgt_catalog=},{src_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)

# COMMAND ----------

job_name = 'ntb_third_level_post_registration_dashboard'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Sources: PostReg Dashboard

# COMMAND ----------

df718 = spark.sql(f"""select * from  {trgt_catalog}.silver.post_reg_milestone 
                  --hive_metastore.alteryx_etldb_dev.post_reg_milestone
                   --where serial_number in (71226311,71695434,90320710,86096185,79026402,90423411,88529276,88003137,88761139,87783625,88761139,87783625)
                   """)
#df718.display()
df719 = df718
df723 = df718
df725 = df718

df727 = spark.sql(f"""SELECT * FROM {trgt_catalog}.silver.post_reg_detail
                 -- hive_metastore.alteryx_etldb_dev.post_reg_detail 
                  --where serial_number in (71226311,71695434,90320710,86096185,79026402,90423411,88529276,88003137,88761139,87783625,88761139,87783625)
                  """)
#df727.display()
df729= df727
df731 = df727
df733 = df727

df695 = spark.sql(f"""SELECT * FROM {trgt_catalog}.silver.milestone
                  --hive_metastore.alteryx_etldb_dev.milestone 
                  --where ser_num in (71226311,71695434,90320710,86096185,79026402,90423411,88529276,88003137,88761139,87783625,88761139,87783625)
                  """)
#df695.display()

df697 = spark.sql(f"""SELECT * FROM {trgt_catalog}.silver.bibliography
                  --hive_metastore.alteryx_etldb_dev.bibliography 
                 --where ser_num in (71226311,71695434,90320710,86096185,79026402,90423411,88529276,88003137,88761139,87783625,88761139,87783625)
                  """)
df697 = df697.select([f.col(col).alias(col.lower()) for col in df697.columns])
#df697.display()

df703 = spark.sql(f"""SELECT * FROM {trgt_catalog}.silver.correspondence
                  --hive_metastore.alteryx_etldb_dev.correspondence 
                  --where ser_num in (71226311,71695434,90320710,86096185,79026402,90423411,88529276,88003137,88761139,87783625,88761139,87783625)
                  """)
#df703.display()

df699 = spark.sql(f"""SELECT * FROM {trgt_catalog}.silver.class
                  --hive_metastore.alteryx_etldb_dev.class 
                  --where ser_num in (71226311,71695434,90320710,86096185,79026402,90423411,88529276,88003137,88761139,87783625,88761139,87783625)
                  """)
#df699.display()

df701 = spark.sql(f""" SELECT * FROM {trgt_catalog}.silver.owner
                  --hive_metastore.alteryx_etldb_dev.owner 
                  --where ser_num in (71226311,71695434,90320710,86096185,79026402,90423411,88529276,88003137,88761139,87783625,88761139,87783625)
                  """)
#df701.display()

df737 = spark.sql(f"""SELECT * FROM --hive_metastore.alteryx_etldb_dev.pr_detail_counts
                   {trgt_catalog}.silver.pr_detail_counts
                  """)
#df737.display()

df735 = spark.sql(f"""SELECT * FROM --hive_metastore.alteryx_etldb_dev.pr_milestone_counts
                  {trgt_catalog}.silver.pr_milestone_counts
                  """)
#df735.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##ETL Logic starts below
# MAGIC ###Convert current date to est

# COMMAND ----------

df653 = df718.withColumn("Live_reg", when((col("Expiration_DT").isNull())| (col("Expiration_DT") == lit("")), lit(1)).otherwise(lit(0)))\
    .withColumn("Exp_FY",when(month("Expiration_DT")>9,year("Expiration_DT")+1).otherwise(year("Expiration_DT")))\
    .withColumn("Exp_FY_RT",when(month("Expiration_DT_RealTime")>9,year("Expiration_DT_RealTime")+1).otherwise(year("Expiration_DT_RealTime")))\
    .withColumn("Reg_FY",when(month("REGISTRATION_DT")>9,year("REGISTRATION_DT")+1).otherwise(year("REGISTRATION_DT")))\
    .withColumn("Today",current_date())\
    .withColumn("Today_FY",when(month(current_date())>9,year(current_date())+1).otherwise(year(current_date())))\
    .withColumn("FY_Exp_Diff",col("Today_FY")- (when(col("Exp_FY").isNull(),0).otherwise(col("Exp_FY"))))\
    .withColumn("FY_Reg_Diff",col("Today_FY")-(when(col("Reg_FY").isNull(),0).otherwise(col("Reg_FY"))))\
    .withColumn("Six_YR_FY",when(month("Six_YR_DT")>9,year("Six_YR_DT")+1).otherwise(year("Six_YR_DT")))\
    .withColumn("Ten_YR_FY",when(month("LAST_10YR_DT")>9,year("LAST_10YR_DT")+1).otherwise(year("LAST_10YR_DT")))\
    .withColumn("Include_6YR_AVG",when(((month("Expiration_Type_RealTime")== "6 YEAR") &\
         (((col("Today_FY")- (when(col("Exp_FY_RT").isNull(),0).otherwise(col("Exp_FY_RT")))) >0) &\
        ((col("Today_FY")- (when(col("Exp_FY_RT").isNull(),0).otherwise(col("Exp_FY_RT"))))<=5)))|\
        (((col("Today_FY")- (when(col("Six_YR_FY").isNull(),0).otherwise(col("Six_YR_FY")))) >0) &\
        ((col("Today_FY")- (when(col("Six_YR_FY").isNull(),0).otherwise(col("Six_YR_FY"))))<=5)),lit(1)).otherwise(lit(0)))\
    .withColumn("Include_10YR_AVG",when(((month("Expiration_Type_RealTime")== "10 YEAR") &\
         (((col("Today_FY")- (when(col("Exp_FY_RT").isNull(),0).otherwise(col("Exp_FY_RT")))) >0) &\
        ((col("Today_FY")- (when(col("Exp_FY_RT").isNull(),0).otherwise(col("Exp_FY_RT"))))<=5)))|\
        (((col("Today_FY")- (when(year("LAST_10YR_DT").isNull(),0).otherwise(year("LAST_10YR_DT")))) >0) &\
        ((col("Today_FY")- (when(year("LAST_10YR_DT").isNull(),0).otherwise(year("LAST_10YR_DT"))))<=5)),lit(1)).otherwise(lit(0)))
#df653.display()

df667 = df653.groupBy().agg(f.max("Today_FY").alias("Max_Today_FY")).collect()[0][0]
#print(df667)

df666 = df653.withColumn("Max_Today_FY",lit(f"{df667}"))
#df666.display()

df652 = df666.withColumn("Reg_Age",when(col('Expiration_DT').isNotNull() ,when(col('Expiration_DT')>col('REGISTRATION_DT'), floor(months_between(col('Expiration_DT'),col('REGISTRATION_DT'))/12)).otherwise(lit(0))).otherwise(floor(months_between(current_date(),col('REGISTRATION_DT'))/12)))\
    .withColumn("Average_Life_Include", expr("case when Today_FY - Reg_FY <5 then 0 when((Exp_FY<1990) and (Expiration_TYPE =='6 YEAR')) then 0 else 1 end"))\
    .withColumn("Sixyr_Num",expr("case when Expiration_DT is null and floor((months_between(current_date(),REGISTRATION_DT))/12) >6  then 1\
        when Six_YR_DT is not null then 1 \
        when Expiration_DT is not null and floor((months_between(Expiration_DT,REGISTRATION_DT))/12) >6 then 1\
        else 0 end"))\
    .withColumn("Sixyr_Denom",expr("case when Expiration_TYPE = '6 YEAR' then 1\
        when Expiration_DT is null and floor((months_between(current_date(),REGISTRATION_DT))/12) >6  then 1\
        when Six_YR_DT is not null then 1 \
        when Expiration_DT is not null and floor((months_between(Expiration_DT,REGISTRATION_DT))/12) >6 then 1\
        else 0 end"))\
    .withColumn("Tenyr_Num",expr("case when Reg_Age >10 or LAST_10YR_DT is not null  then 1\
        else 0 end"))\
    .withColumn("Tenyr_Denom",expr("case when Reg_Age >10 or LAST_10YR_DT is not null or  floor((months_between(Expiration_DT_RealTime,REGISTRATION_DT))/12) <=10 then 1\
        else 0 end"))\
    .withColumn("twentyyr_Num",expr("case when Reg_Age >20 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >18 then 1\
        else 0 end"))\
    .withColumn("twentyyr_denom",expr("case when Reg_Age >20 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >18 or floor((months_between(Expiration_DT_RealTime,REGISTRATION_DT))/12) <=20  then 1\
        else 0 end"))\
    .withColumn("thirtyyr_num",expr("case when Reg_Age >30 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >28 then 1\
        else 0 end"))\
    .withColumn("thirtyyr_denom",expr("case when Reg_Age >30 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >28 or floor((months_between(Expiration_DT_RealTime,REGISTRATION_DT))/12) <=30  then 1\
        else 0 end"))\
    .withColumn("fortyyr_num",expr("case when Reg_Age >40 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >38 then 1\
        else 0 end"))\
    .withColumn("fortyyr_denom",expr("case when Reg_Age >40 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >38 or floor((months_between(Expiration_DT_RealTime,REGISTRATION_DT))/12) <=40  then 1\
        else 0 end"))\
    .withColumn("fiftyyr_num",expr("case when Reg_Age >50 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >48 then 1\
        else 0 end"))\
    .withColumn("fiftyyr_denom",expr("case when Reg_Age >50 or floor((months_between(LAST_10YR_DT,REGISTRATION_DT))/12) >48 or Expiration_DT_RealTime is not null  then 1\
        else 0 end"))\
    .withColumn("Milestone",lit(1))\
    .withColumn("Max_Dt_Filter",col('REGISTRATION_DT')<= current_date())
#df652.display()
#df652.count()
#6,438,539

# COMMAND ----------

df650 = df666.withColumn("Expiration_Type_RealTime2",col("Expiration_Type_RealTime"))\
    .withColumn("Expiration_DT_RealTime2",col("Expiration_DT_RealTime"))\
        .withColumn("Max_Rows",lit(2))
df654 = df650.selectExpr("SERIAL_NUMBER","Number_Renewals as Number_Renewals_MS","Expiration_DT_RealTime as END_ACTION_DATE", "Expiration_Type_RealTime as POSTREG_CATEGORY ", "Expiration_Type_RealTime2","Expiration_DT_RealTime2","Max_Rows")
df656 = df654.where(col('END_ACTION_DATE').isNotNull() )
df638 = df656.where(col('POSTREG_CATEGORY') == "6 YEAR")
df638_f = df656.where(col('POSTREG_CATEGORY') != "6 YEAR").drop("Max_Rows")
#df638.display()
#df638_f.display()
#generate rows
df637 = df638.withColumn("SERIAL_NUMBER", expr("explode(array_repeat(SERIAL_NUMBER,int(2)))"))
w = Window.partitionBy("SERIAL_NUMBER").orderBy(desc("SERIAL_NUMBER"))
df637 = df637.withColumn("RowCount", row_number().over(w))
#df637.display()

df633 = df637.withColumn("POSTREG_CATEGORY", when(col("RowCount")==2, "10 YEAR").otherwise(col("POSTREG_CATEGORY")))\
    .withColumn("END_ACTION_DATE", when(col("RowCount")==2,add_months(col("END_ACTION_DATE"),12*4)).otherwise(col("END_ACTION_DATE"))).drop("RowCount","Max_Rows")
#df633.display()
df635 = df633.unionByName(df638_f,allowMissingColumns=True)
#df635.display()
#df635.count()
#5,997,777

# COMMAND ----------

#w = Window.orderBy(col("serial_number"), col("START_ACTION_DATE"))
#record id
w = Window.orderBy(col("serial_number"), col("START_ACTION_DATE").asc_nulls_first(), col("END_ACTION_DATE").asc_nulls_first())
df640 = df727.withColumn("RecordID", row_number().over(w))
#df640.display()
df659 = df640.withColumn("Max_DT", when(col("START_ACTION_DATE") > col("END_ACTION_DATE"), col("START_ACTION_DATE")).otherwise(col("END_ACTION_DATE")))
#df659.display()
df655 = df635.unionByName(df659,allowMissingColumns=True)
#df655.display()
df660 = df659.groupBy().agg(f.max("Max_DT").alias("Max_Max_DT")).collect()[0][0]
df658 = df655.withColumn("Max_Max_DT",lit(f"{df660}"))
#df658.display()
df651 = df658.withColumn("MAX_FY_PH",when(month("Max_Max_DT")>9,year("Max_Max_DT")+1).otherwise(year("Max_Max_DT")))\
    .withColumn("SixYR_Disposed_Count", when((col("POSTREG_CATEGORY")=="6 YEAR") & (col("END_CM_DESC").isNotNull()) & (col("END_CM_DESC")!="CANCELLED" ), lit(1)).otherwise(lit(0)))\
    .withColumn("SixYR_Base", when(col("POSTREG_CATEGORY")=="6 YEAR", lit(1)).otherwise(lit(0)))\
    .withColumn("TenYR_Disposed_Count", when((col("POSTREG_CATEGORY")=="10 YEAR") & (col("END_CM_DESC").isNotNull()) & (col("END_CM_DESC")!="CANCELLED" ), lit(1)).otherwise(lit(0)))\
    .withColumn("TenYR_Base", when(col("POSTREG_CATEGORY")=="10 YEAR", lit(1)).otherwise(lit(0)))\
    .withColumn("End_Action_FY",when(month("END_ACTION_DATE")>9,year("END_ACTION_DATE")+1).otherwise(year("END_ACTION_DATE")))\
    .withColumn("RENEWAL_NUMBER",expr("case when Expiration_DT_RealTime2 is not null and (POSTREG_CATEGORY ='10 YEAR') then Number_Renewals_MS +1\
        when RENEWAL_NUMBER is null then 1 \
        else RENEWAL_NUMBER end"))\
    .withColumn("RENEWAL_NUMBER_GRP", when(col("RENEWAL_NUMBER")==1, "First Renewal").otherwise("Second+ Renewal"))\
    .withColumn("Top10_FY_Exclude_CFY",when(((col("MAX_FY_PH") - col("End_Action_FY")) > 0)  & ((col("MAX_FY_PH") - col("End_Action_FY")) < 11) , lit(1) ).otherwise(lit(0)))\
    .withColumn("Top5_FY_Exclude_CFY",when(((col("MAX_FY_PH") - col("End_Action_FY")) > 0)  & ((col("MAX_FY_PH") - col("End_Action_FY")) < 6) , lit(1) ).otherwise(lit(0)))\
    .withColumn("Category",expr("case when POSTREG_CATEGORY ='6 YEAR' and (fifteen_flag !=1 or fifteen_flag is null) then '6yr'\
        when POSTREG_CATEGORY ='6 YEAR' and fifteen_flag =1 then '6yr (§15)'\
            when POSTREG_CATEGORY ='10 YEAR' and (fifteen_flag !=1 or fifteen_flag is null) then '10yr'\
                when POSTREG_CATEGORY ='10 YEAR' and fifteen_flag =1 then '10yr (§15)'\
                    when upper(POSTREG_CATEGORY) ='SECTION 7' then '§7'\
                        when upper(POSTREG_CATEGORY) ='SEPARATE 15' then 'Separate §15'\
        else POSTREG_CATEGORY end"))\
    .withColumn("REG_FY",when(month("REGISTRATION_DT")>9,year("REGISTRATION_DT")+1).otherwise(year("REGISTRATION_DT")))\
    .withColumn("Drop_Off_Year",when(col("REG_FY") == col("MAX_FY_PH")-12 , lit(1)).otherwise(lit(0)))    
#df651.display()
#df651.count()
#9,915,475

# COMMAND ----------

# MAGIC %md
# MAGIC ##Characteristics
# MAGIC ###ProSe Field Addition (10/21/2019) 
# MAGIC <pre>
# MAGIC See Container: Characteristics
# MAGIC 1. Derive ProSe field from ATTY_NM (null vs. non-null) in Correspondence Tbl
# MAGIC 2. Assign field value to "ProSe" for null instances
# MAGIC 3. Correct the value to "Non Pro Se" when same owner has a ProSe instance
# MAGIC </pre>

# COMMAND ----------

df681 = df703.withColumn("non_pro_se", when(col("atty_nm").isNull(),"PRO SE").otherwise("NON PRO SE")).select("ser_num","non_pro_se").distinct().withColumnRenamed("SER_NUM","Input_6_SER_NUM")
#df681.display()
df615 = df699.where((col('class_status') != "INACTIVE-Insufficient Fee Received") & (col('class_status') != "" ))
#df615.display()
df667 = df615.groupBy("ser_num").agg(f.count("Class").alias("Reg_Class_Count")).withColumnRenamed("SER_NUM","Input_2_SER_NUM")
#df667.display()
df613 = df615.groupBy("ser_num").agg(concat_ws(";",collect_list("Class")).alias("Concat_Class")) .withColumn("Concat_Class", concat(lit(';'), col("Concat_Class"), lit(';'))).withColumnRenamed("SER_NUM","Input_5_SER_NUM")
#df613.display()

df614 = df699.where(col("class_status")=="ACTIVE").groupBy("ser_num").agg(count("Class").alias("Active_Class_Count")).withColumnRenamed("SER_NUM","Input_4_SER_NUM")
#df614.display()


df631 =df701.where(col("current_owner")=="Y").groupBy("ser_num").agg(max("PARTY_TYPE").alias("Max_PARTY_TYPE"),min("Owner_Num").alias("Min_Owner_Num"))
#df631.display()
df701_renamed = df701.withColumnRenamed("ser_num","Right_SER_NUM").withColumnRenamed("max_party_type","Right_max_party_type")
df629 = df631.alias("df631").join(df701_renamed.alias("df701_renamed"),(df631.ser_num == df701_renamed.Right_SER_NUM) & (df631.Max_PARTY_TYPE == df701_renamed.party_type) & (df631.Min_Owner_Num == df701_renamed.owner_num) , "inner")
#df629.display()

df623 = df629.withColumn(
    "Name_Update", col("name")
).fillna(
    '', subset=['Name_Update'] # replace nulls with spaces
).withColumn(
    "Name_Update", trim(col("Name_Update")) # trim leading and trailing whitespace
).withColumn(
    "Name_Update", upper(col("Name_Update")) # convert to uppercase
).withColumn(
    "Name_Update", regexp_replace(col("Name_Update"), "\s+", " ") # remove tabs newlines and duplicate whitespace
).withColumn(
    "Name_Update", regexp_replace(col("Name_Update"), "[^0-9A-Za-zÀ-ÖØ-öø-ÿ\s]", "")
#).withColumn(
#    "Name_Update", regexp_replace(upper("Name_Update"), " COMPANY | CORP | CO | LTD | LLC | INC | LP | LLP | CHTD | PA | FSB | NA | LLLP | PLLC | PC | DBA "," ")
#).withColumn(
#    "Name_Update", regexp_replace(upper("Name_Update"), " COMPANY| CORP| CO| LTD| LLC| INC| LP| LLP| CHTD| PA| FSB| NA| LLLP| PLLC| PC| DBA","")
)
df624 = df623.withColumn("Name_Update", regexp_replace("Name_Update", " COMPANY| CORP| CO| LTD| LLC| INC| LP| LLP| CHTD| PA| FSB| NA| LLLP| PLLC| PC| DBA"," "))

##Added sort on serial number in alteryx workflow to match databricks 
df624 = df624.withColumn(
    "Name_Update", col("Name_Update")
).fillna(
    '', subset=['Name_Update'] # replace nulls with spaces
).withColumn(
    "Name_Update", trim(col("Name_Update")) # trim leading and trailing whitespace 
).withColumn(
    "Name_Update", regexp_replace(col("Name_Update"), "\s+", " ") # remove tabs newlines and duplicate whitespace
)

#added sort on name in alteryx
w = Window.partitionBy("Name_Update").orderBy(desc("ser_num"),desc("name"))
df621 = df624.withColumn("row_id", row_number().over(w).alias("row_id"))
df621 = df621.filter(col("row_id") == 1).select("Name_Update", "name")
#df621.display()
df624 = df624.drop("name")
df620 = df621.alias("df621").join(df624.alias("df624"), df621.Name_Update == df624.Name_Update, "inner").drop(df621.Name_Update).withColumnRenamed("ser_num","Input_3_SER_NUM")
#df620.display()

#Alteryx join multiple tool = full outer join
df618 = df697.alias("df697").join(df681.alias("df681"),df697.ser_num == df681.Input_6_SER_NUM,"full_outer")\
    .join(df613.alias("df613"),df697.ser_num == df613.Input_5_SER_NUM,"full_outer")\
    .join(df614.alias("df614"),df697.ser_num == df614.Input_4_SER_NUM,"full_outer")\
    .join(df667.alias("df667"),df697.ser_num == df667.Input_2_SER_NUM,"full_outer")\
    .join(df620.alias("df620"),df697.ser_num == df620.Input_3_SER_NUM,"full_outer")
#https://community.alteryx.com/t5/Tool-Mastery/Tool-Mastery-Join-Multiple/ta-p/124619
#df618.display()
#df618.count()
#12,386,792

# COMMAND ----------

# DBTITLE 1,ProSe Addition (10/21/2019) 
df696 = df695.drop("non_pro_se")
df619 = df696.join(df618, "ser_num","inner")
#df619.display()

df617 = df619.withColumn("Filing_FY",when(month("Pendency_Cal_Start_DT")>9,year("Pendency_Cal_Start_DT")+1).otherwise(year("Pendency_Cal_Start_DT")))\
    .withColumn("Filing_FY_Month_INT",when(col("Pendency_Cal_Start_DT").isNotNull(),month("Pendency_Cal_Start_DT")).otherwise(lit(0)))\
    .withColumn("Filing_FY_Quarter",expr("case when Filing_FY_Month_INT < 4  then 'Q2'\
         when Filing_FY_Month_INT < 7  then 'Q3'\
             when Filing_FY_Month_INT < 10  then 'Q4'\
                 else 'Q1' end"))\
    .withColumn("Filing_FY_Month", date_format(col("Pendency_Cal_Start_DT"), "MMMM"))\
    .withColumn("STE_CTRY_CD", expr("case when   STATE_CD ='AL' THEN 'AL'   when   STATE_CD ='AK' THEN 'AK'    when   STATE_CD ='AZ' THEN 'AZ'   when   STATE_CD ='AR' THEN 'AR'   when   STATE_CD ='CA' THEN 'CA'    when   STATE_CD ='CO' THEN 'CO'   when   STATE_CD ='CT' THEN 'CT'   when   STATE_CD ='DC' THEN 'DC'  when   STATE_CD ='DE' THEN 'DE'    when   STATE_CD ='FL' THEN 'FL'   when   STATE_CD ='GA' THEN 'GA'   when   STATE_CD ='HI' THEN 'HI'   when   STATE_CD ='ID' THEN 'ID'    when   STATE_CD ='IL' THEN 'IL'   when   STATE_CD ='IN' THEN 'IN'   when   STATE_CD ='IA' THEN 'IA'    when   STATE_CD ='KS' THEN 'KS'   when   STATE_CD ='KY' THEN 'KY'   when   STATE_CD ='LA' THEN 'LA'    when   STATE_CD ='ME' THEN 'ME'   when   STATE_CD ='MD' THEN 'MD'   when   STATE_CD ='MA' THEN 'MA'    when   STATE_CD ='MI' THEN 'MI'   when   STATE_CD ='MN' THEN 'MN'   when   STATE_CD ='MS' THEN 'MS'    when   STATE_CD ='MO' THEN 'MO'   when   STATE_CD ='MT' THEN 'MT'   when   STATE_CD ='NE' THEN 'NE'    when   STATE_CD ='NV' THEN 'NV'   when   STATE_CD ='NH' THEN 'NH'   when   STATE_CD ='NJ' THEN 'NJ'    when   STATE_CD ='NM' THEN 'NM'   when   STATE_CD ='NY' THEN 'NY'   when   STATE_CD ='NC' THEN 'NC'    when   STATE_CD ='ND' THEN 'ND'   when   STATE_CD ='OH' THEN 'OH'   when   STATE_CD ='OK' THEN 'OK'    when   STATE_CD ='OR' THEN 'OR'   when   STATE_CD ='PA' THEN 'PA'   when   STATE_CD ='RI' THEN 'RI'   when   STATE_CD ='SC' THEN 'SC'   when   STATE_CD ='SD' THEN 'SD'   when   STATE_CD ='TN' THEN 'TN'   when   STATE_CD ='TX' THEN 'TX'   when   STATE_CD ='UT' THEN 'UT'   when   STATE_CD ='VT' THEN 'VT'   when   STATE_CD ='VA' THEN 'VA'   when   STATE_CD ='WA' THEN 'WA'   when   STATE_CD ='WV' THEN 'WV'   when   STATE_CD ='WI' THEN 'WI'   when   STATE_CD ='WY' THEN 'WY' ELSE 'OTHER'  end "))\
    .fillna("Unknown", subset=["country_or_area_name"])\
    .withColumn("filing_basis_grp", when(col("filing_basis_grp").contains("MULTIPLE"), "MULTI-BASIS").otherwise(col("filing_basis_grp")))\
    .withColumn("Group_Type", expr("""case when upper(filing_basis_grp) ="MADRID" then "Madrid" when lower(country_or_area_name) ="united states of america" then "Domestic" when lower(country_or_area_name) !="united states of america" then "Foreign" else null end"""))\
    .withColumn("non_pro_se", when(col("non_pro_se").isNull(),"PRO SE").otherwise(col("non_pro_se")))   


#df617.display()
df628  = df617.selectExpr("ser_num","pendency_cal_start_dt","non_pro_se","test_pctram_link as pctram_link","law_office","filing_basis_grp","filing_method_cur","am_stat","name as owner_name","city","STE_CTRY_CD as state","country_or_area_name","Reg_Class_Count","Active_Class_Count","Group_Type","Concat_Class","mark_nm_short").distinct()
#df628.display()
df686 = df628.where((col('owner_name').isNotNull()) & (col("non_pro_se")=="NON PRO SE"))
#df686.display()

df684 = df686.dropDuplicates(["owner_name"]).selectExpr("ser_num","non_pro_se as right_non_pro_se","owner_name")
#df684.display()
df685 = df628.alias("df628").join(df684.alias("df684"),"owner_name","left" ).select("df628.*",col("right_non_pro_se"))
#df685.display()
df687 = df685.withColumn("non_pro_se", when((col("right_non_pro_se").isNotNull()) & (col("non_pro_se")!=col("right_non_pro_se")), col("right_non_pro_se")).otherwise(col("non_pro_se"))).drop("right_non_pro_se")
#df687.display()
spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df687")
df687 = df687.checkpoint(True)
#df687.count()
#12,386,792

# COMMAND ----------

df657 = df652.alias("df652").join(df687.alias("df687"), df652.serial_number == df687.ser_num,"inner")
#df657.display()
df649 = df657.select("serial_number","registration_dt","six_yr_dt","last_10yr_dt","next_10yr_renewal","number_renewals","next_6yr_dt","expiration_dt","expiration_type","registration_number","am_dt_cncl","live_registration","expiration_dt_realtime","expiration_type_realtime",'Live_reg', 'Exp_FY', 'Exp_FY_RT', 'Reg_FY', 'Today','Today_FY', 'FY_Exp_Diff', 'FY_Reg_Diff', 'Six_YR_FY', 'Ten_YR_FY', 'Include_6YR_AVG', 'Include_10YR_AVG', 'Max_Today_FY', 'Reg_Age', 'Average_Life_Include', 'Sixyr_Num', 'Sixyr_Denom', 'Tenyr_Num', 'Tenyr_Denom', 'twentyyr_Num', 'twentyyr_denom', 'thirtyyr_num', 'thirtyyr_denom', 'fortyyr_num', 'fortyyr_denom', 'fiftyyr_num', 'fiftyyr_denom', 'Milestone','pendency_cal_start_dt', 'non_pro_se', 'pctram_link', 'law_office', 'filing_basis_grp', 'filing_method_cur', 'am_stat','owner_name','city', 'state', 'country_or_area_name', 'Reg_Class_Count', 'Active_Class_Count', 'Group_Type', 'Concat_Class', 'mark_nm_short','Max_Dt_Filter').distinct()
df647 = df649.select("serial_number","registration_dt","expiration_dt").distinct()
#df647.display()

#transpose
df646 = df647.melt(
    ids=["serial_number"], values=["registration_dt", "expiration_dt"],
    variableColumnName="LiveRegH_Name", valueColumnName="LiveRegH_Value"
)
#df646.display()

df645 = df646.withColumn("LiveRegH_DT",expr("case when LiveRegH_Name ='registration_dt' then LiveRegH_Value\
        when LiveRegH_Name ='expiration_dt' then date_add(LiveRegH_Value,1)\
        else null end"))\
        .withColumn("LiveRegH_Count", expr("case when LiveRegH_Name = 'registration_dt' then 1\
            when  LiveRegH_Name = 'expiration_dt' then -1\
                else 0 end"))\
        .withColumn("Max_Dt_Filter",when(col("LiveRegH_DT")<=current_date(),lit(1)).otherwise(lit(0)))
#df645.display()
df645  = df645.withColumn("LiveRegH_Name", expr("case when LiveRegH_Name = 'registration_dt' then 'REGISTRATION_DT' when LiveRegH_Name = 'expiration_dt' then 'Expiration_DT' end "))
df642 = df645.alias("df645").join(df687.alias("df687"),df645.serial_number == df687.ser_num, "inner")
#df642.display()
#df642.count()
#12,877,078

# COMMAND ----------

# MAGIC %md
# MAGIC ###Percentile

# COMMAND ----------

df643 = df651.alias("df651").join(df687.alias("df687"),df651.SERIAL_NUMBER == df687.ser_num, "inner")
spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df643")
df643 = df643.checkpoint(True)

df600 = df643.where(col('first_action_pendency').isNotNull()).select("RecordID","SERIAL_NUMBER","POSTREG_CATEGORY","first_action_pendency").distinct()
#df600.display()

w = Window.orderBy("RecordID","SERIAL_NUMBER")
df_608 = df600.withColumn("__RecordIDOrig", row_number().over(w))#.orderBy("POSTREG_CATEGORY", "first_action_pendency")
#df_608.display()
df610 = df_608.groupBy("POSTREG_CATEGORY").agg(f.count("SERIAL_NUMBER").alias("__Count"))
#df610.display()

df603 = df_608.alias("df608").join(df610.alias("df610"), "POSTREG_CATEGORY","inner").withColumnRenamed("df610.POSTREG_CATEGORY","Right_POSTREG_CATEGORY")

w = Window.orderBy("POSTREG_CATEGORY", "first_action_pendency","SERIAL_NUMBER")
#df602 = df603.withColumn("__RecordID",f.when((col('POSTREG_CATEGORY') == lag(col('POSTREG_CATEGORY'),1).over(w)),row_number().over(w)-1).otherwise("0"))
#df602.display()
df602 = df603.withColumn("__RecordID",f.when((col('POSTREG_CATEGORY') == lag(col('POSTREG_CATEGORY'),1).over(w)), row_number().over(Window.partitionBy("POSTREG_CATEGORY").orderBy("POSTREG_CATEGORY", "first_action_pendency","SERIAL_NUMBER"))-1).otherwise("0"))

df609 = df602.withColumn("Percentile",when(col("POSTREG_CATEGORY") == lag(col('POSTREG_CATEGORY'),1).over(w),\
       when(col("SERIAL_NUMBER") == lag(col('SERIAL_NUMBER'),1).over(w), (round(100*(lag(col("__RecordID"),1).over(w))/(lag(col("__Count"),1).over(w)-1), 2))).otherwise(round(100*col("__RecordID")/(col("__Count")-1), 2)))\
.otherwise(round(100*col("__RecordID")/(col("__Count")-1), 2) ))
#df609.display()
df604 = df609.select("__RecordIDOrig","Percentile","RecordID").orderBy("__RecordIDOrig")
#df604.display()
df605 = df604.alias("df604").join(df600.alias("df600"),"RecordID","inner").withColumnRenamed("Percentile","FA_PERCENTILE").drop("df600.RecordID","df604.__RecordIDOrig")

df587 = df605.withColumn("FA_PERCENTILE_INCLUDE", when(col("FA_PERCENTILE")<95 , lit(1)).otherwise(lit(0)))
#df587.display()
#df587.count()
#3,537,238

# COMMAND ----------

df599 = df643.where(col('total_pendency').isNotNull()).select("RecordID","SERIAL_NUMBER","POSTREG_CATEGORY","total_pendency").distinct()
w = Window.orderBy("RecordID","SERIAL_NUMBER")
df595 = df599.withColumn("__RecordIDOrig", row_number().over(w))#.orderBy("POSTREG_CATEGORY", "first_action_pendency")
df597 = df599.groupBy("POSTREG_CATEGORY").agg(f.count("SERIAL_NUMBER").alias("__Count"))

df590 = df595.alias("df595").join(df597.alias("df597"), "POSTREG_CATEGORY","inner").withColumnRenamed("df597.POSTREG_CATEGORY","Right_POSTREG_CATEGORY")

w = Window.orderBy("POSTREG_CATEGORY", "total_pendency","SERIAL_NUMBER")
#df589 = df590.withColumn("__RecordID",f.when((col('POSTREG_CATEGORY') == lag(col('POSTREG_CATEGORY'),1).over(w))  ,row_number().over(w)-1).otherwise("0"))
#df602.display()
df589 = df590.withColumn("__RecordID",f.when((col('POSTREG_CATEGORY') == lag(col('POSTREG_CATEGORY'),1).over(w))  ,row_number().over(Window.partitionBy("POSTREG_CATEGORY").orderBy("POSTREG_CATEGORY", "total_pendency","SERIAL_NUMBER"))-1).otherwise("0"))

df596 = df589.withColumn("Percentile",when(col("POSTREG_CATEGORY") == lag(col('POSTREG_CATEGORY'),1).over(w),\
       when(col("SERIAL_NUMBER") == lag(col('SERIAL_NUMBER'),1).over(w), (round(100*(lag(col("__RecordID"),1).over(w))/(lag(col("__Count"),1).over(w)-1), 2))).otherwise(round(100*col("__RecordID")/(col("__Count")-1), 2)))\
.otherwise(round(100*col("__RecordID")/(col("__Count")-1), 2) ))

df591 = df596.select("__RecordIDOrig","Percentile","RecordID").orderBy("__RecordIDOrig")
df592 = df591.alias("df591").join(df599.alias("df599"),"RecordID","inner").withColumnRenamed("Percentile","TP_PERCENTILE").drop("df591.RecordID","df591.__RecordIDOrig")
df586 = df592.withColumn("TP_PERCENTILE_INCLUDE", when(col("TP_PERCENTILE")<95 , lit(1)).otherwise(lit(0)))

df585 = df587.alias("df587").join(df586.alias("df586"),(df587.RecordID == df586.RecordID) & (df587.SERIAL_NUMBER == df586.SERIAL_NUMBER),"inner" ).select("df587.*","df586.TP_PERCENTILE","df586.TP_PERCENTILE_INCLUDE").drop("__RecordIDOrig","POSTREG_CATEGORY","first_action_pendency").withColumnRenamed("RecordID","Right_RecordID").withColumnRenamed("SERIAL_NUMBER","Right_SERIAL_NUMBER")
#df585.display()

df641 = df643.alias("df643").join(df585.alias("df585"),(df643.RecordID == df585.Right_RecordID) & (df643.SERIAL_NUMBER == df585.Right_SERIAL_NUMBER),"left").drop("Right_SERIAL_NUMBER")
#df641.display()

df648 = df641.select("RecordID","SERIAL_NUMBER","REGISTRATION_DT","REGISTRATION_NUMBER","POSTREG_CATEGORY","START_ACTION_NUMBER","END_ACTION_NUMBER","START_ACTION_DATE","END_ACTION_DATE","START_5_CHARACTERS","END_5_CHARACTERS","START_CM_DESC","END_CM_DESC","fifteen_flag","INVENTORY","FIRST_ACTION_DATE","FIRST_ACTION_CODE","RENEWAL_DT","RENEWAL_NUMBER","FIRST_ACTION_PENDENCY","TOTAL_PENDENCY","Max_Max_DT","Expiration_Type_RealTime2","Expiration_DT_RealTime2","MAX_FY_PH","SixYR_Disposed_Count","SixYR_Base","TenYR_Disposed_Count","TenYR_Base","End_Action_FY","SER_NUM","Pendency_Cal_Start_DT","non_pro_se","pctram_link","LAW_OFFICE","FILING_BASIS_GRP","FILING_METHOD_CUR","AM_STAT","Owner_Name","CITY","State","country_or_area_name","Reg_Class_Count","Active_Class_Count","Group_Type","FA_PERCENTILE","Right_RecordID","FA_PERCENTILE_INCLUDE","TP_PERCENTILE","TP_PERCENTILE_INCLUDE","Top10_FY_Exclude_CFY","Top5_FY_Exclude_CFY","RENEWAL_NUMBER_GRP","Category","Concat_Class","FIRST_ACTION_INVENTORY","REG_FY","Drop_Off_Year").distinct()

#df648.display()
#df648.count()
#9,915,475

# COMMAND ----------

# MAGIC %md
# MAGIC ##WORKLOAD HISTORY (FROM FY: 1990) AND FUTURE 10 YEARS ESTIMATION

# COMMAND ----------

# MAGIC %md
# MAGIC ###10yr6yrcombined

# COMMAND ----------

df42 = df719.withColumn("Next_6YR_DT_NoNull", when((col("Next_6YR_DT").isNull())| (col("Next_6YR_DT") == '') ,add_months(col("REGISTRATION_DT"),12*6)).otherwise(col("Next_6YR_DT")))\
    .withColumn("Next_10Yr_DT_NoNull", when(col("Next_10Yr_Renewal").isNull(),add_months(col("REGISTRATION_DT"),12*10)).otherwise(col("Next_10Yr_Renewal")))

df43 = df42.where((datediff(col("Next_6YR_DT_NoNull"),current_date())>=0) & (floor((months_between(col("Next_6YR_DT_NoNull"),current_date()))/12) <=5 ))

df43_f = df42.where(~((datediff(col("Next_6YR_DT_NoNull"),current_date())>=0) & (floor((months_between(col("Next_6YR_DT_NoNull"),current_date()))/12) <=5 )))
#df42.where((datediff(col("Next_6YR_DT_NoNull"),current_date())<5))
#df43_f.display()

df44 = df42.where((datediff(col("Next_10Yr_DT_NoNull"),current_date())>=0) & (floor((months_between(col("Next_10Yr_DT_NoNull"),current_date()))/12) <=9 ))
                  #(datediff(col("Next_10Yr_DT_NoNull"),current_date())>=9))
#df44.display()
df44_f = df42.where(~((datediff(col("Next_10Yr_DT_NoNull"),current_date())>=0) & (floor((months_between(col("Next_10Yr_DT_NoNull"),current_date()))/12) <=9 )))
#df44_f.display()


df47 = df43.withColumn("PostRegCat",lit("6 YEAR")).withColumn("Count", lit(1)).selectExpr("Next_6YR_DT_NoNull as Date","PostRegCat","Count")
#df47.display()

df81 = df43_f.where((datediff(col("Next_6YR_DT_NoNull"),current_date())<0))
df83 = df81.withColumn("PostRegCat",lit("6 YEAR")).withColumn("Count", lit(1)).withColumn("Fiscal_YR",when(month("Next_6YR_DT_NoNull")>9,year("Next_6YR_DT_NoNull")+1).otherwise(year("Next_6YR_DT_NoNull"))).where(col("Fiscal_YR")>1989).selectExpr("Next_6YR_DT_NoNull as Date","PostRegCat","Count")
#df83.display()

df48 = df44.withColumn("PostRegCat",lit("10 YEAR")).withColumn("Count", lit(1)).selectExpr("Next_10Yr_DT_NoNull as Date","PostRegCat","Count")

df84 = df44_f.where((datediff(col("Next_10Yr_DT_NoNull"),current_date())<0))
df85 = df84.withColumn("PostRegCat",lit("10 YEAR")).withColumn("Count", lit(1)).withColumn("Fiscal_YR",when(month("Next_10Yr_DT_NoNull")>9,year("Next_10Yr_DT_NoNull")+1).otherwise(year("Next_10Yr_DT_NoNull"))).where(col("Fiscal_YR")>1989).selectExpr("Next_10Yr_DT_NoNull as Date","PostRegCat","Count")
#df85.display()

df49 = df47.unionByName(df83,allowMissingColumns=True).unionByName(df48,allowMissingColumns=True).unionByName(df85,allowMissingColumns=True)
#df49.display()

df50 = df49.groupBy("Date","PostRegCat").agg(f.sum("Count").alias("Base_Total"))
#df50.display()
#df50.count()
#7,638

# COMMAND ----------

# MAGIC %md
# MAGIC ###6YRAVG

# COMMAND ----------

df57 = df719.where(col("six_yr_dt").isNotNull())
df53 = df57.withColumn("six_FY",when(month("six_yr_dt")>9,year("six_yr_dt")+1).otherwise(year("six_yr_dt")))
df56 = df53.groupBy("six_FY").agg(f.countDistinct("SERIAL_NUMBER").alias("six_total"))
#df56.display()

df54 = df719.where((col("six_yr_dt").isNotNull()) | (col("Expiration_Type_RealTime") == "6 YEAR"))
df55 = df54.withColumn("Date", when(col("six_yr_dt").isNotNull(), col("six_yr_dt")).otherwise(col("Expiration_DT_RealTime")))\
    .withColumn("Base_FY",when(month("Date")>9,year("Date")+1).otherwise(year("Date")))
df58 = df55.groupBy("Base_FY").agg(f.countDistinct("SERIAL_NUMBER").alias("Base_total"))
#df58.display()

df59 = df56.join(df58, df56.six_FY == df58.Base_FY, "inner")#inner
df60 = df59.withColumn("Rate",round(col("six_total")/col("Base_total"),2)).orderBy("six_FY")
df62 = df60.groupBy().agg(max("six_FY").alias("Max_six_FY"))
#df62.display()
df63 = df60.join(df62,df60.six_FY == df62.Max_six_FY,"anti")#anti
#df63.display()

##Alteryx Missing sort before filtering last 5 rows
df65 = spark.createDataFrame(df63.tail(5),df63.schema)
#df65.display()
df64 = df65.agg(round(avg("Rate"),17).cast(DecimalType(precision=19, scale=17)).alias("Avg_6YR_Rate")).collect()[0][0]
#df64.display()

df51 = df50.withColumn("Avg_6YR_Rate",lit(f"{df64}"))
#
# df51.display()
#df51.count()
#7,638
#print(df64)

# COMMAND ----------

# MAGIC %md
# MAGIC ###10YRAVG

# COMMAND ----------

df71 = df719.where(col("last_10yr_dt").isNotNull())
df67 = df71.withColumn("ten_FY",when(month("last_10yr_dt")>9,year("last_10yr_dt")+1).otherwise(year("last_10yr_dt")))
df70 = df67.groupBy("ten_FY").agg(f.countDistinct("SERIAL_NUMBER").alias("ten_total"))

df68 = df719.where((col("last_10yr_dt").isNotNull()) | (col("Expiration_Type_RealTime").isNotNull()))

df69 = df68.withColumn("Expiration_DT_RealTime",when(col("Expiration_Type_RealTime") == "6 YEAR",add_months(col("REGISTRATION_DT"),12*10)).otherwise(col("Expiration_DT_RealTime")))\
    .withColumn("Date", expr("case when Expiration_DT_RealTime is null then LAST_10YR_DT\
        when LAST_10YR_DT is null then Expiration_DT_RealTime\
            when LAST_10YR_DT > Expiration_DT_RealTime then LAST_10YR_DT \
                else Expiration_DT_RealTime end"))\
    .withColumn("Base_FY",when(month("Date")>9,year("Date")+1).otherwise(year("Date")))
#df69.display()
df72 = df69.groupBy("Base_FY").agg(f.countDistinct("SERIAL_NUMBER").alias("Base_total"))

df73 = df70.join(df72, df70.ten_FY == df72.Base_FY,"inner")
#df73.display()

df74 = df73.withColumn("Rate",round(col("ten_total")/col("Base_total"),2)).orderBy("ten_FY")

df77 = df74.agg(max("ten_FY").alias("Max_ten_FY"))
#df77.display()

df78 = df74.join(df77,df74.ten_FY == df77.Max_ten_FY,"anti")#anti
#df78.display()

#alteryx missing sort before filtering last 5 rows

df75 = spark.createDataFrame(df78.tail(5),df78.schema)
#df75.display()
df79 = df75.agg(round(avg("Rate"),17).cast(DecimalType(precision=19, scale=17)).alias("Avg_10YR_Rate")).collect()[0][0]
df52 = df51.withColumn("Avg_10YR_Rate",lit(f"{df79}"))
#df52.display()
spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df52")
df52 = df52.checkpoint(True)
#df52.count()
#7,638

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get History From Post Registration Detail Table

# COMMAND ----------

df94 = df729.where((col("POSTREG_CATEGORY")  == "10 YEAR") | (col("POSTREG_CATEGORY")  == "6 YEAR"))
#df94.count()

df92 = df94.withColumn("Fiscal_Year",when(month("START_ACTION_DATE")>9,year("START_ACTION_DATE")+1).otherwise(year("START_ACTION_DATE"))).withColumn("Count",lit(1))
df93 = df92.groupBy("START_ACTION_DATE","POSTREG_CATEGORY").agg(sum("Count").alias("Sum_Count"))
#df93.count()

# COMMAND ----------

df94 = df729.where((col("POSTREG_CATEGORY")  == "10 YEAR") | (col("POSTREG_CATEGORY")  == "6 YEAR"))
#df94.display()
df92 = df94.withColumn("Fiscal_Year",when(month("START_ACTION_DATE")>9,year("START_ACTION_DATE")+1).otherwise(year("START_ACTION_DATE"))).withColumn("Count",lit(1))
df93 = df92.groupBy("START_ACTION_DATE","POSTREG_CATEGORY").agg(sum("Count").alias("Sum_Count"))

df95 = df93.withColumn("Actual_Estimated",lit("Actual"))\
    .withColumn("Fiscal_Year",when(month("START_ACTION_DATE")>9,year("START_ACTION_DATE")+1).otherwise(year("START_ACTION_DATE")))\
    .withColumn("AVG_6YR_Rate",lit(None))\
    .withColumn("AVG_10YR_Rate",lit(None)).where(col("Fiscal_Year")>1989).selectExpr("START_ACTION_DATE as Date","POSTREG_CATEGORY as PostRegCat","Sum_Count as Base_Total")    
#df95.display()
#df95.count()
#22,546

# COMMAND ----------

# MAGIC %md
# MAGIC ##WORKLOAD HISTORY (FROM FY: 1990) AND FUTURE 10 YEARS ESTIMATION
# MAGIC ###FILL 10TH YEAR Gap Through End of the Fiscal Year 

# COMMAND ----------

##Get 10 Year Volume By Year - Calculate YOY Change and Rolling 5 Years Average to fill Gap for Future 10th FY
df114 = df52.where(col("PostRegCat") == "10 YEAR" )
df113 =df114.withColumn("Fiscal_Year",when(month("Date")>9,year("Date")+1).otherwise(year("Date")))
df112=df113.groupBy("Fiscal_Year").agg(sum("Base_Total").alias("10YR_Volume"))
df110 = df112.agg(max("Fiscal_Year").alias("Max_Fiscal_Year"))
df109_inner = df112.alias("df112").join(df110.alias("df110"), df110.Max_Fiscal_Year == df112.Fiscal_Year,"inner").select("df112.*")
#df109_inner.display()
df109_anti = df112.alias("df112").join(df110.alias("df110"), df110.Max_Fiscal_Year == df112.Fiscal_Year,"anti").select("df112.*")
#df109_anti.display()
df108 = df109_inner.withColumn("Actual_Estimated",lit("Actual")).withColumn("10YR_Actual_Volume",col("10YR_Volume"))
df123 = df108.drop("10YR_Actual_Volume")
#df108.display()
df107 = df109_inner.withColumn("Actual_Estimated",lit("Estimated")).withColumn("10YR_Volume",lit(None)).withColumn("10YR_Growth",lit(None))
#df107.display()
df106 = df109_anti.withColumn("Actual_Estimated",lit("Actual"))
#df106.display()
df105 = df106.unionByName(df107,allowMissingColumns=True)
#df105.display()

###########Calculations not matching with Alteryx due to missing order by  on Fiscal_Year

df104 = df105.withColumn("10YR_Growth",expr("round(((10YR_Volume - (lag(10YR_Volume,1,10YR_Volume) over(order By Fiscal_Year)))/ (lag(10YR_Volume,1,10YR_Volume) over(order By Fiscal_Year)))*100,6)").cast(DecimalType(precision=19, scale=6)))
#df104 = df105.withColumn("10YR_Growth",expr("((10YR_Volume - (lag(10YR_Volume,1,10YR_Volume) over(order By Fiscal_Year)))/ (lag(10YR_Volume,1,10YR_Volume) over(order By Fiscal_Year)))*100"))

#SetValues to ClosestValid Row --last 5
default_lag_value =df104.where(col('10YR_Growth').isNotNull()).agg(first("10YR_Growth").alias("default_10YR_Growth")).collect()[0][0]
df103 = df104.withColumn("5_YR_AVG_Growth_Rate",expr(f"""case when 10YR_Growth is not null then round((((lag(10YR_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(10YR_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(10YR_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(10YR_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ 10YR_Growth )/5),6) else null end""").cast(DecimalType(precision=19, scale=6)))
#df103.display()

#verify the average when 10YR_Growth is nul
#.cast(DecimalType(precision=19, scale=6))

#df103_avg = df103.withColumn("5_YR_AVG_Growth_Rate", when(col("5_YR_AVG_Growth_Rate")==0,0).otherwise(col("5_YR_AVG_Growth_Rate")/5))
#df103_avg.display()
#case when 10YR_Growth is not null
#row=1 (row1,row1,row1,row1,row1)
#row=2 (row2,row1,row1,row1,row1)
#row=3 (row3,row2,row1,row1,row1)
#row=4 (row4,row3,row2,row1,row1)
#row=5 (row5,row4,row3,row2,row1)
#avg  row1+row2+rpw3+row4+row5/5

df102 = df103.withColumn("10YR_Volume",expr("case when 10YR_Volume is null then round((lag(10YR_Volume,1) over(order By Fiscal_Year) + (lag(10YR_Volume,1) over(order By Fiscal_Year) * lag(5_YR_AVG_Growth_Rate,1) over(order By Fiscal_Year)) /100),0) else 10YR_Volume end").cast(DecimalType(precision=19, scale=0)))
#df102.display()

df101 = df102.withColumn("10YR_Growth",expr("round(((10YR_Volume - (lag(10YR_Volume,1,10YR_Volume) over(order By Fiscal_Year)))/ (lag(10YR_Volume,1,10YR_Volume) over(order By Fiscal_Year)))*100,6)").cast(DecimalType(precision=19, scale=6)))
#df101.display()


default_lag_value =df101.where(col('10YR_Growth').isNotNull()).agg(first("10YR_Growth").alias("default_10YR_Growth")).collect()[0][0]
df128 = df101.withColumn("5_YR_AVG_Growth_Rate",expr(f"""case when 10YR_Growth is not null then round((((lag(10YR_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(10YR_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(10YR_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(10YR_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ 10YR_Growth )/5),6) else null end""").cast(DecimalType(precision=19, scale=6)))
#df128.display()
#https://stackoverflow.com/questions/67205503/how-to-fill-in-null-values-in-pyspark
#https://stackoverflow.com/questions/36343482/fill-in-null-with-previously-known-good-value-with-pyspark

#df128.count()

# COMMAND ----------

df120_inner = df128.alias("df128").join(df108.alias("df108"),df128.Fiscal_Year == df108.Fiscal_Year ,"inner").select("df128.*","df108.10YR_Actual_Volume")
#df120_inner.display()
df120_anti = df128.alias("df128").join(df108.alias("df108"),df128.Fiscal_Year == df108.Fiscal_Year,"anti").select("df128.*")
#df120_anti.display()

df121 = df120_inner.withColumn("10YR_Volume",col("10YR_Volume")-col("10YR_Actual_Volume")).drop("10YR_Actual_Volume")
#df121.display()

df124 = df121.unionByName(df120_anti,allowMissingColumns=True).unionByName(df123,allowMissingColumns=True)
#df124.display()

df129 = df124.orderBy("Fiscal_Year").where(col("Actual_Estimated")=="Estimated")
#df129.display()
#df129.count()

# COMMAND ----------

##Get Filler Dates through End of the LAST (10TH) Fiscal Year by determining MAX Filing Date 
df115 = df52.groupBy("PostRegCat").agg(max("Date").alias("Max_Date"))
df117 = df115.where(col("PostRegCat")=="10 YEAR")
#df117.display()
df116 = df117.withColumn("Max_DT_FY",when(month("Max_Date")>9,year("Max_Date")+1).otherwise(year("Max_Date")))\
    .withColumn("FY_END_DATE",concat(col("Max_DT_FY"),lit("-09-30")).cast(DateType()))\
    .withColumn("Days_To_Fiscal_End",datediff(col("FY_END_DATE"),col("Max_Date")))
#df116.display()

df118 = df116.selectExpr("PostRegCat","Max_Date as 10YR_Future_Start","Max_DT_FY","FY_END_DATE as 10YR_Future_End","Days_To_Fiscal_End")
#df118.display()

from pyspark.sql.functions import col, to_date, date_add

# Create a DataFrame with all dates in the range (for example, covering the possible fiscal years)
min_date = df118.selectExpr("to_date(10YR_Future_Start)").collect()[0][0]
max_date = df118.selectExpr("to_date(10YR_Future_End)").collect()[0][0]

# Generate a DataFrame with all dates from min_date+1 to max_date
date_range = spark.range(0, (max_date - min_date).days + 1).toDF("offset") \
    .withColumn("Fiscal_Date", date_add(lit(min_date), col("offset").cast("int"))) \
    .filter(col("Fiscal_Date") > min_date)  # start from min_date + 1

# Cross join and filter to get only the relevant dates for each row in df118
df126 = (
    df118.crossJoin(date_range)
    .filter(
        (col("Fiscal_Date") <= col("10YR_Future_End")) &
        (col("Fiscal_Date") > col("10YR_Future_Start"))
    )
)

df127 = df126.selectExpr("PostRegCat","Max_DT_FY","Days_To_Fiscal_End","Fiscal_Date as Date")
#df127.display()
##(3) FILL Gap for the Tail End of Final FY: About same day 10 years From Now TO End of 10th FY Example: May 3 2029 - Sep 30, 2029)
df130 = df129.alias("df129").join(df127.alias("df127"),df129.Fiscal_Year == df127.Max_DT_FY, "inner")
#df130.display()

df132 = df130.withColumn("Base_Total", round((col("10YR_Volume")/col("Days_To_Fiscal_End")),2 ).cast(DecimalType(precision=19, scale=2))).select("Date","PostRegCat","Base_Total","Actual_Estimated")
#df132.display()
#df132.count()

# COMMAND ----------

##(2) Future Estimates (RegDT+10 Yrs) Today TO Sometime Same Day 10 Yr From Now)
df145 = df48.groupBy("Date","PostRegCat").agg(sum("Count").alias("Base_Total"))
df142 = df145.where(col("PostRegCat") == "10 YEAR").withColumn("Actual_Estimated",lit("Estimated")).select("Date","PostRegCat","Base_Total","Actual_Estimated")
#df142.display()

#(1) History From Post Reg Detail Table (FY 1990 to Yesterday)
df140 = df95.where(col("PostRegCat") == "10 YEAR").withColumn("Actual_Estimated",lit("Actual")).select("Date","PostRegCat","Base_Total","Actual_Estimated")
#df140.display()

df133 = df132.unionByName(df142,allowMissingColumns=True).unionByName(df140,allowMissingColumns=True).orderBy("Date")
#df133.display()
##Append 6 YR and 10 YR Rates and Format 
df147 = df133.withColumn("Avg_6YR_Rate",lit(f"{df64}").cast(DecimalType(precision=19, scale=17)))
#df147.display()

df148 = df147.withColumn("Avg_10YR_Rate",lit(f"{df79}").cast(DecimalType(precision=19, scale=17)))
#df148.display()

df100 = df148.withColumn("Fiscal_Year",when(month("Date")>9,year("Date")+1).otherwise(year("Date"))).orderBy("Date")
#df100.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fill 6 YEAR Gaps through END of 10th Fiscal Year 

# COMMAND ----------

#Data from 6th FY may be incomplete depending on where we stand in the current FY, so calculate Growth Rate to fill that gap for partial 6th FY. Also build out estimates for FYs 7, 8, 9 and 10 

df241 = df52.where(col("PostRegCat") == "6 YEAR")
df240 = df241.withColumn("Fiscal_Year",when(month("Date")>9,year("Date")+1).otherwise(year("Date"))).orderBy("Date")
df239 = df240.groupBy("Fiscal_Year").agg(sum("Base_Total").alias("SIX_YR_Volume"))
#df239.display()
df238 = df239.agg(max("Fiscal_Year").alias("Max_Fiscal_Year"))

df237_inner = df239.alias("df239").join(df238.alias("df238"), df239.Fiscal_Year == df238.Max_Fiscal_Year, "inner").select("df239.*")
#df237_inner.display()

df237_anti = df239.alias("df239").join(df238.alias("df238"), df239.Fiscal_Year == df238.Max_Fiscal_Year, "anti").select("df239.*")
#df237_anti.display()

df160 = df237_inner.withColumn("Actual_Estimated", lit("Actual")).withColumn("SIX_YR_Actual_Volume",col("SIX_YR_Volume"))
#df160.display()

df161 = df237_inner.withColumn("SIX_YR_Volume", lit(None)).withColumn("SIX_YR_Growth",lit(None)).withColumn("Actual_Estimated", lit("Estimated"))
#df161.display()

df159 = df237_anti.withColumn("Actual_Estimated", lit("Actual"))

df158 = df159.unionByName(df161,allowMissingColumns=True)
#df158.display()

##Alteryx missing sort on fiscal year

df204 = df158.withColumn("SIX_YR_Growth",expr("round(((SIX_YR_Volume - (lag(SIX_YR_Volume,1,SIX_YR_Volume) over(order By Fiscal_Year)))/ (lag(SIX_YR_Volume,1,SIX_YR_Volume) over(order By Fiscal_Year)))*100 ,6)").cast(DecimalType(precision=19, scale=6)))
#df204.display()

df204_notnull = df204.where(col('SIX_YR_Growth').isNotNull())
if df204_notnull.count()>0:
    default_lag_value = df204.where(col('SIX_YR_Growth').isNotNull()).agg(first("SIX_YR_Growth").alias("default_SIX_YR_Growth")).collect()[0][0]
else:
    default_lag_value = 0
#print(default_lag_value)

df203 = df204.withColumn("5_YR_AVG_Growth_Rate",expr(f"""case when SIX_YR_Growth is not null then round((((lag(SIX_YR_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ SIX_YR_Growth )/5),6) else null end""").cast(DecimalType(precision=19, scale=6)))
#df203.display()

df151 = df203.withColumn("SIX_YR_Volume",expr("case when SIX_YR_Volume is null then (lag(SIX_YR_Volume,1) over(order By Fiscal_Year) + (lag(SIX_YR_Volume,1) over(order By Fiscal_Year) * lag(5_YR_AVG_Growth_Rate,1) over(order By Fiscal_Year)) /100) else SIX_YR_Volume end").cast(IntegerType()))
#df151.display()


df205 = df151.withColumn("SIX_YR_Growth",expr("case when SIX_YR_Growth is null then round((((SIX_YR_Volume - (lag(SIX_YR_Volume,1,SIX_YR_Volume) over(order By Fiscal_Year)))/ (lag(SIX_YR_Volume,1,SIX_YR_Volume) over(order By Fiscal_Year)))*100),6) else SIX_YR_Growth end ").cast(DecimalType(precision=19, scale=6)))
#df205.display()

df205_notnull = df205.where(col('SIX_YR_Growth').isNotNull())
if df205_notnull.count()>0:
    default_lag_value =df205.where(col('SIX_YR_Growth').isNotNull()).agg(first("SIX_YR_Growth").alias("default_SIX_YR_Growth")).collect()[0][0]
else:
    default_lag_value = 0

df242 = df205.withColumn("5_YR_AVG_Growth_Rate",expr(f"""case when SIX_YR_Growth is not null then round((((lag(SIX_YR_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ SIX_YR_Growth )/5),6) else null end""").cast(DecimalType(precision=19, scale=6)))
#df242.display()

df206 = df242.select("Fiscal_Year","SIX_YR_Volume","SIX_YR_Growth","5_YR_AVG_Growth_Rate","Actual_Estimated")
#df206.display()
#df206.count()

# COMMAND ----------

df157_inner = df206.alias("df206").withColumnRenamed("SIX_YR_Volume","Right_SIX_YR_Volume").join(df160.alias("df160"),df206.Fiscal_Year == df160.Fiscal_Year, "inner").select("df206.Fiscal_Year","df206.5_YR_AVG_Growth_Rate","df206.Actual_Estimated","df160.SIX_YR_Volume","Right_SIX_YR_Volume","df206.SIX_YR_Growth","df160.SIX_YR_Actual_Volume")

df157_anti = df206.alias("df206").join(df160.alias("df160"),df206.Fiscal_Year == df160.Fiscal_Year, "anti")

df156 = df157_inner.withColumn("Six_YR_Volume",(col("Right_SIX_YR_Volume")- col("SIX_YR_Actual_Volume")).cast(IntegerType())).select("SIX_YR_Volume","5_YR_AVG_Growth_Rate","Actual_Estimated","SIX_YR_Growth","Fiscal_Year")

df154 = df160.select("SIX_YR_Volume","Actual_Estimated","Fiscal_Year")
df153 = df156.unionByName(df157_anti,allowMissingColumns=True).unionByName(df154,allowMissingColumns=True)
#df153.display()
df221 = df153.orderBy("Fiscal_Year")
df245 = df221.where(col("Actual_Estimated")=="Estimated")
#df245.display()
#df245.count()

# COMMAND ----------

df202 = df52.groupBy("PostRegCat").agg(max("Date").alias("Max_Date"))
#df202.display()
#Calculate Date Gaps for 6 Yr and 10 Yr
df201 = df202.where(col("PostRegCat") == "6 YEAR")
df171 = df201.select("Max_Date")
#df171.display()
df170 = df171.withColumn("Fiscal_Year",year(col("Max_Date"))).withColumn("FY_PLUS1",col("Fiscal_Year")+1).withColumn("FY_PLUS2",col("Fiscal_Year")+2).withColumn("FY_PLUS3",col("Fiscal_Year")+3).withColumn("FY_PLUS4",col("Fiscal_Year")+4).withColumn("FY_PLUS5",col("Fiscal_Year")+5)
#df170.display()

df169 = df170.melt(
    ids=["Max_Date"], values=["FY_PLUS1", "FY_PLUS2","FY_PLUS3", "FY_PLUS4","FY_PLUS5"],
    variableColumnName="Name", valueColumnName="FISCAL_YEAR"
).drop("Max_Date","Name")
#df169.display()
df209 = df169.withColumn("Actual_Estimated",lit("Estimated"))

df207 = df209.unionByName(df206,allowMissingColumns=True)
#df207.display()
df208 = df207.orderBy("Fiscal_Year")
#df208.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Estimate New Regs for Future Years Based on Growth (5YR Rolling Avg)

# COMMAND ----------

def new_regs_for_future_years(df_input):#df208
    df177 = df_input.withColumn("SIX_YR_Volume",expr("case when SIX_YR_Volume is null then round((lag(SIX_YR_Volume,1) over(order By Fiscal_Year) + (lag(SIX_YR_Volume,1) over(order By Fiscal_Year) * lag(5_YR_AVG_Growth_Rate,1) over(order By Fiscal_Year)) /100)) else SIX_YR_Volume end").cast(IntegerType()))

    df178 = df177.withColumn("SIX_YR_Growth",expr("case when SIX_YR_Growth is null then round((((SIX_YR_Volume - (lag(SIX_YR_Volume,1,SIX_YR_Volume) over(order By Fiscal_Year)))/ (lag(SIX_YR_Volume,1,SIX_YR_Volume) over(order By Fiscal_Year)))*100),6) else SIX_YR_Growth end ").cast(DecimalType(precision=19, scale=6)))
    #df178.display()

    df178_notnull = df178.where(col('SIX_YR_Growth').isNotNull())
    if df178_notnull.count()>0:
        default_lag_value = df178.where(col('SIX_YR_Growth').isNotNull()).agg(first("SIX_YR_Growth").alias("default_SIX_YR_Growth")).collect()[0][0]
    else:
        default_lag_value = 0
    #print(default_lag_value)

    df179 = df178.withColumn("5_YR_AVG_Growth_Rate",expr(f"""case when SIX_YR_Growth is not null then round((((lag(SIX_YR_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(SIX_YR_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ SIX_YR_Growth )/5),6) else 5_YR_AVG_Growth_Rate end""").cast(DecimalType(precision=19, scale=6)))

    #df179.display()

    df_output = df179.withColumn("5_YR_AVG_Growth_Rate",expr("case  when SIX_YR_Volume is null then null else 5_YR_AVG_Growth_Rate end "))
    #df180.display()

    return(df_output)

# COMMAND ----------

df_output1 = new_regs_for_future_years(df208)
df_output2 = new_regs_for_future_years(df_output1)
df_output3 = new_regs_for_future_years(df_output2)
df_output4 = new_regs_for_future_years(df_output3)
df_output5 = new_regs_for_future_years(df_output4)
df224 = df_output5.withColumn("FY_NUMBER",col("FISCAL_YEAR").cast(IntegerType())).where(col("Actual_Estimated")== "Estimated")
#df224.count()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, date_add, lit
from pyspark.sql.types import IntegerType
#Get Missing Calendar Dates for Partial 6th FY Year (Example: May 2, 2025 - Sep 30 2025)
df677 = df201.withColumn("6YR_Future_Date",when(month("Max_Date")<=9,add_months(col("Max_Date"),12*4)).otherwise(add_months(col("Max_Date"),12*5)))
df676 = df677.selectExpr("PostRegCat","Max_Date as 6YR_Future_Start","6YR_Future_Date as 6YR_Future_End")
df678 = df676.withColumn("GET_RIGHT_FY",when(month("6YR_Future_Start")<=9, year(col("6YR_Future_Start"))).otherwise(year(add_months(col("6YR_Future_Start"),12))))\
    .withColumn("FY_END_DATE",concat(col("GET_RIGHT_FY"),lit("-09-30")).cast(DateType()))\
    .withColumn("Days_To_Fiscal_End",datediff(col("FY_END_DATE"),col("6YR_Future_Start")))\
    .withColumn("Actual_Estimated",lit("Estimated"))
#df678.display()    

# Get min and max dates for the range
min_date = df678.agg({"6YR_Future_Start": "min"}).collect()[0][0]
max_date = df678.agg({"FY_END_DATE": "max"}).collect()[0][0]

# Generate a DataFrame with all dates in the possible range
date_range = (
    spark.range(0, (max_date - min_date).days + 1)
    .toDF("offset")
    .withColumn("offset", col("offset").cast(IntegerType()))
    .withColumn("Fiscal_Date", date_add(lit(min_date), col("offset")))
)

# Cross join and filter to get only the relevant dates for each row in df678
df174 = (
    df678.crossJoin(date_range)
    .filter(
        (col("Fiscal_Date") > col("6YR_Future_Start")) &
        (col("Fiscal_Date") <= col("FY_END_DATE"))
    )
)
#df174.display()
df172 = df174.selectExpr("GET_RIGHT_FY as FISCAL_YEAR","Fiscal_Date as Date","PostRegCat","Days_To_Fiscal_End","Actual_Estimated")
#df172.count()
#4

# COMMAND ----------

df246 = df245.alias("df245").join(df172.alias("df172"),df245.Fiscal_Year == df172.FISCAL_YEAR,"inner").selectExpr("df245.*","df172.FISCAL_YEAR as Right_FISCAL_YEAR","df172.Date","df172.PostRegCat","df172.Days_To_Fiscal_End","df172.Actual_Estimated as Right_Actual_Estimated")
df248 = df246.withColumn("Base_Total", round((col("SIX_YR_Volume")/col("Days_To_Fiscal_End")),2).cast(DecimalType(precision=19, scale=2)))\
    .withColumn("Fiscal_Year",when(month("Date")>9,year("Date")+1).otherwise(year("Date"))).select("Fiscal_Year","Date","PostRegCat","Base_Total","Actual_Estimated")
#df248.display()
#df248.count()

# COMMAND ----------

from pyspark.sql.functions import col, date_add, lit
from pyspark.sql.types import IntegerType

#Get Calendar Dates for Full FYs 7, 8, 9 and 10; Example:
#Oct 1 2025 - Sep 30 2026
#Oct 1 2026 - Sep 30 2027
#Oct 1 2027 - Sep 30 2028
#Oct 1 2028 - Sep 30 2029
df675 = df201.withColumn("6YR_Future_Start_YR",year(col("Max_Date")))\
    .withColumn("FY_START_DATE",concat(col("6YR_Future_Start_YR"),lit("-10-01")).cast(DateType()))\
    .withColumn("6YR_Future_End_YR",when(month("Max_Date")<=9,year(add_months(col("Max_Date"),12*4))).otherwise(year(add_months(col("Max_Date"),12*5))))\
    .withColumn("FY_END_DATE",concat(col("6YR_Future_End_YR"),lit("-09-30")).cast(DateType()))

#generate rows
min_date = df675.agg({"FY_START_DATE": "min"}).collect()[0][0]
max_date = df675.agg({"FY_END_DATE": "max"}).collect()[0][0]

# Generate a DataFrame with all dates in the possible range
date_range = (
    spark.range(0, (max_date - min_date).days + 1)
    .toDF("offset")
    .withColumn("offset", col("offset").cast(IntegerType()))
    .withColumn("Fiscal_Date", date_add(lit(min_date), col("offset")))
)

# Cross join and filter to get only the relevant dates for each row in df675
df212 = (
    df675.crossJoin(date_range)
    .filter(
        (col("Fiscal_Date") >= col("FY_START_DATE")) &
        (col("Fiscal_Date") <= col("FY_END_DATE"))
    )
)


df214 = df212.withColumn("Actual_Estimated",lit("Estimated"))\
    .withColumn("FISCAL_YEAR",when(month("Fiscal_Date")>9,year("Fiscal_Date")+1).otherwise(year("Fiscal_Date")))
df215 = df214.selectExpr("FISCAL_YEAR","Fiscal_Date as Date","PostRegCat","Actual_Estimated")

df216 = df172.unionByName(df215,allowMissingColumns=True)
#df216.count()

# COMMAND ----------

#Get Actual Data and Combine with Estimated Dates
df165 = df95.where(col("PostRegCat")=="6 YEAR")
df167 = df165.withColumn("FISCAL_YEAR",when(month("Date")>9,year("Date")+1).otherwise(year("Date")))\
    .withColumn("Actual_Estimated",lit("Actual"))\
    .withColumn("FY_NUMBER",col("FISCAL_YEAR").cast(IntegerType()))
df220 = df221.alias("df221").join(df167.alias("df167"), (df221.Fiscal_Year == df167.FISCAL_YEAR) & (df221.Actual_Estimated == df167.Actual_Estimated),"inner").selectExpr("df221.Fiscal_Year","df167.FISCAL_YEAR as Right_FISCAL_YEAR","df167.Date","df167.PostRegCat","df167.Base_Total","df167.Actual_Estimated")
#df220.display()

df232 = df167.groupBy().agg(max("Date").alias("Max_Date"), max("FY_NUMBER").alias("Max_FY_NUMBER"))

df233 = df224.alias("df224").join(df232.alias("df232"), df224.FY_NUMBER == df232.Max_FY_NUMBER, "anti")
#df233.display()

df250 = df233.alias("df233").join(df245.alias("df245"),df233.FISCAL_YEAR == df245.Fiscal_Year,"anti")
#df250.display()
#df251 = df250.withColumn("FY_RMG_DAYS",lit(365)).withColumn("Base_Total", (col("SIX_YR_Volume")/col("FY_RMG_DAYS")).cast(DecimalType(precision=19, scale=2))).select("Fiscal_Year","Base_Total","Actual_Estimated")
df251 = df250.withColumn("FY_RMG_DAYS",lit(365)).withColumn("Base_Total", round((col("SIX_YR_Volume")/col("FY_RMG_DAYS")),2)).select("Fiscal_Year","Base_Total","Actual_Estimated")
#df251.display()
df252 = df251.alias("df251").join(df215.alias("df215"), df251.Fiscal_Year == df215.FISCAL_YEAR,"inner").select("df251.Base_Total","df251.Actual_Estimated","df215.FISCAL_YEAR","df215.Date","df215.PostRegCat")
#df252.display()


# COMMAND ----------

df257 = df47.groupby("Date","PostRegCat").agg(sum("Count").alias("Base_Total")).where(col("PostRegCat")=="6 YEAR")
df259 = df257.withColumn("Actual_Estimated",lit("Estimated"))\
    .withColumn("Fiscal_Year",when(month("Date")>9,year("Date")+1).otherwise(year("Date")))

df234 = df232.withColumn("Current_FY_END",concat(col("Max_FY_NUMBER"),lit("-09-30")).cast(DateType()))\
    .withColumn("FY_RMG_DAYS",datediff(col("Current_FY_END"),col("Max_Date")))
df234_max_date = df234.select("Max_Date").collect()[0][0]
df234_fy_rmg_date = df234.select("FY_RMG_DAYS").collect()[0][0]

df235 = df156.withColumn("Max_Date",lit(f"{df234_max_date}")).withColumn("FY_RMG_DAYS",lit(f"{df234_fy_rmg_date}"))

df243 = df167.select("FISCAL_YEAR","Date","PostRegCat","Base_Total","Actual_Estimated")
df254=df243.unionByName(df259,allowMissingColumns=True).unionByName(df252,allowMissingColumns=True).unionByName(df248,allowMissingColumns=True)
df260 = df254.orderBy("Date")

df263 = df260.withColumn("Avg_6YR_Rate",lit(f"{df64}"))
df264 = df263.withColumn("Avg_10YR_Rate",lit(f"{df79}"))
#df264.display()
#df264.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Section 7 - Get Historical Counts, Calculate Growth (5 Year Rolling Averages) and Build Estimates For Next 10 Years

# COMMAND ----------

df307 = df723.select("Expiration_DT","REGISTRATION_DT","SERIAL_NUMBER").distinct()
df310 = df307.melt(
    ids=["serial_number"], values=["REGISTRATION_DT", "Expiration_DT"],
    variableColumnName="LiveRegH_Name", valueColumnName="LiveRegH_Value")
#df310.display()    
df309 = df310.withColumn("LiveRegH_DT",expr("case when LiveRegH_Name ='REGISTRATION_DT' then LiveRegH_Value\
        when LiveRegH_Name ='Expiration_DT' then date_add(LiveRegH_Value,1)\
        else null end"))\
        .withColumn("LiveRegH_Count", expr("case when LiveRegH_Name = 'REGISTRATION_DT' then 1\
            when  LiveRegH_Name = 'Expiration_DT' then -1\
                else 0 end"))\
        .withColumn("LiveRegH_FY",when(month("LiveRegH_DT")>9,year("LiveRegH_DT")+1).otherwise(year("LiveRegH_DT")))\
#df309.display()

df311 = df309.where(col("LiveRegH_DT").isNotNull()).orderBy("LiveRegH_DT",desc("LiveRegH_Count"),"serial_number")
#df311.display()

import sys
#running total
df312 = df311.withColumn('RunTot_LiveRegH_Count', f.sum(df311.LiveRegH_Count).over(Window.partitionBy().orderBy("LiveRegH_DT",desc("LiveRegH_Count"),"serial_number").rowsBetween(-sys.maxsize, 0)))
#df312.display()
spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df312_runtot")
df312 = df312.checkpoint(True)

df315 = df312.groupBy("LiveRegH_FY").agg(max("LiveRegH_DT").alias("Max_LiveRegH_DT"), max("RunTot_LiveRegH_Count").alias("Max_RunTot_LiveRegH_Count")).selectExpr("LiveRegH_FY as FY","Max_LiveRegH_DT","Max_RunTot_LiveRegH_Count as Live_Registrations")
#df315.display()
#df315.count()

# COMMAND ----------

df273 = df731.where(col("postreg_category")=="SECTION 7")
df274 = df273.withColumn("SEC7_Disposed_Count",when((col("END_CM_DESC").isNotNull()) & (col("END_CM_DESC") != "CANCELLED"),lit(1)).otherwise(lit(0)))\
    .withColumn("End_Action_FY",when(month("END_ACTION_DATE")>9,year("END_ACTION_DATE")+1).otherwise(year("END_ACTION_DATE")))\
    .withColumn("Start_Action_FY",when(month("START_ACTION_DATE")>9,year("START_ACTION_DATE")+1).otherwise(year("START_ACTION_DATE")))\
    .withColumn("Registration_FY",when(month("REGISTRATION_DT")>9,year("REGISTRATION_DT")+1).otherwise(year("REGISTRATION_DT")))
df276 = df274.where(col("START_ACTION_DATE").isNotNull())

df275 = df276.groupBy("Start_Action_FY").agg(count("SERIAL_NUMBER").alias("Section_7_Volume"))
df319 = df275.where(col("Start_Action_FY")>1989)
df321 = df319.withColumn("Count",lit(1))

df276_right = df276.withColumnRenamed("Start_Action_FY","Right_Start_Action_FY")

df318 = df321.alias("df321").join(df276_right.alias("df276_right"), df321.Start_Action_FY == df276_right.Right_Start_Action_FY, "inner")
#df318.display()
#df318.count()

# COMMAND ----------

df278 = df275.alias("df275").join(df315.alias("df315"), df275.Start_Action_FY == df315.FY,"inner")#inner to validate the data
df333 = df278.withColumn("Fiscal_Year",col("Start_Action_FY").cast(IntegerType())).orderBy(desc("Fiscal_Year"))
df331 = spark.createDataFrame(df333.tail(df333.count()-1), df333.schema)
df334 = df331.orderBy("Fiscal_Year")
df335 = df334.withColumn("Fiscal_Year",col("Start_Action_FY").cast(IntegerType()))\
    .withColumn("FY_MINUS_1",col("Fiscal_Year")-1)\
    .withColumn("FY_START_DT",concat(col("FY_MINUS_1"),lit("-10-01")).cast(DateType()))\
    .withColumn("FY_END_DT",concat(col("Start_Action_FY"),lit("-09-30")).cast(DateType()))\
    .withColumn("Sec7_Rate_Per_Regs",round(col("Section_7_Volume")/col("Live_Registrations"),6).cast(DecimalType(precision=19, scale=6)))\
    .withColumn("Sec7_Rate_Per_Regs",when((current_date()>= col("FY_START_DT")) & (current_date()<= col("FY_END_DT")), lit(None)).otherwise(col("Sec7_Rate_Per_Regs")))
#df335.display()
#df335.count()

# COMMAND ----------

df291 = df335.withColumn("Reg_Growth",expr("round((((Live_Registrations - (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))/ (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))*100),6) ").cast(DecimalType(precision=19, scale=6)))
#df178.display()

df291_notnull = df291.where(col('Reg_Growth').isNotNull())
if df291_notnull.count()>0:
    default_lag_value = df291.where(col('Reg_Growth').isNotNull()).agg(first("Reg_Growth").alias("default_Reg_Growth")).collect()[0][0]
else:
    default_lag_value = 0
#print(default_lag_value)

df292 = df291.withColumn("5_YR_AVG_Growth_Rate",expr(f""" round((((lag(Reg_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ Reg_Growth )/5),6) """).cast(DecimalType(precision=19, scale=6)))

df292_notnull = df292.where(col('Sec7_Rate_Per_Regs').isNotNull())
if df292_notnull.count()>0:
    default_lag_value = df292.where(col('Sec7_Rate_Per_Regs').isNotNull()).agg(first("Sec7_Rate_Per_Regs").alias("default_Sec7_Rate_Per_Regs")).collect()[0][0]
else:
    default_lag_value = 0
#print(default_lag_value)

df279 = df292.withColumn("5YR_Rate_Avg",expr(f""" round((((lag(Sec7_Rate_Per_Regs,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec7_Rate_Per_Regs,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec7_Rate_Per_Regs,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec7_Rate_Per_Regs,1,{default_lag_value})over(order By Fiscal_Year))+ Sec7_Rate_Per_Regs )/5),6) """).cast(DecimalType(precision=19, scale=6)))

#df283 = df279.orderBy(desc("Fiscal_Year"))
df280 = df279.withColumn("Predicted_5YR_AVG", col("5YR_Rate_Avg")*col("Live_Registrations"))\
    .withColumn("Scetion_7_Volume",when((current_date()>= col("FY_START_DT")) & (current_date() <= col("FY_END_DT")),col("Predicted_5YR_AVG")).otherwise(col("Section_7_Volume")))\
    .withColumn("Sec7_Rate_Per_Regs",round((col("Section_7_Volume")/col("Live_Registrations")),6).cast(DecimalType(precision=19, scale=6)))
    
df282 = df280.select("Start_Action_FY","Predicted_5YR_AVG","5YR_Rate_Avg","Reg_Growth","Sec7_Rate_Per_Regs","FY_END_DT","FY_START_DT","FY_MINUS_1","Fiscal_Year","Live_Registrations","Max_LiveRegH_DT","Section_7_Volume","5_YR_AVG_Growth_Rate")
df281= df282.withColumn("5YR_Delta_PCT",abs(col("Section_7_Volume") - col("Predicted_5YR_AVG"))/((col("Section_7_Volume")+col("Predicted_5YR_AVG"))/2)*100)

w2 = Window.partitionBy().orderBy(desc(col("Fiscal_Year")))
df285 = df281.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row").select("Fiscal_Year")
#df285.display()

#df286.......

df286 = df285.withColumn("FY_CURRENT",col("FISCAL_YEAR")+1).withColumn("FY_PLUS1",col("Fiscal_Year")+2).withColumn("FY_PLUS2",col("Fiscal_Year")+3).withColumn("FY_PLUS3",col("Fiscal_Year")+4).withColumn("FY_PLUS4",col("Fiscal_Year")+5).withColumn("FY_PLUS5",col("Fiscal_Year")+6).withColumn("FY_PLUS6",col("Fiscal_Year")+7).withColumn("FY_PLUS7",col("Fiscal_Year")+8).withColumn("FY_PLUS8",col("Fiscal_Year")+9).withColumn("FY_PLUS9",col("Fiscal_Year")+10).withColumn("FY_PLUS10",col("Fiscal_Year")+11)
#df170.display()

df287 = df286.melt(
    ids=["Fiscal_Year"], values=["FY_CURRENT","FY_PLUS1", "FY_PLUS2","FY_PLUS3", "FY_PLUS4","FY_PLUS5","FY_PLUS6", "FY_PLUS7","FY_PLUS8", "FY_PLUS9","FY_PLUS10"],
    variableColumnName="Name", valueColumnName="Value"
).drop("Fiscal_Year","Name")
#df169.display()
df288 = df287.withColumnRenamed("Value","FISCAL_YEAR")
df337 = df288.withColumn("FY_START_DT",concat((col("FISCAL_YEAR")-1),lit("-10-01")).cast(DateType()))\
    .withColumn("FY_END_DT",concat(col("FISCAL_YEAR"),lit("-09-30")).cast(DateType()))
df289 = df337.unionByName(df281,allowMissingColumns=True)
df290 = df289.select("FY_START_DT","Max_LiveRegH_DT","FY_END_DT","Predicted_5YR_AVG","Reg_Growth","5_YR_AVG_Growth_Rate","Sec7_Rate_Per_Regs","5YR_Delta_PCT","5YR_Rate_Avg","Live_Registrations","Fiscal_Year","Section_7_Volume","FY_MINUS_1")
#df290.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ESTIMATE ONE YEAR AT A TIME FOR NEXT 10 FISCAL YEARS USING 5 YR AVGS

# COMMAND ----------

# DBTITLE 1,Define common function for next 11 runs
def estimate_next10(df_input):
    df347 = df_input.withColumn("Live_Registrations",expr("case when Live_Registrations is not null then Live_Registrations else round((lag(Live_Registrations,1) over(order By Fiscal_Year) + ((lag(Live_Registrations,1) over(order By Fiscal_Year) * lag(5_YR_AVG_Growth_Rate,1) over(order By Fiscal_Year)) /100)),0) end"))

    df351 = df347.withColumn("Reg_Growth",expr("case when Reg_Growth is not null then Reg_Growth else round(((Live_Registrations - (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))/ (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))*100,6) end").cast(DecimalType(precision=19, scale=6)))


    df351_notnull = df351.where(col('Reg_Growth').isNotNull())
    if df351_notnull.count()>0:
        default_lag_value = df351.where(col('Reg_Growth').isNotNull()).agg(first("Reg_Growth").alias("default_Reg_Growth")).collect()[0][0]
    else:
        default_lag_value = 0
    #print(default_lag_value)

    df350 = df351.withColumn("5_YR_AVG_Growth_Rate",expr(f"""case when 5_YR_AVG_Growth_Rate is not null then 5_YR_AVG_Growth_Rate else round((((lag(Reg_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ Reg_Growth )/5),6)  end""").cast(DecimalType(precision=19, scale=6)))

    df348 = df350.withColumn("Section_7_Volume",expr("case when Section_7_Volume is not null then Section_7_Volume else round(((lag(5YR_Rate_Avg,1,5YR_Rate_Avg) over(order By Fiscal_Year))*Live_Registrations),2) end")) 

    df349 = df348.withColumn("5_YR_AVG_Growth_Rate",when(col("Live_Registrations").isNull(),lit(None)).otherwise(col("5_YR_AVG_Growth_Rate")))\
        .withColumn("Sec7_Rate_Per_Regs", when(col("Sec7_Rate_Per_Regs").isNull(),round(col("Section_7_Volume")/col("Live_Registrations"),6)). otherwise(col("Sec7_Rate_Per_Regs")).cast(DecimalType(precision=19, scale=6)))


    df349_notnull = df349.where(col('Sec7_Rate_Per_Regs').isNotNull())
    if df349_notnull.count()>0:
        default_lag_value = df349.where(col('Sec7_Rate_Per_Regs').isNotNull()).agg(first("Sec7_Rate_Per_Regs").alias("default_Reg_Growth")).collect()[0][0]
    else:
        default_lag_value = 0
    #print(default_lag_value)
    df352 = df349.withColumn("5YR_Rate_Avg",expr(f"""case when 5YR_Rate_Avg is not null then 5YR_Rate_Avg else round((((lag(Sec7_Rate_Per_Regs,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec7_Rate_Per_Regs,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec7_Rate_Per_Regs,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec7_Rate_Per_Regs,1,{default_lag_value})over(order By Fiscal_Year))+ Sec7_Rate_Per_Regs )/5),6)  end""").cast(DecimalType(precision=19, scale=6)))

    df_output = df352.withColumn("5YR_Rate_Avg",when(col("Live_Registrations").isNull(), lit(None)).otherwise(col("5YR_Rate_Avg")))

    return(df_output)

# COMMAND ----------

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df_output")
#1
df_output1=estimate_next10(df290)
df_output1 = df_output1.checkpoint(True)
#2
df_output2=estimate_next10(df_output1)
df_output2 = df_output2.checkpoint(True)
#3
df_output3=estimate_next10(df_output2)
df_output3 = df_output3.checkpoint(True)
#4
df_output4=estimate_next10(df_output3)
df_output4 = df_output4.checkpoint(True)
#5
df_output5=estimate_next10(df_output4)
df_output5 = df_output5.checkpoint(True)
#6
df_output6=estimate_next10(df_output5)
df_output6 = df_output6.checkpoint(True)
#7
df_output7=estimate_next10(df_output6)
df_output7 = df_output7.checkpoint(True)
#8
df_output8=estimate_next10(df_output7)
df_output8 = df_output8.checkpoint(True)
#9
df_output9=estimate_next10(df_output8)
df_output9 = df_output9.checkpoint(True)
#10
df_output10=estimate_next10(df_output9)
df_output10 = df_output10.checkpoint(True)
#11
df_output11=estimate_next10(df_output10)
df_output11 = df_output11.checkpoint(True)
#df_output11.count()

# COMMAND ----------

# DBTITLE 1,Replaced 413 with df419 for unit testing
#df419
df425 = df_output11.select("FY_START_DT","FY_MINUS_1","Max_LiveRegH_DT","Fiscal_Year","5YR_Rate_Avg","Sec7_Rate_Per_Regs","Section_7_Volume","5_YR_AVG_Growth_Rate","Reg_Growth","Live_Registrations","FY_END_DT")
df300 = df425.select("FY_START_DT","Fiscal_Year","FY_END_DT").where(col("Fiscal_Year")>= year(current_date()))

df301_min = df300.agg(min(col("FY_START_DT"))).collect()[0][0]
df301_max = df300.agg(max(col("FY_END_DT"))).collect()[0][0]

df303 = df300.withColumn("Max_FY_END_DT",lit(f"{df301_max}")).withColumn("Min_FY_START_DT",lit(f"{df301_min}"))

# Get min and max dates for the range
min_date = df303.selectExpr("to_date(Min_FY_START_DT)").collect()[0][0]
max_date = df303.selectExpr("to_date(Max_FY_END_DT)").collect()[0][0]

# Generate a DataFrame with all dates in the possible range
date_range = (
    spark.range(0, (max_date - min_date).days + 1)
    .toDF("offset")
    .withColumn("offset", col("offset").cast(IntegerType()))
    .withColumn("Fiscal_Date", date_add(lit(min_date), col("offset")))
)

# Cross join and filter to get only the relevant dates for each row in df303
df302 = (
    df303.crossJoin(date_range)
    .filter(
        (col("Fiscal_Date") >= col("Min_FY_START_DT")) &
        (col("Fiscal_Date") <= col("Max_FY_END_DT"))
    )
)

df305 = (
    df302.select(
        "FY_START_DT", "FY_END_DT", "Min_FY_START_DT", "Max_FY_END_DT", "Fiscal_Date"
    )
    .withColumn(
        "Fiscal_Year",
        when(month("Fiscal_Date") > 9, year("Fiscal_Date") + 1).otherwise(year("Fiscal_Date"))
    )
)

df330 = df425.where((current_date()>=col("FY_START_DT")) & (current_date()<=col("FY_END_DT")))
df330_f = df425.where(~((current_date()>=col("FY_START_DT")) & (current_date()<=col("FY_END_DT"))))

df326 = df318.groupBy("Start_Action_FY").agg(sum("Count").alias("Actual_Volume"))
df326 = df326.withColumnRenamed("Start_Action_FY","Right_Fiscal_year")

df320 = df318.groupBy("START_ACTION_DATE").agg(sum("Count").alias("Base_Total"))
df322 = df320.withColumn("Actual_Estimated",lit("Actual"))\
    .withColumn("PostRegCat",lit("SECTION 7"))\
    .withColumn("Fiscal_Year",when(month("START_ACTION_DATE")>9,year("START_ACTION_DATE")+1).otherwise(year("START_ACTION_DATE")))

df323 = df322.selectExpr("START_ACTION_DATE as Date","PostRegCat","Base_Total","Actual_Estimated","Fiscal_Year")

df327 = df330.alias("df330").join(df326.alias("df326"),df330.Fiscal_Year == df326.Right_Fiscal_year,"inner").withColumnRenamed("Section_7_Volume","Estimated_Volume")
df336 = df327.withColumn("Section_7_Volume", col("Estimated_Volume")-col("Actual_Volume")).drop("Right_Fiscal_year","FY_MINUS_1","Max_LiveRegH_DT")

df293 = df330_f.withColumn("Fiscal_year_C",col("Fiscal_Year")).drop("Fiscal_year_C","FY_MINUS_1","Max_LiveRegH_DT")

df338 = df293.unionByName(df336,allowMissingColumns=True)
df339 = df338.orderBy("Fiscal_Year").withColumnRenamed("Fiscal_Year","Right_Fiscal_Year")


df296 = df305.alias("df305").join(df339.alias("df339"), df305.Fiscal_Year == df339.Right_Fiscal_Year,"inner").selectExpr("Fiscal_Date","Fiscal_Year","df339.Live_Registrations","df339.Reg_Growth","df339.5_YR_AVG_Growth_Rate","df339.Section_7_Volume","df339.Sec7_Rate_Per_Regs","df339.5YR_Rate_Avg").distinct()

df299 = df296.where(col("Fiscal_Date")>current_date())
df297 = df299.withColumn("PostRegCat",lit("SECTION 7"))\
    .withColumn("Actual_Estimated",lit("Estimated"))\
    .withColumn("Avg_6YR_Rate",lit(None))\
    .withColumn("Avg_10YR_Rate",lit(None))\
    .withColumn("TODAYS_FY",when(month(current_date())>9,year(current_date())+1).otherwise(year(current_date())))\
    .withColumn("TODAY_FY_END",concat(col("TODAYS_FY"),lit("-09-30")).cast(DateType()))\
    .withColumn("CUR_FY_RMG_DAYS",datediff(col("TODAY_FY_END"),current_date()))\
    .withColumn("Base_Total",when(col("Fiscal_Year") == col("TODAYS_FY"), round((col("Section_7_Volume")/col("CUR_FY_RMG_DAYS")),2)).otherwise(round((col("Section_7_Volume")/365),2)))

df298 = df297.selectExpr("Fiscal_Year","TODAYS_FY","Actual_Estimated","Base_Total","Avg_6YR_Rate","TODAY_FY_END","PostRegCat","Fiscal_Date as Date","Avg_10YR_Rate")

df324 = df298.unionByName(df323, allowMissingColumns=True).drop("TODAYS_FY","TODAY_FY_END")
df342 = df324.select("Fiscal_Year","Date","PostRegCat","Base_Total","Avg_6YR_Rate","Avg_10YR_Rate","Actual_Estimated")
#df342.count()
#13,126???
##45,996

# COMMAND ----------

df464 = df725.select("serial_number","registration_dt","expiration_dt").distinct()

#transpose
df467 = df464.melt(
    ids=["serial_number"], values=["registration_dt", "expiration_dt"],
    variableColumnName="LiveRegH_Name", valueColumnName="LiveRegH_Value"
)
#df467.display()

df466 = df467.withColumn("LiveRegH_DT",expr("case when LiveRegH_Name ='registration_dt' then LiveRegH_Value\
        when LiveRegH_Name ='expiration_dt' then date_add(LiveRegH_Value,1)\
        else null end"))\
        .withColumn("LiveRegH_Count", expr("case when LiveRegH_Name = 'registration_dt' then 1\
            when  LiveRegH_Name = 'expiration_dt' then -1\
                else 0 end"))\
        .withColumn("LiveRegH_FY",when(month("LiveRegH_DT")>9,year("LiveRegH_DT")+1).otherwise(year("LiveRegH_DT")))
#df645.display()

df470 = df466.where(col("LiveRegH_DT").isNotNull()).orderBy("LiveRegH_DT",desc("LiveRegH_Count"),"serial_number")

import sys
#running total
df469 = df470.withColumn('RunTot_LiveRegH_Count', f.sum(df470.LiveRegH_Count).over(Window.partitionBy().orderBy("LiveRegH_DT",desc("LiveRegH_Count"),"LiveRegH_FY","serial_number").rowsBetween(-sys.maxsize, 0)))
#df469.display()
spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df469_runtot")
df469 = df469.checkpoint(True)

df472 = df469.groupBy("LiveRegH_FY").agg(max("LiveRegH_DT").alias("Max_LiveRegH_DT"), max("RunTot_LiveRegH_Count").alias("Max_RunTot_LiveRegH_Count")).selectExpr("LiveRegH_FY as FY","Max_LiveRegH_DT","Max_RunTot_LiveRegH_Count as Live_Registrations")
#df472.display()
#df472.count()
#150

# COMMAND ----------

df428 = df733.where(col("postreg_category")=="SEPARATE 15")
df429 = df428.withColumn("SEC15_Disposed_Count",when((col("END_CM_DESC").isNotNull()) & (col("END_CM_DESC") != "CANCELLED"),lit(1)).otherwise(lit(0)))\
    .withColumn("End_Action_FY",when(month("END_ACTION_DATE")>9,year("END_ACTION_DATE")+1).otherwise(year("END_ACTION_DATE")))\
    .withColumn("Start_Action_FY",when(month("START_ACTION_DATE")>9,year("START_ACTION_DATE")+1).otherwise(year("START_ACTION_DATE")))\
    .withColumn("Registration_FY",when(month("REGISTRATION_DT")>9,year("REGISTRATION_DT")+1).otherwise(year("REGISTRATION_DT")))
df431 = df429.where(col("START_ACTION_DATE").isNotNull())

df430 = df431.groupBy("Start_Action_FY").agg(count("SERIAL_NUMBER").alias("Section_15_Volume"))
df478 = df430.where(col("Start_Action_FY")>1989).withColumn("Count",lit(1))

df431_right = df431.withColumnRenamed("Start_Action_FY","Right_Start_Action_FY")

df475 = df478.alias("df478").join(df431_right.alias("df431_right"), df478.Start_Action_FY == df431_right.Right_Start_Action_FY, "inner")
#df475.display()
#df475.count()
#24481

# COMMAND ----------

df433 = df430.alias("df430").join(df472.alias("df472"), df430.Start_Action_FY == df472.FY,"inner")
df489 = df433.withColumn("Fiscal_Year",col("Start_Action_FY").cast(IntegerType())).orderBy(desc("Fiscal_Year"))
df488 = spark.createDataFrame(df489.tail(df489.count()-1), df489.schema)
df491 = df488.orderBy("Fiscal_Year")
df492 = df491.withColumn("Fiscal_Year",col("Start_Action_FY").cast(IntegerType()))\
    .withColumn("FY_MINUS_1",col("Fiscal_Year")-1)\
    .withColumn("FY_START_DT",concat(col("FY_MINUS_1"),lit("-10-01")).cast(DateType()))\
    .withColumn("FY_END_DT",concat(col("Start_Action_FY"),lit("-09-30")).cast(DateType()))\
    .withColumn("Sec15_Rate_Per_Regs",round((col("Section_15_Volume")/col("Live_Registrations")),6).cast(DecimalType(precision=19, scale=6)))\
    .withColumn("Sec15_Rate_Per_Regs",when((current_date()>= col("FY_START_DT")) & (current_date()<= col("FY_END_DT")), lit(None)).otherwise(col("Sec15_Rate_Per_Regs")))
#df492.display()
#df492.count()
#49

# COMMAND ----------

df446 = df492.withColumn("Reg_Growth",expr("round((((Live_Registrations - (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))/ (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))*100),6) ").cast(DecimalType(precision=19, scale=6)))
#df178.display()

df446_notnull = df446.where(col('Reg_Growth').isNotNull())
if df446_notnull.count()>0:
    default_lag_value = df446.where(col('Reg_Growth').isNotNull()).agg(first("Reg_Growth").alias("default_Reg_Growth")).collect()[0][0]
else:
    default_lag_value = 0
#print(default_lag_value)

df447 = df446.withColumn("5_YR_AVG_Growth_Rate",expr(f""" round((((lag(Reg_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ Reg_Growth )/5),6) """).cast(DecimalType(precision=19, scale=6)))

df447_notnull = df447.where(col('Sec15_Rate_Per_Regs').isNotNull())
if df447_notnull.count()>0:
    default_lag_value = df447.where(col('Sec15_Rate_Per_Regs').isNotNull()).agg(first("Sec15_Rate_Per_Regs").alias("default_Sec15_Rate_Per_Regs")).collect()[0][0]
else:
    default_lag_value = 0
#print(default_lag_value)

df434 = df447.withColumn("5YR_Rate_Avg",expr(f""" round((((lag(Sec15_Rate_Per_Regs,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec15_Rate_Per_Regs,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec15_Rate_Per_Regs,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec15_Rate_Per_Regs,1,{default_lag_value})over(order By Fiscal_Year))+ Sec15_Rate_Per_Regs )/5),6) """).cast(DecimalType(precision=19, scale=6)))

#df283 = df279.orderBy(desc("Fiscal_Year"))
df435 = df434.withColumn("Predicted_5YR_AVG", col("5YR_Rate_Avg")*col("Live_Registrations"))\
    .withColumn("Scetion_15_Volume",round(when((current_date()>= col("FY_START_DT")) & (current_date() <= col("FY_END_DT")),col("Predicted_5YR_AVG")).otherwise(col("Section_15_Volume"))))\
    .withColumn("Sec15_Rate_Per_Regs",round((col("Section_15_Volume")/col("Live_Registrations")),6).cast(DecimalType(precision=19, scale=6)))
    
df437 = df435.select("Start_Action_FY","Predicted_5YR_AVG","5YR_Rate_Avg","Reg_Growth","Sec15_Rate_Per_Regs","FY_END_DT","FY_START_DT","FY_MINUS_1","Fiscal_Year","Live_Registrations","Max_LiveRegH_DT","Section_15_Volume","5_YR_AVG_Growth_Rate")

df436= df437.withColumn("5YR_Delta_PCT",abs(col("Section_15_Volume") - col("Predicted_5YR_AVG"))/((col("Section_15_Volume")+col("Predicted_5YR_AVG"))/2)*100)

w2 = Window.partitionBy().orderBy(desc(col("Fiscal_Year")))
df440 = df436.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row").select("Fiscal_Year")


df441 = df440.withColumn("FY_CURRENT",col("FISCAL_YEAR")+1).withColumn("FY_PLUS1",col("Fiscal_Year")+2).withColumn("FY_PLUS2",col("Fiscal_Year")+3).withColumn("FY_PLUS3",col("Fiscal_Year")+4).withColumn("FY_PLUS4",col("Fiscal_Year")+5).withColumn("FY_PLUS5",col("Fiscal_Year")+6).withColumn("FY_PLUS6",col("Fiscal_Year")+7).withColumn("FY_PLUS7",col("Fiscal_Year")+8).withColumn("FY_PLUS8",col("Fiscal_Year")+9).withColumn("FY_PLUS9",col("Fiscal_Year")+10).withColumn("FY_PLUS10",col("Fiscal_Year")+11)
#df170.display()

df442 = df441.melt(
    ids=["Fiscal_Year"], values=["FY_CURRENT","FY_PLUS1", "FY_PLUS2","FY_PLUS3", "FY_PLUS4","FY_PLUS5","FY_PLUS6", "FY_PLUS7","FY_PLUS8", "FY_PLUS9","FY_PLUS10"],
    variableColumnName="Name", valueColumnName="Value"
).drop("Fiscal_Year","Name")
#df169.display()
df443 = df442.withColumnRenamed("Value","FISCAL_YEAR")
df494 = df443.withColumn("FY_START_DT",concat((col("FISCAL_YEAR")-1),lit("-10-01")).cast(DateType()))\
    .withColumn("FY_END_DT",concat(col("FISCAL_YEAR"),lit("-09-30")).cast(DateType()))


df444 = df494.unionByName(df436,allowMissingColumns=True)
df445 = df444.select("FY_START_DT","Max_LiveRegH_DT","FY_END_DT","Predicted_5YR_AVG","Reg_Growth","5_YR_AVG_Growth_Rate","Sec15_Rate_Per_Regs","5YR_Delta_PCT","5YR_Rate_Avg","Live_Registrations","Fiscal_Year","Section_15_Volume","FY_MINUS_1")
#df445.count()
#60

# COMMAND ----------

# MAGIC %md
# MAGIC ###PREDICT ONE YEAR AT A TIME FOR FUTURE FYs USING ROLLING 5YR AVGS

# COMMAND ----------

# DBTITLE 1,Define Common func
def estimate_sec15_next10(df_input):
    df510 = df_input.withColumn("Live_Registrations",expr("case when Live_Registrations is not null then Live_Registrations else (lag(Live_Registrations,1) over(order By Fiscal_Year) + ((lag(Live_Registrations,1) over(order By Fiscal_Year) * lag(5_YR_AVG_Growth_Rate,1) over(order By Fiscal_Year)) /100)) end"))


    df509 = df510.withColumn("Reg_Growth",expr("case when Reg_Growth is not null then Reg_Growth else round(((Live_Registrations - (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))/ (lag(Live_Registrations,1,Live_Registrations) over(order By Fiscal_Year)))*100,6) end").cast(DecimalType(precision=19, scale=6)))


    df509_notnull = df509.where(col('Reg_Growth').isNotNull())
    if df509_notnull.count()>0:
        default_lag_value = df509.where(col('Reg_Growth').isNotNull()).agg(first("Reg_Growth").alias("default_Reg_Growth")).collect()[0][0]
    else:
        default_lag_value = 0
    #print(default_lag_value)

    df508 = df509.withColumn("5_YR_AVG_Growth_Rate",expr(f"""case when 5_YR_AVG_Growth_Rate is not null then 5_YR_AVG_Growth_Rate else round((((lag(Reg_Growth,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Reg_Growth,1,{default_lag_value})over(order By Fiscal_Year))+ Reg_Growth )/5),6)  end""").cast(DecimalType(precision=19, scale=6)))

    df507 = df508.withColumn("Section_15_Volume",expr("case when Section_15_Volume is not null then Section_15_Volume else (lag(5YR_Rate_Avg,1,5YR_Rate_Avg) over(order By Fiscal_Year))*Live_Registrations end")) 

    df506 = df507.withColumn("5_YR_AVG_Growth_Rate",when(col("Live_Registrations").isNull(),lit(None)).otherwise(col("5_YR_AVG_Growth_Rate")))\
        .withColumn("Sec15_Rate_Per_Regs", when(col("Sec15_Rate_Per_Regs").isNull(),round((col("Section_15_Volume")/col("Live_Registrations")),6)).otherwise(col("Sec15_Rate_Per_Regs")).cast(DecimalType(precision=19, scale=6)))


    df506_notnull = df506.where(col('Sec15_Rate_Per_Regs').isNotNull())
    if df506_notnull.count()>0:
        default_lag_value = df506.where(col('Sec15_Rate_Per_Regs').isNotNull()).agg(first("Sec15_Rate_Per_Regs").alias("default_Reg_Growth")).collect()[0][0]
    else:
        default_lag_value = 0
    #print(default_lag_value)
    df505 = df506.withColumn("5YR_Rate_Avg",expr(f"""case when 5YR_Rate_Avg is not null then 5YR_Rate_Avg else round((((lag(Sec15_Rate_Per_Regs,4,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec15_Rate_Per_Regs,3,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec15_Rate_Per_Regs,2,{default_lag_value})over(order By Fiscal_Year))+(lag(Sec15_Rate_Per_Regs,1,{default_lag_value})over(order By Fiscal_Year))+ Sec15_Rate_Per_Regs )/5) ,6) end""").cast(DecimalType(precision=19, scale=6)))

    df_output = df505.withColumn("5YR_Rate_Avg",when(col("Live_Registrations").isNull(), lit(None)).otherwise(col("5YR_Rate_Avg")))
    return(df_output)

# COMMAND ----------

spark.sparkContext.setCheckpointDir(CHK_POINT_DIR+"_df_output")
#1
df_output1 = estimate_sec15_next10(df445)
df_output1 = df_output1.checkpoint(True)
#2
df_output2=estimate_sec15_next10(df_output1)
df_output2 = df_output2.checkpoint(True)
#3
df_output3=estimate_sec15_next10(df_output2)
df_output3 = df_output3.checkpoint(True)
#4
df_output4=estimate_sec15_next10(df_output3)
df_output4 = df_output4.checkpoint(True)
#5
df_output5=estimate_sec15_next10(df_output4)
df_output5 = df_output5.checkpoint(True)
#6
df_output6=estimate_sec15_next10(df_output5)
df_output6 = df_output6.checkpoint(True)
#7
df_output7=estimate_sec15_next10(df_output6)
df_output7 = df_output7.checkpoint(True)
#8
df_output8=estimate_sec15_next10(df_output7)
df_output8 = df_output8.checkpoint(True)
#9
df_output9=estimate_sec15_next10(df_output8)
df_output9 = df_output9.checkpoint(True)
#10
df_output10=estimate_sec15_next10(df_output9)
df_output10 = df_output10.checkpoint(True)
#11
df_output11=estimate_sec15_next10(df_output10)
df_output11 = df_output11.checkpoint(True)
#df_output11.count()
#60

# COMMAND ----------

from pyspark.sql.functions import col, date_add, lit, current_date, month, year, when, concat
from pyspark.sql.types import DateType

df448 = df_output11.select(
    "Section_15_Volume", "FY_END_DT", "FY_START_DT", "FY_MINUS_1", "Max_LiveRegH_DT",
    "Fiscal_Year", "5YR_Rate_Avg", "5_YR_AVG_Growth_Rate", "Reg_Growth",
    "Live_Registrations", "Sec15_Rate_Per_Regs"
)

df456 = df448.withColumn(
    "Fiscal_Start", concat(col("Fiscal_Year"), lit("-01-01")).cast(DateType())
).withColumn(
    "Fiscal_End", concat(col("Fiscal_Year"), lit("-12-31")).cast(DateType())
)

df457 = df456.select(
    "Section_15_Volume", "Fiscal_Year", "FY_START_DT", "FY_END_DT", "Sec15_Rate_Per_Regs"
)

df458_min = df457.agg({"FY_START_DT": "min"}).collect()[0][0]
df458_max = df457.agg({"FY_END_DT": "max"}).collect()[0][0]

df460 = df457.withColumn("Max_FY_END_DT", lit(df458_max)).withColumn("Min_FY_START_DT", lit(df458_min))

# Get start_date as a Python date object
start_date_row = (
    df457.select(date_add(current_date(), 1).alias("start_date"))
    .limit(1)
    .collect()
)
start_date = start_date_row[0]["start_date"]

end_date = df458_max

# Generate a DataFrame with all dates in the possible range
num_days = (end_date - start_date).days + 1
date_range = (
    spark.range(0, num_days)
    .withColumn("Fiscal_Date", date_add(lit(start_date), col("id").cast(IntegerType())))
    .drop("id")
)

df459 = (
    df460.crossJoin(date_range)
    .filter(
        (col("Fiscal_Date") >= lit(start_date)) &
        (col("Fiscal_Date") <= col("Max_FY_END_DT"))
    )
)

df462 = df459.withColumn(
    "Fiscal_Year",
    when(month("Fiscal_Date") > 9, year("Fiscal_Date") + 1).otherwise(year("Fiscal_Date"))
)

# COMMAND ----------


df487 = df448.where((current_date()>=col("FY_START_DT")) & (current_date()<=col("FY_END_DT")))
df487_f = df448.where(~((current_date()>=col("FY_START_DT")) & (current_date()<=col("FY_END_DT"))))#.withColumnRenamed("Section_15_Volume","Estimated_Volume")


df483 = df475.groupBy("Start_Action_FY").agg(sum("Count").alias("Actual_Volume"))
df483 = df483.withColumnRenamed("Start_Action_FY","Right_Fiscal_year")

df477 = df475.groupBy("START_ACTION_DATE").agg(sum("Count").alias("Base_Total"))
df479 = df477.withColumn("Actual_Estimated",lit("Actual"))\
    .withColumn("PostRegCat",lit("SEPARATE 15"))\
    .withColumn("Fiscal_Year",when(month("START_ACTION_DATE")>9,year("START_ACTION_DATE")+1).otherwise(year("START_ACTION_DATE")))

df480 = df479.selectExpr("START_ACTION_DATE as Date","PostRegCat","Base_Total","Actual_Estimated","Fiscal_Year")
#df484 = df487.join("")

df484 = df487.alias("df487").join(df483.alias("df483"),df487.Fiscal_Year == df483.Right_Fiscal_year,"inner").withColumnRenamed("Section_15_Volume","Estimated_Volume")

df485 = df484.withColumn("Section_15_Volume", col("Estimated_Volume")-col("Actual_Volume")).drop("Right_Fiscal_year","FY_MINUS_1","Max_LiveRegH_DT")

df450 = df487_f.withColumn("Fiscal_year_C",col("Fiscal_Year")).drop("Fiscal_year_C","FY_MINUS_1","Max_LiveRegH_DT")

df495 = df450.unionByName(df485,allowMissingColumns=True)
df496 = df495.orderBy("Fiscal_Year").withColumnRenamed("Fiscal_Year","Right_Fiscal_Year")

df452 = df462.alias("df462").join(df496.alias("df496"),df462.Fiscal_Year == df496.Right_Fiscal_Year,"inner").selectExpr("df496.Section_15_Volume","Fiscal_Date","Fiscal_Year").distinct()
df455 = df452.where(col("Fiscal_Date")>current_date())

df453 = df455.withColumn("PostRegCat",lit("SEPARATE 15"))\
    .withColumn("Actual_Estimated",lit("Estimated"))\
    .withColumn("Avg_6YR_Rate",lit(None))\
    .withColumn("Avg_10YR_Rate",lit(None))\
    .withColumn("TODAYS_FY",when(month(current_date())>9,year(current_date())+1).otherwise(year(current_date())))\
    .withColumn("TODAY_FY_END",concat(col("TODAYS_FY"),lit("-09-30")).cast(DateType()))\
    .withColumn("CUR_FY_RMG_DAYS",datediff(col("TODAY_FY_END"),current_date()))\
    .withColumn("Base_Total",when(col("Fiscal_Year") == col("TODAYS_FY"), round((col("Section_15_Volume")/col("CUR_FY_RMG_DAYS")),2)).otherwise(round((col("Section_15_Volume")/365),2)).cast(DecimalType(precision=19, scale=2)))

df454 = df453.selectExpr("Fiscal_Year","TODAYS_FY","Actual_Estimated","Base_Total","Avg_6YR_Rate","TODAY_FY_END","PostRegCat","Fiscal_Date as Date","Avg_10YR_Rate","Section_15_Volume")

df481 = df454.unionByName(df480,allowMissingColumns=True)
df499 = df481.select("Fiscal_Year","Date","PostRegCat","Base_Total","Avg_6YR_Rate","Avg_10YR_Rate","Actual_Estimated")
#df499.count()
#10,599

# COMMAND ----------

# MAGIC %md
# MAGIC ##HISTORY and FUTURE 10 YEAR ESTIMATES: COMBINE ALL INDIVIDUALS SECTIONS, SORT AND OUTPUT

# COMMAND ----------

df268 = df499.unionByName(df342).unionByName(df264).unionByName(df100)
df269 = df268.orderBy("Date").select("*").distinct()
#df269.display()
#df269.count()
#51,960???

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validation and Output

# COMMAND ----------

df4 = df649.withColumn("record_output_date",current_date())
df3 = df4.groupBy("record_output_date").agg(count("serial_number").alias("output_record_count"))

df5 = df3.unionByName(df735,allowMissingColumns=True)
#df5.display()
w = Window.orderBy(desc("record_output_date"))
df12 = df5.withColumn("record_output_percent_change",round(((col('output_record_count') - lead(col('output_record_count'),1).over(w))/lead(col('output_record_count'),1).over(w)),6).cast(DecimalType(precision=19, scale=6)))
#df12.display()

df7 = df12.withColumn("continue_process",when((col('output_record_count') >= lead(col('output_record_count'),1,0).over(w)) & (col('record_output_percent_change')<0.05), lit(1)).otherwise(lit(0)))
#df7.display()
df8 = df7.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")
#df8.display()
df9 = df8.select("continue_process").collect()[0][0]
#implement Stop process?
#####################1####################################################
df14 = df649.withColumn("continue_process",lit(f"{df9}"))
df705 = df14.selectExpr("serial_number" ,"registration_dt" ,"six_yr_dt" ,"last_10yr_dt" ,"next_10yr_renewal" ,"number_renewals" ,"next_6yr_dt" ,"expiration_dt" ,"expiration_type" ,"registration_number" ,"am_dt_cncl" ,"live_registration" ,"expiration_dt_realtime" ,"expiration_type_realtime" ,"live_reg" ,"exp_fy" ,"exp_fy_rt" ,"reg_fy" ,"today" ,"today_fy" ,"fy_exp_diff" ,"fy_reg_diff" ,"six_yr_fy" ,"ten_yr_fy" ,"include_6yr_avg" ,"include_10yr_avg" ,"max_today_fy" ,"reg_age" ,"average_life_include" ,"sixyr_num" ,"sixyr_denom" ,"tenyr_num" ,"tenyr_denom" ,"twentyyr_num" ,"twentyyr_denom" ,"thirtyyr_num" ,"thirtyyr_denom" ,"fortyyr_num" ,"fortyyr_denom" ,"fiftyyr_num" ,"fiftyyr_denom" ,"milestone" ,"pendency_cal_start_dt" ,"non_pro_se" ,"pctram_link" ,"law_office" ,"filing_basis_grp" ,"filing_method_cur" ,"am_stat" ,"owner_name" ,"city" ,"state" ,"country_or_area_name" ,"reg_class_count" ,"active_class_count" ,"group_type" ,"concat_class" ,"mark_nm_short" ,"max_dt_filter" ,"current_timestamp() as create_ts" ,"'-1' as create_user_id" ,"current_timestamp() as update_ts" ,"'-1' as update_user_id")
#print(f'{df705.count()=}')
######################2######################################################
df13 = df8.unionByName(df735, allowMissingColumns=True)
df707 = df13.selectExpr("record_output_date","output_record_count","record_output_percent_change","continue_process","current_timestamp() as create_ts" ,"'-1' as create_user_id" ,"current_timestamp() as update_ts" ,"'-1' as update_user_id")
#print(f'{df707.count()=}')
#####################3#######################################################
df19 = df648.withColumn("record_output_date", current_date())
df18 = df19.groupBy("record_output_date").agg(count("serial_number").alias("output_record_count"))

df20 = df18.unionByName(df737, allowMissingColumns=True)
w = Window.orderBy(desc("record_output_date"))
df27 = df20.withColumn("record_output_percent_change",round(((col('output_record_count') - lead(col('output_record_count'),1).over(w))/lead(col('output_record_count'),1).over(w)),6).cast(DecimalType(precision=19, scale=6)))

df22 = df27.withColumn("continue_process",expr(" case when output_record_count >= lead(output_record_count,1,0) over(order By record_output_date desc) and record_output_percent_change <0.05 then 1 when (output_record_count - lead(output_record_count,1) over(order By record_output_date desc))  >-500 then 1 else 0  end"))
df23 = df22.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")
df36 = df23.agg(first(col("continue_process")).alias("detail_continue_process")).collect()[0][0]
df35 = df8.agg(first(col("continue_process")).alias("milestone_continue_process"))
df40 = df35.withColumn("detail_continue_process",lit(f"{df36}"))
df37 = df40.withColumn("continue_process",when((col("milestone_continue_process")==1) & (col("detail_continue_process")==1),1).otherwise(0)).collect()[0][0]
df38 = df642.withColumn("continue_process",lit(f"{df37}"))

df740 = df38.selectExpr("SERIAL_NUMBER","MARK_NM_SHORT","Concat_Class","Group_Type","Active_Class_Count","Reg_Class_Count","Country_or_Area_Name","State","CITY","Owner_Name","Continue_Process","AM_STAT","FILING_BASIS_GRP","LAW_OFFICE","PCTRAM_LINK","NON_PRO_SE","Pendency_Cal_Start_DT","SER_NUM","Max_Dt_Filter","LiveRegH_Count","LiveRegH_DT","LiveRegH_Value","LiveRegH_Name","FILING_METHOD_CUR","current_timestamp() as create_ts" ,"'-1' as create_user_id" ,"current_timestamp() as update_ts" ,"'-1' as update_user_id")
#print(f'{df740.count()=}')
############################4############################################################

df39 = df269.withColumn("continue_process",lit(f"{df37}"))
df742= df39.selectExpr("Fiscal_Year","Date","PostRegCat","Base_Total","Avg_6YR_Rate","Avg_10YR_Rate","Actual_Estimated","Continue_Process","current_timestamp() as create_ts" ,"'-1' as create_user_id" ,"current_timestamp() as update_ts" ,"'-1' as update_user_id")
#print(f'{df742.count()=}')
#########################5##################################################################
df23_cp = df23.select("continue_process").collect()[0][0]
df29 = df648.withColumn("continue_process",lit(f"{df23_cp}"))
df709 = df29.selectExpr("recordid","serial_number","registration_dt","registration_number","postreg_category","start_action_number","end_action_number","start_action_date","end_action_date","start_5_characters","end_5_characters","start_cm_desc","end_cm_desc","fifteen_flag","inventory","first_action_date","first_action_code","renewal_dt","renewal_number","first_action_pendency","total_pendency","max_max_dt","expiration_type_realtime2","expiration_dt_realtime2","max_fy_ph","sixyr_disposed_count","sixyr_base","tenyr_disposed_count","tenyr_base","end_action_fy","ser_num","pendency_cal_start_dt","non_pro_se","pctram_link","law_office","filing_basis_grp","filing_method_cur","am_stat","owner_name","city","state","country_or_area_name","reg_class_count","active_class_count","group_type","fa_percentile","right_recordid","fa_percentile_include","tp_percentile","tp_percentile_include","top10_fy_exclude_cfy","top5_fy_exclude_cfy","renewal_number_grp","category","concat_class","first_action_inventory","reg_fy","drop_off_year","current_timestamp() as create_ts" ,"'-1' as create_user_id" ,"current_timestamp() as update_ts" ,"'-1' as update_user_id")
#print(f'{df709.count()=}')
##########################6###############################################################
df28 = df737.unionByName(df23,allowMissingColumns=True)
df711 = df28.selectExpr("record_output_date","output_record_count","record_output_percent_change","continue_process","current_timestamp() as create_ts" ,"'-1' as create_user_id" ,"current_timestamp() as update_ts" ,"'-1' as update_user_id")
#print(f'{df711.count()=}')
#1: 6,438,539
#2: 57
#3: 12,877,078
#4: 51,960???51958
#5: 9,915,475
#6: 55?? 56


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Load

# COMMAND ----------

try:
    df705.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.gold.post_reg_dashboard')
    df707.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.silver.pr_milestone_counts')
    df740.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.gold.post_reg_dashboard_running')#register table in alteryx etldb
    df742.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.gold.post_reg_workforce')#register table in alteryx etldb
    df709.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.gold.post_reg_detail_dashboard')
    df711.write.mode("overwrite").format("delta").insertInto(f'{trgt_catalog}.silver.pr_detail_counts')
    
    recs_count = df705.count()
    end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.fs.rm(CHK_POINT_DIR,True)
    dbutils.notebook.exit(f"Completed Loading post_reg_dashboard Tables ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    dbutils.fs.rm(CHK_POINT_DIR,True)
    raise
dbutils.notebook.exit(f"Completed loading third level post_reg_dashboard Tables ")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unit Test cells below

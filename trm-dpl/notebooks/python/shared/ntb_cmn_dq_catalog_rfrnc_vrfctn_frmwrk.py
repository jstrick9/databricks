# Databricks notebook source
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("PROC_CTGRY_CD", "SRC_TO_BRNZ", "PROC_CTGRY_CD")
dbutils.widgets.text("dbx_env","dev")
##TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
#JBTEASPS,--ORA-28000: The account is locked.
# DATABRIDGE,--unable to connect
#TMNGIDMP--dev catalog missing

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME")
PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")


src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"


config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/jayanth.bandi@uspto.gov/bdr-ng-trm-dpl_optmz/notebooks/config/dev/tmngpdb-conf.yaml"
if SRC_SYS_NAME  == 'PROCEEDING':
    SRC_SYS_NAME = "TMPROCEEDING"
    
print(f'{SRC_SYS_NAME=},{PROC_CTGRY_CD=},{config_file_name=},{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

spark.sql("set SRC_SYS_NAME = " + dbutils.widgets.get("SRC_SYS_NAME"))
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))
job_name = 'ntb_'+SRC_SYS_NAME.lower()+'_dq_catalog_vrfctn_frmwrk'
spark.sql("set job_name = "+str(job_name))
print(f'{job_name=}')
job_start_ts = datetime.datetime.now()

# COMMAND ----------

common_configs = read_yaml(config_file)
src_db = common_configs['schema']['src_db_name']
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
receiver_email = common_configs['data_quality']['receiver_email']
if SRC_SYS_NAME == 'DATABRIDGE' :
    trm_scope =  common_configs['secrets']['mysql_scope']   
else:
    trm_scope = common_configs['secrets']['trm_scope']

database = 'bronze'


proc_id = spark.sql(f"""select proc_id from {data_quality_catalog}.silver.CMN_PROC_DEFN_RFRNC where proc_name='{job_name}'""").collect()[0][0]
spark.conf.set('config.proc_id', proc_id)

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.src_db', src_db.lower()) 
spark.conf.set('config.database', database)
spark.conf.set('config.job_name', job_name)
spark.conf.set('config.src_sys_name', SRC_SYS_NAME)
spark.conf.set('config.trm_scope', trm_scope.lower()) 

emailid = receiver_email
env = dbx_env.upper()

if PROC_CTGRY_CD =='SRC_TO_BRNZ':
    trgt_db_name = "BRONZE"

print(f'{src_db=},{trgt_catalog=}, {data_quality_catalog=} ,{receiver_email=},{trm_scope=},{trgt_db_name=}')

# COMMAND ----------

df_metadata = spark.sql(f"""select SRC_SYS_NAME,CNCTN_DTL_DESC,SRC_TBL_NAME, TRGT_TBL_NAME, IN_DBX_IND, OBJECT_TYPE,'' as ERROR_TYPE_CD, '' as ERROR_MESSAGE from {data_quality_catalog}.silver.cmn_catalog_rfrnc
where SRC_SYS_NAME = '{SRC_SYS_NAME}'
and TARGET_DB_NAME = '{trgt_db_name}'""")

SRC_SCHEMA = src_db.upper()
TRGT_SCHEMA = trgt_catalog+'.bronze'
print(f'{SRC_SCHEMA=},{TRGT_SCHEMA=}')

# COMMAND ----------

# DBTITLE 1,Catalog Verification
if (PROC_CTGRY_CD == 'SRC_TO_BRNZ'):# set condition for oracle vs mysql db call
    try:
        if SRC_SYS_NAME == 'JBTEASPS':
                    pushdown_query = f"""(SELECT table_name AS SRC_TBL_NAME FROM information_schema.tables WHERE table_schema = '{SRC_SCHEMA}')"""
                    df_read_src_catalog = read_data_from_postgres_conn(pushdown_query,trm_scope)
        else:
                    pushdown_query = f"""(SELECT table_name as SRC_TBL_NAME from all_tables where owner = '{SRC_SCHEMA}')"""
                    df_read_src_catalog = read_data_from_oracle_conn_dsu_cmn(pushdown_query, trm_scope)
        #df_read_src_catalog.display()
        src_table_count = df_read_src_catalog.count()
        spark.conf.set('config.src_table_count', src_table_count)
        df_read_catalog_rfrnc = spark.sql(f"""select SRC_TBL_NAME from {data_quality_catalog}.silver.cmn_catalog_rfrnc where SRC_SYS_NAME = '{SRC_SYS_NAME}' and TARGET_DB_NAME = '{trgt_db_name}'and trgt_tbl_name is not null""")
        #df_read_catalog_rfrnc.display()
        rfrnc_src_table_count = df_read_catalog_rfrnc.count()
        spark.conf.set('config.rfrnc_src_table_count', rfrnc_src_table_count)
        if src_table_count == rfrnc_src_table_count and df_read_src_catalog.exceptAll(df_read_catalog_rfrnc).count()==0:
            data_quality_result =  f"Source and Refernce Catalog Count Matches for SRC_SYS_NAME {SRC_SYS_NAME}"
            #end job
            dbutils.notebook.exit(f"Completed data verification for  {trgt_catalog}.{PROC_CTGRY_CD} tables")
        else:
            print(f'{src_table_count=},{rfrnc_src_table_count=}')
            #data_quality_result =  f"Source and Refrence Catalog Count does not Match for SRC_SYS_NAME: {SRC_SYS_NAME}"

            #catalog verification Result
            df_src_variance = df_read_src_catalog.exceptAll(df_read_catalog_rfrnc)
            df_src_variance = df_src_variance.withColumn("ERROR_TYPE_CD_SRC",lit("WARNING"))#"New Tables created in source but missing in catalog refrence file"
            
            df_rfrnc_variance = df_read_catalog_rfrnc.exceptAll(df_read_src_catalog)
            df_rfrnc_variance = df_rfrnc_variance.withColumn("ERROR_TYPE_CD_TRGT",lit("ERROR"))#"Source Table names present in catalog refrence file but missing in source db"
        
            df_results = df_metadata.join(df_src_variance,"SRC_TBL_NAME","full_outer").withColumn("ERROR_TYPE_CD",expr("case when ERROR_TYPE_CD_SRC is not null then ERROR_TYPE_CD_SRC end")).drop("ERROR_TYPE_CD_SRC")
            df_results = df_results.join(df_rfrnc_variance,"SRC_TBL_NAME","left").withColumn("ERROR_TYPE_CD",expr("case when ERROR_TYPE_CD is null and ERROR_TYPE_CD_TRGT = 'ERROR' and IN_DBX_IND = 'Y' then 'ERROR' when ERROR_TYPE_CD is null and ERROR_TYPE_CD_TRGT ='ERROR' and IN_DBX_IND =  'N' then 'IGNORE' else ERROR_TYPE_CD end")).drop("ERROR_TYPE_CD_TRGT")
            
            df_results = df_results.withColumn("ERROR_MESSAGE",expr("case when ERROR_TYPE_CD= 'WARNING' then 'New Table found in source db'  when ERROR_TYPE_CD= 'ERROR' then 'Table missing in source db but exists in Databricks' when ERROR_TYPE_CD= 'IGNORE' then 'Table missing in source db and does not exist in Databricks' else null end"))
            #df_results.display()
            df_results.createOrReplaceTempView("temp_dq_counts")
            #data_quality_result
            df_error_message = spark.sql("""
            select error_type_cd||': '||table_counts||' '||error_message  as error_message
            from(select distinct error_type_cd, error_message, count(*) table_counts from temp_dq_counts where error_message is not null group by error_message,error_type_cd)""")

            from pyspark.sql.functions import collect_list
            data_quality_result = df_error_message.orderBy('error_message').groupby().agg(collect_list('error_message').alias("data_quality_result")).withColumn("data_quality_result", concat_ws(", ", "data_quality_result")).collect()[0][0]

            data_quality_result_detailed = df_results.filter("ERROR_MESSAGE is not null").orderBy('SRC_TBL_NAME').groupby('ERROR_MESSAGE','ERROR_TYPE_CD').agg(count(col('SRC_TBL_NAME')).alias("table_counts"),collect_list('SRC_TBL_NAME').alias("SRC_TBL_NAME_LIST")).withColumn("DQ_TABLE_LIST", concat_ws(", ", "SRC_TBL_NAME_LIST")).drop("SRC_TBL_NAME_LIST")

            data_quality_result_detailed = data_quality_result_detailed.withColumn("ERROR_MESSAGE",expr("ERROR_TYPE_CD||': '||table_counts||' '||ERROR_MESSAGE")).drop("ERROR_TYPE_CD","table_counts")
            #data_quality_result_detailed.display()

    except Exception as e:
                raise
                data_quality_result =  "Exception message: {}".format(e)
print(data_quality_result)
spark.conf.set('config.DQ_RSLT_MSG', data_quality_result)

# COMMAND ----------

# DBTITLE 1,Calculate Variance
df_dq_query_counts_load = spark.sql("""
select var_check.*
,case when abs(RPTD_VRNC_PCT) > ERR_THRSHLD_PCT then 'Y' else 'N' end as VRNC_IND 
from(
select distinct
  ${config.proc_id} AS PROC_ID,
  '${config.job_name}' AS PROC_NAME ,
  '${PROC_CTGRY_CD}' as PROC_CTGRY_CD,
  1 as QUERY_SET_ID,
  'CM' as QUERY_DQ_CD,
  '${config.src_db}' as SRC_QUERY_NAME,
  '${config.trgt_catalog}' as TRGT_QUERY_NAME,
  null as JOB_LOG_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as JOB_START_TS,
  ${config.src_table_count} as RPTD_SRC_RSLT_CNT,
  ${config.rfrnc_src_table_count} as RPTD_TRGT_RSLT_CNT,
  0 as ERR_THRSHLD_PCT,
  CASE
  when ${config.src_table_count} is null or ${config.rfrnc_src_table_count} is null then -100
    WHEN ${config.src_table_count} = 0
    AND ${config.rfrnc_src_table_count} != 0 THEN -100
    WHEN ${config.src_table_count} = 0 THEN 0.0000
    ELSE round(ABS(
      (
        (
          (
            FLOAT(${config.src_table_count}) - FLOAT(${config.rfrnc_src_table_count})
          ) * 100.0000
        ) / FLOAT(${config.src_table_count})
      )
    ),2)
  END AS RPTD_VRNC_PCT,
  '${config.DQ_RSLT_MSG}' AS DQ_RSLT_MSG,
  'ETL' AS AUDT_INSRT_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as AUDT_INSRT_TS,
  '${config.src_sys_name}' as SRC_SYS_NAME
from
  temp_dq_counts)var_check""")

df_dq_query_counts_load.display()
df_dq_query_counts_load.createOrReplaceTempView("temp_dq_src_trgt_counts_load")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   ${config.data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT (
# MAGIC     PROC_ID,
# MAGIC     PROC_NAME, 
# MAGIC     PROC_CTGRY_CD,
# MAGIC     QUERY_SET_ID,
# MAGIC     QUERY_DQ_CD,
# MAGIC     SRC_QUERY_NAME,
# MAGIC     TRGT_QUERY_NAME,
# MAGIC     JOB_LOG_ID,
# MAGIC     JOB_START_TS,
# MAGIC     RPTD_SRC_RSLT_CNT,
# MAGIC     RPTD_TRGT_RSLT_CNT,
# MAGIC     ERR_THRSHLD_PCT,
# MAGIC     RPTD_VRNC_PCT,
# MAGIC     DQ_RSLT_MSG,
# MAGIC     AUDT_INSRT_ID,
# MAGIC     AUDT_INSRT_TS,
# MAGIC     SRC_SYS_NAME
# MAGIC   )
# MAGIC select
# MAGIC   PROC_ID,
# MAGIC   PROC_NAME ,
# MAGIC   PROC_CTGRY_CD,
# MAGIC   QUERY_SET_ID,
# MAGIC   QUERY_DQ_CD,
# MAGIC   SRC_QUERY_NAME,
# MAGIC   TRGT_QUERY_NAME,
# MAGIC   JOB_LOG_ID,
# MAGIC   JOB_START_TS,
# MAGIC   RPTD_SRC_RSLT_CNT,
# MAGIC   RPTD_TRGT_RSLT_CNT,
# MAGIC   ERR_THRSHLD_PCT,
# MAGIC   RPTD_VRNC_PCT,
# MAGIC   DQ_RSLT_MSG,
# MAGIC   AUDT_INSRT_ID,
# MAGIC   AUDT_INSRT_TS,
# MAGIC   SRC_SYS_NAME
# MAGIC from
# MAGIC   temp_dq_src_trgt_counts_load
# MAGIC
# MAGIC   

# COMMAND ----------

df_dq_query_counts_variance = df_dq_query_counts_load.filter("VRNC_IND='Y' or DQ_RSLT_MSG is not null ").select("SRC_SYS_NAME","PROC_CTGRY_CD",col("SRC_QUERY_NAME").alias("SRC_DB_NAME"),col("TRGT_QUERY_NAME").alias("TRGT_DB_NAME"),col("RPTD_SRC_RSLT_CNT").alias("RPTD_SRC_TBL_CNT"),col("RPTD_TRGT_RSLT_CNT").alias("RPTD_TRGT_TBL_CNT"),"RPTD_VRNC_PCT",col("DQ_RSLT_MSG").alias("DQ_ERROR_MESSAGE"),"JOB_START_TS")

#df_dq_query_counts_variance.display()
from pyspark.sql.functions import col, explode, regexp_replace, split

df_dq_query_counts_variance_split = df_dq_query_counts_variance.withColumn(
    "DQ_ERROR_MESSAGE", 
    explode(split(col("DQ_ERROR_MESSAGE"),","))
)

df_dq_query_counts_variance_detail = df_dq_query_counts_variance_split.alias("df_error").join(data_quality_result_detailed.alias("df_error_detail"),trim(df_dq_query_counts_variance_split.DQ_ERROR_MESSAGE)==data_quality_result_detailed.ERROR_MESSAGE,"inner").drop("ERROR_MESSAGE")

df_dq_query_counts_variance_detail = df_dq_query_counts_variance_detail.selectExpr("SRC_SYS_NAME","PROC_CTGRY_CD","SRC_DB_NAME","TRGT_DB_NAME","RPTD_SRC_TBL_CNT","RPTD_TRGT_TBL_CNT","RPTD_VRNC_PCT","DQ_ERROR_MESSAGE","DQ_TABLE_LIST","JOB_START_TS").orderBy("DQ_ERROR_MESSAGE")
df_dq_query_counts_variance_detail.display()


if df_dq_query_counts_variance_detail.count() >0:
    Appdf=df_dq_query_counts_variance_detail
    parms = {}
    pd.set_option('display.max_colwidth', 0)
    parms['INDEXED']=Appdf.toPandas().to_html()
    notify = Notify()
    templ_str = f'{SRC_SYS_NAME}: {PROC_CTGRY_CD}  Catalog Quality Report'
    msg = notify.compose_email( templ_str, f'{SRC_SYS_NAME} {PROC_CTGRY_CD} Catalog Quality Report - '+env, emailid, parms )
    notify.send_mail(msg)
else:
    print(f"No email notification sent for data variance as all table counts match for {trgt_catalog} {PROC_CTGRY_CD}  tables")

# COMMAND ----------

dbutils.notebook.exit(f"Completed data verification for  {trgt_catalog}.{PROC_CTGRY_CD} tables")

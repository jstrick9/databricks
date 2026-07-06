# Databricks notebook source
# MAGIC %md
# MAGIC ##Create new proc name

# COMMAND ----------

dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("PROC_CTGRY_CD", "SRC_TO_BRNZ", "PROC_CTGRY_CD")
dbutils.widgets.text("dbx_env","dev")
##TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP
#JBTEASPS,--ORA-28000: The account is locked.
# DATABRIDGE,--unable to connect
#TMNGIDMP--dev catalog missing
#PROCEEDING

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME")
PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")


src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"


config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

if SRC_SYS_NAME  == 'PROCEEDING':
    SRC_SYS_NAME = "TMPROCEEDING"
#config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{SRC_SYS_NAME=},{PROC_CTGRY_CD=},{config_file_name=},{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.window import Window

# COMMAND ----------

spark.sql("set SRC_SYS_NAME = " + dbutils.widgets.get("SRC_SYS_NAME"))
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))
job_name = 'ntb_'+SRC_SYS_NAME.lower()+'_dq_catalog_ddl_vrfctn_frmwrk'
spark.sql("set job_name = "+str(job_name))
print(f'{job_name=}')
job_start_ts = datetime.datetime.now()

# COMMAND ----------

common_configs = read_yaml(config_file)
src_db = common_configs['schema']['src_db_name']
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
receiver_email = common_configs['data_quality']['receiver_email']
if SRC_SYS_NAME == 'DATABRIDGE'  :
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

df_metadata = spark.sql(f"""select SRC_SYS_NAME,CNCTN_DTL_DESC,SRC_TBL_NAME, TRGT_TBL_NAME, IN_DBX_IND, OBJECT_TYPE
                        from {data_quality_catalog}.silver.cmn_catalog_rfrnc
                        where SRC_SYS_NAME = '{SRC_SYS_NAME}'
                        and TARGET_DB_NAME = '{trgt_db_name}'
                        and SRC_TBL_NAME not in ('CDC_BATCH_JOB_CONTROL','CDC_BATCH_JOB_HISTORY')
                        """)

SRC_SCHEMA = src_db.upper()
TRGT_SCHEMA = trgt_catalog+'.bronze'
df_metadata.display()
print(f'{SRC_SCHEMA=},{TRGT_SCHEMA=}')

# COMMAND ----------

# DBTITLE 1,DDL Verification
df_metadata_ddl_vrfctn = df_metadata.filter("IN_DBX_IND = 'Y'")#AND TRGT_TBL_NAME in( 'ATTORNEY_HOLD','USE_IN_ANOTHER_FORM')

if (PROC_CTGRY_CD == 'SRC_TO_BRNZ'):
    ddl_verification_rows =[]

    for r in df_metadata_ddl_vrfctn.collect():
        dct = r.asDict()
        src_tbl_nm = dct["SRC_TBL_NAME"]
        trgt_tbl_nm = dct["TRGT_TBL_NAME"]
        try:
            print("\n")
            print(f"Performing ddl verification {src_tbl_nm}:")
            #source table
            if SRC_SYS_NAME == 'JBTEASPS':
                src_tbl_nm = src_tbl_nm.lower()
                SRC_SCHEMA = SRC_SCHEMA.lower()
                pushdown_query = (
                    f"(SELECT upper(column_name) as column_name "
                    f"FROM information_schema.columns "
                    f"WHERE lower(table_name) = '{src_tbl_nm}' "
                    f"AND lower(table_schema) = '{SRC_SCHEMA}' "
                    f"ORDER BY column_name)"
                    )
                df_read_src_tbl_ddl = read_data_from_postgres_conn(pushdown_query, trm_scope)
                display(df_read_src_tbl_ddl)
            else:
                pushdown_query = f"""(SELECT upper(all_tab.column_name) as column_name FROM all_tab_columns all_tab  WHERE all_tab.TABLE_NAME = '{src_tbl_nm}' AND owner='{SRC_SCHEMA}' order by all_tab.column_name) """#,  all_tab.data_type
                df_read_src_tbl_ddl = read_data_from_oracle_conn_dsu_cmn(pushdown_query, trm_scope)
            src_tbl_col_count = df_read_src_tbl_ddl.count()
            dct['RPTD_SRC_RSLT_CNT'] = src_tbl_col_count
            df_read_trgt_tbl_ddl = spark.sql(f"""show columns from {TRGT_SCHEMA}.{trgt_tbl_nm}""").select(upper("col_name").alias("col_name")).orderBy("col_name")#,"data_type"
            trgt_tbl_col_count = df_read_trgt_tbl_ddl.count()
            dct['RPTD_TRGT_RSLT_CNT'] = trgt_tbl_col_count

            if src_tbl_col_count == trgt_tbl_col_count and df_read_src_tbl_ddl.exceptAll(df_read_trgt_tbl_ddl).count() ==0 and df_read_trgt_tbl_ddl.exceptAll(df_read_src_tbl_ddl).count() ==0:
                print(f'{src_tbl_col_count=} and {trgt_tbl_col_count=}')
                ddl_quality_result =f'Number of Columns and Column names match between Source {SRC_SCHEMA}.{src_tbl_nm} and Target {TRGT_SCHEMA}.{trgt_tbl_nm}'
                #end job
                #dbutils.notebook.exit(f"Completed ddl verification for  {trgt_catalog}.{PROC_CTGRY_CD} tables")
            else:
                print(f"Number of Columns or Column names do not match between Source {SRC_SCHEMA}.{src_tbl_nm} and Target {TRGT_SCHEMA}.{trgt_tbl_nm}")
                df_ddl_mismatch = df_read_src_tbl_ddl.alias("df_src").join(df_read_trgt_tbl_ddl.alias("df_trgt"),df_read_src_tbl_ddl.COLUMN_NAME==df_read_trgt_tbl_ddl.col_name,"full_outer").withColumnRenamed("COLUMN_NAME","src_col_name").withColumnRenamed("col_name","trgt_col_name").withColumn("error_message",expr("case when src_col_name is not null and trgt_col_name is null then 'New Columns in source table' when src_col_name is  null and trgt_col_name is not null then 'Columns not found in source table'  else null end")).filter("error_message is not null")
                df_ddl_mismatch.createOrReplaceTempView("temp_dq_ddl_check")
                
                #ddl_quality_result
                df_error_message = spark.sql("""select error_message,nvl(src_col_name,trgt_col_name) as column_name  from temp_dq_ddl_check""")
                from pyspark.sql.functions import collect_list
                ddl_quality_result = df_error_message.orderBy('error_message').groupby('error_message').agg(collect_list('column_name').alias("column_name")).withColumn("error_columns", concat_ws(", ","column_name")).withColumn("DQ_RSLT_MSG", concat_ws(": ","error_message","error_columns")).select("DQ_RSLT_MSG")

                ddl_quality_result = ddl_quality_result.orderBy('DQ_RSLT_MSG').groupby().agg(collect_list('DQ_RSLT_MSG').alias("DQ_RSLT_MSG")).withColumn("DQ_RSLT_MSG", concat_ws(", ", "DQ_RSLT_MSG")).collect()[0][0]               
                

            dct['DQ_RSLT_MSG'] = ddl_quality_result
            ddl_verification_rows.append(dct)
        except Exception as e:
                #raise
                ddl_quality_result =  "Exception message: {}".format(e)
                print("Exception message: {}".format(e))

# COMMAND ----------

df_dq_query_counts = spark.createDataFrame(ddl_verification_rows)
df_dq_query_counts.createOrReplaceTempView("temp_dq_src_trgt_counts")

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
  SRC_TBL_NAME as SRC_QUERY_NAME,
  TRGT_TBL_NAME as TRGT_QUERY_NAME,
  null as JOB_LOG_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as JOB_START_TS,
  RPTD_SRC_RSLT_CNT as RPTD_SRC_RSLT_CNT,
  RPTD_TRGT_RSLT_CNT as RPTD_TRGT_RSLT_CNT,
  0 as ERR_THRSHLD_PCT,
  CASE
  when RPTD_SRC_RSLT_CNT is null or RPTD_TRGT_RSLT_CNT is null then -100
    WHEN RPTD_SRC_RSLT_CNT = 0
    AND RPTD_TRGT_RSLT_CNT != 0 THEN -100
    WHEN RPTD_SRC_RSLT_CNT = 0 THEN 0.0000
    ELSE ROUND(ABS(
      (
        (
          (
            FLOAT(RPTD_SRC_RSLT_CNT) - FLOAT(RPTD_TRGT_RSLT_CNT)
          ) * 100.0000
        ) / FLOAT(RPTD_SRC_RSLT_CNT)
      )
    ),2)
  END AS RPTD_VRNC_PCT,
  DQ_RSLT_MSG AS DQ_RSLT_MSG,
  'ETL' AS AUDT_INSRT_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as AUDT_INSRT_TS,
  '${config.src_sys_name}' as SRC_SYS_NAME
from
  temp_dq_src_trgt_counts)var_check""")

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

df_dq_query_counts_variance = df_dq_query_counts_load.filter("VRNC_IND='Y' or DQ_RSLT_MSG is not null").select("SRC_SYS_NAME","PROC_CTGRY_CD",col("SRC_QUERY_NAME").alias("SRC_TABLE_NAME"),col("TRGT_QUERY_NAME").alias("TRGT_TABLE_NAME"),col("RPTD_SRC_RSLT_CNT").alias("RPTD_SRC_TBL_COL_CNT"),col("RPTD_TRGT_RSLT_CNT").alias("RPTD_TRGT_TBL_COL_CNT"),"RPTD_VRNC_PCT",col("DQ_RSLT_MSG").alias("DQ_ERROR_MESSAGE"),"JOB_START_TS")

#df_dq_query_counts_variance.display()

if df_dq_query_counts_variance.count() >0:
    Appdf=df_dq_query_counts_variance
    parms = {}
    pd.set_option('display.max_colwidth', 0)
    parms['INDEXED']=Appdf.toPandas().to_html()
    notify = Notify()
    templ_str = f'{SRC_SYS_NAME}: {PROC_CTGRY_CD}  Catalog DDL Verification Quality Report'
    msg = notify.compose_email( templ_str, f'{SRC_SYS_NAME} {PROC_CTGRY_CD} Catalog DDL Verification Quality Report - '+env, emailid, parms )
    notify.send_mail(msg)
else:
    print(f"No email notification sent for data variance as all table ddls match for {trgt_catalog} {PROC_CTGRY_CD}  tables")

# COMMAND ----------

dbutils.notebook.exit(f"Completed data verification for  {trgt_catalog}.{PROC_CTGRY_CD} tables")

# COMMAND ----------



# Databricks notebook source
# DBTITLE 1,Create widgets
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("PROC_CTGRY_CD", "SRC_TO_BRNZ", "PROC_CTGRY_CD")
#dbutils.widgets.text("config_file","../config/dev/tmngpdb-conf.yaml")
dbutils.widgets.text("dbx_env","dev")
#comments
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TRMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP

# COMMAND ----------

#dbx_env = dbutils.widgets.get("config_file").rstrip().split("/", 3)[2]
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME")
PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")


src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"


config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{SRC_SYS_NAME=},{PROC_CTGRY_CD=},{config_file_name=},{config_file=},{dbx_env=}')

# COMMAND ----------

# DBTITLE 1,Run common functions ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Define Table list
# MAGIC %run ./ntb_tm_brnz_table_list

# COMMAND ----------

spark.sql("set SRC_SYS_NAME = " + dbutils.widgets.get("SRC_SYS_NAME"))
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))
job_name = 'ntb_'+SRC_SYS_NAME.lower()+'_dq_data_vrfctn_frmwrk'
spark.sql("set job_name = "+str(job_name))
print(f'{job_name=}')
job_start_ts = datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Define variables from config file
common_configs = read_yaml(config_file)
src_db = common_configs['schema']['src_db_name']
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
receiver_email = common_configs['data_quality']['receiver_email']
brz_allowed_variance = common_configs['data_quality']['brz_allowed_variance']
trm_scope = common_configs['secrets']['trm_scope']
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME")

database = 'bronze'


proc_id = spark.sql(f"""select proc_id from {data_quality_catalog}.silver.CMN_PROC_DEFN_RFRNC where proc_name='{job_name}'""").collect()[0][0]
spark.conf.set('conf.proc_id', proc_id)
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.src_db', src_db.lower()) 
spark.conf.set('conf.database', database)
spark.conf.set('conf.job_name', job_name)
spark.conf.set('conf.brz_allowed_variance', brz_allowed_variance)
spark.conf.set('config.trm_scope', trm_scope.lower()) 


emailid = receiver_email
env = dbx_env.upper()
if SRC_SYS_NAME == 'TMNGPDB':
    groups = common_configs['DMS']['groups']
    spark.conf.set('conf.groups', groups)
    schema_metadata =""
    for i in range(1,groups+1):
        schema_metadata = schema_metadata + "tmngpdb_metadata_group"+str(i) +"+"
    schema_metadata = schema_metadata[:-1]
else:
    schema_metadata = src_name+"_metadata"

print(f'{schema_metadata=},{src_db=},{trgt_catalog=}, {data_quality_catalog=} ,{receiver_email=},{brz_allowed_variance=},{trm_scope=},{SRC_SYS_NAME=}')

# COMMAND ----------

# DBTITLE 1,Define schema from table list
Schema = ["TABLE_GROUP_NAME","TABLE_NAME","FULL_LOAD","DQ_FLTR"]
df_metadata = spark.createDataFrame(data = eval(schema_metadata), schema = Schema)
#src_scope_name = "oracle_trmpvt_server"
df_metadata.display()    
df_metadata = df_metadata.select(f.upper('TABLE_NAME').alias("TABLE_NAME"),f.upper('DQ_FLTR').alias("DQ_FLTR")).distinct()

SRC_SCHEMA = src_db
TRGT_SCHEMA = trgt_catalog+'.bronze'

# COMMAND ----------

# DBTITLE 1,Calculate source and target table counts
if (PROC_CTGRY_CD == 'SRC_TO_BRNZ'):
    query_cnt_rows =[]

    for r in df_metadata.collect():
        dct = r.asDict()
        tbl_nm = dct["TABLE_NAME"]
        fltr_col = dct["DQ_FLTR"]
        try:
            print("\n")
            print(f"Performing Count and Data comparison for {tbl_nm}:")
            if fltr_col == '':
                pushdown_query = f"""(select count(*)  from {SRC_SCHEMA}.{tbl_nm})"""
            else:
                #pushdown_query = f"""(select count(*) from {SRC_SCHEMA}.{tbl_nm} where ({fltr_col}) <= to_date(current_date))"""
                pushdown_query = f"""(select count(*)  from {SRC_SCHEMA}.{tbl_nm})"""
            try:
                if SRC_SYS_NAME == 'JBTEASPS':
                    df_read_src_tbl = read_data_from_postgres_conn(pushdown_query,trm_scope)
                else:
                    df_read_src_tbl = read_data_from_oracle_conn_dsu_cmn(pushdown_query, trm_scope)
                #src_count = df_read_src_tbl.count()
                src_count = int(df_read_src_tbl.collect()[0][0]) 
                #src_count = None
                dct['RPTD_SRC_RSLT_CNT'] = src_count
            except Exception as e:
                #raise
                data_quality_result =  "Exception message: {}".format(e)
                src_count = None
                dct['RPTD_SRC_RSLT_CNT'] = src_count
                print("Exception message: {}".format(e))



            try:
                trgt_query =  f"""(select count(*) from {TRGT_SCHEMA}.{tbl_nm})"""
                #trgt_query =  f"""(select * from {TRGT_SCHEMA}.{tbl_nm} limit 160 )""" # for demo purpose
                df_read_trgt_tbl = spark.sql(trgt_query)
                #trgt_count = df_read_trgt_tbl.count()
                trgt_count = df_read_trgt_tbl.collect()[0][0]
                dct['RPTD_TRGT_RSLT_CNT'] = trgt_count
            except Exception as e:
                #raise
                data_quality_result =  "Exception message: {}".format(e)
                trgt_count = None
                dct['RPTD_TRGT_RSLT_CNT'] = trgt_count
                print("Exception message: {}".format(e))

            if src_count == trgt_count:
                #if df_read_src_tbl.exceptAll(df_read_trgt_tbl).count() ==0:
                    #sample data matches
                data_quality_result =  f"Source and Target Count Matches for Table {tbl_nm}"
                #else:
                    #sample data does not match
                #    data_quality_result =  f"Source and Target Count Match but Data Does Not Match for Table {tbl_nm}"
            else:
                print(f'{src_count=} and {trgt_count=}')
                data_quality_result =  f"Source and Target Count Does Not Match for Table {tbl_nm}"

        
            print(data_quality_result)
            dct['DQ_RSLT_MSG'] = data_quality_result
            query_cnt_rows.append(dct)
        
        except Exception as e:
            raise
            print("Exception message: {}".format(e))

# COMMAND ----------

# DBTITLE 1,Create temp table
df_dq_query_counts = spark.createDataFrame(query_cnt_rows)
df_dq_query_counts.createOrReplaceTempView("temp_dq_src_trgt_counts")

# COMMAND ----------

# DBTITLE 1,Calculate Variance
df_dq_query_counts_load = spark.sql("""
select var_check.*
,case when RPTD_VRNC_PCT > ERR_THRSHLD_PCT then 'Y' else 'N' end as VRNC_IND 
from (
select
  ${conf.proc_id} AS PROC_ID,
  '${conf.job_name}' AS PROC_NAME ,
  '${PROC_CTGRY_CD}' as PROC_CTGRY_CD,
  1 as QUERY_SET_ID,
  'CM' as QUERY_DQ_CD,
  TABLE_NAME as SRC_QUERY_NAME,
  TABLE_NAME as TRGT_QUERY_NAME,
  null as JOB_LOG_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as JOB_START_TS,
  RPTD_SRC_RSLT_CNT as RPTD_SRC_RSLT_CNT,
  RPTD_TRGT_RSLT_CNT as RPTD_TRGT_RSLT_CNT,
  cast(${conf.brz_allowed_variance} as double) as ERR_THRSHLD_PCT,
  CASE
  when RPTD_SRC_RSLT_CNT is null or RPTD_TRGT_RSLT_CNT is null then -100
    WHEN RPTD_SRC_RSLT_CNT = 0
    AND RPTD_TRGT_RSLT_CNT != 0 THEN -100
    WHEN RPTD_SRC_RSLT_CNT = 0 THEN 0.0000
    ELSE ABS(
      (
        (
          (
            FLOAT(RPTD_SRC_RSLT_CNT) - FLOAT(RPTD_TRGT_RSLT_CNT)
          ) * 100.0000
        ) / FLOAT(RPTD_SRC_RSLT_CNT)
      )
    )
  END AS RPTD_VRNC_PCT,
  DQ_RSLT_MSG AS DQ_RSLT_MSG,
  'ETL' AS AUDT_INSRT_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as AUDT_INSRT_TS,
  '${SRC_SYS_NAME}' as SRC_SYS_NAME
from
  temp_dq_src_trgt_counts)var_check""")

df_dq_query_counts_load.display()
df_dq_query_counts_load.createOrReplaceTempView("temp_dq_src_trgt_counts_load")

# COMMAND ----------

# DBTITLE 1,Insert results into CMN_PROC_VRFCTN_RSLT table
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

# DBTITLE 1,Create dataframe for tables with variance
df_dq_query_counts_variance = df_dq_query_counts_load.filter("VRNC_IND='Y'").select("SRC_SYS_NAME","PROC_CTGRY_CD",col("SRC_QUERY_NAME").alias("TRGT_TABLE_NAME"),"RPTD_SRC_RSLT_CNT","RPTD_TRGT_RSLT_CNT","RPTD_VRNC_PCT","JOB_START_TS")
df_dq_query_counts_variance.display()


# COMMAND ----------

# DBTITLE 1,Send email notification
if df_dq_query_counts_variance.count() >0:
    Appdf=df_dq_query_counts_variance
    parms = {}
    pd.set_option('display.max_colwidth', 0)
    parms['INDEXED']=Appdf.toPandas().to_html()
    notify = Notify()
    templ_str = f'{SRC_SYS_NAME}: Bronze Data Quality Report'
    msg = notify.compose_email( templ_str, f'{SRC_SYS_NAME} Bronze Data Quality Report - '+env, emailid, parms )
    notify.send_mail(msg)
else:
    print(f"No email notification sent for data variance as all table counts match for {trgt_catalog}.{database} tables")

# COMMAND ----------

dbutils.notebook.exit(f"Completed data verification for  {trgt_catalog}.{database} tables")

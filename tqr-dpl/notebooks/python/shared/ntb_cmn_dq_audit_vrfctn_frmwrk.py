# Databricks notebook source
# MAGIC %md
# MAGIC ###Purpose
# MAGIC <pre>
# MAGIC This ntbk is executed to capture the data quality counts for each job id
# MAGIC author: Pawanpreet Sangari
# MAGIC </pre>

# COMMAND ----------

dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("PROC_CTGRY_CD", "", "PROC_CTGRY_CD")
#dbutils.widgets.text("config_file","../config/dev/lom-conf.yaml")
#config_file = "../"+dbutils.widgets.get("config_file").rstrip()
#dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../notebooks/config/lom-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ./ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME")
PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")
SRC_NAME = dbutils.widgets.get("SRC_SYS_NAME").lower()
print(f'{SRC_SYS_NAME=},{PROC_CTGRY_CD=}')
spark.sql("set SRC_SYS_NAME = " + dbutils.widgets.get("SRC_SYS_NAME"))
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))
spark.sql("set SRC_NAME = " + dbutils.widgets.get("SRC_SYS_NAME").lower())

AUDT_INSRT_TS = datetime.datetime.now()
print(f'{AUDT_INSRT_TS=}')
spark.sql("set AUDT_INSRT_TS = " +str(AUDT_INSRT_TS))

# COMMAND ----------

if SRC_SYS_NAME == 'LOM':
    proc_db = lom_db
print(proc_db)
spark.sql(f"set proc_db = {proc_db}")

# COMMAND ----------

df_dq_vrfctn_query = spark.sql("""
select
  rfrnc.SRC_SYS_NAME,
  rfrnc.proc_name,
  rfrnc.PROC_CTGRY_CD,
  rfrnc.PROC_ID,
  rfrnc.job_log_id,
  rfrnc.job_start_ts,
  rfrnc.PROC_CNFG_FILE_PATH,
  src_query.src_query_set_id as query_set_id,
  src_query.SRC_QUERY_NAME,
  src_query.SRC_CNCTN_DTL_DESC,
  src_query.SRC_QUERY_TEXT,
  trgt_query.TRGT_QUERY_NAME,
  trgt_query.TRGT_CNCTN_DTL_DESC,
  trgt_query.TRGT_QUERY_TEXT,
  src_query.ERR_THRSHLD_PCT,
  src_query.QUERY_DQ_CD,
  'DQ_FRMWRK' AS AUDT_INSRT_ID,
  '${AUDT_INSRT_TS}' as AUDT_INSRT_TS
from
  (
    select
      rfrnc.SRC_SYS_NAME,
      rfrnc.proc_name,
      rfrnc.PROC_CTGRY_CD,
      rfrnc.PROC_ID,
      rfrnc.PROC_CNFG_FILE_PATH,
      max_by(job.job_log_id, job.start_ts) as job_log_id,
      max(job.start_ts) as job_start_ts
    from
      ${data_quality_db}.SILVER.CMN_PROC_DEFN_RFRNC rfrnc
      inner join ${proc_db}.silver.job_log job on rfrnc.PROC_NAME = job.job_nm
    where
      rfrnc.PROC_CTGRY_CD = '${PROC_CTGRY_CD}'
      and rfrnc.SRC_SYS_NAME = '${SRC_SYS_NAME}'
      and job.status_ct = 'completed'
    group by
      rfrnc.SRC_SYS_NAME,
      rfrnc.proc_name,
      rfrnc.PROC_CTGRY_CD,
      rfrnc.PROC_ID,
      rfrnc.PROC_CNFG_FILE_PATH
  ) rfrnc
  inner join (
    select
      asctn.PROC_NAME,
      asctn.SRC_SYS_NAME,
      asctn.QUERY_SET_ID as src_query_set_id,
      asctn.SRC_QUERY_NAME,
      asctn.ERR_THRSHLD_PCT,
      asctn.QUERY_DQ_CD,
      query_rfrnc.CNCTN_DTL_DESC AS SRC_CNCTN_DTL_DESC,
      query_rfrnc.QUERY_TEXT as SRC_QUERY_TEXT
    from
      ${data_quality_db}.SILVER.CMN_PROC_VRFCTN_QUERY_ASCTN asctn
      inner join ${data_quality_db}.SILVER.CMN_DQ_VRFCTN_QUERY_RFRNC query_rfrnc on asctn.SRC_QUERY_NAME = query_rfrnc.QUERY_NAME
      where asctn.QUERY_DQ_CD = 'CM'
  ) src_query on rfrnc.PROC_NAME = src_query.PROC_NAME
  and rfrnc.SRC_SYS_NAME = src_query.SRC_SYS_NAME
  INNER JOIN (
    select
      asctn.PROC_NAME,
      asctn.SRC_SYS_NAME,
      asctn.QUERY_SET_ID as trgt_query_set_id,
      asctn.TRGT_QUERY_NAME,
      asctn.QUERY_DQ_CD,
      asctn.ERR_THRSHLD_PCT,
      query_rfrnc.CNCTN_DTL_DESC AS TRGT_CNCTN_DTL_DESC,
      query_rfrnc.QUERY_TEXT as TRGT_QUERY_TEXT
    from
      ${data_quality_db}.SILVER.CMN_PROC_VRFCTN_QUERY_ASCTN asctn
      inner join ${data_quality_db}.SILVER.CMN_DQ_VRFCTN_QUERY_RFRNC query_rfrnc on asctn.TRGT_QUERY_NAME = query_rfrnc.QUERY_NAME
      where asctn.QUERY_DQ_CD = 'CM'
  ) trgt_query ON rfrnc.PROC_NAME = trgt_query.PROC_NAME
  and rfrnc.SRC_SYS_NAME = trgt_query.SRC_SYS_NAME
  and src_query.src_query_set_id = trgt_query.trgt_query_set_id
  """)
df_dq_vrfctn_query.display()

# COMMAND ----------

PROC_CNFG_FILE_PATH = df_dq_vrfctn_query.select("PROC_CNFG_FILE_PATH").collect()[0][0]
config_file_path = PROC_CNFG_FILE_PATH

if SRC_SYS_NAME == 'LOM':
    configs = read_yaml(config_file_path)
    dataload_dt = configs['lom']['dataload_dt']
    print(f'{dataload_dt=}')
    spark.sql(f"set dataload_dt = {dataload_dt}")

    pub_code_list = configs['lom_pub']['codes']
    print(f'{pub_code_list=}')
    spark.sql(f"set pub_code_list = {pub_code_list}")

    first_code_list = configs['lom_first']['codes']
    print(f'{first_code_list=}')
    spark.sql(f"set first_code_list = {first_code_list}")

    non_first_code_list = configs['lom_non-first']['codes']
    print(f'{non_first_code_list=}')
    spark.sql(f"set non_first_code_list = {non_first_code_list}")

# COMMAND ----------

# DBTITLE 1,Replace Variables with corresponding values
df_cleaned_dq_query = df_dq_vrfctn_query.withColumn("CLEANED_SRC_QUERY_TXT", f.regexp_replace(f.regexp_replace(f.regexp_replace(f.regexp_replace(f.expr("SRC_QUERY_TEXT"),"\{dataload_dt\}",dataload_dt),"\{pub_code_list\}",pub_code_list),"\{first_code_list\}",first_code_list),"\{non_first_code_list\}",non_first_code_list))

# COMMAND ----------

src_query_cnt_rows = []
df_dq_src_delta_vrfctn_query = df_cleaned_dq_query.filter(df_cleaned_dq_query.SRC_CNCTN_DTL_DESC.contains('DELTA_LAKE'))
for r  in df_dq_src_delta_vrfctn_query.collect():
  dct = r.asDict()
  dct['RPTD_SRC_RSLT_CNT'] = int(spark.sql(r["CLEANED_SRC_QUERY_TXT"]).collect()[0][0])
  src_query_cnt_rows.append(dct)

# COMMAND ----------

df_dq_src_mysql_vrfctn_query = df_cleaned_dq_query.filter(df_cleaned_dq_query.SRC_CNCTN_DTL_DESC.contains('MYSQL_TQR_LOM_DB'))
for r  in df_dq_src_mysql_vrfctn_query.collect():
  dct = r.asDict()
  dct['RPTD_SRC_RSLT_CNT'] = int(read_data_from_mysql_conn_dsu(r["SRC_QUERY_TEXT"],"tqr_lom").collect()[0][0])
  src_query_cnt_rows.append(dct)

# COMMAND ----------

df_dq_src_query_counts = spark.createDataFrame(src_query_cnt_rows)

# COMMAND ----------

trgt_query_cnt_rows =[]
df_dq_trgt_delta_query_counts = df_dq_src_query_counts.filter(df_dq_src_query_counts.TRGT_CNCTN_DTL_DESC.contains('DELTA_LAKE'))
for r  in df_dq_trgt_delta_query_counts.collect():
  dct = r.asDict()
  dct['RPTD_TRGT_RSLT_CNT'] = int(spark.sql(r["TRGT_QUERY_TEXT"]).collect()[0][0])
  trgt_query_cnt_rows.append(dct)

# COMMAND ----------

df_dq_trgt_mysql_query_counts = df_dq_src_query_counts.filter(df_dq_src_query_counts.TRGT_CNCTN_DTL_DESC.contains('MYSQL_TQR_LOM_DB'))
for r  in df_dq_trgt_mysql_query_counts.collect():
  dct = r.asDict()
  dct['RPTD_TRGT_RSLT_CNT'] = int(read_data_from_mysql_conn_dsu(r["TRGT_QUERY_TEXT"],"tqr_lom").collect()[0][0])
  trgt_query_cnt_rows.append(dct)

# COMMAND ----------

df_dq_query_counts = spark.createDataFrame(trgt_query_cnt_rows)

# COMMAND ----------

df_dq_query_counts.createOrReplaceTempView("temp_dq_src_trgt_counts")

# COMMAND ----------

# DBTITLE 1,Load into data quality verification result table
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
# MAGIC   PROC_NAME,
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
# MAGIC   CASE
# MAGIC     WHEN RPTD_SRC_RSLT_CNT = 0
# MAGIC     AND RPTD_TRGT_RSLT_CNT != 0 THEN -100
# MAGIC     WHEN RPTD_SRC_RSLT_CNT = 0 THEN 0.0000
# MAGIC     ELSE ABS(
# MAGIC       (
# MAGIC         (
# MAGIC           (
# MAGIC             FLOAT(RPTD_SRC_RSLT_CNT) - FLOAT(RPTD_TRGT_RSLT_CNT)
# MAGIC           ) * 100.0000
# MAGIC         ) / FLOAT(RPTD_SRC_RSLT_CNT)
# MAGIC       )
# MAGIC     )
# MAGIC   END AS RPTD_VRNC_PCT,
# MAGIC   NULL AS DQ_RSLT_MSG,
# MAGIC   AUDT_INSRT_ID,
# MAGIC   cast( AUDT_INSRT_TS as TIMESTAMP) as AUDT_INSRT_TS,
# MAGIC   SRC_SYS_NAME
# MAGIC from
# MAGIC   temp_dq_src_trgt_counts

# COMMAND ----------

#Create a Report
df_dq_report=spark.sql(f"""select PROC_ID,SRC_SYS_NAME,PROC_CTGRY_CD,PROC_NAME,JOB_START_TS,AUDT_INSRT_TS,QUERY_SET_ID,RPTD_VRNC_PCT
from {data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT
where  PROC_CTGRY_CD = '{PROC_CTGRY_CD}' 
AND AUDT_INSRT_TS = '{AUDT_INSRT_TS}'
AND SRC_SYS_NAME = '{SRC_SYS_NAME}'
AND RPTD_VRNC_PCT!='0.0' """)
df_dq_report.show(40,False)

# COMMAND ----------

dbutils.notebook.exit(f"Completed Data Audit check for SRC_SYS_NAME: {SRC_SYS_NAME} and proc category code: {PROC_CTGRY_CD} ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  ${config.data_quality_db}.SILVER.CMN_PROC_VRFCTN_RSLT order by proc_vrfctn_rslt_id desc limit 5

# COMMAND ----------



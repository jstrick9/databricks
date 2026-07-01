# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC 1. Update alteryx workflows to create tables corresponding to hyper files for dq comparison
# MAGIC 2. Add dq job in wfs
# MAGIC 3. Update email ids in config file
# MAGIC
# MAGIC </pre>

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("PROC_CTGRY_CD", "FIRST_LEVEL", "PROC_CTGRY_CD")#FIRST_LEVEL, SEC_THIRD_LEVEL
dbutils.widgets.text("DASHBOARD_NAME", "", "DASHBOARD_NAME")#POST_REG,TM_QUALITY,FILINGS_GOODS_SERVICES,FORM_PARAGRAPH,PENDENCY_AND_INVENTORY,TTAB

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")
DASHBOARD_NAME = dbutils.widgets.get("DASHBOARD_NAME")
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=},{PROC_CTGRY_CD=}')

# COMMAND ----------

# DBTITLE 1,Run common functions ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']
print(f"{trgt_catalog=},{src_catalog=}")
spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))
spark.sql("set DASHBOARD_NAME = " + dbutils.widgets.get("DASHBOARD_NAME"))

job_start_ts = datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Define Table list
trm_reports_first_level_list = [#dbx_table_name, legacy_table_name
    #('prosecution_history','legacy_prosecution_hstry'),
    #('correspondence','legacy_correspondence'),
    #('milestone','legacy_milestone'),
    #('bibliography','legacy_bibliography'),
    #('class','legacy_class'),
    #('owner','legacy_owner'),
    #('divisionals','legacy_divisionals')
    ('silver.prosecution_history','prosecution_hstry'),
    ('silver.correspondence','correspondence'),
    ('silver.milestone','milestone'),
    ('silver.bibliography','bibliography'),
    ('silver.class','class'),
    ('silver.owner','owner'),
    ('silver.divisionals','divisionals')
]

trm_reports_sec_third_level_post_reg_list = [#dbx_table_name, legacy_table_name
    ('silver.post_reg_milestone','post_reg_milestone'),
    ('silver.post_reg_detail','post_reg_detail'),
    ('silver.pr_milestone_counts','pr_milestone_counts'),
    ('silver.pr_detail_counts','pr_detail_counts'),
    ('gold.post_reg_dashboard','post_reg_dashboard'),
    ('gold.post_reg_dashboard_running','post_reg_dashboard_running'),
    ('gold.post_reg_detail_dashboard','post_reg_detail_dashboard'),
    ('gold.post_reg_workforce','post_reg_workforce'),
]
trm_reports_sec_third_level_tm_quality_list = [#dbx_table_name, legacy_table_name
    ('silver.quality_counts','quality_counts'),
    ('gold.quality_dashboard','quality_dashboard'),
    ('gold.quality_dashboard_pivot','quality_dashboard_pivot'),
]

trm_reports_sec_third_level_pendency_and_inventory_list = [#dbx_table_name, legacy_table_name
    ('silver.on_hold','on_hold'),
    ('silver.pendency_counts','pendency_counts'),
    ('gold.pendency_dashboard','pendency_dashboard'),
    ('gold.inventory_madrid','inventory_madrid'),
    ('gold.inventory_dashboard_bd_occurrence','inventory_dashboard_bd_occurrence'),
    ('gold.inventory_dashboard_ea_counts','inventory_dashboard_ea_counts'),
    ('gold.inventory_unexamined_hstry','inventory_unexamined_hstry'),
    ('gold.inventory_dashboard_ratio','inventory_dashboard_ratio'),
    ('gold.inventory_dashboard_filings','inventory_dashboard_filings'),
    ('gold.inventory_dashboard_pendency','inventory_dashboard_pendency'),
    ('gold.inventory_dashboard_running','inventory_dashboard_running'),
]

trm_reports_sec_third_level_form_paragraph_list = [#dbx_table_name, legacy_table_name
    ('silver.form_paragraph_counts','form_paragraph_counts'),
    ('gold.form_paragraph_dashboard','form_paragraph_dashboard'),
]

trm_reports_sec_third_level_filings_goods_services_list = [#dbx_table_name, legacy_table_name
    ('silver.fixed_class_counts','fixed_class_counts'),
    ('silver.filings_counts','filings_counts'),
    ('silver.goods_services_sn_list','goods_services_sn_list'),
    ('gold.filings_dashboard','filings_dashboard'),
    ('gold.goods_services_dashboard','goods_services_dashboard'),
]

trm_reports_sec_third_level_ttab_list = [#dbx_table_name, legacy_table_name
    ('silver.ttab_detail_summary','ttab_detail_summary'),
    ('silver.ttab_detail_counts','ttab_detail_counts'),
    ('gold.ttab_detail','ttab_detail'),
    ('gold.ttab_workloads','ttab_workloads'),
    ('gold.ttab_decision_rates','ttab_decision_rates'),
]

if PROC_CTGRY_CD == 'FIRST_LEVEL':
    table_list = "trm_reports_"+PROC_CTGRY_CD.lower()+"_list"
    job_name = "ntb_trm_reports_"+PROC_CTGRY_CD.lower()+"_data_vrfctn_frmwrk"
else:
    table_list = "trm_reports_"+PROC_CTGRY_CD.lower()+"_"+DASHBOARD_NAME.lower()+"_list"
    job_name = "ntb_trm_reports_"+PROC_CTGRY_CD.lower()+"_"+DASHBOARD_NAME.lower()+"_data_vrfctn_frmwrk"
print(f'{table_list=},{job_name=}')
spark.sql("set job_name = "+str(job_name))

# COMMAND ----------

# DBTITLE 1,Define variables from config file
common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
receiver_email = common_configs['data_quality']['receiver_email']
brz_allowed_variance = common_configs['data_quality']['brz_allowed_variance']

#trgt_database = 'silver'
emailid = receiver_email
env = dbx_env.upper()
proc_id = spark.sql(f"""select proc_id from {data_quality_catalog}.silver.CMN_PROC_DEFN_RFRNC where proc_name='{job_name}'""").collect()[0][0]
spark.conf.set('conf.proc_id', proc_id)
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.altrx_catalog', altrx_catalog.lower()) 
spark.conf.set('config.altrx_schema', altrx_schema.lower())
#spark.conf.set('conf.trgt_database', trgt_database)
spark.conf.set('conf.job_name', job_name)
spark.conf.set('conf.brz_allowed_variance', brz_allowed_variance)

print(f'{trgt_catalog=}, {altrx_catalog=},{altrx_schema=}, {data_quality_catalog=} ,{receiver_email=},{brz_allowed_variance=}')

# COMMAND ----------

# DBTITLE 1,Define schema from table list
Schema = ["dbx_table_name","legacy_table_name"]
df_metadata = spark.createDataFrame(data = eval(table_list), schema = Schema)
df_metadata.display()    
df_metadata = df_metadata.select(f.upper('dbx_table_name').alias("dbx_table_name"),f.upper('legacy_table_name').alias("legacy_table_name")).distinct()

LEGACY_SCHEMA = altrx_catalog+'.'+altrx_schema
TRGT_SCHEMA = trgt_catalog
print(f'{LEGACY_SCHEMA=},{TRGT_SCHEMA=}')

# COMMAND ----------

# DBTITLE 1,Calculate Legacy and Databricks table counts
query_cnt_rows =[]

for r in df_metadata.collect():
    dct = r.asDict()
    dbx_tbl_nm = dct["dbx_table_name"]
    legacy_tbl_nm = dct["legacy_table_name"]
    try:
        print("\n")
        print(f"Performing Count comparison for {dbx_tbl_nm}:")
        pushdown_query = f"""(select count(*)  from {LEGACY_SCHEMA}.{legacy_tbl_nm})"""#{LEGACY_SCHEMA}.{legacy_tbl_nm}
        df_read_legacy_tbl = spark.sql(pushdown_query)
        legacy_tbl_count = df_read_legacy_tbl.collect()[0][0]
        dct['RPTD_LEGACY_RSLT_CNT'] = legacy_tbl_count
    except Exception as e:
                #raise
                data_quality_result =  "Exception message: {}".format(e)
                legacy_tbl_count = 0
                dct['RPTD_LEGACY_RSLT_CNT'] = legacy_tbl_count
                print("Exception message: {}".format(e))
    try:
        pushdown_query = f"""(select count(*)  from {TRGT_SCHEMA}.{dbx_tbl_nm})"""
        df_read_dbx_tbl = spark.sql(pushdown_query)
        dbx_tbl_count = df_read_dbx_tbl.collect()[0][0]
        dct['RPTD_DBX_RSLT_CNT'] = dbx_tbl_count
    except Exception as e:
        data_quality_result =  "Exception message: {}".format(e)
        dbx_tbl_count = 0
        dct['RPTD_DBX_RSLT_CNT'] = dbx_tbl_count
        print("Exception message: {}".format(e))

    if legacy_tbl_count == dbx_tbl_count:
                #if df_read_src_tbl.exceptAll(df_read_trgt_tbl).count() ==0:
                    #sample data matches
        data_quality_result =  f"Source and Target Count Matches for Table {dbx_tbl_nm}"
                #else:
                    #sample data does not match
                #    data_quality_result =  f"Source and Target Count Match but Data Does Not Match for Table {tbl_nm}"
    else:
        print(f'{legacy_tbl_count=} and {dbx_tbl_count=}')
        data_quality_result =  f"Legacy and Databricks Table Count Does Not Match for Table {dbx_tbl_nm}"

        
    print(data_quality_result)
    dct['DQ_RSLT_MSG'] = data_quality_result
    query_cnt_rows.append(dct)

# COMMAND ----------

# DBTITLE 1,Create temp table
df_dq_query_counts = spark.createDataFrame(query_cnt_rows)
df_dq_query_counts.createOrReplaceTempView("temp_dq_legacy_trgt_counts")

# COMMAND ----------

df_dq_query_counts.display()

# COMMAND ----------

# DBTITLE 1,Calculate Variance
df_dq_query_counts_load = spark.sql("""
select var_check.*
,case when abs(RPTD_VRNC_PCT) > ERR_THRSHLD_PCT then 'Y' else 'N' end as VRNC_IND 
from (
select
  ${conf.proc_id} AS PROC_ID,
  '${conf.job_name}' AS PROC_NAME ,
  '${PROC_CTGRY_CD}' as PROC_CTGRY_CD,
  1 as QUERY_SET_ID,
  'CM' as QUERY_DQ_CD,
  legacy_table_name as SRC_QUERY_NAME,
  dbx_table_name as TRGT_QUERY_NAME,
  null as JOB_LOG_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as JOB_START_TS,
  RPTD_LEGACY_RSLT_CNT as RPTD_SRC_RSLT_CNT,
  RPTD_DBX_RSLT_CNT as RPTD_TRGT_RSLT_CNT,
  cast(${conf.brz_allowed_variance} as double) as ERR_THRSHLD_PCT,
  CASE
  when RPTD_LEGACY_RSLT_CNT is null or RPTD_DBX_RSLT_CNT is null then -100
    WHEN RPTD_LEGACY_RSLT_CNT = 0
    AND RPTD_DBX_RSLT_CNT != 0 THEN -100
    WHEN RPTD_LEGACY_RSLT_CNT = 0 THEN 0.0000
    ELSE round(ABS(
      (
        (
          (
            FLOAT(RPTD_LEGACY_RSLT_CNT) - FLOAT(RPTD_DBX_RSLT_CNT)
          ) * 100.0000
        ) / FLOAT(RPTD_LEGACY_RSLT_CNT)
      )
    ),2)
  END AS RPTD_VRNC_PCT,
  DQ_RSLT_MSG AS DQ_RSLT_MSG,
  'ETL' AS AUDT_INSRT_ID,
  from_utc_timestamp(current_timestamp(),'America/New_York') as AUDT_INSRT_TS,
  'TRM_REPORTS' as SRC_SYS_NAME
from
  temp_dq_legacy_trgt_counts)var_check""")

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

# DBTITLE 1,Create Dataframe for tables with variance
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
    templ_str = f'TRM Reports: {PROC_CTGRY_CD} {DASHBOARD_NAME}Data Quality Report'
    msg = notify.compose_email( templ_str, f'TRM Reports {PROC_CTGRY_CD} {DASHBOARD_NAME} Data Quality Report - '+env, emailid, parms )
    notify.send_mail(msg)
else:
    print(f"No email notification sent for data variance as all table counts match for {trgt_catalog} {PROC_CTGRY_CD} {DASHBOARD_NAME} tables")

# COMMAND ----------

dbutils.notebook.exit(f"Completed data verification for  {trgt_catalog} {PROC_CTGRY_CD} {DASHBOARD_NAME} tables")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe hive_metastore.alteryx_etldb_dev.owner

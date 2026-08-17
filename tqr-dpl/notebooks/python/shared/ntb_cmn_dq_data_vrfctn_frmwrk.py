# Databricks notebook source
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("PROC_CTGRY_CD", "", "PROC_CTGRY_CD")
dbutils.widgets.text("dbx_env","dev") 

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME")
PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")

src_name = SRC_SYS_NAME.lower()
proc_ctgry = PROC_CTGRY_CD.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{SRC_SYS_NAME=},{PROC_CTGRY_CD=},{config_file_name=},{config_file=},{dbx_env=}')

spark.sql("set SRC_SYS_NAME = " + dbutils.widgets.get("SRC_SYS_NAME"))
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))

# COMMAND ----------

# MAGIC %run ./ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

job_name = 'ntb_'+src_name+"_"+proc_ctgry+'_dq_data_vrfctn_frmwrk'
spark.sql("set job_name = "+str(job_name))
import pytz
from pytz import timezone
job_start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{job_name=},{job_start_ts=}')

# COMMAND ----------

common_configs = read_yaml(config_file)
#print(common_configs)
receiver_email = common_configs['data_quality']['receiver_email']
dataverify_dt = common_configs['data_quality']['dataverify_dt']
data_quality_catalog =  common_configs['schema']['data_quality_catalog']
tqr_catalog = common_configs['schema']['tqr_catalog']
trm_catalog = common_configs['schema']['trm_catalog']
tqr_src_db = common_configs['schema']['tqr_src_db']
if PROC_CTGRY_CD == 'SRC_TO_BRNZ':
    allowed_variance = common_configs['data_quality']['brnz_allowed_variance']
else:
    allowed_variance = common_configs['data_quality']['gold_allowed_variance']

print(f'{data_quality_catalog=} ,{receiver_email=},{allowed_variance=}')

proc_id = spark.sql(f"""select proc_id from {data_quality_catalog}.silver.CMN_PROC_DEFN_RFRNC where proc_name='{job_name}'""").collect()[0][0]
spark.conf.set('conf.proc_id', proc_id)
spark.conf.set('config.data_quality_catalog', data_quality_catalog.lower())
#spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
#spark.conf.set('config.src_db', src_db.lower()) 
#spark.conf.set('conf.database', database)
spark.conf.set('conf.job_name', job_name)
spark.conf.set('conf.allowed_variance', allowed_variance)

emailid = receiver_email
env = dbx_env.upper()
schema_metadata = src_name+"_metadata"


# COMMAND ----------

# DBTITLE 1,Define schema from table list
if ((SRC_SYS_NAME == 'TQR') and (PROC_CTGRY_CD == 'SRC_TO_BRNZ')):
    TQR_SRC_MYSQL_TBLS = [{'TABLE_NAME': 'business_event'}, {'TABLE_NAME': 'stnd_business_event_reason'}, {'TABLE_NAME': 'stnd_fee_process_type'},{'TABLE_NAME': 'stnd_mark_drawing_type'}, {'TABLE_NAME': 'tm_employee_assignment'}, {'TABLE_NAME': 'tm_filing_basis'}, {'TABLE_NAME': 'tm_literal'}, {'TABLE_NAME': 'trademark'}]
    TQR_TBLS = spark.createDataFrame(TQR_SRC_MYSQL_TBLS)

elif ((SRC_SYS_NAME == 'TQR') and (PROC_CTGRY_CD == 'GLD_TO_DM')): 
    TQR_DM_TBLS = [{'TABLE_NAME': 'event_inventory_pool'},{'TABLE_NAME': 'quality_review_metric'}]
    TQR_TBLS = spark.createDataFrame(TQR_DM_TBLS)

SRC_SCHEMA = tqr_src_db
TRGT_SCHEMA = trm_catalog+'.bronze'
print(TRGT_SCHEMA)

# COMMAND ----------

# DBTITLE 1,Bronze: Calculate source and target table counts 
if ((SRC_SYS_NAME == 'TQR') and (PROC_CTGRY_CD == 'SRC_TO_BRNZ')):
    query_cnt_rows =[]
    for r in TQR_TBLS.collect(): 
        dct = r.asDict()
        tbl_nm = dct["TABLE_NAME"]
        try:
            print("\n")
            print(f"Performing Count and Data comparison for {tbl_nm}:")
            pushdown_query = f"""(select * from {SRC_SCHEMA}.{tbl_nm})"""
            try:
                df_read_TQR_src_tbl = read_data_from_oracle_conn_dsu(pushdown_query, SRC_SCHEMA)
                src_count = df_read_TQR_src_tbl.count()
                dct['RPTD_SRC_RSLT_CNT'] = src_count
            except Exception as e:
                #raise
                print("Exception message: {}".format(e))
                src_count = None
                dct['RPTD_SRC_RSLT_CNT'] = src_count
                print("Exception message: {}".format(e))

            

            trgt_query =  f"""(select * from {TRGT_SCHEMA}.{tbl_nm})"""
            df_read_TQR_trgt_tbl = spark.sql(trgt_query)
            #df_read_TQR_trgt_tbl.display()
            trgt_count = df_read_TQR_trgt_tbl.count()
            dct['RPTD_TRGT_RSLT_CNT'] = trgt_count

            if src_count == trgt_count:
                if df_read_TQR_src_tbl.exceptAll(df_read_TQR_trgt_tbl).count() ==0:
                    #sample data matches
                    data_quality_result =  f"Source and Target Count and Data Match for Table {tbl_nm}"
                else:
                    #sample data does not match
                    data_quality_result =  f"Source and Target Count Match but Data Does Not Match for Table {tbl_nm}"
            else:
                print(f'{src_count=} and {trgt_count=}')
                data_quality_result =  f"Data match not performed as Source and Target Count Does Not Match for Table {tbl_nm}"

        
            print(data_quality_result)
            dct['DQ_RSLT_MSG'] = data_quality_result
            query_cnt_rows.append(dct)
        except Exception as e:
            raise
            print("Exception message: {}".format(e))

# COMMAND ----------

# DBTITLE 1,Gold: Calculate source and target table counts 
if ((SRC_SYS_NAME == 'TQR') and (PROC_CTGRY_CD == 'GLD_TO_DM')):
    
    src_query_text = f"""(select review_type_cd,serial_num_tx,source_system_nm,search_present_in,source_event_dt,docket_in,mark_literal_element_tx,mark_drawing_type_cd,mark_drawing_type_title_tx,mark_description_tx,examiner_employee_no,organization_cd, event_json_doc,inventory_create_ts,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no from {tqr_catalog}.gold.event_inventory  where source_event_dt >=  {dataverify_dt} )"""
    trgt_query_text = f"""(select fk_review_type_id,serial_no,source_system_nm,search_present_in,source_event_dt,docket_in,mark_literal_element_tx,mark_drawing_type_cd,mark_drawing_type_title_tx,mark_description_tx,examiner_employee_no,organization_cd,event_json_doc,inventory_create_ts,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no from event_inventory_pool where source_event_dt >=  {dataverify_dt} ) """
    quality_metric_query_text = f"""select  DATE_FORMAT(latest_review_status_ts, '%M-%d-%Y'), count(1) from bdr.v_quality_review
    where latest_overall_review_status_cd in ('C_FINALIZED','FINALIZED')
    and quality_review_id not in (select fk_quality_review_id from bdr.quality_review_metric)
    group by  DATE_FORMAT(latest_review_status_ts, '%M-%d-%Y')"""
    #call common fucntion for full data match
    #result = full_data_match('GLD_TO_DM','TQR','ntb_tqr_mysql_event_inventory_pool_load',src_query_text,trgt_query_text,'MYSQL_TQR_DB')
    #print(result)
    query_cnt_rows =[]
    for r in TQR_TBLS.collect(): 
        dct = r.asDict()
        tbl_nm = dct["TABLE_NAME"]
        try:
            if tbl_nm !='quality_review_metric':
                print("\n")
                print(f"Performing Count and Data comparison for {tbl_nm}:")
                src_query = src_query_text
                df_src = spark.sql(src_query)
                src_count = df_src.count()
                dct['RPTD_SRC_RSLT_CNT'] = src_count
            #Query Target table
                trgt_query = trgt_query_text
                df_trgt = read_data_from_mysql_conn_dsu(trgt_query, "bdr")
                trgt_count = df_trgt.count()
                dct['RPTD_TRGT_RSLT_CNT'] = trgt_count
            #Compare src and target df for sample data match
                if src_count == trgt_count:
                    if df_src.exceptAll(df_trgt).count() ==0 and df_trgt.exceptAll(df_src).count() ==0:
                    #sample data matches
                        data_quality_result =  f"Source and Target Count and Data Match for Table {tbl_nm}"
                    else:
                        #sample data does not match
                        data_quality_result =  f"Source and Target Count Match but Data Does Not Match for Table {tbl_nm}"
                else:
                    print(f'{src_count=} and {trgt_count=}')
                    data_quality_result =  f"Data match not performed as Source and Target Count Does Not Match for Table {tbl_nm}"
            else:
                trgt_query = quality_metric_query_text
                df_trgt = read_data_from_mysql_conn_dsu(trgt_query, "bdr")
                trgt_count = df_trgt.count()
                dct['RPTD_TRGT_RSLT_CNT'] = trgt_count
                dct['RPTD_SRC_RSLT_CNT'] = 0
                if trgt_count >0:
                    if trgt_count ==1:
                        data_quality_result =  f"{trgt_count} review is not processed into {tbl_nm} table"
                    else:
                        data_quality_result =  f"{trgt_count} reviews are not processed into {tbl_nm} table "
                else:
                    data_quality_result =  f"no reviews are left for processing into {tbl_nm} table"
            print(data_quality_result)
            dct['DQ_RSLT_MSG'] = data_quality_result
            
            query_cnt_rows.append(dct)

        except Exception as e:
            print("Exception message: {}".format(e))      

# COMMAND ----------

# DBTITLE 1,Create temp table
df_dq_query_counts = spark.createDataFrame(query_cnt_rows)
df_dq_query_counts.createOrReplaceTempView("temp_dq_src_trgt_counts")
df_dq_query_counts.display()

# COMMAND ----------

# DBTITLE 1,Calculate Variance
df_dq_query_counts_load = spark.sql("""
select var_check.*
,case when RPTD_VRNC_PCT > ERR_THRSHLD_PCT or DQ_RSLT_MSG like '%Data Does Not Match for Table%' then 'Y' else 'N' end as VRNC_IND 
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
  cast(${conf.allowed_variance} as double) as ERR_THRSHLD_PCT,
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
# MAGIC   ${config.data_quality_catalog}.SILVER.CMN_PROC_VRFCTN_RSLT (
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
df_dq_query_counts_variance = df_dq_query_counts_load.select("SRC_SYS_NAME","PROC_CTGRY_CD",col("SRC_QUERY_NAME").alias("TRGT_TABLE_NAME"),"RPTD_SRC_RSLT_CNT","RPTD_TRGT_RSLT_CNT","RPTD_VRNC_PCT","JOB_START_TS")
df_dq_query_counts_variance.display()

# COMMAND ----------

# DBTITLE 1,Send email notification
import pandas as pd
if df_dq_query_counts_variance.count() >0:
    Appdf=df_dq_query_counts_variance
    parms = {}
    pd.set_option('display.max_colwidth', 0)
    parms['INDEXED']=Appdf.toPandas().to_html()
    notify = Notify()
    templ_str = f'{SRC_SYS_NAME}: {PROC_CTGRY_CD} Data Quality Report'
    msg = notify.compose_email( templ_str, f'{SRC_SYS_NAME} {PROC_CTGRY_CD} Data Quality Report - '+env, emailid, parms )
    notify.send_mail(msg)
else:
    print(f"No email notification sent for data variance as all table counts match for all tables")

# COMMAND ----------

dbutils.notebook.exit(f"Completed data verification for  TQR {PROC_CTGRY_CD} tables")

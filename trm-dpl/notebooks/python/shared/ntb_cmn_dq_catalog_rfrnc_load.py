# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("PROC_CTGRY_CD", "", "PROC_CTGRY_CD")
#SRC_TO_BRNZ, GLD_TO_DM, BRNZ_TO_SLVR, SLVR_TO_GLD
#TMBUSCALENDAR,TMINTLTM,TMNGPDB,DATABRIDGE,EOGADMIN,JBTEASPS,PROCEEDING,TMPRODVTY,TMREVIEWS,TMWORKER, TMNGFPEPP, EFOIAP, TMNGIDMP

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()

PROC_CTGRY_CD = dbutils.widgets.get("PROC_CTGRY_CD")
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

if SRC_SYS_NAME  == 'PROCEEDING':
    SRC_SYS_NAME = "TMPROCEEDING"

spark.sql("set SRC_SYS_NAME = " + dbutils.widgets.get("SRC_SYS_NAME"))
spark.sql("set PROC_CTGRY_CD = " + dbutils.widgets.get("PROC_CTGRY_CD"))


print(f'{config_file=}, {SRC_SYS_NAME=}')

# COMMAND ----------

# MAGIC %run ./ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
data_quality_db = common_configs['schema']['data_quality_catalog']
receiver_email = common_configs['data_quality']['receiver_email']
emailid = receiver_email
print(SRC_SYS_NAME)

if SRC_SYS_NAME == 'TMNGPDB' or SRC_SYS_NAME == 'TMBUSCALENDAR'  or SRC_SYS_NAME == 'TMINTLTM' or SRC_SYS_NAME == 'EFOIAP' or SRC_SYS_NAME == 'EOGADMIN' or SRC_SYS_NAME == 'TMPRODVTY'  or SRC_SYS_NAME == 'TMREVIEWS' or SRC_SYS_NAME == 'TMWORKER' or SRC_SYS_NAME == 'TMNGFPEPP'  or SRC_SYS_NAME == 'JBTEASPS' or SRC_SYS_NAME == 'TMPROCEEDING' or SRC_SYS_NAME == 'TMNGIDMP' :
    CNCTN_DTL_DESC =  common_configs['secrets']['trm_scope']
elif SRC_SYS_NAME == 'DATABRIDGE'  :
    CNCTN_DTL_DESC =  common_configs['secrets']['mysql_scope']   
    
#SRC_TO_BRNZ, GLD_TO_DM, BRNZ_TO_SLVR, SLVR_TO_GLD
if PROC_CTGRY_CD =='SRC_TO_BRNZ':
    TARGET_DB_NAME = "BRONZE"
elif PROC_CTGRY_CD =='BRNZ_TO_SLVR':
    TARGET_DB_NAME = "SILVER"
elif PROC_CTGRY_CD =='SLVR_TO_GLD':
    TARGET_DB_NAME = "GOLD"

spark.conf.set('conf.data_quality_db', data_quality_db.lower())
spark.conf.set('conf.src_sys_name', SRC_SYS_NAME)
spark.conf.set('conf.proc_ctgry_cd', PROC_CTGRY_CD)
spark.conf.set('conf.config_file_name', config_file_name)
spark.conf.set('conf.cnctn_dtl_desc', CNCTN_DTL_DESC)
spark.conf.set('conf.target_db_name', TARGET_DB_NAME)
print(f'{SRC_SYS_NAME=},{CNCTN_DTL_DESC=},{TARGET_DB_NAME=}')


# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table  ${conf.data_quality_db}.silver.CMN_CATALOG_RFRNC_STG

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ${conf.data_quality_db}.SILVER.CMN_CATALOG_RFRNC WHERE SRC_SYS_NAME='${conf.src_sys_name}' and PROC_CTGRY_CD='${conf.proc_ctgry_cd}'

# COMMAND ----------

# DBTITLE 1,Validate CMN_CATALOG_RFRNC_STG TABLE
df_cnt = spark.sql("""Select UPPER(TARGET_CATALOG_NAME)AS TARGET_CATALOG_NAME,UPPER(TRGT_TBL_NAME)AS TRGT_TBL_NAME, UPPER(SRC_TBL_NAME) AS  SRC_TABLE_NAME, count(*) from ${conf.data_quality_db}.silver.cmn_catalog_rfrnc_stg  where SRC_SYS_NAME = '${conf.src_sys_name}'  and TARGET_DB_NAME = '${conf.target_db_name}' group by UPPER(TARGET_CATALOG_NAME),UPPER(TRGT_TBL_NAME),UPPER(SRC_TBL_NAME) having count(*) >1""" )
df_cnt.display()
if  (df_cnt.count()>0):
    raise ValueError('There are duplicate Table enteries in CMN_CATALOG_RFRNC_STG TABLE')
else:
    print('No enteries are found with duplicate table names in CMN_CATALOG_RFRNC_STG TABLE')

# COMMAND ----------

# MAGIC %sql
# MAGIC  INSERT INTO ${conf.data_quality_db}.SILVER.CMN_CATALOG_RFRNC(
# MAGIC   SOURCE_DB_NAME,TARGET_CATALOG_NAME,TARGET_DB_NAME,CNCTN_DTL_DESC,SRC_TBL_NAME,TRGT_TBL_NAME,IN_DBX_IND,OBJECT_TYPE,SRC_SYS_NAME,PROC_CTGRY_CD,AUDT_INSRT_ID,AUDT_INSRT_TS
# MAGIC  )
# MAGIC select
# MAGIC   SOURCE_DB_NAME,
# MAGIC   TARGET_CATALOG_NAME,
# MAGIC   TARGET_DB_NAME,
# MAGIC   '${conf.cnctn_dtl_desc}' as CNCTN_DTL_DESC ,
# MAGIC   SRC_TBL_NAME,
# MAGIC   TRGT_TBL_NAME,
# MAGIC   case when TRGT_TBL_NAME is not null then 'Y' else 'N' end as IN_DBX_IND,
# MAGIC   OBJECT_TYPE,
# MAGIC   '${conf.src_sys_name}' as SRC_SYS_NAME,
# MAGIC   '${conf.proc_ctgry_cd}' as PROC_CTGRY_CD ,
# MAGIC   'ETL' AS AUDT_INSRT_ID,
# MAGIC   from_utc_timestamp(current_timestamp(),'America/New_York')as AUDT_INSRT_TS
# MAGIC from
# MAGIC   ${conf.data_quality_db}.SILVER.CMN_CATALOG_RFRNC_STG
# MAGIC   where SRC_SYS_NAME = '${conf.src_sys_name}'
# MAGIC   and TARGET_DB_NAME = '${conf.target_db_name}'

# COMMAND ----------

# DBTITLE 1,Validate CMN_CATALOG_RFRNC TABLE
df_cnt = spark.sql("""Select UPPER(TARGET_CATALOG_NAME)AS TARGET_CATALOG_NAME,UPPER(TRGT_TBL_NAME)AS TRGT_TBL_NAME, UPPER(SRC_TBL_NAME) AS  SRC_TABLE_NAME, count(*) from  ${conf.data_quality_db}.silver.cmn_catalog_rfrnc group by UPPER(TARGET_CATALOG_NAME),UPPER(TRGT_TBL_NAME),UPPER(SRC_TBL_NAME) having count(*) >1""" )
df_cnt.display()
if  (df_cnt.count()>0):
    raise ValueError('There are duplicate Table enteries in CMN_CATALOG_RFRNC TABLE')
else:
    print('No enteries are found with duplicate table names in CMN_CATALOG_RFRNC TABLE')

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {data_quality_db}.SILVER.CMN_CATALOG_RFRNC for {SRC_SYS_NAME} {PROC_CTGRY_CD}. ")

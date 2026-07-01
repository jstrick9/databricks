# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will gives us the Input for Class ETL which will be used by successor notebook which has ETL code for class.ETL

# COMMAND ----------

# DBTITLE 1,Setting environment
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
#dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %run ./ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

# DBTITLE 1,Setting Config Param
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmintltm_catalog = common_configs['schema']['tmintltm_src_catalog']
# print(reporting_catalog)
# print(tmngpdb_catalog)
data_layer = "bronze"
schema_silver = "silver"
table_silver= "class"
print("printing envino veriable *******")
print(tmngpdb_catalog,tmintltm_catalog,data_layer)

# COMMAND ----------

ip1_query = f'''
select
  CLS.CLASS_NO AS CL_PRIME_CLS,
  CAST(split(CLS.FK_TRADEMARK_GID,':')[2] AS INTEGER) AS CL_SER_NUM ,
  CLS.CL_CLS_US_CT AS CL_CLS_US_CT,
  CLS.FK_TM_CLASS_STATUS_CD AS CL_CLS_STAT,
  RPAD(NVL(CLS.CL_CLS_US,CLS.CLASS_NO), 297,'ÿ') AS CL_CLS_US,
  CLS.STATUS_DT AS CL_DT_STAT,
  NVL(CLS.CL_FLG_ANOTH_FORM,0) AS CL_FLG_ANOTH_FORM,
  CLS.LAST_MOD_TS AS LAST_MODIFIED_DATE,
  AM.FILING_DT AS AM_DT_FIL,
  RI.NOTIFICATION_DT AS RI_NOTIF_DT

   from (SELECT SC.CLASS_NO , CL.FK_TRADEMARK_GID, CL.FK_TM_CLASS_STATUS_CD,
   case when NVL(CAST(date_format(CL.STATUS_DT ,'yyyyMMdd') AS INTEGER),0) = 10101 then null
   else CL.STATUS_DT end as STATUS_DT,
   CL.fk_class_id, CR.CL_CLS_US_CT, CL.LAST_MOD_TS, FLAG.CL_FLG_ANOTH_FORM,
   US.CL_CLS_US AS CL_CLS_US
   FROM {tmngpdb_catalog}.{data_layer}.TM_CLASS CL
   left join  {tmngpdb_catalog}.{data_layer}.stnd_class SC
   on CL.fk_class_id = SC.class_id
   LEFT JOIN (SELECT FK_TRADEMARK_GID, FK_CLASS_ID, COUNT(*) AS CL_CLS_US_CT 
   FROM {tmngpdb_catalog}.{data_layer}.TM_CLASS_REFERENCE
   GROUP BY FK_CLASS_ID,FK_TRADEMARK_GID)CR
   ON CL.fk_class_id = CR.FK_CLASS_ID
   AND CL.FK_TRADEMARK_GID = CR.FK_TRADEMARK_GID
   LEFT JOIN (Select DISTINCT FK_TRADEMARK_GID , FK_CLASS_ID , 1 AS CL_FLG_ANOTH_FORM 
   from {tmngpdb_catalog}.{data_layer}.USE_IN_ANOTHER_FORM) FLAG
   ON  CL.fk_class_id = FLAG.FK_CLASS_ID
   AND CL.FK_TRADEMARK_GID = FLAG.FK_TRADEMARK_GID
   LEFT JOIN (select  FK_TRADEMARK_GID  ,FK_CLASS_ID, 
   concat_ws('',sort_array(collect_list(case when SC.FK_CLASS_SCHEDULE_CD='CRT' then SC.CLASS_NO||' ' else SC.CLASS_NO end))) AS CL_CLS_US
              from {tmngpdb_catalog}.{data_layer}.TM_CLASS_REFERENCE
              INNER JOIN {tmngpdb_catalog}.{data_layer}.stnd_class SC
              ON FK_REFERENCED_CLASS_ID = SC.CLASS_ID
              WHERE SC.FK_CLASS_SCHEDULE_CD IN ('US','CRT')
              GROUP BY FK_TRADEMARK_GID  ,FK_CLASS_ID
              order by FK_CLASS_ID)US
    ON CL.fk_class_id = US.FK_CLASS_ID
   AND CL.FK_TRADEMARK_GID = US.FK_TRADEMARK_GID
   )cls
   RIGHT JOIN {tmngpdb_catalog}.{data_layer}.TRADEMARK AM
   ON AM.TRADEMARK_GID = CLS.FK_TRADEMARK_GID
   LEFT JOIN {tmintltm_catalog}.{data_layer}.INTERNATIONAL_REG_TM RI 
   ON AM.TRADEMARK_GID = RI.CFK_TRADEMARK_GID
   Where CLS.FK_TRADEMARK_GID Is Not Null'''

ip1_df= spark.sql(ip1_query)

# COMMAND ----------

ip2_query = f'''select
  'GS' || SCL.CLASS_NO ||CASE
    WHEN GDS_SRVCS_STMNT_TX LIKE '%((%' THEN 3
    ELSE 1
  END AS VT_TEXT_TYPE,
  GDS_SRVCS_STMNT_TX AS VT_TEXT,
  CAST(split(FK_TRADEMARK_GID, ':') [2] AS INTEGER) AS VT_SER_NUM,
  1 AS VT_ENT_NUM
from
  {tmngpdb_catalog}.{data_layer}.TM_CLASS CL
  INNER JOIN {tmngpdb_catalog}.{data_layer}.STND_CLASS SCL ON CL.FK_CLASS_ID = SCL.CLASS_ID'''

ip2_df = spark.sql(ip2_query)



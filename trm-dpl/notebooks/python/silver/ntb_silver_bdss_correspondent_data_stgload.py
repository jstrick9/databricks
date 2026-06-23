# Databricks notebook source
# MAGIC %md
# MAGIC ##Query7

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("rundate","")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

import pytz
from pytz import timezone
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

from datetime import date

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    #rdate = date.today()
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()
    #rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()- timedelta(days=1) # unit test

else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d')

print(rdate)
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
foreign_oracle_catalog = common_configs['schema']['foreign_oracle_catalog']
foreign_oracle_db = common_configs['schema']['src_db_name']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.dbx_env', dbx_env.lower())

if trgt_catalog.count("_") == 1:
    env = ""
else:
    env = "_"+trgt_catalog.split("_",2)[-1]

print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{dbx_env=},{env=}')
from pyspark.sql.functions import col, lit

# COMMAND ----------

job_name = 'ntb_silver_bdss_correspondent_data_stgload'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

# name_line_1_tx addr1,
# name_line_2_tx addr2,
# street_line_1_tx addr3,
# street_line_2_tx addr4,
# coalesce(city_nm, '') || ' ' || coalesce(geographic_region_cd, '') || ' ' || coalesce(postal_cd, '') addr5,

df_correspondent_stg = spark.sql(f"""
WITH base AS (
  SELECT
    CAST(split(TM_PARTY_ROLE.fk_trademark_gid, ':')[2] AS INTEGER) AS sernum,
    CASE
      WHEN name_line_1_tx IS NULL AND name_line_2_tx IS NULL THEN NULL
      WHEN name_line_1_tx IS NULL THEN name_line_2_tx
      WHEN name_line_2_tx IS NULL THEN name_line_1_tx
      WHEN name_line_1_tx IS NOT NULL AND name_line_2_tx IS NOT NULL THEN name_line_1_tx || ' ' || name_line_2_tx
    END AS addr1,
    street_line_1_tx AS addr2,
    street_line_2_tx AS addr3,
    coalesce(city_nm , '') || ', ' || coalesce(geographic_region_cd, '') || ' ' || coalesce(postal_cd, '') AS addr4,
    country_nm AS addr5,
    row_number() OVER (ORDER BY fk_trademark_gid) AS rs
  FROM {foreign_oracle_catalog}.{foreign_oracle_db}.MAILING_ADDRESS
  LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.tm_mailing_addr
  ON tm_mailing_addr.FK_MAILING_ADDRESS_GID = MAILING_ADDRESS.MAILING_ADDRESS_GID
  INNER JOIN
  (
    SELECT *
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PARTY_ROLE
    WHERE FK_TM_PARTY_ROLE_CD = 'COR'
  ) TM_PARTY_ROLE
  ON tm_mailing_addr.FK_TM_PARTY_ROLE_ID = TM_PARTY_ROLE.TM_PARTY_ROLE_ID
  WHERE TM_PARTY_ROLE.fk_trademark_gid IN 
  (
    SELECT DISTINCT concat('Trademark:0:', sernum)
    FROM {trgt_catalog}.silver.tmappl_daily_consolidated_vw
  )
)

SELECT sernum, address1, address2, address3, address4, address5
FROM
(
  SELECT sernum, rs, '' address1, '' address2, '' address3, '' address4, '' address5 FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION  all

  SELECT sernum, rs, addr5, '', '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr4, '', '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr4, addr5, '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr3, '', '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr3, addr5, '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr3, addr4, '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr3, addr4, addr5, '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr2, '', '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr2,  addr5, '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr2,  addr4, '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr2,  addr4, addr5, '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr2, addr3, '', '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr2, addr3, addr5, '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr2, addr3, addr4, '', '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr2, addr3, addr4, addr5, '' FROM base WHERE addr1 IS NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum , rs, addr1, '', '', '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr5, '', '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr4, '', '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr4,  addr5, '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr3, '', '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr3, addr5, '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr3, addr4, '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr3, addr4, addr5, '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr2, '', '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION 

  SELECT sernum, rs, addr1, addr2, addr5, '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr2, addr4, '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr2, addr4, addr5, '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr2, addr3, '', '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr2, addr3, addr5, '' FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NULL AND addr5 IS NOT NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr2, addr3, addr4, ''  FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NULL
  
  UNION all

  SELECT sernum, rs, addr1, addr2, addr3, addr4, addr5 FROM base WHERE addr1 IS NOT NULL AND addr2 IS NOT NULL AND addr3 IS NOT NULL AND addr4 IS NOT NULL AND addr5 IS NOT NULL
)
 """
)

# COMMAND ----------

try:
    df_correspondent_stg.write.mode("overwrite").format("delta").saveAsTable(f'{trgt_catalog}.silver.bdss_correspondent_data_daily_stg')
    recs_count = df_correspondent_stg.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into bdss_correspondent_data_daily_stg Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
dbutils.notebook.exit(f"Completed loading  bdss_correspondent_data_daily_stg Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  trm_tmngpdb_dev.silver.bdss_correspondent_data_daily_stg

# Databricks notebook source
# MAGIC %md
# MAGIC ##Query4

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

job_name = 'ntb_silver_bdss_vt_text_data_stgload'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

# decode(nvl(length(VT.vt_text), 0), 40, VT.vt_text, nvl(VT.vt_text || ' ', '')) AS vt_text,
# VT_LS AS (
#     SELECT
#       CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) VT_SER_NUM,
#       'LS' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE,
#       STATEMENT_TX VT_TEXT
#     FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT
#     WHERE FK_STATEMENT_TYPE_CD = 'LS' 
# ),

# UNION ALL
# SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_LS

df_vt_text_stg = spark.sql(f"""                     
 WITH VT_AOBOOR AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      (FK_REG_STMNT_TYPE_CD || '000' || SEQUENCE_NO) AS VT_TEXT_TYPE,
      DECODE(NVL(LENGTH(STATEMENT_TX), 0), 40, STATEMENT_TX, NVL(STATEMENT_TX || ' ', '') ) AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_REGISTRATION_STATEMENT
),

VT_AF AS (
    SELECT
    CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
    (
      'AF' ||
      case when b.CLASS_NO = 'A' 
      then 'A  '
      when b.CLASS_NO = 'B' 
      then 'B  '
      else trim(to_char(b.CLASS_NO, '000'))
      end
        || DECODE(a.FK_CLASS_STATEMENT_TYPE_CD, 'ANY01', '1', '2')
    ) AS VT_TEXT_TYPE,
    a.STATEMENT_TX AS VT_TEXT,
    b.CLASS_NO,
    a.FK_CLASS_STATEMENT_TYPE_CD
  FROM {foreign_oracle_catalog}.{foreign_oracle_db}.USE_IN_ANOTHER_FORM a
  INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.STND_CLASS b ON a.FK_CLASS_ID = b.CLASS_ID
),

VT_CU AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CU' || trim(to_char(STATEMENT_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.CONCURRENT_USE
),

VT_CS AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CS' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'CS'
),

VT_CC AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CC' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'CC'
),

VT_CD AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CD' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'CD'
),

VT_DM AS (
    SELECT 
      CAST(SERIAL_NUM_TX AS INTEGER) AS VT_SER_NUM,
      'DM0000' AS VT_TEXT_TYPE,
      CAST(SUBSTR(MARK_DESCRIPTION_TX, 1, 32767) AS STRING) AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TRADEMARK
    WHERE MARK_DESCRIPTION_TX IS NOT NULL
),

VT_DO AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'D0' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'D0' 
),

VT_D1 AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'DS' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'DS' 
),

VT_GS AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      ('GS' ||
      case when b.CLASS_NO = 'A'
      then 'A  '
      when b.CLASS_NO = 'B' 
      then 'B  '
      when b.CLASS_NO = 'NRN' 
      then 'NRN'
      else trim(to_char(b.CLASS_NO, '000'))
      end
      || '1') AS VT_TEXT_TYPE,
      CAST(a.GDS_SRVCS_STMNT_TX AS STRING) AS VT_TEXT,
      b.CLASS_NO
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_CLASS a
    INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.STND_CLASS b ON a.FK_CLASS_ID = b.CLASS_ID
    WHERE a.GDS_SRVCS_STMNT_TX IS NOT NULL
),

VT_IN AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'IN' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'IN' 
),

VT_NR AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'NR' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'NR' 
),

VT_PM AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'PM' || trim(to_char(SEQUENCE_NO, '0000')) AS VT_TEXT_TYPE,
      PSEUDO_MARK_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_PSEUDO_MARK
),

VT_TF AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TF0000' AS VT_TEXT_TYPE,
      LIMITATION_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.SECTION_2F_STATEMENT
    WHERE LIMITATION_TX IS NOT NULL
),

VT_TR AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TR' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'TR' 
),

VT_TL AS (
    SELECT 
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TL' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'TL' 
),

VT_TN AS (
    SELECT 
      CAST(regexp_substr(fk_parent_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TNSFOO' AS VT_TEXT_TYPE,
      regexp_substr(FK_RELATED_TRADEMARK_GID, '[^:]+$') AS VT_TEXT
    FROM {foreign_oracle_catalog}.{foreign_oracle_db}.TM_RELATIONSHIP
    WHERE FK_RELATIONSHIP_TYPE_CD = 'TNSF' 
)

SELECT 
    vt_text_type,
    decode(nvl(length(VT.vt_text), 0), 40, VT.vt_text, nvl(VT.vt_text, '')) AS vt_text,
    vt_ser_num AS sernum 
FROM
  (
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT FROM VT_AOBOOR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_AF
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_CU
    UNION ALL
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT FROM VT_CS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_CC
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_CD
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_DM
    UNION ALL
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT FROM VT_DO
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_D1
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_GS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_NR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_PM
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_TF
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_TR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_TL
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT FROM VT_TN
  ) VT
WHERE vt_ser_num IN (SELECT DISTINCT sernum FROM {trgt_catalog}.silver.tmappl_daily_consolidated_vw)
ORDER BY vt_text_type

  """
)

# COMMAND ----------

try:
    df_vt_text_stg.write.mode("overwrite").format("delta").saveAsTable(f'{trgt_catalog}.silver.bdss_vt_text_data_daily_stg')
    recs_count = df_vt_text_stg.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into bdss_vt_text_data_daily_stg Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
dbutils.notebook.exit(f"Completed loading  bdss_vt_text_data_daily_stg Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_tmngpdb_dev.silver.bdss_vt_text_data_daily_stg
# MAGIC

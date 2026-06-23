# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbutils.widgets.text("SRC_SYS_NAME", "", "SRC_SYS_NAME")
dbutils.widgets.text("rundate","")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
env_name = dbx_env.upper()
SRC_SYS_NAME = dbutils.widgets.get("SRC_SYS_NAME").rstrip()
src_name = SRC_SYS_NAME.lower()
config_file_name = src_name+"-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

import pytz
from pytz import timezone

print(f"{dbx_env=}")
print(f"{config_file=}")

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,define rundate
from datetime import date, timedelta

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date() - timedelta(days=1)
    rday = rdate.strftime("%A")
    rdate = rdate.strftime('%d-%b-%y')
else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d') - timedelta(days=1)
    rday = rdate.strftime("%A")
    rdate = rdate.strftime('%d-%b-%y')
    
print(f"{rday=}")
print(f"{rdate=}")
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

# DBTITLE 1,formatted_rundate for bdx daily job run
if rundate == '':
    formatted_rundate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date() - timedelta(days=1) 
    formatted_rundate = formatted_rundate.strftime('%d-%b-%Y')
else:
    formatted_rundate = rdate
    
print(f"{formatted_rundate=}")

# COMMAND ----------

common_configs = read_yaml(config_file)

trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()
trm_scope = common_configs['secrets']['part1_trm_scope']
ptas_scope = common_configs['secrets']['ptas_scope']
receiver_email = common_configs['data_quality']['tmapplser_quality_email_list']
bdx_api = common_configs['schema']['bdx_api']

emailid = receiver_email
env = dbx_env.upper()
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.trm_scope', trm_scope.lower()) 
spark.conf.set('config.ptas_scope', ptas_scope.lower())
spark.conf.set('config.dbx_env', dbx_env.lower())

if trgt_catalog.count("_") == 1:
    env = ""
else:
    env = "_"+trgt_catalog.split("_",2)[-1]

print(f"{src_db_name=}")
print(f"{trgt_catalog=}")
print(f"{data_quality_catalog=}")
print(f"{trm_scope=}")
print(f"{ptas_scope=}")
print(f"{env=}")
print(f"{bdx_api=}")

from pyspark.sql.functions import col, lit, from_utc_timestamp, current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${config.trgt_catalog}.silver.tmapplser_txn_tables
# MAGIC (
# MAGIC   actcd STRING COMMENT 'The action code associated with the load', 
# MAGIC   sernum STRING COMMENT 'The serial number of load',
# MAGIC   pulldt DATE COMMENT 'The date row was pulled from source tables',
# MAGIC   tabname STRING COMMENT 'The table_name were row was pulled from',
# MAGIC   create_ts TIMESTAMP COMMENT 'The date and time that the record is inserted in the database',
# MAGIC   create_user_id string COMMENT 'The User Identifier of the logged-on AIS User that initiated the insert of the record into the database',
# MAGIC   last_mod_ts TIMESTAMP COMMENT 'The date and time that the record was last modified in the database.Upon creation, this will be the same as the Create Timestamp',
# MAGIC   last_mod_user_id string COMMENT 'The User Identifier of the logged on User that initiated the last modification to the record in the database',
# MAGIC   lock_control_no INT COMMENT 'A Number used  to verify that the record being updated has not been altered since it was retrieved for update when optimistic locking is used.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://bdr-databricks-app-${config.dbx_env}/eds/delta_tables/${config.trgt_catalog}/silver/tmapplser_txn_tables'
# MAGIC TBLPROPERTIES (
# MAGIC   'databricks.delta.autocompact.enabled'= true,
# MAGIC   'delta.enableChangeDataFeed' = true,
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported'
# MAGIC );

# COMMAND ----------

job_name = 'ntb_silver_tmapplser_txn_tables_inc_load_txn_tables'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

# DBTITLE 1,Create function getactioncode
# MAGIC %md
# MAGIC from pyspark.sql.types import StringType
# MAGIC import pyspark
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC def getactioncode(serial_no):
# MAGIC     spark = SparkSession.builder.appName('getactioncode').getOrCreate()
# MAGIC     df = spark.sql(
# MAGIC         f"""
# MAGIC         select case when (select count(1) from {trgt_catalog}.silver.tmapplser where sernum = {serial_no}) = 0 then 'NA' else 'TX' end as stat
# MAGIC         """
# MAGIC     )
# MAGIC     row =  df.first()['stat']
# MAGIC     return row
# MAGIC spark.udf.register("getactioncode", getactioncode,StringType())

# COMMAND ----------

# DBTITLE 1,Create function getactioncode
# %sql
# CREATE or replace FUNCTION ${config.trgt_catalog}.silver.getactioncode(serial_no STRING)
#   RETURNS STRING
#   LANGUAGE SQL
#   RETURN select case when (select count(1) from ${config.trgt_catalog}.silver.tmapplser where sernum = serial_no) = 0 then 'NA' else 'TX' end as stat

# COMMAND ----------

trm_catalog = src_db_name

# COMMAND ----------

# DBTITLE 1,Check and Insert data "if rday=sat or monday"
if date.today().strftime("%A") != 'Monday':
  df_insert1_query = f"""
    SELECT
      TRIM(TO_CHAR(OG_CATG, '00')) AS actcd,
      SER_NUM,
      cast('{rdate}' AS DATE) + 1 AS pulldt,
      'og_h' AS tabname
    FROM (
      SELECT
        DISTINCT regexp_substr(fk_trademark_gid, '[^:]+$') AS SER_NUM,
        CAST(PS1.LEGACY_DES_CD AS INT) AS OG_CATG
      FROM {trm_catalog}.TM_PUBLICATION P1
        INNER JOIN {trm_catalog}.TM_PUBLICATION_SUBCT PS1
        ON P1.TM_PUBLICATION_GID = PS1.FK_TM_PUBLICATION_GID
        INNER JOIN {trm_catalog}.OG_PUBLICATION_TM OPT
        ON P1.TM_PUBLICATION_GID = OPT.FK_TM_PUBLICATION_GID
        INNER JOIN {trm_catalog}.OG_PUBLICATION OP 
        ON OP.OG_PUBLICATION_GID = OPT.FK_OG_PUBLICATION_GID
      WHERE
        CAST(PUBLICATION_DT AS DATE) = CAST('{rdate}' AS DATE) + 1
        AND legacy_og_status_cd = '055'
        AND CAST(PS1.LEGACY_DES_CD AS INT) <> 5
    )         
  """
else:
  df_insert1_query = f"""
    SELECT
      DISTINCT TRIM(TO_CHAR(PS1.LEGACY_DES_CD, '00')) AS actcd,
      regexp_substr(fk_trademark_gid, '[^:]+$') AS SER_NUM,
      CAST(PUBLICATION_DT AS DATE) AS pulldt,
      'og' AS tabname
    FROM {trm_catalog}.TM_PUBLICATION P1
      INNER JOIN {trm_catalog}.TM_PUBLICATION_SUBCT PS1
      ON P1.TM_PUBLICATION_GID = PS1.FK_TM_PUBLICATION_GID
      INNER JOIN {trm_catalog}.OG_PUBLICATION_TM OPT
      ON P1.TM_PUBLICATION_GID = OPT.FK_TM_PUBLICATION_GID
      INNER JOIN {trm_catalog}.OG_PUBLICATION OP 
      ON OP.OG_PUBLICATION_GID = OPT.FK_OG_PUBLICATION_GID
    WHERE
      CAST(PUBLICATION_DT AS DATE) = CAST('{rdate}' AS DATE) + 1
      AND legacy_og_status_cd = '055'
      AND cast(PS1.LEGACY_DES_CD as int) <> 5
  """

df_insert1 = read_data_from_oracle_conn_dsu_cmn(df_insert1_query, trm_scope)    
df_insert1.display()

# COMMAND ----------

# DBTITLE 1,Insert from am_h
df_insert2_query = f"""

  WITH tmm AS (
    SELECT
      DISTINCT fk_trademark_gid
    FROM {trm_catalog}.tm_milestone
    WHERE fk_tm_milestone_cd = 'REG'
      AND (
        milestone_dt IS NULL
        OR (
          CAST(milestone_dt AS DATE) < current_date
        )
      )
  ),

  th AS (
    SELECT
      DISTINCT trademark_gid,
      serial_num_tx,
      filing_dt
    FROM {trm_catalog}.trademark
    WHERE (
      trunc(last_mod_ts) = '{rdate}'
      OR trunc(create_ts) = '{rdate}'
    )
    AND legacy_status_cd <> 0
    AND (
      (
        legacy_status_cd = 630
        AND filing_dt IS NOT NULL
      )
      OR legacy_status_cd <> 630
    ) 
    AND (
      serial_num_tx < 60000000
      OR serial_num_tx > 69999999
    )
  )

  SELECT   
    DISTINCT th.serial_num_tx AS SER_NUM,
    CAST('{rdate}' AS DATE) AS pulldt,
    'am_h' AS tabname,
    CASE WHEN trunc(th.filing_dt) BETWEEN CAST('{rdate}' AS DATE) AND CAST('{rdate}' AS DATE) + 1
      THEN 1
      ELSE 0
    END AS AM_FLG_NEW_APPL   
  FROM th
    LEFT JOIN tmm 
    ON tmm.fk_trademark_gid = th.trademark_gid

"""

df_insert2 = read_data_from_oracle_conn_dsu_cmn(df_insert2_query, trm_scope)
df_insert2.createOrReplaceTempView("df_insert2_view")
df_insert2 = spark.sql(
  f"""
    SELECT
      DECODE(AM_FLG_NEW_APPL, 0, 'TX', {trgt_catalog}.silver.getactioncode(SER_NUM)) AS actcd,
      SER_NUM,
      pulldt,
      'am_h' AS tabname
    FROM df_insert2_view                
  """
)
df_insert2.display()

# COMMAND ----------

# DBTITLE 1,Insert from am_addr_h
df_insert3_query = f"""

  WITH ma AS (
    SELECT 
      DISTINCT MAILING_ADDRESS_GID
    FROM {trm_catalog}.MAILING_ADDRESS
    WHERE trunc(last_mod_ts) = '{rdate}'
      OR trunc(create_ts) = '{rdate}'
  ),

  tmm AS (
    SELECT
      DISTINCT fk_trademark_gid
    FROM {trm_catalog}.tm_milestone
    WHERE fk_tm_milestone_cd = 'REG'
      AND milestone_dt IS NULL
      OR CAST(milestone_dt AS DATE) < current_date
  ),

  tm AS (
    SELECT
      DISTINCT trademark_gid,
      serial_num_tx
    FROM {trm_catalog}.trademark
    WHERE legacy_status_cd <> 0
      AND (
        (
          legacy_status_cd = 630
          AND filing_dt IS NOT NULL
        )
        OR legacy_status_cd <> 630
      )
      AND (
        serial_num_tx < 60000000
        OR serial_num_tx > 69999999
      )
  ),

  tmpr AS (
    SELECT
      DISTINCT TM_PARTY_ROLE_ID,
      fk_trademark_gid
    FROM {trm_catalog}.TM_PARTY_ROLE
    WHERE FK_TM_PARTY_ROLE_CD = 'COR'
  )

  SELECT    
    DISTINCT 'TX' AS actcd,
    tm.serial_num_tx AS SER_NUM,
    CAST('{rdate}' AS DATE) AS pulldt,
    'am_addr_h' AS tabname
  FROM ma
    LEFT JOIN {trm_catalog}.tm_mailing_addr tmma
    ON tmma.FK_MAILING_ADDRESS_GID  = ma.MAILING_ADDRESS_GID
    INNER JOIN tmpr
    ON tmma.FK_TM_PARTY_ROLE_ID = tmpr.TM_PARTY_ROLE_ID
    INNER JOIN tm
    ON tmpr.fk_trademark_gid = tm.trademark_gid
    LEFT JOIN tmm
    ON tmm.fk_trademark_gid = tm.trademark_gid

"""

df_insert3 = read_data_from_oracle_conn_dsu_cmn(df_insert3_query, trm_scope)
df_insert3.display()

# COMMAND ----------

# DBTITLE 1,Insert from cl_h
df_insert4_query = f"""

  WITH tmm AS (
    SELECT
      DISTINCT fk_trademark_gid
    FROM {trm_catalog}.tm_milestone
    WHERE fk_tm_milestone_cd = 'REG'
      AND (
        milestone_dt IS NULL
        OR CAST(milestone_dt AS DATE) < current_date
      )
  ),

  th AS (
    SELECT
      DISTINCT trademark_gid
    FROM {trm_catalog}.trademark
    WHERE legacy_status_cd <> 0
    AND (
      (
        legacy_status_cd = 630
        AND filing_dt IS NOT NULL
      )
      OR legacy_status_cd <> 630
    ) 
    AND (
      serial_num_tx < 60000000
      OR serial_num_tx > 69999999
    )
  ),

  tc AS (
    SELECT
      DISTINCT fk_trademark_gid
    FROM {trm_catalog}.tm_class
    WHERE trunc(last_mod_ts) = '{rdate}'
      OR trunc(create_ts) = '{rdate}'
  )

  SELECT
    DISTINCT 'TX' AS actcd,
    regexp_substr(tc.fk_trademark_gid, '[^:]+$') AS SER_NUM,
    CAST('{rdate}' AS DATE) AS pulldt,
    'cl_h' AS tabname
  FROM tc
    INNER JOIN th
    ON tc.fk_trademark_gid = th.trademark_gid
    LEFT JOIN tmm
    ON tmm.fk_trademark_gid  = th.trademark_gid

"""

df_insert4 = read_data_from_oracle_conn_dsu_cmn(df_insert4_query, trm_scope)
df_insert4.display()

# COMMAND ----------

# DBTITLE 1,Insert from cm_h
df_insert5_query = f"""

   WITH tmm AS (
      SELECT
         DISTINCT fk_trademark_gid
      FROM {trm_catalog}.tm_milestone
      WHERE fk_tm_milestone_cd = 'REG'
         AND (
            milestone_dt IS NULL
            OR CAST(milestone_dt AS DATE) < current_date
         )
   ),

   th AS (
      SELECT
         DISTINCT trademark_gid
      FROM {trm_catalog}.trademark
      WHERE legacy_status_cd <> 0
      AND (
         (
            legacy_status_cd = 630
            AND filing_dt IS NOT NULL
         )
         OR legacy_status_cd <> 630
      ) 
      AND (
         serial_num_tx < 60000000
         OR serial_num_tx > 69999999
      )
   ),

   be AS (
      SELECT
         DISTINCT cfk_object_gid
      FROM {trm_catalog}.business_event
      WHERE trunc(last_mod_ts) = '{rdate}'
         OR trunc(create_ts) = '{rdate}'
   )

   SELECT
      DISTINCT 'TX' AS actcd,
      regexp_substr(be.cfk_object_gid, '[^:]+$') AS SER_NUM,
      CAST('{rdate}' AS DATE) AS pulldt,
      'cm_h' AS tabname
   FROM be
      INNER JOIN th
      ON be.cfk_object_gid = th.trademark_gid
      LEFT JOIN tmm
      ON tmm.fk_trademark_gid = th.trademark_gid
   
"""

df_insert5 = read_data_from_oracle_conn_dsu_cmn(df_insert5_query, trm_scope)
df_insert5.display()

# COMMAND ----------

# DBTITLE 1,Insert from fn_h
df_insert6_query = f"""

   WITH tmm AS (
      SELECT
         DISTINCT fk_trademark_gid
      FROM {trm_catalog}.tm_milestone
      WHERE fk_tm_milestone_cd = 'REG'
         AND (
            milestone_dt IS NULL
            OR CAST(milestone_dt AS DATE) < current_date
         )
   ),

   th AS (
      SELECT
         DISTINCT trademark_gid
      FROM {trm_catalog}.trademark
      WHERE legacy_status_cd <> 0
      AND (
         (
            legacy_status_cd = 630
            AND filing_dt IS NOT NULL
         )
         OR legacy_status_cd <> 630
      ) 
      AND (
         serial_num_tx < 60000000
         OR serial_num_tx > 69999999
      )
   ),

   fbh AS (
      SELECT
         DISTINCT fk_trademark_gid
      FROM {trm_catalog}.tm_foreign_basis
      WHERE trunc(last_mod_ts) = '{rdate}'
         OR trunc(create_ts) = '{rdate}'
   )

   SELECT
      DISTINCT 'TX' AS actcd,
      regexp_substr(fbh.fk_trademark_gid, '[^:]+$') AS SER_NUM,
      CAST('{rdate}' AS DATE) AS pulldt,
      'fn_h' AS tabname
   FROM fbh
      INNER JOIN th
      ON fbh.fk_trademark_gid = th.trademark_gid
      LEFT JOIN tmm
      ON tmm.fk_trademark_gid  = th.trademark_gid

"""

df_insert6 = read_data_from_oracle_conn_dsu_cmn(df_insert6_query, trm_scope)
df_insert6.display()

# COMMAND ----------

# DBTITLE 1,Insert from PR_H
df_insert7_query = f"""

   WITH tmm AS (
      SELECT
         DISTINCT fk_trademark_gid
      FROM {trm_catalog}.tm_milestone
      WHERE fk_tm_milestone_cd = 'REG'  
         AND (
            milestone_dt IS NULL
            OR CAST(milestone_dt AS DATE) < current_date
         )
   ),

   th AS (
      SELECT
         DISTINCT trademark_gid
      FROM {trm_catalog}.trademark
      WHERE legacy_status_cd <> 0
         AND (
            (
               legacy_status_cd = 630
               AND filing_dt IS NOT NULL
            )
            OR legacy_status_cd <> 630
         ) 
         AND (
            serial_num_tx < 60000000
            OR serial_num_tx > 69999999
         )
   ),

   pr AS (
      SELECT
         DISTINCT fk_trademark_gid,
         fk_prior_trademark_gid
      FROM {trm_catalog}.tm_prior_registration
      WHERE trunc(last_mod_ts) = '{rdate}'
         OR trunc(create_ts) = '{rdate}'
   )

   SELECT
      DISTINCT 'TX' AS actcd,
      regexp_substr(pr.fk_trademark_gid, '[^:]+$') AS SER_NUM,
      CAST('{rdate}' AS DATE) AS pulldt,
      'pr_h' AS tabname 
   FROM pr
      INNER JOIN th
      ON th.trademark_gid = pr.fk_prior_trademark_gid
      LEFT JOIN tmm
      ON tmm.fk_trademark_gid = th.trademark_gid

"""

df_insert7 = read_data_from_oracle_conn_dsu_cmn(df_insert7_query, trm_scope)
df_insert7.display()

# COMMAND ----------

# DBTITLE 1,PY_H
df_insert8_query = f"""

   WITH tmm AS (
      SELECT
         DISTINCT fk_trademark_gid
      FROM {trm_catalog}.tm_milestone
      WHERE fk_tm_milestone_cd = 'REG'
         AND milestone_dt IS NULL
         OR CAST(milestone_dt AS DATE) < current_date
   ),

   tm AS (
      SELECT
         DISTINCT trademark_gid,
         serial_num_tx
      FROM {trm_catalog}.trademark
      WHERE legacy_status_cd <> 0
      AND (
         (
            legacy_status_cd = 630
            AND filing_dt IS NOT NULL
         )
         OR legacy_status_cd <> 630
      )
      AND (
         serial_num_tx < 60000000
         OR serial_num_tx > 69999999
      )
   ),

   ip AS (
      SELECT
         DISTINCT INTERESTED_PARTY_GID
      FROM {trm_catalog}.INTERESTED_PARTY
      WHERE trunc(last_mod_ts) = '{rdate}'
         OR trunc(create_ts) = '{rdate}'
   )

   SELECT
      DISTINCT 'TX' AS actcd,
      regexp_substr(tmpr.fk_trademark_gid, '[^:]+$') AS SER_NUM,
      CAST('{rdate}' AS DATE) AS pulldt,
      'py_h' AS tabname
   FROM ip
      INNER JOIN {trm_catalog}.TM_PARTY_ROLE tmpr
      ON ip.INTERESTED_PARTY_GID = tmpr.FK_INTERESTED_PARTY_GID
      INNER JOIN tm
      ON tm.trademark_gid = tmpr.fk_trademark_gid
      LEFT JOIN tmm
      ON tmm.fk_trademark_gid = tm.trademark_gid

"""

df_insert8 = read_data_from_oracle_conn_dsu_cmn(df_insert8_query, trm_scope)
df_insert8.display()

# COMMAND ----------

df_insert9_query = f"""
  WITH VT_AOBOOR AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      (FK_REG_STMNT_TYPE_CD || '000' || SEQUENCE_NO) AS VT_TEXT_TYPE,
      DECODE(
        NVL(LENGTH(STATEMENT_TX), 0),
        40,
        STATEMENT_TX,
        NVL(STATEMENT_TX || ' ', '')
      ) AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_REGISTRATION_STATEMENT
  ),

  VT_AF AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      ('AF' ||
        CASE WHEN b.CLASS_NO = 'A' 
          THEN 'A  '
        WHEN b.CLASS_NO = 'B' 
          THEN 'B  '
          ELSE trim(to_char(b.CLASS_NO, '000'))
        END || DECODE(a.FK_CLASS_STATEMENT_TYPE_CD, 'ANY01', '1', '2')
      ) AS VT_TEXT_TYPE,
      a.STATEMENT_TX AS VT_TEXT,
      a.last_mod_ts,
      a.create_ts
    FROM {trm_catalog}.USE_IN_ANOTHER_FORM a
      INNER JOIN {trm_catalog}.STND_CLASS b
      ON a.FK_CLASS_ID = b.CLASS_ID
  ),

  VT_CU AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CU' || trim(to_char(STATEMENT_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.CONCURRENT_USE
  ),

  VT_CS AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CS' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'CS'
  ),

  VT_CC AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CC' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'CC'
  ),

  VT_CD AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'CD' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'CD'
  ),

  VT_DM AS (
    SELECT 
      CAST(A.SERIAL_NUM_TX AS INTEGER) AS VT_SER_NUM,
      'DM0000' AS VT_TEXT_TYPE,
      COALESCE(
        CAST(A.MARK_DESCRIPTION_TX AS VARCHAR(32767)),
        B.LITERAL_ELEMENT_TX,
      '') AS VT_TEXT,
      A.last_mod_ts,
      A.create_ts
    FROM {trm_catalog}.TRADEMARK A
      LEFT JOIN {trm_catalog}.TM_LITERAL B
      ON A.TRADEMARK_GID = B.FK_TRADEMARK_GID
  ),

  VT_DO AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'D0' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'D0' 
  ),

  VT_D1 AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'D1' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'D1' 
  ),

  VT_GS AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      ('GS' ||
        CASE WHEN b.CLASS_NO = 'A'
          THEN 'A  '
        WHEN b.CLASS_NO = 'B' 
          THEN 'B  '
        WHEN b.CLASS_NO = 'NRN' 
          THEN 'NRN'
          ELSE trim(to_char(b.CLASS_NO, '000'))
        END || '1'
      ) AS VT_TEXT_TYPE,
      CAST(a.GDS_SRVCS_STMNT_TX AS VARCHAR(32767)) AS VT_TEXT,
      a.last_mod_ts,
      a.create_ts
    FROM {trm_catalog}.TM_CLASS a
      INNER JOIN {trm_catalog}.STND_CLASS b
      ON a.FK_CLASS_ID = b.CLASS_ID
    WHERE a.GDS_SRVCS_STMNT_TX IS NOT NULL
  ),

  VT_IN AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'IN' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'IN' 
  ),

  VT_LS AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'LS' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'LS' 
  ),

  VT_NR AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'NR' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'NR' 
  ),

  VT_PM AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'PM' || trim(to_char(SEQUENCE_NO, '0000')) AS VT_TEXT_TYPE,
      PSEUDO_MARK_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_PSEUDO_MARK
  ),

  VT_TF AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TF0000' AS VT_TEXT_TYPE,
      LIMITATION_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.SECTION_2F_STATEMENT
    WHERE LIMITATION_TX IS NOT NULL
  ),

  VT_TR AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TR' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'TR' 
  ),

  VT_TL AS (
    SELECT
      CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TL' || trim(to_char(ORDER_NO, '0000')) AS VT_TEXT_TYPE,
      STATEMENT_TX AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT
    WHERE FK_STATEMENT_TYPE_CD = 'TL' 
  ),

  VT_TN AS (
    SELECT
      CAST(regexp_substr(fk_parent_trademark_gid, '[^:]+$') AS INTEGER) AS VT_SER_NUM,
      'TNSFOO' AS VT_TEXT_TYPE,
      regexp_substr(FK_RELATED_TRADEMARK_GID, '[^:]+$') AS VT_TEXT,
      last_mod_ts,
      create_ts
    FROM {trm_catalog}.TM_RELATIONSHIP
    WHERE FK_RELATIONSHIP_TYPE_CD = 'TNSF' 
  ),

  tmm AS (
    SELECT
      DISTINCT fk_trademark_gid
    FROM {trm_catalog}.tm_milestone
    WHERE fk_tm_milestone_cd = 'REG'
      AND (
        milestone_dt IS NULL
        OR CAST(milestone_dt AS DATE) < current_date
      )
  ),

  th AS (
    SELECT
      DISTINCT trademark_gid,
      serial_num_tx
    FROM {trm_catalog}.trademark
    WHERE legacy_status_cd <> 0
    AND (
      (
        legacy_status_cd = 630
        AND filing_dt IS NOT NULL
      )
      OR legacy_status_cd <> 630
    ) 
    AND (
      serial_num_tx < 60000000
      OR serial_num_tx > 69999999
    )
  )

  SELECT 
    DISTINCT 'TX' AS actcd,
    CAST(vt.VT_SER_NUM AS varchar(32767)) AS SER_NUM,
    CAST('{rdate}' AS DATE) AS pulldt,
    'vt_h' AS tabname
  FROM (
      SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_AOBOOR
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_AF
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_CU
      UNION ALL
      SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_CS
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_CC
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_CD
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_DM
      UNION ALL
      SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_DO
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_D1
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_GS
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_LS
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_NR
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_PM
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_TF
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT , last_mod_ts, create_ts FROM VT_TR
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_TL
      UNION ALL
      SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts, create_ts FROM VT_TN
  ) VT
    INNER JOIN th
    ON th.serial_num_tx = vt.VT_SER_NUM
    LEFT JOIN tmm
    ON tmm.fk_trademark_gid = th.trademark_gid
  WHERE trunc(vt.last_mod_ts) = '{rdate}'
    OR trunc(vt.create_ts) = '{rdate}'

"""

df_insert9 = read_data_from_oracle_conn_dsu_cmn(df_insert9_query, trm_scope)
df_insert9.display()

# COMMAND ----------

df_insert10_query = f"""

   WITH tmm AS (
      SELECT
         DISTINCT fk_trademark_gid
      FROM {trm_catalog}.tm_milestone
      WHERE fk_tm_milestone_cd = 'REG'
         AND (
            milestone_dt IS NULL
            OR CAST(milestone_dt AS DATE) < current_date
         )
   ),

   th AS (
      SELECT
         DISTINCT serial_num_tx,
         trademark_gid
      FROM {trm_catalog}.trademark
      WHERE legacy_status_cd <> 0
      AND (
         (
            legacy_status_cd = 630
            AND filing_dt IS NOT NULL
         )
         OR legacy_status_cd <> 630
      ) 
      AND (
         serial_num_tx < 60000000
         OR serial_num_tx > 69999999
      )
   ),

   ir AS (
      SELECT
         DISTINCT INTERNATIONAL_REG_GID
      FROM TMINTLTM.INTERNATIONAL_REGISTRATION
      WHERE trunc(last_mod_ts) = '{rdate}'
         OR trunc(create_ts) = '{rdate}'
   )

   SELECT
      DISTINCT 'TX' AS actcd,
      irt.DN_SERIAL_NUM AS SER_NUM,
      CAST('{rdate}' AS DATE) AS pulldt,
      'ri_h' AS tabname
   FROM ir
      INNER JOIN TMINTLTM.INTERNATIONAL_REG_TM irt
      ON irt.FK_INTERNATIONAL_REG_GID = ir.INTERNATIONAL_REG_GID
      INNER JOIN th
      ON th.serial_num_tx = irt.DN_SERIAL_NUM
      LEFT JOIN tmm
      ON tmm.fk_trademark_gid = th.trademark_gid

"""

df_insert10 = read_data_from_oracle_conn_dsu_cmn(df_insert10_query, trm_scope)
df_insert10.display()

# COMMAND ----------

df_insert11_query = f"""
   SELECT 
      DISTINCT 'TX' as actcd,
      regexp_substr(tde.fk_trademark_gid, '[^:]+$') AS SER_NUM,
      CAST('{rdate}' AS DATE) AS pulldt,
      'wp_h' AS tabname
   FROM {trm_catalog}.TM_DESIGN_ELEMENT tde
      INNER JOIN {trm_catalog}.trademark th
      ON th.trademark_gid= tde.fk_trademark_gid
      LEFT JOIN (
         SELECT *
         FROM {trm_catalog}.tm_milestone
         WHERE fk_tm_milestone_cd = 'REG'
      ) tmh
      ON tmh.fk_trademark_gid = th.trademark_gid
   WHERE (
         trunc(tde.last_mod_ts) = '{rdate}'
         OR trunc(tde.create_ts) = '{rdate}'
      )
      AND (
         milestone_dt IS NULL
         OR CAST(milestone_dt AS DATE) < current_date
      )
      AND th.legacy_status_cd <> 0
      AND (
         (
            th.legacy_status_cd = 630
            AND th.filing_dt IS NOT NULL
         )
         OR th.legacy_status_cd <> 630
      ) 
      AND (
         serial_num_tx < 60000000
         OR serial_num_tx > 69999999
      )
"""

df_insert11 = read_data_from_oracle_conn_dsu_cmn(df_insert11_query, trm_scope)
df_insert11.display()

# COMMAND ----------

df_insert12_query = f"""
  SELECT
    DISTINCT 'IB' AS actcd,
    regexp_substr(bah.cfk_trademark_gid, '[^:]+$') AS SER_NUM,
    CAST('{rdate}' AS DATE) AS pulldt,
    'mas_h' AS tabname
  FROM tmintltm.base_application bah 
    INNER JOIN {trm_catalog}.trademark th
    ON th.trademark_gid = bah.cfk_trademark_gid
    LEFT JOIN (
      SELECT *
      FROM {trm_catalog}.tm_milestone
      WHERE fk_tm_milestone_cd = 'REG'
    ) tmh
    ON tmh.fk_trademark_gid = th.trademark_gid
  WHERE (
      trunc(bah.last_mod_ts) = '{rdate}'
      OR trunc(bah.create_ts) = '{rdate}'
    )
    AND (
      milestone_dt IS NULL
      OR CAST(milestone_dt AS DATE) < current_date
    )
    AND th.legacy_status_cd <> 0
    AND (
      (
        th.legacy_status_cd = 630
        AND th.filing_dt IS NOT NULL
      )
      OR th.legacy_status_cd <> 630
    ) 
    AND (
      serial_num_tx < 60000000
      OR serial_num_tx > 69999999
    )
"""

df_insert12 = read_data_from_oracle_conn_dsu_cmn(df_insert12_query, trm_scope)
df_insert12.display()

# COMMAND ----------

df_insert13_query = f"""
  SELECT 
    DISTINCT 'IB' AS actcd,
    regexp_substr(bah.cfk_trademark_gid, '[^:]+$') AS SER_NUM,
    CAST('{rdate}' AS DATE) AS pulldt,
    'mhi_h' AS tabname
  FROM tmintltm.international_appl_event iae
    INNER JOIN tmintltm.international_appl_evnt_rsn iaer
    ON iae.international_appl_evnt_rsn_id = iaer.international_appl_evnt_rsn_id
    INNER JOIN tmintltm.base_application bah
    ON bah.FK_INTERNATIONAL_APPL_GID = iae.fk_international_appl_gid
    INNER JOIN {trm_catalog}.trademark th
    ON th.trademark_gid= bah.cfk_trademark_gid
    LEFT JOIN (
      SELECT *
      FROM {trm_catalog}.tm_milestone
      WHERE fk_tm_milestone_cd = 'REG'
    ) tmh
    ON tmh.fk_trademark_gid  = th.trademark_gid
  WHERE (
      trunc(iae.last_mod_ts) = '{rdate}'
      or trunc(iae.create_ts) = '{rdate}'
    )
    AND (
      milestone_dt IS NULL
      OR CAST(milestone_dt AS DATE) < current_date
    )
    AND th.legacy_status_cd <> 0
    AND (
      (
        th.legacy_status_cd = 630
        AND th.filing_dt IS NOT NULL
      )
      OR th.legacy_status_cd <> 630
    ) 
    AND (
      serial_num_tx < 60000000
      OR serial_num_tx > 69999999
    )
"""

df_insert13 = read_data_from_oracle_conn_dsu_cmn(df_insert13_query, trm_scope)
df_insert13.display()

# COMMAND ----------

try:
    df_union_tmapplser_inc_load = (
        df_insert1.union(df_insert2)
            .union(df_insert3)
            .union(df_insert4)
            .union(df_insert5)
            .union(df_insert6)
            .union(df_insert7)
            .union(df_insert8)
            .union(df_insert9)
            .union(df_insert10)
            .union(df_insert11)
            .union(df_insert12)
            .union(df_insert13)
    )
    df_union_tmapplser_inc_load = df_union_tmapplser_inc_load.dropDuplicates(['SER_NUM', 'pulldt'])

    df_union_tmapplser_inc_load = (
        df_union_tmapplser_inc_load.withColumn("create_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))
            .withColumn("create_user_id", lit("tmapplser"))
            .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))
            .withColumn("last_mod_user_id", lit("tmapplser"))
            .withColumn("lock_control_no", lit("0"))
    )
    df_union_tmapplser_inc_load.createOrReplaceTempView("temp_tmapplser_daily_load")
    print(f"Row count before merge -> {df_union_tmapplser_inc_load.count()}")

    df_tmapplser_inc_merge = spark.sql(
        f"""
            MERGE INTO {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES trgt
            USING temp_tmapplser_daily_load src
            ON trgt.SERNUM = src.SER_NUM
            AND trgt.pulldt = src.pulldt
            WHEN NOT MATCHED THEN INSERT(actcd,sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
            VALUES(actcd,SER_NUM,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
        """
    )     
    spark.sql(f"OPTIMIZE {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES ZORDER BY (SERNUM, pulldt)") 

    recs_count = df_tmapplser_inc_merge.select("num_inserted_rows").collect()[0][0]
    print(f"Rows inserted -> {recs_count}")
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Make modifications to view

# COMMAND ----------

spark.sql(f"""
  CREATE VIEW IF NOT EXISTS {trgt_catalog}.silver.tmappl_daily_txn_hstry_vw AS

  -- INTERSERCT A U B
  SELECT 
    SERNUM, 
    PULLDT, 
    TABLE_LOC,
    ACTCD_HX ACTCD,
    CASE WHEN  TABNAME_TXN == TABNAME_HX
      THEN TABNAME_HX
    ELSE TABNAME_HX || ',' || REPLACE(TABNAME_TXN,'_h')
    END TABNAME,
    CREATE_TS,
    CREATE_USER_ID,
    LAST_MOD_TS,
    LAST_MOD_USER_ID,
    LOCK_CONTROL_NO
  FROM (
    SELECT 
      SERNUM, 
      PULLDT, 
      TABLE_LOC,
      CREATE_TS,
      CREATE_USER_ID,
      LAST_MOD_TS,
      LAST_MOD_USER_ID,
      LOCK_CONTROL_NO,
      (
        SELECT MIN(ACTCD) 
        FROM {trgt_catalog}.silver.TMAPPLSER_HSTRY_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS ACTCD_HX, 
      (
        SELECT MIN(ACTCD) 
        FROM {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS ACTCD_TXN, 
      (
        SELECT MIN(TABNAME) 
        FROM {trgt_catalog}.silver.TMAPPLSER_HSTRY_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS TABNAME_HX,
      (
        SELECT MIN(TABNAME) 
        FROM {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS TABNAME_TXN
    FROM (
      SELECT SERNUM, PULLDT, 'both' AS TABLE_LOC, CREATE_TS, CREATE_USER_ID, LAST_MOD_TS, LAST_MOD_USER_ID, LOCK_CONTROL_NO
      FROM {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES
      INTERSECT
      SELECT SERNUM, PULLDT, 'both' AS TABLE_LOC, CREATE_TS, CREATE_USER_ID, LAST_MOD_TS, LAST_MOD_USER_ID, LOCK_CONTROL_NO
      FROM {trgt_catalog}.silver.TMAPPLSER_HSTRY_TABLES
    ) inside_both
  )

  UNION ALL

  -- A  N B
  SELECT
    SERNUM, 
    PULLDT, 
    TABLE_LOC,
    ACTCD_TXN ACTCD,
    TABNAME_TXN TABNAME,
    CREATE_TS,
    CREATE_USER_ID,
    LAST_MOD_TS,
    LAST_MOD_USER_ID,
    LOCK_CONTROL_NO
  FROM (
    SELECT 
      SERNUM, 
      PULLDT, 
      'tmapplser_txn_tables' AS TABLE_LOC,
      CREATE_TS,
      CREATE_USER_ID,
      LAST_MOD_TS,
      LAST_MOD_USER_ID,
      LOCK_CONTROL_NO,
      (
        SELECT MIN(ACTCD) 
        FROM {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS ACTCD_TXN,
      (
        SELECT MIN(TABNAME) 
        FROM {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS TABNAME_TXN
    FROM (
        SELECT SERNUM, PULLDT, CREATE_TS, CREATE_USER_ID, LAST_MOD_TS, LAST_MOD_USER_ID, LOCK_CONTROL_NO
        FROM {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES
        MINUS
        SELECT SERNUM, PULLDT, CREATE_TS, CREATE_USER_ID, LAST_MOD_TS, LAST_MOD_USER_ID, LOCK_CONTROL_NO
        FROM {trgt_catalog}.silver.TMAPPLSER_HSTRY_TABLES
    ) inside_both
  )

  UNION ALL

-- B N A

  SELECT 
    SERNUM, 
    PULLDT, 
    TABLE_LOC,
    ACTCD_HX ACTCD,
    TABNAME_HX TABNAME,
    CREATE_TS,
    CREATE_USER_ID,
    LAST_MOD_TS,
    LAST_MOD_USER_ID,
    LOCK_CONTROL_NO
  FROM (
    SELECT 
      SERNUM, 
      PULLDT, 
      'tmapplser_hstry_tables' AS TABLE_LOC,
      CREATE_TS,
      CREATE_USER_ID,
      LAST_MOD_TS,
      LAST_MOD_USER_ID,
      LOCK_CONTROL_NO,
      (
        SELECT MIN(ACTCD) 
        FROM {trgt_catalog}.silver.TMAPPLSER_HSTRY_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS ACTCD_HX, 
      (
        SELECT MIN(TABNAME) 
        FROM {trgt_catalog}.silver.TMAPPLSER_HSTRY_TABLES A 
        WHERE A.SERNUM = inside_both.SERNUM 
        AND inside_both.PULLDT = A.PULLDT
      ) AS TABNAME_HX
    FROM (
      SELECT SERNUM, PULLDT, CREATE_TS, CREATE_USER_ID, LAST_MOD_TS, LAST_MOD_USER_ID, LOCK_CONTROL_NO
      FROM  {trgt_catalog}.silver.TMAPPLSER_HSTRY_TABLES
      MINUS
      SELECT SERNUM, PULLDT, CREATE_TS, CREATE_USER_ID, LAST_MOD_TS, LAST_MOD_USER_ID, LOCK_CONTROL_NO
      FROM  {trgt_catalog}.silver.TMAPPLSER_TXN_TABLES
    ) inside_both
  )
""")

# COMMAND ----------

# DBTITLE 1,Send Email to Stakeholders and Exit Notebook
from datetime import date

Appdf= df_tmapplser_inc_merge
parms = {}
pd.set_option('display.max_colwidth', 0)
parms['INDEXED'] = Appdf.toPandas().to_html()
notify = Notify()
templ_str = f'{SRC_SYS_NAME} : BDSS Txn Tmapplser Data Load Result'
msg = notify.compose_email( templ_str, f'{SRC_SYS_NAME} bdss Load for {formatted_rundate} - databricks '+env, emailid, parms )
notify.send_mail(msg)

# COMMAND ----------

dbutils.notebook.exit(f"Completed loading TMAPPLSER TXN Table ")

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
print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,define rundate
from datetime import date

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    #rdate = date.today()
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()
    #rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()- timedelta(days=1) # unit test
    rday = rdate.strftime("%A")
else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d').date() 
    rday = rdate.strftime("%A")
print(rday)
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

# DBTITLE 1,formatted_rundate for bdx daily job run
if rundate == '':
    formatted_rundate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).strftime('%d-%b-%Y')
else:
    formatted_rundate = rdate.strftime('%d-%b-%Y')
formatted_rundate

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()
trm_scope = common_configs['secrets']['trm_scope']
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

print(f'{src_db_name=},{trgt_catalog=}, {data_quality_catalog=},{trm_scope=},{ptas_scope=},{dbx_env=},{env=},{bdx_api=}')
from pyspark.sql.functions import col, lit

# COMMAND ----------

job_name = 'ntb_silver_tmapplser_inc_load'

#control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,job_start_ts)
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

# DBTITLE 1,Check and Insert data "if rday=sat or monday"
# if rday == 'Saturday':
#     spark.sql(f""" 
#               DELETE FROM {trgt_catalog}.silver.TMAPPLSER
#               WHERE PULLDT < (CURRENT_DATE - 60 )
#               """)
# Removed 60 day delete per direction to keep historical values
#if rday == 'Monday':
if date.today().strftime("%A") != 'Monday':
        df_insert1 = spark.sql(
            f"""
             --INSERT  INTO {trgt_catalog}.silver.TMAPPLSER (actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
            SELECT
              TRIM(TO_CHAR(OG_CATG, '00')) as actcd,
              SER_NUM,
              cast('{rdate}' as date) + 1 as pulldt,
              'og_h' as tabname,
              from_utc_timestamp(current_timestamp(),'America/New_York')as create_ts,'tmapplser' as cuser,
              from_utc_timestamp(current_timestamp(),'America/New_York')as last_mod_ts,'tmapplser' as luser,0 as lcno
            FROM
              (
                SELECT
                  DISTINCT 
                       regexp_substr(fk_trademark_gid, '[^:]+$') SER_NUM,
                       cast(PS1.LEGACY_DES_CD as int) OG_CATG
                  FROM
                    {trgt_catalog}.bronze.TM_PUBLICATION_H P1
                    INNER JOIN {trgt_catalog}.bronze.TM_PUBLICATION_SUBCT_H PS1
                    ON P1.TM_PUBLICATION_GID = PS1.FK_TM_PUBLICATION_GID
                    INNER JOIN {trgt_catalog}.bronze.OG_PUBLICATION_TM_H OPT
                    ON P1.TM_PUBLICATION_GID = OPT.FK_TM_PUBLICATION_GID
                    INNER JOIN {trgt_catalog}.bronze.OG_PUBLICATION_H OP 
                    ON OP.OG_PUBLICATION_GID = OPT.FK_OG_PUBLICATION_GID
                        WHERE
                  cast(PUBLICATION_DT as date) = cast('{rdate}' as date) + 1
                  AND legacy_og_status_cd = '055'
                  AND cast(PS1.LEGACY_DES_CD as int) <> 5
                --ORDER BY
                  --fk_trademark_gid
              )         
            """
        )
    
else:
        df_insert1 = spark.sql(f"""
          --INSERT  INTO {trgt_catalog}.silver.TMAPPLSER ( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
            SELECT
              TRIM(TO_CHAR(PS1.LEGACY_DES_CD, '00')) as actcd,
             regexp_substr(fk_trademark_gid, '[^:]+$') SER_NUM,
              cast(PUBLICATION_DT as date) as pulldt,
              'og' as tabname,from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,
              from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'tmapplser' as luser,0 as lcno
              FROM
                    {trgt_catalog}.bronze.TM_PUBLICATION P1
                    INNER JOIN {trgt_catalog}.bronze.TM_PUBLICATION_SUBCT PS1
                    ON P1.TM_PUBLICATION_GID = PS1.FK_TM_PUBLICATION_GID
                    INNER JOIN {trgt_catalog}.bronze.OG_PUBLICATION_TM OPT
                    ON P1.TM_PUBLICATION_GID = OPT.FK_TM_PUBLICATION_GID
                    INNER JOIN {trgt_catalog}.bronze.OG_PUBLICATION OP 
                    ON OP.OG_PUBLICATION_GID = OPT.FK_OG_PUBLICATION_GID
            WHERE
              cast(PUBLICATION_DT as date) = cast('{rdate}' as date) + 1
              AND legacy_og_status_cd = '055'
              AND cast(PS1.LEGACY_DES_CD as int) <> 5
           --ORDER BY
                  --fk_trademark_gid
                  
                  
                  """)
df_insert1.display()

# COMMAND ----------

# DBTITLE 1,Insert from am_h
df_insert2 = spark.sql(f"""
with mx_order as (
select be.cfk_object_gid, max(be.order_no) order_no from {trgt_catalog}.bronze.business_event be inner join {trgt_catalog}.bronze.stnd_business_event_reason sbe on be.fk_business_event_reason_id = sbe.business_event_reason_id group by be.cfk_object_gid
),
business_data as (
select mo.cfk_object_gid, case when be.fk_business_event_reason_id in (655, 656) then 1 else 0  end as AM_FLG_NEW_APPL
from  {trgt_catalog}.bronze.business_event be inner join mx_order mo on be.cfk_object_gid = mo.cfk_object_gid and be.order_no = mo.order_no
)
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER (actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)

select   DECODE(AM_FLG_NEW_APPL, 0, 'TX', {trgt_catalog}.silver.getactioncode(serial_num_tx)) as actcd,
         --  regexp_substr(fk_trademark_gid, '[^:]+$') 
          serial_num_tx   SER_NUM,
          '{rdate}' as pulldt,
          'am_h' as tabname,from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'tmapplser' as luser,0 as lcno
from business_data  bd inner join  {trgt_catalog}.bronze.trademark_h  th on  bd.cfk_object_gid = th.trademark_gid
                                left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    (th.last_mod_ts  ) >=  (cast('{rdate}' as timestamp)  - INTERVAL 5 HOURS) 
          AND (th.last_mod_ts ) < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
      AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert2.display()

# COMMAND ----------

# DBTITLE 1,Insert from am_addr_h
df_insert3 = spark.sql(f"""
with
  b as  (
  select  mailing_address_gid , last_mod_ts from {trgt_catalog}.BRONZE.MAILING_ADDRESS_H
)
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER (actcd,sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
select    
           DISTINCT 'TX' as actcd,
          serial_num_tx SER_NUM,
         '{rdate}' as pulldt ,
          'am_addr_h' as tabname,
          from_utc_timestamp(current_timestamp(),'America/New_York')as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York')as last_mod_ts,'tmapplser' as luser,0 as lcno
            from b 
LEFT JOIN {trgt_catalog}.BRONZE.tm_mailing_addr ON   tm_mailing_addr.FK_MAILING_ADDRESS_GID  = b.MAILING_ADDRESS_GID
    INNER JOIN  (SELECT * FROM {trgt_catalog}.BRONZE.TM_PARTY_ROLE WHERE FK_TM_PARTY_ROLE_CD = 'COR') TM_PARTY_ROLE  ON tm_mailing_addr.FK_TM_PARTY_ROLE_ID  = TM_PARTY_ROLE.TM_PARTY_ROLE_ID
    inner join  {trgt_catalog}.bronze.trademark_h  th on  TM_PARTY_ROLE.fk_trademark_gid = th.trademark_gid
                                left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
     (b.last_mod_ts  ) >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS)
          AND (b.last_mod_ts ) < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
""")
df_insert3.display()

# COMMAND ----------

# DBTITLE 1,Insert from cl_h
df_insert4 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER (actcd,sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
SELECT
          DISTINCT 'TX' as actcd,
         regexp_substr(tc.fk_trademark_gid, '[^:]+$')   SER_NUM,
          '{rdate}' pulldt,
          'cl_h' as tabname,
          from_utc_timestamp(current_timestamp(),'America/New_York')as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York')as last_mod_ts,'tmapplser' as luser,0 as lcno
        FROM
            {trgt_catalog}.bronze.tm_class_h tc inner join  {trgt_catalog}.bronze.trademark_h  th on  tc.fk_trademark_gid = th.trademark_gid
                                left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    (tc.last_mod_ts  ) >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS)
          AND cast(tc.last_mod_ts as date) < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert4.display()

# COMMAND ----------

# DBTITLE 1,Insert from cm_h
df_insert5 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER (actcd,sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
SELECT
          DISTINCT 'TX' as actcd,
          regexp_substr(be.cfk_object_gid, '[^:]+$')  SER_NUM,
          '{rdate}' as pulldt,
          'cm_h' as tabname,
          from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'tmapplser' as luser,0 as lcno
         FROM
            {trgt_catalog}.bronze.business_event be inner join  {trgt_catalog}.bronze.trademark_h  th on  be.cfk_object_gid = th.trademark_gid
                                left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    be.last_mod_ts   >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS) 
          AND be.last_mod_ts  < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
    """)
df_insert5.display()

# COMMAND ----------

# DBTITLE 1,Insert from fn_h
df_insert6 = spark.sql(f"""
 --INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
 SELECT
          DISTINCT 'TX' as actcd,
          regexp_substr(fbh.fk_trademark_gid, '[^:]+$')   SER_NUM,
           '{rdate}' as pulldt,
          'fn_h' as tabname,
          from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York')as last_mod_ts,'tmapplser' as luser,0 as lcno
 FROM
            {trgt_catalog}.bronze.tm_foreign_basis_h fbh inner join  {trgt_catalog}.bronze.trademark_h  th on  fbh.fk_trademark_gid = th.trademark_gid
                                left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    fbh.last_mod_ts  >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS)  
          AND fbh.last_mod_ts  < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
         """)
df_insert6.display()

# COMMAND ----------

# DBTITLE 1,Insert from PR_H
df_insert7 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
select
  DISTINCT 'TX' as actcd,
          regexp_substr(pr.fk_trademark_gid, '[^:]+$')  SER_NUM,
           '{rdate}' pulldt,
          'pr_h' as tabname,
          from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York')as last_mod_ts,'tmapplser' as luser,0 as lcno
from {trgt_catalog}.bronze.tm_prior_registration_h pr
inner join {trgt_catalog}.bronze.trademark_h th on th.trademark_gid =pr.fk_prior_trademark_gid
left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    pr.last_mod_ts  >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS)  
          AND pr.last_mod_ts  < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert7.display()

# COMMAND ----------

# DBTITLE 1,PY_H
df_insert8 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
SELECT      DISTINCT 'TX' as actcd,
          regexp_substr(TM_PARTY_ROLE_H.fk_trademark_gid, '[^:]+$')   SER_NUM,
          '{rdate}' pulldt,
          'py_h' as tabname,
          from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'tmapplser' as luser,0 as lcno
FROM {trgt_catalog}.BRONZE.INTERESTED_PARTY_H INNER JOIN {trgt_catalog}.BRONZE.TM_PARTY_ROLE_H ON INTERESTED_PARTY_H.INTERESTED_PARTY_GID = TM_PARTY_ROLE_H.FK_INTERESTED_PARTY_GID
           inner join {trgt_catalog}.bronze.trademark_h th on th.trademark_gid =TM_PARTY_ROLE_H .fk_trademark_gid
left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    cast(INTERESTED_PARTY_H.last_mod_ts as date) >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS)  
          AND cast(INTERESTED_PARTY_H.last_mod_ts as date) < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert8.display()

# COMMAND ----------

df_insert9 = spark.sql(f"""
WITH VT_AOBOOR AS (
    SELECT
    CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM,
    (FK_REG_STMNT_TYPE_CD
     || '000'
     || SEQUENCE_NO)                                                VT_TEXT_TYPE,
    DECODE( NVL(LENGTH(STATEMENT_TX), 0), 40, STATEMENT_TX, NVL(STATEMENT_TX
                                                      || ' ', '') ) VT_TEXT, last_mod_ts FROM {trgt_catalog}.BRONZE.TM_REGISTRATION_STATEMENT_H
),

VT_AF AS (
    SELECT
  CAST(
    regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER
  ) VT_SER_NUM,
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
  a.STATEMENT_TX VT_TEXT
 , a.last_mod_ts
FROM
  {trgt_catalog}.BRONZE.USE_IN_ANOTHER_FORM_H a
  INNER JOIN {trgt_catalog}.BRONZE.STND_CLASS b ON a.FK_CLASS_ID = b.CLASS_ID
),
VT_CU AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM,
    'CU' || trim(to_char(STATEMENT_NO, '0000')) VT_TEXT_TYPE,
    STATEMENT_TX VT_TEXT, last_mod_ts

    FROM {trgt_catalog}.BRONZE.CONCURRENT_USE_H
),
VT_CS AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'CS' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'CS'
),
VT_CC AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'CC' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'CC'
)
,
VT_CD AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'CD' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'CD'
),
VT_DM AS (
    SELECT  CAST(A.SERIAL_NUM_TX AS INTEGER) VT_SER_NUM, 'DM0000' VT_TEXT_TYPE , COALESCE(CAST(A.MARK_DESCRIPTION_TX AS STRING),B.LITERAL_ELEMENT_TX, '')  AS VT_TEXT, A.last_mod_ts
    FROM {trgt_catalog}.BRONZE.TRADEMARK_H A LEFT JOIN  {trgt_catalog}.BRONZE.TM_LITERAL_H B ON A.TRADEMARK_GID = B.FK_TRADEMARK_GID
),
VT_DO AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'D0' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'D0' 
),
VT_D1 AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'D1' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'DS' 
),
VT_GS AS (
  
    SELECT
  CAST(
    regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER
  ) VT_SER_NUM,
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
  CAST(a.GDS_SRVCS_STMNT_TX AS STRING) VT_TEXT
  , a.last_mod_ts
FROM
  {trgt_catalog}.BRONZE.TM_CLASS_H a
  INNER JOIN {trgt_catalog}.BRONZE.STND_CLASS b ON a.FK_CLASS_ID = b.CLASS_ID
WHERE a.GDS_SRVCS_STMNT_TX IS NOT NULL


),
VT_IN AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'IN' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'IN' 


)
,
VT_LS AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'LS' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'LS' 


)
,
VT_NR AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'NR' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'NR' 


),

VT_PM AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'PM' || trim(to_char(SEQUENCE_NO, '0000')) VT_TEXT_TYPE, PSEUDO_MARK_TX VT_TEXT, last_mod_ts
    FROM {trgt_catalog}.BRONZE.TM_PSEUDO_MARK_H
),
VT_TF AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TF0000' VT_TEXT_TYPE, LIMITATION_TX VT_TEXT, last_mod_ts
    FROM {trgt_catalog}.BRONZE.SECTION_2F_STATEMENT_H
    WHERE LIMITATION_TX IS NOT NULL
),
VT_TR AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TR' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'TR' 


),
VT_TL AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TL' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts
  

    FROM {trgt_catalog}.BRONZE.TM_ADDITIONAL_STATEMENT_H

    WHERE FK_STATEMENT_TYPE_CD = 'TL' 


),
VT_TN AS (
  
    SELECT CAST(regexp_substr(fk_parent_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TNSFOO' VT_TEXT_TYPE,  regexp_substr(FK_RELATED_TRADEMARK_GID, '[^:]+$')  VT_TEXT, last_mod_ts

    FROM {trgt_catalog}.BRONZE.TM_RELATIONSHIP_H

    WHERE FK_RELATIONSHIP_TYPE_CD = 'TNSF' 


)

--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
SELECT 
 DISTINCT 'TX'as actcd,
          cast(VT_SER_NUM as string) SER_NUM ,
          '{rdate}' pulldt,
          'vt_h' as tabname,
          from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'tmapplser' as luser ,0 as lcno
FROM
  (
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts FROM VT_AOBOOR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_AF
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_CU
    UNION ALL
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_CS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts FROM VT_CC
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_CD
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_DM
    UNION ALL
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_DO
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_D1
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts FROM VT_GS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_LS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_NR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_PM
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts FROM VT_TF
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT , last_mod_ts FROM VT_TR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts  FROM VT_TL
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts FROM VT_TN
  

  ) VT
  inner join {trgt_catalog}.bronze.trademark_h th on th.serial_num_tx = vt.VT_SER_NUM
left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    VT.last_mod_ts  >= (cast('{rdate}' as timestamp)- INTERVAL 5 HOURS)  
          AND VT.last_mod_ts    < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)

        """)
df_insert9.display()

# COMMAND ----------

df_insert10 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
select   DISTINCT 'TX' as actcd,
          IRT.DN_SERIAL_NUM SER_NUM,
          '{rdate}' pulldt,
          'ri_h' as tabname,from_utc_timestamp(current_timestamp(),'America/New_York')as create_ts,'tmapplser' as cuser,from_utc_timestamp(current_timestamp(),'America/New_York')as last_updt_ts,'tmapplser' as luser,0 as lcno
           from 
TRM_TMINTLTM{env}.BRONZE.INTERNATIONAL_REGISTRATION_H RI inner join TRM_TMINTLTM{env}.BRONZE.INTERNATIONAL_REG_TM_H IRT ON IRT.FK_INTERNATIONAL_REG_GID = RI.INTERNATIONAL_REG_GID
 inner join {trgt_catalog}.bronze.trademark_h th on th.serial_num_tx = IRT.DN_SERIAL_NUM
left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    RI.last_mod_ts >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS)  
          AND RI.last_mod_ts < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert10.display()

# COMMAND ----------

df_insert11 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
SELECT 
      DISTINCT 'TX' as actcd,
         regexp_substr(tde.fk_trademark_gid, '[^:]+$')   SER_NUM,
         '{rdate}'  pulldt,
          'wp_h' as tabname,from_utc_timestamp(current_timestamp(),'America/New_York')as create_ts,'tmapplser' as cuser,
          from_utc_timestamp(current_timestamp(),'America/New_York')as last_mod_ts,'tmapplser' as luser,0 as lcno
FROM
  {trgt_catalog}.bronze.TM_DESIGN_ELEMENT tde inner join {trgt_catalog}.bronze.trademark_h th on th.trademark_gid= tde.fk_trademark_gid
left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
where   
    tde.last_mod_ts  >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS) 
          AND tde.last_mod_ts  < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert11.display()

# COMMAND ----------

df_insert12 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
select
  DISTINCT 'IB' as actcd,
        regexp_substr( bah.cfk_trademark_gid, '[^:]+$')  SER_NUM,
        '{rdate}' pulldt,
          'mas_h' as tabname,from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'tmapplser' as luser,0 as lcno
FROM
  trm_tmintltm{env}.bronze.base_application_h bah  inner join {trgt_catalog}.bronze.trademark_h th on th.trademark_gid= bah.cfk_trademark_gid
left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
where   
      bah.last_mod_ts  >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS) 
          AND bah.last_mod_ts  < (cast('{rdate}' as timestamp) + (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert12.display()


# COMMAND ----------

df_insert13 = spark.sql(f"""
--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
select 
    DISTINCT 'IB' as actcd,
           regexp_substr( bah.cfk_trademark_gid, '[^:]+$') SER_NUM,
         '{rdate}' pulldt,
          'mhi_h' as tabname,from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'tmapplser' as cuser,from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'tmapplser' as luser,0 as lcno

from  trm_tmintltm{env}.bronze.international_appl_event iae inner join  trm_tmintltm{env}.bronze.international_appl_evnt_rsn iaer on iae.international_appl_evnt_rsn_id = iaer.international_appl_evnt_rsn_id
      inner join  trm_tmintltm{env}.bronze.base_application_h bah on bah.FK_INTERNATIONAL_APPL_GID = iae.fk_international_appl_gid
       inner join {trgt_catalog}.bronze.trademark_h th on th.trademark_gid= bah.cfk_trademark_gid
left join  (select * from {trgt_catalog}.bronze.tm_milestone_h where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
where   
    iae.last_mod_ts  >= (cast('{rdate}' as timestamp) - INTERVAL 5 HOURS) 
          AND iae.last_mod_ts  < (cast('{rdate}' as timestamp)+ (INTERVAL 1 DAY - INTERVAL 5 HOURS))
       AND (milestone_dt IS NULL
          OR CAST(milestone_dt AS DATE) < current_date())
          AND th.legacy_status_cd <> 0
          AND ((th.legacy_status_cd = 630
          AND th.filing_dt IS NOT NULL)
          OR th.legacy_status_cd <>630) 
           AND (serial_num_tx < 60000000
          OR serial_num_tx > 69999999)
          """)
df_insert13.display()

# COMMAND ----------

try:
    df_union_tmapplser_inc_load = df_insert1.union(df_insert2).union(df_insert3).union(df_insert4).union(df_insert5).union(df_insert6).union(df_insert7).union(df_insert8).union(df_insert9).union(df_insert10).union(df_insert11).union(df_insert12).union(df_insert13)
    df_union_tmapplser_inc_load = df_union_tmapplser_inc_load.dropDuplicates(['SER_NUM', 'pulldt'])
    df_union_tmapplser_inc_load.createOrReplaceTempView("temp_tmapplser_daily_load")

    df_tmapplser_inc_merge = spark.sql(f"""MERGE INTO {trgt_catalog}.silver.TMAPPLSER trgt
                                   USING temp_tmapplser_daily_load src
                                   ON trgt.SERNUM = src.SER_NUM
                                   and trgt.pulldt = src.pulldt
                                   WHEN NOT MATCHED THEN INSERT(actcd,sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
                                    VALUES(actcd,SER_NUM,pulldt,tabname,create_ts,cuser,last_mod_ts,luser,lcno  )
                                """)        
    recs_count = df_tmapplser_inc_merge.select("num_inserted_rows").collect()[0][0]
    print(recs_count)
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    #dbutils.fs.rm(CHK_POINT_DIR,True)
    raise
                       

# COMMAND ----------

# DBTITLE 1,Send Email to Stakeholders and Exit Notebook
from datetime import date
Appdf= df_tmapplser_inc_merge
parms = {}
pd.set_option('display.max_colwidth', 0)
parms['INDEXED']=Appdf.toPandas().to_html()
notify = Notify()
templ_str = f'{SRC_SYS_NAME} : BDSS Tmapplser Data Load Result'
msg = notify.compose_email( templ_str, f'{SRC_SYS_NAME} bdss Load for {formatted_rundate} - databricks '+env, emailid, parms )
notify.send_mail(msg)
#dbutils.notebook.exit(f"Completed loading TMAPPLSER Table ")

# COMMAND ----------

# import requests

# url = f"{bdx_api}{formatted_rundate}"

# headers = {
#     'Content-Type': 'application/json',
#     'Accept': 'application/json'
# }
# if recs_count>0:
#     try:
#         response = requests.post(url, headers=headers, timeout=60)  # Timeout set to 60 seconds
#         # To display the response content
#         print(response.text)
#         #print(url)
#     except requests.exceptions.ConnectTimeout:
#         print("The request timed out. Please try again later or increase the timeout.")
# else:
#     print("BDX Daily TMAPPL XML job not executed as recs_count = 0.")

# COMMAND ----------

dbutils.notebook.exit(f"Completed loading TMAPPLSER Table ")

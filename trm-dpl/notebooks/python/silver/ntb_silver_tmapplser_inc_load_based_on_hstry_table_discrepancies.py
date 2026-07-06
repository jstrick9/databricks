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

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,define rundate
from datetime import date, timedelta

rundate = dbutils.widgets.get("rundate")
if rundate == '':
    #rdate = date.today()
    rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date() - timedelta(days=1)
    #rdate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date()- timedelta(days=1) # unit test
    rday = rdate.strftime("%A")
    rdate = rdate.strftime('%d-%b-%y')
else:
    rdate = rundate
    import datetime
    rdate = datetime.datetime.strptime(rundate, '%Y-%m-%d') - timedelta(days=1)
    rday = rdate.strftime("%A")
    rdate = rdate.strftime('%d-%b-%y')
    
print(rday, rdate)
spark.conf.set('conf.rdate', str(rdate))

# COMMAND ----------

# DBTITLE 1,formatted_rundate for bdx daily job run
if rundate == '':
    formatted_rundate = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).date() - timedelta(days=1) 
    formatted_rundate = formatted_rundate.strftime('%d-%b-%Y')
else:
    formatted_rundate = rdate
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

from pyspark.sql.functions import from_utc_timestamp, current_timestamp, lit


# COMMAND ----------

job_name = 'ntb_silver_tmapplser_inc_load_hstry_discrepancies'
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

#trm_catalog = 'TMNGPDB'
trm_catalog = src_db_name


# COMMAND ----------

df_missing_apps = spark.sql(
  f"""
    SELECT DISTINCT trademark_gid
    FROM {trgt_catalog}.silver.temp_summary_daily_event_pull
    WHERE
      event_dt = to_date('{rdate}',  'dd-MMM-yy')
      AND serial_num_tx NOT IN
      (
          SELECT sernum  
          FROM {trgt_catalog}.silver.tmappl_daily_txn_hstry_vw 
          WHERE pulldt IN (to_date('{rdate}',  'dd-MMM-yy'), date_sub(to_date('{rdate}',  'dd-MMM-yy'), 1))
      )
  """
)

# COMMAND ----------

if df_missing_apps.count() == 0:
    dbutils.notebook.exit("No missing apps found, exiting notebook.")

# COMMAND ----------

display(df_missing_apps)

# COMMAND ----------

from pyspark.sql.functions import collect_list, lit, concat_ws

trademark_gid_list = df_missing_apps.select(collect_list("trademark_gid")).first()[0]
trademark_gid_str = f""" IN ({','.join([f"(0, '{str(gid)}')" for gid in trademark_gid_list])})"""
display(trademark_gid_str)

# COMMAND ----------

# DBTITLE 1,Check and Insert data "if rday=sat or monday"
if date.today().strftime("%A") != 'Monday':
        df_insert1_query =  \
            f"""
            SELECT
              TRIM(TO_CHAR(OG_CATG, '00')) as actcd,
              SER_NUM,
              cast('{rdate}' as date) + 1 as pulldt,
              'og_h' as tabname
            FROM
              (
                SELECT
                  DISTINCT 
                       regexp_substr(fk_trademark_gid, '[^:]+$') SER_NUM,
                       cast(PS1.LEGACY_DES_CD as int) OG_CATG
                  FROM
                    {trm_catalog}.TM_PUBLICATION_H P1
                    INNER JOIN {trm_catalog}.TM_PUBLICATION_SUBCT_H PS1
                    ON P1.TM_PUBLICATION_GID = PS1.FK_TM_PUBLICATION_GID
                    INNER JOIN {trm_catalog}.OG_PUBLICATION_TM_H OPT
                    ON P1.TM_PUBLICATION_GID = OPT.FK_TM_PUBLICATION_GID
                    INNER JOIN {trm_catalog}.OG_PUBLICATION_H OP 
                    ON OP.OG_PUBLICATION_GID = OPT.FK_OG_PUBLICATION_GID
                        WHERE cast(PUBLICATION_DT as date) = cast('{rdate}' as date) + 1 AND 
                        (0,fk_trademark_gid) {trademark_gid_str}
              )         
            """ 
        
    
else:
        df_insert1_query = f""" \
            SELECT
              TRIM(TO_CHAR(PS1.LEGACY_DES_CD, '00')) as actcd,
             regexp_substr(fk_trademark_gid, '[^:]+$') SER_NUM,
              cast(PUBLICATION_DT as date) as pulldt,
              'og' as tabname
              FROM
                    {trm_catalog}.TM_PUBLICATION P1
                    INNER JOIN {trm_catalog}.TM_PUBLICATION_SUBCT PS1
                    ON P1.TM_PUBLICATION_GID = PS1.FK_TM_PUBLICATION_GID
                    INNER JOIN {trm_catalog}.OG_PUBLICATION_TM OPT
                    ON P1.TM_PUBLICATION_GID = OPT.FK_TM_PUBLICATION_GID
                    INNER JOIN {trm_catalog}.OG_PUBLICATION OP 
                    ON OP.OG_PUBLICATION_GID = OPT.FK_OG_PUBLICATION_GID
            WHERE  cast(PUBLICATION_DT as date) = cast('{rdate}' as date) + 1 AND  (0,fk_trademark_gid)
                  """ +trademark_gid_str


# COMMAND ----------

df_insert1 = read_data_from_oracle_conn_dsu_cmn(df_insert1_query, trm_scope)
df_insert1 = df_insert1.withColumn("create_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0"))


# COMMAND ----------

# DBTITLE 1,Insert from am_h
df_insert2_query = f"""
select   
          serial_num_tx   SER_NUM,
          cast('{rdate}' as date) as pulldt,
          'am_h' as tabname,
           case when trunc(filing_dt) between '{rdate}'  and cast('{rdate}' as date) + 1
            then 1
            else 0
            END AS  AM_FLG_NEW_APPL
        
from  {trm_catalog}.trademark_h th left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where  (trunc(th.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1 or trunc(th.begin_effective_ts) =  '{rdate}'  or trunc(th.end_effective_ts) =  '{rdate}' ) AND (0,th.trademark_gid)"""  + trademark_gid_str


# COMMAND ----------

df_insert2 = read_data_from_oracle_conn_dsu_cmn(df_insert2_query,trm_scope)

# COMMAND ----------


df_insert2.createOrReplaceTempView("df_insert2_view")

# COMMAND ----------

df_insert2 = spark.sql(f"""
                           select DECODE(AM_FLG_NEW_APPL, 0, 'TX', {trgt_catalog}.silver.getactioncode(ser_num)) as actcd,
                           ser_num, pulldt, 'am_h' as tabname,
                           from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts, 'tmapplser' as cuser,
                           from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts, 'tmapplser' as luser, 0 as lcno
                           from df_insert2_view
                           """)

# COMMAND ----------

# DBTITLE 1,Insert from am_addr_h
df_insert3_query = f"""
WITH b AS (
    SELECT 
        mailing_address_gid, 
        last_mod_ts,
        begin_effective_ts, 
        end_effective_ts 
    FROM {trm_catalog}.MAILING_ADDRESS_H
)
SELECT DISTINCT 
    'TX' AS actcd,
    serial_num_tx AS SER_NUM,
    CAST('{rdate}' AS DATE) AS pulldt,
    'am_addr_h' AS tabname
FROM b 
LEFT JOIN {trm_catalog}.tm_mailing_addr 
    ON tm_mailing_addr.FK_MAILING_ADDRESS_GID = b.MAILING_ADDRESS_GID
INNER JOIN (
    SELECT * 
    FROM {trm_catalog}.TM_PARTY_ROLE 
    WHERE FK_TM_PARTY_ROLE_CD = 'COR'
) TM_PARTY_ROLE 
    ON tm_mailing_addr.FK_TM_PARTY_ROLE_ID = TM_PARTY_ROLE.TM_PARTY_ROLE_ID
INNER JOIN {trm_catalog}.trademark_h th 
    ON TM_PARTY_ROLE.fk_trademark_gid = th.trademark_gid
LEFT JOIN (
    SELECT * 
    FROM {trm_catalog}.tm_milestone 
    WHERE fk_tm_milestone_cd = 'REG'
) tmh 
    ON tmh.fk_trademark_gid = th.trademark_gid
WHERE     
    (TRUNC(b.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
    OR TRUNC(b.begin_effective_ts) = '{rdate}'
    OR TRUNC(b.end_effective_ts) = '{rdate}')  
    AND (0, th.trademark_gid)
""" + trademark_gid_str

# COMMAND ----------

df_insert3 = read_data_from_oracle_conn_dsu_cmn(df_insert3_query,trm_scope)
df_insert3 = df_insert3.withColumn("create_ts",from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 

# COMMAND ----------

df_insert3.display()

# COMMAND ----------

# DBTITLE 1,Insert from cl_h
df_insert4_query = f"""
SELECT
          DISTINCT 'TX' as actcd,
         regexp_substr(tc.fk_trademark_gid, '[^:]+$')   SER_NUM,
          cast('{rdate}' as date) pulldt,
          'cl_h' as tabname
        FROM
            {trm_catalog}.tm_class_h tc inner join  {trm_catalog}.trademark_h  th on  tc.fk_trademark_gid = th.trademark_gid
                                left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   (trunc(tc.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
     or trunc(tc.begin_effective_ts) =  '{rdate}'
    or trunc(tc.end_effective_ts) =  '{rdate}') AND  (0,th.trademark_gid)
          """ + trademark_gid_str


# COMMAND ----------

df_insert4 = read_data_from_oracle_conn_dsu_cmn(df_insert4_query,trm_scope)
df_insert4 = df_insert4.withColumn("create_ts",from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 

# COMMAND ----------

df_insert4.display()

# COMMAND ----------

# DBTITLE 1,Insert from cm_h
df_insert5_query = f"""
SELECT
          DISTINCT 'TX' as actcd,
          regexp_substr(be.cfk_object_gid, '[^:]+$')  SER_NUM,
          cast('{rdate}' as date) as pulldt,
          'cm_h' as tabname
         FROM
            {trm_catalog}.business_event be inner join  {trm_catalog}.trademark_h  th on  be.cfk_object_gid = th.trademark_gid
                                left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    (trunc(be.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1 or trunc(be.create_ts) = '{rdate}')
       AND (0,th.trademark_gid)
    """+ trademark_gid_str

# COMMAND ----------

df_insert5 = read_data_from_oracle_conn_dsu_cmn(df_insert5_query, trm_scope)
df_insert5 = df_insert5.withColumn("create_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 


# COMMAND ----------

df_insert5.display()

# COMMAND ----------

# DBTITLE 1,Insert from fn_h
df_insert6_query = f"""
 SELECT
          DISTINCT 'TX' as actcd,
          regexp_substr(fbh.fk_trademark_gid, '[^:]+$')   SER_NUM,
           cast('{rdate}' as date) as pulldt,
          'fn_h' as tabname
 FROM
            {trm_catalog}.tm_foreign_basis_h fbh inner join  {trm_catalog}.trademark_h  th on  fbh.fk_trademark_gid = th.trademark_gid
                                left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    (trunc(fbh.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
    or trunc(fbh.begin_effective_ts) =  '{rdate}'
    or trunc(fbh.end_effective_ts) =  '{rdate}') and (0,th.trademark_gid)
         """+ trademark_gid_str

# COMMAND ----------

df_insert6 = read_data_from_oracle_conn_dsu_cmn(df_insert6_query,trm_scope)
df_insert6 = df_insert6.withColumn("create_ts",from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 

# COMMAND ----------

df_insert6.display()

# COMMAND ----------

# DBTITLE 1,Insert from PR_H
df_insert7_query =f"""
select
  DISTINCT 'TX' as actcd,
          regexp_substr(pr.fk_trademark_gid, '[^:]+$')  SER_NUM,
           cast('{rdate}' as date) pulldt,
          'pr_h' as tabname
        
from {trm_catalog}.tm_prior_registration_h pr
inner join {trm_catalog}.trademark_h th on th.trademark_gid =pr.fk_prior_trademark_gid
left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    (trunc(pr.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
    or trunc(pr.begin_effective_ts) =  '{rdate}'
    or trunc(pr.end_effective_ts) =  '{rdate}')
    and (0,th.trademark_gid)
          """+ trademark_gid_str

# COMMAND ----------

df_insert7 = read_data_from_oracle_conn_dsu_cmn(df_insert7_query, trm_scope)
df_insert7 = df_insert7.withColumn("create_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0"))

# COMMAND ----------

df_insert7.display()

# COMMAND ----------

# DBTITLE 1,PY_H
df_insert8_query = f"""
SELECT      DISTINCT 'TX' as actcd,
          regexp_substr(TM_PARTY_ROLE_h.fk_trademark_gid, '[^:]+$')   SER_NUM,
          cast('{rdate}' as date) pulldt,
          'py_h' as tabname
FROM {trm_catalog}.INTERESTED_PARTY_h INNER JOIN {trm_catalog}.TM_PARTY_ROLE_h ON INTERESTED_PARTY_h.INTERESTED_PARTY_GID = TM_PARTY_ROLE_h.FK_INTERESTED_PARTY_GID
           inner join {trm_catalog}.trademark_h th on th.trademark_gid =TM_PARTY_ROLE_h .fk_trademark_gid
left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    (trunc(INTERESTED_PARTY_h.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
    or trunc(INTERESTED_PARTY_h.begin_effective_ts) =  '{rdate}'
    or trunc(INTERESTED_PARTY_h.end_effective_ts) =  '{rdate}')
    and (0,th.trademark_gid)
          """+ trademark_gid_str

# COMMAND ----------

df_insert8 = read_data_from_oracle_conn_dsu_cmn(df_insert8_query,trm_scope)
df_insert8 = df_insert8.withColumn("create_ts",from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 


# COMMAND ----------

df_insert8.display()

# COMMAND ----------

df_insert9_query = f"""
WITH VT_AOBOOR AS (
    SELECT
    CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM,
    (FK_REG_STMNT_TYPE_CD
     || '000'
     || SEQUENCE_NO)                                                VT_TEXT_TYPE,
    DECODE( NVL(LENGTH(STATEMENT_TX), 0), 40, STATEMENT_TX, NVL(STATEMENT_TX
                                                      || ' ', '') ) VT_TEXT, last_mod_ts,null as begin_effective_ts, null as end_effective_ts FROM {trm_catalog}.TM_REGISTRATION_STATEMENT
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
 , a.last_mod_ts,
 a.begin_effective_ts,a.end_effective_ts
FROM
  {trm_catalog}.USE_IN_ANOTHER_FORM_h a
  INNER JOIN {trm_catalog}.STND_CLASS b ON a.FK_CLASS_ID = b.CLASS_ID
),
VT_CU AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM,
    'CU' || trim(to_char(STATEMENT_NO, '0000')) VT_TEXT_TYPE,
    STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts

    FROM {trm_catalog}.CONCURRENT_USE_h
),
VT_CS AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'CS' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
      FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'CS'
),
VT_CC AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'CC' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'CC'
)
,
VT_CD AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'CD' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'CD'
),
VT_DM AS (
    SELECT  CAST(A.SERIAL_NUM_TX AS INTEGER) VT_SER_NUM, 'DM0000' VT_TEXT_TYPE , COALESCE(CAST(A.MARK_DESCRIPTION_TX AS VARCHAR(32767)),B.LITERAL_ELEMENT_TX, '')  AS VT_TEXT, A.last_mod_ts,a.begin_effective_ts,a.end_effective_ts
    FROM {trm_catalog}.TRADEMARK_h A LEFT JOIN  {trm_catalog}.TM_LITERAL_h B ON A.TRADEMARK_GID = B.FK_TRADEMARK_GID
),
VT_DO AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'D0' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'D0' 
),
VT_D1 AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'D1' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

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
  CAST(a.GDS_SRVCS_STMNT_TX AS VARCHAR(32767)) VT_TEXT
  , a.last_mod_ts,a.begin_effective_ts,a.end_effective_ts
FROM
  {trm_catalog}.TM_CLASS_h a
  INNER JOIN {trm_catalog}.STND_CLASS b ON a.FK_CLASS_ID = b.CLASS_ID
WHERE a.GDS_SRVCS_STMNT_TX IS NOT NULL


),
VT_IN AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'IN' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'IN' 


)
,
VT_LS AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'LS' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'LS' 


)
,
VT_NR AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'NR' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'NR' 


),

VT_PM AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'PM' || trim(to_char(SEQUENCE_NO, '0000')) VT_TEXT_TYPE, PSEUDO_MARK_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
    FROM {trm_catalog}.TM_PSEUDO_MARK_h
),
VT_TF AS (
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TF0000' VT_TEXT_TYPE, LIMITATION_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
    FROM {trm_catalog}.SECTION_2F_STATEMENT_h
    WHERE LIMITATION_TX IS NOT NULL
),
VT_TR AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TR' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'TR' 


),
VT_TL AS (
  
    SELECT CAST(regexp_substr(fk_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TL' || trim(to_char(ORDER_NO, '0000')) VT_TEXT_TYPE, STATEMENT_TX VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts
  

    FROM {trm_catalog}.TM_ADDITIONAL_STATEMENT_h

    WHERE FK_STATEMENT_TYPE_CD = 'TL' 


),
VT_TN AS (
  
    SELECT CAST(regexp_substr(fk_parent_trademark_gid, '[^:]+$') AS INTEGER)  VT_SER_NUM, 'TNSFOO' VT_TEXT_TYPE,  regexp_substr(FK_RELATED_TRADEMARK_GID, '[^:]+$')  VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts

    FROM {trm_catalog}.TM_RELATIONSHIP_h

    WHERE FK_RELATIONSHIP_TYPE_CD = 'TNSF' 


)

--INSERT  INTO {trgt_catalog}.silver.TMAPPLSER( actcd, sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
SELECT 
 DISTINCT 'TX'as actcd,
          cast(VT_SER_NUM as varchar(32767)) SER_NUM ,
          cast('{rdate}' as date) pulldt,
          'vt_h' as tabname
FROM
  (
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts FROM VT_AOBOOR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_AF
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_CU
    UNION ALL
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_CS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts FROM VT_CC
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_CD
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_DM
    UNION ALL
    SELECT  VT_SER_NUM,VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_DO
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_D1
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts FROM VT_GS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_LS
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts FROM VT_NR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_PM
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts FROM VT_TF
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT , last_mod_ts,begin_effective_ts,end_effective_ts FROM VT_TR
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts  FROM VT_TL
    UNION ALL
    SELECT  VT_SER_NUM, VT_TEXT_TYPE, VT_TEXT, last_mod_ts,begin_effective_ts,end_effective_ts FROM VT_TN
  

  ) VT
  inner join {trm_catalog}.trademark_h th on th.serial_num_tx = vt.VT_SER_NUM
left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where  
    (trunc(VT.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1)
       AND (0,th.trademark_gid)

        """+ trademark_gid_str

# COMMAND ----------

df_insert9 = read_data_from_oracle_conn_dsu_cmn(df_insert9_query,trm_scope)
df_insert9 = df_insert9.withColumn("create_ts",from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 

# COMMAND ----------

df_insert9.display()

# COMMAND ----------

df_insert10_query = f"""
select   DISTINCT 'TX' as actcd,
          IRT.DN_SERIAL_NUM SER_NUM,
          cast('{rdate}' as date) pulldt,
          'ri_h' as tabname
         from
TMINTLTM.INTERNATIONAL_REGISTRATION_h RI inner join TMINTLTM.INTERNATIONAL_REG_TM_h IRT ON IRT.FK_INTERNATIONAL_REG_GID = RI.INTERNATIONAL_REG_GID
 inner join {trm_catalog}.trademark_h th on th.serial_num_tx = IRT.DN_SERIAL_NUM
left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
 where   
    (trunc(RI.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
    or trunc(RI.begin_effective_ts) =  '{rdate}'
    or trunc(RI.end_effective_ts) =  '{rdate}')
       AND (0,th.trademark_gid)
          """+ trademark_gid_str

# COMMAND ----------

df_insert10 = read_data_from_oracle_conn_dsu_cmn(df_insert10_query, trm_scope)
df_insert10 = df_insert10.withColumn("create_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0")) 

# COMMAND ----------

df_insert10.display()

# COMMAND ----------

df_insert11_query = f"""
SELECT 
      DISTINCT 'TX' as actcd,
         regexp_substr(tde.fk_trademark_gid, '[^:]+$')   SER_NUM,
        cast('{rdate}' as date)  pulldt,
          'wp_h' as tabname
FROM
  {trm_catalog}.TM_DESIGN_ELEMENT tde inner join {trm_catalog}.trademark_h th on th.trademark_gid= tde.fk_trademark_gid
left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
where   
    trunc(tde.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
       AND (0,th.trademark_gid)
          """+ trademark_gid_str


# COMMAND ----------

df_insert11 = read_data_from_oracle_conn_dsu_cmn(df_insert11_query, trm_scope)
df_insert11 = df_insert11.withColumn("create_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0"))

# COMMAND ----------

df_insert11.display()

# COMMAND ----------

df_insert12_query = f"""
select
  DISTINCT 'IB' as actcd,
        regexp_substr( bah.cfk_trademark_gid, '[^:]+$')  SER_NUM,
        cast('{rdate}' as date) pulldt,
          'mas_h' as tabname
FROM
  tmintltm.base_application_h bah  inner join {trm_catalog}.trademark_h th on th.trademark_gid= bah.cfk_trademark_gid
left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
where   
     ( trunc(bah.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
      or trunc(bah.begin_effective_ts) =  '{rdate}'
    or trunc(bah.end_effective_ts) =  '{rdate}')
       AND (0,th.trademark_gid)
          """+ trademark_gid_str



# COMMAND ----------

df_insert12 = read_data_from_oracle_conn_dsu_cmn(df_insert12_query, trm_scope)
df_insert12 = df_insert12.withColumn("create_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(), 'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0"))

# COMMAND ----------

df_insert12.display()

# COMMAND ----------

df_insert13_query = f"""
select 
    DISTINCT 'IB' as actcd,
           regexp_substr( bah.cfk_trademark_gid, '[^:]+$') SER_NUM,
         cast('{rdate}' as date) as pulldt,
          'mhi_h' as tabname
from  tmintltm.international_appl_event iae inner join  tmintltm.international_appl_evnt_rsn iaer on iae.international_appl_evnt_rsn_id = iaer.international_appl_evnt_rsn_id
      inner join  tmintltm.base_application_h bah on bah.FK_INTERNATIONAL_APPL_GID = iae.fk_international_appl_gid
       inner join {trm_catalog}.trademark_h th on th.trademark_gid= bah.cfk_trademark_gid
left join  (select * from {trm_catalog}.tm_milestone where fk_tm_milestone_cd ='REG' ) tmh on tmh.fk_trademark_gid  = th.trademark_gid
where   
    trunc(iae.last_mod_ts) between cast('{rdate}' as date) and cast('{rdate}' as date) + 1
    AND (0,th.trademark_gid)
          """+ trademark_gid_str
# df_insert13.display()

# COMMAND ----------

df_insert13 = read_data_from_oracle_conn_dsu_cmn(df_insert13_query,trm_scope)
df_insert13 = df_insert13.withColumn("create_ts",from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("create_user_id", lit("tmapplser"))\
    .withColumn("last_mod_ts", from_utc_timestamp(current_timestamp(),'America/New_York'))\
    .withColumn("last_mod_user_id", lit("tmapplser"))\
    .withColumn("lock_control_no", lit("0"))

# COMMAND ----------

df_insert13.display()

# COMMAND ----------

# DBTITLE 1,Create TMAPPLSER_HSTRY_MISSING_APPS Table
# MAGIC %sql
# MAGIC create table if not exists ${config.trgt_catalog}.silver.TMAPPLSER_HSTRY_MISSING_APPS
# MAGIC (
# MAGIC   actcd STRING comment 'The action code associated with the load', 
# MAGIC   sernum STRING comment 'The serial number of load',
# MAGIC   pulldt DATE comment 'The date row was pulled from source tables',
# MAGIC   tabname STRING comment 'The table_name were row was pulled from',
# MAGIC   create_ts TIMESTAMP  comment 'The date and time that the record is inserted in the database',
# MAGIC   create_user_id string   comment 'The User Identifier of the logged-on AIS User that initiated the insert of the record into the database',
# MAGIC   last_mod_ts TIMESTAMP  comment 'The date and time that the record was last modified in the database.Upon creation, this will be the same as the Create Timestamp' ,
# MAGIC   last_mod_user_id string  comment 'The User Identifier of the logged on User that initiated the last modification to the record in the database' ,
# MAGIC   lock_control_no INT  comment 'A Number used  to verify that the record being updated has not been altered since it was retrieved for update when optimistic locking is used.'
# MAGIC )
# MAGIC using delta
# MAGIC location 's3://bdr-databricks-app-${config.dbx_env}/eds/delta_tables/${config.trgt_catalog}/silver/tmapplser_hstry_missing_apps'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true,'delta.enableChangeDataFeed' = true, 'delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

try:
    df_union_tmapplser_inc_load = df_insert1.union(df_insert2).union(df_insert3).union(df_insert4).union(df_insert5).union(df_insert6).union(df_insert7).union(df_insert8).union(df_insert9).union(df_insert10).union(df_insert11).union(df_insert12).union(df_insert13)
    df_union_tmapplser_inc_load = df_union_tmapplser_inc_load.dropDuplicates(['SER_NUM', 'pulldt'])
    df_union_tmapplser_inc_load.createOrReplaceTempView("temp_tmapplser_daily_load")

    df_tmapplser_inc_merge = spark.sql(f"""MERGE INTO {trgt_catalog}.silver.TMAPPLSER_HSTRY_MISSING_APPS trgt
                                   USING (select /*+ BROADCAST(temp_tmapplser_daily_load) */ * from temp_tmapplser_daily_load) src
                                   ON trgt.SERNUM = src.SER_NUM
                                   and trgt.pulldt = src.pulldt
                                   WHEN NOT MATCHED THEN INSERT(actcd,sernum,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no)
                                    VALUES(actcd,SER_NUM,pulldt,tabname,create_ts,create_user_id,last_mod_ts,last_mod_user_id,lock_control_no  )
                                """)        
    recs_count = df_tmapplser_inc_merge.select("num_inserted_rows").collect()[0][0]
    print(recs_count)
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
                       

# COMMAND ----------

# DBTITLE 1,Send Email to Stakeholders and Exit Notebook
from datetime import date
Appdf= df_tmapplser_inc_merge
parms = {}
pd.set_option('display.max_colwidth', 0)
parms['INDEXED']=Appdf.toPandas().to_html()
notify = Notify()
templ_str = f'{SRC_SYS_NAME} : BDSS Hstry Temp Summary Daily Event Discrepancy  Data Load'
msg = notify.compose_email( templ_str, f'{SRC_SYS_NAME} bdss Load for {formatted_rundate} - databricks '+env, emailid, parms )
notify.send_mail(msg)
# dbutils.notebook.exit(f"Completed loading TMAPPLSER Table ")

# COMMAND ----------

dbutils.notebook.exit(f"Completed loading TMAPPLSER TXN Table ")

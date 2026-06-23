# Databricks notebook source
# MAGIC %md
# MAGIC ##Query2

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

worker_src = "tmworker"+"-conf.yaml"
worker_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+worker_src

tmintltm_src = "tmintltm"+"-conf.yaml"
tmintltm_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+tmintltm_src

import pytz
from pytz import timezone
print(f'{config_file=},{dbx_env=}')
print(f'{worker_file=},{dbx_env=}')
print(f'{tmintltm_file=},{dbx_env=}')

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
foreign_tmngpdb_oracle_db = common_configs['schema']['src_db_name']

data_quality_catalog = common_configs['schema']['data_quality_catalog']
src_db_name = common_configs['schema']['src_db_name'].upper()

foreign_worker_oracle_db = read_yaml(worker_file)['schema']['src_db_name']
foreign_tmintltm_oracle_db = read_yaml(tmintltm_file)['schema']['src_db_name']

spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.trgt_catalog', trgt_catalog.lower()) 
spark.conf.set('config.dbx_env', dbx_env.lower())

if trgt_catalog.count("_") == 1:
    env = ""
else:
    env = "_"+trgt_catalog.split("_",2)[-1]

print(f'{src_db_name=},{trgt_catalog=},{data_quality_catalog=},{dbx_env=},{env=}')
from pyspark.sql.functions import col, lit

# COMMAND ----------

job_name = 'ntb_silver_bdss_case_file_data_stgload'

start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{data_quality_catalog}',f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

df_case_file_stg = spark.sql(f"""
WITH INTERNATIONAL AS (
  SELECT
    CAST(
      regexp_substr(INTERNATIONAL_REG_TM.CFK_TRADEMARK_GID, '[^:]+$') AS INTEGER
    ) RI_SER_NUM,
    INTERNATIONAL_TM.INTERNATIONAL_REG_NO RI_INTL_REG_NUM,
    date_format(INTERNATIONAL_TM.INTERNATIONAL_REG_DT, 'yyyyMMdd') INTL_REG_DT,
    CASE
      WHEN INTERNATIONAL_REG_TM.PRIORITY_CLAIMED_DT IS NULL THEN 'F'
      ELSE 'T'
    END AS FLG_PRIOR_CLMD,
    COALESCE(date_format(INTERNATIONAL_REG_TM.PRIORITY_CLAIMED_DT, 'yyyyMMdd'), 0) PRIOR_CLMD_DT,
    COALESCE(date_format(INTERNATIONAL_REG_TM.CANCELLATION_DT, 'yyyyMMdd'), 0) DEATH_DT,
    TRIM(TO_CHAR(INTERNATIONAL_REG_TM.CFK_STATUS_CD, '000')) STAT,
    COALESCE(date_format(INTERNATIONAL_REG_TM.STATUS_DT, 'yyyyMMdd'), 0) STAT_DT,
    COALESCE(date_format(INTERNATIONAL_REG_TM.IB_RENEWAL_DT, 'yyyyMMdd'), 0) RNWL_DT,
    COALESCE(date_format(INTERNATIONAL_REG_TM.AUTO_PROTECT_DT, 'yyyyMMdd'), 0) AUTO_PROTEC_DT,
    COALESCE(date_format(INTERNATIONAL_REG_TM.IB_PUBLICATION_DT, 'yyyyMMdd'), 0) IB_PUB_DT,
    DECODE(INTERNATIONAL_REG_TM.FIRST_REFUSAL_IN, 'Y', 'T', 'F') FLG_1ST_REF
  FROM
    {foreign_oracle_catalog}.{foreign_tmintltm_oracle_db}.INTERNATIONAL_REG_TM
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmintltm_oracle_db}.INTERNATIONAL_REGISTRATION ON INTERNATIONAL_REG_TM.FK_INTERNATIONAL_REG_GID = INTERNATIONAL_REGISTRATION.INTERNATIONAL_REG_GID
    LEFT JOIN (
      SELECT
        *
      FROM
        {foreign_oracle_catalog}.{foreign_tmintltm_oracle_db}.INTERNATIONAL_TM
      WHERE
        SOURCE_CT = 'FGN'
    ) INTERNATIONAL_TM ON INTERNATIONAL_TM.INTERNATIONAL_REG_NO = INTERNATIONAL_REGISTRATION.FK_INTERNATIONAL_REG_NO
),
FILED AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'FILED'
),
ABAND AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'ABAND'
),
RENEW AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'RENEW'
),
AMNDR AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'AMNDR'
),
PUB AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'PUB'
),
PUB12 AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'PUB12'
),
REG AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'REG'
),
CNCL AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'CNCL'
),
NOA AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'NOA'
),
TRMNT AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'TRMNTT'
),
SUSP AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'SUSP'
),
PEND AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE
  WHERE
    FK_TM_MILESTONE_CD = 'PEND'
),
MILESTONE AS (
  SELECT
    DISTINCT T.FK_TRADEMARK_GID AS FK_TRADEMARK_GID,
    CASE
      WHEN AMNDR.FK_TM_MILESTONE_CD = 'AMNDR' THEN date_format(AMNDR.MILESTONE_DT, 'yyyyMMdd')
      ELSE 0
    END DT_AMND_REG,
    CASE
      WHEN CNCL.FK_TM_MILESTONE_CD = 'CNCL' THEN date_format(CNCL.MILESTONE_DT, 'yyyyMMdd')
      ELSE NULL
    END DT_CNCL,
    CASE
      WHEN FILED.FK_TM_MILESTONE_CD = 'FILED' THEN date_format(FILED.MILESTONE_DT, 'yyyyMMdd')
      ELSE 0
    END DT_FIL,
    CASE
      WHEN PUB.FK_TM_MILESTONE_CD = 'PUB' THEN date_format(PUB.MILESTONE_DT, 'yyyyMMdd')
      ELSE 0
    END DT_PUB,
    CASE
      WHEN PUB12.FK_TM_MILESTONE_CD = 'PUB12' THEN date_format(PUB12.MILESTONE_DT, 'yyyyMMdd')
      ELSE 0
    END DT_PUB_12C,
    CASE
      WHEN REG.FK_TM_MILESTONE_CD = 'REG' THEN date_format(REG.MILESTONE_DT, 'yyyyMMdd')
      ELSE 0
    END DT_REG,
    CASE
      WHEN ABAND.FK_TM_MILESTONE_CD = 'ABAND' THEN date_format(ABAND.MILESTONE_DT, 'yyyyMMdd')
      ELSE 0
    END DT_ABAN,
    CASE 
      WHEN RENEW.FK_TM_MILESTONE_CD = 'RENEW' THEN date_format(RENEW.MILESTONE_DT, 'yyyyMMdd')
      ELSE 0
    END DT_RNWL
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MILESTONE T
    left JOIN FILED ON T.FK_TRADEMARK_GID = FILED.FK_TRADEMARK_GID
    left JOIN ABAND ON T.FK_TRADEMARK_GID = ABAND.FK_TRADEMARK_GID
    left JOIN RENEW ON T.FK_TRADEMARK_GID = RENEW.FK_TRADEMARK_GID
    left JOIN AMNDR ON T.FK_TRADEMARK_GID = AMNDR.FK_TRADEMARK_GID
    left JOIN PUB ON T.FK_TRADEMARK_GID = PUB.FK_TRADEMARK_GID
    left JOIN PUB12 ON T.FK_TRADEMARK_GID = PUB12.FK_TRADEMARK_GID
    left JOIN REG ON T.FK_TRADEMARK_GID = REG.FK_TRADEMARK_GID
    left JOIN CNCL ON T.FK_TRADEMARK_GID = CNCL.FK_TRADEMARK_GID
),
ONEA AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASIS
  WHERE
    FK_FILING_BASIS_CD = '1(a)'
),
ONEB AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASIS
  WHERE
    FK_FILING_BASIS_CD = '1(b)'
),
NOBASIS AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASIS
  WHERE
    FK_FILING_BASIS_CD = 'NOBAS'
),
FORTYFOURD AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASIS
  WHERE
    FK_FILING_BASIS_CD = '44(d)'
),
FORTYFOURE AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASIS
  WHERE
    FK_FILING_BASIS_CD = '44(e)'
),
SIXTYSIXA AS (
  SELECT
    *
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASIS
  WHERE
    FK_FILING_BASIS_CD = '66(a)'
),
FILING_BASIS AS (
  SELECT
    DISTINCT T.FK_TRADEMARK_GID AS FK_TRADEMARK_GID,
    CASE
      WHEN SIXTYSIXA.FK_FILING_BASIS_CD = '66(a)' THEN DECODE(SIXTYSIXA.FILED_IN, 'Y', 'T', 'F')
    END FLG_66A_FIL,
    CASE
      WHEN SIXTYSIXA.FK_FILING_BASIS_CD = '66(a)' THEN DECODE(SIXTYSIXA.CURRENT_IN, 'Y', 'T', 'F')
    END FLG_66A_CUR,
    CASE
      WHEN ONEA.FK_FILING_BASIS_CD = '1(a)' THEN DECODE(ONEA.FILED_IN, 'Y', 'T', 'F')
    END FLG_USE_FIL,
    CASE
      WHEN ONEA.FK_FILING_BASIS_CD = '1(a)' THEN DECODE(ONEA.CURRENT_IN, 'Y', 'T', 'F')
    END FLG_USE_CUR,
    CASE
      WHEN ONEA.FK_FILING_BASIS_CD = '1(a)' THEN DECODE(ONEA.AMENDED_IN, 'Y', 'T', 'F')
    END FLG_USE_AMED,
    CASE
      WHEN ONEB.FK_FILING_BASIS_CD = '1(b)' THEN DECODE(ONEB.FILED_IN, 'Y', 'T', 'F')
    END FLG_ITU_FIL,
    CASE
      WHEN ONEB.FK_FILING_BASIS_CD = '1(b)' THEN DECODE(ONEB.AMENDED_IN, 'Y', 'T', 'F')
    END FLG_ITU_AMED,
    CASE
      WHEN ONEB.FK_FILING_BASIS_CD = '1(b)' THEN DECODE(ONEB.CURRENT_IN, 'Y', 'T', 'F')
    END FLG_ITU_CUR,
    CASE
      WHEN NOBASIS.FK_FILING_BASIS_CD = 'NOBAS' THEN DECODE(NOBASIS.CURRENT_IN, 'Y', 'T', 'F')
    END FLG_NO_BAS_CUR,
    CASE
      WHEN NOBASIS.FK_FILING_BASIS_CD = 'NOBAS' THEN DECODE(NOBASIS.FILED_IN, 'Y', 'T', 'F')
    END FLG_NO_BAS_FIL,
    CASE
      WHEN NOBASIS.FK_FILING_BASIS_CD = 'NOBAS' THEN DECODE(NOBASIS.AMENDED_IN, 'Y', 'T', 'F')
    END FLG_NO_BAS_AMED,
    CASE
      WHEN FORTYFOURD.FK_FILING_BASIS_CD = '44(d)' THEN DECODE(FORTYFOURD.AMENDED_IN, 'Y', 'T', 'F')
    END FLG_44D_AMED,
    CASE
      WHEN FORTYFOURD.FK_FILING_BASIS_CD = '44(d)' THEN DECODE(FORTYFOURD.CURRENT_IN, 'Y', 'T', 'F')
    END FLG_44D_CUR,
    CASE
      WHEN FORTYFOURD.FK_FILING_BASIS_CD = '44(d)' THEN DECODE(FORTYFOURD.FILED_IN, 'Y', 'T', 'F')
    END FLG_44D_FIL,
    CASE
      WHEN FORTYFOURE.FK_FILING_BASIS_CD = '44(e)' THEN DECODE(FORTYFOURE.AMENDED_IN, 'Y', 'T', 'F')
    END FLG_44E_AMED,
    CASE
      WHEN FORTYFOURE.FK_FILING_BASIS_CD = '44(e)' THEN DECODE(FORTYFOURE.CURRENT_IN, 'Y', 'T', 'F')
    END FLG_44E_CUR,
    CASE
      WHEN FORTYFOURE.FK_FILING_BASIS_CD = '44(e)' THEN DECODE(FORTYFOURE.FILED_IN, 'Y', 'T', 'F')
    END FLG_44E_FIL
  FROM {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASIS T
    LEFT JOIN SIXTYSIXA ON T.FK_TRADEMARK_GID = SIXTYSIXA.FK_TRADEMARK_GID
    LEFT JOIN ONEA ON T.FK_TRADEMARK_GID = ONEA.FK_TRADEMARK_GID
    LEFT JOIN NOBASIS ON T.FK_TRADEMARK_GID = NOBASIS.FK_TRADEMARK_GID
    LEFT JOIN ONEB ON T.FK_TRADEMARK_GID = ONEB.FK_TRADEMARK_GID
    LEFT JOIN FORTYFOURD ON T.FK_TRADEMARK_GID = FORTYFOURD.FK_TRADEMARK_GID
    LEFT JOIN FORTYFOURE ON T.FK_TRADEMARK_GID = FORTYFOURE.FK_TRADEMARK_GID
),
TM AS (
  select
    *
  from
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MARK_TYPE
  WHERE
    fk_mark_type_cd = 'TM'
),
SM AS (
  select
    *
  from
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MARK_TYPE
  WHERE
    fk_mark_type_cd = 'SM'
),
CM AS (
  select
    *
  from
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MARK_TYPE
  WHERE
    fk_mark_type_cd = 'CM'
),
COLL_SM AS (
  select
    *
  from
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MARK_TYPE
  WHERE
    fk_mark_type_cd = 'COLL_SM'
),
COLL_TM AS (
  select
    *
  from
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MARK_TYPE
  WHERE
    fk_mark_type_cd = 'COLL_TM'
),
COLL_MM AS (
  select
    *
  from
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MARK_TYPE
  WHERE
    fk_mark_type_cd = 'COLL_MM'
),
MARK_TYPE AS (
  SELECT
    DISTINCT TT.FK_TRADEMARK_GID AS FK_TRADEMARK_GID,
    CASE
      WHEN TM.FK_MARK_TYPE_CD = 'TM' THEN DECODE(TM.FK_MARK_TYPE_CD, 'TM', 'T', 'F')
    END FLG_TM,
    CASE
      WHEN SM.FK_MARK_TYPE_CD = 'SM' THEN DECODE(SM.FK_MARK_TYPE_CD, 'SM', 'T', 'F')
    END FLG_SM,
    CASE
      WHEN COLL_SM.FK_MARK_TYPE_CD = 'COLL_SM' THEN DECODE(COLL_SM.FK_MARK_TYPE_CD, 'COLL_SM', 'T', 'F')
    END FLG_COLL_SM,
    CASE
      WHEN COLL_TM.FK_MARK_TYPE_CD = 'COLL_TM' THEN DECODE(COLL_TM.FK_MARK_TYPE_CD, 'COLL_TM', 'T', 'F')
    END FLG_COLL_TM,
    CASE
      WHEN COLL_MM.FK_MARK_TYPE_CD = 'COLL_MM' THEN DECODE(COLL_MM.FK_MARK_TYPE_CD, 'COLL_MM', 'T', 'F')
    END FLG_COLL_MM,
    CASE
      WHEN CM.FK_MARK_TYPE_CD = 'CM' THEN DECODE(CM.FK_MARK_TYPE_CD, 'CM', 'T', 'F')
    END FLG_CM
  FROM
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_MARK_TYPE TT
    LEFT JOIN TM ON TT.fk_trademark_gid = TM.fk_trademark_gid
    LEFT JOIN SM ON TT.fk_trademark_gid = SM.fk_trademark_gid
    LEFT JOIN CM ON CM.fk_trademark_gid = TT.fk_trademark_gid
    LEFT JOIN COLL_TM ON COLL_TM.fk_trademark_gid = TT.fk_trademark_gid
    LEFT JOIN COLL_SM on COLL_SM.fk_trademark_gid = TT.fk_trademark_gid
    LEFT JOIN COLL_MM ON COLL_MM.fk_trademark_gid = TT.fk_trademark_gid
),
CASE_FILE AS (
  select
    DISTINCT COALESCE(DT_AMND_REG, 0) DT_AMND_REG,
    COALESCE(
      TRIM(TM_POST_REGISTRATION.CFK_CANCELLATION_REASON_CD),
      ''
    ) CNCL_CD,
    COALESCE(DT_CNCL, 0) DT_CNCL,
    COALESCE(DT_FIL, 0) DT_FIL,
    COALESCE(DT_PUB, 0) DT_PUB,
    COALESCE(DT_PUB_12C, 0) DT_PUB_12_C,
    COALESCE(DT_REG, 0) DT_REG,
    COALESCE(date_format(T.STATUS_DT, 'yyyyMMdd'), 0) DT_STAT,
    COALESCE(DT_ABAN, 0) DT_ABAN,
    DECODE(
      TM_STATES.REGISTER_AMENDED_PRINCIPAL_IN,
      'Y',
      'T',
      'F'
    ) FLG_AMND_PRIN,
    DECODE(
      TM_STATES.REGISTER_AMENDED_SUPL_IN,
      'Y',
      'T',
      'F'
    ) FLG_AMND_SUPL,
    COALESCE(MT.FLG_TM, 'F') FLG_TM,
    COALESCE(MT.FLG_COLL_TM, 'F') FLG_COLL_TM,
    COALESCE(MT.FLG_SM, 'F') FLG_SM,
    COALESCE(MT.FLG_COLL_SM, 'F') FLG_COLL_SM,
    COALESCE(MT.FLG_COLL_MM, 'F') FLG_COLL_MM,
    COALESCE(MT.FLG_CM, 'F') FLG_CM,
    DECODE(
      TM_APPEALS.CNCL_PENDING_TTAB_PRCDNG_IN,
      'Y',
      'T',
      'F'
    ) FLG_CNCL_PEND,
    DECODE(
      TM_STATES.CONCURRENT_USE_PUBLISHED_IN,
      'Y',
      'T',
      'F'
    ) FLG_PUB_CNCR,
    DECODE(TM_STATES.CONCURRENT_USE_IN, 'Y', 'T', 'F') FLG_CNCR,
    DECODE(
      TM_STATES.CNCR_USE_PEND_TTAB_PRCDNG_IN,
      'Y',
      'T',
      'F'
    ) FLG_CNCR_PEND,
    DECODE(
      TM_STATES.INTF_PENDING_TTAB_PRCDNG_IN,
      'Y',
      'T',
      'F'
    ) FLG_INTF_PEND,
    DECODE(
      TM_APPEALS.OPPOSITION_PEND_TTAB_PRCDNG_IN,
      'Y',
      'T',
      'F'
    ) FLG_OPPS_PEND,
    DECODE(
      TM_POST_REGISTRATION.REPUBLISH_SECTION_12_IN,
      'Y',
      'T',
      'F'
    ) FLG_RPB_SCT_12,
    CASE
      WHEN SECTION_2F_STATEMENT.SECTION_2F_CT IN ('BOTH', 'WHOLE')
        THEN 'T'
        ELSE 'F'
    END AS FLG_SCT_2F,
    CASE
      WHEN SECTION_2F_STATEMENT.SECTION_2F_CT IN ('PART')
        THEN 'T'
        ELSE 'F'
    END AS FLG_SCT_2F_PT,
    DECODE(
      TM_POST_REGISTRATION.SECTION_8_ACCEPTED_IN,
      'Y',
      'T',
      'F'
    ) FLG_SCT_8_ACPT,
    DECODE(
      TM_POST_REGISTRATION.SECTION_8_FILED_IN,
      'Y',
      'T',
      'F'
    ) FLG_SCT_8_FIL,
    DECODE(
      TM_POST_REGISTRATION.SECTION_8_PARTIAL_ACCEPTED_IN,
      'Y',
      'T',
      'F'
    ) FLG_SCT_8_P_A,
    DECODE(
      TM_POST_REGISTRATION.SECTION_15_ACKD_IN,
      'Y',
      'T',
      'F'
    ) FLG_SCT_15_ACK,
    DECODE(
      TM_POST_REGISTRATION.SECTION_15_FILED_IN,
      'Y',
      'T',
      'F'
    ) FLG_SCT_15_FIL,
    DECODE(
      TM_POST_REGISTRATION.RENEWAL_FILED_IN,
      'Y',
      'T',
      'F'
    ) FLG_RNWL_FIL,
    DECODE(T.REGISTRY_CT, 'S', 'T', 'F') FLG_SUPL_REG,
    DECODE(
      TM_FILING_BASES.FOREIGN_PRIORITY_CLAIMED_IN,
      'Y',
      'T',
      'F'
    ) FLG_FRPR_CLMD,
    DECODE(
      TM_POST_REGISTRATION.REGISTRATION_AMENDED_IN,
      'Y',
      'T',
      'F'
    ) FLG_CHNG_REG,
    CAST(T.LEGACY_STATUS_CD AS INT) AS AM_STAT,
    T.FK_MARK_DRAWING_TYPE_CD AM_MARK_DWG_CD,
    TRIM(TM_LOCATIONS.CFK_ASGND_EXAM_LAW_OFC_ORG_CD) LO_ASGN,
    FLG_ITU_FIL,
    FLG_ITU_CUR,
    FLG_ITU_AMED,
    COALESCE(TRIM(T.EXTERNAL_REFERENCE_TX), '') ATTY_DKT_NUM,
    FLG_USE_FIL,
    FLG_USE_AMED,
    FLG_USE_CUR,
    FLG_44D_FIL,
    FLG_44D_AMED,
    FLG_44D_CUR,
    FLG_44E_FIL,
    FLG_44E_AMED,
    FLG_44E_CUR,
    FLG_NO_BAS_CUR,
    FLG_NO_BAS_FIL,
    DECODE(
      TM_DRAWING.SPCL_FORM_FILD_COLOR_DWG_IN,
      'Y',
      'T',
      'F'
    ) FLG_C_DRW_FIL,
    DECODE(TM_DRAWING.COLOR_IN, 'Y', 'T', 'F') FLG_C_DRW_CUR,
    DECODE(
      TM_DRAWING.SPCL_FORM_FILED_3D_DRAWING_IN,
      'Y',
      'T',
      'F'
    ) FLG_3D_DRW_FIL,
    DECODE(TM_DRAWING.THREE_DIMENSION_IN, 'Y', 'T', 'F') FLG_3D_DRW_CUR,
    CASE
      WHEN T.FK_MARK_DRAWING_TYPE_CD IN (0, 1, 4) THEN 'T'
      ELSE 'F'
    END FLG_STD_CHAR,
    FLG_66A_FIL,
    FLG_66A_CUR,
    CASE
      WHEN TRIM(STANDARD_CHARACTER_TX) IS NOT NULL
      THEN COALESCE(TRIM(STANDARD_CHARACTER_TX), '')
      ELSE COALESCE(TRIM(LITERAL_ELEMENT_TX), '')
    END AS MARK_1_LIN,
    COALESCE(DT_RNWL, 0) DT_RNWL,
    COALESCE(TRIM(WORKER_NM), '') EMPE_NAM,
    CASE
      WHEN LENGTH(STANDARD_CHARACTER_TX) > 40
      OR LENGTH(LITERAL_ELEMENT_TX) > 40 THEN 1
      ELSE 0
    END AM_FLG_MARK_OFLW,
    DECODE(ACTV_PR_OTHER_PRIOR_REG_IN, 'Y', 'T', 'F') FLG_AND_OTH_CD,
    date_format(TM_LOCATIONS.CURRENT_LOCATION_DT, 'yyyyMMdd') DT_IN_LOC,
    COALESCE(
      TRIM(TM_ORGANIZATION_LOCATION.LOCATION_DESC_TX),
      ''
    ) CURR_LOC,
    date_format(T.LAST_MOD_TS, 'yyyyMMdd') APPLY_TIME,
    SERIAL_NUM_TX AM_SER_NUM,
    TRIM(
      TO_CHAR(COALESCE(REGISTRATION_NUM, 0), '0000000')
    ) REG_NUM
  from
    {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.trademark t
    left join FILING_BASIS f on t.trademark_gid = f.FK_TRADEMARK_GID
    left join milestone m on m.FK_TRADEMARK_GID = t.trademark_gid
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_POST_REGISTRATION ON T.TRADEMARK_GID = TM_POST_REGISTRATION.FK_TRADEMARK_GID
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_STATES ON T.TRADEMARK_GID = TM_STATES.FK_TRADEMARK_GID
    LEFT JOIN MARK_TYPE MT ON MT.FK_TRADEMARK_GID = T.trademark_gid
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_APPEALS ON T.TRADEMARK_GID = TM_APPEALS.CFK_TRADEMARK_GID
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.SECTION_2F_STATEMENT ON T.TRADEMARK_GID = SECTION_2F_STATEMENT.FK_TRADEMARK_GID
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_FILING_BASES ON T.TRADEMARK_GID = TM_FILING_BASES.FK_TRADEMARK_GID
    LEFT JOIN (
      SELECT
        DISTINCT FK_TRADEMARK_GID,
        CFK_EMPLOYEE_NO
      FROM
        {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_EMPLOYEE_ASSIGNMENT
      WHERE
        FK_TM_EMPLOYEE_ROLE_CD = 'EA'
    ) TM_EMPLOYEE_ASSIGNMENT ON T.TRADEMARK_GID = TM_EMPLOYEE_ASSIGNMENT.FK_TRADEMARK_GID
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_DRAWING ON T.TRADEMARK_GID = TM_DRAWING.FK_TRADEMARK_GID
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_LITERAL ON T.TRADEMARK_GID = TM_LITERAL.FK_TRADEMARK_GID
    LEFT JOIN (
      SELECT
        fk_trademark_gid,
        actv_pr_other_prior_reg_in
      FROM
        (
          SELECT
            distinct fk_trademark_gid,
            actv_pr_other_prior_reg_in,
            row_number() over (
              partition by fk_trademark_gid
              order by
                last_mod_ts
            ) rowcount
          FROM
            {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_ADDITIONAL_STATEMENT
          WHERE
            actv_pr_other_prior_reg_in IS NOT NULL
        )
      where
        rowcount = 1
    ) TA ON T.TRADEMARK_GID = TA.FK_TRADEMARK_GID
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_LOCATIONS ON TM_LOCATIONS.FK_TRADEMARK_GID = T.TRADEMARK_GID
    LEFT JOIN {foreign_oracle_catalog}.{foreign_tmngpdb_oracle_db}.TM_ORGANIZATION_LOCATION ON TM_LOCATIONS.FK_CURRENT_LOCATION_CD = TM_ORGANIZATION_LOCATION.LOCATION_CD
    LEFT JOIN {foreign_oracle_catalog}.{foreign_worker_oracle_db}.WORKER WORKER ON WORKER.WORKER_NO = TM_EMPLOYEE_ASSIGNMENT.CFK_EMPLOYEE_NO
    WHERE SERIAL_NUM_TX in (select distinct sernum from {trgt_catalog}.silver.tmappl_daily_consolidated_vw )
)

SELECT *
FROM INTERNATIONAL I
RIGHT JOIN CASE_FILE CF ON CF.AM_SER_NUM = I.RI_SER_NUM

"""
)

# COMMAND ----------

try:
    df_case_file_stg.write.mode("overwrite").format("delta").saveAsTable(f'{trgt_catalog}.silver.bdss_case_file_data_daily_stg')
    recs_count = df_case_file_stg.count()
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'completed',0,recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading {recs_count} records into .bdss_case_file_data_daily_stg Table ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{data_quality_catalog}",f"{trgt_catalog}.silver", job_name, start_ts,'failed',0,0,e)
    raise
dbutils.notebook.exit(f"Completed loading bdss_wipo_daily_stg Table ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trm_tmngpdb_dev.silver.bdss_case_file_data_daily_stg

# Databricks notebook source
# MAGIC %md
# MAGIC ##EpQuery Logic from Tableau EPQUERY Production report
# MAGIC <pre>
# MAGIC modeul = EPquerySub
# MAGIC This query has 2 subueries and parameters are passed from Tableau Report at run time
# MAGIC Source catalogs:trm_tmworker,trm_tmprodvty,trm_tmngpdb
# MAGIC Source Tables:
# MAGIC trm_tmworker{env}.bronze.worker
# MAGIC trm_tmprodvty{env}.bronze.production_transaction
# MAGIC trm_tmprodvty{env}.bronze.productivity_action
# MAGIC trm_tmworker{env}.bronze.tm_organization_rltnshp
# MAGIC trm_tmprodvty{env}.bronze.productivity_action
# MAGIC trm_tmngpdb{env}.bronze.trademark
# MAGIC trm_tmngpdb{env}.bronze.tm_literal
# MAGIC trm_tmngpdb{env}.bronze.tm_itu 
# MAGIC </pre>

# COMMAND ----------

# DBTITLE 1,Set config file
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Set parameter values
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['reporting_catalog']
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
tqr_catalog = common_configs['schema']['tqr_catalog']

if dbx_env.lower() !='prod':
    env = '_'+dbx_env.lower()
else:
    env =''
print(env)

# COMMAND ----------

common_configs = read_yaml(config_file)
tmprodvty_catalog = common_configs['schema']['tmprodvty_catalog']
print(tmprodvty_catalog)

# COMMAND ----------


spark.sql(f"select distinct title_tx, productivity_action_id from trm_tmprodvty{env}.bronze.productivity_action PA")

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_second_level_epquery_stg'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Query1

# COMMAND ----------

df_epquery_stg1 = spark.sql(f"""
select 
    DISTINCT 
    EP.CFK_OBJECT_GID,
    SUBSTR(EP.CFK_OBJECT_GID, 13, LENGTH(EP.CFK_OBJECT_GID)) EP_SER_NUM,
    CRT_CNT,
    No_CRT_CNT,
    DN_WORKER_NO,
    ep.production_credit_tran_id,
    --DN_WORKER_TM_ORGANIZATION_CD,
    DN_WORKER_TM_ORGANIZATION_CD,
    CURRENT_LO,
    --DESCRIPTION_TX,
    PA.PRODUCTIVITY_ACTION_CD EP_TRAN_CD,
    TRANSACTION_EFFECTIVE_DT,
    CFK_BCR_PAY_PERIOD_RANGE_NAME EP_PP_PERIOD,
    PA1.PRODUCTIVITY_ACTION_CD EP_TRAN_IND,
    SUBSEQUENT_ACTION_IN PAS_INDICATOR,
    DN_ACTION_NO EP_ACTN_NUM,
    PRIORITY_IN EP_FLG_PRIORITY,
    EP.CREATE_TS,
    UNIT_COUNT_NO EP_ACT_CREDIT,
    DN_WORKER_TM_ORGANIZATION_CD EP_LO,
    COALESCE(STANDARD_CHARACTER_TX, LITERAL_ELEMENT_TX) as AM_MARK,
    INTENT_TO_USE_DT,
    CASE
      WHEN INTENT_TO_USE_DT IS NOT NULL AND TRANSACTION_EFFECTIVE_DT > TC.CREATE_TS THEN 'YES'
      ELSE 'NO'
    END IU_SW,
    CASE
      WHEN pa.Productivity_action_cd = 6422
      and INTENT_TO_USE_DT is not null then 0
      WHEN pa.Productivity_action_cd = 6422
      and (NO_CRT_CNT < CRT_CNT) then 0
      WHEN pa.Productivity_action_cd = 6422
      and (
        coalesce(CRT_CNT, 0) = 0
        and NO_CRT_CNT > 0
      ) then 0
      WHEN pa.Productivity_action_cd = 6422
      and (NO_CRT_CNT > CRT_CNT) then 1
      WHEN pa.Productivity_action_cd = 6422
      and (
        CRT_CNT is null
        and NO_CRT_CNT is not null
      ) then 0 --WHEN  pa.Productivity_action_cd = 6422 and  (CRT_CNT is null and NO_CRT_CNT is  null)   then 0
      ELSE 1
    end INCLUDE_6422_SW,
    --pa.Productivity_action_cd = 6422 AND NO_CRT_CNT = 0 AND CRT_CNT = 0 and (INTENT_TO_USE_DT is null OR TRIM(LENGTH(INTENT_TO_USE_DT)) = 0) THEN 0
    CASE
      WHEN PA.PRODUCTIVITY_ACTION_CD IN (6321, 6663, 6664) THEN UNIT_COUNT_NO * - 1
      ELSE UNIT_COUNT_NO
    END WK_ACTV_CLS,
    WORKER_NM Worker_name,
    BRS_USER_ID,
    WORKER_NO wk_no,
    PA.title_tx Tran_action_type,
    rl.fk_parent_TM_ORGANIZATION_gid
from trm_tmworker{env}.bronze.worker W
    LEFT JOIN (select distinct worker_no as worker_num, first(dn_worker_tm_organization_cd) over(partition by worker_no order by max(EP.transaction_effective_dt) desc) as current_lo
                from trm_tmworker{env}.bronze.worker W
                LEFT JOIN trm_tmprodvty{env}.bronze.production_transaction EP ON WORKER_NO = DN_WORKER_NO
                AND DELETE_IN = 'N' 
                where dn_worker_tm_organization_cd like 'LO%'
                group by worker_no,dn_worker_tm_organization_cd)c_lo
    ON w.WORKER_NO = c_lo.worker_num
    LEFT JOIN trm_tmprodvty{env}.bronze.production_transaction EP ON WORKER_NO = DN_WORKER_NO
    AND DELETE_IN = 'N' --ON  ORG.ORGANIZATION_CD = EP.DN_WORKER_TM_ORGANIZATION_CD
    LEFT JOIN trm_tmprodvty{env}.bronze.productivity_action PA 
    ON FK_GENERATING_PRODVTY_ACTN_ID = PA.PRODUCTIVITY_ACTION_ID
--AND DN_WORKER_NO > 49999
left join trm_tmworker{env}.bronze.tm_organization_rltnshp rl on EP.CFK_WORKER_TM_ORGANIZATION_gid = rl.fk_child_TM_ORGANIZATION_gid
LEFT JOIN trm_tmprodvty{env}.bronze.productivity_action PA1 ON FK_CORRECTED_PRODVTY_ACTN_ID = PA1.PRODUCTIVITY_ACTION_ID
LEFT JOIN trm_tmngpdb{env}.bronze.trademark AM ON EP.CFK_OBJECT_GID = AM.TRADEMARK_GID
LEFT JOIN trm_tmngpdb{env}.bronze.tm_literal AM_LIT ON CFK_OBJECT_GID = AM_LIT.FK_TRADEMARK_GID
LEFT JOIN (
  SELECT
    FK_TRADEMARK_GID,
    potentiel_abandonment_dt INTENT_TO_USE_DT,
    create_ts
  FROM
    trm_tmngpdb{env}.bronze.tm_itu
) TC ON EP.CFK_OBJECT_GID = TC.FK_TRADEMARK_GID
LEFT JOIN (
  SELECT
    CFK_OBJECT_GID,
    -- COUNT(*) CRT_CNT
    min(r_num) no_CRT_CNT
  from
    (
      SELECT
        CFK_OBJECT_GID,
        production_credit_tran_id,
        PRODUCTIVITY_ACTION_CD,
        ROW_NUMBER() OVER (
          PARTITION BY CFK_OBJECT_GID
          ORDER BY
            production_credit_tran_id desc
        ) R_NUM,
        case
          when PRODUCTIVITY_ACTION_CD IN (
            '6321',
            '6325',
            '6328',
            '6329',
            '6330',
            '6331',
            '6332',
            '6333',
            '6334',
            '6335',
            '6663',
            '6664'
          ) THEN 'CRTV'
          when PRODUCTIVITY_ACTION_CD IN ('6322', '6323', '6324', '6338', '6339') then 'NCRT'
          else '0'
        END status
      FROM
        trm_tmprodvty{env}.bronze.production_transaction EP1
        INNER JOIN trm_tmprodvty{env}.bronze.productivity_action PA2 ON FK_GENERATING_PRODVTY_ACTN_ID = PA2.PRODUCTIVITY_ACTION_ID --AND CFK_BCR_PAY_PERIOD_RANGE_NAME between 202201  and  substr(<Parameters.Pay Period>, 5,6)
        --AND DN_WORKER_NO > 49999
        AND DELETE_IN = 'N'
        AND PRODUCTIVITY_ACTION_CD IN (
          '6321',
          '6325',
          '6328',
          '6329',
          '6330',
          '6331',
          '6332',
          '6333',
          '6334',
          '6335',
          '6663',
          '6664',
          '6322',
          '6323',
          '6324',
          '6338',
          '6339'
        ) --and CFK_OBJECT_GID = 'Trademark:0:88523376'
    ) a
  where
    status = 'NCRT'
  group by
    CFK_OBJECT_GID
) NO_CRT ON EP.CFK_OBJECT_GID = NO_CRT.CFK_OBJECT_GID
LEFT JOIN (
  SELECT
    CFK_OBJECT_GID,
    -- COUNT(*) CRT_CNT
    min(r_num) CRT_CNT
  from
    (
      SELECT
        CFK_OBJECT_GID,
        production_credit_tran_id,
        PRODUCTIVITY_ACTION_CD,
        ROW_NUMBER() OVER (
          PARTITION BY CFK_OBJECT_GID
          ORDER BY
            production_credit_tran_id desc
        ) R_NUM,
        case
          when PRODUCTIVITY_ACTION_CD IN (
            '6321',
            '6325',
            '6328',
            '6329',
            '6330',
            '6331',
            '6332',
            '6333',
            '6334',
            '6335',
            '6663',
            '6664'
          ) THEN 'CRTV'
          when PRODUCTIVITY_ACTION_CD IN ('6322', '6323', '6324', '6338', '6339') then 'NCRT'
          else '0'
        END status
      FROM
        trm_tmprodvty{env}.bronze.production_transaction EP1
        INNER JOIN trm_tmprodvty{env}.bronze.productivity_action PA2 ON FK_GENERATING_PRODVTY_ACTN_ID = PA2.PRODUCTIVITY_ACTION_ID
        --AND DN_WORKER_NO > 49999
        AND DELETE_IN = 'N'
        AND PRODUCTIVITY_ACTION_CD IN (
          '6321',
          '6325',
          '6328',
          '6329',
          '6330',
          '6331',
          '6332',
          '6333',
          '6334',
          '6335',
          '6663',
          '6664',
          '6322',
          '6323',
          '6324',
          '6338',
          '6339'
        ) --and CFK_OBJECT_GID = 'Trademark:0:88523376'
    ) a
  where
    status = 'CRTV'
  group by
    CFK_OBJECT_GID
) CRT ON EP.CFK_OBJECT_GID = CRT.CFK_OBJECT_GID
WHERE
  DN_WORKER_TM_ORGANIZATION_CD LIKE 'LO%'
  """)

# COMMAND ----------

df_epquery_stg1.write.mode("overwrite").format("delta").saveAsTable(f'trm_reporting{env}.silver.epquery_stg1')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Query2

# COMMAND ----------

df_epquery_stg2 = spark.sql(f"""
SELECT
distinct CFK_OBJECT_GID,
DN_WORKER_NO,
CREATE_TS,
EP_SER_NUM,
DN_WORKER_TM_ORGANIZATION_CD LO,
CURRENT_LO,
IU_SW,
EP_TRAN_CD,
CRT_CNT,
No_CRT_CNT,
EP_ACTN_NUM,
EP_TRAN_IND,
BRS_USER_ID,
wk_no,
fk_parent_TM_ORGANIZATION_gid,
AM_MARK,
INCLUDE_6422_SW,
--FY_PROCESS_SW,
WK_ACTV_CLS,
EP_FLG_PRIORITY,
EP_PP_PERIOD,
TRANSACTION_EFFECTIVE_DT,
extract(month from to_date(Transaction_effective_dt)) tran_month,
'All Law Offices' as ALO,
case
when fk_parent_TM_ORGANIZATION_gid = 'TMOrganization:0:EXAMA' then 'Group A'
when fk_parent_TM_ORGANIZATION_gid = 'TMOrganization:0:EXAMB' then 'Group B'
when fk_parent_TM_ORGANIZATION_gid = 'TMOrganization:0:EXAMC' then 'Group C'
when fk_parent_TM_ORGANIZATION_gid = 'TMOrganization:0:EXAMD' then 'Group D'
END `Group`,
case when a.EP_ACTN_NUM = 1 and a.EP_TRAN_CD = '6121' then WK_ACTV_CLS else 0 end AAU_REJ_CR_FY,
/*AAU_REJ_CR_PP,*/--AAU_REJ_CR_FY,--DONE
PAS_INDICATOR,
case when a.EP_ACTN_NUM = 1 and ( a.EP_TRAN_CD = '6121' or ( a.EP_TRAN_CD in ('6321', '6663', '6664')and a.EP_TRAN_IND = '6121')) then WK_ACTV_CLS else 0 end AAU_REJ_CT_FY,
case when a.EP_ACTN_NUM = 1 and a.EP_TRAN_CD in ('6321', '6663', '6664') and a.EP_TRAN_IND = '6121' then WK_ACTV_CLS else 0 end AAU_REJ_CC_FY,
/*AAU_REJ_CT_PP,*/--AAU_REJ_CT_FY,--DONE
/*AAU_REJ_CC_PP,*/--AAU_REJ_CC_FY,--DONE
/*APP_PUB_CR_PP,*/--APP_PUB_CR_FY,--DONE
case when a.EP_TRAN_CD in ('6338', '6339', '6341', '6342') then WK_ACTV_CLS else 0 end APP_PUB_CR_FY,
case when (a.EP_TRAN_CD in ('6338', '6339', '6341', '6342')) or (a.EP_TRAN_CD in ('6321', '6663', '6664')and a.EP_TRAN_IND in ('6338', '6339', '6341', '6342')) 
then WK_ACTV_CLS else 0 end APP_PUB_CT_FY,
/*APP_PUB_CT_PP,*/--APP_PUB_CT_FY--DONE
case when a.EP_TRAN_CD in ('6321', '6663', '6664')and a.EP_TRAN_IND in ('6338', '6339', '6341', '6342') then WK_ACTV_CLS else 0 end APP_PUB_CC_FY,
/* APP_PUB_CC_PP,*/--APP_PUB_CC_FY,--DONE
case when (a.EP_TRAN_CD in ('6125', '6126', '6128')or (a.EP_TRAN_CD = '6332'and IU_SW = 'YES' ))and a.EP_ACTN_NUM = 1 then WK_ACTV_CLS else 0 end SOU_REJ_CR_FY,
/*SOU_REJ_CR_PP,*/--SOU_REJ_CR_FY,--DONE
case when (a.EP_TRAN_CD in ('6125', '6126', '6128') or ( a.EP_TRAN_CD = '6332'and IU_SW = 'YES' )or ( a.EP_TRAN_CD in ('6231', '6663', '6664')
and (a.EP_TRAN_IND in ('6125', '6126', '6128')or ( a.EP_TRAN_IND = '6332' and IU_SW = 'YES' ))) )and a.EP_ACTN_NUM = 1 then WK_ACTV_CLS else 0 end SOU_REJ_CT_FY,
/*SOU_REJ_CT_PP,*/--SOU_REJ_CT_FY,--DONE
case when a.EP_TRAN_CD in ('6231', '6663', '6664')and ( a.EP_TRAN_IND in ('6125', '6126', '6128')or ( a.EP_TRAN_IND = '6332'and IU_SW = 'YES') ) 
and a.EP_ACTN_NUM = 1 then WK_ACTV_CLS else 0 end SOU_REJ_CC_FY,
/*SOU_REJ_CC_PP,*/--SOU_REJ_CC_FY,--done
case when a.EP_TRAN_CD in ('6355') OR (a.EP_TRAN_CD in ('6321', '6663', '6664') and a.EP_TRAN_IND = '6355') then WK_ACTV_CLS else 0 end NSFR_CT_FY,
/*NSFR_CT_PP,*/--NSFR_CT_FY,--done
case when a.EP_TRAN_CD in ('6355') then WK_ACTV_CLS else 0 end NSFR_CR_FY,
/*NSFR_CR_PP,*/--NSFR_CR_FY,--done
case when a.EP_TRAN_CD in ('6321', '6663', '6664')and a.EP_TRAN_IND = '6355' then WK_ACTV_CLS else 0 end NSFR_CC_FY,
/* NSFR_CC_PP,*/--NSFR_CC_FY,--done
case when a.EP_TRAN_CD in ('6155') then WK_ACTV_CLS else 0 end NSFR_SOU_CR_FY,
/* NSFR_SOU_CR_PP,*/--SFR_SOU_CR_FY,--done
case when (a.EP_TRAN_CD in ('6155')OR (a.EP_TRAN_CD in ('6321', '6663', '6664') and a.EP_TRAN_IND = '6155' ) ) then WK_ACTV_CLS else 0 end NSFR_SOU_CT_FY,
/* NSFR_SOU_CT_PP,*/--NSFR_SOU_CT_FY,--done
case when a.EP_TRAN_CD in ('6321', '6663', '6664')and a.EP_TRAN_IND = '6155' then WK_ACTV_CLS else 0 end NSFR_SOU_CC_FY,
/* NSFR_SOU_CC_PP,*/--NSFR_SOU_CC_FY,--done
case when a.EP_TRAN_CD  = 6422 AND IU_SW = 'NO' AND CRT_CNT is null and NO_CRT_CNT is null and Include_6422_sw = 1 THEN 1 
WHEN A.EP_TRAN_CD = 6422 AND IU_SW = 'NO' AND CRT_CNT is null and NO_CRT_CNT is null THEN 0 
WHEN A.EP_TRAN_CD = 6422 AND Include_6422_sw = 0 Then 0 
When IU_SW = 'NO' and ( A.EP_TRAN_CD IN (6322, 6323,6324,6337,6830,6340,4025,4017,7788,6422)OR (A.EP_TRAN_CD IN (6830, 4025, 4017)) ) then WK_ACTV_CLS 
WHEN IU_SW = 'NO' and A.EP_TRAN_CD = 6323 AND A.EP_TRAN_IND > 0 then WK_ACTV_CLS 
ELSE 0 END ABAN_CR_FY,
/* ABAN_CR_PP,*/--ABAN_CR_FY,
CASE WHEN A.EP_TRAN_CD = 6422 AND IU_SW = 'NO' AND CRT_CNT is null and NO_CRT_CNT is null and Include_6422_sw = 1 THEN 1
WHEN A.EP_TRAN_CD = 6422 AND IU_SW = 'NO' AND CRT_CNT is null and NO_CRT_CNT is null THEN 0
WHEN EP_TRAN_CD = 6422 AND Include_6422_sw = 0 Then 0
WHEN A.EP_TRAN_CD IN (6321, 6663, 6664)
AND ( A.EP_TRAN_IND = 6323 AND COALESCE(PAS_INDICATOR, 'N') <> 'Y' AND IU_SW = 'YES') THEN 0 
WHEN (IU_SW = 'NO' AND (A.EP_TRAN_CD IN (6322,6323,6324,6337,6830, 6340,4025,4017, 7788,6422 ) OR (IU_SW = 'YES' AND A.EP_TRAN_CD IN (6321, 6663, 6664)
AND A.EP_TRAN_IND IN ( 6322,6323,6324,6337,6830, 6340,4025,4017,7788,6422 ) ) OR (A.EP_TRAN_CD = 6323 AND A.EP_TRAN_IND > 0) OR A.EP_TRAN_CD IN (6830, 4025, 4017)))
OR (A.EP_TRAN_CD IN (6321, 6663, 6664) AND ((A.EP_TRAN_IND = 6323 AND PAS_INDICATOR = 'Y' )OR A.EP_TRAN_IND = 6830 OR (A.EP_TRAN_CD IN (6321, 6663, 6664) 
AND A.EP_TRAN_IND IN ( 6322, 6323,6324, 6337,6340,4025,4017,6830,6422) AND IU_SW = 'YES'AND PAS_INDICATOR = 'Y') OR ( a.EP_TRAN_IND = '6324' AND IU_SW = 'NO' ) OR A.EP_TRAN_IND IN (6322, 6323, 6337, 6340, 4025, 4017, 6830, 6422)
)) THEN WK_ACTV_CLS else 0 end ABAN_CT_FY,
/* ABAN_CT_PP,*/--ABAN_CT_FY,--done
case
WHEN A.EP_TRAN_CD = 6422
AND IU_SW = 'NO'
AND CRT_CNT is null
and NO_CRT_CNT is null THEN 0
WHEN EP_TRAN_CD = 6422
AND Include_6422_sw = 0 Then 0
WHEN A.EP_TRAN_CD IN (6321, 6663, 6664)
AND (
A.EP_TRAN_IND = 6323
AND COALESCE(PAS_INDICATOR, 'N') <> 'Y'
AND IU_SW = 'YES'
) THEN 0
when a.EP_TRAN_CD in ('6321', '6663', '6664')
and (
(
a.EP_TRAN_IND = '6323'
and PAS_INDICATOR = 'Y'
)
or a.EP_TRAN_IND = '6830'
or (
a.EP_TRAN_IND in ('6322','6323','6324','6337','6340','4025','4017','6830','6422')
and IU_SW = 'YES'
and PAS_INDICATOR = 'Y'
)
OR (
a.EP_TRAN_IND = '6324'
AND IU_SW = 'NO'
)
or a.EP_TRAN_IND in ('6322','6323','6337','6340','4025','4017','6830','6422')
) then WK_ACTV_CLS
else 0
end ABAN_CC_FY,
/*ABAN_CC_PP,*/--ABAN_CC_FY,--done
CASE
WHEN a.EP_TRAN_CD in ('6138', '6139', '6141') THEN WK_ACTV_CLS
ELSE 0
END SOU_ACC_CR_FY,
/*SOU_ACC_CR_PP,*/--SOU_ACC_CR_FY,--done
CASE
WHEN (
a.EP_TRAN_CD in ('6138', '6139', '6141')
or (
a.EP_TRAN_IND in ('6138', '6139', '6141', '6142')
AND a.EP_TRAN_CD in ('6321', '6663', '6664')
)
) THEN WK_ACTV_CLS
ELSE 0
END SOU_ACC_CT_FY,
/*SOU_ACC_CT_PP,*/--SOU_ACC_CT_FY,--done
CASE
WHEN a.EP_TRAN_IND in ('6138', '6139', '6141', '6142')
AND a.EP_TRAN_CD in ('6321', '6663', '6664') THEN WK_ACTV_CLS
ELSE 0
END SOU_ACC_CC_FY,
/*SOU_ACC_CC_PP,*/--SOU_ACC_CC_FY,--done
CASE
WHEN A.EP_TRAN_CD = 6422
AND Include_6422_sw = 0 Then 0
WHEN A.EP_TRAN_CD = 6422
AND Include_6422_sw = 1 Then WK_ACTV_CLS
WHEN A.EP_TRAN_CD IN (6325,6326,6328,6329,6338,6339,6340,6341,6342,6335,6356)
AND A.EP_ACTN_NUM = 2 THEN WK_ACTV_CLS
WHEN (
A.EP_TRAN_CD IN (6325,6326,6328,6329,6338,6339,6340,6341,6342,6335,6356)
OR (
A.EP_TRAN_CD IN (6322,6323,6324,6332,6333,6337,4025,4017,6334,6115,7788,6422)
AND IU_SW = 'NO'
)
)
AND (
A.EP_ACTN_NUM = 2
OR (
A.EP_ACTN_NUM < 2
AND A.EP_TRAN_CD = 6356
)
) THEN WK_ACTV_CLS
WHEN A.EP_TRAN_CD IN (6321, 6663, 6664)
AND (
(
A.EP_TRAN_IND IN (6325,6326,6328,6329,6338,6339,6340,6341,6342)
OR (
A.EP_TRAN_IND IN (6322,6323,6324,6332,6333,6337,4025,4017,6334,6115,7788,6422)
AND IU_SW = 'NO'
)
)
)
AND A.EP_ACTN_NUM = 2 THEN WK_ACTV_CLS
ELSE 0
END A2ND_ACT_CR_FY,
/*A2ND_ACT_CR_PP,*/--A2ND_ACT_CR_FY,--done
CASE
WHEN (
a.EP_TRAN_CD in ('6321', '6663', '6664')
and a.EP_TRAN_IND = '6120'
)
or a.EP_TRAN_CD = '6120' THEN WK_ACTV_CLS
ELSE 0
END AAU_ACC_CR_FY,
/*AAU_ACC_CR_PP,*/--AAU_ACC_CR_FY,--done
CASE
WHEN A.EP_TRAN_CD = 6422
AND Include_6422_sw = 0 Then 0
WHEN A.EP_ACTN_NUM > 2
and EP_TRAN_CD = 6422
AND Include_6422_sw = 1 Then 0
WHEN A.EP_ACTN_NUM > 2
AND (
A.EP_TRAN_CD IN(6325,6326,6328,6329,6330,6338,6339,6340,6341,6830,6342,6335,6344,6356)
OR (
A.EP_TRAN_CD IN (6322,6323,6324,6332,6333,6337,6336,4025,4017,6334,6115,7788,6422)
AND IU_SW = 'NO'
)
)
OR (
A.EP_TRAN_CD IN (6321, 6663, 6664)
AND (
A.EP_TRAN_IND IN(6325,6326,6328,6329,6330,6338,6339,6340,6341,6830,6342,6335,6344,6356)
OR (
A.EP_TRAN_IND IN (6322,6323,6324,6332,6333,6337,6336,4025,4017,6334,6115,7788,6422)
AND IU_SW = 'NO'
)
)
AND (
A.EP_ACTN_NUM > 2
OR A.EP_TRAN_CD = 6115
)
) THEN WK_ACTV_CLS
ELSE 0
END SUB_ACT_CR_FY,
/*SUB_ACT_CR_PP,*/--SUB_ACT_CR_FY,--done
CASE
WHEN a.TRANSACTION_EFFECTIVE_DT > '01-OCT-21'
and a.TRANSACTION_EFFECTIVE_DT < '07-FEB-22'
AND a.EP_ACTN_NUM = 1
AND a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_CD in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_IND in ('6322', '6332', '6422')
and IU_SW = 'NO'
)
) THEN WK_ACTV_CLS
ELSE 0
END FA_INIT2_FY_CL,
 /*FA_INIT2_PP_CL,*/--FA_INIT2_FY_CL,--done
CASE
WHEN a.TRANSACTION_EFFECTIVE_DT > '01-OCT-21'
and a.TRANSACTION_EFFECTIVE_DT < '07-FEB-22'
AND a.EP_ACTN_NUM = 1
AND a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_CD in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_IND in ('6322', '6332', '6422')
and IU_SW = 'NO'
)
) THEN WK_ACTV_CLS
ELSE 0
END TOT_FA_INIT2_FY_CL,
 /*TOT_FA_INIT2_PP_CL,*/--TOT_FA_INIT2_FY_CL,--done
CASE
WHEN a.TRANSACTION_EFFECTIVE_DT > '01-OCT-21'
and a.TRANSACTION_EFFECTIVE_DT < '07-FEB-22'
AND a.EP_ACTN_NUM = 1
AND a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_IND in ('6322', '6332', '6422')
and IU_SW = 'NO'
)
) THEN WK_ACTV_CLS
ELSE 0
END FA_INIT2_CORR_FY_CL,
 /*FA_INIT2_CORR_PP_CL,*/--FA_INIT2_CORR_FY_CL,--done
case
WHEN a.TRANSACTION_EFFECTIVE_DT > '01-OCT-21'
and a.TRANSACTION_EFFECTIVE_DT < '07-FEB-22'
AND a.EP_ACTN_NUM = 1
AND a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_IND in ('6322', '6332', '6422')
and IU_SW = 'NO'
)
) THEN WK_ACTV_CLS
ELSE 0
END TOT_FA_INIT2_CORR_FY_CL,
/* TOT_FA_INIT2_CORR_PP_CL,*/--TOT_FA_INIT2_CORR_FY_CL,--done

CASE
WHEN a.EP_ACTN_NUM = 1
AND (
a.EP_TRAN_CD in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_CD in ('6322', '6332', '6422')
and IU_SW = 'NO'
) 
) THEN WK_ACTV_CLS
ELSE 0
END FA_INIT_FY_CL,
/*FA_INIT_PP_CL,*/--FA_INIT_FY_CL,--done
CASE
WHEN A.EP_TRAN_CD = 6422
AND A.EP_ACTN_NUM = 1
AND IU_SW = 'NO'
AND CRT_CNT is null
and NO_CRT_CNT is null THEN WK_ACTV_CLS
WHEN A.EP_TRAN_CD = 6422
AND A.EP_ACTN_NUM = 1
AND Include_6422_sw = 0 Then 0
WHEN A.EP_ACTN_NUM = 1
and EP_TRAN_CD = 6422
AND Include_6422_sw = 1 Then 0
WHEN A.EP_ACTN_NUM = 1
AND (
A.EP_TRAN_CD IN (6325, 6326, 6328, 6338, 6339, 7777)
OR (
A.EP_TRAN_CD IN (6322, 6332, 6422)
AND IU_SW = 'NO'
)
OR (
A.EP_TRAN_CD IN (6321, 6663, 6664)
AND (
A.EP_TRAN_IND IN (6325, 6326, 6328, 6338, 6339, 7777)
OR (
A.EP_TRAN_IND IN (6322, 6332, 6422)
AND IU_SW = 'NO'
)
)
)
) THEN WK_ACTV_CLS
ELSE 0
END TOT_FA_INIT_FY_CL,
/*TOT_FA_INIT_PP_CL,*/--TOT_FA_INIT_FY_CL,--done
CASE
WHEN a.EP_ACTN_NUM = 1
AND (
a.EP_TRAN_CD in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_CD in ('6322', '6332', '6422')
and IU_SW = 'NO'
)
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_IND in ('6322', '6332', '6422')
AND IU_SW = 'NO'
)
)
)
) THEN WK_ACTV_CLS
ELSE 0
END FA_INIT_CORR_FY_CL_TEM1,
/*FA_INIT_CORR_PP_CL,*/--FA_INIT_CORR_FY_CL,--done
CASE
WHEN a.EP_ACTN_NUM = 1
AND a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_IND in ('6322', '6332', '6422')
and IU_SW = 'NO'
)
) THEN WK_ACTV_CLS
ELSE 0
END FA_INIT_CORR_FY_CL,

CASE
WHEN a.EP_ACTN_NUM = 1
AND NOT (
a.EP_SER_NUM > 78999999
AND a.EP_SER_NUM < 80000000
)
AND (
a.EP_TRAN_CD in ('6338', '6339')
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND a.EP_TRAN_IND in ('6338', '6339')
)
) THEN WK_ACTV_CLS
ELSE 0
END FA_PUBS_FY_CL,
 /* FA_PUBS_PP_CL,*/--FA_PUBS_FY_CL,--done
--FA_PUB
CASE
WHEN a.TRANSACTION_EFFECTIVE_DT > '01-OCT-21'
and a.TRANSACTION_EFFECTIVE_DT < '07-FEB-22'
AND a.EP_ACTN_NUM = 1
AND(
(
a.EP_TRAN_CD in ('6125', '6126', '6128', '6138', '6139')
OR (
a.EP_TRAN_CD in ('6322', '6332', '6422')
AND IU_SW = 'YES'
)
)
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6125', '6126', '6128', '6138', '6139')
OR (
a.EP_TRAN_IND in ('6322', '6422')
AND IU_SW = 'YES'
)
)
)
) --AND a.EP_SER_NUM > 78999999 AND and a.EP_SER_NUM < 80000000
THEN WK_ACTV_CLS
ELSE 0
END NFA_SOU2_CR_FY,
/*NFA_SOU2_CR_PP,*/--NFA_SOU2_CR_FY,--done
CASE
WHEN a.EP_ACTN_NUM = 1
AND(
(
a.EP_TRAN_CD in ('6125', '6126', '6128', '6138', '6139')
OR (
a.EP_TRAN_CD in ('6322', '6332', '6442')
AND IU_SW = 'YES'
)
)
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6125', '6126', '6128', '6138', '6139')
OR (
a.EP_TRAN_IND in ('6322', '6422')
AND IU_SW = 'YES'
)
)
)
) THEN WK_ACTV_CLS
ELSE 0
END NFA_SOU_CR_FY,
/* NFA_SOU_CR_PP,*/--NFA_SOU_CR_FY,--done
CASE
WHEN A.EP_TRAN_CD = 6422
AND Include_6422_sw = 0 Then 0
WHEN (
(
a.EP_TRAN_CD in ('6322','6323','6324','6337','4025','4017','6115','6130','6144','6156','7788','6422')
AND IU_SW = 'YES'
)
OR (
a.EP_TRAN_CD in ('6125','6126','6128','6129','6130','6138','6139','6141','6142')
OR (
a.EP_TRAN_CD in ('6332', '6333', '6334')
AND IU_SW = 'YES'
)
OR (
a.EP_TRAN_CD = '6336'
AND a.EP_ACTN_NUM > 2
)
)
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND a.EP_TRAN_IND in ('6322','6323','6324','6337','4025','4017','6115','6130','6144','6156','6422')
AND IU_SW = 'YES'
)
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND a.EP_TRAN_IND in ('6125','6126','6128','6129','6130','6138','6139','6141','6142')
OR (
a.EP_TRAN_IND in ('6332', '6333', '6334')
AND IU_SW = 'YES'
)
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND a.EP_TRAN_IND = '6336'
AND a.EP_ACTN_NUM > 2
)
)
) THEN WK_ACTV_CLS
ELSE 0
END TOT_SOU_CR_FY,
/*TOT_SOU_CR_PP,*/--TOT_SOU_CR_FY,--done
CASE
WHEN a.EP_TRAN_CD = '6329' THEN WK_ACTV_CLS
ELSE 0
END FIN_REFI_CR_FY,
/*FIN_REFI_CR_PP,*/--FIN_REFI_CR_FY,--done
CASE
WHEN a.EP_TRAN_CD = '6329'
OR (
a.EP_TRAN_IND = '6329'
AND a.EP_TRAN_CD in ('6321', '6663', '6664')
) THEN WK_ACTV_CLS
ELSE 0
END FIN_REFI_CT_FY,
/* FIN_REFI_CT_PP,*/--FIN_REFI_CT_FY,--done
CASE
WHEN a.EP_TRAN_IND = '6329'
AND a.EP_TRAN_CD in ('6321', '6663', '6664') THEN WK_ACTV_CLS
ELSE 0
END FIN_REFI_CC_FY,
/*FIN_REFI_CC_PP,*/--FIN_REFI_CC_FY,--done
CASE
WHEN a.EP_TRAN_CD = '6129' THEN WK_ACTV_CLS
ELSE 0
END FIN_REFS_CR_FY,
/*FIN_REFS_CR_PP,*/--FIN_REFS_CR_FY,--done
CASE
WHEN a.EP_TRAN_CD = '6129'
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
and a.EP_TRAN_IND = '6129'
) THEN WK_ACTV_CLS
ELSE 0
END FIN_REFS_CT_FY,
 /* FIN_REFS_CT_PP,*/--FIN_REFS_CT_FY,--done
CASE
WHEN a.EP_TRAN_CD in ('6321', '6663', '6664')
and a.EP_TRAN_IND = '6129' THEN WK_ACTV_CLS
ELSE 0
END FIN_REFS_CC_FY,
/*FIN_REFS_CC_PP,*/--FIN_REFS_CC_FY,--done
CASE
WHEN (
(
a.EP_TRAN_CD in ('6328', '6128', '6126', '6326')
AND EP_FLG_PRIORITY = 'N'
)
OR (
(
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6328', '6128')
OR (
a.EP_TRAN_IND in ('6126', '6326')
AND EP_FLG_PRIORITY = 'N'
)
)
)
)
) THEN WK_ACTV_CLS
ELSE 0
END NEA_FY,
 /* NEA_PP,*/--NEA_FY,--done
CASE
WHEN a.EP_SER_NUM > 78999999
AND a.EP_SER_NUM < 80000000 THEN WK_ACTV_CLS
ELSE 0
END A66A2_FY,
 /*A66A2_PP,*/--A66A2_FY,--done
CASE
WHEN a.EP_SER_NUM > 78999999
AND a.EP_SER_NUM < 80000000
AND a.EP_ACTN_NUM = 1
AND (
a.EP_TRAN_CD in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_CD in ('6322', '6332', '6422')
and IU_SW = 'NO'
)
OR (
a.EP_TRAN_CD in ('6321', '6663', '6664')
AND (
a.EP_TRAN_IND in ('6325', '6326', '6328', '6338', '6339', '7777')
OR (
a.EP_TRAN_IND in ('6322', '6332', '6422')
AND IU_SW = 'NO'
)
)
)
) THEN WK_ACTV_CLS
ELSE 0
END A66A_FY,
/*A66A_PP,*/--A66A_FY,--done
 WORKER_NAME,
' All Examiners' all_ex,
production_credit_tran_id,
Tran_action_type
FROM
(
select
* --,
--(CASE WHEN EP_PP_PERIOD = SUBSTR(<Parameters.Pay Period>,5,6) THEN 1 ELSE 0 END) AS FY_PROCESS_SW
FROM
trm_reporting{env}.silver.epquery_stg1 -- WHERE ((EP_PP_PERIOD between substr(<Parameters.Pay Period>, 5,4) || '01' and substr(<Parameters.Pay Period>, 5,6)
--) OR ( <Parameters.Pay Period> = 'None PP' and TRANSACTION_EFFECTIVE_DT between <Parameters.Start Date> and<Parameters.End Date>))
) A
""")

# COMMAND ----------

df_epquery_stg2.write.mode("overwrite").format("delta").saveAsTable(f'trm_reporting{env}.silver.epquery_stg2')

# COMMAND ----------

df_epquery_stg3 = spark.sql(f"""
SELECT *,
  WORKER_NAME || ' ' || DN_WORKER_NO as `Examining_Attorney`,
  substr(AM_MARK, 1, 40) AS Mark,
  APP_PUB_CT_FY + ABAN_CT_FY + TOT_FA_INIT_FY_CL `Total_BDS`,
  CASE
    WHEN EP_TRAN_CD = '6321' THEN 'Y'
    WHEN EP_TRAN_CD in ('6663', '6664') THEN 'R'
    else ' '
  END `Correction`,
  CASE
  WHEN EP_TRAN_CD = '7788' THEN Tran_action_type
  WHEN EP_TRAN_CD in ('4025', '4017')  OR EP_TRAN_IND in (4025, 4017) THEN 'Abandoned - Death of IR'
  WHEN EP_TRAN_CD = '6139'  AND EP_TRAN_IND = '6346' THEN 'Allowance For Registration After Final'
  WHEN EP_TRAN_CD = '6142'  AND EP_TRAN_IND = '6346' THEN 'Allow For Reg And Aban After Final'
  WHEN EP_TRAN_IND in ('6347') THEN 'Publication Approval After Final-Prin Reg'
  WHEN EP_TRAN_CD in ('6339', '6342')  OR EP_TRAN_IND in ('6346') THEN 'Publication Approval After Final-Supl Reg'
 -- WHEN EP_TRAN_CD in ('6140','6141','6142','6322','6323','6324','6337','6340','6341','6342','6830','7777','7788','4025','4017','6422'  )
  WHEN EP_TRAN_CD in ('6140','6141','6142','6322','6323','6324','6337','6340','6341','6342','6830','7788','4025','4017','6422'  ) -- Removed 7777 from this line Jesse Adorjan
  OR EP_TRAN_IND in (6344) THEN 'Abandonment After Final Refusal'
  WHEN EP_TRAN_CD = '7777' THEN 'Retoactive First Action Credit' -- Add this line to name 7777 Jesse ADorjan
  WHEN (EP_TRAN_CD = '6126' OR EP_TRAN_IND = '6126' ) AND EP_FLG_PRIORITY = 'N' THEN 'Combined EA/PA - SOU Exam'
  WHEN (EP_TRAN_CD = '6326' OR EP_TRAN_IND = '6326')  AND EP_FLG_PRIORITY = 'N' THEN 'Combined EA/PA'  
  WHEN (EP_TRAN_CD = '6344' OR ( EP_TRAN_CD = '6321'  AND EP_TRAN_IND = '6344' )) THEN 'Request For Consideration Denied'
  WHEN PAS_INDICATOR = 'Y' AND EP_TRAN_IND IN ('6322','6323','6324','6337','6340','4025','4017','6422') THEN 'Abandonment After Final Refusal'
  WHEN PAS_INDICATOR = 'Y'  AND EP_TRAN_IND IN ('6338', '6341') THEN 'Publication Approval After Final-Prin Reg'
  WHEN PAS_INDICATOR = 'Y'  AND EP_TRAN_IND IN ('6339', '6342') THEN 'Publication Approval After Final-Supl Reg'
  WHEN EP_TRAN_IND = '6338'  OR EP_TRAN_CD = '6338' THEN 'Approval for Pub (PR)'
  WHEN EP_TRAN_CD = '6155' THEN  regexp_replace(
    Tran_action_type,
    '�',
    ""
  ) 
  ELSE
  regexp_replace(
    Tran_action_type,
    '�',
    "'"
  ) 
  END `Action_Type`,
substr((CREATE_TS), 11, 8) ||' '||    substr((CREATE_TS), 27, 2) `Action_Time`,
CASE --WHEN EP_TRAN_CD = '7777' THEN 'ADCL'
WHEN  EP_TRAN_CD = '7788' THEN 'DROP'
WHEN EP_TRAN_CD IN (6321, 6663, 6664) THEN EP_TRAN_IND
ELSE EP_TRAN_CD END `Tran_Code`, 
APP_PUB_CT_FY + ABAN_CT_FY + TOT_FA_INIT_FY_CL `Total_Balance_Disposals_Y`,
TOT_FA_INIT_FY_CL + SUB_ACT_CR_FY + A2ND_ACT_CR_FY + NSFR_CT_FY + TOT_SOU_CR_FY + NSFR_SOU_CT_FY `Total_Action_Counts_Y`
,TOT_FA_INIT_FY_CL - FA_PUBS_FY_CL - A66A_FY  `Net_First_Actions_Modified_Y`
,TOT_FA_INIT_FY_CL+ SUB_ACT_CR_FY + A2ND_ACT_CR_FY NET_ACT_CR_FY
,case when tran_month between  10 and 12 THEN 'First Quarter BDs'
            WHEN tran_month between 1 AND  3 THEN 'Second Quarter BDs'
           WHEN tran_month between 4 AND 6 THEN 'Third Quarter BDs'
           WHEN tran_month between 7 AND 9 THEN 'Fourth Quarter BDs'
          END QTR,
case when tran_month between  10 and 12 THEN '1'
            WHEN tran_month between 1 AND  3 THEN '2'
           WHEN tran_month between 4 AND 6 THEN '3'
           WHEN tran_month between 7 AND 9 THEN '4'
  END QTR_no

FROM
  trm_reporting{env}.silver.epquery_stg2
  WHERE
   (APP_PUB_CT_FY + ABAN_CT_FY + TOT_FA_INIT_FY_CL <> 0
    OR ( TOT_FA_INIT_FY_CL + SUB_ACT_CR_FY + A2ND_ACT_CR_FY + NSFR_CT_FY + TOT_SOU_CR_FY + NSFR_SOU_CT_FY ) <> 0 ) 
    and WORKER_NAME is not null
  """)

# COMMAND ----------

df_epquery_stg3.write.mode("overwrite").format("delta").saveAsTable(f'trm_reporting{env}.silver.epquery_stg3')

# COMMAND ----------

end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', 0,"job completed successfully")

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading epquery_stg Tables ")

# COMMAND ----------



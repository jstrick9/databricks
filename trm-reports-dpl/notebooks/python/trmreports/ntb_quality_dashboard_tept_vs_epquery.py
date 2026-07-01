# Databricks notebook source
# MAGIC %md
# MAGIC # EPQUERY

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['reporting_catalog']
altrx_catalog = common_configs['schema']['altrx_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
tqr_catalog = common_configs['schema']['tqr_catalog']
tept_catalog = common_configs['schema']['tept_catalog']
tmworker_catalog = common_configs['schema']['tmworker_catalog']
tmprodvty_catalog = common_configs['schema']['tmprodvty_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
if dbx_env.lower() !='prod':
    env = '_'+dbx_env.lower()
else:
    env =''
print(env)

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_quality_dashboard_tept_vs_epquery'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

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
            WHEN pa.Productivity_action_cd = 6422 AND NO_CRT_CNT = 0 AND CRT_CNT = 0 THEN 1
            WHEN pa.Productivity_action_cd = 6422 AND INTENT_TO_USE_DT is not null THEN 0
            WHEN pa.Productivity_action_cd = 6422 AND NO_CRT_CNT > 0 THEN 0
            WHEN pa.Productivity_action_cd = 6422 AND CRT_CNT > 0 THEN 1
            ELSE 0
        END INCLUDE_6422_SW_TEST,
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
df_epquery_stg2.createOrReplaceTempView("temp_df_epquery_stg2")

# COMMAND ----------

# MAGIC %md
# MAGIC # TEPT

# COMMAND ----------

job_name = 'ntb_quality_dashboard_tept_vs_epquery'
job_start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')

#control_dt = begin_job_cntl(f'{tept_catalog}.silver', job_name, job_start_ts)
#print(f'{control_dt=}')

# COMMAND ----------

spark.sql(f"""
CREATE
OR REPLACE TEMP VIEW temp_fiscal_year AS
select
  distinct A.EMPLOYEE_NO,
  B.fiscal_year_no,
  B.quarter_no
from
  {tept_catalog}.bronze.employee_fiscal_year AS A,
  {tept_catalog}.bronze.stnd_fiscal_year_quarter AS B
where
  A.FISCAL_YEAR_NO = B.FISCAL_YEAR_NO
  AND cast(from_utc_timestamp(current_timestamp(), 'US/Eastern') as date) between B.quarter_start_dt  and B.quarter_end_dt
""")

# COMMAND ----------

fy_yr_df=spark.sql("""SELECT DISTINCT FISCAL_YEAR_NO FROM temp_fiscal_year""")
fy_yr=fy_yr_df.collect()[0][0]
print(f'{fy_yr=}')

# COMMAND ----------

min_fy_start_dt_df = spark.sql(f"""select cast(min(quarter_bi_week_start_dt) as string) from {tept_catalog}.bronze.stnd_quarter_bi_week as b where fiscal_year_no={fy_yr} """ )

min_dt = min_fy_start_dt_df.collect()[0][0]  

print(f'{min_dt=}')

# COMMAND ----------

max_pp_end_dt_df = spark.sql(f"""select cast(max(quarter_bi_week_end_dt) as string) from {tept_catalog}.bronze.stnd_quarter_bi_week as b where fiscal_year_no={fy_yr} and cast(from_utc_timestamp(current_timestamp(), 'US/Eastern') as date) between b.quarter_bi_week_start_dt  and b.quarter_bi_week_end_dt""")

max_dt = max_pp_end_dt_df.collect()[0][0] 

print(f'{max_dt=}') 

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW temp_fy_qrt_wid
AS select distinct 
						a.employee_no,
						b.fiscal_year_no,
						b.quarter_no,
						b.quarter_bi_week_id,
						b.quarter_bi_week_start_dt,
						b.quarter_bi_week_end_dt 
		from  temp_fiscal_year as a
        join {tept_catalog}.bronze.stnd_quarter_bi_week as b
		on a.fiscal_year_no=b.fiscal_year_no
""")

# COMMAND ----------

bi_weeks = spark.sql(f"""
select fiscal_year_no, 
quarter_no,
quarter_bi_week_id,
explode(sequence(quarter_bi_week_start_dt, quarter_bi_week_end_dt, interval 1 day)) as work_date
from {tept_catalog}.bronze.stnd_quarter_bi_week
where fiscal_year_no = {fy_yr}
""")

bi_weeks.createOrReplaceTempView("bi_weeks")

# COMMAND ----------

#Create temp table ept_data
ept_load_df = spark.sql(f"""SELECT DISTINCT cast(EP.EP_EXMR_NUM as integer) EP_EXMR_NUM,
                cast(EP.EP_FY_PP as integer) EP_FY_PP,
                bws.quarter_bi_week_id as bi_week_id,
                CAST(REGEXP_SUBSTR(EP.CFK_OBJECT_GID, '[^:]+$') AS INTEGER) AS EP_SER_NUM,
                EP.EP_EXMR_LO,
                EE_EMPE_NAM,
                cast(EP.EP_TRAN_IND as integer) EP_TRAN_IND,
                cast(EP.EP_TRAN_CD as integer) EP_TRAN_CD,
                cast(EP.EP_ACTN_NUM as integer) EP_ACTN_NUM,
                EP.EP_ACTN_CREDIT,
                EP.EP_ACTN_CT_DT,
                EP.EP_FLG_PRIORITY,
                SUBSTR(EE_EMPE_LO, 3, 1) LO,
                EP.EE_EMPE_LO EE_LO,
                TT_TEXT_1 AS law_office,
                EP_SYS_TI,
                EP_SYS_DT,
                CASE 
                WHEN INTENT_TO_USE_DT IS NOT NULL AND EP.TRANSACTION_EFFECTIVE_DT > IU.CREATE_TS THEN 'YES'
                ELSE 'NO' 
                END IU_SW,
                CASE
                WHEN EP.EP_TRAN_CD = 6422 AND NO_CRT_CNT = 0 AND CRT_CNT = 0 THEN 1
                WHEN EP.EP_TRAN_CD = 6422 AND INTENT_TO_USE_DT is not null THEN 0
                WHEN EP.EP_TRAN_CD = 6422 AND NO_CRT_CNT > 0 THEN 0
                WHEN EP.EP_TRAN_CD = 6422 AND CRT_CNT > 0 THEN 1
                ELSE 0
                END Include_6422_sw,
                CASE 
                WHEN EP.EP_TRAN_CD IN (6321, 6663, 6664) THEN EP_ACTN_CREDIT * -1
                ELSE EP_ACTN_CREDIT
                END WK_ACTV_CLS
                FROM
                (
                SELECT distinct WRKR.WORKER_NO AS EE_EMPE_NUM,
                WRKR.WORKER_NM AS EE_EMPE_NAM,
                LC.CFK_ASGND_EXAM_LAW_OFC_ORG_CD as EP_EXMR_LO,
                STL.TT_TEXT as TT_TEXT_1,
                TRAN.DN_WORKER_NO AS EP_EXMR_NUM,
                SUBSTR(tran.cfk_bcr_pay_period_range_name, 4) AS EP_FY_PP,
                TRAN.CFK_OBJECT_GID,
                TRAN.DN_WORKER_TM_ORGANIZATION_CD AS EE_EMPE_LO,
                ACT_SUB.PRODUCTIVITY_ACTION_CD AS EP_TRAN_IND,
                ACT.PRODUCTIVITY_ACTION_CD AS EP_TRAN_CD,
                TRAN.DN_ACTION_NO AS EP_ACTN_NUM,
                TRAN.UNIT_COUNT_NO AS EP_ACTN_CREDIT,
                TRAN.TRANSACTION_EFFECTIVE_DT,
                CAST(date_format(TRAN.TRANSACTION_EFFECTIVE_DT, 'yyyyMMdd') AS INTEGER ) AS EP_ACTN_CT_DT,
                CASE
                WHEN TRAN.PRIORITY_IN = 'N' THEN 0
                WHEN TRAN.PRIORITY_IN = 'Y' THEN 1
                END AS EP_FLG_PRIORITY,
                CAST(date_format(TRAN.LAST_MOD_TS, 'yyyyMMdd') AS INTEGER) AS EP_SYS_DT,
                CAST(date_format(TRAN.LAST_MOD_TS, 'Hms') AS INTEGER) AS EP_SYS_TI
                FROM
                {tmworker_catalog}.BRONZE.WORKER WRKR
                INNER JOIN (SELECT DISTINCT FK_WORKER_GID, FK_TM_ORGANIZATION_GID, ROW_NUMBER() OVER (PARTITION BY FK_WORKER_GID  ORDER BY BEGIN_EFFECTIVE_DT DESC) R_NUM FROM
                {tmworker_catalog}.BRONZE.WORKER_ROLE) WR ON WRKR.WORKER_GID = WR.FK_WORKER_GID AND R_NUM=1
                INNER JOIN {tmprodvty_catalog}.BRONZE.PRODUCTION_TRANSACTION TRAN ON WRKR.WORKER_NO = TRAN.DN_WORKER_NO and TRAN.DELETE_IN ='N'
                INNER JOIN (SELECT DN_WORKER_NO DNWORKERNO, CFK_OBJECT_GID, ROW_NUMBER() OVER (PARTITION BY DN_WORKER_NO ORDER BY PRODUCTION_CREDIT_TRAN_ID DESC) ROWNO
                FROM {tmprodvty_catalog}.BRONZE.PRODUCTION_TRANSACTION WHERE DELETE_IN ='N' ) lo_filt
                ON TRAN.DN_WORKER_NO = LO_FILT.DNWORKERNO
                AND ROWNO = 1 AND SUBSTR(DN_WORKER_TM_ORGANIZATION_CD,1,2) = 'LO'
                INNER JOIN (
                SELECT * FROM {tmworker_catalog}.BRONZE.TM_Organization
                -- WHERE SUBSTR(ORGANIZATION_CD, 1, 2) = 'LO'
                ) ORG ON WR.FK_TM_ORGANIZATION_GID = ORG.TM_ORGANIZATION_GID
                INNER JOIN {tmprodvty_catalog}.BRONZE.PRODUCTIVITY_ACTION ACT ON TRAN.FK_GENERATING_PRODVTY_ACTN_ID = ACT.PRODUCTIVITY_ACTION_ID
                INNER JOIN {tmprodvty_catalog}.BRONZE.PRODUCTIVITY_ACTION ACT_SUB ON NVL(TRAN.FK_CORRECTED_PRODVTY_ACTN_ID, 0) = ACT_SUB.PRODUCTIVITY_ACTION_ID
                LEFT JOIN {tmngpdb_catalog}.BRONZE.TM_LOCATIONS LC ON lo_filt.CFK_OBJECT_GID = LC.fk_trademark_gid
                LEFT JOIN {tmworker_catalog}.BRONZE.SYNC_TRANSLATE_LOCATION STL ON LC.CFK_ASGND_EXAM_LAW_OFC_ORG_CD = STL.LAW_OFFICE_CD
                ) EP
                LEFT JOIN (
                select fk_trademark_gid, potentiel_abandonment_dt INTENT_TO_USE_DT, create_ts
                from {tmngpdb_catalog}.BRONZE.TM_ITU
                ) IU ON EP.CFK_OBJECT_GID = IU.fk_trademark_gid
                LEFT JOIN (
                select distinct TRAN.CFK_OBJECT_GID,COUNT(*) NO_CRT_CNT
                from {tmprodvty_catalog}.BRONZE.PRODUCTION_TRANSACTION TRAN
                INNER JOIN {tmprodvty_catalog}.BRONZE.PRODUCTIVITY_ACTION ACT ON TRAN.FK_GENERATING_PRODVTY_ACTN_ID = ACT.PRODUCTIVITY_ACTION_ID
                WHERE ACT.PRODUCTIVITY_ACTION_CD in (6322, 6323, 6324, 6338, 6339)
                GROUP BY TRAN.CFK_OBJECT_GID
                ) NO_CRT ON EP.CFK_OBJECT_GID = NO_CRT.CFK_OBJECT_GID
                LEFT JOIN (
                select distinct TRAN.CFK_OBJECT_GID,COUNT(*) CRT_CNT
                from {tmprodvty_catalog}.BRONZE.PRODUCTION_TRANSACTION TRAN
                INNER JOIN {tmprodvty_catalog}.BRONZE.PRODUCTIVITY_ACTION ACT ON TRAN.FK_GENERATING_PRODVTY_ACTN_ID = ACT.PRODUCTIVITY_ACTION_ID
                WHERE ACT.PRODUCTIVITY_ACTION_CD in (
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
                )
                GROUP BY TRAN.CFK_OBJECT_GID
                ) CRT ON EP.CFK_OBJECT_GID = CRT.CFK_OBJECT_GID
                left join bi_weeks bws on date_format(EP.TRANSACTION_EFFECTIVE_DT, 'y-MM-d') = bws.work_date
    """)

ept_load_df.createOrReplaceTempView("temp_ept_data")
print('Created Temp Table temp_ept_data')

# COMMAND ----------

#Create Temp Table pas_data

pas_load_df=spark.sql(f"""SELECT EP1.EP_FY_PP, EP1.EP_EXMR_NUM, EP1.EP_SER_NUM, EP1.EP_TRAN_CD, EP1.EP_TRAN_IND,
                    EP1.EP_ACTN_NUM, cast(EP1.EP_TRAN_IND as varchar(12)) AS PAS_ENT_KEY, EP1.EP_ACTN_CT_DT AS PAS_PR_DT_STAT, EP1.EP_ACTN_CT_DT,
                    CASE WHEN EP1.SUBSEQUENT_ACTION_IN = 'Y' THEN 'YES' ELSE 'NO' END PAS_INDICATOR
                    FROM (SELECT CAST(TRAN.DN_WORKER_NO AS INTEGER) AS EP_EXMR_NUM,
                    CAST(SUBSTR(tran.cfk_bcr_pay_period_range_name, 4) AS INTEGER) AS EP_FY_PP,
                    CAST(REGEXP_SUBSTR(TRAN.CFK_OBJECT_GID, '[^:]+$') AS INTEGER) AS EP_SER_NUM, 
                    CAST(ACT_SUB.PRODUCTIVITY_ACTION_CD AS INTEGER) AS EP_TRAN_IND,
                    CAST(ACT.PRODUCTIVITY_ACTION_CD AS INTEGER) AS EP_TRAN_CD,
                    CAST(TRAN.DN_ACTION_NO AS INTEGER) AS EP_ACTN_NUM,
                    CAST(date_format(TRAN.TRANSACTION_EFFECTIVE_DT, 'yyyyMMdd') AS INTEGER ) AS EP_ACTN_CT_DT, SUBSEQUENT_ACTION_IN, TRANSACTION_EFFECTIVE_DT
                    FROM {tmprodvty_catalog}.BRONZE.PRODUCTION_TRANSACTION TRAN
                    INNER JOIN {tmprodvty_catalog}.BRONZE.PRODUCTIVITY_ACTION ACT ON TRAN.FK_GENERATING_PRODVTY_ACTN_ID = ACT.PRODUCTIVITY_ACTION_ID
                    INNER JOIN {tmprodvty_catalog}.BRONZE.PRODUCTIVITY_ACTION ACT_SUB ON NVL(TRAN.FK_CORRECTED_PRODVTY_ACTN_ID, 0) = ACT_SUB.PRODUCTIVITY_ACTION_ID
                    WHERE TRAN.DELETE_IN ='N') EP1
                    WHERE EP1.TRANSACTION_EFFECTIVE_DT >= to_date('{min_dt}', 'yyyy-MM-dd') and EP1.TRANSACTION_EFFECTIVE_DT < date_add(to_date('{max_dt}', 'yyyy-MM-dd'), 1) 
                    AND SUBSEQUENT_ACTION_IN = 'Y'""")

pas_load_df.createOrReplaceTempView("temp_pas_data")
print("Create temp table temp_pas_data ")	

# COMMAND ----------

from pyspark.sql.functions import collect_list, concat_ws
#Join temp table ept_data and pas_data to create "temp_data" temp table
temp_ept_pas_df= spark.sql(""" 
                        SELECT  DISTINCT A.EP_EXMR_NUM,a.bi_week_id, 
                            A.EP_FY_PP, A.EP_SER_NUM, A.EP_EXMR_LO,  PAS_INDICATOR,  EE_LO,A.EP_ACTN_CT_DT, EP_SYS_TI,EE_EMPE_NAM,
                            LO,law_office,
                            A.EP_ACTN_NUM,
                            A.EP_TRAN_CD,
                            A.EP_TRAN_IND, IU_SW,
                            CASE WHEN A.EP_TRAN_CD IN (  6338, 6339, 6341, 6342)
                            THEN WK_ACTV_CLS
                            WHEN A.EP_TRAN_CD IN (6321, 6663, 6664) AND A.EP_TRAN_IND IN ( 6338, 6339,6341,6342)
                            THEN WK_ACTV_CLS 
                            ELSE 0 
                            END APP_PUB_CT_FY,
                            CASE WHEN A.EP_TRAN_CD IN (6355) 
                            OR (A.EP_TRAN_CD IN (6321, 6663, 6664) AND A.EP_TRAN_IND = 6355)
                            THEN 
                            WK_ACTV_CLS 
                            ELSE 0 
                            END NSFR_CT_FY,
                            CASE WHEN (A.EP_TRAN_CD IN (6155) OR
                            (A.EP_TRAN_CD IN (6321, 6663, 6664) AND A.EP_TRAN_IND = 6155))  
                            THEN WK_ACTV_CLS 
                            ELSE 0  
                            END NSFR_SOU_CT_FY,
                            CASE 
                            WHEN A.EP_TRAN_CD IN (6321, 6663, 6664) AND (A.EP_TRAN_IND = 6323 AND COALESCE(PAS_INDICATOR, 'NO') <> 'YES' AND IU_SW = 'YES')
                            THEN 0
                            WHEN A.EP_TRAN_CD IN (6321, 6663, 6664) AND A.EP_TRAN_IND = 6324 AND IU_SW = 'YES' 
                            THEN 0
                            WHEN A.EP_TRAN_CD = 6422 AND  Include_6422_sw = 0 Then 0
                            WHEN (IU_SW = 'NO' AND (A.EP_TRAN_CD IN (6322, 6323, 6324, 6337, 6830, 6340,4025, 4017, 7788, 6422) OR 
                            (IU_SW = 'YES' AND A.EP_TRAN_CD IN (6321, 6663, 6664) AND A.EP_TRAN_IND IN (6322, 6323, 6324, 6337, 6830,6340,  4025, 4017, 7788, 6422)) 
                            OR (A.EP_TRAN_CD = 6323 AND A.EP_TRAN_IND > 0) OR A.EP_TRAN_CD IN (6830, 4025, 4017))) OR (A.EP_TRAN_CD IN (6321, 6663, 6664) AND ((A.EP_TRAN_IND = 6323 AND PAS_INDICATOR = 'YES') OR A.EP_TRAN_IND = 6830
                            OR (A.EP_TRAN_IND IN (6322, 6323, 6324, 6337, 6340, 4025, 4017, 6830, 6422) AND IU_SW = 'YES' AND PAS_INDICATOR = 'YES') 
                            OR A.EP_TRAN_IND IN (6322, 6323, 6324, 6337, 6340, 4025, 4017, 6830, 6422)))
                            THEN WK_ACTV_CLS ELSE 0 END ABAN_CT_FY,
                            CASE 
                            WHEN A.EP_TRAN_CD = 6422 AND  Include_6422_sw = 0 Then 0
                            WHEN A.EP_TRAN_CD IN (6325, 6326, 6328, 6329, 6338, 6339, 6340, 6341, 6342, 6335, 6356) AND A.EP_ACTN_NUM = 2
                            THEN WK_ACTV_CLS
                            WHEN (A.EP_TRAN_CD IN (6325, 6326, 6328, 6329, 6338, 6339, 6340, 6341, 6342, 6335, 6356)
                            OR (  A.EP_TRAN_CD IN (6322, 6323, 6324, 6332, 6333, 6337, 4025, 4017, 6334, 6115, 7788, 6422)   
                            AND IU_SW = 'NO')) AND (A.EP_ACTN_NUM = 2 OR (A.EP_ACTN_NUM < 2 AND A.EP_TRAN_CD = 6356))
                            THEN WK_ACTV_CLS
                            WHEN A.EP_TRAN_CD IN (6321, 6663, 6664) AND ((A.EP_TRAN_IND IN (6325, 6326, 6328, 6329, 
                            6338, 6339, 6340, 6341, 6342) OR 
                            (A.EP_TRAN_IND IN (6322, 6323, 6324, 6332, 6333, 6337, 4025, 4017, 6334, 6115, 7788, 6422)
                            AND IU_SW = 'NO'))) AND A.EP_ACTN_NUM = 2 
                            THEN WK_ACTV_CLS
                            ELSE 0 END A2ND_ACT_CR_FY,
                            CASE WHEN  A.EP_ACTN_NUM > 2 
                            AND ((A.EP_TRAN_CD IN(6325, 6326, 6328, 6329, 6330, 6338, 6339, 6340, 6341, 6830, 6342, 6335, 6344, 6356) 
                            OR (A.EP_TRAN_CD IN (6322, 6323, 6324, 6332, 6333, 6337, 6336, 4025, 4017, 6334, 6115, 7788, 6422) AND IU_SW = 'NO'))
                            OR ( A.EP_TRAN_CD IN (6321, 6663, 6664)
                            AND (A.EP_TRAN_IND IN(6325, 6326, 6328, 6329, 6330, 6338, 6339, 6340, 6341, 6830, 6342, 6335, 6344, 6356) 
                            OR (A.EP_TRAN_IND IN (6322, 6323, 6324, 6332, 6333, 6337, 6336, 4025, 4017, 6334, 6115, 7788, 6422) AND IU_SW = 'NO'
                            ))
                            AND (A.EP_ACTN_NUM > 2 OR  A.EP_TRAN_CD = 6115))
                            )
                            THEN WK_ACTV_CLS
                            ELSE 0 
                            END SUB_ACT_CR_FY,
                            CASE WHEN A.EP_TRAN_CD = 6422 AND A.EP_ACTN_NUM = 1 AND   Include_6422_sw = 0 Then 0
                            WHEN A.EP_ACTN_NUM = 1
                            AND (A.EP_TRAN_CD IN (6325, 6326, 6328, 6338, 6339, 7777)  OR (A.EP_TRAN_CD IN (6322, 6332, 6422) AND IU_SW = 'NO') 
                            OR (A.EP_TRAN_CD IN (6321, 6663, 6664) AND  
                            (A.EP_TRAN_IND IN (6325, 6326, 6328, 6338, 6339, 7777)
                            OR (A.EP_TRAN_IND IN (6322, 6332, 6422) AND  IU_SW = 'NO')
                            )
                            )
                            )    
                            THEN  WK_ACTV_CLS
                            ELSE 0
                            END TOT_FA_INIT_FY_CL,
                            CASE WHEN A.EP_ACTN_NUM = 1 
                            AND NOT ( A.EP_SER_NUM > 78999999 AND A.EP_SER_NUM < 80000000)
                            AND (A.EP_TRAN_CD IN (6338, 6339)
                            OR (A.EP_TRAN_CD IN (6321,6663, 6664) AND A.EP_TRAN_IND IN (6338, 6339))
                            )
                            THEN WK_ACTV_CLS
                            ELSE 0
                            END FA_PUBS_FY_CL,
                            CASE WHEN  ((A.EP_TRAN_CD IN (6322, 6323, 6324, 6337, 4025, 4017, 6115, 6130, 6144, 6156, 7788, 6422) AND IU_SW = 'YES')
                            OR (A.EP_TRAN_CD IN (6125, 6126,6128, 6129, 6130, 6138, 6139, 6141, 6142) 
                            OR (A.EP_TRAN_CD IN (6332, 6333, 6334) AND IU_SW = 'YES')
                            OR (A.EP_TRAN_CD = 6336 AND  A.EP_ACTN_NUM > 2))
                            OR (A.EP_TRAN_CD IN  (6321, 6663, 6664) AND A.EP_TRAN_IND IN (6322, 6323, 6324, 6337, 4025, 4017, 6115, 6130, 6144, 6156, 6422) 
                            AND IU_SW = 'YES')
                            OR (A.EP_TRAN_IND IN (6125, 6126,6128, 6129, 6130, 6138, 6139, 6141, 6142) 
                            OR (A.EP_TRAN_IND IN (6332, 6333, 6334) AND IU_SW = 'YES')
                            OR (A.EP_TRAN_IND = 6336 AND  A.EP_ACTN_NUM > 2))
                            )
                            THEN WK_ACTV_CLS
                            ELSE 0
                            END TOT_SOU_CR_FY,      
                            CASE WHEN (
                            (A.EP_TRAN_CD IN (6328, 6128, 6126, 6326) AND   EP_FLG_PRIORITY = 0)
                                OR (
                                    (A.EP_TRAN_CD IN (6321, 6663, 6664)  AND (A.EP_TRAN_IND IN (6328, 6128)
                                    OR ( A.EP_TRAN_IND IN (6126, 6326) AND EP_FLG_PRIORITY = 0 )
                                    )
                                    )) )
                            THEN  WK_ACTV_CLS
                            ELSE 0
                            END NEA_FY,
                            CASE WHEN A.EP_SER_NUM > 78999999 AND A.EP_SER_NUM < 80000000
                                AND A.EP_ACTN_NUM = 1
                                AND (A.EP_TRAN_CD IN (6325, 6326, 6328, 6338, 6339, 7777) 
                                OR (A.EP_TRAN_CD IN (6322, 6332, 6422) AND IU_SW = 'NO') 
                                OR (A.EP_TRAN_CD IN (6321, 6663, 6664) AND  
                                (A.EP_TRAN_IND IN (6325, 6326, 6328, 6338, 6339, 7777)
                                OR (A.EP_TRAN_IND IN (6322, 6332, 6422) AND  IU_SW = 'NO')
                                )
                                )
                                OR (( A.EP_TRAN_CD IN (6125, 6126, 6128, 6138, 6139) OR (A.EP_TRAN_CD IN ( 6322, 6332, 6442) AND IU_SW  = 'YES')
                                )OR (A.EP_TRAN_CD IN (6321, 6663, 6664) AND ( A.EP_TRAN_IND IN  (6125, 6126, 6128, 6138, 6139) OR (A.EP_TRAN_IND IN ( 6322, 6422) AND IU_SW  = 'YES')
                                ) ))
                                )   
                                THEN  WK_ACTV_CLS
                                ELSE 0
                                END A66A_FY,
                                CASE WHEN ((A.EP_TRAN_CD in (6322, 6332) AND IU_SW <> 'YES') 
                                    OR A.EP_TRAN_CD in ( 6325, 6326, 6328, 6338, 6339, 7777)
                                    OR A.EP_TRAN_IND in ( 6325, 6326, 6328, 6338, 6339, 7777)
                                    OR (A.EP_TRAN_IND in (6322, 6332) AND IU_SW <> 'YES')
                                    )
                                    AND A.EP_ACTN_NUM = 1
                                THEN WK_ACTV_CLS 
                                ELSE 0
                                END TOT_FA_INIT_FY
                            FROM   temp_ept_data as A 
                            LEFT JOIN temp_pas_data as PAS 
                            ON A.EP_EXMR_NUM = PAS.EP_EXMR_NUM 
                            AND A.EP_SER_NUM = PAS.EP_SER_NUM  
                            AND PAS.EP_FY_PP = A.EP_FY_PP""")



#Group by the necessary columns and then apply collect_list
temp_ept_pas_df = temp_ept_pas_df.groupBy("EP_EXMR_NUM", "EP_FY_PP",
"bi_week_id","EP_SER_NUM","EP_EXMR_LO","PAS_INDICATOR","EE_LO","EP_ACTN_CT_DT","EP_SYS_TI","EE_EMPE_NAM","LO","law_office","EP_ACTN_NUM","EP_TRAN_CD"
,"EP_TRAN_IND",
"IU_SW",
"APP_PUB_CT_FY",
"NSFR_CT_FY",
"NSFR_SOU_CT_FY",
"ABAN_CT_FY",
"A2ND_ACT_CR_FY",
"SUB_ACT_CR_FY","TOT_FA_INIT_FY_CL","FA_PUBS_FY_CL","TOT_SOU_CR_FY","NEA_FY","A66A_FY","TOT_FA_INIT_FY").agg(
    concat_ws(',', collect_list("EP_SER_NUM")).alias("tept_ser_num_list")
)

temp_ept_pas_df.createOrReplaceTempView("temp_ept_pas_data")
print('Create temp table temp_ept_pas_data')

# COMMAND ----------

#Create temp table ept_stg_data
ept_stg_df= spark.sql("""
                    SELECT  DISTINCT EP_EXMR_NUM,EP_FY_PP,law_office, ENAME,fiscal_year_no,cast(bi_week_id as DOUBLE) as bi_week_id,quarter_no ,cast(BDS_CURRENT as DECIMAL(38,2)),
                                cast(TOTAL_ACTION_COUNT_CURRENT as DECIMAL(38,2))
                                ,cast(TOT_FA_INIT_FY_CL as DECIMAL(38,2)),cast(A66A_FY as DECIMAL(38,2)),cast(NEA_FY as DECIMAL(38,2)),cast(FA_PUBS_FY_CL as DECIMAL(38,2))
                    FROM(SELECT CASE WHEN G.EE_EMPE_NAM IS NULL THEN 'NO EE RECORD FOR ' || EP_EXMR_NUM ELSE  G.EE_EMPE_NAM END ENAME,G.EP_FY_PP, EP_EXMR_NUM, BI_WEEK_ID,FY.fiscal_year_no,G.law_office,FY.quarter_no,SUM(APP_PUB_CT_FY) + SUM(ABAN_CT_FY) +  SUM(TOT_FA_INIT_FY_CL) BDS_CURRENT
                    ,SUM(TOT_FA_INIT_FY_CL)+ SUM(SUB_ACT_CR_FY) + SUM(A2ND_ACT_CR_FY) + SUM(NSFR_CT_FY)  + SUM(TOT_SOU_CR_FY)  + SUM(NSFR_SOU_CT_FY) TOTAL_ACTION_COUNT_CURRENT,
                    SUM(TOT_FA_INIT_FY_CL) TOT_FA_INIT_FY_CL,SUM(A66A_FY) A66A_FY,
                    SUM(NEA_FY) NEA_FY,SUM(FA_PUBS_FY_CL) FA_PUBS_FY_CL 
                    FROM  temp_ept_pas_data as G 
                    JOIN temp_fy_qrt_wid as FY 
                    ON G.EP_EXMR_NUM=FY.EMPLOYEE_NO and G.BI_WEEK_ID=FY.quarter_bi_week_id
                    GROUP BY G.EP_EXMR_NUM, G.EE_EMPE_NAM,FY.fiscal_year_no,FY.quarter_no,G.bi_week_id,G.EP_FY_PP,G.law_office
                    )stg_data
                    WHERE  (BDS_CURRENT <> 0 OR TOTAL_ACTION_COUNT_CURRENT <> 0)
                    """)
ept_stg_df.createOrReplaceTempView("temp_ept_stg_data")
print('created temp table temp_ept_stg_data')

# COMMAND ----------

# MAGIC %md
# MAGIC Difference between TEPT and EP_QUERY

# COMMAND ----------

hv_stg_tram_df=spark.sql(f"""
    select ename, cast(EP_EXMR_NUM as decimal(5,0)) ep_exmr_num,EP_FY_PP,law_office,fiscal_year_no, bi_week_id, cast(BDS_CURRENT as DECIMAL(38,2)) as tept_balanced_disposals
    FROM temp_ept_stg_data
""")


# COMMAND ----------

ep_query = spark.sql(f"""
SELECT 
    DN_WORKER_NO AS ep_exmr_num,
    RIGHT(EP_PP_PERIOD, 3) AS ep_pp_period,
    CONCAT_WS(',', COLLECT_LIST(EP_SER_NUM)) AS ep_ser_num_list,
    SUM(APP_PUB_CT_FY + ABAN_CT_FY + TOT_FA_INIT_FY_CL) AS ep_query_balanced_disposals
FROM 
    temp_df_epquery_stg2
GROUP BY 
    DN_WORKER_NO, RIGHT(EP_PP_PERIOD, 3)""")

#display(ep_query)

# COMMAND ----------

joined_df = ep_query.join(
    hv_stg_tram_df,
    (ep_query.ep_exmr_num == hv_stg_tram_df.ep_exmr_num) & 
    (ep_query.ep_pp_period == hv_stg_tram_df.EP_FY_PP) &
    (ep_query.ep_query_balanced_disposals != hv_stg_tram_df.tept_balanced_disposals)
    ,"inner"
).select(hv_stg_tram_df.ep_exmr_num  , ep_query.ep_pp_period
 , ep_query.ep_ser_num_list
, ep_query.ep_query_balanced_disposals,
hv_stg_tram_df.law_office
, hv_stg_tram_df.ename
, hv_stg_tram_df.bi_week_id
, hv_stg_tram_df.tept_balanced_disposals
)

# COMMAND ----------

joined_df.createOrReplaceTempView("temp_joined_df")

# COMMAND ----------

joined_df.write.mode("overwrite").format("delta").insertInto(
    f"{reporting_catalog}.gold.ep_query_tept_dashboard")

# COMMAND ----------

dbutils.notebook.exit(f"Updated Table for dashboard ")
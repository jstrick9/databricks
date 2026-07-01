-- Databricks notebook source
-- DBTITLE 1,Daily 1:30 AM


-- COMMAND ----------

-- DBTITLE 1,TR25 - Applications Filed by State & Country
CREATE OR REPLACE MATERIALIZED VIEW trm_reporting.tableau.tr25_applications_filed_by_state_country_mvw AS
SELECT DISTINCT 
        TM.SERIAL_NUM_TX AS SER_NUM,                  -- SERIAL NUMBER
        TM.REGISTRATION_NUM,                          -- REGISTRATION NUM
        TM.fk_fee_process_type_cd,                    -- FILING TYPE
        SOT.OWNER_TYPE_CD AS OWNER_TYPE,              -- OWNER TYPE (OA = ORIGINAL APPLICATION | OR = ORIGINAL REGISTRANT)
        SC.CLASS_NO,                                  -- CLASS CODE / NUMBER
        TMC.FK_TM_CLASS_STATUS_CD AS CLASS_STATUS,    -- CLASS STATUS
        MAOWN.COUNTRY_CD,                             -- COUNTRY CODE
        MAOWN.COUNTRY_NM AS COUNTRY_NM,               -- COUNTRY NAME
        MAOWN.GEOGRAPHIC_REGION_CD AS STATE_ABBREV,   -- STATE ABBREV
        MAOWN.GEOGRAPHIC_REGION_NM AS STATE,          -- STATE NAME
        TM.STATUS_DT,                                 -- STATUS DATE
        TM.FILING_DT AS FILING_DT                     -- FILING DATE
    FROM TRM_TMNGPDB.BRONZE.TRADEMARK TM
    LEFT OUTER JOIN TRM_TMNGPDB.BRONZE.TM_CLASS TMC ON TM.TRADEMARK_GID = TMC.FK_TRADEMARK_GID
    LEFT OUTER JOIN TRM_TMNGPDB.BRONZE.STND_CLASS SC ON SC.CLASS_ID = TMC.FK_CLASS_ID
    LEFT OUTER JOIN TRM_TMNGPDB.BRONZE.TM_PARTY_ROLE_OWNER TPRO ON TPRO.FK_TRADEMARK_GID = TM.TRADEMARK_GID
    LEFT OUTER JOIN TRM_TMNGPDB.BRONZE.STND_OWNER_TYPE SOT ON TPRO.FK_OWNER_TYPE_ID = SOT.OWNER_TYPE_ID
    LEFT OUTER JOIN TRM_TMNGPDB.BRONZE.TM_PARTY_ROLE TPR ON
        (TPRO.FK_TRADEMARK_GID = TPR.FK_TRADEMARK_GID
        AND TPRO.FK_TM_PARTY_ROLE_CD = TPR.FK_TM_PARTY_ROLE_CD
        AND TPRO.FK_PARTY_ROLE_SEQUENCE_NO = TPR.PARTY_ROLE_SEQUENCE_NO)
    LEFT OUTER JOIN TRM_TMNGPDB.BRONZE.TM_MAILING_ADDR TMAOWN ON TMAOWN.FK_TM_PARTY_ROLE_ID = TPR.TM_PARTY_ROLE_ID
    LEFT OUTER JOIN TRM_TMNGPDB.BRONZE.MAILING_ADDRESS MAOWN ON MAOWN.MAILING_ADDRESS_GID = TMAOWN.FK_MAILING_ADDRESS_GID
    WHERE 
        TMC.FK_TM_CLASS_STATUS_CD IN ('6','8','P','W')
        AND SOT.OWNER_TYPE_CD = 'OA'

-- COMMAND ----------

-- DBTITLE 1,Daily 9:00 AM


-- COMMAND ----------

-- DBTITLE 1,CM40 - Overdue Processing Report
CREATE OR REPLACE MATERIALIZED VIEW trm_reporting.tableau.cm40_overdue_processing_report_mvw
AS
SELECT
  t.serial_num_tx AS serial_no,
  receiving_date,
  correspondence_type,
  t.legacy_status_cd AS status_code,
  t.status_dt AS status_date,
  l.fk_charge_to_location_cd || ' ' || location_desc_tx AS charge_to_location,
  coalesce(lie_name, 'UNASSIGNED') AS lie_name,
  coalesce(paralegal_name, 'UNASSIGNED') AS paralegal_name
FROM
  trm_tmngpdb.bronze.trademark t
    INNER JOIN trm_tmngpdb.bronze.tm_locations l
      ON t.trademark_gid = l.fk_trademark_gid
    INNER JOIN trm_tmngpdb.bronze.tm_organization_location tl
      ON l.fk_charge_to_location_cd = tl.location_cd
    LEFT JOIN (
      SELECT
        fk_trademark_gid,
        worker_nm AS lie_name
      FROM
        trm_tmworker.bronze.worker w
          INNER JOIN trm_tmngpdb.bronze.tm_employee_assignment te
            ON w.worker_no = te.cfk_employee_no
      WHERE
        fk_tm_employee_role_cd = 'LIE'
    ) lie
      ON lie.fk_trademark_gid = t.trademark_gid
    LEFT JOIN (
      SELECT
        fk_trademark_gid,
        worker_nm AS paralegal_name
      FROM
        trm_tmworker.bronze.worker w
          INNER JOIN trm_tmngpdb.bronze.tm_employee_assignment te
            ON w.worker_no = te.cfk_employee_no
      WHERE
        fk_tm_employee_role_cd = 'PARA'
    ) para
      ON para.fk_trademark_gid = t.trademark_gid
    INNER JOIN (
      SELECT
        cfk_object_gid AS serial_number,
        CAST(be.effective_ts AS DATE) AS receiving_date,
        be.order_no,
        description_tx AS correspondence_type
      FROM
        trm_tmngpdb.bronze.business_event be
          INNER JOIN trm_tmngpdb.bronze.stnd_business_event_reason sb
            ON be.fk_business_event_reason_id = sb.business_event_reason_id
      WHERE
        business_event_reason_cd IN (
          'CMMPI',
          'PRRRI',
          'CRFAI',
          'DRRRI',
          'EAAUI',
          'EMRVE',
          'FAXXI',
          'IUAFP',
          'IUAFS',
          'IUFSP',
          'MAILI',
          'D1BRI',
          'EISUI',
          'EXT1S',
          'EXT2S',
          'EXT3S',
          'EXT4S',
          'EXT5S',
          'EEXTI',
          'TPEXI'
        )
        AND sb.tm_milestone_in = 'N'
        AND CAST(be.effective_ts AS DATE) <= (current_date() - 60)
    ) d
      ON d.serial_number = t.trademark_gid
WHERE
  legacy_status_cd IN (
    688, 718, 719, 720, 721, 722, 724, 725, 726, 727, 728, 729, 730, 731, 732, 733, 734, 744, 745
  )
  AND EXISTS (
    SELECT
      be1.cfk_object_gid
    FROM
      trm_tmngpdb.bronze.business_event be1
        INNER JOIN trm_tmngpdb.bronze.stnd_business_event_reason sb1
          ON be1.fk_business_event_reason_id = sb1.business_event_reason_id
    WHERE
      be1.cfk_object_gid = d.serial_number
    GROUP BY
      be1.cfk_object_gid
    HAVING
      MAX(be1.order_no) = d.order_no
  )

-- COMMAND ----------

-- DBTITLE 1,Saturday 11:00 PM


-- COMMAND ----------

-- DBTITLE 1,CM157 - Error Cases Bounced from Photcomp Report
CREATE OR REPLACE MATERIALIZED VIEW trm_reporting_dev.tableau.cm157_error_cases_bounced_from_photcomp_mvw
AS
WITH RPT_CM156_ERROR AS (
  SELECT
    A.CFK_OBJECT_GID,
    A.ORDER_NO
  FROM
    trm_tmngpdb_dev.bronze.TRADEMARK TM
      LEFT JOIN trm_tmngpdb_dev.bronze.BUSINESS_EVENT A
        ON TM.TRADEMARK_GID = A.CFK_OBJECT_GID
      LEFT JOIN trm_tmngpdb_dev.bronze.TM_LOCATIONS LOC
        ON TM.TRADEMARK_GID = LOC.FK_TRADEMARK_GID
      LEFT JOIN trm_tmngpdb_dev.bronze.STND_BUSINESS_EVENT_REASON SBER
        ON A.FK_BUSINESS_EVENT_REASON_ID = SBER.BUSINESS_EVENT_REASON_ID
  WHERE
    TM.LEGACY_STATUS_CD IN (681, 819, 690, 692, 694)
    AND LOC.FK_CURRENT_LOCATION_CD = '650'
    AND SBER.BUSINESS_EVENT_REASON_CD = 'ERRRO'
    AND EXISTS (
      SELECT
        B.CFK_OBJECT_GID,
        B.ORDER_NO
      FROM
        trm_tmngpdb_dev.bronze.BUSINESS_EVENT B
          LEFT JOIN trm_tmngpdb_dev.bronze.STND_BUSINESS_EVENT_REASON SBER
            ON B.FK_BUSINESS_EVENT_REASON_ID = SBER.BUSINESS_EVENT_REASON_ID
      WHERE
        B.CFK_OBJECT_GID = A.CFK_OBJECT_GID
        AND (
          SBER.BUSINESS_EVENT_REASON_CD NOT IN (
            'ARAAI',
            'CFITO',
            'COARI',
            'OPNRP',
            'OPNSP',
            'OPNXP',
            'REAPI',
            'STALO',
            'TCCAI',
            'UNDCO',
            'UNDNO',
            'UNDRO',
            'WOAGI',
            'WOARI'
          )
        )
      GROUP BY
        B.CFK_OBJECT_GID,
        B.ORDER_NO
      HAVING
        A.ORDER_NO = MAX(B.ORDER_NO)
    )
),
J_NOTE_LAST_ENTRY AS (
  SELECT
    FK_TRADEMARK_GID JNOTE_SER_NUM,
    A.create_ts,
    A.completed_ts
  FROM
    trm_tmngpdb_dev.bronze.internal_note A
  WHERE
    EXISTS (
      SELECT
        B.FK_TRADEMARK_GID
      FROM
        trm_tmngpdb_dev.bronze.internal_note B
      WHERE
        B.FK_TRADEMARK_GID = A.FK_TRADEMARK_GID
      GROUP BY
        B.FK_TRADEMARK_GID
      HAVING
        A.last_mod_ts = MAX(B.last_mod_ts)
    )
)
SELECT DISTINCT
  TM.SERIAL_NUM_TX SERIAL_NUM,
  concat('*', TM.SERIAL_NUM_TX, '*') AS Barcode,
  TM.TRADEMARK_GID,
  J_NOTE_LAST_ENTRY.JNOTE_SER_NUM,
  J_NOTE_LAST_ENTRY.completed_ts,
  J_NOTE_LAST_ENTRY.create_ts,
  TM.LEGACY_STATUS_CD,                --AM_STAT
  TM.STATUS_DT,                       --AM_STAT_DT
  LOC.FK_CURRENT_LOCATION_CD,         --AM_LOC
  W.WORKER_NM EMPLOYEE_NAME,          --EE_EMPE_NAM
  OG.LEGACY_OG_STATUS_CD,             --OG_STAT
  OG.FK_TRADEMARK_GID,
  CASE
    WHEN J_NOTE_LAST_ENTRY.create_ts IS NULL THEN TM.status_dt
    WHEN J_NOTE_LAST_ENTRY.create_ts < TM.status_dt THEN TM.status_dt
    ELSE J_NOTE_LAST_ENTRY.create_ts
  END AS rpt_ts
FROM
  trm_tmngpdb_dev.bronze.TRADEMARK TM
    INNER JOIN RPT_CM156_ERROR
      ON TM.TRADEMARK_GID = RPT_CM156_ERROR.CFK_OBJECT_GID
    LEFT JOIN J_NOTE_LAST_ENTRY
      ON TM.TRADEMARK_GID = J_NOTE_LAST_ENTRY.JNOTE_SER_NUM
    LEFT JOIN trm_tmngpdb_dev.bronze.TM_LOCATIONS LOC
      ON TM.TRADEMARK_GID = LOC.FK_TRADEMARK_GID
    LEFT JOIN trm_tmngpdb_dev.bronze.TM_EMPLOYEE_ASSIGNMENT EA
      ON TM.TRADEMARK_GID = EA.FK_TRADEMARK_GID
      AND FK_TM_EMPLOYEE_ROLE_CD = 'EA'
    LEFT JOIN trm_tmworker_dev.bronze.worker W
      ON EA.CFK_EMPLOYEE_NO = W.WORKER_NO
    LEFT JOIN trm_tmngpdb_dev.bronze.TM_PUBLICATION OG
      ON TM.TRADEMARK_GID = OG.FK_TRADEMARK_GID
WHERE
  TM.LEGACY_STATUS_CD IN (681, 690, 692, 694, 819)
  AND LOC.FK_CURRENT_LOCATION_CD LIKE '65%'
  AND (
    J_NOTE_LAST_ENTRY.completed_ts IS NULL
    OR J_NOTE_LAST_ENTRY.completed_ts < TM.status_dt
  )
  AND CASE
    WHEN TM.TRADEMARK_GID = OG.FK_TRADEMARK_GID THEN to_number(OG.LEGACY_OG_STATUS_CD, '999')
    ELSE 99
  END > 55
GROUP BY
  TM.SERIAL_NUM_TX,
  TM.TRADEMARK_GID,
  J_NOTE_LAST_ENTRY.JNOTE_SER_NUM,
  TM.LEGACY_STATUS_CD,
  TM.STATUS_DT,
  J_NOTE_LAST_ENTRY.completed_ts,
  J_NOTE_LAST_ENTRY.create_ts,
  LOC.FK_CURRENT_LOCATION_CD,
  W.WORKER_NM,
  OG.LEGACY_OG_STATUS_CD,
  OG.FK_TRADEMARK_GID

-- COMMAND ----------

-- DBTITLE 1,Tuesday 5:45 AM


-- COMMAND ----------

-- DBTITLE 1,CM155 - Proofreaders Daily Report
CREATE OR REPLACE MATERIALIZED VIEW trm_reporting.tableau.cm155_proofreaders_report_mvw
AS
SELECT DISTINCT
  TM.TRADEMARK_GID AS SERIAL_NUM,                   -- AM_SER_NUM
  TM.LEGACY_STATUS_CD AS STATUS_CD,                 -- AM_STAT
  LEG_STAT.DESCRIPTION_TX AS STAT_CD_DESCRIPTION,   -- AM_STAT DESCRIPTION
  TM.STATUS_DT,                                     -- AM_STAT_DT
  PUB.FK_TRADEMARK_GID AS OG_SER_NUM,               -- OG_SER_NUM
  PUB_S.LEGACY_DES_CD AS OG_CATG,                   -- OG_CATG
  PUB.LEGACY_OG_STATUS_CD AS OG_STAT,               -- OG_STAT
  TM_ITU.ITU_CASE_PUBD_FOR_OPSTN_IN AS FLG_ITU_PUBO,-- AM_FLG_ITU_PUBO (note: DECODE(AM_FLG_ITU_PUBO,1,'Y','N') (RULE 1046 MUST BE TRUE)
  TFB.CURRENT_IN AS FLG_ITU_CUR,                    -- AM_FLG_ITU_CUR (note: if AM-FLG-ITU-CUR = 1 MOVE 'Y' ELSE 'N')
  TM.REGISTRY_CT AS FLG_SUPL_REG,                   -- AM_FLG_SUPL_REG (note: If am_flg_supl_reg = 0 then 'P' else if am_flg_supl_reg = 1 then 'S' Else 'U')
  EMP.CFK_EMPLOYEE_NO AS LIE_NUM1,                  -- AM_LIE_NUM
  WORKER.WORKER_NO AS LIE_NUM2,
  WORKER.WORKER_NM AS LIE_NAME,
  nvl(law_office_nm, 'OTHER LOCATIONS') AS LAW_OFFICE
FROM
  trm_tmngpdb.bronze.TRADEMARK TM
    LEFT JOIN trm_tmngpdb.bronze.TM_LOCATIONS LOC
      ON LOC.FK_TRADEMARK_GID = TM.TRADEMARK_GID
    LEFT JOIN trm_tmngpdb.bronze.STND_LEGACY_STATUS LEG_STAT
      ON LEG_STAT.STATUS_NO = TM.LEGACY_STATUS_CD
    LEFT JOIN trm_tmngpdb.bronze.TM_PUBLICATION PUB
      ON PUB.FK_TRADEMARK_GID = TM.TRADEMARK_GID
    LEFT JOIN trm_tmngpdb.bronze.TM_ITU
      ON TM_ITU.FK_TRADEMARK_GID = TM.TRADEMARK_GID
    LEFT JOIN trm_tmngpdb.bronze.TM_FILING_BASIS TFB
      ON TFB.FK_TRADEMARK_GID = TM.TRADEMARK_GID
    LEFT JOIN trm_tmngpdb.bronze.TM_EMPLOYEE_ASSIGNMENT EMP
      ON EMP.FK_TRADEMARK_GID = TM.TRADEMARK_GID
    LEFT JOIN trm_tmngpdb.bronze.TM_PUBLICATION_SUBCT PUB_S
      ON PUB_S.FK_TM_PUBLICATION_GID = PUB.TM_PUBLICATION_GID
    LEFT JOIN trm_tmworker.bronze.WORKER
      ON WORKER.WORKER_NO = EMP.CFK_EMPLOYEE_NO
    LEFT JOIN trm_reporting.silver.vw_law_offices VLO
      on LOC.CFK_ASGND_EXAM_LAW_OFC_ORG_CD = VLO.law_office_cd
WHERE
  TM.LEGACY_STATUS_CD IN (681, 689, 690, 692, 694, 819, 773, 777)

-- COMMAND ----------

-- DBTITLE 1,CM196 - Pro Se Cases & Classes
CREATE OR REPLACE MATERIALIZED VIEW trm_reporting.tableau.cm196_pro_se_cases_classes_mvw
AS
WITH ACTIVE_CLASSES AS (
  SELECT
    FK_TRADEMARK_GID,
    COUNT(*) CLASS_COUNTS
  FROM
    TRM_TMNGPDB.BRONZE.TM_CLASS
  WHERE
    FK_TM_CLASS_STATUS_CD IN ('6', '8', 'P', 'W')
  GROUP BY
    FK_TRADEMARK_GID
),
PROSE_CASES AS (
  SELECT
    DISTINCT FK_TRADEMARK_GID
  FROM
    TRM_TMNGPDB.BRONZE.TM_PARTY_ROLE TMPR
    INNER JOIN TRM_TMNGPDB.BRONZE.STND_TM_PARTY_ROLE STND_PARTY ON TMPR.FK_TM_PARTY_ROLE_CD = STND_PARTY.TM_PARTY_ROLE_CD
  WHERE
    STND_PARTY.TITLE_TX NOT IN ('Second Attorney', 'Attorney')
)
SELECT
  DISTINCT TM.FILING_DT,          --AM_DT_FIL
  TM.SERIAL_NUM_TX SERIAL_NUM,    --AM_SER_NUM
  ACTIVE_CLASSES.CLASS_COUNTS,    --AM_CLS_CT_ACTV
  BASIS.FILED_IN,                 --AM_FLG_66A_FIL
  TM.FK_FILED_FEE_PROCESS_TYPE_CD,--AM_FLG_TEAPL_FIL & AM_FLG_TEASRF_FIL
  IP.FK_LEGAL_ENTITY_TYPE_CD,     --PY.PY_ENTITY_TYPE
  STND.DESCRIPTION_TX ENTITY_DESC,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'PAPER' THEN 1
    ELSE 0
  END PAPER_FLAG,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'MADRD' THEN 1
    ELSE 0
  END MADRID_FLAG,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'TEAS' THEN 1
    ELSE 0
  END TEAS_FLAG,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'TEASP' THEN 1
    ELSE 0
  END TEAS_PLUS_FLAG,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'TEASR' THEN 1
    ELSE 0
  END TEAS_REDUCED_FEE_FLAG,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'TEASE' THEN 1
    ELSE 0
  END TEAS_ELECTRONIC_FLAG
FROM
  TRM_TMNGPDB.BRONZE.TRADEMARK TM
  LEFT JOIN ACTIVE_CLASSES ON TM.TRADEMARK_GID = ACTIVE_CLASSES.FK_TRADEMARK_GID
  LEFT JOIN TRM_TMNGPDB.BRONZE.TM_FILING_BASIS BASIS ON TM.TRADEMARK_GID = BASIS.FK_TRADEMARK_GID
  LEFT JOIN TRM_TMNGPDB.BRONZE.TM_PARTY_ROLE TMPR ON TM.TRADEMARK_GID = TMPR.FK_TRADEMARK_GID
  LEFT JOIN TRM_TMNGPDB.BRONZE.INTERESTED_PARTY IP ON TMPR.FK_INTERESTED_PARTY_GID = IP.INTERESTED_PARTY_GID
  LEFT JOIN TRM_TMNGPDB.BRONZE.STND_LEGAL_ENTITY_TYPE STND ON IP.FK_LEGAL_ENTITY_TYPE_CD = STND.LEGAL_ENTITY_TYPE_CD
  INNER JOIN PROSE_CASES ON PROSE_CASES.FK_TRADEMARK_GID = TM.TRADEMARK_GID
WHERE
  IP.FK_LEGAL_ENTITY_TYPE_CD <> 1 --NOT EXISTS IN SUBQUERY WHERE VT.VT_TEXT_TYPE = 'AT'
  AND TRIM(IP.FK_LEGAL_ENTITY_TYPE_CD) IN (
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
    '10',
    '11',
    '12',
    '13',
    '14',
    '15',
    '16',
    '17',
    '18',
    '19',
    '20',
    '21',
    '98',
    '99'
  )

-- COMMAND ----------

-- DBTITLE 1,CM28 - NOA Cancelled - No Processing
CREATE OR REPLACE MATERIALIZED VIEW trm_reporting.tableau.cm28_noa_cancelled_no_processing_mvw
AS
SELECT
  *
FROM
  (
    SELECT DISTINCT
      TM.SERIAL_NUM_TX AS SERIAL_NUM,   --AM_SER_NUM
      TM.STATUS_DT,                     --AM_STAT_DT
      SBER.TITLE_TX AS DESCRIPTION,     --ENT_CODE1.DESCRIPTION
      BE.EFFECTIVE_TS,                  --CM_ENT_DT1 (main)
      BE.LAST_MOD_TS AS LAST_MODIFIED,  --CM_ENT_DT2
      BE.CFK_PROCEEDING_NO AS PRCD_NUM, --CM_PRCD_NUM
      CASE
        WHEN WORKER.WORKER_NM IS NULL THEN 'Employee Name Not Available'
        ELSE WORKER.WORKER_NM
      END AS EMPLOYEE_NAME,             --EE_EMPE_NAM
      RANK() OVER (PARTITION BY TM.SERIAL_NUM_TX ORDER BY BE.EFFECTIVE_TS DESC) RANK
    FROM
      trm_tmngpdb.bronze.trademark TM
        LEFT JOIN trm_tmngpdb.bronze.tm_employee_assignment EA
          ON TM.TRADEMARK_GID = EA.FK_TRADEMARK_GID
          AND FK_TM_EMPLOYEE_ROLE_CD = 'EA'
        LEFT JOIN trm_tmworker.bronze.worker
          ON EA.CFK_EMPLOYEE_NO = WORKER.WORKER_NO
        LEFT JOIN trm_tmngpdb.bronze.tm_locations LOC
          ON LOC.FK_TRADEMARK_GID = TM.TRADEMARK_GID
        LEFT JOIN trm_tmngpdb.bronze.business_event BE
          ON BE.CFK_OBJECT_GID = TM.TRADEMARK_GID
        LEFT JOIN trm_tmngpdb.bronze.stnd_business_event_reason SBER
          ON SBER.BUSINESS_EVENT_REASON_ID = BE.FK_BUSINESS_EVENT_REASON_ID
    WHERE
      (
        TM.LEGACY_STATUS_CD = '690'
        AND LOC.FK_CHARGE_TO_LOCATION_CD NOT IN ('845', '650')
      )
      AND SBER.FK_BUSINESS_EVENT_RSN_CT_CD = 'TMARK'
      AND datediff(current_date(), CAST(BE.EFFECTIVE_TS AS DATE)) >= 30
  ) A
WHERE
  RANK = 1
ORDER BY
  SERIAL_NUM

-- COMMAND ----------

-- DBTITLE 1,Wednesday 5:45 AM


-- COMMAND ----------

-- DBTITLE 1,CM115 - Total Inventory Report by Location Code
CREATE OR REPLACE MATERIALIZED VIEW trm_reporting.tableau.cm115_total_inventory_report_by_location_code_mvw
AS
WITH ACTIVE_CLASSES AS (
SELECT FK_TRADEMARK_GID, COUNT(*) COUNTS 
FROM TRM_TMNGPDB.BRONZE.TM_CLASS WHERE FK_TM_CLASS_STATUS_CD IN ('6', '8','P','W')
GROUP BY FK_TRADEMARK_GID 
)

SELECT
LOC.FK_CURRENT_LOCATION_CD,
TOL.LOCATION_DESC_TX,
COUNT(DISTINCT TM.SERIAL_NUM_TX) CASEFILE_COUNT,
SUM(CASE WHEN LOC.CASE_REPORTED_LOST_IN='Y' THEN 1 ELSE 0 END) LOST_FILE_COUNT,
SUM(ACTIVE_CLASSES.COUNTS) ACTIVE_CLASS_COUNTS,
MIN(TM.FILING_DT) OLD_FILING_DT
FROM TRM_TMNGPDB.BRONZE.TRADEMARK TM
LEFT JOIN TRM_TMNGPDB.BRONZE.TM_LOCATIONS LOC ON TM.TRADEMARK_GID = LOC.FK_TRADEMARK_GID
LEFT JOIN TRM_TMNGPDB.BRONZE.TM_ORGANIZATION_LOCATION TOL ON LOC.FK_CURRENT_LOCATION_CD = TOL.LOCATION_CD
LEFT JOIN ACTIVE_CLASSES ON TM.TRADEMARK_GID = ACTIVE_CLASSES.FK_TRADEMARK_GID
WHERE LOC.FK_CURRENT_LOCATION_CD IS NOT NULL
GROUP BY LOC.FK_CURRENT_LOCATION_CD, TOL.LOCATION_DESC_TX
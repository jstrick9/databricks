# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Metadata
# MAGIC
# MAGIC **Created by:** Drew McPherson  
# MAGIC **Created on:** 2026-03-23   
# MAGIC **Last updated by:** Drew McPherson  
# MAGIC **Last updated on:** 2026-03-23  
# MAGIC
# MAGIC ## Changelog
# MAGIC - **2026-03-23 (Drew McPherson):** Initial table creation.
# MAGIC - **2026-05-08 (Drew McPherson):** Add additional milestone date fields.

# COMMAND ----------

# DBTITLE 1,Load Libraries
from pyspark.sql import functions as F 
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Set Config File
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
dbutils.widgets.text("tm_status_date", "2023-09-30") 
tm_status_date = dbutils.widgets.get("tm_status_date")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")



# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set parameter values
common_configs = read_yaml(config_file)
tmngpdb_src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
reporting_catalog = common_configs["schema"]["reporting_catalog"]
trgt_catalog = common_configs["schema"]["trgt_catalog"]

# COMMAND ----------

# DBTITLE 1,Print values
print(f"{tmngpdb_src_catalog=},{reporting_catalog=},{trgt_catalog=}")

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = "grand_model_milestone"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Load Records
case_dictionary_df = spark.sql(f"""

-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Common Table Expressions (CTEs)
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

-- =======================================================================================
-- Divisionals: Parents and Children
-- =======================================================================================

  -- The purpose of this CTE is to identify which cases are divisional parents and divisional children to prevent double counting cases (divisional children inherit their parent's prosecution history).

  WITH div_children AS (
  SELECT 
      RIGHT(fk_child_trademark_gid, 8) AS serial_number,
      TRUE as div_child
  FROM `{tmngpdb_src_catalog}`.bronze.tm_divisional_child
  WHERE DATE(tm_divisional_status_dt) > '{tm_status_date}'
  GROUP BY RIGHT(fk_child_trademark_gid, 8)
  ),

  div_parents AS (
  SELECT 
      RIGHT(fk_trademark_gid, 8) AS serial_number,
      TRUE as div_parent
  FROM `{tmngpdb_src_catalog}`.bronze.tm_divisional_child
  WHERE DATE(tm_divisional_status_dt) > '{tm_status_date}'
  GROUP BY RIGHT(fk_trademark_gid, 8)
  ),

-- =======================================================================================
-- Post-Reg: Start and End Fields
-- =======================================================================================

  --This section captures the most recent post-reg action for the case. 

  post_reg_dash AS (
  SELECT
      serial_number,
      start_action_date,
      end_action_date,
      LEFT(start_5_characters, 4) AS prg_dash_start_cd,
      LEFT(end_5_characters, 4) AS prg_dash_end_cd,
      postreg_category,
      ROW_NUMBER() OVER (
        PARTITION BY serial_number
        ORDER BY start_action_date DESC
      ) AS rn
    FROM `{reporting_catalog}`.gold.post_reg_detail_dashboard
    WHERE start_action_date > '{tm_status_date}'
    QUALIFY rn = 1
  ),

pre_exam_rec AS(
  SELECT ser_num, 
  MAX(date(pre_exam_received_ts)) AS pre_exam_received_dt
  FROM `{reporting_catalog}`.silver.pea_trademark_applications
  GROUP BY ser_num
    )


-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Basic Case Details
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  -- This section captures basic details about the case that provide quick insight into its movement through the trademark system.

SELECT
  ms.ser_num AS ser_num,
  MAX(ms.filing_dt) AS filing_dt,
  MAX(ph_action_date) AS latest_ph_action_dt,
  MAX(ms.disposal_dt) AS disposal_dt,
  MAX(ms.abandonment_dt) AS abandonment_dt,
  DATE(MAX(it.POTENTIEL_ABANDONMENT_DT)) AS potential_abandonment_dt,
  MIN(
    CASE WHEN ph_action_code LIKE 'ABN%' THEN ph_action_date
    END) AS abn_min_dt,
  MAX(
    CASE WHEN ph_action_code LIKE 'ABN%' THEN ph_action_date
    END) AS abn_max_dt,
  MAX(ms.revival_dt) AS revival_dt,
  ANY_VALUE(dp.div_parent) AS div_parent,
  ANY_VALUE(dc.div_child) AS div_child,


-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- Key Dates
-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  -- The purpose of this section is to capture key dates in the cases' progress across the trademark system. It function as an expansion of the functionality of the milestone table.

  -- =======================================================================================
  -- Phase: Pre-Exam (PEX)
  -- =======================================================================================
    MAX(pxu.pre_exam_received_dt) AS pre_exam_received_dt,
    --REPR: SN ASSIGNED FOR SECT 66A APPL FROM IB
    MAX(
      CASE WHEN ph_action_code IN ('REPR') THEN ph_action_date
      END) AS repr_dt,
    -- NWAP: NEW APPLICATION ENTERED
    MAX(
      CASE WHEN ph_action_code IN ('NWAP') THEN ph_action_date
      END) AS nwap_dt,
    -- NWOS: NEW APPLICATION OFFICE SUPPLIED DATA ENTERED
    MAX(
      CASE WHEN ph_action_code IN ('NWOS') THEN ph_action_date
      END) AS nwos_dt,

  -- =======================================================================================
  -- Phase: Exam (EXA)
  -- =======================================================================================
    
    --DOCK: ASSIGNED TO EXAMINER
    MAX(ms.dock_dt) AS dock_dt,
    --CNSA: APPROVED FOR PUB - PRINCIPAL REGISTER
    MAX(
      CASE WHEN ph_action_code IN ('CNSA') THEN ph_action_date
      END) AS cnsa_dt,
    -- NOAM: NOA MAILED - SOU REQUIRED FROM APPLICANT
    MAX(
      CASE WHEN ph_action_code IN ('NOAM') THEN ph_action_date
      END) AS noam_dt,
    MAX(ms.noa_dt) AS noa_dt, -- Included for transparency

  -- =======================================================================================
  -- Phase: Publish for Opposition (PUB)
  -- =======================================================================================

    -- PUBO: PUBLISHED FOR OPPOSITION
    MAX(
      CASE WHEN ph_action_code IN ('PUBO') THEN ph_action_date
      END) AS pubo_dt,
    -- CNTA: APPROVED FOR REGISTRATION SUPPLEMENTAL REGISTER
    MAX(
      CASE WHEN ph_action_code IN ('CNTA') THEN ph_action_date
      END) AS cnta_dt,
    MAX(ms.published_dt) AS published_dt, -- Included for transparency

  -- =======================================================================================
  -- Phase: Intent to Use (ITU)
  -- =======================================================================================
    
    -- AITU: CASE ASSIGNED TO INTENT TO USE PARALEGAL
    MAX(
      CASE WHEN ph_action_code IN ('AITU') THEN ph_action_date
      END) AS aitu_dt,

    -- -------------------------------------------------------------------------------------
    -- Process: Extension Requests
    -- -------------------------------------------------------------------------------------

    --EEXT SOU TEAS EXTENSION RECEIVED 
    -- MAX is used here because a new EEXT is issued with each extension
    MAX(
      CASE WHEN ph_action_code IN ('EEXT') THEN ph_action_date
      END) AS eext_max_dt,
    --EXT#: SOU EXTENSION # FILED
    MAX(ms.ext1_dt) AS ext1_dt,
    MAX(ms.ext2_dt) AS ext2_dt,
    MAX(ms.ext3_dt) AS ext3_dt,
    MAX(ms.ext4_dt) AS ext4_dt,
    MAX(ms.ext5_dt) AS ext5_dt,
    -- EX#G: SOU EXTENSION # GRANTED
    MAX(
      CASE WHEN ph_action_code IN ('EX1G') THEN ph_action_date
      END) AS ex1g_dt,
    MAX(
      CASE WHEN ph_action_code IN ('EX2G') THEN ph_action_date
      END) AS ex2g_dt,
    MAX(
      CASE WHEN ph_action_code IN ('EX3G') THEN ph_action_date
      END) AS ex3g_dt,
    MAX(
      CASE WHEN ph_action_code IN ('EX4G') THEN ph_action_date
      END) AS ex4g_dt,
    MAX(
      CASE WHEN ph_action_code IN ('EX5G') THEN ph_action_date
      END) AS ex5g_dt,
    MIN(
      CASE WHEN ph_action_code IN ('INCE') THEN ph_action_date
      END) AS ince_min_dt,

    -- -------------------------------------------------------------------------------------
    -- Process: Statement of Use
    -- -------------------------------------------------------------------------------------  

    -- EISU: TEAS STATEMENT OF USE RECEIVED
    MAX(
      CASE WHEN ph_action_code IN ('EISU') THEN ph_action_date
      END) AS eisu_dt,
    -- SUPC: STATEMENT OF USE PROCESSING COMPLETE
    MAX(
      CASE WHEN ph_action_code IN ('SUPC') THEN ph_action_date
      END) AS supc_dt,
    -- SUNA: NOTICE OF ACCEPTANCE OF STATEMENT OF USE MAILED
    MAX(
      CASE WHEN ph_action_code IN ('SUNA') THEN ph_action_date
      END) AS suna_dt,
    -- IUCN: NOTICE OF ALLOWANCE CANCELLED
    MAX(
      CASE WHEN ph_action_code IN ('IUCN') THEN ph_action_date
      END) AS iucn_dt,
    -- PCBG: PETITION TO DIRECTOR - CHANGE BASIS - GRANTED
    MAX(
      CASE WHEN ph_action_code IN ('PCBG') THEN ph_action_date
      END) AS pcbg_dt,
    -- DP1B: 1(B) BASIS DELETED; PROCEED TO REGISTRATION
    MAX(
      CASE WHEN ph_action_code IN ('DP1B') THEN ph_action_date
      END) AS dp1b_dt,
    -- AUPC: AMENDMENT TO USE PROCESSING COMPLETE
    MAX(
      CASE WHEN ph_action_code IN ('AUPC') THEN ph_action_date
      END) AS aupc_dt,
    -- INCS: ITU OFFICE ACTION ISSUED FOR STATEMENT OF USE
    MIN(
      CASE WHEN ph_action_code IN ('INCS') THEN ph_action_date
      END) AS incs_min_dt,

    ----------------------------------------------------------------------------------------
    -- Divisional Requests (DIV)
    ----------------------------------------------------------------------------------------

    -- DRRR: DIVISIONAL REQUEST RECEIVED
    MAX(
      CASE WHEN ph_action_code IN ('DRRR') THEN ph_action_date
      END) AS drrr_dt,
    -- ERTD: TEAS REQUEST TO DIVIDE RECEIVED
    MAX(
      CASE WHEN ph_action_code IN ('ERTD') THEN ph_action_date
      END) AS ertd_dt,
    -- ERTR: TEAS REQUEST TO DIVIDE REGISTRATION
    MAX(
      CASE WHEN ph_action_code IN ('ERTR') THEN ph_action_date
      END) AS ertr_dt,
    -- RTDR: REQUEST TO DIVIDE RECEIVED
    MAX(
      CASE WHEN ph_action_code IN ('RTDR') THEN ph_action_date
      END) AS rtdr_dt,
    -- DPCC: DIVISIONAL PROCESSING COMPLETE
    MAX(
      CASE WHEN ph_action_code IN ('DPCC') THEN ph_action_date
      END) AS dpcc_dt,
    -- UNTD: REQUEST TO DIVIDE UNTIMELY, REFUSED, OR WITHDRAWN
    MAX(
      CASE WHEN ph_action_code IN ('UNTD') THEN ph_action_date
      END) AS untd_dt,
    -- INCD: ITU OFFICE ACTION ISSUED FOR DIVISIONAL REQUEST
    MIN(
      CASE WHEN ph_action_code IN ('INCD') THEN ph_action_date
      END) AS incd_min_dt,

  -- =======================================================================================
  -- Phase: Registration (REG)
  -- =======================================================================================
    -- R.PR: REGISTERED-PRINCIPAL REGISTER
    MAX(
      CASE WHEN ph_action_code IN ('R.PR') THEN ph_action_date
      END) AS r_pr_dt,
    -- R.SR: REGISTERED-SUPPLEMENTAL REGISTER
    MAX(
      CASE WHEN ph_action_code IN ('R.SR') THEN ph_action_date
      END) AS r_sr_dt,
    MAX(ms.registration_dt) AS registration_dt,

  -- =======================================================================================
  -- Phase: Post-Registration (PRG)
  -- =======================================================================================

    -- APRE: CASE ASSIGNED TO POST REGISTRATION PARALEGAL
    MAX(
      CASE WHEN ph_action_code IN ('APRE') THEN ph_action_date
      END) AS prg_apre_dt,

    ----------------------------------------------------------------------------------------
    -- Renewals (REN)
    ----------------------------------------------------------------------------------------

    -- Renewal requires MAX PH codes, because a case can involve multiple renewals.
    -- Since these renewals are 6-10 years apart, overlap between renewal actions is not a concern.

    ANY_VALUE(p.start_action_date) AS prg_dash_start_dt,
    ANY_VALUE(p.end_action_date) AS prg_dash_end_dt,
    ANY_VALUE(p.prg_dash_start_cd) AS prg_dash_start_cd,
    ANY_VALUE(p.prg_dash_end_cd) AS prg_dash_end_cd,
    ANY_VALUE(p.postreg_category) AS postreg_category,

    -- ES8R: TEAS SECTION 8 RECEIVED
    MAX(
      CASE WHEN ph_action_code IN ('ES8R') THEN ph_action_date
      END) AS prg_es8r_dt,
    -- E89R: TEAS SECTION 8 & 9 RECEIVED
    MAX(
      CASE WHEN ph_action_code IN ('E89R') THEN ph_action_date
      END) AS prg_e89r_dt,
    -- 8.OK: REGISTERED - SEC. 8 (6-YR) ACCEPTED
    MAX(
      CASE WHEN ph_action_code IN ('8.OK') THEN ph_action_date
      END) AS prg_8ook_dt,
    -- 8PRT: REGISTERED - PARTIAL SEC. 8 (10-YR) ACCEPTED
    MAX(
      CASE WHEN ph_action_code IN ('8PRT') THEN ph_action_date
      END) AS prg_8prt_dt,
    -- 8.PR: REGISTERED - PARTIAL SEC. 8 (6-YR) ACCEPTED
    MAX(
      CASE WHEN ph_action_code IN ('8.PR') THEN ph_action_date
      END) AS prg_8opr_dt,
    -- C8..: CANCELLED SEC. 8 (6-YR)
    MAX(
      CASE WHEN ph_action_code IN ('C8..') THEN ph_action_date
      END) AS prg_c8oo_dt,
    -- C8.T: CANCELLED SEC. 8 (10-YR)
    MAX(
      CASE WHEN ph_action_code IN ('C8.T') THEN ph_action_date
      END) AS prg_c8ot_dt,
    -- CAEX: CANCELLED SEC. 8 (10-YR)/EXPIRED SECTION 9
    MAX(
      CASE WHEN ph_action_code IN ('CAEX') THEN ph_action_date
      END) AS prg_caex_dt,
    -- 89AG: REGISTERED - SEC. 8 (10-YR) ACCEPTED/SEC. 9 GRANTED
    MAX(
      CASE WHEN ph_action_code IN ('89AG') THEN ph_action_date
      END) AS prg_89ag_dt,
    -- S89G: REGISTERED-SUBSEQUENT SEC. 8 (10 YR) ACCEPTED/SEC. 9 GRANTED
    MAX(
      CASE WHEN ph_action_code IN ('S89G') THEN ph_action_date
      END) AS prg_s89g_dt

FROM `{reporting_catalog}`.silver.milestone ms
  LEFT JOIN `{reporting_catalog}`.silver.prosecution_history ph
    ON ph.serial_number = ms.ser_num
  LEFT JOIN `{tmngpdb_src_catalog}`.bronze.tm_itu it
    ON ms.ser_num = RIGHT(it.FK_TRADEMARK_GID, 8) 
  LEFT JOIN div_children dc 
    ON ms.ser_num = dc.serial_number
  LEFT JOIN div_parents dp
    ON ms.ser_num = dp.serial_number
  LEFT JOIN post_reg_dash p
    ON ms.ser_num = p.serial_number
  LEFT JOIN pre_exam_rec pxu
    ON ms.ser_num = pxu.ser_num
GROUP BY ms.ser_num
HAVING MAX(date(ph_action_date)) > '{tm_status_date}'
-- This table is only being used for recent data. This condition ensures we are only looking at recent cases. This approach is used instead of filing date because post-reg actions cannot be filtered according to filing date. 
""")

# COMMAND ----------

# DBTITLE 1,Add Timestamps
case_dictionary_df = (
    case_dictionary_df.withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", f.lit("ETL"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", f.lit("ETL"))
)

# COMMAND ----------

# DBTITLE 1,Write to Table
target_table_name = f"{trgt_catalog}.gold.grand_model_milestone"
case_dictionary_df.write.mode("overwrite").insertInto(target_table_name)

# COMMAND ----------

# DBTITLE 1,End Job Control
recs_count = spark.table(target_table_name).count()

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    recs_count,
    "job completed successfully",
)
dbutils.notebook.exit(f"Completed Loading Grand Model Milestone Table")
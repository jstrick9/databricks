# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Metadata  
# MAGIC
# MAGIC **Created by:** Drew McPherson  
# MAGIC **Created on:** 2026-04-06  
# MAGIC **Last updated by:** Drew McPherson  
# MAGIC **Last updated on:** 2026-06-23  
# MAGIC
# MAGIC ## Changelog  
# MAGIC - **2026-04-06 (Drew McPherson):** Initial table creation.  
# MAGIC - **2026-05-12 (Drew McPherson):** Refactored current approach to autoprocessor to be case-based, rather than aggregate-based. Developed calculations for autoprocessor data from the Statement of Use and Extension Request processes.  Created new inventories for the Statement of Use Process: SOU Unassigned Inventory and SOU First Action. Resolved issue with month-to-date calculation.
# MAGIC - **2026-05-19 (Drew McPherson):** Inserted a condition that cases must both have a phase entry and exit date to be counted in inventory. This resolves an issue where EXT cases, which have many potential exits besides their extry/exit pair, had highly inflated output numbers. Also changed the initial tag for detailed inventories to “det” to avoid accidental double counting.
# MAGIC - **2026-06-09 (Drew McPherson):** Added addtional table output (tms_workflow_monitoring_detail). Modified EXT5 entry conditions to add a condition that would discover unlogged EXT5 events in order to adress backdating/timelag on EXT5 events. Adjusted DIV to "not child" rather than "parent". Removed "potential abandonment date" from exit criteria. Parameterized autoprocessors. Added reference appendices.
# MAGIC - **2026-06-23 (Drew McPherson):** Replaced trm_worker with {tmworker_catalog}. Removed EISU as an exit condition from Extension Requests. Applied EEXT "invisible extension" logic to all Extensions. Removed date restrictions from ITU records, enabling very old records to appear. Added itu/prg/pxu assignment columns to detail table.

# COMMAND ----------

# DBTITLE 1,Load Libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

# COMMAND ----------

# DBTITLE 1,Parameters and Configs
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

dbutils.widgets.text("tm_status_date", "2023-09-30") 
tm_status_date = dbutils.widgets.get("tm_status_date")

dbutils.widgets.text("calendar_start_date", "2024-10-01") 
calendar_start_date = dbutils.widgets.get("calendar_start_date")

pxu_autoprocessor = "30078" # Autoprocessor ID as defined by pre-exam. This will need to change if the ID is changed.
itu_autoprocessor = "88889" # Autoprocessor ID as defined by ITU. This will need to change if the ID is changed.
outlier_days = 1095 # Number of days a case can be open without being designated an outlier

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Environment Parameter Values
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["reporting_catalog"]
trgt_catalog = common_configs["schema"]["trgt_catalog"]
tmprodvty_catalog = common_configs["schema"]["tmprodvty_catalog"]
tmngpdb_src_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmworker_catalog = common_configs["schema"]["tmworker_catalog"]

# COMMAND ----------

# DBTITLE 1,Print values
print(f"{reporting_catalog=},{trgt_catalog=}")

# COMMAND ----------

# DBTITLE 1,Start Job Control
#%skip
job_name = "tms_workflow_monitoring"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Set Relevant Processes
processes = ["prg_s15_s15", "prg_06y_s08", "prg_10y_s89", "prg_06y_815", "prg_06y_s71", "prg_10y_s71", "prg_06y_715", "prg_10y_715", "prg_s07_s07", "prg_s07_sur", "prg_s07_7rf", "itu_ext_ex1", "itu_ext_ex2", "itu_ext_ex3", "itu_ext_ex4", "itu_ext_ex5", "int_pxu", "itu_sou", "itu_div"]
# det_sou_una and det_sou_fac are not included because they do not currently have dashboard usage and each process is compute-intensive. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Trademark Services Workload Monitoring Detail
# MAGIC
# MAGIC This provides a serial-number level table with start and end dates for each case.

# COMMAND ----------

# DBTITLE 0,Phase Gates
case_dictionary_exp_df = spark.sql(f"""

  -- =======================================================================================
  -- Post-Reg (PRG)
  -- =======================================================================================

  -- Begin with creating a CTE that directly draws Post-Reg start and end dates from the post-reg dashboard

WITH ranked_post_reg AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ser_num, postreg_category, start_5_characters
            ORDER BY start_action_date DESC
        ) AS rn
    FROM `{reporting_catalog}`.gold.post_reg_detail_dashboard
    WHERE start_action_date > '{tm_status_date}'
),

post_reg_dates AS (
  SELECT
    ser_num,
    MAX(CASE
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'ES71I'
        AND rn = 1
      THEN start_action_date
      END) AS prg_06y_s71_entry_dt,

    MAX(CASE 
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'ES71I'
        AND rn = 1
      THEN end_action_date
      END) AS prg_06y_s71_exit_dt,
    MAX(CASE
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'E815I'
        AND rn = 1
      THEN start_action_date
      END) AS prg_06y_815_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'E815I'
        AND rn = 1
      THEN end_action_date
      END) AS prg_06y_815_exit_dt,
    MAX(CASE
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'ES75I'
        AND rn = 1
      THEN start_action_date
      END) AS prg_06y_715_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'ES75I'
        AND rn = 1
      THEN end_action_date
      END) AS prg_06y_715_exit_dt,
    MAX(CASE
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'ES8RI'
        AND rn = 1
      THEN start_action_date
      END) AS prg_06y_s08_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '6 YEAR'
        AND start_5_characters = 'ES8RI'
        AND rn = 1
      THEN end_action_date
      END) AS prg_06y_s08_exit_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '10 YEAR'
        AND start_5_characters = 'E89RI'
        AND rn = 1
      THEN start_action_date
      END) AS prg_10y_s89_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '10 YEAR'
        AND start_5_characters = 'E89RI'
        AND rn = 1
      THEN end_action_date
      END) AS prg_10y_s89_exit_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '10 YEAR'
        AND start_5_characters = 'ES75I'
        AND rn = 1
      THEN start_action_date
      END) AS prg_10y_715_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '10 YEAR'
        AND start_5_characters = 'ES75I'
        AND rn = 1
      THEN end_action_date
      END) AS prg_10y_715_exit_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '10 YEAR'
        AND start_5_characters = 'ES71I'
        AND rn = 1
      THEN start_action_date
      END) AS prg_10y_s71_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = '10 YEAR'
        AND start_5_characters = 'ES71I'
        AND rn = 1
      THEN end_action_date
      END) AS prg_10y_s71_exit_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SECTION 7'
        AND start_5_characters = 'ES7SI'
        AND rn = 1
      THEN start_action_date
      END) AS prg_s07_sur_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SECTION 7'
        AND start_5_characters = 'ES7SI'
        AND rn = 1
      THEN end_action_date
      END) AS prg_s07_sur_exit_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SECTION 7'
        AND start_5_characters = 'ES7RI'
        AND rn = 1
      THEN start_action_date
      END) AS prg_s07_s07_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SECTION 7'
        AND start_5_characters = 'ES7RI'
        AND rn = 1
      THEN end_action_date
      END) AS prg_s07_s07_exit_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SECTION 7'
        AND start_5_characters = 'C7RFI'
        AND rn = 1
      THEN start_action_date
      END) AS prg_s07_7rf_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SECTION 7'
        AND start_5_characters = 'C7RFI'
        AND rn = 1
      THEN end_action_date
      END) AS prg_s07_7rf_exit_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SEPARATE 15'
        AND start_5_characters = 'E15RI'
        AND rn = 1
      THEN start_action_date
      END) AS prg_s15_s15_entry_dt,
    MAX(CASE 
      WHEN 
        postreg_category = 'SEPARATE 15'
        AND start_5_characters = 'E15RI'
        AND rn = 1
      THEN end_action_date
      END) AS prg_s15_s15_exit_dt
  FROM ranked_post_reg
  GROUP BY ser_num
),

autoprocessor_pxu AS (
  SELECT
    ser_num,
    MAX(
      CASE WHEN assignee = '{pxu_autoprocessor}'
      AND history_from = '101'
      AND history_to IN ('103', '103a')
      THEN 1 ELSE 0 END
    )                            AS int_pxu_auto,
    MIN_BY(assignee, history_ts) AS pxu_assigned_eid,
    CAST(MIN(history_ts) AS DATE) AS pxu_assigned_date
  FROM `{reporting_catalog}`.silver.pea_trademark_applications
  WHERE assignee IS NOT NULL
  GROUP BY ser_num
),

autoprocessor_itu AS(
  SELECT serial_number as ser_num, 
  MAX(
    CASE WHEN ph_action_code = 'SUPC'
    AND tm_worker_eid = '{itu_autoprocessor}'
    THEN 1 ELSE 0 END
  ) AS itu_sou_auto,
    MAX(
    CASE WHEN ph_action_code = 'EX1G'
    AND tm_worker_eid = '{itu_autoprocessor}'
    THEN 1 ELSE 0 END
  ) AS itu_ext_ex1_auto,
    MAX(
    CASE WHEN ph_action_code = 'EX2G'
    AND tm_worker_eid = '{itu_autoprocessor}'
    THEN 1 ELSE 0 END
  ) AS itu_ext_ex2_auto,
      MAX(
    CASE WHEN ph_action_code = 'EX3G'
    AND tm_worker_eid = '{itu_autoprocessor}'
    THEN 1 ELSE 0 END
  ) AS itu_ext_ex3_auto,
      MAX(
    CASE WHEN ph_action_code = 'EX4G'
    AND tm_worker_eid = '{itu_autoprocessor}'
    THEN 1 ELSE 0 END
  ) AS itu_ext_ex4_auto,
      MAX(
    CASE WHEN ph_action_code = 'EX5G'
    AND tm_worker_eid = '{itu_autoprocessor}'
    THEN 1 ELSE 0 END
  ) AS itu_ext_ex5_auto
  FROM `{reporting_catalog}`.silver.prosecution_history
  WHERE ph_action_code IN ('SUPC', 'EX1G', 'EX2G', 'EX3G', 'EX4G','EX5G')
  AND tm_worker_eid = '{itu_autoprocessor}'
  GROUP BY ser_num
  ),

ph_assignment AS (
  SELECT
    serial_number AS ser_num,
    MAX(CASE WHEN ph_action_code = 'AITU' THEN ph_action_date END)  AS itu_assigned_date,
    MAX_BY(CASE WHEN ph_action_code = 'AITU' THEN tm_worker_eid END, CASE WHEN ph_action_code = 'AITU' THEN ph_action_date END) AS itu_assigned_eid,
    MAX(CASE WHEN ph_action_code = 'APRE' THEN ph_action_date END)  AS prg_assigned_date,
    MAX_BY(CASE WHEN ph_action_code = 'APRE' THEN tm_worker_eid END, CASE WHEN ph_action_code = 'APRE' THEN ph_action_date END) AS prg_assigned_eid
  FROM `{reporting_catalog}`.silver.prosecution_history
  WHERE ph_action_code IN ('AITU', 'APRE')
  GROUP BY serial_number
),

employee AS (
    SELECT worker_no, worker_nm
    FROM `{tmworker_catalog}`.bronze.worker
)

SELECT
cd.ser_num,
cd.pre_exam_received_dt,

  -- =======================================================================================
  -- Pre-Exam (PXU)
  -- =======================================================================================

    -- Entry
    COALESCE(apx.int_pxu_auto, 0) AS int_pxu_auto,
    CASE
        WHEN cd.pre_exam_received_dt IS NOT NULL
          AND cd.pre_exam_received_dt > '{tm_status_date}'
          THEN cd.pre_exam_received_dt
        WHEN nwap_dt IS NOT NULL
          AND nwap_dt > '{tm_status_date}'
          THEN nwap_dt
        WHEN repr_dt IS NOT NULL
          AND repr_dt > '{tm_status_date}'
          THEN repr_dt

        END AS int_pxu_entry_dt,

    -- Exit

      CASE
        WHEN nwap_dt > nwos_dt
          THEN nwap_dt
        WHEN repr_dt > nwos_dt THEN repr_dt
        WHEN nwos_dt IS NOT NULL THEN nwos_dt
        WHEN
            nwap_dt IS NOT NULL
            AND nwos_dt IS NULL
            AND abandonment_dt IS NOT NULL
            AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
            THEN abn_max_dt
        ELSE NULL
      END AS int_pxu_exit_dt,

  -- =======================================================================================
  -- Statement of Use (SOU)
  -- =======================================================================================

    ----------------------------------------------------------------------------------------
    -- Overall
    ----------------------------------------------------------------------------------------
    ait.itu_sou_auto,
    -- Entry
      CASE
        WHEN eisu_dt IS NOT NULL
          --AND eisu_dt > '{tm_status_date}'
          THEN eisu_dt
        END AS itu_sou_entry_dt,

    -- Exit (exit must be >= entry to ensure we are in the current cycle)
      CASE
        WHEN supc_dt >= eisu_dt THEN supc_dt
        WHEN suna_dt >= eisu_dt THEN suna_dt
        WHEN iucn_dt >= eisu_dt THEN iucn_dt
        WHEN dp1b_dt >= eisu_dt THEN dp1b_dt
        WHEN aupc_dt >= eisu_dt THEN aupc_dt
        WHEN
          (supc_dt IS NULL OR supc_dt < eisu_dt)
          AND abandonment_dt IS NOT NULL
          AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
          AND abn_max_dt >= eisu_dt
          THEN abn_max_dt
          ELSE NULL
        END AS itu_sou_exit_dt,

    ----------------------------------------------------------------------------------------
    -- ITU DET (detail) SOU UNA Unassigned Inventory
    ----------------------------------------------------------------------------------------

    -- Detail phases (phases that break a process into shorter processes) are given the det designation so they are not double-counted with the main process.

    -- Entry
      CASE
        WHEN eisu_dt IS NOT NULL
          --AND eisu_dt > '{tm_status_date}'
          THEN eisu_dt
        END AS det_sou_una_entry_dt,

    -- Exit
      CASE
        WHEN eisu_dt IS NOT NULL AND aitu_dt IS NOT NULL THEN aitu_dt
        WHEN incs_min_dt >= eisu_dt THEN incs_min_dt
        WHEN eext_max_dt >= eisu_dt THEN eext_max_dt
        WHEN supc_dt >= eisu_dt THEN supc_dt
        WHEN suna_dt >= eisu_dt THEN suna_dt
        WHEN iucn_dt >= eisu_dt THEN iucn_dt
        WHEN dp1b_dt >= eisu_dt THEN dp1b_dt
        WHEN aupc_dt >= eisu_dt THEN aupc_dt
        WHEN abandonment_dt IS NOT NULL
          AND (abn_max_dt > revival_dt OR revival_dt IS NULL) 
          AND abn_max_dt >= eisu_dt
          THEN abn_max_dt
       ELSE NULL
       END AS det_sou_una_exit_dt,

    ----------------------------------------------------------------------------------------
    -- ITU DET (detail) SOU FAC First Action
    ----------------------------------------------------------------------------------------

    -- Detail phases (phases that break a process into shorter processes) are given the det designation so they are not double-counted with the main process.

    -- Entry
      CASE
        WHEN aitu_dt IS NOT NULL
          --AND aitu_dt > '{tm_status_date}'
          THEN aitu_dt
        END AS det_sou_fac_entry_dt,

    -- Exit
      CASE
        WHEN aitu_dt IS NOT NULL AND supc_dt IS NOT NULL THEN supc_dt
        WHEN aitu_dt IS NOT NULL AND incs_min_dt IS NOT NULL THEN incs_min_dt
        WHEN aitu_dt IS NOT NULL AND suna_dt IS NOT NULL THEN suna_dt
        WHEN aitu_dt IS NOT NULL AND iucn_dt IS NOT NULL THEN iucn_dt
        WHEN aitu_dt IS NOT NULL AND dp1b_dt IS NOT NULL THEN dp1b_dt
        WHEN aitu_dt IS NOT NULL AND aupc_dt IS NOT NULL THEN aupc_dt
        WHEN aitu_dt IS NOT NULL AND abandonment_dt IS NOT NULL
          AND (abn_max_dt > revival_dt OR revival_dt IS NULL) 
          THEN abn_max_dt
        ELSE NULL
        END AS det_sou_fac_exit_dt,


  -- =======================================================================================
  -- Extension Requests (EXT)
  -- =======================================================================================

    ----------------------------------------------------------------------------------------
    -- Overall
    ----------------------------------------------------------------------------------------

      -- Entry
        ait.itu_ext_ex1_auto,
        ait.itu_ext_ex2_auto,
        ait.itu_ext_ex3_auto,
        ait.itu_ext_ex4_auto,
        ait.itu_ext_ex5_auto,
        CASE
            WHEN ext1_dt IS NOT NULL
              --AND ext1_dt > '{tm_status_date}' 
              THEN ext1_dt 
            WHEN ext1_dt IS NULL
              AND eext_max_dt IS NOT NULL
              THEN eext_max_dt
          END AS itu_ext_ex1_entry_dt,       
        CASE
            WHEN ext2_dt IS NOT NULL
              --AND ext2_dt > '{tm_status_date}'
              THEN ext2_dt
            -- Catch "invisible" EXT2 entry records where the extension request is in progress but not yet backdated.
            WHEN ext2_dt IS NULL
              AND eext_max_dt > ex1g_dt
              THEN eext_max_dt
          END AS itu_ext_ex2_entry_dt,
        CASE
            WHEN ext3_dt IS NOT NULL
              --AND ext3_dt > '{tm_status_date}'
              THEN ext3_dt
            -- Catch "invisible" EXT3 entry records where the extension request is in progress but not yet backdated.
            WHEN ext3_dt IS NULL
              AND eext_max_dt > ex2g_dt
              THEN eext_max_dt
          END AS itu_ext_ex3_entry_dt,
        CASE
            WHEN ext4_dt IS NOT NULL
              --AND ext4_dt > '{tm_status_date}'
              THEN ext4_dt
            -- Catch "invisible" EXT4 entry records where the extension request is in progress but not yet backdated.
            WHEN ext4_dt IS NULL
              AND eext_max_dt > ex3g_dt
              THEN eext_max_dt
          END AS itu_ext_ex4_entry_dt,        
        CASE
            WHEN ext5_dt IS NOT NULL
              --AND ext5_dt > '{tm_status_date}'
              THEN ext5_dt 
            -- EXT5s are generally processed manually. Often, the EXT5 ph action is not logged until after the record is processed, making it invisible. The condition below is designed to catch "invisible" EXT5 entry records.
            WHEN ext5_dt IS NULL
              AND eext_max_dt > ex4g_dt
              THEN eext_max_dt
            END AS itu_ext_ex5_entry_dt,
      -- Exit
          CASE
            WHEN ext1_dt > ex1g_dt THEN ext1_dt
            WHEN ex1g_dt IS NOT NULL THEN ex1g_dt
            --WHEN ex1g_dt IS NULL AND eisu_dt IS NOT NULL THEN eisu_dt
            WHEN ex1g_dt IS NULL AND (ext1_dt IS NOT NULL OR eext_max_dt IS NOT NULL) AND iucn_dt IS NOT NULL THEN iucn_dt
            WHEN ex1g_dt IS NULL AND (ext1_dt IS NOT NULL OR eext_max_dt IS NOT NULL) AND supc_dt IS NOT NULL THEN supc_dt
            WHEN 
                ex1g_dt IS NULL
                AND (ext1_dt IS NOT NULL OR eext_max_dt IS NOT NULL)
                --AND eisu_dt IS NULL
                AND supc_dt IS NULL
                AND iucn_dt IS NULL
                AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
                THEN abn_max_dt
            WHEN abandonment_dt IS NOT NULL AND (ext1_dt IS NOT NULL OR eext_max_dt IS NOT NULL) then abandonment_dt
            END AS itu_ext_ex1_exit_dt,
        CASE
            WHEN ext2_dt > ex2g_dt THEN ext2_dt
            WHEN ex2g_dt IS NOT NULL THEN ex2g_dt 
            --WHEN ex2g_dt IS NULL AND eisu_dt IS NOT NULL THEN eisu_dt
            WHEN ex2g_dt IS NULL AND (ext2_dt IS NOT NULL OR eext_max_dt > ex1g_dt) AND iucn_dt IS NOT NULL THEN iucn_dt
            WHEN ex2g_dt IS NULL AND (ext2_dt IS NOT NULL OR eext_max_dt > ex1g_dt) AND supc_dt IS NOT NULL THEN supc_dt
            WHEN 
                ex2g_dt IS NULL
                --AND eisu_dt IS NULL
                AND (ext2_dt IS NOT NULL OR eext_max_dt > ex1g_dt)
                AND supc_dt IS NULL
                AND iucn_dt IS NULL
                AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
                THEN abn_max_dt
            WHEN abandonment_dt IS NOT NULL AND (ext2_dt IS NOT NULL OR eext_max_dt > ex1g_dt) then abandonment_dt
            END AS itu_ext_ex2_exit_dt,
        CASE
            WHEN ext3_dt > ex3g_dt THEN ext3_dt
            WHEN ex3g_dt IS NOT NULL THEN ex3g_dt
            --WHEN ex3g_dt IS NULL AND eisu_dt IS NOT NULL THEN eisu_dt
            WHEN ex3g_dt IS NULL AND (ext3_dt IS NOT NULL OR eext_max_dt > ex2g_dt) AND iucn_dt IS NOT NULL THEN iucn_dt
            WHEN ex3g_dt IS NULL AND (ext3_dt IS NOT NULL OR eext_max_dt > ex2g_dt) AND supc_dt IS NOT NULL THEN supc_dt
            WHEN 
                ex3g_dt IS NULL
                --AND eisu_dt IS NULL
                AND (ext3_dt IS NOT NULL OR eext_max_dt > ex2g_dt)
                AND supc_dt IS NULL
                AND iucn_dt IS NULL
                AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
                THEN abn_max_dt
            WHEN abandonment_dt IS NOT NULL AND (ext3_dt IS NOT NULL OR eext_max_dt > ex2g_dt) then abandonment_dt
            END AS itu_ext_ex3_exit_dt,
        CASE
            WHEN ext4_dt > ex4g_dt THEN ext4_dt
            WHEN ex4g_dt IS NOT NULL THEN ex4g_dt
            --WHEN ex4g_dt IS NULL AND eisu_dt IS NOT NULL THEN eisu_dt
            WHEN ex4g_dt IS NULL AND (ext4_dt IS NOT NULL OR eext_max_dt > ex3g_dt) AND iucn_dt IS NOT NULL THEN iucn_dt
            WHEN ex4g_dt IS NULL AND (ext4_dt IS NOT NULL OR eext_max_dt > ex3g_dt) AND supc_dt IS NOT NULL THEN supc_dt
            WHEN
                ex4g_dt IS NULL
                --AND eisu_dt IS NULL
                AND (ext4_dt IS NOT NULL OR eext_max_dt > ex3g_dt)
                AND supc_dt IS NULL
                AND iucn_dt IS NULL
                AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
                THEN abn_max_dt
            WHEN abandonment_dt IS NOT NULL AND (ext4_dt IS NOT NULL OR eext_max_dt > ex3g_dt) then abandonment_dt
            END AS itu_ext_ex4_exit_dt,
        CASE
            WHEN ext5_dt > ex5g_dt THEN ext5_dt
            WHEN ex5g_dt IS NOT NULL THEN ex5g_dt
            --WHEN ex5g_dt IS NULL AND eisu_dt IS NOT NULL THEN eisu_dt
            WHEN ex5g_dt IS NULL AND (ext5_dt IS NOT NULL OR eext_max_dt > ex4g_dt) AND iucn_dt IS NOT NULL THEN iucn_dt
            WHEN ex5g_dt IS NULL AND (ext5_dt IS NOT NULL OR eext_max_dt > ex4g_dt) AND supc_dt IS NOT NULL THEN supc_dt
            WHEN
                ex5g_dt IS NULL
                --AND eisu_dt IS NULL
                AND (ext5_dt IS NOT NULL OR eext_max_dt > ex4g_dt)
                AND supc_dt IS NULL
                AND iucn_dt IS NULL
                AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
                THEN abn_max_dt
            WHEN abandonment_dt IS NOT NULL AND (ext5_dt IS NOT NULL OR eext_max_dt > ex4g_dt) then abandonment_dt
            END AS itu_ext_ex5_exit_dt,

  -- =======================================================================================
  -- Divisional Requests (DIV)
  -- =======================================================================================

    -- Entry (uses COALESCE+GREATEST to get the most recent divisional request, since divisionals are repeatable)
      CASE
        WHEN 
            (drrr_dt IS NOT NULL OR ertd_dt IS NOT NULL)
            AND div_child IS NOT TRUE
            THEN COALESCE(GREATEST(drrr_dt, ertd_dt), drrr_dt, ertd_dt)
        END AS itu_div_entry_dt,

    -- Exit (exit must be >= entry to ensure we are in the current cycle)
      CASE
        WHEN 
            (drrr_dt IS NOT NULL OR ertd_dt IS NOT NULL)
            AND div_child IS NOT TRUE
            AND dpcc_dt IS NOT NULL
            AND dpcc_dt >= COALESCE(GREATEST(drrr_dt, ertd_dt), drrr_dt, ertd_dt)
            THEN dpcc_dt
        WHEN 
            (drrr_dt IS NOT NULL OR ertd_dt IS NOT NULL)
            AND div_child IS NOT TRUE
            AND untd_dt IS NOT NULL
            AND untd_dt >= COALESCE(GREATEST(drrr_dt, ertd_dt), drrr_dt, ertd_dt)
            THEN untd_dt
        WHEN
            (drrr_dt IS NOT NULL OR ertd_dt IS NOT NULL)
            AND dpcc_dt IS NULL
            AND untd_dt IS NULL
            AND div_child IS NOT TRUE
            AND abandonment_dt IS NOT NULL
            AND (abn_max_dt > revival_dt OR revival_dt IS NULL)
            AND abn_max_dt >= COALESCE(GREATEST(drrr_dt, ertd_dt), drrr_dt, ertd_dt)
            THEN abn_max_dt
        WHEN
            (drrr_dt IS NOT NULL OR ertd_dt IS NOT NULL)
            AND dpcc_dt IS NULL
            AND untd_dt IS NULL
            AND div_child IS NOT TRUE
            AND abandonment_dt IS NOT NULL
            AND prg_c8oo_dt >= COALESCE(GREATEST(drrr_dt, ertd_dt), drrr_dt, ertd_dt)
            THEN prg_c8oo_dt
        WHEN
            (drrr_dt IS NOT NULL OR ertd_dt IS NOT NULL)
            AND dpcc_dt IS NULL
            AND untd_dt IS NULL
            AND div_child IS NOT TRUE
            AND abandonment_dt IS NOT NULL
            AND prg_c8ot_dt >= COALESCE(GREATEST(drrr_dt, ertd_dt), drrr_dt, ertd_dt)
            THEN prg_c8ot_dt
        WHEN
            (drrr_dt IS NOT NULL OR ertd_dt IS NOT NULL)
            AND dpcc_dt IS NULL
            AND untd_dt IS NULL
            AND div_child IS NOT TRUE
            AND abandonment_dt IS NOT NULL
            AND prg_caex_dt >= COALESCE(GREATEST(drrr_dt, ertd_dt), drrr_dt, ertd_dt)
            THEN prg_caex_dt
          ELSE NULL
        END AS itu_div_exit_dt,

  -- =======================================================================================
  -- Post-Registration
  -- =======================================================================================

    ----------------------------------------------------------------------------------------
    -- Section 8: Declaration of Use and Excusable Nonuse
    -- 6 Year
    ----------------------------------------------------------------------------------------

    -- Relevant Codes
        -- ES8R: TEAS SECTION 8 RECEIVED
        -- 8.AF: REGISTERED - SEC. 8 (6-YR) FILED
        -- 8AFT: REGISTERED - SEC. 8 (10-YR) FILED/CHECK RECORD FOR SEC. 9

      prg.prg_06y_s08_entry_dt,
      prg.prg_06y_s08_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 8-15:	Declaration of Use and Excusable Nonuse and Incontestability
    -- 6 Year
    ----------------------------------------------------------------------------------------

      -- Relevant Codes
        -- E815: TEAS SECTION 8 & 15 RECEIVED
        -- 815F: REGISTERED - SEC. 8 (6-YR) & SEC. 15 FILED

      prg.prg_06y_815_entry_dt,
      prg.prg_06y_815_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 71: Declaration of Use and Excusable Nonuse
    -- 6 Year
    -- Registered Extension Protection (Registered 66a Filing)
    ----------------------------------------------------------------------------------------

    -- Relevant Codes
        -- ES71: TEAS SECTION 71 RECEIVED
        -- 71AF: REGISTERED-SEC.71 FILED

      prg.prg_06y_s71_entry_dt,
      prg.prg_06y_s71_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 71-15: Declaration of Use and Excusable Nonuse and Incontestability 
    -- 6 Year
    -- Registered Extension Protection (Registered 66a Filing)
    ----------------------------------------------------------------------------------------

      -- Relevant Codes
        -- ES75: TEAS SECTION 71 & 15 RECEIVED
        -- 715F: REGISTERED - SEC. 71 & SEC. 15 FILED

      prg.prg_06y_715_entry_dt,
      prg.prg_06y_715_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 8-9 Declaration of Use and Excusable Nonuse for Renewal
    -- 10 Year
    ----------------------------------------------------------------------------------------

     -- Relevant Codes
        -- E89R: TEAS SECTION 8 & 9 RECEIVED
        -- 8AFT: REGISTERED - SEC. 8 (10-YR) FILED/CHECK RECORD FOR SEC. 9
        -- 9.AF: REGISTERED - SEC. 9 FILED/CHECK RECORD FOR SEC. 8
        -- 89AF: REGISTERED - COMBINED SECTION 8 (10-YR) & SEC. 9 FILED

      prg.prg_10y_s89_entry_dt,
      prg.prg_10y_s89_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 71: Declaration of Use and Excusable Nonuse
    -- 10 Year
    -- Registered Extension Protection (Registered 66a Filing)
    ----------------------------------------------------------------------------------------

    -- Relevant Codes
      -- ES71: TEAS SECTION 71 RECEIVED
      -- 71AF: REGISTERED-SEC.71 FILED

      prg.prg_10y_s71_entry_dt,
      prg.prg_10y_s71_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 71-15: Declaration of Use and Excusable Nonuse and Incontestability
    -- 10 Year
    -- Registered Extension Protection (Registered 66a Filing)
    ----------------------------------------------------------------------------------------

      -- Relevant Codes
        -- ES75: TEAS SECTION 71 & 15 RECEIVED
        -- 715F: REGISTERED - SEC. 71 & SEC. 15 FILED

      prg.prg_10y_715_entry_dt,
      prg.prg_10y_715_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 7: Request for Amendment or Correction 
    ----------------------------------------------------------------------------------------

      -- Relevant Codes
        -- ES7R: TEAS SECTION 7 REQUEST RECEIVED
        -- AMD7: SEC 7 REQUEST FILED
        -- C.7F: REQUEST FOR NEW CERTIFICATE FILED

      prg.prg_s07_s07_entry_dt,
      prg.prg_s07_s07_exit_dt,
      prg.prg_s07_7rf_entry_dt,
      prg.prg_s07_7rf_exit_dt,

    ----------------------------------------------------------------------------------------
    -- SUR Section 7: Surrender Registration for Cancellation 
    ----------------------------------------------------------------------------------------

      -- Relevant Codes
        -- ES7S: TEAS SECTION 7 SURRENDER RECEIVED
        -- C7PF: RQST FOR SECT 7 PARTIAL SURRENDER FILED
        -- C7RF: REQUEST FOR SECT 7 TOTAL SURRENDER FILED

      prg.prg_s07_sur_entry_dt,
      prg.prg_s07_sur_exit_dt,

    ----------------------------------------------------------------------------------------
    -- Section 15:  Incontestability
    ----------------------------------------------------------------------------------------

    -- Relevant Codes
        -- E15R: TEAS SECTION 15 RECEIVED
        -- 15AF: REGISTERED - SEC. 15 AFFIDAVIT FILED

      prg.prg_s15_s15_entry_dt,
      prg.prg_s15_s15_exit_dt,

  -- =======================================================================================
  -- ITU Assignment (AITU)
  -- =======================================================================================

    pa.itu_assigned_date,
    pa.itu_assigned_eid,
    emp_itu.worker_nm          AS itu_assigned_name,

  -- =======================================================================================
  -- Post-Reg Assignment (APRE)
  -- =======================================================================================

    pa.prg_assigned_date,
    pa.prg_assigned_eid,
    emp_prg.worker_nm          AS prg_assigned_name,

  -- =======================================================================================
  -- Pre-Exam Assignment (PXU)
  -- =======================================================================================

    apx.pxu_assigned_eid,
    apx.pxu_assigned_date,
    emp_pxu.worker_nm          AS pxu_assigned_name

FROM `{reporting_catalog}`.gold.grand_model_milestone cd
LEFT JOIN post_reg_dates prg
ON cd.ser_num = prg.ser_num
LEFT JOIN autoprocessor_itu ait
ON cd.ser_num = ait.ser_num
LEFT JOIN autoprocessor_pxu apx
ON cd.ser_num = apx.ser_num
LEFT JOIN ph_assignment pa
ON cd.ser_num = pa.ser_num
LEFT JOIN employee emp_itu
ON pa.itu_assigned_eid = emp_itu.worker_no
LEFT JOIN employee emp_prg
ON pa.prg_assigned_eid = emp_prg.worker_no
LEFT JOIN employee emp_pxu
ON apx.pxu_assigned_eid = emp_pxu.worker_no

""")

case_dictionary_exp_df.cache()
case_dictionary_exp_df.count()  # IMPORTANT - Materialize the cache
case_dictionary_exp_df.createOrReplaceTempView("case_dictionary_exp")
#display(case_dictionary_exp_df)

# COMMAND ----------

# DBTITLE 1,Select and Reorder Columns
case_dictionary_columns = set(case_dictionary_exp_df.columns)

case_dictionary_exp_reordered_df = case_dictionary_exp_df.select(
    "ser_num",
    "int_pxu_entry_dt",
    "int_pxu_exit_dt",
    "int_pxu_auto",
    "itu_sou_entry_dt",
    "itu_sou_exit_dt",
    "itu_sou_auto",
    "itu_ext_ex1_entry_dt",
    "itu_ext_ex1_exit_dt",
    "itu_ext_ex1_auto",
    "itu_ext_ex2_entry_dt",
    "itu_ext_ex2_exit_dt",
    "itu_ext_ex2_auto",
    "itu_ext_ex3_entry_dt",
    "itu_ext_ex3_exit_dt",
    "itu_ext_ex3_auto",
    "itu_ext_ex4_entry_dt",
    "itu_ext_ex4_exit_dt",
    "itu_ext_ex4_auto",
    "itu_ext_ex5_entry_dt",
    "itu_ext_ex5_exit_dt",
    "itu_ext_ex5_auto",
    "itu_div_entry_dt",
    "itu_div_exit_dt",
    "prg_06y_s08_entry_dt",
    "prg_06y_s08_exit_dt",
    "prg_06y_815_entry_dt",
    "prg_06y_815_exit_dt",
    "prg_06y_s71_entry_dt",
    "prg_06y_s71_exit_dt",
    "prg_06y_715_entry_dt",
    "prg_06y_715_exit_dt",
    "prg_10y_s89_entry_dt",
    "prg_10y_s89_exit_dt",
    "prg_10y_s71_entry_dt",
    "prg_10y_s71_exit_dt",
    "prg_10y_715_entry_dt",
    "prg_10y_715_exit_dt",
    "prg_s07_s07_entry_dt",
    "prg_s07_s07_exit_dt",
    "prg_s07_7rf_entry_dt",
    "prg_s07_7rf_exit_dt",
    "prg_s07_sur_entry_dt",
    "prg_s07_sur_exit_dt",
    "prg_s15_s15_entry_dt",
    "prg_s15_s15_exit_dt",
    "itu_assigned_date",
    "itu_assigned_eid",
    "itu_assigned_name",
    "prg_assigned_date",
    "prg_assigned_eid",
    "prg_assigned_name",
    "pxu_assigned_date",
    "pxu_assigned_eid",
    "pxu_assigned_name"
).withColumn("create_ts", F.current_timestamp()).withColumn("create_user_id", F.lit("ETL")).withColumn("update_ts", F.current_timestamp()).withColumn("update_user_id", F.lit("ETL"))
#display(case_dictionary_exp_reordered_df.filter(F.col("itu_div_entry_dt").isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Trademark Services Workload Monitoring Dashboard
# MAGIC
# MAGIC This is an aggregated daily count of records for each process.

# COMMAND ----------

# DBTITLE 1,Monthly Workers
workers_df = spark.sql(f"""
WITH pxu_workers AS (SELECT
  DATE_TRUNC('MONTH', calendar_day) AS month,
  COUNT(DISTINCT worker_nm) AS pxu_workers
FROM `{reporting_catalog}`.gold.pea_worker_performance
WHERE
  daily_teas_processed + daily_madrd_processed + daily_paper_processed > 0
  AND worker_nm != 'Auto-Processor'
GROUP BY ALL
)

SELECT
  DATE_TRUNC('MONTH', transaction_effective_dt) AS month,
  COUNT(DISTINCT CASE WHEN dn_worker_role_cd = 'IUP' 
    THEN cfk_worker_gid 
    END) AS itu_workers,
  COUNT(DISTINCT CASE WHEN dn_worker_role_cd = 'PRE' 
    THEN cfk_worker_gid 
    END) AS prg_workers,
    p.pxu_workers
FROM `{tmprodvty_catalog}`.bronze.production_transaction_live t
LEFT JOIN pxu_workers p 
ON DATE_TRUNC('MONTH', t.transaction_effective_dt) = p.month
WHERE dn_worker_role_cd IN ('IUP', 'PRE')
GROUP BY ALL
""")

workers_df.createOrReplaceTempView("workers")

# COMMAND ----------

# DBTITLE 1,Calendar (Past)
# This step creates a generic blank calendar that other dataframes will use to align their daily counts. 

calendar_df = spark.sql(f"""
SELECT explode(
    sequence(
      to_date('{calendar_start_date}'),
      current_date(),
      interval 1 day
    )
  ) AS calendar_day
""")
calendar_df.createOrReplaceTempView("calendar")
#display(calendar_df)

# COMMAND ----------

# DBTITLE 1,Calendar (Future)
# Create a calendar for the next 12 months starting from tomorrow
future_calendar_df = spark.sql("""
SELECT explode(
    sequence(
      current_date() + interval 1 day,
      add_months(current_date(), 12),
      interval 1 day
    )
  ) AS calendar_day
""")
future_calendar_df.createOrReplaceTempView("future_calendar")
#display(future_calendar_df)

# COMMAND ----------

# DBTITLE 1,F Case Flow Monthly
def case_flow_monthly(process):
    entry_col = f"{process}_entry_dt"
    exit_col = f"{process}_exit_dt"
    auto_col = f"{process}_auto"

    # Check if autoprocessor column exists
    has_auto = auto_col in case_dictionary_columns

    # Entry counts
    entry_df = spark.sql(
        f"""
        SELECT 
            DATE_TRUNC('MONTH', {entry_col}) AS month,
            COUNT(*) AS intake
        FROM case_dictionary_exp
        WHERE {entry_col} IS NOT NULL
        GROUP BY ALL
        """
    )
    
    # Output counts with optional autoprocessor aggregation
    if has_auto:
        output_df = spark.sql(
            f"""
            SELECT 
                DATE_TRUNC('MONTH', {exit_col}) AS month,
                COUNT(*) AS output,
                SUM(COALESCE({auto_col}, 0)) AS monthly_output_auto
            FROM case_dictionary_exp
            WHERE {exit_col} IS NOT NULL
            AND {entry_col} IS NOT NULL
            GROUP BY ALL
            """
    )

    else:
        output_df = spark.sql(
            f"""
            SELECT 
                DATE_TRUNC('MONTH', {exit_col}) AS month,
                COUNT(*) AS output,
                0 AS monthly_output_auto
            FROM case_dictionary_exp
            WHERE {exit_col} IS NOT NULL
            AND {entry_col} IS NOT NULL
            GROUP BY ALL
            """
        )

    # Join everything
    flow_df = entry_df.join(
        output_df,
        on="month",
        how="outer"
    ).filter(
        f"month > '{tm_status_date}'"
    ).selectExpr(
        "month",
        "COALESCE(intake, 0) AS intake",
        "COALESCE(output, 0) AS output",
        "COALESCE(monthly_output_auto, 0) AS monthly_output_auto",
        "COALESCE(output, 0) - COALESCE(monthly_output_auto, 0) AS monthly_output_manual"
    )

    flow_df.createOrReplaceTempView(f"{process}_monthly_flow")
    return(flow_df)

# COMMAND ----------

# DBTITLE 1,F Case Flow Daily
def case_flow_daily(process):
    entry_col = f"{process}_entry_dt"
    exit_col = f"{process}_exit_dt"
    auto_col = f"{process}_auto"

    # Check if autoprocessor column exists
    has_auto = auto_col in case_dictionary_columns

    entry_df = spark.sql(
        f"""
        SELECT 
            {entry_col} AS calendar_day,
            COUNT(*) AS intake
        FROM case_dictionary_exp
        WHERE {entry_col} IS NOT NULL
        GROUP BY calendar_day
        """
    )

    if has_auto:
        output_df = spark.sql(
            f"""
            SELECT 
                {exit_col} AS calendar_day,
                COUNT(*) AS output,
                SUM(COALESCE({auto_col}, 0)) AS output_auto,
                ROUND(AVG(CASE WHEN DATEDIFF(DAY, {entry_col}, {exit_col}) <= {outlier_days} 
                    THEN DATEDIFF(DAY, {entry_col}, {exit_col}) ELSE NULL END), 1) AS {process}_avg_resolution_days,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
                    CASE WHEN DATEDIFF(DAY, {entry_col}, {exit_col}) <= {outlier_days} 
                    THEN DATEDIFF(DAY, {entry_col}, {exit_col}) ELSE NULL END), 1) AS {process}_median_resolution_days
            FROM case_dictionary_exp
            WHERE {exit_col} IS NOT NULL
            AND {entry_col} IS NOT NULL
            GROUP BY calendar_day
            """
        )

    else:
        output_df = spark.sql(
            f"""
            SELECT 
                {exit_col} AS calendar_day,
                COUNT(*) AS output,
                0 AS output_auto,
                ROUND(AVG(CASE WHEN DATEDIFF(DAY, {entry_col}, {exit_col}) <= {outlier_days} 
                    THEN DATEDIFF(DAY, {entry_col}, {exit_col}) ELSE NULL END), 1) AS {process}_avg_resolution_days,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
                    CASE WHEN DATEDIFF(DAY, {entry_col}, {exit_col}) <= {outlier_days} 
                    THEN DATEDIFF(DAY, {entry_col}, {exit_col}) ELSE NULL END), 1) AS {process}_median_resolution_days
            FROM case_dictionary_exp
            WHERE {exit_col} IS NOT NULL
            AND {entry_col} IS NOT NULL
            GROUP BY calendar_day
            """
        )

# Calculate resolution averages only on days with 5 or more cases to reduce the effect of outliers.

    output_df = output_df.withColumn(
    f"{process}_avg_resolution_days",
    F.when(F.col("output") >= 5, F.col(f"{process}_avg_resolution_days")).otherwise(None)
    ).withColumn(
    f"{process}_median_resolution_days",
    F.when(F.col("output") >= 5, F.col(f"{process}_median_resolution_days")).otherwise(None)
)

# Join input and output

    flow_df = entry_df.join(
        output_df,
        on="calendar_day",
        how="outer"
    ).filter(
        f"calendar_day > '{tm_status_date}'"
    ).selectExpr(
        "calendar_day",
        "COALESCE(intake, 0) AS intake",
        "COALESCE(output, 0) AS output",
        "COALESCE(output_auto, 0) AS output_auto",
        "COALESCE(output, 0) - COALESCE(output_auto, 0) AS output_manual",
        f"{process}_avg_resolution_days",
        f"{process}_median_resolution_days"
    )

    flow_df.createOrReplaceTempView(f"{process}_daily_flow")
    return(flow_df)

# COMMAND ----------

# DBTITLE 1,F Daily Averages
# Calculate average daily intake and output from the past year

def case_flow_daily_avg(process):
    daily_flow_table = f"{process}_daily_flow"
    avg_table = f"{process}_daily_avg"
    avg_df = spark.sql(
        f"""
        SELECT
            AVG(intake) AS {process}_avg_daily_intake,
            AVG(output) AS {process}_avg_daily_output,
            AVG(output_auto) AS {process}_avg_daily_output_auto,
            AVG(output_manual) AS {process}_avg_daily_output_manual,
            AVG(intake - output) AS {process}_avg_daily_net_change
        FROM {daily_flow_table}
        WHERE calendar_day >= DATE_SUB(current_date(), 365)
          AND calendar_day < current_date()
        """
    )
    avg_df.createOrReplaceTempView(f"{process}_daily_avg")
    return avg_df

# COMMAND ----------

# DBTITLE 1,F Backlog_Past
def backlog_past(process):
    backlog_df = spark.sql(
        f"""
        SELECT
          c.calendar_day AS calendar_day,
          COUNT(DISTINCT dic.ser_num) AS cases
        FROM calendar c
        LEFT JOIN case_dictionary_exp dic
          ON c.calendar_day >= dic.{process}_entry_dt
          AND (c.calendar_day <= dic.{process}_exit_dt OR dic.{process}_exit_dt IS NULL)
        GROUP BY calendar_day
        """
    )
    backlog_df.createOrReplaceTempView(f"{process}_backlog")
    return(backlog_df)

# COMMAND ----------

# DBTITLE 1,F Backlog_Projected
def backlog_projected(process):
    inv_projection_df = spark.sql(
        f"""
        WITH current_backlog AS (
          SELECT cases AS {process}_current_inv
          FROM {process}_backlog
          WHERE calendar_day = current_date()
        ),
        future_days AS (
          SELECT 
            calendar_day,
            DATEDIFF(calendar_day, current_date()) AS days_from_today
          FROM future_calendar
        ),
        averages AS (
          SELECT 
            {process}_avg_daily_intake,
            {process}_avg_daily_output,
            {process}_avg_daily_output_auto,
            {process}_avg_daily_output_manual,
            {process}_avg_daily_net_change
          FROM {process}_daily_avg
        )
        SELECT 
          fd.calendar_day,
          GREATEST(0, CAST(ci.{process}_current_inv + (fd.days_from_today * a.{process}_avg_daily_net_change) AS INT)) AS projected_cases,
          CAST(ROUND(a.{process}_avg_daily_intake, 0) AS INT) AS daily_intake,
          CAST(ROUND(a.{process}_avg_daily_output, 0) AS INT) AS daily_output,
          CAST(ROUND(a.{process}_avg_daily_output_auto, 0) AS INT) AS daily_output_auto,
          CAST(ROUND(a.{process}_avg_daily_output_manual, 0) AS INT) AS daily_output_manual,
          CAST(ROUND(a.{process}_avg_daily_net_change, 0) AS INT) AS daily_net_change
        FROM future_days fd
        CROSS JOIN current_backlog ci
        CROSS JOIN averages a
        """
    )
    inv_projection_df.createOrReplaceTempView(f"{process}_inv_projection")
    return(inv_projection_df)

# COMMAND ----------

# DBTITLE 1,F Full_Backlog
def full_backlog(process):
    df = spark.sql(
        f"""
        SELECT 
          b.calendar_day,
          b.cases,
          COALESCE(df.intake, 0) AS intake,
          COALESCE(df.output, 0) AS output,
          COALESCE(df.output_auto, 0) AS output_auto,
          COALESCE(df.output_manual, 0) AS output_manual,
          COALESCE(mf.intake, 0) AS monthly_intake,
          COALESCE(mf.output, 0) AS monthly_output,
          COALESCE(mf.monthly_output_auto, 0) AS monthly_output_auto,
          COALESCE(mf.monthly_output_manual, 0) AS monthly_output_manual,
          ROUND(df.{process}_avg_resolution_days, 1) AS avg_resolution_days,
          df.{process}_median_resolution_days AS median_resolution_days,
          CASE 
            WHEN SUBSTR('{process}', 1, 3) LIKE '%int%'
            THEN w.pxu_workers
            WHEN  SUBSTR('{process}', 1, 3) LIKE '%itu%'
            THEN w.itu_workers
            WHEN  SUBSTR('{process}', 1, 3) LIKE '%prg%'
            THEN w.prg_workers
            END AS monthly_workers,
          SUBSTR('{process}', 1, 3) AS phase,
          SUBSTR('{process}', 1, 7) AS grouping,
          '{process}' AS process,
          'Historical' AS time_period
        FROM {process}_backlog b
        LEFT JOIN {process}_daily_flow df
          ON b.calendar_day = df.calendar_day
        LEFT JOIN {process}_monthly_flow mf
          ON DATE_TRUNC('MONTH',b.calendar_day) = mf.month
        LEFT JOIN workers w
          ON DATE_TRUNC('MONTH',b.calendar_day) = w.month

        UNION ALL

        SELECT
          calendar_day,
          projected_cases AS cases,
          daily_intake,
          daily_output,
          daily_output_auto,
          daily_output_manual,
          daily_intake * DAY(LAST_DAY(calendar_day)) AS monthly_intake,
          daily_output * DAY(LAST_DAY(calendar_day)) AS monthly_output,
          daily_output_auto * DAY(LAST_DAY(calendar_day)) AS monthly_output_auto,
          daily_output_manual * DAY(LAST_DAY(calendar_day)) AS monthly_output_manual,
          NULL AS avg_resolution_days,
          NULL AS median_resolution_days,
          NULL AS monthly_workers,
          SUBSTR('{process}', 1, 3) AS phase,
          SUBSTR('{process}', 1, 7) AS grouping,
          '{process}' AS process,
          'Future' AS time_period
        FROM {process}_inv_projection
        """
    )
    df.createOrReplaceTempView(f"{process}_full_backlog")
    return(df)

# COMMAND ----------

# DBTITLE 1,Create Individual Dataframes
for process in processes:
    case_flow_daily(process)
    case_flow_monthly(process)
    case_flow_daily_avg(process)
    backlog_past(process)
    backlog_projected(process)
    full_backlog(process)

    for suffix in ["daily_flow", "monthly_flow", "daily_avg", "backlog", "inv_projection"]:
        spark.catalog.dropTempView(f"{process}_{suffix}")


# COMMAND ----------

# DBTITLE 1,Union Dataframes
batch_size = 6
batch_union_tables = []

for i in range(0, len(processes), batch_size):
    batch = processes[i:i+batch_size]
    dfs = []
    for proc in batch:
        df = (
            spark.table(f"{proc}_full_backlog")
            .select(
                "calendar_day",
                "cases",
                "intake",
                "output",
                "output_auto",
                "output_manual",
                "monthly_intake",
                "monthly_output",
                "monthly_output_auto",
                "monthly_output_manual",
                "avg_resolution_days",
                "median_resolution_days",
                "monthly_workers",
                "time_period",
                "process"
            )
        )
        dfs.append(df)
    batch_union = reduce(lambda a, b: a.unionByName(b), dfs)
    temp_view_name = f"batch_union_{i//batch_size}"
    batch_union.createOrReplaceTempView(temp_view_name)
    batch_union_tables.append(temp_view_name)

# Union all batch results
union_query = " UNION ALL ".join([f"SELECT * FROM {tbl}" for tbl in batch_union_tables])
df_long = spark.sql(union_query)

# COMMAND ----------

# DBTITLE 1,Final Transformations
window = Window.partitionBy(
    "process",
    F.year("calendar_day"),
    F.month("calendar_day")
).orderBy(
    "calendar_day"
).rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_long = (
    df_long
    .withColumn("month_to_date_intake", F.sum("intake").over(window))
    .withColumn("month_to_date_output", F.sum("output").over(window))
    .withColumn("monthly_throughput", F.col("monthly_intake") - F.col("monthly_output"))
    .withColumn(
        "monthly_work_rate",
        F.when(
            F.col("monthly_workers") != 0,
            F.round(F.col("monthly_output_manual") / F.col("monthly_workers"), 0)
        ).otherwise(None)
    )
    .withColumn(
        "target_pendency",
        F.when(F.substring(F.col("process"), 1, 3) == "int", F.lit(10))
         .when(F.substring(F.col("process"), 1, 3) == "itu", F.lit(15))
         .when(F.substring(F.col("process"), 1, 3) == "prg", F.lit(90))
         .otherwise(None)
    )
    .withColumn("grouping", F.substring(F.col("process"), 1, 7))
    .withColumn("phase", F.substring(F.col("process"), 1, 3))
    .withColumn(
        "fiscal_year",
        F.when(
            F.month("calendar_day") >= 10, F.year("calendar_day") + 1
        ).otherwise(F.year("calendar_day"))
    )
    .withColumn("create_ts", F.current_timestamp())
    .withColumn("create_user_id", F.lit("ETL"))
    .withColumn("update_ts", F.current_timestamp())
    .withColumn("update_user_id", F.lit("ETL"))
)
#display(df_long)

# COMMAND ----------

# DBTITLE 1,Set Column Order
desired_columns = [
    "calendar_day", "cases", "intake", "output", "output_auto", "output_manual",
    "monthly_intake", "monthly_output", "monthly_output_auto", "monthly_output_manual", "month_to_date_intake", "month_to_date_output", "monthly_throughput", "avg_resolution_days", "median_resolution_days", "monthly_workers", "monthly_work_rate", "target_pendency","time_period", "process", "grouping", "phase", "fiscal_year", "create_ts", "create_user_id", "update_ts", "update_user_id"
]

df_long = df_long.select(desired_columns)
#display(df_long)

# COMMAND ----------

# DBTITLE 1,Write to TMS Workflow Monitoring Table
#%skip
dashboard_table_name = f"{trgt_catalog}.gold.tms_workflow_monitoring_dashboard"
count_dashboard = df_long.count()
df_long.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(dashboard_table_name)

# COMMAND ----------

# DBTITLE 1,Write to TMS Workflow Detail Table
#%skip
detail_table_name = f"{trgt_catalog}.gold.tms_workflow_monitoring_detail"
count_detail = case_dictionary_exp_reordered_df.count()
case_dictionary_exp_reordered_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(detail_table_name)

# COMMAND ----------

# DBTITLE 1,Unpersist Cached Dataframe
case_dictionary_exp_df.unpersist()

# COMMAND ----------

# DBTITLE 1,End Job Control
#%skip
table_counts: list[int] = [
    count_dashboard,
    count_detail,
]
num_empty_tables: int = count_empty(table_counts)

if not num_empty_tables:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        count_dashboard + count_detail,
        "job completed successfully",
    )
    dbutils.notebook.exit(
        f"""
        Job completed with:
        - [{count_dashboard}] records for `{dashboard_table_name}`
        - [{count_detail}] records for `{detail_table_name}`
        """
    )
else:
    raise ValueError(
        f"{num_empty_tables} tables loaded 0 records. Tables must have at least 1 record to move on to next task."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Appendices
# MAGIC - This section contains the official queries that Trademark Services currently (6/4/2026) uses to calculate their inventories. 
# MAGIC - There are differences in approach between Trademark Services and this table. 
# MAGIC - These queries are included as transparency and reference if future work needs to be conducted to reconcile. 

# COMMAND ----------

# DBTITLE 1,DIV Unassigned
# MAGIC %skip
# MAGIC %sql
# MAGIC select
# MAGIC     t1_0.fk_trademark_gid,
# MAGIC     t2_0.legacy_status_cd,
# MAGIC     t1_0.latest_itu_filng_received_dt,
# MAGIC     t3_0.cfk_employee_no,
# MAGIC     t4_0.cfk_asgnd_exam_law_ofc_org_cd,
# MAGIC     t6_0.literal_element_tx,
# MAGIC     t5_0.first_action_publication_in
# MAGIC from
# MAGIC     trm_tmngpdb.bronze.tm_itu t1_0 
# MAGIC         join trm_tmngpdb.bronze.trademark t2_0 on t1_0.fk_trademark_gid = t2_0.trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_employee_assignment t3_0 on t1_0.fk_trademark_gid = t3_0.fk_trademark_gid
# MAGIC         and t3_0.fk_tm_employee_role_cd = 'EA'
# MAGIC         left join trm_tmngpdb.bronze.tm_locations t4_0 on t1_0.fk_trademark_gid = t4_0.fk_trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_office_actions t5_0 on t1_0.fk_trademark_gid = t5_0.fk_trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_literal t6_0 on t1_0.fk_trademark_gid = t6_0.fk_trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_filings t7_0 on t2_0.trademark_gid = t7_0.fk_trademark_gid
# MAGIC where
# MAGIC     t1_0.latest_itu_filng_received_dt is not null
# MAGIC   and not exists(
# MAGIC     select
# MAGIC         1
# MAGIC     from
# MAGIC         trm_tmngpdb.bronze.tm_employee_assignment t8_0
# MAGIC     where
# MAGIC         t8_0.fk_trademark_gid = t1_0.fk_trademark_gid
# MAGIC       and t8_0.fk_tm_employee_role_cd = 'ITU'
# MAGIC )
# MAGIC /*  and (
# MAGIC     ? is null
# MAGIC         or t1_0.fk_trademark_gid > ?
# MAGIC     )
# MAGIC */
# MAGIC   and (
# MAGIC     case when t7_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t7_0.cfk_last_incng_corr_event_cd end = 'DRRRI'
# MAGIC         or case when t7_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t7_0.cfk_last_incng_corr_event_cd end = 'ERTDI'
# MAGIC     )
# MAGIC   and (
# MAGIC     t2_0.legacy_status_cd = 616
# MAGIC         or t2_0.legacy_status_cd = 688
# MAGIC         or t2_0.legacy_status_cd between 717
# MAGIC         and 725
# MAGIC         or t2_0.legacy_status_cd between 730
# MAGIC         and 734
# MAGIC         or t2_0.legacy_status_cd between 744
# MAGIC         and 746
# MAGIC     )
# MAGIC order by
# MAGIC     t1_0.fk_trademark_gid
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,DIV Assigned
# MAGIC %skip
# MAGIC %sql
# MAGIC select 
# MAGIC   RIGHT(t1_0.fk_trademark_gid, 8) AS serial_number, 
# MAGIC   t2_0.legacy_status_cd, 
# MAGIC   t1_0.latest_itu_filng_received_dt, 
# MAGIC   t3_0.cfk_employee_no, 
# MAGIC   t3_0.effective_dt, 
# MAGIC   t4_0.cfk_employee_no, 
# MAGIC   t5_0.cfk_asgnd_exam_law_ofc_org_cd, 
# MAGIC   t7_0.literal_element_tx, 
# MAGIC   t6_0.first_action_publication_in 
# MAGIC from 
# MAGIC   trm_tmngpdb.bronze.tm_itu t1_0 
# MAGIC   join trm_tmngpdb.bronze.trademark t2_0 on t1_0.fk_trademark_gid = t2_0.trademark_gid 
# MAGIC   join trm_tmngpdb.bronze.tm_employee_assignment t3_0 on t1_0.fk_trademark_gid = t3_0.fk_trademark_gid 
# MAGIC   and t3_0.fk_tm_employee_role_cd = 'ITU' 
# MAGIC   left join trm_tmngpdb.bronze.tm_employee_assignment t4_0 on t1_0.fk_trademark_gid = t4_0.fk_trademark_gid 
# MAGIC   and t4_0.fk_tm_employee_role_cd = 'EA' 
# MAGIC   left join trm_tmngpdb.bronze.tm_locations t5_0 on t1_0.fk_trademark_gid = t5_0.fk_trademark_gid 
# MAGIC   left join trm_tmngpdb.bronze.tm_office_actions t6_0 on t1_0.fk_trademark_gid = t6_0.fk_trademark_gid 
# MAGIC   left join trm_tmngpdb.bronze.tm_literal t7_0 on t1_0.fk_trademark_gid = t7_0.fk_trademark_gid 
# MAGIC   left join trm_tmngpdb.bronze.tm_filings t8_0 on t2_0.trademark_gid = t8_0.fk_trademark_gid 
# MAGIC where 
# MAGIC   t1_0.latest_itu_filng_received_dt is not null 
# MAGIC   --and (
# MAGIC   --  ? is null 
# MAGIC   --  or t1_0.fk_trademark_gid > ?
# MAGIC   --) 
# MAGIC   --and t3_0.cfk_employee_no = ? 
# MAGIC   and (
# MAGIC     case when t8_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t8_0.cfk_last_incng_corr_event_cd end = 'DRRRI' 
# MAGIC     or case when t8_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t8_0.cfk_last_incng_corr_event_cd end = 'ERTDI'
# MAGIC   ) 
# MAGIC   and (
# MAGIC     t2_0.legacy_status_cd = 616 
# MAGIC     or t2_0.legacy_status_cd = 688 
# MAGIC     or t2_0.legacy_status_cd between 717 
# MAGIC     and 725 
# MAGIC     or t2_0.legacy_status_cd between 730 
# MAGIC     and 734 
# MAGIC     or t2_0.legacy_status_cd between 744 
# MAGIC     and 746
# MAGIC   ) 
# MAGIC order by 
# MAGIC   t1_0.fk_trademark_gid

# COMMAND ----------

# DBTITLE 1,EXT Unassigned
# MAGIC %skip
# MAGIC %sql
# MAGIC select
# MAGIC     t1_0.fk_trademark_gid,
# MAGIC     t2_0.legacy_status_cd,
# MAGIC     t1_0.latest_itu_filng_received_dt,
# MAGIC     t3_0.cfk_employee_no,
# MAGIC     t4_0.cfk_asgnd_exam_law_ofc_org_cd,
# MAGIC     t6_0.literal_element_tx,
# MAGIC     t5_0.first_action_publication_in
# MAGIC from
# MAGIC     trm_tmngpdb.bronze.tm_itu t1_0
# MAGIC         join trm_tmngpdb.bronze.trademark t2_0 on t1_0.fk_trademark_gid = t2_0.trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_employee_assignment t3_0 on t1_0.fk_trademark_gid = t3_0.fk_trademark_gid
# MAGIC         and t3_0.fk_tm_employee_role_cd = 'EA'
# MAGIC         left join trm_tmngpdb.bronze.tm_locations t4_0 on t1_0.fk_trademark_gid = t4_0.fk_trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_office_actions t5_0 on t1_0.fk_trademark_gid = t5_0.fk_trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_literal t6_0 on t1_0.fk_trademark_gid = t6_0.fk_trademark_gid
# MAGIC         left join trm_tmngpdb.bronze.tm_filings t7_0 on t2_0.trademark_gid = t7_0.fk_trademark_gid
# MAGIC where
# MAGIC     t1_0.latest_itu_filng_received_dt is not null
# MAGIC   and not exists(
# MAGIC     select
# MAGIC         1
# MAGIC     from
# MAGIC         trm_tmngpdb.bronze.tm_employee_assignment t8_0
# MAGIC     where
# MAGIC         t8_0.fk_trademark_gid = t1_0.fk_trademark_gid
# MAGIC       and t8_0.fk_tm_employee_role_cd = 'ITU'
# MAGIC )
# MAGIC /*  
# MAGIC     and (
# MAGIC     ? is null
# MAGIC         or t1_0.fk_trademark_gid > ?
# MAGIC     )
# MAGIC */
# MAGIC   and (
# MAGIC     (
# MAGIC         case when t7_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t7_0.cfk_last_incng_corr_event_cd end = 'EEXTI'
# MAGIC             and (
# MAGIC             t2_0.legacy_status_cd in (616, 688, 724, 725, 748)
# MAGIC                 or t2_0.legacy_status_cd between 730
# MAGIC                 and 734
# MAGIC                 or t2_0.legacy_status_cd between 744
# MAGIC                 and 746
# MAGIC             )
# MAGIC         )
# MAGIC         or (
# MAGIC         case when t7_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t7_0.cfk_last_incng_corr_event_cd end = 'INCEO'
# MAGIC             and (
# MAGIC             (
# MAGIC                 select
# MAGIC                     max(b1_0.order_no)
# MAGIC                 from
# MAGIC                     trm_tmngpdb.bronze.business_event b1_0
# MAGIC                         join trm_tmngpdb.bronze.stnd_business_event_reason b2_0 on b2_0.business_event_reason_id = b1_0.fk_business_event_reason_id
# MAGIC                 where
# MAGIC                     b1_0.cfk_object_gid = t1_0.fk_trademark_gid
# MAGIC                   and (
# MAGIC                           b2_0.legacy_cm_ent_cd || b2_0.legacy_cm_ent_type_cd
# MAGIC                           )= 'INCEO'
# MAGIC                   and (
# MAGIC                     (current_date - 999)> b1_0.effective_ts
# MAGIC                     )
# MAGIC             )>=(
# MAGIC                 select
# MAGIC                     max(b4_0.order_no)
# MAGIC                 from
# MAGIC                     trm_tmngpdb.bronze.business_event b4_0
# MAGIC                         join trm_tmngpdb.bronze.stnd_business_event_reason b5_0 on b5_0.business_event_reason_id = b4_0.fk_business_event_reason_id
# MAGIC                 where
# MAGIC                     b4_0.cfk_object_gid = t1_0.fk_trademark_gid
# MAGIC                   and (
# MAGIC                           b5_0.legacy_cm_ent_cd || b5_0.legacy_cm_ent_type_cd
# MAGIC                           ) not in (
# MAGIC                                     'ADCHM', 'ALIEA', 'APETA', 'ASGNI',
# MAGIC                                     'ASCKI', 'ASDFI', 'CHLDM', 'CFITO',
# MAGIC                                     'COARI', 'DOCKD', 'FINCP', 'FINOP',
# MAGIC                                     'FINPP', 'FINTP', 'FINVP', 'GBONP',
# MAGIC                                     'GPNXP', 'GP2NP', 'IRRXP', 'LNNXP',
# MAGIC                                     'OPNRP', 'OPNSP', 'OPNXP', 'PLGLA',
# MAGIC                                     'REAPI', 'RFNPP', 'RFNTP', 'RINXP',
# MAGIC                                     'RRXXP', 'RTNXP', 'TCCAI', 'RNWLP',
# MAGIC                                     'ZZAXZ', 'ZZBXZ', 'AITUA', 'ZZZXZ',
# MAGIC                                     'ZZZYZ', 'ZZZZZ', 'NWAPI', 'TUPSU',
# MAGIC                                     'AITUA', 'NREPP', 'ARAAI', 'CRCVM',
# MAGIC                                     'CORRI', 'EXT1S', 'EXT2S', 'EXT3S',
# MAGIC                                     'EXT4S', 'EXT55', 'MREIO'
# MAGIC                           )
# MAGIC             )
# MAGIC             )
# MAGIC         )
# MAGIC     )
# MAGIC order by
# MAGIC     t1_0.fk_trademark_gid
# MAGIC

# COMMAND ----------

# DBTITLE 1,EXT Assigned
# MAGIC %skip
# MAGIC %sql
# MAGIC select 
# MAGIC   t1_0.fk_trademark_gid, 
# MAGIC   t2_0.legacy_status_cd, 
# MAGIC   t1_0.latest_itu_filng_received_dt, 
# MAGIC   t3_0.cfk_employee_no, 
# MAGIC   t3_0.effective_dt, 
# MAGIC   t4_0.cfk_employee_no, 
# MAGIC   t5_0.cfk_asgnd_exam_law_ofc_org_cd, 
# MAGIC   t7_0.literal_element_tx, 
# MAGIC   t6_0.first_action_publication_in 
# MAGIC from 
# MAGIC   trm_tmngpdb.bronze.tm_itu t1_0 
# MAGIC   join trm_tmngpdb.bronze.trademark t2_0 on t1_0.fk_trademark_gid = t2_0.trademark_gid 
# MAGIC   join trm_tmngpdb.bronze.tm_employee_assignment t3_0 on t1_0.fk_trademark_gid = t3_0.fk_trademark_gid 
# MAGIC   and t3_0.fk_tm_employee_role_cd = 'ITU' 
# MAGIC   left join trm_tmngpdb.bronze.tm_employee_assignment t4_0 on t1_0.fk_trademark_gid = t4_0.fk_trademark_gid 
# MAGIC   and t4_0.fk_tm_employee_role_cd = 'EA' 
# MAGIC   left join trm_tmngpdb.bronze.tm_locations t5_0 on t1_0.fk_trademark_gid = t5_0.fk_trademark_gid 
# MAGIC   left join trm_tmngpdb.bronze.tm_office_actions t6_0 on t1_0.fk_trademark_gid = t6_0.fk_trademark_gid 
# MAGIC   left join trm_tmngpdb.bronze.tm_literal t7_0 on t1_0.fk_trademark_gid = t7_0.fk_trademark_gid 
# MAGIC   left join trm_tmngpdb.bronze.tm_filings t8_0 on t2_0.trademark_gid = t8_0.fk_trademark_gid 
# MAGIC where 
# MAGIC   t1_0.latest_itu_filng_received_dt is not null 
# MAGIC /*
# MAGIC   and (
# MAGIC     ? is null 
# MAGIC     or t1_0.fk_trademark_gid > ?
# MAGIC   ) 
# MAGIC   and t3_0.cfk_employee_no = ? 
# MAGIC */
# MAGIC   and (
# MAGIC     (
# MAGIC       case when t8_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t8_0.cfk_last_incng_corr_event_cd end = 'EEXTI' 
# MAGIC       and (
# MAGIC         t2_0.legacy_status_cd in (616, 688, 724, 725, 748) 
# MAGIC         or t2_0.legacy_status_cd between 730 
# MAGIC         and 734 
# MAGIC         or t2_0.legacy_status_cd between 744 
# MAGIC         and 746
# MAGIC       )
# MAGIC     ) 
# MAGIC     or (
# MAGIC       case when t8_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t8_0.cfk_last_incng_corr_event_cd end = 'INCEO' 
# MAGIC       and (
# MAGIC         (
# MAGIC           select 
# MAGIC             max(b1_0.order_no) 
# MAGIC           from 
# MAGIC             trm_tmngpdb.bronze.business_event b1_0 
# MAGIC             join trm_tmngpdb.bronze.stnd_business_event_reason b2_0 on b2_0.business_event_reason_id = b1_0.fk_business_event_reason_id 
# MAGIC           where 
# MAGIC             b1_0.cfk_object_gid = t1_0.fk_trademark_gid 
# MAGIC             and (
# MAGIC               b2_0.legacy_cm_ent_cd || b2_0.legacy_cm_ent_type_cd
# MAGIC             )= 'INCEO' 
# MAGIC             and (
# MAGIC               (current_date - 30)> b1_0.effective_ts
# MAGIC             )
# MAGIC         )>=(
# MAGIC           select 
# MAGIC             max(b4_0.order_no) 
# MAGIC           from 
# MAGIC             trm_tmngpdb.bronze.business_event b4_0 
# MAGIC             join trm_tmngpdb.bronze.stnd_business_event_reason b5_0 on b5_0.business_event_reason_id = b4_0.fk_business_event_reason_id 
# MAGIC           where 
# MAGIC             b4_0.cfk_object_gid = t1_0.fk_trademark_gid 
# MAGIC             and (
# MAGIC               b5_0.legacy_cm_ent_cd || b5_0.legacy_cm_ent_type_cd
# MAGIC             ) not in (
# MAGIC               'ADCHM', 'ALIEA', 'APETA', 'ASGNI', 
# MAGIC               'ASCKI', 'ASDFI', 'CHLDM', 'CFITO', 
# MAGIC               'COARI', 'DOCKD', 'FINCP', 'FINOP', 
# MAGIC               'FINPP', 'FINTP', 'FINVP', 'GBONP', 
# MAGIC               'GPNXP', 'GP2NP', 'IRRXP', 'LNNXP', 
# MAGIC               'OPNRP', 'OPNSP', 'OPNXP', 'PLGLA', 
# MAGIC               'REAPI', 'RFNPP', 'RFNTP', 'RINXP', 
# MAGIC               'RRXXP', 'RTNXP', 'TCCAI', 'RNWLP', 
# MAGIC               'ZZAXZ', 'ZZBXZ', 'AITUA', 'ZZZXZ', 
# MAGIC               'ZZZYZ', 'ZZZZZ', 'NWAPI', 'TUPSU', 
# MAGIC               'AITUA', 'NREPP', 'ARAAI', 'CRCVM', 
# MAGIC               'CORRI', 'EXT1S', 'EXT2S', 'EXT3S', 
# MAGIC               'EXT4S', 'EXT55', 'MREIO'
# MAGIC             )
# MAGIC         )
# MAGIC       )
# MAGIC     )
# MAGIC   ) 
# MAGIC -- order by 
# MAGIC --  t1_0.fk_trademark_gid offset ? rows fetch first ? rows only

# COMMAND ----------

# DBTITLE 1,SOU Unassigned
# MAGIC %skip
# MAGIC %sql
# MAGIC select
# MAGIC     t1_0.fk_trademark_gid,
# MAGIC     t2_0.legacy_status_cd,
# MAGIC     t1_0.latest_itu_filng_received_dt,
# MAGIC     t3_0.cfk_employee_no,
# MAGIC     t4_0.cfk_asgnd_exam_law_ofc_org_cd,
# MAGIC     t6_0.literal_element_tx,
# MAGIC     t5_0.first_action_publication_in
# MAGIC from
# MAGIC     tm_itu t1_0
# MAGIC         join trademark t2_0 on t1_0.fk_trademark_gid = t2_0.trademark_gid
# MAGIC         left join tm_employee_assignment t3_0 on t1_0.fk_trademark_gid = t3_0.fk_trademark_gid
# MAGIC         and t3_0.fk_tm_employee_role_cd = 'EA'
# MAGIC         left join tm_locations t4_0 on t1_0.fk_trademark_gid = t4_0.fk_trademark_gid
# MAGIC         left join tm_office_actions t5_0 on t1_0.fk_trademark_gid = t5_0.fk_trademark_gid
# MAGIC         left join tm_literal t6_0 on t1_0.fk_trademark_gid = t6_0.fk_trademark_gid
# MAGIC         left join tm_filings t7_0 on t2_0.trademark_gid = t7_0.fk_trademark_gid
# MAGIC where
# MAGIC     t1_0.latest_itu_filng_received_dt is not null
# MAGIC   and not exists(
# MAGIC     select
# MAGIC         1
# MAGIC     from
# MAGIC         tm_employee_assignment t8_0
# MAGIC     where
# MAGIC         t8_0.fk_trademark_gid = t1_0.fk_trademark_gid
# MAGIC       and t8_0.fk_tm_employee_role_cd = 'ITU'
# MAGIC )
# MAGIC   and (
# MAGIC     ? is null
# MAGIC         or t1_0.fk_trademark_gid > ?
# MAGIC     )
# MAGIC   and t1_0.latest_itu_filng_received_dt is not null
# MAGIC   and (
# MAGIC     (
# MAGIC         case when t7_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t7_0.cfk_last_incng_corr_event_cd end = 'EISUI'
# MAGIC             and (
# MAGIC             t2_0.legacy_status_cd in (616, 688)
# MAGIC                 or t2_0.legacy_status_cd between 717
# MAGIC                 and 725
# MAGIC                 or t2_0.legacy_status_cd between 730
# MAGIC                 and 734
# MAGIC                 or t2_0.legacy_status_cd between 744
# MAGIC                 and 746
# MAGIC             )
# MAGIC         )
# MAGIC         or (
# MAGIC         case when t7_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t7_0.cfk_last_incng_corr_event_cd end = 'INCSO'
# MAGIC             and (
# MAGIC             (
# MAGIC                 select
# MAGIC                     max(b1_0.order_no)
# MAGIC                 from
# MAGIC                     business_event b1_0
# MAGIC                         join stnd_business_event_reason b2_0 on b2_0.business_event_reason_id = b1_0.fk_business_event_reason_id
# MAGIC                 where
# MAGIC                     b1_0.cfk_object_gid = t1_0.fk_trademark_gid
# MAGIC                   and (
# MAGIC                           b2_0.legacy_cm_ent_cd || b2_0.legacy_cm_ent_type_cd
# MAGIC                           )= 'INCSO'
# MAGIC                   and (
# MAGIC                     (current_date - 888)> b1_0.effective_ts
# MAGIC                     )
# MAGIC             )>=(
# MAGIC                 select
# MAGIC                     max(b4_0.order_no)
# MAGIC                 from
# MAGIC                     business_event b4_0
# MAGIC                         join stnd_business_event_reason b5_0 on b5_0.business_event_reason_id = b4_0.fk_business_event_reason_id
# MAGIC                 where
# MAGIC                     b4_0.cfk_object_gid = t1_0.fk_trademark_gid
# MAGIC                   and (
# MAGIC                           b5_0.legacy_cm_ent_cd || b5_0.legacy_cm_ent_type_cd
# MAGIC                           ) not in (
# MAGIC                                     'ADCHM', 'ALIEA', 'APETA', 'ASGNI',
# MAGIC                                     'ASCKI', 'ASDFI', 'CHLDM', 'CFITO',
# MAGIC                                     'COARI', 'DOCKD', 'FINCP', 'FINOP',
# MAGIC                                     'FINPP', 'FINTP', 'FINVP', 'GBONP',
# MAGIC                                     'GPNXP', 'GP2NP', 'IRRXP', 'LNNXP',
# MAGIC                                     'OPNRP', 'OPNSP', 'OPNXP', 'PLGLA',
# MAGIC                                     'REAPI', 'RFNPP', 'RFNTP', 'RINXP',
# MAGIC                                     'RRXXP', 'RTNXP', 'TCCAI', 'RNWLP',
# MAGIC                                     'ZZAXZ', 'ZZBXZ', 'AITUA', 'ZZZXZ',
# MAGIC                                     'ZZZYZ', 'ZZZZZ', 'NWAPI', 'TUPSU',
# MAGIC                                     'AITUA', 'NREPP', 'ARAAI', 'CRCVM',
# MAGIC                                     'CORRI', 'EXT1S', 'EXT2S', 'EXT3S',
# MAGIC                                     'EXT4S', 'EXT55', 'MREIO'
# MAGIC                           )
# MAGIC             )
# MAGIC             )
# MAGIC         )
# MAGIC     )
# MAGIC order by
# MAGIC     t1_0.fk_trademark_gid
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,SOU Assigned
# MAGIC %skip
# MAGIC %sql
# MAGIC select 
# MAGIC   t1_0.fk_trademark_gid, 
# MAGIC   t2_0.legacy_status_cd, 
# MAGIC   t1_0.latest_itu_filng_received_dt, 
# MAGIC   t3_0.cfk_employee_no, 
# MAGIC   t3_0.effective_dt, 
# MAGIC   t4_0.cfk_employee_no, 
# MAGIC   t5_0.cfk_asgnd_exam_law_ofc_org_cd, 
# MAGIC   t7_0.literal_element_tx, 
# MAGIC   t6_0.first_action_publication_in 
# MAGIC from 
# MAGIC   tm_itu t1_0 
# MAGIC   join trademark t2_0 on t1_0.fk_trademark_gid = t2_0.trademark_gid 
# MAGIC   join tm_employee_assignment t3_0 on t1_0.fk_trademark_gid = t3_0.fk_trademark_gid 
# MAGIC   and t3_0.fk_tm_employee_role_cd = 'ITU' 
# MAGIC   left join tm_employee_assignment t4_0 on t1_0.fk_trademark_gid = t4_0.fk_trademark_gid 
# MAGIC   and t4_0.fk_tm_employee_role_cd = 'EA' 
# MAGIC   left join tm_locations t5_0 on t1_0.fk_trademark_gid = t5_0.fk_trademark_gid 
# MAGIC   left join tm_office_actions t6_0 on t1_0.fk_trademark_gid = t6_0.fk_trademark_gid 
# MAGIC   left join tm_literal t7_0 on t1_0.fk_trademark_gid = t7_0.fk_trademark_gid 
# MAGIC   left join tm_filings t8_0 on t2_0.trademark_gid = t8_0.fk_trademark_gid 
# MAGIC where 
# MAGIC   t1_0.latest_itu_filng_received_dt is not null 
# MAGIC   and (
# MAGIC     ? is null 
# MAGIC     or t1_0.fk_trademark_gid > ?
# MAGIC   ) 
# MAGIC   and t3_0.cfk_employee_no = ? 
# MAGIC   and t1_0.latest_itu_filng_received_dt is not null 
# MAGIC   and (
# MAGIC     (
# MAGIC       case when t8_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t8_0.cfk_last_incng_corr_event_cd end = 'EISUI' 
# MAGIC       and (
# MAGIC         t2_0.legacy_status_cd in (616, 688) 
# MAGIC         or t2_0.legacy_status_cd between 717 
# MAGIC         and 725 
# MAGIC         or t2_0.legacy_status_cd between 730 
# MAGIC         and 734 
# MAGIC         or t2_0.legacy_status_cd between 744 
# MAGIC         and 746
# MAGIC       )
# MAGIC     ) 
# MAGIC     or (
# MAGIC       case when t8_0.cfk_last_incng_corr_event_cd is null then t2_0.last_event_type_cd else t8_0.cfk_last_incng_corr_event_cd end = 'INCSO' 
# MAGIC       and (
# MAGIC         (
# MAGIC           select 
# MAGIC             max(b1_0.order_no) 
# MAGIC           from 
# MAGIC             business_event b1_0 
# MAGIC             join stnd_business_event_reason b2_0 on b2_0.business_event_reason_id = b1_0.fk_business_event_reason_id 
# MAGIC           where 
# MAGIC             b1_0.cfk_object_gid = t1_0.fk_trademark_gid 
# MAGIC             and (
# MAGIC               b2_0.legacy_cm_ent_cd || b2_0.legacy_cm_ent_type_cd
# MAGIC             )= 'INCSO' 
# MAGIC             and (
# MAGIC               (current_date - 888)> b1_0.effective_ts
# MAGIC             )
# MAGIC         )>=(
# MAGIC           select 
# MAGIC             max(b4_0.order_no) 
# MAGIC           from 
# MAGIC             business_event b4_0 
# MAGIC             join stnd_business_event_reason b5_0 on b5_0.business_event_reason_id = b4_0.fk_business_event_reason_id 
# MAGIC           where 
# MAGIC             b4_0.cfk_object_gid = t1_0.fk_trademark_gid 
# MAGIC             and (
# MAGIC               b5_0.legacy_cm_ent_cd || b5_0.legacy_cm_ent_type_cd
# MAGIC             ) not in (
# MAGIC               'ADCHM', 'ALIEA', 'APETA', 'ASGNI', 
# MAGIC               'ASCKI', 'ASDFI', 'CHLDM', 'CFITO', 
# MAGIC               'COARI', 'DOCKD', 'FINCP', 'FINOP', 
# MAGIC               'FINPP', 'FINTP', 'FINVP', 'GBONP', 
# MAGIC               'GPNXP', 'GP2NP', 'IRRXP', 'LNNXP', 
# MAGIC               'OPNRP', 'OPNSP', 'OPNXP', 'PLGLA', 
# MAGIC               'REAPI', 'RFNPP', 'RFNTP', 'RINXP', 
# MAGIC               'RRXXP', 'RTNXP', 'TCCAI', 'RNWLP', 
# MAGIC               'ZZAXZ', 'ZZBXZ', 'AITUA', 'ZZZXZ', 
# MAGIC               'ZZZYZ', 'ZZZZZ', 'NWAPI', 'TUPSU', 
# MAGIC               'AITUA', 'NREPP', 'ARAAI', 'CRCVM', 
# MAGIC               'CORRI', 'EXT1S', 'EXT2S', 'EXT3S', 
# MAGIC               'EXT4S', 'EXT55', 'MREIO'
# MAGIC             )
# MAGIC         )
# MAGIC       )
# MAGIC     )
# MAGIC   ) 
# MAGIC order by 
# MAGIC   case when t1_0.latest_itu_filng_received_dt > t3_0.effective_dt then t1_0.latest_itu_filng_received_dt else t3_0.effective_dt end desc, 
# MAGIC   t1_0.fk_trademark_gid fetch first ? rows only
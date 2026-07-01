# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Metadata
# MAGIC - **Created by:** Drew McPherson
# MAGIC - **Created on:** 2026-06-30
# MAGIC - **Last updated by:** Drew McPherson
# MAGIC - **Last updated on:** 2026-06-30
# MAGIC
# MAGIC ## Changelog
# MAGIC - **2026-06-30 (Drew McPherson):** Initial table creation

# COMMAND ----------

# DBTITLE 1,Import Libraries
import yaml
from datetime import date

# COMMAND ----------

# DBTITLE 1,Environmental Settings
today = date.today()
current_fy_start_year = today.year if today.month >= 10 else today.year - 1
filing_start_date = date(current_fy_start_year - 6, 10, 1).strftime('%Y-%m-%d')

dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

dbutils.widgets.text(
    "filing_start_date", "",
    label="Override filing start (YYYY-MM-DD, blank = auto 6-FY rolling)"
)

_override = dbutils.widgets.get("filing_start_date").strip()
if _override:
    filing_start_date = _override
else:
    current_fy_start_year = today.year if today.month >= 10 else today.year - 1
    filing_start_date = date(current_fy_start_year - 6, 10, 1).strftime('%Y-%m-%d')

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=},{filing_start_date=}")

# COMMAND ----------

# DBTITLE 1,Execute common function ntbk
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Read Config
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["reporting_catalog"]
trgt_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
target_table = f"{trgt_catalog}.gold.case_outcome_features"
print(reporting_catalog, trgt_catalog, tmngpdb_catalog, target_table)

# COMMAND ----------

# DBTITLE 1,Job Control
job_name = "ml_case_outcome_feature_load"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Build Feature Table
features_df = spark.sql(f"""
WITH 
-- ============================================================================
-- QUALIFIED SERIALS: materialise the 6-FY rolling window up front.
-- Every CTE that does not need full history semi-joins or sources from here,
-- so the date filter is pushed as early as possible in the plan.
-- ============================================================================
qualified_serials AS (
  SELECT ser_num, filing_dt, disposal_type, non_pro_se
  FROM `{reporting_catalog}`.`silver`.`milestone`
  WHERE filing_dt IS NOT NULL
    AND filing_dt >= '{filing_start_date}'
    AND ser_num IS NOT NULL
),

-- ============================================================================
-- ABANDONMENT TYPES: Detailed Abandonment Categories
-- ============================================================================
abandonment_types AS (
  -- Semi-join to qualified_serials limits prosecution_history to in-window records.
  -- WHERE tightened to only ABN0/ABN2 since those are the only codes used here.
  SELECT
    serial_number,
    1 AS abn_exa_flag
  FROM `{reporting_catalog}`.`silver`.`prosecution_history`
  WHERE ph_action_code IN ('ABN0', 'ABN2')
    AND serial_number IN (SELECT ser_num FROM qualified_serials)
  GROUP BY serial_number
),

-- ============================================================================
-- CLASS-BASED FEATURES
-- ============================================================================
class_base AS (
    -- Single filtered scan of silver.class; all class-based CTEs derive from here.
    SELECT ser_num, `class`, goods_and_services_desc
    FROM `{reporting_catalog}`.`silver`.`class`
    WHERE ser_num IS NOT NULL
      AND TRY_CAST(`class` AS INT) BETWEEN 1 AND 45
      AND ser_num IN (SELECT ser_num FROM qualified_serials)
),

class_features AS (
    -- Derives from class_base (one scan for all class features).
    -- Combines former class_agg counts with class indicators and combo string.
    SELECT 
        ser_num,
        COUNT(*)                                                                                         AS num_classes_at_application,
        SUM(LENGTH(goods_and_services_desc) - LENGTH(REPLACE(goods_and_services_desc, ';', '')))         AS gs_number,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(CAST(`class` AS STRING)))) AS class_combo_raw,
        MAX(CASE WHEN `class` = '001' THEN 1 ELSE 0 END) AS has_class_1,
        MAX(CASE WHEN `class` = '002' THEN 1 ELSE 0 END) AS has_class_2,
        MAX(CASE WHEN `class` = '003' THEN 1 ELSE 0 END) AS has_class_3,
        MAX(CASE WHEN `class` = '004' THEN 1 ELSE 0 END) AS has_class_4,
        MAX(CASE WHEN `class` = '005' THEN 1 ELSE 0 END) AS has_class_5,
        MAX(CASE WHEN `class` = '006' THEN 1 ELSE 0 END) AS has_class_6,
        MAX(CASE WHEN `class` = '007' THEN 1 ELSE 0 END) AS has_class_7,
        MAX(CASE WHEN `class` = '008' THEN 1 ELSE 0 END) AS has_class_8,
        MAX(CASE WHEN `class` = '009' THEN 1 ELSE 0 END) AS has_class_9,
        MAX(CASE WHEN `class` = '010' THEN 1 ELSE 0 END) AS has_class_10,
        MAX(CASE WHEN `class` = '011' THEN 1 ELSE 0 END) AS has_class_11,
        MAX(CASE WHEN `class` = '012' THEN 1 ELSE 0 END) AS has_class_12,
        MAX(CASE WHEN `class` = '013' THEN 1 ELSE 0 END) AS has_class_13,
        MAX(CASE WHEN `class` = '014' THEN 1 ELSE 0 END) AS has_class_14,
        MAX(CASE WHEN `class` = '015' THEN 1 ELSE 0 END) AS has_class_15,
        MAX(CASE WHEN `class` = '016' THEN 1 ELSE 0 END) AS has_class_16,
        MAX(CASE WHEN `class` = '017' THEN 1 ELSE 0 END) AS has_class_17,
        MAX(CASE WHEN `class` = '018' THEN 1 ELSE 0 END) AS has_class_18,
        MAX(CASE WHEN `class` = '019' THEN 1 ELSE 0 END) AS has_class_19,
        MAX(CASE WHEN `class` = '020' THEN 1 ELSE 0 END) AS has_class_20,
        MAX(CASE WHEN `class` = '021' THEN 1 ELSE 0 END) AS has_class_21,
        MAX(CASE WHEN `class` = '022' THEN 1 ELSE 0 END) AS has_class_22,
        MAX(CASE WHEN `class` = '023' THEN 1 ELSE 0 END) AS has_class_23,
        MAX(CASE WHEN `class` = '024' THEN 1 ELSE 0 END) AS has_class_24,
        MAX(CASE WHEN `class` = '025' THEN 1 ELSE 0 END) AS has_class_25,
        MAX(CASE WHEN `class` = '026' THEN 1 ELSE 0 END) AS has_class_26,
        MAX(CASE WHEN `class` = '027' THEN 1 ELSE 0 END) AS has_class_27,
        MAX(CASE WHEN `class` = '028' THEN 1 ELSE 0 END) AS has_class_28,
        MAX(CASE WHEN `class` = '029' THEN 1 ELSE 0 END) AS has_class_29,
        MAX(CASE WHEN `class` = '030' THEN 1 ELSE 0 END) AS has_class_30,
        MAX(CASE WHEN `class` = '031' THEN 1 ELSE 0 END) AS has_class_31,
        MAX(CASE WHEN `class` = '032' THEN 1 ELSE 0 END) AS has_class_32,
        MAX(CASE WHEN `class` = '033' THEN 1 ELSE 0 END) AS has_class_33,
        MAX(CASE WHEN `class` = '034' THEN 1 ELSE 0 END) AS has_class_34,
        MAX(CASE WHEN `class` = '035' THEN 1 ELSE 0 END) AS has_class_35,
        MAX(CASE WHEN `class` = '036' THEN 1 ELSE 0 END) AS has_class_36,
        MAX(CASE WHEN `class` = '037' THEN 1 ELSE 0 END) AS has_class_37,
        MAX(CASE WHEN `class` = '038' THEN 1 ELSE 0 END) AS has_class_38,
        MAX(CASE WHEN `class` = '039' THEN 1 ELSE 0 END) AS has_class_39,
        MAX(CASE WHEN `class` = '040' THEN 1 ELSE 0 END) AS has_class_40,
        MAX(CASE WHEN `class` = '041' THEN 1 ELSE 0 END) AS has_class_41,
        MAX(CASE WHEN `class` = '042' THEN 1 ELSE 0 END) AS has_class_42,
        MAX(CASE WHEN `class` = '043' THEN 1 ELSE 0 END) AS has_class_43,
        MAX(CASE WHEN `class` = '044' THEN 1 ELSE 0 END) AS has_class_44,
        MAX(CASE WHEN `class` = '045' THEN 1 ELSE 0 END) AS has_class_45
    FROM class_base
    GROUP BY ser_num
),

top_combos AS (
    -- Identifies the 250 most common class combinations to use as categories.
    -- All others will be labelled 'other' in class_combo below.
    SELECT class_combo_raw AS class_combo
    FROM class_features
    GROUP BY class_combo_raw
    ORDER BY COUNT(*) DESC, class_combo_raw
    LIMIT 250
),
class_combo AS (
    -- Assigns each application its combination string if it ranks in the top 250, else 'other'.
    SELECT
        cf.ser_num,
        CASE
            WHEN tc.class_combo IS NOT NULL THEN cf.class_combo_raw
            ELSE 'other'
        END AS class_combo
    FROM class_features cf
    LEFT JOIN top_combos tc ON cf.class_combo_raw = tc.class_combo
),

ranked_gs AS (
  SELECT 
    goods_and_services_desc, 
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS gs_rank
  FROM class_base
  GROUP BY goods_and_services_desc
),
gs_popularity AS (
  SELECT 
    goods_and_services_desc,
    CASE 
      WHEN gs_rank <= 100 THEN 'top_100'
      WHEN gs_rank <= 999 THEN 'mid_101_999'
      ELSE 'low_1000_plus'
    END AS popularity_category
  FROM ranked_gs
),
gs_categorized AS (
    SELECT *
    FROM (
      SELECT
        c.ser_num AS serial_number,
        gp.popularity_category
      FROM class_base c
      JOIN gs_popularity gp ON c.goods_and_services_desc = gp.goods_and_services_desc
    ) src
    PIVOT (
      COUNT(*) FOR popularity_category IN ('top_100', 'mid_101_999', 'low_1000_plus')
    )
),

goods_or_services AS (
    SELECT 
      ser_num,
      SUM(CASE WHEN goods_or_services = 'Services' THEN 1 ELSE 0 END) AS services,
      SUM(CASE WHEN goods_or_services = 'Goods' THEN 1 ELSE 0 END) AS goods
    FROM {reporting_catalog}.gold.goods_services_dashboard
    GROUP BY ser_num
),

-- ============================================================================
-- TEMPORAL FEATURES
-- ============================================================================
week_counts AS (
  -- Reuses qualified_serials instead of re-scanning milestone; date filter already applied.
  SELECT
    DATE_TRUNC('WEEK', filing_dt) AS filing_week,
    COUNT(*) AS filings_in_week
  FROM qualified_serials
  GROUP BY DATE_TRUNC('WEEK', filing_dt)
),
percentile_threshold AS (
  SELECT percentile_approx(filings_in_week, 0.9) AS threshold
  FROM week_counts
),
high_filing_weeks AS (
  SELECT filing_week
  FROM week_counts, percentile_threshold
  WHERE filings_in_week >= threshold
),
high_filing AS (
    -- Reuses qualified_serials to avoid a third scan of milestone.
    SELECT
      ser_num,
      MAX(CASE
        WHEN DATE_TRUNC('WEEK', filing_dt) IN (SELECT filing_week FROM high_filing_weeks)
        THEN 1
        ELSE 0
      END) AS is_high_filing_week
    FROM qualified_serials
    GROUP BY ser_num
),

-- ============================================================================
-- APPLICANT FEATURES
-- ============================================================================
owner_cte AS (
  -- Semi-join to qualified_serials prevents a full scan of the owner table.
  SELECT
    ser_num,
    MIN(ctry_nm) AS ctry_nm,
    MIN(state_cd) AS state_cd,
    MIN(entity_type) AS entity_type,
    MAX(CASE WHEN ctry_nm LIKE 'UNITED STATES OF AMERICA' THEN 1 ELSE 0 END) AS us_filing
  FROM {reporting_catalog}.silver.owner
  WHERE ser_num IN (SELECT ser_num FROM qualified_serials)
  GROUP BY ser_num
),

-- ============================================================================
-- MARK FEATURES
-- ============================================================================
pseudo_mark AS (
  SELECT 
    RIGHT(fk_trademark_gid, 8) AS ser_num,
    1 as pseudo
  FROM {tmngpdb_catalog}.bronze.tm_pseudo_mark
  WHERE RIGHT(fk_trademark_gid, 8) IN (SELECT ser_num FROM qualified_serials)
  GROUP BY RIGHT(fk_trademark_gid, 8)
),

design_element_count AS (
  SELECT 
    RIGHT(fk_trademark_gid, 8) AS ser_num, 
    COUNT(DISTINCT fk_design_search_group_cd) AS designs
  FROM {tmngpdb_catalog}.bronze.tm_design_element
  WHERE RIGHT(fk_trademark_gid, 8) IN (SELECT ser_num FROM qualified_serials)
  GROUP BY RIGHT(fk_trademark_gid, 8)
),

ds_disclaimers AS (
    SELECT 
        RIGHT(fk_trademark_gid, 8) AS ser_num,
        SUM(CASE WHEN fk_statement_type_cd = 'DS' THEN 1 ELSE 0 END) AS ds_count,
        SUM(CASE WHEN fk_statement_type_cd = 'CC' THEN 1 ELSE 0 END) AS cc_count
    FROM {tmngpdb_catalog}.bronze.tm_additional_statement 
    WHERE fk_statement_type_cd IN ('DS', 'CC')
    AND RIGHT(fk_trademark_gid, 8) IN (SELECT ser_num FROM qualified_serials)
    GROUP BY RIGHT(fk_trademark_gid, 8)
)

-- ============================================================================
-- FINAL JOIN
-- ============================================================================
SELECT
  -- Key
  b.SER_NUM AS serial_number,
  
  -- Target Variables
  COALESCE(m.disposal_type, 'NONE') AS disposal_type,
  CASE WHEN m.disposal_type = 'REGISTRATION' THEN 1 ELSE 0 END AS disposal_reg_flag,
  CASE WHEN m.disposal_type = 'ABANDONMENT' THEN 1 ELSE 0 END AS disposal_abn_flag,
  CASE WHEN m.disposal_type = 'NOA' THEN 1 ELSE 0 END AS disposal_noa_flag,
  CASE WHEN m.disposal_type IS NULL THEN 1 ELSE 0 END AS disposal_nul_flag,
  COALESCE(abt.abn_exa_flag, 0) AS abn_exa_flag,
  
  -- Filing and Basis
  b.FILING_BASIS_FIL AS filing_basis_fil,
  COALESCE(b.MARK_DWG_DESC, 'OTHER') AS mark_dwg_desc,
 
  -- Legal Representation
  COALESCE(m.non_pro_se, 'OTHER') AS legal_representation,
  
  -- Mark Description
  COALESCE(character_length(t.mark_description_tx), 0) AS mark_description_length,
  
  -- Temporal Features
  COALESCE(hf.is_high_filing_week, 0) AS is_high_filing_week,
  CAST(month(m.filing_dt) AS STRING) AS filing_month,
  dayname(m.filing_dt) AS filing_day,
  CAST(dayofmonth(m.filing_dt) AS STRING) AS filing_day_of_month,
  
  -- Applicant Features
  COALESCE(o.us_filing, 0) AS us_filing,
  COALESCE(o.ctry_nm, 'OTHER') AS country_of_origin,
  COALESCE(o.state_cd, 'OTHER') AS state_of_origin,
  CAST(COALESCE(o.entity_type, 0) AS STRING) AS entity_type,
  
  -- Mark Characteristics
  COALESCE(pm.pseudo, 0) AS pseudo,
  COALESCE(des.designs, 0) AS designs,
  COALESCE(dsd.ds_count, 0) AS disclaimer,
  COALESCE(dsd.cc_count, 0) AS color,

  -- Class Features
  COALESCE(cf.num_classes_at_application, 0) AS num_classes_at_application,
  COALESCE(cf.gs_number, 0) AS gs_number,
  COALESCE(gs.top_100, 0) AS gs_top_100,
  COALESCE(gs.mid_101_999, 0) AS gs_mid_101_999,
  COALESCE(gs.low_1000_plus, 0) AS gs_low_1000_plus,
  COALESCE(ROUND(gos.goods, 0), 0) AS goods,
  COALESCE(ROUND(gos.services, 0), 0) AS services,
  COALESCE(cc.class_combo, 'OTHER') AS class_combo,
  
  -- Individual Class Indicators
  COALESCE(cf.has_class_1, 0) AS has_class_1,
  COALESCE(cf.has_class_2, 0) AS has_class_2,
  COALESCE(cf.has_class_3, 0) AS has_class_3,
  COALESCE(cf.has_class_4, 0) AS has_class_4,
  COALESCE(cf.has_class_5, 0) AS has_class_5,
  COALESCE(cf.has_class_6, 0) AS has_class_6,
  COALESCE(cf.has_class_7, 0) AS has_class_7,
  COALESCE(cf.has_class_8, 0) AS has_class_8,
  COALESCE(cf.has_class_9, 0) AS has_class_9,
  COALESCE(cf.has_class_10, 0) AS has_class_10,
  COALESCE(cf.has_class_11, 0) AS has_class_11,
  COALESCE(cf.has_class_12, 0) AS has_class_12,
  COALESCE(cf.has_class_13, 0) AS has_class_13,
  COALESCE(cf.has_class_14, 0) AS has_class_14,
  COALESCE(cf.has_class_15, 0) AS has_class_15,
  COALESCE(cf.has_class_16, 0) AS has_class_16,
  COALESCE(cf.has_class_17, 0) AS has_class_17,
  COALESCE(cf.has_class_18, 0) AS has_class_18,
  COALESCE(cf.has_class_19, 0) AS has_class_19,
  COALESCE(cf.has_class_20, 0) AS has_class_20,
  COALESCE(cf.has_class_21, 0) AS has_class_21,
  COALESCE(cf.has_class_22, 0) AS has_class_22,
  COALESCE(cf.has_class_23, 0) AS has_class_23,
  COALESCE(cf.has_class_24, 0) AS has_class_24,
  COALESCE(cf.has_class_25, 0) AS has_class_25,
  COALESCE(cf.has_class_26, 0) AS has_class_26,
  COALESCE(cf.has_class_27, 0) AS has_class_27,
  COALESCE(cf.has_class_28, 0) AS has_class_28,
  COALESCE(cf.has_class_29, 0) AS has_class_29,
  COALESCE(cf.has_class_30, 0) AS has_class_30,
  COALESCE(cf.has_class_31, 0) AS has_class_31,
  COALESCE(cf.has_class_32, 0) AS has_class_32,
  COALESCE(cf.has_class_33, 0) AS has_class_33,
  COALESCE(cf.has_class_34, 0) AS has_class_34,
  COALESCE(cf.has_class_35, 0) AS has_class_35,
  COALESCE(cf.has_class_36, 0) AS has_class_36,
  COALESCE(cf.has_class_37, 0) AS has_class_37,
  COALESCE(cf.has_class_38, 0) AS has_class_38,
  COALESCE(cf.has_class_39, 0) AS has_class_39,
  COALESCE(cf.has_class_40, 0) AS has_class_40,
  COALESCE(cf.has_class_41, 0) AS has_class_41,
  COALESCE(cf.has_class_42, 0) AS has_class_42,
  COALESCE(cf.has_class_43, 0) AS has_class_43,
  COALESCE(cf.has_class_44, 0) AS has_class_44,
  COALESCE(cf.has_class_45, 0) AS has_class_45,

  -- Audit columns: track when each row was last refreshed and by which process.
  current_timestamp() AS create_ts,
  'ETL' AS create_user
  
-- qualified_serials is the scan anchor: only in-window applications flow through.
-- bibliography is joined in rather than driven from, avoiding a full biblio scan.
FROM qualified_serials m
  JOIN {reporting_catalog}.silver.bibliography b ON b.SER_NUM = m.ser_num
  LEFT JOIN abandonment_types abt ON m.ser_num = abt.serial_number
  LEFT JOIN gs_categorized gs ON m.ser_num = gs.serial_number
  LEFT JOIN {tmngpdb_catalog}.bronze.trademark t ON m.ser_num = t.serial_num_tx
  LEFT JOIN owner_cte o ON m.ser_num = o.ser_num
  LEFT JOIN high_filing hf ON m.ser_num = hf.ser_num
  LEFT JOIN class_combo cc ON m.ser_num = cc.ser_num
  LEFT JOIN class_features cf ON m.ser_num = cf.ser_num
  LEFT JOIN goods_or_services gos ON m.ser_num = gos.ser_num
  LEFT JOIN pseudo_mark pm ON m.ser_num = pm.ser_num
  LEFT JOIN design_element_count des ON m.ser_num = des.ser_num
  LEFT JOIN ds_disclaimers dsd ON m.ser_num = dsd.ser_num
WHERE b.SER_NUM IS NOT NULL
""")
#display(features_df)

# COMMAND ----------

# DBTITLE 1,Write Feature Table
# ============================================================================
# Write feature table
# First run: bootstraps with a full load since MERGE requires the target to exist.
# Subsequent runs: MERGE is incremental —
#   MATCHED     -> refreshes all columns; create_ts tracks last-updated timestamp.
#   NOT MATCHED -> inserts new applications not yet in the table.
# ============================================================================

features_df.createOrReplaceTempView("staging")

table_exists = spark.catalog.tableExists(target_table)

if not table_exists:
    print(f"Target table {target_table} does not exist — performing initial full load.")
    features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
else:
    print(f"Target table {target_table} exists — performing incremental MERGE.")
    spark.sql(f"""
        MERGE INTO {target_table} target
        USING staging source
        ON target.serial_number = source.serial_number
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

count_features = spark.table(target_table).count()
print(f"Feature table written: {target_table}")
print(f"Total rows: {count_features:,}")

# COMMAND ----------

# DBTITLE 1,End Job Control
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    count_features,
    "job completed successfully",
)
dbutils.notebook.exit(
    f"""
    Job completed with:
    - [{count_features}] records for `{target_table}`
    """
)

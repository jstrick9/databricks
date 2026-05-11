-- Databricks notebook source
-- Create the audit_quality schema in trm_domain_dev catalog
CREATE SCHEMA IF NOT EXISTS trm_domain_dev.audit_quality
COMMENT 'Centralized Data Quality audit tables — DQX error logs, metrics, and monitoring';

-- COMMAND ----------

-- DROP TABLE IF EXISTS trm_domain_dev.audit_quality.error_log;

-- COMMAND ----------

-- trm_domain_dev.audit_quality.error_log
-- Updated with AI Remediation & Intelligence columns for "Level 4" Self-Healing

CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.error_log (
    -- 1. RUN IDENTIFICATION
    error_log_id            STRING          NOT NULL    COMMENT 'Unique error log UUID per violation instance',
    run_id                  STRING          NOT NULL    COMMENT 'Unique DQX run identifier (UUID)',
    run_timestamp           TIMESTAMP       NOT NULL    COMMENT 'When the DQ run started',
    
    -- 2. SOURCE IDENTIFICATION
    catalog_name            STRING          NOT NULL    COMMENT 'Source logical catalog (e.g., trm_reporting)',
    schema_name             STRING          NOT NULL    COMMENT 'Source schema (e.g., silver)',
    table_name              STRING          NOT NULL    COMMENT 'Source table (e.g., bibliography)',
    
    -- 3. ERROR DETAILS
    check_name              STRING                      COMMENT 'Name of the DQX check that failed',
    check_function          STRING                      COMMENT 'Function name (e.g., is_not_null, regex_match)',
    column_name             STRING                      COMMENT 'Column that failed the check',
    error_message           STRING          NOT NULL    COMMENT 'Human-readable error description',
    failed_value            STRING                      COMMENT 'The actual value that failed (cast to string)',
    criticality             STRING          NOT NULL    COMMENT 'error or warning',
    
    -- 4. AI REMEDIATION & INTELLIGENCE (NEW)
    suggested_fix           STRING                      COMMENT 'AI-proposed corrected value',
    fix_confidence_score    DOUBLE                      COMMENT 'Confidence level of AI suggestion (0.0 to 1.0)',
    ai_explanation          STRING                      COMMENT 'AI reasoning for the proposed fix (for transparency)',
    ai_model_version        STRING                      COMMENT 'The specific model used (e.g., llama-3-1-70b)',
    remediation_status      STRING                      COMMENT 'PENDING, APPLIED, REJECTED, or IGNORED',
    remediation_timestamp   TIMESTAMP                   COMMENT 'When the fix was physically applied to source',
    remediation_user        STRING                      COMMENT 'Data Steward who approved the fix',
    
    -- 5. OUTPUT TRACKING
    quarantine_table        STRING                      COMMENT 'Full physical path to quarantine table',
    
    -- 6. AUDIT FIELDS
    created_at              TIMESTAMP       NOT NULL    COMMENT 'When this error was logged',
    created_by              STRING                      COMMENT 'User or service principal that ran DQ',

    -- 7. RESOLUTION TRACKING (Self-Healing)
    error_status            STRING          NOT NULL    COMMENT 'ACTIVE, RESOLVED, or REACTIVATED',
    resolution_reason       STRING                      COMMENT 'DATA_FIXED (Auto), RULE_RELAXED, or MANUAL_FIX',
    resolved_at             TIMESTAMP                   COMMENT 'When this error was resolved',
    resolved_run_id         STRING                      COMMENT 'The run identifier that confirmed the fix',

    -- 8. HASHES & SCD2 METADATA
    _natural_key_hash       STRING          NOT NULL    COMMENT 'Deterministic hash of the business key',
    _record_data_hash       STRING          NOT NULL    COMMENT 'Hash of row content to detect data drift',
    _created_date           DATE                        COMMENT 'SCD2 created date',
    _created_timestamp      TIMESTAMP                   COMMENT 'SCD2 created timestamp',
    _updated_timestamp      TIMESTAMP                   COMMENT 'SCD2 updated timestamp',
    _is_record_active       BOOLEAN                     COMMENT 'SCD2 active flag'
)
USING DELTA
PARTITIONED BY (catalog_name, schema_name)
COMMENT 'Enterprise DQ Error Log with AI-powered remediation and self-healing tracking'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.columnMapping.mode' = 'name'
);

-- COMMAND ----------

-- Central registry of all onboarded tables and their DQ metadata
CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.dq_table_registry (
    registry_id             STRING      NOT NULL,
    catalog_name            STRING      NOT NULL,
    schema_name             STRING      NOT NULL,
    table_name              STRING      NOT NULL,
    
    -- Onboarding metadata
    onboarded_date          DATE,
    onboarded_by            STRING,
    table_description       STRING,
    data_owner              STRING,
    data_steward            STRING,
    
    -- Configuration references
    checks_yaml_path        STRING,
    hash_config_yaml_path   STRING,
    canonical_script_path   STRING,
    
    -- Runtime stats (updated after each run)
    last_run_id             STRING,
    last_run_timestamp      TIMESTAMP,
    last_run_status         STRING,      -- PASS / QUARANTINED
    last_total_rows         BIGINT,
    last_clean_rows         BIGINT,
    last_quarantined_rows   BIGINT,
    last_run_duration_sec   INT,
    
    -- Health score (computed)
    health_score_pct        DOUBLE,       -- (clean / total) * 100
    consecutive_failures    INT,
    
    -- SCD2 metadata
    _created_date           DATE,
    _created_timestamp      TIMESTAMP,
    _updated_timestamp      TIMESTAMP,
    _is_record_active       BOOLEAN
);


-- COMMAND ----------

-- Aggregated run history for trending dashboards
CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.dq_run_history (
    run_id                  STRING       NOT NULL,
    run_timestamp           TIMESTAMP    NOT NULL,
    catalog_name            STRING       NOT NULL,
    schema_name             STRING       NOT NULL,
    table_name              STRING       NOT NULL,
    dbx_env                 STRING,
    load_method             STRING,
    status                  STRING,
    total_rows              BIGINT,
    clean_rows              BIGINT,
    quarantined_rows        BIGINT,
    error_count             BIGINT,
    warning_count           BIGINT,
    resolved_count          BIGINT,
    run_duration_seconds    INT,
    _created_timestamp      TIMESTAMP
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.dq_root_cause_analysis (
    rca_id                  STRING      NOT NULL,
    run_id                  STRING      NOT NULL,
    catalog_name            STRING      NOT NULL,
    schema_name             STRING      NOT NULL,
    table_name              STRING      NOT NULL,
    analysis_timestamp      STRING,
    spikes_detected         STRING,     -- JSON array
    new_error_types         STRING,     -- JSON array
    resolved_errors         STRING,     -- JSON array
    trending_errors         STRING,     -- JSON array
    ai_summary              STRING,
    recommended_actions     STRING,     -- JSON array
    _created_timestamp      TIMESTAMP   
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);



-- COMMAND ----------

-- Dashboard view: Latest RCA per table
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_latest_rca AS
SELECT
    catalog_name,
    schema_name,
    table_name,
    analysis_timestamp,
    ai_summary,
    recommended_actions,
    spikes_detected,
    new_error_types,
    _created_timestamp
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY catalog_name, schema_name, table_name
               ORDER BY _created_timestamp DESC
           ) AS rn
    FROM trm_domain_dev.audit_quality.dq_root_cause_analysis
)
WHERE rn = 1;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.dq_config_sync_log (
  sync_id                 STRING,
  synced_at_utc           TIMESTAMP,
  synced_by               STRING,

  source_path             STRING,
  target_path             STRING,
  config_kind             STRING,   -- checks | hash_configs

  status                  STRING,   -- COPIED | SKIP_UNCHANGED | SKIP_INVALID_YAML | ERROR_*

  source_sha256           STRING,
  target_sha256_before    STRING,
  target_sha256_after     STRING,

  bytes_written           BIGINT,
  archived_old_version    BOOLEAN,
  archive_path            STRING,

  notes                   STRING
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.dq_config_archive_purge_log (
  purge_run_id        STRING,
  event_id            STRING,

  purged_at_utc        TIMESTAMP,
  purged_by            STRING,

  archive_volume_root  STRING,
  keep_days            INT,
  cutoff_utc           TIMESTAMP,
  dry_run              BOOLEAN,

  folder_path          STRING,   -- NULL for summary row
  folder_timestamp     STRING,   -- parsed from folder name if present
  action               STRING,   -- SUMMARY | KEEP | WOULD_DELETE | DELETED | SKIP | ERROR
  reason               STRING,   -- optional

  scanned              INT,      -- populated only on SUMMARY row
  eligible             INT,      -- populated only on SUMMARY row
  deleted              INT,      -- populated only on SUMMARY row
  skipped              INT,      -- populated only on SUMMARY row
  errors               INT       -- populated only on SUMMARY row
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.dq_system_validation_log (
  validation_run_id     STRING,
  status                STRING,
  started_at_utc        STRING,
  finished_at_utc       STRING,
  checks_passed         INT,
  checks_failed         INT,
  details_json          STRING,
  _created_timestamp    TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS trm_domain_dev.audit_quality.dq_release_gate_log (
  gate_run_id           STRING,
  status                STRING,
  started_at_utc        STRING,
  finished_at_utc       STRING,
  passed                INT,
  failed                INT,
  details_json          STRING,
  _created_timestamp    TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ================================================================
-- ENTERPRISE DATA QUALITY DASHBOARD VIEWS
-- Run once per environment to create the analytics layer
-- ================================================================

-- View 1: Overall Data Health by Table (Current State)
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_dq_health_scorecard AS
SELECT
    r.catalog_name,
    r.schema_name,
    r.table_name,
    r.last_run_timestamp,
    r.last_run_status,
    r.last_total_rows,
    r.last_clean_rows,
    r.last_quarantined_rows,
    r.health_score_pct,
    r.consecutive_failures,
    r.data_owner,
    r.data_steward,
    CASE
        WHEN r.health_score_pct >= 99  THEN 'EXCELLENT'
        WHEN r.health_score_pct >= 95  THEN 'GOOD'
        WHEN r.health_score_pct >= 90  THEN 'FAIR'
        WHEN r.health_score_pct >= 80  THEN 'POOR'
        ELSE 'CRITICAL'
    END AS health_grade,
    CASE
        WHEN r.consecutive_failures >= 3 THEN 'DEGRADING'
        WHEN r.consecutive_failures = 0  THEN 'STABLE'
        ELSE 'AT_RISK'
    END AS trend_status,
    rca.ai_summary AS latest_rca_summary
FROM trm_domain_dev.audit_quality.dq_table_registry r
LEFT JOIN trm_domain_dev.audit_quality.v_latest_rca rca
    ON r.catalog_name = rca.catalog_name
   AND r.schema_name  = rca.schema_name
   AND r.table_name   = rca.table_name
WHERE r._is_record_active = true;


-- View 2: Active Errors and Warnings (For Data Stewards)
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_active_violations AS
SELECT
    e.catalog_name,
    e.schema_name,
    e.table_name,
    e.check_name,
    e.check_function,
    e.column_name,
    e.error_message,
    e.criticality,
    e.error_status,
    e.run_id,
    e.run_timestamp,
    e.created_at,
    DATEDIFF(current_date(), e.created_at) AS days_open,
    e._natural_key_hash,
    e._record_data_hash
FROM trm_domain_dev.audit_quality.error_log e
WHERE e.error_status = 'ACTIVE'
  AND e._is_record_active = true
ORDER BY e.criticality ASC, e.created_at DESC;


-- View 3: Daily Failure Trend (For Power BI / Tableau)
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_daily_failure_trend AS
SELECT
    DATE(run_timestamp)        AS run_date,
    catalog_name,
    schema_name,
    table_name,
    COUNT(DISTINCT run_id)     AS total_runs,
    SUM(total_rows)            AS total_rows_processed,
    SUM(clean_rows)            AS total_clean_rows,
    SUM(quarantined_rows)      AS total_quarantined_rows,
    AVG(
        CASE WHEN total_rows > 0
             THEN (clean_rows / total_rows) * 100
             ELSE 100
        END
    )                          AS avg_health_score_pct,
    SUM(error_count)           AS total_errors,
    SUM(warning_count)         AS total_warnings,
    SUM(resolved_count)        AS total_resolved
FROM trm_domain_dev.audit_quality.dq_run_history
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 2, 3, 4;


-- View 4: Top Failing Checks Enterprise-Wide (Last 30 days)
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_top_failing_checks AS
SELECT
    catalog_name,
    schema_name,
    table_name,
    check_name,
    check_function,
    column_name,
    criticality,
    COUNT(*)                        AS total_failures,
    COUNT(DISTINCT run_id)          AS runs_with_failure,
    COUNT(DISTINCT _natural_key_hash) AS unique_records_failed,
    MIN(run_timestamp)              AS first_seen,
    MAX(run_timestamp)              AS last_seen
FROM trm_domain_dev.audit_quality.error_log
WHERE error_status = 'ACTIVE'
  AND run_timestamp >= DATEADD(day, -30, current_timestamp())
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY total_failures DESC
LIMIT 50;


-- View 5: Self-Healing Effectiveness (Records auto-resolved)
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_self_healing_summary AS
SELECT
    DATE(resolved_at)               AS resolution_date,
    catalog_name,
    schema_name,
    table_name,
    resolution_reason,
    COUNT(*)                        AS records_resolved,
    AVG(
        DATEDIFF(resolved_at, created_at)
    )                               AS avg_days_to_resolution
FROM trm_domain_dev.audit_quality.error_log
WHERE error_status = 'RESOLVED'
  AND resolved_at IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC;


-- View 6: PII Column Inventory (Governance View)
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_pii_inventory AS
SELECT
    t.table_catalog,
    t.table_schema,
    t.table_name,
    c.column_name,
    c.data_type,
    ct.tag_value                    AS pii_tag,
    t.last_altered                 AS tagged_date
FROM system.information_schema.tables t
JOIN system.information_schema.columns c
    ON t.table_catalog = c.table_catalog
   AND t.table_schema  = c.table_schema
   AND t.table_name    = c.table_name
JOIN system.information_schema.column_tags ct
    ON c.table_catalog = ct.catalog_name
   AND c.table_schema  = ct.schema_name
   AND c.table_name    = ct.table_name
   AND c.column_name   = ct.column_name
WHERE ct.tag_name IN ('pii', 'sensitive', 'personal_data')
ORDER BY 1, 2, 3, 4;

-- COMMAND ----------

CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_error_summary AS
SELECT
    DATE(run_timestamp)              AS run_date,
    catalog_name,
    schema_name,
    table_name,
    criticality,
    check_name,
    check_function,
    column_name,
    COUNT(*)                         AS error_count,
    MAX(run_timestamp)               AS last_occurrence
FROM trm_domain_dev.audit_quality.error_log
GROUP BY
    DATE(run_timestamp),
    catalog_name,
    schema_name,
    table_name,
    criticality,
    check_name,
    check_function,
    column_name
ORDER BY
    run_date DESC,
    error_count DESC;

-- COMMAND ----------

-- Track pass rates over time — used for executive dashboards
CREATE OR REPLACE VIEW trm_domain_dev.audit_quality.v_sla_compliance AS
SELECT
    DATE(run_timestamp) AS run_date,
    catalog_name,
    schema_name,
    table_name,
    run_id,
    COUNT(CASE WHEN criticality = 'error' THEN 1 END) AS error_count,
    COUNT(CASE WHEN criticality = 'warning' THEN 1 END) AS warning_count,
    CASE 
        WHEN COUNT(CASE WHEN criticality = 'error' THEN 1 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS sla_status
FROM trm_domain_dev.audit_quality.error_log
GROUP BY 1, 2, 3, 4, 5
ORDER BY run_date DESC;

-- COMMAND ----------


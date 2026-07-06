-- Databricks notebook source
-- DBTITLE 1,Ingest incoming pqt data using Autoloader
CREATE
OR REFRESH STREAMING LIVE TABLE PRODUCTION_TRANSACTION_RAW AS
SELECT
  op,
  production_credit_tran_id,
  cfk_object_gid,
  cfk_object_type_cd,
  fk_generating_prodvty_actn_id,
  fk_corrected_prodvty_actn_id,
  unit_count_no,
  transaction_effective_dt,
  dn_worker_no,
  dn_worker_tm_organization_cd,
  dn_worker_role_cd,
  cfk_worker_gid,
  cfk_worker_tm_organization_gid,
  cfk_worker_user_role_id,
  dn_contributor_worker_no,
  dn_contributor_worker_role_cd,
  dn_contributor_tm_org_cd,
  cfk_contributor_worker_gid,
  cfk_contributor_user_role_id,
  cfk_contributor_tm_org_gid,
  cfk_bcr_pay_period_range_name,
  dn_action_no,
  priority_in,
  transaction_ct,
  work_unit_cd,
  lock_control_no,
  create_ts,
  create_user_id,
  last_mod_ts,
  last_mod_user_id,
  subsequent_action_in,
  delete_in,
  to_timestamp(sourceRecordTime) as source_record_time,
  current_timestamp() as raw_create_ts
FROM
  STREAM read_files(
    's3://${cdc_bucket}/eds/DMS/TMPRODVTY/PRODUCTION_TRANSACTION/',
    format => 'parquet'
  )

-- COMMAND ----------

CREATE
OR REFRESH STREAMING LIVE TABLE PRODUCTIVITY_ACTION_RAW AS
SELECT
  op,
  sourceRecordTime,
  productivity_action_id,
  productivity_action_cd,
  sub_action_cd,
  title_tx,
  lock_control_no,
  create_ts,
  create_user_id,
  last_mod_ts,
  last_mod_user_id,
  to_timestamp(sourceRecordTime) as source_record_time,
  current_timestamp() as raw_create_ts
FROM
  STREAM read_files(
    's3://${cdc_bucket}/eds/DMS/TMPRODVTY/PRODUCTIVITY_ACTION/',
    format => 'parquet'
  )

-- COMMAND ----------

CREATE
OR REFRESH STREAMING LIVE TABLE WORKER_TIME_ENTRY_RAW AS
SELECT
  op ,
  worker_time_entry_id,
  cfk_pp_range_nm,
  entry_date,
  cfk_worker_gid,
  cfk_user_role_id,
  cfk_tm_organization_gid,
  regular_hours_qt,
  overtime_hours_qt,
  fk_task_cd,
  lock_control_no,
  create_ts,
  create_user_id,
  last_mod_ts,
  last_mod_user_id,
  to_timestamp(sourceRecordTime) as source_record_time,
  current_timestamp() as raw_create_ts
FROM
  STREAM read_files(
    's3://${cdc_bucket}/eds/DMS/TMPRODVTY/WORKER_TIME_ENTRY/',
    format => 'parquet'
  )

-- COMMAND ----------


CREATE
OR REFRESH STREAMING LIVE TABLE PRODUCTION_TRANSACTION_ERRLOG_RAW AS
SELECT
  op,
  `ORA_ERR_NUMBER$` as ORA_ERR_NUMBER,
  `ORA_ERR_MESG$` as ORA_ERR_MESG,
  `ORA_ERR_OPTYP$` as ORA_ERR_OPTYP,
  `ORA_ERR_TAG$` as ORA_ERR_TAG,
  PRODUCTION_CREDIT_TRAN_ID,
  CFK_OBJECT_GID,
  CFK_OBJECT_TYPE_CD,
  FK_GENERATING_PRODVTY_ACTN_ID,
  FK_CORRECTED_PRODVTY_ACTN_ID,
  UNIT_COUNT_NO,
  TRANSACTION_EFFECTIVE_DT,
  DN_WORKER_NO,
  DN_WORKER_TM_ORGANIZATION_CD,
  DN_WORKER_ROLE_CD,
  CFK_WORKER_GID,
  CFK_WORKER_TM_ORGANIZATION_GID,
  CFK_WORKER_USER_ROLE_ID,
  DN_CONTRIBUTOR_WORKER_NO,
  DN_CONTRIBUTOR_WORKER_ROLE_CD,
  DN_CONTRIBUTOR_TM_ORG_CD,
  CFK_CONTRIBUTOR_WORKER_GID,
  CFK_CONTRIBUTOR_USER_ROLE_ID,
  CFK_CONTRIBUTOR_TM_ORG_GID,
  CFK_BCR_PAY_PERIOD_RANGE_NAME,
  DN_ACTION_NO,
  PRIORITY_IN,
  TRANSACTION_CT,
  WORK_UNIT_CD,
  LOCK_CONTROL_NO,
  CREATE_TS,
  CREATE_USER_ID,
  LAST_MOD_TS,
  LAST_MOD_USER_ID,
  SUBSEQUENT_ACTION_IN,
  DELETE_IN,
  to_timestamp(sourceRecordTime) as source_record_time,
  current_timestamp() as raw_create_ts
FROM
  STREAM read_files(
    's3://${cdc_bucket}/eds/DMS/TMPRODVTY/PRODUCTION_TRANSACTION_ERRLOG/',
    format => 'parquet'
  )

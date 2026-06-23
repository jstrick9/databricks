-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Check Data quality rules expectations

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE PRODUCTION_TRANSACTION_CLEAN(
  CONSTRAINT valid_id        EXPECT (PRODUCTION_CREDIT_TRAN_ID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (op IN ('I', 'U', 'D'))                 ON VIOLATION DROP ROW,
  CONSTRAINT valid_sequence  EXPECT (source_record_time IS NOT NULL)         ON VIOLATION DROP ROW  
)
COMMENT "Cleansed CDC data with valid ID, operation type, and sequence timestamp enforced"
TBLPROPERTIES ("quality" = "silver", "layer" = "clean") 
AS SELECT
  *,
  current_timestamp() as etl_updt_ts
FROM STREAM(LIVE.PRODUCTION_TRANSACTION_RAW);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE PRODUCTIVITY_ACTION_CLEAN(
  CONSTRAINT valid_id        EXPECT (PRODUCTIVITY_ACTION_ID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (op IN ('I', 'U', 'D'))              ON VIOLATION DROP ROW,
  CONSTRAINT valid_sequence  EXPECT (source_record_time IS NOT NULL)      ON VIOLATION DROP ROW
)
COMMENT "Cleansed CDC data with valid ID, operation type, and sequence timestamp enforced"
TBLPROPERTIES ("quality" = "silver", "layer" = "clean")
AS SELECT
  *,
  current_timestamp() as etl_updt_ts
FROM STREAM(LIVE.PRODUCTIVITY_ACTION_RAW);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE WORKER_TIME_ENTRY_CLEAN(
  CONSTRAINT valid_id        EXPECT (WORKER_TIME_ENTRY_ID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (op IN ('I', 'U', 'D'))            ON VIOLATION DROP ROW,
  CONSTRAINT valid_sequence  EXPECT (source_record_time IS NOT NULL)    ON VIOLATION DROP ROW 
)
COMMENT "Cleansed CDC data with valid ID, operation type, and sequence timestamp enforced"
TBLPROPERTIES ("quality" = "silver", "layer" = "clean")
AS SELECT
  *,
  current_timestamp() as etl_updt_ts
FROM STREAM(LIVE.WORKER_TIME_ENTRY_RAW);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE PRODUCTION_TRANSACTION_ERRLOG_CLEAN(
  CONSTRAINT valid_operation EXPECT (op IN ('I', 'U', 'D')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sequence  EXPECT (source_record_time IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleansed CDC error log data with operation type and sequence timestamp enforced"
TBLPROPERTIES ("quality" = "silver", "layer" = "clean")
AS SELECT
  *,
  current_timestamp() as etl_updt_ts
FROM STREAM(LIVE.PRODUCTION_TRANSACTION_ERRLOG_RAW);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Load data in streaming live bronze table

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE PRODUCTION_TRANSACTION_LIVE
TBLPROPERTIES ("quality" = "gold", "layer" = "live"); 

APPLY CHANGES INTO LIVE.PRODUCTION_TRANSACTION_LIVE
FROM STREAM(LIVE.PRODUCTION_TRANSACTION_CLEAN)
  KEYS (PRODUCTION_CREDIT_TRAN_ID)
  APPLY AS DELETE WHEN op = "D"
  SEQUENCE BY source_record_time
  COLUMNS * EXCEPT (op);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE PRODUCTIVITY_ACTION_LIVE
TBLPROPERTIES ("quality" = "gold", "layer" = "live");

APPLY CHANGES INTO LIVE.PRODUCTIVITY_ACTION_LIVE
FROM STREAM(LIVE.PRODUCTIVITY_ACTION_CLEAN)
  KEYS (PRODUCTIVITY_ACTION_ID)
  APPLY AS DELETE WHEN op = "D"
  SEQUENCE BY source_record_time
  COLUMNS * EXCEPT (op);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE WORKER_TIME_ENTRY_LIVE
TBLPROPERTIES ("quality" = "gold", "layer" = "live");

APPLY CHANGES INTO LIVE.WORKER_TIME_ENTRY_LIVE
FROM STREAM(LIVE.WORKER_TIME_ENTRY_CLEAN)
  KEYS (WORKER_TIME_ENTRY_ID)
  APPLY AS DELETE WHEN op = "D"
  SEQUENCE BY source_record_time
  COLUMNS * EXCEPT (op);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE PRODUCTION_TRANSACTION_ERRLOG_LIVE
TBLPROPERTIES ("quality" = "gold", "layer" = "live");

APPLY CHANGES INTO LIVE.PRODUCTION_TRANSACTION_ERRLOG_LIVE
FROM STREAM(LIVE.PRODUCTION_TRANSACTION_ERRLOG_CLEAN)
  KEYS (ora_err_tag, PRODUCTION_CREDIT_TRAN_ID, create_ts, last_mod_ts)
  APPLY AS DELETE WHEN op = "D"
  SEQUENCE BY source_record_time
  COLUMNS * EXCEPT (op);

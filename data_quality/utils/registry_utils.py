"""
Updates the central DQ registry and run history after every pipeline execution.
Called at the end of process_table_dq().
"""
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from datetime import datetime, timezone
import uuid

spark = SparkSession.builder.getOrCreate()

def update_registry_and_history(
    run_result: dict,
    catalog: str,
    schema: str,
    table_name: str,
    dbx_env: str,
    run_start_time: datetime,
    domain_catalog: str
):
    run_end_time = datetime.now(timezone.utc)
    duration_sec = int((run_end_time - run_start_time).total_seconds())
    
    total = run_result.get("total", 0)
    clean = run_result.get("valid", 0)
    quarantined = run_result.get("quarantined", 0)
    health_pct = round((clean / total) * 100, 2) if total > 0 else 0.0
    
    registry_table = f"{domain_catalog}.audit_quality.dq_table_registry"
    history_table = f"{domain_catalog}.audit_quality.dq_run_history"
    
    # === UPDATE REGISTRY (upsert) ===
    spark.sql(f"""
        MERGE INTO {registry_table} tgt
        USING (SELECT 
            '{catalog}' as catalog_name,
            '{schema}' as schema_name,
            '{table_name}' as table_name
        ) src
        ON tgt.catalog_name = src.catalog_name
           AND tgt.schema_name = src.schema_name
           AND tgt.table_name = src.table_name
           AND tgt._is_record_active = true
        WHEN MATCHED THEN UPDATE SET
            tgt.last_run_id = '{run_result["run_id"]}',
            tgt.last_run_timestamp = current_timestamp(),
            tgt.last_run_status = '{run_result["status"]}',
            tgt.last_total_rows = {total},
            tgt.last_clean_rows = {clean},
            tgt.last_quarantined_rows = {quarantined},
            tgt.last_run_duration_sec = {duration_sec},
            tgt.health_score_pct = {health_pct},
            tgt.consecutive_failures = CASE 
                WHEN '{run_result["status"]}' = 'QUARANTINED' THEN tgt.consecutive_failures + 1
                ELSE 0 END,
            tgt._updated_timestamp = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            registry_id, catalog_name, schema_name, table_name,
            onboarded_date, last_run_id, last_run_timestamp, last_run_status,
            last_total_rows, last_clean_rows, last_quarantined_rows,
            last_run_duration_sec, health_score_pct, consecutive_failures,
            _created_date, _created_timestamp, _updated_timestamp, _is_record_active
        ) VALUES (
            uuid(), '{catalog}', '{schema}', '{table_name}',
            current_date(), '{run_result["run_id"]}', current_timestamp(), '{run_result["status"]}',
            {total}, {clean}, {quarantined},
            {duration_sec}, {health_pct}, 0,
            current_date(), current_timestamp(), current_timestamp(), true
        )
    """)
    
    # === APPEND TO RUN HISTORY ===
    history_row = spark.createDataFrame([Row(
        run_id=run_result["run_id"],
        run_timestamp=run_start_time,
        catalog_name=catalog,
        schema_name=schema,
        table_name=table_name,
        dbx_env=dbx_env,
        load_method=run_result.get("load_method", "Unknown"),
        status=run_result["status"],
        total_rows=total,
        clean_rows=clean,
        quarantined_rows=quarantined,
        error_count=0,
        warning_count=0,
        resolved_count=0,
        run_duration_seconds=duration_sec,
        _created_timestamp=datetime.utcnow()
    )])
    
    history_row.write.mode("append").saveAsTable(history_table)
    print(f"Registry and run history updated for {catalog}.{schema}.{table_name}")
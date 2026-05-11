# utils/registry_utils.py
from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def update_registry_and_history(
    result: dict,
    catalog: str,
    schema: str,
    table_name: str,
    dbx_env: str,
    run_start_time: datetime,
    domain_catalog: str
):
    """Updates dq_table_registry and inserts into dq_run_history."""
    registry_table = f"{domain_catalog}.audit_quality.dq_table_registry"
    history_table = f"{domain_catalog}.audit_quality.dq_run_history"

    health_score = round((result.get("valid", 0) / max(result.get("total", 1), 1)) * 100, 2)

    # === Update Registry (UPSERT) ===
    registry_df = spark.createDataFrame([{
        "registry_id": f"{catalog}_{schema}_{table_name}",
        "catalog_name": catalog,
        "schema_name": schema,
        "table_name": table_name,
        "last_run_id": result["run_id"],
        "last_run_timestamp": run_start_time,
        "last_run_status": result["status"],
        "last_total_rows": result.get("total", 0),
        "last_clean_rows": result.get("valid", 0),
        "last_quarantined_rows": result.get("quarantined", 0),
        "last_run_duration_sec": int((datetime.now(datetime.timezone.utc) - run_start_time).total_seconds()),
        "health_score_pct": health_score,
        "_updated_timestamp": datetime.now(datetime.timezone.utc),
        "_is_record_active": True
    }])

    DeltaTable.forName(spark, registry_table).alias("target").merge(
        registry_df.alias("source"),
        "target.catalog_name = source.catalog_name AND target.schema_name = source.schema_name AND target.table_name = source.table_name"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    # === Insert into Run History ===
    history_df = spark.createDataFrame([{
        "run_id": result["run_id"],
        "run_timestamp": run_start_time,
        "catalog_name": catalog,
        "schema_name": schema,
        "table_name": table_name,
        "dbx_env": dbx_env,
        "load_method": result.get("load_method", "Incremental"),
        "status": result["status"],
        "total_rows": result.get("total", 0),
        "clean_rows": result.get("valid", 0),
        "quarantined_rows": result.get("quarantined", 0),
        "error_count": result.get("quarantined", 0),
        "warning_count": 0,  # Extend if you track warnings separately
        "resolved_count": 0,
        "run_duration_seconds": int((datetime.now(datetime.timezone.utc) - run_start_time).total_seconds()),
        "_created_timestamp": datetime.now(datetime.timezone.utc)
    }])

    history_df.write.format("delta").mode("append").saveAsTable(history_table)


def analyze_run(
    catalog: str,
    schema: str,
    table_name: str,
    run_id: str,
    domain_catalog: str
) -> dict:
    """
    Performs Root Cause Analysis on the latest violations.
    Writes results to dq_root_cause_analysis.
    """
    from pyspark.sql import functions as F

    error_log = spark.table(f"{domain_catalog}.audit_quality.error_log")

    recent_violations = error_log.filter(
        (F.col("catalog_name") == catalog) &
        (F.col("schema_name") == schema) &
        (F.col("table_name") == table_name) &
        (F.col("run_id") == run_id)
    )

    if recent_violations.isEmpty():
        return {"status": "NO_VIOLATIONS"}

    # Simple RCA logic (can be expanded significantly)
    new_errors = recent_violations.select("check_name", "column_name").distinct().collect()
    spikes = recent_violations.groupBy("check_name").count().filter(F.col("count") > 10).collect()

    rca_record = {
        "rca_id": f"{run_id}_{table_name}",
        "run_id": run_id,
        "catalog_name": catalog,
        "schema_name": schema,
        "table_name": table_name,
        "analysis_timestamp": datetime.now(datetime.timezone.utc),
        "spikes_detected": str([row.check_name for row in spikes]),
        "new_error_types": str([f"{row.check_name}:{row.column_name}" for row in new_errors]),
        "resolved_errors": "[]",
        "trending_errors": "[]",
        "ai_summary": f"Detected {len(new_errors)} new error types on {table_name}.",
        "recommended_actions": '["Review data quality rules", "Investigate source system"]',
        "_created_timestamp": datetime.now(datetime.timezone.utc)
    }

    rca_df = spark.createDataFrame([rca_record])
    rca_df.write.format("delta").mode("append").saveAsTable(
        f"{domain_catalog}.audit_quality.dq_root_cause_analysis"
    )

    return rca_record
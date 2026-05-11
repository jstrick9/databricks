"""
Event-Driven DQ Triggering.
Monitors S3/ADLS paths and Delta table Change Data Feed
to automatically trigger DQ runs when new data arrives.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from datetime import datetime, timezone
import time
import threading

spark = SparkSession.builder.getOrCreate()

# Registry of active stream monitors
_active_monitors = {}


def monitor_s3_path(
    path: str,
    catalog: str,
    schema: str,
    table_name: str,
    dbx_env: str,
    load_method: str = "Incremental",
    file_format: str = "delta",
    trigger_interval_seconds: int = 300,
    auto_initial_load: bool = False
) -> StreamingQuery:
    """
    Monitor an S3/ADLS path for new files and trigger DQ automatically.

    Args:
        path:                    S3 or ADLS path to monitor
        auto_initial_load:       If True, run Initial load if clean table
                                 doesn't exist yet. If False, wait for manual trigger.
        trigger_interval_seconds: How often to check for new files (default: 5 min)
    """
    from engine.dq_engine import process_table_dq

    print(f"Starting S3/ADLS monitor: {path}")
    print(f"Will trigger DQ for: {catalog}.{schema}.{table_name}")

    # Check if clean table exists for auto_initial_load decision
    if auto_initial_load:
        try:
            from engine.dq_engine import load_env_config
            config = load_env_config(dbx_env, catalog)
            catalog_physical = config["trgt_catalog"]
            clean_table = f"{catalog_physical}.{schema}.{table_name}_clean"
            spark.table(clean_table).limit(1).collect()
            print(f"Clean table exists — using {load_method} load")
        except Exception:
            print(f"Clean table does not exist — auto-triggering Initial load")
            load_method = "Initial"

    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        row_count = batch_df.count()
        print(f"\n{'='*60}")
        print(f"EVENT TRIGGER: {row_count:,} new records detected in {path}")
        print(f"Triggering DQ for {catalog}.{schema}.{table_name}")
        print(f"{'='*60}\n")

        try:
            process_table_dq(
                table_name=table_name,
                schema=schema,
                catalog=catalog,
                dbx_env=dbx_env,
                load_method=load_method
            )
        except Exception as e:
            print(f"Event-triggered DQ failed for {table_name}: {e}")

    query = (
        spark.readStream
        .format(file_format)
        .option("maxFilesPerTrigger", 1)
        .load(path)
        .writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime=f"{trigger_interval_seconds} seconds")
        .option("checkpointLocation", f"{path}/_dq_checkpoint/{table_name}")
        .start()
    )

    _active_monitors[f"{catalog}.{schema}.{table_name}_path"] = query
    print(f"S3/ADLS monitor active for {catalog}.{schema}.{table_name}")
    return query


def monitor_delta_cdf(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    dq_catalog: str,
    dq_schema: str,
    dq_table_name: str,
    dbx_env: str,
    checkpoint_path: str,
    load_method: str = "Incremental",
    trigger_interval_seconds: int = 300
) -> StreamingQuery:
    """
    Monitor a Delta table's Change Data Feed (CDF) and trigger DQ
    automatically when new changes are committed.

    Prerequisites:
        The source table must have CDF enabled:
        ALTER TABLE {catalog}.{schema}.{table} 
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
    """
    from engine.dq_engine import process_table_dq

    full_source = f"{source_catalog}.{source_schema}.{source_table}"
    print(f"Starting Delta CDF monitor: {full_source}")
    print(f"Will trigger DQ for: {dq_catalog}.{dq_schema}.{dq_table_name}")

    def process_changes(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        # Filter for insert/update operations only
        changed = batch_df.filter(
            F.col("_change_type").isin("insert", "update_postimage")
        )

        if changed.rdd.isEmpty():
            return

        change_count = changed.count()
        print(f"\n{'='*60}")
        print(f"CDF EVENT: {change_count:,} changes detected in {full_source}")
        print(f"Triggering DQ for {dq_catalog}.{dq_schema}.{dq_table_name}")
        print(f"{'='*60}\n")

        try:
            process_table_dq(
                table_name=dq_table_name,
                schema=dq_schema,
                catalog=dq_catalog,
                dbx_env=dbx_env,
                load_method=load_method
            )
        except Exception as e:
            print(f"CDF-triggered DQ failed: {e}")

    query = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "latest")
        .table(full_source)
        .writeStream
        .foreachBatch(process_changes)
        .trigger(processingTime=f"{trigger_interval_seconds} seconds")
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    _active_monitors[f"{dq_catalog}.{dq_schema}.{dq_table_name}_cdf"] = query
    print(f"Delta CDF monitor active for {dq_table_name}")
    return query


def enable_cdf_on_table(catalog: str, schema: str, table_name: str) -> None:
    """Enable Change Data Feed on a Delta table."""
    try:
        spark.sql(f"""
            ALTER TABLE {catalog}.{schema}.{table_name}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        print(f"CDF enabled on {catalog}.{schema}.{table_name}")
    except Exception as e:
        print(f"Could not enable CDF: {e}")


def stop_monitor(monitor_key: str) -> None:
    """Stop a specific event monitor."""
    if monitor_key in _active_monitors:
        _active_monitors[monitor_key].stop()
        del _active_monitors[monitor_key]
        print(f"Monitor stopped: {monitor_key}")


def list_active_monitors() -> list:
    """List all currently active event monitors."""
    return [
        {
            "key": k,
            "status": v.status["message"],
            "is_active": v.isActive
        }
        for k, v in _active_monitors.items()
    ]
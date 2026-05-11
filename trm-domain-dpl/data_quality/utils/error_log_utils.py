# utils/error_log_utils.py
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from delta.tables import DeltaTable

def build_error_log_from_dqx_results(
    catalog: str,
    schema: str,
    table_name: str,
    run_id: str,
    clean_df,
    error_df,
    run_ts: datetime,
    dbx_env: str
) -> int:
    """
    Production-grade function to persist DQ violations with AI remediation and SCD2.
    Returns the number of new violations written.
    """
    if error_df.isEmpty():
        return 0

    # 1. Explode DQX violation structure
    violations = (
        error_df.select(
            F.col("_natural_key_hash"),
            F.col("_record_data_hash"),
            F.explode_outer(F.col("_errors")).alias("err")
        )
        .select(
            "err.check_name",
            "err.check_function",
            "err.column_name",
            "err.error_message",
            "err.failed_value",
            "err.criticality",
            "_natural_key_hash",
            "_record_data_hash"
        )
    )

    # 2. Add metadata columns
    violations = (
        violations
        .withColumn("error_log_id", F.expr("uuid()"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("run_timestamp", F.lit(run_ts).cast("timestamp"))
        .withColumn("catalog_name", F.lit(catalog))
        .withColumn("schema_name", F.lit(schema))
        .withColumn("table_name", F.lit(table_name))
        .withColumn("quarantine_table", F.lit(f"trm_domain_dev.{schema}.{table_name}_quarantine"))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("created_by", F.current_user())
        .withColumn("error_status", F.lit("ACTIVE"))
        .withColumn("resolution_reason", F.lit(None).cast("string"))
        .withColumn("resolved_at", F.lit(None).cast("timestamp"))
        .withColumn("resolved_run_id", F.lit(None).cast("string"))
        .withColumn("_created_date", F.current_date())
        .withColumn("_created_timestamp", F.current_timestamp())
        .withColumn("_updated_timestamp", F.current_timestamp())
        .withColumn("_is_record_active", F.lit(True))
    )

    # 3. AI Enrichment (only for 'error' criticality)
    def enrich_ai(error_msg, failed_val, col_name):
        if not failed_val or str(failed_val).lower() in ('null', 'none', ''):
            return '{"fix": null, "conf": 0.0, "reason": "Value is null"}'
        # Call the existing AI function
        return _enrich_violations_with_ai(error_msg, failed_val, col_name, table_name)

    ai_udf = F.udf(enrich_ai, StringType())

    violations = violations.withColumn(
        "ai_json",
        F.when(F.col("criticality") == "error",
               ai_udf(F.col("error_message"), F.col("failed_value"), F.col("column_name")))
         .otherwise(F.lit(None))
    )

    violations = violations.withColumn("suggested_fix", F.get_json_object("ai_json", "$.fix")) \
        .withColumn("fix_confidence_score", F.get_json_object("ai_json", "$.conf").cast(DoubleType())) \
        .withColumn("ai_explanation", F.get_json_object("ai_json", "$.reason")) \
        .withColumn("ai_model_version", F.lit("databricks-meta-llama-3-1-70b-instruct")) \
        .withColumn("remediation_status", F.lit("PENDING"))

    violations = violations.drop("ai_json")

    # 4. Write using Delta MERGE (SCD2 pattern)
    error_log_table = "trm_domain_dev.audit_quality.error_log"
    target = DeltaTable.forName(spark, error_log_table)

    # Deactivate old active records for the same natural keys + checks
    merge_condition = """
        target._natural_key_hash = source._natural_key_hash AND
        target.check_name = source.check_name AND
        target.column_name = source.column_name AND
        target._is_record_active = true
    """

    (
        target.alias("target")
        .merge(violations.alias("source"), merge_condition)
        .whenMatchedUpdate(set={
            "_is_record_active": "false",
            "_updated_timestamp": "current_timestamp()"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

    return violations.count()
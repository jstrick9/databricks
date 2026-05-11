"""
Automated Root Cause Analysis Engine.
Compares the current DQ run against historical runs to automatically
detect and explain failure spikes, new error patterns, and trends.
Uses Databricks Foundation Model APIs to generate human-readable summaries.
"""
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from mlflow.deployments import get_deploy_client
import json

spark = SparkSession.builder.getOrCreate()

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = get_deploy_client("databricks")
    return _client

DATABRICKS_LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"


def analyze_run(
    catalog: str,
    schema: str,
    table_name: str,
    current_run_id: str,
    domain_catalog: str,
    lookback_days: int = 7,
    spike_threshold_pct: float = 10.0
) -> dict:
    """
    Full root cause analysis for a DQ run.

    Args:
        catalog:            Logical catalog name
        schema:             Schema name
        table_name:         Table name
        current_run_id:     The run_id just completed
        domain_catalog:     Physical domain catalog (e.g., trm_domain_dev)
        lookback_days:      How many days of history to compare against
        spike_threshold_pct: Percentage increase that triggers a spike alert

    Returns:
        dict with analysis results and AI-generated explanation
    """
    error_log_table = f"{domain_catalog}.audit_quality.error_log"
    history_table = f"{domain_catalog}.audit_quality.dq_run_history"

    analysis = {
        "run_id": current_run_id,
        "catalog": catalog,
        "schema": schema,
        "table_name": table_name,
        "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
        "spikes_detected": [],
        "new_error_types": [],
        "resolved_error_types": [],
        "trending_errors": [],
        "summary": "",
        "recommended_actions": []
    }

    # ================================================================
    # STEP 1: Get current run stats
    # ================================================================
    try:
        current_stats = spark.table(error_log_table).filter(
            (F.col("run_id") == current_run_id) &
            (F.col("catalog_name") == catalog) &
            (F.col("schema_name") == schema) &
            (F.col("table_name") == table_name)
        ).groupBy("check_name", "column_name", "criticality").agg(
            F.count("*").alias("failure_count")
        )

        current_total = current_stats.agg(
            F.sum("failure_count")
        ).collect()[0][0] or 0

        current_by_check = {
            f"{r['check_name']}::{r['column_name']}": r["failure_count"]
            for r in current_stats.collect()
        }
    except Exception as e:
        print(f"Could not query current run stats: {e}")
        return analysis

    # ================================================================
    # STEP 2: Get historical baseline
    # ================================================================
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    try:
        historical_stats = spark.table(error_log_table).filter(
            (F.col("catalog_name") == catalog) &
            (F.col("schema_name") == schema) &
            (F.col("table_name") == table_name) &
            (F.col("run_id") != current_run_id) &
            (F.col("run_timestamp") >= F.lit(cutoff).cast("timestamp"))
        ).groupBy("check_name", "column_name", "criticality").agg(
            F.count("*").alias("total_failures"),
            F.countDistinct("run_id").alias("run_count")
        ).withColumn(
            "avg_failures_per_run",
            F.col("total_failures") / F.col("run_count")
        )

        historical_by_check = {
            f"{r['check_name']}::{r['column_name']}": r["avg_failures_per_run"]
            for r in historical_stats.collect()
        }

        historical_total_avg = sum(historical_by_check.values()) or 0
    except Exception as e:
        print(f"Could not query historical stats: {e}")
        historical_by_check = {}
        historical_total_avg = 0

    # ================================================================
    # STEP 3: Detect spikes, new errors, and resolved errors
    # ================================================================

    # Overall failure spike?
    if historical_total_avg > 0:
        overall_change_pct = (
            (current_total - historical_total_avg) / historical_total_avg
        ) * 100

        if overall_change_pct >= spike_threshold_pct:
            analysis["spikes_detected"].append({
                "type": "overall_failure_spike",
                "current": current_total,
                "historical_avg": round(historical_total_avg, 1),
                "change_pct": round(overall_change_pct, 1),
                "severity": "critical" if overall_change_pct > 50 else "warning"
            })

    # Per-check analysis
    all_check_keys = set(list(current_by_check.keys()) + list(historical_by_check.keys()))

    for check_key in all_check_keys:
        current_count = current_by_check.get(check_key, 0)
        historical_avg = historical_by_check.get(check_key, 0)

        # NEW error type (never seen before)
        if current_count > 0 and historical_avg == 0:
            check_name, col_name = check_key.split("::", 1)
            analysis["new_error_types"].append({
                "check_name": check_name,
                "column_name": col_name,
                "current_failures": current_count,
                "message": f"New failure pattern detected on {col_name} — never failed in last {lookback_days} days"
            })

        # RESOLVED error type (was failing, now clean)
        elif current_count == 0 and historical_avg > 0:
            check_name, col_name = check_key.split("::", 1)
            analysis["resolved_error_types"].append({
                "check_name": check_name,
                "column_name": col_name,
                "previous_avg_failures": round(historical_avg, 1),
                "message": f"Previously failing check on {col_name} is now passing"
            })

        # SPIKE on existing check
        elif historical_avg > 0 and current_count > 0:
            check_pct = ((current_count - historical_avg) / historical_avg) * 100
            if check_pct >= spike_threshold_pct:
                check_name, col_name = check_key.split("::", 1)
                analysis["spikes_detected"].append({
                    "type": "check_spike",
                    "check_name": check_name,
                    "column_name": col_name,
                    "current": current_count,
                    "historical_avg": round(historical_avg, 1),
                    "change_pct": round(check_pct, 1),
                    "severity": "critical" if check_pct > 50 else "warning"
                })

    # ================================================================
    # STEP 4: Get trending errors (consistently high over time)
    # ================================================================
    try:
        trending = spark.table(error_log_table).filter(
            (F.col("catalog_name") == catalog) &
            (F.col("schema_name") == schema) &
            (F.col("table_name") == table_name) &
            (F.col("run_timestamp") >= F.lit(cutoff).cast("timestamp"))
        ).groupBy("check_name", "column_name").agg(
            F.count("*").alias("total_failures"),
            F.countDistinct("run_id").alias("runs_failing")
        ).filter(
            F.col("runs_failing") >= 3  # Failing in at least 3 runs
        ).orderBy(F.col("total_failures").desc()).limit(5)

        for row in trending.collect():
            analysis["trending_errors"].append({
                "check_name": row["check_name"],
                "column_name": row["column_name"],
                "total_failures": row["total_failures"],
                "runs_failing": row["runs_failing"],
                "message": f"Consistently failing across {row['runs_failing']} runs"
            })
    except Exception as e:
        print(f"Could not compute trending errors: {e}")

    # ================================================================
    # STEP 5: AI-generated summary and recommended actions
    # ================================================================
    analysis_data = {
        "overall_spikes": analysis["spikes_detected"],
        "new_error_types": analysis["new_error_types"],
        "resolved_errors": analysis["resolved_error_types"],
        "trending_errors": analysis["trending_errors"],
        "current_total_failures": current_total,
        "historical_avg_failures": round(historical_total_avg, 1)
    }

    try:
        client = _get_client()
        response = client.predict(
            endpoint=DATABRICKS_LLM_ENDPOINT,
            inputs={
                "messages": [
                    {
                        "role": "system",
                        "content": """You are a Data Quality analyst. Given root cause 
analysis data, write a concise 3-4 sentence summary explaining what happened 
and why, followed by a bulleted list of 2-3 specific recommended actions. 
Be direct and specific. Avoid generic advice."""
                    },
                    {
                        "role": "user",
                        "content": f"""Analyze this DQ run for table {catalog}.{schema}.{table_name}:

{json.dumps(analysis_data, indent=2)}

Write a summary and specific recommended actions."""
                    }
                ],
                "max_tokens": 500,
                "temperature": 0.3
            }
        )
        ai_summary = response["choices"][0]["message"]["content"].strip()
        analysis["summary"] = ai_summary

        # Extract recommended actions separately
        lines = ai_summary.split("\n")
        actions = [
            line.lstrip("•-* ").strip()
            for line in lines
            if line.strip().startswith(("•", "-", "*", "1.", "2.", "3."))
        ]
        analysis["recommended_actions"] = actions[:3]

    except Exception as e:
        print(f"AI summary generation failed: {e}")
        analysis["summary"] = _build_fallback_summary(analysis)

    return analysis


def _build_fallback_summary(analysis: dict) -> str:
    """Plain-text fallback summary when AI is unavailable."""
    parts = []

    if analysis["spikes_detected"]:
        spike = analysis["spikes_detected"][0]
        parts.append(
            f"Failure spike detected: {spike.get('change_pct', 0):.1f}% increase "
            f"({spike.get('current', 0):,} vs avg {spike.get('historical_avg', 0):,.1f})."
        )

    if analysis["new_error_types"]:
        cols = [e["column_name"] for e in analysis["new_error_types"]]
        parts.append(f"New error types appeared on columns: {', '.join(cols)}.")

    if analysis["resolved_error_types"]:
        cols = [e["column_name"] for e in analysis["resolved_error_types"]]
        parts.append(f"Previously failing checks resolved on: {', '.join(cols)}.")

    if analysis["trending_errors"]:
        cols = [e["column_name"] for e in analysis["trending_errors"]]
        parts.append(f"Persistently failing columns: {', '.join(cols)}.")

    return " ".join(parts) if parts else "No significant changes detected in this run."


def save_analysis(analysis: dict, domain_catalog: str) -> None:
    """Save the root cause analysis results to a Delta table."""
    rca_table = f"{domain_catalog}.audit_quality.dq_root_cause_analysis"

    rca_row = spark.createDataFrame([{
        "run_id":             analysis["run_id"],
        "catalog_name":       analysis["catalog"],
        "schema_name":        analysis["schema"],
        "table_name":         analysis["table_name"],
        "analysis_timestamp": analysis["analysis_timestamp"],
        "spikes_detected":    json.dumps(analysis["spikes_detected"]),
        "new_error_types":    json.dumps(analysis["new_error_types"]),
        "resolved_errors":    json.dumps(analysis["resolved_error_types"]),
        "trending_errors":    json.dumps(analysis["trending_errors"]),
        "ai_summary":         analysis["summary"],
        "recommended_actions": json.dumps(analysis["recommended_actions"]),
        "_created_timestamp": datetime.now(timezone.utc).isoformat()
    }])

    rca_row.write.mode("append").option("mergeSchema", "true").saveAsTable(rca_table)
    print(f"Root cause analysis saved to {rca_table}")
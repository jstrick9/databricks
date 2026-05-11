# /data_quality/utils/teams_notifier.py
import requests
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

def send_teams_notification(
    run_result: dict,
    catalog: str,
    schema: str,
    table_name: str,
    dbx_env: str,
    run_start_time: datetime
) -> None:
    # from data_quality.engine.dq_engine import load_env_config
    from engine.dq_engine import load_env_config
    
    config = load_env_config(dbx_env, catalog)
    webhook_url = config.get("ms_teams_webhook")
    if not webhook_url:
        print("MS Teams webhook not configured")
        return

    catalog_physical = config["trgt_catalog"]
    domain_catalog = config["domain_catalog"]
    lake_base = config["lake_base_url"]
    workspace_id = config["workspace_id"]

    clean_url = f"{lake_base}/explore/data/{catalog_physical}/{schema}/{table_name}_clean?o={workspace_id}"
    quarantine_url = f"{lake_base}/explore/data/{catalog_physical}/{schema}/{table_name}_quarantine?o={workspace_id}"
    error_log_url = f"{lake_base}/explore/data/{domain_catalog}/audit_quality/error_log?o={workspace_id}"

    status = run_result["status"]
    run_id = run_result["run_id"]
    valid_count = run_result.get("valid", 0)
    quarantined_count = run_result.get("quarantined", 0)
    total_count = valid_count + quarantined_count

    # FIX: Scoped exception handling with logging instead of bare except
    errors = warnings = resolved = 0
    try:
        error_log_table = f"{domain_catalog}.audit_quality.error_log"
        summary = spark.table(error_log_table).filter(F.col("run_id") == run_id) \
            .groupBy("criticality").count().collect()
        errors = next((r["count"] for r in summary if r["criticality"] == "error"), 0)
        warnings = next((r["count"] for r in summary if r["criticality"] == "warning"), 0)
        resolved = spark.table(error_log_table).filter(
            (F.col("run_id") == run_id) & (F.col("error_status") == "RESOLVED")
        ).count()
    except Exception as e:
        print(f"Warning: Could not query error_log for notification metrics: {e}")

    # FIX: Use UTC to match run_start_time (which is now UTC from dq_engine)
    duration = str(datetime.now(timezone.utc) - run_start_time).split(".")[0]

    # FIX: Three-tier color coding for visual urgency
    if status == "PASS":
        color = "good"
    elif quarantined_count > 0 and errors > 100:
        color = "warning"      # Red for critical failures
    else:
        color = "attention"    # Yellow for minor quarantines

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.5",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "TRADEMARK DATA QUALITY RUN COMPLETE",
                        "weight": "bolder",
                        "size": "large",
                        "color": color
                    },
                    {
                        "type": "ColumnSet",
                        "columns": [
                            {"type": "Column", "width": "auto", "items": [{"type": "TextBlock", "text": "Table", "weight": "bolder"}]},
                            {"type": "Column", "width": "stretch", "items": [{"type": "TextBlock", "text": f"`{catalog_physical}.{schema}.{table_name}`"}]}
                        ]
                    },
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "Environment", "value": dbx_env.upper()},
                            {"title": "Status", "value": status},
                            {"title": "Run ID", "value": run_id[:8]},
                            {"title": "Duration", "value": duration},
                            {"title": "Total", "value": f"{total_count:,}"},
                            {"title": "Clean", "value": f"{valid_count:,}"},
                            {"title": "Quarantined", "value": f"{quarantined_count:,}"},
                            {"title": "Errors", "value": str(errors)},
                            {"title": "Warnings", "value": str(warnings)},
                            {"title": "Resolved", "value": str(resolved)}
                        ]
                    }
                ],
                "actions": [
                    {"type": "Action.OpenUrl", "title": "Open Clean Table", "url": clean_url},
                    {"type": "Action.OpenUrl", "title": "Open Quarantine Table", "url": quarantine_url},
                    {"type": "Action.OpenUrl", "title": "Open Error Log", "url": error_log_url}
                ]
            }
        }]
    }

    try:
        response = requests.post(webhook_url, json=card, timeout=15)
        if response.status_code == 200:
            print("MS Teams notification sent successfully")
        else:
            print(f"Teams notification failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"Failed to send Teams notification: {e}")
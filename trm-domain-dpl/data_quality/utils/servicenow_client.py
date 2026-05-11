"""
ServiceNow Integration.
Automatically creates incidents when DQ runs exceed failure thresholds.
Configurable via environment config YAML and Databricks Secrets.
"""
import requests
import json
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient

_w = None

def _get_workspace_client():
    global _w
    if _w is None:
        _w = WorkspaceClient()
    return _w

def _get_secret(scope: str, key: str) -> str:
    return _get_workspace_client().secrets.get(scope=scope, key=key).value

# Default criticality to ServiceNow priority mapping
PRIORITY_MAP = {
    "critical": "1",  # Critical
    "high":     "2",  # High
    "medium":   "3",  # Moderate
    "low":      "4",  # Low
    "warning":  "4"   # Low
}


def create_incident(
    run_result: dict,
    catalog: str,
    schema: str,
    table_name: str,
    dbx_env: str,
    root_cause_analysis: dict = None,
    failure_threshold_pct: float = 5.0,
    secret_scope: str = "dq_secrets"
) -> dict:
    """
    Create a ServiceNow incident when DQ failure rate exceeds the threshold.

    Required Databricks Secrets in scope '{secret_scope}':
      - snow_instance:  ServiceNow instance URL (e.g., https://yourorg.service-now.com)
      - snow_username:  API username
      - snow_password:  API password
      - snow_table:     ServiceNow table (default: 'incident')

    Args:
        failure_threshold_pct: Only create incident if quarantine rate exceeds this %

    Returns:
        dict with incident_number and sys_id if created, or empty dict
    """
    total = run_result.get("total", 0)
    quarantined = run_result.get("quarantined", 0)

    if total == 0:
        return {}

    failure_rate = (quarantined / total) * 100

    if failure_rate < failure_threshold_pct:
        print(f"Failure rate {failure_rate:.1f}% below threshold {failure_threshold_pct}% — no ServiceNow incident created")
        return {}

    # Load ServiceNow credentials
    try:
        snow_instance = _get_secret(secret_scope, "snow_instance")
        snow_username = _get_secret(secret_scope, "snow_username")
        snow_password = _get_secret(secret_scope, "snow_password")
        snow_table = "incident"
        try:
            snow_table = _get_secret(secret_scope, "snow_table")
        except Exception:
            pass
    except Exception as e:
        print(f"Could not load ServiceNow secrets: {e}")
        return {}

    # Determine priority based on failure severity
    priority = "2"  # Default: High
    if failure_rate > 50:
        priority = "1"  # Critical
    elif failure_rate > 20:
        priority = "2"  # High
    elif failure_rate > 5:
        priority = "3"  # Moderate

    # Build incident description
    run_id = run_result.get("run_id", "N/A")
    rca_summary = ""
    if root_cause_analysis and root_cause_analysis.get("summary"):
        rca_summary = f"\n\nROOT CAUSE ANALYSIS:\n{root_cause_analysis['summary']}"

    recommended_actions = ""
    if root_cause_analysis and root_cause_analysis.get("recommended_actions"):
        actions = "\n".join([
            f"  • {a}"
            for a in root_cause_analysis["recommended_actions"]
        ])
        recommended_actions = f"\n\nRECOMMENDED ACTIONS:\n{actions}"

    description = f"""DATA QUALITY ALERT — {dbx_env.upper()} ENVIRONMENT

Table: {catalog}.{schema}.{table_name}
Run ID: {run_id}
Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

FAILURE STATISTICS:
  Total Records:    {total:,}
  Clean Records:    {run_result.get('valid', 0):,}
  Quarantined:      {quarantined:,}
  Failure Rate:     {failure_rate:.1f}%
  Health Score:     {100 - failure_rate:.1f}%
{rca_summary}
{recommended_actions}

ACTION REQUIRED:
Review the quarantine table in Databricks Unity Catalog:
  {catalog}.{schema}.{table_name}_quarantine

Review the error log for detailed violation information:
  audit_quality.error_log (run_id = {run_id})"""

    short_description = (
        f"DQ Alert: {catalog}.{schema}.{table_name} — "
        f"{failure_rate:.1f}% failure rate ({quarantined:,} records quarantined) "
        f"in {dbx_env.upper()}"
    )

    payload = {
        "short_description": short_description,
        "description": description,
        "category": "Data Quality",
        "subcategory": "Data Validation",
        "urgency": priority,
        "impact": priority,
        "priority": priority,
        "assignment_group": "Data Engineering",
        "u_catalog": catalog,
        "u_schema": schema,
        "u_table_name": table_name,
        "u_environment": dbx_env.upper(),
        "u_run_id": run_id,
        "u_failure_rate": str(round(failure_rate, 2)),
        "u_quarantined_count": str(quarantined)
    }

    url = f"{snow_instance}/api/now/table/{snow_table}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        response = requests.post(
            url,
            auth=(snow_username, snow_password),
            headers=headers,
            data=json.dumps(payload),
            timeout=30
        )
        response.raise_for_status()

        result = response.json().get("result", {})
        incident_number = result.get("number", "N/A")
        sys_id = result.get("sys_id", "N/A")

        print(f"ServiceNow incident created: {incident_number} (sys_id: {sys_id})")

        return {
            "incident_number": incident_number,
            "sys_id": sys_id,
            "priority": priority,
            "failure_rate": failure_rate
        }

    except Exception as e:
        print(f"ServiceNow incident creation failed: {e}")
        return {}


def resolve_incident(
    sys_id: str,
    resolution_notes: str,
    secret_scope: str = "dq_secrets"
) -> bool:
    """
    Automatically resolve a ServiceNow incident when DQ issues are fixed.
    Called by the self-healing logic in load_utils.py.
    """
    try:
        snow_instance = _get_secret(secret_scope, "snow_instance")
        snow_username = _get_secret(secret_scope, "snow_username")
        snow_password = _get_secret(secret_scope, "snow_password")
        snow_table = "incident"
    except Exception as e:
        print(f"Could not load ServiceNow secrets: {e}")
        return False

    payload = {
        "state": "6",  # 6 = Resolved in ServiceNow
        "close_code": "Solved (Permanently)",
        "close_notes": resolution_notes
    }

    url = f"{snow_instance}/api/now/table/{snow_table}/{sys_id}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.patch(
            url,
            auth=(snow_username, snow_password),
            headers=headers,
            data=json.dumps(payload),
            timeout=30
        )
        response.raise_for_status()
        print(f"ServiceNow incident {sys_id} resolved successfully")
        return True
    except Exception as e:
        print(f"ServiceNow incident resolution failed: {e}")
        return False
"""
Email Notification Utility.
Sends HTML email alerts via Microsoft Exchange / Office 365
using a service account stored in Databricks Secrets.
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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


def send_email_notification(
    run_result: dict,
    catalog: str,
    schema: str,
    table_name: str,
    dbx_env: str,
    run_start_time: datetime,
    root_cause_analysis: dict = None,
    recipient_emails: list = None,
    secret_scope: str = "dq_secrets"
) -> None:
    """
    Send an HTML email alert after a DQ run completes.

    Secrets required in Databricks Secret Scope '{secret_scope}':
      - email_sender:   Service account email address
      - email_password: Service account password
      - email_smtp:     SMTP server (e.g., smtp.office365.com)
      - email_port:     SMTP port (e.g., 587)
      - email_distro:   Comma-separated default recipients
    """
    try:
        sender = _get_secret(secret_scope, "email_sender")
        password = _get_secret(secret_scope, "email_password")
        smtp_server = _get_secret(secret_scope, "email_smtp")
        smtp_port = int(_get_secret(secret_scope, "email_port"))
        default_distro = _get_secret(secret_scope, "email_distro")
    except Exception as e:
        print(f"Could not load email secrets from scope '{secret_scope}': {e}")
        return

    recipients = recipient_emails or [r.strip() for r in default_distro.split(",")]

    status = run_result.get("status", "UNKNOWN")
    total = run_result.get("total", 0)
    valid = run_result.get("valid", 0)
    quarantined = run_result.get("quarantined", 0)
    run_id = run_result.get("run_id", "N/A")
    duration = str(datetime.now(timezone.utc) - run_start_time).split(".")[0]

    health_pct = round((valid / total) * 100, 1) if total > 0 else 0
    status_color = "#2e7d32" if status == "PASS" else "#c62828"
    status_bg = "#e8f5e9" if status == "PASS" else "#ffebee"

    # Build root cause analysis section
    rca_html = ""
    if root_cause_analysis and (
        root_cause_analysis.get("spikes_detected") or
        root_cause_analysis.get("new_error_types")
    ):
        rca_html = f"""
        <tr>
            <td colspan="2" style="padding: 16px; background: #fff3e0; border-radius: 4px;">
                <strong style="color: #e65100;">🔍 Root Cause Analysis</strong><br><br>
                {root_cause_analysis.get('summary', 'No summary available.')}
            </td>
        </tr>"""

        if root_cause_analysis.get("recommended_actions"):
            actions_html = "".join([
                f"<li>{a}</li>"
                for a in root_cause_analysis["recommended_actions"]
            ])
            rca_html += f"""
        <tr>
            <td colspan="2" style="padding: 12px 16px;">
                <strong>Recommended Actions:</strong>
                <ul style="margin: 8px 0 0 0;">{actions_html}</ul>
            </td>
        </tr>"""

    html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f5f5f5; margin: 0; padding: 20px; }}
        .container {{ max-width: 700px; margin: 0 auto; background: white; border-radius: 8px; 
                      box-shadow: 0 2px 8px rgba(0,0,0,0.1); overflow: hidden; }}
        .header {{ background: {status_color}; color: white; padding: 24px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 22px; font-weight: 600; }}
        .header p {{ margin: 8px 0 0 0; opacity: 0.9; font-size: 14px; }}
        .body {{ padding: 24px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 16px 0; }}
        td {{ padding: 10px 12px; border-bottom: 1px solid #f0f0f0; font-size: 14px; }}
        td:first-child {{ color: #666; font-weight: 500; width: 35%; }}
        .metric {{ text-align: center; padding: 16px; background: {status_bg}; 
                   border-radius: 4px; margin: 8px; display: inline-block; min-width: 120px; }}
        .metric-value {{ font-size: 28px; font-weight: 700; color: {status_color}; }}
        .metric-label {{ font-size: 12px; color: #666; margin-top: 4px; }}
        .metrics-row {{ display: flex; justify-content: center; flex-wrap: wrap; margin: 16px 0; }}
        .footer {{ padding: 16px 24px; background: #f5f5f5; font-size: 12px; color: #999; 
                   text-align: center; }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>{'✅' if status == 'PASS' else '⚠️'} Data Quality Run {status}</h1>
        <p>{catalog}.{schema}.{table_name} | {dbx_env.upper()} Environment</p>
    </div>

    <div class="body">
        <div class="metrics-row">
            <div class="metric">
                <div class="metric-value">{total:,}</div>
                <div class="metric-label">Total Records</div>
            </div>
            <div class="metric">
                <div class="metric-value">{valid:,}</div>
                <div class="metric-label">Clean Records</div>
            </div>
            <div class="metric">
                <div class="metric-value">{quarantined:,}</div>
                <div class="metric-label">Quarantined</div>
            </div>
            <div class="metric">
                <div class="metric-value">{health_pct}%</div>
                <div class="metric-label">Health Score</div>
            </div>
        </div>

        <table>
            <tr><td>Run ID</td><td>{run_id[:8]}...</td></tr>
            <tr><td>Environment</td><td>{dbx_env.upper()}</td></tr>
            <tr><td>Duration</td><td>{duration}</td></tr>
            {rca_html}
        </table>
    </div>

    <div class="footer">
        Generated by Enterprise Data Quality Framework | 
        {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
    </div>
</div>
</body>
</html>"""

    subject_prefix = "✅ DQ PASS" if status == "PASS" else "⚠️ DQ ALERT"
    subject = f"{subject_prefix} | {catalog}.{schema}.{table_name} | {dbx_env.upper()}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        print(f"Email notification sent to: {', '.join(recipients)}")
    except Exception as e:
        print(f"Email send failed: {e}")
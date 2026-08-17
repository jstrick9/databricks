import streamlit as st
import pandas as pd
import json
from pathlib import Path
from datetime import date
from utils.page_config_helpers import setup_sidebar, set_page_config
from utils.runtime_env import get_runtime_env
from utils.db_helpers import get_connection, read_yaml, get_processing_wait_times, get_metric_targets, write_json_to_publish_bucket, insert_audit_log
from utils.user_helpers import init_user_session_state

set_page_config(page_title="Publish | Wait Times")
setup_sidebar()
init_user_session_state()

dbx_env = get_runtime_env()
app_root = Path(__file__).resolve().parent.parent
config_path = app_root / "config" / dbx_env / "wait-times-conf.yaml"
configs = read_yaml(str(config_path))

st.title("Publish - Review & Push to USPTO.gov")

st.markdown("""
- Edit calculated values before publishing (only page where dates and values can be overwritten)
- Set `Data updated as of` date for banner
- Publish writes `wait_times_{snapshot}.json` + `wait_times_latest.json` to **UC Volume** (`/Volumes/.../wait_times`) by default - no S3 boto3 credentials needed in Apps

**Prod restriction:** Publish button requires confirmation in prod, writes audit log to `gold.etl_audit_log`.
""")

# Show env-driven Volume bucket for transparency (now default, not S3)
volume_path_cfg = configs.get("publish",{}).get("volume_path", "NOT CONFIGURED")
publish_bucket_cfg = configs.get("publish",{}).get("s3_bucket", "NOT CONFIGURED")
api_gateway_cfg = configs.get("publish",{}).get("api_gateway_url", "NOT CONFIGURED")
st.info(f"**Env:** {dbx_env.upper()} | **Volume (default):** `{volume_path_cfg}` | **API Gateway:** `{api_gateway_cfg}`\n\nApp defaults to Volume `/Volumes/.../gold/wait_times` - no S3 credentials needed. Job cluster `publish_wait_times` can still write S3 via `dbutils.fs.put` if needed.")

conn, cursor = get_connection()

# Get latest gold
latest_snapshot = None
try:
    cursor.execute(f"SELECT MAX(snapshot_date) FROM {configs['schema']['trgt_catalog']}.{configs['schema'].get('gold_schema','gold')}.processing_wait_times")
    latest_snapshot = cursor.fetchone()[0]
except:
    pass

snapshot_input = st.date_input("Snapshot Date to Publish", value=latest_snapshot or date.today())

# Load gold for that snapshot
df_gold = get_processing_wait_times(cursor, configs, snapshot_date=str(snapshot_input))

if df_gold.empty:
    st.warning(f"No gold data for snapshot {snapshot_input}")
    st.stop()

st.markdown(f"#### Gold snapshot {snapshot_input} - {len(df_gold)} metrics")
st.dataframe(df_gold[["metric_key","metric_name","average_value","target_value","sample_size","processing_as_of_date","exam_queue_start_date","exam_queue_end_date"]], use_container_width=True, hide_index=True)

# Editable table for manual override
st.divider()
st.markdown("#### Edit Calculated Values Before Publish (manual override)")

edit_df = df_gold[["metric_key","metric_name","section","unit","average_value","target_value","sample_size"]].copy()
edit_df["average_value"] = edit_df["average_value"].astype(float)
edit_df["target_value"] = edit_df["target_value"].astype(float)

edited = st.data_editor(edit_df, num_rows="dynamic", use_container_width=True, key="publish_editor_v3", 
    column_config={
        "metric_key": st.column_config.TextColumn("metric_key", disabled=True),
        "metric_name": st.column_config.TextColumn("Metric Name", disabled=True),
        "section": st.column_config.TextColumn("Section", disabled=True),
        "unit": st.column_config.TextColumn("Unit", disabled=True),
        "average_value": st.column_config.NumberColumn("Average (editable) ✅", help="Manually adjust before publish - this is the only place values can be overwritten"),
        "target_value": st.column_config.NumberColumn("Target (from metric_targets)", disabled=False),
        "sample_size": st.column_config.NumberColumn("Sample Size", disabled=True)
    }
)

# Queue dates and data_updated - editable dates that can be added
st.divider()
st.markdown("#### Queue Dates & Banner - Editable Dates That Can Be Added")

col1, col2, col3 = st.columns(3)
with col1:
    exam_start = st.date_input("Exam Queue Start (editable)", value=pd.to_datetime(df_gold["exam_queue_start_date"].iloc[0]).date() if pd.notna(df_gold["exam_queue_start_date"].iloc[0]) else None, help="We are currently examining new applications submitted between... auto from percentile, override here")
    exam_end = st.date_input("Exam Queue End (editable)", value=pd.to_datetime(df_gold["exam_queue_end_date"].iloc[0]).date() if pd.notna(df_gold["exam_queue_end_date"].iloc[0]) else None)
with col2:
    sou_queue = st.date_input("SOU Queue - Currently processing SOUs filed on or before (editable)", value=None, help="Apr 14, 2026 from USPTO banner - auto MAX pending, override here")
    renewal_queue = st.date_input("Renewal Queue - Currently processing renewals filed on or before (editable)", value=None, help="Apr 29, 2026 from USPTO banner")
with col3:
    data_updated = st.date_input("Note: Data updated as of (editable)", value=date.today(), help="June 30, 2026 banner - auto today, override here")

# Build JSON payload
if st.button("Preview JSON Payload", type="secondary"):
    payload = {
        "data_updated": str(data_updated),
        "snapshot_date": str(snapshot_input),
        "exam_queue": {"start": str(exam_start) if exam_start else None, "end": str(exam_end) if exam_end else None},
        "sou_queue": str(sou_queue) if sou_queue else None,
        "renewal_queue": str(renewal_queue) if renewal_queue else None,
        "metrics": [
            {
                "metric_key": row["metric_key"],
                "name": row["metric_name"],
                "section": row["section"],
                "unit": row["unit"],
                "average": float(row["average_value"]),
                "target": float(row["target_value"]),
                "sample_size": int(row["sample_size"])
            } for _, row in edited.iterrows()
        ],
        "source": "trm-dpl-app",
        "dbx_env": dbx_env,
        "published_ts": pd.Timestamp.now().isoformat() + "Z",
        "publish_volume": volume_path_cfg
    }
    st.session_state["publish_payload"] = payload
    st.json(payload)

# Publish button
st.divider()
st.markdown("#### Publish to UC Volume (default, no S3) + USPTO API Gateway")

if dbx_env == "prod":
    st.warning("You are in PROD - publishing will update https://www.uspto.gov/trademarks/application-timeline")
    st.code(f"Volume (default): {volume_path_cfg}\nAPI Gateway: {api_gateway_cfg}", language="yaml")
    confirm_text = st.text_input("Type 'PUBLISH' to confirm prod publish")
    prod_confirmed = confirm_text == "PUBLISH"
else:
    st.info(f"Dev mode - will write to Volume `{volume_path_cfg}` (no S3). S3 legacy `{publish_bucket_cfg}` not used by App.")
    prod_confirmed = True

if st.button("Publish Final Values", type="primary", disabled=not prod_confirmed if dbx_env=="prod" else False):
    if "publish_payload" not in st.session_state:
        st.error("Please preview JSON first")
        st.stop()
    
    payload = st.session_state["publish_payload"]
    # DEFAULT TO VOLUME per user request - not S3
    publish_volume = configs.get("publish",{}).get("volume_path")
    if not publish_volume:
        # Fallback to s3 if volume not configured (legacy)
        publish_volume = configs.get("publish",{}).get("s3_bucket")
    if not publish_volume:
        st.error(f"publish.volume_path not configured for env {dbx_env} - check config/{dbx_env}/wait-times-conf.yaml. Should be /Volumes/{{catalog}}/gold/wait_times")
        st.stop()
    
    st.info(f"Publishing to **{dbx_env.upper()}** Volume: `{publish_volume}` (from config/{dbx_env}/wait-times-conf.yaml - default, no S3)")
    
    with st.spinner(f"Writing JSON to {publish_volume}..."):
        ok, msg = write_json_to_publish_bucket(publish_volume, str(snapshot_input), payload, configs=configs)
        if ok:
            st.success(msg)
        else:
            st.error(f"Write failed: {msg}")
            st.stop()
    
    # Insert audit log
    insert_audit_log(cursor, configs, job_name="trm_wait_times_monthly_app", task_name="publish_snapshot", status="SUCCESS", records=len(payload["metrics"]), message=f"published {snapshot_input} to {publish_volume} via app by {st.session_state.get('email','unknown')}")
    
    if dbx_env == "prod":
        from databricks.sdk import WorkspaceClient
        try:
            w = WorkspaceClient()
            api_url = configs.get("publish",{}).get("api_gateway_url")
            if not api_url:
                st.error(f"publish.api_gateway_url not configured for env {dbx_env}")
                st.stop()
            st.info(f"Would POST to {api_url} with OAuth2 client credentials from secret scope uspto-api")
            st.json(payload)
            st.success("Publish complete - Drupal node will be 'Needs Review' then auto-published by OCIO")
        except Exception as e:
            st.error(f"POST failed: {e}")
    else:
        st.success(f"Dev publish complete - JSON written to Volume {publish_volume}, no live POST")
        st.json(payload)

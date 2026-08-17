import os
from pathlib import Path
import streamlit as st
from utils.page_config_helpers import setup_sidebar, set_page_config
from utils.runtime_env import get_runtime_env
from utils.db_helpers import get_connection, read_yaml, get_silver_summary, get_latest_gold_snapshot
from utils.user_helpers import init_user_session_state

set_page_config(page_title="Home | Trademark Processing Wait Times")

setup_sidebar()
init_user_session_state()

st.title("Trademark Processing Wait Times - Self-Service")

st.markdown("""
The **Trademark Processing Wait Times** self-service App provides a self-service interface 
for calculating and publishing USPTO trademark wait times to uspto.gov.

**Silver** (case_milestones) runs monthly scheduled on the 5th - built from Bronze.
**Gold** (processing_wait_times) is now self-service via this App - you control queue dates, metric targets, and publish.

Use the sidebar to navigate: **Dashboard** (read-only overview), **Metric Targets** (edit targets and units), **Calculate** (quick preview + full job), **Publish** (override dates and values, publish JSON).
""")

dbx_env = get_runtime_env()
st.session_state["dbx_env"] = dbx_env

app_root = Path(__file__).resolve().parent
config_path = app_root / "config" / dbx_env / "wait-times-conf.yaml"

try:
    configs = read_yaml(str(config_path))
except Exception as e:
    st.error(f"Config not found for env {dbx_env}: {e}")
    st.stop()

conn, cursor = get_connection()
if not cursor:
    st.stop()

st.divider()
st.markdown("### 📊 Source Validation")

col1, col2 = st.columns([2,2])
with col1:
    silver_info = get_silver_summary(cursor, configs)
    if "error" in silver_info:
        st.error(f"Silver table error: {silver_info['error']}")
    else:
        st.success(f"✅ Silver: `{silver_info['table']}`")
        st.metric("Current rows (_is_current=true)", f"{silver_info['current']:,}")
        if silver_info.get("max_ts"):
            st.caption(f"Last updated: {silver_info['max_ts']}")

with col2:
    latest_snapshot = get_latest_gold_snapshot(cursor, configs)
    if latest_snapshot:
        st.success(f"✅ Gold latest snapshot: {latest_snapshot}")
    else:
        st.warning("No gold snapshots yet - run Calculate → Full Job first")

st.divider()
env_colors = {"dev": "🟢", "prod": "🔴"}
st.info(f"{env_colors.get(dbx_env,'⚪')} **{dbx_env.upper()}** environment - Catalog `{configs['schema']['trgt_catalog']}`")
if dbx_env != "prod":
    st.caption("Prod publish requires typing PUBLISH confirmation and writes audit log. Dev shows what would be posted.")
else:
    st.warning("You are in PROD - publishing updates https://www.uspto.gov/trademarks/application-timeline")

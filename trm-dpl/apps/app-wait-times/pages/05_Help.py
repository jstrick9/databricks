import streamlit as st
from pathlib import Path
from utils.page_config_helpers import setup_sidebar, set_page_config
from utils.runtime_env import get_runtime_env
from utils.user_helpers import init_user_session_state

set_page_config(page_title="Help | Wait Times")
setup_sidebar()
init_user_session_state()

st.title("📚 Help - User Guides & Architecture")

st.markdown("""
This Help page provides a user guide for users of this Databricks app and an architecture document for data engineers who want to understand how this app was built, build similar apps, and/or refactor this app.

Download the Word docs below - they are saved in `apps/app-wait-times/resources/` and attached to this App.
""")

app_root = Path(__file__).resolve().parent.parent

# User Guide
st.divider()
st.markdown("### 📖 1. App User Guide - For Anyone Using the App")
st.caption("How to use Home, Dashboard, Metric Targets, Calculate, Publish pages. Includes best practices, queue date overrides, value overwriting.")

guide_path = app_root / "resources" / "trademark_wait_times_app_user_guide.docx"
if guide_path.exists():
    with open(guide_path, "rb") as f:
        st.download_button(
            "📥 Download User Guide (docx) - trademark_wait_times_app_user_guide.docx",
            f,
            file_name="trademark_wait_times_app_user_guide.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            type="primary"
        )
    st.info(f"File: {guide_path} - {guide_path.stat().st_size/1024:.1f} KB")
    with st.expander("Preview - User Guide Contents"):
        st.markdown("""
        - Overview: Silver monthly 5th, Gold self-service
        - Home: Source validation + environment, workflow sections removed
        - Dashboard: Read-only overview, no run parameters, no trigger job (per best practice)
        - Metric Targets: Table contains metric_name, section, unit (days/months) - unit editable dropdown
        - Calculate: logic (filing-only ITU, mean ITU, median postreg *0.71 business days, MADRID filing→ib, TEAS 9mo, divisional fallback 160)
        - Publish: ONLY page where dates can be added and values overwritten - editable averages, queue dates, data_updated banner, preview JSON, write S3/Volume, POST API Gateway
        - Best practices
        """)
else:
    st.warning(f"User guide not found at {guide_path} - run pip install python-docx and generate docs")

# Architecture doc
st.divider()
st.markdown("### 🏗️ 2. Architecture Doc - For Data Engineers")
st.caption("Architecture, process overview, how DBX app integrates as self-service tool, performance optimization v15 (55min→22-28min).")

arch_path = app_root / "resources" / "trademark_wait_times_architecture.docx"
if arch_path.exists():
    with open(arch_path, "rb") as f:
        st.download_button(
            "📥 Download Architecture Doc (docx) - trademark_wait_times_architecture.docx",
            f,
            file_name="trademark_wait_times_architecture.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            type="secondary",
            key="arch_help"
        )
    st.info(f"File: {arch_path} - {arch_path.stat().st_size/1024:.1f} KB")
    with st.expander("Preview - Architecture Contents"):
        st.markdown("""
        - Executive summary: Bronze 11 tables → Silver case_milestones 28 cols SCD2 14M → Gold 12 rows → Publish JSON → CMS
        - Medallion: Bronze raw CDC, Silver performance (metrics-wide pre-join 1 shuffle not 7, broadcast <2M, AQE, min 4 workers), Gold filing-only ITU, per-metric lookback
        - DAB: Bundle trm-dpl-wait-times, wf job monthly 5th 06:00, 5 tasks
        - Self-service App integration: silver scheduled, gold+publish via App, WorkspaceClient OAuth, WAIT_TIMES warehouse
        - Pages: Home (short intro + validation), Dashboard (read-only), Metric Targets (contains metric_name, section, unit editable), Calculate (quick preview + full job), Publish (editable dates/values), Help (this page)
        - Grants least-privilege, performance, known limitations (MADRID 74.7 vs live10 needs new column)
        """)
else:
    st.warning(f"Architecture doc not found at {arch_path}")

st.divider()
st.markdown("### 🔗 External References (for context, not primary help)")
st.markdown("""
- Official USPTO Processing Wait Times page: https://www.uspto.gov/trademarks/application-timeline - now secondary, primary help is internal docx per cleanup.
- Trademarks Dashboard: https://www.uspto.gov/dashboards/trademarks
""")

st.divider()
st.markdown("### 📧 Support")
st.markdown("""
- **Data Lake Team:** ODBDDataLakeTeam@uspto.gov
- **App Issues:** Use Report a bug in menu (mailto)
- **Workflow Owner:** joshua.strickland@USPTO.GOV
- **Environment:** Check Home page for dev/lab/prod badge
""")

st.caption("Get Help now serves internal docx in resources/, not external USPTO site per requirement.")

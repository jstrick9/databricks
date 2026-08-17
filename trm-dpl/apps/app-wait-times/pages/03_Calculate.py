import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date
from utils.page_config_helpers import setup_sidebar, set_page_config
from utils.runtime_env import get_runtime_env
from utils.db_helpers import get_connection, read_yaml, calculate_preview_via_sql, trigger_wait_times_job
from utils.user_helpers import init_user_session_state

set_page_config(page_title="Calculate | Wait Times")
setup_sidebar()
init_user_session_state()

dbx_env = get_runtime_env()
app_root = Path(__file__).resolve().parent.parent
config_path = app_root / "config" / dbx_env / "wait-times-conf.yaml"
configs = read_yaml(str(config_path))

st.title("⚙️ Calculate - Full run with custom dates")
st.divider()
st.markdown("### Run Parameters (for Quick Preview and Full Gold Job)")

col1, col2, col3 = st.columns(3)
with col1:
    lookback_years = st.number_input("Silver Lookback Years", 1, 20, 10, help="Silver builds case_milestones from 10 years of Bronze for 6yr/10yr post-reg coverage. Job YAML default 10.")
    lookback_months = st.number_input("Gold Lookback Months (base)", 6, 120, 18, help="Base lookback. Per-metric overrides: TEAS 9mo, MADRID 18mo filing→ib, Ext 24mo, Div 60mo. Adaptive if n < threshold.")
    min_sample = st.number_input("Min Sample Threshold", 10, 1000, 100, help="If n < threshold, Gold tries 24,60,120,None lookbacks. Divisional threshold 15.")
    snapshot = st.date_input("Snapshot Date", value=date.today(), help="Data updated as of date for banner")
with col2:
    exam_start = st.date_input("Exam Queue Start Override (blank=auto percentile 25)", value=None, help="We are currently examining new applications submitted between Mar 03-17 2026 – auto percentile 25/75 of pending")
    exam_end = st.date_input("Exam Queue End Override (blank=auto 75)", value=None)
    data_updated = st.date_input("Data Updated As Of", value=date.today(), help="Note: Data updated as of June 30, 2026 banner – gold processing_as_of_date")
with col3:
    sou_q = st.date_input("SOU Queue Override (blank=auto MAX pending)", value=None, help="Currently processing SOUs filed on or before Apr 14, 2026 – auto MAX sou_filing where sou_processed null")
    ren_q = st.date_input("Renewal Queue Override", value=None, help="Currently processing renewals filed on or before Apr 29, 2026")

st.session_state["calc_params"] = {
    "lookback_years": lookback_years,
    "lookback_months": lookback_months,
    "min_sample_threshold": min_sample,
    "snapshot_date": str(snapshot),
    "exam_start": str(exam_start) if exam_start else "",
    "exam_end": str(exam_end) if exam_end else "",
    "sou_queue": str(sou_q) if sou_q else "",
    "renewal_queue": str(ren_q) if ren_q else "",
    "data_updated": str(data_updated)
}

st.divider()
col_a, col_b = st.columns(2)
with col_a:
    if st.button("⚡ Quick Preview (SQL Warehouse)", type="primary", help="Direct SQL on silver.case_milestones – fast 10-20 sec, uses filing-only for ITU/lop/esu, mean ITU, median postreg *0.71"):
        conn, cursor = get_connection()
        with st.spinner("Calculating preview via SQL..."):
            df = calculate_preview_via_sql(cursor, configs, lookback_months=lookback_months)
            st.session_state["calc_preview"] = df
            st.success(f"Calculated {len(df)} metrics – uses lookback {lookback_months}mo base + per-metric overrides + business days factor")

with col_b:
    if st.button("🚀 Trigger Full Gold Job (with lookback_years + threshold)", help="Triggers wf_trademark_processing_wait_times with job_parameters lookback_years, lookback_months, min_sample_threshold, queue dates – job YAML now uses {{job_parameters.xxx}} so App overrides work"):
        with st.spinner("Triggering job via Jobs API..."):
            ok, msg = trigger_wait_times_job(configs, lookback_months=lookback_months, lookback_years=lookback_years, min_sample_threshold=min_sample, snapshot_date=snapshot, exam_start=exam_start, exam_end=exam_end, sou_queue=sou_q, renewal_queue=ren_q, data_updated=data_updated)
            if ok:
                st.success(msg)
                st.info("Check Jobs UI → wf_trademark_processing_wait_times – it will use your lookback_years=10 (silver 10yr for post-reg) + gold best accuracy. After completion, go to Dashboard (read-only) and Publish (editable dates/values – now defaults to Volumes).")
            else:
                st.error(msg)

if "calc_preview" in st.session_state:
    st.divider()
    st.markdown(f"#### Preview Results (lookback {lookback_months}mo base, threshold {min_sample}, snapshot {snapshot})")
    st.dataframe(st.session_state["calc_preview"], use_container_width=True, hide_index=True)
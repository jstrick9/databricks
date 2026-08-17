import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date
from utils.page_config_helpers import setup_sidebar, set_page_config
from utils.runtime_env import get_runtime_env
from utils.db_helpers import get_connection, read_yaml, get_processing_wait_times, get_metric_targets, get_exam_queue_window
from utils.user_helpers import init_user_session_state

set_page_config(page_title="Dashboard | Wait Times")
setup_sidebar()
init_user_session_state()

dbx_env = get_runtime_env()
app_root = Path(__file__).resolve().parent.parent
config_path = app_root / "config" / dbx_env / "wait-times-conf.yaml"
configs = read_yaml(str(config_path))

st.title("📊 Dashboard – Read-Only Overview")
st.caption("No run parameters – all date overrides and value editing now only in Publish page (per best practice). Use Calculate for on-demand runs.")

conn, cursor = get_connection()

# Silver summary already on Home, but show quick metrics here too
col_a, col_b = st.columns([1,1])
with col_a:
    try:
        cursor.execute(f"SELECT COUNT_IF(first_oa_date IS NULL) as pending, COUNT(*) as total FROM {configs['schema']['trgt_catalog']}.{configs['schema'].get('silver_schema','silver')}.case_milestones WHERE _is_current=true")
        pending, total = cursor.fetchone()
        st.metric("Pending First OA", f"{pending:,}", f"{total:,} total current")
    except Exception as e:
        st.caption(f"Pending query error: {e}")

with col_b:
    try:
        cursor.execute(f"SELECT MAX(snapshot_date) as max_snap, COUNT(DISTINCT snapshot_date) as cnt FROM {configs['schema']['trgt_catalog']}.{configs['schema'].get('gold_schema','gold')}.processing_wait_times")
        max_snap, cnt = cursor.fetchone()
        st.metric("Gold Snapshots", f"{cnt or 0}", f"Latest {max_snap}" if max_snap else "None")
    except Exception as e:
        st.caption(f"Gold count error: {e}")

st.divider()

# Latest Gold Snapshot (read-only)
st.markdown("#### Latest Published Gold Snapshot (read-only)")
latest_df = get_processing_wait_times(cursor, configs)
if not latest_df.empty:
    # Show with nice formatting
    display_df = latest_df[["metric_key","metric_name","section","unit","average_value","target_value","sample_size","snapshot_date","exam_queue_start_date","exam_queue_end_date"]].copy()
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Comparison vs live
    st.divider()
    st.markdown("#### Comparison vs Live USPTO & Targets")
    targets_df = get_metric_targets(cursor, configs)
    live = configs.get("live_uspto_snapshot", {})

    comparison = []
    for _, r in latest_df.iterrows():
        key = r["metric_key"]
        avg = r["average_value"]
        n = r["sample_size"]
        target_val = r["target_value"]
        live_val = live.get(key)
        unit = r["unit"]
        diff_live = abs(avg - live_val) if live_val is not None else None
        comparison.append({
            "metric_key": key,
            "section": r["section"],
            "average": avg,
            "target": target_val,
            "live_uspto": live_val,
            "diff_vs_live": round(diff_live,1) if diff_live is not None else None,
            "n": n,
            "unit": unit
        })
    comp_df = pd.DataFrame(comparison)
    st.dataframe(comp_df, use_container_width=True, hide_index=True)

    # Chart
    st.markdown("#### Average vs Target vs Live")
    chart_df = comp_df[["metric_key","average","target","live_uspto"]].set_index("metric_key")
    st.bar_chart(chart_df)

    # Exam queue window
    st.divider()
    q_row = latest_df.iloc[0]
    exam_start = q_row["exam_queue_start_date"]
    exam_end = q_row["exam_queue_end_date"]
    snapshot = q_row["snapshot_date"]
    st.markdown("#### Exam Queue Window (from Gold)")
    if exam_start and exam_end:
        st.info(f"We are currently examining new applications submitted between: **{exam_start} - {exam_end}** and Note: Data updated as of {q_row['processing_as_of_date']}.")
    else:
        st.caption("Exam queue window not computed – run Calculate → Full Job")

    # SOU / Renewal queue from silver
    try:
        catalog = configs["schema"]["trgt_catalog"]
        silver_schema = configs["schema"].get("silver_schema","silver")
        table = f"{catalog}.{silver_schema}.case_milestones"
        cursor.execute(f"SELECT MAX(sou_filing_date) as sou_max FROM {table} WHERE _is_current=true AND sou_filing_date IS NOT NULL AND sou_processed_date IS NULL")
        sou_max = cursor.fetchone()[0]
        cursor.execute(f"SELECT MAX(renewal_filing_date) as ren_max FROM {table} WHERE _is_current=true AND renewal_filing_date IS NOT NULL AND renewal_processed_date IS NULL")
        ren_max = cursor.fetchone()[0]
        col1, col2 = st.columns(2)
        with col1:
            if sou_max:
                st.caption(f"Currently processing SOUs filed on or before {sou_max} (pending)")
        with col2:
            if ren_max:
                st.caption(f"Currently processing renewals filed on or before {ren_max} (pending)")
    except Exception as e:
        st.caption(f"Queue dates error: {e}")

else:
    st.warning("No gold data yet – go to Calculate → Quick Preview or Full Job to generate first snapshot")
    st.info("After first run, this dashboard will show latest 12 metrics without any inputs.")

st.divider()
st.caption("All date overrides and value editing moved to Publish page per best practice. Dashboard is read-only overview.")

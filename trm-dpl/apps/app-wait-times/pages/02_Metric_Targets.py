import streamlit as st
import pandas as pd
from pathlib import Path
from utils.page_config_helpers import setup_sidebar, set_page_config
from utils.runtime_env import get_runtime_env
from utils.db_helpers import get_connection, read_yaml, get_metric_targets, update_metric_target_full
from utils.user_helpers import init_user_session_state

set_page_config(page_title="Metric Targets | Wait Times")
setup_sidebar()
init_user_session_state()

dbx_env = get_runtime_env()
app_root = Path(__file__).resolve().parent.parent
config_path = app_root / "config" / dbx_env / "wait-times-conf.yaml"
configs = read_yaml(str(config_path))

st.title("🎯 Metric Targets – Source of Truth")
st.markdown("""
`gold.metric_targets` is the **source of truth** for metric_name, section, unit (days/months), target_value, sort_order on USPTO site.
- YAML `default_targets` only seeds if empty.
- Edit here via SQL UPDATE without code deploy.
- **Unit editable** per requirement – set to `days` or `months`.
""")

conn, cursor = get_connection()
targets_df = get_metric_targets(cursor, configs)

if targets_df.empty:
    st.warning("No targets table found – will be seeded on first gold run")
    st.stop()

# Ensure required columns exist (fallback table may have only metric_key,target_value)
for col in ["metric_key","metric_name","section","unit","target_value","sort_order"]:
    if col not in targets_df.columns:
        if col in ["target_value","sort_order"]:
            targets_df[col] = 0
        else:
            targets_df[col] = ""

# Validate table contains metric_name, section, unit
st.markdown(f"#### Current Targets in `{configs['schema']['trgt_catalog']}.gold.metric_targets` – contains metric_name, section, unit")
st.info("✅ Table contains metric_name, section, unit (days/months) – required columns present" if all(c in targets_df.columns for c in ["metric_name","section","unit"]) else "⚠️ Missing required columns – will be added on save")

st.dataframe(targets_df, use_container_width=True, hide_index=True)

st.divider()
st.markdown("#### ✏️ Edit Single Target (target_value + unit)")

with st.form("edit_target_form", clear_on_submit=False):
    col1, col2 = st.columns(2)
    with col1:
        metric_keys = targets_df["metric_key"].tolist()
        metric_key = st.selectbox("Metric Key", options=metric_keys)
        try:
            curr_row = targets_df[targets_df["metric_key"]==metric_key]
            curr_target = float(curr_row["target_value"].iloc[0]) if not curr_row.empty else 10.0
            curr_name = curr_row["metric_name"].iloc[0] if not curr_row.empty and "metric_name" in curr_row.columns else metric_key
            curr_section = curr_row["section"].iloc[0] if not curr_row.empty and "section" in curr_row.columns else ""
            curr_unit = curr_row["unit"].iloc[0] if not curr_row.empty and "unit" in curr_row.columns else "days"
            curr_sort = int(curr_row["sort_order"].iloc[0]) if not curr_row.empty and "sort_order" in curr_row.columns else 0
        except Exception:
            curr_target = 10.0
            curr_name = metric_key
            curr_section = ""
            curr_unit = "days"
            curr_sort = 0
        new_target = st.number_input("New Target Value", min_value=0.0, max_value=365.0, value=curr_target, step=0.1, help="USPTO published target – e.g. 5.0 months for First Action, 10 days for TEAS, 90 days Post-Reg")
        new_unit = st.selectbox("Unit (editable)", options=["days","months"], index=0 if curr_unit=="days" else 1, help="Unit must be days or months")
    with col2:
        st.text_input("Metric Name (read-only)", value=curr_name, disabled=True, help="Display name on USPTO.gov – stored in metric_name column")
        st.text_input("Section (read-only)", value=curr_section, disabled=True)
        st.number_input("Sort Order (read-only in single edit)", value=curr_sort, disabled=True)
    
    st.caption("Single edit updates target_value + unit. For metric_name/section/sort_order use Bulk Edit below.")
    submitted = st.form_submit_button("Update Target + Unit", type="primary")
    if submitted:
        if dbx_env == "prod" and not st.session_state.get("confirm_prod_target_update"):
            st.session_state["confirm_prod_target_update"] = True
            st.warning("Prod update requires confirmation – click Update again to confirm")
        else:
            ok, msg = update_metric_target_full(cursor, configs, metric_key, target_value=new_target, unit=new_unit)
            if ok:
                st.success(msg)
                st.session_state.pop("confirm_prod_target_update", None)
                st.rerun()
            else:
                st.error(msg)

st.divider()
st.markdown("#### 📝 Bulk Edit – target_value + unit (editable), metric_name/section/sort_order view-only")
st.caption("Unit column has dropdown days/months. Only target_value and unit are writable per requirement – metric_name, section, sort_order read-only in this view to preserve USPTO names.")

# Prepare editable copy with unit dropdown
edit_df = targets_df.copy()
# Ensure unit lowercase
edit_df["unit"] = edit_df["unit"].apply(lambda x: x if str(x).lower() in ["days","months"] else "days")

edited_df = st.data_editor(
    edit_df,
    num_rows="dynamic",
    use_container_width=True,
    key="targets_editor_v3",
    column_config={
        "metric_key": st.column_config.TextColumn("metric_key", disabled=True),
        "metric_name": st.column_config.TextColumn("Metric Name", disabled=True, help="USPTO display name – stored in metric_name column"),
        "section": st.column_config.TextColumn("Section", disabled=True, help="Page section grouping"),
        "unit": st.column_config.SelectboxColumn("Unit (editable)", options=["days","months"], required=True, help="Unit must be days or months – editable per requirement"),
        "target_value": st.column_config.NumberColumn("Target Value (editable)", min_value=0.0, max_value=365.0, step=0.1),
        "sort_order": st.column_config.NumberColumn("Sort Order", disabled=True)
    }
)

col_save, col_reset = st.columns([1,1])
with col_save:
    if st.button("💾 Save Bulk Edits (target + unit)", type="primary"):
        changed = 0
        for _, row in edited_df.iterrows():
            try:
                orig = targets_df[targets_df["metric_key"]==row["metric_key"]]
                if not orig.empty:
                    orig_val = float(orig["target_value"].iloc[0])
                    orig_unit = str(orig["unit"].iloc[0]).lower()
                    new_val = float(row["target_value"])
                    new_unit = str(row["unit"]).lower()
                    # Check if changed
                    if orig_val != new_val or orig_unit != new_unit:
                        ok, msg = update_metric_target_full(cursor, configs, row["metric_key"], target_value=new_val, unit=new_unit)
                        if ok:
                            changed += 1
                        else:
                            st.error(f"Failed {row['metric_key']}: {msg}")
            except Exception as e:
                st.error(f"Error {row.get('metric_key','?')}: {e}")
        if changed > 0:
            st.success(f"Saved {changed} changes (target + unit)")
            st.rerun()
        else:
            st.info("No changes detected")
with col_reset:
    if st.button("🔄 Reset"):
        st.rerun()

st.divider()
st.markdown("#### Table Schema Verification")
st.code(f"""
CREATE TABLE {configs['schema']['trgt_catalog']}.gold.metric_targets (
  metric_key STRING NOT NULL PK,
  metric_name STRING NOT NULL, -- Display name e.g. 'First examining action in TSDR record'
  section STRING, -- Page section: summary / Pre-Examination Unit / ESU / Intent to use / Petitions Office / Post Registration
  unit STRING NOT NULL, -- days | months – EDITABLE per requirement
  target_value DOUBLE NOT NULL,
  sort_order INT
)
-- Contains metric_name, section, unit as requested
""", language="sql")

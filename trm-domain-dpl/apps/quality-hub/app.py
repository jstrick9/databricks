import sys
import os

# 1. BOOTSTRAP PATHS (No Streamlit commands yet)
APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import streamlit as st

# ================================================================
# 2. MANDATORY FIRST STREAMLIT COMMAND
# ================================================================
st.set_page_config(
    page_title="USPTO DQ Hub",
    page_icon="🛡️",
    layout="wide"
)

# ================================================================
# 3. IMPORTS & ENVIRONMENT
# ================================================================
import yaml
import pandas as pd
from datetime import datetime, timezone

# ✅ db_helpers is now the only DB layer (db_connection.py removed)
from db_helpers import (
    run_query,
    run_statement,
    get_connection,
    show_temp_message,
    read_yaml,
    validate_source_table,
)

from auth import initialize_user_session, can, get_scoped_filter, require_capability
from dq_config_store import DQConfigStore
from volume_catalog import list_tables_by_type
from yaml_rules import validate_yaml_text
from access_manager import render_access_manager
from onboard_logic import generate_default_checks, generate_default_hash_config

APP_ID = os.environ.get("APP_ID", "dq_hub")
DOMAIN_CATALOG = os.environ.get("DOMAIN_CATALOG", "trm_domain_dev")
DBX_ENV = os.environ.get("DBX_ENV", "dev")

# ✅ UC Volumes should always be: /Volumes/<catalog>/<schema>/<volume>/...
# Keep override via env var for flexibility.
DQ_CONFIGS_ROOT = os.environ.get(
    "DQ_CONFIGS_ROOT",
    f"/Volumes/{DOMAIN_CATALOG}/quality/audit_quality/dq_configs"
)

# Initialize persistent store
store = DQConfigStore(DQ_CONFIGS_ROOT)

# ------------------------------------------------
# Helper: replacement for get_table_schema() which was in db_connection.py
# ------------------------------------------------
def get_table_schema(full_table_name: str) -> pd.DataFrame:
    return run_query(f"DESCRIBE TABLE {full_table_name}")

# ================================================================
# 4. DATA LOADERS (Cached)
# ================================================================
@st.cache_data(ttl=300)
def fetch_health_scorecard():
    scope = get_scoped_filter()
    return run_query(
        f"SELECT * FROM {DOMAIN_CATALOG}.audit_quality.v_dq_health_scorecard WHERE {scope}"
    )

@st.cache_data(ttl=60)
def fetch_active_violations():
    scope = get_scoped_filter()
    return run_query(
        f"SELECT * FROM {DOMAIN_CATALOG}.audit_quality.v_active_violations WHERE {scope} LIMIT 1000"
    )

# ================================================================
# 5. PAGE RENDERERS
# ================================================================
def render_dashboard():
    st.title("📊 Data Quality Scorecard")
    df = fetch_health_scorecard()

    if df.empty:
        st.info("No data found within your assigned scope.")
        return

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        with st.container(border=True):
            st.metric("Tables Monitored", len(df))
    with m2:
        with st.container(border=True):
            avg_h = round(df["health_score_pct"].mean(), 1) if not df.empty else 0
            st.metric("Avg Health Score", f"{avg_h}%")
    with m3:
        with st.container(border=True):
            quarantined = len(df[df["last_run_status"] == "QUARANTINED"])
            st.metric("Tables Quarantined", quarantined)
    with m4:
        with st.container(border=True):
            total_rows = f"{df['last_total_rows'].sum():,}"
            st.metric("Total Rows Scanned", total_rows)

    st.divider()

    st.dataframe(
        df,
        column_order=("table_name", "health_grade", "health_score_pct", "last_run_status", "last_run_timestamp"),
        column_config={
            "table_name": "Source Table",
            "health_score_pct": st.column_config.ProgressColumn(
                "Health", min_value=0, max_value=100, format="%.1f%%"
            ),
            "last_run_timestamp": st.column_config.DatetimeColumn("Last Scanned"),
        },
        use_container_width=True,
        hide_index=True
    )

def render_violations():
    require_capability("view_violations")
    st.title("⚠️ Active Violations")
    df = fetch_active_violations()

    if df.empty:
        st.success("✅ No active violations found.")
        return

    col1, col2 = st.columns([3, 1])
    with col1:
        tbl_sel = st.multiselect("Filter by Table", options=df["table_name"].unique())
    with col2:
        st.write("")
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Export CSV", data=csv, file_name="violations.csv", use_container_width=True)

    filtered = df.copy()
    if tbl_sel:
        filtered = filtered[filtered["table_name"].isin(tbl_sel)]

    st.dataframe(filtered, use_container_width=True, hide_index=True)

def render_rule_builder():
    require_capability("edit_rules")
    st.title("🪄 Rule Builder")

    col_type, col_table = st.columns([1, 3])
    with col_type:
        config_type = st.radio("Config Type", ["Checks", "Hash Configs"])

    config_data = list_tables_by_type(DQ_CONFIGS_ROOT)
    available_tables = config_data.get(config_type, [])

    if not available_tables:
        st.warning(f"No {config_type} found in Volume. Initialize them in Onboarding first.")
        return

    options = [f"{t[0]}.{t[1]}.{t[2]}" for t in available_tables]

    with col_table:
        selected = st.selectbox(f"Select Table ({config_type})", options)

    if selected:
        cat, sch, tbl = selected.split(".")

        if config_type == "Checks":
            path = store.checks_path(cat, sch, tbl)
            load_func = store.load_checks
            save_func = store.save_checks
        else:
            path = store.hash_path(cat, sch, tbl)
            load_func = store.load_hash_config
            save_func = store.save_hash_config

        existing_val = ""
        if store.exists(path):
            existing_val = yaml.dump(load_func(cat, sch, tbl), sort_keys=False)

        with st.container(border=True):
            st.subheader(f"Editing {config_type}: {tbl}")
            new_yaml = st.text_area("YAML Definition", value=existing_val, height=500)

            if st.button(f"💾 Save {config_type} to Volume", type="primary", use_container_width=True):
                try:
                    if config_type == "Checks":
                        final_doc = validate_yaml_text(new_yaml)
                    else:
                        final_doc = yaml.safe_load(new_yaml)

                    save_func(cat, sch, tbl, final_doc)
                    st.toast(f"{config_type} saved successfully!", icon="✅")
                except Exception as e:
                    st.error(f"Error: {e}")

def render_onboarding():
    require_capability("onboard_tables")
    st.title("📥 Smart Table Onboarding")

    with st.container(border=True):
        st.write("Enter the Unity Catalog details to auto-generate DQ configurations.")
        with st.form("onboard_form", border=False):
            c1, c2, c3 = st.columns(3)
            with c1:
                target_catalog = st.text_input("Unity Catalog", value="trm_reporting_dev")
            with c2:
                target_schema = st.text_input("Schema", value="bronze")
            with c3:
                target_table = st.text_input("Table Name", value="attributes")

            submit = st.form_submit_button("Generate & Initialize", type="primary", use_container_width=True)

            if submit:
                try:
                    full_table_path = f"{target_catalog}.{target_schema}.{target_table}"
                    schema_df = get_table_schema(full_table_path)

                    if schema_df.empty:
                        st.error(f"Table not found: {full_table_path}. Ensure the SQL Warehouse can access this catalog.")
                        st.stop()

                    actual_columns = schema_df.iloc[:, 0].tolist()

                    checks_doc = generate_default_checks(actual_columns)
                    hash_doc = generate_default_hash_config(target_table, actual_columns)

                    logical_folder = target_catalog.replace("_dev", "").replace("_prod", "")

                    store.save_checks(logical_folder, target_schema, target_table, checks_doc)
                    store.save_hash_config(logical_folder, target_schema, target_table, hash_doc)

                    st.success(f"✅ Success! Generated configs for {full_table_path}")
                    st.info(f"Files saved to: `/Volumes/.../checks/{logical_folder}/{target_schema}/`")
                    st.balloons()
                    st.cache_data.clear()

                except Exception as e:
                    st.error(f"Onboarding Failed: {e}")

# ================================================================
# 6. MAIN ORCHESTRATION
# ================================================================
def main():
    initialize_user_session()
    profile = st.session_state.user_profile

    with st.sidebar:
        logo_path = os.path.join(APP_DIR, "resources", "images", "uspto_logo.png")
        if os.path.exists(logo_path):
            st.image(logo_path, width="stretch")

        st.write(f"👤 **{profile['display_name']}**")
        st.caption(f"Env: `{DBX_ENV.upper()}` | App: `{APP_ID.upper()}`")
        st.divider()

        menu = {}
        if can("view_dashboards"): menu["📊 Scorecard"] = render_dashboard
        if can("view_violations"): menu["⚠️ Violations"] = render_violations
        if can("edit_rules"):      menu["🪄 Rule Builder"] = render_rule_builder
        if can("onboard_tables"):  menu["📥 Onboarding"] = render_onboarding
        if can("manage_access"):   menu["🔐 Access Manager"] = render_access_manager

        choice = st.radio("Navigation", list(menu.keys()), label_visibility="collapsed")

        st.container(height=100, border=False)

        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    menu[choice]()

if __name__ == "__main__":
    main()
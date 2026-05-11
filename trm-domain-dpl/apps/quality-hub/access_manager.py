import streamlit as st
import pandas as pd
import os
from db_helpers import run_query, run_statement
from auth import require_capability

DOMAIN_CATALOG = os.environ.get("DOMAIN_CATALOG", "trm_domain")
REGISTRY_TABLE = f"{DOMAIN_CATALOG}.operations.app_permission_registry"

def render_access_manager():
    """
    Self-service UI for managing Multi-App permissions.
    Only users with 'manage_access' capability can see this.
    """
    # 1. Security Gate
    require_capability("manage_access")

    st.title("🔐 App Access Manager")
    st.markdown("""
    Use this panel to manage which **Databricks Groups** have access to specific **App Capabilities**.
    Changes take effect the next time a user logs in.
    """)

    # 2. Data Loading
    df = run_query(f"SELECT * FROM {REGISTRY_TABLE} ORDER BY app_id, group_name")
    
    if df.empty:
        st.warning("The registry is currently empty. Add your first mapping below.")
        existing_apps = ["dq_hub"]
    else:
        existing_apps = sorted(df['app_id'].unique().tolist())

    # 3. UI Tabs for Organization
    tab1, tab2, tab3 = st.tabs(["📋 Current Permissions", "➕ Add New Access", "⚙️ Manage Apps"])

    with tab1:
        st.subheader("Active Permission Mappings")
        
        # Filter UI
        col1, col2 = st.columns(2)
        with col1:
            filter_app = st.selectbox("Filter by App ID", ["All"] + existing_apps)
        
        view_df = df.copy()
        if filter_app != "All":
            view_df = view_df[view_df['app_id'] == filter_app]

        # Display Data
        st.dataframe(
            view_df, 
            use_container_width=True,
            column_config={
                "is_active": st.column_config.CheckboxColumn("Active?"),
                "updated_at": st.column_config.DatetimeColumn("Last Updated")
            },
            hide_index=True
        )

        # Deactivation Tool
        st.divider()
        st.subheader("🚫 Deactivate Access")
        col_a, col_b = st.columns([3, 1])
        with col_a:
            to_deactivate = st.selectbox(
                "Select a mapping to deactivate", 
                options=df.index,
                format_func=lambda x: f"{df.iloc[x]['app_id']} | {df.iloc[x]['group_name']} | {df.iloc[x]['capability']}"
            )
        with col_b:
            if st.button("Deactivate Now", type="primary"):
                target = df.iloc[to_deactivate]
                run_statement(f"""
                    UPDATE {REGISTRY_TABLE} 
                    SET is_active = false, updated_at = current_timestamp()
                    WHERE app_id = '{target['app_id']}' 
                      AND group_name = '{target['group_name']}' 
                      AND capability = '{target['capability']}'
                """)
                st.success("Access deactivated!")
                st.rerun()

    with tab2:
        st.subheader("Create New Mapping")
        with st.form("new_perm_form", clear_on_submit=True):
            col_app, col_group = st.columns(2)
            with col_app:
                app_id = st.text_input("App ID", placeholder="e.g., dq_hub, fixer_pro")
            with col_group:
                group_name = st.text_input("Databricks Group Name", placeholder="e.g., dq_stewards_trm")

            col_cap, col_scope = st.columns(2)
            with col_cap:
                capability = st.selectbox("Capability", [
                    "view_dashboards", "view_violations", "edit_rules", 
                    "apply_fixes", "onboard_tables", "admin_sync", "manage_access"
                ])
            with col_scope:
                data_scope = st.text_input("Data Scope (Catalogs)", value="*", help="Enter '*' for all, or comma-separated list like 'trm_reporting, trm_tmngpdb'")

            description = st.text_area("Purpose / Description")
            
            submit = st.form_submit_button("🚀 Save Mapping", type="primary", use_container_width=True)

            if submit:
                if not app_id or not group_name:
                    st.error("App ID and Group Name are required.")
                else:
                    # Get the email of the admin performing the action
                    current_user = st.session_state.user_profile['email']
                    
                    run_statement(f"""
                        INSERT INTO {REGISTRY_TABLE} 
                        (app_id, group_name, capability, data_scope, description, is_active, updated_at, updated_by)
                        VALUES 
                        ('{app_id}', '{group_name}', '{capability}', '{data_scope}', '{description}', true, current_timestamp(), '{current_user}')
                    """)
                    st.success(f"Success! {group_name} granted {capability} for {app_id}.")
                    st.rerun()

    with tab3:
        st.subheader("Platform Summary")
        app_stats = df.groupby('app_id').agg({
            'group_name': 'nunique',
            'capability': 'count'
        }).rename(columns={'group_name': 'Unique Groups', 'capability': 'Total Rules'})
        
        st.write("Overview of all apps managed by this registry:")
        st.table(app_stats)

        st.info("""
        **Developer Tip:**  
        To onboard a brand new app, simply give it a unique `APP_ID` in its `app.yaml` 
        and add the initial permissions here. No database migrations needed.
        """)
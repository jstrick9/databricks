"""
Multi-App Identity & Authorization Manager.
Resolves human identity via SSO headers and handles permission logic.
"""
import streamlit as st
import os
from databricks.sdk import WorkspaceClient

from db_helpers import run_query, build_in_list
from user_helpers import get_current_user as get_sso_user

APP_ID = os.environ.get("APP_ID", "dq_hub")
DOMAIN_CATALOG = os.environ.get("DOMAIN_CATALOG", "trm_domain_dev")

# ================================================================
# ADMIN OVERRIDE (Case-Insensitive)
# ================================================================
MOCK_ADMIN_EMAIL = os.environ.get("MOCK_ADMIN_EMAIL", "joshua.strickland@uspto.gov")


def initialize_user_session():
    """Builds a profile based on SSO Headers and SDK Group lookup."""
    if "user_profile" in st.session_state:
        return

    try:
        # 1) Get Real Human Email from SSO Headers
        sso_data = get_sso_user()
        if not sso_data:
            # If running locally/outside Apps, use dummy data
            user_email = MOCK_ADMIN_EMAIL.lower()
            display_name = f"{MOCK_ADMIN_EMAIL} (Local)"
        else:
            user_email = (sso_data.get("email") or "").lower().strip()
            display_name = sso_data.get("display_name") or user_email

        if not user_email:
            st.error("Auth Initialization Failed: missing user email.")
            st.stop()

        # 2) Get Databricks Groups for this specific user
        w = WorkspaceClient()
        user_groups = []
        try:
            search_results = list(w.users.list(filter=f"userName eq '{user_email}'"))
            if search_results:
                user_obj = search_results[0]
                if user_obj.groups:
                    for g in user_obj.groups:
                        name = getattr(g, "display", None) or getattr(g, "value", None)
                        if name:
                            user_groups.append(name)
        except Exception:
            pass

        allowed_capabilities = []
        data_scope = []

        # 3) Query the Permission Registry table
        registry_table = f"{DOMAIN_CATALOG}.operations.app_permission_registry"
        try:
            if user_groups:
                groups_sql = build_in_list(user_groups)
                query = f"""
                    SELECT capability, data_scope
                    FROM {registry_table}
                    WHERE app_id = '{APP_ID}'
                      AND is_active = true
                      AND group_name IN ({groups_sql})
                """
                perm_df = run_query(query)

                if not perm_df.empty:
                    allowed_capabilities = perm_df["capability"].dropna().unique().tolist()
                    raw_scopes = perm_df["data_scope"].dropna().unique().tolist()

                    for s in raw_scopes:
                        data_scope.extend([item.strip() for item in str(s).split(",") if item.strip()])

                    data_scope = list(set(data_scope))
        except Exception:
            pass

        # 4) MOCK ADMIN OVERRIDE
        if user_email == MOCK_ADMIN_EMAIL.lower():
            allowed_capabilities = [
                "view_dashboards", "view_violations", "edit_rules",
                "onboard_tables", "admin_sync", "manage_access", "apply_fixes"
            ]
            data_scope = ["*"]

        st.session_state.user_profile = {
            "app_id": APP_ID,
            "email": user_email,
            "display_name": display_name,
            "capabilities": allowed_capabilities,
            "data_scope": data_scope,
            "is_admin": (user_email == MOCK_ADMIN_EMAIL.lower() or "manage_access" in allowed_capabilities),
        }

    except Exception as e:
        st.error(f"Auth Initialization Failed: {str(e)}")
        st.stop()


def can(capability):
    """Boolean check for capability."""
    if "user_profile" not in st.session_state:
        initialize_user_session()
    return capability in st.session_state.user_profile["capabilities"]


def get_scoped_filter(column_name="catalog_name"):
    """SQL filter based on user scope."""
    if "user_profile" not in st.session_state:
        initialize_user_session()

    scope = st.session_state.user_profile["data_scope"]
    if "*" in scope or "all" in [s.lower() for s in scope]:
        return "1=1"
    if not scope:
        return "1=0"

    items_sql = build_in_list(scope)
    return f"{column_name} IN ({items_sql})"


def require_capability(capability):
    """Force stop if unauthorized."""
    if not can(capability):
        st.error(f"Access Denied: Missing '{capability}' capability.")
        st.stop()
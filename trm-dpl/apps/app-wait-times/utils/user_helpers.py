import streamlit as st
import os

def init_user_session_state():
    # In Databricks Apps, user info comes from headers via st.context or OIDC
    # Try to get real user, fallback to env var, then dev placeholder
    email = st.session_state.get("email")
    if not email:
        try:
            # Databricks Apps may expose user in context
            # Attempt to get from headers if available via st.context (newer Streamlit)
            user_email = None
            # Try various methods
            if hasattr(st, 'context') and hasattr(st.context, 'headers'):
                headers = st.context.headers
                user_email = headers.get('X-Forwarded-Email') or headers.get('X-Email') or headers.get('Email')
            if not user_email:
                user_email = os.getenv("DATABRICKS_USER_EMAIL") or os.getenv("USER_EMAIL")
            if not user_email:
                # Fallback placeholder – will be overwritten in prod by real header
                user_email = "Unknown"
            st.session_state["email"] = user_email
            st.session_state["display_name"] = user_email.split("@")[0]
        except Exception:
            st.session_state["email"] = "Unknown"
            st.session_state["display_name"] = "App User"
    return {
        "email": st.session_state.get("email", "unknown"),
        "display_name": st.session_state.get("display_name", "Unknown")
    }

def get_current_user():
    return init_user_session_state()

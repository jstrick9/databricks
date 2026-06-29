"""
Advanced Search Confirmation Dialog for TDET Application
"""

import io
import streamlit as st
import pandas as pd

from utils.db_helpers import get_connection
from utils.advanced_search_helpers import (
    run_advanced_search,
    save_advanced_search_history,
    PARAM_LABELS,
    parse_multi_value_input,
)


@st.dialog("Confirm Advanced Search", width="large")
def confirm_advanced_search_dialog(
    params: dict,
    operator: str,
    comments: str,  # Added comments parameter
    first_name: str,
    last_name: str,
    email: str,
    tdet_catalog: str,
    tdet_schema: str,
    table_configs: dict,
):
    """
    Confirmation dialog for advanced search.
    Shows search parameters and runs the query with live status.
    """
    st.write("Please confirm the search parameters below:")
    
    # Show active parameters
    active_params = []
    for param_name, value in params.items():
        if value and value.strip():
            label = PARAM_LABELS.get(param_name, param_name)
            values = parse_multi_value_input(value)
            active_params.append(f"- **{label}**: {', '.join(values)}")
    
    if active_params:
        st.markdown("\n".join(active_params))
        st.markdown(f"- **Operator**: {operator}")
    else:
        st.warning("No search parameters entered.")
        return
    
    st.markdown("---")
    st.markdown(f"- **Comments**: {comments}")
    st.markdown(f"- **Name**: {first_name} {last_name}")
    st.markdown(f"- **Email**: {email}")
    
    # Buttons row
    btn_run_col, btn_edit_col = st.columns(2)
    run_clicked = btn_run_col.button(
        "Confirm & Run ✅", 
        use_container_width=True, 
        key="adv_dlg_confirm_run"
    )
    edit_clicked = btn_edit_col.button(
        "Edit ✏️", 
        use_container_width=True, 
        key="adv_dlg_edit"
    )
    
    # Divider and status area
    st.divider()
    status_area = st.empty()
    
    if edit_clicked:
        st.rerun(scope="app")
    
    # Helper to lock/reset form
    def _lock_and_reset_form(status: str, err_msg: str | None = None):
        st.session_state["adv_form_locked"] = True
        st.session_state["adv_last_status"] = status
        st.session_state["adv_last_error"] = err_msg
    
    if run_clicked:
        with status_area.container():
            status = st.status("Starting advanced search…", expanded=True)
            
            try:
                status.write("Connecting to database…")
                conn, cursor = get_connection()
                if not cursor:
                    status.update(label="Database connection failed.", state="error")
                    _lock_and_reset_form(status="error", err_msg="Database connection failed.")
                    st.rerun(scope="app")
                    return
                
                status.update(label="Executing search query…", state="running")
                result_df, query, active_param_names = run_advanced_search(
                    cursor=cursor,
                    params=params,
                    operator=operator,
                    catalog=tdet_catalog,
                    schema="silver",  # Source table is in silver
                    table="tdet_app_search",
                    limit=10000,
                )
                
                if result_df.empty:
                    status.update(label="No results found.", state="complete")
                    st.session_state["adv_last_result"] = None
                    _lock_and_reset_form(status="complete", err_msg="No matching records found.")
                    st.rerun(scope="app")
                    return
                
                status.write(f"Found {len(result_df):,} records.")
                
                status.update(label="Saving to history…", state="running")
                user_name = f"{first_name} {last_name}"
                search_id, output_file_name = save_advanced_search_history(
                    cursor=cursor,
                    params=params,
                    operator=operator,
                    comments=comments,  # Pass comments
                    result_df=result_df,
                    user_name=user_name,
                    user_email=email,
                    catalog=tdet_catalog,
                    schema=tdet_schema,
                    table_configs=table_configs,
                )
                
                # Store results
                st.session_state["adv_last_result"] = {
                    "output_file_name": output_file_name,
                    "pdf": result_df,
                    "search_id": search_id,
                    "params": params,
                    "operator": operator,
                    "comments": comments,  # Store comments in result
                }
                
                _lock_and_reset_form(status="complete", err_msg=None)
                status.update(label="Complete", state="complete")
                st.rerun(scope="app")
            
            except Exception as e:
                status.update(label="Search failed.", state="error")
                _lock_and_reset_form(status="error", err_msg=str(e))
                st.rerun(scope="app")
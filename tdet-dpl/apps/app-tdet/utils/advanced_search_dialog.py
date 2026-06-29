"""
Advanced Search Confirmation Dialog for TDET Application
Runs processing inside dialog and shows status there.
"""

from typing import Optional
import streamlit as st
import time

from utils.db_helpers import get_connection
from utils.advanced_search_helpers import (
    run_advanced_search,
    save_advanced_search_history,
    PARAM_LABELS,
    parse_multi_value_input,
    get_active_params_summary_string
)


@st.dialog("Confirm Advanced Search", width="large")
def confirm_advanced_search_dialog(
    params: dict,
    match_types: dict,
    operator: str,
    comments: str,
    matter_number: str,
    first_name: str,
    last_name: str,
    email: str,
    tdet_catalog: str,
    tdet_schema: str,
    table_configs: dict,
    limit: Optional[int],
    selected_fields: dict,
    tmngpdb_catalog: str = None
):
    """
    Confirmation dialog for advanced search.
    Runs the processing exactly once and shows status inside the dialog.
    """
    st.write("Please confirm the search parameters below:")
    
    # Show active parameters
    active_params = []
    for param_name, value in params.items():
        if value and value.strip():
            label = PARAM_LABELS.get(param_name, param_name)
            values = parse_multi_value_input(value)
            
            # Show specific fields if subset selected
            fields = selected_fields.get(param_name, [])
            
            active_params.append(f"- **{label}**: {', '.join(values)}")
    
    if active_params:
        st.markdown("\n".join(active_params))
        st.markdown(f"- **Operator**: {operator}")
        if limit:
            st.markdown(f"- **Limit**: {limit:,} records")
    else:
        st.warning("No search parameters entered.")
        return
    
    st.markdown("---")
    st.markdown(f"- **Comments**: {comments}")
    st.markdown(f"- **Matter Number**: {matter_number}")
    st.markdown(f"- **Name**: {first_name} {last_name}")
    st.markdown(f"- **Email**: {email}")
    
    st.warning(f"**IMPORTANT Note: Do not leave the Search page while processing is running. You may close this dialog box with 'X' if you want to continue viewing the page.**")
    st.info(f"**After you click the 'Confirm & Run ✅' button, the process status will show up at the bottom of this dialog box. This dialog box will automatically close after processing has completed.**")
    
    st.divider()
    
    btn_run_col, btn_edit_col = st.columns(2)
    run_clicked = btn_run_col.button("Confirm & Run ✅", use_container_width=True, key="adv_dlg_confirm_run")
    edit_clicked = btn_edit_col.button("Edit ✏️", use_container_width=True, key="adv_dlg_edit")

    # Reserve status area
    st.divider()
    status_area = st.empty()

    if edit_clicked:
        st.rerun(scope="app")

    # Helper to lock/reset form
    def _lock_and_reset_form(status: str, err_msg: str | None = None):
        st.session_state["adv_form_locked"] = True
        st.session_state["adv_last_status"] = status
        st.session_state["adv_last_error"] = err_msg
        # Ensure queue flags are OFF so main page doesn't try to run it too
        st.session_state["adv_run_confirmed"] = False
        st.session_state["adv_run_payload"] = None

    if run_clicked:
        # Run processing inside the dialog
        with status_area.container():
            status = st.status("Starting advanced search…", expanded=True)
            
            # Create Progress UI Elements inside status box
            progress_bar = status.empty()
            details_text = status.empty()
            
            # Define Callback for save_advanced_search_history
            def update_adv_progress(fraction, total, message):
                try:
                    # Clamp fraction between 0.0 and 1.0
                    frac = min(max(fraction, 0.0), 1.0)
                    progress_bar.progress(frac, text=f"{int(frac*100)}%")
                    details_text.markdown(message)
                except Exception:
                    pass

            try:
                status.write("Connecting to database…")
                conn, cursor = get_connection()
                if not cursor:
                    status.update(label="Database connection failed.", state="error")
                    _lock_and_reset_form(status="error", err_msg="Database connection failed.")
                    st.rerun(scope="app")
                    return
                
                status.update(label="Executing search query…", state="running")
                progress_bar.progress(0.05, text="Querying...")
                
                result_df, query, active_param_names = run_advanced_search(
                    cursor=cursor,
                    params=params,
                    operator=operator,
                    catalog=tdet_catalog,
                    schema="silver",
                    table="tdet_app_search",
                    limit=limit,
                    match_types=match_types,
                    user_email=email,
                    selected_fields=selected_fields,
                    tmngpdb_catalog=tmngpdb_catalog
                )
                
                if result_df.empty:
                    status.update(label="No results found.", state="complete")
                    st.session_state["adv_last_result"] = None
                    _lock_and_reset_form(status="complete", err_msg="No matching records found.")
                    st.rerun(scope="app")
                    return
                
                count_msg = f"Found {len(result_df):,} records."
                status.write(count_msg)
                details_text.markdown(count_msg)
                
                status.update(label="Saving to history…", state="running")
                
                user_name = f"{first_name} {last_name}"
                
                # FIXED: Added tmngpdb_catalog to the function call
                search_id, output_file_name = save_advanced_search_history(
                    cursor=cursor,
                    params=params,
                    operator=operator,
                    comments=comments,
                    matter_number=matter_number,
                    result_df=result_df,
                    user_name=user_name,
                    user_email=email,
                    catalog=tdet_catalog,
                    schema=tdet_schema,
                    table_configs=table_configs,
                    match_types=match_types,
                    selected_fields=selected_fields,
                    progress_callback=update_adv_progress, # Pass callback
                    tmngpdb_catalog=tmngpdb_catalog # <--- CRITICAL FIX HERE
                )

                # Build the summary string to persist exactly what was run
                criteria_summary = get_active_params_summary_string(
                    params, operator, limit, match_types, selected_fields
                )
                
                # Store results
                st.session_state["adv_last_result"] = {
                    "output_file_name": output_file_name,
                    "pdf": result_df,
                    "search_id": search_id,
                    "params": params,
                    "operator": operator,
                    "comments": comments,
                    "matter_number": matter_number,
                    "limit": limit,
                    "match_types": match_types,
                    "selected_fields": selected_fields,
                    "criteria_summary": criteria_summary,
                    "tdet_catalog": tdet_catalog,
                    "tdet_schema": tdet_schema,
                }
                
                progress_bar.progress(1.0, text="100%")
                _lock_and_reset_form(status="complete", err_msg=None)
                status.update(label="Complete", state="complete")
                time.sleep(1) # Brief pause so user sees completion
                st.rerun(scope="app")
            
            except Exception as e:
                status.update(label="Search failed.", state="error")
                _lock_and_reset_form(status="error", err_msg=str(e))
import os
import io
import time
import json
from pathlib import Path
import numpy as np
import streamlit as st
import pandas as pd

from utils.db_helpers import (
    get_connection,
    show_temp_message,
    read_yaml,
    validate_source_table,
)
from utils.search_helpers import (
    process_upload,
    save_results_sql,
    normalize_and_validate_email,
    normalize_input_to_name_case,
    confirm_and_run_dialog,
    export_results,
    generate_excel_buffer,
    render_download_button,
)
from utils.advanced_search_helpers import (
    render_advanced_search_form,
    render_active_params_summary,
    validate_search_params,
    run_advanced_search,
    save_advanced_search_history,
    PARAM_LABELS,
    parse_multi_value_input,
    build_advanced_search_comments,
    PARAM_FIELD_OPTIONS,
    get_active_params_summary_string,
)
from utils.advanced_search_dialog import confirm_advanced_search_dialog
from utils.page_config_helpers import setup_sidebar, set_page_config, vertical_divider
from utils.runtime_env import get_runtime_env
from utils.user_helpers import init_user_session_state
from utils.saved_search_helpers import save_new_preset, get_user_presets, delete_preset


# =============================================================================
# DIALOG: Save Search Preset
# =============================================================================
@st.dialog("Save Search Preset")
def save_search_preset_dialog(search_type, config_payload, tdet_catalog, tdet_schema):
    """
    Dialog to save the current search criteria as a preset.
    Args:
        search_type: "ADVANCED" or "HYBRID"
        config_payload: Dict containing ONLY params, match_types, etc. (NO DataFrame)
        tdet_catalog: Database catalog
        tdet_schema: Database schema
    """
    name = st.text_input("Name your search")

    if st.button("Save"):
        user_email_save = st.session_state.get("email")

        if not tdet_catalog or not tdet_schema:
            st.error("Configuration missing. Cannot save.")
            return

        ok, msg = save_new_preset(
            name,
            search_type,
            config_payload,
            user_email_save,
            tdet_catalog,
            tdet_schema,
        )

        if ok:
            st.success(msg)
            # Clear the dialog trigger
            st.session_state.pop("show_save_dialog", None)
            time.sleep(1)
            st.rerun()
        else:
            st.error(msg)


# =============================================================================
# STATE MANAGEMENT HELPERS
# =============================================================================
def _reset_for_new_search():
    """Reset search-related state while preserving user identity (first/last/email)."""
    # -------- Basic search state --------
    st.session_state["form_locked"] = False
    st.session_state["last_status"] = None
    st.session_state["last_error"] = None
    st.session_state["last_result"] = None

    # Clear uploaded file and force a fresh uploader
    st.session_state["uploaded_bytes"] = None
    st.session_state["uploaded_name"] = None
    st.session_state["basic_run_confirmed"] = False
    st.session_state["basic_run_payload"] = None
    st.session_state["uploader_key"] = (st.session_state.get("uploader_key") or 0) + 1

    # Reset Matter Number
    if "matter_number" in st.session_state:
        del st.session_state["matter_number"]
    if "adv_matter_number" in st.session_state:
        del st.session_state["adv_matter_number"]

    # Reset Basic Search comments prefix
    st.session_state["comments_key"] = "Basic Search - "

    # -------- Advanced search state --------
    st.session_state["adv_form_locked"] = False
    st.session_state["adv_last_status"] = None
    st.session_state["adv_last_error"] = None
    st.session_state["adv_last_result"] = None
    st.session_state["adv_run_confirmed"] = False
    st.session_state["adv_run_payload"] = None

    # Reset Advanced Search comments prefix/state
    st.session_state["adv_comments_display"] = "Advanced Search - "
    st.session_state["adv_comments_auto"] = "Advanced Search - "
    st.session_state["adv_comments_user_edited"] = False

    # Clear advanced search parameter fields
    adv_keys_to_reset = [
        # Input Fields
        "adv_name", "adv_search_email", "adv_phone", "adv_address",
        "adv_url", "adv_status", "adv_atty_num", "adv_ph_entry",
        # Field Selectors (Multiselects/Checkboxes)
        "adv_fields_name", "adv_fields_email", "adv_fields_phone", "adv_fields_address",
        # Match Logic Selectors
        "adv_match_name", "adv_match_email", "adv_match_phone", "adv_match_address",
        "adv_match_url", "adv_match_status", "adv_match_atty_num", "adv_match_ph_entry",
        # Global Logic
        "adv_operator", "adv_limit",
    ]
    for key in adv_keys_to_reset:
        if key in st.session_state:
            del st.session_state[key]

    # Clear save dialog trigger
    st.session_state.pop("show_save_dialog", None)


def _lock_and_reset_basic_form(status: str, err_msg: str | None = None):
    """Lock the basic search form after completion/error."""
    st.session_state["form_locked"] = True
    st.session_state["last_status"] = status
    st.session_state["last_error"] = err_msg
    st.session_state["uploaded_bytes"] = None
    st.session_state["uploaded_name"] = None
    st.session_state["uploader_key"] = (st.session_state.get("uploader_key") or 0) + 1
    st.session_state["basic_run_confirmed"] = False
    st.session_state["basic_run_payload"] = None


def _lock_and_reset_adv_form(status: str, err_msg: str | None = None):
    """Lock the advanced search form after completion/error."""
    st.session_state["adv_form_locked"] = True
    st.session_state["adv_last_status"] = status
    st.session_state["adv_last_error"] = err_msg
    st.session_state["adv_run_confirmed"] = False
    st.session_state["adv_run_payload"] = None


# =============================================================================
# SEARCH PAGE
# =============================================================================
def show_search():
    set_page_config(page_title="Search | Trademark Data Extraction Tool (TDET)")
    setup_sidebar()

    # ========== State Initialization ==========
    # Basic search
    st.session_state.setdefault("uploaded_bytes", None)
    st.session_state.setdefault("uploaded_name", None)
    st.session_state.setdefault("last_result", None)
    st.session_state.setdefault("last_error", None)
    st.session_state.setdefault("last_status", None)
    st.session_state.setdefault("form_locked", False)
    st.session_state.setdefault("uploader_key", 0)
    st.session_state.setdefault("basic_run_confirmed", False)
    st.session_state.setdefault("basic_run_payload", None)
    st.session_state.setdefault("comments_key", "Basic Search - ")
    st.session_state.setdefault("matter_number_key", None)

    # Advanced search
    st.session_state.setdefault("adv_form_locked", False)
    st.session_state.setdefault("adv_last_status", None)
    st.session_state.setdefault("adv_last_error", None)
    st.session_state.setdefault("adv_last_result", None)
    st.session_state.setdefault("adv_run_confirmed", False)
    st.session_state.setdefault("adv_run_payload", None)
    st.session_state.setdefault("adv_comments_display", "Advanced Search - ")
    st.session_state.setdefault("adv_comments_auto", "Advanced Search - ")
    st.session_state.setdefault("adv_comments_user_edited", False)
    st.session_state.setdefault("adv_matter_number_key", None)

    # Search mode — single source of truth via the widget key
    st.session_state.setdefault("search_mode_radio", "Basic (File Upload)")
    st.session_state.setdefault("show_save_dialog", None)

    # ========== SSO User login ==========
    sso_user = init_user_session_state()

    # ========== Environment & Config ==========
    dbx_env = get_runtime_env()
    st.session_state["dbx_env"] = dbx_env

    app_root = Path(__file__).resolve().parent.parent
    config_file = app_root / "config" / dbx_env / "tdet-conf.yaml"

    try:
        configs = read_yaml(str(config_file))
        tdet_catalog = configs["schema"]["trgt_catalog"]
        tdet_schema = configs["schema"].get("trgt_schema", "gold")
        
        # Read the Source Catalog directly from YAML based on your configuration structure
        tmngpdb_catalog = configs["schema"].get("tmngpdb_catalog")
        
        table_configs = configs["schema"].get("tables", {})
    except FileNotFoundError:
        st.error(f"Configuration file not found for environment: {dbx_env}")
        st.error("Please contact support at ODBDDataLakeTeam@uspto.gov.")
        st.stop()
    except KeyError as e:
        st.error(f"Invalid configuration: missing required key {e}")
        st.error("Please contact support at ODBDDataLakeTeam@uspto.gov.")
        st.stop()
    except Exception as e:
        st.error("Configuration error occurred.")
        st.error("Please contact support at ODBDDataLakeTeam@uspto.gov.")
        st.stop()

    # ========== Page Header ==========
    st.title("Search")

    # ========== DB Connection (for validation only) ==========
    conn, cursor = get_connection()
    if not cursor:
        st.stop()

    # ========== SAVED SEARCHES SECTION ==========
    st.markdown("### <u>Saved Searches (Load / Manage)</u>", unsafe_allow_html=True)
    with st.expander("Click to View Saved Searches", expanded=False):
        email_clean = st.session_state.get("email") or "unknown"

        # --- Filters & Limit UI ---
        col_f1, col_f2, col_f3 = st.columns([2, 2, 1])
        with col_f1:
            filter_name = st.text_input("Filter by Search Name", key="saved_filter_name")
        with col_f2:
            filter_type = st.selectbox(
                "Filter by Type",
                ["All", "ADVANCED", "HYBRID"],
                key="saved_filter_type",
            )
        with col_f3:
            limit_opts = [5, 10, 15, 20]
            limit_val = st.selectbox("Limit", limit_opts, index=0, key="saved_limit")

        # Build filter dict
        preset_filters = {}
        if filter_name:
            preset_filters["search_name"] = filter_name
        if filter_type != "All":
            preset_filters["search_type"] = filter_type

        # Fetch Data
        presets_df = get_user_presets(
            email_clean,
            tdet_catalog,
            tdet_schema,
            filters=preset_filters,
            limit=limit_val,
        )

        if presets_df.empty:
            st.info("No saved searches found matching criteria.")
        else:
            # --- Selection ---
            col_sel, col_act = st.columns([3, 1])
            with col_sel:
                selected_preset_id = st.selectbox(
                    "Select a saved search:",
                    options=presets_df["id"].tolist(),
                    format_func=lambda x: (
                        f"{presets_df[presets_df['id']==x].iloc[0]['search_name']} "
                        f"({presets_df[presets_df['id']==x].iloc[0]['search_type_code']})"
                    ),
                )

            # --- Get Selected Row ---
            selected_row = presets_df[presets_df["id"] == selected_preset_id].iloc[0]
            selected_config = json.loads(selected_row["config_json"])

            # --- Action Buttons ---
            with col_act:
                col_load, col_del = st.columns(2)

                if col_load.button("Load", use_container_width=True):
                    row_preset = presets_df[presets_df["id"] == selected_preset_id].iloc[0]
                    config = json.loads(row_preset["config_json"])
                    sType = row_preset["search_type_code"]

                    if sType == "ADVANCED":
                        # Set the WIDGET KEY directly — single source of truth
                        st.session_state["search_mode_radio"] = "Advanced (Parameter Search)"

                        params = config.get("params", {})
                        m_types = config.get("match_types", {})
                        s_fields = config.get("selected_fields", {})

                        # Populate session state
                        for k, v in params.items():
                            st.session_state[f"adv_{k}"] = v
                        for k, v in m_types.items():
                            st.session_state[f"adv_match_{k}"] = v

                        # Convert Column Names back to Labels for Multiselects
                        for param_key, cols in s_fields.items():
                            if param_key in PARAM_FIELD_OPTIONS:
                                col_to_label = {
                                    v: k
                                    for k, v in PARAM_FIELD_OPTIONS[param_key].items()
                                }
                                labels = [
                                    col_to_label.get(c)
                                    for c in cols
                                    if c in col_to_label
                                ]
                                st.session_state[f"adv_fields_{param_key}"] = labels

                        st.session_state["adv_operator"] = config.get("operator", "OR")
                        st.session_state["adv_comments_user_edited"] = False
                        st.success(f"Loaded '{row_preset['search_name']}'")
                        time.sleep(0.5)
                        st.rerun()

                    elif sType == "HYBRID":
                        # Set the WIDGET KEY directly — single source of truth
                        st.session_state["search_mode_radio"] = "Basic (File Upload)"
                        st.session_state["use_adv_in_basic"] = True

                        params = config.get("params", {})
                        m_types = config.get("match_types", {})
                        s_fields = config.get("selected_fields", {})

                        for k, v in params.items():
                            st.session_state[f"adv_{k}"] = v
                        for k, v in m_types.items():
                            st.session_state[f"adv_match_{k}"] = v

                        # Convert Column Names back to Labels for Multiselects
                        for param_key, cols in s_fields.items():
                            if param_key in PARAM_FIELD_OPTIONS:
                                col_to_label = {
                                    v: k
                                    for k, v in PARAM_FIELD_OPTIONS[param_key].items()
                                }
                                labels = [
                                    col_to_label.get(c)
                                    for c in cols
                                    if c in col_to_label
                                ]
                                st.session_state[f"adv_fields_{param_key}"] = labels

                        st.success(
                            f"Loaded Hybrid filters for '{row_preset['search_name']}'. "
                            f"Please upload a file."
                        )
                        time.sleep(0.5)
                        st.rerun()

                if col_del.button("Delete", use_container_width=True):
                    delete_preset(selected_preset_id, tdet_catalog, tdet_schema)
                    st.rerun()

            # --- DETAILED VIEW OF SELECTED SEARCH ---
            st.markdown("#### Selected Search Details")
            with st.container(border=True):
                col_d1, col_d2 = st.columns(2)
                with col_d1:
                    st.caption(f"**Name:** {selected_row['search_name']}")
                    st.caption(f"**Type:** {selected_row['search_type_code']}")
                with col_d2:
                    st.caption(f"**Created:** {selected_row['_created_timestamp']}")
                    st.caption(f"**User:** {email_clean}")

                st.markdown("---")
                st.caption("**Configuration Summary:**")

                # Render human-readable summary of JSON config
                if selected_row["search_type_code"] in ["ADVANCED", "HYBRID"]:
                    summary_str = get_active_params_summary_string(
                        selected_config.get("params", {}),
                        selected_config.get("operator", "OR"),
                        selected_config.get("limit"),
                        selected_config.get("match_types", {}),
                        selected_config.get("selected_fields", {}),
                    )
                    st.info(summary_str)
                else:
                    st.text(selected_row["config_json"])

    # ========== Search Mode Selection ==========
    st.divider()
    st.markdown("### <u>Select Search Mode</u>", unsafe_allow_html=True)

    search_mode = st.radio(
        "Choose a search method:",
        options=["Basic (File Upload)", "Advanced (Parameter Search)"],
        horizontal=True,
        key="search_mode_radio",
        # No index= parameter — let the key drive the selection
    )

    # Derive logical mode from widget value (single source of truth)
    is_basic_mode = "Basic" in st.session_state.get("search_mode_radio", "Basic")

    # ========== BASIC SEARCH MODE ==========
    st.divider()
    if is_basic_mode:
        st.markdown(
            "### <u>Basic Search (File Upload)</u>", unsafe_allow_html=True
        )
        st.markdown(
            "Upload an Excel file (.xlsx) with a valid serial number column "
            "in the first column."
        )
        st.markdown(
            "Click on Download Sample Excel File Template (on left sidebar) to "
            "download a sample file to see the expected format and use for your "
            "file upload."
        )
        st.caption(
            "Accepted column headers: **serial_number**, **serial_num**, "
            "**ser_num**, **ser_number**, or **sn**"
        )

        if st.session_state["form_locked"]:
            # Show locked state
            if st.session_state.get("last_status") == "complete":
                pass  # Success — results rendered below
            elif st.session_state.get("last_status") == "error":
                st.error("Your search failed.")
                if st.session_state.get("last_error"):
                    st.caption(st.session_state["last_error"])
                st.markdown("---")
                st.markdown("### Click to Run New Search")
                if st.button("New Search", key="basic_new_search_error"):
                    _reset_for_new_search()
                    st.rerun()
        else:
            # Show form
            uploaded_file = st.file_uploader(
                "**Upload Serial Number Excel (.xlsx)***",
                type=["xlsx"],
                key=f"uploaded_xlsx_{st.session_state['uploader_key']}",
            )
            if uploaded_file is not None:
                st.session_state["uploaded_bytes"] = uploaded_file.getvalue()
                st.session_state["uploaded_name"] = uploaded_file.name

            # Ensure comments_key has default value if empty or not set
            current_comments = st.session_state.get("comments_key", "")
            if not current_comments or current_comments.strip() == "":
                st.session_state["comments_key"] = "Basic Search - "

            # Matter Number
            matter_number = st.text_input(
                "**Matter Number (max 250 chars)***",
                key="matter_number_key",
                placeholder="Enter internal matter number",
            )
            st.session_state["matter_number"] = matter_number

            # --- HYBRID SEARCH TOGGLE ---
            st.divider()
            st.markdown("### <u>Hybrid Search</u>", unsafe_allow_html=True)
            use_adv_filters = st.checkbox(
                "Apply Advanced Search Criteria to File?",
                key="use_adv_in_basic",
                help="Filter the uploaded serial numbers by Name, Email, Status, etc.",
            )

            adv_filter_data = None
            auto_comments_suffix = ""

            if use_adv_filters:
                st.info(
                    "The search will return records from your file ONLY if they "
                    "ALSO match the criteria below."
                )
                params, match_types, selected_fields, operator, limit = (
                    render_advanced_search_form()
                )
                render_active_params_summary(
                    params, operator, limit, match_types, selected_fields
                )

                adv_filter_data = {
                    "params": params,
                    "match_types": match_types,
                    "selected_fields": selected_fields,
                    "operator": operator,
                    "limit": limit,
                }

                # Build suffix
                auto_comments_suffix = build_advanced_search_comments(
                    params, operator, limit, match_types, selected_fields
                ).replace("Advanced Search - ", "")

            st.markdown("---")

            # --- DYNAMIC COMMENTS LOGIC ---
            current_prefix = (
                "Hybrid Search - " if use_adv_filters else "Basic Search - "
            )
            full_auto_comment = (
                current_prefix + auto_comments_suffix
                if use_adv_filters
                else current_prefix
            )

            # Initialize tracking state if missing
            if "last_auto_comment" not in st.session_state:
                st.session_state["last_auto_comment"] = ""
            if "comments_user_edited" not in st.session_state:
                st.session_state["comments_user_edited"] = False

            # If the auto-generated string changed
            if full_auto_comment != st.session_state["last_auto_comment"]:
                current_box_val = st.session_state.get("comments_key", "")
                if (
                    not st.session_state["comments_user_edited"]
                    or current_box_val == st.session_state["last_auto_comment"]
                ):
                    st.session_state["comments_key"] = full_auto_comment
                st.session_state["last_auto_comment"] = full_auto_comment

            # Callback to detect manual edits
            def on_basic_comments_change():
                if (
                    st.session_state.comments_key
                    != st.session_state.last_auto_comment
                ):
                    st.session_state["comments_user_edited"] = True

            # Ensure session state has a value
            if st.session_state.get("comments_key") is None:
                st.session_state["comments_key"] = "Basic Search - "

            st.markdown(
                "#### <u>Your Information</u>", unsafe_allow_html=True
            )

            comments = st.text_area(
                "**Comments (max 1000 chars) — Pre-populated with search type***",
                max_chars=1000,
                key="comments_key",
                on_change=on_basic_comments_change,
                height=150,
            )

            col1, col2 = st.columns([1, 2])
            with col1:
                st.text_input(
                    "**Your First Name***",
                    key="first_name",
                    on_change=normalize_input_to_name_case,
                    args=("first_name",),
                    kwargs={"smart": True},
                )
            with col2:
                st.text_input(
                    "**Your Last Name***",
                    key="last_name",
                    on_change=normalize_input_to_name_case,
                    args=("last_name",),
                    kwargs={"smart": True},
                )

            first_name = st.session_state.get("first_name", "") or ""
            last_name = st.session_state.get("last_name", "") or ""

            st.text_input(
                "**Your Email***",
                key="email",
                on_change=normalize_and_validate_email,
            )
            email = st.session_state.get("email", "") or ""
            if email:
                st.caption(
                    "✅ Email address is valid."
                    if st.session_state.get("email_valid")
                    else "❌ Invalid email format."
                )

            st.markdown(
                "**Comments, first name, last name, and email are required and "
                "pre-populate automatically.  Matter number is required.**"
            )
            st.warning(
                "**IMPORTANT Note: Do not leave the Search page while the file "
                "is processing.**"
            )

            # Validate all required fields
            is_params_valid = True  # Default to True if not using filters
            if use_adv_filters:
                is_params_valid = validate_search_params(params)[0]
            is_file_uploaded = uploaded_file is not None
            is_first_name_valid = bool(first_name and first_name.strip())
            is_last_name_valid = bool(last_name and last_name.strip())
            is_email_valid = bool(
                email
                and email.strip()
                and st.session_state.get("email_valid", False)
            )
            is_matter_number_valid = bool(
                matter_number and matter_number.strip()
            )

            all_required_filled = (
                is_file_uploaded
                and is_first_name_valid
                and is_last_name_valid
                and is_email_valid
                and is_matter_number_valid
            )

            # Must have all standard fields AND (if filters, params valid)
            run_enabled = all_required_filled and is_params_valid

            # Show what's missing
            if not run_enabled:
                missing_fields = []
                if not is_file_uploaded:
                    missing_fields.append("Upload Excel file")
                if not is_first_name_valid:
                    missing_fields.append("First Name")
                if not is_last_name_valid:
                    missing_fields.append("Last Name")
                if not is_email_valid:
                    if not email:
                        missing_fields.append("Email")
                    else:
                        missing_fields.append("Valid Email")
                if not is_matter_number_valid:
                    missing_fields.append("Matter Number")
                if use_adv_filters and not is_params_valid:
                    missing_fields.append("At least one search parameter")

                st.warning(
                    f"Please complete the following required fields to enable "
                    f"search: **{', '.join(missing_fields)}**"
                )

            if run_enabled:
                search_type_code = (
                        "HYBRID" if use_adv_filters else "BASIC"
                    )
                if st.button(
                    f"Run {search_type_code} Search", key="run_search", type="primary"
                ):
                    final_comments = comments
                    search_type_code = search_type_code
                    
                    st.session_state["basic_run_confirmed"] = True
                    st.session_state["basic_run_payload"] = {
                        "comments": final_comments,
                        "matter_number": matter_number,
                        "first_name": first_name,
                        "last_name": last_name,
                        "email": email,
                        "tdet_catalog": tdet_catalog,
                        "tdet_schema": tdet_schema,
                        "table_configs": table_configs,
                        "adv_filters": adv_filter_data,
                        "search_type_code": search_type_code,
                        "tmngpdb_catalog": tmngpdb_catalog
                    }

                    confirm_and_run_dialog(
                        final_comments,
                        matter_number,
                        first_name,
                        last_name,
                        email,
                        tdet_catalog,
                        tdet_schema,
                        table_configs,
                        adv_filters=adv_filter_data,
                        search_type_code=search_type_code,
                        tmngpdb_catalog=tmngpdb_catalog 
                    )
                    st.stop()

        # ========== Basic Search Results Rendering ==========
        res = st.session_state.get("last_result")
        if res:
            output_file_name = res["output_file_name"]
            pdf = res["pdf"]

            dt_cols = pdf.select_dtypes(
                include=["datetime64[ns, UTC]", "datetime64[ns]"]
            ).columns
            for col in dt_cols:
                pdf[col] = pd.to_datetime(pdf[col]).dt.tz_localize(None)

            # --- CACHING LOGIC ---
            if "excel_bytes" not in res:
                with st.spinner("Preparing download file..."):
                    res["excel_bytes"] = generate_excel_buffer(pdf)

            final_data = res["excel_bytes"]

            col_dl, col_div, col_new = st.columns([1, 0.06, 1])

            with col_dl:
                render_download_button(
                    final_data,
                    output_file_name,
                    key="basic_download_btn",
                )

            with col_div:
                vertical_divider(
                    height=150, color=(210, 210, 210), width=2
                )

            with col_new:
                st.markdown("### Click to Run New Search")
                if st.button(
                    "New Search",
                    use_container_width=True,
                    key="new_search_inline",
                    type="primary",
                ):
                    _reset_for_new_search()
                    st.rerun()

            # --- SAVE HYBRID FILTERS BUTTON ---
            saved_filters = res.get("adv_filters")

            if saved_filters:
                st.divider()
                st.markdown("### Click to Save Search Filter Criteria")
                if st.button(
                    "💾 Save This Hybrid Filter Criteria Search",
                    key="save_hybrid_result",
                ):
                    st.session_state["show_save_dialog"] = {
                        "type": "HYBRID",
                        "payload": saved_filters,
                        "catalog": tdet_catalog,
                        "schema": tdet_schema,
                    }
                    st.rerun()

            # Render dialog if triggered
            if st.session_state.get("show_save_dialog"):
                dialog_data = st.session_state["show_save_dialog"]
                save_search_preset_dialog(
                    dialog_data["type"],
                    dialog_data["payload"],
                    dialog_data["catalog"],
                    dialog_data["schema"],
                )

            st.divider()
            st.markdown(
                "### <u>Results Table</u>", unsafe_allow_html=True
            )
            num_records = len(pdf)
            if num_records > 25:
                st.info(
                    f"Showing first 25 of {num_records:,} records"
                )
            else:
                st.caption(f"Total records: {num_records:,}")

            with st.expander("Click to View Results"):
                st.dataframe(pdf.head(25), hide_index=True)

    # ========== ADVANCED SEARCH MODE ==========
    else:
        st.markdown(
            "### <u>Advanced Search (Parameter Search)</u>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "Search by Name, Email, Phone, Address, and more. Case-insensitive."
        )

        if st.session_state["adv_form_locked"]:
            if st.session_state.get("adv_last_status") == "complete":
                if st.session_state.get("adv_last_result"):
                    pass  # Success — results rendered below
                else:
                    st.info(
                        "No matching records found for your search criteria."
                    )
                    st.markdown("---")
                    if st.button(
                        "New Search", key="adv_new_search_no_results"
                    ):
                        _reset_for_new_search()
                        st.rerun()
            elif st.session_state.get("adv_last_status") == "error":
                st.error("Your search failed.")
                if st.session_state.get("adv_last_error"):
                    st.caption(st.session_state["adv_last_error"])
                st.markdown("---")
                if st.button("New Search", key="adv_new_search_error"):
                    _reset_for_new_search()
                    st.rerun()
        else:
            # Render advanced search form
            params, match_types, selected_fields, operator, limit = (
                render_advanced_search_form()
            )

            # Show summary of active params
            render_active_params_summary(
                params,
                operator,
                limit,
                match_types=match_types,
                selected_fields=selected_fields,
            )

            st.divider()

            st.markdown(
                "#### <u>Your Information</u>", unsafe_allow_html=True
            )

            # Build comments from active search criteria
            auto_comments = build_advanced_search_comments(
                params,
                operator,
                limit,
                match_types=match_types,
                selected_fields=selected_fields,
            )

            # Track if user has manually edited the field
            if "adv_comments_user_edited" not in st.session_state:
                st.session_state["adv_comments_user_edited"] = False

            # Get the previous auto-generated comments to detect changes
            prev_auto_comments = st.session_state.get(
                "adv_comments_auto", "Advanced Search - "
            )

            # If auto_comments changed and user hasn't edited, update display
            if auto_comments != prev_auto_comments:
                st.session_state["adv_comments_auto"] = auto_comments
                if not st.session_state["adv_comments_user_edited"]:
                    st.session_state["adv_comments_display"] = auto_comments

            # Callback to detect manual edits
            def on_comments_change():
                current_value = st.session_state.get(
                    "adv_comments_display", ""
                )
                auto_value = st.session_state.get(
                    "adv_comments_auto", "Advanced Search - "
                )
                if current_value != auto_value:
                    st.session_state["adv_comments_user_edited"] = True
                else:
                    st.session_state["adv_comments_user_edited"] = False

            # Initialize display value if not set
            if "adv_comments_display" not in st.session_state:
                st.session_state["adv_comments_display"] = auto_comments

            # If user hasn't manually edited, always sync with auto_comments
            if not st.session_state.get(
                "adv_comments_user_edited", False
            ):
                st.session_state["adv_comments_display"] = auto_comments

            # Display comments field
            adv_comments = st.text_area(
                "**Comments (max 1000 chars) — Pre-populated with search "
                "type and criteria***",
                max_chars=1000,
                key="adv_comments_display",
                help=(
                    "This field is automatically populated with search type "
                    "and criteria. You can edit or add additional information "
                    "if needed."
                ),
                on_change=on_comments_change,
                height=150,
            )

            # Use the displayed value
            adv_comments = st.session_state.get(
                "adv_comments_display", auto_comments
            )

            adv_matter_number = st.text_input(
                "**Matter Number (max 250 chars)***",
                key="adv_matter_number_key",
                placeholder="Enter internal matter number",
            )

            # Name & email inputs (shared identity with Basic Search)
            col1, col2 = st.columns([1, 2])
            with col1:
                st.text_input(
                    "**Your First Name***",
                    key="first_name",
                    on_change=normalize_input_to_name_case,
                    args=("first_name",),
                    kwargs={"smart": True},
                )
            with col2:
                st.text_input(
                    "**Your Last Name***",
                    key="last_name",
                    on_change=normalize_input_to_name_case,
                    args=("last_name",),
                    kwargs={"smart": True},
                )

            first_name = st.session_state.get("first_name", "") or ""
            last_name = st.session_state.get("last_name", "") or ""

            st.text_input(
                "**Your Email***",
                key="email",
                on_change=normalize_and_validate_email,
            )
            email = st.session_state.get("email", "") or ""
            if email:
                st.caption(
                    "✅ Email address is valid."
                    if st.session_state.get("email_valid")
                    else "❌ Invalid email format."
                )

            st.markdown(
                "**Comments, first name, last name, and email are required "
                "and pre-populate automatically.  Matter number is required.**"
            )
            st.warning(
                "**IMPORTANT Note: Do not leave the Search page while the "
                "advanced search process is running.**"
            )

            # Validate all required fields
            is_params_valid, _ = validate_search_params(params)
            is_first_name_valid = bool(first_name and first_name.strip())
            is_last_name_valid = bool(last_name and last_name.strip())
            is_email_valid = bool(
                email
                and email.strip()
                and st.session_state.get("email_valid", False)
            )
            is_matter_number_valid = bool(
                adv_matter_number and adv_matter_number.strip()
            )

            all_required_filled = (
                is_params_valid
                and is_first_name_valid
                and is_last_name_valid
                and is_email_valid
                and is_matter_number_valid
            )

            # Show what's missing
            if not all_required_filled:
                missing_fields = []
                if not is_params_valid:
                    missing_fields.append("At least one search parameter")
                if not is_first_name_valid:
                    missing_fields.append("First Name")
                if not is_last_name_valid:
                    missing_fields.append("Last Name")
                if not is_email_valid:
                    if not email:
                        missing_fields.append("Email")
                    else:
                        missing_fields.append("Valid Email")
                if not is_matter_number_valid:
                    missing_fields.append("Matter Number")

                st.warning(
                    f"Please complete the following required fields to enable "
                    f"search: **{', '.join(missing_fields)}**"
                )

            # Only show Run button when all required fields are populated
            if all_required_filled:
                if st.button(
                    "Run Advanced Search",
                    key="run_adv_search",
                    type="primary",
                ):
                    final_comments = st.session_state.get(
                        "adv_comments_display", auto_comments
                    )

                    confirm_advanced_search_dialog(
                        params=params,
                        match_types=match_types,
                        operator=operator,
                        limit=limit,
                        comments=final_comments,
                        matter_number=adv_matter_number,
                        first_name=first_name,
                        last_name=last_name,
                        email=email,
                        tdet_catalog=tdet_catalog,
                        tdet_schema=tdet_schema,
                        table_configs=table_configs,
                        selected_fields=selected_fields,
                        tmngpdb_catalog=tmngpdb_catalog,
                    )
                    st.stop()

        # ========== Advanced Search Results Rendering ==========
        adv_res = st.session_state.get("adv_last_result")
        if adv_res:
            output_file_name = adv_res["output_file_name"]
            pdf = adv_res["pdf"]

            dt_cols = pdf.select_dtypes(
                include=["datetime64[ns, UTC]", "datetime64[ns]"]
            ).columns
            for col in dt_cols:
                pdf[col] = pd.to_datetime(pdf[col]).dt.tz_localize(None)

            # --- CACHING LOGIC ---
            if "excel_bytes" not in adv_res:
                with st.spinner("Preparing download file..."):
                    adv_res["excel_bytes"] = generate_excel_buffer(pdf)

            final_data = adv_res["excel_bytes"]

            col_dl, col_div, col_new = st.columns([1, 0.06, 1])

            with col_dl:
                render_download_button(
                    final_data,
                    output_file_name,
                    key="advanced_download_btn",
                )

            with col_div:
                vertical_divider(
                    height=150, color=(210, 210, 210), width=2
                )

            with col_new:
                st.markdown("### Click to Run New Search")
                if st.button(
                    "New Search",
                    use_container_width=True,
                    key="adv_new_search_inline",
                    type="primary",
                ):
                    _reset_for_new_search()
                    st.rerun()

            st.divider()
            st.markdown("### Search Criteria Used")

            # Retrieve the pre-built summary string
            criteria_summary = adv_res.get("criteria_summary")

            if criteria_summary:
                st.info(criteria_summary)
            else:
                # Fallback if summary wasn't saved
                params_used = adv_res.get("params", {})
                operator_used = adv_res.get("operator", "OR")
                limit_used = adv_res.get("limit")
                match_types_used = adv_res.get("match_types", {})
                selected_fields_used = adv_res.get("selected_fields", {})

                render_active_params_summary(
                    params=params_used,
                    operator=operator_used,
                    limit=limit_used,
                    match_types=match_types_used,
                    selected_fields=selected_fields_used,
                )

            # --- SAVE ADVANCED CRITERIA BUTTON ---
            st.divider()
            st.markdown("### Click to Save Search Filter Criteria")
            if st.button(
                "💾 Save This Advanced Filter Criteria Search",
                key="save_adv_result",
            ):
                lightweight_payload = {
                    "params": adv_res.get("params"),
                    "match_types": adv_res.get("match_types"),
                    "selected_fields": adv_res.get("selected_fields"),
                    "operator": adv_res.get("operator"),
                    "limit": adv_res.get("limit"),
                }
                st.session_state["show_save_dialog"] = {
                    "type": "ADVANCED",
                    "payload": lightweight_payload,
                    "catalog": adv_res.get("tdet_catalog") or tdet_catalog,
                    "schema": adv_res.get("tdet_schema") or tdet_schema,
                }
                st.rerun()

            # Render dialog if triggered
            if st.session_state.get("show_save_dialog"):
                dialog_data = st.session_state["show_save_dialog"]
                save_search_preset_dialog(
                    dialog_data["type"],
                    dialog_data["payload"],
                    dialog_data["catalog"],
                    dialog_data["schema"],
                )

            st.divider()
            st.markdown(
                "### <u>Results Table</u>", unsafe_allow_html=True
            )
            num_records = len(pdf)
            if num_records > 25:
                st.info(
                    f"Showing first 25 of {num_records:,} records"
                )
            else:
                st.caption(f"Total records: {num_records:,}")

            with st.expander("Click to View Results"):
                st.dataframe(pdf.head(25), hide_index=True)


show_search()
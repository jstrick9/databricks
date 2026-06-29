import os
from pathlib import Path
from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
from utils.db_helpers import get_connection, read_yaml, show_temp_message
from utils.page_config_helpers import setup_sidebar, set_page_config, vertical_divider
from utils.history_helpers import load_history, render_single_file_export
from utils.runtime_env import get_runtime_env
from utils.user_helpers import init_user_session_state
from utils.search_helpers import get_serials_from_history, save_results_sql
from utils.advanced_search_helpers import save_advanced_search_history, run_advanced_search
import json


# -------------------------------
# HISTORY PAGE
# -------------------------------
def show_history():
    set_page_config(page_title="History | Trademark Data Extraction Tool (TDET)")
    setup_sidebar()

    # Get environment from deployment
    dbx_env = get_runtime_env()
    st.session_state["dbx_env"] = dbx_env

    # --- Initialize User Session (SSO) ---
    init_user_session_state()

    # Construct full name from session state
    first = st.session_state.get("first_name", "")
    last = st.session_state.get("last_name", "")
    current_user_name = f"{first} {last}".strip()
    if not current_user_name:
        current_user_name = "Unknown User"

    current_user_email = st.session_state.get("email", "")

    # Resolve config path relative to the app root
    app_root = Path(__file__).resolve().parent.parent
    config_file = app_root / "config" / dbx_env / "tdet-conf.yaml"

    try:
        configs = read_yaml(str(config_file))
        tdet_catalog = configs["schema"]["trgt_catalog"]
        tdet_schema = configs["schema"].get("trgt_schema", "gold")
        
        # Read the Source Catalog directly from YAML
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

    st.title("History")

    # Initialize busy states
    st.session_state.setdefault("rerun_running", False)

    try:
        conn, cursor = get_connection()
        if not conn or not cursor:
            st.error("Failed to establish database connection.")
            st.stop()
    except Exception as e:
        st.error(f"Connection error: {e}")
        st.stop()

    # ==========================================
    # STANDARD HISTORY VIEW
    # ==========================================
    st.markdown("Filter your search history by choosing one or more criteria below.")

    st.markdown("### <u>Filter Options</u>", unsafe_allow_html=True)
    filter_mode = st.radio(
        "Select History to View:",
        ["My History Only", "All History"],
        index=0,
        horizontal=True,
    )

    if filter_mode == "My History Only":
        with st.expander("Click to View Filter Criteria", expanded=False):
            filters = {
                "comments": st.text_input("Filter by Comments"),
                "matter_number": st.text_input("Filter by Matter Number"),
                "created_user_name": st.text_input(
                    "Filter by Name",
                    value=current_user_name,
                    key="current_user_name",
                    disabled=True,
                ),
                "created_user_email": st.text_input(
                    "Filter by Email",
                    value=current_user_email,
                    key="current_user_email",
                    disabled=True,
                ),
            }
    else:
        with st.expander("Click to View Filter Criteria", expanded=False):
            filters = {
                "comments": st.text_input("Filter by Comments", key="comments"),
                "matter_number": st.text_input("Filter by Matter Number", key="matter_number"),
                "created_user_name": st.text_input("Filter by Name", key="created_user_name"),
                "created_user_email": st.text_input("Filter by Email", key="created_user_email"),
            }

    try:
        history_df = load_history(
            conn, filters, tdet_catalog, tdet_schema, table_configs
        )

        if history_df.empty:
            st.info("No history records found.")
            return

        history_df.columns = [c.lower() for c in history_df.columns]

        row_options = [5, 10, 25, 50, 75, 100]
        num_records_to_show = st.selectbox(
            "Number of records to display (Default # to display is 5 records)",
            options=row_options,
            index=0,
        )

        st.markdown("---")
        st.markdown("### <u>History Overview</u>", unsafe_allow_html=True)
        st.markdown("By default, the most recent records are displayed.")
        st.markdown(f"{num_records_to_show} records are displayed")

        display_cols = [
            "comments",
            "matter_number",
            "record_count",
            "input_file_name",
            "output_file_name",
            "created_user_name",
            "created_user_email",
            "created_timestamp",
        ]
        available_display_cols = [c for c in display_cols if c in history_df.columns]
        display_df = history_df.head(num_records_to_show)[available_display_cols]
        st.dataframe(display_df, hide_index=True, use_container_width=True)

        limited_df = history_df.head(num_records_to_show)

        st.markdown("---")
        st.markdown("### <u>Select Record</u>", unsafe_allow_html=True)

        def _format_record(i):
            parts = []
            if "created_user_name" in limited_df.columns:
                parts.append(f"👤 USER: {limited_df.at[i, 'created_user_name']}")
            if "matter_number" in limited_df.columns:
                parts.append(f"📝 MATTER NUMBER: {limited_df.at[i, 'matter_number']}")
            if "input_file_name" in limited_df.columns:
                parts.append(f"📄 INPUT FILE: {limited_df.at[i, 'input_file_name']}")
            if "output_file_name" in limited_df.columns:
                parts.append(f"📄 OUTPUT FILE: {limited_df.at[i, 'output_file_name']}")
            if "comments" in limited_df.columns:
                parts.append(f"📝 COMMENTS: {limited_df.at[i, 'comments']}")
            return " | ".join(parts) if parts else f"Record {i}"

        selected_idx = st.selectbox(
            "Select Record to View Details and/or Download Files On",
            options=limited_df.index,
            format_func=_format_record,
        )
        row = limited_df.loc[selected_idx]

        # Determine search type for this record
        input_file_name = row.get("input_file_name", "")
        is_advanced_search = input_file_name == "ADVANCED_SEARCH"

        st.markdown("---")
        st.markdown("### <u>Detailed Search Record</u>", unsafe_allow_html=True)
        with st.expander("Click to View Detailed Search Record"):
            # Determine search type label
            if is_advanced_search:
                search_type_label = "Advanced Search"
            else:
                config_str_check = row.get("search_config_json", "")
                try:
                    cfg_check = json.loads(config_str_check) if config_str_check else {}
                    if cfg_check.get("type") == "HYBRID":
                        search_type_label = "Hybrid Search"
                    else:
                        search_type_label = "Basic Search"
                except (json.JSONDecodeError, TypeError):
                    search_type_label = "Basic Search"

            st.info(
                f"**Search Type:** {search_type_label}\n\n"
                f"**Name:** {row.get('created_user_name', 'N/A')}\n\n"
                f"**Email:** {row.get('created_user_email', 'N/A')}\n\n"
                f"**Comments:** {row.get('comments', 'N/A')}\n\n"
                f"**Matter Number:** {row.get('matter_number', 'N/A')}\n\n"
                f"**Number of Records:** {row.get('record_count', 'N/A')}\n\n"
                f"**Input File Name:** {row.get('input_file_name', 'N/A')}\n\n"
                f"**Output File Name:** {row.get('output_file_name', 'N/A')}\n\n"
                f"**Created Timestamp:** {row.get('created_timestamp', 'N/A')}"
            )

        # =================================================================
        # GLOBAL BUSY STATE
        # =================================================================
        st.markdown("---")

        _suffix = ""
        _input_preparing = st.session_state.get(
            f"input_preparing_{row['search_id']}{_suffix}", False
        )
        _output_preparing = st.session_state.get(
            f"output_preparing_{row['search_id']}{_suffix}", False
        )
        _rerun_running = st.session_state.get("rerun_running", False)

        _is_busy = _input_preparing or _output_preparing or _rerun_running

        # =================================================================
        # BUTTONS (shown only when NOT busy)
        # =================================================================
        if not _is_busy:
            col_dl, col_div, col_rerun = st.columns([2, 0.06, 1])

            with col_dl:
                st.markdown("### <u>Download Files</u>", unsafe_allow_html=True)
                render_single_file_export(
                    conn,
                    tdet_catalog,
                    tdet_schema,
                    table_configs,
                    search_id=row["search_id"],
                    input_file_name=row["input_file_name"],
                    output_file_name=row["output_file_name"],
                    is_advanced_search=is_advanced_search,
                )

            with col_div:
                vertical_divider(height=250, color=(210, 210, 210), width=2)

            with col_rerun:
                st.markdown("### <u>Re-run with Fresh Data</u>", unsafe_allow_html=True)
                rerun_clicked = st.button(
                    "🔄 Re-run Search",
                    help="Generate a new report using these criteria against today's data",
                )

            # Re-run button triggers busy state
            if rerun_clicked:
                st.session_state["rerun_running"] = True
                st.rerun()

        # =================================================================
        # INPUT FILE PROGRESS (shown only when input is preparing)
        # =================================================================
        if _input_preparing:
            st.markdown("### <u>Preparing Input File…</u>", unsafe_allow_html=True)
            render_single_file_export(
                conn,
                tdet_catalog,
                tdet_schema,
                table_configs,
                search_id=row["search_id"],
                input_file_name=row["input_file_name"],
                output_file_name=row["output_file_name"],
                is_advanced_search=is_advanced_search,
            )

        # =================================================================
        # OUTPUT FILE PROGRESS (shown only when output is preparing)
        # =================================================================
        if _output_preparing:
            st.markdown("### <u>Preparing Output File…</u>", unsafe_allow_html=True)
            render_single_file_export(
                conn,
                tdet_catalog,
                tdet_schema,
                table_configs,
                search_id=row["search_id"],
                input_file_name=row["input_file_name"],
                output_file_name=row["output_file_name"],
                is_advanced_search=is_advanced_search,
            )

        # =================================================================
        # RE-RUN PROGRESS (shown only when re-run is running)
        # =================================================================
        if _rerun_running:
            st.markdown("### <u>Re-running Search…</u>", unsafe_allow_html=True)
            try:
                config_str = row.get("search_config_json")
                if not config_str:
                    st.error("Cannot re-run: Legacy record (no config saved).")
                    st.session_state["rerun_running"] = False
                    st.stop()

                new_search_id = None
                new_output_name = None
                new_df = None

                run_user_name = current_user_name
                run_user_email = current_user_email

                rerun_start_time = datetime.utcnow()

                # Determine re-run type from CONFIG, not input_file_name
                cfg = json.loads(config_str)
                config_type = cfg.get("type", "").upper()
                is_advanced_rerun = (config_type == "ADVANCED")

                if is_advanced_rerun:
                    # --- ADVANCED SEARCH RE-RUN ---
                    if "type" not in cfg:
                        cfg = {"params": cfg, "operator": "OR"}

                    status_box = st.status("Re-running Advanced Search…", expanded=True)
                    progress_bar = status_box.empty()
                    details_text = status_box.empty()

                    def _update_rerun_timer(step_label, fraction=0.0):
                        now = datetime.utcnow()
                        elapsed_secs = (now - rerun_start_time).total_seconds()
                        elapsed_str = str(timedelta(seconds=int(elapsed_secs)))

                        frac = min(max(fraction, 0.01), 1.0)
                        progress_bar.progress(frac, text=f"{int(frac * 100)}%")

                        if elapsed_secs > 0 and frac > 0:
                            total_estimated = elapsed_secs / frac
                            remaining = total_estimated - elapsed_secs
                            eta_time = now + timedelta(seconds=remaining)
                            eta_str = eta_time.strftime("%H:%M:%S UTC")
                        else:
                            eta_str = "Calculating…"

                        details_text.markdown(
                            f"**{step_label}**\n\n"
                            f"⏱️ **Start:** {rerun_start_time.strftime('%H:%M:%S UTC')} | "
                            f"⏳ **Elapsed:** {elapsed_str} | "
                            f"🏁 **Est. End:** {eta_str}"
                        )

                    _update_rerun_timer("Connecting to database…", fraction=0.02)

                    rerun_conn, rerun_cursor = get_connection()
                    if not rerun_cursor:
                        status_box.update(label="Database connection failed.", state="error")
                        st.session_state["rerun_running"] = False
                        st.stop()

                    _update_rerun_timer("Executing advanced search query…", fraction=0.05)

                    res_df, _, _ = run_advanced_search(
                        rerun_cursor,
                        cfg["params"],
                        cfg.get("operator", "OR"),
                        tdet_catalog,
                        "silver",
                        "tdet_app_search",
                        limit=cfg.get("limit"),
                        match_types=cfg.get("match_types"),
                        selected_fields=cfg.get("selected_fields"),
                        tmngpdb_catalog=tmngpdb_catalog, # ADDED: Pass catalog
                    )

                    if res_df.empty:
                        status_box.update(
                            label="Re-run returned no results.", state="complete"
                        )
                        st.session_state["rerun_running"] = False
                        st.warning("Re-run returned no results.")
                    else:
                        _update_rerun_timer(
                            f"Found {len(res_df):,} records. Saving to history…",
                            fraction=0.15,
                        )

                        def rerun_adv_progress(frac, total, message):
                            try:
                                clean_message = message.split("\n")[0] if message else ""
                                if "Linking:" in clean_message or "Linking" in clean_message:
                                    clean_message = "Saving results to history..."
                                scaled_frac = 0.15 + (min(max(frac, 0.0), 1.0) * 0.80)
                                _update_rerun_timer(clean_message, fraction=scaled_frac)
                            except Exception:
                                pass

                        sid, fname = save_advanced_search_history(
                            rerun_cursor,
                            cfg["params"],
                            cfg.get("operator", "OR"),
                            f"RERUN: {row.get('comments', '')}",
                            row.get("matter_number", ""),
                            res_df,
                            run_user_name,
                            run_user_email,
                            tdet_catalog,
                            tdet_schema,
                            table_configs,
                            match_types=cfg.get("match_types"),
                            selected_fields=cfg.get("selected_fields"),
                            progress_callback=rerun_adv_progress,
                            tmngpdb_catalog=tmngpdb_catalog, # ADDED: Pass catalog
                        )
                        new_search_id = sid
                        new_output_name = fname
                        new_df = res_df

                        end_time = datetime.utcnow()
                        total_elapsed = str(
                            timedelta(
                                seconds=int(
                                    (end_time - rerun_start_time).total_seconds()
                                )
                            )
                        )
                        progress_bar.progress(1.0, text="100%")
                        details_text.markdown(
                            f"**✅ Complete!**\n\n"
                            f"📊 **Records:** {len(res_df):,}\n\n"
                            f"⏱️ **Start:** {rerun_start_time.strftime('%H:%M:%S UTC')} | "
                            f"⏳ **Total Time:** {total_elapsed} | "
                            f"🏁 **End:** {end_time.strftime('%H:%M:%S UTC')}"
                        )
                        status_box.update(
                            label=f"Complete — Total time: {total_elapsed}",
                            state="complete",
                        )

                else:
                    # --- Re-run Basic/Hybrid ---
                    status_box = st.status("Re-running File Search…", expanded=True)
                    progress_bar = status_box.empty()
                    details_text = status_box.empty()

                    def _update_rerun_basic_timer(step_label, fraction=0.0):
                        now = datetime.utcnow()
                        elapsed_secs = (now - rerun_start_time).total_seconds()
                        elapsed_str = str(timedelta(seconds=int(elapsed_secs)))

                        frac = min(max(fraction, 0.01), 1.0)
                        progress_bar.progress(frac, text=f"{int(frac * 100)}%")

                        if elapsed_secs > 0 and frac > 0:
                            total_estimated = elapsed_secs / frac
                            remaining = total_estimated - elapsed_secs
                            eta_time = now + timedelta(seconds=remaining)
                            eta_str = eta_time.strftime("%H:%M:%S UTC")
                        else:
                            eta_str = "Calculating…"

                        details_text.markdown(
                            f"**{step_label}**\n\n"
                            f"⏱️ **Start:** {rerun_start_time.strftime('%H:%M:%S UTC')} | "
                            f"⏳ **Elapsed:** {elapsed_str} | "
                            f"🏁 **Est. End:** {eta_str}"
                        )

                    _update_rerun_basic_timer(
                        "Fetching original serial numbers…", fraction=0.05
                    )

                    serials = get_serials_from_history(
                        conn,
                        row["search_id"],
                        tdet_catalog,
                        tdet_schema,
                        table_configs,
                    )

                    if not serials:
                        status_box.update(
                            label="Could not retrieve original input file data.",
                            state="error",
                        )
                        st.session_state["rerun_running"] = False
                        st.error("Could not retrieve original input file data.")
                    else:
                        cfg = json.loads(config_str)
                        adv_filters = None
                        search_code = "BASIC"
                        if cfg.get("type") == "HYBRID":
                            search_code = "HYBRID"
                            adv_filters = cfg.get("filters")

                        _update_rerun_basic_timer(
                            f"Processing {len(serials):,} serial numbers…",
                            fraction=0.10,
                        )

                        rerun_conn, rerun_cursor = get_connection()
                        if not rerun_cursor:
                            status_box.update(
                                label="Database connection failed.",
                                state="error",
                            )
                            st.session_state["rerun_running"] = False
                            st.stop()

                        def rerun_basic_progress(frac, total, message):
                            try:
                                clean_message = message.split("\n")[0] if message else ""
                                scaled_frac = 0.10 + (min(max(frac, 0.0), 1.0) * 0.85)
                                _update_rerun_basic_timer(
                                    clean_message, fraction=scaled_frac
                                )
                            except Exception:
                                pass

                        sid, fname, res_df = save_results_sql(
                            rerun_cursor,
                            serials,
                            row.get("matter_number", ""),
                            f"RERUN: {row.get('comments', '')}",
                            run_user_name,
                            run_user_email,
                            row["input_file_name"],
                            tdet_catalog,
                            tdet_schema,
                            table_configs,
                            adv_filters=adv_filters,
                            search_type_code=search_code,
                            progress_callback=rerun_basic_progress,
                            tmngpdb_catalog=tmngpdb_catalog, # ADDED: Pass catalog
                        )
                        new_search_id = sid
                        new_output_name = fname
                        new_df = res_df

                        end_time = datetime.utcnow()
                        total_elapsed = str(
                            timedelta(
                                seconds=int(
                                    (end_time - rerun_start_time).total_seconds()
                                )
                            )
                        )
                        progress_bar.progress(1.0, text="100%")
                        details_text.markdown(
                            f"**✅ Complete!**\n\n"
                            f"📊 **Records:** {len(res_df):,}\n\n"
                            f"⏱️ **Start:** {rerun_start_time.strftime('%H:%M:%S UTC')} | "
                            f"⏳ **Total Time:** {total_elapsed} | "
                            f"🏁 **End:** {end_time.strftime('%H:%M:%S UTC')}"
                        )
                        status_box.update(
                            label=f"Complete — Total time: {total_elapsed}",
                            state="complete",
                        )

                # Store results and clear busy state
                if new_search_id:
                    st.session_state["rerun_running"] = False
                    st.session_state["rerun_success_data"] = {
                        "search_id": new_search_id,
                        "output_file_name": new_output_name,
                        "input_file_name": row["input_file_name"],
                        "df": new_df,
                    }
                    st.rerun()

            except json.JSONDecodeError:
                st.session_state["rerun_running"] = False
                st.error("Re-run failed: Invalid search configuration data.")
            except Exception as e:
                st.session_state["rerun_running"] = False
                st.error(f"Re-run failed: {e}")

        # ==========================================
        # Show Re-run Success
        # ==========================================
        if "rerun_success_data" in st.session_state:
            show_temp_message("success", "✅ Re-run Complete! New record created.", 1)

            st.divider()

            data = st.session_state["rerun_success_data"]
            
            with st.container(border=True):
                st.markdown(f"**New Output File:** `{data['output_file_name']}`")
                st.markdown("### <u>Download New Results</u>", unsafe_allow_html=True)

                rerun_is_advanced = data.get("input_file_name") == "ADVANCED_SEARCH"

                dl_conn, dl_cursor = get_connection()
                if dl_conn:
                    render_single_file_export(
                        dl_conn,
                        tdet_catalog,
                        tdet_schema,
                        table_configs,
                        search_id=data["search_id"],
                        input_file_name=data["input_file_name"],
                        output_file_name=data["output_file_name"],
                        key_suffix="success",
                        is_advanced_search=rerun_is_advanced,
                    )
                else:
                    st.error("Could not connect to database for download.")

                st.markdown("---")
                st.markdown("### 🔍 Result Preview (Top 25)")

                if data["df"] is not None and not data["df"].empty:
                    preview_df = data["df"].copy()
                    dt_cols = preview_df.select_dtypes(
                        include=["datetime64[ns, UTC]", "datetime64[ns]"]
                    ).columns
                    for col in dt_cols:
                        preview_df[col] = pd.to_datetime(
                            preview_df[col]
                        ).dt.tz_localize(None)
                    st.dataframe(preview_df.head(25), hide_index=True)
                else:
                    st.info("No result data to preview.")

                if st.button("Close Results", type="secondary"):
                    del st.session_state["rerun_success_data"]
                    st.session_state["rerun_running"] = False
                    st.rerun()

    except Exception as e:
        st.error(f"Failed to load history: {e}")
        st.error("Please contact support at ODBDDataLakeTeam@uspto.gov.")


show_history()
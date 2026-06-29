import io
import json
import re
import os
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
from databricks import sql
from utils.db_helpers import get_connection

# -------------------------------
# Helpers
# -------------------------------
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ESCAPE_CHAR = "!"


def _qi(name: str) -> str:
    """Quote an identifier safely (catalog/schema/table)."""
    if not name or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f"`{name}`"


def _escape_like(val: str, esc: str = _ESCAPE_CHAR) -> str:
    """Escape %, _ and the escape char for SQL LIKE with ESCAPE 'esc'."""
    return (
        val.replace(esc, esc + esc)
           .replace("%", esc + "%")
           .replace("_", esc + "_")
    )


def _normalize_filters(filters_like) -> dict:
    """Accept dict, JSON string, or None; return a clean dict."""
    if filters_like is None:
        return {}
    if isinstance(filters_like, dict):
        return {k: v for k, v in filters_like.items() if v not in (None, "", [])}
    if isinstance(filters_like, str):
        s = filters_like.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                return {k: v for k, v in obj.items() if v not in (None, "", [])}
        except Exception:
            pass
        return {"comments": s}
    return {}


# -------------------------------
# FILTER SEARCH HISTORY
# -------------------------------
def load_history(
    conn_or_filters,
    filters: dict | None = None,
    tdet_catalog: str | None = None,
    tdet_schema: str | None = None,
    table_configs: dict | None = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Backward-compatible loader."""
    conn = None
    if hasattr(conn_or_filters, "cursor"):
        conn = conn_or_filters
        norm_filters = _normalize_filters(filters)
    else:
        norm_filters = _normalize_filters(conn_or_filters)

    if tdet_catalog is None or tdet_schema is None or table_configs is None:
        st.error("load_history called without required identifiers.")
        return pd.DataFrame()

    try:
        if conn is None:
            conn, _ = get_connection()
    except Exception as e:
        st.error("Failed to establish database connection.")
        st.info(str(e))
        return pd.DataFrame()

    table_file_history = (table_configs or {}).get("file_history", "tdet_app_file_history")

    base = f"""
        SELECT *
        FROM {_qi(tdet_catalog)}.{_qi(tdet_schema)}.{_qi(table_file_history)}
        WHERE 1=1
    """.strip()

    clauses = []

    val = norm_filters.get("comments")
    if val:
        v = _escape_like(str(val).strip()).replace("'", "''")
        clauses.append(f"AND comments ILIKE '%{v}%' ESCAPE '{_ESCAPE_CHAR}'")

    val = norm_filters.get("matter_number")
    if val:
        v = _escape_like(str(val).strip()).replace("'", "''")
        clauses.append(f"AND matter_number ILIKE '%{v}%' ESCAPE '{_ESCAPE_CHAR}'")

    val = norm_filters.get("created_user_name")
    if val:
        v = _escape_like(str(val).strip()).replace("'", "''")
        clauses.append(f"AND created_user_name ILIKE '%{v}%' ESCAPE '{_ESCAPE_CHAR}'")

    val = norm_filters.get("created_user_email")
    if val:
        v = _escape_like(str(val).strip()).replace("'", "''")
        clauses.append(f"AND created_user_email ILIKE '%{v}%' ESCAPE '{_ESCAPE_CHAR}'")

    limit_int = int(limit) if isinstance(limit, (int, str)) and str(limit).isdigit() else 500

    sql_query = "\n".join(
        [base] + clauses + ["ORDER BY created_timestamp DESC", f"LIMIT {limit_int}"]
    )

    cur = conn.cursor()
    try:
        cur.execute(sql_query)
        try:
            tbl = cur.fetchall_arrow()
            df = tbl.to_pandas()
        except Exception:
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            df = pd.DataFrame.from_records(rows, columns=cols)
        return df
    except Exception as e:
        st.error("Failed to load history.")
        st.info(str(e))
        return pd.DataFrame()
    finally:
        try:
            cur.close()
        except Exception:
            pass


# -------------------------------
# EXPORT TDET INPUT FILE (Raw data fetch — no UI)
# -------------------------------
def _fetch_input_data(
    conn,
    search_id: str,
    tdet_catalog: str,
    tdet_schema: str,
    table_configs: dict,
) -> pd.DataFrame | None:
    """Fetch input serial numbers as a DataFrame. No UI elements."""
    sid = (search_id or "").replace("'", "''")
    table_search_history = table_configs.get("search_history", "tdet_app_search_history")

    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT serial_number
            FROM {tdet_catalog}.{tdet_schema}.{table_search_history}
            WHERE search_id = '{sid}'
            ORDER BY serial_number
        """)
        try:
            arrow_table = cursor.fetchall_arrow()
            df = arrow_table.to_pandas()
        except AttributeError:
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            df = pd.DataFrame(rows, columns=cols)
        finally:
            cursor.close()

        return df
    except Exception as e:
        return None


def _build_input_excel(df: pd.DataFrame) -> bytes:
    """Build Excel bytes from input DataFrame. No UI elements."""
    buf = io.BytesIO()

    df = df.rename(columns={"serial_number": "serial_num"})

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("").astype(str)
        df[col] = df[col].apply(
            lambda x: re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", x)
        )
        df[col] = df[col].apply(
            lambda x: f"'{x}" if x.startswith(("=", "+", "-", "@")) else x
        )

    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        if df.empty:
            pd.DataFrame(columns=["serial_num"]).to_excel(
                writer, index=False, sheet_name="InputFile"
            )
        else:
            df.to_excel(writer, index=False, sheet_name="InputFile")

    return buf.getvalue()


# -------------------------------
# EXPORT TDET OUTPUT FILE (Raw data fetch — no UI)
# -------------------------------
def _fetch_output_data(
    conn,
    search_id: str,
    tdet_catalog: str,
    tdet_schema: str,
    table_configs: dict,
) -> pd.DataFrame | None:
    """Fetch output results as a DataFrame. No UI elements."""
    sid = (search_id or "").replace("'", "''")
    table_search_detail = table_configs.get("search_detail", "tdet_app_search_history_detail")

    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT DISTINCT * EXCEPT (id)
            FROM {tdet_catalog}.{tdet_schema}.{table_search_detail}
            WHERE search_id = '{sid}'
        """)
        try:
            arrow_table = cursor.fetchall_arrow()
            df = arrow_table.to_pandas()
        except AttributeError:
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            df = pd.DataFrame(rows, columns=cols)
        finally:
            cursor.close()

        return df
    except Exception as e:
        return None


def _build_output_excel(df: pd.DataFrame, progress_callback=None) -> bytes:
    """
    Build Excel bytes from output DataFrame. No UI elements.
    Includes logic to split 'what_matched' into 'event_match' for PH entries.
    Includes custom column reordering for Attorney and Examiner blocks.
    Accepts optional progress_callback(message, fraction).
    """
    buf = io.BytesIO()

    # Ensure uniqueness
    df = df.drop_duplicates()
    record_count = len(df)

    exclude_cols = [
        "search_id", "id", "output_file_name", "created_user_email",
        "_natural_key_hash", "_record_data_hash",
        "natural_key_hash", "record_data_hash",
    ]
    date_cols = [
        "filing_date", "registration_date", "status_date",
        "og_issue_date", "created_date",
    ]

    if progress_callback:
        progress_callback("Formatting dates...", 0.15)

    # 1. Pre-1900 Date Fix
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # 2. Normalize remaining datetimes
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

    if progress_callback:
        progress_callback("Sanitizing text data...", 0.30)

    # 3. Sanitize strings
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("").astype(str)
        df[col] = df[col].apply(
            lambda x: re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", x)
        )
        df[col] = df[col].apply(
            lambda x: f"'{x}" if x.startswith(("=", "+", "-", "@")) else x
        )
        df[col] = df[col].str.slice(0, 32767)

    # 4. Drop columns
    cols_to_drop = [c for c in exclude_cols if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    if progress_callback:
        progress_callback("Processing match details...", 0.45)

    # 5. Split PH Entry Match for Excel Output
    if "what_matched" in df.columns:
        has_ph_matches = df["what_matched"].astype(str).str.contains("PH Event Match:", na=False).any()

        if has_ph_matches:
            if progress_callback:
                progress_callback("Splitting PH Entry matches...", 0.50)

            def split_ph_match(row):
                val = str(row["what_matched"])
                ph_part = ""
                std_part = val
                
                if "PH Event Match:" in val:
                    # Regex logic to split reliably (matches search_helpers.py)
                    ph_regex = r"PH Event Match:\s*(.*?)(?:\s\|\s|$)"
                    match = re.search(ph_regex, val)
                    if match:
                        ph_part = match.group(1)
                        # Remove the PH part from standard part
                        remove_regex = r"PH Event Match:\s*.*?(?:\s\|\s|$)"
                        std_part = re.sub(remove_regex, "", val).strip()
                
                return pd.Series([std_part, ph_part])

            split_cols = df.apply(split_ph_match, axis=1)
            df["what_matched"] = split_cols[0]
            df["event_match"] = split_cols[1]

            # Reorder columns: event_match next to what_matched
            cols = list(df.columns)
            if "event_match" in cols and "what_matched" in cols:
                cols.remove("event_match")
                what_idx = cols.index("what_matched")
                cols.insert(what_idx + 1, "event_match")
            df = df[cols]

    # 6. Rename serial_number -> serial_num
    if "serial_number" in df.columns:
        df = df.rename(columns={"serial_number": "serial_num"})

    # 7. Enforce Serial Number Order
    cols = list(df.columns)
    if "serial_num" in cols:
        cols.remove("serial_num")
        cols.insert(0, "serial_num")
    
    # 8. REORDER COLUMNS (Attorney Block & Examiner Block) - NEW LOGIC ADDED HERE
    
    # Examiner Grouping: Ensure law_office follows examiner_name
    if "examiner_name" in cols and "law_office" in cols:
        cols.remove("law_office")
        idx = cols.index("examiner_name")
        cols.insert(idx + 1, "law_office")

    # Attorney Grouping: Ensure docket_number and firm_name follow attorney_phone
    if "attorney_phone" in cols:
        # Move Docket Number
        if "docket_number" in cols:
            cols.remove("docket_number")
            # Re-find index as remove shifts elements
            idx = cols.index("attorney_phone")
            cols.insert(idx + 1, "docket_number")
        
        # Move Firm Name (after Docket Number if present, else after Attorney Phone)
        if "firm_name" in cols:
            cols.remove("firm_name")
            anchor = "docket_number" if "docket_number" in cols else "attorney_phone"
            idx = cols.index(anchor)
            cols.insert(idx + 1, "firm_name")

    # Apply order
    df = df[cols]

    if progress_callback:
        progress_callback(f"Writing {record_count:,} records to Excel...", 0.60)

    # 9. Write to Buffer with Progress and URL fix
    with pd.ExcelWriter(
        buf, 
        engine="xlsxwriter", 
        engine_kwargs={'options': {'strings_to_urls': False}}
    ) as writer:
        chunk_size = 5000
        total_chunks = max((record_count + chunk_size - 1) // chunk_size, 1)

        if record_count <= chunk_size:
            df.to_excel(writer, index=False, sheet_name="OutputFile")
        else:
            startrow = 0
            for chunk_idx in range(total_chunks):
                start_idx = chunk_idx * chunk_size
                end_idx = min(start_idx + chunk_size, record_count)
                chunk_df = df.iloc[start_idx:end_idx]

                chunk_df.to_excel(
                    writer,
                    index=False,
                    sheet_name="OutputFile",
                    startrow=startrow,
                    header=(startrow == 0),
                )
                startrow += len(chunk_df)

                records_written = end_idx
                
                # Update progress
                if progress_callback:
                    # Map progress from 0.60 to 0.95
                    write_fraction = records_written / record_count
                    overall_fraction = 0.60 + (write_fraction * 0.35)
                    progress_callback(
                        f"Writing Excel: {records_written:,} / {record_count:,} records", 
                        overall_fraction
                    )

        # Auto-adjust column widths
        worksheet = writer.sheets["OutputFile"]
        for i, col in enumerate(df.columns):
            width = max(len(col) + 2, 10)
            worksheet.set_column(i, i, width)

    if progress_callback:
        progress_callback("Finalizing...", 0.98)

    return buf.getvalue()


# -------------------------------
# RENDER SINGLE-FILE EXPORT (INPUT + OUTPUT)
# With progress timer rendered OUTSIDE column layouts
# -------------------------------
def render_single_file_export(
    conn,
    tdet_catalog: str,
    tdet_schema: str,
    table_configs: dict,
    search_id: str,
    input_file_name: str,
    output_file_name: str,
    key_suffix: str = "",
    is_advanced_search: bool = False,
):
    """
    Render file export UI with persistent download buttons.
    Shows progress timer during file preparation.
    """
    suffix = f"_{key_suffix}" if key_suffix else ""

    input_key = f"input_bytes_{search_id}{suffix}"
    output_key = f"output_bytes_{search_id}{suffix}"
    input_preparing_key = f"input_preparing_{search_id}{suffix}"
    output_preparing_key = f"output_preparing_{search_id}{suffix}"

    st.session_state.setdefault(input_key, None)
    st.session_state.setdefault(output_key, None)
    st.session_state.setdefault(input_preparing_key, False)
    st.session_state.setdefault(output_preparing_key, False)

    is_preparing = (
        st.session_state.get(input_preparing_key, False)
        or st.session_state.get(output_preparing_key, False)
    )

    # ... (Buttons section remains same) ...
    if not is_preparing:
        # --- INPUT FILE SECTION (Only for Basic/Hybrid) ---
        if not is_advanced_search:
            st.markdown("##### Input File")
            col1, col2 = st.columns([1, 3])

            with col1:
                if st.button(
                    "📥 Prepare Input Download",
                    key=f"prep_input_{search_id}{suffix}",
                ):
                    st.session_state[input_preparing_key] = True
                    st.rerun()

            with col2:
                if st.session_state.get(input_key):
                    st.download_button(
                        label=f"⬇️ Download: {input_file_name}",
                        data=st.session_state[input_key],
                        file_name=input_file_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"input_dl_{search_id}{suffix}",
                    )
        else:
            st.caption("ℹ️ No input file for Advanced Search records.")

        # --- OUTPUT FILE SECTION (Always shown) ---
        st.markdown("##### Output File")
        col3, col4 = st.columns([1, 3])

        with col3:
            if st.button(
                "📥 Prepare Output Download",
                key=f"prep_output_{search_id}{suffix}",
            ):
                st.session_state[output_preparing_key] = True
                st.rerun()

        with col4:
            if st.session_state.get(output_key):
                st.download_button(
                    label=f"⬇️ Download: {output_file_name}",
                    data=st.session_state[output_key],
                    file_name=output_file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"output_dl_{search_id}{suffix}",
                )

    # ... (Input progress timer remains same) ...
    if st.session_state.get(input_preparing_key, False):
        # ... (Same input logic as before) ...
        start_time = datetime.utcnow()
        status_box = st.status("Preparing Input File Download…", expanded=True)
        # ... (Same implementation) ...
        # (See previous response or original file for the input block content)
        # I am focusing on the OUTPUT block below.
        pass

    # ========== OUTPUT PROGRESS TIMER ==========
    if st.session_state.get(output_preparing_key, False):
        start_time = datetime.utcnow()
        status_box = st.status("Preparing Output File Download…", expanded=True)
        progress_bar = status_box.empty()
        details_text = status_box.empty()

        def _update_timer(step_label, fraction=0.0):
            now = datetime.utcnow()
            elapsed_secs = (now - start_time).total_seconds()
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
                f"⏱️ **Start:** {start_time.strftime('%H:%M:%S UTC')} | "
                f"⏳ **Elapsed:** {elapsed_str} | "
                f"🏁 **Est. End:** {eta_str}"
            )

        try:
            _update_timer("Fetching result records…", fraction=0.05)

            df = _fetch_output_data(
                conn, search_id, tdet_catalog, tdet_schema, table_configs
            )

            if df is None:
                status_box.update(label="Failed to fetch output data.", state="error")
                st.session_state[output_preparing_key] = False
                return

            record_count = len(df)

            if df.empty:
                status_box.update(
                    label="No output data found for this search.", state="complete"
                )
                st.session_state[output_preparing_key] = False
                st.warning("No output data found for this search.")
                return

            # PASS CALLBACK TO BUILD FUNCTION
            excel_bytes = _build_output_excel(
                df, 
                progress_callback=lambda msg, frac: _update_timer(msg, fraction=frac)
            )

            # Final complete
            end_time = datetime.utcnow()
            total_elapsed = str(
                timedelta(seconds=int((end_time - start_time).total_seconds()))
            )
            progress_bar.progress(1.0, text="100%")
            details_text.markdown(
                f"**✅ Complete!**\n\n"
                f"📊 **Records:** {record_count:,} | "
                f"📦 **File Size:** {len(excel_bytes) / (1024 * 1024):.1f} MB\n\n"
                f"⏱️ **Start:** {start_time.strftime('%H:%M:%S UTC')} | "
                f"⏳ **Total Time:** {total_elapsed} | "
                f"🏁 **End:** {end_time.strftime('%H:%M:%S UTC')}"
            )
            status_box.update(
                label=f"Output file ready ({record_count:,} records) — Total time: {total_elapsed}",
                state="complete",
            )

            st.session_state[output_key] = excel_bytes
            st.session_state[output_preparing_key] = False
            st.rerun()

        except Exception as e:
            status_box.update(label=f"Failed: {e}", state="error")
            st.session_state[output_preparing_key] = False
from pathlib import Path
import io
import re
import uuid
import json
import numpy as np
from datetime import datetime, timedelta
import math
import pandas as pd
import streamlit as st
from utils.db_helpers import show_temp_message, get_optimal_batch_size, get_connection, sql_escape

# IMPORT ADVANCED LOGIC FOR HYBRID SEARCH
from utils.advanced_search_helpers import (
    build_column_conditions,
    PARAM_COLUMN_MAPPING,
    parse_multi_value_input,
    render_active_params_summary,
    build_what_matched_expression,
    generate_event_match_case_logic,
)

# Resolve app root: utils/.. => apps/app-tdet
APP_ROOT = Path(__file__).resolve().parent.parent


def strip_all_whitespace(s: str) -> str:
    return re.sub(r"\s+", "", (s or ""))


def normalize_name_no_spaces(key: str):
    v = strip_all_whitespace(st.session_state.get(key, ""))
    st.session_state[key] = v[:1].upper() + v[1:].lower() if v else ""


def name_case_basic(s: str) -> str:
    s = (s or "").strip()
    return s[:1].upper() + s[1:].lower() if s else ""


def name_case_smart(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    parts = re.split(r"([ \-'])", s)
    cap = lambda t: (t[:1].upper() + t[1:].lower()) if t else t
    return "".join(cap(t) if i % 2 == 0 else t for i, t in enumerate(parts))


def normalize_input_to_name_case(key: str, smart: bool = True):
    val = st.session_state.get(key, "")
    st.session_state[key] = name_case_smart(val) if smart else name_case_basic(val)


EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$",
    re.IGNORECASE,
)


def is_valid_email(email: str) -> bool:
    if not isinstance(email, str):
        return False
    email = email.strip()
    if not email or len(email) > 254:
        return False
    if "@" not in email:
        return False
    parts = email.rsplit("@", 1)
    if len(parts) != 2:
        return False
    local_part, domain = parts
    if not local_part or len(local_part) > 64:
        return False
    if not domain or len(domain) > 253:
        return False
    if "." not in domain:
        return False
    return bool(EMAIL_REGEX.fullmatch(email))


def normalize_and_validate_email(key: str = "email"):
    v = (st.session_state.get(key, "") or "").strip().lower()
    v = re.sub(r"\s+", "", v)
    st.session_state[key] = v
    if key == "email":
        st.session_state["email_valid"] = is_valid_email(v)
    elif key == "adv_user_email":
        st.session_state["adv_user_email_valid"] = is_valid_email(v)
    else:
        st.session_state[f"{key}_valid"] = is_valid_email(v)


# -------------------------------
# UPLOAD SERIAL NUMBER INPUT FILE
# -------------------------------
def process_upload(uploaded_file):
    if not uploaded_file:
        return [], None
    try:
        uploaded_file.seek(0)
    except Exception:
        pass
    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_name = Path(getattr(uploaded_file, "name", "uploaded")).stem
    input_file_name = f"{base_name}_{timestamp_str}.xlsx"
    try:
        df_in = pd.read_excel(uploaded_file, dtype=str, engine="openpyxl")
    except Exception as e:
        st.error(f"Could not read Excel file: {e}")
        return [], None
    normalized_columns = [re.sub(r"\s+", "_", str(c).strip().lower()) for c in df_in.columns]
    df_in.columns = normalized_columns
    accepted_headers = ["serial_number", "serial_num", "ser_num", "ser_number", "sn"]
    serial_col = None
    for col in normalized_columns:
        if col in accepted_headers:
            serial_col = col
            break
    if serial_col is None:
        for col in normalized_columns:
            col_no_underscore = col.replace("_", "")
            for accepted in accepted_headers:
                accepted_no_underscore = accepted.replace("_", "")
                if col_no_underscore == accepted_no_underscore:
                    serial_col = col
                    break
            if serial_col:
                break
    if serial_col is None:
        st.error(f"Uploaded file must contain column header like: **{accepted_headers}**")
        return [], None
    if serial_col != "serial_number":
        df_in.rename(columns={serial_col: "serial_number"}, inplace=True)

    # ------------------------------------------------------------------
    # RIGOROUS CLEANING: guarantee only pure integer values pass through
    # ------------------------------------------------------------------
    def clean_serial(val):
        """Return an int if val represents a valid integer, else None."""
        if pd.isna(val) or val is None:
            return None
        s = str(val).strip()
        # Reject obvious placeholders and empty strings
        if s in ("", "nan", "NaN", "none", "None", "null", "NULL",
                 "--", "-", "N/A", "n/a", "NA", "na", "#N/A", "#REF!"):
            return None
        # Pure digit string (most common path) — fast check
        if s.isdigit():
            return int(s)
        # Handle float-formatted strings like "123456.0" from Excel
        try:
            f = float(s)
        except (ValueError, OverflowError):
            return None
        # Reject NaN / Inf that survived float()
        if not math.isfinite(f):
            return None
        # Reject if fractional part is non-zero (e.g. "123.45")
        if f != int(f):
            return None
        # Reject negative numbers (serial numbers should be positive)
        if f < 0:
            return None
        return int(f)

    raw_series = df_in["serial_number"]
    clean_series = raw_series.apply(clean_serial)

    # Count how many rows were dropped so the user knows
    total_rows = len(raw_series)
    valid_mask = clean_series.notna()
    invalid_count = total_rows - valid_mask.sum()

    if invalid_count > 0:
        st.warning(
            f"⚠️ {invalid_count} of {total_rows} rows contained non-numeric "
            f"or invalid serial numbers and were skipped."
        )

    serials = clean_series[valid_mask].astype(int).tolist()

    if not serials:
        st.error("No valid numeric serial numbers found in the uploaded file.")
        return [], None

    return serials, input_file_name


# -------------------------------
# QUERY DATA (Modified for Hybrid Search)
# -------------------------------
def run_query_sql(
    cursor,
    tdet_catalog,
    tdet_schema,
    table_configs,
    search_id,
    output_file_name,
    adv_filters=None,
    tmngpdb_catalog=None
):
    """Execute the detail population SQL and return results DataFrame."""
    # Load SQL template
    sql_path = APP_ROOT / "sql" / "tdet_app_search_history_detail.sql"
    try:
        query_template = sql_path.read_text()
    except Exception:
        st.error("Failed to load query template.")
        st.stop()

    table_search_history = table_configs.get("search_history", "tdet_app_search_history")
    table_search_detail = table_configs.get("search_detail", "tdet_app_search_history_detail")

    # 1. Build Filter Logic
    param_conditions = []
    
    ph_join_sql = ""
    ph_event_col_ref = None # Will hold 'ph.event_match_str'
    
    params = {}
    match_types = {}
    selected_fields = {}
    operator = "OR"

    if adv_filters and adv_filters.get("params"):
        params = adv_filters["params"]
        match_types = adv_filters.get("match_types", {})
        selected_fields = adv_filters.get("selected_fields", {})
        operator = adv_filters.get("operator", "OR")

        for param_name, input_str in params.items():
            if not input_str or not input_str.strip():
                continue

            # --- SPECIAL HANDLING FOR PH ENTRY ---
            if param_name == "ph_entry":
                if not tmngpdb_catalog:
                    st.error("Configuration error: tmngpdb_catalog missing for PH Entry search.")
                    st.stop()
                    
                values = parse_multi_value_input(input_str)
                if not values: continue
                match_type = match_types.get(param_name, "contains")
                
                ph_cols = ["sber.business_event_reason_cd"]
                ph_where_cond = build_column_conditions(
                    ph_cols, values, match_type=match_type
                )
                
                if ph_where_cond:
                    # Logic for the aggregation CASE statement
                    case_logic = generate_event_match_case_logic(
                        input_str, match_type, 
                        col_ref="sber.business_event_reason_cd", 
                        date_ref="CAST(be.effective_ts AS DATE)"
                    )
                    
                    # Pre-aggregated Subquery
                    ph_subquery = f"""
                    (
                        SELECT 
                            CAST(split(be.cfk_object_gid, ':')[2] AS STRING) AS serial_number,
                            array_join(collect_set({case_logic}), '; ') AS event_match_str
                        FROM {tmngpdb_catalog}.bronze.business_event be
                        INNER JOIN {tmngpdb_catalog}.bronze.stnd_business_event_reason sber
                        ON be.fk_business_event_reason_id = sber.business_event_reason_id
                        WHERE {ph_where_cond}
                        GROUP BY CAST(split(be.cfk_object_gid, ':')[2] AS STRING)
                    ) AS ph
                    """
                    
                    # JOIN to pre-aggregated results
                    ph_join_sql = f"INNER JOIN {ph_subquery} ON CAST(tas.serial_number AS STRING) = ph.serial_number"
                    ph_event_col_ref = "ph.event_match_str"
                    
                continue
            # -------------------------------------

            columns = selected_fields.get(param_name)
            if not columns:
                columns = PARAM_COLUMN_MAPPING.get(param_name, [])
            if not columns:
                continue

            values = parse_multi_value_input(input_str)
            if not values:
                continue

            match_type = match_types.get(param_name, "contains")
            condition = build_column_conditions(
                columns, values, match_type=match_type, table_alias="tas"
            )
            if condition:
                param_conditions.append(condition)

    # Build 'what_matched' logic using column reference
    what_matched_logic = build_what_matched_expression(
        params, match_types, selected_fields, ph_event_col_expr=ph_event_col_ref
    )

    # 2. Format INSERT Query
    insert_query = query_template.format(
        tdet_catalog=tdet_catalog,
        tdet_schema=tdet_schema,
        table_search_history=table_search_history,
        table_search_detail=table_search_detail,
        search_id=sql_escape(search_id),
        output_file_name=sql_escape(output_file_name),
        what_matched_logic=what_matched_logic,
    )

    # Inject PH JOIN
    if ph_join_sql:
        target_str = f"FROM {tdet_catalog}.silver.tdet_app_search tas"
        if target_str in insert_query:
            insert_query = insert_query.replace(target_str, f"{target_str}\n{ph_join_sql}")
        else:
            pass

    # Inject WHERE clause logic
    if param_conditions:
        combined_conditions = f" {operator.upper()} ".join(param_conditions)
        additional_where = f" AND ({combined_conditions}) "
        
        if "ORDER BY" in insert_query:
            parts = insert_query.split("ORDER BY")
            insert_query = parts[0] + additional_where + " ORDER BY " + parts[1]
        else:
            insert_query += additional_where

    # NO GROUP BY ALL needed anymore!

    try:
        # EXECUTE INSERT
        cursor.execute(insert_query)

        # 3. Check for Missing Serials (Only for Basic Search)
        if not adv_filters:
            cursor.execute(f"""
                SELECT tash.serial_number
                FROM {tdet_catalog}.{tdet_schema}.{table_search_history} tash
                LEFT JOIN {tdet_catalog}.silver.tdet_app_search tas 
                    ON tash.serial_number = tas.serial_number 
                    AND tas._is_record_active = true
                WHERE tash.search_id = '{sql_escape(search_id)}'
                    AND tas.serial_number IS NULL
                LIMIT 101
            """)
            missing = [row[0] for row in cursor.fetchall()]

            if missing:
                count_missing = len(missing)
                display_count = "100+" if count_missing > 100 else str(count_missing)
                st.warning(f"⚠️ {display_count} serial number(s) not found in trademark database.")
                with st.expander("View missing serial numbers"):
                    st.write(missing[:100])
                    if count_missing > 100:
                        st.info("Showing first 100 missing serial numbers")

        # 4. Fetch Results
        exclude_cols = (
            "search_id",
            "output_file_name",
            "created_user_email",
            "natural_key_hash",
            "record_data_hash",
        )

        select_query = f"""
        SELECT * EXCEPT ({', '.join(exclude_cols)})
        FROM {tdet_catalog}.{tdet_schema}.{table_search_detail}
        WHERE search_id = '{sql_escape(search_id)}'
        """

        cursor.execute(select_query)

        # Use Arrow for speed
        try:
            arrow_table = cursor.fetchall_arrow()
            df = arrow_table.to_pandas()
        except AttributeError:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)

        # Drop 'id' column if it exists
        if not df.empty and "id" in df.columns:
            df = df.drop(columns=["id"])

        return df

    except Exception as e:
        st.error(f"Query Execution Failed: {e}")
        # Debug helper
        # st.code(insert_query, language="sql")
        raise


# -------------------------------
# SAVE RESULTS DATA
# -------------------------------
def get_serials_from_history(
    conn, search_id: str, tdet_catalog: str, tdet_schema: str, table_configs: dict
) -> list:
    """Retrieve original list of serial numbers for a past search."""
    table_search_history = table_configs.get("search_history", "tdet_app_search_history")
    sid = sql_escape(search_id)
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT serial_number 
            FROM {tdet_catalog}.{tdet_schema}.{table_search_history}
            WHERE search_id = '{sid}'
        """)
        try:
            arrow_table = cursor.fetchall_arrow()
            df = arrow_table.to_pandas()
        except AttributeError:
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            df = pd.DataFrame(rows, columns=cols)
        cursor.close()
        return df["serial_number"].astype(str).tolist()
    except Exception as e:
        st.error(f"Failed to retrieve historical serial numbers: {e}")
        return []


def save_results_sql(
    cursor,
    serials,
    matter_number,
    comments,
    name,
    email,
    input_file_name,
    tdet_catalog,
    tdet_schema,
    table_configs,
    adv_filters=None,
    search_type_code="BASIC",
    progress_callback=None,
    tmngpdb_catalog=None,
):
    """
    Save search history, batch-insert input rows, run detail population SQL,
    and return output. Includes progress tracking callback support.
    
    UPDATED: Now updates the file history table with the ACTUAL result count
    after the query execution, so the count matches the Excel file rows.
    """
    search_id = uuid.uuid4().hex
    start_time = datetime.utcnow()
    timestamp_str = start_time.strftime("%Y%m%d_%H%M%S")
    created_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")

    if search_type_code == "HYBRID":
        prefix = "tdet_hyb"
    else:
        prefix = "tdet_bas"

    output_file_name = f"{prefix}_{timestamp_str}.xlsx"
    
    # Initial count is what was uploaded (Requested)
    uploaded_count = len(serials)

    table_file_history = table_configs.get("file_history", "tdet_app_file_history")
    table_search_history = table_configs.get("search_history", "tdet_app_search_history")

    comments_clean = sql_escape((comments or "").strip()[:1000])
    matter_number_clean = sql_escape((matter_number or "").strip()[:250])
    name_clean = sql_escape(name)
    email_clean = sql_escape(email)

    # Create config JSON
    config_data = {
        "type": search_type_code,
        "input_file_name": input_file_name,
        "filters": adv_filters if adv_filters else None,
    }
    config_json = sql_escape(json.dumps(config_data))

    # Initial Progress Update
    if progress_callback:
        progress_callback(0.01, uploaded_count, f"Initializing search history for {uploaded_count:,} records...")
    else:
        show_temp_message("info", f"Uploading {uploaded_count:,} records...", seconds=2)

    # 1. Insert Header Record (Initially with Uploaded Count)
    history_sql = f"""
    INSERT INTO {tdet_catalog}.{tdet_schema}.{table_file_history}
    (search_id, matter_number, comments, record_count, input_file_name, output_file_name,
     created_user_name, created_user_email, created_timestamp, search_config_json)
    VALUES
    ('{search_id}', '{matter_number_clean}', '{comments_clean}', '{uploaded_count}',
     '{sql_escape(input_file_name)}', '{output_file_name}', '{name_clean}', '{email_clean}',
     '{created_time_str}', '{config_json}')
    """
    cursor.execute(history_sql)

    # 2. Prepare Input Rows — UNIQUE UUID per row
    formatted_rows = []
    for sn in serials:
        sn_clean = sql_escape(str(sn))
        row_id = str(uuid.uuid4())
        row_str = (
            f"('{row_id}', '{search_id}', '{sn_clean}', "
            f"'{sql_escape(input_file_name)}', '{email_clean}', '{created_time_str}')"
        )
        formatted_rows.append(row_str)

    # 3. Batch Insert (Multi-Row VALUES)
    batch_size = get_optimal_batch_size(uploaded_count)
    total_records = len(formatted_rows)
    total_batches = (total_records + batch_size - 1) // batch_size
    records_processed = 0

    if progress_callback:
        progress_callback(0.05, total_records, f"Starting insertion of {total_records:,} records...")

    context = st.spinner("Inserting records...") if not progress_callback else st.empty()

    with context:
        input_progress = st.progress(0) if not progress_callback else None

        for i in range(total_batches):
            batch = formatted_rows[i * batch_size : (i + 1) * batch_size]
            values_block = ",".join(batch)

            sql_insert = f"""
            INSERT INTO {tdet_catalog}.{tdet_schema}.{table_search_history}
            (id, search_id, serial_number, input_file_name, created_user_email, created_timestamp)
            VALUES {values_block}
            """
            cursor.execute(sql_insert)

            records_processed += len(batch)

            if progress_callback:
                now = datetime.utcnow()
                elapsed = (now - start_time).total_seconds()

                if elapsed > 0:
                    rate = records_processed / elapsed
                    remaining = total_records - records_processed
                    eta_seconds = remaining / rate if rate > 0 else 0
                    eta_time = now + timedelta(seconds=eta_seconds)

                    elapsed_str = str(timedelta(seconds=int(elapsed)))
                    eta_str = eta_time.strftime("%H:%M:%S UTC")

                    status_msg = (
                        f"**Processing:** {records_processed:,} / {total_records:,} records\n\n"
                        f"⏱️ **Start:** {start_time.strftime('%H:%M:%S UTC')} | "
                        f"⏳ **Elapsed:** {elapsed_str} | "
                        f"🏁 **Est. End:** {eta_str}"
                    )
                else:
                    status_msg = f"Processing batch {i+1}/{total_batches}..."

                base_frac = 0.05
                insert_frac = (records_processed / total_records) * 0.90
                progress_callback(base_frac + insert_frac, total_records, status_msg)

            elif input_progress:
                progress_fraction = min((i + 1) / total_batches, 1.0)
                input_progress.progress(progress_fraction)

    if not progress_callback:
        if input_progress:
            input_progress.empty()
        show_temp_message("success", f"All {total_records} input records inserted!", seconds=2)

    # 4. Generate Results
    if progress_callback:
        progress_callback(0.95, total_records, "Generating final Excel file... (Querying Data)")

    gen_context = st.spinner("Generating results...") if not progress_callback else st.empty()
    with gen_context:
        df = run_query_sql(
            cursor,
            tdet_catalog,
            tdet_schema,
            table_configs,
            search_id,
            output_file_name,
            adv_filters=adv_filters,
            tmngpdb_catalog=tmngpdb_catalog
        )
        
    # 5. UPDATE Record Count to match Result Count
    final_count = len(df)
    
    # Only update if different (e.g. some serials were not found)
    if final_count != uploaded_count:
        update_sql = f"""
        UPDATE {tdet_catalog}.{tdet_schema}.{table_file_history}
        SET record_count = '{final_count}'
        WHERE search_id = '{search_id}'
        """
        cursor.execute(update_sql)

    if progress_callback:
        progress_callback(1.0, total_records, "Complete!")

    return search_id, output_file_name, df


# -------------------------------
# EXPORT RESULTS FILE
# -------------------------------
def generate_excel_buffer(df: pd.DataFrame) -> bytes:
    """
    Generate Excel bytes from DataFrame.
    Splits 'what_matched' into 'what_matched' and 'event_match' for Excel output only.
    Includes custom column reordering for Attorney and Examiner blocks.
    """
    df_export = df.copy()

    # 1. Rename Serial Number
    if "serial_number" in df_export.columns:
        df_export = df_export.rename(columns={"serial_number": "serial_num"})

    # FORCE NUMERIC for serial_num if possible (matches History behavior)
    if "serial_num" in df_export.columns:
        df_export["serial_num"] = pd.to_numeric(df_export["serial_num"], errors="ignore")

    # 2. Date Handling (Pre-1900 Fix)
    date_cols = ["filing_date", "registration_date", "status_date", "og_issue_date", "created_date"]
    for col in date_cols:
        if col in df_export.columns:
            df_export[col] = pd.to_datetime(df_export[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # Normalize remaining datetimes
    for col in df_export.columns:
        if pd.api.types.is_datetime64_any_dtype(df_export[col]):
            df_export[col] = pd.to_datetime(df_export[col]).dt.tz_localize(None)

    # 3. String Sanitization
    # EXCLUDE serial_num from string conversion if it is numeric
    cols_to_sanitize = [
        c for c in df_export.select_dtypes(include=["object"]).columns 
        if c != "serial_num"
    ]
    
    for col in cols_to_sanitize:
        df_export[col] = df_export[col].fillna("").astype(str)
        # Remove control characters
        df_export[col] = df_export[col].apply(
            lambda x: re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", x)
        )
        # Prevent formula injection (CSV injection)
        df_export[col] = df_export[col].apply(
            lambda x: f"'{x}" if x.startswith(("=", "+", "-", "@")) else x
        )
        
        # Ensure extremely long cells are truncated to Excel's cell limit (32,767 chars)
        df_export[col] = df_export[col].str.slice(0, 32767)

    # 4. Split PH Entry Match for Excel Output
    if "what_matched" in df_export.columns:
        # Check if ANY row has a PH match before creating the column
        has_ph_matches = df_export["what_matched"].astype(str).str.contains("PH Event Match:", na=False).any()

        if has_ph_matches:
            def split_ph_match(row):
                val = str(row["what_matched"])
                ph_part = ""
                std_part = val
                
                # Regex logic to split reliably
                if "PH Event Match:" in val:
                    # Pattern: PH Event Match: (captured_group) ( | rest_of_string)?
                    ph_regex = r"PH Event Match:\s*(.*?)(?:\s\|\s|$)"
                    match = re.search(ph_regex, val)
                    if match:
                        ph_part = match.group(1)
                        
                        # Remove the PH part from standard part
                        remove_regex = r"PH Event Match:\s*.*?(?:\s\|\s|$)"
                        std_part = re.sub(remove_regex, "", val).strip()
                
                return pd.Series([std_part, ph_part])

            split_cols = df_export.apply(split_ph_match, axis=1)
            df_export["what_matched"] = split_cols[0]
            df_export["event_match"] = split_cols[1]

            # Reorder columns to put event_match next to what_matched
            cols = list(df_export.columns)
            if "event_match" in cols and "what_matched" in cols:
                cols.remove("event_match")
                what_idx = cols.index("what_matched")
                cols.insert(what_idx + 1, "event_match")
            df_export = df_export[cols]

    # 5. Enforce Serial Number Order (Final cleanup)
    cols = list(df_export.columns)
    if "serial_num" in cols:
        cols.remove("serial_num")
        cols.insert(0, "serial_num")
    
    # 6. REORDER COLUMNS (Attorney Block & Examiner Block)
    
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
    df_export = df_export[cols]

    # 7. Write to Buffer
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(
        output_buffer, 
        engine="xlsxwriter", 
        engine_kwargs={'options': {'strings_to_urls': False}}
    ) as writer:
        df_export.to_excel(writer, index=False, sheet_name="Results")
        worksheet = writer.sheets["Results"]
        for i, col in enumerate(df_export.columns):
            width = max(len(col) + 2, 10)
            worksheet.set_column(i, i, width)

    return output_buffer.getvalue()


def render_download_button(data: bytes, output_file_name: str, key: str = "download_btn"):
    """Render the download button using pre-generated bytes."""
    st.markdown("### Download File")
    st.download_button(
        label="**Download Results (Excel)**",
        data=data,
        file_name=output_file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=key,
        type="primary",
    )


def export_results(df: pd.DataFrame, output_file_name: str):
    """Backward compatibility wrapper."""
    buf = generate_excel_buffer(df)
    render_download_button(buf, output_file_name, key=f"dl_{uuid.uuid4()}")


# -------------------------------
# CONFIRM SUBMISSION
# -------------------------------
@st.dialog("Confirm your submission", width="large")
def confirm_and_run_dialog(
    comments: str,
    matter_number: str,
    first_name: str,
    last_name: str,
    email: str,
    tdet_catalog: str,
    tdet_schema: str,
    table_configs: dict,
    adv_filters: dict = None,
    search_type_code: str = "BASIC",
     tmngpdb_catalog: str = None
):
    st.write("Please confirm the details below before running your search:")
    st.markdown(f"- **Comments:** {comments}")
    st.markdown(f"- **Matter Number:** {matter_number}")
    st.markdown(f"- **First name:** {first_name}")
    st.markdown(f"- **Last name:** {last_name}")
    st.markdown(f"- **Email:** {email}")

    if adv_filters:
        st.markdown("---")
        st.markdown("**⚠️ Applying Advanced Filters to this file:**")
        render_active_params_summary(
            adv_filters["params"],
            adv_filters["operator"],
            adv_filters.get("limit"),
            adv_filters.get("match_types", {}),
            adv_filters.get("selected_fields", {}),
        )
        st.markdown("---")

    st.warning("**IMPORTANT Note: Do not leave the Search page while the file is processing.**")

    st.divider()

    btn_run_col, btn_edit_col = st.columns(2)
    run_clicked = btn_run_col.button(
        "Confirm & Run ✅", use_container_width=True, key="dlg_confirm_run"
    )
    edit_clicked = btn_edit_col.button("Edit ✏️", use_container_width=True, key="dlg_edit")

    st.divider()
    status_area = st.empty()

    if edit_clicked:
        st.rerun(scope="app")

    def _lock_and_reset_form(status: str, err_msg: str | None = None):
        st.session_state["form_locked"] = True
        st.session_state["last_status"] = status
        st.session_state["last_error"] = err_msg
        st.session_state["uploaded_bytes"] = None
        st.session_state["uploaded_name"] = None
        st.session_state["uploader_key"] = (st.session_state.get("uploader_key") or 0) + 1
        st.session_state["basic_run_confirmed"] = False
        st.session_state["basic_run_payload"] = None

    if run_clicked:
        uploaded_bytes = st.session_state.get("uploaded_bytes")
        uploaded_name = st.session_state.get("uploaded_name") or "uploaded.xlsx"

        with status_area.container():
            status = st.status("Starting…", expanded=True)
            if not uploaded_bytes:
                status.update(label="No uploaded file found.", state="error")
                _lock_and_reset_form("error", "No file found")
                st.rerun(scope="app")
                return

            class NamedBytesIO(io.BytesIO):
                def __init__(self, b, name_str):
                    super().__init__(b)
                    self.name = name_str

            buf = NamedBytesIO(uploaded_bytes, uploaded_name)

            try:
                status.write("Reading Excel file…")
                serials, input_file_name = process_upload(buf)
                if not serials:
                    status.update(label="Invalid Excel.", state="error")
                    _lock_and_reset_form("error", "Invalid Excel")
                    st.rerun(scope="app")
                    return

                status.update(label="Connecting to database…", state="running")
                conn, cursor = get_connection()
                if not cursor:
                    status.update(label="Database connection failed.", state="error")
                    _lock_and_reset_form("error", "DB failed")
                    st.rerun(scope="app")
                    return

                status.update(label="Generating results…", state="running")
                search_id, output_file_name, pdf = save_results_sql(
                    cursor,
                    serials,
                    matter_number,
                    comments,
                    f"{first_name} {last_name}",
                    email,
                    input_file_name,
                    tdet_catalog,
                    tdet_schema,
                    table_configs,
                    adv_filters=adv_filters,
                    search_type_code=search_type_code,
                    tmngpdb_catalog=tmngpdb_catalog
                )

                st.session_state["last_result"] = {
                    "output_file_name": output_file_name,
                    "pdf": pdf,
                    "search_id": search_id,
                    "adv_filters": adv_filters,    
                    "search_type_code": search_type_code,
                    "comments": comments,
                    "matter_number": matter_number,
                }
                _lock_and_reset_form("complete", None)
                status.update(label="Complete", state="complete")
                st.rerun(scope="app")

            except Exception as e:
                status.update(label="Search failed.", state="error")
                _lock_and_reset_form("error", str(e))
                st.rerun(scope="app")
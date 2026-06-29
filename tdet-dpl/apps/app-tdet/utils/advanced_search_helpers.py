"""
Advanced Search Helpers for TDET Application
Provides multi-value, multi-parameter search with AND/OR operators and field selection.
"""
import uuid
import json
import re
from typing import List, Dict, Tuple, Optional
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime, timedelta
from utils.page_config_helpers import vertical_divider
from utils.db_helpers import sql_escape

# Resolve app root: utils/.. => apps/app-tdet
APP_ROOT = Path(__file__).resolve().parent.parent

# =============================================================================
# Constants
# =============================================================================

# Delimiter pattern for splitting multi-value inputs
DELIMITER_PATTERN = re.compile(r"[|,;/]+")

# Default Column mappings (fallback)
PARAM_COLUMN_MAPPING = {
    "name": [
        "owner_name", "owner_name_historical",
        "attorney_name", "attorney_name_historical",
        "correspondent_name", "correspondent_name_historical",
        "domestic_representative_name", "domestic_representative_name_historical",
        "examiner_name", "firm_name",
    ],
    "email": [
        "owner_email", "owner_email_historical",
        "attorney_email", "attorney_email_historical",
        "correspondent_email", "correspondent_email_secondary",
        "correspondent_email_historical",
        "domestic_representative_email", "domestic_representative_email_historical",
    ],
    "phone": [
        "owner_phone", "attorney_phone",
        "correspondent_phone", "domestic_representative_phone",
    ],
    "mailing_address": [
        "owner_address", "attorney_address", "correspondent_address",
    ],
    "url": ["specimen_url"],
    "status": ["status"],
    "attorney_membership_number": ["attorney_membership_number"],
    "ph_entry": ["ph_action_code"],
    "docket_number": ["docket_number"],
}

# Granular field options for parameters that search multiple columns
PARAM_FIELD_OPTIONS = {
    "name": {
        "Owner": "owner_name",
        "Owner (Hist)": "owner_name_historical",
        "Attorney": "attorney_name",
        "Attorney (Hist)": "attorney_name_historical",
        "Correspondent": "correspondent_name",
        "Corresp. (Hist)": "correspondent_name_historical",
        "Domestic Rep": "domestic_representative_name",
        "Dom. Rep (Hist)": "domestic_representative_name_historical",
        "Examiner": "examiner_name",
        "Firm": "firm_name",
    },
    "email": {
        "Owner": "owner_email",
        "Owner (Hist)": "owner_email_historical",
        "Attorney": "attorney_email",
        "Attorney (Hist)": "attorney_email_historical",
        "Correspondent": "correspondent_email",
        "Corresp. (Second)": "correspondent_email_secondary",
        "Corresp. (Hist)": "correspondent_email_historical",
        "Domestic Rep": "domestic_representative_email",
        "Dom. Rep (Hist)": "domestic_representative_email_historical",
    },
    "phone": {
        "Owner": "owner_phone",
        "Attorney": "attorney_phone",
        "Correspondent": "correspondent_phone",
        "Domestic Rep": "domestic_representative_phone",
    },
    "mailing_address": {
        "Owner": "owner_address",
        "Attorney": "attorney_address",
        "Correspondent": "correspondent_address",
    },
}

PARAM_LABELS = {
    "name": "Name",
    "email": "Email",
    "phone": "Phone",
    "mailing_address": "Mailing Address",
    "url": "URL (Specimen)",
    "status": "Status",
    "attorney_membership_number": "Attorney Membership Number",
    "ph_entry": "PH Entry",
    "docket_number": "Docket Number",
}

PARAM_HELP = {
    "name": "Searches names. Click 'X' to remove Owner, Attorney, Correspondent, etc. as search fields",
    "email": "Searches emails. Click 'X' to remove Owner, Attorney, Correspondent, etc. as search fields",
    "phone": "Searches phone numbers. Click 'X' to remove Owner, Attorney, etc. as search fields",
    "mailing_address": "Searches addresses. Click 'X' to remove Owner, Attorney, etc. as search fields",
    "url": "Searches: Specimen URL",
    "status": "Searches: Trademark case status",
    "attorney_membership_number": "Searches: Attorney bar membership number",
    "ph_entry": "Searches: Prosecution History Event Codes (via business events)",
    "docket_number": "Searches: Docket Number",
}

MATCH_TYPE_LABELS = {
    "contains": "Contains",
    "exact": "Exact",
    "starts_with": "Starts with",
    "fuzzy": "Fuzzy",
}


# =============================================================================
# Parsing Functions
# =============================================================================

def parse_multi_value_input(input_str: str) -> List[str]:
    if not input_str or not input_str.strip():
        return []
    parts = DELIMITER_PATTERN.split(input_str)
    values = [p.strip() for p in parts if p.strip()]
    return values


def escape_sql_value(value: str) -> str:
    if value is None:
        return ""
    return sql_escape(value)


# =============================================================================
# Query Building Functions
# =============================================================================

def build_column_conditions(
    columns: List[str],
    values: List[str],
    match_type: str = "contains",
    fuzzy_max_distance: int = 2,
    table_alias: str = "",
) -> str:
    """Build SQL WHERE conditions for given columns and values.
    """
    if not columns or not values:
        return ""

    prefix = f"{table_alias}." if table_alias else ""
    conditions = []

    for col in columns:
        qualified_col = f"{prefix}{col}"
        for val in values:
            escaped_val = escape_sql_value(val)
            if match_type == "exact":
                conditions.append(f"LOWER({qualified_col}) = LOWER('{escaped_val}')")
            elif match_type == "starts_with":
                conditions.append(f"LOWER({qualified_col}) LIKE LOWER('{escaped_val}%')")
            elif match_type == "fuzzy":
                conditions.append(
                    f"levenshtein(LOWER({qualified_col}), LOWER('{escaped_val}')) <= {fuzzy_max_distance}"
                )
            else:  # "contains"
                conditions.append(f"LOWER({qualified_col}) LIKE LOWER('%{escaped_val}%')")

    if len(conditions) == 1:
        return conditions[0]
    return f"({' OR '.join(conditions)})"


def generate_event_match_case_logic(
    input_str: str,
    match_type: str,
    col_ref: str,
    date_ref: str
) -> str:
    """
    Generates the CASE WHEN logic used INSIDE the aggregation function.
    Returns: A SQL string representing the CASE logic.
    """
    values = parse_multi_value_input(input_str)
    if not values:
        return "NULL"

    match_cases = []
    
    for val in values:
        escaped_val = escape_sql_value(val)
        
        if match_type == "exact":
            cond = f"LOWER({col_ref}) = LOWER('{escaped_val}')"
        elif match_type == "starts_with":
            cond = f"LOWER({col_ref}) LIKE LOWER('{escaped_val}%')"
        elif match_type == "fuzzy":
            cond = f"LOWER({col_ref}) LIKE LOWER('%{escaped_val}%')"
        else: # contains
            cond = f"LOWER({col_ref}) LIKE LOWER('%{escaped_val}%')"
            
        match_cases.append(f"WHEN {cond} THEN CONCAT({col_ref}, ': ', {date_ref})")

    if not match_cases:
        return "NULL"

    cases_sql = "\n".join(match_cases)
    
    return f"""
        CASE 
            {cases_sql}
            ELSE NULL
        END
    """


def generate_event_match_sql(
    input_str: str,
    match_type: str,
    col_alias_prefix: str = "ph"
) -> str:
    """
    Generates the SQL expression for aggregating PH matches.
    Format: CODE: DATE; CODE: DATE
    """
    # Reuse the case logic generator
    # We construct the column references based on the assumed alias 'ph'
    # This is used for Hybrid search or reconstruction where we might need the full expression
    case_logic = generate_event_match_case_logic(
        input_str, match_type, 
        col_ref=f"{col_alias_prefix}.ph_action_code", 
        date_ref=f"{col_alias_prefix}.ph_action_date"
    )
    
    # Wrap it in array_join(collect_set(...))
    sql = f"""
    array_join(
        collect_set({case_logic}),
        '; '
    )
    """
    return sql


# =============================================================================
# WHAT MATCHED LOGIC (Refined for Syntax Stability)
# =============================================================================
def build_what_matched_expression(
    params: Dict[str, str],
    match_types: Dict[str, str],
    selected_fields: Dict[str, List[str]],
    ph_event_col_expr: str = None
) -> str:
    """
    Builds a SQL expression for the 'what_matched' column.
    
    Args:
        ph_event_col_expr: The NAME of the column (e.g. 'ph.event_match_str') OR expression from the subquery.
    """
    
    all_case_parts = []

    # 1. Add PH Entry First (if exists)
    if ph_event_col_expr:
        # Since ph_event_col_expr is a pre-calculated string from the subquery,
        # checking IS NOT NULL is safe and simple scalar logic.
        all_case_parts.append(f"CASE WHEN {ph_event_col_expr} IS NOT NULL THEN CONCAT('PH Event Match: ', {ph_event_col_expr}) ELSE NULL END")

    # 2. Add Standard Matches
    for param_name, input_str in params.items():
        if not input_str or not input_str.strip():
            continue

        if param_name == "ph_entry":
            continue

        columns = selected_fields.get(param_name)
        if not columns:
            columns = PARAM_COLUMN_MAPPING.get(param_name, [])
        if not columns:
            continue

        values = parse_multi_value_input(input_str)
        if not values:
            continue

        match_type = match_types.get(param_name, "contains")

        for col in columns:
            col_cond = build_column_conditions(
                [col], values, match_type=match_type, table_alias="tas"
            )

            label = col.replace("_", " ").title()

            case_part = (
                f"CASE WHEN {col_cond} THEN CONCAT('{label}: ', tas.{col}) ELSE NULL END"
            )
            all_case_parts.append(case_part)

    # 3. Construct the SQL Expression
    if not all_case_parts:
        return "CAST(tas.serial_number AS STRING)" # Fallback

    # Concatenate all parts with pipe delimiter
    combined_expr = f"CONCAT_WS(' | ', {', '.join(all_case_parts)})"
    
    # NULLIF ensures empty string becomes NULL, COALESCE ensures we fallback to serial number
    return f"COALESCE(NULLIF({combined_expr}, ''), CAST(tas.serial_number AS STRING))"


# =============================================================================
# Main Builder
# =============================================================================

def build_advanced_search_query(
    params: Dict[str, str],
    operator: str,
    catalog: str,
    schema: str,
    table: str = "tdet_app_search",
    limit: Optional[int] = None,
    match_types: Optional[Dict[str, str]] = None,
    user_email: str = "",
    selected_fields: Optional[Dict[str, List[str]]] = None,
    tmngpdb_catalog: str = None,
) -> Tuple[str, bool, List[str]]:

    if match_types is None:
        match_types = {}
    if selected_fields is None:
        selected_fields = {}

    param_conditions = []
    active_params = []
    
    ph_join_sql = ""
    ph_event_col_ref = None # Will hold the subquery column name 'ph.event_match_str'
    has_ph_search = False

    for param_name, input_str in params.items():
        if not input_str.strip():
            continue

        values = parse_multi_value_input(input_str)
        if not values:
            continue
        match_type = match_types.get(param_name, "contains")

        # ---------------------------------------------------------
        # 1. SPECIAL HANDLING: PH Entry
        # ---------------------------------------------------------
        if param_name == "ph_entry":
            if not tmngpdb_catalog:
                st.error("Configuration Error: 'tmngpdb_catalog' is required for PH Entry search.")
                raise ValueError("Missing tmngpdb_catalog")

            ph_cols = ["sber.business_event_reason_cd"]
            ph_where_cond = build_column_conditions(
                ph_cols, values, match_type=match_type
            )
            
            if ph_where_cond:
                has_ph_search = True
                active_params.append(param_name)
                
                # Logic for the aggregation CASE statement
                case_logic = generate_event_match_case_logic(
                    input_str, match_type, 
                    col_ref="sber.business_event_reason_cd", 
                    date_ref="CAST(be.effective_ts AS DATE)"
                )
                
                # Pre-aggregated subquery with correct catalog
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
                
                ph_join_sql = f"INNER JOIN {ph_subquery} ON CAST(tas.serial_number AS STRING) = ph.serial_number"
                ph_event_col_ref = "ph.event_match_str"
                
            continue

        # ---------------------------------------------------------
        # 2. STANDARD HANDLING
        # ---------------------------------------------------------
        columns = selected_fields.get(param_name) or PARAM_COLUMN_MAPPING.get(param_name, [])
        if not columns:
            continue

        condition = build_column_conditions(
            columns, values, match_type=match_type, table_alias="tas"
        )
        if condition:
            param_conditions.append(condition)
            active_params.append(param_name)

    # Generate Unified 'what_matched' column logic
    what_matched_col = build_what_matched_expression(
        params, match_types, selected_fields, ph_event_col_expr=ph_event_col_ref
    )

    created_time = datetime.utcnow()
    current_time = created_time.strftime("%Y-%m-%d %H:%M:%S")

    # Base Select List
    select_list = f"""
            tas.serial_number,
            tas.mark_tx,
            tas.filing_date,
            tas.filed_bases,
            tas.current_bases,
            tas.registration_number,
            tas.registration_date,
            tas.owner_name,
            tas.owner_name_historical,
            tas.owner_address,
            tas.owner_country,
            tas.owner_email,
            tas.owner_email_historical,
            tas.owner_phone,
            tas.attorney_membership_number,
            tas.attorney_name,
            tas.attorney_name_historical,
            tas.attorney_address,
            tas.attorney_email,
            tas.attorney_email_historical,
            tas.attorney_phone,
            tas.correspondent_name,
            tas.correspondent_name_historical,
            tas.correspondent_address,
            tas.correspondent_email,
            tas.correspondent_email_secondary,
            tas.correspondent_email_historical,
            tas.correspondent_phone,
            tas.domestic_representative_name,
            tas.domestic_representative_name_historical,
            tas.domestic_representative_email,
            tas.domestic_representative_email_historical,
            tas.domestic_representative_phone,
            tas.examiner_number,
            tas.examiner_name,
            tas.docket_number,
            tas.firm_name,
            tas.law_office,
            tas.class_list,
            tas.status,
            tas.status_date,
            tas.og_issue_date,
            tas.og_status,
            tas.og_category,
            tas.international_registration_number,
            tas.international_us_reference_number,
            tas.specimen_url,
            {what_matched_col} AS what_matched,
            tas._created_date AS created_date,
            TIMESTAMP('{current_time}') AS _created_timestamp
    """

    base_query = f"""
        SELECT {select_list}
        FROM {catalog}.{schema}.{table} AS tas
        {ph_join_sql}
        WHERE tas._is_record_active = true
    """
    
    if param_conditions:
        combined = f" {operator.upper()} ".join(param_conditions)
        query = f"{base_query}\n  AND ({combined})"
    else:
        query = base_query

    query += "\nORDER BY serial_number ASC"

    if limit is not None:
        query += f"\nLIMIT {int(limit)}"

    return query, (len(active_params) > 0), active_params


def validate_search_params(params: Dict[str, str]) -> Tuple[bool, str]:
    has_value = False
    for param_name, input_str in params.items():
        if input_str and input_str.strip():
            values = parse_multi_value_input(input_str)
            if values:
                has_value = True
                break

    if not has_value:
        return False, "Please enter at least one search parameter."
    return True, ""


# =============================================================================
# Helper: Format Criteria String (Shared by Comments and Summary)
# =============================================================================

def _format_criteria_string(
    params: Dict[str, str],
    match_types: Dict[str, str],
    selected_fields: Dict[str, List[str]],
) -> List[str]:
    """Helper to build list of formatted criteria strings."""
    active_criteria = []

    # Reverse mapping to look up Display Label from column name
    col_to_label = {}
    for param_key, options in PARAM_FIELD_OPTIONS.items():
        for label, col_name in options.items():
            col_to_label[col_name] = label

    for param_name, input_str in params.items():
        if not input_str or not input_str.strip():
            continue

        main_label = PARAM_LABELS.get(param_name, param_name)
        values = parse_multi_value_input(input_str)
        if not values:
            continue
        values_str = ", ".join(values)

        m_type_key = match_types.get(param_name, "contains")
        m_type_label = MATCH_TYPE_LABELS.get(m_type_key, m_type_key.title())

        current_cols = selected_fields.get(param_name, [])
        subset_str = ""

        if param_name in PARAM_FIELD_OPTIONS:
            labels = sorted(col_to_label.get(c, c) for c in current_cols)
            if not labels:
                subset_str = " (None selected)"
            else:
                subset_str = f" ({', '.join(labels)})"

        entry = f"{main_label}: {values_str}{subset_str} [{m_type_label}]"
        active_criteria.append(entry)

    return active_criteria


def get_active_params_summary_string(
    params: Dict[str, str],
    operator: str,
    limit: Optional[int] = None,
    match_types: Optional[Dict[str, str]] = None,
    selected_fields: Optional[Dict[str, List[str]]] = None,
) -> str:
    """Returns the formatted summary string."""
    if match_types is None:
        match_types = {}
    if selected_fields is None:
        selected_fields = {}

    active_list = _format_criteria_string(params, match_types, selected_fields)
    summary_text = f" {operator} ".join(active_list)

    if limit:
        summary_text += f" | **Limit**: {limit:,} records"

    return summary_text


# =============================================================================
# Comment and Summary Builders
# =============================================================================

def render_active_params_summary(
    params: Dict[str, str],
    operator: str,
    limit: Optional[int] = None,
    match_types: Optional[Dict[str, str]] = None,
    selected_fields: Optional[Dict[str, List[str]]] = None,
):
    """Display a summary of active search parameters with full detail."""
    if match_types is None:
        match_types = {}
    if selected_fields is None:
        selected_fields = {}

    active_list = _format_criteria_string(params, match_types or {}, selected_fields or {})

    if active_list:
        st.markdown("##### Active Search Criteria")
        summary_text = f" {operator} ".join(active_list)

        if limit:
            summary_text += f" | **Limit**: {limit:,} records"

        st.info(summary_text)


def build_advanced_search_comments(
    params: Dict[str, str],
    operator: str,
    limit: Optional[int] = None,
    match_types: Optional[Dict[str, str]] = None,
    selected_fields: Optional[Dict[str, List[str]]] = None,
) -> str:
    """Build detailed comments string."""
    if match_types is None:
        match_types = {}
    if selected_fields is None:
        selected_fields = {}

    active_list = _format_criteria_string(params, match_types, selected_fields)

    if not active_list:
        base_comment = "Advanced Search - "
    else:
        criteria_str = f" {operator} ".join(active_list)
        base_comment = f"Advanced Search - {criteria_str}"

    if limit:
        base_comment += f" | Limit #: {limit:,}"

    if len(base_comment) > 1000:
        base_comment = base_comment[:997] + "..."

    return base_comment


# =============================================================================
# Query Execution & History
# =============================================================================

def run_advanced_search(
    cursor,
    params: Dict[str, str],
    operator: str,
    catalog: str,
    schema: str = "silver",
    table: str = "tdet_app_search",
    limit: Optional[int] = None,
    match_types: Optional[Dict[str, str]] = None,
    user_email: str = "",
    selected_fields: Optional[Dict[str, List[str]]] = None,
    tmngpdb_catalog: str = None,
) -> Tuple[pd.DataFrame, str, List[str]]:

    query, has_conditions, active_params = build_advanced_search_query(
        params,
        operator,
        catalog,
        schema,
        table,
        limit,
        match_types,
        user_email,
        selected_fields,
        tmngpdb_catalog=tmngpdb_catalog,
    )

    if not has_conditions:
        return pd.DataFrame(), query, []

    try:
        cursor.execute(query)
        try:
            arrow_table = cursor.fetchall_arrow()
            df = arrow_table.to_pandas()
        except AttributeError:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)

        return df, query, active_params
    except Exception as e:
        import traceback

        st.error(f"DEBUG: {traceback.format_exc()}")
        st.error("Failed to generate advanced search results.")
        raise RuntimeError(f"Advanced search query failed: {e}")


def save_advanced_search_history(
    cursor,
    params: Dict[str, str],
    operator: str,
    comments: str,
    matter_number: str,
    result_df: pd.DataFrame,
    user_name: str,
    user_email: str,
    catalog: str,
    schema: str,
    table_configs: dict,
    match_types: Dict[str, str] = None,
    selected_fields: Dict[str, List[str]] = None,
    progress_callback=None,
    tmngpdb_catalog: str = None,  # REQUIRED for PH Search
) -> Tuple[str, str]:

    start_time = datetime.utcnow()
    search_id = uuid.uuid4().hex
    created_time = datetime.utcnow()
    output_file_name = f"tdet_adv_{created_time.strftime('%Y%m%d_%H%M%S')}.xlsx"

    comments_clean = sql_escape((comments or "").strip()[:1000])
    matter_number_clean = sql_escape((matter_number or "").strip()[:250])
    user_name_clean = sql_escape(user_name)
    user_email_clean = sql_escape(user_email)
    num_records = len(result_df)

    table_file_history = table_configs.get("file_history", "tdet_app_file_history")
    table_search_history = table_configs.get("search_history", "tdet_app_search_history")
    table_search_detail = table_configs.get("search_detail", "tdet_app_search_history_detail")

    # Create config JSON
    config_data = {
        "type": "ADVANCED",
        "params": params,
        "match_types": match_types,
        "selected_fields": selected_fields,
        "operator": operator,
        "limit": None,
    }
    config_json = sql_escape(json.dumps(config_data))

    if progress_callback:
        progress_callback(0.01, num_records, "Initializing search history...")

    # Header
    history_sql = f"""
    INSERT INTO {catalog}.{schema}.{table_file_history}
    (search_id, matter_number, comments, record_count, input_file_name, output_file_name,
     created_user_name, created_user_email, created_timestamp, search_config_json)
    VALUES
    ('{search_id}', '{matter_number_clean}', '{comments_clean}', '{num_records}',
     'ADVANCED_SEARCH', '{output_file_name}', '{user_name_clean}', 
     '{user_email_clean}', '{created_time.strftime("%Y-%m-%d %H:%M:%S")}', '{config_json}')
    """
    cursor.execute(history_sql)

    if num_records > 0:
        if progress_callback:
            progress_callback(0.05, num_records, f"Linking {num_records:,} records to history...")

        ts_str = created_time.strftime("%Y-%m-%d %H:%M:%S")

        serials = result_df["serial_number"].astype(str).values
        # Unique UUID per row
        row_ids = [str(uuid.uuid4()) for _ in range(len(serials))]

        values_list = [
            f"('{rid}', '{search_id}', '{sql_escape(sn)}', 'ADVANCED_SEARCH', '{user_email_clean}', '{ts_str}')"
            for rid, sn in zip(row_ids, serials)
        ]

        chunk_size = 5000
        total_batches = (num_records + chunk_size - 1) // chunk_size
        records_processed = 0
        base_insert_sql = (
            f"INSERT INTO {catalog}.{schema}.{table_search_history} "
            f"(id, search_id, serial_number, input_file_name, created_user_email, created_timestamp) VALUES "
        )

        for i in range(total_batches):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size
            chunk = values_list[start_idx:end_idx]

            cursor.execute(base_insert_sql + ",".join(chunk))

            records_processed += len(chunk)
            if progress_callback:
                now = datetime.utcnow()
                elapsed = (now - start_time).total_seconds()

                if elapsed > 0:
                    rate = records_processed / elapsed
                    remaining = num_records - records_processed
                    eta_seconds = remaining / rate if rate > 0 else 0
                    eta_time = now + timedelta(seconds=eta_seconds)

                    elapsed_str = str(timedelta(seconds=int(elapsed)))
                    eta_str = eta_time.strftime("%H:%M:%S UTC")

                    status_msg = (
                        f"**Linking:** {records_processed:,} / {num_records:,} records\n\n"
                        f"⏱️ **Start:** {start_time.strftime('%H:%M:%S UTC')} | "
                        f"⏳ **Elapsed:** {elapsed_str} | "
                        f"🏁 **Est. End:** {eta_str}"
                    )
                else:
                    status_msg = f"Linking batch {i+1}/{total_batches}..."

                frac = 0.05 + ((records_processed / num_records) * 0.75)
                progress_callback(frac, num_records, status_msg)

        # RECONSTRUCTING PH LOGIC FOR DETAIL INSERT
        ph_event_col_ref = None
        ph_join_sql = ""
        
        # USE THE PASSED CATALOG
        ph_cat = tmngpdb_catalog 

        for param_name, input_str in params.items():
            if param_name == "ph_entry" and input_str.strip():
                # Safety check
                if not ph_cat:
                    st.error("Configuration Error: 'tmngpdb_catalog' is missing for PH Entry search history save.")
                    raise ValueError("Missing tmngpdb_catalog in save_advanced_search_history")

                match_type = match_types.get(param_name, "contains")
                ph_cols = ["sber.business_event_reason_cd"]
                values = parse_multi_value_input(input_str)
                ph_where_cond = build_column_conditions(ph_cols, values, match_type=match_type)
                
                if ph_where_cond:
                    # Same pre-aggregation strategy as build_advanced_search_query
                    case_logic = generate_event_match_case_logic(
                        input_str, match_type, 
                        col_ref="sber.business_event_reason_cd", 
                        date_ref="CAST(be.effective_ts AS DATE)"
                    )
                    
                    ph_subquery = f"""
                    (
                        SELECT 
                            CAST(split(be.cfk_object_gid, ':')[2] AS STRING) AS serial_number,
                            array_join(collect_set({case_logic}), '; ') AS event_match_str
                        FROM {ph_cat}.bronze.business_event be
                        INNER JOIN {ph_cat}.bronze.stnd_business_event_reason sber
                        ON be.fk_business_event_reason_id = sber.business_event_reason_id
                        WHERE {ph_where_cond}
                        GROUP BY CAST(split(be.cfk_object_gid, ':')[2] AS STRING)
                    ) AS ph
                    """
                    
                    ph_join_sql = f"INNER JOIN {ph_subquery} ON CAST(tas.serial_number AS STRING) = ph.serial_number"
                    ph_event_col_ref = "ph.event_match_str"

        # Re-build expression with PH logic included (using column reference)
        what_matched_sql = build_what_matched_expression(
            params, match_types or {}, selected_fields or {}, ph_event_col_expr=ph_event_col_ref
        )

        if progress_callback:
            progress_callback(
                0.85, num_records, "Populating final detail table (this may take a moment)..."
            )

        sql_path = APP_ROOT / "sql" / "tdet_app_search_history_detail.sql"
        try:
            query_template = sql_path.read_text()
            detail_sql = query_template.format(
                tdet_catalog=catalog,
                tdet_schema=schema,
                table_search_history=table_search_history,
                table_search_detail=table_search_detail,
                search_id=sql_escape(search_id),
                output_file_name=sql_escape(output_file_name),
                what_matched_logic=what_matched_sql,
            )
            
            # Inject PH JOIN for the INSERT statement too
            if ph_join_sql:
                # Robust injection using Regex to find the FROM clause
                target_regex = r"FROM\s+[\w\.]+\s+tas"
                match = re.search(target_regex, detail_sql, re.IGNORECASE)
                
                if match:
                    target_str = match.group(0)
                    detail_sql = detail_sql.replace(target_str, f"{target_str}\n{ph_join_sql}")
                else:
                    # Fallback
                    fallback_str = f"FROM {catalog}.silver.tdet_app_search tas"
                    if fallback_str in detail_sql:
                        detail_sql = detail_sql.replace(fallback_str, f"{fallback_str}\n{ph_join_sql}")
                    else:
                        st.warning("Warning: Could not inject PH Entry JOIN. Results may be incomplete.")

            cursor.execute(detail_sql)
        except Exception as e:
            st.error(f"Failed to execute detail population SQL: {e}")
            raise

    # Cleanup for export (drop hashes)
    cols_to_drop = [
        "_natural_key_hash", "_record_data_hash",
        "natural_key_hash", "record_data_hash",
    ]
    result_df.drop(
        columns=[c for c in cols_to_drop if c in result_df.columns], inplace=True
    )

    if progress_callback:
        progress_callback(1.0, num_records, "Complete!")

    return search_id, output_file_name


# =============================================================================
# UI Rendering
# =============================================================================

def _reset_advanced_search_callback():
    """Callback for Reset button."""
    params_to_reset = [
        "name", "email", "phone", "mailing_address",
        "url", "status", "attorney_membership_number", "ph_entry", "docket_number",
    ]

    for p_key in params_to_reset:
        st.session_state[f"adv_{p_key}"] = ""
        st.session_state[f"adv_match_{p_key}"] = "Contains"

        if p_key in PARAM_FIELD_OPTIONS:
            all_options = list(PARAM_FIELD_OPTIONS[p_key].keys())
            st.session_state[f"adv_fields_{p_key}"] = all_options

    st.session_state["adv_operator"] = "OR"
    st.session_state["adv_limit"] = None
    st.session_state["adv_comments_user_edited"] = False
    st.session_state["adv_matter_number"] = None


def render_advanced_search_form() -> Tuple[
    Dict[str, str], Dict[str, str], Dict[str, List[str]], str, Optional[int]
]:
    """Render the advanced search form in Streamlit."""
    col_header, col_reset = st.columns([4, 1])
    with col_header:
        st.markdown("#### <u>Search Parameters</u>", unsafe_allow_html=True)
        st.caption(
            "Enter one or more values per field. "
            "Separate multiple values with: **' | '** or **' , '** or **' ; '** or **' / '**"
        )
        st.caption("Example: `John Doe; Jane Smith| Jim Thorpe`")

    with col_reset:
        st.button(
            "Reset Criteria",
            key="adv_reset_criteria",
            type="primary",
            help="Clear all search parameters to defaults",
            on_click=_reset_advanced_search_callback,
        )

    params: Dict[str, str] = {}
    match_types: Dict[str, str] = {}
    selected_fields: Dict[str, List[str]] = {}

    match_label_to_value = {
        "Contains": "contains",
        "Exact": "exact",
        "Starts with": "starts_with",
        "Fuzzy": "fuzzy",
    }

    col1, col2 = st.columns(2)

    def render_param_block(param_key, label, placeholder, default_match_index=0):
        with st.expander(label, expanded=False):
            st.caption(PARAM_HELP.get(param_key, ""))

            field_options = PARAM_FIELD_OPTIONS.get(param_key)
            if field_options:
                options = list(field_options.keys())

                if f"adv_fields_{param_key}" not in st.session_state:
                    st.session_state[f"adv_fields_{param_key}"] = options

                selected_labels = st.multiselect(
                    f"Search specific {label} fields:",
                    options=options,
                    key=f"adv_fields_{param_key}",
                    help="Uncheck fields to exclude them from the search.",
                )
                selected_cols = [field_options[lbl] for lbl in selected_labels]
                selected_fields[param_key] = selected_cols
            else:
                selected_fields[param_key] = PARAM_COLUMN_MAPPING.get(param_key, [])

            if f"adv_{param_key}" not in st.session_state:
                st.session_state[f"adv_{param_key}"] = ""

            params[param_key] = st.text_input(
                "Values",
                key=f"adv_{param_key}",
                placeholder=placeholder,
            )

            if f"adv_match_{param_key}" not in st.session_state:
                st.session_state[f"adv_match_{param_key}"] = "Contains"

            match_label = st.selectbox(
                f"Match ({label})",
                options=list(match_label_to_value.keys()),
                index=default_match_index,
                key=f"adv_match_{param_key}",
            )
            match_types[param_key] = match_label_to_value[match_label]

    # LEFT COLUMN
    with col1:
        render_param_block("name", "Name", "e.g., John Doe; Jane Smith")
        render_param_block("email", "Email", "e.g., john@example.com, jane@example.com")
        render_param_block("phone", "Phone", "e.g., 555-1234; 555-5678", default_match_index=1)
        render_param_block("mailing_address", "Mailing Address", "e.g., 123 Main St; New York")

    # RIGHT COLUMN
    with col2:
        render_param_block("url", "URL (Specimen)", "e.g., example.com")
        render_param_block("status", "Status", "e.g., REGISTERED; PENDING")
        render_param_block(
            "attorney_membership_number",
            "Attorney Membership Number",
            "e.g., 12345",
            default_match_index=1,
        )
        render_param_block("ph_entry", "PH Entry", "e.g., BARR; BARX")
        render_param_block("docket_number", "Docket Number", "e.g., ABC-123")

    # Settings Row
    st.markdown("#### <u>Search Settings</u>", unsafe_allow_html=True)
    col_op, col_div, col_lim = st.columns([2, 0.1, 2])

    with col_op:
        st.caption("Combine Parameters With")
        if "adv_operator" not in st.session_state:
            st.session_state["adv_operator"] = "OR"

        operator = st.radio(
            "Operator",
            options=["OR", "AND"],
            horizontal=True,
            key="adv_operator",
            help="**OR**: Match ANY parameter. **AND**: Match ALL parameters.",
            label_visibility="collapsed",
        )

    with col_div:
        vertical_divider(height=60, color=(210, 210, 210), width=2)

    with col_lim:
        st.caption("Limit Number of Records (Optional)")
        if "adv_limit" not in st.session_state:
            st.session_state["adv_limit"] = None

        limit_input = st.number_input(
            "Limit",
            min_value=1,
            step=1,
            key="adv_limit",
            label_visibility="collapsed",
            placeholder="e.g. 10000",
        )

    limit = int(limit_input) if limit_input else None

    return params, match_types, selected_fields, operator, limit
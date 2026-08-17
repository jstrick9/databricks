import os
import time
from pathlib import Path
from datetime import datetime, timedelta
import pytz
import yaml
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient


# -------------------------------
# READ CONFIG FILES
# -------------------------------
def read_yaml(file_path: str):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def _app_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _get_env(default: str = "lab") -> str:
    return os.getenv("ENVIRONMENT", default)


def _get_configs(dbx_env: str):
    cfg_path = _app_root() / "config" / dbx_env / "ph_config.yaml"

    if not cfg_path.exists():
        raise FileNotFoundError(f"Configuration file not found for environment: {dbx_env}")

    try:
        cfg = read_yaml(str(cfg_path))
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in configuration file: {e}")

    if "schema" not in cfg:
        raise KeyError("Missing required config section: schema")
    if "catalog" not in cfg["schema"]:
        raise KeyError("Missing required config key: schema.catalog")

    return cfg


def _normalize_host(host: str | None) -> str | None:
    if not host:
        return None
    return host.replace("https://", "").replace("http://", "").rstrip("/")


# -------------------------------
# SQL VALUE ESCAPING
# -------------------------------
def sql_escape(value) -> str:
    """Escape a value for safe SQL string interpolation."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "''")


# -------------------------------
# TEMP MESSAGES
# -------------------------------
def show_temp_message(message_type, message, seconds=3):
    placeholder = st.empty()
    getattr(placeholder, message_type, placeholder.info)(message)
    time.sleep(seconds)
    placeholder.empty()


# -------------------------------
# DB CONNECTION WITH AUTO-RECONNECT
# -------------------------------
def _resolve_http_path_by_name(w: WorkspaceClient, name: str) -> str:
    all_warehouses = list(w.warehouses.list())
    matches = [wh for wh in all_warehouses if (wh.name or "").strip().lower() == name.strip().lower()]
    if not matches:
        visible_names = [f"'{wh.name}' (ID: {wh.id})" for wh in all_warehouses] if all_warehouses else ["None - App identity has 0 visible warehouses!"]
        raise ValueError(
            f"No SQL Warehouse found matching name '{name}'.\n"
            f"Visible warehouses to this app: {', '.join(visible_names)}.\n"
            f"👉 FIX: In Databricks UI, go to SQL Warehouses -> '{name}' -> Permissions -> Grant 'CAN USE' to the application's Service Principal or app group!"
        )
    if len(matches) > 1:
        ids = ", ".join(getattr(m, "id", "unknown") for m in matches)
        raise ValueError(f"Multiple SQL Warehouses match name '{name}'. Matches: {ids}")
    wh = matches[0]
    http_path = getattr(wh.odbc_params, "http_path", None) or getattr(wh.odbc_params, "path", None)
    if not http_path:
        raise ValueError(f"Warehouse '{name}' has no http_path")
    return http_path


def _create_fresh_connection():
    """
    Create a new SQL Warehouse connection.
    Uses the app's service principal (OAuth) for auth.
    """
    w = WorkspaceClient()

    host = _normalize_host(w.config.host)
    if not host:
        raise ValueError("Unable to determine Databricks host")

    wname = os.getenv("DATABRICKS_WAREHOUSE_NAME")
    if not wname:
        raise ValueError("DATABRICKS_WAREHOUSE_NAME environment variable not set")

    http_path = _resolve_http_path_by_name(w, wname)

    headers = w.config.authenticate()
    token = headers.get("Authorization", "").split(" ", 1)[-1]
    if not token:
        raise ValueError("Failed to obtain OAuth token from WorkspaceClient")

    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


def _is_connection_alive(conn) -> bool:
    """Check if the connection is still valid with a lightweight query."""
    if conn is None:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        return True
    except Exception:
        return False


def _close_connection_safely(conn):
    """Close a connection without raising errors."""
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def get_connection(max_retries: int = 3, retry_delay_seconds: int = 2):
    """
    Get Databricks SQL Warehouse connection with auto-reconnect.

    - Checks if cached connection is alive
    - Creates a new connection if stale
    - Retries with backoff on failure

    Returns: (connection, cursor) tuple
    """
    # Check for existing cached connection
    cached_conn = st.session_state.get("_db_connection")

    if cached_conn is not None and _is_connection_alive(cached_conn):
        return cached_conn, cached_conn.cursor()

    # Connection is stale or missing; close old one and reconnect
    if cached_conn is not None:
        _close_connection_safely(cached_conn)
        st.session_state.pop("_db_connection", None)

    status_placeholder = st.empty()

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            status_placeholder.info(f"🔄 Connecting to SQL Warehouse... (attempt {attempt}/{max_retries})")

            conn = _create_fresh_connection()

            if _is_connection_alive(conn):
                st.session_state["_db_connection"] = conn
                status_placeholder.empty()
                return conn, conn.cursor()
            else:
                raise ValueError("Connection created but health check failed")

        except Exception as e:
            last_error = e
            if attempt < max_retries:
                wait_time = retry_delay_seconds * attempt
                status_placeholder.warning(f"⏳ Retrying in {wait_time}s... ({attempt}/{max_retries})")
                time.sleep(wait_time)

    # All retries exhausted
    status_placeholder.empty()
    st.error("❌ Failed to connect to Databricks SQL Warehouse.")
    with st.expander("🔍 Technical Details"):
        st.code(str(last_error))
    st.info("Please try refreshing the page. If the issue persists, contact support.")
    st.stop()


def clear_connection_cache():
    """Manually clear the cached connection."""
    cached_conn = st.session_state.get("_db_connection")
    if cached_conn is not None:
        _close_connection_safely(cached_conn)
        st.session_state.pop("_db_connection", None)


# -------------------------------
# SOURCE TABLE VALIDATION
# -------------------------------
def validate_source_table(cursor, configs):
    """Verify ETL source table exists and contains recent data."""
    catalog = configs["schema"].get("catalog")
    schema = configs["schema"].get("schema", "silver")
    table = configs["schema"].get("table", "prosecution_history")

    full_table_name = f"{catalog}.{schema}.{table}"

    try:
        cursor.execute(f"""
            SELECT 
                COUNT(*) as record_count,
                MAX(ph_action_date) as latest_data_date,
                COUNT(DISTINCT serial_number) as unique_serials
            FROM {full_table_name}
        """)
        result = cursor.fetchone()

        if not result:
            st.error(f"Unable to query source table: {full_table_name}")
            st.info("Please contact the data team.")
            return False

        record_count, latest_date, unique_serials = result

        if record_count == 0:
            st.error("⚠️ Source table exists but contains no records.")
            st.warning(f"**Table:** {full_table_name}")
            st.info("ETL may not have run yet. Please contact the data team.")
            return False

        if latest_date:
            if isinstance(latest_date, str):
                latest_date = datetime.strptime(latest_date[:10], "%Y-%m-%d").date()
            elif hasattr(latest_date, "date"):
                latest_date = latest_date.date()

            days_old = (datetime.now().date() - latest_date).days
            expected_freshness_days = 1  # 24 hours expected freshness

            st.success(f"✅ Source table validated: `{full_table_name}`")

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Records", f"{record_count:,}")
            with col2:
                st.metric("Unique Serial Numbers", f"{unique_serials:,}")

            if days_old <= expected_freshness_days:
                st.success(f"📅 **Data Freshness:** Current (last update: {latest_date})")
            elif days_old <= 7:
                st.warning(f"⚠️ **Data Freshness:** {days_old} day(s) old (last update: {latest_date})")
                st.info("Data is slightly stale but usable. ETL may be delayed.")
            else:
                st.error(f"❌ **Data Freshness:** {days_old} day(s) old (last update: {latest_date})")
                st.warning("Data is significantly stale. Please contact the data team before proceeding.")

            et_tz = pytz.timezone("America/New_York")
            current_time_et = datetime.now(et_tz)
            etl_start_time = datetime.strptime("04:00", "%H:%M").time()
            etl_end_time = datetime.strptime("04:45", "%H:%M").time()

            if etl_start_time <= current_time_et.time() <= etl_end_time:
                st.info("🔄 **ETL Status:** Daily refresh in progress (4:00-4:45 AM ET)")
                st.info(f"**Current Time (ET):** {current_time_et.strftime('%I:%M %p')}")
                st.caption("Data may be refreshing. For most up-to-date results, try again after 4:45 AM ET.")
            else:
                next_etl = datetime.combine(datetime.now().date(), etl_start_time)
                if current_time_et.time() > etl_start_time:
                    next_etl += timedelta(days=1)

                next_etl_et = et_tz.localize(next_etl)
                st.info(f"⏰ **Next ETL Run:** {next_etl_et.strftime('%Y-%m-%d at %I:%M %p ET')}")

        return True

    except Exception as e:
        st.error("❌ Required source data table not found or inaccessible")
        st.error(f"**Expected Table:** `{full_table_name}`")
        st.info("Please verify Unity Catalog table permissions for the application Service Principal.")
        with st.expander("🔍 Technical Details (for support)"):
            st.code(str(e))
        return False

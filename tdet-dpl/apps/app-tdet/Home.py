import os
import streamlit as st
from pathlib import Path
from utils.page_config_helpers import (
    setup_sidebar,
    set_page_config,
    vertical_divider,
)
from utils.runtime_env import get_runtime_env
from utils.db_helpers import (
    get_connection,
    show_temp_message,
    read_yaml,
    validate_source_table,
)
from utils.user_helpers import init_user_session_state

# -------------------------------
# HOME PAGE
# -------------------------------
set_page_config(page_title="Home | Trademark Data Extraction Tool (TDET)")
setup_sidebar()

st.title("Trademark Data Extraction Tool (TDET)")
st.markdown(
    """
Welcome to the **Trademark Data Extraction Tool (TDET)**.  
Use the left sidebar to navigate/access:
- 🔍 **Search** — 
    - Run a Basic Search (upload serial numbers file).
    - Hybrid Search (upload serial numbers file and apply advanced filters).
    - Advanced Search (search by Name, Email, Phone, Address, and more).  
    - Save your search criteria as presets to quickly load and run frequently used queries.
- 📜 **History** — View past searches, download original input/output files, and re-run any past search against the latest data. 
- 📘 **Help** — Access the TDET user guide in a downloadable pdf format
- 📥 **Download** — Downloadable template file used to upload serial numbers on the Search page 
"""
)

# ========== SSO User login ==========
sso_user = init_user_session_state()

# ========== Environment & Config ==========
dbx_env = get_runtime_env()
st.session_state["dbx_env"] = dbx_env

app_root = Path(__file__).resolve().parent
config_file = app_root / "config" / dbx_env / "tdet-conf.yaml"

try:
    configs = read_yaml(str(config_file))
    tdet_catalog = configs["schema"]["trgt_catalog"]
    tdet_schema = configs["schema"].get("trgt_schema", "gold")
    table_configs = configs["schema"].get("tables", {})
except FileNotFoundError:
    st.error(f"Configuration file not found for environment: {dbx_env}")
    st.error("Please contact support at ODBDDataLakeTeam@uspto.gov.")
    st.stop()
except KeyError as e:
    st.error(f"Invalid configuration: missing required key {e}")
    st.error("Please contact support at ODBDDataLakeTeam@uspto.gov.")
    st.stop()
except Exception:
    st.error("Configuration error occurred.")
    st.error("Please contact support at ODBDDataLakeTeam@uspto.gov.")
    st.stop()

conn, cursor = get_connection()
if not cursor:
    st.stop()

st.divider()
st.markdown(
    "### <u>Source Validation Information</u>", unsafe_allow_html=True
)
with st.expander("Click to View Source Info"):
    if not validate_source_table(cursor, configs):
        st.stop()

st.divider()
st.markdown("### <u>Search Options</u>", unsafe_allow_html=True)

col_1, col_div, col_2 = st.columns([1, 0.06, 1])

with col_1:
    st.markdown("#### 🔍 Basic Search")
    st.markdown(
        """
    Upload an Excel file (.xlsx) with serial numbers to retrieve trademark details.

    **Hybrid Feature:**
    You can apply **Advanced Filters** (Name, Status, etc.) to your uploaded file to narrow down the results.
    
    **Accepted column headers:**
    - serial_number
    - serial_num
    - ser_num
    - ser_number
    - sn
    """
    )

with col_div:
    vertical_divider(height=400, color=(210, 210, 210), width=2)

with col_2:
    st.markdown("#### 🔎 Advanced Search")
    st.markdown(
        """
    Search by one or more criteria without uploading a file.
    
    **Search parameters:**
    - Name (Owner, Attorney, Correspondent)
    - Email, Phone, Mailing Address
    - URL, Status, Attorney Membership Number
    - PH Entry / Docket Number
    
    **Features:**
    - Multi-value search using delimiters (| , ; /)
    - AND/OR operators to combine parameters
    """
    )

st.markdown("---")
if dbx_env == "lab":
    st.markdown("### Current Environment")
    env_colors = {"lab": "🟢", "prod": "🔴"}
    env_color = env_colors.get(dbx_env.lower(), "⚪")

    st.info(f"{env_color} **{dbx_env.upper()}**")
    st.caption(
        "Environment is determined by deployment configuration and cannot be changed."
    )
from pathlib import Path
import streamlit as st
from utils.page_config_helpers import setup_sidebar, set_page_config

# -------------------------------
# HELP PAGE 
# -------------------------------
def show_help():
    set_page_config(page_title="Help | Trademark Data Extraction Tool (TDET)")
    setup_sidebar()

    st.title("Help & User Guide")
    st.markdown("""
    This section provides guidance on how to use the TDET application.  
    Download the official User Guide below:
    """)

    # Resolve path relative to the app root: apps/app-tdet/file_template/tdet_user_guide.pdf
    app_root = Path(__file__).resolve().parent.parent
    template_path = app_root / "resources" / "file_template" / "tdet_user_guide.pdf"

    if not template_path.exists():
        st.error("Configuration error. Please contact support at ODBDDataLakeTeam@uspto.gov.")
        return

    # Read template in binary mode
    with open(template_path, "rb") as f:
        template_file = f.read()

    st.download_button(
        label="Download TDET User Guide (PDF)",
        data=template_file,
        file_name="tdet_user_guide.pdf",
        mime="application/pdf",
        type="primary",
        icon="📥",
    )

show_help()
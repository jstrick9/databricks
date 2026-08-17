import streamlit as st
from pathlib import Path
import numpy as np
from PIL import Image

APP_ROOT = Path(__file__).resolve().parent.parent

@st.cache_data
def load_sidebar_logo():
    logo_path = APP_ROOT / "resources" / "images" / "uspto_logo.png"
    if logo_path.exists():
        try:
            return Image.open(logo_path)
        except:
            return None
    return None

def vertical_divider(height=260, color=(200,200,200), width=2):
    arr = np.zeros((height, width, 3), dtype=np.uint8)
    arr[:,:] = color
    st.image(arr, width=width)

def setup_sidebar():
    with st.sidebar:
        logo = load_sidebar_logo()
        if logo:
            st.image(logo, use_column_width=True)
        st.markdown("### Trademark Processing Wait Times")
        st.caption("Self-service for processing wait times publishing")
        st.markdown("**📚 User Guides**")
        guide_path = APP_ROOT / "resources" / "trademark_wait_times_app_user_guide.docx"
        arch_path = APP_ROOT / "resources" / "trademark_wait_times_architecture.docx"
        if guide_path.exists():
            with open(guide_path, "rb") as f:
                st.download_button("📥 User Guide (docx)", f, file_name="trademark_wait_times_app_user_guide.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", key="sb_user_guide")
        if arch_path.exists():
            with open(arch_path, "rb") as f:
                st.download_button("📥 Architecture Doc (docx)", f, file_name="trademark_wait_times_architecture.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", key="sb_arch_doc")
        st.caption("Need help? Use Help page → downloads, or email ODBDDataLakeTeam@uspto.gov")

def set_page_config(page_title=None, page_icon=None):
    if page_icon is None:
        page_icon = "⏱️"
    st.set_page_config(
        page_title=page_title or "Wait Times",
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            # Changed from USPTO timeline URL to internal help - docx in resources per requirement
            "Get help": None,  # Help now via Help page download buttons, not external USPTO site
            "Report a bug": "mailto:ODBDDataLakeTeam@uspto.gov",
            "About": "USPTO Trademark Processing Wait Times - Self-service App. User guides in resources/ - Download via Home or Help page."
        }
    )

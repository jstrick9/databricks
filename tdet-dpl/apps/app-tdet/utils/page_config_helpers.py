import os
from pathlib import Path
import numpy as np
import streamlit as st
from PIL import Image
from utils.user_helpers import get_current_user

# Resolve app root: utils/.. => apps/app-tdet
APP_ROOT = Path(__file__).resolve().parent.parent

# -------------------------------
# UI HELPERS
# -------------------------------
def vertical_divider(height=260, color=(200, 200, 200), width=2):
    """Renders a thin vertical bar as an image."""
    arr = np.zeros((height, width, 3), dtype=np.uint8)
    arr[:, :] = color # RGB
    st.image(arr, width=width)

# -------------------------------
# RESOURCE CACHING
# -------------------------------
@st.cache_data
def load_sidebar_logo():
    """Load sidebar logo image with caching."""
    logo_path = APP_ROOT / "resources" / "images" / "uspto_logo.png"
    if logo_path.exists():
        try:
            return Image.open(logo_path)
        except Exception:
            return None
    return None

@st.cache_data
def load_excel_template():
    """Load Excel template with caching."""
    template_path = APP_ROOT / "resources" / "file_template" / "tdet_serial_number_search_sample_template.xlsx"
    if template_path.exists():
        try:
            with open(template_path, "rb") as f:
                return f.read()
        except Exception:
            return None
    return None

@st.cache_data
def load_page_icon():
    """Load page icon with caching."""
    icon_path = APP_ROOT / "resources" / "images" / "trademark_dna_icon.jpg"
    if icon_path.exists():
        try:
            return Image.open(icon_path)
        except Exception:
            return None
    return None

# -------------------------------
# SIDEBAR CONFIGURATION
# -------------------------------
def setup_sidebar():
    """Configure sidebar with logo, environment indicator, and template download."""
    with st.sidebar:
        # Sidebar download section
        st.markdown("### Downloadable Template File")
        st.caption("For your convenience, a downloadable template file is provided for reference.")

        template_data = load_excel_template()
        if template_data:
            st.download_button(
                label="Download Sample Excel Template",
                data=template_data,
                file_name="tdet_serial_number_search_sample_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                icon="📥",
            )
        else:
            st.info("Template currently unavailable.")

        st.markdown("---")
        # Logged-in user (from SSO headers)
        user = get_current_user()
        if user:
            st.markdown(
                f"#### 👤 **Logged in as:**  \n{user.get('display_name', user.get('email', 'Unknown'))}"
            )

        st.markdown("---")
        # Sidebar Logo
        logo = load_sidebar_logo()
        if logo:
            st.image(logo, width='stretch')
        else:
            st.markdown("### 🏛️ USPTO TDET") 

# -------------------------------
# WEBPAGE & MENU CONFIGURATION
# -------------------------------
def set_page_config(page_title=None, page_icon=None):
    """Configure page settings."""
    # Resolve default icon if none provided
    if page_icon is None:
        page_icon = load_page_icon()
        if page_icon is None:
            page_icon = "🔍"

    st.set_page_config(
        page_title=page_title,
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            "Get help": "https://usptogov.sharepoint.com/:b:/r/sites/O3G-TrademarkDataandAnalyticsProductGroup/Shared%20Documents/Trademark%20DnA%20Team/Training%20and%20How-To%20Guides/tdet_user_guide.pdf?csf=1&web=1&e=UNQojG",
            "Report a bug": "mailto:ODBDDataLakeTeam@uspto.gov?subject=TDET%20Bug%20Report&body=Please%20describe%20the%20issue...",
            "About": "Trademark Data Extraction Tool v1.0",
        },
    )
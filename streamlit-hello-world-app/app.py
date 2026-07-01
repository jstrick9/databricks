import os
import streamlit as st
from datetime import date
from pathlib import Path
from utils.db_helpers import get_connection, read_yaml
from utils.ph_helpers import run_ph_code_search, generate_excel, PRESET_ACTION_CODES

# --- SETUP ---
st.set_page_config(page_title="PH Code Search", layout="wide")
page = st.sidebar.selectbox("Navigate", ["Analysis", "Code Search"])  # Home page not shown

runtime_env = os.getenv("DB_APP_ENV", "dev")
config_path = Path(f"config/{runtime_env}/ph_config.yaml")
if not config_path.exists():
    st.error(f"Config not found at {config_path}")
    st.stop()

config = read_yaml(str(config_path))
CATALOG = config['schema']['catalog']
SCHEMA = config['schema']['schema']
TABLE = config['schema']['table']

# --- CUSTOM CSS ---
st.markdown("""
<style>
/* Page description styling */
.page-description {
    font-size: 1.1rem;
    font-weight: 600;
    color: #1f4e79;
    background: linear-gradient(135deg, #e8f4fd, #d6eaf8);
    padding: 14px 20px;
    border-left: 5px solid #2196F3;
    border-radius: 8px;
    margin-bottom: 20px;
}

/* Analysis card styling */
.analysis-card {
    background: #ffffff;
    border: 2px solid #e0e0e0;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
    transition: all 0.2s ease;
    cursor: pointer;
}
.analysis-card:hover {
    border-color: #2196F3;
    box-shadow: 0 4px 12px rgba(33, 150, 243, 0.2);
    transform: scale(1.02);
}
.analysis-card:active {
    transform: scale(1.04);
    border-color: #1565C0;
    box-shadow: 0 6px 16px rgba(21, 101, 192, 0.3);
}
.analysis-card h4 {
    margin: 0 0 6px 0;
    color: #1a237e;
    font-size: 1.05rem;
}
.analysis-card p {
    margin: 0;
    color: #555;
    font-size: 0.9rem;
}

/* Selected card */
.analysis-card-selected {
    background: #e3f2fd;
    border: 2px solid #1976D2;
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 12px;
    transform: scale(1.03);
    box-shadow: 0 4px 14px rgba(25, 118, 210, 0.25);
}
.analysis-card-selected h4 {
    margin: 0 0 6px 0;
    color: #0d47a1;
    font-size: 1.05rem;
}
.analysis-card-selected p {
    margin: 0;
    color: #333;
    font-size: 0.9rem;
    font-weight: 500;
}
</style>
""", unsafe_allow_html=True)

st.title(f"{config['app']['title']}")

if page == "Code Search":
    st.markdown('<div class="page-description">🔍 Search for trademark prosecution history records by selecting one or more action codes and a date range.</div>', unsafe_allow_html=True)
    st.markdown("""
    Select one or more **Prosecution History Action Codes** from the list below to retrieve matching records.
    """)
    with st.form("search_form"):
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            start_date = st.date_input("Start Date", value=date(2020, 10, 1), key="cs_start")
        with col_date2:
            end_date = st.date_input("End Date", value=date.today(), key="cs_end")
        selected_codes = st.multiselect(
            "Select Action Codes",
            options=PRESET_ACTION_CODES,
            default=None,
            help="Select one or multiple codes to filter the history table."
        )
        limit_val = st.number_input("Max Records", value=10000, step=1000)
        submitted = st.form_submit_button("Search", type="primary")

    if submitted:
        if not selected_codes:
            st.warning("Please select at least one Action Code.")
        else:
            conn, cursor = get_connection()
            if cursor:
                with st.spinner(f"Searching for {', '.join(selected_codes)}..."):
                    df = run_ph_code_search(
                        cursor, CATALOG, SCHEMA, TABLE, selected_codes, limit_val, start_date, end_date
                    )
                if not df.empty:
                    st.success(f"Found {len(df):,} records.")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    excel_data = generate_excel(df)
                    st.download_button(
                        label="📥 Download Excel",
                        data=excel_data,
                        file_name=f"ph_search_{len(df)}_rows.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )
                else:
                    st.info("No records found matching those codes.")

elif page == "Analysis":
    st.markdown('<div class="page-description">📊 Run pre-built analyses to explore patterns and relationships between prosecution history action codes over time.</div>', unsafe_allow_html=True)

    # Analysis options with descriptions
    analysis_options = {
        "TROA & EROI Frequency": "Shows the total count of TROA (Response to Office Action) and EROI (Extension Request) filings side by side in one table for the selected date range.",
        "TPDR \u2192 INCE \u2192 TROA Cases": "Finds cases where a Petition to the Director was filed, followed by an extension office action, and then a response to that office action.",
        "Response Within 6-Month NOA": "Lists all responses (TROA) or extensions (EROI) filed within 180 days of an extension-related office action being issued.",
        "Post-TROA Outcomes": "Shows what happens after an applicant responds to an office action, such as abandonment, suspension, or registration.",
        "Petition to Director Anomaly": "Flags potentially anomalous cases where a TEAS Petition to the Director (TPDR) was filed before an ITU extension office action (INCE) was issued, which then triggered a Response to Office Action (TROA) — an atypical sequence suggesting the petition may have been filed preemptively or out of the normal prosecution order."
    }

    # Initialize session state for selected analysis
    if "selected_analysis" not in st.session_state:
        st.session_state.selected_analysis = list(analysis_options.keys())[0]

    st.markdown("#### Select an Analysis")

    # Render clickable cards
    for title, desc in analysis_options.items():
        is_selected = st.session_state.selected_analysis == title
        card_class = "analysis-card-selected" if is_selected else "analysis-card"
        st.markdown(f'<div class="{card_class}"><h4>{title}</h4><p>{desc}</p></div>', unsafe_allow_html=True)
        if st.button(f"Select", key=f"btn_{title}", type="primary" if is_selected else "secondary"):
            st.session_state.selected_analysis = title
            st.rerun()

    analysis_type = st.session_state.selected_analysis

    st.divider()
    st.markdown(f"### 🛠️ Running: {analysis_type}")

    # Per-analysis date filters
    col_date1, col_date2 = st.columns(2)
    with col_date1:
        start_date = st.date_input("Start Date", value=date(2020, 10, 1), key=f"analysis_start_{analysis_type}")
    with col_date2:
        end_date = st.date_input("End Date", value=date.today(), key=f"analysis_end_{analysis_type}")

    # Post-TROA Outcomes filter
    POST_TROA_CODES = ['MAB6', 'ABN6S', 'ABN7S', 'ABN9O', 'SUPC', 'EXRA', 'DPCC']
    if analysis_type == "Post-TROA Outcomes":
        outcome_filter = st.multiselect(
            "Filter by Next Action Code",
            options=["All"] + POST_TROA_CODES,
            default=["All"],
            help="Select 'All' to see all outcomes, or pick specific action codes to filter."
        )
        if "All" in outcome_filter or not outcome_filter:
            selected_outcomes = POST_TROA_CODES
        else:
            selected_outcomes = outcome_filter

    conn, cursor = get_connection()
    if cursor:
        if analysis_type == "TROA & EROI Frequency":
            query = f"""
            SELECT
              ph_action_code AS `action code`,
              COUNT(*) AS `total filings`
            FROM {CATALOG}.{SCHEMA}.{TABLE}
            WHERE ph_action_code IN ('TROA', 'EROI')
              AND ph_action_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY ph_action_code
            ORDER BY ph_action_code
            """
        elif analysis_type == "TPDR \u2192 INCE \u2192 TROA Cases":
            query = f"""
            SELECT DISTINCT
              ph_tpdr.serial_number,
              ph_tpdr.ph_action_date AS `DATE OF TEAS PETITION TO DIRECTOR RECEIVED`,
              ph_ince.ph_action_date AS `DATE OF ITU OFFICE ACTION ISSUED FOR EXTENSION REQUEST `,
              ph_troa.ph_action_date AS `DATE OF TEAS RESPONSE TO OFFICE ACTION RECEIVED `
            FROM {CATALOG}.{SCHEMA}.{TABLE} ph_tpdr
            JOIN {CATALOG}.{SCHEMA}.{TABLE} ph_ince
              ON ph_tpdr.serial_number = ph_ince.serial_number
              AND ph_ince.ph_action_date > ph_tpdr.ph_action_date
            JOIN {CATALOG}.{SCHEMA}.{TABLE} ph_troa
              ON ph_tpdr.serial_number = ph_troa.serial_number
              AND ph_troa.ph_action_date > ph_ince.ph_action_date
            WHERE ph_tpdr.ph_action_code = 'TPDR'
              AND ph_ince.ph_action_code = 'INCE'
              AND ph_troa.ph_action_code = 'TROA'
              AND ph_tpdr.ph_action_date BETWEEN '{start_date}' AND '{end_date}'
            """
        elif analysis_type == "Response Within 6-Month NOA":
            query = f"""
            SELECT
              ph1.serial_number,
              ph1.ph_action_date AS `DATE OF ITU OFFICE ACTION ISSUED FOR EXTENSION REQUEST`,
              ph2.ph_action_code AS `response code`,
              ph2.ph_action_date AS `response date`,
              DATEDIFF(ph2.ph_action_date, ph1.ph_action_date) AS days_between
            FROM {CATALOG}.{SCHEMA}.{TABLE} ph1
            JOIN {CATALOG}.{SCHEMA}.{TABLE} ph2
              ON ph1.serial_number = ph2.serial_number
              AND ph2.ph_action_date > ph1.ph_action_date
              AND DATEDIFF(ph2.ph_action_date, ph1.ph_action_date) <= 180
            WHERE ph1.ph_action_code = 'INCE'
              AND ph2.ph_action_code IN ('TROA', 'EROI')
              AND ph1.ph_action_date BETWEEN '{start_date}' AND '{end_date}'
            """
        elif analysis_type == "Post-TROA Outcomes":
            outcomes_str = ", ".join(f"'{c}'" for c in selected_outcomes)
            query = f"""
            SELECT
              ph_troa.serial_number,
              ph_troa.ph_action_date AS `DATE OF TEAS RESPONSE TO OFFICE ACTION RECEIVED`,
              ph_next.ph_action_code AS `next action`,
              ph_next.ph_action_date AS `next action date`
            FROM {CATALOG}.{SCHEMA}.{TABLE} ph_troa
            JOIN {CATALOG}.{SCHEMA}.{TABLE} ph_next
              ON ph_troa.serial_number = ph_next.serial_number
              AND ph_next.ph_action_date > ph_troa.ph_action_date
            WHERE ph_troa.ph_action_code = 'TROA'
              AND ph_next.ph_action_code IN ({outcomes_str})
              AND ph_troa.ph_action_date BETWEEN '{start_date}' AND '{end_date}'
            """
        elif analysis_type == "Petition to Director Anomaly":
            query = f"""
            SELECT
              ph_tpdr.serial_number,
              ph_tpdr.ph_action_date AS `DATE OF TEAS PETITION TO DIRECTOR RECEIVED`,
              ph_ince.ph_action_date AS `DATE OF ITU OFFICE ACTION ISSUED FOR EXTENSION REQUEST DATE`,
              ph_troa.ph_action_date AS `DATE OF TEAS RESPONSE TO OFFICE ACTION RECEIVED DATE`
            FROM {CATALOG}.{SCHEMA}.{TABLE} ph_tpdr
            JOIN {CATALOG}.{SCHEMA}.{TABLE} ph_ince
              ON ph_tpdr.serial_number = ph_ince.serial_number
              AND ph_ince.ph_action_date > ph_tpdr.ph_action_date
            JOIN {CATALOG}.{SCHEMA}.{TABLE} ph_troa
              ON ph_ince.serial_number = ph_troa.serial_number
              AND ph_troa.ph_action_date > ph_ince.ph_action_date
            WHERE ph_tpdr.ph_action_code = 'TPDR'
              AND ph_ince.ph_action_code = 'INCE'
              AND ph_troa.ph_action_code = 'TROA'
              AND ph_tpdr.ph_action_date BETWEEN '{start_date}' AND '{end_date}'
            """
        else:
            query = None

        if query:
            with st.spinner("Running analysis..."):
                cursor.execute(query)
                df = cursor.fetchall_arrow().to_pandas()
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
                excel_data = generate_excel(df)
                st.download_button(
                    label="📥 Download Excel",
                    data=excel_data,
                    file_name=f"analysis_{analysis_type.replace(' ', '_')}_{len(df)}_rows.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )
            else:
                st.info("No records found for this analysis.")

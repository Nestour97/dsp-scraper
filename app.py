import os
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from dsp_scrapers import DSP_OPTIONS, run_scraper

# ---------- SONY (BLACK + RED) THEME ----------
SONY_BG = "#050505"
SONY_PANEL = "#0b0b0b"
SONY_CARD = "#151515"
SONY_RED = "#e31c23"
SONY_RED_SOFT = "#ff4b5c"
SONY_TEXT_MUTED = "#c7c7c7"

st.set_page_config(
    page_title="Sony-Style DSP Price Scraper",
    layout="wide",  # more like Power BI
)

# Remember last run so table doesn’t disappear on every interaction
if "results" not in st.session_state:
    st.session_state["results"] = None

# ---------- GLOBAL CSS ----------
st.markdown(
    f"""
    <style>
    .stApp {{
        background-color: {SONY_BG};
        color: white;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}

    h1, h2, h3, h4 {{
        color: white;
    }}

    section[data-testid="stSidebar"] {{
        background-color: {SONY_PANEL};
        border-right: 1px solid #262626;
    }}

    /* Main content width */
    .block-container {{
        padding-top: 1.5rem;
        padding-bottom: 3rem;
        max-width: 1400px;
    }}

    .stRadio > label, .stSelectbox > label {{
        font-weight: 600;
        color: {SONY_TEXT_MUTED};
    }}

    .stButton>button {{
        background: linear-gradient(90deg, {SONY_RED}, {SONY_RED_SOFT});
        color: white;
        border: none;
        padding: 0.7rem 1.4rem;
        border-radius: 999px;
        font-weight: 600;
        cursor: pointer;
        letter-spacing: 0.03em;
        text-transform: uppercase;
        font-size: 0.8rem;
        transition: transform 0.1s ease-in-out, box-shadow 0.1s ease-in-out;
        box-shadow: 0 0 16px rgba(227, 28, 35, 0.45);
    }}

    .stButton>button:hover {{
        transform: translateY(-1px);
        box-shadow: 0 0 26px rgba(227, 28, 35, 0.7);
    }}

    .stDownloadButton>button {{
        background: transparent;
        border: 1px solid {SONY_RED_SOFT};
        color: white;
        padding: 0.5rem 1.2rem;
        border-radius: 999px;
        font-weight: 500;
        font-size: 0.85rem;
    }}

    .stDownloadButton>button:hover {{
        background: {SONY_RED};
        border-color: {SONY_RED_SOFT};
    }}

    .sony-card {{
        background: radial-gradient(circle at 10% 0%, rgba(227, 28, 35, 0.12) 0, transparent 55%),
                    {SONY_CARD};
        padding: 1.3rem 1.6rem;
        border-radius: 1.2rem;
        border: 1px solid rgba(255, 255, 255, 0.04);
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.7);
    }}

    .small-text {{
        font-size: 0.85rem;
        color: {SONY_TEXT_MUTED};
    }}

    /* --- AG Grid (Power BI style) --- */
    .ag-theme-streamlit .ag-root-wrapper,
    .ag-theme-streamlit .ag-root-wrapper-body,
    .ag-theme-streamlit .ag-header,
    .ag-theme-streamlit .ag-row,
    .ag-theme-streamlit .ag-cell {{
        background-color: {SONY_CARD};
        color: white;
        border-color: #303030;
        font-size: 0.85rem;
    }}

    .ag-theme-streamlit .ag-header {{
        background-color: #101010;
        border-bottom: 1px solid #303030;
    }}

    .ag-theme-streamlit .ag-header-cell-label {{
        color: #f4f4f4;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.08em;
    }}

    .ag-theme-streamlit .ag-row-hover {{
        background-color: #26060a !important;
    }}

    .ag-theme-streamlit .ag-row-selected {{
        background-color: #3a0a10 !important;
    }}

    .ag-theme-streamlit .ag-floating-filter-input,
    .ag-theme-streamlit .ag-input-field-input,
    .ag-theme-streamlit .ag-text-field-input {{
        background-color: #111;
        border-radius: 999px;
        color: white;
        border: 1px solid #333;
    }}

    .ag-theme-streamlit .ag-icon {{
        color: {SONY_TEXT_MUTED};
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- HEADER ----------
st.markdown(
    """
    <div style="display:flex; align-items:center; gap:0.9rem; margin-bottom:1.1rem;">
        <div style="
            width: 46px; height: 46px;
            border-radius: 999px;
            background: radial-gradient(circle at 30% 30%, #ffffff, #ff8a9b);
            display:flex; align-items:center; justify-content:center;
            font-weight:800; color:#000;
            box-shadow: 0 0 18px rgba(255, 76, 91, 0.7);
        ">
            D
        </div>
        <div>
            <h1 style="margin-bottom:0;">DSP Price Scraper</h1>
            <p class="small-text" style="margin-top:0.2rem;">
                Central hub for your global DSP pricing, dressed in a Sony-style black + red skin.
            </p>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------- SIDEBAR ----------
st.sidebar.header("⚙️ Scraper Options")

dsp_name = st.sidebar.selectbox(
    "Choose a DSP",
    options=list(DSP_OPTIONS.keys()),
)

mode = st.sidebar.radio(
    "Mode",
    options=["Test (quick run)", "Full (all countries)"],
    index=0,
)

test_mode = mode.startswith("Test")

st.sidebar.markdown(
    "<p class='small-text'>For now, Apple Music + Disney+ ignore per-country selection and always run global logic from your scripts.</p>",
    unsafe_allow_html=True,
)

# ---------- MAIN CONTENT ----------
col_info, col_empty = st.columns([2.5, 1])

with col_info:
    st.markdown(
        """
        <div class="sony-card">
            <h3 style="margin-top:0;">🎧 How it works</h3>
            <ul style="margin-bottom:0;">
                <li>Select <b>Apple Music</b> or <b>Disney+</b> in the sidebar.</li>
                <li>Pick <b>Test</b> for a small run, or <b>Full</b> for all countries.</li>
                <li>Click <b>Run Scraper</b> to launch your existing Python code.</li>
                <li>Explore the results in the interactive table below (sort, filter, search).</li>
                <li>Download the full Excel extract with one click.</li>
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col_empty:
    st.empty()

st.write("")  # spacing

# ---------- RUN SCRAPER BUTTON ----------
run_clicked = st.button("🚀 Run Scraper")

if run_clicked:
    with st.spinner(f"Running {dsp_name} scraper…"):
        try:
            excel_path = run_scraper(dsp_name=dsp_name, test_mode=test_mode)

            if not excel_path or not os.path.exists(excel_path):
                st.error(f"Scraper finished, but I couldn't find the file: {excel_path}")
            else:
                st.session_state["results"] = {
                    "excel_path": excel_path,
                    "dsp_name": dsp_name,
                    "test_mode": test_mode,
                }
                st.success(f"Scraping completed for {dsp_name}! Scroll down to explore the data.")
        except Exception as e:
            st.error(f"An error occurred while running the scraper: {e}")

# ---------- POWER BI STYLE TABLE + DOWNLOAD ----------
results = st.session_state.get("results")

if results and results.get("excel_path") and os.path.exists(results["excel_path"]):
    excel_path = results["excel_path"]

    st.markdown("### 📊 Data explorer (Power BI-style)")
    st.caption("Use the column headers to sort & filter. The sidebar inside the grid lets you show/hide fields, just like Power BI.")

    # Load Excel and build grid options
    df = pd.read_excel(excel_path)

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filter=True,
        sortable=True,
        resizable=True,
        floatingFilter=True,  # little filter boxes under each header
    )
    gb.configure_pagination(
        enabled=True,
        paginationAutoPageSize=False,
        paginationPageSize=25,
    )
    gb.configure_side_bar()  # shows Filters / Columns panel (Power BI vibe)
    grid_options = gb.build()

    AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.NO_UPDATE,
        theme="streamlit",  # restyled by CSS above
        height=520,
        fit_columns_on_grid_load=True,
    )

    # Download button under the table
    with open(excel_path, "rb") as f:
        data = f.read()

    st.download_button(
        "📥 Download full Excel file",
        data=data,
        file_name=os.path.basename(excel_path),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

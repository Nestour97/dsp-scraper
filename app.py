import os
import sys
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from dsp_scrapers import DSP_OPTIONS, run_scraper

# ---------- GLOBAL CONFIG ----------

SONY_BG = "#050505"
SONY_PANEL = "#0b0b0b"
SONY_CARD = "#151515"
SONY_RED = "#e31c23"
SONY_RED_SOFT = "#ff4b5c"
SONY_TEXT_MUTED = "#c7c7c7"

# Local image paths (you need to add these files to your repo)
SONY_LOGO_PATH = Path("sony_logo.png")
APPLE_LOGO_PATH = Path("apple_music_logo.png")
DISNEY_LOGO_PATH = Path("disney_plus_logo.png")

st.set_page_config(
    page_title="DSP Price Scraper",
    page_icon="🎧",
    layout="wide",
)

# ---------- UTILITIES ----------


def ensure_playwright_for_disney():
    """
    Make sure Playwright's Chromium browser is installed.
    Safe to call multiple times; it will just be a no-op if already installed.
    """
    if st.session_state.get("playwright_ready", False):
        return

    try:
        # Use `python -m playwright install chromium` to avoid PATH issues
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        st.session_state["playwright_ready"] = True
    except Exception as e:
        st.warning(
            "Could not auto-install Playwright browsers. "
            "Disney+ scraping may fail until Chromium is installed.\n\n"
            f"Technical detail: {e}"
        )


def load_excel_as_df(excel_path: str) -> pd.DataFrame:
    path = Path(excel_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")
    return pd.read_excel(path)


def render_powerbi_grid(df: pd.DataFrame, excel_path: str) -> None:
    st.subheader("📊 Data explorer (Power BI–style)")

    with st.container():
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(
            sortable=True,
            filter=True,
            resizable=True,
        )
        gb.configure_pagination(
            enabled=True,
            paginationAutoPageSize=False,
            paginationPageSize=50,
        )
        gb.configure_side_bar()

        grid_options = gb.build()

        AgGrid(
            df,
            gridOptions=grid_options,
            update_mode=GridUpdateMode.NO_UPDATE,
            theme="streamlit",
            height=700,  # bigger table
            fit_columns_on_grid_load=True,
        )

        # Download button under the table
        with open(excel_path, "rb") as f:
            data = f.read()
        st.download_button(
            "📥 Download full Excel file",
            data=data,
            file_name=os.path.basename(excel_path),
            mime=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"
            ),
        )


def nice_error_box(err: Exception) -> None:
    st.error(
        "An error occurred while running the scraper:\n\n"
        f"`{type(err).__name__}: {err}`"
    )
    with st.expander("Show full traceback / debug info"):
        st.exception(err)


def run_and_render(dsp_name: str, test_mode: bool):
    """
    Wrapper used by the UI. Handles:
    - optional Playwright setup (Disney+)
    - 3-step progress bar
    - Excel → DataFrame → grid
    """
    if dsp_name not in DSP_OPTIONS:
        st.error(f"Unknown DSP: {dsp_name}")
        return

    # Disney+ needs a browser
    if dsp_name == "Disney+":
        ensure_playwright_for_disney()

    st.markdown("### 🚀 Run status")
    progress = st.progress(0)
    status = st.empty()

    # Phase 1 – boot
    progress.progress(10)
    status.markdown("Booting scraper…")

    try:
        # Phase 2 – main scraping
        progress.progress(35)
        status.markdown(
            "Scraping prices… this can take a few minutes in **Full** mode."
        )
        excel_path = run_scraper(dsp_name, test_mode=test_mode)

        # Phase 3 – load results
        progress.progress(70)
        status.markdown("Loading results into the data explorer…")

        df = load_excel_as_df(excel_path)

        progress.progress(100)
        status.markdown(
            f"✅ Finished! Scraped **{len(df):,} rows** for **{dsp_name}**."
        )

        render_powerbi_grid(df, excel_path)

        # Optional: show Apple Music "missing countries" log if present
        if dsp_name == "Apple Music":
            missing_csv = Path("apple_music_missing.csv")
            if missing_csv.exists():
                try:
                    miss_df = pd.read_csv(missing_csv)
                    with st.expander(
                        "Apple Music – countries that failed (debug log)"
                    ):
                        st.dataframe(miss_df)
                except Exception:
                    pass

    except Exception as e:
        nice_error_box(e)


def logo_or_title(path: Path, fallback_title: str, width: int = 140):
    """
    Helper: show an image if it exists, otherwise just show the title text.
    """
    if path.is_file():
        st.image(str(path), width=width)
    else:
        st.markdown(f"### {fallback_title}")


# ---------- SONY SKIN / CSS ----------

st.markdown(
    f"""
    <style>
    body {{
        background-color: {SONY_BG};
    }}
    .block-container {{
        padding-top: 1.2rem;
        padding-bottom: 1.5rem;
    }}
    .sony-card {{
        background: radial-gradient(circle at 0 0, #222 0, {SONY_CARD} 40%, {SONY_BG} 100%);
        border-radius: 22px;
        padding: 1.6rem 1.8rem;
        border: 1px solid #222;
        box-shadow: 0 22px 60px rgba(0,0,0,0.85);
    }}
    .sony-panel {{
        background: {SONY_PANEL};
        border-radius: 18px;
        padding: 1.2rem 1.4rem;
        border: 1px solid #222;
    }}
    .sony-pill {{
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        background: linear-gradient(135deg, {SONY_RED_SOFT}, {SONY_RED});
        color: #fff;
        padding: 0.35rem 0.85rem;
        border-radius: 999px;
        font-size: 0.78rem;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }}
    .small-text {{
        font-size: 0.85rem;
        color: {SONY_TEXT_MUTED};
    }}
    .ag-theme-streamlit .ag-root-wrapper {{
        border-radius: 18px;
        border: 1px solid #333;
    }}
    .ag-theme-streamlit .ag-header {{
        background: #101010;
        color: #f5f5f5;
        font-weight: 600;
    }}
    .ag-theme-streamlit .ag-row-even {{
        background-color: #101010;
    }}
    .ag-theme-streamlit .ag-row-odd {{
        background-color: #0a0a0a;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- SIDEBAR ----------

with st.sidebar:
    st.markdown("## ⚙️ Scraper Options")

    st.markdown(
        "Pick the mode once, then run the scraper from the tab you care about."
    )
    mode_label = st.radio(
        "Mode",
        ["Test (quick run)", "Full (all countries)"],
        index=0,
        help=(
            "Test: only a small subset of countries. "
            "Full: everything, but slower."
        ),
    )
    test_mode = mode_label.startswith("Test")

    st.markdown("---")
    st.markdown(
        "<span class='small-text'>For now, Apple Music and Disney+ "
        "always run their built-in global logic.</span>",
        unsafe_allow_html=True,
    )

# ---------- HEADER ----------

header_col1, header_col2, header_col3 = st.columns([0.9, 4, 1.4])

with header_col1:
    st.markdown(
        """
        <div style="
            width: 52px; height: 52px;
            border-radius: 999px;
            background: radial-gradient(circle at 30% 30%, #ffffff, #ff8a9b);
            display:flex; align-items:center; justify-content:center;
            font-weight:800; color:#000;
            box-shadow: 0 0 22px rgba(255, 76, 91, 0.9);
        ">
            D
        </div>
        """,
        unsafe_allow_html=True,
    )

with header_col2:
    st.markdown(
        """
        <div>
            <div class="sony-pill">Sony-style DSP command centre</div>
            <h1 style="margin-top:0.6rem; margin-bottom:0.2rem;">DSP Price Scraper</h1>
            <p class="small-text">
                Central hub for your global DSP pricing extraction.
                Run Apple Music or Disney+ in one click, then explore the results
                in a Power BI-style grid or download to Excel.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with header_col3:
    if SONY_LOGO_PATH.is_file():
        st.image(str(SONY_LOGO_PATH), caption="Sony-flavoured UI", use_column_width=True)
    else:
        st.markdown(
            "<p style='text-align:right; color:#777;'>"
            "Add <code>images/sony_logo.png</code> to show the Sony logo here."
            "</p>",
            unsafe_allow_html=True,
        )

st.markdown("")
st.markdown(
    """
    <div class="sony-panel">
        <b>How it works</b>
        <ul class="small-text">
            <li>Select <b>Apple Music</b> or <b>Disney+</b> in the tabs below.</li>
            <li>Pick <b>Test</b> for a quick smoke-test, or <b>Full</b> for all countries.</li>
            <li>Hit <b>Run scraper</b> to launch the existing Python code.</li>
            <li>Explore the results in the interactive table (sort, filter, search).</li>
            <li>Download the full Excel extract with one click.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("")
st.markdown("## 🎛️ Choose your DSP")

# ---------- MAIN TABS ----------

apple_tab, disney_tab = st.tabs(
    [
        " Apple Music",
        "Disney+",
    ]
)

with apple_tab:
    col_logo, col_body = st.columns([1, 4])

    with col_logo:
        logo_or_title(APPLE_LOGO_PATH, "Apple Music")

    with col_body:
        st.markdown(
            "### Apple Music pricing\n"
            "Scrape global Apple Music plan prices, currencies and country codes."
        )
        if st.button("🚀 Run Apple Music scraper", key="run_apple"):
            run_and_render("Apple Music", test_mode=test_mode)

with disney_tab:
    col_logo, col_body = st.columns([1, 4])

    with col_logo:
        logo_or_title(DISNEY_LOGO_PATH, "Disney+")

    with col_body:
        st.markdown(
            "### Disney+ pricing\n"
            "Scrape global Disney+ subscription prices using the Playwright-powered scraper."
        )
        if st.button("🚀 Run Disney+ scraper", key="run_disney"):
            run_and_render("Disney+", test_mode=test_mode)


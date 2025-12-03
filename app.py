import os
import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pycountry
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from dsp_scrapers import DSP_OPTIONS, run_scraper

# ---------- PAGE CONFIG ----------

st.set_page_config(
    page_title="DSP Price Scraper",
    page_icon="🎧",
    layout="wide",
)

# ---------- CONSTANTS / PATHS ----------

SONY_LOGO_PATH = Path("sony_logo.png")
APPLE_LOGO_PATH = Path("apple_music_logo.png")
DISNEY_LOGO_PATH = Path("disney_plus_logo.png")

# Build list of country options once (for Test mode)
COUNTRY_LABELS = []
LABEL_TO_CODE = {}
for c in pycountry.countries:
    code = c.alpha_2
    name = c.name
    label = f"{name} ({code})"
    COUNTRY_LABELS.append(label)
    LABEL_TO_CODE[label] = code
COUNTRY_LABELS.sort()

# Session state store for cached results
if "results" not in st.session_state:
    st.session_state["results"] = {}


def _result_key(dsp_name: str, mode_label: str, codes: list[str]) -> str:
    """Stable key for cached results."""
    codes_part = ",".join(sorted(codes)) if codes else "ALL"
    return f"{dsp_name}::{mode_label}::{codes_part}"


# ---------- BASIC STYLING ----------

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
    }
    .section-card {
        background-color: #0b0b0b;
        color: #f3f3f3;
        border-radius: 14px;
        padding: 1rem 1.3rem;
        border: 1px solid #202020;
    }
    .section-card ul {
        margin-top: 0.4rem;
        margin-bottom: 0;
    }
    .section-muted {
        color: #777777;
        font-size: 0.88rem;
    }
    .run-button button {
        border-radius: 999px !important;
        padding: 0.4rem 1.2rem !important;
        font-weight: 600 !important;
    }
    .ag-theme-streamlit .ag-root-wrapper {
        border-radius: 12px;
        border: 1px solid #dddddd;
    }
    .ag-theme-streamlit .ag-header {
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- UTILITIES ----------


def ensure_playwright_for_disney() -> None:
    """Install Playwright Chromium browser if needed (no-op once done)."""
    if st.session_state.get("playwright_ready"):
        return
    try:
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
            "Disney+ scraping may fail until Chromium is installed. "
            f"Details: {e}"
        )


def load_excel_as_df(excel_path: str) -> pd.DataFrame:
    path = Path(excel_path)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found: {path}")
    return pd.read_excel(path)


def render_powerbi_grid(df: pd.DataFrame, excel_path: str) -> None:
    st.subheader("Data explorer (Power BI–style)")

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
        height=600,
        fit_columns_on_grid_load=True,
    )

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
    with st.expander("Show full traceback"):
        st.exception(err)


def run_and_render(dsp_name: str, mode_label: str, selected_codes: list[str]):
    """
    Run scraper in a background thread and show a smooth, continuous
    progress bar. Also caches results in session_state.
    """
    test_mode = mode_label.startswith("Test")
    key = _result_key(dsp_name, mode_label, selected_codes)

    if dsp_name == "Disney+":
        ensure_playwright_for_disney()

    st.markdown("### Run status")
    progress = st.progress(0)
    status = st.empty()

    def _scrape():
        # run_scraper is from dsp_scrapers.__init__
        return run_scraper(dsp_name, test_mode, selected_codes)

    # Run scraper in a worker thread and animate the bar in this thread
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_scrape)
        pct = 0
        with st.spinner(f"Running {dsp_name} scraper…"):
            while not future.done():
                pct = min(pct + 2, 95)
                progress.progress(pct)
                status.markdown(
                    f"<b>{pct}%</b> – scraping {dsp_name}…",
                    unsafe_allow_html=True,
                )
                time.sleep(0.20)

        try:
            excel_path = future.result()
        except Exception as e:
            progress.progress(0)
            nice_error_box(e)
            return

    # Finalise progress
    progress.progress(100)
    status.markdown(
        f"<b>100%</b> – finished {dsp_name} scrape.",
        unsafe_allow_html=True,
    )

    # Load and cache
    df = load_excel_as_df(excel_path)
    rows = len(df)

    st.session_state["results"][key] = {
        "df": df,
        "excel": excel_path,
        "rows": rows,
        "ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    st.success(f"Scraped {rows:,} rows for {dsp_name}.")
    render_powerbi_grid(df, excel_path)

    # Apple-specific debug log if present
    if dsp_name == "Apple Music":
        missing_csv = Path("apple_music_missing.csv")
        if missing_csv.exists():
            try:
                miss_df = pd.read_csv(missing_csv)
                with st.expander("Apple Music – countries that failed (debug log)"):
                    st.dataframe(miss_df)
            except Exception:
                pass


def show_cached_result(dsp_name: str, mode_label: str, selected_codes: list[str]):
    key = _result_key(dsp_name, mode_label, selected_codes)
    cached = st.session_state["results"].get(key)
    if not cached:
        return

    st.caption(
        f"Showing last run for **{dsp_name}** "
        f"({cached['rows']:,} rows, scraped at {cached['ts']})."
    )
    render_powerbi_grid(cached["df"], cached["excel"])


def logo(path: Path, width: int, alt: str):
    if path.is_file():
        st.image(str(path), width=width)
    else:
        st.markdown(f"**{alt}**")


# ---------- SIDEBAR (global options) ----------

with st.sidebar:
    st.header("Scraper options")

    mode_label = st.radio(
        "Mode",
        ["Full (all countries)", "Test (choose countries)"],
        help=(
            "Full: run every available country. "
            "Test: choose a subset of countries to scrape."
        ),
        index=0,
    )

    selected_codes: list[str] = []
    if mode_label.startswith("Test"):
        selected_labels = st.multiselect(
            "Countries for test runs",
            COUNTRY_LABELS,
            help=(
                "Start typing a country name, then pick as many as you like. "
                "This custom selection currently applies to Apple Music."
            ),
        )
        selected_codes = [LABEL_TO_CODE[l] for l in selected_labels]

    st.markdown(
        "<p class='section-muted'>"
        "Results stay on screen after a run. "
        "Switch between tabs to compare DSPs.</p>",
        unsafe_allow_html=True,
    )

# ---------- HEADER ----------

col1, col2, col3 = st.columns([1.2, 3, 1])

with col1:
    st.markdown("### DSP Price Scraper")
    st.markdown(
        "<p class='section-muted'>Central hub for your global DSP pricing.</p>",
        unsafe_allow_html=True,
    )

with col2:
    st.write("")

with col3:
    if SONY_LOGO_PATH.is_file():
        st.image(str(SONY_LOGO_PATH), width=140, caption="Sony-flavoured UI")
    else:
        st.markdown(
            "<p style='text-align:right; font-size:0.85rem; color:#777;'>"
            "Add <code>sony_logo.png</code> in the repo root to show the logo here."
            "</p>",
            unsafe_allow_html=True,
        )

st.markdown("")
st.markdown(
    """
    <div class="section-card">
        <b>How it works</b>
        <ul>
            <li>Select <b>Apple Music</b> or <b>Disney+</b> in the tabs below.</li>
            <li>Pick <b>Full</b> for all countries, or <b>Test</b> to choose a subset.</li>
            <li>Hit <b>Run scraper</b> to launch the underlying Python code.</li>
            <li>Explore the results in the interactive table (sort, filter, search).</li>
            <li>Download the full Excel extract with one click.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("## Choose your DSP")

# ---------- MAIN TABS ----------

apple_tab, disney_tab = st.tabs(["Apple Music", "Disney+"])

with apple_tab:
    top_col1, top_col2 = st.columns([1, 4])
    with top_col1:
        logo(APPLE_LOGO_PATH, width=80, alt="Apple Music")
    with top_col2:
        st.markdown("### Apple Music pricing")
        st.markdown(
            "Scrape global Apple Music plan prices, currencies and country codes."
        )

    run_button = st.button(
        "Run Apple Music scraper",
        key="run_apple",
        help="Launch Apple Music scraper",
    )

    if run_button:
        run_and_render("Apple Music", mode_label, selected_codes)
    else:
        # If we have previous results matching the current settings, show them
        show_cached_result("Apple Music", mode_label, selected_codes)

with disney_tab:
    top_col1, top_col2 = st.columns([1, 4])
    with top_col1:
        logo(DISNEY_LOGO_PATH, width=90, alt="Disney+")
    with top_col2:
        st.markdown("### Disney+ pricing")
        st.markdown(
            "Scrape global Disney+ subscription prices using the Playwright-powered scraper."
        )

    run_button = st.button(
        "Run Disney+ scraper",
        key="run_disney",
        help="Launch Disney+ scraper",
    )

    if run_button:
        run_and_render("Disney+", mode_label, selected_codes)
    else:
        show_cached_result("Disney+", mode_label, selected_codes)

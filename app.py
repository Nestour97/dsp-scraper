import sys
import time
import base64
import subprocess
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pycountry
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from dsp_scrapers import DSP_OPTIONS, run_scraper  # unified scraper

# ---------- PAGE CONFIG ----------

st.set_page_config(
    page_title="DSP Price Scraper",
    page_icon="🎧",
    layout="wide",
)

# ---------- LOGO PATHS ----------

SONY_LOGO_PATH = Path("sony_logo.png")
APPLE_LOGO_PATH = Path("apple_music_logo.png")
DISNEY_LOGO_PATH = Path("disney_plus_logo.png")

DSP_LOGOS = {
    "Apple Music": APPLE_LOGO_PATH,
    "Disney+": DISNEY_LOGO_PATH,
    # e.g. "Spotify": Path("spotify_logo.png"),
}

# ---------- COUNTRY OPTIONS ----------

COUNTRY_LABELS: list[str] = []
LABEL_TO_CODE: dict[str, str] = {}

for c in pycountry.countries:
    code = c.alpha_2
    name = c.name
    label = f"{name} ({code})"
    COUNTRY_LABELS.append(label)
    LABEL_TO_CODE[label] = code

COUNTRY_LABELS.sort()

# ---------- SESSION STATE ----------

if "results" not in st.session_state:
    st.session_state["results"] = {}

if "playwright_ready" not in st.session_state:
    st.session_state["playwright_ready"] = False


def result_key(dsp_name: str, mode_label: str, country_codes: list[str]) -> str:
    codes_part = ",".join(sorted(country_codes)) if country_codes else "ALL"
    return f"{dsp_name}::{mode_label}::{codes_part}"


# ---------- GLOBAL STYLES ----------

st.markdown(
    """
    <style>
    body {
        background-color: #000000;
        color: #f5f5f5;
    }

    /* hide sidebar completely */
    [data-testid="stSidebar"] {
        display: none;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2.5rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
        max-width: 1700px;
        margin-left: auto;
        margin-right: auto;
        background-color: #000000;
    }

    h1, h2, h3, h4, h5, h6, label, p {
        color: #f5f5f5 !important;
    }

    .header-wrapper {
        text-align: center;
        max-width: 900px;
        margin-left: auto;
        margin-right: auto;
        margin-bottom: 1.8rem;
    }
    .header-title {
        font-size: 2.5rem;
        font-weight: 800;
        letter-spacing: 0.09em;
        margin-top: 0.6rem;
        margin-bottom: 0.35rem;
        color: #ffffff;
        text-transform: uppercase;
    }
    .header-subtitle {
        font-size: 0.98rem;
        color: #f2f2f2;
        margin: 0 auto 0.5rem auto;
    }
    .header-pill {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 0.16rem 0.9rem;
        border-radius: 999px;
        font-size: 0.76rem;
        background: #e31c23;
        color: #ffffff;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-top: 0.25rem;
    }

    .how-card {
        background-color: #050505;
        border-radius: 0.8rem;
        padding: 0.9rem 1.3rem;
        border: 1px solid #262626;
        color: #f5f5f5;
        margin-bottom: 1.2rem;
    }
    .how-card ul {
        margin-top: 0.35rem;
        margin-bottom: 0;
        padding-left: 1.1rem;
    }
    .how-card li {
        font-size: 0.9rem;
    }

    .section-heading {
        font-size: 1.2rem;
        font-weight: 600;
        margin-top: 0.9rem;
        margin-bottom: 0.4rem;
        color: #ffffff;
    }

    .side-note {
        font-size: 0.86rem;
        color: #cccccc;
    }

    /* center DSP tabs and enlarge labels */
    .stTabs [role="tablist"] {
        justify-content: center;
    }
    .stTabs [role="tab"] p {
        font-size: 1.02rem;
        font-weight: 600;
    }

    /* AgGrid styling */
    .ag-theme-streamlit .ag-root-wrapper {
        border-radius: 0.7rem;
        border: 1px solid #444444;
    }
    .ag-theme-streamlit .ag-header {
        background: #111111;
        color: #fafafa;
        font-weight: 600;
    }
    .ag-theme-streamlit .ag-row-even {
        background-color: #050505;
    }
    .ag-theme-streamlit .ag-row-odd {
        background-color: #020202;
    }

    /* Primary buttons (run buttons) */
    div.stButton > button {
        border-radius: 999px !important;
        background: #e31c23 !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding-left: 1.3rem !important;
        padding-right: 1.3rem !important;
    }

    /* Download button in Sony red */
    .stDownloadButton > button {
        border-radius: 999px !important;
        background: #e31c23 !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding-left: 1.3rem !important;
        padding-right: 1.3rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- UTILITIES ----------


def ensure_playwright_for_disney() -> None:
    """Install Playwright Chromium browser if needed (no-op after first time)."""
    if st.session_state["playwright_ready"]:
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
    st.markdown("<h3>Data explorer (Power BI-style)</h3>", unsafe_allow_html=True)

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
        height=620,
        fit_columns_on_grid_load=True,
    )

    with open(excel_path, "rb") as f:
        data = f.read()
    st.download_button(
        "📥 Download Excel extract",
        data=data,
        file_name=Path(excel_path).name,
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


def format_mm_ss(seconds: float) -> str:
    seconds = max(int(seconds), 0)
    m, s = divmod(seconds, 60)
    return f"{m:02d}:{s:02d}"


def estimate_country_count(dsp_name: str, mode_label: str, codes: list[str]) -> int:
    if codes:
        return len(codes)

    if dsp_name == "Apple Music":
        return 230
    if dsp_name == "Disney+":
        return 90
    return 150


def estimate_expected_seconds(dsp_name: str, mode_label: str, codes: list[str]) -> float:
    n = estimate_country_count(dsp_name, mode_label, codes)

    if dsp_name == "Apple Music":
        per = 3.0
    elif dsp_name == "Disney+":
        per = 4.5
    else:
        per = 3.0

    return max(n * per, 20.0)


def logo(path: Path, width: int, alt: str):
    if path.is_file():
        st.image(str(path), width=width)
    else:
        st.markdown(f"**{alt}**")


def centered_sony_logo():
    """Embed Sony logo as base64 and truly centre it."""
    if not SONY_LOGO_PATH.is_file():
        return
    data = base64.b64encode(SONY_LOGO_PATH.read_bytes()).decode()
    st.markdown(
        f"""
        <p style="text-align:center; margin-bottom:0.3rem;">
            <img src="data:image/png;base64,{data}" width="120">
        </p>
        """,
        unsafe_allow_html=True,
    )


def show_cached_result(dsp_name: str, mode_label: str, codes: list[str]) -> None:
    key = result_key(dsp_name, mode_label, codes)
    cached = st.session_state["results"].get(key)
    if not cached:
        return

    st.caption(
        f"Showing last run for **{dsp_name}** "
        f"({cached['rows']:,} rows, scraped at {cached['ts']})."
    )
    render_powerbi_grid(cached["df"], cached["excel"])


def run_and_render(dsp_name: str, mode_label: str, codes: list[str]) -> None:
    """Run scraper in background thread and show continuous progress + ETA."""
    test_mode = mode_label == "Test"

    if dsp_name == "Disney+":
        ensure_playwright_for_disney()

    st.markdown("<h3>Run status</h3>", unsafe_allow_html=True)
    progress = st.progress(0)
    status = st.empty()

    expected_total = estimate_expected_seconds(dsp_name, mode_label, codes)
    start = time.time()

    def run_worker():
        return run_scraper(dsp_name, test_mode, codes)

    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(run_worker)

        with st.spinner(f"Running {dsp_name} scraper…"):
            while not future.done():
                elapsed = time.time() - start
                pct_est = min(int(100 * (elapsed / expected_total)), 95)
                remaining_est = max(expected_total - elapsed, 0)

                progress.progress(pct_est)
                status.markdown(
                    f"**{pct_est}%** · Elapsed {format_mm_ss(elapsed)} "
                    f"· Est. remaining {format_mm_ss(remaining_est)}",
                    unsafe_allow_html=True,
                )
                time.sleep(0.35)

        try:
            excel_path = future.result()
        except Exception as e:
            progress.progress(0)
            nice_error_box(e)
            return

    elapsed = time.time() - start
    progress.progress(100)
    status.markdown(
        f"**100%** · Elapsed {format_mm_ss(elapsed)} · Est. remaining 00:00",
        unsafe_allow_html=True,
    )

    df = load_excel_as_df(excel_path)
    rows = len(df)

    st.success(f"Scraped {rows:,} rows for {dsp_name}.")
    st.session_state["results"][result_key(dsp_name, mode_label, codes)] = {
        "df": df,
        "excel": excel_path,
        "rows": rows,
        "ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    render_powerbi_grid(df, excel_path)

    if dsp_name == "Apple Music":
        missing_csv = Path("apple_music_missing.csv")
        if missing_csv.exists():
            try:
                miss_df = pd.read_csv(missing_csv)
                with st.expander("Apple Music – countries that failed (debug log)"):
                    st.dataframe(miss_df)
            except Exception:
                pass


# ---------- HEADER ----------

centered_sony_logo()

st.markdown(
    """
    <div class="header-wrapper">
        <div class="header-title">DSP PRICE SCRAPER</div>
        <p class="header-subtitle">
            Central hub for Apple Music &amp; Disney+ pricing. Run scrapes on demand,
            explore the results in a Power BI-style grid, and export straight to Excel.
        </p>
        <div class="header-pill">DSP ANALYTICS TOOL</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="how-card">
        <b>How it works</b>
        <ul>
            <li>Select <b>Apple Music</b>, <b>Disney+</b>, or another DSP in the tabs below.</li>
            <li>In each tab, choose <b>Full</b> for all countries or <b>Test</b> to target specific markets.</li>
            <li>In Test mode, use the search box and select countries from the dropdown.</li>
            <li>Click <b>Run scraper</b> to launch the underlying Python script for that DSP.</li>
            <li>Track progress with a live percentage, elapsed time, and estimated remaining time.</li>
            <li>Explore and download the results from the interactive table.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="section-heading">Choose your DSP</div>', unsafe_allow_html=True)

# ---------- MAIN TABS ----------

dsp_names = list(DSP_OPTIONS.keys())
tabs = st.tabs(dsp_names)

for dsp_name, tab in zip(dsp_names, tabs):
    with tab:
        logo_path = DSP_LOGOS.get(dsp_name)

        head_left, head_right = st.columns([1, 2])

        with head_left:
            if logo_path is not None:
                logo(logo_path, width=80, alt=dsp_name)
            # bigger DSP name
            st.markdown(
                f"<h3 style='margin-bottom:0.3rem;'>{dsp_name} pricing</h3>",
                unsafe_allow_html=True,
            )
            st.markdown(
                "<p>Scrape global subscription prices using your Python scraper.</p>",
                unsafe_allow_html=True,
            )

        with head_right:
            mode_label = st.radio(
                "Mode",
                ["Full", "Test"],
                index=0,
                horizontal=True,
                key=f"mode_{dsp_name}",
            )

            if mode_label == "Test":
                selected_labels = st.multiselect(
                    "Countries (type to search)",
                    COUNTRY_LABELS,
                    key=f"countries_{dsp_name}",
                    help=(
                        "Start typing a country name or code, then press Enter or "
                        "click to add. You can select multiple countries."
                    ),
                )
                selected_codes = [LABEL_TO_CODE[l] for l in selected_labels]
            else:
                selected_codes = []

        st.markdown("")  # spacing

        run_button = st.button(
            f"Run {dsp_name} scraper",
            key=f"run_{dsp_name}",
        )

        if run_button:
            run_and_render(dsp_name, mode_label, selected_codes)
        else:
            show_cached_result(dsp_name, mode_label, selected_codes)

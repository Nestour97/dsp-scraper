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

from dsp_scrapers import DSP_OPTIONS, run_scraper  # your unified scraper

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

# ---------- COUNTRY OPTIONS (for Test mode) ----------

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
    # key -> {df, excel, rows, ts}
    st.session_state["results"] = {}

if "playwright_ready" not in st.session_state:
    st.session_state["playwright_ready"] = False


def result_key(dsp_name: str, mode_label: str, country_codes: list[str]) -> str:
    codes_part = ",".join(sorted(country_codes)) if country_codes else "ALL"
    return f"{dsp_name}::{mode_label}::{codes_part}"


# ---------- STYLE (centered logo + animated hero) ----------

st.markdown(
    """
    <style>
    body {
        background-color: #050509;
    }
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }

    /* animated gradient behind header */
    @keyframes heroGradient {
        0%   { background-position: 0% 50%; }
        50%  { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }

    .header-wrapper {
        text-align: center;
        margin-bottom: 1.4rem;
        padding: 1.2rem 1.4rem 1.3rem 1.4rem;
        border-radius: 20px;
        border: 1px solid #24263b;
        background: linear-gradient(120deg, #101021, #1a0f3a, #101021);
        background-size: 200% 200%;
        animation: heroGradient 18s ease-in-out infinite;
        box-shadow: 0 22px 50px rgba(0,0,0,0.85);
    }
    .header-pill {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 0.16rem 0.75rem;
        border-radius: 999px;
        font-size: 0.78rem;
        background: rgba(0,0,0,0.25);
        color: #f5f5ff;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 0.4rem;
    }
    .header-title {
        font-size: 2.1rem;
        font-weight: 700;
        letter-spacing: 0.03em;
        margin-bottom: 0.25rem;
        color: #ffffff;
    }
    .header-subtitle {
        font-size: 0.96rem;
        color: #cbcbea;
        max-width: 680px;
        margin: 0.1rem auto 0 auto;
    }
    .how-card {
        background-color: #06060b;
        border-radius: 16px;
        padding: 0.85rem 1.1rem;
        border: 1px solid #27283b;
        color: #f3f3f3;
        margin-bottom: 1rem;
    }
    .how-card ul {
        margin-top: 0.3rem;
        margin-bottom: 0;
        padding-left: 1.1rem;
    }
    .how-card li {
        font-size: 0.9rem;
    }
    .section-heading {
        font-size: 1.25rem;
        font-weight: 600;
        margin-top: 0.7rem;
        margin-bottom: 0.35rem;
        color: #f5f5ff;
    }
    .side-note {
        font-size: 0.86rem;
        color: #b4b4c8;
    }

    .ag-theme-streamlit .ag-root-wrapper {
        border-radius: 14px;
        border: 1px solid #444659;
    }
    .ag-theme-streamlit .ag-header {
        background: #0b0c14;
        color: #f1f1ff;
        font-weight: 600;
    }
    .ag-theme-streamlit .ag-row-even {
        background-color: #070710;
    }
    .ag-theme-streamlit .ag-row-odd {
        background-color: #05050b;
    }

    .run-button button {
        border-radius: 999px !important;
        background: linear-gradient(135deg, #7f5af0, #ff6bcb) !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- UTILS ----------


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
    st.subheader("Data explorer (Power BI-style)")

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

    # rough defaults for "Full" runs
    if dsp_name == "Apple Music":
        return 230
    if dsp_name == "Disney+":
        return 90
    return 150


def estimate_expected_seconds(dsp_name: str, mode_label: str, codes: list[str]) -> float:
    n = estimate_country_count(dsp_name, mode_label, codes)

    if dsp_name == "Apple Music":
        per = 3.0  # seconds per country (conservative)
    elif dsp_name == "Disney+":
        per = 4.5
    else:
        per = 3.0

    return max(n * per, 20.0)  # at least 20 seconds for visual smoothness


def logo(path: Path, width: int, alt: str):
    if path.is_file():
        st.image(str(path), width=width)
    else:
        st.markdown(f"**{alt}**")


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
    test_mode = mode_label.startswith("Test")

    if dsp_name == "Disney+":
        ensure_playwright_for_disney()

    st.markdown("### Run status")
    progress = st.progress(0)
    status = st.empty()

    expected_total = estimate_expected_seconds(dsp_name, mode_label, codes)
    start = time.time()

    def run_worker():
        # call into dsp_scrapers.run_scraper
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
                    f"**{pct_est}%** • Elapsed {format_mm_ss(elapsed)} "
                    f"• Est. remaining {format_mm_ss(remaining_est)}",
                    unsafe_allow_html=True,
                )
                time.sleep(0.35)

        try:
            excel_path = future.result()
        except Exception as e:
            progress.progress(0)
            nice_error_box(e)
            return

    # Done
    elapsed = time.time() - start
    progress.progress(100)
    status.markdown(
        f"**100%** • Elapsed {format_mm_ss(elapsed)} • Est. remaining 00:00",
        unsafe_allow_html=True,
    )

    df = load_excel_as_df(excel_path)
    rows = len(df)

    st.session_state["results"][result_key(dsp_name, mode_label, codes)] = {
        "df": df,
        "excel": excel_path,
        "rows": rows,
        "ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    st.success(f"Scraped {rows:,} rows for {dsp_name}.")
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


# ---------- SIDEBAR (mode + ONE search/multiselect) ----------

with st.sidebar:
    st.markdown("### Mode")

    mode_label = st.radio(
        "Choose run mode",
        ["Full (all countries)", "Test (choose countries)"],
        index=0,
        help="Full = scrape every country. Test = pick a subset for quicker runs.",
    )

    selected_codes: list[str] = []

    if mode_label.startswith("Test"):
        st.markdown("#### Countries for test runs")

        # single multiselect with built-in search – type to search, select, keep typing
        selected_labels = st.multiselect(
            "Start typing a country name or code",
            COUNTRY_LABELS,
            help=(
                "Type to filter the list, press Enter or click to add. "
                "You can keep typing to add multiple countries."
            ),
        )
        selected_codes = [LABEL_TO_CODE[l] for l in selected_labels]

    st.markdown(
        "<p class='side-note'>Results stay on screen after a run. "
        "Switch between tabs to compare DSPs.</p>",
        unsafe_allow_html=True,
    )

# ---------- CENTERED HEADER (logo → title → description) ----------

logo_row = st.columns([1, 1, 1])
with logo_row[1]:
    if SONY_LOGO_PATH.is_file():
        st.image(str(SONY_LOGO_PATH), width=120)

st.markdown(
    """
    <div class="header-wrapper">
        <div class="header-pill">DSP analytics tool</div>
        <div class="header-title">DSP Price Scraper</div>
        <p class="header-subtitle">
            Central hub for Apple Music &amp; Disney+ pricing. Run scrapes on demand,
            explore the results in a Power BI-style grid, and export straight to Excel.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="how-card">
        <b>How it works</b>
        <ul>
            <li>Select <b>Apple Music</b> or <b>Disney+</b> in the tabs below.</li>
            <li>Use the sidebar to pick <b>Full</b> or <b>Test</b>. In Test mode you can choose multiple countries from the search box.</li>
            <li>Click <b>Run scraper</b> to launch the underlying Python script.</li>
            <li>Track progress with a live percentage, elapsed time and estimated remaining time.</li>
            <li>Explore and download the results from the interactive table.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="section-heading">Choose your DSP</div>', unsafe_allow_html=True)

# ---------- MAIN TABS ----------

apple_tab, disney_tab = st.tabs(["Apple Music", "Disney+"])

with apple_tab:
    c1, c2 = st.columns([1, 4])

    with c1:
        logo(APPLE_LOGO_PATH, width=80, alt="Apple Music")

    with c2:
        st.markdown("#### Apple Music pricing")
        st.markdown(
            "Scrape global Apple Music plan prices, currencies and country codes."
        )

    run_button = st.button(
        "Run Apple Music scraper",
        key="run_apple",
        help="Launch Apple Music scraper",
        type="primary",
    )

    if run_button:
        run_and_render("Apple Music", mode_label, selected_codes)
    else:
        show_cached_result("Apple Music", mode_label, selected_codes)

with disney_tab:
    c1, c2 = st.columns([1, 4])

    with c1:
        logo(DISNEY_LOGO_PATH, width=90, alt="Disney+")

    with c2:
        st.markdown("#### Disney+ pricing")
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

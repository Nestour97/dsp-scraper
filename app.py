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
    # Add new DSPs here later: "Spotify": Path("spotify_logo.png"), etc.
}

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


# ---------- GLOBAL STYLES (full-width black Sony look) ----------

st.markdown(
    """
    <style>
    /* Entire page */
    body {
        background-color: #000000;
        color: #f5f5f5;
    }

    /* Main content area */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2.5rem;
        padding-left: 4rem;
        padding-right: 4rem;
        max-width: 1400px;
        margin: 0 auto;
        background-color: #000000;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #050505;
        color: #f5f5f5;
    }

    /* Header */
    .header-wrapper {
        text-align: center;
        margin-bottom: 1.6rem;
    }
    .header-title {
        font-size: 2.4rem;
        font-weight: 800;
        letter-spacing: 0.08em;
        margin-top: 0.5rem;
        margin-bottom: 0.35rem;
        color: #ffffff;
        text-transform: uppercase;
    }
    .header-subtitle {
        font-size: 0.98rem;
        color: #d2d2d2;
        max-width: 900px;
        margin: 0 auto;
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
        margin-top: 0.35rem;
    }

    /* How it works card */
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
        color: #bfbfbf;
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

    /* Primary run buttons */
    .run-button button {
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


# ---------- SIDEBAR (mode + multiselect) ----------

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
        selected_labels = st.multiselect(
            "Start typing a country name or code",
            COUNTRY_LABELS,
            help=(
                "Click once, then type to search. "
                "Press Enter or click to add, and keep typing to add more."
            ),
        )
        selected_codes = [LABEL_TO_CODE[l] for l in selected_labels]

    st.markdown(
        "<p class='side-note'>Results stay on screen after a run. "
        "Switch between tabs to compare DSPs.</p>",
        unsafe_allow_html=True,
    )

# ---------- SONY HEADER (logo → big title → description) ----------

logo_row = st.columns([1, 1, 1])
with logo_row[1]:
    if SONY_LOGO_PATH.is_file():
        st.image(str(SONY_LOGO_PATH), width=140)

st.markdown(
    """
    <div class="header-wrapper">
        <div class="header-title">DSP PRICE SCRAPER</div>
        <p class="header-subtitle">
            Central hub for Apple Music &amp; Disney+ pricing. Run scrapes on demand,
            explore the results in a Power BI-style grid, and export straight to Excel.
        </p>
        <div class="header-pill">DSP analytics tool</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="how-card">
        <b>How it works</b>
        <ul>
            <li>Select <b>Apple Music</b> or <b>Disney+</b> in the tabs below (more DSPs can be added later).</li>
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

# ---------- MAIN TABS (ready for more DSPs later) ----------

dsp_names = list(DSP_OPTIONS.keys())
tabs = st.tabs(dsp_names)

for dsp_name, tab in zip(dsp_names, tabs):
    with tab:
        logo_path = DSP_LOGOS.get(dsp_name)
        c1, c2 = st.columns([1, 4])

        with c1:
            if logo_path is not None:
                logo(logo_path, width=80, alt=dsp_name)
            else:
                st.markdown(f"#### {dsp_name}")

        with c2:
            st.markdown(f"#### {dsp_name} pricing")
            st.markdown(
                f"Scrape global {dsp_name} subscription prices using your Python scraper."
            )

        run_button = st.button(
            f"Run {dsp_name} scraper",
            key=f"run_{dsp_name}",
            help=f"Launch {dsp_name} scraper",
        )

        if run_button:
            run_and_render(dsp_name, mode_label, selected_codes)
        else:
            show_cached_result(dsp_name, mode_label, selected_codes)

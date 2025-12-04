import os
import time
import threading
import base64
from pathlib import Path
from typing import Dict, List

import pandas as pd
import pycountry
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from dsp_scrapers import run_scraper

# ---------- GLOBAL CONSTANTS ----------

SONY_RED = "#e31c23"

# "Single source of truth" full-run files per DSP.
# These are the files your scrapers create in full mode.
FULL_RESULT_FILES = {
    "Apple Music": Path("dsp_scrapers/apple_music_plans_all.xlsx"),
    "iCloud+": Path("dsp_scrapers/icloud_plus_pricing_all.xlsx"),
    "Spotify": Path("dsp_scrapers/spotify_cleaned_playwright.xlsx"),
    "Netflix": Path("dsp_scrapers/netflix_pricing_by_country.xlsx"),
    "Disney+": Path("dsp_scrapers/disney_prices_enriched.xlsx"),
}

# Nice labels for the country multiselect
COUNTRY_OPTIONS: List[str] = sorted(
    [f"{c.name} ({c.alpha_2})" for c in pycountry.countries],
    key=str.lower,
)


# ---------- SMALL HELPERS ----------

def _extract_alpha2(selection: List[str]) -> List[str]:
    """Turn ['France (FR)', 'Japan (JP)'] into ['FR', 'JP']."""
    codes: List[str] = []
    for item in selection:
        if "(" in item and ")" in item:
            codes.append(item.split("(")[-1].strip(") ").upper())
    return codes


def centered_sony_logo() -> None:
    logo_path = Path("sony_logo.png")
    if not logo_path.is_file():
        return
    data = base64.b64encode(logo_path.read_bytes()).decode("utf-8")
    st.markdown(
        f"""
        <p style="text-align:center; margin-bottom:0.3rem;">
            <img src="data:image/png;base64,{data}" width="120">
        </p>
        """,
        unsafe_allow_html=True,
    )


def run_with_progress(dsp_name: str, test_mode: bool, test_countries: List[str]):
    """
    Run the selected scraper in a background thread and show a smooth progress bar.
    """
    status_placeholder = st.empty()
    progress = st.progress(0, text=f"Starting {dsp_name} scraper…")

    result: Dict[str, str] = {"path": "", "error": ""}

    def worker():
        try:
            result["path"] = run_scraper(
                dsp_name=dsp_name,
                test_mode=test_mode,
                test_countries=test_countries or None,
            )
        except Exception as e:  # noqa: BLE001
            result["error"] = str(e)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    start = time.time()
    # Very rough estimate: tests quick, full slower
    expected = 90 if test_mode else 600

    while thread.is_alive():
        elapsed = time.time() - start
        pct = min(0.95, elapsed / expected)
        pct_int = int(pct * 100)
        remaining = max(0, int(expected - elapsed))
        progress.progress(
            pct_int,
            text=f"{dsp_name}: {pct_int}% • Est. remaining ~{remaining:02d}s",
        )
        time.sleep(0.5)

    thread.join()

    if result["error"]:
        progress.empty()
        status_placeholder.error(
            f"Error while running {dsp_name}: {result['error']}"
        )
        return None

    progress.progress(100, text=f"{dsp_name}: 100% • Completed")
    status_placeholder.success("Scrape finished successfully.")
    return result["path"]


def render_table(excel_path: Path, dsp_name: str) -> None:
    if not excel_path or not excel_path.exists():
        st.error("File not found – scraper may not have produced an output.")
        return

    st.markdown(f"### 📊 Data explorer – {dsp_name}")

    df = pd.read_excel(excel_path)

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filter=True,
        sortable=True,
        resizable=True,
        floatingFilter=True,
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
        height=520,
        fit_columns_on_grid_load=True,
    )

    with excel_path.open("rb") as f:
        data = f.read()

    st.download_button(
        "📥 Download full Excel file",
        data=data,
        file_name=excel_path.name,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key=f"download_{dsp_name}",
    )


def dsp_panel(dsp_name: str, logo_filename: str, description: str) -> None:
    # Per-DSP results stored in the session
    if "dsp_results" not in st.session_state:
        st.session_state["dsp_results"] = {}
    results_dict: Dict[str, Path] = st.session_state["dsp_results"]

    # Header row
    col_logo, col_text = st.columns([1, 5])
    with col_logo:
        if os.path.exists(logo_filename):
            st.image(logo_filename, width=56)
    with col_text:
        st.markdown(
            f"#### {dsp_name}\n"
            f"<p class='small-text'>{description}</p>",
            unsafe_allow_html=True,
        )

    # Mode selector
    st.markdown("##### Mode")
    mode = st.radio(
        "Mode",
        options=["Full (all countries)", "Test (quick run)"],
        horizontal=True,
        label_visibility="collapsed",
        key=f"mode_{dsp_name}",
    )
    test_mode = mode.startswith("Test")

    # Test countries multiselect
    selected_codes: List[str] = []
    if test_mode:
        st.markdown("##### Countries for test runs (optional)")
        selected_labels = st.multiselect(
            "Start typing a country name or code",
            options=COUNTRY_OPTIONS,
            default=st.session_state.get(f"test_countries_{dsp_name}", []),
            label_visibility="collapsed",
            key=f"countries_{dsp_name}",
        )
        st.session_state[f"test_countries_{dsp_name}"] = selected_labels
        selected_codes = _extract_alpha2(selected_labels)

    st.write("")

    # For FULL mode, if a global cached file exists on disk and nothing
    # has been run in this session yet, pre-load it so everyone shares
    # the same 'truth' without re-running.
    full_cache_path = FULL_RESULT_FILES.get(dsp_name)
    if (
        not test_mode
        and dsp_name not in results_dict
        and full_cache_path is not None
        and full_cache_path.exists()
    ):
        st.info(
            "Using cached full run from disk for this DSP. "
            "Run a new full scrape if you want to refresh it."
        )
        results_dict[dsp_name] = full_cache_path

    # Run button
    if st.button(f"🚀 Run {dsp_name} scraper", key=f"run_{dsp_name}"):
        path_str = run_with_progress(
            dsp_name=dsp_name,
            test_mode=test_mode,
            test_countries=selected_codes,
        )
        if path_str:
            results_dict[dsp_name] = Path(path_str)
            # If this was a full run, and we know the cache file location,
            # copy/overwrite it so future users see the refreshed data.
            if not test_mode and full_cache_path is not None:
                try:
                    Path(path_str).replace(full_cache_path)
                except Exception:  # noqa: BLE001
                    # If moving fails, at least keep the session copy.
                    pass

    # Show latest result for this DSP (full or test for this user)
    if dsp_name in results_dict:
        st.markdown("---")
        render_table(results_dict[dsp_name], dsp_name)
    else:
        if not test_mode and full_cache_path is not None:
            st.warning(
                "No full run cached yet for this DSP – run a full scrape to "
                "populate it."
            )


# ---------- STREAMLIT PAGE CONFIG & STYLES ----------

st.set_page_config(
    page_title="DSP Price Scraper",
    page_icon="🎧",
    layout="wide",
)

st.markdown(
    f"""
    <style>
    body {{
        background-color: #000000;
        color: #f5f5f5;
    }}
    [data-testid="stSidebar"] {{
        display: none;
    }}
    .block-container {{
        padding-top: 2rem;
        padding-bottom: 2.5rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
        max-width: 1700px;
        margin-left: auto;
        margin-right: auto;
        background-color: #000000;
    }}
    h1, h2, h3, h4, h5, h6, label, p {{
        color: #f5f5f5 !important;
    }}
    .header-wrapper {{
        text-align: center;
        max-width: 900px;
        margin-left: auto;
        margin-right: auto;
        margin-bottom: 1.8rem;
    }}
    .header-title {{
        font-size: 2.5rem;
        font-weight: 800;
        letter-spacing: 0.09em;
        margin-top: 0.6rem;
        margin-bottom: 0.35rem;
        color: #ffffff;
        text-transform: uppercase;
    }}
    .header-subtitle {{
        font-size: 0.98rem;
        color: #f2f2f2;
        margin: 0 auto 0.5rem auto;
    }}
    .header-pill {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 0.16rem 0.9rem;
        border-radius: 999px;
        font-size: 0.76rem;
        background: {SONY_RED};
        color: #ffffff;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-top: 0.25rem;
    }}
    .how-card {{
        background-color: #050505;
        border-radius: 0.8rem;
        padding: 0.9rem 1.3rem;
        border: 1px solid #262626;
        color: #f5f5f5;
        margin-bottom: 1.2rem;
    }}
    .how-card ul {{
        margin-top: 0.35rem;
        margin-bottom: 0;
        padding-left: 1.1rem;
    }}
    .how-card li {{
        font-size: 0.9rem;
    }}
    .section-heading {{
        font-size: 1.2rem;
        font-weight: 600;
        margin-top: 0.9rem;
        margin-bottom: 0.4rem;
        color: #ffffff;
    }}
    .stTabs [role="tablist"] {{
        justify-content: center;
    }}
    .stTabs [role="tab"] p {{
        font-size: 1.02rem;
        font-weight: 600;
    }}
    div.stButton > button {{
        border-radius: 999px !important;
        background: {SONY_RED} !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding-left: 1.3rem !important;
        padding-right: 1.3rem !important;
    }}
    .stDownloadButton > button {{
        border-radius: 999px !important;
        background: {SONY_RED} !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding-left: 1.3rem !important;
        padding-right: 1.3rem !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------- HEADER ----------

centered_sony_logo()

st.markdown(
    """
    <div class="header-wrapper">
        <div class="header-title">DSP PRICE SCRAPER</div>
        <p class="header-subtitle">
            Central hub for Apple Music, iCloud+, Spotify, Netflix &amp; Disney+ pricing.
            Run scrapes on demand, explore the results in a Power BI-style grid,
            and export straight to Excel.
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
            <li>Select <b>Apple</b>, <b>Spotify</b>, <b>Netflix</b> or <b>Disney+</b> in the tabs below.</li>
            <li>Within Apple you can choose between <b>Apple Music</b> and <b>iCloud+</b>.</li>
            <li>Use <b>Full</b> mode for a complete global run, or <b>Test</b> for a quick sample.</li>
            <li>Click <b>Run scraper</b> to launch the underlying Python code for that DSP.</li>
            <li>Track progress with a live percentage, elapsed time and estimated remaining time.</li>
            <li>Explore and download the results from the interactive table.</li>
        </ul>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-heading">Choose your DSP</div>',
    unsafe_allow_html=True,
)

# ---------- MAIN TABS ----------

main_tabs = st.tabs(["Apple", "Spotify", "Netflix", "Disney+"])

# Apple: Apple Music + iCloud+
with main_tabs[0]:
    apple_tabs = st.tabs(["Apple Music", "iCloud+"])

    with apple_tabs[0]:
        dsp_panel(
            dsp_name="Apple Music",
            logo_filename="apple_music_logo.png",
            description=(
                "Scrape global Apple Music subscription prices, "
                "currencies and country codes."
            ),
        )

    with apple_tabs[1]:
        dsp_panel(
            dsp_name="iCloud+",
            logo_filename="icloud_logo.png",
            description=(
                "Scrape iCloud+ storage plan prices by country, "
                "including plan size and currency."
            ),
        )

# Spotify
with main_tabs[1]:
    dsp_panel(
        dsp_name="Spotify",
        logo_filename="spotify_logo.png",
        description=(
            "Scrape Spotify Premium plan prices by country using "
            "the Playwright-based scraper."
        ),
    )

# Netflix
with main_tabs[2]:
    dsp_panel(
        dsp_name="Netflix",
        logo_filename="netflix_logo.png",
        description=(
            "Scrape Netflix plan pricing for each available country "
            "from your global pricing table."
        ),
    )

# Disney+
with main_tabs[3]:
    dsp_panel(
        dsp_name="Disney+",
        logo_filename="disney_plus_logo.png",
        description=(
            "Scrape Disney+ subscription pricing using the "
            "Playwright-powered scraper."
        ),
    )

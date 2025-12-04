# dsp_scrapers/netflix_scraper.py

from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import pycountry

# This is the "one version of the truth" file for full runs.
# Put your full Netflix pricing file here (you already have this from Colab).
EXCEL_PATH = Path(__file__).with_name("netflix_pricing_by_country.xlsx")


def _load_full_table() -> pd.DataFrame:
    """
    Load the full Netflix pricing table from disk. This is the global 'truth'
    used for both full and test runs.

    To refresh the data, overwrite EXCEL_PATH (e.g. by running your
    original Playwright scraper offline or in a separate script).
    """
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(
            f"Netflix Excel file not found at {EXCEL_PATH}. "
            "Make sure netflix_pricing_by_country.xlsx is placed next to "
            "netflix_scraper.py or update EXCEL_PATH."
        )
    return pd.read_excel(EXCEL_PATH)


def _country_name_to_alpha2(name: str) -> Optional[str]:
    """
    Convert a country name from the Excel (e.g. 'South Korea') into
    an ISO-2 code (e.g. 'KR') using pycountry.
    """
    name = str(name).strip()
    if not name:
        return None
    try:
        return pycountry.countries.lookup(name).alpha_2
    except LookupError:
        return None


def _filter_by_iso2(df: pd.DataFrame, iso2_codes: Iterable[str]) -> pd.DataFrame:
    """
    Filter the Netflix dataframe by a list of ISO-2 country codes.
    """
    iso2_list: List[str] = [c.upper() for c in iso2_codes] if iso2_codes else []
    if not iso2_list:
        return df

    df = df.copy()

    # If your file already has a code column, use it.
    code_col = None
    for candidate in ["CountryCode", "Country_Code", "ISO2", "ISO_2"]:
        if candidate in df.columns:
            code_col = candidate
            break

    if code_col is None:
        # Build a new code column from 'Country'
        if "Country" not in df.columns:
            raise KeyError(
                "Netflix Excel is missing a 'Country' column. "
                "Either add one or adjust _filter_by_iso2()."
            )
        df["CountryCode"] = df["Country"].apply(_country_name_to_alpha2)
        code_col = "CountryCode"

    return df[df[code_col].isin(iso2_list)].copy()


def run_netflix_scraper(
    test_mode: bool,
    test_countries: Optional[Iterable[str]] = None,
) -> str:
    """
    Unified entrypoint used by the Streamlit app.

    Parameters
    ----------
    test_mode:
        - False -> return the global full table (EXCEL_PATH).
        - True  -> return a *filtered* subset for the selected ISO-2 countries.
    test_countries:
        Iterable of ISO-2 codes (e.g. ['KR', 'DE']). Only used in test_mode.

    Returns
    -------
    str
        Path to the Excel file that the app should display.
    """
    df_full = _load_full_table()

    if not test_mode:
        # Full mode: just return the shared "truth" file.
        # If you want to re-scrape from Netflix instead, call your Playwright
        # script here, then overwrite EXCEL_PATH before returning it.
        return str(EXCEL_PATH)

    # Test mode: filter by the selected countries and save a temp file.
    filtered = _filter_by_iso2(df_full, test_countries or [])
    out_path = EXCEL_PATH.with_name("netflix_pricing_by_country_TEST.xlsx")
    filtered.to_excel(out_path, index=False)
    return str(out_path)

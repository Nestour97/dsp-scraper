# dsp_scrapers/icloud_plus_scraper.py

import math
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- Constants ---------------------------------------------------------------

URL = "https://support.apple.com/en-gb/108047"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}

# For TEST mode we’ll only keep a few countries so the run is quick
TEST_COUNTRIES = [
    "United Kingdom",
    "United States",
    "Brazil",
    "Japan",
    "South Africa",
]

OUT_BASENAME = "icloud_plus_pricing"


# --- Normalisation helpers ---------------------------------------------------

def norm(s: str) -> str:
    """Normalise whitespace + odd unicode characters."""
    if not s:
        return ""
    s = (
        s.replace("\xa0", " ")
        .replace("\u202f", " ")
        .replace("\u2009", " ")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("\ufeff", "")
        .replace("\u00ad", "")
        .replace("\u00ac", " ")
    )
    s = s.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", s).strip()


COUNTRY_RE = re.compile(
    r"""
    ^(?P<country>.+?)\s*(?:\d+(?:,\d+)*)?\s*
    \((?P<ccy>[A-Z]{2,5}|Euro)\)\s*(?:\d+(?:,\d+)*)?\s*$
    """,
    re.VERBOSE,
)

PLAN_RE = re.compile(
    r"""^\s*(?P<size>\d+)\s*[^A-Za-z0-9]{0,5}\s*(?P<unit>GB|TB)\s*[:\-]?\s*(?P<price>.+?)\s*$""",
    re.IGNORECASE,
)

NUM_TOKEN = re.compile(r"\d[\d.,]*")


def standardize_plan(size: str, unit: str) -> str:
    return f"{int(size)} {unit.upper()}"


def parse_numeric_price(s: str) -> float:
    """Turn a messy price string into a float, or NaN on failure."""
    if not s:
        return math.nan
    s = norm(s)
    s = re.sub(r"^[^\d]+", "", s)  # strip leading currency text/symbols

    m = NUM_TOKEN.search(s)
    if not m:
        return math.nan

    token = m.group(0)

    if "," in token and "." in token:
        last_c, last_d = token.rfind(","), token.rfind(".")
        dec = "," if last_c > last_d else "."
        thou = "." if dec == "," else ","
        token = token.replace(thou, "").replace(dec, ".")
    elif "," in token:
        parts = token.split(",")
        token = token.replace(",", ".") if len(parts[-1]) == 2 else token.replace(",", "")
    elif "." in token:
        parts = token.split(".")
        if len(parts) > 2 and all(len(p) == 3 for p in parts[1:]) and len(parts[-1]) in (
            3,
            0,
        ):
            token = "".join(parts)

    try:
        return float(token)
    except Exception:
        return math.nan


# --- Scraping core -----------------------------------------------------------

def fetch_html() -> str:
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


def parse_all_rows(html: str) -> List[Dict[str, Any]]:
    """Parse the support article into per-country iCloud+ plans."""
    soup = BeautifulSoup(html, "lxml")
    paras = soup.select("p.gb-paragraph") or soup.select("article p, main p")

    rows: List[Dict[str, Any]] = []
    cur_country, cur_ccy = None, None

    for p in paras:
        text = norm(p.get_text(" ", strip=True))
        if not text:
            continue

        mc = COUNTRY_RE.match(text)
        if mc:
            country = re.sub(r"\d+$", "", mc.group("country").strip())
            cur_country, cur_ccy = country, mc.group("ccy").strip()
            continue

        mp = PLAN_RE.match(text)
        if mp and cur_country and cur_ccy:
            rows.append(
                {
                    "Country": cur_country,
                    "Currency": cur_ccy,
                    "Plan": standardize_plan(mp.group("size"), mp.group("unit")),
                    "Price": parse_numeric_price(mp.group("price").strip()),
                    "Price_Display": mp.group("price").strip(),
                }
            )

    return rows


# --- Public API --------------------------------------------------------------

def run_icloud_plus_scraper(test_mode: bool = True) -> str:
    """
    Run the iCloud+ scraper.

    test_mode=True  -> keep a small sample of countries
    test_mode=False -> return all countries from the article

    Returns
    -------
    str
        Absolute path to the created Excel file.
    """
    html = fetch_html()
    rows = parse_all_rows(html)
    if not rows:
        raise RuntimeError("iCloud+ scraper parsed 0 rows – Apple may have changed the page.")

    df = pd.DataFrame(rows, columns=["Country", "Currency", "Plan", "Price", "Price_Display"])

    # Order plans nicely
    try:
        plan_order = pd.CategoricalDtype(["50 GB", "200 GB", "2 TB", "6 TB", "12 TB"], ordered=True)
        df["Plan"] = df["Plan"].astype(plan_order)
    except Exception:
        pass

    df.sort_values(["Country", "Plan"], inplace=True, kind="stable", ignore_index=True)

    if test_mode:
        df = df[df["Country"].isin(TEST_COUNTRIES)].copy()

    suffix = "_TEST" if test_mode else "_all"
    out_path = Path(f"{OUT_BASENAME}{suffix}.xlsx").resolve()

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="iCloud+ Prices")

    return str(out_path)

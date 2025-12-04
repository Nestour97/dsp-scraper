
# dsp_scrapers/netflix_scraper.py
# dsp_scrapers/netflix_scraper.py


import asyncio
import asyncio
import re
import re
from pathlib import Path
from pathlib import Path
from typing import List, Dict, Any
from typing import Any, Dict, List


import pandas as pd
import pandas as pd
from bs4 import BeautifulSoup
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright.async_api import async_playwright




# --- Helpers taken from your notebook ---------------------------------------
# --- Helpers taken from your notebook ---------------------------------------


def extract_price_details(price_text: str):
def extract_price_details(price_text: str):
    """
    """
    Split 'X CUR / month (note...)' into (currency, amount, note, raw_text).
    Split 'X CUR / month (note...)' into (currency, amount, note, raw_text).
    """
    """
    if not price_text or "month" not in price_text.lower():
    if not price_text or "month" not in price_text.lower():
        return "Unknown", "", price_text, price_text
        return "Unknown", "", price_text, price_text


    text = price_text.strip()
    text = price_text.strip()
    month_split = re.split(r"/\s*month", text, flags=re.IGNORECASE)
    month_split = re.split(r"/\s*month", text, flags=re.IGNORECASE)
    price_part = month_split[0].strip()
    price_part = month_split[0].strip()
    note_part = month_split[1].strip() if len(month_split) > 1 else ""
    note_part = month_split[1].strip() if len(month_split) > 1 else ""


    number_match = re.search(r"([\d,.]+)", price_part)
    number_match = re.search(r"([\d,.]+)", price_part)
    currency_match = re.search(r"([^\d\s,.]+)", price_part)
    currency_match = re.search(r"([^\d\s,.]+)", price_part)


    amount = number_match.group(1).replace(",", "") if number_match else ""
    amount = number_match.group(1).replace(",", "") if number_match else ""
    currency = currency_match.group(1) if currency_match else "Unknown"
    currency = currency_match.group(1) if currency_match else "Unknown"
@@ -110,106 +110,169 @@ async def process_country(country_label: str, page) -> List[Dict[str, Any]]:
async def _run_netflix_async(
async def _run_netflix_async(
    test_mode: bool = True, test_countries: List[str] | None = None
    test_mode: bool = True, test_countries: List[str] | None = None
) -> str:
) -> str:
    """
    """
    Scrape Netflix pricing for all countries (or a subset in test mode).
    Scrape Netflix pricing for all countries (or a subset in test mode).


    Returns absolute path to the Excel file.
    Returns absolute path to the Excel file.
    """
    """
    async with async_playwright() as pw:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        context = await browser.new_context()


        # First page just to get the list of countries
        # First page just to get the list of countries
        page = await context.new_page()
        page = await context.new_page()
        await page.goto("https://help.netflix.com/en/node/24926", timeout=60000)
        await page.goto("https://help.netflix.com/en/node/24926", timeout=60000)
        await page.wait_for_timeout(3000)
        await page.wait_for_timeout(3000)


        try:
        try:
            await page.click("#onetrust-accept-btn-handler", timeout=3000)
            await page.click("#onetrust-accept-btn-handler", timeout=3000)
        except Exception:
        except Exception:
            pass
            pass


        countries_data = await page.evaluate("window.netflix.data.article.allCountries")
        countries_data = await page.evaluate("window.netflix.data.article.allCountries")
        all_countries = [entry["label"] for entry in countries_data]
        all_countries = [entry["label"] for entry in countries_data]
        label_to_iso = {name: _country_name_to_iso2(name) for name in all_countries}
        label_to_iso = {name: _country_name_to_iso2(name) for name in all_countries}
        iso_to_labels: Dict[str, List[str]] = {}
        for label, iso in label_to_iso.items():
            if iso:
                iso_to_labels.setdefault(iso, []).append(label)
        await page.close()
        await page.close()


        suffix = "_TEST" if test_mode else ""
        suffix = "_TEST" if test_mode else ""


        if test_mode:
        if test_mode:
            if test_countries:
            if test_countries:
                wanted = {c.upper() for c in test_countries}
                wanted = [c.upper() for c in test_countries if c]
                countries = [
                countries: List[str] = []
                    name
                unmatched: List[str] = []
                    for name, iso in label_to_iso.items()

                    if iso and iso in wanted
                for iso_code in wanted:
                ]
                    labels = _labels_for_iso(iso_code, iso_to_labels, all_countries)
                    if labels:
                        countries.extend(labels)
                    else:
                        unmatched.append(iso_code)

                # Deduplicate while preserving order
                seen_labels: set[str] = set()
                countries = [c for c in countries if not (c in seen_labels or seen_labels.add(c))]

                if unmatched:
                    print(
                        "⚠️ Netflix: no country label match for ISO codes: "
                        + ", ".join(unmatched)
                    )

                if not countries:
                if not countries:
                    print(
                    print(
                        "⚠️ Netflix: no matches for requested ISO codes; using quick sample."
                        "⚠️ Netflix: no matches for requested ISO codes; using quick sample."
                    )
                    )
                    countries = all_countries[:8]
                    countries = all_countries[:8]
            else:
            else:
                # Default quick sample run
                # Default quick sample run
                countries = all_countries[:8]
                countries = all_countries[:8]
        else:
        else:
            countries = all_countries
            countries = all_countries


        print(f"🌍 Netflix: scraping {len(countries)} countries (test_mode={test_mode})")
        print(f"🌍 Netflix: scraping {len(countries)} countries (test_mode={test_mode})")


        results: List[Dict[str, Any]] = []
        results: List[Dict[str, Any]] = []
        batch_size = 6
        batch_size = 6


        for i in range(0, len(countries), batch_size):
        for i in range(0, len(countries), batch_size):
            batch = countries[i : i + batch_size]
            batch = countries[i : i + batch_size]
            tasks = []
            tasks = []
            pages = []
            pages = []


            for country in batch:
            for country in batch:
                tab = await context.new_page()
                tab = await context.new_page()
                pages.append(tab)
                pages.append(tab)
                tasks.append(process_country(country, tab))
                tasks.append(process_country(country, tab))


            batch_results = await asyncio.gather(*tasks)
            batch_results = await asyncio.gather(*tasks)
            for res in batch_results:
            for res in batch_results:
                results.extend(res)
                results.extend(res)


            for p in pages:
            for p in pages:
                await p.close()
                await p.close()


        await browser.close()
        await browser.close()


    df = pd.DataFrame(results)
    df = pd.DataFrame(results)
    out_name = f"netflix_pricing_by_country{suffix}.xlsx"
    out_name = f"netflix_pricing_by_country{suffix}.xlsx"
    out_path = Path(out_name).resolve()
    out_path = Path(out_name).resolve()
    df.to_excel(out_path, index=False, engine="openpyxl")
    df.to_excel(out_path, index=False, engine="openpyxl")


    print(f"✅ Netflix: saved {out_path}")
    print(f"✅ Netflix: saved {out_path}")
    return str(out_path)
    return str(out_path)




import pycountry
import pycountry
# ... keep your other imports
# ... keep your other imports



def _country_name_to_iso2(name: str) -> str:
def _country_name_to_iso2(name: str) -> str:
    """Best-effort map from 'Korea, Republic of' -> 'KR' etc."""
    """Best-effort map from 'Korea, Republic of' -> 'KR' etc."""
    try:
    try:
        return pycountry.countries.lookup(name).alpha_2.upper()
        return pycountry.countries.lookup(name).alpha_2.upper()
    except Exception:
    except Exception:
        return ""
        return ""




def _normalize_country_key(name: str) -> str:
    """Lowercase and strip non-alphanumerics for loose matching."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _labels_for_iso(
    iso_code: str, iso_to_labels: Dict[str, List[str]], all_labels: List[str]
) -> List[str]:
    """Resolve an ISO alpha-2 code to Netflix country labels (best effort)."""
    if iso_code in iso_to_labels:
        return iso_to_labels[iso_code]

    country = pycountry.countries.get(alpha_2=iso_code)
    if not country:
        return []

    candidates: List[str] = []
    norm_name = _normalize_country_key(country.name)
    norm_official = _normalize_country_key(getattr(country, "official_name", ""))

    for label in all_labels:
        norm_label = _normalize_country_key(label)
        if norm_label == norm_name or norm_name in norm_label or norm_label in norm_name:
            candidates.append(label)
        elif norm_official and (
            norm_label == norm_official
            or norm_official in norm_label
            or norm_label in norm_official
        ):
            candidates.append(label)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_candidates: List[str] = []
    for label in candidates:
        if label not in seen:
            seen.add(label)
            unique_candidates.append(label)
    return unique_candidates


def scrape_all_netflix_countries() -> str:
def scrape_all_netflix_countries() -> str:
    """Compatibility wrapper for older callers expecting this function name."""
    """Compatibility wrapper for older callers expecting this function name."""
    return asyncio.run(_run_netflix_async(test_mode=False))
    return asyncio.run(_run_netflix_async(test_mode=False))




def run_netflix_scraper(test_mode: bool = True, test_countries=None) -> str:
def run_netflix_scraper(test_mode: bool = True, test_countries=None) -> str:
    """
    """
    Wrapper used by the Streamlit app.
    Wrapper used by the Streamlit app.


    test_mode = True  -> scrape only the selected countries (or a small sample)
    test_mode = True  -> scrape only the selected countries (or a small sample)
    test_mode = False -> scrape all countries
    test_mode = False -> scrape all countries
    test_countries    -> list of ISO alpha-2 codes from the UI (e.g. ["KR"])
    test_countries    -> list of ISO alpha-2 codes from the UI (e.g. ["KR"])
    """
    """
    return asyncio.run(_run_netflix_async(test_mode=test_mode, test_countries=test_countries))
    return asyncio.run(
        _run_netflix_async(test_mode=test_mode, test_countries=test_countries)
    )

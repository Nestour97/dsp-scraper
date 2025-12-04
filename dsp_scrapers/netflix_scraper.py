# dsp_scrapers/netflix_scraper.py

import asyncio
import re
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


# --- Helpers taken from your notebook ---------------------------------------

def extract_price_details(price_text: str):
    """
    Split 'X CUR / month (note...)' into (currency, amount, note, raw_text).
    """
    if not price_text or "month" not in price_text.lower():
        return "Unknown", "", price_text, price_text

    text = price_text.strip()
    month_split = re.split(r"/\s*month", text, flags=re.IGNORECASE)
    price_part = month_split[0].strip()
    note_part = month_split[1].strip() if len(month_split) > 1 else ""

    number_match = re.search(r"([\d,.]+)", price_part)
    currency_match = re.search(r"([^\d\s,.]+)", price_part)

    amount = number_match.group(1).replace(",", "") if number_match else ""
    currency = currency_match.group(1) if currency_match else "Unknown"

    return currency, amount, note_part, text


async def process_country(country_label: str, page) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []

    try:
        await page.goto("https://help.netflix.com/en/node/24926", timeout=60000)
        await page.wait_for_timeout(2000)

        # Cookie banner
        try:
            await page.click("#onetrust-accept-btn-handler", timeout=3000)
        except Exception:
            pass

        # Open country selector
        await page.click("div.css-hlgwow", timeout=5000)
        input_box = await page.wait_for_selector('//input[@type="text"]')
        await input_box.fill("")
        await input_box.type(country_label)
        await input_box.press("Enter")
        await page.wait_for_timeout(3000)

        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        pricing_header = soup.find("h3", string=lambda s: s and "Pricing" in s)
        if pricing_header:
            ul = pricing_header.find_next("ul")
            if ul:
                for li in ul.find_all("li"):
                    if ":" in li.text:
                        plan, price_text = li.text.strip().split(":", 1)
                        currency, amount, note, raw = extract_price_details(price_text.strip())
                        results.append(
                            {
                                "Country": country_label,
                                "Plan": plan.strip(),
                                "Price_Display": raw,
                                "Currency": currency,
                                "Amount": amount,
                                "Note": note,
                            }
                        )

        if not results:
            results.append(
                {
                    "Country": country_label,
                    "Plan": "N/A",
                    "Price_Display": "N/A",
                    "Currency": "",
                    "Amount": "",
                    "Note": "",
                }
            )

        print(f"✅ {country_label}")
        return results

    except Exception as e:
        print(f"❌ Error: {country_label} — {e}")
        return [
            {
                "Country": country_label,
                "Plan": "ERROR",
                "Price_Display": str(e),
                "Currency": "",
                "Amount": "",
                "Note": "",
            }
        ]


# --- Main async runner -------------------------------------------------------

async def _run_netflix_async(test_mode: bool = True) -> str:
    """
    Scrape Netflix pricing for all countries (or a subset in test mode).

    Returns absolute path to the Excel file.
    """
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()

        # First page just to get the list of countries
        page = await context.new_page()
        await page.goto("https://help.netflix.com/en/node/24926", timeout=60000)
        await page.wait_for_timeout(3000)

        try:
            await page.click("#onetrust-accept-btn-handler", timeout=3000)
        except Exception:
            pass

        countries_data = await page.evaluate("window.netflix.data.article.allCountries")
        all_countries = [entry["label"] for entry in countries_data]
        await page.close()

        if test_mode:
            # Quick sample run
            countries = all_countries[:8]
            suffix = "_TEST"
        else:
            countries = all_countries
            suffix = ""

        print(f"🌍 Netflix: scraping {len(countries)} countries (test_mode={test_mode})")

        results: List[Dict[str, Any]] = []
        batch_size = 6

        for i in range(0, len(countries), batch_size):
            batch = countries[i : i + batch_size]
            tasks = []
            pages = []

            for country in batch:
                tab = await context.new_page()
                pages.append(tab)
                tasks.append(process_country(country, tab))

            batch_results = await asyncio.gather(*tasks)
            for res in batch_results:
                results.extend(res)

            for p in pages:
                await p.close()

        await browser.close()

    df = pd.DataFrame(results)
    out_name = f"netflix_pricing_by_country{suffix}.xlsx"
    out_path = Path(out_name).resolve()
    df.to_excel(out_path, index=False, engine="openpyxl")

    print(f"✅ Netflix: saved {out_path}")
    return str(out_path)


import pycountry
# ... keep your other imports

def _country_name_to_iso2(name: str) -> str:
    """Best-effort map from 'Korea, Republic of' -> 'KR' etc."""
    try:
        return pycountry.countries.lookup(name).alpha_2.upper()
    except Exception:
        return ""

def run_netflix_scraper(test_mode: bool = True, test_countries=None) -> str:
    """
    Wrapper used by the Streamlit app.

    test_mode = True  -> we post-filter to the selected countries
    test_mode = False -> full list
    test_countries    -> list of ISO alpha-2 codes from the UI (e.g. ["KR"])
    """
    # ---- existing scraping logic ----
    # This part should be whatever you currently run to produce the full file.
    # At the end you have a DataFrame `df_full` and you save it once.

    df_full = scrape_all_netflix_countries()   # <-- this represents your existing logic
    out_name = "netflix_pricing_by_country.xlsx"
    df_full.to_excel(out_name, index=False)

    # ---- new test filtering ----
    # If not in Test mode or no UI countries provided, just return full file.
    if not test_mode or not test_countries:
        return out_name

    wanted = {c.upper() for c in test_countries}

    df = df_full.copy()
    df["iso2"] = df["Country"].apply(_country_name_to_iso2)
    df = df[df["iso2"].isin(wanted)].drop(columns=["iso2"])

    test_out = "netflix_pricing_by_country_TEST.xlsx"
    df.to_excel(test_out, index=False)
    return test_out

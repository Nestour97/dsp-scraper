# apple_music_plans_robust.py
# ----------------------------------------------
# ✅ Produces an Excel with Apple Music plan prices per country.
# ✅ Works in FULL mode (all countries) or TEST/SUBSET mode.
# ✅ Uses robust fetching with redirects + fallbacks.
# ✅ Logs issues to CSV + SQLite.
#
# NOTE (Fix for Streamlit “stale output” issue):
# - Previously, Apple Music could scrape 0 rows for selected countries (storefront redirects/mismatch),
#   which meant NO new Excel was written. Streamlit then displayed an OLD Excel from a previous run.
# - This version ALWAYS writes an output file (even if rows are placeholders),
#   and ALWAYS returns the actual output path.
# ----------------------------------------------

import os
import re
import time
import json
import math
import queue
import sqlite3
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import pandas as pd
import pycountry
import requests
from bs4 import BeautifulSoup

# ================== CONFIG ==================

TEST_MODE = True
TEST_COUNTRIES = ["GB", "US", "DE", "FR", "JP"]

# If Apple Music doesn't list in ISO countries (or redirects), we sometimes include extra storefront-ish regions.
EXTRA_REGIONS = set()

# Plans we output (Apple Music):
TIER_ORDER = ["Individual", "Student", "Family"]

# Output names
OUT_TEST = "apple_music_plans_TEST.xlsx"
OUT_ALL = "apple_music_plans_all.xlsx"

# Logging of missing/redirect issues
MISSING_CSV = "apple_music_missing.csv"
MISSING_DB = "apple_music_missing.sqlite"

# Concurrency controls
MAX_WORKERS = 10
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "en-GB,en;q=0.9",
    }
)

BANNER_SEMAPHORE = threading.BoundedSemaphore(6)
MISSING_BUFFER = []  # collected issues for saving

# ================== HELPERS ==================


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _cast_num(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            if math.isnan(v):
                return None
            return float(v)
        return float(str(v).strip())
    except Exception:
        return None


def get_country_name_from_code(alpha2: str) -> str:
    try:
        c = pycountry.countries.get(alpha_2=alpha2.upper())
        if c:
            return c.name
    except Exception:
        pass
    return alpha2.upper()


def normalize_country_name(name: str) -> str:
    return _clean(name)


def init_missing_db():
    try:
        conn = sqlite3.connect(MISSING_DB)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS missing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                country TEXT,
                country_code TEXT,
                url TEXT,
                reason TEXT
            )
            """
        )
        conn.commit()
        conn.close()
    except Exception:
        # If DB fails, we still keep CSV in memory
        pass


def log_missing(country: str, cc: str, url: str, reason: str):
    item = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "country": country,
        "country_code": cc,
        "url": url,
        "reason": reason,
    }
    MISSING_BUFFER.append(item)
    try:
        conn = sqlite3.connect(MISSING_DB)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO missing (ts, country, country_code, url, reason) VALUES (?, ?, ?, ?, ?)",
            (item["ts"], item["country"], item["country_code"], item["url"], item["reason"]),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def looks_like_us_hub_url(url: str) -> bool:
    # Apple sometimes redirects some locales to a generic /us/ hub /music/ pages.
    try:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if len(parts) >= 1 and parts[0].lower() == "us":
            return True
    except Exception:
        return False
    return False


def _extract_cc(url: str) -> str:
    """Extract storefront country code from URLs like https://music.apple.com/gb/..."""
    try:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if parts and len(parts[0]) == 2:
            return parts[0].upper()
    except Exception:
        pass
    return ""


# ================== PRICE EXTRACTION ==================


def pick_recurring_price_token(text: str):
    """
    Attempt to pick the 'recurring' price token from Apple Music banner text.
    Returns (token_str, numeric_value, debug_info).
    """
    t = _clean(text)
    if not t:
        return None, None, {"reason": "empty_text"}

    # Find currency-like + number patterns:
    # Examples: "£10.99/month", "€10,99 per month", "$10.99/mo", etc.
    # We'll capture token and value.
    patterns = [
        r"([£€$]\s?\d+[.,]?\d*)\s*(?:/|per\s+)?\s*(?:month|mo|mth)\b",
        r"(\d+[.,]?\d*)\s*([£€$])\s*(?:/|per\s+)?\s*(?:month|mo|mth)\b",
    ]

    candidates = []
    for pat in patterns:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            tok = m.group(0)
            num = None
            # Extract a number
            n = re.search(r"\d+[.,]?\d*", tok)
            if n:
                raw = n.group(0).replace(",", ".")
                try:
                    num = float(raw)
                except Exception:
                    num = None
            candidates.append((tok, num))

    # If no "per month" tokens, try a looser pass (some pages omit period words)
    if not candidates:
        for m in re.finditer(r"([£€$]\s?\d+[.,]?\d*)", t):
            tok = m.group(0)
            n = re.search(r"\d+[.,]?\d*", tok)
            num = None
            if n:
                raw = n.group(0).replace(",", ".")
                try:
                    num = float(raw)
                except Exception:
                    num = None
            candidates.append((tok, num))

    # Prefer the first token with a valid numeric
    for tok, num in candidates:
        if tok and num is not None:
            return tok, num, {"picked": tok, "num": num, "candidates": candidates[:5]}

    # Fallback
    if candidates:
        tok, num = candidates[0]
        return tok, num, {"picked": tok, "num": num, "candidates": candidates[:5]}
    return None, None, {"reason": "no_candidates"}


# ================== MUSIC.APPLE.COM BANNER FALLBACK ==================


async def _get_music_banner_text_async(alpha2: str):
    """
    Fetch banner text from music.apple.com for a given storefront.
    Returns (text, final_url).
    """
    # We'll use aiohttp-like logic without dependency; just run in executor
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_music_banner_text_sync, alpha2)


def _get_music_banner_text_sync(alpha2: str):
    url = f"https://music.apple.com/{alpha2.lower()}/new"
    try:
        resp = SESSION.get(url, timeout=20, allow_redirects=True)
        final_url = resp.url
        if resp.status_code != 200:
            return "", final_url

        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for prominent hero/banner text
        # Apple changes this frequently; we grab visible text blobs.
        # Heuristic: find a large section with pricing.
        text = soup.get_text(" ", strip=True)
        return text, final_url
    except Exception:
        return "", url


def banner_individual_row(alpha2: str, country_name: str, meta=None):
    with BANNER_SEMAPHORE:
        try:
            text, final_url = asyncio.run(_get_music_banner_text_async(alpha2))
        except RuntimeError:
            holder = {}

            def runner():
                holder["pair"] = asyncio.run(_get_music_banner_text_async(alpha2))

            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join()
            text, final_url = holder.get("pair", ("", ""))

    store_cc = _extract_cc(final_url)
    if not store_cc or store_cc != alpha2.upper():
        reason = f"music.apple.com storefront mismatch (requested={alpha2}, final={store_cc or 'NONE'})"
        log_missing(
            country_name,
            alpha2,
            final_url or f"https://music.apple.com/{alpha2.lower()}/new",
            reason,
        )
        # IMPORTANT: return a placeholder row so the output still reflects the requested country
        return [
            {
                "Country": country_name,
                "Country Code": alpha2,
                "Currency": "",
                "Currency Source": "",
                "Currency Raw": "",
                "Plan": "Individual",
                "Price Display": "",
                "Price Value": None,
                "Source": "music.apple.com banner (fallback)",
                "Redirected": meta.get("Redirected", True) if meta else True,
                "Redirected To": meta.get("Redirected To", store_cc or "") if meta else (store_cc or ""),
                "Redirect Reason": meta.get("Redirect Reason", reason) if meta else reason,
                "Apple URL": (
                    meta.get("Apple URL", final_url or f"https://music.apple.com/{alpha2.lower()}/new")
                    if meta
                    else (final_url or f"https://music.apple.com/{alpha2.lower()}/new")
                ),
                "Has Apple Music Page": meta.get("Has Apple Music Page", False) if meta else False,
            }
        ]

    chosen_tok, chosen_val, _dbg = pick_recurring_price_token(text)
    if not chosen_tok or chosen_val is None:
        reason = "Could not find recurring price token in banner text"
        log_missing(
            country_name,
            alpha2,
            final_url or f"https://music.apple.com/{alpha2.lower()}/new",
            reason,
        )
        return [
            {
                "Country": country_name,
                "Country Code": alpha2,
                "Currency": "",
                "Currency Source": "",
                "Currency Raw": "",
                "Plan": "Individual",
                "Price Display": "",
                "Price Value": None,
                "Source": "music.apple.com banner (fallback)",
                "Redirected": meta.get("Redirected", False) if meta else False,
                "Redirected To": meta.get("Redirected To", "") if meta else "",
                "Redirect Reason": meta.get("Redirect Reason", reason) if meta else reason,
                "Apple URL": (
                    meta.get("Apple URL", final_url or f"https://music.apple.com/{alpha2.lower()}/new")
                    if meta
                    else (final_url or f"https://music.apple.com/{alpha2.lower()}/new")
                ),
                "Has Apple Music Page": meta.get("Has Apple Music Page", True) if meta else True,
            }
        ]

    disp = _clean(chosen_tok)
    val = _cast_num(float(chosen_val))

    # Currency guess from display
    cur_symbol = ""
    m = re.search(r"[£€$]", disp)
    if m:
        cur_symbol = m.group(0)

    row = {
        "Country": country_name,
        "Country Code": alpha2,
        "Currency": cur_symbol,
        "Currency Source": "symbol",
        "Currency Raw": cur_symbol,
        "Plan": "Individual",
        "Price Display": disp,
        "Price Value": val,
        "Source": "music.apple.com banner (fallback)",
        "Redirected": meta.get("Redirected", False) if meta else False,
        "Redirected To": meta.get("Redirected To", "") if meta else "",
        "Redirect Reason": meta.get("Redirect Reason", "") if meta else "",
        "Apple URL": (
            meta.get("Apple URL", final_url or f"https://music.apple.com/{alpha2.lower()}/new")
            if meta
            else (final_url or f"https://music.apple.com/{alpha2.lower()}/new")
        ),
        "Has Apple Music Page": meta.get("Has Apple Music Page", True) if meta else True,
    }
    return [row]


# ================= Redirect detection =================


def detect_redirect_reason(requested_cc: str, final_url: str) -> str:
    """
    Classify likely redirect reason (heuristic).
    """
    if not final_url:
        return "No final URL"
    if looks_like_us_hub_url(final_url) and requested_cc.upper() != "US":
        return "Redirected to US hub"
    final_cc = _extract_cc(final_url)
    if final_cc and final_cc != requested_cc.upper():
        return f"Storefront mismatch ({requested_cc}->{final_cc})"
    return ""


# ================== SCRAPING APPLE.COM ==================


def scrape_apple_com_page_for_pricing(html: str):
    """
    Apple pages can carry pricing in embedded JSON and/or readable text.
    We'll return (found_prices_by_plan, raw_currency_info).
    The plan keys: Individual, Student, Family
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Try to find recurring prices (monthly) using regex tokens
    # We'll heuristically map by nearby keywords.
    found = {}

    # Extract all currency-number tokens
    tokens = []
    for m in re.finditer(r"([£€$]\s?\d+[.,]?\d*)", text):
        tok = m.group(0)
        n = re.search(r"\d+[.,]?\d*", tok)
        num = None
        if n:
            raw = n.group(0).replace(",", ".")
            try:
                num = float(raw)
            except Exception:
                num = None
        tokens.append((tok, num))

    # Keyword-based mapping
    # This is a heuristic, because Apple changes page layouts.
    lower = text.lower()

    def closest_price_after(keyword: str):
        idx = lower.find(keyword)
        if idx < 0:
            return None
        window = text[idx : idx + 400]
        tok, num, _ = pick_recurring_price_token(window)
        if tok and num is not None:
            return tok, num
        return None

    # Individual
    p = closest_price_after("individual")
    if p:
        found["Individual"] = {"display": p[0], "value": p[1]}

    # Student
    p = closest_price_after("student")
    if p:
        found["Student"] = {"display": p[0], "value": p[1]}

    # Family
    p = closest_price_after("family")
    if p:
        found["Family"] = {"display": p[0], "value": p[1]}

    # Currency guess from any token
    cur_symbol = ""
    if tokens:
        m = re.search(r"[£€$]", tokens[0][0])
        if m:
            cur_symbol = m.group(0)

    currency_info = {"symbol": cur_symbol, "raw": cur_symbol}
    return found, currency_info


def scrape_country(alpha2: str):
    """
    Scrape Apple Music pricing for a single country code.
    Returns list of plan rows.
    """
    cc = alpha2.upper()
    country_name = normalize_country_name(get_country_name_from_code(cc))

    base = "https://www.apple.com"
    paths = [
        "",  # fallback
        cc.lower(),
    ]

    last_url = None
    had_apple_page = False

    for path in paths:
        url = f"{base}/apple-music/" if path == "" else f"{base}/{path}/apple-music/"
        last_url = url
        try:
            resp = SESSION.get(url, timeout=15, allow_redirects=True)

            if resp.status_code == 200 and "apple.com" in urlparse(resp.url).netloc:
                had_apple_page = True

            if cc != "US" and looks_like_us_hub_url(resp.url):
                # Many locales redirect to US content; treat as redirect failure.
                reason = "Redirected to US hub"
                log_missing(country_name, cc, resp.url, reason)
                # We'll continue and try banner fallback at end
                break

            if resp.status_code != 200:
                log_missing(country_name, cc, url, f"HTTP {resp.status_code}")
                continue

            # We got a page. Try to extract plan pricing.
            found, currency_info = scrape_apple_com_page_for_pricing(resp.text)
            if found:
                rows = []
                for plan in TIER_ORDER:
                    if plan in found:
                        rows.append(
                            {
                                "Country": country_name,
                                "Country Code": cc,
                                "Currency": currency_info.get("symbol", ""),
                                "Currency Source": "symbol",
                                "Currency Raw": currency_info.get("raw", ""),
                                "Plan": plan,
                                "Price Display": _clean(found[plan]["display"]),
                                "Price Value": _cast_num(found[plan]["value"]),
                                "Source": "apple.com/apple-music",
                                "Redirected": (resp.url != url),
                                "Redirected To": resp.url if (resp.url != url) else "",
                                "Redirect Reason": detect_redirect_reason(cc, resp.url),
                                "Apple URL": resp.url,
                                "Has Apple Music Page": True,
                            }
                        )
                if rows:
                    return rows

            # Page ok but no data
            log_missing(country_name, cc, resp.url, "No plan prices parsed from apple.com page")

        except Exception as e:
            log_missing(country_name, cc, url, f"Request exception: {type(e).__name__}: {e}")

    # Fallback: use music.apple.com banner for Individual price only
    meta = {
        "Redirected": True,
        "Redirected To": "",
        "Redirect Reason": "Fallback to music.apple.com banner",
        "Apple URL": last_url or f"https://www.apple.com/{cc.lower()}/apple-music/",
        "Has Apple Music Page": had_apple_page,
    }
    return banner_individual_row(cc, country_name, meta=meta)


# ================== MAIN RUNNER ==================


def run_scraper(country_codes_override=None):
    init_missing_db()

    iso_codes = {c.alpha_2 for c in pycountry.countries}
    all_codes = sorted(iso_codes.union(EXTRA_REGIONS))

    if country_codes_override:
        requested = {(cc or "").strip().upper() for cc in country_codes_override if (cc or "").strip()}
        requested = {cc for cc in requested if len(cc) == 2}
        all_codes = sorted(requested)
        print(f"🎯 Subset mode: scraping {len(all_codes)} countries: {all_codes}")
    elif TEST_MODE:
        all_codes = sorted({c.strip().upper() for c in TEST_COUNTRIES if c and len(c.strip()) == 2})
        print(f"🧪 TEST MODE: scraping {len(all_codes)} countries: {all_codes}")
    else:
        print(f"🌍 FULL MODE: scraping {len(all_codes)} countries")

    all_rows = []
    failed_codes = []

    # First pass concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scrape_country, cc): cc for cc in all_codes}
        for fut in as_completed(futures):
            cc = futures[fut]
            try:
                res = fut.result()
                if res:
                    all_rows.extend(res)
                else:
                    failed_codes.append(cc)
            except Exception as e:
                failed_codes.append(cc)
                cn = normalize_country_name(get_country_name_from_code(cc))
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Future exception: {type(e).__name__}: {e}",
                )

    # Retry sequentially for failed countries
    if failed_codes:
        print(f"🔁 Retrying {len(failed_codes)} failed countries sequentially…")
        for cc in failed_codes:
            try:
                res = scrape_country(cc)
                if res:
                    all_rows.extend(res)
                    # remove earlier missing buffer entries for this cc if it succeeded
                    MISSING_BUFFER[:] = [m for m in MISSING_BUFFER if m.get("country_code") != cc]
            except Exception as e:
                cn = normalize_country_name(get_country_name_from_code(cc))
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Retry exception: {type(e).__name__}: {e}",
                )

    # IMPORTANT FIX: Always write an output file, even if nothing scraped,
    # so Streamlit doesn't show a stale Excel from a previous run.
    if not all_rows:
        print("⚠️ No rows scraped at all – writing placeholder output so the UI doesn't show stale results.")
        for cc in all_codes:
            cn = normalize_country_name(get_country_name_from_code(cc))
            miss = next(
                (
                    m for m in reversed(MISSING_BUFFER)
                    if (m.get("country_code") or "").strip().upper() == cc
                ),
                None,
            )
            reason = (miss.get("reason") if miss else "No data scraped.")
            url = (miss.get("url") if miss else f"https://www.apple.com/{cc.lower()}/apple-music/")
            all_rows.append(
                {
                    "Country": cn,
                    "Country Code": cc,
                    "Currency": "",
                    "Currency Source": "",
                    "Currency Raw": "",
                    "Plan": "Individual",
                    "Price Display": "",
                    "Price Value": None,
                    "Source": "N/A",
                    "Redirected": False,
                    "Redirected To": "",
                    "Redirect Reason": reason,
                    "Apple URL": url,
                    "Has Apple Music Page": False,
                }
            )

    df = pd.DataFrame(all_rows)

    # Ensure required columns exist (in case placeholder rows were created)
    required_cols = [
        "Country",
        "Country Code",
        "Currency",
        "Currency Source",
        "Currency Raw",
        "Plan",
        "Price Display",
        "Price Value",
        "Source",
        "Redirected",
        "Redirected To",
        "Redirect Reason",
        "Apple URL",
        "Has Apple Music Page",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["Plan"] = pd.Categorical(df["Plan"], TIER_ORDER, ordered=True)
    df.sort_values(["Country", "Plan"], inplace=True, ignore_index=True)

    out_name = OUT_TEST if (TEST_MODE or country_codes_override) else OUT_ALL
    df.to_excel(out_name, index=False)
    print(f"✅ Exported to {out_name} (rows={len(df)})")

    if MISSING_BUFFER:
        pd.DataFrame(MISSING_BUFFER).to_csv(MISSING_CSV, index=False)
        print(f"⚠️ Logged {len(MISSING_BUFFER)} issues to {MISSING_CSV} / {MISSING_DB}")

    return out_name


# ================== STREAMLIT WRAPPER ==================


def run_apple_music_scraper(test_mode: bool = True, test_countries=None) -> str:
    """Public wrapper used by the Streamlit app.

    Returns an absolute path to the Excel output.
    """
    global TEST_MODE, TEST_COUNTRIES
    TEST_MODE = bool(test_mode)

    country_override = None
    if TEST_MODE and test_countries:
        TEST_COUNTRIES = [
            c.strip().upper() for c in test_countries if c and len(c.strip()) == 2
        ]
        country_override = TEST_COUNTRIES
        print(f"[APPLE MUSIC] UI-driven test countries: {TEST_COUNTRIES}")

    start = time.time()
    out_name = run_scraper(country_codes_override=country_override)
    print(f"[APPLE MUSIC] Finished in {round(time.time() - start, 2)}s")

    if not out_name:
        raise RuntimeError("Apple Music scraper produced no output file.")

    from pathlib import Path
    return str(Path(out_name).resolve())


if __name__ == "__main__":
    start = time.time()
    run_scraper()
    print(f"⏱️ Finished in {round(time.time() - start, 2)}s")

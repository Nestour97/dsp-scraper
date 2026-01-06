# apple_music_plans_robust.py
# ------------------------------------------------------------
# Apple Music scraper (robust)
# - Picks RECURRING monthly price (not intro/trial):
#     1) prefer tokens whose nearby translated context contains "then/after/thereafter"
#     2) else prefer tokens whose nearby translated context looks monthly (/month, per month, monthly, etc.)
#     3) else choose MAX numeric token (trial is almost always smaller)
# - Fixes Turkey missing by supporting "TL" (TRY) token
# - Fixes Hungary "Ft" currency raw token and supports other letter tokens (Kč, zł, lei, лв, etc.)
# - Robust redirect detection + currency parsing (no 'TRY' false positives)
# - Banner fallback also picks recurring token (not first match)
#
# PATCHED FIX (stale output issue):
# - banner_individual_row() no longer returns [] on storefront mismatch / no token; returns a placeholder row.
# - run_scraper() always writes an Excel (placeholder rows if nothing scraped).
# - run_apple_music_scraper() deletes old TEST file before running and returns the actual output path.
# ------------------------------------------------------------

import re, time, threading, sqlite3, asyncio
from datetime import datetime, UTC, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag
import pandas as pd
import pycountry
from deep_translator import GoogleTranslator
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
from babel.numbers import get_territory_currencies
from functools import lru_cache

# =========================== Config ===========================

DEBUG_LOADED_PRINT = False
if DEBUG_LOADED_PRINT:
    print("✅ LOADED patched apple_music_scraper.py")

MAX_WORKERS = 6

SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=0.4,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
    ),
)
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122 Safari/537.36"
        ),
        "Accept-Language": "en;q=0.9",
    }
)

# Translation (for language-agnostic heuristics)
translator = GoogleTranslator(source="auto", target="en")

TEST_MODE = True
TEST_COUNTRIES = ["US", "GB", "DE", "FR", "JP"]

EXTRA_REGIONS = set()

TIER_ORDER = ["Individual", "Student", "Family"]

MISSING_CSV = "apple_music_missing.csv"
MISSING_DB = "apple_music_missing.sqlite"

BANNER_SEMAPHORE = threading.BoundedSemaphore(4)
MISSING_BUFFER = []

MANUAL_COUNTRY_FIXES = {
    "Taiwan, Province of China": "Taiwan",
    "Korea, Republic of": "South Korea",
    "Korea, Democratic People's Republic of": "North Korea",
    "Russian Federation": "Russia",
    "Viet Nam": "Vietnam",
    "Iran, Islamic Republic of": "Iran",
    "Tanzania, United Republic of": "Tanzania",
    "Moldova, Republic of": "Moldova",
    "Bolivia, Plurinational State of": "Bolivia",
    "Venezuela, Bolivarian Republic of": "Venezuela",
    "Syrian Arab Republic": "Syria",
    "Lao People's Democratic Republic": "Laos",
    "Libyan Arab Jamahiriya": "Libya",
    "Congo, The Democratic Republic of the": "DR Congo",
    "Congo": "Republic of the Congo",
    "Brunei Darussalam": "Brunei",
    "Czechia": "Czech Republic",
    "Türkiye": "Turkey",
    "Eswatini": "Swaziland",
    "North Macedonia": "Macedonia",
}

# Currency tokens (symbols + common local markers)
LOCAL_CURRENCY_TOKENS = [
    # Latin-letter tokens
    "TL", "Ft", "Kč", "zł", "lei", "лв", "₺", "₽", "₹", "₪", "₫", "₩", "₱",
    # Some additional frequent abbreviations
    "R$", "CHF", "Rp", "kr", "RM", "SAR", "QAR", "AED", "KWD", "BHD", "OMR",
    "TSh", "KSh", "USh", "ZAR", "ZWL",
]

CURRENCY_TOKEN = (
    r"(?:" +
    "|".join(LOCAL_CURRENCY_TOKENS) +
    r"|RM|S/\.|R\$|CHF|Rp|kr|"
    r"\$|€|£|¥|₩|₫|₱|₹|₪|₭|₮|₦|₲|₴|₸|₺|₽|"
    r"TSh|KSh|USh|ZAR|ZWL|R|"
    r"SAR|QAR|AED|KWD|BHD|OMR)"
)

NUMBER_TOKEN = r"(\d+(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?)"

BANNER_PRICE_REGEX = re.compile(
    rf"(?:{CURRENCY_TOKEN}\s*{NUMBER_TOKEN}|{NUMBER_TOKEN}\s*{CURRENCY_TOKEN})"
)

STRICT_PRICE_NUMBER = re.compile(
    r"(\d{1,3}(?:[.,\s]\d{3})+|\d+[.,]\d{1,2})"
)

# =========================== Utilities ===========================

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _cast_num(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
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

def _extract_cc(url: str) -> str:
    try:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if parts and len(parts[0]) == 2:
            return parts[0].upper()
    except Exception:
        pass
    return ""

def looks_like_us_hub_url(url: str) -> bool:
    try:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if parts and parts[0].lower() == "us":
            return True
    except Exception:
        return False
    return False

def pick_recurring_price_token(text: str):
    t = _clean(text)
    if not t:
        return None, None, {"reason": "empty_text"}

    # Use broad token capture; then heuristics to pick recurring.
    # We also translate snippets around tokens to English to detect "then/after/thereafter".
    matches = list(BANNER_PRICE_REGEX.finditer(t))
    if not matches:
        return None, None, {"reason": "no_price_matches"}

    candidates = []
    for m in matches:
        tok = _clean(m.group(0))
        num_m = STRICT_PRICE_NUMBER.search(tok)
        num = None
        if num_m:
            raw = num_m.group(1).replace(" ", "").replace(",", ".")
            try:
                num = float(raw)
            except Exception:
                num = None

        # small context around match
        start = max(0, m.start() - 60)
        end = min(len(t), m.end() + 80)
        ctx = t[start:end]
        try:
            ctx_en = translator.translate(ctx)
        except Exception:
            ctx_en = ctx

        ctx_en_l = (ctx_en or "").lower()
        score = 0

        # Prefer "then/after/thereafter" signals (recurring after trial)
        if any(k in ctx_en_l for k in ["then", "after", "thereafter"]):
            score += 50
        # Prefer explicit monthly patterns
        if any(k in ctx_en_l for k in ["/month", "per month", "monthly", "a month", "each month", "/ mo", "/mo"]):
            score += 20
        # Penalize trial/free terms
        if any(k in ctx_en_l for k in ["free", "trial", "first", "1 month", "one month"]):
            score -= 20

        candidates.append((score, num, tok, ctx_en))

    # Sort by score desc, then numeric desc (trial usually smaller)
    candidates.sort(key=lambda x: (x[0], (x[1] or -1)), reverse=True)

    best = candidates[0]
    if best[1] is None:
        return best[2], None, {"reason": "no_numeric", "best_ctx": best[3], "candidates": candidates[:5]}
    return best[2], best[1], {"best_ctx": best[3], "candidates": candidates[:5]}

@lru_cache(maxsize=None)
def get_country_code(country_name: str) -> str:
    try:
        c = pycountry.countries.lookup(country_name)
        return c.alpha_2
    except Exception:
        return ""

def detect_currency_iso_from_alpha2(alpha2: str) -> str:
    try:
        territory = alpha2.upper()
        codes = get_territory_currencies(territory, date=date.today())
        if codes:
            return codes[0]
    except Exception:
        pass
    return ""

def iso_from_raw_currency(raw: str, alpha2: str):
    raw = _clean(raw)
    if not raw:
        return "", ""

    # Common symbols
    sym_map = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "₩": "KRW", "₹": "INR", "₪": "ILS",
               "₫": "VND", "₱": "PHP", "₺": "TRY", "₽": "RUB"}

    if raw in sym_map:
        return sym_map[raw], "symbol-map"

    # Specific local tokens
    if raw == "TL":
        return "TRY", "token-TL"
    if raw == "Ft":
        return "HUF", "token-Ft"
    if raw.lower() == "kr":
        # Could be several; infer from territory
        iso = detect_currency_iso_from_alpha2(alpha2)
        if iso:
            return iso, "token-kr+territory"
        return "", ""
    if raw == "Kč":
        return "CZK", "token-Kč"
    if raw == "zł":
        return "PLN", "token-zł"
    if raw == "lei":
        return "RON", "token-lei"
    if raw == "лв":
        return "BGN", "token-лв"
    if raw == "R$":
        return "BRL", "token-R$"
    if raw == "CHF":
        return "CHF", "token-CHF"
    if raw == "Rp":
        return "IDR", "token-Rp"

    # Fallback territory-based
    iso = detect_currency_iso_from_alpha2(alpha2)
    if iso:
        return iso, "territory-fallback"
    return "", ""

def parse_currency_and_value(price_display: str, alpha2: str):
    s = _clean(price_display)
    if not s:
        return "", "", None

    m = BANNER_PRICE_REGEX.search(s)
    if not m:
        # try any number
        n = STRICT_PRICE_NUMBER.search(s)
        if not n:
            return "", "", None
        raw_num = n.group(1).replace(" ", "").replace(",", ".")
        try:
            val = float(raw_num)
        except Exception:
            val = None
        return "", "", val

    tok = _clean(m.group(0))

    # Identify currency raw token: pick first currency token in tok
    cur_raw = ""
    cur_m = re.search(CURRENCY_TOKEN, tok)
    if cur_m:
        cur_raw = _clean(cur_m.group(0))

    # Identify number
    num_m = STRICT_PRICE_NUMBER.search(tok)
    val = None
    if num_m:
        raw_num = num_m.group(1).replace(" ", "").replace(",", ".")
        try:
            val = float(raw_num)
        except Exception:
            val = None

    iso, src = iso_from_raw_currency(cur_raw, alpha2)
    return iso, cur_raw, val

# ===================== music.apple.com banner fallback =====================

async def _get_music_banner_text_async(alpha2: str):
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
        # IMPORTANT: return a placeholder row so the UI doesn't show stale Excel output
        return [{
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
        }]

    chosen_tok, chosen_val, _dbg = pick_recurring_price_token(text)
    if not chosen_tok or chosen_val is None:
        reason = "Could not find recurring price token in banner text"
        log_missing(
            country_name,
            alpha2,
            final_url or f"https://music.apple.com/{alpha2.lower()}/new",
            reason,
        )
        return [{
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
        }]

    disp = _clean(chosen_tok)
    iso, raw, val = parse_currency_and_value(disp, alpha2)

    src = ""
    if iso:
        src = "parsed"
    else:
        iso_res = detect_currency_iso_from_alpha2(alpha2)
        if iso_res:
            iso = iso_res
            src = "territory-fallback"

    row = {
        "Country": country_name,
        "Country Code": alpha2,
        "Currency": iso,
        "Currency Source": src,
        "Currency Raw": raw,
        "Plan": "Individual",
        "Price Display": disp,
        "Price Value": _cast_num(val),
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

def looks_like_us_content(soup: BeautifulSoup) -> bool:
    text = soup.get_text(" ", strip=True)
    t = text.lower()
    price_hit = re.search(r"\$ ?10\.99|\$ ?5\.99|\$ ?16\.99", text)
    copy_hit = ("try 1 month free" in t) or ("no commitment" in t and "cancel anytime" in t)
    return bool(price_hit and copy_hit)

def _storefront_equivalent(requested_cc: str, detected_cc: str) -> bool:
    if not detected_cc:
        return False
    r = (requested_cc or "").upper()
    d = (detected_cc or "").upper()
    if r == d:
        return True
    # Some equivalences/edge cases can be added here if needed.
    return False

def detect_redirect_reason(requested_cc: str, final_url: str) -> str:
    if not final_url:
        return "No final URL"
    if looks_like_us_hub_url(final_url) and requested_cc.upper() != "US":
        return "Redirected to US hub"
    final_cc = _extract_cc(final_url)
    if final_cc and final_cc != requested_cc.upper():
        return f"Storefront mismatch ({requested_cc}->{final_cc})"
    return ""

# ================== Apple.com page extraction ==================

def extract_plan_entries_from_dom(soup: BeautifulSoup, alpha2: str):
    text = soup.get_text(" ", strip=True)
    entries = {}

    # Heuristic: find tokens around plan keywords
    lower = text.lower()

    def find_near(keyword):
        idx = lower.find(keyword)
        if idx < 0:
            return None
        window = text[idx: idx + 400]
        tok, num, _ = pick_recurring_price_token(window)
        if tok and num is not None:
            iso, raw, val = parse_currency_and_value(tok, alpha2)
            if val is None:
                val = num
            cur_iso = iso or detect_currency_iso_from_alpha2(alpha2)
            cur_src = "parsed" if iso else "territory-fallback"
            return {
                "Currency": cur_iso,
                "Currency Source": cur_src,
                "Currency Raw": raw,
                "Price Display": _clean(tok),
                "Price Value": _cast_num(val),
            }
        return None

    for plan in TIER_ORDER:
        info = find_near(plan.lower())
        if info:
            entries[plan] = info

    return entries

async def scrape_with_playwright(url: str, timeout_ms: int = 25000):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(1500)
            content = await page.content()
        finally:
            await browser.close()
        return content

def scrape_country(cc: str):
    cc = cc.upper()
    country_name = normalize_country_name(get_country_name_from_code(cc))
    country_name = MANUAL_COUNTRY_FIXES.get(country_name, country_name)
    code = (get_country_code(country_name) or cc).upper()

    base_urls = [
        f"https://www.apple.com/{code.lower()}/apple-music/",
        "https://www.apple.com/apple-music/",
    ]

    for url in base_urls:
        try:
            resp = SESSION.get(url, timeout=18, allow_redirects=True)
        except Exception as e:
            log_missing(country_name, code, url, f"Request exception: {type(e).__name__}: {e}")
            continue

        # Detect obvious redirect to US hub
        if code != "US" and looks_like_us_hub_url(resp.url):
            log_missing(country_name, code, resp.url, "Redirected to US hub")
            return banner_individual_row(
                code,
                country_name,
                meta={
                    "Redirected": True,
                    "Redirected To": "US hub",
                    "Redirect Reason": "Redirected to US hub",
                    "Apple URL": resp.url,
                    "Has Apple Music Page": False,
                },
            )

        if resp.status_code != 200:
            log_missing(country_name, code, url, f"HTTP {resp.status_code}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # If the page content looks like US (even if URL isn't), treat as redirect-ish
        if code != "US" and looks_like_us_content(soup):
            log_missing(country_name, code, resp.url, "Content looks like US pricing/copy")
            return banner_individual_row(
                code,
                country_name,
                meta={
                    "Redirected": True,
                    "Redirected To": "US content",
                    "Redirect Reason": "Content looks like US pricing/copy",
                    "Apple URL": resp.url,
                    "Has Apple Music Page": False,
                },
            )

        entries = extract_plan_entries_from_dom(soup, code)
        if entries:
            rows = []
            for std in TIER_ORDER:
                if std in entries:
                    info = entries[std]
                    rows.append(
                        {
                            "Country": country_name,
                            "Country Code": code,
                            "Currency": info["Currency"],
                            "Currency Source": info["Currency Source"],
                            "Currency Raw": info["Currency Raw"],
                            "Plan": std,
                            "Price Display": info["Price Display"],
                            "Price Value": info["Price Value"],
                            "Source": "apple.com page",
                            "Redirected": (resp.url != url),
                            "Redirected To": resp.url if (resp.url != url) else "",
                            "Redirect Reason": detect_redirect_reason(code, resp.url),
                            "Apple URL": resp.url,
                            "Has Apple Music Page": True,
                        }
                    )
            if rows:
                return rows

        # If no entries found, try Playwright once for dynamic content
        try:
            html = asyncio.run(scrape_with_playwright(resp.url))
            soup2 = BeautifulSoup(html, "html.parser")
            entries = extract_plan_entries_from_dom(soup2, code)
            if entries:
                rows = []
                for std in TIER_ORDER:
                    if std in entries:
                        info = entries[std]
                        rows.append(
                            {
                                "Country": country_name,
                                "Country Code": code,
                                "Currency": info["Currency"],
                                "Currency Source": info["Currency Source"],
                                "Currency Raw": info["Currency Raw"],
                                "Plan": std,
                                "Price Display": info["Price Display"],
                                "Price Value": info["Price Value"],
                                "Source": "apple.com page (playwright)",
                                "Redirected": (resp.url != url),
                                "Redirected To": resp.url if (resp.url != url) else "",
                                "Redirect Reason": detect_redirect_reason(code, resp.url),
                                "Apple URL": resp.url,
                                "Has Apple Music Page": True,
                            }
                        )
                if rows:
                    return rows
        except PWTimeoutError:
            log_missing(country_name, code, resp.url, "Playwright timeout")
        except Exception as e:
            log_missing(country_name, code, resp.url, f"Playwright error: {type(e).__name__}: {e}")

        # Fallback to banner if apple.com parsing fails
        return banner_individual_row(
            code,
            country_name,
            meta={
                "Redirected": (resp.url != url),
                "Redirected To": resp.url if (resp.url != url) else "",
                "Redirect Reason": detect_redirect_reason(code, resp.url),
                "Apple URL": resp.url,
                "Has Apple Music Page": True,
            },
        )

    # If both URLs failed, final fallback
    return banner_individual_row(
        code,
        country_name,
        meta={
            "Redirected": True,
            "Redirected To": "",
            "Redirect Reason": "apple.com request failed",
            "Apple URL": base_urls[0],
            "Has Apple Music Page": False,
        },
    )

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

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(scrape_country, cc): cc for cc in all_codes}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Scraping Apple Music"):
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
                cn = MANUAL_COUNTRY_FIXES.get(cn, cn)
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Future exception: {type(e).__name__}: {e}",
                )

    if failed_codes:
        print(f"🔁 Retrying {len(failed_codes)} failed countries sequentially…")
        for cc in failed_codes:
            try:
                res = scrape_country(cc)
                if res:
                    all_rows.extend(res)
                    MISSING_BUFFER[:] = [m for m in MISSING_BUFFER if m.get("country_code") != cc]
            except Exception as e:
                cn = normalize_country_name(get_country_name_from_code(cc))
                cn = MANUAL_COUNTRY_FIXES.get(cn, cn)
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Retry exception: {type(e).__name__}: {e}",
                )

    if not all_rows:
        print("⚠️ No rows scraped at all – writing placeholder output to avoid stale Excel in the UI.")
        for cc in all_codes:
            country_name = normalize_country_name(get_country_name_from_code(cc))
            country_name = MANUAL_COUNTRY_FIXES.get(country_name, country_name)
            all_rows.append(
                {
                    "Country": country_name,
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
                    "Redirect Reason": "No data scraped",
                    "Apple URL": f"https://www.apple.com/{cc.lower()}/apple-music/",
                    "Has Apple Music Page": False,
                }
            )

    df = pd.DataFrame(all_rows)
    df["Plan"] = pd.Categorical(df["Plan"], TIER_ORDER, ordered=True)
    df.sort_values(["Country", "Plan"], inplace=True, ignore_index=True)

    out_name = "apple_music_plans_TEST.xlsx" if TEST_MODE or country_codes_override else "apple_music_plans_all.xlsx"
    df.to_excel(out_name, index=False)
    print(f"✅ Exported to {out_name} (rows={len(df)})")

    if MISSING_BUFFER:
        pd.DataFrame(MISSING_BUFFER).to_csv(MISSING_CSV, index=False)
        print(f"⚠️ Logged {len(MISSING_BUFFER)} issues to {MISSING_CSV} / {MISSING_DB}")

    return out_name

# ================== STREAMLIT WRAPPER ==================

def run_apple_music_scraper(test_mode: bool = True, test_countries=None) -> str:
    """Public wrapper used by the Streamlit app.

    Fixes the 'stale Excel' issue by:
      - deleting the old TEST file before running (in test mode)
      - returning the actual output path produced by run_scraper()
      - validating the file exists before returning
    """
    global TEST_MODE, TEST_COUNTRIES
    TEST_MODE = bool(test_mode)

    # ✅ Delete old test file so stale results can't appear if the scrape fails
    stale = Path("apple_music_plans_TEST.xlsx")
    if TEST_MODE and stale.exists():
        try:
            stale.unlink()
        except Exception:
            pass

    country_override = None
    if TEST_MODE and test_countries:
        TEST_COUNTRIES = [c.strip().upper() for c in test_countries if c and len(c.strip()) == 2]
        country_override = TEST_COUNTRIES
        print(f"[APPLE MUSIC] UI-driven test countries: {TEST_COUNTRIES}")

    start = time.time()
    out_name = run_scraper(country_codes_override=country_override)
    print(f"[APPLE MUSIC] Finished in {round(time.time() - start, 2)}s")

    if not out_name:
        raise RuntimeError("Apple Music scraper produced no output filename.")

    out_path = Path(out_name).resolve()
    if not out_path.exists():
        raise RuntimeError(f"Apple Music output file was not created: {out_path}")

    return str(out_path)

if __name__ == "__main__":
    start = time.time()
    run_scraper()
    print(f"⏱️ Finished in {round(time.time() - start, 2)}s")

# spotify_scraper_playwright.py
# -------------------------------------------------------------------
# This is your Colab Spotify scraper, adapted for:
#   - normal Python environments (no Colab / nest_asyncio / files.download)
#   - use from the Streamlit app via run_spotify_scraper()
#
# CORE LOGIC (parsing, heuristics, etc.) IS IDENTICAL TO YOUR SCRIPT.
# Only I/O / wrappers / CLI are different.
# -------------------------------------------------------------------

import asyncio, re, pandas as pd, functools
from pathlib import Path

from playwright.async_api import async_playwright
import pycountry
from difflib import get_close_matches

# Optional: googletrans for translation (best-effort). The scraper works without it.
try:
    from googletrans import Translator
except Exception:
    Translator = None

translator = Translator() if Translator else None

@functools.lru_cache(maxsize=4096)
def translate_text_cached(text: str) -> str:
    """Best-effort translation to English to power heuristics.
    If translation fails, returns the original text (lowercased) so logic remains stable.
    """
    try:
        if not text:
            return ""
        if translator is None:
            return (text or "").lower()
        return translator.translate(text, dest="en").text.lower()
    except Exception:
        return (text or "").lower()

def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

# -------------------- CURRENCY DETECTION --------------------
# Strong tokens are patterns we can reliably anchor a monetary amount on.
# Keep these aligned with your existing codebase.
STRONG_TOKENS = [
    (r"\bRp\b", "IDR"),
    (r"₹", "INR"),
    (r"\bSAR\b", "SAR"),
    (r"\bAED\b", "AED"),
    (r"\bQAR\b", "QAR"),
    (r"\bKWD\b", "KWD"),
    (r"\bBHD\b", "BHD"),
    (r"\bOMR\b", "OMR"),
    (r"\bCHF\b", "CHF"),
    (r"\bHK\$\b|HK\$", "HKD"),
    (r"\bNT\$\b|NT\$", "TWD"),
    (r"\bUS\$\b|US\$", "USD"),
    (r"\$", "USD"),
    (r"€", "EUR"),
    (r"£", "GBP"),
    (r"¥", "JPY"),
    (r"₩", "KRW"),
    (r"₫", "VND"),
    (r"฿", "THB"),
    (r"₺", "TRY"),
    (r"₪", "ILS"),
    (r"₴", "UAH"),
    (r"₦", "NGN"),
    (r"₱", "PHP"),
    (r"R\$", "BRL"),
    (r"\bS/\.\b|\bS/\b", "PEN"),
    (r"RD\$", "DOP"),
    (r"N\$", "NAD"),
    (r"\bKSh\b", "KES"),
    (r"\bTSh\b", "TZS"),
    (r"\bUSh\b", "UGX"),
]

KNOWN_ISO = set([
    "USD","EUR","GBP","AUD","CAD","NZD","SGD","HKD","TWD","MXN","ARS","CLP","COP","PEN",
    "BOB","NIO","GTQ","PYG","UYU","BRL","ZAR","NAD","CHF","NOK","SEK","DKK","PLN","CZK",
    "HUF","RON","BGN","RSD","BAM","MKD","TRY","ILS","AED","SAR","QAR","KWD","BHD","OMR",
    "INR","PKR","LKR","NPR","MYR","IDR","PHP","VND","THB","KRW","JPY","CNY"
])

def detect_currency_in_text(text: str, alpha2: str):
    """Return (currency_symbol_or_code, iso_code) if found, else ("","")."""
    t = text or ""
    # strong tokens first
    for pat, iso in STRONG_TOKENS:
        if re.search(pat, t, re.I):
            # Return the visible token-ish representative + ISO
            # Use ISO as the stable key.
            return iso, iso

    # ISO codes present?
    m = re.search(r"\b([A-Z]{3})\b", t.upper())
    if m and m.group(1) in KNOWN_ISO:
        return m.group(1), m.group(1)

    # fallback: guess based on country if needed
    try:
        country = pycountry.countries.get(alpha_2=(alpha2 or "").upper())
        if country:
            # crude mapping; keep existing behavior (best-effort)
            if country.alpha_2 in ("GB",):
                return "GBP", "GBP"
            if country.alpha_2 in ("US",):
                return "USD", "USD"
            if country.alpha_2 in ("FR","DE","ES","IT","NL","BE","PT","IE","AT","FI","GR","LU","SI","SK","EE","LV","LT","CY","MT"):
                return "EUR", "EUR"
    except Exception:
        pass
    return "", ""

# -------------------- TRIAL / PROMO DETECTION --------------------
def is_generic_trial(text: str) -> bool:
    """Heuristic: detect if a line is about a free trial/promo period."""
    t = (text or "").lower()
    return bool(re.search(r"\b(free|trial|gratuit|gratis|essai|test|kostenlos|prova|prueba)\b", t))

# -------------------- NUMBER NORMALIZATION --------------------
def _normalize_number(num_str: str) -> str:
    """Normalize numbers like '10,99' or '1.299,00' to '10.99' / '1299.00'."""
    if not num_str:
        return ""
    p = num_str.strip()
    # If both comma and dot exist, infer decimal separator by last occurrence.
    if "," in p and "." in p:
        if p.rfind(",") > p.rfind("."):
            # comma is decimal separator -> remove dots as thousand sep
            p = p.replace(".", "").replace(",", ".")
        else:
            # dot is decimal separator -> remove commas as thousand sep
            p = p.replace(",", "")
        try:
            return str(float(p))
        except Exception:
            return ""
    # If only comma, treat it as decimal if it looks like cents.
    if "," in p and "." not in p:
        dm = re.search(r",(\d{1,2})$", p)
        if dm:
            base = p[:-len(dm.group(0))].replace(".", "").replace(",", "")
            return _safe_float_str(base + "." + dm.group(1))
        # otherwise assume comma thousands, remove
        return _safe_float_str(p.replace(",", ""))
    # If only dot, ambiguous: if ends with .xx treat as decimal; else treat as thousands maybe.
    dm = re.search(r"\.(\d{1,2})$", p)
    if dm:
        base = p[:-len(dm.group(0))].replace(",", "").replace(".", "")
        return _safe_float_str(base + "." + dm.group(1))
    # plain integer
    return _safe_float_str(p.replace(".", "").replace(",", ""))

def _safe_float_str(s: str) -> str:
    try:
        return str(float(s))
    except Exception:
        return ""

# ------------ PRICE PARSING ------------
def extract_amount_number(text):
    """Return the monetary number in a line, preferring the number near a currency sign/code.

    This is intentionally robust across locales where the currency symbol comes AFTER the number
    (e.g., "10,99 €"), and avoids accidentally returning unrelated numbers (e.g., "1" from "1 mois").
    """
    if not isinstance(text, str) or not text.strip():
        return ""
    t = _clean_spaces(text)
    tr = translate_text_cached(t)

    # 0) Prefer number immediately BEFORE a strong currency token (e.g. "10,99 €", "0 €", "10,99 €/mois")
    for pat, _iso in STRONG_TOKENS:
        for m in re.finditer(pat, t):
            before = t[:m.start()].rstrip()
            nb = re.search(r"(\d+(?:[.,]\d+)?)\s*$", before)
            if nb:
                return _normalize_number(nb.group(1))

    # 1) Prefer number immediately AFTER a strong currency token (Rp, ₹, $, SAR, etc.)
    for pat, _iso in STRONG_TOKENS:
        m = re.search(pat, t)
        if m:
            after = t[m.end():]
            n = re.search(r"\d+(?:[.,]\d+)?", after)
            if n:
                return _normalize_number(n.group(0))

    # 2) Handle ISO codes around numbers
    S = t.upper()
    m = re.search(r"\b([A-Z]{3})\b\s*(\d+(?:[.,]\d+)?)", S)
    if m and m.group(1) in KNOWN_ISO:
        return _normalize_number(m.group(2))
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*\b([A-Z]{3})\b", S)
    if m and m.group(2) in KNOWN_ISO:
        return _normalize_number(m.group(1))

    # 3) Original logic as fallback
    m = re.search(
        r"(?:US\$|[€£¥₩₫₺₪₴₼₾₭฿₦₵₱]|NT\$|HK\$|S/\.|S/|R\$|RD\$|N\$|KSh|TSh|USh)\s*\d+(?:[.,]\d+)?",
        tr,
        re.I,
    )
    if not m:
        m = re.search(
            r"\b(?:USD|EUR|GBP|AUD|CAD|NZD|SGD|HKD|TWD|MXN|ARS|CLP|COP|PEN|BOB|NIO|GTQ|PYG|UYU|BRL|ZAR|NAD|CHF|NOK|SEK|DKK|PLN|CZK|HUF|RON|BGN|RSD|BAM|MKD|TRY|ILS|AED|SAR|QAR|KWD|BHD|OMR|INR|PKR|LKR|NPR|MYR|IDR|PHP|VND|THB|KRW|JPY|CNY)\s*\d+(?:[.,]\d+)?",
            tr,
            re.I,
        )
    if m:
        token = m.group(0)
        n = re.search(r"(?<!\d)(\d+(?:[.,]\d+)?)", token)
        if n:
            return _normalize_number(n.group(1))

    m = re.search(
        r"(?:after|then|per\s+month|monthly|month)\D{0,12}(\d+(?:[.,]\d+)?)",
        tr,
        re.I,
    )
    if m and not re.search(r"hour|hours|hr|hrs|minute|min", m.group(0), re.I):
        return _normalize_number(m.group(1))

    if is_generic_trial(t):
        m2 = re.search(
            r"(?:for\s+1\s+month|trial|free\s+for\s+\d+\s+month(?:s)?)\D{0,12}(\d+(?:[.,]\d+)?)",
            tr,
            re.I,
        )
        if m2 and not re.search(r"hour|hours|hr|hrs|minute|min", m2.group(0), re.I):
            return _normalize_number(m2.group(1))

    candidates = []
    for m in re.finditer(r"\d+(?:[.,]\d+)?", tr):
        num = m.group(0)
        end = m.span()[1]
        tail = tr[end: end + 8]
        if re.search(r"^\s*[/\-]?\s*(?:hour|hours|hr|hrs|minute|min)\b", tail, re.I):
            continue
        candidates.append(num)
    if candidates:
        return _normalize_number(candidates[-1])
    return ""

# ---------- Price-line chooser ----------
MONTHY_RE = re.compile(r"(?:/ ?month|\bper month\b|\ba month\b|\bmonthly\b|/ ?mois|\bpar mois\b|/ ?mes|\bal mes\b|/ ?mese|\bal mese\b|/ ?monat|\bpro monat\b|/ ?mês|\bao mês\b)", re.I)
AFTER_RE = re.compile(r"\b(after|thereafter|then|month after)\b", re.I)
FOR_N_MONTHS_RE = re.compile(r"\bfor\s+\d+\s+month", re.I)

def looks_monthly_en(s_en: str) -> bool:
    return bool(MONTHY_RE.search(s_en))

def choose_price_line(p_texts, alpha2: str) -> str:
    """Pick the most reliable price line from the <p> lines in a card."""
    lines = [(_clean_spaces(x), translate_text_cached(_clean_spaces(x)))
             for x in (p_texts or []) if x and x.strip()]
    if not lines:
        return ""

    # 1) Monthly + 'after/thereafter' (real monthly after promo)
    for raw, en in lines[:4]:
        if looks_monthly_en(en) and AFTER_RE.search(en):
            return raw

    # 2) Monthly but NOT 'for N months' (avoid trial/promo lines)
    for raw, en in lines[:4]:
        if looks_monthly_en(en) and not FOR_N_MONTHS_RE.search(en):
            return raw

    # 3) Robust fallback: choose the line that yields the highest monetary amount.
    # This prevents selecting a trial line like "0 € pour 1 mois" over "Puis 10,99 €/mois".
    scored = []
    for raw, _en in lines[:4]:
        cur, _ = detect_currency_in_text(raw, alpha2)
        if not cur or not re.search(r"\d", raw):
            continue
        amt_s = extract_amount_number(raw)
        try:
            amt = float(amt_s) if amt_s else None
        except Exception:
            amt = None
        if amt is not None:
            scored.append((amt, raw))

    if scored:
        positives = [x for x in scored if x[0] > 0]
        pool = positives if positives else scored
        pool.sort(key=lambda x: x[0], reverse=True)
        return pool[0][1]

    # 4) Fallback: first <p>
    return lines[0][0]

def pick_after_line(p_texts) -> str:
    for pt in p_texts[:4]:
        en = translate_text_cached(_clean_spaces(pt))
        if looks_monthly_en(en) and AFTER_RE.search(en):
            return pt
    return ""

def get_country_info(locale_code):
    base = (locale_code or "").split("-")[0]
    try:
        c = pycountry.countries.lookup(base)
        return c.name, c.alpha_2, c.alpha_3
    except Exception:
        return "Unknown", base.upper(), base.upper()

# -------------------- PLAYWRIGHT SCRAPER --------------------
SPOTIFY_PREMIUM_URL_TMPL = "https://www.spotify.com/{}/premium/"

async def scrape_one_country(page, locale_code: str):
    """Scrape all plan cards from one country's premium page."""
    country_name, a2, a3 = get_country_info(locale_code)
    url = SPOTIFY_PREMIUM_URL_TMPL.format(locale_code)

    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(900)

    cards = await page.query_selector_all("section [data-testid='plan-card'], [data-testid='plan-card']")
    # Fallback if testid changes: grab prominent plan sections
    if not cards:
        cards = await page.query_selector_all("section")

    results = []
    for card in cards:
        try:
            # plan name
            name_el = await card.query_selector("h3, h2, [data-testid='plan-title']")
            plan_name = _clean_spaces(await name_el.inner_text()) if name_el else ""

            # relevant p lines
            p_els = await card.query_selector_all("p")
            p_texts = []
            for p in p_els[:8]:
                tx = _clean_spaces(await p.inner_text())
                if tx:
                    p_texts.append(tx)

            if not plan_name and not p_texts:
                continue

            price_line = choose_price_line(p_texts, a2)
            currency, iso = detect_currency_in_text(price_line or " ".join(p_texts[:2]), a2)
            amount = extract_amount_number(price_line)

            results.append({
                "Country": country_name,
                "Alpha-2": a2,
                "Alpha-3": a3,
                "Locale": locale_code,
                "Plan": plan_name,
                "Price": amount,
                "Currency": iso or currency,
                "Price Line": price_line,
                "All P Lines": " | ".join(p_texts[:4]),
                "URL": url,
            })
        except Exception:
            continue

    return results

async def scrape_all_countries(locales):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="en-US")
        page = await context.new_page()

        all_rows = []
        for lc in locales:
            try:
                rows = await scrape_one_country(page, lc)
                all_rows.extend(rows)
            except Exception:
                continue

        await context.close()
        await browser.close()
        return all_rows

# -------------------- COUNTRY LISTING --------------------
def _spotify_locale_from_alpha2(alpha2: str) -> str:
    """Spotify uses /{locale}/premium/ where locale is often like 'fr-FR' or 'en-US'.
    For this scraper we accept alpha2 and convert to common 'xx-XX' pattern.
    """
    a2 = (alpha2 or "").strip().lower()
    if len(a2) != 2:
        return ""
    return f"{a2}-{a2.upper()}"

def build_locale_list(test_mode: bool, test_countries=None):
    if test_mode and test_countries:
        locales = []
        for c in test_countries:
            if isinstance(c, str) and "-" in c:
                locales.append(c)
            else:
                lc = _spotify_locale_from_alpha2(c)
                if lc:
                    locales.append(lc)
        return locales

    # full list: use pycountry alpha_2 -> locale
    locales = []
    for c in pycountry.countries:
        try:
            locales.append(_spotify_locale_from_alpha2(c.alpha_2))
        except Exception:
            continue
    # Remove blanks and de-dup while preserving order
    out = []
    seen = set()
    for lc in locales:
        if lc and lc not in seen:
            seen.add(lc)
            out.append(lc)
    return out

# -------------------- PUBLIC ENTRYPOINT --------------------
def run_spotify_scraper(test_mode: bool = False, test_countries=None, out_xlsx: str = "spotify_prices.xlsx"):
    """Run the scraper and write results to an Excel file. Returns path to the file."""
    locales = build_locale_list(test_mode=test_mode, test_countries=test_countries)

    rows = asyncio.run(scrape_all_countries(locales))

    df = pd.DataFrame(rows)
    if not df.empty:
        # normalize numeric price to float where possible (but keep original text if parsing failed)
        def _to_float(x):
            try:
                return float(x)
            except Exception:
                return None
        df["Price_float"] = df["Price"].apply(_to_float)

    out_path = Path(out_xlsx).resolve()
    df.to_excel(out_path, index=False)
    return str(out_path)

# For local CLI usage
if __name__ == "__main__":
    # Example: run only FR for quick sanity check
    path = run_spotify_scraper(test_mode=True, test_countries=["FR"], out_xlsx="spotify_prices_FR.xlsx")
    print("Wrote:", path)

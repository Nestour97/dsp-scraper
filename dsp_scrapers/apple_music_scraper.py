# apple_music_plans_robust.py
# ------------------------------------------------------------
# Apple Music scraper with strong redirect detection + Spotify-style
# currency parsing (no 'TRY' false positives), USD disambiguation,
# and rich provenance columns.
# ------------------------------------------------------------

import re, time, threading, sqlite3, asyncio
from datetime import datetime, UTC, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

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

MAX_WORKERS = 6

# Robust session
SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504]
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

translator = GoogleTranslator(source="auto", target="en")

TIER_ORDER = ["Student", "Individual", "Family"]

EXTRA_REGIONS = {"HK", "MO", "XK", "PR"}
MANUAL_REGION_METADATA = {
    "XK": {"name": "Kosovo"},
    "MO": {"name": "Macao"},
    "HK": {"name": "Hong Kong"},
    "PR": {"name": "Puerto Rico"},
}

# CN uses a different host
APPLE_BASE_BY_CC = {"CN": "https://www.apple.com.cn"}

REGION_LOCALE_PATHS = {
    "HK": ["hk/en", "hk/zh", "hk/zh-tw", "hk-zh", "hk-zh-tw", "hk"],
    "MO": ["mo/en", "mo/zh", "mo/zh-tw", "mo-zh", "mo-zh-tw", "mo"],
    "CN": [""],  # -> https://www.apple.com.cn/apple-music/
}
# --- FIX: ensure correct storefront paths for US and GB (UK preferred path)
REGION_LOCALE_PATHS.update({
    "US": [""],          # -> https://www.apple.com/apple-music/
    "GB": ["uk", "gb"],  # -> https://www.apple.com/uk/apple-music/ then /gb/ as fallback
})
# --- FIX END

MISSING_DB = "apple_music_missing.sqlite"
MISSING_CSV = "apple_music_missing.csv"
MISSING_BUFFER = []

PRICE_HINT_SELECTORS = [
    ("p", "plan-type cost"),
    ("span", "price"),
    ("span", "current-price"),
    ("span", "ac-price"),
    ("div", "price"),
    ("li", "price"),
    ("span", "price-point"),
    ("span", "pricing__amount"),
    ("div", "pricing"),
]

# includes Kazakhstani tenge symbol ₸
CURRENCY_CHARS = r"[$€£¥₩₫₱₹₪₭₮₦₲₴₡₵₺₼₸៛₨₥₾฿]"

# allow for Tenge (₸) and others; used in banner/DOM price tokenization
CURRENCY_TOKEN = (
    r"(US\$|CA\$|AU\$|HK\$|NT\$|MOP\$|NZ\$|RM|S/\.|R\$|CHF|Rp|kr|"
    r"\$|€|£|¥|₩|₫|₱|₹|₪|₭|₮|₦|₲|₴|₸|TSh|KSh|USh|ZAR|ZWL|R|"
    r"SAR|QAR|AED|KWD|BHD|OMR)"
)

# allow spaces as thousand separators, e.g. "₸1 490,00" or "1 490.00"
NUMBER_TOKEN = r"(\d+(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?)"

# allow arbitrary whitespace between currency and number and vice versa
BANNER_PRICE_REGEX = re.compile(
    rf"(?:{CURRENCY_TOKEN}\s*{NUMBER_TOKEN}|{NUMBER_TOKEN}\s*{CURRENCY_TOKEN})"
)

# stricter numeric pattern, also allowing spaces as thousands separators
STRICT_PRICE_NUMBER = re.compile(
    r"(\d{1,3}(?:[.,\s]\d{3})+|\d+[.,]\d{1,2})"
)

BANNER_SEMAPHORE = threading.Semaphore(3)

COUNTRY_CORRECTIONS = {
    "台灣": "Taiwan",
    "대한민국": "South Korea",
    "ไทย": "Thailand",
    "澳門": "Macao",
}
MANUAL_COUNTRY_FIXES = {
    "Space": "Macao",
    "Italia": "Italy",
    "Suisse": "Switzerland",
    "Finnish": "Finland",
    "The Netherlands": "Netherlands",
    "Moldova, Republic of": "Moldova",
    "Greek": "Greece",
}

TEST_MODE = False
TEST_COUNTRIES = [
    "US", "AF", "AQ", "DZ", "AR", "BR", "BG", "CO", "ID", "IN", "IQ", "HK","NO","DK",
    "MO", "CN", "KW", "SA", "ZA", "JP", "KR", "EC", "BO"," KZ", "NG", "PH", "PK","HK","KH","RU","AM","TR","TJ","XF"
]

APPLE_US_HUB = "https://www.apple.com/apple-music/"

# ================= Spotify-style currency logic =================

def _clean(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

@lru_cache(maxsize=4096)
def translate_text_cached(text: str) -> str:
    try:
        return (translator.translate(text or "") or "").lower()
    except Exception:
        return (text or "").lower()

HARDCODE_FALLBACKS = {
    # Americas + dollarised
    "US": "USD", "CA": "CAD", "MX": "MXN", "BR": "BRL", "AR": "ARS",
    "CL": "CLP", "CO": "COP", "PE": "PEN", "UY": "UYU", "PY": "PYG",
    "BO": "BOB", "NI": "NIO", "GT": "GTQ", "CR": "CRC", "PA": "PAB",
    "HN": "HNL", "DO": "DOP", "JM": "JMD", "BB": "BBD", "BS": "BSD",
    "BZ": "BZD", "EC": "USD", "SV": "USD", "PR": "USD",

    # Europe & Eurasia
    "GB": "GBP", "IE": "EUR", "FR": "EUR", "DE": "EUR", "ES": "EUR", "IT": "EUR",
    "PT": "EUR", "NL": "EUR", "BE": "EUR", "LU": "EUR", "AT": "EUR", "FI": "EUR",
    "EE": "EUR", "LV": "EUR", "LT": "EUR", "SK": "EUR", "SI": "EUR", "GR": "EUR",
    "CY": "EUR", "MT": "EUR", "BG": "BGN", "RO": "RON", "PL": "PLN", "CZ": "CZK",
    "HU": "HUF", "HR": "EUR", "DK": "DKK", "SE": "SEK", "NO": "NOK", "IS": "ISK",
    "CH": "CHF", "RS": "RSD", "BA": "BAM", "MK": "MKD", "AL": "ALL",
    "UA": "UAH", "GE": "GEL", "AZ": "AZN", "AM": "AMD", "KZ": "KZT", "MD": "MDL",
    "BY": "BYN", "TR": "TRY", "RU": "RUB",

    # MENA
    "AE": "AED", "SA": "SAR", "QA": "QAR", "KW": "KWD", "BH": "BHD", "OM": "OMR",
    "IL": "ILS", "EG": "EGP", "MA": "MAD", "TN": "TND", "DZ": "DZD", "IQ": "IQD",

    # Africa
    "ZA": "ZAR", "NG": "NGN", "GH": "GHS", "KE": "KES", "TZ": "TZS", "UG": "UGX",
    "CM": "XAF", "CI": "XOF", "SN": "XOF", "RW": "RWF", "BI": "BIF", "CD": "CDF",
    "BJ": "XOF", "TD": "XAF", "CG": "XAF", "GA": "XAF", "NE": "XOF",

    # APAC & Pacific
    "JP": "JPY", "KR": "KRW", "CN": "CNY", "TW": "TWD", "HK": "HKD", "MO": "MOP",
    "SG": "SGD", "MY": "MYR", "TH": "THB", "VN": "VND", "PH": "PHP", "ID": "IDR",
    "IN": "INR", "PK": "PKR", "LK": "LKR", "NP": "NPR", "BD": "BDT",
    # 👇 manual override: Cambodia Apple Music uses USD pricing in this scraper
    "KH": "USD", "MN": "MNT", "TJ": "TJS",
    "AU": "AUD", "NZ": "NZD",
    "KI": "AUD", "NR": "AUD", "TV": "AUD", "MH": "USD",
}
KNOWN_ISO = set(HARDCODE_FALLBACKS.values())  # we will ignore bare 'TRY' as ISO

# Strong tokens (explicit, unambiguous)
STRONG_TOKENS = [
    (r"(?i)US\$", "USD"), (r"(?i)\$US", "USD"), (r"(?i)U\$S", "USD"),
    (r"(?i)\bA\$", "AUD"), (r"(?i)\bNZ\$", "NZD"), (r"(?i)\bHK\$", "HKD"),
    (r"(?i)\bNT\$", "TWD"), (r"(?i)\bS\$", "SGD"), (r"(?i)\bRD\$", "DOP"),
    (r"(?i)\bN\$", "NAD"),
    (r"R\$", "BRL"), (r"S/\.", "PEN"), (r"S/", "PEN"), (r"Bs\.?", "BOB"),
    (r"Gs\.?", "PYG"), (r"₲", "PYG"), (r"Q(?=[\s\d])", "GTQ"),
    (r"KSh", "KES"), (r"TSh", "TZS"), (r"USh", "UGX"), (r"Rp", "IDR"),
    (r"€", "EUR"), (r"£", "GBP"), (r"₹", "INR"),
    (r"(?<![A-Z])R\s?(?=\d)", "ZAR"),  # 'R 69,99' South Africa
]

# single-symbol mapping, now including tenge ₸
SINGLE_SYMBOL_TO_ISO = {
    "₩": "KRW",
    "₫": "VND",
    "₺": "TRY",
    "₪": "ILS",
    "₴": "UAH",
    "₼": "AZN",
    "₾": "GEL",
    "₭": "LAK",
    "฿": "THB",
    "₦": "NGN",
    "₵": "GHS",
    "₱": "PHP",
    "₸": "KZT",  # Kazakhstani tenge
}

AMBIG_TOKENS = {
    r"\$": {"USD","MXN","ARS","CLP","COP","CAD","AUD","NZD","SGD","HKD","TWD",
            "UYU","BBD","BSD","DOP","CRC","PAB","HNL","JMD"},
    r"\bkr\.?\b": {"SEK","NOK","DKK","ISK"},
    r"\bRs\.?\b": {"INR","PKR","LKR","NPR"},
    r"₨": {"INR","PKR","LKR","NPR"},
    r"(?i)\bC\$\b": {"CAD","NIO"},
}

DOLLAR_CURRENCIES = {"USD","CAD","AUD","NZD","SGD","HKD","TWD","MXN","ARS",
                     "CLP","COP","UYU","BBD","BSD","DOP","CRC","PAB","HNL","JMD"}

def default_currency_for_alpha2(alpha2: str) -> str:
    """
    Territory → ISO currency, with our HARDCODE_FALLBACKS taking precedence
    (so we can force USD for Cambodia etc.).
    """
    iso2 = (alpha2 or "").upper()
    # Manual overrides (including dollarised / special territories) win
    if iso2 in HARDCODE_FALLBACKS:
        return HARDCODE_FALLBACKS[iso2]
    # Otherwise, ask Babel
    try:
        currs = get_territory_currencies(iso2, date=date.today(), non_tender=False)
        if currs:
            return currs[0]
    except Exception:
        pass
    return ""

def detect_currency_in_text(text: str, alpha2: str):
    s = _clean(text)
    if not s:
        return "", "territory_default"

    # strong tokens
    for pat, iso in STRONG_TOKENS:
        if re.search(pat, s):
            return iso, "symbol"

    # single-char symbols
    if "¥" in s and not re.search(r"[A-Z]{3}", s, re.I):
        cc = (alpha2 or "").upper()
        return ("CNY" if cc == "CN" else "JPY" if cc == "JP" else default_currency_for_alpha2(cc), "symbol")
    for sym, iso in SINGLE_SYMBOL_TO_ISO.items():
        if sym in s:
            return iso, "symbol"

    # ISO near number — ignore 'TRY' entirely to avoid "Try 1 month free"
    S = s.upper()
    for m in re.finditer(r"\b([A-Z]{3})\b", S):
        code = m.group(1)
        if code == "TRY":
            continue
        if code in KNOWN_ISO:
            a, b = m.span()
            window = S[max(0, a - 6):min(len(S), b + 6)]
            if re.search(r"\d", window):
                return code, "code"

    # ambiguous → default
    for pat in AMBIG_TOKENS.keys():
        if re.search(pat, s):
            return default_currency_for_alpha2(alpha2), "ambiguous->default"

    return default_currency_for_alpha2(alpha2), "territory_default"

def detect_currency_from_display(display_text: str, alpha2: str):
    # normalize spaces / NBSPs etc
    s = _clean(display_text or "")
    if not s:
        return "", "empty", ""

    # strong tokens (US$, HK$, etc.)
    for pat, iso in STRONG_TOKENS:
        m = re.search(pat, s)
        if m:
            return iso, "symbol", m.group(0)

    # single-char symbols with special ¥ handling
    if "¥" in s and not re.search(r"[A-Z]{3}", s, re.I):
        cc = (alpha2 or "").upper()
        return (
            "CNY" if cc == "CN" else "JPY" if cc == "JP" else default_currency_for_alpha2(cc),
            "symbol",
            "¥",
        )
    for sym, iso in SINGLE_SYMBOL_TO_ISO.items():
        if sym in s:
            return iso, "symbol", sym

    # explicit ISO codes like "USD", "GBP", etc. (still ignoring TRY)
    S = s.upper()
    for m in re.finditer(r"\b([A-Z]{3})\b", S):
        code = m.group(1)
        if code == "TRY":
            continue
        if code in KNOWN_ISO:
            return code, "code", code

    # ambiguous bare symbols → fall back to territory default
    # allow arbitrary whitespace after '$' so "$ 5.26" is caught
    if re.search(r"(^|[^A-Z])\$(?=\s*\d)", s):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "$"
    if re.search(r"\bkr\b", s, re.I):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "kr"
    if re.search(r"\bRs\b", s):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "Rs"
    if "₨" in s:
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "₨"

    # nothing explicit → territory default (Babel / hardcoded)
    return default_currency_for_alpha2(alpha2), "territory_default", ""

def resolve_dollar_ambiguity(iso_guess: str, raw_token: str, amount, alpha2: str, context_text: str):
    """Heuristic USD resolver for naked '$' in non-$ countries (GCC, etc.)."""
    if raw_token != "$":
        return iso_guess, None
    default_iso = default_currency_for_alpha2(alpha2)
    if default_iso in DOLLAR_CURRENCIES:
        return iso_guess, None
    # explicit hints anywhere
    if re.search(r"(?i)\bUS\$|\$US|\bUSD\b", context_text):
        return "USD", "context-usd"
    # GCC heuristic
    if alpha2 in {"KW", "QA", "BH", "OM"}:
        return "USD", "gcc-usd"
    # very small $-price → likely USD
    try:
        v = float(amount)
        if v <= 50:
            return "USD", "small-$-usd"
    except Exception:
        pass
    return iso_guess, None

def _normalize_number(p: str) -> str:
    p = (p or "").replace(" ", "")
    dm = re.search(r"([.,])(\d{1,2})$", p)
    if dm:
        frac = dm.group(2)
        base = p[:-len(dm.group(0))].replace(".", "").replace(",", "")
        try:
            return str(float(base + "." + frac))
        except Exception:
            return ""
    try:
        return str(float(p.replace(".", "").replace(",", "")))
    except Exception:
        return ""

def extract_amount_number(text: str) -> str:
    """
    Extract numeric amount from a price string, allowing for:
    - thousand separators with spaces or punctuation (e.g. '₸1 490,00')
    - decimals with . or ,
    """
    if not isinstance(text, str) or not text.strip():
        return ""
    t = _clean(text)
    S = t.upper()

    # loose numeric pattern: digits plus optional spaces/.,,
    num_pat = r"\d[\d\s.,]*"

    # strong tokens like 'US$' etc.
    for pat, _ in STRONG_TOKENS:
        m = re.search(pat, t)
        if m:
            n = re.search(num_pat, t[m.end():])
            if n:
                return _normalize_number(n.group(0))

    # ISO with number after (e.g. "USD 3.29" or "KZT 1 490,00")
    m = re.search(r"\b([A-Z]{3})\b\s*(" + num_pat + ")", S)
    if m and m.group(1) != "TRY":
        return _normalize_number(m.group(2))

    # number before ISO (e.g. "3.29 USD")
    m = re.search("(" + num_pat + r")\s*\b([A-Z]{3})\b", S)
    if m and m.group(2) != "TRY":
        return _normalize_number(m.group(1))

    # symbol-based patterns including ₸
    m = re.search(
        r"(?:US\$|[€£¥₩₫₺₪₴₼₾₭฿₦₵₱₸]|NT\$|HK\$|S/\.|S/|R\$|RD\$|N\$|KSh|TSh|USh|Rp)\s*" + num_pat,
        t,
        re.I,
    )
    if m:
        n = re.search(num_pat, m.group(0))
        if n:
            return _normalize_number(n.group(0))

    # fallback: last numeric chunk in string
    cand = [m.group(0) for m in re.finditer(num_pat, t)]
    if cand:
        return _normalize_number(cand[-1])
    return ""

# ================= Helpers =================

@lru_cache(maxsize=None)
def normalize_country_name(name):
    name = COUNTRY_CORRECTIONS.get(name, name)
    try:
        return pycountry.countries.lookup(name).name
    except Exception:
        try:
            t = translator.translate(name)
            t = COUNTRY_CORRECTIONS.get(t, t)
            return pycountry.countries.lookup(t).name
        except Exception:
            return name

@lru_cache(maxsize=None)
def get_country_code(name):
    name = MANUAL_COUNTRY_FIXES.get(name, name)
    try:
        return pycountry.countries.lookup(name).alpha_2
    except Exception:
        try:
            m = pycountry.countries.search_fuzzy(name)
            return m[0].alpha_2 if m else ""
        except Exception:
            return ""

@lru_cache(maxsize=None)
def get_country_name_from_code(code):
    try:
        obj = pycountry.countries.get(alpha_2=code)
        if obj:
            return obj.name
    except Exception:
        pass
    return MANUAL_REGION_METADATA.get(code, {}).get("name", code)

@lru_cache(maxsize=None)
def standardize_plan(plan_text, idx):
    s = translate_text_cached(plan_text)
    if "student" in s:
        return "Student"
    if "individual" in s or "personal" in s:
            return "Individual"
    if "family" in s:
        return "Family"
    return TIER_ORDER[idx] if idx < len(TIER_ORDER) else plan_text

def init_missing_db():
    con = sqlite3.connect(MISSING_DB)
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS missing (
        ts TEXT, country TEXT, country_code TEXT, url TEXT, reason TEXT)"""
    )
    con.commit()
    con.close()

def log_missing(country, code, url, reason):
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    con = sqlite3.connect(MISSING_DB)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO missing (ts,country,country_code,url,reason) VALUES (?,?,?,?,?)",
        (ts, country, code, url, reason),
    )
    con.commit()
    con.close()
    MISSING_BUFFER.append(
        {"ts": ts, "country": country, "country_code": code, "url": url, "reason": reason}
    )

# ================= DOM parsing =================

def _price_tokens_from_text(text: str):
    if not text:
        return []
    text = _clean(text)  # normalize NBSPs, trim, etc.

    tokens = [m.group(0) for m in BANNER_PRICE_REGEX.finditer(text)]
    tokens += [
        m.group(0)
        for m in re.finditer(r"\b[A-Z]{3}\b\s*" + NUMBER_TOKEN, text)
    ]
    tokens += [
        m.group(0)
        for m in re.finditer(NUMBER_TOKEN + r"\s*\b[A-Z]{3}\b", text)
    ]
    seen, out = set(), []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def candidate_price_nodes(card: Tag):
    nodes = list(card.select(
        "p.plan-type.cost, p.tile-headline, "
        "[class*=cost], [class*=price], [class*=headline], [class*=subhead]"
    ))
    for el in card.find_all(True):
        txt = (el.get("aria-label") or el.get_text(" ", strip=True) or "")
        if txt and re.search(rf"{CURRENCY_CHARS}|Rp|SAR|QAR|AED|KWD|BHD|OMR|RM|HK\$|NT\$|US\$", txt):
            nodes.append(el)
    seen, out = set(), []
    for n in nodes:
        if id(n) not in seen:
            seen.add(id(n)); out.append(n)
    return out

def extract_plan_entries_from_dom_apple(soup: BeautifulSoup, alpha2: str):
    section = (soup.find("section", attrs={"data-analytics-name": re.compile("plans", re.I)})
               or soup.find("section", class_=re.compile("section-plans", re.I))
               or soup)
    cards = section.select("div.plan-list-item, li.gallery-item, li[role='listitem']")
    if not cards:
        return {}

    def classify(card: Tag) -> str:
        cid = (card.get("id") or "").lower()
        if cid in {"student", "individual", "family"}:
            return cid.capitalize()
        low = {c.lower() for c in (card.get("class") or [])}
        if "student" in low:
            return "Student"
        if "individual" in low or "personal" in low:
            return "Individual"
        if "family" in low:
            return "Family"
        head = card.select_one("h2, h3, h4, p")
        if head:
            return standardize_plan(head.get_text(" ", strip=True), 0)
        return "Individual"

    entries = {}
    for card in cards:
        std = classify(card)
        full_text = " ".join(card.stripped_strings)

        chosen_tok, chosen_val = None, None
        for el in candidate_price_nodes(card):
            raw = (el.get("aria-label") or el.get_text(" ", strip=True) or "")
            for tok in _price_tokens_from_text(raw):
                num = extract_amount_number(tok)
                if not num:
                    continue
                try:
                    val = float(num) if "." in num else int(num)
                except Exception:
                    continue
                chosen_tok, chosen_val = tok, val
                break
            if chosen_tok:
                break
        if not chosen_tok:
            continue

        iso, src, raw_cur = detect_currency_from_display(chosen_tok, alpha2)
        # context override if still ambiguous/default
        if src in {"ambiguous_symbol->default", "territory_default"}:
            iso2, src2 = detect_currency_in_text(full_text, alpha2)
            if src2 in {"symbol", "code"} and iso2:
                iso, src, raw_cur = iso2, f"context-{src2}", raw_cur
            # dollar resolver
            iso_res, why = resolve_dollar_ambiguity(iso, raw_cur, chosen_val, alpha2, full_text)
            if why:
                iso, src = iso_res, f"heuristic-{why}"

        if std not in entries:
            entries[std] = {
                "Currency": iso,
                "Currency Source": src,
                "Currency Raw": raw_cur,
                "Price Display": _clean(chosen_tok),
                "Price Value": chosen_val,
            }
    return entries

def extract_plan_entries_from_dom_generic(soup: BeautifulSoup, alpha2: str):
    entries = {}
    plan_lists = soup.find_all(attrs={"class": re.compile(r"(plan|tier|pricing)", re.I)}) or [soup]
    for container in plan_lists:
        cards = container.find_all(True, class_=re.compile(r"(plan|tier|card)", re.I)) or [container]
        for idx, card in enumerate(cards):
            lab = (card.find("p", class_=re.compile("plan-type|name", re.I))
                   or card.find(re.compile("h[2-4]")) or card)
            plan_name = _clean(lab.get_text()) if lab else f"Plan {idx+1}"
            std = standardize_plan(plan_name, idx)
            raw_text = " ".join(card.stripped_strings)

            tok = ""
            for el in candidate_price_nodes(card):
                txt = (el.get("aria-label") or el.get_text(" ", strip=True) or "")
                for t in _price_tokens_from_text(txt):
                    tok = t
                    break
                if tok:
                    break
            if not tok:
                continue

            num = extract_amount_number(tok)
            if not num:
                continue
            try:
                val = float(num) if "." in num else int(num)
            except Exception:
                continue

            iso, src, raw_cur = detect_currency_from_display(tok, alpha2)
            if src in {"ambiguous_symbol->default", "territory_default"}:
                iso2, src2 = detect_currency_in_text(raw_text, alpha2)
                if src2 in {"symbol", "code"} and iso2:
                    iso, src, raw_cur = iso2, f"context-{src2}", raw_cur
                iso_res, why = resolve_dollar_ambiguity(iso, raw_cur, val, alpha2, raw_text)
                if why:
                    iso, src = iso_res, f"heuristic-{why}"

            entries.setdefault(
                std,
                {
                    "Currency": iso,
                    "Currency Source": src,
                    "Currency Raw": raw_cur,
                    "Price Display": _clean(tok),
                    "Price Value": val,
                },
            )
    return entries

def extract_plan_entries_from_dom(soup: BeautifulSoup, alpha2: str):
    entries = extract_plan_entries_from_dom_apple(soup, alpha2)
    if entries:
        return entries
    return extract_plan_entries_from_dom_generic(soup, alpha2)

# ================= Banner fallback =================

APPLE_HOST_RE = r"apple\.com(?:\.cn)?"
CC_URL_RE = re.compile(rf"{APPLE_HOST_RE}/([a-z]{{2}})(?:/|-[a-z]{{2}}(?:-[a-z]{{2}})?/)", re.I)
MUSIC_CC_URL_RE = re.compile(r"music\.apple\.com/([a-z]{2})/", re.I)

def _extract_cc(url):
    if not url:
        return ""
    m = CC_URL_RE.search(url) or MUSIC_CC_URL_RE.search(url)
    return (m.group(1) or "").upper() if m else ""

async def _get_music_banner_text_async(country_code: str):
    cc = country_code.lower()
    candidates = [
        f"https://music.apple.com/{cc}/new",
        f"https://music.apple.com/{cc}/browse",
        f"https://music.apple.com/{cc}/listen-now",
    ]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        last = ""
        for url in candidates:
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_load_state("networkidle", timeout=10000)
                last = page.url or (resp.url if resp else url)
                sels = [
                    "cwc-music-upsell-banner-web [data-test='subheader-text']",
                    "[data-test='subheader-text']",
                    ".cwc-upsell-banner__subhead",
                ]
                for sel in sels:
                    try:
                        el = page.locator(sel).first
                        await el.wait_for(state="visible", timeout=3000)
                        t = await el.inner_text()
                        if t and t.strip():
                            await browser.close()
                            return t, last
                    except PWTimeoutError:
                        continue
                t = await page.evaluate("document.body && document.body.innerText || ''")
                if t and (BANNER_PRICE_REGEX.search(t) or STRICT_PRICE_NUMBER.search(t)):
                    await browser.close()
                    return t, last
            except Exception:
                continue
        await browser.close()
        return "", last

def _extract_price_from_banner_text(text: str):
    if not text:
        return "", "", None
    clean = _clean(text)
    m = BANNER_PRICE_REGEX.search(clean)
    if m:
        g = m.groups()
        # groups: (currency1, number1, number2, currency2) due to our pattern structure
        if g[0] and g[1]:
            currency, num = g[0], g[1]
            disp = f"{currency} {num}"
        elif g[2] and g[3]:
            num, currency = g[2], g[3]
            disp = f"{num} {currency}"
        else:
            return "", "", None
    else:
        m2 = STRICT_PRICE_NUMBER.search(clean)
        if not m2:
            return "", "", None
        num = m2.group(1)
        currency = ""
        disp = num
    try:
        n = _normalize_number(num)
        val = float(n) if "." in n else int(n)
    except Exception:
        val = None
    return currency, disp, val

def banner_individual_row(alpha2: str, country_name: str, meta=None):
    # Fetch the upsell banner text from music.apple.com for this country code
    with BANNER_SEMAPHORE:
        try:
            text, final_url = asyncio.run(_get_music_banner_text_async(alpha2))
        except RuntimeError:
            # In case we're already inside an event loop (e.g. Jupyter),
            # run the coroutine in a separate thread.
            holder = {}

            def runner():
                holder["pair"] = asyncio.run(_get_music_banner_text_async(alpha2))

            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join()
            text, final_url = holder.get("pair", ("", ""))

    # IMPORTANT: ensure we actually landed on a country-specific music.apple.com storefront.
    # If AF redirects to US (music.apple.com/us/...), we must NOT assign that price to AF.
    store_cc = _extract_cc(final_url)
    if not store_cc or store_cc != alpha2.upper():
        # Log as "missing / mismatch" and return no rows for this country
        log_missing(
            country_name,
            alpha2,
            final_url or f"https://music.apple.com/{alpha2.lower()}/new",
            f"music.apple.com storefront mismatch (requested={alpha2}, final={store_cc or 'NONE'})",
        )
        return []

    # Extract the price from the banner text
    cur_sym, disp, val = _extract_price_from_banner_text(text)
    if val is None:
        # No usable numeric price found
        return []

    # Detect currency using your Spotify-style logic
    iso, src, raw = detect_currency_from_display(disp or cur_sym, alpha2)
    raw = raw or cur_sym or ""

    if src in {"ambiguous_symbol->default", "territory_default"}:
        # First let the full banner copy try to upgrade symbol/code detection
        iso2, src2 = detect_currency_in_text(text, alpha2)
        if iso2 and src2 in {"symbol", "code"}:
            iso, src = iso2, f"context-{src2}"

        # Then run the same USD disambiguation used for DOM parsing
        iso_res, why = resolve_dollar_ambiguity(iso, raw, val, alpha2, text)
        if why:
            iso, src = iso_res, f"heuristic-{why}"

    row = {
        "Country": country_name,
        "Country Code": alpha2,
        "Currency": iso,
        "Currency Source": src,
        "Currency Raw": raw,
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

def looks_like_us_hub_url(url: str) -> bool:
    if not url:
        return False
    u = urlparse(url)
    return (u.netloc.endswith("apple.com") and u.path.rstrip("/") == "/apple-music")

def looks_like_us_hub_html(soup: BeautifulSoup) -> bool:
    can = soup.find("link", rel=re.compile("canonical", re.I))
    if can and can.get("href") and looks_like_us_hub_url(can.get("href")):
        return True
    og = soup.find("meta", property="og:url")
    if og and og.get("content") and looks_like_us_hub_url(og.get("content")):
        return True
    return False

def looks_like_us_content(soup: BeautifulSoup) -> bool:
    """English US hub signature (used only when no plan entries found)."""
    text = soup.get_text(" ", strip=True)
    t = text.lower()
    price_hit = re.search(r"\$ ?10\.99|\$ ?5\.99|\$ ?16\.99", text)
    copy_hit = ("try 1 month free" in t) or ("no commitment" in t and "cancel anytime" in t)
    return bool(price_hit and copy_hit)

# --- FIX: treat 'UK' as equivalent to 'GB' for storefront detection
def _storefront_equivalent(requested_cc: str, detected_cc: str) -> bool:
    if not detected_cc:
        return False
    r = (requested_cc or "").upper()
    d = (detected_cc or "").upper()
    if r == d:
        return True
    if r == "GB" and d == "UK":
        return True
    return False
# --- FIX END

# ================= Main scrape =================

def scrape_country(alpha2: str):
    cc = alpha2.upper()
    base = APPLE_BASE_BY_CC.get(cc, "https://www.apple.com")
    paths = REGION_LOCALE_PATHS.get(cc, [cc.lower()])

    last_status, last_url = None, None
    had_apple_page = False  # track if we ever saw a 200 Apple Music landing page

    for path in paths:
        url = f"{base}/apple-music/" if path == "" else f"{base}/{path}/apple-music/"
        last_url = url
        try:
            resp = SESSION.get(url, timeout=15, allow_redirects=True)
            last_status = resp.status_code

            # If we got a 200 from Apple, mark that the landing page exists
            if resp.status_code == 200 and "apple.com" in urlparse(resp.url).netloc:
                had_apple_page = True

            # (A) final URL is US hub
            # --- FIX: do NOT treat US itself as a redirect to US hub
            if cc != "US" and looks_like_us_hub_url(resp.url):
                cn = normalize_country_name(get_country_name_from_code(cc))
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": "US hub",
                        "Redirect Reason": "Final URL is US hub",
                        "Apple URL": resp.url,
                        # no country-specific page, we landed on generic hub
                        "Has Apple Music Page": False,
                    },
                )
            # --- FIX END

            # (B) redirected to different storefront
            final_cc = _extract_cc(resp.url)
            # --- FIX: allow 'UK' as equivalent when we asked for 'GB'
            if final_cc and not _storefront_equivalent(cc, final_cc) and not resp.url.startswith(APPLE_BASE_BY_CC.get(cc, "")):
                cn = normalize_country_name(get_country_name_from_code(cc))
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": final_cc,
                        "Redirect Reason": f"HTTP redirect to {final_cc}",
                        "Apple URL": resp.url,
                        # again, no standalone page for this cc
                        "Has Apple Music Page": False,
                    },
                )
            # --- FIX END

            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # (C) canonical/OG = US hub
            # --- FIX: skip for US itself
            if cc != "US" and looks_like_us_hub_html(soup):
                cn = normalize_country_name(get_country_name_from_code(cc))
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": "US hub",
                        "Redirect Reason": "Canonical/OG URL indicates US hub",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )
            # --- FIX END

            country_name = normalize_country_name(get_country_name_from_code(cc))
            country_name = MANUAL_COUNTRY_FIXES.get(country_name, country_name)
            code = (get_country_code(country_name) or cc).upper()

            entries = extract_plan_entries_from_dom(soup, code)

            # (D) No plan entries: check for US content signature and flag
            # --- FIX: skip for US itself
            if cc != "US" and not entries and looks_like_us_content(soup):
                return banner_individual_row(
                    code,
                    country_name,
                    meta={
                        "Redirected": True,
                        "Redirected To": "US hub",
                        "Redirect Reason": "Page content matches US hub",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )
            # --- FIX END

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
                            "Source": "apple.com.cn page" if base.endswith(".cn") else "apple.com page",
                            "Redirected": False,
                            "Redirected To": "",
                            "Redirect Reason": "",
                            "Apple URL": resp.url,
                            "Has Apple Music Page": True,
                        }
                    )
            if rows:
                return rows

            # Fallback banner if still nothing — but the landing page *did* exist,
            # we just failed to parse prices from it.
            return banner_individual_row(
                code,
                country_name,
                meta={
                    "Redirected": False,
                    "Redirected To": "",
                    "Redirect Reason": "",
                    "Apple URL": resp.url,
                    "Has Apple Music Page": True,
                },
            )

        except Exception:
            continue

    # Total failure → banner attempt; we *never* saw a working landing page
    cn = normalize_country_name(get_country_name_from_code(cc))
    return banner_individual_row(
        cc,
        cn,
        meta={
            "Redirected": False,
            "Redirected To": "",
            "Redirect Reason": "No country-specific Apple Music page; banner-only",
            "Apple URL": last_url or "",
            "Has Apple Music Page": had_apple_page,  # will be False in AF-style cases
        },
    )

# ================= Runner & tests =================

def run_currency_tests():
    samples = [
        ("KW", "Kuwait", "Try 1 month free — $5.49"),
        ("DZ", "Algeria", "US$ 5,49"),
        ("AR", "Argentina", "US$ 3,29"),
        ("BO", "Bolivia", "US$ 6,49"),
        ("BG", "Bulgaria", "BGN 9,99"),
        ("ZA", "South Africa", "R 69,99"),
        ("HK", "Hong Kong", "HK$108"),
        ("IN", "India", "₹59/month"),
        ("CN", "China", "每月 ¥11"),
        ("JP", "Japan", "¥1080"),
        ("KZ", "Kazakhstan", "₸1 490,00 / month"),
        ("KH", "Cambodia", "$ 3,29 / month"),
        ("RU", "Russia", "169,00 R"),
    ]
    print("CC  Country         Raw Text                           -> ISO  (source)")
    print("-" * 96)
    for cc, name, text in samples:
        iso, src = detect_currency_in_text(text, cc)
        print(f"{cc:<3} {name:<13} {text:<35} -> {iso:<4} ({src})")

def run_scraper(country_codes_override=None):
    """
    Core Apple Music scraping logic.

    If country_codes_override is given (iterable of ISO-2 codes), only those
    territories are scraped, regardless of TEST_MODE.
    """
    init_missing_db()

    iso_codes = {c.alpha_2 for c in pycountry.countries}
    all_codes = sorted(iso_codes.union(EXTRA_REGIONS))

    # Decide which territories to scrape
    if country_codes_override:
        requested = {
            (cc or "").strip().upper()
            for cc in country_codes_override
            if (cc or "").strip()
        }
        requested = {cc for cc in requested if len(cc) == 2}
        all_codes = sorted(requested)
        print(f"🎯 Subset mode: scraping {len(all_codes)} countries: {all_codes}")
    elif TEST_MODE:
        all_codes = sorted({c.upper() for c in TEST_COUNTRIES})
        print(f"🧪 TEST MODE: scraping {len(all_codes)} countries: {all_codes}")
    else:
        print(f"🌍 FULL MODE: scraping {len(all_codes)} countries")

    if not all_codes:
        print("⚠️ No country codes to scrape.")
        return

    all_rows = []
    failed_codes = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(scrape_country, cc): cc for cc in all_codes}
        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Scraping countries",
            unit="cc",
        ):
            cc = futures[fut]
            try:
                res = fut.result()
                if res:
                    all_rows.extend(res)
            except Exception as e:
                failed_codes.append(cc)
                cn = normalize_country_name(get_country_name_from_code(cc))
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Future exception: {type(e).__name__}: {e}",
                )

    # Second pass: retry failures sequentially (less chance of rate limiting)
    if failed_codes:
        print(f"🔁 Retrying {len(failed_codes)} failed countries sequentially…")
        for cc in failed_codes:
            try:
                res = scrape_country(cc)
                if res:
                    all_rows.extend(res)
                    # drop previous missing-log entries for this cc
                    MISSING_BUFFER[:] = [
                        m for m in MISSING_BUFFER if m.get("country_code") != cc
                    ]
            except Exception as e:
                cn = normalize_country_name(get_country_name_from_code(cc))
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Retry exception: {type(e).__name__}: {e}",
                )

    if not all_rows:
        print("⚠️ No rows scraped at all.")
        return

    df = pd.DataFrame(all_rows)
    df["Plan"] = pd.Categorical(df["Plan"], TIER_ORDER, ordered=True)
    df.sort_values(["Country", "Plan"], inplace=True, ignore_index=True)

    cols = [
        "Country",
        "Country Code",
        "Currency",
        "Currency Raw",
        "Plan",
        "Price Display",
        "Price Value",
    ]
    df = df[cols]

    # Name file depending on full vs test/subset
    out_name = (
        "apple_music_plans_TEST.xlsx"
        if (TEST_MODE or country_codes_override)
        else "apple_music_plans_all.xlsx"
    )
    df.to_excel(out_name, index=False)
    print(f"✅ Exported to {out_name} (rows={len(df)})")

    if MISSING_BUFFER:
        pd.DataFrame(MISSING_BUFFER).to_csv(MISSING_CSV, index=False)
        print(f"⚠️ Logged {len(MISSING_BUFFER)} issues to {MISSING_CSV} / {MISSING_DB}")

    return out_name


def run_apple_music_scraper(test_mode=True, country_codes=None):
    """
    Wrapper used by the web app.

    test_mode = True  -> behaves like your TEST_MODE run
    test_mode = False -> full all-countries run

    If country_codes is provided (list of ISO-2 codes), only those countries
    are scraped (regardless of test_mode).
    """
    global TEST_MODE
    TEST_MODE = bool(test_mode)

    start = time.time()
    out_name = run_scraper(country_codes_override=country_codes)
    print(f"[APPLE MUSIC] Finished in {round(time.time() - start, 2)}s")

    return out_name


if __name__ == "__main__":
    run_currency_tests()
    start = time.time()
    run_scraper()
    print(f"⏱️ Finished in {round(time.time() - start, 2)}s")

if __name__ == "__main__":
    run_currency_tests()
    start = time.time()
    run_scraper()
    print(f"⏱️ Finished in {round(time.time() - start, 2)}s")





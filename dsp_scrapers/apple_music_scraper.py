# apple_music_scraper.py
# ------------------------------------------------------------
# Apple Music scraper with:
# - Strong redirect detection
# - Robust currency parsing (Spotify-style)
# - Multi-language recurring-price selection (avoids intro/trial prices)
# - Fixes:
#   * CN: supports RMB pricing on apple.com.cn + multiple plans
#   * CO / BR: avoids footnote "8 $" / "9 $" mis-parses + stitches split prices
#   * Many locales: avoids promo/intro "3 months for ..." picking instead of monthly
#   * If Individual looks suspicious, overrides with music.apple.com recurring price
# - More robust DOM extraction with semantic fallback (helps JP and markup changes)
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
# ensure correct storefront paths for US and GB
REGION_LOCALE_PATHS.update(
    {
        "US": [""],  # -> https://www.apple.com/apple-music/
        "GB": ["uk", "gb"],  # -> https://www.apple.com/uk/apple-music/ then /gb/ as fallback
    }
)

MISSING_DB = "apple_music_missing.sqlite"
MISSING_CSV = "apple_music_missing.csv"
MISSING_BUFFER = []

# includes fullwidth yen ￥, adds RMB/CN¥ support
CURRENCY_CHARS = r"[$€£¥￥₩₫₱₹₪₭₮₦₲₴₡₵₺₼₸៛₨₥₾฿]"

CURRENCY_TOKEN = (
    r"(RMB|CN¥|US\$|CA\$|AU\$|HK\$|NT\$|MOP\$|NZ\$|RM|S/\.|R\$|CHF|Rp|kr|"
    r"\$|€|£|¥|￥|₩|₫|₱|₹|₪|₭|₮|₦|₲|₴|₸|TSh|KSh|USh|ZAR|ZWL|R|"
    r"SAR|QAR|AED|KWD|BHD|OMR)"
)

# allow spaces as thousand separators, e.g. "₸1 490,00" or "1 490.00"
NUMBER_TOKEN = r"(\d+(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?)"

# allow arbitrary whitespace between currency and number and vice versa
BANNER_PRICE_REGEX = re.compile(
    rf"(?:{CURRENCY_TOKEN}\s*{NUMBER_TOKEN}|{NUMBER_TOKEN}\s*{CURRENCY_TOKEN})\*?"
)

# stricter numeric pattern, also allowing spaces as thousands separators
STRICT_PRICE_NUMBER = re.compile(r"(\d{1,3}(?:[.,\s]\d{3})+|\d+[.,]\d{1,2})")

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
    "US",
    "AF",
    "DZ",
    "AR",
    "BR",
    "BG",
    "CO",
    "ID",
    "IN",
    "IQ",
    "HK",
    "NO",
    "DK",
    "MO",
    "CN",
    "KW",
    "SA",
    "ZA",
    "JP",
    "KR",
    "EC",
    "BO",
    "KZ",
    "NG",
    "PH",
    "PK",
    "KH",
    "RU",
    "AM",
    "TR",
    "TJ",
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
    "US": "USD",
    "CA": "CAD",
    "MX": "MXN",
    "BR": "BRL",
    "AR": "ARS",
    "CL": "CLP",
    "CO": "COP",
    "PE": "PEN",
    "UY": "UYU",
    "PY": "PYG",
    "BO": "BOB",
    "NI": "NIO",
    "GT": "GTQ",
    "CR": "CRC",
    "PA": "PAB",
    "HN": "HNL",
    "DO": "DOP",
    "JM": "JMD",
    "BB": "BBD",
    "BS": "BSD",
    "BZ": "BZD",
    "EC": "USD",
    "SV": "USD",
    "PR": "USD",
    # Europe & Eurasia
    "GB": "GBP",
    "IE": "EUR",
    "FR": "EUR",
    "DE": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "PT": "EUR",
    "NL": "EUR",
    "BE": "EUR",
    "LU": "EUR",
    "AT": "EUR",
    "FI": "EUR",
    "EE": "EUR",
    "LV": "EUR",
    "LT": "EUR",
    "SK": "EUR",
    "SI": "EUR",
    "GR": "EUR",
    "CY": "EUR",
    "MT": "EUR",
    "BG": "BGN",
    "RO": "RON",
    "PL": "PLN",
    "CZ": "CZK",
    "HU": "HUF",
    "HR": "EUR",
    "DK": "DKK",
    "SE": "SEK",
    "NO": "NOK",
    "IS": "ISK",
    "CH": "CHF",
    "RS": "RSD",
    "BA": "BAM",
    "MK": "MKD",
    "AL": "ALL",
    "UA": "UAH",
    "GE": "GEL",
    "AZ": "AZN",
    "AM": "AMD",
    "KZ": "KZT",
    "MD": "MDL",
    "BY": "BYN",
    "TR": "TRY",
    "RU": "RUB",
    # MENA
    "AE": "AED",
    "SA": "SAR",
    "QA": "QAR",
    "KW": "KWD",
    "BH": "BHD",
    "OM": "OMR",
    "IL": "ILS",
    "EG": "EGP",
    "MA": "MAD",
    "TN": "TND",
    "DZ": "DZD",
    "IQ": "IQD",
    # Africa
    "ZA": "ZAR",
    "NG": "NGN",
    "GH": "GHS",
    "KE": "KES",
    "TZ": "TZS",
    "UG": "UGX",
    "CM": "XAF",
    "CI": "XOF",
    "SN": "XOF",
    "RW": "RWF",
    "BI": "BIF",
    "CD": "CDF",
    "BJ": "XOF",
    "TD": "XAF",
    "CG": "XAF",
    "GA": "XAF",
    "NE": "XOF",
    # APAC & Pacific
    "JP": "JPY",
    "KR": "KRW",
    "CN": "CNY",
    "TW": "TWD",
    "HK": "HKD",
    "MO": "MOP",
    "SG": "SGD",
    "MY": "MYR",
    "TH": "THB",
    "VN": "VND",
    "PH": "PHP",
    "ID": "IDR",
    "IN": "INR",
    "PK": "PKR",
    "LK": "LKR",
    "NP": "NPR",
    "BD": "BDT",
    # manual override: Cambodia uses USD in this scraper
    "KH": "USD",
    "MN": "MNT",
    "TJ": "TJS",
    "AU": "AUD",
    "NZ": "NZD",
    "KI": "AUD",
    "NR": "AUD",
    "TV": "AUD",
    "MH": "USD",
}
KNOWN_ISO = set(HARDCODE_FALLBACKS.values())

STRONG_TOKENS = [
    (r"(?i)\bRMB\b", "CNY"),
    (r"CN¥", "CNY"),
    (r"(?i)US\$", "USD"),
    (r"(?i)\$US", "USD"),
    (r"(?i)U\$S", "USD"),
    (r"(?i)\bA\$", "AUD"),
    (r"(?i)\bNZ\$", "NZD"),
    (r"(?i)\bHK\$", "HKD"),
    (r"(?i)\bNT\$", "TWD"),
    (r"(?i)\bS\$", "SGD"),
    (r"(?i)\bRD\$", "DOP"),
    (r"(?i)\bN\$", "NAD"),
    (r"R\$", "BRL"),
    (r"S/\.", "PEN"),
    (r"S/", "PEN"),
    (r"Bs\.?", "BOB"),
    (r"Gs\.?", "PYG"),
    (r"₲", "PYG"),
    (r"Q(?=[\s\d])", "GTQ"),
    (r"KSh", "KES"),
    (r"TSh", "TZS"),
    (r"USh", "UGX"),
    (r"Rp", "IDR"),
    (r"€", "EUR"),
    (r"£", "GBP"),
    (r"₹", "INR"),
    (r"(?<![A-Z])R\s?(?=\d)", "ZAR"),
]

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
    "₸": "KZT",
    "￥": "JPY",  # refined by CC logic in detect_currency_* (CN -> CNY, JP -> JPY)
}

AMBIG_TOKENS = {
    r"\$": {
        "USD",
        "MXN",
        "ARS",
        "CLP",
        "COP",
        "CAD",
        "AUD",
        "NZD",
        "SGD",
        "HKD",
        "TWD",
        "UYU",
        "BBD",
        "BSD",
        "DOP",
        "CRC",
        "PAB",
        "HNL",
        "JMD",
    },
    r"\bkr\.?\b": {"SEK", "NOK", "DKK", "ISK"},
    r"\bRs\.?\b": {"INR", "PKR", "LKR", "NPR"},
    r"₨": {"INR", "PKR", "LKR", "NPR"},
    r"(?i)\bC\$\b": {"CAD", "NIO"},
}

DOLLAR_CURRENCIES = {
    "USD",
    "CAD",
    "AUD",
    "NZD",
    "SGD",
    "HKD",
    "TWD",
    "MXN",
    "ARS",
    "CLP",
    "COP",
    "UYU",
    "BBD",
    "BSD",
    "DOP",
    "CRC",
    "PAB",
    "HNL",
    "JMD",
}


def default_currency_for_alpha2(alpha2: str) -> str:
    iso2 = (alpha2 or "").upper()
    if iso2 in HARDCODE_FALLBACKS:
        return HARDCODE_FALLBACKS[iso2]
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

    for pat, iso in STRONG_TOKENS:
        if re.search(pat, s):
            return iso, "symbol"

    if ("¥" in s or "￥" in s) and not re.search(r"[A-Z]{3}", s, re.I):
        cc = (alpha2 or "").upper()
        return (
            "CNY"
            if cc == "CN"
            else "JPY"
            if cc == "JP"
            else default_currency_for_alpha2(cc),
            "symbol",
        )

    for sym, iso in SINGLE_SYMBOL_TO_ISO.items():
        if sym in s:
            if sym in {"¥", "￥"}:
                cc = (alpha2 or "").upper()
                return (
                    "CNY"
                    if cc == "CN"
                    else "JPY"
                    if cc == "JP"
                    else default_currency_for_alpha2(cc),
                    "symbol",
                )
            return iso, "symbol"

    S = s.upper()
    for m in re.finditer(r"\b([A-Z]{3})\b", S):
        code = m.group(1)
        if code == "TRY":  # avoid "Try 1 month free"
            continue
        if code in KNOWN_ISO:
            a, b = m.span()
            window = S[max(0, a - 6) : min(len(S), b + 6)]
            if re.search(r"\d", window):
                return code, "code"

    for pat in AMBIG_TOKENS.keys():
        if re.search(pat, s):
            return default_currency_for_alpha2(alpha2), "ambiguous->default"

    return default_currency_for_alpha2(alpha2), "territory_default"


def detect_currency_from_display(display_text: str, alpha2: str):
    s = _clean(display_text or "")
    if not s:
        return "", "empty", ""

    for pat, iso in STRONG_TOKENS:
        m = re.search(pat, s)
        if m:
            return iso, "symbol", m.group(0)

    if ("¥" in s or "￥" in s) and not re.search(r"[A-Z]{3}", s, re.I):
        cc = (alpha2 or "").upper()
        return (
            "CNY"
            if cc == "CN"
            else "JPY"
            if cc == "JP"
            else default_currency_for_alpha2(cc),
            "symbol",
            "¥" if "¥" in s else "￥",
        )

    for sym, iso in SINGLE_SYMBOL_TO_ISO.items():
        if sym in s:
            if sym in {"¥", "￥"}:
                cc = (alpha2 or "").upper()
                return (
                    "CNY"
                    if cc == "CN"
                    else "JPY"
                    if cc == "JP"
                    else default_currency_for_alpha2(cc),
                    "symbol",
                    sym,
                )
            return iso, "symbol", sym

    S = s.upper()
    for m in re.finditer(r"\b([A-Z]{3})\b", S):
        code = m.group(1)
        if code == "TRY":
            continue
        if code in KNOWN_ISO:
            return code, "code", code

    if re.search(r"(^|[^A-Z])\$(?=\s*\d)", s):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "$"
    if re.search(r"\bkr\b", s, re.I):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "kr"
    if re.search(r"\bRs\b", s):
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "Rs"
    if "₨" in s:
        return default_currency_for_alpha2(alpha2), "ambiguous_symbol->default", "₨"

    return default_currency_for_alpha2(alpha2), "territory_default", ""


def resolve_dollar_ambiguity(iso_guess: str, raw_token: str, amount, alpha2: str, context_text: str):
    if raw_token != "$":
        return iso_guess, None
    default_iso = default_currency_for_alpha2(alpha2)
    if default_iso in DOLLAR_CURRENCIES:
        return iso_guess, None
    if re.search(r"(?i)\bUS\$|\$US|\bUSD\b", context_text):
        return "USD", "context-usd"
    if alpha2 in {"KW", "QA", "BH", "OM"}:
        return "USD", "gcc-usd"
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
        base = p[: -len(dm.group(0))].replace(".", "").replace(",", "")
        try:
            return str(float(base + "." + frac))
        except Exception:
            return ""
    try:
        return str(float(p.replace(".", "").replace(",", "")))
    except Exception:
        return ""


def extract_amount_number(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    t = _clean(text)
    S = t.upper()

    num_pat = r"\d[\d\s.,]*"

    for pat, _ in STRONG_TOKENS:
        m = re.search(pat, t)
        if m:
            n = re.search(num_pat, t[m.end() :])
            if n:
                return _normalize_number(n.group(0))

    m = re.search(r"\b([A-Z]{3})\b\s*(" + num_pat + ")", S)
    if m and m.group(1) != "TRY":
        return _normalize_number(m.group(2))

    m = re.search("(" + num_pat + r")\s*\b([A-Z]{3})\b", S)
    if m and m.group(2) != "TRY":
        return _normalize_number(m.group(1))

    m = re.search(
        r"(?:RMB|CN¥|US\$|[€£¥￥₩₫₺₪₴₼₾₭฿₦₵₱₸]|NT\$|HK\$|S/\.|S/|R\$|RD\$|N\$|KSh|TSh|USh|Rp)\s*"
        + num_pat,
        t,
        re.I,
    )
    if m:
        n = re.search(num_pat, m.group(0))
        if n:
            return _normalize_number(n.group(0))

    cand = [m.group(0) for m in re.finditer(num_pat, t)]
    if cand:
        return _normalize_number(cand[-1])
    return ""


# ================= Helpers =================


def normalize_for_price_extraction(s: str) -> str:
    """
    Makes flattened DOM text more regex-friendly:
    - collapses whitespace
    - stitches "8 . 500" -> "8.500"
    - stitches "$ 8.500" -> "$8.500"
    - stitches "8 $ 500" -> "$8 500" (so NUMBER_TOKEN can capture 8 500)
    """
    s = _clean(s or "")
    if not s:
        return ""

    s = re.sub(r"\s+", " ", s)

    # stitch numeric punctuation if it got spaced out by DOM flattening: "8 . 500" => "8.500"
    s = re.sub(r"(\d)\s*([.,])\s*(\d)", r"\1\2\3", s)

    # stitch currency separated from number: "$ 8" => "$8"
    s = re.sub(rf"({CURRENCY_TOKEN})\s+(\d)", r"\1\2", s, flags=re.I)

    # stitch cases where currency lands between thousand groups: "8 $ 500" => "$8 500"
    s = re.sub(
        rf"(\d{{1,3}})\s*({CURRENCY_TOKEN})\s*(\d{{3}})\b",
        r"\2\1 \3",
        s,
        flags=re.I,
    )

    return s


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
        {
            "ts": ts,
            "country": country,
            "country_code": code,
            "url": url,
            "reason": reason,
        }
    )


# ================= DOM parsing =================

# --- prefer recurring monthly price over intro/trial price (multi-language) ---

MONTHLY_POS_RE = re.compile(
    r"(?i)("
    r"/\s*month\b|per\s+month\b|monthly\b|/\s*mo\b|/\s*mth\b|"
    # Spanish / Portuguese
    r"\bal\s+m[eéê]s\b|\bpor\s+m[eéê]s\b|/m[eéê]s\b|\bmensual(?:es)?\b|"
    # French
    r"\bpar\s+mois\b|/mois\b|\bmensuel(?:le|s)?\b|"
    # German
    r"\bpro\s+monat\b|/monat\b|\bmonatlich\b|"
    # Italian
    r"\bal\s+mese\b|/mese\b|\bmensile\b|"
    # Dutch
    r"\bper\s+maand\b|/maand\b|\bmaandelijks\b|"
    # East Asia
    r"/月|毎月|每月|月額|/월|매월|"
    # SE Asia / Turkish / Thai
    r"/(?:bulan|ay|เดือน)\b|per\s+bulan\b|ayl[ıi]k\b|ต่อเดือน\b"
    r")"
)

THEN_POS_RE = re.compile(
    r"(?i)\b("
    r"then|thereafter|after|"
    r"luego|despu[eé]s|a\s+partir\s+de|"
    r"ensuite|puis|apr[eè]s|"
    r"danach|"
    r"之后|以後|その後|以降|"
    r"이후|다음|"
    r"depois|ap[oó]s"
    r")\b"
)

INTRO_NEG_RE = re.compile(
    r"(?i)\b("
    r"try|trial|free|offer|limited|intro|new\s+subscribers?|"
    r"\bfor\s+\d+\s+months?\b|"
    r"\b\d+\s+mes(?:es)?\b|\bpor\s+\d+\s+mes(?:es)?\b|"  # es/pt: "3 meses", "por 3 meses"
    r"\b\d+\s+mois\b|"
    r"\b\d+\s+monate?\b|"
    r"\b\d+\s+(?:个月|か月)\b|"
    r"\b\d+\s+개월\b"
    r")\b"
)


def pick_best_price_token(text: str):
    """
    Select the most likely *recurring monthly* price token from a block of text.

    Returns: (token_str, numeric_value) or None if we only see intro/promo prices.
    """
    if not text:
        return None

    s = normalize_for_price_extraction(text)
    matches = list(BANNER_PRICE_REGEX.finditer(s))
    if not matches:
        return None

    any_intro = False
    candidates = []

    for m in matches:
        tok = m.group(0)
        num = extract_amount_number(tok)
        if not num:
            continue
        try:
            val = float(num)
        except Exception:
            continue

        start, end = m.span()
        # local window around this price
        context = s[max(0, start - 60) : min(len(s), end + 60)]

        has_month = bool(MONTHLY_POS_RE.search(context))
        has_intro = bool(INTRO_NEG_RE.search(context))
        has_then = bool(THEN_POS_RE.search(context))

        any_intro = any_intro or has_intro

        candidates.append(
            {
                "token": tok,
                "value": val,
                "start": start,
                "has_month": has_month,
                "has_intro": has_intro,
                "has_then": has_then,
            }
        )

    if not candidates:
        return None

    # 1) Strong preference: tokens whose *local* context looks monthly
    monthly = [c for c in candidates if c["has_month"]]
    if monthly:
        def score(c):
            s = 0
            s += 10  # monthly context
            if c["has_then"]:
                s += 5  # "...then X/month" is extra good
            if c["has_intro"]:
                s -= 4  # discounted if the same window mentions "3 months", "offer", etc.
            if c["value"] < 1:
                s -= 8  # tiny values like 0.90 are almost always promo
            return (s, c["value"])

        best = max(monthly, key=score)
        return best["token"], best["value"]

    # 2) No explicit '/month' near the prices: use "then/after" if present
    thens = list(THEN_POS_RE.finditer(s))
    if thens:
        pivot = thens[-1].start()
        after = [c for c in candidates if c["start"] > pivot]
        if after:
            best = max(
                after,
                key=lambda c: (0 if not c["has_intro"] else -5, c["value"]),
            )
            return best["token"], best["value"]

    # 3) If everything looks like promo ("3 months for ..."), don't return anything
    if any_intro:
        return None

    # 4) Last-ditch: whatever biggest price we saw
    best = max(candidates, key=lambda c: c["value"])
    return best["token"], best["value"]


def candidate_price_nodes(card: Tag):
    nodes = list(
        card.select(
            "p.plan-type.cost, p.tile-headline, "
            "[class*=cost], [class*=price], [class*=headline], [class*=subhead]"
        )
    )
    for el in card.find_all(True):
        txt = (el.get("aria-label") or el.get_text(" ", strip=True) or "")
        if txt and re.search(
            rf"{CURRENCY_CHARS}|RMB|CN¥|Rp|SAR|QAR|AED|KWD|BHD|OMR|RM|HK\$|NT\$|US\$",
            txt,
        ):
            nodes.append(el)
    seen, out = set(), []
    for n in nodes:
        if id(n) not in seen:
            seen.add(id(n))
            out.append(n)
    return out


# Semantic plan keyword matching (helps JP + markup changes)
PLAN_RE = {
    "Student": re.compile(
        r"(?i)\b(student|estudiante|étudiant|etudiant|schüler|schueler|студент|학생|sinh\s+viên)\b|学生|學生"
    ),
    "Individual": re.compile(
        r"(?i)\b(individual|personal|individuale|individuel|individuell|individuo)\b|個人|个人|개인"
    ),
    "Family": re.compile(
        r"(?i)\b(family|familia|famille|familie|famiglia|fam[íi]lia)\b|家族|家庭|가족"
    ),
}


def extract_plan_entries_semantic(section: Tag, alpha2: str):
    """
    Fallback when Apple changes card structure:
    - locate plan-name nodes (multi-language)
    - climb ancestor containers to find the nearest recurring price token
    """
    entries = {}
    if not section:
        return entries

    candidates = []
    for el in section.find_all(["h1", "h2", "h3", "h4", "p", "span", "div", "li"]):
        txt = _clean(el.get_text(" ", strip=True))
        if not txt or len(txt) > 140:
            continue
        for plan, rx in PLAN_RE.items():
            if rx.search(txt):
                candidates.append((plan, el))
                break

    def ancestor_blocks(el: Tag, max_up=6):
        cur = el
        for _ in range(max_up):
            if not cur or not isinstance(cur, Tag):
                break
            yield cur
            cur = cur.parent if hasattr(cur, "parent") else None

    for plan, el in candidates:
        best = None
        for block in ancestor_blocks(el, max_up=7):
            block_text = " ".join(block.stripped_strings)
            token_val = pick_best_price_token(block_text)
            if token_val:
                # prefer smaller blocks (closer ancestor) by breaking early
                best = (block_text, token_val)
                break
        if not best:
            continue

        _, (tok, val) = best
        iso, src, raw_cur = detect_currency_from_display(tok, alpha2)
        if src in {"ambiguous_symbol->default", "territory_default"}:
            iso2, src2 = detect_currency_in_text(best[0], alpha2)
            if src2 in {"symbol", "code"} and iso2:
                iso, src = iso2, f"context-{src2}"
            iso_res, why = resolve_dollar_ambiguity(
                iso, raw_cur, val, alpha2, best[0]
            )
            if why:
                iso, src = iso_res, f"heuristic-{why}"

        if plan not in entries:
            entries[plan] = {
                "Currency": iso,
                "Currency Source": src,
                "Currency Raw": raw_cur,
                "Price Display": _clean(tok),
                "Price Value": val,
            }

    return entries


def extract_plan_entries_from_dom_apple(soup: BeautifulSoup, alpha2: str):
    section = (
        soup.find("section", attrs={"data-analytics-name": re.compile("plans", re.I)})
        or soup.find("section", class_=re.compile("section-plans|plans", re.I))
        or soup
    )

    # Try known Apple-ish card containers
    cards = section.select(
        "div.plan-list-item, li.gallery-item, li[role='listitem'], "
        "div[class*='plan'], div[class*='pricing'], article"
    )

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
        head = card.select_one("h1, h2, h3, h4, p, span")
        if head:
            # try plan regex first
            ht = head.get_text(" ", strip=True)
            for p, rx in PLAN_RE.items():
                if rx.search(ht):
                    return p
            return standardize_plan(ht, 0)
        return "Individual"

    entries = {}
    if cards:
        for card in cards:
            std = classify(card)
            full_text = " ".join(card.stripped_strings)

            # Prefer recurring price from the full card text first
            token_val = pick_best_price_token(full_text)

            # If that fails, try richer aria/text nodes
            if not token_val:
                for el in candidate_price_nodes(card):
                    raw = (
                        el.get("aria-label")
                        or el.get_text(" ", strip=True)
                        or ""
                    )
                    token_val = pick_best_price_token(raw)
                    if token_val:
                        break

            if not token_val:
                continue

            chosen_tok, chosen_val = token_val

            iso, src, raw_cur = detect_currency_from_display(chosen_tok, alpha2)
            if src in {"ambiguous_symbol->default", "territory_default"}:
                iso2, src2 = detect_currency_in_text(full_text, alpha2)
                if src2 in {"symbol", "code"} and iso2:
                    iso, src = iso2, f"context-{src2}"
                iso_res, why = resolve_dollar_ambiguity(
                    iso, raw_cur, chosen_val, alpha2, full_text
                )
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

    # Semantic fallback (helps JP + structure changes)
    if len(entries) < 2:
        sem = extract_plan_entries_semantic(section, alpha2)
        for k, v in sem.items():
            entries.setdefault(k, v)

    return entries


def extract_plan_entries_from_dom_generic(soup: BeautifulSoup, alpha2: str):
    entries = {}
    plan_lists = soup.find_all(
        attrs={"class": re.compile(r"(plan|tier|pricing)", re.I)}
    ) or [soup]
    for container in plan_lists:
        cards = container.find_all(
            True, class_=re.compile(r"(plan|tier|card|pricing)", re.I)
        ) or [container]
        for idx, card in enumerate(cards):
            lab = (
                card.find("p", class_=re.compile("plan-type|name", re.I))
                or card.find(re.compile("h[1-6]"))
                or card
            )
            plan_name = _clean(lab.get_text()) if lab else f"Plan {idx+1}"
            std = standardize_plan(plan_name, idx)
            raw_text = " ".join(card.stripped_strings)

            token_val = pick_best_price_token(raw_text)
            if not token_val:
                for el in candidate_price_nodes(card):
                    txt = (
                        el.get("aria-label")
                        or el.get_text(" ", strip=True)
                        or ""
                    )
                    token_val = pick_best_price_token(txt)
                    if token_val:
                        break

            if not token_val:
                continue

            tok, val = token_val

            iso, src, raw_cur = detect_currency_from_display(tok, alpha2)
            if src in {"ambiguous_symbol->default", "territory_default"}:
                iso2, src2 = detect_currency_in_text(raw_text, alpha2)
                if src2 in {"symbol", "code"} and iso2:
                    iso, src = iso2, f"context-{src2}"
                iso_res, why = resolve_dollar_ambiguity(
                    iso, raw_cur, val, alpha2, raw_text
                )
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
CC_URL_RE = re.compile(
    rf"{APPLE_HOST_RE}/([a-z]{{2}})(?:/|-[a-z]{{2}}(?:-[a-z]{{2}})?/)", re.I
)
MUSIC_CC_URL_RE = re.compile(r"music\.apple\.com/([a-z]{{2}})/", re.I)


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
                resp = await page.goto(
                    url, wait_until="domcontentloaded", timeout=20000
                )
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
                t = await page.evaluate(
                    "document.body && document.body.innerText || ''"
                )
                if t and (
                    BANNER_PRICE_REGEX.search(t)
                    or STRICT_PRICE_NUMBER.search(t)
                ):
                    await browser.close()
                    return t, last
            except Exception:
                continue
        await browser.close()
        return "", last


def _extract_price_from_banner_text(text: str):
    # Use the same recurring-price selection here too
    if not text:
        return "", "", None
    tok_val = pick_best_price_token(text)
    if not tok_val:
        return "", "", None
    tok, val = tok_val
    return "", _clean(tok), val


def fetch_banner_individual_price(alpha2: str):
    """
    Returns recurring Individual price from music.apple.com banner, or None.
    Used to override suspicious 'Individual' prices scraped from Apple marketing pages.
    """
    with BANNER_SEMAPHORE:
        try:
            text, final_url = asyncio.run(_get_music_banner_text_async(alpha2))
        except RuntimeError:
            holder = {}

            def runner():
                holder["pair"] = asyncio.run(
                    _get_music_banner_text_async(alpha2)
                )

            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join()
            text, final_url = holder.get("pair", ("", ""))

    store_cc = _extract_cc(final_url)
    if not store_cc or store_cc != alpha2.upper():
        return None

    _, disp, val = _extract_price_from_banner_text(text)
    if val is None:
        return None

    iso, src, raw_cur = detect_currency_from_display(disp, alpha2)
    if src in {"ambiguous_symbol->default", "territory_default"}:
        iso2, src2 = detect_currency_in_text(text, alpha2)
        if iso2 and src2 in {"symbol", "code"}:
            iso, src = iso2, f"context-{src2}"
        iso_res, why = resolve_dollar_ambiguity(
            iso, raw_cur, val, alpha2, text
        )
        if why:
            iso, src = iso_res, f"heuristic-{why}"

    return {
        "Currency": iso,
        "Currency Source": src,
        "Currency Raw": raw_cur or "",
        "Price Display": _clean(disp),
        "Price Value": val,
    }


def banner_individual_row(alpha2: str, country_name: str, meta=None):
    with BANNER_SEMAPHORE:
        try:
            text, final_url = asyncio.run(_get_music_banner_text_async(alpha2))
        except RuntimeError:
            holder = {}

            def runner():
                holder["pair"] = asyncio.run(
                    _get_music_banner_text_async(alpha2)
                )

            t = threading.Thread(target=runner, daemon=True)
            t.start()
            t.join()
            text, final_url = holder.get("pair", ("", ""))

    store_cc = _extract_cc(final_url)
    if not store_cc or store_cc != alpha2.upper():
        log_missing(
            country_name,
            alpha2,
            final_url or f"https://music.apple.com/{alpha2.lower()}/new",
            f"music.apple.com storefront mismatch (requested={alpha2}, final={store_cc or 'NONE'})",
        )
        return []

    cur_sym, disp, val = _extract_price_from_banner_text(text)
    if val is None:
        return []

    iso, src, raw = detect_currency_from_display(disp or cur_sym, alpha2)
    raw = raw or cur_sym or ""

    if src in {"ambiguous_symbol->default", "territory_default"}:
        iso2, src2 = detect_currency_in_text(text, alpha2)
        if iso2 and src2 in {"symbol", "code"}:
            iso, src = iso2, f"context-{src2}"
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
            meta.get(
                "Apple URL",
                final_url or f"https://music.apple.com/{alpha2.lower()}/new",
            )
            if meta
            else (final_url or f"https://music.apple.com/{alpha2.lower()}/new")
        ),
        "Has Apple Music Page": meta.get("Has Apple Music Page", True)
        if meta
        else True,
    }
    return [row]


# ================= Redirect detection =================


def looks_like_us_hub_url(url: str) -> bool:
    if not url:
        return False
    u = urlparse(url)
    return (
        u.netloc.endswith("apple.com") and u.path.rstrip("/") == "/apple-music"
    )


def looks_like_us_hub_html(soup: BeautifulSoup) -> bool:
    can = soup.find("link", rel=re.compile("canonical", re.I))
    if can and can.get("href") and looks_like_us_hub_url(can.get("href")):
        return True
    og = soup.find("meta", property="og:url")
    if og and og.get("content") and looks_like_us_hub_url(og.get("content")):
        return True
    return False


def looks_like_us_content(soup: BeautifulSoup) -> bool:
    text = soup.get_text(" ", strip=True)
    t = text.lower()
    price_hit = re.search(r"\$ ?10\.99|\$ ?5\.99|\$ ?16\.99", text)
    copy_hit = ("try 1 month free" in t) or (
        "no commitment" in t and "cancel anytime" in t
    )
    return bool(price_hit and copy_hit)


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


# ================= Main scrape =================


def scrape_country(alpha2: str):
    cc = alpha2.upper()
    base = APPLE_BASE_BY_CC.get(cc, "https://www.apple.com")
    paths = REGION_LOCALE_PATHS.get(cc, [cc.lower()])

    last_url = None
    had_apple_page = False

    for path in paths:
        url = (
            f"{base}/apple-music/"
            if path == ""
            else f"{base}/{path}/apple-music/"
        )
        last_url = url
        try:
            resp = SESSION.get(url, timeout=15, allow_redirects=True)

            if (
                resp.status_code == 200
                and "apple.com" in urlparse(resp.url).netloc
            ):
                had_apple_page = True

            # (A) final URL is US hub (skip for US)
            if cc != "US" and looks_like_us_hub_url(resp.url):
                cn = normalize_country_name(
                    get_country_name_from_code(cc)
                )
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": "US hub",
                        "Redirect Reason": "Final URL is US hub",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )

            # (B) redirected to different storefront
            final_cc = _extract_cc(resp.url)
            if final_cc and not _storefront_equivalent(cc, final_cc):
                cn = normalize_country_name(
                    get_country_name_from_code(cc)
                )
                return banner_individual_row(
                    cc,
                    cn,
                    meta={
                        "Redirected": True,
                        "Redirected To": final_cc,
                        "Redirect Reason": f"HTTP redirect to {final_cc}",
                        "Apple URL": resp.url,
                        "Has Apple Music Page": False,
                    },
                )

            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # (C) canonical/OG = US hub (skip for US)
            if cc != "US" and looks_like_us_hub_html(soup):
                cn = normalize_country_name(
                    get_country_name_from_code(cc)
                )
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

            country_name = normalize_country_name(
                get_country_name_from_code(cc)
            )
            country_name = MANUAL_COUNTRY_FIXES.get(
                country_name, country_name
            )
            code = (get_country_code(country_name) or cc).upper()

            entries = extract_plan_entries_from_dom(soup, code)

            # If Individual looks like promo/footnote garbage, override with music banner recurring price
            if entries and "Individual" in entries:
                ind_val = entries["Individual"].get("Price Value")
                stud_val = entries.get("Student", {}).get("Price Value")

                suspicious = False
                if stud_val is not None and ind_val is not None and ind_val < stud_val:
                    suspicious = True
                if ind_val is not None and ind_val < 10:
                    suspicious = True

                if suspicious:
                    banner = fetch_banner_individual_price(code)
                    if banner:
                        entries["Individual"].update(banner)

            # (D) No plan entries: check for US content signature and flag (skip for US)
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
                            "Source": "apple.com.cn page"
                            if base.endswith(".cn")
                            else "apple.com page",
                            "Redirected": False,
                            "Redirected To": "",
                            "Redirect Reason": "",
                            "Apple URL": resp.url,
                            "Has Apple Music Page": True,
                        }
                    )
            if rows:
                return rows

            # Fallback banner if parse failed but page exists
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

    cn = normalize_country_name(get_country_name_from_code(cc))
    return banner_individual_row(
        cc,
        cn,
        meta={
            "Redirected": False,
            "Redirected To": "",
            "Redirect Reason": "No country-specific Apple Music page; banner-only",
            "Apple URL": last_url or "",
            "Has Apple Music Page": had_apple_page,
        },
    )


# ================= Runner =================


def run_scraper(country_codes_override=None):
    init_missing_db()

    iso_codes = {c.alpha_2 for c in pycountry.countries}
    all_codes = sorted(iso_codes.union(EXTRA_REGIONS))

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
        all_codes = sorted(
            {
                c.strip().upper()
                for c in TEST_COUNTRIES
                if c and len(c.strip()) == 2
            }
        )
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
                cn = normalize_country_name(
                    get_country_name_from_code(cc)
                )
                log_missing(
                    cn,
                    cc,
                    f"https://www.apple.com/{cc.lower()}/apple-music/",
                    f"Future exception: {type(e).__name__}: {e}",
                )

    if failed_codes:
        print(
            f"🔁 Retrying {len(failed_codes)} failed countries sequentially…"
        )
        for cc in failed_codes:
            try:
                res = scrape_country(cc)
                if res:
                    all_rows.extend(res)
                    MISSING_BUFFER[:] = [
                        m
                        for m in MISSING_BUFFER
                        if m.get("country_code") != cc
                    ]
            except Exception as e:
                cn = normalize_country_name(
                    get_country_name_from_code(cc)
                )
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
    df.sort_values(
        ["Country", "Plan"], inplace=True, ignore_index=True
    )

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

    out_name = (
        "apple_music_plans_TEST.xlsx"
        if TEST_MODE
        else "apple_music_plans_all.xlsx"
    )
    df.to_excel(out_name, index=False)
    print(f"✅ Exported to {out_name} (rows={len(df)})")

    if MISSING_BUFFER:
        pd.DataFrame(MISSING_BUFFER).to_csv(MISSING_CSV, index=False)
        print(
            f"⚠️ Logged {len(MISSING_BUFFER)} issues to {MISSING_CSV} / {MISSING_DB}"
        )

    return out_name


def run_apple_music_scraper(test_mode: bool = True, test_countries=None) -> str:
    """
    Wrapper used by the web app.

    test_mode = True  -> test run
    test_mode = False -> full run
    test_countries    -> optional list of ISO alpha-2 codes
    """
    global TEST_MODE, TEST_COUNTRIES

    TEST_MODE = bool(test_mode)

    if TEST_MODE and test_countries:
        TEST_COUNTRIES = [
            c.strip().upper()
            for c in test_countries
            if c and len(c.strip()) == 2
        ]
        print(f"[APPLE MUSIC] UI-driven test countries: {TEST_COUNTRIES}")

    start = time.time()
    out = run_scraper()
    print(f"[APPLE MUSIC] Finished in {round(time.time() - start, 2)}s")

    return (
        out
        or (
            "apple_music_plans_TEST.xlsx"
            if TEST_MODE
            else "apple_music_plans_all.xlsx"
        )
    )

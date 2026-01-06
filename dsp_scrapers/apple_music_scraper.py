# apple_music_plans_robust.py
# -----------------------------
# Robust Apple Music pricing scraper:
# - Handles multiple offer layouts (grid, hero, and a11y-only)
# - Normalises "price display" vs numeric "price value"
# - Attempts to capture extra metadata (offer type, terms, etc.)
# - Designed to be resilient to minor layout changes.

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
from pathlib import Path  # put near the top of th

import requests
from bs4 import BeautifulSoup, Tag, NavigableString
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
from functools import lru_cache

# Always write outputs to a fixed base directory (repo root: one level above dsp_scrapers)
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR  # or BASE_DIR / "outputs" if you prefer a subfolder

# =========================== Config ===========================

APPLE_MUSIC_BASE_URL = "https://www.apple.com/apple-music/"

# The country list is intentionally minimal here; in production you might pull
# from a canonical list. The app passes explicit country codes during test runs.
COUNTRIES = {
    # code: (storefront, currency_hint)
    "US": ("us", "USD"),
    "GB": ("uk", "GBP"),
    "FR": ("fr", "EUR"),
    "DE": ("de", "EUR"),
    "BR": ("br", "BRL"),
    "IN": ("in", "INR"),
    "JP": ("jp", "JPY"),
    "TR": ("tr", "TRY"),
    "HU": ("hu", "HUF"),
    "ID": ("id", "IDR"),
    "KW": ("kw", "KWD"),
    # ... add more as needed
}

# Plan tiers in desired output order
TIER_ORDER = [
    "Student",
    "Individual",
    "Family",
    "Voice",
    "Individual (Sim Only)",  # example of a more niche tier
]

# Where to log cases where we can't confidently find the recurring price
MISSING_DB = OUTPUT_DIR / "apple_music_missing.sqlite"
MISSING_CSV = OUTPUT_DIR / "apple_music_missing.csv"

# Global toggle used by run_apple_music_scraper() wrapper
TEST_MODE = True
TEST_COUNTRIES: List[str] = []

# =========================== HTTP Session ===========================


def get_retrying_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # Apple-specific headers to avoid locale surprises
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/15.1 Safari/605.1.15"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


SESSION = get_retrying_session()

# =========================== Helpers ===========================


def debug(msg: str) -> None:
    """Lightweight debug print that can be toggled or redirected later."""
    print(msg, file=sys.stderr)


@dataclass
class PriceCandidate:
    """
    Represents a potential "price" token found in text or attributes.

    kind: short label describing where we found it
    raw: original string
    value: parsed Decimal if we could parse it, else None
    currency: guessed currency symbol or code if detectable
    """

    kind: str
    raw: str
    value: Optional[Decimal]
    currency: Optional[str]
    extra: Dict[str, Any]

    def score(self) -> float:
        """
        Heuristic scoring function used to rank candidates.

        We heavily prefer:
        - Values above a tiny threshold (to avoid "free" or "0.00")
        - "per month" or subscription context
        - Non-trial / non-free labels
        """
        if self.value is None:
            base = 0.0
        else:
            base = float(self.value)

        # Very cheap values (<1 in major units) are usually promos
        # or trial footnotes in many markets.
        if self.value is not None and self.value < Decimal("1"):
            base *= 0.3

        score = base

        text = (self.raw or "").lower()

        # Penalise "free" / "trial" language.
        if any(x in text for x in ["free", "trial", "3 month", "one month"]):
            score *= 0.5

        # Boost "per month" / "a month" / "/month".
        if any(
            x in text
            for x in [
                "/month",
                "per month",
                "a month",
                "a mês",
                "por mês",
                "par mois",
                "im monat",
            ]
        ):
            score *= 1.4

        # Small boost if we have an explicit currency
        if self.currency:
            score *= 1.1

        # Add minor adjustment based on heuristics in `extra`
        if self.extra.get("emphasis"):
            score *= 1.1

        if self.extra.get("is_striked"):
            # cross-out = probably old or promotional price
            score *= 0.7

        return score


CURRENCY_SYMBOLS = {
    "$": "USD",
    "£": "GBP",
    "€": "EUR",
    "¥": "JPY",
    "₩": "KRW",
    "₹": "INR",
    "₺": "TRY",
    "₫": "VND",
    "₱": "PHP",
    "₫": "VND",
    "₴": "UAH",
    "₦": "NGN",
    "₲": "PYG",
    "₵": "GHS",
    "R$": "BRL",
    "₽": "RUB",
    "₡": "CRC",
    "₸": "KZT",
    "₪": "ILS",
    "₨": "PKR",  # ambiguous but fine as hint
    "zł": "PLN",
}


def normalise_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_decimal_from_text(text: str) -> Optional[Decimal]:
    """
    Extract a decimal number from text with best-effort handling
    of commas vs dots.

    Examples:
    - "R$ 10,90" -> 10.90
    - "£4.99 / month" -> 4.99
    """
    if not text:
        return None

    # Keep digits, separators, etc.
    cleaned = re.sub(r"[^0-9,.\-]", "", text)
    if not cleaned:
        return None

    # If we have both comma and dot, assume comma is thousands for most markets.
    # e.g. "1,299.00" -> "1299.00".
    if "," in cleaned and "." in cleaned:
        # count occurs to guess.
        # If comma appears before dot, likely a thousands separator.
        if cleaned.index(",") < cleaned.index("."):
            cleaned = cleaned.replace(",", "")

    # If we only have comma, treat it as decimal separator.
    elif "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def guess_currency(text: str, fallback: Optional[str] = None) -> Optional[str]:
    if not text:
        return fallback
    for symbol, code in CURRENCY_SYMBOLS.items():
        if symbol in text:
            return code
    return fallback


def pick_recurring_price_token(tokens: List[PriceCandidate]) -> Optional[PriceCandidate]:
    """
    Given a list of price candidates, attempt to choose the best one that
    represents the "standard recurring monthly price".
    """
    if not tokens:
        return None

    # Filter out obviously bogus / ultra-small / 0 tokens
    filtered = []
    for t in tokens:
        if t.value is None:
            continue
        if t.value <= Decimal("0"):
            continue
        filtered.append(t)

    if not filtered:
        return None

    # Score them with our heuristic.
    scored = sorted(filtered, key=lambda t: t.score(), reverse=True)
    best = scored[0]

    # If there is a second candidate very close in score, we might
    # keep both for debugging, but for now we just return best.
    return best


def ensure_missing_db_schema(path: str | Path) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS missing_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT,
                country_name TEXT,
                plan_name TEXT,
                context TEXT,
                snippet TEXT,
                url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def log_missing_price(
    country_code: str,
    country_name: str,
    plan_name: str,
    context: str,
    snippet: str,
    url: str,
) -> None:
    ensure_missing_db_schema(MISSING_DB)
    conn = sqlite3.connect(MISSING_DB)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO missing_prices (country_code, country_name, plan_name, context, snippet, url)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (country_code, country_name, plan_name, context, snippet[:500], url),
        )
        conn.commit()
    finally:
        conn.close()


# Also keep an in-memory list so we can export a CSV at the end.
MISSING_BUFFER: List[Dict[str, Any]] = []


def buffer_missing_price(
    country_code: str,
    country_name: str,
    plan_name: str,
    context: str,
    snippet: str,
    url: str,
) -> None:
    MISSING_BUFFER.append(
        dict(
            country_code=country_code,
            country_name=country_name,
            plan_name=plan_name,
            context=context,
            snippet=snippet,
            url=url,
        )
    )
    log_missing_price(
        country_code=country_code,
        country_name=country_name,
        plan_name=plan_name,
        context=context,
        snippet=snippet,
        url=url,
    )


# =========================== Scraping ===========================


def build_country_url(country_code: str) -> str:
    cc = country_code.upper()
    storefront, _ = COUNTRIES.get(cc, (cc.lower(), None))
    if storefront == "us":
        # Apple uses /apple-music/ for US root, no /us/ prefix.
        return APPLE_MUSIC_BASE_URL
    return f"https://www.apple.com/{storefront}/apple-music/"


@lru_cache(maxsize=128)
def fetch_country_page(country_code: str) -> BeautifulSoup:
    url = build_country_url(country_code)
    debug(f"[HTTP] GET {url}")
    resp = SESSION.get(url, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_json_ld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            # Apple sometimes concatenates multiple JSON-LD objects without a list.
            text = tag.string
            if not text:
                continue
            text = text.strip()
            if not text:
                continue

            # Attempt to parse as either list or single object.
            obj = json.loads(text)
            if isinstance(obj, dict):
                data.append(obj)
            elif isinstance(obj, list):
                data.extend(o for o in obj if isinstance(o, dict))
        except Exception as e:
            debug(f"[WARN] Failed to parse JSON-LD: {e}")
    return data


def find_offers_in_json_ld(ld_blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    offers: List[Dict[str, Any]] = []
    for block in ld_blocks:
        # Look for @type = Offer or Product with offers field.
        if block.get("@type") == "Offer":
            offers.append(block)
        if block.get("@type") == "Product":
            off = block.get("offers")
            if isinstance(off, list):
                offers.extend(o for o in off if isinstance(o, dict))
            elif isinstance(off, dict):
                offers.append(off)
    return offers


def extract_price_candidate_from_offer(
    offer: Dict[str, Any],
    fallback_currency: Optional[str],
) -> Optional[PriceCandidate]:
    price = offer.get("price")
    price_currency = offer.get("priceCurrency", fallback_currency)
    desc_parts = [
        offer.get("name"),
        offer.get("description"),
    ]
    desc = " ".join(p for p in desc_parts if p)
    if not price:
        return None

    value = None
    try:
        value = Decimal(str(price))
    except InvalidOperation:
        value = parse_decimal_from_text(str(price))

    raw_display = str(price)
    if desc:
        raw_display = f"{raw_display} ({desc})"

    return PriceCandidate(
        kind="json_ld_offer",
        raw=raw_display,
        value=value,
        currency=price_currency,
        extra={"source": "json-ld"},
    )


def extract_price_candidates_from_text(
    container: Tag,
    fallback_currency: Optional[str],
) -> List[PriceCandidate]:
    """
    Walk text and some attributes in a container looking for price-like tokens.
    """
    candidates: List[PriceCandidate] = []

    # Look at visible text nodes.
    for descendant in container.descendants:
        if isinstance(descendant, NavigableString):
            text = normalise_whitespace(str(descendant))
            if not text:
                continue
            if re.search(r"\d", text) and any(
                s in text for s in ["$", "€", "£", "¥", "₹", "₺", "₩", "R$", "zł", "₱", "₫"]
            ):
                val = parse_decimal_from_text(text)
                cur = guess_currency(text, fallback_currency)
                # Additional heuristics: emphasised text is often the main price.
                parent = descendant.parent
                extra = {
                    "emphasis": parent.name in {"strong", "b"} or "emphasis" in (parent.get("class") or []),
                    "is_striked": parent.name == "s" or "strikethrough" in (parent.get("class") or []),
                }
                candidates.append(
                    PriceCandidate(
                        kind="text",
                        raw=text,
                        value=val,
                        currency=cur,
                        extra=extra,
                    )
                )

    # Look at aria-label / data-* attributes on some elements.
    for elem in container.find_all(True):
        for attr in ["aria-label", "data-price", "data-ac-price", "data-price-display"]:
            if attr in elem.attrs:
                text = normalise_whitespace(elem.attrs.get(attr) or "")
                if not text:
                    continue
                if re.search(r"\d", text):
                    val = parse_decimal_from_text(text)
                    cur = guess_currency(text, fallback_currency)
                    candidates.append(
                        PriceCandidate(
                            kind=f"attr:{attr}",
                            raw=text,
                            value=val,
                            currency=cur,
                            extra={"source": "attribute"},
                        )
                    )

    return candidates


def guess_country_currency(country_code: str) -> Optional[str]:
    _, hint = COUNTRIES.get(country_code.upper(), (None, None))
    return hint


def parse_hero_banner_price(
    soup: BeautifulSoup,
    country_code: str,
) -> Dict[str, Any]:
    """
    Some Apple Music pages use a hero banner with localized text like:
      "Individual • $10.99/month after free trial"

    We attempt to extract at least one "Individual" tier from this region.
    """
    hero = soup.find("section", {"class": re.compile(r"hero|banner", re.I)})
    result: Dict[str, Any] = {}
    if not hero:
        return result

    text = normalise_whitespace(hero.get_text(" "))
    if not text:
        return result

    fallback_currency = guess_country_currency(country_code)

    # Very rough pattern; we only keep if we see both price and per month.
    if "month" not in text.lower():
        return result

    # search for something like "R$ 21,90" or "$10.99"
    m = re.search(
        r"(?P<price>(?:R\$|€|£|\$|¥|₹|₺)?\s*[\d.,]+)",
        text,
    )
    if not m:
        return result

    raw = m.group("price")
    value = parse_decimal_from_text(raw)
    currency = guess_currency(raw, fallback_currency)
    result["Individual"] = PriceCandidate(
        kind="hero_text",
        raw=raw + " (hero banner)",
        value=value,
        currency=currency,
        extra={"source": "hero"},
    )
    return result


def parse_plan_grid(
    soup: BeautifulSoup,
    country_code: str,
    country_name: str,
) -> List[Dict[str, Any]]:
    """
    Parse the main price grid if present. Apple often uses <section> with
    "pricing-table" or similar classes.
    """
    sections = soup.find_all(
        ["section", "div"],
        {"class": re.compile(r"pricing|plans|tiers|grid", re.I)},
    )

    results: List[Dict[str, Any]] = []

    fallback_currency = guess_country_currency(country_code)

    for sec in sections:
        # Each plan is often a card-like element.
        # We search for headings that contain known tier names.
        headings = sec.find_all(["h2", "h3", "h4"])
        for h in headings:
            h_text = normalise_whitespace(h.get_text(" "))
            if not h_text:
                continue

            plan_name = None
            for tier in TIER_ORDER:
                if tier.lower() in h_text.lower():
                    plan_name = tier
                    break
            if not plan_name:
                # If we don't recognise the plan name but we see "student"
                # or "family", normalise to those anyway.
                if "student" in h_text.lower():
                    plan_name = "Student"
                elif "family" in h_text.lower():
                    plan_name = "Family"
                elif "voice" in h_text.lower():
                    plan_name = "Voice"
                elif "individual" in h_text.lower():
                    plan_name = "Individual"

            if not plan_name:
                continue

            # The price is typically somewhere near this heading.
            container = h.parent
            if not isinstance(container, Tag):
                continue

            candidates = extract_price_candidates_from_text(container, fallback_currency)

            # Additional fallback: sometimes price is slightly further down.
            if not candidates:
                sibling = container.find_next_sibling()
                if sibling:
                    candidates = extract_price_candidates_from_text(
                        sibling, fallback_currency
                    )

            best = pick_recurring_price_token(candidates)

            if not best:
                # Still store something so we know it existed, but log it.
                snippet = normalise_whitespace(container.get_text(" "))
                buffer_missing_price(
                    country_code=country_code,
                    country_name=country_name,
                    plan_name=plan_name,
                    context="plan_grid",
                    snippet=snippet[:300],
                    url=build_country_url(country_code),
                )
                results.append(
                    dict(
                        Country=country_name,
                        CountryCode=country_code,
                        Currency=fallback_currency or "",
                        CurrencyRaw="",
                        Plan=plan_name,
                        PriceDisplay="",
                        PriceValue=None,
                        Notes="Unable to confidently parse price from plan grid",
                    )
                )
                continue

            results.append(
                dict(
                    Country=country_name,
                    CountryCode=country_code,
                    Currency=best.currency or (fallback_currency or ""),
                    CurrencyRaw=best.raw,
                    Plan=plan_name,
                    PriceDisplay=best.raw,
                    PriceValue=float(best.value) if best.value is not None else None,
                    Notes=best.kind,
                )
            )

    return results


def merge_with_hero_fallback(
    main_results: List[Dict[str, Any]],
    hero_map: Dict[str, PriceCandidate],
    country_code: str,
    country_name: str,
) -> List[Dict[str, Any]]:
    """
    For each standard tier, if we failed to find a price in the grid
    but the hero banner has one, use it.
    """
    by_plan = {(r["Plan"], r["CountryCode"]): r for r in main_results}

    for tier, candidate in hero_map.items():
        key = (tier, country_code)
        if key not in by_plan:
            # Add a new row using hero price
            main_results.append(
                dict(
                    Country=country_name,
                    CountryCode=country_code,
                    Currency=candidate.currency or "",
                    CurrencyRaw=candidate.raw,
                    Plan=tier,
                    PriceDisplay=candidate.raw,
                    PriceValue=float(candidate.value)
                    if candidate.value is not None
                    else None,
                    Notes=candidate.kind,
                )
            )
        else:
            row = by_plan[key]
            if not row.get("PriceValue") and candidate.value is not None:
                row["Currency"] = candidate.currency or row.get("Currency") or ""
                row["CurrencyRaw"] = candidate.raw
                row["PriceDisplay"] = candidate.raw
                row["PriceValue"] = float(candidate.value)
                row["Notes"] = f"{row.get('Notes','')}; hero_fallback".strip("; ")

    return main_results


def scrape_country(country_code: str) -> List[Dict[str, Any]]:
    """
    Full scrape pipeline for one country:
    - fetch and parse HTML
    - inspect JSON-LD offers (if any)
    - inspect pricing tables / grids
    - use hero banner as fallback
    """
    country_code = country_code.upper()
    storefront, currency_hint = COUNTRIES.get(country_code, (country_code.lower(), None))
    soup = fetch_country_page(country_code)
    url = build_country_url(country_code)

    # Apple often embeds the human-readable country name in <title> or meta.
    title = soup.title.string if soup.title else ""
    m = re.search(r"Apple Music\s*-\s*(.+)", title or "", re.I)
    if m:
        country_name = m.group(1).strip()
    else:
        # Fallback: use the country code.
        country_name = country_code

    debug(f"[SCRAPE] {country_code} ({country_name}) at {url}")

    # 1) JSON-LD offers (not always present, but very structured when they are).
    ld_blocks = extract_json_ld(soup)
    offers = find_offers_in_json_ld(ld_blocks)
    json_ld_candidates: Dict[str, PriceCandidate] = {}
    for off in offers:
        candidate = extract_price_candidate_from_offer(off, currency_hint)
        if not candidate or candidate.value is None:
            continue
        # Some offers might have name like "Apple Music Individual".
        name = off.get("name") or off.get("category") or ""
        norm_name = ""
        ln = name.lower()
        if "student" in ln:
            norm_name = "Student"
        elif "family" in ln:
            norm_name = "Family"
        elif "voice" in ln:
            norm_name = "Voice"
        elif "individual" in ln or "personal" in ln:
            norm_name = "Individual"

        if not norm_name:
            # If we can't tell, skip; we only want canonical tiers here.
            continue

        existing = json_ld_candidates.get(norm_name)
        if not existing or (candidate.value and existing.value and candidate.value < existing.value):
            # In JSON-LD, we somewhat prefer the cheaper one if multiple.
            json_ld_candidates[norm_name] = candidate

    # 2) Parse the main grid / pricing tables.
    grid_results = parse_plan_grid(soup, country_code, country_name)

    # 3) Hero banner fallback for "Individual" if missing or ambiguous.
    hero_map = parse_hero_banner_price(soup, country_code)

    # If JSON-LD gave us something strong, blend it into hero map (which is used as fallback).
    for tier, cand in json_ld_candidates.items():
        if tier not in hero_map:
            hero_map[tier] = cand

    merged = merge_with_hero_fallback(grid_results, hero_map, country_code, country_name)

    # Ensure we have *at least* Individual in some shape; if not, log a missing.
    has_individual = any(
        r for r in merged if r["Plan"] == "Individual" and r.get("PriceValue") is not None
    )
    if not has_individual:
        snippet = normalise_whitespace(soup.get_text(" "))
        buffer_missing_price(
            country_code=country_code,
            country_name=country_name,
            plan_name="Individual",
            context="no_individual_found",
            snippet=snippet[:300],
            url=url,
        )

    return merged


def run_scraper(
    country_codes_override: Optional[Iterable[str]] = None,
) -> str:
    """
    Entry point used both by CLI and the Streamlit integration.

    - country_codes_override: if provided, restricts scraping to these 2-letter ISO codes.
    - returns the absolute path to the Excel file created.
    """
    global MISSING_BUFFER
    MISSING_BUFFER = []

    if country_codes_override:
        to_scrape = [c.upper() for c in country_codes_override]
    else:
        to_scrape = sorted(COUNTRIES.keys())

    all_rows: List[Dict[str, Any]] = []
    for i, cc in enumerate(to_scrape, start=1):
        try:
            debug(f"[{i}/{len(to_scrape)}] Scraping {cc}")
            rows = scrape_country(cc)
            all_rows.extend(rows)
        except Exception as e:
            debug(f"[ERROR] Failed to scrape {cc}: {e}")
            buffer_missing_price(
                country_code=cc,
                country_name=cc,
                plan_name="ALL",
                context="country_failed",
                snippet=str(e),
                url=build_country_url(cc),
            )

    if not all_rows:
        raise RuntimeError("No rows scraped; aborting write step")

    df = pd.DataFrame(all_rows)
    df["Plan"] = pd.Categorical(df["Plan"], TIER_ORDER, ordered=True)
    df.sort_values(["Country", "Plan"], inplace=True, ignore_index=True)

    # Always write to a fixed location so CLI and Streamlit use the SAME file
    out_name = "apple_music_plans_TEST.xlsx" if TEST_MODE or country_codes_override else "apple_music_plans_all.xlsx"
    full_path = OUTPUT_DIR / out_name

    df.to_excel(full_path, index=False)
    print(f"✅ Exported to {full_path} (rows={len(df)})")

    if MISSING_BUFFER:
        pd.DataFrame(MISSING_BUFFER).to_csv(MISSING_CSV, index=False)
        print(f"⚠️ Logged {len(MISSING_BUFFER)} issues to {MISSING_CSV} / {MISSING_DB}")

    return str(full_path)


def run_apple_music_scraper(test_mode: bool = True, test_countries=None) -> str | None:
    """
    Entry point used by the Streamlit app.

    - In test_mode, honours `test_countries` by passing them into run_scraper.
    - In full mode, ignores `test_countries` and scrapes all countries.
    - Returns the absolute path to the Excel file, or None if nothing was written.
    """
    global TEST_MODE, TEST_COUNTRIES
    TEST_MODE = bool(test_mode)

    country_override = None
    if TEST_MODE and test_countries:
        TEST_COUNTRIES = [
            c.strip().upper()
            for c in test_countries
            if c and len(c.strip()) == 2
        ]
        country_override = TEST_COUNTRIES
        print(f"[APPLE MUSIC] UI-driven test countries: {TEST_COUNTRIES}")

    start = time.time()
    excel_path = run_scraper(country_codes_override=country_override)
    print(f"[APPLE MUSIC] Finished in {round(time.time() - start, 2)}s")

    return excel_path


if __name__ == "__main__":
    # Simple CLI for local testing:
    #   python apple_music_plans_robust.py US GB FR
    args = sys.argv[1:]
    if args:
        codes = [a.upper() for a in args]
        TEST_MODE = True
        TEST_COUNTRIES = codes
        path = run_scraper(country_codes_override=codes)
    else:
        TEST_MODE = False
        path = run_scraper()
    print(f"Output written to: {path}")

import asyncio
import re
import functools
import pandas as pd
from playwright.async_api import async_playwright
import pycountry
from difflib import get_close_matches
from tqdm.auto import tqdm
from datetime import date
from babel.numbers import get_territory_currencies
from googletrans import Translator

# ---------- Config ----------
STANDARD_PLAN_NAMES = [
    "Platinum",
    "Lite",
    "Individual",
    "Student",
    "Family",
    "Duo",
    "Audiobooks",
    "Basic",
    "Mini",
    "Standard",
]

MAX_CONCURRENCY = 3
HEADLESS = True

# ---- Test mode ----
TEST_MODE = False
TEST_MARKETS = ["kr"]

translator = Translator()

# ---------- Utilities ----------
def log(msg):
    print(msg, flush=True)

@functools.lru_cache(maxsize=2048)
def translate_text_cached(text: str) -> str:
    try:
        return translator.translate(text or "", dest="en").text.lower()
    except Exception:
        return (text or "").lower()

def _clean_spaces(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

def _normalize_number(p: str) -> str:
    """
    Normalize localized numbers:
      - "10,99" -> "10.99"
      - "1 299,00" -> "1299.00"
    """
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

def normalize_plan_name(name: str) -> str:
    raw = (name or "").strip().lower()

    # Manual overrides
    if re.search(r"\b(personal|personnel|staff)\b", raw):
        return "Individual"

    # 1) Direct substring
    for std in STANDARD_PLAN_NAMES:
        if std.lower() in raw:
            return std

    # 2) Translate then match
    translated = translate_text_cached(raw)
    for std in STANDARD_PLAN_NAMES:
        if std.lower() in translated:
            return std

    # 3) Token-based
    tokens = re.findall(r"[a-z]+", raw)
    for token in tokens:
        for std in STANDARD_PLAN_NAMES:
            if token == std.lower():
                return std

    # 4) Fuzzy fallback
    match = get_close_matches(
        translated,
        [n.lower() for n in STANDARD_PLAN_NAMES],
        n=1,
        cutoff=0.6,
    )
    if match:
        return match[0].capitalize()

    return "Other"

def default_currency_for_alpha2(alpha2: str) -> str:
    iso2 = (alpha2 or "").upper()
    try:
        currs = get_territory_currencies(iso2, date=date.today(), non_tender=False)
        if currs:
            return currs[0]
    except Exception:
        pass
    return ""

# -------------------------------------------------------------------
# PRICE PARSING (ROBUST)
# -------------------------------------------------------------------
# KEY CHANGE: include \b[A-Z]{3}\b so AED/SAR/QAR/etc work everywhere.
# -------------------------------------------------------------------

# Symbol-ish tokens (keep some common ones)
CURRENCY_SYMBOL_TOKEN = (
    r"(US\$|\$US|U\$S|€|£|¥|₹|₩|₫|₺|₪|₴|₼|₾|₭|฿|₦|₵|₱|Rp|R\$|S/\.|S/|RM|zł|Kč|Ft|lei|лв|"
    r"KSh|TSh|USh|HK\$|NT\$|S\$|A\$|NZ\$|RD\$|N\$)"
)

# Accept ISO codes too (AED/SAR/etc)
CURRENCY_TOKEN = rf"(?:{CURRENCY_SYMBOL_TOKEN}|\b[A-Z]{{3}}\b)"
NUMBER_TOKEN = r"(\d+(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?)"

PRICE_TOKEN_RE = re.compile(
    rf"(?:{CURRENCY_TOKEN}\s*{NUMBER_TOKEN}|{NUMBER_TOKEN}\s*{CURRENCY_TOKEN})"
)

# “After/then” signals across several languages (good enough coverage)
AFTERISH_RAW_RE = re.compile(
    r"(?i)\b("
    r"then|after|thereafter|"
    r"puis|ensuite|après|apres|"
    r"danach|nach|"
    r"despu[eé]s|luego|"
    r"poi|dopo|"
    r"depois|ap[oó]s"
    r")\b"
)

# “Monthly” signals across several languages
MONTHISH_RAW_RE = re.compile(
    r"(?i)("
    r"/\s*month|\bper\s+month\b|\bmonthly\b|"
    r"/\s*mo\b|"
    r"/\s*mois\b|\bmois\b|"
    r"pro\s+monat|/monat|monatlich|"
    r"/\s*mes\b|\bal\s+mes\b|"
    r"/\s*mese\b|\bal\s+mese\b|"
    r"/\s*m[eê]s\b"
    r")"
)

# “Trial” signals
TRIALISH_RAW_RE = re.compile(
    r"(?i)\b("
    r"free|trial|"
    r"for\s+\d+\s+month|for\s+one\s+month|"
    r"pour\s+\d+\s+mois|"
    r"f[uü]r\s+\d+\s+monat|"
    r"por\s+\d+\s+mes|"
    r"por\s+\d+\s+m[eê]s"
    r")\b"
)

def detect_currency_from_token(token: str, alpha2: str) -> str:
    """
    Prefer explicit ISO codes if present, otherwise use symbol mapping, otherwise territory.
    """
    t = _clean_spaces(token)

    m = re.search(r"\b([A-Z]{3})\b", t)
    if m:
        # if Spotify prints AED/SAR/etc, that's the currency
        return m.group(1).upper()

    # symbol mapping
    if "€" in t:
        return "EUR"
    if "£" in t:
        return "GBP"
    if "₹" in t:
        return "INR"
    if "₩" in t:
        return "KRW"
    if "Rp" in t:
        return "IDR"
    if "R$" in t:
        return "BRL"
    if "US$" in t or "$US" in t or "U$S" in t:
        return "USD"

    return default_currency_for_alpha2(alpha2)

def pick_best_after_line(text_block: str) -> str:
    """
    Find a line/snippet that looks like “then X/month” (in any language),
    and contains a PRICE token.
    """
    t = _clean_spaces(text_block or "")
    if not t:
        return ""

    # split into rough lines
    rough_lines = []
    for chunk in re.split(r"[\n\r]+", t):
        chunk = _clean_spaces(chunk)
        if chunk:
            rough_lines.append(chunk)

    # also split long lines by punctuation/bullets to isolate the “after” clause
    extra = []
    for ln in rough_lines:
        for part in re.split(r"[•|,;]+", ln):
            part = _clean_spaces(part)
            if part:
                extra.append(part)

    lines = rough_lines + extra

    # Best: after-ish + month-ish + price token
    for ln in lines:
        if AFTERISH_RAW_RE.search(ln) and MONTHISH_RAW_RE.search(ln) and PRICE_TOKEN_RE.search(ln):
            return ln

    # Next: after-ish + price token
    for ln in lines:
        if AFTERISH_RAW_RE.search(ln) and PRICE_TOKEN_RE.search(ln):
            return ln

    return ""

def pick_recurring_price_token(card_text: str) -> tuple[str, str]:
    """
    Picks recurring monthly price token from a SINGLE plan card.
    Strategy:
      1) If any token has AFTER-ish context, choose best among those.
      2) Else choose token with MONTH-ish and not TRIAL-ish.
      3) Else choose max numeric token.
    Returns: (price_display_token, normalized_amount_str)
    """
    text = _clean_spaces(card_text or "")
    if not text:
        return "", ""

    cands = []
    for m in PRICE_TOKEN_RE.finditer(text):
        token = _clean_spaces(m.group(0))

        nums = re.findall(r"\d+(?:[.,]\d+)?", token)
        if not nums:
            continue

        norm = _normalize_number(nums[0])
        if not norm:
            continue

        try:
            val = float(norm)
        except Exception:
            continue

        a, b = m.span()
        ctx = text[max(0, a - 90): min(len(text), b + 90)]
        ctx_en = translate_text_cached(ctx)

        afterish = bool(AFTERISH_RAW_RE.search(ctx)) or ("then" in ctx_en) or ("after" in ctx_en) or ("thereafter" in ctx_en)
        monthish = bool(MONTHISH_RAW_RE.search(ctx)) or ("/month" in ctx_en) or ("per month" in ctx_en) or ("monthly" in ctx_en)
        trialish = bool(TRIALISH_RAW_RE.search(ctx)) or ("trial" in ctx_en) or ("free" in ctx_en and "month" in ctx_en)

        score = val
        if trialish:
            score *= 0.15
        if monthish:
            score *= 1.6
        if afterish:
            score *= 1.8

        cands.append((afterish, monthish, trialish, score, val, m.start(), token, norm))

    if not cands:
        return "", ""

    after_pool = [c for c in cands if c[0]]
    pool = after_pool if after_pool else [c for c in cands if c[1] and not c[2]] or cands

    pool.sort(key=lambda x: (x[3], x[4], x[5]), reverse=True)
    best = pool[0]
    return best[6], best[7]

# ---------- Country info ----------
def get_country_info(locale_code):
    base = (locale_code or "").split("-")[0]
    try:
        c = pycountry.countries.lookup(base)
        return c.name, c.alpha_2, c.alpha_3
    except Exception:
        return "Unknown", base.upper(), base.upper()

# ---------- Playwright helpers ----------
async def new_context(playwright):
    browser = await playwright.chromium.launch(
        headless=HEADLESS,
        args=[
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-extensions",
        ],
    )
    ctx = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="UTC",
        ignore_https_errors=True,
    )

    async def route_block(route):
        if route.request.resource_type in {"image", "media", "font"}:
            await route.abort()
        else:
            await route.continue_()

    await ctx.route("**/*", route_block)
    await ctx.add_cookies([{"name": "sp_lang", "value": "en", "domain": ".spotify.com", "path": "/"}])
    return browser, ctx

async def safe_goto(page, url, timeout=60000):
    for i in range(3):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return True
        except Exception:
            if i == 2:
                return False
            await asyncio.sleep(1.0 + i * 0.6)
    return False

# ---------- Market discovery ----------
async def fetch_markets(playwright):
    browser, ctx = await new_context(playwright)
    page = await ctx.new_page()
    ok = await safe_goto(page, "https://www.spotify.com/select-your-country-region/", timeout=70000)
    result = []
    if ok:
        links = await page.eval_on_selector_all(
            "a[href^='/']:not([href*='help']):not([href='#'])",
            "els => els.map(a => a.getAttribute('href'))",
        )
        base_choice = {}
        for href in links or []:
            if not href:
                continue
            code = href.strip("/").split("/")[0]
            if not re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", code):
                continue
            base = code.split("-")[0]
            if base not in base_choice or code.endswith("-en"):
                base_choice[base] = code
        result = list(base_choice.values())
    try:
        await browser.close()
    except Exception:
        pass
    return result

# ---------- Robust plan-card discovery ----------
async def find_plan_cards(page):
    """
    Works across layout changes:
      - looks at headings: h1..h4 and [role=heading]
      - climbs ancestors until it finds a container that:
          - contains exactly 1 heading (same selector set)
          - contains a price token OR has a CTA element
      - keeps smallest container per (std plan, title)
    """
    heading_sel = "h1,h2,h3,h4,[role='heading']"
    headings = await page.query_selector_all(heading_sel)
    cards_by_key = {}

    js_find_container = r"""
    (node) => {
      const headingSel = "h1,h2,h3,h4,[role='heading']";
      const priceRe = /(?:\b[A-Z]{3}\b|US\$|\$US|U\$S|€|£|¥|₹|₩|Rp|R\$)\s*\d|\d\s*(?:\b[A-Z]{3}\b|US\$|\$US|U\$S|€|£|¥|₹|₩|Rp|R\$)/;
      let el = node;
      while (el && el !== document.body) {
        const hCount = el.querySelectorAll(headingSel).length;
        const t = (el.textContent || "");
        const hasPrice = priceRe.test(t);
        const hasCTA = !!el.querySelector("a,button");
        if (hCount === 1 && (hasPrice || hasCTA)) return el;
        el = el.parentElement;
      }
      return node.parentElement;
    }
    """

    for h in headings:
        try:
            title = _clean_spaces(await h.inner_text())
            if not title:
                continue

            std = normalize_plan_name(title)
            if std == "Other":
                continue

            title_key = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
            key = (std, title_key)

            handle = await h.evaluate_handle(js_find_container)
            el = handle.as_element()
            if el is None:
                continue

            text_len = await el.evaluate("el => (el.textContent || '').length")

            if key not in cards_by_key or text_len < cards_by_key[key][0]:
                cards_by_key[key] = (text_len, el, title, std)
        except Exception:
            continue

    out = []
    for _, el, title, std in cards_by_key.values():
        out.append((el, title, std))
    return out

# ---------- Scrape one market ----------
async def scrape_country(locale, playwright, semaphore):
    async with semaphore:
        browser, ctx = await new_context(playwright)
        page = await ctx.new_page()
        url = f"https://www.spotify.com/{locale}/premium/"

        cname, a2, a3 = get_country_info(locale)
        plans = []

        ok = await safe_goto(page, url, timeout=70000)
        if ok:
            await page.wait_for_timeout(1800)

            card_items = await find_plan_cards(page)

            for el, title, std in card_items:
                try:
                    # visible <p> lines (trial info)
                    p_tags = await el.query_selector_all("p")
                    p_texts = []
                    for p in p_tags:
                        try:
                            t = await p.inner_text()
                            if t:
                                p_texts.append(_clean_spaces(t))
                        except Exception:
                            pass

                    # include hidden/legal with textContent
                    try:
                        card_text = _clean_spaces(await el.evaluate("(x) => x.textContent || ''"))
                    except Exception:
                        card_text = " ".join(p_texts)

                    # Prefer after/then line if present
                    after_line = pick_best_after_line(card_text)

                    if after_line:
                        price_display, amount = pick_recurring_price_token(after_line)
                        price_after_trial = after_line
                    else:
                        price_display, amount = pick_recurring_price_token(card_text)
                        price_after_trial = ""

                    if not amount:
                        continue

                    currency = detect_currency_from_token(price_display, a2) or default_currency_for_alpha2(a2)
                    trial = p_texts[0] if p_texts else ""

                    plans.append(
                        {
                            "Country Code": locale,
                            "Country Name (resolved)": cname,
                            "Country Standard Name": cname,
                            "Alpha-2": a2,
                            "Alpha-3": a3,
                            "Plan Name": title,
                            "Standard Plan Name": std,
                            "Trial Info": trial,
                            "Currency": currency,
                            "Price": amount,  # recurring after-trial when available
                            "Billing Frequency": "month",
                            "Price After Trial": price_after_trial,
                            "URL": url,
                        }
                    )
                except Exception:
                    pass

        try:
            await browser.close()
        except Exception:
            pass
        return plans

# ---------- Master runner ----------
async def run():
    async with async_playwright() as pw:
        log("🔎 Discovering markets from directory…")
        markets = await fetch_markets(pw)
        if not markets:
            log("❌ Couldn’t resolve markets.")
            return

        if TEST_MODE:
            desired = set(TEST_MARKETS)
            desired_bases = {c.split("-")[0] for c in desired}
            picked = []
            for loc in markets:
                base = loc.split("-")[0]
                if base in desired_bases and (loc.endswith("-en") or base not in [p.split("-")[0] for p in picked]):
                    picked.append(loc)
            for code in TEST_MARKETS:
                if code not in picked and code.split("-")[0] not in [p.split("-")[0] for p in picked]:
                    picked.append(code)
            markets = picked
            log(f"🧪 Test mode: scraping {len(markets)} markets: {markets}")
        else:
            log(f"✅ Found {len(markets)} markets (English preferred where available).")

        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        tasks = [scrape_country(loc, pw, sem) for loc in markets]
        all_plans = []

        pbar = tqdm(total=len(tasks), desc="Scraping /premium pages", unit="market")
        for fut in asyncio.as_completed(tasks):
            res = await fut
            if res:
                all_plans.extend(res)
            pbar.update(1)
        pbar.close()

        if not all_plans:
            log("❌ No plan cards scraped.")
            return

        df = pd.DataFrame(all_plans)
        df["Numerical Price"] = pd.to_numeric(df["Price"], errors="coerce")
        df.sort_values(["Alpha-2", "Standard Plan Name", "Plan Name"], inplace=True, kind="stable")

        desired_columns = [
            "Country Standard Name",
            "Alpha-2",
            "Alpha-3",
            "Country Code",
            "Country Name (resolved)",
            "Standard Plan Name",
            "Plan Name",
            "Trial Info",
            "Currency",
            "Price",
            "Billing Frequency",
            "Price After Trial",
            "URL",
        ]
        df = df[desired_columns]

        df.rename(
            columns={
                "Alpha-2": "Country Alpha-2",
                "Alpha-3": "Country Alpha-3",
                "Country Name (resolved)": "Country Name",
            },
            inplace=True,
        )

        base = f"spotify_cleaned_playwright{'_TEST' if TEST_MODE else ''}"
        csv_out = f"{base}.csv"
        xlsx_out = f"{base}.xlsx"

        df.to_csv(csv_out, index=False, encoding="utf-8")
        with pd.ExcelWriter(xlsx_out, engine="openpyxl") as w:
            df.to_excel(w, index=False)

        log(
            f"\n🎉 Done! Saved {csv_out} and {xlsx_out} | "
            f"Rows: {len(df)} Countries: {df['Country Alpha-2'].nunique()}"
        )

        from pathlib import Path
        return str(Path(xlsx_out).resolve())

# ---------------------------------------------------------------------------
# Streamlit wrapper
# ---------------------------------------------------------------------------
async def _run_spotify_async(test_mode: bool = True, test_countries=None) -> str:
    global TEST_MODE, TEST_MARKETS
    TEST_MODE = bool(test_mode)

    if TEST_MODE and test_countries:
        TEST_MARKETS = [c.lower() for c in test_countries]
        print(f"[SPOTIFY] UI-driven TEST_MARKETS: {TEST_MARKETS}")

    return await run()

def run_spotify_scraper(test_mode: bool = True, test_countries=None) -> str:
    return asyncio.run(_run_spotify_async(test_mode=test_mode, test_countries=test_countries))

import asyncio
import re
import functools
import pandas as pd
from playwright.async_api import async_playwright
import pycountry
from difflib import get_close_matches
from googletrans import Translator
from tqdm.auto import tqdm
from datetime import date
from babel.numbers import get_territory_currencies

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

translator = Translator()
MAX_CONCURRENCY = 3
HEADLESS = True

# ---- Test mode ----
TEST_MODE = False
TEST_MARKETS = ["kr"]

# ---------- Utilities ----------
def log(msg):
    print(msg, flush=True)

@functools.lru_cache(maxsize=1024)
def translate_text_cached(text: str) -> str:
    try:
        return translator.translate(text or "", dest="en").text.lower()
    except Exception:
        return (text or "").lower()

def _clean_spaces(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

def normalize_plan_name(name: str) -> str:
    raw = (name or "").strip().lower()

    # Manual overrides
    if re.search(r"\b(personal|personnel|staff)\b", raw):
        return "Individual"

    # 1) Direct substring match
    for std in STANDARD_PLAN_NAMES:
        if std.lower() in raw:
            return std

    # 2) Try translated
    translated = translate_text_cached(raw)
    for std in STANDARD_PLAN_NAMES:
        if std.lower() in translated:
            return std

    # 3) Token-based exact
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

def is_generic_trial(text: str) -> bool:
    text = (text or "").strip()
    if not text:
        return False
    translated = translate_text_cached(text)
    promo = [
        "go premium", "cancel anytime", "no commitment", "no ads",
        "annulez à tout moment", "enjoy music", "try premium"
    ]
    return sum(p in translated for p in promo) > 1

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

# ---------- Currency ----------
# (keep your rich token support; the key fix is: we only choose prices that include these tokens)
CURRENCY_TOKEN = r"(US\$|\$US|U\$S|€|£|¥|₹|₩|₫|₺|₪|₴|₼|₾|₭|฿|₦|₵|₱|Rp|R\$|S/\.|S/|RM|zł|Kč|Ft|lei|лв|KSh|TSh|USh|HK\$|NT\$|S\$|A\$|NZ\$|RD\$|N\$)"
NUMBER_TOKEN = r"(\d+(?:[.,\s]\d{3})*(?:[.,]\d{1,2})?)"
PRICE_TOKEN_RE = re.compile(rf"(?:{CURRENCY_TOKEN}\s*{NUMBER_TOKEN}|{NUMBER_TOKEN}\s*{CURRENCY_TOKEN})")

MONTHY_RE = re.compile(r"(?:/ ?month|\bper month\b|\ba month\b|\bmonthly\b)", re.I)
AFTER_RE = re.compile(r"\b(after|thereafter|then)\b", re.I)
FOR_N_MONTHS_RE = re.compile(r"\bfor\s+\d+\s+month", re.I)

def looks_monthly_en(s_en: str) -> bool:
    return bool(MONTHY_RE.search(s_en))

def default_currency_for_alpha2(alpha2: str) -> str:
    iso2 = (alpha2 or "").upper()
    try:
        currs = get_territory_currencies(iso2, date=date.today(), non_tender=False)
        if currs:
            return currs[0]
    except Exception:
        pass
    return ""

def detect_currency_in_text(text: str, alpha2: str) -> str:
    """
    Very lightweight for output column.
    Since we now pick a token that *contains* a currency, we can infer currency from it.
    """
    t = _clean_spaces(text)
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
    # fallback territory
    return default_currency_for_alpha2(alpha2)

# ---------- NEW: robust recurring picker (currency-attached only) ----------
EN_TRIAL_RE = re.compile(r"(?i)\b(free|trial|for\s+\d+\s+month|for\s+one\s+month|1\s+month)\b")
EN_AFTER_RE = re.compile(r"(?i)\b(then|after|thereafter)\b")
EN_MONTH_RE = re.compile(r"(?i)(/\s*month\b|\bper\s+month\b|\bmonthly\b|\ba\s+month\b|\beach\s+month\b)")

def pick_recurring_price_token(full_text: str) -> tuple[str, str]:
    """
    Picks the recurring monthly price token (not the trial).
    IMPORTANT: only considers tokens that contain a currency (via PRICE_TOKEN_RE),
    which prevents selecting duration-only numbers like "1 month".
    Returns (display_token, amount_str).
    """
    text = _clean_spaces(full_text or "")
    if not text:
        return "", ""

    candidates = []
    for m in PRICE_TOKEN_RE.finditer(text):
        token = m.group(0)
        # extract numeric portion from the matched token
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
        ctx_raw = text[max(0, a - 70): min(len(text), b + 70)]
        ctx_en = translate_text_cached(ctx_raw)

        has_after = bool(EN_AFTER_RE.search(ctx_en))
        has_month = bool(EN_MONTH_RE.search(ctx_en))
        trialish = bool(EN_TRIAL_RE.search(ctx_en)) or is_generic_trial(ctx_raw)

        # scoring: prefer explicit after/then, then monthly, penalize trial contexts
        score = val
        if trialish:
            score *= 0.2
        if has_month:
            score *= 1.8
        if has_after:
            score *= 1.6

        # tie-break: later occurrences often are the recurring “then X/month”
        candidates.append((score, val, m.start(), token, norm))

    if not candidates:
        return "", ""

    candidates.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    best = candidates[0]
    return _clean_spaces(best[3]), str(best[4])

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
    await ctx.add_cookies(
        [{"name": "sp_lang", "value": "en", "domain": ".spotify.com", "path": "/"}]
    )
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

def pick_after_line(p_texts) -> str:
    # keep your old behaviour, but it’s optional
    for pt in (p_texts or [])[:6]:
        en = translate_text_cached(_clean_spaces(pt))
        if looks_monthly_en(en) and AFTER_RE.search(en):
            return pt
    return ""

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
            await page.wait_for_timeout(1200)
            cards = await page.query_selector_all("section:has(h3), div:has(h3), article:has(h3)")

            seen = set()
            for card in cards:
                try:
                    h3 = await card.query_selector("h3")
                    title = await (h3.inner_text() if h3 else "Unknown")
                    if not title.strip():
                        continue

                    std = normalize_plan_name(title)
                    if std == "Other":
                        continue

                    title_key = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
                    key = (std, title_key)
                    if key in seen:
                        continue
                    seen.add(key)

                    # Grab p texts (for Trial Info display)
                    p_tags = await card.query_selector_all("p")
                    p_texts = []
                    for p in p_tags:
                        try:
                            t = await p.inner_text()
                            if t:
                                p_texts.append(t)
                        except Exception:
                            pass

                    # IMPORTANT: use textContent to include hidden/legal "then X/month"
                    try:
                        full_text_all = await card.evaluate("(el) => el.textContent || ''")
                    except Exception:
                        full_text_all = " ".join(p_texts)

                    try:
                        full_text_visible = await card.inner_text()
                    except Exception:
                        full_text_visible = " ".join(p_texts)

                    # ---------- FIX: choose recurring price from currency-attached tokens ----------
                    price_display, amount = pick_recurring_price_token(full_text_all)

                    # fallback to visible text if needed
                    if not amount:
                        price_display, amount = pick_recurring_price_token(full_text_visible)

                    # still nothing? last resort: try first price-like token from p_texts join
                    if not amount:
                        price_display, amount = pick_recurring_price_token(" ".join(p_texts))

                    # Trial line shown to user (keep simple)
                    trial = p_texts[0] if p_texts else ""

                    # Optional: a line that says "then/after"
                    after = pick_after_line(p_texts)

                    if amount:
                        currency = detect_currency_in_text(price_display, a2) or default_currency_for_alpha2(a2)

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
                                "Price": amount,
                                "Billing Frequency": "month",
                                "Price After Trial": after,
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
            log("❌ Couldn’t resolve markets (Spotify blocked/empty). Re-run shortly.")
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
            log("❌ No plan cards scraped. Try again.")
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

        log(f"\n🎉 Done! Saved {csv_out} and {xlsx_out} | Rows: {len(df)} Countries: {df['Country Alpha-2'].nunique()}")

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

"""Phase 3a — SIGNALS: web intelligence for each scored company.

Sources (all via Google search / public endpoints — zero cost):
  1. LinkedIn hiring posts (site:linkedin.com/posts "hiring")
  2. Blind offer evaluations (site:teamblind.com "offer evaluation")
  3. Blind sentiment / red flags (PIP, hire-to-fire, layoffs)
  4. Layoffs.fyi / TrueUp layoff check
  5. Levels.fyi salary data (structured .md endpoint)
  6. Crunchbase / Google funding signals

Input:  data/scored.json
Output: data/signals.json (scored + signals merged)
"""
import re
import json
import time
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from jobclaw.logger import get_logger

log = get_logger("signals")

DATA = Path(__file__).resolve().parent.parent / "data"
SCORED_JSON = DATA / "scored.json"
SIGNALS_JSON = DATA / "signals.json"
SIGNALS_CACHE_JSON = DATA / "signals_cache.json"  # per-company cache across runs

_S = requests.Session()
_S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})


def _ddg(query: str, num: int = 5) -> list[dict]:
    """DuckDuckGo search — no bot detection, supports site: operators."""
    try:
        from ddgs import DDGS
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=num):
                results.append({
                    "title": r.get("title", ""),
                    "snippet": r.get("body", "")[:300],
                    "url": r.get("href", ""),
                })
        return results
    except Exception as e:
        log.debug(f"DDG search failed for '{query[:60]}': {e}")
        return []


def _google(query: str, num: int = 5) -> list[dict]:
    """Google search via HTML scraping (best-effort — can be blocked by CAPTCHA).
    Uses robust anchor+h3 selector instead of the stale div.g."""
    from urllib.parse import quote_plus
    url = f"https://www.google.com/search?q={quote_plus(query)}&num={num * 2}&hl=en&gl=us"
    try:
        resp = _S.get(url, timeout=12)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href.startswith("http") or "google.com" in href or href in seen:
                continue
            h3 = a.find("h3")
            if not h3:
                continue
            seen.add(href)
            snippet = ""
            parent = a.parent
            for _ in range(5):
                if parent is None:
                    break
                text = parent.get_text(separator=" ", strip=True)
                if len(text) > len(h3.get_text()) + 50:
                    snippet = text.replace(h3.get_text(), "").strip()[:300]
                    break
                parent = parent.parent
            results.append({"title": h3.get_text(strip=True), "snippet": snippet, "url": href})
            if len(results) >= num:
                break
        return results
    except Exception:
        return []


def _search(query: str, num: int = 5) -> list[dict]:
    """Unified search: DuckDuckGo first, Google fallback if DDG returns nothing."""
    results = _ddg(query, num)
    if not results:
        log.debug(f"DDG empty — trying Google for: {query[:60]}")
        results = _google(query, num)
    return results


# ─── 1. LinkedIn Hiring Posts ──────────────────────────────────

def _hiring_posts(company: str, keywords: list[str]) -> list[dict]:
    posts = []
    seen = set()
    queries = [
        f'site:linkedin.com/posts "{company}" "hiring"',
        f'site:linkedin.com/posts "{company}" "open role"',
        f'site:linkedin.com/posts "{company}" "join my team"',
    ]
    for kw in keywords[:2]:
        queries.append(f'site:linkedin.com/posts "{kw}" "hiring" 2026')

    for q in queries[:4]:
        for r in _search(q, num=3):
            if r["url"] in seen or "linkedin.com" not in r["url"]:
                continue
            seen.add(r["url"])
            poster = ""
            if " on LinkedIn" in r["title"]:
                poster = r["title"].split(" on LinkedIn")[0].strip()
            elif " - " in r["title"]:
                poster = r["title"].split(" - ")[0].strip()
            posts.append({"poster": poster, "snippet": r["snippet"][:200], "url": r["url"]})
        time.sleep(0.8)
    return posts


# ─── 2. Blind Offers ──────────────────────────────────────────

def _blind_offers(company: str) -> list[str]:
    results = _search(f'site:teamblind.com "offer evaluation" "{company}"', num=5)
    offers = []
    for r in results:
        s = r.get("snippet", "").lower()
        if any(kw in s for kw in ["tc", "base", "rsu", "equity", "$", "comp", "offer"]):
            offers.append(r["snippet"][:250])
    return offers[:3]


# ─── 3. Blind Sentiment ───────────────────────────────────────

def _blind_sentiment(company: str) -> dict:
    pos = _search(f'site:teamblind.com "{company}" "good WLB" OR "great culture" OR "recommend" 2025 2026', num=3)
    neg = _search(f'site:teamblind.com "{company}" "PIP" OR "hire to fire" OR "toxic" OR "layoffs" 2025 2026', num=3)
    time.sleep(0.8)
    return {
        "positive": [r["snippet"][:150] for r in pos[:2]],
        "negative": [r["snippet"][:150] for r in neg[:2]],
        "red_flags": len(neg) > len(pos),
    }


# ─── 4. Layoff Check ──────────────────────────────────────────

def _layoff_check(company: str) -> dict:
    results = _search(f'"{company}" layoffs 2025 2026 site:layoffs.fyi OR site:trueup.io', num=3)
    for r in results:
        s = r.get("snippet", "").lower()
        if any(kw in s for kw in ["laid off", "layoff", "cut", "reduce", "eliminated"]):
            return {"had_layoffs": True, "detail": r["snippet"][:200]}
    return {"had_layoffs": False, "detail": ""}


# ─── 5. Levels.fyi Salary ─────────────────────────────────────

def _levels_salary(company: str) -> dict:
    slug = re.sub(r'[^a-z0-9-]', '', company.lower().replace(" ", "-"))
    data = {"base_range": "", "tc_range": "", "source": "levels.fyi"}
    try:
        resp = _S.get(f"https://www.levels.fyi/companies/{slug}/salaries.md", timeout=10)
        if resp.status_code == 200 and len(resp.text) > 100:
            text = resp.text[:3000]
            tc = re.findall(r'\$[\d,]+[kK]?\s*[-–]\s*\$[\d,]+[kK]?', text)
            if tc:
                data["tc_range"] = tc[0]
            base = re.findall(r'[Bb]ase.*?\$[\d,]+', text)
            if base:
                data["base_range"] = base[0][:100]
    except Exception:
        pass
    return data


# ─── 6. Funding / Crunchbase ──────────────────────────────────

def _funding_signal(company: str) -> str:
    results = _search(f'"{company}" funding round raised series 2024 2025 2026', num=3)
    for r in results:
        s = r.get("snippet", "").lower()
        if any(kw in s for kw in ["series", "raised", "funding", "valuation", "seed"]):
            return r["snippet"][:200]
    return ""


# ─── 7. Levels.fyi Offer Submissions ─────────────────────────

def _levels_offers(company: str) -> list[str]:
    """Check for recent offer submissions on Levels.fyi, indicating active hiring."""
    results = _search(f'site:levels.fyi "{company}" offer OR submission 2025 2026', num=5)
    offers = []
    for r in results:
        s = r.get("snippet", "").lower()
        if any(kw in s for kw in ["offer", "submitted", "accepted", "tc", "compensation"]):
            offers.append(r["snippet"][:200])
    return offers[:3]


# ─── Main ──────────────────────────────────────────────────────

def run_signals(resume: dict) -> list[dict]:
    """Enrich scored jobs with web signals. Returns updated list."""
    log.info("=" * 50)
    log.info("PHASE 3a: SIGNALS")
    log.info("=" * 50)

    if not SCORED_JSON.exists():
        log.warning("No scored.json — run scorer first")
        return []

    scored = json.loads(SCORED_JSON.read_text())
    if not scored:
        log.info("No scored matches to enrich")
        return []

    keywords = resume.get("scout", {}).get("hiring_post_keywords", resume.get("target_roles", []))

    # Load persistent per-company cache (survives across runs)
    company_cache: dict[str, dict] = {}
    if SIGNALS_CACHE_JSON.exists():
        try:
            company_cache = json.loads(SIGNALS_CACHE_JSON.read_text())
            log.info(f"Loaded signals cache: {len(company_cache)} companies already enriched")
        except Exception:
            company_cache = {}

    new_companies = sum(1 for j in scored if j.get("company", "") not in company_cache)
    log.info(f"Enriching {len(scored)} matches ({new_companies} new companies, {len(scored) - new_companies} cached)")

    for i, job in enumerate(scored):
        company = job.get("company", "")
        log.info(f"({i+1}/{len(scored)}) {company}")

        if company in company_cache:
            job["signals"] = company_cache[company]
            log.debug(f"  (cached)")
            continue

        signals: dict = {}

        # 1. LinkedIn hiring posts
        try:
            signals["hiring_posts"] = _hiring_posts(company, keywords)
            if signals["hiring_posts"]:
                log.info(f"  📢 {len(signals['hiring_posts'])} hiring posts found")
        except Exception as e:
            log.warning(f"  Hiring posts failed: {e}")
            signals["hiring_posts"] = []

        # 2. Blind offers
        try:
            signals["blind_offers"] = _blind_offers(company)
            if signals["blind_offers"]:
                log.info(f"  💰 {len(signals['blind_offers'])} Blind offer data points")
        except Exception as e:
            log.warning(f"  Blind offers failed: {e}")
            signals["blind_offers"] = []
        time.sleep(0.8)

        # 3. Blind sentiment
        try:
            signals["blind_sentiment"] = _blind_sentiment(company)
            if signals["blind_sentiment"]["red_flags"]:
                log.warning(f"  ⚠️  Blind red flags detected")
        except Exception as e:
            log.warning(f"  Blind sentiment failed: {e}")
            signals["blind_sentiment"] = {"positive": [], "negative": [], "red_flags": False}
        time.sleep(0.8)

        # 4. Layoff check
        try:
            signals["layoffs"] = _layoff_check(company)
            if signals["layoffs"]["had_layoffs"]:
                log.warning(f"  📉 Recent layoffs detected")
            else:
                log.info(f"  ✅ No recent layoffs")
        except Exception as e:
            log.warning(f"  Layoff check failed: {e}")
            signals["layoffs"] = {"had_layoffs": False, "detail": ""}
        time.sleep(0.8)

        # 5. Salary data
        try:
            signals["salary"] = _levels_salary(company)
            if signals["salary"]["tc_range"]:
                log.info(f"  💵 TC: {signals['salary']['tc_range']}")
        except Exception as e:
            log.warning(f"  Levels.fyi failed: {e}")
            signals["salary"] = {"base_range": "", "tc_range": "", "source": ""}
        time.sleep(0.5)

        # 6. Funding
        try:
            signals["funding"] = _funding_signal(company)
            if signals["funding"]:
                log.info(f"  🚀 Funding: {signals['funding'][:60]}...")
        except Exception as e:
            log.warning(f"  Funding check failed: {e}")
            signals["funding"] = ""

        # 7. Levels.fyi offers
        try:
            signals["levels_offers"] = _levels_offers(company)
            if signals["levels_offers"]:
                log.info(f"  📊 Levels.fyi offers: {len(signals['levels_offers'])} submissions")
        except Exception as e:
            log.warning(f"  Levels.fyi offers failed: {e}")
            signals["levels_offers"] = []

        company_cache[company] = signals
        job["signals"] = signals
        # Persist cache after each company so progress isn't lost on interrupt
        SIGNALS_CACHE_JSON.write_text(json.dumps(company_cache, indent=2))
        time.sleep(1)

    SIGNALS_JSON.write_text(json.dumps(scored, indent=2))
    log.info(f"Signals saved to signals.json")
    return scored

"""Phase 3a — SIGNALS: web intelligence for each scored company.

Sources (all via Google search / public endpoints — zero cost):
  1. LinkedIn hiring posts (site:linkedin.com/posts "hiring")
  2. Blind offer evaluations (site:teamblind.com "offer evaluation")
  3. Blind sentiment / red flags (PIP, hire-to-fire, layoffs)
  4. Layoffs.fyi / TrueUp layoff check
  5. Levels.fyi salary data (structured .md endpoint)
  6. Crunchbase / Google funding signals
  7. Levels.fyi offer submissions

Performance design:
  - Signals 1, 2, 4 are bulk-fetched ONCE (a few broad searches → pool),
    then all per-company lookups hit the in-memory dict — zero extra searches.
  - Signals 3, 5, 6, 7 remain per-company but run in parallel (3 workers).
  - A global search throttle (0.5s between searches) replaces scattered sleeps
    while still allowing concurrent HTTP requests (levels.fyi, etc).

Typical runtime: ~5-6 min for 85 companies (vs ~30 min sequential).

Input:  data/scored.json
Output: data/signals.json (scored + signals merged)
"""
import re
import json
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from bs4 import BeautifulSoup
from jobclaw.logger import get_logger

log = get_logger("signals")

DATA = Path(__file__).resolve().parent.parent / "data"
SCORED_JSON = DATA / "scored.json"
SIGNALS_JSON = DATA / "signals.json"
SIGNALS_CACHE_JSON = DATA / "signals_cache.json"

_S = requests.Session()
_S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})

# ── Global search throttle — max 2 concurrent, min 0.5s between ──
_search_sem = threading.Semaphore(2)
_search_lock = threading.Lock()
_last_search_at: float = 0.0
_SEARCH_INTERVAL = 0.5  # seconds between any two searches


def _ddg(query: str, num: int = 5, timelimit: str | None = None) -> list[dict]:
    """timelimit: 'd'=day, 'w'=week, 'm'=month, 'y'=year, None=any."""
    try:
        from ddgs import DDGS
        results = []
        kwargs = {"max_results": num}
        if timelimit:
            kwargs["timelimit"] = timelimit
        with DDGS() as ddgs:
            for r in ddgs.text(query, **kwargs):
                results.append({
                    "title": r.get("title", ""),
                    "snippet": r.get("body", "")[:300],
                    "url": r.get("href", ""),
                })
        return results
    except Exception as e:
        log.debug(f"DDG search failed for '{query[:60]}': {e}")
        return []


# Max age for hiring posts: 2 months
_MAX_POST_AGE_MONTHS = 2


def _post_age_ok(text: str) -> bool:
    """Return False if the text contains a relative date indicating the post is too old.
    LinkedIn snippets often contain '1 year ago', '8 months ago', etc.
    Anything older than _MAX_POST_AGE_MONTHS is rejected.
    """
    tl = text.lower()
    m = re.search(r'(\d+)\s+year', tl)
    if m:
        return False  # any "X year(s) ago" is too old
    m = re.search(r'(\d+)\s+month', tl)
    if m and int(m.group(1)) > _MAX_POST_AGE_MONTHS:
        return False
    return True


def _google(query: str, num: int = 5) -> list[dict]:
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


def _search(query: str, num: int = 5, timelimit: str | None = None) -> list[dict]:
    """Throttled unified search: DDG first, Google fallback."""
    global _last_search_at
    with _search_sem:
        with _search_lock:
            wait = _SEARCH_INTERVAL - (time.time() - _last_search_at)
            if wait > 0:
                time.sleep(wait)
            _last_search_at = time.time()
        results = _ddg(query, num, timelimit=timelimit)
        if not results:
            log.debug(f"DDG empty — trying Google for: {query[:60]}")
            with _search_lock:
                wait = _SEARCH_INTERVAL - (time.time() - _last_search_at)
                if wait > 0:
                    time.sleep(wait)
                _last_search_at = time.time()
            results = _google(query, num)
    return results


# ─── Bulk pool helpers ─────────────────────────────────────────

_NOISE_WORDS = {
    "the", "and", "for", "with", "from", "this", "that", "have", "has",
    "are", "was", "were", "will", "been", "they", "their", "offer", "offers",
    "hiring", "tech", "jobs", "company", "inc", "llc", "corp", "ltd",
}


def _extract_company_names(text: str) -> list[str]:
    """Extract probable company names (capitalized tokens) from freeform text."""
    candidates = re.findall(r'\b([A-Z][A-Za-z0-9\.\-]{1,30})\b', text)
    return [c for c in candidates if c.lower() not in _NOISE_WORDS and len(c) > 2]


# ─── 1. LinkedIn Hiring Posts (bulk pool) ─────────────────────

_hiring_posts_pool: dict[str, list[dict]] | None = None


def _build_hiring_posts_pool(hm_titles: list[str], target_roles: list[str]) -> dict[str, list[dict]]:
    """Search LinkedIn hiring posts by HM title and role — build a company→posts index.
    ~8-10 searches instead of 4 per company.
    """
    log.info("Building hiring posts pool (bulk fetch)...")
    queries = []
    for title in hm_titles[:3]:
        queries += [
            f'site:linkedin.com/posts "{title}" "hiring" 2026',
            f'site:linkedin.com/posts "{title}" "join my team" 2026',
        ]
    for role in target_roles[:2]:
        queries.append(f'site:linkedin.com/posts "{role}" "we are hiring" 2026')
    queries.append('site:linkedin.com/posts "engineering" "hiring" "open role" 2026')

    pool: dict[str, list[dict]] = {}
    seen_urls: set[str] = set()

    for q in queries[:8]:
        for r in _search(q, num=5, timelimit='m'):
            url = r.get("url", "")
            if url in seen_urls or "linkedin.com" not in url:
                continue
            title = r.get("title", "")
            snippet = r.get("snippet", "")
            # Reject posts older than _MAX_POST_AGE_MONTHS
            if not _post_age_ok(title + " " + snippet):
                continue
            seen_urls.add(url)

            poster = title.split(" on LinkedIn")[0].strip() if " on LinkedIn" in title else ""

            # Extract "at CompanyName" from text
            text = title + " " + snippet
            m = re.search(r'\bat\s+([A-Z][A-Za-z0-9][A-Za-z0-9\s\.\-]{1,30}?)(?:\s*[,!.\n]|$)', text)
            company_hint = m.group(1).strip() if m else ""

            post = {"poster": poster, "snippet": snippet[:200], "url": url}

            # Index by company hint and by all capitalized names in text
            names = ([company_hint] if company_hint else []) + _extract_company_names(text)
            for name in set(names):
                key = name.lower()
                if len(key) > 3:
                    pool.setdefault(key, []).append(post)

    log.info(f"Hiring posts pool: {len(seen_urls)} posts → {len(pool)} company keys")
    return pool


def _hiring_posts(company: str, keywords: list[str]) -> list[dict]:
    """Look up company in pool; fall back to 1 targeted search on pool miss."""
    global _hiring_posts_pool
    if _hiring_posts_pool is None:
        # Pool not initialized — do direct search (shouldn't normally happen)
        return _hiring_posts_direct(company)

    key = company.lower().strip()
    matches: list[dict] = []
    seen_urls: set[str] = set()
    for word in key.split():
        if len(word) > 3:
            for post in _hiring_posts_pool.get(word, []):
                if post["url"] not in seen_urls:
                    seen_urls.add(post["url"])
                    matches.append(post)

    # Pool miss — one targeted fallback search (last month only)
    if not matches:
        for r in _search(f'site:linkedin.com/posts "{company}" "hiring"', num=3, timelimit='m'):
            if "linkedin.com" not in r.get("url", "") or r["url"] in seen_urls:
                continue
            if not _post_age_ok(r.get("title", "") + " " + r.get("snippet", "")):
                continue
            poster = r["title"].split(" on LinkedIn")[0].strip() if " on LinkedIn" in r["title"] else ""
            matches.append({"poster": poster, "snippet": r["snippet"][:200], "url": r["url"]})

    return matches[:5]


def _hiring_posts_direct(company: str) -> list[dict]:
    """Direct search fallback (used when pool unavailable)."""
    posts = []
    seen: set[str] = set()
    for q in [
        f'site:linkedin.com/posts "{company}" "hiring"',
        f'site:linkedin.com/posts "{company}" "join my team"',
    ]:
        for r in _search(q, num=3, timelimit='m'):
            if r["url"] in seen or "linkedin.com" not in r["url"]:
                continue
            if not _post_age_ok(r.get("title", "") + " " + r.get("snippet", "")):
                continue
            seen.add(r["url"])
            poster = r["title"].split(" on LinkedIn")[0].strip() if " on LinkedIn" in r["title"] else ""
            posts.append({"poster": poster, "snippet": r["snippet"][:200], "url": r["url"]})
    return posts


# ─── 2. Blind Offers (bulk pool) ──────────────────────────────

_blind_offer_pool: dict[str, list[str]] | None = None


def _build_blind_offer_pool() -> dict[str, list[str]]:
    """Scrape recent Blind offer posts in bulk, index by company name."""
    log.info("Building Blind offer pool (bulk fetch)...")
    queries = [
        'site:teamblind.com "offer evaluation" 2025 2026',
        'site:teamblind.com "offer" "TC" "RSU" 2026',
        'site:teamblind.com "offer evaluation" software engineer 2026',
        'site:teamblind.com "offer evaluation" senior staff principal 2026',
    ]
    seen_urls: set[str] = set()
    pool: dict[str, list[str]] = {}

    for q in queries:
        for r in _search(q, num=8):
            url = r.get("url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)
            snippet = r.get("snippet", "")
            if not any(kw in snippet.lower() for kw in ["tc", "base", "rsu", "equity", "$", "comp", "offer"]):
                continue
            for name in set(_extract_company_names(r.get("title", "") + " " + snippet)):
                pool.setdefault(name.lower(), []).append(snippet[:250])

    log.info(f"Blind offer pool: {len(seen_urls)} posts → {len(pool)} company mentions")
    return pool


def _blind_offers(company: str) -> list[str]:
    global _blind_offer_pool
    if _blind_offer_pool is None:
        _blind_offer_pool = _build_blind_offer_pool()
    matches: list[str] = []
    seen: set[str] = set()
    for word in company.lower().strip().split():
        if len(word) > 3:
            for s in _blind_offer_pool.get(word, []):
                if s not in seen:
                    seen.add(s)
                    matches.append(s)
    return matches[:3]


# ─── 3. Blind Sentiment ───────────────────────────────────────

def _blind_sentiment(company: str) -> dict:
    pos = _search(f'site:teamblind.com "{company}" "good WLB" OR "great culture" OR "recommend" 2025 2026', num=3)
    neg = _search(f'site:teamblind.com "{company}" "PIP" OR "hire to fire" OR "toxic" OR "layoffs" 2025 2026', num=3)
    return {
        "positive": [r["snippet"][:150] for r in pos[:2]],
        "negative": [r["snippet"][:150] for r in neg[:2]],
        "red_flags": len(neg) > len(pos),
    }


# ─── 4. Layoff Check (bulk pool) ──────────────────────────────

_layoffs_pool: set[str] | None = None


def _build_layoffs_pool() -> set[str]:
    """Fetch recent tech layoff data in bulk. Returns set of company name tokens."""
    log.info("Building layoffs pool (bulk fetch)...")
    queries = [
        'site:layoffs.fyi 2025 2026',
        'site:layoffs.fyi tech layoffs 2025 2026',
        'tech company "laid off" employees 2025 2026 site:techcrunch.com',
        '"lays off" OR "job cuts" tech 2026 site:bloomberg.com OR site:reuters.com',
        'site:trueup.io layoffs tech 2025 2026',
    ]
    companies: set[str] = set()
    for q in queries:
        for r in _search(q, num=10):
            for name in _extract_company_names(r.get("title", "") + " " + r.get("snippet", "")):
                companies.add(name.lower())

    log.info(f"Layoffs pool: {len(companies)} company name tokens")
    return companies


def _layoff_check(company: str) -> dict:
    global _layoffs_pool
    if _layoffs_pool is None:
        _layoffs_pool = _build_layoffs_pool()
    key = company.lower().strip()
    for word in key.split():
        if len(word) > 3 and word in _layoffs_pool:
            return {"had_layoffs": True, "detail": "Found in recent layoff reports"}
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


# ─── 6. Funding ───────────────────────────────────────────────

def _funding_signal(company: str) -> str:
    results = _search(f'"{company}" funding round raised series 2024 2025 2026', num=3)
    for r in results:
        if any(kw in r.get("snippet", "").lower() for kw in ["series", "raised", "funding", "valuation", "seed"]):
            return r["snippet"][:200]
    return ""


# ─── 7. Levels.fyi Offer Submissions ─────────────────────────

def _levels_offers(company: str) -> list[str]:
    results = _search(f'site:levels.fyi "{company}" offer OR submission 2025 2026', num=5)
    offers = []
    for r in results:
        if any(kw in r.get("snippet", "").lower() for kw in ["offer", "submitted", "accepted", "tc", "compensation"]):
            offers.append(r["snippet"][:200])
    return offers[:3]


# ─── Per-company enrichment (runs in thread) ──────────────────

def _enrich_company(company: str, keywords: list[str]) -> dict:
    """Run all signals for one company. Safe to call from a thread."""
    signals: dict = {}

    try:
        signals["hiring_posts"] = _hiring_posts(company, keywords)
    except Exception:
        signals["hiring_posts"] = []

    try:
        signals["blind_offers"] = _blind_offers(company)
    except Exception:
        signals["blind_offers"] = []

    try:
        signals["blind_sentiment"] = _blind_sentiment(company)
    except Exception:
        signals["blind_sentiment"] = {"positive": [], "negative": [], "red_flags": False}

    try:
        signals["layoffs"] = _layoff_check(company)
    except Exception:
        signals["layoffs"] = {"had_layoffs": False, "detail": ""}

    try:
        signals["salary"] = _levels_salary(company)
    except Exception:
        signals["salary"] = {"base_range": "", "tc_range": "", "source": ""}

    try:
        signals["funding"] = _funding_signal(company)
    except Exception:
        signals["funding"] = ""

    try:
        signals["levels_offers"] = _levels_offers(company)
    except Exception:
        signals["levels_offers"] = []

    return signals


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

    # Load persistent cache
    company_cache: dict[str, dict] = {}
    if SIGNALS_CACHE_JSON.exists():
        try:
            company_cache = json.loads(SIGNALS_CACHE_JSON.read_text())
            log.info(f"Loaded signals cache: {len(company_cache)} companies already enriched")
        except Exception:
            company_cache = {}

    # Unique companies that need enrichment
    todo = list(dict.fromkeys(
        j.get("company", "") for j in scored
        if j.get("company", "") and j.get("company", "") not in company_cache
    ))
    log.info(f"{len(todo)} new companies to enrich, {len(company_cache)} cached")

    if todo:
        # ── Phase A: Bulk pool builds in parallel (3 workers, no interdependencies) ──
        log.info("Pre-fetching bulk pools in parallel (Blind offers, hiring posts, layoffs)...")
        global _blind_offer_pool, _layoffs_pool, _hiring_posts_pool
        hm_titles = resume.get("hm_titles_above_me", [])
        target_roles = resume.get("target_roles", [])
        with ThreadPoolExecutor(max_workers=3) as pool_executor:
            blind_fut   = pool_executor.submit(_build_blind_offer_pool)
            layoffs_fut = pool_executor.submit(_build_layoffs_pool)
            hiring_fut  = pool_executor.submit(_build_hiring_posts_pool, hm_titles, target_roles)
            _blind_offer_pool   = blind_fut.result()
            _layoffs_pool       = layoffs_fut.result()
            _hiring_posts_pool  = hiring_fut.result()

        # ── Phase B: Parallel per-company enrichment ──
        cache_lock = threading.Lock()

        def _enrich_and_cache(company: str) -> tuple[str, dict]:
            signals = _enrich_company(company, keywords)
            with cache_lock:
                company_cache[company] = signals
                SIGNALS_CACHE_JSON.write_text(json.dumps(company_cache, indent=2))
            return company, signals

        log.info(f"Enriching {len(todo)} companies (3 parallel workers)...")
        done_count = 0
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(_enrich_and_cache, c): c for c in todo}
            for future in as_completed(futures):
                company = futures[future]
                done_count += 1
                try:
                    _, signals = future.result()
                    hp = len(signals.get("hiring_posts", []))
                    bo = len(signals.get("blind_offers", []))
                    layoff = signals.get("layoffs", {}).get("had_layoffs", False)
                    flags = signals.get("blind_sentiment", {}).get("red_flags", False)
                    status = " | ".join(filter(None, [
                        f"📢 {hp} posts" if hp else "",
                        f"💰 {bo} offers" if bo else "",
                        "📉 layoffs" if layoff else "",
                        "⚠️ flags" if flags else "",
                    ])) or "no signals"
                    log.info(f"  ({done_count}/{len(todo)}) {company} — {status}")
                except Exception as e:
                    log.warning(f"  ({done_count}/{len(todo)}) {company} failed: {e}")

    # Attach cached signals to each job
    for job in scored:
        company = job.get("company", "")
        job["signals"] = company_cache.get(company, {
            "hiring_posts": [], "blind_offers": [],
            "blind_sentiment": {"positive": [], "negative": [], "red_flags": False},
            "layoffs": {"had_layoffs": False, "detail": ""},
            "salary": {"base_range": "", "tc_range": "", "source": ""},
            "funding": "", "levels_offers": [],
        })

    SIGNALS_JSON.write_text(json.dumps(scored, indent=2))
    log.info(f"Signals saved to signals.json")
    return scored

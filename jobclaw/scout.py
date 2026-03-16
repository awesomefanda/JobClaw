"""Phase 1 — DISCOVER: aggregate job leads from every source.

Sources:
  1. JobSpy (Indeed, LinkedIn, Glassdoor, ZipRecruiter)
  1b. Google Careers (custom Playwright scraper)
  2. Greenhouse boards (public JSON API)
  3. Lever boards (public JSON API)
  4. Y Combinator "Work at a Startup" (workatastartup.com)
  5. Hacker News "Who's Hiring" monthly thread
  6. Wellfound (AngelList) role pages
  11. TeamBlind Job Board (encrypted REST API)

Each source is wrapped in try/except — if one fails we log and continue.
Output: data/jobs.csv (cumulative, deduped)
"""
import csv
import hashlib
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from jobclaw.logger import get_logger

log = get_logger("scout")

DATA = Path(__file__).resolve().parent.parent / "data"
JOBS_CSV = DATA / "jobs.csv"

CSV_FIELDS = [
    "id", "title", "company", "location", "is_remote", "job_url",
    "salary_min", "salary_max", "job_type", "description",
    "date_posted", "source", "founder_email", "scraped_at",
]

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})


def _id(company: str, title: str, loc: str) -> str:
    raw = f"{company.lower().strip()}|{title.lower().strip()}|{loc.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _safe_float(val) -> str:
    if val is None:
        return ""
    try:
        import math
        f = float(val)
        return "" if math.isnan(f) else str(int(f))
    except (ValueError, TypeError):
        return ""


def _existing_ids() -> set:
    if not JOBS_CSV.exists():
        return set()
    ids = set()
    with open(JOBS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ids.add(row.get("id", ""))
    return ids


# ─── Source 1: JobSpy ──────────────────────────────────────────

def _scrape_jobspy(resume: dict) -> list[dict]:
    try:
        from jobspy import scrape_jobs
    except ImportError:
        log.error("python-jobspy not installed. Run: pip install python-jobspy")
        return []

    scout = resume.get("scout", {})
    platforms = [p for p in scout.get("platforms", ["indeed", "linkedin", "google"]) if p != "google"]  # Exclude Google, handled separately
    hours_old = scout.get("hours_old", 72)
    max_results = scout.get("max_results", 50)
    is_remote = resume.get("preferences", {}).get("remote", True)
    location = resume.get("location", "") if not is_remote else ""

    jobs = []
    for term in resume.get("target_roles", ["software engineer"]):
        # Always search remote jobs. Also search local on-site if remote=False.
        searches = [("", True)]
        if not is_remote:
            searches.append((location, False))
        for loc, remote_flag in searches:
            log.info(f"JobSpy: '{term}' | remote={remote_flag} | location='{loc}' | platforms={platforms}")
            try:
                df = scrape_jobs(
                    site_name=platforms,
                    search_term=term,
                    location=loc,
                    is_remote=remote_flag,
                    results_wanted=max_results,
                    hours_old=hours_old,
                    country_indeed="USA",
                    description_format="markdown",
                )
            except Exception as e:
                log.warning(f"JobSpy error for '{term}' (remote={remote_flag}): {e}")
                continue

            if df is None or df.empty:
                log.debug(f"JobSpy: no results for '{term}' (remote={remote_flag})")
                continue

            now = datetime.now().isoformat()
            for _, row in df.iterrows():
                company = str(row.get("company", "") or "").strip()
                title = str(row.get("title", "") or "").strip()
                city = str(row.get("city", "") or "")
                state = str(row.get("state", "") or "")
                job_loc = f"{city}, {state}".strip(", ") if city or state else ""

                jobs.append({
                    "id": _id(company, title, job_loc),
                    "title": title, "company": company, "location": job_loc,
                    "is_remote": str(row.get("is_remote", False)),
                    "job_url": str(row.get("job_url", "") or ""),
                    "salary_min": _safe_float(row.get("min_amount")),
                    "salary_max": _safe_float(row.get("max_amount")),
                    "job_type": str(row.get("job_type", "") or ""),
                    "description": str(row.get("description", "") or "")[:8000],
                    "date_posted": str(row.get("date_posted", "") or ""),
                    "source": str(row.get("site", "") or "jobspy"),
                    "founder_email": "", "scraped_at": now,
                })

    log.info(f"JobSpy: {len(jobs)} total listings")
    return jobs


# ─── Source 1b: Google Careers ─────────────────────────────────

def _scrape_google(resume: dict) -> list[dict]:
    try:
        from .scraper import scrape_google_jobs
        from .parser import parse_all_google_jobs
    except ImportError:
        log.error("Google scraper not available. Install playwright and ensure scraper.py exists.")
        return []

    scout = resume.get("scout", {})
    if "google" not in scout.get("platforms", []):
        return []

    # Derive country from resume location (e.g. "San Jose, CA" → "United States")
    # Stored in preferences.country; defaults to United States
    country = resume.get("preferences", {}).get("country", "United States")

    # Scrape raw HTML — one search per target role (not all joined into one broken string)
    for role in resume.get("target_roles", ["software engineer"])[:3]:
        scrape_google_jobs(
            search_term=role,
            max_jobs=scout.get("max_results", 20),
            delay=3.0,
            country=country,
        )

    # Parse to structured data
    parsed_jobs = parse_all_google_jobs()

    # Convert to CSV format
    jobs = []
    now = datetime.now().isoformat()
    for pj in parsed_jobs:
        jobs.append({
            "id": pj["id"],
            "title": pj["title"],
            "company": pj["company"],
            "location": pj["location"],
            "is_remote": "true" if "remote" in pj["location"].lower() else "false",
            "job_url": pj["url"],
            "salary_min": "",
            "salary_max": "",
            "job_type": "",
            "description": pj["full_markdown"],
            "date_posted": "",
            "source": "google_careers",
            "founder_email": "",
            "scraped_at": now,
        })

    log.info(f"Google Careers: {len(jobs)} listings")
    return jobs


# ─── Source 2: Greenhouse ──────────────────────────────────────

def _scrape_greenhouse(boards: list[str]) -> list[dict]:
    jobs = []
    now = datetime.now().isoformat()
    for board in boards:
        try:
            resp = _SESSION.get(
                f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs",
                timeout=15,
            )
            if resp.status_code != 200:
                log.warning(f"Greenhouse {board}: HTTP {resp.status_code}")
                continue
            for j in resp.json().get("jobs", []):
                loc = j.get("location", {}).get("name", "")
                company = board.replace("-", " ").title()
                jobs.append({
                    "id": _id(company, j.get("title", ""), loc),
                    "title": j.get("title", ""), "company": company,
                    "location": loc, "is_remote": str("Remote" in loc),
                    "job_url": j.get("absolute_url", ""),
                    "salary_min": "", "salary_max": "", "job_type": "",
                    "description": "", "date_posted": str(j.get("updated_at", ""))[:10],
                    "source": "greenhouse", "founder_email": "", "scraped_at": now,
                })
        except Exception as e:
            log.warning(f"Greenhouse {board} failed: {e}")
    if jobs:
        log.info(f"Greenhouse: {len(jobs)} listings from {len(boards)} boards")
    return jobs


# ─── Source 3: Lever ───────────────────────────────────────────

def _scrape_lever(boards: list[str]) -> list[dict]:
    jobs = []
    now = datetime.now().isoformat()
    for board in boards:
        try:
            resp = _SESSION.get(f"https://api.lever.co/v0/postings/{board}", timeout=15)
            if resp.status_code != 200:
                log.warning(f"Lever {board}: HTTP {resp.status_code}")
                continue
            data = resp.json()
            for j in (data if isinstance(data, list) else []):
                loc = j.get("categories", {}).get("location", "")
                company = board.replace("-", " ").title()
                jobs.append({
                    "id": _id(company, j.get("text", ""), loc),
                    "title": j.get("text", ""), "company": company,
                    "location": loc, "is_remote": str("Remote" in loc),
                    "job_url": j.get("hostedUrl", ""),
                    "salary_min": "", "salary_max": "", "job_type": "",
                    "description": (j.get("descriptionPlain", "") or "")[:8000],
                    "date_posted": "", "source": "lever",
                    "founder_email": "", "scraped_at": now,
                })
        except Exception as e:
            log.warning(f"Lever {board} failed: {e}")
    if jobs:
        log.info(f"Lever: {len(jobs)} listings from {len(boards)} boards")
    return jobs


# ─── Source 4: Y Combinator "Work at a Startup" ───────────────

def _scrape_yc_jobs(target_roles: list[str]) -> list[dict]:
    """Scrape YC's Work at a Startup. Public HTML, no login needed."""
    jobs = []
    now = datetime.now().isoformat()
    base = "https://www.workatastartup.com/jobs"

    for role_slug in ["software-engineer", "engineering-manager", "staff-engineer"]:
        try:
            resp = _SESSION.get(f"{base}?role={role_slug}&remote=true", timeout=15)
            if resp.status_code != 200:
                log.debug(f"YC jobs: HTTP {resp.status_code} for {role_slug}")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            # YC uses React hydration — look for script tags with JSON data
            for script in soup.find_all("script", type="application/json"):
                try:
                    data = json.loads(script.string or "")
                    # Navigate the nested structure if possible
                    if isinstance(data, dict) and "jobs" in str(data)[:200].lower():
                        log.debug(f"YC: found JSON payload for {role_slug}")
                except Exception:
                    continue
        except Exception as e:
            log.debug(f"YC scrape failed for {role_slug}: {e}")

    # Fallback: Google search for YC jobs
    if not jobs:
        try:
            for role in target_roles[:2]:
                query = f'site:workatastartup.com "{role}" remote'
                results = _search(query, num=10)
                for r in results:
                    if "workatastartup.com" not in r.get("url", ""):
                        continue
                    title_parts = r.get("title", "").split(" at ")
                    title = title_parts[0].strip() if title_parts else r.get("title", "")
                    company = title_parts[1].split("|")[0].strip() if len(title_parts) > 1 else ""
                    if company and title:
                        jobs.append({
                            "id": _id(company, title, "Remote"),
                            "title": title, "company": company,
                            "location": "Remote", "is_remote": "True",
                            "job_url": r.get("url", ""),
                            "salary_min": "", "salary_max": "", "job_type": "",
                            "description": r.get("snippet", ""),
                            "date_posted": "", "source": "yc_waaas",
                            "founder_email": "", "scraped_at": now,
                        })
        except Exception as e:
            log.warning(f"YC Google fallback failed: {e}")

    if jobs:
        log.info(f"YC Work at a Startup: {len(jobs)} listings")
    else:
        log.debug("YC Work at a Startup: no listings found")
    return jobs


# ─── Source 5: Hacker News "Who's Hiring" ──────────────────────

import json

def _scrape_hn_hiring(target_skills: list[str]) -> list[dict]:
    """Scrape the latest HN 'Who is Hiring?' thread. Each top-level comment is a job."""
    jobs = []
    now = datetime.now().isoformat()

    # Find the latest "Who is hiring" thread
    try:
        search_url = "https://hn.algolia.com/api/v1/search?query=who+is+hiring&tags=story&hitsPerPage=5"
        resp = _SESSION.get(search_url, timeout=10)
        if resp.status_code != 200:
            log.warning(f"HN Algolia search failed: HTTP {resp.status_code}")
            return []

        hits = resp.json().get("hits", [])
        # Find the most recent "Ask HN: Who is hiring?" post
        story_id = None
        for hit in hits:
            title = hit.get("title", "").lower()
            if "who is hiring" in title and "ask hn" in title:
                story_id = hit.get("objectID")
                log.info(f"HN: found thread '{hit.get('title')}' (ID: {story_id})")
                break

        if not story_id:
            log.debug("HN: no 'Who is Hiring' thread found")
            return []

        # Fetch top-level comments (each is a job posting)
        items_url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
        story = _SESSION.get(items_url, timeout=10).json()
        kid_ids = story.get("kids", [])[:100]  # First 100 comments

        skills_lower = [s.lower() for s in target_skills]

        for kid_id in kid_ids[:50]:  # Process top 50
            try:
                comment = _SESSION.get(
                    f"https://hacker-news.firebaseio.com/v0/item/{kid_id}.json",
                    timeout=5,
                ).json()
                text = comment.get("text", "") or ""
                if not text or comment.get("deleted"):
                    continue

                # Parse: first line is usually "Company | Role | Location | ..."
                plain = BeautifulSoup(text, "html.parser").get_text(separator="\n")
                lines = plain.strip().split("\n")
                first_line = lines[0] if lines else ""

                # Extract company/role from first line (pipe-delimited)
                parts = [p.strip() for p in first_line.split("|")]
                company = parts[0] if parts else ""
                title = parts[1] if len(parts) > 1 else ""
                loc = parts[2] if len(parts) > 2 else ""

                if not company or not title:
                    continue

                # Check if relevant to our skills
                text_lower = plain.lower()
                if not any(s in text_lower for s in skills_lower):
                    continue

                # Extract email if present
                email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', plain)
                founder_email = email_match.group(0) if email_match else ""

                jobs.append({
                    "id": _id(company, title, loc),
                    "title": title, "company": company,
                    "location": loc, "is_remote": str("remote" in loc.lower()),
                    "job_url": f"https://news.ycombinator.com/item?id={kid_id}",
                    "salary_min": "", "salary_max": "", "job_type": "",
                    "description": plain[:5000],
                    "date_posted": "", "source": "hn_hiring",
                    "founder_email": founder_email, "scraped_at": now,
                })
            except Exception:
                continue

    except Exception as e:
        log.warning(f"HN scrape failed: {e}")

    if jobs:
        log.info(f"HN Who's Hiring: {len(jobs)} relevant listings")
    else:
        log.debug("HN Who's Hiring: no relevant listings found")
    return jobs


# ─── Source 6: Wellfound (AngelList) ──────────────────────────

def _scrape_wellfound(target_roles: list[str]) -> list[dict]:
    """Search Wellfound via Google (direct scraping requires proxies)."""
    jobs = []
    now = datetime.now().isoformat()

    for role in target_roles[:2]:
        try:
            query = f'site:wellfound.com/jobs "{role}" remote'
            results = _search(query, num=8)
            for r in results:
                url = r.get("url", "")
                if "wellfound.com" not in url:
                    continue
                title = r.get("title", "").split(" at ")[0].strip() if " at " in r.get("title", "") else ""
                company = ""
                if " at " in r.get("title", ""):
                    company = r["title"].split(" at ")[1].split("|")[0].split("-")[0].strip()
                if not title or not company:
                    continue
                jobs.append({
                    "id": _id(company, title, "Remote"),
                    "title": title, "company": company,
                    "location": "Remote", "is_remote": "True",
                    "job_url": url,
                    "salary_min": "", "salary_max": "", "job_type": "",
                    "description": r.get("snippet", ""),
                    "date_posted": "", "source": "wellfound",
                    "founder_email": "", "scraped_at": now,
                })
        except Exception as e:
            log.warning(f"Wellfound scrape failed for '{role}': {e}")

    if jobs:
        log.info(f"Wellfound: {len(jobs)} listings")
    else:
        log.debug("Wellfound: no listings found")
    return jobs


# ─── Search helpers (DDG primary, Google fallback) ─────────────

def _ddg_search(query: str, num: int = 5) -> list[dict]:
    """DuckDuckGo search — no bot detection, supports site: operators.
    Returns [{title, snippet, url}]."""
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


def _google_search(query: str, num: int = 5) -> list[dict]:
    """Google search via HTML scraping (best-effort — can be blocked by CAPTCHA).
    Uses robust anchor+h3 selector instead of the stale div.g."""
    from urllib.parse import quote_plus
    url = f"https://www.google.com/search?q={quote_plus(query)}&num={num * 2}&hl=en&gl=us"
    try:
        resp = _SESSION.get(url, timeout=12)
        if resp.status_code != 200:
            log.debug(f"Google blocked (HTTP {resp.status_code}) for: {query[:60]}")
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
    results = _ddg_search(query, num)
    if not results:
        log.debug(f"DDG empty — trying Google for: {query[:60]}")
        results = _google_search(query, num)
    return results


# ─── Keyword filter ───────────────────────────────────────────

def _keyword_filter(jobs: list[dict], exclude: list[str]) -> list[dict]:
    if not exclude:
        return jobs
    exclude_lower = [kw.lower() for kw in exclude]
    filtered = [
        j for j in jobs
        if not any(kw in (j["title"] + " " + j.get("description", "")).lower() for kw in exclude_lower)
    ]
    removed = len(jobs) - len(filtered)
    if removed:
        log.info(f"Keyword filter removed {removed} jobs")
    return filtered


# Known non-US country tokens — used to reject jobs with explicit foreign locations
_FOREIGN_COUNTRY_TOKENS = [
    "india", "canada", "united kingdom", "uk", "germany", "france", "australia",
    "singapore", "netherlands", "ireland", "poland", "brazil", "mexico", "spain",
    "sweden", "denmark", "norway", "finland", "switzerland", "austria", "belgium",
    "japan", "china", "south korea", "israel", "new zealand", "portugal", "italy",
    "czechia", "czech republic", "hungary", "romania", "ukraine", "argentina",
    "colombia", "chile", "malaysia", "indonesia", "philippines", "vietnam",
    # City-level hints that are unambiguously non-US
    "bangalore", "hyderabad", "mumbai", "delhi", "pune", "chennai", "noida",
    "toronto", "vancouver", "montreal", "london", "amsterdam", "berlin", "paris",
    "sydney", "melbourne", "dublin", "warsaw", "krakow", "tel aviv", "tokyo",
    "beijing", "shanghai", "seoul", "singapore city",
]


def _location_filter(jobs: list[dict], resume: dict) -> list[dict]:
    """Remove jobs explicitly located in a foreign country.

    Passes through:
      - Remote jobs (is_remote=True or "remote" in location)
      - Jobs with no location (can't tell — let scorer decide)
      - Jobs where the user's country appears in the location
    Removes:
      - Jobs whose location contains a known foreign country/city token
        and does NOT contain the user's country.
    """
    prefs = resume.get("preferences", {})
    user_country = prefs.get("country", "United States").lower()
    country_tokens = set(user_country.split())  # {"united", "states"}

    kept, removed = [], 0
    for j in jobs:
        loc = j.get("location", "").lower().strip()
        is_remote = str(j.get("is_remote", "")).lower() in ("true", "1", "yes", "remote")

        # Always keep remote jobs
        if is_remote or "remote" in loc:
            kept.append(j)
            continue

        # No location info — keep (scorer will handle it)
        if not loc:
            kept.append(j)
            continue

        # Keep if user's country is mentioned
        if any(tok in loc for tok in country_tokens):
            kept.append(j)
            continue

        # Reject if a known foreign token appears
        if any(foreign in loc for foreign in _FOREIGN_COUNTRY_TOKENS):
            log.debug(f"Location filter: dropped '{j.get('title')}' @ '{j.get('company')}' — location '{j.get('location')}'")
            removed += 1
            continue

        kept.append(j)

    if removed:
        log.info(f"Location filter removed {removed} jobs outside {prefs.get('country', 'United States')}")
    return kept


# ─── Source 7: Proactive LinkedIn Hiring Post Leads ────────────

def _scrape_linkedin_hiring_posts(resume: dict) -> list[dict]:
    """Convert LinkedIn hiring posts from the Playwright scraper cache into job leads.

    Reads data/linkedin_posts.json (written by signals phase or a prior run).
    Returns empty list if no cache exists yet — signals phase will populate it.
    """
    from pathlib import Path as _Path
    cache_file = _Path(__file__).resolve().parent.parent / "data" / "linkedin_posts.json"
    if not cache_file.exists():
        log.debug("LinkedIn posts cache not found — will be populated during signals phase")
        return []

    import json as _json
    try:
        pool: dict = _json.loads(cache_file.read_text())
    except Exception as e:
        log.warning(f"LinkedIn posts cache read error: {e}")
        return []

    target_roles = resume.get("target_roles", [])
    now = datetime.now().isoformat()
    jobs = []
    seen_urls: set[str] = set()

    for company, posts in pool.items():
        for post in posts:
            url = post.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            poster = post.get("poster", "")
            snippet = post.get("snippet", "")
            if not poster or not company:
                continue

            inferred_title = f"Hiring post by {poster}"
            for role in target_roles:
                if role.lower() in snippet.lower():
                    inferred_title = role
                    break

            jobs.append({
                "id": _id(company, inferred_title, "hiring_post"),
                "title": inferred_title,
                "company": company.title(),
                "location": "", "is_remote": "",
                "job_url": url,
                "salary_min": "", "salary_max": "", "job_type": "",
                "description": f"HIRING POST by {poster}: {snippet}",
                "date_posted": "",
                "source": "linkedin_hiring_post",
                "founder_email": "",
                "scraped_at": now,
                "_poster_name": poster,
                "_poster_headline": "",
                "_post_url": url,
                "_post_snippet": snippet,
            })

    if jobs:
        log.info(f"LinkedIn hiring posts: {len(jobs)} leads from cache")
    return jobs


# ─── Source 8: Blind Offer Feed (Proactive) ────────────────────

def _scrape_blind_offers(resume: dict) -> list[dict]:
    """Scrape Blind "Offer Evaluation" posts to find companies actively closing candidates.

    Key insight: "Airbnb vs Google vs Netflix E5 MLE" means ALL THREE are hiring NOW.
    We extract every company mentioned and treat each as a confirmed-hiring signal.
    """
    jobs = []
    now = datetime.now().isoformat()
    seen_companies: set[str] = set()

    # Build queries using Blind level terms (Groq inferred these)
    level_terms = resume.get("blind_level_terms", [])
    target_roles = resume.get("target_roles", [])

    queries = []
    for term in level_terms[:4]:
        queries.append(f'site:teamblind.com "offer evaluation" "{term}" 2026')
        queries.append(f'site:teamblind.com "offer" "{term}" "vs" 2026')

    # Generic role queries
    for role in target_roles[:2]:
        short_role = role.split()[-1] if role.split() else role  # "Engineer", "Director"
        queries.append(f'site:teamblind.com "offer evaluation" "{short_role}" 2026')

    # Broad catch-all
    queries.append('site:teamblind.com "offer evaluation" "engineer" 2026')

    log.info(f"Blind offers: searching {len(queries)} queries")

    for q in queries:
        try:
            results = _search(q, num=5)
            for r in results:
                title = r.get("title", "")
                snippet = r.get("snippet", "")

                # Extract company names from title
                # Patterns: "Airbnb vs Google vs Netflix for E5 MLE"
                #           "Meta E4 offer evaluation"
                #           "Databricks offer evaluation"
                companies = _extract_companies_from_blind(title, snippet)

                for company in companies:
                    if company.lower() in seen_companies:
                        continue
                    seen_companies.add(company.lower())

                    # Extract comp data from snippet
                    comp_data = ""
                    for kw in ["TC", "Base", "RSU", "equity", "$", "comp"]:
                        if kw.lower() in snippet.lower():
                            comp_data = snippet[:250]
                            break

                    jobs.append({
                        "id": _id(company, "blind_confirmed_hiring", ""),
                        "title": f"[Blind] Confirmed hiring at {company}",
                        "company": company,
                        "location": "", "is_remote": "",
                        "job_url": r.get("url", ""),
                        "salary_min": "", "salary_max": "", "job_type": "",
                        "description": f"BLIND OFFER POST: {title}\n\nComp data: {comp_data}" if comp_data else f"BLIND OFFER POST: {title}",
                        "date_posted": "",
                        "source": "blind_offer_feed",
                        "founder_email": "",
                        "scraped_at": now,
                    })

            time.sleep(1)
        except Exception as e:
            log.debug(f"Blind offer query failed: {e}")
            continue

    if jobs:
        log.info(f"Blind offers: {len(jobs)} companies confirmed hiring")
    else:
        log.debug("Blind offers: no companies found")
    return jobs


def _extract_companies_from_blind(title: str, snippet: str) -> list[str]:
    """Extract company names from Blind post titles.
    Handles: 'Airbnb vs Google vs Netflix for E5 MLE'
             'Meta E4 offer evaluation'
             'Databricks offer evaluation'
    """
    # Known major tech companies for matching
    KNOWN = {
        "google", "meta", "amazon", "apple", "microsoft", "netflix",
        "airbnb", "uber", "lyft", "stripe", "databricks", "snowflake",
        "openai", "anthropic", "nvidia", "salesforce", "oracle", "ibm",
        "coinbase", "robinhood", "square", "block", "snap", "pinterest",
        "twitter", "x", "linkedin", "tiktok", "bytedance", "palantir",
        "cloudflare", "datadog", "confluent", "mongodb", "elastic",
        "splunk", "vmware", "adobe", "intuit", "paypal", "shopify",
        "doordash", "instacart", "roblox", "spotify", "reddit",
        "dropbox", "figma", "vercel", "supabase", "hashicorp",
        "atlassian", "twilio", "okta", "crowdstrike", "zscaler",
        "palo alto", "fortinet", "servicenow", "workday", "plaid",
        "rippling", "gusto", "toast", "brex", "ramp", "deel",
        "notion", "airtable", "monday", "asana", "clickup",
        "temporal", "cockroach", "planetscale", "neon",
    }

    companies = []
    text = title.lower()

    # Pattern 1: "X vs Y vs Z" — split on "vs"
    if " vs " in text:
        parts = text.split(" vs ")
        for part in parts:
            # Clean: take first word(s) before level terms
            cleaned = part.strip()
            for stop in ["for ", "e3", "e4", "e5", "e6", "e7", "l3", "l4", "l5", "l6", "l7",
                          "ic3", "ic4", "ic5", "ic6", "sde", "swe", "mle", "offer", "|"]:
                if stop in cleaned:
                    cleaned = cleaned.split(stop)[0].strip()
            # Match against known companies
            for known in KNOWN:
                if known in cleaned:
                    companies.append(known.title())
                    break
            else:
                # Not in known list but might still be a company name
                word = cleaned.split()[0] if cleaned.split() else ""
                if word and len(word) > 2 and word not in ("the", "for", "and", "at", "my"):
                    companies.append(word.title())

    # Pattern 2: "Company offer evaluation" or "Company Level"
    if not companies:
        for known in KNOWN:
            if known in text:
                companies.append(known.title())

    # Also check snippet for company names
    snippet_lower = snippet.lower()
    for known in KNOWN:
        if known in snippet_lower and known.title() not in companies:
            companies.append(known.title())

    return list(dict.fromkeys(companies))[:5]  # dedup, max 5


import time


# ─── Source 9: RSS Job Feeds ───────────────────────────────────

_RSS_FEEDS = [
    # (url, source_label, is_json)
    ("https://weworkremotely.com/remote-jobs.rss",     "weworkremotely",        False),
    ("https://himalayas.app/jobs/rss",                  "himalayas",             False),
    ("https://www.realworkfromanywhere.com/rss.xml",    "realworkfromanywhere",  False),
    ("https://remoteok.com/remote-jobs.rss",            "remoteok",              False),
    ("https://remotive.com/api/remote-jobs",            "remotive",              True),
]


def _scrape_rss_feeds(resume: dict) -> list[dict]:
    """Scrape remote job RSS/JSON feeds. No bot detection — feeds are public APIs.
    Uses BeautifulSoup for XML parsing (handles broken namespaces/entities gracefully).
    """
    skills   = [s.lower() for s in resume.get("technical_skills", [])]
    roles    = [r.lower() for r in resume.get("target_roles", [])]
    keywords = skills + roles

    jobs = []
    now = datetime.now().isoformat()

    for feed_url, source, is_json in _RSS_FEEDS:
        try:
            resp = _SESSION.get(feed_url, timeout=15)
            if resp.status_code != 200:
                log.debug(f"RSS {source}: HTTP {resp.status_code}")
                continue

            count = 0

            # ── Remotive: JSON API ──────────────────────────────
            if is_json:
                for item in resp.json().get("jobs", []):
                    title   = (item.get("title") or "").strip()
                    company = (item.get("company_name") or "").strip()
                    desc    = BeautifulSoup(item.get("description") or "", "html.parser").get_text()[:5000]
                    url     = item.get("url") or ""
                    tags    = " ".join(item.get("tags") or []).lower()
                    if not any(kw in (title + " " + desc + " " + tags).lower() for kw in keywords):
                        continue
                    jobs.append({
                        "id": _id(company, title, "Remote"),
                        "title": title, "company": company,
                        "location": item.get("candidate_required_location") or "Remote",
                        "is_remote": "True", "job_url": url,
                        "salary_min": "", "salary_max": "", "job_type": "",
                        "description": desc,
                        "date_posted": (item.get("publication_date") or "")[:10],
                        "source": source, "founder_email": "", "scraped_at": now,
                    })
                    count += 1

            # ── All other feeds: RSS/XML (parsed by BeautifulSoup) ──
            else:
                soup = BeautifulSoup(resp.content, "xml")
                if not soup.find("item"):
                    # Some feeds use Atom <entry> instead of <item>
                    soup = BeautifulSoup(resp.content, "html.parser")

                for item in soup.find_all("item"):
                    def _tag(name):
                        el = item.find(name)
                        return el.get_text(strip=True) if el else ""

                    title   = _tag("title")
                    url     = _tag("link") or _tag("guid")
                    company = _tag("author") or _tag("dc:creator") or ""
                    pub     = _tag("pubdate") or _tag("pubDate") or ""

                    # WeWorkRemotely encodes company in title: "Company: Role"
                    if source == "weworkremotely" and ": " in title:
                        parts   = title.split(": ", 1)
                        company = parts[0].strip()
                        title   = parts[1].strip()

                    # Description: prefer content:encoded
                    desc_raw = ""
                    for tag in ("content:encoded", "content", "description"):
                        el = item.find(tag)
                        if el:
                            desc_raw = el.get_text(separator=" ")
                            break

                    desc = desc_raw[:5000]
                    if not any(kw in (title + " " + desc).lower() for kw in keywords):
                        continue

                    jobs.append({
                        "id": _id(company or source, title, "Remote"),
                        "title": title, "company": company or source,
                        "location": "Remote", "is_remote": "True",
                        "job_url": url,
                        "salary_min": "", "salary_max": "", "job_type": "",
                        "description": desc,
                        "date_posted": pub[:10],
                        "source": source, "founder_email": "", "scraped_at": now,
                    })
                    count += 1

            if count:
                log.info(f"RSS {source}: {count} matching listings")
            else:
                log.debug(f"RSS {source}: 0 matches (feed has {len(soup.find_all('item') if not is_json else [])} items)")

        except Exception as e:
            log.warning(f"RSS {source} failed: {e}")

    log.info(f"RSS feeds total: {len(jobs)} matching listings")
    return jobs


# ─── Source 10: Levels.fyi Jobs ───────────────────────────────

def _get_level_equivalencies(resume: dict) -> list[str]:
    """Get level equivalencies from Levels.fyi to expand search terms.
    
    For example, if user is Staff Engineer, find what that level is called
    at different companies to search for more relevant job titles.
    """
    current_level = resume.get("current_level", "staff")
    target_roles = resume.get("target_roles", [])
    
    # Map common levels to search terms
    level_mappings = {
        "junior": ["junior", "associate", "entry level", "l1", "l2"],
        "mid": ["mid", "senior associate", "l3", "l4"],
        "senior": ["senior", "lead", "l4", "l5", "e4", "e5"],
        "staff": ["staff", "principal", "l5", "l6", "e5", "e6"],
        "principal": ["principal", "staff", "senior staff", "l6", "l7", "e6", "e7"],
        "director": ["director", "engineering director", "d1", "d2"],
        "senior_director": ["senior director", "vp", "head of"],
        "vp": ["vp", "vice president", "head of", "svp"],
        "svp": ["svp", "senior vp", "chief"],
        "cto": ["cto", "chief technology officer"]
    }
    
    expanded_roles = set(target_roles)  # Start with original roles
    
    # Add level-specific variations
    if current_level in level_mappings:
        expanded_roles.update(level_mappings[current_level])
    
    # Try to get company-specific equivalencies from levels.fyi
    # This is a simplified approach - in practice, we'd need to scrape
    # multiple company salary pages to build equivalency mappings
    
    return list(expanded_roles)


def _levels_decrypt(payload_b64: str) -> dict:
    """Decrypt levels.fyi API response (AES-ECB + zlib)."""
    import base64
    import hashlib
    import zlib
    try:
        from Cryptodome.Cipher import AES
    except ImportError:
        from Crypto.Cipher import AES
    key = base64.b64encode(hashlib.md5(b"levelstothemoon!!").digest()).decode("ascii")[:16].encode()
    ct = base64.b64decode(payload_b64)
    decrypted = AES.new(key, AES.MODE_ECB).decrypt(ct)
    return json.loads(zlib.decompress(decrypted).decode("utf-8"))


_LEVELS_API = "https://api.levels.fyi/v1/job/search"
_LEVELS_SESSION = requests.Session()
_LEVELS_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, */*",
    "Referer": "https://www.levels.fyi/",
    "Origin": "https://www.levels.fyi",
})


def _levels_api_search(search_text: str, location_slug: str = "united-states",
                        work_arrangements: list | None = None, offset: int = 0,
                        limit: int = 25, posted_days: int = 30) -> tuple[list, int]:
    """Call levels.fyi REST API. Returns (companies_list, total_matching_jobs)."""
    params: list[tuple[str, str]] = [
        ("searchText", search_text),
        ("locationSlugs[]", location_slug),
        ("limit", str(limit)),
        ("offset", str(offset)),
        ("sortBy", "date_published"),
        ("postedAfterValue", str(posted_days)),
        ("postedAfterTimeType", "days"),
    ]
    for wa in (work_arrangements or ["remote", "hybrid"]):
        params.append(("workArrangements[]", wa))

    try:
        resp = _LEVELS_SESSION.get(_LEVELS_API, params=params, timeout=15)
        if resp.status_code != 200:
            log.debug(f"levels.fyi API returned {resp.status_code}")
            return [], 0
        data = resp.json()
        payload = data.get("payload", "")
        if not payload:
            return [], 0
        decoded = _levels_decrypt(payload)
        results = decoded.get("results", [])
        total = decoded.get("totalMatchingJobs", 0)
        return results, total
    except Exception as e:
        log.debug(f"levels.fyi API error: {e}")
        return [], 0


def _scrape_levels_jobs(resume: dict) -> list[dict]:
    """Fetch jobs from levels.fyi REST API (api.levels.fyi/v1/job/search).

    Uses proper AES-ECB decryption of the API response.
    Returns salary data (minBaseSalary/maxBaseSalary) directly with each job.
    Paginates across up to 4 pages (100 companies, ~300 jobs max).
    """
    now = datetime.now().isoformat()
    target_roles = resume.get("target_roles", [])
    prefs = resume.get("preferences", {})
    is_remote = prefs.get("remote", True)
    country = prefs.get("country", "United States")
    hours_old = resume.get("scout", {}).get("hours_old", 168)
    posted_days = max(1, hours_old // 24)

    # Location slug
    location = resume.get("location", "")
    if "san francisco" in location.lower() or "bay area" in location.lower():
        location_slug = "san-francisco-bay-area"
    elif "new york" in location.lower():
        location_slug = "new-york-city"
    elif "seattle" in location.lower():
        location_slug = "seattle"
    elif "united states" in country.lower() or "usa" in country.lower():
        location_slug = "united-states"
    else:
        location_slug = "united-states"

    work_arrangements = ["remote"] if is_remote else ["remote", "hybrid", "office"]

    jobs: list[dict] = []
    seen_ids: set[str] = set()

    search_terms = target_roles[:3]  # top 3 roles
    if not search_terms:
        search_terms = ["Software Engineer"]

    for term in search_terms:
        log.info(f"Levels.fyi API: '{term}' in {location_slug} (last {posted_days}d)")
        for page in range(4):  # up to 4 pages = 100 companies
            offset = page * 25
            results, total = _levels_api_search(
                search_text=term,
                location_slug=location_slug,
                work_arrangements=work_arrangements,
                offset=offset,
                posted_days=posted_days,
            )
            if not results:
                break
            if page == 0:
                log.info(f"  Total matching: {total}")

            for company_group in results:
                company = company_group.get("companyName", "")
                for j in company_group.get("jobs", []):
                    job_id = str(j.get("id", ""))
                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    title = j.get("title", "")
                    locs = j.get("locations", [])
                    loc_str = locs[0] if locs else location_slug.replace("-", " ").title()
                    arrangement = j.get("workArrangement", "")
                    min_base = j.get("minBaseSalary") or ""
                    max_base = j.get("maxBaseSalary") or ""
                    apply_url = j.get("applicationUrl", f"https://www.levels.fyi/jobs?searchText={term}")
                    posted = j.get("postingDate", "")[:10] if j.get("postingDate") else ""

                    jobs.append({
                        "id": _id(company, title, job_id),
                        "title": title,
                        "company": company,
                        "location": loc_str,
                        "is_remote": "true" if arrangement == "remote" else "false",
                        "job_url": apply_url,
                        "salary_min": str(min_base),
                        "salary_max": str(max_base),
                        "job_type": "fulltime",
                        "description": f"Levels.fyi: {title} at {company}. Salary: ${min_base}–${max_base}",
                        "date_posted": posted,
                        "source": "levels_fyi_jobs",
                        "founder_email": "",
                        "scraped_at": now,
                    })

            if offset + 25 >= min(total, 100):
                break  # don't over-paginate

    log.info(f"Levels.fyi API: {len(jobs)} jobs from {len(search_terms)} search terms")
    return jobs


# ─── Source 11: TeamBlind Job Board ───────────────────────────
#
# PROTOCOL (reverse-engineered from /_next/static/chunks/bb905db63220295b.js
# and /_next/static/chunks/9679ba756ef6a504.js):
#
# The site uses a custom "encryptedClientFetch" pattern:
#   1. Generate a random 256-bit hex string as the session AES key.
#   2. AES-encrypt the JSON request body (always "{}") using sjcl (CCM mode,
#      PBKDF2 key derivation, 10 000 iterations, 128-bit AES).  The output is
#      a JSON string like {"iv":...,"v":1,"iter":10000,"ks":128,"ts":64,
#      "mode":"ccm","adata":"","cipher":"aes","salt":...,"ct":...}.
#   3. RSA-encrypt the raw hex key with the site's PKCS#1 (1024-bit) public key.
#   4. POST {"payload": <sjcl_output>, "encClientKey": <rsa_b64>} to the API.
#   5. Response is the JSON string of the sjcl cipher; decrypt with the same key.
#
# All filter parameters travel as URL query-string parameters (not body).
# The body is always the encrypted empty object "{}".
#
# Discovered endpoints (all accept POST via encryptedClientFetch):
#   GET-style: POST /api/jobs?[params]
#     searchKeyword    string        free-text search
#     page             int           0-based page number (50 jobs/page)
#     offset           int           absolute offset (page * 50)
#     remoteOnly       bool          true/false
#     datePosted       int           days: -1=any, 1, 7, 14, 30
#     yearsOfExperience string       "min-max" e.g. "3-7"
#     salary           string        "min-max" e.g. "150000-300000"
#     locations        str (pipe-sep) location IDs e.g. "M807" or "M807|M819"
#     companies        str (pipe-sep) company IDs e.g. "100006"
#     companySizes     str (pipe-sep) e.g. "5,000+ employees"
#     skills           str (pipe-sep) skill IDs e.g. "546" (Python)
#
#   Response JSON fields: feeds (list), total (int), hasMore (bool),
#                         currentOffset (int), maxId (int)
#   Each feed item: type, title, companyName, companyId, location, highlights
#                   (includes salary range), id (job ID), isBookmarked,
#                   isPromoted, companyLogo, metadata
#
#   POST /api/jobs/suggestions/search-bar?keyword=<str>  -> [{id, name, type}]
#   POST /api/jobs/suggestions/location?keyword=<str>    -> [{id, name}]
#   POST /api/jobs/suggestions/company?keyword=<str>     -> [{id, name}]
#   POST /api/jobs/suggestions/skill?keyword=<str>       -> [{id, name}]
#   POST /api/jobs/bookmarks?page=<int>                  -> same as /api/jobs (auth req)
#
# RSA public key (PKCS#1, 1024-bit, PKCS1v15 padding):
_BLIND_RSA_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBITANBgkqhkiG9w0BAQEFAAOCAQ4AMIIBCQKCAQBOBw7Q2T0Wmb/qNPuNbk+f
ZWRbKgBwikJa2vJ5Ht+quwhLbvpUVOKwlNM93huIzkM5wWTRoVpLmczfCt3CyxBd
eU5PxY8JhXxHch/h41e/AgKXrOPFDJuH5T2V++Zw21ArC6rk3YFScNH9xOa0YXfY
x2RQxLM7hD7Bzy5mtxN5nqULxDhYWTeZT6aQw9Wii/0HBoePqgW77TpXcgQxJ5AP
bQQ7QlGdAFMWgjhFWret7cffGrd2lFn5RCgMU316UKf2CTkB4orcsiqCYJ76+LZJ
jLT7kk0ZWYk8Xnn7uwpiCMVipOmZS7cmX3MWiRhbQqkw1UGi2SWn2Ov7plwgx9CB
AgMBAAE=
-----END PUBLIC KEY-----"""

_BLIND_JOBS_PER_PAGE = 50


_SJCL_L = 2                     # sjcl default L value
_SJCL_NONCE_LEN = 15 - _SJCL_L  # 13 bytes nonce for AES-CCM

_blind_pub_key = None  # cached RSA key object


def _sjcl_b64e(b: bytes) -> str:
    return __import__("base64").b64encode(b).decode().rstrip("=")


def _sjcl_b64d(s: str) -> bytes:
    return __import__("base64").b64decode(s + "=" * ((-len(s)) % 4))


def _sjcl_encrypt(hex_key: str, plaintext: str) -> str:
    """Replicate sjcl.encrypt(hexKey, plaintext) — AES-CCM + PBKDF2-SHA256."""
    import os
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.ciphers.aead import AESCCM
    salt = os.urandom(8)
    iv   = os.urandom(_SJCL_NONCE_LEN)
    kdf  = PBKDF2HMAC(algorithm=hashes.SHA256(), length=16, salt=salt, iterations=10000)
    ct   = AESCCM(kdf.derive(hex_key.encode()), tag_length=8).encrypt(iv, plaintext.encode(), b"")
    return json.dumps({"iv": _sjcl_b64e(iv), "v": 1, "iter": 10000, "ks": 128, "ts": 64,
                       "mode": "ccm", "adata": "", "cipher": "aes",
                       "salt": _sjcl_b64e(salt), "ct": _sjcl_b64e(ct)}, separators=(",", ":"))


def _sjcl_decrypt(hex_key: str, sjcl_json_str: str) -> str:
    """Replicate sjcl.decrypt(hexKey, cipherObj) — AES-CCM + PBKDF2-SHA256."""
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.ciphers.aead import AESCCM
    d    = json.loads(sjcl_json_str)
    salt = _sjcl_b64d(d["salt"])
    iv   = _sjcl_b64d(d["iv"])[:_SJCL_NONCE_LEN]  # sjcl stores 16 bytes, uses first 13
    ct   = _sjcl_b64d(d["ct"])
    kdf  = PBKDF2HMAC(algorithm=hashes.SHA256(), length=d.get("ks", 128) // 8,
                      salt=salt, iterations=d.get("iter", 10000))
    return AESCCM(kdf.derive(hex_key.encode()), tag_length=d.get("ts", 64) // 8).decrypt(iv, ct, b"").decode()


def _blind_encrypted_fetch(path: str) -> dict | list | None:
    """POST to teamblind.com using the encryptedClientFetch protocol (pure Python).

    Protocol (reverse-engineered):
      1. Generate random 256-bit hex string as session key.
      2. sjcl.encrypt(hexKey, '{}')  →  AES-CCM/PBKDF2 JSON blob.
      3. RSA-PKCS1v15 encrypt hexKey with site's 1024-bit public key.
      4. POST {payload, encClientKey} — query params carry the real filters.
      5. Decrypt response: json.loads(sjcl.decrypt(hexKey, json.loads(body))).
    """
    import os, base64
    global _blind_pub_key

    try:
        from cryptography.hazmat.primitives.asymmetric import padding as rsa_padding
        from cryptography.hazmat.primitives.serialization import load_pem_public_key

        if _blind_pub_key is None:
            _blind_pub_key = load_pem_public_key(_BLIND_RSA_PUBLIC_KEY.strip().encode())

        hex_key        = os.urandom(32).hex()
        encrypted_body = _sjcl_encrypt(hex_key, "{}")
        enc_client_key = base64.b64encode(
            _blind_pub_key.encrypt(hex_key.encode(), rsa_padding.PKCS1v15())
        ).decode()

        resp = requests.post(
            f"https://www.teamblind.com{path}",
            data=json.dumps({"payload": encrypted_body, "encClientKey": enc_client_key}),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, */*",
                "Origin": "https://www.teamblind.com",
                "Referer": "https://www.teamblind.com/jobs",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
            timeout=20,
        )
        if resp.status_code != 200:
            log.debug(f"Blind API {path} → HTTP {resp.status_code}")
            return None

        inner = json.loads(resp.text)   # outer JSON string wrapping the sjcl cipher JSON
        return json.loads(_sjcl_decrypt(hex_key, inner))

    except Exception as e:
        log.debug(f"Blind encrypted fetch failed for {path}: {e}")
        return None


def _scrape_teamblind_jobs(resume: dict) -> list[dict]:
    """Fetch job listings from TeamBlind Job Board via the encrypted REST API.

    TeamBlind's /api/jobs endpoint uses a custom encryption layer (sjcl AES-CCM
    + RSA-PKCS1v15) for all requests.  See the protocol comment above for details.
    Returns up to 200 listings (4 pages) matching the resume's target roles.

    Requires Node.js with /tmp/node_modules/sjcl and /tmp/node_modules/node-rsa.
    Install once with: npm install --prefix /tmp sjcl node-rsa
    """
    now = datetime.now().isoformat()
    target_roles = resume.get("target_roles", [])
    prefs = resume.get("preferences", {})
    is_remote = prefs.get("remote", True)
    scout_cfg = resume.get("scout", {})
    max_results = scout_cfg.get("max_results", 50)

    jobs: list[dict] = []
    seen_ids: set[str] = set()

    # Build search terms: use first 3 target roles + "software engineer" catch-all
    search_terms = list(dict.fromkeys(target_roles[:3] + ["software engineer"]))

    for term in search_terms:
        pages_to_fetch = min(4, (max_results + _BLIND_JOBS_PER_PAGE - 1) // _BLIND_JOBS_PER_PAGE)
        for page in range(pages_to_fetch):
            offset = page * _BLIND_JOBS_PER_PAGE
            params = f"searchKeyword={requests.utils.quote(term)}&page={page}&offset={offset}"
            if is_remote:
                params += "&remoteOnly=true"

            data = _blind_encrypted_fetch(f"/api/jobs?{params}")
            if data is None or "feeds" not in data:
                log.debug(f"TeamBlind: no data for '{term}' page {page}")
                break

            feeds: list[dict] = data.get("feeds", [])
            for item in feeds:
                job_id = str(item.get("id", ""))
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                title = item.get("title", "").strip()
                company = item.get("companyName", "").strip()
                location = item.get("location", "").strip()
                highlights = item.get("highlights", [])

                # Parse salary range from highlights (e.g. "$176K-$264K")
                salary_min = salary_max = ""
                for h in highlights:
                    if h.startswith("$") and "-" in h:
                        parts = h.replace("$", "").replace("K", "000").split("-")
                        try:
                            salary_min = str(int(float(parts[0].replace(",", ""))))
                            salary_max = str(int(float(parts[1].replace(",", ""))))
                        except (ValueError, IndexError):
                            pass
                        break

                skills_str = ", ".join(h for h in highlights if not h.startswith("$") and h != "Remote")

                is_remote_job = (
                    "remote" in location.lower()
                    or any(h.lower() == "remote" for h in highlights)
                )

                jobs.append({
                    "id": _id(company, title, location) if not job_id else f"tblind_{job_id}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "is_remote": str(is_remote_job),
                    "job_url": f"https://www.teamblind.com/jobs/{job_id}",
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                    "job_type": "",
                    "description": f"Skills/highlights: {skills_str}" if skills_str else "",
                    "date_posted": "",
                    "source": "teamblind",
                    "founder_email": "",
                    "scraped_at": now,
                })

            has_more = data.get("hasMore", False)
            if not has_more or len(jobs) >= max_results:
                break

        if len(jobs) >= max_results:
            break

    log.info(f"TeamBlind: {len(jobs)} job listings")
    return jobs


# ─── Main entry point ─────────────────────────────────────────

def run_scout(resume: dict) -> int:
    """Run all scout sources. Returns count of new jobs added to CSV."""
    log.info("=" * 50)
    log.info("PHASE 1: DISCOVER")
    log.info("=" * 50)

    existing = _existing_ids()
    log.info(f"Existing jobs in CSV: {len(existing)}")

    gh = resume.get("scout", {}).get("greenhouse_boards", [])
    lv = resume.get("scout", {}).get("lever_boards", [])

    # All sources are independent — run in parallel
    sources = {
        "jobspy":          lambda: _scrape_jobspy(resume),
        "google_careers":  lambda: _scrape_google(resume),
        "greenhouse":      lambda: _scrape_greenhouse(gh) if gh else [],
        "lever":           lambda: _scrape_lever(lv) if lv else [],
        "yc":              lambda: _scrape_yc_jobs(resume.get("target_roles", [])),
        "hn_hiring":       lambda: _scrape_hn_hiring(resume.get("technical_skills", ["python"])),
        "wellfound":       lambda: _scrape_wellfound(resume.get("target_roles", [])),
        "linkedin_posts":  lambda: _scrape_linkedin_hiring_posts(resume),
        "blind_feed":      lambda: _scrape_blind_offers(resume),
        "rss_feeds":       lambda: _scrape_rss_feeds(resume),
        "levels_fyi":      lambda: _scrape_levels_jobs(resume),
        "teamblind":       lambda: _scrape_teamblind_jobs(resume),
    }

    all_jobs: list[dict] = []
    log.info(f"Scraping {len(sources)} sources in parallel (5 workers)...")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fn): name for name, fn in sources.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                jobs = future.result()
                all_jobs.extend(jobs)
                log.info(f"  ✓ {name}: {len(jobs)} jobs")
            except Exception as e:
                log.warning(f"  ✗ {name} failed: {e}")

    # Dedup + filter
    seen = set()
    new_jobs = []
    levels_jobs_kept = 0
    for j in all_jobs:
        jid = j["id"]
        if jid not in existing and jid not in seen:
            seen.add(jid)
            new_jobs.append(j)
            if j.get("source") == "levels_fyi_jobs":
                levels_jobs_kept += 1

    log.info(f"After dedup: {len(new_jobs)} jobs total, {levels_jobs_kept} levels.fyi jobs")

    new_jobs = _keyword_filter(new_jobs, resume.get("keywords_exclude", []))
    new_jobs = _location_filter(new_jobs, resume)

    levels_jobs_after_filter = sum(1 for j in new_jobs if j.get("source") == "levels_fyi_jobs")
    log.info(f"After keyword+location filter: {len(new_jobs)} jobs total, {levels_jobs_after_filter} levels.fyi jobs")

    # Append to CSV
    file_exists = JOBS_CSV.exists()
    log.info(f"About to write {len(new_jobs)} jobs to CSV. Levels.fyi jobs: {sum(1 for j in new_jobs if j.get('source') == 'levels_fyi_jobs')}")
    
    with open(JOBS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        # Strip extra fields (like _poster_name) before writing to CSV
        levels_jobs_written = 0
        for j in new_jobs:
            if j.get("source") == "levels_fyi_jobs":
                levels_jobs_written += 1
            # Ensure all fields are strings and clean them
            row = {}
            for k in CSV_FIELDS:
                value = j.get(k, "")
                if not isinstance(value, str):
                    value = str(value)
                # Remove newlines and normalize whitespace
                value = value.replace('\n', ' ').replace('\r', ' ').strip()
                row[k] = value
            try:
                writer.writerow(row)
            except Exception as e:
                log.warning(f"Failed to write job {j.get('id', 'unknown')}: {e}")
                continue
        
        log.info(f"Levels.fyi jobs written to CSV: {levels_jobs_written}")

    log.info(f"New unique jobs: {len(new_jobs)}")
    log.info(f"Total in CSV: {len(existing) + len(new_jobs)}")
    return len(new_jobs)

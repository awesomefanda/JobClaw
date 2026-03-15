"""Phase 1 — DISCOVER: aggregate job leads from every source.

Sources:
  1. JobSpy (Indeed, LinkedIn, Google Jobs, Glassdoor, ZipRecruiter)
  2. Greenhouse boards (public JSON API)
  3. Lever boards (public JSON API)
  4. Y Combinator "Work at a Startup" (workatastartup.com)
  5. Hacker News "Who's Hiring" monthly thread
  6. Wellfound (AngelList) role pages

Each source is wrapped in try/except — if one fails we log and continue.
Output: data/jobs.csv (cumulative, deduped)
"""
import csv
import hashlib
import re
import requests
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
    platforms = scout.get("platforms", ["indeed", "linkedin", "google"])
    hours_old = scout.get("hours_old", 72)
    max_results = scout.get("max_results", 50)
    is_remote = resume.get("preferences", {}).get("remote", True)
    location = "" if is_remote else resume.get("location", "")

    jobs = []
    for term in resume.get("target_roles", ["software engineer"]):
        log.info(f"JobSpy: searching '{term}' on {platforms}")
        try:
            df = scrape_jobs(
                site_name=platforms,
                search_term=term,
                location=location,
                is_remote=is_remote,
                results_wanted=max_results,
                hours_old=hours_old,
                country_indeed="USA",
                description_format="markdown",
            )
        except Exception as e:
            log.warning(f"JobSpy error for '{term}': {e}")
            continue

        if df is None or df.empty:
            log.debug(f"JobSpy: no results for '{term}'")
            continue

        now = datetime.now().isoformat()
        for _, row in df.iterrows():
            company = str(row.get("company", "") or "").strip()
            title = str(row.get("title", "") or "").strip()
            city = str(row.get("city", "") or "")
            state = str(row.get("state", "") or "")
            loc = f"{city}, {state}".strip(", ") if city or state else ""

            jobs.append({
                "id": _id(company, title, loc),
                "title": title, "company": company, "location": loc,
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


# ─── Source 7: Proactive LinkedIn Hiring Post Leads ────────────

def _scrape_linkedin_hiring_posts(resume: dict) -> list[dict]:
    """Find LinkedIn posts from hiring managers PROACTIVELY.

    Key insight: a Director posting "I'm hiring" is a lead for Staff/Principal
    roles even if no job listing exists. We search by the titles of people who
    HIRE at the user's level, not the user's own title.
    """
    jobs = []
    now = datetime.now().isoformat()
    seen_urls: set[str] = set()

    # Get hiring manager titles from resume (Groq inferred these)
    hm_titles = resume.get("hm_titles_above_me", [])
    target_kw = resume.get("target_keywords", resume.get("target_roles", []))
    target_roles = resume.get("target_roles", [])

    # Build queries — search for people above your level saying "hiring"
    queries = []

    # Queries based on who hires you (most valuable)
    for hm in hm_titles[:3]:
        queries.append(f'site:linkedin.com/posts "{hm}" "hiring" 2026')
        queries.append(f'site:linkedin.com/posts "{hm}" "building my team" 2026')

    # Queries based on your target roles
    for kw in target_kw[:4]:
        queries.append(f'site:linkedin.com/posts "hiring" "{kw}" 2026')
        queries.append(f'site:linkedin.com/posts "open role" "{kw}"')

    # Generic high-signal phrases
    for role in target_roles[:2]:
        queries.append(f'site:linkedin.com/posts "join my team" "{role}"')
        queries.append(f'site:linkedin.com/posts "DM me" "{role}" hiring')
        queries.append(f'site:linkedin.com/posts "looking for" "{role}"')

    log.info(f"LinkedIn hiring posts: searching {len(queries)} queries")

    for q in queries:
        try:
            results = _search(q, num=5)
            for r in results:
                url = r.get("url", "")
                if url in seen_urls or "linkedin.com" not in url:
                    continue
                seen_urls.add(url)

                title_raw = r.get("title", "")
                snippet = r.get("snippet", "")

                # Extract poster name
                poster = ""
                if " on LinkedIn" in title_raw:
                    poster = title_raw.split(" on LinkedIn")[0].strip()
                elif " - " in title_raw:
                    poster = title_raw.split(" - ")[0].strip()

                # Extract company from title/snippet
                company = ""
                for marker in ["@", " at ", "| "]:
                    if marker in title_raw:
                        parts = title_raw.split(marker)
                        if len(parts) > 1:
                            company = parts[-1].split("-")[0].split("|")[0].split(",")[0].strip()
                            break
                if not company:
                    # Try snippet
                    for marker in ["@", " at "]:
                        if marker in snippet:
                            parts = snippet.split(marker)
                            if len(parts) > 1:
                                company = parts[1].split(".")[0].split(",")[0].split(" ")[0].strip()
                                break

                if not poster or not company:
                    continue

                # This is a FIRST-CLASS LEAD — the poster is the contact
                inferred_title = f"Hiring post by {poster}"
                for role in target_roles:
                    if role.lower() in (title_raw + snippet).lower():
                        inferred_title = role
                        break

                jobs.append({
                    "id": _id(company, inferred_title, "hiring_post"),
                    "title": inferred_title,
                    "company": company,
                    "location": "", "is_remote": "",
                    "job_url": url,
                    "salary_min": "", "salary_max": "", "job_type": "",
                    "description": f"HIRING POST by {poster}: {snippet}",
                    "date_posted": "",
                    "source": "linkedin_hiring_post",
                    "founder_email": "",
                    "scraped_at": now,
                    # Extra fields for this source
                    "_poster_name": poster,
                    "_poster_headline": "",
                    "_post_url": url,
                    "_post_snippet": snippet,
                })

            time.sleep(1.5)  # Be polite to Google
        except Exception as e:
            log.debug(f"Hiring post query failed: {e}")
            continue

    if jobs:
        log.info(f"LinkedIn hiring posts: {len(jobs)} leads from {len(seen_urls)} unique posts")
    else:
        log.debug("LinkedIn hiring posts: no leads found")
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


# ─── Main entry point ─────────────────────────────────────────

def run_scout(resume: dict) -> int:
    """Run all scout sources. Returns count of new jobs added to CSV."""
    log.info("=" * 50)
    log.info("PHASE 1: DISCOVER")
    log.info("=" * 50)

    existing = _existing_ids()
    log.info(f"Existing jobs in CSV: {len(existing)}")

    all_jobs: list[dict] = []

    # Source 1: JobSpy (Indeed, LinkedIn, Google Jobs, Glassdoor, ZipRecruiter)
    all_jobs.extend(_scrape_jobspy(resume))

    # Source 2: Greenhouse boards
    gh = resume.get("scout", {}).get("greenhouse_boards", [])
    if gh:
        all_jobs.extend(_scrape_greenhouse(gh))

    # Source 3: Lever boards
    lv = resume.get("scout", {}).get("lever_boards", [])
    if lv:
        all_jobs.extend(_scrape_lever(lv))

    # Source 4: YC Work at a Startup
    all_jobs.extend(_scrape_yc_jobs(resume.get("target_roles", [])))

    # Source 5: Hacker News "Who's Hiring"
    all_jobs.extend(_scrape_hn_hiring(resume.get("technical_skills", ["python"])))

    # Source 6: Wellfound (AngelList)
    all_jobs.extend(_scrape_wellfound(resume.get("target_roles", [])))

    # Source 7: LinkedIn Hiring Posts (PROACTIVE — people saying "I'm hiring")
    all_jobs.extend(_scrape_linkedin_hiring_posts(resume))

    # Source 8: Blind Offer Feed (companies confirmed closing candidates NOW)
    all_jobs.extend(_scrape_blind_offers(resume))

    # Source 9: RSS job feeds (WeWorkRemotely, Himalayas, RealWorkFromAnywhere, RemoteOK, Remotive)
    all_jobs.extend(_scrape_rss_feeds(resume))

    # Dedup + filter
    seen = set()
    new_jobs = []
    for j in all_jobs:
        jid = j["id"]
        if jid not in existing and jid not in seen:
            seen.add(jid)
            new_jobs.append(j)

    new_jobs = _keyword_filter(new_jobs, resume.get("keywords_exclude", []))

    # Append to CSV
    file_exists = JOBS_CSV.exists()
    with open(JOBS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        # Strip extra fields (like _poster_name) before writing to CSV
        for j in new_jobs:
            row = {k: j.get(k, "") for k in CSV_FIELDS}
            writer.writerow(row)

    log.info(f"New unique jobs: {len(new_jobs)}")
    log.info(f"Total in CSV: {len(existing) + len(new_jobs)}")
    return len(new_jobs)

"""Phase 3b — CONTACTS: find who to reach out to + assign track.

1. Apollo People Search (FREE, no credits) → hiring managers
2. LinkedIn connections CSV → warm introductions
3. Field leads (resume-driven) → GitHub contributors, dev.to authors,
   Stack Overflow experts, Hacker News users in the same tech field
4. Cross-reference hiring posts from signals phase
5. Calculate action_score and assign Track A/B

Input:  data/signals.json + connections.csv
Output: data/enriched.json
"""
import json
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from jobclaw.logger import get_logger
from jobclaw import config

log = get_logger("contacts")

DATA = Path(__file__).resolve().parent.parent / "data"
SIGNALS_JSON = DATA / "signals.json"
ENRICHED_JSON = DATA / "enriched.json"
FIELD_LEADS_JSON = DATA / "field_leads.json"


# ─── Apollo People Search (FREE) ──────────────────────────────

def _apollo_search(company: str, hm_titles: list[str]) -> list[dict]:
    """Search Apollo for engineering managers/directors at company. FREE endpoint."""
    if not config.APOLLO_API_KEY:
        return []
    titles = hm_titles or [
        "Engineering Manager", "Director of Engineering",
        "VP Engineering", "Head of Engineering",
        "CTO", "Principal Engineer", "Staff Engineer",
    ]
    try:
        resp = requests.post(
            "https://api.apollo.io/api/v1/mixed_people/search",
            headers={
                "Content-Type": "application/json",
                "x-api-key": config.APOLLO_API_KEY,
            },
            json={
                "q_organization_name": company,
                "person_titles": titles,
                "person_seniorities": ["director", "vp", "manager", "senior"],
                "per_page": 5,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.debug(f"Apollo {company}: HTTP {resp.status_code}")
            return []
        people = []
        for p in resp.json().get("people", []):
            people.append({
                "name": p.get("name", ""),
                "title": p.get("title", ""),
                "linkedin_url": p.get("linkedin_url", ""),
                "city": p.get("city", ""),
                "source": "apollo",
            })
        return people
    except Exception as e:
        log.debug(f"Apollo search failed for {company}: {e}")
        return []


# ─── Resume-driven field leads ─────────────────────────────────

def _derive_lead_sources(resume: dict) -> dict:
    """Use Groq to identify repos, tags, and topics matching the resume.
    Falls back to skill extraction if Groq is unavailable.
    """
    skills = resume.get("technical_skills", [])
    domain = resume.get("domain_expertise", [])
    target_roles = resume.get("target_roles", [])

    fallback = {
        "github_repos": [],
        "devto_tags": [s.lower().replace(" ", "") for s in (skills + domain)[:4]],
        "stackoverflow_tags": [s.lower().replace(" ", "-") for s in skills[:4]],
        "hn_search_terms": target_roles[:2],
        "field_summary": "software engineer",
    }

    if not config.GROQ_API_KEY:
        return fallback

    from groq import Groq
    client = Groq(api_key=config.GROQ_API_KEY)
    prompt = f"""Analyze this software engineer's profile and return ONLY valid JSON:
{{
  "github_repos": ["owner/repo"],
  "devto_tags": ["tag"],
  "stackoverflow_tags": ["tag"],
  "hn_search_terms": ["term"],
  "field_summary": "2-3 word description"
}}

Rules:
- github_repos: 5-8 popular open-source repos central to their stack (real repos that exist)
- devto_tags: 4-6 dev.to article tags (lowercase, no spaces, e.g. "javascript" not "JavaScript")
- stackoverflow_tags: 4-6 Stack Overflow tags for their primary skills
- hn_search_terms: 3-4 short Hacker News search terms (technologies or domain areas)
- field_summary: e.g. "Python backend", "ML infra", "iOS mobile", "distributed systems"

Candidate:
Skills: {', '.join(skills)}
Domain: {', '.join(domain)}
Target roles: {', '.join(target_roles)}
Experience: {resume.get('experience_years', '')} years

Return ONLY JSON. No markdown fences."""

    try:
        resp = client.chat.completions.create(
            model=config.GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.2,
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        result = json.loads(text)
        log.info(f"Lead sources derived: {result.get('field_summary', '?')} | "
                 f"{len(result.get('github_repos', []))} repos, "
                 f"{len(result.get('stackoverflow_tags', []))} SO tags, "
                 f"{len(result.get('devto_tags', []))} dev.to tags")
        return result
    except Exception as e:
        log.warning(f"Lead source derivation failed: {e} — using fallback")
        return fallback


def _github_repo_contributors(repo: str) -> list[dict]:
    """Get top contributors from a GitHub repo (unauthenticated: 60/hr, token: 5000/hr)."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if config.GITHUB_TOKEN:
        headers["Authorization"] = f"token {config.GITHUB_TOKEN}"
    try:
        resp = requests.get(
            f"https://api.github.com/repos/{repo}/contributors",
            headers=headers,
            params={"per_page": 8, "anon": "false"},
            timeout=10,
        )
        if resp.status_code == 404:
            log.debug(f"GitHub repo not found: {repo}")
            return []
        if resp.status_code == 403:
            log.debug(f"GitHub rate limit hit for {repo}")
            return []
        if resp.status_code != 200:
            return []
        leads = []
        for c in resp.json():
            login = c.get("login", "")
            if not login or "[bot]" in login:
                continue
            leads.append({
                "name": login,
                "username": login,
                "title": "",
                "company": "",
                "bio": "",
                "profile_url": f"https://github.com/{login}",
                "source": "github_contributor",
                "source_detail": f"contributor to {repo}",
            })
        return leads
    except Exception as e:
        log.debug(f"GitHub contributors failed for {repo}: {e}")
        return []


def _github_enrich_user(login: str) -> dict:
    """Fetch name, company, bio from a GitHub user profile."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if config.GITHUB_TOKEN:
        headers["Authorization"] = f"token {config.GITHUB_TOKEN}"
    try:
        resp = requests.get(
            f"https://api.github.com/users/{login}",
            headers=headers,
            timeout=8,
        )
        if resp.status_code != 200:
            return {}
        u = resp.json()
        return {
            "name": u.get("name") or login,
            "company": (u.get("company") or "").lstrip("@").strip(),
            "bio": (u.get("bio") or "").strip(),
            "location": (u.get("location") or "").strip(),
            "followers": u.get("followers", 0),
        }
    except Exception:
        return {}


def _devto_authors(tag: str) -> list[dict]:
    """Get popular article authors from dev.to. Completely free, no auth needed."""
    try:
        resp = requests.get(
            "https://dev.to/api/articles",
            params={"tag": tag, "top": 30, "per_page": 8},
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        seen = set()
        leads = []
        for article in resp.json():
            user = article.get("user", {})
            username = user.get("username", "")
            if not username or username in seen:
                continue
            seen.add(username)
            leads.append({
                "name": user.get("name") or username,
                "username": username,
                "title": "",
                "company": "",
                "bio": "",
                "profile_url": f"https://dev.to/{username}",
                "source": "devto_author",
                "source_detail": f"popular #{tag} author on dev.to",
            })
        return leads
    except Exception as e:
        log.debug(f"dev.to authors failed for {tag}: {e}")
        return []


def _stackoverflow_top_users(tag: str) -> list[dict]:
    """Get top answerers for a Stack Overflow tag this month."""
    params = {"site": "stackoverflow", "pagesize": 8}
    try:
        resp = requests.get(
            f"https://api.stackexchange.com/2.3/tags/{tag}/top-answerers/month",
            params=params,
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        leads = []
        for item in resp.json().get("items", []):
            user = item.get("user", {})
            name = user.get("display_name", "")
            if not name:
                continue
            leads.append({
                "name": name,
                "username": str(user.get("user_id", "")),
                "title": "",
                "company": "",
                "bio": "",
                "profile_url": user.get("link", ""),
                "source": "stackoverflow_expert",
                "source_detail": f"top #{tag} answerer on Stack Overflow",
            })
        return leads
    except Exception as e:
        log.debug(f"Stack Overflow top users failed for {tag}: {e}")
        return []


def _hn_field_users(query: str) -> list[dict]:
    """Find active HN users discussing relevant technical topics."""
    try:
        resp = requests.get(
            "https://hn.algolia.com/api/v1/search",
            params={
                "query": query,
                "tags": "comment",
                "numericFilters": "created_at_i>1700000000",  # since Nov 2023
                "hitsPerPage": 20,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        seen = set()
        leads = []
        for hit in resp.json().get("hits", []):
            author = hit.get("author", "")
            if not author or author in seen:
                continue
            seen.add(author)
            leads.append({
                "name": author,
                "username": author,
                "title": "",
                "company": "",
                "bio": "",
                "profile_url": f"https://news.ycombinator.com/user?id={author}",
                "source": "hackernews_user",
                "source_detail": f"HN user active in '{query}'",
            })
            if len(leads) >= 8:
                break
        return leads
    except Exception as e:
        log.debug(f"HN search failed for '{query}': {e}")
        return []


def find_field_leads(resume: dict) -> list[dict]:
    """Find engineers in the same field who could provide referrals.

    Sources are derived from the resume — no hardcoded tech stacks.
    GitHub contributors → dev.to authors → Stack Overflow experts → HN users.
    """
    # Return cached leads if available (expensive to re-fetch every run)
    if FIELD_LEADS_JSON.exists():
        try:
            cached = json.loads(FIELD_LEADS_JSON.read_text())
            if cached:
                log.info(f"Using {len(cached)} cached field leads from field_leads.json")
                return cached
        except Exception:
            pass

    log.info("Deriving lead sources from resume...")
    sources = _derive_lead_sources(resume)

    all_leads: list[dict] = []
    seen: set[str] = set()

    def _add(leads: list[dict]):
        for lead in leads:
            key = f"{lead.get('source')}:{lead.get('username') or lead.get('name')}"
            if key not in seen:
                seen.add(key)
                all_leads.append(lead)

    # All 4 sources are independent — fetch in parallel
    repos     = sources.get("github_repos", [])[:5]
    devto_tags = sources.get("devto_tags", [])[:3]
    so_tags   = sources.get("stackoverflow_tags", [])[:2]
    hn_terms  = sources.get("hn_search_terms", [])[:2]

    def _fetch_github_repo(repo: str) -> list[dict]:
        raw = _github_repo_contributors(repo)
        enriched = []
        # Enrich top 3 contributors in parallel within each repo
        with ThreadPoolExecutor(max_workers=3) as ex:
            profile_futures = {ex.submit(_github_enrich_user, c["username"]): c for c in raw[:3]}
            for pf in as_completed(profile_futures):
                c = profile_futures[pf]
                profile = pf.result() or {}
                enriched.append({**c, **profile, "source": c["source"], "source_detail": c["source_detail"]})
        return enriched

    tasks: list[tuple[str, callable]] = []
    for repo in repos:
        tasks.append((f"github:{repo}", lambda r=repo: _fetch_github_repo(r)))
    for tag in devto_tags:
        tasks.append((f"devto:{tag}", lambda t=tag: _devto_authors(t)[:6]))
    for tag in so_tags:
        tasks.append((f"so:{tag}", lambda t=tag: _stackoverflow_top_users(t)[:5]))
    for term in hn_terms:
        tasks.append((f"hn:{term}", lambda t=term: _hn_field_users(t)[:6]))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fn): label for label, fn in tasks}
        for future in as_completed(futures):
            label = futures[future]
            try:
                results = future.result()
                _add(results)
                if results:
                    log.info(f"  {label}: {len(results)} leads")
            except Exception as e:
                log.debug(f"  {label} failed: {e}")

    log.info(f"Total field leads: {len(all_leads)} across "
             f"{len(repos)} GitHub repos, dev.to, Stack Overflow, HN")

    FIELD_LEADS_JSON.write_text(json.dumps(all_leads, indent=2))
    return all_leads


# ─── Action Score Calculator ──────────────────────────────────

def _action_score(
    fit_score: float,
    has_connection: bool,
    has_hiring_post: bool,
    hiring_post_is_connection: bool,
    has_apollo_contact: bool,
    has_founder_email: bool,
    blind_confirmed_hiring: bool,
    is_hiring_post_lead: bool,
    recently_funded: bool,
    no_layoffs: bool,
    has_red_flags: bool,
    had_layoffs: bool,
    levels_fyi_offers: bool,
    has_field_leads: bool = False,
) -> float:
    """Calculate actionability score — higher = more likely to get a response."""
    score = fit_score
    if has_founder_email:          score += 0.25  # HN/YC with direct email
    if has_connection:             score += 0.20
    if is_hiring_post_lead:        score += 0.20  # source IS a hiring post = named person
    if blind_confirmed_hiring:     score += 0.15
    if has_hiring_post:            score += 0.15
    if levels_fyi_offers:          score += 0.12  # Levels.fyi offer submissions = actively hiring
    if hiring_post_is_connection:  score += 0.10
    if has_apollo_contact:         score += 0.10
    if has_field_leads:            score += 0.08  # same-field engineers who could refer
    if recently_funded:            score += 0.05
    if no_layoffs:                 score += 0.05
    if has_red_flags:              score -= 0.10
    if had_layoffs:                score -= 0.15
    return round(score, 3)


# ─── Main ──────────────────────────────────────────────────────

def run_contacts(resume: dict) -> list[dict]:
    """Find contacts, assign tracks, calculate action scores."""
    log.info("=" * 50)
    log.info("PHASE 3b: CONTACTS & RANKING")
    log.info("=" * 50)

    if not SIGNALS_JSON.exists():
        log.warning("No signals.json — run signals phase first")
        return []

    scored = json.loads(SIGNALS_JSON.read_text())
    if not scored:
        log.info("No matches to enrich with contacts")
        return []

    # Load LinkedIn connections
    conn_index = config.load_connections()

    # Find field leads once for all jobs (resume-driven, cached after first run)
    field_leads = find_field_leads(resume)
    if field_leads:
        log.info(f"Field leads available: {len(field_leads)}")

    log.info(f"Enriching {len(scored)} matches with contacts")

    hm_titles = resume.get("hm_titles_above_me", [])

    # Pre-fetch Apollo for all unique companies in parallel (3 workers)
    unique_companies = list(dict.fromkeys(j.get("company", "") for j in scored if j.get("company")))
    apollo_cache: dict[str, list[dict]] = {}
    log.info(f"Fetching Apollo contacts for {len(unique_companies)} companies (3 parallel workers)...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        apollo_futures = {executor.submit(_apollo_search, c, hm_titles): c for c in unique_companies}
        for future in as_completed(apollo_futures):
            company = apollo_futures[future]
            try:
                apollo_cache[company] = future.result()
            except Exception:
                apollo_cache[company] = []

    for i, job in enumerate(scored):
        company = job.get("company", "")
        signals = job.get("signals", {})
        log.info(f"({i+1}/{len(scored)}) {company} — {job.get('title', '')}")

        # 1. LinkedIn connections (in-memory, instant)
        my_conns = config.find_connections_at(company, conn_index)
        job["my_connections"] = my_conns[:5]
        if my_conns:
            log.info(f"  🤝 {len(my_conns)} connection(s) at {company}")

        # 2. Apollo (pre-fetched above)
        apollo = apollo_cache.get(company, [])
        job["apollo_contacts"] = apollo[:5]
        if apollo:
            log.info(f"  🔍 Apollo: {len(apollo)} contacts")
            for a in apollo[:2]:
                log.info(f"     • {a['name']} — {a['title']}")

        # 3. Field leads (same-field engineers, potential referrers)
        job["field_leads"] = field_leads[:5]

        hiring_posts = signals.get("hiring_posts", [])
        founder_email = job.get("founder_email", "")

        # 4. Determine best contact
        best_contact = {}
        contact_source = ""

        if my_conns:
            c = my_conns[0]
            best_contact = {"name": c["name"], "title": c["position"], "linkedin_url": "", "email": c.get("email", "")}
            contact_source = "your_connection"
        elif hiring_posts:
            p = hiring_posts[0]
            best_contact = {"name": p.get("poster", ""), "title": "Hiring post author", "linkedin_url": p.get("url", ""), "email": ""}
            contact_source = "hiring_post"
        elif apollo:
            a = apollo[0]
            best_contact = {"name": a["name"], "title": a["title"], "linkedin_url": a.get("linkedin_url", ""), "email": ""}
            contact_source = "apollo"
        elif founder_email:
            best_contact = {"name": "", "title": "Founder (from HN/YC)", "linkedin_url": "", "email": founder_email}
            contact_source = "founder_direct"
        elif field_leads:
            fl = field_leads[0]
            best_contact = {"name": fl["name"], "title": fl.get("source_detail", ""), "linkedin_url": fl.get("profile_url", ""), "email": ""}
            contact_source = "field_lead"

        best_contact["source"] = contact_source
        job["best_contact"] = best_contact

        # 5. Determine track
        has_warm_path = bool(my_conns or hiring_posts or apollo or founder_email)
        job["track"] = "A" if has_warm_path else "B"

        # 6. Action score
        layoff_data = signals.get("layoffs", {})
        sentiment = signals.get("blind_sentiment", {})
        funding = signals.get("funding", "")

        job["action_score"] = _action_score(
            fit_score=job.get("fit_score", 0),
            has_connection=bool(my_conns),
            has_hiring_post=bool(hiring_posts),
            hiring_post_is_connection=False,
            has_apollo_contact=bool(apollo),
            has_founder_email=bool(founder_email),
            blind_confirmed_hiring=job.get("source", "") == "blind_offer_feed",
            is_hiring_post_lead=job.get("source", "") == "linkedin_hiring_post",
            recently_funded=bool(funding),
            no_layoffs=not layoff_data.get("had_layoffs", False),
            has_red_flags=sentiment.get("red_flags", False),
            had_layoffs=layoff_data.get("had_layoffs", False),
            levels_fyi_offers=bool(signals.get("levels_offers", [])),
            has_field_leads=bool(field_leads),
        )

        log.info(f"  Track {job['track']} | action_score={job['action_score']:.2f} | contact={contact_source or 'none'}")

    # Sort by action_score descending
    scored.sort(key=lambda x: x.get("action_score", 0), reverse=True)

    ENRICHED_JSON.write_text(json.dumps(scored, indent=2))

    track_a = sum(1 for j in scored if j.get("track") == "A")
    track_b = sum(1 for j in scored if j.get("track") == "B")
    log.info(f"Track A (warm path): {track_a}")
    log.info(f"Track B (cold apply): {track_b}")
    log.info(f"Saved to enriched.json")
    return scored

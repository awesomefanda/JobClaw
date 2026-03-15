"""Phase 3b — CONTACTS: find who to reach out to + assign track.

1. Apollo People Search (FREE, no credits) → hiring managers
2. LinkedIn connections CSV → warm introductions
3. Cross-reference hiring posts from signals phase
4. Calculate action_score and assign Track A/B

Input:  data/signals.json + connections.csv
Output: data/enriched.json
"""
import json
import requests
from pathlib import Path
from jobclaw.logger import get_logger
from jobclaw import config

log = get_logger("contacts")

DATA = Path(__file__).resolve().parent.parent / "data"
SIGNALS_JSON = DATA / "signals.json"
ENRICHED_JSON = DATA / "enriched.json"


# ─── Apollo People Search (FREE) ──────────────────────────────

def _apollo_search(company: str) -> list[dict]:
    """Search Apollo for engineering managers/directors at company. FREE endpoint."""
    if not config.APOLLO_API_KEY:
        return []
    try:
        resp = requests.post(
            "https://api.apollo.io/api/v1/mixed_people/search",
            headers={
                "Content-Type": "application/json",
                "x-api-key": config.APOLLO_API_KEY,
            },
            json={
                "q_organization_name": company,
                "person_titles": [
                    "Engineering Manager", "Director of Engineering",
                    "VP Engineering", "Head of Engineering",
                    "CTO", "Principal Engineer", "Staff Engineer",
                ],
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
) -> float:
    """Calculate actionability score — higher = more likely to get a response."""
    score = fit_score
    if has_founder_email:         score += 0.25  # HN/YC with direct email
    if has_connection:            score += 0.20
    if is_hiring_post_lead:       score += 0.20  # source IS a hiring post = named person to contact
    if blind_confirmed_hiring:    score += 0.15  # Blind confirms company closing candidates
    if has_hiring_post:           score += 0.15  # found a hiring post for this company
    if hiring_post_is_connection: score += 0.10
    if has_apollo_contact:        score += 0.10
    if recently_funded:           score += 0.05
    if no_layoffs:                score += 0.05
    if has_red_flags:             score -= 0.10
    if had_layoffs:               score -= 0.15
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

    # Load connections
    conn_index = config.load_connections()

    log.info(f"Enriching {len(scored)} matches with contacts")

    for i, job in enumerate(scored):
        company = job.get("company", "")
        signals = job.get("signals", {})
        log.info(f"({i+1}/{len(scored)}) {company} — {job.get('title','')}")

        # 1. Find connections
        my_conns = config.find_connections_at(company, conn_index)
        job["my_connections"] = my_conns[:5]
        if my_conns:
            log.info(f"  🤝 {len(my_conns)} connection(s) at {company}")

        # 2. Apollo search
        apollo = _apollo_search(company)
        job["apollo_contacts"] = apollo[:5]
        if apollo:
            log.info(f"  🔍 Apollo: {len(apollo)} contacts")
            for a in apollo[:2]:
                log.info(f"     • {a['name']} — {a['title']}")

        # 3. Best contact (priority: connection > hiring post > apollo > founder email)
        hiring_posts = signals.get("hiring_posts", [])
        founder_email = job.get("founder_email", "")

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

        best_contact["source"] = contact_source
        job["best_contact"] = best_contact

        # 4. Determine track
        has_warm_path = bool(my_conns or hiring_posts or apollo or founder_email)
        job["track"] = "A" if has_warm_path else "B"

        # 5. Action score
        layoff_data = signals.get("layoffs", {})
        sentiment = signals.get("blind_sentiment", {})
        funding = signals.get("funding", "")

        job["action_score"] = _action_score(
            fit_score=job.get("fit_score", 0),
            has_connection=bool(my_conns),
            has_hiring_post=bool(hiring_posts),
            hiring_post_is_connection=False,  # TODO: cross-ref post authors with connections
            has_apollo_contact=bool(apollo),
            has_founder_email=bool(founder_email),
            blind_confirmed_hiring=job.get("source", "") == "blind_offer_feed",
            is_hiring_post_lead=job.get("source", "") == "linkedin_hiring_post",
            recently_funded=bool(funding),
            no_layoffs=not layoff_data.get("had_layoffs", False),
            has_red_flags=sentiment.get("red_flags", False),
            had_layoffs=layoff_data.get("had_layoffs", False),
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

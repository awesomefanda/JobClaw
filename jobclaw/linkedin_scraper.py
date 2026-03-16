"""LinkedIn hiring posts scraper using Playwright + saved session.

Requires data/linkedin_session.json (created by running test_linkedin.py once).
Returns: {company_lower: [{"poster", "company", "snippet", "url"}]}
"""
import re
import time
import random
from pathlib import Path
from urllib.parse import quote_plus
from jobclaw.logger import get_logger

log = get_logger("linkedin_scraper")

DATA = Path(__file__).resolve().parent.parent / "data"
SESSION_FILE = DATA / "linkedin_session.json"

_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
"""

_ENG_KEYWORDS = [
    "engineer", "engineering", "software", "backend", "platform", "infrastructure",
    "data engineer", "sde", "swe", "developer", "architect", "staff", "principal",
    "fullstack", "full stack", "frontend", "distributed systems", "ml engineer",
    "machine learning", "devops", "site reliability", "sre",
]


def _extract_company(text: str) -> str:
    m = re.search(
        r'\bat\s+([A-Z][A-Za-z0-9][A-Za-z0-9\s\.\-]{1,30}?)(?:\s*[,!.\n]|$)',
        text
    )
    return m.group(1).strip() if m else ""


def _parse_posts(raw_text: str, extra_keywords: list[str]) -> list[dict]:
    """Split LinkedIn page text on 'Feed post\\n' and filter for engineering posts."""
    all_kw = list(set(_ENG_KEYWORDS + [kw.lower() for kw in extra_keywords]))
    blocks = raw_text.split("Feed post\n")
    posts = []
    seen: set[str] = set()

    for block in blocks[1:]:
        tl = block.lower()
        if not any(kw in tl for kw in all_kw):
            continue

        snippet = block[:500].strip()
        if snippet in seen:
            continue
        seen.add(snippet)

        lines = block.strip().split("\n")
        poster = lines[0].strip() if lines else ""
        poster = re.sub(r',?\s*(Hiring|Verified|3rd\+|2nd|1st|LION).*', '', poster).strip()

        urls = re.findall(r'https://www\.linkedin\.com/posts/[^\s\n?]+', block)
        if not urls:
            urls = re.findall(r'https://www\.linkedin\.com/feed/update/[^\s\n?]+', block)
        url = urls[0] if urls else ""

        company = _extract_company(block[:400])

        posts.append({
            "poster": poster[:80],
            "company": company,
            "snippet": snippet[:300],
            "url": url,
        })

    return posts


def _search_urls(resume: dict) -> list[str]:
    base = (
        "https://www.linkedin.com/search/results/content/"
        "?keywords={kw}&origin=CLUSTER_EXPANSION"
        "&datePosted=%5B%22past-month%22%5D"
    )
    queries = [
        "#hiring engineer",
        "#hiring software engineer",
        "#hiring backend",
        "#hiring platform engineer",
        "#hiring infrastructure",
    ]
    for role in resume.get("target_roles", [])[:3]:
        queries.append(f"#hiring {role}")
    return [base.format(kw=quote_plus(q)) for q in queries]


def scrape_hiring_posts(resume: dict) -> dict[str, list[dict]]:
    """Scrape LinkedIn #hiring posts using saved Playwright session.

    Returns {company_lower: [post_dict, ...]} pool for use in signals.py.
    Returns {} if no session file or session expired.
    """
    if not SESSION_FILE.exists():
        log.warning(
            "No LinkedIn session — run: venv\\Scripts\\python test_linkedin.py "
            "to log in once and save your session."
        )
        return {}

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright not installed — skipping LinkedIn scraping")
        return {}

    extra_kw = resume.get("target_keywords", resume.get("target_roles", []))
    urls = _search_urls(resume)
    pool: dict[str, list[dict]] = {}
    seen_snippets: set[str] = set()
    total = 0

    log.info(f"LinkedIn scraper: {len(urls)} queries (headless)")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                storage_state=str(SESSION_FILE),
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            ctx.add_init_script(_STEALTH_JS)
            page = ctx.new_page()

            for i, url in enumerate(urls):
                try:
                    page.goto(url, timeout=30000)
                    time.sleep(random.uniform(3, 5))

                    if "login" in page.url or "checkpoint" in page.url:
                        log.warning(
                            "LinkedIn session expired. Run test_linkedin.py again to refresh."
                        )
                        break

                    for _ in range(4):
                        page.evaluate("window.scrollBy(0, 900)")
                        time.sleep(random.uniform(1.2, 2.0))

                    posts = _parse_posts(page.inner_text("body"), extra_kw)

                    added = 0
                    for post in posts:
                        if post["snippet"] in seen_snippets:
                            continue
                        seen_snippets.add(post["snippet"])
                        total += 1
                        added += 1
                        company = post.get("company", "")
                        if company and len(company) > 2:
                            pool.setdefault(company.lower(), []).append(post)

                    log.info(f"  [{i+1}/{len(urls)}] {added} new engineering posts")
                    time.sleep(random.uniform(2, 4))

                except Exception as e:
                    log.warning(f"  [{i+1}/{len(urls)}] query failed: {e}")
                    continue

            page.close()
            ctx.close()
            browser.close()

    except Exception as e:
        log.warning(f"LinkedIn scraper failed: {e}")
        return {}

    log.info(f"LinkedIn scraper done: {total} posts → {len(pool)} company keys")
    return pool

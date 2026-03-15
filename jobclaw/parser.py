"""
Google Careers Parser (Karpathy-inspired).

Parse cached HTML from scraper.py into clean Markdown for LLM analysis.
Extract job details and convert to structured format.
"""

import json
import re
from pathlib import Path

from bs4 import BeautifulSoup
from markdownify import markdownify
from jobclaw.logger import get_logger

log = get_logger("parser")

DATA = Path(__file__).resolve().parent.parent / "data"
GOOGLE_JOBS_DIR = DATA / "google_jobs"
PARSED_DIR = DATA / "parsed_google_jobs"
PARSED_DIR.mkdir(exist_ok=True)


def parse_job_html(html_path: Path) -> dict:
    """
    Parse a single job HTML file into structured data and Markdown.

    Args:
        html_path: Path to the HTML file.

    Returns:
        Dict with job details.
    """
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")

    # Extract basic info
    title = soup.find("h1")
    title = title.get_text(strip=True) if title else "Unknown Title"

    # Location might be in a p tag like "Google | Location"
    location_elem = soup.find("p")
    if location_elem and "|" in location_elem.get_text():
        location = location_elem.get_text().split("|")[-1].strip()
    else:
        location = ""

    # Extract job description (look for divs with substantial text)
    desc_elem = None
    for div in soup.find_all("div"):
        text = div.get_text(strip=True)
        if len(text) > 500 and not any(skip in text.lower() for skip in ["privacy", "terms", "footer"]):
            desc_elem = div
            break

    if desc_elem:
        # Convert to Markdown
        description_md = markdownify(str(desc_elem), heading_style="ATX")
        # Clean up
        description_md = re.sub(r'\n{3,}', '\n\n', description_md)
    else:
        description_md = ""

    # Requirements (look for another substantial div)
    req_elem = None
    for div in soup.find_all("div"):
        text = div.get_text(strip=True)
        if len(text) > 200 and len(text) < 500 and "requirement" in text.lower():
            req_elem = div
            break

    requirements_md = markdownify(str(req_elem), heading_style="ATX") if req_elem else ""

    # Extract company (usually Google)
    company = "Google"

    # Generate job ID from filename
    job_id = html_path.stem

    # Find job URL from HTML (if embedded)
    url_elem = soup.find("link", rel="canonical")
    url = url_elem.get("href") if url_elem else f"https://careers.google.com/jobs/{job_id}"

    job_data = {
        "id": job_id,
        "title": title,
        "company": company,
        "location": location,
        "url": url,
        "description_markdown": description_md,
        "requirements_markdown": requirements_md,
        "full_markdown": f"# {title}\n\n**Company:** {company}\n\n**Location:** {location}\n\n## Description\n\n{description_md}\n\n## Requirements\n\n{requirements_md}",
    }

    return job_data


def parse_all_google_jobs():
    """
    Parse all cached Google job HTML files into JSON/Markdown.
    """
    html_files = list(GOOGLE_JOBS_DIR.glob("*.html"))
    log.info(f"Parsing {len(html_files)} Google job HTML files")

    jobs = []
    for html_path in html_files:
        try:
            job_data = parse_job_html(html_path)
            jobs.append(job_data)

            # Save individual parsed JSON
            json_path = PARSED_DIR / f"{job_data['id']}.json"
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(job_data, f, indent=2, ensure_ascii=False)

            # Save Markdown
            md_path = PARSED_DIR / f"{job_data['id']}.md"
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(job_data["full_markdown"])

        except Exception as e:
            log.error(f"Failed to parse {html_path}: {e}")
            continue

    # Save combined JSON
    combined_path = PARSED_DIR / "google_jobs.json"
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

    log.info(f"Parsed {len(jobs)} jobs, saved to {PARSED_DIR}")
    return jobs


if __name__ == "__main__":
    parse_all_google_jobs()
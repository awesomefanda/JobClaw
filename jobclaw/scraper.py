"""
Google Careers Scraper using Playwright (Karpathy-inspired approach).

Resilient scraping: Headed browser, human-like delays, local caching.
Targets Google Careers for Staff Software Engineer roles.
"""

import argparse
import hashlib
import json
import os
import random
import re
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright
from jobclaw.logger import get_logger

log = get_logger("scraper")

DATA = Path(__file__).resolve().parent.parent / "data"
GOOGLE_JOBS_DIR = DATA / "google_jobs"
GOOGLE_JOBS_DIR.mkdir(exist_ok=True)


def _job_id(url: str) -> str:
    """Generate a unique ID for a job based on its URL."""
    return hashlib.md5(url.encode()).hexdigest()[:12]


def scrape_google_jobs(search_term: str = "Staff Software Engineer", max_jobs: int = 20, delay: float = 3.0):
    """
    Scrape Google Careers for jobs.

    Args:
        search_term: Job title to search for.
        max_jobs: Maximum number of jobs to scrape.
        delay: Delay between requests in seconds.
    """
    base_url = "https://www.google.com/about/careers/applications/jobs/results/"
    search_url = f"{base_url}?q={search_term.replace(' ', '+')}"

    log.info(f"Starting Google Careers scrape for '{search_term}' (max {max_jobs} jobs)")

    jobs = []

    with sync_playwright() as p:
        # Launch headed browser to mimic human behavior
        browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()

        try:
            # Navigate to search results
            log.info(f"Navigating to {search_url}")
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for page to load (Google Careers loads dynamically)
            page.wait_for_timeout(10000)  # Wait 10 seconds for JS to load

            # Scroll to load more jobs
            for _ in range(3):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(2000)

            # Extract job links from search results
            job_links = page.query_selector_all("a[href*='jobs/results/'][href*='?q=']")
            job_urls = []
            for link in job_links:
                href = link.get_attribute("href")
                if href and re.search(r'\d{10,}', href):  # Has long ID
                    if href.startswith("/"):
                        full_url = f"https://www.google.com{href}"
                    elif href.startswith("jobs/"):
                        full_url = f"https://www.google.com/about/careers/applications/{href}"
                    else:
                        full_url = href
                    if full_url not in job_urls:
                        job_urls.append(full_url)

            job_urls = job_urls[:max_jobs]  # Limit to max_jobs
            log.info(f"Found {len(job_urls)} job URLs")

            # Scrape individual job pages
            for i, job_url in enumerate(job_urls):
                job_id = _job_id(job_url)
                html_path = GOOGLE_JOBS_DIR / f"{job_id}.html"

                if html_path.exists():
                    log.debug(f"Skipping {job_id} (cached)")
                    jobs.append({"id": job_id, "url": job_url, "cached": True})
                    continue

                log.info(f"Scraping job {i+1}/{len(job_urls)}: {job_url}")

                try:
                    page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(3000)  # Wait for content to load

                    # Save raw HTML immediately (Karpathy-style caching)
                    html_content = page.content()
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(html_content)

                    jobs.append({"id": job_id, "url": job_url, "cached": False})

                except Exception as e:
                    log.warning(f"Failed to scrape {job_url}: {e}")
                    continue

                # Human-like delay between requests
                if i < len(job_urls) - 1:
                    sleep_time = delay + random.uniform(0, 2)
                    log.debug(f"Sleeping {sleep_time:.1f}s")
                    time.sleep(sleep_time)

        except Exception as e:
            log.error(f"Scrape failed: {e}")
        finally:
            browser.close()

    log.info(f"Scraped {len(jobs)} jobs, saved to {GOOGLE_JOBS_DIR}")
    return jobs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Google Careers")
    parser.add_argument("--search", default="Staff Software Engineer", help="Search term")
    parser.add_argument("--max-jobs", type=int, default=20, help="Max jobs to scrape")
    parser.add_argument("--delay", type=float, default=3.0, help="Delay between requests")
    args = parser.parse_args()

    scrape_google_jobs(args.search, args.max_jobs, args.delay)
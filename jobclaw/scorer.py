"""Phase 2 — SCORE: Groq LLM scores each JD against resume.

Reads unscored jobs from data/jobs.csv.
Outputs data/scored.json (only matches >= MIN_FIT_SCORE).
Tracks scored IDs in data/scored_ids.txt to avoid re-scoring.

Batches all jobs into a minimum number of Groq calls to avoid
burning through the daily token limit.
"""
import csv
import json
import math
import time
from pathlib import Path
from jobclaw.logger import get_logger
from jobclaw import config

log = get_logger("scorer")

DATA = Path(__file__).resolve().parent.parent / "data"
JOBS_CSV = DATA / "jobs.csv"
SCORED_JSON = DATA / "scored.json"
SCORED_IDS = DATA / "scored_ids.txt"

# ~5 jobs per batch balances prompt size vs. call count
BATCH_SIZE = 5


def _already_scored() -> set:
    if not SCORED_IDS.exists():
        return set()
    return set(SCORED_IDS.read_text().strip().split("\n"))


def _mark_scored(jid: str):
    with open(SCORED_IDS, "a") as f:
        f.write(f"{jid}\n")


def _unscored_jobs(seen: set) -> list[dict]:
    if not JOBS_CSV.exists():
        return []
    jobs = []
    with open(JOBS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("id") not in seen:
                jobs.append(row)
    return jobs


def _score_batch(client, resume: dict, jobs: list[dict]) -> list[dict | None]:
    """Score multiple jobs in a single Groq call.
    Returns a list aligned with the input jobs list.
    """
    jobs_block = ""
    for i, job in enumerate(jobs):
        jobs_block += f"""JOB {i + 1}:
Title: {job.get('title', '')}
Company: {job.get('company', '')}
Location: {job.get('location', '')}
Description:
{job.get('description', '')[:2000]}
---
"""

    prompt = f"""Score each job's fit for the candidate. Return ONLY a valid JSON array with exactly {len(jobs)} objects, one per job in order:
[
  {{
    "score": 0.0,
    "reasoning": "2-3 sentences",
    "matching_skills": ["skill1"],
    "missing_skills": ["skill1"],
    "outreach_draft": "Short personalised LinkedIn message under 120 words"
  }}
]

CANDIDATE:
Name: {resume.get('name', '')}
Summary: {resume.get('summary', '')}
Skills: {', '.join(resume.get('technical_skills', []))}
Domain: {', '.join(resume.get('domain_expertise', []))}
Experience: {resume.get('experience_years', '')} years
Target roles: {', '.join(resume.get('target_roles', []))}

JOBS TO SCORE:
{jobs_block}
Return ONLY the JSON array. No markdown fences. Array must have exactly {len(jobs)} elements."""

    try:
        resp = client.chat.completions.create(
            model=config.GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=700 * len(jobs),
            temperature=0.3,
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        results = json.loads(text)
        if not isinstance(results, list):
            log.warning("Groq returned non-list response for batch")
            return [None] * len(jobs)
        # Pad or trim to match job count
        while len(results) < len(jobs):
            results.append(None)
        return results[: len(jobs)]
    except json.JSONDecodeError as e:
        log.warning(f"JSON parse error in batch: {e}")
        return [None] * len(jobs)
    except Exception as e:
        log.warning(f"Groq error in batch: {e}")
        return [None] * len(jobs)


def run_scorer(resume: dict) -> list[dict]:
    """Score all unscored jobs. Returns list of scored matches."""
    log.info("=" * 50)
    log.info("PHASE 2: SCORE")
    log.info("=" * 50)

    if not config.GROQ_API_KEY:
        log.error("GROQ_API_KEY not set in .env — cannot score")
        return []

    from groq import Groq
    client = Groq(api_key=config.GROQ_API_KEY)

    seen = _already_scored()
    jobs = _unscored_jobs(seen)
    log.info(f"Unscored jobs: {len(jobs)}")

    if not jobs:
        if SCORED_JSON.exists():
            return json.loads(SCORED_JSON.read_text())
        return []

    matches = []
    if SCORED_JSON.exists():
        try:
            matches = json.loads(SCORED_JSON.read_text())
        except Exception:
            matches = []

    total_batches = math.ceil(len(jobs) / BATCH_SIZE)
    log.info(f"Scoring in {total_batches} batch(es) of up to {BATCH_SIZE} jobs each")

    for batch_idx in range(total_batches):
        batch = jobs[batch_idx * BATCH_SIZE : (batch_idx + 1) * BATCH_SIZE]
        log.info(f"Batch {batch_idx + 1}/{total_batches} — scoring {len(batch)} jobs...")

        results = _score_batch(client, resume, batch)

        for job, result in zip(batch, results):
            _mark_scored(job["id"])

            if result and result.get("score", 0) >= config.MIN_FIT_SCORE:
                matches.append({
                    **{k: job.get(k, "") for k in CSV_FIELDS_SUBSET},
                    "fit_score": result["score"],
                    "reasoning": result.get("reasoning", ""),
                    "matching_skills": result.get("matching_skills", []),
                    "missing_skills": result.get("missing_skills", []),
                    "outreach_draft": result.get("outreach_draft", ""),
                })
                log.info(f"  ✅ {job.get('company')} — {result['score']:.2f} MATCH")
            elif result:
                log.debug(f"  ❌ {job.get('company')} — {result.get('score', 0):.2f} below threshold")
            else:
                log.debug(f"  ⚠️  {job.get('company')} — scoring failed")

        # Brief pause between batches to respect per-minute rate limits
        if batch_idx < total_batches - 1:
            time.sleep(3)

    SCORED_JSON.write_text(json.dumps(matches, indent=2))
    log.info(f"Total matches: {len(matches)} (saved to scored.json)")
    return matches


CSV_FIELDS_SUBSET = [
    "id", "title", "company", "location", "is_remote", "job_url",
    "salary_min", "salary_max", "job_type", "date_posted", "source",
    "founder_email",
]

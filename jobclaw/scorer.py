"""Phase 2 — SCORE: Groq LLM scores each JD against resume.

Reads unscored jobs from data/jobs.csv.
Outputs data/scored.json (only matches >= MIN_FIT_SCORE).
Tracks scored IDs in data/scored_ids.txt to avoid re-scoring.
"""
import csv
import json
import time
from pathlib import Path
from jobclaw.logger import get_logger
from jobclaw import config

log = get_logger("scorer")

DATA = Path(__file__).resolve().parent.parent / "data"
JOBS_CSV = DATA / "jobs.csv"
SCORED_JSON = DATA / "scored.json"
SCORED_IDS = DATA / "scored_ids.txt"


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


def _score_one(client, resume: dict, job: dict) -> dict | None:
    prompt = f"""Score this job's fit for the candidate. Return ONLY valid JSON:
{{
  "score": 0.0-1.0,
  "reasoning": "2-3 sentences",
  "matching_skills": ["skill1", "skill2"],
  "missing_skills": ["skill1"],
  "outreach_draft": "Short personalised LinkedIn message (under 120 words) the candidate could send to the hiring manager. Reference something specific about the role. Warm, genuine tone."
}}

CANDIDATE:
Name: {resume.get('name','')}
Summary: {resume.get('summary','')}
Skills: {', '.join(resume.get('technical_skills',[]))}
Domain: {', '.join(resume.get('domain_expertise',[]))}
Experience: {resume.get('experience_years','')} years
Target roles: {', '.join(resume.get('target_roles',[]))}

JOB:
Title: {job.get('title','')}
Company: {job.get('company','')}
Location: {job.get('location','')}
Source: {job.get('source','')}
Description:
{job.get('description','')[:3000]}

Return ONLY JSON. No markdown fences."""

    try:
        resp = client.chat.completions.create(
            model=config.GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.3,
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.warning(f"JSON parse error for {job.get('company')}: {e}")
        return None
    except Exception as e:
        log.warning(f"Groq error for {job.get('company')}: {e}")
        return None


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
        # Load previous scored results if they exist
        if SCORED_JSON.exists():
            return json.loads(SCORED_JSON.read_text())
        return []

    matches = []
    # Load previous matches to append to
    if SCORED_JSON.exists():
        try:
            matches = json.loads(SCORED_JSON.read_text())
        except Exception:
            matches = []

    for i, job in enumerate(jobs):
        log.info(f"({i+1}/{len(jobs)}) {job.get('company','')} — {job.get('title','')}")

        result = _score_one(client, resume, job)
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
            log.info(f"  ✅ {result['score']:.2f} — MATCH")
        elif result:
            log.debug(f"  ❌ {result.get('score', 0):.2f} — below threshold")
        else:
            log.debug(f"  ⚠️  scoring failed — skipping")

        time.sleep(2.5)  # Groq free tier: 30 req/min

    SCORED_JSON.write_text(json.dumps(matches, indent=2))
    log.info(f"Total matches: {len(matches)} (saved to scored.json)")
    return matches


CSV_FIELDS_SUBSET = [
    "id", "title", "company", "location", "is_remote", "job_url",
    "salary_min", "salary_max", "job_type", "date_posted", "source",
    "founder_email",
]

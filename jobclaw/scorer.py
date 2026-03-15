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
import re
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

# Fallback model with higher daily token limit (500k TPD vs 100k for 70b)
GROQ_FALLBACK_MODEL = "llama-3.1-8b-instant"
# Final fallback: Gemini 2.0 Flash (free via Google AI Studio, 1500 req/day)
GEMINI_FALLBACK_MODEL = "gemini-2.0-flash"


def _parse_retry_seconds(error_message: str) -> float:
    """Parse 'Please try again in 6m56.448s' → seconds."""
    m = re.search(r'try again in\s+(?:(\d+)m)?(\d+(?:\.\d+)?)s', str(error_message))
    if m:
        minutes = int(m.group(1) or 0)
        seconds = float(m.group(2) or 0)
        return minutes * 60 + seconds
    return 0.0


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


def _build_prompt(resume: dict, jobs: list[dict]) -> str:
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
    return f"""Score each job's fit for the candidate. Return ONLY a valid JSON array with exactly {len(jobs)} objects, one per job in order:
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


def _parse_response(text: str, job_count: int) -> list[dict | None]:
    if text.startswith("```"):
        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    results = json.loads(text)
    if not isinstance(results, list):
        return [None] * job_count
    while len(results) < job_count:
        results.append(None)
    return results[:job_count]


def _try_gemini(prompt: str, job_count: int) -> list[dict | None]:
    if not config.GEMINI_API_KEY:
        return None  # signal: not available
    try:
        import google.generativeai as genai
    except ImportError:
        log.warning("google-generativeai not installed — run: pip install google-generativeai")
        return None  # not available, fall through to Groq

    try:
        genai.configure(api_key=config.GEMINI_API_KEY)
        model = genai.GenerativeModel(GEMINI_FALLBACK_MODEL)
        resp = model.generate_content(prompt)
        return _parse_response(resp.text.strip(), job_count)
    except json.JSONDecodeError as e:
        log.warning(f"Gemini JSON parse error: {e}")
        return [None] * job_count
    except Exception as e:
        err = str(e)
        if "429" in err or "quota" in err.lower() or "rate" in err.lower():
            log.warning(f"Gemini rate limited — falling back to Groq")
            return "RATE_LIMITED"
        log.warning(f"Gemini error: {e}")
        return None  # unexpected error, fall through to Groq


def _try_groq(client, prompt: str, job_count: int) -> list[dict | None]:
    active_model = config.GROQ_MODEL
    tried_fallback = False

    for _ in range(3):
        try:
            resp = client.chat.completions.create(
                model=active_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=700 * job_count,
                temperature=0.3,
            )
            return _parse_response(resp.choices[0].message.content.strip(), job_count)
        except json.JSONDecodeError as e:
            log.warning(f"Groq JSON parse error: {e}")
            return [None] * job_count
        except Exception as e:
            err = str(e)
            if "429" in err or "rate_limit" in err.lower():
                if not tried_fallback:
                    log.warning(f"Rate limit on {active_model} — switching to {GROQ_FALLBACK_MODEL}")
                    active_model = GROQ_FALLBACK_MODEL
                    tried_fallback = True
                    continue
                wait = _parse_retry_seconds(err)
                log.warning(f"Rate limit on {active_model} — exhausted Groq options")
                if wait > 0:
                    log.warning(f"(Groq asks to wait {wait:.0f}s — try setting GEMINI_API_KEY to avoid this)")
                return "RATE_LIMITED"
            else:
                log.warning(f"Groq error: {e}")
                return [None] * job_count

    return "RATE_LIMITED"


def _score_batch(client, resume: dict, jobs: list[dict]) -> list[dict | None]:
    """Score a batch. Uses Gemini first if key available, otherwise Groq."""
    prompt = _build_prompt(resume, jobs)
    job_count = len(jobs)

    if config.GEMINI_API_KEY:
        result = _try_gemini(prompt, job_count)
        if result is None:
            pass  # GEMINI_API_KEY was set but genai unavailable — fall through
        elif result != "RATE_LIMITED":
            return result
        else:
            log.warning("Gemini rate limited — falling back to Groq")

    if config.GROQ_API_KEY:
        result = _try_groq(client, prompt, job_count)
        if result != "RATE_LIMITED":
            return result
        log.warning("All scoring providers rate limited for this batch — skipping")

    return [None] * job_count


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

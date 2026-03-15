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

    genai.configure(api_key=config.GEMINI_API_KEY)
    model = genai.GenerativeModel(GEMINI_FALLBACK_MODEL)

    for attempt in range(3):
        try:
            resp = model.generate_content(prompt)
            return _parse_response(resp.text.strip(), job_count)
        except json.JSONDecodeError as e:
            log.warning(f"Gemini JSON parse error: {e}")
            return [None] * job_count
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower() or "rate" in err.lower():
                wait = _parse_retry_seconds(err) or 5
                log.warning(f"Gemini rate limited — waiting {wait:.0f}s before retry (attempt {attempt + 1}/3)...")
                time.sleep(wait)
            else:
                log.warning(f"Gemini error: {e}")
                return None  # unexpected error, fall through to Groq

    log.warning("Gemini rate limit persists — falling back to Groq")
    return "RATE_LIMITED"


def _try_groq(client, prompt: str, job_count: int, model: str | None = None) -> list[dict | None]:
    active_model = model or config.GROQ_MODEL
    tried_fallback = False

    try:
        resp = client.chat.completions.create(
            model=active_model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=700 * job_count,
            temperature=0.3,
        )
        return _parse_response(resp.choices[0].message.content.strip(), job_count)
    except json.JSONDecodeError as e:
        log.warning(f"Groq JSON parse error ({active_model}): {e}")
        return [None] * job_count
    except Exception as e:
        err = str(e)
        if "429" in err or "rate_limit" in err.lower():
            return "RATE_LIMITED"
        log.warning(f"Groq error ({active_model}): {e}")
        return [None] * job_count


def _score_batch(client, resume: dict, jobs: list[dict], provider_idx: int = 0) -> list[dict | None]:
    """Score a batch.

    Strategy (optimised for free tiers, longer-run friendly):
      1. Gemini 2.0 Flash — used for every batch; waits out rate limits rather
         than burning Groq's much smaller daily token budget.
      2. Groq llama-3.3-70b — emergency fallback only (Gemini unavailable/broken).
      3. Groq llama-3.1-8b-instant — last resort.
    """
    prompt = _build_prompt(resume, jobs)
    job_count = len(jobs)

    # Primary: Gemini (wait out rate limits, don't fall back just for throttling)
    if config.GEMINI_API_KEY:
        result = _try_gemini(prompt, job_count)
        if result is not None and result != "RATE_LIMITED":
            return result
        if result == "RATE_LIMITED":
            # Gemini daily quota truly exhausted — fall through to Groq
            log.warning("Gemini daily quota exhausted — falling back to Groq for remaining batches")

    # Emergency fallback: Groq (preserve for when Gemini is genuinely unavailable)
    if config.GROQ_API_KEY:
        for model in [config.GROQ_MODEL, GROQ_FALLBACK_MODEL]:
            result = _try_groq(client, prompt, job_count, model=model)
            if result is not None and result != "RATE_LIMITED":
                return result

    log.warning("All providers exhausted for this batch — skipping")
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
            time.sleep(5)  # 5s keeps us under Gemini's 15 RPM free-tier limit

    SCORED_JSON.write_text(json.dumps(matches, indent=2))
    log.info(f"Total matches: {len(matches)} (saved to scored.json)")
    return matches


CSV_FIELDS_SUBSET = [
    "id", "title", "company", "location", "is_remote", "job_url",
    "salary_min", "salary_max", "job_type", "date_posted", "source",
    "founder_email",
]

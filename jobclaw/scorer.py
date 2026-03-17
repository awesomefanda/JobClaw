"""Phase 2 — SCORE: LLM scores each JD against resume.

Reads unscored jobs from data/jobs.csv.
Outputs data/scored.json (only matches >= MIN_FIT_SCORE).
Tracks scored IDs in data/scored_ids.txt to avoid re-scoring.

Provider chain (all free):
  1. Gemini 2.0 Flash       — 1500 req/day via Google AI Studio
  2. Claude (OAuth)          — reuses ~/.claude/.credentials.json from your
                               Claude Code subscription ($20/mo) — no extra cost
  3. Groq llama-3.3-70b     — 100k tokens/day
  4. Groq llama-3.1-8b      — 500k tokens/day (last resort)
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

GROQ_FALLBACK_MODEL  = "llama-3.1-8b-instant"
GEMINI_MODEL         = "gemini-2.0-flash"
CLAUDE_OAUTH_MODEL   = "claude-haiku-4-5-20251001"   # fastest Claude model

_CLAUDE_CREDS_PATH   = Path.home() / ".claude" / ".credentials.json"
_ANTHROPIC_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
_ANTHROPIC_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"


def _parse_retry_seconds(error_message: str) -> float:
    """Parse 'Please try again in 6m56.448s' → seconds."""
    m = re.search(r'try again in\s+(?:(\d+)m)?(\d+(?:\.\d+)?)s', str(error_message))
    if m:
        minutes = int(m.group(1) or 0)
        seconds = float(m.group(2) or 0)
        return minutes * 60 + seconds
    return 0.0


def _load_claude_token() -> str | None:
    """Read Claude Code OAuth token from ~/.claude/.credentials.json.
    Auto-refreshes if expiring within 10 minutes.
    Returns access token string, or None if unavailable.
    """
    if not _CLAUDE_CREDS_PATH.exists():
        return None
    try:
        creds = json.loads(_CLAUDE_CREDS_PATH.read_text(encoding="utf-8"))
        oauth = creds.get("claudeAiOauth", creds)
        expires_at = oauth.get("expiresAt", 0)
        if expires_at < 1e12:          # convert seconds → milliseconds if needed
            expires_at *= 1000
        remaining_min = (expires_at - time.time() * 1000) / 60000

        if remaining_min < 10:
            # Refresh the token
            refresh_token = oauth.get("refreshToken") or oauth.get("refresh_token")
            if not refresh_token:
                return None
            import requests as _req
            resp = _req.post(
                _ANTHROPIC_TOKEN_URL,
                json={"grant_type": "refresh_token",
                      "client_id": _ANTHROPIC_CLIENT_ID,
                      "refresh_token": refresh_token},
                timeout=20,
            )
            if resp.status_code != 200:
                log.debug(f"Claude token refresh failed: {resp.status_code}")
                return None
            data = resp.json()
            oauth["accessToken"] = data["access_token"]
            if data.get("refresh_token"):
                oauth["refreshToken"] = data["refresh_token"]
            oauth["expiresAt"] = int(time.time() * 1000) + data.get("expires_in", 28800) * 1000
            if "claudeAiOauth" in creds:
                creds["claudeAiOauth"] = oauth
            else:
                creds = oauth
            _CLAUDE_CREDS_PATH.write_text(json.dumps(creds, indent=2), encoding="utf-8")
            log.debug("Claude OAuth token refreshed")

        return oauth.get("accessToken") or oauth.get("access_token")
    except Exception as e:
        log.debug(f"Claude token load failed: {e}")
        return None


def _try_claude_oauth(prompt: str, job_count: int) -> list[dict | None] | None:
    """Call Anthropic Messages API using Claude Code OAuth token.
    Returns None if unavailable, 'RATE_LIMITED' if quota hit, else results.
    """
    token = _load_claude_token()
    if not token:
        return None   # not available

    import requests as _req
    try:
        resp = _req.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": CLAUDE_OAUTH_MODEL,
                "max_tokens": 700 * job_count,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        if resp.status_code == 429:
            return "RATE_LIMITED"
        if resp.status_code != 200:
            log.debug(f"Claude OAuth API error: {resp.status_code} {resp.text[:200]}")
            return None
        text = resp.json()["content"][0]["text"].strip()
        return _parse_response(text, job_count)
    except json.JSONDecodeError as e:
        log.warning(f"Claude OAuth JSON parse error: {e}")
        return [None] * job_count
    except Exception as e:
        log.debug(f"Claude OAuth call failed: {e}")
        return None


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
        from google import genai
    except ImportError:
        log.warning("google-genai not installed — run: pip install google-genai")
        return None  # not available, fall through to Groq

    client = genai.Client(api_key=config.GEMINI_API_KEY)

    for attempt in range(3):
        try:
            resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
            return _parse_response(resp.text.strip(), job_count)
        except json.JSONDecodeError as e:
            log.warning(f"Gemini JSON parse error: {e}")
            return [None] * job_count
        except Exception as e:
            err = str(e)
            if "429" in err or "RESOURCE_EXHAUSTED" in err or "quota" in err.lower():
                wait = _parse_retry_seconds(err) or 10
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


def _score_batch(client, resume: dict, jobs: list[dict]) -> list[dict | None]:
    """Score a batch using the free-tier provider chain.

      1. Gemini 2.0 Flash   — 1500 req/day; waits out rate limits patiently
      2. Claude OAuth        — reuses Claude Code subscription, no extra cost
      3. Groq llama-3.3-70b — 100k tokens/day emergency fallback
      4. Groq llama-3.1-8b  — 500k tokens/day last resort
    """
    prompt = _build_prompt(resume, jobs)
    job_count = len(jobs)

    # 1. Gemini — primary, wait out per-minute limits but not daily quota
    if config.GEMINI_API_KEY:
        result = _try_gemini(prompt, job_count)
        if result is not None and result != "RATE_LIMITED":
            return result
        if result == "RATE_LIMITED":
            log.warning("Gemini daily quota exhausted — trying Claude OAuth")

    # 2. Claude OAuth — free via Claude Code subscription
    result = _try_claude_oauth(prompt, job_count)
    if result is not None and result != "RATE_LIMITED":
        log.debug("Scored via Claude OAuth")
        return result
    if result == "RATE_LIMITED":
        log.warning("Claude OAuth rate limited — falling back to Groq")

    # 3 & 4. Groq — last resort
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

    has_any_provider = (
        config.GEMINI_API_KEY
        or _CLAUDE_CREDS_PATH.exists()
        or config.GROQ_API_KEY
    )
    if not has_any_provider:
        log.error("No scorer available. Set GEMINI_API_KEY or GROQ_API_KEY in .env, "
                  "or log in to Claude Code (claude login).")
        return []

    client = None
    if config.GROQ_API_KEY:
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

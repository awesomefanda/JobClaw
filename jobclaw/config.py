"""Configuration & bootstrap for JobClaw.

Handles:
  - Loading .env for API keys
  - Parsing resume from PDF / DOCX / JSON
  - One-time Groq call to structure a PDF/DOCX resume
  - Loading LinkedIn connections CSV
"""
import os
import csv
import json
from pathlib import Path
from dotenv import load_dotenv
from jobclaw.logger import get_logger

log = get_logger("config")

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
LOCAL = ROOT / ".local"   # gitignored — personal files (resume, connections, .env)
DATA.mkdir(exist_ok=True)
LOCAL.mkdir(exist_ok=True)

load_dotenv(ROOT / ".env")
load_dotenv(LOCAL / ".env", override=True)  # .local/.env overrides root .env

# ── Env helpers ────────────────────────────────────────────────

def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)

GROQ_API_KEY    = env("GROQ_API_KEY")
APOLLO_API_KEY  = env("APOLLO_API_KEY")
GITHUB_TOKEN    = env("GITHUB_TOKEN")       # optional — raises rate limit from 60 to 5000 req/hr
MIN_FIT_SCORE   = float(env("MIN_FIT_SCORE", "0.75"))
GROQ_MODEL      = env("GROQ_MODEL", "llama-3.3-70b-versatile")
LOG_LEVEL       = env("LOG_LEVEL", "INFO")

# ── Resume loading ─────────────────────────────────────────────

PARSED_RESUME = DATA / "parsed_resume.json"

def _extract_pdf_text(path: Path) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        log.error(f"PDF extraction failed: {e}")
        return ""

def _extract_docx_text(path: Path) -> str:
    try:
        from docx import Document
        doc = Document(str(path))
        return "\n".join(p.text for p in doc.paragraphs)
    except Exception as e:
        log.error(f"DOCX extraction failed: {e}")
        return ""

def _extract_resume_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return _extract_pdf_text(path)
    elif suffix in (".docx", ".doc"):
        return _extract_docx_text(path)
    elif suffix == ".json":
        return ""  # already structured
    elif suffix in (".txt", ".md"):
        return path.read_text(errors="ignore")[:15000]
    else:
        log.warning(f"Unknown resume format: {suffix}. Trying as text.")
        return path.read_text(errors="ignore")[:15000]


def _parse_resume_with_groq(text: str) -> dict:
    """One-time Groq call to understand the person and infer search strategy."""
    if not GROQ_API_KEY:
        log.error("GROQ_API_KEY not set — cannot parse resume")
        return {}
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_API_KEY)
        resp = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": f"""Read this resume carefully. Return ONLY valid JSON with ALL of these fields:
{{
  "name": "",
  "email": "",
  "phone": "",
  "linkedin_url": "",
  "location": "city, state",
  "experience_years": 0,
  "summary": "3-4 sentence summary",
  "technical_skills": ["skill1", "skill2"],
  "domain_expertise": ["area1", "area2"],
  "education": "",

  "current_level": "one of: junior, mid, senior, staff, principal, director, senior_director, vp, svp, cto",
  "is_manager": false,
  "track": "one of: ic, management, mixed",

  "target_roles": ["5-7 job titles this person should search for — at their level or one step up. Be specific with real titles companies actually use."],
  "target_keywords": ["5-8 search keywords to find these roles on job boards"],
  "keywords_exclude": ["terms clearly irrelevant to this person"],

  "hm_titles_above_me": ["3-5 titles of people who would HIRE someone at this level. E.g. if the person is Staff Engineer, the hiring managers are Engineering Manager, Director of Engineering, VP Engineering. If the person is Director, the hiring managers are VP, SVP, CTO."],

  "blind_level_terms": ["level codes used on Blind/Levels.fyi for this person's level. E.g. E5, L6, IC5, Staff, Principal, Director, etc."]
}}

RESUME TEXT:
{text[:6000]}

Think step by step:
1. What level is this person currently at?
2. Are they IC or management track?
3. What titles should they search for (their level + one step up)?
4. Who hires people at this level? (one or two levels above)
5. What Blind/Levels.fyi level codes apply?

Return ONLY JSON. No markdown. No explanation."""}],
            max_tokens=1200,
            temperature=0.2,
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        return json.loads(raw)
    except Exception as e:
        log.error(f"Groq resume parse failed: {e}")
        return {}


def load_resume() -> dict:
    """Load or bootstrap the parsed resume.

    Priority:
      1. data/parsed_resume.json (cached from previous run)
      2. .local/resume.json or resume.json (manual entry)
      3. .local/resume.{pdf,docx,...} then resume.{pdf,docx,...} (auto-parsed via Groq)
    """
    # 1. Cached parsed resume
    if PARSED_RESUME.exists():
        data = json.loads(PARSED_RESUME.read_text())
        if data.get("name") or data.get("summary"):
            log.info(f"Loaded cached resume: {data.get('name', 'unknown')}")
            return data

    # 2. Manual JSON resume (.local first, then root)
    for json_path in [LOCAL / "resume.json", ROOT / "resume.json"]:
        if json_path.exists():
            data = json.loads(json_path.read_text())
            if data.get("name") or data.get("summary"):
                log.info(f"Loaded {json_path.relative_to(ROOT)}: {data.get('name', 'unknown')}")
                PARSED_RESUME.write_text(json.dumps(data, indent=2))
                return data

    # 3. PDF or DOCX — check .local first, then root
    for ext in ("pdf", "docx", "doc", "txt"):
        for search_dir in (LOCAL, ROOT):
            candidate = search_dir / f"resume.{ext}"
            if candidate.exists():
                log.info(f"Found {candidate.relative_to(ROOT)} — extracting text...")
                text = _extract_resume_text(candidate)
                if not text:
                    log.warning(f"Could not extract text from {candidate.name}")
                    continue
                log.info("Parsing resume via Groq (one-time)...")
                data = _parse_resume_with_groq(text)
                if data:
                    # Auto-generate smart defaults based on parsed level
                    level = data.get("current_level", "senior")
                    is_mgr = data.get("is_manager", False)

                    # Platforms: directors live on LinkedIn; ICs spread across boards
                    if level in ("director", "senior_director", "vp", "svp", "cto"):
                        platforms = ["linkedin", "indeed", "google"]
                    else:
                        platforms = ["indeed", "linkedin", "google", "glassdoor", "zip_recruiter"]

                    data.setdefault("preferences", {"remote": True, "minimum_salary_usd": 200000})
                    data.setdefault("keywords_exclude", data.get("keywords_exclude", []))
                    data.setdefault("hm_titles_above_me", [])
                    data.setdefault("blind_level_terms", [])
                    data.setdefault("scout", {
                        "platforms": platforms,
                        "hours_old": 168,      # 7 days
                        "max_results": 100,    # per platform per search term
                        "greenhouse_boards": [],
                        "lever_boards": [],
                        "hiring_post_keywords": data.get("target_keywords", data.get("target_roles", [])),
                    })
                    PARSED_RESUME.write_text(json.dumps(data, indent=2))
                    log.info(f"Resume parsed and cached: {data.get('name', 'unknown')}")
                    log.info(f"Detected level: {level} | Track: {data.get('track', '?')} | Manager: {is_mgr}")
                    log.info(f"Target roles: {', '.join(data.get('target_roles', [])[:3])}...")
                    log.info(f"Hiring managers to find: {', '.join(data.get('hm_titles_above_me', [])[:3])}...")
                    log.info(f"Review/edit: data/parsed_resume.json")
                    return data

    log.error("No resume found. Place resume.pdf / resume.docx / resume.json in .local/ or project root.")
    return {}


# ── Connections CSV ────────────────────────────────────────────

def load_connections() -> dict[str, list[dict]]:
    """Load LinkedIn connections CSV. Returns {company_lower: [{name, position, email}]}
    Looks in .local/connections.csv first, then connections.csv in root.
    """
    csv_path = LOCAL / "connections.csv"
    if not csv_path.exists():
        csv_path = ROOT / "connections.csv"
    if not csv_path.exists():
        log.info("No connections.csv found — network matching disabled")
        return {}

    index: dict[str, list[dict]] = {}
    count = 0
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                first = (row.get("First Name", "") or "").strip()
                last = (row.get("Last Name", "") or "").strip()
                company = (row.get("Company", "") or "").strip()
                if not company:
                    continue
                index.setdefault(company.lower(), []).append({
                    "name": f"{first} {last}".strip(),
                    "position": (row.get("Position", "") or "").strip(),
                    "email": (row.get("Email Address", "") or row.get("Email", "") or "").strip(),
                })
                count += 1
    except Exception as e:
        log.error(f"Error reading connections.csv: {e}")
        return {}

    log.info(f"Loaded {count} connections across {len(index)} companies")
    return index


def find_connections_at(company: str, index: dict) -> list[dict]:
    """Fuzzy match company name against connections index."""
    if not index:
        return []
    cl = company.lower().strip()
    matches = []
    for key, conns in index.items():
        if cl in key or key in cl:
            matches.extend(conns)
    return matches

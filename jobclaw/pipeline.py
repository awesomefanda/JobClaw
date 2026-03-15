"""JobClaw Pipeline — orchestrates all phases.

Phase 0: Bootstrap (parse resume, load connections)
Phase 1: Discover (scout all sources → jobs.csv)
Phase 2: Score (Groq LLM → scored.json)
Phase 3a: Signals (web intel → signals.json)
Phase 3b: Contacts (Apollo + network → enriched.json)
Phase 5: Report (Excel output)

Each phase is wrapped in try/except.
If a phase fails, we log the error and continue with what we have.
"""
import sys
from datetime import datetime
from jobclaw.logger import get_logger
from jobclaw import config

log = get_logger("pipeline")


def run(resume_override: dict | None = None):
    """Run the full pipeline."""

    log.info("")
    log.info("🦀" + "=" * 55)
    log.info("   JOBCLAW — Job Intelligence Pipeline")
    log.info(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 57)
    log.info("")

    # ── Phase 0: Bootstrap ─────────────────────────────────────
    resume = resume_override
    if not resume:
        resume = config.load_resume()
    if not resume or (not resume.get("name") and not resume.get("summary")):
        log.error("No resume loaded. Place resume.pdf, resume.docx, or resume.json in project root.")
        log.error("Then run again.")
        return None

    log.info(f"Candidate: {resume.get('name', 'unknown')}")
    log.info(f"Targets: {', '.join(resume.get('target_roles', []))}")
    log.info(f"Min fit score: {config.MIN_FIT_SCORE}")
    log.info("")

    # ── Phase 1: Discover ──────────────────────────────────────
    new_count = 0
    try:
        from jobclaw.scout import run_scout
        new_count = run_scout(resume)
    except Exception as e:
        log.error(f"PHASE 1 (Scout) failed: {e}", exc_info=True)
        log.warning("Continuing with any existing jobs.csv data...")

    log.info("")

    # ── Phase 2: Score ─────────────────────────────────────────
    scored = []
    try:
        from jobclaw.scorer import run_scorer
        scored = run_scorer(resume)
    except Exception as e:
        log.error(f"PHASE 2 (Scorer) failed: {e}", exc_info=True)
        log.warning("Continuing with any existing scored.json...")

    if not scored:
        log.warning("No scored matches — check if jobs were found and GROQ_API_KEY is set")
        # Try loading previous results
        from pathlib import Path
        import json
        scored_path = Path(__file__).resolve().parent.parent / "data" / "scored.json"
        if scored_path.exists():
            scored = json.loads(scored_path.read_text())
            log.info(f"Loaded {len(scored)} previous scored matches")

    if not scored:
        log.error("No matches to process. Pipeline stopping.")
        return None

    log.info("")

    # ── Phase 3a: Signals ──────────────────────────────────────
    try:
        from jobclaw.signals import run_signals
        scored = run_signals(resume)
    except Exception as e:
        log.error(f"PHASE 3a (Signals) failed: {e}", exc_info=True)
        log.warning("Continuing without signal enrichment...")

    log.info("")

    # ── Phase 3b: Contacts ─────────────────────────────────────
    enriched = scored
    try:
        from jobclaw.contacts import run_contacts
        enriched = run_contacts(resume)
    except Exception as e:
        log.error(f"PHASE 3b (Contacts) failed: {e}", exc_info=True)
        log.warning("Continuing without contact enrichment...")

    log.info("")

    # ── Phase 5: Report ────────────────────────────────────────
    filepath = ""
    try:
        from jobclaw.report import generate_report
        filepath = generate_report(enriched)
    except Exception as e:
        log.error(f"PHASE 5 (Report) failed: {e}", exc_info=True)

    # ── Summary ────────────────────────────────────────────────
    log.info("")
    log.info("🦀" + "=" * 55)
    log.info("   PIPELINE COMPLETE")
    if filepath:
        log.info(f"   📊 Report: {filepath}")
    log.info(f"   Total matches: {len(enriched)}")
    track_a = sum(1 for j in enriched if j.get("track") == "A")
    track_b = sum(1 for j in enriched if j.get("track") == "B")
    log.info(f"   Track A (warm): {track_a} | Track B (cold): {track_b}")
    if enriched:
        top = enriched[0]
        log.info(f"   🏆 Top match: {top.get('company')} — {top.get('title')} (action_score={top.get('action_score',0):.2f})")
    log.info("=" * 57)
    log.info("")

    return filepath

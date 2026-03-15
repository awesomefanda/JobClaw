#!/usr/bin/env python3
"""
JobClaw — Run the full pipeline.

Usage:
    python run.py                   # Full pipeline → Excel report
    python run.py --scout-only      # Just scrape new jobs
    python run.py --score-only      # Just score unscored jobs
    python run.py --signals-only    # Just fetch signals for scored jobs
    python run.py --report-only     # Just regenerate the report
"""
import sys
import os

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def main():
    args = set(sys.argv[1:])

    if "--scout-only" in args:
        from jobclaw.config import load_resume
        from jobclaw.scout import run_scout
        run_scout(load_resume())
        return

    if "--score-only" in args:
        from jobclaw.config import load_resume
        from jobclaw.scorer import run_scorer
        run_scorer(load_resume())
        return

    if "--signals-only" in args:
        from jobclaw.config import load_resume
        from jobclaw.signals import run_signals
        run_signals(load_resume())
        return

    if "--report-only" in args:
        from jobclaw.report import generate_report
        generate_report()
        return

    # Full pipeline
    from jobclaw.pipeline import run
    result = run()

    if result:
        print(f"\n  Open the report: {result}\n")
    else:
        print("\n  Pipeline completed with errors. Check logs/ for details.\n")


if __name__ == "__main__":
    main()

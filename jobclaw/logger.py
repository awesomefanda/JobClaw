"""Centralised logging for JobClaw.

Every module does:
    from jobclaw.logger import log
    log.info("message")

Logs go to both stderr (coloured) and data/jobclaw.log (plain).
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

_LOG_DIR = Path(__file__).resolve().parent.parent / "data"
_LOG_DIR.mkdir(exist_ok=True)
_LOG_FILE = _LOG_DIR / "jobclaw.log"

_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# ── Formatter with colour for console ──────────────────────────
class _ColourFormatter(logging.Formatter):
    COLOURS = {
        logging.DEBUG: "\033[90m",      # grey
        logging.INFO: "\033[36m",       # cyan
        logging.WARNING: "\033[33m",    # yellow
        logging.ERROR: "\033[31m",      # red
        logging.CRITICAL: "\033[1;31m", # bold red
    }
    RESET = "\033[0m"

    def format(self, record):
        colour = self.COLOURS.get(record.levelno, "")
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        module = record.name.replace("jobclaw.", "")
        msg = record.getMessage()
        return f"{colour}[{ts}] {record.levelname:<7} {module}: {msg}{self.RESET}"


class _PlainFormatter(logging.Formatter):
    def format(self, record):
        ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        module = record.name.replace("jobclaw.", "")
        return f"[{ts}] {record.levelname:<7} {module}: {record.getMessage()}"


def _setup():
    root = logging.getLogger("jobclaw")
    if root.handlers:
        return root
    root.setLevel(getattr(logging, _LEVEL, logging.INFO))

    # Console handler
    ch = logging.StreamHandler(sys.stderr)
    ch.setFormatter(_ColourFormatter())
    root.addHandler(ch)

    # File handler
    try:
        fh = logging.FileHandler(_LOG_FILE, encoding="utf-8")
        fh.setFormatter(_PlainFormatter())
        root.addHandler(fh)
    except Exception:
        pass  # silently skip file logging if dir not writable

    return root


log = _setup()


def get_logger(name: str) -> logging.Logger:
    """Get a child logger, e.g. get_logger('scout')."""
    return logging.getLogger(f"jobclaw.{name}")

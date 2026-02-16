"""
Centralized JSONL logging configuration.

Call setup_logging() once at process startup. All src.* loggers
inherit the rotating file handler automatically. Console output
is unaffected — print() statements remain the console UX.
"""

import json
import logging
import logging.handlers
from datetime import datetime, timezone

from src.config import LOG_FILE


class JSONLFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.name,
            "function": record.funcName,
            "event": record.getMessage(),
            "data": getattr(record, "data", {}),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging() -> None:
    """Configure the 'src' logger with a rotating JSONL file handler.

    Idempotent — safe to call multiple times (e.g. uvicorn reload).
    """
    src_logger = logging.getLogger("src")
    if src_logger.handlers:
        return

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.handlers.RotatingFileHandler(
        filename=str(LOG_FILE),
        maxBytes=50 * 1024 * 1024,  # 50 MB
        backupCount=5,
        encoding="utf-8",
    )
    handler.setFormatter(JSONLFormatter())

    src_logger.setLevel(logging.DEBUG)
    src_logger.addHandler(handler)
    src_logger.propagate = False

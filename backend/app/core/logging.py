import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict

_LOGGING_CONFIGURED = False


class JsonFormatter(logging.Formatter):
    """
    Minimal JSON formatter for structured logs.

    We intentionally keep this lightweight and dependency-free.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": getattr(record, "service", "deep_research_backend"),
        }

        # Common structured fields
        for field in ("job_id", "request_id", "connector", "step"):
            if hasattr(record, field):
                log_record[field] = getattr(record, field)

        return json.dumps(log_record)


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logger once with JSON output.

    Safe to call multiple times – subsequent calls are no-ops.
    """
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)

    _LOGGING_CONFIGURED = True


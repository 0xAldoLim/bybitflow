"""Small structured-log formatter with explicitly allowlisted context fields."""

import json
import logging
import re

_SECRET = re.compile(r"https?://\S*(?:webhooks?|token|key|secret)\S*", re.IGNORECASE)


def safe_detail(exc, limit=200):
    """Exception text for logs: one line, bounded, with credential-bearing URLs removed."""
    text = " ".join(str(exc).split())
    return _SECRET.sub("[redacted-url]", text)[:limit]


class JsonFormatter(logging.Formatter):
    def format(self, record):
        row = {
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            "created_ms": int(record.created * 1000),
        }
        for key in ("symbol", "error_type", "error_category", "location", "detail"):
            if hasattr(record, key):
                row[key] = getattr(record, key)
        return json.dumps(row)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    # HTTP request logs can contain webhook URL tokens. Never enable them for this app.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

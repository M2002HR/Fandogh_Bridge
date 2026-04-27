from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        service_name = getattr(record, "service_name", None) or os.getenv("SERVICE_NAME", "fandogh-bridge")
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service.name": service_name,
            "event": getattr(record, "event", None),
            "platform": getattr(record, "platform", None),
            "user_id": getattr(record, "user_id", None),
            "chat_id": getattr(record, "chat_id", None),
            "update_id": getattr(record, "update_id", None),
            "status": getattr(record, "status", None),
            "error": getattr(record, "error", None),
            "message": record.getMessage(),
        }
        details = getattr(record, "details", None)
        if details is not None:
            payload["details"] = details
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
            if not payload["error"] and len(record.exc_info) > 1 and record.exc_info[1] is not None:
                payload["error"] = str(record.exc_info[1])
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str, log_format: str | None = None) -> None:
    resolved_level = getattr(logging, level.upper(), logging.INFO)
    resolved_format = (log_format or os.getenv("LOG_FORMAT", "json")).strip().lower()
    httpx_level = getattr(logging, os.getenv("HTTPX_LOG_LEVEL", "WARNING").strip().upper(), logging.WARNING)
    handler = logging.StreamHandler(stream=sys.stdout)
    if resolved_format == "plain":
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    else:
        handler.setFormatter(JsonLogFormatter())
    logging.basicConfig(
        level=resolved_level,
        handlers=[handler],
        force=True,
    )
    logging.getLogger("httpx").setLevel(httpx_level)
    logging.getLogger("httpcore").setLevel(httpx_level)

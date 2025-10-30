from __future__ import annotations
import json
import logging
import os
from datetime import datetime
from typing import Any

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        extra = {
            k: v
            for k, v in record.__dict__.items()
            if k not in logging.LogRecord.__dict__
            and k
            not in {
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelno",
                "levelname",
                "lineno",
                "module",
                "msecs",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            }
        }
        if extra:
            data.update(extra)
        return json.dumps(data, ensure_ascii=False)

def configure_logging(level: str | int | None = None) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    level = level or os.getenv("LOG_LEVEL", "INFO")
    root.setLevel(level)

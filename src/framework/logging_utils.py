"""Structured logging for the framework.

Every log line is JSON so that driver logs can be shipped to a log analytics
workspace and queried by batch_id / table without regex parsing.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict, Optional

_CONFIGURED = False
_LOGGER_NAME = "etl_framework"


class _JsonFormatter(logging.Formatter):
    """Render a log record as a single line of JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Anything attached via logger.info(..., extra={"context": {...}}) is merged in.
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            payload.update(context)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Install the JSON handler once per driver JVM."""
    global _CONFIGURED
    logger = logging.getLogger(_LOGGER_NAME)
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
        _CONFIGURED = True
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))


class FrameworkLogger:
    """Thin wrapper that stamps a fixed context onto every message.

    The context normally carries batch_id, layer and the fully qualified target table,
    so a single query over the driver logs reconstructs one table's ingestion.
    """

    def __init__(self, context: Optional[Dict[str, Any]] = None, level: str = "INFO"):
        configure_logging(level)
        self._logger = logging.getLogger(_LOGGER_NAME)
        self._context: Dict[str, Any] = dict(context or {})

    def child(self, **extra: Any) -> "FrameworkLogger":
        """Return a logger carrying this context plus `extra`."""
        merged = dict(self._context)
        merged.update(extra)
        clone = FrameworkLogger.__new__(FrameworkLogger)
        clone._logger = self._logger
        clone._context = merged
        return clone

    def bind(self, **extra: Any) -> None:
        """Add keys to this logger's context in place."""
        self._context.update(extra)

    def _emit(self, level: int, message: str, exc_info: bool, extra: Dict[str, Any]) -> None:
        context = dict(self._context)
        context.update(extra)
        self._logger.log(level, message, exc_info=exc_info, extra={"context": context})

    def debug(self, message: str, **extra: Any) -> None:
        self._emit(logging.DEBUG, message, False, extra)

    def info(self, message: str, **extra: Any) -> None:
        self._emit(logging.INFO, message, False, extra)

    def warning(self, message: str, **extra: Any) -> None:
        self._emit(logging.WARNING, message, False, extra)

    def error(self, message: str, exc_info: bool = False, **extra: Any) -> None:
        self._emit(logging.ERROR, message, exc_info, extra)

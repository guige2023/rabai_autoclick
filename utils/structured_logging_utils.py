"""Structured logging with JSON output, context propagation, and log levels."""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

__all__ = ["LogLevel", "LogRecord", "StructuredLogger", "setup_logging"]


class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


LOG_LEVEL_MAP = {
    "DEBUG": LogLevel.DEBUG,
    "INFO": LogLevel.INFO,
    "WARNING": LogLevel.WARNING,
    "WARN": LogLevel.WARNING,
    "ERROR": LogLevel.ERROR,
    "CRITICAL": LogLevel.CRITICAL,
}


@dataclass
class LogRecord:
    """A structured log record."""
    timestamp: float
    level: LogLevel
    logger: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    exc_info: str | None = None
    location: str = ""

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S.%f", time.gmtime(self.timestamp))[:-3] + "Z",
            "level": self.level.name,
            "logger": self.logger,
            "msg": self.message,
        }
        if self.context:
            out["context"] = self.context
        if self.exc_info:
            out["exc"] = self.exc_info
        if self.location:
            out["loc"] = self.location
        return out

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


class StructuredLogger:
    """Logger that outputs structured JSON with context propagation."""

    def __init__(
        self,
        name: str,
        min_level: LogLevel = LogLevel.INFO,
        output: Any = None,
        include_caller: bool = False,
    ) -> None:
        self.name = name
        self._min_level = min_level
        self._output = output or sys.stdout
        self._include_caller = include_caller
        self._context: dict[str, Any] = {}
        self._local = threading.local()

    def bind(self, **kwargs: Any) -> StructuredLogger:
        """Return a new logger with additional bound context."""
        new_logger = self._copy()
        new_logger._context = {**self._context, **kwargs}
        return new_logger

    def _copy(self) -> StructuredLogger:
        logger = StructuredLogger(self.name, self._min_level, self._output, self._include_caller)
        logger._context = self._context.copy()
        return logger

    def _log(self, level: LogLevel, msg: str, **kwargs: Any) -> None:
        if level.value < self._min_level.value:
            return

        ctx = {**self._context, **kwargs}
        exc_info = None
        if kwargs.get("exc_info"):
            import traceback
            exc_info = traceback.format_exc()

        import inspect
        location = ""
        if self._include_caller:
            frame = inspect.currentframe()
            if frame:
                caller = frame.f_back
                if caller:
                    location = f"{caller.f_code.co_filename}:{caller.f_lineno}"

        record = LogRecord(
            timestamp=time.time(),
            level=level,
            logger=self.name,
            message=msg,
            context=ctx,
            exc_info=exc_info,
            location=location,
        )

        self._output.write(record.to_json() + "\n")
        self._output.flush()

    def debug(self, msg: str, **kwargs: Any) -> None:
        self._log(LogLevel.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        self._log(LogLevel.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        self._log(LogLevel.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        self._log(LogLevel.ERROR, msg, **kwargs)

    def critical(self, msg: str, **kwargs: Any) -> None:
        self._log(LogLevel.CRITICAL, msg, **kwargs)

    def exception(self, msg: str, **kwargs: Any) -> None:
        kwargs["exc_info"] = True
        self._log(LogLevel.ERROR, msg, **kwargs)


def setup_logging(
    name: str = "app",
    level: str = "INFO",
    json_output: bool = True,
    output: Any = None,
) -> StructuredLogger:
    """Configure application-wide structured logging."""
    level_enum = LOG_LEVEL_MAP.get(level.upper(), LogLevel.INFO)
    return StructuredLogger(
        name=name,
        min_level=level_enum,
        output=output or sys.stdout,
        include_caller=level_enum == LogLevel.DEBUG,
    )

"""Logging configuration utilities: structured logging, multi-output, and log levels."""

from __future__ import annotations

import logging
import sys
import json
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "LogConfig",
    "LoggingConfigurator",
    "structured_logger",
]


@dataclass
class LogConfig:
    """Logging configuration."""

    level: str = "INFO"
    format: str = "json"
    output: list[str] = field(default_factory=lambda: ["stdout"])
    handlers: dict[str, dict[str, Any]] = field(default_factory=dict)


class StructuredFormatter(logging.Formatter):
    """Format log records as structured JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        return json.dumps(log_data)


class LoggingConfigurator:
    """Configure logging with multiple handlers and formats."""

    def __init__(self) -> None:
        self._handlers: dict[str, logging.Handler] = {}

    def configure(self, config: LogConfig) -> None:
        """Configure logging from a LogConfig object."""
        root = logging.getLogger()
        root.setLevel(getattr(logging, config.level.upper()))

        for handler_name, handler in self._handlers.items():
            root.removeHandler(handler)

        self._handlers.clear()

        if "stdout" in config.output:
            handler = logging.StreamHandler(sys.stdout)
            if config.format == "json":
                handler.setFormatter(StructuredFormatter())
            else:
                handler.setFormatter(
                    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
                )
            root.addHandler(handler)
            self._handlers["stdout"] = handler

        if "stderr" in config.output:
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            root.addHandler(handler)
            self._handlers["stderr"] = handler

    def add_file_handler(
        self,
        filename: str,
        level: str = "DEBUG",
        format_: str = "json",
    ) -> None:
        handler = logging.FileHandler(filename)
        handler.setLevel(getattr(logging, level.upper()))
        if format_ == "json":
            handler.setFormatter(StructuredFormatter())
        else:
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
        root = logging.getLogger()
        root.addHandler(handler)
        self._handlers[filename] = handler


def structured_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Get a structured JSON logger."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)

    return logger

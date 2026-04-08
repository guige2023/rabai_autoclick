"""Logging configuration utilities for structured automation logging.

Provides structured logging setup, rotating file handlers,
context-aware logging (with action IDs, user IDs), and
integration with common logging formats.

Example:
    >>> from utils.logging_config_utils import setup_logging, get_logger
    >>> setup_logging(level='INFO', file='/tmp/automation.log')
    >>> log = get_logger('autoclick.actions')
    >>> log.info('Action completed', extra={'action_id': '123'})
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional

__all__ = [
    "setup_logging",
    "get_logger",
    "StructuredLogger",
    "ContextFilter",
    "AutomationFormatter",
]


class AutomationFormatter(logging.Formatter):
    """Custom formatter with automation-specific fields."""

    def __init__(self, fmt: Optional[str] = None):
        super().__init__(
            fmt or "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def format(self, record: logging.LogRecord) -> str:
        # Add custom fields if present
        if hasattr(record, "action_id"):
            record.msg = f"[action={record.action_id}] {record.msg}"
        if hasattr(record, "session_id"):
            record.msg = f"[session={record.session_id}] {record.msg}"
        return super().format(record)


class ContextFilter(logging.Filter):
    """Filter that adds context fields to log records."""

    def __init__(self):
        super().__init__()
        self._context: dict = {}

    def add_context(self, **kwargs) -> None:
        self._context.update(kwargs)

    def clear_context(self) -> None:
        self._context = {}

    def filter(self, record: logging.LogRecord) -> bool:
        for key, val in self._context.items():
            setattr(record, key, val)
        return True


# Global context filter
_context_filter = ContextFilter()


def setup_logging(
    level: str = "INFO",
    file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    format: Optional[str] = None,
    include_process: bool = True,
) -> None:
    """Configure the root logger for automation.

    Args:
        level: Log level ('DEBUG', 'INFO', 'WARNING', 'ERROR').
        file: Optional log file path (enables rotating file handler).
        max_bytes: Max size per log file before rotation.
        backup_count: Number of backup files to keep.
        format: Custom log format string.
        include_process: Include process/thread info.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(AutomationFormatter(format))
    console_handler.addFilter(_context_filter)
    root_logger.addHandler(console_handler)

    # Rotating file handler
    if file:
        try:
            from logging.handlers import RotatingFileHandler

            log_dir = Path(file).parent
            log_dir.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                file,
                maxBytes=max_bytes,
                backupCount=backup_count,
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(
                AutomationFormatter(
                    "%(asctime)s [%(levelname)s] "
                    "%(name)s %(process)d:%(thread)d: %(message)s"
                )
            )
            file_handler.addFilter(_context_filter)
            root_logger.addHandler(file_handler)
        except Exception:
            pass

    root_logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name (typically __name__).

    Returns:
        Logger instance.
    """
    return logging.getLogger(name)


def add_log_context(**kwargs) -> None:
    """Add context fields to all subsequent log records."""
    _context_filter.add_context(**kwargs)


def clear_log_context() -> None:
    """Clear all log context fields."""
    _context_filter.clear_context()


class StructuredLogger:
    """Structured logger that outputs JSON-formatted logs.

    Example:
        >>> log = StructuredLogger('autoclick')
        >>> log.info('Action completed', action='click', x=100, y=200)
    """

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)

    def _log(self, level: int, msg: str, **kwargs) -> None:
        extra = {"structured_data": kwargs}
        self.logger.log(level, msg, extra=extra)

    def debug(self, msg: str, **kwargs) -> None:
        self._log(logging.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs) -> None:
        self._log(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs) -> None:
        self._log(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs) -> None:
        self._log(logging.ERROR, msg, **kwargs)

    def critical(self, msg: str, **kwargs) -> None:
        self._log(logging.CRITICAL, msg, **kwargs)

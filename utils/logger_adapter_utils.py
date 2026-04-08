"""
Logger Adapter Utilities

Provides utilities for adapting logging across
different logging frameworks in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass
import logging


class LoggerAdapter:
    """
    Adapter for various logging frameworks.
    
    Provides a unified interface for logging
    across different backends.
    """

    def __init__(self, name: str = "automation") -> None:
        self._logger = logging.getLogger(name)
        self._handlers: list[Callable[[str, str, dict], None]] = []

    def set_level(self, level: int) -> None:
        """Set logging level."""
        self._logger.setLevel(level)

    def add_handler(self, handler: Callable[[str, str, dict], None]) -> None:
        """Add a custom log handler."""
        self._handlers.append(handler)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self._logger.debug(message, extra=kwargs)
        self._notify_handlers("DEBUG", message, kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self._logger.info(message, extra=kwargs)
        self._notify_handlers("INFO", message, kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self._logger.warning(message, extra=kwargs)
        self._notify_handlers("WARNING", message, kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self._logger.error(message, extra=kwargs)
        self._notify_handlers("ERROR", message, kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message."""
        self._logger.critical(message, extra=kwargs)
        self._notify_handlers("CRITICAL", message, kwargs)

    def _notify_handlers(
        self,
        level: str,
        message: str,
        context: dict[str, Any],
    ) -> None:
        """Notify custom handlers."""
        for handler in self._handlers:
            handler(level, message, context)

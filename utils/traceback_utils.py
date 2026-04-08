"""Traceback utilities for RabAI AutoClick.

Provides:
- Exception traceback formatting
- Stack trace utilities
- Error chain handling
"""

from __future__ import annotations

import sys
import traceback
from typing import (
    Any,
    Callable,
    List,
    Optional,
)


def format_exception(exc: Exception) -> str:
    """Format an exception as a string.

    Args:
        exc: Exception to format.

    Returns:
        Formatted traceback string.
    """
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def get_traceback_string(exc: Exception) -> str:
    """Get traceback as a string.

    Args:
        exc: Exception object.

    Returns:
        Traceback string.
    """
    return traceback.format_exc()


def extract_stackframes(tb: Optional[Any]) -> List[str]:
    """Extract frame info from traceback.

    Args:
        tb: Traceback object.

    Returns:
        List of formatted frame strings.
    """
    if tb is None:
        return []
    return traceback.format_tb(tb)


def current_stack() -> List[str]:
    """Get current call stack.

    Returns:
        List of stack frame strings.
    """
    return traceback.format_stack()


def reraise(
    exc: Exception,
    cause: Optional[Exception] = None,
) -> None:
    """Reraise an exception with a cause.

    Args:
        exc: Exception to reraise.
        cause: Original cause.
    """
    if cause is not None:
        exc.__cause__ = cause
    raise exc


__all__ = [
    "format_exception",
    "get_traceback_string",
    "extract_stackframes",
    "current_stack",
    "reraise",
]

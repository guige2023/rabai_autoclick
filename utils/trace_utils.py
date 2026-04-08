"""
Trace Utilities

Provides utilities for tracing execution
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TraceEntry:
    """An entry in a trace."""
    timestamp: datetime
    event: str
    data: dict[str, Any] | None = None


class Trace:
    """
    Collects execution traces.
    
    Records events with timestamps for
    debugging and analysis.
    """

    def __init__(self, name: str = "trace") -> None:
        self._name = name
        self._entries: list[TraceEntry] = []

    def record(
        self,
        event: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Record a trace entry."""
        entry = TraceEntry(
            timestamp=datetime.now(),
            event=event,
            data=data,
        )
        self._entries.append(entry)

    def get_entries(self) -> list[TraceEntry]:
        """Get all trace entries."""
        return list(self._entries)

    def clear(self) -> None:
        """Clear all entries."""
        self._entries.clear()

    def export(self) -> list[dict[str, Any]]:
        """Export trace as list of dicts."""
        return [
            {
                "timestamp": e.timestamp.isoformat(),
                "event": e.event,
                "data": e.data,
            }
            for e in self._entries
        ]


def trace_function(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to trace function calls."""
    def wrapper(*args: Any, **kwargs: Any) -> T:
        result = func(*args, **kwargs)
        return result
    return wrapper

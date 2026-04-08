"""
Render watch utilities for monitoring element re-renders.

Provides render change detection and monitoring for UI elements
to track when elements are redrawn or updated.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class RenderEvent:
    """A single render event."""
    element_id: str
    timestamp_ms: float
    render_count: int
    bounds_changed: bool
    content_changed: bool
    style_changed: bool


@dataclass
class RenderWatchStats:
    """Statistics for a watched element."""
    element_id: str
    total_renders: int
    first_render_ms: float
    last_render_ms: float
    avg_render_interval_ms: float
    render_per_second: float


class RenderWatcher:
    """Watches elements for render changes."""

    def __init__(self):
        self._watches: dict[str, dict] = {}
        self._callbacks: list[Callable[[RenderEvent], None]] = []

    def watch(self, element_id: str) -> None:
        """Start watching an element."""
        self._watches[element_id] = {
            "render_count": 0,
            "first_render_ms": 0.0,
            "last_render_ms": 0.0,
            "last_bounds": None,
            "last_content": None,
        }

    def unwatch(self, element_id: str) -> None:
        """Stop watching an element."""
        self._watches.pop(element_id, None)

    def record_render(
        self,
        element_id: str,
        bounds: Optional[tuple[float, float, float, float]] = None,
        content: Optional[str] = None,
    ) -> RenderEvent:
        """Record a render event for an element."""
        if element_id not in self._watches:
            self.watch(element_id)

        watch = self._watches[element_id]
        timestamp = time.time() * 1000
        watch["render_count"] += 1

        if watch["first_render_ms"] == 0:
            watch["first_render_ms"] = timestamp

        # Check what changed
        bounds_changed = bounds_changed_fn(watch["last_bounds"], bounds)
        content_changed = watch["last_content"] != content

        watch["last_render_ms"] = timestamp
        watch["last_bounds"] = bounds
        watch["last_content"] = content

        event = RenderEvent(
            element_id=element_id,
            timestamp_ms=timestamp,
            render_count=watch["render_count"],
            bounds_changed=bounds_changed,
            content_changed=content_changed,
            style_changed=False,  # Simplified
        )

        for callback in self._callbacks:
            callback(event)

        return event

    def add_callback(self, callback: Callable[[RenderEvent], None]) -> None:
        """Add a render event callback."""
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[RenderEvent], None]) -> None:
        """Remove a callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def get_stats(self, element_id: str) -> Optional[RenderWatchStats]:
        """Get statistics for a watched element."""
        if element_id not in self._watches:
            return None

        watch = self._watches[element_id]
        if watch["render_count"] == 0:
            return RenderWatchStats(
                element_id=element_id,
                total_renders=0,
                first_render_ms=0.0,
                last_render_ms=0.0,
                avg_render_interval_ms=0.0,
                render_per_second=0.0,
            )

        duration = watch["last_render_ms"] - watch["first_render_ms"]
        avg_interval = duration / (watch["render_count"] - 1) if watch["render_count"] > 1 else 0.0
        rps = watch["render_count"] / (duration / 1000.0) if duration > 0 else 0.0

        return RenderWatchStats(
            element_id=element_id,
            total_renders=watch["render_count"],
            first_render_ms=watch["first_render_ms"],
            last_render_ms=watch["last_render_ms"],
            avg_render_interval_ms=avg_interval,
            render_per_second=rps,
        )

    def is_stable(self, element_id: str, threshold_ms: float = 500.0) -> bool:
        """Check if an element is stable (not rendering frequently)."""
        stats = self.get_stats(element_id)
        if not stats or stats.total_renders == 0:
            return True
        return stats.avg_render_interval_ms >= threshold_ms


def bounds_changed_fn(
    prev: Optional[tuple[float, float, float, float]],
    curr: Optional[tuple[float, float, float, float]],
) -> bool:
    """Check if bounds changed between two states."""
    if prev is None or curr is None:
        return prev != curr
    return prev != curr


__all__ = ["RenderWatcher", "RenderEvent", "RenderWatchStats"]

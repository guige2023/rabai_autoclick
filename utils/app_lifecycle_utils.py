"""
Application Lifecycle Utilities

Track and manage the lifecycle states of applications being
automated: launch, activation, deactivation, and termination.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Callable, Optional


class AppLifecycleState(Enum):
    """Application lifecycle states."""
    UNKNOWN = auto()
    NOT_RUNNING = auto()
    LAUNCHING = auto()
    RUNNING = auto()
    ACTIVATED = auto()
    DEACTIVATED = auto()
    TERMINATING = auto()
    TERMINATED = auto()


@dataclass
class LifecycleTransition:
    """A lifecycle state transition event."""
    app_id: str
    from_state: AppLifecycleState
    to_state: AppLifecycleState
    timestamp_ms: float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class AppLifecycleInfo:
    """Current lifecycle information for an application."""
    app_id: str
    bundle_id: str
    state: AppLifecycleState
    launched_at_ms: Optional[float] = None
    activated_at_ms: Optional[float] = None
    transition_count: int = 0


class AppLifecycleMonitor:
    """Monitor application lifecycle transitions."""

    def __init__(self):
        self._apps: dict[str, AppLifecycleInfo] = {}
        self._history: list[LifecycleTransition] = []
        self._handlers: dict[AppLifecycleState, list[Callable]] = {}

    def register_handler(self, state: AppLifecycleState, handler: Callable[[LifecycleTransition], None]) -> None:
        """Register a callback for a specific lifecycle state."""
        if state not in self._handlers:
            self._handlers[state] = []
        self._handlers[state].append(handler)

    def update_state(self, app_id: str, bundle_id: str, new_state: AppLifecycleState) -> LifecycleTransition:
        """Update the lifecycle state of an application."""
        current = self._apps.get(app_id)
        old_state = current.state if current else AppLifecycleState.UNKNOWN

        now_ms = time.time() * 1000
        transition = LifecycleTransition(
            app_id=app_id,
            from_state=old_state,
            to_state=new_state,
            timestamp_ms=now_ms,
        )
        self._history.append(transition)

        self._apps[app_id] = AppLifecycleInfo(
            app_id=app_id,
            bundle_id=bundle_id,
            state=new_state,
            launched_at_ms=now_ms if new_state == AppLifecycleState.LAUNCHING else (current.launched_at_ms if current else None),
            activated_at_ms=now_ms if new_state == AppLifecycleState.ACTIVATED else (current.activated_at_ms if current else None),
            transition_count=(current.transition_count + 1 if current else 1),
        )

        if new_state in self._handlers:
            for handler in self._handlers[new_state]:
                handler(transition)

        return transition

    def get_info(self, app_id: str) -> Optional[AppLifecycleInfo]:
        """Get current lifecycle info for an application."""
        return self._apps.get(app_id)

    def get_state(self, app_id: str) -> AppLifecycleState:
        """Get the current state of an application."""
        info = self._apps.get(app_id)
        return info.state if info else AppLifecycleState.UNKNOWN

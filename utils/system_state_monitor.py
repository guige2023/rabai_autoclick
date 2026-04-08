"""
System State Monitor.

Monitor overall macOS system state including active windows,
running applications, menu bar state, and system events.

Usage:
    from utils.system_state_monitor import SystemStateMonitor

    monitor = SystemStateMonitor()
    monitor.start()
    state = monitor.get_current_state()
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from collections import deque
import time

if TYPE_CHECKING:
    pass


@dataclass
class SystemState:
    """Current system state snapshot."""
    timestamp: float
    frontmost_app: Optional[str] = None
    frontmost_bundle_id: Optional[str] = None
    window_count: int = 0
    running_app_count: int = 0
    menu_bar_app: Optional[str] = None
    active_displays: int = 1
    mouse_position: tuple = field(default_factory=lambda: (0, 0))


@dataclass
class StateChange:
    """Represents a change in system state."""
    change_type: str
    old_state: Optional[SystemState]
    new_state: SystemState
    timestamp: float = field(default_factory=time.time)


class SystemStateMonitor:
    """
    Monitor macOS system state changes.

    Tracks frontmost app, window counts, and other system
    state indicators with change callbacks.

    Example:
        monitor = SystemStateMonitor()
        monitor.on_change(lambda c: print(f"Change: {c.change_type}"))
        monitor.start()
    """

    def __init__(
        self,
        poll_interval: float = 0.5,
        max_history: int = 100,
    ) -> None:
        """
        Initialize the system state monitor.

        Args:
            poll_interval: Seconds between state checks.
            max_history: Maximum history to retain.
        """
        self._poll_interval = poll_interval
        self._max_history = max_history
        self._running = False
        self._callbacks: List[Callable[[StateChange], None]] = []
        self._history: deque[SystemState] = deque(maxlen=max_history)
        self._last_state: Optional[SystemState] = None

    def start(self) -> None:
        """Start monitoring system state."""
        self._running = True

    def stop(self) -> None:
        """Stop monitoring system state."""
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def on_change(
        self,
        callback: Callable[[StateChange], None],
    ) -> None:
        """
        Register a callback for state changes.

        Args:
            callback: Function called with StateChange on each change.
        """
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def get_current_state(self) -> SystemState:
        """
        Get the current system state.

        Returns:
            SystemState snapshot.
        """
        return self._capture_state()

    def _capture_state(self) -> SystemState:
        """Capture the current system state."""
        import subprocess

        frontmost_app = None
        frontmost_bundle_id = None

        try:
            result = subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to get '
                 "{name, bundle identifier} of first process "
                 "whose frontmost is true"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                parts = result.stdout.strip().rsplit(", ", 1)
                if len(parts) == 2:
                    frontmost_app = parts[0].strip()
                    frontmost_bundle_id = parts[1].strip()
        except Exception:
            pass

        running_apps = 0
        try:
            result = subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to '
                 "count every process"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                running_apps = int(result.stdout.strip())
        except Exception:
            pass

        return SystemState(
            timestamp=time.time(),
            frontmost_app=frontmost_app,
            frontmost_bundle_id=frontmost_bundle_id,
            running_app_count=running_apps,
        )

    def poll(self) -> Optional[StateChange]:
        """
        Poll for state changes.

        Returns:
            StateChange if the state changed since last poll, None otherwise.
        """
        if not self._running:
            return None

        current = self._capture_state()
        self._history.append(current)

        change = None
        if self._last_state:
            if self._detect_change(self._last_state, current):
                change = StateChange(
                    change_type="state_changed",
                    old_state=self._last_state,
                    new_state=current,
                )
                for cb in self._callbacks:
                    try:
                        cb(change)
                    except Exception:
                        pass

        self._last_state = current
        return change

    def _detect_change(
        self,
        old: SystemState,
        new: SystemState,
    ) -> bool:
        """Detect if states are different."""
        return (
            old.frontmost_app != new.frontmost_app or
            old.running_app_count != new.running_app_count
        )

    def get_history(
        self,
        since: Optional[float] = None,
    ) -> List[SystemState]:
        """
        Get state history.

        Args:
            since: Optional Unix timestamp filter.

        Returns:
            List of SystemState objects.
        """
        if since is None:
            return list(self._history)
        return [s for s in self._history if s.timestamp >= since]

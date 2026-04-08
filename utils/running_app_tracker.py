"""
Running Application Tracker.

Track which applications are running, monitor frontmost app changes,
and detect application activation/deactivation events.

Usage:
    from utils.running_app_tracker import RunningAppTracker, get_running_apps

    tracker = RunningAppTracker()
    tracker.on_activation(callback)
    tracker.start()
"""

from __future__ import annotations

import time
from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from collections import deque

if TYPE_CHECKING:
    pass


@dataclass
class AppInfo:
    """Information about a running application."""
    pid: int
    name: str
    bundle_id: Optional[str] = None
    is_active: bool = False
    activation_time: Optional[float] = None
    process_name: Optional[str] = None

    def __repr__(self) -> str:
        return f"AppInfo({self.name!r}, pid={self.pid}, active={self.is_active})"


@dataclass
class AppChangeEvent:
    """Event representing an application state change."""
    event_type: str  # "activated", "deactivated", "launched", "terminated"
    app: AppInfo
    timestamp: float = field(default_factory=time.time)
    previous_app: Optional[AppInfo] = None

    def __repr__(self) -> str:
        return f"AppChangeEvent({self.event_type}, {self.app.name})"


class RunningAppTracker:
    """
    Track running applications and monitor app changes.

    Provides callbacks for app activation, deactivation, launch,
    and termination events. Useful for scripts that need to
    respond to the user's current working environment.

    Example:
        def on_app_change(event):
            print(f"Switched to: {event.app.name}")

        tracker = RunningAppTracker()
        tracker.on_change(on_app_change)
        tracker.start()
    """

    def __init__(
        self,
        poll_interval: float = 0.5,
        max_history: int = 100,
    ) -> None:
        """
        Initialize the tracker.

        Args:
            poll_interval: Seconds between polling checks.
            max_history: Maximum number of events to retain.
        """
        self._poll_interval = poll_interval
        self._max_history = max_history
        self._running = False
        self._callbacks: List[Callable[[AppChangeEvent], None]] = []
        self._history: deque[AppChangeEvent] = deque(maxlen=max_history)
        self._known_pids: Dict[int, AppInfo] = {}
        self._last_frontmost: Optional[AppInfo] = None

    def start(self) -> None:
        """Start tracking application changes."""
        self._running = True
        self._refresh_known_apps()

    def stop(self) -> None:
        """Stop tracking application changes."""
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def on_change(
        self,
        callback: Callable[[AppChangeEvent], None],
    ) -> None:
        """
        Register a callback for app change events.

        Args:
            callback: Function invoked with AppChangeEvent on changes.
        """
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def _emit(self, event: AppChangeEvent) -> None:
        """Emit an event to all registered callbacks."""
        self._history.append(event)
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def _refresh_known_apps(self) -> None:
        """Refresh the list of known running applications."""
        try:
            import subprocess
            result = subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to get '
                 '{name, bundle id, pid} of every process where background only is false'],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0:
                return

            lines = result.stdout.strip().split(", ")
            current_pids: Dict[int, AppInfo] = {}

            for line in lines:
                parts = line.rsplit(", ", 2)
                if len(parts) >= 3:
                    name, bundle_id, pid_str = parts[-3], parts[-2], parts[-1]
                    try:
                        pid = int(pid_str)
                        app = AppInfo(
                            pid=pid,
                            name=name.strip(),
                            bundle_id=bundle_id.strip() if bundle_id != "missing value" else None,
                        )
                        current_pids[pid] = app
                    except ValueError:
                        pass

            new_pids = set(current_pids.keys()) - set(self._known_pids.keys())
            terminated_pids = set(self._known_pids.keys()) - set(current_pids.keys())

            for pid in new_pids:
                app = current_pids[pid]
                self._known_pids[pid] = app
                self._emit(AppChangeEvent(event_type="launched", app=app))

            for pid in terminated_pids:
                app = self._known_pids[pid]
                del self._known_pids[pid]
                self._emit(AppChangeEvent(event_type="terminated", app=app))

        except Exception:
            pass

    def check_frontmost(self) -> Optional[AppInfo]:
        """
        Check which application is currently frontmost.

        Returns:
            AppInfo of the frontmost app, or None.
        """
        try:
            import subprocess
            result = subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to get '
                 '{name, bundle id, pid} of first process '
                 'whose frontmost is true'],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return None

            parts = result.stdout.strip().rsplit(", ", 2)
            if len(parts) >= 3:
                name, bundle_id, pid_str = parts[-3], parts[-2], parts[-1]
                pid = int(pid_str)
                app = AppInfo(
                    pid=pid,
                    name=name.strip(),
                    bundle_id=bundle_id.strip() if bundle_id != "missing value" else None,
                    is_active=True,
                )
                return app
        except Exception:
            pass
        return None

    def poll(self) -> None:
        """
        Poll for changes. Call this in a loop or timer.

        Detects app launches, terminations, and frontmost changes.
        """
        if not self._running:
            return

        self._refresh_known_apps()
        current_frontmost = self.check_frontmost()

        if current_frontmost and self._last_frontmost:
            if current_frontmost.pid != self._last_frontmost.pid:
                old_app = self._last_frontmost
                new_app = current_frontmost

                old_app.is_active = False
                new_app.is_active = True
                new_app.activation_time = time.time()

                self._emit(AppChangeEvent(
                    event_type="deactivated",
                    app=old_app,
                    previous_app=new_app,
                ))
                self._emit(AppChangeEvent(
                    event_type="activated",
                    app=new_app,
                    previous_app=old_app,
                ))

                self._last_frontmost = new_app
        elif current_frontmost and not self._last_frontmost:
            current_frontmost.is_active = True
            current_frontmost.activation_time = time.time()
            self._emit(AppChangeEvent(event_type="activated", app=current_frontmost))
            self._last_frontmost = current_frontmost

    def get_running_apps(self) -> List[AppInfo]:
        """
        Get all currently running applications.

        Returns:
            List of AppInfo objects.
        """
        self._refresh_known_apps()
        return list(self._known_pids.values())

    def get_history(
        self,
        since: Optional[float] = None,
        event_type: Optional[str] = None,
    ) -> List[AppChangeEvent]:
        """
        Get the event history.

        Args:
            since: Optional Unix timestamp to filter events after.
            event_type: Optional event type to filter (e.g., "activated").

        Returns:
            List of AppChangeEvent objects, newest first.
        """
        events = list(reversed(self._history))
        if since is not None:
            events = [e for e in events if e.timestamp >= since]
        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]
        return events


def get_running_apps() -> List[AppInfo]:
    """
    Convenience function to get all running apps.

    Returns:
        List of AppInfo for all running applications.
    """
    tracker = RunningAppTracker()
    return tracker.get_running_apps()

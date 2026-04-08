"""
Window Transition Detector Utility

Detects and classifies window transitions and animations on macOS.
Useful for timing automation steps to window state changes.

Example:
    >>> detector = WindowTransitionDetector()
    >>> detector.start_watch()
    >>> event = detector.wait_for_transition("zoom", timeout=5.0)
    >>> print(event.transition_type)
"""

from __future__ import annotations

import subprocess
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable


class TransitionType(Enum):
    """Window transition types."""
    UNKNOWN = "unknown"
    ZOOM = "zoom"
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"
    RESTORE = "restore"
    OPEN = "open"
    CLOSE = "close"
    HIDE = "hide"
    UNHIDE = "unhide"
    MOVE = "move"
    RESIZE = "resize"
    FULLSCREEN_ENTER = "fullscreen_enter"
    FULLSCREEN_EXIT = "fullscreen_exit"
    LAYER_CHANGE = "layer_change"


@dataclass
class TransitionEvent:
    """A detected window transition."""
    window_id: int
    window_title: str
    transition_type: TransitionType
    timestamp: float = field(default_factory=time.time)
    duration: float = 0.0  # Animation duration if known
    from_state: Optional[str] = None
    to_state: Optional[str] = None


class WindowTransitionDetector:
    """
    Detects window transitions and animations.

    Uses CGWindowList observation and window bounds monitoring.
    """

    def __init__(self) -> None:
        self._watching = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: list[Callable[[TransitionEvent], None]] = []
        self._previous_windows: dict[int, tuple[int, int, int, int]] = {}
        self._lock = threading.Lock()

    def add_callback(self, callback: Callable[[TransitionEvent], None]) -> None:
        """Register a callback for transition events."""
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[TransitionEvent], None]) -> None:
        """Remove a registered callback."""
        self._callbacks.remove(callback)

    def start_watch(self, interval: float = 0.1) -> None:
        """Start watching for transitions."""
        if self._watching:
            return
        self._watching = True
        self._thread = threading.Thread(
            target=self._watch_loop,
            args=(interval,),
            daemon=True,
        )
        self._thread.start()

    def stop_watch(self) -> None:
        """Stop watching for transitions."""
        self._watching = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _get_window_list(self) -> list[dict]:
        """Get current window list via osascript."""
        script = """
        tell application "System Events"
            set windowList to {}
            try
                set frontApp to first process whose frontmost is true
                set appName to name of frontApp
                tell process appName
                    set winCount to count of windows
                    repeat with i from 1 to winCount
                        try
                            set wName to name of window i
                            set wPos to position of window i
                            set wSize to size of window i
                            set wId to id of window i
                            set windowList to windowList & {{wName, wPos, wSize, wId}}
                        end try
                    end repeat
                end tell
            end try
        end tell
        return windowList
        """

        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2.0,
            )
            if result.returncode == 0:
                return self._parse_window_list(result.stdout)
        except Exception:
            pass
        return []

    def _parse_window_list(self, output: str) -> list[dict]:
        """Parse osascript window list output."""
        windows: list[dict] = []
        for line in output.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) >= 5:
                try:
                    windows.append({
                        "name": parts[0].strip(),
                        "x": int(parts[1].strip()),
                        "y": int(parts[2].strip()),
                        "width": int(parts[3].strip()),
                        "height": int(parts[4].strip()),
                        "id": int(parts[5].strip()) if len(parts) > 5 else 0,
                    })
                except (ValueError, IndexError):
                    pass
        return windows

    def _classify_transition(
        self,
        old: dict,
        new: dict,
    ) -> TransitionType:
        """Classify the transition between two window states."""
        old_x, old_y = old.get("x", 0), old.get("y", 0)
        old_w, old_h = old.get("width", 0), old.get("height", 0)
        new_x, new_y = new.get("x", 0), new.get("y", 0)
        new_w, new_h = new.get("width", 0), new.get("height", 0)

        old_area = old_w * old_h
        new_area = new_w * new_h

        if old_area == 0 and new_area > 0:
            return TransitionType.OPEN
        if old_area > 0 and new_area == 0:
            return TransitionType.CLOSE
        if old_y < 0 or old_x < 0:
            if new_y >= 0 and new_x >= 0:
                return TransitionType.RESTORE

        if old_w < new_w and old_h < new_h:
            if abs(new_w - old_w) > 50 or abs(new_h - old_h) > 50:
                return TransitionType.ZOOM
        if old_w > new_w and old_h > new_h:
            if abs(old_w - new_w) > 50 or abs(old_h - new_h) > 50:
                return TransitionType.MINIMIZE

        if (old_x, old_y) != (new_x, new_y):
            return TransitionType.MOVE
        if (old_w, old_h) != (new_w, new_h):
            return TransitionType.RESIZE

        return TransitionType.UNKNOWN

    def _watch_loop(self, interval: float) -> None:
        """Background watch loop."""
        while self._watching:
            try:
                windows = self._get_window_list()
                with self._lock:
                    for win in windows:
                        wid = win["id"]
                        bounds = (win["x"], win["y"], win["width"], win["height"])
                        if wid in self._previous_windows:
                            prev = self._previous_windows[wid]
                            if prev != bounds:
                                # Find previous title
                                prev_title = "Unknown"
                                transition = self._classify_transition(
                                    {"x": prev[0], "y": prev[1], "width": prev[2], "height": prev[3]},
                                    win,
                                )
                                if transition != TransitionType.UNKNOWN:
                                    event = TransitionEvent(
                                        window_id=wid,
                                        window_title=win.get("name", "Unknown"),
                                        transition_type=transition,
                                    )
                                    self._dispatch(event)
                        self._previous_windows[wid] = bounds

                    # Clean up closed windows
                    active_ids = {w["id"] for w in windows}
                    closed = [
                        wid for wid in self._previous_windows
                        if wid not in active_ids
                    ]
                    for wid in closed:
                        del self._previous_windows[wid]
            except Exception:
                pass
            time.sleep(interval)

    def _dispatch(self, event: TransitionEvent) -> None:
        """Dispatch event to all callbacks."""
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def wait_for_transition(
        self,
        transition_type: str | TransitionType,
        timeout: float = 10.0,
        window_name: Optional[str] = None,
    ) -> Optional[TransitionEvent]:
        """
        Wait for a specific transition type.

        Args:
            transition_type: Type to wait for.
            timeout: Maximum seconds to wait.
            window_name: Optional window name filter.

        Returns:
            TransitionEvent if detected, None on timeout.
        """
        if isinstance(transition_type, str):
            try:
                transition_type = TransitionType(transition_type)
            except ValueError:
                return None

        result: list[TransitionEvent] = []
        lock = threading.Lock()

        def callback(event: TransitionEvent):
            if event.transition_type == transition_type:
                if window_name is None or window_name.lower() in event.window_title.lower():
                    with lock:
                        result.append(event)

        self.add_callback(callback)
        try:
            start = time.time()
            while time.time() - start < timeout:
                with lock:
                    if result:
                        return result[0]
                time.sleep(0.1)
        finally:
            self.remove_callback(callback)

        return None

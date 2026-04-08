"""
Focus Manager Utility

Tracks and manages keyboard focus across macOS windows and elements.
Provides focus ring information and focus change notifications.

Example:
    >>> manager = FocusManager()
    >>> focused = manager.get_focused_element()
    >>> print(focused.name if focused else "No focus")
    >>> manager.wait_for_focus("TextField", timeout=5.0)
"""

from __future__ import annotations

import subprocess
import time
import threading
from dataclasses import dataclass, field
from typing import Optional, Callable


@dataclass
class FocusInfo:
    """Information about the currently focused element."""
    window_title: Optional[str] = None
    window_id: Optional[int] = None
    element_role: Optional[str] = None
    element_name: Optional[str] = None
    element_value: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class FocusManager:
    """
    Manages keyboard focus tracking on macOS.

    Provides:
        - Current focus information
        - Focus change callbacks
        - Waiting for specific focus states
    """

    def __init__(self) -> None:
        self._callbacks: list[Callable[[FocusInfo], None]] = []
        self._last_focus: Optional[FocusInfo] = None
        self._lock = threading.Lock()

    def get_focused_element(self) -> Optional[FocusInfo]:
        """
        Get information about the currently focused element.

        Returns:
            FocusInfo with current focus details, or None on failure.
        """
        try:
            # Use osascript to query accessibility
            script = """
            tell application "System Events"
                set focusedApp to (path to frontmost application)
                set frontApp to name of first process whose frontmost is true
            end tell

            tell application frontApp
                try
                    set winTitle to name of front window
                on error
                    set winTitle to ""
                end try
            end tell

            return frontApp & "||" & winTitle
            """

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2.0,
            )

            if result.returncode == 0:
                parts = result.stdout.strip().split("||")
                app_name = parts[0] if len(parts) > 0 else None
                window_title = parts[1] if len(parts) > 1 else None

                return FocusInfo(
                    window_title=window_title,
                    element_role="window",
                    element_name=app_name,
                )
        except Exception:
            pass
        return None

    def get_frontmost_window_info(self) -> Optional[FocusInfo]:
        """
        Get details about the frontmost window using CGWindowList.

        Returns:
            FocusInfo with window details.
        """
        try:
            script = """
            tell application "System Events"
                set frontApp to first process whose frontmost is true
                set appName to name of frontApp
                try
                    set winTitle to name of front window of frontApp
                on error
                    set winTitle to ""
                end try
            end tell

            return appName & "||" & winTitle
            """

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2.0,
            )

            if result.returncode == 0:
                parts = result.stdout.strip().split("||")
                return FocusInfo(
                    window_title=parts[1] if len(parts) > 1 else None,
                    element_name=parts[0] if parts else None,
                )
        except Exception:
            pass
        return None

    def add_focus_callback(self, callback: Callable[[FocusInfo], None]) -> None:
        """Register a callback for focus changes."""
        with self._lock:
            self._callbacks.append(callback)

    def remove_focus_callback(self, callback: Callable[[FocusInfo], None]) -> None:
        """Remove a registered callback."""
        with self._lock:
            self._callbacks.remove(callback)

    def _notify_focus_change(self, info: FocusInfo) -> None:
        """Notify all callbacks of a focus change."""
        with self._lock:
            callbacks = list(self._callbacks)
        for cb in callbacks:
            try:
                cb(info)
            except Exception:
                pass

    def wait_for_focus(
        self,
        name_contains: Optional[str] = None,
        role: Optional[str] = None,
        timeout: float = 10.0,
    ) -> bool:
        """
        Wait for focus to match criteria.

        Args:
            name_contains: Wait until element name contains this string.
            role: Wait until element has this role.
            timeout: Maximum seconds to wait.

        Returns:
            True if conditions met, False on timeout.
        """
        start = time.time()
        while time.time() - start < timeout:
            info = self.get_focused_element()
            if info:
                name_match = not name_contains or (
                    info.element_name and name_contains.lower() in info.element_name.lower()
                )
                role_match = not role or info.element_role == role
                if name_match and role_match:
                    return True
            time.sleep(0.2)
        return False

    def wait_for_focus_loss(
        self,
        name_contains: Optional[str] = None,
        timeout: float = 5.0,
    ) -> bool:
        """
        Wait until focus moves away from matching element.

        Args:
            name_contains: Element name that should lose focus.
            timeout: Maximum seconds to wait.

        Returns:
            True when focus is lost, False on timeout.
        """
        start = time.time()
        while time.time() - start < timeout:
            info = self.get_focused_element()
            if info:
                if not name_contains:
                    return True
                if not (info.element_name and name_contains.lower() in info.element_name.lower()):
                    return True
            time.sleep(0.2)
        return False

    def start_watchdog(
        self,
        interval: float = 0.5,
        on_change: Optional[Callable[[FocusInfo, FocusInfo], None]] = None,
    ) -> threading.Event:
        """
        Start a background focus watchdog thread.

        Args:
            interval: Seconds between checks.
            on_change: Optional callback(old_info, new_info).

        Returns:
            Threading event to signal stop.
        """
        stop_event = threading.Event()

        def run():
            last: Optional[FocusInfo] = None
            while not stop_event.wait(interval):
                current = self.get_focused_element()
                if current != last:
                    if on_change and last is not None:
                        try:
                            on_change(last, current)
                        except Exception:
                            pass
                    self._notify_focus_change(current)
                    last = current

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return stop_event

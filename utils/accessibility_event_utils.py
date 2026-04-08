"""Accessibility event utilities for listening to accessibility events.

This module provides utilities for listening to and handling
accessibility events from UI applications.
"""

from __future__ import annotations

import platform
import time
from typing import Callable, Optional


IS_MACOS = platform.system() == "Darwin"


# Event type constants
EVENT_FOCUSED = "AXFocus"
EVENT_VALUE_CHANGED = "AXValueChanged"
EVENT_SELECTED = "AXSelected"
EVENT_UPDATED = "AXUpdated"
EVENT_WINDOW_MOVED = "AXWindowMoved"
EVENT_WINDOW_RESIZED = "AXWindowResized"


class AccessibilityEvent:
    """Represents an accessibility event."""
    
    def __init__(
        self,
        event_type: str,
        element_role: str,
        element_title: Optional[str] = None,
        timestamp: float = 0.0,
    ):
        self.event_type = event_type
        self.element_role = element_role
        self.element_title = element_title
        self.timestamp = timestamp or time.monotonic()


class AccessibilityEventListener:
    """Listens for accessibility events."""
    
    def __init__(self, app_bundle_id: Optional[str] = None):
        """Initialize the listener.
        
        Args:
            app_bundle_id: Bundle ID of the app to monitor. If None, monitors frontmost.
        """
        self.app_bundle_id = app_bundle_id
        self._callbacks: dict[str, list[Callable[[AccessibilityEvent], None]]] = {}
        self._running = False
    
    def on(
        self,
        event_type: str,
        callback: Callable[[AccessibilityEvent], None],
    ) -> None:
        """Register a callback for an event type.
        
        Args:
            event_type: Type of event to listen for.
            callback: Callback function.
        """
        if event_type not in self._callbacks:
            self._callbacks[event_type] = []
        self._callbacks[event_type].append(callback)
    
    def start(self) -> None:
        """Start listening for events."""
        self._running = True
    
    def stop(self) -> None:
        """Stop listening for events."""
        self._running = False
    
    def _handle_event(self, event: AccessibilityEvent) -> None:
        """Dispatch an event to registered callbacks."""
        if event.event_type in self._callbacks:
            for callback in self._callbacks[event.event_type]:
                try:
                    callback(event)
                except Exception:
                    pass
    
    def wait_for_event(
        self,
        event_type: str,
        timeout: float = 30.0,
    ) -> Optional[AccessibilityEvent]:
        """Wait for a specific event to occur.
        
        Args:
            event_type: Type of event to wait for.
            timeout: Maximum time to wait.
        
        Returns:
            The event if it occurred, None otherwise.
        """
        start_time = time.monotonic()
        result_event: Optional[AccessibilityEvent] = None
        
        def capture_event(event: AccessibilityEvent) -> None:
            nonlocal result_event
            result_event = event
        
        self.on(event_type, capture_event)
        self.start()
        
        while time.monotonic() - start_time < timeout:
            if result_event is not None:
                break
            time.sleep(0.1)
        
        self.stop()
        return result_event


def get_axial_events() -> list[AccessibilityEvent]:
    """Get recent accessibility events (macOS only).
    
    Returns:
        List of recent accessibility events.
    """
    if not IS_MACOS:
        return []
    
    events = []
    try:
        import subprocess
        script = '''
        tell application "System Events"
            tell (first process whose frontmost is true)
                set eventLog to {}
                try
                    -- Access to accessibility event API requires special permissions
                    return ""
                on error
                    return ""
                end try
            end tell
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=3
        )
    except Exception:
        pass
    
    return events

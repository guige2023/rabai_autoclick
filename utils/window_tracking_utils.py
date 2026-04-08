"""
Window tracking utilities for monitoring window lifecycle events.

Provides window open/close/focus detection and tracking
for automation workflows.
"""

from __future__ import annotations

import time
import subprocess
import threading
from typing import Optional, List, Dict, Callable, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class WindowEvent(Enum):
    """Window lifecycle events."""
    OPENED = "opened"
    CLOSED = "closed"
    FOCUSED = "focused"
    UNFOCUSED = "unfocused"
    MINIMIZED = "minimized"
    MAXIMIZED = "maximized"
    MOVED = "moved"
    RESIZED = "resized"


@dataclass
class WindowState:
    """Window state snapshot."""
    window_id: int
    owner_name: str
    title: str
    bounds: tuple[int, int, int, int]
    is_minimized: bool
    is_on_screen: bool
    level: int
    timestamp: float


@dataclass
class WindowEventRecord:
    """Record of window event."""
    event: WindowEvent
    window_id: int
    owner_name: str
    title: str
    timestamp: float
    data: Dict[str, Any] = field(default_factory=dict)


class WindowTracker:
    """Tracks window lifecycle events."""
    
    def __init__(self, poll_interval: float = 0.5):
        """
        Initialize window tracker.
        
        Args:
            poll_interval: Polling interval in seconds.
        """
        self.poll_interval = poll_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: Dict[WindowEvent, List[Callable]] = {
            event: [] for event in WindowEvent
        }
        self._event_history: List[WindowEventRecord] = []
        self._known_windows: Dict[int, WindowState] = {}
        self._lock = threading.Lock()
    
    def start(self) -> None:
        """Start tracking windows."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._track_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop tracking windows."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
    
    def on_event(self, event: WindowEvent,
                 callback: Callable[[WindowEventRecord], None]) -> None:
        """
        Register event callback.
        
        Args:
            event: Event to listen for.
            callback: Callback function.
        """
        with self._lock:
            self._callbacks[event].append(callback)
    
    def get_history(self, event_type: Optional[WindowEvent] = None,
                    limit: int = 100) -> List[WindowEventRecord]:
        """
        Get event history.
        
        Args:
            event_type: Optional filter by type.
            limit: Max records to return.
            
        Returns:
            List of event records.
        """
        with self._lock:
            history = self._event_history.copy()
        
        if event_type:
            history = [h for h in history if h.event == event_type]
        
        return history[-limit:]
    
    def _track_loop(self) -> None:
        """Main tracking loop."""
        while self._running:
            try:
                self._check_windows()
            except Exception:
                pass
            time.sleep(self.poll_interval)
    
    def _check_windows(self) -> None:
        """Check for window changes."""
        import Quartz
        
        current_windows = {}
        window_list = Quartz.CGWindowListCopyWindowInfo(
            Quartz.kCGWindowListOptionOnScreenOnly,
            0
        )
        
        for window in window_list:
            wid = window.get('kCGWindowNumber', 0)
            if wid == 0:
                continue
            
            bounds = window.get('kCGWindowBounds', {})
            state = WindowState(
                window_id=wid,
                owner_name=window.get('kCGWindowOwnerName', ''),
                title=window.get('kCGWindowName', ''),
                bounds=(
                    int(bounds.get('X', 0)),
                    int(bounds.get('Y', 0)),
                    int(bounds.get('Width', 0)),
                    int(bounds.get('Height', 0))
                ),
                is_minimized=False,
                is_on_screen=True,
                level=window.get('kCGWindowLayer', 0),
                timestamp=time.time()
            )
            current_windows[wid] = state
        
        with self._lock:
            for wid, state in current_windows.items():
                if wid not in self._known_windows:
                    event = WindowEventRecord(
                        event=WindowEvent.OPENED,
                        window_id=wid,
                        owner_name=state.owner_name,
                        title=state.title,
                        timestamp=time.time()
                    )
                    self._event_history.append(event)
                    self._emit(event)
                elif self._known_windows[wid].title != state.title:
                    event = WindowEventRecord(
                        event=WindowEvent.FOCUSED,
                        window_id=wid,
                        owner_name=state.owner_name,
                        title=state.title,
                        timestamp=time.time(),
                        data={'old_title': self._known_windows[wid].title}
                    )
                    self._event_history.append(event)
                    self._emit(event)
            
            for wid in list(self._known_windows.keys()):
                if wid not in current_windows:
                    old_state = self._known_windows[wid]
                    event = WindowEventRecord(
                        event=WindowEvent.CLOSED,
                        window_id=wid,
                        owner_name=old_state.owner_name,
                        title=old_state.title,
                        timestamp=time.time()
                    )
                    self._event_history.append(event)
                    self._emit(event)
            
            self._known_windows = current_windows
    
    def _emit(self, record: WindowEventRecord) -> None:
        """Emit event to callbacks."""
        with self._lock:
            callbacks = self._callbacks[record.event].copy()
        
        for callback in callbacks:
            try:
                callback(record)
            except Exception:
                pass


def get_active_window() -> Optional[WindowState]:
    """
    Get currently active/focused window.
    
    Returns:
        WindowState of active window, or None.
    """
    import Quartz
    
    try:
        front_app = Quartz.NSWorkspace.sharedWorkspace().frontmostApplication()
        pid = front_app.processIdentifier()
        
        window_list = Quartz.CGWindowListCopyWindowInfo(
            Quartz.kCGWindowListOptionOnScreenOnly,
            0
        )
        
        for window in window_list:
            if window.get('kCGWindowOwnerPID') == pid:
                wid = window.get('kCGWindowNumber', 0)
                bounds = window.get('kCGWindowBounds', {})
                
                return WindowState(
                    window_id=wid,
                    owner_name=window.get('kCGWindowOwnerName', ''),
                    title=window.get('kCGWindowName', ''),
                    bounds=(
                        int(bounds.get('X', 0)),
                        int(bounds.get('Y', 0)),
                        int(bounds.get('Width', 0)),
                        int(bounds.get('Height', 0))
                    ),
                    is_minimized=False,
                    is_on_screen=True,
                    level=window.get('kCGWindowLayer', 0),
                    timestamp=time.time()
                )
    except Exception:
        pass
    return None


def wait_for_window(title: str, timeout: float = 10.0,
                     poll_interval: float = 0.5) -> Optional[WindowState]:
    """
    Wait for window with specific title to appear.
    
    Args:
        title: Window title to match.
        timeout: Max wait time.
        poll_interval: Check interval.
        
    Returns:
        WindowState when found, None on timeout.
    """
    import Quartz
    
    start = time.time()
    
    while time.time() - start < timeout:
        window_list = Quartz.CGWindowListCopyWindowInfo(
            Quartz.kCGWindowListOptionOnScreenOnly,
            0
        )
        
        for window in window_list:
            win_title = window.get('kCGWindowName', '')
            if title.lower() in win_title.lower():
                bounds = window.get('kCGWindowBounds', {})
                return WindowState(
                    window_id=window.get('kCGWindowNumber', 0),
                    owner_name=window.get('kCGWindowOwnerName', ''),
                    title=win_title,
                    bounds=(
                        int(bounds.get('X', 0)),
                        int(bounds.get('Y', 0)),
                        int(bounds.get('Width', 0)),
                        int(bounds.get('Height', 0))
                    ),
                    is_minimized=False,
                    is_on_screen=True,
                    level=window.get('kCGWindowLayer', 0),
                    timestamp=time.time()
                )
        
        time.sleep(poll_interval)
    
    return None

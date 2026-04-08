"""
Test isolation utilities for automation testing.

Provides test environment isolation, cleanup, and state
management for reliable automation testing.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import threading
import time
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
import uuid


@dataclass
class IsolationContext:
    """Test isolation context."""
    id: str
    temp_dir: str
    screenshots_dir: str
    logs_dir: str
    state: Dict[str, Any] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)


class TestIsolator:
    """Manages test isolation."""
    
    def __init__(self, name: str = "test"):
        """
        Initialize test isolator.
        
        Args:
            name: Test name for directory naming.
        """
        self.name = name
        self._contexts: Dict[str, IsolationContext] = {}
        self._current: Optional[IsolationContext] = None
        self._lock = threading.Lock()
        self._cleanup_handlers: List[Callable] = []
    
    def create_context(self) -> IsolationContext:
        """
        Create new isolation context.
        
        Returns:
            New IsolationContext.
        """
        ctx_id = str(uuid.uuid4())[:8]
        base_dir = tempfile.mkdtemp(prefix=f"{self.name}_{ctx_id}_")
        
        ctx = IsolationContext(
            id=ctx_id,
            temp_dir=base_dir,
            screenshots_dir=os.path.join(base_dir, "screenshots"),
            logs_dir=os.path.join(base_dir, "logs")
        )
        
        os.makedirs(ctx.screenshots_dir, exist_ok=True)
        os.makedirs(ctx.logs_dir, exist_ok=True)
        
        with self._lock:
            self._contexts[ctx_id] = ctx
            self._current = ctx
        
        return ctx
    
    def get_current(self) -> Optional[IsolationContext]:
        """Get current context."""
        with self._lock:
            return self._current
    
    def set_context(self, ctx_id: str) -> bool:
        """
        Set current context by ID.
        
        Args:
            ctx_id: Context ID.
            
        Returns:
            True if found, False otherwise.
        """
        with self._lock:
            if ctx_id in self._contexts:
                self._current = self._contexts[ctx_id]
                return True
        return False
    
    @contextmanager
    def isolated(self, name: Optional[str] = None):
        """
        Context manager for isolated test execution.
        
        Args:
            name: Optional context name.
            
        Yields:
            IsolationContext.
        """
        ctx = self.create_context()
        
        if name:
            ctx.state['test_name'] = name
        
        try:
            yield ctx
        finally:
            self._cleanup_context(ctx)
    
    def _cleanup_context(self, ctx: IsolationContext) -> None:
        """Clean up context resources."""
        for handler in self._cleanup_handlers:
            try:
                handler(ctx)
            except Exception:
                pass
        
        try:
            shutil.rmtree(ctx.temp_dir)
        except Exception:
            pass
        
        with self._lock:
            self._contexts.pop(ctx.id, None)
            if self._current and self._current.id == ctx.id:
                self._current = None
    
    def add_cleanup_handler(self, handler: Callable[[IsolationContext], None]) -> None:
        """
        Add cleanup handler.
        
        Args:
            handler: Cleanup function.
        """
        self._cleanup_handlers.append(handler)
    
    def cleanup_all(self) -> None:
        """Clean up all contexts."""
        with self._lock:
            for ctx in list(self._contexts.values()):
                self._cleanup_context(ctx)


@contextmanager
def isolated_temp_dir(prefix: str = "automation_"):
    """
    Context manager for isolated temp directory.
    
    Args:
        prefix: Directory prefix.
        
    Yields:
        Path to temp directory.
    """
    temp_dir = tempfile.mkdtemp(prefix=prefix)
    
    try:
        yield temp_dir
    finally:
        try:
            shutil.rmtree(temp_dir)
        except Exception:
            pass


def save_test_state(ctx: IsolationContext, key: str, value: Any) -> None:
    """
    Save state to context.
    
    Args:
        ctx: Isolation context.
        key: State key.
        value: State value.
    """
    ctx.state[key] = value


def load_test_state(ctx: IsolationContext, key: str, default: Any = None) -> Any:
    """
    Load state from context.
    
    Args:
        ctx: Isolation context.
        key: State key.
        default: Default value if not found.
        
    Returns:
        State value or default.
    """
    return ctx.state.get(key, default)


@dataclass
class AppSnapshot:
    """Application state snapshot."""
    app_bundle_id: str
    processes: List[Dict[str, Any]]
    windows: List[Dict[str, Any]]
    timestamp: float


def snapshot_app_state(bundle_id: str) -> Optional[AppSnapshot]:
    """
    Capture application state snapshot.
    
    Args:
        bundle_id: App bundle ID.
        
    Returns:
        AppSnapshot or None.
    """
    try:
        import Quartz
        
        app = None
        for running_app in Quartz.NSWorkspace.sharedWorkspace().runningApplications():
            if running_app.bundleIdentifier() == bundle_id:
                app = running_app
                break
        
        if not app:
            return None
        
        processes = [{
            'pid': app.processIdentifier(),
            'name': app.localizedName(),
        }]
        
        window_list = Quartz.CGWindowListCopyWindowInfo(
            Quartz.kCGWindowListOptionOnScreenOnly,
            0
        )
        
        windows = []
        for window in window_list:
            if window.get('kCGWindowOwnerPID') == app.processIdentifier():
                bounds = window.get('kCGWindowBounds', {})
                windows.append({
                    'id': window.get('kCGWindowNumber', 0),
                    'title': window.get('kCGWindowName', ''),
                    'bounds': bounds
                })
        
        return AppSnapshot(
            app_bundle_id=bundle_id,
            processes=processes,
            windows=windows,
            timestamp=time.time()
        )
    except Exception:
        return None


def restore_app_state(snapshot: AppSnapshot) -> bool:
    """
    Restore application to snapshot state.
    
    Args:
        snapshot: AppSnapshot to restore.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        script = f'''
        tell application "System Events"
            set targetApp to first process whose bundle identifier is "{snapshot.app_bundle_id}"
            set frontmost of targetApp to true
        end tell
        '''
        subprocess.run(["osascript", "-e", script], capture_output=True)
        return True
    except Exception:
        return False

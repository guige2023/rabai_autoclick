"""Debug panel management utilities."""

from typing import Dict, Any, Optional, List, Callable
from enum import Enum
import time
import threading


class LogLevel(Enum):
    """Log levels."""
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


class DebugPanel:
    """In-memory debug panel for collecting runtime information."""

    def __init__(self, max_entries: int = 500):
        """Initialize debug panel.
        
        Args:
            max_entries: Maximum log entries to keep.
        """
        self.max_entries = max_entries
        self._logs: List[Dict[str, Any]] = []
        self._metrics: Dict[str, Any] = {}
        self._breakpoints: Dict[str, bool] = {}
        self._watch_vars: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._enabled = True

    def log(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add log entry.
        
        Args:
            message: Log message.
            level: Log level.
            context: Additional context data.
        """
        if not self._enabled:
            return
        with self._lock:
            entry = {
                "timestamp": time.time(),
                "level": level.name,
                "message": message,
                "context": context or {},
            }
            self._logs.append(entry)
            if len(self._logs) > self.max_entries:
                self._logs = self._logs[-self.max_entries:]

    def debug(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log debug message."""
        self.log(message, LogLevel.DEBUG, context)

    def info(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log info message."""
        self.log(message, LogLevel.INFO, context)

    def warning(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log warning message."""
        self.log(message, LogLevel.WARNING, context)

    def error(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log error message."""
        self.log(message, LogLevel.ERROR, context)

    def metric(self, name: str, value: Any) -> None:
        """Set a metric value."""
        with self._lock:
            self._metrics[name] = {
                "value": value,
                "timestamp": time.time(),
            }

    def watch(self, name: str, value: Any) -> None:
        """Watch a variable value."""
        with self._lock:
            self._watch_vars[name] = {
                "value": value,
                "timestamp": time.time(),
            }

    def set_breakpoint(self, name: str, enabled: bool = True) -> None:
        """Set or clear a breakpoint."""
        self._breakpoints[name] = enabled

    def check_breakpoint(self, name: str) -> bool:
        """Check if breakpoint is enabled."""
        return self._breakpoints.get(name, False)

    def get_recent_logs(self, count: int = 50, level: Optional[LogLevel] = None) -> List[Dict[str, Any]]:
        """Get recent log entries.
        
        Args:
            count: Number of entries to return.
            level: Filter by log level.
        
        Returns:
            List of log entries.
        """
        with self._lock:
            logs = self._logs
            if level is not None:
                logs = [l for l in logs if l["level"] == level.name]
            return logs[-count:]

    def get_metrics(self) -> Dict[str, Any]:
        """Get all current metrics."""
        with self._lock:
            return dict(self._metrics)

    def get_watch_vars(self) -> Dict[str, Any]:
        """Get all watched variables."""
        with self._lock:
            return dict(self._watch_vars)

    def clear(self) -> None:
        """Clear all debug data."""
        with self._lock:
            self._logs.clear()
            self._metrics.clear()
            self._watch_vars.clear()

    def enable(self) -> None:
        """Enable debug panel."""
        self._enabled = True

    def disable(self) -> None:
        """Disable debug panel."""
        self._enabled = False

    def get_summary(self) -> Dict[str, Any]:
        """Get debug panel summary."""
        with self._lock:
            error_count = sum(1 for l in self._logs if l["level"] == "ERROR")
            warning_count = sum(1 for l in self._logs if l["level"] == "WARNING")
            return {
                "enabled": self._enabled,
                "total_logs": len(self._logs),
                "error_count": error_count,
                "warning_count": warning_count,
                "metric_count": len(self._metrics),
                "watch_count": len(self._watch_vars),
                "breakpoint_count": len(self._breakpoints),
            }


_global_panel: Optional[DebugPanel] = None


def get_debug_panel() -> DebugPanel:
    """Get the global debug panel instance."""
    global _global_panel
    if _global_panel is None:
        _global_panel = DebugPanel()
    return _global_panel

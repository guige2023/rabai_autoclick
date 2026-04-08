"""Automation Debug Action.

Provides debugging capabilities for automation workflows.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
import time
import traceback


@dataclass
class DebugEntry:
    timestamp: float
    level: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None


class AutomationDebugAction:
    """Debug helper for automation executions."""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self.entries: List[DebugEntry] = []
        self.breakpoints: Dict[str, bool] = {}

    def log(self, level: str, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        if not self.enabled:
            return
        self.entries.append(DebugEntry(
            timestamp=time.time(),
            level=level.upper(),
            message=message,
            context=context or {},
        ))

    def debug(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("DEBUG", message, context)

    def info(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("INFO", message, context)

    def warning(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("WARNING", message, context)

    def error(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        self.log("ERROR", message, context)
        if self.entries:
            self.entries[-1].stack_trace = traceback.format_exc()

    def set_breakpoint(self, step_name: str, enabled: bool = True) -> None:
        self.breakpoints[step_name] = enabled

    def hit_breakpoint(self, step_name: str) -> bool:
        return self.breakpoints.get(step_name, False)

    def get_entries(
        self,
        level: Optional[str] = None,
        since: Optional[float] = None,
    ) -> List[DebugEntry]:
        result = self.entries
        if level:
            result = [e for e in result if e.level == level.upper()]
        if since:
            result = [e for e in result if e.timestamp >= since]
        return result

    def clear(self) -> None:
        self.entries.clear()

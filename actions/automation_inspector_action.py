"""Automation Inspector Action.

Inspects automation state, execution flow, and performance.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import inspect


@dataclass
class InspectionReport:
    timestamp: float
    automation_name: str
    state: Dict[str, Any]
    execution_flow: List[str]
    performance: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


class AutomationInspectorAction:
    """Inspects automation state and execution."""

    def __init__(self) -> None:
        self.history: List[InspectionReport] = []

    def inspect(
        self,
        automation_name: str,
        state: Dict[str, Any],
        execution_flow: List[str],
        performance: Dict[str, Any],
        **metadata,
    ) -> InspectionReport:
        report = InspectionReport(
            timestamp=time.time(),
            automation_name=automation_name,
            state=dict(state),
            execution_flow=list(execution_flow),
            performance=dict(performance),
            metadata=metadata,
        )
        self.history.append(report)
        return report

    def inspect_function(
        self,
        fn: Callable,
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        start = time.time()
        sig = inspect.signature(fn)
        args = args or ()
        kwargs = kwargs or {}
        try:
            result = fn(*args, **kwargs)
            duration_ms = (time.time() - start) * 1000
            return {
                "function": fn.__name__,
                "signature": str(sig),
                "result": result,
                "duration_ms": duration_ms,
                "success": True,
            }
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            return {
                "function": fn.__name__,
                "signature": str(sig),
                "error": str(e),
                "duration_ms": duration_ms,
                "success": False,
            }

    def get_reports(self, automation_name: Optional[str] = None, limit: int = 50) -> List[InspectionReport]:
        result = self.history
        if automation_name:
            result = [r for r in result if r.automation_name == automation_name]
        return result[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_reports": len(self.history),
            "automations": list(set(r.automation_name for r in self.history)),
        }

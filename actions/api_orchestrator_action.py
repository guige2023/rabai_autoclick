"""API Orchestrator Action.

Orchestrates multiple API calls with dependency resolution.
"""
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import time


class CallStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class API call:
    call_id: str
    name: str
    fn: Callable
    dependencies: Set[str] = field(default_factory=set)
    status: CallStatus = CallStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


class APIOrchestratorAction:
    """Orchestrates multiple API calls with dependency resolution."""

    def __init__(self, max_concurrent: int = 5) -> None:
        self.max_concurrent = max_concurrent
        self.calls: Dict[str, "APICall"] = {}
        self.results: Dict[str, Any] = {}

    def add_call(
        self,
        call_id: str,
        name: str,
        fn: Callable,
        dependencies: Optional[Set[str]] = None,
    ) -> None:
        self.calls[call_id] = API call(
            call_id=call_id,
            name=name,
            fn=fn,
            dependencies=dependencies or set(),
        )

    def _get_ready_calls(self) -> List["APICall"]:
        ready = []
        for call in self.calls.values():
            if call.status != CallStatus.PENDING:
                continue
            deps_satisfied = all(
                self.calls[dep].status == CallStatus.COMPLETED
                for dep in call.dependencies
                if dep in self.calls
            )
            if deps_satisfied:
                ready.append(call)
        return ready

    def execute_all(self) -> Dict[str, Any]:
        max_iterations = len(self.calls) * 2
        for _ in range(max_iterations):
            ready = self._get_ready_calls()
            if not ready:
                break
            for call in ready[: self.max_concurrent]:
                call.status = CallStatus.RUNNING
                call.started_at = time.time()
                try:
                    deps_results = {dep: self.results[dep] for dep in call.dependencies if dep in self.results}
                    call.result = call.fn(deps_results)
                    call.status = CallStatus.COMPLETED
                    self.results[call.call_id] = call.result
                except Exception as e:
                    call.status = CallStatus.FAILED
                    call.error = str(e)
                call.completed_at = time.time()
        return self.results

    def get_status(self) -> Dict[str, Any]:
        counts = {}
        for call in self.calls.values():
            counts[call.status.value] = counts.get(call.status.value, 0) + 1
        return {
            "total": len(self.calls),
            "by_status": counts,
            "completed": sum(1 for c in self.calls.values() if c.status == CallStatus.COMPLETED),
            "failed": sum(1 for c in self.calls.values() if c.status == CallStatus.FAILED),
        }

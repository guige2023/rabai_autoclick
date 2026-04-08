"""API Fanout Action.

Fans out API requests to multiple targets and aggregates responses.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time


class FanoutMode(Enum):
    BROADCAST = "broadcast"
    FIRST_RESPONSE = "first_response"
    ALL_MUST_SUCCEED = "all_must_succeed"


@dataclass
class FanoutTarget:
    name: str
    fn: Callable
    timeout_sec: float = 30.0
    enabled: bool = True


@dataclass
class FanoutResult:
    target_name: str
    success: bool
    response: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0


class APIFanoutAction:
    """Fans out API requests to multiple targets."""

    def __init__(self, mode: FanoutMode = FanoutMode.BROADCAST) -> None:
        self.mode = mode
        self.targets: Dict[str, FanoutTarget] = {}

    def add_target(self, target: FanoutTarget) -> None:
        self.targets[target.name] = target

    def execute(self, payload: Any) -> List[FanoutResult]:
        results = []
        for name, target in self.targets.items():
            if not target.enabled:
                continue
            start = time.time()
            try:
                result = target.fn(payload)
                results.append(FanoutResult(
                    target_name=name,
                    success=True,
                    response=result,
                    duration_ms=(time.time() - start) * 1000,
                ))
            except Exception as e:
                results.append(FanoutResult(
                    target_name=name,
                    success=False,
                    error=str(e),
                    duration_ms=(time.time() - start) * 1000,
                ))
                if self.mode == FanoutMode.ALL_MUST_SUCCEED:
                    break
        return results

    def get_stats(self) -> Dict[str, Any]:
        return {
            "targets": len(self.targets),
            "enabled": sum(1 for t in self.targets.values() if t.enabled),
            "mode": self.mode.value,
        }

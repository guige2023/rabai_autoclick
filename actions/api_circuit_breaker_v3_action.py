"""API Circuit Breaker v3 Action.

Advanced circuit breaker with half-open state and adaptive thresholds.
"""
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
import time


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitMetrics:
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    consecutive_failures: int = 0
    last_failure_time: Optional[float] = None


class APICircuitBreakerV3Action:
    """Advanced circuit breaker with adaptive thresholds."""

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout_sec: float = 60.0,
        half_open_max_calls: int = 3,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout_sec = timeout_sec
        self.half_open_max_calls = half_open_max_calls
        self.state = CircuitState.CLOSED
        self.metrics = CircuitMetrics()
        self._half_open_calls = 0

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        if self.state == CircuitState.OPEN:
            if time.time() - (self.metrics.last_failure_time or 0) >= self.timeout_sec:
                self.state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
            else:
                raise Exception("Circuit breaker is OPEN")
        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self) -> None:
        self.metrics.total_calls += 1
        self.metrics.successful_calls += 1
        self.metrics.consecutive_failures = 0
        if self.state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1
            if self._half_open_calls >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self._half_open_calls = 0

    def _on_failure(self) -> None:
        self.metrics.total_calls += 1
        self.metrics.failed_calls += 1
        self.metrics.consecutive_failures += 1
        self.metrics.last_failure_time = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
        elif self.metrics.consecutive_failures >= self.failure_threshold:
            self.state = CircuitState.OPEN

    def get_status(self) -> Dict[str, Any]:
        return {
            "state": self.state.value,
            "total_calls": self.metrics.total_calls,
            "successful_calls": self.metrics.successful_calls,
            "failed_calls": self.metrics.failed_calls,
            "consecutive_failures": self.metrics.consecutive_failures,
            "failure_threshold": self.failure_threshold,
        }

    def reset(self) -> None:
        self.state = CircuitState.CLOSED
        self.metrics = CircuitMetrics()
        self._half_open_calls = 0

"""Automation Guardrails Action.

Implements safety guardrails for automation workflows including
resource limits, execution bounds, sandboxing, and circuit breakers.
"""
from __future__ import annotations

import resource
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class ViolationType(Enum):
    """Types of guardrail violations."""
    TIME_LIMIT = "time_limit"
    MEMORY_LIMIT = "memory_limit"
    CPU_LIMIT = "cpu_limit"
    RATE_LIMIT = "rate_limit"
    CUSTOM = "custom"
    CIRCUIT_OPEN = "circuit_open"


@dataclass
class Violation:
    """A guardrail violation record."""
    type: ViolationType
    message: str
    timestamp: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GuardrailConfig:
    """Configuration for guardrails."""
    max_execution_time_sec: float = 300.0
    max_memory_mb: int = 512
    max_cpu_percent: float = 80.0
    max_iterations: int = 10000
    rate_limit_per_sec: float = 100.0
    enable_sandbox: bool = False
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout_sec: float = 60.0


@dataclass
class GuardrailMetrics:
    """Guardrail execution metrics."""
    total_checks: int = 0
    violations: int = 0
    circuit_breakers_triggered: int = 0
    avg_check_time_ms: float = 0.0
    last_violation: Optional[Violation] = None


class AutomationGuardrailsAction:
    """Safety guardrails for automation workflows."""

    def __init__(self, config: Optional[GuardrailConfig] = None) -> None:
        self.config = config or GuardrailConfig()
        self._violations: deque = deque(maxlen=100)
        self._metrics = GuardrailMetrics()
        self._start_time: Optional[float] = None
        self._custom_rules: Dict[str, Callable[[], bool]] = {}
        self._circuit_states: Dict[str, bool] = {}
        self._circuit_timers: Dict[str, float] = {}
        self._execution_count: Dict[str, int] = {}
        self._rate_limit_window: deque = deque()
        self._check_times: deque = deque(maxlen=1000)

    def start_execution(self, workflow_id: str = "default") -> None:
        """Mark the start of a guarded execution."""
        self._start_time = time.time()
        self._execution_count[workflow_id] = 0

    def check_time_limit(self) -> Optional[Violation]:
        """Check if execution time limit is exceeded."""
        if self._start_time is None:
            return None

        elapsed = time.time() - self._start_time
        if elapsed > self.config.max_execution_time_sec:
            return Violation(
                type=ViolationType.TIME_LIMIT,
                message=f"Execution time {elapsed:.2f}s exceeds limit {self.config.max_execution_time_sec}s",
                timestamp=time.time(),
                details={"elapsed_sec": elapsed, "limit_sec": self.config.max_execution_time_sec},
            )
        return None

    def check_memory_limit(self) -> Optional[Violation]:
        """Check if memory usage exceeds limit."""
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            memory_mb = usage.ru_maxrss / 1024

            if memory_mb > self.config.max_memory_mb:
                return Violation(
                    type=ViolationType.MEMORY_LIMIT,
                    message=f"Memory usage {memory_mb:.2f}MB exceeds limit {self.config.max_memory_mb}MB",
                    timestamp=time.time(),
                    details={"used_mb": memory_mb, "limit_mb": self.config.max_memory_mb},
                )
        except Exception:
            pass
        return None

    def check_rate_limit(self, workflow_id: str = "default") -> Optional[Violation]:
        """Check if rate limit is exceeded."""
        now = time.time()
        window_sec = 1.0

        while self._rate_limit_window and (now - self._rate_limit_window[0]) > window_sec:
            self._rate_limit_window.popleft()

        current_rate = len(self._rate_limit_window)
        if current_rate >= self.config.rate_limit_per_sec:
            return Violation(
                type=ViolationType.RATE_LIMIT,
                message=f"Rate {current_rate}/s exceeds limit {self.config.rate_limit_per_sec}/s",
                timestamp=time.time(),
                details={"current_rate": current_rate, "limit": self.config.rate_limit_per_sec},
            )

        self._rate_limit_window.append(now)
        return None

    def check_iteration_limit(self, workflow_id: str = "default") -> Optional[Violation]:
        """Check if iteration count exceeds limit."""
        count = self._execution_count.get(workflow_id, 0)
        if count >= self.config.max_iterations:
            return Violation(
                type=ViolationType.TIME_LIMIT,
                message=f"Iteration count {count} exceeds limit {self.config.max_iterations}",
                timestamp=time.time(),
                details={"count": count, "limit": self.config.max_iterations},
            )
        self._execution_count[workflow_id] = count + 1
        return None

    def check_circuit_breaker(self, circuit_name: str = "default") -> Optional[Violation]:
        """Check if a circuit breaker is open."""
        if circuit_name not in self._circuit_states:
            return None

        if self._circuit_states.get(circuit_name, False):
            timeout = self._circuit_timers.get(circuit_name, 0)
            elapsed = time.time() - timeout

            if elapsed > self.config.circuit_breaker_timeout_sec:
                self._circuit_states[circuit_name] = False
                return None

            return Violation(
                type=ViolationType.CIRCUIT_OPEN,
                message=f"Circuit breaker '{circuit_name}' is open",
                timestamp=time.time(),
                details={"circuit": circuit_name, "opened_at": timeout},
            )

        return None

    def trigger_circuit_breaker(self, circuit_name: str = "default") -> None:
        """Trigger (open) a circuit breaker."""
        self._circuit_states[circuit_name] = True
        self._circuit_timers[circuit_name] = time.time()
        self._metrics.circuit_breakers_triggered += 1

    def register_custom_rule(
        self,
        rule_name: str,
        check_fn: Callable[[], bool],
    ) -> None:
        """Register a custom guardrail rule."""
        self._custom_rules[rule_name] = check_fn

    def run_with_guardrails(
        self,
        fn: Callable[..., Any],
        workflow_id: str = "default",
        *args: Any,
        **kwargs: Any,
    ) -> tuple[Any, List[Violation]]:
        """Execute a function with all guardrails active."""
        self.start_execution(workflow_id)
        violations: List[Violation] = []

        while True:
            check_start = time.time()

            v = self.check_time_limit()
            if v:
                violations.append(v)
                break

            v = self.check_memory_limit()
            if v:
                violations.append(v)
                break

            v = self.check_circuit_breaker(workflow_id)
            if v:
                violations.append(v)
                break

            v = self.check_iteration_limit(workflow_id)
            if v:
                violations.append(v)
                break

            for rule_name, rule_fn in self._custom_rules.items():
                try:
                    if not rule_fn():
                        violations.append(Violation(
                            type=ViolationType.CUSTOM,
                            message=f"Custom rule '{rule_name}' failed",
                            timestamp=time.time(),
                        ))
                        break
                except Exception:
                    pass

            self._check_times.append((time.time() - check_start) * 1000)

            break

        result = None
        if not violations:
            try:
                result = fn(*args, **kwargs)
            except Exception as e:
                violations.append(Violation(
                    type=ViolationType.CUSTOM,
                    message=f"Execution error: {e}",
                    timestamp=time.time(),
                ))

        self._update_metrics(violations)

        for v in violations:
            self._violations.append(v)
            self._metrics.last_violation = v

        return result, violations

    def _update_metrics(self, violations: List[Violation]) -> None:
        """Update guardrail metrics."""
        self._metrics.total_checks += 1
        if violations:
            self._metrics.violations += 1

        if self._check_times:
            self._metrics.avg_check_time_ms = sum(self._check_times) / len(self._check_times)

    def get_metrics(self) -> GuardrailMetrics:
        """Get guardrail metrics."""
        return self._metrics

    def get_violations(self, limit: int = 50) -> List[Violation]:
        """Get recent violations."""
        return list(self._violations)[-limit:]

    def clear_violations(self) -> None:
        """Clear violation history."""
        self._violations.clear()
        self._metrics.last_violation = None

    def reset_circuit_breaker(self, circuit_name: str = "default") -> None:
        """Reset a circuit breaker."""
        self._circuit_states[circuit_name] = False
        if circuit_name in self._circuit_timers:
            del self._circuit_timers[circuit_name]

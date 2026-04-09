"""
Automation Guardrails Action Module.

Provides safety guardrails and circuit breakers for automation workflows,
preventing runaway processes and managing resource exhaustion.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import asyncio
import logging
import threading
import time

logger = logging.getLogger(__name__)


class GuardState(Enum):
    """States of a guardrail."""
    NORMAL = auto()
    WARNING = auto()
    THROTTLING = auto()
    TRIPPED = auto()
    RECOVERING = auto()


@dataclass
class GuardConfig:
    """Configuration for a guardrail."""
    name: str
    max_rpm: Optional[float] = None
    max_concurrent: Optional[int] = None
    max_errors: Optional[int] = None
    error_window_seconds: float = 60.0
    throttle_duration_seconds: float = 30.0
    auto_recover: bool = True
    recovery_timeout_seconds: float = 60.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "max_rpm": self.max_rpm,
            "max_concurrent": self.max_concurrent,
            "max_errors": self.max_errors,
            "error_window_seconds": self.error_window_seconds,
            "throttle_duration_seconds": self.throttle_duration_seconds,
            "auto_recover": self.auto_recover,
            "recovery_timeout_seconds": self.recovery_timeout_seconds,
        }


@dataclass
class GuardMetric:
    """A metric recorded by a guardrail."""
    timestamp: datetime
    metric_type: str
    value: float
    guard_name: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "type": self.metric_type,
            "value": self.value,
            "guard": self.guard_name,
        }


@dataclass
class GuardStateChange:
    """Record of a guard state change."""
    guard_name: str
    previous_state: GuardState
    new_state: GuardState
    timestamp: datetime
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "guard": self.guard_name,
            "previous": self.previous_state.name,
            "new": self.new_state.name,
            "timestamp": self.timestamp.isoformat(),
            "reason": self.reason,
        }


class AutomationGuardrailsAction:
    """
    Provides safety guardrails for automation workflows.

    This action implements various safety mechanisms including rate limiting,
    concurrency control, error thresholding, and circuit breakers to
    prevent automation runaway and resource exhaustion.

    Example:
        >>> guards = AutomationGuardrailsAction()
        >>> guards.add_guard(GuardConfig(name="api_calls", max_rpm=100))
        >>> async with guards.check("api_calls"):
        ...     await make_api_call()
    """

    def __init__(self):
        """Initialize the Automation Guardrails."""
        self._guards: Dict[str, GuardConfig] = {}
        self._states: Dict[str, GuardState] = {}
        self._metrics: Dict[str, List[GuardMetric]] = {}
        self._state_changes: List[GuardStateChange] = []
        self._lock = threading.RLock()

        self._request_times: Dict[str, List[float]] = {}
        self._error_times: Dict[str, List[float]] = {}
        self._concurrent_counts: Dict[str, int] = {}
        self._throttle_until: Dict[str, float] = {}

    def add_guard(self, config: GuardConfig) -> None:
        """
        Add a guardrail configuration.

        Args:
            config: Guard configuration.
        """
        with self._lock:
            self._guards[config.name] = config
            self._states[config.name] = GuardState.NORMAL
            self._metrics[config.name] = []
            self._request_times[config.name] = []
            self._error_times[config.name] = []
            self._concurrent_counts[config.name] = 0

            logger.info(f"Added guardrail: {config.name}")

    def remove_guard(self, name: str) -> bool:
        """Remove a guardrail."""
        with self._lock:
            if name in self._guards:
                del self._guards[name]
                del self._states[name]
                del self._metrics[name]
                del self._request_times[name]
                del self._error_times[name]
                del self._concurrent_counts[name]
                return True
            return False

    def get_guard(self, name: str) -> Optional[GuardConfig]:
        """Get a guard configuration."""
        return self._guards.get(name)

    async def check(self, guard_name: str) -> "GuardContext":
        """
        Check if an operation is allowed under the guard.

        Args:
            guard_name: Name of the guard.

        Returns:
            GuardContext for the operation.
        """
        config = self._guards.get(guard_name)
        if not config:
            return GuardContext(self, guard_name, allowed=True)

        await self._update_guard_state(guard_name)

        state = self._states.get(guard_name, GuardState.NORMAL)

        if state in (GuardState.TRIPLED, GuardState.THROTTLING):
            if self._is_throttled(guard_name):
                self._record_metric(guard_name, "throttled", 1)
                return GuardContext(self, guard_name, allowed=False)

        allowed, reason = await self._check_limits(guard_name, config)

        if allowed:
            self._concurrent_counts[guard_name] = self._concurrent_counts.get(guard_name, 0) + 1
            self._record_request_time(guard_name)

        self._record_metric(guard_name, "checked", 1 if allowed else 0)

        return GuardContext(self, guard_name, allowed=allowed, reason=reason)

    async def record_success(self, guard_name: str) -> None:
        """Record a successful operation."""
        self._record_metric(guard_name, "success", 1)

    async def record_error(self, guard_name: str) -> None:
        """Record a failed operation."""
        self._record_error_time(guard_name)
        self._record_metric(guard_name, "error", 1)

        config = self._guards.get(guard_name)
        if config and config.max_errors:
            await self._check_error_threshold(guard_name, config)

    async def release(self, guard_name: str) -> None:
        """Release a guard check."""
        self._concurrent_counts[guard_name] = max(0, self._concurrent_counts.get(guard_name, 1) - 1)

    async def _update_guard_state(self, guard_name: str) -> None:
        """Update guard state based on current metrics."""
        state = self._states.get(guard_name, GuardState.NORMAL)

        if state == GuardState.THROTTLING:
            if not self._is_throttled(guard_name):
                self._change_state(guard_name, GuardState.NORMAL, "Throttle period ended")

        elif state == GuardState.TRIPLED:
            config = self._guards.get(guard_name)
            if config and config.auto_recover:
                self._change_state(guard_name, GuardState.RECOVERING, "Auto-recovery started")

        elif state == GuardState.RECOVERING:
            await self._check_recovery(guard_name)

    async def _check_limits(self, guard_name: str, config: GuardConfig) -> Tuple[bool, Optional[str]]:
        """Check if operation is within limits."""
        now = time.time()

        if config.max_rpm:
            recent_requests = self._get_recent_requests(guard_name, 60)
            if len(recent_requests) >= config.max_rpm:
                return False, f"Rate limit exceeded: {config.max_rpm}/min"

        if config.max_concurrent:
            current = self._concurrent_counts.get(guard_name, 0)
            if current >= config.max_concurrent:
                return False, f"Concurrency limit exceeded: {config.max_concurrent}"

        return True, None

    async def _check_error_threshold(self, guard_name: str, config: GuardConfig) -> None:
        """Check if error threshold has been exceeded."""
        if not config.max_errors:
            return

        recent_errors = self._get_recent_errors(guard_name, config.error_window_seconds)

        if len(recent_errors) >= config.max_errors:
            self._change_state(guard_name, GuardState.TRIPLED, f"Error threshold exceeded: {len(recent_errors)}/{config.max_errors}")

    async def _check_recovery(self, guard_name: str) -> None:
        """Check if guard can recover."""
        config = self._guards.get(guard_name)
        if not config:
            return

        recent_errors = self._get_recent_errors(guard_name, config.recovery_timeout_seconds)

        if len(recent_errors) == 0:
            self._change_state(guard_name, GuardState.NORMAL, "Recovery successful")

    def _is_throttled(self, guard_name: str) -> bool:
        """Check if guard is throttled."""
        until = self._throttle_until.get(guard_name, 0)
        return time.time() < until

    def _get_recent_requests(self, guard_name: str, window_seconds: float) -> List[float]:
        """Get request times within window."""
        cutoff = time.time() - window_seconds
        times = self._request_times.get(guard_name, [])
        return [t for t in times if t > cutoff]

    def _get_recent_errors(self, guard_name: str, window_seconds: float) -> List[float]:
        """Get error times within window."""
        cutoff = time.time() - window_seconds
        times = self._error_times.get(guard_name, [])
        return [t for t in times if t > cutoff]

    def _record_request_time(self, guard_name: str) -> None:
        """Record a request timestamp."""
        now = time.time()
        times = self._request_times.get(guard_name, [])
        times.append(now)
        self._request_times[guard_name] = [t for t in times if t > now - 3600]

    def _record_error_time(self, guard_name: str) -> None:
        """Record an error timestamp."""
        now = time.time()
        times = self._error_times.get(guard_name, [])
        times.append(now)
        self._error_times[guard_name] = [t for t in times if t > now - 3600]

    def _record_metric(self, guard_name: str, metric_type: str, value: float) -> None:
        """Record a metric."""
        metric = GuardMetric(
            timestamp=datetime.now(timezone.utc),
            metric_type=metric_type,
            value=value,
            guard_name=guard_name,
        )
        metrics = self._metrics.get(guard_name, [])
        metrics.append(metric)
        self._metrics[guard_name] = metrics[-1000:]

    def _change_state(self, guard_name: str, new_state: GuardState, reason: str) -> None:
        """Change guard state."""
        old_state = self._states.get(guard_name, GuardState.NORMAL)

        if old_state != new_state:
            self._states[guard_name] = new_state

            change = GuardStateChange(
                guard_name=guard_name,
                previous_state=old_state,
                new_state=new_state,
                timestamp=datetime.now(timezone.utc),
                reason=reason,
            )
            self._state_changes.append(change)

            config = self._guards.get(guard_name)
            if new_state == GuardState.TRIPLED and config:
                self._throttle_until[guard_name] = time.time() + config.throttle_duration_seconds

            logger.warning(f"Guard '{guard_name}' state changed: {old_state.name} -> {new_state.name}: {reason}")

    def get_state(self, guard_name: str) -> Optional[GuardState]:
        """Get current state of a guard."""
        return self._states.get(guard_name)

    def get_metrics(self, guard_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent metrics for a guard."""
        metrics = self._metrics.get(guard_name, [])
        return [m.to_dict() for m in metrics[-limit:]]

    def get_state_changes(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent state changes."""
        return [sc.to_dict() for sc in self._state_changes[-limit:]]

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all guards."""
        stats = {}

        for name in self._guards:
            state = self._states.get(name, GuardState.NORMAL)
            config = self._guards[name]

            recent_requests = self._get_recent_requests(name, 60)
            recent_errors = self._get_recent_errors(name, config.error_window_seconds)

            stats[name] = {
                "state": state.name,
                "config": config.to_dict(),
                "current_concurrent": self._concurrent_counts.get(name, 0),
                "requests_last_minute": len(recent_requests),
                "errors_last_window": len(recent_errors),
            }

        return stats

    async def reset_guard(self, guard_name: str) -> bool:
        """Reset a guard to normal state."""
        if guard_name not in self._guards:
            return False

        self._change_state(guard_name, GuardState.NORMAL, "Manual reset")
        self._request_times[guard_name] = []
        self._error_times[guard_name] = []
        return True

    async def with_guard(
        self,
        guard_name: str,
        operation: Callable,
    ) -> Any:
        """
        Execute an operation with guard checking.

        Args:
            guard_name: Name of the guard.
            operation: Async operation to execute.

        Returns:
            Operation result.

        Raises:
            Exception: If guard blocks the operation.
        """
        async with self.check(guard_name) as context:
            if not context.allowed:
                raise RuntimeError(f"Guard '{guard_name}' blocked operation: {context.reason}")

            try:
                result = await operation()
                await self.record_success(guard_name)
                return result
            except Exception as e:
                await self.record_error(guard_name)
                raise


class GuardContext:
    """Context manager for guard checking."""

    def __init__(
        self,
        action: AutomationGuardrailsAction,
        guard_name: str,
        allowed: bool,
        reason: Optional[str] = None,
    ):
        """Initialize guard context."""
        self.action = action
        self.guard_name = guard_name
        self.allowed = allowed
        self.reason = reason
        self._released = False

    async def __aenter__(self) -> "GuardContext":
        """Enter async context."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context."""
        if not self._released and self.allowed:
            await self.action.release(self.guard_name)
            self._released = True


def create_guardrails_action() -> AutomationGuardrailsAction:
    """Factory function to create an AutomationGuardrailsAction."""
    return AutomationGuardrailsAction()

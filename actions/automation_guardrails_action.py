"""
Automation Guardrails Action Module.

Safety guardrails and circuit breakers for automation
workflows with threshold monitoring and auto-recovery.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class GuardrailMetric:
    """Guardrail metric tracking."""
    name: str
    current_value: float
    threshold: float
    operator: str
    triggered: bool = False


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics."""
    state: CircuitState
    failure_count: int
    success_count: int
    last_failure_time: Optional[float]
    total_calls: int


class AutomationGuardrailsAction:
    """
    Safety guardrails for automation workflows.

    Example:
        guardrails = AutomationGuardrailsAction()
        guardrails.add_metric("error_rate", current_val, threshold=0.1, operator="gt")
        guardrails.execute_with_guardrails(some_function, *args)
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        """
        Initialize guardrails action.

        Args:
            config: Circuit breaker configuration.
        """
        self.config = config or CircuitBreakerConfig()
        self._circuit_state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._total_calls = 0
        self._half_open_calls = 0
        self._metrics: dict[str, GuardrailMetric] = {}
        self._guard_handlers: dict[str, Callable] = {}

    def add_metric(
        self,
        name: str,
        current_value: float,
        threshold: float,
        operator: str = "gt"
    ) -> GuardrailMetric:
        """
        Add metric for monitoring.

        Args:
            name: Metric name.
            current_value: Current metric value.
            threshold: Threshold value.
            operator: Comparison operator (gt, lt, gte, lte, eq).

        Returns:
            Created GuardrailMetric.
        """
        triggered = self._check_threshold(current_value, threshold, operator)

        metric = GuardrailMetric(
            name=name,
            current_value=current_value,
            threshold=threshold,
            operator=operator,
            triggered=triggered
        )

        self._metrics[name] = metric

        if triggered:
            self._trigger_guard(name)

        return metric

    def register_guard_handler(
        self,
        metric_name: str,
        handler: Callable[["GuardrailMetric"], None]
    ) -> None:
        """
        Register handler for when guardrail triggers.

        Args:
            metric_name: Metric name to watch.
            handler: Function to call when triggered.
        """
        self._guard_handlers[metric_name] = handler

    def _check_threshold(
        self,
        value: float,
        threshold: float,
        operator: str
    ) -> bool:
        """Check if threshold is breached."""
        if operator == "gt":
            return value > threshold
        elif operator == "lt":
            return value < threshold
        elif operator == "gte":
            return value >= threshold
        elif operator == "lte":
            return value <= threshold
        elif operator == "eq":
            return value == threshold
        return False

    def _trigger_guard(self, metric_name: str) -> None:
        """Trigger guardrail handler."""
        logger.warning(f"Guardrail triggered: {metric_name}")

        handler = self._guard_handlers.get(metric_name)
        if handler:
            metric = self._metrics[metric_name]
            try:
                handler(metric)
            except Exception as e:
                logger.error(f"Guard handler failed: {e}")

    def check_circuit_breaker(self) -> bool:
        """
        Check circuit breaker state.

        Returns:
            True if requests should be allowed.
        """
        if self._circuit_state == CircuitState.CLOSED:
            return True

        if self._circuit_state == CircuitState.OPEN:
            if self._last_failure_time:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.config.timeout:
                    self._circuit_state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info("Circuit breaker entering half-open state")
                    return True

            return False

        if self._circuit_state == CircuitState.HALF_OPEN:
            if self._half_open_calls < self.config.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

        return True

    def record_success(self) -> None:
        """Record successful call."""
        self._total_calls += 1
        self._failure_count = 0

        if self._circuit_state == CircuitState.HALF_OPEN:
            self._success_count += 1

            if self._success_count >= self.config.success_threshold:
                self._circuit_state = CircuitState.CLOSED
                self._success_count = 0
                logger.info("Circuit breaker closed")

    def record_failure(self) -> None:
        """Record failed call."""
        self._total_calls += 1
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._circuit_state == CircuitState.HALF_OPEN:
            self._circuit_state = CircuitState.OPEN
            logger.warning("Circuit breaker opened from half-open")

        elif self._circuit_state == CircuitState.CLOSED:
            if self._failure_count >= self.config.failure_threshold:
                self._circuit_state = CircuitState.OPEN
                logger.warning(f"Circuit breaker opened after {self._failure_count} failures")

    async def execute_with_guardrails(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """
        Execute function with guardrails protection.

        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Function result.

        Raises:
            Exception: If guardrails block execution or function fails.
        """
        if not self.check_circuit_breaker():
            raise Exception("Circuit breaker is open - execution blocked")

        for metric in self._metrics.values():
            if metric.triggered:
                raise Exception(f"Guardrail triggered: {metric.name} ({metric.operator} {metric.threshold})")

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = await asyncio.to_thread(func, *args, **kwargs)

            self.record_success()
            return result

        except Exception as e:
            self.record_failure()
            raise

    def get_circuit_state(self) -> CircuitState:
        """Get current circuit breaker state."""
        return self._circuit_state

    def get_circuit_stats(self) -> CircuitBreakerStats:
        """Get circuit breaker statistics."""
        return CircuitBreakerStats(
            state=self._circuit_state,
            failure_count=self._failure_count,
            success_count=self._success_count,
            last_failure_time=self._last_failure_time,
            total_calls=self._total_calls
        )

    def reset_circuit(self) -> None:
        """Reset circuit breaker to closed state."""
        self._circuit_state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        logger.info("Circuit breaker reset")

    def update_metric(
        self,
        name: str,
        value: float
    ) -> None:
        """
        Update metric value.

        Args:
            name: Metric name.
            value: New value.
        """
        if name not in self._metrics:
            return

        metric = self._metrics[name]
        old_triggered = metric.triggered
        metric.current_value = value
        metric.triggered = self._check_threshold(value, metric.threshold, metric.operator)

        if metric.triggered and not old_triggered:
            self._trigger_guard(name)

    def get_all_metrics(self) -> list[GuardrailMetric]:
        """Get all monitored metrics."""
        return list(self._metrics.values())

    def get_triggered_metrics(self) -> list[GuardrailMetric]:
        """Get metrics that have triggered."""
        return [m for m in self._metrics.values() if m.triggered]

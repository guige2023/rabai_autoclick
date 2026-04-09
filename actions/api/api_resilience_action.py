"""
API Resilience Action Module.

Provides resilience patterns for API clients including bulkhead isolation,
timeout strategies, and graceful degradation mechanisms.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DegradationLevel(Enum):
    """Service degradation levels."""
    FULL = "full"          # All features available
    GRACEFUL = "graceful"  # Reduced functionality
    MINIMAL = "minimal"    # Core features only
    NONE = "none"          # Service unavailable


@dataclass
class ResilienceConfig:
    """Configuration for resilience strategies."""
    timeout_seconds: float = 30.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0
    bulkhead_max_concurrent: int = 10
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    fallback_enabled: bool = True


@dataclass
class ResilienceMetrics:
    """Metrics for resilience monitoring."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    timed_out_calls: int = 0
    rejected_calls: int = 0
    fallbacks_used: int = 0
    circuit_breaker_trips: int = 0
    avg_latency_ms: float = 0.0


class Bulkhead:
    """
    Bulkhead pattern implementation for resource isolation.

    Limits concurrent executions to prevent resource exhaustion
    and provides isolation between different operation types.
    """

    def __init__(self, max_concurrent: int = 10, max_queue_size: int = 100) -> None:
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active_count = 0
        self._rejected_count = 0

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute a function within the bulkhead limit."""
        if self._active_count >= self.max_concurrent:
            self._rejected_count += 1
            raise RuntimeError(
                f"Bulkhead limit reached: {self._active_count}/{self.max_concurrent}"
            )

        async with self._semaphore:
            self._active_count += 1
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)
            finally:
                self._active_count -= 1

    @property
    def active_count(self) -> int:
        """Number of currently active executions."""
        return self._active_count

    @property
    def rejected_count(self) -> int:
        """Number of rejected calls due to limit."""
        return self._rejected_count


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject all
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.

    Monitors failure rates and opens the circuit to prevent
    cascading failures when a service is degraded.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_seconds: float = 60.0,
        half_open_attempts: int = 3,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.half_open_attempts = half_open_attempts
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._opened_at: Optional[float] = None

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, checking for timeout transitions."""
        if self._state == CircuitState.OPEN:
            if self._opened_at and (time.time() - self._opened_at) >= self.timeout_seconds:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
        return self._state

    def record_success(self) -> None:
        """Record a successful call."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.half_open_attempts:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                logger.info("Circuit breaker CLOSED (recovery successful)")
        elif self._state == CircuitState.CLOSED:
            self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = time.time()
            logger.warning("Circuit breaker OPEN (half-open test failed)")
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = time.time()
            logger.warning(f"Circuit breaker OPEN (threshold reached: {self.failure_count})")

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute a function through the circuit breaker."""
        if self.state == CircuitState.OPEN:
            self._failure_count += 1
            raise RuntimeError("Circuit breaker is OPEN")

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            raise


class GracefulDegradationManager:
    """
    Manages graceful degradation of service functionality.

    Tracks degradation level and provides fallback mechanisms
    for different service states.
    """

    def __init__(self) -> None:
        self._degradation_level = DegradationLevel.FULL
        self._feature_states: Dict[str, DegradationLevel] = {}
        self._fallback_handlers: Dict[str, Callable[[], Any]] = {}

    def set_degradation_level(
        self,
        level: DegradationLevel,
        reason: str = "",
    ) -> None:
        """Set the overall degradation level."""
        old_level = self._degradation_level
        self._degradation_level = level
        logger.info(f"Degradation: {old_level.value} -> {level.value} ({reason})")

    def get_degradation_level(self) -> DegradationLevel:
        """Get the current degradation level."""
        return self._degradation_level

    def register_fallback(
        self,
        feature: str,
        handler: Callable[[], Any],
    ) -> None:
        """Register a fallback handler for a feature."""
        self._fallback_handlers[feature] = handler

    def set_feature_state(
        self,
        feature: str,
        level: DegradationLevel,
    ) -> None:
        """Set degradation level for a specific feature."""
        self._feature_states[feature] = level

    def is_feature_available(self, feature: str) -> bool:
        """Check if a feature is available at current degradation level."""
        if feature in self._feature_states:
            return self._feature_states[feature] != DegradationLevel.NONE
        return self._degradation_level != DegradationLevel.NONE

    def execute_or_fallback(
        self,
        feature: str,
        primary_fn: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Optional[T]:
        """Execute primary function or fallback if degraded."""
        if not self.is_feature_available(feature):
            fallback = self._fallback_handlers.get(feature)
            if fallback:
                return fallback()
            return None

        try:
            return primary_fn(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Primary failed for {feature}, using fallback: {e}")
            fallback = self._fallback_handlers.get(feature)
            if fallback:
                return fallback()
            return None


class APIResilienceAction:
    """
    Unified resilience action for API clients.

    Combines bulkhead, circuit breaker, graceful degradation,
    and timeout management into a single interface.

    Example:
        resilience = APIResilienceAction(config=ResilienceConfig(
            timeout_seconds=10.0,
            retry_attempts=3,
            bulkhead_max_concurrent=5,
        ))
        resilience.register_fallback("get_user", lambda: {"id": "default"})

        result = await resilience.execute(api.get_user, user_id=123)
    """

    def __init__(self, config: Optional[ResilienceConfig] = None) -> None:
        self.config = config or ResilienceConfig()
        self.bulkhead = Bulkhead(
            max_concurrent=self.config.bulkhead_max_concurrent
        )
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_threshold,
            timeout_seconds=self.config.circuit_breaker_timeout_seconds,
        )
        self.degradation = GracefulDegradationManager()
        self.metrics = ResilienceMetrics()
        self._latency_sum = 0.0
        self._latency_count = 0

    def register_fallback(
        self,
        feature: str,
        handler: Callable[[], Any],
    ) -> None:
        """Register a fallback handler."""
        self.degradation.register_fallback(feature, handler)

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        feature: Optional[str] = None,
        use_fallback: bool = True,
        **kwargs: Any,
    ) -> Optional[T]:
        """Execute a function with all resilience mechanisms."""
        self.metrics.total_calls += 1
        start = time.time()

        # Check feature availability
        if feature and not self.degradation.is_feature_available(feature):
            self.metrics.rejected_calls += 1
            if use_fallback:
                return self.degradation.execute_or_fallback(feature, func, *args, **kwargs)
            raise RuntimeError(f"Feature '{feature}' is not available")

        try:
            # Execute through circuit breaker and bulkhead
            result = await asyncio.wait_for(
                self.circuit_breaker.execute(
                    lambda: self.bulkhead.execute(func, *args, **kwargs)
                ),
                timeout=self.config.timeout_seconds,
            )
            self.metrics.successful_calls += 1
            self._update_latency(time.time() - start)
            return result

        except asyncio.TimeoutError:
            self.metrics.timed_out_calls += 1
            self.circuit_breaker.record_failure()
            self._update_latency(time.time() - start)
            if use_fallback and feature:
                return self.degradation.execute_or_fallback(feature, func, *args, **kwargs)
            raise

        except Exception as e:
            self.metrics.failed_calls += 1
            self.circuit_breaker.record_failure()
            self._update_latency(time.time() - start)
            if use_fallback and feature:
                return self.degradation.execute_or_fallback(feature, func, *args, **kwargs)
            raise

    def _update_latency(self, duration_seconds: float) -> None:
        """Update latency metrics."""
        self._latency_sum += duration_seconds * 1000
        self._latency_count += 1
        self.metrics.avg_latency_ms = self._latency_sum / self._latency_count

    def get_metrics(self) -> ResilienceMetrics:
        """Get current resilience metrics."""
        return self.metrics

    def trigger_degradation(
        self,
        level: DegradationLevel,
        reason: str = "",
    ) -> None:
        """Manually trigger a degradation level."""
        self.degradation.set_degradation_level(level, reason)

    def reset(self) -> None:
        """Reset all resilience state."""
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_threshold,
            timeout_seconds=self.config.circuit_breaker_timeout_seconds,
        )
        self.metrics = ResilienceMetrics()
        self._latency_sum = 0.0
        self._latency_count = 0
        logger.info("Resilience state reset")

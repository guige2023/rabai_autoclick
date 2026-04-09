"""API gateway fallback action for degraded service handling.

Provides fallback mechanisms when primary API endpoints fail,
including circuit breaker patterns and graceful degradation.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class FallbackState(Enum):
    """States for fallback mechanism."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FALLBACK_ACTIVE = "fallback_active"
    FAILOVER = "failover"


@dataclass
class FallbackConfig:
    """Configuration for fallback behavior."""
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    fallback_enabled: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class FallbackMetrics:
    """Metrics for fallback operations."""
    primary_requests: int = 0
    fallback_requests: int = 0
    failures: int = 0
    successful_recoveries: int = 0
    state: FallbackState = FallbackState.HEALTHY


class APIGatewayFallbackAction:
    """Handle API fallback and graceful degradation.

    Args:
        config: Fallback configuration options.
        fallback_handler: Callable to invoke when falling back.

    Example:
        >>> action = APIGatewayFallbackAction()
        >>> result = await action.execute(primary_fn, fallback_fn)
    """

    def __init__(
        self,
        config: Optional[FallbackConfig] = None,
        fallback_handler: Optional[Callable] = None,
    ) -> None:
        self.config = config or FallbackConfig()
        self.fallback_handler = fallback_handler
        self.metrics = FallbackMetrics()
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._state = FallbackState.HEALTHY

    async def execute(
        self,
        primary_fn: Callable[..., Any],
        fallback_fn: Optional[Callable[..., Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute with fallback support.

        Args:
            primary_fn: Primary function to execute.
            fallback_fn: Fallback function if primary fails.
            *args: Positional arguments for the functions.
            **kwargs: Keyword arguments for the functions.

        Returns:
            Result from primary or fallback function.

        Raises:
            Exception: If both primary and fallback fail.
        """
        handler = fallback_fn or self.fallback_handler
        if not handler:
            return await primary_fn(*args, **kwargs)

        self.metrics.primary_requests += 1

        try:
            result = await self._call_with_retry(primary_fn, *args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            logger.warning(f"Primary API failed: {e}")

            if self.config.fallback_enabled and self._should_fallback():
                self.metrics.fallback_requests += 1
                self._set_state(FallbackState.FALLBACK_ACTIVE)
                return await handler(*args, **kwargs)
            raise

    async def _call_with_retry(
        self,
        fn: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Call function with retry logic.

        Args:
            fn: Function to call.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Result from function call.

        Raises:
            Exception: If all retries fail.
        """
        last_error: Optional[Exception] = None
        for attempt in range(self.config.max_retries):
            try:
                if asyncio.iscoroutinefunction(fn):
                    return await fn(*args, **kwargs)
                return fn(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
        raise last_error or Exception("All retries failed")

    def _should_fallback(self) -> bool:
        """Check if fallback should be triggered.

        Returns:
            True if fallback conditions are met.
        """
        if self._state == FallbackState.FALLBACK_ACTIVE:
            return True
        if self._failure_count >= self.config.failure_threshold:
            return True
        return False

    def _on_success(self) -> None:
        """Handle successful operation."""
        self._failure_count = 0
        if self._state != FallbackState.HEALTHY:
            self.metrics.successful_recoveries += 1
            self._set_state(FallbackState.HEALTHY)

    def _on_failure(self) -> None:
        """Handle failed operation."""
        self._failure_count += 1
        self._last_failure_time = time.time()
        self.metrics.failures += 1

        if self._failure_count >= self.config.failure_threshold:
            if self._state == FallbackState.HEALTHY:
                self._set_state(FallbackState.DEGRADED)

    def _set_state(self, state: FallbackState) -> None:
        """Update fallback state.

        Args:
            state: New state to set.
        """
        if self._state != state:
            logger.info(f"Fallback state: {self._state.value} -> {state.value}")
            self._state = state
            self.metrics.state = state

    def get_metrics(self) -> FallbackMetrics:
        """Get current fallback metrics.

        Returns:
            Current metrics snapshot.
        """
        return self.metrics

    def reset(self) -> None:
        """Reset fallback state and metrics."""
        self._failure_count = 0
        self._last_failure_time = None
        self._set_state(FallbackState.HEALTHY)
        self.metrics = FallbackMetrics()

    def is_healthy(self) -> bool:
        """Check if the service is in healthy state.

        Returns:
            True if state is HEALTHY.
        """
        return self._state == FallbackState.HEALTHY

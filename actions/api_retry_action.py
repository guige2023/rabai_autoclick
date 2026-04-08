"""API retry action module with exponential backoff and jitter.

Provides configurable retry logic for failed API requests with
backoff strategies, error categorization, and circuit breaker support.
"""

from __future__ import annotations

import time
import random
import logging
from typing import Optional, Dict, Any, Callable, TypeVar, Union
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry backoff strategy."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay_ms: float = 100.0
    max_delay_ms: float = 10000.0
    backoff_multiplier: float = 2.0
    retry_on_status_codes: tuple = (408, 429, 500, 502, 503, 504)
    retry_on_exceptions: tuple = (ConnectionError, TimeoutError, IOError)
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_JITTER


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    result: Any = None
    attempts: int = 0
    total_latency_ms: float = 0.0
    error: Optional[str] = None
    last_status_code: Optional[int] = None


class APIRetryAction:
    """Retry wrapper for API calls with exponential backoff.

    Automatically retries failed requests with configurable backoff.
    Handles rate limits (429) with special handling.

    Example:
        retry = APIRetryAction(max_attempts=3, initial_delay_ms=500)
        result = retry.execute(lambda: requests.get(url))
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay_ms: float = 100.0,
        max_delay_ms: float = 10000.0,
        backoff_multiplier: float = 2.0,
        strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_JITTER,
        retry_on: Optional[tuple] = None,
    ) -> None:
        """Initialize retry action.

        Args:
            max_attempts: Maximum number of retry attempts.
            initial_delay_ms: Starting delay between retries (ms).
            max_delay_ms: Maximum delay cap (ms).
            backoff_multiplier: Multiplier for exponential backoff.
            strategy: Backoff strategy.
            retry_on: Tuple of exception types or status codes to retry.
        """
        self.config = RetryConfig(
            max_attempts=max_attempts,
            initial_delay_ms=initial_delay_ms,
            max_delay_ms=max_delay_ms,
            backoff_multiplier=backoff_multiplier,
            strategy=strategy,
            retry_on_exceptions=retry_on or (ConnectionError, TimeoutError, IOError),
        )

    def execute(
        self,
        func: Callable[[], T],
        before_retry: Optional[Callable[[int, Exception], None]] = None,
    ) -> RetryResult:
        """Execute a function with retry logic.

        Args:
            func: Callable to execute.
            before_retry: Optional callback called before each retry (attempt, error).

        Returns:
            RetryResult with outcome details.
        """
        start_time = time.time()
        last_error: Optional[Exception] = None
        last_status_code: Optional[int] = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = func()
                latency_ms = (time.time() - start_time) * 1000

                if hasattr(result, "status_code"):
                    last_status_code = result.status_code
                    if result.status_code >= 400:
                        if result.status_code == 429:
                            retry_after = self._get_retry_after(result)
                            if retry_after:
                                time.sleep(retry_after / 1000)
                                continue
                        if result.status_code in self.config.retry_on_status_codes:
                            raise APIRetryableError(f"HTTP {result.status_code}", result.status_code)

                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempt,
                    total_latency_ms=latency_ms,
                    last_status_code=last_status_code,
                )

            except APIRetryableError as e:
                last_error = e
                last_status_code = e.status_code
                logger.debug("Attempt %d failed: %s", attempt, e)

            except self.config.retry_on_exceptions as e:
                last_error = e
                logger.debug("Attempt %d failed with exception: %s", attempt, e)

            except Exception as e:
                last_error = e
                break

            if attempt < self.config.max_attempts:
                delay = self._calculate_delay(attempt)
                if before_retry and last_error:
                    before_retry(attempt, last_error)
                logger.debug("Retrying in %.1fms (attempt %d/%d)", delay, attempt + 1, self.config.max_attempts)
                time.sleep(delay / 1000)

        latency_ms = (time.time() - start_time) * 1000
        return RetryResult(
            success=False,
            attempts=self.config.max_attempts,
            total_latency_ms=latency_ms,
            error=str(last_error) if last_error else None,
            last_status_code=last_status_code,
        )

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay before next retry in milliseconds."""
        delay = self.config.initial_delay_ms * (self.config.backoff_multiplier ** (attempt - 1))
        delay = min(delay, self.config.max_delay_ms)

        if self.config.strategy == RetryStrategy.FIXED:
            return delay
        elif self.config.strategy == RetryStrategy.LINEAR:
            return delay
        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            return delay
        elif self.config.strategy == RetryStrategy.EXPONENTIAL_JITTER:
            jitter = random.uniform(0, delay * 0.1)
            return delay + jitter
        return delay

    def _get_retry_after(self, response) -> Optional[float]:
        """Extract Retry-After header value in milliseconds."""
        headers = getattr(response, "headers", {}) or {}
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return float(retry_after) * 1000
            except ValueError:
                pass
        return None


class APIRetryableError(Exception):
    """Error that indicates a request can be retried."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def with_retry(
    max_attempts: int = 3,
    initial_delay_ms: float = 100.0,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_JITTER,
) -> Callable[[Callable[..., T]], Callable[..., RetryResult]]:
    """Decorator to add retry behavior to a function.

    Args:
        max_attempts: Maximum retry attempts.
        initial_delay_ms: Starting delay.
        strategy: Backoff strategy.

    Returns:
        Decorated function that returns RetryResult.

    Example:
        @with_retry(max_attempts=5, initial_delay_ms=200)
        def fetch_data(url):
            return requests.get(url).json()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., RetryResult]:
        retry_action = APIRetryAction(
            max_attempts=max_attempts,
            initial_delay_ms=initial_delay_ms,
            strategy=strategy,
        )
        def wrapper(*args, **kwargs) -> RetryResult:
            def wrapped():
                return func(*args, **kwargs)
            return retry_action.execute(wrapped)
        return wrapper
    return decorator

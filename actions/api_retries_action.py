"""API Retries Action Module.

Provides intelligent retry logic for API requests with exponential
backoff, jitter, rate limit handling, and configurable retry policies.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy types."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIBONACCI = "fibonacci"
    CONSTANT = "constant"


class RetryableErrorType(Enum):
    """Types of errors that can be retried."""
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    NETWORK_ERROR = "network_error"
    SERVICE_UNAVAILABLE = "service_unavailable"
    GATEWAY_TIMEOUT = "gateway_timeout"


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    retryable_errors: Set[RetryableErrorType] = field(default_factory=lambda: {
        RetryableErrorType.TIMEOUT,
        RetryableErrorType.RATE_LIMIT,
        RetryableErrorType.SERVER_ERROR,
        RetryableErrorType.NETWORK_ERROR,
        RetryableErrorType.SERVICE_UNAVAILABLE,
        RetryableErrorType.GATEWAY_TIMEOUT,
    })
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retry_on_timeout: bool = True
    retry_on_rate_limit: bool = True
    respect_retry_after_header: bool = True


@dataclass
class RetryAttempt:
    """Record of a single retry attempt."""
    attempt_number: int
    timestamp: datetime
    delay_used: float
    error_type: Optional[RetryableErrorType] = None
    error_message: Optional[str] = None
    success: bool = False
    duration_ms: float = 0.0


@dataclass
class RetryResult:
    """Result of retry operation."""
    success: bool
    final_error: Optional[str] = None
    total_attempts: int = 0
    total_duration_ms: float = 0.0
    attempts: List[RetryAttempt] = field(default_factory=list)
    retry_after_header: Optional[float] = None
    backoff_used: float = 0.0


class BackoffCalculator:
    """Calculate backoff delays for retries."""

    def __init__(self, policy: RetryPolicy):
        self._policy = policy
        self._fib_cache = {0: 0, 1: 1}

    def calculate_delay(self, attempt: int, retry_after: Optional[float] = None) -> float:
        """Calculate delay for a given attempt number."""
        if retry_after and self._policy.respect_retry_after_header:
            return min(retry_after, self._policy.max_delay_seconds)

        if self._policy.strategy == RetryStrategy.EXPONENTIAL:
            delay = self._policy.initial_delay_seconds * (self._policy.exponential_base ** (attempt - 1))
        elif self._policy.strategy == RetryStrategy.LINEAR:
            delay = self._policy.initial_delay_seconds * attempt
        elif self._policy.strategy == RetryStrategy.FIBONACCI:
            delay = self._policy.initial_delay_seconds * self._fibonacci(attempt)
        elif self._policy.strategy == RetryStrategy.CONSTANT:
            delay = self._policy.initial_delay_seconds
        else:
            delay = self._policy.initial_delay_seconds

        delay = min(delay, self._policy.max_delay_seconds)

        if self._policy.jitter:
            jitter_range = delay * self._policy.jitter_factor
            delay = delay + random.uniform(-jitter_range, jitter_range)
            delay = max(0.1, delay)

        return delay

    def _fibonacci(self, n: int) -> int:
        """Calculate fibonacci number with caching."""
        if n not in self._fib_cache:
            self._fib_cache[n] = self._fibonacci(n - 1) + self._fibonacci(n - 2)
        return self._fib_cache[n]


class ErrorClassifier:
    """Classify errors to determine retry eligibility."""

    RETRYABLE_STATUS_CODES = {
        408, 429, 500, 502, 503, 504,
        509, 511, 520, 521, 522, 523, 524
    }

    RATE_LIMIT_STATUS_CODES = {429, 509, 511}

    @staticmethod
    def classify_error(
        error: Exception,
        status_code: Optional[int] = None,
        response_headers: Optional[Dict[str, str]] = None
    ) -> Optional[RetryableErrorType]:
        """Classify an error into a retryable type."""
        error_str = str(error).lower()

        if status_code:
            if status_code in ErrorClassifier.RATE_LIMIT_STATUS_CODES:
                return RetryableErrorType.RATE_LIMIT
            if status_code == 504:
                return RetryableErrorType.GATEWAY_TIMEOUT
            if status_code in ErrorClassifier.RETRYABLE_STATUS_CODES:
                return RetryableErrorType.SERVER_ERROR

        if "timeout" in error_str or "timed out" in error_str:
            return RetryableErrorType.TIMEOUT
        if "connection" in error_str or "network" in error_str or "refused" in error_str:
            return RetryableErrorType.NETWORK_ERROR
        if "rate limit" in error_str or "too many requests" in error_str:
            return RetryableErrorType.RATE_LIMIT
        if "unavailable" in error_str or "503" in error_str:
            return RetryableErrorType.SERVICE_UNAVAILABLE

        return None

    @staticmethod
    def get_retry_after_seconds(headers: Dict[str, str]) -> Optional[float]:
        """Extract Retry-After header value."""
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return None


class ApiRetriesAction(BaseAction):
    """Action for managing API retries with intelligent backoff."""

    def __init__(self):
        super().__init__(name="api_retries")
        self._policy = RetryPolicy()
        self._backoff_calculator = BackoffCalculator(self._policy)
        self._lock = threading.Lock()
        self._retry_stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total_retries": 0,
            "total_successes": 0,
            "total_failures": 0,
            "total_delay_seconds": 0.0,
        })

    def configure(self, policy: RetryPolicy):
        """Configure retry policy."""
        self._policy = policy
        self._backoff_calculator = BackoffCalculator(policy)

    def execute_with_retry(
        self,
        func: Callable[..., T],
        *args: Any,
        retry_id: Optional[str] = None,
        **kwargs: Any
    ) -> RetryResult:
        """Execute a function with retry logic."""
        retry_id = retry_id or f"retry_{id(func)}"
        start_time = time.time()
        attempts: List[RetryAttempt] = []

        for attempt_num in range(1, self._policy.max_attempts + 1):
            attempt_start = time.time()
            error_type: Optional[RetryableErrorType] = None
            error_message: Optional[str] = None
            success = False
            retry_after: Optional[float] = None

            try:
                result = func(*args, **kwargs)
                success = True
                error_type = None
            except Exception as e:
                error_type = ErrorClassifier.classify_error(e)
                error_message = str(e)

                if error_type and error_type not in self._policy.retryable_errors:
                    logger.debug(f"Error {error_type} not retryable")
                    break

            duration_ms = (time.time() - attempt_start) * 1000

            attempt_record = RetryAttempt(
                attempt_number=attempt_num,
                timestamp=datetime.now(),
                delay_used=0.0,
                error_type=error_type,
                error_message=error_message,
                success=success,
                duration_ms=duration_ms
            )
            attempts.append(attempt_record)

            if success:
                total_duration_ms = (time.time() - start_time) * 1000
                self._record_success(retry_id, len(attempts), total_duration_ms)
                return RetryResult(
                    success=True,
                    total_attempts=len(attempts),
                    total_duration_ms=total_duration_ms,
                    attempts=attempts
                )

            if attempt_num >= self._policy.max_attempts:
                break

            retry_after = None
            if self._policy.respect_retry_after_header:
                pass

            delay = self._backoff_calculator.calculate_delay(attempt_num, retry_after)
            attempt_record.delay_used = delay

            logger.debug(
                f"Retry {attempt_num}/{self._policy.max_attempts} for {retry_id} "
                f"after {delay:.2f}s delay. Error: {error_message}"
            )

            time.sleep(delay)

        total_duration_ms = (time.time() - start_time) * 1000
        total_delay = sum(a.delay_used for a in attempts)
        self._record_failure(retry_id, len(attempts), total_delay)

        return RetryResult(
            success=False,
            final_error=attempts[-1].error_message if attempts else "Unknown error",
            total_attempts=len(attempts),
            total_duration_ms=total_duration_ms,
            attempts=attempts,
            backoff_used=total_delay
        )

    def _record_success(self, retry_id: str, attempts: int, duration_ms: float):
        """Record successful retry statistics."""
        with self._lock:
            stats = self._retry_stats[retry_id]
            stats["total_successes"] += 1
            stats["total_retries"] += max(0, attempts - 1)
            stats["last_success_duration_ms"] = duration_ms
            stats["last_attempt_count"] = attempts

    def _record_failure(self, retry_id: str, attempts: int, delay_seconds: float):
        """Record failed retry statistics."""
        with self._lock:
            stats = self._retry_stats[retry_id]
            stats["total_failures"] += 1
            stats["total_retries"] += attempts
            stats["total_delay_seconds"] += delay_seconds
            stats["last_attempt_count"] = attempts

    def get_stats(self, retry_id: Optional[str] = None) -> Dict[str, Any]:
        """Get retry statistics."""
        with self._lock:
            if retry_id:
                return dict(self._retry_stats.get(retry_id, {}))
            return {
                k: dict(v) for k, v in self._retry_stats.items()
            }

    def reset_stats(self, retry_id: Optional[str] = None):
        """Reset retry statistics."""
        with self._lock:
            if retry_id:
                self._retry_stats[retry_id] = {
                    "total_retries": 0,
                    "total_successes": 0,
                    "total_failures": 0,
                    "total_delay_seconds": 0.0,
                }
            else:
                self._retry_stats.clear()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute retry action on a callable."""
        try:
            func = params.get("func")
            if not func or not callable(func):
                return ActionResult(success=False, error="func is required and must be callable")

            args = params.get("args", ())
            kwargs = params.get("kwargs", {})
            retry_id = params.get("retry_id")

            result = self.execute_with_retry(func, *args, retry_id=retry_id, **kwargs)

            return ActionResult(
                success=result.success,
                data={
                    "success": result.success,
                    "total_attempts": result.total_attempts,
                    "total_duration_ms": result.total_duration_ms,
                    "backoff_used_seconds": result.backoff_used,
                    "error": result.final_error
                }
            )
        except Exception as e:
            logger.exception("Retry execution failed")
            return ActionResult(success=False, error=str(e))

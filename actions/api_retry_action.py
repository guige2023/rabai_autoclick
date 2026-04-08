# Copyright (c) 2024. coded by claude
"""API Retry Strategy Action Module.

Implements various retry strategies for API calls including exponential backoff,
jitter, and circuit breaker patterns.
"""
from typing import Optional, Callable, Any, TypeVar, List
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import asyncio
import random
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryStrategy(Enum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIBONACCI = "fibonacci"


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    retry_on: Optional[List[Exception]] = None


@dataclass
class RetryResult:
    success: bool
    attempts: int
    final_error: Optional[Exception]
    total_time_ms: float


class RetryHandler:
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()

    def calculate_delay(self, attempt: int) -> float:
        if self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.base_delay * (2 ** (attempt - 1))
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.base_delay * attempt
        elif self.config.strategy == RetryStrategy.FIBONACCI:
            delay = self.config.base_delay * self._fibonacci(attempt)
        else:
            delay = self.config.base_delay
        delay = min(delay, self.config.max_delay)
        if self.config.jitter:
            delay *= 0.5 + random.random()
        return delay

    def _fibonacci(self, n: int) -> int:
        if n <= 1:
            return 1
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    def should_retry(self, exception: Exception) -> bool:
        if self.config.retry_on is None:
            return True
        return any(isinstance(exception, exc_type) for exc_type in self.config.retry_on)

    async def execute(self, func: Callable[..., Any]) -> RetryResult:
        start_time = datetime.now()
        last_error: Optional[Exception] = None
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = func()
                if asyncio.iscoroutine(result):
                    result = await result
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                return RetryResult(success=True, attempts=attempt, final_error=None, total_time_ms=elapsed)
            except Exception as e:
                last_error = e
                if attempt == self.config.max_attempts or not self.should_retry(e):
                    break
                delay = self.calculate_delay(attempt)
                logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)
        elapsed = (datetime.now() - start_time).total_seconds() * 1000
        return RetryResult(success=False, attempts=self.config.max_attempts, final_error=last_error, total_time_ms=elapsed)

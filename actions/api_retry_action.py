"""
API Retry Action Module.

Provides configurable retry logic for API requests with exponential
 backoff, jitter, and status-code-aware handling.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Optional, Set
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.2
    retryable_status_codes: Set[int] = field(default_factory=lambda: {408, 429, 500, 502, 503, 504})
    retryable_exceptions: tuple[type[Exception], ...] = (TimeoutError, ConnectionError, asyncio.TimeoutError)
    fatal_status_codes: Set[int] = field(default_factory=lambda: {400, 401, 403, 404})


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    data: Optional[Any] = None
    attempts: int = 0
    total_time_ms: float = 0.0
    error: Optional[str] = None
    last_status_code: Optional[int] = None


class APIMockClient:
    """Mock HTTP client for demonstration."""

    def __init__(self) -> None:
        self.request_count = 0

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> tuple[int, Any]:
        self.request_count += 1
        await asyncio.sleep(0.01)
        return (200, {"status": "ok", "request_num": self.request_count})


class APIRetryAction:
    """
    Retry wrapper for API requests.

    Automatically retries failed requests with configurable backoff
    and status-code-aware decision making.

    Example:
        client = APIMockClient()
        retry = APIRetryAction(client, RetryConfig(max_attempts=5))
        result = await retry.execute("GET", "https://api.example.com/data")
    """

    def __init__(
        self,
        client: Any,
        config: Optional[RetryConfig] = None,
    ) -> None:
        self.client = client
        self.config = config or RetryConfig()

    async def execute(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> RetryResult:
        """Execute request with automatic retries."""
        start_time = time.monotonic()
        last_error = None
        last_status = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                status, data = await self.client.request(method, url, **kwargs)
                last_status = status

                if status in self.config.fatal_status_codes:
                    return RetryResult(
                        success=False,
                        attempts=attempt,
                        total_time_ms=(time.monotonic() - start_time) * 1000,
                        error=f"Fatal status code: {status}",
                        last_status_code=status,
                    )

                if status < 400:
                    return RetryResult(
                        success=True,
                        data=data,
                        attempts=attempt,
                        total_time_ms=(time.monotonic() - start_time) * 1000,
                        last_status_code=status,
                    )

                if status not in self.config.retryable_status_codes:
                    return RetryResult(
                        success=False,
                        data=data,
                        attempts=attempt,
                        total_time_ms=(time.monotonic() - start_time) * 1000,
                        error=f"Unexpected status: {status}",
                        last_status_code=status,
                    )

                last_error = f"Retryable status: {status}"

            except self.config.retryable_exceptions as e:
                last_error = str(e)

            except Exception as e:
                return RetryResult(
                    success=False,
                    attempts=attempt,
                    total_time_ms=(time.monotonic() - start_time) * 1000,
                    error=f"Non-retryable error: {e}",
                )

            if attempt < self.config.max_attempts:
                delay = self._calculate_delay(attempt)
                logger.debug(f"Attempt {attempt} failed, retrying in {delay:.2f}s: {last_error}")
                await asyncio.sleep(delay)

        return RetryResult(
            success=False,
            attempts=self.config.max_attempts,
            total_time_ms=(time.monotonic() - start_time) * 1000,
            error=last_error or "Max retries exceeded",
            last_status_code=last_status,
        )

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** (attempt - 1)),
            self.config.max_delay,
        )

        if self.config.jitter:
            jitter_range = delay * self.config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    async def execute_batch(
        self,
        requests: list[tuple[str, str]],
    ) -> list[RetryResult]:
        """Execute multiple requests with retries in parallel."""
        tasks = [self.execute(method, url) for method, url in requests]
        return await asyncio.gather(*tasks)

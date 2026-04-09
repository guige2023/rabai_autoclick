"""API Adaptive Action Module.

Adaptive API client with automatic retry, timeout, and fallback handling.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategies."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"


@dataclass
class APIConfig:
    """API client configuration."""
    base_url: str
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    backoff_factor: float = 2.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0


@dataclass
class APIResponse(Generic[T]):
    """API response wrapper."""
    data: T | None
    status_code: int
    success: bool
    error: str | None = None
    attempt_count: int = 1
    latency_ms: float = 0.0


class APIClient:
    """Adaptive API client with retry and circuit breaker."""

    def __init__(self, config: APIConfig) -> None:
        self.config = config
        self._session: Any = None
        self._failure_count = 0
        self._circuit_open_since: float | None = None
        self._last_success: float | None = None

    async def request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> APIResponse:
        """Make an HTTP request with retry logic."""
        if self._is_circuit_open():
            return APIResponse(
                data=None,
                status_code=503,
                success=False,
                error="Circuit breaker is open"
            )
        url = f"{self.config.base_url}{endpoint}"
        last_error: str | None = None
        for attempt in range(self.config.max_retries + 1):
            start = time.monotonic()
            try:
                response = await self._do_request(method, url, **kwargs)
                latency = (time.monotonic() - start) * 1000
                if 200 <= response.status < 300:
                    self._on_success()
                    data = await response.json() if response.content else None
                    return APIResponse(
                        data=data,
                        status_code=response.status,
                        success=True,
                        attempt_count=attempt + 1,
                        latency_ms=latency
                    )
                elif response.status >= 500:
                    last_error = f"Server error: {response.status}"
                else:
                    return APIResponse(
                        data=None,
                        status_code=response.status,
                        success=False,
                        error=f"Client error: {response.status}",
                        attempt_count=attempt + 1,
                        latency_ms=latency
                    )
            except asyncio.TimeoutError:
                last_error = "Request timed out"
            except Exception as e:
                last_error = str(e)
            if attempt < self.config.max_retries:
                delay = self._calculate_delay(attempt)
                await asyncio.sleep(delay)
        self._on_failure()
        return APIResponse(
            data=None,
            status_code=0,
            success=False,
            error=last_error,
            attempt_count=self.config.max_retries + 1
        )

    async def _do_request(self, method: str, url: str, **kwargs) -> Any:
        """Make the actual HTTP request."""
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, **kwargs) as response:
                return response

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate retry delay based on strategy."""
        if self.config.retry_strategy == RetryStrategy.EXPONENTIAL:
            return self.config.retry_delay * (self.config.backoff_factor ** attempt)
        elif self.config.retry_strategy == RetryStrategy.LINEAR:
            return self.config.retry_delay * (attempt + 1)
        return self.config.retry_delay

    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open."""
        if self._circuit_open_since is None:
            return False
        if time.time() - self._circuit_open_since > self.config.circuit_breaker_timeout:
            self._circuit_open_since = None
            return False
        return True

    def _on_success(self) -> None:
        """Handle successful request."""
        self._failure_count = 0
        self._last_success = time.time()

    def _on_failure(self) -> None:
        """Handle failed request."""
        self._failure_count += 1
        if self._failure_count >= self.config.circuit_breaker_threshold:
            self._circuit_open_since = time.time()


class APIPool:
    """Pool of API clients for load balancing."""

    def __init__(self) -> None:
        self._clients: list[APIClient] = []
        self._index = 0
        self._lock = asyncio.Lock()

    def add_client(self, client: APIClient) -> None:
        """Add a client to the pool."""
        self._clients.append(client)

    async def request(self, method: str, endpoint: str, **kwargs) -> APIResponse:
        """Make request using round-robin load balancing."""
        async with self._lock:
            if not self._clients:
                return APIResponse(None, 0, False, "No clients in pool")
            client = self._clients[self._index]
            self._index = (self._index + 1) % len(self._clients)
        return await client.request(method, endpoint, **kwargs)

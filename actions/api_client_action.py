"""
API client with retry and resilience module.

Provides a robust HTTP client with automatic retry,
circuit breaker, and error handling.

Author: Aito Auto Agent
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Optional,
)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


@dataclass
class ApiResponse:
    """Standardized API response."""
    success: bool
    status_code: Optional[int] = None
    data: Optional[Any] = None
    error: Optional[str] = None
    headers: dict = field(default_factory=dict)
    duration_ms: float = 0.0


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    half_open_max_calls: int = 3


class CircuitBreaker:
    """
    Circuit breaker for API calls.

    Prevents cascading failures by opening the circuit
    when failure rate is high.
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self._config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitState.HALF_OPEN
            return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset."""
        if self._last_failure_time is None:
            return True
        return (time.time() - self._last_failure_time) >= self._config.timeout_seconds

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._failure_count = 0

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._success_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.CLOSED:
                if self._failure_count >= self._config.failure_threshold:
                    self._state = CircuitState.OPEN

            elif self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._success_count = 0

    def can_execute(self) -> bool:
        """Check if request can be executed."""
        return self.state != CircuitState.OPEN


class RetryHandler:
    """Handles retry logic with backoff."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay_ms: float = 100,
        max_delay_ms: float = 5000,
        exponential_base: float = 2.0,
        jitter: bool = True
    ):
        self._max_attempts = max_attempts
        self._initial_delay = initial_delay_ms / 1000
        self._max_delay = max_delay_ms / 1000
        self._exponential_base = exponential_base
        self._jitter = jitter

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt."""
        delay = min(
            self._initial_delay * (self._exponential_base ** attempt),
            self._max_delay
        )

        if self._jitter:
            import random
            delay *= (0.5 + random.random())

        return delay

    def should_retry(
        self,
        attempt: int,
        response: Optional[ApiResponse],
        exception: Optional[Exception]
    ) -> bool:
        """Determine if request should be retried."""
        if attempt >= self._max_attempts:
            return False

        if exception is not None:
            return True

        if response is not None:
            return response.status_code in (429, 500, 502, 503, 504)

        return False


class ApiClient:
    """
    Robust API client with retry and circuit breaker.

    Example:
        client = ApiClient(base_url="https://api.example.com")

        response = client.get("/users/123")

        if response.success:
            print(response.data)
        else:
            print(f"Error: {response.error}")
    """

    def __init__(
        self,
        base_url: str = "",
        timeout: float = 30.0,
        max_retries: int = 3,
        enable_circuit_breaker: bool = True
    ):
        self._base_url = base_url.rstrip("/") if base_url else ""
        self._timeout = timeout
        self._retry_handler = RetryHandler(max_attempts=max_retries)
        self._circuit_breaker = CircuitBreaker() if enable_circuit_breaker else None
        self._session = None
        self._default_headers = {}

    def set_header(self, name: str, value: str) -> ApiClient:
        """Set default header."""
        self._default_headers[name] = value
        return self

    def set_headers(self, headers: dict[str, str]) -> ApiClient:
        """Set multiple default headers."""
        self._default_headers.update(headers)
        return self

    def get_session(self):
        """Get or create requests session."""
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update(self._default_headers)
        return self._session

    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> ApiResponse:
        """Make HTTP request and return standardized response."""
        import requests

        start_time = time.time()
        session = self.get_session()

        try:
            response = session.request(
                method=method,
                url=url,
                timeout=kwargs.pop("timeout", self._timeout),
                **kwargs
            )

            duration_ms = (time.time() - start_time) * 1000

            success = 200 <= response.status_code < 300

            return ApiResponse(
                success=success,
                status_code=response.status_code,
                data=response.json() if response.content else None,
                error=None if success else response.text,
                headers=dict(response.headers),
                duration_ms=duration_ms
            )

        except requests.exceptions.Timeout:
            return ApiResponse(
                success=False,
                error="Request timeout",
                duration_ms=(time.time() - start_time) * 1000
            )

        except requests.exceptions.RequestException as e:
            return ApiResponse(
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000
            )

    def _execute_with_retry(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> ApiResponse:
        """Execute request with retry logic."""
        attempt = 0
        last_response = None

        while True:
            if self._circuit_breaker and not self._circuit_breaker.can_execute():
                return ApiResponse(
                    success=False,
                    error="Circuit breaker is open"
                )

            response = self._make_request(method, url, **kwargs)

            if response.success or not self._retry_handler.should_retry(
                attempt, response, None
            ):
                if self._circuit_breaker:
                    if response.success:
                        self._circuit_breaker.record_success()
                    else:
                        self._circuit_breaker.record_failure()

                return response

            if attempt < self._retry_handler._max_attempts - 1:
                delay = self._retry_handler.calculate_delay(attempt)
                time.sleep(delay)

            last_response = response
            attempt += 1

        return last_response or ApiResponse(success=False, error="Max retries exceeded")

    def get(
        self,
        path: str,
        params: Optional[dict] = None,
        **kwargs
    ) -> ApiResponse:
        """Make GET request."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._execute_with_retry("GET", url, params=params, **kwargs)

    def post(
        self,
        path: str,
        data: Optional[Any] = None,
        json: Optional[dict] = None,
        **kwargs
    ) -> ApiResponse:
        """Make POST request."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._execute_with_retry("POST", url, data=data, json=json, **kwargs)

    def put(
        self,
        path: str,
        data: Optional[Any] = None,
        json: Optional[dict] = None,
        **kwargs
    ) -> ApiResponse:
        """Make PUT request."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._execute_with_retry("PUT", url, data=data, json=json, **kwargs)

    def patch(
        self,
        path: str,
        data: Optional[Any] = None,
        json: Optional[dict] = None,
        **kwargs
    ) -> ApiResponse:
        """Make PATCH request."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._execute_with_retry("PATCH", url, data=data, json=json, **kwargs)

    def delete(self, path: str, **kwargs) -> ApiResponse:
        """Make DELETE request."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._execute_with_retry("DELETE", url, **kwargs)

    def head(self, path: str, **kwargs) -> ApiResponse:
        """Make HEAD request."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._execute_with_retry("HEAD", url, **kwargs)

    def options(self, path: str, **kwargs) -> ApiResponse:
        """Make OPTIONS request."""
        url = f"{self._base_url}/{path.lstrip('/')}"
        return self._execute_with_retry("OPTIONS", url, **kwargs)

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            self._session.close()
            self._session = None


class BatchApiClient:
    """
    Batch API client for parallel requests.

    Example:
        client = BatchApiClient(max_concurrent=5)

        results = client.execute([
            lambda: api.get("/users/1"),
            lambda: api.get("/users/2"),
        ])
    """

    def __init__(self, max_concurrent: int = 5):
        self._max_concurrent = max_concurrent
        self._executor = None

    def execute(
        self,
        requests: list[Callable[[], ApiResponse]]
    ) -> list[ApiResponse]:
        """Execute requests in parallel."""
        import concurrent.futures

        results = [None] * len(requests)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._max_concurrent
        ) as executor:
            futures = {
                executor.submit(req): i
                for i, req in enumerate(requests)
            }

            for future in concurrent.futures.as_completed(futures):
                index = futures[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    results[index] = ApiResponse(success=False, error=str(e))

        return results


def create_api_client(
    base_url: str = "",
    **kwargs
) -> ApiClient:
    """Factory to create an ApiClient."""
    return ApiClient(base_url=base_url, **kwargs)


def create_batch_client(max_concurrent: int = 5) -> BatchApiClient:
    """Factory to create a BatchApiClient."""
    return BatchApiClient(max_concurrent=max_concurrent)

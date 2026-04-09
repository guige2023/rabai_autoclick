"""
Adaptive Timeout Manager.

Dynamically adjusts timeouts based on historical performance,
network conditions, and endpoint-specific characteristics.

Example:
    >>> manager = AdaptiveTimeoutManager()
    >>> timeout = manager.get_timeout("api.example.com")
    >>> result = fetch_with_timeout(url, timeout=timeout)
    >>> manager.record_latency("api.example.com", result.latency)
"""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EndpointStats:
    """Statistics for a specific endpoint."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    timeouts: int = 0
    total_latency: float = 0.0
    min_latency: float = float('inf')
    max_latency: float = 0.0
    p50_latency: float = 0.0
    p95_latency: float = 0.0
    p99_latency: float = 0.0
    last_request_time: float = 0.0
    latencies: list[float] = field(default_factory=list)


@dataclass
class TimeoutConfig:
    """Configuration for adaptive timeout behavior."""
    initial_timeout: float = 5.0
    min_timeout: float = 0.5
    max_timeout: float = 60.0
    backoff_factor: float = 1.5
    success_factor: float = 0.9
    failure_factor: float = 1.5
    window_size: int = 100


class AdaptiveTimeoutManager:
    """
    Adaptive timeout manager that adjusts based on endpoint performance.

    Maintains per-endpoint statistics and dynamically adjusts timeouts
    to optimize for success rate while minimizing latency.
    """

    def __init__(
        self,
        initial_timeout: float = 5.0,
        min_timeout: float = 0.5,
        max_timeout: float = 60.0,
        backoff_factor: float = 1.5,
        success_factor: float = 0.9,
        failure_factor: float = 1.5,
        window_size: int = 100
    ):
        """
        Initialize the adaptive timeout manager.

        Args:
            initial_timeout: Starting timeout in seconds
            min_timeout: Minimum allowed timeout
            max_timeout: Maximum allowed timeout
            backoff_factor: Multiplier for timeout on failure
            success_factor: How much to reduce timeout on success
            failure_factor: How much to increase timeout on failure
            window_size: Number of recent requests to consider
        """
        self._config = TimeoutConfig(
            initial_timeout=initial_timeout,
            min_timeout=min_timeout,
            max_timeout=max_timeout,
            backoff_factor=backoff_factor,
            success_factor=success_factor,
            failure_factor=failure_factor,
            window_size=window_size
        )
        self._timeouts: dict[str, float] = defaultdict(lambda: initial_timeout)
        self._stats: dict[str, EndpointStats] = defaultdict(EndpointStats)
        self._lock = threading.Lock()

    def get_timeout(self, endpoint: str) -> float:
        """
        Get the current timeout for an endpoint.

        Args:
            endpoint: Endpoint identifier

        Returns:
            Recommended timeout in seconds
        """
        with self._lock:
            return self._timeouts.get(endpoint, self._config.initial_timeout)

    def record_latency(
        self,
        endpoint: str,
        latency: float,
        success: bool = True,
        was_timeout: bool = False
    ) -> None:
        """
        Record the result of a request.

        Args:
            endpoint: Endpoint identifier
            latency: Request latency in seconds
            success: Whether the request succeeded
            was_timeout: Whether the request timed out
        """
        with self._lock:
            stats = self._stats[endpoint]
            current_timeout = self._timeouts[endpoint]

            stats.total_requests += 1
            stats.last_request_time = time.time()
            stats.total_latency += latency
            stats.latencies.append(latency)

            if len(stats.latencies) > self._config.window_size:
                stats.latencies = stats.latencies[-self._config.window_size:]

            if latency < stats.min_latency:
                stats.min_latency = latency
            if latency > stats.max_latency:
                stats.max_latency = latency

            self._update_percentiles(stats)

            if success:
                stats.successful_requests += 1
                new_timeout = current_timeout * self._config.success_factor
                new_timeout = max(self._config.min_timeout, new_timeout)
                self._timeouts[endpoint] = new_timeout
            else:
                stats.failed_requests += 1
                if was_timeout:
                    stats.timeouts += 1
                factor = self._config.backoff_factor if was_timeout else self._config.failure_factor
                new_timeout = current_timeout * factor
                new_timeout = min(self._config.max_timeout, new_timeout)
                self._timeouts[endpoint] = new_timeout

    def _update_percentiles(self, stats: EndpointStats) -> None:
        """Update percentile calculations."""
        if not stats.latencies:
            return

        sorted_latencies = sorted(stats.latencies)
        n = len(sorted_latencies)

        stats.p50_latency = sorted_latencies[int(n * 0.5)]
        stats.p95_latency = sorted_latencies[int(n * 0.95)] if n >= 20 else sorted_latencies[-1]
        stats.p99_latency = sorted_latencies[int(n * 0.99)] if n >= 100 else sorted_latencies[-1]

    def get_stats(self, endpoint: str) -> Optional[EndpointStats]:
        """Get statistics for an endpoint."""
        with self._lock:
            if endpoint not in self._stats:
                return None
            stats = self._stats[endpoint]
            return EndpointStats(
                total_requests=stats.total_requests,
                successful_requests=stats.successful_requests,
                failed_requests=stats.failed_requests,
                timeouts=stats.timeouts,
                total_latency=stats.total_latency,
                min_latency=stats.min_latency if stats.min_latency != float('inf') else 0,
                max_latency=stats.max_latency,
                p50_latency=stats.p50_latency,
                p95_latency=stats.p95_latency,
                p99_latency=stats.p99_latency,
                last_request_time=stats.last_request_time
            )

    def get_all_stats(self) -> dict[str, EndpointStats]:
        """Get statistics for all endpoints."""
        with self._lock:
            return {
                endpoint: EndpointStats(
                    total_requests=stats.total_requests,
                    successful_requests=stats.successful_requests,
                    failed_requests=stats.failed_requests,
                    timeouts=stats.timeouts,
                    total_latency=stats.total_latency,
                    min_latency=stats.min_latency if stats.min_latency != float('inf') else 0,
                    max_latency=stats.max_latency,
                    p50_latency=stats.p50_latency,
                    p95_latency=stats.p95_latency,
                    p99_latency=stats.p99_latency,
                    last_request_time=stats.last_request_time
                )
                for endpoint, stats in self._stats.items()
            }

    def reset_endpoint(self, endpoint: str) -> None:
        """Reset timeout and stats for an endpoint."""
        with self._lock:
            if endpoint in self._timeouts:
                self._timeouts[endpoint] = self._config.initial_timeout
            if endpoint in self._stats:
                del self._stats[endpoint]

    def reset_all(self) -> None:
        """Reset all endpoints to initial state."""
        with self._lock:
            self._timeouts.clear()
            self._stats.clear()

    def set_timeout(self, endpoint: str, timeout: float) -> None:
        """
        Manually set timeout for an endpoint.

        Args:
            endpoint: Endpoint identifier
            timeout: Timeout value in seconds
        """
        with self._lock:
            bounded_timeout = max(self._config.min_timeout, min(self._config.max_timeout, timeout))
            self._timeouts[endpoint] = bounded_timeout


def calculate_timeout_from_percentile(
    p50: float,
    p95: float,
    p99: float,
    target_success_rate: float = 0.99
) -> float:
    """
    Calculate optimal timeout based on latency percentiles.

    Args:
        p50: 50th percentile latency
        p95: 95th percentile latency
        p99: 99th percentile latency
        target_success_rate: Desired success rate

    Returns:
        Recommended timeout value
    """
    if target_success_rate >= 0.99:
        return max(p99 * 1.1, p50 * 2)
    elif target_success_rate >= 0.95:
        return max(p95 * 1.1, p50 * 1.5)
    else:
        return max(p50 * 1.2, p95)

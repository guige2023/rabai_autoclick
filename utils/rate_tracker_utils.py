"""
Rate tracker utilities for monitoring request rates.

Provides sliding window rate tracking,
alerting on rate anomalies, and throughput monitoring.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Literal


@dataclass
class RateSnapshot:
    """Point-in-time rate measurement."""
    timestamp: float
    rate: float
    count: int


@dataclass
class RateAlert:
    """Alert triggered by rate threshold."""
    state: Literal["spike", "drop", "normal"]
    rate: float
    threshold: float
    timestamp: float


class SlidingWindowRateTracker:
    """
    Track rates over a sliding time window.

    Useful for tracking requests per second, errors per minute, etc.
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        bucket_count: int = 60,
    ):
        self.window_seconds = window_seconds
        self.bucket_count = bucket_count
        self._bucket_size = window_seconds / bucket_count
        self._buckets: deque[float] = deque([0.0] * bucket_count)
        self._timestamps: deque[float] = deque()
        self._total = 0
        self._lock = threading.Lock()
        self._last_update = time.time()

    def record(self, count: int = 1) -> None:
        """Record events."""
        now = time.time()
        with self._lock:
            self._prune(now)
            self._total += count
            self._buckets[-1] += count
            self._timestamps.append(now)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            removed = self._timestamps.popleft()
            idx = int((removed - (now - self.window_seconds)) / self._bucket_size)
            if 0 <= idx < len(self._buckets):
                self._buckets[idx] = 0.0

        while len(self._timestamps) > self.bucket_count * 2:
            self._timestamps.popleft()

        while len(self._buckets) < self.bucket_count:
            self._buckets.appendleft(0.0)

    @property
    def current_rate(self) -> float:
        """Current rate (events per second)."""
        with self._lock:
            self._prune(time.time())
            total = sum(self._buckets)
            return total / self.window_seconds

    @property
    def total_count(self) -> int:
        return self._total

    def get_snapshot(self) -> RateSnapshot:
        """Get current rate snapshot."""
        rate = self.current_rate
        return RateSnapshot(
            timestamp=time.time(),
            rate=rate,
            count=self._total,
        )


class RateAnomalyDetector:
    """
    Detect rate anomalies (spikes or drops).

    Uses standard deviation from rolling average.
    """

    def __init__(
        self,
        threshold_std: float = 2.0,
        window_samples: int = 100,
    ):
        self.threshold_std = threshold_std
        self.window_samples = window_samples
        self._rates: deque[float] = deque(maxlen=window_samples)
        self._alerts: deque[RateAlert] = deque(maxlen=100)
        self._lock = threading.Lock()

    def record(self, rate: float) -> RateAlert | None:
        """Record rate and check for anomalies."""
        with self._lock:
            self._rates.append(rate)
            if len(self._rates) < 10:
                return None

            import statistics
            mean = statistics.mean(self._rates)
            stdev = statistics.stdev(self._rates)

            if stdev < 0.001:
                return None

            z_score = abs(rate - mean) / stdev
            now = time.time()

            if z_score > self.threshold_std:
                if rate > mean:
                    alert = RateAlert("spike", rate, mean + z_score * stdev, now)
                else:
                    alert = RateAlert("drop", rate, mean - z_score * stdev, now)
                self._alerts.append(alert)
                return alert

            if self._alerts and self._alerts[-1].state != "normal":
                normal_alert = RateAlert("normal", rate, mean, now)
                self._alerts.append(normal_alert)
                return normal_alert

            return None

    def get_recent_alerts(self, count: int = 10) -> list[RateAlert]:
        """Get recent alerts."""
        with self._lock:
            return list(self._alerts)[-count:]


class ThroughputMonitor:
    """
    Monitor throughput with bytes/requests tracking.

    Provides peak, average, and current throughput metrics.
    """

    def __init__(self, interval_seconds: float = 1.0):
        self.interval = interval_seconds
        self._lock = threading.Lock()
        self._current_throughput = 0.0
        self._peak_throughput = 0.0
        self._total_bytes = 0
        self._total_requests = 0
        self._start_time = time.time()
        self._last_update = time.time()
        self._last_bytes = 0
        self._last_requests = 0

    def record(self, bytes_count: int = 0, requests: int = 1) -> None:
        """Record throughput data."""
        with self._lock:
            now = time.time()
            elapsed = now - self._last_update

            if elapsed >= self.interval:
                self._current_throughput = (bytes_count - self._last_bytes) / elapsed
                self._peak_throughput = max(self._peak_throughput, self._current_throughput)
                self._last_update = now
                self._last_bytes = bytes_count

            self._total_bytes += bytes_count
            self._total_requests += requests

    @property
    def current_mbps(self) -> float:
        """Current throughput in MB/s."""
        return self._current_throughput / (1024 * 1024)

    @property
    def peak_mbps(self) -> float:
        """Peak throughput in MB/s."""
        return self._peak_throughput / (1024 * 1024)

    @property
    def total_mb(self) -> float:
        """Total data in MB."""
        return self._total_bytes / (1024 * 1024)

    @property
    def total_requests(self) -> int:
        return self._total_requests

    def get_stats(self) -> dict:
        """Get all statistics."""
        return {
            "current_mbps": self.current_mbps,
            "peak_mbps": self.peak_mbps,
            "total_mb": self.total_mb,
            "total_requests": self.total_requests,
            "uptime_seconds": time.time() - self._start_time,
        }

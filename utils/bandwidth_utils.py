"""
Bandwidth measurement and monitoring utilities.

Provides bandwidth testing, speed estimation,
and network throughput monitoring.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable


@dataclass
class BandwidthResult:
    """Result of a bandwidth measurement."""
    bytes_transferred: int
    duration_seconds: float
    bits_per_second: float
    megabytes_per_second: float


def measure_bandwidth(
    transfer_func: Callable[[], int],
    timeout: float = 30.0,
) -> BandwidthResult:
    """
    Measure bandwidth of a transfer function.

    Args:
        transfer_func: Function that performs data transfer, returns bytes transferred
        timeout: Maximum time to allow

    Returns:
        Bandwidth measurement result
    """
    start = time.perf_counter()
    bytes_transferred = transfer_func()
    duration = time.perf_counter() - start

    bps = (bytes_transferred * 8) / duration if duration > 0 else 0.0
    mbps = bytes_transferred / (1024 * 1024) / duration if duration > 0 else 0.0

    return BandwidthResult(
        bytes_transferred=bytes_transferred,
        duration_seconds=duration,
        bits_per_second=bps,
        megabytes_per_second=mbps,
    )


def estimate_transfer_time(
    file_size_bytes: int,
    bandwidth_mbps: float,
) -> float:
    """
    Estimate time to transfer a file.

    Args:
        file_size_bytes: File size in bytes
        bandwidth_mbps: Available bandwidth in Mbps

    Returns:
        Estimated time in seconds
    """
    if bandwidth_mbps <= 0:
        return float("inf")
    megabits = (file_size_bytes * 8) / 1_000_000
    return megabits / bandwidth_mbps


def format_bandwidth(bps: float) -> str:
    """
    Human-readable bandwidth string.

    Args:
        bps: Bits per second

    Returns:
        Formatted string (e.g. "100.5 Mbps")
    """
    units = [
        (1_000_000_000_000, "Tbps"),
        (1_000_000_000, "Gbps"),
        (1_000_000, "Mbps"),
        (1_000, "Kbps"),
    ]
    for threshold, unit in units:
        if bps >= threshold:
            return f"{bps / threshold:.2f} {unit}"
    return f"{bps:.2f} bps"


def parse_bandwidth_string(bw_str: str) -> float:
    """
    Parse bandwidth string to bits per second.

    Args:
        bw_str: Bandwidth string like "100 Mbps"

    Returns:
        Bits per second
    """
    bw_str = bw_str.strip().lower()
    multipliers = {
        "tbps": 1e12,
        "gbps": 1e9,
        "mbps": 1e6,
        "kbps": 1e3,
        "bps": 1.0,
    }
    for suffix, mult in multipliers.items():
        if suffix in bw_str:
            number = float(bw_str.replace(suffix, "").strip())
            return number * mult
    return float(bw_str)


@dataclass
class ThroughputSample:
    """Single throughput sample."""
    timestamp: float
    bytes: int


class ThroughputMonitor:
    """Monitor throughput over a sliding window."""

    def __init__(self, window_seconds: float = 60.0):
        self.window_seconds = window_seconds
        self._samples: list[ThroughputSample] = []
        self._last_bytes: int = 0

    def add_sample(self, total_bytes: int) -> float:
        """
        Add a sample and return current throughput in B/s.

        Args:
            total_bytes: Total bytes transferred so far

        Returns:
            Current throughput (bytes/sec)
        """
        now = time.time()
        delta = total_bytes - self._last_bytes
        self._last_bytes = total_bytes
        self._samples.append(ThroughputSample(timestamp=now, bytes=delta))
        self._prune_old_samples(now)
        return self.current_throughput

    def _prune_old_samples(self, now: float) -> None:
        cutoff = now - self.window_seconds
        self._samples = [s for s in self._samples if s.timestamp > cutoff]

    @property
    def current_throughput(self) -> float:
        """Current throughput in bytes per second."""
        if not self._samples:
            return 0.0
        total = sum(s.bytes for s in self._samples)
        if len(self._samples) < 2:
            return 0.0
        span = self._samples[-1].timestamp - self._samples[0].timestamp
        if span <= 0:
            return 0.0
        return total / span

    @property
    def peak_throughput(self) -> float:
        """Peak throughput in window."""
        if not self._samples:
            return 0.0
        return max(self._samples, key=lambda s: s.bytes).bytes / self.window_seconds

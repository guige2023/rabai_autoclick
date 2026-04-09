"""
API Canary Deployment Action Module.

Canary deployment support for API services with gradual traffic shifting,
metric tracking, and automatic rollback capabilities.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class CanaryStatus(Enum):
    """Canary deployment status."""
    IDLE = "idle"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class CanaryConfig:
    """Configuration for a canary deployment."""
    name: str
    baseline_version: str
    canary_version: str
    initial_traffic_percent: float = 5.0
    increment_percent: float = 10.0
    increment_interval_seconds: float = 60.0
    max_traffic_percent: float = 100.0
    auto_rollback_threshold: float = 0.05  # 5% error rate increase
    analysis_window_seconds: float = 60.0


@dataclass
class TrafficAllocation:
    """Traffic allocation between baseline and canary."""
    baseline_percent: float
    canary_percent: float


@dataclass
class CanaryMetrics:
    """Metrics collected during canary analysis."""
    baseline_requests: int = 0
    baseline_errors: int = 0
    baseline_latency_p50_ms: float = 0.0
    baseline_latency_p99_ms: float = 0.0
    canary_requests: int = 0
    canary_errors: int = 0
    canary_latency_p50_ms: float = 0.0
    canary_latency_p99_ms: float = 0.0
    error_rate_delta: float = 0.0
    latency_delta_ms: float = 0.0


@dataclass
class CanaryReport:
    """Report from a canary deployment analysis."""
    config_name: str
    status: CanaryStatus
    current_traffic: TrafficAllocation
    metrics: CanaryMetrics
    recommendation: str
    timestamp: float = field(default_factory=time.time)


class CanaryRouter:
    """
    Routes requests to either baseline or canary based on traffic allocation.

    Uses consistent hashing for stable routing within sessions.
    """

    def __init__(self, seed: int = 42) -> None:
        self._seed = seed
        self._request_count = 0

    def route(self, request_id: str, allocation: TrafficAllocation) -> str:
        """Determine which version to route a request to."""
        import hashlib
        hash_val = int(hashlib.md5(f"{request_id}:{self._seed}".encode()).hexdigest(), 16)
        bucket = (hash_val % 1000) / 10.0  # 0.0 to 99.9

        if bucket < allocation.canary_percent:
            return "canary"
        return "baseline"


class CanaryAnalyzer:
    """
    Analyzes metrics to determine if a canary is healthy.

    Compares baseline and canary metrics to detect regressions.
    """

    def __init__(
        self,
        error_threshold: float = 0.05,
        latency_threshold_percent: float = 10.0,
    ) -> None:
        self.error_threshold = error_threshold
        self.latency_threshold_percent = latency_threshold_percent

    def analyze(self, metrics: CanaryMetrics) -> CanaryStatus:
        """Analyze canary metrics and return recommendation."""
        # Check error rate
        if metrics.error_rate_delta > self.error_threshold:
            return CanaryStatus.FAILED

        # Check latency regression
        if metrics.baseline_latency_p99_ms > 0:
            latency_increase = (
                (metrics.canary_latency_p99_ms - metrics.baseline_latency_p99_ms)
                / metrics.baseline_latency_p99_ms
            )
            if latency_increase > (self.latency_threshold_percent / 100.0):
                return CanaryStatus.FAILED

        return CanaryStatus.PASSED

    def get_recommendation(self, status: CanaryStatus, metrics: CanaryMetrics) -> str:
        """Get human-readable recommendation."""
        if status == CanaryStatus.PASSED:
            if metrics.error_rate_delta > 0:
                return f"Continue: error rate delta {metrics.error_rate_delta:.3%} within threshold"
            return "Continue: canary is healthy"

        if status == CanaryStatus.FAILED:
            reasons = []
            if metrics.error_rate_delta > self.error_threshold:
                reasons.append(f"error rate increase of {metrics.error_rate_delta:.3%}")
            return f"Rollback: {'; '.join(reasons) if reasons else 'latency regression detected'}"

        return "Monitor: insufficient data for decision"


class APICanaryAction:
    """
    Canary deployment orchestrator for API services.

    Manages traffic shifting, metric collection, analysis,
    and automatic rollback for canary deployments.

    Example:
        canary = APICanaryAction(config=CanaryConfig(
            name="v2-rollout",
            baseline_version="v1.0.0",
            canary_version="v2.0.0",
        ))

        canary.start()
        report = await canary.analyze()
        canary.promote_or_rollback(report)
    """

    def __init__(self, config: CanaryConfig) -> None:
        self.config = config
        self.router = CanaryRouter()
        self.analyzer = CanaryAnalyzer(
            error_threshold=config.auto_rollback_threshold,
        )
        self._status = CanaryStatus.IDLE
        self._current_traffic: Optional[TrafficAllocation] = None
        self._metrics_history: List[CanaryMetrics] = []
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def status(self) -> CanaryStatus:
        """Get current canary status."""
        return self._status

    def start(self) -> None:
        """Start the canary deployment with initial traffic allocation."""
        if self._running:
            logger.warning("Canary deployment already running")
            return

        self._running = True
        self._current_traffic = TrafficAllocation(
            baseline_percent=100.0 - self.config.initial_traffic_percent,
            canary_percent=self.config.initial_traffic_percent,
        )
        self._status = CanaryStatus.RUNNING
        logger.info(
            f"Started canary deployment '{self.config.name}': "
            f"baseline={self.config.baseline_version} -> canary={self.config.canary_version} "
            f"(initial traffic: {self.config.initial_traffic_percent}%)"
        )

    async def stop(self) -> None:
        """Stop the canary deployment."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"Stopped canary deployment '{self.config.name}'")

    def record_request(
        self,
        request_id: str,
        version: str,
        latency_ms: float,
        is_error: bool,
    ) -> None:
        """Record a request metric."""
        # In a real implementation this would update internal metrics
        pass

    def allocate_traffic(self, request_id: str) -> str:
        """Get the version to route a request to."""
        if not self._current_traffic:
            return "baseline"
        return self.router.route(request_id, self._current_traffic)

    async def analyze(self) -> CanaryReport:
        """Analyze current canary metrics and return a report."""
        if not self._current_traffic:
            raise RuntimeError("Canary not started")

        # Gather metrics (in real impl, this would query metrics store)
        metrics = CanaryMetrics(
            baseline_requests=1000,
            baseline_errors=10,
            baseline_latency_p50_ms=50.0,
            baseline_latency_p99_ms=200.0,
            canary_requests=200,
            canary_errors=2,
            canary_latency_p50_ms=55.0,
            canary_latency_p99_ms=210.0,
        )

        # Calculate deltas
        baseline_error_rate = (
            metrics.baseline_errors / metrics.baseline_requests
            if metrics.baseline_requests > 0 else 0
        )
        canary_error_rate = (
            metrics.canary_errors / metrics.canary_requests
            if metrics.canary_requests > 0 else 0
        )
        metrics.error_rate_delta = canary_error_rate - baseline_error_rate
        metrics.latency_delta_ms = (
            metrics.canary_latency_p99_ms - metrics.baseline_latency_p99_ms
        )

        self._metrics_history.append(metrics)

        # Analyze
        self._status = self.analyzer.analyze(metrics)
        recommendation = self.analyzer.get_recommendation(self._status, metrics)

        return CanaryReport(
            config_name=self.config.name,
            status=self._status,
            current_traffic=self._current_traffic,
            metrics=metrics,
            recommendation=recommendation,
        )

    def shift_traffic(self, new_canary_percent: float) -> None:
        """Shift traffic to a new allocation."""
        if not self._current_traffic:
            raise RuntimeError("Canary not started")

        new_percent = min(new_canary_percent, self.config.max_traffic_percent)
        self._current_traffic = TrafficAllocation(
            baseline_percent=100.0 - new_percent,
            canary_percent=new_percent,
        )
        logger.info(f"Traffic shifted: canary={new_percent}%")

    def promote(self) -> None:
        """Promote canary to baseline (full rollout)."""
        self._status = CanaryStatus.PASSED
        self._running = False
        logger.info(
            f"Canary '{self.config.name}' promoted: "
            f"{self.config.canary_version} is now the baseline"
        )

    def rollback(self) -> None:
        """Rollback canary deployment."""
        self._status = CanaryStatus.ROLLED_BACK
        self._running = False
        if self._current_traffic:
            self._current_traffic = TrafficAllocation(baseline_percent=100.0, canary_percent=0.0)
        logger.warning(f"Canary '{self.config.name}' rolled back")

    def promote_or_rollback(self, report: CanaryReport) -> bool:
        """Decide whether to promote or rollback based on analysis report."""
        if report.status == CanaryStatus.PASSED:
            if self._current_traffic and self._current_traffic.canary_percent >= self.config.max_traffic_percent:
                self.promote()
                return True
            # Shift more traffic
            next_percent = min(
                self._current_traffic.canary_percent + self.config.increment_percent,
                self.config.max_traffic_percent,
            )
            self.shift_traffic(next_percent)
            return True

        self.rollback()
        return False

    def get_metrics_history(self) -> List[CanaryMetrics]:
        """Get historical metrics."""
        return self._metrics_history.copy()

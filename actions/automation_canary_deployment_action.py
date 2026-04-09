"""
Automation Canary Deployment Module.

Implements canary deployment strategy for gradual rollouts.
Routes percentage of traffic to new version, monitors metrics,
and auto-promotes or rolls back based on health checks.
"""

from __future__ import annotations

import time
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class CanaryState(Enum):
    """Canary deployment states."""
    IDLE = "idle"
    STARTING = "starting"
    ROUTING = "routing"
    MONITORING = "monitoring"
    PROMOTING = "promoting"
    ROLLING_BACK = "rolling_back"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TrafficRoute:
    """Represents a traffic route configuration."""
    version: str
    weight: int
    healthy: bool = True


@dataclass
class CanaryConfig:
    """Configuration for canary deployment."""
    initial_weight: int = 5
    increment: int = 10
    increment_interval: int = 600
    max_weight: int = 100
    success_threshold: float = 0.99
    error_threshold: float = 0.05
    min_traffic: int = 100
    auto_promote: bool = True
    auto_rollback: bool = True


@dataclass
class CanaryMetrics:
    """Metrics collected during canary deployment."""
    version: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    latency_p50_ms: float = 0.0
    latency_p99_ms: float = 0.0
    error_rate: float = 0.0
    success_rate: float = 0.0


class CanaryDeployer:
    """
    Canary deployment orchestrator.

    Gradually shifts traffic to new version while monitoring
    error rates and latency. Auto-promotes or rolls back based
    on configurable thresholds.

    Example:
        deployer = CanaryDeployer(config)
        deployer.register_version("v1.0.0", weight=100)
        await deployer.start_canary("v2.0.0")
        deployer.on_metrics(lambda m: check_health(m))
    """

    def __init__(self, config: Optional[CanaryConfig] = None) -> None:
        self._config = config or CanaryConfig()
        self._routes: dict[str, TrafficRoute] = {}
        self._state = CanaryState.IDLE
        self._current_version: str = "v1.0.0"
        self._canary_version: Optional[str] = None
        self._canary_metrics = CanaryMetrics(version="")
        self._metrics_callback: Optional[Callable[[CanaryMetrics], bool]] = None
        self._promote_callback: Optional[Callable[[str], Any]] = None
        self._rollback_callback: Optional[Callable[[str], Any]] = None
        self._last_increment: float = 0

    def register_version(
        self,
        version: str,
        weight: int = 100,
        healthy: bool = True
    ) -> None:
        """Register a version with its traffic weight."""
        self._routes[version] = TrafficRoute(
            version=version,
            weight=weight,
            healthy=healthy
        )
        if weight > 0 and self._state == CanaryState.IDLE:
            self._current_version = version

    def set_metrics_callback(
        self,
        callback: Callable[[CanaryMetrics], bool]
    ) -> None:
        """Set callback for health evaluation. Returns True if healthy."""
        self._metrics_callback = callback

    def set_promote_callback(
        self,
        callback: Callable[[str], Any]
    ) -> None:
        """Set callback when version is promoted."""
        self._promote_callback = callback

    def set_rollback_callback(
        self,
        callback: Callable[[str], Any]
    ) -> None:
        """Set callback when version is rolled back."""
        self._rollback_callback = callback

    async def start_canary(self, version: str) -> bool:
        """
        Start canary deployment for a new version.

        Args:
            version: New version to deploy

        Returns:
            True if canary started successfully
        """
        if self._state != CanaryState.IDLE:
            return False

        if version not in self._routes:
            self.register_version(version, weight=0)

        self._canary_version = version
        self._canary_metrics = CanaryMetrics(version=version)
        self._routes[version].weight = self._config.initial_weight
        self._routes[self._current_version].weight -= self._config.initial_weight
        self._state = CanaryState.STARTING
        self._last_increment = time.time()

        self._state = CanaryState.ROUTING
        return True

    async def record_request(
        self,
        version: str,
        success: bool,
        latency_ms: float
    ) -> None:
        """Record a request for metrics tracking."""
        if version == self._canary_version:
            metrics = self._canary_metrics
            metrics.total_requests += 1
            if success:
                metrics.successful_requests += 1
            else:
                metrics.failed_requests += 1

            self._update_latency_stats(latency_ms)

    def _update_latency_stats(self, latency_ms: float) -> None:
        """Update latency statistics."""
        metrics = self._canary_metrics
        if metrics.total_requests == 1:
            metrics.latency_p50_ms = latency_ms
            metrics.latency_p99_ms = latency_ms
        else:
            alpha = 0.1
            metrics.latency_p50_ms = latency_ms * alpha + metrics.latency_p50_ms * (1 - alpha)
            metrics.latency_p99_ms = max(metrics.latency_p99_ms, latency_ms)

    async def evaluate_canary(self) -> CanaryState:
        """
        Evaluate canary health and potentially adjust traffic.

        Returns:
            Current canary state
        """
        if self._state not in (CanaryState.ROUTING, CanaryState.MONITORING):
            return self._state

        if self._metrics_callback:
            is_healthy = await self._metrics_callback(self._canary_metrics)
        else:
            is_healthy = self._evaluate_default_health()

        if not is_healthy and self._config.auto_rollback:
            await self._rollback()
            return self._state

        self._state = CanaryState.MONITORING

        elapsed = time.time() - self._last_increment
        if elapsed >= self._config.increment_interval:
            await self._increment_traffic()

        return self._state

    def _evaluate_default_health(self) -> bool:
        """Default health evaluation based on thresholds."""
        metrics = self._canary_metrics

        if metrics.total_requests < self._config.min_traffic:
            return True

        metrics.error_rate = metrics.failed_requests / metrics.total_requests
        metrics.success_rate = metrics.successful_requests / metrics.total_requests

        return metrics.error_rate < self._config.error_threshold

    async def _increment_traffic(self) -> None:
        """Increment canary traffic weight."""
        if not self._canary_version:
            return

        current_weight = self._routes[self._canary_version].weight
        new_weight = min(current_weight + self._config.increment, self._config.max_weight)

        self._routes[self._canary_version].weight = new_weight
        self._routes[self._current_version].weight -= (new_weight - current_weight)

        self._last_increment = time.time()

        if new_weight >= 100 and self._config.auto_promote:
            await self._promote()

    async def _promote(self) -> None:
        """Promote canary to full deployment."""
        self._state = CanaryState.PROMOTING

        if self._promote_callback and self._canary_version:
            await self._promote_callback(self._canary_version)

        self._routes[self._current_version].weight = 0
        self._current_version = self._canary_version
        self._state = CanaryState.COMPLETED

    async def _rollback(self) -> None:
        """Rollback canary deployment."""
        self._state = CanaryState.ROLLING_BACK

        if self._rollback_callback and self._canary_version:
            await self._rollback_callback(self._canary_version)

        if self._canary_version:
            self._routes[self._canary_version].weight = 0
            self._routes[self._current_version].weight = 100

        self._canary_version = None
        self._state = CanaryState.FAILED

    async def manual_rollback(self) -> bool:
        """Manually trigger rollback."""
        if self._state in (CanaryState.IDLE, CanaryState.COMPLETED):
            return False
        await self._rollback()
        return True

    async def manual_promote(self) -> bool:
        """Manually promote canary."""
        if self._state not in (CanaryState.ROUTING, CanaryState.MONITORING):
            return False
        await self._promote()
        return True

    def route_request(self) -> str:
        """
        Route a request to appropriate version based on weights.

        Returns:
            Version name that should handle the request
        """
        versions = [(v, r.weight) for v, r in self._routes.items() if r.weight > 0]
        if not versions:
            return self._current_version

        total_weight = sum(w for _, w in versions)
        rand = random.randint(1, total_weight)

        cumulative = 0
        for version, weight in versions:
            cumulative += weight
            if rand <= cumulative:
                return version

        return versions[0][0]

    def get_status(self) -> dict[str, Any]:
        """Get current canary deployment status."""
        return {
            "state": self._state.value,
            "current_version": self._current_version,
            "canary_version": self._canary_version,
            "routes": {v: r.weight for v, r in self._routes.items()},
            "metrics": {
                "total_requests": self._canary_metrics.total_requests,
                "successful_requests": self._canary_metrics.successful_requests,
                "failed_requests": self._canary_metrics.failed_requests,
                "error_rate": self._canary_metrics.error_rate,
                "success_rate": self._canary_metrics.success_rate,
                "latency_p50_ms": self._canary_metrics.latency_p50_ms,
                "latency_p99_ms": self._canary_metrics.latency_p99_ms
            } if self._canary_metrics.version else None,
            "last_increment": self._last_increment,
            "next_increment_in": max(0, self._config.increment_interval - (time.time() - self._last_increment))
        }

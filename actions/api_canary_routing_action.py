"""
API Canary Routing Action Module.

Provides canary release capabilities for gradual traffic
shifting and A/B testing with automatic rollback support.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class CanaryStatus(Enum):
    """Canary deployment status."""

    INACTIVE = "inactive"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    PROMOTED = "promoted"
    ROLLED_BACK = "rolled_back"


@dataclass
class CanaryRoute:
    """Represents a canary route configuration."""

    name: str
    primary_url: str
    canary_url: str
    traffic_percentage: float = 0.0
    max_rps: int = 0
    current_rps: int = 0


@dataclass
class CanaryMetrics:
    """Metrics for canary deployment."""

    total_requests: int = 0
    primary_requests: int = 0
    canary_requests: int = 0
    primary_errors: int = 0
    canary_errors: int = 0
    primary_latency_ms: float = 0.0
    canary_latency_ms: float = 0.0
    error_rate_primary: float = 0.0
    error_rate_canary: float = 0.0


class APICanaryRoutingAction:
    """
    Manages canary routing for API traffic.

    Supports:
    - Gradual traffic shifting (0-100%)
    - Automatic metric collection and comparison
    - Configurable rollback thresholds
    - A/B testing with sticky sessions

    Example:
        canary = APICanaryRoutingAction()
        canary.create_route("v1", "http://prod", "http://canary")
        await canary.set_traffic_percentage(10)
        await canary.promote()
    """

    def __init__(
        self,
        error_rate_threshold: float = 5.0,
        latency_threshold_ms: float = 100.0,
    ) -> None:
        """
        Initialize canary routing.

        Args:
            error_rate_threshold: Max error rate % before rollback.
            latency_threshold_ms: Max latency difference before rollback.
        """
        self.error_rate_threshold = error_rate_threshold
        self.latency_threshold_ms = latency_threshold_ms
        self._routes: dict[str, CanaryRoute] = {}
        self._metrics: dict[str, CanaryMetrics] = {}
        self._sticky_sessions: dict[str, str] = {}
        self._status: dict[str, CanaryStatus] = {}

    def create_route(
        self,
        name: str,
        primary_url: str,
        canary_url: str,
    ) -> CanaryRoute:
        """
        Create a new canary route.

        Args:
            name: Route name.
            primary_url: Primary service URL.
            canary_url: Canary service URL.

        Returns:
            Created CanaryRoute.
        """
        route = CanaryRoute(
            name=name,
            primary_url=primary_url,
            canary_url=canary_url,
        )
        self._routes[name] = route
        self._metrics[name] = CanaryMetrics()
        self._status[name] = CanaryStatus.INACTIVE
        logger.info(f"Created canary route: {name}")
        return route

    async def set_traffic_percentage(self, name: str, percentage: float) -> bool:
        """
        Set the percentage of traffic to route to canary.

        Args:
            name: Route name.
            percentage: Traffic % for canary (0-100).

        Returns:
            True if successful.
        """
        if name not in self._routes:
            logger.error(f"Route not found: {name}")
            return False

        route = self._routes[name]
        route.traffic_percentage = max(0.0, min(100.0, percentage))

        if self._status[name] == CanaryStatus.INACTIVE and percentage > 0:
            self._status[name] = CanaryStatus.INITIALIZING
        elif percentage > 0:
            self._status[name] = CanaryStatus.RUNNING
        elif percentage == 0:
            self._status[name] = CanaryStatus.PAUSED

        logger.info(f"Traffic percentage set: {name} -> {percentage}%")
        return True

    async def route_request(
        self,
        name: str,
        request_id: str,
        path: str,
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """
        Route a request to either primary or canary.

        Args:
            name: Route name.
            request_id: Unique request identifier.
            path: Request path.
            headers: Optional request headers.

        Returns:
            Response data with routing info.
        """
        if name not in self._routes:
            return {"error": "Route not found"}

        route = self._routes[name]
        metrics = self._metrics[name]

        if route.traffic_percentage == 0:
            target = "primary"
            url = route.primary_url
        elif route.traffic_percentage >= 100:
            target = "canary"
            url = route.canary_url
        else:
            if request_id in self._sticky_sessions:
                target = self._sticky_sessions[request_id]
            else:
                import random
                target = "canary" if random.random() * 100 < route.traffic_percentage else "primary"
                self._sticky_sessions[request_id] = target

            url = route.canary_url if target == "canary" else route.primary_url

        metrics.total_requests += 1
        if target == "primary":
            metrics.primary_requests += 1
        else:
            metrics.canary_requests += 1

        return {
            "request_id": request_id,
            "target": target,
            "url": url,
            "path": path,
            "canary_percentage": route.traffic_percentage,
        }

    async def record_success(self, name: str, target: str, latency_ms: float) -> None:
        """
        Record a successful request.

        Args:
            name: Route name.
            target: Which endpoint received the request.
            latency_ms: Request latency in milliseconds.
        """
        if name not in self._metrics:
            return

        metrics = self._metrics[name]
        if target == "primary":
            total = metrics.primary_requests
            metrics.primary_latency_ms = (
                (metrics.primary_latency_ms * (total - 1) + latency_ms) / total
            )
        else:
            total = metrics.canary_requests
            metrics.canary_latency_ms = (
                (metrics.canary_latency_ms * (total - 1) + latency_ms) / total
            )

    async def record_error(self, name: str, target: str) -> None:
        """
        Record a failed request.

        Args:
            name: Route name.
            target: Which endpoint received the request.
        """
        if name not in self._metrics:
            return

        metrics = self._metrics[name]
        if target == "primary":
            metrics.primary_errors += 1
            if metrics.primary_requests > 0:
                metrics.error_rate_primary = (
                    metrics.primary_errors / metrics.primary_requests * 100
                )
        else:
            metrics.canary_errors += 1
            if metrics.canary_requests > 0:
                metrics.error_rate_canary = (
                    metrics.canary_errors / metrics.canary_requests * 100
                )

    async def check_rollback(self, name: str) -> bool:
        """
        Check if canary should be rolled back based on metrics.

        Args:
            name: Route name.

        Returns:
            True if rollback is recommended.
        """
        if name not in self._metrics:
            return False

        metrics = self._metrics[name]

        if metrics.canary_requests < 10:
            return False

        if metrics.error_rate_canary > self.error_rate_threshold:
            logger.warning(f"Canary error rate too high: {metrics.error_rate_canary:.2f}%")
            return True

        latency_diff = abs(metrics.canary_latency_ms - metrics.primary_latency_ms)
        if latency_diff > self.latency_threshold_ms:
            logger.warning(f"Canary latency diff too high: {latency_diff:.2f}ms")
            return True

        return False

    async def promote(self, name: str) -> bool:
        """
        Promote canary to primary.

        Args:
            name: Route name.

        Returns:
            True if promotion was successful.
        """
        if name not in self._routes:
            return False

        route = self._routes[name]
        route.primary_url = route.canary_url
        self._status[name] = CanaryStatus.PROMOTED
        logger.info(f"Canary promoted: {name}")
        return True

    async def rollback(self, name: str) -> bool:
        """
        Rollback canary deployment.

        Args:
            name: Route name.

        Returns:
            True if rollback was successful.
        """
        if name not in self._routes:
            return False

        route = self._routes[name]
        route.traffic_percentage = 0.0
        self._status[name] = CanaryStatus.ROLLED_BACK
        logger.info(f"Canary rolled back: {name}")
        return True

    def get_metrics(self, name: str) -> Optional[dict[str, Any]]:
        """
        Get canary metrics.

        Args:
            name: Route name.

        Returns:
            Metrics dictionary or None if not found.
        """
        if name not in self._metrics:
            return None

        metrics = self._metrics[name]
        return {
            "total_requests": metrics.total_requests,
            "primary_requests": metrics.primary_requests,
            "canary_requests": metrics.canary_requests,
            "error_rate_primary": f"{metrics.error_rate_primary:.2f}%",
            "error_rate_canary": f"{metrics.error_rate_canary:.2f}%",
            "latency_primary_ms": f"{metrics.primary_latency_ms:.2f}",
            "latency_canary_ms": f"{metrics.canary_latency_ms:.2f}",
            "status": self._status[name].value,
        }

"""API Degradation Action Module.

Provides API service degradation strategies with graceful
fallback capabilities and health-based routing.

Author: RabAi Team
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional
from enum import Enum

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HealthStatus(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class DegradationLevel(Enum):
    """Degradation severity levels."""
    NONE = 0
    LIGHT = 1
    MODERATE = 2
    SEVERE = 3
    FULL = 4


@dataclass
class ServiceEndpoint:
    """Service endpoint configuration."""
    name: str
    url: str
    priority: int = 0
    weight: float = 1.0
    enabled: bool = True
    timeout_ms: int = 5000


@dataclass
class HealthMetrics:
    """Health metrics for a service."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    timeout_requests: int = 0
    avg_latency_ms: float = 0.0
    last_check: float = 0.0
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests

    @property
    def error_rate(self) -> float:
        return 1.0 - self.success_rate


@dataclass
class DegradationRule:
    """Rule for service degradation."""
    condition: str
    level: DegradationLevel
    action: str
    target: str = "*"
    duration_seconds: int = 0


class ServiceHealthMonitor:
    """Monitors service health and computes status."""

    def __init__(
        self,
        success_rate_threshold: float = 0.95,
        latency_threshold_ms: float = 1000.0,
        max_consecutive_failures: int = 5
    ):
        self.success_rate_threshold = success_rate_threshold
        self.latency_threshold_ms = latency_threshold_ms
        self.max_consecutive_failures = max_consecutive_failures
        self._metrics: Dict[str, HealthMetrics] = {}
        self._latency_history: Dict[str, Deque[float]] = {}

    def record_request(
        self,
        service: str,
        success: bool,
        latency_ms: float,
        is_timeout: bool = False
    ) -> None:
        """Record request result."""
        if service not in self._metrics:
            self._metrics[service] = HealthMetrics()
            self._latency_history[service] = deque(maxlen=100)

        metrics = self._metrics[service]

        metrics.total_requests += 1
        metrics.avg_latency_ms = (
            metrics.avg_latency_ms * (metrics.total_requests - 1) + latency_ms
        ) / metrics.total_requests

        if success:
            metrics.successful_requests += 1
            metrics.consecutive_failures = 0
            metrics.consecutive_successes += 1
        else:
            metrics.failed_requests += 1
            metrics.consecutive_failures += 1
            metrics.consecutive_successes = 0

        if is_timeout:
            metrics.timeout_requests += 1

        metrics.last_check = time.time()
        self._latency_history[service].append(latency_ms)

    def get_health_status(self, service: str) -> HealthStatus:
        """Get health status for service."""
        if service not in self._metrics:
            return HealthStatus.UNKNOWN

        metrics = self._metrics[service]

        if metrics.consecutive_failures >= self.max_consecutive_failures:
            return HealthStatus.UNHEALTHY

        if metrics.success_rate < self.success_rate_threshold:
            return HealthStatus.DEGRADED

        if metrics.avg_latency_ms > self.latency_threshold_ms:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def get_degradation_level(self, service: str) -> DegradationLevel:
        """Compute degradation level for service."""
        if service not in self._metrics:
            return DegradationLevel.NONE

        metrics = self._metrics[service]
        status = self.get_health_status(service)

        if status == HealthStatus.UNHEALTHY:
            if metrics.consecutive_failures >= 10:
                return DegradationLevel.FULL
            return DegradationLevel.SEVERE

        if status == HealthStatus.DEGRADED:
            if metrics.success_rate < 0.8:
                return DegradationLevel.MODERATE
            return DegradationLevel.LIGHT

        return DegradationLevel.NONE

    def get_available_services(
        self,
        required_level: DegradationLevel = DegradationLevel.NONE
    ) -> List[str]:
        """Get list of available services at or above degradation level."""
        available = []

        for service in self._metrics:
            level = self.get_degradation_level(service)
            if level.value <= required_level.value:
                available.append(service)

        return available

    def get_metrics(self, service: str) -> Optional[HealthMetrics]:
        """Get metrics for service."""
        return self._metrics.get(service)

    def reset_metrics(self, service: Optional[str] = None) -> None:
        """Reset metrics for service or all services."""
        if service:
            if service in self._metrics:
                del self._metrics[service]
            if service in self._latency_history:
                del self._latency_history[service]
        else:
            self._metrics.clear()
            self._latency_history.clear()


class APIDegradationAction(BaseAction):
    """Action for API degradation operations."""

    def __init__(self):
        super().__init__("api_degradation")
        self._monitor = ServiceHealthMonitor()
        self._endpoints: Dict[str, List[ServiceEndpoint]] = {}
        self._rules: List[DegradationRule] = []
        self._fallback_handlers: Dict[str, Callable] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute degradation action."""
        try:
            operation = params.get("operation", "record")

            if operation == "record":
                return self._record(params)
            elif operation == "health":
                return self._get_health(params)
            elif operation == "register_endpoint":
                return self._register_endpoint(params)
            elif operation == "add_rule":
                return self._add_rule(params)
            elif operation == "get_available":
                return self._get_available(params)
            elif operation == "degradation_level":
                return self._get_degradation_level(params)
            elif operation == "reset":
                return self._reset(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _record(self, params: Dict[str, Any]) -> ActionResult:
        """Record request result."""
        service = params.get("service", "default")
        success = params.get("success", True)
        latency_ms = params.get("latency_ms", 0.0)
        is_timeout = params.get("timeout", False)

        self._monitor.record_request(service, success, latency_ms, is_timeout)

        level = self._monitor.get_degradation_level(service)
        status = self._monitor.get_health_status(service)

        return ActionResult(
            success=True,
            data={
                "service": service,
                "status": status.value,
                "degradation_level": level.value
            }
        )

    def _get_health(self, params: Dict[str, Any]) -> ActionResult:
        """Get service health status."""
        service = params.get("service", "default")

        metrics = self._monitor.get_metrics(service)
        status = self._monitor.get_health_status(service)
        level = self._monitor.get_degradation_level(service)

        data = {
            "service": service,
            "status": status.value,
            "degradation_level": level.value
        }

        if metrics:
            data.update({
                "total_requests": metrics.total_requests,
                "success_rate": metrics.success_rate,
                "error_rate": metrics.error_rate,
                "avg_latency_ms": metrics.avg_latency_ms,
                "consecutive_failures": metrics.consecutive_failures,
                "last_check": metrics.last_check
            })

        return ActionResult(success=True, data=data)

    def _register_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Register service endpoint."""
        service = params.get("service", "default")
        url = params.get("url", "")
        priority = params.get("priority", 0)
        weight = params.get("weight", 1.0)

        endpoint = ServiceEndpoint(
            name=service,
            url=url,
            priority=priority,
            weight=weight
        )

        if service not in self._endpoints:
            self._endpoints[service] = []

        self._endpoints[service].append(endpoint)
        self._endpoints[service].sort(key=lambda e: e.priority, reverse=True)

        return ActionResult(
            success=True,
            message=f"Endpoint registered for: {service}"
        )

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Add degradation rule."""
        rule = DegradationRule(
            condition=params.get("condition", ""),
            level=DegradationLevel(params.get("level", 0)),
            action=params.get("action", ""),
            target=params.get("target", "*"),
            duration_seconds=params.get("duration_seconds", 0)
        )

        self._rules.append(rule)

        return ActionResult(
            success=True,
            message=f"Rule added: {rule.condition} -> {rule.action}"
        )

    def _get_available(self, params: Dict[str, Any]) -> ActionResult:
        """Get available services."""
        level_str = params.get("max_level", "none")

        try:
            level = DegradationLevel(level_str)
        except ValueError:
            level = DegradationLevel.NONE

        available = self._monitor.get_available_services(level)

        return ActionResult(
            success=True,
            data={"available_services": available}
        )

    def _get_degradation_level(self, params: Dict[str, Any]) -> ActionResult:
        """Get degradation level for service."""
        service = params.get("service", "default")
        level = self._monitor.get_degradation_level(service)

        return ActionResult(
            success=True,
            data={
                "service": service,
                "level": level.value,
                "level_name": level.name
            }
        )

    def _reset(self, params: Dict[str, Any]) -> ActionResult:
        """Reset metrics."""
        service = params.get("service")

        self._monitor.reset_metrics(service)

        return ActionResult(
            success=True,
            message=f"Metrics reset for: {service or 'all services'}"
        )

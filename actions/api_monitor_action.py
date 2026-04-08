"""API Monitor Action Module.

Provides API request/response monitoring, metrics collection,
alerting on failures, and latency tracking.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict, deque
import logging

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert level."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class RequestMetrics:
    """Request metrics snapshot."""
    endpoint: str
    method: str
    status_code: int
    latency_ms: float
    timestamp: float
    error: Optional[str] = None
    size_bytes: int = 0


@dataclass
class AlertRule:
    """Alert rule configuration."""
    name: str
    condition: Callable[["APIMonitorAction"], bool]
    level: AlertLevel
    message: str
    cooldown_seconds: float = 60.0


class APIMonitorAction:
    """API request monitor with metrics and alerting.

    Example:
        monitor = APIMonitorAction()

        await monitor.track_request(
            endpoint="/api/users",
            method="GET",
            status_code=200,
            latency_ms=45.0
        )

        stats = monitor.get_stats()
        alerts = monitor.check_alerts()
    """

    def __init__(
        self,
        retention_seconds: float = 3600.0,
        max_requests: int = 10000,
    ) -> None:
        self.retention_seconds = retention_seconds
        self.max_requests = max_requests

        self._requests: deque[RequestMetrics] = deque(maxlen=max_requests)
        self._endpoint_stats: Dict[str, Dict] = defaultdict(lambda: {
            "count": 0,
            "errors": 0,
            "total_latency": 0.0,
            "status_codes": defaultdict(int),
        })
        self._alert_states: Dict[str, float] = {}
        self._alert_rules: List[AlertRule] = []
        self._callbacks: Dict[AlertLevel, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def register_alert_callback(
        self,
        level: AlertLevel,
        callback: Callable[[str, AlertLevel], None],
    ) -> None:
        """Register callback for alert level."""
        self._callbacks[level].append(callback)

    def add_alert_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self._alert_rules.append(rule)

    async def track_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        latency_ms: float,
        error: Optional[str] = None,
        size_bytes: int = 0,
    ) -> None:
        """Track an API request.

        Args:
            endpoint: API endpoint
            method: HTTP method
            status_code: Response status code
            latency_ms: Request latency in milliseconds
            error: Optional error message
            size_bytes: Response size in bytes
        """
        async with self._lock:
            metrics = RequestMetrics(
                endpoint=endpoint,
                method=method,
                status_code=status_code,
                latency_ms=latency_ms,
                timestamp=time.time(),
                error=error,
                size_bytes=size_bytes,
            )

            self._requests.append(metrics)
            self._update_endpoint_stats(metrics)
            await self._cleanup_old_requests()

    def _update_endpoint_stats(self, metrics: RequestMetrics) -> None:
        """Update endpoint statistics."""
        key = f"{metrics.method}:{metrics.endpoint}"
        stats = self._endpoint_stats[key]

        stats["count"] += 1
        stats["total_latency"] += metrics.latency_ms
        stats["status_codes"][metrics.status_code] += 1

        if metrics.status_code >= 400 or metrics.error:
            stats["errors"] += 1

    async def _cleanup_old_requests(self) -> None:
        """Remove requests older than retention period."""
        cutoff = time.time() - self.retention_seconds
        while self._requests and self._requests[0].timestamp < cutoff:
            self._requests.popleft()

    async def check_alerts(self) -> List[Dict[str, Any]]:
        """Check all alert rules and return triggered alerts.

        Returns:
            List of triggered alerts
        """
        triggered: List[Dict[str, Any]] = []
        now = time.time()

        for rule in self._alert_rules:
            if self._is_in_cooldown(rule.name, now):
                continue

            if rule.condition(self):
                alert = {
                    "name": rule.name,
                    "level": rule.level,
                    "message": rule.message,
                    "timestamp": now,
                }
                triggered.append(alert)
                self._alert_states[rule.name] = now

                for callback in self._callbacks.get(rule.level, []):
                    try:
                        callback(rule.name, rule.level)
                    except Exception as e:
                        logger.error(f"Alert callback failed: {e}")

        return triggered

    def _is_in_cooldown(self, rule_name: str, now: float) -> bool:
        """Check if alert is in cooldown period."""
        if rule_name not in self._alert_states:
            return False
        return now - self._alert_states[rule_name] < self._get_rule(rule_name).cooldown_seconds

    def _get_rule(self, rule_name: str) -> AlertRule:
        """Get alert rule by name."""
        return next(r for r in self._alert_rules if r.name == rule_name)

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        total_requests = len(self._requests)
        if total_requests == 0:
            return {"total_requests": 0}

        recent_requests = [
            r for r in self._requests
            if time.time() - r.timestamp < 60
        ]

        errors = sum(1 for r in self._requests if r.status_code >= 400 or r.error)
        avg_latency = sum(r.latency_ms for r in self._requests) / total_requests

        return {
            "total_requests": total_requests,
            "recent_requests_1m": len(recent_requests),
            "error_rate": errors / total_requests if total_requests > 0 else 0,
            "avg_latency_ms": avg_latency,
            "endpoints": {
                key: {
                    "count": stats["count"],
                    "errors": stats["errors"],
                    "avg_latency_ms": stats["total_latency"] / stats["count"] if stats["count"] > 0 else 0,
                    "status_codes": dict(stats["status_codes"]),
                }
                for key, stats in self._endpoint_stats.items()
            },
        }

    def get_endpoint_stats(self, endpoint: str, method: str) -> Dict[str, Any]:
        """Get statistics for specific endpoint."""
        key = f"{method}:{endpoint}"
        stats = self._endpoint_stats.get(key, {})
        count = stats.get("count", 0)
        total_latency = stats.get("total_latency", 0)

        return {
            "endpoint": endpoint,
            "method": method,
            "count": count,
            "errors": stats.get("errors", 0),
            "error_rate": stats.get("errors", 0) / count if count > 0 else 0,
            "avg_latency_ms": total_latency / count if count > 0 else 0,
            "status_codes": dict(stats.get("status_codes", {})),
        }

    def get_recent_failures(self, limit: int = 10) -> List[RequestMetrics]:
        """Get recent failed requests."""
        failures = [
            r for r in self._requests
            if r.status_code >= 400 or r.error
        ]
        return sorted(failures, key=lambda r: r.timestamp, reverse=True)[:limit]

    def get_latency_percentiles(
        self,
        percentiles: List[int] = [50, 90, 95, 99],
    ) -> Dict[int, float]:
        """Get latency percentiles."""
        if not self._requests:
            return {p: 0.0 for p in percentiles}

        latencies = sorted(r.latency_ms for r in self._requests)
        n = len(latencies)

        return {
            p: latencies[int(n * p / 100)] if n > 0 else 0.0
            for p in percentiles
        }

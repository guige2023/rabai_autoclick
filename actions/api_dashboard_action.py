"""API Dashboard Action Module.

Provides real-time API monitoring dashboard with metrics visualization,
health overview, and alert summaries for API operations.
"""

import time
import threading
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DashboardMetricType(Enum):
    """Dashboard metric types."""
    REQUEST_RATE = "request_rate"
    ERROR_RATE = "error_rate"
    LATENCY_P50 = "latency_p50"
    LATENCY_P95 = "latency_p95"
    LATENCY_P99 = "latency_p99"
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    ACTIVE_CONNECTIONS = "active_connections"
    QUEUE_DEPTH = "queue_depth"


@dataclass
class DashboardMetric:
    """Individual dashboard metric."""
    metric_type: DashboardMetricType
    value: float
    unit: str
    timestamp: float
    trend: str  # "up", "down", "stable"
    change_pct: float = 0.0


@dataclass
class DashboardSnapshot:
    """Dashboard snapshot at a point in time."""
    timestamp: float
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    avg_latency_ms: float = 0.0
    error_rate_pct: float = 0.0
    uptime_seconds: float = 0.0
    services: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class ApiDashboardAction(BaseAction):
    """API Monitoring Dashboard.

    Real-time API dashboard providing metrics overview,
    health status, and alerting capabilities.
    """
    action_type = "api_dashboard"
    display_name = "API监控仪表板"
    description = "实时API监控仪表板，指标可视化和告警"

    _dashboard_data: Dict[str, Any] = {}
    _snapshots: List[DashboardSnapshot] = []
    _lock = threading.RLock()
    _start_time: float = field(default_factory=time.time)

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dashboard operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'render', 'snapshot', 'metrics',
                               'health_check', 'alerts', 'export'
                - time_range: str (optional) - '1m', '5m', '15m', '1h'
                - service_name: str (optional) - specific service to query
                - refresh: bool (optional) - force refresh data

        Returns:
            ActionResult with dashboard data.
        """
        start_time = time.time()
        operation = params.get('operation', 'render')

        try:
            with self._lock:
                if operation == 'render':
                    return self._render_dashboard(params, start_time)
                elif operation == 'snapshot':
                    return self._take_snapshot(params, start_time)
                elif operation == 'metrics':
                    return self._get_metrics(params, start_time)
                elif operation == 'health_check':
                    return self._health_check(params, start_time)
                elif operation == 'alerts':
                    return self._get_alerts(params, start_time)
                elif operation == 'export':
                    return self._export_dashboard(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Dashboard error: {str(e)}",
                duration=time.time() - start_time
            )

    def _render_dashboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Render dashboard view."""
        time_range = params.get('time_range', '5m')
        refresh = params.get('refresh', False)

        if time_range == '1m':
            cutoff = time.time() - 60
        elif time_range == '5m':
            cutoff = time.time() - 300
        elif time_range == '15m':
            cutoff = time.time() - 900
        elif time_range == '1h':
            cutoff = time.time() - 3600
        else:
            cutoff = time.time() - 300

        recent_snapshots = [s for s in self._snapshots if s.timestamp >= cutoff]

        total_req = sum(s.total_requests for s in recent_snapshots)
        total_err = sum(s.failed_requests for s in recent_snapshots)
        error_rate = (total_err / total_req * 100) if total_req > 0 else 0.0
        avg_latency = sum(s.avg_latency_ms for s in recent_snapshots) / len(recent_snapshots) if recent_snapshots else 0.0

        uptime = time.time() - self._start_time

        dashboard = {
            'time_range': time_range,
            'refreshed_at': time.time(),
            'summary': {
                'total_requests': total_req,
                'error_rate_pct': round(error_rate, 2),
                'avg_latency_ms': round(avg_latency, 2),
                'uptime_seconds': round(uptime, 1),
            },
            'services': self._get_service_status(),
            'snapshot_count': len(recent_snapshots),
        }

        return ActionResult(
            success=True,
            message="Dashboard rendered",
            data=dashboard,
            duration=time.time() - start_time
        )

    def _take_snapshot(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Take a dashboard snapshot."""
        snapshot = DashboardSnapshot(
            timestamp=time.time(),
            total_requests=params.get('total_requests', 0),
            successful_requests=params.get('successful_requests', 0),
            failed_requests=params.get('failed_requests', 0),
            avg_latency_ms=params.get('avg_latency_ms', 0.0),
            error_rate_pct=params.get('error_rate_pct', 0.0),
            services=params.get('services', {}),
        )

        self._snapshots.append(snapshot)
        if len(self._snapshots) > 1000:
            self._snapshots = self._snapshots[-500:]

        return ActionResult(
            success=True,
            message="Snapshot taken",
            data={'snapshot_id': len(self._snapshots) - 1, 'timestamp': snapshot.timestamp},
            duration=time.time() - start_time
        )

    def _get_metrics(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get dashboard metrics."""
        service_name = params.get('service_name')
        metrics: List[DashboardMetric] = []

        services = self._get_service_status()
        for svc, status in services.items():
            if service_name and svc != service_name:
                continue

            metrics.extend([
                DashboardMetric(DashboardMetricType.REQUEST_RATE, status.get('requests_per_sec', 0.0), 'req/s', time.time(), 'stable'),
                DashboardMetric(DashboardMetricType.ERROR_RATE, status.get('error_rate', 0.0), '%', time.time(), 'stable'),
                DashboardMetric(DashboardMetricType.LATENCY_P50, status.get('latency_p50', 0.0), 'ms', time.time(), 'stable'),
                DashboardMetric(DashboardMetricType.LATENCY_P95, status.get('latency_p95', 0.0), 'ms', time.time(), 'stable'),
            ])

        return ActionResult(
            success=True,
            message=f"Retrieved {len(metrics)} metrics",
            data={'metrics': [{'type': m.metric_type.value, 'value': m.value, 'unit': m.unit, 'timestamp': m.timestamp} for m in metrics]},
            duration=time.time() - start_time
        )

    def _health_check(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform health check across all services."""
        services = self._get_service_status()
        healthy = sum(1 for s in services.values() if s.get('status') == 'healthy')
        total = len(services)

        return ActionResult(
            success=healthy == total,
            message=f"Health check: {healthy}/{total} services healthy",
            data={'healthy': healthy, 'total': total, 'services': services},
            duration=time.time() - start_time
        )

    def _get_alerts(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get active alerts."""
        services = self._get_service_status()
        alerts = []

        for svc, status in services.items():
            if status.get('error_rate', 0) > 5.0:
                alerts.append({
                    'service': svc,
                    'severity': 'warning',
                    'message': f"High error rate: {status['error_rate']}%",
                    'timestamp': time.time(),
                })
            if status.get('latency_p95', 0) > 1000:
                alerts.append({
                    'service': svc,
                    'severity': 'critical',
                    'message': f"High latency P95: {status['latency_p95']}ms",
                    'timestamp': time.time(),
                })

        return ActionResult(
            success=True,
            message=f"Found {len(alerts)} alerts",
            data={'alerts': alerts, 'count': len(alerts)},
            duration=time.time() - start_time
        )

    def _export_dashboard(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Export dashboard data."""
        export_format = params.get('format', 'json')

        data = {
            'exported_at': time.time(),
            'uptime_seconds': time.time() - self._start_time,
            'snapshots': [
                {'timestamp': s.timestamp, 'total_requests': s.total_requests,
                 'failed_requests': s.failed_requests, 'avg_latency_ms': s.avg_latency_ms}
                for s in self._snapshots[-100:]
            ],
            'services': self._get_service_status(),
        }

        return ActionResult(
            success=True,
            message=f"Dashboard exported as {export_format}",
            data=data,
            duration=time.time() - start_time
        )

    def _get_service_status(self) -> Dict[str, Dict[str, Any]]:
        """Get current service status from stored data."""
        if not self._dashboard_data:
            self._dashboard_data = {
                'api_gateway': {'status': 'healthy', 'requests_per_sec': 125.0, 'error_rate': 0.5, 'latency_p50': 12.0, 'latency_p95': 45.0, 'latency_p99': 120.0},
                'auth_service': {'status': 'healthy', 'requests_per_sec': 80.0, 'error_rate': 0.2, 'latency_p50': 8.0, 'latency_p95': 25.0, 'latency_p99': 60.0},
                'data_service': {'status': 'healthy', 'requests_per_sec': 200.0, 'error_rate': 0.8, 'latency_p50': 18.0, 'latency_p95': 65.0, 'latency_p99': 180.0},
            }
        return self._dashboard_data

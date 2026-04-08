"""
API Monitoring Action Module.

Monitors API health, latency, and availability with alerts,
dashboards, and incident management integration.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from actions.base_action import BaseAction


@dataclass
class HealthStatus:
    """API health status."""
    endpoint: str
    healthy: bool
    latency_ms: float
    last_check: float
    consecutive_failures: int


class APIMonitoringAction(BaseAction):
    """Monitor API health and performance."""

    def __init__(self) -> None:
        super().__init__("api_monitoring")
        self._endpoints: dict[str, HealthStatus] = {}
        self._alert_handlers: list[Callable] = []

    def execute(self, context: dict, params: dict) -> dict:
        """
        Monitor or check API endpoints.

        Args:
            context: Execution context
            params: Parameters:
                - action: register, check, status, alert
                - endpoint: Endpoint URL to monitor
                - check_interval: Seconds between checks
                - timeout: Request timeout
                - healthy_threshold: Consecutive successes to be healthy
                - unhealthy_threshold: Consecutive failures to be unhealthy

        Returns:
            Health status or alert information
        """
        import time
        import urllib.request
        import urllib.error

        action = params.get("action", "check")

        if action == "register":
            endpoint = params.get("endpoint", "")
            self._endpoints[endpoint] = HealthStatus(
                endpoint=endpoint,
                healthy=True,
                latency_ms=0,
                last_check=time.time(),
                consecutive_failures=0
            )
            return {"registered": True, "endpoint": endpoint}

        elif action == "check":
            endpoint = params.get("endpoint", "")
            timeout = params.get("timeout", 5)
            healthy_thresh = params.get("healthy_threshold", 3)
            unhealthy_thresh = params.get("unhealthy_threshold", 3)

            if endpoint not in self._endpoints:
                return {"error": "Endpoint not registered"}

            start = time.time()
            try:
                req = urllib.request.Request(endpoint, method="GET")
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    latency = (time.time() - start) * 1000
                    status = self._endpoints[endpoint]
                    status.latency_ms = latency
                    status.last_check = time.time()
                    if status.consecutive_failures > 0:
                        status.consecutive_failures -= 1
                    if status.consecutive_failures < healthy_thresh:
                        status.healthy = True
                    return {"healthy": status.healthy, "latency_ms": latency}
            except Exception as e:
                status = self._endpoints[endpoint]
                status.last_check = time.time()
                status.consecutive_failures += 1
                if status.consecutive_failures >= unhealthy_thresh:
                    status.healthy = False
                    self._trigger_alerts(status)
                return {"healthy": False, "error": str(e)}

        elif action == "status":
            return {
                "endpoints": [vars(s) for s in self._endpoints.values()],
                "healthy_count": sum(1 for s in self._endpoints.values() if s.healthy),
                "unhealthy_count": sum(1 for s in self._endpoints.values() if not s.healthy)
            }

        elif action == "alert":
            message = params.get("message", "")
            for handler in self._alert_handlers:
                try:
                    handler(message)
                except Exception:
                    pass
            return {"alerts_sent": len(self._alert_handlers)}

        return {"error": f"Unknown action: {action}"}

    def _trigger_alerts(self, status: HealthStatus) -> None:
        """Trigger alerts for unhealthy endpoint."""
        for handler in self._alert_handlers:
            try:
                handler(f"Endpoint {status.endpoint} is unhealthy!")
            except Exception:
                pass

    def register_alert_handler(self, handler: Callable) -> None:
        """Register an alert handler."""
        self._alert_handlers.append(handler)

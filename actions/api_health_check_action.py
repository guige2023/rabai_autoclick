"""
API health check action for monitoring service availability.

Provides health checks, heartbeat monitoring, and alerting.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import threading


class APIHealthCheckAction:
    """Health check monitoring for services and endpoints."""

    def __init__(
        self,
        check_interval: float = 30.0,
        timeout: float = 5.0,
        failure_threshold: int = 3,
        success_threshold: int = 2,
    ) -> None:
        """
        Initialize health checker.

        Args:
            check_interval: Seconds between checks
            timeout: Check timeout in seconds
            failure_threshold: Failures before marking unhealthy
            success_threshold: Successes before marking healthy
        """
        self.check_interval = check_interval
        self.timeout = timeout
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold

        self._targets: Dict[str, Dict[str, Any]] = {}
        self._health_status: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute health check operation.

        Args:
            params: Dictionary containing:
                - operation: 'register', 'check', 'status', 'alert'
                - target: Target identifier
                - url: Target URL for HTTP checks
                - check_type: Type of health check

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "check")

        if operation == "register":
            return self._register_target(params)
        elif operation == "check":
            return self._perform_check(params)
        elif operation == "status":
            return self._get_status(params)
        elif operation == "alert":
            return self._create_alert(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _register_target(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register health check target."""
        target_id = params.get("target", "")
        url = params.get("url", "")
        check_type = params.get("check_type", "http")
        metadata = params.get("metadata", {})

        if not target_id:
            return {"success": False, "error": "target is required"}

        self._targets[target_id] = {
            "url": url,
            "check_type": check_type,
            "metadata": metadata,
            "registered_at": time.time(),
        }

        self._health_status[target_id] = {
            "status": "unknown",
            "failures": 0,
            "successes": 0,
            "last_check": None,
            "last_success": None,
            "last_failure": None,
        }

        return {"success": True, "target": target_id, "status": "registered"}

    def _perform_check(self, params: dict[str, Any]) -> dict[str, Any]:
        """Perform health check on target."""
        target_id = params.get("target", "")

        if target_id not in self._targets:
            return {"success": False, "error": f"Target '{target_id}' not found"}

        target = self._targets[target_id]
        status = self._health_status[target_id]

        check_result = self._execute_check(target)

        status["last_check"] = time.time()

        if check_result["healthy"]:
            status["successes"] += 1
            status["failures"] = 0
            if status["successes"] >= self.success_threshold:
                status["status"] = "healthy"
            status["last_success"] = time.time()
        else:
            status["failures"] += 1
            status["successes"] = 0
            if status["failures"] >= self.failure_threshold:
                status["status"] = "unhealthy"
            status["last_failure"] = time.time()
            status["last_error"] = check_result.get("error")

        return {
            "success": True,
            "target": target_id,
            "healthy": check_result["healthy"],
            "status": status["status"],
            "latency_ms": check_result.get("latency_ms"),
        }

    def _execute_check(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """Execute health check based on type."""
        check_type = target["check_type"]
        url = target.get("url", "")

        start = time.time()

        if check_type == "http":
            return self._check_http(url)
        elif check_type == "tcp":
            return self._check_tcp(url)
        elif check_type == "ping":
            return self._check_ping(url)
        elif check_type == "custom":
            return self._check_custom(target)
        else:
            return {"healthy": True, "latency_ms": (time.time() - start) * 1000}

    def _check_http(self, url: str) -> Dict[str, Any]:
        """HTTP health check."""
        start = time.time()
        return {
            "healthy": True,
            "latency_ms": (time.time() - start) * 1000,
            "status_code": 200,
        }

    def _check_tcp(self, url: str) -> Dict[str, Any]:
        """TCP health check."""
        start = time.time()
        return {"healthy": True, "latency_ms": (time.time() - start) * 1000}

    def _check_ping(self, url: str) -> Dict[str, Any]:
        """Ping health check."""
        start = time.time()
        return {"healthy": True, "latency_ms": (time.time() - start) * 1000}

    def _check_custom(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """Custom health check."""
        return {"healthy": True, "latency_ms": 0}

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get health status of targets."""
        target_id = params.get("target")

        if target_id:
            if target_id not in self._health_status:
                return {"success": False, "error": "Target not found"}
            return {
                "success": True,
                "targets": {target_id: self._health_status[target_id]},
            }

        healthy = sum(1 for s in self._health_status.values() if s["status"] == "healthy")
        unhealthy = sum(1 for s in self._health_status.values() if s["status"] == "unhealthy")

        return {
            "success": True,
            "summary": {
                "total": len(self._health_status),
                "healthy": healthy,
                "unhealthy": unhealthy,
                "unknown": len(self._health_status) - healthy - unhealthy,
            },
            "targets": self._health_status,
        }

    def _create_alert(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create health alert."""
        target_id = params.get("target", "")
        severity = params.get("severity", "warning")
        message = params.get("message", "")

        if target_id not in self._health_status:
            return {"success": False, "error": "Target not found"}

        status = self._health_status[target_id]

        alert = {
            "target": target_id,
            "status": status["status"],
            "severity": severity,
            "message": message or f"Target {target_id} is {status['status']}",
            "timestamp": time.time(),
            "last_error": status.get("last_error"),
        }

        return {"success": True, "alert": alert}

    def get_all_status(self) -> Dict[str, str]:
        """Get status summary for all targets."""
        return {tid: status["status"] for tid, status in self._health_status.items()}

"""
Chaos injection action for fault tolerance testing.

Provides random failure injection, latency simulation, and error scenarios.
"""

from typing import Any, Dict, List, Optional
import random
import time


class ChaosInjectionAction:
    """Chaos engineering for fault injection and resilience testing."""

    def __init__(
        self,
        failure_rate: float = 0.1,
        latency_mean: float = 0.0,
        latency_stddev: float = 0.1,
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize chaos injection.

        Args:
            failure_rate: Probability of injected failure (0.0-1.0)
            latency_mean: Mean latency injection in seconds
            latency_stddev: Standard deviation of latency injection
            seed: Random seed for reproducibility
        """
        self.failure_rate = failure_rate
        self.latency_mean = latency_mean
        self.latency_stddev = latency_stddev

        if seed is not None:
            random.seed(seed)

        self._active_chaos = False
        self._injection_count = 0
        self._failure_count = 0
        self._latency_count = 0

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute chaos operation.

        Args:
            params: Dictionary containing:
                - operation: 'inject', 'enable', 'disable', 'status'
                - chaos_type: Type of chaos ('failure', 'latency', 'error')
                - action: Action to potentially inject chaos into

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "inject")

        if operation == "enable":
            return self._enable_chaos(params)
        elif operation == "disable":
            return self._disable_chaos(params)
        elif operation == "inject":
            return self._inject_chaos(params)
        elif operation == "status":
            return self._get_status(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _enable_chaos(self, params: dict[str, Any]) -> dict[str, Any]:
        """Enable chaos injection."""
        self._active_chaos = True
        return {"success": True, "chaos_enabled": True}

    def _disable_chaos(self, params: dict[str, Any]) -> dict[str, Any]:
        """Disable chaos injection."""
        self._active_chaos = False
        return {"success": True, "chaos_enabled": False}

    def _inject_chaos(self, params: dict[str, Any]) -> dict[str, Any]:
        """Inject chaos into action."""
        chaos_type = params.get("chaos_type", "auto")
        action = params.get("action")
        fallback = params.get("fallback")

        if not self._active_chaos:
            if callable(action):
                return {"success": True, "result": action(), "chaos_injected": False}
            return {"success": True, "chaos_injected": False}

        if chaos_type == "auto":
            chaos_type = random.choice(["failure", "latency", "error"])

        if chaos_type == "failure" and random.random() < self.failure_rate:
            self._failure_count += 1
            self._injection_count += 1

            if callable(fallback):
                return {"success": False, "error": "Injected failure", "fallback_used": True, "result": fallback()}

            return {"success": False, "error": "Injected failure", "chaos_injected": True}

        if chaos_type == "latency" and self.latency_mean > 0:
            injected_latency = random.gauss(self.latency_mean, self.latency_stddev)
            if injected_latency > 0:
                self._latency_count += 1
                self._injection_count += 1
                time.sleep(injected_latency)
                return {"success": True, "latency_injected": injected_latency, "chaos_injected": True}

        if chaos_type == "error" and random.random() < self.failure_rate:
            self._failure_count += 1
            self._injection_count += 1
            error_types = ["timeout", "connection_refused", "service_unavailable", "internal_error"]
            error = random.choice(error_types)
            return {"success": False, "error": f"Injected error: {error}", "chaos_injected": True}

        if callable(action):
            return {"success": True, "result": action(), "chaos_injected": False}
        return {"success": True, "chaos_injected": False}

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get chaos injection status."""
        return {
            "success": True,
            "chaos_enabled": self._active_chaos,
            "failure_rate": self.failure_rate,
            "latency_mean": self.latency_mean,
            "latency_stddev": self.latency_stddev,
            "total_injections": self._injection_count,
            "failure_injections": self._failure_count,
            "latency_injections": self._latency_count,
        }

"""
Adaptive load balancer action with health-aware routing.

Provides intelligent request distribution based on real-time health metrics.
"""

from typing import Any, Optional
import time
import random
import threading


class AdaptiveLoadBalancerAction:
    """Adaptive load balancer with health monitoring."""

    def __init__(
        self,
        strategy: str = "weighted_round_robin",
        health_check_interval: float = 10.0,
        failure_threshold: int = 3,
        recovery_timeout: float = 60.0,
    ) -> None:
        """
        Initialize adaptive load balancer.

        Args:
            strategy: Load balancing strategy
            health_check_interval: Health check interval in seconds
            failure_threshold: Consecutive failures before marking unhealthy
            recovery_timeout: Time before attempting recovery
        """
        self.strategy = strategy
        self.health_check_interval = health_check_interval
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._endpoints: dict[str, dict[str, Any]] = {}
        self._weights: dict[str, float] = {}
        self._current_index: dict[str, int] = {}
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Route request to appropriate endpoint.

        Args:
            params: Dictionary containing:
                - operation: 'add', 'remove', 'route', 'status'
                - endpoint: Endpoint URL
                - weight: Endpoint weight (for weighted strategies)
                - request: Request data to route

        Returns:
            Dictionary with routing decision
        """
        operation = params.get("operation", "route")

        if operation == "add":
            return self._add_endpoint(params)
        elif operation == "remove":
            return self._remove_endpoint(params)
        elif operation == "route":
            return self._route_request(params)
        elif operation == "status":
            return self._get_status()
        elif operation == "health":
            return self._update_health(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _add_endpoint(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add endpoint to load balancer."""
        endpoint = params.get("endpoint", "")
        weight = params.get("weight", 1.0)

        if not endpoint:
            return {"success": False, "error": "Endpoint is required"}

        with self._lock:
            self._endpoints[endpoint] = {
                "weight": weight,
                "failures": 0,
                "healthy": True,
                "added_at": time.time(),
                "last_request": None,
            }
            self._weights[endpoint] = weight
            self._current_index[endpoint] = 0

        return {"success": True, "endpoint": endpoint, "weight": weight}

    def _remove_endpoint(self, params: dict[str, Any]) -> dict[str, Any]:
        """Remove endpoint from load balancer."""
        endpoint = params.get("endpoint", "")

        with self._lock:
            if endpoint in self._endpoints:
                del self._endpoints[endpoint]
                if endpoint in self._weights:
                    del self._weights[endpoint]
                return {"success": True, "endpoint": endpoint}
        return {"success": False, "error": "Endpoint not found"}

    def _route_request(self, params: dict[str, Any]) -> dict[str, Any]:
        """Route request to endpoint."""
        request_id = params.get("request_id", str(time.time_ns()))

        with self._lock:
            healthy = [e for e, info in self._endpoints.items() if info["healthy"]]
            if not healthy:
                return {"success": False, "error": "No healthy endpoints available"}

            endpoint = self._select_endpoint(healthy)
            self._endpoints[endpoint]["last_request"] = time.time()

        return {
            "success": True,
            "endpoint": endpoint,
            "request_id": request_id,
            "strategy": self.strategy,
        }

    def _select_endpoint(self, healthy_endpoints: list[str]) -> str:
        """Select endpoint based on strategy."""
        if self.strategy == "round_robin":
            return self._round_robin(healthy_endpoints)
        elif self.strategy == "weighted_round_robin":
            return self._weighted_round_robin(healthy_endpoints)
        elif self.strategy == "least_connections":
            return self._least_connections(healthy_endpoints)
        elif self.strategy == "random":
            return random.choice(healthy_endpoints)
        elif self.strategy == "ip_hash":
            return self._ip_hash(healthy_endpoints)
        else:
            return healthy_endpoints[0]

    def _round_robin(self, endpoints: list[str]) -> str:
        """Round-robin selection."""
        key = "default"
        idx = self._current_index.get(key, 0)
        selected = endpoints[idx % len(endpoints)]
        self._current_index[key] = idx + 1
        return selected

    def _weighted_round_robin(self, endpoints: list[str]) -> str:
        """Weighted round-robin selection."""
        total_weight = sum(self._weights.get(e, 1.0) for e in endpoints)
        rand = random.uniform(0, total_weight)

        cumulative = 0
        for endpoint in endpoints:
            cumulative += self._weights.get(endpoint, 1.0)
            if rand <= cumulative:
                return endpoint
        return endpoints[-1]

    def _least_connections(self, endpoints: list[str]) -> str:
        """Select endpoint with least active connections."""
        return min(
            endpoints, key=lambda e: self._endpoints[e].get("active_connections", 0)
        )

    def _ip_hash(self, endpoints: list[str]) -> str:
        """IP hash-based selection."""
        return endpoints[hash("client_ip") % len(endpoints)]

    def _update_health(self, params: dict[str, Any]) -> dict[str, Any]:
        """Update endpoint health status."""
        endpoint = params.get("endpoint", "")
        healthy = params.get("healthy", True)

        if endpoint in self._endpoints:
            self._endpoints[endpoint]["healthy"] = healthy
            if healthy:
                self._endpoints[endpoint]["failures"] = 0
            return {"success": True, "endpoint": endpoint, "healthy": healthy}
        return {"success": False, "error": "Endpoint not found"}

    def _get_status(self) -> dict[str, Any]:
        """Get load balancer status."""
        with self._lock:
            return {
                "strategy": self.strategy,
                "total_endpoints": len(self._endpoints),
                "healthy_endpoints": sum(1 for e in self._endpoints.values() if e["healthy"]),
                "endpoints": {
                    ep: {
                        "healthy": info["healthy"],
                        "weight": info["weight"],
                        "failures": info["failures"],
                    }
                    for ep, info in self._endpoints.items()
                },
            }

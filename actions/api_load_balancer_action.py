"""API Load Balancer Action Module.

Provides intelligent load balancing for API requests with support for
multiple algorithms, health checking, circuit breaking, and adaptive
routing based on real-time metrics.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class LoadBalancingAlgorithm(Enum):
    """Load balancing algorithm types."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    WEIGHTED = "weighted"
    RANDOM = "random"
    ADAPTIVE = "adaptive"
    THROTTLE = "throttle"


class NodeState(Enum):
    """API node states."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    DRAINING = "draining"


@dataclass
class APINode:
    """An API endpoint node."""
    node_id: str
    url: str
    weight: int = 1
    max_connections: int = 100
    state: NodeState = NodeState.HEALTHY
    active_connections: int = 0
    last_health_check: Optional[datetime] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    avg_latency_ms: float = 0.0
    total_requests: int = 0
    total_errors: int = 0


@dataclass
class LoadBalancerConfig:
    """Configuration for load balancer."""
    algorithm: LoadBalancingAlgorithm = LoadBalancingAlgorithm.ROUND_ROBIN
    health_check_interval_seconds: int = 30
    health_check_timeout_seconds: float = 5.0
    health_check_path: str = "/health"
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout_seconds: float = 60.0
    adaptive_weights_enabled: bool = True
    weight_update_interval_seconds: int = 60
    drain_timeout_seconds: float = 30.0


@dataclass
class RoutingDecision:
    """Result of a routing decision."""
    node: APINode
    algorithm_used: LoadBalancingAlgorithm
    routing_metadata: Dict[str, Any] = field(default_factory=dict)


class HealthChecker:
    """Health checking for API nodes."""

    def __init__(self, timeout: float = 5.0, check_path: str = "/health"):
        self._timeout = timeout
        self._check_path = check_path

    def check_node(self, node: APINode) -> bool:
        """Perform health check on a node."""
        try:
            import urllib.request
            import urllib.error

            url = node.url.rstrip("/") + self._check_path
            request = urllib.request.Request(url, method="GET")
            response = urllib.request.urlopen(request, timeout=self._timeout)
            return response.status == 200

        except ImportError:
            import http.client
            try:
                from urllib.parse import urlparse
                parsed = urlparse(node.url)
                conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=self._timeout)
                conn.request("GET", self._check_path)
                resp = conn.getresponse()
                conn.close()
                return resp.status == 200
            except Exception:
                return False
        except Exception as e:
            logger.warning(f"Health check failed for {node.node_id}: {e}")
            return False


class CircuitBreaker:
    """Circuit breaker for node failure handling."""

    def __init__(self, threshold: int = 5, timeout: float = 60.0):
        self._threshold = threshold
        self._timeout = timeout
        self._state: Dict[str, str] = defaultdict(lambda: "closed")
        self._failure_count: Dict[str, int] = defaultdict(int)
        self._last_failure_time: Dict[str, datetime] = {}
        self._lock = threading.Lock()

    def record_success(self, node_id: str):
        """Record successful request."""
        with self._lock:
            self._failure_count[node_id] = 0
            self._state[node_id] = "closed"

    def record_failure(self, node_id: str):
        """Record failed request."""
        with self._lock:
            self._failure_count[node_id] += 1
            self._last_failure_time[node_id] = datetime.now()
            if self._failure_count[node_id] >= self._threshold:
                self._state[node_id] = "open"

    def is_open(self, node_id: str) -> bool:
        """Check if circuit is open for node."""
        with self._lock:
            if self._state[node_id] == "open":
                last_failure = self._last_failure_time.get(node_id)
                if last_failure:
                    elapsed = (datetime.now() - last_failure).total_seconds()
                    if elapsed >= self._timeout:
                        self._state[node_id] = "half-open"
                        return False
                return True
            return False

    def get_state(self, node_id: str) -> str:
        """Get circuit state for node."""
        return self._state[node_id]


class AdaptiveWeightCalculator:
    """Calculate adaptive weights based on node performance."""

    @staticmethod
    def calculate_weight(node: APINode, baseline_latency: float = 100.0) -> float:
        """Calculate weight based on node health metrics."""
        error_rate = node.total_errors / max(node.total_requests, 1)
        latency_factor = baseline_latency / max(node.avg_latency_ms, 1)
        health_factor = 1.0 - error_rate

        weight = health_factor * latency_factor * node.weight
        return max(0.1, min(weight, 10.0))


class ApiLoadBalancerAction(BaseAction):
    """Action for load balancing API requests."""

    def __init__(self):
        super().__init__(name="api_load_balancer")
        self._config = LoadBalancerConfig()
        self._nodes: Dict[str, APINode] = {}
        self._circuit_breaker = CircuitBreaker(
            threshold=self._config.circuit_breaker_threshold,
            timeout=self._config.circuit_breaker_timeout
        )
        self._health_checker = HealthChecker(
            timeout=self._config.health_check_timeout_seconds,
            check_path=self._config.health_check_path
        )
        self._lock = threading.Lock()
        self._round_robin_index: Dict[str, int] = defaultdict(int)
        self._request_counters: Dict[str, int] = defaultdict(int)
        self._routing_history: List[RoutingDecision] = []
        self._last_weight_update = datetime.now()

    def configure(self, config: LoadBalancerConfig):
        """Configure load balancer settings."""
        self._config = config
        self._circuit_breaker = CircuitBreaker(
            threshold=config.circuit_breaker_threshold,
            timeout=config.circuit_breaker_timeout_seconds
        )
        self._health_checker = HealthChecker(
            timeout=config.health_check_timeout_seconds,
            check_path=config.health_check_path
        )

    def add_node(self, node_id: str, url: str, weight: int = 1) -> ActionResult:
        """Add a node to the load balancer."""
        try:
            with self._lock:
                if node_id in self._nodes:
                    return ActionResult(success=False, error=f"Node {node_id} already exists")

                node = APINode(node_id=node_id, url=url, weight=weight)
                self._nodes[node_id] = node
                return ActionResult(success=True, data={"node_id": node_id, "url": url})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def remove_node(self, node_id: str, drain: bool = True) -> ActionResult:
        """Remove a node from the load balancer."""
        try:
            with self._lock:
                if node_id not in self._nodes:
                    return ActionResult(success=False, error=f"Node {node_id} not found")

                if drain:
                    self._nodes[node_id].state = NodeState.DRAINING
                else:
                    del self._nodes[node_id]

                return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def route(self, request_context: Optional[Dict[str, Any]] = None) -> RoutingDecision:
        """Route a request to an appropriate node."""
        with self._lock:
            self._update_weights_if_needed()
            eligible_nodes = self._get_eligible_nodes()

            if not eligible_nodes:
                raise RuntimeError("No eligible nodes available")

            if self._config.algorithm == LoadBalancingAlgorithm.ROUND_ROBIN:
                node = self._round_robin_select(eligible_nodes)
            elif self._config.algorithm == LoadBalancingAlgorithm.LEAST_CONNECTIONS:
                node = self._least_connections_select(eligible_nodes)
            elif self._config.algorithm == LoadBalancingAlgorithm.WEIGHTED:
                node = self._weighted_select(eligible_nodes)
            elif self._config.algorithm == LoadBalancingAlgorithm.RANDOM:
                node = self._random_select(eligible_nodes)
            elif self._config.algorithm == LoadBalancingAlgorithm.ADAPTIVE:
                node = self._adaptive_select(eligible_nodes)
            else:
                node = self._round_robin_select(eligible_nodes)

            node.active_connections += 1
            self._request_counters[node.node_id] += 1

            decision = RoutingDecision(
                node=node,
                algorithm_used=self._config.algorithm,
                routing_metadata={
                    "eligible_count": len(eligible_nodes),
                    "request_id": self._request_counters.get(node.node_id, 0)
                }
            )
            self._routing_history.append(decision)
            return decision

    def _get_eligible_nodes(self) -> List[APINode]:
        """Get list of nodes eligible for routing."""
        eligible = []
        for node in self._nodes.values():
            if node.state == NodeState.UNHEALTHY:
                continue
            if self._circuit_breaker.is_open(node.node_id):
                continue
            if node.state == NodeState.DRAINING and node.active_connections > 0:
                continue
            eligible.append(node)
        return eligible

    def _round_robin_select(self, nodes: List[APINode]) -> APINode:
        """Select node using round-robin."""
        node_id = nodes[0].node_id
        current_index = self._round_robin_index[node_id]
        self._round_robin_index[node_id] = (current_index + 1) % len(nodes)
        return nodes[current_index % len(nodes)]

    def _least_connections_select(self, nodes: List[APINode]) -> APINode:
        """Select node with least active connections."""
        return min(nodes, key=lambda n: n.active_connections)

    def _weighted_select(self, nodes: List[APINode]) -> APINode:
        """Select node using weighted probability."""
        total_weight = sum(n.weight for n in nodes)
        import random
        r = random.uniform(0, total_weight)
        cumulative = 0
        for node in nodes:
            cumulative += node.weight
            if r <= cumulative:
                return node
        return nodes[-1]

    def _random_select(self, nodes: List[APINode]) -> APINode:
        """Select node randomly."""
        import random
        return random.choice(nodes)

    def _adaptive_select(self, nodes: List[APINode]) -> APINode:
        """Select node using adaptive weights."""
        weights = [AdaptiveWeightCalculator.calculate_weight(n) for n in nodes]
        total_weight = sum(weights)
        import random
        r = random.uniform(0, total_weight)
        cumulative = 0
        for i, node in enumerate(nodes):
            cumulative += weights[i]
            if r <= cumulative:
                return node
        return nodes[-1]

    def _update_weights_if_needed(self):
        """Update node weights based on performance."""
        if not self._config.adaptive_weights_enabled:
            return

        elapsed = (datetime.now() - self._last_weight_update).total_seconds()
        if elapsed < self._config.weight_update_interval_seconds:
            return

        for node in self._nodes.values():
            new_weight = AdaptiveWeightCalculator.calculate_weight(node)
            node.weight = int(new_weight)

        self._last_weight_update = datetime.now()

    def record_success(self, node_id: str, latency_ms: float):
        """Record successful request completion."""
        with self._lock:
            if node_id in self._nodes:
                node = self._nodes[node_id]
                node.active_connections = max(0, node.active_connections - 1)
                node.consecutive_successes += 1
                node.consecutive_failures = 0
                node.total_requests += 1
                node.avg_latency_ms = (
                    (node.avg_latency_ms * (node.total_requests - 1) + latency_ms)
                    / node.total_requests
                )
                self._circuit_breaker.record_success(node_id)

    def record_failure(self, node_id: str):
        """Record failed request."""
        with self._lock:
            if node_id in self._nodes:
                node = self._nodes[node_id]
                node.active_connections = max(0, node.active_connections - 1)
                node.consecutive_failures += 1
                node.consecutive_successes = 0
                node.total_errors += 1
                self._circuit_breaker.record_failure(node_id)

                if node.consecutive_failures >= self._config.circuit_breaker_threshold:
                    node.state = NodeState.UNHEALTHY

    def perform_health_checks(self) -> ActionResult:
        """Perform health checks on all nodes."""
        try:
            results = {}
            for node_id, node in list(self._nodes.items()):
                is_healthy = self._health_checker.check_node(node)
                node.last_health_check = datetime.now()

                if is_healthy:
                    if node.state == NodeState.UNHEALTHY:
                        node.state = NodeState.HEALTHY
                    results[node_id] = {"status": "healthy", "reachable": True}
                else:
                    results[node_id] = {"status": "unhealthy", "reachable": False}

            return ActionResult(
                success=True,
                data={"checks": results, "timestamp": datetime.now().isoformat()}
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        with self._lock:
            total_requests = sum(n.total_requests for n in self._nodes.values())
            total_errors = sum(n.total_errors for n in self._nodes.values())
            total_connections = sum(n.active_connections for n in self._nodes.values())

            return {
                "total_nodes": len(self._nodes),
                "healthy_nodes": sum(1 for n in self._nodes.values() if n.state == NodeState.HEALTHY),
                "total_requests": total_requests,
                "total_errors": total_errors,
                "active_connections": total_connections,
                "error_rate": total_errors / max(total_requests, 1),
                "algorithm": self._config.algorithm.value,
                "nodes": {
                    node_id: {
                        "state": node.state.value,
                        "active_connections": node.active_connections,
                        "total_requests": node.total_requests,
                        "avg_latency_ms": node.avg_latency_ms,
                        "circuit_state": self._circuit_breaker.get_state(node_id)
                    }
                    for node_id, node in self._nodes.items()
                }
            }

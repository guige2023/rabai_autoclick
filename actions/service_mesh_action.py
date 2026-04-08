"""Service Mesh Action Module.

Provides service mesh capabilities including traffic management,
load balancing, circuit breaking, retries, and service discovery.
"""
from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class LoadBalanceStrategy(Enum):
    """Load balancing strategy."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    WEIGHTED = "weighted"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    CONSISTENT_HASH = "consistent_hash"


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class ServiceEndpoint:
    """Service endpoint (host:port)."""
    id: str
    host: str
    port: int
    weight: int = 100
    healthy: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    connections: int = 0
    failures: int = 0
    last_failure: float = 0.0


@dataclass
class CircuitBreaker:
    """Circuit breaker for service protection."""
    name: str
    state: CircuitState = CircuitState.CLOSED
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    half_open_requests: int = 1
    total_failures: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: float = 0.0


@dataclass
class RoutingResult:
    """Routing decision result."""
    success: bool
    endpoint: Optional[ServiceEndpoint]
    strategy: str
    reason: str
    circuit_state: CircuitState = CircuitState.CLOSED
    error: Optional[str] = None


@dataclass
class MeshStats:
    """Service mesh statistics."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    circuit_open_count: int = 0
    retries: int = 0
    timeouts: int = 0


class ConsistentHash:
    """Consistent hash ring implementation."""

    def __init__(self, nodes: Optional[List[str]] = None, replicas: int = 100):
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self._replicas = replicas

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node: str) -> None:
        """Add node to hash ring."""
        for i in range(self._replicas):
            key = int(hashlib.md5(f"{node}:{i}".encode()).hexdigest(), 16)
            self._ring[key] = node
            self._sorted_keys.append(key)
        self._sorted_keys.sort()

    def remove_node(self, node: str) -> None:
        """Remove node from hash ring."""
        for i in range(self._replicas):
            key = int(hashlib.md5(f"{node}:{i}".encode()).hexdigest(), 16)
            if key in self._ring:
                del self._ring[key]
                self._sorted_keys.remove(key)

    def get_node(self, key: str) -> Optional[str]:
        """Get node for given key using consistent hash."""
        if not self._ring:
            return None
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        idx = 0
        for i, k in enumerate(self._sorted_keys):
            if k >= hash_val:
                idx = i
                break
        else:
            idx = 0
        return self._ring.get(self._sorted_keys[idx])


class ServiceMesh:
    """In-memory service mesh simulator."""

    def __init__(self):
        self._services: Dict[str, Dict[str, ServiceEndpoint]] = {}
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._hash_rings: Dict[str, ConsistentHash] = {}
        self._stats = MeshStats()

    def register_service(self, service_name: str, endpoint: ServiceEndpoint) -> bool:
        """Register service endpoint."""
        if service_name not in self._services:
            self._services[service_name] = {}
        self._services[service_name][endpoint.id] = endpoint
        self._update_hash_ring(service_name)
        return True

    def _update_hash_ring(self, service_name: str) -> None:
        """Update consistent hash ring for service."""
        endpoints = self._services.get(service_name, {})
        nodes = [f"{e.host}:{e.port}" for e in endpoints.values() if e.healthy]
        self._hash_rings[service_name] = ConsistentHash(nodes)

    def get_endpoint(self, service_name: str,
                     strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN,
                     client_ip: Optional[str] = None) -> Optional[ServiceEndpoint]:
        """Get endpoint for service using load balancing strategy."""
        endpoints = self._services.get(service_name, {})
        healthy = [e for e in endpoints.values() if e.healthy]

        if not healthy:
            return None

        if strategy == LoadBalanceStrategy.RANDOM:
            return random.choice(healthy)
        elif strategy == LoadBalanceStrategy.ROUND_ROBIN:
            min_conn = min(e.connections for e in healthy)
            candidates = [e for e in healthy if e.connections == min_conn]
            return random.choice(candidates)
        elif strategy == LoadBalanceStrategy.WEIGHTED:
            total_weight = sum(e.weight for e in healthy)
            r = random.randint(1, total_weight)
            cumulative = 0
            for e in healthy:
                cumulative += e.weight
                if r <= cumulative:
                    return e
            return healthy[-1]
        elif strategy == LoadBalanceStrategy.IP_HASH and client_ip:
            hash_ring = self._hash_rings.get(service_name)
            if hash_ring:
                node = hash_ring.get_node(client_ip)
                if node:
                    for e in healthy:
                        if f"{e.host}:{e.port}" == node:
                            return e
            return random.choice(healthy)
        elif strategy == LoadBalanceStrategy.CONSISTENT_HASH and client_ip:
            hash_ring = self._hash_rings.get(service_name)
            if hash_ring:
                node = hash_ring.get_node(client_ip)
                if node:
                    for e in healthy:
                        if f"{e.host}:{e.port}" == node:
                            return e
            return random.choice(healthy)

        return healthy[0]

    def record_success(self, service_name: str, endpoint_id: str) -> None:
        """Record successful request."""
        self._stats.successful_requests += 1
        self._stats.total_requests += 1

        endpoints = self._services.get(service_name, {})
        if endpoint_id in endpoints:
            endpoints[endpoint_id].connections = max(0, endpoints[endpoint_id].connections - 1)
            endpoints[endpoint_id].failures = 0

        cb = self._circuit_breakers.get(service_name)
        if cb and cb.state == CircuitState.HALF_OPEN:
            cb.consecutive_successes += 1
            if cb.consecutive_successes >= cb.success_threshold:
                cb.state = CircuitState.CLOSED
                cb.consecutive_failures = 0

    def record_failure(self, service_name: str, endpoint_id: str) -> None:
        """Record failed request."""
        self._stats.failed_requests += 1
        self._stats.total_requests += 1

        endpoints = self._services.get(service_name, {})
        if endpoint_id in endpoints:
            endpoints[endpoint_id].failures += 1
            endpoints[endpoint_id].last_failure = time.time()
            endpoints[endpoint_id].connections = max(0, endpoints[endpoint_id].connections - 1)

        cb = self._circuit_breakers.get(service_name)
        if cb:
            cb.consecutive_failures += 1
            cb.total_failures += 1
            cb.last_failure_time = time.time()

            if cb.state == CircuitState.HALF_OPEN:
                cb.state = CircuitState.OPEN
                self._stats.circuit_open_count += 1
            elif (cb.consecutive_failures >= cb.failure_threshold and
                  cb.state == CircuitState.CLOSED):
                cb.state = CircuitState.OPEN
                self._stats.circuit_open_count += 1

    def get_circuit_state(self, service_name: str) -> CircuitState:
        """Get circuit breaker state for service."""
        cb = self._circuit_breakers.get(service_name)
        if not cb:
            return CircuitState.CLOSED

        if cb.state == CircuitState.OPEN:
            if time.time() - cb.last_failure_time >= cb.timeout_seconds:
                cb.state = CircuitState.HALF_OPEN
                cb.consecutive_successes = 0
                cb.consecutive_failures = 0

        return cb.state

    def create_circuit_breaker(self, name: str,
                               failure_threshold: int = 5,
                               timeout_seconds: float = 30.0) -> CircuitBreaker:
        """Create circuit breaker for service."""
        cb = CircuitBreaker(
            name=name,
            failure_threshold=failure_threshold,
            timeout_seconds=timeout_seconds
        )
        self._circuit_breakers[name] = cb
        return cb

    def get_stats(self) -> MeshStats:
        """Get mesh statistics."""
        return self._stats


_global_mesh = ServiceMesh()


class ServiceMeshAction:
    """Service mesh traffic management action.

    Example:
        action = ServiceMeshAction()

        action.register_service("user-service", {
            "id": "user-1", "host": "10.0.0.1", "port": 8080
        })

        result = action.route("user-service", strategy="round_robin")
        if result.success:
            print(f"Routed to {result.endpoint.host}:{result.endpoint.port}")

        action.record_success("user-service", result.endpoint.id)
    """

    def __init__(self):
        self._mesh = _global_mesh

    def register_service(self, service_name: str,
                         endpoint: Dict[str, Any]) -> Dict[str, Any]:
        """Register service endpoint.

        Args:
            service_name: Name of the service
            endpoint: Dict with id, host, port, weight, metadata

        Returns:
            Dict with success status
        """
        try:
            ep = ServiceEndpoint(
                id=endpoint["id"],
                host=endpoint["host"],
                port=endpoint["port"],
                weight=endpoint.get("weight", 100),
                metadata=endpoint.get("metadata", {})
            )
            self._mesh.register_service(service_name, ep)
            return {
                "success": True,
                "service": service_name,
                "endpoint": ep.id,
                "message": f"Registered {ep.id} for {service_name}"
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    def route(self, service_name: str,
              strategy: str = "round_robin",
              client_ip: Optional[str] = None) -> RoutingResult:
        """Route request to service using load balancing.

        Args:
            service_name: Target service name
            strategy: Load balancing strategy
            client_ip: Client IP for hash-based strategies

        Returns:
            RoutingResult with selected endpoint
        """
        try:
            strategy_enum = LoadBalanceStrategy(strategy.lower())
        except ValueError:
            strategy_enum = LoadBalanceStrategy.ROUND_ROBIN

        circuit_state = self._mesh.get_circuit_state(service_name)

        if circuit_state == CircuitState.OPEN:
            return RoutingResult(
                success=False,
                endpoint=None,
                strategy=strategy,
                reason="Circuit breaker open",
                circuit_state=circuit_state,
                error="Service unavailable due to circuit breaker"
            )

        endpoint = self._mesh.get_endpoint(service_name, strategy_enum, client_ip)

        if endpoint:
            endpoint.connections += 1
            return RoutingResult(
                success=True,
                endpoint=endpoint,
                strategy=strategy,
                reason=f"Selected by {strategy}",
                circuit_state=circuit_state
            )

        return RoutingResult(
            success=False,
            endpoint=None,
            strategy=strategy,
            reason="No healthy endpoints",
            circuit_state=circuit_state,
            error="No healthy service endpoints"
        )

    def record_success(self, service_name: str, endpoint_id: str) -> Dict[str, Any]:
        """Record successful request.

        Args:
            service_name: Service name
            endpoint_id: Endpoint ID

        Returns:
            Dict with success status
        """
        self._mesh.record_success(service_name, endpoint_id)
        return {"success": True, "message": "Success recorded"}

    def record_failure(self, service_name: str, endpoint_id: str) -> Dict[str, Any]:
        """Record failed request.

        Args:
            service_name: Service name
            endpoint_id: Endpoint ID

        Returns:
            Dict with success status
        """
        self._mesh.record_failure(service_name, endpoint_id)
        return {"success": True, "message": "Failure recorded"}

    def create_circuit_breaker(self, service_name: str,
                               failure_threshold: int = 5,
                               timeout_seconds: float = 30.0) -> Dict[str, Any]:
        """Create circuit breaker for service.

        Args:
            service_name: Service name
            failure_threshold: Failures before opening circuit
            timeout_seconds: Time before half-open

        Returns:
            Dict with circuit breaker info
        """
        cb = self._mesh.create_circuit_breaker(
            service_name,
            failure_threshold,
            timeout_seconds
        )
        return {
            "success": True,
            "service": service_name,
            "failure_threshold": cb.failure_threshold,
            "timeout_seconds": cb.timeout_seconds,
            "message": f"Circuit breaker created for {service_name}"
        }

    def get_circuit_state(self, service_name: str) -> Dict[str, Any]:
        """Get circuit breaker state.

        Args:
            service_name: Service name

        Returns:
            Dict with circuit state
        """
        state = self._mesh.get_circuit_state(service_name)
        return {
            "success": True,
            "service": service_name,
            "state": state.value,
            "message": f"Circuit state: {state.value}"
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get service mesh statistics."""
        stats = self._mesh.get_stats()
        return {
            "success": True,
            "stats": {
                "total_requests": stats.total_requests,
                "successful_requests": stats.successful_requests,
                "failed_requests": stats.failed_requests,
                "circuit_open_count": stats.circuit_open_count,
                "retries": stats.retries,
                "timeouts": stats.timeouts
            },
            "message": "Stats retrieved"
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute service mesh action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "register", "route", "record_success", "record_failure",
                         "create_circuit_breaker", "get_circuit_state", "get_stats"
            - service_name: Service name
            - endpoint: Endpoint dict (for register)
            - strategy: Load balancing strategy (for route)
            - client_ip: Client IP (for hash-based routing)
            - endpoint_id: Endpoint ID (for record_success/record_failure)
            - failure_threshold: Circuit breaker threshold
            - timeout_seconds: Circuit breaker timeout

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "route")
    action = ServiceMeshAction()

    try:
        if operation == "register":
            service_name = params.get("service_name", "")
            endpoint = params.get("endpoint", {})
            if not service_name or not endpoint:
                return {"success": False, "message": "service_name and endpoint required"}
            return action.register_service(service_name, endpoint)

        elif operation == "route":
            service_name = params.get("service_name", "")
            strategy = params.get("strategy", "round_robin")
            client_ip = params.get("client_ip")
            if not service_name:
                return {"success": False, "message": "service_name required"}
            result = action.route(service_name, strategy, client_ip)
            return {
                "success": result.success,
                "endpoint": {
                    "id": result.endpoint.id,
                    "host": result.endpoint.host,
                    "port": result.endpoint.port,
                    "weight": result.endpoint.weight
                } if result.endpoint else None,
                "strategy": result.strategy,
                "reason": result.reason,
                "circuit_state": result.circuit_state.value,
                "error": result.error,
                "message": result.reason
            }

        elif operation == "record_success":
            service_name = params.get("service_name", "")
            endpoint_id = params.get("endpoint_id", "")
            if not service_name or not endpoint_id:
                return {"success": False, "message": "service_name and endpoint_id required"}
            return action.record_success(service_name, endpoint_id)

        elif operation == "record_failure":
            service_name = params.get("service_name", "")
            endpoint_id = params.get("endpoint_id", "")
            if not service_name or not endpoint_id:
                return {"success": False, "message": "service_name and endpoint_id required"}
            return action.record_failure(service_name, endpoint_id)

        elif operation == "create_circuit_breaker":
            service_name = params.get("service_name", "")
            failure_threshold = params.get("failure_threshold", 5)
            timeout_seconds = params.get("timeout_seconds", 30.0)
            if not service_name:
                return {"success": False, "message": "service_name required"}
            return action.create_circuit_breaker(service_name, failure_threshold, timeout_seconds)

        elif operation == "get_circuit_state":
            service_name = params.get("service_name", "")
            if not service_name:
                return {"success": False, "message": "service_name required"}
            return action.get_circuit_state(service_name)

        elif operation == "get_stats":
            return action.get_stats()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Service mesh error: {str(e)}"}

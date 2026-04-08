"""Load Balancer Action Module.

Provides load balancing algorithms and health checking for
distributing requests across multiple backends.
"""
from __future__ import annotations

import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class LBAlgorithm(Enum):
    """Load balancing algorithm."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    RANDOM = "random"
    CONSISTENT_HASH = "consistent_hash"


@dataclass
class Backend:
    """Load balancer backend."""
    id: str
    host: str
    port: int
    weight: int = 100
    healthy: bool = True
    connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    latency_ms: float = 0.0
    last_health_check: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheck:
    """Health check configuration."""
    interval_seconds: float = 30.0
    timeout_seconds: float = 5.0
    healthy_threshold: int = 3
    unhealthy_threshold: int = 3
    check_path: str = "/health"


@dataclass
class LBResult:
    """Load balancing result."""
    success: bool
    backend: Optional[Backend]
    algorithm: str
    reason: str


class LoadBalancerStore:
    """In-memory load balancer store."""

    def __init__(self):
        self._backends: Dict[str, Dict[str, Backend]] = defaultdict(dict)
        self._counters: Dict[str, int] = defaultdict(int)

    def add_backend(self, pool: str, backend: Backend) -> None:
        """Add backend to pool."""
        self._backends[pool][backend.id] = backend

    def remove_backend(self, pool: str, backend_id: str) -> bool:
        """Remove backend from pool."""
        if backend_id in self._backends.get(pool, {}):
            del self._backends[pool][backend_id]
            return True
        return False

    def get_backends(self, pool: str) -> List[Backend]:
        """Get healthy backends."""
        backends = self._backends.get(pool, {})
        return [b for b in backends.values() if b.healthy]


_global_store = LoadBalancerStore()


class LoadBalancerAction:
    """Load balancer action.

    Example:
        action = LoadBalancerAction()

        action.add_backend("api", {"id": "api-1", "host": "10.0.0.1", "port": 8080})
        result = action.select_backend("api", algorithm="round_robin")
    """

    def __init__(self, store: Optional[LoadBalancerStore] = None):
        self._store = store or _global_store
        self._pools: Dict[str, LBAlgorithm] = {}

    def create_pool(self, name: str, algorithm: str = "round_robin") -> Dict[str, Any]:
        """Create load balancer pool."""
        try:
            algo = LBAlgorithm(algorithm)
        except ValueError:
            return {"success": False, "message": f"Invalid algorithm: {algorithm}"}

        self._pools[name] = algo
        return {
            "success": True,
            "pool": name,
            "algorithm": algo.value,
            "message": f"Created pool: {name}"
        }

    def add_backend(self, pool: str, backend: Dict[str, Any]) -> Dict[str, Any]:
        """Add backend to pool."""
        if pool not in self._pools:
            return {"success": False, "message": "Pool not found"}

        be = Backend(
            id=backend["id"],
            host=backend["host"],
            port=backend["port"],
            weight=backend.get("weight", 100),
            metadata=backend.get("metadata", {})
        )
        self._store.add_backend(pool, be)

        return {
            "success": True,
            "backend_id": be.id,
            "pool": pool,
            "message": f"Added {be.host}:{be.port} to {pool}"
        }

    def remove_backend(self, pool: str, backend_id: str) -> Dict[str, Any]:
        """Remove backend from pool."""
        if self._store.remove_backend(pool, backend_id):
            return {"success": True, "message": f"Removed {backend_id}"}
        return {"success": False, "message": "Backend not found"}

    def select_backend(self, pool: str, client_ip: Optional[str] = None,
                       algorithm: Optional[str] = None) -> Dict[str, Any]:
        """Select backend using load balancing."""
        algo = self._pools.get(pool)
        if not algo:
            return {"success": False, "message": "Pool not found"}

        if algorithm:
            try:
                algo = LBAlgorithm(algorithm)
            except ValueError:
                pass

        backends = self._store.get_backends(pool)
        if not backends:
            return {"success": False, "message": "No healthy backends"}

        selected = None

        if algo == LBAlgorithm.ROUND_ROBIN:
            idx = self._store._counters[pool] % len(backends)
            selected = backends[idx]
            self._store._counters[pool] += 1

        elif algo == LBAlgorithm.WEIGHTED_ROUND_ROBIN:
            total_weight = sum(b.weight for b in backends)
            r = random.randint(1, total_weight)
            cumulative = 0
            for b in backends:
                cumulative += b.weight
                if r <= cumulative:
                    selected = b
                    break

        elif algo == LBAlgorithm.LEAST_CONNECTIONS:
            selected = min(backends, key=lambda b: b.connections)

        elif algo == LBAlgorithm.IP_HASH and client_ip:
            idx = hash(client_ip) % len(backends)
            selected = backends[idx]

        elif algo == LBAlgorithm.RANDOM:
            selected = random.choice(backends)

        else:
            selected = backends[0]

        if selected:
            selected.connections += 1
            selected.total_requests += 1

        return {
            "success": True,
            "backend": {
                "id": selected.id,
                "host": selected.host,
                "port": selected.port,
                "connections": selected.connections
            },
            "algorithm": algo.value if algo else "unknown",
            "message": f"Selected {selected.host}:{selected.port}"
        }

    def record_success(self, pool: str, backend_id: str) -> Dict[str, Any]:
        """Record successful request."""
        backends = self._store.get_backends(pool)
        for b in backends:
            if b.id == backend_id:
                b.connections = max(0, b.connections - 1)
                b.failed_requests = 0
                return {"success": True, "message": "Recorded success"}
        return {"success": False, "message": "Backend not found"}

    def record_failure(self, pool: str, backend_id: str) -> Dict[str, Any]:
        """Record failed request."""
        backends = self._store.get_backends(pool)
        for b in backends:
            if b.id == backend_id:
                b.connections = max(0, b.connections - 1)
                b.failed_requests += 1
                if b.failed_requests >= 3:
                    b.healthy = False
                return {"success": True, "message": "Recorded failure"}
        return {"success": False, "message": "Backend not found"}

    def get_pool_stats(self, pool: str) -> Dict[str, Any]:
        """Get pool statistics."""
        backends = list(self._store._backends.get(pool, {}).values())
        healthy = [b for b in backends if b.healthy]

        return {
            "success": True,
            "pool": pool,
            "total_backends": len(backends),
            "healthy_backends": len(healthy),
            "backends": [
                {
                    "id": b.id,
                    "host": b.host,
                    "port": b.port,
                    "healthy": b.healthy,
                    "connections": b.connections,
                    "total_requests": b.total_requests,
                    "failed_requests": b.failed_requests
                }
                for b in backends
            ]
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute load balancer action."""
    operation = params.get("operation", "")
    action = LoadBalancerAction()

    try:
        if operation == "create_pool":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.create_pool(name, params.get("algorithm", "round_robin"))

        elif operation == "add_backend":
            pool = params.get("pool", "")
            backend = params.get("backend", {})
            if not pool or not backend:
                return {"success": False, "message": "pool and backend required"}
            return action.add_backend(pool, backend)

        elif operation == "remove_backend":
            pool = params.get("pool", "")
            backend_id = params.get("backend_id", "")
            if not pool or not backend_id:
                return {"success": False, "message": "pool and backend_id required"}
            return action.remove_backend(pool, backend_id)

        elif operation == "select":
            pool = params.get("pool", "")
            if not pool:
                return {"success": False, "message": "pool required"}
            return action.select_backend(
                pool,
                client_ip=params.get("client_ip"),
                algorithm=params.get("algorithm")
            )

        elif operation == "record_success":
            pool = params.get("pool", "")
            backend_id = params.get("backend_id", "")
            if not pool or not backend_id:
                return {"success": False, "message": "pool and backend_id required"}
            return action.record_success(pool, backend_id)

        elif operation == "record_failure":
            pool = params.get("pool", "")
            backend_id = params.get("backend_id", "")
            if not pool or not backend_id:
                return {"success": False, "message": "pool and backend_id required"}
            return action.record_failure(pool, backend_id)

        elif operation == "stats":
            pool = params.get("pool", "")
            if not pool:
                return {"success": False, "message": "pool required"}
            return action.get_pool_stats(pool)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Load balancer error: {str(e)}"}

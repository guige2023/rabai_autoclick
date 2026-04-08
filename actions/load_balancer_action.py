"""Load balancer action module for RabAI AutoClick.

Provides load balancing:
- LoadBalancer: Generic load balancer
- RoundRobin: Round-robin strategy
- LeastConnections: Least connections strategy
- WeightedBalancer: Weighted load balancing
- HealthAwareBalancer: Health-aware load balancing
"""

import time
import random
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BalanceStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    RANDOM = "random"
    IP_HASH = "ip_hash"
    LATENCY = "latency"


@dataclass
class Backend:
    """Backend server."""
    id: str
    host: str
    port: int
    weight: float = 1.0
    max_connections: int = 100
    current_connections: int = 0
    latency: float = 0.0
    is_healthy: bool = True
    last_health_check: float = 0.0
    metadata: Dict = field(default_factory=dict)


@dataclass
class BalanceResult:
    """Load balance result."""
    selected_backend: Optional[Backend]
    strategy: BalanceStrategy
    total_backends: int
    healthy_backends: int


class LoadBalancer:
    """Generic load balancer."""

    def __init__(self, strategy: BalanceStrategy = BalanceStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self._backends: Dict[str, Backend] = {}
        self._round_robin_index: Dict[str, int] = {}
        self._lock = threading.RLock()

    def add_backend(self, backend: Backend) -> bool:
        """Add backend."""
        with self._lock:
            self._backends[backend.id] = backend
            if backend.id not in self._round_robin_index:
                self._round_robin_index[backend.id] = 0
            return True

    def remove_backend(self, backend_id: str) -> bool:
        """Remove backend."""
        with self._lock:
            if backend_id in self._backends:
                del self._backends[backend_id]
                return True
            return False

    def get_backend(self, backend_id: str) -> Optional[Backend]:
        """Get backend by ID."""
        with self._lock:
            return self._backends.get(backend_id)

    def select(self, client_ip: Optional[str] = None) -> BalanceResult:
        """Select backend."""
        with self._lock:
            healthy = [b for b in self._backends.values() if b.is_healthy]

            if not healthy:
                return BalanceResult(
                    selected_backend=None,
                    strategy=self.strategy,
                    total_backends=len(self._backends),
                    healthy_backends=0,
                )

            selected = None

            if self.strategy == BalanceStrategy.ROUND_ROBIN:
                selected = self._round_robin_select(healthy)
            elif self.strategy == BalanceStrategy.LEAST_CONNECTIONS:
                selected = self._least_connections_select(healthy)
            elif self.strategy == BalanceStrategy.WEIGHTED:
                selected = self._weighted_select(healthy)
            elif self.strategy == BalanceStrategy.RANDOM:
                selected = random.choice(healthy)
            elif self.strategy == BalanceStrategy.IP_HASH:
                selected = self._ip_hash_select(healthy, client_ip or "")
            elif self.strategy == BalanceStrategy.LATENCY:
                selected = self._latency_select(healthy)

            if selected:
                selected.current_connections += 1

            return BalanceResult(
                selected_backend=selected,
                strategy=self.strategy,
                total_backends=len(self._backends),
                healthy_backends=len(healthy),
            )

    def release(self, backend_id: str):
        """Release backend connection."""
        with self._lock:
            backend = self._backends.get(backend_id)
            if backend and backend.current_connections > 0:
                backend.current_connections -= 1

    def _round_robin_select(self, backends: List[Backend]) -> Backend:
        """Round-robin selection."""
        if not backends:
            return None

        idx = 0
        for backend in backends:
            if self._round_robin_index.get(backend.id, 0) <= idx:
                self._round_robin_index[backend.id] = idx + 1
                return backend
            idx += 1

        self._round_robin_index[backends[0].id] = 1
        return backends[0]

    def _least_connections_select(self, backends: List[Backend]) -> Backend:
        """Select backend with least connections."""
        if not backends:
            return None
        return min(backends, key=lambda b: b.current_connections)

    def _weighted_select(self, backends: List[Backend]) -> Backend:
        """Weighted selection."""
        if not backends:
            return None
        total_weight = sum(b.weight for b in backends)
        r = random.uniform(0, total_weight)
        cumsum = 0
        for backend in backends:
            cumsum += backend.weight
            if r <= cumsum:
                return backend
        return backends[-1]

    def _ip_hash_select(self, backends: List[Backend], client_ip: str) -> Backend:
        """IP hash selection."""
        if not backends:
            return None
        hash_val = sum(ord(c) for c in client_ip)
        idx = hash_val % len(backends)
        return backends[idx]

    def _latency_select(self, backends: List[Backend]) -> Backend:
        """Select by latency."""
        if not backends:
            return None
        available = [b for b in backends if b.latency > 0]
        if not available:
            return backends[0]
        return min(available, key=lambda b: b.latency)

    def list_backends(self) -> List[Dict]:
        """List all backends."""
        with self._lock:
            return [
                {
                    "id": b.id,
                    "host": b.host,
                    "port": b.port,
                    "weight": b.weight,
                    "connections": b.current_connections,
                    "latency": b.latency,
                    "healthy": b.is_healthy,
                }
                for b in self._backends.values()
            ]


class LoadBalancerAction(BaseAction):
    """Load balancer action."""
    action_type = "load_balancer"
    display_name = "负载均衡"
    description = "服务负载均衡调度"

    def __init__(self):
        super().__init__()
        self._balancers: Dict[str, LoadBalancer] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "balance")

            if operation == "create":
                return self._create_balancer(params)
            elif operation == "add":
                return self._add_backend(params)
            elif operation == "remove":
                return self._remove_backend(params)
            elif operation == "balance":
                return self._balance(params)
            elif operation == "release":
                return self._release(params)
            elif operation == "list":
                return self._list_backends(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Load balancer error: {str(e)}")

    def _create_balancer(self, params: Dict) -> ActionResult:
        """Create load balancer."""
        name = params.get("name", "default")
        strategy_str = params.get("strategy", "round_robin").upper()

        try:
            strategy = BalanceStrategy[strategy_str]
        except KeyError:
            return ActionResult(success=False, message=f"Unknown strategy: {strategy_str}")

        balancer = LoadBalancer(strategy=strategy)
        self._balancers[name] = balancer

        return ActionResult(success=True, message=f"Load balancer '{name}' created with {strategy.value} strategy")

    def _add_backend(self, params: Dict) -> ActionResult:
        """Add backend to balancer."""
        name = params.get("name", "default")
        backend_id = params.get("backend_id")
        host = params.get("host")
        port = params.get("port", 80)

        if not backend_id or not host:
            return ActionResult(success=False, message="backend_id and host are required")

        balancer = self._balancers.get(name)
        if not balancer:
            balancer = LoadBalancer()
            self._balancers[name] = balancer

        backend = Backend(
            id=backend_id,
            host=host,
            port=port,
            weight=params.get("weight", 1.0),
            max_connections=params.get("max_connections", 100),
        )

        balancer.add_backend(backend)
        return ActionResult(success=True, message=f"Backend '{backend_id}' added to '{name}'")

    def _remove_backend(self, params: Dict) -> ActionResult:
        """Remove backend from balancer."""
        name = params.get("name", "default")
        backend_id = params.get("backend_id")

        if not backend_id:
            return ActionResult(success=False, message="backend_id is required")

        balancer = self._balancers.get(name)
        if not balancer:
            return ActionResult(success=False, message=f"Balancer '{name}' not found")

        success = balancer.remove_backend(backend_id)
        return ActionResult(success=success, message="Backend removed" if success else "Backend not found")

    def _balance(self, params: Dict) -> ActionResult:
        """Select backend."""
        name = params.get("name", "default")
        client_ip = params.get("client_ip")

        balancer = self._balancers.get(name)
        if not balancer:
            balancer = LoadBalancer()
            self._balancers[name] = balancer

        result = balancer.select(client_ip)

        if result.selected_backend:
            return ActionResult(
                success=True,
                message=f"Selected backend: {result.selected_backend.id}",
                data={
                    "backend_id": result.selected_backend.id,
                    "host": result.selected_backend.host,
                    "port": result.selected_backend.port,
                    "strategy": result.strategy.value,
                    "healthy_backends": result.healthy_backends,
                },
            )
        else:
            return ActionResult(success=False, message="No healthy backends available")

    def _release(self, params: Dict) -> ActionResult:
        """Release backend connection."""
        name = params.get("name", "default")
        backend_id = params.get("backend_id")

        balancer = self._balancers.get(name)
        if not balancer:
            return ActionResult(success=False, message=f"Balancer '{name}' not found")

        balancer.release(backend_id)
        return ActionResult(success=True, message=f"Released backend '{backend_id}'")

    def _list_backends(self, params: Dict) -> ActionResult:
        """List backends."""
        name = params.get("name", "default")

        balancer = self._balancers.get(name)
        if not balancer:
            return ActionResult(success=True, message="No backends", data={"backends": []})

        backends = balancer.list_backends()
        return ActionResult(success=True, message=f"{len(backends)} backends", data={"backends": backends})

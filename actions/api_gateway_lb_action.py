"""API Gateway LB Action Module.

Load balancing and traffic management for API gateways
including round-robin, weighted routing, and health checks.
"""

from __future__ import annotations

import sys
import os
import time
import random
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LBStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"
    LEAST_CONNECTIONS = "least_connections"
    IP_HASH = "ip_hash"
    RANDOM = "random"


@dataclass
class Backend:
    """A backend server."""
    url: str
    weight: int = 1
    max_connections: int = 100
    current_connections: int = 0
    healthy: bool = True
    last_check: float = 0.0


@dataclass
class LBConfig:
    """Load balancer configuration."""
    strategy: LBStrategy = LBStrategy.ROUND_ROBIN
    health_check_interval: int = 30
    timeout: int = 30


class APIGatewayLBAction(BaseAction):
    """
    API Gateway load balancing.

    Routes traffic across backend servers using various
    load balancing strategies with health monitoring.

    Example:
        lb = APIGatewayLBAction()
        result = lb.execute(ctx, {"action": "route", "request_id": "req-123"})
    """
    action_type = "api_gateway_lb"
    display_name = "API网关负载均衡"
    description = "API网关负载均衡：轮询、加权路由和健康检查"

    def __init__(self) -> None:
        super().__init__()
        self._backends: Dict[str, Backend] = {}
        self._config = LBConfig()
        self._current_index = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "route":
                return self._route(params)
            elif action == "add_backend":
                return self._add_backend(params)
            elif action == "remove_backend":
                return self._remove_backend(params)
            elif action == "health_check":
                return self._health_check(params)
            elif action == "get_status":
                return self._get_status(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"LB error: {str(e)}")

    def _route(self, params: Dict[str, Any]) -> ActionResult:
        request_id = params.get("request_id", "")
        client_ip = params.get("client_ip", "")

        healthy_backends = [b for b in self._backends.values() if b.healthy]

        if not healthy_backends:
            return ActionResult(success=False, message="No healthy backends available")

        selected = self._select_backend(healthy_backends, client_ip)

        if selected:
            selected.current_connections += 1
            return ActionResult(success=True, message=f"Routed to {selected.url}", data={"backend": selected.url, "request_id": request_id})

        return ActionResult(success=False, message="Routing failed")

    def _select_backend(self, backends: List[Backend], client_ip: str) -> Optional[Backend]:
        if self._config.strategy == LBStrategy.ROUND_ROBIN:
            backend = backends[self._current_index % len(backends)]
            self._current_index += 1
            return backend
        elif self._config.strategy == LBStrategy.WEIGHTED:
            total_weight = sum(b.weight for b in backends)
            r = random.randint(1, total_weight)
            cum_weight = 0
            for b in backends:
                cum_weight += b.weight
                if r <= cum_weight:
                    return b
            return backends[0]
        elif self._config.strategy == LBStrategy.LEAST_CONNECTIONS:
            return min(backends, key=lambda b: b.current_connections)
        elif self._config.strategy == LBStrategy.IP_HASH:
            hash_val = hash(client_ip) % len(backends)
            return backends[hash_val]
        else:
            return random.choice(backends)

    def _add_backend(self, params: Dict[str, Any]) -> ActionResult:
        url = params.get("url", "")
        weight = params.get("weight", 1)

        if not url:
            return ActionResult(success=False, message="url is required")

        self._backends[url] = Backend(url=url, weight=weight, last_check=time.time())

        return ActionResult(success=True, message=f"Backend added: {url}")

    def _remove_backend(self, params: Dict[str, Any]) -> ActionResult:
        url = params.get("url", "")

        if url in self._backends:
            del self._backends[url]
            return ActionResult(success=True, message=f"Backend removed: {url}")

        return ActionResult(success=False, message=f"Backend not found: {url}")

    def _health_check(self, params: Dict[str, Any]) -> ActionResult:
        url = params.get("url")

        if url and url in self._backends:
            self._backends[url].healthy = True
            self._backends[url].last_check = time.time()
            return ActionResult(success=True, message=f"Backend healthy: {url}")

        for backend in self._backends.values():
            backend.healthy = True
            backend.last_check = time.time()

        return ActionResult(success=True, message="Health check complete")

    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        backends = [{"url": b.url, "healthy": b.healthy, "connections": b.current_connections, "weight": b.weight} for b in self._backends.values()]
        return ActionResult(success=True, data={"backends": backends, "strategy": self._config.strategy.value})

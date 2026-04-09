"""API Mesh Action Module.

Provides service mesh capabilities for distributed API communication
including circuit breaking per service, retries, and observability.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ServiceHealth(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class ServiceConfig:
    """Configuration for a mesh service."""
    name: str
    url: str
    timeout: float = 5.0
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 30.0
    weight: int = 1


@dataclass
class CircuitBreakerState:
    """Circuit breaker state per service."""
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    state: str = "closed"  # closed, open, half_open
    next_attempt_time: float = 0.0


class APIMeshAction(BaseAction):
    """Service mesh for API routing with fault tolerance.

    Routes requests across multiple services with circuit breaker
    patterns, retry logic, and health monitoring.

    Args:
        context: Execution context.
        params: Dict with keys:
            - services: List[Dict] with service configs
            - service_name: Name of service to call
            - request_data: Data to send
            - operation: Operation type (call, health_check, get_status)
    """
    action_type = "api_mesh"
    display_name = "API网格"
    description = "分布式服务网格路由与容错"

    def get_required_params(self) -> List[str]:
        return ["services", "service_name"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "request_data": None,
            "operation": "call",
            "default_timeout": 5.0,
            "enable_logging": True,
        }

    def __init__(self) -> None:
        super().__init__()
        self._services: Dict[str, ServiceConfig] = {}
        self._circuit_breakers: Dict[str, CircuitBreakerState] = {}
        self._health_history: Dict[str, List[float]] = {}
        self._request_counts: Dict[str, int] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mesh operation."""
        start_time = time.time()

        # Validate and extract parameters
        is_valid, error_msg = self.validate_params(params)
        if not is_valid:
            return ActionResult(success=False, message=error_msg, duration=time.time() - start_time)

        services = params.get("services", [])
        service_name = params.get("service_name")
        operation = params.get("operation", "call")
        request_data = params.get("request_data")
        enable_logging = params.get("enable_logging", True)

        # Initialize services
        for svc in services:
            name = svc.get("name")
            if name:
                self._services[name] = ServiceConfig(**svc)
                if name not in self._circuit_breakers:
                    self._circuit_breakers[name] = CircuitBreakerState()
                if name not in self._health_history:
                    self._health_history[name] = []
                if name not in self._request_counts:
                    self._request_counts[name] = 0

        # Execute operation
        if operation == "health_check":
            return self._health_check(service_name, enable_logging, start_time)
        elif operation == "get_status":
            return self._get_mesh_status(start_time)
        else:
            return self._route_request(
                service_name, request_data, enable_logging, start_time
            )

    def _route_request(
        self,
        service_name: str,
        request_data: Any,
        enable_logging: bool,
        start_time: float
    ) -> ActionResult:
        """Route request to service with circuit breaker."""
        if service_name not in self._services:
            return ActionResult(
                success=False,
                message=f"Service '{service_name}' not found in mesh",
                duration=time.time() - start_time
            )

        svc = self._services[service_name]
        cb = self._circuit_breakers[service_name]

        # Check circuit breaker
        if cb.state == "open":
            if time.time() < cb.next_attempt_time:
                return ActionResult(
                    success=False,
                    message=f"Circuit breaker OPEN for {service_name}. Retry after {cb.next_attempt_time - time.time():.1f}s",
                    data={"circuit_state": "open", "service": service_name},
                    duration=time.time() - start_time
                )
            cb.state = "half_open"

        # Simulate service call (in real impl, use aiohttp/httpx)
        self._request_counts[service_name] += 1
        success = self._simulate_call(svc, request_data)

        if success:
            cb.success_count += 1
            cb.failure_count = 0
            if cb.state == "half_open":
                cb.state = "closed"
                logger.info(f"Circuit breaker CLOSED for {service_name}")
            return ActionResult(
                success=True,
                message=f"Request routed to {service_name}",
                data={
                    "service": service_name,
                    "circuit_state": cb.state,
                    "response": {"status": "ok", "data": request_data}
                },
                duration=time.time() - start_time
            )
        else:
            cb.failure_count += 1
            cb.last_failure_time = time.time()
            if cb.failure_count >= svc.circuit_breaker_threshold:
                cb.state = "open"
                cb.next_attempt_time = time.time() + svc.circuit_breaker_timeout
                logger.warning(f"Circuit breaker OPEN for {service_name} after {cb.failure_count} failures")
            return ActionResult(
                success=False,
                message=f"Request failed on {service_name} (failure {cb.failure_count}/{svc.circuit_breaker_threshold})",
                data={
                    "service": service_name,
                    "circuit_state": cb.state,
                    "failure_count": cb.failure_count
                },
                duration=time.time() - start_time
            )

    def _health_check(self, service_name: str, enable_logging: bool, start_time: float) -> ActionResult:
        """Perform health check on service."""
        if service_name not in self._services:
            return ActionResult(success=False, message=f"Service '{service_name}' not found", duration=time.time() - start_time)

        svc = self._services[service_name]
        cb = self._circuit_breakers[service_name]

        # Simulate health check latency
        latency = 0.01
        is_healthy = cb.state != "open"

        self._health_history[service_name].append(latency)
        if len(self._health_history[service_name]) > 100:
            self._health_history[service_name] = self._health_history[service_name][-100:]

        return ActionResult(
            success=is_healthy,
            message=f"Health check {'PASSED' if is_healthy else 'FAILED'} for {service_name}",
            data={
                "service": service_name,
                "healthy": is_healthy,
                "circuit_state": cb.state,
                "latency_ms": latency * 1000,
                "failure_count": cb.failure_count,
                "request_count": self._request_counts.get(service_name, 0)
            },
            duration=time.time() - start_time
        )

    def _get_mesh_status(self, start_time: float) -> ActionResult:
        """Get overall mesh status."""
        status = {}
        for name, svc in self._services.items():
            cb = self._circuit_breakers.get(name, CircuitBreakerState())
            health_history = self._health_history.get(name, [])
            avg_latency = sum(health_history) / len(health_history) if health_history else 0.0
            status[name] = {
                "url": svc.url,
                "circuit_state": cb.state,
                "failure_count": cb.failure_count,
                "request_count": self._request_counts.get(name, 0),
                "avg_latency_ms": avg_latency * 1000,
                "healthy": cb.state != "open"
            }
        return ActionResult(
            success=True,
            message="Mesh status retrieved",
            data={"services": status, "total_services": len(status)},
            duration=time.time() - start_time
        )

    def _simulate_call(self, svc: ServiceConfig, data: Any) -> bool:
        """Simulate a service call. Replace with actual HTTP client."""
        # Simulate random success rate of 90%
        import random
        return random.random() < 0.9


from enum import Enum

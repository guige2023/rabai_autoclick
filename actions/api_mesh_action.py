"""
API Mesh and Service Mesh Communication Module.

Provides service-to-service communication, circuit breaking,
load balancing, and distributed tracing across microservices.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    HEALTHY = auto()
    DEGRADED = auto()
    UNHEALTHY = auto()
    UNKNOWN = auto()


@dataclass(frozen=True)
class ServiceEndpoint:
    """A single service endpoint."""
    url: str
    weight: int = 1
    tags: FrozenSet[str] = field(default_factory=frozenset)
    metadata: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)


@dataclass
class ServiceNode:
    """Represents a registered service in the mesh."""
    name: str
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    health_check_url: Optional[str] = None
    status: ServiceStatus = ServiceStatus.UNKNOWN
    last_health_check: Optional[datetime] = None
    consecutive_failures: int = 0
    latency_ms_avg: float = 0.0
    request_count: int = 0
    failure_count: int = 0


@dataclass
class MeshRequest:
    """A request routed through the service mesh."""
    request_id: str
    service_name: str
    path: str
    method: str = "GET"
    headers: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)
    body: Optional[bytes] = None
    timeout_ms: float = 5000.0
    retries: int = 0
    traced: bool = True


@dataclass
class MeshResponse:
    """Response from a service mesh request."""
    request_id: str
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[bytes] = None
    latency_ms: float = 0.0
    endpoint_used: str = ""
    from_cache: bool = False
    error: Optional[str] = None


class CircuitBreaker:
    """
    Circuit breaker pattern implementation for service mesh.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state: str = "closed"
        self._failure_count: int = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls: int = 0

    @property
    def state(self) -> str:
        if self._state == "open":
            if (
                self._last_failure_time
                and time.time() - self._last_failure_time >= self.recovery_timeout
            ):
                self._state = "half_open"
                self._half_open_calls = 0
                logger.info("Circuit breaker entering half_open state")
        return self._state

    def record_success(self) -> None:
        if self._state == "half_open":
            self._half_open_calls += 1
            if self._half_open_calls >= self.half_open_max_calls:
                self._state = "closed"
                self._failure_count = 0
                logger.info("Circuit breaker closed after successful recovery")
        elif self._state == "closed":
            self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == "half_open":
            self._state = "open"
            logger.warning("Circuit breaker reopened after half_open failure")
        elif (
            self._state == "closed"
            and self._failure_count >= self.failure_threshold
        ):
            self._state = "open"
            logger.warning(
                "Circuit breaker opened after %d failures", self._failure_count
            )

    def is_available(self) -> bool:
        return self.state != "open" or (
            self._last_failure_time
            and time.time() - self._last_failure_time >= self.recovery_timeout
        )


class LoadBalancer:
    """Load balancing strategies for service endpoints."""

    def __init__(self, strategy: str = "round_robin"):
        self.strategy = strategy
        self._index: int = 0
        self._weights: Dict[str, int] = {}
        self._call_counts: Dict[str, int] = defaultdict(int)

    def select(
        self, endpoints: List[ServiceEndpoint]
    ) -> Optional[ServiceEndpoint]:
        if not endpoints:
            return None

        healthy = [ep for ep in endpoints if ep.weight > 0]
        if not healthy:
            return None

        if self.strategy == "round_robin":
            selected = healthy[self._index % len(healthy)]
            self._index += 1
            return selected

        elif self.strategy == "weighted_round_robin":
            total_weight = sum(ep.weight for ep in healthy)
            selected_weight = self._index % total_weight
            cumulative = 0
            for ep in healthy:
                cumulative += ep.weight
                if cumulative > selected_weight:
                    self._index += 1
                    return ep
            self._index += 1
            return healthy[-1]

        elif self.strategy == "least_connections":
            return min(healthy, key=lambda ep: self._call_counts.get(ep.url, 0))

        elif self.strategy == "consistent_hash":
            hash_val = hashlib.md5(str(self._index).encode()).hexdigest()
            self._index += 1
            return healthy[int(hash_val, 16) % len(healthy)]

        elif self.strategy == "ip_hash":
            return healthy[self._index % len(healthy)]

        return healthy[0]

    def record_call(self, endpoint_url: str) -> None:
        self._call_counts[endpoint_url] += 1

    def record_complete(self, endpoint_url: str) -> None:
        self._call_counts[endpoint_url] = max(0, self._call_counts[endpoint_url] - 1)


class ServiceMesh:
    """
    Service mesh for managing microservice communication.
    """

    def __init__(self):
        self._services: Dict[str, ServiceNode] = {}
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._load_balancers: Dict[str, LoadBalancer] = {}
        self._request_history: Dict[str, List[Tuple[float, int]]] = defaultdict(list)
        self._pending_requests: Dict[str, MeshRequest] = {}

    def register_service(
        self,
        name: str,
        endpoints: List[ServiceEndpoint],
        health_check_url: Optional[str] = None,
    ) -> None:
        self._services[name] = ServiceNode(
            name=name,
            endpoints=endpoints,
            health_check_url=health_check_url,
        )
        self._circuit_breakers[name] = CircuitBreaker()
        self._load_balancers[name] = LoadBalancer("round_robin")
        logger.info("Registered service '%s' with %d endpoints", name, len(endpoints))

    def update_endpoints(self, name: str, endpoints: List[ServiceEndpoint]) -> None:
        if name in self._services:
            self._services[name].endpoints = endpoints

    def get_service(self, name: str) -> Optional[ServiceNode]:
        return self._services.get(name)

    async def route_request(self, request: MeshRequest) -> MeshResponse:
        """Route a request through the service mesh."""
        import aiohttp

        start_time = time.time()
        service = self._services.get(request.service_name)

        if not service:
            return MeshResponse(
                request_id=request.request_id,
                status_code=404,
                error=f"Service not found: {request.service_name}",
                latency_ms=0.0,
            )

        cb = self._circuit_breakers.get(request.service_name)
        if cb and not cb.is_available():
            return MeshResponse(
                request_id=request.request_id,
                status_code=503,
                error="Service circuit breaker open",
                latency_ms=(time.time() - start_time) * 1000,
            )

        lb = self._load_balancers.get(request.service_name)
        endpoint = lb.select(service.endpoints) if lb else None

        if not endpoint:
            return MeshResponse(
                request_id=request.request_id,
                status_code=503,
                error="No healthy endpoints available",
                latency_ms=(time.time() - start_time) * 1000,
            )

        url = f"{endpoint.url}{request.path}"
        headers = dict(request.headers)

        if request.traced:
            headers["X-Request-ID"] = request.request_id
            headers["X-Trace-Time"] = str(time.time())

        lb.record_call(endpoint.url)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    request.method,
                    url,
                    headers=headers,
                    data=request.body,
                    timeout=aiohttp.ClientTimeout(total=request.timeout_ms / 1000),
                ) as resp:
                    body = await resp.read()
                    latency = (time.time() - start_time) * 1000

                    if cb:
                        if resp.status < 500:
                            cb.record_success()
                        else:
                            cb.record_failure()

                    if service.status != ServiceStatus.HEALTHY and resp.status < 500:
                        service.status = ServiceStatus.HEALTHY

                    return MeshResponse(
                        request_id=request.request_id,
                        status_code=resp.status,
                        headers=dict(resp.headers),
                        body=body,
                        latency_ms=latency,
                        endpoint_used=endpoint.url,
                    )

        except asyncio.TimeoutError:
            if cb:
                cb.record_failure()
            return MeshResponse(
                request_id=request.request_id,
                status_code=504,
                error="Request timeout",
                latency_ms=(time.time() - start_time) * 1000,
                endpoint_used=endpoint.url,
            )
        except Exception as exc:
            if cb:
                cb.record_failure()
            logger.error("Mesh request error: %s", exc)
            return MeshResponse(
                request_id=request.request_id,
                status_code=500,
                error=str(exc),
                latency_ms=(time.time() - start_time) * 1000,
                endpoint_used=endpoint.url,
            )

    async def health_check(self, service_name: str) -> ServiceStatus:
        """Perform health check on a service."""
        import aiohttp

        service = self._services.get(service_name)
        if not service or not service.health_check_url:
            return ServiceStatus.UNKNOWN

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    service.health_check_url, timeout=aiohttp.ClientTimeout(total=5.0)
                ) as resp:
                    service.last_health_check = datetime.utcnow()
                    if resp.status == 200:
                        service.status = ServiceStatus.HEALTHY
                        service.consecutive_failures = 0
                    else:
                        service.consecutive_failures += 1
                        if service.consecutive_failures >= 3:
                            service.status = ServiceStatus.UNHEALTHY
                    return service.status
        except Exception:
            service.consecutive_failures += 1
            if service.consecutive_failures >= 3:
                service.status = ServiceStatus.UNHEALTHY
            return ServiceStatus.UNHEALTHY

    def get_mesh_stats(self) -> Dict[str, Any]:
        """Get service mesh statistics."""
        stats = {}
        for name, service in self._services.items():
            cb = self._circuit_breakers.get(name)
            stats[name] = {
                "status": service.status.name,
                "endpoint_count": len(service.endpoints),
                "circuit_breaker": cb.state if cb else "unknown",
                "latency_ms_avg": service.latency_ms_avg,
                "request_count": service.request_count,
                "failure_count": service.failure_count,
            }
        return stats

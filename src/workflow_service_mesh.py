"""
Workflow Service Mesh

A comprehensive service mesh for workflow microservices featuring:
1. Service registry: Register and discover services
2. Load balancing: Multiple balancing strategies
3. Circuit breaker: Protect services from cascade failures
4. Retry policies: Configurable retry with backoff
5. Service mesh routing: Traffic management between services
6. mTLS: Mutual TLS for service-to-service communication
7. Service mesh telemetry: Metrics, logs, traces
8. Service mesh policies: Rate limiting, auth, quota
9. Canary deployment: Gradual traffic shifting
10. Fault injection: Inject failures for testing
"""

import asyncio
import hashlib
import logging
import random
import ssl
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from contextlib import asynccontextmanager
import json

logger = logging.getLogger(__name__)


class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    RANDOM = "random"
    WEIGHTED = "weighted"
    IP_HASH = "ip_hash"
    LATENCY_BASED = "latency_based"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class RetryPolicy(Enum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


class FaultType(Enum):
    DELAY = "delay"
    ERROR = "error"
    ABORT = "abort"
    TIMEOUT = "timeout"


@dataclass
class ServiceEndpoint:
    id: str
    host: str
    port: int
    weight: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    healthy: bool = True
    version: str = "v1"
    tags: Set[str] = field(default_factory=set)


@dataclass
class Service:
    name: str
    endpoints: List[ServiceEndpoint] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 30.0
    half_open_max_calls: int = 3


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 30.0
    policy: RetryPolicy = RetryPolicy.EXPONENTIAL
    retryable_errors: Tuple[str, ...] = ("timeout", "connection_error", "unavailable")


@dataclass
class RateLimitConfig:
    requests_per_second: float = 100.0
    burst_size: int = 200
    quota: Optional[int] = None
    quota_reset: datetime = field(default_factory=lambda: datetime.now() + timedelta(hours=1))


@dataclass
class CanaryConfig:
    weight: int = 0
    max_weight: int = 100
    step: int = 10
    interval: float = 60.0


@dataclass
class FaultInjectionConfig:
    enabled: bool = False
    fault_type: FaultType = FaultType.DELAY
    probability: float = 0.1
    delay_ms: int = 100
    error_code: int = 500


class TelemetryData:
    def __init__(self):
        self.request_count: Dict[str, int] = defaultdict(int)
        self.error_count: Dict[str, int] = defaultdict(int)
        self.latency_sum: Dict[str, float] = defaultdict(float)
        self.latency_count: Dict[str, int] = defaultdict(int)
        self.active_connections: Dict[str, int] = defaultdict(int)
        self.traces: List[Dict[str, Any]] = []
        self.lock = threading.Lock()

    def record_request(self, service: str, endpoint: str, latency: float, error: bool = False):
        with self.lock:
            self.request_count[f"{service}:{endpoint}"] += 1
            self.latency_sum[f"{service}:{endpoint}"] += latency
            self.latency_count[f"{service}:{endpoint}"] += 1
            if error:
                self.error_count[f"{service}:{endpoint}"] += 1

    def record_trace(self, trace: Dict[str, Any]):
        with self.lock:
            self.traces.append(trace)
            if len(self.traces) > 10000:
                self.traces = self.traces[-5000:]

    def get_average_latency(self, service: str, endpoint: str) -> float:
        key = f"{service}:{endpoint}"
        if self.latency_count[key] == 0:
            return 0.0
        return self.latency_sum[key] / self.latency_count[key]

    def get_error_rate(self, service: str, endpoint: str) -> float:
        key = f"{service}:{endpoint}"
        total = self.request_count[key]
        if total == 0:
            return 0.0
        return self.error_count[key] / total

    def get_metrics(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "request_counts": dict(self.request_count),
                "error_counts": dict(self.error_count),
                "latency_sums": dict(self.latency_sum),
                "latency_counts": dict(self.latency_count),
                "active_connections": dict(self.active_connections),
                "trace_count": len(self.traces)
            }


class CircuitBreaker:
    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        self.lock = threading.Lock()

    def record_success(self):
        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info(f"Circuit breaker {self.name} closed")
            else:
                self.failure_count = 0

    def record_failure(self):
        with self.lock:
            self.last_failure_time = time.time()
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.half_open_calls = 0
                logger.warning(f"Circuit breaker {self.name} opened from half_open")
            else:
                self.failure_count += 1
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened")

    def can_execute(self) -> bool:
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.config.timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    self.success_count = 0
                    logger.info(f"Circuit breaker {self.name} half_open")
                    return True
                return False
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < self.config.half_open_max_calls:
                    self.half_open_calls += 1
                    return True
                return False
            return False

    def get_state(self) -> CircuitState:
        with self.lock:
            return self.state


class CircuitBreakerManager:
    def __init__(self):
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.lock = threading.Lock()

    def get_breaker(self, service: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        with self.lock:
            if service not in self.breakers:
                self.breakers[service] = CircuitBreaker(service, config or CircuitBreakerConfig())
            return self.breakers[service]


class ServiceRegistry:
    def __init__(self):
        self.services: Dict[str, Service] = {}
        self.lock = threading.RLock()
        self.service_versions: Dict[str, Set[str]] = defaultdict(set)
        self.service_tags: Dict[str, Set[str]] = defaultdict(set)

    def register(self, name: str, host: str, port: int, **kwargs) -> ServiceEndpoint:
        with self.lock:
            endpoint_id = str(uuid.uuid4())
            endpoint = ServiceEndpoint(
                id=endpoint_id,
                host=host,
                port=port,
                weight=kwargs.get("weight", 1),
                metadata=kwargs.get("metadata", {}),
                healthy=kwargs.get("healthy", True),
                version=kwargs.get("version", "v1"),
                tags=set(kwargs.get("tags", []))
            )
            if name not in self.services:
                self.services[name] = Service(name=name)
            self.services[name].endpoints.append(endpoint)
            self.service_versions[name].add(endpoint.version)
            self.service_tags[name].update(endpoint.tags)
            logger.info(f"Registered endpoint {endpoint_id} for service {name}")
            return endpoint

    def unregister(self, name: str, endpoint_id: str) -> bool:
        with self.lock:
            if name not in self.services:
                return False
            self.services[name].endpoints = [
                e for e in self.services[name].endpoints if e.id != endpoint_id
            ]
            if not self.services[name].endpoints:
                del self.services[name]
            logger.info(f"Unregistered endpoint {endpoint_id} from service {name}")
            return True

    def discover(self, name: str, version: Optional[str] = None, tags: Optional[Set[str]] = None) -> List[ServiceEndpoint]:
        with self.lock:
            if name not in self.services:
                return []
            endpoints = self.services[name].endpoints
            if version:
                endpoints = [e for e in endpoints if e.version == version]
            if tags:
                endpoints = [e for e in endpoints if e.tags.intersection(tags)]
            return [e for e in endpoints if e.healthy]

    def update_health(self, name: str, endpoint_id: str, healthy: bool):
        with self.lock:
            if name in self.services:
                for endpoint in self.services[name].endpoints:
                    if endpoint.id == endpoint_id:
                        endpoint.healthy = healthy
                        break

    def get_all_services(self) -> Dict[str, Service]:
        with self.lock:
            return dict(self.services)

    def get_versions(self, name: str) -> Set[str]:
        with self.lock:
            return self.service_versions.get(name, set())

    def get_tags(self, name: str) -> Set[str]:
        with self.lock:
            return self.service_tags.get(name, set())


class LoadBalancer:
    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self.counters: Dict[str, int] = defaultdict(int)
        self.connection_counts: Dict[str, int] = defaultdict(int)
        self.latencies: Dict[str, List[float]] = defaultdict(list)
        self.lock = threading.Lock()

    def select(self, endpoints: List[ServiceEndpoint], client_ip: Optional[str] = None) -> Optional[ServiceEndpoint]:
        if not endpoints:
            return None

        with self.lock:
            if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
                return self._round_robin(endpoints)
            elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
                return self._least_connections(endpoints)
            elif self.strategy == LoadBalancingStrategy.RANDOM:
                return self._random(endpoints)
            elif self.strategy == LoadBalancingStrategy.WEIGHTED:
                return self._weighted(endpoints)
            elif self.strategy == LoadBalancingStrategy.IP_HASH:
                return self._ip_hash(endpoints, client_ip)
            elif self.strategy == LoadBalancingStrategy.LATENCY_BASED:
                return self._latency_based(endpoints)
            return endpoints[0]

    def _round_robin(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        key = id(endpoints)
        self.counters[key] = (self.counters[key] + 1) % len(endpoints)
        return endpoints[self.counters[key]]

    def _least_connections(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        return min(endpoints, key=lambda e: self.connection_counts.get(e.id, 0))

    def _random(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        return random.choice(endpoints)

    def _weighted(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        total_weight = sum(e.weight for e in endpoints)
        if total_weight == 0:
            return endpoints[0]
        r = random.uniform(0, total_weight)
        cumsum = 0
        for endpoint in endpoints:
            cumsum += endpoint.weight
            if r <= cumsum:
                return endpoint
        return endpoints[-1]

    def _ip_hash(self, endpoints: List[ServiceEndpoint], client_ip: Optional[str]) -> ServiceEndpoint:
        if not client_ip:
            return endpoints[0]
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        return endpoints[hash_val % len(endpoints)]

    def _latency_based(self, endpoints: List[ServiceEndpoint]) -> ServiceEndpoint:
        if not self.latencies:
            return endpoints[0]
        avg_latencies = {
            e.id: sum(self.latencies[e.id]) / len(self.latencies[e.id]) if self.latencies[e.id] else 0
            for e in endpoints
        }
        return min(endpoints, key=lambda e: avg_latencies[e.id])

    def record_connection(self, endpoint_id: str):
        with self.lock:
            self.connection_counts[endpoint_id] += 1

    def release_connection(self, endpoint_id: str):
        with self.lock:
            if self.connection_counts[endpoint_id] > 0:
                self.connection_counts[endpoint_id] -= 1

    def record_latency(self, endpoint_id: str, latency: float):
        with self.lock:
            self.latencies[endpoint_id].append(latency)
            if len(self.latencies[endpoint_id]) > 100:
                self.latencies[endpoint_id] = self.latencies[endpoint_id][-50:]


class RetryHandler:
    def __init__(self, config: RetryConfig):
        self.config = config

    def should_retry(self, error_type: str, attempt: int) -> bool:
        if attempt >= self.config.max_attempts:
            return False
        return error_type in self.config.retryable_errors

    def get_delay(self, attempt: int) -> float:
        if self.config.policy == RetryPolicy.EXPONENTIAL:
            delay = self.config.base_delay * (2 ** attempt)
        elif self.config.policy == RetryPolicy.LINEAR:
            delay = self.config.base_delay * (attempt + 1)
        else:
            delay = self.config.base_delay
        return min(delay, self.config.max_delay)


class RateLimiter:
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.tokens: Dict[str, float] = defaultdict(lambda: config.burst_size)
        self.last_update: Dict[str, float] = defaultdict(time.time)
        self.quotas_used: Dict[str, int] = defaultdict(int)
        self.lock = threading.Lock()

    def allow_request(self, client_id: str) -> bool:
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update[client_id]
            self.tokens[client_id] = min(
                self.config.burst_size,
                self.tokens[client_id] + elapsed * self.config.requests_per_second
            )
            self.last_update[client_id] = now

            if self.tokens[client_id] < 1:
                return False

            if self.config.quota:
                if self.quotas_used[client_id] >= self.config.quota:
                    if now >= self.config.quota_reset.timestamp():
                        self.quotas_used[client_id] = 0
                        self.config.quota_reset = datetime.now() + timedelta(hours=1)
                    else:
                        return False
                self.quotas_used[client_id] += 1

            self.tokens[client_id] -= 1
            return True

    def get_remaining_quota(self, client_id: str) -> int:
        with self.lock:
            if not self.config.quota:
                return -1
            return max(0, self.config.quota - self.quotas_used[client_id])


class mTLSContext:
    def __init__(self, cert_path: str, key_path: str, ca_path: Optional[str] = None):
        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_path = ca_path
        self._context: Optional[ssl.SSLContext] = None

    def get_context(self) -> ssl.SSLContext:
        if self._context is None:
            self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            self._context.load_cert_chain(self.cert_path, self.key_path)
            if self.ca_path:
                self._context.load_verify_locations(self.ca_path)
            self._context.verify_mode = ssl.CERT_REQUIRED
        return self._context


class mTLSManager:
    def __init__(self):
        self.contexts: Dict[str, mTLSContext] = {}
        self.enabled_services: Set[str] = set()
        self.lock = threading.Lock()

    def configure_mtls(self, service_name: str, cert_path: str, key_path: str, ca_path: Optional[str] = None):
        with self.lock:
            self.contexts[service_name] = mTLSContext(cert_path, key_path, ca_path)
            self.enabled_services.add(service_name)

    def is_mtls_enabled(self, service_name: str) -> bool:
        with self.lock:
            return service_name in self.enabled_services

    def get_context(self, service_name: str) -> Optional[ssl.SSLContext]:
        with self.lock:
            ctx = self.contexts.get(service_name)
            return ctx.get_context() if ctx else None


class FaultInjector:
    def __init__(self):
        self.configs: Dict[str, FaultInjectionConfig] = {}
        self.fault_counters: Dict[str, int] = defaultdict(int)
        self.lock = threading.Lock()

    def configure(self, service: str, config: FaultInjectionConfig):
        with self.lock:
            self.configs[service] = config

    def should_inject_fault(self, service: str) -> bool:
        with self.lock:
            config = self.configs.get(service)
            if not config or not config.enabled:
                return False
            if random.random() < config.probability:
                self.fault_counters[service] += 1
                return True
            return False

    def inject_fault(self, service: str) -> Tuple[FaultType, Dict[str, Any]]:
        with self.lock:
            config = self.configs.get(service)
            if not config:
                return FaultType.DELAY, {}

            if config.fault_type == FaultType.DELAY:
                return FaultType.DELAY, {"delay_ms": config.delay_ms}
            elif config.fault_type == FaultType.ERROR:
                return FaultType.ERROR, {"error_code": config.error_code}
            elif config.fault_type == FaultType.ABORT:
                return FaultType.ABORT, {}
            elif config.fault_type == FaultType.TIMEOUT:
                return FaultType.TIMEOUT, {"delay_ms": config.delay_ms}
            return FaultType.DELAY, {"delay_ms": config.delay_ms}

    def get_fault_stats(self) -> Dict[str, int]:
        with self.lock:
            return dict(self.fault_counters)


class CanaryDeployment:
    def __init__(self):
        self.weights: Dict[str, int] = {}
        self.configs: Dict[str, CanaryConfig] = {}
        self.lock = threading.Lock()

    def configure(self, service: str, config: CanaryConfig):
        with self.lock:
            self.configs[service] = config
            if service not in self.weights:
                self.weights[service] = config.weight

    def should_route_to_canary(self, service: str) -> bool:
        with self.lock:
            weight = self.weights.get(service, 0)
            return random.randint(1, 100) <= weight

    def increase_weight(self, service: str) -> int:
        with self.lock:
            if service in self.configs:
                config = self.configs[service]
                self.weights[service] = min(self.weights.get(service, 0) + config.step, config.max_weight)
                return self.weights[service]
            return 0

    def decrease_weight(self, service: str) -> int:
        with self.lock:
            if service in self.configs:
                self.weights[service] = max(self.weights.get(service, 0) - self.configs[service].step, 0)
                return self.weights[service]
            return 0

    def get_weight(self, service: str) -> int:
        with self.lock:
            return self.weights.get(service, 0)

    def promote_canary(self, service: str) -> bool:
        with self.lock:
            if service in self.configs and self.weights.get(service, 0) >= self.configs[service].max_weight:
                return True
            return False


class PolicyManager:
    def __init__(self):
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self.auth_policies: Dict[str, Callable[[str], bool]] = {}
        self.lock = threading.Lock()

    def set_rate_limiter(self, service: str, limiter: RateLimiter):
        with self.lock:
            self.rate_limiters[service] = limiter

    def check_rate_limit(self, service: str, client_id: str) -> bool:
        with self.lock:
            limiter = self.rate_limiters.get(service)
            if limiter:
                return limiter.allow_request(client_id)
            return True

    def set_auth_policy(self, service: str, policy: Callable[[str], bool]):
        with self.lock:
            self.auth_policies[service] = policy

    def check_auth(self, service: str, token: str) -> bool:
        with self.lock:
            policy = self.auth_policies.get(service)
            if policy:
                return policy(token)
            return True


class RoutingRule:
    def __init__(self, name: str, source: str, destination: str, weight: int = 100, match: Optional[Dict[str, Any]] = None):
        self.name = name
        self.source = source
        self.destination = destination
        self.weight = weight
        self.match = match or {}


class Router:
    def __init__(self):
        self.rules: List[RoutingRule] = []
        self.default_routes: Dict[str, str] = {}
        self.lock = threading.Lock()

    def add_rule(self, rule: RoutingRule):
        with self.lock:
            self.rules.append(rule)

    def set_default_route(self, service: str, destination: str):
        with self.lock:
            self.default_routes[service] = destination

    def get_route(self, service: str, request: Optional[Dict[str, Any]] = None) -> Optional[str]:
        with self.lock:
            matching_rules = [r for r in self.rules if r.source == service]
            for rule in matching_rules:
                if self._matches(rule.match, request or {}):
                    if random.randint(1, 100) <= rule.weight:
                        return rule.destination
            return self.default_routes.get(service)

    def _matches(self, match: Dict[str, Any], request: Dict[str, Any]) -> bool:
        for key, value in match.items():
            if request.get(key) != value:
                return False
        return True


class WorkflowServiceMesh:
    def __init__(self):
        self.registry = ServiceRegistry()
        self.load_balancer = LoadBalancer()
        self.circuit_breaker_manager = CircuitBreakerManager()
        self.retry_handler = RetryHandler(RetryConfig())
        self.mtls_manager = mTLSManager()
        self.telemetry = TelemetryData()
        self.policy_manager = PolicyManager()
        self.canary = CanaryDeployment()
        self.fault_injector = FaultInjector()
        self.router = Router()
        self.lock = threading.RLock()
        self._running = False
        self._background_tasks: List[asyncio.Task] = []

    def start(self):
        self._running = True
        logger.info("WorkflowServiceMesh started")

    def stop(self):
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()
        logger.info("WorkflowServiceMesh stopped")

    def register_service(self, name: str, host: str, port: int, **kwargs) -> ServiceEndpoint:
        return self.registry.register(name, host, port, **kwargs)

    def unregister_service(self, name: str, endpoint_id: str) -> bool:
        return self.registry.unregister(name, endpoint_id)

    def discover_service(self, name: str, version: Optional[str] = None, tags: Optional[Set[str]] = None) -> List[ServiceEndpoint]:
        return self.registry.discover(name, version, tags)

    def set_load_balancing_strategy(self, strategy: LoadBalancingStrategy):
        self.load_balancer.strategy = strategy

    def configure_circuit_breaker(self, service: str, config: CircuitBreakerConfig):
        self.circuit_breaker_manager.get_breaker(service, config)

    def configure_retry(self, config: RetryConfig):
        self.retry_handler.config = config

    def configure_rate_limiter(self, service: str, config: RateLimitConfig):
        self.policy_manager.set_rate_limiter(service, RateLimiter(config))

    def configure_mtls(self, service: str, cert_path: str, key_path: str, ca_path: Optional[str] = None):
        self.mtls_manager.configure_mtls(service, cert_path, key_path, ca_path)

    def configure_fault_injection(self, service: str, config: FaultInjectionConfig):
        self.fault_injector.configure(service, config)

    def configure_canary(self, service: str, config: CanaryConfig):
        self.canary.configure(service, config)

    def add_routing_rule(self, rule: RoutingRule):
        self.router.add_rule(rule)

    def set_default_route(self, service: str, destination: str):
        self.router.set_default_route(service, destination)

    @asynccontextmanager
    async def call_service(self, service: str, request: Optional[Dict[str, Any]] = None):
        start_time = time.time()
        trace_id = str(uuid.uuid4())
        trace = {
            "trace_id": trace_id,
            "service": service,
            "start_time": start_time,
            "request": request
        }

        try:
            route = self.router.get_route(service, request)
            target_service = route or service

            endpoints = self.discover_service(target_service)
            if not endpoints:
                raise Exception(f"No healthy endpoints for service {target_service}")

            if self.canary.should_route_to_canary(target_service):
                canary_endpoints = self.discover_service(f"{target_service}-canary")
                if canary_endpoints:
                    endpoints = canary_endpoints

            breaker = self.circuit_breaker_manager.get_breaker(target_service)
            if not breaker.can_execute():
                raise Exception(f"Circuit breaker open for {target_service}")

            client_ip = request.get("client_ip") if request else None
            endpoint = self.load_balancer.select(endpoints, client_ip)
            if not endpoint:
                raise Exception(f"No endpoint selected for {target_service}")

            self.load_balancer.record_connection(endpoint.id)

            if self.fault_injector.should_inject_fault(target_service):
                fault_type, fault_data = self.fault_injector.inject_fault(target_service)
                trace["fault"] = {"type": fault_type.value, "data": fault_data}
                if fault_type == FaultType.DELAY:
                    await asyncio.sleep(fault_data.get("delay_ms", 100) / 1000)
                elif fault_type == FaultType.ERROR:
                    raise Exception(f"Injected error: {fault_data.get('error_code')}")
                elif fault_type == FaultType.ABORT:
                    raise Exception("Injected abort")

            client_id = request.get("client_id", "anonymous") if request else "anonymous"
            if not self.policy_manager.check_rate_limit(target_service, client_id):
                raise Exception(f"Rate limit exceeded for {client_id}")

            token = request.get("token", "") if request else ""
            if not self.policy_manager.check_auth(target_service, token):
                raise Exception("Authentication failed")

            if self.mtls_manager.is_mtls_enabled(target_service):
                trace["mtls"] = True

            logger.debug(f"Calling service {target_service} at {endpoint.host}:{endpoint.port}")

            latency = time.time() - start_time
            self.load_balancer.record_latency(endpoint.id, latency)
            self.telemetry.record_request(target_service, endpoint.id, latency)

            breaker.record_success()
            trace["success"] = True
            trace["endpoint"] = f"{endpoint.host}:{endpoint.port}"

            yield endpoint

        except Exception as e:
            latency = time.time() - start_time
            self.telemetry.record_request(service, "unknown", latency, error=True)
            breaker.record_failure()
            trace["success"] = False
            trace["error"] = str(e)
            logger.error(f"Service call failed: {e}")
            raise

        finally:
            if endpoint:
                self.load_balancer.release_connection(endpoint.id)
            trace["end_time"] = time.time()
            trace["latency"] = time.time() - start_time
            self.telemetry.record_trace(trace)

    async def call_service_with_retry(
        self,
        service: str,
        request: Optional[Dict[str, Any]] = None,
        retry_config: Optional[RetryConfig] = None
    ):
        config = retry_config or self.retry_handler.config
        attempt = 0
        last_error = None

        while attempt < config.max_attempts:
            try:
                async with self.call_service(service, request) as endpoint:
                    return endpoint
            except Exception as e:
                last_error = e
                error_type = self._classify_error(e)
                if not self.retry_handler.should_retry(error_type, attempt):
                    raise
                delay = self.retry_handler.get_delay(attempt)
                logger.warning(f"Retry {attempt + 1}/{config.max_attempts} for {service} after {delay}s: {e}")
                await asyncio.sleep(delay)
                attempt += 1

        raise last_error

    def _classify_error(self, error: Exception) -> str:
        error_str = str(error).lower()
        if "timeout" in error_str:
            return "timeout"
        elif "connection" in error_str:
            return "connection_error"
        elif "circuit" in error_str:
            return "circuit_breaker"
        return "unknown"

    def get_telemetry(self) -> Dict[str, Any]:
        return {
            "metrics": self.telemetry.get_metrics(),
            "circuit_breakers": {
                name: breaker.get_state().value
                for name, breaker in self.circuit_breaker_manager.breakers.items()
            },
            "canary_weights": dict(self.canary.weights),
            "fault_injection_stats": self.fault_injector.get_fault_stats(),
            "services": {
                name: {
                    "endpoints": len(svc.endpoints),
                    "versions": list(self.registry.get_versions(name)),
                    "tags": list(self.registry.get_tags(name))
                }
                for name, svc in self.registry.get_all_services().items()
            }
        }

    def get_service_health(self, name: str) -> Dict[str, Any]:
        endpoints = self.registry.discover(name)
        return {
            "service": name,
            "healthy_count": sum(1 for e in endpoints if e.healthy),
            "unhealthy_count": sum(1 for e in endpoints if not e.healthy),
            "total_count": len(endpoints),
            "endpoints": [
                {
                    "id": e.id,
                    "host": e.host,
                    "port": e.port,
                    "healthy": e.healthy,
                    "version": e.version
                }
                for e in endpoints
            ]
        }

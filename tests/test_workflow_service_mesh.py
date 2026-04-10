"""Tests for Workflow Service Mesh Module.

Tests service mesh functionality including service registry, load balancing,
circuit breaker, retry policies, rate limiting, mTLS, fault injection,
canary deployment, and telemetry.
"""

import unittest
import sys
import time
import asyncio
import threading
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import random
import hashlib
import ssl
import uuid

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')


# =============================================================================
# Mock Module Imports and Data Structures
# =============================================================================

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


# =============================================================================
# Mock Telemetry Data
# =============================================================================

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


# =============================================================================
# Mock Circuit Breaker
# =============================================================================

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
            else:
                self.failure_count = 0

    def record_failure(self):
        with self.lock:
            self.last_failure_time = time.time()
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.half_open_calls = 0
            else:
                self.failure_count += 1
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitState.OPEN

    def can_execute(self) -> bool:
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.config.timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    self.success_count = 0
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


# =============================================================================
# Mock Circuit Breaker Manager
# =============================================================================

class CircuitBreakerManager:
    def __init__(self):
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.lock = threading.Lock()

    def get_breaker(self, service: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        with self.lock:
            if service not in self.breakers:
                self.breakers[service] = CircuitBreaker(service, config or CircuitBreakerConfig())
            return self.breakers[service]


# =============================================================================
# Mock Service Registry
# =============================================================================

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


# =============================================================================
# Mock Load Balancer
# =============================================================================

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


# =============================================================================
# Mock Retry Handler
# =============================================================================

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


# =============================================================================
# Mock Rate Limiter
# =============================================================================

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


# =============================================================================
# Mock mTLS Manager
# =============================================================================

class mTLSContext:
    def __init__(self, cert_path: str, key_path: str, ca_path: Optional[str] = None):
        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_path = ca_path
        self._context: Optional[ssl.SSLContext] = None

    def get_context(self) -> ssl.SSLContext:
        if self._context is None:
            self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            # In production, would load certs here
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


# =============================================================================
# Mock Fault Injector
# =============================================================================

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


# =============================================================================
# Mock Canary Deployment
# =============================================================================

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


# =============================================================================
# Mock Policy Manager
# =============================================================================

class PolicyManager:
    def __init__(self):
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self.auth_policies: Dict[str, callable] = {}
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

    def set_auth_policy(self, service: str, policy: callable):
        with self.lock:
            self.auth_policies[service] = policy

    def check_auth(self, service: str, token: str) -> bool:
        with self.lock:
            policy = self.auth_policies.get(service)
            if policy:
                return policy(token)
            return True


# =============================================================================
# Mock Routing Rule and Router
# =============================================================================

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


# =============================================================================
# Test Data Classes
# =============================================================================

class TestServiceEndpoint(unittest.TestCase):
    """Test ServiceEndpoint dataclass."""

    def test_creation(self):
        """Test service endpoint creation."""
        endpoint = ServiceEndpoint(
            id="ep1",
            host="localhost",
            port=8080
        )
        self.assertEqual(endpoint.id, "ep1")
        self.assertEqual(endpoint.host, "localhost")
        self.assertEqual(endpoint.port, 8080)
        self.assertEqual(endpoint.weight, 1)
        self.assertTrue(endpoint.healthy)
        self.assertEqual(endpoint.version, "v1")

    def test_with_metadata(self):
        """Test endpoint with metadata."""
        endpoint = ServiceEndpoint(
            id="ep1",
            host="localhost",
            port=8080,
            metadata={"key": "value"},
            tags={"production", "stable"}
        )
        self.assertEqual(endpoint.metadata["key"], "value")
        self.assertIn("production", endpoint.tags)


class TestCircuitBreakerConfig(unittest.TestCase):
    """Test CircuitBreakerConfig dataclass."""

    def test_defaults(self):
        """Test default configuration."""
        config = CircuitBreakerConfig()
        self.assertEqual(config.failure_threshold, 5)
        self.assertEqual(config.success_threshold, 2)
        self.assertEqual(config.timeout, 30.0)
        self.assertEqual(config.half_open_max_calls, 3)


class TestRetryConfig(unittest.TestCase):
    """Test RetryConfig dataclass."""

    def test_defaults(self):
        """Test default configuration."""
        config = RetryConfig()
        self.assertEqual(config.max_attempts, 3)
        self.assertEqual(config.base_delay, 0.1)
        self.assertEqual(config.max_delay, 30.0)
        self.assertEqual(config.policy, RetryPolicy.EXPONENTIAL)
        self.assertIn("timeout", config.retryable_errors)


class TestRateLimitConfig(unittest.TestCase):
    """Test RateLimitConfig dataclass."""

    def test_defaults(self):
        """Test default configuration."""
        config = RateLimitConfig()
        self.assertEqual(config.requests_per_second, 100.0)
        self.assertEqual(config.burst_size, 200)
        self.assertIsNone(config.quota)


class TestCanaryConfig(unittest.TestCase):
    """Test CanaryConfig dataclass."""

    def test_defaults(self):
        """Test default configuration."""
        config = CanaryConfig()
        self.assertEqual(config.weight, 0)
        self.assertEqual(config.max_weight, 100)
        self.assertEqual(config.step, 10)
        self.assertEqual(config.interval, 60.0)


class TestFaultInjectionConfig(unittest.TestCase):
    """Test FaultInjectionConfig dataclass."""

    def test_defaults(self):
        """Test default configuration."""
        config = FaultInjectionConfig()
        self.assertFalse(config.enabled)
        self.assertEqual(config.fault_type, FaultType.DELAY)
        self.assertEqual(config.probability, 0.1)
        self.assertEqual(config.delay_ms, 100)


# =============================================================================
# Test Enums
# =============================================================================

class TestLoadBalancingStrategy(unittest.TestCase):
    """Test LoadBalancingStrategy enum."""

    def test_values(self):
        """Test enum values."""
        self.assertEqual(LoadBalancingStrategy.ROUND_ROBIN.value, "round_robin")
        self.assertEqual(LoadBalancingStrategy.LEAST_CONNECTIONS.value, "least_connections")
        self.assertEqual(LoadBalancingStrategy.RANDOM.value, "random")
        self.assertEqual(LoadBalancingStrategy.WEIGHTED.value, "weighted")
        self.assertEqual(LoadBalancingStrategy.IP_HASH.value, "ip_hash")
        self.assertEqual(LoadBalancingStrategy.LATENCY_BASED.value, "latency_based")


class TestCircuitState(unittest.TestCase):
    """Test CircuitState enum."""

    def test_values(self):
        """Test enum values."""
        self.assertEqual(CircuitState.CLOSED.value, "closed")
        self.assertEqual(CircuitState.OPEN.value, "open")
        self.assertEqual(CircuitState.HALF_OPEN.value, "half_open")


class TestRetryPolicy(unittest.TestCase):
    """Test RetryPolicy enum."""

    def test_values(self):
        """Test enum values."""
        self.assertEqual(RetryPolicy.EXPONENTIAL.value, "exponential")
        self.assertEqual(RetryPolicy.LINEAR.value, "linear")
        self.assertEqual(RetryPolicy.FIXED.value, "fixed")


class TestFaultType(unittest.TestCase):
    """Test FaultType enum."""

    def test_values(self):
        """Test enum values."""
        self.assertEqual(FaultType.DELAY.value, "delay")
        self.assertEqual(FaultType.ERROR.value, "error")
        self.assertEqual(FaultType.ABORT.value, "abort")
        self.assertEqual(FaultType.TIMEOUT.value, "timeout")


# =============================================================================
# Test Circuit Breaker
# =============================================================================

class TestCircuitBreaker(unittest.TestCase):
    """Test CircuitBreaker class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=1.0,
            half_open_max_calls=2
        )
        self.breaker = CircuitBreaker("test_service", self.config)

    def test_initial_state(self):
        """Test initial circuit breaker state."""
        self.assertEqual(self.breaker.get_state(), CircuitState.CLOSED)

    def test_record_success_closed(self):
        """Test recording success in closed state."""
        self.breaker.record_success()
        self.assertEqual(self.breaker.get_state(), CircuitState.CLOSED)
        self.assertEqual(self.breaker.failure_count, 0)

    def test_record_failure_below_threshold(self):
        """Test recording failures below threshold."""
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.assertEqual(self.breaker.get_state(), CircuitState.CLOSED)
        self.assertEqual(self.breaker.failure_count, 2)

    def test_record_failure_at_threshold(self):
        """Test recording failures at threshold opens circuit."""
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.assertEqual(self.breaker.get_state(), CircuitState.OPEN)

    def test_can_execute_closed(self):
        """Test can_execute when closed."""
        self.assertTrue(self.breaker.can_execute())

    def test_can_execute_open(self):
        """Test can_execute when open (before timeout)."""
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.assertFalse(self.breaker.can_execute())

    def test_can_execute_half_open_after_timeout(self):
        """Test can_execute transitions to half_open after timeout."""
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.assertEqual(self.breaker.get_state(), CircuitState.OPEN)
        time.sleep(1.1)
        self.assertTrue(self.breaker.can_execute())
        self.assertEqual(self.breaker.get_state(), CircuitState.HALF_OPEN)

    def test_half_open_success_threshold(self):
        """Test half_open recovers after success threshold."""
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.breaker.record_failure()
        time.sleep(1.1)
        self.breaker.can_execute()
        self.assertEqual(self.breaker.get_state(), CircuitState.HALF_OPEN)
        self.breaker.record_success()
        self.breaker.record_success()
        self.assertEqual(self.breaker.get_state(), CircuitState.CLOSED)

    def test_half_open_failure(self):
        """Test half_open goes back to open on failure."""
        self.breaker.record_failure()
        self.breaker.record_failure()
        self.breaker.record_failure()
        time.sleep(1.1)
        self.breaker.can_execute()
        self.breaker.record_failure()
        self.assertEqual(self.breaker.get_state(), CircuitState.OPEN)


# =============================================================================
# Test Circuit Breaker Manager
# =============================================================================

class TestCircuitBreakerManager(unittest.TestCase):
    """Test CircuitBreakerManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = CircuitBreakerManager()

    def test_get_breaker_creates_new(self):
        """Test getting breaker creates new one if not exists."""
        breaker = self.manager.get_breaker("new_service")
        self.assertIsNotNone(breaker)
        self.assertEqual(breaker.name, "new_service")

    def test_get_breaker_returns_existing(self):
        """Test getting breaker returns existing one."""
        breaker1 = self.manager.get_breaker("service1")
        breaker2 = self.manager.get_breaker("service1")
        self.assertIs(breaker1, breaker2)

    def test_get_breaker_with_config(self):
        """Test getting breaker with custom config."""
        config = CircuitBreakerConfig(failure_threshold=10)
        breaker = self.manager.get_breaker("service1", config)
        self.assertEqual(breaker.config.failure_threshold, 10)


# =============================================================================
# Test Service Registry
# =============================================================================

class TestServiceRegistry(unittest.TestCase):
    """Test ServiceRegistry class."""

    def setUp(self):
        """Set up test fixtures."""
        self.registry = ServiceRegistry()

    def test_register_service(self):
        """Test registering a service endpoint."""
        endpoint = self.registry.register("service1", "localhost", 8080)
        self.assertIsNotNone(endpoint.id)
        self.assertEqual(endpoint.host, "localhost")
        self.assertEqual(endpoint.port, 8080)

    def test_register_multiple_endpoints(self):
        """Test registering multiple endpoints for same service."""
        self.registry.register("service1", "localhost", 8080)
        self.registry.register("service1", "localhost", 8081)
        service = self.registry.get_all_services()["service1"]
        self.assertEqual(len(service.endpoints), 2)

    def test_unregister_endpoint(self):
        """Test unregistering an endpoint."""
        endpoint = self.registry.register("service1", "localhost", 8080)
        result = self.registry.unregister("service1", endpoint.id)
        self.assertTrue(result)
        self.assertEqual(len(self.registry.discover("service1")), 0)

    def test_discover_service(self):
        """Test discovering service endpoints."""
        self.registry.register("service1", "localhost", 8080)
        self.registry.register("service1", "localhost", 8081)
        endpoints = self.registry.discover("service1")
        self.assertEqual(len(endpoints), 2)

    def test_discover_with_version(self):
        """Test discovering service with version filter."""
        self.registry.register("service1", "localhost", 8080, version="v1")
        self.registry.register("service1", "localhost", 8081, version="v2")
        endpoints = self.registry.discover("service1", version="v1")
        self.assertEqual(len(endpoints), 1)
        self.assertEqual(endpoints[0].version, "v1")

    def test_discover_with_tags(self):
        """Test discovering service with tags filter."""
        self.registry.register("service1", "localhost", 8080, tags=["production"])
        self.registry.register("service1", "localhost", 8081, tags=["staging"])
        endpoints = self.registry.discover("service1", tags={"production"})
        self.assertEqual(len(endpoints), 1)

    def test_discover_unhealthy(self):
        """Test that unhealthy endpoints are not discovered."""
        endpoint = self.registry.register("service1", "localhost", 8080)
        self.registry.update_health("service1", endpoint.id, False)
        endpoints = self.registry.discover("service1")
        self.assertEqual(len(endpoints), 0)

    def test_get_versions(self):
        """Test getting service versions."""
        self.registry.register("service1", "localhost", 8080, version="v1")
        self.registry.register("service1", "localhost", 8081, version="v2")
        versions = self.registry.get_versions("service1")
        self.assertIn("v1", versions)
        self.assertIn("v2", versions)

    def test_get_tags(self):
        """Test getting service tags."""
        self.registry.register("service1", "localhost", 8080, tags=["production", "stable"])
        tags = self.registry.get_tags("service1")
        self.assertIn("production", tags)
        self.assertIn("stable", tags)


# =============================================================================
# Test Load Balancer
# =============================================================================

class TestLoadBalancer(unittest.TestCase):
    """Test LoadBalancer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.endpoints = [
            ServiceEndpoint(id="ep1", host="host1", port=8080),
            ServiceEndpoint(id="ep2", host="host2", port=8080),
            ServiceEndpoint(id="ep3", host="host3", port=8080),
        ]

    def test_round_robin(self):
        """Test round robin selection."""
        balancer = LoadBalancer(LoadBalancingStrategy.ROUND_ROBIN)
        selected = [balancer.select(self.endpoints).id for _ in range(6)]
        self.assertEqual(selected, ["ep2", "ep3", "ep1", "ep2", "ep3", "ep1"])

    def test_least_connections(self):
        """Test least connections selection."""
        balancer = LoadBalancer(LoadBalancingStrategy.LEAST_CONNECTIONS)
        balancer.record_connection("ep1")
        balancer.record_connection("ep1")
        selected = balancer.select(self.endpoints)
        self.assertEqual(selected.id, "ep2")  # ep2 has 0 connections

    def test_random(self):
        """Test random selection."""
        balancer = LoadBalancer(LoadBalancingStrategy.RANDOM)
        selected_ids = {balancer.select(self.endpoints).id for _ in range(100)}
        self.assertEqual(len(selected_ids), 3)  # All endpoints should be selected

    def test_weighted(self):
        """Test weighted selection."""
        endpoints = [
            ServiceEndpoint(id="ep1", host="host1", port=8080, weight=3),
            ServiceEndpoint(id="ep2", host="host2", port=8080, weight=1),
        ]
        balancer = LoadBalancer(LoadBalancingStrategy.WEIGHTED)
        selected = [balancer.select(endpoints).id for _ in range(100)]
        self.assertGreater(selected.count("ep1"), selected.count("ep2"))

    def test_ip_hash(self):
        """Test IP hash selection."""
        balancer = LoadBalancer(LoadBalancingStrategy.IP_HASH)
        selected1 = balancer.select(self.endpoints, "192.168.1.1")
        selected2 = balancer.select(self.endpoints, "192.168.1.1")
        self.assertEqual(selected1.id, selected2.id)  # Same IP should get same endpoint

    def test_latency_based(self):
        """Test latency-based selection."""
        balancer = LoadBalancer(LoadBalancingStrategy.LATENCY_BASED)
        balancer.record_latency("ep1", 100.0)
        balancer.record_latency("ep1", 100.0)
        balancer.record_latency("ep2", 50.0)
        balancer.record_latency("ep2", 50.0)
        selected = balancer.select(self.endpoints)
        self.assertEqual(selected.id, "ep2")  # ep2 has lower latency

    def test_record_release_connection(self):
        """Test recording and releasing connections."""
        balancer = LoadBalancer()
        balancer.record_connection("ep1")
        balancer.record_connection("ep1")
        self.assertEqual(balancer.connection_counts["ep1"], 2)
        balancer.release_connection("ep1")
        self.assertEqual(balancer.connection_counts["ep1"], 1)


# =============================================================================
# Test Retry Handler
# =============================================================================

class TestRetryHandler(unittest.TestCase):
    """Test RetryHandler class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = RetryConfig(
            max_attempts=3,
            base_delay=0.1,
            max_delay=30.0,
            policy=RetryPolicy.EXPONENTIAL,
            retryable_errors=("timeout", "connection_error")
        )
        self.handler = RetryHandler(self.config)

    def test_should_retry_retryable_error(self):
        """Test should retry for retryable error."""
        self.assertTrue(self.handler.should_retry("timeout", 0))
        self.assertTrue(self.handler.should_retry("connection_error", 0))

    def test_should_retry_non_retryable_error(self):
        """Test should not retry for non-retryable error."""
        self.assertFalse(self.handler.should_retry("unknown_error", 0))

    def test_should_retry_max_attempts(self):
        """Test should not retry when max attempts reached."""
        self.assertFalse(self.handler.should_retry("timeout", 3))

    def test_exponential_delay(self):
        """Test exponential backoff delay."""
        config = RetryConfig(policy=RetryPolicy.EXPONENTIAL, base_delay=0.1)
        handler = RetryHandler(config)
        self.assertAlmostEqual(handler.get_delay(0), 0.1)
        self.assertAlmostEqual(handler.get_delay(1), 0.2)
        self.assertAlmostEqual(handler.get_delay(2), 0.4)

    def test_linear_delay(self):
        """Test linear backoff delay."""
        config = RetryConfig(policy=RetryPolicy.LINEAR, base_delay=0.1)
        handler = RetryHandler(config)
        self.assertAlmostEqual(handler.get_delay(0), 0.1)
        self.assertAlmostEqual(handler.get_delay(1), 0.2)
        self.assertAlmostEqual(handler.get_delay(2), 0.3)

    def test_fixed_delay(self):
        """Test fixed backoff delay."""
        config = RetryConfig(policy=RetryPolicy.FIXED, base_delay=0.5)
        handler = RetryHandler(config)
        self.assertEqual(handler.get_delay(0), 0.5)
        self.assertEqual(handler.get_delay(1), 0.5)
        self.assertEqual(handler.get_delay(2), 0.5)

    def test_max_delay_cap(self):
        """Test max delay cap."""
        config = RetryConfig(policy=RetryPolicy.EXPONENTIAL, base_delay=10.0, max_delay=15.0)
        handler = RetryHandler(config)
        self.assertEqual(handler.get_delay(10), 15.0)  # Would be 10240 without cap


# =============================================================================
# Test Rate Limiter
# =============================================================================

class TestServiceMeshRateLimiter(unittest.TestCase):
    """Test RateLimiter class for service mesh."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = RateLimitConfig(
            requests_per_second=10.0,
            burst_size=10,
            quota=100
        )
        self.limiter = RateLimiter(self.config)

    def test_allows_request_under_limit(self):
        """Test allowing request under limit."""
        self.assertTrue(self.limiter.allow_request("client1"))

    def test_blocks_request_over_limit(self):
        """Test blocking request over limit."""
        for _ in range(10):
            self.limiter.allow_request("client1")
        self.assertFalse(self.limiter.allow_request("client1"))

    def test_token_refill(self):
        """Test token refill over time."""
        self.limiter.tokens["client1"] = 0
        self.limiter.last_update["client1"] = time.time() - 1.0  # 1 second ago
        result = self.limiter.allow_request("client1")
        self.assertTrue(result)

    def test_quota_enforcement(self):
        """Test quota enforcement."""
        self.limiter.quotas_used["client1"] = 100
        result = self.limiter.allow_request("client1")
        self.assertFalse(result)

    def test_get_remaining_quota(self):
        """Test getting remaining quota."""
        self.limiter.quotas_used["client1"] = 50
        remaining = self.limiter.get_remaining_quota("client1")
        self.assertEqual(remaining, 50)

    def test_get_remaining_quota_no_limit(self):
        """Test getting remaining quota when no limit set."""
        config = RateLimitConfig()
        limiter = RateLimiter(config)
        remaining = limiter.get_remaining_quota("client1")
        self.assertEqual(remaining, -1)


# =============================================================================
# Test mTLS Manager
# =============================================================================

class TestMTLSManager(unittest.TestCase):
    """Test mTLSManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = mTLSManager()

    def test_configure_mtls(self):
        """Test configuring mTLS."""
        self.manager.configure_mtls("service1", "/path/cert.pem", "/path/key.pem")
        self.assertTrue(self.manager.is_mtls_enabled("service1"))

    def test_is_mtls_enabled_false(self):
        """Test mTLS not enabled by default."""
        self.assertFalse(self.manager.is_mtls_enabled("unknown_service"))

    def test_get_context(self):
        """Test getting SSL context."""
        self.manager.configure_mtls("service1", "/path/cert.pem", "/path/key.pem")
        ctx = self.manager.get_context("service1")
        self.assertIsNotNone(ctx)
        self.assertEqual(ctx.verify_mode, ssl.CERT_REQUIRED)


# =============================================================================
# Test Fault Injector
# =============================================================================

class TestFaultInjector(unittest.TestCase):
    """Test FaultInjector class."""

    def setUp(self):
        """Set up test fixtures."""
        self.injector = FaultInjector()

    def test_configure_fault_injection(self):
        """Test configuring fault injection."""
        config = FaultInjectionConfig(
            enabled=True,
            fault_type=FaultType.DELAY,
            probability=1.0
        )
        self.injector.configure("service1", config)
        self.assertTrue(self.injector.should_inject_fault("service1"))

    def test_should_not_inject_when_disabled(self):
        """Test no fault injection when disabled."""
        config = FaultInjectionConfig(enabled=False)
        self.injector.configure("service1", config)
        self.assertFalse(self.injector.should_inject_fault("service1"))

    def test_inject_delay(self):
        """Test injecting delay fault."""
        config = FaultInjectionConfig(
            enabled=True,
            fault_type=FaultType.DELAY,
            delay_ms=500
        )
        self.injector.configure("service1", config)
        fault_type, fault_data = self.injector.inject_fault("service1")
        self.assertEqual(fault_type, FaultType.DELAY)
        self.assertEqual(fault_data["delay_ms"], 500)

    def test_inject_error(self):
        """Test injecting error fault."""
        config = FaultInjectionConfig(
            enabled=True,
            fault_type=FaultType.ERROR,
            error_code=500
        )
        self.injector.configure("service1", config)
        fault_type, fault_data = self.injector.inject_fault("service1")
        self.assertEqual(fault_type, FaultType.ERROR)
        self.assertEqual(fault_data["error_code"], 500)

    def test_get_fault_stats(self):
        """Test getting fault statistics."""
        config = FaultInjectionConfig(enabled=True, probability=1.0)
        self.injector.configure("service1", config)
        for _ in range(5):
            if self.injector.should_inject_fault("service1"):
                self.injector.inject_fault("service1")
        stats = self.injector.get_fault_stats()
        self.assertGreater(stats.get("service1", 0), 0)


# =============================================================================
# Test Canary Deployment
# =============================================================================

class TestCanaryDeployment(unittest.TestCase):
    """Test CanaryDeployment class."""

    def setUp(self):
        """Set up test fixtures."""
        self.canary = CanaryDeployment()
        self.config = CanaryConfig(weight=0, max_weight=100, step=10)
        self.canary.configure("service1", self.config)

    def test_configure_canary(self):
        """Test configuring canary deployment."""
        self.assertEqual(self.canary.get_weight("service1"), 0)

    def test_increase_weight(self):
        """Test increasing canary weight."""
        weight = self.canary.increase_weight("service1")
        self.assertEqual(weight, 10)

    def test_decrease_weight(self):
        """Test decreasing canary weight."""
        self.canary.weights["service1"] = 50
        weight = self.canary.decrease_weight("service1")
        self.assertEqual(weight, 40)

    def test_weight_capped_at_max(self):
        """Test weight is capped at max."""
        self.canary.weights["service1"] = 95
        weight = self.canary.increase_weight("service1")
        self.assertEqual(weight, 100)

    def test_weight_floored_at_zero(self):
        """Test weight is floored at zero."""
        self.canary.weights["service1"] = 5
        weight = self.canary.decrease_weight("service1")
        self.assertEqual(weight, 0)

    def test_promote_canary(self):
        """Test promoting canary."""
        self.canary.weights["service1"] = 100
        self.assertTrue(self.canary.promote_canary("service1"))

    def test_cannot_promote_early(self):
        """Test cannot promote canary before max weight."""
        self.canary.weights["service1"] = 50
        self.assertFalse(self.canary.promote_canary("service1"))


# =============================================================================
# Test Policy Manager
# =============================================================================

class TestPolicyManager(unittest.TestCase):
    """Test PolicyManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = PolicyManager()

    def test_set_rate_limiter(self):
        """Test setting rate limiter."""
        config = RateLimitConfig(requests_per_second=10)
        limiter = RateLimiter(config)
        self.manager.set_rate_limiter("service1", limiter)
        self.assertTrue(self.manager.check_rate_limit("service1", "client1"))

    def test_check_rate_limit_no_limiter(self):
        """Test check rate limit returns True when no limiter set."""
        self.assertTrue(self.manager.check_rate_limit("unknown_service", "client1"))

    def test_set_auth_policy(self):
        """Test setting auth policy."""
        self.manager.set_auth_policy("service1", lambda t: t == "valid_token")
        self.assertTrue(self.manager.check_auth("service1", "valid_token"))
        self.assertFalse(self.manager.check_auth("service1", "invalid_token"))

    def test_check_auth_no_policy(self):
        """Test check auth returns True when no policy set."""
        self.assertTrue(self.manager.check_auth("unknown_service", "any_token"))


# =============================================================================
# Test Router
# =============================================================================

class TestRouter(unittest.TestCase):
    """Test Router class."""

    def setUp(self):
        """Set up test fixtures."""
        self.router = Router()

    def test_add_rule(self):
        """Test adding routing rule."""
        rule = RoutingRule("rule1", "service1", "endpoint1")
        self.router.add_rule(rule)
        route = self.router.get_route("service1")
        self.assertEqual(route, "endpoint1")

    def test_default_route(self):
        """Test default route."""
        self.router.set_default_route("service1", "default_endpoint")
        route = self.router.get_route("service1")
        self.assertEqual(route, "default_endpoint")

    def test_rule_takes_precedence(self):
        """Test that rules take precedence over default route."""
        self.router.set_default_route("service1", "default")
        self.router.add_rule(RoutingRule("rule1", "service1", "rule_endpoint", weight=100))
        route = self.router.get_route("service1")
        self.assertEqual(route, "rule_endpoint")

    def test_matching_rule_with_conditions(self):
        """Test matching rule with conditions."""
        self.router.add_rule(RoutingRule(
            "rule1",
            "service1",
            "vip_endpoint",
            weight=100,
            match={"tier": "vip"}
        ))
        route = self.router.get_route("service1", {"tier": "vip"})
        self.assertEqual(route, "vip_endpoint")

    def test_non_matching_rule_condition(self):
        """Test non-matching rule condition falls through to default."""
        self.router.set_default_route("service1", "default")
        self.router.add_rule(RoutingRule(
            "rule1",
            "service1",
            "vip_endpoint",
            weight=100,
            match={"tier": "vip"}
        ))
        route = self.router.get_route("service1", {"tier": "basic"})
        self.assertEqual(route, "default")


# =============================================================================
# Test Telemetry Data
# =============================================================================

class TestTelemetryData(unittest.TestCase):
    """Test TelemetryData class."""

    def setUp(self):
        """Set up test fixtures."""
        self.telemetry = TelemetryData()

    def test_record_request(self):
        """Test recording request."""
        self.telemetry.record_request("service1", "ep1", 0.1)
        self.assertEqual(self.telemetry.request_count["service1:ep1"], 1)

    def test_record_request_with_error(self):
        """Test recording request with error."""
        self.telemetry.record_request("service1", "ep1", 0.1, error=True)
        self.assertEqual(self.telemetry.error_count["service1:ep1"], 1)

    def test_get_average_latency(self):
        """Test getting average latency."""
        self.telemetry.record_request("service1", "ep1", 100.0)
        self.telemetry.record_request("service1", "ep1", 200.0)
        avg = self.telemetry.get_average_latency("service1", "ep1")
        self.assertEqual(avg, 150.0)

    def test_get_error_rate(self):
        """Test getting error rate."""
        self.telemetry.record_request("service1", "ep1", 0.1, error=False)
        self.telemetry.record_request("service1", "ep1", 0.1, error=False)
        self.telemetry.record_request("service1", "ep1", 0.1, error=True)
        rate = self.telemetry.get_error_rate("service1", "ep1")
        self.assertAlmostEqual(rate, 1/3)

    def test_get_metrics(self):
        """Test getting all metrics."""
        self.telemetry.record_request("service1", "ep1", 0.1)
        metrics = self.telemetry.get_metrics()
        self.assertIn("request_counts", metrics)
        self.assertIn("error_counts", metrics)
        self.assertIn("latency_sums", metrics)
        self.assertIn("latency_counts", metrics)

    def test_record_trace(self):
        """Test recording traces."""
        self.telemetry.record_trace({"trace_id": "123"})
        self.assertEqual(len(self.telemetry.traces), 1)

    def test_trace_limit(self):
        """Test traces are limited to prevent memory issues."""
        for i in range(15000):
            self.telemetry.record_trace({"trace_id": str(i)})
        self.assertLessEqual(len(self.telemetry.traces), 10000)


if __name__ == "__main__":
    unittest.main()

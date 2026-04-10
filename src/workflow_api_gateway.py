"""
Workflow API Gateway - Multi-engine routing, load balancing, authentication,
rate limiting, circuit breaker, caching, metrics, WebSocket, and versioning support.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.parse import urlencode
import weakref

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
    from fastapi.responses import JSONResponse, Response
    from starlette.datastructures import Headers
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

logger = logging.getLogger(__name__)


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    RANDOM = "random"
    IP_HASH = "ip_hash"


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class EngineInstance:
    """Represents a workflow engine instance."""
    id: str
    name: str
    url: str
    weight: int = 1
    is_healthy: bool = True
    current_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    avg_response_time: float = 0.0
    last_health_check: float = field(default_factory=time.time)
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: int = 100
    burst_size: int = 200
    window_seconds: float = 1.0


@dataclass
class ClientMetrics:
    """Metrics for a client."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency: float = 0.0
    rate_limit_hits: int = 0
    auth_failures: int = 0
    endpoint_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class WorkflowCacheEntry:
    """Cached workflow result."""
    workflow_id: str
    result: Any
    cached_at: float
    ttl: float
    version: str
    tags: Set[str] = field(default_factory=set)

    def is_expired(self, current_time: float) -> bool:
        return current_time > (self.cached_at + self.ttl)


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_calls = 0
        self._lock = asyncio.Lock()

    async def record_success(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info("Circuit breaker closed")
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0

    async def record_failure(self) -> None:
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning("Circuit breaker opened from half_open")
            elif (self.failure_count >= self.config.failure_threshold and
                  self.state == CircuitState.CLOSED):
                self.state = CircuitState.OPEN
                logger.warning("Circuit breaker opened")

    async def can_execute(self) -> bool:
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True

            if self.state == CircuitState.OPEN:
                if (time.time() - self.last_failure_time) >= self.config.timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    self.success_count = 0
                    logger.info("Circuit breaker half_open")
                    return True
                return False

            if self.state == CircuitState.HALF_OPEN:
                return self.half_open_calls < self.config.half_open_max_calls

            return False

    async def on_execute(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_calls += 1


class AuthProvider:
    """Authentication provider base."""

    def validate_api_key(self, api_key: str, client_id: str) -> bool:
        """Validate API key."""
        raise NotImplementedError

    def validate_oauth(self, token: str) -> Optional[Dict[str, Any]]:
        """Validate OAuth token."""
        raise NotImplementedError

    def hash_api_key(self, api_key: str) -> str:
        """Hash API key for storage."""
        return hashlib.sha256(api_key.encode()).hexdigest()


class SimpleAuthProvider(AuthProvider):
    """Simple in-memory auth provider."""

    def __init__(self):
        self._api_keys: Dict[str, Dict[str, Any]] = {}
        self._oauth_tokens: Dict[str, Dict[str, Any]] = {}

    def register_api_key(self, client_id: str, api_key: str, metadata: Optional[Dict] = None) -> None:
        hashed = self.hash_api_key(api_key)
        self._api_keys[hashed] = {
            "client_id": client_id,
            "metadata": metadata or {},
            "created_at": time.time()
        }

    def validate_api_key(self, api_key: str, client_id: str) -> bool:
        hashed = self.hash_api_key(api_key)
        if hashed not in self._api_keys:
            return False
        return self._api_keys[hashed]["client_id"] == client_id

    def validate_oauth(self, token: str) -> Optional[Dict[str, Any]]:
        if not JWT_AVAILABLE:
            return None
        try:
            payload = jwt.decode(token, options={"verify_signature": False})
            return payload
        except Exception:
            return None

    def register_oauth_token(self, token: str, payload: Dict[str, Any]) -> None:
        self._oauth_tokens[token] = {
            "payload": payload,
            "created_at": time.time()
        }


class RateLimiter:
    """Token bucket rate limiter with per-client limits."""

    def __init__(self, default_config: RateLimitConfig):
        self.default_config = default_config
        self._client_limits: Dict[str, RateLimitConfig] = {}
        self._buckets: Dict[str, Dict[str, float]] = defaultdict(lambda: {
            "tokens": 0.0,
            "last_update": time.time()
        })
        self._lock = asyncio.Lock()

    async def set_client_limit(self, client_id: str, config: RateLimitConfig) -> None:
        async with self._lock:
            self._client_limits[client_id] = config

    async def check_limit(self, client_id: str) -> bool:
        config = self._client_limits.get(client_id, self.default_config)
        bucket = self._buckets[client_id]

        async with self._lock:
            current_time = time.time()
            time_passed = current_time - bucket["last_update"]
            bucket["last_update"] = current_time

            bucket["tokens"] = min(
                config.burst_size,
                bucket["tokens"] + time_passed * config.requests_per_second
            )

            if bucket["tokens"] >= 1.0:
                bucket["tokens"] -= 1.0
                return True
            return False

    async def get_remaining(self, client_id: str) -> float:
        config = self._client_limits.get(client_id, self.default_config)
        bucket = self._buckets[client_id]
        async with self._lock:
            return bucket["tokens"]


class LoadBalancer:
    """Load balancer for distributing requests across engine instances."""

    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN):
        self.strategy = strategy
        self._engines: Dict[str, EngineInstance] = {}
        self._round_robin_index: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    def register_engine(self, engine: EngineInstance) -> None:
        self._engines[engine.id] = engine

    def unregister_engine(self, engine_id: str) -> None:
        if engine_id in self._engines:
            del self._engines[engine_id]

    async def get_engine(self, client_ip: Optional[str] = None, tags: Optional[Set[str]] = None) -> Optional[EngineInstance]:
        if not self._engines:
            return None

        healthy_engines = [e for e in self._engines.values() if e.is_healthy]

        if tags:
            tagged_engines = [e for e in healthy_engines if tags.intersection(e.tags)]
            if tagged_engines:
                healthy_engines = tagged_engines

        if not healthy_engines:
            return None

        async with self._lock:
            if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
                return self._round_robin(healthy_engines)
            elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
                return min(healthy_engines, key=lambda e: e.current_connections)
            elif self.strategy == LoadBalancingStrategy.WEIGHTED:
                return self._weighted(healthy_engines)
            elif self.strategy == LoadBalancingStrategy.RANDOM:
                import random
                return random.choice(healthy_engines)
            elif self.strategy == LoadBalancingStrategy.IP_HASH:
                return self._ip_hash(healthy_engines, client_ip or "")
            return healthy_engines[0]

    def _round_robin(self, engines: List[EngineInstance]) -> EngineInstance:
        index = self._round_robin_index["global"] % len(engines)
        self._round_robin_index["global"] += 1
        return engines[index]

    def _weighted(self, engines: List[EngineInstance]) -> EngineInstance:
        total_weight = sum(e.weight for e in engines)
        import random
        r = random.randint(1, total_weight)
        cumulative = 0
        for engine in engines:
            cumulative += engine.weight
            if r <= cumulative:
                return engine
        return engines[-1]

    def _ip_hash(self, engines: List[EngineInstance], client_ip: str) -> EngineInstance:
        hash_val = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        return engines[hash_val % len(engines)]


class WorkflowCache:
    """Cache for workflow results."""

    def __init__(self, default_ttl: float = 300.0, max_size: int = 1000):
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._cache: Dict[str, WorkflowCacheEntry] = {}
        self._access_order: List[str] = []
        self._lock = asyncio.Lock()
        self._redis_client: Optional[Any] = None
        self._use_redis = False

    def enable_redis(self, redis_url: str) -> bool:
        if not REDIS_AVAILABLE:
            logger.warning("Redis not available")
            return False
        try:
            self._redis_client = redis.from_url(redis_url)
            self._use_redis = True
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

    async def get(self, key: str, version: Optional[str] = None) -> Optional[Any]:
        if self._use_redis and self._redis_client:
            return self._get_redis(key, version)

        async with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]
            current_time = time.time()

            if entry.is_expired(current_time):
                del self._cache[key]
                return None

            if version and entry.version != version:
                return None

            self._access_order.remove(key)
            self._access_order.append(key)
            return entry.result

    async def set(self, key: str, value: Any, ttl: Optional[float] = None,
                  version: Optional[str] = None, tags: Optional[Set[str]] = None) -> None:
        if self._use_redis and self._redis_client:
            await self._set_redis(key, value, ttl, version)
            return

        async with self._lock:
            if len(self._cache) >= self.max_size and key not in self._cache:
                oldest = self._access_order.pop(0)
                del self._cache[oldest]

            self._cache[key] = WorkflowCacheEntry(
                workflow_id=key,
                result=value,
                cached_at=time.time(),
                ttl=ttl or self.default_ttl,
                version=version or "v1",
                tags=tags or set()
            )
            self._access_order.append(key)

    async def invalidate(self, key: str) -> None:
        if self._use_redis and self._redis_client:
            return await self._invalidate_redis(key)

        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._access_order.remove(key)

    async def invalidate_by_tag(self, tag: str) -> None:
        async with self._lock:
            keys_to_remove = [
                k for k, v in self._cache.items() if tag in v.tags
            ]
            for key in keys_to_remove:
                del self._cache[key]
                self._access_order.remove(key)

    def _get_redis(self, key: str, version: Optional[str]) -> Optional[Any]:
        try:
            data = self._redis_client.get(f"workflow_cache:{key}")
            if data:
                entry = json.loads(data)
                if version and entry.get("version") != version:
                    return None
                return entry.get("result")
        except Exception as e:
            logger.error(f"Redis get error: {e}")
        return None

    async def _set_redis(self, key: str, value: Any, ttl: Optional[float], version: Optional[str]) -> None:
        try:
            entry = {"result": value, "version": version or "v1"}
            self._redis_client.setex(
                f"workflow_cache:{key}",
                ttl or self.default_ttl,
                json.dumps(entry)
            )
        except Exception as e:
            logger.error(f"Redis set error: {e}")

    async def _invalidate_redis(self, key: str) -> None:
        try:
            self._redis_client.delete(f"workflow_cache:{key}")
        except Exception as e:
            logger.error(f"Redis invalidate error: {e}")


class MetricsCollector:
    """Collects and tracks metrics per client and endpoint."""

    def __init__(self):
        self._client_metrics: Dict[str, ClientMetrics] = defaultdict(ClientMetrics)
        self._endpoint_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_latency": 0.0,
            "avg_latency": 0.0,
            "rate_limit_hits": 0
        })
        self._engine_metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "total_requests": 0,
            "current_connections": 0,
            "avg_response_time": 0.0
        })
        self._lock = asyncio.Lock()

    async def record_request(self, client_id: str, endpoint: str,
                             latency: float, success: bool, engine_id: str) -> None:
        async with self._lock:
            client = self._client_metrics[client_id]
            client.total_requests += 1
            client.total_latency += latency

            if success:
                client.successful_requests += 1
            else:
                client.failed_requests += 1

            if endpoint not in client.endpoint_metrics:
                client.endpoint_metrics[endpoint] = {
                    "total": 0, "success": 0, "failure": 0, "latency": 0.0
                }
            ep = client.endpoint_metrics[endpoint]
            ep["total"] += 1
            ep["latency"] += latency
            if success:
                ep["success"] += 1
            else:
                ep["failure"] += 1

            self._endpoint_metrics[endpoint]["total_requests"] += 1
            self._endpoint_metrics[endpoint]["total_latency"] += latency
            if success:
                self._endpoint_metrics[endpoint]["successful_requests"] += 1
            else:
                self._endpoint_metrics[endpoint]["failed_requests"] += 1

            self._engine_metrics[engine_id]["total_requests"] += 1
            self._engine_metrics[engine_id]["avg_response_time"] = (
                (self._engine_metrics[engine_id]["avg_response_time"] * 0.7) + (latency * 0.3)
            )

    async def record_rate_limit_hit(self, client_id: str) -> None:
        async with self._lock:
            self._client_metrics[client_id].rate_limit_hits += 1
            for ep in self._endpoint_metrics.values():
                ep["rate_limit_hits"] = ep.get("rate_limit_hits", 0) + 1

    async def record_auth_failure(self, client_id: str) -> None:
        async with self._lock:
            self._client_metrics[client_id].auth_failures += 1

    async def get_client_metrics(self, client_id: str) -> Dict[str, Any]:
        async with self._lock:
            client = self._client_metrics.get(client_id)
            if not client:
                return {}
            return {
                "total_requests": client.total_requests,
                "successful_requests": client.successful_requests,
                "failed_requests": client.failed_requests,
                "avg_latency": client.total_latency / client.total_requests if client.total_requests else 0,
                "rate_limit_hits": client.rate_limit_hits,
                "auth_failures": client.auth_failures,
                "endpoints": client.endpoint_metrics
            }

    async def get_endpoint_metrics(self, endpoint: str) -> Dict[str, Any]:
        async with self._lock:
            ep = self._endpoint_metrics.get(endpoint)
            if not ep:
                return {}
            return {
                "total_requests": ep["total_requests"],
                "successful_requests": ep["successful_requests"],
                "failed_requests": ep["failed_requests"],
                "avg_latency": ep["total_latency"] / ep["total_requests"] if ep["total_requests"] else 0,
                "rate_limit_hits": ep.get("rate_limit_hits", 0)
            }

    async def get_all_metrics(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "clients": dict(self._client_metrics),
                "endpoints": dict(self._endpoint_metrics),
                "engines": dict(self._engine_metrics)
            }


class WebSocketManager:
    """Manages WebSocket connections for real-time updates."""

    def __init__(self):
        self._connections: Dict[str, List[WebSocket]] = defaultdict(list)
        self._client_subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._max_connections_per_client = 10

    async def connect(self, client_id: str, websocket: WebSocket) -> bool:
        async with self._lock:
            client_connections = self._connections.get(client_id, [])
            if len(client_connections) >= self._max_connections_per_client:
                return False

            await websocket.accept()
            self._connections[client_id].append(websocket)
            return True

    async def disconnect(self, client_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            if client_id in self._connections:
                try:
                    self._connections[client_id].remove(websocket)
                    if not self._connections[client_id]:
                        del self._connections[client_id]
                except ValueError:
                    pass

    async def subscribe(self, client_id: str, workflow_id: str) -> None:
        async with self._lock:
            self._client_subscriptions[client_id].add(workflow_id)

    async def unsubscribe(self, client_id: str, workflow_id: str) -> None:
        async with self._lock:
            if client_id in self._client_subscriptions:
                self._client_subscriptions[client_id].discard(workflow_id)

    async def broadcast_to_workflow(self, workflow_id: str, message: Dict[str, Any]) -> None:
        async with self._lock:
            for client_id, subscriptions in self._client_subscriptions.items():
                if workflow_id in subscriptions:
                    await self._send_to_client(client_id, message)

    async def broadcast_all(self, message: Dict[str, Any]) -> None:
        async with self._lock:
            for client_id in self._connections:
                await self._send_to_client(client_id, message)

    async def _send_to_client(self, client_id: str, message: Dict[str, Any]) -> None:
        if client_id in self._connections:
            dead_connections = []
            for ws in self._connections[client_id]:
                try:
                    await ws.send_json(message)
                except Exception:
                    dead_connections.append(ws)

            for ws in dead_connections:
                try:
                    self._connections[client_id].remove(ws)
                except ValueError:
                    pass


class RequestRouter:
    """Routes requests based on workflow ID, tags, or headers."""

    def __init__(self):
        self._routes: Dict[str, Dict[str, Any]] = {}
        self._tag_routes: Dict[str, List[str]] = defaultdict(list)
        self._header_routes: Dict[str, Dict[str, str]] = {}
        self._default_engine: Optional[str] = None
        self._lock = asyncio.Lock()

    async def add_route(self, pattern: str, engine_id: str,
                        tags: Optional[List[str]] = None,
                        metadata: Optional[Dict[str, Any]] = None) -> None:
        async with self._lock:
            self._routes[pattern] = {
                "engine_id": engine_id,
                "tags": set(tags) if tags else set(),
                "metadata": metadata or {}
            }
            if tags:
                for tag in tags:
                    self._tag_routes[tag].append(pattern)

    async def add_header_route(self, header_name: str, header_value: str, engine_id: str) -> None:
        async with self._lock:
            key = f"{header_name}:{header_value}"
            self._header_routes[key] = {"header_name": header_name, "header_value": header_value, "engine_id": engine_id}

    async def set_default_engine(self, engine_id: str) -> None:
        async with self._lock:
            self._default_engine = engine_id

    async def route(self, workflow_id: Optional[str] = None,
                    tags: Optional[Set[str]] = None,
                    headers: Optional[Dict[str, str]] = None) -> Optional[str]:
        async with self._lock:
            if workflow_id:
                for pattern, route in self._routes.items():
                    if self._match_pattern(workflow_id, pattern):
                        return route["engine_id"]

            if tags:
                for tag in tags:
                    if tag in self._tag_routes:
                        patterns = self._tag_routes[tag]
                        if patterns:
                            return self._routes[patterns[0]]["engine_id"]

            if headers:
                for key, route in self._header_routes.items():
                    header_name = route["header_name"]
                    header_value = route["header_value"]
                    if headers.get(header_name) == header_value:
                        return route["engine_id"]

            return self._default_engine

    def _match_pattern(self, workflow_id: str, pattern: str) -> bool:
        if "*" in pattern:
            import fnmatch
            return fnmatch.fnmatch(workflow_id, pattern)
        return workflow_id == pattern


class WorkflowAPIGateway:
    """
    API Gateway for managing multiple workflow engines with support for:
    - Multi-engine routing
    - Load balancing
    - Authentication (API key and OAuth)
    - Rate limiting
    - Circuit breaker
    - Caching
    - Metrics
    - WebSocket
    - API versioning
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        config = config or {}

        self.api_versions = config.get("api_versions", ["v1", "v2"])
        self.current_version = config.get("current_version", "v1")

        self.auth_provider = SimpleAuthProvider()
        self.rate_limiter = RateLimiter(
            default_config=RateLimitConfig(
                requests_per_second=config.get("requests_per_second", 100),
                burst_size=config.get("burst_size", 200)
            )
        )
        self.load_balancer = LoadBalancer(
            strategy=LoadBalancingStrategy(config.get("load_balancing_strategy", "round_robin"))
        )
        self.cache = WorkflowCache(
            default_ttl=config.get("cache_ttl", 300),
            max_size=config.get("cache_max_size", 1000)
        )
        self.metrics = MetricsCollector()
        self.ws_manager = WebSocketManager()
        self.router = RequestRouter()

        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.circuit_config = CircuitBreakerConfig(
            failure_threshold=config.get("circuit_failure_threshold", 5),
            success_threshold=config.get("circuit_success_threshold", 2),
            timeout=config.get("circuit_timeout", 60.0)
        )

        self._engine_request_counts: Dict[str, int] = defaultdict(int)
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._running = False
        self._lock = asyncio.Lock()

        if config.get("redis_url"):
            self.cache.enable_redis(config["redis_url"])

    async def start(self) -> None:
        """Start the gateway."""
        self._running = True
        logger.info("Workflow API Gateway started")

    async def stop(self) -> None:
        """Stop the gateway."""
        self._running = False
        self._executor.shutdown(wait=True)
        logger.info("Workflow API Gateway stopped")

    def register_engine(self, engine: EngineInstance) -> None:
        """Register a workflow engine instance."""
        self.load_balancer.register_engine(engine)
        self.circuit_breakers[engine.id] = CircuitBreaker(self.circuit_config)
        logger.info(f"Registered engine: {engine.name} ({engine.id})")

    def unregister_engine(self, engine_id: str) -> None:
        """Unregister a workflow engine instance."""
        self.load_balancer.unregister_engine(engine_id)
        if engine_id in self.circuit_breakers:
            del self.circuit_breakers[engine_id]
        logger.info(f"Unregistered engine: {engine_id}")

    async def authenticate(self, request: Request) -> Optional[str]:
        """
        Authenticate a request using API key or OAuth.
        Returns client_id if authenticated, None otherwise.
        """
        api_key = request.headers.get("X-API-Key")
        auth_header = request.headers.get("Authorization")

        if api_key:
            client_id = request.headers.get("X-Client-ID", "unknown")
            if self.auth_provider.validate_api_key(api_key, client_id):
                return client_id
            await self.metrics.record_auth_failure(client_id)
            return None

        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:]
            payload = self.auth_provider.validate_oauth(token)
            if payload:
                return payload.get("sub", payload.get("client_id", "unknown"))
            await self.metrics.record_auth_failure("oauth")
            return None

        return None

    async def check_rate_limit(self, client_id: str) -> bool:
        """Check if client is within rate limits."""
        allowed = await self.rate_limiter.check_limit(client_id)
        if not allowed:
            await self.metrics.record_rate_limit_hit(client_id)
        return allowed

    async def route_request(self, workflow_id: Optional[str],
                           tags: Optional[Set[str]] = None,
                           headers: Optional[Dict[str, str]] = None) -> Optional[EngineInstance]:
        """Route request to appropriate engine."""
        engine_id = await self.router.route(workflow_id, tags, headers)

        if not engine_id:
            engine = await self.load_balancer.get_engine(
                client_ip=headers.get("X-Forwarded-For") if headers else None,
                tags=tags
            )
            return engine

        return self.load_balancer._engines.get(engine_id)

    async def execute_with_circuit_breaker(self, engine_id: str,
                                           func: Callable) -> Any:
        """Execute a function with circuit breaker protection."""
        breaker = self.circuit_breakers.get(engine_id)
        if not breaker:
            return await func()

        if not await breaker.can_execute():
            raise HTTPException(status_code=503, detail="Service temporarily unavailable")

        await breaker.on_execute()

        try:
            result = await func()
            await breaker.record_success()
            return result
        except Exception as e:
            await breaker.record_failure()
            raise

    async def handle_request(self, request: Request, workflow_id: Optional[str] = None,
                            use_cache: bool = True) -> Response:
        """
        Main request handler.
        """
        if not self._running:
            raise HTTPException(status_code=503, detail="Gateway not running")

        client_id = await self.authenticate(request)
        if not client_id:
            return JSONResponse(
                status_code=401,
                content={"error": "Unauthorized"}
            )

        if not await self.check_rate_limit(client_id):
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded"}
            )

        tags = set(request.headers.get("X-Tags", "").split(",")) if request.headers.get("X-Tags") else None
        headers = dict(request.headers)

        cache_key = f"{workflow_id}:{client_id}" if workflow_id else None
        if use_cache and cache_key:
            cached = await self.cache.get(cache_key, self.current_version)
            if cached:
                return JSONResponse(content={"data": cached, "cached": True})

        engine = await self.route_request(workflow_id, tags, headers)
        if not engine:
            return JSONResponse(
                status_code=404,
                content={"error": "No engine available"}
            )

        start_time = time.time()

        async def call_engine():
            return {"status": "success", "engine": engine.name, "workflow_id": workflow_id}

        try:
            result = await self.execute_with_circuit_breaker(
                engine.id,
                call_engine
            )

            latency = time.time() - start_time
            await self.metrics.record_request(client_id, request.url.path, latency, True, engine.id)

            if use_cache and cache_key:
                await self.cache.set(cache_key, result, version=self.current_version)

            return JSONResponse(content={"data": result, "cached": False})
        except HTTPException:
            raise
        except Exception as e:
            latency = time.time() - start_time
            await self.metrics.record_request(client_id, request.url.path, latency, False, engine.id)
            return JSONResponse(
                status_code=500,
                content={"error": str(e)}
            )

    async def handle_websocket(self, websocket: WebSocket, client_id: str) -> None:
        """Handle WebSocket connection."""
        connected = await self.ws_manager.connect(client_id, websocket)
        if not connected:
            await websocket.close(code=1008, reason="Too many connections")
            return

        try:
            while True:
                data = await websocket.receive_json()
                action = data.get("action")

                if action == "subscribe":
                    workflow_id = data.get("workflow_id")
                    if workflow_id:
                        await self.ws_manager.subscribe(client_id, workflow_id)
                        await websocket.send_json({"status": "subscribed", "workflow_id": workflow_id})

                elif action == "unsubscribe":
                    workflow_id = data.get("workflow_id")
                    if workflow_id:
                        await self.ws_manager.unsubscribe(client_id, workflow_id)
                        await websocket.send_json({"status": "unsubscribed", "workflow_id": workflow_id})

                elif action == "ping":
                    await websocket.send_json({"status": "pong"})

        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            await self.ws_manager.disconnect(client_id, websocket)

    async def broadcast_workflow_update(self, workflow_id: str, update: Dict[str, Any]) -> None:
        """Broadcast workflow update to subscribed clients."""
        await self.ws_manager.broadcast_to_workflow(workflow_id, {
            "type": "workflow_update",
            "workflow_id": workflow_id,
            "data": update
        })

    async def get_metrics(self, client_id: Optional[str] = None) -> Dict[str, Any]:
        """Get metrics."""
        if client_id:
            return await self.metrics.get_client_metrics(client_id)
        return await self.metrics.get_all_metrics()

    async def health_check(self) -> Dict[str, Any]:
        """Health check endpoint."""
        healthy_engines = [
            {
                "id": e.id,
                "name": e.name,
                "is_healthy": e.is_healthy,
                "current_connections": e.current_connections
            }
            for e in self.load_balancer._engines.values()
        ]

        return {
            "status": "healthy" if self._running else "unhealthy",
            "engines": healthy_engines,
            "circuit_breakers": {
                eid: {"state": cb.state.value, "failures": cb.failure_count}
                for eid, cb in self.circuit_breakers.items()
            }
        }


def create_gateway(config: Optional[Dict[str, Any]] = None) -> WorkflowAPIGateway:
    """Factory function to create a gateway instance."""
    return WorkflowAPIGateway(config)


if __name__ == "__main__":
    gateway = create_gateway({
        "api_versions": ["v1", "v2"],
        "current_version": "v1",
        "requests_per_second": 100,
        "burst_size": 200,
        "cache_ttl": 300,
        "load_balancing_strategy": "round_robin"
    })

    engine1 = EngineInstance(
        id="engine-1",
        name="Primary Engine",
        url="http://localhost:8001",
        weight=2,
        tags={"production", "high-priority"}
    )

    engine2 = EngineInstance(
        id="engine-2",
        name="Secondary Engine",
        url="http://localhost:8002",
        weight=1,
        tags={"production"}
    )

    gateway.register_engine(engine1)
    gateway.register_engine(engine2)

    gateway.auth_provider.register_api_key("client-1", "test-api-key-123")

    import asyncio

    async def test():
        await gateway.start()

        router = RequestRouter()
        await router.add_route("workflow-*", "engine-1", tags=["production"])
        await router.set_default_engine("engine-2")

        print("Gateway configured and running")
        print(f"Engines: {list(gateway.load_balancer._engines.keys())}")

        await gateway.stop()

    asyncio.run(test())

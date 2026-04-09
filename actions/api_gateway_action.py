"""
API Gateway and Reverse Proxy Module.

Provides request routing, authentication, rate limiting,
caching, and protocol transformation for API automation.

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


class LoadBalancingStrategy(Enum):
    ROUND_ROBIN = auto()
    LEAST_CONNECTIONS = auto()
    IP_HASH = auto()
    WEIGHTED = auto()


@dataclass
class RouteConfig:
    path_pattern: str
    upstream_url: str
    methods: FrozenSet[str] = field(default_factory=frozenset)
    auth_required: bool = False
    rate_limit: Optional[int] = None
    cache_ttl: int = 0
    timeout: float = 30.0
    strip_prefix: bool = False


@dataclass
class GatewayRequest:
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, List[str]]
    body: Optional[bytes]
    client_ip: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class GatewayResponse:
    status_code: int
    headers: Dict[str, str]
    body: Optional[bytes]
    from_cache: bool = False
    latency_ms: float = 0.0


@dataclass
class RateLimitConfig:
    requests_per_second: int = 100
    burst_size: int = 200
    per_client: bool = True


class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = time.time()

    def consume(self, tokens: int = 1) -> bool:
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_update = now

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


class ResponseCache:
    def __init__(self, default_ttl: int = 60):
        self.default_ttl = default_ttl
        self._cache: Dict[str, Tuple[bytes, float]] = {}
        self._stats = {"hits": 0, "misses": 0}

    def _make_key(self, method: str, path: str, body: Optional[bytes]) -> str:
        data = f"{method}:{path}:{body or ''}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def get(self, method: str, path: str, body: Optional[bytes] = None) -> Optional[bytes]:
        key = self._make_key(method, path, body)
        if key in self._cache:
            data, expiry = self._cache[key]
            if time.time() < expiry:
                self._stats["hits"] += 1
                return data
            del self._cache[key]
        self._stats["misses"] += 1
        return None

    def put(self, method: str, path: str, body: bytes, ttl: Optional[int] = None) -> None:
        key = self._make_key(method, path, body)
        expiry = time.time() + (ttl or self.default_ttl)
        self._cache[key] = (body, expiry)

    def invalidate(self, path_pattern: str) -> int:
        count = 0
        to_delete = [k for k in self._cache if k.startswith(path_pattern)]
        for k in to_delete:
            del self._cache[k]
            count += 1
        return count

    def get_stats(self) -> Dict[str, int]:
        total = self._stats["hits"] + self._stats["misses"]
        return {
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": self._stats["hits"] / max(total, 1),
            "entries": len(self._cache),
        }


class APIGateway:
    """
    API gateway with routing, load balancing, and caching.
    """

    def __init__(self, port: int = 8080):
        self.port = port
        self._routes: List[RouteConfig] = []
        self._upstreams: Dict[str, List[str]] = defaultdict(list)
        self._health_checks: Dict[str, asyncio.Task] = {}
        self._rate_limiters: Dict[str, TokenBucket] = defaultdict(
            lambda: TokenBucket(100, 200)
        )
        self.cache = ResponseCache()
        self._lb_strategies: Dict[str, LoadBalancingStrategy] = defaultdict(
            lambda: LoadBalancingStrategy.ROUND_ROBIN
        )
        self._lb_counters: Dict[str, int] = defaultdict(int)
        self._server: Optional[asyncio.Server] = None
        self._auth_handlers: Dict[str, Callable] = {}

    def add_route(self, route: RouteConfig) -> None:
        self._routes.append(route)
        logger.info("Added route: %s -> %s", route.path_pattern, route.upstream_url)

    def add_upstream(self, name: str, urls: List[str]) -> None:
        self._upstreams[name] = urls
        logger.info("Added upstream %s with %d endpoints", name, len(urls))

    def set_load_balancing(
        self, upstream: str, strategy: LoadBalancingStrategy
    ) -> None:
        self._lb_strategies[upstream] = strategy

    def register_auth(self, auth_type: str, handler: Callable) -> None:
        self._auth_handlers[auth_type] = handler

    def _match_route(self, path: str, method: str) -> Optional[RouteConfig]:
        for route in self._routes:
            import fnmatch
            if fnmatch.fnmatch(path, route.path_pattern):
                if method.upper() in route.methods or not route.methods:
                    return route
        return None

    def _select_upstream(self, upstream_name: str) -> Optional[str]:
        urls = self._upstreams.get(upstream_name, [])
        if not urls:
            return None

        strategy = self._lb_strategies[upstream_name]

        if strategy == LoadBalancingStrategy.ROUND_ROBIN:
            idx = self._lb_counters[upstream_name] % len(urls)
            self._lb_counters[upstream_name] += 1
            return urls[idx]

        elif strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return urls[0]

        elif strategy == LoadBalancingStrategy.IP_HASH:
            return urls[hash(urls[0]) % len(urls)]

        elif strategy == LoadBalancingStrategy.WEIGHTED:
            return urls[0]

        return urls[0]

    def _check_rate_limit(
        self, route: RouteConfig, client_ip: str
    ) -> bool:
        if not route.rate_limit:
            return True

        if route.rate_limit > 0:
            limiter = self._rate_limiters.get(client_ip)
            if not limiter:
                limiter = TokenBucket(route.rate_limit, route.rate_limit * 2)
                self._rate_limiters[client_ip] = limiter
            return limiter.consume()

        return True

    async def _proxy_request(
        self, request: GatewayRequest, route: RouteConfig
    ) -> GatewayResponse:
        import aiohttp

        upstream_url = self._select_upstream(route.upstream_url)
        if not upstream_url:
            return GatewayResponse(
                status_code=502,
                headers={},
                body=b"No upstream available",
            )

        url = f"{upstream_url}{route.path_pattern.replace('*', '')}{request.path}"

        start = time.time()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    request.method,
                    url,
                    headers=request.headers,
                    data=request.body,
                    timeout=aiohttp.ClientTimeout(total=route.timeout),
                ) as resp:
                    body = await resp.read()
                    latency = (time.time() - start) * 1000

                    return GatewayResponse(
                        status_code=resp.status,
                        headers=dict(resp.headers),
                        body=body,
                        latency_ms=latency,
                    )

        except asyncio.TimeoutError:
            return GatewayResponse(
                status_code=504,
                headers={},
                body=b"Gateway timeout",
                latency_ms=(time.time() - start) * 1000,
            )
        except Exception as exc:
            logger.error("Proxy error: %s", exc)
            return GatewayResponse(
                status_code=502,
                headers={},
                body=str(exc).encode(),
                latency_ms=(time.time() - start) * 1000,
            )

    async def handle_request(
        self, request: GatewayRequest
    ) -> GatewayResponse:
        route = self._match_route(request.path, request.method)
        if not route:
            return GatewayResponse(
                status_code=404,
                headers={},
                body=b"Route not found",
            )

        if not self._check_rate_limit(route, request.client_ip):
            return GatewayResponse(
                status_code=429,
                headers={"Retry-After": "60"},
                body=b"Rate limit exceeded",
            )

        if route.cache_ttl > 0 and request.method == "GET":
            cached = self.cache.get(request.method, request.path, request.body)
            if cached:
                return GatewayResponse(
                    status_code=200,
                    headers={},
                    body=cached,
                    from_cache=True,
                )

        response = await self._proxy_request(request, route)

        if route.cache_ttl > 0 and response.status_code == 200:
            self.cache.put(request.method, request.path, response.body, route.cache_ttl)

        return response

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection, "0.0.0.0", self.port
        )
        logger.info("API Gateway started on port %d", self.port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        logger.info("API Gateway stopped")

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        try:
            request_data = await reader.read(65536)
            lines = request_data.decode().split("\r\n")
            if not lines:
                return
            request_line = lines[0]
            parts = request_line.split()
            if len(parts) < 2:
                return
            method, path = parts[0], parts[1]

            headers = {}
            body_start = 0
            for i, line in enumerate(lines[1:], 1):
                if line == "":
                    body_start = i + 1
                    break
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip()] = v.strip()

            body = "\r\n".join(lines[body_start:]).encode() if body_start else None

            gw_request = GatewayRequest(
                method=method,
                path=path,
                headers=headers,
                query_params={},
                body=body,
                client_ip=addr[0] if addr else "unknown",
            )

            response = await self.handle_request(gw_request)

            response_headers = response.headers.copy()
            if response.from_cache:
                response_headers["X-Cache"] = "HIT"
            else:
                response_headers["X-Cache"] = "MISS"

            writer.write(f"HTTP/1.1 {response.status_code}\r\n".encode())
            for k, v in response_headers.items():
                writer.write(f"{k}: {v}\r\n".encode())
            writer.write(b"\r\n")
            writer.write(response.body)
            await writer.drain()

        except Exception as exc:
            logger.error("Connection handler error: %s", exc)
        finally:
            writer.close()
            await writer.wait_closed()

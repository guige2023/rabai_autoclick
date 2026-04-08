"""
API Client Pool Action Module.

Manages a pool of HTTP clients with connection pooling,
load balancing, and automatic retry on connection failure.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import asyncio
import httpx
import time
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class PoolConfig:
    """Client pool configuration."""
    min_size: int = 5
    max_size: int = 20
    max_keepalive_connections: int = 10
    keepalive_expiry: float = 30.0
    timeout: float = 30.0
    retries: int = 2


@dataclass
class PoolStats:
    """Pool statistics."""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    requests_total: int = 0
    requests_success: int = 0
    requests_failed: int = 0
    avg_response_time_ms: float = 0.0


class APIClientPoolAction:
    """
    HTTP client pool with connection reuse and load balancing.

    Manages a pool of httpx AsyncClients with automatic
    connection health management and retry logic.

    Example:
        pool = APIClientPoolAction(max_size=10)
        async with pool.get_client() as client:
            response = await client.get("https://api.example.com/data")
    """

    def __init__(self, config: Optional[PoolConfig] = None) -> None:
        self.config = config or PoolConfig()
        self._clients: list[httpx.AsyncClient] = []
        self._active: set[int] = set()
        self._stats = PoolStats()
        self._lock = asyncio.Lock()
        self._created = 0

    async def initialize(self) -> None:
        """Pre-initialize client pool."""
        async with self._lock:
            for _ in range(self.config.min_size):
                client = await self._create_client()
                self._clients.append(client)
                self._created += 1

        logger.info("Client pool initialized with %d connections", self.config.min_size)

    async def _create_client(self) -> httpx.AsyncClient:
        """Create a new HTTP client with pool settings."""
        limits = httpx.Limits(
            max_connections=self.config.max_size,
            max_keepalive_connections=self.config.max_keepalive_connections,
            keepalive_expiry=self.config.keepalive_expiry,
        )

        transport = httpx.AsyncHTTPTransport(retries=self.config.retries)

        return httpx.AsyncClient(
            limits=limits,
            timeout=httpx.Timeout(self.config.timeout),
            transport=transport,
        )

    @asynccontextmanager
    async def get_client(self):
        """Get an available client from the pool."""
        client_id = id(time.time())

        async with self._lock:
            if self._clients:
                client = self._clients.pop()
            else:
                client = await self._create_client()
                self._created += 1

            self._active.add(client_id)
            self._stats.active_connections += 1
            self._stats.total_connections += 1

        start_time = time.perf_counter()

        try:
            yield client
            self._stats.requests_success += 1
        except Exception as e:
            self._stats.requests_failed += 1
            logger.error("Request failed: %s", e)
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            self._update_avg_response_time(elapsed_ms)

            async with self._lock:
                self._active.discard(client_id)
                self._stats.active_connections -= 1
                self._stats.idle_connections += 1

                if len(self._clients) < self.config.max_size:
                    self._clients.append(client)
                else:
                    await client.aclose()

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make an HTTP request using a pooled client."""
        async with self.get_client() as client:
            response = await client.request(method, url, **kwargs)
            self._stats.requests_total += 1
            return response

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        """Convenience GET method."""
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        """Convenience POST method."""
        return await self.request("POST", url, **kwargs)

    async def close(self) -> None:
        """Close all clients in the pool."""
        async with self._lock:
            for client in self._clients:
                await client.aclose()
            self._clients.clear()
            self._active.clear()

        logger.info("Client pool closed")

    def _update_avg_response_time(self, elapsed_ms: float) -> None:
        """Update rolling average response time."""
        total = self._stats.requests_total
        if total <= 1:
            self._stats.avg_response_time_ms = elapsed_ms
        else:
            curr_avg = self._stats.avg_response_time_ms
            self._stats.avg_response_time_ms = (
                (curr_avg * (total - 1) + elapsed_ms) / total
            )

    @property
    def stats(self) -> PoolStats:
        """Get current pool statistics."""
        self._stats.idle_connections = len(self._clients)
        return self._stats

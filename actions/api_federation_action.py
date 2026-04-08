"""
API Federation Action Module.

Federated API aggregation that combines data from
multiple API sources into unified responses.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import asyncio
import httpx

logger = logging.getLogger(__name__)


@dataclass
class FederationSource:
    """API source for federation."""
    name: str
    base_url: str
    priority: int = 0
    timeout: float = 10.0
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class FederationResult:
    """Result from federated API call."""
    source_name: str
    success: bool
    data: Any
    latency_ms: float
    error: Optional[str] = None


class APIFederationAction:
    """
    Federated API aggregation across multiple sources.

    Combines responses from multiple APIs,
    supports priority-based fallback and parallel fetching.

    Example:
        fed = APIFederationAction()
        fed.add_source("primary", "https://api.primary.com")
        fed.add_source("backup", "https://api.backup.com", priority=1)
        results = await fed.fetch_all("/users")
    """

    def __init__(
        self,
        default_timeout: float = 10.0,
        parallel: bool = True,
    ) -> None:
        self.default_timeout = default_timeout
        self.parallel = parallel
        self._sources: list[FederationSource] = []

    def add_source(
        self,
        name: str,
        base_url: str,
        priority: int = 0,
        timeout: Optional[float] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> "APIFederationAction":
        """Add an API source to federate."""
        self._sources.append(FederationSource(
            name=name,
            base_url=base_url.rstrip("/"),
            priority=priority,
            timeout=timeout or self.default_timeout,
            headers=headers or {},
        ))
        self._sources.sort(key=lambda s: s.priority)
        return self

    def remove_source(self, name: str) -> bool:
        """Remove a source by name."""
        for i, source in enumerate(self._sources):
            if source.name == name:
                del self._sources[i]
                return True
        return False

    async def fetch_all(
        self,
        path: str,
        method: str = "GET",
        **kwargs: Any,
    ) -> list[FederationResult]:
        """Fetch from all sources in parallel."""
        if not self._sources:
            return []

        tasks = [
            self._fetch_from_source(source, path, method, **kwargs)
            for source in self._sources
        ]

        if self.parallel:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return [r for r in results if isinstance(r, FederationResult)]
        else:
            results = []
            for task in tasks:
                result = await task
                results.append(result)
                if result.success:
                    break
            return results

    async def fetch_first(
        self,
        path: str,
        method: str = "GET",
        **kwargs: Any,
    ) -> Optional[FederationResult]:
        """Fetch from highest priority source."""
        results = await self.fetch_all(path, method, **kwargs)

        for result in results:
            if result.success:
                return result

        return results[0] if results else None

    async def fetch_with_fallback(
        self,
        path: str,
        method: str = "GET",
        **kwargs: Any,
    ) -> Optional[Any]:
        """Fetch with automatic fallback to lower priority sources."""
        for source in self._sources:
            result = await self._fetch_from_source(source, path, method, **kwargs)
            if result.success:
                return result.data

        return None

    async def _fetch_from_source(
        self,
        source: FederationSource,
        path: str,
        method: str,
        **kwargs: Any,
    ) -> FederationResult:
        """Fetch from a single source."""
        import time
        start = time.perf_counter()
        url = f"{source.base_url}{path}"

        try:
            async with httpx.AsyncClient(timeout=source.timeout) as client:
                response = await client.request(
                    method,
                    url,
                    headers=source.headers,
                    **kwargs,
                )
                latency_ms = (time.perf_counter() - start) * 1000

                if response.status_code < 400:
                    return FederationResult(
                        source_name=source.name,
                        success=True,
                        data=response.json() if response.text else None,
                        latency_ms=latency_ms,
                    )
                else:
                    return FederationResult(
                        source_name=source.name,
                        success=False,
                        data=None,
                        latency_ms=latency_ms,
                        error=f"HTTP {response.status_code}",
                    )

        except Exception as e:
            latency_ms = (time.perf_counter() - start) * 1000
            return FederationResult(
                source_name=source.name,
                success=False,
                data=None,
                latency_ms=latency_ms,
                error=str(e),
            )

    @property
    def source_count(self) -> int:
        """Number of configured sources."""
        return len(self._sources)

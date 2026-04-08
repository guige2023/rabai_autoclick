"""
API Aggregator Action - Combines multiple API responses into unified results.

This module provides functionality for aggregating data from multiple API endpoints,
handling response merging, conflict resolution, and data transformation.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol
from enum import Enum
import json


class ConflictStrategy(Enum):
    """Strategy for handling conflicting data from multiple sources."""
    FIRST_WINS = "first_wins"
    LAST_WINS = "last_wins"
    PRIORITY = "priority"
    MERGE = "merge"
    CONFLICT_ERROR = "conflict_error"


@dataclass
class APIEndpoint:
    """Represents a single API endpoint to aggregate."""
    name: str
    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] | None = None
    priority: int = 0
    timeout: float = 30.0
    retry_count: int = 3


@dataclass
class AggregationResult:
    """Result of aggregating multiple API responses."""
    success: bool
    data: Any
    sources: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)
    duration_ms: float = 0.0
    cache_hit: bool = False


class APIClient(Protocol):
    """Protocol for API client implementation."""
    
    async def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]: ...


class SimpleHTTPClient:
    """Simple HTTP client implementation using aiohttp."""
    
    def __init__(self) -> None:
        self._session: Any = None
    
    async def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]:
        try:
            import aiohttp
            timeout_obj = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=body,
                ) as response:
                    data = await response.json()
                    return response.status, data
        except Exception as e:
            return 0, {"error": str(e)}


@dataclass
class CacheEntry:
    """Cache entry for aggregated results."""
    data: Any
    timestamp: float
    ttl: float


class APIAggregatorAction:
    """
    Aggregates data from multiple API endpoints with caching and conflict resolution.
    
    Example:
        aggregator = APIAggregatorAction()
        endpoints = [
            APIEndpoint(name="users", url="https://api.example.com/users"),
            APIEndpoint(name="profiles", url="https://api.example.com/profiles"),
        ]
        result = await aggregator.aggregate(endpoints)
    """
    
    def __init__(
        self,
        client: APIClient | None = None,
        cache_ttl: float = 300.0,
        conflict_strategy: ConflictStrategy = ConflictStrategy.PRIORITY,
    ) -> None:
        self.client = client or SimpleHTTPClient()
        self.cache_ttl = cache_ttl
        self.conflict_strategy = conflict_strategy
        self._cache: dict[str, CacheEntry] = {}
    
    def _get_cache_key(self, endpoints: list[APIEndpoint]) -> str:
        """Generate cache key from endpoints."""
        key_data = json.dumps([{"name": e.name, "url": e.url} for e in endpoints], sort_keys=True)
        return hashlib.sha256(key_data.encode()).hexdigest()
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cache entry is still valid."""
        if key not in self._cache:
            return False
        entry = self._cache[key]
        return (time.time() - entry.timestamp) < entry.ttl
    
    async def aggregate(
        self,
        endpoints: list[APIEndpoint],
        use_cache: bool = True,
        parallel: bool = True,
    ) -> AggregationResult:
        """
        Aggregate data from multiple API endpoints.
        
        Args:
            endpoints: List of API endpoints to call
            use_cache: Whether to use cached results
            parallel: Whether to call endpoints in parallel
            
        Returns:
            AggregationResult with combined data from all endpoints
        """
        start_time = time.time()
        cache_key = self._get_cache_key(endpoints)
        
        if use_cache and self._is_cache_valid(cache_key):
            cached = self._cache[cache_key]
            return AggregationResult(
                success=True,
                data=cached.data,
                sources=[e.name for e in endpoints],
                duration_ms=(time.time() - start_time) * 1000,
                cache_hit=True,
            )
        
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        
        if parallel:
            tasks = [self._call_endpoint(e) for e in endpoints]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for endpoint, response in zip(endpoints, responses):
                if isinstance(response, Exception):
                    errors[endpoint.name] = str(response)
                else:
                    status, data = response
                    if status == 200:
                        results[endpoint.name] = data
                    else:
                        errors[endpoint.name] = f"HTTP {status}: {data}"
        else:
            for endpoint in endpoints:
                try:
                    status, data = await self._call_endpoint(endpoint)
                    if status == 200:
                        results[endpoint.name] = data
                    else:
                        errors[endpoint.name] = f"HTTP {status}: {data}"
                except Exception as e:
                    errors[endpoint.name] = str(e)
        
        aggregated_data = self._merge_results(results, endpoints)
        
        self._cache[cache_key] = CacheEntry(
            data=aggregated_data,
            timestamp=time.time(),
            ttl=self.cache_ttl,
        )
        
        return AggregationResult(
            success=len(results) > 0,
            data=aggregated_data,
            sources=list(results.keys()),
            errors=errors,
            duration_ms=(time.time() - start_time) * 1000,
        )
    
    async def _call_endpoint(self, endpoint: APIEndpoint) -> tuple[int, Any]:
        """Call a single endpoint."""
        return await self.client.request(
            method=endpoint.method,
            url=endpoint.url,
            headers=endpoint.headers,
            params=endpoint.params,
            body=endpoint.body,
            timeout=endpoint.timeout,
        )
    
    def _merge_results(
        self,
        results: dict[str, Any],
        endpoints: list[APIEndpoint],
    ) -> Any:
        """Merge results from multiple endpoints."""
        if len(results) == 1:
            return list(results.values())[0]
        
        endpoint_map = {e.name: e for e in endpoints}
        
        merged: dict[str, Any] = {"_meta": {"sources": list(results.keys())}}
        
        for name, data in results.items():
            priority = endpoint_map[name].priority
            if isinstance(data, dict):
                for key, value in data.items():
                    merged_key = f"{name}_{key}" if key in merged else key
                    if merged_key in merged and merged[merged_key] != value:
                        merged[merged_key] = self._resolve_conflict(
                            merged[merged_key], value, priority, endpoint_map[name]
                        )
                    else:
                        merged[merged_key] = value
            elif isinstance(data, list):
                if "_items" not in merged:
                    merged["_items"] = []
                merged["_items"].extend(data)
            else:
                merged[name] = data
        
        return merged
    
    def _resolve_conflict(
        self,
        existing: Any,
        new: Any,
        new_priority: int,
        endpoint: APIEndpoint,
    ) -> Any:
        """Resolve conflict between two values."""
        if self.conflict_strategy == ConflictStrategy.FIRST_WINS:
            return existing
        elif self.conflict_strategy == ConflictStrategy.LAST_WINS:
            return new
        elif self.conflict_strategy == ConflictStrategy.PRIORITY:
            return new if new_priority > 0 else existing
        elif self.conflict_strategy == ConflictStrategy.MERGE:
            if isinstance(existing, dict) and isinstance(new, dict):
                return {**existing, **new}
            return new
        else:
            return new
    
    def clear_cache(self) -> None:
        """Clear all cached results."""
        self._cache.clear()


# Export public API
__all__ = [
    "APIEndpoint",
    "AggregationResult",
    "APIAggregatorAction",
    "ConflictStrategy",
    "CacheEntry",
]

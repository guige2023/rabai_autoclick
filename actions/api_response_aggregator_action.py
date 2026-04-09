"""API Response Aggregator Action Module.

Aggregates responses from multiple API sources with:
- Parallel fetching
- Response merging
- Conflict resolution
- Timeout management
- Caching

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class AggregationStrategy(Enum):
    """Response aggregation strategies."""
    FIRST = auto()
    LAST = auto()
    CONCATENATE = auto()
    MERGE = auto()
    RESOLVE_CONFLICT = auto()


@dataclass
class SourceResponse:
    """Response from a single source."""
    source_id: str
    status_code: int
    data: Any
    latency_ms: float
    timestamp: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class AggregationConfig:
    """Configuration for response aggregation."""
    strategy: AggregationStrategy = AggregationStrategy.FIRST
    timeout_seconds: float = 30.0
    max_sources: int = 10
    parallel: bool = True
    cache_results: bool = True
    cache_ttl_seconds: float = 300.0


@dataclass
class AggregatedResponse:
    """Aggregated response result."""
    success: bool
    data: Any
    sources_used: List[str] = field(default_factory=list)
    latency_ms: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class APIResponseAggregator:
    """Aggregates responses from multiple API sources.
    
    Features:
    - Parallel and sequential fetching
    - Multiple aggregation strategies
    - Timeout handling
    - Response caching
    - Conflict resolution
    """
    
    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._metrics = {
            "total_aggregations": 0,
            "successful": 0,
            "failed": 0,
            "cache_hits": 0,
            "total_latency_ms": 0.0
        }
    
    async def aggregate(
        self,
        sources: List[Tuple[str, Callable]],
        params: Optional[Dict[str, Any]] = None,
        strategy: Optional[AggregationStrategy] = None
    ) -> AggregatedResponse:
        """Aggregate responses from multiple sources.
        
        Args:
            sources: List of (source_id, fetch_function) tuples
            params: Parameters to pass to fetch functions
            strategy: Override aggregation strategy
            
        Returns:
            Aggregated response
        """
        self._metrics["total_aggregations"] += 1
        start_time = time.time()
        
        if len(sources) > self.config.max_sources:
            sources = sources[:self.config.max_sources]
        
        cache_key = self._compute_cache_key(sources, params)
        
        if self.config.cache_results:
            cached = await self._get_cached(cache_key)
            if cached:
                self._metrics["cache_hits"] += 1
                return cached
        
        strategy = strategy or self.config.strategy
        
        if self.config.parallel:
            responses = await self._fetch_parallel(sources, params)
        else:
            responses = await self._fetch_sequential(sources, params)
        
        aggregated = await self._merge_responses(responses, strategy)
        
        result = AggregatedResponse(
            success=aggregated["success"],
            data=aggregated["data"],
            sources_used=[r.source_id for r in responses if not r.error],
            latency_ms=(time.time() - start_time) * 1000,
            errors=[r.error for r in responses if r.error]
        )
        
        self._metrics["total_latency_ms"] += result.latency_ms
        
        if result.success:
            self._metrics["successful"] += 1
        else:
            self._metrics["failed"] += 1
        
        if self.config.cache_results:
            await self._set_cached(cache_key, result)
        
        return result
    
    async def _fetch_parallel(
        self,
        sources: List[Tuple[str, Callable]],
        params: Optional[Dict[str, Any]]
    ) -> List[SourceResponse]:
        """Fetch from sources in parallel.
        
        Args:
            sources: List of (source_id, fetch_function) tuples
            params: Parameters to pass
            
        Returns:
            List of source responses
        """
        async def fetch_with_timing(source_id: str, func: Callable) -> SourceResponse:
            start = time.time()
            try:
                if asyncio.iscoroutinefunction(func):
                    if params:
                        data = await asyncio.wait_for(func(params), timeout=self.config.timeout_seconds)
                    else:
                        data = await asyncio.wait_for(func(), timeout=self.config.timeout_seconds)
                else:
                    data = func(params) if params else func()
                
                return SourceResponse(
                    source_id=source_id,
                    status_code=200,
                    data=data,
                    latency_ms=(time.time() - start) * 1000
                )
            except asyncio.TimeoutError:
                return SourceResponse(
                    source_id=source_id,
                    status_code=0,
                    data=None,
                    latency_ms=(time.time() - start) * 1000,
                    error="Timeout"
                )
            except Exception as e:
                return SourceResponse(
                    source_id=source_id,
                    status_code=0,
                    data=None,
                    latency_ms=(time.time() - start) * 1000,
                    error=str(e)
                )
        
        tasks = [fetch_with_timing(sid, func) for sid, func in sources]
        return await asyncio.gather(*tasks)
    
    async def _fetch_sequential(
        self,
        sources: List[Tuple[str, Callable]],
        params: Optional[Dict[str, Any]]
    ) -> List[SourceResponse]:
        """Fetch from sources sequentially.
        
        Args:
            sources: List of (source_id, fetch_function) tuples
            params: Parameters to pass
            
        Returns:
            List of source responses
        """
        responses = []
        
        for source_id, func in sources:
            start = time.time()
            try:
                if asyncio.iscoroutinefunction(func):
                    if params:
                        data = await asyncio.wait_for(func(params), timeout=self.config.timeout_seconds)
                    else:
                        data = await asyncio.wait_for(func(), timeout=self.config.timeout_seconds)
                else:
                    data = func(params) if params else func()
                
                responses.append(SourceResponse(
                    source_id=source_id,
                    status_code=200,
                    data=data,
                    latency_ms=(time.time() - start) * 1000
                ))
            except asyncio.TimeoutError:
                responses.append(SourceResponse(
                    source_id=source_id,
                    status_code=0,
                    data=None,
                    latency_ms=(time.time() - start) * 1000,
                    error="Timeout"
                ))
            except Exception as e:
                responses.append(SourceResponse(
                    source_id=source_id,
                    status_code=0,
                    data=None,
                    latency_ms=(time.time() - start) * 1000,
                    error=str(e)
                ))
        
        return responses
    
    async def _merge_responses(
        self,
        responses: List[SourceResponse],
        strategy: AggregationStrategy
    ) -> Dict[str, Any]:
        """Merge responses using the specified strategy.
        
        Args:
            responses: List of source responses
            strategy: Aggregation strategy
            
        Returns:
            Merged result dictionary
        """
        successful = [r for r in responses if not r.error and r.data is not None]
        
        if not successful:
            return {"success": False, "data": None}
        
        if strategy == AggregationStrategy.FIRST:
            return {"success": True, "data": successful[0].data}
        
        if strategy == AggregationStrategy.LAST:
            return {"success": True, "data": successful[-1].data}
        
        if strategy == AggregationStrategy.CONCATENATE:
            concatenated = []
            for r in successful:
                if isinstance(r.data, list):
                    concatenated.extend(r.data)
                elif isinstance(r.data, dict):
                    concatenated.append(r.data)
                else:
                    concatenated.append(r.data)
            return {"success": True, "data": concatenated}
        
        if strategy == AggregationStrategy.MERGE:
            merged = {}
            for r in successful:
                if isinstance(r.data, dict):
                    merged.update(r.data)
            return {"success": True, "data": merged}
        
        if strategy == AggregationStrategy.RESOLVE_CONFLICT:
            merged = successful[0].data.copy() if isinstance(successful[0].data, dict) else {}
            
            for r in successful[1:]:
                if isinstance(r.data, dict):
                    for key, value in r.data.items():
                        if key not in merged or value != merged[key]:
                            merged[f"{key}_conflicted"] = {
                                "values": [merged.get(key), value],
                                "sources": [successful[0].source_id, r.source_id]
                            }
                            merged[key] = value
            
            return {"success": True, "data": merged}
        
        return {"success": True, "data": successful[0].data}
    
    def _compute_cache_key(
        self,
        sources: List[Tuple[str, Callable]],
        params: Optional[Dict[str, Any]]
    ) -> str:
        """Compute cache key for request.
        
        Args:
            sources: List of sources
            params: Request params
            
        Returns:
            Cache key string
        """
        source_ids = ",".join(sorted(s[0] for s in sources))
        params_str = str(sorted(params.items())) if params else ""
        key_str = f"{source_ids}:{params_str}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    async def _get_cached(self, cache_key: str) -> Optional[AggregatedResponse]:
        """Get cached result.
        
        Args:
            cache_key: Cache key
            
        Returns:
            Cached response or None
        """
        async with self._lock:
            if cache_key in self._cache:
                data, expiry = self._cache[cache_key]
                if time.time() < expiry:
                    return data
                del self._cache[cache_key]
        return None
    
    async def _set_cached(
        self,
        cache_key: str,
        result: AggregatedResponse
    ) -> None:
        """Cache a result.
        
        Args:
            cache_key: Cache key
            result: Result to cache
        """
        async with self._lock:
            self._cache[cache_key] = (
                result,
                time.time() + self.config.cache_ttl_seconds
            )
    
    def clear_cache(self) -> None:
        """Clear the response cache."""
        self._cache.clear()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get aggregator metrics."""
        avg_latency = (
            self._metrics["total_latency_ms"] / self._metrics["total_aggregations"]
            if self._metrics["total_aggregations"] > 0 else 0
        )
        
        return {
            **self._metrics,
            "cache_size": len(self._cache),
            "avg_latency_ms": avg_latency
        }

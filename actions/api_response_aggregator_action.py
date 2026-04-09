"""
API Response Aggregator Action Module

Aggregates responses from multiple API endpoints or requests.
Supports parallel execution, result merging, and conflict resolution.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar, Union
from datetime import datetime

T = TypeVar('T')
R = TypeVar('R')


class AggregationStrategy(Enum):
    """Strategy for aggregating multiple responses."""
    FIRST = "first"
    LAST = "last"
    ALL = "all"
    MERGE = "merge"
    CONCAT = "concat"
    REDUCE = "reduce"
    CUSTOM = "custom"


class ConflictResolution(Enum):
    """How to resolve conflicting values."""
    PREFER_FIRST = "prefer_first"
    PREFER_LAST = "prefer_last"
    PREFER_NEWER = "prefer_newer"
    PREFER_OLDER = "prefer_older"
    CONFLICT_ERROR = "conflict_error"


@dataclass
class AggregationConfig:
    """Configuration for aggregation."""
    strategy: AggregationStrategy = AggregationStrategy.ALL
    conflict_resolution: ConflictResolution = ConflictResolution.PREFER_LAST
    timeout_ms: int = 30000
    max_concurrent: int = 10
    continue_on_error: bool = True


@dataclass
class ApiResponse:
    """Represents an API response."""
    endpoint: str
    status_code: int
    data: Any
    headers: dict[str, str] = field(default_factory=dict)
    response_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        return 200 <= self.status_code < 300 and self.error is None


@dataclass
class AggregatedResult:
    """Result of aggregating multiple responses."""
    responses: list[ApiResponse]
    aggregated_data: Any
    success_count: int = 0
    error_count: int = 0
    total_duration_ms: float = 0.0
    
    @property
    def all_successful(self) -> bool:
        return self.error_count == 0 and self.success_count == len(self.responses)


class ResponseMerger:
    """Merge multiple response data structures."""
    
    @staticmethod
    def merge_dicts(d1: dict, d2: dict, resolution: ConflictResolution) -> dict:
        """Merge two dictionaries."""
        result = d1.copy()
        for key, value in d2.items():
            if key in result:
                if resolution == ConflictResolution.PREFER_FIRST:
                    continue
                elif resolution == ConflictResolution.PREFER_LAST:
                    result[key] = value
                elif resolution == ConflictResolution.PREFER_NEWER:
                    if isinstance(value, dict) and "timestamp" in value:
                        result[key] = value
                elif resolution == ConflictResolution.CONFLICT_ERROR:
                    raise ValueError(f"Conflict on key: {key}")
            else:
                result[key] = value
        return result
    
    @staticmethod
    def merge_lists(l1: list, l2: list) -> list:
        """Concatenate two lists."""
        return l1 + l2
    
    @staticmethod
    def merge_values(v1: Any, v2: Any, resolution: ConflictResolution) -> Any:
        """Merge two values."""
        if isinstance(v1, dict) and isinstance(v2, dict):
            return ResponseMerger.merge_dicts(v1, v2, resolution)
        elif isinstance(v1, list) and isinstance(v2, list):
            return ResponseMerger.merge_lists(v1, v2)
        else:
            if v1 == v2:
                return v1
            if resolution in [ConflictResolution.PREFER_FIRST]:
                return v1
            return v2


class ApiResponseAggregator:
    """
    Aggregates responses from multiple API calls.
    
    Example:
        aggregator = ApiResponseAggregator()
        
        result = await aggregator.aggregate([
            lambda: api.get_user(1),
            lambda: api.get_user(2),
            lambda: api.get_user(3),
        ])
    """
    
    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._history: deque[AggregatedResult] = deque(maxlen=100)
        self._stats = {
            "total_aggregations": 0,
            "successful_aggregations": 0,
            "failed_aggregations": 0,
            "total_responses": 0
        }
    
    async def aggregate(
        self,
        requests: list[Callable[[], Any]],
        strategy: Optional[AggregationStrategy] = None,
        merger: Optional[Callable[[list[Any]], Any]] = None
    ) -> AggregatedResult:
        """
        Aggregate responses from multiple requests.
        
        Args:
            requests: List of request functions to call
            strategy: Aggregation strategy override
            merger: Custom merge function
            
        Returns:
            AggregatedResult with combined data
        """
        start_time = time.time()
        self._stats["total_aggregations"] += 1
        
        strategy = strategy or self.config.strategy
        
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        async def execute_with_semaphore(request_fn: Callable) -> ApiResponse:
            async with self._semaphore:
                req_start = time.time()
                try:
                    if asyncio.iscoroutinefunction(request_fn):
                        data = await request_fn()
                    else:
                        data = await asyncio.get_event_loop().run_in_executor(
                            None, request_fn
                        )
                    return ApiResponse(
                        endpoint=str(request_fn),
                        status_code=200,
                        data=data,
                        response_time_ms=(time.time() - req_start) * 1000
                    )
                except Exception as e:
                    if self.config.continue_on_error:
                        return ApiResponse(
                            endpoint=str(request_fn),
                            status_code=500,
                            data=None,
                            error=str(e),
                            response_time_ms=(time.time() - req_start) * 1000
                        )
                    raise
        
        tasks = [execute_with_semaphore(req) for req in requests]
        
        try:
            responses = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=not self.config.continue_on_error),
                timeout=self.config.timeout_ms / 1000.0
            )
        except asyncio.TimeoutError:
            responses = []
            self._stats["failed_aggregations"] += 1
        
        if not isinstance(responses, list):
            responses = [responses]
        
        responses = [r for r in responses if isinstance(r, ApiResponse)]
        self._stats["total_responses"] += len(responses)
        
        success_count = sum(1 for r in responses if r.success)
        error_count = len(responses) - success_count
        
        if merger:
            data = merger([r.data for r in responses if r.success])
        else:
            data = self._apply_strategy(
                [r.data for r in responses if r.success],
                strategy
            )
        
        total_duration_ms = (time.time() - start_time) * 1000
        
        result = AggregatedResult(
            responses=responses,
            aggregated_data=data,
            success_count=success_count,
            error_count=error_count,
            total_duration_ms=total_duration_ms
        )
        
        self._history.append(result)
        
        if result.all_successful:
            self._stats["successful_aggregations"] += 1
        else:
            self._stats["failed_aggregations"] += 1
        
        return result
    
    def _apply_strategy(self, data: list[Any], strategy: AggregationStrategy) -> Any:
        """Apply aggregation strategy to data."""
        if not data:
            return None
        
        if strategy == AggregationStrategy.FIRST:
            return data[0]
        elif strategy == AggregationStrategy.LAST:
            return data[-1]
        elif strategy == AggregationStrategy.ALL:
            return data
        elif strategy == AggregationStrategy.MERGE:
            result = data[0]
            for item in data[1:]:
                result = ResponseMerger.merge_values(
                    result, item, self.config.conflict_resolution
                )
            return result
        elif strategy == AggregationStrategy.CONCAT:
            if isinstance(data[0], list):
                result = []
                for item in data:
                    if isinstance(item, list):
                        result.extend(item)
                    else:
                        result.append(item)
                return result
            return data
        elif strategy == AggregationStrategy.REDUCE:
            return data
        
        return data
    
    async def aggregate_parallel(
        self,
        requests: list[Callable[[], Any]],
        batch_size: Optional[int] = None,
        **kwargs
    ) -> list[AggregatedResult]:
        """Aggregate in batches."""
        batch_size = batch_size or self.config.max_concurrent
        results = []
        
        for i in range(0, len(requests), batch_size):
            batch = requests[i:i + batch_size]
            result = await self.aggregate(batch, **kwargs)
            results.append(result)
        
        return results
    
    def get_history(self, limit: int = 100) -> list[AggregatedResult]:
        """Get aggregation history."""
        return list(self._history)[-limit:]
    
    def get_stats(self) -> dict[str, Any]:
        """Get aggregation statistics."""
        return {
            **self._stats,
            "avg_responses_per_aggregation": (
                self._stats["total_responses"] / self._stats["total_aggregations"]
                if self._stats["total_aggregations"] > 0 else 0
            ),
            "success_rate": (
                self._stats["successful_aggregations"] / self._stats["total_aggregations"]
                if self._stats["total_aggregations"] > 0 else 0
            )
        }

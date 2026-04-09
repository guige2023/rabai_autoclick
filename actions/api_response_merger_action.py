"""
API Response Merger Module.

Provides response aggregation, merging, and fan-out/fan-in patterns
for distributed API requests.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TypeVar
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class MergeStrategy(Enum):
    """Strategy for merging responses."""
    FIRST = "first"
    LAST = "last"
    ALL = "all"
    CONCAT = "concat"
    MERGE = "merge"
    RESOLVE = "resolve"  # Wait for all, merge results
    RACE = "race"  # Return first to complete
    ANY = "any"  # Return first successful


@dataclass
class MergeConfig:
    """Configuration for response merging."""
    strategy: MergeStrategy = MergeStrategy.ALL
    timeout: float = 30.0
    fail_fast: bool = True
    max_results: Optional[int] = None
    preserve_order: bool = False
    dedup_key: Optional[str] = None


@dataclass
class MergedResponse:
    """Container for merged response."""
    responses: List[Any]
    errors: List[Exception]
    success_count: int
    error_count: int
    total_latency: float
    strategy: MergeStrategy
    
    
@dataclass
class ResponseContext:
    """Context for a single response."""
    request_id: str
    response: Any
    latency: float
    success: bool
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ResponseMerger:
    """
    Merge responses from multiple API requests.
    
    Example:
        merger = ResponseMerger(MergeConfig(
            strategy=MergeStrategy.RESOLVE,
            timeout=10.0
        ))
        
        # Fan-out requests
        requests = [
            ("GET", "https://api.example.com/users/1"),
            ("GET", "https://api.example.com/users/2"),
            ("GET", "https://api.example.com/users/3"),
        ]
        
        async for result in merger.merge_requests(requests, handler):
            print(result)
    """
    
    def __init__(self, config: Optional[MergeConfig] = None) -> None:
        """
        Initialize the response merger.
        
        Args:
            config: Merge configuration.
        """
        self.config = config or MergeConfig()
        
    async def merge_requests(
        self,
        requests: List[Tuple[str, str, Optional[Dict], Optional[Dict], Optional[bytes]]],
        handler: Callable[..., Awaitable[Any]],
    ) -> MergedResponse:
        """
        Merge responses from multiple requests.
        
        Args:
            requests: List of (method, url, params, headers, body) tuples.
            handler: Async function to execute each request.
            
        Returns:
            MergedResponse with all results.
        """
        start_time = time.time()
        contexts: List[ResponseContext] = []
        errors: List[Exception] = []
        
        if self.config.strategy == MergeStrategy.RACE:
            return await self._merge_race(requests, handler, start_time)
        elif self.config.strategy == MergeStrategy.ANY:
            return await self._merge_any(requests, handler, start_time)
        elif self.config.strategy == MergeStrategy.RESOLVE:
            return await self._merge_resolve(requests, handler, start_time)
        else:
            return await self._merge_all(requests, handler, start_time)
            
    async def merge_responses(
        self,
        futures: List[asyncio.Future],
    ) -> MergedResponse:
        """
        Merge multiple response futures.
        
        Args:
            futures: List of response futures.
            
        Returns:
            MergedResponse with results.
        """
        start_time = time.time()
        responses = []
        errors = []
        
        try:
            results = await asyncio.gather(*futures, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    errors.append(result)
                else:
                    responses.append(result)
                    
        except Exception as e:
            errors.append(e)
            
        total_latency = time.time() - start_time
        
        return MergedResponse(
            responses=responses,
            errors=errors,
            success_count=len(responses),
            error_count=len(errors),
            total_latency=total_latency,
            strategy=self.config.strategy,
        )
        
    async def _merge_all(
        self,
        requests: List[Tuple[str, str, Optional[Dict], Optional[Dict], Optional[bytes]]],
        handler: Callable[..., Awaitable[Any]],
        start_time: float,
    ) -> MergedResponse:
        """Merge all responses (wait for all)."""
        tasks = []
        
        for i, (method, url, params, headers, body) in enumerate(requests):
            request_id = f"merge_{i}_{int(time.time() * 1000)}"
            task = self._execute_and_wrap(
                request_id, method, url, params, headers, body, handler
            )
            tasks.append(task)
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        responses = []
        errors = []
        
        for result in results:
            if isinstance(result, ResponseContext):
                if result.success:
                    responses.append(result.response)
                else:
                    errors.append(Exception(result.error or "Unknown error"))
            else:
                errors.append(result)
                
        total_latency = time.time() - start_time
        
        return MergedResponse(
            responses=responses,
            errors=errors,
            success_count=len(responses),
            error_count=len(errors),
            total_latency=total_latency,
            strategy=self.config.strategy,
        )
        
    async def _merge_race(
        self,
        requests: List[Tuple[str, str, Optional[Dict], Optional[Dict], Optional[bytes]]],
        handler: Callable[..., Awaitable[Any]],
        start_time: float,
    ) -> MergedResponse:
        """Return response from first request to complete."""
        tasks = []
        
        for i, (method, url, params, headers, body) in enumerate(requests):
            request_id = f"race_{i}_{int(time.time() * 1000)}"
            task = self._execute_and_wrap(
                request_id, method, url, params, headers, body, handler
            )
            tasks.append(task)
            
        # Wait for first result
        done, pending = await asyncio.wait(
            tasks,
            timeout=self.config.timeout,
            return_when=asyncio.FIRST_COMPLETED
        )
        
        # Cancel pending
        for task in pending:
            task.cancel()
            
        # Get first result
        responses = []
        errors = []
        
        for task in done:
            try:
                result = task.result()
                if isinstance(result, ResponseContext):
                    if result.success:
                        responses.append(result.response)
                    else:
                        errors.append(Exception(result.error or "Unknown error"))
                else:
                    errors.append(result)
            except Exception as e:
                errors.append(e)
                
        total_latency = time.time() - start_time
        
        return MergedResponse(
            responses=responses,
            errors=errors,
            success_count=len(responses),
            error_count=len(errors),
            total_latency=total_latency,
            strategy=self.config.strategy,
        )
        
    async def _merge_any(
        self,
        requests: List[Tuple[str, str, Optional[Dict], Optional[Dict], Optional[bytes]]],
        handler: Callable[..., Awaitable[Any]],
        start_time: float,
    ) -> MergedResponse:
        """Return first successful response."""
        tasks = []
        
        for i, (method, url, params, headers, body) in enumerate(requests):
            request_id = f"any_{i}_{int(time.time() * 1000)}"
            task = self._execute_and_wrap(
                request_id, method, url, params, headers, body, handler
            )
            tasks.append(task)
            
        # Wait for first successful
        done, pending = await asyncio.wait(
            tasks,
            timeout=self.config.timeout,
            return_when=asyncio.FIRST_EXCEPTION
        )
        
        # Check for success
        for task in done:
            try:
                result = task.result()
                if isinstance(result, ResponseContext) and result.success:
                    # Cancel pending
                    for p in pending:
                        p.cancel()
                    return MergedResponse(
                        responses=[result.response],
                        errors=[],
                        success_count=1,
                        error_count=0,
                        total_latency=time.time() - start_time,
                        strategy=self.config.strategy,
                    )
            except Exception:
                pass
                
        # No success, return what we have
        return await self._merge_all(requests, handler, start_time)
        
    async def _merge_resolve(
        self,
        requests: List[Tuple[str, str, Optional[Dict], Optional[Dict], Optional[bytes]]],
        handler: Callable[..., Awaitable[Any]],
        start_time: float,
    ) -> MergedResponse:
        """Wait for all, then merge results."""
        merged = await self._merge_all(requests, handler, start_time)
        merged.strategy = MergeStrategy.RESOLVE
        return merged
        
    async def _execute_and_wrap(
        self,
        request_id: str,
        method: str,
        url: str,
        params: Optional[Dict],
        headers: Optional[Dict],
        body: Optional[bytes],
        handler: Callable[..., Awaitable[Any]],
    ) -> ResponseContext:
        """Execute request and wrap in ResponseContext."""
        start_time = time.time()
        
        try:
            result = await asyncio.wait_for(
                handler(method, url, params, headers, body),
                timeout=self.config.timeout
            )
            latency = time.time() - start_time
            
            return ResponseContext(
                request_id=request_id,
                response=result,
                latency=latency,
                success=True,
            )
        except asyncio.TimeoutError:
            return ResponseContext(
                request_id=request_id,
                response=None,
                latency=self.config.timeout,
                success=False,
                error=f"Request timeout after {self.config.timeout}s",
            )
        except Exception as e:
            return ResponseContext(
                request_id=request_id,
                response=None,
                latency=time.time() - start_time,
                success=False,
                error=str(e),
            )
            
    def merge_data(
        self,
        data: List[Dict[str, Any]],
        merge_key: str,
    ) -> Dict[str, List[Any]]:
        """
        Merge list of dicts by key.
        
        Args:
            data: List of dictionaries.
            merge_key: Key to merge on.
            
        Returns:
            Merged dictionary.
        """
        result: Dict[str, List[Any]] = defaultdict(list)
        
        for item in data:
            if merge_key in item:
                key = item[merge_key]
                result[key].append(item)
                
        return dict(result)


class FanOutMerger:
    """
    Fan-out/fan-in request pattern with merging.
    
    Example:
        fanout = FanOutMerger(max_concurrency=10)
        
        async for result in fanout.execute(
            items=user_ids,
            func=lambda uid: fetch_user(uid),
            merge_strategy="merge_by_id"
        ):
            print(result)
    """
    
    def __init__(
        self,
        max_concurrency: int = 10,
        timeout: float = 30.0,
    ) -> None:
        """
        Initialize fan-out merger.
        
        Args:
            max_concurrency: Maximum concurrent executions.
            timeout: Timeout per item.
        """
        self.max_concurrency = max_concurrency
        self.timeout = timeout
        self._semaphore = asyncio.Semaphore(max_concurrency)
        
    async def execute(
        self,
        items: List[T],
        func: Callable[[T], Awaitable[R]],
        merge_strategy: str = "all",
    ) -> MergedResponse:
        """
        Execute function for each item with fan-out/fan-in.
        
        Args:
            items: List of items to process.
            func: Async function to execute per item.
            merge_strategy: How to merge results.
            
        Returns:
            MergedResponse with all results.
        """
        start_time = time.time()
        results: List[R] = []
        errors: List[Exception] = []
        
        async def execute_with_semaphore(item: T, index: int) -> Tuple[int, R, Optional[Exception]]:
            async with self._semaphore:
                try:
                    result = await asyncio.wait_for(func(item), timeout=self.timeout)
                    return (index, result, None)
                except Exception as e:
                    return (index, None, e)
                    
        tasks = [execute_with_semaphore(item, i) for i, item in enumerate(items)]
        task_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect results
        for result in task_results:
            if isinstance(result, Exception):
                errors.append(result)
            else:
                index, value, error = result
                if error:
                    errors.append(error)
                else:
                    results.append(value)
                    
        total_latency = time.time() - start_time
        
        return MergedResponse(
            responses=results,
            errors=errors,
            success_count=len(results),
            error_count=len(errors),
            total_latency=total_latency,
            strategy=MergeStrategy.ALL,
        )
        
    async def execute_with_buckets(
        self,
        items: List[T],
        func: Callable[[T], Awaitable[R]],
        bucket_count: int = 3,
    ) -> List[List[R]]:
        """
        Execute items in buckets, return results per bucket.
        
        Args:
            items: List of items to process.
            func: Async function to execute per item.
            bucket_count: Number of buckets.
            
        Returns:
            List of result lists per bucket.
        """
        # Split items into buckets
        bucket_size = (len(items) + bucket_count - 1) // bucket_count
        buckets = [
            items[i:i + bucket_size]
            for i in range(0, len(items), bucket_size)
        ]
        
        async def process_bucket(bucket_items: List[T]) -> List[R]:
            results = []
            for item in bucket_items:
                result = await func(item)
                results.append(result)
            return results
            
        bucket_results = await asyncio.gather(*[
            process_bucket(bucket) for bucket in buckets
        ])
        
        return list(bucket_results)

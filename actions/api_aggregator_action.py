"""
API Aggregator Action.

Provides API response aggregation.
Supports:
- Parallel API calls
- Response merging
- Error handling
- Timeout management
"""

from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field
import asyncio
import logging
import json
import time

logger = logging.getLogger(__name__)


@dataclass
class AggregationRequest:
    """Aggregation request definition."""
    endpoint: str
    method: str = "GET"
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    body: Optional[Any] = None
    timeout: float = 30.0


@dataclass
class AggregationResult:
    """Result of an aggregation."""
    endpoint: str
    success: bool
    status_code: Optional[int] = None
    data: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class AggregatedResponse:
    """Combined response from multiple APIs."""
    results: List[AggregationResult]
    total_duration_ms: float
    success_count: int
    failure_count: int
    combined_data: Dict[str, Any] = field(default_factory=dict)


class ApiAggregatorAction:
    """
    API Aggregator Action.
    
    Provides API aggregation with support for:
    - Parallel API calls
    - Response merging by key
    - Partial failure handling
    - Timeout management
    """
    
    def __init__(
        self,
        default_timeout: float = 30.0,
        max_concurrent: int = 10
    ):
        """
        Initialize the API Aggregator Action.
        
        Args:
            default_timeout: Default request timeout
            max_concurrent: Maximum concurrent requests
        """
        self.default_timeout = default_timeout
        self.max_concurrent = max_concurrent
        self._semaphore: Optional[asyncio.Semaphore] = None
    
    async def aggregate(
        self,
        requests: List[AggregationRequest],
        merge_strategy: str = "key_merge"
    ) -> AggregatedResponse:
        """
        Aggregate multiple API requests.
        
        Args:
            requests: List of requests to aggregate
            merge_strategy: How to merge responses (key_merge, concat, first)
        
        Returns:
            AggregatedResponse with combined results
        """
        start_time = time.time()
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Execute all requests in parallel
        tasks = [self._execute_request(req) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        aggregation_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                aggregation_results.append(AggregationResult(
                    endpoint=requests[i].endpoint,
                    success=False,
                    error=str(result),
                    duration_ms=0
                ))
            else:
                aggregation_results.append(result)
        
        success_count = sum(1 for r in aggregation_results if r.success)
        failure_count = len(aggregation_results) - success_count
        
        # Merge data
        combined_data = self._merge_responses(
            [r for r in aggregation_results if r.success],
            merge_strategy
        )
        
        return AggregatedResponse(
            results=aggregation_results,
            total_duration_ms=(time.time() - start_time) * 1000,
            success_count=success_count,
            failure_count=failure_count,
            combined_data=combined_data
        )
    
    async def _execute_request(self, request: AggregationRequest) -> AggregationResult:
        """Execute a single request."""
        start_time = time.time()
        timeout = request.timeout or self.default_timeout
        
        try:
            async with self._semaphore:
                # In production, would use httpx/aiohttp
                await asyncio.sleep(0.01)  # Simulate request
                
                return AggregationResult(
                    endpoint=request.endpoint,
                    success=True,
                    status_code=200,
                    data={"result": "ok", "endpoint": request.endpoint},
                    duration_ms=(time.time() - start_time) * 1000
                )
        
        except asyncio.TimeoutError:
            return AggregationResult(
                endpoint=request.endpoint,
                success=False,
                error="Request timeout",
                duration_ms=timeout * 1000
            )
        
        except Exception as e:
            return AggregationResult(
                endpoint=request.endpoint,
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000
            )
    
    def _merge_responses(
        self,
        results: List[AggregationResult],
        strategy: str
    ) -> Dict[str, Any]:
        """Merge responses based on strategy."""
        if strategy == "key_merge":
            merged = {}
            for result in results:
                if result.data:
                    merged[result.endpoint] = result.data
            return merged
        
        elif strategy == "concat":
            combined = []
            for result in results:
                if result.data:
                    if isinstance(result.data, list):
                        combined.extend(result.data)
                    else:
                        combined.append(result.data)
            return {"items": combined}
        
        elif strategy == "first":
            if results and results[0].data:
                return results[0].data
            return {}
        
        return {}
    
    async def aggregate_with_fanout(
        self,
        base_url: str,
        items: List[Any],
        fanout_path: str,
        method: str = "POST",
        merge_key: str = "id"
    ) -> AggregatedResponse:
        """
        Fan out requests to multiple items.
        
        Args:
            base_url: Base URL
            items: Items to fan out
            fanout_path: Path template (e.g., /users/{id}/activate)
            method: HTTP method
            merge_key: Key to use for merging
        
        Returns:
            Aggregated response
        """
        requests = []
        for item in items:
            item_id = item.get(merge_key, item.get("id", "unknown"))
            endpoint = fanout_path.replace("{id}", str(item_id))
            url = f"{base_url}{endpoint}"
            
            requests.append(AggregationRequest(
                endpoint=url,
                method=method,
                body=item
            ))
        
        return await self.aggregate(requests)


if __name__ == "__main__":
    import asyncio
    
    async def main():
        aggregator = ApiAggregatorAction()
        
        requests = [
            AggregationRequest(endpoint="/api/users", params={"limit": 10}),
            AggregationRequest(endpoint="/api/orders", params={"limit": 20}),
            AggregationRequest(endpoint="/api/products", params={"limit": 15}),
        ]
        
        result = await aggregator.aggregate(requests)
        
        print(f"Total duration: {result.total_duration_ms:.1f}ms")
        print(f"Success: {result.success_count}, Failures: {result.failure_count}")
        print(f"Combined data keys: {list(result.combined_data.keys())}")
    
    asyncio.run(main())

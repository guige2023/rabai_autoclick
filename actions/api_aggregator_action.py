"""
API Aggregator Action Module.

Aggregates data from multiple API endpoints with fan-out/fan-in
patterns, parallel execution, and result merging.

Author: RabAi Team
"""

from __future__ import annotations

import json
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AggregationStrategy(Enum):
    """Aggregation strategies."""
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    PRIORITY = "priority"
    FAN_OUT = "fan_out"
    ROUND_ROBIN = "round_robin"


class MergeStrategy(Enum):
    """Result merging strategies."""
    APPEND = "append"
    MERGE_DICT = "merge_dict"
    CONCATENATE = "concatenate"
    UNION = "union"
    INTERSECTION = "intersection"


@dataclass
class ApiEndpoint:
    """An API endpoint to aggregate."""
    name: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    timeout: float = 30.0
    priority: int = 0
    expected_status: int = 200
    response_path: Optional[str] = None
    retry_count: int = 0


@dataclass
class AggregationResult:
    """Result from aggregating multiple endpoints."""
    endpoint_name: str
    success: bool
    status_code: Optional[int]
    data: Any
    latency_ms: float
    error: Optional[str] = None


class ApiAggregatorAction(BaseAction):
    """API aggregator action.
    
    Aggregates data from multiple API endpoints with
    configurable execution strategies and result merging.
    """
    action_type = "api_aggregator"
    display_name = "API聚合"
    description = "多API端点数据聚合"
    
    def __init__(self):
        super().__init__()
        self._results: Dict[str, List[AggregationResult]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Aggregate data from multiple API endpoints.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - endpoints: List of endpoint definitions
                - strategy: Aggregation strategy (parallel/sequential/priority/fan_out)
                - merge_strategy: How to merge results
                - max_concurrency: Max parallel requests
                - timeout: Overall timeout
                - merge_key: Key field for dict merge
                - global_headers: Headers to apply to all requests
                
        Returns:
            ActionResult with aggregated data.
        """
        start_time = time.time()
        
        endpoints_data = params.get("endpoints", [])
        strategy_str = params.get("strategy", "parallel")
        merge_str = params.get("merge_strategy", "append")
        max_concurrency = params.get("max_concurrency", 10)
        global_timeout = params.get("timeout", 120)
        merge_key = params.get("merge_key")
        global_headers = params.get("global_headers", {})
        
        try:
            strategy = AggregationStrategy(strategy_str)
        except ValueError:
            strategy = AggregationStrategy.PARALLEL
        
        try:
            merge = MergeStrategy(merge_str)
        except ValueError:
            merge = MergeStrategy.APPEND
        
        endpoints = [self._parse_endpoint(ep, global_headers) for ep in endpoints_data]
        
        if not endpoints:
            return ActionResult(
                success=False,
                message="No endpoints to aggregate",
                duration=time.time() - start_time
            )
        
        try:
            if strategy == AggregationStrategy.PARALLEL:
                results = self._aggregate_parallel(endpoints, max_concurrency, global_timeout)
            elif strategy == AggregationStrategy.SEQUENTIAL:
                results = self._aggregate_sequential(endpoints, global_timeout)
            elif strategy == AggregationStrategy.PRIORITY:
                results = self._aggregate_priority(endpoints, max_concurrency, global_timeout)
            elif strategy == AggregationStrategy.FAN_OUT:
                results = self._aggregate_fan_out(endpoints, global_timeout)
            elif strategy == AggregationStrategy.ROUND_ROBIN:
                results = self._aggregate_round_robin(endpoints, global_timeout)
            else:
                results = self._aggregate_parallel(endpoints, max_concurrency, global_timeout)
            
            merged = self._merge_results(results, merge, merge_key)
            
            return ActionResult(
                success=sum(1 for r in results if r.success) > 0,
                message=f"Aggregated {len(results)} endpoints: {sum(1 for r in results if r.success)} succeeded",
                data={
                    "merged": merged,
                    "results": [
                        {"endpoint": r.endpoint_name, "success": r.success,
                         "status_code": r.status_code, "latency_ms": r.latency_ms}
                        for r in results
                    ],
                    "succeeded": sum(1 for r in results if r.success),
                    "failed": sum(1 for r in results if not r.success),
                    "total_duration_ms": (time.time() - start_time) * 1000
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Aggregation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _parse_endpoint(self, ep: Dict[str, Any], global_headers: Dict) -> ApiEndpoint:
        """Parse endpoint definition."""
        headers = dict(global_headers)
        headers.update(ep.get("headers", {}))
        
        return ApiEndpoint(
            name=ep.get("name", ep.get("url", "unnamed")),
            url=ep.get("url", ""),
            method=ep.get("method", "GET"),
            headers=headers,
            body=ep.get("body"),
            timeout=ep.get("timeout", 30.0),
            priority=ep.get("priority", 0),
            expected_status=ep.get("expected_status", 200),
            response_path=ep.get("response_path"),
            retry_count=ep.get("retry_count", 0)
        )
    
    def _call_endpoint(self, endpoint: ApiEndpoint) -> AggregationResult:
        """Call a single endpoint."""
        start = time.time()
        
        for attempt in range(endpoint.retry_count + 1):
            try:
                body_bytes = None
                if endpoint.body is not None:
                    if isinstance(endpoint.body, dict):
                        body_bytes = json.dumps(endpoint.body).encode("utf-8")
                    elif isinstance(endpoint.body, str):
                        body_bytes = endpoint.body.encode("utf-8")
                    else:
                        body_bytes = str(endpoint.body).encode("utf-8")
                
                headers = dict(endpoint.headers)
                if body_bytes and "Content-Type" not in headers:
                    headers["Content-Type"] = "application/json"
                
                req = Request(endpoint.url, data=body_bytes, headers=headers, method=endpoint.method)
                
                with urlopen(req, timeout=endpoint.timeout) as response:
                    body = response.read()
                    try:
                        data = json.loads(body)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        data = body.decode("utf-8", errors="replace")
                    
                    if endpoint.response_path:
                        data = self._extract_path(data, endpoint.response_path)
                    
                    return AggregationResult(
                        endpoint_name=endpoint.name,
                        success=response.status == endpoint.expected_status,
                        status_code=response.status,
                        data=data,
                        latency_ms=(time.time() - start) * 1000
                    )
                    
            except HTTPError as e:
                if attempt == endpoint.retry_count:
                    return AggregationResult(
                        endpoint_name=endpoint.name,
                        success=False,
                        status_code=e.code,
                        data=None,
                        latency_ms=(time.time() - start) * 1000,
                        error=f"HTTP {e.code}: {str(e)}"
                    )
            except URLError as e:
                if attempt == endpoint.retry_count:
                    return AggregationResult(
                        endpoint_name=endpoint.name,
                        success=False,
                        status_code=None,
                        data=None,
                        latency_ms=(time.time() - start) * 1000,
                        error=f"URL error: {str(e)}"
                    )
            except Exception as e:
                if attempt == endpoint.retry_count:
                    return AggregationResult(
                        endpoint_name=endpoint.name,
                        success=False,
                        status_code=None,
                        data=None,
                        latency_ms=(time.time() - start) * 1000,
                        error=str(e)
                    )
        
        return AggregationResult(
            endpoint_name=endpoint.name,
            success=False,
            status_code=None,
            data=None,
            latency_ms=(time.time() - start) * 1000,
            error="Max retries exceeded"
        )
    
    def _extract_path(self, data: Any, path: str) -> Any:
        """Extract value from data using dot notation path."""
        if not path:
            return data
        
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if 0 <= idx < len(current) else None
            else:
                return None
        return current
    
    def _aggregate_parallel(
        self, endpoints: List[ApiEndpoint], max_concurrency: int, timeout: float
    ) -> List[AggregationResult]:
        """Execute endpoints in parallel."""
        results = []
        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            futures = {executor.submit(self._call_endpoint, ep): ep for ep in endpoints}
            for future in as_completed(futures, timeout=timeout):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    ep = futures[future]
                    results.append(AggregationResult(
                        endpoint_name=ep.name,
                        success=False,
                        status_code=None,
                        data=None,
                        latency_ms=0,
                        error=str(e)
                    ))
        return results
    
    def _aggregate_sequential(
        self, endpoints: List[ApiEndpoint], timeout: float
    ) -> List[AggregationResult]:
        """Execute endpoints sequentially."""
        results = []
        deadline = time.time() + timeout
        for ep in endpoints:
            if time.time() > deadline:
                results.append(AggregationResult(
                    endpoint_name=ep.name,
                    success=False,
                    status_code=None,
                    data=None,
                    latency_ms=0,
                    error="Timeout exceeded"
                ))
            else:
                results.append(self._call_endpoint(ep))
        return results
    
    def _aggregate_priority(
        self, endpoints: List[ApiEndpoint], max_concurrency: int, timeout: float
    ) -> List[AggregationResult]:
        """Execute endpoints by priority."""
        sorted_endpoints = sorted(endpoints, key=lambda e: -e.priority)
        return self._aggregate_parallel(sorted_endpoints, max_concurrency, timeout)
    
    def _aggregate_fan_out(
        self, endpoints: List[ApiEndpoint], timeout: float
    ) -> List[AggregationResult]:
        """Fan out: execute all regardless of failures."""
        return self._aggregate_parallel(endpoints, len(endpoints), timeout)
    
    def _aggregate_round_robin(
        self, endpoints: List[ApiEndpoint], timeout: float
    ) -> List[AggregationResult]:
        """Round robin execution (same as sequential for now)."""
        return self._aggregate_sequential(endpoints, timeout)
    
    def _merge_results(
        self, results: List[AggregationResult], strategy: MergeStrategy, merge_key: Optional[str]
    ) -> Any:
        """Merge results using specified strategy."""
        successful_results = [r.data for r in results if r.success and r.data is not None]
        
        if not successful_results:
            return None
        
        if strategy == MergeStrategy.APPEND:
            return [{"endpoint": r.endpoint_name, "data": r.data} for r in results if r.data is not None]
        
        elif strategy == MergeStrategy.CONCATENATE:
            all_items = []
            for data in successful_results:
                if isinstance(data, list):
                    all_items.extend(data)
                else:
                    all_items.append(data)
            return all_items
        
        elif strategy == MergeStrategy.MERGE_DICT:
            merged = {}
            for data in successful_results:
                if isinstance(data, dict):
                    merged.update(data)
            return merged
        
        elif strategy == MergeStrategy.UNION:
            if merge_key:
                seen = {}
                for data in successful_results:
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and merge_key in item:
                                seen[item[merge_key]] = item
                            elif isinstance(item, str):
                                seen[item] = item
                    elif isinstance(data, dict):
                        if merge_key in data:
                            seen[data[merge_key]] = data
                return list(seen.values())
            return successful_results
        
        elif strategy == MergeStrategy.INTERSECTION:
            if merge_key:
                key_sets = []
                for data in successful_results:
                    if isinstance(data, list):
                        keys = {item.get(merge_key) for item in data if isinstance(item, dict) and merge_key in item}
                        key_sets.append(keys)
                    elif isinstance(data, dict) and merge_key in data:
                        key_sets.append({data[merge_key]})
                
                if key_sets:
                    common_keys = set.intersection(*key_sets) if len(key_sets) > 1 else key_sets[0]
                    result = []
                    for data in successful_results:
                        if isinstance(data, list):
                            result.extend([item for item in data if isinstance(item, dict) and item.get(merge_key) in common_keys])
                    return result
            return successful_results
        
        return successful_results
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate aggregator parameters."""
        if "endpoints" not in params:
            return False, "Missing required parameter: endpoints"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["endpoints"]

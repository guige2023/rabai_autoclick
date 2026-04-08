"""API Aggregation Action Module.

Provides API response aggregation, batching,
and combining capabilities for multiple API calls.
"""

from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import time
from datetime import datetime


class AggregationStrategy(Enum):
    """Strategy for aggregating responses."""
    FIRST = "first"
    LAST = "last"
    CONCAT = "concat"
    MERGE = "merge"
    OVERWRITE = "overwrite"
    CUSTOM = "custom"


@dataclass
class APIEndpoint:
    """Defines an API endpoint to call."""
    name: str
    url: str
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    timeout: int = 30
    retry_count: int = 0
    depends_on: List[str] = field(default_factory=list)


@dataclass
class APIResponse:
    """Stores an API response."""
    endpoint_name: str
    status_code: int
    headers: Dict[str, str]
    body: Any
    duration_ms: float
    success: bool
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class BatchConfig:
    """Configuration for batch operations."""
    max_concurrent: int = 5
    batch_size: int = 10
    continue_on_error: bool = True
    timeout_per_request: int = 30
    retry_count: int = 0


@dataclass
class AggregationConfig:
    """Configuration for response aggregation."""
    strategy: AggregationStrategy
    custom_aggregator: Optional[Callable] = None
    key_field: Optional[str] = None


class RequestBatcher:
    """Batches multiple requests for efficient execution."""

    def __init__(self, config: Optional[BatchConfig] = None):
        self.config = config or BatchConfig()
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def execute_batch(
        self,
        endpoints: List[APIEndpoint],
        executor: Callable[[APIEndpoint], asyncio.Task],
    ) -> List[APIResponse]:
        """Execute endpoints in batches."""
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        responses = []

        for i in range(0, len(endpoints), self.config.batch_size):
            batch = endpoints[i:i + self.config.batch_size]
            batch_results = await self._execute_batch_concurrent(batch, executor)
            responses.extend(batch_results)

            if not self.config.continue_on_error:
                failed = [r for r in batch_results if not r.success]
                if failed:
                    break

        return responses

    async def _execute_batch_concurrent(
        self,
        endpoints: List[APIEndpoint],
        executor: Callable[[APIEndpoint], asyncio.Task],
    ) -> List[APIResponse]:
        """Execute a single batch concurrently."""
        tasks = [executor(ep) for ep in endpoints]
        return await asyncio.gather(*tasks, return_exceptions=True)


class ResponseAggregator:
    """Aggregates multiple API responses."""

    def __init__(self):
        self._custom_aggregators: Dict[str, Callable] = {}

    def register_aggregator(
        self,
        name: str,
        aggregator: Callable,
    ):
        """Register a custom aggregator."""
        self._custom_aggregators[name] = aggregator

    def aggregate(
        self,
        responses: List[APIResponse],
        config: AggregationConfig,
    ) -> Any:
        """Aggregate responses based on strategy."""
        if config.strategy == AggregationStrategy.FIRST:
            return self._aggregate_first(responses)
        elif config.strategy == AggregationStrategy.LAST:
            return self._aggregate_last(responses)
        elif config.strategy == AggregationStrategy.CONCAT:
            return self._aggregate_concat(responses)
        elif config.strategy == AggregationStrategy.MERGE:
            return self._aggregate_merge(responses, config.key_field)
        elif config.strategy == AggregationStrategy.OVERWRITE:
            return self._aggregate_overwrite(responses)
        elif config.strategy == AggregationStrategy.CUSTOM and config.custom_aggregator:
            return config.custom_aggregator(responses)
        return responses

    def _aggregate_first(self, responses: List[APIResponse]) -> Any:
        """Get first successful response."""
        for response in responses:
            if response.success:
                return response.body
        return None

    def _aggregate_last(self, responses: List[APIResponse]) -> Any:
        """Get last successful response."""
        for response in reversed(responses):
            if response.success:
                return response.body
        return None

    def _aggregate_concat(self, responses: List[APIResponse]) -> List[Any]:
        """Concatenate all successful responses."""
        results = []
        for response in responses:
            if response.success:
                if isinstance(response.body, list):
                    results.extend(response.body)
                else:
                    results.append(response.body)
        return results

    def _aggregate_merge(
        self,
        responses: List[APIResponse],
        key_field: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Merge responses into single dict."""
        merged = {}
        for response in responses:
            if response.success and isinstance(response.body, dict):
                merged.update(response.body)
        return merged

    def _aggregate_overwrite(self, responses: List[APIResponse]) -> Any:
        """Overwrite with last successful response."""
        result = None
        for response in responses:
            if response.success:
                result = response.body
        return result


class ResponseCombiner:
    """Combines responses with different strategies."""

    def combine_sequential(
        self,
        responses: List[APIResponse],
    ) -> List[Dict[str, Any]]:
        """Combine responses sequentially."""
        return [
            {
                "name": r.endpoint_name,
                "status": r.status_code,
                "data": r.body,
                "duration_ms": r.duration_ms,
            }
            for r in responses
        ]

    def combine_by_status(
        self,
        responses: List[APIResponse],
    ) -> Dict[str, List[APIResponse]]:
        """Group responses by status code."""
        result: Dict[str, List[APIResponse]] = {}
        for response in responses:
            key = str(response.status_code)
            if key not in result:
                result[key] = []
            result[key].append(response)
        return result

    def combine_success_failure(
        self,
        responses: List[APIResponse],
    ) -> Dict[str, List[APIResponse]]:
        """Separate successful and failed responses."""
        return {
            "success": [r for r in responses if r.success],
            "failure": [r for r in responses if not r.success],
        }

    def combine_summary(
        self,
        responses: List[APIResponse],
    ) -> Dict[str, Any]:
        """Create summary of all responses."""
        total = len(responses)
        successful = sum(1 for r in responses if r.success)
        failed = total - successful

        total_duration = sum(r.duration_ms for r in responses)
        avg_duration = total_duration / total if total > 0 else 0

        return {
            "total": total,
            "successful": successful,
            "failed": failed,
            "success_rate": successful / total if total > 0 else 0,
            "total_duration_ms": total_duration,
            "avg_duration_ms": avg_duration,
        }


class DependencyResolver:
    """Resolves dependencies between endpoints."""

    def resolve_order(
        self,
        endpoints: List[APIEndpoint],
    ) -> List[List[APIEndpoint]]:
        """Resolve dependencies and return execution levels."""
        levels: List[List[APIEndpoint]] = []
        remaining = endpoints.copy()
        resolved: set = set()

        while remaining:
            current_level = []

            for endpoint in remaining[:]:
                can_execute = all(dep in resolved for dep in endpoint.depends_on)

                if can_execute:
                    current_level.append(endpoint)
                    remaining.remove(endpoint)
                    resolved.add(endpoint.name)

            if not current_level:
                raise ValueError("Circular dependency detected")

            levels.append(current_level)

        return levels

    def validate_dependencies(
        self,
        endpoints: List[APIEndpoint],
    ) -> List[str]:
        """Validate that all dependencies are satisfiable."""
        endpoint_names = {ep.name for ep in endpoints}
        errors = []

        for endpoint in endpoints:
            for dep in endpoint.depends_on:
                if dep not in endpoint_names:
                    errors.append(
                        f"Endpoint '{endpoint.name}' depends on unknown '{dep}'"
                    )

        return errors


class APIAggregatorAction:
    """High-level API aggregation action."""

    def __init__(
        self,
        batcher: Optional[RequestBatcher] = None,
        aggregator: Optional[ResponseAggregator] = None,
        combiner: Optional[ResponseCombiner] = None,
    ):
        self.batcher = batcher or RequestBatcher()
        self.aggregator = aggregator or ResponseAggregator()
        self.combiner = combiner or ResponseCombiner()

    async def aggregate_responses(
        self,
        responses: List[APIResponse],
        strategy: str = "concat",
    ) -> Any:
        """Aggregate responses with specified strategy."""
        config = AggregationConfig(strategy=AggregationStrategy(strategy))
        return self.aggregator.aggregate(responses, config)

    def combine_responses(
        self,
        responses: List[APIResponse],
        format: str = "sequential",
    ) -> Any:
        """Combine responses in specified format."""
        if format == "sequential":
            return self.combiner.combine_sequential(responses)
        elif format == "by_status":
            return self.combiner.combine_by_status(responses)
        elif format == "success_failure":
            return self.combiner.combine_success_failure(responses)
        elif format == "summary":
            return self.combiner.combine_summary(responses)
        return responses

    def get_summary(
        self,
        responses: List[APIResponse],
    ) -> Dict[str, Any]:
        """Get summary statistics."""
        return self.combiner.combine_summary(responses)


# Module exports
__all__ = [
    "APIAggregatorAction",
    "RequestBatcher",
    "ResponseAggregator",
    "ResponseCombiner",
    "DependencyResolver",
    "APIEndpoint",
    "APIResponse",
    "BatchConfig",
    "AggregationConfig",
    "AggregationStrategy",
]

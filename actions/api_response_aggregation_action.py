"""API Response Aggregation Action module.

Aggregates multiple API responses into unified results
with support for parallel fetching, response merging,
and conflict resolution.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import aiohttp


class MergeStrategy(Enum):
    """Strategy for merging multiple responses."""

    PREFER_FIRST = "prefer_first"
    PREFER_LAST = "prefer_last"
    MERGE_OBJECTS = "merge_objects"
    CONCATENATE_ARRAYS = "concatenate_arrays"
    RESOLVE_CONFLICTS = "resolve_conflicts"


@dataclass
class AggregatedResponse:
    """Result of response aggregation."""

    responses: list[Any]
    merged: Any
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    errors: list[Exception] = field(default_factory=list)
    latency_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "response_count": len(self.responses),
            "has_errors": len(self.errors) > 0,
            "error_count": len(self.errors),
            "conflict_count": len(self.conflicts),
            "latency_ms": self.latency_ms,
        }


@dataclass
class AggregationConfig:
    """Configuration for response aggregation."""

    merge_strategy: MergeStrategy = MergeStrategy.MERGE_OBJECTS
    timeout: float = 30.0
    max_concurrent: int = 10
    stop_on_first_error: bool = False


class ResponseAggregator:
    """Aggregates multiple API responses."""

    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()

    async def fetch_and_aggregate(
        self,
        requests: list[dict[str, Any]],
    ) -> AggregatedResponse:
        """Fetch multiple URLs and aggregate responses.

        Args:
            requests: List of request specs with 'url' and optional 'params'

        Returns:
            AggregatedResponse
        """
        import time

        start = time.monotonic()
        responses = []
        errors = []

        semaphore = asyncio.Semaphore(self.config.max_concurrent)

        async def fetch_one(req: dict[str, Any]) -> Any:
            async with semaphore:
                url = req["url"]
                method = req.get("method", "GET")
                params = req.get("params", {})

                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.request(
                            method,
                            url,
                            params=params,
                            timeout=aiohttp.ClientTimeout(total=self.config.timeout),
                        ) as response:
                            response.raise_for_status()
                            return await response.json()
                except Exception as e:
                    if self.config.stop_on_first_error:
                        raise
                    errors.append(e)
                    return None

        results = await asyncio.gather(*[fetch_one(r) for r in requests], return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                errors.append(result)
            elif result is not None:
                responses.append(result)

        merged = self._merge_responses(responses)
        conflicts = self._detect_conflicts(responses, merged)

        return AggregatedResponse(
            responses=responses,
            merged=merged,
            conflicts=conflicts,
            errors=errors,
            latency_ms=(time.monotonic() - start) * 1000,
        )

    def _merge_responses(self, responses: list[Any]) -> Any:
        """Merge responses based on strategy."""
        if not responses:
            return None

        if len(responses) == 1:
            return responses[0]

        strategy = self.config.merge_strategy

        if strategy == MergeStrategy.PREFER_FIRST:
            return responses[0]
        elif strategy == MergeStrategy.PREFER_LAST:
            return responses[-1]
        elif strategy == MergeStrategy.MERGE_OBJECTS:
            return self._merge_objects(responses)
        elif strategy == MergeStrategy.CONCATENATE_ARRAYS:
            return self._concat_arrays(responses)
        elif strategy == MergeStrategy.RESOLVE_CONFLICTS:
            return self._merge_with_conflict_resolution(responses)

        return responses[0]

    def _merge_objects(self, responses: list[Any]) -> dict[str, Any]:
        """Merge object responses recursively."""
        if not responses:
            return {}

        result = {}
        for response in responses:
            if isinstance(response, dict):
                for key, value in response.items():
                    if key not in result:
                        result[key] = value
                    elif isinstance(result[key], dict) and isinstance(value, dict):
                        result[key] = self._merge_objects([result[key], value])
                    elif isinstance(result[key], list) and isinstance(value, list):
                        result[key] = result[key] + value
                    else:
                        result[key] = value
        return result

    def _concat_arrays(self, responses: list[Any]) -> list[Any]:
        """Concatenate array responses."""
        result = []
        seen = set()

        for response in responses:
            if isinstance(response, list):
                for item in response:
                    item_key = self._make_hashable(item)
                    if item_key not in seen:
                        seen.add(item_key)
                        result.append(item)
            else:
                item_key = self._make_hashable(response)
                if item_key not in seen:
                    seen.add(item_key)
                    result.append(response)

        return result

    def _merge_with_conflict_resolution(
        self,
        responses: list[Any],
    ) -> dict[str, Any]:
        """Merge responses with conflict resolution."""
        return self._merge_objects(responses)

    def _detect_conflicts(
        self,
        responses: list[Any],
        merged: Any,
    ) -> list[dict[str, Any]]:
        """Detect conflicting values in responses."""
        conflicts = []

        if not all(isinstance(r, dict) for r in responses if isinstance(r, dict)):
            return conflicts

        all_keys = set()
        for response in responses:
            if isinstance(response, dict):
                all_keys.update(response.keys())

        for key in all_keys:
            values = []
            for response in responses:
                if isinstance(response, dict) and key in response:
                    values.append(response[key])

            if len(set(str(v) for v in values)) > 1:
                conflicts.append({
                    "key": key,
                    "values": values,
                    "resolved": merged.get(key) if isinstance(merged, dict) else None,
                })

        return conflicts

    def _make_hashable(self, item: Any) -> Any:
        """Convert item to hashable representation."""
        if isinstance(item, dict):
            return tuple(sorted((k, self._make_hashable(v)) for k, v in item.items()))
        elif isinstance(item, list):
            return tuple(self._make_hashable(i) for i in item)
        return item


class ParallelFetchAggregator:
    """Parallel fetcher with aggregation."""

    def __init__(
        self,
        max_concurrent: int = 10,
        timeout: float = 30.0,
    ):
        self.max_concurrent = max_concurrent
        self.timeout = timeout

    async def fetch_parallel(
        self,
        urls: list[str],
        merger: Optional[Callable[[list[Any]], Any]] = None,
    ) -> tuple[Any, list[Any]]:
        """Fetch URLs in parallel and optionally merge.

        Args:
            urls: List of URLs to fetch
            merger: Optional function to merge responses

        Returns:
            Tuple of (merged_result, individual_responses)
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        results = []
        errors = []

        async def fetch(url: str) -> Any:
            async with semaphore:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            url,
                            timeout=aiohttp.ClientTimeout(total=self.timeout),
                        ) as response:
                            response.raise_for_status()
                            return await response.json()
                except Exception as e:
                    errors.append(e)
                    return None

        fetched = await asyncio.gather(*[fetch(u) for u in urls], return_exceptions=True)

        for result in fetched:
            if isinstance(result, Exception):
                errors.append(result)
            elif result is not None:
                results.append(result)

        if merger:
            merged = merger(results)
        else:
            merged = results

        return merged, results


class FanOutAggregator:
    """Fan-out aggregation with result collection."""

    def __init__(
        self,
        max_workers: int = 10,
        timeout: float = 30.0,
    ):
        self.max_workers = max_workers
        self.timeout = timeout

    async def fan_out(
        self,
        func: Callable[..., Any],
        items: list[Any],
        return_exceptions: bool = True,
    ) -> list[Any]:
        """Execute function for each item in parallel.

        Args:
            func: Function to execute for each item
            items: List of items to process
            return_exceptions: Whether to return exceptions

        Returns:
            List of results
        """
        semaphore = asyncio.Semaphore(self.max_workers)

        async def process(item: Any) -> Any:
            async with semaphore:
                try:
                    if asyncio.iscoroutinefunction(func):
                        return await func(item)
                    return func(item)
                except Exception as e:
                    if return_exceptions:
                        return e
                    raise

        return await asyncio.gather(*[process(item) for item in items])

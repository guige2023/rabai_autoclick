"""API response aggregator for combining multiple API results.

This module provides response aggregation:
- Merge results from multiple APIs
- Fan-out/fan-in patterns
- Result deduplication
- Conflict resolution

Example:
    >>> from actions.api_aggregator_action import ResponseAggregator
    >>> aggregator = ResponseAggregator()
    >>> result = aggregator.aggregate([api1(), api2()])
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class AggregationConfig:
    """Configuration for aggregation."""
    dedup_by: Optional[str] = None
    merge_strategy: str = "first"
    conflict_resolver: Optional[Callable[[Any, Any], Any]] = None


class ResponseAggregator:
    """Aggregate API responses.

    Example:
        >>> agg = ResponseAggregator()
        >>> agg.add_response(api1_response)
        >>> agg.add_response(api2_response)
        >>> merged = agg.merge()
    """

    def __init__(self, config: Optional[AggregationConfig] = None) -> None:
        self.config = config or AggregationConfig()
        self._responses: list[Any] = []
        self._metadata: dict[str, Any] = {}

    def add_response(
        self,
        response: Any,
        source: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add a response to aggregate.

        Args:
            response: Response data.
            source: Optional source identifier.
            metadata: Optional metadata for this response.
        """
        self._responses.append(response)
        if metadata:
            self._metadata[source or len(self._responses)] = metadata

    def merge(self) -> Any:
        """Merge all added responses.

        Returns:
            Merged result.
        """
        if not self._responses:
            return None
        if all(isinstance(r, dict) for r in self._responses):
            return self._merge_dicts()
        elif all(isinstance(r, list) for r in self._responses):
            return self._merge_lists()
        return self._responses[0]

    def _merge_dicts(self) -> dict[str, Any]:
        """Merge dictionary responses."""
        result: dict[str, Any] = {}
        for response in self._responses:
            if not isinstance(response, dict):
                continue
            for key, value in response.items():
                if key not in result:
                    result[key] = value
                else:
                    result[key] = self._resolve_conflict(result[key], value)
        return result

    def _merge_lists(self) -> list[Any]:
        """Merge list responses."""
        if self.config.dedup_by:
            return self._deduplicate_lists()
        result = []
        for response in self._responses:
            if isinstance(response, list):
                result.extend(response)
        return result

    def _deduplicate_lists(self) -> list[dict[str, Any]]:
        """Deduplicate list items by key."""
        seen = set()
        result = []
        for response in self._responses:
            if not isinstance(response, list):
                continue
            for item in response:
                if isinstance(item, dict):
                    key_value = item.get(self.config.dedup_by)
                    if key_value is not None and key_value not in seen:
                        seen.add(key_value)
                        result.append(item)
                else:
                    result.append(item)
        return result

    def _resolve_conflict(self, value1: Any, value2: Any) -> Any:
        """Resolve conflicting values."""
        if self.config.conflict_resolver:
            return self.config.conflict_resolver(value1, value2)
        if self.config.merge_strategy == "first":
            return value1
        elif self.config.merge_strategy == "last":
            return value2
        elif self.config.merge_strategy == "combine":
            if isinstance(value1, list) and isinstance(value2, list):
                return value1 + value2
            return [value1, value2]
        return value1


def aggregate_responses(
    responses: list[Any],
    strategy: str = "first",
) -> Any:
    """Quick aggregate multiple responses.

    Args:
        responses: List of responses.
        strategy: Merge strategy.

    Returns:
        Aggregated result.
    """
    agg = ResponseAggregator(
        config=AggregationConfig(merge_strategy=strategy)
    )
    for response in responses:
        agg.add_response(response)
    return agg.merge()


def merge_by_key(
    items: list[dict[str, Any]],
    key: str,
    merge_strategy: str = "first",
) -> list[dict[str, Any]]:
    """Merge items with the same key value.

    Args:
        items: List of dicts.
        key: Key to merge by.
        merge_strategy: How to handle conflicts.

    Returns:
        Merged list.
    """
    groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        groups[item.get(key)].append(item)
    result = []
    for group_key, group_items in groups.items():
        merged = {}
        for item in group_items:
            for k, v in item.items():
                if k == key:
                    merged[k] = v
                elif k not in merged:
                    merged[k] = v
                else:
                    if merge_strategy == "last":
                        merged[k] = v
                    elif merge_strategy == "combine":
                        if isinstance(merged[k], list):
                            merged[k].append(v) if isinstance(v, list) else merged[k].extend([v])
                        else:
                            merged[k] = [merged[k], v]
        result.append(merged)
    return result


def fan_out_aggregate(
    func: Callable[..., list[Any]],
    items: list[Any],
    max_workers: int = 5,
) -> list[Any]:
    """Fan-out execution and aggregate results.

    Args:
        func: Function that returns a list of results.
        items: Items to process.
        max_workers: Maximum parallel workers.

    Returns:
        Combined results from all executions.
    """
    import concurrent.futures
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(func, item) for item in items]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
            except Exception as e:
                logger.error(f"Fan-out task failed: {e}")
    return results

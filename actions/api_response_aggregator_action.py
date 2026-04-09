"""
API Response Aggregator Action Module.

Combines multiple API responses into unified results with
ranking, deduplication, merging strategies, and conflict resolution.
"""

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, Optional, TypeVar

T = TypeVar("T")


class MergeStrategy(Enum):
    """Strategy for combining overlapping data."""

    PREFER_FIRST = "prefer_first"
    PREFER_LAST = "prefer_last"
    PREFER_NEWER = "prefer_newer"
    PREFER_OLDER = "prefer_older"
    CONCATENATE = "concatenate"
    MERGE_DEEP = "merge_deep"
    CUSTOM = "custom"


class RankingMethod(Enum):
    """Method for ranking aggregated results."""

    SOURCE_ORDER = "source_order"
    FRESHNESS = "freshness"
    RELEVANCE_SCORE = "relevance_score"
    CONFIDENCE = "confidence"
    CUSTOM = "custom"


@dataclass
class AggregatedItem(Generic[T]):
    """A single aggregated result item."""

    data: T
    source_id: str
    rank: float
    timestamp: float
    confidence: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)
    _dedup_key: str = ""

    @property
    def dedup_key(self) -> str:
        """Get deduplication key."""
        if not self._dedup_key:
            self._dedup_key = self._compute_key()
        return self._dedup_key

    def _compute_key(self) -> str:
        """Compute hash-based dedup key from data."""
        data_str = str(self.data)
        return hashlib.sha256(data_str.encode()).hexdigest()[:24]


@dataclass
class AggregationConfig:
    """Configuration for aggregation behavior."""

    merge_strategy: MergeStrategy = MergeStrategy.PREFER_LAST
    ranking_method: RankingMethod = RankingMethod.SOURCE_ORDER
    max_results: int = 100
    deduplicate: bool = True
    dedup_similarity_threshold: float = 0.95
    preserve_metadata: bool = True
    conflict_resolver: Optional[Callable[[list[Any]], Any]] = None


@dataclass
class AggregationStats:
    """Statistics for aggregation operations."""

    total_responses: int = 0
    total_items: int = 0
    deduped_items: int = 0
    merged_items: int = 0
    ranking_time: float = 0.0
    merge_time: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Export as dictionary."""
        return {
            "total_responses": self.total_responses,
            "total_items": self.total_items,
            "deduped_items": self.deduped_items,
            "merged_items": self.merged_items,
            "ranking_time": round(self.ranking_time, 4),
            "merge_time": round(self.merge_time, 4),
        }


class APIResponseAggregator:
    """
    Aggregates multiple API responses with configurable merging and ranking.

    Supports deduplication, deep merging, conflict resolution,
    and flexible ranking strategies.
    """

    def __init__(self, config: Optional[AggregationConfig] = None) -> None:
        """
        Initialize the aggregator.

        Args:
            config: Aggregation configuration.
        """
        self._config = config or AggregationConfig()
        self._stats = AggregationStats()

    def add_response(
        self,
        source_id: str,
        items: list[Any],
        metadata: Optional[dict[str, Any]] = None,
        timestamp: Optional[float] = None,
    ) -> list[AggregatedItem[Any]]:
        """
        Add a response from a source to be aggregated.

        Args:
            source_id: Identifier for this response source.
            items: List of data items from this response.
            metadata: Optional metadata about the response.
            timestamp: Optional timestamp (defaults to now).

        Returns:
            List of AggregatedItem wrappers.
        """
        self._stats.total_responses += 1
        ts = timestamp or time.time()
        results: list[AggregatedItem[Any]] = []

        for idx, item in enumerate(items):
            rank = self._calculate_rank(idx, ts, metadata)
            confidence = metadata.get("confidence", 1.0) if metadata else 1.0
            agg_item = AggregatedItem(
                data=item,
                source_id=source_id,
                rank=rank,
                timestamp=ts,
                confidence=confidence,
                metadata=metadata or {},
            )
            results.append(agg_item)
            self._stats.total_items += 1

        return results

    def _calculate_rank(
        self,
        index: int,
        timestamp: float,
        metadata: Optional[dict[str, Any]],
    ) -> float:
        """Calculate rank score based on ranking method."""
        if self._config.ranking_method == RankingMethod.SOURCE_ORDER:
            return -index
        elif self._config.ranking_method == RankingMethod.FRESHNESS:
            return timestamp
        elif self._config.ranking_method == RankingMethod.RELEVANCE_SCORE:
            return metadata.get("relevance_score", 0.0) if metadata else 0.0
        elif self._config.ranking_method == RankingMethod.CONFIDENCE:
            return metadata.get("confidence", 1.0) if metadata else 1.0
        return -index

    def _deduplicate(
        self,
        items: list[AggregatedItem[Any]],
    ) -> list[AggregatedItem[Any]]:
        """Remove duplicate items based on dedup key."""
        if not self._config.deduplicate:
            return items
        seen: dict[str, AggregatedItem[Any]] = {}
        unique: list[AggregatedItem[Any]] = []

        for item in items:
            key = item.dedup_key
            if key not in seen:
                seen[key] = item
                unique.append(item)
            else:
                self._stats.deduped_items += 1
                existing = seen[key]
                if self._should_replace(existing, item):
                    seen[key] = item
                    unique[unique.index(existing)] = item

        return unique

    def _should_replace(
        self,
        existing: AggregatedItem[Any],
        new: AggregatedItem[Any],
    ) -> bool:
        """Determine if new item should replace existing based on merge strategy."""
        if self._config.merge_strategy == MergeStrategy.PREFER_FIRST:
            return False
        if self._config.merge_strategy == MergeStrategy.PREFER_LAST:
            return True
        if self._config.merge_strategy == MergeStrategy.PREFER_NEWER:
            return new.timestamp > existing.timestamp
        if self._config.merge_strategy == MergeStrategy.PREFER_OLDER:
            return new.timestamp < existing.timestamp
        if self._config.merge_strategy == MergeStrategy.RELEVANCE_SCORE:
            return new.rank > existing.rank
        return False

    def _merge_values(
        self,
        values: list[Any],
        path: str = "",
    ) -> Any:
        """Merge multiple values based on merge strategy."""
        if not values:
            return None
        if len(values) == 1:
            return values[0]

        if self._config.merge_strategy == MergeStrategy.CONCATENATE:
            if all(isinstance(v, list) for v in values):
                result: list[Any] = []
                for v in values:
                    result.extend(v)
                return result
            return values[-1]

        if self._config.merge_strategy == MergeStrategy.MERGE_DEEP:
            if all(isinstance(v, dict) for v in values):
                merged: dict[str, Any] = {}
                for v in values:
                    for k, val in v.items():
                        if k in merged:
                            merged[k] = self._merge_values(
                                [merged[k], val], f"{path}.{k}"
                            )
                        else:
                            merged[k] = val
                return merged

        if self._config.conflict_resolver:
            return self._config.conflict_resolver(values)

        return values[-1]

    def aggregate(
        self,
        responses: list[tuple[str, list[Any], Optional[dict[str, Any]], Optional[float]]],
    ) -> list[AggregatedItem[Any]]:
        """
        Aggregate multiple responses into a unified result list.

        Args:
            responses: List of (source_id, items, metadata, timestamp) tuples.

        Returns:
            Deduplicated, ranked, and merged result list.
        """
        start_merge = time.time()
        all_items: list[AggregatedItem[Any]] = []

        for source_id, items, metadata, timestamp in responses:
            agg_items = self.add_response(source_id, items, metadata, timestamp)
            all_items.extend(agg_items)

        all_items = self._deduplicate(all_items)
        all_items = self._rank(all_items)
        self._stats.merge_time = time.time() - start_merge

        if self._config.max_results > 0 and len(all_items) > self._config.max_results:
            all_items = all_items[: self._config.max_results]

        return all_items

    def _rank(
        self,
        items: list[AggregatedItem[Any]],
    ) -> list[AggregatedItem[Any]]:
        """Rank items based on configured ranking method."""
        start_rank = time.time()
        sorted_items = sorted(items, key=lambda x: x.rank, reverse=True)
        for idx, item in enumerate(sorted_items):
            item.rank = float(len(items) - idx)
        self._stats.ranking_time = time.time() - start_rank
        return sorted_items

    def merge_two(
        self,
        left: list[AggregatedItem[Any]],
        right: list[AggregatedItem[Any]],
    ) -> list[AggregatedItem[Any]]:
        """
        Merge two already-aggregated lists.

        Args:
            left: First list of aggregated items.
            right: Second list of aggregated items.

        Returns:
            Merged result list.
        """
        combined = left + right
        return self._deduplicate(self._rank(combined))

    def stats(self) -> AggregationStats:
        """Return current aggregation statistics."""
        return self._stats


def create_aggregator(
    merge_strategy: MergeStrategy = MergeStrategy.PREFER_LAST,
    ranking_method: RankingMethod = RankingMethod.SOURCE_ORDER,
    max_results: int = 100,
) -> APIResponseAggregator:
    """
    Factory function to create a configured aggregator.

    Args:
        merge_strategy: Strategy for merging overlapping data.
        ranking_method: Method for ranking results.
        max_results: Maximum number of results to return.

    Returns:
        Configured APIResponseAggregator instance.
    """
    config = AggregationConfig(
        merge_strategy=merge_strategy,
        ranking_method=ranking_method,
        max_results=max_results,
    )
    return APIResponseAggregator(config)

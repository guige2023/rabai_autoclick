"""API response aggregator action for merging multiple API responses.

Combines responses from multiple endpoints into a single
unified response with conflict resolution strategies.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ConflictResolution(Enum):
    """Strategies for resolving data conflicts."""
    LATEST = "latest"
    FIRST = "first"
    MERGE = "merge"
    PRIORITY = "priority"


@dataclass
class AggregatedResponse:
    """Result of aggregating multiple responses."""
    data: dict[str, Any]
    sources: list[str]
    conflicts_resolved: int
    aggregation_time_ms: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResponseSource:
    """A single API response source."""
    name: str
    data: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    priority: int = 1


class APIResponseAggregatorAction:
    """Aggregate responses from multiple API sources.

    Args:
        conflict_resolution: Strategy for handling conflicting data.

    Example:
        >>> aggregator = APIResponseAggregatorAction()
        >>> result = aggregator.aggregate(responses)
    """

    def __init__(
        self,
        conflict_resolution: ConflictResolution = ConflictResolution.LATEST,
    ) -> None:
        self.conflict_resolution = conflict_resolution
        self._sources: list[ResponseSource] = []

    def add_source(self, source: ResponseSource) -> "APIResponseAggregatorAction":
        """Add a response source.

        Args:
            source: Response source to add.

        Returns:
            Self for method chaining.
        """
        self._sources.append(source)
        return self

    def add_response(
        self,
        name: str,
        data: dict[str, Any],
        priority: int = 1,
    ) -> "APIResponseAggregatorAction":
        """Add a response source by name and data.

        Args:
            name: Source identifier.
            data: Response data dictionary.
            priority: Higher priority sources take precedence.

        Returns:
            Self for method chaining.
        """
        self._sources.append(ResponseSource(name, data, priority=priority))
        return self

    def aggregate(self) -> AggregatedResponse:
        """Aggregate all added responses.

        Returns:
            Aggregated response with merged data.
        """
        import time
        start_time = time.time()

        if not self._sources:
            return AggregatedResponse(
                data={},
                sources=[],
                conflicts_resolved=0,
                aggregation_time_ms=0.0,
            )

        merged: dict[str, Any] = {}
        conflicts = 0

        for key in self._get_all_keys():
            values = [(s, s.data.get(key)) for s in self._sources if key in s.data]

            if len(values) == 1:
                merged[key] = values[0][1]
            else:
                merged[key], resolved = self._resolve_conflict(key, values)
                conflicts += resolved

        result = AggregatedResponse(
            data=merged,
            sources=[s.name for s in self._sources],
            conflicts_resolved=conflicts,
            aggregation_time_ms=(time.time() - start_time) * 1000,
            metadata={
                "resolution_strategy": self.conflict_resolution.value,
                "source_count": len(self._sources),
            },
        )

        return result

    def _get_all_keys(self) -> set[str]:
        """Get all unique keys from all sources.

        Returns:
            Set of all dictionary keys.
        """
        keys: set[str] = set()
        for source in self._sources:
            keys.update(source.data.keys())
        return keys

    def _resolve_conflict(
        self,
        key: str,
        values: list[tuple[ResponseSource, Any]],
    ) -> tuple[Any, int]:
        """Resolve conflict for a single key.

        Args:
            key: The conflicting key.
            values: List of (source, value) tuples.

        Returns:
            Tuple of (resolved value, was_conflict).
        """
        if self.conflict_resolution == ConflictResolution.FIRST:
            return values[0][1], 0

        if self.conflict_resolution == ConflictResolution.LATEST:
            latest = max(values, key=lambda x: x[0].timestamp)
            return latest[1], 1

        if self.conflict_resolution == ConflictResolution.PRIORITY:
            highest = max(values, key=lambda x: x[0].priority)
            return highest[1], 1

        if self.conflict_resolution == ConflictResolution.MERGE:
            if isinstance(values[0][1], dict):
                result = {}
                for source, value in sorted(values, key=lambda x: x[0].priority):
                    result.update(value)
                return result, 1

        return values[0][1], 0

    def clear(self) -> None:
        """Clear all added sources."""
        self._sources.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get aggregation statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            "source_count": len(self._sources),
            "total_keys": len(self._get_all_keys()),
            "strategy": self.conflict_resolution.value,
        }

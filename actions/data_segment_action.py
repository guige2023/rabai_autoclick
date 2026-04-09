"""
Data segmentation module.

Provides data segmentation, grouping, and partitioning
utilities for analytics and processing pipelines.

Author: Aito Auto Agent
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    TypeVar,
)
import hashlib


T = TypeVar('T')
K = TypeVar('K')


class SegmentStrategy(Enum):
    """Segmentation strategy types."""
    HASH = auto()
    ROUND_ROBIN = auto()
    RANGE = auto()
    CUSTOM = auto()


@dataclass
class Segment(Generic[T]):
    """Represents a data segment."""
    id: str
    name: str
    data: list[T] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.data)

    def is_empty(self) -> bool:
        return len(self.data) == 0

    def add(self, item: T) -> None:
        self.data.append(item)

    def remove(self, item: T) -> bool:
        try:
            self.data.remove(item)
            return True
        except ValueError:
            return False


@dataclass
class SegmentStats:
    """Statistics for segments."""
    total_items: int = 0
    segment_count: int = 0
    min_size: int = 0
    max_size: int = 0
    avg_size: float = 0.0
    empty_segments: int = 0


class DataSegmenter(Generic[T]):
    """
    Data segmentation for distributed processing.

    Example:
        segmenter = DataSegmenter(num_segments=4)

        # Add items
        for item in data:
            segmenter.add(item)

        # Process segments
        for segment in segmenter.get_segments():
            process(segment.data)
    """

    def __init__(
        self,
        num_segments: int = 4,
        strategy: SegmentStrategy = SegmentStrategy.HASH,
        custom_key_func: Optional[Callable[[T], str]] = None
    ):
        self._num_segments = num_segments
        self._strategy = strategy
        self._custom_key_func = custom_key_func
        self._segments: dict[int, Segment[T]] = {
            i: Segment(id=str(i), name=f"segment_{i}")
            for i in range(num_segments)
        }
        self._round_robin_index = 0

    def add(self, item: T, segment_id: Optional[int] = None) -> int:
        """
        Add item to a segment.

        Args:
            item: Item to add
            segment_id: Optional specific segment ID

        Returns:
            Segment ID where item was placed
        """
        if segment_id is not None:
            target = segment_id
        elif self._strategy == SegmentStrategy.HASH:
            target = self._hash_segment(item)
        elif self._strategy == SegmentStrategy.ROUND_ROBIN:
            target = self._round_robin_segment()
        elif self._strategy == SegmentStrategy.RANGE:
            target = self._range_segment(item)
        else:
            target = self._hash_segment(item)

        self._segments[target].add(item)
        return target

    def add_batch(self, items: list[T]) -> dict[int, int]:
        """
        Add multiple items to segments.

        Returns:
            Dict mapping segment_id to count of items added
        """
        counts = defaultdict(int)

        for item in items:
            segment_id = self.add(item)
            counts[segment_id] += 1

        return dict(counts)

    def _hash_segment(self, item: T) -> int:
        """Hash-based segmentation."""
        key = self._custom_key_func(item) if self._custom_key_func else str(item)
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_value % self._num_segments

    def _round_robin_segment(self) -> int:
        """Round-robin segmentation."""
        segment = self._round_robin_index
        self._round_robin_index = (self._round_robin_index + 1) % self._num_segments
        return segment

    def _range_segment(self, item: T) -> int:
        """Range-based segmentation."""
        if isinstance(item, (int, float)):
            return int(item) % self._num_segments
        return self._hash_segment(item)

    def get_segment(self, segment_id: int) -> Segment[T]:
        """Get segment by ID."""
        return self._segments.get(segment_id, Segment(id=str(segment_id), name=f"segment_{segment_id}"))

    def get_segments(self) -> list[Segment[T]]:
        """Get all segments."""
        return list(self._segments.values())

    def get_non_empty_segments(self) -> list[Segment[T]]:
        """Get all non-empty segments."""
        return [s for s in self._segments.values() if not s.is_empty()]

    def get_stats(self) -> SegmentStats:
        """Get segmentation statistics."""
        sizes = [len(s) for s in self._segments.values()]
        non_empty = [s for s in sizes if s > 0]

        return SegmentStats(
            total_items=sum(sizes),
            segment_count=len(self._segments),
            min_size=min(sizes) if sizes else 0,
            max_size=max(sizes) if sizes else 0,
            avg_size=sum(sizes) / len(sizes) if sizes else 0.0,
            empty_segments=len([s for s in sizes if s == 0])
        )

    def rebalance(self) -> None:
        """Rebalance items across segments."""
        all_items = []
        for segment in self._segments.values():
            all_items.extend(segment.data)
            segment.data.clear()

        self.add_batch(all_items)

    def merge_segments(self, segment_ids: list[int]) -> Segment[T]:
        """
        Merge multiple segments into one.

        Args:
            segment_ids: IDs of segments to merge

        Returns:
            Merged segment
        """
        merged = Segment(id="merged", name="merged_segments")

        for sid in segment_ids:
            if sid in self._segments:
                merged.data.extend(self._segments[sid].data)
                self._segments[sid].data.clear()

        return merged

    def clear(self) -> None:
        """Clear all segments."""
        for segment in self._segments.values():
            segment.data.clear()

    def __len__(self) -> int:
        return sum(len(s) for s in self._segments.values())


class GroupBySegmenter(Generic[T, K]):
    """
    Group data by key function.

    Example:
        segmenter = GroupBySegmenter(key_func=lambda x: x["category"])

        for item in data:
            segmenter.add(item)

        for category, items in segmenter.get_groups().items():
            print(f"{category}: {len(items)}")
    """

    def __init__(self, key_func: Callable[[T], K]):
        self._key_func = key_func
        self._groups: dict[K, list[T]] = defaultdict(list)

    def add(self, item: T) -> K:
        """Add item and return its group key."""
        key = self._key_func(item)
        self._groups[key].append(item)
        return key

    def add_batch(self, items: list[T]) -> dict[K, int]:
        """Add multiple items and return counts."""
        counts = defaultdict(int)

        for item in items:
            key = self.add(item)
            counts[key] += 1

        return dict(counts)

    def get_group(self, key: K) -> list[T]:
        """Get all items in a group."""
        return self._groups.get(key, [])

    def get_groups(self) -> dict[K, list[T]]:
        """Get all groups."""
        return dict(self._groups)

    def get_keys(self) -> list[K]:
        """Get all group keys."""
        return list(self._groups.keys())

    def __len__(self) -> int:
        return len(self._groups)


class TimeWindowSegmenter(Generic[T]):
    """
    Segment data by time windows.

    Example:
        segmenter = TimeWindowSegmenter(
            window_size_seconds=3600,
            key_func=lambda x: x["timestamp"]
        )

        for item in data:
            segmenter.add(item)

        for window, items in segmenter.get_windows().items():
            print(f"{window}: {len(items)}")
    """

    def __init__(
        self,
        window_size_seconds: float,
        key_func: Optional[Callable[[T], float]] = None
    ):
        self._window_size = window_size_seconds
        self._key_func = key_func
        self._windows: dict[int, list[T]] = defaultdict(list)

    def add(self, item: T) -> int:
        """Add item and return its window ID."""
        timestamp = self._get_timestamp(item)
        window_id = int(timestamp / self._window_size)

        self._windows[window_id].append(item)
        return window_id

    def _get_timestamp(self, item: T) -> float:
        """Extract timestamp from item."""
        if self._key_func:
            return self._key_func(item)
        elif isinstance(item, dict) and "timestamp" in item:
            return item["timestamp"]
        elif isinstance(item, (int, float)):
            return item
        else:
            import time
            return time.time()

    def get_window(self, window_id: int) -> list[T]:
        """Get all items in a window."""
        return self._windows.get(window_id, [])

    def get_windows(self) -> dict[int, list[T]]:
        """Get all windows."""
        return dict(self._windows)

    def get_window_range(
        self,
        start_time: float,
        end_time: float
    ) -> list[T]:
        """Get items within time range."""
        start_id = int(start_time / self._window_size)
        end_id = int(end_time / self._window_size)

        results = []
        for wid in range(start_id, end_id + 1):
            results.extend(self._windows.get(wid, []))

        return results

    def get_stats(self) -> dict[str, Any]:
        """Get window statistics."""
        window_sizes = [len(w) for w in self._windows.values()]

        return {
            "total_windows": len(self._windows),
            "total_items": sum(window_sizes),
            "min_per_window": min(window_sizes) if window_sizes else 0,
            "max_per_window": max(window_sizes) if window_sizes else 0,
            "avg_per_window": sum(window_sizes) / len(window_sizes) if window_sizes else 0.0
        }


def create_segmenter(
    num_segments: int = 4,
    strategy: SegmentStrategy = SegmentStrategy.HASH
) -> DataSegmenter:
    """Factory to create a DataSegmenter."""
    return DataSegmenter(num_segments=num_segments, strategy=strategy)


def create_group_by(key_func: Callable) -> GroupBySegmenter:
    """Factory to create a GroupBySegmenter."""
    return GroupBySegmenter(key_func=key_func)


def create_time_window(
    window_size_seconds: float,
    key_func: Optional[Callable] = None
) -> TimeWindowSegmenter:
    """Factory to create a TimeWindowSegmenter."""
    return TimeWindowSegmenter(
        window_size_seconds=window_size_seconds,
        key_func=key_func
    )

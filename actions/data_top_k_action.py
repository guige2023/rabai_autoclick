"""Data Top-K Action Module.

Provides top-K elements tracking using space-efficient
data structures for streaming data analysis.

Author: RabAi Team
"""

from __future__ import annotations

import heapq
import sys
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TopKEntry:
    """Entry in top-K tracker."""
    value: Any
    score: float
    count: int = 1

    def __lt__(self, other: "TopKEntry") -> bool:
        return self.score < other.score


class TopKTracker:
    """Tracks top-K elements with approximate frequency counting."""

    def __init__(self, k: int = 10, min_threshold: float = 0.0):
        self.k = k
        self.min_threshold = min_threshold
        self._heap: List[TopKEntry] = []
        self._counter: Dict[Any, Tuple[float, int]] = {}
        self._total_count = 0

    def add(self, value: Any, score: float = 1.0) -> bool:
        """Add element to tracker."""
        self._total_count += 1

        if value in self._counter:
            current_score, count = self._counter[value]
            new_score = current_score + score
            self._counter[value] = (new_score, count + 1)

            for entry in self._heap:
                if entry.value == value:
                    entry.score = new_score
                    entry.count = count + 1
                    break

            heapq.heapify(self._heap)
            return True

        if len(self._heap) < self.k:
            entry = TopKEntry(value=value, score=score, count=1)
            heapq.heappush(self._heap, entry)
            self._counter[value] = (score, 1)
            return True

        smallest = self._heap[0]

        if score > smallest.score:
            heapq.heappop(self._heap)
            new_entry = TopKEntry(value=value, score=score, count=1)
            heapq.heappush(self._heap, new_entry)
            self._counter[value] = (score, 1)
            return True

        self._counter[value] = (score, 1)
        return False

    def get_top_k(self) -> List[TopKEntry]:
        """Get top-K elements sorted by score."""
        sorted_entries = sorted(self._heap, key=lambda e: e.score, reverse=True)
        return sorted_entries[:self.k]

    def get_count(self, value: Any) -> int:
        """Get count for value."""
        if value not in self._counter:
            return 0
        return self._counter[value][1]

    def get_score(self, value: Any) -> float:
        """Get score for value."""
        if value not in self._counter:
            return 0.0
        return self._counter[value][0]

    def contains(self, value: Any) -> bool:
        """Check if value is in top-K."""
        return value in self._counter

    def get_rank(self, value: Any) -> Optional[int]:
        """Get rank of value (1-indexed)."""
        if value not in self._counter:
            return None

        top_k = self.get_top_k()
        for i, entry in enumerate(top_k):
            if entry.value == value:
                return i + 1

        return None

    def get_frequency(self, value: Any) -> float:
        """Get frequency of value as percentage."""
        if self._total_count == 0:
            return 0.0
        count = self.get_count(value)
        return (count / self._total_count) * 100

    def remove(self, value: Any) -> bool:
        """Remove value from tracker."""
        if value not in self._counter:
            return False

        del self._counter[value]

        for i, entry in enumerate(self._heap):
            if entry.value == value:
                del self._heap[i]
                heapq.heapify(self._heap)
                return True

        return False

    def merge(self, other: "TopKTracker") -> None:
        """Merge another tracker into this one."""
        for entry in other._heap:
            self.add(entry.value, entry.score)

    def clear(self) -> None:
        """Clear all entries."""
        self._heap.clear()
        self._counter.clear()
        self._total_count = 0

    def get_statistics(self) -> Dict[str, Any]:
        """Get tracker statistics."""
        top_k = self.get_top_k()

        return {
            "k": self.k,
            "total_count": self._total_count,
            "tracked_elements": len(self._counter),
            "heap_size": len(self._heap),
            "min_threshold": self.min_threshold,
            "top_elements": [
                {"value": e.value, "score": e.score, "count": e.count}
                for e in top_k[:5]
            ]
        }


class DataTopKAction(BaseAction):
    """Action for top-K tracking operations."""

    def __init__(self):
        super().__init__("data_top_k")
        self._trackers: Dict[str, TopKTracker] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute top-K action."""
        try:
            operation = params.get("operation", "add")

            if operation == "create":
                return self._create(params)
            elif operation == "add":
                return self._add(params)
            elif operation == "get_top":
                return self._get_top(params)
            elif operation == "get_rank":
                return self._get_rank(params)
            elif operation == "contains":
                return self._contains(params)
            elif operation == "remove":
                return self._remove(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "merge":
                return self._merge(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get_tracker(self, name: str) -> Optional[TopKTracker]:
        """Get tracker by name."""
        return self._trackers.get(name)

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create new top-K tracker."""
        name = params.get("name", "default")
        k = params.get("k", 10)
        min_threshold = params.get("min_threshold", 0.0)

        self._trackers[name] = TopKTracker(k=k, min_threshold=min_threshold)

        return ActionResult(
            success=True,
            message=f"Top-K tracker created: {name} (k={k})"
        )

    def _add(self, params: Dict[str, Any]) -> ActionResult:
        """Add element to tracker."""
        name = params.get("name", "default")
        value = params.get("value")
        score = params.get("score", 1.0)

        if name not in self._trackers:
            self._trackers[name] = TopKTracker()

        added = self._trackers[name].add(value, score)
        tracker = self._trackers[name]

        return ActionResult(
            success=True,
            data={
                "added": added,
                "in_top_k": tracker.contains(value),
                "current_rank": tracker.get_rank(value),
                "total_count": tracker._total_count
            }
        )

    def _get_top(self, params: Dict[str, Any]) -> ActionResult:
        """Get top-K elements."""
        name = params.get("name", "default")
        k = params.get("k")

        if name not in self._trackers:
            return ActionResult(success=False, message=f"Tracker not found: {name}")

        tracker = self._trackers[name]
        top_k = tracker.get_top_k()

        if k:
            top_k = top_k[:k]

        return ActionResult(
            success=True,
            data={
                "elements": [
                    {
                        "value": e.value,
                        "score": e.score,
                        "count": e.count,
                        "frequency": tracker.get_frequency(e.value)
                    }
                    for e in top_k
                ]
            }
        )

    def _get_rank(self, params: Dict[str, Any]) -> ActionResult:
        """Get rank of value."""
        name = params.get("name", "default")
        value = params.get("value")

        if name not in self._trackers:
            return ActionResult(success=False, message=f"Tracker not found: {name}")

        tracker = self._trackers[name]
        rank = tracker.get_rank(value)

        return ActionResult(
            success=rank is not None,
            data={
                "value": value,
                "rank": rank,
                "score": tracker.get_score(value),
                "count": tracker.get_count(value)
            }
        )

    def _contains(self, params: Dict[str, Any]) -> ActionResult:
        """Check if value is in top-K."""
        name = params.get("name", "default")
        value = params.get("value")

        if name not in self._trackers:
            return ActionResult(success=False, message=f"Tracker not found: {name}")

        contains = self._trackers[name].contains(value)

        return ActionResult(
            success=True,
            data={"value": value, "in_top_k": contains}
        )

    def _remove(self, params: Dict[str, Any]) -> ActionResult:
        """Remove value from tracker."""
        name = params.get("name", "default")
        value = params.get("value")

        if name not in self._trackers:
            return ActionResult(success=False, message=f"Tracker not found: {name}")

        removed = self._trackers[name].remove(value)

        return ActionResult(
            success=removed,
            message="Removed" if removed else "Not found"
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get tracker statistics."""
        name = params.get("name", "default")

        if name not in self._trackers:
            return ActionResult(success=False, message=f"Tracker not found: {name}")

        stats = self._trackers[name].get_statistics()
        return ActionResult(success=True, data=stats)

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear tracker."""
        name = params.get("name", "default")

        if name not in self._trackers:
            return ActionResult(success=False, message=f"Tracker not found: {name}")

        self._trackers[name].clear()

        return ActionResult(success=True, message=f"Tracker cleared: {name}")

    def _merge(self, params: Dict[str, Any]) -> ActionResult:
        """Merge two trackers."""
        target = params.get("target", "default")
        source = params.get("source")

        if target not in self._trackers:
            self._trackers[target] = TopKTracker()

        if source and source in self._trackers:
            self._trackers[target].merge(self._trackers[source])

        return ActionResult(
            success=True,
            message=f"Merged {source} into {target}"
        )

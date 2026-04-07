"""Bisect action v3 - specialized search and data structures.

Extended bisect utilities for strings, time series,
and specialized data structures.
"""

from __future__ import annotations

import bisect
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from typing import Any, Callable, Sequence

__all__ = [
    "bisect_string_prefix",
    "bisect_string_range",
    "bisect_datetime",
    "bisect_time_range",
    "bisect_weighted_median",
    "bisect_prefix_group",
    "bisect_score",
    "bisect_running_rank",
    "sorted_string_search",
    "sorted_prefix_tree",
    "TimeSeriesIndex",
    "ScoreIndex",
    "PrefixIndex",
    "LexiconIndex",
]


def bisect_string_prefix(strings: Sequence[str], prefix: str) -> tuple[int, int]:
    """Find range of strings starting with prefix.

    Args:
        strings: Sorted string list.
        prefix: Prefix to search for.

    Returns:
        (first_index, last_index) of matching strings.
    """
    if not strings or not prefix:
        return (0, len(strings))
    left = bisect.bisect_left(strings, prefix)
    right = bisect.bisect_right(strings, prefix + chr(255))
    return (left, right)


def bisect_string_range(strings: Sequence[str], lo: str, hi: str) -> tuple[int, int]:
    """Find strings in lexicographic range [lo, hi].

    Args:
        strings: Sorted strings.
        lo: Lower bound.
        hi: Upper bound.

    Returns:
        (left_index, right_index).
    """
    left = bisect.bisect_left(strings, lo)
    right = bisect.bisect_right(strings, hi)
    return (left, right)


def bisect_datetime(timestamps: Sequence[datetime], target: datetime) -> int | None:
    """Find datetime index or insertion point.

    Args:
        timestamps: Sorted datetime list.
        target: Target datetime.

    Returns:
        Index if found, None otherwise.
    """
    ordinals = [t.timestamp() for t in timestamps]
    target_ord = target.timestamp()
    idx = bisect.bisect_left(ordinals, target_ord)
    if idx < len(ordinals) and abs(ordinals[idx] - target_ord) < 1e-9:
        return idx
    return None


def bisect_time_range(timestamps: Sequence[datetime], start: datetime, end: datetime) -> tuple[int, int]:
    """Find datetime range.

    Args:
        timestamps: Sorted datetimes.
        start: Range start.
        end: Range end.

    Returns:
        (first_index, last_index).
    """
    ordinals = [t.timestamp() for t in timestamps]
    start_ord = start.timestamp()
    end_ord = end.timestamp()
    left = bisect.bisect_left(ordinals, start_ord)
    right = bisect.bisect_right(ordinals, end_ord)
    return (left, right)


def bisect_weighted_median(values: Sequence[float], weights: Sequence[float]) -> float:
    """Find weighted median.

    Args:
        values: Sorted values.
        weights: Corresponding weights.

    Returns:
        Weighted median value.
    """
    if len(values) != len(weights):
        raise ValueError("values and weights must have same length")
    if not values:
        raise ValueError("Empty input")
    pairs = sorted(zip(values, weights))
    total_weight = sum(weights)
    cumsum = 0.0
    half = total_weight / 2
    for v, w in pairs:
        cumsum += w
        if cumsum >= half:
            return v
    return values[-1]


def bisect_prefix_group(strings: Sequence[str]) -> dict[str, list[str]]:
    """Group strings by prefix.

    Args:
        strings: Sorted strings.

    Returns:
        Dict mapping prefix to list of matching strings.
    """
    result = {}
    for s in strings:
        if not s:
            continue
        for i in range(1, len(s) + 1):
            prefix = s[:i]
            if prefix not in result:
                left, right = bisect_string_prefix(strings, prefix)
                result[prefix] = list(strings[left:right])
    return result


def bisect_score(items: Sequence[Any], target_score: float, key: Callable[[Any], float]) -> int | None:
    """Binary search by score.

    Args:
        items: Items to search.
        target_score: Score to find.
        key: Function to extract score from item.

    Returns:
        Index if found, None otherwise.
    """
    scores = [key(item) for item in items]
    idx = bisect.bisect_left(scores, target_score)
    if idx < len(scores) and scores[idx] == target_score:
        return idx
    return None


def bisect_running_rank(sorted_values: Sequence[float], value: float) -> int:
    """Get rank of value in sorted list (1-based, ties get average).

    Args:
        sorted_values: Sorted values.
        value: Value to rank.

    Returns:
        1-based rank.
    """
    left = bisect.bisect_left(sorted_values, value)
    right = bisect.bisect_right(sorted_values, value)
    return (left + right + 1) // 2


def sorted_string_search(strings: Sequence[str], pattern: str) -> list[int]:
    """Find all strings containing pattern using binary search.

    Args:
        strings: Sorted strings.
        pattern: Pattern to search.

    Returns:
        List of matching indices.
    """
    results = []
    for i, s in enumerate(strings):
        if pattern in s:
            results.append(i)
    return results


class TimeSeriesIndex:
    """Time-indexed data structure with binary search."""

    def __init__(self) -> None:
        self._times: list[datetime] = []
        self._values: list[Any] = []

    def insert(self, time: datetime, value: Any) -> int:
        """Insert value at time maintaining sort.

        Args:
            time: Timestamp.
            value: Value to store.

        Returns:
            Index where inserted.
        """
        idx = bisect.bisect_left(self._times, time.timestamp())
        self._times.insert(idx, time.timestamp())
        self._values.insert(idx, value)
        return idx

    def query_range(self, start: datetime, end: datetime) -> list[tuple[datetime, Any]]:
        """Query values in time range.

        Args:
            start: Start time.
            end: End time.

        Returns:
            List of (time, value) tuples.
        """
        left, right = bisect_time_range([datetime.fromtimestamp(t) for t in self._times], start, end)
        return [(datetime.fromtimestamp(self._times[i]), self._values[i]) for i in range(left, right)]

    def query_nearest(self, target: datetime) -> tuple[datetime, Any] | None:
        """Find nearest value to target time.

        Args:
            target: Target time.

        Returns:
            (time, value) or None.
        """
        if not self._times:
            return None
        idx = bisect.bisect_left(self._times, target.timestamp())
        candidates = []
        if idx < len(self._times):
            candidates.append((abs(self._times[idx] - target.timestamp()), idx))
        if idx > 0:
            candidates.append((abs(self._times[idx - 1] - target.timestamp()), idx - 1))
        best = min(candidates)
        return (datetime.fromtimestamp(self._times[best[1]]), self._values[best[1]])

    def __len__(self) -> int:
        return len(self._times)


class ScoreIndex:
    """Index sorted by score with O(log n) operations."""

    def __init__(self) -> None:
        self._items: list[Any] = []
        self._scores: list[float] = []
        self._key: Callable[[Any], float] = lambda x: x

    def set_key(self, key: Callable[[Any], float]) -> None:
        """Set score extraction function."""
        self._key = key

    def insert(self, item: Any) -> int:
        """Insert item maintaining sort order.

        Args:
            item: Item to insert.

        Returns:
            Index where inserted.
        """
        score = self._key(item)
        idx = bisect.bisect_left(self._scores, score)
        self._items.insert(idx, item)
        self._scores.insert(idx, score)
        return idx

    def search(self, target_score: float) -> int | None:
        """Find exact score.

        Args:
            target_score: Score to find.

        Returns:
            Index if found, None otherwise.
        """
        return bisect_score(self._items, target_score, self._key)

    def range(self, lo: float, hi: float) -> list[Any]:
        """Get items in score range.

        Args:
            lo: Lower bound.
            hi: Upper bound.

        Returns:
            Items in [lo, hi].
        """
        left = bisect.bisect_left(self._scores, lo)
        right = bisect.bisect_right(self._scores, hi)
        return list(self._items[left:right])

    def k_smallest(self, k: int) -> list[Any]:
        """Get k smallest items."""
        return list(self._items[:k])

    def k_largest(self, k: int) -> list[Any]:
        """Get k largest items."""
        return list(self._items[-k:])

    def __len__(self) -> int:
        return len(self._items)


class PrefixIndex:
    """String prefix index with binary search."""

    def __init__(self) -> None:
        self._strings: list[str] = []

    def insert(self, s: str) -> int:
        """Insert string maintaining order."""
        idx = bisect.bisect_left(self._strings, s)
        self._strings.insert(idx, s)
        return idx

    def search_prefix(self, prefix: str) -> list[str]:
        """Find all strings with given prefix."""
        left, right = bisect_string_prefix(self._strings, prefix)
        return list(self._strings[left:right])

    def search_range(self, lo: str, hi: str) -> list[str]:
        """Find strings in lexicographic range."""
        left, right = bisect_string_range(self._strings, lo, hi)
        return list(self._strings[left:right])

    def longest_common_prefix(self) -> str:
        """Find LCP of all strings."""
        if not self._strings:
            return ""
        if len(self._strings) == 1:
            return self._strings[0]
        lcp = ""
        for chars in zip(*self._strings):
            if len(set(chars)) == 1:
                lcp += chars[0]
            else:
                break
        return lcp

    def __len__(self) -> int:
        return len(self._strings)


class LexiconIndex:
    """Sorted lexicon with word-level indexing."""

    def __init__(self) -> None:
        self._words: list[str] = []

    def insert(self, text: str) -> None:
        """Insert text (tokenized by spaces)."""
        for word in text.lower().split():
            idx = bisect.bisect_left(self._words, word)
            if idx >= len(self._words) or self._words[idx] != word:
                self._words.insert(idx, word)

    def search_exact(self, word: str) -> bool:
        """Check if exact word exists."""
        idx = bisect.bisect_left(self._words, word.lower())
        return idx < len(self._words) and self._words[idx] == word.lower()

    def search_prefix(self, prefix: str) -> list[str]:
        """Find words starting with prefix."""
        return PrefixIndex().search_prefix(self._words)

    def autocomplete(self, prefix: str, limit: int = 10) -> list[str]:
        """Get autocomplete suggestions."""
        matches = self.search_prefix(prefix.lower())
        return matches[:limit]

    def __len__(self) -> int:
        return len(self._words)

"""joiner action module for rabai_autoclick.

Provides data joining and merging operations: hash join, sort-merge join,
nested loop join, cross join, and set operations on collections.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar, Union

__all__ = [
    "JoinType",
    "JoinResult",
    "HashJoiner",
    "SortMergeJoiner",
    "NestedLoopJoiner",
    "CrossJoiner",
    "join",
    "hash_join",
    "sort_merge_join",
    "inner_join",
    "left_join",
    "right_join",
    "full_join",
    "cross_join",
    "merge_sorted",
    "union_all",
    "union_distinct",
    "intersect",
    "except_set",
    "symmetric_diff",
]


T = TypeVar("T")
K = TypeVar("K")


class JoinType(Enum):
    """Types of joins."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    SEMI = "semi"
    ANTI = "anti"


@dataclass
class JoinResult(Generic[T]):
    """Result of a join operation."""
    items: List[T]
    left_unmatched: List[Any] = None
    right_unmatched: List[Any] = None


class HashJoiner:
    """Hash-based join for large datasets."""

    def __init__(
        self,
        left_key: Callable[[T], K],
        right_key: Callable[[T], K],
    ) -> None:
        self.left_key = left_key
        self.right_key = right_key

    def build_hash_table(
        self,
        items: Sequence[T],
        key_fn: Callable[[T], K],
    ) -> Dict[K, List[T]]:
        """Build hash table from items."""
        table: Dict[K, List[T]] = defaultdict(list)
        for item in items:
            key = key_fn(item)
            table[key].append(item)
        return dict(table)

    def join(
        self,
        left: Sequence[T],
        right: Sequence[T],
        join_type: JoinType = JoinType.INNER,
    ) -> JoinResult:
        """Execute hash join.

        Args:
            left: Left relation.
            right: Right relation.
            join_type: Type of join.

        Returns:
            JoinResult with matched and unmatched items.
        """
        right_hash = self.build_hash_table(right, self.right_key)
        left_hash = self.build_hash_table(left, self.left_key)
        matched_left: set = set()
        matched_right: set = set()
        results: List = []

        for l_idx, l_item in enumerate(left):
            l_key = self.left_key(l_item)
            if l_key in right_hash:
                for r_item in right_hash[l_key]:
                    matched_left.add(l_idx)
                    results.append((l_item, r_item))

        if join_type == JoinType.INNER:
            return JoinResult(items=results)
        elif join_type == JoinType.LEFT:
            unmatched = [left[i] for i in range(len(left)) if i not in matched_left]
            return JoinResult(items=results, left_unmatched=unmatched)
        elif join_type == JoinType.RIGHT:
            matched_right_keys = {self.right_key(r) for _, r in results}
            unmatched = [r for r in right if self.right_key(r) not in matched_right_keys]
            return JoinResult(items=results, right_unmatched=unmatched)
        elif join_type == JoinType.FULL:
            unmatched_left = [left[i] for i in range(len(left)) if i not in matched_left]
            matched_right_keys = {self.right_key(r) for _, r in results}
            unmatched_right = [r for r in right if self.right_key(r) not in matched_right_keys]
            return JoinResult(
                items=results,
                left_unmatched=unmatched_left,
                right_unmatched=unmatched_right,
            )
        return JoinResult(items=results)


class SortMergeJoiner:
    """Sort-merge join for pre-sorted inputs."""

    def __init__(
        self,
        left_key: Callable[[T], Any],
        right_key: Callable[[T], Any],
    ) -> None:
        self.left_key = left_key
        self.right_key = right_key

    def join(
        self,
        left: Sequence[T],
        right: Sequence[T],
        join_type: JoinType = JoinType.INNER,
    ) -> JoinResult:
        """Execute sort-merge join.

        Args:
            left: Left relation (must be sorted by join key).
            right: Right relation (must be sorted by join key).
            join_type: Type of join.

        Returns:
            JoinResult with matched and unmatched items.
        """
        sorted_left = sorted(left, key=self.left_key)
        sorted_right = sorted(right, key=self.right_key)

        results: List = []
        l_idx = 0
        r_idx = 0
        matched_left: set = set()
        matched_right: set = set()

        while l_idx < len(sorted_left) and r_idx < len(sorted_right):
            l_key = self.left_key(sorted_left[l_idx])
            r_key = self.right_key(sorted_right[r_idx])

            if l_key < r_key:
                l_idx += 1
            elif l_key > r_key:
                r_idx += 1
            else:
                matched_left.add(l_idx)
                matched_right.add(r_idx)
                results.append((sorted_left[l_idx], sorted_right[r_idx]))
                l_idx += 1
                r_idx += 1

        return JoinResult(items=results)


class NestedLoopJoiner:
    """Nested loop join (simple but O(n*m))."""

    def __init__(
        self,
        left_key: Callable[[T], Any],
        right_key: Callable[[T], Any],
    ) -> None:
        self.left_key = left_key
        self.right_key = right_key

    def join(
        self,
        left: Sequence[T],
        right: Sequence[T],
        join_type: JoinType = JoinType.INNER,
    ) -> JoinResult:
        """Execute nested loop join."""
        results: List = []
        matched_left: set = set()
        matched_right: set = set()

        for l_idx, l_item in enumerate(left):
            for r_idx, r_item in enumerate(right):
                if self.left_key(l_item) == self.right_key(r_item):
                    matched_left.add(l_idx)
                    matched_right.add(r_idx)
                    results.append((l_item, r_item))

        return JoinResult(items=results)


class CrossJoiner:
    """Cartesian product join."""

    def __init__(self) -> None:
        pass

    def join(
        self,
        left: Sequence[T],
        right: Sequence[T],
    ) -> List[Tuple[T, T]]:
        """Execute cross join (cartesian product)."""
        return [(l, r) for l in left for r in right]


def hash_join(
    left: Sequence[T],
    right: Sequence[T],
    left_key: Callable[[T], K],
    right_key: Callable[[T], K],
    join_type: JoinType = JoinType.INNER,
) -> JoinResult:
    """Hash-based join helper.

    Args:
        left: Left relation.
        right: Right relation.
        left_key: Key extractor for left.
        right_key: Key extractor for right.
        join_type: Type of join.

    Returns:
        JoinResult with matched items.
    """
    joiner = HashJoiner(left_key, right_key)
    return joiner.join(left, right, join_type)


def sort_merge_join(
    left: Sequence[T],
    right: Sequence[T],
    left_key: Callable[[T], Any],
    right_key: Callable[[T], Any],
    join_type: JoinType = JoinType.INNER,
) -> JoinResult:
    """Sort-merge join helper."""
    joiner = SortMergeJoiner(left_key, right_key)
    return joiner.join(left, right, join_type)


def inner_join(
    left: Sequence[T],
    right: Sequence[T],
    left_key: Callable[[T], K],
    right_key: Callable[[T], K],
) -> List[Tuple[T, T]]:
    """Inner join (only matching pairs)."""
    result = hash_join(left, right, left_key, right_key, JoinType.INNER)
    return result.items


def left_join(
    left: Sequence[T],
    right: Sequence[T],
    left_key: Callable[[T], K],
    right_key: Callable[[T], K],
) -> Tuple[List[Tuple[T, T]], List[T]]:
    """Left join (all left + matched right)."""
    result = hash_join(left, right, left_key, right_key, JoinType.LEFT)
    return result.items, result.left_unmatched


def right_join(
    left: Sequence[T],
    right: Sequence[T],
    left_key: Callable[[T], K],
    right_key: Callable[[T], K],
) -> Tuple[List[Tuple[T, T]], List[T]]:
    """Right join (matched left + all right)."""
    result = hash_join(left, right, left_key, right_key, JoinType.RIGHT)
    return result.items, result.right_unmatched


def full_join(
    left: Sequence[T],
    right: Sequence[T],
    left_key: Callable[[T], K],
    right_key: Callable[[T], K],
) -> Tuple[List[Tuple[T, T]], List[T], List[T]]:
    """Full outer join (all left, all right, matched pairs)."""
    result = hash_join(left, right, left_key, right_key, JoinType.FULL)
    return result.items, result.left_unmatched, result.right_unmatched


def cross_join(
    left: Sequence[T],
    right: Sequence[T],
) -> List[Tuple[T, T]]:
    """Cartesian product of left and right."""
    return CrossJoiner().join(left, right)


def merge_sorted(
    left: Sequence[T],
    right: Sequence[T],
    key: Callable[[T], Any],
) -> List[T]:
    """Merge two sorted sequences.

    Args:
        left: First sorted sequence.
        right: Second sorted sequence.
        key: Key function for comparison.

    Returns:
        Merged sorted sequence.
    """
    result: List[T] = []
    l_idx, r_idx = 0, 0

    while l_idx < len(left) and r_idx < len(right):
        if key(left[l_idx]) <= key(right[r_idx]):
            result.append(left[l_idx])
            l_idx += 1
        else:
            result.append(right[r_idx])
            r_idx += 1

    result.extend(left[l_idx:])
    result.extend(right[r_idx:])
    return result


def union_all(left: Sequence[T], right: Sequence[T]) -> List[T]:
    """Concatenate sequences (including duplicates)."""
    return list(left) + list(right)


def union_distinct(left: Sequence[T], right: Sequence[T]) -> List[T]:
    """Union of two sequences (no duplicates)."""
    seen: set = set()
    result: List[T] = []
    for item in list(left) + list(right):
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def intersect(left: Sequence[T], right: Sequence[T]) -> List[T]:
    """Intersection of two sequences (items in both)."""
    right_set = set(right)
    seen: set = set()
    result: List[T] = []
    for item in left:
        if item in right_set and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def except_set(left: Sequence[T], right: Sequence[T]) -> List[T]:
    """Set difference: items in left but not in right."""
    right_set = set(right)
    seen: set = set()
    result: List[T] = []
    for item in left:
        if item not in right_set and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def symmetric_diff(left: Sequence[T], right: Sequence[T]) -> List[T]:
    """Symmetric difference: items in either but not both."""
    left_only = except_set(left, right)
    right_only = except_set(right, left)
    return left_only + right_only


def join(
    left: Sequence[T],
    right: Sequence[T],
    key: Callable[[T], K],
    join_type: str = "inner",
) -> JoinResult:
    """Generic join helper.

    Args:
        left: Left sequence.
        right: Right sequence.
        key: Key function (used for both sides).
        join_type: Join type ("inner", "left", "right", "full", "cross").

    Returns:
        JoinResult with matched items.
    """
    jtype = JoinType.INNER
    if join_type == "left":
        jtype = JoinType.LEFT
    elif join_type == "right":
        jtype = JoinType.RIGHT
    elif join_type == "full":
        jtype = JoinType.FULL
    elif join_type == "cross":
        jtype = JoinType.CROSS

    if jtype == JoinType.CROSS:
        return JoinResult(items=cross_join(left, right))

    return hash_join(left, right, key, key, jtype)


from enum import Enum

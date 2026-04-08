"""
Container Utilities

Provides utilities for working with containers,
collections, and data structures in automation.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, TypeVar, Generic, Iterator
from dataclasses import dataclass

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


@dataclass
class ContainerStats:
    """Statistics about a container."""
    size: int
    is_empty: bool
    unique_count: int | None = None


class ContainerUtils:
    """
    Utilities for working with containers.
    
    Provides common operations on collections
    and data structures.
    """

    @staticmethod
    def stats(items: list[T] | tuple[T, ...]) -> ContainerStats:
        """Get statistics about a container."""
        return ContainerStats(
            size=len(items),
            is_empty=len(items) == 0,
            unique_count=len(set(items)),
        )

    @staticmethod
    def group_by(
        items: list[T],
        key_func: Callable[[T], K],
    ) -> dict[K, list[T]]:
        """Group items by a key function."""
        result: dict[K, list[T]] = {}
        for item in items:
            key = key_func(item)
            if key not in result:
                result[key] = []
            result[key].append(item)
        return result

    @staticmethod
    def partition(
        items: list[T],
        predicate: Callable[[T], bool],
    ) -> tuple[list[T], list[T]]:
        """Partition items into two lists based on predicate."""
        matched = []
        unmatched = []
        for item in items:
            if predicate(item):
                matched.append(item)
            else:
                unmatched.append(item)
        return matched, unmatched

    @staticmethod
    def flatten(nested: list[list[T]]) -> list[T]:
        """Flatten a nested list."""
        result = []
        for inner in nested:
            result.extend(inner)
        return result

    @staticmethod
    def unique(items: list[T], key: Callable[[T], Any] | None = None) -> list[T]:
        """Get unique items preserving order."""
        if key is None:
            seen = set()
            result = []
            for item in items:
                if item not in seen:
                    seen.add(item)
                    result.append(item)
            return result
        else:
            seen = set()
            result = []
            for item in items:
                k = key(item)
                if k not in seen:
                    seen.add(k)
                    result.append(item)
            return result


def batch(items: list[T], size: int) -> list[list[T]]:
    """
    Split items into batches of specified size.
    
    Args:
        items: Items to batch.
        size: Batch size.
        
    Returns:
        List of batches.
    """
    result = []
    for i in range(0, len(items), size):
        result.append(items[i:i + size])
    return result


def chunk(items: list[T], chunk_count: int) -> list[list[T]]:
    """
    Split items into roughly equal chunks.
    
    Args:
        items: Items to chunk.
        chunk_count: Number of chunks.
        
    Returns:
        List of chunks.
    """
    if chunk_count <= 0:
        return [items]
    return batch(items, (len(items) + chunk_count - 1) // chunk_count)


def transpose(matrix: list[list[T]]) -> list[list[T]]:
    """Transpose a matrix (list of lists)."""
    if not matrix or not matrix[0]:
        return []
    return [list(row) for row in zip(*matrix)]

"""
Count-Min Sketch algorithm implementation for frequency estimation.

Provides memory-efficient probabilistic data structure for
estimating item frequencies in streaming data.

Example:
    >>> from utils.count_min_sketch_utils import CountMinSketch
    >>> sketch = CountMinSketch(width=1000, depth=10)
    >>> sketch.add("item")
    >>> sketch.estimate("item")
"""

from __future__ import annotations

import hashlib
import struct
from typing import Any, Dict, Optional


class CountMinSketch:
    """
    Count-Min Sketch for frequency estimation.

    Uses a table of hash functions to estimate the count
    of items. False positives are possible, but false
    negatives are not.

    Attributes:
        width: Number of columns in the table.
        depth: Number of hash functions (rows).
    """

    def __init__(
        self,
        width: int = 1000,
        depth: int = 10,
        seed: int = 0x1234ABCD,
    ) -> None:
        """
        Initialize the Count-Min Sketch.

        Args:
            width: Number of columns (larger = lower false positive rate).
            depth: Number of hash functions (larger = lower overestimation).
            seed: Random seed for hash functions.
        """
        self.width = width
        self.depth = depth
        self.seed = seed
        self._table = [[0] * width for _ in range(depth)]
        self._total_items = 0

    def _hash(self, item: Any, row: int) -> int:
        """
        Generate a hash for an item at a specific row.

        Args:
            item: Item to hash.
            row: Row index (determines which hash function).

        Returns:
            Hash value within [0, width).
        """
        data = f"{self.seed}:{row}:{item}".encode("utf-8")
        hash_val = hashlib.md5(data).digest()
        return struct.unpack("<Q", hash_val[:8])[0] % self.width

    def add(self, item: Any, count: int = 1) -> None:
        """
        Add an item to the sketch.

        Args:
            item: Item to add.
            count: Number to increment by.
        """
        for row in range(self.depth):
            col = self._hash(item, row)
            self._table[row][col] += count
        self._total_items += count

    def __iadd__(self, item: Any) -> "CountMinSketch":
        """Support += operator."""
        self.add(item)
        return self

    def estimate(self, item: Any) -> int:
        """
        Estimate the count of an item.

        Args:
            item: Item to estimate.

        Returns:
            Estimated count (minimum across all rows).
        """
        min_count = float("inf")
        for row in range(self.depth):
            col = self._hash(item, row)
            min_count = min(min_count, self._table[row][col])
        return int(min_count)

    def merge(self, other: "CountMinSketch") -> "CountMinSketch":
        """
        Merge another sketch into this one.

        Args:
            other: Another sketch with same dimensions.

        Returns:
            This sketch (modified in place).

        Raises:
            ValueError: If dimensions don't match.
        """
        if self.width != other.width or self.depth != other.depth:
            raise ValueError("Cannot merge sketches with different dimensions")

        for row in range(self.depth):
            for col in range(self.width):
                self._table[row][col] += other._table[row][col]

        self._total_items += other._total_items
        return self

    def merge_of(self, *sketches: "CountMinSketch") -> "CountMinSketch":
        """
        Create a new sketch that is the merge of multiple sketches.

        Args:
            *sketches: Sketches to merge.

        Returns:
            New merged sketch.
        """
        if not sketches:
            return CountMinSketch(self.width, self.depth, self.seed)

        result = CountMinSketch(self.width, self.depth, self.seed)
        for sketch in sketches:
            result.merge(sketch)
        return result

    @property
    def total_items(self) -> int:
        """Get total number of items added."""
        return self._total_items

    @property
    def table_size(self) -> int:
        """Get the memory size of the table."""
        return self.width * self.depth

    def clear(self) -> None:
        """Reset the sketch to empty state."""
        self._table = [[0] * self.width for _ in range(self.depth)]
        self._total_items = 0

    def top_k(self, k: int = 10, items: Optional[list] = None) -> list:
        """
        Get approximate top-k most frequent items.

        Args:
            k: Number of top items to return.
            items: Optional list of items to check (if not provided,
                   only previously seen items can be estimated).

        Returns:
            List of (item, estimated_count) tuples.
        """
        if items is None:
            return []

        estimates = [(item, self.estimate(item)) for item in items]
        estimates.sort(key=lambda x: x[1], reverse=True)
        return estimates[:k]


class CountMinSketchWithDecay:
    """
    Count-Min Sketch with temporal decay.

    Applies decay to older counts for better tracking of
    recent frequency patterns.
    """

    def __init__(
        self,
        width: int = 1000,
        depth: int = 10,
        decay_factor: float = 0.99,
        seed: int = 0x1234ABCD,
    ) -> None:
        """
        Initialize the decaying Count-Min Sketch.

        Args:
            width: Number of columns.
            depth: Number of hash functions.
            decay_factor: Factor to multiply counts by each tick (0-1).
            seed: Random seed for hash functions.
        """
        self.width = width
        self.depth = depth
        self.decay_factor = decay_factor
        self._sketch = CountMinSketch(width, depth, seed)
        self._tick_count = 0

    def add(self, item: Any, count: int = 1) -> None:
        """Add an item with decay."""
        self._apply_decay()
        self._sketch.add(item, count)

    def _apply_decay(self) -> None:
        """Apply decay to all counts."""
        if self.decay_factor >= 1.0:
            return

        for row in range(self._sketch.depth):
            for col in range(self._sketch.width):
                self._sketch._table[row][col] = int(
                    self._sketch._table[row][col] * self.decay_factor
                )

        self._sketch._total_items = int(
            self._sketch._total_items * self.decay_factor
        )
        self._tick_count += 1

    def estimate(self, item: Any) -> int:
        """Estimate the count of an item."""
        return self._sketch.estimate(item)

    def clear(self) -> None:
        """Clear the sketch."""
        self._sketch.clear()
        self._tick_count = 0


def create_count_min_sketch(
    epsilon: float = 0.01,
    delta: float = 0.01,
) -> CountMinSketch:
    """
    Factory to create a Count-Min Sketch with given error bounds.

    Args:
        epsilon: Error bound on frequency estimates (relative).
        delta: Probability of error.

    Returns:
        Configured CountMinSketch instance.
    """
    import math

    width = int(math.ceil(math.e / epsilon))
    depth = int(math.ceil(math.log(1 / delta)))

    return CountMinSketch(width=width, depth=depth)

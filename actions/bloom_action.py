"""bloom_action module for rabai_autoclick.

Provides Bloom filter implementation: probabilistic set membership,
scalable Bloom filter, and counting Bloom filter.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from typing import Any, Callable, Optional, Sequence

__all__ = [
    "BloomFilter",
    "ScalableBloomFilter",
    "CountingBloomFilter",
    "PartitionedBloomFilter",
    "BloomFilterError",
    "OptimalParams",
    "estimate_fpp",
    "optimal_size",
]


class BloomFilterError(Exception):
    """Raised when Bloom filter operations fail."""
    pass


@dataclass
class OptimalParams:
    """Optimal Bloom filter parameters."""
    size: int
    num_hashes: int
    fpp: float


def estimate_fpp(
    num_items: int,
    size_bits: int,
    num_hashes: int,
) -> float:
    """Estimate false positive probability.

    Args:
        num_items: Number of items to insert.
        size_bits: Size of filter in bits.
        num_hashes: Number of hash functions.

    Returns:
        Estimated FPP.
    """
    if size_bits == 0 or num_hashes == 0:
        return 1.0
    return (1.0 - math.exp(-num_hashes * num_items / size_bits)) ** num_hashes


def optimal_size(num_items: int, fpp: float) -> tuple[int, int]:
    """Calculate optimal size and hash count.

    Args:
        num_items: Expected number of items.
        fpp: Desired false positive probability.

    Returns:
        (size_bits, num_hashes) tuple.
    """
    if fpp <= 0 or fpp >= 1:
        raise BloomFilterError("FPP must be between 0 and 1")
    if num_items <= 0:
        raise BloomFilterError("num_items must be positive")

    size_bits = int(-num_items * math.log(fpp) / (math.log(2) ** 2))
    num_hashes = int((size_bits / num_items) * math.log(2))

    return max(1, size_bits), max(1, num_hashes)


class BloomFilter:
    """Standard Bloom filter for probabilistic set membership."""

    def __init__(
        self,
        size: int = 1000,
        num_hashes: int = 7,
        hash_func: Optional[Callable[[bytes], int]] = None,
    ) -> None:
        self.size = size
        self.num_hashes = num_hashes
        self.hash_func = hash_func or (lambda x: int(hashlib.md5(x).hexdigest(), 16))
        self._bits: list = [False] * size
        self._count = 0

    def _get_bit_indices(self, item: bytes) -> list:
        """Get bit indices for item using double hashing."""
        h1 = self.hash_func(item) % self.size
        h2 = self.hash_func(item + b"salt") % (self.size - 1) + 1
        return [(h1 + i * h2) % self.size for i in range(self.num_hashes)]

    def add(self, item: bytes) -> bool:
        """Add item to filter.

        Args:
            item: Item to add (bytes).

        Returns:
            True if already present (might be FP), False if newly added.
        """
        indices = self._get_bit_indices(item)
        already_set = all(self._bits[i] for i in indices)
        for i in indices:
            self._bits[i] = True
        if not already_set:
            self._count += 1
        return already_set

    def __contains__(self, item: bytes) -> bool:
        """Check if item might be in set."""
        indices = self._get_bit_indices(item)
        return all(self._bits[i] for i in indices)

    def might_contain(self, item: bytes) -> bool:
        """Check if item might be in set."""
        return item in self

    def is_empty(self) -> bool:
        """Check if filter is empty."""
        return self._count == 0

    def fill_rate(self) -> float:
        """Get fraction of bits that are set."""
        return sum(self._bits) / self.size

    def clear(self) -> None:
        """Clear all bits."""
        self._bits = [False] * self.size
        self._count = 0

    def __len__(self) -> int:
        return self._count


class ScalableBloomFilter:
    """Scalable Bloom filter that grows as needed."""

    def __init__(
        self,
        initial_size: int = 1000,
        initial_hashes: int = 7,
        fpp: float = 0.01,
        scale_factor: float = 2.0,
    ) -> None:
        self.initial_size = initial_size
        self.initial_hashes = initial_hashes
        self.fpp = fpp
        self.scale_factor = scale_factor
        self._filters: list = []
        self._current: Optional[BloomFilter] = None
        self._total_count = 0
        self._add_filter()

    def _add_filter(self) -> None:
        """Add a new filter to the chain."""
        if self._filters:
            size = int(self._filters[-1].size * self.scale_factor)
        else:
            size = self.initial_size
        f = BloomFilter(size=size, num_hashes=self.initial_hashes)
        self._filters.append(f)
        self._current = f

    def add(self, item: bytes) -> bool:
        """Add item to filter."""
        if self._current is None:
            self._add_filter()
        result = self._current.add(item)
        self._total_count += 1
        return result

    def __contains__(self, item: bytes) -> bool:
        """Check if item might be in set."""
        return any(item in f for f in self._filters)

    def might_contain(self, item: bytes) -> bool:
        """Check if item might be in set."""
        return item in self

    def fill_rate(self) -> float:
        """Get average fill rate of filters."""
        if not self._filters:
            return 0.0
        return sum(f.fill_rate() for f in self._filters) / len(self._filters)

    def __len__(self) -> int:
        return self._total_count


class CountingBloomFilter:
    """Counting Bloom filter that supports deletions."""

    def __init__(
        self,
        size: int = 1000,
        num_hashes: int = 7,
        counter_bits: int = 4,
    ) -> None:
        self.size = size
        self.num_hashes = num_hashes
        self.counter_bits = counter_bits
        self.max_counter = (1 << counter_bits) - 1
        self._counters: list = [0] * size
        self._count = 0

    def _get_bit_indices(self, item: bytes) -> list:
        """Get bit indices for item."""
        h1 = hash(item) & 0xFFFFFFFF
        h2 = hash(item + "salt") & 0xFFFFFFFF
        return [(h1 + i * h2) & 0xFFFFFFFF % self.size for i in range(self.num_hashes)]

    def add(self, item: bytes) -> None:
        """Add item to filter."""
        indices = self._get_bit_indices(item)
        for i in indices:
            if self._counters[i] < self.max_counter:
                self._counters[i] += 1
        self._count += 1

    def remove(self, item: bytes) -> bool:
        """Remove item from filter."""
        indices = self._get_bit_indices(item)
        existed = all(self._counters[i] > 0 for i in indices)
        if existed:
            for i in indices:
                if self._counters[i] > 0:
                    self._counters[i] -= 1
            self._count -= 1
        return existed

    def __contains__(self, item: bytes) -> bool:
        """Check if item might be in set."""
        indices = self._get_bit_indices(item)
        return all(self._counters[i] > 0 for i in indices)

    def count(self, item: bytes) -> int:
        """Get approximate count for item."""
        indices = self._get_bit_indices(item)
        return min(self._counters[i] for i in indices)

    def __len__(self) -> int:
        return self._count


class PartitionedBloomFilter:
    """Partitioned Bloom filter for parallel insertion."""

    def __init__(
        self,
        num_partitions: int = 8,
        items_per_partition: int = 1000,
        fpp: float = 0.01,
    ) -> None:
        self.num_partitions = num_partitions
        self._partitions: list = []
        size_bits, num_hashes = optimal_size(items_per_partition, fpp)
        for _ in range(num_partitions):
            self._partitions.append(BloomFilter(size=size_bits, num_hashes=num_hashes))

    def add(self, item: bytes) -> bool:
        """Add item to filter using partitioned hashing."""
        partition_idx = self.hash_func(item) % self.num_partitions
        return self._partitions[partition_idx].add(item)

    def hash_func(self, item: bytes) -> int:
        """Hash function to determine partition."""
        return int(hashlib.sha256(item).hexdigest(), 16)

    def __contains__(self, item: bytes) -> bool:
        """Check if item might be in set."""
        partition_idx = self.hash_func(item) % self.num_partitions
        return item in self._partitions[partition_idx]

    def __len__(self) -> int:
        return sum(len(p) for p in self._partitions)

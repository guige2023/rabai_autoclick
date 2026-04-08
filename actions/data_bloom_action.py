"""
Data Bloom Filter Action Module.

Probabilistic membership testing using bloom filters,
space-efficient set membership with configurable false positive rate.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass
import logging
import math
import mmh3
import copy

logger = logging.getLogger(__name__)


@dataclass
class BloomConfig:
    """Bloom filter configuration."""
    expected_items: int = 10000
    false_positive_rate: float = 0.01
    num_hashes: int = 0
    bit_size: int = 0


class DataBloomFilterAction:
    """
    Bloom filter for probabilistic membership testing.

    Space-efficient set membership with tunable
    false positive rate.

    Example:
        bloom = DataBloomFilterAction(expected_items=10000, fpr=0.01)
        bloom.add("item1")
        bloom.add("item2")
        print(bloom.contains("item1"))  # True
        print(bloom.contains("item3"))  # False (or False positive)
    """

    def __init__(
        self,
        expected_items: int = 10000,
        false_positive_rate: float = 0.01,
        bit_array: Optional[list[bool]] = None,
    ) -> None:
        self.expected_items = expected_items
        self.false_positive_rate = false_positive_rate

        self._num_hashes = self._optimal_num_hashes(
            expected_items, false_positive_rate
        )
        self._bit_size = self._optimal_bit_size(
            expected_items, false_positive_rate
        )

        if bit_array:
            self._bits = bit_array
        else:
            self._bits = [False] * self._bit_size

        self._count = 0

    def add(self, item: Any) -> None:
        """Add an item to the bloom filter."""
        for seed in range(self._num_hashes):
            idx = self._get_index(item, seed)
            self._bits[idx] = True
        self._count += 1

    def contains(self, item: Any) -> bool:
        """Check if item might be in the set."""
        for seed in range(self._num_hashes):
            idx = self._get_index(item, seed)
            if not self._bits[idx]:
                return False
        return True

    def might_contain(self, item: Any) -> bool:
        """Alias for contains()."""
        return self.contains(item)

    def clear(self) -> None:
        """Reset the bloom filter."""
        self._bits = [False] * self._bit_size
        self._count = 0

    def union(self, other: "DataBloomFilterAction") -> None:
        """Union this bloom filter with another."""
        if self._bit_size != other._bit_size:
            raise ValueError("Cannot union bloom filters with different sizes")

        for i in range(self._bit_size):
            self._bits[i] = self._bits[i] or other._bits[i]

        self._count = max(self._count, other._count)

    def intersection(
        self,
        other: "DataBloomFilterAction",
    ) -> "DataBloomFilterAction":
        """Intersect this bloom filter with another."""
        if self._bit_size != other._bit_size:
            raise ValueError("Cannot intersect bloom filters with different sizes")

        result_bits = [
            self._bits[i] and other._bits[i]
            for i in range(self._bit_size)
        ]

        result = DataBloomFilterAction.__new__(DataBloomFilterAction)
        result._bits = result_bits
        result._bit_size = self._bit_size
        result._num_hashes = self._num_hashes
        result._count = min(self._count, other._count)
        result.expected_items = self.expected_items
        result.false_positive_rate = self.false_positive_rate

        return result

    def _get_index(self, item: Any, seed: int) -> int:
        """Get bit index for item using MurmurHash3."""
        if isinstance(item, str):
            data = item.encode()
        elif isinstance(item, bytes):
            data = item
        else:
            data = str(item).encode()

        hash_val = mmh3.hash64(data, seed=seed, signed=False)
        if isinstance(hash_val, tuple):
            hash_val = hash_val[0]

        return hash_val % self._bit_size

    @staticmethod
    def _optimal_num_hashes(
        n: int,
        p: float,
    ) -> int:
        """Calculate optimal number of hash functions."""
        if n <= 0 or p <= 0:
            return 7
        m = -n * math.log(p) / (math.log(2) ** 2)
        k = (m / n) * math.log(2)
        return max(1, int(round(k)))

    @staticmethod
    def _optimal_bit_size(
        n: int,
        p: float,
    ) -> int:
        """Calculate optimal bit array size."""
        if n <= 0 or p <= 0:
            return 1000
        m = -n * math.log(p) / (math.log(2) ** 2)
        return max(1, int(math.ceil(m)))

    @property
    def num_bits(self) -> int:
        """Number of bits in the filter."""
        return self._bit_size

    @property
    def num_hashes(self) -> int:
        """Number of hash functions."""
        return self._num_hashes

    @property
    def count(self) -> int:
        """Number of items added."""
        return self._count

    def estimated_fill_ratio(self) -> float:
        """Estimate how full the filter is."""
        return sum(self._bits) / self._bit_size if self._bit_size > 0 else 0.0

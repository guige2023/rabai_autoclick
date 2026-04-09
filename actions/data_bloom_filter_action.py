"""
Data Bloom Filter Action Module.

Provides a memory-efficient probabilistic data structure for
membership testing in data processing pipelines.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Union
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import logging
import math

logger = logging.getLogger(__name__)


@dataclass
class BloomFilterStats:
    """Statistics for a bloom filter."""
    size_bits: int
    size_bytes: int
    item_count: int
    expected_false_positive_rate: float
    actual_false_positive_rate: Optional[float] = None
    hash_functions: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "size_bits": self.size_bits,
            "size_bytes": self.size_bytes,
            "item_count": self.item_count,
            "expected_false_positive_rate": self.expected_false_positive_rate,
            "actual_false_positive_rate": self.actual_false_positive_rate,
            "hash_functions": self.hash_functions,
        }


class DataBloomFilterAction:
    """
    Implements a Bloom Filter for efficient membership testing.

    A Bloom filter is a probabilistic data structure that can
    test whether an element is possibly in a set or definitely not.
    It may give false positives but never false negatives.

    Example:
        >>> bloom = DataBloomFilterAction(expected_items=10000, false_positive_rate=0.01)
        >>> bloom.add("item1")
        >>> bloom.contains("item1")
        True
        >>> bloom.contains("item2")
        False
    """

    def __init__(
        self,
        expected_items: int = 10000,
        false_positive_rate: float = 0.01,
        custom_hash_func: Optional[Callable[[Any], int]] = None,
    ):
        """
        Initialize the Bloom Filter.

        Args:
            expected_items: Expected number of items to store.
            false_positive_rate: Desired false positive probability.
            custom_hash_func: Optional custom hash function.
        """
        self.expected_items = expected_items
        self.false_positive_rate = false_positive_rate
        self.custom_hash_func = custom_hash_func

        self._size_bits = self._calculate_size(expected_items, false_positive_rate)
        self._hash_count = self._calculate_hash_count(self._size_bits, expected_items)
        self._bit_array: List[bool] = [False] * self._size_bits
        self._item_count = 0

    @staticmethod
    def _calculate_size(n: int, p: float) -> int:
        """Calculate optimal bit array size."""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(math.ceil(m))

    @staticmethod
    def _calculate_hash_count(m: int, n: int) -> int:
        """Calculate optimal number of hash functions."""
        k = (m / n) * math.log(2)
        return max(1, int(math.ceil(k)))

    def _hash(self, item: Any, seed: int) -> int:
        """Hash an item with a seed."""
        if self.custom_hash_func:
            hash_val = self.custom_hash_func(item)
        else:
            item_bytes = str(item).encode("utf-8")
            hash_val = int(hashlib.sha256(item_bytes + str(seed).encode()).hexdigest(), 16)

        return hash_val % self._size_bits

    def _get_hashes(self, item: Any) -> List[int]:
        """Get all hash values for an item."""
        return [self._hash(item, i) for i in range(self._hash_count)]

    def add(self, item: Any) -> None:
        """
        Add an item to the filter.

        Args:
            item: Item to add.
        """
        for hash_val in self._get_hashes(item):
            self._bit_array[hash_val] = True
        self._item_count += 1

    def add_many(self, items: List[Any]) -> None:
        """
        Add multiple items to the filter.

        Args:
            items: Items to add.
        """
        for item in items:
            self.add(item)

    def contains(self, item: Any) -> bool:
        """
        Check if an item might be in the filter.

        Args:
            item: Item to check.

        Returns:
            True if possibly present, False if definitely not.
        """
        for hash_val in self._get_hashes(item):
            if not self._bit_array[hash_val]:
                return False
        return True

    def might_contain(self, item: Any) -> bool:
        """Alias for contains()."""
        return self.contains(item)

    def definitely_not_contains(self, item: Any) -> bool:
        """
        Check if an item is definitely not in the filter.

        Args:
            item: Item to check.

        Returns:
            True if definitely not present.
        """
        return not self.contains(item)

    def get_stats(self) -> BloomFilterStats:
        """Get filter statistics."""
        expected_fp = self.false_positive_rate

        return BloomFilterStats(
            size_bits=self._size_bits,
            size_bytes=self._size_bits // 8,
            item_count=self._item_count,
            expected_false_positive_rate=expected_fp,
            hash_functions=self._hash_count,
        )

    def clear(self) -> None:
        """Clear the filter."""
        self._bit_array = [False] * self._size_bits
        self._item_count = 0

    def union(self, other: "DataBloomFilterAction") -> bool:
        """
        Perform OR operation with another bloom filter.

        Args:
            other: Another filter of the same size.

        Returns:
            True if union succeeded.
        """
        if self._size_bits != other._size_bits:
            logger.error("Cannot union filters of different sizes")
            return False

        for i in range(self._size_bits):
            self._bit_array[i] = self._bit_array[i] or other._bit_array[i]

        self._item_count = max(self._item_count, other._item_count)
        return True

    def intersection(self, other: "DataBloomFilterAction") -> bool:
        """
        Perform AND operation with another bloom filter.

        Args:
            other: Another filter of the same size.

        Returns:
            True if intersection succeeded.
        """
        if self._size_bits != other._size_bits:
            logger.error("Cannot intersect filters of different sizes")
            return False

        for i in range(self._size_bits):
            self._bit_array[i] = self._bit_array[i] and other._bit_array[i]

        return True

    def estimated_count(self) -> int:
        """Estimate the number of items in the filter."""
        if self._item_count == 0:
            return 0

        set_bits = sum(1 for bit in self._bit_array if bit)
        m = self._size_bits
        k = self._hash_count

        if set_bits == 0:
            return 0

        count = math.log(1 - set_bits / m) / (k * math.log(1 - 1 / m))
        return max(0, int(-count))

    def fill_ratio(self) -> float:
        """Get the ratio of set bits to total bits."""
        set_bits = sum(1 for bit in self._bit_array if bit)
        return set_bits / self._size_bits if self._size_bits > 0 else 0

    def serialize(self) -> bytes:
        """
        Serialize the filter to bytes.

        Returns:
            Serialized filter data.
        """
        import json

        data = {
            "size_bits": self._size_bits,
            "hash_count": self._hash_count,
            "expected_items": self.expected_items,
            "false_positive_rate": self.false_positive_rate,
            "bit_array": [
                1 if bit else 0 for bit in self._bit_array
            ],
            "item_count": self._item_count,
        }

        json_str = json.dumps(data)
        return json_str.encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "DataBloomFilterAction":
        """
        Deserialize a filter from bytes.

        Args:
            data: Serialized filter data.

        Returns:
            Reconstructed BloomFilterAction.
        """
        import json

        parsed = json.loads(data.decode("utf-8"))

        filter_obj = cls(
            expected_items=parsed["expected_items"],
            false_positive_rate=parsed["false_positive_rate"],
        )

        filter_obj._size_bits = parsed["size_bits"]
        filter_obj._hash_count = parsed["hash_count"]
        filter_obj._item_count = parsed["item_count"]
        filter_obj._bit_array = [
            bool(bit) for bit in parsed["bit_array"]
        ]

        return filter_obj


class ScalableBloomFilter(DataBloomFilterAction):
    """
    A scalable bloom filter that grows as needed.

    Starts with a small filter and adds new segments when
    the false positive rate gets too high.
    """

    def __init__(
        self,
        initial_items: int = 1000,
        false_positive_rate: float = 0.01,
        max_filters: int = 10,
    ):
        """
        Initialize scalable bloom filter.

        Args:
            initial_items: Initial expected items.
            false_positive_rate: Target false positive rate.
            max_filters: Maximum number of filter segments.
        """
        super().__init__(initial_items, false_positive_rate)
        self.max_filters = max_filters
        self._filters: List[DataBloomFilterAction] = [self]
        self._current_filter_index = 0

    def add(self, item: Any) -> None:
        """Add an item to the current filter."""
        current = self._filters[self._current_filter_index]

        if current.fill_ratio() > 0.5 and len(self._filters) < self.max_filters:
            new_filter = DataBloomFilterAction(
                expected_items=current.expected_items * 2,
                false_positive_rate=current.false_positive_rate,
            )
            self._filters.append(new_filter)
            self._current_filter_index += 1

        self._filters[self._current_filter_index].add(item)

    def contains(self, item: Any) -> bool:
        """Check if item might be in any filter."""
        return any(f.contains(item) for f in self._filters)

    def get_stats(self) -> Dict[str, Any]:
        """Get combined statistics."""
        total_items = sum(f.item_count for f in self._filters)
        total_bits = sum(f._size_bits for f in self._filters)
        total_bytes = total_bits // 8

        return {
            "filter_count": len(self._filters),
            "total_items": total_items,
            "total_size_bits": total_bits,
            "total_size_bytes": total_bytes,
            "filters": [f.get_stats().to_dict() for f in self._filters],
        }


def create_bloom_filter(
    expected_items: int = 10000,
    false_positive_rate: float = 0.01,
    **kwargs,
) -> DataBloomFilterAction:
    """Factory function to create a DataBloomFilterAction."""
    return DataBloomFilterAction(
        expected_items=expected_items,
        false_positive_rate=false_positive_rate,
        **kwargs,
    )

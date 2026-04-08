"""
Bloom filter data structure implementation.

Provides a production-ready Bloom filter for probabilistic set
membership testing with configurable false positive rate and
optimal hash function selection.

Example:
    >>> from utils.bloom_filter_utils import BloomFilter
    >>> bf = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
    >>> bf.add("hello")
    >>> bf.check("hello")
    True
"""

from __future__ import annotations

import math
import struct
from typing import Any, Callable, List, Optional, Union


class BloomFilter:
    """
    Bloom filter for probabilistic set membership testing.

    Supports adding elements and checking membership with a
    configurable false positive rate. False negatives are impossible.

    Attributes:
        expected_elements: Expected number of elements to be added.
        false_positive_rate: Desired false positive probability.
        num_bits: Total number of bits in the filter.
        num_hashes: Number of hash functions to use.
    """

    _murmur_seed = 0x1234ABCD

    def __init__(
        self,
        expected_elements: int = 100000,
        false_positive_rate: float = 0.01,
        filename: Optional[str] = None,
    ) -> None:
        """
        Initialize the Bloom filter.

        Args:
            expected_elements: Expected number of elements to add.
            false_positive_rate: Desired false positive rate (0-1).
            filename: Optional file to persist the filter.

        Raises:
            ValueError: If parameters are invalid.
        """
        if expected_elements <= 0:
            raise ValueError("expected_elements must be positive")
        if not 0 < false_positive_rate < 1:
            raise ValueError("false_positive_rate must be between 0 and 1")

        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        self.filename = filename

        self.num_bits = self._optimal_size(expected_elements, false_positive_rate)
        self.num_hashes = self._optimal_hashes(self.num_bits, expected_elements)

        self._bit_array = bytearray((self.num_bits + 7) // 8)

        if filename:
            self._load()

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Calculate optimal bit array size."""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(math.ceil(m))

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        """Calculate optimal number of hash functions."""
        k = (m / n) * math.log(2)
        return max(1, int(math.ceil(k)))

    def _set_bit(self, index: int) -> None:
        """Set a bit in the array."""
        byte_idx = index // 8
        bit_idx = index % 8
        self._bit_array[byte_idx] |= 1 << bit_idx

    def _get_bit(self, index: int) -> bool:
        """Get a bit from the array."""
        byte_idx = index // 8
        bit_idx = index % 8
        return bool(self._bit_array[byte_idx] & (1 << bit_idx))

    def _hash(self, item: Any) -> tuple[int, int]:
        """
        Generate two base hash values for double hashing.

        Args:
            item: Element to hash.

        Returns:
            Tuple of (h1, h2) base hash values.
        """
        item_bytes = str(item).encode("utf-8")
        h1 = self._murmur_hash(item_bytes, self._murmur_seed)
        h2 = self._murmur_hash(item_bytes, self._murmur_seed ^ 0x5A3C9F6E)
        return (h1, h2)

    @staticmethod
    def _murmur_hash(data: bytes, seed: int) -> int:
        """MurmurHash3 32-bit implementation."""
        c1 = 0xCC9E2D51
        c2 = 0x1B873593

        length = len(data)
        h1 = seed
        rounded_end = (length // 4) * 4

        for i in range(0, rounded_end, 4):
            k = struct.unpack("<I", data[i : i + 4])[0]
            k = (k * c1) & 0xFFFFFFFF
            k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
            k = (k * c2) & 0xFFFFFFFF
            h1 ^= k
            h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
            h1 = ((h1 * 5) + 0xE6546B64) & 0xFFFFFFFF

        k = 0
        tail = data[rounded_end:]
        if len(tail) >= 3:
            k ^= tail[2] << 16
        if len(tail) >= 2:
            k ^= tail[1] << 8
        if tail:
            k ^= tail[0]
            k = (k * c1) & 0xFFFFFFFF
            k = ((k << 15) | (k >> 17)) & 0xFFFFFFFF
            k = (k * c2) & 0xFFFFFFFF
            h1 ^= k

        h1 ^= length
        h1 ^= h1 >> 16
        h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
        h1 ^= h1 >> 13
        h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
        h1 ^= h1 >> 16

        return h1 % self.num_bits

    def _get_indices(self, item: Any) -> List[int]:
        """Get bit indices for an item using double hashing."""
        h1, h2 = self._hash(item)
        return [(h1 + i * h2) % self.num_bits for i in range(self.num_hashes)]

    def add(self, item: Any) -> None:
        """
        Add an item to the filter.

        Args:
            item: Element to add.
        """
        for index in self._get_indices(item):
            self._set_bit(index)

    def check(self, item: Any) -> bool:
        """
        Check if an item might be in the filter.

        Args:
            item: Element to check.

        Returns:
            True if possibly in set, False if definitely not.
        """
        return all(self._get_bit(index) for index in self._get_indices(item))

    def __contains__(self, item: Any) -> bool:
        """Support 'in' operator."""
        return self.check(item)

    def __add__(self, item: Any) -> None:
        """Support + operator for adding."""
        self.add(item)

    @property
    def fill_ratio(self) -> float:
        """Get the fraction of bits that are set."""
        bits_set = sum(1 for byte in self._bit_array for _ in range(8))
        return bits_set / self.num_bits

    def save(self) -> None:
        """Save the filter to file."""
        if not self.filename:
            return
        with open(self.filename, "wb") as f:
            f.write(struct.pack("<I", self.expected_elements))
            f.write(struct.pack("<d", self.false_positive_rate))
            f.write(struct.pack("<I", self.num_bits))
            f.write(struct.pack("<I", self.num_hashes))
            f.write(self._bit_array)

    def _load(self) -> None:
        """Load the filter from file."""
        if not self.filename:
            return
        try:
            with open(self.filename, "rb") as f:
                self.expected_elements = struct.unpack("<I", f.read(4))[0]
                self.false_positive_rate = struct.unpack("<d", f.read(8))[0]
                self.num_bits = struct.unpack("<I", f.read(4))[0]
                self.num_hashes = struct.unpack("<I", f.read(4))[0]
                self._bit_array = bytearray(f.read())
        except (FileNotFoundError, struct.error):
            pass

    def union(self, other: BloomFilter) -> BloomFilter:
        """
        Create a new filter that is the union of this and another.

        Args:
            other: Another BloomFilter with same size and hash count.

        Returns:
            New filter representing the union.

        Raises:
            ValueError: If filters are incompatible.
        """
        if (
            self.num_bits != other.num_bits
            or self.num_hashes != other.num_hashes
        ):
            raise ValueError("Cannot union filters with different parameters")

        result = BloomFilter(
            self.expected_elements, self.false_positive_rate
        )
        result._bit_array = bytearray(
            a | b
            for a, b in zip(self._bit_array, other._bit_array)
        )
        return result

    def intersection(self, other: BloomFilter) -> BloomFilter:
        """
        Create a new filter that is the intersection of this and another.

        Args:
            other: Another BloomFilter with same size and hash count.

        Returns:
            New filter representing the intersection.
        """
        if (
            self.num_bits != other.num_bits
            or self.num_hashes != other.num_hashes
        ):
            raise ValueError("Cannot intersect filters with different parameters")

        result = BloomFilter(
            self.expected_elements, self.false_positive_rate
        )
        result._bit_array = bytearray(
            a & b
            for a, b in zip(self._bit_array, other._bit_array)
        )
        return result

    def clear(self) -> None:
        """Clear all bits in the filter."""
        self._bit_array = bytearray((self.num_bits + 7) // 8)


class CountingBloomFilter:
    """
    Counting Bloom filter that supports removal of elements.

    Uses counters instead of bits to track element occurrences,
    allowing false positive removal.

    Attributes:
        expected_elements: Expected number of elements.
        false_positive_rate: Desired false positive rate.
        counter_bits: Bits per counter.
    """

    def __init__(
        self,
        expected_elements: int = 100000,
        false_positive_rate: float = 0.01,
        counter_bits: int = 4,
    ) -> None:
        """
        Initialize the counting Bloom filter.

        Args:
            expected_elements: Expected number of elements.
            false_positive_rate: Desired false positive rate.
            counter_bits: Bits per counter (determines max count).
        """
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        self.counter_bits = counter_bits

        num_bits = BloomFilter._optimal_size(expected_elements, false_positive_rate)
        self.num_hashes = BloomFilter._optimal_hashes(num_bits, expected_elements)
        self.num_counters = num_bits

        self._max_counter = (1 << counter_bits) - 1
        self._counters = bytearray(self.num_counters * counter_bits)

    def _set_counter(self, index: int, value: int) -> None:
        """Set a counter value."""
        value = min(value, self._max_counter)
        for bit in range(self.counter_bits):
            byte_idx = (index * self.counter_bits + bit) // 8
            bit_idx = (index * self.counter_bits + bit) % 8
            if value & (1 << bit):
                self._counters[byte_idx] |= 1 << bit_idx

    def _get_counter(self, index: int) -> int:
        """Get a counter value."""
        value = 0
        for bit in range(self.counter_bits):
            byte_idx = (index * self.counter_bits + bit) // 8
            bit_idx = (index * self.counter_bits + bit) % 8
            if self._counters[byte_idx] & (1 << bit_idx):
                value |= 1 << bit
        return value

    def _increment_counter(self, index: int) -> None:
        """Increment a counter."""
        current = self._get_counter(index)
        if current < self._max_counter:
            self._set_counter(index, current + 1)

    def _decrement_counter(self, index: int) -> None:
        """Decrement a counter."""
        current = self._get_counter(index)
        if current > 0:
            self._set_counter(index, current - 1)

    def _get_indices(self, item: Any) -> List[int]:
        """Get counter indices using double hashing."""
        item_bytes = str(item).encode("utf-8")
        h1 = BloomFilter._murmur_hash(item_bytes, 0x1234ABCD)
        h2 = BloomFilter._murmur_hash(item_bytes, 0x5A3C9F6E)
        return [(h1 + i * h2) % self.num_counters for i in range(self.num_hashes)]

    def add(self, item: Any) -> None:
        """Add an item to the filter."""
        for index in self._get_indices(item):
            self._increment_counter(index)

    def remove(self, item: Any) -> None:
        """Remove an item from the filter."""
        for index in self._get_indices(item):
            self._decrement_counter(index)

    def check(self, item: Any) -> bool:
        """Check if an item might be in the filter."""
        return all(self._get_counter(index) > 0 for index in self._get_indices(item))


def create_bloom_filter(
    expected_elements: int = 100000,
    false_positive_rate: float = 0.01,
    counting: bool = False,
    **kwargs
) -> Union[BloomFilter, CountingBloomFilter]:
    """
    Factory function to create a Bloom filter.

    Args:
        expected_elements: Expected number of elements.
        false_positive_rate: Desired false positive rate.
        counting: Use CountingBloomFilter.
        **kwargs: Additional arguments.

    Returns:
        BloomFilter or CountingBloomFilter instance.
    """
    if counting:
        return CountingBloomFilter(
            expected_elements=expected_elements,
            false_positive_rate=false_positive_rate,
            **kwargs
        )
    return BloomFilter(
        expected_elements=expected_elements,
        false_positive_rate=false_positive_rate,
        **kwargs
    )

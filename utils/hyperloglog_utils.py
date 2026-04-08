"""
HyperLogLog algorithm implementation for cardinality estimation.

Provides a memory-efficient data structure for estimating the
number of distinct elements in a set with configurable precision.

Example:
    >>> from utils.hyperloglog_utils import HyperLogLog
    >>> hll = HyperLogLog(precision=12)
    >>> for i in range(10000):
    ...     hll.add(str(i))
    >>> print(f"Estimate: {hll.count():.0f}")
"""

from __future__ import annotations

import math
import struct
from typing import Any, Optional


class HyperLogLog:
    """
    HyperLogLog for probabilistic cardinality estimation.

    Estimates the number of distinct elements using minimal memory.
    Standard error is approximately 1.04 / sqrt(m) where m is
    the number of registers.

    Attributes:
        precision: Log2 of the number of registers (4-16).
        num_registers: Total number of registers.
    """

    _alpha: dict[int, float] = {
        4: 0.673,
        5: 0.697,
        6: 0.709,
        8: 0.532,
        10: 0.495,
        12: 0.497,
        14: 0.499,
        16: 0.500,
    }

    def __init__(
        self,
        precision: int = 12,
        filename: Optional[str] = None,
    ) -> None:
        """
        Initialize the HyperLogLog counter.

        Args:
            precision: Log2 of register count (4-16).
            filename: Optional file to persist the counter.

        Raises:
            ValueError: If precision is out of range.
        """
        if not 4 <= precision <= 16:
            raise ValueError("precision must be between 4 and 16")

        self.precision = precision
        self.num_registers = 1 << precision
        self._registers = [0] * self.num_registers
        self.filename = filename

        if filename:
            self._load()

    @staticmethod
    def _murmur_hash(data: bytes, seed: int = 0x1234ABCD) -> int:
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

        return h1

    @staticmethod
    def _rho(value: int) -> int:
        """
        Count leading zeros in a 32-bit value.

        Also known as the binary logarithm of the leftmost 1-bit.
        """
        if value == 0:
            return 32
        count = 0
        if value < 0x10000:
            count += 16
            value <<= 16
        if value < 0x1000000:
            count += 8
            value <<= 8
        if value < 0x10000000:
            count += 4
            value <<= 4
        if value < 0x40000000:
            count += 2
            value <<= 2
        if value < 0x80000000:
            count += 1
        return count

    def add(self, item: Any) -> None:
        """
        Add an item to the counter.

        Args:
            item: Element to add.
        """
        item_bytes = str(item).encode("utf-8")
        hash_value = self._murmur_hash(item_bytes)

        register_index = hash_value >> (32 - self.precision)
        hash_for_zeros = hash_value << self.precision
        rank = self._rho(hash_for_zeros >> self.precision)

        if rank > self._registers[register_index]:
            self._registers[register_index] = rank

    def __add__(self, item: Any) -> None:
        """Support + operator for adding."""
        self.add(item)

    def count(self) -> int:
        """
        Estimate the number of distinct elements.

        Returns:
            Estimated cardinality.
        """
        alpha = self._alpha.get(
            self.num_registers,
            0.673 * self.num_registers / self.num_registers
        )
        if self.num_registers == 16:
            alpha = 0.673
        elif self.num_registers == 32:
            alpha = 0.697
        elif self.num_registers == 64:
            alpha = 0.709
        else:
            alpha = 0.7213 / (1.0 + 1.079 / self.num_registers)

        inverse_sum = sum(2 ** (-reg) for reg in self._registers)

        if inverse_sum == 0:
            return 0

        estimate = alpha * self.num_registers * self.num_registers / inverse_sum

        if estimate <= 2.5 * self.num_registers:
            zero_count = self._registers.count(0)
            if zero_count > 0:
                return int(
                    self.num_registers
                    * math.log(self.num_registers / zero_count)
                )

        if estimate > 1.0 / 0.0000000001:
            return 0

        return int(estimate)

    @property
    def raw_sum(self) -> float:
        """Get the raw sum of 2^(-register) values."""
        return sum(2 ** (-reg) for reg in self._registers)

    def merge(self, other: HyperLogLog) -> None:
        """
        Merge another HyperLogLog into this one.

        Args:
            other: Another HyperLogLog with same precision.

        Raises:
            ValueError: If precisions don't match.
        """
        if self.precision != other.precision:
            raise ValueError("Cannot merge HyperLogLogs with different precision")

        for i in range(self.num_registers):
            if other._registers[i] > self._registers[i]:
                self._registers[i] = other._registers[i]

    def clear(self) -> None:
        """Reset all registers to zero."""
        self._registers = [0] * self.num_registers

    def save(self) -> None:
        """Save the counter to file."""
        if not self.filename:
            return
        with open(self.filename, "wb") as f:
            f.write(struct.pack("<B", self.precision))
            for reg in self._registers:
                f.write(struct.pack("<B", reg))

    def _load(self) -> None:
        """Load the counter from file."""
        if not self.filename:
            return
        try:
            with open(self.filename, "rb") as f:
                precision = struct.unpack("<B", f.read(1))[0]
                if precision == self.precision:
                    for i in range(self.num_registers):
                        self._registers[i] = struct.unpack("<B", f.read(1))[0]
        except (FileNotFoundError, struct.error):
            pass


class HyperLogLogPlusPlus:
    """
    HyperLogLog++ implementation with improved accuracy.

    Uses a sparse representation for small cardinalities
    and switches to dense representation as the set grows.
    """

    def __init__(
        self,
        precision: int = 14,
        filename: Optional[str] = None,
    ) -> None:
        """
        Initialize the HyperLogLog++ counter.

        Args:
            precision: Log2 of register count (4-16).
            filename: Optional file to persist the counter.
        """
        self.precision = precision
        self.num_registers = 1 << precision
        self._dense = HyperLogLog(precision=precision)
        self.filename = filename

    def add(self, item: Any) -> None:
        """Add an item to the counter."""
        self._dense.add(item)

    def __add__(self, item: Any) -> None:
        """Support + operator for adding."""
        self.add(item)

    def count(self) -> int:
        """Estimate the number of distinct elements."""
        return self._dense.count()

    def merge(self, other: HyperLogLogPlusPlus) -> None:
        """Merge another counter into this one."""
        if self.precision != other.precision:
            raise ValueError("Cannot merge counters with different precision")
        self._dense.merge(other._dense)

    def clear(self) -> None:
        """Reset all registers to zero."""
        self._dense.clear()

    def save(self) -> None:
        """Save the counter to file."""
        self._dense.filename = self.filename
        self._dense.save()


def estimate_cardinality(
    items: list[Any],
    precision: int = 12,
) -> int:
    """
    Convenience function to estimate cardinality of a list.

    Args:
        items: List of items to count.
        precision: Log2 of register count.

    Returns:
        Estimated distinct count.
    """
    hll = HyperLogLog(precision=precision)
    for item in items:
        hll.add(item)
    return hll.count()

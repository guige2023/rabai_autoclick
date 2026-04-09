"""Data Bloom Filter Action Module.

Provides probabilistic membership testing using Bloom filters
for fast duplicate detection and set membership queries.
"""

from __future__ import annotations

import logging
import math
import mmh3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class BloomFilterConfig:
    """Configuration for a Bloom filter."""
    expected_items: int = 10000
    false_positive_rate: float = 0.01
    num_hashes: int = 0
    bit_size: int = 0

    def __post_init__(self):
        if self.num_hashes == 0 or self.bit_size == 0:
            self.num_hashes, self.bit_size = self._calculate_optimal(
                self.expected_items,
                self.false_positive_rate
            )

    @staticmethod
    def _calculate_optimal(
        n: int,
        p: float
    ) -> Tuple[int, int]:
        """Calculate optimal number of hashes and bit size.

        Args:
            n: Expected number of items
            p: Desired false positive probability

        Returns:
            Tuple of (num_hashes, bit_size)
        """
        m = math.ceil(-n * math.log(p) / (math.log(2) ** 2))
        k = max(1, round(m / n * math.log(2)))
        return k, m


class BloomFilter:
    """Bloom filter for probabilistic membership testing."""

    def __init__(self, config: Optional[BloomFilterConfig] = None):
        self._config = config or BloomFilterConfig()
        self._bits = [False] * self._config.bit_size
        self._count = 0
        self._hash_count = self._config.num_hashes

    @property
    def config(self) -> BloomFilterConfig:
        """Return filter configuration."""
        return self._config

    @property
    def count(self) -> int:
        """Return number of items added."""
        return self._count

    def _get_bit_positions(self, item: bytes) -> List[int]:
        """Get bit positions for an item using double hashing."""
        h1 = mmh3.hash64(item, signed=False)[0]
        h2 = mmh3.hash64(item, signed=False, seed=1)[0]

        positions = []
        for i in range(self._hash_count):
            pos = (h1 + i * h2) % self._config.bit_size
            positions.append(pos)
        return positions

    def add(self, item: bytes) -> None:
        """Add an item to the filter."""
        for pos in self._get_bit_positions(item):
            self._bits[pos] = True
        self._count += 1

    def add_string(self, s: str) -> None:
        """Add a string to the filter."""
        self.add(s.encode("utf-8"))

    def might_contain(self, item: bytes) -> bool:
        """Check if item might be in the filter.

        Returns:
            True if possibly present, False if definitely not present.
        """
        for pos in self._get_bit_positions(item):
            if not self._bits[pos]:
                return False
        return True

    def might_contain_string(self, s: str) -> bool:
        """Check if string might be in the filter."""
        return self.might_contain(s.encode("utf-8"))

    def estimated_fill_ratio(self) -> float:
        """Return estimated fill ratio."""
        if not self._bits:
            return 0.0
        return sum(self._bits) / len(self._bits)

    def reset(self) -> None:
        """Reset the filter to empty state."""
        self._bits = [False] * self._config.bit_size
        self._count = 0

    def copy(self) -> "BloomFilter":
        """Create a copy of this filter."""
        new_filter = BloomFilter(self._config)
        new_filter._bits = self._bits.copy()
        new_filter._count = self._count
        return new_filter

    def union(self, other: "BloomFilter") -> bool:
        """Union this filter with another. Returns False if incompatible."""
        if len(self._bits) != len(other._bits):
            return False
        for i in range(len(self._bits)):
            self._bits[i] = self._bits[i] or other._bits[i]
        return True

    def intersection(self, other: "BloomFilter") -> bool:
        """Intersect this filter with another. Returns False if incompatible."""
        if len(self._bits) != len(other._bits):
            return False
        for i in range(len(self._bits)):
            self._bits[i] = self._bits[i] and other._bits[i]
        return True


class CountingBloomFilter:
    """Bloom filter with counters for item removal."""

    def __init__(self, config: Optional[BloomFilterConfig] = None, max_count: int = 255):
        self._config = config or BloomFilterConfig()
        self._counters = [0] * self._config.bit_size
        self._count = 0
        self._hash_count = self._config.num_hashes
        self._max_count = max_count

    @property
    def count(self) -> int:
        """Return number of items added."""
        return self._count

    def _get_bit_positions(self, item: bytes) -> List[int]:
        """Get bit positions for an item."""
        h1 = mmh3.hash64(item, signed=False)[0]
        h2 = mmh3.hash64(item, signed=False, seed=1)[0]

        positions = []
        for i in range(self._hash_count):
            pos = (h1 + i * h2) % self._config.bit_size
            positions.append(pos)
        return positions

    def add(self, item: bytes) -> None:
        """Add an item to the filter."""
        for pos in self._get_bit_positions(item):
            if self._counters[pos] < self._max_count:
                self._counters[pos] += 1
        self._count += 1

    def add_string(self, s: str) -> None:
        """Add a string to the filter."""
        self.add(s.encode("utf-8"))

    def remove(self, item: bytes) -> bool:
        """Remove an item from the filter. Returns True if successful."""
        positions = self._get_bit_positions(item)

        # Check if all counters are > 0
        can_remove = all(self._counters[pos] > 0 for pos in positions)
        if not can_remove:
            return False

        for pos in positions:
            if self._counters[pos] > 0:
                self._counters[pos] -= 1
        self._count -= 1
        return True

    def remove_string(self, s: str) -> bool:
        """Remove a string from the filter."""
        return self.remove(s.encode("utf-8"))

    def might_contain(self, item: bytes) -> bool:
        """Check if item might be in the filter."""
        for pos in self._get_bit_positions(item):
            if self._counters[pos] == 0:
                return False
        return True

    def might_contain_string(self, s: str) -> bool:
        """Check if string might be in the filter."""
        return self.might_contain(s.encode("utf-8"))

    def reset(self) -> None:
        """Reset the filter to empty state."""
        self._counters = [0] * self._config.bit_size
        self._count = 0


class ScalableBloomFilter:
    """Scalable Bloom filter that grows as needed."""

    def __init__(
        self,
        initial_config: Optional[BloomFilterConfig] = None,
        max_fill_ratio: float = 0.75,
        growth_factor: float = 2.0
    ):
        self._initial_config = initial_config or BloomFilterConfig()
        self._max_fill_ratio = max_fill_ratio
        self._growth_factor = growth_factor
        self._filters: List[BloomFilter] = [BloomFilter(self._initial_config)]
        self._current_scale = 0
        self._count = 0

    @property
    def count(self) -> int:
        """Return total number of items added."""
        return self._count

    def add(self, item: bytes) -> None:
        """Add an item to the filter."""
        current = self._filters[-1]
        if current.estimated_fill_ratio() > self._max_fill_ratio:
            self._grow()
            current = self._filters[-1]
        current.add(item)
        self._count += 1

    def add_string(self, s: str) -> None:
        """Add a string to the filter."""
        self.add(s.encode("utf-8"))

    def might_contain(self, item: bytes) -> bool:
        """Check if item might be in any filter."""
        return any(f.might_contain(item) for f in self._filters)

    def might_contain_string(self, s: str) -> bool:
        """Check if string might be in the filter."""
        return self.might_contain(s.encode("utf-8"))

    def _grow(self) -> None:
        """Add a new filter with larger capacity."""
        n = self._initial_config.expected_items
        p = self._initial_config.false_positive_rate
        scale = self._current_scale + 1

        new_n = int(n * (self._growth_factor ** scale))
        new_config = BloomFilterConfig(expected_items=new_n, false_positive_rate=p)
        self._filters.append(BloomFilter(new_config))
        self._current_scale += 1

    def get_filters(self) -> List[BloomFilter]:
        """Return all filters."""
        return self._filters.copy()


class DataBloomAction:
    """Main action class for Bloom filter operations."""

    def __init__(self):
        self._bloom = BloomFilter()
        self._counting_bloom: Optional[CountingBloomFilter] = None
        self._scalable_bloom: Optional[ScalableBloomFilter] = None
        self._mode = "standard"

    def configure(
        self,
        mode: str = "standard",
        expected_items: int = 10000,
        false_positive_rate: float = 0.01
    ) -> None:
        """Configure the Bloom filter mode and parameters."""
        config = BloomFilterConfig(
            expected_items=expected_items,
            false_positive_rate=false_positive_rate
        )

        if mode == "standard":
            self._bloom = BloomFilter(config)
            self._counting_bloom = None
            self._scalable_bloom = None
        elif mode == "counting":
            self._counting_bloom = CountingBloomFilter(config)
            self._bloom = None
            self._scalable_bloom = None
        elif mode == "scalable":
            self._scalable_bloom = ScalableBloomFilter(config)
            self._bloom = None
            self._counting_bloom = None

        self._mode = mode

    def add(self, item: str) -> None:
        """Add an item to the filter."""
        if self._mode == "standard":
            self._bloom.add_string(item)
        elif self._mode == "counting":
            self._counting_bloom.add_string(item)
        elif self._mode == "scalable":
            self._scalable_bloom.add_string(item)

    def might_contain(self, item: str) -> bool:
        """Check if item might be in the filter."""
        if self._mode == "standard":
            return self._bloom.might_contain_string(item)
        elif self._mode == "counting":
            return self._counting_bloom.might_contain_string(item)
        elif self._mode == "scalable":
            return self._scalable_bloom.might_contain_string(item)
        return False

    def remove(self, item: str) -> bool:
        """Remove an item (counting mode only)."""
        if self._mode == "counting":
            return self._counting_bloom.remove_string(item)
        return False

    def get_stats(self) -> Dict[str, Any]:
        """Return filter statistics."""
        if self._mode == "standard":
            return {
                "mode": self._mode,
                "count": self._bloom.count,
                "bit_size": self._bloom.config.bit_size,
                "hash_count": self._bloom._hash_count,
                "fill_ratio": round(self._bloom.estimated_fill_ratio(), 4)
            }
        elif self._mode == "counting":
            return {
                "mode": self._mode,
                "count": self._counting_bloom.count,
                "bit_size": self._counting_bloom.config.bit_size
            }
        elif self._mode == "scalable":
            return {
                "mode": self._mode,
                "count": self._scalable_bloom.count,
                "num_filters": len(self._scalable_bloom.get_filters())
            }
        return {}

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the Bloom filter action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform (add, check, remove, stats, configure)
                - item: Item to add or check
                - Other operation-specific fields

        Returns:
            Dictionary with operation results.
        """
        operation = context.get("operation", "check")

        if operation == "configure":
            self.configure(
                mode=context.get("mode", "standard"),
                expected_items=context.get("expected_items", 10000),
                false_positive_rate=context.get("false_positive_rate", 0.01)
            )
            return {"success": True, "mode": self._mode}

        elif operation == "add":
            item = context.get("item", "")
            self.add(item)
            return {"success": True, "count": self.get_stats()["count"]}

        elif operation == "check":
            item = context.get("item", "")
            result = self.might_contain(item)
            return {"success": True, "might_contain": result}

        elif operation == "remove":
            item = context.get("item", "")
            result = self.remove(item)
            return {"success": True, "removed": result}

        elif operation == "stats":
            return {"success": True, "stats": self.get_stats()}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

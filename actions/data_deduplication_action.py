"""Data Deduplication Engine.

This module provides data deduplication capabilities:
- Exact and fuzzy deduplication
- Configurable key extraction
- Bloom filter for memory-efficient dedup
- Batch processing support

Example:
    >>> from actions.data_deduplication_action import DataDeduper
    >>> deduper = DataDeduper(key_func=lambda x: x["id"])
    >>> unique = deduper.deduplicate(items)
"""

from __future__ import annotations

import hashlib
import logging
import threading
from typing import Any, Callable, Optional, Hashable
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataDeduper:
    """Data deduplication engine with multiple strategies."""

    def __init__(
        self,
        key_func: Optional[Callable[[Any], Hashable]] = None,
        strategy: str = "exact",
        fuzzy_threshold: float = 0.85,
    ) -> None:
        """Initialize the deduplicator.

        Args:
            key_func: Function to extract dedup key from items.
            strategy: "exact", "fuzzy", or "bloom".
            fuzzy_threshold: Similarity threshold for fuzzy dedup (0-1).
        """
        self._key_func = key_func or (lambda x: x)
        self._strategy = strategy
        self._fuzzy_threshold = fuzzy_threshold
        self._seen_keys: set[Hashable] = set()
        self._bloom_filter: Optional[BloomFilter] = None
        self._lock = threading.RLock()
        self._stats: dict[str, int] = {"checked": 0, "duplicates": 0, "unique": 0}

    def set_bloom_filter(self, size: int = 100000, num_hashes: int = 7) -> None:
        """Enable bloom filter for memory-efficient deduplication.

        Args:
            size: Expected number of items.
            num_hashes: Number of hash functions.
        """
        self._bloom_filter = BloomFilter(size=size, num_hashes=num_hashes)
        logger.info("Enabled bloom filter (size=%d, hashes=%d)", size, num_hashes)

    def deduplicate(
        self,
        items: list[Any],
        key_func: Optional[Callable[[Any], Hashable]] = None,
    ) -> list[Any]:
        """Remove duplicates from a list of items.

        Args:
            items: List of items to deduplicate.
            key_func: Override key extraction function.

        Returns:
            List of unique items in original order.
        """
        if not items:
            return []

        extractor = key_func or self._key_func

        if self._strategy == "bloom" and self._bloom_filter:
            return self._deduplicate_bloom(items, extractor)
        elif self._strategy == "fuzzy":
            return self._deduplicate_fuzzy(items, extractor)
        else:
            return self._deduplicate_exact(items, extractor)

    def _deduplicate_exact(
        self,
        items: list[Any],
        key_func: Callable[[Any], Hashable],
    ) -> list[Any]:
        """Exact deduplication using a set."""
        unique = []
        with self._lock:
            for item in items:
                key = key_func(item)
                self._stats["checked"] += 1
                if key not in self._seen_keys:
                    self._seen_keys.add(key)
                    unique.append(item)
                    self._stats["unique"] += 1
                else:
                    self._stats["duplicates"] += 1
        return unique

    def _deduplicate_bloom(
        self,
        items: list[Any],
        key_func: Callable[[Any], Hashable],
    ) -> list[Any]:
        """Bloom filter deduplication."""
        unique = []
        bloom = self._bloom_filter
        if bloom is None:
            return items

        with self._lock:
            for item in items:
                key = key_func(item)
                self._stats["checked"] += 1
                key_hash = self._hash_key(key)
                if not bloom.check(key_hash):
                    bloom.add(key_hash)
                    unique.append(item)
                    self._stats["unique"] += 1
                else:
                    self._stats["duplicates"] += 1
        return unique

    def _deduplicate_fuzzy(
        self,
        items: list[Any],
        key_func: Callable[[Any], Hashable],
    ) -> list[Any]:
        """Fuzzy deduplication using similarity."""
        unique = []
        seen_signatures: list[tuple[Hashable, Any]] = []

        with self._lock:
            for item in items:
                key = key_func(item)
                self._stats["checked"] += 1

                is_dup = False
                for _, seen_item in seen_signatures:
                    if self._is_similar(key, key_func(seen_item)):
                        is_dup = True
                        break

                if not is_dup:
                    seen_signatures.append((key, item))
                    unique.append(item)
                    self._stats["unique"] += 1
                else:
                    self._stats["duplicates"] += 1

        return unique

    def _is_similar(self, a: Hashable, b: Hashable) -> bool:
        """Check if two keys are similar."""
        if not isinstance(a, str) or not isinstance(b, str):
            return a == b

        if a == b:
            return True

        len_a, len_b = len(a), len(b)
        if min(len_a, len_b) == 0:
            return False

        max_len = max(len_a, len_b)
        distance = self._levenshtein(a, b)
        similarity = 1.0 - (distance / max_len)
        return similarity >= self._fuzzy_threshold

    def _levenshtein(self, a: str, b: str) -> int:
        """Compute Levenshtein edit distance."""
        if len(a) < len(b):
            return self._levenshtein(b, a)
        if len(b) == 0:
            return len(a)

        prev = range(len(b) + 1)
        for i, ca in enumerate(a):
            curr = [i + 1]
            for j, cb in enumerate(b):
                insertions = prev[j + 1] + 1
                deletions = curr[j] + 1
                substitutions = prev[j] + (ca != cb)
                curr.append(min(insertions, deletions, substitutions))
            prev = curr

        return prev[-1]

    def _hash_key(self, key: Hashable) -> int:
        """Hash a key to an integer."""
        if isinstance(key, str):
            data = key.encode()
        else:
            data = str(key).encode()
        return int(hashlib.sha256(data).hexdigest(), 16)

    def reset(self) -> None:
        """Reset the seen keys and statistics."""
        with self._lock:
            self._seen_keys.clear()
            if self._bloom_filter:
                self._bloom_filter.clear()
            self._stats = {"checked": 0, "duplicates": 0, "unique": 0}

    def get_stats(self) -> dict[str, int]:
        """Get deduplication statistics."""
        with self._lock:
            return dict(self._stats)


class BloomFilter:
    """Simple bloom filter for memory-efficient membership testing."""

    def __init__(self, size: int = 100000, num_hashes: int = 7) -> None:
        """Initialize bloom filter.

        Args:
            size: Bit array size.
            num_hashes: Number of hash functions.
        """
        self._size = size
        self._num_hashes = num_hashes
        self._bits = [False] * size
        self._count = 0

    def add(self, item_hash: int) -> None:
        """Add an item hash to the filter."""
        for i in self._get_indices(item_hash):
            self._bits[i] = True
        self._count += 1

    def check(self, item_hash: int) -> bool:
        """Check if an item hash might be in the filter."""
        return all(self._bits[i] for i in self._get_indices(item_hash))

    def clear(self) -> None:
        """Clear the filter."""
        self._bits = [False] * self._size
        self._count = 0

    def _get_indices(self, item_hash: int) -> list[int]:
        """Get bit indices for an item hash."""
        result = []
        for i in range(self._num_hashes):
            idx = (item_hash + i * item_hash) % self._size
            result.append(idx)
        return result

    def __len__(self) -> int:
        return self._count

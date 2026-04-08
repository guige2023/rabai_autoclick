"""
Data Cuckoo Filter Action Module.

Cuckoo filter implementation for membership testing
with better space efficiency than bloom filters.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass
import logging
import hashlib
import random

logger = logging.getLogger(__name__)


@dataclass
class CuckooBucket:
    """Single bucket in cuckoo filter."""
    fingerprints: list[Optional[int]]


class DataCuckooFilterAction:
    """
    Cuckoo filter for probabilistic membership testing.

    Supports add, contains, delete operations
    with lower space overhead than bloom filters.

    Example:
        filter = DataCuckooFilterAction(capacity=1000)
        filter.add("item1")
        print(filter.contains("item1"))  # True
        filter.delete("item1")
    """

    BUCKET_SIZE = 4
    FINGERPRINT_BITS = 8

    def __init__(
        self,
        capacity: int = 10000,
        fingerprint_size: int = 8,
        max_kicks: int = 500,
    ) -> None:
        self.capacity = capacity
        self.fingerprint_size = fingerprint_size
        self.max_kicks = max_kicks

        self._num_buckets = max(1, capacity // self.BUCKET_SIZE)
        self._buckets: list[CuckooBucket] = [
            CuckooBucket(fingerprints=[None] * self.BUCKET_SIZE)
            for _ in range(self._num_buckets)
        ]
        self._count = 0
        self._fingerprint_mask = (1 << self.fingerprint_size) - 1

    def add(self, item: Any) -> bool:
        """Add an item to the filter."""
        fp = self._compute_fingerprint(item)
        i1 = self._hash1(item)
        i2 = i1 ^ self._hash2(fp)

        if self._insert(fp, i1):
            return True
        if self._insert(fp, i2):
            return True

        i = i1 if random.random() < 0.5 else i2
        for _ in range(self.max_kicks):
            fp, i = self._kick(fp, i)
            if self._insert(fp, i):
                return True

        logger.warning("Cuckoo filter full, cannot insert")
        return False

    def contains(self, item: Any) -> bool:
        """Check if item might be in the filter."""
        fp = self._compute_fingerprint(item)
        i1 = self._hash1(item)
        i2 = i1 ^ self._hash2(fp)

        return (
            self._bucket_contains(i1, fp) or
            self._bucket_contains(i2, fp)
        )

    def delete(self, item: Any) -> bool:
        """Delete an item from the filter."""
        fp = self._compute_fingerprint(item)
        i1 = self._hash1(item)
        i2 = i1 ^ self._hash2(fp)

        if self._bucket_delete(i1, fp):
            self._count -= 1
            return True

        if self._bucket_delete(i2, fp):
            self._count -= 1
            return True

        return False

    def clear(self) -> None:
        """Clear all entries."""
        for bucket in self._buckets:
            bucket.fingerprints = [None] * self.BUCKET_SIZE
        self._count = 0

    def _compute_fingerprint(self, item: Any) -> int:
        """Compute fingerprint for an item."""
        if isinstance(item, str):
            data = item.encode()
        elif isinstance(item, bytes):
            data = item
        else:
            data = str(item).encode()

        hash_bytes = hashlib.sha256(data).digest()[:4]
        fp = int.from_bytes(hash_bytes, "big")
        return fp & self._fingerprint_mask

    def _hash1(self, item: Any) -> int:
        """Primary hash function."""
        if isinstance(item, str):
            data = item.encode()
        elif isinstance(item, bytes):
            data = item
        else:
            data = str(item).encode()

        hash_val = int(hashlib.sha256(data).hexdigest(), 16)
        return hash_val % self._num_buckets

    def _hash2(self, fingerprint: int) -> int:
        """Secondary hash based on fingerprint."""
        return (fingerprint * 0x9e3779b9) % self._num_buckets

    def _insert(self, fp: int, i: int) -> bool:
        """Insert fingerprint into bucket."""
        bucket = self._buckets[i % self._num_buckets]

        for idx in range(self.BUCKET_SIZE):
            if bucket.fingerprints[idx] is None:
                bucket.fingerprints[idx] = fp
                self._count += 1
                return True

        return False

    def _kick(self, fp: int, i: int) -> tuple[int, int]:
        """Kick out a fingerprint and reinsert elsewhere."""
        bucket = self._buckets[i % self._num_buckets]
        idx = random.randint(0, self.BUCKET_SIZE - 1)

        old_fp = bucket.fingerprints[idx]
        bucket.fingerprints[idx] = fp

        new_i = i ^ self._hash2(old_fp)
        return old_fp, new_i

    def _bucket_contains(self, i: int, fp: int) -> bool:
        """Check if bucket contains fingerprint."""
        bucket = self._buckets[i % self._num_buckets]
        return fp in bucket.fingerprints

    def _bucket_delete(self, i: int, fp: int) -> bool:
        """Delete fingerprint from bucket."""
        bucket = self._buckets[i % self._num_buckets]

        for idx in range(self.BUCKET_SIZE):
            if bucket.fingerprints[idx] == fp:
                bucket.fingerprints[idx] = None
                return True

        return False

    @property
    def count(self) -> int:
        """Number of items in filter."""
        return self._count

    def estimated_fill_ratio(self) -> float:
        """Estimate how full the filter is."""
        total_slots = self._num_buckets * self.BUCKET_SIZE
        filled = sum(
            sum(1 for fp in b.fingerprints if fp is not None)
            for b in self._buckets
        )
        return filled / total_slots if total_slots > 0 else 0.0

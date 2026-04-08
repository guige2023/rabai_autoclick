"""
Data Hash Index Action Module.

Hash-based indexing for fast data lookups,
supports multiple hash functions and collision handling.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Iterator
from dataclasses import dataclass
import logging
import hashlib
import bisect

logger = logging.getLogger(__name__)


@dataclass
class HashIndexEntry:
    """Entry in the hash index."""
    key: str
    value: Any
    hash_value: int


class DataHashIndexAction:
    """
    Hash-based index for O(1) average-case lookups.

    Supports multiple hash functions, collision handling
    via chaining, and range queries via ordered buckets.

    Example:
        index = DataHashIndexAction()
        index.insert("user:123", {"name": "Alice"})
        result = index.lookup("user:123")
    """

    def __init__(
        self,
        num_buckets: int = 256,
        hash_func: Optional[Callable[[str], int]] = None,
    ) -> None:
        self.num_buckets = num_buckets
        self.hash_func = hash_func or self._default_hash
        self._buckets: list[list[HashIndexEntry]] = [[] for _ in range(num_buckets)]
        self._size: int = 0

    def insert(self, key: str, value: Any) -> None:
        """Insert a key-value pair into the index."""
        hash_val = self.hash_func(key)
        bucket_idx = hash_val % self.num_buckets
        bucket = self._buckets[bucket_idx]

        for entry in bucket:
            if entry.key == key:
                entry.value = value
                return

        bucket.append(HashIndexEntry(key=key, value=value, hash_value=hash_val))
        self._size += 1

    def lookup(self, key: str) -> Optional[Any]:
        """Lookup value by key."""
        bucket_idx = self.hash_func(key) % self.num_buckets
        bucket = self._buckets[bucket_idx]

        for entry in bucket:
            if entry.key == key:
                return entry.value

        return None

    def contains(self, key: str) -> bool:
        """Check if key exists in index."""
        return self.lookup(key) is not None

    def delete(self, key: str) -> bool:
        """Delete a key from the index."""
        bucket_idx = self.hash_func(key) % self.num_buckets
        bucket = self._buckets[bucket_idx]

        for i, entry in enumerate(bucket):
            if entry.key == key:
                del bucket[i]
                self._size -= 1
                return True

        return False

    def update(self, key: str, value: Any) -> bool:
        """Update existing key's value."""
        if self.contains(key):
            self.insert(key, value)
            return True
        return False

    def get_bucket_size(self, bucket_idx: int) -> int:
        """Get size of a specific bucket."""
        if 0 <= bucket_idx < self.num_buckets:
            return len(self._buckets[bucket_idx])
        return 0

    def find_keys(
        self,
        predicate: Callable[[str, Any], bool],
    ) -> list[str]:
        """Find all keys matching a predicate."""
        results = []
        for bucket in self._buckets:
            for entry in bucket:
                if predicate(entry.key, entry.value):
                    results.append(entry.key)
        return results

    def keys(self) -> Iterator[str]:
        """Iterate over all keys."""
        for bucket in self._buckets:
            for entry in bucket:
                yield entry.key

    def values(self) -> Iterator[Any]:
        """Iterate over all values."""
        for bucket in self._buckets:
            for entry in bucket:
                yield entry.value

    def items(self) -> Iterator[tuple[str, Any]]:
        """Iterate over all key-value pairs."""
        for bucket in self._buckets:
            for entry in bucket:
                yield (entry.key, entry.value)

    def clear(self) -> None:
        """Clear all entries from the index."""
        for bucket in self._buckets:
            bucket.clear()
        self._size = 0

    @property
    def size(self) -> int:
        """Number of entries in the index."""
        return self._size

    @property
    def load_factor(self) -> float:
        """Average entries per bucket."""
        return self._size / self.num_buckets if self.num_buckets > 0 else 0.0

    @staticmethod
    def _default_hash(key: str) -> int:
        """Default hash function using MD5."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def __len__(self) -> int:
        return self.size

    def __contains__(self, key: str) -> bool:
        return self.contains(key)

"""
Data Storage Action Module.

Provides local data storage with support for caching,
persistence, and data expiration for automation workflows.
"""

import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class StoredItem:
    """A stored data item with metadata."""
    key: str
    value: Any
    created_at: float
    expires_at: Optional[float] = None
    access_count: int = 0
    last_accessed: Optional[float] = None


class DataStorage:
    """In-memory data storage with optional persistence."""

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: Optional[float] = None,
    ):
        """
        Initialize data storage.

        Args:
            max_size: Maximum number of items.
            default_ttl: Default time-to-live in seconds.
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._store: OrderedDict[str, StoredItem] = OrderedDict()

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """
        Store a value.

        Args:
            key: Storage key.
            value: Value to store.
            ttl: Optional time-to-live in seconds.
        """
        if len(self._store) >= self.max_size and key not in self._store:
            self._evict_oldest()

        now = time.time()
        expires_at = None
        effective_ttl = ttl if ttl is not None else self.default_ttl
        if effective_ttl:
            expires_at = now + effective_ttl

        item = StoredItem(
            key=key,
            value=value,
            created_at=now,
            expires_at=expires_at,
        )
        self._store[key] = item

    def get(
        self,
        key: str,
        default: Any = None,
        update_access: bool = True,
    ) -> Any:
        """
        Retrieve a value.

        Args:
            key: Storage key.
            default: Default if not found or expired.
            update_access: Whether to update access metadata.

        Returns:
            Stored value or default.
        """
        item = self._store.get(key)

        if item is None:
            return default

        if item.expires_at and time.time() > item.expires_at:
            del self._store[key]
            return default

        if update_access:
            item.access_count += 1
            item.last_accessed = time.time()

        return item.value

    def delete(self, key: str) -> bool:
        """
        Delete a value.

        Args:
            key: Storage key.

        Returns:
            True if deleted, False if not found.
        """
        if key in self._store:
            del self._store[key]
            return True
        return False

    def has(self, key: str) -> bool:
        """
        Check if key exists and is not expired.

        Args:
            key: Storage key.

        Returns:
            True if exists and not expired.
        """
        item = self._store.get(key)
        if item is None:
            return False
        if item.expires_at and time.time() > item.expires_at:
            del self._store[key]
            return False
        return True

    def clear(self) -> None:
        """Clear all stored data."""
        self._store.clear()

    def keys(self) -> list[str]:
        """Get all keys (excluding expired)."""
        self._cleanup_expired()
        return list(self._store.keys())

    def size(self) -> int:
        """Get current storage size."""
        self._cleanup_expired()
        return len(self._store)

    def _evict_oldest(self) -> None:
        """Evict the oldest accessed item."""
        if self._store:
            self._store.popitem(last=False)

    def _cleanup_expired(self) -> None:
        """Remove expired items."""
        now = time.time()
        expired = [
            key for key, item in self._store.items()
            if item.expires_at and now > item.expires_at
        ]
        for key in expired:
            del self._store[key]

    def export_json(self) -> str:
        """Export storage as JSON string."""
        self._cleanup_expired()
        data = {
            key: item.value
            for key, item in self._store.items()
        }
        return json.dumps(data, indent=2)

    def import_json(self, json_str: str) -> int:
        """
        Import data from JSON string.

        Args:
            json_str: JSON string to import.

        Returns:
            Number of items imported.
        """
        try:
            data = json.loads(json_str)
            count = 0
            for key, value in data.items():
                self.set(key, value)
                count += 1
            return count
        except json.JSONDecodeError:
            return 0

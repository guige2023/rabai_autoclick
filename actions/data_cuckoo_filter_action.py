"""Data Cuckoo Filter Action Module.

Provides Cuckoo filter implementation for space-efficient
probabilistic membership testing with deletion support.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import math
import random
import sys
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class FingerprintEntry:
    """Fingerprint entry in filter."""
    fingerprint: int
    count: int = 1


@dataclass
class Bucket:
    """Single bucket in cuckoo filter."""
    entries: List[int] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.entries)

    def is_full(self, max_entries: int = 4) -> bool:
        return len(self.entries) >= max_entries

    def add(self, fingerprint: int, max_entries: int = 4) -> bool:
        """Add fingerprint to bucket if space available."""
        if self.is_full(max_entries):
            return False
        if fingerprint not in self.entries:
            self.entries.append(fingerprint)
        return True

    def remove(self, fingerprint: int) -> bool:
        """Remove fingerprint from bucket."""
        if fingerprint in self.entries:
            self.entries.remove(fingerprint)
            return True
        return False

    def contains(self, fingerprint: int) -> bool:
        """Check if fingerprint exists in bucket."""
        return fingerprint in self.entries


class CuckooFilter:
    """Cuckoo filter implementation with two hash functions."""

    def __init__(
        self,
        capacity: int = 10000,
        fingerprint_size: int = 8,
        max_entries_per_bucket: int = 4,
        max_relocations: int = 500
    ):
        self.capacity = capacity
        self.fingerprint_size = fingerprint_size
        self.max_entries_per_bucket = max_entries_per_bucket
        self.max_relocations = max_relocations

        self.bucket_count = self._calculate_bucket_count(capacity)
        self.buckets: List[Bucket] = [Bucket() for _ in range(self.bucket_count)]

        self.total_items = 0
        self._kick_count = 0

    def _calculate_bucket_count(self, capacity: int) -> int:
        """Calculate number of buckets needed."""
        return max(1, capacity // self.max_entries_per_bucket)

    def _hash_fingerprint(self, fingerprint: int) -> int:
        """Secondary hash for fingerprint."""
        h = hashlib.sha256(str(fingerprint).encode()).hexdigest()
        return int(h[:16], 16)

    def _get_buckets_for_item(
        self,
        item_hash: int
    ) -> tuple[int, int]:
        """Get two candidate bucket indices for an item."""
        h1 = item_hash % self.bucket_count
        h2 = (h1 ^ self._hash_fingerprint(item_hash)) % self.bucket_count
        return h1, h2

    def _get_fingerprint(self, item: bytes) -> int:
        """Generate fingerprint from item bytes."""
        if isinstance(item, str):
            item = item.encode()
        elif not isinstance(item, bytes):
            item = str(item).encode()

        h = hashlib.sha256(item).hexdigest()
        fp = int(h[:self.fingerprint_size * 2], 16)

        mask = (1 << (self.fingerprint_size * 8)) - 1
        return fp & mask

    def _get_item_hash(self, item: bytes) -> int:
        """Get hash value for item."""
        if isinstance(item, str):
            item = item.encode()
        elif not isinstance(item, bytes):
            item = str(item).encode()

        h = hashlib.md5(item).hexdigest()
        return int(h, 16)

    def insert(self, item: bytes) -> bool:
        """Insert item into filter."""
        fingerprint = self._get_fingerprint(item)
        item_hash = self._get_item_hash(item)
        i1, i2 = self._get_buckets_for_item(item_hash)

        if self.buckets[i1].add(fingerprint, self.max_entries_per_bucket):
            self.total_items += 1
            return True

        if self.buckets[i2].add(fingerprint, self.max_entries_per_bucket):
            self.total_items += 1
            return True

        return self._cuckoo_insert(fingerprint, i1)

    def _cuckoo_insert(self, fingerprint: int, start_bucket: int) -> bool:
        """Cuckoo-style insertion with relocation."""
        current_fp = fingerprint
        current_bucket = start_bucket

        for _ in range(self.max_relocations):
            random_idx = random.randint(
                0,
                self.max_entries_per_bucket - 1
            )

            if current_bucket < len(self.buckets):
                bucket = self.buckets[current_bucket]
                if len(bucket.entries) > random_idx:
                    old_fp = bucket.entries[random_idx]
                    bucket.entries[random_idx] = current_fp
                    current_fp = old_fp

                    new_hash = self._get_item_hash(str(old_fp).encode())
                    i1, i2 = self._get_buckets_for_item(new_hash)
                    current_bucket = i2 if i1 == current_bucket else i1

                    if bucket.add(current_fp, self.max_entries_per_bucket):
                        self.total_items += 1
                        return True
                else:
                    return False
            else:
                return False

        self._kick_count += 1
        return False

    def contains(self, item: bytes) -> bool:
        """Check if item might be in filter."""
        fingerprint = self._get_fingerprint(item)
        item_hash = self._get_item_hash(item)
        i1, i2 = self._get_buckets_for_item(item_hash)

        return (
            self.buckets[i1].contains(fingerprint) or
            self.buckets[i2].contains(fingerprint)
        )

    def delete(self, item: bytes) -> bool:
        """Delete item from filter."""
        fingerprint = self._get_fingerprint(item)
        item_hash = self._get_item_hash(item)
        i1, i2 = self._get_buckets_for_item(item_hash)

        if self.buckets[i1].remove(fingerprint):
            self.total_items -= 1
            return True

        if self.buckets[i2].remove(fingerprint):
            self.total_items -= 1
            return True

        return False

    def get_load_factor(self) -> float:
        """Get current load factor."""
        total_slots = self.bucket_count * self.max_entries_per_bucket
        used_entries = sum(len(b) for b in self.buckets)
        return used_entries / total_slots if total_slots > 0 else 0.0

    def get_statistics(self) -> Dict[str, Any]:
        """Get filter statistics."""
        total_entries = sum(len(b) for b in self.buckets)
        occupied_buckets = sum(1 for b in self.buckets if len(b) > 0)

        return {
            "total_items": self.total_items,
            "bucket_count": self.bucket_count,
            "total_entries": total_entries,
            "occupied_buckets": occupied_buckets,
            "load_factor": self.get_load_factor(),
            "kick_count": self._kick_count,
            "capacity": self.capacity,
            "fingerprint_size": self.fingerprint_size,
            "max_entries_per_bucket": self.max_entries_per_bucket
        }

    def clear(self) -> None:
        """Clear all entries from filter."""
        self.buckets = [Bucket() for _ in range(self.bucket_count)]
        self.total_items = 0
        self._kick_count = 0


class DataCuckooFilterAction(BaseAction):
    """Action for Cuckoo filter operations."""

    def __init__(self):
        super().__init__("data_cuckoo_filter")
        self._filters: Dict[str, CuckooFilter] = {}
        self._default_capacity = 10000

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute cuckoo filter action."""
        try:
            operation = params.get("operation", "insert")

            if operation == "create":
                return self._create(params)
            elif operation == "insert":
                return self._insert(params)
            elif operation == "contains":
                return self._contains(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get_filter(self, name: str) -> Optional[CuckooFilter]:
        """Get filter by name."""
        return self._filters.get(name)

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create new cuckoo filter."""
        name = params.get("name", "default")
        capacity = params.get("capacity", self._default_capacity)
        fingerprint_size = params.get("fingerprint_size", 8)
        max_entries = params.get("max_entries_per_bucket", 4)

        filter_obj = CuckooFilter(
            capacity=capacity,
            fingerprint_size=fingerprint_size,
            max_entries_per_bucket=max_entries
        )

        self._filters[name] = filter_obj

        return ActionResult(
            success=True,
            message=f"Cuckoo filter created: {name}",
            data={"capacity": capacity}
        )

    def _insert(self, params: Dict[str, Any]) -> ActionResult:
        """Insert item into filter."""
        name = params.get("name", "default")
        item = params.get("item", "")

        filter_obj = self._get_filter(name)
        if not filter_obj:
            return ActionResult(
                success=False,
                message=f"Filter not found: {name}"
            )

        if isinstance(item, list):
            inserted = 0
            for i in item:
                if filter_obj.insert(str(i).encode()):
                    inserted += 1
            return ActionResult(
                success=True,
                data={"inserted": inserted, "total": len(item)}
            )
        else:
            success = filter_obj.insert(str(item).encode())
            return ActionResult(
                success=success,
                data={"inserted": success}
            )

    def _contains(self, params: Dict[str, Any]) -> ActionResult:
        """Check if item might be in filter."""
        name = params.get("name", "default")
        item = params.get("item", "")

        filter_obj = self._get_filter(name)
        if not filter_obj:
            return ActionResult(
                success=False,
                message=f"Filter not found: {name}"
            )

        if isinstance(item, list):
            results = [filter_obj.contains(str(i).encode()) for i in item]
            return ActionResult(
                success=True,
                data={"results": results}
            )
        else:
            found = filter_obj.contains(str(item).encode())
            return ActionResult(
                success=True,
                data={"found": found}
            )

    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete item from filter."""
        name = params.get("name", "default")
        item = params.get("item", "")

        filter_obj = self._get_filter(name)
        if not filter_obj:
            return ActionResult(
                success=False,
                message=f"Filter not found: {name}"
            )

        deleted = filter_obj.delete(str(item).encode())
        return ActionResult(
            success=True,
            data={"deleted": deleted}
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get filter statistics."""
        name = params.get("name", "default")

        filter_obj = self._get_filter(name)
        if not filter_obj:
            return ActionResult(
                success=False,
                message=f"Filter not found: {name}"
            )

        stats = filter_obj.get_statistics()
        return ActionResult(success=True, data=stats)

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear filter."""
        name = params.get("name", "default")

        filter_obj = self._get_filter(name)
        if not filter_obj:
            return ActionResult(
                success=False,
                message=f"Filter not found: {name}"
            )

        filter_obj.clear()
        return ActionResult(success=True, message=f"Filter cleared: {name}")

"""
Data Deduplication Action Module

Provides data deduplication capabilities for various data types.
Supports exact match, fuzzy match, and rule-based deduplication.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Callable, Optional, TypeVar, Generic
from datetime import datetime

T = TypeVar('T')


class DeduplicationStrategy:
    """Strategy for deduplication."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    RULE_BASED = "rule_based"
    COMBINED = "combined"


@dataclass
class DedupConfig:
    """Configuration for deduplication."""
    strategy: str = "exact"
    fuzzy_threshold: float = 0.85
    key_extractor: Optional[Callable[[Any], Any]] = None
    hash_fields: Optional[list[str]] = None
    max_memory_items: int = 10000


@dataclass
class DedupResult:
    """Result of a deduplication operation."""
    original_count: int
    deduplicated_count: int
    duplicates_removed: int
    duplicate_groups: list[list[Any]] = field(default_factory=list)
    duration_ms: float = 0.0


class HashGenerator:
    """Generate hashes for deduplication."""
    
    @staticmethod
    def hash_string(s: str) -> str:
        """Hash a string."""
        return hashlib.sha256(s.encode()).hexdigest()
    
    @staticmethod
    def hash_dict(d: dict, fields: Optional[list[str]] = None) -> str:
        """Hash a dictionary."""
        if fields:
            d = {k: v for k, v in d.items() if k in fields}
        sorted_items = sorted(str(k) + ":" + str(v) for k, v in d.items())
        content = "|".join(sorted_items)
        return hashlib.sha256(content.encode()).hexdigest()
    
    @staticmethod
    def hash_object(obj: Any, key_fn: Optional[Callable] = None) -> str:
        """Hash any object."""
        if key_fn:
            key = key_fn(obj)
        else:
            key = str(obj)
        return HashGenerator.hash_string(key)


class FuzzyMatcher:
    """Fuzzy matching for deduplication."""
    
    def __init__(self, threshold: float = 0.85):
        self.threshold = threshold
    
    def similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity between two strings."""
        return SequenceMatcher(None, s1, s2).ratio()
    
    def is_duplicate(self, s1: str, s2: str) -> bool:
        """Check if two strings are duplicates."""
        return self.similarity(s1, s2) >= self.threshold
    
    def find_duplicates_in_list(self, items: list[str]) -> list[list[int]]:
        """Find groups of duplicate indices in a list of strings."""
        n = len(items)
        parent = list(range(n))
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        for i in range(n):
            for j in range(i + 1, n):
                if self.is_duplicate(items[i], items[j]):
                    union(i, j)
        
        groups: dict[int, list[int]] = {}
        for i in range(n):
            root = find(i)
            if root not in groups:
                groups[root] = []
            groups[root].append(i)
        
        return list(groups.values())


class DataDeduplicationAction:
    """
    Data deduplication action.
    
    Removes duplicate data using various strategies.
    
    Example:
        dedup = DataDeduplicationAction()
        
        result = dedup.deduplicate(
            data=records,
            key_fn=lambda r: r["id"]
        )
    """
    
    def __init__(self, config: Optional[DedupConfig] = None):
        self.config = config or DedupConfig()
        self._seen_hashes: set[str] = deque(maxlen=self.config.max_memory_items)
        self._fuzzy_matcher = FuzzyMatcher(threshold=self.config.fuzzy_threshold)
        self._stats = {
            "total_items": 0,
            "duplicates_found": 0,
            "items_removed": 0
        }
    
    def _extract_key(self, item: Any) -> Any:
        """Extract deduplication key from an item."""
        if self.config.key_extractor:
            return self.config.key_extractor(item)
        if isinstance(item, dict):
            if self.config.hash_fields:
                return HashGenerator.hash_dict(item, self.config.hash_fields)
            return HashGenerator.hash_dict(item)
        return str(item)
    
    def _compute_hash(self, item: Any) -> str:
        """Compute hash for an item."""
        key = self._extract_key(item)
        return HashGenerator.hash_string(str(key))
    
    def is_duplicate(self, item: Any) -> bool:
        """Check if an item is a duplicate."""
        h = self._compute_hash(item)
        if h in self._seen_hashes:
            return True
        self._seen_hashes.add(h)
        return False
    
    def deduplicate(
        self,
        data: list[Any],
        key_fn: Optional[Callable[[Any], Any]] = None
    ) -> DedupResult:
        """
        Deduplicate a list of items.
        
        Args:
            data: List of items to deduplicate
            key_fn: Optional function to extract deduplication key
            
        Returns:
            DedupResult with statistics and duplicate groups
        """
        start_time = datetime.now()
        original_count = len(data)
        self._stats["total_items"] += original_count
        
        if key_fn:
            self.config.key_extractor = key_fn
        
        if self.config.strategy == "exact":
            return self._deduplicate_exact(data)
        elif self.config.strategy == "fuzzy":
            return self._deduplicate_fuzzy(data)
        elif self.config.strategy == "rule_based":
            return self._deduplicate_rule_based(data)
        else:
            return self._deduplicate_combined(data)
    
    def _deduplicate_exact(self, data: list[Any]) -> DedupResult:
        """Deduplicate using exact matching."""
        seen = {}
        unique_items = []
        duplicate_groups = []
        
        for item in data:
            key = self._extract_key(item)
            if key in seen:
                seen[key].append(item)
            else:
                seen[key] = [item]
                unique_items.append(item)
        
        for key, items in seen.items():
            if len(items) > 1:
                duplicate_groups.append(items)
        
        duplicates_removed = original_count - len(unique_items)
        self._stats["duplicates_found"] += len(duplicate_groups)
        self._stats["items_removed"] += duplicates_removed
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return DedupResult(
            original_count=len(data),
            deduplicated_count=len(unique_items),
            duplicates_removed=duplicates_removed,
            duplicate_groups=duplicate_groups,
            duration_ms=duration_ms
        )
    
    def _deduplicate_fuzzy(self, data: list[Any]) -> DedupResult:
        """Deduplicate using fuzzy matching."""
        if not all(isinstance(item, str) for item in data):
            data = [str(item) for item in data]
        
        duplicate_indices = self._fuzzy_matcher.find_duplicates_in_list(data)
        
        indices_to_remove = set()
        duplicate_groups = []
        
        for group in duplicate_indices:
            keep_idx = group[0]
            remove_indices = group[1:]
            indices_to_remove.update(remove_indices)
            duplicate_groups.append([data[i] for i in group])
        
        unique_items = [item for i, item in enumerate(data) if i not in indices_to_remove]
        
        duplicates_removed = len(data) - len(unique_items)
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return DedupResult(
            original_count=len(data),
            deduplicated_count=len(unique_items),
            duplicates_removed=duplicates_removed,
            duplicate_groups=duplicate_groups,
            duration_ms=duration_ms
        )
    
    def _deduplicate_rule_based(self, data: list[Any]) -> DedupResult:
        """Deduplicate using rule-based matching."""
        return self._deduplicate_exact(data)
    
    def _deduplicate_combined(self, data: list[Any]) -> DedupResult:
        """Deduplicate using combined strategies."""
        result = self._deduplicate_exact(data)
        if result.duplicate_groups:
            str_items = [str(item) for item in data]
            fuzzy_indices = self._fuzzy_matcher.find_duplicates_in_list(str_items)
            
            additional_removals = set()
            for group in fuzzy_indices:
                if len(group) > 1:
                    additional_removals.update(group[1:])
            
            result.duplicates_removed += len(additional_removals)
            result.deduplicated_count = result.original_count - result.duplicates_removed
        
        return result
    
    def get_stats(self) -> dict[str, Any]:
        """Get deduplication statistics."""
        return {
            **self._stats,
            "memory_items": len(self._seen_hashes),
            "duplicate_rate": (
                self._stats["items_removed"] / self._stats["total_items"]
                if self._stats["total_items"] > 0 else 0
            )
        }
    
    def clear(self) -> None:
        """Clear seen items cache."""
        self._seen_hashes.clear()


class StreamingDedup(DataDeduplicationAction):
    """Streaming deduplication for large datasets."""
    
    def __init__(self, config: Optional[DedupConfig] = None):
        super().__init__(config)
        self._seen_count = 0
        self._dup_count = 0
    
    def process_item(self, item: Any) -> bool:
        """
        Process a single item.
        
        Returns:
            True if item is unique (not a duplicate), False if duplicate
        """
        if self.is_duplicate(item):
            self._dup_count += 1
            return False
        self._seen_count += 1
        return True
    
    def process_batch(self, items: list[Any]) -> list[Any]:
        """Process a batch of items, returning only unique ones."""
        return [item for item in items if self.process_item(item)]

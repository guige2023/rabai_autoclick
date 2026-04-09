"""
Data Deduplicator Module.

Provides data deduplication with multiple strategies:
exact match, fuzzy match, similarity-based, and probabilistic deduplication.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DeduplicationStrategy(Enum):
    """Deduplication strategy types."""
    EXACT = "exact"
    FINGERPRINT = "fingerprint"
    SIMILARITY = "similarity"
    FUZZY = "fuzzy"
    PROBABILISTIC = "probabilistic"
    MERGE = "merge"


@dataclass
class DeduplicationConfig:
    """Configuration for deduplication."""
    strategy: DeduplicationStrategy = DeduplicationStrategy.EXACT
    hash_fields: List[str] = field(default_factory=list)
    similarity_threshold: float = 0.9
    fingerprint_bits: int = 128
    bloom_filter_size: int = 1000000
    bloom_filter_hashes: int = 7
    merge_strategy: str = "newest"  # newest, oldest, combine


@dataclass
class DuplicateGroup:
    """Container for a group of duplicates."""
    group_id: str
    items: List[Any]
    canonical_item: Any
    confidence: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeduplicationResult:
    """Result of deduplication operation."""
    unique_items: List[Any]
    duplicates: List[DuplicateGroup]
    total_items: int
    duplicate_count: int
    processing_time: float


class Deduplicator:
    """
    Data deduplication with multiple strategies.
    
    Example:
        dedup = Deduplicator(DeduplicationConfig(
            strategy=DeduplicationStrategy.EXACT,
            hash_fields=["email", "phone"]
        ))
        
        result = await dedup.deduplicate(data)
        print(f"Found {result.duplicate_count} duplicate groups")
    """
    
    def __init__(self, config: Optional[DeduplicationConfig] = None) -> None:
        """
        Initialize the deduplicator.
        
        Args:
            config: Deduplication configuration.
        """
        self.config = config or DeduplicationConfig()
        self._seen_hashes: Set[str] = set()
        self._bloom_filter: Optional[BloomFilter] = None
        self._similarity_index: List[Tuple[str, Any]] = []
        
        if self.config.strategy == DeduplicationStrategy.PROBABILISTIC:
            self._bloom_filter = BloomFilter(
                self.config.bloom_filter_size,
                self.config.bloom_filter_hashes
            )
            
    async def deduplicate(
        self,
        items: List[Any],
        key_func: Optional[Callable[[Any], Any]] = None,
    ) -> DeduplicationResult:
        """
        Deduplicate a list of items.
        
        Args:
            items: List of items to deduplicate.
            key_func: Optional function to extract deduplication key.
            
        Returns:
            DeduplicationResult with unique items and duplicates.
        """
        start_time = time.time()
        
        if self.config.strategy == DeduplicationStrategy.EXACT:
            return self._deduplicate_exact(items, key_func)
        elif self.config.strategy == DeduplicationStrategy.FINGERPRINT:
            return self._deduplicate_fingerprint(items, key_func)
        elif self.config.strategy == DeduplicationStrategy.SIMILARITY:
            return self._deduplicate_similarity(items, key_func)
        elif self.config.strategy == DeduplicationStrategy.FUZZY:
            return self._deduplicate_fuzzy(items, key_func)
        elif self.config.strategy == DeduplicationStrategy.PROBABILISTIC:
            return self._deduplicate_probabilistic(items, key_func)
        elif self.config.strategy == DeduplicationStrategy.MERGE:
            return self._deduplicate_merge(items, key_func)
        else:
            return self._deduplicate_exact(items, key_func)
            
    def _deduplicate_exact(
        self,
        items: List[Any],
        key_func: Optional[Callable[[Any], Any]],
    ) -> DeduplicationResult:
        """Exact match deduplication."""
        unique: List[Any] = []
        duplicate_groups: List[DuplicateGroup] = []
        seen_keys: Dict[str, List[Any]] = {}
        
        for item in items:
            key = self._get_key(item, key_func)
            key_str = str(key) if key is not None else str(item)
            key_hash = hashlib.sha256(key_str.encode()).hexdigest()
            
            if key_hash in seen_keys:
                seen_keys[key_hash].append(item)
            else:
                seen_keys[key_hash] = [item]
                unique.append(item)
                
        # Build duplicate groups
        group_id = 0
        for key_hash, group_items in seen_keys.items():
            if len(group_items) > 1:
                duplicate_groups.append(DuplicateGroup(
                    group_id=f"dup_{group_id}",
                    items=group_items,
                    canonical_item=group_items[0],
                    confidence=1.0,
                ))
                group_id += 1
                
        return DeduplicationResult(
            unique_items=unique,
            duplicates=duplicate_groups,
            total_items=len(items),
            duplicate_count=sum(len(g.items) - 1 for g in duplicate_groups),
            processing_time=time.time() - start_time,
        )
        
    def _deduplicate_fingerprint(
        self,
        items: List[Any],
        key_func: Optional[Callable[[Any], Any]],
    ) -> DeduplicationResult:
        """Fingerprint-based deduplication."""
        unique = []
        duplicate_groups = []
        fingerprints: Dict[str, List[Any]] = {}
        
        for item in items:
            key = self._get_key(item, key_func)
            
            if self.config.hash_fields and isinstance(key, dict):
                fp = self._generate_fingerprint(key)
            elif isinstance(item, dict):
                fp = self._generate_fingerprint(item)
            else:
                fp = hashlib.sha256(str(key).encode()).hexdigest()
                
            if fp in fingerprints:
                fingerprints[fp].append(item)
            else:
                fingerprints[fp] = [item]
                unique.append(item)
                
        group_id = 0
        for fp, group_items in fingerprints.items():
            if len(group_items) > 1:
                duplicate_groups.append(DuplicateGroup(
                    group_id=f"fp_{group_id}",
                    items=group_items,
                    canonical_item=group_items[0],
                    confidence=1.0,
                    metadata={"fingerprint": fp[:16]},
                ))
                group_id += 1
                
        return DeduplicationResult(
            unique_items=unique,
            duplicates=duplicate_groups,
            total_items=len(items),
            duplicate_count=sum(len(g.items) - 1 for g in duplicate_groups),
            processing_time=time.time() - start_time,
        )
        
    def _deduplicate_similarity(
        self,
        items: List[Any],
        key_func: Optional[Callable[[Any], Any]],
    ) -> DeduplicationResult:
        """Similarity-based deduplication using MinHash."""
        unique = []
        duplicate_groups = []
        processed: Set[int] = set()
        
        for i, item in enumerate(items):
            if i in processed:
                continue
                
            key = self._get_key(item, key_func)
            item_str = str(key) if key is not None else str(item)
            
            similar_group = [item]
            processed.add(i)
            
            for j in range(i + 1, len(items)):
                if j in processed:
                    continue
                    
                other_key = self._get_key(items[j], key_func)
                other_str = str(other_key) if other_key is not None else str(items[j])
                
                similarity = self._calculate_similarity(item_str, other_str)
                
                if similarity >= self.config.similarity_threshold:
                    similar_group.append(items[j])
                    processed.add(j)
                    
            if len(similar_group) > 1:
                duplicate_groups.append(DuplicateGroup(
                    group_id=f"sim_{len(duplicate_groups)}",
                    items=similar_group,
                    canonical_item=similar_group[0],
                    confidence=self.config.similarity_threshold,
                ))
            else:
                unique.append(item)
                
        return DeduplicationResult(
            unique_items=unique,
            duplicates=duplicate_groups,
            total_items=len(items),
            duplicate_count=sum(len(g.items) - 1 for g in duplicate_groups),
            processing_time=time.time() - start_time,
        )
        
    def _deduplicate_fuzzy(
        self,
        items: List[Any],
        key_func: Optional[Callable[[Any], Any]],
    ) -> DeduplicationResult:
        """Fuzzy deduplication using edit distance."""
        unique = []
        duplicate_groups = []
        processed: Set[int] = set()
        
        for i, item in enumerate(items):
            if i in processed:
                continue
                
            key = self._get_key(item, key_func)
            item_str = str(key) if key is not None else str(item)
            
            similar_group = [item]
            processed.add(i)
            
            for j in range(i + 1, len(items)):
                if j in processed:
                    continue
                    
                other_key = self._get_key(items[j], key_func)
                other_str = str(other_key) if other_key is not None else str(items[j])
                
                distance = self._levenshtein_distance(item_str, other_str)
                max_len = max(len(item_str), len(other_str))
                
                if max_len > 0 and 1 - (distance / max_len) >= self.config.similarity_threshold:
                    similar_group.append(items[j])
                    processed.add(j)
                    
            if len(similar_group) > 1:
                duplicate_groups.append(DuplicateGroup(
                    group_id=f"fuzzy_{len(duplicate_groups)}",
                    items=similar_group,
                    canonical_item=similar_group[0],
                    confidence=self.config.similarity_threshold,
                ))
            else:
                unique.append(item)
                
        return DeduplicationResult(
            unique_items=unique,
            duplicates=duplicate_groups,
            total_items=len(items),
            duplicate_count=sum(len(g.items) - 1 for g in duplicate_groups),
            processing_time=time.time() - start_time,
        )
        
    def _deduplicate_probabilistic(
        self,
        items: List[Any],
        key_func: Optional[Callable[[Any], Any]],
    ) -> DeduplicationResult:
        """Probabilistic deduplication using Bloom filter."""
        unique = []
        duplicate_groups = []
        seen_in_main: Dict[str, List[Any]] = {}
        
        for item in items:
            key = self._get_key(item, key_func)
            key_str = str(key) if key is not None else str(item)
            
            if self._bloom_filter and self._bloom_filter.check(key_str):
                # Might be duplicate
                for unique_item in unique:
                    uk = self._get_key(unique_item, key_func)
                    if str(uk) == key_str:
                        if key_str not in seen_in_main:
                            seen_in_main[key_str] = [unique_item]
                        seen_in_main[key_str].append(item)
                        break
            else:
                unique.append(item)
                self._bloom_filter.add(key_str)
                
        # Build groups
        for key, dup_items in seen_in_main.items():
            if len(dup_items) > 1:
                duplicate_groups.append(DuplicateGroup(
                    group_id=f"prob_{len(duplicate_groups)}",
                    items=dup_items,
                    canonical_item=dup_items[0],
                    confidence=0.95,  # Bloom filter confidence
                ))
                
        return DeduplicationResult(
            unique_items=unique,
            duplicates=duplicate_groups,
            total_items=len(items),
            duplicate_count=sum(len(g.items) - 1 for g in duplicate_groups),
            processing_time=time.time() - start_time,
        )
        
    def _deduplicate_merge(
        self,
        items: List[Any],
        key_func: Optional[Callable[[Any], Any]],
    ) -> DeduplicationResult:
        """Merge duplicates based on strategy."""
        dedup_result = self._deduplicate_exact(items, key_func)
        
        if self.config.merge_strategy == "newest":
            for group in dedup_result.duplicates:
                # Sort by timestamp and take newest
                if isinstance(group.items[0], dict) and "created_at" in group.items[0]:
                    sorted_items = sorted(
                        group.items,
                        key=lambda x: x.get("created_at", ""),
                        reverse=True
                    )
                    group.canonical_item = sorted_items[0]
                    
        return dedup_result
        
    def _get_key(self, item: Any, key_func: Optional[Callable]) -> Any:
        """Extract deduplication key from item."""
        if key_func:
            return key_func(item)
        elif isinstance(item, dict):
            if self.config.hash_fields:
                return {k: item.get(k) for k in self.config.hash_fields}
            return item.get("id") or item.get("email") or item
        else:
            return item
            
    def _generate_fingerprint(self, data: Dict[str, Any]) -> str:
        """Generate fingerprint from data."""
        parts = []
        for field in self.config.hash_fields:
            value = data.get(field)
            if value is not None:
                parts.append(str(value))
                
        combined = "|".join(sorted(parts))
        return hashlib.sha256(combined.encode()).hexdigest()[:self.config.fingerprint_bits // 4]
        
    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """Calculate Jaccard similarity using MinHash."""
        set1 = set(s1.split())
        set2 = set(s2.split())
        
        if not set1 or not set2:
            return 0.0
            
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
        
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein edit distance."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
            
        if len(s2) == 0:
            return len(s1)
            
        previous_row = list(range(len(s2) + 1))
        
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
                
            previous_row = current_row
            
        return previous_row[-1]


class BloomFilter:
    """
    Bloom filter for probabilistic membership testing.
    
    Example:
        bf = BloomFilter(size=1000000, num_hashes=7)
        bf.add("item1")
        
        if bf.check("item1"):  # Might be true (false positive possible)
            print("Probably present")
    """
    
    def __init__(self, size: int, num_hashes: int) -> None:
        """
        Initialize Bloom filter.
        
        Args:
            size: Number of bits in the filter.
            num_hashes: Number of hash functions.
        """
        self.size = size
        self.num_hashes = num_hashes
        self._bits = [False] * size
        self._count = 0
        
    def add(self, item: str) -> None:
        """Add an item to the filter."""
        for seed in range(self.num_hashes):
            index = self._hash(item, seed)
            self._bits[index] = True
        self._count += 1
        
    def check(self, item: str) -> bool:
        """Check if item might be in the filter."""
        for seed in range(self.num_hashes):
            index = self._hash(item, seed)
            if not self._bits[index]:
                return False
        return True
        
    def _hash(self, item: str, seed: int) -> int:
        """Generate hash for item."""
        h = hashlib.sha256(f"{seed}{item}".encode()).hexdigest()
        return int(h, 16) % self.size
        
    def get_stats(self) -> Dict[str, Any]:
        """Get Bloom filter statistics."""
        bits_set = sum(self._bits)
        return {
            "size": self.size,
            "num_hashes": self.num_hashes,
            "items_added": self._count,
            "bits_set": bits_set,
            "false_positive_rate": self._estimate_fpr(),
        }
        
    def _estimate_fpr(self) -> float:
        """Estimate false positive rate."""
        import math
        k = self.num_hashes
        m = self.size
        n = self._count
        
        exponent = -k * n / m
        return (1 - math.exp(exponent)) ** k

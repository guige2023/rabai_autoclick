"""
Data Deduplication Action Module

Record deduplication with multiple strategies (hash, fuzzy, exact).
Configurable comparison keys and merge policies.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class DedupStrategy(Enum):
    """Deduplication strategies."""
    
    EXACT = "exact"
    HASH = "hash"
    FUZZY = "fuzzy"
    COMPOSITE = "composite"


@dataclass
class DedupConfig:
    """Configuration for deduplication."""
    
    strategy: DedupStrategy = DedupStrategy.HASH
    hash_fields: List[str] = field(default_factory=list)
    similarity_threshold: float = 0.85
    comparison_keys: List[str] = field(default_factory=list)
    merge_policy: str = "latest"


@dataclass
class DeduplicationResult:
    """Result of deduplication operation."""
    
    total_records: int
    unique_records: int
    duplicates_found: int
    merged_records: int
    removed_duplicates: int


class HashDeduplicator:
    """Hash-based deduplication."""
    
    def __init__(self, fields: List[str]):
        self.fields = fields
    
    def compute_hash(self, record: Dict) -> str:
        """Compute hash for a record."""
        values = []
        for field in self.fields:
            value = record.get(field, "")
            values.append(f"{field}:{value}")
        
        combined = "|".join(values)
        return hashlib.sha256(combined.encode()).hexdigest()
    
    def find_duplicates(
        self,
        records: List[Dict]
    ) -> Tuple[List[Dict], Dict[str, List[int]]]:
        """Find duplicate records."""
        hash_to_indices = defaultdict(list)
        
        for i, record in enumerate(records):
            record_hash = self.compute_hash(record)
            hash_to_indices[record_hash].append(i)
        
        unique = []
        duplicate_groups = {}
        
        for record_hash, indices in hash_to_indices.items():
            representative_idx = indices[0]
            unique.append(records[representative_idx])
            
            if len(indices) > 1:
                duplicate_groups[representative_idx] = indices
        
        return unique, duplicate_groups


class FuzzyDeduplicator:
    """Fuzzy matching deduplication."""
    
    def __init__(self, fields: List[str], threshold: float = 0.85):
        self.fields = fields
        self.threshold = threshold
    
    def similarity(self, record1: Dict, record2: Dict) -> float:
        """Calculate similarity between two records."""
        scores = []
        
        for field in self.fields:
            val1 = str(record1.get(field, ""))
            val2 = str(record2.get(field, ""))
            
            if not val1 and not val2:
                scores.append(1.0)
            elif not val1 or not val2:
                scores.append(0.0)
            else:
                ratio = SequenceMatcher(None, val1, val2).ratio()
                scores.append(ratio)
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def find_duplicates(
        self,
        records: List[Dict]
    ) -> Tuple[List[Dict], Dict[int, List[int]]]:
        """Find duplicate records using fuzzy matching."""
        n = len(records)
        duplicate_groups = defaultdict(list)
        unique_indices = set()
        
        for i in range(n):
            if i in unique_indices:
                continue
            
            group = [i]
            
            for j in range(i + 1, n):
                if j in unique_indices:
                    continue
                
                similarity = self.similarity(records[i], records[j])
                if similarity >= self.threshold:
                    group.append(j)
                    unique_indices.add(j)
            
            if len(group) > 1:
                duplicate_groups[i] = group
            
            unique_indices.add(i)
        
        unique = [records[i] for i in sorted(unique_indices)]
        
        return unique, dict(duplicate_groups)


class CompositeDeduplicator:
    """Combines multiple deduplication strategies."""
    
    def __init__(self, deduplicators: List[Any]):
        self.deduplicators = deduplicators
    
    def find_duplicates(
        self,
        records: List[Dict]
    ) -> Tuple[List[Dict], Dict[int, List[int]]]:
        """Find duplicates using multiple strategies."""
        all_groups = defaultdict(list)
        
        for deduplicator in self.deduplicators:
            unique, groups = deduplicator.find_duplicates(records)
            
            for rep_idx, dup_indices in groups.items():
                all_groups[rep_idx].extend(dup_indices)
        
        for rep_idx in all_groups:
            all_groups[rep_idx] = list(set(all_groups[rep_idx]))
        
        unique_indices = set()
        for indices in all_groups.values():
            for idx in indices:
                unique_indices.add(idx)
        
        unique = [records[i] for i in sorted(unique_indices)]
        
        return unique, dict(all_groups)


class MergeEngine:
    """Merges duplicate records based on policy."""
    
    def __init__(self, policy: str = "latest"):
        self.policy = policy
    
    def merge(
        self,
        records: List[Dict],
        duplicate_groups: Dict[int, List[int]]
    ) -> Tuple[List[Dict], int]:
        """Merge duplicate records."""
        merged = []
        merged_count = 0
        
        seen_groups = set()
        
        for rep_idx, dup_indices in duplicate_groups.items():
            group_key = tuple(sorted(dup_indices))
            if group_key in seen_groups:
                continue
            seen_groups.add(group_key)
            
            group_records = [records[i] for i in dup_indices]
            
            if len(group_records) == 1:
                merged.append(group_records[0])
            else:
                merged_record = self._merge_group(group_records)
                merged.append(merged_record)
                merged_count += 1
        
        for i, record in enumerate(records):
            if not any(i in dup_indices for dup_indices in duplicate_groups.values()):
                merged.append(record)
        
        return merged, merged_count
    
    def _merge_group(self, records: List[Dict]) -> Dict:
        """Merge a group of duplicate records."""
        if self.policy == "latest":
            return records[-1]
        
        if self.policy == "first":
            return records[0]
        
        if self.policy == "union":
            result = {}
            for record in records:
                for key, value in record.items():
                    if key not in result:
                        result[key] = value
                    elif isinstance(value, list) and isinstance(result[key], list):
                        result[key] = list(set(result[key] + value))
            return result
        
        return records[-1]


class DataDeduplicationAction:
    """
    Main data deduplication action handler.
    
    Provides multiple deduplication strategies with configurable
    comparison keys and merge policies.
    """
    
    def __init__(self, config: Optional[DedupConfig] = None):
        self.config = config or DedupConfig()
        self._deduplicator: Optional[Any] = None
        self._initialize_deduplicator()
        self._stats = {
            "total_processed": 0,
            "duplicates_found": 0
        }
    
    def _initialize_deduplicator(self) -> None:
        """Initialize the appropriate deduplicator."""
        if self.config.strategy == DedupStrategy.EXACT:
            self._deduplicator = HashDeduplicator(self.config.comparison_keys)
        
        elif self.config.strategy == DedupStrategy.HASH:
            self._deduplicator = HashDeduplicator(
                self.config.hash_fields or self.config.comparison_keys
            )
        
        elif self.config.strategy == DedupStrategy.FUZZY:
            self._deduplicator = FuzzyDeduplicator(
                self.config.comparison_keys,
                self.config.similarity_threshold
            )
        
        elif self.config.strategy == DedupStrategy.COMPOSITE:
            deduplicators = [
                HashDeduplicator(self.config.hash_fields or self.config.comparison_keys),
                FuzzyDeduplicator(
                    self.config.comparison_keys,
                    self.config.similarity_threshold
                )
            ]
            self._deduplicator = CompositeDeduplicator(deduplicators)
    
    def deduplicate(
        self,
        records: List[Dict],
        return_duplicate_groups: bool = False
    ) -> DeduplicationResult:
        """Deduplicate a list of records."""
        self._stats["total_processed"] += len(records)
        
        unique, duplicate_groups = self._deduplicator.find_duplicates(records)
        
        if return_duplicate_groups:
            return unique, duplicate_groups
        
        total_duplicates = sum(
            len(indices) - 1
            for indices in duplicate_groups.values()
        )
        
        self._stats["duplicates_found"] += total_duplicates
        
        return DeduplicationResult(
            total_records=len(records),
            unique_records=len(unique),
            duplicates_found=total_duplicates,
            merged_records=0,
            removed_duplicates=total_duplicates
        )
    
    def deduplicate_and_merge(
        self,
        records: List[Dict]
    ) -> Tuple[List[Dict], DeduplicationResult]:
        """Deduplicate and merge records."""
        unique, duplicate_groups = self.deduplicate(
            records,
            return_duplicate_groups=True
        )
        
        merger = MergeEngine(policy=self.config.merge_policy)
        merged, merged_count = merger.merge(records, duplicate_groups)
        
        result = DeduplicationResult(
            total_records=len(records),
            unique_records=len(merged),
            duplicates_found=len(records) - len(merged),
            merged_records=merged_count,
            removed_duplicates=len(records) - len(merged)
        )
        
        return merged, result
    
    def find_similar(
        self,
        record: Dict,
        records: List[Dict],
        limit: int = 10
    ) -> List[Tuple[Dict, float]]:
        """Find similar records to a given record."""
        if isinstance(self._deduplicator, FuzzyDeduplicator):
            similarities = []
            for r in records:
                similarity = self._deduplicator.similarity(record, r)
                if similarity >= self.config.similarity_threshold:
                    similarities.append((r, similarity))
            
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:limit]
        
        return []
    
    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics."""
        return {
            **self._stats,
            "strategy": self.config.strategy.value,
            "comparison_keys": self.config.comparison_keys,
            "similarity_threshold": self.config.similarity_threshold
        }
    
    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = {
            "total_processed": 0,
            "duplicates_found": 0
        }

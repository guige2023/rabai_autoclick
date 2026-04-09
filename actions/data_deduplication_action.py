"""
Data Deduplication Module.

Provides comprehensive deduplication for datasets including
exact match, fuzzy match, and similarity-based deduplication
with configurable matching thresholds and merge strategies.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, Union, TypeVar, Generic, Iterator
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import logging
import hashlib
import json
from collections import defaultdict
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

T = TypeVar("T")


class MatchType(Enum):
    """Type of match found."""
    EXACT = auto()
    FUZZY = auto()
    SIMILAR = auto()
    DUPLICATE_GROUP = auto()


class DedupeStrategy(Enum):
    """Deduplication strategy."""
    EXACT_MATCH = auto()
    FINGERPRINT = auto()
    SIMILARITY = auto()
    CLUSTERING = auto()
    CUSTOM = auto()


@dataclass
class DuplicateGroup:
    """Group of duplicate records."""
    group_id: str
    record_ids: List[str]
    records: List[Dict[str, Any]]
    match_type: MatchType
    similarity_score: float = 1.0
    canonical_record: Optional[Dict[str, Any]] = None


@dataclass
class DeduplicationResult:
    """Result of deduplication operation."""
    total_records: int
    unique_records: int
    duplicates_removed: int
    duplicate_groups: List[DuplicateGroup]
    merge_log: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)


class ExactMatcher:
    """Exact matching using hashes."""
    
    @staticmethod
    def hash_record(record: Dict[str, Any], fields: List[str]) -> str:
        """Generate hash for specific fields."""
        values = []
        for field_name in sorted(fields):
            value = record.get(field_name)
            values.append(f"{field_name}:{value}")
        
        combined = "|".join(values)
        return hashlib.sha256(combined.encode()).hexdigest()
    
    @staticmethod
    def find_exact_duplicates(
        records: List[Dict[str, Any]],
        match_fields: List[str]
    ) -> Dict[str, List[int]]:
        """Find exact duplicates and return groups."""
        hash_to_indices = defaultdict(list)
        
        for idx, record in enumerate(records):
            record_hash = ExactMatcher.hash_record(record, match_fields)
            hash_to_indices[record_hash].append(idx)
        
        return {
            h: indices for h, indices in hash_to_indices.items()
            if len(indices) > 1
        }


class FuzzyMatcher:
    """Fuzzy matching for approximate duplicates."""
    
    def __init__(
        self,
        similarity_threshold: float = 0.85,
        string_fields: Optional[List[str]] = None
    ) -> None:
        self.similarity_threshold = similarity_threshold
        self.string_fields = string_fields or ["name", "title"]
    
    def calculate_similarity(
        self,
        record1: Dict[str, Any],
        record2: Dict[str, Any]
    ) -> float:
        """Calculate similarity between two records."""
        scores = []
        
        for field_name in self.string_fields:
            val1 = str(record1.get(field_name, ""))
            val2 = str(record2.get(field_name, ""))
            
            if val1 and val2:
                score = SequenceMatcher(None, val1.lower(), val2.lower()).ratio()
                scores.append(score)
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def find_similar_pairs(
        self,
        records: List[Dict[str, Any]]
    ) -> List[Tuple[int, int, float]]:
        """Find all similar record pairs."""
        pairs = []
        n = len(records)
        
        for i in range(n):
            for j in range(i + 1, n):
                similarity = self.calculate_similarity(records[i], records[j])
                
                if similarity >= self.similarity_threshold:
                    pairs.append((i, j, similarity))
        
        return pairs


class UnionFind:
    """Union-Find data structure for grouping."""
    
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.rank = [0] * n
    
    def find(self, x: int) -> int:
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x: int, y: int) -> None:
        px, py = self.find(x), self.find(y)
        
        if px == py:
            return
        
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        
        self.parent[py] = px
        
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1


class RecordMerger:
    """Merges duplicate records with conflict resolution."""
    
    def __init__(
        self,
        merge_strategy: str = "prefer_longest"
    ) -> None:
        self.merge_strategy = merge_strategy
    
    def merge(
        self,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Merge multiple records into one.
        
        Args:
            records: List of duplicate records
            
        Returns:
            Merged record
        """
        if not records:
            return {}
        
        if len(records) == 1:
            return dict(records[0])
        
        merged = {}
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        for field_name in all_fields:
            values = [r.get(field_name) for r in records if field_name in r]
            values = [v for v in values if v is not None]
            
            if not values:
                continue
            
            if len(values) == 1:
                merged[field_name] = values[0]
            else:
                merged[field_name] = self._resolve_conflict(values, field_name)
        
        return merged
    
    def _resolve_conflict(
        self,
        values: List[Any],
        field_name: str
    ) -> Any:
        """Resolve conflict between multiple values."""
        if self.merge_strategy == "prefer_longest":
            return max(str(v) for v in values)
        elif self.merge_strategy == "prefer_recent":
            # Assume records have 'updated_at' field
            if "updated_at" in field_name.lower():
                return max(values)
            return values[0]
        elif self.merge_strategy == "concat":
            return ", ".join(str(v) for v in values)
        
        return values[0]


class DataDeduplicator:
    """
    Comprehensive data deduplication engine.
    
    Supports multiple deduplication strategies including
    exact match, fuzzy matching, and clustering-based deduplication.
    """
    
    def __init__(
        self,
        strategy: DedupeStrategy = DedupeStrategy.EXACT_MATCH,
        match_fields: Optional[List[str]] = None,
        similarity_threshold: float = 0.85
    ) -> None:
        self.strategy = strategy
        self.match_fields = match_fields or ["name", "email"]
        self.similarity_threshold = similarity_threshold
        
        self.exact_matcher = ExactMatcher()
        self.fuzzy_matcher = FuzzyMatcher(
            similarity_threshold=similarity_threshold,
            string_fields=match_fields
        )
        self.merger = RecordMerger()
    
    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        record_ids: Optional[List[str]] = None
    ) -> DeduplicationResult:
        """
        Deduplicate records.
        
        Args:
            records: List of records to deduplicate
            record_ids: Optional list of record IDs
            
        Returns:
            DeduplicationResult
        """
        if not records:
            return DeduplicationResult(
                total_records=0,
                unique_records=0,
                duplicates_removed=0,
                duplicate_groups=[],
                merge_log=[]
            )
        
        if record_ids is None:
            record_ids = [str(i) for i in range(len(records))]
        
        if self.strategy == DedupeStrategy.EXACT_MATCH:
            return self._deduplicate_exact(records, record_ids)
        elif self.strategy == DedupeStrategy.SIMILARITY:
            return self._deduplicate_fuzzy(records, record_ids)
        elif self.strategy == DedupeStrategy.CLUSTERING:
            return self._deduplicate_clustering(records, record_ids)
        
        return self._deduplicate_exact(records, record_ids)
    
    def _deduplicate_exact(
        self,
        records: List[Dict[str, Any]],
        record_ids: List[str]
    ) -> DeduplicationResult:
        """Exact match deduplication."""
        duplicate_hashes = self.exact_matcher.find_exact_duplicates(
            records, self.match_fields
        )
        
        duplicate_groups = []
        merge_log = []
        seen_hashes = set()
        
        for record_hash, indices in duplicate_hashes.items():
            if record_hash in seen_hashes:
                continue
            seen_hashes.add(record_hash)
            
            group_records = [records[i] for i in indices]
            group_ids = [record_ids[i] for i in indices]
            
            merged = self.merger.merge(group_records)
            
            group = DuplicateGroup(
                group_id=f"exact_{len(duplicate_groups)}",
                record_ids=group_ids,
                records=group_records,
                match_type=MatchType.EXACT,
                similarity_score=1.0,
                canonical_record=merged
            )
            
            duplicate_groups.append(group)
            merge_log.append({
                "action": "merge",
                "records": group_ids,
                "merged": merged
            })
        
        unique_count = len(records) - sum(
            len(g.record_ids) - 1 for g in duplicate_groups
        )
        
        return DeduplicationResult(
            total_records=len(records),
            unique_records=unique_count,
            duplicates_removed=len(records) - unique_count,
            duplicate_groups=duplicate_groups,
            merge_log=merge_log
        )
    
    def _deduplicate_fuzzy(
        self,
        records: List[Dict[str, Any]],
        record_ids: List[str]
    ) -> DeduplicationResult:
        """Fuzzy match deduplication."""
        similar_pairs = self.fuzzy_matcher.find_similar_pairs(records)
        
        # Build union-find structure
        uf = UnionFind(len(records))
        for idx1, idx2, _ in similar_pairs:
            uf.union(idx1, idx2)
        
        # Group duplicates
        groups: Dict[int, List[int]] = defaultdict(list)
        for i in range(len(records)):
            root = uf.find(i)
            groups[root].append(i)
        
        # Build duplicate groups
        duplicate_groups = []
        merge_log = []
        
        for root, indices in groups.items():
            if len(indices) == 1:
                continue
            
            group_records = [records[i] for i in indices]
            group_ids = [record_ids[i] for i in indices]
            
            avg_similarity = sum(
                self.fuzzy_matcher.calculate_similarity(records[p[0]], records[p[1]])
                for p in similar_pairs
                if p[0] in indices and p[1] in indices
            ) / max(1, len(similar_pairs))
            
            merged = self.merger.merge(group_records)
            
            group = DuplicateGroup(
                group_id=f"fuzzy_{len(duplicate_groups)}",
                record_ids=group_ids,
                records=group_records,
                match_type=MatchType.FUZZY,
                similarity_score=avg_similarity,
                canonical_record=merged
            )
            
            duplicate_groups.append(group)
            merge_log.append({
                "action": "merge",
                "records": group_ids,
                "merged": merged,
                "similarity": avg_similarity
            })
        
        unique_count = len(records) - sum(
            len(g.record_ids) - 1 for g in duplicate_groups
        )
        
        return DeduplicationResult(
            total_records=len(records),
            unique_records=unique_count,
            duplicates_removed=len(records) - unique_count,
            duplicate_groups=duplicate_groups,
            merge_log=merge_log
        )
    
    def _deduplicate_clustering(
        self,
        records: List[Dict[str, Any]],
        record_ids: List[str]
    ) -> DeduplicationResult:
        """Clustering-based deduplication."""
        # Use fuzzy matching as foundation
        return self._deduplicate_fuzzy(records, record_ids)


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample data with duplicates
    records = [
        {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "phone": "555-1234"},
        {"id": 2, "name": "Alice Johnson", "email": "alice@example.com", "phone": "555-1234"},  # Exact dup
        {"id": 3, "name": "Alice Jhnson", "email": "alice@example.com", "phone": "555-1235"},  # Fuzzy dup
        {"id": 4, "name": "Bob Smith", "email": "bob@example.com", "phone": "555-5678"},
        {"id": 5, "name": "Carol Williams", "email": "carol@example.com", "phone": "555-9999"},
    ]
    
    print("=== Data Deduplication Demo ===\n")
    
    # Exact match deduplication
    deduper = DataDeduplicator(
        strategy=DedupeStrategy.EXACT_MATCH,
        match_fields=["name", "email"]
    )
    
    result = deduper.deduplicate(records)
    
    print(f"Total records: {result.total_records}")
    print(f"Unique records: {result.unique_records}")
    print(f"Duplicates removed: {result.duplicates_removed}")
    print(f"Duplicate groups: {len(result.duplicate_groups)}")
    
    for group in result.duplicate_groups:
        print(f"\n  Group {group.group_id}:")
        print(f"    Type: {group.match_type.name}")
        print(f"    Records: {group.record_ids}")
        print(f"    Merged: {group.canonical_record}")
    
    # Fuzzy match deduplication
    print("\n=== Fuzzy Matching ===")
    
    fuzzy_deduper = DataDeduplicator(
        strategy=DedupeStrategy.SIMILARITY,
        match_fields=["name"],
        similarity_threshold=0.8
    )
    
    result = fuzzy_deduper.deduplicate(records)
    print(f"Fuzzy duplicate groups: {len(result.duplicate_groups)}")

"""Data Deduplication Action Module.

Provides data deduplication capabilities including exact match,
fuzzy match, and similarity-based deduplication for datasets.

Example:
    >>> from actions.data.data_dedupe_action import DataDeduplicator, DedupStrategy
    >>> deduper = DataDeduplicator()
    >>> unique_records = deduper.deduplicate(records, key="email")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import hashlib
import re
import threading


class DedupStrategy(Enum):
    """Deduplication strategies."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    SIMILARITY = "similarity"
    COMPOSITE = "composite"


class MatchConfidence(Enum):
    """Match confidence levels."""
    EXACT = "exact"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class DedupConfig:
    """Deduplication configuration.
    
    Attributes:
        strategy: Deduplication strategy
        similarity_threshold: Threshold for fuzzy matching
        ignore_fields: Fields to ignore in comparison
        case_sensitive: Case-sensitive comparison
        normalize_whitespace: Normalize whitespace
    """
    strategy: DedupStrategy = DedupStrategy.EXACT
    similarity_threshold: float = 0.85
    ignore_fields: Set[str] = field(default_factory=set)
    case_sensitive: bool = False
    normalize_whitespace: bool = True


@dataclass
class DuplicateGroup:
    """Group of duplicate records.
    
    Attributes:
        group_id: Unique group identifier
        record_ids: IDs of duplicate records
        match_type: Type of match found
        confidence: Match confidence score
    """
    group_id: str
    record_ids: List[str] = field(default_factory=list)
    match_type: MatchConfidence = MatchConfidence.EXACT
    confidence: float = 1.0


@dataclass
class DedupResult:
    """Deduplication result.
    
    Attributes:
        unique_records: List of unique records
        duplicate_groups: Groups of duplicates found
        removed_count: Number of records removed
        stats: Deduplication statistics
    """
    unique_records: List[Dict[str, Any]]
    duplicate_groups: List[DuplicateGroup]
    removed_count: int
    stats: Dict[str, int] = field(default_factory=dict)


class DataDeduplicator:
    """Data deduplication engine.
    
    Provides multiple deduplication strategies for cleaning
    datasets including exact, fuzzy, and similarity-based matching.
    
    Attributes:
        _config: Deduplication configuration
        _seen_keys: Seen record keys
        _seen_hashes: Seen record hashes
        _lock: Thread safety lock
    """
    
    def __init__(self, config: Optional[DedupConfig] = None) -> None:
        """Initialize deduplicator.
        
        Args:
            config: Deduplication configuration
        """
        self._config = config or DedupConfig()
        self._seen_keys: Dict[str, str] = {}
        self._seen_hashes: Set[str] = set()
        self._lock = threading.RLock()
    
    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        key: Optional[str] = None,
        keys: Optional[List[str]] = None,
    ) -> DedupResult:
        """Deduplicate records.
        
        Args:
            records: List of records to deduplicate
            key: Single key to use for comparison
            keys: Multiple keys to use for composite comparison
            
        Returns:
            Deduplication result
        """
        if self._config.strategy == DedupStrategy.EXACT:
            return self._deduplicate_exact(records, key, keys)
        elif self._config.strategy == DedupStrategy.FUZZY:
            return self._deduplicate_fuzzy(records, key, keys)
        elif self._config.strategy == DedupStrategy.SIMILARITY:
            return self._deduplicate_similarity(records)
        else:
            return self._deduplicate_exact(records, key, keys)
    
    def _deduplicate_exact(
        self,
        records: List[Dict[str, Any]],
        key: Optional[str] = None,
        keys: Optional[List[str]] = None,
    ) -> DedupResult:
        """Deduplicate using exact matching.
        
        Args:
            records: List of records
            key: Single key to compare
            keys: Multiple keys to compare
            
        Returns:
            Deduplication result
        """
        self._seen_keys.clear()
        self._seen_hashes.clear()
        
        unique_records: List[Dict[str, Any]] = []
        duplicate_groups: List[DuplicateGroup] = []
        group_counter = 0
        
        for record in records:
            record_id = record.get("id", str(id(record)))
            
            # Compute key for comparison
            if key:
                compare_key = self._normalize_value(record.get(key))
            elif keys:
                compare_key = self._compute_composite_key(record, keys)
            else:
                compare_key = self._compute_hash_key(record)
            
            if compare_key in self._seen_keys:
                # Duplicate found
                group_id = self._seen_keys[compare_key]
                for group in duplicate_groups:
                    if group.group_id == group_id:
                        group.record_ids.append(record_id)
                        group.confidence = 1.0
                        break
            else:
                # Unique record
                group_id = f"group_{group_counter}"
                group_counter += 1
                self._seen_keys[compare_key] = group_id
                
                if len(duplicate_groups) > 0 or len(unique_records) > 0:
                    # Only add new groups for duplicates, not initial record
                    pass
                
                unique_records.append(record)
                duplicate_groups.append(DuplicateGroup(
                    group_id=group_id,
                    record_ids=[record_id],
                    match_type=MatchConfidence.EXACT,
                    confidence=1.0,
                ))
        
        # Remove groups with only one record (not actually duplicates)
        actual_dup_groups = [g for g in duplicate_groups if len(g.record_ids) > 1]
        
        return DedupResult(
            unique_records=unique_records,
            duplicate_groups=actual_dup_groups,
            removed_count=sum(len(g.record_ids) - 1 for g in actual_dup_groups),
            stats={
                "total_records": len(records),
                "unique_records": len(unique_records),
                "duplicate_groups": len(actual_dup_groups),
                "duplicates_removed": sum(len(g.record_ids) - 1 for g in actual_dup_groups),
            },
        )
    
    def _deduplicate_fuzzy(
        self,
        records: List[Dict[str, Any]],
        key: Optional[str] = None,
        keys: Optional[List[str]] = None,
    ) -> DedupResult:
        """Deduplicate using fuzzy matching.
        
        Args:
            records: List of records
            key: Primary key to compare
            keys: Multiple keys to compare
            
        Returns:
            Deduplication result
        """
        unique_records: List[Dict[str, Any]] = []
        duplicate_groups: List[DuplicateGroup] = []
        
        for record in records:
            record_id = record.get("id", str(id(record)))
            
            if key:
                compare_value = self._normalize_value(record.get(key, ""))
            elif keys:
                compare_value = self._normalize_value(
                    " ".join(str(record.get(k, "")) for k in keys)
                )
            else:
                compare_value = self._normalize_value(str(record))
            
            # Find best match among unique records
            best_match = None
            best_similarity = 0.0
            
            for idx, unique_rec in enumerate(unique_records):
                if key:
                    unique_value = self._normalize_value(unique_rec.get(key, ""))
                elif keys:
                    unique_value = self._normalize_value(
                        " ".join(str(unique_rec.get(k, "")) for k in keys)
                    )
                else:
                    unique_value = self._normalize_value(str(unique_rec))
                
                similarity = self._calculate_similarity(compare_value, unique_value)
                if similarity >= self._config.similarity_threshold and similarity > best_similarity:
                    best_match = idx
                    best_similarity = similarity
            
            if best_match is not None:
                # Found duplicate
                group_id = f"fuzzy_group_{len(duplicate_groups)}"
                duplicate_groups.append(DuplicateGroup(
                    group_id=group_id,
                    record_ids=[record_id],
                    match_type=self._similarity_to_confidence(best_similarity),
                    confidence=best_similarity,
                ))
            else:
                unique_records.append(record)
        
        return DedupResult(
            unique_records=unique_records,
            duplicate_groups=duplicate_groups,
            removed_count=len(records) - len(unique_records),
            stats={
                "total_records": len(records),
                "unique_records": len(unique_records),
                "duplicate_groups": len(duplicate_groups),
                "duplicates_removed": len(records) - len(unique_records),
            },
        )
    
    def _deduplicate_similarity(self, records: List[Dict[str, Any]]) -> DedupResult:
        """Deduplicate using record similarity.
        
        Args:
            records: List of records
            
        Returns:
            Deduplication result
        """
        unique_records: List[Dict[str, Any]] = []
        duplicate_groups: List[DuplicateGroup] = []
        
        for record in records:
            record_id = record.get("id", str(id(record)))
            record_hash = self._compute_record_hash(record)
            
            if record_hash in self._seen_hashes:
                continue
            
            self._seen_hashes.add(record_hash)
            
            # Check similarity with existing unique records
            best_similarity = 0.0
            best_group_idx = -1
            
            for idx, unique_rec in enumerate(unique_records):
                similarity = self._calculate_record_similarity(record, unique_rec)
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_group_idx = idx
            
            if best_similarity >= self._config.similarity_threshold:
                # Similar to existing record
                duplicate_groups[best_group_idx].record_ids.append(record_id)
                duplicate_groups[best_group_idx].confidence = best_similarity
            else:
                unique_records.append(record)
                duplicate_groups.append(DuplicateGroup(
                    group_id=f"sim_group_{len(duplicate_groups)}",
                    record_ids=[record_id],
                    match_type=self._similarity_to_confidence(best_similarity),
                    confidence=best_similarity,
                ))
        
        return DedupResult(
            unique_records=unique_records,
            duplicate_groups=[g for g in duplicate_groups if len(g.record_ids) > 1],
            removed_count=len(records) - len(unique_records),
            stats={},
        )
    
    def _normalize_value(self, value: Any) -> str:
        """Normalize a value for comparison.
        
        Args:
            value: Value to normalize
            
        Returns:
            Normalized string
        """
        if value is None:
            return ""
        
        str_value = str(value)
        
        if self._config.normalize_whitespace:
            str_value = re.sub(r'\s+', ' ', str_value).strip()
        
        if not self._config.case_sensitive:
            str_value = str_value.lower()
        
        return str_value
    
    def _compute_composite_key(
        self,
        record: Dict[str, Any],
        keys: List[str],
    ) -> str:
        """Compute composite key from multiple fields.
        
        Args:
            record: Record to compute key for
            keys: Fields to include in key
            
        Returns:
            Composite key string
        """
        values = []
        for k in keys:
            if k in self._config.ignore_fields:
                continue
            values.append(self._normalize_value(record.get(k)))
        return "|".join(values)
    
    def _compute_hash_key(self, record: Dict[str, Any]) -> str:
        """Compute hash key for record.
        
        Args:
            record: Record to hash
            
        Returns:
            Hash key string
        """
        filtered = {k: v for k, v in record.items() if k not in self._config.ignore_fields}
        key_str = str(sorted(filtered.items()))
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _compute_record_hash(self, record: Dict[str, Any]) -> str:
        """Compute hash for full record.
        
        Args:
            record: Record to hash
            
        Returns:
            Record hash
        """
        filtered = {k: v for k, v in record.items() if k not in self._config.ignore_fields}
        key_str = str(sorted(filtered.items()))
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate string similarity using Jaccard.
        
        Args:
            str1: First string
            str2: Second string
            
        Returns:
            Similarity score 0-1
        """
        if str1 == str2:
            return 1.0
        
        set1 = set(str1.split())
        set2 = set(str2.split())
        
        if not set1 or not set2:
            return 0.0
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    def _calculate_record_similarity(
        self,
        record1: Dict[str, Any],
        record2: Dict[str, Any],
    ) -> float:
        """Calculate record-level similarity.
        
        Args:
            record1: First record
            record2: Second record
            
        Returns:
            Similarity score 0-1
        """
        all_keys = set(record1.keys()) | set(record2.keys())
        all_keys -= self._config.ignore_fields
        
        if not all_keys:
            return 1.0
        
        matches = 0
        for key in all_keys:
            v1 = self._normalize_value(record1.get(key))
            v2 = self._normalize_value(record2.get(key))
            if v1 == v2:
                matches += 1
        
        return matches / len(all_keys)
    
    def _similarity_to_confidence(self, similarity: float) -> MatchConfidence:
        """Convert similarity score to confidence level.
        
        Args:
            similarity: Similarity score
            
        Returns:
            Match confidence level
        """
        if similarity >= 0.95:
            return MatchConfidence.EXACT
        elif similarity >= 0.85:
            return MatchConfidence.HIGH
        elif similarity >= 0.7:
            return MatchConfidence.MEDIUM
        else:
            return MatchConfidence.LOW

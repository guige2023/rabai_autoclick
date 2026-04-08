"""
Data Deduplication Action Module.

Deduplicates data using exact match, fuzzy match, and configurable
key-based strategies with memory-efficient processing.

Author: RabAi Team
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DedupeStrategy(Enum):
    """Deduplication strategies."""
    EXACT = "exact"
    KEY_BASED = "key_based"
    FUZZY = "fuzzy"
    SIMILARITY = "similarity"
    SEQUENCE = "sequence"


class DedupeAlgorithm(Enum):
    """Hashing algorithms for exact dedup."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"


@dataclass
class DedupeConfig:
    """Configuration for deduplication."""
    strategy: DedupeStrategy = DedupeStrategy.EXACT
    key_fields: List[str] = field(default_factory=list)
    threshold: float = 0.85
    algorithm: DedupeAlgorithm = DedupeAlgorithm.SHA256
    case_sensitive: bool = True
    ignore_whitespace: bool = True
    ignore_fields: List[str] = field(default_factory=list)
    keep: str = "first"


@dataclass
class DedupeResult:
    """Result of deduplication."""
    original_count: int
    deduplicated_count: int
    duplicates_removed: int
    items: List[Any]
    removed_indices: List[int]


class DataDedupeAction(BaseAction):
    """Data deduplication action.
    
    Removes duplicate records using multiple strategies
    with configurable comparison functions and key extraction.
    """
    action_type = "data_dedupe"
    display_name = "数据去重"
    description = "数据重复记录去除"
    
    def __init__(self):
        super().__init__()
        self._default_config = DedupeConfig()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Deduplicate data records.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - data: List of records to deduplicate
                - strategy: Deduplication strategy
                - key_fields: Fields to use for key-based dedup
                - threshold: Similarity threshold for fuzzy dedup
                - algorithm: Hash algorithm for exact dedup
                - case_sensitive: Case-sensitive comparison
                - ignore_whitespace: Ignore whitespace differences
                - keep: Which item to keep (first/last)
                
        Returns:
            ActionResult with deduplicated data.
        """
        start_time = time.time()
        
        data = params.get("data", [])
        config_dict = params.get("config", {})
        
        strategy = DedupeStrategy(config_dict.get("strategy", "exact"))
        key_fields = config_dict.get("key_fields", [])
        threshold = config_dict.get("threshold", 0.85)
        algorithm = DedupeAlgorithm(config_dict.get("algorithm", "sha256"))
        case_sensitive = config_dict.get("case_sensitive", True)
        ignore_whitespace = config_dict.get("ignore_whitespace", True)
        ignore_fields = config_dict.get("ignore_fields", [])
        keep = config_dict.get("keep", "first")
        
        if not data:
            return ActionResult(
                success=True,
                message="No data to deduplicate",
                data={"original_count": 0, "deduplicated_count": 0, "duplicates_removed": 0},
                duration=time.time() - start_time
            )
        
        try:
            if strategy == DedupeStrategy.EXACT:
                result = self._dedupe_exact(data, algorithm, case_sensitive, ignore_whitespace, keep)
            elif strategy == DedupeStrategy.KEY_BASED:
                result = self._dedupe_key_based(data, key_fields, case_sensitive, ignore_whitespace, keep)
            elif strategy == DedupeStrategy.FUZZY:
                result = self._dedupe_fuzzy(data, threshold, case_sensitive, ignore_whitespace, keep)
            elif strategy == DedupeStrategy.SIMILARITY:
                result = self._dedupe_similarity(data, threshold, keep)
            elif strategy == DedupeStrategy.SEQUENCE:
                result = self._dedupe_sequence(data, ignore_fields, keep)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown strategy: {strategy}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Deduplication complete: {result.duplicates_removed} duplicates removed",
                data={
                    "original_count": result.original_count,
                    "deduplicated_count": result.deduplicated_count,
                    "duplicates_removed": result.duplicates_removed,
                    "items": result.items,
                    "removed_indices": result.removed_indices
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Deduplication failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _normalize(self, value: Any, case_sensitive: bool, ignore_whitespace: bool) -> Any:
        """Normalize a value for comparison."""
        if isinstance(value, str):
            result = value
            if not case_sensitive:
                result = result.lower()
            if ignore_whitespace:
                result = " ".join(result.split())
            return result
        elif isinstance(value, (list, dict)):
            return str(value)
        return value
    
    def _hash_item(self, item: Any, algorithm: DedupeAlgorithm, case_sensitive: bool, ignore_whitespace: bool) -> str:
        """Hash an item for exact deduplication."""
        if isinstance(item, dict):
            normalized = {k: self._normalize(v, case_sensitive, ignore_whitespace) for k, v in item.items()}
            content = str(sorted(normalized.items()))
        elif isinstance(item, (list, tuple)):
            content = str([self._normalize(v, case_sensitive, ignore_whitespace) for v in item])
        else:
            content = str(self._normalize(item, case_sensitive, ignore_whitespace))
        
        if algorithm == DedupeAlgorithm.MD5:
            return hashlib.md5(content.encode()).hexdigest()
        elif algorithm == DedupeAlgorithm.SHA1:
            return hashlib.sha1(content.encode()).hexdigest()
        else:
            return hashlib.sha256(content.encode()).hexdigest()
    
    def _dedupe_exact(
        self, data: List[Any], algorithm: DedupeAlgorithm, case_sensitive: bool, ignore_whitespace: bool, keep: str
    ) -> DedupeResult:
        """Deduplicate using exact match (hash-based)."""
        seen = OrderedDict()
        items = []
        removed_indices = []
        
        for i, item in enumerate(data):
            key = self._hash_item(item, algorithm, case_sensitive, ignore_whitespace)
            
            if key not in seen:
                seen[key] = i
                items.append(item)
            elif keep == "last":
                items[seen[key]] = item
                removed_indices.append(i)
            else:
                removed_indices.append(i)
        
        return DedupeResult(
            original_count=len(data),
            deduplicated_count=len(items),
            duplicates_removed=len(data) - len(items),
            items=items,
            removed_indices=removed_indices
        )
    
    def _dedupe_key_based(
        self, data: List[Any], key_fields: List[str], case_sensitive: bool, ignore_whitespace: bool, keep: str
    ) -> DedupeResult:
        """Deduplicate using key field extraction."""
        if not key_fields:
            return self._dedupe_exact(data, DedupeAlgorithm.SHA256, case_sensitive, ignore_whitespace, keep)
        
        seen = OrderedDict()
        items = []
        removed_indices = []
        
        for i, item in enumerate(data):
            if isinstance(item, dict):
                key_values = []
                for field in key_fields:
                    value = item.get(field)
                    key_values.append(self._normalize(value, case_sensitive, ignore_whitespace))
                key = tuple(key_values)
            else:
                key = str(item)
            
            if key not in seen:
                seen[key] = i
                items.append(item)
            elif keep == "last":
                items[seen[key]] = item
                removed_indices.append(i)
            else:
                removed_indices.append(i)
        
        return DedupeResult(
            original_count=len(data),
            deduplicated_count=len(items),
            duplicates_removed=len(data) - len(items),
            items=items,
            removed_indices=removed_indices
        )
    
    def _dedupe_fuzzy(
        self, data: List[Any], threshold: float, case_sensitive: bool, ignore_whitespace: bool, keep: str
    ) -> DedupeResult:
        """Deduplicate using fuzzy string matching."""
        items = []
        removed_indices = []
        
        for i, item in enumerate(data):
            if isinstance(item, str):
                item_str = self._normalize(item, case_sensitive, ignore_whitespace)
                is_duplicate = False
                
                for existing in items:
                    existing_str = self._normalize(existing, case_sensitive, ignore_whitespace) if isinstance(existing, str) else ""
                    if self._calculate_similarity(item_str, existing_str) >= threshold:
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    items.append(item)
                else:
                    removed_indices.append(i)
            elif isinstance(item, dict):
                items.append(item)
            else:
                items.append(item)
        
        return DedupeResult(
            original_count=len(data),
            deduplicated_count=len(items),
            duplicates_removed=len(data) - len(items),
            items=items,
            removed_indices=removed_indices
        )
    
    def _dedupe_similarity(self, data: List[Any], threshold: float, keep: str) -> DedupeResult:
        """Deduplicate using similarity-based grouping."""
        groups: List[List[Tuple[int, Any]]] = []
        items = []
        removed_indices = []
        
        for i, item in enumerate(data):
            item_str = str(item) if not isinstance(item, str) else item
            best_match = -1
            
            for g, group in enumerate(groups):
                existing = group[0][1]
                existing_str = str(existing) if not isinstance(existing, str) else existing
                if self._calculate_similarity(item_str, existing_str) >= threshold:
                    best_match = g
                    break
            
            if best_match >= 0:
                if keep == "last":
                    groups[best_match].append((i, item))
                    removed_indices.append(i)
                else:
                    groups[best_match].insert(0, (i, item))
                    removed_indices.append(i)
            else:
                groups.append([(i, item)])
        
        for group in groups:
            items.append(group[0][1] if keep == "first" else group[-1][1])
        
        return DedupeResult(
            original_count=len(data),
            deduplicated_count=len(items),
            duplicates_removed=len(data) - len(items),
            items=items,
            removed_indices=sorted(set(removed_indices))
        )
    
    def _dedupe_sequence(self, data: List[Any], ignore_fields: List[str], keep: str) -> DedupeResult:
        """Deduplicate consecutive sequence items."""
        items = []
        removed_indices = []
        prev_item = None
        
        for i, item in enumerate(data):
            is_dup = False
            
            if isinstance(item, dict) and isinstance(prev_item, dict):
                filtered_item = {k: v for k, v in item.items() if k not in ignore_fields}
                filtered_prev = {k: v for k, v in prev_item.items() if k not in ignore_fields}
                is_dup = filtered_item == filtered_prev
            else:
                is_dup = item == prev_item
            
            if not is_dup:
                items.append(item)
                prev_item = item
            else:
                removed_indices.append(i)
        
        return DedupeResult(
            original_count=len(data),
            deduplicated_count=len(items),
            duplicates_removed=len(data) - len(items),
            items=items,
            removed_indices=removed_indices
        )
    
    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity between two strings using Jaccard index."""
        if not s1 or not s2:
            return 0.0
        
        set1 = set(s1.lower().split())
        set2 = set(s2.lower().split())
        
        if not set1 or not set2:
            return 0.0
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0.0
        
        return intersection / union
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate dedupe parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []

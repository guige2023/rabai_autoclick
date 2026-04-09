"""
Data Deduplicator Action Module.

Detects and removes duplicate data.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


@dataclass
class DuplicateGroup:
    """A group of duplicate records."""
    canonical_id: str
    duplicates: List[str] = field(default_factory=list)
    count: int = 0


@dataclass
class DedupConfig:
    """Configuration for deduplication."""
    hash_fields: Optional[List[str]] = None
    threshold: float = 1.0
    ignore_fields: Optional[List[str]] = None


class DataDeduplicatorAction:
    """
    Deduplicate data using various strategies.

    Supports exact, fuzzy, and probabilistic deduplication.
    """

    def __init__(
        self,
        config: Optional[DedupConfig] = None,
    ) -> None:
        self.config = config or DedupConfig()
        self._hash_cache: Dict[str, str] = {}

    def deduplicate(
        self,
        records: List[Dict[str, Any]],
        key_fields: Optional[List[str]] = None,
    ) -> Tuple[List[Dict[str, Any]], List[DuplicateGroup]]:
        """
        Deduplicate records.

        Args:
            records: List of records
            key_fields: Fields to use for comparison

        Returns:
            Tuple of (unique_records, duplicate_groups)
        """
        if key_fields:
            return self._dedupe_by_key(records, key_fields)

        return self._dedupe_by_hash(records)

    def _dedupe_by_key(
        self,
        records: List[Dict[str, Any]],
        key_fields: List[str],
    ) -> Tuple[List[Dict[str, Any]], List[DuplicateGroup]]:
        """Dedupe by specific key fields."""
        seen: Dict[str, Dict[str, Any]] = {}
        groups: Dict[str, DuplicateGroup] = {}
        unique: List[Dict[str, Any]] = []

        for record in records:
            key = self._compute_key(record, key_fields)

            if key not in seen:
                seen[key] = record
                unique.append(record)
                groups[key] = DuplicateGroup(
                    canonical_id=str(id(record)),
                    count=1,
                )
            else:
                groups[key].duplicates.append(str(id(record)))
                groups[key].count += 1

        return unique, list(groups.values())

    def _dedupe_by_hash(
        self,
        records: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[DuplicateGroup]]:
        """Dedupe by hashing all fields."""
        seen: Dict[str, Dict[str, Any]] = {}
        groups: Dict[str, DuplicateGroup] = {}
        unique: List[Dict[str, Any]] = []

        for record in records:
            record_hash = self._hash_record(record)

            if record_hash not in seen:
                seen[record_hash] = record
                unique.append(record)
                groups[record_hash] = DuplicateGroup(
                    canonical_id=record_hash,
                    count=1,
                )
            else:
                groups[record_hash].duplicates.append(record_hash)
                groups[record_hash].count += 1

        return unique, list(groups.values())

    def _compute_key(
        self,
        record: Dict[str, Any],
        fields: List[str],
    ) -> str:
        """Compute key from fields."""
        values = [str(record.get(f, "")) for f in fields]
        return "|".join(values)

    def _hash_record(
        self,
        record: Dict[str, Any],
    ) -> str:
        """Hash a record."""
        ignore = self.config.ignore_fields or []
        filtered = {k: v for k, v in record.items() if k not in ignore}

        key = str(sorted(filtered.items()))
        return hashlib.md5(key.encode()).hexdigest()

    def find_similar(
        self,
        records: List[Dict[str, Any]],
        target: Dict[str, Any],
        similarity_func: Optional[Callable[[Dict, Dict], float]] = None,
        threshold: float = 0.8,
    ) -> List[Tuple[Dict[str, Any], float]]:
        """
        Find records similar to target.

        Args:
            records: List of records
            target: Target record
            similarity_func: Custom similarity function
            threshold: Minimum similarity threshold

        Returns:
            List of (record, similarity) tuples
        """
        if similarity_func is None:
            similarity_func = self._jaccard_similarity

        results = []
        for record in records:
            sim = similarity_func(record, target)
            if sim >= threshold:
                results.append((record, sim))

        return sorted(results, key=lambda x: x[1], reverse=True)

    def _jaccard_similarity(
        self,
        a: Dict[str, Any],
        b: Dict[str, Any],
    ) -> float:
        """Jaccard similarity between records."""
        keys_a = set(a.keys())
        keys_b = set(b.keys())

        if not keys_a or not keys_b:
            return 0.0

        intersection = len(keys_a & keys_b)
        union = len(keys_a | keys_b)

        return intersection / union if union > 0 else 0.0

    def cluster(
        self,
        records: List[Dict[str, Any]],
        threshold: float = 0.8,
    ) -> List[List[Dict[str, Any]]]:
        """
        Cluster similar records together.

        Args:
            records: List of records
            threshold: Similarity threshold

        Returns:
            List of clusters
        """
        clusters: List[List[Dict[str, Any]]] = []
        assigned = set()

        for i, record in enumerate(records):
            if i in assigned:
                continue

            cluster = [record]
            assigned.add(i)

            for j, other in enumerate(records):
                if j in assigned:
                    continue

                if self._jaccard_similarity(record, other) >= threshold:
                    cluster.append(other)
                    assigned.add(j)

            clusters.append(cluster)

        return clusters

    def merge_duplicates(
        self,
        records: List[Dict[str, Any]],
        merge_func: Optional[Callable[[List[Dict[str, Any]]], Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Dedupe and merge duplicates.

        Args:
            records: List of records
            merge_func: Function to merge duplicates

        Returns:
            List of merged records
        """
        unique, groups = self.deduplicate(records)

        if merge_func is None:
            merge_func = self._default_merge

        result = []
        for group in groups:
            if group.count > 1:
                members = [r for r in records if str(id(r)) in [group.canonical_id] + group.duplicates]
                merged = merge_func(members)
                result.append(merged)
            else:
                for record in records:
                    if str(id(record)) == group.canonical_id:
                        result.append(record)
                        break

        return result

    def _default_merge(
        self,
        records: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Default merge strategy - prefer non-null values."""
        if not records:
            return {}

        keys = set()
        for r in records:
            keys.update(r.keys())

        result = {}
        for key in keys:
            values = [r.get(key) for r in records if key in r]
            non_null = [v for v in values if v is not None]
            result[key] = non_null[0] if non_null else None

        return result

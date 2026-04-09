"""
Data Merger Action Module.

Merges and joins data from multiple sources.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class MergeStrategy(Enum):
    """Merge strategies."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


class DataMergerAction:
    """
    Merge and join data from multiple sources.

    Supports SQL-style joins and custom merge strategies.
    """

    def __init__(self) -> None:
        self._transforms: Dict[str, Callable[[Any], Any]] = {}

    def add_field_transform(
        self,
        field: str,
        transform: Callable[[Any], Any],
    ) -> None:
        """Add a field transformation for merging."""
        self._transforms[field] = transform

    def merge(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        on: Optional[Dict[str, str]] = None,
        strategy: MergeStrategy = MergeStrategy.INNER,
    ) -> List[Dict[str, Any]]:
        """
        Merge two lists of records.

        Args:
            left: Left dataset
            right: Right dataset
            on: Join keys {"left_key": "right_key"}
            strategy: Merge strategy

        Returns:
            Merged records
        """
        if on is None:
            return self._merge_all(left, right, strategy)

        return self._merge_on_key(left, right, on, strategy)

    def _merge_on_key(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        on: Dict[str, str],
        strategy: MergeStrategy,
    ) -> List[Dict[str, Any]]:
        """Merge on specified keys."""
        left_key = list(on.keys())[0]
        right_key = on[left_key]

        right_index = {r.get(right_key): r for r in right}

        result = []

        for l_record in left:
            l_val = l_record.get(left_key)
            r_record = right_index.get(l_val)

            if r_record is not None:
                merged = {**l_record, **r_record}
                result.append(merged)
            elif strategy in (MergeStrategy.LEFT, MergeStrategy.FULL):
                result.append(l_record.copy())

        if strategy in (MergeStrategy.RIGHT, MergeStrategy.FULL):
            matched_keys = {r.get(right_key) for r in result}
            for r_record in right:
                r_val = r_record.get(right_key)
                if r_val not in matched_keys:
                    result.append(r_record.copy())

        return result

    def _merge_all(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        strategy: MergeStrategy,
    ) -> List[Dict[str, Any]]:
        """Merge all records without key."""
        if strategy == MergeStrategy.CROSS:
            return self._cross_join(left, right)

        return []

    def _cross_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Cross join all records."""
        result = []
        for l in left:
            for r in right:
                result.append({**l, **r})
        return result

    def union(
        self,
        datasets: List[List[Dict[str, Any]]],
        dedupe: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Union multiple datasets.

        Args:
            datasets: List of datasets
            dedupe: Remove duplicates

        Returns:
            Combined dataset
        """
        result = []
        seen_keys: Set[str] = set()

        for dataset in datasets:
            for record in dataset:
                if dedupe:
                    key = self._record_key(record)
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                result.append(record)

        return result

    def intersect(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        key_fields: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Find intersection of two datasets.

        Args:
            left: Left dataset
            right: Right dataset
            key_fields: Fields to use for comparison

        Returns:
            Records in both datasets
        """
        right_keys = {self._record_key(r, key_fields) for r in right}
        result = []

        for record in left:
            if self._record_key(record, key_fields) in right_keys:
                result.append(record)

        return result

    def difference(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        key_fields: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Find records in left but not in right.

        Args:
            left: Left dataset
            right: Right dataset
            key_fields: Fields to use for comparison

        Returns:
            Records only in left
        """
        right_keys = {self._record_key(r, key_fields) for r in right}
        result = []

        for record in left:
            if self._record_key(record, key_fields) not in right_keys:
                result.append(record)

        return result

    def _record_key(
        self,
        record: Dict[str, Any],
        fields: Optional[List[str]] = None,
    ) -> str:
        """Generate a unique key for a record."""
        if fields is None:
            fields = sorted(record.keys())

        values = [str(record.get(f, "")) for f in fields]
        return "|".join(values)

    def lookup_merge(
        self,
        primary: List[Dict[str, Any]],
        lookup: Dict[str, Dict[str, Any]],
        key_field: str,
        lookup_fields: List[str],
        prefix: str = "",
    ) -> List[Dict[str, Any]]:
        """
        Merge lookup data into primary dataset.

        Args:
            primary: Primary dataset
            lookup: Lookup table
            key_field: Key field in primary
            lookup_fields: Fields to add from lookup
            prefix: Prefix for lookup field names

        Returns:
            Merged dataset
        """
        result = []

        for record in primary:
            new_record = record.copy()
            key = record.get(key_field)

            if key in lookup:
                lookup_record = lookup[key]
                for field in lookup_fields:
                    new_name = f"{prefix}{field}" if prefix else field
                    if field in lookup_record:
                        new_record[new_name] = lookup_record[field]

            result.append(new_record)

        return result

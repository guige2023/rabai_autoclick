"""Data Grouper Action Module.

Provides multi-level data grouping with aggregation,
nested grouping, and group filtering.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class Group:
    """Data group."""
    key: Any
    items: List[Dict] = field(default_factory=list)
    subgroups: Dict[str, "Group"] = field(default_factory=dict)


class DataGrouperAction:
    """Multi-level data grouper.

    Example:
        grouper = DataGrouperAction()

        result = grouper.group(
            data=[
                {"region": "US", "product": "A", "sales": 100},
                {"region": "EU", "product": "A", "sales": 200},
            ],
            group_by=["region", "product"]
        )
    """

    def __init__(self) -> None:
        pass

    def group(
        self,
        data: List[Dict[str, Any]],
        group_by: List[str],
        aggregations: Optional[Dict[str, Callable]] = None,
    ) -> List[Dict[str, Any]]:
        """Group data by fields.

        Args:
            data: List of records
            group_by: Fields to group by
            aggregations: Optional aggregation functions

        Returns:
            List of grouped results
        """
        if not data or not group_by:
            return data

        root = self._build_group_tree(data, group_by)
        return self._flatten_groups(root, group_by, aggregations)

    def _build_group_tree(
        self,
        data: List[Dict],
        group_by: List[str],
    ) -> Group:
        """Build hierarchical group tree."""
        root = Group(key=None)

        for record in data:
            self._insert_record(root, record, group_by, 0)

        return root

    def _insert_record(
        self,
        group: Group,
        record: Dict,
        group_by: List[str],
        depth: int,
    ) -> None:
        """Insert record into group tree."""
        if depth >= len(group_by):
            group.items.append(record)
            return

        key = record.get(group_by[depth])
        if key not in group.subgroups:
            group.subgroups[key] = Group(key=key)
            group.subgroups[key].items = []

        self._insert_record(
            group.subgroups[key],
            record,
            group_by,
            depth + 1
        )

    def _flatten_groups(
        self,
        group: Group,
        group_by: List[str],
        aggregations: Optional[Dict[str, Callable]],
        level: int = 0,
    ) -> List[Dict[str, Any]]:
        """Flatten group tree to list."""
        results = []

        if level == len(group_by):
            if group.items:
                result = {}

                for agg_name, agg_func in (aggregations or {}).items():
                    result[agg_name] = agg_func([item for item in group.items])

                result["_items"] = group.items
                results.append(result)

            for subgroup in group.subgroups.values():
                results.extend(
                    self._flatten_groups(subgroup, group_by, aggregations, level + 1)
                )
        else:
            if group.key is not None:
                current_results = []

                for subgroup in group.subgroups.values():
                    current_results.extend(
                        self._flatten_groups(subgroup, group_by, aggregations, level + 1)
                    )

                for result in current_results:
                    result[group_by[level]] = group.key
                    results.append(result)

        return results

    def group_by_function(
        self,
        data: List[T],
        key_func: Callable[[T], Any],
    ) -> Dict[Any, List[T]]:
        """Group data using key function.

        Args:
            data: List of items
            key_func: Function to extract group key

        Returns:
            Dict mapping keys to lists of items
        """
        result: Dict[Any, List[T]] = defaultdict(list)

        for item in data:
            key = key_func(item)
            result[key].append(item)

        return dict(result)

    def partition_by(
        self,
        data: List[Dict],
        predicate: Callable[[Dict], bool],
    ) -> Tuple[List[Dict], List[Dict]]:
        """Partition data into two groups by predicate.

        Returns:
            Tuple of (matching, non_matching)
        """
        matching = []
        non_matching = []

        for record in data:
            if predicate(record):
                matching.append(record)
            else:
                non_matching.append(record)

        return matching, non_matching

    def count_by(
        self,
        data: List[Dict],
        field: str,
    ) -> Dict[Any, int]:
        """Count items by field value.

        Returns:
            Dict mapping field values to counts
        """
        counts: Dict[Any, int] = defaultdict(int)

        for record in data:
            key = record.get(field)
            counts[key] += 1

        return dict(counts)

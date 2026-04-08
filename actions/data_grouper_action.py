"""
Data Grouper Action Module.

Groups datasets by one or more fields with support for
 aggregation functions and nested grouping.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from collections import defaultdict
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class Aggregation:
    """An aggregation specification."""
    field: str
    func: Callable[[list[Any]], Any]
    alias: str


@dataclass
class GroupResult:
    """Result of grouping operation."""
    groups: dict[str, list[dict[str, Any]]]
    aggregated: dict[str, dict[str, Any]]
    group_keys: list[str]
    group_count: int = 0


class DataGrouperAction:
    """
    Data grouping with aggregation support.

    Groups records by specified fields and computes
    aggregations within each group.

    Example:
        grouper = DataGrouperAction()
        grouper.group_by("department")
        grouper.aggregate("salary", sum, "total_salary")
        grouper.aggregate("salary", len, "count")
        result = grouper.group(data)
    """

    def __init__(self) -> None:
        self._group_fields: list[str] = []
        self._aggregations: list[Aggregation] = []

    def group_by(
        self,
        field: str,
        *additional_fields: str,
    ) -> "DataGrouperAction":
        """Set the fields to group by."""
        self._group_fields = [field] + list(additional_fields)
        return self

    def aggregate(
        self,
        field: str,
        func: Callable[[list[Any]], Any],
        alias: Optional[str] = None,
    ) -> "DataGrouperAction":
        """Add an aggregation function."""
        agg = Aggregation(
            field=field,
            func=func,
            alias=alias or f"{field}_{func.__name__}",
        )
        self._aggregations.append(agg)
        return self

    def sum(self, field: str, alias: Optional[str] = None) -> "DataGrouperAction":
        """Add sum aggregation."""
        return self.aggregate(field, sum, alias or f"{field}_sum")

    def count(self, field: str, alias: Optional[str] = None) -> "DataGrouperAction":
        """Add count aggregation."""
        return self.aggregate(field, len, alias or f"{field}_count")

    def avg(self, field: str, alias: Optional[str] = None) -> "DataGrouperAction":
        """Add average aggregation."""
        return self.aggregate(field, lambda x: sum(x) / len(x) if x else 0, alias or f"{field}_avg")

    def min(self, field: str, alias: Optional[str] = None) -> "DataGrouperAction":
        """Add min aggregation."""
        return self.aggregate(field, min, alias or f"{field}_min")

    def max(self, field: str, alias: Optional[str] = None) -> "DataGrouperAction":
        """Add max aggregation."""
        return self.aggregate(field, max, alias or f"{field}_max")

    def group(
        self,
        data: list[dict[str, Any]],
    ) -> GroupResult:
        """Group data and compute aggregations."""
        if not self._group_fields:
            return GroupResult(
                groups={"_all": data},
                aggregated={"_all": self._compute_aggregations(data)},
                group_keys=["_all"],
                group_count=1,
            )

        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for record in data:
            key = self._make_group_key(record)
            groups[key].append(record)

        aggregated: dict[str, dict[str, Any]] = {}
        for key, records in groups.items():
            agg_result = self._compute_aggregations(records)
            agg_result[self._group_fields[0]] = key
            aggregated[key] = agg_result

        return GroupResult(
            groups=dict(groups),
            aggregated=aggregated,
            group_keys=list(groups.keys()),
            group_count=len(groups),
        )

    def _make_group_key(self, record: dict[str, Any]) -> str:
        """Create group key from record."""
        values: list[str] = []

        for field_name in self._group_fields:
            value = record.get(field_name)
            values.append(str(value) if value is not None else "_null_")

        return "|".join(values)

    def _compute_aggregations(
        self,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Compute all aggregations for a group."""
        result: dict[str, Any] = {"_count": len(records)}

        for agg in self._aggregations:
            values = [
                record.get(agg.field)
                for record in records
                if agg.field in record and record[agg.field] is not None
            ]

            try:
                result[agg.alias] = agg.func(values)
            except Exception as e:
                logger.debug(f"Aggregation failed for {agg.alias}: {e}")
                result[agg.alias] = None

        return result

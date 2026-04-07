"""
Data Aggregator Action Module.

Aggregates data from multiple sources, merges results,
groups by dimensions, and computes rollups.

Example:
    >>> from data_aggregator_action import DataAggregator
    >>> agg = DataAggregator()
    >>> agg.add_source(scrape_result_1)
    >>> agg.add_source(scrape_result_2)
    >>> rolled = agg.rollup(group_by="category", metrics=["count", "sum"])
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    dimensions: dict[str, Any]
    metrics: dict[str, Any]
    source_count: int = 0


@dataclass
class AggregatedData:
    """Aggregated dataset."""
    results: list[AggregationResult]
    totals: dict[str, Any]
    grand_total: int


class DataAggregator:
    """Aggregate and rollup data from multiple sources."""

    def __init__(self):
        self._data: list[dict[str, Any]] = []
        self._sources: list[str] = []

    def add_source(self, data: Any, source_name: str = "") -> "DataAggregator":
        """
        Add a data source to aggregate.

        Args:
            data: List of dicts, single dict, or JSON string
            source_name: Optional source identifier

        Returns:
            Self for chaining
        """
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                return self

        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            return self

        self._data.extend(data)
        if source_name:
            self._sources.extend([source_name] * len(data))
        else:
            self._sources.extend(["unknown"] * len(data))

        return self

    def clear(self) -> None:
        """Clear all accumulated data."""
        self._data.clear()
        self._sources.clear()

    def rollup(
        self,
        group_by: str,
        metrics: list[str],
        filters: Optional[dict[str, Any]] = None,
    ) -> list[AggregationResult]:
        """
        Group data by dimension and compute metrics.

        Args:
            group_by: Field name to group by
            metrics: List of metric definitions
                      - "count" - count of records
                      - "sum:field" - sum of numeric field
                      - "avg:field" - average of numeric field
                      - "min:field" - minimum of field
                      - "max:field" - maximum of field
                      - "first:field" - first value
                      - "last:field" - last value
                      - "list:field" - list of unique values
            filters: Optional field -> value filters

        Returns:
            List of AggregationResult per group
        """
        filtered = self._apply_filters(self._data, filters or {})

        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in filtered:
            key = str(record.get(group_by, ""))
            groups[key].append(record)

        results: list[AggregationResult] = []
        for key, group_records in groups.items():
            dimensions = {group_by: key}
            metrics_result = self._compute_metrics(group_records, metrics)
            results.append(AggregationResult(
                dimensions=dimensions,
                metrics=metrics_result,
                source_count=len(group_records),
            ))

        return results

    def _apply_filters(
        self,
        data: list[dict[str, Any]],
        filters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        result = data
        for field, value in filters.items():
            if callable(value):
                result = [r for r in result if value(r.get(field))]
            else:
                result = [r for r in result if r.get(field) == value]
        return result

    def _compute_metrics(
        self,
        records: list[dict[str, Any]],
        metrics: list[str],
    ) -> dict[str, Any]:
        result = {}
        values_by_field: dict[str, list[Any]] = defaultdict(list)

        for metric in metrics:
            if metric == "count":
                result["count"] = len(records)
            elif metric.startswith(("sum:", "avg:", "min:", "max:", "first:", "last:", "list:")):
                op, _, field_name = metric.partition(":")
                for r in records:
                    values_by_field[field_name].append(r.get(field_name))

        for field_name, values in values_by_field.items():
            non_null = [v for v in values if v is not None]
            if not non_null:
                continue

            for metric in metrics:
                if metric.startswith(f"sum:{field_name}"):
                    try:
                        result[f"sum_{field_name}"] = sum(float(v) for v in non_null)
                    except (ValueError, TypeError):
                        result[f"sum_{field_name}"] = 0
                elif metric.startswith(f"avg:{field_name}"):
                    try:
                        result[f"avg_{field_name}"] = sum(float(v) for v in non_null) / len(non_null)
                    except (ValueError, TypeError):
                        result[f"avg_{field_name}"] = 0
                elif metric.startswith(f"min:{field_name}"):
                    try:
                        result[f"min_{field_name}"] = min(float(v) for v in non_null)
                    except (ValueError, TypeError):
                        result[f"min_{field_name}"] = non_null[0]
                elif metric.startswith(f"max:{field_name}"):
                    try:
                        result[f"max_{field_name}"] = max(float(v) for v in non_null)
                    except (ValueError, TypeError):
                        result[f"max_{field_name}"] = non_null[0]
                elif metric.startswith(f"first:{field_name}"):
                    result[f"first_{field_name}"] = non_null[0]
                elif metric.startswith(f"last:{field_name}"):
                    result[f"last_{field_name}"] = non_null[-1]
                elif metric.startswith(f"list:{field_name}"):
                    result[f"list_{field_name}"] = list(set(non_null))

        return result

    def pivot(
        self,
        index: str,
        columns: str,
        values: str,
        agg_func: str = "sum",
    ) -> list[dict[str, Any]]:
        """
        Pivot data from long to wide format.

        Args:
            index: Row dimension
            columns: Column dimension
            values: Value field
            agg_func: Aggregation function

        Returns:
            Pivoted data
        """
        pivot: dict[str, dict[str, Any]] = {}

        for record in self._data:
            idx_val = str(record.get(index, ""))
            col_val = str(record.get(columns, ""))
            val = record.get(values)

            if idx_val not in pivot:
                pivot[idx_val] = {index: idx_val}

            existing = pivot[idx_val].get(col_val)
            if existing is None:
                pivot[idx_val][col_val] = val
            else:
                try:
                    if agg_func == "sum":
                        pivot[idx_val][col_val] = float(existing) + float(val)
                    elif agg_func == "count":
                        pivot[idx_val][col_val] = existing + 1
                    elif agg_func == "avg":
                        pivot[idx_val][col_val] = (float(existing) + float(val)) / 2
                    elif agg_func == "min":
                        pivot[idx_val][col_val] = min(existing, val)
                    elif agg_func == "max":
                        pivot[idx_val][col_val] = max(existing, val)
                except (ValueError, TypeError):
                    pivot[idx_val][col_val] = val

        return list(pivot.values())

    def unpivot(
        self,
        id_cols: list[str],
        value_name: str = "variable",
        label_name: str = "value",
    ) -> list[dict[str, Any]]:
        """Unpivot from wide to long format."""
        result: list[dict[str, Any]] = []
        for record in self._data:
            base = {col: record.get(col) for col in id_cols}
            for key, val in record.items():
                if key not in id_cols:
                    new_record = dict(base)
                    new_record[value_name] = key
                    new_record[label_name] = val
                    result.append(new_record)
        return result

    def merge(
        self,
        other: "DataAggregator",
        on: str,
        how: str = "inner",
    ) -> "DataAggregator":
        """Merge another aggregator's data."""
        left = {r[on]: r for r in self._data if on in r}
        right = {r[on]: r for r in other._data if on in r}

        result = DataAggregator()
        if how == "inner":
            keys = set(left.keys()) & set(right.keys())
        elif how == "left":
            keys = set(left.keys())
        elif how == "right":
            keys = set(right.keys())
        else:
            keys = set(left.keys()) | set(right.keys())

        for key in keys:
            if key in left and key in right:
                merged = {**left[key], **right[key]}
                result._data.append(merged)
            elif key in left and how in ("left", "full"):
                result._data.append(dict(left[key]))
            elif key in right and how in ("right", "full"):
                result._data.append(dict(right[key]))

        return result

    def sample(self, n: int, random_seed: Optional[int] = None) -> list[dict[str, Any]]:
        """Random sample of aggregated data."""
        import random
        if random_seed is not None:
            random.seed(random_seed)
        return random.sample(self._data, min(n, len(self._data)))

    def to_list(self) -> list[dict[str, Any]]:
        """Return data as list."""
        return list(self._data)

    def summary(self) -> dict[str, Any]:
        """Get summary statistics."""
        return {
            "total_records": len(self._data),
            "sources": len(set(self._sources)),
            "fields": list(set(k for r in self._data for k in r.keys())),
        }


if __name__ == "__main__":
    agg = DataAggregator()
    agg.add_source([
        {"category": "A", "value": 10, "region": "North"},
        {"category": "A", "value": 20, "region": "South"},
        {"category": "B", "value": 15, "region": "North"},
        {"category": "B", "value": 25, "region": "South"},
    ])
    results = agg.rollup(group_by="category", metrics=["count", "sum:value", "avg:value"])
    for r in results:
        print(f"Category {r.dimensions}: {r.metrics}")

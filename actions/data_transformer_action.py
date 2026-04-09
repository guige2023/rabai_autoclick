"""
Data Transformation and Mapping Module.

Provides data transformation, mapping, and conversion utilities
for ETL pipelines and data processing workflows.

Author: AutoGen
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TransformType(Enum):
    MAP = auto()
    FILTER = auto()
    AGGREGATE = auto()
    JOIN = auto()
    SPLIT = auto()
    MERGE = auto()
    SORT = auto()


@dataclass
class FieldMapping:
    source_field: str
    target_field: str
    transform: Optional[Callable] = None
    default: Any = None
    required: bool = False


@dataclass
class TransformConfig:
    name: str
    mappings: List[FieldMapping] = field(default_factory=list)
    transforms: List[Callable] = field(default_factory=list)


class DataTransformer:
    """
    Transforms data records using field mappings and custom transforms.
    """

    def __init__(self):
        self._mappings: List[FieldMapping] = []
        self._pre_transforms: List[Callable] = []
        self._post_transforms: List[Callable] = []

    def map_field(
        self,
        source: str,
        target: str,
        transform: Optional[Callable] = None,
        default: Any = None,
        required: bool = False,
    ) -> "DataTransformer":
        self._mappings.append(
            FieldMapping(
                source_field=source,
                target_field=target,
                transform=transform,
                default=default,
                required=required,
            )
        )
        return self

    def add_transform(self, func: Callable, position: str = "pre") -> "DataTransformer":
        if position == "pre":
            self._pre_transforms.append(func)
        else:
            self._post_transforms.append(func)
        return self

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        result = {}

        for t in self._pre_transforms:
            record = t(record) or record

        for mapping in self._mappings:
            value = record.get(mapping.source_field, mapping.default)

            if value is None and mapping.required:
                raise ValueError(f"Required field '{mapping.source_field}' is missing")

            if mapping.transform and value is not None:
                try:
                    value = mapping.transform(value)
                except Exception as exc:
                    logger.warning("Transform error for '%s': %s", mapping.source_field, exc)

            result[mapping.target_field] = value

        for t in self._post_transforms:
            result = t(result) or result

        return result

    def transform_batch(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        return [self.transform_record(r) for r in records]


class PivotTable:
    """Creates pivot tables from data records."""

    def pivot(
        self,
        records: List[Dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        aggfunc: str = "sum",
    ) -> Dict[str, Any]:
        pivot_data: Dict[str, Dict[str, Any]] = defaultdict(dict)
        column_values: Set[str] = set()

        for record in records:
            idx_val = record.get(index)
            col_val = record.get(columns)
            val = record.get(values)

            if idx_val is not None and col_val is not None:
                column_values.add(col_val)
                pivot_data[idx_val][col_val] = val

        return {
            "index": index,
            "columns": columns,
            "values": values,
            "data": dict(pivot_data),
            "column_order": sorted(column_values),
        }


class DataAggregator:
    """Aggregates data with group-by and various aggregation functions."""

    AGG_FUNCTIONS = {
        "sum": lambda vals: sum(vals),
        "avg": lambda vals: sum(vals) / len(vals) if vals else 0,
        "min": lambda vals: min(vals) if vals else None,
        "max": lambda vals: max(vals) if vals else None,
        "count": lambda vals: len(vals),
        "first": lambda vals: vals[0] if vals else None,
        "last": lambda vals: vals[-1] if vals else None,
        "median": lambda vals: sorted(vals)[len(vals) // 2] if vals else None,
    }

    def aggregate(
        self,
        records: List[Dict[str, Any]],
        group_by: List[str],
        aggregations: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        groups: Dict[Tuple, Dict[str, List]] = defaultdict(lambda: defaultdict(list))

        for record in records:
            key = tuple(record.get(gb) for gb in group_by)
            for field_name, agg_func_name in aggregations.items():
                value = record.get(field_name)
                if value is not None:
                    groups[key][field_name].append(value)

        results = []
        for key, field_values in groups.items():
            result = dict(zip(group_by, key))
            for field_name, agg_func_name in aggregations.items():
                values = field_values.get(field_name, [])
                agg_func = self.AGG_FUNCTIONS.get(agg_func_name, self.AGG_FUNCTIONS["count"])
                try:
                    result[f"{field_name}_{agg_func_name}"] = agg_func(values)
                except Exception:
                    result[f"{field_name}_{agg_func_name}"] = None
            results.append(result)

        return results


class DataJoiner:
    """Joins multiple datasets."""

    @staticmethod
    def join(
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
        join_type: str = "inner",
    ) -> List[Dict[str, Any]]:
        right_index: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for r in right:
            right_index[r.get(right_key)].append(r)

        results = []
        for l in left:
            matched = right_index.get(l.get(left_key), [])
            if matched:
                for r in matched:
                    results.append({**l, **r})
            elif join_type == "left":
                results.append({**l, **{f"{k}_right": v for k, v in r.items()}} if not matched else {})

        return results


class SchemaConverter:
    """Converts data schemas between different formats."""

    def __init__(self):
        self._converters: Dict[Tuple[str, str], Callable] = {}

    def register_converter(
        self, from_format: str, to_format: str, converter: Callable
    ) -> None:
        self._converters[(from_format, to_format)] = converter

    def convert(
        self, data: Any, from_format: str, to_format: str
    ) -> Any:
        key = (from_format, to_format)
        if key in self._converters:
            return self._converters[key](data)

        if from_format == "json" and to_format == "dict":
            if isinstance(data, str):
                return json.loads(data)
            return data

        if from_format == "dict" and to_format == "json":
            return json.dumps(data, default=str)

        if from_format == "csv" and to_format == "dict":
            return self._csv_to_dicts(data)

        if from_format == "dict" and to_format == "csv":
            return self._dicts_to_csv(data)

        raise ValueError(f"No converter registered for {from_format} -> {to_format}")

    def _csv_to_dicts(self, csv_data: str) -> List[Dict[str, Any]]:
        lines = csv_data.strip().split("\n")
        if not lines:
            return []
        headers = lines[0].split(",")
        return [
            dict(zip(headers, line.split(",")))
            for line in lines[1:]
        ]

    def _dicts_to_csv(self, records: List[Dict[str, Any]]) -> str:
        if not records:
            return ""
        headers = list(records[0].keys())
        lines = [",".join(headers)]
        for record in records:
            lines.append(",".join(str(record.get(h, "")) for h in headers))
        return "\n".join(lines)

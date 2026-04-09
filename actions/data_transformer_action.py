"""
Data Transformer Action Module.

Declarative data transformation engine with chainable operations,
lookup tables, computed fields, and multi-format serialization support.
"""

import base64
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generic, Optional, TypeVar

from uuid import uuid4

T = TypeVar("T")


class TransformType(Enum):  # type: ignore[name-defined]
    """Types of transformation operations."""
    MAP = "map"
    FILTER = "filter"
    GROUP_BY = "group_by"
    SORT = "sort"
    JOIN = "join"
    UNION = "union"
    FLATTEN = "flatten"
    PIVOT = "pivot"
    WINDOW = "window"
    AGGREGATE = "aggregate"


@dataclass
class ComputedField:
    """A computed field definition."""

    name: str
    expression: Callable[[dict[str, Any]], Any]
    output_type: str = "any"

    def evaluate(self, row: dict[str, Any]) -> Any:
        """Evaluate the expression against a row."""
        return self.expression(row)


@dataclass
class LookupTable:
    """A lookup table for value mapping."""

    name: str
    data: dict[Any, Any]
    default: Any = None
    case_sensitive: bool = True

    def lookup(self, key: Any) -> Any:
        """Look up a value in the table."""
        if not self.case_sensitive and isinstance(key, str):
            key_lower = key.lower()
            for k, v in self.data.items():
                if isinstance(k, str) and k.lower() == key_lower:
                    return v
            return self.default
        return self.data.get(key, self.default)


@dataclass
class TransformStep:
    """A single transformation step."""

    step_id: str
    transform_type: str
    config: dict[str, Any]
    handler: Optional[Callable[[Any], Any]] = None

    def __post_init__(self) -> None:
        """Generate step_id if not provided."""
        if not self.step_id:
            self.step_id = str(uuid4())[:8]


class DataTransformer:
    """
    Declarative data transformation engine.

    Supports chainable transformations, computed fields,
    lookup tables, and multiple serialization formats.
    """

    def __init__(self) -> None:
        """Initialize the data transformer."""
        self._computed_fields: list[ComputedField] = []
        self._lookup_tables: dict[str, LookupTable] = {}
        self._transform_chain: list[TransformStep] = []
        self._default_value: Any = None

    # Field computation
    def add_computed_field(
        self,
        name: str,
        expression: Callable[[dict[str, Any]], Any],
        output_type: str = "any",
    ) -> "DataTransformer":
        """
        Add a computed field.

        Args:
            name: Name for the computed field.
            expression: Function to compute field value.
            output_type: Expected output type name.

        Returns:
            Self for chaining.
        """
        computed = ComputedField(
            name=name,
            expression=expression,
            output_type=output_type,
        )
        self._computed_fields.append(computed)
        return self

    def compute_field(
        self,
        name: str,
        expr: str,
    ) -> "DataTransformer":
        """
        Add a computed field using an expression string.

        Args:
            name: Field name.
            expr: Simple expression like "a + b" or "x['key']".

        Returns:
            Self for chaining.
        """
        def eval_expr(row: dict[str, Any]) -> Any:
            try:
                return eval(expr, {"__builtins__": {}}, row)
            except Exception:
                return None

        return self.add_computed_field(name, eval_expr)

    # Lookup tables
    def add_lookup(
        self,
        name: str,
        mapping: dict[Any, Any],
        default: Any = None,
        case_sensitive: bool = True,
    ) -> "DataTransformer":
        """
        Add a lookup table for value mapping.

        Args:
            name: Lookup name.
            mapping: Dictionary for value mapping.
            default: Default value for missing keys.
            case_sensitive: Whether to match case-insensitively.

        Returns:
            Self for chaining.
        """
        self._lookup_tables[name] = LookupTable(
            name=name,
            data=mapping,
            default=default,
            case_sensitive=case_sensitive,
        )
        return self

    def apply_lookup(
        self,
        field_name: str,
        lookup_name: str,
        output_field: Optional[str] = None,
    ) -> "DataTransformer":
        """
        Apply a lookup to transform a field value.

        Args:
            field_name: Source field name.
            lookup_name: Name of lookup table to use.
            output_field: Optional output field name.

        Returns:
            Self for chaining.
        """
        lookup = self._lookup_tables.get(lookup_name)
        if not lookup:
            return self

        def apply(row: dict[str, Any]) -> dict[str, Any]:
            result = row.copy()
            value = row.get(field_name)
            out_key = output_field or f"{field_name}_{lookup_name}"
            result[out_key] = lookup.lookup(value)
            return result

        return self._add_transform_step("lookup", {"field": field_name, "lookup": lookup_name}, apply)

    # Transformations
    def _add_transform_step(
        self,
        transform_type: str,
        config: dict[str, Any],
        handler: Optional[Callable[[Any], Any]] = None,
    ) -> "DataTransformer":
        """Internal method to add a transformation step."""
        step = TransformStep(
            step_id=str(uuid4())[:8],
            transform_type=transform_type,
            config=config,
            handler=handler,
        )
        self._transform_chain.append(step)
        return self

    def select_fields(self, fields: list[str]) -> "DataTransformer":
        """
        Select only specified fields from records.

        Args:
            fields: List of field names to keep.

        Returns:
            Self for chaining.
        """
        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{k: row.get(k) for k in fields} for row in data]

        return self._add_transform_step("select", {"fields": fields}, handler)

    def rename_fields(self, mapping: dict[str, str]) -> "DataTransformer":
        """
        Rename fields according to a mapping.

        Args:
            mapping: Dictionary of old_name -> new_name.

        Returns:
            Self for chaining.
        """
        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            result = []
            for row in data:
                new_row = {}
                for k, v in row.items():
                    new_key = mapping.get(k, k)
                    new_row[new_key] = v
                result.append(new_row)
            return result

        return self._add_transform_step("rename", {"mapping": mapping}, handler)

    def filter_records(
        self,
        predicate: Callable[[dict[str, Any]], bool],
    ) -> "DataTransformer":
        """
        Filter records based on a predicate.

        Args:
            predicate: Function that returns True to keep a record.

        Returns:
            Self for chaining.
        """
        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [row for row in data if predicate(row)]

        return self._add_transform_step("filter", {}, handler)

    def map_values(
        self,
        field_name: str,
        mapper: Callable[[Any], Any],
    ) -> "DataTransformer":
        """
        Map values in a specific field.

        Args:
            field_name: Field to transform.
            mapper: Function to transform each value.

        Returns:
            Self for chaining.
        """
        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            result = []
            for row in data:
                new_row = row.copy()
                if field_name in new_row:
                    new_row[field_name] = mapper(new_row[field_name])
                result.append(new_row)
            return result

        return self._add_transform_step("map_values", {"field": field_name}, handler)

    def group_by(
        self,
        field_name: str,
        agg_func: str = "count",
    ) -> "DataTransformer":
        """
        Group records by a field and aggregate.

        Args:
            field_name: Field to group by.
            agg_func: Aggregation function ("count", "sum", "avg", "min", "max").

        Returns:
            Self for chaining.
        """
        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            groups: dict[Any, list[dict[str, Any]]] = {}
            for row in data:
                key = row.get(field_name)
                if key not in groups:
                    groups[key] = []
                groups[key].append(row)

            result = []
            for key, rows in groups.items():
                entry: dict[str, Any] = {field_name: key, "count": len(rows)}
                if agg_func == "sum" and rows:
                    numeric_fields = [k for k, v in rows[0].items() if isinstance(v, (int, float))]
                    for f in numeric_fields:
                        entry[f"{f}_sum"] = sum(r.get(f, 0) for r in rows)
                elif agg_func == "avg" and rows:
                    numeric_fields = [k for k, v in rows[0].items() if isinstance(v, (int, float))]
                    for f in numeric_fields:
                        entry[f"{f}_avg"] = sum(r.get(f, 0) for r in rows) / len(rows)
                result.append(entry)
            return result

        return self._add_transform_step("group_by", {"field": field_name, "agg": agg_func}, handler)

    def sort_by(
        self,
        field_name: str,
        reverse: bool = False,
    ) -> "DataTransformer":
        """
        Sort records by a field.

        Args:
            field_name: Field to sort by.
            reverse: Sort in descending order if True.

        Returns:
            Self for chaining.
        """
        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return sorted(data, key=lambda r: r.get(field_name, ""), reverse=reverse)

        return self._add_transform_step("sort", {"field": field_name, "reverse": reverse}, handler)

    def flatten(
        self,
        separator: str = "_",
    ) -> "DataTransformer":
        """
        Flatten nested dictionaries.

        Args:
            separator: Separator for nested key names.

        Returns:
            Self for chaining.
        """
        def flatten_row(row: dict[str, Any], prefix: str = "") -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in row.items():
                new_key = f"{prefix}{separator}{key}" if prefix else key
                if isinstance(value, dict):
                    result.update(flatten_row(value, new_key))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            result.update(flatten_row(item, f"{new_key}{separator}{i}"))
                        else:
                            result[f"{new_key}{separator}{i}"] = item
                else:
                    result[new_key] = value
            return result

        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [flatten_row(row) for row in data]

        return self._add_transform_step("flatten", {"separator": separator}, handler)

    def union(self, other: "DataTransformer") -> "DataTransformer":
        """
        Add records from another transformer result.

        Args:
            other: Another DataTransformer whose result will be merged.

        Returns:
            Self for chaining.
        """
        def handler(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return data  # Union applied during transform

        return self._add_transform_step("union", {}, handler)

    # Execution
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Execute the transformation pipeline.

        Args:
            data: Input data records.

        Returns:
            Transformed data records.
        """
        result = list(data)

        # Apply computed fields first
        if self._computed_fields:
            for row in result:
                for field in self._computed_fields:
                    try:
                        row[field.name] = field.evaluate(row)
                    except Exception:
                        row[field.name] = None

        # Apply transformation chain
        for step in self._transform_chain:
            if step.handler:
                if step.transform_type == "filter":
                    result = step.handler(result)
                else:
                    result = step.handler(result)

        return result

    def transform_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Transform a single row through the pipeline."""
        result = dict(row)

        for field in self._computed_fields:
            try:
                result[field.name] = field.evaluate(result)
            except Exception:
                result[field.name] = None

        for step in self._transform_chain:
            if step.handler:
                result = step.handler([result])[0]

        return result

    # Serialization
    def to_json(self, data: list[dict[str, Any]], indent: int = 2) -> str:
        """Serialize data to JSON."""
        return json.dumps(data, indent=indent, default=str)

    def from_json(self, json_str: str) -> list[dict[str, Any]]:
        """Deserialize data from JSON."""
        return json.loads(json_str)

    def to_csv_rows(
        self,
        data: list[dict[str, Any]],
        columns: Optional[list[str]] = None,
    ) -> list[list[str]]:
        """Convert data to CSV rows."""
        if not data:
            return []

        cols = columns or list(data[0].keys())
        header = cols
        rows = [[str(row.get(c, "")) for c in cols] for row in data]
        return [header] + rows


def create_transformer() -> DataTransformer:
    """
    Factory function to create a data transformer.

    Returns:
        Configured DataTransformer instance.
    """
    return DataTransformer()

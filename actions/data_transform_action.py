"""
Data Transform Action Module.

Provides data transformation, mapping, validation, and conversion
capabilities for ETL pipelines and data processing workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import json
import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


class TransformType(Enum):
    """Data transformation types."""
    MAP = "map"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SORT = "sort"
    JOIN = "join"
    UNION = "union"
    GROUP = "group"
    PIVOT = "pivot"


@dataclass
class FieldMapping:
    """Mapping definition for field transformation."""
    source_field: str
    target_field: str
    transform_func: Optional[Callable] = None
    default_value: Any = None
    required: bool = False


@dataclass
class SchemaDefinition:
    """Schema definition for data validation."""
    name: str
    fields: Dict[str, "FieldDefinition"]
    strict: bool = False

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate data against schema."""
        errors = []

        for field_name, field_def in self.fields.items():
            if field_name not in data:
                if field_def.required:
                    errors.append(f"Missing required field: {field_name}")
                continue

            value = data[field_name]
            if not field_def.validate_value(value):
                errors.append(f"Invalid value for field {field_name}: {value}")

        if self.strict:
            extra_fields = set(data.keys()) - set(self.fields.keys())
            if extra_fields:
                errors.append(f"Extra fields not allowed: {extra_fields}")

        return len(errors) == 0, errors


@dataclass
class FieldDefinition:
    """Field definition for schema."""
    field_type: type
    required: bool = False
    default: Any = None
    validator: Optional[Callable[[Any], bool]] = None
    choices: Optional[List[Any]] = None
    min_value: Optional[Number] = None
    max_value: Optional[Number] = None
    pattern: Optional[str] = None
    items: Optional["FieldDefinition"] = None

    def validate_value(self, value: Any) -> bool:
        """Validate a value against this field definition."""
        if value is None:
            return not self.required

        if self.validator:
            return self.validator(value)

        if self.choices and value not in self.choices:
            return False

        if isinstance(value, (int, float)):
            if self.min_value is not None and value < self.min_value:
                return False
            if self.max_value is not None and value > self.max_value:
                return False

        if self.pattern and isinstance(value, str):
            if not re.match(self.pattern, value):
                return False

        return True


@dataclass
class TransformResult:
    """Result of a transformation operation."""
    success: bool
    data: Any
    errors: List[str] = field(default_factory=list)
    transformed_count: int = 0
    skipped_count: int = 0
    execution_time: float = 0.0


@dataclass
class JoinConfig:
    """Configuration for join operations."""
    left_key: str
    right_key: str
    join_type: str = "inner"
    left_suffix: str = "left_"
    right_suffix: str = "right_"


class DataMapper:
    """Maps and transforms data between different schemas."""

    def __init__(self):
        self.mappings: List[FieldMapping] = []

    def add_mapping(
        self,
        source: str,
        target: str,
        transform: Optional[Callable] = None,
        default: Any = None,
        required: bool = False
    ):
        """Add field mapping."""
        self.mappings.append(FieldMapping(
            source_field=source,
            target_field=target,
            transform_func=transform,
            default_value=default,
            required=required
        ))

    def map(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply mappings to data."""
        result = {}

        for mapping in self.mappings:
            value = data.get(mapping.source_field, mapping.default_value)

            if value is None and mapping.required:
                continue

            if mapping.transform_func:
                try:
                    value = mapping.transform_func(value)
                except Exception:
                    continue

            result[mapping.target_field] = value

        return result

    def map_batch(
        self,
        data_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply mappings to batch of data."""
        return [self.map(data) for data in data_list]


class DataFilter:
    """Filters data based on conditions."""

    def __init__(self):
        self.conditions: List[Tuple[str, str, Any]] = []

    def add_condition(
        self,
        field: str,
        operator: str,
        value: Any
    ):
        """Add filter condition."""
        self.conditions.append((field, operator, value))

    def matches(self, data: Dict[str, Any]) -> bool:
        """Check if data matches all conditions."""
        for field, operator, expected in self.conditions:
            value = self._get_nested(data, field)

            if operator == "==":
                if value != expected:
                    return False
            elif operator == "!=":
                if value == expected:
                    return False
            elif operator == ">":
                if not (value and value > expected):
                    return False
            elif operator == "<":
                if not (value and value < expected):
                    return False
            elif operator == ">=":
                if not (value and value >= expected):
                    return False
            elif operator == "<=":
                if not (value and value <= expected):
                    return False
            elif operator == "in":
                if value not in expected:
                    return False
            elif operator == "not in":
                if value in expected:
                    return False
            elif operator == "contains":
                if expected not in str(value):
                    return False
            elif operator == "startswith":
                if not str(value).startswith(str(expected)):
                    return False
            elif operator == "endswith":
                if not str(value).endswith(str(expected)):
                    return False
            elif operator == "exists":
                if value is None:
                    return False
            elif operator == "regex":
                if not re.search(expected, str(value)):
                    return False

        return True

    def _get_nested(self, data: Dict[str, Any], field: str) -> Any:
        """Get nested field value using dot notation."""
        keys = field.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def filter(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter list of data."""
        return [d for d in data_list if self.matches(d)]


class DataAggregator:
    """Aggregates data with various operations."""

    def __init__(self):
        self.group_by: List[str] = []
        self.aggregations: List[Tuple[str, str, str]] = []

    def add_group_by(self, field: str):
        """Add field to group by."""
        self.group_by.append(field)

    def add_aggregation(
        self,
        field: str,
        operation: str,
        alias: Optional[str] = None
    ):
        """Add aggregation operation."""
        if alias is None:
            alias = f"{operation}_{field}"
        self.aggregations.append((field, operation, alias))

    def aggregate(
        self,
        data_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Perform aggregation on data."""
        if not self.group_by:
            return self._aggregate_all(data_list)

        groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)

        for item in data_list:
            key = tuple(self._get_nested(item, f) for f in self.group_by)
            groups[key].append(item)

        results = []
        for key, items in groups.items():
            result = dict(zip(self.group_by, key))
            result.update(self._compute_aggregations(items))
            results.append(result)

        return results

    def _aggregate_all(
        self,
        data_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Aggregate without grouping."""
        result = self._compute_aggregations(data_list)
        return [result]

    def _compute_aggregations(
        self,
        items: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Compute aggregation functions on items."""
        result = {}

        for field, operation, alias in self.aggregations:
            values = [self._get_nested(item, field) for item in items]
            values = [v for v in values if v is not None]

            if not values:
                result[alias] = None
                continue

            if operation == "sum":
                result[alias] = sum(values)
            elif operation == "avg":
                result[alias] = sum(values) / len(values)
            elif operation == "min":
                result[alias] = min(values)
            elif operation == "max":
                result[alias] = max(values)
            elif operation == "count":
                result[alias] = len(values)
            elif operation == "count_distinct":
                result[alias] = len(set(values))
            elif operation == "first":
                result[alias] = values[0]
            elif operation == "last":
                result[alias] = values[-1]
            elif operation == "list":
                result[alias] = values

        return result

    def _get_nested(self, data: Dict[str, Any], field: str) -> Any:
        """Get nested field value."""
        keys = field.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


class DataJoiner:
    """Joins multiple datasets."""

    def __init__(self):
        self.datasets: List[Tuple[List[Dict[str, Any]], str]] = []
        self.join_configs: List[JoinConfig] = []

    def add_dataset(
        self,
        data: List[Dict[str, Any]],
        alias: str
    ):
        """Add dataset with alias."""
        self.datasets.append((data, alias))

    def join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        config: JoinConfig
    ) -> List[Dict[str, Any]]:
        """Perform join operation."""
        index: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)

        for item in right_data:
            key = self._get_nested(item, config.right_key)
            index[key].append(item)

        results = []
        for left_item in left_data:
            left_key = self._get_nested(left_item, config.left_key)
            right_items = index.get(left_key, [])

            if not right_items and config.join_type == "left":
                merged = {}
                merged.update({f"{config.left_suffix}{k}": v for k, v in left_item.items()})
                results.append(merged)
            elif not right_items and config.join_type != "left":
                continue

            for right_item in right_items:
                merged = {}
                merged.update({f"{config.left_suffix}{k}": v for k, v in left_item.items()})
                merged.update({f"{config.right_suffix}{k}": v for k, v in right_item.items()})
                results.append(merged)

        if config.join_type == "outer":
            right_only = []
            left_keys = {self._get_nested(item, config.left_key) for item in left_data}
            for right_item in right_data:
                right_key = self._get_nested(right_item, config.right_key)
                if right_key not in left_keys:
                    merged = {}
                    merged.update({f"{config.right_suffix}{k}": v for k, v in right_item.items()})
                    results.append(merged)

        return results

    def _get_nested(self, data: Dict[str, Any], field: str) -> Any:
        """Get nested field value."""
        keys = field.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


class DataTransformer:
    """Main data transformation orchestrator."""

    def __init__(self):
        self.mappers: List[DataMapper] = []
        self.filters: List[DataFilter] = []
        self.aggregators: List[DataAggregator] = []
        self.transform_functions: List[Callable] = []

    def add_mapper(self, mapper: DataMapper):
        """Add data mapper."""
        self.mappers.append(mapper)

    def add_filter(self, filter: DataFilter):
        """Add data filter."""
        self.filters.append(filter)

    def add_transform(self, func: Callable):
        """Add transformation function."""
        self.transform_functions.append(func)

    def transform(
        self,
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
        continue_on_error: bool = True
    ) -> TransformResult:
        """Execute transformation pipeline."""
        start_time = datetime.now()
        errors = []

        if isinstance(data, dict):
            data = [data]

        transformed = data

        for mapper in self.mappers:
            transformed = mapper.map_batch(transformed)

        for filter in self.filters:
            transformed = filter.filter(transformed)

        for func in self.transform_functions:
            try:
                transformed = [func(item) for item in transformed]
            except Exception as e:
                if continue_on_error:
                    errors.append(str(e))
                else:
                    return TransformResult(
                        success=False,
                        data=[],
                        errors=[str(e)],
                        execution_time=(datetime.now() - start_time).total_seconds()
                    )

        return TransformResult(
            success=True,
            data=transformed,
            errors=errors,
            transformed_count=len(transformed),
            execution_time=(datetime.now() - start_time).total_seconds()
        )


def main():
    """Demonstrate data transformation."""
    mapper = DataMapper()
    mapper.add_mapping("id", "user_id", required=True)
    mapper.add_mapping("name", "full_name", str.title)
    mapper.add_mapping("email", "email_address")

    data = [
        {"id": 1, "name": "john doe", "email": "john@example.com"},
        {"id": 2, "name": "jane smith", "email": "jane@example.com"}
    ]

    mapped = mapper.map_batch(data)
    print(f"Mapped: {mapped}")

    filter = DataFilter()
    filter.add_condition("age", ">=", 18)
    filter.add_condition("status", "==", "active")

    filtered = filter.filter(data)
    print(f"Filtered: {filtered}")


if __name__ == "__main__":
    main()

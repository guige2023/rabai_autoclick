"""
Data Transform Action Module

Data transformation utilities including field mapping, type conversion,
aggregation, normalization, and custom transformations with schema validation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TransformType(Enum):
    """Types of data transformations."""

    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    FLATTEN = "flatten"
    PIVOT = "pivot"
    GROUP = "group"
    SORT = "sort"
    NORMALIZE = "normalize"
    VALIDATE = "validate"
    CAST = "cast"


@dataclass
class FieldMapping:
    """Mapping from source field to target field."""

    source: str
    target: str
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None
    required: bool = False


@dataclass
class TransformSchema:
    """Schema definition for data validation."""

    fields: Dict[str, "FieldSchema"] = field(default_factory=dict)
    required_fields: Set[str] = field(default_factory=set)
    allow_extra_fields: bool = True


@dataclass
class FieldSchema:
    """Schema for a single field."""

    field_type: type = str
    required: bool = False
    default: Any = None
    validator: Optional[Callable[[Any], bool]] = None
    transformer: Optional[Callable[[Any], Any]] = None
    choices: Optional[Set[Any]] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None


@dataclass
class TransformResult:
    """Result of a transformation operation."""

    success: bool
    data: Any = None
    errors: List[Dict[str, Any]] = field(default_factory=list)
    transformed_count: int = 0
    skipped_count: int = 0


class DataMapper:
    """
    Maps fields from source to target using field mappings.

    Supports nested fields, computed fields, and default values.
    """

    def __init__(self, mappings: Optional[List[FieldMapping]] = None):
        self.mappings = mappings or []

    def add_mapping(
        self,
        source: str,
        target: str,
        transform: Optional[Callable[[Any], Any]] = None,
        default: Any = None,
        required: bool = False,
    ) -> "DataMapper":
        """Add a field mapping."""
        self.mappings.append(FieldMapping(
            source=source,
            target=target,
            transform=transform,
            default=default,
            required=required,
        ))
        return self

    def map(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply mappings to data."""
        result = {}

        for mapping in self.mappings:
            value = data.get(mapping.source)

            if value is None:
                if mapping.required:
                    raise ValueError(f"Required field '{mapping.source}' is missing")
                value = mapping.default

            if mapping.transform and value is not None:
                value = mapping.transform(value)

            result[mapping.target] = value

        return result

    def map_many(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply mappings to multiple records."""
        return [self.map(record) for record in data]


class DataNormalizer:
    """
    Normalizes data values to standard formats.

    Supports string normalization, number formatting, date parsing,
    and custom normalization rules.
    """

    @staticmethod
    def normalize_string(value: Any, lowercase: bool = True, trim: bool = True) -> str:
        """Normalize a string value."""
        if value is None:
            return ""
        s = str(value)
        if trim:
            s = s.strip()
        if lowercase:
            s = s.lower()
        return s

    @staticmethod
    def normalize_number(value: Any, decimal_places: int = 2) -> Optional[float]:
        """Normalize a numeric value."""
        if value is None:
            return None
        try:
            return round(float(value), decimal_places)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_boolean(value: Any) -> bool:
        """Normalize a boolean value."""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "yes", "1", "on")
        return bool(value)

    @staticmethod
    def normalize_date(
        value: Any,
        input_formats: Optional[List[str]] = None,
        output_format: str = "%Y-%m-%d",
    ) -> Optional[str]:
        """Normalize a date value."""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value.strftime(output_format)

        if isinstance(value, str):
            formats = input_formats or [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S",
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.strftime(output_format)
                except ValueError:
                    continue

        return None

    @staticmethod
    def normalize_email(value: Any) -> Optional[str]:
        """Normalize an email address."""
        if value is None:
            return None
        email = DataNormalizer.normalize_string(value)
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if re.match(pattern, email):
            return email
        return None

    @staticmethod
    def normalize_phone(
        value: Any,
        country_code: str = "+1",
    ) -> Optional[str]:
        """Normalize a phone number."""
        if value is None:
            return None
        digits = re.sub(r"\D", "", str(value))
        if len(digits) == 10:
            return f"{country_code}{digits}"
        if len(digits) == 11 and digits[0] == "1":
            return f"+{digits}"
        return None


class DataAggregator:
    """
    Aggregates data using various operations.

    Supports sum, average, count, min, max, and custom aggregations.
    """

    @staticmethod
    def sum_field(data: List[Dict[str, Any]], field: str) -> Optional[float]:
        """Sum values of a field."""
        values = [d.get(field, 0) for d in data if d.get(field) is not None]
        return sum(values) if values else None

    @staticmethod
    def avg_field(data: List[Dict[str, Any]], field: str) -> Optional[float]:
        """Average values of a field."""
        values = [d.get(field, 0) for d in data if d.get(field) is not None]
        return sum(values) / len(values) if values else None

    @staticmethod
    def count_field(data: List[Dict[str, Any]], field: str) -> int:
        """Count non-null values of a field."""
        return sum(1 for d in data if d.get(field) is not None)

    @staticmethod
    def min_field(data: List[Dict[str, Any]], field: str) -> Optional[Any]:
        """Get minimum value of a field."""
        values = [d.get(field) for d in data if d.get(field) is not None]
        return min(values) if values else None

    @staticmethod
    def max_field(data: List[Dict[str, Any]], field: str) -> Optional[Any]:
        """Get maximum value of a field."""
        values = [d.get(field) for d in data if d.get(field) is not None]
        return max(values) if values else None

    @staticmethod
    def group_by(
        data: List[Dict[str, Any]],
        group_field: str,
        agg_field: Optional[str] = None,
        agg_func: str = "sum",
    ) -> Dict[Any, Any]:
        """Group data by field and aggregate."""
        groups: Dict[Any, List[Dict]] = {}
        for item in data:
            key = item.get(group_field)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        if agg_field is None:
            return {k: len(v) for k, v in groups.items()}

        result = {}
        for key, items in groups.items():
            values = [d.get(agg_field, 0) for d in items if d.get(agg_field) is not None]
            if agg_func == "sum":
                result[key] = sum(values)
            elif agg_func == "avg":
                result[key] = sum(values) / len(values) if values else 0
            elif agg_func == "count":
                result[key] = len(values)
            elif agg_func == "min":
                result[key] = min(values) if values else None
            elif agg_func == "max":
                result[key] = max(values) if values else None

        return result


class DataValidator:
    """
    Validates data against schemas.

    Supports field types, required fields, choices, ranges, and custom validators.
    """

    def __init__(self, schema: Optional[TransformSchema] = None):
        self.schema = schema

    def validate(self, data: Dict[str, Any]) -> TransformResult:
        """Validate data against schema."""
        result = TransformResult(success=True)

        if not self.schema:
            result.data = data
            return result

        # Check required fields
        for field_name in self.schema.required_fields:
            if field_name not in data or data[field_name] is None:
                result.errors.append({
                    "type": "required",
                    "field": field_name,
                    "message": f"Required field '{field_name}' is missing",
                })
                result.success = False

        # Validate each field
        for field_name, field_schema in self.schema.fields.items():
            value = data.get(field_name)

            if value is None:
                if field_schema.required:
                    continue
                continue

            # Type check
            if field_schema.field_type and not isinstance(value, field_schema.field_type):
                try:
                    value = field_schema.field_type(value)
                    data[field_name] = value
                except (ValueError, TypeError):
                    result.errors.append({
                        "type": "type",
                        "field": field_name,
                        "message": f"Field '{field_name}' has invalid type",
                    })
                    result.success = False
                    continue

            # Choices check
            if field_schema.choices and value not in field_schema.choices:
                result.errors.append({
                    "type": "choices",
                    "field": field_name,
                    "message": f"Field '{field_name}' value not in allowed choices",
                })
                result.success = False

            # Range check
            if field_schema.min_value is not None and value < field_schema.min_value:
                result.errors.append({
                    "type": "range",
                    "field": field_name,
                    "message": f"Field '{field_name}' below minimum value",
                })
                result.success = False

            if field_schema.max_value is not None and value > field_schema.max_value:
                result.errors.append({
                    "type": "range",
                    "field": field_name,
                    "message": f"Field '{field_name}' above maximum value",
                })
                result.success = False

            # Custom validator
            if field_schema.validator and not field_schema.validator(value):
                result.errors.append({
                    "type": "custom",
                    "field": field_name,
                    "message": f"Field '{field_name}' failed custom validation",
                })
                result.success = False

            # Apply transformer
            if field_schema.transformer and result.success:
                try:
                    data[field_name] = field_schema.transformer(value)
                except Exception as e:
                    result.errors.append({
                        "type": "transform",
                        "field": field_name,
                        "message": f"Field '{field_name}' transformation failed: {e}",
                    })
                    result.success = False

        result.data = data if result.success else None
        return result


class DataTransformAction:
    """
    Main action class for data transformations.

    Combines mapping, normalization, validation, and aggregation
    into a unified transformation pipeline.
    """

    def __init__(
        self,
        schema: Optional[TransformSchema] = None,
        mappings: Optional[List[FieldMapping]] = None,
    ):
        self.schema = schema
        self.mapper = DataMapper(mappings)
        self.validator = DataValidator(schema)
        self._stats = {
            "transformed": 0,
            "validated": 0,
            "errors": 0,
        }

    def add_mapping(
        self,
        source: str,
        target: str,
        transform: Optional[Callable[[Any], Any]] = None,
        default: Any = None,
        required: bool = False,
    ) -> "DataTransformAction":
        """Add a field mapping."""
        self.mapper.add_mapping(source, target, transform, default, required)
        return self

    def transform(self, data: Dict[str, Any]) -> TransformResult:
        """Transform a single record."""
        try:
            # Map fields
            mapped = self.mapper.map(data)

            # Validate
            validation_result = self.validator.validate(mapped)

            if validation_result.success:
                self._stats["transformed"] += 1
                self._stats["validated"] += 1
            else:
                self._stats["errors"] += 1

            return TransformResult(
                success=validation_result.success,
                data=validation_result.data,
                errors=validation_result.errors,
                transformed_count=1,
            )

        except Exception as e:
            self._stats["errors"] += 1
            return TransformResult(
                success=False,
                errors=[{"type": "transform", "message": str(e)}],
            )

    def transform_many(self, data: List[Dict[str, Any]]) -> List[TransformResult]:
        """Transform multiple records."""
        return [self.transform(record) for record in data]

    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_by: str,
        agg_field: str,
        agg_func: str = "sum",
    ) -> Dict[Any, Any]:
        """Aggregate data."""
        return DataAggregator.group_by(data, group_by, agg_field, agg_func)

    def get_stats(self) -> Dict[str, int]:
        """Get transformation statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = {"transformed": 0, "validated": 0, "errors": 0}


def demo_transform():
    """Demonstrate transformation usage."""
    data = [
        {"name": " Alice ", "age": "30", "score": 85.5, "active": "true"},
        {"name": "Bob", "age": "25", "score": 92.0, "active": "yes"},
        {"name": "Charlie", "age": "35", "score": 78.0, "active": "no"},
    ]

    action = DataTransformAction()
    action.add_mapping("name", "full_name", lambda x: x.strip().title())
    action.add_mapping("age", "age_years", int)
    action.add_mapping("score", "grade", lambda x: round(x))
    action.add_mapping("active", "is_active", lambda x: x.lower() in ("true", "yes"))

    for record in data:
        result = action.transform(record)
        if result.success:
            print(f"Transformed: {result.data}")


if __name__ == "__main__":
    demo_transform()

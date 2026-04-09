"""Data Transform v2 with schema validation and chaining.

This module provides powerful data transformation with:
- Schema-based validation
- Chained transformations
- Type coercion and conversion
- Field mapping and renaming
- Conditional transformations
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TransformType(Enum):
    """Types of transform operations."""

    MAP = "map"
    FILTER = "filter"
    FLATTEN = "flatten"
    GROUP = "group"
    SORT = "sort"
    LIMIT = "limit"
    DEDUP = "deduplicate"
    VALIDATE = "validate"
    COERCE = "coerce"
    ENRICH = "enrich"


@dataclass
class SchemaField:
    """Schema definition for a single field."""

    name: str
    field_type: type | str | None = None
    required: bool = True
    default: Any = None
    validator: Callable[[Any], bool] | None = None
    transformer: Callable[[Any], Any] | None = None
    nullable: bool = False


@dataclass
class Schema:
    """Schema definition for data validation."""

    name: str
    fields: list[SchemaField] = field(default_factory=list)
    strict: bool = False  # Reject unknown fields if True

    def validate(self, data: dict) -> tuple[bool, list[str]]:
        """Validate data against schema.

        Args:
            data: Data to validate

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        for field_def in self.fields:
            value = data.get(field_def.name)

            # Check required
            if value is None:
                if field_def.required:
                    errors.append(f"Missing required field: {field_def.name}")
                if field_def.default is not None:
                    data[field_def.name] = field_def.default
                continue

            # Check nullable
            if value is None and not field_def.nullable:
                errors.append(f"Field cannot be null: {field_def.name}")
                continue

            # Check type
            if field_def.field_type and value is not None:
                expected = field_def.field_type
                if isinstance(expected, str):
                    expected = self._parse_type(expected)

                if not isinstance(value, expected):
                    # Try coercion
                    try:
                        data[field_def.name] = self._coerce_type(value, expected)
                    except (ValueError, TypeError):
                        errors.append(
                            f"Invalid type for {field_def.name}: "
                            f"expected {expected.__name__}, got {type(value).__name__}"
                        )
                        continue

            # Check validator
            if field_def.validator and value is not None:
                try:
                    if not field_def.validator(value):
                        errors.append(f"Validation failed for {field_def.name}")
                except Exception as e:
                    errors.append(f"Validator error for {field_def.name}: {e}")

            # Apply transformer
            if field_def.transformer and value is not None:
                try:
                    data[field_def.name] = field_def.transformer(value)
                except Exception as e:
                    errors.append(f"Transform error for {field_def.name}: {e}")

        # Check unknown fields
        if self.strict:
            known_fields = {f.name for f in self.fields}
            unknown = set(data.keys()) - known_fields
            if unknown:
                errors.append(f"Unknown fields: {unknown}")

        return len(errors) == 0, errors

    def _parse_type(self, type_str: str) -> type:
        """Parse type string to type."""
        type_map = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "list": list,
            "dict": dict,
        }
        return type_map.get(type_str, str)

    def _coerce_type(self, value: Any, target_type: type) -> Any:
        """Coerce value to target type."""
        if target_type == int:
            return int(value)
        elif target_type == float:
            return float(value)
        elif target_type == str:
            return str(value)
        elif target_type == bool:
            return bool(value)
        elif target_type == list:
            if isinstance(value, (list, tuple, set)):
                return list(value)
            return [value]
        elif target_type == dict:
            if isinstance(value, dict):
                return value
            return {"value": value}
        return value


@dataclass
class TransformStep:
    """A single transformation step."""

    name: str
    transform_type: TransformType
    func: Callable[[Any], Any]
    condition: Callable[[Any], bool] | None = None
    priority: int = 0


class DataTransformV2:
    """Advanced data transformation engine with schema validation."""

    def __init__(self, schema: Schema | None = None):
        """Initialize data transformer.

        Args:
            schema: Optional schema for validation
        """
        self.schema = schema
        self._steps: list[TransformStep] = []
        self._field_mappings: dict[str, str] = {}
        self._default_values: dict[str, Any] = {}

    def add_step(
        self,
        name: str,
        func: Callable[[Any], Any],
        transform_type: TransformType = TransformType.MAP,
        condition: Callable[[Any], bool] | None = None,
        priority: int = 0,
    ) -> "DataTransformV2":
        """Add a transformation step.

        Args:
            name: Step name
            func: Transform function
            transform_type: Type of transformation
            condition: Optional condition to apply step
            priority: Execution priority (lower = earlier)

        Returns:
            Self for chaining
        """
        step = TransformStep(
            name=name,
            transform_type=transform_type,
            func=func,
            condition=condition,
            priority=priority,
        )
        self._steps.append(step)
        self._steps.sort(key=lambda s: s.priority)
        return self

    def map_field(self, from_name: str, to_name: str) -> "DataTransformV2":
        """Add field mapping.

        Args:
            from_name: Source field name
            to_name: Destination field name

        Returns:
            Self for chaining
        """
        self._field_mappings[from_name] = to_name
        return self

    def set_default(self, field_name: str, default_value: Any) -> "DataTransformV2":
        """Set default value for a field.

        Args:
            field_name: Field name
            default_value: Default value

        Returns:
            Self for chaining
        """
        self._default_values[field_name] = default_value
        return self

    def transform(self, data: Any) -> tuple[bool, Any, list[str]]:
        """Transform data through all steps.

        Args:
            data: Input data

        Returns:
            Tuple of (success, transformed_data, errors)
        """
        errors = []

        # Apply field mappings
        if isinstance(data, dict):
            data = self._apply_mappings(data)

        # Apply default values
        if isinstance(data, dict):
            data = self._apply_defaults(data)

        # Validate against schema
        if self.schema:
            valid, schema_errors = self.schema.validate(data)
            if not valid:
                errors.extend(schema_errors)
                if self.schema.strict:
                    return False, data, errors

        # Apply transformation steps
        for step in self._steps:
            try:
                if step.condition and not step.condition(data):
                    continue

                if step.transform_type == TransformType.MAP:
                    data = self._apply_map(data, step.func)
                elif step.transform_type == TransformType.FILTER:
                    data = self._apply_filter(data, step.func)
                elif step.transform_type == TransformType.FLATTEN:
                    data = self._apply_flatten(data)
                elif step.transform_type == TransformType.SORT:
                    data = self._apply_sort(data, step.func)
                elif step.transform_type == TransformType.LIMIT:
                    data = self._apply_limit(data, step.func)
                elif step.transform_type == TransformType.DEDUP:
                    data = self._apply_dedup(data, step.func)
                elif step.transform_type == TransformType.COERCE:
                    data = self._apply_coerce(data, step.func)

            except Exception as e:
                errors.append(f"Transform error in {step.name}: {e}")
                logger.warning(f"Transform error in {step.name}: {e}")

        return len(errors) == 0, data, errors

    def transform_batch(
        self,
        records: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """Transform multiple records.

        Args:
            records: List of records to transform

        Returns:
            Tuple of (successful_records, failed_records)
        """
        successful = []
        failed = []

        for record in records:
            success, transformed, errors = self.transform(record)
            if success:
                successful.append(transformed)
            else:
                failed.append({"record": record, "errors": errors})

        return successful, failed

    def _apply_mappings(self, data: dict) -> dict:
        """Apply field mappings."""
        result = {}
        for key, value in data.items():
            new_key = self._field_mappings.get(key, key)
            result[new_key] = value
        return result

    def _apply_defaults(self, data: dict) -> dict:
        """Apply default values."""
        for field_name, default_value in self._default_values.items():
            if field_name not in data:
                data[field_name] = default_value
        return data

    def _apply_map(self, data: Any, func: Callable[[Any], Any]) -> Any:
        """Apply map transformation."""
        if isinstance(data, list):
            return [func(item) for item in data]
        elif isinstance(data, dict):
            return {k: func(v) for k, v in data.items()}
        return func(data)

    def _apply_filter(self, data: Any, func: Callable[[Any], bool]) -> Any:
        """Apply filter transformation."""
        if isinstance(data, list):
            return [item for item in data if func(item)]
        elif isinstance(data, dict):
            return {k: v for k, v in data.items() if func(v)}
        return data

    def _apply_flatten(self, data: Any, depth: int = 1) -> Any:
        """Flatten nested structure."""
        if depth <= 0:
            return data

        if isinstance(data, list):
            result = []
            for item in data:
                flattened = self._apply_flatten(item, depth - 1)
                if isinstance(flattened, list):
                    result.extend(flattened)
                else:
                    result.append(flattened)
            return result
        elif isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if isinstance(value, (list, dict)):
                    flattened = self._apply_flatten(value, depth - 1)
                    if isinstance(flattened, dict):
                        result.update(flattened)
                    else:
                        result[key] = flattened
                else:
                    result[key] = value
            return result

        return data

    def _apply_sort(self, data: Any, key_func: Callable[[Any], Any]) -> Any:
        """Apply sort transformation."""
        if isinstance(data, list):
            return sorted(data, key=key_func)
        return data

    def _apply_limit(self, data: Any, limit: int) -> Any:
        """Apply limit transformation."""
        if isinstance(data, list):
            return data[:limit]
        return data

    def _apply_dedup(
        self,
        data: Any,
        key_func: Callable[[Any], Any] | None = None,
    ) -> Any:
        """Apply deduplication."""
        if isinstance(data, list):
            if key_func:
                seen = set()
                result = []
                for item in data:
                    key = key_func(item)
                    if key not in seen:
                        seen.add(key)
                        result.append(item)
                return result
            else:
                seen = []
                result = []
                for item in data:
                    if item not in seen:
                        seen.append(item)
                        result.append(item)
                return result
        return data

    def _apply_coerce(
        self,
        data: dict,
        type_mapping: Callable[[str, Any], tuple[type, Any]],
    ) -> dict:
        """Apply type coercion."""
        result = {}
        for key, value in data.items():
            target_type, coerced = type_mapping(key, value)
            try:
                if target_type == int:
                    result[key] = int(coerced)
                elif target_type == float:
                    result[key] = float(coerced)
                elif target_type == str:
                    result[key] = str(coerced)
                elif target_type == bool:
                    result[key] = bool(coerced)
                else:
                    result[key] = coerced
            except (ValueError, TypeError):
                result[key] = value
        return result


class BuiltinTransforms:
    """Built-in transformation functions."""

    @staticmethod
    def to_upper(value: str) -> str:
        """Convert string to uppercase."""
        return str(value).upper()

    @staticmethod
    def to_lower(value: str) -> str:
        """Convert string to lowercase."""
        return str(value).lower()

    @staticmethod
    def trim(value: str) -> str:
        """Trim whitespace from string."""
        return str(value).strip()

    @staticmethod
    def truncate(value: str, max_length: int) -> str:
        """Truncate string to max length."""
        s = str(value)
        return s[:max_length] + ("..." if len(s) > max_length else "")

    @staticmethod
    def parse_json(value: str) -> dict | list:
        """Parse JSON string."""
        import json
        return json.loads(value)

    @staticmethod
    def to_json(value: Any) -> str:
        """Convert to JSON string."""
        import json
        return json.dumps(value)

    @staticmethod
    def regex_extract(value: str, pattern: str, group: int = 0) -> str:
        """Extract regex match."""
        match = re.search(pattern, str(value))
        if match:
            return match.group(group)
        return ""

    @staticmethod
    def regex_replace(value: str, pattern: str, replacement: str) -> str:
        """Replace regex match."""
        return re.sub(pattern, replacement, str(value))

    @staticmethod
    def if_null(value: Any, default: Any) -> Any:
        """Return default if value is None."""
        return default if value is None else value

    @staticmethod
    def if_empty(value: Any, default: Any) -> Any:
        """Return default if value is empty."""
        if not value:
            return default
        return value

    @staticmethod
    def coalesce(*values) -> Any:
        """Return first non-null value."""
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def add_days(date_str: str, days: int, fmt: str = "%Y-%m-%d") -> str:
        """Add days to date."""
        from datetime import datetime, timedelta
        dt = datetime.strptime(date_str, fmt)
        dt += timedelta(days=days)
        return dt.strftime(fmt)

    @staticmethod
    def format_currency(value: float, symbol: str = "$", decimals: int = 2) -> str:
        """Format as currency."""
        return f"{symbol}{value:,.{decimals}f}"


def create_transformer(schema: Schema | None = None) -> DataTransformV2:
    """Create a configured data transformer.

    Args:
        schema: Optional schema for validation

    Returns:
        Configured DataTransformV2 instance
    """
    return DataTransformV2(schema=schema)

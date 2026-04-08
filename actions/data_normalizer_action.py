"""Data Normalizer Action Module.

Provides data normalization, deduplication, and schema validation
for inconsistent external data sources.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Union
from datetime import datetime
import logging

T = TypeVar("T")

logger = logging.getLogger(__name__)


class NormalizationStrategy(Enum):
    """Normalization strategy."""
    STRICT = "strict"
    LENIENT = "lenient"
    AGGRESSIVE = "aggressive"


@dataclass
class FieldSchema:
    """Schema definition for a field."""
    name: str
    field_type: type
    required: bool = True
    default: Any = None
    validator: Optional[Callable[[Any], bool]] = None
    transformer: Optional[Callable[[Any], Any]] = None
    normalizer: Optional[Callable[[Any], Any]] = None


@dataclass
class NormalizationResult:
    """Result of normalization operation."""
    success: bool
    data: Dict[str, Any]
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    normalized_fields: Set[str] = field(default_factory=set)


class DataNormalizerAction:
    """Data normalizer with schema validation.

    Example:
        normalizer = DataNormalizerAction()

        normalizer.register_field(FieldSchema(
            name="email",
            field_type=str,
            validator=lambda x: "@" in x
        ))

        normalizer.register_field(FieldSchema(
            name="phone",
            field_type=str,
            transformer=lambda x: re.sub(r"[^\d]", "", x)
        ))

        result = normalizer.normalize({
            "email": "  User@Example.com  ",
            "phone": "(123) 456-7890"
        })
    """

    def __init__(
        self,
        strategy: NormalizationStrategy = NormalizationStrategy.LENIENT,
        case_sensitive: bool = False,
    ) -> None:
        self.strategy = strategy
        self.case_sensitive = case_sensitive
        self._schemas: Dict[str, FieldSchema] = {}
        self._dedup_fields: List[str] = []
        self._transformation_rules: Dict[str, List[Callable]] = {}

    def register_field(self, schema: FieldSchema) -> None:
        """Register a field schema."""
        self._schemas[schema.name] = schema

    def register_fields(self, schemas: List[FieldSchema]) -> None:
        """Register multiple field schemas."""
        for schema in schemas:
            self.register_field(schema)

    def add_transformation(
        self,
        field_name: str,
        transformer: Callable[[Any], Any],
    ) -> None:
        """Add transformation rule for field."""
        if field_name not in self._transformation_rules:
            self._transformation_rules[field_name] = []
        self._transformation_rules[field_name].append(transformer)

    def set_dedup_fields(self, fields: List[str]) -> None:
        """Set fields for deduplication."""
        self._dedup_fields = fields

    def normalize(
        self,
        data: Dict[str, Any],
        partial: bool = False,
    ) -> NormalizationResult:
        """Normalize data according to registered schemas.

        Args:
            data: Input data dictionary
            partial: Allow partial normalization (skip missing fields)

        Returns:
            NormalizationResult with normalized data
        """
        result_data: Dict[str, Any] = {}
        errors: List[str] = []
        warnings: List[str] = []
        normalized_fields: Set[str] = set()

        for field_name, schema in self._schemas.items():
            if field_name not in data:
                if schema.required and not partial:
                    errors.append(f"Missing required field: {field_name}")
                    continue
                if schema.default is not None:
                    result_data[field_name] = schema.default
                continue

            value = data[field_name]
            field_errors: List[str] = []

            try:
                value = self._normalize_value(value, schema)
                value = self._apply_transformations(field_name, value)
                value = self._validate_value(value, schema, field_errors)

                if field_errors:
                    errors.extend(field_errors)
                    continue

                result_data[field_name] = value
                normalized_fields.add(field_name)

            except Exception as e:
                logger.error(f"Normalization error for {field_name}: {e}")
                errors.append(f"{field_name}: {str(e)}")

        deduplicated = self._deduplicate(result_data)

        return NormalizationResult(
            success=len(errors) == 0,
            data=deduplicated,
            errors=errors,
            warnings=warnings,
            normalized_fields=normalized_fields,
        )

    def normalize_batch(
        self,
        data_list: List[Dict[str, Any]],
    ) -> List[NormalizationResult]:
        """Normalize batch of data records."""
        return [self.normalize(data) for data in data_list]

    def _normalize_value(self, value: Any, schema: FieldSchema) -> Any:
        """Normalize a single value."""
        if value is None:
            return schema.default

        if schema.normalizer:
            return schema.normalizer(value)

        if schema.field_type == str:
            return self._normalize_string(value)

        if schema.field_type in (int, float):
            return self._normalize_number(value)

        return value

    def _normalize_string(self, value: Any) -> str:
        """Normalize string value."""
        str_value = str(value).strip()

        if self.strategy == NormalizationStrategy.AGGRESSIVE:
            str_value = re.sub(r"\s+", " ", str_value)
            str_value = str_value.lower()

        if not self.case_sensitive:
            str_value = str_value.lower()

        return str_value

    def _normalize_number(self, value: Any) -> Union[int, float]:
        """Normalize numeric value."""
        if isinstance(value, (int, float)):
            return value

        str_value = re.sub(r"[^\d.-]", "", str(value))

        if "." in str_value:
            return float(str_value)
        return int(str_value)

    def _apply_transformations(
        self,
        field_name: str,
        value: Any,
    ) -> Any:
        """Apply transformation rules to value."""
        if field_name not in self._transformation_rules:
            return value

        for transformer in self._transformation_rules[field_name]:
            value = transformer(value)

        return value

    def _validate_value(
        self,
        value: Any,
        schema: FieldSchema,
        errors: List[str],
    ) -> Any:
        """Validate value against schema."""
        if schema.validator and not schema.validator(value):
            errors.append(f"Validation failed for {schema.name}: {value}")
            return value

        if schema.transformer:
            return schema.transformer(value)

        return value

    def _deduplicate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Deduplicate data based on dedup fields."""
        if not self._dedup_fields:
            return data

        seen: Dict[str, Set[Any]] = {}
        result = {}

        for field_name in self._dedup_fields:
            if field_name in data:
                value = data[field_name]
                if field_name not in seen:
                    seen[field_name] = set()
                if value not in seen[field_name]:
                    seen[field_name].add(value)
                    result[field_name] = value
                else:
                    logger.warning(f"Duplicate value for {field_name}: {value}")

        for k, v in data.items():
            if k not in self._dedup_fields:
                result[k] = v

        return result

    def infer_schema(self, data: Dict[str, Any]) -> List[FieldSchema]:
        """Infer schema from sample data."""
        schemas: List[FieldSchema] = []

        for field_name, value in data.items():
            field_type = type(value)
            schema = FieldSchema(
                name=field_name,
                field_type=field_type,
                required=False,
                default=value,
            )
            schemas.append(schema)

        return schemas

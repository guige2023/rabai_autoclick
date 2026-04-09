"""
Data Projection Action Module.

Provides data projection capabilities for reshaping,
filtering, and transforming data structures.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ProjectionType(Enum):
    """Types of data projections."""

    IDENTITY = "identity"
    SELECT = "select"
    OMIT = "omit"
    RENAME = "rename"
    TRANSFORM = "transform"
    FLATTEN = "flatten"
    NEST = "nest"
    PIVOT = "pivot"


@dataclass
class ProjectionField:
    """Represents a projection field specification."""

    name: str
    source: Optional[str] = None
    transform: Optional[Callable] = None
    default: Any = None


class DataProjectionAction:
    """
    Performs data projections for reshaping structures.

    Features:
    - Field selection and omission
    - Field renaming and transformation
    - Nested structure handling
    - Custom projection functions

    Example:
        proj = DataProjectionAction()
        result = proj.project(data, select=["id", "name", "value"])
    """

    def __init__(self) -> None:
        """Initialize projection action."""
        self._projection_cache: dict[str, Callable] = {}

    def project(
        self,
        data: list[dict[str, Any]],
        projection_type: ProjectionType = ProjectionType.SELECT,
        fields: Optional[list[str]] = None,
        rename_map: Optional[dict[str, str]] = None,
        transform_map: Optional[dict[str, Callable]] = None,
        default_value: Any = None,
    ) -> list[dict[str, Any]]:
        """
        Project a list of records.

        Args:
            data: Input data records.
            projection_type: Type of projection.
            fields: Fields to select/omit.
            rename_map: Field rename mapping.
            transform_map: Field transformation functions.
            default_value: Default value for missing fields.

        Returns:
            Projected data records.
        """
        if not data:
            return []

        results = []

        for record in data:
            if projection_type == ProjectionType.SELECT:
                result = self._select_fields(record, fields or [], default_value)
            elif projection_type == ProjectionType.OMIT:
                result = self._omit_fields(record, fields or [])
            elif projection_type == ProjectionType.RENAME:
                result = self._rename_fields(record, rename_map or {})
            elif projection_type == ProjectionType.TRANSFORM:
                result = self._transform_fields(record, transform_map or {})
            else:
                result = record.copy()

            results.append(result)

        logger.debug(f"Projected {len(data)} records with {projection_type.value}")
        return results

    def _select_fields(
        self,
        record: dict[str, Any],
        fields: list[str],
        default: Any,
    ) -> dict[str, Any]:
        """Select specific fields from a record."""
        return {k: record.get(k, default) for k in fields}

    def _omit_fields(
        self,
        record: dict[str, Any],
        fields: list[str],
    ) -> dict[str, Any]:
        """Omit specific fields from a record."""
        return {k: v for k, v in record.items() if k not in fields}

    def _rename_fields(
        self,
        record: dict[str, Any],
        rename_map: dict[str, str],
    ) -> dict[str, Any]:
        """Rename fields in a record."""
        result = {}
        for k, v in record.items():
            new_name = rename_map.get(k, k)
            result[new_name] = v
        return result

    def _transform_fields(
        self,
        record: dict[str, Any],
        transform_map: dict[str, Callable],
    ) -> dict[str, Any]:
        """Apply transformations to fields."""
        result = {}
        for k, v in record.items():
            if k in transform_map:
                try:
                    result[k] = transform_map[k](v)
                except Exception as e:
                    logger.warning(f"Transform failed for {k}: {e}")
                    result[k] = v
            else:
                result[k] = v
        return result

    def flatten(
        self,
        data: list[dict[str, Any]],
        separator: str = ".",
        max_depth: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Flatten nested structures.

        Args:
            data: Input data records.
            separator: Key separator for flattened keys.
            max_depth: Maximum flattening depth.

        Returns:
            Flattened data records.
        """
        results = []

        for record in data:
            flattened = self._flatten_dict(record, separator, 0, max_depth)
            results.append(flattened)

        return results

    def _flatten_dict(
        self,
        obj: Any,
        separator: str,
        current_depth: int,
        max_depth: int,
    ) -> Any:
        """Recursively flatten a dictionary."""
        if current_depth >= max_depth or not isinstance(obj, dict):
            return obj

        result = {}
        for key, value in obj.items():
            if isinstance(value, dict):
                nested = self._flatten_dict(
                    value, separator, current_depth + 1, max_depth
                )
                for nkey, nvalue in nested.items():
                    result[f"{key}{separator}{nkey}"] = nvalue
            elif isinstance(value, list):
                result[key] = ",".join(str(v) for v in value)
            else:
                result[key] = value

        return result

    def nest(
        self,
        data: list[dict[str, Any]],
        key_path: str,
        separator: str = ".",
    ) -> list[dict[str, Any]]:
        """
        Create nested structure from flat keys.

        Args:
            data: Input data records.
            key_path: Key path to create nesting.
            separator: Key separator.

        Returns:
            Nested data records.
        """
        results = []

        for record in data:
            nested = self._nest_dict(record, key_path.split(separator))
            results.append(nested)

        return results

    def _nest_dict(
        self,
        obj: dict[str, Any],
        keys: list[str],
    ) -> dict[str, Any]:
        """Recursively nest a dictionary."""
        if not keys:
            return obj

        result = {}
        current_key = keys[0]

        remaining = obj
        for k in keys:
            if k in remaining:
                result[k] = remaining[k]

        return result

    def create_projection(
        self,
        name: str,
        fields: list[ProjectionField],
    ) -> Callable[[dict[str, Any]], dict[str, Any]]:
        """
        Create a reusable projection function.

        Args:
            name: Projection name.
            fields: List of projection fields.

        Returns:
            Projection function.
        """
        def projection(record: dict[str, Any]) -> dict[str, Any]:
            result = {}
            for field_spec in fields:
                source = field_spec.source or field_spec.name
                value = record.get(source, field_spec.default)

                if field_spec.transform:
                    try:
                        value = field_spec.transform(value)
                    except Exception as e:
                        logger.warning(f"Transform error: {e}")

                result[field_spec.name] = value
            return result

        self._projection_cache[name] = projection
        return projection

    def apply_projection(
        self,
        name: str,
        data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Apply a named projection to data.

        Args:
            name: Projection name.
            data: Input data.

        Returns:
            Projected data.
        """
        if name not in self._projection_cache:
            logger.error(f"Projection not found: {name}")
            return data

        projection_fn = self._projection_cache[name]
        return [projection_fn(record) for record in data]

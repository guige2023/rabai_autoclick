"""
Data Coalesce Action Module.

Provides null/coalescing operations for data structures,
handles missing values, defaults, and fallback chains.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class CoalesceSpec:
    """Specification for a coalescing operation."""
    sources: list[str]
    default: Any = None
    transform: Optional[Callable[[Any], Any]] = None


class DataCoalesceAction:
    """
    Coalescing operations for handling missing/null data.

    Provides first-non-null resolution, default values,
    and chained fallback lookups.

    Example:
        coal = DataCoalesceAction()
        result = coal.first_non_null(
            data.get("field1"),
            data.get("field2"),
            default="fallback"
        )
    """

    def __init__(self) -> None:
        self._specs: list[CoalesceSpec] = []

    def first_non_null(
        self,
        *values: Any,
        default: Any = None,
        transform: Optional[Callable[[Any], Any]] = None,
    ) -> Any:
        """Return the first non-null/non-None value."""
        for value in values:
            if value is not None:
                if transform:
                    try:
                        value = transform(value)
                    except Exception as e:
                        logger.warning("Transform failed: %s", e)
                        continue
                return value
        return default

    def first_non_empty(
        self,
        *values: Any,
        default: Any = None,
        empty_is_none: bool = True,
    ) -> Any:
        """Return first non-empty value (treats empty string as None)."""
        for value in values:
            if value is None:
                continue

            if empty_is_none and isinstance(value, (str, list, dict)):
                if not value:
                    continue

            if value is not None:
                return value

        return default

    def coalesce_path(
        self,
        data: dict,
        *paths: str,
        default: Any = None,
        separator: str = ".",
    ) -> Any:
        """Try multiple paths and return first found value."""
        for path in paths:
            value = self._get_nested(data, path, separator=separator)
            if value is not None:
                return value
        return default

    def coalesce_dict(
        self,
        *dicts: dict,
        key: str,
        default: Any = None,
    ) -> Any:
        """Try getting key from multiple dicts in order."""
        for d in dicts:
            if isinstance(d, dict) and key in d:
                value = d[key]
                if value is not None:
                    return value
        return default

    def add_spec(
        self,
        sources: list[str],
        default: Any = None,
        transform: Optional[Callable[[Any], Any]] = None,
    ) -> "DataCoalesceAction":
        """Add a named coalesce specification."""
        self._specs.append(CoalesceSpec(
            sources=sources,
            default=default,
            transform=transform,
        ))
        return self

    def apply_specs(
        self,
        data: dict,
    ) -> dict[str, Any]:
        """Apply all registered coalesce specs to data."""
        result = {}
        for spec in self._specs:
            value = self.coalesce_path(data, *spec.sources, default=spec.default)
            if spec.transform and value is not None:
                try:
                    value = spec.transform(value)
                except Exception as e:
                    logger.warning("Spec transform failed: %s", e)
                    value = spec.default
            result[spec.sources[0]] = value
        return result

    def fill_defaults(
        self,
        data: dict,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        """Fill missing keys in data according to schema defaults."""
        result = dict(data)

        for key, spec in schema.items():
            if key not in result or result[key] is None:
                if isinstance(spec, dict) and "default" in spec:
                    result[key] = spec["default"]
                elif isinstance(spec, dict) and "coalesce" in spec:
                    sources = spec["coalesce"]
                    result[key] = self.coalesce_path(
                        data, *sources,
                        default=spec.get("default"),
                    )

        return result

    def _get_nested(
        self,
        data: dict,
        path: str,
        separator: str = ".",
    ) -> Any:
        """Get nested value using dot notation."""
        parts = path.split(separator)
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, (list, tuple)):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None

            if current is None:
                return None

        return current

"""
Data Projection Action Module.

Projects data through transformations, extractions,
and reshaping with support for nested structures.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class ProjectionSpec:
    """Specification for a single projection."""
    source_path: str
    target_path: Optional[str] = None
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None


class DataProjectionAction:
    """
    Data projection and transformation engine.

    Transforms and reshapes data using path-based extraction,
    inline transformations, and restructuring.

    Example:
        proj = DataProjectionAction()
        proj.add_field("name", "user.name")
        proj.add_field("age", "user.profile.age", transform=int)
        proj.add_field("tags", "user.interests", default=[])
        result = proj.apply(data)
    """

    def __init__(self) -> None:
        self._fields: list[ProjectionSpec] = []
        self._default_handler: Optional[Callable[[dict], dict]] = None

    def add_field(
        self,
        target: str,
        source: str,
        transform: Optional[Callable[[Any], Any]] = None,
        default: Any = None,
    ) -> "DataProjectionAction":
        """Add a field projection."""
        self._fields.append(ProjectionSpec(
            source_path=source,
            target_path=target,
            transform=transform,
            default=default,
        ))
        return self

    def add_computed(
        self,
        target: str,
        compute: Callable[[dict], Any],
    ) -> "DataProjectionAction":
        """Add a computed field."""
        self._fields.append(ProjectionSpec(
            source_path=target,
            transform=compute,
        ))
        return self

    def set_default_handler(
        self,
        handler: Callable[[dict], dict],
    ) -> "DataProjectionAction":
        """Set default handler for unhandled source fields."""
        self._default_handler = handler
        return self

    def apply(self, data: Any) -> dict[str, Any]:
        """Apply all projections to data."""
        if not isinstance(data, dict):
            data = {"_root": data}

        result: dict[str, Any] = {}

        for spec in self._fields:
            value = self._extract_path(data, spec.source_path)

            if value is None and spec.default is not None:
                value = spec.default

            if spec.transform is not None and value is not None:
                try:
                    if callable(spec.transform):
                        value = spec.transform(value)
                except Exception as e:
                    logger.warning("Transform failed for %s: %s", spec.source_path, e)
                    value = None

            target = spec.target_path or spec.source_path
            result[target] = value

        if self._default_handler:
            result = self._default_handler(result)

        return result

    def apply_batch(self, items: list[Any]) -> list[dict[str, Any]]:
        """Apply projections to a batch of items."""
        return [self.apply(item) for item in items]

    def _extract_path(self, data: Any, path: str) -> Any:
        """Extract value from nested data using dot notation."""
        if not path:
            return data

        parts = path.split(".")
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

    def _set_path(self, data: dict, path: str, value: Any) -> None:
        """Set a value in nested dict using dot notation."""
        parts = path.split(".")
        current = data

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

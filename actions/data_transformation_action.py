"""Data Transformation Pipeline.

This module provides data transformation capabilities:
- Chained transformations
- Schema validation
- Type coercion
- Custom transformers

Example:
    >>> from actions.data_transformation_action import DataTransformer
    >>> transformer = DataTransformer()
    >>> transformer.add_step("normalize", normalize_text)
    >>> result = transformer.transform({"name": "  JOHN  "})
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class TransformStep:
    """A single transformation step."""
    name: str
    func: Callable[[Any], Any]
    skip_on_error: bool = False
    description: str = ""


@dataclass
class TransformResult:
    """Result of a transformation."""
    success: bool
    data: Any
    steps_applied: int
    errors: list[str]
    transformations: list[str]


class DataTransformer:
    """Chained data transformation pipeline."""

    def __init__(self) -> None:
        """Initialize the data transformer."""
        self._steps: list[TransformStep] = []
        self._lock = threading.Lock()
        self._stats = {"transforms": 0, "errors": 0}

    def add_step(
        self,
        name: str,
        func: Callable[[Any], Any],
        skip_on_error: bool = False,
        description: str = "",
    ) -> None:
        """Add a transformation step.

        Args:
            name: Step name.
            func: Transform function.
            skip_on_error: Skip this step if it errors.
            description: Human-readable description.
        """
        with self._lock:
            step = TransformStep(
                name=name,
                func=func,
                skip_on_error=skip_on_error,
                description=description,
            )
            self._steps.append(step)
            logger.info("Added transform step: %s", name)

    def transform(self, data: Any) -> TransformResult:
        """Apply all transformation steps.

        Args:
            data: Data to transform.

        Returns:
            TransformResult with transformed data and stats.
        """
        result = data
        errors = []
        applied = []

        with self._lock:
            steps = list(self._steps)
            self._stats["transforms"] += 1

        for step in steps:
            try:
                result = step.func(result)
                applied.append(step.name)
            except Exception as e:
                logger.error("Transform step %s failed: %s", step.name, e)
                errors.append(f"{step.name}: {type(e).__name__}: {e}")
                self._stats["errors"] += 1
                if not step.skip_on_error:
                    break

        return TransformResult(
            success=len(errors) == 0,
            data=result,
            steps_applied=len(applied),
            errors=errors,
            transformations=applied,
        )

    def transform_record(
        self,
        record: dict[str, Any],
        field_mapping: Optional[dict[str, str]] = None,
        exclude_fields: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """Transform a single record with field mapping.

        Args:
            record: Record dict to transform.
            field_mapping: Dict mapping source to target field names.
            exclude_fields: Fields to exclude.

        Returns:
            Transformed record.
        """
        result = {}

        if field_mapping:
            for src, dst in field_mapping.items():
                result[dst] = record.get(src)
        else:
            result = dict(record)

        if exclude_fields:
            for field in exclude_fields:
                result.pop(field, None)

        return result

    def transform_batch(
        self,
        records: list[dict[str, Any]],
        field_mapping: Optional[dict[str, str]] = None,
        exclude_fields: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        """Transform a batch of records.

        Args:
            records: List of records.
            field_mapping: Field name mapping.
            exclude_fields: Fields to exclude.

        Returns:
            List of transformed records.
        """
        return [
            self.transform_record(r, field_mapping, exclude_fields)
            for r in records
        ]

    def remove_step(self, name: str) -> bool:
        """Remove a transformation step.

        Args:
            name: Step name.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            for i, step in enumerate(self._steps):
                if step.name == name:
                    self._steps.pop(i)
                    logger.info("Removed transform step: %s", name)
                    return True
            return False

    def list_steps(self) -> list[TransformStep]:
        """List all transformation steps."""
        with self._lock:
            return list(self._steps)

    def clear_steps(self) -> None:
        """Remove all transformation steps."""
        with self._lock:
            self._steps.clear()

    def get_stats(self) -> dict[str, int]:
        """Get transformation statistics."""
        with self._lock:
            return dict(self._stats)


class BuiltInTransformers:
    """Collection of built-in transformer functions."""

    @staticmethod
    def lowercase(key: str) -> Callable[[dict], dict]:
        """Create a lowercase transformer for a field."""
        def transform(data: dict) -> dict:
            result = dict(data)
            if key in result and isinstance(result[key], str):
                result[key] = result[key].lower()
            return result
        return transform

    @staticmethod
    def uppercase(key: str) -> Callable[[dict], dict]:
        """Create an uppercase transformer for a field."""
        def transform(data: dict) -> dict:
            result = dict(data)
            if key in result and isinstance(result[key], str):
                result[key] = result[key].upper()
            return result
        return transform

    @staticmethod
    def strip_whitespace(key: str) -> Callable[[dict], dict]:
        """Create a strip transformer for a field."""
        def transform(data: dict) -> dict:
            result = dict(data)
            if key in result and isinstance(result[key], str):
                result[key] = result[key].strip()
            return result
        return transform

    @staticmethod
    def to_int(key: str, default: int = 0) -> Callable[[dict], dict]:
        """Create an integer coercion transformer."""
        def transform(data: dict) -> dict:
            result = dict(data)
            try:
                result[key] = int(data.get(key, default))
            except (ValueError, TypeError):
                result[key] = default
            return result
        return transform

    @staticmethod
    def to_float(key: str, default: float = 0.0) -> Callable[[dict], dict]:
        """Create a float coercion transformer."""
        def transform(data: dict) -> dict:
            result = dict(data)
            try:
                result[key] = float(data.get(key, default))
            except (ValueError, TypeError):
                result[key] = default
            return result
        return transform

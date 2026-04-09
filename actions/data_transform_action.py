"""
Data Transform Action Module.

Data transformation pipeline with mapping, filtering,
aggregation, and custom transformation functions.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar, Optional
import logging

logger = logging.getLogger(__name__)
T = TypeVar("T")
R = TypeVar("R")


@dataclass
class TransformStep:
    """
    Single transformation step.

    Attributes:
        name: Step identifier.
        transform_func: Transformation function.
        filter_func: Optional filter function.
        error_handler: Optional error handler.
    """
    name: str
    transform_func: Callable[[Any], Any]
    filter_func: Optional[Callable[[Any], bool]] = None
    error_handler: Optional[Callable[[Exception, Any], Any]] = None


@dataclass
class FieldMapping:
    """Field mapping for data transformation."""
    source_field: str
    target_field: str
    transform: Optional[Callable] = None
    default: Any = None


@dataclass
class TransformResult(Generic[T]):
    """Result of transformation."""
    success: bool
    data: Optional[T]
    errors: list[str]
    transformed_count: int
    filtered_count: int


class DataTransformAction(Generic[T]):
    """
    Data transformation pipeline with multiple operations.

    Example:
        transformer = DataTransformAction[dict]()
        transformer.map_field("user_id", "id")
        transformer.add_step("normalize", normalize_func)
        result = transformer.transform(data_records)
    """

    def __init__(self):
        """Initialize data transform action."""
        self._steps: list[TransformStep] = []
        self._field_mappings: list[FieldMapping] = []
        self._default_filter: Optional[Callable] = None

    def map_field(
        self,
        source: str,
        target: str,
        transform: Optional[Callable] = None,
        default: Any = None
    ) -> "DataTransformAction":
        """
        Add field mapping.

        Args:
            source: Source field name.
            target: Target field name.
            transform: Optional transformation function.
            default: Default value if source is missing.

        Returns:
            Self for method chaining.
        """
        mapping = FieldMapping(
            source_field=source,
            target_field=target,
            transform=transform,
            default=default
        )
        self._field_mappings.append(mapping)
        return self

    def add_step(
        self,
        name: str,
        transform_func: Callable[[Any], Any],
        filter_func: Optional[Callable] = None,
        error_handler: Optional[Callable] = None
    ) -> "DataTransformAction":
        """
        Add transformation step.

        Args:
            name: Step identifier.
            transform_func: Transformation function.
            filter_func: Optional filter function.
            error_handler: Optional error handler.

        Returns:
            Self for method chaining.
        """
        step = TransformStep(
            name=name,
            transform_func=transform_func,
            filter_func=filter_func,
            error_handler=error_handler
        )
        self._steps.append(step)
        return self

    def filter_with(self, filter_func: Callable[[Any], bool]) -> "DataTransformAction":
        """
        Set default filter function.

        Args:
            filter_func: Filter function that returns True to keep.

        Returns:
            Self for method chaining.
        """
        self._default_filter = filter_func
        return self

    def transform(self, data: list[T]) -> TransformResult[T]:
        """
        Transform data through pipeline.

        Args:
            data: List of records to transform.

        Returns:
            TransformResult with transformed data.
        """
        errors = []
        transformed_count = 0
        filtered_count = 0
        results = []

        for record in data:
            try:
                transformed = self._apply_pipeline(record)

                if transformed is None:
                    filtered_count += 1
                    continue

                results.append(transformed)
                transformed_count += 1

            except Exception as e:
                errors.append(f"Record transform error: {e}")
                logger.error(f"Transform error for record: {e}")

        return TransformResult(
            success=len(errors) == 0,
            data=results,
            errors=errors,
            transformed_count=transformed_count,
            filtered_count=filtered_count
        )

    def transform_one(self, record: T) -> Optional[T]:
        """
        Transform a single record.

        Args:
            record: Record to transform.

        Returns:
            Transformed record or None if filtered out.
        """
        return self._apply_pipeline(record)

    def _apply_pipeline(self, record: T) -> Optional[T]:
        """Apply all transformations to a record."""
        if self._default_filter and not self._default_filter(record):
            return None

        result = record

        for mapping in self._field_mappings:
            result = self._apply_mapping(result, mapping)

        for step in self._steps:
            if step.filter_func and not step.filter_func(result):
                return None

            try:
                result = step.transform_func(result)
            except Exception as e:
                if step.error_handler:
                    result = step.error_handler(e, result)
                else:
                    raise

        return result

    def _apply_mapping(self, record: T, mapping: FieldMapping) -> T:
        """Apply field mapping to record."""
        if isinstance(record, dict):
            source_val = record.get(mapping.source_field, mapping.default)

            if mapping.transform:
                source_val = mapping.transform(source_val)

            result = dict(record)
            result[mapping.target_field] = source_val

            if mapping.source_field != mapping.target_field:
                result.pop(mapping.source_field, None)

            return result

        elif hasattr(record, "__dict__"):
            source_val = getattr(record, mapping.source_field, mapping.default)

            if mapping.transform:
                source_val = mapping.transform(source_val)

            result = {**vars(record), mapping.target_field: source_val}

            if mapping.source_field != mapping.target_field:
                result.pop(mapping.source_field, None)

            class DynamicObject:
                def __init__(self, data):
                    for k, v in data.items():
                        setattr(self, k, v)

            return DynamicObject(result)

        return record

    def batch_transform(
        self,
        data: list[T],
        batch_size: int = 100
    ) -> TransformResult[T]:
        """
        Transform data in batches.

        Args:
            data: List of records.
            batch_size: Records per batch.

        Returns:
            TransformResult with all transformed data.
        """
        all_results = []
        all_errors = []
        total_transformed = 0
        total_filtered = 0

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            result = self.transform(batch)

            all_results.extend(result.data or [])
            all_errors.extend(result.errors)
            total_transformed += result.transformed_count
            total_filtered += result.filtered_count

        return TransformResult(
            success=len(all_errors) == 0,
            data=all_results,
            errors=all_errors,
            transformed_count=total_transformed,
            filtered_count=total_filtered
        )

    def clear(self) -> None:
        """Clear all transformations and mappings."""
        self._steps.clear()
        self._field_mappings.clear()
        self._default_filter = None

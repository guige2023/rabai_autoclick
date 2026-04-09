"""Data Transform Pipeline Action Module.

Provides a declarative pipeline for transforming data with:
- Field mapping and renaming
- Type conversion
- Value transformation
- Conditional logic
- Error handling and recovery

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class TransformType(Enum):
    """Types of field transformations."""
    MAP = auto()
    RENAME = auto()
    CONVERT = auto()
    FILTER = auto()
    AGGREGATE = auto()
    CONDITIONAL = auto()
    LOOKUP = auto()
    COMPUTED = auto()


@dataclass
class FieldTransform:
    """A single field transformation."""
    name: str
    transform_type: TransformType
    source_fields: List[str] = field(default_factory=list)
    target_field: Optional[str] = None
    transform_fn: Optional[Callable] = None
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    default_value: Any = None
    skip_on_error: bool = True
    error_value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformStep:
    """A step in the transformation pipeline."""
    name: str
    transforms: List[FieldTransform] = field(default_factory=list)
    parallel: bool = False
    continue_on_error: bool = True


@dataclass
class TransformResult:
    """Result of a transformation operation."""
    success: bool
    output: List[Dict[str, Any]] = field(default_factory=list)
    records_processed: int = 0
    records_transformed: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    duration_ms: float = 0.0


class DataTransformPipeline:
    """Pipeline for declarative data transformation.
    
    Supports:
    - Field mapping and renaming
    - Type conversion
    - Value transformations
    - Conditional logic
    - Lookup operations
    - Computed fields
    - Error handling
    """
    
    def __init__(self, name: str = "default"):
        self.name = name
        self._steps: List[TransformStep] = []
        self._lookup_tables: Dict[str, Dict[str, Any]] = {}
        self._global_error_handler: Optional[Callable] = None
        self._metrics = {
            "records_processed": 0,
            "records_transformed": 0,
            "errors": 0
        }
    
    def add_step(self, step: TransformStep) -> "DataTransformPipeline":
        """Add a transformation step.
        
        Args:
            step: Transform step to add
            
        Returns:
            Self for chaining
        """
        self._steps.append(step)
        return self
    
    def map_field(
        self,
        source_field: str,
        target_field: str,
        transform_fn: Optional[Callable[[Any], Any]] = None,
        default: Any = None,
        skip_on_error: bool = True
    ) -> "DataTransformPipeline":
        """Map a source field to target field with optional transformation.
        
        Args:
            source_field: Source field name
            target_field: Target field name
            transform_fn: Optional transformation function
            default: Default value if source is missing
            skip_on_error: Whether to skip record on error
            
        Returns:
            Self for chaining
        """
        step = TransformStep(name=f"map_{source_field}_to_{target_field}")
        step.transforms.append(FieldTransform(
            name=f"{source_field}->{target_field}",
            transform_type=TransformType.MAP,
            source_fields=[source_field],
            target_field=target_field,
            transform_fn=transform_fn,
            default_value=default,
            skip_on_error=skip_on_error
        ))
        self._steps.append(step)
        return self
    
    def rename_field(
        self,
        source_field: str,
        target_field: str
    ) -> "DataTransformPipeline":
        """Rename a field.
        
        Args:
            source_field: Source field name
            target_field: Target field name
            
        Returns:
            Self for chaining
        """
        return self.map_field(source_field, target_field)
    
    def convert_type(
        self,
        field_name: str,
        target_type: type,
        default: Any = None,
        skip_on_error: bool = True
    ) -> "DataTransformPipeline":
        """Convert field type.
        
        Args:
            field_name: Field to convert
            target_type: Target Python type
            default: Default value on conversion error
            skip_on_error: Whether to skip on error
            
        Returns:
            Self for chaining
        """
        def type_converter(value: Any) -> Any:
            if value is None:
                return default
            try:
                if target_type == bool:
                    if isinstance(value, str):
                        return value.lower() in ("true", "1", "yes", "on")
                return target_type(value)
            except (ValueError, TypeError):
                return default
        
        return self.map_field(field_name, field_name, type_converter, default=default, skip_on_error=skip_on_error)
    
    def filter_fields(
        self,
        include_fields: Optional[List[str]] = None,
        exclude_fields: Optional[List[str]] = None
    ) -> "DataTransformPipeline":
        """Filter to include or exclude specific fields.
        
        Args:
            include_fields: Fields to include (if set)
            exclude_fields: Fields to exclude
            
        Returns:
            Self for chaining
        """
        def filter_fn(record: Dict[str, Any]) -> Dict[str, Any]:
            result = {}
            
            if include_fields:
                for f in include_fields:
                    if f in record:
                        result[f] = record[f]
            elif exclude_fields:
                for f, v in record.items():
                    if f not in exclude_fields:
                        result[f] = v
            else:
                result = record.copy()
            
            return result
        
        step = TransformStep(name=f"filter_fields")
        step.transforms.append(FieldTransform(
            name="filter",
            transform_type=TransformType.FILTER,
            transform_fn=filter_fn
        ))
        self._steps.append(step)
        return self
    
    def add_computed_field(
        self,
        target_field: str,
        compute_fn: Callable[[Dict[str, Any]], Any],
        source_fields: Optional[List[str]] = None
    ) -> "DataTransformPipeline":
        """Add a computed field based on other fields.
        
        Args:
            target_field: Name for computed field
            compute_fn: Function that takes record and returns computed value
            source_fields: Fields used in computation (for dependency tracking)
            
        Returns:
            Self for chaining
        """
        step = TransformStep(name=f"computed_{target_field}")
        step.transforms.append(FieldTransform(
            name=f"computed_{target_field}",
            transform_type=TransformType.COMPUTED,
            source_fields=source_fields or [],
            target_field=target_field,
            transform_fn=compute_fn
        ))
        self._steps.append(step)
        return self
    
    def add_conditional(
        self,
        condition: Callable[[Dict[str, Any]], bool],
        then_transforms: List[FieldTransform],
        else_transforms: Optional[List[FieldTransform]] = None
    ) -> "DataTransformPipeline":
        """Add conditional transformation.
        
        Args:
            condition: Function returning True for 'then' path
            then_transforms: Transforms to apply when condition is True
            else_transforms: Transforms to apply when condition is False
            
        Returns:
            Self for chaining
        """
        step = TransformStep(name="conditional")
        for t in then_transforms:
            t.condition = condition
            step.transforms.append(t)
        if else_transforms:
            for t in else_transforms:
                t.condition = lambda r, cond=condition: not cond(r)
                step.transforms.append(t)
        self._steps.append(step)
        return self
    
    def add_lookup(
        self,
        target_field: str,
        lookup_key: str,
        lookup_table: Dict[str, Any],
        default: Any = None
    ) -> "DataTransformPipeline":
        """Add lookup transformation.
        
        Args:
            target_field: Field to populate with lookup result
            lookup_key: Field to use as lookup key
            lookup_table: Dictionary mapping keys to values
            default: Default if key not found
            
        Returns:
            Self for chaining
        """
        def lookup_fn(record: Dict[str, Any]) -> Any:
            key = record.get(lookup_key)
            return lookup_table.get(key, default)
        
        step = TransformStep(name=f"lookup_{target_field}")
        step.transforms.append(FieldTransform(
            name=f"lookup_{target_field}",
            transform_type=TransformType.LOOKUP,
            source_fields=[lookup_key],
            target_field=target_field,
            transform_fn=lookup_fn
        ))
        self._steps.append(step)
        return self
    
    def add_lookup_table(self, name: str, table: Dict[str, Any]) -> None:
        """Register a lookup table for later use.
        
        Args:
            name: Table name
            table: Lookup dictionary
        """
        self._lookup_tables[name] = table
    
    async def transform(
        self,
        records: List[Dict[str, Any]],
        continue_on_error: bool = True
    ) -> TransformResult:
        """Execute the transformation pipeline.
        
        Args:
            records: Input records
            continue_on_error: Whether to continue on transformation error
            
        Returns:
            Transformation result
        """
        start_time = asyncio.get_event_loop().time()
        output = []
        errors = []
        warnings = []
        transformed_count = 0
        
        for i, record in enumerate(records):
            try:
                transformed_record = record.copy()
                record_changed = False
                
                for step in self._steps:
                    for tf in step.transforms:
                        if tf.condition and not tf.condition(transformed_record):
                            continue
                        
                        try:
                            result_record, changed = await self._apply_transform(
                                transformed_record, tf
                            )
                            if changed:
                                transformed_record = result_record
                                record_changed = True
                        except Exception as e:
                            if not tf.skip_on_error:
                                raise
                            warnings.append(f"Transform {tf.name} error on record {i}: {e}")
                            if tf.error_value is not None:
                                if tf.target_field:
                                    transformed_record[tf.target_field] = tf.error_value
                
                output.append(transformed_record)
                if record_changed:
                    transformed_count += 1
                    
            except Exception as e:
                error_entry = {
                    "record_index": i,
                    "error": str(e),
                    "record": record
                }
                errors.append(error_entry)
                self._metrics["errors"] += 1
                
                if not continue_on_error:
                    break
        
        end_time = asyncio.get_event_loop().time()
        
        return TransformResult(
            success=len(errors) == 0,
            output=output,
            records_processed=len(records),
            records_transformed=transformed_count,
            errors=errors,
            warnings=warnings[:100],
            duration_ms=(end_time - start_time) * 1000
        )
    
    async def _apply_transform(
        self,
        record: Dict[str, Any],
        tf: FieldTransform
    ) -> tuple[Dict[str, Any], bool]:
        """Apply a single field transformation.
        
        Args:
            record: Input record
            tf: Field transform to apply
            
        Returns:
            Tuple of (transformed record, whether it changed)
        """
        changed = False
        
        if tf.transform_type == TransformType.MAP:
            if not tf.source_fields:
                return record, False
            
            source_value = record.get(tf.source_fields[0])
            
            if source_value is None and tf.default_value is not None:
                source_value = tf.default_value
            
            if source_value is not None:
                if tf.transform_fn:
                    source_value = tf.transform_fn(source_value)
                
                target = tf.target_field or tf.source_fields[0]
                record[target] = source_value
                changed = True
        
        elif tf.transform_type == TransformType.RENAME:
            if tf.source_fields and tf.target_field:
                if tf.source_fields[0] in record:
                    record[tf.target_field] = record.pop(tf.source_fields[0])
                    changed = True
        
        elif tf.transform_type == TransformType.FILTER:
            if tf.transform_fn:
                record = tf.transform_fn(record)
                changed = True
        
        elif tf.transform_type == TransformType.COMPUTED:
            if tf.transform_fn:
                computed_value = tf.transform_fn(record)
                target = tf.target_field
                if target:
                    record[target] = computed_value
                    changed = True
        
        elif tf.transform_type == TransformType.LOOKUP:
            if tf.transform_fn:
                lookup_value = tf.transform_fn(record)
                target = tf.target_field
                if target:
                    record[target] = lookup_value
                    changed = True
        
        return record, changed
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline metrics."""
        return {
            **self._metrics,
            "steps": len(self._steps),
            "transforms": sum(len(s.transforms) for s in self._steps)
        }


class TransformBuilder:
    """Builder for constructing complex transformations.
    
    Provides a fluent interface for building transformation pipelines.
    """
    
    def __init__(self, name: str = "default"):
        self.pipeline = DataTransformPipeline(name)
    
    def select(self, *fields: str) -> "TransformBuilder":
        """Select specific fields.
        
        Args:
            *fields: Fields to select
            
        Returns:
            Self for chaining
        """
        self.pipeline.filter_fields(include_fields=list(fields))
        return self
    
    def exclude(self, *fields: str) -> "TransformBuilder":
        """Exclude specific fields.
        
        Args:
            *fields: Fields to exclude
            
        Returns:
            Self for chaining
        """
        self.pipeline.filter_fields(exclude_fields=list(fields))
        return self
    
    def map(self, source: str, target: str, fn: Optional[Callable] = None) -> "TransformBuilder":
        """Map source to target.
        
        Args:
            source: Source field
            target: Target field
            fn: Optional transformation function
            
        Returns:
            Self for chaining
        """
        self.pipeline.map_field(source, target, fn)
        return self
    
    def rename(self, source: str, target: str) -> "TransformBuilder":
        """Rename field.
        
        Args:
            source: Source field
            target: Target field
            
        Returns:
            Self for chaining
        """
        self.pipeline.rename_field(source, target)
        return self
    
    def to_int(self, field: str, default: int = 0) -> "TransformBuilder":
        """Convert field to integer."""
        self.pipeline.convert_type(field, int, default)
        return self
    
    def to_float(self, field: str, default: float = 0.0) -> "TransformBuilder":
        """Convert field to float."""
        self.pipeline.convert_type(field, float, default)
        return self
    
    def to_string(self, field: str) -> "TransformBuilder":
        """Convert field to string."""
        self.pipeline.convert_type(field, str, "")
        return self
    
    def to_bool(self, field: str) -> "TransformBuilder":
        """Convert field to boolean."""
        self.pipeline.convert_type(field, bool, False)
        return self
    
    def compute(self, field: str, fn: Callable[[Dict[str, Any]], Any]) -> "TransformBuilder":
        """Add computed field.
        
        Args:
            field: Target field name
            fn: Computation function
            
        Returns:
            Self for chaining
        """
        self.pipeline.add_computed_field(field, fn)
        return self
    
    def lookup(self, field: str, key: str, table: Dict[str, Any], default: Any = None) -> "TransformBuilder":
        """Add lookup transformation.
        
        Args:
            field: Target field
            key: Lookup key field
            table: Lookup table
            default: Default if not found
            
        Returns:
            Self for chaining
        """
        self.pipeline.add_lookup(field, key, table, default)
        return self
    
    def when(self, condition: Callable[[Dict[str, Any]], bool]) -> "ConditionalBuilder":
        """Start conditional transformation.
        
        Args:
            condition: Condition function
            
        Returns:
            ConditionalBuilder for building conditional logic
        """
        return ConditionalBuilder(self.pipeline, condition)
    
    def build(self) -> DataTransformPipeline:
        """Build the pipeline.
        
        Returns:
            Configured DataTransformPipeline
        """
        return self.pipeline


class ConditionalBuilder:
    """Builder for conditional transformations."""
    
    def __init__(self, pipeline: DataTransformPipeline, condition: Callable[[Dict[str, Any]], bool]):
        self._pipeline = pipeline
        self._condition = condition
        self._then_transforms: List[FieldTransform] = []
        self._else_transforms: List[FieldTransform] = []
    
    def then_map(self, source: str, target: str, fn: Optional[Callable] = None) -> "ConditionalBuilder":
        """Add then-branch map transform."""
        self._then_transforms.append(FieldTransform(
            name=f"then_{source}->{target}",
            transform_type=TransformType.MAP,
            source_fields=[source],
            target_field=target,
            transform_fn=fn
        ))
        return self
    
    def else_map(self, source: str, target: str, fn: Optional[Callable] = None) -> "ConditionalBuilder":
        """Add else-branch map transform."""
        self._else_transforms.append(FieldTransform(
            name=f"else_{source}->{target}",
            transform_type=TransformType.MAP,
            source_fields=[source],
            target_field=target,
            transform_fn=fn
        ))
        return self
    
    def end(self) -> TransformBuilder:
        """End conditional and return to TransformBuilder."""
        self._pipeline.add_conditional(
            self._condition,
            self._then_transforms,
            self._else_transforms if self._else_transforms else None
        )
        return TransformBuilder(self._pipeline.name)

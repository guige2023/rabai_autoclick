"""Data Transform Action Module.

Provides comprehensive data transformation capabilities including
mapping, filtering, aggregation, and schema conversion operations.

Example:
    >>> from actions.data.data_transform_action import DataTransformAction
    >>> action = DataTransformAction()
    >>> result = action.transform(data, pipeline=[map_fn, filter_fn])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import threading


class TransformType(Enum):
    """Types of data transformations."""
    MAP = "map"
    FILTER = "filter"
    FLATTEN = "flatten"
    GROUP = "group"
    SORT = "sort"
    PROJECT = "project"
    AGGREGATE = "aggregate"
    JOIN = "join"
    UNION = "union"
    DIFFERENCE = "difference"


class DataType(Enum):
    """Supported data types for transformation."""
    OBJECT = "object"
    ARRAY = "array"
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    NULL = "null"


@dataclass
class FieldMapping:
    """Mapping definition for field transformation.
    
    Attributes:
        source_field: Original field name or path
        target_field: Destination field name
        transform: Optional transformation function
        default: Default value if source is missing
    """
    source_field: str
    target_field: str
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None
    required: bool = False


@dataclass
class TransformResult:
    """Result of a transformation operation.
    
    Attributes:
        success: Whether the transformation succeeded
        data: Transformed data
        errors: List of transformation errors
        metadata: Additional transformation metadata
    """
    success: bool
    data: Any = None
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataTransformAction:
    """Handles data transformation operations.
    
    Provides a flexible pipeline-based approach to data transformation
    with support for various transformation types and error handling.
    
    Attributes:
        pipeline: Ordered list of transformation functions
        mappings: Field mapping definitions
    
    Example:
        >>> action = DataTransformAction()
        >>> action.add_mapping("id", "user_id", transform=int)
        >>> result = action.transform(data)
    """
    
    def __init__(self):
        """Initialize the data transform action."""
        self._pipeline: List[Tuple[TransformType, Callable[..., Any]]] = []
        self._mappings: List[FieldMapping] = []
        self._field_renames: Dict[str, str] = {}
        self._computed_fields: Dict[str, Callable[[Dict], Any]] = {}
        self._validators: List[Callable[[Any], bool]] = []
        self._error_handlers: Dict[str, Callable[[Exception], Any]] = {}
        self._lock = threading.RLock()
    
    def add_mapping(
        self,
        source_field: str,
        target_field: str,
        transform: Optional[Callable[[Any], Any]] = None,
        default: Any = None,
        required: bool = False
    ) -> "DataTransformAction":
        """Add a field mapping for transformation.
        
        Args:
            source_field: Source field name or path
            target_field: Target field name
            transform: Optional transformation function
            default: Default value if source is missing
            required: Whether the source field is required
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._mappings.append(FieldMapping(
                source_field=source_field,
                target_field=target_field,
                transform=transform,
                default=default,
                required=required
            ))
            return self
    
    def add_rename(self, source: str, target: str) -> "DataTransformAction":
        """Add a simple field rename.
        
        Args:
            source: Original field name
            target: New field name
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._field_renames[source] = target
            return self
    
    def add_computed_field(
        self,
        field_name: str,
        compute_fn: Callable[[Dict], Any]
    ) -> "DataTransformAction":
        """Add a computed field.
        
        Args:
            field_name: Name of the computed field
            compute_fn: Function to compute the field value
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._computed_fields[field_name] = compute_fn
            return self
    
    def add_filter(self, filter_fn: Callable[[Any], bool]) -> "DataTransformAction":
        """Add a filter transformation.
        
        Args:
            filter_fn: Function that returns True to keep elements
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._pipeline.append((TransformType.FILTER, filter_fn))
            return self
    
    def add_map(self, map_fn: Callable[[Any], Any]) -> "DataTransformAction":
        """Add a map transformation.
        
        Args:
            map_fn: Function to apply to each element
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._pipeline.append((TransformType.MAP, map_fn))
            return self
    
    def add_validator(
        self,
        validator_fn: Callable[[Any], bool],
        error_message: str = "Validation failed"
    ) -> "DataTransformAction":
        """Add a data validator.
        
        Args:
            validator_fn: Function that returns True if data is valid
            error_message: Error message if validation fails
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            def wrapped_validator(data: Any) -> bool:
                try:
                    return validator_fn(data)
                except Exception as e:
                    raise ValueError(f"{error_message}: {e}")
            
            self._validators.append(wrapped_validator)
            return self
    
    def transform(self, data: Any) -> TransformResult:
        """Apply all transformations to the data.
        
        Args:
            data: Input data to transform
        
        Returns:
            TransformResult containing transformed data and any errors
        """
        errors: List[str] = []
        current_data = data
        metadata: Dict[str, Any] = {
            "start_time": datetime.now(),
            "transformations": []
        }
        
        try:
            # Validate input
            for validator in self._validators:
                if not validator(current_data):
                    errors.append(f"Validation failed for: {current_data}")
            
            # Apply field mappings
            if self._mappings:
                current_data = self._apply_mappings(current_data, errors)
            
            # Apply renames
            if self._field_renames:
                current_data = self._apply_renames(current_data, errors)
            
            # Apply computed fields
            if self._computed_fields:
                current_data = self._apply_computed_fields(current_data, errors)
            
            # Apply pipeline transformations
            for transform_type, transform_fn in self._pipeline:
                try:
                    if transform_type == TransformType.MAP:
                        current_data = self._transform_map(current_data, transform_fn)
                    elif transform_type == TransformType.FILTER:
                        current_data = self._transform_filter(current_data, transform_fn)
                    elif transform_type == TransformType.FLATTEN:
                        current_data = self._transform_flatten(current_data)
                    elif transform_type == TransformType.SORT:
                        current_data = self._transform_sort(current_data, transform_fn)
                    elif transform_type == TransformType.GROUP:
                        current_data = self._transform_group(current_data, transform_fn)
                    
                    metadata["transformations"].append(transform_type.value)
                    
                except Exception as e:
                    error_msg = f"Transformation {transform_type.value} failed: {str(e)}"
                    errors.append(error_msg)
                    if transform_type in self._error_handlers:
                        current_data = self._error_handlers[transform_type](e)
            
            metadata["end_time"] = datetime.now()
            metadata["error_count"] = len(errors)
            
            return TransformResult(
                success=len(errors) == 0,
                data=current_data,
                errors=errors,
                metadata=metadata
            )
            
        except Exception as e:
            return TransformResult(
                success=False,
                data=None,
                errors=[f"Transform failed: {str(e)}"],
                metadata=metadata
            )
    
    def _apply_mappings(self, data: Any, errors: List[str]) -> Any:
        """Apply field mappings to data.
        
        Args:
            data: Input data
            errors: List to append errors to
        
        Returns:
            Data with mappings applied
        """
        if isinstance(data, dict):
            result = {}
            for mapping in self._mappings:
                try:
                    value = self._get_nested_value(data, mapping.source_field)
                    if value is None and mapping.required:
                        errors.append(f"Required field missing: {mapping.source_field}")
                        value = mapping.default
                    elif value is None:
                        value = mapping.default
                    
                    if mapping.transform and value is not None:
                        value = mapping.transform(value)
                    
                    self._set_nested_value(result, mapping.target_field, value)
                except Exception as e:
                    errors.append(f"Mapping error for {mapping.source_field}: {str(e)}")
            
            return result
        return data
    
    def _apply_renames(self, data: Any, errors: List[str]) -> Any:
        """Apply field renames to data.
        
        Args:
            data: Input data
            errors: List to append errors to
        
        Returns:
            Data with renames applied
        """
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                new_key = self._field_renames.get(key, key)
                result[new_key] = value
            return result
        return data
    
    def _apply_computed_fields(self, data: Any, errors: List[str]) -> Any:
        """Apply computed fields to data.
        
        Args:
            data: Input data
            errors: List to append errors to
        
        Returns:
            Data with computed fields added
        """
        if isinstance(data, dict):
            result = data.copy()
            for field_name, compute_fn in self._computed_fields.items():
                try:
                    result[field_name] = compute_fn(result)
                except Exception as e:
                    errors.append(f"Computed field {field_name} failed: {str(e)}")
                    result[field_name] = None
            return result
        return data
    
    def _transform_map(
        self,
        data: Any,
        map_fn: Callable[[Any], Any]
    ) -> Any:
        """Apply map transformation.
        
        Args:
            data: Input data
            map_fn: Function to apply
        
        Returns:
            Transformed data
        """
        if isinstance(data, list):
            return [map_fn(item) for item in data]
        return map_fn(data)
    
    def _transform_filter(
        self,
        data: Any,
        filter_fn: Callable[[Any], bool]
    ) -> Any:
        """Apply filter transformation.
        
        Args:
            data: Input data
            filter_fn: Filter function
        
        Returns:
            Filtered data
        """
        if isinstance(data, list):
            return [item for item in data if filter_fn(item)]
        return data
    
    def _transform_flatten(self, data: Any) -> Any:
        """Flatten nested data structures.
        
        Args:
            data: Input data
        
        Returns:
            Flattened data
        """
        if isinstance(data, list):
            result = []
            for item in data:
                if isinstance(item, list):
                    result.extend(self._transform_flatten(item))
                else:
                    result.append(item)
            return result
        return data
    
    def _transform_sort(
        self,
        data: Any,
        key_fn: Callable[[Any], Any]
    ) -> Any:
        """Sort data.
        
        Args:
            data: Input data
            key_fn: Sort key function
        
        Returns:
            Sorted data
        """
        if isinstance(data, list):
            return sorted(data, key=key_fn)
        return data
    
    def _transform_group(
        self,
        data: Any,
        key_fn: Callable[[Any], Any]
    ) -> Dict[Any, List[Any]]:
        """Group data by key.
        
        Args:
            data: Input data
            key_fn: Grouping key function
        
        Returns:
            Grouped data
        """
        if isinstance(data, list):
            groups: Dict[Any, List[Any]] = {}
            for item in data:
                key = key_fn(item)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
            return groups
        return data
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Get a nested value from a dictionary using dot notation.
        
        Args:
            data: Dictionary to search
            path: Dot-separated path to the value
        
        Returns:
            Value at the path, or None if not found
        """
        keys = path.split(".")
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        
        return current
    
    def _set_nested_value(self, data: Dict, path: str, value: Any) -> None:
        """Set a nested value in a dictionary using dot notation.
        
        Args:
            data: Dictionary to modify
            path: Dot-separated path to the value
            value: Value to set
        """
        keys = path.split(".")
        current = data
        
        for i, key in enumerate(keys[:-1]):
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def clear(self) -> "DataTransformAction":
        """Clear all transformations and mappings.
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._pipeline.clear()
            self._mappings.clear()
            self._field_renames.clear()
            self._computed_fields.clear()
            self._validators.clear()
            return self

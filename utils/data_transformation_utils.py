"""
Data Transformation Utilities for UI Automation.

This module provides utilities for transforming and manipulating data
during automation workflows, including mapping, filtering, and conversion.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class DataMapper:
    """
    Maps data from one structure to another.
    
    Example:
        mapper = DataMapper()
        mapper.add_mapping("firstName", "first_name")
        mapper.add_mapping("lastName", "last_name")
        
        result = mapper.map({"firstName": "John", "lastName": "Doe"})
    """
    
    def __init__(self):
        self._mappings: dict[str, str] = {}
        self._transformers: dict[str, Callable[[Any], Any]] = {}
        self._defaults: dict[str, Any] = {}
    
    def add_mapping(self, source: str, target: str) -> 'DataMapper':
        """Add a simple field mapping."""
        self._mappings[source] = target
        return self
    
    def add_transform(
        self,
        target: str,
        transformer: Callable[[Any], Any]
    ) -> 'DataMapper':
        """Add a transformation for a target field."""
        self._transformers[target] = transformer
        return self
    
    def add_default(self, target: str, default: Any) -> 'DataMapper':
        """Add a default value for a target field."""
        self._defaults[target] = default
        return self
    
    def map(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Map data according to configured mappings.
        
        Args:
            data: Source data dictionary
            
        Returns:
            Mapped data dictionary
        """
        result = {}
        
        # Apply mappings
        for source, target in self._mappings.items():
            if source in data:
                result[target] = data[source]
        
        # Apply transformers
        for target, transformer in self._transformers.items():
            if target in result:
                try:
                    result[target] = transformer(result[target])
                except Exception:
                    pass
        
        # Apply defaults
        for target, default in self._defaults.items():
            if target not in result:
                result[target] = default
        
        return result


def filter_dict(
    data: dict[str, Any],
    include_keys: Optional[list[str]] = None,
    exclude_keys: Optional[list[str]] = None
) -> dict[str, Any]:
    """
    Filter dictionary by keys.
    
    Args:
        data: Dictionary to filter
        include_keys: Keys to include (if specified)
        exclude_keys: Keys to exclude (if specified)
        
    Returns:
        Filtered dictionary
    """
    if include_keys:
        return {k: v for k, v in data.items() if k in include_keys}
    
    if exclude_keys:
        return {k: v for k, v in data.items() if k not in exclude_keys}
    
    return dict(data)


def merge_dicts(
    *dicts: dict[str, Any],
    conflict_resolution: Callable[[Any, Any], Any] = None
) -> dict[str, Any]:
    """
    Merge multiple dictionaries.
    
    Args:
        *dicts: Dictionaries to merge
        conflict_resolution: Function to resolve conflicts
        
    Returns:
        Merged dictionary
    """
    result = {}
    
    for d in dicts:
        for key, value in d.items():
            if key in result and conflict_resolution:
                result[key] = conflict_resolution(result[key], value)
            elif key not in result:
                result[key] = value
    
    return result


def flatten_dict(
    data: dict[str, Any],
    separator: str = ".",
    prefix: str = ""
) -> dict[str, Any]:
    """
    Flatten a nested dictionary.
    
    Args:
        data: Dictionary to flatten
        separator: Key separator for nested keys
        prefix: Prefix for keys
        
    Returns:
        Flattened dictionary
    """
    result = {}
    
    for key, value in data.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key
        
        if isinstance(value, dict):
            result.update(flatten_dict(value, separator, new_key))
        else:
            result[new_key] = value
    
    return result


def unflatten_dict(
    data: dict[str, Any],
    separator: str = "."
) -> dict[str, Any]:
    """
    Unflatten a dictionary with dot-separated keys.
    
    Args:
        data: Flattened dictionary
        separator: Key separator
        
    Returns:
        Nested dictionary
    """
    result = {}
    
    for key, value in data.items():
        parts = key.split(separator)
        current = result
        
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        
        current[parts[-1]] = value
    
    return result


def group_by(
    items: list[dict[str, Any]],
    key: str
) -> dict[Any, list[dict[str, Any]]]:
    """
    Group items by a key.
    
    Args:
        items: List of dictionaries
        key: Key to group by
        
    Returns:
        Dictionary of grouped items
    """
    result = {}
    
    for item in items:
        if key in item:
            group_key = item[key]
            if group_key not in result:
                result[group_key] = []
            result[group_key].append(item)
    
    return result


def sort_dict(
    data: dict[str, Any],
    by: str = "key",
    reverse: bool = False
) -> dict[str, Any]:
    """
    Sort a dictionary.
    
    Args:
        data: Dictionary to sort
        by: Sort by "key" or "value"
        reverse: Sort in descending order
        
    Returns:
        Sorted dictionary
    """
    if by == "key":
        return dict(sorted(data.items(), key=lambda x: x[0], reverse=reverse))
    else:
        return dict(sorted(data.items(), key=lambda x: x[1], reverse=reverse))


def transform_values(
    data: dict[str, Any],
    transformer: Callable[[Any], Any]
) -> dict[str, Any]:
    """
    Transform all values in a dictionary.
    
    Args:
        data: Dictionary to transform
        transformer: Function to apply to each value
        
    Returns:
        Transformed dictionary
    """
    return {k: transformer(v) for k, v in data.items()}


def extract_fields(
    items: list[dict[str, Any]],
    fields: list[str],
    defaults: Optional[dict[str, Any]] = None
) -> list[dict[str, Any]]:
    """
    Extract specific fields from list of dictionaries.
    
    Args:
        items: List of dictionaries
        fields: Fields to extract
        defaults: Default values for missing fields
        
    Returns:
        List of dictionaries with only specified fields
    """
    defaults = defaults or {}
    result = []
    
    for item in items:
        extracted = {}
        for field_name in fields:
            extracted[field_name] = item.get(field_name, defaults.get(field_name))
        result.append(extracted)
    
    return result


class DataPipeline:
    """
    A pipeline for transforming data.
    
    Example:
        pipeline = (DataPipeline()
            .add_step(filter_dict, {"exclude_keys": ["password"]})
            .add_step(flatten_dict)
            .add_step(sort_dict))
        
        result = pipeline.execute(data)
    """
    
    def __init__(self, data: Optional[dict[str, Any]] = None):
        self._data = data
        self._steps: list[tuple[Callable, tuple, dict]] = []
    
    def add_step(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> 'DataPipeline':
        """Add a transformation step to the pipeline."""
        self._steps.append((func, args, kwargs))
        return self
    
    def execute(self, data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """
        Execute the pipeline.
        
        Args:
            data: Input data (overrides initial data)
            
        Returns:
            Transformed data
        """
        current = data if data is not None else self._data
        
        for func, args, kwargs in self._steps:
            current = func(current, *args, **kwargs)
        
        return current
    
    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        """Execute pipeline as callable."""
        return self.execute(data)

"""
Data Transformation Action - Transforms and reshapes data structures.

This module provides data transformation capabilities including
pivoting, unpivoting, mapping, and structural transformations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


class TransformType(Enum):
    """Types of data transformations."""
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    MAP = "map"
    FILTER = "filter"
    FLATTEN = "flatten"
    NEST = "nest"
    RENAME = "rename"
    CAST = "cast"


@dataclass
class TransformSpec:
    """Specification for a transformation."""
    transform_type: TransformType
    source_field: str | None = None
    target_field: str | None = None
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformResult:
    """Result of transformation."""
    transformed_data: list[dict[str, Any]]
    transform_count: int
    error_count: int
    errors: list[str] = field(default_factory=list)


class DataTransformer:
    """
    Transforms data structures.
    
    Example:
        transformer = DataTransformer()
        result = transformer.pivot(
            data,
            index="date",
            columns="product",
            values="sales"
        )
    """
    
    def __init__(self) -> None:
        self._transforms = {
            TransformType.PIVOT: self._pivot,
            TransformType.UNPIVOT: self._unpivot,
            TransformType.MAP: self._map_values,
            TransformType.FILTER: self._filter_fields,
            TransformType.FLATTEN: self._flatten,
            TransformType.NEST: self._nest,
            TransformType.RENAME: self._rename_fields,
            TransformType.CAST: self._cast_fields,
        }
    
    def transform(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Apply transformation to data."""
        transform_func = self._transforms.get(spec.transform_type)
        if transform_func:
            return transform_func(data, spec)
        return data
    
    def _pivot(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Pivot data from long to wide format."""
        index = spec.params.get("index")
        columns = spec.params.get("columns")
        values = spec.params.get("values")
        
        if not all([index, columns, values]):
            return data
        
        pivot: dict[tuple, dict[str, Any]] = {}
        
        for record in data:
            idx_val = record.get(index)
            col_val = record.get(columns)
            val = record.get(values)
            
            key = (idx_val,)
            if key not in pivot:
                pivot[key] = {index: idx_val}
            
            pivot[key][col_val] = val
        
        return list(pivot.values())
    
    def _unpivot(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Unpivot data from wide to long format."""
        id_vars = spec.params.get("id_vars", [])
        value_vars = spec.params.get("value_vars", [])
        
        if not value_vars:
            return data
        
        result = []
        
        for record in data:
            base = {k: record[k] for k in id_vars if k in record}
            
            for var in value_vars:
                if var in record:
                    result.append({
                        **base,
                        spec.params.get("variable_column", "variable"): var,
                        spec.params.get("value_column", "value"): record[var],
                    })
        
        return result
    
    def _map_values(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Apply mapping to field values."""
        field_name = spec.source_field or spec.params.get("field")
        mapping = spec.params.get("mapping", {})
        
        if not field_name:
            return data
        
        result = []
        for record in data:
            new_record = record.copy()
            if field_name in record:
                new_record[field_name] = mapping.get(record[field_name], record[field_name])
            result.append(new_record)
        
        return result
    
    def _filter_fields(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Filter which fields are included."""
        include = spec.params.get("include")
        exclude = spec.params.get("exclude")
        
        result = []
        for record in data:
            filtered = {}
            for key, value in record.items():
                if include and key not in include:
                    continue
                if exclude and key in exclude:
                    continue
                filtered[key] = value
            result.append(filtered)
        
        return result
    
    def _flatten(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Flatten nested structures."""
        separator = spec.params.get("separator", "_")
        prefix = spec.params.get("prefix", "")
        
        result = []
        for record in data:
            flattened = {}
            self._flatten_record(record, prefix, separator, flattened)
            result.append(flattened)
        
        return result
    
    def _flatten_record(
        self,
        record: Any,
        prefix: str,
        separator: str,
        result: dict[str, Any],
    ) -> None:
        """Recursively flatten a record."""
        if isinstance(record, dict):
            for key, value in record.items():
                new_key = f"{prefix}{separator}{key}" if prefix else key
                self._flatten_record(value, new_key, separator, result)
        elif isinstance(record, list):
            for i, item in enumerate(record):
                new_key = f"{prefix}{separator}{i}"
                self._flatten_record(item, new_key, separator, result)
        else:
            result[prefix] = record
    
    def _nest(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Nest flat fields into nested structure."""
        path = spec.params.get("path", [])
        
        if not path:
            return data
        
        result = []
        for record in data:
            nested = {}
            for key, value in record.items():
                parts = key.split("_")
                
                if len(parts) >= 2 and parts[0] == path[0]:
                    current = nested
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = value
                else:
                    nested[key] = value
            
            result.append(nested)
        
        return result
    
    def _rename_fields(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Rename fields."""
        mapping = spec.params.get("mapping", {})
        
        result = []
        for record in data:
            renamed = {}
            for key, value in record.items():
                new_key = mapping.get(key, key)
                renamed[new_key] = value
            result.append(renamed)
        
        return result
    
    def _cast_fields(
        self,
        data: list[dict[str, Any]],
        spec: TransformSpec,
    ) -> list[dict[str, Any]]:
        """Cast field types."""
        type_map = spec.params.get("types", {})
        
        result = []
        for record in data:
            casted = {}
            for key, value in record.items():
                if key in type_map:
                    try:
                        casted[key] = self._cast_value(value, type_map[key])
                    except Exception:
                        casted[key] = value
                else:
                    casted[key] = value
            result.append(casted)
        
        return result
    
    def _cast_value(self, value: Any, target_type: str) -> Any:
        """Cast value to target type."""
        if value is None:
            return None
        
        if target_type == "int":
            return int(value)
        elif target_type == "float":
            return float(value)
        elif target_type == "str":
            return str(value)
        elif target_type == "bool":
            return bool(value)
        
        return value


class DataTransformationAction:
    """
    Data transformation action for automation workflows.
    
    Example:
        action = DataTransformationAction()
        result = await action.transform(
            records,
            TransformSpec(
                transform_type=TransformType.PIVOT,
                params={"index": "date", "columns": "product", "values": "sales"}
            )
        )
    """
    
    def __init__(self) -> None:
        self.transformer = DataTransformer()
    
    async def transform(
        self,
        data: list[dict[str, Any]],
        specs: list[TransformSpec],
    ) -> TransformResult:
        """Apply multiple transformations."""
        errors = []
        error_count = 0
        
        for spec in specs:
            try:
                data = self.transformer.transform(data, spec)
            except Exception as e:
                errors.append(f"Transform {spec.transform_type}: {str(e)}")
                error_count += 1
        
        return TransformResult(
            transformed_data=data,
            transform_count=len(specs),
            error_count=error_count,
            errors=errors,
        )
    
    def pivot(
        self,
        data: list[dict[str, Any]],
        index: str,
        columns: str,
        values: str,
    ) -> list[dict[str, Any]]:
        """Pivot data."""
        spec = TransformSpec(
            transform_type=TransformType.PIVOT,
            params={"index": index, "columns": columns, "values": values},
        )
        return self.transformer.transform(data, spec)
    
    def flatten(
        self,
        data: list[dict[str, Any]],
        separator: str = "_",
    ) -> list[dict[str, Any]]:
        """Flatten nested structures."""
        spec = TransformSpec(
            transform_type=TransformType.FLATTEN,
            params={"separator": separator},
        )
        return self.transformer.transform(data, spec)


# Export public API
__all__ = [
    "TransformType",
    "TransformSpec",
    "TransformResult",
    "DataTransformer",
    "DataTransformationAction",
]

"""Data Mapping Action.

Maps fields between source and target schemas with transformations.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass


@dataclass
class FieldMap:
    source_field: str
    target_field: str
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None
    required: bool = False


class DataMappingAction:
    """Maps fields from source to target schemas."""

    def __init__(self, maps: Optional[List[FieldMap]] = None) -> None:
        self.maps = maps or []

    def add_map(
        self,
        source_field: str,
        target_field: str,
        transform: Optional[Callable[[Any], Any]] = None,
        default: Any = None,
        required: bool = False,
    ) -> "DataMappingAction":
        self.maps.append(FieldMap(
            source_field=source_field,
            target_field=target_field,
            transform=transform,
            default=default,
            required=required,
        ))
        return self

    def map_dict(self, source: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for fm in self.maps:
            value = source.get(fm.source_field)
            if value is None:
                value = fm.default
            if value is not None and fm.transform:
                try:
                    value = fm.transform(value)
                except Exception:
                    pass
            result[fm.target_field] = value
        return result

    def map_list(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.map_dict(item) for item in items]

    def get_missing_fields(self, source: Dict[str, Any]) -> List[str]:
        return [fm.source_field for fm in self.maps if fm.required and fm.source_field not in source]

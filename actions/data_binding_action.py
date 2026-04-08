"""Data Binding Action.

Binds data from multiple sources into unified objects.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass
import json


T = TypeVar("T")


@dataclass
class BindingRule:
    target_field: str
    source_path: str
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None


class DataBindingAction:
    """Binds data from various sources to typed target objects."""

    def __init__(self, bindings: Optional[List[BindingRule]] = None) -> None:
        self.bindings = bindings or []
        self._cache: Dict[str, Any] = {}

    def add_binding(
        self,
        target_field: str,
        source_path: str,
        transform: Optional[Callable[[Any], Any]] = None,
        default: Any = None,
    ) -> "DataBindingAction":
        self.bindings.append(BindingRule(
            target_field=target_field,
            source_path=source_path,
            transform=transform,
            default=default,
        ))
        return self

    def resolve_path(self, data: Dict[str, Any], path: str) -> Any:
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
            if value is None:
                return None
        return value

    def bind(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for rule in self.bindings:
            value = self.resolve_path(source_data, rule.source_path)
            if value is None:
                value = rule.default
            if value is not None and rule.transform:
                try:
                    value = rule.transform(value)
                except Exception:
                    pass
            result[rule.target_field] = value
        return result

    def bind_list(
        self,
        items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        return [self.bind(item) for item in items]

    def cache(self, key: str, value: Any) -> None:
        self._cache[key] = value

    def get_cached(self, key: str, default: Any = None) -> Any:
        return self._cache.get(key, default)

    def clear_cache(self) -> None:
        self._cache.clear()

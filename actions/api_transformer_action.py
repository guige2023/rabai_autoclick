"""API Transformer Action Module.

Provides request/response transformation with mapping,
extraction, and conditional transformation chains.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Pattern, Union
import logging

logger = logging.getLogger(__name__)


@dataclass
class TransformRule:
    """Single transformation rule."""
    field_path: str
    transform_type: str
    value: Any = None
    condition: Optional[Callable[[Any], bool]] = None


class APITransformerAction:
    """Request/response transformer.

    Example:
        transformer = APITransformerAction()

        transformer.add_rule(TransformRule(
            field_path="user.id",
            transform_type="rename",
            value="userId"
        ))

        result = transformer.transform_response(api_response)
    """

    def __init__(self) -> None:
        self._request_rules: List[TransformRule] = []
        self._response_rules: List[TransformRule] = []

    def add_request_rule(self, rule: TransformRule) -> "APITransformerAction":
        """Add request transformation rule."""
        self._request_rules.append(rule)
        return self

    def add_response_rule(self, rule: TransformRule) -> "APITransformerAction":
        """Add response transformation rule."""
        self._response_rules.append(rule)
        return self

    def transform_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform request data.

        Args:
            data: Request payload

        Returns:
            Transformed data
        """
        result = self._deep_copy(data)

        for rule in self._request_rules:
            if rule.condition and not rule.condition(data):
                continue
            self._apply_rule(result, rule)

        return result

    def transform_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform response data.

        Args:
            data: Response payload

        Returns:
            Transformed data
        """
        result = self._deep_copy(data)

        for rule in self._response_rules:
            if rule.condition and not rule.condition(data):
                continue
            self._apply_rule(result, rule)

        return result

    def _apply_rule(self, data: Dict, rule: TransformRule) -> None:
        """Apply transformation rule to data."""
        path_parts = rule.field_path.split(".")

        if rule.transform_type == "rename":
            self._rename_field(data, path_parts, rule.value)
        elif rule.transform_type == "remove":
            self._remove_field(data, path_parts)
        elif rule.transform_type == "extract":
            self._extract_field(data, path_parts, rule.value)
        elif rule.transform_type == "default":
            self._set_default(data, path_parts, rule.value)
        elif rule.transform_type == "transform":
            self._transform_value(data, path_parts, rule.value)
        elif rule.transform_type == "map":
            self._map_value(data, path_parts, rule.value)

    def _get_nested(self, data: Any, path: List[str]) -> Any:
        """Get nested value from path."""
        current = data
        for part in path:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        return current

    def _set_nested(self, data: Dict, path: List[str], value: Any) -> None:
        """Set nested value at path."""
        current = data
        for part in path[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[path[-1]] = value

    def _rename_field(
        self,
        data: Dict,
        path: List[str],
        new_name: str,
    ) -> None:
        """Rename field."""
        value = self._get_nested(data, path)
        if value is not None:
            self._remove_field(data, path)
            path[-1] = new_name
            self._set_nested(data, path, value)

    def _remove_field(self, data: Dict, path: List[str]) -> None:
        """Remove field."""
        if len(path) == 1:
            data.pop(path[0], None)
        else:
            parent = self._get_nested(data, path[:-1])
            if isinstance(parent, dict):
                parent.pop(path[-1], None)

    def _extract_field(
        self,
        data: Dict,
        path: List[str],
        extract_path: str,
    ) -> None:
        """Extract field to new path."""
        value = self._get_nested(data, path)
        if value is not None and isinstance(value, dict):
            extract_parts = extract_path.split(".")
            target = data
            for part in extract_parts[:-1]:
                if part not in target:
                    target[part] = {}
                target = target[part]
            target[extract_parts[-1]] = value

    def _set_default(
        self,
        data: Dict,
        path: List[str],
        default_value: Any,
    ) -> None:
        """Set default value if field is missing."""
        current = self._get_nested(data, path)
        if current is None:
            self._set_nested(data, path, default_value)

    def _transform_value(
        self,
        data: Dict,
        path: List[str],
        transform_fn: Callable,
    ) -> None:
        """Apply transform function to field."""
        value = self._get_nested(data, path)
        if value is not None:
            try:
                new_value = transform_fn(value)
                self._set_nested(data, path, new_value)
            except Exception as e:
                logger.error(f"Transform failed: {e}")

    def _map_value(
        self,
        data: Dict,
        path: List[str],
        mapping: Dict,
    ) -> None:
        """Map value using mapping dict."""
        value = self._get_nested(data, path)
        if value in mapping:
            self._set_nested(data, path, mapping[value])

    def _deep_copy(self, data: Any) -> Any:
        """Deep copy data structure."""
        if isinstance(data, dict):
            return {k: self._deep_copy(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._deep_copy(item) for item in data]
        else:
            return data

"""API Response Transformer Action.

Transforms, normalizes, and reshapes API responses including
field mapping, type coercion, pagination flattening, and
schema validation.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union


class TransformType(Enum):
    """Types of transformations."""
    MAP_FIELDS = "map_fields"
    RENAME_FIELDS = "rename_fields"
    REMOVE_FIELDS = "remove_fields"
    COERCE_TYPES = "coerce_types"
    FLATTEN = "flatten"
    UNFLATTEN = "unflatten"
    FILTER = "filter"
    EXTRACT = "extract"
    TEMPLATE = "template"


@dataclass
class FieldMapping:
    """A field mapping definition."""
    source_field: str
    target_field: str
    transform_fn: Optional[Callable[[Any], Any]] = None


@dataclass
class TransformRule:
    """A single transformation rule."""
    rule_id: str
    transform_type: TransformType
    config: Dict[str, Any]
    enabled: bool = True
    priority: int = 0


@dataclass
class TransformResult:
    """Result of a transformation operation."""
    success: bool
    data: Any
    transformations_applied: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    transformed_at: datetime = field(default_factory=datetime.now)


class APIResponseTransformerAction:
    """Transforms and normalizes API responses."""

    def __init__(self) -> None:
        self._rules: List[TransformRule] = []
        self._field_mappings: Dict[str, FieldMapping] = {}
        self._type_coercions: Dict[str, Callable] = {}

    def add_mapping(
        self,
        source: str,
        target: str,
        transform_fn: Optional[Callable[[Any], Any]] = None,
    ) -> None:
        """Add a field mapping."""
        self._field_mappings[source] = FieldMapping(
            source_field=source,
            target_field=target,
            transform_fn=transform_fn,
        )

    def add_rule(
        self,
        rule_id: str,
        transform_type: TransformType,
        config: Dict[str, Any],
        priority: int = 0,
    ) -> None:
        """Add a transformation rule."""
        rule = TransformRule(
            rule_id=rule_id,
            transform_type=transform_type,
            config=config,
            priority=priority,
        )
        self._rules.append(rule)
        self._rules.sort(key=lambda r: r.priority)

    def add_type_coercion(
        self,
        field_path: str,
        coerce_fn: Callable[[Any], Any],
    ) -> None:
        """Add a type coercion for a field."""
        self._type_coercions[field_path] = coerce_fn

    def transform(
        self,
        data: Any,
        rules: Optional[List[str]] = None,
    ) -> TransformResult:
        """Apply transformations to data."""
        errors: List[str] = []
        warnings: List[str] = []
        applied: List[str] = []
        current = data

        rules_to_apply = self._rules
        if rules:
            rules_to_apply = [r for r in self._rules if r.rule_id in rules]

        for rule in rules_to_apply:
            if not rule.enabled:
                continue

            try:
                if rule.transform_type == TransformType.MAP_FIELDS:
                    current = self._map_fields(current)
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.RENAME_FIELDS:
                    current = self._rename_fields(current, rule.config)
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.REMOVE_FIELDS:
                    current = self._remove_fields(current, rule.config)
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.COERCE_TYPES:
                    current = self._coerce_types(current)
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.FLATTEN:
                    current = self._flatten(current, rule.config.get("separator", "."))
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.UNFLATTEN:
                    current = self._unflatten(current, rule.config.get("separator", "."))
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.FILTER:
                    current = self._filter(current, rule.config)
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.EXTRACT:
                    current = self._extract(current, rule.config)
                    applied.append(rule.rule_id)
                elif rule.transform_type == TransformType.TEMPLATE:
                    current = self._apply_template(current, rule.config)
                    applied.append(rule.rule_id)

            except Exception as e:
                errors.append(f"{rule.rule_id}: {e}")

        return TransformResult(
            success=len(errors) == 0,
            data=current,
            transformations_applied=applied,
            errors=errors,
            warnings=warnings,
        )

    def _map_fields(self, data: Any) -> Any:
        """Apply field mappings."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                mapping = self._field_mappings.get(key)
                if mapping:
                    new_key = mapping.target_field
                    new_value = mapping.transform_fn(value) if mapping.transform_fn else value
                    result[new_key] = self._map_fields(new_value)
                else:
                    result[key] = self._map_fields(value)
            return result
        elif isinstance(data, list):
            return [self._map_fields(item) for item in data]
        return data

    def _rename_fields(
        self,
        data: Any,
        config: Dict[str, Any],
    ) -> Any:
        """Rename fields based on config."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                new_key = config.get(key, key)
                result[new_key] = self._rename_fields(value, config)
            return result
        elif isinstance(data, list):
            return [self._rename_fields(item, config) for item in data]
        return data

    def _remove_fields(
        self,
        data: Any,
        config: Dict[str, Any],
    ) -> Any:
        """Remove specified fields."""
        fields_to_remove: Set[str] = set(config.get("fields", []))
        patterns: List[str] = config.get("patterns", [])

        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in fields_to_remove:
                    continue
                skip = False
                for pattern in patterns:
                    if re.search(pattern, key):
                        skip = True
                        break
                if skip:
                    continue
                result[key] = self._remove_fields(value, config)
            return result
        elif isinstance(data, list):
            return [self._remove_fields(item, config) for item in data]
        return data

    def _coerce_types(self, data: Any) -> Any:
        """Apply type coercions."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if key in self._type_coercions:
                    try:
                        result[key] = self._type_coercions[key](value)
                    except Exception:
                        result[key] = value
                else:
                    result[key] = self._coerce_types(value)
            return result
        elif isinstance(data, list):
            return [self._coerce_types(item) for item in data]
        return data

    def _flatten(
        self,
        data: Any,
        separator: str = ".",
    ) -> Any:
        """Flatten nested structures."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    flattened = self._flatten(value, separator)
                    for sub_key, sub_value in flattened.items():
                        result[f"{key}{separator}{sub_key}"] = sub_value
                else:
                    result[key] = value
            return result
        elif isinstance(data, list):
            return [self._flatten(item, separator) for item in data]
        return data

    def _unflatten(
        self,
        data: Any,
        separator: str = ".",
    ) -> Any:
        """Unflatten a flat structure."""
        if isinstance(data, dict):
            result: Dict[str, Any] = {}
            for key, value in data.items():
                parts = key.split(separator)
                current = result
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = value
            return result
        elif isinstance(data, list):
            return [self._unflatten(item, separator) for item in data]
        return data

    def _filter(
        self,
        data: Any,
        config: Dict[str, Any],
    ) -> Any:
        """Filter data based on conditions."""
        condition: Callable[[Any], bool] = config.get("condition", lambda x: True)

        if isinstance(data, list):
            filtered = [item for item in data if self._filter(item, config) if condition(item)]
            return filtered
        elif isinstance(data, dict):
            return {k: v for k, v in data.items() if condition(v)}
        return data

    def _extract(
        self,
        data: Any,
        config: Dict[str, Any],
    ) -> Any:
        """Extract specific fields or paths."""
        paths: List[str] = config.get("paths", [])

        if isinstance(data, dict):
            result = {}
            for path in paths:
                value = self._get_nested(data, path)
                if value is not None:
                    result[path] = value
            return result if result else data
        return data

    def _get_nested(self, data: Any, path: str) -> Any:
        """Get a nested value using dot notation."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list):
                try:
                    idx = int(key)
                    current = current[idx]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        return current

    def _apply_template(
        self,
        data: Any,
        config: Dict[str, Any],
    ) -> Any:
        """Apply a template to generate new fields."""
        template: str = config.get("template", "")
        output_field: str = config.get("output_field", "generated")

        if isinstance(data, dict):
            result = dict(data)
            try:
                result[output_field] = template.format(**data)
            except (KeyError, IndexError):
                pass
            return result
        elif isinstance(data, list):
            return [{**item, output_field: template.format(**item)} if isinstance(item, dict) else item for item in data]
        return data

    def enable_rule(self, rule_id: str) -> bool:
        """Enable a transformation rule."""
        for rule in self._rules:
            if rule.rule_id == rule_id:
                rule.enabled = True
                return True
        return False

    def disable_rule(self, rule_id: str) -> bool:
        """Disable a transformation rule."""
        for rule in self._rules:
            if rule.rule_id == rule_id:
                rule.enabled = False
                return True
        return False

"""Data Transformer Action Module.

Transform data between different schemas and formats.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
import jsonpath_ng


@dataclass
class FieldMapping:
    """Field mapping definition."""
    source_field: str
    target_field: str
    transform: Callable[[Any], Any] | None = None
    default: Any = None
    required: bool = False


@dataclass
class TransformRule:
    """Transformation rule for a record."""
    mappings: list[FieldMapping]
    computed_fields: list[tuple[str, Callable[[dict], Any]]] = field(default_factory=list)
    filter_func: Callable[[dict], bool] | None = None


class DataTransformer:
    """Transform data records between schemas."""

    def __init__(self) -> None:
        self._rules: dict[str, TransformRule] = {}

    def register_rule(self, name: str, rule: TransformRule) -> None:
        """Register a transformation rule."""
        self._rules[name] = rule

    def transform(self, data: dict, rule_name: str) -> dict | None:
        """Transform a single record using a named rule."""
        rule = self._rules.get(rule_name)
        if not rule:
            raise ValueError(f"Unknown transform rule: {rule_name}")
        if rule.filter_func and not rule.filter_func(data):
            return None
        result = {}
        for mapping in rule.mappings:
            value = data.get(mapping.source_field, mapping.default)
            if mapping.transform and value is not None:
                try:
                    value = mapping.transform(value)
                except Exception:
                    value = mapping.default
            if value is None and mapping.required:
                return None
            result[mapping.target_field] = value
        for field_name, compute_fn in rule.computed_fields:
            try:
                result[field_name] = compute_fn(data)
            except Exception:
                pass
        return result

    def transform_batch(self, data: list[dict], rule_name: str) -> list[dict]:
        """Transform multiple records."""
        return [r for r in (self.transform(d, rule_name) for d in data) if r is not None]


class JSONPathExtractor:
    """Extract data using JSONPath expressions."""

    def __init__(self) -> None:
        self._compiled: dict[str, jsonpath_ng.JSONPath] = {}

    def compile(self, expression: str) -> jsonpath_ng.JSONPath:
        """Compile a JSONPath expression."""
        if expression not in self._compiled:
            self._compiled[expression] = jsonpath_ng.parse(expression)
        return self._compiled[expression]

    def extract(self, data: dict, expression: str, multiple: bool = False) -> Any:
        """Extract values using JSONPath."""
        compiled = self.compile(expression)
        matches = [m.value for m in compiled.find(data)]
        if multiple:
            return matches
        return matches[0] if matches else None

    def extract_all(self, data: dict, expressions: dict[str, str]) -> dict:
        """Extract multiple fields using JSONPath expressions."""
        result = {}
        for field_name, expression in expressions.items():
            result[field_name] = self.extract(data, expression)
        return result


class SchemaConverter:
    """Convert data between different schema formats."""

    @staticmethod
    def flatten(data: dict, separator: str = ".") -> dict:
        """Flatten nested dictionary."""
        result = {}
        def _flatten(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    _flatten(v, f"{prefix}{separator}{k}" if prefix else k)
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    _flatten(v, f"{prefix}[{i}]")
            else:
                result[prefix] = obj
        _flatten(data)
        return result

    @staticmethod
    def unflatten(data: dict, separator: str = ".") -> dict:
        """Unflatten dictionary to nested structure."""
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

    @staticmethod
    def rename_keys(data: dict, key_mapping: dict[str, str]) -> dict:
        """Rename keys in a dictionary."""
        return {key_mapping.get(k, k): v for k, v in data.items()}

    @staticmethod
    def select_fields(data: dict, fields: list[str]) -> dict:
        """Select specific fields from dictionary."""
        return {k: v for k, v in data.items() if k in fields}

    @staticmethod
    def omit_fields(data: dict, fields: list[str]) -> dict:
        """Omit specific fields from dictionary."""
        return {k: v for k, v in data.items() if k not in fields}

# Copyright (c) 2024. coded by claude
"""Data Transform Action Module.

Provides data transformation utilities including mapping, filtering,
aggregation, and schema validation for API data processing.
"""
from typing import Optional, Dict, Any, List, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class TransformRule:
    source_field: str
    target_field: str
    transformer: Optional[Callable[[Any], Any]] = None
    default_value: Any = None
    required: bool = False


@dataclass
class TransformResult:
    success: bool
    data: Optional[Dict[str, Any]]
    errors: List[str] = field(default_factory=list)


class DataTransformer:
    def __init__(self, rules: Optional[List[TransformRule]] = None):
        self.rules = rules or []

    def add_rule(self, rule: TransformRule) -> None:
        self.rules.append(rule)

    def transform(self, data: Dict[str, Any]) -> TransformResult:
        result: Dict[str, Any] = {}
        errors: List[str] = []

        for rule in self.rules:
            value = data.get(rule.source_field)
            if value is None:
                if rule.required:
                    errors.append(f"Required field '{rule.source_field}' is missing")
                    continue
                value = rule.default_value
            if rule.transformer and value is not None:
                try:
                    value = rule.transformer(value)
                except Exception as e:
                    errors.append(f"Transform failed for '{rule.source_field}': {e}")
                    continue
            result[rule.target_field] = value

        return TransformResult(
            success=len(errors) == 0,
            data=result if not errors or any(rule.target_field in result for rule in self.rules) else None,
            errors=errors,
        )


class DataFilter:
    @staticmethod
    def filter_dict(data: Dict[str, Any], predicate: Callable[[str, Any], bool]) -> Dict[str, Any]:
        return {k: v for k, v in data.items() if predicate(k, v)}

    @staticmethod
    def filter_list(data: List[Any], predicate: Callable[[Any], bool]) -> List[Any]:
        return [item for item in data if predicate(item)]

    @staticmethod
    def flatten_dict(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict):
                for sub_key, sub_value in DataFilter.flatten_dict(value, separator).items():
                    result[f"{key}{separator}{sub_key}"] = sub_value
            else:
                result[key] = value
        return result

    @staticmethod
    def unflatten_dict(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
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

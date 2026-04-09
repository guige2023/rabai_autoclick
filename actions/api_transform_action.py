"""
API Transform Action Module.

Provides request/response transformation with mapping,
filtering, and enrichment capabilities.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import json
import re


class TransformType(Enum):
    """Transform types."""
    MAP = "map"
    FILTER = "filter"
    ENRICH = "enrich"
    VALIDATE = "validate"
    NORMALIZE = "normalize"


@dataclass
class FieldMapping:
    """Field mapping definition."""
    source_field: str
    target_field: str
    transform_func: Optional[Callable[[Any], Any]] = None
    default_value: Any = None
    required: bool = False


@dataclass
class TransformRule:
    """Transform rule definition."""
    name: str
    field_mappings: list[FieldMapping]
    filter_func: Optional[Callable[[dict], bool]] = None
    enrich_func: Optional[Callable[[dict], dict]] = None
    pre_transform: Optional[Callable] = None
    post_transform: Optional[Callable] = None


@dataclass
class TransformResult:
    """Result of transformation."""
    success: bool
    data: Any = None
    errors: list[str] = field(default_factory=list)
    transformed_fields: int = 0
    dropped_fields: int = 0


class ValueNormalizer:
    """Value normalization utilities."""

    @staticmethod
    def normalize_string(value: Any) -> str:
        """Normalize string value."""
        if value is None:
            return ""
        return str(value).strip().lower()

    @staticmethod
    def normalize_number(value: Any) -> Optional[float]:
        """Normalize numeric value."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def normalize_boolean(value: Any) -> bool:
        """Normalize boolean value."""
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    @staticmethod
    def normalize_list(value: Any) -> list:
        """Normalize to list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]


class APIRequestTransformer:
    """Transform API requests."""

    def __init__(self, rules: list[TransformRule]):
        self.rules = rules
        self._normalizer = ValueNormalizer()

    def _get_nested_value(self, data: dict, path: str) -> Any:
        """Get nested value using dot notation."""
        parts = path.split(".")
        value = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _set_nested_value(self, data: dict, path: str, value: Any) -> None:
        """Set nested value using dot notation."""
        parts = path.split(".")
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    async def transform(self, data: dict, rule_name: Optional[str] = None) -> TransformResult:
        """Transform request data."""
        result = TransformResult(success=True)

        rules_to_apply = [
            r for r in self.rules
            if rule_name is None or r.name == rule_name
        ]

        current_data = data.copy()

        for rule in rules_to_apply:
            if rule.pre_transform:
                current_data = rule.pre_transform(current_data)
                if not isinstance(current_data, dict):
                    result.errors.append(f"pre_transform returned non-dict for rule {rule.name}")
                    result.success = False
                    continue

            transformed_data = {}
            dropped = 0

            for mapping in rule.field_mappings:
                source_value = self._get_nested_value(current_data, mapping.source_field)

                if source_value is None:
                    if mapping.required:
                        result.errors.append(f"Required field {mapping.source_field} is missing")
                        result.success = False
                    if mapping.default_value is not None:
                        source_value = mapping.default_value
                    else:
                        continue

                if mapping.transform_func:
                    try:
                        if asyncio.iscoroutinefunction(mapping.transform_func):
                            source_value = await mapping.transform_func(source_value)
                        else:
                            source_value = mapping.transform_func(source_value)
                    except Exception as e:
                        result.errors.append(f"Transform error for {mapping.source_field}: {e}")
                        result.success = False
                        continue

                self._set_nested_value(transformed_data, mapping.target_field, source_value)
                result.transformed_fields += 1

            if rule.filter_func:
                try:
                    if rule.filter_func(transformed_data):
                        current_data = transformed_data
                    else:
                        result.success = False
                        result.errors.append(f"Filter rejected document in rule {rule.name}")
                        continue
                except Exception as e:
                    result.errors.append(f"Filter error: {e}")
                    result.success = False

            if rule.enrich_func:
                try:
                    if asyncio.iscoroutinefunction(rule.enrich_func):
                        transformed_data = await rule.enrich_func(transformed_data)
                    else:
                        transformed_data = rule.enrich_func(transformed_data)
                except Exception as e:
                    result.errors.append(f"Enrich error: {e}")
                    result.success = False

            current_data = transformed_data

            if rule.post_transform:
                current_data = rule.post_transform(current_data)

        result.data = current_data
        return result


class APIResponseTransformer:
    """Transform API responses."""

    def __init__(self, rules: list[TransformRule]):
        self.rules = rules
        self._normalizer = ValueNormalizer()

    async def transform(self, response: Any, rule_name: Optional[str] = None) -> TransformResult:
        """Transform response data."""
        result = TransformResult(success=True)

        if isinstance(response, dict):
            data = response
        elif isinstance(response, list):
            data = {"items": response, "_is_array": True}
        else:
            result.errors.append(f"Unsupported response type: {type(response)}")
            result.success = False
            return result

        rules_to_apply = [
            r for r in self.rules
            if rule_name is None or r.name == rule_name
        ]

        for rule in rules_to_apply:
            transformed_items = []

            if "_is_array" in data and isinstance(data.get("items"), list):
                for item in data["items"]:
                    transformed = self._apply_rule(item, rule)
                    if transformed:
                        transformed_items.append(transformed)
                data["items"] = transformed_items
            else:
                data = self._apply_rule(data, rule)

        result.data = data
        return result

    def _apply_rule(self, data: dict, rule: TransformRule) -> dict:
        """Apply single rule to data."""
        result_data = data.copy()

        for mapping in rule.field_mappings:
            source_value = data.get(mapping.source_field)
            if source_value is None and mapping.default_value is not None:
                source_value = mapping.default_value

            if mapping.transform_func:
                try:
                    source_value = mapping.transform_func(source_value)
                except Exception:
                    pass

            result_data[mapping.target_field] = source_value

        return result_data


class APITransformAction:
    """
    Request/response transformation with mapping and enrichment.

    Example:
        rules = [
            TransformRule(
                name="user_request",
                field_mappings=[
                    FieldMapping("userId", "user_id", required=True),
                    FieldMapping("name", "display_name"),
                    FieldMapping("email", "email_address"),
                ]
            )
        ]

        transformer = APITransformAction(rules=rules)
        result = await transformer.transform_request(user_data, rule_name="user_request")
    """

    def __init__(self, rules: Optional[list[TransformRule]] = None):
        self.rules = rules or []
        self._request_transformer = APIRequestTransformer(self.rules)
        self._response_transformer = APIResponseTransformer(self.rules)

    def add_rule(self, rule: TransformRule) -> "APITransformAction":
        """Add transform rule."""
        self.rules.append(rule)
        return self

    async def transform_request(
        self,
        data: dict,
        rule_name: Optional[str] = None
    ) -> TransformResult:
        """Transform request."""
        return await self._request_transformer.transform(data, rule_name)

    async def transform_response(
        self,
        response: Any,
        rule_name: Optional[str] = None
    ) -> TransformResult:
        """Transform response."""
        return await self._response_transformer.transform(response, rule_name)

    def add_field_mapping(
        self,
        source_field: str,
        target_field: str,
        rule_name: str,
        **kwargs: Any
    ) -> "APITransformAction":
        """Add field mapping to existing or new rule."""
        mapping = FieldMapping(
            source_field=source_field,
            target_field=target_field,
            **kwargs
        )

        for rule in self.rules:
            if rule.name == rule_name:
                rule.field_mappings.append(mapping)
                return self

        new_rule = TransformRule(
            name=rule_name,
            field_mappings=[mapping]
        )
        self.rules.append(new_rule)
        return self

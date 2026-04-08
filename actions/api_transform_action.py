"""API Transform Action Module.

Transforms API requests and responses with mapping,
filtering, and enrichment operations.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class TransformRule:
    """A transformation rule."""
    name: str
    source_path: str
    target_path: str
    transform_type: str
    config: Dict[str, Any] = field(default_factory=dict)


class APITransformAction(BaseAction):
    """
    API request/response transformation.

    Transforms API payloads with field mapping,
    filtering, and custom transformations.

    Example:
        transformer = APITransformAction()
        result = transformer.execute(ctx, {"action": "transform", "data": {"a": 1}, "rules": [{"name": "rename", "source_path": "a", "target_path": "b"}]})
    """
    action_type = "api_transform"
    display_name = "API转换"
    description = "API请求/响应转换和映射"

    def __init__(self) -> None:
        super().__init__()
        self._rules: List[TransformRule] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "transform":
                return self._transform(params)
            elif action == "add_rule":
                return self._add_rule(params)
            elif action == "map_fields":
                return self._map_fields(params)
            elif action == "filter_fields":
                return self._filter_fields(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {str(e)}")

    def _transform(self, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", {})
        rules = params.get("rules", [])

        if not isinstance(data, dict):
            return ActionResult(success=False, message="data must be a dictionary")

        result = dict(data)

        for rule_data in rules:
            rule = TransformRule(name=rule_data.get("name", ""), source_path=rule_data.get("source_path", ""), target_path=rule_data.get("target_path", ""), transform_type=rule_data.get("type", "copy"), config=rule_data.get("config", {}))
            result = self._apply_rule(result, rule)

        return ActionResult(success=True, message="Transformation complete", data={"result": result})

    def _apply_rule(self, data: Dict[str, Any], rule: TransformRule) -> Dict[str, Any]:
        result = dict(data)

        if rule.transform_type == "copy":
            value = self._get_nested_value(data, rule.source_path)
            self._set_nested_value(result, rule.target_path, value)

        elif rule.transform_type == "rename":
            value = self._get_nested_value(data, rule.source_path)
            self._delete_nested_value(result, rule.source_path)
            self._set_nested_value(result, rule.target_path, value)

        elif rule.transform_type == "delete":
            self._delete_nested_value(result, rule.source_path)

        elif rule.transform_type == "static":
            self._set_nested_value(result, rule.target_path, rule.config.get("value", ""))

        elif rule.transform_type == "merge":
            source_value = self._get_nested_value(data, rule.source_path, {})
            target_value = self._get_nested_value(result, rule.target_path, {})
            if isinstance(source_value, dict) and isinstance(target_value, dict):
                target_value.update(source_value)
                self._set_nested_value(result, rule.target_path, target_value)

        return result

    def _get_nested_value(self, data: Dict[str, Any], path: str, default: Any = None) -> Any:
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value if value is not None else default

    def _set_nested_value(self, data: Dict[str, Any], path: str, value: Any) -> None:
        keys = path.split(".")
        current = data
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    def _delete_nested_value(self, data: Dict[str, Any], path: str) -> None:
        keys = path.split(".")
        current = data
        for key in keys[:-1]:
            if key not in current:
                return
            current = current[key]
        if keys[-1] in current:
            del current[keys[-1]]

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        source = params.get("source_path", "")
        target = params.get("target_path", "")
        rule_type = params.get("type", "copy")

        if not name:
            return ActionResult(success=False, message="name is required")

        rule = TransformRule(name=name, source_path=source, target_path=target, transform_type=rule_type)
        self._rules.append(rule)

        return ActionResult(success=True, message=f"Rule added: {name}")

    def _map_fields(self, params: Dict[str, Any]) -> ActionResult:
        mappings = params.get("mappings", {})
        data = params.get("data", {})

        result = {}
        for source, target in mappings.items():
            if source in data:
                result[target] = data[source]

        return ActionResult(success=True, data={"result": result})

    def _filter_fields(self, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", {})
        include = params.get("include", [])
        exclude = params.get("exclude", [])

        if include:
            result = {k: v for k, v in data.items() if k in include}
        elif exclude:
            result = {k: v for k, v in data.items() if k not in exclude}
        else:
            result = dict(data)

        return ActionResult(success=True, data={"result": result})

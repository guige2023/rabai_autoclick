"""JSON9 action module for RabAI AutoClick.

Provides additional JSON operations:
- JSONParseAction: Parse JSON string
- JSONStringifyAction: Convert to JSON string
- JSONGetAction: Get value from JSON
- JSONSetAction: Set value in JSON
- JSONKeysAction: Get JSON keys
- JSONValuesAction: Get JSON values
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JSONParseAction(BaseAction):
    """Parse JSON string."""
    action_type = "json9_parse"
    display_name = "解析JSON"
    description = "解析JSON字符串"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON parse.

        Args:
            context: Execution context.
            params: Dict with json_str, output_var.

        Returns:
            ActionResult with parsed JSON.
        """
        json_str = params.get('json_str', '')
        output_var = params.get('output_var', 'parsed_json')

        try:
            import json

            resolved = context.resolve_value(json_str)

            if isinstance(resolved, str):
                result = json.loads(resolved)
            else:
                result = resolved

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"解析JSON成功",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except json.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"JSON解析失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_json'}


class JSONStringifyAction(BaseAction):
    """Convert to JSON string."""
    action_type = "json9_stringify"
    display_name = "转换为JSON"
    description = "将对象转换为JSON字符串"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON stringify.

        Args:
            context: Execution context.
            params: Dict with obj, indent, output_var.

        Returns:
            ActionResult with JSON string.
        """
        obj = params.get('obj', None)
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'json_string')

        try:
            import json

            resolved = context.resolve_value(obj) if obj is not None else None
            resolved_indent = int(context.resolve_value(indent)) if indent else 2

            result = json.dumps(resolved, indent=resolved_indent, ensure_ascii=False)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为JSON: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2, 'output_var': 'json_string'}


class JSONGetAction(BaseAction):
    """Get value from JSON."""
    action_type = "json9_get"
    display_name = "JSON取值"
    description = "从JSON获取值"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON get.

        Args:
            context: Execution context.
            params: Dict with json_obj, path, output_var.

        Returns:
            ActionResult with value.
        """
        json_obj = params.get('json_obj', {})
        path = params.get('path', '')
        output_var = params.get('output_var', 'json_value')

        try:
            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                import json
                resolved = json.loads(resolved)

            resolved_path = context.resolve_value(path) if path else ''

            if resolved_path:
                parts = resolved_path.split('.')
                result = resolved
                for part in parts:
                    if isinstance(result, dict):
                        result = result.get(part)
                    elif isinstance(result, list):
                        try:
                            result = result[int(part)]
                        except (ValueError, IndexError):
                            result = None
                    else:
                        result = None
            else:
                result = resolved

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON取值: {resolved_path}",
                data={
                    'json_obj': resolved,
                    'path': resolved_path,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON取值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '', 'output_var': 'json_value'}


class JSONSetAction(BaseAction):
    """Set value in JSON."""
    action_type = "json9_set"
    display_name = "JSON设值"
    description = "在JSON中设置值"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON set.

        Args:
            context: Execution context.
            params: Dict with json_obj, path, value, output_var.

        Returns:
            ActionResult with modified JSON.
        """
        json_obj = params.get('json_obj', {})
        path = params.get('path', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'modified_json')

        try:
            import copy

            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                import json
                resolved = json.loads(resolved)

            resolved = copy.deepcopy(resolved)
            resolved_path = context.resolve_value(path) if path else ''
            resolved_value = context.resolve_value(value) if value is not None else None

            if resolved_path:
                parts = resolved_path.split('.')
                current = resolved
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = resolved_value
            else:
                resolved = resolved_value

            context.set(output_var, resolved)

            return ActionResult(
                success=True,
                message=f"JSON设值: {resolved_path}",
                data={
                    'path': resolved_path,
                    'value': resolved_value,
                    'result': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON设值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_obj', 'path', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'modified_json'}


class JSONKeysAction(BaseAction):
    """Get JSON keys."""
    action_type = "json9_keys"
    display_name = "JSON键"
    description = "获取JSON对象的所有键"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON keys.

        Args:
            context: Execution context.
            params: Dict with json_obj, output_var.

        Returns:
            ActionResult with keys.
        """
        json_obj = params.get('json_obj', {})
        output_var = params.get('output_var', 'json_keys')

        try:
            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                import json
                resolved = json.loads(resolved)

            if isinstance(resolved, dict):
                result = list(resolved.keys())
            elif isinstance(resolved, (list, tuple)):
                result = list(range(len(resolved)))
            else:
                result = []

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON键: {len(result)}个",
                data={
                    'json_obj': resolved,
                    'keys': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取JSON键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_keys'}


class JSONValuesAction(BaseAction):
    """Get JSON values."""
    action_type = "json9_values"
    display_name = "JSON值"
    description = "获取JSON对象的所有值"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON values.

        Args:
            context: Execution context.
            params: Dict with json_obj, output_var.

        Returns:
            ActionResult with values.
        """
        json_obj = params.get('json_obj', {})
        output_var = params.get('output_var', 'json_values')

        try:
            resolved = context.resolve_value(json_obj)

            if isinstance(resolved, str):
                import json
                resolved = json.loads(resolved)

            if isinstance(resolved, dict):
                result = list(resolved.values())
            elif isinstance(resolved, (list, tuple)):
                result = list(resolved)
            else:
                result = [resolved]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON值: {len(result)}个",
                data={
                    'json_obj': resolved,
                    'values': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取JSON值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_values'}
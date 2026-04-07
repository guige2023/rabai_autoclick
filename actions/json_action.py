"""JSON action module for RabAI AutoClick.

Provides JSON operations:
- ParseJsonAction: Parse JSON string
- ToJsonAction: Convert to JSON string
- GetJsonValueAction: Get value from JSON by path
- SetJsonValueAction: Set value in JSON
"""

import json
from typing import Any, Dict, List, Optional, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ParseJsonAction(BaseAction):
    """Parse a JSON string."""
    action_type = "parse_json"
    display_name = "解析JSON"
    description = "解析JSON字符串为对象"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parsing JSON.

        Args:
            context: Execution context.
            params: Dict with json_string, output_var.

        Returns:
            ActionResult with parsed object.
        """
        json_string = params.get('json_string', '')
        output_var = params.get('output_var', 'parsed_json')

        # Validate json_string
        if not json_string:
            return ActionResult(
                success=False,
                message="未指定JSON字符串"
            )
        valid, msg = self.validate_type(json_string, str, 'json_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_string = context.resolve_value(json_string)
            parsed = json.loads(resolved_string)

            # Store in context
            context.set(output_var, parsed)

            return ActionResult(
                success=True,
                message=f"JSON解析成功: {type(parsed).__name__}",
                data={
                    'parsed': parsed,
                    'type': type(parsed).__name__,
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
                message=f"解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_json'}


class ToJsonAction(BaseAction):
    """Convert an object to JSON string."""
    action_type = "to_json"
    display_name = "转换为JSON"
    description = "将对象转换为JSON字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute converting to JSON.

        Args:
            context: Execution context.
            params: Dict with data, pretty, output_var.

        Returns:
            ActionResult with JSON string.
        """
        data = params.get('data', None)
        pretty = params.get('pretty', True)
        output_var = params.get('output_var', 'json_string')

        if data is None:
            return ActionResult(
                success=False,
                message="未指定数据"
            )

        valid, msg = self.validate_type(pretty, bool, 'pretty')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)

            if pretty:
                json_string = json.dumps(resolved_data, ensure_ascii=False, indent=2)
            else:
                json_string = json.dumps(resolved_data, ensure_ascii=False)

            # Store in context
            context.set(output_var, json_string)

            return ActionResult(
                success=True,
                message=f"转换为JSON成功: {len(json_string)} 字符",
                data={
                    'json': json_string,
                    'length': len(json_string),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'pretty': True,
            'output_var': 'json_string'
        }


class GetJsonValueAction(BaseAction):
    """Get a value from JSON by path."""
    action_type = "get_json_value"
    display_name = "获取JSON值"
    description = "从JSON对象中获取指定路径的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting JSON value.

        Args:
            context: Execution context.
            params: Dict with json_data, path, default, output_var.

        Returns:
            ActionResult with value at path.
        """
        json_data = params.get('json_data', None)
        path = params.get('path', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'json_value')

        # Validate json_data
        if json_data is None:
            return ActionResult(
                success=False,
                message="未指定JSON数据"
            )

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(json_data)

            # Navigate the path
            value = resolved_data
            for key in path.split('.'):
                if not key:
                    continue

                if isinstance(value, dict):
                    value = value.get(key)
                elif isinstance(value, list):
                    try:
                        index = int(key)
                        value = value[index]
                    except (ValueError, IndexError):
                        value = default
                else:
                    value = default
                    break

                if value is None:
                    value = default
                    break

            # Store in context
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"获取JSON值: {path} = {repr(value)[:50]}",
                data={
                    'path': path,
                    'value': value,
                    'found': value is not None,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取JSON值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'path': '',
            'default': None,
            'output_var': 'json_value'
        }


class SetJsonValueAction(BaseAction):
    """Set a value in JSON object."""
    action_type = "set_json_value"
    display_name = "设置JSON值"
    description = "在JSON对象中设置指定路径的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute setting JSON value.

        Args:
            context: Execution context.
            params: Dict with json_data, path, value, output_var.

        Returns:
            ActionResult with modified JSON.
        """
        json_data = params.get('json_data', {})
        path = params.get('path', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'json_result')

        # Validate json_data
        valid, msg = self.validate_type(json_data, (dict, list), 'json_data')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(json_data)
            resolved_value = context.resolve_value(value) if value is not None else None

            # Deep copy to avoid modifying original
            import copy
            result = copy.deepcopy(resolved_data)

            # Navigate to parent
            if path:
                keys = path.split('.')
                current = result

                for key in keys[:-1]:
                    if isinstance(current, dict):
                        if key not in current:
                            current[key] = {}
                        current = current[key]
                    elif isinstance(current, list):
                        try:
                            index = int(key)
                            if index >= len(current):
                                current.extend([{}] * (index - len(current) + 1))
                            current = current[index]
                        except ValueError:
                            return ActionResult(
                                success=False,
                                message=f"无效的路径: {path}"
                            )
                    else:
                        return ActionResult(
                            success=False,
                            message=f"无法在路径 {path} 处设置值"
                        )

                # Set the value
                final_key = keys[-1]
                if isinstance(current, dict):
                    current[final_key] = resolved_value
                elif isinstance(current, list):
                    try:
                        index = int(final_key)
                        if index >= len(current):
                            current.extend([None] * (index - len(current) + 1))
                        current[index] = resolved_value
                    except ValueError:
                        return ActionResult(
                            success=False,
                            message=f"无效的数组索引: {final_key}"
                        )
            else:
                result = resolved_value

            # Store in context
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"已设置JSON值: {path}",
                data={
                    'path': path,
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置JSON值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'path': '',
            'value': None,
            'output_var': 'json_result'
        }
"""Json4 action module for RabAI AutoClick.

Provides additional JSON operations:
- JsonToPythonAction: JSON to Python object
- JsonFromPythonAction: Python object to JSON
- JsonGetValueAction: Get value by key path
- JsonSetValueAction: Set value by key path
- JsonHasKeyAction: Check if key exists
"""

import json
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JsonToPythonAction(BaseAction):
    """JSON to Python object."""
    action_type = "json4_to_python"
    display_name = "JSON转Python"
    description = "将JSON字符串转换为Python对象"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON to Python.

        Args:
            context: Execution context.
            params: Dict with json_str, output_var.

        Returns:
            ActionResult with Python object.
        """
        json_str = params.get('json_str', '')
        output_var = params.get('output_var', 'python_obj')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(json_str)
            result = json.loads(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON转Python完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON转Python失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'python_obj'}


class JsonFromPythonAction(BaseAction):
    """Python object to JSON."""
    action_type = "json4_from_python"
    display_name = "Python转JSON"
    description = "将Python对象转换为JSON字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Python to JSON.

        Args:
            context: Execution context.
            params: Dict with python_obj, indent, output_var.

        Returns:
            ActionResult with JSON string.
        """
        python_obj = params.get('python_obj', None)
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'json_str')

        try:
            resolved = context.resolve_value(python_obj)
            resolved_indent = int(context.resolve_value(indent)) if indent else 2

            result = json.dumps(resolved, indent=resolved_indent, ensure_ascii=False)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Python转JSON完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Python转JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['python_obj']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2, 'output_var': 'json_str'}


class JsonGetValueAction(BaseAction):
    """Get value by key path."""
    action_type = "json4_get_value"
    display_name = "JSON获取值"
    description = "通过键路径获取JSON中的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get value.

        Args:
            context: Execution context.
            params: Dict with json_str, key_path, output_var.

        Returns:
            ActionResult with value.
        """
        json_str = params.get('json_str', '')
        key_path = params.get('key_path', '')
        output_var = params.get('output_var', 'json_value')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_json = context.resolve_value(json_str)
            resolved_path = context.resolve_value(key_path) if key_path else ''

            obj = json.loads(resolved_json)

            if resolved_path:
                keys = resolved_path.split('.')
                for key in keys:
                    if isinstance(obj, dict):
                        obj = obj.get(key)
                    elif isinstance(obj, list) and key.isdigit():
                        obj = obj[int(key)]
                    else:
                        obj = None
                        break

            context.set(output_var, obj)

            return ActionResult(
                success=True,
                message=f"JSON获取值完成",
                data={
                    'key_path': resolved_path,
                    'result': obj,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON获取值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key_path': '', 'output_var': 'json_value'}


class JsonSetValueAction(BaseAction):
    """Set value by key path."""
    action_type = "json4_set_value"
    display_name = "JSON设置值"
    description = "通过键路径设置JSON中的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set value.

        Args:
            context: Execution context.
            params: Dict with json_str, key_path, value, output_var.

        Returns:
            ActionResult with updated JSON.
        """
        json_str = params.get('json_str', '')
        key_path = params.get('key_path', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'updated_json')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_json = context.resolve_value(json_str)
            resolved_path = context.resolve_value(key_path) if key_path else ''
            resolved_value = context.resolve_value(value) if value is not None else None

            obj = json.loads(resolved_json)

            if resolved_path:
                keys = resolved_path.split('.')
                current = obj
                for key in keys[:-1]:
                    if isinstance(current, dict):
                        current = current.get(key)
                    elif isinstance(current, list) and key.isdigit():
                        current = current[int(key)]
                    else:
                        current = None
                        break

                if current is not None:
                    last_key = keys[-1]
                    if isinstance(current, dict):
                        current[last_key] = resolved_value
                    elif isinstance(current, list) and last_key.isdigit():
                        current[int(last_key)] = resolved_value

            result = json.dumps(obj, ensure_ascii=False)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON设置值完成",
                data={
                    'key_path': resolved_path,
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON设置值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str', 'key_path', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'updated_json'}


class JsonHasKeyAction(BaseAction):
    """Check if key exists."""
    action_type = "json4_has_key"
    display_name = "JSON键存在检查"
    description = "检查JSON中键路径是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute has key.

        Args:
            context: Execution context.
            params: Dict with json_str, key_path, output_var.

        Returns:
            ActionResult with existence check.
        """
        json_str = params.get('json_str', '')
        key_path = params.get('key_path', '')
        output_var = params.get('output_var', 'has_key_result')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_json = context.resolve_value(json_str)
            resolved_path = context.resolve_value(key_path) if key_path else ''

            obj = json.loads(resolved_json)
            exists = True

            if resolved_path:
                keys = resolved_path.split('.')
                for key in keys:
                    if isinstance(obj, dict):
                        if key in obj:
                            obj = obj[key]
                        else:
                            exists = False
                            break
                    elif isinstance(obj, list) and key.isdigit():
                        idx = int(key)
                        if 0 <= idx < len(obj):
                            obj = obj[idx]
                        else:
                            exists = False
                            break
                    else:
                        exists = False
                        break

            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"JSON键存在: {'是' if exists else '否'}",
                data={
                    'key_path': resolved_path,
                    'exists': exists,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON键存在检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str', 'key_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'has_key_result'}

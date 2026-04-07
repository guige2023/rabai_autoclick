"""JSON5 action module for RabAI AutoClick.

Provides additional JSON operations:
- JSONMinifyAction: Minify JSON
- JSONPrettyAction: Pretty print JSON
- JSONValidateAction: Validate JSON
- JSONMergeAction: Merge JSON objects
- JSONPathAction: Get JSON path value
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JSONMinifyAction(BaseAction):
    """Minify JSON."""
    action_type = "json5_minify"
    display_name = "压缩JSON"
    description = "压缩JSON字符串"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON minify.

        Args:
            context: Execution context.
            params: Dict with json_string, output_var.

        Returns:
            ActionResult with minified JSON.
        """
        json_string = params.get('json_string', '')
        output_var = params.get('output_var', 'minified_json')

        try:
            import json

            resolved = context.resolve_value(json_string)

            parsed = json.loads(resolved)
            result = json.dumps(parsed, separators=(',', ':'))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON压缩成功",
                data={
                    'minified': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON压缩失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'minified_json'}


class JSONPrettyAction(BaseAction):
    """Pretty print JSON."""
    action_type = "json5_pretty"
    display_name = "格式化JSON"
    description = "格式化输出JSON"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON pretty.

        Args:
            context: Execution context.
            params: Dict with json_string, indent, output_var.

        Returns:
            ActionResult with pretty JSON.
        """
        json_string = params.get('json_string', '')
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'pretty_json')

        try:
            import json

            resolved = context.resolve_value(json_string)
            resolved_indent = int(context.resolve_value(indent)) if indent else 2

            parsed = json.loads(resolved)
            result = json.dumps(parsed, indent=resolved_indent)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON格式化成功",
                data={
                    'pretty': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2, 'output_var': 'pretty_json'}


class JSONValidateAction(BaseAction):
    """Validate JSON."""
    action_type = "json5_validate"
    display_name = "验证JSON"
    description = "验证JSON格式"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON validate.

        Args:
            context: Execution context.
            params: Dict with json_string, output_var.

        Returns:
            ActionResult with validation result.
        """
        json_string = params.get('json_string', '')
        output_var = params.get('output_var', 'json_valid')

        try:
            import json

            resolved = context.resolve_value(json_string)

            json.loads(resolved)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"JSON验证: 有效",
                data={
                    'valid': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            context.set(output_var, False)
            return ActionResult(
                success=True,
                message=f"JSON验证: 无效",
                data={
                    'valid': False,
                    'error': str(e),
                    'output_var': output_var
                }
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_valid'}


class JSONMergeAction(BaseAction):
    """Merge JSON objects."""
    action_type = "json5_merge"
    display_name = "合并JSON"
    description = "合并多个JSON对象"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON merge.

        Args:
            context: Execution context.
            params: Dict with json_objects, output_var.

        Returns:
            ActionResult with merged JSON.
        """
        json_objects = params.get('json_objects', [])
        output_var = params.get('output_var', 'merged_json')

        try:
            import json

            resolved = context.resolve_value(json_objects)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            merged = {}
            for obj in resolved:
                if isinstance(obj, dict):
                    merged.update(obj)
                else:
                    parsed = json.loads(obj) if isinstance(obj, str) else obj
                    merged.update(parsed)

            result = json.dumps(merged)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON合并成功: {len(merged)}个键",
                data={
                    'merged': result,
                    'keys_count': len(merged),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON合并失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_objects']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'merged_json'}


class JSONPathAction(BaseAction):
    """Get JSON path value."""
    action_type = "json5_path"
    display_name = "获取JSON路径"
    description = "获取JSON路径的值"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON path.

        Args:
            context: Execution context.
            params: Dict with json_string, path, output_var.

        Returns:
            ActionResult with path value.
        """
        json_string = params.get('json_string', '')
        path = params.get('path', '')
        output_var = params.get('output_var', 'path_value')

        try:
            import json

            resolved = context.resolve_value(json_string)
            resolved_path = context.resolve_value(path) if path else ''

            parsed = json.loads(resolved) if isinstance(resolved, str) else resolved

            if not resolved_path:
                return ActionResult(
                    success=False,
                    message="JSON路径不能为空"
                )

            keys = resolved_path.split('.')
            result = parsed
            for key in keys:
                if isinstance(result, dict) and key in result:
                    result = result[key]
                else:
                    result = None
                    break

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON路径获取成功",
                data={
                    'path': resolved_path,
                    'value': result,
                    'found': result is not None,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON路径获取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_string', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'path_value'}
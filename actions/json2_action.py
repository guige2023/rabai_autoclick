"""JSON2 action module for RabAI AutoClick.

Provides advanced JSON operations:
- JsonPrettyPrintAction: Pretty print JSON
- JsonValidateAction: Validate JSON
- JsonMergeAction: Merge JSON objects
- JsonFlattenAction: Flatten nested JSON
"""

import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JsonPrettyPrintAction(BaseAction):
    """Pretty print JSON."""
    action_type = "json_pretty_print"
    display_name = "格式化JSON"
    description = "美化输出JSON"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pretty print.

        Args:
            context: Execution context.
            params: Dict with json_string, indent, output_var.

        Returns:
            ActionResult with pretty printed JSON.
        """
        json_string = params.get('json_string', '')
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'json_result')

        valid, msg = self.validate_type(json_string, str, 'json_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(json_string)
            resolved_indent = context.resolve_value(indent)

            data = json.loads(resolved)
            result = json.dumps(data, indent=int(resolved_indent), ensure_ascii=False)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON已格式化: {len(result)} 字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except json.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"无效的JSON: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2, 'output_var': 'json_result'}


class JsonValidateAction(BaseAction):
    """Validate JSON."""
    action_type = "json_validate"
    display_name = "验证JSON"
    description = "验证JSON格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate.

        Args:
            context: Execution context.
            params: Dict with json_string, output_var.

        Returns:
            ActionResult with validation result.
        """
        json_string = params.get('json_string', '')
        output_var = params.get('output_var', 'json_valid')

        valid, msg = self.validate_type(json_string, str, 'json_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(json_string)
            json.loads(resolved)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="JSON验证通过",
                data={
                    'valid': True,
                    'output_var': output_var
                }
            )
        except json.JSONDecodeError as e:
            context.set(output_var, False)
            return ActionResult(
                success=True,
                message=f"JSON验证失败: {str(e)}",
                data={
                    'valid': False,
                    'error': str(e),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_valid'}


class JsonMergeAction(BaseAction):
    """Merge JSON objects."""
    action_type = "json_merge"
    display_name = "合并JSON"
    description = "合并多个JSON对象"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge.

        Args:
            context: Execution context.
            params: Dict with json_objects, output_var.

        Returns:
            ActionResult with merged JSON.
        """
        json_objects = params.get('json_objects', [])
        output_var = params.get('output_var', 'json_result')

        valid, msg = self.validate_type(json_objects, (list, tuple), 'json_objects')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(json_objects) < 2:
            return ActionResult(
                success=False,
                message="至少需要2个JSON对象"
            )

        try:
            resolved_objects = [context.resolve_value(obj) for obj in json_objects]

            merged = {}
            for obj_str in resolved_objects:
                obj = json.loads(obj_str) if isinstance(obj_str, str) else obj_str
                merged.update(obj)

            result = json.dumps(merged, ensure_ascii=False)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"JSON已合并: {len(merged)} 个键",
                data={
                    'result': result,
                    'keys': len(merged),
                    'output_var': output_var
                }
            )
        except json.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"无效的JSON: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_objects']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_result'}


class JsonFlattenAction(BaseAction):
    """Flatten nested JSON."""
    action_type = "json_flatten"
    display_name = "扁平化JSON"
    description = "扁平化嵌套的JSON对象"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with json_string, separator, output_var.

        Returns:
            ActionResult with flattened JSON.
        """
        json_string = params.get('json_string', '')
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'json_result')

        valid, msg = self.validate_type(json_string, str, 'json_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(json_string)
            resolved_sep = context.resolve_value(separator)

            data = json.loads(resolved)

            def flatten(d, parent_key='', sep=resolved_sep):
                items = []
                for k, v in d.items():
                    new_key = f"{parent_key}{sep}{k}" if parent_key else k
                    if isinstance(v, dict):
                        items.extend(flatten(v, new_key, sep=sep).items())
                    elif isinstance(v, list):
                        for i, item in enumerate(v):
                            if isinstance(item, dict):
                                items.extend(flatten(item, f"{new_key}[{i}]", sep=sep).items())
                            else:
                                items.append((f"{new_key}[{i}]", item))
                    else:
                        items.append((new_key, v))
                return dict(items)

            result = flatten(data)
            result_str = json.dumps(result, ensure_ascii=False)

            context.set(output_var, result_str)

            return ActionResult(
                success=True,
                message=f"JSON已扁平化: {len(result)} 个键",
                data={
                    'result': result_str,
                    'keys': len(result),
                    'output_var': output_var
                }
            )
        except json.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"无效的JSON: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扁平化JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '.', 'output_var': 'json_result'}
"""Json3 action module for RabAI AutoClick.

Provides additional JSON operations:
- JsonPrettyPrintAction: Pretty print JSON
- JsonMinifyAction: Minify JSON
- JsonValidateAction: Validate JSON string
- JsonFlattenAction: Flatten nested JSON
- JsonUnflattenAction: Unflatten JSON object
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JsonPrettyPrintAction(BaseAction):
    """Pretty print JSON."""
    action_type = "json3_pretty"
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
            params: Dict with json_str, indent, output_var.

        Returns:
            ActionResult with pretty JSON.
        """
        json_str = params.get('json_str', '')
        indent = params.get('indent', 2)
        output_var = params.get('output_var', 'pretty_json')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import json
            resolved_json = context.resolve_value(json_str)
            resolved_indent = int(context.resolve_value(indent)) if indent else 2

            parsed = json.loads(resolved_json)
            pretty = json.dumps(parsed, indent=resolved_indent, ensure_ascii=False)
            context.set(output_var, pretty)

            return ActionResult(
                success=True,
                message=f"JSON已格式化: {len(pretty)} 字符",
                data={
                    'original_length': len(resolved_json),
                    'result': pretty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': 2, 'output_var': 'pretty_json'}


class JsonMinifyAction(BaseAction):
    """Minify JSON."""
    action_type = "json3_minify"
    display_name = "压缩JSON"
    description = "压缩JSON去除空白"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute minify.

        Args:
            context: Execution context.
            params: Dict with json_str, output_var.

        Returns:
            ActionResult with minified JSON.
        """
        json_str = params.get('json_str', '')
        output_var = params.get('output_var', 'minified_json')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import json
            resolved_json = context.resolve_value(json_str)

            parsed = json.loads(resolved_json)
            minified = json.dumps(parsed, separators=(',', ':'), ensure_ascii=False)
            context.set(output_var, minified)

            return ActionResult(
                success=True,
                message=f"JSON已压缩: {len(minified)} 字符",
                data={
                    'original_length': len(resolved_json),
                    'result': minified,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"压缩JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'minified_json'}


class JsonValidateAction(BaseAction):
    """Validate JSON string."""
    action_type = "json3_validate"
    display_name = "验证JSON"
    description = "验证JSON字符串是否有效"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate.

        Args:
            context: Execution context.
            params: Dict with json_str, output_var.

        Returns:
            ActionResult with validation result.
        """
        json_str = params.get('json_str', '')
        output_var = params.get('output_var', 'is_valid_json')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import json
            resolved_json = context.resolve_value(json_str)

            json.loads(resolved_json)
            is_valid = True
            context.set(output_var, is_valid)

            return ActionResult(
                success=True,
                message="JSON有效",
                data={
                    'is_valid': is_valid,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=True,
                message=f"JSON无效: {str(e)}",
                data={
                    'is_valid': False,
                    'error': str(e),
                    'output_var': output_var
                }
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_valid_json'}


class JsonFlattenAction(BaseAction):
    """Flatten nested JSON."""
    action_type = "json3_flatten"
    display_name = "扁平化JSON"
    description = "将嵌套JSON展平为扁平对象"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with json_str, separator, output_var.

        Returns:
            ActionResult with flattened dict.
        """
        json_str = params.get('json_str', '')
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'flattened_json')

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import json
            resolved_json = context.resolve_value(json_str)
            resolved_sep = context.resolve_value(separator) if separator else '.'

            parsed = json.loads(resolved_json)

            def flatten(obj: Any, prefix: str = '') -> Dict[str, Any]:
                result = {}
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_key = f"{prefix}{resolved_sep}{k}" if prefix else k
                        if isinstance(v, (dict, list)):
                            result.update(flatten(v, new_key))
                        else:
                            result[new_key] = v
                elif isinstance(obj, list):
                    for i, v in enumerate(obj):
                        new_key = f"{prefix}[{i}]"
                        if isinstance(v, (dict, list)):
                            result.update(flatten(v, new_key))
                        else:
                            result[new_key] = v
                else:
                    result[prefix] = obj
                return result

            flattened = flatten(parsed)
            result_str = json.dumps(flattened, ensure_ascii=False)
            context.set(output_var, flattened)

            return ActionResult(
                success=True,
                message=f"JSON已扁平化: {len(flattened)} 个键",
                data={
                    'original_keys': len(parsed) if isinstance(parsed, dict) else 'array',
                    'result': result_str,
                    'flattened': flattened,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扁平化JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['json_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '.', 'output_var': 'flattened_json'}


class JsonUnflattenAction(BaseAction):
    """Unflatten JSON object."""
    action_type = "json3_unflatten"
    display_name = "还原JSON"
    description = "将扁平对象还原为嵌套JSON"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unflatten.

        Args:
            context: Execution context.
            params: Dict with flat_dict, separator, output_var.

        Returns:
            ActionResult with nested JSON.
        """
        flat_dict = params.get('flat_dict', {})
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'unflattened_json')

        try:
            import json
            resolved_dict = context.resolve_value(flat_dict)
            resolved_sep = context.resolve_value(separator) if separator else '.'

            if not isinstance(resolved_dict, dict):
                return ActionResult(
                    success=False,
                    message="flat_dict 必须是字典"
                )

            def unflatten(d: Dict[str, Any], sep: str) -> Any:
                result = {}
                for key, value in d.items():
                    parts = key.split(sep)
                    current = result
                    for part in parts[:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[parts[-1]] = value
                return result

            unflattened = unflatten(resolved_dict, resolved_sep)
            result_str = json.dumps(unflattened, ensure_ascii=False)
            context.set(output_var, unflattened)

            return ActionResult(
                success=True,
                message=f"JSON已还原",
                data={
                    'result': result_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"还原JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['flat_dict']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '.', 'output_var': 'unflattened_json'}

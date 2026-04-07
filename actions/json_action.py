"""JSON action module for RabAI AutoClick.

Provides JSON operations:
- JsonParseAction: Parse JSON string
- JsonStringifyAction: Convert to JSON string
- JsonQueryAction: Query JSON path
- JsonMergeAction: Merge JSON objects
- JsonGetAction: Get JSON value by key
- JsonSetAction: Set JSON value by key
- JsonDeleteAction: Delete JSON key
- JsonValidateAction: Validate JSON format
- JsonPrettyAction: Pretty print JSON
- JsonFlattenAction: Flatten nested JSON
- JsonUnflattenAction: Unflatten to nested JSON
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import json as json_module
    JSON_AVAILABLE = True
except ImportError:
    JSON_AVAILABLE = False


class JsonParseAction(BaseAction):
    """Parse JSON string."""
    action_type = "json_parse"
    display_name = "JSON解析"
    description = "解析JSON字符串为对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON parsing.

        Args:
            context: Execution context.
            params: Dict with json_str, output_var.

        Returns:
            ActionResult with parsed object.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_str = params.get('json_str', '')
        output_var = params.get('output_var', 'json_parsed')

        if not json_str:
            return ActionResult(success=False, message="JSON字符串不能为空")

        valid, msg = self.validate_type(json_str, str, 'json_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            parsed = json_module.loads(json_str)

            context.set(output_var, parsed)

            return ActionResult(
                success=True,
                message="JSON解析成功",
                data={'parsed': parsed}
            )

        except json_module.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"JSON解析失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON解析异常: {str(e)}"
            )


class JsonStringifyAction(BaseAction):
    """Convert to JSON string."""
    action_type = "json_stringify"
    display_name = "JSON序列化"
    description = "将对象转换为JSON字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON stringification.

        Args:
            context: Execution context.
            params: Dict with obj, indent, sort_keys, output_var.

        Returns:
            ActionResult with JSON string.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        obj = params.get('obj', None)
        indent = params.get('indent', None)
        sort_keys = params.get('sort_keys', False)
        output_var = params.get('output_var', 'json_string')

        if obj is None:
            return ActionResult(success=False, message="对象不能为空")

        try:
            json_str = json_module.dumps(
                obj,
                indent=indent,
                sort_keys=sort_keys,
                ensure_ascii=False
            )

            context.set(output_var, json_str)

            return ActionResult(
                success=True,
                message=f"JSON序列化成功: {len(json_str)} 字符",
                data={'json_str': json_str, 'length': len(json_str)}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON序列化失败: {str(e)}"
            )


class JsonQueryAction(BaseAction):
    """Query JSON path."""
    action_type = "json_query"
    display_name = "JSON查询"
    description = "使用JSONPath查询JSON数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON path query.

        Args:
            context: Execution context.
            params: Dict with json_data, path, output_var.

        Returns:
            ActionResult with queried value.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_data = params.get('json_data', None)
        path = params.get('path', '')
        output_var = params.get('output_var', 'json_query_result')

        if json_data is None:
            return ActionResult(success=False, message="JSON数据不能为空")

        if not path:
            return ActionResult(success=False, message="查询路径不能为空")

        try:
            parts = path.replace('[', '.').replace(']', '').split('.')
            parts = [p for p in parts if p]

            current = json_data
            for part in parts:
                if isinstance(current, dict):
                    if part in current:
                        current = current[part]
                    else:
                        return ActionResult(
                            success=False,
                            message=f"键不存在: {part}"
                        )
                elif isinstance(current, list):
                    try:
                        idx = int(part)
                        current = current[idx]
                    except (ValueError, IndexError):
                        return ActionResult(
                            success=False,
                            message=f"索引无效: {part}"
                        )
                else:
                    return ActionResult(
                        success=False,
                        message=f"无法在{type(current)}中查询"
                    )

            context.set(output_var, current)

            return ActionResult(
                success=True,
                message="查询成功",
                data={'value': current, 'path': path}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON查询失败: {str(e)}"
            )


class JsonMergeAction(BaseAction):
    """Merge JSON objects."""
    action_type = "json_merge"
    display_name = "JSON合并"
    description = "合并多个JSON对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON merge.

        Args:
            context: Execution context.
            params: Dict with objects (list), deep, output_var.

        Returns:
            ActionResult with merged object.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        objects = params.get('objects', [])
        deep = params.get('deep', True)
        output_var = params.get('output_var', 'json_merged')

        if not objects:
            return ActionResult(success=False, message="对象列表不能为空")

        try:
            import copy

            if deep:
                result = copy.deepcopy(objects[0]) if objects else {}
            else:
                result = objects[0].copy() if objects else {}

            for obj in objects[1:]:
                if deep:
                    obj_copy = copy.deepcopy(obj)
                else:
                    obj_copy = obj.copy()

                for key, value in obj_copy.items():
                    if key in result and deep and isinstance(result[key], dict) and isinstance(value, dict):
                        result[key] = self._deep_merge(result[key], value)
                    else:
                        result[key] = value

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="JSON合并成功",
                data={'merged': result}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON合并失败: {str(e)}"
            )

    def _deep_merge(self, dict1: Dict, dict2: Dict) -> Dict:
        """Deep merge two dictionaries."""
        import copy
        result = copy.deepcopy(dict1)
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result


class JsonGetAction(BaseAction):
    """Get JSON value by key."""
    action_type = "json_get"
    display_name = "JSON取值"
    description = "获取JSON对象中的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON get.

        Args:
            context: Execution context.
            params: Dict with json_data, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_data = params.get('json_data', None)
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'json_value')

        if json_data is None:
            return ActionResult(success=False, message="JSON数据不能为空")

        if not key:
            return ActionResult(success=False, message="键不能为空")

        try:
            if isinstance(json_data, dict):
                value = json_data.get(key, default)
            elif isinstance(json_data, list):
                try:
                    idx = int(key)
                    value = json_data[idx] if -len(json_data) <= idx < len(json_data) else default
                except ValueError:
                    value = default
            else:
                value = default

            context.set(output_var, value)

            return ActionResult(
                success=True,
                message="获取成功",
                data={'key': key, 'value': value}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON取值失败: {str(e)}"
            )


class JsonSetAction(BaseAction):
    """Set JSON value by key."""
    action_type = "json_set"
    display_name = "JSON设值"
    description = "设置JSON对象中的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON set.

        Args:
            context: Execution context.
            params: Dict with json_data, key, value, output_var.

        Returns:
            ActionResult with modified object.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_data = params.get('json_data', None)
        key = params.get('key', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'json_modified')

        if json_data is None:
            return ActionResult(success=False, message="JSON数据不能为空")

        if not key:
            return ActionResult(success=False, message="键不能为空")

        try:
            import copy
            result = copy.deepcopy(json_data)

            if isinstance(result, dict):
                result[key] = value
            elif isinstance(result, list):
                try:
                    idx = int(key)
                    if -len(result) <= idx < len(result):
                        result[idx] = value
                    else:
                        return ActionResult(
                            success=False,
                            message=f"索引超出范围: {idx}"
                        )
                except ValueError:
                    return ActionResult(
                        success=False,
                        message=f"无效索引: {key}"
                    )
            else:
                return ActionResult(
                    success=False,
                    message=f"无法在{type(result)}中设值"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="设值成功",
                data={'key': key, 'value': value, 'result': result}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON设值失败: {str(e)}"
            )


class JsonDeleteAction(BaseAction):
    """Delete JSON key."""
    action_type = "json_delete"
    display_name = "JSON删除"
    description = "删除JSON对象中的键"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON delete.

        Args:
            context: Execution context.
            params: Dict with json_data, key, output_var.

        Returns:
            ActionResult with modified object.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_data = params.get('json_data', None)
        key = params.get('key', '')
        output_var = params.get('output_var', 'json_deleted')

        if json_data is None:
            return ActionResult(success=False, message="JSON数据不能为空")

        if not key:
            return ActionResult(success=False, message="键不能为空")

        try:
            import copy
            result = copy.deepcopy(json_data)

            if isinstance(result, dict):
                if key in result:
                    del result[key]
                else:
                    return ActionResult(
                        success=False,
                        message=f"键不存在: {key}"
                    )
            elif isinstance(result, list):
                try:
                    idx = int(key)
                    if -len(result) <= idx < len(result):
                        del result[idx]
                    else:
                        return ActionResult(
                            success=False,
                            message=f"索引超出范围: {idx}"
                        )
                except ValueError:
                    return ActionResult(
                        success=False,
                        message=f"无效索引: {key}"
                    )
            else:
                return ActionResult(
                    success=False,
                    message=f"无法在{type(result)}中删除"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="删除成功",
                data={'key': key, 'result': result}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON删除失败: {str(e)}"
            )


class JsonValidateAction(BaseAction):
    """Validate JSON format."""
    action_type = "json_validate"
    display_name = "JSON验证"
    description = "验证JSON格式是否正确"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON validation.

        Args:
            context: Execution context.
            params: Dict with json_str, output_var.

        Returns:
            ActionResult with validation result.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_str = params.get('json_str', '')
        output_var = params.get('output_var', 'json_valid')

        if not json_str:
            return ActionResult(success=False, message="JSON字符串不能为空")

        try:
            json_module.loads(json_str)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="JSON格式有效",
                data={'valid': True}
            )

        except json_module.JSONDecodeError as e:
            context.set(output_var, False)
            return ActionResult(
                success=True,
                message=f"JSON格式无效: {str(e)}",
                data={'valid': False, 'error': str(e)}
            )


class JsonPrettyAction(BaseAction):
    """Pretty print JSON."""
    action_type = "json_pretty"
    display_name = "JSON美化"
    description = "格式化输出JSON"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON pretty print.

        Args:
            context: Execution context.
            params: Dict with json_data, indent, sort_keys, output_var.

        Returns:
            ActionResult with formatted JSON string.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_data = params.get('json_data', None)
        indent = params.get('indent', 2)
        sort_keys = params.get('sort_keys', False)
        output_var = params.get('output_var', 'json_pretty')

        if json_data is None:
            return ActionResult(success=False, message="JSON数据不能为空")

        try:
            pretty_str = json_module.dumps(
                json_data,
                indent=indent,
                sort_keys=sort_keys,
                ensure_ascii=False
            )

            context.set(output_var, pretty_str)

            return ActionResult(
                success=True,
                message=f"JSON美化成功: {len(pretty_str)} 字符",
                data={'pretty': pretty_str, 'length': len(pretty_str)}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON美化失败: {str(e)}"
            )


class JsonFlattenAction(BaseAction):
    """Flatten nested JSON."""
    action_type = "json_flatten"
    display_name = "JSON扁平化"
    description = "将嵌套JSON展平为扁平结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON flatten.

        Args:
            context: Execution context.
            params: Dict with json_data, separator, output_var.

        Returns:
            ActionResult with flattened object.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        json_data = params.get('json_data', None)
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'json_flat')

        if json_data is None:
            return ActionResult(success=False, message="JSON数据不能为空")

        try:
            flattened = {}

            def _flatten(obj: Any, prefix: str = '') -> None:
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        new_key = f"{prefix}{separator}{key}" if prefix else key
                        _flatten(value, new_key)
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        new_key = f"{prefix}[{i}]"
                        _flatten(item, new_key)
                else:
                    flattened[prefix] = obj

            _flatten(json_data)

            context.set(output_var, flattened)

            return ActionResult(
                success=True,
                message=f"JSON扁平化成功: {len(flattened)} 键",
                data={'flattened': flattened, 'keys': list(flattened.keys())}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON扁平化失败: {str(e)}"
            )


class JsonUnflattenAction(BaseAction):
    """Unflatten to nested JSON."""
    action_type = "json_unflatten"
    display_name = "JSON嵌套化"
    description = "将扁平JSON还原为嵌套结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON unflatten.

        Args:
            context: Execution context.
            params: Dict with flat_data, separator, output_var.

        Returns:
            ActionResult with nested object.
        """
        if not JSON_AVAILABLE:
            return ActionResult(success=False, message="JSON库不可用")

        flat_data = params.get('flat_data', None)
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'json_nested')

        if flat_data is None:
            return ActionResult(success=False, message="扁平数据不能为空")

        if not isinstance(flat_data, dict):
            return ActionResult(success=False, message="扁平数据必须是字典")

        try:
            result = {}

            for flat_key, value in flat_data.items():
                keys = flat_key.split(separator)
                current = result

                for i, key in enumerate(keys[:-1]):
                    if key not in current:
                        current[key] = {}
                    current = current[key]

                current[keys[-1]] = value

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="JSON嵌套化成功",
                data={'nested': result}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"JSON嵌套化失败: {str(e)}"
            )

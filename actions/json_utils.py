"""JSON utilities action module for RabAI AutoClick.

Provides JSON manipulation actions including parse, stringify,
path query, merge, and transform operations.
"""

import json
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JsonParseAction(BaseAction):
    """Parse JSON string into Python object.
    
    Supports custom error handling, default values on failure,
    and nested key extraction.
    """
    action_type = "json_parse"
    display_name = "JSON解析"
    description = "将JSON字符串解析为Python对象，支持默认值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Parse a JSON string.
        
        Args:
            context: Execution context.
            params: Dict with keys: json_string, default_value,
                   encoding, save_to_var.
        
        Returns:
            ActionResult with parsed Python object.
        """
        json_string = params.get('json_string', '')
        default_value = params.get('default_value', None)
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if not json_string:
            result_data = {'parsed': None, 'success': False}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message="JSON字符串为空",
                data=result_data
            )

        # Handle bytes or string
        if isinstance(json_string, bytes):
            json_string = json_string.decode(encoding, errors='replace')

        try:
            parsed = json.loads(json_string)
            result_data = {
                'parsed': parsed,
                'success': True,
                'type': type(parsed).__name__
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"JSON解析成功: {type(parsed).__name__}",
                data=result_data
            )
        except json.JSONDecodeError as e:
            result_data = {
                'parsed': default_value,
                'success': False,
                'error': f"JSON解析失败: {e.msg} (位置 {e.pos})",
                'error_pos': e.pos,
                'error_lineno': e.lineno,
                'error_colno': e.colno
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"JSON解析失败: {e.msg} at position {e.pos}",
                data=result_data
            )
        except Exception as e:
            result_data = {
                'parsed': default_value,
                'success': False,
                'error': str(e)
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"JSON解析异常: {str(e)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'default_value': None,
            'encoding': 'utf-8',
            'save_to_var': None
        }


class JsonStringifyAction(BaseAction):
    """Convert Python object to JSON string.
    
    Supports pretty printing, custom separators, and 
    handling of non-serializable objects.
    """
    action_type = "json_stringify"
    display_name = "JSON序列化"
    description = "将Python对象序列化为JSON字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Serialize Python object to JSON string.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, pretty, indent,
                   ensure_ascii, default_handler, save_to_var.
        
        Returns:
            ActionResult with JSON string.
        """
        data = params.get('data', None)
        pretty = params.get('pretty', False)
        indent = params.get('indent', 2)
        ensure_ascii = params.get('ensure_ascii', False)
        default_handler = params.get('default_handler', 'str')
        save_to_var = params.get('save_to_var', None)

        if data is None:
            result_data = {'json_string': None, 'success': False}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message="数据对象为空",
                data=result_data
            )

        # Define default handler
        def safe_handler(obj):
            if default_handler == 'str':
                return str(obj)
            elif default_handler == 'repr':
                return repr(obj)
            elif default_handler == 'none':
                return None
            else:
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        try:
            if pretty:
                json_string = json.dumps(
                    data,
                    indent=indent,
                    ensure_ascii=ensure_ascii,
                    default=safe_handler
                )
            else:
                json_string = json.dumps(
                    data,
                    separators=(',', ':'),
                    ensure_ascii=ensure_ascii,
                    default=safe_handler
                )

            result_data = {
                'json_string': json_string,
                'success': True,
                'length': len(json_string)
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"JSON序列化成功: {len(json_string)} bytes",
                data=result_data
            )
        except Exception as e:
            result_data = {
                'json_string': None,
                'success': False,
                'error': str(e)
            }
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"JSON序列化失败: {str(e)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'pretty': False,
            'indent': 2,
            'ensure_ascii': False,
            'default_handler': 'str',
            'save_to_var': None
        }


class JsonPathAction(BaseAction):
    """Query JSON data using dot-notation paths.
    
    Supports nested key access, list indexing, and 
    default values for missing paths.
    """
    action_type = "json_path"
    display_name = "JSON路径查询"
    description = "使用点号路径查询JSON数据中的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Query JSON data using path.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, path, default_value,
                   save_to_var.
        
        Returns:
            ActionResult with queried value.
        """
        data = params.get('data', None)
        path = params.get('path', '')
        default_value = params.get('default_value', None)
        save_to_var = params.get('save_to_var', None)

        if data is None:
            return ActionResult(
                success=False,
                message="JSON数据为空"
            )

        if not path:
            result_data = {'value': data, 'found': True}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=True,
                message=f"返回完整数据: {type(data).__name__}",
                data=result_data
            )

        # Parse path
        parts = path.split('.')
        current = data
        found = True

        for part in parts:
            if current is None:
                found = False
                break

            # Handle list index: key[0] or key[0:2]
            if '[' in part:
                key, indices = part.split('[', 1)
                if key and key not in ('', '.'):
                    if isinstance(current, dict):
                        if key in current:
                            current = current[key]
                        else:
                            found = False
                            break
                    else:
                        found = False
                        break
                else:
                    current = current if isinstance(current, (list, dict)) else None

                # Handle indices
                indices = indices.rstrip(']')
                if indices.isdigit():
                    idx = int(indices)
                    if isinstance(current, list) and 0 <= idx < len(current):
                        current = current[idx]
                    else:
                        found = False
                        break
                elif ':' in indices:
                    try:
                        start, end = indices.split(':')
                        start = int(start) if start else 0
                        end = int(end) if end else len(current)
                        if isinstance(current, list):
                            current = current[start:end]
                        else:
                            found = False
                            break
                    except ValueError:
                        found = False
                        break
            else:
                if isinstance(current, dict):
                    if part in current:
                        current = current[part]
                    else:
                        found = False
                        break
                elif isinstance(current, list):
                    try:
                        idx = int(part)
                        if 0 <= idx < len(current):
                            current = current[idx]
                        else:
                            found = False
                            break
                    except ValueError:
                        found = False
                        break
                else:
                    found = False
                    break

        value = current if found else default_value
        result_data = {
            'value': value,
            'found': found,
            'path': path
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if found:
            return ActionResult(
                success=True,
                message=f"路径查询成功: {path}",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"路径不存在，使用默认值: {path}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['data', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'default_value': None,
            'save_to_var': None
        }


class JsonMergeAction(BaseAction):
    """Merge multiple JSON objects.
    
    Supports deep merge, conflict resolution strategies,
    and array concatenation options.
    """
    action_type = "json_merge"
    display_name = "JSON合并"
    description = "合并多个JSON对象，支持深度合并和冲突处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Merge multiple JSON objects.
        
        Args:
            context: Execution context.
            params: Dict with keys: objects (list), strategy,
                   save_to_var.
        
        Returns:
            ActionResult with merged object.
        """
        objects = params.get('objects', [])
        strategy = params.get('strategy', 'deep')
        save_to_var = params.get('save_to_var', None)

        if not objects:
            return ActionResult(
                success=False,
                message="没有要合并的对象"
            )

        if len(objects) == 1:
            result = objects[0]
        else:
            result = objects[0]
            for obj in objects[1:]:
                result = self._merge_two(result, obj, strategy)

        result_data = {
            'merged': result,
            'count': len(objects),
            'strategy': strategy
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"成功合并 {len(objects)} 个对象",
            data=result_data
        )

    def _merge_two(self, base: Any, overlay: Any, strategy: str) -> Any:
        """Merge two objects based on strategy."""
        if strategy == 'deep' and isinstance(base, dict) and isinstance(overlay, dict):
            result = dict(base)
            for key, value in overlay.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self._merge_two(result[key], value, strategy)
                elif key in result and isinstance(result[key], list) and isinstance(value, list):
                    result[key] = result[key] + value
                else:
                    result[key] = value
            return result
        elif strategy == 'overlay':
            return overlay
        else:
            return overlay

    def get_required_params(self) -> List[str]:
        return ['objects']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'strategy': 'deep',
            'save_to_var': None
        }

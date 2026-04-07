"""Conversion action module for RabAI AutoClick.

Provides type conversion operations:
- ToStringAction: Convert to string
- ToIntAction: Convert to integer
- ToFloatAction: Convert to float
- ToBoolAction: Convert to boolean
- ToListAction: Convert to list
- ToDictAction: Convert to dictionary
- ToJsonAction: Convert to JSON string
- FromJsonAction: Parse JSON string
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ToStringAction(BaseAction):
    """Convert value to string."""
    action_type = "to_string"
    display_name = "转换为字符串"
    description = "将值转换为字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute string conversion.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with string value.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'result')

        try:
            resolved = context.resolve_value(value)
            result = str(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为字符串: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result'}


class ToIntAction(BaseAction):
    """Convert value to integer."""
    action_type = "to_int"
    display_name = "转换为整数"
    description = "将值转换为整数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute integer conversion.

        Args:
            context: Execution context.
            params: Dict with value, default, output_var.

        Returns:
            ActionResult with integer value.
        """
        value = params.get('value', 0)
        default = params.get('default', 0)
        output_var = params.get('output_var', 'result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str) and resolved.strip() == '':
                result = default
            else:
                result = int(float(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为整数: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError):
            context.set(output_var, default)
            return ActionResult(
                success=True,
                message=f"转换失败，使用默认值: {default}",
                data={
                    'result': default,
                    'used_default': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为整数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': 0, 'output_var': 'result'}


class ToFloatAction(BaseAction):
    """Convert value to float."""
    action_type = "to_float"
    display_name = "转换为浮点数"
    description = "将值转换为浮点数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute float conversion.

        Args:
            context: Execution context.
            params: Dict with value, default, output_var.

        Returns:
            ActionResult with float value.
        """
        value = params.get('value', 0.0)
        default = params.get('default', 0.0)
        output_var = params.get('output_var', 'result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str) and resolved.strip() == '':
                result = default
            else:
                result = float(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为浮点数: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError):
            context.set(output_var, default)
            return ActionResult(
                success=True,
                message=f"转换失败，使用默认值: {default}",
                data={
                    'result': default,
                    'used_default': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为浮点数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': 0.0, 'output_var': 'result'}


class ToBoolAction(BaseAction):
    """Convert value to boolean."""
    action_type = "to_bool"
    display_name = "转换为布尔值"
    description = "将值转换为布尔值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute boolean conversion.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with boolean value.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'result')

        try:
            resolved = context.resolve_value(value)

            # Handle string representations of boolean
            if isinstance(resolved, str):
                result = resolved.lower() in ('true', '1', 'yes', 'on')
            else:
                result = bool(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为布尔值: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为布尔值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result'}


class ToListAction(BaseAction):
    """Convert value to list."""
    action_type = "to_list"
    display_name = "转换为列表"
    description = "将值转换为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list conversion.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with list value.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, list):
                result = resolved
            elif isinstance(resolved, tuple):
                result = list(resolved)
            elif isinstance(resolved, str):
                result = [resolved]
            elif hasattr(resolved, '__iter__'):
                result = list(resolved)
            else:
                result = [resolved]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为列表: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result'}


class ToDictAction(BaseAction):
    """Convert value to dictionary."""
    action_type = "to_dict"
    display_name = "转换为字典"
    description = "将值转换为字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dictionary conversion.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with dictionary value.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, dict):
                result = resolved
            elif isinstance(resolved, str):
                # Try to parse as JSON
                try:
                    import json
                    result = json.loads(resolved)
                    if not isinstance(result, dict):
                        return ActionResult(
                            success=False,
                            message="字符串不是有效的字典JSON"
                        )
                except json.JSONDecodeError:
                    return ActionResult(
                        success=False,
                        message="无法将字符串转换为字典"
                    )
            else:
                return ActionResult(
                    success=False,
                    message=f"无法将 {type(resolved).__name__} 转换为字典"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为字典: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result'}


class ToJsonAction(BaseAction):
    """Convert value to JSON string."""
    action_type = "to_json"
    display_name = "转换为JSON"
    description = "将值转换为JSON字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON conversion.

        Args:
            context: Execution context.
            params: Dict with value, indent, output_var.

        Returns:
            ActionResult with JSON string.
        """
        value = params.get('value', None)
        indent = params.get('indent', None)
        output_var = params.get('output_var', 'result')

        try:
            resolved = context.resolve_value(value)

            import json

            if indent is not None:
                indent = int(indent)

            result = json.dumps(resolved, indent=indent, ensure_ascii=False)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为JSON: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为JSON失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'indent': None, 'output_var': 'result'}


class FromJsonAction(BaseAction):
    """Parse JSON string to value."""
    action_type = "from_json"
    display_name = "解析JSON"
    description = "解析JSON字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON parsing.

        Args:
            context: Execution context.
            params: Dict with json_string, output_var.

        Returns:
            ActionResult with parsed value.
        """
        json_string = params.get('json_string', '')
        output_var = params.get('output_var', 'result')

        valid, msg = self.validate_type(json_string, str, 'json_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(json_string)

            import json
            result = json.loads(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message="JSON解析成功",
                data={
                    'result': result,
                    'type': type(result).__name__,
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
        return ['json_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result'}
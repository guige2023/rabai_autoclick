"""Dict8 action module for RabAI AutoClick.

Provides additional dict operations:
- DictMergeAction: Merge multiple dictionaries
- DictFilterAction: Filter dictionary by keys
- DictMapAction: Map function to dictionary values
- DictInvertAction: Invert dictionary keys and values
- DictDeepGetAction: Get nested dictionary value
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictMergeAction(BaseAction):
    """Merge multiple dictionaries."""
    action_type = "dict8_merge"
    display_name = "合并字典"
    description = "合并多个字典"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge.

        Args:
            context: Execution context.
            params: Dict with dicts, output_var.

        Returns:
            ActionResult with merged dictionary.
        """
        dicts = params.get('dicts', [])
        output_var = params.get('output_var', 'merged_dict')

        try:
            resolved = context.resolve_value(dicts)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = {}
            for d in resolved:
                if isinstance(d, dict):
                    result.update(d)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"合并字典: {len(result)}个键",
                data={
                    'merged': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dicts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'merged_dict'}


class DictFilterAction(BaseAction):
    """Filter dictionary by keys."""
    action_type = "dict8_filter"
    display_name = "过滤字典"
    description = "根据键过滤字典"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter.

        Args:
            context: Execution context.
            params: Dict with dict, keys, mode, output_var.

        Returns:
            ActionResult with filtered dictionary.
        """
        input_dict = params.get('dict', {})
        keys = params.get('keys', [])
        mode = params.get('mode', 'include')
        output_var = params.get('output_var', 'filtered_dict')

        try:
            resolved_dict = context.resolve_value(input_dict)
            resolved_keys = context.resolve_value(keys) if keys else []

            if not isinstance(resolved_dict, dict):
                return ActionResult(
                    success=False,
                    message="过滤字典失败: 输入不是字典"
                )

            if not isinstance(resolved_keys, (list, tuple)):
                resolved_keys = [resolved_keys]

            if mode == 'include':
                result = {k: resolved_dict[k] for k in resolved_keys if k in resolved_dict}
            else:
                result = {k: v for k, v in resolved_dict.items() if k not in resolved_keys}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤字典: {len(result)}个键",
                data={
                    'original': resolved_dict,
                    'filtered': result,
                    'mode': mode,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict', 'keys']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'include', 'output_var': 'filtered_dict'}


class DictMapAction(BaseAction):
    """Map function to dictionary values."""
    action_type = "dict8_map"
    display_name = "映射字典值"
    description = "对字典值应用映射函数"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute map.

        Args:
            context: Execution context.
            params: Dict with dict, func, keys, output_var.

        Returns:
            ActionResult with mapped dictionary.
        """
        input_dict = params.get('dict', {})
        func = params.get('func', 'upper')
        keys = params.get('keys', None)
        output_var = params.get('output_var', 'mapped_dict')

        try:
            resolved_dict = context.resolve_value(input_dict)
            resolved_func = context.resolve_value(func) if func else 'upper'
            resolved_keys = context.resolve_value(keys) if keys else None

            if not isinstance(resolved_dict, dict):
                return ActionResult(
                    success=False,
                    message="映射字典失败: 输入不是字典"
                )

            result = dict(resolved_dict)

            if resolved_keys is None:
                keys_to_map = result.keys()
            elif isinstance(resolved_keys, (list, tuple)):
                keys_to_map = resolved_keys
            else:
                keys_to_map = [resolved_keys]

            for key in keys_to_map:
                if key in result:
                    value = result[key]
                    if isinstance(value, str):
                        if resolved_func == 'upper':
                            result[key] = value.upper()
                        elif resolved_func == 'lower':
                            result[key] = value.lower()
                        elif resolved_func == 'title':
                            result[key] = value.title()
                        elif resolved_func == 'strip':
                            result[key] = value.strip()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"映射字典: {len(result)}个值",
                data={
                    'original': resolved_dict,
                    'mapped': result,
                    'function': resolved_func,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"映射字典值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict', 'func']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'keys': None, 'output_var': 'mapped_dict'}


class DictInvertAction(BaseAction):
    """Invert dictionary keys and values."""
    action_type = "dict8_invert"
    display_name = "反转字典"
    description = "交换字典的键和值"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute invert.

        Args:
            context: Execution context.
            params: Dict with dict, output_var.

        Returns:
            ActionResult with inverted dictionary.
        """
        input_dict = params.get('dict', {})
        output_var = params.get('output_var', 'inverted_dict')

        try:
            resolved_dict = context.resolve_value(input_dict)

            if not isinstance(resolved_dict, dict):
                return ActionResult(
                    success=False,
                    message="反转字典失败: 输入不是字典"
                )

            result = {v: k for k, v in resolved_dict.items()}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转字典: {len(result)}个键值对",
                data={
                    'original': resolved_dict,
                    'inverted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'inverted_dict'}


class DictDeepGetAction(BaseAction):
    """Get nested dictionary value."""
    action_type = "dict8_deep_get"
    display_name = "获取嵌套值"
    description = "获取嵌套字典的值"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deep get.

        Args:
            context: Execution context.
            params: Dict with dict, path, default, output_var.

        Returns:
            ActionResult with nested value.
        """
        input_dict = params.get('dict', {})
        path = params.get('path', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'deep_value')

        try:
            resolved_dict = context.resolve_value(input_dict)
            resolved_path = context.resolve_value(path) if path else ''
            resolved_default = context.resolve_value(default) if default is not None else None

            if not isinstance(resolved_dict, dict):
                return ActionResult(
                    success=False,
                    message="获取嵌套值失败: 输入不是字典"
                )

            if not resolved_path:
                return ActionResult(
                    success=False,
                    message="获取嵌套值失败: 路径不能为空"
                )

            keys = resolved_path.split('.')
            result = resolved_dict

            for key in keys:
                if isinstance(result, dict) and key in result:
                    result = result[key]
                else:
                    result = resolved_default
                    break

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取嵌套值: {resolved_path}",
                data={
                    'path': resolved_path,
                    'value': result,
                    'found': result != resolved_default,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取嵌套值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict', 'path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'deep_value'}
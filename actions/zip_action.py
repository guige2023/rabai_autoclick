"""Zip action module for RabAI AutoClick.

Provides zip/unzip operations:
- ZipListsAction: Zip lists together
- ZipUnpackAction: Unpack zipped list
- ZipDictAction: Create dict from paired lists
- ZipEnumerateAction: Enumerate with index
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ZipListsAction(BaseAction):
    """Zip lists together."""
    action_type = "zip_lists"
    display_name = "合并列表"
    description = "将多个列表配对合并"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zip.

        Args:
            context: Execution context.
            params: Dict with lists, output_var.

        Returns:
            ActionResult with zipped list.
        """
        lists = params.get('lists', [])
        output_var = params.get('output_var', 'zipped_list')

        try:
            resolved_lists = [context.resolve_value(lst) for lst in lists]

            list_objects = []
            for lst in resolved_lists:
                items = context.get(lst) if isinstance(lst, str) else lst
                if not isinstance(items, (list, tuple)):
                    items = [items]
                list_objects.append(items)

            min_len = min(len(lst) for lst in list_objects)
            result = [tuple(lst[i] for lst in list_objects) for i in range(min_len)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表合并完成: {len(result)} 组",
                data={
                    'input_count': len(list_objects),
                    'result_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['lists']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zipped_list'}


class ZipUnpackAction(BaseAction):
    """Unpack zipped list."""
    action_type = "zip_unpack"
    display_name = "解包列表"
    description = "将配对列表解包为多个列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unpack.

        Args:
            context: Execution context.
            params: Dict with zipped_var, output_vars.

        Returns:
            ActionResult with unpacked lists.
        """
        zipped_var = params.get('zipped_var', '')
        output_vars = params.get('output_vars', [])

        valid, msg = self.validate_type(zipped_var, str, 'zipped_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(zipped_var)
            resolved_outputs = [context.resolve_value(v) for v in output_vars]

            zipped = context.get(resolved_var)
            if not isinstance(zipped, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            if len(zipped) == 0:
                return ActionResult(
                    success=False,
                    message="空列表无法解包"
                )

            num_lists = len(zipped[0])
            if len(resolved_outputs) < num_lists:
                resolved_outputs = [f'unpacked_{i}' for i in range(num_lists)]

            for i in range(num_lists):
                result_list = [item[i] for item in zipped]
                context.set(resolved_outputs[i], result_list)

            return ActionResult(
                success=True,
                message=f"解包完成: {num_lists} 个列表",
                data={
                    'zipped_count': len(zipped),
                    'unpacked_count': num_lists,
                    'output_vars': resolved_outputs
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解包列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zipped_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_vars': []}


class ZipDictAction(BaseAction):
    """Create dict from paired lists."""
    action_type = "zip_dict"
    display_name = "创建字典"
    description = "从键列表和值列表创建字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create dict.

        Args:
            context: Execution context.
            params: Dict with keys_var, values_var, output_var.

        Returns:
            ActionResult with created dict.
        """
        keys_var = params.get('keys_var', '')
        values_var = params.get('values_var', '')
        output_var = params.get('output_var', 'created_dict')

        valid, msg = self.validate_type(keys_var, str, 'keys_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(values_var, str, 'values_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_keys = context.resolve_value(keys_var)
            resolved_values = context.resolve_value(values_var)

            keys = context.get(resolved_keys) if isinstance(resolved_keys, str) else resolved_keys
            values = context.get(resolved_values) if isinstance(resolved_values, str) else resolved_values

            if not isinstance(keys, (list, tuple)):
                keys = [keys]
            if not isinstance(values, (list, tuple)):
                values = [values]

            result = dict(zip(keys, values))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典创建: {len(result)} 键值对",
                data={
                    'key_count': len(keys),
                    'value_count': len(values),
                    'dict_size': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['keys_var', 'values_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'created_dict'}


class ZipEnumerateAction(BaseAction):
    """Enumerate with index."""
    action_type = "zip_enumerate"
    display_name = "枚举列表"
    description = "枚举列表元素及其索引"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enumerate.

        Args:
            context: Execution context.
            params: Dict with list_var, start, output_var.

        Returns:
            ActionResult with enumerated list.
        """
        list_var = params.get('list_var', '')
        start = params.get('start', 0)
        output_var = params.get('output_var', 'enumerated_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_start = int(context.resolve_value(start))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            result = [(i, item) for i, item in enumerate(items, start=resolved_start)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"枚举完成: {len(result)} 项",
                data={
                    'count': len(result),
                    'start': resolved_start,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"枚举列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'output_var': 'enumerated_list'}

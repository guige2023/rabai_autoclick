"""List2 action module for RabAI AutoClick.

Provides additional list operations:
- ListConcatAction: Concatenate lists
- ListSliceAction: Slice list
- ListTakeAction: Take first n elements
- ListDropAction: Drop first n elements
- ListFlattenAction: Flatten nested list
- ListZipAction: Zip lists
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListConcatAction(BaseAction):
    """Concatenate lists."""
    action_type = "list_concat"
    display_name = "连接列表"
    description = "连接多个列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute concat.

        Args:
            context: Execution context.
            params: Dict with lists, output_var.

        Returns:
            ActionResult with concatenated list.
        """
        lists = params.get('lists', [])
        output_var = params.get('output_var', 'concat_result')

        try:
            resolved_lists = [context.resolve_value(lst) for lst in lists]

            result = []
            for lst in resolved_lists:
                items = context.get(lst) if isinstance(lst, str) else lst
                if isinstance(items, (list, tuple)):
                    result.extend(items)
                else:
                    result.append(items)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表连接: {len(result)} 项",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['lists']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'concat_result'}


class ListSliceAction(BaseAction):
    """Slice list."""
    action_type = "list_slice"
    display_name = "切片列表"
    description = "获取列表切片"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute slice.

        Args:
            context: Execution context.
            params: Dict with list_var, start, end, step, output_var.

        Returns:
            ActionResult with sliced list.
        """
        list_var = params.get('list_var', '')
        start = params.get('start', 0)
        end = params.get('end', None)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'slice_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_start = int(context.resolve_value(start))
            resolved_end = int(context.resolve_value(end)) if end is not None else None
            resolved_step = int(context.resolve_value(step))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = items[resolved_start:resolved_end:resolved_step]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表切片: {len(result)} 项",
                data={
                    'start': resolved_start,
                    'end': resolved_end,
                    'step': resolved_step,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"切片列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'end': None, 'step': 1, 'output_var': 'slice_result'}


class ListTakeAction(BaseAction):
    """Take first n elements."""
    action_type = "list_take"
    display_name = "获取前N项"
    description = "获取列表前N个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute take.

        Args:
            context: Execution context.
            params: Dict with list_var, count, output_var.

        Returns:
            ActionResult with taken elements.
        """
        list_var = params.get('list_var', '')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'take_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_count = int(context.resolve_value(count))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            result = list(items)[:resolved_count]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取前{resolved_count}项: {len(result)} 项",
                data={
                    'count': resolved_count,
                    'result_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取前N项失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'take_result'}


class ListDropAction(BaseAction):
    """Drop first n elements."""
    action_type = "list_drop"
    display_name = "删除前N项"
    description: "删除列表前N个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute drop.

        Args:
            context: Execution context.
            params: Dict with list_var, count, output_var.

        Returns:
            ActionResult with remaining elements.
        """
        list_var = params.get('list_var', '')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'drop_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_count = int(context.resolve_value(count))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = list(items)[resolved_count:]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"删除前{resolved_count}项: {len(result)} 项剩余",
                data={
                    'count': resolved_count,
                    'result_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除前N项失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'drop_result'}


class ListFlattenAction(BaseAction):
    """Flatten nested list."""
    action_type = "list_flatten"
    display_name = "扁平化列表"
    description: "将嵌套列表展平"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with flattened list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'flatten_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            def flatten(lst):
                result = []
                for item in lst:
                    if isinstance(item, (list, tuple)):
                        result.extend(flatten(item))
                    else:
                        result.append(item)
                return result

            result = flatten(items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表扁平化: {len(items)} -> {len(result)}",
                data={
                    'original_count': len(items),
                    'result_count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扁平化列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'flatten_result'}


class ListZipAction(BaseAction):
    """Zip lists."""
    action_type = "list_zip"
    display_name: "打包列表"
    description = "将多个列表打包成元组列表"

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
            ActionResult with zipped lists.
        """
        lists = params.get('lists', [])
        output_var = params.get('output_var', 'zip_result')

        try:
            resolved_lists = [context.resolve_value(lst) for lst in lists]

            list_objects = []
            for lst in resolved_lists:
                items = context.get(lst) if isinstance(lst, str) else lst
                if not isinstance(items, (list, tuple)):
                    items = [items]
                list_objects.append(items)

            min_len = min(len(lst) for lst in list_objects) if list_objects else 0
            result = [tuple(lst[i] for lst in list_objects) for i in range(min_len)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表打包: {len(result)} 组",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"打包列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['lists']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zip_result'}

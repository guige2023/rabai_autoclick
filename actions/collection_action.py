"""Collection action module for RabAI AutoClick.

Provides collection operations:
- CollectionZipAction: Zip multiple lists
- CollectionUnzipAction: Unzip list of tuples
- CollectionEnumerateAction: Enumerate list
- CollectionRangeAction: Create range
- CollectionRepeatAction: Repeat elements
- CollectionCycleAction: Cycle through elements
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CollectionZipAction(BaseAction):
    """Zip multiple lists."""
    action_type = "collection_zip"
    display_name = "合并列表"
    description = "将多个列表合并为一个"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zip operation.

        Args:
            context: Execution context.
            params: Dict with lists, output_var.

        Returns:
            ActionResult with zipped list.
        """
        lists = params.get('lists', [])
        output_var = params.get('output_var', 'collection_result')

        valid, msg = self.validate_type(lists, (list, tuple), 'lists')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(lists) < 2:
            return ActionResult(
                success=False,
                message="至少需要2个列表"
            )

        try:
            resolved_lists = [context.resolve_value(lst) for lst in lists]
            result = list(zip(*resolved_lists))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表合并完成: {len(result)} 组",
                data={
                    'result': result,
                    'count': len(result),
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
        return {'output_var': 'collection_result'}


class CollectionUnzipAction(BaseAction):
    """Unzip list of tuples."""
    action_type = "collection_unzip"
    display_name = "拆分列表"
    description = "将合并的列表拆分"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unzip operation.

        Args:
            context: Execution context.
            params: Dict with zipped, output_var.

        Returns:
            ActionResult with unzipped lists.
        """
        zipped = params.get('zipped', [])
        output_var = params.get('output_var', 'collection_result')

        valid, msg = self.validate_type(zipped, (list, tuple), 'zipped')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(zipped)
            result = [list(x) for x in zip(*resolved)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表拆分完成: {len(result)} 个列表",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"拆分列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zipped']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'collection_result'}


class CollectionEnumerateAction(BaseAction):
    """Enumerate list."""
    action_type = "collection_enumerate"
    display_name = "枚举列表"
    description = "枚举列表元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute enumerate operation.

        Args:
            context: Execution context.
            params: Dict with items, start, output_var.

        Returns:
            ActionResult with enumerated list.
        """
        items = params.get('items', [])
        start = params.get('start', 0)
        output_var = params.get('output_var', 'collection_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_start = context.resolve_value(start)
            result = list(enumerate(resolved_items, start=int(resolved_start)))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表枚举完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"枚举列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'output_var': 'collection_result'}


class CollectionRangeAction(BaseAction):
    """Create range."""
    action_type = "collection_range"
    display_name = "创建范围"
    description = "创建数字范围"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute range creation.

        Args:
            context: Execution context.
            params: Dict with start, stop, step, output_var.

        Returns:
            ActionResult with range list.
        """
        start = params.get('start', 0)
        stop = params.get('stop', 10)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'collection_result')

        try:
            resolved_start = context.resolve_value(start)
            resolved_stop = context.resolve_value(stop)
            resolved_step = context.resolve_value(step)

            result = list(range(int(resolved_start), int(resolved_stop), int(resolved_step)))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围创建完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建范围失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['start', 'stop']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'step': 1, 'output_var': 'collection_result'}


class CollectionRepeatAction(BaseAction):
    """Repeat elements."""
    action_type = "collection_repeat"
    display_name = "重复元素"
    description = "重复元素指定次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute repeat operation.

        Args:
            context: Execution context.
            params: Dict with item, times, output_var.

        Returns:
            ActionResult with repeated list.
        """
        item = params.get('item', None)
        times = params.get('times', 1)
        output_var = params.get('output_var', 'collection_result')

        try:
            resolved_item = context.resolve_value(item)
            resolved_times = context.resolve_value(times)
            result = [resolved_item] * int(resolved_times)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"重复完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重复元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['item', 'times']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'collection_result'}


class CollectionCycleAction(BaseAction):
    """Cycle through elements."""
    action_type = "collection_cycle"
    display_name = "循环列表"
    description = "循环遍历列表元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cycle operation.

        Args:
            context: Execution context.
            params: Dict with items, count, output_var.

        Returns:
            ActionResult with cycled list.
        """
        items = params.get('items', [])
        count = params.get('count', 10)
        output_var = params.get('output_var', 'collection_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(items) == 0:
            return ActionResult(
                success=False,
                message="列表不能为空"
            )

        try:
            resolved_items = context.resolve_value(items)
            resolved_count = context.resolve_value(count)

            result = []
            for i in range(int(resolved_count)):
                result.append(resolved_items[i % len(resolved_items)])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"循环完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"循环列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'collection_result'}
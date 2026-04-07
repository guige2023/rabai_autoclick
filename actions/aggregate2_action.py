"""Aggregate2 action module for RabAI AutoClick.

Provides advanced aggregation operations:
- AggregateJoinAction: Join list elements
- AggregateSplitAction: Split string into list
- AggregatePartitionAction: Partition list into chunks
- AggregateWindowAction: Create sliding window
- AggregateGroupAction: Group by function
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateJoinAction(BaseAction):
    """Join list elements."""
    action_type = "aggregate_join"
    display_name = "连接列表"
    description = "将列表元素连接成字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join.

        Args:
            context: Execution context.
            params: Dict with list_var, separator, output_var.

        Returns:
            ActionResult with joined string.
        """
        list_var = params.get('list_var', '')
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'joined_string')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_sep = context.resolve_value(separator)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = str(items)

            result = resolved_sep.join(str(item) for item in items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接完成: {len(result)} 字符",
                data={
                    'item_count': len(items),
                    'result_length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '', 'output_var': 'joined_string'}


class AggregateSplitAction(BaseAction):
    """Split string into list."""
    action_type = "aggregate_split"
    display_name = "分割字符串"
    description = "将字符串分割成列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split.

        Args:
            context: Execution context.
            params: Dict with string_var, separator, output_var.

        Returns:
            ActionResult with split list.
        """
        string_var = params.get('string_var', '')
        separator = params.get('separator', ' ')
        output_var = params.get('output_var', 'split_list')

        valid, msg = self.validate_type(string_var, str, 'string_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(string_var)
            resolved_sep = context.resolve_value(separator)

            if not isinstance(resolved_var, str):
                resolved_var = str(resolved_var)

            result = resolved_var.split(resolved_sep)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分割完成: {len(result)} 项",
                data={
                    'original_length': len(resolved_var),
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['string_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': ' ', 'output_var': 'split_list'}


class AggregatePartitionAction(BaseAction):
    """Partition list into chunks."""
    action_type = "aggregate_partition"
    display_name = "分区列表"
    description = "将列表分成多个块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute partition.

        Args:
            context: Execution context.
            params: Dict with list_var, size, output_var.

        Returns:
            ActionResult with partitioned list.
        """
        list_var = params.get('list_var', '')
        size = params.get('size', 2)
        output_var = params.get('output_var', 'partitioned_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_size = int(context.resolve_value(size))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = [items[i:i + resolved_size] for i in range(0, len(items), resolved_size)]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分区完成: {len(result)} 块",
                data={
                    'original_count': len(items),
                    'partition_count': len(result),
                    'partition_size': resolved_size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分区列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'partitioned_list'}


class AggregateWindowAction(BaseAction):
    """Create sliding window."""
    action_type = "aggregate_window"
    display_name = "滑动窗口"
    description = "创建滑动窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute window.

        Args:
            context: Execution context.
            params: Dict with list_var, size, step, output_var.

        Returns:
            ActionResult with windowed list.
        """
        list_var = params.get('list_var', '')
        size = params.get('size', 2)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'windowed_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_size = int(context.resolve_value(size))
            resolved_step = int(context.resolve_value(step))

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            result = []
            for i in range(0, len(items) - resolved_size + 1, resolved_step):
                result.append(items[i:i + resolved_size])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"滑动窗口完成: {len(result)} 窗口",
                data={
                    'original_count': len(items),
                    'window_count': len(result),
                    'window_size': resolved_size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建滑动窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'step': 1, 'output_var': 'windowed_list'}


class AggregateGroupAction(BaseAction):
    """Group by function."""
    action_type = "aggregate_group"
    display_name = "分组"
    description = "按函数分组列表元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute group.

        Args:
            context: Execution context.
            params: Dict with list_var, key_func, output_var.

        Returns:
            ActionResult with grouped dict.
        """
        list_var = params.get('list_var', '')
        key_func = params.get('key_func', 'lambda x: x')
        output_var = params.get('output_var', 'grouped_dict')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_key = context.resolve_value(key_func)

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是列表"
                )

            key_fn = context.safe_exec(f"return_value = {resolved_key}")

            groups = {}
            for item in items:
                key = key_fn(item)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)

            context.set(output_var, groups)

            return ActionResult(
                success=True,
                message=f"分组完成: {len(groups)} 组",
                data={
                    'original_count': len(items),
                    'group_count': len(groups),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key_func': 'lambda x: x', 'output_var': 'grouped_dict'}

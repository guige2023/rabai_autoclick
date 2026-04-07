"""Set3 action module for RabAI AutoClick.

Provides additional set operations:
- SetCreateAction: Create a set
- SetAddAction: Add element to set
- SetRemoveAction: Remove element from set
- SetUnionAction: Union of sets
- SetIntersectionAction: Intersection of sets
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SetCreateAction(BaseAction):
    """Create a set."""
    action_type = "set3_create"
    display_name = "创建集合"
    description = "从列表创建集合"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set create.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with created set.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'set_result')

        try:
            resolved = context.resolve_value(items)

            if isinstance(resolved, list):
                result = set(resolved)
            elif isinstance(resolved, set):
                result = resolved
            elif isinstance(resolved, tuple):
                result = set(resolved)
            else:
                result = {resolved}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建集合: {len(result)} 个元素",
                data={
                    'items': resolved,
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_result'}


class SetAddAction(BaseAction):
    """Add element to set."""
    action_type = "set3_add"
    display_name = "添加元素"
    description = "向集合添加元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set add.

        Args:
            context: Execution context.
            params: Dict with set_var, item, output_var.

        Returns:
            ActionResult with modified set.
        """
        set_var = params.get('set_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'set_result')

        valid, msg = self.validate_type(set_var, str, 'set_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set = context.resolve_value(set_var)
            resolved_item = context.resolve_value(item) if item is not None else None

            s = context.get(resolved_set) if isinstance(resolved_set, str) else resolved_set

            if not isinstance(s, set):
                return ActionResult(
                    success=False,
                    message="set_var 必须是集合类型"
                )

            s.add(resolved_item)
            context.set(output_var, s)

            return ActionResult(
                success=True,
                message=f"添加元素: {resolved_item}",
                data={
                    'item': resolved_item,
                    'result': s,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_result'}


class SetRemoveAction(BaseAction):
    """Remove element from set."""
    action_type = "set3_remove"
    display_name = "移除元素"
    description = "从集合移除元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set remove.

        Args:
            context: Execution context.
            params: Dict with set_var, item, output_var.

        Returns:
            ActionResult with modified set.
        """
        set_var = params.get('set_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'set_result')

        valid, msg = self.validate_type(set_var, str, 'set_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set = context.resolve_value(set_var)
            resolved_item = context.resolve_value(item) if item is not None else None

            s = context.get(resolved_set) if isinstance(resolved_set, str) else resolved_set

            if not isinstance(s, set):
                return ActionResult(
                    success=False,
                    message="set_var 必须是集合类型"
                )

            existed = resolved_item in s
            s.discard(resolved_item)
            context.set(output_var, s)

            return ActionResult(
                success=True,
                message=f"移除元素: {'成功' if existed else '元素不存在'}",
                data={
                    'item': resolved_item,
                    'existed': existed,
                    'result': s,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_result'}


class SetUnionAction(BaseAction):
    """Union of sets."""
    action_type = "set3_union"
    display_name = "集合并集"
    description = "计算两个集合的并集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set union.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with union set.
        """
        set1_var = params.get('set1', '')
        set2_var = params.get('set2', '')
        output_var = params.get('output_var', 'union_result')

        valid, msg = self.validate_type(set1_var, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2_var, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set1 = context.resolve_value(set1_var)
            resolved_set2 = context.resolve_value(set2_var)

            s1 = context.get(resolved_set1) if isinstance(resolved_set1, str) else resolved_set1
            s2 = context.get(resolved_set2) if isinstance(resolved_set2, str) else resolved_set2

            if not isinstance(s1, (set, list, tuple)):
                s1 = {s1}
            if not isinstance(s2, (set, list, tuple)):
                s2 = {s2}

            result = set(s1) | set(s2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"集合并集: {len(result)} 个元素",
                data={
                    'set1': s1,
                    'set2': s2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合并集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'union_result'}


class SetIntersectionAction(BaseAction):
    """Intersection of sets."""
    action_type = "set3_intersection"
    display_name = "集合交集"
    description = "计算两个集合的交集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set intersection.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with intersection set.
        """
        set1_var = params.get('set1', '')
        set2_var = params.get('set2', '')
        output_var = params.get('output_var', 'intersection_result')

        valid, msg = self.validate_type(set1_var, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2_var, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set1 = context.resolve_value(set1_var)
            resolved_set2 = context.resolve_value(set2_var)

            s1 = context.get(resolved_set1) if isinstance(resolved_set1, str) else resolved_set1
            s2 = context.get(resolved_set2) if isinstance(resolved_set2, str) else resolved_set2

            if not isinstance(s1, (set, list, tuple)):
                s1 = {s1}
            if not isinstance(s2, (set, list, tuple)):
                s2 = {s2}

            result = set(s1) & set(s2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"集合交集: {len(result)} 个元素",
                data={
                    'set1': s1,
                    'set2': s2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合交集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'intersection_result'}
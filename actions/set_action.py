"""Set action module for RabAI AutoClick.

Provides set operations:
- SetCreateAction: Create a new set
- SetAddAction: Add item to set
- SetRemoveAction: Remove item from set
- SetUnionAction: Union of two sets
- SetIntersectionAction: Intersection of two sets
- SetDifferenceAction: Difference of two sets
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SetCreateAction(BaseAction):
    """Create a new set."""
    action_type = "set_create"
    display_name = "创建集合"
    description = "创建一个新的集合"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute creating a set.

        Args:
            context: Execution context.
            params: Dict with set_var, items.

        Returns:
            ActionResult indicating success.
        """
        set_var = params.get('set_var', 'items')
        items = params.get('items', [])

        valid, msg = self.validate_type(set_var, str, 'set_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            result_set = set(items)
            context.set(set_var, result_set)

            return ActionResult(
                success=True,
                message=f"已创建集合 {set_var}: {len(result_set)} 项",
                data={
                    'set': list(result_set),
                    'count': len(result_set),
                    'output_var': set_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'items': []}


class SetAddAction(BaseAction):
    """Add item to set."""
    action_type = "set_add"
    display_name = "集合添加"
    description = "向集合添加元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute adding to set.

        Args:
            context: Execution context.
            params: Dict with set_var, item.

        Returns:
            ActionResult indicating success.
        """
        set_var = params.get('set_var', 'items')
        item = params.get('item', None)

        valid, msg = self.validate_type(set_var, str, 'set_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_set = context.get(set_var, set())

            if not isinstance(current_set, set):
                current_set = set()

            current_set.add(item)
            context.set(set_var, current_set)

            return ActionResult(
                success=True,
                message=f"已添加元素到 {set_var}: {len(current_set)} 项",
                data={
                    'set': list(current_set),
                    'count': len(current_set)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合添加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class SetRemoveAction(BaseAction):
    """Remove item from set."""
    action_type = "set_remove"
    display_name = "集合移除"
    description = "从集合移除元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute removing from set.

        Args:
            context: Execution context.
            params: Dict with set_var, item.

        Returns:
            ActionResult indicating success.
        """
        set_var = params.get('set_var', 'items')
        item = params.get('item', None)

        valid, msg = self.validate_type(set_var, str, 'set_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_set = context.get(set_var, set())

            if not isinstance(current_set, set):
                return ActionResult(
                    success=False,
                    message=f"变量 {set_var} 不是集合"
                )

            if item not in current_set:
                return ActionResult(
                    success=False,
                    message=f"元素不在集合中: {item}"
                )

            current_set.discard(item)
            context.set(set_var, current_set)

            return ActionResult(
                success=True,
                message=f"已从 {set_var} 移除元素: {len(current_set)} 项",
                data={
                    'set': list(current_set),
                    'count': len(current_set)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合移除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class SetUnionAction(BaseAction):
    """Union of two sets."""
    action_type = "set_union"
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
            params: Dict with set1_var, set2_var, output_var.

        Returns:
            ActionResult with union set.
        """
        set1_var = params.get('set1_var', 'set1')
        set2_var = params.get('set2_var', 'set2')
        output_var = params.get('output_var', 'union_set')

        valid, msg = self.validate_type(set1_var, str, 'set1_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2_var, str, 'set2_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            set1 = context.get(set1_var, set())
            set2 = context.get(set2_var, set())

            if not isinstance(set1, set):
                set1 = set()
            if not isinstance(set2, set):
                set2 = set()

            result = set1 | set2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"并集完成: {len(result)} 项",
                data={
                    'union': list(result),
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合并集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1_var', 'set2_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'union_set'}


class SetIntersectionAction(BaseAction):
    """Intersection of two sets."""
    action_type = "set_intersection"
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
            params: Dict with set1_var, set2_var, output_var.

        Returns:
            ActionResult with intersection set.
        """
        set1_var = params.get('set1_var', 'set1')
        set2_var = params.get('set2_var', 'set2')
        output_var = params.get('output_var', 'intersection_set')

        valid, msg = self.validate_type(set1_var, str, 'set1_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2_var, str, 'set2_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            set1 = context.get(set1_var, set())
            set2 = context.get(set2_var, set())

            if not isinstance(set1, set):
                set1 = set()
            if not isinstance(set2, set):
                set2 = set()

            result = set1 & set2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"交集完成: {len(result)} 项",
                data={
                    'intersection': list(result),
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合交集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1_var', 'set2_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'intersection_set'}


class SetDifferenceAction(BaseAction):
    """Difference of two sets."""
    action_type = "set_difference"
    display_name = "集合差集"
    description = "计算两个集合的差集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set difference.

        Args:
            context: Execution context.
            params: Dict with set1_var, set2_var, output_var.

        Returns:
            ActionResult with difference set.
        """
        set1_var = params.get('set1_var', 'set1')
        set2_var = params.get('set2_var', 'set2')
        output_var = params.get('output_var', 'difference_set')

        valid, msg = self.validate_type(set1_var, str, 'set1_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2_var, str, 'set2_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            set1 = context.get(set1_var, set())
            set2 = context.get(set2_var, set())

            if not isinstance(set1, set):
                set1 = set()
            if not isinstance(set2, set):
                set2 = set()

            result = set1 - set2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"差集完成: {len(result)} 项",
                data={
                    'difference': list(result),
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1_var', 'set2_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'difference_set'}
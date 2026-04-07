"""Set2 action module for RabAI AutoClick.

Provides advanced set operations:
- Set2CreateAction: Create set
- Set2AddAction: Add to set
- Set2RemoveAction: Remove from set
- Set2UnionAction: Union of sets
- Set2IntersectionAction: Intersection of sets
- Set2DifferenceAction: Difference of sets
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Set2CreateAction(BaseAction):
    """Create set."""
    action_type = "set2_create"
    display_name = "创建集合"
    description = "创建集合"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, items.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', '')
        items = params.get('items', [])

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_items = context.resolve_value(items)

            context.set(f'_set_{resolved_name}', set(resolved_items))

            return ActionResult(
                success=True,
                message=f"集合 {resolved_name} 创建: {len(resolved_items)} 项",
                data={
                    'name': resolved_name,
                    'count': len(resolved_items)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'items': []}


class Set2AddAction(BaseAction):
    """Add to set."""
    action_type = "set2_add"
    display_name = "添加集合元素"
    description = "添加元素到集合"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with name, item.

        Returns:
            ActionResult indicating added.
        """
        name = params.get('name', '')
        item = params.get('item', None)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_item = context.resolve_value(item)

            s = context.get(f'_set_{resolved_name}', set())
            s.add(resolved_item)
            context.set(f'_set_{resolved_name}', s)

            return ActionResult(
                success=True,
                message=f"添加元素到集合 {resolved_name}: {len(s)} 项",
                data={
                    'name': resolved_name,
                    'item': resolved_item,
                    'count': len(s)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加集合元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class Set2RemoveAction(BaseAction):
    """Remove from set."""
    action_type = "set2_remove"
    display_name = "移除集合元素"
    description = "从集合移除元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove.

        Args:
            context: Execution context.
            params: Dict with name, item.

        Returns:
            ActionResult indicating removed.
        """
        name = params.get('name', '')
        item = params.get('item', None)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_item = context.resolve_value(item)

            s = context.get(f'_set_{resolved_name}', set())
            if resolved_item in s:
                s.remove(resolved_item)
                context.set(f'_set_{resolved_name}', s)

            return ActionResult(
                success=True,
                message=f"从集合 {resolved_name} 移除元素",
                data={
                    'name': resolved_name,
                    'item': resolved_item,
                    'count': len(s)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除集合元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class Set2UnionAction(BaseAction):
    """Union of sets."""
    action_type = "set2_union"
    display_name = "集合并集"
    description = "计算集合并集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute union.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_name.

        Returns:
            ActionResult with union result.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_name = params.get('output_name', 'union_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}', set())
            s2 = context.get(f'_set_{resolved_set2}', set())

            result = s1 | s2
            context.set(f'_set_{output_name}', result)

            return ActionResult(
                success=True,
                message=f"集合并集: {len(result)} 项",
                data={
                    'set1': resolved_set1,
                    'set2': resolved_set2,
                    'output_name': output_name,
                    'count': len(result)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算集合并集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_name': 'union_result'}


class Set2IntersectionAction(BaseAction):
    """Intersection of sets."""
    action_type = "set2_intersection"
    display_name = "集合交集"
    description = "计算集合交集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute intersection.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_name.

        Returns:
            ActionResult with intersection result.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_name = params.get('output_name', 'intersection_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}', set())
            s2 = context.get(f'_set_{resolved_set2}', set())

            result = s1 & s2
            context.set(f'_set_{output_name}', result)

            return ActionResult(
                success=True,
                message=f"集合交集: {len(result)} 项",
                data={
                    'set1': resolved_set1,
                    'set2': resolved_set2,
                    'output_name': output_name,
                    'count': len(result)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算集合交集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_name': 'intersection_result'}


class Set2DifferenceAction(BaseAction):
    """Difference of sets."""
    action_type = "set2_difference"
    display_name = "集合差集"
    description = "计算集合差集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute difference.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_name.

        Returns:
            ActionResult with difference result.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_name = params.get('output_name', 'difference_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}', set())
            s2 = context.get(f'_set_{resolved_set2}', set())

            result = s1 - s2
            context.set(f'_set_{output_name}', result)

            return ActionResult(
                success=True,
                message=f"集合差集: {len(result)} 项",
                data={
                    'set1': resolved_set1,
                    'set2': resolved_set2,
                    'output_name': output_name,
                    'count': len(result)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算集合差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_name': 'difference_result'}

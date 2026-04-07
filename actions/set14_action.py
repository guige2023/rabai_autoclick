"""Set14 action module for RabAI AutoClick.

Provides additional set operations:
- SetAddAction: Add to set
- SetRemoveAction: Remove from set
- SetContainsAction: Check membership
- SetUnionAction: Union of sets
- SetIntersectionAction: Intersection of sets
- SetDifferenceAction: Difference of sets
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SetAddAction(BaseAction):
    """Add to set."""
    action_type = "set14_add"
    display_name = "集合添加"
    description = "添加到集合"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with set_name, value, output_var.

        Returns:
            ActionResult with add result.
        """
        set_name = params.get('set_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'add_result')

        try:
            resolved_set = context.resolve_value(set_name) if set_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_sets'):
                context._sets = {}

            if resolved_set not in context._sets:
                context._sets[resolved_set] = set()

            context._sets[resolved_set].add(resolved_value)

            context.set(output_var, len(context._sets[resolved_set]))

            return ActionResult(
                success=True,
                message=f"集合添加: {resolved_value} -> {resolved_set} ({len(context._sets[resolved_set])}项)",
                data={
                    'set': resolved_set,
                    'value': resolved_value,
                    'size': len(context._sets[resolved_set]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合添加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'add_result'}


class SetRemoveAction(BaseAction):
    """Remove from set."""
    action_type = "set14_remove"
    display_name = "集合移除"
    description = "从集合移除"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove.

        Args:
            context: Execution context.
            params: Dict with set_name, value, output_var.

        Returns:
            ActionResult with remove result.
        """
        set_name = params.get('set_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'remove_result')

        try:
            resolved_set = context.resolve_value(set_name) if set_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_sets'):
                context._sets = {}

            if resolved_set not in context._sets:
                return ActionResult(
                    success=False,
                    message=f"集合不存在: {resolved_set}"
                )

            removed = resolved_value in context._sets[resolved_set]
            if removed:
                context._sets[resolved_set].discard(resolved_value)

            context.set(output_var, removed)

            return ActionResult(
                success=True,
                message=f"集合移除: {resolved_value} {'成功' if removed else '不存在'}",
                data={
                    'set': resolved_set,
                    'value': resolved_value,
                    'removed': removed,
                    'size': len(context._sets[resolved_set]),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合移除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'remove_result'}


class SetContainsAction(BaseAction):
    """Check membership."""
    action_type = "set14_contains"
    display_name = "集合包含"
    description = "检查集合包含"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains.

        Args:
            context: Execution context.
            params: Dict with set_name, value, output_var.

        Returns:
            ActionResult with contains result.
        """
        set_name = params.get('set_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'contains_result')

        try:
            resolved_set = context.resolve_value(set_name) if set_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_sets'):
                context._sets = {}

            if resolved_set not in context._sets:
                contains = False
            else:
                contains = resolved_value in context._sets[resolved_set]

            context.set(output_var, contains)

            return ActionResult(
                success=True,
                message=f"集合包含: {resolved_value} in {resolved_set} = {contains}",
                data={
                    'set': resolved_set,
                    'value': resolved_value,
                    'contains': contains,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合包含检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}


class SetUnionAction(BaseAction):
    """Union of sets."""
    action_type = "set14_union"
    display_name = "集合并集"
    description = "集合并集"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute union.

        Args:
            context: Execution context.
            params: Dict with set_name, other_set, output_var.

        Returns:
            ActionResult with union result.
        """
        set_name = params.get('set_name', 'default')
        other_set = params.get('other_set', 'other')
        output_var = params.get('output_var', 'union_result')

        try:
            resolved_set = context.resolve_value(set_name) if set_name else 'default'
            resolved_other = context.resolve_value(other_set) if other_set else 'other'

            if not hasattr(context, '_sets'):
                context._sets = {}

            set1 = context._sets.get(resolved_set, set())
            set2 = context._sets.get(resolved_other, set())

            result = set1.union(set2)

            context.set(output_var, list(result))

            return ActionResult(
                success=True,
                message=f"集合并集: {resolved_set} | {resolved_other} = {len(result)}项",
                data={
                    'set1': resolved_set,
                    'set2': resolved_other,
                    'result': list(result),
                    'size': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合并集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_name', 'other_set']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'union_result'}


class SetIntersectionAction(BaseAction):
    """Intersection of sets."""
    action_type = "set14_intersection"
    display_name = "集合交集"
    description = "集合交集"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute intersection.

        Args:
            context: Execution context.
            params: Dict with set_name, other_set, output_var.

        Returns:
            ActionResult with intersection result.
        """
        set_name = params.get('set_name', 'default')
        other_set = params.get('other_set', 'other')
        output_var = params.get('output_var', 'intersection_result')

        try:
            resolved_set = context.resolve_value(set_name) if set_name else 'default'
            resolved_other = context.resolve_value(other_set) if other_set else 'other'

            if not hasattr(context, '_sets'):
                context._sets = {}

            set1 = context._sets.get(resolved_set, set())
            set2 = context._sets.get(resolved_other, set())

            result = set1.intersection(set2)

            context.set(output_var, list(result))

            return ActionResult(
                success=True,
                message=f"集合交集: {resolved_set} & {resolved_other} = {len(result)}项",
                data={
                    'set1': resolved_set,
                    'set2': resolved_other,
                    'result': list(result),
                    'size': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合交集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_name', 'other_set']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'intersection_result'}


class SetDifferenceAction(BaseAction):
    """Difference of sets."""
    action_type = "set14_difference"
    display_name = "集合差集"
    description = "集合差集"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute difference.

        Args:
            context: Execution context.
            params: Dict with set_name, other_set, output_var.

        Returns:
            ActionResult with difference result.
        """
        set_name = params.get('set_name', 'default')
        other_set = params.get('other_set', 'other')
        output_var = params.get('output_var', 'difference_result')

        try:
            resolved_set = context.resolve_value(set_name) if set_name else 'default'
            resolved_other = context.resolve_value(other_set) if other_set else 'other'

            if not hasattr(context, '_sets'):
                context._sets = {}

            set1 = context._sets.get(resolved_set, set())
            set2 = context._sets.get(resolved_other, set())

            result = set1.difference(set2)

            context.set(output_var, list(result))

            return ActionResult(
                success=True,
                message=f"集合差集: {resolved_set} - {resolved_other} = {len(result)}项",
                data={
                    'set1': resolved_set,
                    'set2': resolved_other,
                    'result': list(result),
                    'size': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set_name', 'other_set']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'difference_result'}
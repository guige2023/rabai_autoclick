"""Set5 action module for RabAI AutoClick.

Provides additional set operations:
- SetIsSubsetAction: Check if subset
- SetIsSupersetAction: Check if superset
- SetIsDisjointAction: Check if disjoint
- SetIntersectionUpdateAction: Update set with intersection
- SetSymmetricDifferenceUpdateAction: Update set with symmetric difference
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SetIsSubsetAction(BaseAction):
    """Check if subset."""
    action_type = "set5_is_subset"
    display_name = "判断子集"
    description = "判断是否为子集"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is subset.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with subset result.
        """
        set1 = params.get('set1', [])
        set2 = params.get('set2', [])
        output_var = params.get('output_var', 'is_subset')

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            if not isinstance(resolved1, (list, tuple, set)):
                resolved1 = {resolved1}
            if not isinstance(resolved2, (list, tuple, set)):
                resolved2 = {resolved2}

            result = set(resolved1).issubset(set(resolved2))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"子集判断: {'是' if result else '否'}",
                data={
                    'set1': list(resolved1),
                    'set2': list(resolved2),
                    'is_subset': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"子集判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_subset'}


class SetIsSupersetAction(BaseAction):
    """Check if superset."""
    action_type = "set5_is_superset"
    display_name = "判断父集"
    description = "判断是否为父集"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is superset.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with superset result.
        """
        set1 = params.get('set1', [])
        set2 = params.get('set2', [])
        output_var = params.get('output_var', 'is_superset')

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            if not isinstance(resolved1, (list, tuple, set)):
                resolved1 = {resolved1}
            if not isinstance(resolved2, (list, tuple, set)):
                resolved2 = {resolved2}

            result = set(resolved1).issuperset(set(resolved2))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"父集判断: {'是' if result else '否'}",
                data={
                    'set1': list(resolved1),
                    'set2': list(resolved2),
                    'is_superset': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"父集判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_superset'}


class SetIsDisjointAction(BaseAction):
    """Check if disjoint."""
    action_type = "set5_is_disjoint"
    display_name = "判断不相交"
    description = "判断两个集合是否不相交"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is disjoint.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with disjoint result.
        """
        set1 = params.get('set1', [])
        set2 = params.get('set2', [])
        output_var = params.get('output_var', 'is_disjoint')

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            if not isinstance(resolved1, (list, tuple, set)):
                resolved1 = {resolved1}
            if not isinstance(resolved2, (list, tuple, set)):
                resolved2 = {resolved2}

            result = set(resolved1).isdisjoint(set(resolved2))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"不相交判断: {'是' if result else '否'}",
                data={
                    'set1': list(resolved1),
                    'set2': list(resolved2),
                    'is_disjoint': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"不相交判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_disjoint'}


class SetIntersectionUpdateAction(BaseAction):
    """Update set with intersection."""
    action_type = "set5_intersection_update"
    display_name = "集合交集更新"
    description = "将集合更新为与另一个集合的交集"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute intersection update.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with updated set.
        """
        set1 = params.get('set1', [])
        set2 = params.get('set2', [])
        output_var = params.get('output_var', 'intersection_result')

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            if not isinstance(resolved1, (list, tuple, set)):
                resolved1 = {resolved1}
            if not isinstance(resolved2, (list, tuple, set)):
                resolved2 = {resolved2}

            result = list(set(resolved1) & set(resolved2))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"集合交集更新: {len(result)}个元素",
                data={
                    'set1': list(resolved1),
                    'set2': list(resolved2),
                    'intersection': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合交集更新失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'intersection_result'}


class SetSymmetricDifferenceUpdateAction(BaseAction):
    """Update set with symmetric difference."""
    action_type = "set5_symmetric_difference_update"
    display_name = "集合对称差更新"
    description = "将集合更新为与另一个集合的对称差"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute symmetric difference update.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with updated set.
        """
        set1 = params.get('set1', [])
        set2 = params.get('set2', [])
        output_var = params.get('output_var', 'symmetric_difference_result')

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            if not isinstance(resolved1, (list, tuple, set)):
                resolved1 = {resolved1}
            if not isinstance(resolved2, (list, tuple, set)):
                resolved2 = {resolved2}

            result = list(set(resolved1) ^ set(resolved2))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"集合对称差更新: {len(result)}个元素",
                data={
                    'set1': list(resolved1),
                    'set2': list(resolved2),
                    'symmetric_difference': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"集合对称差更新失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'symmetric_difference_result'}
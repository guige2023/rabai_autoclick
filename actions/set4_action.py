"""Set4 action module for RabAI AutoClick.

Provides additional set operations:
- SetIsSubsetAction: Check if subset
- SetIsSupersetAction: Check if superset
- SetIsDisjointAction: Check if disjoint
- SetDifferenceAction: Set difference
- SetSymmetricDifferenceAction: Symmetric difference
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SetIsSubsetAction(BaseAction):
    """Check if subset."""
    action_type = "set4_is_subset"
    display_name = "判断子集"
    description = "检查集合是否为子集"

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
            ActionResult with subset check.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_var = params.get('output_var', 'is_subset_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            s1 = context.get(resolved1) if isinstance(resolved1, str) else resolved1
            s2 = context.get(resolved2) if isinstance(resolved2, str) else resolved2

            if not isinstance(s1, (set, frozenset, list, tuple)):
                s1 = {s1}
            if not isinstance(s2, (set, frozenset, list, tuple)):
                s2 = {s2}

            result = set(s1).issubset(set(s2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"子集判断: {'是' if result else '否'}",
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
                message=f"判断子集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_subset_result'}


class SetIsSupersetAction(BaseAction):
    """Check if superset."""
    action_type = "set4_is_superset"
    display_name = "判断超集"
    description = "检查集合是否为超集"

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
            ActionResult with superset check.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_var = params.get('output_var', 'is_superset_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            s1 = context.get(resolved1) if isinstance(resolved1, str) else resolved1
            s2 = context.get(resolved2) if isinstance(resolved2, str) else resolved2

            if not isinstance(s1, (set, frozenset, list, tuple)):
                s1 = {s1}
            if not isinstance(s2, (set, frozenset, list, tuple)):
                s2 = {s2}

            result = set(s1).issuperset(set(s2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"超集判断: {'是' if result else '否'}",
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
                message=f"判断超集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_superset_result'}


class SetIsDisjointAction(BaseAction):
    """Check if disjoint."""
    action_type = "set4_is_disjoint"
    display_name = "判断不相交"
    description = "检查两个集合是否不相交"

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
            ActionResult with disjoint check.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_var = params.get('output_var', 'is_disjoint_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            s1 = context.get(resolved1) if isinstance(resolved1, str) else resolved1
            s2 = context.get(resolved2) if isinstance(resolved2, str) else resolved2

            if not isinstance(s1, (set, frozenset, list, tuple)):
                s1 = {s1}
            if not isinstance(s2, (set, frozenset, list, tuple)):
                s2 = {s2}

            result = set(s1).isdisjoint(set(s2))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"不相交判断: {'是' if result else '否'}",
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
                message=f"判断不相交失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_disjoint_result'}


class SetDifferenceAction(BaseAction):
    """Set difference."""
    action_type = "set4_difference"
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
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with difference.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_var = params.get('output_var', 'difference_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            s1 = context.get(resolved1) if isinstance(resolved1, str) else resolved1
            s2 = context.get(resolved2) if isinstance(resolved2, str) else resolved2

            if not isinstance(s1, (set, frozenset, list, tuple)):
                s1 = {s1}
            if not isinstance(s2, (set, frozenset, list, tuple)):
                s2 = {s2}

            result = set(s1) - set(s2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"集合差集: {len(result)} 个元素",
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
                message=f"计算集合差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'difference_result'}


class SetSymmetricDifferenceAction(BaseAction):
    """Symmetric difference."""
    action_type = "set4_symmetric_difference"
    display_name = "对称差集"
    description = "计算集合对称差集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute symmetric difference.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_var.

        Returns:
            ActionResult with symmetric difference.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_var = params.get('output_var', 'symmetric_diff_result')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved1 = context.resolve_value(set1)
            resolved2 = context.resolve_value(set2)

            s1 = context.get(resolved1) if isinstance(resolved1, str) else resolved1
            s2 = context.get(resolved2) if isinstance(resolved2, str) else resolved2

            if not isinstance(s1, (set, frozenset, list, tuple)):
                s1 = {s1}
            if not isinstance(s2, (set, frozenset, list, tuple)):
                s2 = {s2}

            result = set(s1) ^ set(s2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"对称差集: {len(result)} 个元素",
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
                message=f"计算对称差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'symmetric_diff_result'}

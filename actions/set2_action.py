"""Set3 action module for RabAI AutoClick.

Provides additional set operations:
- Set3IsSubsetAction: Check if subset
- Set3IsSupersetAction: Check if superset
- Set3IsDisjointAction: Check if disjoint
- Set3SymmetricDiffAction: Symmetric difference
- Set3CartesianProductAction: Cartesian product
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Set3IsSubsetAction(BaseAction):
    """Check if subset."""
    action_type = "set3_is_subset"
    display_name = "检查子集"
    description = "检查是否为子集"

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
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}') if context.exists(f'_set_{resolved_set1}') else set(context.get(resolved_set1) if isinstance(context.get(resolved_set1), (list, tuple)) else [context.get(resolved_set1)])
            s2 = context.get(f'_set_{resolved_set2}') if context.exists(f'_set_{resolved_set2}') else set(context.get(resolved_set2) if isinstance(context.get(resolved_set2), (list, tuple)) else [context.get(resolved_set2)])

            result = s1.issubset(s2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"子集检查: {'是' if result else '否'}",
                data={
                    'set1': resolved_set1,
                    'set2': resolved_set2,
                    'is_subset': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查子集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_subset_result'}


class Set3IsSupersetAction(BaseAction):
    """Check if superset."""
    action_type = "set3_is_superset"
    display_name = "检查超集"
    description = "检查是否为超集"

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
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}') if context.exists(f'_set_{resolved_set1}') else set(context.get(resolved_set1) if isinstance(context.get(resolved_set1), (list, tuple)) else [context.get(resolved_set1)])
            s2 = context.get(f'_set_{resolved_set2}') if context.exists(f'_set_{resolved_set2}') else set(context.get(resolved_set2) if isinstance(context.get(resolved_set2), (list, tuple)) else [context.get(resolved_set2)])

            result = s1.issuperset(s2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"超集检查: {'是' if result else '否'}",
                data={
                    'set1': resolved_set1,
                    'set2': resolved_set2,
                    'is_superset': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查超集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_superset_result'}


class Set3IsDisjointAction(BaseAction):
    """Check if disjoint."""
    action_type = "set3_is_disjoint"
    display_name = "检查不相交"
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
            ActionResult with disjoint result.
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
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}') if context.exists(f'_set_{resolved_set1}') else set(context.get(resolved_set1) if isinstance(context.get(resolved_set1), (list, tuple)) else [context.get(resolved_set1)])
            s2 = context.get(f'_set_{resolved_set2}') if context.exists(f'_set_{resolved_set2}') else set(context.get(resolved_set2) if isinstance(context.get(resolved_set2), (list, tuple)) else [context.get(resolved_set2)])

            result = s1.isdisjoint(s2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"不相交检查: {'是' if result else '否'}",
                data={
                    'set1': resolved_set1,
                    'set2': resolved_set2,
                    'is_disjoint': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查不相交失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_disjoint_result'}


class Set3SymmetricDiffAction(BaseAction):
    """Symmetric difference."""
    action_type = "set3_symmetric_diff"
    display_name = "对称差集"
    description = "计算对称差集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute symmetric difference.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_name.

        Returns:
            ActionResult with symmetric difference.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_name = params.get('output_name', 'symmetric_diff')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}') if context.exists(f'_set_{resolved_set1}') else set(context.get(resolved_set1) if isinstance(context.get(resolved_set1), (list, tuple)) else [context.get(resolved_set1)])
            s2 = context.get(f'_set_{resolved_set2}') if context.exists(f'_set_{resolved_set2}') else set(context.get(resolved_set2) if isinstance(context.get(resolved_set2), (list, tuple)) else [context.get(resolved_set2)])

            result = s1.symmetric_difference(s2)
            context.set(f'_set_{output_name}', result)

            return ActionResult(
                success=True,
                message=f"对称差集: {len(result)} 项",
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
                message=f"计算对称差集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_name': 'symmetric_diff'}


class Set3CartesianProductAction(BaseAction):
    """Cartesian product."""
    action_type = "set3_cartesian_product"
    display_name = "笛卡尔积"
    description = "计算集合的笛卡尔积"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cartesian product.

        Args:
            context: Execution context.
            params: Dict with set1, set2, output_name.

        Returns:
            ActionResult with cartesian product.
        """
        set1 = params.get('set1', '')
        set2 = params.get('set2', '')
        output_name = params.get('output_name', 'cartesian_product')

        valid, msg = self.validate_type(set1, str, 'set1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(set2, str, 'set2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_set1 = context.resolve_value(set1)
            resolved_set2 = context.resolve_value(set2)

            s1 = context.get(f'_set_{resolved_set1}') if context.exists(f'_set_{resolved_set1}') else set(context.get(resolved_set1) if isinstance(context.get(resolved_set1), (list, tuple)) else [context.get(resolved_set1)])
            s2 = context.get(f'_set_{resolved_set2}') if context.exists(f'_set_{resolved_set2}') else set(context.get(resolved_set2) if isinstance(context.get(resolved_set2), (list, tuple)) else [context.get(resolved_set2)])

            result = [(a, b) for a in s1 for b in s2]
            context.set(f'_set_{output_name}', result)

            return ActionResult(
                success=True,
                message=f"笛卡尔积: {len(result)} 项",
                data={
                    'set1_size': len(s1),
                    'set2_size': len(s2),
                    'output_name': output_name,
                    'count': len(result)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算笛卡尔积失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['set1', 'set2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_name': 'cartesian_product'}

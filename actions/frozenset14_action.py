"""FrozenSet14 action module for RabAI AutoClick.

Provides additional frozenset operations:
- FrozenSetCreateAction: Create frozenset
- FrozenSetOperationsAction: Frozenset operations
- FrozenSetIsSubsetAction: Check if subset
- FrozenSetIsSupersetAction: Check if superset
- FrozenSetIsDisjointAction: Check if disjoint
- FrozenSetConvertAction: Convert to/from frozenset
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FrozenSetCreateAction(BaseAction):
    """Create frozenset."""
    action_type = "frozenset14_create"
    display_name = "创建不可变集合"
    description = "创建不可变集合"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, values, output_var.

        Returns:
            ActionResult with create result.
        """
        name = params.get('name', 'frozenset')
        values = params.get('values', [])
        output_var = params.get('output_var', 'create_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'frozenset'
            resolved_values = context.resolve_value(values) if values else []

            if not isinstance(resolved_values, (list, tuple, set, frozenset)):
                resolved_values = [resolved_values]

            result = frozenset(resolved_values)

            if not hasattr(context, '_frozensets'):
                context._frozensets = {}
            context._frozensets[resolved_name] = result

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建不可变集合: {resolved_name} = {set(result)}",
                data={
                    'name': resolved_name,
                    'frozenset': set(result),
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建不可变集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'create_result'}


class FrozenSetOperationsAction(BaseAction):
    """Frozenset operations."""
    action_type = "frozenset14_operations"
    display_name = "不可变集合运算"
    description = "不可变集合运算"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute operations.

        Args:
            context: Execution context.
            params: Dict with name, operation, other_name, output_var.

        Returns:
            ActionResult with operation result.
        """
        name = params.get('name', 'frozenset1')
        operation = params.get('operation', 'union')
        other_name = params.get('other_name', 'frozenset2')
        output_var = params.get('output_var', 'operation_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'frozenset1'
            resolved_op = context.resolve_value(operation) if operation else 'union'
            resolved_other = context.resolve_value(other_name) if other_name else 'frozenset2'

            if not hasattr(context, '_frozensets') or resolved_name not in context._frozensets:
                return ActionResult(
                    success=False,
                    message=f"不可变集合不存在: {resolved_name}"
                )

            fs1 = context._frozensets[resolved_name]
            fs2 = context._frozensets.get(resolved_other, frozenset())

            if resolved_op == 'union':
                result = fs1.union(fs2)
            elif resolved_op == 'intersection':
                result = fs1.intersection(fs2)
            elif resolved_op == 'difference':
                result = fs1.difference(fs2)
            elif resolved_op == 'symmetric_difference':
                result = fs1.symmetric_difference(fs2)
            else:
                result = fs1.union(fs2)

            context.set(output_var, set(result))

            return ActionResult(
                success=True,
                message=f"不可变集合运算: {resolved_name} {resolved_op} {resolved_other} = {set(result)}",
                data={
                    'name': resolved_name,
                    'operation': resolved_op,
                    'other': resolved_other,
                    'result': set(result),
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"不可变集合运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'operation', 'other_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'operation_result'}


class FrozenSetIsSubsetAction(BaseAction):
    """Check if subset."""
    action_type = "frozenset14_issubset"
    display_name = "是否子集"
    description = "检查是否子集"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute issubset.

        Args:
            context: Execution context.
            params: Dict with name, other_name, output_var.

        Returns:
            ActionResult with issubset result.
        """
        name = params.get('name', 'frozenset1')
        other_name = params.get('other_name', 'frozenset2')
        output_var = params.get('output_var', 'issubset_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'frozenset1'
            resolved_other = context.resolve_value(other_name) if other_name else 'frozenset2'

            if not hasattr(context, '_frozensets'):
                context._frozensets = {}

            fs1 = context._frozensets.get(resolved_name, frozenset())
            fs2 = context._frozensets.get(resolved_other, frozenset())

            is_subset = fs1.issubset(fs2)

            context.set(output_var, is_subset)

            return ActionResult(
                success=True,
                message=f"是否子集: {resolved_name} <= {resolved_other} = {is_subset}",
                data={
                    'name': resolved_name,
                    'other': resolved_other,
                    'is_subset': is_subset,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"是否子集检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'other_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'issubset_result'}


class FrozenSetIsSupersetAction(BaseAction):
    """Check if superset."""
    action_type = "frozenset14_issuperset"
    display_name = "是否超集"
    description = "检查是否超集"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute issuperset.

        Args:
            context: Execution context.
            params: Dict with name, other_name, output_var.

        Returns:
            ActionResult with issuperset result.
        """
        name = params.get('name', 'frozenset1')
        other_name = params.get('other_name', 'frozenset2')
        output_var = params.get('output_var', 'issuperset_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'frozenset1'
            resolved_other = context.resolve_value(other_name) if other_name else 'frozenset2'

            if not hasattr(context, '_frozensets'):
                context._frozensets = {}

            fs1 = context._frozensets.get(resolved_name, frozenset())
            fs2 = context._frozensets.get(resolved_other, frozenset())

            is_superset = fs1.issuperset(fs2)

            context.set(output_var, is_superset)

            return ActionResult(
                success=True,
                message=f"是否超集: {resolved_name} >= {resolved_other} = {is_superset}",
                data={
                    'name': resolved_name,
                    'other': resolved_other,
                    'is_superset': is_superset,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"是否超集检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'other_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'issuperset_result'}


class FrozenSetIsDisjointAction(BaseAction):
    """Check if disjoint."""
    action_type = "frozenset14_isdisjoint"
    display_name = "是否不相交"
    description = "检查是否不相交"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute isdisjoint.

        Args:
            context: Execution context.
            params: Dict with name, other_name, output_var.

        Returns:
            ActionResult with isdisjoint result.
        """
        name = params.get('name', 'frozenset1')
        other_name = params.get('other_name', 'frozenset2')
        output_var = params.get('output_var', 'isdisjoint_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'frozenset1'
            resolved_other = context.resolve_value(other_name) if other_name else 'frozenset2'

            if not hasattr(context, '_frozensets'):
                context._frozensets = {}

            fs1 = context._frozensets.get(resolved_name, frozenset())
            fs2 = context._frozensets.get(resolved_other, frozenset())

            is_disjoint = fs1.isdisjoint(fs2)

            context.set(output_var, is_disjoint)

            return ActionResult(
                success=True,
                message=f"是否不相交: {resolved_name} & {resolved_other} =空? {is_disjoint}",
                data={
                    'name': resolved_name,
                    'other': resolved_other,
                    'is_disjoint': is_disjoint,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"是否不相交检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'other_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isdisjoint_result'}


class FrozenSetConvertAction(BaseAction):
    """Convert to/from frozenset."""
    action_type = "frozenset14_convert"
    display_name = "不可变集合转换"
    description = "转换为不可变集合或从不可变集合转换"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with name, direction, output_var.

        Returns:
            ActionResult with convert result.
        """
        name = params.get('name', 'frozenset')
        direction = params.get('direction', 'to_frozenset')
        output_var = params.get('output_var', 'convert_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'frozenset'
            resolved_dir = context.resolve_value(direction) if direction else 'to_frozenset'

            if not hasattr(context, '_frozensets'):
                context._frozensets = {}

            if resolved_dir == 'to_frozenset':
                if resolved_name in context._frozensets:
                    result = context._frozensets[resolved_name]
                else:
                    return ActionResult(
                        success=False,
                        message=f"不可变集合不存在: {resolved_name}"
                    )
                context.set(output_var, set(result))
            else:
                if resolved_name in context._frozensets:
                    fs = context._frozensets[resolved_name]
                else:
                    fs = frozenset()
                context.set(output_var, set(fs))

            return ActionResult(
                success=True,
                message=f"不可变集合转换: {resolved_name} -> {direction}",
                data={
                    'name': resolved_name,
                    'direction': resolved_dir,
                    'result': set(fs),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"不可变集合转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'direction']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'convert_result'}
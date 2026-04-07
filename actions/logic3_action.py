"""Logic3 action module for RabAI AutoClick.

Provides additional logic operations:
- LogicAllAction: Check if all values are true
- LogicAnyAction: Check if any value is true
- LogicNoneAction: Check if all values are false
- LogicTernaryAction: Ternary/conditional expression
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogicAllAction(BaseAction):
    """Check if all values are true."""
    action_type = "logic3_all"
    display_name = "逻辑与全部"
    description = "检查所有值是否都为真"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logic all.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with check result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'all_result')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, list):
                resolved = [resolved]

            result = all(bool(v) for v in resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑与全部: {'是' if result else '否'}",
                data={
                    'values': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑与全部失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'all_result'}


class LogicAnyAction(BaseAction):
    """Check if any value is true."""
    action_type = "logic3_any"
    display_name = "逻辑或任意"
    description = "检查任意值是否为真"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logic any.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with check result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'any_result')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, list):
                resolved = [resolved]

            result = any(bool(v) for v in resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑或任意: {'是' if result else '否'}",
                data={
                    'values': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑或任意失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'any_result'}


class LogicNoneAction(BaseAction):
    """Check if all values are false."""
    action_type = "logic3_none"
    display_name = "逻辑非全部"
    description = "检查所有值是否都为假"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logic none.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with check result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'none_result')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, list):
                resolved = [resolved]

            result = not any(bool(v) for v in resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑非全部: {'是' if result else '否'}",
                data={
                    'values': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑非全部失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'none_result'}


class LogicTernaryAction(BaseAction):
    """Ternary/conditional expression."""
    action_type = "logic3_ternary"
    display_name = "三元表达式"
    description = "根据条件返回不同值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ternary.

        Args:
            context: Execution context.
            params: Dict with condition, true_value, false_value, output_var.

        Returns:
            ActionResult with selected value.
        """
        condition = params.get('condition', False)
        true_value = params.get('true_value', None)
        false_value = params.get('false_value', None)
        output_var = params.get('output_var', 'ternary_result')

        try:
            resolved_condition = bool(context.resolve_value(condition))
            resolved_true = context.resolve_value(true_value) if true_value is not None else None
            resolved_false = context.resolve_value(false_value) if false_value is not None else None

            result = resolved_true if resolved_condition else resolved_false
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"三元表达式: {'true_value' if resolved_condition else 'false_value'}",
                data={
                    'condition': resolved_condition,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"三元表达式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'true_value': None, 'false_value': None, 'output_var': 'ternary_result'}
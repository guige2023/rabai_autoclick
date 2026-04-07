"""Logic action module for RabAI AutoClick.

Provides logical operations:
- LogicAndAction: Logical AND
- LogicOrAction: Logical OR
- LogicNotAction: Logical NOT
- LogicXorAction: Logical XOR
- LogicIfAction: If-then-else logic
- LogicTernaryAction: Ternary conditional
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogicAndAction(BaseAction):
    """Logical AND."""
    action_type = "logic_and"
    display_name = "逻辑与"
    description = "逻辑与运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical AND.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with AND result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'logic_result')

        valid, msg = self.validate_type(values, (list, tuple), 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_values = [bool(context.resolve_value(v)) for v in values]
            result = all(resolved_values)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑与: {'True' if result else 'False'}",
                data={
                    'result': result,
                    'values': resolved_values,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑与失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'logic_result'}


class LogicOrAction(BaseAction):
    """Logical OR."""
    action_type = "logic_or"
    display_name = "逻辑或"
    description = "逻辑或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical OR.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with OR result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'logic_result')

        valid, msg = self.validate_type(values, (list, tuple), 'values')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_values = [bool(context.resolve_value(v)) for v in values]
            result = any(resolved_values)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑或: {'True' if result else 'False'}",
                data={
                    'result': result,
                    'values': resolved_values,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑或失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'logic_result'}


class LogicNotAction(BaseAction):
    """Logical NOT."""
    action_type = "logic_not"
    display_name = "逻辑非"
    description = "逻辑非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical NOT.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with NOT result.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'logic_result')

        try:
            resolved = context.resolve_value(value)
            result = not bool(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑非: {result}",
                data={
                    'result': result,
                    'input': bool(resolved),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑非失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'logic_result'}


class LogicXorAction(BaseAction):
    """Logical XOR."""
    action_type = "logic_xor"
    display_name = "逻辑异或"
    description = "逻辑异或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logical XOR.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with XOR result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'logic_result')

        try:
            resolved_v1 = bool(context.resolve_value(value1))
            resolved_v2 = bool(context.resolve_value(value2))
            result = resolved_v1 != resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑异或: {result}",
                data={
                    'result': result,
                    'value1': resolved_v1,
                    'value2': resolved_v2,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑异或失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'logic_result'}


class LogicIfAction(BaseAction):
    """If-then-else logic."""
    action_type = "logic_if"
    display_name = "逻辑条件"
    description = "条件执行逻辑"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute if-then-else.

        Args:
            context: Execution context.
            params: Dict with condition, then_value, else_value, output_var.

        Returns:
            ActionResult with selected value.
        """
        condition = params.get('condition', False)
        then_value = params.get('then_value', None)
        else_value = params.get('else_value', None)
        output_var = params.get('output_var', 'logic_result')

        try:
            resolved_condition = context.resolve_value(condition)

            if bool(resolved_condition):
                result = context.resolve_value(then_value)
                selected = 'then'
            else:
                result = context.resolve_value(else_value)
                selected = 'else'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"条件选择: {selected}",
                data={
                    'result': result,
                    'condition': bool(resolved_condition),
                    'selected': selected,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"条件执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition', 'then_value', 'else_value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'logic_result'}


class LogicTernaryAction(BaseAction):
    """Ternary conditional."""
    action_type = "logic_ternary"
    display_name = "三元运算"
    description = "三元条件运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ternary operation.

        Args:
            context: Execution context.
            params: Dict with condition, true_val, false_val, output_var.

        Returns:
            ActionResult with selected value.
        """
        condition = params.get('condition', False)
        true_val = params.get('true_val', None)
        false_val = params.get('false_val', None)
        output_var = params.get('output_var', 'logic_result')

        try:
            resolved_condition = context.resolve_value(condition)

            if bool(resolved_condition):
                result = context.resolve_value(true_val)
            else:
                result = context.resolve_value(false_val)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"三元运算: {'true_val' if bool(resolved_condition) else 'false_val'}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"三元运算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['condition', 'true_val', 'false_val']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'logic_result'}
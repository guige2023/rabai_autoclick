"""Logic5 action module for RabAI AutoClick.

Provides additional logic operations:
- LogicAndAction: Logical AND
- LogicOrAction: Logical OR
- LogicNotAction: Logical NOT
- LogicXorAction: Logical XOR
- LogicNandAction: Logical NAND
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LogicAndAction(BaseAction):
    """Logical AND."""
    action_type = "logic5_and"
    display_name = "逻辑与"
    description = "执行逻辑与运算"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute AND.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with AND result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'and_result')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"逻辑与失败: 输入不是列表"
                )

            result = all(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑与: {'真' if result else '假'}",
                data={
                    'values': resolved,
                    'result': result,
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
        return {'output_var': 'and_result'}


class LogicOrAction(BaseAction):
    """Logical OR."""
    action_type = "logic5_or"
    display_name = "逻辑或"
    description = "执行逻辑或运算"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OR.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with OR result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'or_result')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"逻辑或失败: 输入不是列表"
                )

            result = any(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑或: {'真' if result else '假'}",
                data={
                    'values': resolved,
                    'result': result,
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
        return {'output_var': 'or_result'}


class LogicNotAction(BaseAction):
    """Logical NOT."""
    action_type = "logic5_not"
    display_name = "逻辑非"
    description = "执行逻辑非运算"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute NOT.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with NOT result.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'not_result')

        try:
            resolved = context.resolve_value(value)
            result = not bool(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑非: {'真' if result else '假'}",
                data={
                    'value': resolved,
                    'result': result,
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
        return {'output_var': 'not_result'}


class LogicXorAction(BaseAction):
    """Logical XOR."""
    action_type = "logic5_xor"
    display_name = "逻辑异或"
    description = "执行逻辑异或运算"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute XOR.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with XOR result.
        """
        value1 = params.get('value1', False)
        value2 = params.get('value2', False)
        output_var = params.get('output_var', 'xor_result')

        try:
            resolved1 = bool(context.resolve_value(value1))
            resolved2 = bool(context.resolve_value(value2))
            result = resolved1 != resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑异或: {'真' if result else '假'}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
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
        return {'output_var': 'xor_result'}


class LogicNandAction(BaseAction):
    """Logical NAND."""
    action_type = "logic5_nand"
    display_name = "逻辑与非"
    description = "执行逻辑与非运算"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute NAND.

        Args:
            context: Execution context.
            params: Dict with values, output_var.

        Returns:
            ActionResult with NAND result.
        """
        values = params.get('values', [])
        output_var = params.get('output_var', 'nand_result')

        try:
            resolved = context.resolve_value(values)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"逻辑与非失败: 输入不是列表"
                )

            result = not all(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"逻辑与非: {'真' if result else '假'}",
                data={
                    'values': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"逻辑与非失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'nand_result'}
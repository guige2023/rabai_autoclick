"""Bitwise3 action module for RabAI AutoClick.

Provides additional bitwise operations:
- BitwiseAndAction: Bitwise AND
- BitwiseOrAction: Bitwise OR
- BitwiseXorAction: Bitwise XOR
- BitwiseNotAction: Bitwise NOT
- BitwiseLshiftAction: Left shift
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BitwiseAndAction(BaseAction):
    """Bitwise AND."""
    action_type = "bitwise3_and"
    display_name = "按位与"
    description = "执行按位与运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise AND.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with AND result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'bitwise_result')

        try:
            resolved1 = int(context.resolve_value(value1))
            resolved2 = int(context.resolve_value(value2))

            result = resolved1 & resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位与: {resolved1} & {resolved2} = {result}",
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
                message=f"按位与失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bitwise_result'}


class BitwiseOrAction(BaseAction):
    """Bitwise OR."""
    action_type = "bitwise3_or"
    display_name = "按位或"
    description = "执行按位或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise OR.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with OR result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'bitwise_result')

        try:
            resolved1 = int(context.resolve_value(value1))
            resolved2 = int(context.resolve_value(value2))

            result = resolved1 | resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位或: {resolved1} | {resolved2} = {result}",
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
                message=f"按位或失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bitwise_result'}


class BitwiseXorAction(BaseAction):
    """Bitwise XOR."""
    action_type = "bitwise3_xor"
    display_name = "按位异或"
    description = "执行按位异或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise XOR.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with XOR result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'bitwise_result')

        try:
            resolved1 = int(context.resolve_value(value1))
            resolved2 = int(context.resolve_value(value2))

            result = resolved1 ^ resolved2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位异或: {resolved1} ^ {resolved2} = {result}",
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
                message=f"按位异或失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value1', 'value2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bitwise_result'}


class BitwiseNotAction(BaseAction):
    """Bitwise NOT."""
    action_type = "bitwise3_not"
    display_name = "按位取反"
    description = "执行按位取反运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise NOT.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with NOT result.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'bitwise_result')

        try:
            resolved = int(context.resolve_value(value))

            result = ~resolved
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位取反: ~{resolved} = {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按位取反失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bitwise_result'}


class BitwiseLshiftAction(BaseAction):
    """Left shift."""
    action_type = "bitwise3_lshift"
    display_name = "左移"
    description = "执行左移位运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute left shift.

        Args:
            context: Execution context.
            params: Dict with value, bits, output_var.

        Returns:
            ActionResult with left shift result.
        """
        value = params.get('value', 0)
        bits = params.get('bits', 0)
        output_var = params.get('output_var', 'bitwise_result')

        try:
            resolved_value = int(context.resolve_value(value))
            resolved_bits = int(context.resolve_value(bits))

            result = resolved_value << resolved_bits
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"左移: {resolved_value} << {resolved_bits} = {result}",
                data={
                    'value': resolved_value,
                    'bits': resolved_bits,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"左移失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'bits']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bitwise_result'}

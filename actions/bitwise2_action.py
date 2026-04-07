"""Bitwise2 action module for RabAI AutoClick.

Provides additional bitwise operations:
- BitwiseAndAction: Bitwise AND
- BitwiseOrAction: Bitwise OR
- BitwiseXorAction: Bitwise XOR
- BitwiseNotAction: Bitwise NOT
- BitwiseLeftShiftAction: Left shift
- BitwiseRightShiftAction: Right shift
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BitwiseAndAction(BaseAction):
    """Bitwise AND."""
    action_type = "bitwise_and"
    display_name = "按位与"
    description = "按位与运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise and.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with bitwise AND result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'bitwise_and_result')

        try:
            resolved_v1 = int(context.resolve_value(value1))
            resolved_v2 = int(context.resolve_value(value2))

            result = resolved_v1 & resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位与: {resolved_v1} & {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
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
        return {'output_var': 'bitwise_and_result'}


class BitwiseOrAction(BaseAction):
    """Bitwise OR."""
    action_type = "bitwise_or"
    display_name = "按位或"
    description = "按位或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise or.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with bitwise OR result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'bitwise_or_result')

        try:
            resolved_v1 = int(context.resolve_value(value1))
            resolved_v2 = int(context.resolve_value(value2))

            result = resolved_v1 | resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位或: {resolved_v1} | {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
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
        return {'output_var': 'bitwise_or_result'}


class BitwiseXorAction(BaseAction):
    """Bitwise XOR."""
    action_type = "bitwise_xor"
    display_name = "按位异或"
    description = "按位异或运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise xor.

        Args:
            context: Execution context.
            params: Dict with value1, value2, output_var.

        Returns:
            ActionResult with bitwise XOR result.
        """
        value1 = params.get('value1', 0)
        value2 = params.get('value2', 0)
        output_var = params.get('output_var', 'bitwise_xor_result')

        try:
            resolved_v1 = int(context.resolve_value(value1))
            resolved_v2 = int(context.resolve_value(value2))

            result = resolved_v1 ^ resolved_v2
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位异或: {resolved_v1} ^ {resolved_v2} = {result}",
                data={
                    'value1': resolved_v1,
                    'value2': resolved_v2,
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
        return {'output_var': 'bitwise_xor_result'}


class BitwiseNotAction(BaseAction):
    """Bitwise NOT."""
    action_type = "bitwise_not"
    display_name = "按位非"
    description = "按位非运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise not.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with bitwise NOT result.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'bitwise_not_result')

        try:
            resolved_value = int(context.resolve_value(value))

            result = ~resolved_value
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位非: ~{resolved_value} = {result}",
                data={
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按位非失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bitwise_not_result'}


class BitwiseLeftShiftAction(BaseAction):
    """Left shift."""
    action_type = "bitwise_left_shift"
    display_name = "左移"
    description = "按位左移运算"

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
        bits = params.get('bits', 1)
        output_var = params.get('output_var', 'left_shift_result')

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
        return {'output_var': 'left_shift_result'}


class BitwiseRightShiftAction(BaseAction):
    """Right shift."""
    action_type = "bitwise_right_shift"
    display_name = "右移"
    description = "按位右移运算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute right shift.

        Args:
            context: Execution context.
            params: Dict with value, bits, output_var.

        Returns:
            ActionResult with right shift result.
        """
        value = params.get('value', 0)
        bits = params.get('bits', 1)
        output_var = params.get('output_var', 'right_shift_result')

        try:
            resolved_value = int(context.resolve_value(value))
            resolved_bits = int(context.resolve_value(bits))

            result = resolved_value >> resolved_bits
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"右移: {resolved_value} >> {resolved_bits} = {result}",
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
                message=f"右移失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'bits']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'right_shift_result'}

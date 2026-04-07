"""Bitwise13 action module for RabAI AutoClick.

Provides additional bitwise operations:
- BitwiseAndAction: Bitwise AND
- BitwiseOrAction: Bitwise OR
- BitwiseXorAction: Bitwise XOR
- BitwiseNotAction: Bitwise NOT
- BitwiseLeftShiftAction: Left shift
- BitwiseRightShiftAction: Right shift
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BitwiseAndAction(BaseAction):
    """Bitwise AND."""
    action_type = "bitwise13_and"
    display_name = "按位与"
    description = "按位与运算"
    version = "13.0"

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
        output_var = params.get('output_var', 'and_result')

        try:
            resolved1 = int(context.resolve_value(value1)) if value1 else 0
            resolved2 = int(context.resolve_value(value2)) if value2 else 0

            result = resolved1 & resolved2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位与: {result}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
                    'binary': bin(result),
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
        return {'output_var': 'and_result'}


class BitwiseOrAction(BaseAction):
    """Bitwise OR."""
    action_type = "bitwise13_or"
    display_name = "按位或"
    description = "按位或运算"
    version = "13.0"

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
        output_var = params.get('output_var', 'or_result')

        try:
            resolved1 = int(context.resolve_value(value1)) if value1 else 0
            resolved2 = int(context.resolve_value(value2)) if value2 else 0

            result = resolved1 | resolved2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位或: {result}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
                    'binary': bin(result),
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
        return {'output_var': 'or_result'}


class BitwiseXorAction(BaseAction):
    """Bitwise XOR."""
    action_type = "bitwise13_xor"
    display_name = "按位异或"
    description = "按位异或运算"
    version = "13.0"

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
        output_var = params.get('output_var', 'xor_result')

        try:
            resolved1 = int(context.resolve_value(value1)) if value1 else 0
            resolved2 = int(context.resolve_value(value2)) if value2 else 0

            result = resolved1 ^ resolved2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位异或: {result}",
                data={
                    'value1': resolved1,
                    'value2': resolved2,
                    'result': result,
                    'binary': bin(result),
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
        return {'output_var': 'xor_result'}


class BitwiseNotAction(BaseAction):
    """Bitwise NOT."""
    action_type = "bitwise13_not"
    display_name = "按位非"
    description = "按位非运算"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bitwise NOT.

        Args:
            context: Execution context.
            params: Dict with value, bits, output_var.

        Returns:
            ActionResult with NOT result.
        """
        value = params.get('value', 0)
        bits = params.get('bits', 8)
        output_var = params.get('output_var', 'not_result')

        try:
            resolved_value = int(context.resolve_value(value)) if value else 0
            resolved_bits = int(context.resolve_value(bits)) if bits else 8

            # Create mask with resolved_bits bits
            mask = (1 << resolved_bits) - 1
            result = ~resolved_value & mask

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按位非: {result}",
                data={
                    'value': resolved_value,
                    'bits': resolved_bits,
                    'result': result,
                    'binary': bin(result),
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
        return {'bits': 8, 'output_var': 'not_result'}


class BitwiseLeftShiftAction(BaseAction):
    """Left shift."""
    action_type = "bitwise13_left_shift"
    display_name = "左移"
    description = "左移运算"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute left shift.

        Args:
            context: Execution context.
            params: Dict with value, shift, output_var.

        Returns:
            ActionResult with left shift result.
        """
        value = params.get('value', 0)
        shift = params.get('shift', 1)
        output_var = params.get('output_var', 'left_shift_result')

        try:
            resolved_value = int(context.resolve_value(value)) if value else 0
            resolved_shift = int(context.resolve_value(shift)) if shift else 1

            result = resolved_value << resolved_shift

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"左移: {result}",
                data={
                    'value': resolved_value,
                    'shift': resolved_shift,
                    'result': result,
                    'binary': bin(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"左移失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'shift']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'left_shift_result'}


class BitwiseRightShiftAction(BaseAction):
    """Right shift."""
    action_type = "bitwise13_right_shift"
    display_name = "右移"
    description = "右移运算"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute right shift.

        Args:
            context: Execution context.
            params: Dict with value, shift, output_var.

        Returns:
            ActionResult with right shift result.
        """
        value = params.get('value', 0)
        shift = params.get('shift', 1)
        output_var = params.get('output_var', 'right_shift_result')

        try:
            resolved_value = int(context.resolve_value(value)) if value else 0
            resolved_shift = int(context.resolve_value(shift)) if shift else 1

            result = resolved_value >> resolved_shift

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"右移: {result}",
                data={
                    'value': resolved_value,
                    'shift': resolved_shift,
                    'result': result,
                    'binary': bin(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"右移失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'shift']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'right_shift_result'}
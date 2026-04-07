"""Bitwise4 action module for RabAI AutoClick.

Provides additional bitwise operations:
- BitwiseRshiftAction: Right shift
- BitwiseToBinaryAction: Convert to binary
- BitwiseFromBinaryAction: Convert from binary
- BitwiseCountOnesAction: Count set bits
- BitwiseParityAction: Check parity
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BitwiseRshiftAction(BaseAction):
    """Right shift."""
    action_type = "bitwise4_rshift"
    display_name = "右移"
    description = "执行右移位运算"

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
        bits = params.get('bits', 0)
        output_var = params.get('output_var', 'bitwise_result')

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
        return {'output_var': 'bitwise_result'}


class BitwiseToBinaryAction(BaseAction):
    """Convert to binary."""
    action_type = "bitwise4_to_binary"
    display_name = "转二进制"
    description = "将数字转换为二进制字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to binary.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with binary string.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'binary_result')

        try:
            resolved = int(context.resolve_value(value))

            result = bin(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"二进制: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转二进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'binary_result'}


class BitwiseFromBinaryAction(BaseAction):
    """Convert from binary."""
    action_type = "bitwise4_from_binary"
    display_name = "从二进制转"
    description = "将二进制字符串转换为数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute from binary.

        Args:
            context: Execution context.
            params: Dict with binary_str, output_var.

        Returns:
            ActionResult with number.
        """
        binary_str = params.get('binary_str', '0')
        output_var = params.get('output_var', 'number_result')

        valid, msg = self.validate_type(binary_str, str, 'binary_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(binary_str)
            result = int(resolved, 2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"从二进制转: {resolved} = {result}",
                data={
                    'binary': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从二进制转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['binary_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'number_result'}


class BitwiseCountOnesAction(BaseAction):
    """Count set bits."""
    action_type = "bitwise4_count_ones"
    display_name = "计算1的个数"
    description = "计算数字二进制中1的个数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count ones.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with count of ones.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'ones_count')

        try:
            resolved = int(context.resolve_value(value))

            result = bin(resolved).count('1')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"1的个数: {result}",
                data={
                    'value': resolved,
                    'binary': bin(resolved),
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算1的个数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ones_count'}


class BitwiseParityAction(BaseAction):
    """Check parity."""
    action_type = "bitwise4_parity"
    display_name = "检查奇偶性"
    description = "检查二进制中1的个数是奇数还是偶数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parity check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with parity result (True=odd, False=even).
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'parity_result')

        try:
            resolved = int(context.resolve_value(value))

            ones_count = bin(resolved).count('1')
            result = ones_count % 2 == 1
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"奇偶性: {'奇数' if result else '偶数'} ({ones_count}个1)",
                data={
                    'value': resolved,
                    'ones_count': ones_count,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查奇偶性失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parity_result'}

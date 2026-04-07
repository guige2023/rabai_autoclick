"""Hex action module for RabAI AutoClick.

Provides hexadecimal operations:
- HexEncodeAction: Encode string to hex
- HexDecodeAction: Decode hex to string
- HexToIntAction: Convert hex to integer
- IntToHexAction: Convert integer to hex
- HexValidateAction: Validate hex string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HexEncodeAction(BaseAction):
    """Encode string to hex."""
    action_type = "hex_encode"
    display_name = "Hex编码"
    description = "将字符串编码为十六进制"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hex encode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hex encoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'hex_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.encode('utf-8').hex()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Hex编码完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hex编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_result'}


class HexDecodeAction(BaseAction):
    """Decode hex to string."""
    action_type = "hex_decode"
    display_name = "Hex解码"
    description = "将十六进制解码为字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hex decode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with decoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'decode_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = bytes.fromhex(resolved).decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Hex解码完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hex解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decode_result'}


class HexToIntAction(BaseAction):
    """Convert hex to integer."""
    action_type = "hex_to_int"
    display_name = "Hex转整数"
    description = "将十六进制转换为整数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hex to int.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with integer.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'int_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = int(resolved, 16)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Hex转整数: {result}",
                data={
                    'hex': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hex转整数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'int_result'}


class IntToHexAction(BaseAction):
    """Convert integer to hex."""
    action_type = "int_to_hex"
    display_name = "整数转Hex"
    description = "将整数转换为十六进制"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute int to hex.

        Args:
            context: Execution context.
            params: Dict with value, pad, output_var.

        Returns:
            ActionResult with hex string.
        """
        value = params.get('value', 0)
        pad = params.get('pad', None)
        output_var = params.get('output_var', 'hex_result')

        try:
            resolved = int(context.resolve_value(value))
            resolved_pad = int(context.resolve_value(pad)) if pad else None

            if resolved_pad:
                result = format(resolved, f'0{resolved_pad}x')
            else:
                result = format(resolved, 'x')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"整数转Hex: {result}",
                data={
                    'integer': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"整数转Hex失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 0, 'pad': None, 'output_var': 'hex_result'}


class HexValidateAction(BaseAction):
    """Validate hex string."""
    action_type = "hex_validate"
    display_name = "Hex验证"
    description = "验证是否为有效的十六进制字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hex validate.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'validate_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            int(resolved, 16)
            result = True
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Hex验证: {'有效' if result else '无效'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            context.set(output_var, False)

            return ActionResult(
                success=True,
                message=f"Hex验证: 无效",
                data={
                    'value': resolved,
                    'result': False,
                    'output_var': output_var
                }
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validate_result'}
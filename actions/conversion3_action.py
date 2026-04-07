"""Conversion3 action module for RabAI AutoClick.

Provides additional conversion operations:
- ConversionToOrdAction: Convert character to ordinal
- ConversionToChrAction: Convert ordinal to character
- ConversionToHexAction: Convert to hexadecimal
- ConversionToOctAction: Convert to octal
- ConversionToBytesAction: Convert to bytes
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConversionToOrdAction(BaseAction):
    """Convert character to ordinal."""
    action_type = "conversion3_to_ord"
    display_name = "字符转序数"
    description = "将字符转换为其Unicode序数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to ord.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with ordinal number.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'ord_result')

        try:
            resolved = str(context.resolve_value(value))

            if len(resolved) != 1:
                return ActionResult(
                    success=False,
                    message="值必须是单个字符"
                )

            result = ord(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符序数: ord('{resolved}') = {result}",
                data={
                    'char': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符转序数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ord_result'}


class ConversionToChrAction(BaseAction):
    """Convert ordinal to character."""
    action_type = "conversion3_to_chr"
    display_name = "序数转字符"
    description = "将Unicode序数转换为字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to chr.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with character.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'chr_result')

        try:
            resolved = int(context.resolve_value(value))
            result = chr(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"序数字符: chr({resolved}) = '{result}'",
                data={
                    'ordinal': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"序数转字符失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'chr_result'}


class ConversionToHexAction(BaseAction):
    """Convert to hexadecimal."""
    action_type = "conversion3_to_hex"
    display_name = "转十六进制"
    description = "将整数转换为十六进制字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to hex.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with hex string.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'hex_result')

        try:
            resolved = int(context.resolve_value(value))
            result = hex(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"十六进制: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转十六进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_result'}


class ConversionToOctAction(BaseAction):
    """Convert to octal."""
    action_type = "conversion3_to_oct"
    display_name = "转八进制"
    description = "将整数转换为八进制字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to oct.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with octal string.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'oct_result')

        try:
            resolved = int(context.resolve_value(value))
            result = oct(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"八进制: {result}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转八进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'oct_result'}


class ConversionToBytesAction(BaseAction):
    """Convert to bytes."""
    action_type = "conversion3_to_bytes"
    display_name = "转字节串"
    description = "将字符串转换为字节串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to bytes.

        Args:
            context: Execution context.
            params: Dict with value, encoding, output_var.

        Returns:
            ActionResult with bytes.
        """
        value = params.get('value', '')
        encoding = params.get('encoding', 'utf-8')
        output_var = params.get('output_var', 'bytes_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_encoding = str(context.resolve_value(encoding)) if encoding else 'utf-8'

            result = resolved.encode(resolved_encoding)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字节串: {len(result)} bytes",
                data={
                    'original': resolved,
                    'encoding': resolved_encoding,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转字节串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'encoding': 'utf-8', 'output_var': 'bytes_result'}

"""Encode4 action module for RabAI AutoClick.

Provides additional encoding operations:
- EncodeBase32EncodeAction: Base32 encode
- EncodeBase32DecodeAction: Base32 decode
- EncodeBase16EncodeAction: Base16 encode
- EncodeBase16DecodeAction: Base16 decode
- EncodeQuotedPrintableAction: Quoted printable encode
"""

import base64
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EncodeBase32EncodeAction(BaseAction):
    """Base32 encode."""
    action_type = "encode4_base32_encode"
    display_name = "Base32编码"
    description = "将字符串进行Base32编码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base32 encode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with Base32 encoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'base32_result')

        try:
            resolved = str(context.resolve_value(value))
            encoded = base64.b32encode(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"Base32编码完成: {len(encoded)} 字符",
                data={
                    'original': resolved,
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base32编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base32_result'}


class EncodeBase32DecodeAction(BaseAction):
    """Base32 decode."""
    action_type = "encode4_base32_decode"
    display_name = "Base32解码"
    description = "将Base32字符串解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base32 decode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with decoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'decoded_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            decoded = base64.b32decode(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"Base32解码完成",
                data={
                    'original': resolved,
                    'result': decoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base32解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decoded_result'}


class EncodeBase16EncodeAction(BaseAction):
    """Base16 encode."""
    action_type = "encode4_base16_encode"
    display_name = "Base16编码"
    description = "将字符串进行Base16编码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base16 encode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with Base16 encoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'base16_result')

        try:
            resolved = str(context.resolve_value(value))
            encoded = base64.b16encode(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"Base16编码完成: {len(encoded)} 字符",
                data={
                    'original': resolved,
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base16编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base16_result'}


class EncodeBase16DecodeAction(BaseAction):
    """Base16 decode."""
    action_type = "encode4_base16_decode"
    display_name = "Base16解码"
    description = "将Base16字符串解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base16 decode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with decoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'decoded_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            decoded = base64.b16decode(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"Base16解码完成",
                data={
                    'original': resolved,
                    'result': decoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base16解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decoded_result'}


class EncodeQuotedPrintableAction(BaseAction):
    """Quoted printable encode."""
    action_type = "encode4_quoted_printable"
    display_name = "Quoted Printable编码"
    description = "将字符串进行Quoted Printable编码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute quoted printable encode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with encoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'qp_result')

        try:
            import quopri
            resolved = str(context.resolve_value(value))
            encoded = quopri.encodestring(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"Quoted Printable编码完成",
                data={
                    'original': resolved,
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Quoted Printable编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'qp_result'}

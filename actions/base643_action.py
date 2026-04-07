"""Base643 action module for RabAI AutoClick.

Provides additional base64 operations:
- Base64EncodeAction: Base64 encode
- Base64DecodeAction: Base64 decode
- Base64URLEncodeAction: Base64 URL encode
- Base64URLDecodeAction: Base64 URL decode
- Base64ToHexAction: Base64 to hex
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Base64 encode."""
    action_type = "base643_encode"
    display_name = "Base64编码"
    description = "Base64编码"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 encode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with base64 encoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'base64_encoded')

        try:
            import base64

            resolved = context.resolve_value(text)
            encoded = base64.b64encode(str(resolved).encode()).decode()
            result = encoded

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64编码: {result[:50]}...",
                data={
                    'original': resolved,
                    'encoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_encoded'}


class Base64DecodeAction(BaseAction):
    """Base64 decode."""
    action_type = "base643_decode"
    display_name = "Base64解码"
    description = "Base64解码"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 decode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with base64 decoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'base64_decoded')

        try:
            import base64

            resolved = context.resolve_value(text)
            decoded = base64.b64decode(resolved.encode()).decode()
            result = decoded

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64解码: {result[:50]}...",
                data={
                    'original': resolved,
                    'decoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_decoded'}


class Base64URLEncodeAction(BaseAction):
    """Base64 URL encode."""
    action_type = "base643_url_encode"
    display_name = "Base64URL编码"
    description = "Base64 URL安全编码"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 URL encode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with base64 URL encoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'base64_url_encoded')

        try:
            import base64

            resolved = context.resolve_value(text)
            encoded = base64.urlsafe_b64encode(str(resolved).encode()).decode()
            result = encoded

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64URL编码: {result[:50]}...",
                data={
                    'original': resolved,
                    'encoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64URL编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_url_encoded'}


class Base64URLDecodeAction(BaseAction):
    """Base64 URL decode."""
    action_type = "base643_url_decode"
    display_name = "Base64URL解码"
    description = "Base64 URL安全解码"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 URL decode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with base64 URL decoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'base64_url_decoded')

        try:
            import base64

            resolved = context.resolve_value(text)
            decoded = base64.urlsafe_b64decode(resolved.encode()).decode()
            result = decoded

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64URL解码: {result[:50]}...",
                data={
                    'original': resolved,
                    'decoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64URL解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_url_decoded'}


class Base64ToHexAction(BaseAction):
    """Base64 to hex."""
    action_type = "base643_to_hex"
    display_name = "Base64转十六进制"
    description = "将Base64转换为十六进制"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 to hex.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with hex string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'hex_from_base64')

        try:
            import base64
            import binascii

            resolved = context.resolve_value(text)
            decoded = base64.b64decode(resolved.encode())
            result = binascii.hexlify(decoded).decode()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64转十六进制: {result[:50]}...",
                data={
                    'original': resolved,
                    'hex': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64转十六进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_from_base64'}
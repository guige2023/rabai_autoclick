"""Base6411 action module for RabAI AutoClick.

Provides additional base64 operations:
- Base64EncodeAction: Base64 encode
- Base64DecodeAction: Base64 decode
- Base64URLEncodeAction: Base64 URL encode
- Base64URLDecodeAction: Base64 URL decode
- Base32EncodeAction: Base32 encode
- Base32DecodeAction: Base32 decode
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Base64 encode."""
    action_type = "base6411_encode"
    display_name = "Base64编码"
    description = "Base64编码"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 encode.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with encoded data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'encoded_data')

        try:
            import base64

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            result = base64.b64encode(resolved).decode('ascii')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64编码: {len(result)}字符",
                data={
                    'original_size': len(resolved),
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encoded_data'}


class Base64DecodeAction(BaseAction):
    """Base64 decode."""
    action_type = "base6411_decode"
    display_name = "Base64解码"
    description = "Base64解码"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 decode.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with decoded data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'decoded_data')

        try:
            import base64

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('ascii')

            result = base64.b64decode(resolved).decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64解码: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decoded_data'}


class Base64URLEncodeAction(BaseAction):
    """Base64 URL encode."""
    action_type = "base6411_url_encode"
    display_name = "Base64URL编码"
    description = "Base64 URL安全编码"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 URL encode.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with encoded data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'encoded_data')

        try:
            import base64

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            result = base64.urlsafe_b64encode(resolved).decode('ascii')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64URL编码: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64URL编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encoded_data'}


class Base64URLDecodeAction(BaseAction):
    """Base64 URL decode."""
    action_type = "base6411_url_decode"
    display_name = "Base64URL解码"
    description = "Base64 URL安全解码"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 URL decode.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with decoded data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'decoded_data')

        try:
            import base64

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('ascii')

            result = base64.urlsafe_b64decode(resolved).decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64URL解码: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64URL解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decoded_data'}


class Base32EncodeAction(BaseAction):
    """Base32 encode."""
    action_type = "base6411_base32_encode"
    display_name = "Base32编码"
    description = "Base32编码"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base32 encode.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with encoded data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'encoded_data')

        try:
            import base64

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            result = base64.b32encode(resolved).decode('ascii')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base32编码: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base32编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'encoded_data'}


class Base32DecodeAction(BaseAction):
    """Base32 decode."""
    action_type = "base6411_base32_decode"
    display_name = "Base32解码"
    description = "Base32解码"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base32 decode.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with decoded data.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'decoded_data')

        try:
            import base64

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('ascii')

            result = base64.b32decode(resolved).decode('utf-8')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base32解码: {len(result)}字符",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base32解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decoded_data'}
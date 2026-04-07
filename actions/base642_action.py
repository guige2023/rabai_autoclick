"""Base642 action module for RabAI AutoClick.

Provides additional base64 operations:
- Base64EncodeAction: Base64 encode
- Base64DecodeAction: Base64 decode
- Base64UrlSafeAction: URL-safe base64 encode
- Base64FromBytesAction: Bytes to base64
- Base64ToBytesAction: Base64 to bytes
"""

import base64
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Base64 encode."""
    action_type = "base642_encode"
    display_name = "Base642编码"
    description = "将字符串进行Base64编码"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 encode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with base64 encoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'base64_result')

        try:
            resolved = str(context.resolve_value(value))
            encoded = base64.b64encode(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"Base64编码完成: {len(encoded)} 字符",
                data={
                    'original': resolved,
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_result'}


class Base64DecodeAction(BaseAction):
    """Base64 decode."""
    action_type = "base642_decode"
    display_name = "Base642解码"
    description = "将Base64字符串解码"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 decode.

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
            decoded = base64.b64decode(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"Base64解码完成",
                data={
                    'original': resolved,
                    'result': decoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decode_result'}


class Base64UrlSafeAction(BaseAction):
    """URL-safe base64 encode."""
    action_type = "base642_urlsafe"
    display_name = "Base64URL安全编码"
    description = "进行URL安全的Base64编码"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute url-safe base64 encode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with url-safe base64 string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'urlsafe_result')

        try:
            resolved = str(context.resolve_value(value))
            encoded = base64.urlsafe_b64encode(resolved.encode('utf-8')).decode('utf-8')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"URL安全Base64编码完成",
                data={
                    'original': resolved,
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL安全Base64编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'urlsafe_result'}


class Base64FromBytesAction(BaseAction):
    """Bytes to base64."""
    action_type = "base642_from_bytes"
    display_name = "字节转Base64"
    description = "将字节数组转换为Base64"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bytes to base64.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with base64 string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'base64_result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            encoded = base64.b64encode(resolved).decode('utf-8')
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"字节转Base64完成",
                data={
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字节转Base64失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_result'}


class Base64ToBytesAction(BaseAction):
    """Base64 to bytes."""
    action_type = "base642_to_bytes"
    display_name = "Base64转字节"
    description = "将Base64转换为字节数组"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 to bytes.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with bytes.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'bytes_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            decoded = base64.b64decode(resolved.encode('utf-8'))
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"Base64转字节完成: {len(decoded)} 字节",
                data={
                    'original': resolved,
                    'result': decoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64转字节失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bytes_result'}
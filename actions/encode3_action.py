"""Encode3 action module for RabAI AutoClick.

Provides additional encoding operations:
- EncodeBase64EncodeAction: Base64 encode
- EncodeBase64DecodeAction: Base64 decode
- EncodeUrlEncodeAction: URL encode
- EncodeUrlDecodeAction: URL decode
- EncodeHexEncodeAction: Hex encode
"""

import base64
import urllib.parse
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EncodeBase64EncodeAction(BaseAction):
    """Base64 encode."""
    action_type = "encode3_base64_encode"
    display_name = "Base64编码"
    description = "将字符串进行Base64编码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base64 encode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with Base64 encoded string.
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


class EncodeBase64DecodeAction(BaseAction):
    """Base64 decode."""
    action_type = "encode3_base64_decode"
    display_name = "Base64解码"
    description = "将Base64字符串解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Base64 decode.

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
        return {'output_var': 'decoded_result'}


class EncodeUrlEncodeAction(BaseAction):
    """URL encode."""
    action_type = "encode3_url_encode"
    display_name = "URL编码"
    description = "将字符串进行URL编码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL encode.

        Args:
            context: Execution context.
            params: Dict with value, safe, output_var.

        Returns:
            ActionResult with URL encoded string.
        """
        value = params.get('value', '')
        safe = params.get('safe', '')
        output_var = params.get('output_var', 'url_encoded')

        try:
            resolved = str(context.resolve_value(value))
            resolved_safe = context.resolve_value(safe) if safe else ''

            encoded = urllib.parse.quote(resolved, safe=resolved_safe)
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"URL编码完成",
                data={
                    'original': resolved,
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'safe': '', 'output_var': 'url_encoded'}


class EncodeUrlDecodeAction(BaseAction):
    """URL decode."""
    action_type = "encode3_url_decode"
    display_name = "URL解码"
    description = "将URL编码字符串解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL decode.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with decoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'url_decoded')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            decoded = urllib.parse.unquote(resolved)
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"URL解码完成",
                data={
                    'original': resolved,
                    'result': decoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_decoded'}


class EncodeHexEncodeAction(BaseAction):
    """Hex encode."""
    action_type = "encode3_hex_encode"
    display_name = "十六进制编码"
    description = "将字符串进行十六进制编码"

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
            encoded = resolved.encode('utf-8').hex()
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"十六进制编码完成: {len(encoded)} 字符",
                data={
                    'original': resolved,
                    'result': encoded,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"十六进制编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_result'}

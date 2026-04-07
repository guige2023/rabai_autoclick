"""Encode6 action module for RabAI AutoClick.

Provides additional encoding operations:
- EncodeBase64Action: Base64 encode/decode
- EncodeHexAction: Hex encode/decode
- EncodeURLAction: URL encode/decode
- EncodeHTMLAction: HTML encode/decode
- EncodeUnicodeAction: Unicode escape/unescape
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EncodeBase64Action(BaseAction):
    """Base64 encode/decode."""
    action_type = "encode6_base64"
    display_name = "Base64编码"
    description = "Base64编码解码"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute base64 encode/decode.

        Args:
            context: Execution context.
            params: Dict with data, mode, output_var.

        Returns:
            ActionResult with encoded/decoded data.
        """
        data = params.get('data', '')
        mode = params.get('mode', 'encode')
        output_var = params.get('output_var', 'base64_result')

        try:
            import base64

            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            if mode == 'encode':
                result = base64.b64encode(resolved).decode('ascii')
            else:
                result = base64.b64decode(resolved).decode('utf-8')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Base64{mode}成功",
                data={
                    'mode': mode,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64{mode}失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'encode', 'output_var': 'base64_result'}


class EncodeHexAction(BaseAction):
    """Hex encode/decode."""
    action_type = "encode6_hex"
    display_name = "Hex编码"
    description = "Hex编码解码"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hex encode/decode.

        Args:
            context: Execution context.
            params: Dict with data, mode, output_var.

        Returns:
            ActionResult with encoded/decoded data.
        """
        data = params.get('data', '')
        mode = params.get('mode', 'encode')
        output_var = params.get('output_var', 'hex_result')

        try:
            resolved = context.resolve_value(data)

            if isinstance(resolved, str):
                resolved = resolved.encode('utf-8')

            if mode == 'encode':
                result = resolved.hex()
            else:
                result = bytes.fromhex(resolved).decode('utf-8')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Hex{mode}成功",
                data={
                    'mode': mode,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hex{mode}失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'encode', 'output_var': 'hex_result'}


class EncodeURLAction(BaseAction):
    """URL encode/decode."""
    action_type = "encode6_url"
    display_name = "URL编码"
    description = "URL编码解码"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL encode/decode.

        Args:
            context: Execution context.
            params: Dict with data, mode, output_var.

        Returns:
            ActionResult with encoded/decoded data.
        """
        data = params.get('data', '')
        mode = params.get('mode', 'encode')
        output_var = params.get('output_var', 'url_result')

        try:
            import urllib.parse

            resolved = context.resolve_value(data)

            if mode == 'encode':
                result = urllib.parse.quote(resolved)
            else:
                result = urllib.parse.unquote(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL{mode}成功",
                data={
                    'mode': mode,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL{mode}失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'encode', 'output_var': 'url_result'}


class EncodeHTMLAction(BaseAction):
    """HTML encode/decode."""
    action_type = "encode6_html"
    display_name = "HTML编码"
    description = "HTML编码解码"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML encode/decode.

        Args:
            context: Execution context.
            params: Dict with data, mode, output_var.

        Returns:
            ActionResult with encoded/decoded data.
        """
        data = params.get('data', '')
        mode = params.get('mode', 'encode')
        output_var = params.get('output_var', 'html_result')

        try:
            import html

            resolved = context.resolve_value(data)

            if mode == 'encode':
                result = html.escape(resolved)
            else:
                result = html.unescape(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTML{mode}成功",
                data={
                    'mode': mode,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML{mode}失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'encode', 'output_var': 'html_result'}


class EncodeUnicodeAction(BaseAction):
    """Unicode escape/unescape."""
    action_type = "encode6_unicode"
    display_name = "Unicode编码"
    description = "Unicode转义"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unicode escape/unescape.

        Args:
            context: Execution context.
            params: Dict with data, mode, output_var.

        Returns:
            ActionResult with escaped/unescaped data.
        """
        data = params.get('data', '')
        mode = params.get('mode', 'escape')
        output_var = params.get('output_var', 'unicode_result')

        try:
            resolved = context.resolve_value(data)

            if mode == 'escape':
                result = resolved.encode('unicode_escape').decode('utf-8')
            else:
                result = resolved.encode('utf-8').decode('unicode_escape')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"Unicode {mode}成功",
                data={
                    'mode': mode,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Unicode {mode}失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'escape', 'output_var': 'unicode_result'}
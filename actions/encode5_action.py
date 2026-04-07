"""Encode5 action module for RabAI AutoClick.

Provides additional encoding operations:
- EncodeURLAction: URL encode
- EncodeURLDecodeAction: URL decode
- EncodeHTMLAction: HTML encode
- EncodeHTMLDecodeAction: HTML decode
- EncodeHexAction: Hex encode
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EncodeURLAction(BaseAction):
    """URL encode."""
    action_type = "encode5_url"
    display_name = "URL编码"
    description = "URL编码"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL encode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with URL encoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'url_encoded')

        try:
            import urllib.parse

            resolved = context.resolve_value(text)
            result = urllib.parse.quote(str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL编码: {result[:50]}...",
                data={
                    'original': resolved,
                    'encoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_encoded'}


class EncodeURLDecodeAction(BaseAction):
    """URL decode."""
    action_type = "encode5_url_decode"
    display_name = "URL解码"
    description = "URL解码"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL decode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with URL decoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'url_decoded')

        try:
            import urllib.parse

            resolved = context.resolve_value(text)
            result = urllib.parse.unquote(str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL解码: {result[:50]}...",
                data={
                    'original': resolved,
                    'decoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_decoded'}


class EncodeHTMLAction(BaseAction):
    """HTML encode."""
    action_type = "encode5_html"
    display_name = "HTML编码"
    description = "HTML编码"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML encode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with HTML encoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'html_encoded')

        try:
            import html

            resolved = context.resolve_value(text)
            result = html.escape(str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTML编码: {result[:50]}...",
                data={
                    'original': resolved,
                    'encoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_encoded'}


class EncodeHTMLDecodeAction(BaseAction):
    """HTML decode."""
    action_type = "encode5_html_decode"
    display_name = "HTML解码"
    description = "HTML解码"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute HTML decode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with HTML decoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'html_decoded')

        try:
            import html

            resolved = context.resolve_value(text)
            result = html.unescape(str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"HTML解码: {result[:50]}...",
                data={
                    'original': resolved,
                    'decoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_decoded'}


class EncodeHexAction(BaseAction):
    """Hex encode."""
    action_type = "encode5_hex"
    display_name = "十六进制编码"
    description = "将字符串编码为十六进制"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hex encode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with hex encoded text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'hex_encoded')

        try:
            resolved = context.resolve_value(text)
            result = str(resolved).encode('utf-8').hex()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"十六进制编码: {result[:50]}...",
                data={
                    'original': resolved,
                    'encoded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"十六进制编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_encoded'}
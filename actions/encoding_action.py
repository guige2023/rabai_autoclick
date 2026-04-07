"""Encoding/decoding action module for RabAI AutoClick.

Provides encoding operations:
- Base64EncodeAction: Base64 encode
- Base64DecodeAction: Base64 decode
- HexEncodeAction: Hex encode
- HexDecodeAction: Hex decode
- UrlEncodeAction: URL encode
- UrlDecodeAction: URL decode
- HtmlEncodeAction: HTML entity encode
- HtmlDecodeAction: HTML entity decode
"""

from __future__ import annotations

import base64
import sys
import html
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Base64 encode."""
    action_type = "base64_encode"
    display_name = "Base64编码"
    description = "Base64编码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute base64 encode."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'base64_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            if isinstance(resolved, str):
                data = resolved.encode('utf-8')
            else:
                data = resolved

            result = base64.b64encode(data).decode('ascii')
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Base64 encoded", data={'encoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 encode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'base64_result'}


class Base64DecodeAction(BaseAction):
    """Base64 decode."""
    action_type = "base64_decode"
    display_name = "Base64解码"
    description = "Base64解码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute base64 decode."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'base64_decoded')
        as_string = params.get('as_string', True)

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            data = base64.b64decode(str(resolved))

            if as_string:
                try:
                    result = data.decode('utf-8')
                except UnicodeDecodeError:
                    result = data.hex()
            else:
                result = data

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Base64 decoded", data={'decoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 decode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'as_string': True, 'output_var': 'base64_decoded'}


class HexEncodeAction(BaseAction):
    """Hex encode."""
    action_type = "hex_encode"
    display_name = "Hex编码"
    description = "Hex编码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute hex encode."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'hex_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            if isinstance(resolved, str):
                data = resolved.encode('utf-8')
            else:
                data = resolved

            result = data.hex()
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Hex encoded", data={'encoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Hex encode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hex_result'}


class HexDecodeAction(BaseAction):
    """Hex decode."""
    action_type = "hex_decode"
    display_name = "Hex解码"
    description = "Hex解码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute hex decode."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'hex_decoded')
        as_string = params.get('as_string', True)

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            data = bytes.fromhex(str(resolved))

            if as_string:
                try:
                    result = data.decode('utf-8')
                except UnicodeDecodeError:
                    result = data.hex()
            else:
                result = data

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Hex decoded", data={'decoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Hex decode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'as_string': True, 'output_var': 'hex_decoded'}


class HtmlEncodeAction(BaseAction):
    """HTML entity encode."""
    action_type = "html_encode"
    display_name = "HTML编码"
    description = "HTML实体编码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTML encode."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'html_encoded')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            result = html.escape(str(resolved))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"HTML encoded", data={'encoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"HTML encode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_encoded'}


class HtmlDecodeAction(BaseAction):
    """HTML entity decode."""
    action_type = "html_decode"
    display_name = "HTML解码"
    description = "HTML实体解码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTML decode."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'html_decoded')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            result = html.unescape(str(resolved))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"HTML decoded", data={'decoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"HTML decode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_decoded'}


class QuotedPrintableAction(BaseAction):
    """Quoted-printable encode/decode."""
    action_type = "quoted_printable"
    display_name = "QuotedPrintable编码"
    description = "QuotedPrintable编码解码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute quoted-printable."""
        value = params.get('value', '')
        decode = params.get('decode', False)
        output_var = params.get('output_var', 'qp_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            import quopri

            resolved = context.resolve_value(value) if context else value
            resolved_decode = context.resolve_value(decode) if context else decode

            if isinstance(resolved, str):
                data = resolved.encode('utf-8')
            else:
                data = resolved

            if resolved_decode:
                result = quopri.decodestring(data).decode('utf-8')
            else:
                result = quopri.encodestring(data).decode('utf-8')

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Quoted-printable {'decoded' if resolved_decode else 'encoded'}", data={'result': result})
        except ImportError:
            return ActionResult(success=False, message="quopri not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Quoted-printable error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decode': False, 'output_var': 'qp_result'}


class PercentEncodeAction(BaseAction):
    """Percent encoding (UTF-8)."""
    action_type = "percent_encode"
    display_name = "Percent编码"
    description = "Percent编码(UTF-8)"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute percent encode."""
        value = params.get('value', '')
        safe = params.get('safe', '/')
        output_var = params.get('output_var', 'percent_encoded')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            from urllib.parse import quote

            resolved = context.resolve_value(value) if context else value
            resolved_safe = context.resolve_value(safe) if context else safe

            result = quote(str(resolved), safe=resolved_safe)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Percent encoded", data={'encoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Percent encode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'safe': '/', 'output_var': 'percent_encoded'}

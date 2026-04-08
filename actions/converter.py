"""Converter action module for RabAI AutoClick.

Provides data type conversion actions including encoding,
format conversion, unit conversion, and base transformation.
"""

import sys
import os
import base64
import json
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Encode string to Base64.
    
    Supports UTF-8 encoding, URL-safe mode, and custom charset.
    """
    action_type = "base64_encode"
    display_name = "Base64编码"
    description = "将字符串编码为Base64"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Encode to Base64.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, url_safe, encoding,
                   save_to_var.
        
        Returns:
            ActionResult with encoded string.
        """
        text = params.get('text', '')
        url_safe = params.get('url_safe', False)
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if not text:
            return ActionResult(success=False, message="Input text is empty")

        try:
            if isinstance(text, str):
                text_bytes = text.encode(encoding)
            else:
                text_bytes = text

            encoded = base64.b64encode(text_bytes)
            if url_safe:
                result = encoded.decode(encoding).replace('+', '-').replace('/', '_')
            else:
                result = encoded.decode(encoding)

            result_data = {
                'encoded': result,
                'original_length': len(text),
                'encoded_length': len(result),
                'url_safe': url_safe
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"Base64编码成功: {len(result)} chars",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'url_safe': False,
            'encoding': 'utf-8',
            'save_to_var': None
        }


class Base64DecodeAction(BaseAction):
    """Decode Base64 to string.
    
    Supports URL-safe mode, JSON decode, and error handling.
    """
    action_type = "base64_decode"
    display_name = "Base64解码"
    description = "将Base64字符串解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Decode from Base64.
        
        Args:
            context: Execution context.
            params: Dict with keys: encoded, url_safe,
                   decode_json, encoding, save_to_var.
        
        Returns:
            ActionResult with decoded string.
        """
        encoded = params.get('encoded', '')
        url_safe = params.get('url_safe', False)
        decode_json = params.get('decode_json', False)
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if not encoded:
            return ActionResult(success=False, message="Encoded text is empty")

        try:
            if url_safe:
                encoded = encoded.replace('-', '+').replace('_', '/')

            # Add padding if needed
            padding = 4 - len(encoded) % 4
            if padding != 4:
                encoded += '=' * padding

            decoded_bytes = base64.b64decode(encoded)
            result = decoded_bytes.decode(encoding, errors='replace')

            result_data = {
                'decoded': result,
                'decoded_bytes': len(decoded_bytes)
            }

            if decode_json:
                try:
                    result_data['json'] = json.loads(result)
                    result_data['decoded'] = result_data['json']
                except json.JSONDecodeError:
                    result_data['json_error'] = 'Not valid JSON'

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message="Base64解码成功",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Base64解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['encoded']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'url_safe': False,
            'decode_json': False,
            'encoding': 'utf-8',
            'save_to_var': None
        }


class URLEncodeAction(BaseAction):
    """URL-encode a string.
    
    Supports query parameter encoding, full URL encoding,
    and quote plus/space modes.
    """
    action_type = "url_encode"
    display_name = "URL编码"
    description = "对字符串进行URL编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """URL encode a string.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, safe, quote_plus,
                   save_to_var.
        
        Returns:
            ActionResult with encoded string.
        """
        from urllib.parse import quote, quote_plus

        text = params.get('text', '')
        safe = params.get('safe', '')
        quote_plus = params.get('quote_plus', False)
        save_to_var = params.get('save_to_var', None)

        if not text:
            return ActionResult(success=False, message="Input text is empty")

        try:
            if quote_plus:
                result = quote_plus(text, safe=safe)
            else:
                result = quote(text, safe=safe)

            result_data = {
                'encoded': result,
                'original': text,
                'quote_plus': quote_plus
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"URL编码成功",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'safe': '',
            'quote_plus': False,
            'save_to_var': None
        }


class URLDecodeAction(BaseAction):
    """URL-decode a string.
    
    Supports query parameter decoding, plus as space,
    and full URL decoding.
    """
    action_type = "url_decode"
    display_name = "URL解码"
    description = "对URL编码字符串进行解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """URL decode a string.
        
        Args:
            context: Execution context.
            params: Dict with keys: encoded, encoding,
                   save_to_var.
        
        Returns:
            ActionResult with decoded string.
        """
        from urllib.parse import unquote, unquote_plus

        encoded = params.get('encoded', '')
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if not encoded:
            return ActionResult(success=False, message="Input text is empty")

        try:
            # Try plus as space (query param style)
            result = unquote_plus(encoded, encoding=encoding)
            if '%' not in encoded and '+' not in encoded:
                # Might be full URL encoded
                result = unquote(encoded, encoding=encoding)

            result_data = {
                'decoded': result,
                'original': encoded
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message="URL解码成功",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['encoded']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'encoding': 'utf-8',
            'save_to_var': None
        }


class HexEncodeAction(BaseAction):
    """Encode string to hexadecimal.
    
    Supports byte-level encoding and custom separators.
    """
    action_type = "hex_encode"
    display_name = "Hex编码"
    description = "将字符串编码为十六进制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Encode to hex.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, separator, upper,
                   encoding, save_to_var.
        
        Returns:
            ActionResult with encoded hex string.
        """
        text = params.get('text', '')
        separator = params.get('separator', '')
        upper = params.get('upper', True)
        encoding = params.get('encoding', 'utf-8')
        save_to_var = params.get('save_to_var', None)

        if not text:
            return ActionResult(success=False, message="Input text is empty")

        try:
            if isinstance(text, str):
                text_bytes = text.encode(encoding)
            else:
                text_bytes = text

            hex_str = text_bytes.hex()
            if upper:
                hex_str = hex_str.upper()

            if separator:
                hex_str = separator.join(
                    hex_str[i:i+2] for i in range(0, len(hex_str), 2)
                )

            result_data = {
                'encoded': hex_str,
                'original_length': len(text),
                'hex_length': len(hex_str)
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message="Hex编码成功",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Hex编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'separator': '',
            'upper': True,
            'encoding': 'utf-8',
            'save_to_var': None
        }


class TypeConvertAction(BaseAction):
    """Convert value between basic Python types.
    
    Supports int, float, str, bool, list, dict conversions
    with error handling and default values.
    """
    action_type = "type_convert"
    display_name = "类型转换"
    description = "转换Python数据类型"

    VALID_TYPES = ["int", "float", "str", "bool", "list", "dict"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Convert value type.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, target_type,
                   default_value, save_to_var.
        
        Returns:
            ActionResult with converted value.
        """
        value = params.get('value', None)
        target_type = params.get('target_type', 'str')
        default_value = params.get('default_value', None)
        save_to_var = params.get('save_to_var', None)

        if target_type not in self.VALID_TYPES:
            return ActionResult(
                success=False,
                message=f"Invalid target_type: {target_type}. Valid: {self.VALID_TYPES}"
            )

        try:
            if target_type == 'int':
                if isinstance(value, bool):
                    result = int(value)
                else:
                    result = int(float(value))
            elif target_type == 'float':
                result = float(value)
            elif target_type == 'str':
                result = str(value)
            elif target_type == 'bool':
                result = bool(value)
            elif target_type == 'list':
                if isinstance(value, str):
                    result = [value]
                else:
                    result = list(value) if value is not None else []
            elif target_type == 'dict':
                if isinstance(value, str):
                    result = json.loads(value)
                else:
                    result = dict(value) if value is not None else {}
            else:
                result = value

            result_data = {
                'converted': result,
                'original': value,
                'original_type': type(value).__name__,
                'target_type': target_type
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"类型转换成功: {type(value).__name__} -> {target_type}",
                data=result_data
            )
        except (ValueError, TypeError, json.JSONDecodeError) as e:
            if default_value is not None:
                result_data = {
                    'converted': default_value,
                    'original': value,
                    'used_default': True,
                    'error': str(e)
                }
                if save_to_var:
                    context.variables[save_to_var] = result_data
                return ActionResult(
                    success=True,
                    message=f"类型转换失败，使用默认值",
                    data=result_data
                )
            return ActionResult(
                success=False,
                message=f"类型转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'target_type']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'default_value': None,
            'save_to_var': None
        }

"""Encoder action module for RabAI AutoClick.

Provides encoding and decoding actions for various formats
including base64, URL encoding, HTML entities, and hex.
"""

import base64
import binascii
import html
import urllib.parse
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EncoderAction(BaseAction):
    """Base encoder/decoder action."""
    action_type = "encode"
    display_name = "编码"
    description = "数据编码操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Encode data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, encoding.
        
        Returns:
            ActionResult with encoded data.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'base64')
        
        if not data and data != 0:
            return ActionResult(success=False, message="data is required")
        
        try:
            if encoding == 'base64':
                result = self._encode_base64(str(data))
            elif encoding == 'url':
                result = self._encode_url(str(data))
            elif encoding == 'html':
                result = self._encode_html(str(data))
            elif encoding == 'hex':
                result = self._encode_hex(str(data))
            elif encoding == 'json':
                result = self._encode_json(data)
            else:
                return ActionResult(success=False, message=f"Unknown encoding: {encoding}")
            
            return ActionResult(success=True, message=f"Encoded to {encoding}", data={"result": result, "encoding": encoding})
        except Exception as e:
            return ActionResult(success=False, message=f"Encoding failed: {str(e)}")
    
    def _encode_base64(self, data: str) -> str:
        return base64.b64encode(data.encode('utf-8')).decode('ascii')
    
    def _encode_url(self, data: str) -> str:
        return urllib.parse.quote(data)
    
    def _encode_html(self, data: str) -> str:
        return html.escape(data)
    
    def _encode_hex(self, data: str) -> str:
        return data.encode('utf-8').hex()
    
    def _encode_json(self, data: Any) -> str:
        return json.dumps(data, ensure_ascii=False)


class DecoderAction(BaseAction):
    """Base decoder action."""
    action_type = "decode"
    display_name = "解码"
    description = "数据解码操作"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Decode data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, encoding.
        
        Returns:
            ActionResult with decoded data.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'base64')
        
        if not data and data != 0:
            return ActionResult(success=False, message="data is required")
        
        try:
            if encoding == 'base64':
                result = self._decode_base64(str(data))
            elif encoding == 'url':
                result = self._decode_url(str(data))
            elif encoding == 'html':
                result = self._decode_html(str(data))
            elif encoding == 'hex':
                result = self._decode_hex(str(data))
            elif encoding == 'json':
                result = self._decode_json(str(data))
            else:
                return ActionResult(success=False, message=f"Unknown encoding: {encoding}")
            
            return ActionResult(success=True, message=f"Decoded from {encoding}", data={"result": result, "encoding": encoding})
        except Exception as e:
            return ActionResult(success=False, message=f"Decoding failed: {str(e)}")
    
    def _decode_base64(self, data: str) -> str:
        return base64.b64decode(data.encode('ascii')).decode('utf-8')
    
    def _decode_url(self, data: str) -> str:
        return urllib.parse.unquote(data)
    
    def _decode_html(self, data: str) -> str:
        return html.unescape(data)
    
    def _decode_hex(self, data: str) -> str:
        return bytes.fromhex(data).decode('utf-8')
    
    def _decode_json(self, data: str) -> Any:
        return json.loads(data)


class Base64EncodeAction(BaseAction):
    """Encode data to base64."""
    action_type = "base64_encode"
    display_name = "Base64编码"
    description = "Base64编码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Encode to base64.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with base64 encoded data.
        """
        data = params.get('data', '')
        
        if not data and data != 0:
            return ActionResult(success=False, message="data is required")
        
        try:
            encoded = base64.b64encode(str(data).encode('utf-8')).decode('ascii')
            return ActionResult(success=True, message="Base64 encoded", data={"result": encoded})
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 encode failed: {str(e)}")


class Base64DecodeAction(BaseAction):
    """Decode data from base64."""
    action_type = "base64_decode"
    display_name = "Base64解码"
    description = "Base64解码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Decode from base64.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with decoded data.
        """
        data = params.get('data', '')
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        try:
            decoded = base64.b64decode(data.encode('ascii')).decode('utf-8')
            return ActionResult(success=True, message="Base64 decoded", data={"result": decoded})
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 decode failed: {str(e)}")


class URLEncodeAction(BaseAction):
    """URL encode data."""
    action_type = "url_encode"
    display_name = "URL编码"
    description = "URL编码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """URL encode.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with URL encoded data.
        """
        data = params.get('data', '')
        
        if not data and data != 0:
            return ActionResult(success=False, message="data is required")
        
        try:
            encoded = urllib.parse.quote(str(data))
            return ActionResult(success=True, message="URL encoded", data={"result": encoded})
        except Exception as e:
            return ActionResult(success=False, message=f"URL encode failed: {str(e)}")


class URLDecodeAction(BaseAction):
    """URL decode data."""
    action_type = "url_decode"
    display_name = "URL解码"
    description = "URL解码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """URL decode.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with URL decoded data.
        """
        data = params.get('data', '')
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        try:
            decoded = urllib.parse.unquote(str(data))
            return ActionResult(success=True, message="URL decoded", data={"result": decoded})
        except Exception as e:
            return ActionResult(success=False, message=f"URL decode failed: {str(e)}")


class HTMLEncodeAction(BaseAction):
    """HTML encode data."""
    action_type = "html_encode"
    display_name = "HTML编码"
    description = "HTML实体编码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """HTML encode.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with HTML encoded data.
        """
        data = params.get('data', '')
        
        if not data and data != 0:
            return ActionResult(success=False, message="data is required")
        
        try:
            encoded = html.escape(str(data))
            return ActionResult(success=True, message="HTML encoded", data={"result": encoded})
        except Exception as e:
            return ActionResult(success=False, message=f"HTML encode failed: {str(e)}")


class HTMLDecodeAction(BaseAction):
    """HTML decode data."""
    action_type = "html_decode"
    display_name = "HTML解码"
    description = "HTML实体解码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """HTML decode.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with HTML decoded data.
        """
        data = params.get('data', '')
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        try:
            decoded = html.unescape(str(data))
            return ActionResult(success=True, message="HTML decoded", data={"result": decoded})
        except Exception as e:
            return ActionResult(success=False, message=f"HTML decode failed: {str(e)}")


class HexEncodeAction(BaseAction):
    """Encode data to hex."""
    action_type = "hex_encode"
    display_name = "Hex编码"
    description = "十六进制编码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Encode to hex.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with hex encoded data.
        """
        data = params.get('data', '')
        
        if not data and data != 0:
            return ActionResult(success=False, message="data is required")
        
        try:
            encoded = str(data).encode('utf-8').hex()
            return ActionResult(success=True, message="Hex encoded", data={"result": encoded})
        except Exception as e:
            return ActionResult(success=False, message=f"Hex encode failed: {str(e)}")


class HexDecodeAction(BaseAction):
    """Decode data from hex."""
    action_type = "hex_decode"
    display_name = "Hex解码"
    description = "十六进制解码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Decode from hex.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with decoded data.
        """
        data = params.get('data', '')
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        try:
            decoded = bytes.fromhex(str(data)).decode('utf-8')
            return ActionResult(success=True, message="Hex decoded", data={"result": decoded})
        except Exception as e:
            return ActionResult(success=False, message=f"Hex decode failed: {str(e)}")

"""Data encoding action module for RabAI AutoClick.

Provides data encoding and decoding with multiple formats:
base64, hex, URL, HTML, JSON, and custom encodings.
"""

import sys
import os
import base64
import json
import urllib.parse
import html
import zlib
import gzip
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataEncodingAction(BaseAction):
    """Encode and decode data in various formats.
    
    Supports base64, hex, URL, HTML, JSON, gzip,
    and sequential encoding chains.
    """
    action_type = "data_encoding"
    display_name = "数据编码"
    description = "数据编码解码：base64/hex/URL/HTML/JSON/gzip"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Encode or decode data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (encode/decode)
                - data: str, data to encode/decode
                - encoding: str (base64/hex/url/html/json/gzip/compress)
                - chain: list of encodings for sequential transform
                - encoding_type: str (for json decode target: str/float/int/bool/list/dict)
                - save_to_var: str
        
        Returns:
            ActionResult with encoded/decoded data.
        """
        operation = params.get('operation', 'encode')
        data = params.get('data', '')
        encoding = params.get('encoding', 'base64')
        chain = params.get('chain', [])
        encoding_type = params.get('encoding_type', 'str')
        save_to_var = params.get('save_to_var', None)

        if data is None or data == '':
            return ActionResult(success=False, message="data is required")

        try:
            if chain:
                result = self._apply_chain(data, operation, chain)
            else:
                result = self._apply_single(data, operation, encoding, encoding_type)

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = result

            op_str = "Decoded" if operation == 'decode' else "Encoded"
            return ActionResult(
                success=True,
                message=f"{op_str} using {encoding}",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Encoding error: {e}")

    def _apply_single(
        self, data: Any, operation: str, encoding: str, encoding_type: str
    ) -> Any:
        """Apply single encoding/decoding."""
        if operation == 'encode':
            return self._encode(str(data), encoding)
        else:
            return self._decode(str(data), encoding, encoding_type)

    def _apply_chain(self, data: Any, operation: str, chain: List[str]) -> Any:
        """Apply chain of encodings."""
        result = data
        ops = chain if operation == 'encode' else list(reversed(chain))
        for enc in ops:
            if operation == 'encode':
                result = self._encode(str(result), enc)
            else:
                result = self._decode(str(result), enc, 'str')
        return result

    def _encode(self, data: str, encoding: str) -> str:
        """Encode data."""
        if encoding == 'base64':
            return base64.b64encode(data.encode('utf-8')).decode('ascii')
        elif encoding == 'hex':
            return data.encode('utf-8').hex()
        elif encoding == 'url':
            return urllib.parse.quote_plus(data)
        elif encoding == 'html':
            return html.escape(data)
        elif encoding == 'json':
            return json.dumps(data)
        elif encoding == 'gzip':
            compressed = gzip.compress(data.encode('utf-8'))
            return base64.b64encode(compressed).decode('ascii')
        elif encoding == 'compress':
            compressed = zlib.compress(data.encode('utf-8'))
            return base64.b64encode(compressed).decode('ascii')
        elif encoding == 'unicode':
            return data.encode('unicode_escape').decode('ascii')
        else:
            raise ValueError(f"Unknown encoding: {encoding}")

    def _decode(self, data: str, encoding: str, target_type: str) -> Any:
        """Decode data."""
        if encoding == 'base64':
            decoded = base64.b64decode(data.encode('ascii')).decode('utf-8')
        elif encoding == 'hex':
            decoded = bytes.fromhex(data).decode('utf-8')
        elif encoding == 'url':
            decoded = urllib.parse.unquote_plus(data)
        elif encoding == 'html':
            decoded = html.unescape(data)
        elif encoding == 'json':
            return json.loads(data)
        elif encoding == 'gzip':
            compressed = base64.b64decode(data.encode('ascii'))
            decoded = gzip.decompress(compressed).decode('utf-8')
        elif encoding == 'compress':
            compressed = base64.b64decode(data.encode('ascii'))
            decoded = zlib.decompress(compressed).decode('utf-8')
        elif encoding == 'unicode':
            decoded = data.encode('ascii').decode('unicode_escape')
        else:
            raise ValueError(f"Unknown encoding: {encoding}")

        if target_type == 'str':
            return decoded
        elif target_type == 'int':
            return int(decoded)
        elif target_type == 'float':
            return float(decoded)
        elif target_type == 'bool':
            return decoded.lower() in ('true', '1', 'yes')
        elif target_type == 'list':
            try:
                return json.loads(decoded)
            except Exception:
                return [decoded]
        elif target_type == 'dict':
            try:
                return json.loads(decoded)
            except Exception:
                return {'value': decoded}
        return decoded

    def get_required_params(self) -> List[str]:
        return ['operation', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'encoding': 'base64',
            'chain': [],
            'encoding_type': 'str',
            'save_to_var': None,
        }

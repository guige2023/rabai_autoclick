"""Encoding action module for RabAI AutoClick.

Provides encoding/decoding actions: base64, URL,
HTML, JSON, hex, and custom encoding.
"""

import sys
import os
import base64
import urllib.parse
import html
import json
import quopri
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EncodeAction(BaseAction):
    """Encode data using various encoding schemes.
    
    Supports base64, URL, HTML, hex, unicode-escape,
    and quoted-printable encoding.
    """
    action_type = "encode"
    display_name = "编码"
    description = "使用多种编码方案编码数据"

    ENCODINGS = ['base64', 'url', 'html', 'hex', 'unicode', 'quoted-printable', 'utf8']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Encode data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: str (data to encode)
                - encoding: str (encoding type)
                - save_to_var: str
        
        Returns:
            ActionResult with encoded data.
        """
        data = params.get('data', '')
        encoding = params.get('encoding', 'base64')
        save_to_var = params.get('save_to_var', 'encoded')

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode('utf-8')

            if encoding == 'base64':
                result = base64.b64encode(data_bytes).decode('ascii')
            elif encoding == 'url':
                result = urllib.parse.quote(data_bytes)
            elif encoding == 'html':
                result = html.escape(data_bytes.decode('utf-8', errors='replace'))
            elif encoding == 'hex':
                result = data_bytes.hex()
            elif encoding == 'unicode':
                result = data_bytes.decode('utf-8', errors='replace').encode('unicode_escape').decode('ascii')
            elif encoding == 'quoted-printable':
                result = quopri.encodestring(data_bytes).decode('ascii')
            elif encoding == 'utf8':
                result = data_bytes.decode('utf-8')
            else:
                result = data_bytes.decode('utf-8', errors='replace')

            if context and save_to_var:
                context.variables[save_to_var] = result

            return ActionResult(
                success=True,
                data={'encoded': result, 'encoding': encoding},
                message=f"Encoded with {encoding}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Encoding error: {e}")


class DecodeAction(BaseAction):
    """Decode data from various encoding schemes.
    
    Supports base64, URL, HTML, hex, unicode-escape,
    and quoted-printable decoding.
    """
    action_type = "decode"
    display_name = "解码"
    description = "从多种编码方案解码数据"

    DECODINGS = ['base64', 'url', 'html', 'hex', 'unicode', 'quoted-printable', 'utf8']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Decode data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: str (data to decode)
                - decoding: str (decoding type)
                - save_to_var: str
        
        Returns:
            ActionResult with decoded data.
        """
        data = params.get('data', '')
        decoding = params.get('decoding', 'base64')
        save_to_var = params.get('save_to_var', 'decoded')

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if isinstance(data, str):
                data_str = data
            else:
                data_str = str(data)

            if decoding == 'base64':
                result = base64.b64decode(data_str).decode('utf-8', errors='replace')
            elif decoding == 'url':
                result = urllib.parse.unquote(data_str)
            elif decoding == 'html':
                result = html.unescape(data_str)
            elif decoding == 'hex':
                result = bytes.fromhex(data_str).decode('utf-8', errors='replace')
            elif decoding == 'unicode':
                result = data_str.encode('utf-8').decode('unicode_escape')
            elif decoding == 'quoted-printable':
                result = quopri.decodestring(data_str).decode('utf-8', errors='replace')
            elif decoding == 'utf8':
                result = data_str.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
            else:
                result = data_str

            if context and save_to_var:
                context.variables[save_to_var] = result

            return ActionResult(
                success=True,
                data={'decoded': result, 'decoding': decoding},
                message=f"Decoded from {decoding}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Decoding error: {e}")


class HashAction(BaseAction):
    """Compute cryptographic hashes of data.
    
    Supports MD5, SHA1, SHA256, SHA512, and their
    HMAC variants.
    """
    action_type = "hash_compute"
    display_name = "哈希计算"
    description = "计算数据的加密哈希值"

    HASH_TYPES = ['md5', 'sha1', 'sha256', 'sha512', 'sha384', 'sha3_256', 'sha3_512']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compute hash.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: str (data to hash)
                - hash_type: str (md5/sha1/sha256/sha512/sha384/sha3_256/sha3_512)
                - as_hex: bool (return hex string, default True)
                - as_base64: bool (return base64, default False)
                - hmac_key: str (if provided, compute HMAC)
                - save_to_var: str
        
        Returns:
            ActionResult with hash result.
        """
        import hashlib

        data = params.get('data', '')
        hash_type = params.get('hash_type', 'sha256')
        as_hex = params.get('as_hex', True)
        as_base64 = params.get('as_base64', False)
        hmac_key = params.get('hmac_key', '')
        save_to_var = params.get('save_to_var', 'hash_result')

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if isinstance(data, str):
                data_bytes = data.encode('utf-8')
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode('utf-8')

            if hmac_key:
                key_bytes = hmac_key.encode('utf-8')
                if hash_type == 'md5':
                    h = hmac.new(key_bytes, data_bytes, hashlib.md5)
                elif hash_type == 'sha1':
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha1)
                elif hash_type == 'sha256':
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha256)
                elif hash_type == 'sha512':
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha512)
                elif hash_type == 'sha384':
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha384)
                else:
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha256)
            else:
                if hash_type == 'md5':
                    h = hashlib.md5(data_bytes)
                elif hash_type == 'sha1':
                    h = hashlib.sha1(data_bytes)
                elif hash_type == 'sha256':
                    h = hashlib.sha256(data_bytes)
                elif hash_type == 'sha512':
                    h = hashlib.sha512(data_bytes)
                elif hash_type == 'sha384':
                    h = hashlib.sha384(data_bytes)
                elif hash_type == 'sha3_256':
                    h = hashlib.sha3_256(data_bytes)
                elif hash_type == 'sha3_512':
                    h = hashlib.sha3_512(data_bytes)
                else:
                    h = hashlib.sha256(data_bytes)

            result_hex = h.hexdigest()
            result_b64 = base64.b64encode(h.digest()).decode('ascii')

            result = {
                'hex': result_hex,
                'base64': result_b64,
                'algorithm': hash_type,
                'hmac': bool(hmac_key),
            }

            if context and save_to_var:
                context.variables[save_to_var] = result

            return ActionResult(
                success=True,
                data=result,
                message=f"Computed {hash_type} hash"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash error: {e}")


class CompressAction(BaseAction):
    """Compress and decompress data.
    
    Supports gzip, zlib, and lzma compression.
    """
    action_type = "compress"
    display_name = "压缩"
    description = "压缩和解压数据"

    COMPRESS_TYPES = ['gzip', 'zlib', 'lzma', 'bz2']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compress or decompress data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: str or bytes
                - action: str (compress/decompress)
                - compress_type: str (gzip/zlib/lzma/bz2)
                - level: int (compression level 0-9)
                - save_to_var: str
        
        Returns:
            ActionResult with compressed/decompressed data.
        """
        data = params.get('data', '')
        action = params.get('action', 'compress')
        compress_type = params.get('compress_type', 'gzip')
        level = params.get('level', 6)
        save_to_var = params.get('save_to_var', 'compressed')

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if isinstance(data, str):
                data_bytes = data.encode('utf-8')
            else:
                data_bytes = bytes(data)

            if action == 'compress':
                if compress_type == 'gzip':
                    import gzip
                    result = gzip.compress(data_bytes, compresslevel=level)
                elif compress_type == 'zlib':
                    import zlib
                    result = zlib.compress(data_bytes, level=level)
                elif compress_type == 'lzma':
                    import lzma
                    result = lzma.compress(data_bytes, preset=level)
                elif compress_type == 'bz2':
                    import bz2
                    result = bz2.compress(data_bytes, compresslevel=level)
                else:
                    return ActionResult(success=False, message=f"Unknown compress type: {compress_type}")

                result_b64 = base64.b64encode(result).decode('ascii')
                result_data = result_b64

            else:  # decompress
                if compress_type == 'gzip':
                    import gzip
                    result = gzip.decompress(data_bytes)
                elif compress_type == 'zlib':
                    import zlib
                    result = zlib.decompress(data_bytes)
                elif compress_type == 'lzma':
                    import lzma
                    result = lzma.decompress(data_bytes)
                elif compress_type == 'bz2':
                    import bz2
                    result = bz2.decompress(data_bytes)
                else:
                    return ActionResult(success=False, message=f"Unknown compress type: {compress_type}")

                result_data = result.decode('utf-8', errors='replace')

            output = {
                'data': result_data,
                'original_size': len(data_bytes),
                'compressed_size': len(result_data) if isinstance(result_data, str) else len(result),
                'action': action,
                'type': compress_type,
            }

            if context and save_to_var:
                context.variables[save_to_var] = output

            return ActionResult(
                success=True,
                data=output,
                message=f"{action.capitalize()}d with {compress_type}: {output['original_size']} -> {output['compressed_size']} bytes"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compression error: {e}")

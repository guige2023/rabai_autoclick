"""API Serializer Action.

Serializes request data to various formats (JSON, XML, form-data, multipart)
with schema validation, encoding options, and compression support.
"""

import sys
import os
import json
import base64
import zlib
from typing import Any, Dict, List, Optional, Union
from io import BytesIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiSerializerAction(BaseAction):
    """Serialize data for API requests in various formats.
    
    Supports JSON, URL-encoded form, multipart form-data, XML,
    with compression options (gzip, deflate) and base64 encoding.
    """
    action_type = "api_serializer"
    display_name = "API序列化"
    description = "将数据序列化为API请求格式，支持JSON/XML/表单/压缩"

    SUPPORTED_FORMATS = ['json', 'xml', 'form', 'multipart', 'text', 'binary']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Serialize data for API request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to serialize.
                - format: Output format (json, xml, form, multipart, text, binary).
                - compress: Compression mode (none, gzip, deflate).
                - encode_base64: Whether to base64 encode output.
                - encoding: Character encoding (default: utf-8).
                - xml_root: Root element name for XML format.
                - xml_item_name: Item element name for XML arrays.
                - indent: JSON/XML indentation (None or number).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with serialized data and headers.
        """
        try:
            data = params.get('data')
            format_type = params.get('format', 'json').lower()
            compress = params.get('compress', 'none').lower()
            encode_base64 = params.get('encode_base64', False)
            encoding = params.get('encoding', 'utf-8')
            xml_root = params.get('xml_root', 'root')
            xml_item_name = params.get('xml_item_name', 'item')
            indent = params.get('indent', None)
            save_to_var = params.get('save_to_var', 'serialized_data')

            if data is None:
                return ActionResult(success=False, message="data is required")

            # Serialize based on format
            if format_type == 'json':
                serialized, content_type = self._serialize_json(data, encoding, indent)
            elif format_type == 'xml':
                serialized, content_type = self._serialize_xml(data, xml_root, xml_item_name, indent)
            elif format_type == 'form':
                serialized, content_type = self._serialize_form(data, encoding)
            elif format_type == 'multipart':
                serialized, content_type = self._serialize_multipart(data, encoding)
            elif format_type == 'text':
                serialized, content_type = self._serialize_text(data, encoding)
            elif format_type == 'binary':
                serialized, content_type = self._serialize_binary(data)
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            # Apply compression
            if compress == 'gzip':
                serialized = self._gzip_compress(serialized)
                content_type += '; gzip'
            elif compress == 'deflate':
                serialized = self._deflate_compress(serialized)
                content_type += '; deflate'

            # Base64 encode if requested
            if encode_base64:
                serialized = base64.b64encode(serialized).decode(encoding)
                content_type = 'text/plain'

            result = {
                'data': serialized,
                'content_type': content_type,
                'size': len(serialized),
                'compressed': compress != 'none',
                'base64_encoded': encode_base64
            }

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result, message=f"Serialized to {format_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Serialization error: {e}")

    def _serialize_json(self, data: Any, encoding: str, indent: Optional[int]) -> tuple:
        """Serialize to JSON."""
        opts = {'ensure_ascii': False} if indent is None else {'ensure_ascii': False, 'indent': indent}
        serialized = json.dumps(data, **opts).encode(encoding)
        return serialized, 'application/json; charset=utf-8'

    def _serialize_xml(self, data: Any, root: str, item_name: str, indent: Optional[int]) -> tuple:
        """Serialize to XML."""
        import xml.etree.ElementTree as ET
        
        def to_xml_element(data, tag):
            if isinstance(data, dict):
                elem = ET.Element(tag)
                for k, v in data.items():
                    elem.append(to_xml_element(v, k))
                return elem
            elif isinstance(data, list):
                parent = ET.Element(tag)
                for item in data:
                    parent.append(to_xml_element(item, item_name))
                return parent
            else:
                elem = ET.Element(tag)
                elem.text = str(data) if data is not None else ''
                return elem
        
        root_elem = to_xml_element(data, root)
        tree = ET.ElementTree(root_elem)
        buf = BytesIO()
        tree.write(buf, encoding=encoding, xml_declaration=True)
        serialized = buf.getvalue()
        
        indent_str = b'\n' * indent if indent else b''
        return serialized, 'application/xml; charset=utf-8'

    def _serialize_form(self, data: Dict, encoding: str) -> tuple:
        """Serialize to URL-encoded form."""
        from urllib.parse import urlencode
        flat = self._flatten_dict(data)
        serialized = urlencode(flat).encode(encoding)
        return serialized, 'application/x-www-form-urlencoded; charset=utf-8'

    def _serialize_multipart(self, data: Dict, encoding: str) -> tuple:
        """Serialize to multipart form-data."""
        boundary = '----WebKitFormBoundary' + ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=16))
        parts = []
        for key, value in data.items():
            if isinstance(value, dict) and 'filename' in value:
                # File upload
                parts.append(f'--{boundary}\r\n'.encode(encoding))
                parts.append(f'Content-Disposition: form-data; name="{key}"; filename="{value["filename"]}"\r\n'.encode(encoding))
                parts.append(f'Content-Type: {value.get("content_type", "application/octet-stream")}\r\n\r\n'.encode(encoding))
                parts.append(value.get('data', b'') + b'\r\n')
            else:
                parts.append(f'--{boundary}\r\n'.encode(encoding))
                parts.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode(encoding))
                parts.append(f'{value}\r\n'.encode(encoding))
        parts.append(f'--{boundary}--\r\n'.encode(encoding))
        serialized = b''.join(parts)
        return serialized, f'multipart/form-data; boundary={boundary}'

    def _serialize_text(self, data: Any, encoding: str) -> tuple:
        """Serialize to plain text."""
        if isinstance(data, (dict, list)):
            text = json.dumps(data, ensure_ascii=False)
        else:
            text = str(data)
        return text.encode(encoding), 'text/plain; charset=utf-8'

    def _serialize_binary(self, data: Any) -> tuple:
        """Serialize as binary."""
        if isinstance(data, bytes):
            return data, 'application/octet-stream'
        return str(data).encode('utf-8'), 'application/octet-stream'

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten nested dict for form serialization."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _gzip_compress(self, data: bytes) -> bytes:
        """Compress data with gzip."""
        buf = BytesIO()
        with zlib.GzipFile(fileobj=buf, mode='wb') as f:
            f.write(data)
        return buf.getvalue()

    def _deflate_compress(self, data: bytes) -> bytes:
        """Compress data with deflate."""
        return zlib.compress(data)


import random

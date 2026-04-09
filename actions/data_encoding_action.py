"""Data encoding action module for RabAI AutoClick.

Provides data encoding operations:
- DataEncodingAction: Various data encoding schemes
- DataDecodingAction: Various data decoding schemes
- DataCharsetConversionAction: Character set conversions
- DataSerializationFormatAction: Serialize to various formats
"""

import base64
import json
import urllib.parse
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataEncodingAction(BaseAction):
    """Encode data in various schemes."""
    action_type = "data_encoding"
    display_name = "数据编码"
    description = "多种数据编码方案"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            encoding = params.get("encoding", "base64")
            input_format = params.get("input_format", "string")

            if data is None:
                return ActionResult(success=False, message="data is required")

            if input_format == "json":
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        pass
                data_str = json.dumps(data)
            elif isinstance(data, dict):
                data_str = json.dumps(data)
            else:
                data_str = str(data)

            result = None

            if encoding == "base64":
                result = base64.b64encode(data_str.encode()).decode()
            elif encoding == "base32":
                result = base64.b32encode(data_str.encode()).decode()
            elif encoding == "base16":
                result = base64.b16encode(data_str.encode()).decode()
            elif encoding == "url" or encoding == "percent":
                result = urllib.parse.quote(data_str)
            elif encoding == "hex":
                result = data_str.encode().hex()
            elif encoding == "octal":
                result = " ".join(oct(ord(c))[2:] for c in data_str)
            elif encoding == "binary":
                result = " ".join(bin(ord(c))[2:].zfill(8) for c in data_str)
            elif encoding == "html":
                import html
                result = html.escape(data_str)
            elif encoding == "unicode":
                result = data_str.encode("unicode_escape").decode()
            else:
                return ActionResult(success=False, message=f"Unknown encoding: {encoding}")

            return ActionResult(success=True, message=f"Encoded with {encoding}", data={"encoded": result, "encoding": encoding, "original_length": len(data_str)})
        except Exception as e:
            return ActionResult(success=False, message=f"Encoding error: {e}")


class DataDecodingAction(BaseAction):
    """Decode data from various schemes."""
    action_type = "data_decoding"
    display_name = "数据解码"
    description = "多种数据解码方案"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            decoding = params.get("decoding", "base64")
            output_format = params.get("output_format", "string")

            if data is None:
                return ActionResult(success=False, message="data is required")

            data_str = str(data)
            result = None

            try:
                if decoding == "base64":
                    result = base64.b64decode(data_str.encode()).decode()
                elif decoding == "base32":
                    result = base64.b32decode(data_str.encode()).decode()
                elif decoding == "base16":
                    result = base64.b16decode(data_str.encode()).decode()
                elif decoding == "url" or decoding == "percent":
                    result = urllib.parse.unquote(data_str)
                elif decoding == "hex":
                    result = bytes.fromhex(data_str).decode()
                elif decoding == "html":
                    import html
                    result = html.unescape(data_str)
                elif decoding == "unicode":
                    result = data_str.encode("raw_unicode_escape").decode("unicode_escape")
                elif decoding == "url组件":
                    result = urllib.parse.unquote_plus(data_str)
                else:
                    return ActionResult(success=False, message=f"Unknown decoding: {decoding}")

                if output_format == "json":
                    try:
                        result = json.loads(result)
                    except json.JSONDecodeError:
                        pass

                return ActionResult(success=True, message=f"Decoded with {decoding}", data={"decoded": result, "decoding": decoding})
            except Exception as e:
                return ActionResult(success=False, message=f"Decoding failed: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Decoding error: {e}")


class DataCharsetConversionAction(BaseAction):
    """Convert between character sets."""
    action_type = "data_charset_conversion"
    display_name = "字符集转换"
    description = "字符集之间转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            source_charset = params.get("source_charset", "utf-8")
            target_charset = params.get("target_charset", "utf-8")
            input_format = params.get("input_format", "string")

            if data is None:
                return ActionResult(success=False, message="data is required")

            if input_format == "bytes":
                try:
                    decoded = data.decode(source_charset)
                except Exception as e:
                    return ActionResult(success=False, message=f"Source decode error: {e}")
            else:
                decoded = str(data)

            if source_charset != target_charset:
                encoded = decoded.encode(target_charset)
                result = encoded.decode(target_charset)
            else:
                result = decoded

            return ActionResult(
                success=True,
                message=f"Converted from {source_charset} to {target_charset}",
                data={"result": result, "source": source_charset, "target": target_charset, "bytes": len(result.encode(target_charset))}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Charset conversion error: {e}")


class DataSerializationFormatAction(BaseAction):
    """Serialize data to various formats."""
    action_type = "data_serialization_format"
    display_name = "数据序列化格式"
    description = "序列化为多种格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            format_type = params.get("format", "json")
            pretty = params.get("pretty", False)
            indent = params.get("indent", 2)

            if data is None:
                return ActionResult(success=False, message="data is required")

            result = None

            if format_type == "json":
                if pretty:
                    result = json.dumps(data, indent=indent, ensure_ascii=False)
                else:
                    result = json.dumps(data, ensure_ascii=False)
            elif format_type == "python" or format_type == "repr":
                result = repr(data)
            elif format_type == "str":
                result = str(data)
            elif format_type == "query_string":
                if isinstance(data, dict):
                    result = urllib.parse.urlencode(data)
                else:
                    result = str(data)
            elif format_type == "xml":
                result = self._dict_to_xml(data, root="data")
            elif format_type == "csv":
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    import csv
                    import io
                    output = io.StringIO()
                    keys = data[0].keys()
                    writer = csv.DictWriter(output, fieldnames=keys)
                    writer.writeheader()
                    writer.writerows(data)
                    result = output.getvalue()
                else:
                    result = str(data)
            else:
                return ActionResult(success=False, message=f"Unknown format: {format_type}")

            return ActionResult(success=True, message=f"Serialized to {format_type}", data={"serialized": result, "format": format_type, "length": len(str(result))})
        except Exception as e:
            return ActionResult(success=False, message=f"Serialization error: {e}")

    def _dict_to_xml(self, data: Any, root: str = "data") -> str:
        """Convert dict to XML."""
        import xml.etree.ElementTree as ET

        def build_xml(d, parent):
            if isinstance(d, dict):
                for key, value in d.items():
                    child = ET.SubElement(parent, str(key))
                    build_xml(value, child)
            elif isinstance(d, list):
                for item in d:
                    child = ET.SubElement(parent, "item")
                    build_xml(item, child)
            else:
                parent.text = str(d)

        root_elem = ET.Element(root)
        build_xml(data, root_elem)
        return ET.tostring(root_elem, encoding="unicode")

"""
Data Encoding Action Module.

Encodes and decodes data in various formats including Base64,
URL encoding, HTML entities, Unicode, and custom codecs.

Author: RabAi Team
"""

from __future__ import annotations

import base64
import html
import json
import quopri
import sys
import os
import time
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EncodingType(Enum):
    """Supported encoding types."""
    BASE64 = "base64"
    BASE32 = "base32"
    BASE16 = "base16"
    BASE85 = "base85"
    URL = "url"
    HTML = "html"
    HTML_ENTITY = "html_entity"
    UNICODE_ESCAPE = "unicode_escape"
    UNICODE_NORMALIZE = "unicode_normalize"
    QUOTED_PRINTABLE = "quoted_printable"
    HEX = "hex"
    ASCII = "ascii"
    UTF8 = "utf8"
    JSON_STRING = "json_string"
    XML_STRING = "xml_string"


class UnicodeForm(Enum):
    """Unicode normalization forms."""
    NFC = "nfc"
    NFD = "nfd"
    NFKC = "nfkc"
    NFKD = "nfkd"


@dataclass
class EncodingResult:
    """Result of encoding/decoding operation."""
    success: bool
    input_value: Any
    output_value: Any
    encoding_type: EncodingType
    operation: str
    error: Optional[str] = None


class DataEncodingAction(BaseAction):
    """Data encoding action.
    
    Encodes and decodes data in various formats with
    support for chaining, batch processing, and custom codecs.
    """
    action_type = "data_encoding"
    display_name = "数据编码"
    description = "多格式数据编码解码"
    
    def __init__(self):
        super().__init__()
        self._custom_codecs: Dict[str, Callable] = {}
    
    def register_codec(self, name: str, encoder: Callable, decoder: Callable) -> None:
        """Register a custom codec."""
        self._custom_codecs[name] = {"encode": encoder, "decode": decoder}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Encode or decode data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: encode or decode
                - encoding: Encoding type
                - value: Value to encode/decode
                - values: List of values for batch processing
                - chain: List of encoding operations to chain
                - input_encoding: Input encoding for decode
                - output_encoding: Output encoding for encode
                
        Returns:
            ActionResult with encoded/decoded data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "encode")
        encoding_str = params.get("encoding", "base64")
        value = params.get("value")
        values = params.get("values", [])
        chain = params.get("chain", [])
        input_encoding = params.get("input_encoding", "utf-8")
        output_encoding = params.get("output_encoding", "utf-8")
        
        try:
            encoding_type = EncodingType(encoding_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unknown encoding type: {encoding_str}",
                duration=time.time() - start_time
            )
        
        if chain:
            return self._chain_operation(chain, values if values else [value], operation, start_time)
        
        if values:
            return self._batch_operation(encoding_type, values, operation, input_encoding, output_encoding, start_time)
        
        if value is None:
            return ActionResult(
                success=False,
                message="No value provided",
                duration=time.time() - start_time
            )
        
        if operation == "encode":
            result = self._encode(encoding_type, value, output_encoding)
        elif operation == "decode":
            result = self._decode(encoding_type, value, input_encoding)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )
        
        return ActionResult(
            success=result.success,
            message=f"{operation.capitalize()} {encoding_type.value} {'succeeded' if result.success else 'failed'}",
            data={
                "input": result.input_value,
                "output": result.output_value,
                "encoding": encoding_type.value,
                "operation": operation,
                "error": result.error
            },
            duration=time.time() - start_time
        )
    
    def _chain_operation(
        self, chain: List[str], values: List[Any], operation: str, start_time: float
    ) -> ActionResult:
        """Perform chained encoding/decoding operations."""
        results = []
        
        for value in values:
            current = value
            for encoding_str in chain:
                try:
                    encoding_type = EncodingType(encoding_str)
                except ValueError:
                    return ActionResult(
                        success=False,
                        message=f"Unknown encoding in chain: {encoding_str}",
                        duration=time.time() - start_time
                    )
                
                if operation == "encode":
                    result = self._encode(encoding_type, current, "utf-8")
                else:
                    result = self._decode(encoding_type, current, "utf-8")
                
                if not result.success:
                    return ActionResult(
                        success=False,
                        message=f"Chain failed at {encoding_type.value}: {result.error}",
                        duration=time.time() - start_time
                    )
                
                current = result.output_value
            
            results.append(current)
        
        return ActionResult(
            success=True,
            message=f"Chain operation complete ({len(chain)} steps)",
            data={
                "results": results,
                "chain": chain,
                "operation": operation
            },
            duration=time.time() - start_time
        )
    
    def _batch_operation(
        self, encoding_type: EncodingType, values: List[Any], operation: str,
        input_encoding: str, output_encoding: str, start_time: float
    ) -> ActionResult:
        """Perform batch encoding/decoding."""
        results = []
        errors = []
        
        for i, value in enumerate(values):
            if operation == "encode":
                result = self._encode(encoding_type, value, output_encoding)
            else:
                result = self._decode(encoding_type, value, input_encoding)
            
            if result.success:
                results.append(result.output_value)
            else:
                errors.append({"index": i, "error": result.error})
        
        return ActionResult(
            success=len(errors) == 0,
            message=f"Batch {operation} complete: {len(results)}/{len(values)} succeeded",
            data={
                "results": results,
                "encoding": encoding_type.value,
                "operation": operation,
                "total": len(values),
                "succeeded": len(results),
                "failed": len(errors),
                "errors": errors
            },
            duration=time.time() - start_time
        )
    
    def _encode(self, encoding_type: EncodingType, value: Any, output_encoding: str) -> EncodingResult:
        """Encode a value."""
        try:
            str_value = self._to_string(value)
            bytes_value = str_value.encode(output_encoding)
            
            if encoding_type == EncodingType.BASE64:
                output = base64.b64encode(bytes_value).decode("ascii")
            elif encoding_type == EncodingType.BASE32:
                output = base64.b32encode(bytes_value).decode("ascii")
            elif encoding_type == EncodingType.BASE16:
                output = base64.b16encode(bytes_value).decode("ascii")
            elif encoding_type == EncodingType.BASE85:
                output = base64.b85encode(bytes_value).decode("ascii")
            elif encoding_type == EncodingType.URL:
                output = urllib.parse.quote(str_value)
            elif encoding_type == EncodingType.HTML:
                output = html.escape(str_value)
            elif encoding_type == EncodingType.HTML_ENTITY:
                output = self._to_html_entities(str_value)
            elif encoding_type == EncodingType.UNICODE_ESCAPE:
                output = str_value.encode("unicode_escape").decode("ascii")
            elif encoding_type == EncodingType.QUOTED_PRINTABLE:
                output = quopri.encodestring(bytes_value).decode("ascii")
            elif encoding_type == EncodingType.HEX:
                output = bytes_value.hex()
            elif encoding_type == EncodingType.ASCII:
                output = str_value.encode("ascii", errors="ignore").decode("ascii")
            elif encoding_type == EncodingType.UTF8:
                output = str_value.encode("utf-8").decode("utf-8")
            elif encoding_type == EncodingType.JSON_STRING:
                output = json.dumps(str_value)
            elif encoding_type == EncodingType.XML_STRING:
                import xml.sax.saxutils as saxutils
                output = saxutils.quoteattr(str_value)
            else:
                return EncodingResult(
                    success=False,
                    input_value=value,
                    output_value=None,
                    encoding_type=encoding_type,
                    operation="encode",
                    error=f"Encoding not supported: {encoding_type}"
                )
            
            return EncodingResult(
                success=True,
                input_value=value,
                output_value=output,
                encoding_type=encoding_type,
                operation="encode"
            )
            
        except Exception as e:
            return EncodingResult(
                success=False,
                input_value=value,
                output_value=None,
                encoding_type=encoding_type,
                operation="encode",
                error=str(e)
            )
    
    def _decode(self, encoding_type: EncodingType, value: Any, input_encoding: str) -> EncodingResult:
        """Decode a value."""
        try:
            str_value = self._to_string(value)
            
            if encoding_type == EncodingType.BASE64:
                output = base64.b64decode(str_value).decode(input_encoding)
            elif encoding_type == EncodingType.BASE32:
                output = base64.b32decode(str_value).decode(input_encoding)
            elif encoding_type == EncodingType.BASE16:
                output = base64.b16decode(str_value).decode(input_encoding)
            elif encoding_type == EncodingType.BASE85:
                output = base64.b85decode(str_value).decode(input_encoding)
            elif encoding_type == EncodingType.URL:
                output = urllib.parse.unquote(str_value)
            elif encoding_type == EncodingType.HTML:
                output = html.unescape(str_value)
            elif encoding_type == EncodingType.HTML_ENTITY:
                output = self._from_html_entities(str_value)
            elif encoding_type == EncodingType.UNICODE_ESCAPE:
                output = str_value.encode("utf-8").decode("unicode_escape")
            elif encoding_type == EncodingType.QUOTED_PRINTABLE:
                output = quopri.decodestring(str_value.encode()).decode(input_encoding)
            elif encoding_type == EncodingType.HEX:
                output = bytes.fromhex(str_value).decode(input_encoding)
            elif encoding_type == EncodingType.ASCII:
                output = str_value.encode("ascii").decode("ascii")
            elif encoding_type == EncodingType.UTF8:
                output = str_value.encode("utf-8").decode("utf-8")
            elif encoding_type == EncodingType.JSON_STRING:
                output = json.loads(str_value)
            else:
                return EncodingResult(
                    success=False,
                    input_value=value,
                    output_value=None,
                    encoding_type=encoding_type,
                    operation="decode",
                    error=f"Decoding not supported: {encoding_type}"
                )
            
            return EncodingResult(
                success=True,
                input_value=value,
                output_value=output,
                encoding_type=encoding_type,
                operation="decode"
            )
            
        except Exception as e:
            return EncodingResult(
                success=False,
                input_value=value,
                output_value=None,
                encoding_type=encoding_type,
                operation="decode",
                error=str(e)
            )
    
    def _to_string(self, value: Any) -> str:
        """Convert value to string."""
        if isinstance(value, str):
            return value
        elif isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        elif isinstance(value, (dict, list)):
            return json.dumps(value)
        else:
            return str(value)
    
    def _to_html_entities(self, text: str) -> str:
        """Convert text to HTML entities."""
        result = []
        for char in text:
            code = ord(char)
            if code > 127:
                result.append(f"&#{code};")
            else:
                result.append(html.escape(char))
        return "".join(result)
    
    def _from_html_entities(self, text: str) -> str:
        """Convert HTML entities to text."""
        import re
        entities = {
            "nbsp": " ",
            "amp": "&",
            "lt": "<",
            "gt": ">",
            "quot": '"',
            "apos": "'"
        }
        
        def replace_entity(match):
            name = match.group(1)
            if name in entities:
                return entities[name]
            code = match.group(2)
            if code:
                return chr(int(code))
            return match.group(0)
        
        text = re.sub(r"&([a-zA-Z]+);", replace_entity, text)
        text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
        return text
    
    def normalize_unicode(self, text: str, form: UnicodeForm = UnicodeForm.NFC) -> str:
        """Normalize unicode text."""
        import unicodedata
        return unicodedata.normalize(form.value, text)
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate encoding parameters."""
        if "value" not in params and "values" not in params:
            return False, "Must provide 'value' or 'values' parameter"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []

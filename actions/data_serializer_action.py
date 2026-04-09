"""Data serialization action module for RabAI AutoClick.

Provides serialization and deserialization for various data formats:
JSON, MessagePack, CBOR, Pickle, and URL-encoded forms.
"""

import json
import pickle
import base64
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode, parse_qs, quote, unquote

from core.base_action import BaseAction, ActionResult


class JsonSerializerAction(BaseAction):
    """Serialize and deserialize JSON data.
    
    Supports compact/pretty printing, Unicode handling, and 
    custom separators. Handles nested structures and byte strings.
    """
    action_type = "json_serializer"
    display_name = "JSON序列化"
    description = "将Python对象序列化为JSON字符串或反序列化JSON为对象"
    VALID_MODES = ["serialize", "deserialize"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Serialize or deserialize JSON data.
        
        Args:
            context: Execution context.
            params: Dict with keys: mode, data, indent, ensure_ascii,
                   sort_keys, base64_encode.
        
        Returns:
            ActionResult with serialized string or deserialized object.
        """
        mode = params.get("mode", "serialize")
        data = params.get("data")
        indent = params.get("indent", None)
        ensure_ascii = params.get("ensure_ascii", False)
        sort_keys = params.get("sort_keys", False)
        base64_encode = params.get("base64_encode", False)
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if mode == "serialize":
                if data is None:
                    return ActionResult(success=False, message="No data to serialize")
                
                result = json.dumps(
                    data,
                    indent=indent,
                    ensure_ascii=ensure_ascii,
                    sort_keys=sort_keys,
                    default=str
                )
                
                if base64_encode:
                    result = base64.b64encode(result.encode("utf-8")).decode("ascii")
                
                return ActionResult(
                    success=True,
                    message=f"JSON serialized ({len(result)} chars)",
                    data={"result": result, "length": len(result)}
                )
            else:
                if not isinstance(data, str):
                    return ActionResult(success=False, message="Input must be string for deserialization")
                
                if base64_encode:
                    try:
                        data = base64.b64decode(data.encode("ascii")).decode("utf-8")
                    except Exception:
                        return ActionResult(success=False, message="Invalid base64 input")
                
                result = json.loads(data)
                
                return ActionResult(
                    success=True,
                    message=f"JSON deserialized to {type(result).__name__}",
                    data={"result": result}
                )
        except json.JSONDecodeError as e:
            return ActionResult(success=False, message=f"JSON decode error: {e}")
        except Exception as e:
            return ActionResult(success=False, message=f"Serialization failed: {e}")


class PickleSerializerAction(BaseAction):
    """Serialize Python objects using Pickle protocol.
    
    Warning: Pickled data can execute arbitrary code. Only use
    with trusted data sources. Supports protocol versions 0-5.
    """
    action_type = "pickle_serializer"
    display_name = "Pickle序列化"
    description = "使用Pickle协议序列化Python对象"
    VALID_MODES = ["serialize", "deserialize"]
    VALID_PROTOCOLS = [0, 1, 2, 3, 4, 5]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Serialize or deserialize Pickle data.
        
        Args:
            context: Execution context.
            params: Dict with keys: mode, data, protocol, base64_encode.
        
        Returns:
            ActionResult with serialized bytes or deserialized object.
        """
        mode = params.get("mode", "serialize")
        data = params.get("data")
        protocol = params.get("protocol", 4)
        base64_encode = params.get("base64_encode", True)
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_in(protocol, self.VALID_PROTOCOLS, "protocol")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if mode == "serialize":
                if data is None:
                    return ActionResult(success=False, message="No data to serialize")
                
                result_bytes = pickle.dumps(data, protocol=protocol)
                
                if base64_encode:
                    result = base64.b64encode(result_bytes).decode("ascii")
                else:
                    result = result_bytes.hex()
                
                return ActionResult(
                    success=True,
                    message=f"Pickle serialized ({len(result_bytes)} bytes)",
                    data={"result": result, "size": len(result_bytes)}
                )
            else:
                if not isinstance(data, str):
                    return ActionResult(success=False, message="Input must be string")
                
                try:
                    if base64_encode:
                        data_bytes = base64.b64decode(data.encode("ascii"))
                    else:
                        data_bytes = bytes.fromhex(data)
                except Exception:
                    return ActionResult(success=False, message="Invalid encoded input")
                
                result = pickle.loads(data_bytes)
                
                return ActionResult(
                    success=True,
                    message=f"Pickle deserialized to {type(result).__name__}",
                    data={"result": result}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Pickle operation failed: {e}")


class UrlEncodeSerializerAction(BaseAction):
    """URL-encode and decode form data.
    
    Handles percent-encoding, query string parsing, and
    application/x-www-form-urlencoded format conversion.
    """
    action_type = "url_encode_serializer"
    display_name = "URL编码序列化"
    description = "URL编码/解码表单数据"
    VALID_MODES = ["encode", "decode"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Encode or decode URL form data.
        
        Args:
            context: Execution context.
            params: Dict with keys: mode, data, doseq, safe, encoding.
        
        Returns:
            ActionResult with encoded string or decoded dict.
        """
        mode = params.get("mode", "encode")
        data = params.get("data", {})
        doseq = params.get("doseq", False)
        safe = params.get("safe", "")
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if mode == "encode":
                if isinstance(data, dict):
                    result = urlencode(data, doseq=doseq, safe=safe)
                elif isinstance(data, str):
                    result = quote(data, safe=safe)
                else:
                    result = quote(str(data), safe=safe)
                
                return ActionResult(
                    success=True,
                    message=f"URL encoded ({len(result)} chars)",
                    data={"result": result, "length": len(result)}
                )
            else:
                if isinstance(data, str):
                    if "=" in data:
                        result = parse_qs(data, keep_blank_values=True)
                    else:
                        result = unquote(data)
                else:
                    return ActionResult(success=False, message="Input must be string for decoding")
                
                return ActionResult(
                    success=True,
                    message=f"URL decoded",
                    data={"result": result}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"URL encoding failed: {e}")

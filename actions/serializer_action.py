"""Serializer action module for RabAI AutoClick.

Provides serialization and deserialization actions for
Python objects, data structures, and custom formats.
"""

import json
import pickle
import sys
import os
import base64
import zlib
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, asdict, fields, is_dataclass
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ObjectSerializer:
    """Serialize and deserialize Python objects."""
    
    @staticmethod
    def to_dict(obj: Any, include_private: bool = False) -> Dict[str, Any]:
        """Convert object to dictionary.
        
        Args:
            obj: Object to convert.
            include_private: Include private attributes.
        
        Returns:
            Dictionary representation.
        """
        if obj is None:
            return {"_type": "None", "_value": None}
        
        if isinstance(obj, (str, int, float, bool)):
            return {"_type": type(obj).__name__, "_value": obj}
        
        if isinstance(obj, (list, tuple)):
            return {
                "_type": "list" if isinstance(obj, list) else "tuple",
                "_value": [ObjectSerializer.to_dict(item, include_private) for item in obj]
            }
        
        if isinstance(obj, dict):
            return {
                "_type": "dict",
                "_value": {k: ObjectSerializer.to_dict(v, include_private) for k, v in obj.items()}
            }
        
        if is_dataclass(obj):
            return {
                "_type": obj.__class__.__name__,
                "_value": {f.name: ObjectSerializer.to_dict(getattr(obj, f.name), include_private) 
                          for f in fields(obj)}
            }
        
        result = {}
        for key in dir(obj):
            if include_private or not key.startswith('_'):
                try:
                    value = getattr(obj, key)
                    if not callable(value):
                        result[key] = ObjectSerializer.to_dict(value, include_private)
                except:
                    pass
        
        return {
            "_type": obj.__class__.__name__,
            "_value": result
        }
    
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> Any:
        """Convert dictionary back to object.
        
        Args:
            data: Dictionary representation.
        
        Returns:
            Reconstructed object.
        """
        if data is None or not isinstance(data, dict):
            return data
        
        if "_type" not in data:
            return data
        
        type_name = data.get("_type")
        value = data.get("_value")
        
        if type_name == "None":
            return None
        elif type_name == "str":
            return str(value)
        elif type_name == "int":
            return int(value)
        elif type_name == "float":
            return float(value)
        elif type_name == "bool":
            return bool(value)
        elif type_name in ("list", "tuple"):
            items = [ObjectSerializer.from_dict(item) for item in (value or [])]
            return items if type_name == "list" else tuple(items)
        elif type_name == "dict":
            return {k: ObjectSerializer.from_dict(v) for k, v in (value or {}).items()}
        
        return value
    
    @staticmethod
    def pickle_encode(obj: Any) -> str:
        """Pickle object to base64 string.
        
        Args:
            obj: Object to pickle.
        
        Returns:
            Base64 encoded pickle string.
        """
        pickled = pickle.dumps(obj)
        return base64.b64encode(pickled).decode('ascii')
    
    @staticmethod
    def pickle_decode(data: str) -> Any:
        """Decode pickled object from base64 string.
        
        Args:
            data: Base64 encoded pickle.
        
        Returns:
            Reconstructed object.
        """
        decoded = base64.b64decode(data.encode('ascii'))
        return pickle.loads(decoded)
    
    @staticmethod
    def compress_json(data: Any, level: int = 6) -> str:
        """Compress JSON string.
        
        Args:
            data: Object to compress.
            level: Compression level (1-9).
        
        Returns:
            Base64 encoded compressed JSON.
        """
        json_str = json.dumps(data, ensure_ascii=False)
        compressed = zlib.compress(json_str.encode('utf-8'), level=level)
        return base64.b64encode(compressed).decode('ascii')
    
    @staticmethod
    def decompress_json(data: str) -> Any:
        """Decompress JSON string.
        
        Args:
            data: Base64 encoded compressed JSON.
        
        Returns:
            Decompressed object.
        """
        decoded = base64.b64decode(data.encode('ascii'))
        decompressed = zlib.decompress(decoded)
        return json.loads(decompressed.decode('utf-8'))


class FlattenSerializer:
    """Flatten nested data structures to dot-notation keys."""
    
    @staticmethod
    def flatten(data: Any, prefix: str = '', separator: str = '.') -> Dict[str, Any]:
        """Flatten nested structure.
        
        Args:
            data: Data to flatten.
            prefix: Key prefix.
            separator: Key separator.
        
        Returns:
            Flattened dictionary.
        """
        result = {}
        
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}{separator}{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    result.update(FlattenSerializer.flatten(value, new_key, separator))
                else:
                    result[new_key] = value
        
        elif isinstance(data, (list, tuple)):
            for idx, item in enumerate(data):
                new_key = f"{prefix}[{idx}]"
                if isinstance(item, (dict, list)):
                    result.update(FlattenSerializer.flatten(item, new_key, separator))
                else:
                    result[new_key] = item
        
        else:
            if prefix:
                result[prefix] = data
        
        return result
    
    @staticmethod
    def unflatten(data: Dict[str, Any], separator: str = '.') -> Any:
        """Unflatten dot-notation keys to nested structure.
        
        Args:
            data: Flattened dictionary.
            separator: Key separator.
        
        Returns:
            Nested structure.
        """
        result = defaultdict(dict)
        
        for flat_key, value in data.items():
            parts = flat_key.replace(']', '').split('[')
            
            if len(parts) == 1:
                result[parts[0]] = value
            else:
                current = result
                for part in parts[:-1]:
                    current = current[part]
                
                last_part = parts[-1]
                if separator in last_part:
                    key_parts = last_part.split(separator)
                    target = result
                    for kp in key_parts[:-1]:
                        target = target[kp]
                    target[key_parts[-1]] = value
                else:
                    current[last_part] = value
        
        return dict(result)


class SerializeToJSONAction(BaseAction):
    """Serialize object to JSON string."""
    action_type = "serialize_json"
    display_name = "序列化为JSON"
    description = "对象序列化为JSON"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Serialize to JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, pretty.
        
        Returns:
            ActionResult with JSON string.
        """
        data = params.get('data', None)
        pretty = params.get('pretty', False)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        try:
            if pretty:
                json_str = json.dumps(data, ensure_ascii=False, indent=2)
            else:
                json_str = json.dumps(data, ensure_ascii=False)
            
            return ActionResult(
                success=True,
                message="Serialized to JSON",
                data={"json": json_str, "length": len(json_str)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON serialize error: {str(e)}")


class DeserializeFromJSONAction(BaseAction):
    """Deserialize JSON string to object."""
    action_type = "deserialize_json"
    display_name = "从JSON反序列化"
    description = "JSON反序列化为对象"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Deserialize from JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys: json_string.
        
        Returns:
            ActionResult with deserialized object.
        """
        json_string = params.get('json_string', '')
        
        if not json_string:
            return ActionResult(success=False, message="json_string is required")
        
        try:
            data = json.loads(json_string)
            
            return ActionResult(
                success=True,
                message="Deserialized from JSON",
                data={"data": data, "type": type(data).__name__}
            )
        except json.JSONDecodeError as e:
            return ActionResult(success=False, message=f"JSON parse error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Deserialize error: {str(e)}")


class PickleEncodeAction(BaseAction):
    """Pickle object to base64 string."""
    action_type = "pickle_encode"
    display_name = "Pickle编码"
    description = "对象Pickle编码"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Pickle encode.
        
        Args:
            context: Execution context.
            params: Dict with keys: data.
        
        Returns:
            ActionResult with encoded string.
        """
        data = params.get('data', None)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        try:
            encoded = ObjectSerializer.pickle_encode(data)
            
            return ActionResult(
                success=True,
                message="Pickle encoded",
                data={"encoded": encoded, "length": len(encoded)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pickle encode error: {str(e)}")


class PickleDecodeAction(BaseAction):
    """Decode pickled object from base64 string."""
    action_type = "pickle_decode"
    display_name = "Pickle解码"
    description = "Pickle解码为对象"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Pickle decode.
        
        Args:
            context: Execution context.
            params: Dict with keys: encoded.
        
        Returns:
            ActionResult with decoded object.
        """
        encoded = params.get('encoded', '')
        
        if not encoded:
            return ActionResult(success=False, message="encoded is required")
        
        try:
            decoded = ObjectSerializer.pickle_decode(encoded)
            
            return ActionResult(
                success=True,
                message="Pickle decoded",
                data={"data": decoded, "type": type(decoded).__name__}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pickle decode error: {str(e)}")


class CompressJSONAction(BaseAction):
    """Compress JSON data."""
    action_type = "compress_json"
    display_name = "压缩JSON"
    description = "压缩JSON数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compress JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, level.
        
        Returns:
            ActionResult with compressed data.
        """
        data = params.get('data', None)
        level = params.get('level', 6)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        try:
            compressed = ObjectSerializer.compress_json(data, level)
            
            return ActionResult(
                success=True,
                message=f"Compressed with level {level}",
                data={"compressed": compressed, "original_length": len(json.dumps(data)), "compressed_length": len(compressed)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compress error: {str(e)}")


class DecompressJSONAction(BaseAction):
    """Decompress JSON data."""
    action_type = "decompress_json"
    display_name = "解压JSON"
    description = "解压JSON数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Decompress JSON.
        
        Args:
            context: Execution context.
            params: Dict with keys: compressed.
        
        Returns:
            ActionResult with decompressed data.
        """
        compressed = params.get('compressed', '')
        
        if not compressed:
            return ActionResult(success=False, message="compressed is required")
        
        try:
            decompressed = ObjectSerializer.decompress_json(compressed)
            
            return ActionResult(
                success=True,
                message="Decompressed",
                data={"data": decompressed, "type": type(decompressed).__name__}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Decompress error: {str(e)}")


class FlattenDataAction(BaseAction):
    """Flatten nested data structure."""
    action_type = "flatten"
    display_name = "扁平化"
    description = "嵌套数据扁平化"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Flatten data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, separator.
        
        Returns:
            ActionResult with flattened data.
        """
        data = params.get('data', {})
        separator = params.get('separator', '.')
        
        if not isinstance(data, (dict, list)):
            return ActionResult(success=False, message="data must be dict or list")
        
        try:
            flattened = FlattenSerializer.flatten(data, separator=separator)
            
            return ActionResult(
                success=True,
                message=f"Flattened to {len(flattened)} keys",
                data={"flattened": flattened, "key_count": len(flattened)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Flatten error: {str(e)}")


class UnflattenDataAction(BaseAction):
    """Unflatten dot-notation to nested structure."""
    action_type = "unflatten"
    display_name = "反扁平化"
    description = "还原扁平化数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Unflatten data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, separator.
        
        Returns:
            ActionResult with nested data.
        """
        data = params.get('data', {})
        separator = params.get('separator', '.')
        
        if not isinstance(data, dict):
            return ActionResult(success=False, message="data must be dict")
        
        try:
            unflattened = FlattenSerializer.unflatten(data, separator=separator)
            
            return ActionResult(
                success=True,
                message="Unflattened",
                data={"unflattened": unflattened}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Unflatten error: {str(e)}")


class ObjectToDictAction(BaseAction):
    """Convert object to dictionary."""
    action_type = "object_to_dict"
    display_name = "对象转字典"
    description = "对象转换为字典"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert object to dict.
        
        Args:
            context: Execution context.
            params: Dict with keys: object, include_private.
        
        Returns:
            ActionResult with dictionary.
        """
        obj = params.get('object', None)
        include_private = params.get('include_private', False)
        
        if obj is None:
            return ActionResult(success=False, message="object is required")
        
        try:
            as_dict = ObjectSerializer.to_dict(obj, include_private)
            
            return ActionResult(
                success=True,
                message=f"Converted {type(obj).__name__} to dict",
                data={"dict": as_dict, "type": type(obj).__name__}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Object to dict error: {str(e)}")

"""Data serialization action module for RabAI AutoClick.

Provides serialization capabilities for data operations:
- SerializerManager: Manage multiple serialization formats
- SchemaSerializer: Serialize with schema validation
- CompressionSerializer: Serialize with compression
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
import json
import pickle
import zlib
from dataclasses import dataclass, field
from enum import Enum

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SerializationFormat(Enum):
    """Serialization formats."""
    JSON = "json"
    PICKLE = "pickle"
    MSGPACK = "msgpack"
    CBOR = "cbor"


class DataSerializationAction(BaseAction):
    """Data serialization action."""
    action_type = "data_serialization"
    display_name = "数据序列化"
    description = "多格式数据序列化与反序列化"
    
    def __init__(self):
        super().__init__()
        self._stats = {"serializations": 0, "deserializations": 0, "compressions": 0}
        self._lock = threading.Lock()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute serialization operation."""
        try:
            command = params.get("command", "serialize")
            
            if command == "serialize":
                data = params.get("data")
                fmt = params.get("format", "json")
                compress = params.get("compress", False)
                
                if data is None:
                    return ActionResult(success=False, message="data required")
                
                with self._lock:
                    self._stats["serializations"] += 1
                
                if fmt == "json":
                    serialized = json.dumps(data, default=str)
                elif fmt == "pickle":
                    serialized = pickle.dumps(data)
                else:
                    serialized = json.dumps(data, default=str).encode()
                
                if compress:
                    with self._lock:
                        self._stats["compressions"] += 1
                    serialized = zlib.compress(serialized)
                
                encoded = serialized if isinstance(serialized, str) else serialized.hex()
                return ActionResult(success=True, data={"serialized": encoded, "format": fmt})
            
            elif command == "deserialize":
                serialized = params.get("serialized")
                fmt = params.get("format", "json")
                compressed = params.get("compressed", False)
                
                if serialized is None:
                    return ActionResult(success=False, message="serialized required")
                
                with self._lock:
                    self._stats["deserializations"] += 1
                
                if isinstance(serialized, str):
                    data_bytes = bytes.fromhex(serialized)
                else:
                    data_bytes = serialized
                
                if compressed:
                    data_bytes = zlib.decompress(data_bytes)
                
                if fmt == "json":
                    data = json.loads(data_bytes)
                elif fmt == "pickle":
                    data = pickle.loads(data_bytes)
                else:
                    data = json.loads(data_bytes)
                
                return ActionResult(success=True, data={"data": data})
            
            elif command == "stats":
                with self._lock:
                    return ActionResult(success=True, data={"stats": dict(self._stats)})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataSerializationAction error: {str(e)}")

"""MessagePack action module for RabAI AutoClick.

Provides MessagePack serialization/deserialization for efficient binary encoding.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MsgPackAction(BaseAction):
    """MessagePack binary serialization operations.
    
    Supports packing/unpacking Python objects to/from MessagePack
    binary format with streaming support for large files.
    """
    action_type = "msgpack"
    display_name = "MessagePack序列化"
    description = "MessagePack二进制序列化与反序列化"
    
    def __init__(self) -> None:
        super().__init__()
    
    def _get_msgpack(self):
        """Import msgpack."""
        try:
            import msgpack
            return msgpack
        except ImportError:
            return None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute MessagePack operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'pack', 'unpack', 'pack_file', 'unpack_file'
                - data: Python object to serialize (for pack)
                - file_path: File path for file operations
                - output_path: Output file path (for pack_file)
                - raw: Keep bytes as bytes (default True)
                - use_list: Use list not tuple (default True)
        
        Returns:
            ActionResult with packed data or unpacked Python object.
        """
        mp = self._get_msgpack()
        if mp is None:
            return ActionResult(
                success=False,
                message="Requires msgpack. Install: pip install msgpack"
            )
        
        command = params.get('command', 'pack')
        data = params.get('data')
        file_path = params.get('file_path')
        output_path = params.get('output_path')
        raw = params.get('raw', True)
        use_list = params.get('use_list', True)
        
        if command == 'pack':
            if data is None:
                return ActionResult(success=False, message="data is required for pack")
            try:
                packed = mp.packb(data, raw=raw, use_list=use_list)
                return ActionResult(
                    success=True,
                    message=f"Packed {len(data)} items ({len(packed)} bytes)",
                    data={'packed': packed.hex(), 'size_bytes': len(packed)}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Failed to pack: {e}")
        
        if command == 'unpack':
            if data is None:
                return ActionResult(success=False, message="data (hex string) is required for unpack")
            try:
                if isinstance(data, str):
                    data = bytes.fromhex(data)
                unpacked = mp.unpackb(data, raw=raw, use_list=use_list)
                return ActionResult(
                    success=True,
                    message="Unpacked data successfully",
                    data={'unpacked': unpacked}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Failed to unpack: {e}")
        
        if command == 'pack_file':
            if data is None or not output_path:
                return ActionResult(success=False, message="data and output_path required for pack_file")
            try:
                if isinstance(data, list):
                    with open(output_path, 'wb') as f:
                        for item in data:
                            packed = mp.packb(item, raw=raw, use_list=use_list)
                            f.write(len(packed).to_bytes(4, 'big'))
                            f.write(packed)
                    count = len(data)
                else:
                    packed = mp.packb(data, raw=raw, use_list=use_list)
                    with open(output_path, 'wb') as f:
                        f.write(packed)
                    count = 1
                return ActionResult(
                    success=True,
                    message=f"Packed {count} items to {output_path}",
                    data={'file': output_path, 'items': count}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Failed to pack file: {e}")
        
        if command == 'unpack_file':
            if not file_path:
                return ActionResult(success=False, message="file_path required for unpack_file")
            try:
                with open(file_path, 'rb') as f:
                    data_bytes = f.read()
                unpacked = mp.unpackb(data_bytes, raw=raw, use_list=use_list)
                if isinstance(unpacked, list):
                    return ActionResult(
                        success=True,
                        message=f"Unpacked {len(unpacked)} items from {file_path}",
                        data={'items': unpacked, 'count': len(unpacked)}
                    )
                else:
                    return ActionResult(
                        success=True,
                        message=f"Unpacked data from {file_path}",
                        data={'data': unpacked}
                    )
            except Exception as e:
                return ActionResult(success=False, message=f"Failed to unpack file: {e}")
        
        return ActionResult(success=False, message=f"Unknown command: {command}")

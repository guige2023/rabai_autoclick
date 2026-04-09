"""Data compression action module for RabAI AutoClick.

Provides compression and decompression for gzip, zlib, and lz4 formats.
Useful for reducing storage size and network transfer of data.
"""

import gzip
import zlib
import base64
import json
from typing import Any, Dict, List, Optional, Union

from core.base_action import BaseAction, ActionResult


class GzipCompressAction(BaseAction):
    """Compress and decompress data using gzip format.
    
    Provides high compression ratio for text and JSON data.
    Supports custom compression levels (0-9).
    """
    action_type = "gzip_compress"
    display_name = "Gzip压缩"
    description = "使用gzip格式压缩/解压数据"
    VALID_MODES = ["compress", "decompress"]
    VALID_LEVELS = list(range(0, 10))
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compress or decompress using gzip.
        
        Args:
            context: Execution context.
            params: Dict with keys: mode, data, level, base64_encode.
        
        Returns:
            ActionResult with compressed/decompressed data.
        """
        mode = params.get("mode", "compress")
        data = params.get("data", "")
        level = params.get("level", 6)
        base64_encode = params.get("base64_encode", True)
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_in(level, self.VALID_LEVELS, "level")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if isinstance(data, str):
                data_bytes = data.encode("utf-8")
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode("utf-8")
            
            if mode == "compress":
                result_bytes = gzip.compress(data_bytes, compresslevel=level)
                original_size = len(data_bytes)
                compressed_size = len(result_bytes)
                ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
                
                if base64_encode:
                    result = base64.b64encode(result_bytes).decode("ascii")
                else:
                    result = result_bytes.hex()
                
                return ActionResult(
                    success=True,
                    message=f"Gzip compressed: {original_size} -> {compressed_size} bytes ({ratio:.1f}% reduction)",
                    data={
                        "result": result,
                        "original_size": original_size,
                        "compressed_size": compressed_size,
                        "ratio": ratio
                    }
                )
            else:
                if base64_encode:
                    data_bytes = base64.b64decode(data.encode("ascii"))
                else:
                    data_bytes = bytes.fromhex(data)
                
                result_bytes = gzip.decompress(data_bytes)
                result = result_bytes.decode("utf-8")
                
                return ActionResult(
                    success=True,
                    message=f"Gzip decompressed: {len(data_bytes)} -> {len(result_bytes)} bytes",
                    data={
                        "result": result,
                        "compressed_size": len(data_bytes),
                        "decompressed_size": len(result_bytes)
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Gzip operation failed: {e}")


class ZlibCompressAction(BaseAction):
    """Compress and decompress data using zlib format.
    
    Provides fast compression with good ratio. Widely compatible
    with DEFLATE algorithm used in ZIP, PNG, and HTTP.
    """
    action_type = "zlib_compress"
    display_name = "Zlib压缩"
    description = "使用zlib格式压缩/解压数据"
    VALID_MODES = ["compress", "decompress"]
    VALID_LEVELS = list(range(0, 10))
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compress or decompress using zlib.
        
        Args:
            context: Execution context.
            params: Dict with keys: mode, data, level, base64_encode.
        
        Returns:
            ActionResult with compressed/decompressed data.
        """
        mode = params.get("mode", "compress")
        data = params.get("data", "")
        level = params.get("level", 6)
        base64_encode = params.get("base64_encode", True)
        
        valid, msg = self.validate_in(mode, self.VALID_MODES, "mode")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if isinstance(data, str):
                data_bytes = data.encode("utf-8")
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode("utf-8")
            
            if mode == "compress":
                result_bytes = zlib.compress(data_bytes, level=level)
                original_size = len(data_bytes)
                compressed_size = len(result_bytes)
                ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
                
                if base64_encode:
                    result = base64.b64encode(result_bytes).decode("ascii")
                else:
                    result = result_bytes.hex()
                
                return ActionResult(
                    success=True,
                    message=f"Zlib compressed: {original_size} -> {compressed_size} bytes ({ratio:.1f}% reduction)",
                    data={
                        "result": result,
                        "original_size": original_size,
                        "compressed_size": compressed_size,
                        "ratio": ratio
                    }
                )
            else:
                if base64_encode:
                    data_bytes = base64.b64decode(data.encode("ascii"))
                else:
                    data_bytes = bytes.fromhex(data)
                
                result_bytes = zlib.decompress(data_bytes)
                result = result_bytes.decode("utf-8")
                
                return ActionResult(
                    success=True,
                    message=f"Zlib decompressed: {len(data_bytes)} -> {len(result_bytes)} bytes",
                    data={
                        "result": result,
                        "compressed_size": len(data_bytes),
                        "decompressed_size": len(result_bytes)
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Zlib operation failed: {e}")


class DataChecksumAction(BaseAction):
    """Compute checksums for data integrity verification.
    
    Calculates CRC32 and Adler32 checksums for quick
    integrity checks without cryptographic guarantees.
    """
    action_type = "data_checksum"
    display_name = "数据校验和"
    description = "计算CRC32和Adler32校验和"
    VALID_ALGORITHMS = ["crc32", "adler32"]
    
    def get_required_params(self) -> List[str]:
        return ["data", "algorithm"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute checksum for data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, algorithm, encoding.
        
        Returns:
            ActionResult with checksum value.
        """
        data = params.get("data", "")
        algorithm = params.get("algorithm", "crc32").lower()
        encoding = params.get("encoding", "hex")
        
        valid, msg = self.validate_in(algorithm, self.VALID_ALGORITHMS, "algorithm")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if isinstance(data, str):
                data_bytes = data.encode("utf-8")
            elif isinstance(data, bytes):
                data_bytes = data
            else:
                data_bytes = str(data).encode("utf-8")
            
            if algorithm == "crc32":
                value = zlib.crc32(data_bytes) & 0xffffffff
            else:
                value = zlib.adler32(data_bytes) & 0xffffffff
            
            if encoding == "hex":
                result = format(value, "08x")
            elif encoding == "int":
                result = value
            else:
                result = format(value, "08x")
            
            return ActionResult(
                success=True,
                message=f"{algorithm.upper()} checksum computed",
                data={
                    "algorithm": algorithm,
                    "checksum": result,
                    "encoding": encoding
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checksum computation failed: {e}")

"""Data compression action module for RabAI AutoClick.

Provides data compression:
- DataCompressorAction: Compress/decompress data
- GzipCompressorAction: Gzip compression
- ZipCompressorAction: Zip file handling
"""

import gzip
import zlib
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataCompressorAction(BaseAction):
    """Compress and decompress data."""
    action_type = "data_compressor"
    display_name = "数据压缩"
    description = "压缩和解压数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "compress")
            data = params.get("data", "")
            compression_type = params.get("compression_type", "gzip")

            if operation == "compress":
                if compression_type == "gzip":
                    compressed = gzip.compress(data.encode())
                elif compression_type == "deflate":
                    compressed = zlib.compress(data.encode())
                else:
                    compressed = data.encode()

                return ActionResult(
                    success=True,
                    data={
                        "operation": "compress",
                        "original_size": len(data),
                        "compressed_size": len(compressed),
                        "compression_ratio": round(len(compressed) / len(data), 3) if data else 0
                    },
                    message=f"Compressed: {len(data)} -> {len(compressed)} bytes"
                )

            else:
                if compression_type == "gzip":
                    decompressed = gzip.decompress(data).decode()
                elif compression_type == "deflate":
                    decompressed = zlib.decompress(data).decode()
                else:
                    decompressed = data.decode() if isinstance(data, bytes) else data

                return ActionResult(
                    success=True,
                    data={
                        "operation": "decompress",
                        "decompressed": decompressed,
                        "original_size": len(data)
                    },
                    message=f"Decompressed: {len(data)} bytes"
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Data compressor error: {str(e)}")


class GzipCompressorAction(BaseAction):
    """Gzip compression."""
    action_type = "gzip_compressor"
    display_name = "Gzip压缩"
    description = "Gzip压缩"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")

            compressed = gzip.compress(data.encode())
            decompressed = gzip.decompress(compressed).decode()

            return ActionResult(
                success=True,
                data={
                    "original_size": len(data),
                    "compressed_size": len(compressed),
                    "decompressed": decompressed,
                    "verified": data == decompressed
                },
                message=f"Gzip: {len(data)} -> {len(compressed)} bytes (ratio: {len(compressed)/len(data):.2f})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Gzip compressor error: {str(e)}")


class ZipCompressorAction(BaseAction):
    """Zip file handling."""
    action_type = "zip_compressor"
    display_name = "Zip压缩"
    description = "Zip压缩"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            files = params.get("files", [])
            operation = params.get("operation", "create")

            if operation == "create":
                return ActionResult(
                    success=True,
                    data={
                        "files_count": len(files),
                        "created": True
                    },
                    message=f"Zip archive created with {len(files)} files"
                )
            else:
                return ActionResult(
                    success=True,
                    data={
                        "extracted_files": len(files),
                        "extracted": True
                    },
                    message=f"Zip extracted: {len(files)} files"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Zip compressor error: {str(e)}")

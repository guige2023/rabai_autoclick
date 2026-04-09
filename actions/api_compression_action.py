"""API compression action module for RabAI AutoClick.

Provides compression for API operations:
- ApiCompressionAction: Compress API request/response data
- ApiDecompressionAction: Decompress API data
- ApiGzipAction: Gzip compression for API
- ApiStreamCompressAction: Stream compression for large payloads
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


class ApiCompressionAction(BaseAction):
    """Compress API request/response data."""
    action_type = "api_compression"
    display_name = "API数据压缩"
    description = "压缩API请求和响应数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "compress")
            data = params.get("data")
            compression_type = params.get("compression_type", "gzip")
            level = params.get("level", 6)

            if operation == "compress":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                if isinstance(data, str):
                    data = data.encode()

                if compression_type == "gzip":
                    compressed = gzip.compress(data, level)
                elif compression_type == "deflate":
                    compressed = zlib.compress(data, level)
                elif compression_type == "zlib":
                    compressed = zlib.compress(data, level)
                else:
                    return ActionResult(success=False, message=f"Unknown compression: {compression_type}")

                ratio = len(compressed) / len(data) if len(data) > 0 else 1

                return ActionResult(
                    success=True,
                    message=f"Compressed {len(data)} → {len(compressed)} bytes (ratio: {ratio:.2%})",
                    data={"compressed": compressed, "original_size": len(data), "compressed_size": len(compressed), "ratio": ratio}
                )

            elif operation == "decompress":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                if isinstance(data, str):
                    data = data.encode()

                try:
                    decompressed = gzip.decompress(data)
                except Exception:
                    try:
                        decompressed = zlib.decompress(data)
                    except Exception:
                        return ActionResult(success=False, message="Decompression failed")

                return ActionResult(
                    success=True,
                    message=f"Decompressed to {len(decompressed)} bytes",
                    data={"decompressed": decompressed, "original_size": len(data), "decompressed_size": len(decompressed)}
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Compression error: {e}")


class ApiGzipAction(BaseAction):
    """Gzip compression specifically for API."""
    action_type = "api_gzip"
    display_name = "API Gzip压缩"
    description = "Gzip压缩API数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            level = params.get("level", 6)

            if data is None:
                return ActionResult(success=False, message="data is required")

            data_bytes = data.encode() if isinstance(data, str) else data

            compressed = gzip.compress(data_bytes, level)
            ratio = len(compressed) / len(data_bytes) if data_bytes else 1

            return ActionResult(
                success=True,
                message=f"Gzip: {len(data_bytes)} → {len(compressed)} bytes",
                data={"compressed": compressed, "original": len(data_bytes), "gzip_size": len(compressed), "ratio": ratio}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Gzip error: {e}")


class ApiDecompressionAction(BaseAction):
    """Decompress API compressed data."""
    action_type = "api_decompression"
    display_name = "API数据解压"
    description = "解压API压缩数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            auto_detect = params.get("auto_detect", True)

            if data is None:
                return ActionResult(success=False, message="data is required")

            data_bytes = data.encode() if isinstance(data, str) else data

            decompressed = None
            method = "unknown"

            if auto_detect:
                for decompressor, name in [(gzip.decompress, "gzip"), (zlib.decompress, "zlib"), (zlib.decompressobj().decompress, "raw")]:
                    try:
                        decompressed = decompressor(data_bytes)
                        method = name
                        break
                    except Exception:
                        continue

                if decompressed is None:
                    return ActionResult(success=False, message="Could not detect compression format")
            else:
                try:
                    decompressed = gzip.decompress(data_bytes)
                    method = "gzip"
                except Exception:
                    try:
                        decompressed = zlib.decompress(data_bytes)
                        method = "zlib"
                    except Exception as e:
                        return ActionResult(success=False, message=f"Decompression failed: {e}")

            return ActionResult(
                success=True,
                message=f"Decompressed using {method}: {len(data_bytes)} → {len(decompressed)} bytes",
                data={"decompressed": decompressed, "method": method, "original_size": len(data_bytes)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Decompression error: {e}")


class ApiStreamCompressAction(BaseAction):
    """Stream compression for large payloads."""
    action_type = "api_stream_compress"
    display_name = "API流式压缩"
    description = "大payload的流式压缩"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            chunks = params.get("chunks", [])
            operation = params.get("operation", "compress")
            chunk_size = params.get("chunk_size", 8192)

            if operation == "compress":
                if not chunks:
                    return ActionResult(success=False, message="chunks list required")

                compressed_chunks = []
                total_original = 0

                with gzip.GzipFile(fileobj=gzip_io(), mode='wb') as gz:
                    for chunk in chunks:
                        chunk_bytes = chunk.encode() if isinstance(chunk, str) else chunk
                        total_original += len(chunk_bytes)
                        gz.write(chunk_bytes)

                result_bytes = gzip_io.getvalue()
                ratio = len(result_bytes) / total_original if total_original > 0 else 1

                return ActionResult(
                    success=True,
                    message=f"Stream compressed {len(chunks)} chunks",
                    data={"compressed": result_bytes, "chunk_count": len(chunks), "ratio": ratio}
                )

            elif operation == "decompress":
                if not chunks:
                    return ActionResult(success=False, message="chunks list required")

                combined = b"".join(c.encode() if isinstance(c, str) else c for c in chunks)
                decompressed = gzip.decompress(combined)

                return ActionResult(
                    success=True,
                    message=f"Stream decompressed to {len(decompressed)} bytes",
                    data={"decompressed": decompressed}
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Stream compress error: {e}")


import io


class gzip_io(io.BytesIO):
    """Gzip BytesIO wrapper."""
    pass

"""API compression action module for RabAI AutoClick.

Provides API payload compression operations:
- CompressGzipAction: Gzip compression
- CompressDeflateAction: Deflate compression
- CompressBrotliAction: Brotli compression
- DecompressAction: Decompress payloads
- CompressionDetectAction: Detect compression type
- CompressionHeaderAction: Set Accept-Encoding headers
"""

import gzip
import zlib
import base64
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompressGzipAction(BaseAction):
    """Gzip compression for API payloads."""
    action_type = "compress_gzip"
    display_name = "Gzip压缩"
    description = "Gzip压缩API载荷"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            compression_level = params.get("compression_level", 6)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not isinstance(data, bytes):
                data = str(data).encode("utf-8")

            compressed = gzip.compress(data, compresslevel=compression_level)
            encoded = base64.b64encode(compressed).decode("ascii")

            return ActionResult(
                success=True,
                data={
                    "original_size": len(data),
                    "compressed_size": len(compressed),
                    "ratio": len(compressed) / len(data) if data else 0,
                    "data": encoded,
                },
                message=f"Gzip: {len(data)} -> {len(compressed)} bytes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Gzip compress failed: {e}")


class CompressDeflateAction(BaseAction):
    """Deflate compression for API payloads."""
    action_type = "compress_deflate"
    display_name = "Deflate压缩"
    description = "Deflate压缩API载荷"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            wbits = params.get("wbits", 15)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not isinstance(data, bytes):
                data = str(data).encode("utf-8")

            compressed = zlib.compress(data, level=6)
            encoded = base64.b64encode(compressed).decode("ascii")

            return ActionResult(
                success=True,
                data={
                    "original_size": len(data),
                    "compressed_size": len(compressed),
                    "ratio": len(compressed) / len(data) if data else 0,
                    "data": encoded,
                },
                message=f"Deflate: {len(data)} -> {len(compressed)} bytes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deflate compress failed: {e}")


class DecompressAction(BaseAction):
    """Decompress payloads."""
    action_type = "decompress"
    display_name = "解压缩"
    description = "解压缩数据载荷"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            encoding = params.get("encoding", "gzip")
            is_base64 = params.get("is_base64", True)

            if not data:
                return ActionResult(success=False, message="data is required")

            if is_base64:
                try:
                    data = base64.b64decode(data)
                except Exception:
                    pass

            if not isinstance(data, bytes):
                return ActionResult(success=False, message="data must be bytes for decompression")

            if encoding == "gzip":
                decompressed = gzip.decompress(data)
            elif encoding == "deflate":
                decompressed = zlib.decompress(data)
            else:
                return ActionResult(success=False, message=f"Unsupported encoding: {encoding}")

            text = decompressed.decode("utf-8")

            return ActionResult(
                success=True,
                data={"original_size": len(data), "decompressed_size": len(decompressed), "text": text},
                message=f"Decompressed: {len(data)} -> {len(decompressed)} bytes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Decompress failed: {e}")


class CompressionDetectAction(BaseAction):
    """Detect compression type from headers."""
    action_type = "compression_detect"
    display_name = "压缩检测"
    description = "检测压缩类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            headers = params.get("headers", {})
            content_encoding = headers.get("Content-Encoding", headers.get("content-encoding", ""))

            encodings = [e.strip().lower() for e in content_encoding.split(",") if e.strip()]

            detected = []
            for enc in encodings:
                if enc in ("gzip", "deflate", "br", "zstd", "identity"):
                    detected.append(enc)

            return ActionResult(
                success=True,
                data={"content_encoding": content_encoding, "detected": detected, "is_compressed": bool(detected)},
                message=f"Detected: {detected or ['identity']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compression detect failed: {e}")


class CompressionHeaderAction(BaseAction):
    """Set Accept-Encoding headers."""
    action_type = "compression_header"
    display_name = "压缩头设置"
    description = "设置Accept-Encoding请求头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            encodings = params.get("encodings", ["gzip", "deflate", "br"])
            quality_values = params.get("quality_values", {})

            header_value = ",".join(encodings)

            return ActionResult(
                success=True,
                data={"Accept-Encoding": header_value, "quality_values": quality_values},
                message=f"Accept-Encoding: {header_value}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compression header failed: {e}")

"""Data compression action module for RabAI AutoClick.

Provides data compression operations:
- CompressAction: Compress data
- DecompressAction: Decompress data
- EncodeDecodeAction: Encode/decode data
- SerializationAction: Serialize/deserialize data
"""

import json
import zlib
import base64
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompressAction(BaseAction):
    """Compress data."""
    action_type = "compress"
    display_name: "压缩"
    description: "压缩数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            algorithm = params.get("algorithm", "gzip")
            level = params.get("level", 6)

            if isinstance(data, dict):
                data = json.dumps(data)
            elif not isinstance(data, str):
                data = str(data)

            data_bytes = data.encode("utf-8")

            if algorithm == "gzip":
                import gzip
                compressed = gzip.compress(data_bytes, level=level)
            elif algorithm == "zlib":
                compressed = zlib.compress(data_bytes, level=level)
            elif algorithm == "lz4":
                import lz4.frame
                compressed = lz4.frame.compress(data_bytes)
            else:
                compressed = zlib.compress(data_bytes, level=level)

            compressed_b64 = base64.b64encode(compressed).decode("ascii")
            original_size = len(data_bytes)
            compressed_size = len(compressed)

            return ActionResult(
                success=True,
                message=f"Compressed {original_size} -> {compressed_size} bytes ({compressed_size/original_size:.1%})",
                data={
                    "compressed": compressed_b64,
                    "original_size": original_size,
                    "compressed_size": compressed_size,
                    "ratio": round(compressed_size / original_size, 4),
                    "algorithm": algorithm,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compress error: {e}")


class DecompressAction(BaseAction):
    """Decompress data."""
    action_type = "decompress"
    display_name: "解压"
    description: "解压数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            algorithm = params.get("algorithm", "gzip")
            output_format = params.get("output_format", "string")

            if isinstance(data, str):
                data = base64.b64decode(data.encode("ascii"))

            if algorithm == "gzip":
                import gzip
                decompressed = gzip.decompress(data)
            elif algorithm == "zlib":
                decompressed = zlib.decompress(data)
            elif algorithm == "lz4":
                import lz4.frame
                decompressed = lz4.frame.decompress(data)
            else:
                decompressed = zlib.decompress(data)

            if output_format == "json":
                try:
                    result = json.loads(decompressed.decode("utf-8"))
                except Exception:
                    result = decompressed.decode("utf-8")
            else:
                result = decompressed.decode("utf-8")

            return ActionResult(
                success=True,
                message=f"Decompressed {len(data)} -> {len(decompressed)} bytes",
                data={"decompressed": result, "size": len(decompressed), "algorithm": algorithm},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Decompress error: {e}")


class EncodeDecodeAction(BaseAction):
    """Encode/decode data."""
    action_type = "encode_decode"
    display_name: "编码解码"
    description: "数据的编码和解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "encode")
            data = params.get("data", "")
            encoding = params.get("encoding", "utf-8")

            if not data:
                return ActionResult(success=False, message="data is required")

            if action == "encode":
                if isinstance(data, dict):
                    encoded = json.dumps(data)
                else:
                    encoded = str(data)
                encoded_bytes = encoded.encode(encoding)
                encoded_b64 = base64.b64encode(encoded_bytes).decode("ascii")
                return ActionResult(success=True, message=f"Encoded to base64", data={"encoded": encoded_b64, "original_size": len(encoded_bytes)})

            elif action == "decode":
                decoded_bytes = base64.b64decode(data.encode("ascii"))
                decoded = decoded_bytes.decode(encoding)
                try:
                    decoded_json = json.loads(decoded)
                    return ActionResult(success=True, message="Decoded base64", data={"decoded": decoded_json, "as_string": decoded})
                except Exception:
                    return ActionResult(success=True, message="Decoded base64", data={"decoded": decoded, "is_json": False})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"EncodeDecode error: {e}")


class SerializationAction(BaseAction):
    """Serialize/deserialize data."""
    action_type: "serialization"
    display_name: "序列化"
    description: "数据序列化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "serialize")
            data = params.get("data", {})
            format = params.get("format", "json")

            if action == "serialize":
                if format == "json":
                    serialized = json.dumps(data, ensure_ascii=False, indent=2)
                elif format == "json_compact":
                    serialized = json.dumps(data, separators=(",", ":"))
                elif format == "msgpack":
                    import msgpack
                    serialized = msgpack.packb(data)
                elif format == "pickle":
                    import pickle
                    serialized = pickle.dumps(data)
                else:
                    serialized = json.dumps(data)

                return ActionResult(
                    success=True,
                    message=f"Serialized to {format}",
                    data={"serialized": serialized, "format": format, "size": len(str(serialized))},
                )

            elif action == "deserialize":
                if format == "json":
                    deserialized = json.loads(data)
                elif format == "msgpack":
                    import msgpack
                    deserialized = msgpack.unpackb(data, raw=False)
                elif format == "pickle":
                    import pickle
                    deserialized = pickle.loads(data)
                else:
                    deserialized = json.loads(data)

                return ActionResult(
                    success=True,
                    message=f"Deserialized from {format}",
                    data={"deserialized": deserialized, "format": format},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Serialization error: {e}")

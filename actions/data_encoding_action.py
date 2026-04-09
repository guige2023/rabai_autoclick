"""Data encoding action module for RabAI AutoClick.

Provides data encoding/decoding operations:
- EncoderAction: Encode data in various formats
- DecoderAction: Decode data from various formats
- CodecRegistryAction: Manage encoding codecs
- CharsetConverterAction: Convert between character encodings
"""

import sys
import os
import base64
import json
import logging
import urllib.parse
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
import zlib
import gzip

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


class Encoder:
    """Encoding operations."""

    @staticmethod
    def base64_encode(data: bytes) -> str:
        return base64.b64encode(data).decode("ascii")

    @staticmethod
    def base64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")

    @staticmethod
    def hex_encode(data: bytes) -> str:
        return data.hex()

    @staticmethod
    def gzip_encode(data: bytes, compression_level: int = 9) -> bytes:
        return gzip.compress(data, level=compression_level)

    @staticmethod
    def zlib_encode(data: bytes, compression_level: int = 9) -> bytes:
        return zlib.compress(data, level=compression_level)

    @staticmethod
    def json_encode(data: Any) -> str:
        return json.dumps(data, ensure_ascii=False)

    @staticmethod
    def url_encode(data: str) -> str:
        return urllib.parse.quote_plus(data)


class Decoder:
    """Decoding operations."""

    @staticmethod
    def base64_decode(data: str) -> bytes:
        padding = 4 - len(data) % 4
        if padding != 4:
            data += "=" * padding
        return base64.b64decode(data)

    @staticmethod
    def base64url_decode(data: str) -> bytes:
        padding = 4 - len(data) % 4
        if padding != 4:
            data += "=" * padding
        return base64.urlsafe_b64decode(data)

    @staticmethod
    def hex_decode(data: str) -> bytes:
        return bytes.fromhex(data)

    @staticmethod
    def gzip_decode(data: bytes) -> bytes:
        return gzip.decompress(data)

    @staticmethod
    def zlib_decode(data: bytes) -> bytes:
        return zlib.decompress(data)

    @staticmethod
    def json_decode(data: str) -> Any:
        return json.loads(data)

    @staticmethod
    def url_decode(data: str) -> str:
        return urllib.parse.unquote_plus(data)


class CharsetConverter:
    """Character encoding conversion."""

    def __init__(self) -> None:
        self._encodings = ["utf-8", "gbk", "gb2312", "gb18030", "big5", "shift_jis", "euc_kr", "iso-8859-1", "ascii"]

    def convert(self, data: str, from_encoding: str, to_encoding: str) -> str:
        try:
            if from_encoding.lower() in ("utf-8", "utf8"):
                byte_data = data.encode(to_encoding)
            else:
                byte_data = data.encode(from_encoding)
                if to_encoding.lower() in ("utf-8", "utf8"):
                    return byte_data.decode("utf-8")
                return byte_data.decode(to_encoding)
            return byte_data.decode(to_encoding)
        except Exception as e:
            raise ValueError(f"Conversion from {from_encoding} to {to_encoding} failed: {e}")

    def detect(self, data: bytes) -> str:
        try:
            data.decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            pass
        try:
            data.decode("gbk")
            return "gbk"
        except UnicodeDecodeError:
            pass
        return "iso-8859-1"


@dataclass
class Codec:
    """A codec definition."""
    name: str
    encode_fn: Callable[[bytes], bytes]
    decode_fn: Callable[[bytes], bytes]
    description: str = ""


class CodecRegistry:
    """Registry for encoding codecs."""

    def __init__(self) -> None:
        self._codecs: Dict[str, Codec] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self.register(Codec(
            name="base64",
            encode_fn=lambda d: Encoder.base64_encode(d).encode(),
            decode_fn=lambda d: Decoder.base64_decode(d.decode()),
            description="Base64 encoding"
        ))
        self.register(Codec(
            name="hex",
            encode_fn=lambda d: Encoder.hex_encode(d).encode(),
            decode_fn=lambda d: Decoder.hex_decode(d.decode()),
            description="Hexadecimal encoding"
        ))
        self.register(Codec(
            name="gzip",
            encode_fn=lambda d: Encoder.gzip_encode(d),
            decode_fn=lambda d: Decoder.gzip_decode(d),
            description="GZIP compression"
        ))
        self.register(Codec(
            name="zlib",
            encode_fn=lambda d: Encoder.zlib_encode(d),
            decode_fn=lambda d: Decoder.zlib_decode(d),
            description="Zlib compression"
        ))

    def register(self, codec: Codec) -> None:
        self._codecs[codec.name] = codec

    def unregister(self, name: str) -> bool:
        if name in self._codecs:
            del self._codecs[name]
            return True
        return False

    def get(self, name: str) -> Optional[Codec]:
        return self._codecs.get(name)

    def list_all(self) -> List[Codec]:
        return list(self._codecs.values())


_registry = CodecRegistry()


class EncoderAction(BaseAction):
    """Encode data in various formats."""
    action_type = "data_encoder"
    display_name = "数据编码"
    description = "将数据编码为指定格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        format_type = params.get("format", "base64")
        input_type = params.get("input_type", "string")

        if input_type == "string":
            data_bytes = data.encode("utf-8")
        elif input_type == "hex":
            data_bytes = bytes.fromhex(data)
        else:
            data_bytes = data

        if format_type == "base64":
            result = Encoder.base64_encode(data_bytes)
        elif format_type == "base64url":
            result = Encoder.base64url_encode(data_bytes)
        elif format_type == "hex":
            result = Encoder.hex_encode(data_bytes)
        elif format_type == "gzip":
            compressed = Encoder.gzip_encode(data_bytes)
            result = base64.b64encode(compressed).decode()
        elif format_type == "zlib":
            compressed = Encoder.zlib_encode(data_bytes)
            result = base64.b64encode(compressed).decode()
        elif format_type == "json":
            try:
                json_data = json.loads(data) if isinstance(data, str) else data
                result = json.dumps(json_data, ensure_ascii=False)
            except json.JSONDecodeError:
                return ActionResult(success=False, message="JSON编码失败：无效的JSON数据")
            data_bytes = result.encode("utf-8")
            return ActionResult(success=True, message="JSON编码完成", data={"result": result})
        elif format_type == "url":
            result = Encoder.url_encode(data if isinstance(data, str) else data.decode("utf-8"))
        else:
            return ActionResult(success=False, message=f"未知格式: {format_type}")

        return ActionResult(
            success=True,
            message=f"编码为 {format_type} 完成",
            data={"format": format_type, "result": result, "original_size": len(data_bytes), "encoded_size": len(result)}
        )


class DecoderAction(BaseAction):
    """Decode data from various formats."""
    action_type = "data_decoder"
    display_name = "数据解码"
    description = "将数据从指定格式解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        format_type = params.get("format", "base64")
        output_type = params.get("output_type", "string")

        if format_type == "base64":
            decoded = Decoder.base64_decode(data)
        elif format_type == "base64url":
            decoded = Decoder.base64url_decode(data)
        elif format_type == "hex":
            decoded = Decoder.hex_decode(data)
        elif format_type == "gzip":
            compressed = base64.b64decode(data)
            decoded = Decoder.gzip_decode(compressed)
        elif format_type == "zlib":
            compressed = base64.b64decode(data)
            decoded = Decoder.zlib_decode(compressed)
        elif format_type == "json":
            try:
                parsed = Decoder.json_decode(data)
                return ActionResult(
                    success=True,
                    message="JSON解码完成",
                    data={"result": parsed, "type": type(parsed).__name__}
                )
            except json.JSONDecodeError as e:
                return ActionResult(success=False, message=f"JSON解码失败: {e}")
        elif format_type == "url":
            decoded = Decoder.url_decode(data).encode("utf-8")
        else:
            return ActionResult(success=False, message=f"未知格式: {format_type}")

        if output_type == "string":
            result = decoded.decode("utf-8")
        elif output_type == "bytes":
            result = decoded
        elif output_type == "hex":
            result = decoded.hex()
        else:
            result = decoded.decode("utf-8")

        return ActionResult(
            success=True,
            message=f"从 {format_type} 解码完成",
            data={"result": result, "format": format_type, "decoded_size": len(decoded)}
        )


class CodecRegistryAction(BaseAction):
    """Manage encoding codecs."""
    action_type = "data_codec_registry"
    display_name = "编解码器注册表"
    description = "管理数据编解码器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "list")
        codec_name = params.get("codec_name", "")

        if operation == "list":
            codecs = _registry.list_all()
            return ActionResult(
                success=True,
                message=f"共 {len(codecs)} 个编解码器",
                data={"codecs": [{"name": c.name, "description": c.description} for c in codecs]}
            )

        if operation == "get":
            codec = _registry.get(codec_name)
            if not codec:
                return ActionResult(success=False, message=f"编解码器 {codec_name} 不存在")
            return ActionResult(
                success=True,
                message=f"编解码器: {codec_name}",
                data={"name": codec.name, "description": codec.description}
            )

        if operation == "unregister":
            if _registry.unregister(codec_name):
                return ActionResult(success=True, message=f"编解码器 {codec_name} 已注销")
            return ActionResult(success=False, message=f"编解码器 {codec_name} 不存在")

        return ActionResult(success=False, message=f"未知操作: {operation}")


class CharsetConverterAction(BaseAction):
    """Convert between character encodings."""
    action_type = "data_charset_converter"
    display_name = "字符编码转换"
    description = "在不同的字符编码之间转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        from_encoding = params.get("from_encoding", "utf-8")
        to_encoding = params.get("to_encoding", "utf-8")
        operation = params.get("operation", "convert")

        converter = CharsetConverter()

        if operation == "convert":
            try:
                result = converter.convert(data, from_encoding, to_encoding)
                return ActionResult(
                    success=True,
                    message=f"从 {from_encoding} 转换到 {to_encoding} 完成",
                    data={"result": result, "from": from_encoding, "to": to_encoding}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"编码转换失败: {e}")

        if operation == "detect":
            if isinstance(data, str):
                data = data.encode(from_encoding)
            detected = converter.detect(data)
            return ActionResult(
                success=True,
                message=f"检测到编码: {detected}",
                data={"detected": detected}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")

"""
Encoding utilities - base64, hex, URL encoding, unicode normalization, compression.
"""
from typing import Any, Dict, List, Optional
import base64
import zlib
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class EncodingAction(BaseAction):
    """Encoding and decoding operations.

    Provides base64, hex, URL encoding, gzip compression, unicode normalization.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "base64_encode")
        text = params.get("text", "")

        try:
            if operation == "base64_encode":
                data = text.encode() if isinstance(text, str) else text
                return {"success": True, "encoded": base64.b64encode(data).decode()}

            elif operation == "base64_decode":
                try:
                    decoded = base64.b64decode(text.encode())
                    return {"success": True, "decoded": decoded.decode("utf-8", errors="replace")}
                except Exception as e:
                    return {"success": False, "error": f"Base64 decode error: {e}"}

            elif operation == "base64_url_encode":
                data = text.encode() if isinstance(text, str) else text
                return {"success": True, "encoded": base64.urlsafe_b64encode(data).decode().rstrip("=")}

            elif operation == "base64_url_decode":
                try:
                    padded = text + "=" * (4 - len(text) % 4)
                    decoded = base64.urlsafe_b64decode(padded)
                    return {"success": True, "decoded": decoded.decode("utf-8", errors="replace")}
                except Exception as e:
                    return {"success": False, "error": f"URL-safe base64 decode error: {e}"}

            elif operation == "hex_encode":
                data = text.encode() if isinstance(text, str) else text
                return {"success": True, "encoded": data.hex()}

            elif operation == "hex_decode":
                try:
                    decoded = bytes.fromhex(text)
                    return {"success": True, "decoded": decoded.decode("utf-8", errors="replace")}
                except Exception as e:
                    return {"success": False, "error": f"Hex decode error: {e}"}

            elif operation == "gzip_compress":
                data = text.encode() if isinstance(text, str) else text
                compressed = zlib.compress(data, level=9)
                return {"success": True, "compressed": base64.b64encode(compressed).decode(), "size_original": len(data), "size_compressed": len(compressed)}

            elif operation == "gzip_decompress":
                try:
                    compressed = base64.b64decode(text)
                    decompressed = zlib.decompress(compressed)
                    return {"success": True, "decompressed": decompressed.decode("utf-8", errors="replace"), "size": len(decompressed)}
                except Exception as e:
                    return {"success": False, "error": f"Gzip decompress error: {e}"}

            elif operation == "deflate":
                data = text.encode() if isinstance(text, str) else text
                compressed = zlib.compress(data)[2:-4]
                return {"success": True, "compressed": base64.b64encode(compressed).decode()}

            elif operation == "inflate":
                try:
                    compressed = base64.b64decode(text)
                    decompressed = zlib.decompress(compressed, -zlib.MAX_WBITS)
                    return {"success": True, "decompressed": decompressed.decode("utf-8", errors="replace")}
                except Exception:
                    try:
                        decompressed = zlib.decompress(compressed)
                        return {"success": True, "decompressed": decompressed.decode("utf-8", errors="replace")}
                    except Exception as e:
                        return {"success": False, "error": f"Inflate error: {e}"}

            elif operation == "unicode_normalize":
                import unicodedata
                form = params.get("form", "NFC")
                normalized = unicodedata.normalize(form, text)
                return {"success": True, "normalized": normalized, "form": form}

            elif operation == "bytes_to_list":
                data = text.encode() if isinstance(text, str) else text
                return {"success": True, "bytes": list(data), "length": len(data)}

            elif operation == "list_to_bytes":
                byte_list = params.get("bytes", [])
                result = bytes(byte_list)
                return {"success": True, "data": result.decode("utf-8", errors="replace") if all(b < 128 for b in byte_list) else result.hex()}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"EncodingAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return EncodingAction().execute(context, params)

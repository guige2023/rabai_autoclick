"""
Cryptography utilities - encryption, hashing, HMAC, encoding, password hashing.
"""
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import hmac
import logging
import os
import base64

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _hash_bytes(data: bytes, algorithm: str) -> str:
    if algorithm == "md5":
        return hashlib.md5(data).hexdigest()
    elif algorithm == "sha1":
        return hashlib.sha1(data).hexdigest()
    elif algorithm == "sha256":
        return hashlib.sha256(data).hexdigest()
    elif algorithm == "sha512":
        return hashlib.sha512(data).hexdigest()
    elif algorithm == "blake2b":
        return hashlib.blake2b(data).hexdigest()
    elif algorithm == "blake2s":
        return hashlib.blake2s(data).hexdigest()
    elif algorithm == "sha3_256":
        return hashlib.sha3_256(data).hexdigest()
    elif algorithm == "sha3_512":
        return hashlib.sha3_512(data).hexdigest()
    return hashlib.sha256(data).hexdigest()


def _pbkdf2_hash(password: str, salt: bytes, iterations: int = 100000, keylen: int = 32) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations, dklen=keylen).hex()


def _secure_compare(a: bytes, b: bytes) -> bool:
    if len(a) != len(b):
        return False
    result = 0
    for x, y in zip(a, b):
        result |= x ^ y
    return result == 0


class CryptographyAction(BaseAction):
    """Cryptography operations.

    Provides hashing, HMAC, secure comparison, salt generation, encoding/decoding.
    Requires pip install cryptography for advanced features (Fernet, RSA), optional.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "hash")
        data = params.get("data", "")
        password = params.get("password", "")

        try:
            if operation == "hash":
                algorithm = params.get("algorithm", "sha256")
                data_bytes = data.encode() if isinstance(data, str) else data
                return {"success": True, "hash": _hash_bytes(data_bytes, algorithm), "algorithm": algorithm}

            elif operation == "hmac":
                key = params.get("key", "")
                key_bytes = key.encode() if isinstance(key, str) else key
                data_bytes = data.encode() if isinstance(data, str) else data
                algorithm = params.get("algorithm", "sha256")
                if algorithm == "sha256":
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha256)
                elif algorithm == "sha512":
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha512)
                else:
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha256)
                return {"success": True, "hmac": h.hexdigest(), "algorithm": algorithm}

            elif operation == "verify_hash":
                data_bytes = data.encode() if isinstance(data, str) else data
                expected = params.get("expected_hash", "")
                algorithm = params.get("algorithm", "sha256")
                actual = _hash_bytes(data_bytes, algorithm)
                return {"success": True, "valid": _secure_compare(actual.encode(), expected.encode())}

            elif operation == "verify_hmac":
                key = params.get("key", "")
                key_bytes = key.encode() if isinstance(key, str) else key
                data_bytes = data.encode() if isinstance(data, str) else data
                expected = params.get("expected_hmac", "")
                algorithm = params.get("algorithm", "sha256")
                if algorithm == "sha256":
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha256)
                elif algorithm == "sha512":
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha512)
                else:
                    h = hmac.new(key_bytes, data_bytes, hashlib.sha256)
                return {"success": True, "valid": _secure_compare(h.hexdigest().encode(), expected.encode())}

            elif operation == "pbkdf2":
                if not password:
                    return {"success": False, "error": "password required"}
                salt = params.get("salt", os.urandom(16))
                salt_bytes = salt.encode() if isinstance(salt, str) else salt
                iterations = int(params.get("iterations", 100000))
                keylen = int(params.get("keylen", 32))
                hashed = _pbkdf2_hash(password, salt_bytes, iterations, keylen)
                return {"success": True, "hash": hashed, "salt": base64.b64encode(salt_bytes).decode(), "iterations": iterations}

            elif operation == "generate_salt":
                length = int(params.get("length", 16))
                salt = os.urandom(length)
                return {"success": True, "salt": base64.b64encode(salt).decode(), "hex": salt.hex()}

            elif operation == "base64_encode":
                data_bytes = data.encode() if isinstance(data, str) else data
                encoded = base64.b64encode(data_bytes).decode()
                return {"success": True, "encoded": encoded}

            elif operation == "base64_decode":
                try:
                    decoded = base64.b64decode(data.encode() if isinstance(data, str) else data)
                    return {"success": True, "decoded": decoded.decode("utf-8", errors="replace")}
                except Exception as e:
                    return {"success": False, "error": f"Base64 decode error: {e}"}

            elif operation == "hex_encode":
                data_bytes = data.encode() if isinstance(data, str) else data
                return {"success": True, "hex": data_bytes.hex()}

            elif operation == "hex_decode":
                try:
                    decoded = bytes.fromhex(data)
                    return {"success": True, "decoded": decoded.decode("utf-8", errors="replace")}
                except Exception as e:
                    return {"success": False, "error": f"Hex decode error: {e}"}

            elif operation == "secure_random":
                length = int(params.get("length", 32))
                rand_bytes = os.urandom(length)
                return {"success": True, "hex": rand_bytes.hex(), "base64": base64.b64encode(rand_bytes).decode()}

            elif operation == "secure_compare":
                a = params.get("a", "")
                b = params.get("b", "")
                a_bytes = a.encode() if isinstance(a, str) else a
                b_bytes = b.encode() if isinstance(b, str) else b
                return {"success": True, "equal": _secure_compare(a_bytes, b_bytes)}

            elif operation == "scrypt_hash":
                try:
                    import hashlib as hl
                    salt = params.get("salt", os.urandom(16))
                    salt_bytes = salt.encode() if isinstance(salt, str) else salt
                    n = int(params.get("n", 16384))
                    r = int(params.get("r", 8))
                    p = int(params.get("p", 1))
                    dklen = int(params.get("dklen", 32))
                    hashed = hl.scrypt(password.encode(), salt=salt_bytes, n=n, r=r, p=p, dklen=dklen)
                    return {"success": True, "hash": hashed.hex(), "salt": base64.b64encode(salt_bytes).decode()}
                except Exception as e:
                    return {"success": False, "error": f"scrypt not available: {e}"}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"CryptographyAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for cryptography operations."""
    return CryptographyAction().execute(context, params)

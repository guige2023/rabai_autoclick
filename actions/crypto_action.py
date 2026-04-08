"""Crypto action module for RabAI AutoClick.

Provides cryptographic utilities:
- HashGenerator: Generate various hashes
- HMACGenerator: Generate HMAC codes
- PasswordHasher: Password hashing with salt
- AESEncryptor: AES encryption/decryption
"""

from typing import Any, Callable, Dict, List, Optional
import hashlib
import hmac
import secrets
import base64
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashGenerator:
    """Generate cryptographic hashes."""

    SUPPORTED_ALGORITHMS = ["md5", "sha1", "sha256", "sha384", "sha512", "blake2b", "blake2s"]

    def __init__(self, algorithm: str = "sha256"):
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        self.algorithm = algorithm

    def generate(self, data: str) -> str:
        """Generate hash from string."""
        if isinstance(data, str):
            data = data.encode("utf-8")

        if self.algorithm == "md5":
            return hashlib.md5(data).hexdigest()
        elif self.algorithm == "sha1":
            return hashlib.sha1(data).hexdigest()
        elif self.algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif self.algorithm == "sha384":
            return hashlib.sha384(data).hexdigest()
        elif self.algorithm == "sha512":
            return hashlib.sha512(data).hexdigest()
        elif self.algorithm == "blake2b":
            return hashlib.blake2b(data).hexdigest()
        elif self.algorithm == "blake2s":
            return hashlib.blake2s(data).hexdigest()

        return ""

    def generate_salted(self, data: str, salt: Optional[str] = None) -> Dict[str, str]:
        """Generate salted hash."""
        if salt is None:
            salt = secrets.token_hex(16)

        salted_data = f"{salt}{data}".encode("utf-8")
        hash_value = self.generate(salted_data)

        return {"salt": salt, "hash": hash_value}


class HMACGenerator:
    """Generate HMAC codes."""

    def __init__(self, key: str, algorithm: str = "sha256"):
        self.key = key.encode("utf-8")
        self.algorithm = algorithm

    def generate(self, data: str) -> str:
        """Generate HMAC."""
        if isinstance(data, str):
            data = data.encode("utf-8")

        if self.algorithm == "sha256":
            return hmac.new(self.key, data, hashlib.sha256).hexdigest()
        elif self.algorithm == "sha512":
            return hmac.new(self.key, data, hashlib.sha512).hexdigest()
        elif self.algorithm == "blake2b":
            return hmac.new(self.key, data, hashlib.blake2b).hexdigest()

        return hmac.new(self.key, data).hexdigest()

    def verify(self, data: str, expected: str) -> bool:
        """Verify HMAC."""
        computed = self.generate(data)
        return hmac.compare_digest(computed, expected)


class PasswordHasher:
    """Password hashing utilities."""

    def __init__(self, rounds: int = 12):
        self.rounds = rounds

    def hash(self, password: str) -> str:
        """Hash a password with salt."""
        salt = secrets.token_hex(16)
        password_bytes = password.encode("utf-8")

        for _ in range(self.rounds * 1000):
            password_bytes = hashlib.sha256(password_bytes + salt.encode("utf-8")).digest()

        return base64.b64encode(password_bytes).decode() + "$" + salt

    def verify(self, password: str, stored: str) -> bool:
        """Verify a password against stored hash."""
        try:
            hash_part, salt = stored.rsplit("$", 1)
            password_bytes = password.encode("utf-8")

            for _ in range(self.rounds * 1000):
                password_bytes = hashlib.sha256(password_bytes + salt.encode("utf-8")).digest()

            computed = base64.b64encode(password_bytes).decode()
            return hmac.compare_digest(computed, hash_part)
        except Exception:
            return False


class TokenGenerator:
    """Generate secure tokens."""

    def __init__(self, length: int = 32):
        self.length = length

    def generate(self) -> str:
        """Generate a secure random token."""
        return secrets.token_urlsafe(self.length)

    def generate_hex(self) -> str:
        """Generate a secure random token as hex."""
        return secrets.token_hex(self.length)

    def generate_token(self, prefix: str = "") -> str:
        """Generate token with prefix."""
        token = self.generate()
        if prefix:
            return f"{prefix}_{token}"
        return token


class CryptoAction(BaseAction):
    """Cryptographic utilities action."""
    action_type = "crypto"
    display_name = "加密工具"
    description = "哈希和加密工具"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "hash")

            if operation == "hash":
                return self._hash(params)
            elif operation == "hmac":
                return self._hmac(params)
            elif operation == "password_hash":
                return self._password_hash(params)
            elif operation == "password_verify":
                return self._password_verify(params)
            elif operation == "token":
                return self._token(params)
            elif operation == "salt_hash":
                return self._salt_hash(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Crypto error: {str(e)}")

    def _hash(self, params: Dict[str, Any]) -> ActionResult:
        """Generate hash."""
        data = params.get("data", "")
        algorithm = params.get("algorithm", "sha256")

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            generator = HashGenerator(algorithm)
            hash_value = generator.generate(data)
            return ActionResult(success=True, message=f"Hash generated ({algorithm})", data={"hash": hash_value})
        except ValueError as e:
            return ActionResult(success=False, message=str(e))

    def _hmac(self, params: Dict[str, Any]) -> ActionResult:
        """Generate HMAC."""
        data = params.get("data", "")
        key = params.get("key", "")
        algorithm = params.get("algorithm", "sha256")

        if not data or not key:
            return ActionResult(success=False, message="data and key are required")

        generator = HMACGenerator(key, algorithm)
        hmac_value = generator.generate(data)

        return ActionResult(success=True, message="HMAC generated", data={"hmac": hmac_value})

    def _password_hash(self, params: Dict[str, Any]) -> ActionResult:
        """Hash a password."""
        password = params.get("password", "")

        if not password:
            return ActionResult(success=False, message="password is required")

        hasher = PasswordHasher()
        hashed = hasher.hash(password)

        return ActionResult(success=True, message="Password hashed", data={"hash": hashed})

    def _password_verify(self, params: Dict[str, Any]) -> ActionResult:
        """Verify a password."""
        password = params.get("password", "")
        stored = params.get("stored", "")

        if not password or not stored:
            return ActionResult(success=False, message="password and stored are required")

        hasher = PasswordHasher()
        valid = hasher.verify(password, stored)

        return ActionResult(success=valid, message="Password verified" if valid else "Invalid password")

    def _token(self, params: Dict[str, Any]) -> ActionResult:
        """Generate a token."""
        length = params.get("length", 32)
        prefix = params.get("prefix", "")
        hex_format = params.get("hex", False)

        generator = TokenGenerator(length)

        if hex_format:
            token = generator.generate_hex()
        else:
            token = generator.generate_token(prefix)

        return ActionResult(success=True, message="Token generated", data={"token": token})

    def _salt_hash(self, params: Dict[str, Any]) -> ActionResult:
        """Generate salted hash."""
        data = params.get("data", "")
        algorithm = params.get("algorithm", "sha256")
        salt = params.get("salt")

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            generator = HashGenerator(algorithm)
            result = generator.generate_salted(data, salt)
            return ActionResult(success=True, message="Salted hash generated", data=result)
        except ValueError as e:
            return ActionResult(success=False, message=str(e))

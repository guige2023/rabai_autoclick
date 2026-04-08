"""API Security Action Module.

Provides API security features: signature verification,
encryption/decryption, and input sanitization.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


@dataclass
class SecurityConfig:
    """Security configuration."""
    secret_key: str
    algorithm: str = "sha256"
    signature_header: str = "X-Signature"
    timestamp_header: str = "X-Timestamp"
    nonce_header: str = "X-Nonce"
    timestamp_tolerance_seconds: float = 300.0


class APISecurityAction:
    """API security handler.

    Example:
        security = APISecurityAction(
            SecurityConfig(secret_key="your-secret-key")
        )

        signature = security.sign({"data": "value"})
        is_valid = security.verify({"data": "value"}, signature)

        encrypted = security.encrypt("sensitive data")
    """

    def __init__(self, config: Optional[SecurityConfig] = None) -> None:
        self.config = config

    def sign(
        self,
        payload: Union[Dict, str, bytes],
    ) -> str:
        """Generate HMAC signature for payload.

        Args:
            payload: Data to sign

        Returns:
            Hex-encoded signature
        """
        if isinstance(payload, dict):
            payload = json.dumps(payload, sort_keys=True)
        if isinstance(payload, str):
            payload = payload.encode("utf-8")

        signature = hmac.new(
            self.config.secret_key.encode("utf-8"),
            payload,
            hashlib.sha256
        ).hexdigest()

        return signature

    def verify(
        self,
        payload: Union[Dict, str, bytes],
        signature: str,
    ) -> bool:
        """Verify HMAC signature.

        Args:
            payload: Original payload
            signature: Signature to verify

        Returns:
            True if valid, False otherwise
        """
        expected = self.sign(payload)
        return secrets.compare_digest(expected, signature)

    def generate_nonce(self) -> str:
        """Generate random nonce.

        Returns:
            Hex-encoded nonce
        """
        return secrets.token_hex(16)

    def verify_timestamp(
        self,
        timestamp: float,
        tolerance: Optional[float] = None,
    ) -> bool:
        """Verify timestamp is within tolerance.

        Args:
            timestamp: Unix timestamp
            tolerance: Tolerance in seconds

        Returns:
            True if within tolerance
        """
        import time
        tolerance = tolerance or self.config.timestamp_tolerance_seconds
        return abs(time.time() - timestamp) <= tolerance

    def encrypt(
        self,
        data: Union[str, bytes],
        key: Optional[str] = None,
    ) -> str:
        """Encrypt data (simplified XOR for demo).

        Args:
            data: Data to encrypt
            key: Encryption key (uses config if not provided)

        Returns:
            Hex-encoded encrypted data
        """
        import base64

        key = (key or self.config.secret_key).encode("utf-8")
        if isinstance(data, str):
            data = data.encode("utf-8")

        key_bytes = key * (len(data) // len(key) + 1)
        encrypted = bytes(a ^ b for a, b in zip(data, key_bytes))

        return base64.b64encode(encrypted).decode("ascii")

    def decrypt(
        self,
        encrypted_data: str,
        key: Optional[str] = None,
    ) -> str:
        """Decrypt data.

        Args:
            encrypted_data: Encrypted string
            key: Decryption key

        Returns:
            Decrypted string
        """
        import base64

        key = (key or self.config.secret_key).encode("utf-8")
        encrypted = base64.b64decode(encrypted_data.encode("ascii"))

        key_bytes = key * (len(encrypted) // len(key) + 1)
        decrypted = bytes(a ^ b for a, b in zip(encrypted, key_bytes))

        return decrypted.decode("utf-8")

    def sanitize(
        self,
        data: Any,
        max_length: int = 10000,
    ) -> Any:
        """Sanitize input data.

        Args:
            data: Data to sanitize
            max_length: Maximum string length

        Returns:
            Sanitized data
        """
        if isinstance(data, str):
            return self._sanitize_string(data, max_length)

        elif isinstance(data, dict):
            return {
                k: self.sanitize(v, max_length)
                for k, v in data.items()
            }

        elif isinstance(data, list):
            return [
                self.sanitize(item, max_length)
                for item in data[:1000]
            ]

        return data

    def _sanitize_string(
        self,
        s: str,
        max_length: int,
    ) -> str:
        """Sanitize string."""
        s = s[:max_length]
        s = s.replace("\x00", "")
        return s

    def hash_pii(
        self,
        data: str,
        salt: Optional[str] = None,
    ) -> str:
        """Hash PII data for privacy.

        Args:
            data: PII data to hash
            salt: Optional salt

        Returns:
            Hashed data
        """
        salt = salt or self.config.secret_key
        combined = f"{data}{salt}".encode("utf-8")

        return hashlib.sha256(combined).hexdigest()[:16]

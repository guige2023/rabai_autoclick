"""Secret manager utilities: encryption, decryption, and secret storage."""

from __future__ import annotations

import base64
import hashlib
import os
import secrets
from dataclasses import dataclass
from typing import Any

__all__ = [
    "SecretManager",
    "encrypt_secret",
    "decrypt_secret",
    "hash_secret",
    "generate_password",
    "generate_token",
]


@dataclass
class Secret:
    """Represents a stored secret."""

    name: str
    encrypted_value: str
    version: int
    created_at: float
    expires_at: float | None = None


class SecretManager:
    """In-memory secret manager with encryption support."""

    def __init__(self, master_key: str | None = None) -> None:
        self._master_key = master_key or os.environ.get("SECRET_MASTER_KEY", "")
        self._secrets: dict[str, Secret] = {}

    def _derive_key(self, salt: bytes) -> bytes:
        """Derive encryption key from master key."""
        return hashlib.pbkdf2_hmac(
            "sha256",
            self._master_key.encode(),
            salt,
            100000,
        )

    def set(self, name: str, value: str, expires_at: float | None = None) -> None:
        """Store a secret."""
        encrypted = encrypt_secret(value, self._master_key)
        existing = self._secrets.get(name)
        version = (existing.version + 1) if existing else 1
        self._secrets[name] = Secret(
            name=name,
            encrypted_value=encrypted,
            version=version,
            created_at=__import__("time").time(),
            expires_at=expires_at,
        )

    def get(self, name: str) -> str | None:
        """Retrieve a secret."""
        secret = self._secrets.get(name)
        if secret is None:
            return None
        return decrypt_secret(secret.encrypted_value, self._master_key)

    def delete(self, name: str) -> bool:
        """Delete a secret."""
        if name in self._secrets:
            del self._secrets[name]
            return True
        return False

    def list_secrets(self) -> list[str]:
        """List all secret names."""
        return list(self._secrets.keys())

    def rotate(self, name: str, new_value: str) -> bool:
        """Rotate a secret to a new value."""
        if name not in self._secrets:
            return False
        self.set(name, new_value)
        return True


def encrypt_secret(plaintext: str, key: str) -> str:
    """Encrypt a secret with AES-like encryption."""
    if not key:
        return base64.b64encode(plaintext.encode()).decode()

    import hashlib
    key_bytes = hashlib.sha256(key.encode()).digest()
    salt = secrets.token_bytes(16)

    encrypted = bytearray()
    for i, byte in enumerate(plaintext.encode()):
        encrypted.append(byte ^ key_bytes[i % len(key_bytes)] ^ salt[i % len(salt)])

    result = base64.b64encode(salt + bytes(encrypted)).decode()
    return result


def decrypt_secret(ciphertext: str, key: str) -> str:
    """Decrypt a secret."""
    if not key:
        return base64.b64decode(ciphertext.encode()).decode()

    try:
        data = base64.b64decode(ciphertext.encode())
        salt = data[:16]
        encrypted = data[16:]

        key_bytes = hashlib.sha256(key.encode()).digest()
        decrypted = bytearray()
        for i, byte in enumerate(encrypted):
            decrypted.append(byte ^ key_bytes[i % len(key_bytes)] ^ salt[i % len(salt)])

        return decrypted.decode()
    except Exception:
        return ""


def hash_secret(secret: str, salt: str | None = None) -> tuple[str, str]:
    """Hash a secret with salt."""
    salt = salt or secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac(
        "sha256",
        secret.encode(),
        salt.encode(),
        100000,
    )
    return base64.b64encode(hashed).decode(), salt


def generate_password(length: int = 16) -> str:
    """Generate a secure random password."""
    import string
    alphabet = string.ascii_letters + string.digits + string.punctuation
    return "".join(secrets.choice(alphabet) for _ in range(length))


def generate_token(length: int = 32) -> str:
    """Generate a secure random token."""
    return secrets.token_urlsafe(length)

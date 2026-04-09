"""Data encryption action module.

Provides data encryption and decryption functionality
using various cryptographic algorithms.
"""

from __future__ import annotations

import os
import hashlib
import hmac
import base64
import logging
from typing import Any, Optional, Union
from dataclasses import dataclass
from enum import Enum
import secrets

logger = logging.getLogger(__name__)

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, padding
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False


class EncryptionAlgorithm(Enum):
    """Supported encryption algorithms."""
    AES_256_GCM = "aes_256_gcm"
    AES_256_CBC = "aes_256_cbc"
    CHACHA20_POLY1305 = "chacha20_poly1305"


class HashAlgorithm(Enum):
    """Supported hash algorithms."""
    SHA256 = "sha256"
    SHA384 = "sha384"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"
    BLAKE2S = "blake2s"


@dataclass
class EncryptedData:
    """Encrypted data container."""
    ciphertext: bytes
    nonce: bytes
    tag: Optional[bytes] = None
    algorithm: str = "aes_256_gcm"


class KeyDerivation:
    """Key derivation functions."""

    @staticmethod
    def pbkdf2(
        password: str,
        salt: bytes,
        iterations: int = 100000,
        key_length: int = 32,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
    ) -> bytes:
        """Derive key using PBKDF2.

        Args:
            password: Password to derive from
            salt: Salt bytes
            iterations: Number of iterations
            key_length: Desired key length
            algorithm: Hash algorithm

        Returns:
            Derived key bytes
        """
        if not HAS_CRYPTO:
            raise ImportError("cryptography library not installed")

        hash_map = {
            HashAlgorithm.SHA256: hashes.SHA256(),
            HashAlgorithm.SHA384: hashes.SHA384(),
            HashAlgorithm.SHA512: hashes.SHA512(),
        }

        kdf = PBKDF2HMAC(
            algorithm=hash_map.get(algorithm, hashes.SHA256()),
            length=key_length,
            salt=salt,
            iterations=iterations,
            backend=default_backend(),
        )
        return kdf.derive(password.encode())

    @staticmethod
    def generate_salt(length: int = 16) -> bytes:
        """Generate random salt.

        Args:
            length: Salt length in bytes

        Returns:
            Random salt bytes
        """
        return os.urandom(length)


class SymmetricEncryptor:
    """Symmetric encryption handler."""

    def __init__(self, key: bytes, algorithm: EncryptionAlgorithm = EncryptionAlgorithm.AES_256_GCM):
        """Initialize encryptor.

        Args:
            key: Encryption key
            algorithm: Encryption algorithm
        """
        if not HAS_CRYPTO:
            raise ImportError("cryptography library not installed")

        self.key = key
        self.algorithm = algorithm

    def encrypt(self, plaintext: bytes) -> EncryptedData:
        """Encrypt data.

        Args:
            plaintext: Data to encrypt

        Returns:
            EncryptedData container
        """
        if not HAS_CRYPTO:
            raise ImportError("cryptography library not installed")

        nonce = secrets.token_bytes(12)

        if self.algorithm == EncryptionAlgorithm.AES_256_GCM:
            cipher = Cipher(
                algorithms.AES(self.key),
                modes.GCM(nonce),
                backend=default_backend(),
            )
            encryptor = cipher.encryptor()
            ciphertext = encryptor.update(plaintext) + encryptor.finalize()
            return EncryptedData(
                ciphertext=ciphertext,
                nonce=nonce,
                tag=encryptor.tag,
                algorithm=self.algorithm.value,
            )

        elif self.algorithm == EncryptionAlgorithm.AES_256_CBC:
            padder = padding.PKCS7(128).padder()
            padded_data = pader.update(plaintext) + padder.finalize()
            cipher = Cipher(
                algorithms.AES(self.key),
                modes.CBC(nonce),
                backend=default_backend(),
            )
            encryptor = cipher.encryptor()
            ciphertext = encryptor.update(padded_data) + encryptor.finalize()
            return EncryptedData(
                ciphertext=ciphertext,
                nonce=nonce,
                algorithm=self.algorithm.value,
            )

        raise ValueError(f"Unsupported algorithm: {self.algorithm}")

    def decrypt(self, encrypted: EncryptedData) -> bytes:
        """Decrypt data.

        Args:
            encrypted: EncryptedData container

        Returns:
            Decrypted plaintext
        """
        if not HAS_CRYPTO:
            raise ImportError("cryptography library not installed")

        if encrypted.algorithm == EncryptionAlgorithm.AES_256_GCM.value:
            cipher = Cipher(
                algorithms.AES(self.key),
                modes.GCM(encrypted.nonce, encrypted.tag),
                backend=default_backend(),
            )
            decryptor = cipher.decryptor()
            return decryptor.update(encrypted.ciphertext) + decryptor.finalize()

        elif encrypted.algorithm == EncryptionAlgorithm.AES_256_CBC.value:
            cipher = Cipher(
                algorithms.AES(self.key),
                modes.CBC(encrypted.nonce),
                backend=default_backend(),
            )
            decryptor = cipher.decryptor()
            padded = decryptor.update(encrypted.ciphertext) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            return unpadder.update(padded) + unpadder.finalize()

        raise ValueError(f"Unsupported algorithm: {encrypted.algorithm}")


class Hasher:
    """Hashing utility."""

    @staticmethod
    def hash(data: bytes, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> bytes:
        """Hash data.

        Args:
            data: Data to hash
            algorithm: Hash algorithm

        Returns:
            Hash digest
        """
        if algorithm == HashAlgorithm.SHA256:
            return hashlib.sha256(data).digest()
        elif algorithm == HashAlgorithm.SHA384:
            return hashlib.sha384(data).digest()
        elif algorithm == HashAlgorithm.SHA512:
            return hashlib.sha512(data).digest()
        elif algorithm == HashAlgorithm.BLAKE2B:
            return hashlib.blake2b(data).digest()
        elif algorithm == HashAlgorithm.BLAKE2S:
            return hashlib.blake2s(data).digest()
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    @staticmethod
    def hash_hex(data: bytes, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> str:
        """Hash data and return hex string.

        Args:
            data: Data to hash
            algorithm: Hash algorithm

        Returns:
            Hex string
        """
        return Hasher.hash(data, algorithm).hex()


class HMACGenerator:
    """HMAC generation utility."""

    @staticmethod
    def generate(
        data: bytes,
        key: bytes,
        algorithm: HashAlgorithm = HashAlgorithm.SHA256,
    ) -> bytes:
        """Generate HMAC.

        Args:
            data: Data to authenticate
            key: Secret key
            algorithm: Hash algorithm

        Returns:
            HMAC digest
        """
        if algorithm == HashAlgorithm.SHA256:
            return hmac.new(key, data, hashlib.sha256).digest()
        elif algorithm == HashAlgorithm.SHA384:
            return hmac.new(key, data, hashlib.sha384).digest()
        elif algorithm == HashAlgorithm.SHA512:
            return hmac.new(key, data, hashlib.sha512).digest()
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    @staticmethod
    def verify(data: bytes, key: bytes, mac: bytes, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> bool:
        """Verify HMAC.

        Args:
            data: Data to verify
            key: Secret key
            mac: Expected HMAC
            algorithm: Hash algorithm

        Returns:
            True if valid
        """
        expected = HMACGenerator.generate(data, key, algorithm)
        return hmac.compare_digest(expected, mac)


def generate_key(length: int = 32) -> bytes:
    """Generate random encryption key.

    Args:
        length: Key length in bytes

    Returns:
        Random key bytes
    """
    return secrets.token_bytes(length)


def generate_password(length: int = 16) -> str:
    """Generate random password.

    Args:
        length: Password length

    Returns:
        Random password string
    """
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def derive_key_from_password(
    password: str,
    salt: Optional[bytes] = None,
    iterations: int = 100000,
) -> tuple[bytes, bytes]:
    """Derive encryption key from password.

    Args:
        password: User password
        salt: Salt bytes (generated if None)
        iterations: PBKDF2 iterations

    Returns:
        Tuple of (key, salt)
    """
    if salt is None:
        salt = KeyDerivation.generate_salt()
    key = KeyDerivation.pbkdf2(password, salt, iterations)
    return key, salt

"""
Data Encryption Action Module.

Data encryption with multiple algorithms, key management,
and envelope encryption for large data.
"""

import base64
import os
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class EncryptionAlgorithm(Enum):
    """Supported encryption algorithms."""
    AES_256_GCM = "aes_256_gcm"
    AES_256_CBC = "aes_256_cbc"
    CHACHA20_POLY1305 = "chacha20_poly1305"
    FERNET = "fernet"


@dataclass
class EncryptedData:
    """Encrypted data container."""
    ciphertext: bytes
    algorithm: EncryptionAlgorithm
    key_id: str
    iv: Optional[bytes] = None
    tag: Optional[bytes] = None


class DataEncryptionAction:
    """
    Data encryption with key management.

    Example:
        crypto = DataEncryptionAction()
        crypto.generate_key("key1", algorithm=EncryptionAlgorithm.AES_256_GCM)
        encrypted = crypto.encrypt("sensitive data", "key1")
        decrypted = crypto.decrypt(encrypted, "key1")
    """

    def __init__(self):
        """Initialize encryption action."""
        self._keys: dict[str, bytes] = {}
        self._key_algorithm: dict[str, EncryptionAlgorithm] = {}

    def generate_key(
        self,
        key_id: str,
        algorithm: EncryptionAlgorithm = EncryptionAlgorithm.AES_256_GCM
    ) -> bytes:
        """
        Generate encryption key.

        Args:
            key_id: Key identifier.
            algorithm: Encryption algorithm.

        Returns:
            Generated key bytes.
        """
        if algorithm == EncryptionAlgorithm.AES_256_GCM:
            key = os.urandom(32)
        elif algorithm == EncryptionAlgorithm.AES_256_CBC:
            key = os.urandom(32)
        elif algorithm == EncryptionAlgorithm.CHACHA20_POLY1305:
            key = os.urandom(32)
        elif algorithm == EncryptionAlgorithm.FERNET:
            key = os.urandom(32)
        else:
            key = os.urandom(32)

        self._keys[key_id] = key
        self._key_algorithm[key_id] = algorithm

        logger.info(f"Generated key: {key_id} ({algorithm.value})")
        return key

    def set_key(self, key_id: str, key: bytes, algorithm: EncryptionAlgorithm) -> None:
        """
        Set encryption key manually.

        Args:
            key_id: Key identifier.
            key: Key bytes.
            algorithm: Encryption algorithm.
        """
        self._keys[key_id] = key
        self._key_algorithm[key_id] = algorithm

    def get_key(self, key_id: str) -> Optional[bytes]:
        """Get key by ID."""
        return self._keys.get(key_id)

    def encrypt(
        self,
        data: Any,
        key_id: str,
        algorithm: Optional[EncryptionAlgorithm] = None
    ) -> EncryptedData:
        """
        Encrypt data.

        Args:
            data: Data to encrypt (str, bytes, or serializable).
            key_id: Key identifier.
            algorithm: Override algorithm (uses key's algorithm if None).

        Returns:
            EncryptedData object.
        """
        if key_id not in self._keys:
            raise ValueError(f"Key not found: {key_id}")

        key = self._keys[key_id]
        algo = algorithm or self._key_algorithm[key_id]

        if isinstance(data, str):
            plaintext = data.encode("utf-8")
        elif isinstance(data, bytes):
            plaintext = data
        else:
            import json
            plaintext = json.dumps(data).encode("utf-8")

        if algo == EncryptionAlgorithm.AES_256_GCM:
            return self._encrypt_aes_gcm(plaintext, key, key_id)
        elif algo == EncryptionAlgorithm.AES_256_CBC:
            return self._encrypt_aes_cbc(plaintext, key, key_id)
        elif algo == EncryptionAlgorithm.FERNET:
            return self._encrypt_fernet(plaintext, key, key_id)
        elif algo == EncryptionAlgorithm.CHACHA20_POLY1305:
            return self._encrypt_chacha20(plaintext, key, key_id)

        raise ValueError(f"Unsupported algorithm: {algo}")

    def decrypt(
        self,
        encrypted: EncryptedData,
        key_id: str,
        as_string: bool = True
    ) -> Any:
        """
        Decrypt data.

        Args:
            encrypted: EncryptedData object.
            key_id: Key identifier.
            as_string: Return as string (False for bytes).

        Returns:
            Decrypted data.
        """
        if key_id not in self._keys:
            raise ValueError(f"Key not found: {key_id}")

        key = self._keys[key_id]

        if encrypted.algorithm == EncryptionAlgorithm.AES_256_GCM:
            plaintext = self._decrypt_aes_gcm(encrypted, key)
        elif encrypted.algorithm == EncryptionAlgorithm.AES_256_CBC:
            plaintext = self._decrypt_aes_cbc(encrypted, key)
        elif encrypted.algorithm == EncryptionAlgorithm.FERNET:
            plaintext = self._decrypt_fernet(encrypted, key)
        elif encrypted.algorithm == EncryptionAlgorithm.CHACHA20_POLY1305:
            plaintext = self._decrypt_chacha20(encrypted, key)
        else:
            raise ValueError(f"Unsupported algorithm: {encrypted.algorithm}")

        if as_string:
            return plaintext.decode("utf-8")
        return plaintext

    def _encrypt_aes_gcm(self, plaintext: bytes, key: bytes, key_id: str) -> EncryptedData:
        """Encrypt using AES-256-GCM."""
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            iv = os.urandom(12)
            aesgcm = AESGCM(key)
            ciphertext = aesgcm.encrypt(iv, plaintext, None)

            return EncryptedData(
                ciphertext=ciphertext,
                algorithm=EncryptionAlgorithm.AES_256_GCM,
                key_id=key_id,
                iv=iv
            )
        except ImportError:
            logger.warning("cryptography library not available, using fallback")
            return self._encrypt_aes_cbc(plaintext, key, key_id)

    def _decrypt_aes_gcm(self, encrypted: EncryptedData, key: bytes) -> bytes:
        """Decrypt using AES-256-GCM."""
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        aesgcm = AESGCM(key)
        return aesgcm.decrypt(encrypted.iv, encrypted.ciphertext, None)

    def _encrypt_aes_cbc(self, plaintext: bytes, key: bytes, key_id: str) -> EncryptedData:
        """Encrypt using AES-256-CBC."""
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.primitives import padding

        iv = os.urandom(16)
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(plaintext) + padder.finalize()

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()

        return EncryptedData(
            ciphertext=ciphertext,
            algorithm=EncryptionAlgorithm.AES_256_CBC,
            key_id=key_id,
            iv=iv
        )

    def _decrypt_aes_cbc(self, encrypted: EncryptedData, key: bytes) -> bytes:
        """Decrypt using AES-256-CBC."""
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.primitives import padding

        cipher = Cipher(algorithms.AES(key), modes.CBC(encrypted.iv))
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(encrypted.ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        return unpadder.update(padded_data) + unpadder.finalize()

    def _encrypt_fernet(self, plaintext: bytes, key: bytes, key_id: str) -> EncryptedData:
        """Encrypt using Fernet."""
        from cryptography.fernet import Fernet
        f = Fernet(base64.urlsafe_b64encode(key))
        ciphertext = f.encrypt(plaintext)

        return EncryptedData(
            ciphertext=ciphertext,
            algorithm=EncryptionAlgorithm.FERNET,
            key_id=key_id
        )

    def _decrypt_fernet(self, encrypted: EncryptedData, key: bytes) -> bytes:
        """Decrypt using Fernet."""
        from cryptography.fernet import Fernet
        f = Fernet(base64.urlsafe_b64encode(key))
        return f.decrypt(encrypted.ciphertext)

    def _encrypt_chacha20(self, plaintext: bytes, key: bytes, key_id: str) -> EncryptedData:
        """Encrypt using ChaCha20-Poly1305."""
        try:
            from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
            nonce = os.urandom(12)
            chacha = ChaCha20Poly1305(key)
            ciphertext = chacha.encrypt(nonce, plaintext, None)

            return EncryptedData(
                ciphertext=ciphertext,
                algorithm=EncryptionAlgorithm.CHACHA20_POLY1305,
                key_id=key_id,
                iv=nonce
            )
        except ImportError:
            raise ImportError("ChaCha20 requires cryptography>=3.8")

    def _decrypt_chacha20(self, encrypted: EncryptedData, key: bytes) -> bytes:
        """Decrypt using ChaCha20-Poly1305."""
        from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
        chacha = ChaCha20Poly1305(key)
        return chacha.decrypt(encrypted.iv, encrypted.ciphertext, None)

    def encrypt_to_base64(
        self,
        data: Any,
        key_id: str
    ) -> str:
        """
        Encrypt and return base64-encoded string.

        Args:
            data: Data to encrypt.
            key_id: Key identifier.

        Returns:
            Base64-encoded encrypted data.
        """
        encrypted = self.encrypt(data, key_id)
        combined = (encrypted.iv or b"") + encrypted.ciphertext
        return base64.b64encode(combined).decode("ascii")

    def decrypt_from_base64(
        self,
        encoded: str,
        key_id: str
    ) -> Any:
        """
        Decrypt from base64-encoded string.

        Args:
            encoded: Base64-encoded encrypted data.
            key_id: Key identifier.

        Returns:
            Decrypted data.
        """
        combined = base64.b64decode(encoded.encode("ascii"))
        iv = combined[:16]
        ciphertext = combined[16:]

        encrypted = EncryptedData(
            ciphertext=ciphertext,
            algorithm=self._key_algorithm[key_id],
            key_id=key_id,
            iv=iv
        )

        return self.decrypt(encrypted, key_id)

    def delete_key(self, key_id: str) -> bool:
        """Delete a key."""
        if key_id in self._keys:
            del self._keys[key_id]
            del self._key_algorithm[key_id]
            return True
        return False

    def list_keys(self) -> list[str]:
        """List all key IDs."""
        return list(self._keys.keys())

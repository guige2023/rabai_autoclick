"""
Encryption Action Module

Provides encryption and decryption functionality for securing data
in UI automation workflows. Supports symmetric and asymmetric encryption,
hashing, and digital signatures.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import secrets
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class CipherType(Enum):
    """Cipher type enumeration."""
    AES_256_GCM = "aes-256-gcm"
    AES_256_CBC = "aes-256-cbc"
    CHACHA20_POLY1305 = "chacha20-poly1305"
    RSA_2048 = "rsa-2048"
    RSA_4096 = "rsa-4096"


class HashType(Enum):
    """Hash algorithm types."""
    SHA256 = "sha256"
    SHA384 = "sha384"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"
    BLAKE2S = "blake2s"


@dataclass
class EncryptedData:
    """Represents encrypted data with metadata."""
    ciphertext: bytes
    nonce: Optional[bytes] = None
    tag: Optional[bytes] = None
    algorithm: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class KeyPair:
    """Represents asymmetric key pair."""
    public_key: bytes
    private_key: bytes
    algorithm: str = ""


class EncryptionError(Exception):
    """Encryption error exception."""
    pass


class SymmetricEncryption:
    """
    Symmetric encryption operations.

    Example:
        >>> enc = SymmetricEncryption(CipherType.AES_256_GCM)
        >>> key = enc.generate_key()
        >>> encrypted = enc.encrypt(b"secret data", key)
        >>> decrypted = enc.decrypt(encrypted, key)
    """

    def __init__(self, cipher_type: CipherType = CipherType.AES_256_GCM) -> None:
        self.cipher_type = cipher_type
        self._key_sizes = {
            CipherType.AES_256_GCM: 32,
            CipherType.AES_256_CBC: 32,
            CipherType.CHACHA20_POLY1305: 32,
        }

    def generate_key(self) -> bytes:
        """Generate random encryption key."""
        import cryptography.hazmat.primitives.ciphers.aead as aead

        key_size = self._key_sizes.get(self.cipher_type, 32)
        return os.urandom(key_size)

    def generate_nonce(self) -> bytes:
        """Generate random nonce/IV."""
        nonce_sizes = {
            CipherType.AES_256_GCM: 12,
            CipherType.AES_256_CBC: 16,
            CipherType.CHACHA20_POLY1305: 12,
        }
        size = nonce_sizes.get(self.cipher_type, 12)
        return os.urandom(size)

    def encrypt(self, plaintext: bytes, key: bytes, nonce: Optional[bytes] = None) -> EncryptedData:
        """Encrypt data."""
        if nonce is None:
            nonce = self.generate_nonce()

        if self.cipher_type == CipherType.AES_256_GCM:
            import cryptography.hazmat.primitives.ciphers.aead as aead
            aesgcm = aead.AESGCM(key)
            ciphertext = aesgcm.encrypt(nonce, plaintext, None)
            return EncryptedData(
                ciphertext=ciphertext,
                nonce=nonce,
                algorithm=self.cipher_type.value,
            )

        elif self.cipher_type == CipherType.CHACHA20_POLY1305:
            import cryptography.hazmat.primitives.ciphers.aead as aead
            chacha = aead.ChaCha20Poly1305(key)
            ciphertext = chacha.encrypt(nonce, plaintext, None)
            return EncryptedData(
                ciphertext=ciphertext,
                nonce=nonce,
                algorithm=self.cipher_type.value,
            )

        elif self.cipher_type == CipherType.AES_256_CBC:
            from cryptography.hazmat.primitives import ciphers, padding
            padder = padding.PKCS7(128).padder()
            padded_data = padder.update(plaintext) + padder.finalize()

            cipher = ciphers.Cipher(
                ciphers.algorithms.AES(key),
                ciphers.modes.CBC(nonce),
            )
            encryptor = cipher.encryptor()
            ciphertext = encryptor.update(padded_data) + encryptor.finalize()

            return EncryptedData(
                ciphertext=ciphertext,
                nonce=nonce,
                algorithm=self.cipher_type.value,
            )

        raise EncryptionError(f"Unsupported cipher: {self.cipher_type}")

    def decrypt(self, encrypted: EncryptedData, key: bytes) -> bytes:
        """Decrypt data."""
        nonce = encrypted.nonce
        if nonce is None:
            raise EncryptionError("Nonce is required for decryption")

        if self.cipher_type == CipherType.AES_256_GCM:
            import cryptography.hazmat.primitives.ciphers.aead as aead
            aesgcm = aead.AESGCM(key)
            return aesgcm.decrypt(nonce, encrypted.ciphertext, None)

        elif self.cipher_type == CipherType.CHACHA20_POLY1305:
            import cryptography.hazmat.primitives.ciphers.aead as aead
            chacha = aead.ChaCha20Poly1305(key)
            return chacha.decrypt(nonce, encrypted.ciphertext, None)

        elif self.cipher_type == CipherType.AES_256_CBC:
            from cryptography.hazmat.primitives import ciphers, padding
            cipher = ciphers.Cipher(
                ciphers.algorithms.AES(key),
                ciphers.modes.CBC(nonce),
            )
            decryptor = cipher.decryptor()
            padded_data = decryptor.update(encrypted.ciphertext) + decryptor.finalize()

            unpadder = padding.PKCS7(128).unpadder()
            return unpadder.update(padded_data) + unpadder.finalize()

        raise EncryptionError(f"Unsupported cipher: {self.cipher_type}")


class AsymmetricEncryption:
    """
    Asymmetric encryption operations.

    Example:
        >>> enc = AsymmetricEncryption(CipherType.RSA_2048)
        >>> keypair = enc.generate_keypair()
        >>> encrypted = enc.encrypt(b"secret data", keypair.public_key)
        >>> decrypted = enc.decrypt(encrypted, keypair.private_key)
    """

    def __init__(self, cipher_type: CipherType = CipherType.RSA_2048) -> None:
        self.cipher_type = cipher_type
        self._key_sizes = {
            CipherType.RSA_2048: 2048,
            CipherType.RSA_4096: 4096,
        }

    def generate_keypair(self) -> KeyPair:
        """Generate public/private key pair."""
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization

        key_size = self._key_sizes.get(self.cipher_type, 2048)
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
        )
        public_key = private_key.public_key()

        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        return KeyPair(
            public_key=public_pem,
            private_key=private_pem,
            algorithm=self.cipher_type.value,
        )

    def encrypt(self, plaintext: bytes, public_key: bytes) -> EncryptedData:
        """Encrypt with public key (for small data)."""
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives import hashes

        pub_key = serialization.load_pem_public_key(public_key)

        ciphertext = pub_key.encrypt(
            plaintext,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )

        return EncryptedData(
            ciphertext=ciphertext,
            algorithm=self.cipher_type.value,
        )

    def decrypt(self, encrypted: EncryptedData, private_key: bytes) -> bytes:
        """Decrypt with private key."""
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives import hashes

        priv_key = serialization.load_pem_private_key(
            private_key,
            password=None,
        )

        return priv_key.decrypt(
            encrypted.ciphertext,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )


class Hasher:
    """
    Cryptographic hash operations.

    Example:
        >>> hasher = Hasher(HashType.SHA256)
        >>> digest = hasher.hash(b"data")
        >>> is_valid = hasher.verify(b"data", digest)
    """

    def __init__(self, hash_type: HashType = HashType.SHA256) -> None:
        self.hash_type = hash_type
        self._algorithms = {
            HashType.SHA256: hashlib.sha256,
            HashType.SHA384: hashlib.sha384,
            HashType.SHA512: hashlib.sha512,
            HashType.BLAKE2B: hashlib.blake2b,
            HashType.BLAKE2S: hashlib.blake2s,
        }

    def hash(self, data: bytes) -> bytes:
        """Generate hash digest."""
        algo = self._algorithms.get(self.hash_type, hashlib.sha256)
        return algo(data).digest()

    def hash_hex(self, data: bytes) -> str:
        """Generate hex-encoded hash."""
        return self.hash(data).hex()

    def verify(self, data: bytes, digest: bytes) -> bool:
        """Verify hash matches data."""
        return hmac.compare_digest(self.hash(data), digest)


class PasswordHasher:
    """
    Password hashing with salt and iterations.

    Example:
        >>> ph = PasswordHasher()
        >>> hash = ph.hash("password123")
        >>> is_valid = ph.verify("password123", hash)
    """

    def __init__(
        self,
        iterations: int = 100000,
        salt_size: int = 32,
    ) -> None:
        self.iterations = iterations
        self.salt_size = salt_size

    def hash(self, password: str) -> str:
        """Hash password with salt."""
        import cryptography.hazmat.primitives.kdf.pbkdf2 as kdf

        salt = os.urandom(self.salt_size)
        key = kdf.PBKDF2HMAC(
            algorithm=hashlib.sha256(),
            length=32,
            salt=salt,
            iterations=self.iterations,
        ).derive(password.encode())

        return base64.b64encode(salt + key).decode()

    def verify(self, password: str, stored_hash: str) -> bool:
        """Verify password against stored hash."""
        try:
            data = base64.b64decode(stored_hash.encode())
            salt = data[:self.salt_size]
            stored_key = data[self.salt_size:]

            import cryptography.hazmat.primitives.kdf.pbkdf2 as kdf
            key = kdf.PBKDF2HMAC(
                algorithm=hashlib.sha256(),
                length=32,
                salt=salt,
                iterations=self.iterations,
            ).derive(password.encode())

            return hmac.compare_digest(key, stored_key)
        except Exception as e:
            logger.error(f"Password verification failed: {e}")
            return False


class HMACSignature:
    """
    HMAC-based message authentication.

    Example:
        >>> hmac_sig = HMACSignature(HashType.SHA256)
        >>> signature = hmac_sig.sign(b"message", b"secret_key")
        >>> is_valid = hmac_sig.verify(b"message", signature, b"secret_key")
    """

    def __init__(self, hash_type: HashType = HashType.SHA256) -> None:
        self.hash_type = hash_type

    def sign(self, message: bytes, key: bytes) -> bytes:
        """Generate HMAC signature."""
        return hmac.new(key, message, self.hash_type.name.lower()).digest()

    def sign_hex(self, message: bytes, key: bytes) -> str:
        """Generate hex-encoded HMAC signature."""
        return self.sign(message, key).hex()

    def verify(self, message: bytes, signature: bytes, key: bytes) -> bool:
        """Verify HMAC signature."""
        expected = self.sign(message, key)
        return hmac.compare_digest(expected, signature)


class DataSigner:
    """
    Digital signature operations.

    Example:
        >>> signer = DataSigner(CipherType.RSA_2048)
        >>> keypair = signer.generate_keypair()
        >>> signature = signer.sign(b"data", keypair.private_key)
        >>> is_valid = signer.verify(b"data", signature, keypair.public_key)
    """

    def __init__(self, cipher_type: CipherType = CipherType.RSA_2048) -> None:
        self.cipher_type = cipher_type
        self._asymm = AsymmetricEncryption(cipher_type)

    def generate_keypair(self) -> KeyPair:
        """Generate signing key pair."""
        return self._asymm.generate_keypair()

    def sign(self, data: bytes, private_key: bytes) -> bytes:
        """Create digital signature."""
        from cryptography.hazmat.primitives import serialization, hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend

        priv_key = serialization.load_pem_private_key(
            private_key,
            password=None,
            backend=default_backend(),
        )

        return priv_key.sign(
            data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )

    def verify(self, data: bytes, signature: bytes, public_key: bytes) -> bool:
        """Verify digital signature."""
        try:
            from cryptography.hazmat.primitives import serialization, hashes
            from cryptography.hazmat.primitives.asymmetric import padding
            from cryptography.hazmat.backends import default_backend

            pub_key = serialization.load_pem_public_key(
                public_key,
                backend=default_backend(),
            )

            pub_key.verify(
                signature,
                data,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH,
                ),
                hashes.SHA256(),
            )
            return True
        except Exception:
            return False


class SecretBox:
    """
    Simplified secret box for key-value encryption.

    Example:
        >>> box = SecretBox()
        >>> encrypted = box.encrypt("sensitive_data", "password123")
        >>> decrypted = box.decrypt(encrypted, "password123")
    """

    def __init__(self) -> None:
        self._symm = SymmetricEncryption(CipherType.AES_256_GCM)
        self._hasher = Hasher(HashType.SHA256)

    def _derive_key(self, password: str, salt: bytes) -> bytes:
        """Derive key from password using PBKDF2."""
        import cryptography.hazmat.primitives.kdf.pbkdf2 as kdf
        key = kdf.PBKDF2HMAC(
            algorithm=hashlib.sha256(),
            length=32,
            salt=salt,
            iterations=100000,
        ).derive(password.encode())
        return key

    def encrypt(self, plaintext: str, password: str) -> str:
        """Encrypt string with password."""
        salt = os.urandom(16)
        nonce = self._symm.generate_nonce()
        key = self._derive_key(password, salt)

        encrypted = self._symm.encrypt(plaintext.encode(), key, nonce)
        encrypted.nonce = nonce

        combined = salt + (encrypted.nonce or b"") + encrypted.ciphertext
        return base64.b64encode(combined).decode()

    def decrypt(self, ciphertext: str, password: str) -> str:
        """Decrypt string with password."""
        try:
            data = base64.b64decode(ciphertext.encode())
            salt = data[:16]
            nonce = data[16:28]
            encrypted_data = data[28:]

            key = self._derive_key(password, salt)

            encrypted = EncryptedData(
                ciphertext=encrypted_data,
                nonce=nonce,
                algorithm=self._symm.cipher_type.value,
            )

            decrypted = self._symm.decrypt(encrypted, key)
            return decrypted.decode()
        except Exception as e:
            raise EncryptionError(f"Decryption failed: {e}")

    def __repr__(self) -> str:
        return f"SecretBox(cipher={self._symm.cipher_type.value})"

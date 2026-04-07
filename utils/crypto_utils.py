"""Cryptographic utilities for RabAI AutoClick.

Provides:
- Key generation (AES, RSA)
- Encryption/decryption helpers
- Secure random generation
- HMAC and signature utilities
"""

import hashlib
import hmac
import os
import secrets
import base64
from typing import Optional, Union

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    HAS_CRYPTOGRAPHY = True
except ImportError:
    HAS_CRYPTOGRAPHY = False


def generate_random_key(length: int = 32) -> bytes:
    """Generate a cryptographically secure random key.

    Args:
        length: Key length in bytes.

    Returns:
        Random bytes of the specified length.
    """
    return secrets.token_bytes(length)


def generate_random_hex(length: int = 32) -> str:
    """Generate a random hex string.

    Args:
        length: Length of hex string (number of hex chars, not bytes).

    Returns:
        Random hex string.
    """
    return secrets.token_hex(length)


def generate_token_urlsafe(length: int = 32) -> str:
    """Generate a URL-safe random token.

    Args:
        length: Length in bytes.

    Returns:
        URL-safe base64 encoded token.
    """
    return base64.urlsafe_b64encode(secrets.token_bytes(length)).rstrip(b"=").decode()


def generate_password(length: int = 16) -> str:
    """Generate a secure random password.

    Args:
        length: Password length.

    Returns:
        Random password string.
    """
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
    return "".join(secrets.choice(chars) for _ in range(length))


if HAS_CRYPTOGRAPHY:
    def generate_aes_key(key_size: int = 256) -> bytes:
        """Generate an AES key.

        Args:
            key_size: Key size in bits (128, 192, or 256).

        Returns:
            AES key bytes.
        """
        if key_size not in (128, 192, 256):
            raise ValueError("key_size must be 128, 192, or 256")
        key_bytes = key_size // 8
        return secrets.token_bytes(key_bytes)

    def aes_encrypt(
        plaintext: bytes,
        key: bytes,
        *,
        mode: str = "CBC",
    ) -> tuple[bytes, bytes]:
        """Encrypt data with AES.

        Args:
            plaintext: Data to encrypt.
            key: AES key (16, 24, or 32 bytes).
            mode: Cipher mode ("CBC" or "GCM").

        Returns:
            Tuple of (ciphertext, iv/nonce).
        """
        if len(key) == 16:
            algo = algorithms.AES(key)
        elif len(key) == 24:
            algo = algorithms.AES(key)
        elif len(key) == 32:
            algo = algorithms.AES(key)
        else:
            raise ValueError("Key must be 16, 24, or 32 bytes")

        if mode == "CBC":
            iv = secrets.token_bytes(16)
            cipher = Cipher(algo, modes.CBC(iv), backend=default_backend())
        elif mode == "GCM":
            iv = secrets.token_bytes(12)
            cipher = Cipher(algo, modes.GCM(iv), backend=default_backend())
        else:
            raise ValueError("Mode must be CBC or GCM")

        encryptor = cipher.encryptor()
        ct = encryptor.update(plaintext) + encryptor.finalize()
        return ct, iv

    def aes_decrypt(
        ciphertext: bytes,
        key: bytes,
        iv: bytes,
        *,
        mode: str = "CBC",
    ) -> bytes:
        """Decrypt data with AES.

        Args:
            ciphertext: Data to decrypt.
            key: AES key.
            iv: Initialization vector/nonce.
            mode: Cipher mode ("CBC" or "GCM").

        Returns:
            Decrypted plaintext.
        """
        if len(key) == 16 or len(key) == 24 or len(key) == 32:
            algo = algorithms.AES(key)
        else:
            raise ValueError("Key must be 16, 24, or 32 bytes")

        if mode == "CBC":
            cipher = Cipher(algo, modes.CBC(iv), backend=default_backend())
        elif mode == "GCM":
            cipher = Cipher(algo, modes.GCM(iv), backend=default_backend())
        else:
            raise ValueError("Mode must be CBC or GCM")

        decryptor = cipher.decryptor()
        return decryptor.update(ciphertext) + decryptor.finalize()

    def generate_rsa_keypair(
        key_size: int = 2048,
    ) -> tuple:
        """Generate an RSA key pair.

        Args:
            key_size: RSA key size in bits (2048 or 4096).

        Returns:
            Tuple of (private_key, public_key).
        """
        if key_size not in (2048, 4096):
            raise ValueError("key_size must be 2048 or 4096")

        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=key_size,
        )
        public_key = private_key.public_key()
        return private_key, public_key

    def rsa_encrypt(
        plaintext: bytes,
        public_key: Any,
    ) -> bytes:
        """Encrypt data with RSA.

        Args:
            plaintext: Data to encrypt (must be small, < key_size//8 - 11 bytes).
            public_key: RSA public key.

        Returns:
            Encrypted data.
        """
        return public_key.encrypt(
            plaintext,
            padding.PKCS1v15(),
        )

    def rsa_decrypt(
        ciphertext: bytes,
        private_key: Any,
    ) -> bytes:
        """Decrypt data with RSA.

        Args:
            ciphertext: Encrypted data.
            private_key: RSA private key.

        Returns:
            Decrypted plaintext.
        """
        return private_key.decrypt(
            ciphertext,
            padding.PKCS1v15(),
        )

    def rsa_sign(
        data: bytes,
        private_key: Any,
    ) -> bytes:
        """Sign data with RSA.

        Args:
            data: Data to sign.
            private_key: RSA private key.

        Returns:
            Signature bytes.
        """
        return private_key.sign(
            data,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )

    def rsa_verify(
        data: bytes,
        signature: bytes,
        public_key: Any,
    ) -> bool:
        """Verify an RSA signature.

        Args:
            data: Original data.
            signature: Signature to verify.
            public_key: RSA public key.

        Returns:
            True if signature is valid.
        """
        try:
            public_key.verify(
                signature,
                data,
                padding.PKCS1v15(),
                hashes.SHA256(),
            )
            return True
        except Exception:
            return False

    def derive_key_pbkdf2(
        password: str,
        salt: bytes,
        length: int = 32,
        iterations: int = 100000,
    ) -> bytes:
        """Derive a key using PBKDF2-HMAC.

        Args:
            password: Password string.
            salt: Salt bytes.
            length: Desired key length in bytes.
            iterations: Number of PBKDF2 iterations.

        Returns:
            Derived key bytes.
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=length,
            salt=salt,
            iterations=iterations,
        )
        return kdf.derive(password.encode())

    def serialize_private_key(private_key: Any, password: Optional[str] = None) -> bytes:
        """Serialize a private key to PEM format.

        Args:
            private_key: RSA private key.
            password: Optional encryption password.

        Returns:
            PEM-encoded key bytes.
        """
        encryption = serialization.BestAvailableEncryption(password.encode()) if password else serialization.NoEncryption()
        return private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=encryption,
        )

    def serialize_public_key(public_key: Any) -> bytes:
        """Serialize a public key to PEM format.

        Args:
            public_key: RSA public key.

        Returns:
            PEM-encoded key bytes.
        """
        return public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )


def hmac_sha256(data: bytes, key: bytes) -> bytes:
    """Compute HMAC-SHA256.

    Args:
        data: Data to authenticate.
        key: HMAC key.

    Returns:
        HMAC bytes.
    """
    return hmac.new(key, data, hashlib.sha256).digest()


def hmac_sha256_hex(data: bytes, key: bytes) -> str:
    """Compute HMAC-SHA256 as hex string.

    Args:
        data: Data to authenticate.
        key: HMAC key.

    Returns:
        HMAC hex string.
    """
    return hmac.new(key, data, hashlib.sha256).hexdigest()


def constant_time_compare(a: bytes, b: bytes) -> bool:
    """Compare two values in constant time.

    Prevents timing attacks.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if equal.
    """
    return hmac.compare_digest(a, b)


def constant_time_compare_str(a: str, b: str) -> bool:
    """Compare two strings in constant time.

    Args:
        a: First string.
        b: Second string.

    Returns:
        True if equal.
    """
    return hmac.compare_digest(a.encode(), b.encode())


def secure_compare(a: Union[bytes, str], b: Union[bytes, str]) -> bool:
    """Secure comparison that prevents timing attacks.

    Args:
        a: First value.
        b: Second value.

    Returns:
        True if equal.
    """
    if isinstance(a, str):
        a = a.encode()
    if isinstance(b, str):
        b = b.encode()
    return constant_time_compare(a, b)

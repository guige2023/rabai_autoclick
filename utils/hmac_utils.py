"""
HMAC (Hash-based Message Authentication Code) utilities.

Provides HMAC generation/verification, constant-time comparison,
and integration with various hash algorithms.
"""

from __future__ import annotations

import hashlib
import hmac as _hmac
import secrets
from typing import Literal


HashAlgorithm = Literal["sha1", "sha256", "sha512", "sha3_256", "sha3_512", "blake2b", "blake2s"]


def generate_hmac(
    message: bytes | str,
    key: bytes | str,
    algorithm: HashAlgorithm = "sha256",
) -> str:
    """
    Generate HMAC for a message.

    Args:
        message: Message to authenticate
        key: Secret key
        algorithm: Hash algorithm to use

    Returns:
        Hex-encoded HMAC digest

    Example:
        >>> generate_hmac("hello", "secret", "sha256")
        'a5c2b13f...'[:64] + '...'
    """
    if isinstance(message, str):
        message = message.encode("utf-8")
    if isinstance(key, str):
        key = key.encode("utf-8")

    return _hmac.new(key, message, getattr(hashlib, algorithm)).hexdigest()


def verify_hmac(
    message: bytes | str,
    mac: str,
    key: bytes | str,
    algorithm: HashAlgorithm = "sha256",
) -> bool:
    """
    Verify HMAC using constant-time comparison.

    Args:
        message: Original message
        mac: Expected HMAC hex digest
        key: Secret key
        algorithm: Hash algorithm used

    Returns:
        True if MAC is valid, False otherwise
    """
    expected = generate_hmac(message, key, algorithm)
    return secrets.compare_digest(expected, mac)


def generate_hmac_key(length: int = 32) -> str:
    """
    Generate a cryptographically random HMAC key.

    Args:
        length: Key length in bytes

    Returns:
        Hex-encoded random key
    """
    return secrets.token_hex(length)


def build_hmac_header(
    message: bytes | str,
    key: bytes | str,
    algorithm: HashAlgorithm = "sha256",
    header_name: str = "X-Signature",
) -> tuple[str, str]:
    """
    Build HMAC header for HTTP requests.

    Args:
        message: Message to sign
        key: Secret key
        algorithm: Hash algorithm
        header_name: HTTP header name

    Returns:
        Tuple of (header_name, header_value)
    """
    digest = generate_hmac(message, key, algorithm)
    return (header_name, f"{algorithm}={digest}")


class HMACVerifier:
    """Stateful HMAC verifier for request validation."""

    def __init__(
        self,
        key: bytes | str,
        algorithm: HashAlgorithm = "sha256",
        header_name: str = "X-Signature",
    ):
        self.key = key if isinstance(key, bytes) else key.encode("utf-8")
        self.algorithm = algorithm
        self.header_name = header_name

    def generate(self, message: bytes | str) -> str:
        return generate_hmac(message, self.key, self.algorithm)

    def verify(self, message: bytes | str, mac: str) -> bool:
        return verify_hmac(message, mac, self.key, self.algorithm)

    def verify_from_headers(
        self,
        message: bytes | str,
        headers: dict[str, str],
    ) -> bool:
        mac = headers.get(self.header_name, "")
        if not mac:
            return False
        algo_prefix = f"{self.algorithm}="
        if mac.startswith(algo_prefix):
            mac = mac[len(algo_prefix):]
        return self.verify(message, mac)


def hmac_sha256(message: bytes | str, key: bytes | str) -> str:
    """Convenience wrapper for HMAC-SHA256."""
    return generate_hmac(message, key, "sha256")


def hmac_sha512(message: bytes | str, key: bytes | str) -> str:
    """Convenience wrapper for HMAC-SHA512."""
    return generate_hmac(message, key, "sha512")


def create_signature_chain(
    messages: list[bytes | str],
    key: bytes | str,
    algorithm: HashAlgorithm = "sha256",
) -> list[str]:
    """
    Create a chain of HMAC signatures where each message signs the previous.

    Args:
        messages: List of messages to chain
        key: Base secret key (will be extended per step)
        algorithm: Hash algorithm

    Returns:
        List of HMAC signatures, one per message
    """
    signatures = []
    current_key = key if isinstance(key, bytes) else key.encode("utf-8")

    for msg in messages:
        msg_bytes = msg if isinstance(msg, bytes) else msg.encode("utf-8")
        sig = generate_hmac(msg_bytes, current_key, algorithm)
        signatures.append(sig)
        current_key = sig.encode("utf-8")

    return signatures

"""
HMAC (Hash-based Message Authentication Code) actions.
"""
from __future__ import annotations

import hmac as hmac_module
import hashlib
from typing import Optional, Union


def compute_hmac(
    message: Union[str, bytes],
    key: Union[str, bytes],
    algorithm: str = 'sha256'
) -> str:
    """
    Compute HMAC for a message.

    Args:
        message: The message to authenticate.
        key: The secret key.
        algorithm: Hash algorithm ('md5', 'sha1', 'sha256', 'sha384', 'sha512').

    Returns:
        Hexadecimal HMAC string.

    Raises:
        ValueError: If algorithm is not supported.
    """
    if isinstance(message, str):
        message = message.encode('utf-8')
    if isinstance(key, str):
        key = key.encode('utf-8')

    algorithm = algorithm.lower().replace('-', '').replace('_', '')

    hash_map = {
        'md5': hashlib.md5,
        'sha1': hashlib.sha1,
        'sha256': hashlib.sha256,
        'sha384': hashlib.sha384,
        'sha512': hashlib.sha512,
        'sha224': hashlib.sha224,
        'sha3224': hashlib.sha224,
        'sha3256': hashlib.sha256,
        'sha3384': hashlib.sha384,
        'sha3512': hashlib.sha512,
    }

    hash_func = hash_map.get(algorithm)
    if hash_func is None:
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    h = hmac_module.new(key, message, hash_func)
    return h.hexdigest()


def verify_hmac(
    message: Union[str, bytes],
    key: Union[str, bytes],
    expected_hmac: str,
    algorithm: str = 'sha256'
) -> bool:
    """
    Verify an HMAC against a message.

    Args:
        message: The message to verify.
        key: The secret key.
        expected_hmac: Expected HMAC value.
        algorithm: Hash algorithm used.

    Returns:
        True if HMAC matches.
    """
    computed = compute_hmac(message, key, algorithm)
    return hmac_module.compare_digest(computed, expected_hmac.lower())


def compute_hmac_base64(
    message: Union[str, bytes],
    key: Union[str, bytes],
    algorithm: str = 'sha256'
) -> str:
    """
    Compute HMAC and return as base64-encoded string.

    Args:
        message: The message to authenticate.
        key: The secret key.
        algorithm: Hash algorithm.

    Returns:
        Base64-encoded HMAC string.
    """
    if isinstance(message, str):
        message = message.encode('utf-8')
    if isinstance(key, str):
        key = key.encode('utf-8')

    algorithm = algorithm.lower().replace('-', '').replace('_', '')

    hash_map = {
        'md5': hashlib.md5,
        'sha1': hashlib.sha1,
        'sha256': hashlib.sha256,
        'sha384': hashlib.sha384,
        'sha512': hashlib.sha512,
    }

    hash_func = hash_map.get(algorithm)
    if hash_func is None:
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    import base64
    h = hmac_module.new(key, message, hash_func)
    return base64.b64encode(h.digest()).decode('ascii')


def create_message_signature(
    message: str,
    secret: str,
    timestamp: Optional[int] = None,
    nonce: Optional[str] = None
) -> dict:
    """
    Create a signed message with metadata.

    Args:
        message: The message to sign.
        secret: Secret key for HMAC.
        timestamp: Unix timestamp (defaults to current time).
        nonce: Random nonce (defaults to generated).

    Returns:
        Dictionary with signature information.
    """
    import time
    import secrets

    if timestamp is None:
        timestamp = int(time.time())
    if nonce is None:
        nonce = secrets.token_hex(8)

    payload = f"{timestamp}:{nonce}:{message}"
    signature = compute_hmac(payload, secret, 'sha256')

    return {
        'message': message,
        'timestamp': timestamp,
        'nonce': nonce,
        'signature': signature,
        'payload': payload,
    }


def verify_message_signature(
    signature_data: dict,
    secret: str,
    max_age_seconds: int = 300
) -> bool:
    """
    Verify a signed message.

    Args:
        signature_data: Dictionary from create_message_signature.
        secret: Secret key for verification.
        max_age_seconds: Maximum age of signature in seconds.

    Returns:
        True if signature is valid and not expired.
    """
    import time

    timestamp = signature_data.get('timestamp', 0)
    nonce = signature_data.get('nonce', '')
    message = signature_data.get('message', '')
    expected_sig = signature_data.get('signature', '')

    if abs(time.time() - timestamp) > max_age_seconds:
        return False

    payload = f"{timestamp}:{nonce}:{message}"
    computed = compute_hmac(payload, secret, 'sha256')

    return hmac_module.compare_digest(computed, expected_sig.lower())


def hmac_digest_size(algorithm: str = 'sha256') -> int:
    """
    Get the digest size in bytes for an algorithm.

    Args:
        algorithm: Hash algorithm name.

    Returns:
        Digest size in bytes.
    """
    algorithm = algorithm.lower().replace('-', '').replace('_', '')
    sizes = {
        'md5': 16,
        'sha1': 20,
        'sha256': 32,
        'sha384': 48,
        'sha512': 64,
        'sha224': 28,
    }
    return sizes.get(algorithm, 32)


def constant_time_compare(val1: str, val2: str) -> bool:
    """
    Compare two values in constant time to prevent timing attacks.

    Args:
        val1: First value.
        val2: Second value.

    Returns:
        True if values are equal.
    """
    return hmac_module.compare_digest(val1, val2)

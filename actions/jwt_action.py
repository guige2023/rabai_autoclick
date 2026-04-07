"""
JWT (JSON Web Token) encoding and decoding actions.
"""
from __future__ import annotations

import json
import base64
from typing import Dict, Any, Optional, List


def _base64url_decode(data: Union[str, bytes]) -> bytes:
    """Decode base64url encoded data."""
    if isinstance(data, str):
        data = data.encode('ascii')

    padding = 4 - (len(data) % 4)
    if padding != 4:
        data += b'=' * padding

    return base64.urlsafe_b64decode(data)


def _base64url_encode(data: bytes) -> str:
    """Encode data to base64url."""
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def decode_jwt(token: str, verify: bool = False) -> Dict[str, Any]:
    """
    Decode a JWT token without verification.

    Args:
        token: The JWT token string.
        verify: Whether to verify signature (requires secret).

    Returns:
        Dictionary with 'header', 'payload', 'signature'.

    Raises:
        ValueError: If token format is invalid.
    """
    parts = token.split('.')

    if len(parts) != 3:
        raise ValueError("Invalid JWT format: expected 3 parts")

    try:
        header_data = _base64url_decode(parts[0])
        header = json.loads(header_data.decode('utf-8'))

        payload_data = _base64url_decode(parts[1])
        payload = json.loads(payload_data.decode('utf-8'))

        signature = parts[2]

        return {
            'header': header,
            'payload': payload,
            'signature': signature,
            'is_expired': _is_token_expired(payload),
            'issued_at': payload.get('iat'),
            'expires_at': payload.get('exp'),
            'issuer': payload.get('iss'),
            'subject': payload.get('sub'),
        }
    except Exception as e:
        raise ValueError(f"Failed to decode JWT: {e}")


def _is_token_expired(payload: Dict[str, Any]) -> bool:
    """Check if token payload indicates expiration."""
    import time

    exp = payload.get('exp')
    if exp is None:
        return False

    return time.time() > exp


def encode_jwt(
    payload: Dict[str, Any],
    secret: str,
    algorithm: str = 'HS256',
    headers: Optional[Dict[str, Any]] = None
) -> str:
    """
    Encode a JWT token.

    Args:
        payload: The JWT payload (claims).
        secret: Secret key for signing.
        algorithm: Signing algorithm ('HS256', 'HS384', 'HS512', 'none').
        headers: Additional header parameters.

    Returns:
        JWT token string.

    Raises:
        ValueError: If algorithm is not supported.
    """
    import hashlib
    import hmac

    supported_algs = ['HS256', 'HS384', 'HS512', 'none']
    if algorithm not in supported_algs:
        raise ValueError(f"Unsupported algorithm: {algorithm}")

    header = {'alg': algorithm, 'typ': 'JWT'}
    if headers:
        header.update(headers)

    header_b64 = _base64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))
    payload_b64 = _base64url_encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))

    signing_input = f"{header_b64}.{payload_b64}"

    if algorithm == 'none':
        signature = ''
    else:
        hash_func = {
            'HS256': hashlib.sha256,
            'HS384': hashlib.sha384,
            'HS512': hashlib.sha512,
        }[algorithm]

        key = secret.encode('utf-8') if isinstance(secret, str) else secret
        sig = hmac.new(key, signing_input.encode('utf-8'), hash_func)
        signature = _base64url_encode(sig.digest())

    return f"{signing_input}.{signature}"


def verify_jwt(
    token: str,
    secret: str,
    algorithms: Optional[List[str]] = None,
    options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Verify and decode a JWT token.

    Args:
        token: The JWT token.
        secret: Secret key for verification.
        algorithms: List of allowed algorithms.
        options: Verification options.

    Returns:
        Decoded payload if valid.

    Raises:
        ValueError: If token is invalid or verification fails.
    """
    import hashlib
    import hmac
    import time

    if algorithms is None:
        algorithms = ['HS256', 'HS384', 'HS512']

    if options is None:
        options = {}

    parts = token.split('.')
    if len(parts) != 3:
        raise ValueError("Invalid JWT format")

    try:
        header_data = _base64url_decode(parts[0])
        header = json.loads(header_data.decode('utf-8'))
    except Exception as e:
        raise ValueError(f"Invalid JWT header: {e}")

    alg = header.get('alg')
    if alg not in algorithms:
        raise ValueError(f"Algorithm '{alg}' not allowed")

    payload_data = _base64url_decode(parts[1])
    payload = json.loads(payload_data.decode('utf-8'))

    if options.get('verify_exp', True):
        exp = payload.get('exp')
        if exp is not None and time.time() > exp:
            raise ValueError("Token has expired")

    if options.get('verify_nbf', True):
        nbf = payload.get('nbf')
        if nbf is not None and time.time() < nbf:
            raise ValueError("Token not yet valid")

    if options.get('verify_iat', False):
        iat = payload.get('iat')
        if iat is not None and time.time() < iat:
            raise ValueError("Token issued in the future")

    if options.get('verify_iss'):
        expected_iss = options['verify_iss']
        if payload.get('iss') != expected_iss:
            raise ValueError(f"Invalid issuer: expected {expected_iss}")

    if options.get('verify_aud'):
        expected_aud = options['verify_aud']
        aud = payload.get('aud')
        if aud != expected_aud:
            raise ValueError(f"Invalid audience: expected {expected_aud}")

    if options.get('verify_sub'):
        expected_sub = options['verify_sub']
        if payload.get('sub') != expected_sub:
            raise ValueError(f"Invalid subject: expected {expected_sub}")

    if alg != 'none':
        key = secret.encode('utf-8') if isinstance(secret, str) else secret
        signing_input = f"{parts[0]}.{parts[1]}"

        hash_func = {
            'HS256': hashlib.sha256,
            'HS384': hashlib.sha384,
            'HS512': hashlib.sha512,
        }[alg]

        sig = hmac.new(key, signing_input.encode('utf-8'), hash_func)
        expected_sig = _base64url_encode(sig.digest())

        if not hmac.compare_digest(parts[2], expected_sig):
            raise ValueError("Invalid signature")

    return payload


def create_access_token(
    subject: str,
    secret: str,
    expires_in: int = 3600,
    issuer: Optional[str] = None,
    additional_claims: Optional[Dict[str, Any]] = None
) -> str:
    """
    Create a JWT access token.

    Args:
        subject: Token subject (usually user ID).
        secret: Secret key for signing.
        expires_in: Expiration time in seconds.
        issuer: Token issuer.
        additional_claims: Additional JWT claims.

    Returns:
        JWT access token string.
    """
    import time

    payload = {
        'sub': subject,
        'iat': int(time.time()),
        'exp': int(time.time()) + expires_in,
        'type': 'access',
    }

    if issuer:
        payload['iss'] = issuer

    if additional_claims:
        payload.update(additional_claims)

    return encode_jwt(payload, secret, 'HS256')


def create_refresh_token(
    subject: str,
    secret: str,
    expires_in: int = 604800
) -> str:
    """
    Create a JWT refresh token.

    Args:
        subject: Token subject.
        secret: Secret key for signing.
        expires_in: Expiration time in seconds (default 7 days).

    Returns:
        JWT refresh token string.
    """
    import time

    payload = {
        'sub': subject,
        'iat': int(time.time()),
        'exp': int(time.time()) + expires_in,
        'type': 'refresh',
    }

    return encode_jwt(payload, secret, 'HS256')


def get_token_claims(token: str) -> Dict[str, Any]:
    """
    Extract all claims from a JWT token.

    Args:
        token: The JWT token.

    Returns:
        Dictionary of all claims.
    """
    decoded = decode_jwt(token)
    return decoded.get('payload', {})


def is_token_expired(token: str) -> bool:
    """
    Check if a token is expired.

    Args:
        token: The JWT token.

    Returns:
        True if token is expired.
    """
    decoded = decode_jwt(token)
    return decoded.get('is_expired', False)


def get_token_expiration(token: str) -> Optional[int]:
    """
    Get the expiration timestamp from a token.

    Args:
        token: The JWT token.

    Returns:
        Unix timestamp of expiration, or None if no expiration.
    """
    decoded = decode_jwt(token)
    return decoded.get('expires_at')

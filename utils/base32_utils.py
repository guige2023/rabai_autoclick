"""
Base32 Encoding/Decoding Utilities.

This module provides Base32 encoding and decoding utilities
for data serialization and encoding in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import base64
from typing import Union, Optional


BASE32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
BASE32_PAD_CHAR = "="


def b32_encode(data: Union[bytes, str], uppercase: bool = True) -> str:
    """
    Encode data to Base32 string.

    Args:
        data: Data to encode (bytes or str)
        uppercase: Use uppercase letters (default: True)

    Returns:
        Base32 encoded string
    """
    if isinstance(data, str):
        data = data.encode("utf-8")

    encoded = base64.b32encode(data).decode("ascii")

    if not uppercase:
        encoded = encoded.lower()

    return encoded


def b32_decode(data: str, strict: bool = False) -> bytes:
    """
    Decode Base32 string to bytes.

    Args:
        data: Base32 encoded string
        strict: Raise error on invalid padding

    Returns:
        Decoded bytes
    """
    if isinstance(data, str):
        data = data.upper().strip()

    return base64.b32decode(data, strict=strict)


def b32_encode_file(path: str, uppercase: bool = True) -> str:
    """
    Encode file contents to Base32.

    Args:
        path: Path to file
        uppercase: Use uppercase letters

    Returns:
        Base32 encoded string
    """
    with open(path, "rb") as f:
        return b32_encode(f.read(), uppercase=uppercase)


def b32_decode_to_file(data: str, path: str) -> None:
    """
    Decode Base32 string to file.

    Args:
        data: Base32 encoded string
        path: Output file path
    """
    decoded = b32_decode(data)
    with open(path, "wb") as f:
        f.write(decoded)


def is_valid_base32(data: str) -> bool:
    """
    Check if string is valid Base32.

    Args:
        data: String to validate

    Returns:
        True if valid Base32
    """
    if not data:
        return False

    data = data.upper().strip()

    if len(data) < 1:
        return False

    for char in data:
        if char not in BASE32_ALPHABET and char != BASE32_PAD_CHAR:
            return False

    pad_count = data.count(BASE32_PAD_CHAR)
    if pad_count > 0:
        if pad_count > 6:
            return False
        if not data.endswith(BASE32_PAD_CHAR * pad_count):
            return False

    return True


def normalize_base32(data: str) -> str:
    """
    Normalize Base32 string to standard format.

    Args:
        data: Input Base32 string

    Returns:
        Normalized Base32 string (uppercase, no whitespace)
    """
    return data.upper().replace(" ", "").replace("-", "").strip()

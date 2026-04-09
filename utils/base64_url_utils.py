"""
Base64 URL-Safe Encoding Utilities.

This module provides URL-safe Base64 encoding and decoding
for web applications and API interactions in UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import base64
from typing import Union


def urlsafe_b64_encode(data: Union[bytes, str], padding: bool = True) -> str:
    """
    URL-safe Base64 encoding.

    Args:
        data: Data to encode
        padding: Include padding characters (default: True)

    Returns:
        URL-safe Base64 encoded string
    """
    if isinstance(data, str):
        data = data.encode("utf-8")

    encoded = base64.urlsafe_b64encode(data).decode("ascii")

    if not padding:
        encoded = encoded.rstrip("=")

    return encoded


def urlsafe_b64_decode(data: str, padding: bool = True) -> bytes:
    """
    URL-safe Base64 decoding.

    Args:
        data: URL-safe Base64 encoded string
        padding: Expect padding characters (default: True)

    Returns:
        Decoded bytes
    """
    if padding:
        padding_needed = 4 - (len(data) % 4)
        if padding_needed < 4:
            data += "=" * padding_needed
    else:
        data = data.replace("-", "+").replace("_", "/")
        padding_needed = 4 - (len(data) % 4)
        if padding_needed < 4:
            data += "=" * padding_needed

    return base64.urlsafe_b64decode(data)


def encode_dict_urlsafe(data: dict, separator: str = "&", padding: bool = True) -> str:
    """
    Encode dictionary to URL-safe Base64.

    Args:
        data: Dictionary to encode
        separator: Key-value separator
        padding: Include padding

    Returns:
        Encoded string
    """
    pairs = [f"{k}={v}" for k, v in sorted(data.items())]
    body = separator.join(pairs)
    return urlsafe_b64_encode(body, padding=padding)


def decode_dict_urlsafe(data: str, separator: str = "&", padding: bool = True) -> dict:
    """
    Decode URL-safe Base64 to dictionary.

    Args:
        data: Encoded string
        separator: Key-value separator
        padding: Expect padding

    Returns:
        Decoded dictionary
    """
    decoded = urlsafe_b64_decode(data, padding=padding).decode("utf-8")
    result = {}
    for pair in decoded.split(separator):
        if "=" in pair:
            k, v = pair.split("=", 1)
            result[k] = v
    return result


def is_valid_urlsafe_base64(data: str) -> bool:
    """
    Validate URL-safe Base64 string.

    Args:
        data: String to validate

    Returns:
        True if valid
    """
    if not data:
        return False

    valid_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=")
    return all(c in valid_chars for c in data)


def strip_base64_padding(data: str) -> str:
    """
    Remove Base64 padding from string.

    Args:
        data: Base64 string with padding

    Returns:
        String without padding
    """
    return data.rstrip("=")


def add_base64_padding(data: str) -> str:
    """
    Add Base64 padding to string.

    Args:
        data: Base64 string without padding

    Returns:
        Padded Base64 string
    """
    padding_needed = 4 - (len(data) % 4)
    if padding_needed < 4:
        return data + "=" * padding_needed
    return data

"""Data verification utilities: format validators, range checkers, and data quality."""

from __future__ import annotations

import re
from typing import Any

__all__ = [
    "is_valid_email",
    "is_valid_url",
    "is_valid_ipv4",
    "is_valid_ipv6",
    "is_valid_uuid",
    "is_valid_hex_color",
    "is_valid_phone",
    "is_valid_credit_card",
    "is_valid_json",
    "range_check",
    "DataVerifier",
]


def is_valid_email(email: str) -> bool:
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def is_valid_url(url: str) -> bool:
    pattern = r"^https?://[^\s/$.?#].[^\s]*$"
    return bool(re.match(pattern, url, re.IGNORECASE))


def is_valid_ipv4(ip: str) -> bool:
    parts = ip.split(".")
    if len(parts) != 4:
        return False
    try:
        return all(0 <= int(p) <= 255 for p in parts)
    except ValueError:
        return False


def is_valid_ipv6(ip: str) -> bool:
    try:
        parts = ip.split(":")
        if len(parts) > 8:
            return False
        return True
    except Exception:
        return False


def is_valid_uuid(value: str) -> bool:
    pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    return bool(re.match(pattern, value, re.IGNORECASE))


def is_valid_hex_color(value: str) -> bool:
    pattern = r"^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$"
    return bool(re.match(pattern, value))


def is_valid_phone(phone: str, country: str = "US") -> bool:
    digits = re.sub(r"\D", "", phone)
    if country == "US":
        return len(digits) == 10 or (len(digits) == 11 and digits[0] == "1")
    return len(digits) >= 7


def is_valid_credit_card(number: str) -> bool:
    digits = re.sub(r"\D", "", number)
    if not digits.isdigit() or len(digits) < 13 or len(digits) > 19:
        return False
    total = 0
    reverse_digits = digits[::-1]
    for i, d in enumerate(reverse_digits):
        n = int(d)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def is_valid_json(value: str) -> bool:
    import json
    try:
        json.loads(value)
        return True
    except Exception:
        return False


def range_check(value: float, min_val: float, max_val: float) -> bool:
    return min_val <= value <= max_val


class DataVerifier:
    """Collection of data quality checks."""

    @staticmethod
    def verify_range(
        value: float,
        min_val: float,
        max_val: float,
        name: str = "value",
    ) -> tuple[bool, str]:
        if value < min_val:
            return False, f"{name} {value} is below minimum {min_val}"
        if value > max_val:
            return False, f"{name} {value} exceeds maximum {max_val}"
        return True, ""

    @staticmethod
    def verify_type(value: Any, expected_type: type) -> tuple[bool, str]:
        if not isinstance(value, expected_type):
            return False, f"Expected {expected_type.__name__}, got {type(value).__name__}"
        return True, ""

    @staticmethod
    def verify_not_none(value: Any, field_name: str = "field") -> tuple[bool, str]:
        if value is None:
            return False, f"{field_name} is required"
        return True, ""

    @staticmethod
    def verify_not_empty(value: str | list, field_name: str = "field") -> tuple[bool, str]:
        if not value:
            return False, f"{field_name} cannot be empty"
        return True, ""

    @staticmethod
    def verify_length(
        value: str,
        min_len: int = 0,
        max_len: int = 10**10,
        field_name: str = "value",
    ) -> tuple[bool, str]:
        if len(value) < min_len:
            return False, f"{field_name} too short (min {min_len})"
        if len(value) > max_len:
            return False, f"{field_name} too long (max {max_len})"
        return True, ""

    @staticmethod
    def verify_pattern(
        value: str,
        pattern: str,
        field_name: str = "value",
    ) -> tuple[bool, str]:
        if not re.match(pattern, value):
            return False, f"{field_name} does not match required pattern"
        return True, ""

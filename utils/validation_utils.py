"""Validation utilities for RabAI AutoClick.

Provides:
- Common validation helpers
- Type validators
- Schema validators
"""

import re
from typing import Any, Callable, Dict, List, Optional, Type


def is_string(value: Any) -> bool:
    """Check if value is a string.

    Args:
        value: Value to check.

    Returns:
        True if string.
    """
    return isinstance(value, str)


def is_int(value: Any) -> bool:
    """Check if value is an integer.

    Args:
        value: Value to check.

    Returns:
        True if integer.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def is_float(value: Any) -> bool:
    """Check if value is a float.

    Args:
        value: Value to check.

    Returns:
        True if float.
    """
    return isinstance(value, float)


def is_bool(value: Any) -> bool:
    """Check if value is a boolean.

    Args:
        value: Value to check.

    Returns:
        True if boolean.
    """
    return isinstance(value, bool)


def is_list(value: Any) -> bool:
    """Check if value is a list.

    Args:
        value: Value to check.

    Returns:
        True if list.
    """
    return isinstance(value, list)


def is_dict(value: Any) -> bool:
    """Check if value is a dictionary.

    Args:
        value: Value to check.

    Returns:
        True if dict.
    """
    return isinstance(value, dict)


def is_email(value: str) -> bool:
    """Check if value is a valid email.

    Args:
        value: Email string to check.

    Returns:
        True if valid email.
    """
    if not isinstance(value, str):
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, value))


def is_url(value: str) -> bool:
    """Check if value is a valid URL.

    Args:
        value: URL string to check.

    Returns:
        True if valid URL.
    """
    if not isinstance(value, str):
        return False
    pattern = r"^https?://[^\s/$.?#].[^\s]*$"
    return bool(re.match(pattern, value))


def is_phone(value: str) -> bool:
    """Check if value is a valid phone number.

    Args:
        value: Phone string to check.

    Returns:
        True if valid phone.
    """
    if not isinstance(value, str):
        return False
    pattern = r"^\+?[\d\s\-()]+$"
    return bool(re.match(pattern, value)) and len(re.sub(r"\D", "", value)) >= 7


def is_ipv4(value: str) -> bool:
    """Check if value is a valid IPv4 address.

    Args:
        value: IP string to check.

    Returns:
        True if valid IPv4.
    """
    if not isinstance(value, str):
        return False
    pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
    if not re.match(pattern, value):
        return False
    parts = value.split(".")
    return all(0 <= int(part) <= 255 for part in parts)


def is_port(value: Any) -> bool:
    """Check if value is a valid port number.

    Args:
        value: Value to check.

    Returns:
        True if valid port.
    """
    try:
        port = int(value)
        return 0 <= port <= 65535
    except (ValueError, TypeError):
        return False


def is_path(value: str) -> bool:
    """Check if value is a valid file path.

    Args:
        value: Path string to check.

    Returns:
        True if valid path.
    """
    if not isinstance(value, str):
        return False
    invalid_chars = '<>:"|?*\x00-\x1f'
    return not any(c in value for c in invalid_chars)


def is_alphanumeric(value: str) -> bool:
    """Check if value is alphanumeric.

    Args:
        value: String to check.

    Returns:
        True if alphanumeric.
    """
    if not isinstance(value, str):
        return False
    return value.isalnum()


def is_alpha(value: str) -> bool:
    """Check if value contains only alphabetic characters.

    Args:
        value: String to check.

    Returns:
        True if alphabetic.
    """
    if not isinstance(value, str):
        return False
    return value.isalpha()


def is_numeric(value: str) -> bool:
    """Check if value is numeric string.

    Args:
        value: String to check.

    Returns:
        True if numeric.
    """
    if not isinstance(value, str):
        return False
    return value.isdigit()


def is_hex_color(value: str) -> bool:
    """Check if value is a valid hex color code.

    Args:
        value: Color string to check.

    Returns:
        True if valid hex color.
    """
    if not isinstance(value, str):
        return False
    pattern = r"^#?([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$"
    return bool(re.match(pattern, value))


def is_username(value: str) -> bool:
    """Check if value is a valid username.

    Args:
        value: Username string to check.

    Returns:
        True if valid username.
    """
    if not isinstance(value, str):
        return False
    if len(value) < 3 or len(value) > 32:
        return False
    pattern = r"^[a-zA-Z0-9_-]+$"
    return bool(re.match(pattern, value))


def is_password_strong(value: str) -> bool:
    """Check if password meets strength requirements.

    Args:
        value: Password string to check.

    Returns:
        True if strong password.
    """
    if not isinstance(value, str):
        return False
    if len(value) < 8:
        return False
    has_upper = any(c.isupper() for c in value)
    has_lower = any(c.islower() for c in value)
    has_digit = any(c.isdigit() for c in value)
    return has_upper and has_lower and has_digit


def is_json_string(value: str) -> bool:
    """Check if value is a valid JSON string.

    Args:
        value: String to check.

    Returns:
        True if valid JSON.
    """
    if not isinstance(value, str):
        return False
    try:
        import json
        json.loads(value)
        return True
    except Exception:
        return False


def is_uuid(value: str) -> bool:
    """Check if value is a valid UUID.

    Args:
        value: String to check.

    Returns:
        True if valid UUID.
    """
    if not isinstance(value, str):
        return False
    pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    return bool(re.match(pattern, value.lower()))


def is_credit_card(value: str) -> bool:
    """Check if value is a valid credit card number.

    Args:
        value: Card number to check.

    Returns:
        True if valid credit card.
    """
    if not isinstance(value, str):
        return False
    digits = re.sub(r"\D", "", value)
    if len(digits) < 13 or len(digits) > 19:
        return False
    total = 0
    for i, digit in enumerate(reversed(digits)):
        n = int(digit)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def in_range(value: float, min_val: float, max_val: float) -> bool:
    """Check if value is in range.

    Args:
        value: Value to check.
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        True if in range.
    """
    try:
        val = float(value)
        return min_val <= val <= max_val
    except (ValueError, TypeError):
        return False


def min_length(value: str, min_len: int) -> bool:
    """Check if string length is at least min_len.

    Args:
        value: String to check.
        min_len: Minimum length.

    Returns:
        True if length is sufficient.
    """
    if not isinstance(value, str):
        return False
    return len(value) >= min_len


def max_length(value: str, max_len: int) -> bool:
    """Check if string length is at most max_len.

    Args:
        value: String to check.
        max_len: Maximum length.

    Returns:
        True if length is within limit.
    """
    if not isinstance(value, str):
        return False
    return len(value) <= max_len


def matches_pattern(value: str, pattern: str) -> bool:
    """Check if value matches regex pattern.

    Args:
        value: String to check.
        pattern: Regex pattern.

    Returns:
        True if matches.
    """
    if not isinstance(value, str):
        return False
    return bool(re.match(pattern, value))


def is_one_of(value: Any, choices: List[Any]) -> bool:
    """Check if value is one of choices.

    Args:
        value: Value to check.
        choices: List of valid choices.

    Returns:
        True if in choices.
    """
    return value in choices


def validate_type(value: Any, expected_type: Type) -> bool:
    """Check if value is of expected type.

    Args:
        value: Value to check.
        expected_type: Expected type.

    Returns:
        True if correct type.
    """
    return isinstance(value, expected_type)


def validate_schema(data: Dict[str, Any], schema: Dict[str, Callable[[Any], bool]]) -> List[str]:
    """Validate data against schema.

    Args:
        data: Data to validate.
        schema: Dict mapping keys to validator functions.

    Returns:
        List of validation error messages.
    """
    errors = []
    for key, validator in schema.items():
        if key not in data:
            errors.append(f"Missing required field: {key}")
            continue
        if not validator(data[key]):
            errors.append(f"Invalid value for field: {key}")
    return errors


def validate_required_keys(data: Dict[str, Any], required_keys: List[str]) -> List[str]:
    """Validate that all required keys are present.

    Args:
        data: Data to validate.
        required_keys: List of required keys.

    Returns:
        List of missing key names.
    """
    return [key for key in required_keys if key not in data]


def validate_email_list(emails: List[str]) -> List[str]:
    """Validate list of email addresses.

    Args:
        emails: List of email strings.

    Returns:
        List of invalid email addresses.
    """
    return [email for email in emails if not is_email(email)]

"""
Data Validation and Sanitization Action Module.

Validates and sanitizes user input, API payloads, and file data.
Supports email, URL, phone, credit card, and custom rule validation.

Example:
    >>> from data_validator_action import DataValidator
    >>> validator = DataValidator()
    >>> result = validator.validate_email("test@example.com")
    >>> validator.validate_record(record, rules={"age": "int|min:0|max:120"})
"""
from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: list[str] = field(default_factory=list)
    sanitized_value: Any = None


@dataclass
class ValidationRule:
    """A single validation rule."""
    name: str
    params: list[Any] = field(default_factory=list)


class DataValidator:
    """Validate and sanitize data with configurable rules."""

    EMAIL_RE = re.compile(
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )
    URL_RE = re.compile(
        r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE
    )
    PHONE_RE = re.compile(r"^\+?[\d\s\-()]{10,}$")
    IPV4_RE = re.compile(
        r"^(?:(?:25[0-5]|2[0-4]\d|1?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|1?\d\d?)$"
    )
    CREDIT_CARD_RE = re.compile(r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$")
    UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )

    def validate_email(self, value: str) -> ValidationResult:
        """Validate email address."""
        if not value:
            return ValidationResult(False, ["Email is required"])
        if not self.EMAIL_RE.match(value.strip()):
            return ValidationResult(False, ["Invalid email format"])
        return ValidationResult(True, sanitized_value=value.strip().lower())

    def validate_url(self, value: str, require_https: bool = False) -> ValidationResult:
        """Validate URL."""
        if not value:
            return ValidationResult(False, ["URL is required"])
        value = value.strip()
        if require_https and not value.lower().startswith("https://"):
            return ValidationResult(False, ["URL must use HTTPS"])
        if not self.URL_RE.match(value):
            return ValidationResult(False, ["Invalid URL format"])
        return ValidationResult(True, sanitized_value=value)

    def validate_phone(
        self,
        value: str,
        country_code: Optional[str] = None,
    ) -> ValidationResult:
        """Validate phone number."""
        if not value:
            return ValidationResult(False, ["Phone number is required"])
        digits = re.sub(r"\D", "", value)
        if country_code == "US" and len(digits) != 10:
            return ValidationResult(False, ["US phone must be 10 digits"])
        if len(digits) < 10:
            return ValidationResult(False, ["Phone must be at least 10 digits"])
        return ValidationResult(True, sanitized_value=digits)

    def validate_required(self, value: Any) -> ValidationResult:
        """Validate that value is not empty."""
        if value is None:
            return ValidationResult(False, ["Value is required"])
        if isinstance(value, str) and not value.strip():
            return ValidationResult(False, ["Value is required"])
        if isinstance(value, (list, dict, tuple)) and len(value) == 0:
            return ValidationResult(False, ["Value cannot be empty"])
        return ValidationResult(True, sanitized_value=value)

    def validate_length(
        self,
        value: str,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ) -> ValidationResult:
        """Validate string length."""
        if min_length and len(value) < min_length:
            return ValidationResult(False, [f"Length must be at least {min_length}"])
        if max_length and len(value) > max_length:
            return ValidationResult(False, [f"Length must be at most {max_length}"])
        return ValidationResult(True, sanitized_value=value)

    def validate_range(
        self,
        value: float,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> ValidationResult:
        """Validate numeric range."""
        if min_val is not None and value < min_val:
            return ValidationResult(False, [f"Value must be at least {min_val}"])
        if max_val is not None and value > max_val:
            return ValidationResult(False, [f"Value must be at most {max_val}"])
        return ValidationResult(True, sanitized_value=value)

    def validate_in(
        self,
        value: Any,
        choices: list[Any],
    ) -> ValidationResult:
        """Validate value is in allowed choices."""
        if value not in choices:
            return ValidationResult(False, [f"Value must be one of: {choices}"])
        return ValidationResult(True, sanitized_value=value)

    def validate_regex(
        self,
        value: str,
        pattern: str,
        flags: int = 0,
    ) -> ValidationResult:
        """Validate against regex pattern."""
        try:
            if not re.search(pattern, value, flags):
                return ValidationResult(False, [f"Value does not match pattern"])
            return ValidationResult(True, sanitized_value=value)
        except Exception as e:
            return ValidationResult(False, [f"Invalid regex: {e}"])

    def validate_ipv4(self, value: str) -> ValidationResult:
        """Validate IPv4 address."""
        if not self.IPV4_RE.match(value):
            return ValidationResult(False, ["Invalid IPv4 address"])
        return ValidationResult(True, sanitized_value=value)

    def validate_credit_card(self, value: str) -> ValidationResult:
        """Validate credit card number (Luhn algorithm)."""
        digits = re.sub(r"\D", "", value)
        if len(digits) < 13 or len(digits) > 19:
            return ValidationResult(False, ["Invalid card number length"])
        if not self._luhn_check(digits):
            return ValidationResult(False, ["Invalid card number (failed checksum)"])
        return ValidationResult(True, sanitized_value=digits)

    def _luhn_check(self, digits: str) -> bool:
        total = 0
        for i, d in enumerate(reversed(digits)):
            n = int(d)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0

    def validate_uuid(self, value: str) -> ValidationResult:
        """Validate UUID format."""
        if not self.UUID_RE.match(value):
            return ValidationResult(False, ["Invalid UUID format"])
        return ValidationResult(True, sanitized_value=value.upper())

    def validate_json(self, value: str) -> ValidationResult:
        """Validate JSON string."""
        import json
        try:
            data = json.loads(value)
            return ValidationResult(True, sanitized_value=data)
        except json.JSONDecodeError as e:
            return ValidationResult(False, [f"Invalid JSON: {e}"])

    def validate_record(
        self,
        record: dict[str, Any],
        rules: dict[str, str],
    ) -> tuple[bool, list[str]]:
        """
        Validate record against rule definitions.

        Rules format: "rule_name:param1:param2" separated by "|"

        Example:
            rules = {
                "email": "required|email",
                "age": "required|int|range:0:120",
                "name": "required|length:1:100",
            }

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors: list[str] = []

        for field_name, rule_str in rules.items():
            value = record.get(field_name)
            field_errors = self._validate_field(field_name, value, rule_str)
            errors.extend(field_errors)

        return len(errors) == 0, errors

    def _validate_field(
        self,
        field_name: str,
        value: Any,
        rule_str: str,
    ) -> list[str]:
        errors: list[str] = []
        rule_parts = rule_str.split("|")

        for part in rule_parts:
            parts = part.split(":")
            rule_name = parts[0]
            params = parts[1:]

            result: Optional[ValidationResult] = None

            if rule_name == "required":
                result = self.validate_required(value)
                if not result.valid:
                    errors.append(f"{field_name}: {result.errors[0]}")
                    return errors
            elif rule_name == "email" and value:
                result = self.validate_email(str(value))
            elif rule_name == "url" and value:
                result = self.validate_url(str(value))
            elif rule_name == "phone" and value:
                result = self.validate_phone(str(value))
            elif rule_name == "int" and value:
                try:
                    int(value)
                    result = ValidationResult(True, sanitized_value=int(value))
                except (ValueError, TypeError):
                    errors.append(f"{field_name}: must be an integer")
                    continue
            elif rule_name == "float" and value:
                try:
                    float(value)
                    result = ValidationResult(True, sanitized_value=float(value))
                except (ValueError, TypeError):
                    errors.append(f"{field_name}: must be a number")
                    continue
            elif rule_name == "min" and len(params) >= 1 and value:
                min_val = float(params[0])
                try:
                    num_val = float(value)
                    result = self.validate_range(num_val, min_val=min_val)
                except (ValueError, TypeError):
                    if len(value) < min_val:
                        errors.append(f"{field_name}: must be at least {min_val} characters")
                        continue
            elif rule_name == "max" and len(params) >= 1 and value:
                max_val = float(params[0])
                try:
                    num_val = float(value)
                    result = self.validate_range(num_val, max_val=max_val)
                except (ValueError, TypeError):
                    if len(value) > max_val:
                        errors.append(f"{field_name}: must be at most {max_val} characters")
                        continue
            elif rule_name == "in" and len(params) >= 1 and value:
                choices = params
                result = self.validate_in(value, choices)
            elif rule_name == "regex" and len(params) >= 1 and value:
                result = self.validate_regex(str(value), params[0])

            if result and not result.valid:
                for error in result.errors:
                    errors.append(f"{field_name}: {error}")

        return errors

    def sanitize_html(self, value: str) -> str:
        """Remove dangerous HTML tags and attributes."""
        dangerous_tags = ["script", "iframe", "object", "embed", "form", "input"]
        dangerous_attrs = ["onerror", "onload", "onclick", "onmouseover", "javascript:"]
        result = value
        for tag in dangerous_tags:
            result = re.sub(f"<{tag}[^>]*>.*?</{tag}>", "", result, flags=re.IGNORECASE | re.DOTALL)
            result = re.sub(f"<{tag}[^>]*/?>", "", result, flags=re.IGNORECASE)
        for attr in dangerous_attrs:
            result = re.sub(f'{attr}="[^"]*"', "", result, flags=re.IGNORECASE)
            result = re.sub(f"{attr}='[^']*'", "", result, flags=re.IGNORECASE)
        return result

    def sanitize_sql(self, value: str) -> str:
        """Escape SQL special characters."""
        return value.replace("'", "''").replace("\\", "\\\\")

    def sanitize_filename(self, value: str) -> str:
        """Sanitize filename to be safe for filesystems."""
        value = re.sub(r"[^\w\s.-]", "_", value)
        value = re.sub(r"[\s]+", "_", value)
        value = value[:255]
        return value.strip("_.")


if __name__ == "__main__":
    validator = DataValidator()

    print("Email tests:")
    print(validator.validate_email("test@example.com"))
    print(validator.validate_email("invalid-email"))

    print("\nPhone tests:")
    print(validator.validate_phone("+1 (555) 123-4567"))

    print("\nRecord validation:")
    record = {"email": "test@example.com", "age": 25, "name": "Alice"}
    rules = {"email": "required|email", "age": "required|int|min:0|max:120", "name": "required"}
    valid, errors = validator.validate_record(record, rules)
    print(f"Valid: {valid}, Errors: {errors}")

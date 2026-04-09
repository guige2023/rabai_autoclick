"""
Input Validator Action Module.

Validates user input for form fields including text patterns,
numeric ranges, email formats, URL validation, and custom rules.
"""

import re
from typing import Any, Callable, Optional, Union


class ValidationResult:
    """Result of a validation check."""

    def __init__(self, valid: bool, error: Optional[str] = None):
        """
        Initialize validation result.

        Args:
            valid: Whether input is valid.
            error: Error message if invalid.
        """
        self.valid = valid
        self.error = error

    def __repr__(self) -> str:
        return f"ValidationResult({'valid' if self.valid else 'invalid'})"


class InputValidator:
    """Validates user input with various rules."""

    EMAIL_PATTERN = re.compile(
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )

    URL_PATTERN = re.compile(
        r"^https?://[^\s/$.?#].[^\s]*$",
        re.IGNORECASE,
    )

    PHONE_PATTERN = re.compile(
        r"^\+?[1-9]\d{1,14}$|^[\d\s\-\(\)]+$"
    )

    IPV4_PATTERN = re.compile(
        r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
        r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    )

    def __init__(self):
        """Initialize input validator."""
        self._custom_rules: dict[str, Callable[[Any], ValidationResult]] = {}

    def validate_email(self, value: str) -> ValidationResult:
        """
        Validate email address.

        Args:
            value: Email string.

        Returns:
            ValidationResult.
        """
        if not value or not self.EMAIL_PATTERN.match(value.strip()):
            return ValidationResult(False, "Invalid email address")
        return ValidationResult(True)

    def validate_url(self, value: str) -> ValidationResult:
        """
        Validate URL.

        Args:
            value: URL string.

        Returns:
            ValidationResult.
        """
        if not value or not self.URL_PATTERN.match(value.strip()):
            return ValidationResult(False, "Invalid URL format")
        return ValidationResult(True)

    def validate_phone(self, value: str) -> ValidationResult:
        """
        Validate phone number.

        Args:
            value: Phone number string.

        Returns:
            ValidationResult.
        """
        if not value:
            return ValidationResult(False, "Phone number is required")
        cleaned = re.sub(r"[\s\-\(\)]", "", value)
        if not self.PHONE_PATTERN.match(cleaned):
            return ValidationResult(False, "Invalid phone number format")
        return ValidationResult(True)

    def validate_required(self, value: Any) -> ValidationResult:
        """
        Validate that a value is not empty.

        Args:
            value: Value to check.

        Returns:
            ValidationResult.
        """
        if value is None:
            return ValidationResult(False, "This field is required")
        if isinstance(value, str) and not value.strip():
            return ValidationResult(False, "This field is required")
        return ValidationResult(True)

    def validate_min_length(
        self,
        value: str,
        min_length: int,
    ) -> ValidationResult:
        """
        Validate minimum string length.

        Args:
            value: String to validate.
            min_length: Minimum required length.

        Returns:
            ValidationResult.
        """
        if len(value) < min_length:
            return ValidationResult(
                False,
                f"Must be at least {min_length} characters"
            )
        return ValidationResult(True)

    def validate_max_length(
        self,
        value: str,
        max_length: int,
    ) -> ValidationResult:
        """
        Validate maximum string length.

        Args:
            value: String to validate.
            max_length: Maximum allowed length.

        Returns:
            ValidationResult.
        """
        if len(value) > max_length:
            return ValidationResult(
                False,
                f"Must be no more than {max_length} characters"
            )
        return ValidationResult(True)

    def validate_range(
        self,
        value: Union[int, float],
        min_val: Optional[Union[int, float]] = None,
        max_val: Optional[Union[int, float]] = None,
    ) -> ValidationResult:
        """
        Validate numeric range.

        Args:
            value: Number to validate.
            min_val: Minimum allowed value.
            max_val: Maximum allowed value.

        Returns:
            ValidationResult.
        """
        try:
            num = float(value)
        except (ValueError, TypeError):
            return ValidationResult(False, "Must be a valid number")

        if min_val is not None and num < min_val:
            return ValidationResult(False, f"Must be at least {min_val}")
        if max_val is not None and num > max_val:
            return ValidationResult(False, f"Must be no more than {max_val}")
        return ValidationResult(True)

    def validate_pattern(
        self,
        value: str,
        pattern: str,
    ) -> ValidationResult:
        """
        Validate against a regex pattern.

        Args:
            value: String to validate.
            pattern: Regex pattern.

        Returns:
            ValidationResult.
        """
        if not re.match(pattern, value):
            return ValidationResult(False, "Invalid format")
        return ValidationResult(True)

    def validate_ipv4(self, value: str) -> ValidationResult:
        """
        Validate IPv4 address.

        Args:
            value: IP address string.

        Returns:
            ValidationResult.
        """
        if not self.IPV4_PATTERN.match(value):
            return ValidationResult(False, "Invalid IPv4 address")
        return ValidationResult(True)

    def add_rule(
        self,
        name: str,
        validator: Callable[[Any], ValidationResult],
    ) -> None:
        """
        Add a custom validation rule.

        Args:
            name: Rule name.
            validator: Validation function.
        """
        self._custom_rules[name] = validator

    def validate_with_rule(
        self,
        value: Any,
        rule_name: str,
    ) -> ValidationResult:
        """
        Validate using a custom rule.

        Args:
            value: Value to validate.
            rule_name: Name of registered rule.

        Returns:
            ValidationResult or error if rule not found.
        """
        if rule_name not in self._custom_rules:
            return ValidationResult(False, f"Unknown validation rule: {rule_name}")
        return self._custom_rules[rule_name](value)

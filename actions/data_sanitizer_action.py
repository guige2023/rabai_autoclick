"""
Data sanitizer module for sensitive data detection and redaction.

Supports PII detection, data masking, and secure data handling.
"""
from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class SanitizationType(Enum):
    """Types of data sanitization."""
    MASK = "mask"
    REDACT = "redact"
    HASH = "hash"
    TOKENIZE = "tokenize"
    PARTIAL = "partial"
    FINGERPRINT = "fingerprint"


class DataCategory(Enum):
    """Categories of sensitive data."""
    EMAIL = "email"
    PHONE = "phone"
    CREDIT_CARD = "credit_card"
    SSN = "ssn"
    PASSWORD = "password"
    API_KEY = "api_key"
    IP_ADDRESS = "ip_address"
    NAME = "name"
    ADDRESS = "address"
    DATE_OF_BIRTH = "date_of_birth"


@dataclass
class SensitiveDataPattern:
    """A pattern for detecting sensitive data."""
    category: DataCategory
    pattern: str
    replacement: str
    description: str = ""


@dataclass
class SanitizationRule:
    """A rule for data sanitization."""
    field: str
    category: DataCategory
    sanitization_type: SanitizationType
    custom_replacement: Optional[str] = None


@dataclass
class SanitizationResult:
    """Result of sanitization."""
    original_value: str
    sanitized_value: str
    category: DataCategory
    field: str


class DataSanitizer:
    """
    Data sanitizer for sensitive data detection and redaction.

    Supports PII detection, data masking, and secure data handling.
    """

    def __init__(self):
        self._patterns: list[SensitiveDataPattern] = []
        self._rules: list[SanitizationRule] = []
        self._token_map: dict[str, str] = {}
        self._initialize_default_patterns()

    def _initialize_default_patterns(self) -> None:
        """Initialize default sensitive data patterns."""
        self.add_pattern(
            DataCategory.EMAIL,
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            "[EMAIL]"
        )

        self.add_pattern(
            DataCategory.PHONE,
            r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
            "[PHONE]"
        )

        self.add_pattern(
            DataCategory.CREDIT_CARD,
            r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
            "[CREDIT_CARD]"
        )

        self.add_pattern(
            DataCategory.SSN,
            r"\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b",
            "[SSN]"
        )

        self.add_pattern(
            DataCategory.API_KEY,
            r"\b[a-zA-Z0-9]{32,}\b",
            "[API_KEY]"
        )

        self.add_pattern(
            DataCategory.IP_ADDRESS,
            r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
            "[IP_ADDRESS]"
        )

    def add_pattern(
        self,
        category: DataCategory,
        pattern: str,
        replacement: str,
        description: str = "",
    ) -> None:
        """Add a sensitive data pattern."""
        sensitive_pattern = SensitiveDataPattern(
            category=category,
            pattern=pattern,
            replacement=replacement,
            description=description,
        )
        self._patterns.append(sensitive_pattern)

    def add_rule(
        self,
        field: str,
        category: DataCategory,
        sanitization_type: SanitizationType,
        custom_replacement: Optional[str] = None,
    ) -> None:
        """Add a field-specific sanitization rule."""
        rule = SanitizationRule(
            field=field,
            category=category,
            sanitization_type=sanitization_type,
            custom_replacement=custom_replacement,
        )
        self._rules.append(rule)

    def sanitize_value(
        self,
        value: str,
        category: DataCategory,
        sanitization_type: SanitizationType = SanitizationType.MASK,
        custom_replacement: Optional[str] = None,
    ) -> str:
        """Sanitize a single value."""
        if sanitization_type == SanitizationType.MASK:
            return self._mask_value(value, category, custom_replacement)
        elif sanitization_type == SanitizationType.REDACT:
            return "[REDACTED]"
        elif sanitization_type == SanitizationType.HASH:
            return self._hash_value(value)
        elif sanitization_type == SanitizationType.TOKENIZE:
            return self._tokenize_value(value)
        elif sanitization_type == SanitizationType.PARTIAL:
            return self._partial_mask(value, category)
        elif sanitization_type == SanitizationType.FINGERPRINT:
            return self._fingerprint_value(value)

        return value

    def _mask_value(
        self,
        value: str,
        category: DataCategory,
        custom_replacement: Optional[str] = None,
    ) -> str:
        """Mask a sensitive value."""
        if category == DataCategory.EMAIL:
            if "@" in value:
                local, domain = value.split("@")
                return f"{local[0]}{'*' * (len(local) - 1)}@{domain}"
        elif category == DataCategory.PHONE:
            return re.sub(r"\d(?=\d{4})", "*", value)
        elif category == DataCategory.CREDIT_CARD:
            return re.sub(r"\d(?=\d{4})", "*", value)
        elif category == DataCategory.SSN:
            return re.sub(r"\d(?=\d{4})", "*", value)
        elif category == DataCategory.PASSWORD:
            return "*" * len(value)

        return custom_replacement or "[MASKED]"

    def _partial_mask(
        self,
        value: str,
        category: DataCategory,
    ) -> str:
        """Partially mask a value, showing some characters."""
        if len(value) <= 4:
            return "*" * len(value)

        if category == DataCategory.NAME:
            return f"{value[0]}{'*' * (len(value) - 1)}"

        visible_chars = 4
        return f"{'*' * (len(value) - visible_chars)}{value[-visible_chars:]}"

    def _hash_value(self, value: str) -> str:
        """Hash a value using SHA256."""
        import hashlib
        return hashlib.sha256(value.encode()).hexdigest()[:16]

    def _tokenize_value(self, value: str) -> str:
        """Tokenize a value with a consistent token."""
        if value not in self._token_map:
            self._token_map[value] = f"[TOKEN_{uuid.uuid4().hex[:8]}]"
        return self._token_map[value]

    def _fingerprint_value(self, value: str) -> str:
        """Create a fingerprint of a value."""
        import hashlib
        return hashlib.md5(value.encode()).hexdigest()[:8]

    def sanitize_data(
        self,
        data: dict,
        rules: Optional[list[SanitizationRule]] = None,
    ) -> dict:
        """Sanitize a data dictionary."""
        result = data.copy()

        for rule in rules or self._rules:
            if rule.field in result:
                result[rule.field] = self.sanitize_value(
                    str(result[rule.field]),
                    rule.category,
                    rule.sanitization_type,
                    rule.custom_replacement,
                )

        return result

    def scan_data(
        self,
        data: dict,
        scan_strings: bool = True,
    ) -> list[SanitizationResult]:
        """Scan data for sensitive information."""
        results = []

        for key, value in data.items():
            if isinstance(value, str):
                detected = self._detect_sensitive_data(value, key)
                results.extend(detected)

            if scan_strings and isinstance(value, str):
                for pattern in self._patterns:
                    matches = re.finditer(pattern.pattern, value)
                    for match in matches:
                        results.append(SanitizationResult(
                            original_value=match.group(),
                            sanitized_value=pattern.replacement,
                            category=pattern.category,
                            field=key,
                        ))

        return results

    def _detect_sensitive_data(
        self,
        value: str,
        field_name: str,
    ) -> list[SanitizationResult]:
        """Detect sensitive data in a field value."""
        results = []

        field_lower = field_name.lower()

        if "email" in field_lower:
            if "@" in value:
                results.append(SanitizationResult(
                    original_value=value,
                    sanitized_value=self.sanitize_value(value, DataCategory.EMAIL),
                    category=DataCategory.EMAIL,
                    field=field_name,
                ))

        elif "phone" in field_lower or "mobile" in field_lower:
            results.append(SanitizationResult(
                original_value=value,
                sanitized_value=self.sanitize_value(value, DataCategory.PHONE),
                category=DataCategory.PHONE,
                field=field_name,
            ))

        elif "password" in field_lower or "secret" in field_lower:
            results.append(SanitizationResult(
                original_value=value,
                sanitized_value=self.sanitize_value(value, DataCategory.PASSWORD, SanitizationType.REDACT),
                category=DataCategory.PASSWORD,
                field=field_name,
            ))

        return results

    def auto_detect_and_sanitize(
        self,
        data: dict,
    ) -> tuple[dict, list[SanitizationResult]]:
        """Automatically detect and sanitize sensitive data."""
        results = self.scan_data(data)

        sanitized = data.copy()
        for result in results:
            if result.field in sanitized:
                sanitized[result.field] = result.sanitized_value

        return sanitized, results

    def list_patterns(self) -> list[SensitiveDataPattern]:
        """List all sensitive data patterns."""
        return self._patterns

    def list_rules(self) -> list[SanitizationRule]:
        """List all sanitization rules."""
        return self._rules

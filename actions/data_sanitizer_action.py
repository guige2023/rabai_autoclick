"""
Data Sanitizer Action Module.

Sanitizes sensitive data from datasets with configurable redaction rules,
 PII detection, and data masking patterns.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SanitizationType(Enum):
    """Type of sanitization to apply."""
    REDACT = "redact"
    MASK = "mask"
    HASH = "hash"
    TOKENIZE = "tokenize"
    PARTIAL = "partial"


@dataclass
class SanitizationRule:
    """A sanitization rule for a field."""
    field: str
    sanitization_type: SanitizationType
    pattern: Optional[str] = None
    replacement: str = "***"
    hash_algorithm: str = "sha256"


@dataclass
class SanitizationResult:
    """Result of sanitization operation."""
    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    fields_sanitized: int = 0
    pii_detected: list[str] = field(default_factory=list)


class DataSanitizerAction:
    """
    Data sanitization for PII and sensitive information.

    Detects and sanitizes sensitive data including emails, phones,
    SSNs, credit cards, and custom patterns.

    Example:
        sanitizer = DataSanitizerAction()
        sanitizer.add_rule("email", SanitizationType.MASK)
        sanitizer.add_rule("ssn", SanitizationType.REDACT)
        sanitizer.add_pattern_rule("custom_id", r"ID-\d{6}")
        result = sanitizer.sanitize(record)
    """

    def __init__(self) -> None:
        self._rules: list[SanitizationRule] = []
        self._pii_patterns: dict[str, re.Pattern] = {
            "email": re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            "phone": re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'),
            "ssn": re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
            "credit_card": re.compile(r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'),
        }

    def add_rule(
        self,
        field: str,
        sanitization_type: SanitizationType,
        replacement: str = "***",
    ) -> "DataSanitizerAction":
        """Add a sanitization rule for a field."""
        rule = SanitizationRule(
            field=field,
            sanitization_type=sanitization_type,
            replacement=replacement,
        )
        self._rules.append(rule)
        return self

    def add_pattern_rule(
        self,
        field: str,
        pattern: str,
        sanitization_type: SanitizationType = SanitizationType.REDACT,
        replacement: str = "***",
    ) -> "DataSanitizerAction":
        """Add a rule based on regex pattern."""
        rule = SanitizationRule(
            field=field,
            sanitization_type=sanitization_type,
            pattern=pattern,
            replacement=replacement,
        )
        self._rules.append(rule)
        return self

    def detect_pii(self, data: dict[str, Any]) -> list[str]:
        """Detect PII fields in data without modifying."""
        pii_found: list[str] = []

        for field_name, value in data.items():
            if not isinstance(value, str):
                continue

            for pii_type, pattern in self._pii_patterns.items():
                if pattern.search(value):
                    pii_found.append(f"{field_name} ({pii_type})")

        return pii_found

    def sanitize(
        self,
        data: dict[str, Any],
        detect_only: bool = False,
    ) -> SanitizationResult:
        """Sanitize sensitive data according to rules."""
        result_data = dict(data)
        fields_sanitized = 0
        pii_detected: list[str] = []

        for field_name, value in data.items():
            rule = self._find_rule(field_name)

            if rule:
                sanitized = self._apply_sanitization(value, rule)
                result_data[field_name] = sanitized
                fields_sanitized += 1
                pii_detected.append(field_name)

            elif isinstance(value, str):
                for pii_type, pattern in self._pii_patterns.items():
                    if pattern.search(value):
                        pii_detected.append(f"{field_name} ({pii_type})")

        return SanitizationResult(
            success=True,
            data=result_data,
            fields_sanitized=fields_sanitized,
            pii_detected=pii_detected,
        )

    def _find_rule(self, field_name: str) -> Optional[SanitizationRule]:
        """Find applicable rule for field."""
        for rule in self._rules:
            if rule.field == field_name:
                return rule
        return None

    def _apply_sanitization(
        self,
        value: Any,
        rule: SanitizationRule,
    ) -> str:
        """Apply sanitization to a value."""
        str_value = str(value)

        if rule.sanitization_type == SanitizationType.REDACT:
            return rule.replacement

        elif rule.sanitization_type == SanitizationType.MASK:
            return self._mask_value(str_value)

        elif rule.sanitization_type == SanitizationType.HASH:
            import hashlib
            if rule.hash_algorithm == "sha256":
                return hashlib.sha256(str_value.encode()).hexdigest()[:16]
            return str_value

        elif rule.sanitization_type == SanitizationType.PARTIAL:
            return self._partial_mask(str_value)

        return str_value

    def _mask_value(self, value: str) -> str:
        """Mask a value showing only first/last chars."""
        if len(value) <= 4:
            return "*" * len(value)
        return value[:2] + "*" * (len(value) - 4) + value[-2:]

    def _partial_mask(self, value: str) -> str:
        """Partially mask showing only last 4 characters."""
        if len(value) <= 4:
            return "*" * len(value)
        return "*" * (len(value) - 4) + value[-4:]

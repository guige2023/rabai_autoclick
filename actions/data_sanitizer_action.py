# Copyright (c) 2024. coded by claude
"""Data Sanitizer Action Module.

Provides data sanitization utilities for API responses including
PII detection, field masking, and data redaction.
"""
from typing import Optional, Dict, Any, List, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
import re
import logging

logger = logging.getLogger(__name__)


class SanitizationLevel(Enum):
    NONE = "none"
    PARTIAL = "partial"
    FULL = "full"


@dataclass
class SanitizationRule:
    field_pattern: str
    replacement: str
    regex: bool = False


@dataclass
class PIIPattern:
    name: str
    pattern: str
    replacement: str


@dataclass
class SanitizationConfig:
    level: SanitizationLevel = SanitizationLevel.PARTIAL
    rules: List[SanitizationRule] = field(default_factory=list)
    pii_patterns: List[PIIPattern] = field(default_factory=list)
    mask_char: str = "*"
    preserve_length: bool = True


class DataSanitizer:
    DEFAULT_PII_PATTERNS = [
        PIIPattern("email", r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "[EMAIL_REDACTED]"),
        PIIPattern("phone", r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", "[PHONE_REDACTED]"),
        PIIPattern("ssn", r"\b\d{3}-\d{2}-\d{4}\b", "[SSN_REDACTED]"),
        PIIPattern("credit_card", r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b", "[CARD_REDACTED]"),
    ]

    def __init__(self, config: Optional[SanitizationConfig] = None):
        self.config = config or SanitizationConfig()
        for pattern in self.DEFAULT_PII_PATTERNS:
            if not any(p.name == pattern.name for p in self.config.pii_patterns):
                self.config.pii_patterns.append(pattern)

    def sanitize(self, data: Any, level: Optional[SanitizationLevel] = None) -> Any:
        effective_level = level or self.config.level
        if effective_level == SanitizationLevel.NONE:
            return data
        if isinstance(data, dict):
            return self._sanitize_dict(data, effective_level)
        elif isinstance(data, list):
            return [self.sanitize(item, effective_level) for item in data]
        elif isinstance(data, str):
            return self._sanitize_string(data)
        return data

    def _sanitize_dict(self, data: Dict[str, Any], level: SanitizationLevel) -> Dict[str, Any]:
        result = {}
        for key, value in data.items():
            if self._should_sanitize_field(key):
                result[key] = self._sanitize_value(value, level)
            elif isinstance(value, dict):
                result[key] = self._sanitize_dict(value, level)
            elif isinstance(value, list):
                result[key] = [self.sanitize(item, level) for item in value]
            else:
                result[key] = value
        return result

    def _sanitize_value(self, value: Any, level: SanitizationLevel) -> Any:
        if level == SanitizationLevel.FULL:
            if isinstance(value, str):
                return self.config.mask_char * 10
            return value
        elif level == SanitizationLevel.PARTIAL:
            if isinstance(value, str):
                return self._sanitize_string(value)
            return value
        return value

    def _sanitize_string(self, text: str) -> str:
        for pii in self.config.pii_patterns:
            text = re.sub(pii.pattern, pii.replacement, text)
        for rule in self.config.rules:
            if rule.regex:
                text = re.sub(rule.pattern, rule.replacement, text)
            else:
                text = text.replace(rule.pattern, rule.replacement)
        return text

    def _should_sanitize_field(self, field_name: str) -> bool:
        sensitive_patterns = {"password", "secret", "token", "key", "auth", "credential", "ssn", "credit"}
        field_lower = field_name.lower()
        return any(pattern in field_lower for pattern in sensitive_patterns)

    def add_rule(self, rule: SanitizationRule) -> None:
        self.config.rules.append(rule)

    def add_pii_pattern(self, pattern: PIIPattern) -> None:
        self.config.pii_patterns.append(pattern)

    def mask_value(self, value: str, visible_chars: int = 4) -> str:
        if len(value) <= visible_chars:
            return self.config.mask_char * len(value)
        return value[:visible_chars] + self.config.mask_char * (len(value) - visible_chars)

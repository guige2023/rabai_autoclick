"""Data sanitization and masking for automation workflows.

Provides PII detection, anonymization, and data masking
to ensure sensitive information is properly protected.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

import copy


class SanitizationLevel(Enum):
    """Level of sanitization to apply."""
    NONE = "none"
    PARTIAL = "partial"
    FULL = "full"
    HASH = "hash"
    REDACT = "redact"


class DataCategory(Enum):
    """Category of sensitive data."""
    EMAIL = "email"
    PHONE = "phone"
    CREDIT_CARD = "credit_card"
    SSN = "ssn"
    NAME = "name"
    ADDRESS = "address"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "date_of_birth"
    PASSWORD = "password"
    API_KEY = "api_key"
    CUSTOM = "custom"


@dataclass
class SanitizationRule:
    """A rule for sanitizing data."""
    rule_id: str
    category: DataCategory
    pattern: Optional[str] = None
    replacement: Optional[str] = None
    mask_char: str = "*"
    mask_length: Optional[int] = None
    preserve_last: int = 0
    preserve_first: int = 0
    enabled: bool = True
    priority: int = 100


@dataclass
class DetectionResult:
    """Result of sensitive data detection."""
    field: str
    category: DataCategory
    value: str
    start_index: int
    end_index: int
    confidence: float


@dataclass
class SanitizationResult:
    """Result of sanitization operation."""
    original_value: Any
    sanitized_value: Any
    field: str
    rule_applied: Optional[str] = None
    category: Optional[DataCategory] = None
    timestamp: float = field(default_factory=time.time)


class PIIPatterns:
    """Pre-built regex patterns for PII detection."""

    EMAIL = re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    )
    PHONE_US = re.compile(
        r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b'
    )
    PHONE_INT = re.compile(
        r'\b\+?[0-9]{1,3}[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{2,4}\b'
    )
    CREDIT_CARD = re.compile(
        r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b'
    )
    SSN = re.compile(
        r'\b[0-9]{3}[-.\s]?[0-9]{2}[-.\s]?[0-9]{4}\b'
    )
    IP_V4 = re.compile(
        r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
    )
    IP_V6 = re.compile(
        r'\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b'
    )
    DATE_ISO = re.compile(
        r'\b[0-9]{4}-[0-9]{2}-[0-9]{2}\b'
    )
    DATE_US = re.compile(
        r'\b(?:0?[1-9]|1[0-2])/(?:0?[1-9]|[12][0-9]|3[01])/(?:19|20)[0-9]{2}\b'
    )


class DataSanitizer:
    """Core data sanitization engine."""

    def __init__(self):
        self._rules: Dict[str, SanitizationRule] = {}
        self._lock = threading.RLock()
        self._field_mappings: Dict[str, DataCategory] = {}
        self._setup_default_rules()

    def _setup_default_rules(self) -> None:
        """Set up default sanitization rules."""
        default_rules = [
            SanitizationRule(
                rule_id="email_partial",
                category=DataCategory.EMAIL,
                mask_length=3,
                preserve_last=0,
                preserve_first=1,
            ),
            SanitizationRule(
                rule_id="phone_partial",
                category=DataCategory.PHONE,
                mask_length=4,
                preserve_last=0,
                preserve_first=0,
            ),
            SanitizationRule(
                rule_id="cc_full",
                category=DataCategory.CREDIT_CARD,
                mask_char="X",
                preserve_last=4,
                preserve_first=0,
            ),
            SanitizationRule(
                rule_id="ssn_full",
                category=DataCategory.SSN,
                mask_char="X",
                preserve_last=0,
                preserve_first=0,
            ),
            SanitizationRule(
                rule_id="ip_hash",
                category=DataCategory.IP_ADDRESS,
            ),
        ]

        for rule in default_rules:
            self._rules[rule.rule_id] = rule

        self._field_mappings = {
            "email": DataCategory.EMAIL,
            "email_address": DataCategory.EMAIL,
            "phone": DataCategory.PHONE,
            "phone_number": DataCategory.PHONE,
            "mobile": DataCategory.PHONE,
            "credit_card": DataCategory.CREDIT_CARD,
            "cc_number": DataCategory.CREDIT_CARD,
            "card_number": DataCategory.CREDIT_CARD,
            "ssn": DataCategory.SSN,
            "social_security": DataCategory.SSN,
            "password": DataCategory.PASSWORD,
            "pwd": DataCategory.PASSWORD,
            "api_key": DataCategory.API_KEY,
            "apikey": DataCategory.API_KEY,
            "secret": DataCategory.API_KEY,
            "ip_address": DataCategory.IP_ADDRESS,
            "ip": DataCategory.IP_ADDRESS,
            "client_ip": DataCategory.IP_ADDRESS,
        }

    def detect_pii(self, text: str) -> List[DetectionResult]:
        """Detect PII in text."""
        results = []

        detections = [
            (DataCategory.EMAIL, PIIPatterns.EMAIL),
            (DataCategory.PHONE, PIIPatterns.PHONE_US),
            (DataCategory.CREDIT_CARD, PIIPatterns.CREDIT_CARD),
            (DataCategory.SSN, PIIPatterns.SSN),
            (DataCategory.IP_ADDRESS, PIIPatterns.IP_V4),
        ]

        for category, pattern in detections:
            for match in pattern.finditer(text):
                confidence = 1.0 if category == DataCategory.CREDIT_CARD else 0.9
                results.append(DetectionResult(
                    field="text",
                    category=category,
                    value=match.group(),
                    start_index=match.start(),
                    end_index=match.end(),
                    confidence=confidence,
                ))

        return results

    def mask_email(self, email: str, level: SanitizationLevel = SanitizationLevel.PARTIAL) -> str:
        """Mask an email address."""
        if not email or "@" not in email:
            return email

        local, domain = email.rsplit("@", 1)
        domain_parts = domain.rsplit(".", 1)

        if level == SanitizationLevel.NONE:
            return email
        elif level == SanitizationLevel.FULL:
            return "*" * len(email)
        elif level == SanitizationLevel.HASH:
            return hashlib.sha256(email.encode()).hexdigest()[:16] + "@" + domain
        elif level == SanitizationLevel.PARTIAL:
            masked_local = local[0] + "*" * (len(local) - 1) if len(local) > 1 else local
            masked_domain = domain if len(domain_parts) < 2 else "*" * len(domain_parts[0]) + "." + domain_parts[1]
            return masked_local + "@" + masked_domain
        elif level == SanitizationLevel.REDACT:
            return "[EMAIL REDACTED]"

        return email

    def mask_phone(
        self,
        phone: str,
        level: SanitizationLevel = SanitizationLevel.PARTIAL,
        preserve_last: int = 4,
    ) -> str:
        """Mask a phone number."""
        digits = re.sub(r'\D', '', phone)
        if len(digits) < preserve_last:
            return "*" * len(phone)

        if level == SanitizationLevel.NONE:
            return phone
        elif level == SanitizationLevel.FULL:
            return "*" * len(phone)
        elif level == SanitizationLevel.HASH:
            return hashlib.sha256(digits.encode()).hexdigest()[:10]
        elif level == SanitizationLevel.PARTIAL:
            visible = digits[-preserve_last:] if preserve_last else ""
            masked = "*" * (len(digits) - preserve_last)
            return masked + visible
        elif level == SanitizationLevel.REDACT:
            return "[PHONE REDACTED]"

        return phone

    def mask_credit_card(
        self,
        cc: str,
        preserve_last: int = 4,
    ) -> str:
        """Mask a credit card number."""
        digits = re.sub(r'\D', '', cc)
        if len(digits) < preserve_last:
            return "*" * len(cc)

        visible = digits[-preserve_last:]
        masked = "*" * (len(digits) - preserve_last)
        return masked + visible

    def mask_ssn(self, ssn: str) -> str:
        """Mask a Social Security Number."""
        digits = re.sub(r'\D', '', ssn)
        if len(digits) != 9:
            return "*" * len(ssn)
        return "***-**-" + digits[-4:]

    def mask_ip(self, ip: str) -> str:
        """Hash an IP address for anonymization."""
        return hashlib.sha256(ip.encode()).hexdigest()[:16]

    def mask_password(self, password: str) -> str:
        """Always mask passwords completely."""
        return "********"

    def mask_custom(
        self,
        value: str,
        mask_char: str = "*",
        preserve_length: int = 0,
    ) -> str:
        """Apply custom masking."""
        if preserve_length > 0 and preserve_length < len(value):
            return mask_char * (len(value) - preserve_length) + value[-preserve_length:]
        return mask_char * len(value)

    def sanitize_value(
        self,
        value: Any,
        category: DataCategory,
        level: SanitizationLevel = SanitizationLevel.PARTIAL,
        rule: Optional[SanitizationRule] = None,
    ) -> Any:
        """Sanitize a single value based on its category."""
        if value is None:
            return value

        str_value = str(value)

        if level == SanitizationLevel.NONE:
            return value

        if level == SanitizationLevel.REDACT:
            return f"[{category.value.upper()} REDACTED]"

        if level == SanitizationLevel.HASH:
            return hashlib.sha256(str_value.encode()).hexdigest()

        if category == DataCategory.EMAIL:
            return self.mask_email(str_value, level)
        elif category == DataCategory.PHONE:
            preserve = rule.preserve_last if rule else 4
            return self.mask_phone(str_value, level, preserve)
        elif category == DataCategory.CREDIT_CARD:
            preserve = rule.preserve_last if rule else 4
            return self.mask_credit_card(str_value, preserve)
        elif category == DataCategory.SSN:
            return self.mask_ssn(str_value)
        elif category == DataCategory.IP_ADDRESS:
            return self.mask_ip(str_value)
        elif category == DataCategory.PASSWORD:
            return self.mask_password(str_value)
        else:
            return self.mask_custom(
                str_value,
                rule.mask_char if rule else "*",
                rule.preserve_last if rule else 0,
            )

    def sanitize_dict(
        self,
        data: Dict[str, Any],
        level: SanitizationLevel = SanitizationLevel.PARTIAL,
        known_fields: Optional[Dict[str, DataCategory]] = None,
    ) -> Dict[str, Any]:
        """Sanitize a dictionary by detecting and masking sensitive fields."""
        result = copy.deepcopy(data)
        known_fields = known_fields or self._field_mappings

        for key, value in result.items():
            if value is None:
                continue

            key_lower = key.lower()

            if key_lower in known_fields:
                category = known_fields[key_lower]
                result[key] = self.sanitize_value(value, category, level)
            elif isinstance(value, str):
                detections = self.detect_pii(value)
                if detections:
                    sanitized = value
                    for detection in reversed(detections):
                        cat = detection.category
                        masked = self.sanitize_value(detection.value, cat, level)
                        sanitized = sanitized[:detection.start_index] + masked + sanitized[detection.end_index:]
                    result[key] = sanitized
            elif isinstance(value, dict):
                result[key] = self.sanitize_dict(value, level, known_fields)
            elif isinstance(value, list):
                result[key] = [
                    self.sanitize_dict({str(i): v}, level, known_fields).get(str(i), v)
                    for i, v in enumerate(value)
                ]

        return result


class AutomationSanitizerAction:
    """Action providing data sanitization for automation workflows."""

    def __init__(self, sanitizer: Optional[DataSanitizer] = None):
        self._sanitizer = sanitizer or DataSanitizer()

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute sanitization on data.

        Required params:
            data: dict - Data to sanitize
            level: str - Sanitization level (none, partial, full, hash, redact)

        Optional params:
            field_mappings: dict - Custom field-to-category mappings
            field: str - Single field to sanitize
            category: str - Category for single field
        """
        data = params.get("data")
        level_str = params.get("level", "partial")
        field_mappings = params.get("field_mappings", {})
        single_field = params.get("field")
        single_category = params.get("category")

        if data is None:
            raise ValueError("data is required")

        try:
            level = SanitizationLevel(level_str.lower())
        except ValueError:
            level = SanitizationLevel.PARTIAL

        if single_field and single_category:
            try:
                category = DataCategory(single_category.lower())
            except ValueError:
                category = DataCategory.CUSTOM

            sanitized = self._sanitizer.sanitize_value(
                data.get(single_field, data),
                category,
                level,
            )
            return {
                "field": single_field,
                "original_value": data.get(single_field),
                "sanitized_value": sanitized,
                "category": category.value,
                "level": level.value,
            }

        all_mappings = {**self._sanitizer._field_mappings, **{k.lower(): DataCategory(v.lower()) for k, v in field_mappings.items()}}
        sanitized_data = self._sanitizer.sanitize_dict(data, level, all_mappings)

        return {
            "data": sanitized_data,
            "level": level.value,
            "fields_sanitized": len(data),
        }

    def detect(
        self,
        data: Any,
        fields_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """Detect sensitive data in input.

        If fields_only=True, returns detected fields from dict keys.
        Otherwise, performs deep detection on values.
        """
        if isinstance(data, dict):
            detected = []
            for key, value in data.items():
                key_lower = key.lower()
                if key_lower in self._sanitizer._field_mappings:
                    detected.append({
                        "field": key,
                        "category": self._sanitizer._field_mappings[key_lower].value,
                        "type": "field_name",
                    })
                elif isinstance(value, str):
                    detections = self._sanitizer.detect_pii(value)
                    for d in detections:
                        detected.append({
                            "field": key,
                            "category": d.category.value,
                            "value": d.value[:4] + "***",
                            "confidence": d.confidence,
                            "type": "value_match",
                        })
            return detected

        elif isinstance(data, str):
            detections = self._sanitizer.detect_pii(data)
            return [
                {
                    "category": d.category.value,
                    "value": d.value[:4] + "***",
                    "start": d.start_index,
                    "end": d.end_index,
                    "confidence": d.confidence,
                }
                for d in detections
            ]

        return []

    def add_field_mapping(self, field_name: str, category: str) -> None:
        """Add a field-to-category mapping."""
        try:
            category_enum = DataCategory(category.lower())
            self._sanitizer._field_mappings[field_name.lower()] = category_enum
        except ValueError:
            pass

    def set_rule(self, rule: Dict[str, Any]) -> None:
        """Set a custom sanitization rule."""
        try:
            category = DataCategory(rule.get("category", "custom").lower())
            sanitization_rule = SanitizationRule(
                rule_id=rule.get("rule_id", str(uuid.uuid4())[:12]),
                category=category,
                pattern=rule.get("pattern"),
                replacement=rule.get("replacement"),
                mask_char=rule.get("mask_char", "*"),
                mask_length=rule.get("mask_length"),
                preserve_last=rule.get("preserve_last", 0),
                preserve_first=rule.get("preserve_first", 0),
                enabled=rule.get("enabled", True),
                priority=rule.get("priority", 100),
            )
            self._sanitizer._rules[sanitization_rule.rule_id] = sanitization_rule
        except Exception:
            pass

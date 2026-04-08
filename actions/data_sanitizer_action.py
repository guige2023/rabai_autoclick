"""Data Sanitizer Action Module.

Provides data sanitization, masking, and privacy
protection for sensitive information.
"""

from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import re
import hashlib
import json
from datetime import datetime


class SanitizationType(Enum):
    """Types of sanitization operations."""
    MASK = "mask"
    REDACT = "redact"
    HASH = "hash"
    ENCRYPT = "encrypt"
    DECRYPT = "decrypt"
    TOKENIZE = "tokenize"
    REMOVE = "remove"


class DataCategory(Enum):
    """Categories of sensitive data."""
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    PASSWORD = "password"
    API_KEY = "api_key"
    IP_ADDRESS = "ip_address"
    NAME = "name"
    ADDRESS = "address"
    DATE_OF_BIRTH = "date_of_birth"


@dataclass
class SanitizationRule:
    """Defines a sanitization rule."""
    name: str
    category: DataCategory
    pattern: Optional[str] = None
    replacement: str = "***REDACTED***"
    hash_algorithm: Optional[str] = None
    preserve_first: int = 0
    preserve_last: int = 0
    custom_sanitizer: Optional[Callable[[str], str]] = None


@dataclass
class SanitizationConfig:
    """Configuration for sanitization."""
    rules: List[SanitizationRule] = field(default_factory=list)
    default_replacement: str = "***REDACTED***"
    hash_salt: Optional[str] = None


class PatternSanitizer:
    """Sanitizes data based on regex patterns."""

    def __init__(self):
        self._patterns: Dict[str, re.Pattern] = {}

    def add_pattern(self, name: str, pattern: str):
        """Add a regex pattern."""
        self._patterns[name] = re.compile(pattern)

    def sanitize(self, text: str, pattern_name: str, replacement: str) -> str:
        """Sanitize text using named pattern."""
        if pattern_name not in self._patterns:
            return text
        return self._patterns[pattern_name].sub(replacement, text)

    def sanitize_all(self, text: str, replacement: str) -> str:
        """Apply all patterns to text."""
        result = text
        for pattern in self._patterns.values():
            result = pattern.sub(replacement, result)
        return result


class EmailSanitizer:
    """Sanitizes email addresses."""

    def __init__(self, preserve_domain: bool = False):
        self.preserve_domain = preserve_domain
        self._pattern = re.compile(
            r'([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        )

    def sanitize(
        self,
        email: str,
        replacement: str = "***REDACTED***",
    ) -> str:
        """Sanitize an email address."""
        def replace(match):
            local = match.group(1)
            domain = match.group(2)

            if self.preserve_domain:
                return f"***@{domain}"
            elif local:
                return f"{local[0]}***@{domain}"
            return replacement

        return self._pattern.sub(replace, email)


class PhoneSanitizer:
    """Sanitizes phone numbers."""

    def __init__(self):
        self._patterns = [
            re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'),
            re.compile(r'\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b'),
            re.compile(r'\+\d{1,3}[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b'),
        ]

    def sanitize(
        self,
        phone: str,
        replacement: str = "***REDACTED***",
    ) -> str:
        """Sanitize a phone number."""
        result = phone
        for pattern in self._patterns:
            result = pattern.sub(replacement, result)
        return result


class SSNSanitizer:
    """Sanitizes Social Security Numbers."""

    def __init__(self):
        self._pattern = re.compile(
            r'\b\d{3}[-]?\d{2}[-]?\d{4}\b'
        )

    def sanitize(
        self,
        ssn: str,
        replacement: str = "***-**-****",
    ) -> str:
        """Sanitize an SSN."""
        def replace(match):
            ssn = match.group(0).replace("-", "")
            return f"***-**-{ssn[-4:]}"

        return self._pattern.sub(replace, ssn)


class CreditCardSanitizer:
    """Sanitizes credit card numbers."""

    def __init__(self):
        self._pattern = re.compile(
            r'\b(?:\d{4}[-\s]?){3}\d{4}\b'
        )

    def sanitize(
        self,
        card: str,
        replacement: str = "****-****-****-****",
    ) -> str:
        """Sanitize a credit card number."""
        def replace(match):
            digits = re.sub(r'[-.\s]', '', match.group(0))
            if len(digits) >= 4:
                return f"****-****-****-{digits[-4:]}"
            return replacement

        return self._pattern.sub(replace, card)


class NameSanitizer:
    """Sanitizes personal names."""

    def __init__(self):
        self._common_names: Set[str] = set()

    def sanitize(
        self,
        name: str,
        replacement: str = "[REDACTED]",
    ) -> str:
        """Sanitize a name."""
        parts = name.split()
        if len(parts) == 1:
            return f"{parts[0][0]}***"
        return f"{parts[0][0]}*** {parts[-1][0]}***"


class IPAddressSanitizer:
    """Sanitizes IP addresses."""

    def __init__(self):
        self._ipv4_pattern = re.compile(
            r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
        )
        self._ipv6_pattern = re.compile(
            r'\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b'
        )

    def sanitize(
        self,
        ip: str,
        replacement: str = "***.***.***.***",
    ) -> str:
        """Sanitize an IP address."""
        result = self._ipv4_pattern.sub(replacement, ip)
        result = self._ipv6_pattern.sub(replacement, result)
        return result


class PasswordSanitizer:
    """Sanitizes passwords."""

    def __init__(self):
        pass

    def sanitize(
        self,
        password: str,
        replacement: str = "********",
    ) -> str:
        """Sanitize a password."""
        return replacement


class HashSanitizer:
    """Hashes values for anonymization."""

    def __init__(self, salt: Optional[str] = None):
        self.salt = salt or ""

    def hash(
        self,
        value: str,
        algorithm: str = "sha256",
        preserve_length: bool = False,
    ) -> str:
        """Hash a value."""
        data = f"{self.salt}{value}".encode()

        if algorithm == "md5":
            result = hashlib.md5(data).hexdigest()
        elif algorithm == "sha1":
            result = hashlib.sha1(data).hexdigest()
        elif algorithm == "sha256":
            result = hashlib.sha256(data).hexdigest()
        elif algorithm == "blake2b":
            result = hashlib.blake2b(data).hexdigest()
        else:
            result = hashlib.sha256(data).hexdigest()

        if preserve_length:
            return result[:len(value)]

        return result


class DataMasker:
    """Masks data while preserving some information."""

    def mask(
        self,
        value: str,
        preserve_start: int = 0,
        preserve_end: int = 0,
        mask_char: str = "*",
    ) -> str:
        """Mask a value, preserving start/end characters."""
        if not value:
            return value

        mask_length = len(value) - preserve_start - preserve_end

        if mask_length <= 0:
            return mask_char * len(value)

        result = value[:preserve_start]
        result += mask_char * max(0, mask_length)
        result += value[preserve_start + mask_length if preserve_start else -preserve_end:]

        return result

    def mask_email(self, email: str) -> str:
        """Mask an email address."""
        if "@" not in email:
            return self.mask(email, preserve_start=1)

        local, domain = email.rsplit("@", 1)
        masked_local = self.mask(local, preserve_start=1, preserve_end=1)
        return f"{masked_local}@{domain}"

    def mask_phone(self, phone: str) -> str:
        """Mask a phone number."""
        digits = re.sub(r'\D', '', phone)
        if len(digits) >= 4:
            return f"***-***-{digits[-4:]}"
        return self.mask(phone)

    def mask_credit_card(self, card: str) -> str:
        """Mask a credit card number."""
        digits = re.sub(r'\D', '', card)
        if len(digits) >= 4:
            return f"****-****-****-{digits[-4:]}"
        return "****-****-****-****"


class DataSanitizer:
    """Main sanitization orchestrator."""

    def __init__(self, config: Optional[SanitizationConfig] = None):
        self.config = config or SanitizationConfig()
        self._sanitizers: Dict[DataCategory, Callable] = {}
        self._pattern_sanitizer = PatternSanitizer()
        self._hash_sanitizer = HashSanitizer()

        self._register_default_sanitizers()

    def _register_default_sanitizers(self):
        """Register default sanitizers."""
        self._sanitizers[DataCategory.EMAIL] = EmailSanitizer()
        self._sanitizers[DataCategory.PHONE] = PhoneSanitizer()
        self._sanitizers[DataCategory.SSN] = SSNSanitizer()
        self._sanitizers[DataCategory.CREDIT_CARD] = CreditCardSanitizer()
        self._sanitizers[DataCategory.PASSWORD] = PasswordSanitizer()
        self._sanitizers[DataCategory.IP_ADDRESS] = IPAddressSanitizer()
        self._sanitizers[DataCategory.NAME] = NameSanitizer()

    def register_sanitizer(
        self,
        category: DataCategory,
        sanitizer: Callable,
    ):
        """Register a custom sanitizer."""
        self._sanitizers[category] = sanitizer

    def sanitize_value(
        self,
        value: str,
        category: DataCategory,
        rule: Optional[SanitizationRule] = None,
    ) -> str:
        """Sanitize a single value."""
        if rule and rule.custom_sanitizer:
            return rule.custom_sanitizer(value)

        sanitizer = self._sanitizers.get(category)

        if isinstance(sanitizer, HashSanitizer):
            algorithm = rule.hash_algorithm if rule else "sha256"
            return sanitizer.hash(value, algorithm=algorithm)

        if sanitizer:
            replacement = rule.replacement if rule else "***REDACTED***"
            return sanitizer.sanitize(value, replacement)

        return self.config.default_replacement

    def sanitize_dict(
        self,
        data: Dict[str, Any],
        field_categories: Dict[str, DataCategory],
    ) -> Dict[str, Any]:
        """Sanitize a dictionary."""
        result = data.copy()

        for field_name, category in field_categories.items():
            if field_name in result:
                result[field_name] = self.sanitize_value(
                    str(result[field_name]),
                    category,
                )

        return result

    def sanitize_recursive(
        self,
        data: Any,
        field_categories: Optional[Dict[str, DataCategory]] = None,
    ) -> Any:
        """Recursively sanitize data structure."""
        if isinstance(data, dict):
            result = {}
            for key, value in data.items():
                if field_categories and key in field_categories:
                    result[key] = self.sanitize_value(
                        str(value),
                        field_categories[key],
                    )
                elif isinstance(value, dict):
                    result[key] = self.sanitize_recursive(value, field_categories)
                elif isinstance(value, list):
                    result[key] = [
                        self.sanitize_recursive(item, field_categories)
                        for item in value
                    ]
                else:
                    result[key] = value
            return result

        elif isinstance(data, list):
            return [
                self.sanitize_recursive(item, field_categories)
                for item in data
            ]

        return data


class DataSanitizerAction:
    """High-level data sanitizer action."""

    def __init__(
        self,
        sanitizer: Optional[DataSanitizer] = None,
        masker: Optional[DataMasker] = None,
    ):
        self.sanitizer = sanitizer or DataSanitizer()
        self.masker = masker or DataMasker()

    def sanitize(
        self,
        data: Any,
        field_categories: Dict[str, DataCategory],
    ) -> Any:
        """Sanitize data based on field categories."""
        return self.sanitizer.sanitize_recursive(data, field_categories)

    def mask_value(
        self,
        value: str,
        preserve_start: int = 0,
        preserve_end: int = 0,
    ) -> str:
        """Mask a value."""
        return self.masker.mask(value, preserve_start, preserve_end)

    def hash_value(
        self,
        value: str,
        algorithm: str = "sha256",
    ) -> str:
        """Hash a value."""
        return self._hash_sanitizer.hash(value, algorithm)

    def add_field_category(
        self,
        field_name: str,
        category: DataCategory,
    ) -> Dict[str, DataCategory]:
        """Add a field to category mapping."""
        return {field_name: category}


# Module exports
__all__ = [
    "DataSanitizerAction",
    "DataSanitizer",
    "PatternSanitizer",
    "EmailSanitizer",
    "PhoneSanitizer",
    "SSNSanitizer",
    "CreditCardSanitizer",
    "NameSanitizer",
    "IPAddressSanitizer",
    "PasswordSanitizer",
    "HashSanitizer",
    "DataMasker",
    "SanitizationRule",
    "SanitizationConfig",
    "SanitizationType",
    "DataCategory",
]

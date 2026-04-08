"""
Data masking and anonymization utilities.

Provides functions for masking sensitive data in text, logs,
and structured data with configurable masking strategies.

Example:
    >>> from utils.data_masking_utils import mask_email, mask_credit_card
    >>> mask_email("john@example.com")
    'j***@example.com'
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Callable, Dict, List, Optional, Pattern, Union


class MaskingRule:
    """Base class for masking rules."""

    def apply(self, text: str) -> str:
        """Apply the masking rule to text."""
        raise NotImplementedError


class RegexMaskingRule(MaskingRule):
    """Mask using a regular expression pattern."""

    def __init__(
        self,
        pattern: Union[str, Pattern],
        replacement: str = "***",
        flags: int = 0,
    ) -> None:
        """
        Initialize the regex masking rule.

        Args:
            pattern: Regex pattern to match.
            replacement: Replacement string.
            flags: Regex flags.
        """
        if isinstance(pattern, str):
            self.pattern = re.compile(pattern, flags)
        else:
            self.pattern = pattern
        self.replacement = replacement

    def apply(self, text: str) -> str:
        """Apply the regex masking rule."""
        return self.pattern.sub(self.replacement, text)


class PartialMaskingRule(MaskingRule):
    """Mask leaving some characters visible."""

    def __init__(
        self,
        pattern: Union[str, Pattern],
        visible_start: int = 2,
        visible_end: int = 2,
        mask_char: str = "*",
        flags: int = 0,
    ) -> None:
        """
        Initialize the partial masking rule.

        Args:
            pattern: Regex pattern to match.
            visible_start: Characters to keep at start.
            visible_end: Characters to keep at end.
            mask_char: Character to use for masking.
            flags: Regex flags.
        """
        if isinstance(pattern, str):
            self.pattern = re.compile(pattern, flags)
        else:
            self.pattern = pattern
        self.visible_start = visible_start
        self.visible_end = visible_end
        self.mask_char = mask_char

    def apply(self, text: str) -> str:
        """Apply partial masking, keeping start and end visible."""

        def replacer(match: re.Match) -> str:
            full = match.group(0)
            if len(full) <= self.visible_start + self.visible_end:
                return self.mask_char * len(full)

            start = full[: self.visible_start]
            end = full[-self.visible_end:] if self.visible_end else ""
            middle_len = len(full) - self.visible_start - self.visible_end
            middle = self.mask_char * middle_len
            return start + middle + end

        return self.pattern.sub(replacer, text)


class DataMasker:
    """
    Comprehensive data masker with built-in rules.

    Provides pre-configured rules for common sensitive data types
    including emails, phone numbers, credit cards, SSNs, etc.
    """

    def __init__(self) -> None:
        """Initialize the data masker."""
        self._rules: List[MaskingRule] = []
        self._add_builtin_rules()

    def _add_builtin_rules(self) -> None:
        """Add built-in masking rules."""
        self.add_rule(
            PartialMaskingRule(
                r"[\w.+-]+@[\w-]+\.[\w.-]+",
                visible_start=2,
                visible_end=0,
            )
        )
        self.add_rule(
            PartialMaskingRule(
                r"\b\d{3}[-.\s]?\d{2}[-.\s]?\d{4}\b",
                visible_start=0,
                visible_end=4,
            )
        )
        self.add_rule(
            PartialMaskingRule(
                r"\+\d{1,3}[\s.-]?\(?\d{1,4}\)?[\s.-]?\d{1,4}[\s.-]?\d{1,9}",
                visible_start=3,
                visible_end=2,
            )
        )
        self.add_rule(
            RegexMaskingRule(r"\b\d{3}-\d{2}-\d{4}\b", "***-**-****")
        )

    def add_rule(self, rule: MaskingRule) -> None:
        """Add a masking rule."""
        self._rules.append(rule)

    def mask(self, text: str) -> str:
        """
        Apply all masking rules to text.

        Args:
            text: Text to mask.

        Returns:
            Masked text.
        """
        for rule in self._rules:
            text = rule.apply(text)
        return text

    def __call__(self, text: str) -> str:
        """Allow using the masker as a function."""
        return self.mask(text)


def mask_email(email: str, visible_start: int = 2) -> str:
    """
    Mask an email address.

    Args:
        email: Email address to mask.
        visible_start: Characters to keep at start.

    Returns:
        Masked email.
    """
    pattern = r"([\w.+-]+)@([\w-]+\.[\w.-]+)"
    match = re.match(pattern, email, re.IGNORECASE)

    if not match:
        return "***@***"

    local = match.group(1)
    domain = match.group(2)

    if len(local) <= visible_start:
        masked_local = local[0] + "***"
    else:
        masked_local = local[:visible_start] + "***"

    return f"{masked_local}@{domain}"


def mask_credit_card(card: str, visible_end: int = 4) -> str:
    """
    Mask a credit card number.

    Args:
        card: Credit card number.
        visible_end: Last digits to keep visible.

    Returns:
        Masked credit card.
    """
    digits = re.sub(r"\D", "", card)

    if len(digits) < 4:
        return "*" * len(digits)

    return "*" * (len(digits) - visible_end) + digits[-visible_end:]


def mask_phone(phone: str, visible_start: int = 3) -> str:
    """
    Mask a phone number.

    Args:
        phone: Phone number to mask.
        visible_start: Digits to keep at start.

    Returns:
        Masked phone number.
    """
    digits = re.sub(r"\D", "", phone)

    if len(digits) <= visible_start:
        return "*" * len(digits)

    return digits[:visible_start] + "*" * (len(digits) - visible_start)


def mask_ssn(ssn: str) -> str:
    """
    Mask a Social Security Number.

    Args:
        ssn: SSN in format XXX-XX-XXXX.

    Returns:
        Masked SSN.
    """
    return re.sub(r"\d{3}-\d{2}-\d{4}", "***-**-****", ssn)


def mask_ip_address(ip: str) -> str:
    """
    Mask an IP address (keeps first octet for IPv4).

    Args:
        ip: IP address to mask.

    Returns:
        Masked IP address.
    """
    parts = ip.split(".")
    if len(parts) == 4:
        return parts[0] + ".*.*.*"
    return "*.*.*.*"


def mask_json(
    data: Any,
    fields: Optional[List[str]] = None,
    mask_char: str = "***",
) -> Any:
    """
    Recursively mask sensitive fields in JSON-like data.

    Args:
        data: JSON data (dict, list, or primitive).
        fields: Field names to mask (case-insensitive).
        mask_char: Character to use for masking.

    Returns:
        Data with masked fields.
    """
    fields = fields or ["password", "secret", "token", "api_key", "ssn"]

    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if any(f.lower() in key.lower() for f in fields):
                result[key] = mask_char
            else:
                result[key] = mask_json(value, fields, mask_char)
        return result
    elif isinstance(data, list):
        return [mask_json(item, fields, mask_char) for item in data]
    return data


def mask_log_line(line: str) -> str:
    """
    Mask common sensitive patterns in log lines.

    Args:
        line: Log line to mask.

    Returns:
        Masked log line.
    """
    masker = DataMasker()
    return masker.mask(line)


def create_custom_masker(
    rules: List[tuple[str, str, int]],
) -> DataMasker:
    """
    Create a custom masker with specified rules.

    Args:
        rules: List of (pattern, replacement, flags) tuples.

    Returns:
        Configured DataMasker.
    """
    masker = DataMasker()
    masker._rules.clear()

    for pattern, replacement, flags in rules:
        masker.add_rule(RegexMaskingRule(pattern, replacement, flags))

    return masker

"""Data masking action module.

Provides data anonymization and masking for sensitive information.
Supports PII detection, redaction patterns, and format-preserving masking.
"""

from __future__ import annotations

import re
import hashlib
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MaskingRule:
    """A masking rule definition."""
    name: str
    pattern: str
    replacement: str
    field: Optional[str] = None


class DataMaskAction:
    """Data masking engine.

    Masks sensitive data in records with various strategies.

    Example:
        masker = DataMaskAction()
        masker.add_rule("email", r"[\w.-]+@[\w.-]+", "***@***.com")
        masker.add_rule("phone", r"\d{3}-\d{4}", "XXX-XXXX")
        masked = masker.mask(data)
    """

    def __init__(self) -> None:
        """Initialize data masker."""
        self._rules: List[MaskingRule] = []
        self._field_masks: Dict[str, Callable[[Any], Any]] = {}

    def add_rule(
        self,
        name: str,
        pattern: str,
        replacement: str,
        field: Optional[str] = None,
    ) -> "DataMaskAction":
        """Add a regex-based masking rule.

        Args:
            name: Rule name.
            pattern: Regex pattern to match.
            replacement: Replacement string.
            field: Optional field name to apply rule to.

        Returns:
            Self for chaining.
        """
        self._rules.append(MaskingRule(
            name=name,
            pattern=pattern,
            replacement=replacement,
            field=field,
        ))
        return self

    def add_field_mask(
        self,
        field: str,
        mask_func: Callable[[Any], Any],
    ) -> "DataMaskAction":
        """Add a field-specific masking function.

        Args:
            field: Field name.
            mask_func: Function that takes value and returns masked value.

        Returns:
            Self for chaining.
        """
        self._field_masks[field] = mask_func
        return self

    def mask_email(self, value: Any) -> str:
        """Mask an email address."""
        s = str(value)
        return re.sub(r"[\w.-]+(@[\w.-]+)", "***\\1", s)

    def mask_phone(self, value: Any) -> str:
        """Mask a phone number."""
        s = str(value)
        digits = re.sub(r"\D", "", s)
        if len(digits) == 11:
            return f"***-****-{digits[-4:]}"
        elif len(digits) >= 7:
            return f"***-****-{digits[-4:]}"
        return "***-****"

    def mask_credit_card(self, value: Any) -> str:
        """Mask a credit card number."""
        s = str(value)
        digits = re.sub(r"\D", "", s)
        if len(digits) >= 4:
            return f"****-****-****-{digits[-4:]}"
        return "****-****-****-****"

    def mask_ssn(self, value: Any) -> str:
        """Mask a Social Security Number."""
        s = str(value)
        digits = re.sub(r"\D", "", s)
        if len(digits) == 9:
            return f"***-**-{digits[-4:]}"
        return "***-**-****"

    def mask_name(self, value: Any) -> str:
        """Mask a person's name."""
        s = str(value).split()
        if len(s) >= 2:
            return f"{s[0][0]}. ***"
        return "***"

    def hash_value(self, value: Any, salt: str = "") -> str:
        """Hash a value with SHA-256."""
        s = f"{salt}{value}"
        return hashlib.sha256(s.encode()).hexdigest()[:16]

    def redact(self, value: Any, char: str = "*") -> str:
        """Redact all characters."""
        return char * len(str(value))

    def mask(
        self,
        data: List[Dict[str, Any]],
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Apply masking to data records.

        Args:
            data: List of dicts to mask.
            fields: Optional list of fields to mask (None = all fields).

        Returns:
            Masked data.
        """
        masked = []

        for record in data:
            new_record = dict(record)

            for field_name, mask_func in self._field_masks.items():
                if field_name in new_record:
                    new_record[field_name] = mask_func(new_record[field_name])

            if fields is None:
                for rule in self._rules:
                    if rule.field:
                        if rule.field in new_record:
                            new_record[rule.field] = self._apply_rule(new_record[rule.field], rule)
                    else:
                        for key in new_record:
                            new_record[key] = self._apply_rule(new_record[key], rule)

            masked.append(new_record)

        return masked

    def mask_record(
        self,
        record: Dict[str, Any],
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Mask a single record."""
        return self.mask([record], fields)[0]

    def _apply_rule(self, value: Any, rule: MaskingRule) -> Any:
        """Apply a masking rule to a value."""
        if value is None:
            return value

        s = str(value)
        return re.sub(rule.pattern, rule.replacement, s)

"""
Data Masking Action - Masks sensitive data fields.

This module provides data masking capabilities for
protecting sensitive information in datasets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MaskingRule:
    """A rule for masking data."""
    field_name: str
    mask_type: str = "partial"
    replacement: str = "*"


@dataclass
class MaskingConfig:
    """Configuration for data masking."""
    rules: list[MaskingRule] = field(default_factory=list)


class DataMasker:
    """Masks sensitive data fields."""
    
    def __init__(self, config: MaskingConfig | None = None) -> None:
        self.config = config or MaskingConfig()
    
    def mask_email(self, email: str, visible_chars: int = 2) -> str:
        """Mask email address."""
        if not email or "@" not in email:
            return email
        local, domain = email.split("@", 1)
        if len(local) <= visible_chars:
            return email
        masked_local = local[:visible_chars] + "*" * (len(local) - visible_chars)
        return f"{masked_local}@{domain}"
    
    def mask_phone(self, phone: str) -> str:
        """Mask phone number."""
        digits = re.sub(r"\D", "", phone)
        if len(digits) <= 4:
            return phone
        return "*" * (len(digits) - 4) + digits[-4:]
    
    def mask_card(self, card: str) -> str:
        """Mask credit card number."""
        digits = re.sub(r"\D", "", card)
        if len(digits) <= 4:
            return card
        return "*" * (len(digits) - 4) + digits[-4:]
    
    def mask_partial(self, value: str, visible_start: int = 0, visible_end: int = 4) -> str:
        """Partially mask a string."""
        if not value:
            return value
        if len(value) <= visible_start + visible_end:
            return "*" * len(value)
        return value[:visible_start] + "*" * (len(value) - visible_start - visible_end) + value[-visible_end:]


class DataMaskingAction:
    """Data masking action for automation workflows."""
    
    def __init__(self) -> None:
        self.config = MaskingConfig()
        self.masker = DataMasker(self.config)
    
    def add_rule(self, field_name: str, mask_type: str = "partial") -> None:
        """Add a masking rule."""
        self.config.rules.append(MaskingRule(field_name=field_name, mask_type=mask_type))
    
    async def mask_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Mask sensitive fields in a record."""
        result = record.copy()
        for rule in self.config.rules:
            if rule.field_name in result:
                value = result[rule.field_name]
                if rule.mask_type == "email":
                    result[rule.field_name] = self.masker.mask_email(str(value))
                elif rule.mask_type == "phone":
                    result[rule.field_name] = self.masker.mask_phone(str(value))
                elif rule.mask_type == "card":
                    result[rule.field_name] = self.masker.mask_card(str(value))
                else:
                    result[rule.field_name] = self.masker.mask_partial(str(value))
        return result
    
    async def mask_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Mask sensitive fields in batch."""
        return [await self.mask_record(r) for r in records]


__all__ = ["MaskingRule", "MaskingConfig", "DataMasker", "DataMaskingAction"]

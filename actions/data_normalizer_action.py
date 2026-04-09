"""Data Normalizer Action Module.

Normalize and standardize data from various sources.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable
import isodate


class NormalizationType(Enum):
    """Type of normalization."""
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    IP_ADDRESS = "ip_address"


@dataclass
class NormalizationResult:
    """Result of normalization."""
    original: Any
    normalized: Any
    success: bool
    error: str | None = None


class DataNormalizer:
    """Data normalizer for common data types."""

    def __init__(self) -> None:
        self._normalizers: dict[NormalizationType, Callable[[Any], NormalizationResult]] = {
            NormalizationType.STRING: self._normalize_string,
            NormalizationType.NUMBER: self._normalize_number,
            NormalizationType.BOOLEAN: self._normalize_boolean,
            NormalizationType.DATE: self._normalize_date,
            NormalizationType.EMAIL: self._normalize_email,
            NormalizationType.PHONE: self._normalize_phone,
            NormalizationType.URL: self._normalize_url,
            NormalizationType.IP_ADDRESS: self._normalize_ip,
        }

    def normalize(self, value: Any, norm_type: NormalizationType) -> NormalizationResult:
        """Normalize a value."""
        normalizer = self._normalizers.get(norm_type)
        if not normalizer:
            return NormalizationResult(value, value, False, f"No normalizer for {norm_type}")
        try:
            return normalizer(value)
        except Exception as e:
            return NormalizationResult(value, value, False, str(e))

    def _normalize_string(self, value: Any) -> NormalizationResult:
        """Normalize string value."""
        if value is None:
            return NormalizationResult(value, "", True)
        return NormalizationResult(value, str(value).strip(), True)

    def _normalize_number(self, value: Any) -> NormalizationResult:
        """Normalize numeric value."""
        if value is None:
            return NormalizationResult(value, None, True)
        try:
            if isinstance(value, (int, float)):
                return NormalizationResult(value, value, True)
            val = float(str(value).replace(",", ""))
            if "." in str(value):
                return NormalizationResult(value, val, True)
            return NormalizationResult(value, int(val), True)
        except (ValueError, TypeError):
            return NormalizationResult(value, None, False, "Invalid number")

    def _normalize_boolean(self, value: Any) -> NormalizationResult:
        """Normalize boolean value."""
        if value is None:
            return NormalizationResult(value, False, True)
        if isinstance(value, bool):
            return NormalizationResult(value, value, True)
        truthy = {"true", "yes", "1", "on", "t", "y"}
        falsy = {"false", "no", "0", "off", "f", "n"}
        lower = str(value).lower().strip()
        if lower in truthy:
            return NormalizationResult(value, True, True)
        if lower in falsy:
            return NormalizationResult(value, False, True)
        return NormalizationResult(value, bool(value), True)

    def _normalize_date(self, value: Any) -> NormalizationResult:
        """Normalize date value to ISO format."""
        if value is None:
            return NormalizationResult(value, None, True)
        if isinstance(value, datetime):
            return NormalizationResult(value, value.isoformat(), True)
        try:
            parsed = isodate.parse_datetime(str(value))
            return NormalizationResult(value, parsed.isoformat(), True)
        except Exception:
            try:
                parsed = isodate.parse_date(str(value))
                dt = datetime.combine(parsed, datetime.min.time(), tzinfo=timezone.utc)
                return NormalizationResult(value, dt.isoformat(), True)
            except Exception as e:
                return NormalizationResult(value, None, False, f"Invalid date: {e}")

    def _normalize_email(self, value: Any) -> NormalizationResult:
        """Normalize email address."""
        if value is None:
            return NormalizationResult(value, None, True)
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        email = str(value).lower().strip()
        if re.match(pattern, email):
            return NormalizationResult(value, email, True)
        return NormalizationResult(value, None, False, "Invalid email format")

    def _normalize_phone(self, value: Any) -> NormalizationResult:
        """Normalize phone number."""
        if value is None:
            return NormalizationResult(value, None, True)
        digits = re.sub(r"\D", "", str(value))
        if len(digits) >= 10:
            return NormalizationResult(value, digits[-10:], True)
        return NormalizationResult(value, digits, False, "Phone number too short")

    def _normalize_url(self, value: Any) -> NormalizationResult:
        """Normalize URL."""
        if value is None:
            return NormalizationResult(value, None, True)
        url = str(value).strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        pattern = r"^https?://[^\s/$.?#].[^\s]*$"
        if re.match(pattern, url, re.IGNORECASE):
            return NormalizationResult(value, url, True)
        return NormalizationResult(value, url, False, "Invalid URL format")

    def _normalize_ip(self, value: Any) -> NormalizationResult:
        """Normalize IP address."""
        if value is None:
            return NormalizationResult(value, None, True)
        import ipaddress
        try:
            ip = ipaddress.ip_address(str(value))
            return NormalizationResult(value, str(ip), True)
        except ValueError as e:
            return NormalizationResult(value, None, False, str(e))


class BatchNormalizer:
    """Batch normalizer for multiple records."""

    def __init__(self, normalizer: DataNormalizer) -> None:
        self.normalizer = normalizer

    def normalize_records(
        self,
        records: list[dict],
        field_mapping: dict[str, NormalizationType]
    ) -> tuple[list[dict], list[dict]]:
        """Normalize multiple records. Returns (successful, failed)."""
        successful = []
        failed = []
        for record in records:
            normalized = dict(record)
            has_error = False
            for field, norm_type in field_mapping.items():
                if field in normalized:
                    result = self.normalizer.normalize(normalized[field], norm_type)
                    normalized[field] = result.normalized
                    if not result.success:
                        has_error = True
            if has_error:
                failed.append(normalized)
            else:
                successful.append(normalized)
        return successful, failed

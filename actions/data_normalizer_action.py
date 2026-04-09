"""Data normalization action module.

Provides data normalization and standardization:
- DataNormalizer: Normalize data values
- SchemaNormalizer: Normalize data to a schema
- TextNormalizer: Text cleaning and normalization
- NumberNormalizer: Numeric value normalization
- DateNormalizer: Date/time normalization
"""

from __future__ import annotations

import re
import math
from typing import Any, Optional, Dict, List, Union, Callable
from dataclasses import dataclass
from enum import Enum
import logging
import datetime

logger = logging.getLogger(__name__)


class NormalizationMode(Enum):
    """Data normalization mode."""
    STRICT = "strict"
    LENIENT = "lenient"
    NONE = "none"


@dataclass
class NormalizationResult:
    """Result of a normalization operation."""
    success: bool
    value: Any
    original_value: Any
    error: Optional[str] = None


class TextNormalizer:
    """Normalize and clean text data."""

    DEFAULT_STRIP_CHARS = " \t\n\r"

    def __init__(
        self,
        lowercase: bool = False,
        strip: bool = True,
        strip_chars: str = DEFAULT_STRIP_CHARS,
        remove_extra_spaces: bool = True,
        normalize_unicode: bool = True,
    ):
        self.lowercase = lowercase
        self.strip = strip
        self.strip_chars = strip_chars
        self.remove_extra_spaces = remove_extra_spaces
        self.normalize_unicode = normalize_unicode

    def normalize(self, value: Any) -> str:
        """Normalize a text value."""
        if value is None:
            return ""

        text = str(value)

        if self.normalize_unicode:
            import unicodedata
            text = unicodedata.normalize("NFKC", text)

        if self.lowercase:
            text = text.lower()

        if self.strip:
            text = text.strip(self.strip_chars)

        if self.remove_extra_spaces:
            text = re.sub(r" +", " ", text)

        return text

    def normalize_list(
        self,
        values: List[Any],
        unique: bool = False,
    ) -> List[str]:
        """Normalize a list of text values."""
        results = [self.normalize(v) for v in values]
        if unique:
            seen = set()
            unique_results = []
            for r in results:
                if r not in seen:
                    seen.add(r)
                    unique_results.append(r)
            return unique_results
        return results


class NumberNormalizer:
    """Normalize numeric values."""

    def __init__(
        self,
        precision: Optional[int] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        clamp: bool = True,
    ):
        self.precision = precision
        self.min_value = min_value
        self.max_value = max_value
        self.clamp = clamp

    def normalize(self, value: Any) -> Optional[float]:
        """Normalize a numeric value."""
        if value is None:
            return None

        try:
            num = float(value)
        except (ValueError, TypeError):
            return None

        if self.clamp:
            if self.min_value is not None:
                num = max(num, self.min_value)
            if self.max_value is not None:
                num = min(num, self.max_value)

        if self.precision is not None:
            num = round(num, self.precision)

        return num

    def normalize_list(self, values: List[Any]) -> List[Optional[float]]:
        """Normalize a list of numeric values."""
        return [self.normalize(v) for v in values]


class DateNormalizer:
    """Normalize date/time values."""

    COMMON_FORMATS = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ]

    def __init__(
        self,
        output_format: str = "%Y-%m-%d %H:%M:%S",
        timezone: Optional[str] = None,
        strict: bool = False,
    ):
        self.output_format = output_format
        self.timezone = timezone
        self.strict = strict

    def normalize(self, value: Any) -> Optional[str]:
        """Normalize a date/time value."""
        if value is None:
            return None

        if isinstance(value, datetime.datetime):
            dt = value
        elif isinstance(value, datetime.date):
            dt = datetime.datetime.combine(value, datetime.time())
        elif isinstance(value, (int, float)):
            try:
                dt = datetime.datetime.fromtimestamp(value)
            except (ValueError, OSError):
                return None
        else:
            dt = self._parse_string(str(value))

        if dt is None:
            if self.strict:
                return None
            return str(value)

        return dt.strftime(self.output_format)

    def _parse_string(self, value: str) -> Optional[datetime.datetime]:
        """Parse a date string using common formats."""
        for fmt in self.COMMON_FORMATS:
            try:
                return datetime.datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None


class DataNormalizer:
    """General-purpose data normalizer."""

    def __init__(
        self,
        text_normalizer: Optional[TextNormalizer] = None,
        number_normalizer: Optional[NumberNormalizer] = None,
        date_normalizer: Optional[DateNormalizer] = None,
        mode: NormalizationMode = NormalizationMode.LENIENT,
    ):
        self.text_normalizer = text_normalizer or TextNormalizer()
        self.number_normalizer = number_normalizer or NumberNormalizer()
        self.date_normalizer = date_normalizer or DateNormalizer()
        self.mode = mode

    def normalize_field(
        self,
        value: Any,
        field_type: str,
        field_config: Optional[Dict[str, Any]] = None,
    ) -> NormalizationResult:
        """Normalize a single field value."""
        original = value

        try:
            if field_type == "text":
                normalized = self.text_normalizer.normalize(value)
            elif field_type == "number":
                normalized = self.number_normalizer.normalize(value)
            elif field_type in ("date", "datetime"):
                normalized = self.date_normalizer.normalize(value)
            elif field_type == "boolean":
                normalized = self._normalize_boolean(value)
            else:
                normalized = value

            return NormalizationResult(
                success=True,
                value=normalized,
                original_value=original,
            )
        except Exception as e:
            if self.mode == NormalizationMode.STRICT:
                return NormalizationResult(
                    success=False,
                    value=original,
                    original_value=original,
                    error=str(e),
                )
            return NormalizationResult(
                success=True,
                value=original,
                original_value=original,
            )

    def _normalize_boolean(self, value: Any) -> bool:
        """Normalize a boolean value."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    def normalize_record(
        self,
        record: Dict[str, Any],
        schema: Dict[str, str],
    ) -> Dict[str, Any]:
        """Normalize all fields in a record according to a schema."""
        normalized = {}
        for field_name, field_type in schema.items():
            value = record.get(field_name)
            result = self.normalize_field(value, field_type)
            normalized[field_name] = result.value
        return normalized

    def normalize_batch(
        self,
        records: List[Dict[str, Any]],
        schema: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Normalize a batch of records."""
        return [self.normalize_record(r, schema) for r in records]


def normalize_text(value: Any, **kwargs) -> str:
    """Convenience function for text normalization."""
    normalizer = TextNormalizer(**kwargs)
    return normalizer.normalize(value)


def normalize_number(value: Any, **kwargs) -> Optional[float]:
    """Convenience function for number normalization."""
    normalizer = NumberNormalizer(**kwargs)
    return normalizer.normalize(value)

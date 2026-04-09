"""
Data Normalizer Action Module.

Provides data normalization and standardization for various
data types including strings, numbers, dates, and complex structures.

Author: rabai_autoclick team
"""

import re
import logging
from typing import Optional, Dict, Any, List, Union, Callable
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass
class NormalizationConfig:
    """Configuration for normalization operations."""
    lowercase: bool = False
    trim_whitespace: bool = True
    remove_accents: bool = False
    strip_special_chars: Optional[str] = None
    max_length: Optional[int] = None


class DataNormalizerAction:
    """
    Data Normalization Utilities.

    Provides comprehensive normalization for strings, numbers,
    dates, and complex data structures.

    Example:
        >>> normalizer = DataNormalizerAction()
        >>> normalizer.normalize_phone("+1 (555) 123-4567")
        '5551234567'
    """

    # Common phone patterns
    PHONE_PATTERN = re.compile(r"[^\d+]")

    # Email pattern
    EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

    # URL pattern
    URL_PATTERN = re.compile(
        r"https?://"
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"
        r"localhost|"
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
        r"(?::\d+)?"
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )

    def __init__(self, config: Optional[NormalizationConfig] = None):
        self.config = config or NormalizationConfig()

    # String Normalization

    def normalize_string(
        self,
        value: str,
        config: Optional[NormalizationConfig] = None,
    ) -> str:
        """
        Normalize a string value.

        Args:
            value: Input string
            config: Optional normalization config

        Returns:
            Normalized string
        """
        cfg = config or self.config
        result = str(value)

        if cfg.trim_whitespace:
            result = result.strip()

        if cfg.lowercase:
            result = result.lower()

        if cfg.remove_accents:
            result = self._remove_accents(result)

        if cfg.strip_special_chars:
            pattern = f"[{re.escape(cfg.strip_special_chars)}]"
            result = re.sub(pattern, "", result)

        if cfg.max_length and len(result) > cfg.max_length:
            result = result[: cfg.max_length]

        return result

    def _remove_accents(self, text: str) -> str:
        """Remove accents from text."""
        import unicodedata
        nfd = unicodedata.normalize("NFD", text)
        return "".join(c for c in nfd if unicodedata.category(c) != "Mn")

    def normalize_whitespace(self, value: str) -> str:
        """
        Normalize whitespace in string.

        Args:
            value: Input string

        Returns:
            String with normalized whitespace
        """
        return re.sub(r"\s+", " ", value).strip()

    def normalize_case(
        self,
        value: str,
        case_type: str = "lower",
    ) -> str:
        """
        Normalize string case.

        Args:
            value: Input string
            case_type: Type of case (lower, upper, title, sentence)

        Returns:
            Case-normalized string
        """
        case_type = case_type.lower()

        if case_type == "lower":
            return value.lower()
        elif case_type == "upper":
            return value.upper()
        elif case_type == "title":
            return value.title()
        elif case_type == "sentence":
            return value.capitalize()
        else:
            raise ValueError(f"Unknown case type: {case_type}")

    def normalize_snake_case(self, value: str) -> str:
        """
        Convert string to snake_case.

        Args:
            value: Input string

        Returns:
            snake_case string
        """
        value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
        value = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", value)
        value = value.replace("-", "_").replace(" ", "_")
        return value.lower()

    def normalize_camel_case(self, value: str) -> str:
        """
        Convert string to camelCase.

        Args:
            value: Input string

        Returns:
            camelCase string
        """
        parts = re.sub(r"[_\-\s]+", " ", value).split()
        if not parts:
            return ""
        return parts[0].lower() + "".join(p.title() for p in parts[1:])

    def normalize_pascal_case(self, value: str) -> str:
        """
        Convert string to PascalCase.

        Args:
            value: Input string

        Returns:
            PascalCase string
        """
        parts = re.sub(r"[_\-\s]+", " ", value).split()
        return "".join(p.title() for p in parts)

    # Number Normalization

    def normalize_number(
        self,
        value: Union[str, int, float],
        precision: Optional[int] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> Union[int, float]:
        """
        Normalize a numeric value.

        Args:
            value: Input number
            precision: Decimal precision
            min_value: Minimum allowed value
            max_value: Maximum allowed value

        Returns:
            Normalized number
        """
        if isinstance(value, str):
            value = self._parse_number(value)

        if value is None:
            return 0

        if precision is not None:
            value = round(value, precision)

        if min_value is not None:
            value = max(value, min_value)

        if max_value is not None:
            value = min(value, max_value)

        return value

    def _parse_number(self, value: str) -> float:
        """Parse number from string."""
        cleaned = re.sub(r"[^\d.\-]", "", value)
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def normalize_percentage(
        self,
        value: Union[str, float, int],
    ) -> float:
        """
        Normalize a percentage value to 0-1 range.

        Args:
            value: Percentage value (0-100 or "50%")

        Returns:
            Value between 0 and 1
        """
        if isinstance(value, str):
            value = value.replace("%", "").strip()
        value = float(value)
        if value > 1:
            value = value / 100
        return max(0.0, min(1.0, value))

    # Contact Information Normalization

    def normalize_phone(
        self,
        value: str,
        country_code: str = "US",
    ) -> str:
        """
        Normalize a phone number.

        Args:
            value: Phone number string
            country_code: Country code for formatting

        Returns:
            Normalized phone number
        """
        digits = re.sub(r"[^\d]", "", value)

        if digits.startswith("1") and len(digits) == 11:
            digits = digits[1:]

        if country_code == "US" and len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

        return digits

    def normalize_email(self, value: str) -> str:
        """
        Normalize an email address.

        Args:
            value: Email address

        Returns:
            Normalized email address
        """
        return value.lower().strip()

    def is_valid_email(self, value: str) -> bool:
        """
        Check if email is valid.

        Args:
            value: Email address

        Returns:
            True if valid
        """
        return bool(self.EMAIL_PATTERN.match(value))

    # URL Normalization

    def normalize_url(
        self,
        value: str,
        strip_query: bool = False,
        strip_fragment: bool = False,
    ) -> str:
        """
        Normalize a URL.

        Args:
            value: URL string
            strip_query: Remove query parameters
            strip_fragment: Remove fragment

        Returns:
            Normalized URL
        """
        parsed = urlparse(value)

        scheme = parsed.scheme.lower() or "https"
        netloc = parsed.netloc.lower()

        if strip_query:
            query = ""
        else:
            query = parsed.query

        if strip_fragment:
            fragment = ""
        else:
            fragment = parsed.fragment

        path = parsed.path or "/"

        return f"{scheme}://{netloc}{path}?{query}#{fragment}".rstrip("?")

    def is_valid_url(self, value: str) -> bool:
        """
        Check if URL is valid.

        Args:
            value: URL string

        Returns:
            True if valid
        """
        return bool(self.URL_PATTERN.match(value))

    # Date Normalization

    def normalize_date(
        self,
        value: Union[str, datetime],
        output_format: str = "%Y-%m-%d",
    ) -> str:
        """
        Normalize a date to specified format.

        Args:
            value: Date value
            output_format: Output format string

        Returns:
            Formatted date string
        """
        if isinstance(value, str):
            parsed = self._parse_date(value)
        else:
            parsed = value

        if parsed is None:
            return ""

        return parsed.strftime(output_format)

    def _parse_date(self, value: str) -> Optional[datetime]:
        """Parse date string to datetime."""
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%m-%d-%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue

        return None

    # Record Normalization

    def normalize_record(
        self,
        record: Dict[str, Any],
        field_configs: Dict[str, NormalizationConfig],
    ) -> Dict[str, Any]:
        """
        Normalize all fields in a record.

        Args:
            record: Data record
            field_configs: Dict mapping field names to NormalizationConfig

        Returns:
            Normalized record
        """
        result = {}

        for field, config in field_configs.items():
            if field in record:
                value = record[field]
                if isinstance(value, str):
                    result[field] = self.normalize_string(value, config)
                elif isinstance(value, (int, float)):
                    result[field] = self.normalize_number(value)
                else:
                    result[field] = value

        return result

    def normalize_batch(
        self,
        records: List[Dict[str, Any]],
        field_configs: Dict[str, NormalizationConfig],
    ) -> List[Dict[str, Any]]:
        """
        Normalize a batch of records.

        Args:
            records: List of data records
            field_configs: Dict mapping field names to NormalizationConfig

        Returns:
            List of normalized records
        """
        return [self.normalize_record(record, field_configs) for record in records]

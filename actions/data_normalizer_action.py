"""
Data Normalizer Action Module.

Normalizes and standardizes data from various sources with
type coercion, format conversion, and structural transformations.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional, TypeVar
from urllib.parse import urlparse

T = TypeVar("T")


@dataclass
class NormalizationRule:
    """A single normalization rule."""

    name: str
    apply: Callable[[Any], Any]
    priority: int = 0


@dataclass
class NormalizationResult:
    """Result of normalizing a value."""

    success: bool
    value: Any
    original_value: Any
    rules_applied: list[str] = field(default_factory=list)
    error: Optional[str] = None


class DataNormalizer:
    """
    Multi-purpose data normalization engine.

    Supports string normalization, type coercion, URL cleaning,
    date parsing, and custom transformation rules.
    """

    # Common date formats to try
    DATE_FORMATS = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%b %d, %Y",
        "%B %d, %Y",
        "%Y%m%d",
        "%Y%m%d%H%M%S",
    ]

    def __init__(self) -> None:
        """Initialize the normalizer with built-in rules."""
        self._rules: list[NormalizationRule] = []
        self._string_normalizers: list[Callable[[str], str]] = []
        self._setup_default_rules()

    def _setup_default_rules(self) -> None:
        """Register default normalization rules."""
        self.add_string_normalizer(self._lowercase)
        self.add_string_normalizer(self._strip_whitespace)
        self.add_string_normalizer(self._collapse_whitespace)

    # String normalizers
    def _lowercase(self, s: str) -> str:
        """Convert string to lowercase."""
        return s.lower()

    def _strip_whitespace(self, s: str) -> str:
        """Strip leading and trailing whitespace."""
        return s.strip()

    def _collapse_whitespace(self, s: str) -> str:
        """Replace multiple whitespace with single space."""
        return re.sub(r"\s+", " ", s)

    def _remove_special_chars(self, s: str, keep: str = "") -> str:
        """Remove special characters, optionally keeping specified ones."""
        pattern = f"[^a-zA-Z0-9{re.escape(keep)}]"
        return re.sub(pattern, "", s)

    def _normalize_url(self, url: str) -> str:
        """Normalize a URL."""
        url = url.strip().lower()
        parsed = urlparse(url)
        if not parsed.scheme:
            url = f"https://{url}"
            parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"
        return f"{parsed.scheme}://{netloc}{path}"

    def _normalize_email(self, email: str) -> str:
        """Normalize an email address."""
        email = email.strip().lower()
        if "@" in email:
            local, domain = email.rsplit("@", 1)
            local = local.lower()
            domain = domain.lower()
            return f"{local}@{domain}"
        return email

    def _normalize_phone(self, phone: str) -> str:
        """Normalize a phone number to digits only."""
        return re.sub(r"[^\d+]", "", phone)

    def _normalize_date(self, date_str: str) -> Optional[str]:
        """Parse and normalize a date string to ISO format."""
        date_str = date_str.strip()
        for fmt in self.DATE_FORMATS:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _coerce_number(self, value: Any) -> float:
        """Coerce a value to a number."""
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = re.sub(r"[^\d.\-]", "", value)
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        return 0.0

    def _coerce_boolean(self, value: Any) -> bool:
        """Coerce a value to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on", "enabled")
        return bool(value)

    def _coerce_string(self, value: Any) -> str:
        """Coerce a value to string."""
        if isinstance(value, (int, float)):
            return str(value)
        if value is None:
            return ""
        return str(value)

    def _normalize_json_keys(self, obj: Any) -> Any:
        """Normalize all dictionary keys to snake_case."""
        if isinstance(obj, dict):
            return {
                re.sub(r"(?<!^)(?=[A-Z])", "_", str(k)).lower(): self._normalize_json_keys(v)
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [self._normalize_json_keys(item) for item in obj]
        return obj

    # Public API
    def add_string_normalizer(
        self,
        normalizer: Callable[[str], str],
        priority: int = 0,
    ) -> None:
        """Add a string normalization function."""
        rule = NormalizationRule(
            name=normalizer.__name__,
            apply=normalizer,
            priority=priority,
        )
        self._rules.append(rule)
        self._rules.sort(key=lambda r: r.priority)

    def normalize_string(
        self,
        value: str,
        rules: Optional[list[str]] = None,
    ) -> NormalizationResult:
        """
        Normalize a string value.

        Args:
            value: String to normalize.
            rules: Optional list of specific rule names to apply.

        Returns:
            NormalizationResult with transformed value.
        """
        original = value
        applied = []

        for rule in self._rules:
            if rules and rule.name not in rules:
                continue
            try:
                value = rule.apply(value)
                applied.append(rule.name)
            except Exception as e:
                return NormalizationResult(
                    success=False,
                    value=original,
                    original_value=original,
                    rules_applied=applied,
                    error=str(e),
                )

        return NormalizationResult(
            success=True,
            value=value,
            original_value=original,
            rules_applied=applied,
        )

    def normalize_url(self, url: str) -> NormalizationResult:
        """Normalize a URL."""
        try:
            normalized = self._normalize_url(url)
            return NormalizationResult(
                success=True,
                value=normalized,
                original_value=url,
                rules_applied=["normalize_url"],
            )
        except Exception as e:
            return NormalizationResult(
                success=False,
                value=url,
                original_value=url,
                error=str(e),
            )

    def normalize_email(self, email: str) -> NormalizationResult:
        """Normalize an email address."""
        try:
            normalized = self._normalize_email(email)
            return NormalizationResult(
                success=True,
                value=normalized,
                original_value=email,
                rules_applied=["normalize_email"],
            )
        except Exception as e:
            return NormalizationResult(
                success=False,
                value=email,
                original_value=email,
                error=str(e),
            )

    def normalize_phone(self, phone: str) -> NormalizationResult:
        """Normalize a phone number."""
        try:
            normalized = self._normalize_phone(phone)
            return NormalizationResult(
                success=True,
                value=normalized,
                original_value=phone,
                rules_applied=["normalize_phone"],
            )
        except Exception as e:
            return NormalizationResult(
                success=False,
                value=phone,
                original_value=phone,
                error=str(e),
            )

    def normalize_date(self, date_str: str) -> NormalizationResult:
        """Parse and normalize a date string."""
        try:
            normalized = self._normalize_date(date_str)
            if normalized is None:
                return NormalizationResult(
                    success=False,
                    value=date_str,
                    original_value=date_str,
                    error="Unable to parse date",
                )
            return NormalizationResult(
                success=True,
                value=normalized,
                original_value=date_str,
                rules_applied=["normalize_date"],
            )
        except Exception as e:
            return NormalizationResult(
                success=False,
                value=date_str,
                original_value=date_str,
                error=str(e),
            )

    def coerce_type(
        self,
        value: Any,
        target_type: str,
    ) -> NormalizationResult:
        """
        Coerce a value to a target type.

        Args:
            value: Value to coerce.
            target_type: Target type name ("str", "int", "float", "bool").

        Returns:
            NormalizationResult with coerced value.
        """
        original = value
        type_map = {
            "str": self._coerce_string,
            "string": self._coerce_string,
            "int": lambda v: int(self._coerce_number(v)),
            "integer": lambda v: int(self._coerce_number(v)),
            "float": self._coerce_number,
            "number": self._coerce_number,
            "bool": self._coerce_boolean,
            "boolean": self._coerce_boolean,
        }

        if target_type not in type_map:
            return NormalizationResult(
                success=False,
                value=original,
                original_value=original,
                error=f"Unknown target type: {target_type}",
            )

        try:
            coerced = type_map[target_type](value)
            return NormalizationResult(
                success=True,
                value=coerced,
                original_value=original,
                rules_applied=[f"coerce_{target_type}"],
            )
        except Exception as e:
            return NormalizationResult(
                success=False,
                value=original,
                original_value=original,
                rules_applied=[f"coerce_{target_type}"],
                error=str(e),
            )

    def normalize_structure(
        self,
        data: dict[str, Any],
        key_case: str = "snake",
    ) -> dict[str, Any]:
        """
        Normalize dictionary keys to a consistent case.

        Args:
            data: Dictionary to normalize.
            key_case: Target case ("snake", "camel", "lower").

        Returns:
            Dictionary with normalized keys.
        """
        if key_case == "snake":
            return self._normalize_json_keys(data)
        if key_case == "camel":
            result: dict[str, Any] = {}
            for k, v in data.items():
                snake = re.sub(r"(?<!^)(?=[A-Z])", "_", str(k)).lower()
                parts = snake.split("_")
                if len(parts) > 1:
                    camel = parts[0] + "".join(p.capitalize() for p in parts[1:])
                else:
                    camel = snake
                result[camel] = v
            return result
        if key_case == "lower":
            return {k.lower(): v for k, v in data.items()}
        return data

    def normalize_batch(
        self,
        items: list[Any],
        normalizer: Callable[[Any], NormalizationResult],
    ) -> list[NormalizationResult]:
        """
        Apply a normalizer function to a batch of items.

        Args:
            items: List of items to normalize.
            normalizer: Normalizer function to apply.

        Returns:
            List of NormalizationResult in same order.
        """
        return [normalizer(item) for item in items]


def create_normalizer() -> DataNormalizer:
    """
    Factory function to create a data normalizer.

    Returns:
        Configured DataNormalizer instance.
    """
    return DataNormalizer()

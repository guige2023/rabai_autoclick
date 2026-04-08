"""
Data Normalizer Action Module.

Normalizes data from various sources with configurable transformation
 rules, standardization, and format conversion.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class NormalizationType(Enum):
    """Type of normalization to apply."""
    STRING = "string"
    NUMBER = "number"
    DATE = "date"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    BOOLEAN = "boolean"
    ENUM = "enum"
    CUSTOM = "custom"


@dataclass
class NormalizationRule:
    """A single normalization rule."""
    field: str
    norm_type: NormalizationType
    rule_config: Any = None
    default_value: Any = None
    required: bool = False


@dataclass
class NormalizationResult:
    """Result of a normalization operation."""
    success: bool
    normalized_data: dict[str, Any]
    errors: list[dict[str, Any]] = field(default_factory=list)
    fields_normalized: int = 0


class DataNormalizerAction:
    """
    Data normalization engine with multiple built-in types.

    Normalizes field values to standard formats and types with
    support for custom transformation functions.

    Example:
        normalizer = DataNormalizerAction()
        normalizer.add_rule("email", NormalizationRule(
            field="email",
            norm_type=NormalizationType.EMAIL,
            required=True,
        ))
        normalizer.add_rule("phone", NormalizationRule(
            field="phone",
            norm_type=NormalizationType.PHONE,
            rule_config={"country": "US"},
        ))
        result = normalizer.normalize({"email": "  Test@Example.com  ", "phone": "555-123-4567"})
    """

    def __init__(
        self,
        strict_mode: bool = False,
        trim_strings: bool = True,
    ) -> None:
        self.strict_mode = strict_mode
        self.trim_strings = trim_strings
        self._rules: list[NormalizationRule] = []
        self._custom_normalizers: dict[str, Callable[[Any], Any]] = {}

    def add_rule(
        self,
        field: str,
        norm_type: NormalizationType,
        rule_config: Any = None,
        default_value: Any = None,
        required: bool = False,
    ) -> "DataNormalizerAction":
        """Add a normalization rule for a field."""
        rule = NormalizationRule(
            field=field,
            norm_type=norm_type,
            rule_config=rule_config,
            default_value=default_value,
            required=required,
        )
        self._rules.append(rule)
        return self

    def add_custom_normalizer(
        self,
        name: str,
        func: Callable[[Any], Any],
    ) -> "DataNormalizerAction":
        """Add a custom normalization function."""
        self._custom_normalizers[name] = func
        return self

    def normalize(
        self,
        data: dict[str, Any],
    ) -> NormalizationResult:
        """Normalize data according to configured rules."""
        errors: list[dict[str, Any]] = []
        normalized = dict(data)
        fields_normalized = 0

        for rule in self._rules:
            value = data.get(rule.field)

            if value is None:
                if rule.required:
                    errors.append({
                        "field": rule.field,
                        "error": "Required field is missing",
                        "value": value,
                    })
                    if self.strict_mode:
                        continue

                if rule.default_value is not None:
                    normalized[rule.field] = rule.default_value
                    fields_normalized += 1
                continue

            if self.trim_strings and isinstance(value, str):
                value = value.strip()
                normalized[rule.field] = value

            try:
                result = self._apply_normalization(value, rule)
                normalized[rule.field] = result
                fields_normalized += 1

            except Exception as e:
                error_entry = {
                    "field": rule.field,
                    "error": str(e),
                    "value": value,
                }
                errors.append(error_entry)

                if rule.required or self.strict_mode:
                    continue

                if rule.default_value is not None:
                    normalized[rule.field] = rule.default_value

        return NormalizationResult(
            success=len(errors) == 0,
            normalized_data=normalized,
            errors=errors,
            fields_normalized=fields_normalized,
        )

    def _apply_normalization(
        self,
        value: Any,
        rule: NormalizationRule,
    ) -> Any:
        """Apply normalization based on rule type."""
        if rule.norm_type == NormalizationType.STRING:
            return self._normalize_string(value)

        elif rule.norm_type == NormalizationType.NUMBER:
            return self._normalize_number(value, rule.rule_config)

        elif rule.norm_type == NormalizationType.DATE:
            return self._normalize_date(value, rule.rule_config)

        elif rule.norm_type == NormalizationType.EMAIL:
            return self._normalize_email(value)

        elif rule.norm_type == NormalizationType.PHONE:
            return self._normalize_phone(value, rule.rule_config)

        elif rule.norm_type == NormalizationType.URL:
            return self._normalize_url(value)

        elif rule.norm_type == NormalizationType.BOOLEAN:
            return self._normalize_boolean(value)

        elif rule.norm_type == NormalizationType.ENUM:
            return self._normalize_enum(value, rule.rule_config)

        elif rule.norm_type == NormalizationType.CUSTOM:
            custom_func = self._custom_normalizers.get(rule.field)
            if custom_func:
                return custom_func(value)
            return value

        return value

    def _normalize_string(self, value: Any) -> str:
        """Normalize a string value."""
        s = str(value).strip()
        s = re.sub(r'\s+', ' ', s)
        return s

    def _normalize_number(
        self,
        value: Any,
        config: Optional[dict[str, Any]],
    ) -> Union[int, float]:
        """Normalize a numeric value."""
        if isinstance(value, (int, float)):
            return value

        cleaned = re.sub(r'[^\d.\-]', '', str(value))

        if '.' in cleaned:
            result = float(cleaned)
        else:
            result = int(cleaned)

        if config:
            if 'min' in config and result < config['min']:
                result = config['min']
            if 'max' in config and result > config['max']:
                result = config['max']

        return result

    def _normalize_date(
        self,
        value: Any,
        format: Optional[str] = None,
    ) -> str:
        """Normalize a date value to ISO format."""
        from datetime import datetime

        if isinstance(value, datetime):
            return value.isoformat()

        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value)
            return dt.isoformat()

        formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']
        if format:
            formats.insert(0, format)

        for fmt in formats:
            try:
                dt = datetime.strptime(str(value), fmt)
                return dt.isoformat()
            except ValueError:
                continue

        return str(value)

    def _normalize_email(self, value: Any) -> str:
        """Normalize an email address."""
        email = str(value).lower().strip()
        pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        if not re.match(pattern, email):
            raise ValueError(f"Invalid email format: {email}")
        return email

    def _normalize_phone(
        self,
        value: Any,
        config: Optional[dict[str, Any]] = None,
    ) -> str:
        """Normalize a phone number."""
        digits = re.sub(r'\D', '', str(value))

        country = config.get('country', 'US') if config else 'US'

        if country == 'US' and len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif country == 'US' and len(digits) == 11 and digits[0] == '1':
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"

        return digits

    def _normalize_url(self, value: Any) -> str:
        """Normalize a URL."""
        url = str(value).strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url

    def _normalize_boolean(self, value: Any) -> bool:
        """Normalize a boolean value."""
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            true_values = {'true', 'yes', '1', 'on', 'enabled'}
            false_values = {'false', 'no', '0', 'off', 'disabled'}

            lowered = value.lower().strip()
            if lowered in true_values:
                return True
            if lowered in false_values:
                return False

        return bool(value)

    def _normalize_enum(
        self,
        value: Any,
        allowed_values: Optional[list[Any]] = None,
    ) -> Any:
        """Normalize an enum value."""
        if allowed_values is None:
            return value

        for allowed in allowed_values:
            if str(value).lower() == str(allowed).lower():
                return allowed

        raise ValueError(f"Value '{value}' not in allowed values: {allowed_values}")

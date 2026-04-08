"""
Data Formatter Action Module.

Formats data values according to specified patterns including dates,
numbers, currencies, percentages, and custom string formatting.

Author: RabAi Team
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union


class FormatType(Enum):
    """Supported format types."""
    NUMBER = "number"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    STRING = "string"
    BOOLEAN = "boolean"
    PHONE = "phone"
    POSTAL_CODE = "postal_code"
    UPPERCASE = "uppercase"
    LOWERCASE = "lowercase"
    TITLE_CASE = "title_case"
    CUSTOM = "custom"


@dataclass
class FormatConfig:
    """Configuration for a single format operation."""
    format_type: FormatType
    pattern: Optional[str] = None
    locale: str = "en_US"
    currency_code: Optional[str] = None
    decimal_places: int = 2
    show_currency_symbol: bool = True
    date_format: str = "%Y-%m-%d"
    time_format: str = "%H:%M:%S"
    custom_fn: Optional[Callable] = None


@dataclass
class FormatResult:
    """Result of formatting an individual value."""
    original_value: Any
    formatted_value: Any
    format_type: FormatType
    success: bool
    error: Optional[str] = None


class DateFormatter:
    """Formats date and datetime values."""

    def __init__(self, config: FormatConfig):
        self.config = config
        self.date_format = config.date_format
        self.time_format = config.time_format

    def format(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"]:
                try:
                    value = datetime.strptime(value, fmt)
                    break
                except ValueError:
                    continue
        if isinstance(value, datetime):
            return value.strftime(self.date_format)
        if isinstance(value, date):
            return value.strftime(self.date_format)
        return str(value)


class NumberFormatter:
    """Formats numeric values."""

    def __init__(self, config: FormatConfig):
        self.config = config
        self.decimal_places = config.decimal_places

    def format(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            num = float(value)
            return f"{num:.{self.decimal_places}f}"
        except (ValueError, TypeError):
            return str(value)


class CurrencyFormatter:
    """Formats currency values."""

    SYMBOLS = {
        "USD": "$",
        "EUR": "€",
        "GBP": "£",
        "JPY": "¥",
        "CNY": "¥",
        "KRW": "₩",
        "INR": "₹",
    }

    def __init__(self, config: FormatConfig):
        self.config = config
        self.currency_code = config.currency_code or "USD"
        self.decimal_places = config.decimal_places
        self.show_symbol = config.show_currency_symbol

    def format(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            num = float(value)
            symbol = self.SYMBOLS.get(self.currency_code, self.currency_code + " ")
            formatted = f"{num:.{self.decimal_places}f}"
            if self.show_symbol:
                return f"{symbol}{formatted}"
            return f"{formatted} {self.currency_code}"
        except (ValueError, TypeError):
            return str(value)


class PercentageFormatter:
    """Formats percentage values."""

    def __init__(self, config: FormatConfig):
        self.config = config
        self.decimal_places = config.decimal_places

    def format(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            num = float(value)
            return f"{num:.{self.decimal_places}f}%"
        except (ValueError, TypeError):
            return str(value)


class PhoneFormatter:
    """Formats phone numbers."""

    def __init__(self, config: FormatConfig):
        self.config = config

    def format(self, value: Any) -> str:
        if value is None:
            return ""
        digits = re.sub(r"\D", "", str(value))
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        if len(digits) == 11 and digits[0] == "1":
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        return digits


class DataFormatter:
    """
    Formats data values according to specified patterns.

    Supports date, number, currency, percentage, phone, and custom formatting
    with locale-aware output.

    Example:
        >>> formatter = DataFormatter()
        >>> formatter.format(1234.567, FormatType.CURRENCY, currency_code="USD")
        '$1234.57'
        >>> formatter.format(0.4567, FormatType.PERCENTAGE)
        '46.57%'
    """

    def __init__(self):
        self._formatters: Dict[FormatType, Callable] = {}

    def format(
        self,
        value: Any,
        format_type: FormatType,
        **kwargs,
    ) -> FormatResult:
        """Format a single value."""
        config = FormatConfig(format_type=format_type, **kwargs)

        try:
            if format_type == FormatType.NUMBER:
                formatted = NumberFormatter(config).format(value)
            elif format_type == FormatType.CURRENCY:
                formatted = CurrencyFormatter(config).format(value)
            elif format_type == FormatType.PERCENTAGE:
                formatted = PercentageFormatter(config).format(value)
            elif format_type == FormatType.DATE:
                formatted = DateFormatter(config).format(value)
            elif format_type == FormatType.TIME:
                formatted = DateFormatter(config).format(value)
            elif format_type == FormatType.PHONE:
                formatted = PhoneFormatter(config).format(value)
            elif format_type == FormatType.UPPERCASE:
                formatted = str(value).upper() if value is not None else ""
            elif format_type == FormatType.LOWERCASE:
                formatted = str(value).lower() if value is not None else ""
            elif format_type == FormatType.TITLE_CASE:
                formatted = str(value).title() if value is not None else ""
            elif format_type == FormatType.BOOLEAN:
                formatted = "true" if value else "false"
            elif format_type == FormatType.STRING:
                formatted = str(value) if value is not None else ""
            else:
                formatted = str(value)

            return FormatResult(
                original_value=value,
                formatted_value=formatted,
                format_type=format_type,
                success=True,
            )
        except Exception as e:
            return FormatResult(
                original_value=value,
                formatted_value=str(value),
                format_type=format_type,
                success=False,
                error=str(e),
            )

    def format_series(
        self,
        values: List[Any],
        format_type: FormatType,
        **kwargs,
    ) -> List[FormatResult]:
        """Format a series of values."""
        return [self.format(v, format_type, **kwargs) for v in values]

    def format_dict(
        self,
        data: Dict[str, Any],
        format_specs: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Format dictionary values based on specifications."""
        result = {}
        for key, value in data.items():
            if key in format_specs:
                spec = format_specs[key]
                fmt_type = FormatType(spec["type"])
                result[key] = self.format(value, fmt_type, **spec.get("kwargs", {})).formatted_value
            else:
                result[key] = value
        return result


def create_formatter() -> DataFormatter:
    """Factory to create a data formatter."""
    return DataFormatter()

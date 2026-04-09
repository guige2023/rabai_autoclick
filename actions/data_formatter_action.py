"""Data Formatter and Serializer.

This module provides data formatting:
- Multi-format serialization
- Date/time formatting
- Number and currency formatting
- Custom formatters

Example:
    >>> from actions.data_formatter_action import DataFormatter
    >>> formatter = DataFormatter()
    >>> result = formatter.format_record(record, format_rules)
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, date
from typing import Any, Callable, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class DataFormatter:
    """Formats data for display and export."""

    def __init__(self) -> None:
        """Initialize the data formatter."""
        self._formatters: dict[str, Callable] = {}
        self._lock = threading.Lock()
        self._stats = {"records_formatted": 0}

    def register_formatter(
        self,
        field_name: str,
        formatter: Callable[[Any], str],
    ) -> None:
        """Register a custom formatter for a field.

        Args:
            field_name: Field name.
            formatter: Function that takes value and returns string.
        """
        with self._lock:
            self._formatters[field_name] = formatter

    def format_record(
        self,
        record: dict[str, Any],
        format_rules: Optional[dict[str, dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        """Format a record according to rules.

        Args:
            record: Record to format.
            format_rules: Dict mapping field names to format configs.

        Returns:
            Formatted record.
        """
        formatted = dict(record)

        if format_rules:
            for field, rules in format_rules.items():
                if field not in formatted:
                    continue

                value = formatted[field]

                if "type" in rules:
                    if rules["type"] == "date":
                        formatted[field] = self.format_date(value, rules.get("format"))
                    elif rules["type"] == "number":
                        formatted[field] = self.format_number(value, rules.get("decimals", 2))
                    elif rules["type"] == "currency":
                        formatted[field] = self.format_currency(value, rules.get("symbol", "$"))
                    elif rules["type"] == "percentage":
                        formatted[field] = self.format_percentage(value, rules.get("decimals", 1))

                if "uppercase" in rules and rules["uppercase"]:
                    formatted[field] = str(value).upper()
                if "lowercase" in rules and rules["lowercase"]:
                    formatted[field] = str(value).lower()
                if "trim" in rules and rules["trim"]:
                    formatted[field] = str(value).strip()

        for field_name, formatter in self._formatters.items():
            if field_name in formatted:
                try:
                    formatted[field_name] = formatter(formatted[field_name])
                except Exception as e:
                    logger.error("Formatter error for %s: %s", field_name, e)

        with self._lock:
            self._stats["records_formatted"] += 1

        return formatted

    def format_date(
        self,
        value: Any,
        fmt: str = "%Y-%m-%d",
    ) -> str:
        """Format a date value.

        Args:
            value: Date value (timestamp, string, or datetime).
            fmt: strftime format string.

        Returns:
            Formatted date string.
        """
        if value is None:
            return ""

        if isinstance(value, datetime):
            return value.strftime(fmt)
        if isinstance(value, date):
            return value.strftime(fmt)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value).strftime(fmt)
        if isinstance(value, str):
            for input_fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(value[:19], input_fmt).strftime(fmt)
                except ValueError:
                    continue
        return str(value)

    def format_number(
        self,
        value: Any,
        decimals: int = 2,
    ) -> str:
        """Format a number.

        Args:
            value: Number value.
            decimals: Number of decimal places.

        Returns:
            Formatted number string.
        """
        try:
            num = float(value)
            return f"{num:,.{decimals}f}"
        except (ValueError, TypeError):
            return str(value)

    def format_currency(
        self,
        value: Any,
        symbol: str = "$",
        decimals: int = 2,
    ) -> str:
        """Format a currency value.

        Args:
            value: Numeric value.
            symbol: Currency symbol.
            decimals: Decimal places.

        Returns:
            Formatted currency string.
        """
        try:
            num = float(value)
            return f"{symbol}{num:,.{decimals}f}"
        except (ValueError, TypeError):
            return str(value)

    def format_percentage(
        self,
        value: Any,
        decimals: int = 1,
    ) -> str:
        """Format a percentage value.

        Args:
            value: Numeric value (0-1 or 0-100).
            decimals: Decimal places.

        Returns:
            Formatted percentage string.
        """
        try:
            num = float(value)
            if num <= 1:
                num *= 100
            return f"{num:.{decimals}f}%"
        except (ValueError, TypeError):
            return str(value)

    def truncate(
        self,
        value: str,
        max_length: int,
        suffix: str = "...",
    ) -> str:
        """Truncate a string.

        Args:
            value: String to truncate.
            max_length: Maximum length.
            suffix: Suffix for truncated strings.

        Returns:
            Truncated string.
        """
        if len(value) <= max_length:
            return value
        return value[:max_length - len(suffix)] + suffix

    def format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human-readable string.

        Args:
            bytes_value: Number of bytes.

        Returns:
            Formatted string like "1.5 MB".
        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"

    def format_phone(
        self,
        phone: str,
        format_type: str = "US",
    ) -> str:
        """Format a phone number.

        Args:
            phone: Raw phone string.
            format_type: Format type (US, international).

        Returns:
            Formatted phone number.
        """
        digits = "".join(c for c in phone if c.isdigit())

        if format_type == "US" and len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif format_type == "US" and len(digits) == 11:
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"

        return phone

    def format_record_batch(
        self,
        records: list[dict[str, Any]],
        format_rules: Optional[dict[str, dict[str, Any]]] = None,
    ) -> list[dict[str, Any]]:
        """Format a batch of records.

        Args:
            records: List of records.
            format_rules: Format rules.

        Returns:
            List of formatted records.
        """
        return [self.format_record(r, format_rules) for r in records]

    def to_json(
        self,
        records: list[dict[str, Any]],
        indent: int = 2,
    ) -> str:
        """Serialize records to JSON.

        Args:
            records: List of records.
            indent: JSON indent.

        Returns:
            JSON string.
        """
        return json.dumps(records, indent=indent, default=str)

    def get_stats(self) -> dict[str, int]:
        """Get formatter statistics."""
        with self._lock:
            return dict(self._stats)

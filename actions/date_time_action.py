"""Date/time action for datetime manipulation.

This module provides datetime utilities including
formatting, parsing, arithmetic, and timezone conversion.

Example:
    >>> action = DateTimeAction()
    >>> result = action.execute(operation="now")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class DateTimeResult:
    """Result from datetime operation."""
    datetime: Any
    formatted: str
    timestamp: float


class DateTimeAction:
    """Date/time manipulation action.

    Provides datetime operations including formatting,
    parsing, arithmetic, and timezone handling.

    Example:
        >>> action = DateTimeAction()
        >>> result = action.execute(
        ...     operation="format",
        ...     datetime="2024-01-15",
        ...     format="%Y-%m-%d %H:%M:%S"
        ... )
    """

    def __init__(self) -> None:
        """Initialize datetime action."""
        pass

    def execute(
        self,
        operation: str,
        datetime_str: Optional[str] = None,
        format: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute datetime operation.

        Args:
            operation: Operation (now, format, parse, add, etc.).
            datetime_str: Datetime string.
            format: Format string.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        from datetime import datetime, timedelta

        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "now":
            dt = datetime.now()
            result["datetime"] = dt.isoformat()
            result["timestamp"] = dt.timestamp()
            if format:
                result["formatted"] = dt.strftime(format)

        elif op == "utcnow":
            dt = datetime.utcnow()
            result["datetime"] = dt.isoformat()
            result["timestamp"] = dt.timestamp()

        elif op == "format":
            if not datetime_str:
                raise ValueError("datetime_str required")
            if not format:
                raise ValueError("format required")
            dt = self._parse_datetime(datetime_str)
            result["formatted"] = dt.strftime(format)

        elif op == "parse":
            if not datetime_str:
                raise ValueError("datetime_str required")
            dt = self._parse_datetime(datetime_str)
            result["datetime"] = dt.isoformat()
            result["timestamp"] = dt.timestamp()
            result["year"] = dt.year
            result["month"] = dt.month
            result["day"] = dt.day
            result["hour"] = dt.hour
            result["minute"] = dt.minute
            result["second"] = dt.second

        elif op == "add":
            if not datetime_str:
                raise ValueError("datetime_str required")
            dt = self._parse_datetime(datetime_str)
            days = kwargs.get("days", 0)
            hours = kwargs.get("hours", 0)
            minutes = kwargs.get("minutes", 0)
            seconds = kwargs.get("seconds", 0)
            new_dt = dt + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
            result["datetime"] = new_dt.isoformat()
            result["timestamp"] = new_dt.timestamp()

        elif op == "subtract":
            if not datetime_str:
                raise ValueError("datetime_str required")
            dt = self._parse_datetime(datetime_str)
            days = kwargs.get("days", 0)
            hours = kwargs.get("hours", 0)
            new_dt = dt - timedelta(days=days, hours=hours)
            result["datetime"] = new_dt.isoformat()
            result["timestamp"] = new_dt.timestamp()

        elif op == "diff":
            dt1_str = kwargs.get("datetime1")
            dt2_str = kwargs.get("datetime2")
            if not dt1_str or not dt2_str:
                raise ValueError("datetime1 and datetime2 required")
            dt1 = self._parse_datetime(dt1_str)
            dt2 = self._parse_datetime(dt2_str)
            diff = dt1 - dt2
            result["seconds"] = diff.total_seconds()
            result["days"] = diff.days
            result["hours"] = diff.total_seconds() / 3600

        elif op == "timestamp":
            if not datetime_str:
                raise ValueError("datetime_str required")
            dt = self._parse_datetime(datetime_str)
            result["timestamp"] = dt.timestamp()

        elif op == "from_timestamp":
            ts = kwargs.get("timestamp")
            if ts is None:
                raise ValueError("timestamp required")
            dt = datetime.fromtimestamp(float(ts))
            result["datetime"] = dt.isoformat()

        elif op == "weekday":
            if not datetime_str:
                raise ValueError("datetime_str required")
            dt = self._parse_datetime(datetime_str)
            result["weekday"] = dt.strftime("%A")
            result["weekday_num"] = dt.weekday()

        elif op == "isoweekday":
            if not datetime_str:
                raise ValueError("datetime_str required")
            dt = self._parse_datetime(datetime_str)
            result["isoweekday"] = dt.isoweekday()

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _parse_datetime(self, datetime_str: str):
        """Parse datetime string with common formats.

        Args:
            datetime_str: Datetime string.

        Returns:
            datetime object.
        """
        from datetime import datetime

        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue

        # Try ISO format
        try:
            from dateutil import parser
            return parser.parse(datetime_str)
        except Exception:
            raise ValueError(f"Could not parse datetime: {datetime_str}")

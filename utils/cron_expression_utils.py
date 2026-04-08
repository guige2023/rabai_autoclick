"""
Cron expression parsing and scheduling utilities.

Provides cron expression parsing, next-run calculation,
and simple scheduling helpers.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from typing import Callable


CRON_FIELDS = ["minute", "hour", "day_of_month", "month", "day_of_week"]


class CronField:
    """Represents a single cron field with pattern matching."""

    def __init__(self, value: str, min_val: int, max_val: int):
        self.value = value
        self.min_val = min_val
        self.max_val = max_val
        self._ranges: list[tuple[int, int]] = []
        self._points: set[int] = set()
        self._last: int = -1
        self._parse()

    def _parse(self) -> None:
        """Parse cron field into ranges and specific points."""
        if self.value == "*":
            self._ranges = [(self.min_val, self.max_val)]
            return

        parts = self.value.split(",")
        for part in parts:
            part = part.strip()
            if "/" in part:
                range_part, step = part.split("/", 1)
                step = int(step)
                if range_part == "*":
                    start, end = self.min_val, self.max_val
                elif "-" in range_part:
                    start_str, end_str = range_part.split("-", 1)
                    start, end = int(start_str), int(end_str)
                else:
                    start = int(range_part)
                    end = self.max_val
                current = start
                while current <= end:
                    self._points.add(current)
                    current += step
            elif "-" in part:
                start_str, end_str = part.split("-", 1)
                self._ranges.append((int(start_str), int(end_str)))
            else:
                self._points.add(int(part))

    def matches(self, value: int) -> bool:
        """Check if value matches this field."""
        if value in self._points:
            return True
        for start, end in self._ranges:
            if start <= value <= end:
                return True
        return False

    def next(self, current: int) -> int | None:
        """Find next value after current."""
        for value in sorted(self._points):
            if value > current:
                return value
        for start, end in sorted(self._ranges):
            if end > current:
                return max(start, current + 1)
        return None


class CronExpression:
    """Parsed cron expression."""

    FIELD_RANGES = {
        "minute": (0, 59),
        "hour": (0, 23),
        "day_of_month": (1, 31),
        "month": (1, 12),
        "day_of_week": (0, 6),
    }

    def __init__(self, expression: str):
        self.raw = expression
        self._fields: list[CronField] = []
        self._parse(expression)

    def _parse(self, expression: str) -> None:
        parts = expression.split()
        if len(parts) not in (5, 6):
            raise ValueError(f"Invalid cron expression: {expression}")

        for i, (name, value) in enumerate(zip(CRON_FIELDS, parts[:5])):
            min_val, max_val = self.FIELD_RANGES[name]
            self._fields.append(CronField(value, min_val, max_val))

    def matches(self, dt: datetime) -> bool:
        """Check if datetime matches the cron expression."""
        values = [dt.minute, dt.hour, dt.day, dt.month, dt.weekday()]
        return all(field.matches(val) for field, val in zip(self._fields, values))

    def next_run(self, after: datetime | None = None) -> datetime:
        """
        Calculate next run time after given datetime.

        Args:
            after: Starting datetime (defaults to now)

        Returns:
            Next matching datetime
        """
        if after is None:
            after = datetime.now()

        dt = after.replace(second=0, microsecond=0)
        if len(self._fields) == 5:
            self._fields.insert(2, CronField("*", 1, 31))

        for _ in range(60 * 60 * 24 * 366):
            minute_field, hour_field, dom_field, month_field, dow_field = self._fields[:5]

            month = month_field.next(dt.month)
            if month is None:
                dt = dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0)
                continue

            if month != dt.month:
                dt = dt.replace(year=dt.year, month=month, day=1, hour=0, minute=0)
                continue

            day = dom_field.next(dt.day)
            if day is None:
                dt = dt.replace(month=month, day=1, hour=0, minute=0)
                continue

            if day != dt.day:
                dt = dt.replace(day=day, hour=0, minute=0)
                continue

            hour = hour_field.next(dt.hour)
            if hour is None:
                dt = dt.replace(day=day, hour=0, minute=0)
                continue

            if hour != dt.hour:
                dt = dt.replace(hour=hour, minute=0)
                continue

            minute = minute_field.next(dt.minute)
            if minute is None:
                dt = dt.replace(hour=hour, minute=0)
                continue

            dt = dt.replace(minute=minute)
            dow = dt.weekday()
            if dow_field.matches(dow):
                return dt

            dt = dt.replace(day=day, hour=hour, minute=minute) + timedelta(days=1)
            dt = dt.replace(hour=0, minute=0)

        raise RuntimeError("Could not find next run within a year")

    def __str__(self) -> str:
        return self.raw


def parse_cron(expression: str) -> CronExpression:
    """Parse cron expression string."""
    return CronExpression(expression)


def is_valid_cron(expression: str) -> bool:
    """Check if expression is valid."""
    try:
        CronExpression(expression)
        return True
    except (ValueError, IndexError):
        return False


class CronScheduler:
    """Simple cron-based scheduler."""

    def __init__(self):
        self._jobs: list[tuple[CronExpression, Callable, str]] = []

    def add_job(
        self,
        expression: str,
        func: Callable,
        name: str = "",
    ) -> None:
        """Add a cron job."""
        cron = CronExpression(expression)
        self._jobs.append((cron, func, name))

    def run_pending(self) -> list[str]:
        """Run any pending jobs that should execute now."""
        now = datetime.now()
        executed = []
        for cron, func, name in self._jobs:
            if cron.matches(now):
                try:
                    func()
                    executed.append(name or str(cron))
                except Exception:
                    pass
        return executed

    def get_next_run(self) -> datetime | None:
        """Get nearest next run time."""
        next_times = [cron.next_run() for cron, _, _ in self._jobs]
        return min(next_times) if next_times else None

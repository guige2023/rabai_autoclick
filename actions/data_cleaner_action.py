"""
Data Cleaning and Normalization Action Module.

Cleans, normalizes, and deduplicates structured data.
Handles missing values, outliers, encoding issues, and format consistency.

Example:
    >>> from data_cleaner_action import DataCleaner, CleanConfig
    >>> cleaner = DataCleaner()
    >>> cleaned = cleaner.clean(records, remove_duplicates=True, trim_strings=True)
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class CleanConfig:
    """Configuration for data cleaning."""
    trim_strings: bool = True
    lowercase_strings: bool = False
    remove_nulls: bool = False
    remove_duplicates: bool = False
    dedupe_key: Optional[str] = None
    normalize_whitespace: bool = True
    remove_html: bool = True
    remove_special_chars: bool = False
    fill_missing: Optional[dict[str, Any]] = None


@dataclass
class CleaningReport:
    """Report of cleaning operations."""
    input_count: int
    output_count: int
    nulls_removed: int
    duplicates_removed: int
    strings_trimmed: int
    html_removed: int
    special_chars_removed: int


class DataCleaner:
    """Clean and normalize structured data."""

    HTML_TAG_RE = re.compile(r"<[^>]+>")
    WHITESPACE_RE = re.compile(r"\s+")
    SPECIAL_CHARS_RE = re.compile(r"[^\w\s.-]")

    def __init__(self):
        self._report: Optional[CleaningReport] = None

    def clean(
        self,
        records: list[dict[str, Any]],
        config: Optional[CleanConfig] = None,
    ) -> list[dict[str, Any]]:
        """
        Clean a list of records with configurable rules.

        Args:
            records: Input list of dictionaries
            config: CleanConfig with cleaning options

        Returns:
            Cleaned list of dictionaries
        """
        config = config or CleanConfig()
        report = CleaningReport(
            input_count=len(records),
            output_count=0,
            nulls_removed=0,
            duplicates_removed=0,
            strings_trimmed=0,
            html_removed=0,
            special_chars_removed=0,
        )

        result: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for record in records:
            cleaned: dict[str, Any] = {}

            for key, value in record.items():
                new_value = self._clean_value(
                    value,
                    config,
                    report,
                )
                cleaned[key] = new_value

            if config.remove_nulls and self._is_all_null(cleaned):
                report.nulls_removed += 1
                continue

            if config.remove_duplicates:
                key_str = self._record_key(cleaned, config.dedupe_key)
                if key_str in seen_keys:
                    report.duplicates_removed += 1
                    continue
                seen_keys.add(key_str)

            result.append(cleaned)

        report.output_count = len(result)
        self._report = report
        return result

    def _clean_value(
        self,
        value: Any,
        config: CleanConfig,
        report: CleaningReport,
    ) -> Any:
        if value is None:
            return None

        if isinstance(value, str):
            original = value

            if config.trim_strings:
                value = value.strip()
                if value != original:
                    report.strings_trimmed += 1

            if config.normalize_whitespace:
                value = self.WHITESPACE_RE.sub(" ", value).strip()

            if config.lowercase_strings:
                value = value.lower()

            if config.remove_html:
                was_html = value != self.HTML_TAG_RE.sub("", value)
                value = self.HTML_TAG_RE.sub("", value)
                if was_html:
                    report.html_removed += 1

            if config.remove_special_chars:
                value = self.SPECIAL_CHARS_RE.sub("", value)
                if value != original:
                    report.special_chars_removed += 1

            if not value:
                return None

        return value

    def _is_all_null(self, record: dict[str, Any]) -> bool:
        return all(v is None for v in record.values())

    def _record_key(self, record: dict[str, Any], key: Optional[str] = None) -> str:
        if key:
            return str(record.get(key, ""))
        return "|".join(f"{k}={v}" for k, v in sorted(record.items()))

    def normalize_names(
        self,
        records: list[dict[str, Any]],
        field: str,
        style: str = "title_case",
    ) -> list[dict[str, Any]]:
        """Normalize name fields to specific style."""
        result: list[dict[str, Any]] = []
        for record in dict(records):
            if field in record and isinstance(record[field], str):
                name = record[field].strip()
                if style == "lower":
                    record[field] = name.lower()
                elif style == "upper":
                    record[field] = name.upper()
                elif style == "title_case":
                    record[field] = self._title_case(name)
                elif style == "snake_case":
                    record[field] = self._to_snake_case(name)
                elif style == "camel_case":
                    record[field] = self._to_camel_case(name)
            result.append(record)
        return result

    def _title_case(self, text: str) -> str:
        text = text.lower()
        words = text.split()
        result = []
        for word in words:
            if len(word) <= 2 and word not in ("of", "in", "on", "at", "to", "by"):
                result.append(word)
            else:
                result.append(word.capitalize())
        return " ".join(result)

    def _to_snake_case(self, text: str) -> str:
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[\s]+", "_", text)
        return text.lower().strip("_")

    def _to_camel_case(self, text: str) -> str:
        parts = re.split(r"[\s_-]+", text)
        return parts[0].lower() + "".join(p.capitalize() for p in parts[1:])

    def standardize_dates(
        self,
        records: list[dict[str, Any]],
        field: str,
        output_format: str = "%Y-%m-%d",
    ) -> list[dict[str, Any]]:
        """Standardize date field to common format."""
        from datetime import datetime
        result: list[dict[str, Any]] = []
        for record in dict(records):
            if field in record and record[field]:
                date_str = str(record[field])
                parsed = self._parse_date(date_str)
                if parsed:
                    record[field] = parsed.strftime(output_format)
            result.append(record)
        return result

    def _parse_date(self, date_str: str) -> Optional[Any]:
        from datetime import datetime
        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y/%m/%d",
            "%b %d, %Y",
            "%B %d, %Y",
            "%d-%b-%Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                pass
        return None

    def normalize_phones(
        self,
        records: list[dict[str, Any]],
        field: str,
        country: str = "US",
    ) -> list[dict[str, Any]]:
        """Normalize phone numbers to standard format."""
        result: list[dict[str, Any]] = []
        for record in dict(records):
            if field in record and record[field]:
                digits = re.sub(r"\D", "", str(record[field]))
                if country == "US" and len(digits) == 10:
                    record[field] = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                elif len(digits) == 11 and digits[0] == "1":
                    record[field] = f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
                else:
                    record[field] = digits
            result.append(record)
        return result

    def normalize_emails(
        self,
        records: list[dict[str, Any]],
        field: str,
    ) -> list[dict[str, Any]]:
        """Normalize email addresses to lowercase."""
        result: list[dict[str, Any]] = []
        for record in dict(records):
            if field in record and record[field]:
                record[field] = str(record[field]).lower().strip()
            result.append(record)
        return result

    def standardize_postal_codes(
        self,
        records: list[dict[str, Any]],
        field: str,
        country: str = "US",
    ) -> list[dict[str, Any]]:
        """Normalize postal codes."""
        result: list[dict[str, Any]] = []
        for record in dict(records):
            if field in record and record[field]:
                code = str(record[field]).strip()
                if country == "US" and len(code) == 5:
                    record[field] = code
                elif country == "US" and len(code) == 9:
                    record[field] = f"{code[:5]}-{code[5:]}"
                elif country == "CA":
                    code = re.sub(r"\s+", "", code.upper())
                    record[field] = code
            result.append(record)
        return result

    def remove_outliers(
        self,
        records: list[dict[str, Any]],
        field: str,
        std_devs: float = 3.0,
    ) -> list[dict[str, Any]]:
        """Remove numeric outliers using standard deviation."""
        values = []
        for record in records:
            try:
                values.append(float(record.get(field, 0)))
            except (ValueError, TypeError):
                pass
        if not values:
            return records

        import statistics
        mean = statistics.mean(values)
        stdev = statistics.stdev(values) if len(values) > 1 else 0
        lower = mean - (std_devs * stdev)
        upper = mean + (std_devs * stdev)

        result: list[dict[str, Any]] = []
        removed = 0
        for record in records:
            try:
                val = float(record.get(field, 0))
                if lower <= val <= upper:
                    result.append(record)
                else:
                    removed += 1
            except (ValueError, TypeError):
                result.append(record)

        return result

    def fill_missing_values(
        self,
        records: list[dict[str, Any]],
        strategy: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Fill missing values using per-field strategies."""
        result: list[dict[str, Any]] = []
        for record in dict(records):
            for field, fill_value in strategy.items():
                if field not in record or record[field] is None:
                    if callable(fill_value):
                        record[field] = fill_value(records)
                    else:
                        record[field] = fill_value
            result.append(record)
        return result

    def get_report(self) -> Optional[CleaningReport]:
        return self._report


if __name__ == "__main__":
    cleaner = DataCleaner()
    records = [
        {"name": "  Alice Smith  ", "email": "ALICE@EXAMPLE.COM", "age": 30},
        {"name": "  Alice Smith  ", "email": "alice@example.com", "age": 25},
        {"name": "  <b>Bob</b>", "email": "bob@example.com", "age": None},
    ]
    config = CleanConfig(trim_strings=True, remove_duplicates=True, remove_html=True, lowercase_strings=True)
    cleaned = cleaner.clean(records, config)
    print(f"Cleaned {len(cleaned)} records")
    for r in cleaned:
        print(r)

"""
Data Normalizer Action Module.

Normalizes data formats across different sources: date formats, number formats,
unit conversions, and string encodings with locale awareness.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class NormalizationResult:
    """Result of data normalization."""
    records: list[dict[str, Any]]
    fields_normalized: list[str]
    conversion_count: int


class DataNormalizerAction(BaseAction):
    """Normalize data formats across different sources."""

    def __init__(self) -> None:
        super().__init__("data_normalizer")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Normalize data in records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - field_mappings: Dict of field -> normalization type
                    types: date, number, string, unit, currency
                - date_formats: List of input date formats to try
                - number_format: Target number format
                - locale: Locale for number/currency formatting

        Returns:
            NormalizationResult with normalized records
        """
        import re

        records = params.get("records", [])
        field_mappings = params.get("field_mappings", {})
        date_formats = params.get("date_formats", ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"])
        conversion_count = 0

        for r in records:
            if not isinstance(r, dict):
                continue
            for field, norm_type in field_mappings.items():
                if field not in r:
                    continue
                value = r[field]
                if value is None:
                    continue

                if norm_type == "date":
                    normalized = self._normalize_date(value, date_formats)
                    if normalized != value:
                        r[field] = normalized
                        conversion_count += 1

                elif norm_type == "number":
                    normalized = self._normalize_number(value)
                    if normalized != value:
                        r[field] = normalized
                        conversion_count += 1

                elif norm_type == "string":
                    normalized = self._normalize_string(value)
                    if normalized != str(value):
                        r[field] = normalized
                        conversion_count += 1

                elif norm_type == "unit":
                    normalized = self._normalize_unit(value)
                    if normalized != value:
                        r[field] = normalized
                        conversion_count += 1

        return NormalizationResult(
            records=records,
            fields_normalized=list(field_mappings.keys()),
            conversion_count=conversion_count
        )

    def _normalize_date(self, value: Any, formats: list[str]) -> Optional[str]:
        """Normalize date string to ISO format."""
        from datetime import datetime
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value).strftime("%Y-%m-%d")
            except Exception:
                return str(value)
        if not isinstance(value, str):
            return str(value)
        for fmt in formats:
            try:
                dt = datetime.strptime(value, fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                continue
        return value

    def _normalize_number(self, value: Any) -> Optional[float]:
        """Normalize number string to float."""
        if isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, str):
            return None
        cleaned = re.sub(r'[,$€£¥]', '', str(value).strip())
        try:
            return float(cleaned)
        except Exception:
            return None

    def _normalize_string(self, value: Any) -> str:
        """Normalize string: trim, lowercase, collapse spaces."""
        if not isinstance(value, str):
            return str(value).strip()
        return re.sub(r'\s+', ' ', value.strip().lower())

    def _normalize_unit(self, value: Any) -> Optional[float]:
        """Normalize unit string to base unit."""
        if isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, str):
            return None
        unit_factors = {
            'km': 1000, 'm': 1, 'cm': 0.01, 'mm': 0.001,
            'kg': 1000, 'g': 1, 'mg': 0.001,
            'l': 1, 'ml': 0.001
        }
        match = re.match(r'([\d.]+)\s*([a-zA-Z]+)', value.strip())
        if match:
            number, unit = float(match.group(1)), match.group(2).lower()
            if unit in unit_factors:
                return number * unit_factors[unit]
        return None

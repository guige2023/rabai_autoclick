"""Data normalizer action module for RabAI AutoClick.

Provides data normalization:
- DataNormalizer: Normalize data values
- StringNormalizer: Normalize strings
- NumberNormalizer: Normalize numbers
- DateNormalizer: Normalize dates
"""

import re
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataNormalizer:
    """General data normalizer."""

    def __init__(self):
        self._normalizers: Dict[str, Callable] = {}

    def register(self, field: str, normalizer: Callable) -> "DataNormalizer":
        """Register normalizer for field."""
        self._normalizers[field] = normalizer
        return self

    def normalize(self, data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """Normalize data."""
        if isinstance(data, list):
            return [self._normalize_single(item) for item in data]
        return self._normalize_single(data)

    def _normalize_single(self, item: Dict) -> Dict:
        """Normalize single item."""
        result = dict(item)
        for field, normalizer in self._normalizers.items():
            if field in result:
                try:
                    result[field] = normalizer(result[field])
                except Exception:
                    pass
        return result


class StringNormalizer:
    """Normalize string values."""

    @staticmethod
    def lowercase(value: str) -> str:
        """Convert to lowercase."""
        return str(value).lower().strip()

    @staticmethod
    def uppercase(value: str) -> str:
        """Convert to uppercase."""
        return str(value).upper().strip()

    @staticmethod
    def trim(value: str) -> str:
        """Trim whitespace."""
        return str(value).strip()

    @staticmethod
    def remove_special_chars(value: str, keep: str = "") -> str:
        """Remove special characters."""
        pattern = f"[^a-zA-Z0-9{re.escape(keep)}]"
        return re.sub(pattern, "", str(value))

    @staticmethod
    def normalize_whitespace(value: str) -> str:
        """Normalize whitespace."""
        return re.sub(r"\s+", " ", str(value)).strip()

    @staticmethod
    def slugify(value: str) -> str:
        """Convert to slug."""
        value = str(value).lower().strip()
        value = re.sub(r"[^\w\s-]", "", value)
        value = re.sub(r"[-\s]+", "-", value)
        return value


class NumberNormalizer:
    """Normalize numeric values."""

    @staticmethod
    def to_int(value: Any, default: int = 0) -> int:
        """Convert to integer."""
        try:
            return int(float(str(value)))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def to_float(value: Any, default: float = 0.0) -> float:
        """Convert to float."""
        try:
            return float(str(value).replace(",", ""))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def clamp(value: Union[int, float], min_val: float, max_val: float) -> Union[int, float]:
        """Clamp value to range."""
        return max(min_val, min(max_val, value))

    @staticmethod
    def round_to(value: float, decimals: int) -> float:
        """Round to decimal places."""
        return round(value, decimals)


class DateNormalizer:
    """Normalize date values."""

    @staticmethod
    def parse_date(value: Any, format_str: Optional[str] = None) -> Optional[datetime]:
        """Parse date."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        if isinstance(value, str):
            if format_str:
                try:
                    return datetime.strptime(value, format_str)
                except ValueError:
                    pass
            formats = [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d-%m-%Y",
                "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
        return None

    @staticmethod
    def to_timestamp(value: Any) -> Optional[float]:
        """Convert to timestamp."""
        dt = DateNormalizer.parse_date(value)
        return dt.timestamp() if dt else None

    @staticmethod
    def to_iso(value: Any) -> Optional[str]:
        """Convert to ISO format."""
        dt = DateNormalizer.parse_date(value)
        return dt.isoformat() if dt else None


class DataNormalizerAction(BaseAction):
    """Data normalizer action."""
    action_type = "data_normalizer"
    display_name = "数据规范化"
    description = "数据格式规范化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "normalize")
            data = params.get("data", [])

            if operation == "normalize":
                return self._normalize(data, params)
            elif operation == "string":
                return self._normalize_string(params)
            elif operation == "number":
                return self._normalize_number(params)
            elif operation == "date":
                return self._normalize_date(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Normalizer error: {str(e)}")

    def _normalize(self, data: List[Dict], params: Dict) -> ActionResult:
        """Normalize data."""
        normalizer = DataNormalizer()
        rules = params.get("rules", {})

        for field, rule in rules.items():
            rule_type = rule.get("type", "string").lower()

            if rule_type == "string":
                ops = rule.get("ops", [])
                normalizer.register(field, lambda v: self._apply_string_ops(v, ops))
            elif rule_type == "number":
                normalizer.register(field, lambda v: NumberNormalizer.to_float(v))
            elif rule_type == "date":
                normalizer.register(field, lambda v: DateNormalizer.to_iso(v))

        result = normalizer.normalize(data)

        return ActionResult(
            success=True,
            message=f"Normalized {len(data)} items",
            data={"data": result, "count": len(result)},
        )

    def _apply_string_ops(self, value: str, ops: List[str]) -> str:
        """Apply string operations."""
        result = str(value)
        for op in ops:
            if op == "lowercase":
                result = StringNormalizer.lowercase(result)
            elif op == "uppercase":
                result = StringNormalizer.uppercase(result)
            elif op == "trim":
                result = StringNormalizer.trim(result)
            elif op == "slugify":
                result = StringNormalizer.slugify(result)
            elif op == "normalize_whitespace":
                result = StringNormalizer.normalize_whitespace(result)
        return result

    def _normalize_string(self, params: Dict) -> ActionResult:
        """Normalize string."""
        value = params.get("value", "")
        ops = params.get("ops", ["trim"])

        result = self._apply_string_ops(value, ops)

        return ActionResult(success=True, message="String normalized", data={"value": result})

    def _normalize_number(self, params: Dict) -> ActionResult:
        """Normalize number."""
        value = params.get("value", 0)
        to_type = params.get("type", "float")

        if to_type == "int":
            result = NumberNormalizer.to_int(value)
        else:
            result = NumberNormalizer.to_float(value)

        return ActionResult(success=True, message="Number normalized", data={"value": result})

    def _normalize_date(self, params: Dict) -> ActionResult:
        """Normalize date."""
        value = params.get("value")
        format_str = params.get("format")

        result = DateNormalizer.to_iso(value)

        return ActionResult(
            success=result is not None,
            message="Date normalized" if result else "Invalid date",
            data={"value": result},
        )

# Copyright (c) 2024. coded by claude
"""Data Normalizer Action Module.

Normalizes API data with support for schema normalization,
date format standardization, and unit conversions.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging
import re

logger = logging.getLogger(__name__)


class DateFormat(Enum):
    ISO8601 = "iso8601"
    RFC3339 = "rfc3339"
    UNIX = "unix"
    CUSTOM = "custom"


@dataclass
class NormalizationRule:
    field: str
    normalize_fn: Callable[[Any], Any]
    apply_if: Optional[Callable[[Any], bool]] = None


@dataclass
class NormalizationConfig:
    date_format: DateFormat = DateFormat.ISO8601
    custom_date_format: Optional[str] = None
    lowercase_keys: bool = False
    strip_whitespace: bool = True
    remove_empty: bool = False
    convert_types: bool = True


class DataNormalizer:
    def __init__(self, config: Optional[NormalizationConfig] = None):
        self.config = config or NormalizationConfig()
        self._rules: List[NormalizationRule] = []

    def normalize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in data.items():
            normalized_key = self._normalize_key(key)
            normalized_value = self._normalize_value(value)
            if self.config.remove_empty and self._is_empty(normalized_value):
                continue
            result[normalized_key] = normalized_value
        for rule in self._rules:
            if key := rule.field:
                if rule.apply_if is None or rule.apply_if(result.get(key)):
                    result[key] = rule.normalize_fn(result.get(key))
        return result

    def _normalize_key(self, key: str) -> str:
        if self.config.lowercase_keys:
            return key.lower()
        return key

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, str):
            if self.config.strip_whitespace:
                value = value.strip()
            if self.config.convert_types:
                value = self._try_convert_type(value)
        elif isinstance(value, datetime):
            value = self._normalize_datetime(value)
        elif isinstance(value, dict):
            value = self.normalize(value)
        elif isinstance(value, list):
            value = [self._normalize_value(item) for item in value]
        return value

    def _try_convert_type(self, value: str) -> Any:
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null":
            return None
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    def _normalize_datetime(self, dt: datetime) -> str:
        if self.config.date_format == DateFormat.ISO8601:
            return dt.isoformat()
        elif self.config.date_format == DateFormat.RFC3339:
            return dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        elif self.config.date_format == DateFormat.UNIX:
            return str(int(dt.timestamp()))
        elif self.config.date_format == DateFormat.CUSTOM and self.config.custom_date_format:
            return dt.strftime(self.config.custom_date_format)
        return dt.isoformat()

    def _is_empty(self, value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str) and not value.strip():
            return True
        if isinstance(value, (list, dict)) and not value:
            return True
        return False

    def add_rule(self, rule: NormalizationRule) -> None:
        self._rules.append(rule)

    def normalize_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.normalize(item) for item in items]

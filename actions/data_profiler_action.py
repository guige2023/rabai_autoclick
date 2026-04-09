"""Data Profiler Action module.

Provides data profiling capabilities for analyzing datasets
including statistics, type inference, quality metrics,
and distribution analysis.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

import numpy as np


class DataType(Enum):
    """Inferred data types."""

    UNKNOWN = "unknown"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    STRING = "string"
    DATETIME = "datetime"
    UUID = "uuid"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    JSON = "json"


@dataclass
class FieldProfile:
    """Profile for a single field/column."""

    name: str
    data_type: DataType
    total_count: int
    null_count: int
    unique_count: int
    fill_rate: float

    numeric_stats: Optional[dict[str, float]] = None
    string_stats: Optional[dict[str, Any]] = None

    top_values: list[tuple[Any, int]] = field(default_factory=list)
    histogram: list[tuple[Any, int]] = field(default_factory=list)

    examples: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "total_count": self.total_count,
            "null_count": self.null_count,
            "unique_count": self.unique_count,
            "fill_rate": self.fill_rate,
            "numeric_stats": self.numeric_stats,
            "string_stats": self.string_stats,
            "top_values": [(str(v), c) for v, c in self.top_values[:10]],
            "examples": [str(e) for e in self.examples[:5]],
        }


@dataclass
class DatasetProfile:
    """Overall dataset profile."""

    name: str
    row_count: int
    field_count: int
    fields: list[FieldProfile]
    created_at: datetime = field(default_factory=datetime.now)

    quality_score: float = 0.0
    issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "row_count": self.row_count,
            "field_count": self.field_count,
            "quality_score": self.quality_score,
            "issues": self.issues,
            "created_at": self.created_at.isoformat(),
            "fields": [f.to_dict() for f in self.fields],
        }


def infer_type(value: Any) -> DataType:
    """Infer data type from a value.

    Args:
        value: Value to analyze

    Returns:
        Inferred DataType
    """
    if value is None:
        return DataType.UNKNOWN

    if isinstance(value, bool):
        return DataType.BOOLEAN

    if isinstance(value, int):
        return DataType.INTEGER

    if isinstance(value, float):
        return DataType.FLOAT

    if isinstance(value, (datetime, np.datetime64)):
        return DataType.DATETIME

    if isinstance(value, str):
        value_lower = value.lower()

        if value_lower in ("true", "false", "yes", "no", "0", "1"):
            return DataType.BOOLEAN

        try:
            int(value)
            return DataType.INTEGER
        except ValueError:
            pass

        try:
            float(value)
            return DataType.FLOAT
        except ValueError:
            pass

        datetime_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
            r"^\d{2}/\d{2}/\d{4}$",
        ]
        for pattern in datetime_patterns:
            if re.match(pattern, value):
                return DataType.DATETIME

        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        if re.match(uuid_pattern, value_lower):
            return DataType.UUID

        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if re.match(email_pattern, value):
            return DataType.EMAIL

        url_pattern = r"^https?://"
        if re.match(url_pattern, value):
            return DataType.URL

        phone_pattern = r"^\+?[\d\s\-\(\)]{10,}$"
        if re.match(phone_pattern, value):
            return DataType.PHONE

        try:
            import json
            json.loads(value)
            return DataType.JSON
        except (ValueError, TypeError):
            pass

        return DataType.STRING

    return DataType.UNKNOWN


def compute_numeric_stats(values: list[float]) -> dict[str, float]:
    """Compute statistics for numeric values."""
    if not values:
        return {}

    arr = np.array(values)
    q1 = np.percentile(arr, 25)
    q3 = np.percentile(arr, 75)

    return {
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "std": float(np.std(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "q1": float(q1),
        "q3": float(q3),
        "iqr": float(q3 - q1),
        "skewness": float(((arr - np.mean(arr)) ** 3).mean() / (np.std(arr) ** 3)) if np.std(arr) > 0 else 0,
        "kurtosis": float((((arr - np.mean(arr)) / np.std(arr)) ** 4).mean()) if np.std(arr) > 0 else 0,
    }


def compute_string_stats(values: list[str]) -> dict[str, Any]:
    """Compute statistics for string values."""
    if not values:
        return {}

    lengths = [len(v) for v in values]

    return {
        "min_length": min(lengths),
        "max_length": max(lengths),
        "avg_length": sum(lengths) / len(lengths),
        "total_chars": sum(lengths),
        "unique_chars": len(set("".join(values))),
    }


def profile_field(name: str, values: list[Any]) -> FieldProfile:
    """Profile a single field.

    Args:
        name: Field name
        values: List of values

    Returns:
        FieldProfile
    """
    total_count = len(values)
    null_count = sum(1 for v in values if v is None or (isinstance(v, str) and not v.strip()))
    non_null = [v for v in values if v is not None and (not isinstance(v, str) or v.strip())]

    unique_values = set(non_null)
    unique_count = len(unique_values)

    fill_rate = (total_count - null_count) / total_count if total_count > 0 else 0.0

    if non_null:
        type_counts = Counter(infer_type(v) for v in non_null[:1000])
        primary_type = type_counts.most_common(1)[0][0]
    else:
        primary_type = DataType.UNKNOWN

    numeric_values = [v for v in non_null if isinstance(v, (int, float)) and not isinstance(v, bool)]
    string_values = [v for v in non_null if isinstance(v, str)]

    numeric_stats = compute_numeric_stats(numeric_values) if numeric_values else None
    string_stats = compute_string_stats(string_values) if string_values else None

    value_counts = Counter(non_null)
    top_values = value_counts.most_common(10)

    examples = list(set(non_null))[:5]

    return FieldProfile(
        name=name,
        data_type=primary_type,
        total_count=total_count,
        null_count=null_count,
        unique_count=unique_count,
        fill_rate=fill_rate,
        numeric_stats=numeric_stats,
        string_stats=string_stats,
        top_values=top_values,
        examples=examples,
    )


def profile_dataset(name: str, data: list[dict[str, Any]]) -> DatasetProfile:
    """Profile an entire dataset.

    Args:
        name: Dataset name
        data: List of records (dicts)

    Returns:
        DatasetProfile
    """
    if not data:
        return DatasetProfile(
            name=name,
            row_count=0,
            field_count=0,
            fields=[],
        )

    row_count = len(data)
    field_names = set()
    for record in data:
        field_names.update(record.keys())

    field_count = len(field_names)

    fields = []
    for field_name in sorted(field_names):
        values = [record.get(field_name) for record in data]
        field_profile = profile_field(field_name, values)
        fields.append(field_profile)

    quality_score = sum(f.fill_rate for f in fields) / len(fields) if fields else 0.0

    issues = []
    for field_profile in fields:
        if field_profile.fill_rate < 0.5:
            issues.append(f"Field '{field_profile.name}' has low fill rate: {field_profile.fill_rate:.2%}")
        if field_profile.unique_count == 1 and field_profile.total_count > 10:
            issues.append(f"Field '{field_profile.name}' has only one unique value")
        if field_profile.null_count > field_profile.total_count * 0.5:
            issues.append(f"Field '{field_profile.name}' is mostly null")

    return DatasetProfile(
        name=name,
        row_count=row_count,
        field_count=field_count,
        fields=fields,
        quality_score=quality_score,
        issues=issues,
    )


def compare_profiles(
    before: DatasetProfile,
    after: DatasetProfile,
) -> dict[str, Any]:
    """Compare two dataset profiles.

    Args:
        before: Earlier profile
        after: Later profile

    Returns:
        Comparison report
    """
    changes = {
        "row_count_change": after.row_count - before.row_count,
        "field_count_change": after.field_count - before.field_count,
        "quality_change": after.quality_score - before.quality_score,
        "field_changes": [],
    }

    before_fields = {f.name: f for f in before.fields}
    after_fields = {f.name: f for f in after.fields}

    all_field_names = set(before_fields.keys()) | set(after_fields.keys())

    for field_name in all_field_names:
        bf = before_fields.get(field_name)
        af = after_fields.get(field_name)

        if bf and af:
            change = {
                "name": field_name,
                "type_changed": bf.data_type != af.data_type,
                "fill_rate_change": af.fill_rate - bf.fill_rate,
                "null_count_change": af.null_count - bf.null_count,
                "unique_count_change": af.unique_count - bf.unique_count,
            }
            changes["field_changes"].append(change)
        elif af and not bf:
            changes["field_changes"].append({
                "name": field_name,
                "status": "added",
            })
        else:
            changes["field_changes"].append({
                "name": field_name,
                "status": "removed",
            })

    return changes

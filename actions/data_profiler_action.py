"""
Data Profiling and Quality Assessment Module.

Profiles datasets to understand schema, statistics, distributions,
null patterns, and quality issues. Generates detailed data reports.

Author: AutoGen
"""
from __future__ import annotations

import json
import logging
import math
import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
)

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Inferred data types."""
    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    BOOLEAN = auto()
    DATETIME = auto()
    DATE = auto()
    TIME = auto()
    UUID = auto()
    EMAIL = auto()
    URL = auto()
    IP_ADDRESS = auto()
    PHONE = auto()
    JSON = auto()
    LIST = auto()
    DICT = auto()
    NULL = auto()
    MIXED = auto()


@dataclass
class FieldProfile:
    """Statistical profile for a single field/column."""
    name: str
    data_type: DataType
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    empty_count: int = 0

    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None

    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None

    top_values: List[Tuple[Any, int]] = field(default_factory=list)
    histogram: Dict[str, int] = field(default_factory=dict)

    patterns: List[str] = field(default_factory=list)
    is_unique: bool = False
    is_nullable: bool = True
    is_sparse: bool = False
    is_imbalanced: bool = False
    imbalance_ratio: float = 0.0

    completeness: float = 1.0
    validity_score: float = 1.0

    def null_pct(self) -> float:
        if self.total_count == 0:
            return 100.0
        return (self.null_count / self.total_count) * 100

    def unique_pct(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.unique_count / self.total_count) * 100


@dataclass
class DatasetProfile:
    """Complete profile for an entire dataset."""
    name: str
    row_count: int = 0
    field_count: int = 0
    fields: List[FieldProfile] = field(default_factory=list)

    quality_score: float = 1.0
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    profiled_at: datetime = field(default_factory=datetime.utcnow)
    sample_size: int = 0
    is_streaming: bool = False


class TypeInferrer:
    """Infers data types from string values."""

    EMAIL_RE = re.compile(r"^[\w\.\+\-]+@[\w\.\-]+\.\w+$")
    URL_RE = re.compile(r"^https?://[\w\.\-]+", re.IGNORECASE)
    UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )
    IP_RE = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
    DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    TIME_RE = re.compile(r"^\d{2}:\d{2}(:\d{2})?(\.\d+)?$")
    DATETIME_RE = re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
    )

    def infer(self, value: Any) -> DataType:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return DataType.NULL

        if isinstance(value, bool):
            return DataType.BOOLEAN

        if isinstance(value, int):
            return DataType.INTEGER

        if isinstance(value, float):
            return DataType.FLOAT

        if isinstance(value, (dict, list)):
            return DataType.DICT if isinstance(value, dict) else DataType.LIST

        if isinstance(value, str):
            s = value.strip()

            if s.lower() in ("true", "false", "yes", "no", "1", "0"):
                return DataType.BOOLEAN

            if self.EMAIL_RE.match(s):
                return DataType.EMAIL
            if self.URL_RE.match(s):
                return DataType.URL
            if self.UUID_RE.match(s):
                return DataType.UUID
            if self.IP_RE.match(s):
                return DataType.IP_ADDRESS

            if self.DATETIME_RE.match(s):
                try:
                    datetime.fromisoformat(s.replace(" ", "T"))
                    return DataType.DATETIME
                except ValueError:
                    pass
            if self.DATE_RE.match(s):
                return DataType.DATE
            if self.TIME_RE.match(s):
                return DataType.TIME

            try:
                int(s)
                return DataType.INTEGER
            except ValueError:
                pass

            try:
                float(s)
                return DataType.FLOAT
            except ValueError:
                pass

            try:
                json.loads(s)
                return DataType.JSON
            except (json.JSONDecodeError, TypeError):
                pass

            return DataType.STRING

        return DataType.MIXED


class DataProfiler:
    """Profiles datasets to generate statistical summaries."""

    def __init__(
        self,
        sample_size: int = 10000,
        histogram_bins: int = 20,
        top_k: int = 10,
    ):
        self.sample_size = sample_size
        self.histogram_bins = histogram_bins
        self.top_k = top_k
        self.type_inferrer = TypeInferrer()

    def profile_dataframe(
        self, df: Any, name: str = "dataset"
    ) -> DatasetProfile:
        """
        Profile a pandas DataFrame or similar tabular data structure.
        Accepts dict of lists, list of dicts, or DataFrame.
        """
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            records = df.to_dict(orient="records")
            columns = list(df.columns)
        elif isinstance(df, dict):
            records = [dict(zip(df.keys(), vals)) for vals in zip(*df.values())]
            columns = list(df.keys())
        elif isinstance(df, list):
            if not df:
                return self._empty_profile(name)
            records = df
            columns = list(df[0].keys()) if isinstance(df[0], dict) else []
        else:
            raise ValueError(f"Unsupported data type: {type(df)}")

        row_count = len(records)
        sample_size = min(self.sample_size, row_count)

        if row_count > self.sample_size:
            records = random.sample(records, sample_size)

        fields: List[FieldProfile] = []
        all_issues: List[str] = []
        all_warnings: List[str] = []

        for col in columns:
            values = [r.get(col) for r in records if col in r]
            field_profile = self._profile_column(col, values)
            fields.append(field_profile)

            if field_profile.null_pct() > 50:
                all_issues.append(
                    f"Field '{col}' has {field_profile.null_pct():.1f}% null values"
                )
            if field_profile.is_imbalanced:
                all_warnings.append(
                    f"Field '{col}' is highly imbalanced (ratio: {field_profile.imbalance_ratio:.1f})"
                )
            if field_profile.is_unique and row_count > 10000:
                all_warnings.append(f"Field '{col}' is unique - may not be informative")

        quality_score = self._compute_quality_score(fields, all_issues)

        return DatasetProfile(
            name=name,
            row_count=row_count,
            field_count=len(columns),
            fields=fields,
            quality_score=quality_score,
            issues=all_issues,
            warnings=all_warnings,
            sample_size=sample_size,
        )

    def _profile_column(self, name: str, values: List[Any]) -> FieldProfile:
        non_null = [v for v in values if v is not None and str(v).strip() != ""]
        empty = [v for v in values if v is None or str(v).strip() == ""]

        inferred_types = [self.type_inferrer.infer(v) for v in non_null[:1000]]
        type_counts = Counter(inferred_types)
        primary_type = type_counts.most_common(1)[0][0] if type_counts else DataType.STRING

        unique_values = set(non_null)
        unique_count = len(unique_values)

        profile = FieldProfile(
            name=name,
            data_type=primary_type,
            total_count=len(values),
            null_count=len(empty),
            unique_count=unique_count,
            empty_count=len(empty),
            is_unique=unique_count == len(non_null) and len(non_null) > 0,
            is_nullable=len(empty) > 0,
            is_sparse=len(empty) / len(values) > 0.3 if values else False,
        )

        numeric_values: List[float] = []
        str_values: List[str] = []

        for v in non_null:
            if isinstance(v, (int, float)):
                numeric_values.append(float(v))
            elif isinstance(v, str):
                str_values.append(v)
                try:
                    numeric_values.append(float(v))
                except ValueError:
                    pass

        if numeric_values:
            profile.min_value = min(numeric_values)
            profile.max_value = max(numeric_values)
            profile.mean_value = sum(numeric_values) / len(numeric_values)

            sorted_vals = sorted(numeric_values)
            mid = len(sorted_vals) // 2
            if len(sorted_vals) % 2 == 0:
                profile.median_value = (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
            else:
                profile.median_value = sorted_vals[mid]

            if len(numeric_values) > 1:
                variance = sum((x - profile.mean_value) ** 2 for x in numeric_values) / len(numeric_values)
                profile.std_dev = math.sqrt(variance)

            self._compute_histogram(profile, numeric_values)

        if str_values:
            profile.min_length = min(len(s) for s in str_values)
            profile.max_length = max(len(s) for s in str_values)
            profile.avg_length = sum(len(s) for s in str_values) / len(str_values)

            self._detect_patterns(profile, str_values)

        if non_null:
            value_counts = Counter(str(v) for v in non_null)
            profile.top_values = value_counts.most_common(self.top_k)

            if value_counts:
                most_common_count = value_counts.most_common(1)[0][1]
                least_common_count = value_counts.most_common()[-1][1]
                if least_common_count > 0:
                    profile.imbalance_ratio = most_common_count / least_common_count
                    profile.is_imbalanced = profile.imbalance_ratio > 20

        profile.completeness = 1 - (profile.null_count / profile.total_count) if profile.total_count > 0 else 0
        profile.validity_score = self._compute_validity(profile)

        return profile

    def _compute_histogram(self, profile: FieldProfile, values: List[float]) -> None:
        if not values or profile.min_value is None or profile.max_value is None:
            return

        if profile.min_value == profile.max_value:
            profile.histogram = {"single_value": len(values)}
            return

        bin_width = (profile.max_value - profile.min_value) / self.histogram_bins
        bins: Dict[str, int] = {f"{profile.min_value:.2f}-{profile.max_value:.2f}": 0}

        for v in values:
            if v == profile.max_value:
                bin_idx = self.histogram_bins - 1
            else:
                bin_idx = int((v - profile.min_value) / bin_width)
                bin_idx = min(bin_idx, self.histogram_bins - 1)

            low = profile.min_value + bin_idx * bin_width
            high = low + bin_width
            key = f"{low:.1f}-{high:.1f}"
            bins[key] = bins.get(key, 0) + 1

        profile.histogram = dict(list(bins.items())[: self.histogram_bins])

    def _detect_patterns(self, profile: FieldProfile, values: List[str]) -> None:
        patterns_found: Set[str] = set()

        for v in values[:500]:
            if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
                patterns_found.add("date_iso")
            elif re.match(r"^\d+$", v):
                patterns_found.add("digits_only")
            elif re.match(r"^[A-Z]{2,}$", v):
                patterns_found.add("uppercase_letters")
            elif re.match(r"^[a-z]+$", v):
                patterns_found.add("lowercase_letters")
            elif re.match(r"^[A-Za-z0-9]+$", v):
                patterns_found.add("alphanumeric")
            elif re.match(r"^[\w\.]+@[\w\.]+\.\w+$", v):
                patterns_found.add("email")
            elif re.match(r"^0x[0-9a-f]+$", v, re.IGNORECASE):
                patterns_found.add("hexadecimal")

        profile.patterns = sorted(patterns_found)

    def _compute_validity(self, profile: FieldProfile) -> float:
        score = 1.0

        if profile.data_type == DataType.MIXED:
            score *= 0.5
        if profile.null_count / profile.total_count > 0.1 if profile.total_count > 0 else False:
            score *= 0.9
        if profile.patterns and profile.unique_count > 100:
            if len(profile.patterns) == 1 and profile.patterns[0] in ("date_iso", "email", "url"):
                score *= 1.0
            elif len(profile.patterns) < 3:
                score *= 0.9

        return max(0.0, min(1.0, score))

    def _compute_quality_score(
        self, fields: List[FieldProfile], issues: List[str]
    ) -> float:
        if not fields:
            return 0.0

        avg_completeness = sum(f.completeness for f in fields) / len(fields)
        avg_validity = sum(f.validity_score for f in fields) / len(fields)

        issue_penalty = min(len(issues) * 0.05, 0.5)

        quality = (avg_completeness * 0.4 + avg_validity * 0.4 + 0.2) - issue_penalty
        return max(0.0, min(1.0, quality))

    def _empty_profile(self, name: str) -> DatasetProfile:
        return DatasetProfile(
            name=name,
            quality_score=0.0,
            issues=[f"Dataset '{name}' is empty"],
        )

    def profile_to_json(self, profile: DatasetProfile) -> str:
        """Serialize a dataset profile to JSON."""

        def sanitize(obj: Any) -> Any:
            if isinstance(obj, FieldProfile):
                return {
                    "name": obj.name,
                    "data_type": obj.data_type.name,
                    "total_count": obj.total_count,
                    "null_count": obj.null_count,
                    "null_pct": obj.null_pct(),
                    "unique_count": obj.unique_count,
                    "unique_pct": obj.unique_pct(),
                    "min_value": str(obj.min_value) if obj.min_value is not None else None,
                    "max_value": str(obj.max_value) if obj.max_value is not None else None,
                    "mean_value": obj.mean_value,
                    "median_value": obj.median_value,
                    "std_dev": obj.std_dev,
                    "min_length": obj.min_length,
                    "max_length": obj.max_length,
                    "avg_length": obj.avg_length,
                    "top_values": [[str(k), v] for k, v in obj.top_values],
                    "patterns": obj.patterns,
                    "is_unique": obj.is_unique,
                    "is_sparse": obj.is_sparse,
                    "is_imbalanced": obj.is_imbalanced,
                    "imbalance_ratio": obj.imbalance_ratio,
                    "completeness": obj.completeness,
                    "validity_score": obj.validity_score,
                }
            elif isinstance(obj, DatasetProfile):
                return {
                    "name": obj.name,
                    "row_count": obj.row_count,
                    "field_count": obj.field_count,
                    "fields": [sanitize(f) for f in obj.fields],
                    "quality_score": obj.quality_score,
                    "issues": obj.issues,
                    "warnings": obj.warnings,
                    "profiled_at": obj.profiled_at.isoformat(),
                    "sample_size": obj.sample_size,
                }
            return obj

        return json.dumps(sanitize(profile), indent=2, default=str)

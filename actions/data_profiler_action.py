"""Data Profiler Action Module.

Profiles data sets with:
- Statistical analysis
- Schema inference
- Data quality assessment
- Pattern detection
- Distribution analysis

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Inferred data types."""
    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    BOOLEAN = auto()
    DATETIME = auto()
    NULL = auto()
    MIXED = auto()
    UNKNOWN = auto()


@dataclass
class FieldProfile:
    """Profile for a single field."""
    name: str
    data_type: DataType
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[Any] = None
    
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    
    patterns: List[Tuple[str, int]] = field(default_factory=list)
    top_values: List[Tuple[Any, int]] = field(default_factory=list)
    
    numeric_stats: Optional[Dict[str, float]] = None
    datetime_range: Optional[Tuple[str, str]] = None


@dataclass
class DatasetProfile:
    """Complete dataset profile."""
    dataset_name: str
    total_records: int = 0
    total_fields: int = 0
    field_profiles: Dict[str, FieldProfile] = field(default_factory=dict)
    data_types: Dict[str, int] = field(default_factory=dict)
    quality_score: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataProfiler:
    """Profiles datasets to understand structure and content.
    
    Features:
    - Automatic type inference
    - Statistical analysis
    - Pattern detection
    - Data quality scoring
    - Schema extraction
    """
    
    def __init__(self, dataset_name: str = "default"):
        self.dataset_name = dataset_name
        self._patterns_cache: Dict[str, List[str]] = {}
    
    async def profile(
        self,
        records: List[Dict[str, Any]],
        sample_size: Optional[int] = None,
        infer_types: bool = True
    ) -> DatasetProfile:
        """Profile a dataset.
        
        Args:
            records: List of records to profile
            sample_size: Optional sample size for large datasets
            infer_types: Whether to infer data types
            
        Returns:
            Dataset profile
        """
        if not records:
            return DatasetProfile(dataset_name=self.dataset_name)
        
        if sample_size and len(records) > sample_size:
            import random
            records = random.sample(records, sample_size)
        
        total_records = len(records)
        
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        field_profiles: Dict[str, FieldProfile] = {}
        data_types: Dict[str, int] = defaultdict(int)
        
        for field_name in all_fields:
            values = [record.get(field_name) for record in records]
            profile = await self._profile_field(field_name, values, infer_types)
            field_profiles[field_name] = profile
            data_types[profile.data_type.name] += 1
        
        quality_score = self._calculate_quality_score(field_profiles, total_records)
        
        return DatasetProfile(
            dataset_name=self.dataset_name,
            total_records=total_records,
            total_fields=len(all_fields),
            field_profiles=field_profiles,
            data_types=dict(data_types),
            quality_score=quality_score
        )
    
    async def _profile_field(
        self,
        field_name: str,
        values: List[Any],
        infer_types: bool
    ) -> FieldProfile:
        """Profile a single field.
        
        Args:
            field_name: Name of the field
            values: List of values
            infer_types: Whether to infer type
            
        Returns:
            Field profile
        """
        non_null_values = [v for v in values if v is not None]
        null_count = len(values) - len(non_null_values)
        
        data_type = DataType.UNKNOWN
        if infer_types and non_null_values:
            data_type = self._infer_type(non_null_values)
        
        profile = FieldProfile(
            name=field_name,
            data_type=data_type,
            total_count=len(values),
            null_count=null_count,
            unique_count=len(set(str(v) for v in non_null_values))
        )
        
        if non_null_values:
            if data_type in (DataType.INTEGER, DataType.FLOAT):
                numeric_stats = self._calculate_numeric_stats(non_null_values)
                profile.numeric_stats = numeric_stats
                profile.min_value = numeric_stats.get("min")
                profile.max_value = numeric_stats.get("max")
                profile.mean_value = numeric_stats.get("mean")
                profile.median_value = numeric_stats.get("median")
            
            if data_type == DataType.STRING:
                lengths = [len(str(v)) for v in non_null_values]
                profile.min_length = min(lengths)
                profile.max_length = max(lengths)
                profile.avg_length = sum(lengths) / len(lengths)
            
            profile.top_values = Counter(non_null_values).most_common(10)
            
            if data_type == DataType.STRING:
                profile.patterns = self._detect_patterns(non_null_values)
            
            if data_type == DataType.DATETIME:
                try:
                    dates = [datetime.fromisoformat(str(v)) for v in non_null_values]
                    dates.sort(key=lambda d: d.timestamp())
                    profile.datetime_range = (
                        dates[0].isoformat(),
                        dates[-1].isoformat()
                    )
                except Exception:
                    pass
        
        return profile
    
    def _infer_type(self, values: List[Any]) -> DataType:
        """Infer data type from values."""
        if not values:
            return DataType.NULL
        
        type_counts = Counter()
        
        for value in values:
            if value is None:
                type_counts[DataType.NULL] += 1
            elif isinstance(value, bool):
                type_counts[DataType.BOOLEAN] += 1
            elif isinstance(value, int):
                type_counts[DataType.INTEGER] += 1
            elif isinstance(value, float):
                type_counts[DataType.FLOAT] += 1
            elif isinstance(value, str):
                if self._looks_like_datetime(value):
                    type_counts[DataType.DATETIME] += 1
                elif value.lower() in ("true", "false", "yes", "no"):
                    type_counts[DataType.BOOLEAN] += 1
                elif self._looks_like_number(value):
                    type_counts[DataType.FLOAT if "." in value else DataType.INTEGER] += 1
                else:
                    type_counts[DataType.STRING] += 1
            else:
                type_counts[DataType.UNKNOWN] += 1
        
        if len(type_counts) == 1:
            return list(type_counts.keys())[0]
        
        most_common = type_counts.most_common(1)[0][0]
        
        if most_common == DataType.NULL and len(type_counts) > 1:
            for dtype, count in type_counts.most_common():
                if dtype != DataType.NULL:
                    most_common = dtype
                    break
        
        return most_common
    
    def _looks_like_datetime(self, value: str) -> bool:
        """Check if string looks like a datetime."""
        datetime_patterns = [
            r"\d{4}-\d{2}-\d{2}",
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
            r"\d{2}/\d{2}/\d{4}",
        ]
        return any(re.match(p, str(value)) for p in datetime_patterns)
    
    def _looks_like_number(self, value: str) -> bool:
        """Check if string looks like a number."""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _calculate_numeric_stats(self, values: List[Any]) -> Dict[str, float]:
        """Calculate numeric statistics."""
        numeric_values = []
        for v in values:
            try:
                numeric_values.append(float(v))
            except (ValueError, TypeError):
                continue
        
        if not numeric_values:
            return {}
        
        sorted_values = sorted(numeric_values)
        n = len(sorted_values)
        
        mean = sum(sorted_values) / n
        
        variance = sum((x - mean) ** 2 for x in sorted_values) / n
        std_dev = math.sqrt(variance)
        
        median = sorted_values[n // 2] if n % 2 == 1 else (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = sorted_values[q1_idx]
        q3 = sorted_values[q3_idx]
        
        return {
            "min": min(sorted_values),
            "max": max(sorted_values),
            "mean": mean,
            "median": median,
            "std_dev": std_dev,
            "variance": variance,
            "q1": q1,
            "q3": q3,
            "iqr": q3 - q1
        }
    
    def _detect_patterns(self, values: List[str]) -> List[Tuple[str, int]]:
        """Detect common patterns in string values."""
        patterns = Counter()
        
        for value in values[:1000]:
            pattern = self._string_to_pattern(str(value))
            patterns[pattern] += 1
        
        return patterns.most_common(10)
    
    def _string_to_pattern(self, value: str) -> str:
        """Convert string to pattern representation."""
        pattern = []
        for char in value:
            if char.isdigit():
                pattern.append("d")
            elif char.isalpha():
                pattern.append("a")
            elif char.isspace():
                pattern.append("s")
            else:
                pattern.append(char)
        return "".join(pattern)
    
    def _calculate_quality_score(
        self,
        field_profiles: Dict[str, FieldProfile],
        total_records: int
    ) -> float:
        """Calculate overall data quality score."""
        if not field_profiles:
            return 0.0
        
        scores = []
        
        for field_name, profile in field_profiles.items():
            completeness = (total_records - profile.null_count) / total_records
            
            uniqueness = profile.unique_count / total_records if total_records > 0 else 0
            
            field_score = (completeness * 0.6) + (uniqueness * 0.4)
            scores.append(field_score)
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def compare_profiles(
        self,
        profile1: DatasetProfile,
        profile2: DatasetProfile
    ) -> Dict[str, Any]:
        """Compare two dataset profiles.
        
        Args:
            profile1: First profile
            profile2: Second profile
            
        Returns:
            Comparison results
        """
        comparison = {
            "record_count_diff": profile2.total_records - profile1.total_records,
            "field_differences": {},
            "quality_change": profile2.quality_score - profile1.quality_score,
            "new_fields": [],
            "removed_fields": []
        }
        
        fields1 = set(profile1.field_profiles.keys())
        fields2 = set(profile2.field_profiles.keys())
        
        comparison["new_fields"] = list(fields2 - fields1)
        comparison["removed_fields"] = list(fields1 - fields2)
        
        common_fields = fields1 & fields2
        
        for field_name in common_fields:
            p1 = profile1.field_profiles[field_name]
            p2 = profile2.field_profiles[field_name]
            
            comparison["field_differences"][field_name] = {
                "type_change": p1.data_type != p2.data_type,
                "null_count_diff": p2.null_count - p1.null_count,
                "unique_count_diff": p2.unique_count - p1.unique_count
            }
        
        return comparison

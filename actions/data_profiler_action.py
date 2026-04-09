"""
Data Profiler Action Module

Statistical profiling of datasets with type inference,
distribution analysis, and anomaly detection.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class FieldType(Enum):
    """Inferred field types."""
    
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    LIST = "list"
    DICT = "dict"
    NULL = "null"
    UNKNOWN = "unknown"


@dataclass
class FieldProfile:
    """Statistical profile of a single field."""
    
    name: str
    field_type: FieldType
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[Any] = None
    
    std_dev: Optional[float] = None
    variance: Optional[float] = None
    
    top_values: List[Tuple[Any, int]] = field(default_factory=list)
    histogram: Dict[str, int] = field(default_factory=dict)
    
    pattern: Optional[str] = None
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class DatasetProfile:
    """Complete profile of a dataset."""
    
    total_records: int
    total_fields: int
    field_profiles: Dict[str, FieldProfile]
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


class TypeInferrer:
    """Infers field types from values."""
    
    DATETIME_PATTERNS = [
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
        r"^\d{10,13}$"
    ]
    
    @classmethod
    def infer(cls, value: Any) -> FieldType:
        """Infer type from a single value."""
        if value is None:
            return FieldType.NULL
        
        if isinstance(value, bool):
            return FieldType.BOOLEAN
        
        if isinstance(value, int):
            return FieldType.INTEGER
        
        if isinstance(value, float):
            return FieldType.FLOAT
        
        if isinstance(value, str):
            return cls._infer_string(value)
        
        if isinstance(value, list):
            return FieldType.LIST
        
        if isinstance(value, dict):
            return FieldType.DICT
        
        return FieldType.UNKNOWN
    
    @classmethod
    def _infer_string(cls, value: str) -> FieldType:
        """Infer type from string value."""
        if value.lower() in ("true", "false"):
            return FieldType.BOOLEAN
        
        if value.lower() in ("null", "none", "nan", ""):
            return FieldType.NULL
        
        if re.match(r"^-?\d+$", value):
            return FieldType.INTEGER
        
        if re.match(r"^-?\d+\.\d+$", value):
            return FieldType.FLOAT
        
        for pattern in cls.DATETIME_PATTERNS:
            if re.match(pattern, value):
                return FieldType.DATETIME
        
        return FieldType.STRING


class StatisticalAnalyzer:
    """Performs statistical analysis on numeric fields."""
    
    @staticmethod
    def compute_stats(values: List[float]) -> Dict[str, float]:
        """Compute basic statistics for numeric values."""
        if not values:
            return {}
        
        n = len(values)
        mean = sum(values) / n
        
        variance = sum((x - mean) ** 2 for x in values) / n
        std_dev = math.sqrt(variance)
        
        sorted_values = sorted(values)
        median = sorted_values[n // 2] if n % 2 == 1 else (
            sorted_values[n // 2 - 1] + sorted_values[n // 2]
        ) / 2
        
        return {
            "mean": mean,
            "median": median,
            "std_dev": std_dev,
            "variance": variance,
            "min": min(values),
            "max": max(values)
        }


class DataProfiler:
    """Profiles datasets to extract statistical information."""
    
    def __init__(self):
        self._type_inferrer = TypeInferrer
        self._stats_analyzer = StatisticalAnalyzer
    
    def profile_field(self, name: str, values: List[Any]) -> FieldProfile:
        """Profile a single field."""
        non_null = [v for v in values if v is not None]
        
        field_type = self._type_inferrer.infer(values[0]) if values else FieldType.UNKNOWN
        
        profile = FieldProfile(
            name=name,
            field_type=field_type,
            total_count=len(values),
            null_count=len(values) - len(non_null),
            unique_count=len(set(str(v) for v in non_null)) if non_null else 0,
            sample_values=list(non_null[:5])
        )
        
        if non_null:
            value_counter = Counter(non_null)
            profile.top_values = value_counter.most_common(10)
        
        if field_type in (FieldType.INTEGER, FieldType.FLOAT):
            try:
                numeric_values = [float(v) for v in non_null if v is not None]
                stats = self._stats_analyzer.compute_stats(numeric_values)
                
                profile.mean_value = stats.get("mean")
                profile.median_value = stats.get("median")
                profile.std_dev = stats.get("std_dev")
                profile.variance = stats.get("variance")
                profile.min_value = stats.get("min")
                profile.max_value = stats.get("max")
                
                profile.histogram = self._compute_histogram(numeric_values)
            
            except Exception as e:
                logger.warning(f"Could not compute numeric stats for {name}: {e}")
        
        if field_type == FieldType.STRING:
            profile.pattern = self._detect_pattern(non_null)
            profile.histogram = self._compute_value_histogram(non_null)
        
        return profile
    
    def _compute_histogram(self, values: List[float], bins: int = 10) -> Dict[str, int]:
        """Compute histogram bins for numeric values."""
        if not values:
            return {}
        
        min_val = min(values)
        max_val = max(values)
        
        if min_val == max_val:
            return {"all_same": len(values)}
        
        bin_width = (max_val - min_val) / bins
        histogram = {}
        
        for value in values:
            if value == max_val:
                bin_key = f"{max_val - bin_width:.2f}-{max_val:.2f}"
            else:
                bin_idx = int((value - min_val) / bin_width)
                bin_start = min_val + bin_idx * bin_width
                bin_end = bin_start + bin_width
                bin_key = f"{bin_start:.2f}-{bin_end:.2f}"
            
            histogram[bin_key] = histogram.get(bin_key, 0) + 1
        
        return histogram
    
    def _compute_value_histogram(self, values: List[str], max_bins: int = 20) -> Dict[str, int]:
        """Compute histogram for categorical values."""
        counter = Counter(values)
        return dict(counter.most_common(max_bins))
    
    def _detect_pattern(self, values: List[str]) -> Optional[str]:
        """Detect common patterns in string values."""
        patterns_found = defaultdict(int)
        
        for value in values[:100]:
            if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
                patterns_found["date_iso"] += 1
            elif re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", value):
                patterns_found["ipv4"] += 1
            elif re.match(r"^[\w.-]+@[\w.-]+\.\w+$", value):
                patterns_found["email"] += 1
            elif re.match(r"^https?://", value):
                patterns_found["url"] += 1
            elif re.match(r"^0x[a-fA-F0-9]+$", value):
                patterns_found["hex"] += 1
            elif re.match(r"^\d+$", value):
                patterns_found["numeric_string"] += 1
        
        if patterns_found:
            return max(patterns_found, key=patterns_found.get)
        
        return None
    
    def profile_dataset(self, records: List[Dict]) -> DatasetProfile:
        """Profile an entire dataset."""
        if not records:
            return DatasetProfile(
                total_records=0,
                total_fields=0,
                field_profiles={}
            )
        
        all_fields: Set[str] = set()
        for record in records:
            all_fields.update(record.keys())
        
        field_values: Dict[str, List[Any]] = {field: [] for field in all_fields}
        
        for record in records:
            for field in all_fields:
                field_values[field].append(record.get(field))
        
        field_profiles = {}
        for field, values in field_values.items():
            field_profiles[field] = self.profile_field(field, values)
        
        return DatasetProfile(
            total_records=len(records),
            total_fields=len(all_fields),
            field_profiles=field_profiles
        )


class DataProfilerAction:
    """
    Main data profiler action handler.
    
    Provides statistical profiling of datasets with type inference,
    distribution analysis, and quality metrics.
    """
    
    def __init__(self):
        self.profiler = DataProfiler()
        self._profiles: Dict[str, DatasetProfile] = {}
    
    def profile(
        self,
        records: List[Dict],
        profile_id: Optional[str] = None
    ) -> DatasetProfile:
        """Profile a dataset."""
        profile = self.profiler.profile_dataset(records)
        
        if profile_id:
            self._profiles[profile_id] = profile
        
        return profile
    
    def compare_profiles(
        self,
        profile1: DatasetProfile,
        profile2: DatasetProfile
    ) -> Dict[str, Any]:
        """Compare two dataset profiles."""
        comparison = {
            "record_count_change": profile2.total_records - profile1.total_records,
            "field_changes": {},
            "type_changes": [],
            "value_changes": {}
        }
        
        all_fields = set(profile1.field_profiles.keys()) | set(profile2.field_profiles.keys())
        
        for field in all_fields:
            p1 = profile1.field_profiles.get(field)
            p2 = profile2.field_profiles.get(field)
            
            if p1 and p2:
                if p1.field_type != p2.field_type:
                    comparison["type_changes"].append({
                        "field": field,
                        "from": p1.field_type.value,
                        "to": p2.field_type.value
                    })
                
                if p1.null_count != p2.null_count:
                    comparison["field_changes"][field] = {
                        "null_count_change": p2.null_count - p1.null_count
                    }
            elif p1:
                comparison["field_changes"][field] = {"status": "removed"}
            elif p2:
                comparison["field_changes"][field] = {"status": "added"}
        
        return comparison
    
    def get_profile(self, profile_id: str) -> Optional[DatasetProfile]:
        """Get a stored profile by ID."""
        return self._profiles.get(profile_id)
    
    def list_profiles(self) -> List[str]:
        """List all stored profile IDs."""
        return list(self._profiles.keys())
    
    def detect_anomalies(
        self,
        profile: DatasetProfile,
        threshold: float = 3.0
    ) -> List[Dict[str, Any]]:
        """Detect anomalies in the profile based on statistical measures."""
        anomalies = []
        
        for field_name, field_profile in profile.field_profiles.items():
            if field_profile.field_type not in (FieldType.INTEGER, FieldType.FLOAT):
                continue
            
            if field_profile.std_dev and field_profile.mean_value:
                if field_profile.std_dev > threshold * abs(field_profile.mean_value):
                    anomalies.append({
                        "field": field_name,
                        "type": "high_variance",
                        "std_dev": field_profile.std_dev,
                        "mean": field_profile.mean_value
                    })
            
            if field_profile.null_count / max(field_profile.total_count, 1) > 0.5:
                anomalies.append({
                    "field": field_name,
                    "type": "high_null_rate",
                    "null_count": field_profile.null_count,
                    "total_count": field_profile.total_count
                })
        
        return anomalies
    
    def generate_report(self, profile: DatasetProfile) -> str:
        """Generate a human-readable report from a profile."""
        lines = [
            f"Dataset Profile",
            f"=" * 50,
            f"Total Records: {profile.total_records}",
            f"Total Fields: {profile.total_fields}",
            f"Generated: {profile.created_at}",
            f"",
            f"Field Profiles",
            f"-" * 50
        ]
        
        for field_name, fp in profile.field_profiles.items():
            lines.append(f"\n{field_name} ({fp.field_type.value}):")
            lines.append(f"  Count: {fp.total_count}")
            lines.append(f"  Null: {fp.null_count} ({fp.null_count / max(fp.total_count, 1) * 100:.1f}%)")
            lines.append(f"  Unique: {fp.unique_count}")
            
            if fp.field_type in (FieldType.INTEGER, FieldType.FLOAT):
                lines.append(f"  Min: {fp.min_value}")
                lines.append(f"  Max: {fp.max_value}")
                lines.append(f"  Mean: {fp.mean_value:.2f}")
                lines.append(f"  Median: {fp.median_value}")
                lines.append(f"  Std Dev: {fp.std_dev:.2f}")
        
        return "\n".join(lines)

"""
Data Profiling and Quality Assessment Module.

Provides comprehensive data analysis including statistical summaries,
distribution analysis, quality scoring, and anomaly detection for
dataset assessment and monitoring.
"""

from typing import (
    Dict, List, Optional, Any, Tuple, Set, Callable,
    Union, Sequence, TypeVar
)
from dataclasses import dataclass, field
from enum import Enum, auto
import statistics
import math
from collections import Counter, defaultdict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T", int, float, str)


class DataType(Enum):
    """Detected data types."""
    INTEGER = auto()
    FLOAT = auto()
    STRING = auto()
    BOOLEAN = auto()
    DATETIME = auto()
    NULL = auto()
    MIXED = auto()
    UNKNOWN = auto()


@dataclass
class ColumnProfile:
    """Statistical profile for a single column."""
    name: str
    data_type: DataType
    total_count: int
    null_count: int
    unique_count: int
    null_ratio: float
    unique_ratio: float
    
    # Numeric stats
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    q1: Optional[float] = None
    q3: Optional[float] = None
    iqr: Optional[float] = None
    
    # String stats
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    top_values: List[Tuple[Any, int]] = field(default_factory=list)
    
    # Distribution
    histogram: Dict[str, int] = field(default_factory=dict)
    is_skewed: bool = False
    has_outliers: bool = False
    outlier_count: int = 0


@dataclass
class DataQualityScore:
    """Overall data quality score."""
    completeness: float  # 0-1 based on null ratio
    consistency: float  # 0-1 based on type consistency
    validity: float  # 0-1 based on value ranges
    overall: float  # Weighted average
    issues: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "completeness": round(self.completeness, 3),
            "consistency": round(self.consistency, 3),
            "validity": round(self.validity, 3),
            "overall": round(self.overall, 3),
            "issues": self.issues
        }


class DataProfiler:
    """
    Comprehensive data profiling and quality assessment.
    
    Analyzes datasets to provide statistical summaries,
    distribution metrics, and quality scores.
    """
    
    def __init__(self) -> None:
        self.column_profiles: Dict[str, ColumnProfile] = {}
        self.quality_score: Optional[DataQualityScore] = None
        self._null_values: Set[Any] = {None, "", "NA", "N/A", "null", "NULL", "None", "nan", "NaN"}
    
    def profile_dataframe(
        self,
        data: List[Dict[str, Any]],
        sample_size: Optional[int] = None
    ) -> Dict[str, ColumnProfile]:
        """
        Profile a list of records (DataFrame-like).
        
        Args:
            data: List of dictionaries representing rows
            sample_size: Optional sample size for large datasets
            
        Returns:
            Dictionary mapping column names to their profiles
        """
        if not data:
            return {}
        
        if sample_size and len(data) > sample_size:
            import random
            data = random.sample(data, sample_size)
        
        # Get all column names
        columns = set()
        for row in data:
            columns.update(row.keys())
        
        self.column_profiles = {}
        
        for col in columns:
            values = [row.get(col) for row in data if col in row]
            self.column_profiles[col] = self._profile_column(col, values)
        
        self.quality_score = self._calculate_quality_score()
        
        return self.column_profiles
    
    def _profile_column(
        self,
        name: str,
        values: List[Any]
    ) -> ColumnProfile:
        """Profile a single column."""
        total = len(values)
        non_null = [v for v in values if v not in self._null_values]
        
        detected_type = self._detect_type(non_null)
        
        profile = ColumnProfile(
            name=name,
            data_type=detected_type,
            total_count=total,
            null_count=total - len(non_null),
            unique_count=len(set(non_null)),
            null_ratio=(total - len(non_null)) / total if total > 0 else 0,
            unique_ratio=len(set(non_null)) / len(non_null) if non_null else 0
        )
        
        if detected_type in [DataType.INTEGER, DataType.FLOAT]:
            self._profile_numeric(profile, non_null)
        elif detected_type == DataType.STRING:
            self._profile_string(profile, non_null)
        
        self._detect_outliers(profile, non_null)
        self._generate_histogram(profile, non_null)
        
        return profile
    
    def _detect_type(self, values: List[Any]) -> DataType:
        """Detect the data type of values."""
        if not values:
            return DataType.NULL
        
        types_present = set()
        for v in values[:100]:  # Sample for efficiency
            if isinstance(v, bool) or str(v).lower() in ("true", "false"):
                types_present.add(DataType.BOOLEAN)
            elif isinstance(v, int) or (isinstance(v, str) and self._is_integer(v)):
                types_present.add(DataType.INTEGER)
            elif isinstance(v, float) or (isinstance(v, str) and self._is_float(v)):
                types_present.add(DataType.FLOAT)
            elif isinstance(v, str):
                types_present.add(DataType.STRING)
        
        if len(types_present) > 1:
            return DataType.MIXED
        elif DataType.BOOLEAN in types_present:
            return DataType.BOOLEAN
        elif DataType.FLOAT in types_present:
            return DataType.FLOAT
        elif DataType.INTEGER in types_present:
            return DataType.INTEGER
        elif DataType.STRING in types_present:
            return DataType.STRING
        
        return DataType.UNKNOWN
    
    def _is_integer(self, val: str) -> bool:
        try:
            int(val)
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_float(self, val: str) -> bool:
        try:
            float(val)
            return "." in str(val)
        except (ValueError, TypeError):
            return False
    
    def _profile_numeric(
        self,
        profile: ColumnProfile,
        values: List[Any]
    ) -> None:
        """Calculate numeric statistics."""
        numeric_values = []
        for v in values:
            try:
                numeric_values.append(float(v))
            except (ValueError, TypeError):
                continue
        
        if not numeric_values:
            return
        
        numeric_values.sort()
        n = len(numeric_values)
        
        profile.min_value = numeric_values[0]
        profile.max_value = numeric_values[-1]
        profile.mean = statistics.mean(numeric_values)
        profile.median = statistics.median(numeric_values)
        
        if n > 1:
            profile.std_dev = statistics.stdev(numeric_values)
        
        # Quartiles
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        profile.q1 = numeric_values[q1_idx]
        profile.q3 = numeric_values[q3_idx]
        profile.iqr = profile.q3 - profile.q1 if profile.q1 and profile.q3 else None
        
        # Top values
        counter = Counter(numeric_values)
        profile.top_values = counter.most_common(5)
    
    def _profile_string(
        self,
        profile: ColumnProfile,
        values: List[Any]
    ) -> None:
        """Calculate string statistics."""
        str_values = [str(v) for v in values]
        lengths = [len(s) for s in str_values]
        
        if lengths:
            profile.min_length = min(lengths)
            profile.max_length = max(lengths)
            profile.avg_length = statistics.mean(lengths)
        
        # Top values
        counter = Counter(str_values)
        profile.top_values = counter.most_common(5)
    
    def _detect_outliers(
        self,
        profile: ColumnProfile,
        values: List[Any]
    ) -> None:
        """Detect outliers using IQR method."""
        if profile.iqr is None or profile.q1 is None or profile.q3 is None:
            return
        
        lower_bound = profile.q1 - 1.5 * profile.iqr
        upper_bound = profile.q3 + 1.5 * profile.iqr
        
        numeric_values = []
        for v in values:
            try:
                numeric_values.append(float(v))
            except (ValueError, TypeError):
                continue
        
        outliers = [
            v for v in numeric_values
            if v < lower_bound or v > upper_bound
        ]
        
        profile.has_outliers = len(outliers) > 0
        profile.outlier_count = len(outliers)
    
    def _generate_histogram(
        self,
        profile: ColumnProfile,
        values: List[Any]
    ) -> None:
        """Generate histogram bins."""
        if profile.data_type not in [DataType.INTEGER, DataType.FLOAT]:
            counter = Counter(str(v) for v in values[:50])
            profile.histogram = dict(counter.most_common(10))
            return
        
        numeric_values = []
        for v in values:
            try:
                numeric_values.append(float(v))
            except (ValueError, TypeError):
                continue
        
        if not numeric_values:
            return
        
        min_val = min(numeric_values)
        max_val = max(numeric_values)
        
        if min_val == max_val:
            profile.histogram = {"all_same": len(numeric_values)}
            return
        
        num_bins = min(10, len(set(numeric_values)))
        bin_width = (max_val - min_val) / num_bins
        
        bins = [min_val + i * bin_width for i in range(num_bins + 1)]
        
        for i in range(num_bins):
            lower = bins[i]
            upper = bins[i + 1]
            count = sum(
                1 for v in numeric_values
                if lower <= v < upper or (i == num_bins - 1 and v == upper)
            )
            label = f"{lower:.1f}-{upper:.1f}"
            profile.histogram[label] = count
    
    def _calculate_quality_score(self) -> DataQualityScore:
        """Calculate overall data quality score."""
        if not self.column_profiles:
            return DataQualityScore(0, 0, 0, 0, ["No data profiled"])
        
        issues = []
        
        # Completeness
        null_ratios = [p.null_ratio for p in self.column_profiles.values()]
        completeness = 1 - statistics.mean(null_ratios) if null_ratios else 0
        if completeness < 0.9:
            issues.append("High null ratio in columns")
        
        # Consistency
        mixed_types = sum(
            1 for p in self.column_profiles.values()
            if p.data_type == DataType.MIXED
        )
        consistency = 1 - (mixed_types / len(self.column_profiles))
        if mixed_types > 0:
            issues.append(f"{mixed_types} columns have mixed types")
        
        # Validity
        outlier_cols = sum(
            1 for p in self.column_profiles.values()
            if p.has_outliers
        )
        validity = 1 - (outlier_cols / len(self.column_profiles)) if self.column_profiles else 0
        if outlier_cols > 0:
            issues.append(f"{outlier_cols} columns have outliers")
        
        overall = 0.4 * completeness + 0.3 * consistency + 0.3 * validity
        
        return DataQualityScore(
            completeness=completeness,
            consistency=consistency,
            validity=validity,
            overall=overall,
            issues=issues
        )
    
    def get_summary(self) -> Dict[str, Any]:
        """Get profiling summary."""
        return {
            "total_columns": len(self.column_profiles),
            "quality_score": self.quality_score.to_dict() if self.quality_score else None,
            "column_types": {
                name: profile.data_type.name
                for name, profile in self.column_profiles.items()
            }
        }


# Entry point for direct execution
if __name__ == "__main__":
    import json
    
    sample_data = [
        {"name": "Alice", "age": 30, "score": 85.5},
        {"name": "Bob", "age": 25, "score": 92.3},
        {"name": "Carol", "age": 35, "score": 78.0},
        {"name": "Dave", "age": 28, "score": 88.9},
        {"name": "Eve", "age": None, "score": 95.0},
        {"name": "Frank", "age": 42, "score": 70.5},
        {"name": "Grace", "age": 31, "score": 88.0},
        {"name": "Henry", "age": 29, "score": 82.5},
        {"name": "Iris", "age": 27, "score": 91.0},
        {"name": "Jack", "age": 33, "score": 76.5},
    ]
    
    profiler = DataProfiler()
    profiles = profiler.profile_dataframe(sample_data)
    
    print("Column Profiles:")
    for name, profile in profiles.items():
        print(f"  {name}: type={profile.data_type.name}, "
              f"null_ratio={profile.null_ratio:.1%}, "
              f"unique_ratio={profile.unique_ratio:.1%}")
    
    quality = profiler.quality_score
    print(f"\nQuality Score: {quality.overall:.2%}")
    print(f"  Completeness: {quality.completeness:.2%}")
    print(f"  Consistency: {quality.consistency:.2%}")
    print(f"  Validity: {quality.validity:.2%}")

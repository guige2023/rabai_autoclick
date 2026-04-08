"""
Data Profiler Action.

Provides data profiling and statistics analysis.
Supports:
- Column profiling
- Data type inference
- Distribution analysis
- Correlation analysis
- Pattern detection
"""

from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime
from collections import Counter, defaultdict
import statistics
import logging
import json
import re

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Profile for a single column."""
    name: str
    total_count: int
    null_count: int
    unique_count: int
    inferred_type: str
    completeness: float
    
    # Numeric stats (if applicable)
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    
    # String stats (if applicable)
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    pattern: Optional[str] = None
    
    # Distribution
    value_distribution: Dict[str, int] = field(default_factory=dict)
    histogram: List[Dict[str, Any]] = field(default_factory=list)
    
    # Patterns
    detected_patterns: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "total_count": self.total_count,
            "null_count": self.null_count,
            "unique_count": self.unique_count,
            "inferred_type": self.inferred_type,
            "completeness": self.completeness,
            "min_value": str(self.min_value) if self.min_value is not None else None,
            "max_value": str(self.max_value) if self.max_value is not None else None,
            "mean": self.mean,
            "median": self.median,
            "std_dev": self.std_dev,
            "min_length": self.min_length,
            "max_length": self.max_length,
            "avg_length": self.avg_length,
            "pattern": self.pattern,
            "value_distribution": dict(list(self.value_distribution.items())[:20]),
            "detected_patterns": self.detected_patterns
        }


@dataclass
class DatasetProfile:
    """Complete dataset profile."""
    dataset_name: str
    timestamp: datetime
    row_count: int
    column_count: int
    column_profiles: Dict[str, ColumnProfile]
    correlations: Dict[str, Dict[str, float]] = field(default_factory=dict)
    quality_score: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dataset_name": self.dataset_name,
            "timestamp": self.timestamp.isoformat(),
            "row_count": self.row_count,
            "column_count": self.column_count,
            "quality_score": self.quality_score,
            "column_profiles": {
                name: profile.to_dict()
                for name, profile in self.column_profiles.items()
            },
            "correlations": self.correlations,
            "metadata": self.metadata
        }


class DataProfilerAction:
    """
    Data Profiler Action.
    
    Provides comprehensive data profiling with support for:
    - Column-level statistics
    - Data type inference
    - Distribution analysis
    - Pattern detection
    - Quality scoring
    """
    
    TYPE_PATTERNS = {
        "integer": r"^-?\d+$",
        "float": r"^-?\d+\.\d+$",
        "boolean": r"^(true|false|0|1|yes|no)$",
        "email": r"^[^@]+@[^@]+\.[^@]+$",
        "url": r"^https?://",
        "date": r"^\d{4}-\d{2}-\d{2}$",
        "datetime": r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
        "phone": r"^\+?[\d\s\-\(\)]+$",
        "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    }
    
    def __init__(self, dataset_name: str = "dataset"):
        """
        Initialize the Data Profiler Action.
        
        Args:
            dataset_name: Name of the dataset
        """
        self.dataset_name = dataset_name
    
    def profile_column(self, values: List[Any]) -> ColumnProfile:
        """
        Profile a single column.
        
        Args:
            values: List of values in the column
        
        Returns:
            ColumnProfile with statistics
        """
        name = "column"  # Would be set externally
        total_count = len(values)
        null_count = sum(1 for v in values if v is None or str(v).strip() == "")
        non_null = [v for v in values if v is not None and str(v).strip() != ""]
        unique_values = set(str(v) for v in non_null)
        
        # Infer type
        inferred_type = self._infer_type(non_null)
        
        # Calculate completeness
        completeness = (total_count - null_count) / total_count if total_count > 0 else 0
        
        # Create base profile
        profile = ColumnProfile(
            name=name,
            total_count=total_count,
            null_count=null_count,
            unique_count=len(unique_values),
            inferred_type=inferred_type,
            completeness=completeness
        )
        
        # Calculate type-specific stats
        if inferred_type in ("integer", "float"):
            self._add_numeric_stats(profile, non_null)
        elif inferred_type == "string":
            self._add_string_stats(profile, non_null)
        
        # Value distribution
        if len(non_null) > 0:
            value_counts = Counter(str(v) for v in non_null)
            profile.value_distribution = dict(value_counts.most_common(20))
        
        # Pattern detection
        profile.detected_patterns = self._detect_patterns(non_null)
        
        # Generate histogram for numeric columns
        if inferred_type in ("integer", "float"):
            profile.histogram = self._generate_histogram(non_null)
        
        return profile
    
    def _infer_type(self, values: List[Any]) -> str:
        """Infer the data type of a column."""
        if not values:
            return "unknown"
        
        type_votes = Counter()
        
        for value in values[:100]:  # Sample first 100
            str_value = str(value).strip()
            
            if str_value == "":
                continue
            
            # Check patterns
            for type_name, pattern in self.TYPE_PATTERNS.items():
                if re.match(pattern, str_value, re.IGNORECASE):
                    type_votes[type_name] += 1
            
            # Check numeric
            try:
                float(str_value)
                type_votes["float"] += 1
                if "." not in str_value:
                    type_votes["integer"] += 1
            except ValueError:
                pass
        
        if not type_votes:
            return "string"
        
        return type_votes.most_common(1)[0][0]
    
    def _add_numeric_stats(
        self,
        profile: ColumnProfile,
        values: List[Any]
    ) -> None:
        """Add numeric statistics to profile."""
        try:
            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (ValueError, TypeError):
                    pass
            
            if not numeric_values:
                return
            
            profile.min_value = min(numeric_values)
            profile.max_value = max(numeric_values)
            profile.mean = statistics.mean(numeric_values)
            profile.median = statistics.median(numeric_values)
            
            if len(numeric_values) > 1:
                profile.std_dev = statistics.stdev(numeric_values)
        
        except Exception as e:
            logger.warning(f"Error calculating numeric stats: {e}")
    
    def _add_string_stats(
        self,
        profile: ColumnProfile,
        values: List[Any]
    ) -> None:
        """Add string statistics to profile."""
        lengths = [len(str(v)) for v in values]
        
        if lengths:
            profile.min_length = min(lengths)
            profile.max_length = max(lengths)
            profile.avg_length = statistics.mean(lengths)
    
    def _detect_patterns(self, values: List[Any]) -> List[str]:
        """Detect common patterns in values."""
        patterns_found = []
        str_values = [str(v) for v in values if v is not None]
        
        if not str_values:
            return patterns_found
        
        # Check for common patterns
        pattern_checks = {
            "email": r"[^@]+@[^@]+\.[^@]+",
            "url": r"https?://[^\s]+",
            "phone": r"\+?[\d\s\-\(\)]{10,}",
            "date_iso": r"\d{4}-\d{2}-\d{2}",
            "datetime_iso": r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
            "uuid": r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            "ipv4": r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}",
            "credit_card": r"\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}",
            "ssn": r"\d{3}-\d{2}-\d{4}",
        }
        
        for pattern_name, pattern in pattern_checks.items():
            matches = sum(1 for v in str_values if re.search(pattern, v))
            match_ratio = matches / len(str_values) if str_values else 0
            if match_ratio > 0.9:  # 90% match threshold
                patterns_found.append(pattern_name)
        
        return patterns_found
    
    def _generate_histogram(self, values: List[Any]) -> List[Dict[str, Any]]:
        """Generate histogram for numeric values."""
        try:
            numeric_values = [float(v) for v in values]
            
            if len(numeric_values) < 2:
                return []
            
            min_val = min(numeric_values)
            max_val = max(numeric_values)
            
            if min_val == max_val:
                return [{"range": f"{min_val}", "count": len(numeric_values)}]
            
            num_buckets = min(10, len(numeric_values) // 10 + 1)
            bucket_size = (max_val - min_val) / num_buckets
            
            buckets = [0] * num_buckets
            for v in numeric_values:
                bucket_index = min(int((v - min_val) / bucket_size), num_buckets - 1)
                buckets[bucket_index] += 1
            
            histogram = []
            for i, count in enumerate(buckets):
                range_start = min_val + i * bucket_size
                range_end = range_start + bucket_size
                histogram.append({
                    "range": f"{range_start:.2f}-{range_end:.2f}",
                    "count": count,
                    "percentage": count / len(numeric_values) * 100
                })
            
            return histogram
        
        except Exception as e:
            logger.warning(f"Error generating histogram: {e}")
            return []
    
    def profile_dataset(
        self,
        records: List[Dict[str, Any]],
        calculate_correlations: bool = True
    ) -> DatasetProfile:
        """
        Profile an entire dataset.
        
        Args:
            records: List of record dictionaries
            calculate_correlations: Whether to calculate correlations
        
        Returns:
            DatasetProfile with complete analysis
        """
        if not records:
            raise ValueError("Cannot profile empty dataset")
        
        # Get all column names
        all_columns = set()
        for record in records:
            all_columns.update(record.keys())
        
        # Profile each column
        column_profiles = {}
        numeric_columns: List[str] = []
        
        for column_name in all_columns:
            values = [record.get(column_name) for record in records]
            profile = self.profile_column(values)
            profile.name = column_name
            column_profiles[column_name] = profile
            
            if profile.inferred_type in ("integer", "float"):
                numeric_columns.append(column_name)
        
        # Calculate correlations for numeric columns
        correlations = {}
        if calculate_correlations and len(numeric_columns) > 1:
            for i, col1 in enumerate(numeric_columns):
                correlations[col1] = {}
                values1 = [
                    float(record.get(col1, 0))
                    for record in records
                    if record.get(col1) is not None
                ]
                
                for col2 in numeric_columns[i + 1:]:
                    values2 = [
                        float(record.get(col2, 0))
                        for record in records
                        if record.get(col2) is not None
                    ]
                    
                    corr = self._calculate_correlation(values1, values2)
                    correlations[col1][col2] = corr
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(column_profiles)
        
        return DatasetProfile(
            dataset_name=self.dataset_name,
            timestamp=datetime.utcnow(),
            row_count=len(records),
            column_count=len(all_columns),
            column_profiles=column_profiles,
            correlations=correlations,
            quality_score=quality_score
        )
    
    def _calculate_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        try:
            n = len(x)
            mean_x = sum(x) / n
            mean_y = sum(y) / n
            
            numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
            denominator_x = sum((xi - mean_x) ** 2 for xi in x) ** 0.5
            denominator_y = sum((yi - mean_y) ** 2 for yi in y) ** 0.5
            
            if denominator_x == 0 or denominator_y == 0:
                return 0.0
            
            return numerator / (denominator_x * denominator_y)
        
        except Exception:
            return 0.0
    
    def _calculate_quality_score(
        self,
        column_profiles: Dict[str, ColumnProfile]
    ) -> float:
        """Calculate overall data quality score."""
        if not column_profiles:
            return 0.0
        
        scores = []
        for profile in column_profiles.values():
            # Completeness contributes 50%
            completeness_score = profile.completeness * 50
            
            # Uniqueness contributes 30%
            uniqueness_score = min(1.0, profile.unique_count / profile.total_count) * 30 if profile.total_count > 0 else 0
            
            # Validity contributes 20%
            validity_score = 20 if profile.inferred_type != "unknown" else 0
            
            scores.append(completeness_score + uniqueness_score + validity_score)
        
        return sum(scores) / len(scores) if scores else 0.0


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample data
    records = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "salary": 75000.50},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25, "salary": 60000.00},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35, "salary": 90000.00},
        {"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28, "salary": 70000.00},
        {"id": 5, "name": "", "email": "invalid-email", "age": 150, "salary": None},
    ]
    
    # Profile dataset
    profiler = DataProfilerAction("employees")
    profile = profiler.profile_dataset(records)
    
    print(f"Dataset: {profile.dataset_name}")
    print(f"Rows: {profile.row_count}, Columns: {profile.column_count}")
    print(f"Quality Score: {profile.quality_score:.1f}%")
    print(f"\nColumn Profiles:")
    
    for name, col_profile in profile.column_profiles.items():
        print(f"\n  {name}:")
        print(f"    Type: {col_profile.inferred_type}")
        print(f"    Completeness: {col_profile.completeness:.1%}")
        print(f"    Unique: {col_profile.unique_count}")
        if col_profile.mean is not None:
            print(f"    Mean: {col_profile.mean:.2f}, Median: {col_profile.median}")
        print(f"    Patterns: {col_profile.detected_patterns}")

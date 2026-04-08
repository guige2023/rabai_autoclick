"""
Data Analyzer Action Module.

Performs statistical analysis on datasets including descriptive statistics,
correlation analysis, distribution testing, and outlier detection.

Author: RabAi Team
"""

from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd


class DistributionType(Enum):
    """Distribution types."""
    NORMAL = "normal"
    UNIFORM = "uniform"
    BIMODAL = "bimodal"
    SKEWED_RIGHT = "skewed_right"
    SKEWED_LEFT = "skewed_left"
    UNKNOWN = "unknown"


@dataclass
class ColumnStats:
    """Statistics for a single column."""
    column: str
    dtype: str
    count: int
    null_count: int
    unique_count: int
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    min: Optional[Any] = None
    max: Optional[Any] = None
    q25: Optional[float] = None
    q75: Optional[float] = None
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    distribution: DistributionType = DistributionType.UNKNOWN

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "dtype": self.dtype,
            "count": self.count,
            "null_count": self.null_count,
            "unique_count": self.unique_count,
            "mean": self.mean,
            "median": self.median,
            "std": self.std,
            "min": self.min,
            "max": self.max,
            "q25": self.q25,
            "q75": self.q75,
            "skewness": self.skewness,
            "kurtosis": self.kurtosis,
            "distribution": self.distribution.value,
        }


@dataclass
class CorrelationPair:
    """Correlation between two columns."""
    column1: str
    column2: str
    correlation: float
    p_value: Optional[float] = None


@dataclass
class AnalysisReport:
    """Comprehensive data analysis report."""
    dataset_name: str
    row_count: int
    column_count: int
    column_stats: List[ColumnStats] = field(default_factory=list)
    correlations: List[CorrelationPair] = field(default_factory=list)
    outliers: Dict[str, List[Any]] = field(default_factory=dict)
    missing_summary: Dict[str, int] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "column_stats": [s.to_dict() for s in self.column_stats],
            "correlations": [
                {"col1": c.column1, "col2": c.column2, "corr": c.correlation}
                for c in self.correlations
            ],
            "outliers": self.outliers,
            "missing_summary": self.missing_summary,
            "generated_at": self.generated_at.isoformat(),
        }


class DataAnalyzer:
    """
    Statistical data analysis engine.

    Computes descriptive statistics, correlations, distributions,
    and detects outliers across all data types.

    Example:
        >>> analyzer = DataAnalyzer()
        >>> report = analyzer.analyze(df, dataset_name="sales")
        >>> print(f"Columns analyzed: {report.column_count}")
    """

    def analyze(
        self,
        df: pd.DataFrame,
        dataset_name: str = "dataset",
        numeric_only: bool = False,
    ) -> AnalysisReport:
        """Perform comprehensive data analysis."""
        column_stats = []
        correlations = []
        outliers = {}
        missing_summary = {}

        # Missing value summary
        for col in df.columns:
            null_count = int(df[col].isnull().sum())
            if null_count > 0:
                missing_summary[col] = null_count

        # Column statistics
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in df.columns:
            stats = self._analyze_column(df[col])
            column_stats.append(stats)

            # Outlier detection for numeric columns
            if col in numeric_cols:
                col_outliers = self._detect_outliers(df[col])
                if col_outliers:
                    outliers[col] = col_outliers

        # Correlation analysis for numeric columns
        if len(numeric_cols) >= 2:
            corr_matrix = df[numeric_cols].corr()
            for i, col1 in enumerate(numeric_cols):
                for col2 in corr_matrix.columns[i+1:]:
                    corr_val = corr_matrix.loc[col1, col2]
                    if abs(corr_val) > 0.5:
                        correlations.append(CorrelationPair(
                            column1=col1,
                            column2=col2,
                            correlation=float(corr_val),
                        ))

        return AnalysisReport(
            dataset_name=dataset_name,
            row_count=len(df),
            column_count=len(df.columns),
            column_stats=column_stats,
            correlations=correlations,
            outliers=outliers,
            missing_summary=missing_summary,
        )

    def _analyze_column(self, series: pd.Series) -> ColumnStats:
        """Analyze a single column."""
        null_count = int(series.isnull().sum())
        non_null = series.dropna()

        stats = ColumnStats(
            column=str(series.name),
            dtype=str(series.dtype),
            count=int(series.count()),
            null_count=null_count,
            unique_count=int(series.nunique()),
        )

        if pd.api.types.is_numeric_dtype(series):
            numeric_data = series.dropna()
            if len(numeric_data) > 0:
                stats.mean = float(numeric_data.mean())
                stats.median = float(numeric_data.median())
                stats.std = float(numeric_data.std()) if len(numeric_data) > 1 else 0.0
                stats.min = float(numeric_data.min())
                stats.max = float(numeric_data.max())
                stats.q25 = float(numeric_data.quantile(0.25))
                stats.q75 = float(numeric_data.quantile(0.75))

                if len(numeric_data) > 2:
                    stats.skewness = float(numeric_data.skew())
                    stats.kurtosis = float(numeric_data.kurtosis())
                    stats.distribution = self._determine_distribution(numeric_data)

        return stats

    def _detect_outliers(self, series: pd.Series, threshold: float = 3.0) -> List[Any]:
        """Detect outliers using IQR method."""
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        outliers = series[(series < lower) | (series > upper)].tolist()
        return outliers[:100]  # Limit to first 100

    def _determine_distribution(self, series: pd.Series) -> DistributionType:
        """Determine the distribution type."""
        skew = series.skew()
        if abs(skew) < 0.5:
            return DistributionType.NORMAL
        elif skew > 0.5:
            return DistributionType.SKEWED_RIGHT
        else:
            return DistributionType.SKEWED_LEFT


def create_analyzer() -> DataAnalyzer:
    """Factory to create a data analyzer."""
    return DataAnalyzer()

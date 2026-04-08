"""
Data Imputer Action Module.

Handles missing data imputation using various strategies including
mean, median, mode, forward fill, backward fill, interpolation, and ML-based.

Author: RabAi Team
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd


class ImputeStrategy(Enum):
    """Imputation strategies."""
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    CONSTANT = "constant"
    FORWARD_FILL = "ffill"
    BACKWARD_FILL = "bfill"
    INTERPOLATE_LINEAR = "interpolate_linear"
    INTERPOLATE_TIME = "interpolate_time"
    KNN = "knn"
    REGRESSOR = "regressor"


@dataclass
class ImputeConfig:
    """Configuration for imputation."""
    strategy: ImputeStrategy
    columns: List[str] = field(default_factory=list)
    constant_value: Any = None
    fill_value: Any = None
    limit: Optional[int] = None
    knn_neighbors: int = 5
    regressor_fn: Optional[Callable] = None
    group_by: Optional[List[str]] = None


@dataclass
class ImputeReport:
    """Report of imputation operations."""
    strategy: ImputeStrategy
    total_missing_before: int
    total_missing_after: int
    columns_imputed: List[str]
    values_filled: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def fill_count(self) -> int:
        return self.total_missing_before - self.total_missing_after


class DataImputer:
    """
    Handles missing data imputation with multiple strategies.

    Supports column-wise and row-wise imputation using statistical methods,
    time-aware methods, and ML-based approaches.

    Example:
        >>> imputer = DataImputer()
        >>> imputer.configure(strategy=ImputeStrategy.MEAN, columns=["age", "income"])
        >>> result = imputer.impute(df)
    """

    def __init__(self):
        self._configs: Dict[str, ImputeConfig] = {}

    def configure(
        self,
        strategy: ImputeStrategy,
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> "DataImputer":
        """Configure imputation strategy for specific columns."""
        col_key = ",".join(sorted(columns or ["__all__"]))
        self._configs[col_key] = ImputeConfig(
            strategy=strategy,
            columns=columns or [],
            **kwargs,
        )
        return self

    def impute(
        self,
        df: pd.DataFrame,
        global_strategy: Optional[ImputeStrategy] = None,
        global_columns: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, ImputeReport]:
        """Impute missing values in DataFrame."""
        df = df.copy()
        reports = []
        total_missing_before = int(df.isnull().sum().sum())

        # Apply global strategy if specified
        if global_strategy and global_columns:
            report = self._impute_columns(df, global_strategy, global_columns)
            reports.append(report)

        # Apply configured strategies
        for col_key, config in self._configs.items():
            if config.columns:
                report = self._impute_columns(df, config.strategy, config.columns, config)
                reports.append(report)

        total_missing_after = int(df.isnull().sum().sum())

        # Merge reports
        combined_report = ImputeReport(
            strategy=global_strategy or reports[0].strategy if reports else ImputeStrategy.MEAN,
            total_missing_before=total_missing_before,
            total_missing_after=total_missing_after,
            columns_imputed=[],
        )
        for r in reports:
            combined_report.columns_imputed.extend(r.columns_imputed)
            combined_report.values_filled.update(r.values_filled)

        return df, combined_report

    def _impute_columns(
        self,
        df: pd.DataFrame,
        strategy: ImputeStrategy,
        columns: List[str],
        config: Optional[ImputeConfig] = None,
    ) -> ImputeReport:
        """Impute specific columns."""
        report = ImputeReport(
            strategy=strategy,
            total_missing_before=int(df[columns].isnull().sum().sum()),
            total_missing_after=0,
            columns_imputed=columns,
        )

        for col in columns:
            if col not in df.columns:
                continue
            missing_before = df[col].isnull().sum()
            if missing_before == 0:
                continue

            if strategy == ImputeStrategy.MEAN:
                fill_val = df[col].mean()
            elif strategy == ImputeStrategy.MEDIAN:
                fill_val = df[col].median()
            elif strategy == ImputeStrategy.MODE:
                fill_val = df[col].mode().iloc[0] if len(df[col].mode()) > 0 else None
            elif strategy == ImputeStrategy.CONSTANT:
                fill_val = config.constant_value if config else None
            elif strategy == ImputeStrategy.FORWARD_FILL:
                df[col] = df[col].ffill(limit=config.limit if config else None)
                report.values_filled[col] = "ffill"
                continue
            elif strategy == ImputeStrategy.BACKWARD_FILL:
                df[col] = df[col].bfill(limit=config.limit if config else None)
                report.values_filled[col] = "bfill"
                continue
            elif strategy == ImputeStrategy.INTERPOLATE_LINEAR:
                df[col] = df[col].interpolate(method="linear", limit=config.limit if config else None)
                report.values_filled[col] = "interpolate"
                continue
            elif strategy == ImputeStrategy.INTERPOLATE_TIME:
                df[col] = df[col].interpolate(method="time", limit=config.limit if config else None)
                report.values_filled[col] = "interpolate_time"
                continue
            else:
                fill_val = None

            if fill_val is not None and not pd.isna(fill_val):
                df[col] = df[col].fillna(fill_val)
                report.values_filled[col] = fill_val

        report.total_missing_after = int(df[columns].isnull().sum().sum())
        return report

    def impute_row_wise(
        self,
        df: pd.DataFrame,
        strategy: ImputeStrategy = ImputeStrategy.MEAN,
        threshold: float = 0.5,
    ) -> pd.DataFrame:
        """
        Impute missing values row-wise (for rows with few missing values).

        Args:
            df: Input DataFrame
            strategy: Strategy to use
            threshold: Maximum fraction of missing values to impute (0.5 = 50%)
        """
        df = df.copy()
        for idx, row in df.iterrows():
            missing_count = row.isnull().sum()
            missing_ratio = missing_count / len(row)
            if missing_ratio > threshold:
                continue
            for col in df.columns:
                if pd.isnull(row[col]):
                    fill_val = None
                    if strategy == ImputeStrategy.MEAN:
                        fill_val = df[col].mean()
                    elif strategy == ImputeStrategy.MEDIAN:
                        fill_val = df[col].median()
                    elif strategy == ImputeStrategy.MODE:
                        fill_val = df[col].mode().iloc[0] if len(df[col].mode()) > 0 else None
                    if fill_val is not None:
                        df.at[idx, col] = fill_val
        return df


def create_imputer(
    strategy: str = "mean",
    columns: Optional[List[str]] = None,
) -> DataImputer:
    """Factory to create a configured data imputer."""
    imputer = DataImputer()
    imputer.configure(ImputeStrategy(strategy), columns)
    return imputer

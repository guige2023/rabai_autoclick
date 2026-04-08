"""
Data Benchmark Action Module.

Benchmarks data operations including read, write, transform, and aggregate
operations with detailed timing and throughput metrics.

Author: RabAi Team
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd


class OperationType(Enum):
    """Types of operations to benchmark."""
    READ_CSV = "read_csv"
    READ_PARQUET = "read_parquet"
    READ_JSON = "read_json"
    WRITE_CSV = "write_csv"
    WRITE_PARQUET = "write_parquet"
    WRITE_JSON = "write_json"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SORT = "sort"
    FILTER = "filter"
    GROUP_BY = "group_by"


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    operation: OperationType
    duration_ms: float
    throughput_mb_per_sec: float
    rows_per_sec: float
    memory_used_mb: float
    iterations: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation": self.operation.value,
            "duration_ms": self.duration_ms,
            "throughput_mb_per_sec": self.throughput_mb_per_sec,
            "rows_per_sec": self.rows_per_sec,
            "memory_used_mb": self.memory_used_mb,
            "iterations": self.iterations,
            "metadata": self.metadata,
        }


@dataclass
class BenchmarkReport:
    """Comprehensive benchmark report."""
    benchmark_id: str
    results: List[BenchmarkResult] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)
    system_info: Dict[str, Any] = field(default_factory=dict)

    def get_fastest(self) -> BenchmarkResult:
        return min(self.results, key=lambda r: r.duration_ms)

    def get_slowest(self) -> BenchmarkResult:
        return max(self.results, key=lambda r: r.duration_ms)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "benchmark_id": self.benchmark_id,
            "results": [r.to_dict() for r in self.results],
            "generated_at": self.generated_at.isoformat(),
            "system_info": self.system_info,
        }


class DataBenchmark:
    """
    Benchmarks data operations with timing and throughput analysis.

    Measures read, write, transform, and aggregate operations with
    detailed performance metrics.

    Example:
        >>> benchmark = DataBenchmark()
        >>> result = benchmark.benchmark_read(df, operation=OperationType.READ_CSV)
        >>> print(f"Throughput: {result.throughput_mb_per_sec:.2f} MB/s")
    """

    def __init__(self):
        self._results: List[BenchmarkResult] = []
        self._benchmark_id = str(uuid.uuid4())

    def benchmark_operation(
        self,
        operation: Callable,
        operation_type: OperationType,
        iterations: int = 3,
        warmup: int = 1,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BenchmarkResult:
        """Benchmark a custom operation."""
        # Warmup runs
        for _ in range(warmup):
            operation()

        # Timed runs
        durations = []
        for _ in range(iterations):
            start = time.perf_counter()
            result = operation()
            duration = (time.perf_counter() - start) * 1000
            durations.append(duration)

        avg_duration = sum(durations) / len(durations)

        result = BenchmarkResult(
            operation=operation_type,
            duration_ms=avg_duration,
            throughput_mb_per_sec=0.0,
            rows_per_sec=0.0,
            memory_used_mb=0.0,
            iterations=iterations,
            metadata=metadata or {},
        )

        self._results.append(result)
        return result

    def benchmark_read(
        self,
        df: pd.DataFrame,
        operation: OperationType = OperationType.READ_CSV,
        iterations: int = 3,
    ) -> BenchmarkResult:
        """Benchmark reading data."""
        row_count = len(df)
        size_bytes = df.memory_usage(deep=True).sum()
        size_mb = size_bytes / (1024 * 1024)

        def read_operation():
            return df.copy()

        result = self.benchmark_operation(
            read_operation,
            operation,
            iterations=iterations,
            metadata={"row_count": row_count, "size_mb": size_mb},
        )
        result.rows_per_sec = row_count / (result.duration_ms / 1000) if result.duration_ms > 0 else 0
        result.throughput_mb_per_sec = size_mb / (result.duration_ms / 1000) if result.duration_ms > 0 else 0

        return result

    def benchmark_transform(
        self,
        df: pd.DataFrame,
        transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
        iterations: int = 3,
    ) -> BenchmarkResult:
        """Benchmark a DataFrame transformation."""
        row_count = len(df)

        def transform():
            return transform_fn(df)

        result = self.benchmark_operation(
            transform,
            OperationType.TRANSFORM,
            iterations=iterations,
            metadata={"row_count": row_count},
        )
        result.rows_per_sec = row_count / (result.duration_ms / 1000) if result.duration_ms > 0 else 0

        return result

    def benchmark_aggregate(
        self,
        df: pd.DataFrame,
        group_cols: List[str],
        agg_funcs: Dict[str, str],
        iterations: int = 3,
    ) -> BenchmarkResult:
        """Benchmark an aggregation operation."""
        row_count = len(df)

        def aggregate():
            return df.groupby(group_cols).agg(agg_funcs)

        result = self.benchmark_operation(
            aggregate,
            OperationType.AGGREGATE,
            iterations=iterations,
            metadata={"row_count": row_count, "group_cols": group_cols},
        )
        result.rows_per_sec = row_count / (result.duration_ms / 1000) if result.duration_ms > 0 else 0

        return result

    def benchmark_join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: str,
        how: str = "inner",
        iterations: int = 3,
    ) -> BenchmarkResult:
        """Benchmark a join operation."""
        total_rows = len(left) + len(right)

        def join_op():
            return left.merge(right, on=on, how=how)

        result = self.benchmark_operation(
            join_op,
            OperationType.JOIN,
            iterations=iterations,
            metadata={"left_rows": len(left), "right_rows": len(right), "how": how},
        )
        result.rows_per_sec = total_rows / (result.duration_ms / 1000) if result.duration_ms > 0 else 0

        return result

    def get_report(self) -> BenchmarkReport:
        """Get comprehensive benchmark report."""
        return BenchmarkReport(
            benchmark_id=self._benchmark_id,
            results=self._results,
            system_info=self._get_system_info(),
        )

    def _get_system_info(self) -> Dict[str, Any]:
        """Get system information for context."""
        import platform
        return {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "processor": platform.processor(),
            "results_count": len(self._results),
        }


def create_benchmark() -> DataBenchmark:
    """Factory to create a data benchmark."""
    return DataBenchmark()

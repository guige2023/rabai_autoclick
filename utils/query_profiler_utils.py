"""
Query profiling and execution analysis utilities.

Provides SQL query profiling, execution plan analysis,
and query performance tracking.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryProfile:
    """Query execution profile."""
    sql: str
    duration_ms: float
    rows_affected: int = 0
    plan: dict[str, Any] | None = None
    timestamp: float = field(default_factory=time.time)
    error: str | None = None


class QueryProfiler:
    """Context manager for profiling SQL queries."""

    def __init__(self, sql: str, params: tuple | None = None):
        self.sql = sql
        self.params = params
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.rows_affected: int = 0
        self.error: str | None = None
        self._profile: QueryProfile | None = None

    def __enter__(self) -> "QueryProfiler":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.end_time = time.time()
        if exc_type is not None:
            self.error = str(exc_val)
        self._profile = QueryProfile(
            sql=self.sql,
            duration_ms=(self.end_time - self.start_time) * 1000,
            rows_affected=self.rows_affected,
            error=self.error,
        )

    @property
    def profile(self) -> QueryProfile | None:
        return self._profile

    @property
    def duration_ms(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time) * 1000
        return 0.0


def extract_table_names(sql: str) -> list[str]:
    """Extract table names from SQL query."""
    patterns = [
        r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        r"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    ]
    tables = set()
    for pattern in patterns:
        matches = re.findall(pattern, sql, re.IGNORECASE)
        tables.update(matches)
    return list(tables)


def estimate_query_complexity(sql: str) -> int:
    """
    Estimate query complexity score.

    Returns:
        Complexity score (higher = more complex)
    """
    score = 0
    sql_upper = sql.upper()

    joins = len(re.findall(r"\bJOIN\b", sql_upper))
    score += joins * 3

    subqueries = len(re.findall(r"\bSELECT\b", sql_upper)) - 1
    score += subqueries * 5

    if "GROUP BY" in sql_upper:
        score += 2
    if "ORDER BY" in sql_upper:
        score += 1
    if "HAVING" in sql_upper:
        score += 2
    if "DISTINCT" in sql_upper:
        score += 1
    if "UNION" in sql_upper:
        score += 3
    if "LIKE '%" in sql_upper or 'LIKE "%' in sql_upper:
        score += 2

    return score


def classify_query(sql: str) -> str:
    """Classify query type."""
    sql_upper = sql.strip().upper()
    if sql_upper.startswith("SELECT"):
        return "SELECT"
    if sql_upper.startswith("INSERT"):
        return "INSERT"
    if sql_upper.startswith("UPDATE"):
        return "UPDATE"
    if sql_upper.startswith("DELETE"):
        return "DELETE"
    if sql_upper.startswith("CREATE"):
        return "DDL"
    if sql_upper.startswith("ALTER"):
        return "DDL"
    if sql_upper.startswith("DROP"):
        return "DDL"
    return "OTHER"


def format_duration(ms: float) -> str:
    """Format duration in human-readable form."""
    if ms < 1:
        return f"{ms * 1000:.2f}µs"
    if ms < 1000:
        return f"{ms:.2f}ms"
    return f"{ms / 1000:.2f}s"


@dataclass
class QueryStats:
    """Aggregated query statistics."""
    total_queries: int = 0
    total_duration_ms: float = 0.0
    slowest_ms: float = 0.0
    fastest_ms: float = float("inf")
    failed: int = 0

    @property
    def avg_ms(self) -> float:
        if self.total_queries == 0:
            return 0.0
        return self.total_duration_ms / self.total_queries

    def record(self, profile: QueryProfile) -> None:
        """Record a query profile."""
        self.total_queries += 1
        if profile.error:
            self.failed += 1
        else:
            self.total_duration_ms += profile.duration_ms
            self.slowest_ms = max(self.slowest_ms, profile.duration_ms)
            self.fastest_ms = min(self.fastest_ms, profile.duration_ms)

    def __str__(self) -> str:
        return (
            f"QueryStats(queries={self.total_queries}, "
            f"avg={format_duration(self.avg_ms)}, "
            f"slowest={format_duration(self.slowest_ms)}, "
            f"failed={self.failed})"
        )

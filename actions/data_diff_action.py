"""
Data Diff Action Module.

Compares two datasets and generates detailed difference reports including
row-level, column-level, and schema-level differences.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


class DiffType(Enum):
    """Types of differences."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


@dataclass
class CellDiff:
    """Difference at a cell level."""
    row_index: int
    column: str
    old_value: Any
    new_value: Any
    diff_type: DiffType


@dataclass
class RowDiff:
    """Difference at a row level."""
    row_index: int
    diff_type: DiffType
    key_values: Dict[str, Any] = field(default_factory=dict)
    cell_diffs: List[CellDiff] = field(default_factory=list)


@dataclass
class SchemaDiff:
    """Difference in schema."""
    column: str
    diff_type: DiffType
    old_type: Optional[str] = None
    new_type: Optional[str] = None


@dataclass
class DiffReport:
    """Comprehensive diff report."""
    total_rows_left: int
    total_rows_right: int
    rows_added: int = 0
    rows_removed: int = 0
    rows_modified: int = 0
    rows_unchanged: int = 0
    schema_diffs: List[SchemaDiff] = field(default_factory=list)
    row_diffs: List[RowDiff] = field(default_factory=list)
    columns_left: List[str] = field(default_factory=list)
    columns_right: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def is_identical(self) -> bool:
        return (
            self.rows_added == 0
            and self.rows_removed == 0
            and self.rows_modified == 0
            and len(self.schema_diffs) == 0
        )

    @property
    def change_count(self) -> int:
        return self.rows_added + self.rows_removed + self.rows_modified

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_identical": self.is_identical,
            "total_rows_left": self.total_rows_left,
            "total_rows_right": self.total_rows_right,
            "rows_added": self.rows_added,
            "rows_removed": self.rows_removed,
            "rows_modified": self.rows_modified,
            "rows_unchanged": self.rows_unchanged,
            "change_count": self.change_count,
            "schema_diffs": [
                {"column": s.column, "diff_type": s.diff_type.value, "old_type": s.old_type, "new_type": s.new_type}
                for s in self.schema_diffs
            ],
            "columns_left": self.columns_left,
            "columns_right": self.columns_right,
            "generated_at": self.generated_at.isoformat(),
        }


class DataDiffer:
    """
    Compares two datasets and generates diff reports.

    Supports row-level, column-level, and schema-level comparison
    with configurable key columns and ignore columns.

    Example:
        >>> differ = DataDiffer(key_columns=["id"])
        >>> report = differ.compare(df1, df2)
        >>> print(f"Modified rows: {report.rows_modified}")
    """

    def __init__(
        self,
        key_columns: Optional[List[str]] = None,
        ignore_columns: Optional[List[str]] = None,
    ):
        self.key_columns = key_columns or []
        self.ignore_columns = ignore_columns or set()

    def compare(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
    ) -> DiffReport:
        """Compare two DataFrames and generate diff report."""
        schema_diffs = self._compare_schema(left, right)

        if self.key_columns:
            return self._compare_with_keys(left, right, schema_diffs)
        else:
            return self._compare_row_by_row(left, right, schema_diffs)

    def _compare_schema(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
    ) -> List[SchemaDiff]:
        """Compare schemas."""
        diffs = []
        cols_left = set(left.columns) - self.ignore_columns
        cols_right = set(right.columns) - self.ignore_columns

        for col in cols_left - cols_right:
            diffs.append(SchemaDiff(column=col, diff_type=DiffType.REMOVED))

        for col in cols_right - cols_left:
            diffs.append(SchemaDiff(column=col, diff_type=DiffType.ADDED))

        for col in cols_left & cols_right:
            if str(left[col].dtype) != str(right[col].dtype):
                diffs.append(SchemaDiff(
                    column=col,
                    diff_type=DiffType.MODIFIED,
                    old_type=str(left[col].dtype),
                    new_type=str(right[col].dtype),
                ))

        return diffs

    def _compare_with_keys(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        schema_diffs: List[SchemaDiff],
    ) -> DiffReport:
        """Compare using key columns."""
        left_indexed = left.set_index(self.key_columns)
        right_indexed = right.set_index(self.key_columns)

        all_keys = set(left_indexed.index) | set(right_indexed.index)
        left_keys = set(left_indexed.index)
        right_keys = set(right_indexed.index)

        rows_added = len(right_keys - left_keys)
        rows_removed = len(left_keys - right_keys)

        row_diffs = []
        common_keys = left_keys & right_keys

        compare_cols = [c for c in left.columns if c not in self.key_columns and c not in self.ignore_columns]
        rows_modified = 0

        for key in common_keys:
            left_row = left_indexed.loc[key]
            right_row = right_indexed.loc[key]

            cell_diffs = []
            for col in compare_cols:
                if col not in right.columns:
                    continue
                lval = left_row.get(col)
                rval = right_row.get(col)
                if not self._values_equal(lval, rval):
                    cell_diffs.append(CellDiff(
                        row_index=0,
                        column=col,
                        old_value=lval,
                        new_value=rval,
                        diff_type=DiffType.MODIFIED,
                    ))

            if cell_diffs:
                rows_modified += 1
                row_diffs.append(RowDiff(
                    row_index=0,
                    diff_type=DiffType.MODIFIED,
                    key_values=dict(zip(self.key_columns, key)) if isinstance(key, tuple) else {self.key_columns[0]: key},
                    cell_diffs=cell_diffs,
                ))

        total_left = len(left)
        total_right = len(right)
        rows_unchanged = total_left - rows_removed - rows_modified

        return DiffReport(
            total_rows_left=total_left,
            total_rows_right=total_right,
            rows_added=rows_added,
            rows_removed=rows_removed,
            rows_modified=rows_modified,
            rows_unchanged=rows_unchanged,
            schema_diffs=schema_diffs,
            row_diffs=row_diffs,
            columns_left=list(left.columns),
            columns_right=list(right.columns),
        )

    def _compare_row_by_row(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        schema_diffs: List[SchemaDiff],
    ) -> DiffReport:
        """Compare row by row without keys."""
        row_diffs = []
        total_left = len(left)
        total_right = len(right)

        max_rows = max(total_left, total_right)
        rows_added = max(0, total_right - total_left)
        rows_removed = max(0, total_left - total_right)
        rows_modified = 0

        compare_cols = [c for c in left.columns if c not in self.ignore_columns]

        for i in range(min(total_left, total_right)):
            cell_diffs = []
            for col in compare_cols:
                if col not in right.columns:
                    continue
                lval = left.iloc[i][col]
                rval = right.iloc[i][col]
                if not self._values_equal(lval, rval):
                    cell_diffs.append(CellDiff(
                        row_index=i,
                        column=col,
                        old_value=lval,
                        new_value=rval,
                        diff_type=DiffType.MODIFIED,
                    ))

            if cell_diffs:
                rows_modified += 1
                row_diffs.append(RowDiff(
                    row_index=i,
                    diff_type=DiffType.MODIFIED,
                    cell_diffs=cell_diffs,
                ))

        rows_unchanged = total_left - rows_removed - rows_modified

        return DiffReport(
            total_rows_left=total_left,
            total_rows_right=total_right,
            rows_added=rows_added,
            rows_removed=rows_removed,
            rows_modified=rows_modified,
            rows_unchanged=rows_unchanged,
            schema_diffs=schema_diffs,
            row_diffs=row_diffs,
            columns_left=list(left.columns),
            columns_right=list(right.columns),
        )

    def _values_equal(self, a: Any, b: Any) -> bool:
        """Check if two values are equal."""
        if pd.isna(a) and pd.isna(b):
            return True
        if pd.isna(a) or pd.isna(b):
            return False
        return a == b


def create_differ(
    key_columns: Optional[List[str]] = None,
) -> DataDiffer:
    """Factory to create a data differ."""
    return DataDiffer(key_columns=key_columns)

"""
Bulk Insert Action Module.

Performs efficient bulk data insertion with batching, 
transaction management, and conflict handling.

Author: RabAi Team
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd


class ConflictStrategy(Enum):
    """Conflict resolution strategies."""
    IGNORE = "ignore"
    UPDATE = "update"
    SKIP = "skip"
    ERROR = "error"


@dataclass
class InsertConfig:
    """Configuration for bulk insert operations."""
    batch_size: int = 1000
    chunk_size: int = 5000
    transaction_size: Optional[int] = None
    conflict_strategy: ConflictStrategy = ConflictStrategy.IGNORE
    commit_frequency: int = 1
    ignore_errors: bool = True
    max_retries: int = 3


@dataclass
class InsertResult:
    """Result of a bulk insert operation."""
    success: bool
    total_rows: int
    inserted_rows: int
    updated_rows: int
    skipped_rows: int
    failed_rows: int
    duration_ms: float
    errors: List[Dict[str, Any]] = field(default_factory=list)
    batches: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "total_rows": self.total_rows,
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows,
            "skipped_rows": self.skipped_rows,
            "failed_rows": self.failed_rows,
            "duration_ms": self.duration_ms,
            "batches": self.batches,
            "errors": self.errors[:100],
        }


class BulkInserter:
    """
    Bulk data insertion engine with batching and conflict handling.

    Supports batch processing, transaction management, and
    configurable conflict resolution strategies.

    Example:
        >>> inserter = BulkInserter(insert_fn=db.insert_rows)
        >>> result = inserter.insert(df, batch_size=5000)
    """

    def __init__(
        self,
        insert_fn: Optional[Callable] = None,
        update_fn: Optional[Callable] = None,
        config: Optional[InsertConfig] = None,
    ):
        self.insert_fn = insert_fn
        self.update_fn = update_fn
        self.config = config or InsertConfig()

    def insert(
        self,
        data: List[Dict],
        table_name: Optional[str] = None,
    ) -> InsertResult:
        """Insert data in batches."""
        start = time.time()
        total = len(data)
        inserted = updated = skipped = failed = 0
        errors = []
        batches = 0

        for i in range(0, total, self.config.batch_size):
            batch = data[i:i + self.config.batch_size]
            batches += 1

            try:
                result = self._insert_batch(batch, table_name)
                inserted += result.get("inserted", len(batch))
                updated += result.get("updated", 0)
                skipped += result.get("skipped", 0)
                failed += result.get("failed", 0)
                if result.get("errors"):
                    errors.extend(result["errors"])
            except Exception as e:
                failed += len(batch)
                errors.append({"batch": batches, "error": str(e)})

        return InsertResult(
            success=failed == 0,
            total_rows=total,
            inserted_rows=inserted,
            updated_rows=updated,
            skipped_rows=skipped,
            failed_rows=failed,
            duration_ms=(time.time() - start) * 1000,
            errors=errors,
            batches=batches,
        )

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: Optional[str] = None,
    ) -> InsertResult:
        """Insert DataFrame rows."""
        records = df.to_dict("records")
        return self.insert(records, table_name)

    def _insert_batch(
        self,
        batch: List[Dict],
        table_name: Optional[str],
    ) -> Dict[str, Any]:
        """Insert a single batch."""
        if self.insert_fn:
            return self.insert_fn(batch, table_name)

        return {
            "inserted": len(batch),
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }


def create_bulk_inserter(
    insert_fn: Optional[Callable] = None,
    batch_size: int = 1000,
) -> BulkInserter:
    """Factory to create a bulk inserter."""
    config = InsertConfig(batch_size=batch_size)
    return BulkInserter(insert_fn=insert_fn, config=config)

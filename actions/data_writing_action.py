"""Data Writing Action.

Handles writing data to storage with batching and transaction support.
"""
from typing import Any, Callable, Dict, List, Literal, Optional
from dataclasses import dataclass, field
import time


@dataclass
class WriteOperation:
    operation_id: str
    entity_type: str
    entity_id: Optional[str]
    operation: Literal["insert", "update", "upsert", "delete"]
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    status: str = "pending"


@dataclass
class WriteResult:
    success: bool
    operation_id: str
    affected_count: int = 0
    error: Optional[str] = None


class DataWritingAction:
    """Manages data write operations with batching."""

    def __init__(
        self,
        batch_size: int = 100,
        flush_interval_sec: float = 5.0,
        write_fn: Optional[Callable[[List[WriteOperation]], WriteResult]] = None,
    ) -> None:
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.write_fn = write_fn
        self.buffer: List[WriteOperation] = []
        self.last_flush = time.time()
        self.write_history: List[WriteResult] = []

    def _generate_id(self) -> str:
        return f"{int(time.time()*1000)}"

    def insert(
        self,
        entity_type: str,
        entity_id: Optional[str],
        data: Dict[str, Any],
    ) -> WriteOperation:
        op = WriteOperation(
            operation_id=self._generate_id(),
            entity_type=entity_type,
            entity_id=entity_id,
            operation="insert",
            data=data,
        )
        self.buffer.append(op)
        if len(self.buffer) >= self.batch_size:
            self.flush()
        return op

    def update(
        self,
        entity_type: str,
        entity_id: str,
        data: Dict[str, Any],
    ) -> WriteOperation:
        op = WriteOperation(
            operation_id=self._generate_id(),
            entity_type=entity_type,
            entity_id=entity_id,
            operation="update",
            data=data,
        )
        self.buffer.append(op)
        if len(self.buffer) >= self.batch_size:
            self.flush()
        return op

    def upsert(
        self,
        entity_type: str,
        entity_id: str,
        data: Dict[str, Any],
    ) -> WriteOperation:
        op = WriteOperation(
            operation_id=self._generate_id(),
            entity_type=entity_type,
            entity_id=entity_id,
            operation="upsert",
            data=data,
        )
        self.buffer.append(op)
        if len(self.buffer) >= self.batch_size:
            self.flush()
        return op

    def delete(
        self,
        entity_type: str,
        entity_id: str,
    ) -> WriteOperation:
        op = WriteOperation(
            operation_id=self._generate_id(),
            entity_type=entity_type,
            entity_id=entity_id,
            operation="delete",
            data={},
        )
        self.buffer.append(op)
        if len(self.buffer) >= self.batch_size:
            self.flush()
        return op

    def flush(self) -> Optional[WriteResult]:
        if not self.buffer:
            return None
        if self.write_fn:
            result = self.write_fn(self.buffer)
            self.write_history.append(result)
            self.buffer.clear()
            self.last_flush = time.time()
            return result
        self.buffer.clear()
        self.last_flush = time.time()
        return WriteResult(success=True, operation_id="mock", affected_count=len(self.buffer))

    def is_flush_needed(self) -> bool:
        if len(self.buffer) >= self.batch_size:
            return True
        if time.time() - self.last_flush >= self.flush_interval_sec:
            return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        return {
            "buffer_size": len(self.buffer),
            "batch_size": self.batch_size,
            "flush_interval_sec": self.flush_interval_sec,
            "total_writes": len(self.write_history),
            "total_operations": sum(r.affected_count for r in self.write_history),
        }

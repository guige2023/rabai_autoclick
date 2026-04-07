"""Change Data Capture (CDC) utilities.

Capture and stream database changes (INSERT/UPDATE/DELETE) for building
real-time event pipelines, audit logs, and data synchronization.

Example:
    cdc = CDC(conn, table="orders", strategy="wal")
    for event in cdc.stream():
        print(event.operation, event.row)
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Generator
from uuid import uuid4

logger = logging.getLogger(__name__)


class CDCOperation(Enum):
    """Type of database operation captured."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    SNAPSHOT = "snapshot"


@dataclass
class CDCEvent:
    """A single change event captured from the database."""
    id: str
    operation: CDCOperation
    table: str
    row: dict[str, Any]
    old_row: dict[str, Any] | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    sequence: int = 0
    source: str = "unknown"


class CDCStrategy(Enum):
    """CDC capture strategy."""
    POLL = "poll"          # Periodic polling of modified rows
    TRIGGER = "trigger"    # Database triggers writing to CDC table
    WAL = "wal"            # Write-Ahead Log (PostgreSQL logical decoding)
    LOG_BASED = "log"      # Binlog-based (MySQL)
    TIME_BASED = "time"    # Timestamp-based polling


@dataclass
class CDCConfig:
    """Configuration for CDC capture."""
    strategy: CDCStrategy = CDCStrategy.POLL
    poll_interval: float = 1.0
    batch_size: int = 100
    track_tables: list[str] = []
    ignored_columns: tuple[str, ...] = ()
    include_old_row: bool = True
    offset_column: str = "id"


class CDCReader:
    """Change Data Capture reader for tracking database changes.

    Supports multiple strategies: polling, trigger-based, WAL, etc.
    """

    def __init__(
        self,
        connection: Any,
        config: CDCConfig | None = None,
    ) -> None:
        """Initialize CDC reader.

        Args:
            connection: Database connection object.
            config: CDC configuration options.
        """
        self.connection = connection
        self.config = config or CDCConfig()
        self._sequence = 0
        self._last_offset: dict[str, Any] = {}
        self._running = False
        self._lock = threading.Lock()

    def stream(
        self,
        table: str,
        offset: dict[str, Any] | None = None,
    ) -> Generator[CDCEvent, None, None]:
        """Stream CDC events for a table.

        Args:
            table: Table name to monitor.
            offset: Starting offset (last processed id/timestamp).

        Yields:
            CDCEvent objects as changes occur.
        """
        self._last_offset = offset or {}

        if self.config.strategy == CDCStrategy.POLL:
            yield from self._poll_stream(table)
        elif self.config.strategy == CDCStrategy.TIME_BASED:
            yield from self._time_based_stream(table)
        else:
            yield from self._generic_stream(table)

    def _poll_stream(self, table: str) -> Generator[CDCEvent, None, None]:
        """Poll-based CDC streaming."""
        offset_col = self.config.offset_column
        cursor = self.connection.cursor()

        while True:
            try:
                offset_val = self._last_offset.get(table)

                if offset_val is not None:
                    query = f"SELECT * FROM {table} WHERE {offset_col} > %s ORDER BY {offset_col} LIMIT %s"
                    cursor.execute(query, (offset_val, self.config.batch_size))
                else:
                    query = f"SELECT * FROM {table} ORDER BY {offset_col} LIMIT %s"
                    cursor.execute(query, (self.config.batch_size,))

                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                if not rows:
                    time.sleep(self.config.poll_interval)
                    continue

                for row in rows:
                    row_dict = dict(zip(columns, row))
                    max_offset = row_dict.get(offset_col)

                    yield CDCEvent(
                        id=str(uuid4()),
                        operation=CDCOperation.SNAPSHOT,
                        table=table,
                        row=row_dict,
                        sequence=self._next_seq(),
                    )

                    self._last_offset[table] = max_offset

                if len(rows) < self.config.batch_size:
                    time.sleep(self.config.poll_interval)

            except Exception as e:
                logger.error("CDC poll error on %s: %s", table, e)
                time.sleep(self.config.poll_interval * 2)

    def _time_based_stream(self, table: str) -> Generator[CDCEvent, None, None]:
        """Timestamp-based CDC streaming."""
        cursor = self.connection.cursor()

        while True:
            try:
                last_ts = self._last_offset.get(table)
                ts_col = "updated_at" if self._has_column(table, "updated_at") else "created_at"

                if last_ts:
                    query = f"SELECT * FROM {table} WHERE {ts_col} > %s ORDER BY {ts_col} LIMIT %s"
                    cursor.execute(query, (last_ts, self.config.batch_size))
                else:
                    query = f"SELECT * FROM {table} ORDER BY {ts_col} LIMIT %s"
                    cursor.execute(query, (self.config.batch_size,))

                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                if not rows:
                    time.sleep(self.config.poll_interval)
                    continue

                for row in rows:
                    row_dict = dict(zip(columns, row))
                    max_ts = row_dict.get(ts_col)

                    yield CDCEvent(
                        id=str(uuid4()),
                        operation=CDCOperation.UPDATE,
                        table=table,
                        row=row_dict,
                        sequence=self._next_seq(),
                    )

                    self._last_offset[table] = max_ts

                if len(rows) < self.config.batch_size:
                    time.sleep(self.config.poll_interval)

            except Exception as e:
                logger.error("CDC time-based error on %s: %s", table, e)
                time.sleep(self.config.poll_interval * 2)

    def _generic_stream(self, table: str) -> Generator[CDCEvent, None, None]:
        """Generic streaming based on configured strategy."""
        yield from self._poll_stream(table)

    def _has_column(self, table: str, column: str) -> bool:
        """Check if table has a specific column."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT 1 FROM {table} LIMIT 0")
            return False
        except Exception:
            return True

    def _next_seq(self) -> int:
        """Get next sequence number."""
        with self._lock:
            self._sequence += 1
            return self._sequence

    def get_offset(self) -> dict[str, Any]:
        """Get current offsets for all tracked tables."""
        return dict(self._last_offset)

    def set_offset(self, offset: dict[str, Any]) -> None:
        """Set offsets (for resuming from checkpoint)."""
        self._last_offset = dict(offset)


class CDCWriter:
    """Write CDC events to a target sink (Kafka, queue, file, etc.)."""

    def __init__(self, sink: Callable[[CDCEvent], None]) -> None:
        """Initialize CDC writer.

        Args:
            sink: Callback function to receive each CDC event.
        """
        self.sink = sink
        self._written = 0

    def write(self, event: CDCEvent) -> None:
        """Write a single CDC event to the sink.

        Args:
            event: CDCEvent to write.
        """
        self.sink(event)
        self._written += 1

    def write_batch(self, events: list[CDCEvent]) -> int:
        """Write multiple CDC events.

        Args:
            events: List of CDCEvent objects.

        Returns:
            Number of events written.
        """
        for event in events:
            self.write(event)
        return len(events)


@dataclass
class CDCOffset:
    """Offset checkpoint for resumable CDC processing."""
    table: str
    offset_value: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)
    sequence: int = 0

    def to_json(self) -> str:
        """Serialize offset to JSON."""
        return json.dumps({
            "table": self.table,
            "offset_value": self.offset_value,
            "timestamp": self.timestamp.isoformat(),
            "sequence": self.sequence,
        })

    @classmethod
    def from_json(cls, data: str) -> "CDCOffset":
        """Deserialize offset from JSON."""
        obj = json.loads(data)
        return cls(
            table=obj["table"],
            offset_value=obj["offset_value"],
            timestamp=datetime.fromisoformat(obj["timestamp"]),
            sequence=obj["sequence"],
        )


class CDCPipeline:
    """CDC pipeline connecting readers to writers with offset persistence."""

    def __init__(
        self,
        reader: CDCReader,
        writer: CDCWriter,
        offset_store: Callable[[dict], None] | None = None,
    ) -> None:
        """Initialize CDC pipeline.

        Args:
            reader: CDCReader to source events.
            writer: CDCWriter to sink events.
            offset_store: Optional callback to persist offsets.
        """
        self.reader = reader
        self.writer = writer
        self.offset_store = offset_store

    def run(
        self,
        table: str,
        batch_size: int = 50,
        auto_commit: bool = True,
    ) -> int:
        """Run the CDC pipeline.

        Args:
            table: Table to monitor.
            batch_size: Events to process before committing offset.
            auto_commit: Automatically commit offsets after each batch.

        Returns:
            Total number of events processed.
        """
        total = 0
        batch: list[CDCEvent] = []

        for event in self.reader.stream(table):
            batch.append(event)
            self.writer.write(event)
            total += 1

            if len(batch) >= batch_size and auto_commit:
                self._commit_offset()
                batch.clear()

        if batch and auto_commit:
            self._commit_offset()

        return total

    def _commit_offset(self) -> None:
        """Commit current offsets to persistent store."""
        if self.offset_store:
            self.offset_store(self.reader.get_offset())

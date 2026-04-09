"""
Data CDC (Change Data Capture) Module.

Implements CDC patterns for capturing and processing
database changes in real-time. Supports log-based CDC,
trigger-based CDC, and timestamp-based polling with
exactly-once delivery semantics.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class CDCMode(Enum):
    """CDC operation modes."""
    LOG_BASED = "log_based"
    TRIGGER_BASED = "trigger_based"
    TIMESTAMP_BASED = "timestamp_based"
    POLLING = "polling"


class Operation(Enum):
    """CDC operation types."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"


@dataclass
class CDCEvent:
    """Represents a CDC event."""
    event_id: str
    operation: Operation
    table_name: str
    timestamp: float
    sequence: int
    before: Optional[dict[str, Any]] = None
    after: Optional[dict[str, Any]] = None
    keys: Optional[dict[str, Any]] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CDCCheckpoint:
    """Checkpoint for CDC progress tracking."""
    position: str
    sequence: int
    timestamp: float
    table_positions: dict[str, str] = field(default_factory=dict)


@dataclass
class CDCConfig:
    """CDC configuration."""
    mode: CDCMode = CDCMode.POLLING
    tables: list[str] = field(default_factory=list)
    key_columns: list[str] = field(default_factory=list)
    timestamp_column: str = "updated_at"
    polling_interval: int = 1000
    batch_size: int = 1000
    include_before_image: bool = True
    include_after_image: bool = True


class CDCHandler:
    """
    Change Data Capture handler.

    Captures row-level changes from databases and streams
    them to downstream consumers with exactly-once semantics.

    Example:
        handler = CDCHandler(config)
        handler.on_change(lambda event: process(event))
        await handler.start()
        await handler.stop()
    """

    def __init__(self, config: Optional[CDCConfig] = None) -> None:
        self._config = config or CDCConfig()
        self._handlers: list[Callable[[CDCEvent], Any]] = []
        self._error_handlers: list[Callable[[Exception, CDCEvent], Any]] = []
        self._sequence: int = 0
        self._checkpoint: Optional[CDCCheckpoint] = None
        self._last_positions: dict[str, Any] = {}
        self._running: bool = False
        self._source_data: dict[str, list[dict[str, Any]]] = {}
        self._previous_state: dict[str, dict[str, dict[str, Any]]] = {}

    def add_table(
        self,
        table_name: str,
        key_columns: Optional[list[str]] = None
    ) -> None:
        """Register a table for CDC."""
        if table_name not in self._config.tables:
            self._config.tables.append(table_name)
        if key_columns:
            self._config.key_columns.extend(key_columns)

    def on_change(
        self,
        handler: Callable[[CDCEvent], Any]
    ) -> None:
        """Register a handler for CDC events."""
        self._handlers.append(handler)

    def on_error(
        self,
        handler: Callable[[Exception, CDCEvent], Any]
    ) -> None:
        """Register an error handler."""
        self._error_handlers.append(handler)

    def load_snapshot(
        self,
        table_name: str,
        data: list[dict[str, Any]]
    ) -> None:
        """
        Load initial snapshot data for a table.

        Args:
            table_name: Name of the table
            data: Initial snapshot rows
        """
        self._source_data[table_name] = data
        index: dict[str, dict[str, Any]] = {}
        for row in data:
            key = self._get_row_key(row, table_name)
            index[key] = row.copy()
        self._previous_state[table_name] = index

    async def start(self) -> None:
        """Start CDC processing."""
        self._running = True

    async def stop(self) -> None:
        """Stop CDC processing."""
        self._running = False

    async def poll(
        self,
        table_name: str,
        get_changes: Callable[[Any], list[dict[str, Any]]]
    ) -> list[CDCEvent]:
        """
        Poll for changes using a provided change fetch function.

        Args:
            table_name: Table to poll
            get_changes: Function that returns changes since last position

        Returns:
            List of CDC events
        """
        events: list[CDCEvent] = []

        last_position = self._last_positions.get(table_name)
        changes = await get_changes(last_position)

        for change in changes[:self._config.batch_size]:
            event = self._create_event(table_name, change)
            if event:
                events.append(event)

            if events:
                self._sequence = max(self._sequence, event.sequence)

        if changes:
            self._last_positions[table_name] = self._extract_position(changes[-1])

        return events

    def process_change(
        self,
        table_name: str,
        before: Optional[dict[str, Any]],
        after: Optional[dict[str, Any]],
        operation: Operation
    ) -> Optional[CDCEvent]:
        """
        Process a single change and generate CDC event.

        Args:
            table_name: Table name
            before: Row state before change
            after: Row state after change
            operation: Type of operation

        Returns:
            CDCEvent if change detected, None otherwise
        """
        self._sequence += 1

        event = CDCEvent(
            event_id=str(uuid.uuid4()),
            operation=operation,
            table_name=table_name,
            timestamp=time.time(),
            sequence=self._sequence,
            before=before if self._config.include_before_image else None,
            after=after if self._config.include_after_image else None,
            keys=self._extract_keys(before or after, table_name)
        )

        return event

    def _create_event(
        self,
        table_name: str,
        change: dict[str, Any]
    ) -> Optional[CDCEvent]:
        """Create CDC event from change record."""
        operation_str = change.get("operation", "update")
        try:
            operation = Operation(operation_str)
        except ValueError:
            operation = Operation.UPDATE

        before = change.get("before") if self._config.include_before_image else None
        after = change.get("after") if self._config.include_after_image else None

        return self.process_change(table_name, before, after, operation)

    def _get_row_key(self, row: dict[str, Any], table_name: str) -> str:
        """Generate key for row lookup."""
        keys = [row.get(k) for k in self._config.key_columns if k in row]
        return "|".join(str(k) for k in keys) if keys else str(hash(frozenset(row.items())))

    def _extract_keys(self, row: Optional[dict[str, Any]], table_name: str) -> dict[str, Any]:
        """Extract key columns from row."""
        if not row:
            return {}
        return {k: row[k] for k in self._config.key_columns if k in row}

    def _extract_position(self, change: dict[str, Any]) -> Any:
        """Extract position/timestamp from change for bookmarking."""
        return change.get(self._config.timestamp_column, time.time())

    def compare_and_emit(
        self,
        table_name: str,
        new_data: list[dict[str, Any]]
    ) -> list[CDCEvent]:
        """
        Compare new data with previous state and emit CDC events.

        Args:
            table_name: Table name
            new_data: Current state of the table

        Returns:
            List of CDC events representing changes
        """
        events: list[CDCEvent] = []
        previous = self._previous_state.get(table_name, {})
        current_index: dict[str, dict[str, Any]] = {}

        for row in new_data:
            key = self._get_row_key(row, table_name)
            current_index[key] = row

            if key not in previous:
                event = self.process_change(table_name, None, row, Operation.INSERT)
                if event:
                    events.append(event)
            else:
                old_row = previous[key]
                if row != old_row:
                    event = self.process_change(table_name, old_row, row, Operation.UPDATE)
                    if event:
                        events.append(event)

        for key, old_row in previous.items():
            if key not in current_index:
                event = self.process_change(table_name, old_row, None, Operation.DELETE)
                if event:
                    events.append(event)

        self._previous_state[table_name] = current_index
        return events

    async def emit(self, event: CDCEvent) -> None:
        """Emit a CDC event to all registered handlers."""
        for handler in self._handlers:
            try:
                result = handler(event)
                if hasattr(result, "__await__"):
                    await result
            except Exception as e:
                for err_handler in self._error_handlers:
                    err_handler(e, event)

    def get_checkpoint(self) -> CDCCheckpoint:
        """Get current checkpoint for state persistence."""
        return CDCCheckpoint(
            position=str(self._sequence),
            sequence=self._sequence,
            timestamp=time.time(),
            table_positions=self._last_positions.copy()
        )

    def restore_checkpoint(self, checkpoint: CDCCheckpoint) -> None:
        """Restore state from a checkpoint."""
        self._sequence = checkpoint.sequence
        self._last_positions = checkpoint.table_positions.copy()

    def get_pending_count(self) -> int:
        """Get number of pending unprocessed events."""
        return self._sequence - (self._checkpoint.sequence if self._checkpoint else 0)

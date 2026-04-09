"""Input Batch Controller Utilities.

Batches multiple input events for efficient processing.

Example:
    >>> from input_batch_controller_utils import InputBatchController
    >>> ctrl = InputBatchController(batch_size=10, flush_interval=0.5)
    >>> ctrl.add_event("click", {"x": 100, "y": 200})
    >>> ctrl.flush()
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional


@dataclass
class BatchEvent:
    """An input event in a batch."""
    event_type: str
    data: Dict[str, Any]
    timestamp: float


class InputBatchController:
    """Batches input events for efficient execution."""

    def __init__(
        self,
        batch_size: int = 10,
        flush_interval: float = 0.5,
        max_batch_size: int = 100,
    ):
        """Initialize batch controller.

        Args:
            batch_size: Target batch size before flush.
            flush_interval: Max seconds between flushes.
            max_batch_size: Hard limit on batch size.
        """
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_batch_size = max_batch_size
        self._batch: Deque[BatchEvent] = deque(maxlen=max_batch_size)
        self._last_flush = time.time()
        self._handler: Optional[Callable[[List[BatchEvent]], None]] = None

    def add_event(self, event_type: str, data: Optional[Dict[str, Any]] = None) -> bool:
        """Add an event to the current batch.

        Args:
            event_type: Type of input event.
            data: Event data.

        Returns:
            True if batch was flushed.
        """
        self._batch.append(BatchEvent(
            event_type=event_type,
            data=data or {},
            timestamp=time.time(),
        ))
        return self._check_flush()

    def _check_flush(self) -> bool:
        """Check if batch should be flushed.

        Returns:
            True if flushed.
        """
        should_flush = (
            len(self._batch) >= self.batch_size or
            time.time() - self._last_flush >= self.flush_interval
        )
        if should_flush:
            self.flush()
            return True
        return False

    def flush(self) -> List[BatchEvent]:
        """Flush the current batch.

        Returns:
            List of flushed events.
        """
        events = list(self._batch)
        self._batch.clear()
        self._last_flush = time.time()

        if events and self._handler:
            self._handler(events)

        return events

    def on_batch(self, handler: Callable[[List[BatchEvent]], None]) -> None:
        """Register batch handler.

        Args:
            handler: Called when a batch is flushed.
        """
        self._handler = handler

    def get_pending_count(self) -> int:
        """Get number of pending events.

        Returns:
            Count of events in batch.
        """
        return len(self._batch)

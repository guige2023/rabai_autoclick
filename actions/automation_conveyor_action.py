"""Automation Conveyor Action.

Implements a conveyor belt pattern for sequential task processing
with configurable speed, batching, pause/resume, and backpressure.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Any, Callable, Deque, Dict, List, Optional


class ConveyorState(Enum):
    """Conveyor operational states."""
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"
    DRAINING = "draining"


@dataclass
class ConveyorItem:
    """An item on the conveyor."""
    id: str
    data: Any
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    retries: int = 0


@dataclass
class ConveyorConfig:
    """Configuration for the conveyor belt."""
    max_batch_size: int = 10
    processing_timeout_sec: float = 60.0
    max_queue_size: int = 1000
    speed_multiplier: float = 1.0
    enable_backpressure: bool = True
    max_retries: int = 3


@dataclass
class ConveyorMetrics:
    """Conveyor performance metrics."""
    total_processed: int = 0
    total_failed: int = 0
    total_retried: int = 0
    current_queue_size: int = 0
    current_batch_size: int = 0
    avg_processing_time: float = 0.0
    throughput_per_sec: float = 0.0


class AutomationConveyorAction:
    """Conveyor belt for sequential batch processing."""

    def __init__(self, config: Optional[ConveyorConfig] = None) -> None:
        self.config = config or ConveyorConfig()
        self._input_queue: Deque[ConveyorItem] = deque()
        self._output_queue: Deque[ConveyorItem] = deque()
        self._processing: Optional[ConveyorItem] = None
        self._state = ConveyorState.STOPPED
        self._lock = Lock()
        self._metrics = ConveyorMetrics()
        self._processing_times: Deque[float] = deque(maxlen=1000)
        self._item_counter = 0
        self._last_throughput_check = time.time()
        self._items_since_last_check = 0

    def start(self) -> None:
        """Start the conveyor."""
        with self._lock:
            if self._state == ConveyorState.STOPPED:
                self._state = ConveyorState.RUNNING

    def stop(self) -> None:
        """Stop the conveyor."""
        with self._lock:
            self._state = ConveyorState.STOPPED

    def pause(self) -> None:
        """Pause the conveyor."""
        with self._lock:
            if self._state == ConveyorState.RUNNING:
                self._state = ConveyorState.PAUSED

    def resume(self) -> None:
        """Resume the conveyor."""
        with self._lock:
            if self._state == ConveyorState.PAUSED:
                self._state = ConveyorState.RUNNING

    def add_item(
        self,
        data: Any,
        item_id: Optional[str] = None,
    ) -> str:
        """Add an item to the conveyor input."""
        if self.config.enable_backpressure and len(self._input_queue) >= self.config.max_queue_size:
            raise RuntimeError("Input queue full, backpressure applied")

        self._item_counter += 1
        item_id = item_id or f"item_{self._item_counter}"

        item = ConveyorItem(
            id=item_id,
            data=data,
            created_at=time.time(),
        )

        self._input_queue.append(item)
        return item_id

    def add_items(self, items: List[Any]) -> List[str]:
        """Add multiple items to the conveyor."""
        item_ids = []
        for item in items:
            try:
                item_id = self.add_item(item)
                item_ids.append(item_id)
            except RuntimeError:
                break
        return item_ids

    def step(self, process_fn: Callable[[Any], Any]) -> Optional[Any]:
        """Execute one processing step. Returns result if completed."""
        with self._lock:
            if self._state != ConveyorState.RUNNING:
                return None

            if self._processing is None and self._input_queue:
                self._processing = self._input_queue.popleft()
                self._processing.started_at = time.time()

            if self._processing is None:
                return None

            item = self._processing
            elapsed = time.time() - item.started_at

            if elapsed > self.config.processing_timeout:
                if item.retries < self.config.max_retries:
                    item.retries += 1
                    self._metrics.total_retried += 1
                    item.started_at = time.time()
                else:
                    item.error = "Timeout"
                    item.completed_at = time.time()
                    self._output_queue.append(item)
                    self._processing = None
                    self._metrics.total_failed += 1

            return None

    def process_batch(
        self,
        process_fn: Callable[[List[Any]], List[Any]],
        batch_size: Optional[int] = None,
    ) -> List[Any]:
        """Process a batch of items through the conveyor."""
        bsize = batch_size or self.config.max_batch_size

        with self._lock:
            if self._state != ConveyorState.RUNNING:
                return []

            batch = []
            while len(batch) < bsize and self._input_queue:
                batch.append(self._input_queue.popleft())

            if not batch:
                return []

        start_time = time.time()
        try:
            results = process_fn([item.data for item in batch])
        except Exception as e:
            results = [None] * len(batch)
            for item in batch:
                item.error = str(e)

        processing_time = time.time() - start_time

        with self._lock:
            for i, item in enumerate(batch):
                item.completed_at = time.time()
                item.result = results[i] if i < len(results) else None
                self._output_queue.append(item)

            self._processing_times.append(processing_time)
            self._metrics.total_processed += len(batch)
            self._metrics.current_queue_size = len(self._input_queue)
            self._metrics.avg_processing_time = sum(self._processing_times) / len(self._processing_times)
            self._items_since_last_check += len(batch)

            now = time.time()
            elapsed = now - self._last_throughput_check
            if elapsed >= 1.0:
                self._metrics.throughput_per_sec = self._items_since_last_check / elapsed
                self._items_since_last_check = 0
                self._last_throughput_check = now

        return [item.result for item in batch if item.result is not None]

    def get_next_batch(self, batch_size: int) -> List[ConveyorItem]:
        """Get a batch of items without processing."""
        with self._lock:
            items = []
            for _ in range(min(batch_size, len(self._input_queue))):
                if self._input_queue:
                    items.append(self._input_queue.popleft())
            return items

    def drain(self) -> List[Any]:
        """Drain all items from the conveyor."""
        with self._lock:
            self._state = ConveyorState.DRAINING

        results = []
        while True:
            with self._lock:
                if not self._input_queue and not self._processing:
                    break

            time.sleep(0.01)

        with self._lock:
            while self._output_queue:
                item = self._output_queue.popleft()
                results.append(item.result)
            self._state = ConveyorState.STOPPED

        return results

    def get_pending_count(self) -> int:
        """Get count of pending items."""
        with self._lock:
            return len(self._input_queue)

    def get_completed_count(self) -> int:
        """Get count of completed items."""
        with self._lock:
            return len(self._output_queue)

    def get_metrics(self) -> ConveyorMetrics:
        """Get conveyor metrics."""
        with self._lock:
            return ConveyorMetrics(
                total_processed=self._metrics.total_processed,
                total_failed=self._metrics.total_failed,
                total_retried=self._metrics.total_retried,
                current_queue_size=len(self._input_queue),
                current_batch_size=len(self._input_queue),
                avg_processing_time=self._metrics.avg_processing_time,
                throughput_per_sec=self._metrics.throughput_per_sec,
            )

    def get_state(self) -> ConveyorState:
        """Get current conveyor state."""
        with self._lock:
            return self._state

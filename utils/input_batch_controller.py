"""
Input Batch Controller Module.

Provides utilities for managing batched input processing, including
input buffering, prioritization, coalescing, and rate limiting.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


logger = logging.getLogger(__name__)


class InputPriority(Enum):
    """Priority levels for input events."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


class InputType(Enum):
    """Types of input events."""
    MOUSE_MOVE = auto()
    MOUSE_DOWN = auto()
    MOUSE_UP = auto()
    MOUSE_CLICK = auto()
    MOUSE_DRAG = auto()
    KEY_DOWN = auto()
    KEY_UP = auto()
    KEY_PRESS = auto()
    TOUCH_START = auto()
    TOUCH_MOVE = auto()
    TOUCH_END = auto()
    SCROLL = auto()


@dataclass
class InputEvent:
    """Represents an input event."""
    input_type: InputType
    priority: InputPriority
    timestamp: float = field(default_factory=time.time)
    x: float = 0.0
    y: float = 0.0
    key_code: int | None = None
    button: int | None = None
    delta: float = 0.0
    pressure: float = 1.0
    touch_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other: InputEvent) -> bool:
        """Compare by priority for heap ordering."""
        return self.priority.value < other.priority.value


@dataclass
class BatchConfig:
    """Configuration for input batching."""
    max_batch_size: int = 100
    max_batch_delay_ms: float = 50.0
    enable_coalescing: bool = True
    enable_rate_limiting: bool = False
    max_events_per_second: float = 1000.0
    priority_mode: bool = True


class InputCoalescer:
    """
    Coalesces similar input events to reduce redundancy.

    Example:
        >>> coalescer = InputCoalescer()
        >>> coalesced = coalescer.coalesce(event_list)
    """

    def __init__(
        self,
        position_threshold: float = 2.0,
        time_threshold_ms: float = 100.0
    ) -> None:
        """
        Initialize the coalescer.

        Args:
            position_threshold: Distance threshold for position coalescing.
            time_threshold_ms: Time window for coalescing.
        """
        self.position_threshold = position_threshold
        self.time_threshold_ms = time_threshold_ms

    def coalesce(
        self,
        events: list[InputEvent]
    ) -> list[InputEvent]:
        """
        Coalesce a list of input events.

        Args:
            events: List of events to coalesce.

        Returns:
            Coalesced list of events.
        """
        if not events:
            return []

        coalesced: list[InputEvent] = []
        pending: dict[int, InputEvent] = {}

        for event in events:
            coalesced_event = self._try_coalesce(event, pending)
            if coalesced_event:
                pending[event.touch_id or event.key_code or 0] = coalesced_event
            else:
                coalesced.append(event)

        coalesced.extend(pending.values())
        coalesced.sort(key=lambda e: e.timestamp)

        return coalesced

    def _try_coalesce(
        self,
        event: InputEvent,
        pending: dict[int, InputEvent]
    ) -> InputEvent | None:
        """Try to coalesce event with pending events."""
        key = event.touch_id or event.key_code or 0

        if key not in pending:
            return None

        pending_event = pending[key]

        if pending_event.input_type != event.input_type:
            return None

        time_diff = (event.timestamp - pending_event.timestamp) * 1000
        if time_diff > self.time_threshold_ms:
            return None

        if event.input_type in {
            InputType.MOUSE_MOVE,
            InputType.TOUCH_MOVE
        }:
            dx = event.x - pending_event.x
            dy = event.y - pending_event.y
            distance = (dx * dx + dy * dy) ** 0.5

            if distance < self.position_threshold:
                pending_event.x = event.x
                pending_event.y = event.y
                pending_event.timestamp = event.timestamp
                return pending_event

        return None


class InputRateLimiter:
    """
    Rate limiter for input events.

    Ensures input events don't exceed configured rates.
    """

    def __init__(self, max_rate: float = 1000.0) -> None:
        """
        Initialize the rate limiter.

        Args:
            max_rate: Maximum events per second.
        """
        self.max_rate = max_rate
        self.min_interval = 1.0 / max_rate if max_rate > 0 else 0
        self._last_event_time: dict[int, float] = {}

    def should_allow(self, event: InputEvent) -> bool:
        """
        Check if an event should be allowed.

        Args:
            event: Input event to check.

        Returns:
            True if event is allowed.
        """
        key = event.touch_id or event.key_code or 0

        now = time.time()
        last_time = self._last_event_time.get(key, 0)

        if now - last_time >= self.min_interval:
            self._last_event_time[key] = now
            return True

        return False

    def get_wait_time(self, event: InputEvent) -> float:
        """
        Get wait time until event is allowed.

        Args:
            event: Input event.

        Returns:
            Seconds to wait.
        """
        key = event.touch_id or event.key_code or 0

        now = time.time()
        last_time = self._last_event_time.get(key, 0)
        elapsed = now - last_time

        wait_time = self.min_interval - elapsed
        return max(0.0, wait_time)

    def reset(self) -> None:
        """Reset rate limiter state."""
        self._last_event_time.clear()


class InputBatchController:
    """
    Main controller for batched input processing.

    Combines batching, coalescing, rate limiting, and prioritization.

    Example:
        >>> controller = InputBatchController(config)
        >>> controller.add_event(event)
        >>> batch = await controller.flush()
    """

    def __init__(self, config: BatchConfig | None = None) -> None:
        """
        Initialize the input batch controller.

        Args:
            config: Batch configuration.
        """
        self.config = config or BatchConfig()

        self._events: list[InputEvent] = []
        self._coalescer = InputCoalescer() if self.config.enable_coalescing else None
        self._rate_limiter = (
            InputRateLimiter(self.config.max_events_per_second)
            if self.config.enable_rate_limiting
            else None
        )

        self._lock = asyncio.Lock()
        self._last_flush_time = time.time()

    async def add_event(self, event: InputEvent) -> bool:
        """
        Add an event to the batch queue.

        Args:
            event: Input event to add.

        Returns:
            True if event was added immediately, False if batched.
        """
        async with self._lock:
            if self._rate_limiter and not self._rate_limiter.should_allow(event):
                logger.debug("Event rate limited")
                return False

            self._events.append(event)

            if len(self._events) >= self.config.max_batch_size:
                await self.flush()

            return True

    async def add_events(self, events: list[InputEvent]) -> int:
        """
        Add multiple events to the batch queue.

        Args:
            events: Events to add.

        Returns:
            Number of events added.
        """
        added = 0
        for event in events:
            if await self.add_event(event):
                added += 1
        return added

    async def flush(self) -> list[InputEvent]:
        """
        Flush the current batch.

        Returns:
            List of batched events.
        """
        async with self._lock:
            if not self._events:
                return []

            batch = self._events.copy()
            self._events.clear()
            self._last_flush_time = time.time()

            if self._coalescer:
                batch = self._coalescer.coalesce(batch)

            if self.config.priority_mode:
                batch.sort(key=lambda e: (e.priority.value, e.timestamp))

            logger.debug(f"Flushed batch of {len(batch)} events")
            return batch

    async def wait_for_batch(self) -> list[InputEvent]:
        """
        Wait for a full batch or timeout.

        Returns:
            Batched events.
        """
        max_wait = self.config.max_batch_delay_ms / 1000.0
        start = time.time()

        while len(self._events) == 0:
            if time.time() - start >= max_wait:
                return await self.flush()
            await asyncio.sleep(0.001)

        if len(self._events) >= self.config.max_batch_size:
            return await self.flush()

        return await self.flush()

    def pending_count(self) -> int:
        """Get number of pending events."""
        return len(self._events)

    def should_auto_flush(self) -> bool:
        """
        Check if automatic flush should occur.

        Returns:
            True if batch should be flushed.
        """
        if len(self._events) >= self.config.max_batch_size:
            return True

        elapsed = (time.time() - self._last_flush_time) * 1000
        if elapsed >= self.config.max_batch_delay_ms and self._events:
            return True

        return False


class InputBatcher:
    """
    Batches input events with configurable processing.

    Example:
        >>> batcher = InputBatcher(processor_func)
        >>> batcher.start()
    """

    def __init__(
        self,
        process_func: Callable[[list[InputEvent]], Any],
        config: BatchConfig | None = None
    ) -> None:
        """
        Initialize the input batcher.

        Args:
            process_func: Function to process batches.
            config: Batch configuration.
        """
        self.process_func = process_func
        self.config = config or BatchConfig()

        self._controller = InputBatchController(self.config)
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start batch processing."""
        self._running = True
        self._task = asyncio.create_task(self._process_loop())
        logger.info("Input batcher started")

    async def stop(self) -> None:
        """Stop batch processing."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        await self._controller.flush()
        logger.info("Input batcher stopped")

    async def add(self, event: InputEvent) -> bool:
        """
        Add an event to be batched.

        Args:
            event: Input event.

        Returns:
            True if added.
        """
        return await self._controller.add_event(event)

    async def _process_loop(self) -> None:
        """Main processing loop."""
        while self._running:
            try:
                batch = await self._controller.wait_for_batch()

                if batch:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        self.process_func,
                        batch
                    )
                    logger.debug(f"Processed batch: {len(batch)} events")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processing error: {e}")


@dataclass
class InputMetrics:
    """Metrics for input batch processing."""
    total_events: int = 0
    total_batches: int = 0
    total_coalesced: int = 0
    total_rate_limited: int = 0
    avg_batch_size: float = 0.0
    avg_batch_delay_ms: float = 0.0


class InputBatchMetrics:
    """
    Collects and reports metrics for input batching.

    Example:
        >>> metrics = InputBatchMetrics()
        >>> metrics.record_batch(10)
        >>> stats = metrics.get_stats()
    """

    def __init__(self) -> None:
        """Initialize the metrics collector."""
        self._total_events: int = 0
        self._total_batches: int = 0
        self._coalesced_count: int = 0
        self._rate_limited_count: int = 0
        self._batch_sizes: list[int] = []
        self._batch_delays: list[float] = []

    def record_batch(
        self,
        batch_size: int,
        delay_ms: float
    ) -> None:
        """
        Record a batch flush.

        Args:
            batch_size: Number of events in batch.
            delay_ms: Time since last batch.
        """
        self._total_events += batch_size
        self._total_batches += 1
        self._batch_sizes.append(batch_size)
        self._batch_delays.append(delay_ms)

        if len(self._batch_sizes) > 1000:
            self._batch_sizes.pop(0)
            self._batch_delays.pop(0)

    def record_coalesced(self, count: int) -> None:
        """
        Record coalesced events.

        Args:
            count: Number of events coalesced.
        """
        self._coalesced_count += count

    def record_rate_limited(self, count: int) -> None:
        """
        Record rate-limited events.

        Args:
            count: Number of events rate limited.
        """
        self._rate_limited_count += count

    def get_metrics(self) -> InputMetrics:
        """
        Get current metrics.

        Returns:
            InputMetrics with current values.
        """
        avg_size = (
            sum(self._batch_sizes) / len(self._batch_sizes)
            if self._batch_sizes
            else 0.0
        )

        avg_delay = (
            sum(self._batch_delays) / len(self._batch_delays)
            if self._batch_delays
            else 0.0
        )

        return InputMetrics(
            total_events=self._total_events,
            total_batches=self._total_batches,
            total_coalesced=self._coalesced_count,
            total_rate_limited=self._rate_limited_count,
            avg_batch_size=avg_size,
            avg_batch_delay_ms=avg_delay
        )

    def reset(self) -> None:
        """Reset all metrics."""
        self._total_events = 0
        self._total_batches = 0
        self._coalesced_count = 0
        self._rate_limited_count = 0
        self._batch_sizes.clear()
        self._batch_delays.clear()

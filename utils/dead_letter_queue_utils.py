"""
Dead Letter Queue (DLQ) Management.

Provides a robust dead letter queue implementation for handling failed
messages, with support for retry policies, alerting, and dead letter
processing workflows.

Example:
    >>> dlq = DeadLetterQueue(max_retries=3)
    >>> try:
    ...     process_message(msg)
    ... except Exception as e:
    ...     dlq.handle_failure(msg, e)
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class FailureReason(Enum):
    """Categorized failure reasons."""
    PROCESSING_ERROR = "processing_error"
    VALIDATION_ERROR = "validation_error"
    TIMEOUT = "timeout"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
    QUOTA_EXCEEDED = "quota_exceeded"
    UNKNOWN = "unknown"


@dataclass
class FailedMessage:
    """Represents a failed message in the DLQ."""
    message_id: str
    payload: Any
    failure_reason: FailureReason
    error_message: str
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0
    original_topic: str = ""
    original_partition: int = -1
    original_offset: int = -1
    metadata: dict[str, Any] = field(default_factory=dict)
    last_retry_time: float = 0.0


@dataclass
class DLQConfig:
    """Configuration for dead letter queue behavior."""
    max_retries: int = 3
    retry_delay_seconds: float = 60.0
    exponential_backoff: bool = True
    backoff_multiplier: float = 2.0
    max_retry_delay: float = 3600.0
    storage_path: Optional[str] = None


@dataclass
class DLQStats:
    """Statistics for DLQ monitoring."""
    total_failures: int = 0
    retried_messages: int = 0
    dead_lettered: int = 0
    recovered_messages: int = 0
    by_reason: dict[FailureReason, int] = field(default_factory=dict)


class DeadLetterQueue:
    """
    Dead Letter Queue implementation for message processing failures.

    Stores failed messages, manages retry logic, and provides
    mechanisms for processing dead letters.
    """

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay_seconds: float = 60.0,
        exponential_backoff: bool = True,
        backoff_multiplier: float = 2.0,
        max_retry_delay: float = 3600.0,
        storage_path: Optional[str] = None
    ):
        """
        Initialize the dead letter queue.

        Args:
            max_retries: Maximum retry attempts before dead-lettering
            retry_delay_seconds: Base delay between retries
            exponential_backoff: Use exponential backoff for retries
            backoff_multiplier: Multiplier for exponential backoff
            max_retry_delay: Maximum delay between retries
            storage_path: Path to persist failed messages
        """
        self._config = DLQConfig(
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            exponential_backoff=exponential_backoff,
            backoff_multiplier=backoff_multiplier,
            max_retry_delay=max_retry_delay,
            storage_path=storage_path
        )
        self._queue: list[FailedMessage] = []
        self._lock = threading.Lock()
        self._stats = DLQStats()
        self._processors: list[Callable[[FailedMessage], bool]] = []

    def handle_failure(
        self,
        message_id: str,
        payload: Any,
        error: Exception,
        original_topic: str = "",
        metadata: Optional[dict[str, Any]] = None
    ) -> bool:
        """
        Handle a message processing failure.

        Args:
            message_id: Unique identifier for the message
            payload: Message content
            error: The exception that occurred
            original_topic: Source topic of the message
            metadata: Additional message metadata

        Returns:
            True if message should be retried, False if dead-lettered
        """
        reason = self._classify_error(error)

        with self._lock:
            existing = self._find_message(message_id)
            if existing:
                return self._retry_message(existing)

            failed_msg = FailedMessage(
                message_id=message_id,
                payload=payload,
                failure_reason=reason,
                error_message=str(error),
                original_topic=original_topic,
                metadata=metadata or {}
            )

            self._queue.append(failed_msg)
            self._stats.total_failures += 1
            self._increment_reason_count(reason)

            if self._should_retry(failed_msg):
                return True
            else:
                self._stats.dead_lettered += 1
                self._notify_processors(failed_msg)
                return False

    def _classify_error(self, error: Exception) -> FailureReason:
        """Classify an error into a failure reason category."""
        error_msg = str(error).lower()

        if "timeout" in error_msg:
            return FailureReason.TIMEOUT
        elif "validation" in error_msg or "invalid" in error_msg:
            return FailureReason.VALIDATION_ERROR
        elif "quota" in error_msg or "limit" in error_msg:
            return FailureReason.QUOTA_EXCEEDED
        elif "connection" in error_msg or "unavailable" in error_msg:
            return FailureReason.DEPENDENCY_UNAVAILABLE
        elif "memory" in error_msg or "resource" in error_msg:
            return FailureReason.RESOURCE_EXHAUSTED
        else:
            return FailureReason.PROCESSING_ERROR

    def _find_message(self, message_id: str) -> Optional[FailedMessage]:
        """Find a message in the queue by ID."""
        for msg in self._queue:
            if msg.message_id == message_id:
                return msg
        return None

    def _should_retry(self, msg: FailedMessage) -> bool:
        """Determine if a message should be retried."""
        return msg.retry_count < self._config.max_retries

    def _retry_message(self, msg: FailedMessage) -> bool:
        """Handle retry for an existing failed message."""
        msg.retry_count += 1
        msg.last_retry_time = time.time()
        self._stats.retried_messages += 1

        if msg.retry_count >= self._config.max_retries:
            self._stats.dead_lettered += 1
            self._notify_processors(msg)
            self._queue.remove(msg)
            return False

        return True

    def _get_retry_delay(self, msg: FailedMessage) -> float:
        """Calculate the retry delay for a message."""
        if self._config.exponential_backoff:
            delay = self._config.retry_delay_seconds * (
                self._config.backoff_multiplier ** (msg.retry_count - 1)
            )
            return min(delay, self._config.max_retry_delay)
        return self._config.retry_delay_seconds

    def _increment_reason_count(self, reason: FailureReason) -> None:
        """Increment failure count for a reason category."""
        if reason not in self._stats.by_reason:
            self._stats.by_reason[reason] = 0
        self._stats.by_reason[reason] += 1

    def _notify_processors(self, msg: FailedMessage) -> None:
        """Notify registered processors of dead-lettered message."""
        for processor in self._processors:
            try:
                processor(msg)
            except Exception:
                pass

    def register_processor(self, processor: Callable[[FailedMessage], bool]) -> None:
        """
        Register a processor for dead letter handling.

        Args:
            processor: Function that processes dead letters
                      Returns True if message was recovered
        """
        with self._lock:
            self._processors.append(processor)

    def get_pending_messages(self, limit: Optional[int] = None) -> list[FailedMessage]:
        """
        Get messages pending retry.

        Args:
            limit: Maximum number of messages to return

        Returns:
            List of pending messages
        """
        with self._lock:
            messages = [
                msg for msg in self._queue
                if self._should_retry(msg) and
                (time.time() - msg.last_retry_time) >= self._get_retry_delay(msg)
            ]
            return messages[:limit] if limit else messages

    def get_dead_lettered_messages(
        self,
        reason: Optional[FailureReason] = None,
        limit: Optional[int] = None
    ) -> list[FailedMessage]:
        """
        Get all dead-lettered messages.

        Args:
            reason: Filter by failure reason
            limit: Maximum number to return

        Returns:
            List of dead-lettered messages
        """
        with self._lock:
            messages = [
                msg for msg in self._queue
                if msg.retry_count >= self._config.max_retries
            ]
            if reason:
                messages = [m for m in messages if m.failure_reason == reason]
            return messages[:limit] if limit else messages

    def requeue(self, message_id: str) -> bool:
        """
        Requeue a dead-lettered message for retry.

        Args:
            message_id: ID of message to requeue

        Returns:
            True if message was requeued
        """
        with self._lock:
            for msg in self._queue:
                if msg.message_id == message_id and msg.retry_count >= self._config.max_retries:
                    msg.retry_count = 0
                    self._stats.dead_lettered -= 1
                    return True
        return False

    def remove(self, message_id: str) -> bool:
        """
        Remove a message from the queue.

        Args:
            message_id: ID of message to remove

        Returns:
            True if message was removed
        """
        with self._lock:
            for i, msg in enumerate(self._queue):
                if msg.message_id == message_id:
                    self._queue.pop(i)
                    return True
        return False

    def get_stats(self) -> DLQStats:
        """Get DLQ statistics."""
        with self._lock:
            return DLQStats(
                total_failures=self._stats.total_failures,
                retried_messages=self._stats.retried_messages,
                dead_lettered=self._stats.dead_lettered,
                recovered_messages=self._stats.recovered_messages,
                by_reason=dict(self._stats.by_reason)
            )

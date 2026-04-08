"""
Guardian/watchdog utilities for monitoring thread health.

Provides watchdog timers, heartbeat monitoring,
and process/thread liveness detection.
"""

from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class WatchdogEvent:
    """Watchdog timeout event."""
    thread_name: str
    timeout_seconds: float
    timestamp: float = field(default_factory=time.time)


class Watchdog:
    """
    Watchdog timer for monitoring task execution.

    Tasks must check in before timeout expires.
    """

    def __init__(
        self,
        timeout_seconds: float,
        on_timeout: Callable[[WatchdogEvent], None] | None = None,
    ):
        self.timeout_seconds = timeout_seconds
        self.on_timeout = on_timeout
        self._last_checkin: float = field(default_factory=time.time)
        self._lock = threading.Lock()
        self._enabled = True

    def checkin(self) -> None:
        """Check in to prevent timeout."""
        with self._lock:
            self._last_checkin = time.time()

    def is_healthy(self) -> bool:
        """Check if still healthy (within timeout)."""
        with self._lock:
            elapsed = time.time() - self._last_checkin
            return elapsed < self.timeout_seconds

    def check(self) -> WatchdogEvent | None:
        """Check and trigger callback if timeout."""
        with self._lock:
            elapsed = time.time() - self._last_checkin
            if elapsed >= self.timeout_seconds and self._enabled:
                event = WatchdogEvent(
                    thread_name=threading.current_thread().name,
                    timeout_seconds=self.timeout_seconds,
                )
                if self.on_timeout:
                    self.on_timeout(event)
                return event
        return None

    def disable(self) -> None:
        self._enabled = False

    def enable(self) -> None:
        self._enabled = True


class HeartbeatMonitor:
    """
    Monitor heartbeats from multiple threads/processes.

    Tracks last heartbeat time for each monitored entity.
    """

    def __init__(
        self,
        timeout_seconds: float = 30.0,
        on_timeout: Callable[[str], None] | None = None,
    ):
        self.timeout_seconds = timeout_seconds
        self.on_timeout = on_timeout
        self._lock = threading.Lock()
        self._heartbeats: dict[str, float] = {}

    def register(self, entity_id: str) -> None:
        """Register an entity for monitoring."""
        with self._lock:
            self._heartbeats[entity_id] = time.time()

    def heartbeat(self, entity_id: str) -> None:
        """Record heartbeat from entity."""
        with self._lock:
            self._heartbeats[entity_id] = time.time()

    def unregister(self, entity_id: str) -> None:
        """Stop monitoring entity."""
        with self._lock:
            self._heartbeats.pop(entity_id, None)

    def is_alive(self, entity_id: str) -> bool:
        """Check if entity is still sending heartbeats."""
        with self._lock:
            last = self._heartbeats.get(entity_id)
            if last is None:
                return False
            return time.time() - last < self.timeout_seconds

    def get_dead_entities(self) -> list[str]:
        """Get list of entities that have timed out."""
        now = time.time()
        with self._lock:
            return [
                eid for eid, last in self._heartbeats.items()
                if now - last >= self.timeout_seconds
            ]

    def check_all(self) -> list[str]:
        """Check all entities, trigger callbacks for dead ones."""
        dead = self.get_dead_entities()
        for entity_id in dead:
            if self.on_timeout:
                self.on_timeout(entity_id)
            self.unregister(entity_id)
        return dead

    @property
    def monitored_count(self) -> int:
        return len(self._heartbeats)


class ThreadGuardian:
    """
    Guardian that monitors thread health and restarts if needed.

    Useful for keeping background workers alive.
    """

    def __init__(
        self,
        target: Callable[..., None],
        args: tuple = (),
        restart_delay: float = 1.0,
        max_restarts: int = 0,
    ):
        self.target = target
        self.args = args
        self.restart_delay = restart_delay
        self.max_restarts = max_restarts

        self._thread: threading.Thread | None = None
        self._running = False
        self._restart_count = 0
        self._exception: Exception | None = None
        self._traceback_str: str | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the guarded thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while self._running:
            try:
                self.target(*self.args)
            except Exception as e:
                with self._lock:
                    self._exception = e
                    self._traceback_str = traceback.format_exc()
                    self._restart_count += 1
                    if self.max_restarts and self._restart_count >= self.max_restarts:
                        self._running = False
                        break
            if self._running:
                time.sleep(self.restart_delay)
                if self._running:
                    self._thread = threading.Thread(target=self._run, daemon=True)
                    self._thread.start()
                    break

    def stop(self) -> None:
        """Stop the guarded thread."""
        with self._lock:
            self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def get_stats(self) -> dict:
        """Get guardian statistics."""
        with self._lock:
            return {
                "running": self._running,
                "restart_count": self._restart_count,
                "has_exception": self._exception is not None,
                "exception": str(self._exception) if self._exception else None,
            }

"""
Signal handling utilities for graceful shutdown.

Provides signal handler registration, graceful shutdown
coordination, and cleanup hooks.
"""

from __future__ import annotations

import signal
import threading
import time
from typing import Callable


class SignalHandler:
    """
    Coordinated signal handler for graceful shutdown.

    Registers handlers for SIGINT, SIGTERM, and custom signals.
    """

    def __init__(self):
        self._shutdown_callbacks: list[Callable[[], None]] = []
        self._force_callbacks: list[Callable[[], None]] = []
        self._lock = threading.Lock()
        self._shutdown_requested = threading.Event()
        self._force_shutdown_requested = threading.Event()
        self._original_handlers: dict[int, signal.Handler | None] = {}
        self._registered = False

    def register(
        self,
        signals: list[int] | None = None,
    ) -> None:
        """
        Register signal handlers.

        Args:
            signals: List of signals (defaults to SIGINT, SIGTERM)
        """
        if signals is None:
            signals = [signal.SIGINT, signal.SIGTERM]

        with self._lock:
            if self._registered:
                return

            for sig in signals:
                self._original_handlers[sig] = signal.signal(sig, self._handler)

            self._registered = True

    def _handler(self, signum: int, frame) -> None:
        if signum == signal.SIGINT or signum == signal.SIGTERM:
            if not self._shutdown_requested.is_set():
                self._shutdown_requested.set()
                self._run_shutdown()
            elif not self._force_shutdown_requested.is_set():
                self._force_shutdown_requested.set()
                self._run_force_shutdown()

    def _run_shutdown(self) -> None:
        callbacks = list(self._shutdown_callbacks)
        for cb in callbacks:
            try:
                cb()
            except Exception:
                pass

    def _run_force_shutdown(self) -> None:
        callbacks = list(self._force_callbacks)
        for cb in callbacks:
            try:
                cb()
            except Exception:
                pass

    def on_shutdown(self, callback: Callable[[], None]) -> None:
        """Register shutdown callback."""
        with self._lock:
            self._shutdown_callbacks.append(callback)

    def on_force_shutdown(self, callback: Callable[[], None]) -> None:
        """Register force shutdown callback."""
        with self._lock:
            self._force_callbacks.append(callback)

    def wait_for_shutdown(self, timeout: float | None = None) -> bool:
        """
        Wait for shutdown signal.

        Args:
            timeout: Max wait time

        Returns:
            True if shutdown was requested
        """
        return self._shutdown_requested.wait(timeout)

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested.is_set()

    @property
    def force_shutdown_requested(self) -> bool:
        return self._force_shutdown_requested.is_set()

    def unregister(self) -> None:
        """Restore original signal handlers."""
        with self._lock:
            for sig, handler in self._original_handlers.items():
                if handler is not None:
                    signal.signal(sig, handler)
            self._original_handlers.clear()
            self._registered = False


class GracefulShutdown:
    """
    Context manager for graceful shutdown.

    Ensures cleanup code runs on shutdown.
    """

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self._handler = SignalHandler()
        self._done = threading.Event()

    def __enter__(self) -> "GracefulShutdown":
        self._handler.register()
        return self

    def __exit__(self, *args: object) -> None:
        self._handler.unregister()

    def register_cleanup(self, callback: Callable[[], None]) -> None:
        self._handler.on_shutdown(callback)

    def wait(self) -> None:
        self._handler.wait_for_shutdown(self.timeout)


class ShutdownCoordinator:
    """
    Coordinate shutdown across multiple components.

    Tracks shutdown state and ensures all components
    have completed before process exit.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._pending: set[str] = set()
        self._completed: set[str] = set()
        self._notified = threading.Event()
        self._all_done = threading.Event()

    def register_component(self, name: str) -> None:
        """Register a component that must complete shutdown."""
        with self._lock:
            self._pending.add(name)

    def complete_component(self, name: str) -> None:
        """Mark component as completed shutdown."""
        with self._lock:
            self._pending.discard(name)
            self._completed.add(name)
            self._notified.set()
            if not self._pending:
                self._all_done.set()

    def wait_all(self, timeout: float | None = None) -> bool:
        """Wait for all components to complete shutdown."""
        return self._all_done.wait(timeout)

    def is_complete(self) -> bool:
        return not bool(self._pending)

    @property
    def pending_components(self) -> set[str]:
        return set(self._pending)

    @property
    def completed_components(self) -> set[str]:
        return set(self._completed)


_global_handler: SignalHandler | None = None
_global_lock = threading.Lock()


def get_global_handler() -> SignalHandler:
    """Get or create global signal handler."""
    global _global_handler
    with _global_lock:
        if _global_handler is None:
            _global_handler = SignalHandler()
            _global_handler.register()
        return _global_handler


def on_shutdown(callback: Callable[[], None]) -> None:
    """Register global shutdown callback."""
    get_global_handler().on_shutdown(callback)

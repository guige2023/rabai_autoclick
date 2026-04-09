"""
Signal handler utilities for automation workflow control.

Provides robust signal handling for graceful shutdown, reload,
and inter-process communication in automation scripts.

Example:
    >>> from signal_handler_utils import SignalHandler, register_handler
    >>> handler = SignalHandler()
    >>> handler.on_signal(signal.SIGTERM, graceful_shutdown)
    >>> handler.start()
"""

from __future__ import annotations

import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


# =============================================================================
# Types
# =============================================================================


class SignalType(Enum):
    """Signal types we handle."""
    SIGINT = signal.SIGINT
    SIGTERM = signal.SIGTERM
    SIGHUP = signal.SIGHUP
    SIGUSR1 = signal.SIGUSR1
    SIGUSR2 = signal.SIGUSR2


@dataclass
class SignalEvent:
    """An event representing a received signal."""
    signal_num: int
    signal_name: str
    timestamp: float
    sender_pid: Optional[int] = None


@dataclass
class HandlerRegistration:
    """A registered signal handler."""
    signal_num: int
    handler: Callable[[SignalEvent], None]
    once: bool = False
    call_count: int = 0


# =============================================================================
# Signal Handler
# =============================================================================


class SignalHandler:
    """
    Central signal handler for automation workflows.

    Example:
        >>> handler = SignalHandler()
        >>> handler.on_sigterm(lambda e: save_state_and_exit())
        >>> handler.on_sighup(lambda e: reload_config())
        >>> handler.start()
    """

    def __init__(self):
        self._handlers: Dict[int, HandlerRegistration] = {}
        self._lock = threading.RLock()
        self._event_log: List[SignalEvent] = []
        self._running = False
        self._original_handlers: Dict[int, Any] = {}
        self._signal_queue: List[SignalEvent] = []
        self._queue_lock = threading.Lock()

    def on_signal(
        self,
        signal_num: int,
        handler: Callable[[SignalEvent], None],
        once: bool = False,
    ) -> None:
        """
        Register a handler for a signal.

        Args:
            signal_num: Signal number (e.g., signal.SIGTERM).
            handler: Function to call when signal is received.
            once: If True, handler is called only once then removed.
        """
        with self._lock:
            self._handlers[signal_num] = HandlerRegistration(
                signal_num=signal_num,
                handler=handler,
                once=once,
            )

    def on_sigterm(self, handler: Callable[[SignalEvent], None]) -> None:
        """Shortcut for SIGTERM handler."""
        self.on_signal(signal.SIGTERM, handler)

    def on_sigint(self, handler: Callable[[SignalEvent], None]) -> None:
        """Shortcut for SIGINT handler."""
        self.on_signal(signal.SIGINT, handler)

    def on_sighup(self, handler: Callable[[SignalEvent], None]) -> None:
        """Shortcut for SIGHUP handler."""
        self.on_signal(signal.SIGHUP, handler)

    def on_usr1(self, handler: Callable[[SignalEvent], None]) -> None:
        """Shortcut for SIGUSR1 handler."""
        self.on_signal(signal.SIGUSR1, handler)

    def on_usr2(self, handler: Callable[[SignalEvent], None]) -> None:
        """Shortcut for SIGUSR2 handler."""
        self.on_signal(signal.SIGUSR2, handler)

    def start(self) -> None:
        """Start handling signals."""
        self._running = True

        for sig_num in [signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGUSR1, signal.SIGUSR2]:
            self._original_handlers[sig_num] = signal.getsignal(sig_num)
            signal.signal(sig_num, self._handle_signal)

    def stop(self) -> None:
        """Stop handling signals and restore original handlers."""
        self._running = False

        for sig_num, handler in self._original_handlers.items():
            signal.signal(sig_num, handler)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Internal signal handler."""
        if not self._running:
            return

        event = SignalEvent(
            signal_num=signum,
            signal_name=signal.Signals(signum).name,
            timestamp=time.monotonic(),
        )

        with self._queue_lock:
            self._signal_queue.append(event)

        with self._lock:
            registration = self._handlers.get(signum)

        if registration:
            registration.call_count += 1
            try:
                registration.handler(event)
            except Exception as e:
                print(f"Signal handler error: {e}", file=sys.stderr)

            if registration.once:
                del self._handlers[signum]

        with self._lock:
            self._event_log.append(event)

    def get_recent_events(self, max_count: int = 100) -> List[SignalEvent]:
        """Get recent signal events."""
        with self._lock:
            return list(self._event_log[-max_count:])

    def get_call_count(self, signal_num: int) -> int:
        """Get how many times a signal handler was called."""
        with self._lock:
            reg = self._handlers.get(signal_num)
            return reg.call_count if reg else 0

    def clear_events(self) -> None:
        """Clear the event log."""
        with self._lock:
            self._event_log.clear()

    def __enter__(self) -> "SignalHandler":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


# =============================================================================
# Graceful Shutdown Handler
# =============================================================================


class GracefulShutdown:
    """
    Manages graceful shutdown with cleanup callbacks.

    Example:
        >>> shutdown = GracefulShutdown()
        >>> shutdown.add_callback(lambda: close_database())
        >>> shutdown.add_callback(lambda: save_state())
        >>> shutdown.on_signal(signal.SIGTERM)
    """

    def __init__(self):
        self._callbacks: List[Callable] = []
        self._running = False
        self._lock = threading.Lock()
        self._signal_handler = SignalHandler()

    def add_callback(self, callback: Callable[[], None]) -> None:
        """
        Add a cleanup callback.

        Callbacks are executed in reverse order (LIFO) on shutdown.
        """
        with self._lock:
            self._callbacks.append(callback)

    def remove_callback(self, callback: Callable) -> None:
        """Remove a cleanup callback."""
        with self._lock:
            self._callbacks.remove(callback)

    def execute(self) -> None:
        """Execute all cleanup callbacks."""
        with self._lock:
            callbacks = list(reversed(self._callbacks))

        for callback in callbacks:
            try:
                callback()
            except Exception as e:
                print(f"Cleanup callback error: {e}", file=sys.stderr)

    def on_signal(self, signum: int) -> None:
        """Register to handle a signal with graceful shutdown."""
        self._signal_handler.on_signal(signum, lambda e: self.trigger())

    def trigger(self) -> None:
        """Trigger graceful shutdown."""
        print("Initiating graceful shutdown...")
        self.execute()
        sys.exit(0)

    def start(self) -> None:
        """Start handling shutdown signals."""
        self._signal_handler.on_sigterm(lambda e: self.trigger())
        self._signal_handler.on_sigint(lambda e: self.trigger())
        self._signal_handler.start()

    def stop(self) -> None:
        """Stop handling signals."""
        self._signal_handler.stop()


# =============================================================================
# Inter-Process Signals
# =============================================================================


def send_signal(pid: int, signal_num: int) -> bool:
    """
    Send a signal to a process.

    Args:
        pid: Target process ID.
        signal_num: Signal number to send.

    Returns:
        True if signal was sent successfully.
    """
    try:
        os.kill(pid, signal_num)
        return True
    except OSError:
        return False


def send_sigterm(pid: int) -> bool:
    """Send SIGTERM to a process."""
    return send_signal(pid, signal.SIGTERM)


def send_sigint(pid: int) -> bool:
    """Send SIGINT to a process."""
    return send_signal(pid, signal.SIGINT)


def send_sighup(pid: int) -> bool:
    """Send SIGHUP to a process."""
    return send_signal(pid, signal.SIGHUP)


def send_sigusr1(pid: int) -> bool:
    """Send SIGUSR1 to a process."""
    return send_signal(pid, signal.SIGUSR1)


def send_sigusr2(pid: int) -> bool:
    """Send SIGUSR2 to a process."""
    return send_signal(pid, signal.SIGUSR2)


# =============================================================================
# Signal-Aware Utilities
# =============================================================================


import os


class InterruptibleSleep:
    """
    Sleep that can be interrupted by a signal.

    Example:
        >>> sleep = InterruptibleSleep()
        >>> sleep.sleep(60)  # wakes on SIGTERM/SIGINT
    """

    def __init__(self):
        self._interrupted = False
        self._handler = SignalHandler()
        self._handler.on_sigterm(lambda e: self._interrupt())
        self._handler.on_sigint(lambda e: self._interrupt())

    def _interrupt(self) -> None:
        self._interrupted = True

    def sleep(self, seconds: float) -> float:
        """
        Sleep for seconds, returning early if interrupted.

        Returns:
            Actual time slept.
        """
        self._interrupted = False
        self._handler.start()

        start = time.monotonic()
        remaining = seconds

        while remaining > 0 and not self._interrupted:
            time.sleep(min(remaining, 0.1))
            remaining = seconds - (time.monotonic() - start)

        self._handler.stop()
        return time.monotonic() - start


def wait_for_signal(
    signal_num: int,
    timeout: Optional[float] = None,
) -> bool:
    """
    Wait for a specific signal.

    Args:
        signal_num: Signal to wait for.
        timeout: Maximum seconds to wait.

    Returns:
        True if signal was received within timeout.
    """
    event = threading.Event()

    def handler(e: SignalEvent) -> None:
        event.set()

    handler_obj = SignalHandler()
    handler_obj.on_signal(signal_num, handler)
    handler_obj.start()

    result = event.wait(timeout=timeout)

    handler_obj.stop()

    return result


# =============================================================================
# Decorator
# =============================================================================


def handle_signals(
    sigterm: Optional[Callable] = None,
    sigint: Optional[Callable] = None,
    sighup: Optional[Callable] = None,
) -> Callable:
    """
    Decorator to add signal handling to a function.

    Example:
        >>> @handle_signals(sigterm=cleanup, sigint=cleanup)
        >>> def main_loop():
        ...     while True:
        ...         pass
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            handler = SignalHandler()
            if sigterm:
                handler.on_sigterm(sigterm)
            if sigint:
                handler.on_sigint(sigint)
            if sighup:
                handler.on_sighup(sighup)

            handler.start()
            try:
                return func(*args, **kwargs)
            finally:
                handler.stop()

        return wrapper

    return decorator

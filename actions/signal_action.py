"""signal_action module for rabai_autoclick.

Provides Unix signal handling utilities: signal listeners,
signal-safe operations, signal masks, and signal coordination.
"""

from __future__ import annotations

import os
import signal
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

__all__ = [
    "SignalHandler",
    "SignalBlocker",
    "SignalWaiter",
    "SignalMonitor",
    "register_handler",
    "ignore_signal",
    "default_handler",
    "block_signals",
    "wait_for_signal",
    "raised_signals",
    "SignalSet",
    "get_signal_name",
    "get_signal_number",
    "SigNum",
    "SigType",
]


class SigNum(Enum):
    """Standard Unix signal numbers."""
    SIGHUP = 1
    SIGINT = 2
    SIGQUIT = 3
    SIGILL = 4
    SIGTRAP = 5
    SIGABRT = 6
    SIGBUS = 7
    SIGFPE = 8
    SIGKILL = 9
    SIGUSR1 = 10
    SIGSEGV = 11
    SIGUSR2 = 12
    SIGPIPE = 13
    SIGALRM = 14
    SIGTERM = 15
    SIGSTKFLT = 16
    SIGCHLD = 17
    SIGCONT = 18
    SIGSTOP = 19
    SIGTSTP = 20
    SIGTTIN = 21
    SIGTTOU = 22
    SIGURG = 23
    SIGXCPU = 24
    SIGXFSZ = 25
    SIGVTALRM = 26
    SIGPROF = 27
    SIGWINCH = 28
    SIGIO = 29
    SIGPWR = 30
    SIGSYS = 31


class SigType(Enum):
    """Signal classification."""
    TERMINAL = auto()
    IGNORED = auto()
    CHILD = auto()
    ALARM = auto()
    ERROR = auto()
    CUSTOM = auto()


_signal_names: Dict[str, int] = {}
_signal_numbers: Dict[int, str] = {}
for _name in dir(signal):
    if _name.startswith("SIG") and not _name.startswith("SIG_"):
        _signal_numbers[getattr(signal, _name)] = _name
        _signal_names[_name] = getattr(signal, _name)


def get_signal_name(sig: int) -> str:
    """Get signal name from number."""
    return _signal_numbers.get(sig, f"SIG_UNKNOWN({sig})")


def get_signal_number(name: str) -> int:
    """Get signal number from name."""
    return _signal_names.get(name, 0)


class SignalHandler:
    """Reusable signal handler with callback registration."""

    def __init__(self) -> None:
        self._handlers: Dict[int, Callable] = {}
        self._old_handlers: Dict[int, Any] = {}
        self._lock = threading.Lock()
        self._signal_queue: List[int] = []
        self._queue_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def register(self, sig: int, handler: Callable[[int], None]) -> None:
        """Register a handler for a signal.

        Args:
            sig: Signal number.
            handler: Callback function receiving signal number.
        """
        with self._lock:
            if sig in self._handlers:
                raise ValueError(f"Handler already registered for {get_signal_name(sig)}")
            self._handlers[sig] = handler
            self._old_handlers[sig] = signal.signal(sig, self._dispatch)

    def unregister(self, sig: int) -> bool:
        """Unregister handler and restore previous handler."""
        with self._lock:
            if sig not in self._handlers:
                return False
            if sig in self._old_handlers:
                signal.signal(sig, self._old_handlers[sig])
            del self._handlers[sig]
            del self._old_handlers[sig]
            return True

    def _dispatch(self, sig: int, frame: Any) -> None:
        """Internal signal dispatch (runs in signal context)."""
        with self._queue_lock:
            self._signal_queue.append(sig)
        if sig in self._handlers:
            try:
                self._handlers[sig](sig)
            except Exception:
                pass

    def start_async_processing(self) -> None:
        """Start background thread to process queued signals."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._process_loop, daemon=True)
        self._thread.start()

    def stop_async_processing(self) -> None:
        """Stop background signal processing."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _process_loop(self) -> None:
        """Background signal processing loop."""
        while self._running:
            sig = None
            with self._queue_lock:
                if self._signal_queue:
                    sig = self._signal_queue.pop(0)
            if sig is not None:
                handler = self._handlers.get(sig)
                if handler:
                    try:
                        handler(sig)
                    except Exception:
                        pass
            else:
                threading.Event().wait(0.01)

    def pending(self) -> List[int]:
        """Return list of pending signals."""
        with self._queue_lock:
            return list(self._signal_queue)

    def clear_pending(self) -> None:
        """Clear pending signal queue."""
        with self._queue_lock:
            self._signal_queue.clear()


class SignalBlocker:
    """Context manager to temporarily block signals."""

    def __init__(self, sigs: Optional[List[int]] = None) -> None:
        self._sigs = sigs or [signal.SIGINT, signal.SIGTERM]
        self._old_mask: Any = None

    def __enter__(self) -> "SignalBlocker":
        self._old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, self._sigs)
        return self

    def __exit__(self, *args: Any) -> None:
        if self._old_mask is not None:
            signal.pthread_sigmask(signal.SIG_SETMASK, self._old_mask)


class SignalWaiter:
    """Wait for one or more signals."""

    def __init__(self, sigs: List[int]) -> None:
        self._sigs = sigs
        self._received: Set[int] = set()
        self._cond = threading.Condition()
        self._handler = signal.signal(sigs[0] if sigs else signal.SIGTERM, self._on_signal)

    def _on_signal(self, sig: int, frame: Any) -> None:
        """Handle received signal."""
        with self._cond:
            self._received.add(sig)
            self._cond.notify_all()

    def wait(self, timeout: Optional[float] = None) -> Optional[int]:
        """Wait for any signal to be received.

        Returns:
            Signal number that was received, or None on timeout.
        """
        with self._cond:
            while not self._received:
                if not self._cond.wait(timeout=timeout):
                    return None
            return next(iter(self._received))

    def wait_all(self, timeout: Optional[float] = None) -> bool:
        """Wait for all signals to be received.

        Returns:
            True if all received, False on timeout.
        """
        with self._cond:
            while len(self._received) < len(self._sigs):
                if not self._cond.wait(timeout=timeout):
                    return False
            return True

    def reset(self) -> None:
        """Reset state to wait again."""
        with self._cond:
            self._received.clear()


class SignalMonitor:
    """Monitor signals over time with statistics."""

    def __init__(self) -> None:
        self._counts: Dict[int, int] = defaultdict(int)
        self._first_seen: Dict[int, float] = {}
        self._last_seen: Dict[int, float] = {}
        self._lock = threading.Lock()

    def record(self, sig: int) -> None:
        """Record a signal occurrence."""
        import time
        now = time.time()
        with self._lock:
            self._counts[sig] += 1
            if sig not in self._first_seen:
                self._first_seen[sig] = now
            self._last_seen[sig] = now

    def count(self, sig: Optional[int] = None) -> int:
        """Get signal count."""
        with self._lock:
            if sig is not None:
                return self._counts[sig]
            return sum(self._counts.values())

    def stats(self) -> Dict[str, Any]:
        """Get signal statistics."""
        import time
        with self._lock:
            return {
                get_signal_name(sig): {
                    "count": count,
                    "first": self._first_seen.get(sig),
                    "last": self._last_seen.get(sig),
                }
                for sig, count in self._counts.items()
            }


class SignalSet:
    """Python wrapper around sigset_t."""

    def __init__(self, sigs: Optional[List[int]] = None) -> None:
        self._set = signal.sigset_t()
        if sigs:
            for sig in sigs:
                signal.sigaddset(self._set, sig)

    def add(self, sig: int) -> None:
        """Add signal to set."""
        signal.sigaddset(self._set, sig)

    def remove(self, sig: int) -> None:
        """Remove signal from set."""
        signal.sigdelset(self._set, sig)

    def contains(self, sig: int) -> bool:
        """Check if signal is in set."""
        return signal.sigismember(self._set, sig)

    def empty(self) -> None:
        """Empty the signal set."""
        signal.sigemptyset(self._set)

    def fill(self) -> None:
        """Fill set with all signals."""
        signal.sigfillset(self._set)


def register_handler(sig: int, handler: Callable[[int], None]) -> None:
    """Register a simple signal handler.

    Args:
        sig: Signal number.
        handler: Callback receiving signal number.
    """
    signal.signal(sig, lambda s, f: handler(s))


def ignore_signal(sig: int) -> None:
    """Ignore a signal (SIG_IGN)."""
    signal.signal(sig, signal.SIG_IGN)


def default_handler(sig: int) -> None:
    """Restore default handler for a signal."""
    signal.signal(sig, signal.SIG_DFL)


def block_signals(sigs: List[int]) -> Any:
    """Block signals and return old mask.

    Returns:
        Old signal mask for restoration.
    """
    return signal.pthread_sigmask(signal.SIG_BLOCK, sigs)


def unblock_signals(sigs: List[int]) -> Any:
    """Unblock signals and return old mask."""
    return signal.pthread_sigmask(signal.SIG_UNBLOCK, sigs)


def wait_for_signal(sig: int, timeout: Optional[float] = None) -> bool:
    """Wait for a specific signal.

    Returns:
        True if signal received, False on timeout.
    """
    waiter = SignalWaiter([sig])
    result = waiter.wait(timeout=timeout)
    return result is not None


_raised_signals: Set[int] = set()


def raised_signals() -> Set[int]:
    """Get set of signals raised since last call."""
    global _raised_signals
    sigs = set(_raised_signals)
    _raised_signals = set()
    return sigs
